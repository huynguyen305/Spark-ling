# EC2 Cost Optimization — Auto-Stop & Scheduled Shutdown

Two-layer cost savings for Spark-ling EC2 instances:

| Layer | Trigger | What it does |
|-------|---------|-------------|
| **CloudWatch Alarm** | CPU < 5% for 1 hour | Auto-stop idle instances |
| **SSM Scheduler** | Cron schedule | Stop at 22:00 ICT / Start at 08:00 ICT weekdays |

Both implementations (CloudFormation and Terraform) are functionally identical.

---

## Option A: CloudFormation

```bash
# Deploy (replace with your instance ID)
aws cloudformation create-stack \
  --stack-name sparkling-cost-opt \
  --template-body file://aws/cost-optimization.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=InstanceId,ParameterValue=i-0123456789abcdef0 \
    ParameterKey=NotificationEmail,ParameterValue=you@example.com

# Check status
aws cloudformation describe-stacks \
  --stack-name sparkling-cost-opt \
  --query 'Stacks[0].StackStatus'

# View outputs (alarm ARN, console URLs)
aws cloudformation describe-stacks \
  --stack-name sparkling-cost-opt \
  --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
  --output table

# Teardown
aws cloudformation delete-stack --stack-name sparkling-cost-opt
```

## Option B: Terraform

```bash
cd aws/terraform/cost-optimization

terraform init
terraform plan -var="instance_id=i-0123456789abcdef0"
terraform apply -var="instance_id=i-0123456789abcdef0"

# With notifications
terraform apply \
  -var="instance_id=i-0123456789abcdef0" \
  -var="notification_email=you@example.com"

# Teardown
terraform destroy
```

---

## Customizing Schedules

Both templates use **UTC** for cron expressions. To convert from **ICT (UTC+7)**:

| Desired ICT time | UTC equivalent |
|-------------------|----------------|
| 08:00 ICT | 01:00 UTC |
| 22:00 ICT | 15:00 UTC |
| 00:00 ICT | 17:00 UTC |

**CloudFormation** — pass as parameter:

```bash
--parameters ParameterKey=StopCron,ParameterValue="cron(0 17 ? * * *)"
```

**Terraform** — pass as variable:

```bash
terraform apply -var="stop_cron=cron(0 17 ? * * *)"
```

## Parameters Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `InstanceId` | *(required)* | EC2 instance to monitor |
| `CpuThreshold` | `5` | CPU % → idle threshold |
| `IdlePeriodMinutes` | `60` | Low-CPU duration before stop |
| `StopCron` | `cron(0 15 ? * * *)` | Nightly stop (22:00 ICT) |
| `StartCron` | `cron(0 1 ? * MON-FRI *)` | Morning start (08:00 ICT) |
| `NotificationEmail` | *(empty)* | Email for alerts |
| `ProjectTag` | `Spark-ling` | Tag for instance targeting |

## How It Works

```
┌─────────────────────────────────────────────────────────┐
│                   EC2 Instance                          │
│                   (Project: Spark-ling)                 │
└───────────┬─────────────────────────────┬───────────────┘
            │                             │
    ┌───────▼────────┐           ┌────────▼───────┐
    │  CloudWatch     │           │  SSM Scheduler  │
    │  Alarm          │           │                 │
    │                 │           │  Stop:  22:00   │
    │  CPU < 5%       │           │  Start: 08:00   │
    │  for 60 min     │           │  (weekdays)     │
    │       │         │           │       │         │
    │  ── STOP ──     │           │  ── STOP/START  │
    └───────┬─────────┘           └────────┬────────┘
            │                              │
    ┌───────▼──────────────────────────────▼───────┐
    │              SNS Notification                 │
    │              (optional email alert)           │
    └──────────────────────────────────────────────┘
```
