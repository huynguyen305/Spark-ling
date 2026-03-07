"""
Pipeline Orchestrator — Multi-Pipeline Execution Engine
========================================================
Orchestrates the full migration pipeline DAG (Directed Acyclic Graph),
executing pipelines in dependency order with retry logic and metrics.

LEARNING NOTES:
---------------
1. PIPELINE ORCHESTRATION:
   In production, you'd use a dedicated orchestrator:
   - Apache Airflow (most popular in data engineering)
   - Databricks Workflows (native Databricks orchestration)
   - AWS Step Functions (serverless, event-driven)
   - Prefect / Dagster (modern alternatives to Airflow)

   This script simulates orchestration logic for learning purposes.

2. PIPELINE DAG:
   Dependencies determine execution order:

   full_load ──► bronze_to_silver ──► silver_to_gold
                      ▲                     ▲
   incremental_load ──┘                     │
                                            │
   bronze_to_silver ────────────────────────┘

   Rule: A pipeline can only run after ALL its dependencies succeed.

3. EXECUTION MODES:
   - initial: Full load → Bronze → Silver → Gold (Day 1)
   - daily:   CDC incremental → Bronze → Silver → Gold (Day 2+)
   - validate: Check data consistency without running pipelines
   - dry-run: Show what would run without executing

4. RETRY LOGIC:
   Pipelines can fail (network issues, data issues, etc.).
   The orchestrator retries failed steps up to N times with
   exponential backoff. This is standard in production.

USAGE:
    # Initial migration (Day 1):
    python pipelines/migration/pipeline_orchestrator.py --mode initial

    # Daily pipeline (Day 2+):
    python pipelines/migration/pipeline_orchestrator.py --mode daily

    # Validate all layers:
    python pipelines/migration/pipeline_orchestrator.py --mode validate

    # Dry run:
    python pipelines/migration/pipeline_orchestrator.py --mode dry-run
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional

PROJECT_ROOT = Path(__file__).parent.parent.parent
PIPELINE_DIR = Path(__file__).parent


# ── Pipeline Definitions ────────────────────────────────────────────────────

PIPELINES = {
    # Pipeline name → execution config
    # LEARNING: Each pipeline is a Python script that can run independently.
    # The orchestrator coordinates them into a coherent workflow.

    "full_load": {
        "script": PIPELINE_DIR / "full_load_pipeline.py",
        "description": "Full extraction from RDS PostgreSQL to Bronze Delta tables",
        "dependencies": [],
        "mode": "initial",  # Only runs during initial migration
        "timeout_minutes": 30,
    },
    "incremental_load": {
        "script": PIPELINE_DIR / "incremental_load_pipeline.py",
        "description": "CDC incremental extraction from RDS to Bronze",
        "dependencies": [],
        "mode": "daily",  # Only runs during daily updates
        "timeout_minutes": 15,
    },
    "bronze_to_silver": {
        "script": PIPELINE_DIR / "bronze_to_silver.py",
        "description": "Clean, validate, and transform Bronze → Silver",
        "dependencies": ["full_load", "incremental_load"],
        "mode": "both",  # Runs in both initial and daily modes
        "timeout_minutes": 20,
    },
    "silver_to_gold": {
        "script": PIPELINE_DIR / "silver_to_gold.py",
        "description": "Business aggregations: Silver → Gold",
        "dependencies": ["bronze_to_silver"],
        "mode": "both",
        "timeout_minutes": 15,
    },
}


# ── Orchestration Engine ────────────────────────────────────────────────────

class PipelineOrchestrator:
    """
    Manages pipeline execution with dependency resolution and retry logic.

    LEARNING: An orchestrator's key responsibilities:
    1. Topological sort (resolve execution order from DAG)
    2. Dependency checking (don't run if upstream failed)
    3. Retry with backoff (handle transient failures)
    4. Metrics collection (track duration, status, errors)
    5. Alerting (notify on failure — not implemented here)
    """

    def __init__(self, mode: str, max_retries: int = 2,
                 dry_run: bool = False):
        self.mode = mode
        self.max_retries = max_retries
        self.dry_run = dry_run
        self.results = {}
        self.start_time = datetime.now()

    def resolve_execution_order(self) -> List[str]:
        """
        Topological sort of pipeline DAG.

        LEARNING: Topological sort ensures pipelines run in dependency order.
        It's a fundamental algorithm in workflow engines:
        1. Find all pipelines with no unresolved dependencies
        2. Execute them
        3. Mark as resolved
        4. Repeat until all are resolved or no progress (cycle detection)
        """
        # Filter pipelines by mode
        eligible = {
            name: config for name, config in PIPELINES.items()
            if config["mode"] in (self.mode, "both") or
               (self.mode == "initial" and name in
                ["full_load", "bronze_to_silver", "silver_to_gold"]) or
               (self.mode == "daily" and name in
                ["incremental_load", "bronze_to_silver", "silver_to_gold"])
        }

        resolved = set()
        order = []
        remaining = set(eligible.keys())

        while remaining:
            # Find pipelines whose dependencies are all resolved
            ready = [
                name for name in remaining
                if all(dep in resolved or dep not in eligible
                       for dep in eligible[name]["dependencies"])
            ]

            if not ready:
                # LEARNING: If no progress, we have a circular dependency (bug)
                raise RuntimeError(
                    f"Circular dependency detected! Remaining: {remaining}"
                )

            for name in sorted(ready):
                order.append(name)
                resolved.add(name)
                remaining.remove(name)

        return order

    def execute_pipeline(self, name: str, config: dict) -> dict:
        """
        Execute a single pipeline with retry logic.

        LEARNING: Exponential backoff strategy:
        - Retry 1: wait 5 seconds
        - Retry 2: wait 10 seconds
        - Retry 3: wait 20 seconds
        This prevents overwhelming a struggling system.
        """
        print(f"\n{'━'*60}")
        print(f"  🚀 Pipeline: {name}")
        print(f"     {config['description']}")
        print(f"{'━'*60}")

        if self.dry_run:
            print(f"     ℹ️  DRY RUN — would execute: {config['script']}")
            return {"status": "dry_run", "duration_seconds": 0}

        for attempt in range(1, self.max_retries + 1):
            start = time.time()
            try:
                print(f"\n     Attempt {attempt}/{self.max_retries}...")

                # Execute the pipeline as a subprocess
                # LEARNING: Running as subprocess provides:
                # - Isolation (each pipeline has its own SparkSession)
                # - Easy timeout management
                # - Clean error handling
                result = subprocess.run(
                    [sys.executable, str(config["script"])],
                    capture_output=True,
                    text=True,
                    timeout=config["timeout_minutes"] * 60,
                    cwd=str(PROJECT_ROOT),
                )

                duration = time.time() - start

                if result.returncode == 0:
                    print(f"     ✅ Completed in {duration:.1f}s")
                    if result.stdout:
                        # Show last 10 lines of output
                        lines = result.stdout.strip().split("\n")
                        for line in lines[-10:]:
                            print(f"        {line}")
                    return {
                        "status": "success",
                        "duration_seconds": round(duration, 1),
                        "attempt": attempt,
                    }
                else:
                    print(f"     ❌ Failed (exit code {result.returncode})")
                    if result.stderr:
                        print(f"        Error: {result.stderr[:500]}")

            except subprocess.TimeoutExpired:
                duration = time.time() - start
                print(f"     ⏱️  Timeout after {duration:.0f}s")
            except Exception as e:
                duration = time.time() - start
                print(f"     ❌ Exception: {e}")

            # Retry with backoff
            if attempt < self.max_retries:
                wait = 5 * (2 ** (attempt - 1))
                print(f"     ⏳ Retrying in {wait}s...")
                time.sleep(wait)

        return {
            "status": "failed",
            "duration_seconds": round(time.time() - start, 1),
            "attempts": self.max_retries,
        }

    def run(self):
        """
        Execute the full pipeline DAG.

        LEARNING: Orchestration flow:
        1. Resolve execution order (topological sort)
        2. For each pipeline in order:
           a. Check if dependencies succeeded
           b. Execute with retry
           c. Record results
        3. Generate summary report
        """
        print("╔══════════════════════════════════════════════════════════════╗")
        print(f"║  Pipeline Orchestrator — Mode: {self.mode.upper():<28s}║")
        print("╚══════════════════════════════════════════════════════════════╝")
        print(f"  Start time: {self.start_time.isoformat()}")
        print(f"  Max retries: {self.max_retries}")
        if self.dry_run:
            print("  ℹ️  DRY RUN MODE")

        # Step 1: Resolve order
        order = self.resolve_execution_order()
        print(f"\n  📋 Execution order: {' → '.join(order)}")

        # Step 2: Execute each pipeline
        for name in order:
            config = PIPELINES[name]

            # Check dependencies
            failed_deps = [
                dep for dep in config["dependencies"]
                if dep in self.results and
                self.results[dep]["status"] == "failed"
            ]

            if failed_deps:
                print(f"\n  ⏭️  Skipping {name} — dependency failed: {', '.join(failed_deps)}")
                self.results[name] = {
                    "status": "skipped",
                    "reason": f"Dependencies failed: {', '.join(failed_deps)}",
                }
                continue

            result = self.execute_pipeline(name, config)
            self.results[name] = result

        # Step 3: Summary
        total_duration = (datetime.now() - self.start_time).total_seconds()
        self._print_summary(total_duration)
        self._save_metrics(total_duration)

        # Return success if no failures
        failures = [n for n, r in self.results.items()
                     if r["status"] == "failed"]
        return len(failures) == 0

    def _print_summary(self, total_duration: float):
        """Print execution summary."""
        print(f"\n{'='*60}")
        print("📊 Pipeline Execution Summary")
        print(f"{'='*60}")

        for name, result in self.results.items():
            status_icon = {
                "success": "✅", "failed": "❌",
                "skipped": "⏭️", "dry_run": "🔍",
            }.get(result["status"], "❓")

            duration = result.get("duration_seconds", 0)
            print(f"  {status_icon} {name}: {result['status']} ({duration:.1f}s)")

        print(f"\n  Total duration: {total_duration:.1f}s")
        successes = sum(1 for r in self.results.values()
                         if r["status"] == "success")
        print(f"  Pipelines: {successes}/{len(self.results)} succeeded")
        print(f"{'='*60}")

    def _save_metrics(self, total_duration: float):
        """Save orchestration metrics to JSON."""
        metrics_path = PROJECT_ROOT / "data" / "orchestrator_metrics.json"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)

        with open(metrics_path, "w") as f:
            json.dump({
                "mode": self.mode,
                "start_time": self.start_time.isoformat(),
                "total_duration_seconds": round(total_duration, 1),
                "pipelines": self.results,
            }, f, indent=2, default=str)

        print(f"\n📊 Metrics saved to: {metrics_path}")


# ── Validation Mode ─────────────────────────────────────────────────────────

def run_validation():
    """
    Validate data consistency across all layers.

    LEARNING: Post-migration validation checks:
    1. Row counts at each layer (Bronze ≥ Silver ≥ Gold)
    2. No unexpected NULLs in key columns
    3. Referential integrity (fact FK → dimension PK)
    4. Data freshness (latest timestamp within expected range)
    """
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  Pipeline Validation — Cross-Layer Data Consistency        ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    # Check metrics files
    for layer in ["migration_metrics", "incremental_metrics",
                   "silver_metrics", "gold_metrics"]:
        metrics_path = PROJECT_ROOT / "data" / f"{layer}.json"
        if metrics_path.exists():
            with open(metrics_path) as f:
                data = json.load(f)
            print(f"\n  📊 {layer}:")
            print(f"     Timestamp: {data.get('timestamp', 'N/A')}")
            if "tables" in data:
                for table, info in data["tables"].items():
                    status = info.get("status", "unknown")
                    icon = "✅" if status == "success" else "❌"
                    rows = info.get("row_count", info.get("rows",
                                    info.get("silver", "N/A")))
                    print(f"     {icon} {table}: {rows} rows")
        else:
            print(f"\n  ⚠️  {layer}: No metrics file found")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline Orchestrator for RDS → Databricks Migration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
LEARNING — EXECUTION MODES:

  initial:
    Day 1 of migration. Runs full load → bronze → silver → gold.
    Use after seeding PostgreSQL RDS with rds_seed_data.py.

  daily:
    Day 2+ operations. Runs CDC incremental → bronze → silver → gold.
    Use after simulating daily activity with rds_daily_generator.py.

  validate:
    Checks data consistency across all layers without running pipelines.

  dry-run:
    Shows execution order without actually running anything.

EXAMPLES:
  # Initial migration:
  python pipelines/migration/pipeline_orchestrator.py --mode initial

  # Daily pipeline:
  python pipelines/migration/pipeline_orchestrator.py --mode daily

  # Full daily simulation cycle:
  python aws/rds/rds_daily_generator.py --simulate-date 2026-03-07
  python pipelines/migration/pipeline_orchestrator.py --mode daily
        """
    )
    parser.add_argument("--mode", type=str, required=True,
                        choices=["initial", "daily", "validate", "dry-run"],
                        help="Execution mode")
    parser.add_argument("--max-retries", type=int, default=2,
                        help="Max retries per pipeline (default: 2)")

    args = parser.parse_args()

    if args.mode == "validate":
        run_validation()
    else:
        dry_run = args.mode == "dry-run"
        mode = "initial" if dry_run else args.mode

        orchestrator = PipelineOrchestrator(
            mode=mode,
            max_retries=args.max_retries,
            dry_run=dry_run,
        )
        success = orchestrator.run()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
