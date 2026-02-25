#!/usr/bin/env bash
# ============================================================
# Spark-ling: Submit PySpark Job to Dataproc
# ============================================================
# Uploads source files and submits a PySpark job to Dataproc.
#
# Usage:
#   ./gcp/submit_job.sh <python_file> [-- <job_args>...]
#
# Examples:
#   ./gcp/submit_job.sh pipelines/daily_transactions.py -- --date 2025-06-15
#   ./gcp/submit_job.sh pipelines/customer_dim_scd.py
#   ./gcp/submit_job.sh src/data_generator.py
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${SCRIPT_DIR}/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "❌ Missing ${ENV_FILE}"
    exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"

# ── Parse arguments ─────────────────────────────────────────
if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <python_file> [-- <job_args>...]"
    echo ""
    echo "Examples:"
    echo "  $0 pipelines/daily_transactions.py -- --date 2025-06-15"
    echo "  $0 pipelines/customer_dim_scd.py"
    exit 1
fi

MAIN_PY="$1"
shift

# Separate job args (after --)
JOB_ARGS=()
if [[ "${1:-}" == "--" ]]; then
    shift
    JOB_ARGS=("$@")
fi

MAIN_PY_PATH="${PROJECT_ROOT}/${MAIN_PY}"
if [[ ! -f "$MAIN_PY_PATH" ]]; then
    echo "❌ File not found: ${MAIN_PY_PATH}"
    exit 1
fi

STAGING="gs://${GCS_BUCKET}/staging"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
JOB_STAGING="${STAGING}/${TIMESTAMP}"

echo "============================================================"
echo "🚀 Spark-ling: Submitting Job to Dataproc"
echo "============================================================"
echo "  Main file:  ${MAIN_PY}"
echo "  Cluster:    ${DATAPROC_CLUSTER} (${GCP_REGION})"
echo "  Staging:    ${JOB_STAGING}"
if [[ ${#JOB_ARGS[@]} -gt 0 ]]; then
    echo "  Job args:   ${JOB_ARGS[*]}"
fi
echo "============================================================"
echo ""

# ── Upload source files ─────────────────────────────────────
echo "📤 Uploading source files to staging..."
gsutil -m cp "${MAIN_PY_PATH}" "${JOB_STAGING}/"

# Upload supporting src/ and configs/ modules
PY_FILES=()
for dir in src configs; do
    if [[ -d "${PROJECT_ROOT}/${dir}" ]]; then
        while IFS= read -r -d '' f; do
            PY_FILES+=("$f")
        done < <(find "${PROJECT_ROOT}/${dir}" -name '*.py' -print0)
    fi
done

if [[ ${#PY_FILES[@]} -gt 0 ]]; then
    gsutil -m cp "${PY_FILES[@]}" "${JOB_STAGING}/deps/"
    echo "   ✅ Uploaded ${#PY_FILES[@]} supporting files."
fi

# Build --py-files argument for additional Python modules
PY_FILES_ARG=""
if [[ ${#PY_FILES[@]} -gt 0 ]]; then
    PY_FILE_URIS=()
    for f in "${PY_FILES[@]}"; do
        PY_FILE_URIS+=("${JOB_STAGING}/deps/$(basename "$f")")
    done
    PY_FILES_ARG=$(IFS=,; echo "${PY_FILE_URIS[*]}")
fi

# ── Submit job ───────────────────────────────────────────────
echo "🚀 Submitting job..."
SUBMIT_CMD=(
    gcloud dataproc jobs submit pyspark
    "${JOB_STAGING}/$(basename "$MAIN_PY")"
    --cluster="${DATAPROC_CLUSTER}"
    --region="${GCP_REGION}"
    --properties="spark.sql.adaptive.enabled=true"
)

if [[ -n "$PY_FILES_ARG" ]]; then
    SUBMIT_CMD+=(--py-files="${PY_FILES_ARG}")
fi

if [[ ${#JOB_ARGS[@]} -gt 0 ]]; then
    SUBMIT_CMD+=(-- "${JOB_ARGS[@]}")
fi

echo "  Command: ${SUBMIT_CMD[*]}"
echo ""

"${SUBMIT_CMD[@]}"

echo ""
echo "============================================================"
echo "✅ Job submitted and completed!"
echo "============================================================"
