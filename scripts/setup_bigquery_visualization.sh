#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   PROJECT_ID=... BUCKET_NAME=... ./scripts/setup_bigquery_visualization.sh
# Optional:
#   REGION=northamerica-northeast1
#   BQ_DATASET=log_analytics
#   BQ_LOCATION=northamerica-northeast1
#   OUTPUT_PREFIX=output

PROJECT_ID="${PROJECT_ID:-}"
REGION="${REGION:-northamerica-northeast1}"
BUCKET_NAME="${BUCKET_NAME:-}"
BQ_DATASET="${BQ_DATASET:-log_analytics}"
BQ_LOCATION="${BQ_LOCATION:-$REGION}"
OUTPUT_PREFIX="${OUTPUT_PREFIX:-output}"

if [[ -z "$PROJECT_ID" || -z "$BUCKET_NAME" ]]; then
  echo "PROJECT_ID and BUCKET_NAME are required"
  echo "Example: PROJECT_ID=my-project BUCKET_NAME=my-bucket ./scripts/setup_bigquery_visualization.sh"
  exit 1
fi

FULL_DATASET_RESOURCE="${PROJECT_ID}:${BQ_DATASET}"
FULL_DATASET_SQL="${PROJECT_ID}.${BQ_DATASET}"

echo "[INFO] Using project: ${PROJECT_ID}"
echo "[INFO] Using bucket: gs://${BUCKET_NAME}/${OUTPUT_PREFIX}"
echo "[INFO] Using BigQuery dataset: ${FULL_DATASET_RESOURCE} (${BQ_LOCATION})"

gcloud config set project "$PROJECT_ID" >/dev/null

if ! bq --location="$BQ_LOCATION" ls -d "$FULL_DATASET_RESOURCE" >/dev/null 2>&1; then
  bq --location="$BQ_LOCATION" mk --dataset --description "Dataproc log analytics outputs" "$FULL_DATASET_RESOURCE"
fi

create_external_table() {
  local table_name="$1"
  local gcs_uri="$2"
  local def_file
  def_file="$(mktemp)"

  bq mkdef --autodetect --source_format=PARQUET "$gcs_uri" > "$def_file"
  bq rm -f -t "${FULL_DATASET_RESOURCE}.${table_name}" >/dev/null 2>&1 || true
  bq mk --external_table_definition="$def_file" "${FULL_DATASET_RESOURCE}.${table_name}"
  rm -f "$def_file"
}

echo "[INFO] Creating external tables over Dataproc parquet outputs"
create_external_table "session_trace" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/session_trace/*.parquet"
create_external_table "user_sessions" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/user_sessions/*.parquet"
create_external_table "slo_hourly" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/slo/hourly/*.parquet"
create_external_table "slo_daily" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/slo/daily/*.parquet"
create_external_table "change_impact_attribution" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/change_impact_attribution/*.parquet"
create_external_table "anomalies" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/anomalies/*.parquet"
create_external_table "top_offenders" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/top_offenders/*.parquet"
create_external_table "skew_study" "gs://${BUCKET_NAME}/${OUTPUT_PREFIX}/skew_study/*.parquet"

echo "[INFO] Creating chart-ready views"
bq query --use_legacy_sql=false <<SQL
CREATE OR REPLACE VIEW `${FULL_DATASET_SQL}.vw_slo_trends` AS
SELECT
  TIMESTAMP(time_bucket) AS time_bucket,
  service,
  endpoint,
  region,
  request_count,
  avg_latency_ms,
  p50_latency_ms,
  p95_latency_ms,
  p99_latency_ms,
  error_rate
FROM `${FULL_DATASET_SQL}.slo_hourly`;

CREATE OR REPLACE VIEW `${FULL_DATASET_SQL}.vw_anomaly_timeline` AS
SELECT
  TIMESTAMP(hour_bucket) AS hour_bucket,
  service,
  endpoint,
  region,
  request_count,
  error_rate,
  p95_latency_ms,
  baseline_error_avg,
  baseline_p95_avg,
  severity,
  error_anomaly,
  latency_anomaly,
  explanation
FROM `${FULL_DATASET_SQL}.anomalies`;

CREATE OR REPLACE VIEW `${FULL_DATASET_SQL}.vw_deployment_impact` AS
SELECT
  TIMESTAMP(time_bucket) AS time_bucket,
  service,
  endpoint,
  region,
  deploy_version,
  TIMESTAMP(deploy_time) AS deploy_time,
  minutes_since_deploy,
  request_count,
  avg_latency_ms,
  p95_latency_ms,
  error_rate
FROM `${FULL_DATASET_SQL}.change_impact_attribution`;

CREATE OR REPLACE VIEW `${FULL_DATASET_SQL}.vw_top_offenders` AS
SELECT
  service,
  endpoint,
  region,
  anomaly_windows,
  max_severity,
  peak_p95_ms,
  peak_error_rate,
  explanations
FROM `${FULL_DATASET_SQL}.top_offenders`;

CREATE OR REPLACE VIEW `${FULL_DATASET_SQL}.vw_skew_benchmark` AS
SELECT
  top_endpoint,
  top_endpoint_share,
  salt_buckets,
  target_partitions,
  before_max_partition_rows,
  before_min_partition_rows,
  before_avg_partition_rows,
  after_max_partition_rows,
  after_min_partition_rows,
  after_avg_partition_rows,
  runtime_before_ms,
  runtime_after_ms,
  runtime_improvement_pct
FROM `${FULL_DATASET_SQL}.skew_study`;
SQL

echo "[DONE] BigQuery dataset and views created"
echo "[NEXT] Open BigQuery Studio and chart these views:"
echo "  - ${FULL_DATASET_SQL}.vw_slo_trends"
echo "  - ${FULL_DATASET_SQL}.vw_anomaly_timeline"
echo "  - ${FULL_DATASET_SQL}.vw_deployment_impact"
echo "  - ${FULL_DATASET_SQL}.vw_top_offenders"
echo "  - ${FULL_DATASET_SQL}.vw_skew_benchmark"
