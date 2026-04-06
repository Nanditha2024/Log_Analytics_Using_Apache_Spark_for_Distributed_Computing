#!/usr/bin/env bash
set -euo pipefail

# CI/CD helper: runs Dataproc Spark job and refreshes BigQuery external tables + views.
#
# Required env vars:
#   PROJECT_ID, REGION, CLUSTER_NAME, BUCKET_NAME
# Optional env vars:
#   BQ_DATASET (default: log_analytics)
#   OUTPUT_PREFIX (default: output)
#   APP_JAR (default: target/scala-2.12/distributed-log-analytics-spark_2.12-0.1.0.jar)
#   LOGS_PATH (default: gs://$BUCKET_NAME/data/logs.parquet)
#   DEPLOYMENTS_PATH (default: gs://$BUCKET_NAME/data/deployments.parquet)
#   HOST_META_PATH (default: gs://$BUCKET_NAME/data/host_meta.parquet)

: "${PROJECT_ID:?PROJECT_ID is required}"
: "${REGION:?REGION is required}"
: "${CLUSTER_NAME:?CLUSTER_NAME is required}"
: "${BUCKET_NAME:?BUCKET_NAME is required}"

BQ_DATASET="${BQ_DATASET:-log_analytics}"
OUTPUT_PREFIX="${OUTPUT_PREFIX:-output}"
APP_JAR="${APP_JAR:-target/scala-2.12/distributed-log-analytics-spark_2.12-0.1.0.jar}"

LOGS_PATH="${LOGS_PATH:-gs://${BUCKET_NAME}/data/logs.parquet}"
DEPLOYMENTS_PATH="${DEPLOYMENTS_PATH:-gs://${BUCKET_NAME}/data/deployments.parquet}"
HOST_META_PATH="${HOST_META_PATH:-gs://${BUCKET_NAME}/data/host_meta.parquet}"
OUTPUT_PATH="gs://${BUCKET_NAME}/${OUTPUT_PREFIX}"

echo "[INFO] Project: ${PROJECT_ID}"
echo "[INFO] Region: ${REGION}"
echo "[INFO] Cluster: ${CLUSTER_NAME}"
echo "[INFO] Output path: ${OUTPUT_PATH}"

gcloud config set project "${PROJECT_ID}" >/dev/null

if [[ ! -f "${APP_JAR}" ]]; then
  echo "[ERROR] JAR not found at ${APP_JAR}. Build it first (for example: sbt clean package)."
  exit 1
fi

echo "[STEP] Submitting Dataproc Spark job"
gcloud dataproc jobs submit spark \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --class="com.loganalytics.Main" \
  --jars="${APP_JAR}" \
  -- \
  --logs "${LOGS_PATH}" \
  --deployments "${DEPLOYMENTS_PATH}" \
  --host-meta "${HOST_META_PATH}" \
  --output "${OUTPUT_PATH}" \
  --input-format parquet \
  --session-timeout-minutes 30 \
  --attribution-window-hours 6 \
  --baseline-hours 24 \
  --salt-buckets 16 \
  --target-partitions 64

echo "[STEP] Verifying expected parquet outputs"
gsutil ls "${OUTPUT_PATH}/slo/hourly/*.parquet" >/dev/null
gsutil ls "${OUTPUT_PATH}/anomalies/*.parquet" >/dev/null
gsutil ls "${OUTPUT_PATH}/change_impact_attribution/*.parquet" >/dev/null

echo "[STEP] Creating dataset (if needed)"
bq --location="${REGION}" mk -d "${PROJECT_ID}:${BQ_DATASET}" >/dev/null 2>&1 || true

echo "[STEP] Recreating external tables"
for t in session_trace user_sessions slo_hourly slo_daily change_impact_attribution anomalies top_offenders skew_study; do
  bq rm -f -t "${PROJECT_ID}:${BQ_DATASET}.${t}" >/dev/null 2>&1 || true
done

bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/session_trace/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.session_trace" >/dev/null
bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/user_sessions/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.user_sessions" >/dev/null
bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/slo/hourly/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.slo_hourly" >/dev/null
bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/slo/daily/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.slo_daily" >/dev/null
bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/change_impact_attribution/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.change_impact_attribution" >/dev/null
bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/anomalies/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.anomalies" >/dev/null
bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/top_offenders/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.top_offenders" >/dev/null
bq mk --external_table_definition="PARQUET=${OUTPUT_PATH}/skew_study/*.parquet" "${PROJECT_ID}:${BQ_DATASET}.skew_study" >/dev/null

echo "[STEP] Creating or replacing chart views"
bq query --quiet --use_legacy_sql=false "
CREATE OR REPLACE VIEW \
  \
\`${PROJECT_ID}.${BQ_DATASET}.vw_slo_trends\` AS
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
FROM \`${PROJECT_ID}.${BQ_DATASET}.slo_hourly\`;

CREATE OR REPLACE VIEW \`${PROJECT_ID}.${BQ_DATASET}.vw_anomaly_timeline\` AS
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
FROM \`${PROJECT_ID}.${BQ_DATASET}.anomalies\`;

CREATE OR REPLACE VIEW \`${PROJECT_ID}.${BQ_DATASET}.vw_deployment_impact\` AS
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
FROM \`${PROJECT_ID}.${BQ_DATASET}.change_impact_attribution\`;

CREATE OR REPLACE VIEW \`${PROJECT_ID}.${BQ_DATASET}.vw_top_offenders\` AS
SELECT
  service,
  endpoint,
  region,
  anomaly_windows,
  max_severity,
  peak_p95_ms,
  peak_error_rate,
  explanations
FROM \`${PROJECT_ID}.${BQ_DATASET}.top_offenders\`;

CREATE OR REPLACE VIEW \`${PROJECT_ID}.${BQ_DATASET}.vw_skew_benchmark\` AS
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
FROM \`${PROJECT_ID}.${BQ_DATASET}.skew_study\`;
"

echo "[STEP] Validation query"
bq query --use_legacy_sql=false \
  "SELECT service, endpoint, ROUND(AVG(p95_latency_ms), 2) AS avg_p95_ms
   FROM \`${PROJECT_ID}.${BQ_DATASET}.vw_slo_trends\`
   GROUP BY service, endpoint
   ORDER BY avg_p95_ms DESC
   LIMIT 20"

echo "[DONE] CI/CD refresh completed"
echo "[INFO] Views ready in BigQuery dataset: ${PROJECT_ID}.${BQ_DATASET}"
