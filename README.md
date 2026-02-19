# Distributed Log Analytics Using Apache Spark (Scala)

This project implements a distributed Spark pipeline for large-scale log analytics with:

- Session/trace reconstruction (window analytics)
- SLO/SLA metrics with p50/p95/p99 percentiles (shuffle + aggregation)
- Deployment change-impact attribution (time-constrained join)
- Rolling anomaly detection and top offenders
- Skew/straggler benchmark with salting mitigation and before/after runtime

## Project Structure

```text
.
├── build.sbt
├── project/
│   └── build.properties
├── scripts/
│   ├── run_local.sh
│   └── run_scale_study.sh
└── src/main/scala/com/loganalytics/
    ├── Main.scala
    ├── analytics/Analytics.scala
    ├── config/AppConfig.scala
    ├── data/SyntheticDataGenerator.scala
    └── io/DataLoader.scala
```

## Input Datasets

Expected schemas:

- `logs(timestamp, service, host, endpoint, status_code, latency_ms, user_id, trace_id)`
- `deployments(service, version, deploy_time)`
- `host_meta(host, region, instance_type)`

Defaults:

- `data/logs.parquet`
- `data/deployments.parquet`
- `data/host_meta.parquet`

## Build

```bash
sbt clean package
```

## Run (existing data)

```bash
spark-submit \
  --master local[*] \
  --class com.loganalytics.Main \
  target/scala-2.12/distributed-log-analytics-spark_2.12-0.1.0.jar \
  --logs data/logs.parquet \
  --deployments data/deployments.parquet \
  --host-meta data/host_meta.parquet \
  --output output \
  --input-format parquet \
  --session-timeout-minutes 30 \
  --attribution-window-hours 6 \
  --baseline-hours 24 \
  --salt-buckets 16 \
  --target-partitions 64
```

## Run (generate reproducible sample data)

```bash
spark-submit \
  --master local[*] \
  --class com.loganalytics.Main \
  target/scala-2.12/distributed-log-analytics-spark_2.12-0.1.0.jar \
  --logs data/logs.parquet \
  --deployments data/deployments.parquet \
  --host-meta data/host_meta.parquet \
  --output output \
  --input-format parquet \
  --generate-sample-data \
  --generated-rows 2000000
```

Synthetic generator is deterministic from fixed random seeds for reproducibility.

## Outputs

Pipeline writes parquet outputs under `--output`:

- `session_trace/`
- `user_sessions/`
- `slo/hourly/`
- `slo/daily/`
- `change_impact_attribution/`
- `anomalies/`
- `top_offenders/`
- `skew_study/`

## Scaling + Skew Study

Use:

```bash
scripts/run_scale_study.sh
```

This runs multiple data sizes and captures:

- top-endpoint skew share
- partition skew stats (max/min/avg partition rows) before and after salting
- runtime before and after salting
- improvement percentage

## Notes

- Spark dependencies are marked `provided` for cluster execution.
- For YARN/Kubernetes clusters, pass cluster-specific `--master`, deploy mode, and resource flags.
- Adjust `--target-partitions` based on executor cores and input size.
