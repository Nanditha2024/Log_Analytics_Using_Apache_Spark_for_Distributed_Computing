# Distributed Log Analytics Using Apache Spark

## 1. Project Overview

This project is a distributed log analytics pipeline built with Apache Spark and Scala. It processes application logs together with deployment metadata and host metadata to produce operational analytics that help explain how a system behaves at scale.

The pipeline is designed to demonstrate the use of distributed computing for:

- session and trace reconstruction,
- service level objective (SLO) reporting,
- deployment change-impact attribution,
- anomaly detection,
- and skew benchmarking for Spark performance tuning.

The application reads structured input data, enriches it with host metadata, computes multiple analytics products, prints summaries to the console, and writes parquet outputs that can be used locally or registered in BigQuery for visualization.

## 2. What Problem This Project Solves

In real systems, raw logs are large, noisy, and difficult to interpret manually. This project transforms those logs into structured outputs so that a reviewer can answer questions such as:

- Which user sessions or traces were created from the raw event stream?
- How do latency and error rates change across services and endpoints?
- Did a deployment affect request volume, latency, or error rates in the following hours?
- Which service and endpoint combinations are anomalous compared with recent history?
- Where is Spark skew likely to slow down distributed aggregation?

The goal is not just to store logs, but to convert them into analytics that are easier to inspect, validate, and present in a report.

## 3. Dataset And Input Files

The project expects three parquet inputs by default under the `data/` directory:

- `data/logs.parquet`
- `data/deployments.parquet`
- `data/host_meta.parquet`

These files are loaded by the main application and used as follows:

- `logs.parquet` contains the main request log stream.
- `deployments.parquet` contains deployment timestamps and versions.
- `host_meta.parquet` contains host-level enrichment data such as region and instance type.

The loader also supports CSV input if you want to provide alternate files and pass `--input-format csv`.

### 3.1 logs.parquet

Expected fields used by the code:

- `timestamp`
- `service`
- `host`
- `endpoint`
- `status_code`
- `latency_ms`
- `user_id`
- `trace_id`

### 3.2 deployments.parquet

Expected fields used by the code:

- `service`
- `version`
- `deploy_time`

### 3.3 host_meta.parquet

Expected fields used by the code:

- `host`
- `region`
- `instance_type`

## 4. Processing Pipeline

The entry point is [`src/main/scala/com/loganalytics/Main.scala`](src/main/scala/com/loganalytics/Main.scala). It orchestrates the full pipeline in this order:

1. Load the input parquet files.
2. Enrich logs with host metadata.
3. Reconstruct trace flows and user sessions.
4. Compute hourly and daily SLO metrics.
5. Attribute log behavior to the nearest deployment window.
6. Detect anomalies in error rate and latency.
7. Run a skew benchmark to compare partitioning strategies.

The core analytics logic is implemented in [`src/main/scala/com/loganalytics/analytics/Analytics.scala`](src/main/scala/com/loganalytics/analytics/Analytics.scala), and input parsing is handled by [`src/main/scala/com/loganalytics/data/DataLoader.scala`](src/main/scala/com/loganalytics/data/DataLoader.scala).

### 4.1 Session And Trace Reconstruction

The application groups log records by `trace_id` to build trace summaries and groups by `user_id` with a configurable inactivity timeout to build user sessions. The result includes request flows, start and end timestamps, event counts, and latency summaries.

### 4.2 SLO Metrics

The application computes hourly and daily aggregations per service, endpoint, and region. Each summary includes request count, average latency, error rate, and percentile latency values.

### 4.3 Change-Impact Attribution

Deployment records are matched against hourly log summaries so that the pipeline can associate observed changes in request behavior with recent deployments.

### 4.4 Anomaly Detection

The anomaly detector compares each hourly bucket with a rolling baseline and flags large deviations in error rate or p95 latency. It also generates a top-offenders summary.

### 4.5 Skew Benchmark

The skew study compares partition distribution and runtime before and after salting the busiest endpoint key. This helps explain why skewed data can slow down Spark jobs.

## 5. Outputs Produced

All outputs are written as parquet files under the configured output directory. By default this is `output/`.

The pipeline creates these subdirectories:

- `output/session_trace`
- `output/user_sessions`
- `output/slo/hourly`
- `output/slo/daily`
- `output/change_impact_attribution`
- `output/anomalies`
- `output/top_offenders`
- `output/skew_study`

The application also prints preview tables to the console so that you can verify the results immediately after a run.

## 6. Technology Stack

- Scala 2.12.18
- Apache Spark 3.5.1
- SBT build tool
- Parquet input and output
- Spark SQL and DataFrame APIs

## 7. Prerequisites

Before running the project, make sure the following are available:

- Java installed and available on your PATH
- SBT installed
- A local Spark-compatible environment or a Dataproc cluster
- The input parquet files in the `data/` directory, or alternate paths passed through command-line arguments

Java 11 is the safest choice if you encounter runtime compatibility issues. Java 17 also works in many Spark 3.5 setups, but if Spark fails to start, switch to Java 11 first.

## 8. How To Run The Project Locally

This is the easiest way to verify the project from a GitHub clone.

### Step 1: Clone the repository

```bash
git clone https://github.com/Nanditha2024/Log_Analytics_Using_Apache_Spark_for_Distributed_Computing.git
cd Log_Analytics_Using_Apache_Spark_for_Distributed_Computing
```

### Step 2: Check the input data

Make sure these files exist:

- `data/logs.parquet`
- `data/deployments.parquet`
- `data/host_meta.parquet`

### Step 3: Build the application JAR

```bash
sbt clean package
```

This produces a JAR under `target/scala-2.12/`.

### Step 4: Run the local pipeline

This can run the helper script:

```bash
./scripts/run_local.sh
```

or run Spark manually:

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

After the run finishes, inspect the `output/` directory for parquet results and review the console summaries.

## 9. How To Run On Dataproc 

For larger datasets, the project is designed to run on a Spark cluster such as Google Cloud Dataproc.

### Step 1: Build the JAR locally

```bash
sbt clean package
```

### Step 2: Upload the JAR and data to cloud storage

Copy the build artifact and the input parquet files to a bucket that your cluster can access.

### Step 3: Submit the Spark job

Use the same application entry point, but point the input and output paths to cloud storage locations.

### Step 4: Register the outputs in BigQuery if needed

The repository includes helper scripts that create BigQuery external tables and chart-ready views over the parquet outputs:

- [`scripts/run_cicd_dataproc_bigquery.sh`](scripts/run_cicd_dataproc_bigquery.sh)
- [`scripts/setup_bigquery_visualization.sh`](scripts/setup_bigquery_visualization.sh)

These scripts are optional, but they are useful if we want to publish the outputs for dashboards or report screenshots.

## 10. Configuration Reference

The application accepts these command-line options:

| Option | Default | Description |
| --- | --- | --- |
| `--logs` | `data/logs.parquet` | Path to the main logs input |
| `--deployments` | `data/deployments.parquet` | Path to deployment metadata |
| `--host-meta` | `data/host_meta.parquet` | Path to host enrichment data |
| `--output` | `output` | Output directory for parquet results |
| `--input-format` | `parquet` | Input format, such as `parquet` or `csv` |
| `--session-timeout-minutes` | `30` | Inactivity threshold for user sessionization |
| `--attribution-window-hours` | `6` | Attribution window after a deployment |
| `--baseline-hours` | `24` | Baseline window for anomaly detection |
| `--salt-buckets` | `16` | Number of salt buckets for skew mitigation |
| `--target-partitions` | `64` | Spark partition count used in the job |

## 11. Project Structure

```text
.
├── build.sbt
├── README.md
├── data/
│   ├── deployments.parquet
│   ├── host_meta.parquet
│   └── logs.parquet
├── output/
├── scripts/
│   ├── run_cicd_dataproc_bigquery.sh
│   ├── run_local.sh
│   ├── run_scale_study.sh
│   ├── setup_bigquery_visualization.sh
│   ├── trim_parquet_files.py
│   └── trim_parquets.sh
└── src/main/scala/com/loganalytics/
    ├── Main.scala
    ├── analytics/Analytics.scala
    ├── config/AppConfig.scala
    └── data/DataLoader.scala
```

## 12. Troubleshooting

### Spark fails to start

If Spark fails with Java compatibility errors, switch to Java 11 and try again.

### A master URL error appears

If you run the job without `spark-submit`, Spark may complain that a master URL is missing. Use the provided local script or run Spark with `--master local[*]`.

### Input file not found

Make sure the parquet files exist at the paths passed to `--logs`, `--deployments`, and `--host-meta`.

### Output directory already exists

The job writes outputs in overwrite mode, so an existing output directory should not block the run. If you are reusing external tools, make sure they point to the latest parquet output.

## 13. Quick Start

To test quickly:

```bash
git clone https://github.com/Nanditha2024/Log_Analytics_Using_Apache_Spark_for_Distributed_Computing.git
cd Log_Analytics_Using_Apache_Spark_for_Distributed_Computing
sbt clean package
./scripts/run_local.sh
```

This will run the end-to-end log analytics pipeline and generate the parquet outputs under `output/`.

