#!/usr/bin/env bash
# 在 Spark 4.1 集群提交；connector 由 --packages 拉取，避免手工找 jar。
set -euo pipefail
cd "$(dirname "$0")"

CONNECTOR="${SPARK_DORIS_CONNECTOR:-org.apache.doris:spark-doris-connector-spark-4.1:26.1.0}"

exec spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --packages "$CONNECTOR" \
  spark_ads_job.py "$@"
