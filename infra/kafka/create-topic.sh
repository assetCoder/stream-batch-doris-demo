#!/usr/bin/env bash
# 创建 3 分区 topic；分区数决定 Flink 源的最大并行度。
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPIC="${KAFKA_TOPIC:-order-events}"
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 \
  --replication-factor 1

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe --topic "$TOPIC"
