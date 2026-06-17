#!/bin/sh
set -eu

# Trên Git Bash (Windows) phải tắt path-conversion để các đường dẫn /opt, /sql truyền
# vào docker exec không bị đổi thành C:\...; trên Linux/WSL biến này vô hại.
export MSYS_NO_PATHCONV=1

docker compose up -d kafka kafka-ui spark-master spark-worker jupyter postgres pgadmin minio streamlit

until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 2
done

create_topic() {
  topic_name="$1"
  partition_count="$2"
  replication_factor="$3"
  retention_ms="$4"

  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$topic_name" \
    --partitions "$partition_count" \
    --replication-factor "$replication_factor" \
    --config "retention.ms=$retention_ms"
}

create_topic "raw-transactions" 3 1 604800000
create_topic "clean-transactions" 3 1 604800000
create_topic "error-transactions" 3 1 2592000000

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
docker compose exec -T spark-master /opt/spark/bin/spark-submit --version
