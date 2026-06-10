#!/bin/sh
set -eu

docker compose up -d kafka spark-master spark-worker
docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec -T spark-master spark-submit --version
