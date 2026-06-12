#!/bin/sh
set -eu

sh scripts/setup.sh
docker compose run --rm pipeline
sh scripts/postgres.sh

docker compose up -d spark-streaming-validator postgres-transaction-updater
docker compose run --rm transaction-producer
sh scripts/publish.sh
