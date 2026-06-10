#!/bin/sh
set -eu

docker compose run --rm pipeline
docker compose --profile tools run --rm minio-client
