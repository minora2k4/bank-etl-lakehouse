#!/bin/sh
set -eu

docker compose run --rm pipeline
sh scripts/publish.sh
