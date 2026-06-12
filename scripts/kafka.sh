#!/bin/sh
set -eu

docker compose up -d kafka kafka-ui

produce_message() {
  topic_name="$1"
  message="$2"
  printf '%s\n' "$message" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic_name"
}

produce_message "raw-transactions" '{"transaction_id":"TXN_SAMPLE_0001","customer_id":"CUST_000001","account_id":"ACC_000001","amount_vnd":1250000,"transaction_type":"PAYMENT","channel":"VPBANK_NEO","currency":"VND","status":"SUCCESS","transaction_time":"2026-06-10T09:00:00"}'
produce_message "raw-transactions" '{"transaction_id":"TXN_SAMPLE_0002","customer_id":"CUST_000002","account_id":"ACC_000002","amount_vnd":550000,"transaction_type":"TRANSFER","channel":"NAPAS_QR","currency":"VND","status":"SUCCESS","transaction_time":"2026-06-10T09:05:00"}'
produce_message "error-transactions" '{"transaction_id":"TXN_SAMPLE_BAD_0001","source_table":"transactions","error_type":"INVALID_AMOUNT","failed_column":"amount_vnd","event_time":"2026-06-10T09:15:00"}'

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
