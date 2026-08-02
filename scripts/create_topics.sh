#!/usr/bin/env bash
# Create the raw ingestion topics.
#
# Partition count is 6 rather than 2. Only two partitions receive data today
# (messages are keyed by symbol, and there are two symbols), but Kafka does not
# allow reducing partitions later and rebalancing keys across a changed
# partition count breaks per-key ordering. Six leaves room to add symbols
# without a migration. The resulting idle partitions are a deliberate, known
# skew -- see DECISIONS.md D-008.
set -euo pipefail

PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS="${RETENTION_MS:-604800000}"   # 7 days
CONTAINER="${CONTAINER:-xstream-redpanda}"

create() {
  local topic="$1"
  echo "creating topic ${topic} (partitions=${PARTITIONS})"
  docker exec "${CONTAINER}" rpk topic create "${topic}" \
    --partitions "${PARTITIONS}" \
    --replicas 1 \
    --topic-config retention.ms="${RETENTION_MS}" \
    --topic-config compression.type=lz4 \
    || echo "  (already exists)"
}

create trades.raw
create orderbook.raw
create divergence.events

echo
docker exec "${CONTAINER}" rpk topic list
