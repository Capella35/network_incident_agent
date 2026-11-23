#!/bin/bash
# Create the milestone-1 topics (idempotent) and enforce bounded retention (guardrail R2).
#
# Retention deletes whole closed segments, so segment.bytes is kept well below
# retention.bytes; otherwise the 1 GiB default segment would never roll and never be deleted.
# Retention by time uses the record timestamp: the replay producer must leave it at send time
# (CreateTime = now), not the 2013 interval time, or records would expire at once.
set -euo pipefail

BIN=/opt/kafka/bin
: "${BOOTSTRAP:?}"

# name | partitions | retention.ms | retention.bytes (per partition) | segment.bytes
TOPICS=(
  "telemetry.raw|6|604800000|268435456|67108864"   # 7 days, 256 MiB/partition, 64 MiB segments
  "telemetry.dlq|1|604800000|67108864|16777216"    # 7 days,  64 MiB,           16 MiB segments
)

for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name partitions ret_ms ret_bytes seg_bytes <<<"$spec"
  configs="retention.ms=$ret_ms,retention.bytes=$ret_bytes,segment.bytes=$seg_bytes,cleanup.policy=delete"
  config_args=()
  IFS=',' read -ra pairs <<<"$configs"
  for pair in "${pairs[@]}"; do config_args+=(--config "$pair"); done

  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    "${config_args[@]}" >/dev/null
  # Re-apply configs so an existing topic picks up changed limits.
  "$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --alter \
    --entity-type topics --entity-name "$name" --add-config "$configs" >/dev/null

  actual=$("$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$name" \
    | sed -n 's/.*PartitionCount: *\([0-9]*\).*/\1/p' | head -n 1)
  if [ "$actual" != "$partitions" ]; then
    echo "kafka-init: $name has $actual partitions, expected $partitions (delete the topic to change it)" >&2
    exit 1
  fi
  echo "kafka-init: $name ok ($partitions partitions; $configs)"
done
