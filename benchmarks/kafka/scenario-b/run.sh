#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

TS="$(timestamp_utc)"
ensure_results_dir "b" "kafka"
RESULT_TXT="results/scenario-b/kafka/outage_30s_${TS}.txt"
RESULT_STATS="results/scenario-b/kafka/outage_30s_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-b/kafka/outage_30s_${TS}_resources.json"
PAYLOAD_FILE="$SCRIPT_DIR/../../common/payloads/sensor.json"
NETWORK="$(get_compose_network)"
OUTAGE_SECONDS=30
DEVICES=50

log_progress "=== Scenario B Kafka: network disconnect ${OUTAGE_SECONDS}s ==="

setup_stack kafka 1
log_progress "Copying benchmark payload to Kafka container..."
kafka_copy_payload "$PAYLOAD_FILE"

start_stats_monitor "$RESULT_STATS"

log_progress "Starting background low-rate producer..."
kafka_exec -d bash -c "
while true; do
  /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic $KAFKA_TOPIC \
    --num-records $((DEVICES * BENCHMARK_MESSAGES_PER_DEVICE)) \
    --throughput $DEVICES \
    --payload-file /tmp/bench_payload.json \
    --producer-props acks=1 bootstrap.servers=localhost:9092 \
    --num-threads $DEVICES >/dev/null 2>&1
  sleep 1
done
" || true

log_progress "Baseline phase — waiting 20s before network outage..."
sleep 20

OFFSET_BEFORE="0"
for ((i = 0; i < 30; i++)); do
  OFFSET_BEFORE="$(get_kafka_consumer_offset data-storage-group || echo 0)"
  if [[ "${OFFSET_BEFORE:-0}" =~ ^[0-9]+$ ]] && (( OFFSET_BEFORE > 0 )); then
    break
  fi
  sleep 1
done
log_progress "Consumer offset before outage: $OFFSET_BEFORE"

BENCH_CONTAINER="$(get_container_name kafka)"
log_progress "Disconnecting $BENCH_CONTAINER from $NETWORK..."
docker network disconnect "$NETWORK" "$BENCH_CONTAINER" || true
log_progress "Network outage in progress (${OUTAGE_SECONDS}s)..."
sleep "$OUTAGE_SECONDS"
log_progress "Reconnecting $BENCH_CONTAINER to $NETWORK..."
docker network connect "$NETWORK" "$BENCH_CONTAINER" || true

sleep 2
LAG_AFTER="$(get_kafka_consumer_lag data-storage-group)"
log_progress "Consumer lag immediately after reconnect: $LAG_AFTER"

RECOVERY_START=$(date +%s)
RECOVERY_SECONDS=""
log_progress "Measuring recovery time (consumer lag)..."
for ((i = 0; i < 120; i++)); do
  LAG="$(get_kafka_consumer_lag data-storage-group)"
  if [[ "${LAG:-999}" -le 5 ]]; then
    RECOVERY_SECONDS=$(( $(date +%s) - RECOVERY_START ))
    break
  fi
  if (( i > 0 && i % 15 == 0 )); then
    log_progress "Still waiting for recovery... lag=$LAG (${i}s)"
  fi
  sleep 1
done
[[ -z "$RECOVERY_SECONDS" ]] && RECOVERY_SECONDS="120+"

log_progress "Collecting post-recovery offset (10s)..."
sleep 10
OFFSET_AFTER="$(get_kafka_consumer_offset data-storage-group)"
log_progress "Consumer offset after recovery: $OFFSET_AFTER"

stop_stats_monitor
aggregate_resources "$RESULT_STATS" "$RESULT_RESOURCES"

BROKER_CONTAINER="$(get_container_name kafka)"
STORAGE_CONTAINER="$(get_container_name data-storage)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"
POSTGRES_CONTAINER="$(get_container_name postgres)"

log_progress "Writing results to $RESULT_TXT"
write_result_header "$RESULT_TXT"
append_result "$RESULT_TXT" "scenario" "B"
append_result "$RESULT_TXT" "broker" "kafka"
append_result "$RESULT_TXT" "outage_seconds" "$OUTAGE_SECONDS"
append_result "$RESULT_TXT" "recovery_time_seconds" "$RECOVERY_SECONDS"
append_result "$RESULT_TXT" "offset_before" "$OFFSET_BEFORE"
append_result "$RESULT_TXT" "offset_after" "$OFFSET_AFTER"
append_result "$RESULT_TXT" "lag_after_reconnect" "$LAG_AFTER"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "b" "kafka" \
  "outage_s,recovery_s,offset_before,offset_after,lag_after,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net,note" \
  "${OUTAGE_SECONDS},${RECOVERY_SECONDS},${OFFSET_BEFORE},${OFFSET_AFTER},${LAG_AFTER},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb),kafka_disconnect_broker"

log_progress "=== Scenario B Kafka done: recovery=${RECOVERY_SECONDS}s offset ${OFFSET_BEFORE}->${OFFSET_AFTER} ==="
teardown_stack
