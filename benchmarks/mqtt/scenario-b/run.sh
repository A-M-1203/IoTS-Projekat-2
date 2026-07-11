#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

TS="$(timestamp_utc)"
ensure_results_dir "b" "mqtt"
RESULT_TXT="results/scenario-b/mqtt/outage_30s_${TS}.txt"
RESULT_STATS="results/scenario-b/mqtt/outage_30s_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-b/mqtt/outage_30s_${TS}_resources.json"
NETWORK="$(get_compose_network)"
OUTAGE_SECONDS=30

log_progress "=== Scenario B MQTT: network disconnect ${OUTAGE_SECONDS}s ==="

setup_stack mqtt 1
start_stats_monitor "$RESULT_STATS"

BENCH_CONTAINER="$(get_container_name emqtt-bench)"
STORAGE_CONTAINER="$(get_container_name data-storage)"

log_progress "Starting background low-rate load (50 devices)..."
emqtt_bench_exec -d pub \
  -h mosquitto -p 1883 -c 50 -q 1 -t "$MQTT_TOPIC" \
  -s "$BENCHMARK_PAYLOAD_SIZE" -n "$BENCHMARK_MESSAGES_PER_DEVICE" -I 1000 -m "$SENSOR_PAYLOAD" || true

log_progress "Baseline phase — waiting 30s before network outage..."
sleep 30
drain_storage_buffer
COUNT_BEFORE="$(get_received_count)"
COUNT_BEFORE="${COUNT_BEFORE:-0}"
RECEIVED_BEFORE="$(wait_for_storage_metric received 1 30)"
STORED_BEFORE="$(get_storage_metrics_stored)"
STORED_BEFORE="${STORED_BEFORE:-0}"
log_progress "Baseline counts before outage: db=$COUNT_BEFORE received=$RECEIVED_BEFORE stored=$STORED_BEFORE"

DISCONNECT_TS=$(date +%s)
log_progress "Disconnecting $BENCH_CONTAINER from $NETWORK..."
docker network disconnect "$NETWORK" "$BENCH_CONTAINER" || true
log_progress "Network outage in progress (${OUTAGE_SECONDS}s)..."
sleep "$OUTAGE_SECONDS"
CONNECT_TS=$(date +%s)
log_progress "Reconnecting $BENCH_CONTAINER to $NETWORK..."
docker network connect "$NETWORK" "$BENCH_CONTAINER" || true

log_progress "Restarting background producer after reconnect..."
emqtt_bench_exec -d pub \
  -h mosquitto -p 1883 -c 50 -q 1 -t "$MQTT_TOPIC" \
  -s "$BENCHMARK_PAYLOAD_SIZE" -n "$BENCHMARK_MESSAGES_PER_DEVICE" -I 1000 -m "$SENSOR_PAYLOAD" || true

RECOVERY_SECONDS=""
log_progress "Measuring recovery time (message flow resumed)..."
for ((i = 0; i < 120; i++)); do
  CURRENT_RECEIVED="$(get_storage_metrics_received)"
  CURRENT_RECEIVED="${CURRENT_RECEIVED:-0}"
  if [[ "$CURRENT_RECEIVED" =~ ^[0-9]+$ ]] && (( CURRENT_RECEIVED > RECEIVED_BEFORE )); then
    RECOVERY_SECONDS=$(( $(date +%s) - CONNECT_TS ))
    log_progress "Message flow resumed after ${RECOVERY_SECONDS}s (received ${RECEIVED_BEFORE} -> ${CURRENT_RECEIVED})"
    break
  fi
  if (( i > 0 && i % 15 == 0 )); then
    log_progress "Still waiting for recovery... received=$CURRENT_RECEIVED baseline=$RECEIVED_BEFORE (${i}s)"
  fi
  sleep 1
done
[[ -z "$RECOVERY_SECONDS" ]] && RECOVERY_SECONDS="120+"

log_progress "Collecting post-recovery metrics (20s)..."
sleep 20
drain_storage_buffer
COUNT_AFTER="$(get_received_count)"
COUNT_AFTER="${COUNT_AFTER:-0}"
RECEIVED_AFTER="$(get_storage_metrics_received)"
RECEIVED_AFTER="${RECEIVED_AFTER:-0}"
STORED_AFTER="$(get_storage_metrics_stored)"
STORED_AFTER="${STORED_AFTER:-0}"

DB_DELTA=$((COUNT_AFTER - COUNT_BEFORE))
METRICS_DELTA=$((RECEIVED_AFTER - RECEIVED_BEFORE))
STORED_DELTA=$((STORED_AFTER - STORED_BEFORE))
MESSAGES_DURING="$METRICS_DELTA"
if [[ "$STORED_DELTA" -gt "$MESSAGES_DURING" ]]; then
  MESSAGES_DURING="$STORED_DELTA"
fi
if [[ "$DB_DELTA" -gt "$MESSAGES_DURING" ]]; then
  MESSAGES_DURING="$DB_DELTA"
fi
[[ "$MESSAGES_DURING" -lt 0 ]] && MESSAGES_DURING=0
log_progress "Messages during outage/recovery window: $MESSAGES_DURING (db_delta=$DB_DELTA metrics_delta=$METRICS_DELTA stored_delta=$STORED_DELTA)"

stop_stats_monitor
aggregate_resources "$RESULT_STATS" "$RESULT_RESOURCES"

BROKER_CONTAINER="$(get_container_name mosquitto)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"
POSTGRES_CONTAINER="$(get_container_name postgres)"

log_progress "Writing results to $RESULT_TXT"
write_result_header "$RESULT_TXT"
append_result "$RESULT_TXT" "scenario" "B"
append_result "$RESULT_TXT" "broker" "mqtt"
append_result "$RESULT_TXT" "outage_seconds" "$OUTAGE_SECONDS"
append_result "$RESULT_TXT" "recovery_time_seconds" "$RECOVERY_SECONDS"
append_result "$RESULT_TXT" "messages_during_test" "$MESSAGES_DURING"
append_result "$RESULT_TXT" "resubscribe_observed" "message flow resumed via /metrics received"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "b" "mqtt" \
  "outage_s,recovery_s,messages,resubscribe_s,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net,note" \
  "${OUTAGE_SECONDS},${RECOVERY_SECONDS},${MESSAGES_DURING},${RECOVERY_SECONDS},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb),mqtt_disconnect_emqtt_bench"

log_progress "=== Scenario B MQTT done: recovery=${RECOVERY_SECONDS}s ==="
teardown_stack
