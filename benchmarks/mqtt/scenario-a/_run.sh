#!/usr/bin/env bash
set -euo pipefail

CLIENTS="${1:?clients required}"
QOS="${2:?qos required}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

CONFIG="devices_${CLIENTS}_qos${QOS}"
TS="$(timestamp_utc)"
ensure_results_dir "a" "mqtt"
RESULT_TXT="results/scenario-a/mqtt/${CONFIG}_${TS}.txt"
RESULT_STATS="results/scenario-a/mqtt/${CONFIG}_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-a/mqtt/${CONFIG}_${TS}_resources.json"

MESSAGES_PER_DEVICE="${BENCHMARK_MESSAGES_PER_DEVICE}"
SENT=$((CLIENTS * MESSAGES_PER_DEVICE))

echo "=== Scenario A MQTT: clients=$CLIENTS qos=$QOS sent=$SENT ==="

setup_stack mqtt 500
start_stats_monitor "$RESULT_STATS"
START_TS=$(date +%s)

BENCH_OUTPUT=$(docker compose --profile mqtt exec -T emqtt-bench emqtt_bench pub \
  -h mosquitto -p 1883 \
  -c "$CLIENTS" -q "$QOS" \
  -t "$MQTT_TOPIC" \
  -s "$BENCHMARK_PAYLOAD_SIZE" \
  -n "$MESSAGES_PER_DEVICE" \
  -m "$SENSOR_PAYLOAD" 2>&1) || true

echo "$BENCH_OUTPUT"

wait_for_drain 180
END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))

RECEIVED_DB="$(get_received_count)"
RECEIVED_DB="${RECEIVED_DB:-0}"
RECEIVED_METRICS="$(get_storage_metrics_stored)"
if [[ "$RECEIVED_METRICS" -gt "$RECEIVED_DB" ]]; then
  RECEIVED="$RECEIVED_METRICS"
else
  RECEIVED="$RECEIVED_DB"
fi

LOSS="$(calc_loss_pct "$SENT" "$RECEIVED")"

stop_stats_monitor
aggregate_resources "$RESULT_STATS" "$RESULT_RESOURCES"

BROKER_CONTAINER="$(get_container_name mosquitto)"
STORAGE_CONTAINER="$(get_container_name data-storage)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"
POSTGRES_CONTAINER="$(get_container_name postgres)"

write_result_header "$RESULT_TXT"
append_result "$RESULT_TXT" "scenario" "A"
append_result "$RESULT_TXT" "broker" "mqtt"
append_result "$RESULT_TXT" "clients" "$CLIENTS"
append_result "$RESULT_TXT" "qos" "$QOS"
append_result "$RESULT_TXT" "sent" "$SENT"
append_result "$RESULT_TXT" "received" "$RECEIVED"
append_result "$RESULT_TXT" "lost_percent" "$LOSS"
append_result "$RESULT_TXT" "duration_seconds" "$DURATION"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "a" "mqtt" \
  "devices,qos,sent,received,lost_percent,duration_s,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net" \
  "${CLIENTS},${QOS},${SENT},${RECEIVED},${LOSS},${DURATION},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb)"

echo "=== Done: sent=$SENT received=$RECEIVED lost=${LOSS}% duration=${DURATION}s ==="
teardown_stack
