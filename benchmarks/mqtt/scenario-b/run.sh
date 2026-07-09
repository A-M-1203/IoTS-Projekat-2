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

echo "=== Scenario B MQTT: network disconnect 30s ==="

setup_stack mqtt 1
start_stats_monitor "$RESULT_STATS"

BENCH_CONTAINER="$(get_container_name emqtt-bench)"
STORAGE_CONTAINER="$(get_container_name data-storage)"

# Start background low-rate load
docker compose --profile mqtt exec -d emqtt-bench emqtt_bench pub \
  -h mosquitto -p 1883 -c 50 -q 1 -t "$MQTT_TOPIC" \
  -s "$BENCHMARK_PAYLOAD_SIZE" -n 1000 -I 1000 -m "$SENSOR_PAYLOAD" || true

sleep 30
COUNT_BEFORE="$(get_received_count)"
COUNT_BEFORE="${COUNT_BEFORE:-0}"

DISCONNECT_TS=$(date +%s)
docker network disconnect "$NETWORK" "$BENCH_CONTAINER" || true
echo "Disconnected $BENCH_CONTAINER from $NETWORK at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
sleep "$OUTAGE_SECONDS"
CONNECT_TS=$(date +%s)
docker network connect "$NETWORK" "$BENCH_CONTAINER" || true
echo "Reconnected $BENCH_CONTAINER at $(date -u +%Y-%m-%dT%H:%M:%SZ)"

RECOVERY_START=$(date +%s)
RECOVERY_SECONDS=""
for ((i = 0; i < 120; i++)); do
  if docker compose --profile mqtt logs --since 2m data-storage 2>/dev/null | grep -q "SUBSCRIBED at"; then
    RECOVERY_SECONDS=$(( $(date +%s) - CONNECT_TS ))
    break
  fi
  sleep 1
done
[[ -z "$RECOVERY_SECONDS" ]] && RECOVERY_SECONDS="120+"

sleep 20
COUNT_AFTER="$(get_received_count)"
COUNT_AFTER="${COUNT_AFTER:-0}"
MESSAGES_DURING=$((COUNT_AFTER - COUNT_BEFORE))

stop_stats_monitor
aggregate_resources "$RESULT_STATS" "$RESULT_RESOURCES"

BROKER_CONTAINER="$(get_container_name mosquitto)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"
POSTGRES_CONTAINER="$(get_container_name postgres)"

write_result_header "$RESULT_TXT"
append_result "$RESULT_TXT" "scenario" "B"
append_result "$RESULT_TXT" "broker" "mqtt"
append_result "$RESULT_TXT" "outage_seconds" "$OUTAGE_SECONDS"
append_result "$RESULT_TXT" "recovery_time_seconds" "$RECOVERY_SECONDS"
append_result "$RESULT_TXT" "messages_during_test" "$MESSAGES_DURING"
append_result "$RESULT_TXT" "resubscribe_observed" "storage SUBSCRIBED log"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "b" "mqtt" \
  "outage_s,recovery_s,messages,resubscribe_s,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net,note" \
  "${OUTAGE_SECONDS},${RECOVERY_SECONDS},${MESSAGES_DURING},${RECOVERY_SECONDS},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb),mqtt_disconnect_emqtt_bench"

echo "=== Scenario B MQTT done: recovery=${RECOVERY_SECONDS}s ==="
teardown_stack
