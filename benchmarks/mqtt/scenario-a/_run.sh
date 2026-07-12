#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

usage() {
  echo "Usage: $0 <clients> <qos>" >&2
  echo "  clients: number of MQTT clients (100, 1000, or 10000)" >&2
  echo "  qos:     0, 1, or 2" >&2
  echo "" >&2
  echo "Examples:" >&2
  echo "  bash $0 100 1" >&2
  echo "  bash run_100_qos1.sh" >&2
  exit 1
}

if [[ $# -lt 2 ]]; then
  usage
fi

CLIENTS="$1"
QOS="$2"

CONFIG="devices_${CLIENTS}_qos${QOS}"
TS="$(timestamp_utc)"
ensure_results_dir "a" "mqtt"
RESULT_TXT="results/scenario-a/mqtt/${CONFIG}_${TS}.txt"
RESULT_STATS="results/scenario-a/mqtt/${CONFIG}_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-a/mqtt/${CONFIG}_${TS}_resources.json"

MESSAGES_PER_DEVICE="${BENCHMARK_MESSAGES_PER_DEVICE}"
SENT=$((CLIENTS * MESSAGES_PER_DEVICE))

PAYLOAD_FILE="$SCRIPT_DIR/../../common/payloads/sensor.json"

log_progress "=== Scenario A MQTT: clients=$CLIENTS qos=$QOS sent=$SENT ==="

setup_stack mqtt 500
wait_for_storage_mqtt_subscribed 30
start_stats_monitor "$RESULT_STATS"
START_TS=$(date +%s)

log_progress "Copying benchmark payload to mosquitto container..."
mqtt_copy_payload "$PAYLOAD_FILE"

log_progress "Publishing $SENT messages via mosquitto_pub ($CLIENTS clients x $MESSAGES_PER_DEVICE msg)..."
START_PUBLISH=$(date +%s)
mqtt_parallel_publish "$CLIENTS" "$QOS" "$MESSAGES_PER_DEVICE" "$MQTT_TOPIC" 200
PUBLISH_SECONDS=$(( $(date +%s) - START_PUBLISH ))
log_progress "Publish finished in ${PUBLISH_SECONDS}s"

METRICS_RECEIVED="$(get_storage_metrics_received)"
METRICS_STORED="$(get_storage_metrics_stored)"
METRICS_ERRORS="$(get_storage_metrics_field errors)"
log_progress "Storage after publish: received=$METRICS_RECEIVED stored=$METRICS_STORED errors=$METRICS_ERRORS"

log_progress "Calculating received count and loss..."
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

log_progress "Writing results to $RESULT_TXT"
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

log_progress "=== Done: sent=$SENT received=$RECEIVED lost=${LOSS}% duration=${DURATION}s ==="
teardown_stack
