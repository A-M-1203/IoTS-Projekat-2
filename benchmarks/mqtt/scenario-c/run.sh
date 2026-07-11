#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

TS="$(timestamp_utc)"
ensure_results_dir "c" "mqtt"
RESULT_TXT="results/scenario-c/mqtt/burst_${TS}.txt"
RESULT_STATS="results/scenario-c/mqtt/burst_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-c/mqtt/burst_${TS}_resources.json"

BASELINE_DEVICES=50
BURST_DEVICES=200
BURST_SECONDS=10
POST_BURST_MONITOR=30
PEAK_BACKLOG=0
PIPELINE_BASELINE=0

track_mqtt_backlog() {
  local backlog
  backlog="$(mqtt_pipeline_backlog "$PIPELINE_BASELINE")"
  [[ "$backlog" -gt "$PEAK_BACKLOG" ]] && PEAK_BACKLOG="$backlog"
  echo "$backlog"
}

log_progress "=== Scenario C MQTT: burst ${BASELINE_DEVICES} -> ${BURST_DEVICES} devices (${BENCHMARK_MESSAGES_PER_DEVICE} msg each) ==="

setup_stack mqtt 500
start_stats_monitor "$RESULT_STATS"

log_progress "Phase 1/3: Baseline load (${BASELINE_DEVICES} devices)..."
emqtt_bench_exec pub \
  -h mosquitto -p 1883 -c "$BASELINE_DEVICES" -q 1 -t "$MQTT_TOPIC" \
  -s "$BENCHMARK_PAYLOAD_SIZE" -L "$BENCHMARK_MESSAGES_PER_DEVICE" \
  -I 1000 -m "$SENSOR_PAYLOAD" || true

for ((i = 0; i < 30; i++)); do
  Q="$(track_mqtt_backlog)"
  if (( i > 0 && i % 10 == 0 )); then
    log_progress "Baseline monitoring... backlog=$Q (${i}/30s)"
  fi
  sleep 1
done

log_progress "Draining baseline pipeline before burst..."
for ((i = 0; i < 120; i++)); do
  drain_storage_buffer
  Q="$(mqtt_pipeline_backlog 0)"
  if [[ "$Q" -le 5 ]]; then
    log_progress "Baseline pipeline drained (lag=$Q)"
    break
  fi
  sleep 1
done

METRICS_RECEIVED_BASELINE="$(get_storage_metrics_received)"
METRICS_STORED_BASELINE="$(get_storage_metrics_stored)"
PIPELINE_BASELINE=$((METRICS_RECEIVED_BASELINE - METRICS_STORED_BASELINE))
[[ "$PIPELINE_BASELINE" -lt 0 ]] && PIPELINE_BASELINE=0
PEAK_BACKLOG=0
log_progress "Pipeline baseline: received=$METRICS_RECEIVED_BASELINE stored=$METRICS_STORED_BASELINE lag=$PIPELINE_BASELINE"

log_progress "Phase 2/3: Burst load (${BURST_DEVICES} devices)..."
BURST_START=$(date +%s)
emqtt_bench_exec pub \
  -h mosquitto -p 1883 -c "$BURST_DEVICES" -q 0 -t "$MQTT_TOPIC" \
  -s "$BENCHMARK_PAYLOAD_SIZE" -L "$BENCHMARK_MESSAGES_PER_DEVICE" \
  -I 1 -m "$SENSOR_PAYLOAD" || true

DEADLINE=$((BURST_START + BURST_SECONDS + POST_BURST_MONITOR))
while (( $(date +%s) < DEADLINE )); do
  Q="$(track_mqtt_backlog)"
  ELAPSED=$(( $(date +%s) - BURST_START ))
  if (( ELAPSED <= BURST_SECONDS )); then
    log_progress "Burst monitoring... backlog=$Q (${ELAPSED}/${BURST_SECONDS}s)"
  elif (( ELAPSED > BURST_SECONDS && (ELAPSED - BURST_SECONDS) % 10 == 0 )); then
    log_progress "Post-burst monitoring... backlog=$Q ($((ELAPSED - BURST_SECONDS))s after burst)"
  fi
  sleep 1
done
BURST_END=$(date +%s)

log_progress "Phase 3/3: Waiting for recovery (pipeline drain)..."
RECOVERY_SECONDS=""
STABLE=0
for ((i = 0; i < 180; i++)); do
  drain_storage_buffer
  Q="$(track_mqtt_backlog)"
  if [[ "$Q" -le 5 ]]; then
    STABLE=$((STABLE + 1))
    [[ "$STABLE" -ge 10 ]] && RECOVERY_SECONDS=$((i + 1)) && break
  else
    STABLE=0
  fi
  if (( i > 0 && i % 30 == 0 )); then
    RECEIVED_NOW="$(get_storage_metrics_received)"
    STORED_NOW="$(get_storage_metrics_stored)"
    log_progress "Recovery in progress... backlog=$Q received=$RECEIVED_NOW stored=$STORED_NOW (${i}s elapsed)"
  fi
  sleep 1
done
[[ -z "$RECOVERY_SECONDS" ]] && RECOVERY_SECONDS="180+"

wait_for_drain 120

stop_stats_monitor
aggregate_resources "$RESULT_STATS" "$RESULT_RESOURCES"

BROKER_CONTAINER="$(get_container_name mosquitto)"
STORAGE_CONTAINER="$(get_container_name data-storage)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"
POSTGRES_CONTAINER="$(get_container_name postgres)"

write_result_header "$RESULT_TXT"
append_result "$RESULT_TXT" "scenario" "C"
append_result "$RESULT_TXT" "broker" "mqtt"
append_result "$RESULT_TXT" "baseline_devices" "$BASELINE_DEVICES"
append_result "$RESULT_TXT" "burst_devices" "$BURST_DEVICES"
append_result "$RESULT_TXT" "burst_duration_s" "$BURST_SECONDS"
append_result "$RESULT_TXT" "peak_backlog" "$PEAK_BACKLOG"
append_result "$RESULT_TXT" "recovery_time_seconds" "$RECOVERY_SECONDS"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "c" "mqtt" \
  "baseline,burst,peak_backlog,recovery_s,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net" \
  "${BASELINE_DEVICES},${BURST_DEVICES},${PEAK_BACKLOG},${RECOVERY_SECONDS},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb)"

log_progress "=== Scenario C MQTT done: peak_backlog=$PEAK_BACKLOG recovery=${RECOVERY_SECONDS}s ==="
teardown_stack
