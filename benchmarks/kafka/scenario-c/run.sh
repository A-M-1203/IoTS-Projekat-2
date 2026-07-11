#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

TS="$(timestamp_utc)"
ensure_results_dir "c" "kafka"
RESULT_TXT="results/scenario-c/kafka/burst_${TS}.txt"
RESULT_STATS="results/scenario-c/kafka/burst_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-c/kafka/burst_${TS}_resources.json"
PAYLOAD_FILE="$SCRIPT_DIR/../../common/payloads/sensor.json"

BASELINE_DEVICES=50
BURST_DEVICES=200
BURST_SECONDS=10
PEAK_LAG=0
STABLE=0

log_progress "=== Scenario C Kafka: burst ${BASELINE_DEVICES} -> ${BURST_DEVICES} devices (1 msg each) ==="

setup_stack kafka 500
log_progress "Copying benchmark payload to Kafka container..."
kafka_copy_payload "$PAYLOAD_FILE"
start_stats_monitor "$RESULT_STATS"

log_progress "Phase 1/3: Baseline load (${BASELINE_DEVICES} devices)..."
kafka_producer_perf \
  --topic "$KAFKA_TOPIC" --num-records $((BASELINE_DEVICES * BENCHMARK_MESSAGES_PER_DEVICE)) \
  --throughput "$BASELINE_DEVICES" --payload-file /tmp/bench_payload.json \
  --producer-props acks=1 bootstrap.servers=localhost:9092 --num-threads "$BASELINE_DEVICES" || true

for ((i = 0; i < 30; i++)); do
  LAG="$(get_kafka_consumer_lag data-storage-group)"
  [[ "$LAG" -gt "$PEAK_LAG" ]] && PEAK_LAG="$LAG"
  if (( i > 0 && i % 10 == 0 )); then
    log_progress "Baseline monitoring... lag=$LAG (${i}/30s)"
  fi
  sleep 1
done

log_progress "Phase 2/3: Burst load (${BURST_DEVICES} devices)..."
kafka_producer_perf \
  --topic "$KAFKA_TOPIC" --num-records $((BURST_DEVICES * BENCHMARK_MESSAGES_PER_DEVICE)) \
  --throughput "$BURST_DEVICES" \
  --payload-file /tmp/bench_payload.json \
  --producer-props acks=1 bootstrap.servers=localhost:9092 --num-threads "$BURST_DEVICES" || true

for ((i = 0; i < BURST_SECONDS; i++)); do
  LAG="$(get_kafka_consumer_lag data-storage-group)"
  [[ "$LAG" -gt "$PEAK_LAG" ]] && PEAK_LAG="$LAG"
  sleep 1
done

log_progress "Phase 3/3: Waiting for recovery (lag drain)..."
RECOVERY_SECONDS=""
for ((i = 0; i < 180; i++)); do
  LAG="$(get_kafka_consumer_lag data-storage-group)"
  if [[ "$LAG" -le 5 ]]; then
    STABLE=$((STABLE + 1))
    [[ "$STABLE" -ge 30 ]] && RECOVERY_SECONDS=$((i + 1)) && break
  else
    STABLE=0
  fi
  if (( i > 0 && i % 30 == 0 )); then
    log_progress "Recovery in progress... lag=$LAG (${i}s elapsed)"
  fi
  sleep 1
done
[[ -z "$RECOVERY_SECONDS" ]] && RECOVERY_SECONDS="180+"

wait_for_drain 120
stop_stats_monitor
aggregate_resources "$RESULT_STATS" "$RESULT_RESOURCES"

BROKER_CONTAINER="$(get_container_name kafka)"
STORAGE_CONTAINER="$(get_container_name data-storage)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"
POSTGRES_CONTAINER="$(get_container_name postgres)"

write_result_header "$RESULT_TXT"
append_result "$RESULT_TXT" "scenario" "C"
append_result "$RESULT_TXT" "broker" "kafka"
append_result "$RESULT_TXT" "baseline_devices" "$BASELINE_DEVICES"
append_result "$RESULT_TXT" "burst_devices" "$BURST_DEVICES"
append_result "$RESULT_TXT" "peak_lag" "$PEAK_LAG"
append_result "$RESULT_TXT" "recovery_time_seconds" "$RECOVERY_SECONDS"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "c" "kafka" \
  "baseline,burst,peak_lag,recovery_s,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net" \
  "${BASELINE_DEVICES},${BURST_DEVICES},${PEAK_LAG},${RECOVERY_SECONDS},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb)"

log_progress "=== Scenario C Kafka done: peak_lag=$PEAK_LAG recovery=${RECOVERY_SECONDS}s ==="
teardown_stack
