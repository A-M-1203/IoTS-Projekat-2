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

BASELINE_RATE=50
BURST_RATE=5000
BURST_SECONDS=10
PEAK_LAG=0
STABLE=0

echo "=== Scenario C Kafka: burst ${BASELINE_RATE} -> ${BURST_RATE} msg/s ==="

setup_stack kafka 500
docker compose --profile kafka cp "$PAYLOAD_FILE" kafka:/tmp/bench_payload.json
start_stats_monitor "$RESULT_STATS"

# Baseline
docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic "$KAFKA_TOPIC" --num-records 1500 --record-size "$BENCHMARK_PAYLOAD_SIZE" \
  --throughput "$BASELINE_RATE" --payload-file /tmp/bench_payload.json \
  --producer-props acks=1 bootstrap.servers=localhost:9092 --num-threads 10 || true

for ((i = 0; i < 30; i++)); do
  LAG="$(get_kafka_consumer_lag data-storage-group)"
  [[ "$LAG" -gt "$PEAK_LAG" ]] && PEAK_LAG="$LAG"
  sleep 1
done

# Burst
docker compose --profile kafka exec -T kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic "$KAFKA_TOPIC" --num-records $((BURST_RATE * BURST_SECONDS)) \
  --record-size "$BENCHMARK_PAYLOAD_SIZE" --throughput "$BURST_RATE" \
  --payload-file /tmp/bench_payload.json \
  --producer-props acks=1 bootstrap.servers=localhost:9092 --num-threads 50 || true

for ((i = 0; i < BURST_SECONDS; i++)); do
  LAG="$(get_kafka_consumer_lag data-storage-group)"
  [[ "$LAG" -gt "$PEAK_LAG" ]] && PEAK_LAG="$LAG"
  sleep 1
done

RECOVERY_SECONDS=""
for ((i = 0; i < 180; i++)); do
  LAG="$(get_kafka_consumer_lag data-storage-group)"
  if [[ "$LAG" -le 5 ]]; then
    STABLE=$((STABLE + 1))
    [[ "$STABLE" -ge 30 ]] && RECOVERY_SECONDS=$((i + 1)) && break
  else
    STABLE=0
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
append_result "$RESULT_TXT" "baseline_msg_per_s" "$BASELINE_RATE"
append_result "$RESULT_TXT" "burst_msg_per_s" "$BURST_RATE"
append_result "$RESULT_TXT" "peak_lag" "$PEAK_LAG"
append_result "$RESULT_TXT" "recovery_time_seconds" "$RECOVERY_SECONDS"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "c" "kafka" \
  "baseline,burst,peak_lag,recovery_s,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net" \
  "${BASELINE_RATE},${BURST_RATE},${PEAK_LAG},${RECOVERY_SECONDS},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb)"

echo "=== Scenario C Kafka done: peak_lag=$PEAK_LAG recovery=${RECOVERY_SECONDS}s ==="
teardown_stack
