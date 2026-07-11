#!/usr/bin/env bash
set -euo pipefail

CLIENTS="${1:?clients required}"
ACKS="${2:?acks required}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

CONFIG="devices_${CLIENTS}_acks${ACKS}"
TS="$(timestamp_utc)"
ensure_results_dir "a" "kafka"
RESULT_TXT="results/scenario-a/kafka/${CONFIG}_${TS}.txt"
RESULT_STATS="results/scenario-a/kafka/${CONFIG}_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-a/kafka/${CONFIG}_${TS}_resources.json"
PAYLOAD_FILE="$SCRIPT_DIR/../../common/payloads/sensor.json"

MESSAGES_PER_DEVICE="${BENCHMARK_MESSAGES_PER_DEVICE}"
SENT=$((CLIENTS * MESSAGES_PER_DEVICE))

log_progress "=== Scenario A Kafka: clients=$CLIENTS acks=$ACKS sent=$SENT ==="

setup_stack kafka 500

log_progress "Copying benchmark payload to Kafka container..."
kafka_copy_payload "$PAYLOAD_FILE"
kafka_exec /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic "$KAFKA_TOPIC" \
  --partitions 1 --replication-factor 1 || true

start_stats_monitor "$RESULT_STATS"
START_TS=$(date +%s)

log_progress "Publishing $SENT messages via kafka-producer-perf-test (this may take a while)..."
BENCH_OUTPUT=$(kafka_producer_perf \
  --topic "$KAFKA_TOPIC" \
  --num-records "$SENT" \
  --throughput -1 \
  --payload-file /tmp/bench_payload.json \
  --producer-props "acks=${ACKS} bootstrap.servers=localhost:9092" \
  --num-threads "$CLIENTS" 2>&1) || true

log_progress "Load generation finished."
echo "$BENCH_OUTPUT"

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

BROKER_CONTAINER="$(get_container_name kafka)"
STORAGE_CONTAINER="$(get_container_name data-storage)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"
POSTGRES_CONTAINER="$(get_container_name postgres)"

write_result_header "$RESULT_TXT"
append_result "$RESULT_TXT" "scenario" "A"
append_result "$RESULT_TXT" "broker" "kafka"
append_result "$RESULT_TXT" "clients" "$CLIENTS"
append_result "$RESULT_TXT" "acks" "$ACKS"
append_result "$RESULT_TXT" "sent" "$SENT"
append_result "$RESULT_TXT" "received" "$RECEIVED"
append_result "$RESULT_TXT" "lost_percent" "$LOSS"
append_result "$RESULT_TXT" "duration_seconds" "$DURATION"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "a" "kafka" \
  "devices,acks,sent,received,lost_percent,duration_s,broker_cpu,broker_ram,broker_net,storage_cpu,storage_ram,storage_net,analytics_cpu,analytics_ram,analytics_net,postgres_cpu,postgres_ram,postgres_net" \
  "${CLIENTS},${ACKS},${SENT},${RECEIVED},${LOSS},${DURATION},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$STORAGE_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$POSTGRES_CONTAINER" net_mb)"

log_progress "=== Done: sent=$SENT received=$RECEIVED lost=${LOSS}% duration=${DURATION}s ==="
teardown_stack
