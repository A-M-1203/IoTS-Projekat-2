#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

TS="$(timestamp_utc)"
ensure_results_dir "d" "mqtt"
RESULT_TXT="results/scenario-d/mqtt/alerting_${TS}.txt"
RESULT_STATS="results/scenario-d/mqtt/alerting_${TS}_stats.csv"
RESULT_RESOURCES="results/scenario-d/mqtt/alerting_${TS}_resources.json"
RUNS=10

log_progress "=== Scenario D MQTT: E2E alerting latency ($RUNS runs) ==="

export BENCHMARK_INSTANT_ALERT=true
export BENCHMARK_ALERT_THRESHOLD=40
export TEMP_ALERT_THRESHOLD=40

setup_stack mqtt 1
write_result_header "$RESULT_TXT"
start_stats_monitor "$RESULT_STATS"

LATENCIES_FILE="$(bench_temp_file "scenario-d-mqtt-${TS}")"
trap 'rm -f "$LATENCIES_FILE"' EXIT

for ((run = 1; run <= RUNS; run++)); do
  log_progress "Alert test run $run/$RUNS..."
  PUBLISHED_AT="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
  PAYLOAD=$(python3 - "$SCRIPT_DIR/../../common/payloads/critical_sensor.json" "$PUBLISHED_AT" "$run" <<'PY'
import json, sys
from pathlib import Path
template = Path(sys.argv[1])
published_at = sys.argv[2]
run_id = sys.argv[3]
payload = json.loads(template.read_text(encoding="utf-8"))
payload["published_at"] = published_at
payload["message_id"] = f"bench-run-{run_id}"
print(json.dumps(payload))
PY
)

  emqtt_bench_exec pub \
    -h mosquitto -p 1883 -c 1 -q 1 -t "$MQTT_TOPIC" \
    -s "$BENCHMARK_PAYLOAD_SIZE" -n "$BENCHMARK_MESSAGES_PER_DEVICE" -m "$PAYLOAD" >/dev/null 2>&1 || true

  sleep 2
  LATENCY=$(curl -sf http://localhost:8000/metrics 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('last_e2e_latency_ms',0))" || echo "0")
  log_progress "  run $run: e2e_latency_ms=$LATENCY"
  echo "run=$run published_at=$PUBLISHED_AT e2e_latency_ms=$LATENCY" >> "$RESULT_TXT"
  echo "$LATENCY" >> "$LATENCIES_FILE"
  sleep 1
done

stop_stats_monitor
aggregate_resources "$RESULT_STATS" "$RESULT_RESOURCES"

STATS=$(compute_latency_stats "$LATENCIES_FILE")

IFS=',' read -r AVG_LAT MIN_LAT MAX_LAT P95_LAT <<< "$STATS"

BROKER_CONTAINER="$(get_container_name mosquitto)"
ANALYTICS_CONTAINER="$(get_container_name analytics)"

append_result "$RESULT_TXT" "avg_e2e_latency_ms" "$AVG_LAT"
append_result "$RESULT_TXT" "min_e2e_latency_ms" "$MIN_LAT"
append_result "$RESULT_TXT" "max_e2e_latency_ms" "$MAX_LAT"
append_result "$RESULT_TXT" "p95_e2e_latency_ms" "$P95_LAT"
append_result "$RESULT_TXT" "resources_json" "$RESULT_RESOURCES"

append_summary_csv "d" "mqtt" \
  "run,published_at,alert_at,e2e_ms,broker_cpu,broker_ram,broker_net,analytics_cpu,analytics_ram,analytics_net" \
  "avg/p95,,,${AVG_LAT}/${P95_LAT},$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$BROKER_CONTAINER" net_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" cpu),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" ram_mb),$(format_resource_pair "$RESULT_RESOURCES" "$ANALYTICS_CONTAINER" net_mb)"

log_progress "=== Scenario D MQTT done: avg=${AVG_LAT}ms p95=${P95_LAT}ms ==="
teardown_stack
