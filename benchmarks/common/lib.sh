#!/usr/bin/env bash
set -euo pipefail

COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$COMMON_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

STATS_PID=""
STATS_MONITOR_CSV=""
COMPOSE_PROFILE=""

abs_path() {
  local path="$1"
  if [[ "$path" = /* || "$path" =~ ^[A-Za-z]:[/\\] ]]; then
    echo "$path"
    return
  fi
  echo "$PROJECT_ROOT/$path"
}

stop_orphan_stats_monitors() {
  local pid_file pid

  shopt -s nullglob globstar
  for pid_file in "$PROJECT_ROOT"/results/**/*.monitor.pid; do
    pid="$(tr -d '[:space:]' <"$pid_file" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
  done
  shopt -u nullglob globstar

  if command -v pgrep >/dev/null 2>&1; then
    while read -r pid; do
      [[ -n "$pid" && "$pid" != "$$" ]] || continue
      kill "$pid" 2>/dev/null || true
    done < <(pgrep -f "[b]enchmarks/common/docker_stats.sh" 2>/dev/null || true)
  fi
}

export BROKER_TYPE="${BROKER_TYPE:-mqtt}"
export MQTT_TOPIC="${MQTT_TOPIC:-iot/agriculture/sensors}"
export KAFKA_TOPIC="${KAFKA_TOPIC:-iot-agriculture-sensors}"
export BENCHMARK_MESSAGES_PER_DEVICE="${BENCHMARK_MESSAGES_PER_DEVICE:-1}"
export BENCHMARK_PAYLOAD_SIZE="${BENCHMARK_PAYLOAD_SIZE:-384}"

# Wrapper for emqtt_bench exec — MSYS_NO_PATHCONV prevents Git Bash on Windows
# from rewriting /emqtt_bench/... to C:/Program Files/Git/emqtt_bench/...
emqtt_bench_exec() {
  local tty_flag="-T"
  if [[ "${1:-}" == "-d" ]]; then
    tty_flag="-d"
    shift
  fi
  MSYS_NO_PATHCONV=1 docker compose --profile mqtt exec "$tty_flag" emqtt-bench /emqtt_bench/bin/emqtt_bench "$@"
}

emqtt_bench_stop() {
  MSYS_NO_PATHCONV=1 docker compose --profile mqtt exec -T emqtt-bench \
    pkill -f emqtt_bench 2>/dev/null || true
}

# Run emqtt_bench pub for a fixed duration, then stop it.
emqtt_bench_pub_for() {
  local duration_s="$1"
  shift

  emqtt_bench_exec -d pub "$@" || true
  sleep "$duration_s"
  emqtt_bench_stop
}

# Wrapper for kafka exec — MSYS_NO_PATHCONV prevents Git Bash on Windows
# from rewriting /opt/kafka/... to C:/Program Files/Git/opt/kafka/...
kafka_exec() {
  local tty_flag="-T"
  if [[ "${1:-}" == "-d" ]]; then
    tty_flag="-d"
    shift
  fi
  MSYS_NO_PATHCONV=1 docker compose --profile kafka exec "$tty_flag" kafka "$@"
}

kafka_copy_payload() {
  local payload_file
  payload_file="$(abs_path "$1")"
  if [[ ! -f "$payload_file" ]]; then
    log_progress "ERROR: Payload file not found: $payload_file" >&2
    return 1
  fi
  # Avoid docker compose cp on Windows — MSYS paths like /c/Faks/... break as C:\c:
  MSYS_NO_PATHCONV=1 docker compose --profile kafka exec -T kafka \
    sh -c 'cat > /tmp/bench_payload.json' < "$payload_file"
}

emqtt_copy_payload() {
  local payload_file
  payload_file="$(abs_path "$1")"
  if [[ ! -f "$payload_file" ]]; then
    log_progress "ERROR: Payload file not found: $payload_file" >&2
    return 1
  fi
  MSYS_NO_PATHCONV=1 docker compose --profile mqtt exec -T emqtt-bench \
    sh -c 'cat > /tmp/bench_payload.json' < "$payload_file"
}

# Copy a JSON payload file into the mosquitto container (Windows-safe).
mqtt_copy_payload() {
  local payload_file
  payload_file="$(abs_path "$1")"
  if [[ ! -f "$payload_file" ]]; then
    log_progress "ERROR: Payload file not found: $payload_file" >&2
    return 1
  fi
  MSYS_NO_PATHCONV=1 docker compose --profile mqtt exec -T mosquitto \
    sh -c 'tr -d "\r" > /tmp/bench_mqtt_payload.json' < "$payload_file"
}

# Publish N messages via parallel mosquitto_pub (reliable JSON delivery on Windows).
mqtt_parallel_publish() {
  local clients="$1"
  local qos="$2"
  local messages_per_client="${3:-1}"
  local topic="${4:-$MQTT_TOPIC}"
  local wave_size="${5:-200}"
  local total=$((clients * messages_per_client))

  MSYS_NO_PATHCONV=1 docker compose --profile mqtt exec -T \
    -e "MQTT_TOPIC=$topic" \
    -e "QOS=$qos" \
    -e "TOTAL=$total" \
    -e "WAVE_SIZE=$wave_size" \
    mosquitto sh -c '
      published=0
      while [ "$published" -lt "$TOTAL" ]; do
        in_wave=0
        while [ "$in_wave" -lt "$WAVE_SIZE" ] && [ "$published" -lt "$TOTAL" ]; do
          mosquitto_pub -h localhost -p 1883 -t "$MQTT_TOPIC" -q "$QOS" \
            -f /tmp/bench_mqtt_payload.json &
          published=$((published + 1))
          in_wave=$((in_wave + 1))
        done
        wait
      done
    '
}

wait_for_storage_mqtt_subscribed() {
  local attempts="${1:-30}"

  log_progress "Waiting for data-storage MQTT subscription..."
  for ((i = 1; i <= attempts; i++)); do
    if docker compose --profile mqtt logs --tail 40 data-storage 2>/dev/null | grep -q "SUBSCRIBED"; then
      log_progress "data-storage subscribed to MQTT."
      return 0
    fi
    sleep 1
  done

  log_progress "WARNING: data-storage SUBSCRIBED not seen in logs" >&2
  return 1
}

# Kafka 3.9+ ProducerPerformance removed --num-threads; strip it for compatibility.
kafka_producer_perf() {
  local args=()
  local skip_next=0

  for arg in "$@"; do
    if (( skip_next )); then
      skip_next=0
      continue
    fi
    if [[ "$arg" == "--num-threads" ]]; then
      skip_next=1
      continue
    fi
    args+=("$arg")
  done

  kafka_exec /opt/kafka/bin/kafka-producer-perf-test.sh "${args[@]}"
}

log_progress() {
  echo "[$(date -u +%H:%M:%S)] $*"
}

timestamp_utc() {
  date -u +%Y%m%d_%H%M%S
}

calc_loss_pct() {
  local sent="$1"
  local received="$2"
  if [[ "$sent" -le 0 ]]; then
    echo "0.00"
    return
  fi
  awk -v s="$sent" -v r="$received" 'BEGIN { printf "%.2f", (s - r) * 100 / s }'
}

get_compose_network() {
  echo "iot-net"
}

wait_for_healthy() {
  local attempts="${1:-90}"
  for ((i = 1; i <= attempts; i++)); do
    local pg_ok=0 broker_ok=0 storage_ok=0 analytics_ok=0

    docker compose --profile "$COMPOSE_PROFILE" exec -T postgres pg_isready -U iot -d iot_agriculture >/dev/null 2>&1 && pg_ok=1

    if [[ "$COMPOSE_PROFILE" == "mqtt" ]]; then
      docker compose --profile mqtt exec -T mosquitto mosquitto_sub -h localhost -t '$SYS/#' -C 1 -W 2 >/dev/null 2>&1 && broker_ok=1
    else
      kafka_exec /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && broker_ok=1
    fi

    curl -sf http://localhost:3000/health >/dev/null 2>&1 && storage_ok=1
    curl -sf http://localhost:8000/health >/dev/null 2>&1 && analytics_ok=1

    if [[ "$pg_ok" -eq 1 && "$broker_ok" -eq 1 && "$storage_ok" -eq 1 && "$analytics_ok" -eq 1 ]]; then
      log_progress "Stack is healthy."
      return 0
    fi
    if (( i % 5 == 0 )); then
      log_progress "Waiting for services... attempt ${i}/${attempts}"
    fi
    sleep 2
  done
  log_progress "ERROR: Stack health check timed out" >&2
  return 1
}

setup_stack() {
  local broker="$1"
  local batch_size="${2:-1}"
  COMPOSE_PROFILE="$broker"

  export BROKER_TYPE="$broker"
  export STORAGE_BATCH_SIZE="$batch_size"
  export BENCHMARK_INSTANT_ALERT="${BENCHMARK_INSTANT_ALERT:-false}"
  export BENCHMARK_ALERT_THRESHOLD="${BENCHMARK_ALERT_THRESHOLD:-40}"
  export TEMP_ALERT_THRESHOLD="${TEMP_ALERT_THRESHOLD:-50}"

  log_progress "Setting up stack (broker=$broker, STORAGE_BATCH_SIZE=$batch_size)"
  log_progress "Stopping any existing containers..."
  docker compose --profile "$broker" down -v --remove-orphans 2>/dev/null || true

  if [[ "$broker" == "mqtt" ]]; then
    log_progress "Starting mosquitto and emqtt-bench..."
    docker compose --profile mqtt up -d mosquitto emqtt-bench
  else
    log_progress "Starting kafka broker..."
    docker compose --profile kafka up -d kafka
  fi

  log_progress "Starting postgres, data-storage, analytics..."
  docker compose --profile "$broker" up -d --build postgres data-storage analytics

  if [[ "$broker" == "kafka" ]]; then
    log_progress "Creating Kafka topic if needed..."
    kafka_exec /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$KAFKA_TOPIC" \
      --partitions 1 --replication-factor 1 || true
  fi

  log_progress "Waiting for all services to become healthy..."
  wait_for_healthy 90
}

teardown_stack() {
  local broker="${COMPOSE_PROFILE:-mqtt}"
  log_progress "Tearing down stack..."
  docker compose --profile "$broker" down -v --remove-orphans 2>/dev/null || true
  log_progress "Stack stopped."
}

start_stats_monitor() {
  local output_csv="$1"
  output_csv="$(abs_path "$output_csv")"
  mkdir -p "$(dirname "$output_csv")"

  stop_orphan_stats_monitors

  bash "$COMMON_DIR/docker_stats.sh" "$output_csv" "$COMPOSE_PROFILE" 2 &
  STATS_PID=$!
  STATS_MONITOR_CSV="$output_csv"
  echo "$STATS_PID" > "${output_csv}.monitor.pid"
  log_progress "Started docker stats monitor (pid=$STATS_PID) -> $output_csv"
}

stop_stats_monitor() {
  if [[ -n "$STATS_PID" ]] && kill -0 "$STATS_PID" 2>/dev/null; then
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
  fi

  if [[ -n "$STATS_MONITOR_CSV" ]]; then
    rm -f "${STATS_MONITOR_CSV}.monitor.pid"
  fi

  STATS_PID=""
  STATS_MONITOR_CSV=""
  stop_orphan_stats_monitors
  log_progress "Stopped docker stats monitor."
}

aggregate_resources() {
  local stats_csv="$1"
  local output_json="$2"
  stats_csv="$(abs_path "$stats_csv")"
  output_json="$(abs_path "$output_json")"
  mkdir -p "$(dirname "$output_json")"
  if [[ -f "$stats_csv" ]]; then
    log_progress "Aggregating container resource metrics from $stats_csv -> $output_json"
    python3 "$COMMON_DIR/aggregate_stats.py" "$stats_csv" "$output_json"
    log_progress "Resource metrics saved to $output_json"
  else
    echo "{}" > "$output_json"
  fi
}

get_received_count() {
  docker compose --profile "$COMPOSE_PROFILE" exec -T postgres \
    psql -U iot -d iot_agriculture -t -A -c "SELECT COUNT(*) FROM sensor_readings;" 2>/dev/null | tr -d '[:space:]'
}

get_storage_metrics_field() {
  local field="$1"
  local metrics_json
  metrics_json="$(curl -sf http://localhost:3000/metrics 2>/dev/null || echo '{}')"
  STORAGE_METRICS_JSON="$metrics_json" python3 - "$field" <<'PY'
import json
import os
import sys

field = sys.argv[1]
try:
    data = json.loads(os.environ.get("STORAGE_METRICS_JSON", "{}"))
    print(int(data.get(field, 0)))
except (TypeError, ValueError, json.JSONDecodeError):
    print(0)
PY
}

get_storage_metrics_received() {
  get_storage_metrics_field "received"
}

get_storage_metrics_stored() {
  get_storage_metrics_field "stored"
}

wait_for_storage_metric() {
  local field="$1"
  local min_value="${2:-1}"
  local attempts="${3:-30}"
  local value=0

  for ((i = 1; i <= attempts; i++)); do
    if [[ "$field" == "stored" ]]; then
      value="$(get_storage_metrics_stored)"
    else
      value="$(get_storage_metrics_received)"
    fi
    if [[ "$value" =~ ^[0-9]+$ ]] && (( value >= min_value )); then
      echo "$value"
      return 0
    fi
    sleep 1
  done

  echo "${value:-0}"
}

drain_storage_buffer() {
  curl -sf -X POST http://localhost:3000/drain >/dev/null 2>&1 || true
}

wait_for_drain() {
  local timeout="${1:-120}"
  local stable_needed=3
  local stable=0
  local last_count=-1

  log_progress "Waiting for messages to be stored (timeout ${timeout}s)..."

  for ((i = 0; i < timeout; i++)); do
    drain_storage_buffer
    local count
    count="$(get_received_count)"
    count="${count:-0}"

    if [[ "$count" == "$last_count" ]]; then
      stable=$((stable + 1))
      if [[ "$stable" -ge "$stable_needed" ]]; then
        log_progress "Drain complete — stored count=$count"
        return 0
      fi
    else
      stable=0
      last_count="$count"
    fi

    if (( i > 0 && i % 15 == 0 )); then
      log_progress "Still draining... stored count=$count (${i}s elapsed)"
    fi
    sleep 1
  done

  log_progress "WARNING: Drain timeout — last count=$last_count"
}

write_result_header() {
  local path="$1"
  mkdir -p "$(dirname "$path")"
  cat >"$path" <<EOF
Benchmark Result
================
EOF
}

append_result() {
  local path="$1"
  local key="$2"
  local value="$3"
  echo "${key}: ${value}" >>"$path"
}

append_summary_csv() {
  local scenario="$1"
  local broker="$2"
  local csv_path="results/scenario-${scenario}/${broker}/summary.csv"
  mkdir -p "$(dirname "$csv_path")"
  if [[ ! -f "$csv_path" ]]; then
    echo "$3" >"$csv_path"
  else
    echo "$4" >>"$csv_path"
  fi
}

resource_field() {
  local json_file="$1"
  local container="$2"
  local field="$3"
  python3 - "$json_file" "$container" "$field" <<'PY'
import json
import sys

json_file, container, field = sys.argv[1:4]
with open(json_file, encoding="utf-8") as handle:
    data = json.load(handle)
entry = data.get(container, {})
print(entry.get(field, "N/A"))
PY
}

format_resource_pair() {
  local json_file="$1"
  local container="$2"
  local metric_prefix="$3"
  json_file="$(abs_path "$json_file")"
  local avg
  local peak
  avg="$(resource_field "$json_file" "$container" "avg_${metric_prefix}" 2>/dev/null || echo "N/A")"
  peak="$(resource_field "$json_file" "$container" "peak_${metric_prefix}" 2>/dev/null || echo "N/A")"
  echo "${avg}/${peak}"
}

get_container_name() {
  local service="$1"
  docker compose --profile "$COMPOSE_PROFILE" ps --format '{{.Service}} {{.Name}}' | awk -v s="$service" '$1==s {print $2; exit}'
}

get_kafka_group_metrics() {
  local group="${1:-data-storage-group}"
  local describe_output
  describe_output="$(kafka_exec /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group "$group" 2>/dev/null || true)"
  python3 - "$describe_output" <<'PY'
import sys

text = sys.argv[1] if len(sys.argv) > 1 else ""
lines = [line.strip().replace("\r", "") for line in text.splitlines() if line.strip()]
header = None
header_idx = -1

for idx, line in enumerate(lines):
    if "CURRENT-OFFSET" in line and "LAG" in line:
        header = line.split()
        header_idx = idx
        break

if header is None:
    print("0\t0\t0")
    raise SystemExit

columns = {
    name: header.index(name)
    for name in ("PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG")
    if name in header
}

current = 0
log_end = 0
lag = 0
current_seen = False

for line in lines[header_idx + 1 :]:
    if line.startswith(("Consumer group", "Warning", "Note:")):
        continue

    parts = line.split()
    if len(parts) <= max(columns.values()):
        continue

    try:
        int(parts[columns["PARTITION"]])
    except (ValueError, KeyError, IndexError):
        continue

    cur = parts[columns["CURRENT-OFFSET"]]
    end = parts[columns["LOG-END-OFFSET"]]
    lag_val = parts[columns["LAG"]]

    if cur != "-":
        current += int(cur)
        current_seen = True
    if end != "-":
        log_end += int(end)
    if lag_val != "-":
        lag += int(lag_val)

offset = current if current_seen else log_end
print(f"{offset}\t{log_end}\t{lag}")
PY
}

get_kafka_consumer_offset() {
  local group="${1:-data-storage-group}"
  get_kafka_group_metrics "$group" | cut -f1 || echo "0"
}

get_kafka_consumer_lag() {
  local group="${1:-data-storage-group}"
  get_kafka_group_metrics "$group" | cut -f3 || echo "0"
}

get_mqtt_queue_depth() {
  local depth
  depth="$(docker compose --profile mqtt exec -T mosquitto mosquitto_sub -h localhost -t '$SYS/broker/messages/stored' -C 1 -W 2 2>/dev/null | tail -1 || echo "0")"
  depth="${depth//[^0-9]/}"
  echo "${depth:-0}"
}

get_storage_buffered() {
  get_storage_metrics_field "buffered"
}

# Storage pipeline lag relative to a captured baseline (fast; no broker $SYS query).
mqtt_pipeline_backlog() {
  local pipeline_baseline="${1:-0}"
  local received stored pipeline_delta

  received="$(get_storage_metrics_received)"
  stored="$(get_storage_metrics_stored)"
  received="${received:-0}"
  stored="${stored:-0}"
  pipeline_delta=$((received - stored - pipeline_baseline))
  [[ "$pipeline_delta" -lt 0 ]] && pipeline_delta=0
  echo "$pipeline_delta"
}

mqtt_total_backlog() {
  mqtt_pipeline_backlog "${2:-0}"
}

# Publish a JSON payload via mosquitto_pub (reliable for benchmark alert payloads).
mqtt_publish_json() {
  local payload="$1"
  local qos="${2:-1}"
  local topic="${3:-$MQTT_TOPIC}"
  local host_file

  host_file="$(bench_temp_file "mqtt-pub")"
  mkdir -p "$(dirname "$host_file")"
  printf '%s' "$payload" > "$host_file"

  MSYS_NO_PATHCONV=1 docker compose --profile mqtt exec -T mosquitto \
    sh -c 'tr -d "\r" > /tmp/bench_mqtt_payload.json' < "$host_file"
  MSYS_NO_PATHCONV=1 docker compose --profile mqtt exec -T mosquitto \
    mosquitto_pub -h localhost -p 1883 -t "$topic" -q "$qos" -f /tmp/bench_mqtt_payload.json

  rm -f "$host_file"
}

get_analytics_metrics_field() {
  local field="$1"
  local metrics_json
  metrics_json="$(curl -sf http://localhost:8000/metrics 2>/dev/null || echo '{}')"
  ANALYTICS_METRICS_JSON="$metrics_json" python3 - "$field" <<'PY'
import json
import os
import sys

field = sys.argv[1]
try:
    data = json.loads(os.environ.get("ANALYTICS_METRICS_JSON", "{}"))
    value = data.get(field)
    if value is None:
        print("")
    else:
        print(value)
except (TypeError, ValueError, json.JSONDecodeError):
    print("")
PY
}

wait_for_e2e_latency() {
  local expected_published_at="$1"
  local timeout="${2:-15}"
  local metrics_json latency=""

  for ((i = 0; i < timeout * 4; i++)); do
    metrics_json="$(curl -sf http://localhost:8000/metrics 2>/dev/null || echo '{}')"
    latency="$(ANALYTICS_METRICS_JSON="$metrics_json" ANALYTICS_EXPECTED_PUBLISHED_AT="$expected_published_at" python3 - <<'PY'
import json
import os

expected = os.environ.get("ANALYTICS_EXPECTED_PUBLISHED_AT", "")
try:
    data = json.loads(os.environ.get("ANALYTICS_METRICS_JSON", "{}"))
except json.JSONDecodeError:
    print("")
    raise SystemExit
if data.get("last_published_at") != expected:
    print("")
    raise SystemExit
latency = data.get("last_e2e_latency_ms")
print("" if latency is None else latency)
PY
)"
    if [[ -n "$latency" ]]; then
      echo "$latency"
      return 0
    fi
    sleep 0.25
  done

  echo "0"
  return 1
}

ensure_results_dir() {
  local scenario="$1"
  local broker="$2"
  mkdir -p "results/scenario-${scenario}/${broker}"
}

bench_temp_file() {
  local label="${1:-bench}"
  mkdir -p "results/.tmp"
  echo "results/.tmp/${label}_$$.tmp"
}

compute_latency_stats() {
  local latencies_file="$1"
  python3 - "$latencies_file" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.is_file():
    print("0,0,0,0")
    raise SystemExit

vals = [float(x.strip()) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]
if not vals:
    print("0,0,0,0")
else:
    vals.sort()
    p95 = vals[max(0, int(0.95 * len(vals)) - 1)]
    print(f"{sum(vals)/len(vals):.2f},{min(vals):.2f},{max(vals):.2f},{p95:.2f}")
PY
}

SENSOR_PAYLOAD="$(cat "$COMMON_DIR/payloads/sensor.json")"
