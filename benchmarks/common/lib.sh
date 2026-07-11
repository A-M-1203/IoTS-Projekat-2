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

get_storage_metrics_received() {
  curl -sf http://localhost:3000/metrics 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('received',0))" 2>/dev/null || echo "0"
}

get_storage_metrics_stored() {
  curl -sf http://localhost:3000/metrics 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('stored',0))" 2>/dev/null || echo "0"
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

get_kafka_consumer_lag() {
  local group="${1:-data-storage-group}"
  kafka_exec /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe --group "$group" 2>/dev/null | awk 'NR>1 {lag+=$6} END {print lag+0}'
}

get_mqtt_queue_depth() {
  docker compose --profile mqtt exec -T mosquitto mosquitto_sub -h localhost -t '$SYS/broker/messages/stored' -C 1 -W 2 2>/dev/null | tail -1 || echo "0"
}

ensure_results_dir() {
  local scenario="$1"
  local broker="$2"
  mkdir -p "results/scenario-${scenario}/${broker}"
}

SENSOR_PAYLOAD="$(cat "$COMMON_DIR/payloads/sensor.json")"
