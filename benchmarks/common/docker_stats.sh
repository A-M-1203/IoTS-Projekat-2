#!/usr/bin/env bash
set -euo pipefail

OUTPUT_CSV="${1:?output csv required}"
COMPOSE_PROFILE="${2:-mqtt}"
INTERVAL="${3:-2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

OUTPUT_CSV="$(cd "$(dirname "$OUTPUT_CSV")" && pwd)/$(basename "$OUTPUT_CSV")"

echo "timestamp,container,cpu_percent,mem_used,net_io" > "$OUTPUT_CSV"

while true; do
  TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  mapfile -t container_ids < <(docker compose --profile "$COMPOSE_PROFILE" ps -q 2>/dev/null || true)

  if ((${#container_ids[@]} == 0)); then
    sleep "$INTERVAL"
    continue
  fi

  mapfile -t lines < <(
    docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}" \
      "${container_ids[@]}" 2>/dev/null || true
  )

  for line in "${lines[@]}"; do
    [[ -z "$line" ]] && continue
    NAME="${line%%,*}"
    REST="${line#*,}"
    CPU="${REST%%,*}"
    REST2="${REST#*,}"
    MEM="${REST2%%,*}"
    NET="${REST2#*,}"
    echo "${TS},${NAME},${CPU},${MEM},${NET}" >> "$OUTPUT_CSV"
  done

  sleep "$INTERVAL"
done
