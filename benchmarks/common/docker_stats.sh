#!/usr/bin/env bash
set -euo pipefail

OUTPUT_CSV="${1:?output csv required}"
INTERVAL="${2:-2}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "timestamp,container,cpu_percent,mem_used,net_io" > "$OUTPUT_CSV"

while true; do
  TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.NetIO}}" 2>/dev/null | while IFS= read -r line; do
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
