#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../../common/lib.sh
source "$SCRIPT_DIR/../../common/lib.sh"

TOTAL=0
for script in "$SCRIPT_DIR"/run_*.sh; do
  [[ "$script" == *"_run.sh" ]] && continue
  TOTAL=$((TOTAL + 1))
done

CURRENT=0
for script in "$SCRIPT_DIR"/run_*.sh; do
  [[ "$script" == *"_run.sh" ]] && continue
  CURRENT=$((CURRENT + 1))
  log_progress "[$CURRENT/$TOTAL] Running $(basename "$script")..."
  bash "$script"
  if [[ "$CURRENT" -lt "$TOTAL" ]]; then
    log_progress "Pausing 10s before next configuration..."
    sleep 10
  fi
done

log_progress "All Scenario A MQTT runs completed."
