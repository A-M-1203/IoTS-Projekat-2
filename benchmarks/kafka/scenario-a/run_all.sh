#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for script in "$SCRIPT_DIR"/run_*.sh; do
  [[ "$script" == *"_run.sh" ]] && continue
  echo "Running $(basename "$script")..."
  bash "$script"
  echo "Sleeping 10s before next run..."
  sleep 10
done

echo "All Scenario A Kafka runs completed."
