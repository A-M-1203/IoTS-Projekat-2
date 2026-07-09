from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

WRAPPER_TEMPLATE = """#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$SCRIPT_DIR/_run.sh" __ARGS__
"""

for clients in (100, 1000, 10000):
    for qos in (0, 1, 2):
        path = ROOT / f"benchmarks/mqtt/scenario-a/run_{clients}_qos{qos}.sh"
        content = WRAPPER_TEMPLATE.replace("__ARGS__", f"{clients} {qos}")
        path.write_text(content, encoding="utf-8", newline="\n")

for clients in (100, 1000, 10000):
    for acks in ("0", "1", "all"):
        path = ROOT / f"benchmarks/kafka/scenario-a/run_{clients}_acks{acks}.sh"
        content = WRAPPER_TEMPLATE.replace("__ARGS__", f"{clients} {acks}")
        path.write_text(content, encoding="utf-8", newline="\n")

print("Generated 18 scenario A wrapper scripts.")
