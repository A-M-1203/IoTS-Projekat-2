#!/usr/bin/env python3
"""Aggregate docker stats CSV samples into per-container avg/peak metrics."""

import csv
import json
import sys
from collections import defaultdict


def parse_mem_mb(value: str) -> float:
    value = value.strip().split("/")[0].strip()
    if value.endswith("GiB"):
        return float(value[:-3]) * 1024
    if value.endswith("MiB"):
        return float(value[:-3])
    if value.endswith("KiB"):
        return float(value[:-3]) / 1024
    if value.endswith("B"):
        return float(value[:-1]) / (1024 * 1024)
    return 0.0


def parse_net_mb(value: str) -> float:
    parts = value.split("/")
    total = 0.0
    for part in parts:
        part = part.strip()
        if part.endswith("GB"):
            total += float(part[:-2]) * 1024
        elif part.endswith("MB"):
            total += float(part[:-2])
        elif part.endswith("kB"):
            total += float(part[:-2]) / 1024
        elif part.endswith("B"):
            total += float(part[:-1]) / (1024 * 1024)
    return total


def parse_cpu(value: str) -> float:
    return float(value.strip().replace("%", ""))


def aggregate(input_csv: str, output_json: str) -> None:
    buckets: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {
            "cpu": [],
            "ram_mb": [],
            "net_mb": [],
        }
    )

    with open(input_csv, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            name = row.get("container", row.get("name", "")).strip()
            if not name:
                continue
            buckets[name]["cpu"].append(parse_cpu(row.get("cpu_percent", "0")))
            buckets[name]["ram_mb"].append(parse_mem_mb(row.get("mem_used", "0")))
            buckets[name]["net_mb"].append(parse_net_mb(row.get("net_io", "0 / 0")))

    result = {}
    for name, values in buckets.items():
        cpu = values["cpu"] or [0.0]
        ram = values["ram_mb"] or [0.0]
        net = values["net_mb"] or [0.0]
        result[name] = {
            "avg_cpu": round(sum(cpu) / len(cpu), 2),
            "peak_cpu": round(max(cpu), 2),
            "avg_ram_mb": round(sum(ram) / len(ram), 2),
            "peak_ram_mb": round(max(ram), 2),
            "avg_net_mb": round(sum(net) / len(net), 2),
            "peak_net_mb": round(max(net), 2),
        }

    with open(output_json, "w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <stats.csv> <resources.json>", file=sys.stderr)
        sys.exit(1)
    aggregate(sys.argv[1], sys.argv[2])
