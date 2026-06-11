import asyncio
import json
import logging
import os
import statistics
import threading
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI

from app.mqtt_consumer import MqttConsumer
from app.kafka_consumer import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("analytics")

BROKER_TYPE = os.getenv("BROKER_TYPE", "mqtt")
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE_SECONDS", "10"))
TEMP_THRESHOLD = float(os.getenv("TEMP_ALERT_THRESHOLD", "50.0"))

window_temps: list[float] = []
window_devices: set[str] = set()
window_start: float = time.time()
window_lock = threading.Lock()

metrics: dict[str, Any] = {
    "messagesProcessed": 0,
    "alertsTriggered": 0,
    "lastWindowAvg": None,
    "lastWindowStart": None,
    "lastWindowEnd": None,
    "e2eLatenciesMs": [],
}


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * p / 100)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


def process_reading(data: dict) -> None:
    global window_start

    temp = data.get("temperature") or data.get("Temperature")
    if temp is None:
        return

    device_id = data.get("device_id") or data.get("deviceId") or "unknown"
    published_at = data.get("published_at") or data.get("publishedAt")

    with window_lock:
        metrics["messagesProcessed"] += 1
        window_temps.append(float(temp))
        window_devices.add(device_id)

        if float(temp) > TEMP_THRESHOLD and published_at:
            try:
                pub_time = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                latency_ms = (now - pub_time).total_seconds() * 1000
                metrics["e2eLatenciesMs"].append(latency_ms)
                if len(metrics["e2eLatenciesMs"]) > 1000:
                    metrics["e2eLatenciesMs"] = metrics["e2eLatenciesMs"][-1000:]
                logger.info(
                    "E2E latency for critical reading: device=%s temp=%.1f latency=%.1fms",
                    device_id, temp, latency_ms,
                )
            except (ValueError, TypeError):
                pass


def flush_window() -> None:
    global window_start, window_temps, window_devices

    with window_lock:
        if not window_temps:
            window_start = time.time()
            return

        avg_temp = sum(window_temps) / len(window_temps)
        start_ts = datetime.fromtimestamp(window_start, tz=timezone.utc).isoformat()
        end_ts = datetime.now(timezone.utc).isoformat()
        device_count = len(window_devices)

        metrics["lastWindowAvg"] = round(avg_temp, 2)
        metrics["lastWindowStart"] = start_ts
        metrics["lastWindowEnd"] = end_ts

        if avg_temp > TEMP_THRESHOLD:
            metrics["alertsTriggered"] += 1
            logger.critical(
                "CRITICAL ALERT [window=%s-%s] avg_temp=%.1f°C devices=%d readings=%d",
                start_ts, end_ts, avg_temp, device_count, len(window_temps),
            )
        else:
            logger.info(
                "Window [%s-%s] avg_temp=%.1f°C devices=%d readings=%d",
                start_ts, end_ts, avg_temp, device_count, len(window_temps),
            )

        window_temps = []
        window_devices = set()
        window_start = time.time()


async def window_loop() -> None:
    while True:
        await asyncio.sleep(WINDOW_SIZE)
        flush_window()


def start_broker_consumer() -> None:
    if BROKER_TYPE == "kafka":
        consumer = KafkaConsumer(process_reading)
    else:
        consumer = MqttConsumer(process_reading)
    consumer.start()


app = FastAPI(title="IoT Analytics Service")


@app.on_event("startup")
async def startup() -> None:
    threading.Thread(target=start_broker_consumer, daemon=True).start()
    asyncio.create_task(window_loop())
    logger.info("Analytics service started (broker=%s, window=%ds, threshold=%.1f°C)",
                BROKER_TYPE, WINDOW_SIZE, TEMP_THRESHOLD)


@app.get("/health")
def health() -> dict:
    return {"status": "healthy", "broker": BROKER_TYPE}


@app.get("/metrics")
def get_metrics() -> dict:
    latencies = metrics["e2eLatenciesMs"]
    return {
        "broker": BROKER_TYPE,
        "windowSizeSeconds": WINDOW_SIZE,
        "tempAlertThreshold": TEMP_THRESHOLD,
        "messagesProcessed": metrics["messagesProcessed"],
        "alertsTriggered": metrics["alertsTriggered"],
        "lastWindowAvg": metrics["lastWindowAvg"],
        "lastWindowStart": metrics["lastWindowStart"],
        "lastWindowEnd": metrics["lastWindowEnd"],
        "e2eLatencyP50Ms": percentile(latencies, 50),
        "e2eLatencyP95Ms": percentile(latencies, 95),
        "e2eLatencyP99Ms": percentile(latencies, 99),
        "e2eSampleCount": len(latencies),
    }
