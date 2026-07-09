import logging
from datetime import datetime, timezone

from app.config import settings
from app.service_metrics import compute_e2e_latency_ms, increment_received, record_alert
from app.window_processor import window_processor

logger = logging.getLogger(__name__)


def process_reading(payload: dict) -> None:
    increment_received()
    device_id = payload.get("device_id", "unknown")
    temperature = float(payload["temperature"])
    published_at = payload.get("published_at")

    window_processor.add_reading(device_id, temperature)

    threshold = (
        settings.benchmark_alert_threshold
        if settings.benchmark_instant_alert
        else settings.temp_alert_threshold
    )

    if settings.benchmark_instant_alert and temperature > threshold:
        e2e_latency_ms = compute_e2e_latency_ms(published_at)
        if e2e_latency_ms is None:
            e2e_latency_ms = 0.0
        alert_at = datetime.now(timezone.utc).isoformat()
        record_alert(published_at, e2e_latency_ms)
        logger.critical(
            "ALERT: Device %s temperature %.2f°C exceeded threshold %.2f°C "
            "e2e_latency_ms=%.2f published_at=%s alert_at=%s",
            device_id,
            temperature,
            threshold,
            e2e_latency_ms,
            published_at,
            alert_at,
        )
