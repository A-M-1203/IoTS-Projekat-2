import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ServiceMetrics:
    received: int = 0
    alerts_triggered: int = 0
    last_e2e_latency_ms: float | None = None
    last_published_at: str | None = None
    last_alert_at: str | None = None


_metrics = ServiceMetrics()
_lock = threading.Lock()


def increment_received() -> None:
    with _lock:
        _metrics.received += 1


def record_alert(published_at: str | None, e2e_latency_ms: float) -> None:
    with _lock:
        _metrics.alerts_triggered += 1
        _metrics.last_e2e_latency_ms = e2e_latency_ms
        _metrics.last_published_at = published_at
        _metrics.last_alert_at = datetime.now(timezone.utc).isoformat()


def get_metrics() -> dict[str, Any]:
    with _lock:
        return {
            "received": _metrics.received,
            "alerts_triggered": _metrics.alerts_triggered,
            "last_e2e_latency_ms": _metrics.last_e2e_latency_ms,
            "last_published_at": _metrics.last_published_at,
            "last_alert_at": _metrics.last_alert_at,
        }


def parse_published_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def compute_e2e_latency_ms(published_at: str | None) -> float | None:
    published = parse_published_at(published_at)
    if published is None:
        return None
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    return (now - published).total_seconds() * 1000
