import asyncio
import logging
import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class WindowStats:
    window_start: str
    window_end: str
    count: int
    avg_temperature: float
    min_temperature: float
    max_temperature: float
    per_device: dict[str, dict[str, float | int]]
    alert_triggered: bool = False


@dataclass
class WindowBuffer:
    temperatures: list[float] = field(default_factory=list)
    device_temperatures: dict[str, list[float]] = field(default_factory=lambda: defaultdict(list))

    def add(self, device_id: str, temperature: float) -> None:
        self.temperatures.append(temperature)
        self.device_temperatures[device_id].append(temperature)

    def clear(self) -> None:
        self.temperatures.clear()
        self.device_temperatures.clear()

    def is_empty(self) -> bool:
        return len(self.temperatures) == 0


class WindowProcessor:
    def __init__(self) -> None:
        self._buffer = WindowBuffer()
        self._lock = threading.Lock()
        self._last_stats: WindowStats | None = None

    def add_reading(self, device_id: str, temperature: float) -> None:
        with self._lock:
            self._buffer.add(device_id, temperature)

    def flush(self) -> WindowStats | None:
        with self._lock:
            if self._buffer.is_empty():
                return None

            window_end = datetime.now(timezone.utc)
            temperatures = list(self._buffer.temperatures)
            per_device: dict[str, dict[str, float | int]] = {}

            for device_id, device_temps in self._buffer.device_temperatures.items():
                per_device[device_id] = {
                    "count": len(device_temps),
                    "avg": round(sum(device_temps) / len(device_temps), 2),
                    "min": min(device_temps),
                    "max": max(device_temps),
                }

            avg_temperature = sum(temperatures) / len(temperatures)
            alert_triggered = avg_temperature > settings.temp_alert_threshold

            stats = WindowStats(
                window_start=window_end.isoformat(),
                window_end=window_end.isoformat(),
                count=len(temperatures),
                avg_temperature=avg_temperature,
                min_temperature=min(temperatures),
                max_temperature=max(temperatures),
                per_device=per_device,
                alert_triggered=alert_triggered,
            )

            self._buffer.clear()
            self._last_stats = stats
            return stats

    def get_last_stats(self) -> dict[str, Any] | None:
        if self._last_stats is None:
            return None

        stats = self._last_stats
        return {
            "window_start": stats.window_start,
            "window_end": stats.window_end,
            "count": stats.count,
            "avg_temperature": round(stats.avg_temperature, 2),
            "min_temperature": round(stats.min_temperature, 2),
            "max_temperature": round(stats.max_temperature, 2),
            "per_device": stats.per_device,
            "alert_triggered": stats.alert_triggered,
            "threshold": settings.temp_alert_threshold,
        }


window_processor = WindowProcessor()


async def window_loop() -> None:
    while True:
        await asyncio.sleep(settings.window_size_seconds)
        stats = window_processor.flush()
        if stats is None:
            logger.info("Window closed with no readings.")
            continue

        logger.info(
            "Window stats: count=%s avg=%.2f min=%.2f max=%.2f threshold=%.2f",
            stats.count,
            stats.avg_temperature,
            stats.min_temperature,
            stats.max_temperature,
            settings.temp_alert_threshold,
        )

        if stats.alert_triggered:
            logger.critical(
                "ALERT: Average temperature %.2f°C exceeded threshold %.2f°C in %ss window (count=%s)",
                stats.avg_temperature,
                settings.temp_alert_threshold,
                settings.window_size_seconds,
                stats.count,
            )
