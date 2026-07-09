import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.service_metrics import get_metrics
from app.subscribers.kafka_subscriber import start_kafka_subscriber
from app.subscribers.mqtt_subscriber import start_mqtt_subscriber
from app.window_processor import window_loop, window_processor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    logger.info("Analytics service starting with broker: %s", settings.broker_type)

    if settings.broker_type == "kafka":
        start_kafka_subscriber()
    else:
        start_mqtt_subscriber()

    window_task = asyncio.create_task(window_loop())
    yield
    window_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await window_task


app = FastAPI(title="IoT Analytics Service", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "broker_type": settings.broker_type}


@app.get("/stats")
async def stats() -> dict:
    last_stats = window_processor.get_last_stats()
    if last_stats is None:
        return {"message": "No window processed yet"}
    return last_stats


@app.get("/metrics")
async def metrics() -> dict:
    return get_metrics()
