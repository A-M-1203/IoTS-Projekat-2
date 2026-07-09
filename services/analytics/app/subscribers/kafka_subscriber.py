import json
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaException

from app.config import settings
from app.window_processor import window_processor

logger = logging.getLogger(__name__)


def start_kafka_subscriber() -> None:
    def run() -> None:
        while True:
            consumer = Consumer(
                {
                    "bootstrap.servers": settings.kafka_bootstrap_servers,
                    "group.id": settings.kafka_group_id,
                    "auto.offset.reset": "earliest",
                }
            )

            try:
                consumer.subscribe([settings.kafka_topic])
                logger.info(
                    "Subscribed to Kafka topic %s at %s",
                    settings.kafka_topic,
                    settings.kafka_bootstrap_servers,
                )

                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        raise KafkaException(msg.error())

                    payload = json.loads(msg.value().decode("utf-8"))
                    device_id = payload.get("device_id", "unknown")
                    temperature = float(payload["temperature"])
                    window_processor.add_reading(device_id, temperature)
            except Exception as exc:
                logger.warning("Kafka subscriber error: %s. Retrying in 3s...", exc)
                time.sleep(3)
            finally:
                consumer.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
