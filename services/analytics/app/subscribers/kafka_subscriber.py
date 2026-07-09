import json
import logging
import threading
import time

from confluent_kafka import Consumer, KafkaException

from app.config import settings
from app.message_handler import process_reading

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
                    "SUBSCRIBED at %s to Kafka topic %s at %s",
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
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
                    process_reading(payload)
            except Exception as exc:
                logger.warning("RECONNECTING after Kafka error: %s. Retrying in 3s...", exc)
                time.sleep(3)
            finally:
                consumer.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
