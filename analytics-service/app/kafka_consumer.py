import asyncio
import json
import logging
import os
import threading

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger("analytics.kafka")


class KafkaConsumer:
    def __init__(self, on_message):
        self.on_message = on_message
        self.brokers = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
        self.topic = os.getenv("KAFKA_TOPIC", "iot-agriculture-readings")
        self.group_id = os.getenv("KAFKA_GROUP_ID", "analytics-group")

    def start(self) -> None:
        threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._consume())

    async def _consume(self) -> None:
        while True:
            consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.brokers,
                group_id=self.group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
            )
            try:
                await consumer.start()
                logger.info("Kafka subscribed to %s (group=%s)", self.topic, self.group_id)
                async for msg in consumer:
                    try:
                        data = json.loads(msg.value.decode())
                        self.on_message(data)
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        logger.error("Kafka message parse error: %s", exc)
            except Exception as exc:
                logger.error("Kafka connection error: %s, retrying...", exc)
                await asyncio.sleep(5)
            finally:
                try:
                    await consumer.stop()
                except Exception:
                    pass
