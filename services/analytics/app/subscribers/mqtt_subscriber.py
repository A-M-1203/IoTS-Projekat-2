import json
import logging
import threading
import time

import paho.mqtt.client as mqtt

from app.config import settings
from app.window_processor import window_processor

logger = logging.getLogger(__name__)


def _on_message(_client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
        device_id = payload.get("device_id", "unknown")
        temperature = float(payload["temperature"])
        window_processor.add_reading(device_id, temperature)
    except Exception as exc:
        logger.error("Failed to process MQTT message: %s", exc)


def start_mqtt_subscriber() -> None:
    def run() -> None:
        while True:
            try:
                client = mqtt.Client(
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                    client_id=f"analytics-{int(time.time())}",
                )
                client.on_message = _on_message
                client.connect(settings.mqtt_host, settings.mqtt_port, keepalive=60)
                client.subscribe(settings.mqtt_topic)
                logger.info(
                    "Subscribed to MQTT topic %s at %s:%s",
                    settings.mqtt_topic,
                    settings.mqtt_host,
                    settings.mqtt_port,
                )
                client.loop_forever()
            except Exception as exc:
                logger.warning("MQTT subscriber error: %s. Retrying in 3s...", exc)
                time.sleep(3)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
