import json
import logging
import os
import threading

import paho.mqtt.client as mqtt

logger = logging.getLogger("analytics.mqtt")


class MqttConsumer:
    def __init__(self, on_message):
        self.on_message = on_message
        self.host = os.getenv("MQTT_HOST", "mosquitto")
        self.port = int(os.getenv("MQTT_PORT", "1883"))
        self.topic = os.getenv("MQTT_TOPIC", "iot/agriculture/readings")
        self.qos = int(os.getenv("MQTT_QOS", "1"))

    def start(self) -> None:
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self) -> None:
        while True:
            try:
                client = mqtt.Client(
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                    client_id=f"analytics-{threading.get_ident()}",
                    clean_session=False,
                )
                client.on_message = self._handle_message
                client.connect(self.host, self.port, 60)
                client.subscribe(self.topic, qos=self.qos)
                logger.info("MQTT subscribed to %s (QoS=%d)", self.topic, self.qos)
                client.loop_forever()
            except Exception as exc:
                logger.error("MQTT connection error: %s, retrying...", exc)
                import time
                time.sleep(5)

    def _handle_message(self, _client, _userdata, msg) -> None:
        try:
            data = json.loads(msg.payload.decode())
            self.on_message(data)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("MQTT message parse error: %s", exc)
