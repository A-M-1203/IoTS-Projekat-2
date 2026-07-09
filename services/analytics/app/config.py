import os


class Settings:
    broker_type: str = os.getenv("BROKER_TYPE", "mqtt").lower()
    mqtt_host: str = os.getenv("MQTT_HOST", "mosquitto")
    mqtt_port: int = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_topic: str = os.getenv("MQTT_TOPIC", "iot/agriculture/sensors")
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "iot-agriculture-sensors")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "analytics-group")
    temp_alert_threshold: float = float(os.getenv("TEMP_ALERT_THRESHOLD", "50"))
    window_size_seconds: int = int(os.getenv("WINDOW_SIZE_SECONDS", "10"))


settings = Settings()
