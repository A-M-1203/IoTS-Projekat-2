module.exports = {
  brokerType: (process.env.BROKER_TYPE || 'mqtt').toLowerCase(),
  mqtt: {
    host: process.env.MQTT_HOST || 'mosquitto',
    port: Number(process.env.MQTT_PORT || 1883),
    topic: process.env.MQTT_TOPIC || 'iot/agriculture/sensors',
    subscribeQos: Number(process.env.MQTT_SUBSCRIBE_QOS || 2),
  },
  kafka: {
    bootstrapServers: process.env.KAFKA_BOOTSTRAP_SERVERS || 'kafka:9092',
    topic: process.env.KAFKA_TOPIC || 'iot-agriculture-sensors',
    groupId: process.env.KAFKA_GROUP_ID || 'data-storage-group',
  },
  postgres: {
    host: process.env.POSTGRES_HOST || 'postgres',
    port: Number(process.env.POSTGRES_PORT || 5432),
    database: process.env.POSTGRES_DB || 'iot_agriculture',
    user: process.env.POSTGRES_USER || 'iot',
    password: process.env.POSTGRES_PASSWORD || 'iot',
  },
  batchSize: Number(process.env.STORAGE_BATCH_SIZE || 1),
  metricsPort: Number(process.env.METRICS_PORT || 3000),
};
