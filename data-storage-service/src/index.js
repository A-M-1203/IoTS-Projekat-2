const { Pool } = require('pg');
const express = require('express');
const { createMqttConsumer } = require('./mqttConsumer');
const { createKafkaConsumer } = require('./kafkaConsumer');

const config = {
  brokerType: process.env.BROKER_TYPE || 'mqtt',
  postgres: {
    host: process.env.POSTGRES_HOST || 'postgres',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    user: process.env.POSTGRES_USER || 'iotuser',
    password: process.env.POSTGRES_PASSWORD || 'iotpass',
    database: process.env.POSTGRES_DB || 'iotdb',
  },
  mqtt: {
    host: process.env.MQTT_HOST || 'mosquitto',
    port: parseInt(process.env.MQTT_PORT || '1883', 10),
    topic: process.env.MQTT_TOPIC || 'iot/agriculture/readings',
    qos: parseInt(process.env.MQTT_QOS || '1', 10),
  },
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || 'kafka:9092').split(','),
    topic: process.env.KAFKA_TOPIC || 'iot-agriculture-readings',
    groupId: process.env.KAFKA_GROUP_ID || 'storage-group',
  },
  batchMode: process.env.BATCH_MODE === 'true',
  batchSize: parseInt(process.env.BATCH_SIZE || '500', 10),
  port: parseInt(process.env.PORT || '3000', 10),
};

const metrics = {
  messagesReceived: 0,
  messagesStored: 0,
  messagesFailed: 0,
  lastStoredAt: null,
};

const pool = new Pool(config.postgres);
let batchBuffer = [];

function mapReading(data) {
  return {
    message_id: data.message_id || data.messageId || null,
    timestamp: data.timestamp || data.Timestamp,
    device_id: data.device_id || data.deviceId,
    location: data.location || data.Location,
    crop_type: data.crop_type || data.cropType,
    season: data.season || data.Season,
    temperature: data.temperature ?? data.Temperature,
    humidity: data.humidity ?? data.Humidity,
    rainfall: data.rainfall ?? data.Rainfall,
    soil_moisture: data.soil_moisture ?? data.soilMoisture,
    soil_ph: data.soil_ph ?? data.soilPh,
    light_intensity: data.light_intensity ?? data.lightIntensity,
    fertilizer_used: data.fertilizer_used ?? data.fertilizerUsed,
    irrigation_needed: data.irrigation_needed ?? data.irrigationNeeded,
    crop_health: data.crop_health || data.cropHealth,
    yield_estimate: data.yield_estimate ?? data.yieldEstimate,
    pest_risk: data.pest_risk || data.pestRisk,
    anomaly_flag: data.anomaly_flag ?? data.anomalyFlag,
  };
}

async function insertSingle(reading) {
  const r = mapReading(reading);
  const query = `
    INSERT INTO sensor_readings (
      message_id, timestamp, device_id, location, crop_type, season,
      temperature, humidity, rainfall, soil_moisture, soil_ph, light_intensity,
      fertilizer_used, irrigation_needed, crop_health, yield_estimate, pest_risk, anomaly_flag
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
    ) ON CONFLICT (message_id) DO NOTHING
  `;
  const values = [
    r.message_id, r.timestamp, r.device_id, r.location, r.crop_type, r.season,
    r.temperature, r.humidity, r.rainfall, r.soil_moisture, r.soil_ph, r.light_intensity,
    r.fertilizer_used, r.irrigation_needed, r.crop_health, r.yield_estimate, r.pest_risk, r.anomaly_flag,
  ];
  await pool.query(query, values);
}

async function insertBatch(readings) {
  if (readings.length === 0) return;

  const columns = [
    'message_id', 'timestamp', 'device_id', 'location', 'crop_type', 'season',
    'temperature', 'humidity', 'rainfall', 'soil_moisture', 'soil_ph', 'light_intensity',
    'fertilizer_used', 'irrigation_needed', 'crop_health', 'yield_estimate', 'pest_risk', 'anomaly_flag',
  ];

  const values = [];
  const placeholders = readings.map((reading, rowIdx) => {
    const r = mapReading(reading);
    const rowPlaceholders = columns.map((_, colIdx) => {
      values.push([
        r.message_id, r.timestamp, r.device_id, r.location, r.crop_type, r.season,
        r.temperature, r.humidity, r.rainfall, r.soil_moisture, r.soil_ph, r.light_intensity,
        r.fertilizer_used, r.irrigation_needed, r.crop_health, r.yield_estimate, r.pest_risk, r.anomaly_flag,
      ][colIdx]);
      return `$${rowIdx * columns.length + colIdx + 1}`;
    });
    return `(${rowPlaceholders.join(', ')})`;
  });

  const query = `
    INSERT INTO sensor_readings (${columns.join(', ')})
    VALUES ${placeholders.join(', ')}
    ON CONFLICT (message_id) DO NOTHING
  `;
  await pool.query(query, values);
}

async function flushBatch() {
  if (batchBuffer.length === 0) return;
  const toInsert = batchBuffer.splice(0, batchBuffer.length);
  try {
    await insertBatch(toInsert);
    metrics.messagesStored += toInsert.length;
    metrics.lastStoredAt = new Date().toISOString();
  } catch (err) {
    console.error('Batch insert failed:', err.message);
    metrics.messagesFailed += toInsert.length;
  }
}

async function handleMessage(data) {
  metrics.messagesReceived++;
  try {
    if (config.batchMode) {
      batchBuffer.push(data);
      if (batchBuffer.length >= config.batchSize) {
        await flushBatch();
      }
    } else {
      await insertSingle(data);
      metrics.messagesStored++;
      metrics.lastStoredAt = new Date().toISOString();
    }
  } catch (err) {
    console.error('Store failed:', err.message);
    metrics.messagesFailed++;
  }
}

async function startConsumer() {
  if (config.brokerType === 'kafka') {
    await createKafkaConsumer(config.kafka, handleMessage);
    console.log(`Kafka consumer started on topic ${config.kafka.topic}`);
  } else {
    await createMqttConsumer(config.mqtt, handleMessage);
    console.log(`MQTT consumer started on topic ${config.mqtt.topic}`);
  }
}

async function waitForPostgres() {
  for (let i = 0; i < 30; i++) {
    try {
      await pool.query('SELECT 1');
      console.log('PostgreSQL connected');
      return;
    } catch {
      console.log('Waiting for PostgreSQL...');
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('PostgreSQL not available');
}

const app = express();

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ status: 'healthy', broker: config.brokerType });
  } catch (err) {
    res.status(503).json({ status: 'unhealthy', error: err.message });
  }
});

app.get('/metrics', async (_req, res) => {
  const dbResult = await pool.query('SELECT COUNT(*) AS count FROM sensor_readings');
  res.json({
    broker: config.brokerType,
    batchMode: config.batchMode,
    batchSize: config.batchSize,
    messagesReceived: metrics.messagesReceived,
    messagesStored: metrics.messagesStored,
    messagesFailed: metrics.messagesFailed,
    pendingBatch: batchBuffer.length,
    dbRowCount: parseInt(dbResult.rows[0].count, 10),
    lastStoredAt: metrics.lastStoredAt,
  });
});

app.post('/flush', async (_req, res) => {
  await flushBatch();
  res.json({ flushed: true, pendingBatch: batchBuffer.length });
});

process.on('SIGTERM', async () => {
  await flushBatch();
  await pool.end();
  process.exit(0);
});

async function main() {
  await waitForPostgres();
  await startConsumer();
  app.listen(config.port, () => {
    console.log(`Storage service listening on port ${config.port} (batchMode=${config.batchMode})`);
  });
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
