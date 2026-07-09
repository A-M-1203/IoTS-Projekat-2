const { Pool } = require('pg');
const config = require('./config');
const {
  incrementStored,
  incrementErrors,
  setBuffered,
  incrementBatchesFlushed,
} = require('./metrics');

const pool = new Pool(config.postgres);

const INSERT_COLUMNS = `
  timestamp, device_id, location, crop_type, season,
  temperature, humidity, rainfall, soil_moisture, soil_ph,
  light_intensity, fertilizer_used, irrigation_needed,
  crop_health, yield_estimate, pest_risk, anomaly_flag
`;

let buffer = [];

function readingToValues(reading) {
  return [
    reading.timestamp,
    reading.device_id,
    reading.location,
    reading.crop_type,
    reading.season,
    reading.temperature,
    reading.humidity,
    reading.rainfall,
    reading.soil_moisture,
    reading.soil_ph,
    reading.light_intensity,
    reading.fertilizer_used,
    reading.irrigation_needed,
    reading.crop_health,
    reading.yield_estimate,
    reading.pest_risk,
    reading.anomaly_flag,
  ];
}

async function waitForDatabase(maxAttempts = 30) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await pool.query('SELECT 1');
      console.log('Connected to PostgreSQL.');
      return;
    } catch (error) {
      console.log(`PostgreSQL not ready (attempt ${attempt}/${maxAttempts}): ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 2000));
    }
  }

  throw new Error('Could not connect to PostgreSQL.');
}

async function flushBuffer() {
  if (buffer.length === 0) {
    return 0;
  }

  const batch = buffer.splice(0, buffer.length);
  setBuffered(0);

  const values = [];
  const params = [];

  batch.forEach((reading, index) => {
    const offset = index * 17;
    const placeholders = Array.from({ length: 17 }, (_, i) => `$${offset + i + 1}`);
    values.push(`(${placeholders.join(', ')})`);
    params.push(...readingToValues(reading));
  });

  const query = `INSERT INTO sensor_readings (${INSERT_COLUMNS}) VALUES ${values.join(', ')}`;

  try {
    await pool.query(query, params);
    incrementStored(batch.length);
    incrementBatchesFlushed();
    console.log(`Flushed batch of ${batch.length} readings to PostgreSQL.`);
    return batch.length;
  } catch (error) {
    incrementErrors();
    console.error(`Failed to flush batch of ${batch.length}: ${error.message}`);
    throw error;
  }
}

async function bufferReading(reading) {
  buffer.push(reading);
  setBuffered(buffer.length);

  if (buffer.length >= config.batchSize) {
    await flushBuffer();
  }
}

async function insertReading(reading) {
  if (config.batchSize <= 1) {
    const query = `INSERT INTO sensor_readings (${INSERT_COLUMNS}) VALUES (${Array.from({ length: 17 }, (_, i) => `$${i + 1}`).join(', ')})`;
    await pool.query(query, readingToValues(reading));
    incrementStored(1);
    return;
  }

  await bufferReading(reading);
}

async function drainBuffer() {
  return flushBuffer();
}

module.exports = {
  pool,
  waitForDatabase,
  insertReading,
  drainBuffer,
};
