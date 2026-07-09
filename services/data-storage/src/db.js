const { Pool } = require('pg');
const config = require('./config');

const pool = new Pool(config.postgres);

const INSERT_QUERY = `
  INSERT INTO sensor_readings (
    timestamp, device_id, location, crop_type, season,
    temperature, humidity, rainfall, soil_moisture, soil_ph,
    light_intensity, fertilizer_used, irrigation_needed,
    crop_health, yield_estimate, pest_risk, anomaly_flag
  ) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9, $10,
    $11, $12, $13,
    $14, $15, $16, $17
  )
`;

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

async function insertReading(reading) {
  const values = [
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

  await pool.query(INSERT_QUERY, values);
}

module.exports = {
  pool,
  waitForDatabase,
  insertReading,
};
