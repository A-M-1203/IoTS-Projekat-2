CREATE TABLE IF NOT EXISTS sensor_readings (
    id              BIGSERIAL PRIMARY KEY,
    message_id      UUID UNIQUE,
    timestamp       TIMESTAMP NOT NULL,
    device_id       VARCHAR(50) NOT NULL,
    location        VARCHAR(100),
    crop_type       VARCHAR(50),
    season          VARCHAR(20),
    temperature     DOUBLE PRECISION,
    humidity        DOUBLE PRECISION,
    rainfall        DOUBLE PRECISION,
    soil_moisture   DOUBLE PRECISION,
    soil_ph         DOUBLE PRECISION,
    light_intensity DOUBLE PRECISION,
    fertilizer_used DOUBLE PRECISION,
    irrigation_needed SMALLINT,
    crop_health     VARCHAR(50),
    yield_estimate  DOUBLE PRECISION,
    pest_risk       VARCHAR(20),
    anomaly_flag    SMALLINT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_device_timestamp
    ON sensor_readings (device_id, timestamp);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_ingested_at
    ON sensor_readings (ingested_at);
