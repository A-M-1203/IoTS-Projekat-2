const mqtt = require('mqtt');
const config = require('../config');
const { insertReading } = require('../db');

function startMqttSubscriber() {
  const url = `mqtt://${config.mqtt.host}:${config.mqtt.port}`;
  const client = mqtt.connect(url, {
    reconnectPeriod: 3000,
  });

  client.on('connect', () => {
    console.log(`MQTT connected to ${url}`);
    client.subscribe(config.mqtt.topic, (err) => {
      if (err) {
        console.error(`MQTT subscribe failed: ${err.message}`);
        return;
      }
      console.log(`Subscribed to MQTT topic: ${config.mqtt.topic}`);
    });
  });

  client.on('message', async (_topic, payload) => {
    try {
      const reading = JSON.parse(payload.toString());
      await insertReading(reading);
      console.log(`Stored reading for device ${reading.device_id}, temperature=${reading.temperature}`);
    } catch (error) {
      console.error(`Failed to store MQTT message: ${error.message}`);
    }
  });

  client.on('error', (error) => {
    console.error(`MQTT error: ${error.message}`);
  });

  return client;
}

module.exports = { startMqttSubscriber };
