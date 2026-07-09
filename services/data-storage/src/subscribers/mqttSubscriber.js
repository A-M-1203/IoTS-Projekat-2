const mqtt = require('mqtt');
const config = require('../config');
const { insertReading } = require('../db');
const { incrementReceived, incrementErrors } = require('../metrics');

function startMqttSubscriber() {
  const url = `mqtt://${config.mqtt.host}:${config.mqtt.port}`;
  const client = mqtt.connect(url, {
    reconnectPeriod: 3000,
  });

  client.on('connect', () => {
    const connectedAt = new Date().toISOString();
    console.log(`MQTT connected to ${url} at ${connectedAt}`);
    if (client.reconnecting) {
      console.log(`RECONNECTED at ${connectedAt}`);
    }

    client.subscribe(config.mqtt.topic, { qos: config.mqtt.subscribeQos }, (err) => {
      if (err) {
        console.error(`MQTT subscribe failed: ${err.message}`);
        return;
      }
      console.log(`SUBSCRIBED at ${new Date().toISOString()} to MQTT topic: ${config.mqtt.topic} (QoS ${config.mqtt.subscribeQos})`);
    });
  });

  client.on('reconnect', () => {
    console.log(`RECONNECTING to MQTT broker at ${new Date().toISOString()}`);
  });

  client.on('message', async (_topic, payload) => {
    incrementReceived();
    try {
      const reading = JSON.parse(payload.toString());
      await insertReading(reading);
    } catch (error) {
      incrementErrors();
      console.error(`Failed to store MQTT message: ${error.message}`);
    }
  });

  client.on('error', (error) => {
    console.error(`MQTT error: ${error.message}`);
  });

  return client;
}

module.exports = { startMqttSubscriber };
