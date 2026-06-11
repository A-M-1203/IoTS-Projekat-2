const mqtt = require('mqtt');

function createMqttConsumer(config, onMessage) {
  return new Promise((resolve, reject) => {
    const url = `mqtt://${config.host}:${config.port}`;
    const client = mqtt.connect(url, {
      reconnectPeriod: 5000,
      clean: false,
      clientId: `storage-service-${Date.now()}`,
    });

    client.on('connect', () => {
      client.subscribe(config.topic, { qos: config.qos }, (err) => {
        if (err) return reject(err);
        resolve(client);
      });
    });

    client.on('message', (_topic, payload) => {
      try {
        const data = JSON.parse(payload.toString());
        onMessage(data);
      } catch (err) {
        console.error('MQTT parse error:', err.message);
      }
    });

    client.on('error', (err) => {
      console.error('MQTT error:', err.message);
    });
  });
}

module.exports = { createMqttConsumer };
