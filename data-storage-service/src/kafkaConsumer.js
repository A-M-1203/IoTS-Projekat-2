const { Kafka } = require('kafkajs');

async function createKafkaConsumer(config, onMessage) {
  const kafka = new Kafka({
    clientId: 'data-storage-service',
    brokers: config.brokers,
    retry: { retries: 10, initialRetryTime: 3000 },
  });

  const consumer = kafka.consumer({ groupId: config.groupId });

  for (let i = 0; i < 30; i++) {
    try {
      await consumer.connect();
      break;
    } catch {
      console.log('Waiting for Kafka...');
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  await consumer.subscribe({ topic: config.topic, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        await onMessage(data);
      } catch (err) {
        console.error('Kafka parse error:', err.message);
      }
    },
  });

  return consumer;
}

module.exports = { createKafkaConsumer };
