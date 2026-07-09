const { Kafka } = require('kafkajs');
const config = require('../config');
const { insertReading } = require('../db');

async function startKafkaSubscriber() {
  const kafka = new Kafka({
    clientId: 'data-storage',
    brokers: [config.kafka.bootstrapServers],
    retry: {
      retries: 10,
      initialRetryTime: 3000,
    },
  });

  const consumer = kafka.consumer({ groupId: config.kafka.groupId });

  while (true) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });
      console.log(`Subscribed to Kafka topic: ${config.kafka.topic}`);
      break;
    } catch (error) {
      console.log(`Kafka connection failed: ${error.message}. Retrying in 3s...`);
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const reading = JSON.parse(message.value.toString());
        await insertReading(reading);
        console.log(`Stored reading for device ${reading.device_id}, temperature=${reading.temperature}`);
      } catch (error) {
        console.error(`Failed to store Kafka message: ${error.message}`);
      }
    },
  });

  return consumer;
}

module.exports = { startKafkaSubscriber };
