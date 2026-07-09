const { Kafka } = require('kafkajs');
const config = require('../config');
const { insertReading } = require('../db');
const { incrementReceived, incrementErrors } = require('../metrics');

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
  let lastOffset = null;

  while (true) {
    try {
      await consumer.connect();
      console.log(`Kafka connected at ${new Date().toISOString()}`);
      await consumer.subscribe({ topic: config.kafka.topic, fromBeginning: true });
      console.log(`SUBSCRIBED at ${new Date().toISOString()} to Kafka topic: ${config.kafka.topic}`);
      break;
    } catch (error) {
      console.log(`Kafka connection failed: ${error.message}. Retrying in 3s...`);
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }

  consumer.on(consumer.events.CONNECT, () => {
    console.log(`RECONNECTED at ${new Date().toISOString()} to Kafka`);
  });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      incrementReceived();
      lastOffset = message.offset;
      try {
        const reading = JSON.parse(message.value.toString());
        await insertReading(reading);
      } catch (error) {
        incrementErrors();
        console.error(
          `Failed to store Kafka message at ${topic}[${partition}] offset ${message.offset}: ${error.message}`,
        );
      }
    },
  });

  setInterval(() => {
    if (lastOffset !== null) {
      console.log(`Kafka last processed offset: ${lastOffset} at ${new Date().toISOString()}`);
    }
  }, 30000);

  return consumer;
}

module.exports = { startKafkaSubscriber };
