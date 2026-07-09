const config = require('./config');
const { waitForDatabase } = require('./db');
const { startMqttSubscriber } = require('./subscribers/mqttSubscriber');
const { startKafkaSubscriber } = require('./subscribers/kafkaSubscriber');

async function main() {
  console.log(`Data Storage starting with broker: ${config.brokerType}`);
  await waitForDatabase();

  if (config.brokerType === 'kafka') {
    await startKafkaSubscriber();
  } else {
    startMqttSubscriber();
  }
}

main().catch((error) => {
  console.error(`Data Storage failed to start: ${error.message}`);
  process.exit(1);
});
