import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import murmurhash from 'murmurhash';

dotenv.config({ path: `.env.${process.env.NODE_ENV}` });

const BROCKER_LISTS = [
  process.env.FIRST_BROKER!,
  process.env.SECOND_BROKER!,
  process.env.THIRD_BROKER!,
  process.env.FOURTH_BROKER!,
  process.env.FIFTH_BROKER!,
  process.env.SIXTH_BROKER!,
];

if (!BROCKER_LISTS.every(Boolean)) {
  throw new Error('One or more Kafka broker addresses are missing from the environment variables.');
}

const CONFIGURATIONS = {
  SSL: process.env.SSL === 'true',
  SASL: process.env.SASL_USERNAME && process.env.SASL_PASSWORD ? {
    mechanism: 'scram-sha-256',
    username: process.env.SASL_USERNAME!,
    password: process.env.SASL_PASSWORD!,
  } : undefined,
};

console.log('Kafka Configuration:');
console.log(`Brokers: ${BROCKER_LISTS.join(', ')}`);
console.log(`SSL: ${CONFIGURATIONS.SSL}`);
console.log(`SASL: ${CONFIGURATIONS.SASL ? 'Enabled' : 'Disabled'}`);

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: BROCKER_LISTS,
  ssl: CONFIGURATIONS.SSL,
  sasl: CONFIGURATIONS.SASL,
});

const producer = kafka.producer({
  retry: {
    retries: 5,
    initialRetryTime: 300,
  },
});

async function produceMessage() {
  try {
    await producer.connect();
    console.log('Producer connected');
    const topic = 'test-topic';

    for (let i = 0; i < 1000; i++) {
      await producer.send({
        topic,
        messages: [
          { key: 'key0', value: getRandomMessage() },
          { key: 'key1', value: getRandomMessage() },
          { key: 'key2', value: getRandomMessage() },
        ],
      });
      console.log(`Batch ${i + 1} sent`);
      await sleep(1000);
    }
  } catch (err) {
    console.error('Error in producer:', err);
  } finally {
    await producer.disconnect();
    console.log('Producer disconnected');
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function getRandomMessage(): string {
  const MESSAGES = [
    'Hello, world!',
    'TypeScript is awesome!',
    'Random message generated.',
    'Keep learning and coding!',
    'Node.js is powerful!',
    'Express.js is powerful!',
    'Ruby is awesome!',
    'Ruby on Rails is awesome!',
    'Pacific Ocean',
    'Atlantic Ocean',
    'Indian Ocean',
    'Antarctic Ocean',
  ];
  return MESSAGES[Math.floor(Math.random() * MESSAGES.length)];
}

produceMessage().catch(err => {
  console.error('Error during message production:', err);
  producer.disconnect();
});

process.on('SIGINT', async () => {
  console.log('Interrupt signal received. Cleaning up...');
  await producer.disconnect();
  process.exit(0);
});
