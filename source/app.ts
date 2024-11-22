import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
// Determine the environment and load the corresponding .env file
const envFile = `.env.${process.env.NODE_ENV}`;

// Load environment variables from .env file
dotenv.config({path: envFile});

const BROCKER_LISTS = [
  process.env.FIRST_BROKER!,
  process.env.SECOND_BROKER!,
  process.env.THIRD_BROKER!,
  process.env.FOURTH_BROKER!,
  process.env.FIFTH_BROKER!,
  process.env.SIXTH_BROKER!,
];

const CONFIGURATIONS = {
  SSL: (process.env.SSL! === 'true'),
  SASL: undefined
};

console.log("------------------------");
console.log(BROCKER_LISTS);
console.log("------------------------");

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: BROCKER_LISTS,
  ssl: CONFIGURATIONS['SSL'],
  sasl: CONFIGURATIONS['SASL'], // Set this if SASL is required.
});

const producer = kafka.producer();

async function produceMessage() {
  await producer.connect();
  console.log('Producer connected');
  const topic = 'test-topic';
  let times = 1
  while (times <= 1000) {
    // Send a message
    await producer.send({
      topic,
      messages: [
        { key: 'key0', value: 'Message for Partition 1' },
        { key: 'key1', value: 'Message for Partition 2' },
        { key: 'key2', value: 'Message for Partition 3' },
      ],
    });
    console.log("Sent");
    await sleep(1000);
    times = times + 1;
  }

  console.log('Message sent');
  await producer.disconnect();
}

produceMessage().catch((err) => {
  console.error('Error in producer:', err);
  producer.disconnect();
});

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
};
