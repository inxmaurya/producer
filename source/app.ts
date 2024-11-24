import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import murmurhash from "murmurhash";
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
console.log(CONFIGURATIONS);
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

  // // Send a message with a specific key
  // const key = "key0";
  // const numPartitions = 3;
  // const partition = calculatePartition(key, numPartitions);

  let times = 1
  while (times <= 1000) {
    // Send a message
    await producer.send({
      topic,
      messages: [
        { key: 'key0', value: getRandomMessage() },
        { key: 'key1', value: getRandomMessage() },
        { key: 'key2', value: getRandomMessage() },
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

// Murmur2 hash function
function calculatePartition(key: string, numPartitions: number): number {
  const hash = murmurhash.v3(key);
  return Math.abs(hash) % numPartitions;
}


const MESSAGES: string[] = [
  "Hello, world!",
  "TypeScript is awesome!",
  "Random message generated.",
  "Keep learning and coding!",
  "Node.js is powerful!",
  "Express.js is powerful!",
  "Ruby is awesome!",
  "Ruby on Rails is awesome!",
  "Pacific Ocrean",
  "Atlantic Ocrean",
  "Hind Ocrean",
  "Antarcatica Ocrean",
];

function getRandomMessage(): string {
  const randomIndex = Math.floor(Math.random() * MESSAGES.length);
  return MESSAGES[randomIndex];
}
