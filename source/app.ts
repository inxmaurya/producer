import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: [
    'b-1.smc.4nooo6.c2.kafka.ap-south-1.amazonaws.com:9094',
    'b-2.smc.4nooo6.c2.kafka.ap-south-1.amazonaws.com:9094',
    'b-3.smc.4nooo6.c2.kafka.ap-south-1.amazonaws.com:9094',
  ],
  ssl: true,
  sasl: undefined, // Set this if SASL is required.
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
        { key: 'key3', value: 'Message for Partition 3' },
      ],
    });
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
