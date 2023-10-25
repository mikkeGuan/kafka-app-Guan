import { Kafka, Partitioners } from 'kafkajs';
import { v4 as UUID } from 'uuid';

console.log("*** Producer starts... ***");

const kafka = new Kafka({
    clientId: 'my-checking-client',
    brokers: ['localhost:9092']
})

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

const run = async () => {
    await producer.connect();

    setInterval(() => {
        queueMessage();
    }, 2500)
}

run().catch(console.error);

function randomizeIntegerBetween(from, to) {
    return (Math.floor(Math.random() * (to - from + 1))) + from;
}

async function queueMessage() {
    const fahrenheitTemperature = randomizeIntegerBetween(0, 100); 

    const success = await producer.send({
        topic: 'tobechecked',
        messages: [
            {
                value: Buffer.from(`${fahrenheitTemperature}°F`), 
            },
        ],
    });

    if (success) {
        console.log(`Message ${fahrenheitTemperature}°F successfully sent to the stream`);
    } else {
        console.log('Problem writing to stream.. ');
    }
}
