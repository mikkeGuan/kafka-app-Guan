import { Kafka } from "kafkajs";

console.log("*** Consumer starts... ***");

const kafka = new Kafka({
    clientId: "checker-server",
    brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({ groupId: "kafka-checker-servers1" });

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: "tobechecked", fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const fahrenheitTemperature = parseFloat(message.value.toString().replace('°F', ''));
            const celsiusTemperature = ((fahrenheitTemperature - 32) * 5) / 9;

            console.log({
                offset: message.offset,
                value: `${celsiusTemperature.toFixed(2)}°C`,
            });
        },
    });
};

run().catch(console.error);
