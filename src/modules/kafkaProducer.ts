import {CompressionTypes, Kafka} from "kafkajs";

const kafkaHosts = process.env.KAFKA_HOSTS ? process.env.KAFKA_HOSTS.split(",") : [];

const kafka = new Kafka({
    clientId: "producer-api-server",
    brokers: kafkaHosts,
});

const producer = kafka.producer();
let connected = false;
producer.connect().then(() => {
    connected = true;
});

producer.on("producer.disconnect", () => {
    connected = false;
});

async function publishRaw(topic, data, partitionNo = null) {
    try {
        if (!connected) {
            await producer.connect();
            connected = true;
        }
        await producer.send({
            topic,
            compression: CompressionTypes.GZIP,
            messages: [{
                partition: partitionNo,
                value: JSON.stringify(data),
            }],
        });
    } catch (e) {
        console.error(e);
    }
}

export {
    publishRaw,
};
