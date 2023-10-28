import { Kafka, Consumer, ConsumerSubscribeTopic, Producer } from "kafkajs";
import { config } from "./config";

let kafkaBrokers = config.kafkaHosts;

if (config.kafkaSource === "DE") {
    kafkaBrokers = config.kafkaHostsDE;
}

const kafka = new Kafka({
    clientId: "consumer-app",
    brokers: kafkaBrokers,
});

// const errorTypes = ['unhandledRejection', 'uncaughtException'];
// const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

export class KafkaConsumer {
    dlqProducer: Producer;
    private consumer: Consumer;

    constructor(groupId: string, sessionTimeout = 60000, rebalanceTimeout = 120000, heartbeatInterval = 3000) {
        this.consumer = kafka.consumer({ groupId, sessionTimeout, rebalanceTimeout, allowAutoTopicCreation: false, heartbeatInterval});
        this.dlqProducer = kafka.producer();
    }

    async subscribe(handler: (msg: any[]) => Promise<void>, topicsToSubscribe: any[]) {

        const consumerOpts = topicsToSubscribe[0].opts;
        const useBatch = consumerOpts.batch || false;
        const numberOfConcurrentPartitions = consumerOpts.numberOfConcurrentPartitions || 1;
        const autoCommitAfterNumberOfMessages = consumerOpts.autoCommitAfterNumberOfMessages || 500;
        const autoCommitIntervalInMs = consumerOpts.autoCommitIntervalInMs || 500;

        await this.consumer.connect();
        for (let i = 0; i < topicsToSubscribe.length; i++) {

            console.log("Consumer subscribe", topicsToSubscribe[i].opts.topic);
            await this.consumer.subscribe(topicsToSubscribe[i].opts);

        }
        await this.dlqProducer.connect();

        if (useBatch) {
            return this.consumer.run({
                eachBatch: async ({ batch }) => {
                    console.log({
                        partition: batch.partition,
                        firstOffset: batch.firstOffset(),
                        lastOffset: batch.lastOffset(),
                        values: batch.messages.map(x => x.value.toString()),
                    });
                    try {
                        await handler(batch.messages.map(x => JSON.parse(x.value.toString())));
                    } catch (e) {
                        console.error(e);
                        await this.dlqProducer.send({
                            topic: `${batch.topic}.dlq`,
                            messages: batch.messages.map(x => ({ ...x, partition: 0 })),
                        });
                    }
                },
            });
        }
        return this.consumer.run({
            autoCommitInterval: autoCommitIntervalInMs,
            autoCommitThreshold: autoCommitAfterNumberOfMessages,
            partitionsConsumedConcurrently: numberOfConcurrentPartitions,
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    console.log({
                        partition,
                        offset: message.offset,
                        value: message.value.toString(),
                        timestamp: message.timestamp,
                    });
                    await handler([JSON.parse(message.value.toString())]);
                } catch (e) {
                    console.error(e);
                    await this.dlqProducer.send({
                        topic: `${topic}.dlq`,
                        messages: [{
                            ...message,
                            partition: 0,
                        }],
                    });
                }
            },
        }).catch(e => kafka.logger().error(`[example/consumer] ${e.message}`, { stack: e.stack }));
    }
}
