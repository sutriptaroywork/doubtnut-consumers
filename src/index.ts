import _ from "lodash";
import { config } from "./modules";
import { KafkaConsumer } from "./modules";
import { common } from "./helpers/common";

async function start() {
    try {
        const subscriberCodeBase = require(`${__dirname}/consumers/${config.consumer.consumerName}`);
        const topicsToSubscribe = [];
        console.log(subscriberCodeBase);
        for (let j = 0; j < subscriberCodeBase.opts.length; j++) {
            if (!_.includes(config.consumer.topics, subscriberCodeBase.opts[j].topic)) {
                continue;
            }
            topicsToSubscribe.push({
                opts: subscriberCodeBase.opts[j],
            });
        }
        if (topicsToSubscribe.length === 0) {
            process.exit(1);
        }
        if (config.mysql.database === "classzoo1") {
            common.createSNIDMapping();
            const intervalObj = setInterval( async () => {
                await common.createSNIDMapping();
              }, 1000 * 60 * 60 * 3);
        }

        console.log("Topics: ", topicsToSubscribe);
        const consumer = new KafkaConsumer(config.consumer.consumerGroup, +config.consumer.sessionTimeout, +config.consumer.rebalanceTimeout, +config.consumer.heartbeatInterval);
        await consumer.subscribe(subscriberCodeBase.onMsg, topicsToSubscribe);
    } catch (e) {
        console.error(e);
    }
}

start();
