# **dn-consumers**

Repository to create/manage/monitor all the consumers

### Steps to create a new consumer

Create a new directory in src/consumers. The directory name will be consumer name.
The consumer directory should contain index.js which will server as starting point of the consumer.
index.js should export a **_onMsg_** function which needs to be async in nature. This function accepts a msg param.
This file should export 1 variables and the **_onMsg_** function mentioned above:

```
const exports opts = [{
    topic: "<topic to subscribe to>", // Topic Property
    fromBeginning: "<boolean value>", // Topic Property
    numberOfConcurrentPartitions: "<no of partitions 1 consumer concurrently consumes(ideally keep it around 5)>", // Consumer Property
    autoCommitAfterNumberOfMessages: "<no of msgs after which auto commit must be done to kafka>", // Consumer Property
    autoCommitIntervalInMs: "<time in ms after which auto commit must be done to kafka>", // Consumer Property
}];
```
> Array of topics to subscribe to, check push-notification for more info

The repository is containerized and deployed in K8S. 


### To make a new deployment: 

New consumer deployments can be done through Jenkins [Job1](https://build.doubtnut.com/job/PRODUCTION/job/consumer-group-eks-deployment/)

- **CONSUMER_GROUP**: "consumer group you want to subscribe your consumer too"
- **CONSUMER_TOPICS**: "topics you want to subscribe to the consumer group, topics must belong to same consumer directory( NOTE: In case of old consumer_group name check for previously subscribed topics) and pass them along as comma separated value, i.e. test-1,test-2"
- **CONSUMER_NAME**: "directory name of the consumer you created in src/consumers"
- **CONSUMER_COUNT**: "no of consumers to create, calculate it on the basis of the formula: no_of_partitions for the topic on kafka / numberOfConcurrentPartitions, write max of the value for all the topics to subscribe"
- **CONSUMER_SESSION_TIMEOUT**: "kafka consumer session timeout (DEFAULT 60000ms ~ 1min)"
- **CONSUMER_REBALANCE_TIMEOUT**: "kafka consumer rebalancing timeout (DEFAULT 120000ms ~ 2min)"

##### NOTE: 
Only Subscribe Topics To the Consumer Group which have same Consumer directory and for multiple topics subscribed, the CONSUMER Property of 1st topic in list are taken as consumer property for whole bunch

### To deploy changes 
To deploy new changes to a consumer file(topic) which is already deployed on [Job1](https://build.doubtnut.com/job/PRODUCTION/job/consumer-group-eks-deployment/), make a new deployment through Jenkins [Job2](https://build.doubtnut.com/job/PRODUCTION/job/dn-consumers-eks-deployment/) where **CONSUMER_GROUP** is the **CONSUMER_GROUP** on which the topic was already subscribed using [Job1](https://build.doubtnut.com/job/PRODUCTION/job/consumer-group-eks-deployment/)


To test it locally, pass the `CONSUMER_GROUP` and `CONSUMER_TOPICS` you want to run in **_env_** file and run `npm run dev`

#### Active Consumers
- championship-coupons
- hook-events-insert
- push-notification
- vvs-update
- package-creation
