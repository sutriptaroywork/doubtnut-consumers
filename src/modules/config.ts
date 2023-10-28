import dotenv from "dotenv";

dotenv.config({ path: ".env" });

export const config = {
    kafkaHosts: process.env.KAFKA_HOSTS.split(","),
    kafkaHostsDE: process.env.KAFKA_HOSTS_DE.split(","),
    kafkaSource: process.env.KAFKA_SOURCE,
    mysql: {
        host: {
            read: process.env.MYSQL_HOST_READ,
            write: process.env.MYSQL_HOST_WRITE,
        },
        user: process.env.MYSQL_USER,
        password: process.env.MYSQL_PASS,
        database: process.env.MYSQL_DATABASE || "classzoo1",
        connectionTimeout: process.env.MYSQL_CONNECTION_TIMEOUT || 10000,
    },
    redis: {
        hosts: process.env.REDIS_CLUSTER_HOSTS.split(","),
        password: process.env.REDIS_CLUSTER_PASS,
    },
    redis_hp: {
        hosts: process.env.REDIS_HP_HOSTS.split(","),
        password: process.env.REDIS_HP_PASS,
    },
    mongo: {
        event: {
            database_url: process.env.MONGO_EVENTS_HOST,
        },
        newton: {
            database_url: process.env.NEWTON_MONGO_HOST,
        },
        studygroup: {
            database_url: process.env.STUDY_GROUP_MONGO_HOST,
        },
        teslaFeed: {
            database_url: process.env.PROD_MONGO_HOST,
        },
    },
    gcp: process.env.GOOGLE_APPLICATION_CREDENTIALS,
    fcm: process.env.GOOGLE_FCM_APLICATION_CREDENTIALS,
    iasVanillaBaseUrl: process.env.IAS_VANILLA_BASE_URL,
    consumer: {
        consumerGroup: process.env.CONSUMER_GROUP,
        consumerName: process.env.CONSUMER_NAME,
        topics: process.env.CONSUMER_TOPICS ? process.env.CONSUMER_TOPICS.split(",").map(element => element.trim()) : [],
        sessionTimeout: process.env.CONSUMER_SESSION_TIMEOUT || 60000,
        rebalanceTimeout: process.env.CONSUMER_REBALANCE_TIMEOUT || 120000,
        heartbeatInterval: process.env.CONSUMER_HEARTBEAT_INTERVAL || 3000,
    },
    youtubeApi: {
        client_secret: process.env.YOUTUBE_CLIENT_SECRET,
        client_id: process.env.YOUTUBE_CLIENT_ID,
    },
    cdnUrl: process.env.CDN_URL,
};
