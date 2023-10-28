import Redis from "ioredis";
import bluebird from "bluebird";
import { config } from "./config";

bluebird.promisifyAll(Redis);

const redisClient = config.redis.hosts.length > 1
    ? new Redis.Cluster(config.redis.hosts.map(host => ({ host, port: 6379 })), { redisOptions: { password: config.redis.password, showFriendlyErrorStack: true } })
    : new Redis({
        host: config.redis.hosts[0], port: 6379, password: config.redis.password, showFriendlyErrorStack: true,
    });

export const redis = redisClient;
