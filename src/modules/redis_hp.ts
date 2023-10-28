import Redis from "ioredis";
import bluebird from "bluebird";
import { config } from "./config";

bluebird.promisifyAll(Redis);

const redisHPClient = config.redis_hp.hosts.length > 1
    ? new Redis.Cluster(config.redis_hp.hosts.map(host => ({ host, port: 6379 })), { redisOptions: { password: config.redis_hp.password, showFriendlyErrorStack: true } })
    : new Redis({
        host: config.redis_hp.hosts[0], port: 6379, password: config.redis_hp.password, showFriendlyErrorStack: true,
    });

export const redis_hp = redisHPClient;
