import { redis  } from "../../modules";

async function getByKey(key) {
    return redis.getAsync(key);
}

export const redisOperations = {
    getByKey,
};
