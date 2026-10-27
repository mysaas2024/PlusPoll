import { createClient } from "redis";
import { logger } from "./logger.js";

const redis = createClient({
  url: process.env.REDIS_URL || "redis://127.0.0.1:6379",
  socket: {
    reconnectStrategy: (retries) => Math.min(retries * 250, 5000),
  },
});

redis.on("error", (error) => {
  logger.warn({ err: error }, "Redis connection error");
});
redis.on("ready", () => logger.info("Redis connection ready"));
redis.on("reconnecting", () => logger.info("Redis reconnecting"));

export async function connectRedis() {
  if (!redis.isOpen) {
    await redis.connect();
  }
}

export async function closeRedis() {
  if (redis.isOpen) {
    await redis.quit();
  }
}

export default redis;