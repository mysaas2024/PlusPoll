import { randomInt, randomUUID } from "node:crypto";
import { getRabbitMQchannel } from "../config/rabbitmq.config.js";
import redis from "../config/redis.config.js";

const EXCHANGE = "email.reset";
const QUEUE = "email.reset.queue";
const ROUTING_KEY = "reset.email.key";

export async function PasswordRestJob({ to, body, subject }) {
  const channel = await getRabbitMQchannel();
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);

  const otp = randomInt(10_000_000, 100_000_000).toString();
  if (!redis.isReady) throw new Error("Redis is required to store a password reset code");
  await redis.set(`OPT:${to}`, otp, { EX: 500 });

  const message = {
    id: randomUUID(),
    type: "reset.password",
    data: { to, subject, body },
    createdAt: new Date().toISOString(),
  };
  const accepted = channel.publish(
    EXCHANGE,
    ROUTING_KEY,
    Buffer.from(JSON.stringify(message)),
    { persistent: true, contentType: "application/json" },
  );
  if (!accepted) throw new Error("RabbitMQ did not accept the password reset job");
  await channel.waitForConfirms();
  return { id: message.id };
}
