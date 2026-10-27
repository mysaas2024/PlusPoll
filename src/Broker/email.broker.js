import { randomUUID } from "node:crypto";
import { getRabbitMQchannel } from "../config/rabbitmq.config.js";

const EXCHANGE = "email.exchange";
const QUEUE = "welcome.mail.queue";
const ROUTING_KEY = "welcome.poll.key";

export async function WelcomeEmailJob({ to, subject, body }) {
  const channel = await getRabbitMQchannel();
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);
  const message = {
    id: randomUUID(),
    type: "welcome.email",
    data: { to, subject, body },
    createdAt: new Date().toISOString(),
  };
  const accepted = channel.publish(
    EXCHANGE,
    ROUTING_KEY,
    Buffer.from(JSON.stringify(message)),
    { persistent: true, contentType: "application/json" },
  );
  if (!accepted) throw new Error("RabbitMQ did not accept the welcome email job");
  await channel.waitForConfirms();
  return message;
}
