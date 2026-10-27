import amqp from "amqplib";
import { logger } from "./logger.js";

const exchanges = ["email.exchange", "email.reset"];
let connection;
let channel;
let connecting;

export async function ConnectRabbit() {
  if (channel) return channel;
  if (!process.env.RABBITMQ_URL) {
    throw new Error("RABBITMQ_URL is not configured");
  }
  if (!connecting) {
    connecting = (async () => {
      const newConnection = await amqp.connect(process.env.RABBITMQ_URL);
      try {
        const newChannel = await newConnection.createConfirmChannel();
        for (const exchange of exchanges) {
          await newChannel.assertExchange(exchange, "direct", { durable: true });
        }
        newConnection.on("error", (error) => {
          logger.error({ err: error }, "RabbitMQ connection error");
        });
        connection = newConnection;
        channel = newChannel;
        logger.info({ exchanges }, "RabbitMQ email exchanges are ready");
      } catch (error) {
        await newConnection.close().catch(() => {});
        throw error;
      }
    })().finally(() => {
      connecting = undefined;
    });
  }
  await connecting;
  return channel;
}

export async function getRabbitMQchannel() {
  if (!channel) {
    throw new Error("RabbitMQ channel is not connected");
  }
  return channel;
}

export async function CloseRabbitMQ() {
  const activeChannel = channel;
  const activeConnection = connection;
  channel = undefined;
  connection = undefined;
  const results = await Promise.allSettled([
    activeChannel?.close(),
    activeConnection?.close(),
  ]);
  const failure = results.find((result) => result.status === "rejected");
  if (failure) throw failure.reason;
}
