import { Kafka, logLevel } from "kafkajs";
import { logger } from "../config/logger.js";
import { kafkaProducerFailures } from "../config/metrics.js";
import { assertVoteEvent } from "./voteEvent.js";

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "plusepoll-api",
  brokers: (process.env.KAFKA_BROKERS || "127.0.0.1:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean),
  logLevel: logLevel.NOTHING,
  retry: { retries: 5, initialRetryTime: 300, maxRetryTime: 30000 },
});

const producer = kafka.producer({
  allowAutoTopicCreation: false,
  idempotent: true,
  maxInFlightRequests: 5,
});
let connected = false;
let connecting;

export const voteTopic = process.env.KAFKA_VOTE_TOPIC || "poll.votes";
export const deadLetterTopic =
  process.env.KAFKA_VOTE_DLQ_TOPIC || "poll.votes.dlq";

export async function connectProducer() {
  if (connected) return;
  if (!connecting) {
    connecting = producer.connect().then(() => {
      connected = true;
      logger.info("Kafka vote producer connected");
    }).finally(() => {
      connecting = undefined;
    });
  }
  await connecting;
}

export async function publishVote(event) {
  assertVoteEvent(event);
  try {
    await connectProducer();
    await producer.send({
      topic: voteTopic,
      acks: -1,
      messages: [
        {
          key: event.pollId,
          value: JSON.stringify(event),
          headers: { "event-type": event.eventType },
        },
      ],
    });
  } catch (error) {
    kafkaProducerFailures.inc();
    logger.error(
      { err: error, eventId: event.eventId, topic: voteTopic },
      "Failed to publish vote event",
    );
    throw error;
  }
}

export async function publishDeadLetter(message) {
  await connectProducer();
  await producer.send({
    topic: deadLetterTopic,
    acks: -1,
    messages: [
      {
        key: message.event?.pollId || null,
        value: JSON.stringify(message),
        headers: { "event-type": "poll.vote.dead-letter" },
      },
    ],
  });
}

export async function disconnectProducer() {
  if (connecting) await connecting.catch(() => {});
  if (connected) {
    await producer.disconnect();
    connected = false;
  }
}

export function isProducerConnected() {
  return connected;
}
