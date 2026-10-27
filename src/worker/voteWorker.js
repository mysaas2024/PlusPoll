import { Kafka, logLevel } from "kafkajs";
import { createServer } from "node:http";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { logger } from "../config/logger.js";
import {
  deadLetterMessages,
  duplicateVotes,
  invalidVoteEvents,
  kafkaMessagesProcessed,
  kafkaProcessingDuration,
  kafkaProcessingFailures,
  postgresOperationErrors,
  votesPersisted,
  register,
} from "../config/metrics.js";
import { prisma } from "../lib/prisma.js";
import {
  closeRedis,
  connectRedis,
  default as redis,
} from "../config/redis.config.js";
import {
  connectProducer,
  deadLetterTopic,
  disconnectProducer,
  publishDeadLetter,
  voteTopic,
} from "../kafka/producer.js";
import { assertVoteEvent } from "../kafka/voteEvent.js";

const kafka = new Kafka({
  clientId: process.env.KAFKA_WORKER_CLIENT_ID || "plusepoll-vote-worker",
  brokers: (process.env.KAFKA_BROKERS || "127.0.0.1:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean),
  logLevel: logLevel.NOTHING,
  retry: { retries: 8, initialRetryTime: 500, maxRetryTime: 30000 },
});
const consumer = kafka.consumer({
  groupId: process.env.KAFKA_VOTE_GROUP_ID || "plusepoll-vote-workers",
});
let stopping = false;
let metricsServer;
const failedDatabaseAttempts = new Map();
const maxDatabaseAttempts = 3;

async function connectRedisWithTimeout() {
  let timer;
  try {
    await Promise.race([
      connectRedis(),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("Redis connection timed out")), 2000);
        timer.unref();
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function isDuplicateVote(error) {
  return error?.code === "P2002";
}

async function sendToDeadLetter({ original, event, reason, partition }) {
  const failure = {
    event,
    originalPayload: original,
    failureReason: reason,
    failedAt: new Date().toISOString(),
    sourceTopic: voteTopic,
    sourcePartition: partition,
  };

  await publishDeadLetter(failure);
  deadLetterMessages.inc();
  invalidVoteEvents.inc();
  logger.warn(
    { eventId: event?.eventId, partition, topic: deadLetterTopic, reason },
    "Rejected vote event sent to dead-letter topic",
  );
}

async function persistVote(event) {
  await prisma.$transaction(async (tx) => {
    const now = new Date();
    const poll = await tx.poll.findFirst({
      where: {
        id: event.pollId,
        status: "ACTIVE",
        AND: [
          { OR: [{ startsAt: null }, { startsAt: { lte: now } }] },
          { OR: [{ endsAt: null }, { endsAt: { gt: now } }] },
        ],
      },
      select: { id: true },
    });
    if (!poll) {
      const error = new Error("Poll is missing or voting is not allowed");
      error.permanent = true;
      throw error;
    }

    const option = await tx.pollOption.findFirst({
      where: { id: event.optionId, pollId: event.pollId },
      select: { id: true },
    });
    if (!option) {
      const error = new Error("Option does not belong to the poll");
      error.permanent = true;
      throw error;
    }

    await tx.vote.create({
      data: {
        pollId: event.pollId,
        optionId: event.optionId,
        userId: event.userId,
      },
    });
  });
}

async function handleVoteMessage({ message, partition }) {
  const startedAt = process.hrtime.bigint();
  const original = message.value?.toString("utf8") || "";
  let event;

  try {
    event = JSON.parse(original);
    assertVoteEvent(event);
  } catch (error) {
    kafkaProcessingFailures.inc();
    await sendToDeadLetter({
      original,
      event,
      reason: "Malformed or invalid vote event",
      partition,
    });
    kafkaProcessingDuration.observe(
      Number(process.hrtime.bigint() - startedAt) / 1e9,
    );
    return;
  }

  try {
    try {
      await persistVote(event);
    } catch (error) {
      if (isDuplicateVote(error)) {
        failedDatabaseAttempts.delete(event.eventId);
        duplicateVotes.inc();
        kafkaMessagesProcessed.inc();
        logger.info(
          { eventId: event.eventId, pollId: event.pollId },
          "Duplicate vote ignored",
        );
        return;
      }

      if (error?.permanent) {
        failedDatabaseAttempts.delete(event.eventId);
        kafkaProcessingFailures.inc();
        await sendToDeadLetter({
          original,
          event,
          reason: error.message,
          partition,
        });
        return;
      }

      postgresOperationErrors.inc();
      kafkaProcessingFailures.inc();
      const attempts = (failedDatabaseAttempts.get(event.eventId) || 0) + 1;
      failedDatabaseAttempts.set(event.eventId, attempts);
      if (attempts >= maxDatabaseAttempts) {
        await sendToDeadLetter({
          original,
          event,
          reason: "Vote persistence failed after bounded retries",
          partition,
        });
        failedDatabaseAttempts.delete(event.eventId);
        return;
      }
      logger.error(
        { err: error, eventId: event.eventId, partition, attempt: attempts },
        "Vote persistence failed; Kafka will retry the message",
      );
      throw error;
    }

    failedDatabaseAttempts.delete(event.eventId);
    votesPersisted.inc();
    kafkaMessagesProcessed.inc();
    try {
      if (redis.isReady) {
        await redis.incr("poll:list:version");
      }
    } catch (error) {
      logger.warn(
        { err: error, eventId: event.eventId },
        "Poll-list cache invalidation failed",
      );
    }
    logger.info(
      { eventId: event.eventId, pollId: event.pollId, partition },
      "Vote persisted",
    );
  } finally {
    kafkaProcessingDuration.observe(
      Number(process.hrtime.bigint() - startedAt) / 1e9,
    );
  }
}

export async function startVoteWorker() {
  if (consumer.isRunning()) {
    throw new Error("Vote worker has already started");
  }
  try {
    const metricsPort = Number(process.env.WORKER_METRICS_PORT || 5001);
    metricsServer = createServer(async (req, res) => {
      if (req.url === "/health") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ success: true, status: "alive" }));
      } else if (req.url === "/metrics") {
        res.writeHead(200, { "content-type": register.contentType });
        res.end(await register.metrics());
      } else {
        res.writeHead(404);
        res.end();
      }
    });
    await new Promise((resolveListen, rejectListen) => {
      metricsServer.once("error", rejectListen);
      metricsServer.listen(metricsPort, "0.0.0.0", resolveListen);
    });
    await prisma.$connect();
    await connectRedisWithTimeout().catch((error) => {
      logger.warn({ err: error }, "Worker starting without Redis");
    });
    await connectProducer();
    await consumer.connect();
    await consumer.subscribe({ topic: voteTopic, fromBeginning: true });
    await consumer.run({
      eachMessage: async (payload) => {
        if (!stopping) {
          await handleVoteMessage(payload);
        }
      },
    });
    logger.info({ topic: voteTopic }, "Vote worker started");
  } catch (error) {
    await stopVoteWorker();
    throw error;
  }
}

export async function stopVoteWorker() {
  stopping = true;
  if (metricsServer?.listening) {
    await new Promise((resolveClose, rejectClose) =>
      metricsServer.close((error) => (error ? rejectClose(error) : resolveClose())),
    ).catch((error) => logger.error({ err: error }, "Failed to close worker metrics server"));
  }
  await consumer.stop().catch((error) => {
    logger.warn({ err: error }, "Failed to stop Kafka consumer cleanly");
  });
  await consumer.disconnect().catch((error) => {
    logger.warn({ err: error }, "Failed to disconnect Kafka consumer");
  });
  await disconnectProducer().catch((error) => {
    logger.error({ err: error }, "Failed to disconnect Kafka producer");
  });
  await closeRedis().catch((error) => {
    logger.warn({ err: error }, "Failed to close Redis");
  });
  await prisma.$disconnect().catch((error) => {
    logger.error({ err: error }, "Failed to disconnect Prisma");
  });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  startVoteWorker().catch((error) => {
    logger.fatal({ err: error }, "Vote worker failed to start");
    process.exitCode = 1;
  });

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.once(signal, async () => {
      logger.info({ signal }, "Vote worker shutting down");
      await stopVoteWorker();
      process.exit(0);
    });
  }
}
