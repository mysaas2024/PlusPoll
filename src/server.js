import "dotenv/config";
import { randomUUID } from "node:crypto";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import express from "express";
import authRoutes from "./routes/authRoutes.js";
import pollRoutes from "./routes/pollRoutes.js";
import limiter from "./middlewares/ratelimiter.js";
import { CloseRabbitMQ, ConnectRabbit } from "./config/rabbitmq.config.js";
import { prisma } from "./lib/prisma.js";
import redis, { closeRedis, connectRedis } from "./config/redis.config.js";
import { logger } from "./config/logger.js";
import {
  httpRequestDuration,
  httpRequestsTotal,
  register,
} from "./config/metrics.js";
import {
  connectProducer,
  disconnectProducer,
  isProducerConnected,
} from "./kafka/producer.js";

const app = express();
const port = Number(process.env.PORT || 5000);
const requestTimeoutMs = Number(process.env.HEALTH_CHECK_TIMEOUT_MS || 2000);

function boundedCheck(check) {
  let timer;
  return Promise.race([
    check(),
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("Dependency check timed out")), requestTimeoutMs);
      timer.unref();
    }),
  ]).finally(() => clearTimeout(timer));
}

app.disable("x-powered-by");
app.use((req, res, next) => {
  req.requestId = randomUUID();
  res.setHeader("X-Request-Id", req.requestId);
  const startedAt = process.hrtime.bigint();
  res.once("finish", () => {
    if (req.path === "/metrics") return;
    const routePath = req.route?.path
      ? `${req.baseUrl}${req.route.path}`
      : req.path === "/" ? "/" : "unmatched";
    const labels = {
      method: req.method,
      route: String(routePath).replace(/:[^/]+/g, ":param"),
      status_code: String(res.statusCode),
    };
    const duration = Number(process.hrtime.bigint() - startedAt) / 1e9;
    httpRequestsTotal.inc(labels);
    httpRequestDuration.observe(labels, duration);
    logger.info(
      {
        requestId: req.requestId,
        method: req.method,
        route: labels.route,
        statusCode: res.statusCode,
        durationMs: Number((duration * 1000).toFixed(2)),
      },
      "HTTP request completed",
    );
  });
  next();
});
app.use(limiter);
app.use(express.json({ limit: "64kb" }));

app.get("/health", (_req, res) => {
  res.status(200).json({ success: true, status: "alive" });
});

app.get("/ready", async (_req, res) => {
  const dependencies = { postgres: false, redis: false, kafka: false };
  const results = await Promise.allSettled([
    boundedCheck(async () => {
      await prisma.$queryRaw`SELECT 1`;
      dependencies.postgres = true;
    }),
    boundedCheck(async () => {
      if (!redis.isReady) throw new Error("Redis is not ready");
      await redis.ping();
      dependencies.redis = true;
    }),
    boundedCheck(async () => {
      if (!isProducerConnected()) await connectProducer();
      if (!isProducerConnected()) throw new Error("Kafka producer is not connected");
      dependencies.kafka = true;
    }),
  ]);
  if (results.some((result) => result.status === "rejected")) {
    return res.status(503).json({ success: false, status: "not_ready", dependencies });
  }
  return res.status(200).json({ success: true, status: "ready", dependencies });
});

app.get("/metrics", async (_req, res, next) => {
  try {
    res.set("Content-Type", register.contentType);
    res.end(await register.metrics());
  } catch (error) {
    next(error);
  }
});

app.use("/auth", authRoutes);
app.use("/poll", pollRoutes);
app.use("/api/polls", pollRoutes);
app.get("/", (_req, res) => {
  res.status(200).json({ success: true, message: "The API is working" });
});
app.use((_req, res) => {
  res.status(404).json({ success: false, message: "Route not found." });
});
app.use((error, req, res, _next) => {
  if (res.headersSent) return;
  const status = Number.isInteger(error.status) && error.status >= 400 && error.status < 500
    ? error.status
    : 500;
  logger.error(
    { err: error, requestId: req.requestId, statusCode: status },
    "HTTP request failed",
  );
  return res.status(status).json({
    success: false,
    message: status === 500 ? "Internal server error." : "Invalid request.",
  });
});

export async function startServer() {
  if (!process.env.JWT_SECRET) {
    throw new Error("JWT_SECRET must be configured before starting the API");
  }
  await prisma.$connect();
  try {
    await boundedCheck(connectRedis);
  } catch (error) {
    logger.warn({ err: error }, "Starting API without Redis cache");
  }
  try {
    await connectProducer();
  } catch (error) {
    logger.error({ err: error }, "Kafka unavailable; vote endpoint will return 503");
  }
  try {
    await boundedCheck(ConnectRabbit);
  } catch (error) {
    logger.warn({ err: error }, "Starting API without RabbitMQ email queue");
  }

  const server = app.listen(port, () => {
    logger.info({ port }, "PlusePoll API listening");
  });
  let shuttingDown = false;
  const shutdown = async (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, "API shutdown started");
    const closeHttpServer = new Promise((resolveClose, reject) => {
      server.close((error) => (error ? reject(error) : resolveClose()));
    });
    let timeout;
    await Promise.race([
      closeHttpServer,
      new Promise((resolveTimeout) => {
        timeout = setTimeout(() => {
          logger.error("HTTP shutdown timed out; closing active connections");
          server.closeAllConnections();
          process.exitCode = 1;
          resolveTimeout();
        }, 10000);
        timeout.unref();
      }),
    ]).catch((error) => {
      logger.error({ err: error }, "HTTP server shutdown failed");
      process.exitCode = 1;
    });
    clearTimeout(timeout);

    const results = await Promise.allSettled([
      disconnectProducer(),
      CloseRabbitMQ(),
      closeRedis(),
      prisma.$disconnect(),
    ]);
    for (const [index, result] of results.entries()) {
      if (result.status === "rejected") {
        logger.error(
          { err: result.reason, dependency: ["kafka", "rabbitmq", "redis", "postgres"][index] },
          "Dependency shutdown failed",
        );
        process.exitCode = 1;
      }
    }
  };
  process.once("SIGINT", () => void shutdown("SIGINT"));
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
  return server;
}

export { app };

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  startServer().catch(async (error) => {
    logger.fatal({ err: error }, "API failed to start");
    await prisma.$disconnect().catch((disconnectError) => {
      logger.error({ err: disconnectError }, "Failed to disconnect Prisma after startup error");
    });
    process.exitCode = 1;
  });
}
