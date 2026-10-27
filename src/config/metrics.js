import client from "prom-client";

export const register = new client.Registry();
register.setDefaultLabels({
  service: "plusepoll",
  environment: process.env.NODE_ENV || "development",
});
client.collectDefaultMetrics({ register });

export const httpRequestsTotal = new client.Counter({
  name: "http_requests_total",
  help: "Total HTTP requests, excluding the metrics endpoint.",
  labelNames: ["method", "route", "status_code"],
  registers: [register],
});

export const httpRequestDuration = new client.Histogram({
  name: "http_request_duration_seconds",
  help: "HTTP request duration in seconds.",
  labelNames: ["method", "route", "status_code"],
  buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
  registers: [register],
});

export const kafkaMessagesProcessed = new client.Counter({
  name: "kafka_messages_processed_total",
  help: "Vote events successfully processed by the Kafka worker.",
  registers: [register],
});
export const kafkaProcessingFailures = new client.Counter({
  name: "kafka_processing_failures_total",
  help: "Vote event processing failures.",
  registers: [register],
});
export const kafkaProcessingDuration = new client.Histogram({
  name: "kafka_processing_duration_seconds",
  help: "Kafka vote event processing duration in seconds.",
  registers: [register],
});
export const kafkaProducerFailures = new client.Counter({
  name: "kafka_producer_publish_failures_total",
  help: "Kafka producer publish failures.",
  registers: [register],
});
export const votesPersisted = new client.Counter({
  name: "votes_persisted_total",
  help: "Votes successfully persisted.",
  registers: [register],
});
export const duplicateVotes = new client.Counter({
  name: "duplicate_votes_rejected_total",
  help: "Duplicate votes rejected by the database uniqueness constraint.",
  registers: [register],
});
export const invalidVoteEvents = new client.Counter({
  name: "invalid_vote_events_total",
  help: "Invalid vote events rejected by the worker.",
  registers: [register],
});
export const deadLetterMessages = new client.Counter({
  name: "dead_letter_messages_total",
  help: "Vote events sent to the dead-letter topic.",
  registers: [register],
});
export const redisCacheOperations = new client.Counter({
  name: "redis_cache_operations_total",
  help: "Poll-list cache operations.",
  labelNames: ["result"],
  registers: [register],
});
export const postgresOperationErrors = new client.Counter({
  name: "postgres_operation_errors_total",
  help: "PostgreSQL operation errors.",
  registers: [register],
});
