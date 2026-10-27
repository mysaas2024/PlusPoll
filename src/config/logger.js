import pino from "pino";

export const logger = pino({
  level: process.env.LOG_LEVEL || "info",
  base: {
    service: "plusepoll",
    environment: process.env.NODE_ENV || "development",
  },
  redact: {
    paths: [
      "req.headers.authorization",
      "req.headers.cookie",
      "password",
      "passwordHash",
      "token",
      "secret",
    ],
    censor: "[REDACTED]",
  },
});
