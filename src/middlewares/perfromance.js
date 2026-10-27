import { performance } from "node:perf_hooks";
import { logger } from "../config/logger.js";

export function RequestTiming(req, res, next) {
  const startedAt = performance.now();
  res.once("finish", () => {
    const routePath = req.route?.path
      ? `${req.baseUrl}${req.route.path}`
      : "unmatched";
    logger.info(
      {
        requestId: req.requestId,
        method: req.method,
        route: String(routePath).replace(/:[^/]+/g, ":param"),
        statusCode: res.statusCode,
        durationMs: Number((performance.now() - startedAt).toFixed(2)),
      },
      "Request timing",
    );
  });
  next();
}
