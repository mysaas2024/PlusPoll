import { prisma } from "../lib/prisma.js";
import redis from "../config/redis.config.js";
import { logger } from "../config/logger.js";
import { postgresOperationErrors, redisCacheOperations } from "../config/metrics.js";
import { publishVote } from "../kafka/producer.js";
import { createVoteEvent } from "../kafka/voteEvent.js";

const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const listRequests = new Map();
const configuredCacheTtl = Number.parseInt(process.env.POLL_CACHE_TTL_SECONDS || "15", 10);
const cacheTtlSeconds = Number.isFinite(configuredCacheTtl)
  ? Math.min(Math.max(configuredCacheTtl, 1), 120)
  : 15;

export function isPollOpen(poll, now = new Date()) {
  return (
    poll.status === "ACTIVE" &&
    (!poll.startsAt || poll.startsAt <= now) &&
    (!poll.endsAt || poll.endsAt > now)
  );
}

function normalizeOptions(value) {
  if (!Array.isArray(value)) return null;
  return value.map((option) => {
    const text = typeof option === "string" ? option : option?.text;
    return typeof text === "string" ? text.trim() : "";
  });
}

export function validatePollInput(body) {
  const title = body?.question ?? body?.title;
  const description = body?.description ?? null;
  const options = normalizeOptions(body?.options);
  const valid =
    typeof title === "string" &&
    Boolean(title.trim()) &&
    title.trim().length <= 200 &&
    (description === null || typeof description === "string") &&
    (description === null || description.length <= 5000) &&
    options !== null &&
    options.length >= 2 &&
    options.length <= 50 &&
    options.every((option) => option && option.length <= 200) &&
    new Set(options.map((option) => option.toLocaleLowerCase())).size === options.length;

  return valid ? { title: title.trim(), description: description?.trim() || null, options } : null;
}

async function getPollList(cacheKey, where, skip, take) {
  if (redis.isReady) {
    try {
      const cached = await redis.get(cacheKey);
      if (cached) {
        redisCacheOperations.labels("hit").inc();
        return JSON.parse(cached);
      }
      redisCacheOperations.labels("miss").inc();
    } catch (error) {
      redisCacheOperations.labels("error").inc();
      logger.warn({ err: error }, "Poll-list cache read failed");
    }
  } else {
    redisCacheOperations.labels("error").inc();
  }

  if (listRequests.has(cacheKey)) return listRequests.get(cacheKey);
  const query = (async () => {
    try {
      const [polls, total] = await prisma.$transaction([
        prisma.poll.findMany({
          where,
          skip,
          take,
          orderBy: { createdAt: "desc" },
          select: {
            id: true,
            title: true,
            description: true,
            status: true,
            startsAt: true,
            endsAt: true,
            createdAt: true,
            options: {
              select: {
                id: true,
                text: true,
                _count: { select: { votes: true } },
              },
            },
          },
        }),
        prisma.poll.count({ where }),
      ]);
      const result = {
        data: polls,
        pagination: {
          page: Math.floor(skip / take) + 1,
          limit: take,
          total,
          totalPages: Math.ceil(total / take),
        },
      };

      if (redis.isReady) {
        try {
          await redis.set(cacheKey, JSON.stringify(result), { EX: cacheTtlSeconds });
        } catch (error) {
          redisCacheOperations.labels("error").inc();
          logger.warn({ err: error }, "Poll-list cache write failed");
        }
      }
      return result;
    } finally {
      listRequests.delete(cacheKey);
    }
  })();
  listRequests.set(cacheKey, query);
  return query;
}

export async function createPoll(req, res) {
  const input = validatePollInput(req.body);
  if (!input) {
    return res.status(400).json({
      success: false,
      message: "Provide a question and at least two unique, non-empty options.",
    });
  }
  if (!req.user?.id || !uuidPattern.test(req.user.id)) {
    return res.status(401).json({ success: false, message: "Authentication required." });
  }

  try {
    const poll = await prisma.poll.create({
      data: {
        title: input.title,
        description: input.description,
        creatorId: req.user.id,
        options: { create: input.options.map((text) => ({ text })) },
      },
      include: { options: true },
    });
    if (redis.isReady) {
      try {
        await redis.incr("poll:list:version");
      } catch (error) {
        logger.warn({ err: error, pollId: poll.id }, "Poll-list cache invalidation failed");
      }
    }
    return res.status(201).json({ success: true, data: poll });
  } catch (error) {
    postgresOperationErrors.inc();
    logger.error({ err: error }, "Poll creation failed");
    return res.status(500).json({ success: false, message: "Unable to create poll." });
  }
}

export async function listPolls(req, res) {
  const page = Number(req.query.page ?? 1);
  const limit = Number(req.query.limit ?? 5);
  if (
    !Number.isSafeInteger(page) ||
    page < 1 ||
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit > 50
  ) {
    return res.status(400).json({
      success: false,
      message: "page must be a positive integer and limit must be between 1 and 50.",
    });
  }

  const now = new Date();
  const where = {
    status: "ACTIVE",
    AND: [
      { OR: [{ startsAt: null }, { startsAt: { lte: now } }] },
      { OR: [{ endsAt: null }, { endsAt: { gt: now } }] },
    ],
  };
  try {
    let version = "offline";
    if (redis.isReady) {
      try {
        version = (await redis.get("poll:list:version")) || "0";
      } catch (error) {
        redisCacheOperations.labels("error").inc();
        logger.warn({ err: error }, "Poll-list cache version lookup failed");
      }
    }
    const cacheKey = `poll:list:v${version}:page${page}:limit${limit}`;
    const result = await getPollList(cacheKey, where, (page - 1) * limit, limit);
    return res.status(200).json({ success: true, ...result });
  } catch (error) {
    postgresOperationErrors.inc();
    logger.error({ err: error }, "Poll listing failed");
    return res.status(500).json({ success: false, message: "Unable to list polls." });
  }
}

export async function getPoll(req, res) {
  const { pollId } = req.params;
  if (!uuidPattern.test(pollId)) {
    return res.status(400).json({ success: false, message: "Invalid poll ID." });
  }

  try {
    const poll = await prisma.poll.findUnique({
      where: { id: pollId },
      select: {
        id: true,
        title: true,
        description: true,
        status: true,
        startsAt: true,
        endsAt: true,
        createdAt: true,
        options: {
          select: {
            id: true,
            text: true,
            _count: { select: { votes: true } },
          },
        },
      },
    });
    if (!poll) {
      return res.status(404).json({ success: false, message: "Poll not found." });
    }
    return res.status(200).json({ success: true, data: poll });
  } catch (error) {
    postgresOperationErrors.inc();
    logger.error({ err: error, pollId }, "Poll retrieval failed");
    return res.status(500).json({ success: false, message: "Unable to retrieve poll." });
  }
}

export async function voteForPoll(req, res) {
  const { pollId } = req.params;
  const { optionId } = req.body || {};
  if (!uuidPattern.test(pollId) || typeof optionId !== "string" || !uuidPattern.test(optionId)) {
    return res.status(400).json({ success: false, message: "Invalid poll ID or option ID." });
  }
  if (!req.user?.id || !uuidPattern.test(req.user.id)) {
    return res.status(401).json({ success: false, message: "Authentication required." });
  }

  try {
    const poll = await prisma.poll.findUnique({
      where: { id: pollId },
      select: {
        id: true,
        status: true,
        startsAt: true,
        endsAt: true,
        options: { where: { id: optionId }, select: { id: true } },
      },
    });
    if (!poll) {
      return res.status(404).json({ success: false, message: "Poll not found." });
    }
    if (!isPollOpen(poll)) {
      return res.status(409).json({ success: false, message: "Voting is not currently open." });
    }
    if (!poll.options.length) {
      return res.status(400).json({
        success: false,
        message: "The selected option does not belong to this poll.",
      });
    }
    const previousVote = await prisma.vote.findFirst({
      where: { pollId, userId: req.user.id },
      select: { id: true },
    });
    if (previousVote) {
      return res.status(409).json({ success: false, message: "You have already voted in this poll." });
    }

    const event = createVoteEvent({ pollId, optionId, userId: req.user.id });
    await publishVote(event);
    return res.status(202).json({
      success: true,
      message: "Vote accepted for asynchronous processing; results may take a short time to update.",
      eventId: event.eventId,
    });
  } catch (error) {
    logger.error({ err: error, pollId }, "Vote submission failed");
    return res.status(503).json({
      success: false,
      message: "Vote service is temporarily unavailable. Please try again.",
    });
  }
}
