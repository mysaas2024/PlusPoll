import { randomUUID } from "node:crypto";

const uuidPattern =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function createVoteEvent({ pollId, optionId, userId }) {
  const event = {
    eventId: randomUUID(),
    eventType: "poll.vote",
    pollId,
    optionId,
    userId,
    createdAt: new Date().toISOString(),
    schemaVersion: 1,
  };

  assertVoteEvent(event);
  return event;
}

export function assertVoteEvent(event) {
  if (
    !event ||
    typeof event !== "object" ||
    typeof event.eventId !== "string" ||
    !uuidPattern.test(event.eventId) ||
    event.eventType !== "poll.vote" ||
    !uuidPattern.test(event.pollId || "") ||
    !uuidPattern.test(event.optionId || "") ||
    !uuidPattern.test(event.userId || "") ||
    typeof event.createdAt !== "string" ||
    Number.isNaN(Date.parse(event.createdAt)) ||
    (event.schemaVersion !== undefined && event.schemaVersion !== 1)
  ) {
    throw new TypeError("Invalid poll vote event");
  }
}
