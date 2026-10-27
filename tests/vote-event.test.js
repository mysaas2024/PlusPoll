import test from "node:test";
import assert from "node:assert/strict";
import { assertVoteEvent, createVoteEvent } from "../src/kafka/voteEvent.js";

const ids = {
  pollId: "550e8400-e29b-41d4-a716-446655440000",
  optionId: "550e8400-e29b-41d4-a716-446655440001",
  userId: "550e8400-e29b-41d4-a716-446655440002",
};

test("vote event has a stable schema and valid identifiers", () => {
  const event = createVoteEvent(ids);
  assert.equal(event.eventType, "poll.vote");
  assert.equal(event.schemaVersion, 1);
  assertVoteEvent(event);
});

test("vote event validation rejects missing or malformed identifiers", () => {
  assert.throws(() => assertVoteEvent({ ...ids, eventType: "poll.vote" }), TypeError);
  assert.throws(
    () =>
      assertVoteEvent({
        eventId: "not-an-id",
        eventType: "poll.vote",
        ...ids,
        createdAt: new Date().toISOString(),
      }),
    TypeError,
  );
});
