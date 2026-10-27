import test from "node:test";
import assert from "node:assert/strict";
import { isPollOpen, validatePollInput } from "../src/controller/poll.conttroller.js";

test("poll creation accepts a valid question and two options", () => {
  assert.deepEqual(
    validatePollInput({ question: "Favorite color?", options: ["Blue", "Green"] }),
    { title: "Favorite color?", description: null, options: ["Blue", "Green"] },
  );
});

test("poll creation rejects missing, blank, duplicate, or undersized options", () => {
  for (const body of [
    {},
    { question: " ", options: ["One", "Two"] },
    { question: "Question", options: ["One"] },
    { question: "Question", options: ["One", ""] },
    { question: "Question", options: ["One", " one "] },
  ]) {
    assert.equal(validatePollInput(body), null);
  }
});

test("poll list input accepts strings and option objects", () => {
  assert.deepEqual(
    validatePollInput({ title: "Q", options: [{ text: "A" }, { text: "B" }] })?.options,
    ["A", "B"],
  );
});

test("voting is constrained by status and opening and closing times", () => {
  const now = new Date("2026-10-27T12:00:00Z");
  assert.equal(isPollOpen({ status: "ACTIVE", startsAt: null, endsAt: null }, now), true);
  assert.equal(
    isPollOpen(
      { status: "ACTIVE", startsAt: new Date("2026-10-27T12:01:00Z"), endsAt: null },
      now,
    ),
    false,
  );
  assert.equal(
    isPollOpen(
      { status: "ACTIVE", startsAt: null, endsAt: new Date("2026-10-27T11:59:00Z") },
      now,
    ),
    false,
  );
});
