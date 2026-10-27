import test from "node:test";
import assert from "node:assert/strict";
import { app } from "../src/server.js";

test("health and metrics endpoints respond without external services", async () => {
  const server = app.listen(0);
  await new Promise((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });
  const address = server.address();
  const baseUrl = `http://127.0.0.1:${address.port}`;

  try {
    const health = await fetch(`${baseUrl}/health`);
    assert.equal(health.status, 200);
    assert.equal((await health.json()).status, "alive");

    const metrics = await fetch(`${baseUrl}/metrics`);
    assert.equal(metrics.status, 200);
    assert.match(metrics.headers.get("content-type"), /text\/plain/);
    assert.match(await metrics.text(), /http_request_duration_seconds/);
  } finally {
    await new Promise((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

test("vote route requires authentication and pagination is validated", async () => {
  const server = app.listen(0);
  await new Promise((resolve, reject) => {
    server.once("listening", resolve);
    server.once("error", reject);
  });
  const baseUrl = `http://127.0.0.1:${server.address().port}`;

  try {
    const vote = await fetch(
      `${baseUrl}/api/polls/550e8400-e29b-41d4-a716-446655440000/votes`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" },
    );
    assert.equal(vote.status, 401);

    const list = await fetch(`${baseUrl}/api/polls?page=0`);
    assert.equal(list.status, 400);
  } finally {
    await new Promise((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});
