import http from "k6/http";
import { check } from "k6";

const baseUrl = __ENV.BASE_URL || "http://localhost:5000";
const pollId = __ENV.POLL_ID;
const optionId = __ENV.OPTION_ID;
const token = __ENV.AUTH_TOKEN;

export const options = {
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<1000"],
  },
  vus: 2,
  duration: "15s",
};

export default function () {
  if (!pollId || !optionId || !token) {
    throw new Error("Set POLL_ID, OPTION_ID, and AUTH_TOKEN before running vote load test.");
  }
  const response = http.post(
    `${baseUrl}/api/polls/${pollId}/votes`,
    JSON.stringify({ optionId }),
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    },
  );
  check(response, {
    "vote is accepted asynchronously": (res) => res.status === 202 || res.status === 409,
  });
}
