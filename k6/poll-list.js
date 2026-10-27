import http from "k6/http";
import { check } from "k6";

const baseUrl = __ENV.BASE_URL || "http://localhost:5000";

export const options = {
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
  vus: 10,
  duration: "30s",
};

export default function () {
  const response = http.get(`${baseUrl}/api/polls?page=1&limit=5`);
  check(response, {
    "poll list returns 200": (res) => res.status === 200,
    "poll list includes pagination": (res) => {
      try {
        return Boolean(JSON.parse(res.body).pagination);
      } catch {
        return false;
      }
    },
  });
}
