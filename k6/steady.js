// k6 load test — steady traffic at the rate limit boundary
// Run: k6 run k6/steady.js
// Docs: https://k6.io/docs/

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const denied = new Counter('requests_denied');
const deniedRate = new Rate('deny_rate');
const latency = new Trend('request_latency', true);

export const options = {
  stages: [
    { duration: '30s', target: 50 },   // ramp up
    { duration: '2m',  target: 50 },   // steady at 50 VUs
    { duration: '30s', target: 0 },    // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(99)<200'],  // 99% of requests under 200ms
    deny_rate: ['rate<0.5'],           // less than 50% denied (healthy)
  },
};

export default function () {
  const res = http.get('http://localhost:8080/api/hello', {
    headers: {
      'X-User-ID': `user-${Math.floor(Math.random() * 10)}`, // 10 distinct users
    },
  });

  latency.add(res.timings.duration);

  const ok = check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'has ratelimit headers': (r) => r.headers['X-Ratelimit-Limit'] !== undefined,
  });

  if (res.status === 429) {
    denied.add(1);
    deniedRate.add(1);
  } else {
    deniedRate.add(0);
  }

  sleep(0.1);
}
