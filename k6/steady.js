// Steady traffic around the limit boundary.
//
// Run: k6 run k6/steady.js
//
// Uses constant-arrival-rate, not constant-vus. The two are easy to confuse and the
// difference is large: arrival-rate sends exactly `rate` requests per second in
// total, while constant-vus sends that many per *virtual user*, so 50 VUs produce
// 50× the intended load and every number in the report is meaningless.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';

// Aim above the configured limit (100/s by default) so the limiter is exercised.
const TARGET_RPS = Number(__ENV.RPS || 150);
const USERS = Number(__ENV.USERS || 20);

const allowed = new Counter('rl_allowed');
const denied = new Counter('rl_denied');
const denyRate = new Rate('deny_rate');
const remaining = new Trend('rl_remaining');

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: TARGET_RPS,
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    // Only 200 and 429 are acceptable here. A 500 is a bug; a 503 means the limiter
    // gave up, which should not happen with a healthy Redis.
    'checks{check:status is 200 or 429}': ['rate==1.0'],
    'checks{check:rate limit headers present}': ['rate==1.0'],
    'checks{check:refusals carry Retry-After}': ['rate==1.0'],
    http_req_duration: ['p(99)<200'],
    // Above the configured limit some traffic must be refused; if none is, the
    // limiter is not enforcing anything.
    deny_rate: ['rate>0.05'],
  },
};

export default function () {
  const res = http.get(`${BASE}/api/hello`, {
    headers: { 'X-User-ID': `steady-${__VU % USERS}` },
    tags: { name: 'api_hello' },
  });

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'rate limit headers present': (r) =>
      r.headers['X-Ratelimit-Limit'] !== undefined &&
      r.headers['X-Ratelimit-Remaining'] !== undefined,
    // A refusal has to say when to come back, or the client retries immediately and
    // adds to the load being shed.
    'refusals carry Retry-After': (r) =>
      r.status !== 429 || r.headers['Retry-After'] !== undefined,
  });

  if (res.status === 429) {
    denied.add(1);
    denyRate.add(true);
    return;
  }

  allowed.add(1);
  denyRate.add(false);
  const rem = res.headers['X-Ratelimit-Remaining'];
  if (rem !== undefined) {
    remaining.add(Number(rem));
  }
}
