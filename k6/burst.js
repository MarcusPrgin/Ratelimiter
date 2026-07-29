// One caller bursting far past its limit.
//
// Run: k6 run k6/burst.js
//
// This is the case the limiter exists for: a single key generating several times its
// quota. It checks two things — that the excess is refused, and that the admitted
// count stays close to the configured limit rather than drifting above it, which is
// what caching decisions locally instead of leasing quota would cause.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const RPS = Number(__ENV.RPS || 600);
const DURATION_S = Number(__ENV.DURATION_S || 10);

// The service's configured limit, so admitted traffic can be judged against it.
const LIMIT = Number(__ENV.LIMIT || 100);

const allowed = new Counter('burst_allowed');
const denied = new Counter('burst_denied');
const enforced = new Rate('limit_enforced');

export const options = {
  scenarios: {
    burst: {
      // constant-arrival-rate sends exactly `rate` requests per second in total.
      // constant-vus would send that many per virtual user, so the load would be
      // multiplied by the VU count and every number below would be meaningless.
      executor: 'constant-arrival-rate',
      rate: RPS,
      timeUnit: '1s',
      duration: `${DURATION_S}s`,
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
  thresholds: {
    // Only 200 and 429 are acceptable. A 5xx means a bug.
    'checks{check:status is 200 or 429}': ['rate==1.0'],
    'checks{check:refusals carry Retry-After}': ['rate==1.0'],
    // At several times the limit, most traffic must be refused.
    limit_enforced: ['rate>0.5'],
  },
};

export default function () {
  const res = http.get(`${BASE}/api/hello`, {
    // One key for every request: a single caller hammering the service.
    headers: { 'X-User-ID': 'burst-single-caller' },
    tags: { name: 'api_hello' },
  });

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
    'refusals carry Retry-After': (r) =>
      r.status !== 429 || r.headers['Retry-After'] !== undefined,
  });

  if (res.status === 429) {
    denied.add(1);
    enforced.add(true);
  } else if (res.status === 200) {
    allowed.add(1);
    enforced.add(false);
  }
}

export function handleSummary(data) {
  const count = (name) => (data.metrics[name] ? data.metrics[name].values.count : 0);
  const a = count('burst_allowed');
  const d = count('burst_denied');
  const budget = LIMIT * DURATION_S;

  return {
    stdout: [
      '',
      '  burst results',
      '  ─────────────',
      `  admitted             ${a}`,
      `  refused              ${d}`,
      `  enforcement          ${a + d > 0 ? ((d / (a + d)) * 100).toFixed(1) : '0.0'}%`,
      `  quota for the run    ~${budget}  (${LIMIT}/s × ${DURATION_S}s)`,
      // Materially above 1.0 means quota was handed out that Redis never counted.
      `  admitted / quota     ${(a / budget).toFixed(2)}x`,
      '',
    ].join('\n'),
  };
}
