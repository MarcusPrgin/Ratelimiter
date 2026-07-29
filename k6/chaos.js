// Traffic while Redis is killed and restarted.
//
//   terminal 1: k6 run k6/chaos.js
//   terminal 2: make redis-stop     # watch the breaker open
//               make redis-start    # watch the half-open probe close it
//
// What this checks depends on the configured fallback strategy, and the difference
// is the point:
//
//   fail_open      — traffic keeps flowing, unmetered. Expect 200s throughout.
//   fail_closed    — traffic is refused with 503, not admitted. Expect 503s.
//   local_fallback — each node enforces from memory. Expect a mix of 200 and 429.
//
// A 500 is a bug under all three. So is a hang: with the circuit breaker working, a
// request during an outage should not wait out the Redis timeout, so latency must
// stay low even while Redis is down.

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const STRATEGY = __ENV.STRATEGY || 'fail_open';

const ok = new Counter('chaos_ok');
const throttled = new Counter('chaos_throttled');
const unavailable = new Counter('chaos_unavailable');
const serverErrors = new Counter('chaos_server_errors');
const transitions = new Counter('chaos_transitions');
const latency = new Trend('chaos_latency', true);

export const options = {
  vus: Number(__ENV.VUS || 20),
  duration: __ENV.DURATION || '2m',
  thresholds: {
    // 429 and 503 are considered failures by k6's default http_req_failed, so assert
    // on the explicit checks instead of that metric.
    'checks{check:no server error}': ['rate==1.0'],
    'checks{check:responded}': ['rate==1.0'],
    // The circuit breaker is what makes this hold. Without it, every request during
    // the outage waits out the full Redis timeout first.
    'chaos_latency': ['p(99)<1000'],
  },
};

let lastStatus = 0;

export default function () {
  const res = http.get(`${BASE}/api/hello`, {
    headers: { 'X-User-ID': `chaos-${__VU}` },
    timeout: '5s',
    tags: { name: 'api_hello' },
  });

  check(res, {
    responded: (r) => r.status > 0,
    'no server error': (r) => r.status !== 500 && r.status !== 502 && r.status !== 504,
  });

  if (res.status > 0) {
    latency.add(res.timings.duration);
  }

  switch (res.status) {
    case 200:
      ok.add(1);
      break;
    case 429:
      throttled.add(1);
      break;
    case 503:
      unavailable.add(1);
      break;
    default:
      if (res.status >= 500 || res.status === 0) {
        serverErrors.add(1);
      }
  }

  // Count changes in outcome, which is where Redis went down or came back.
  if (lastStatus !== 0 && res.status !== lastStatus) {
    transitions.add(1);
  }
  lastStatus = res.status;

  sleep(0.2);
}

export function handleSummary(data) {
  const count = (name) => (data.metrics[name] ? data.metrics[name].values.count : 0);
  const p99 = data.metrics.chaos_latency
    ? data.metrics.chaos_latency.values['p(99)'].toFixed(1)
    : 'n/a';

  const expectation = {
    fail_open: '200s throughout; the limit is not enforced while Redis is down',
    fail_closed: '503s while Redis is down; no request is admitted unmetered',
    local_fallback: 'a mix of 200 and 429; each node enforces from its own memory',
  }[STRATEGY] || 'set STRATEGY to describe what to expect';

  return {
    stdout: [
      '',
      '  chaos results',
      '  ─────────────',
      `  strategy under test  ${STRATEGY}`,
      `  expected             ${expectation}`,
      '',
      `  200 admitted         ${count('chaos_ok')}`,
      `  429 throttled        ${count('chaos_throttled')}`,
      `  503 unavailable      ${count('chaos_unavailable')}`,
      `  5xx / no response    ${count('chaos_server_errors')}   <- must be 0`,
      `  outcome transitions  ${count('chaos_transitions')}`,
      `  latency p99          ${p99} ms   <- should stay low even during the outage`,
      '',
    ].join('\n'),
  };
}
