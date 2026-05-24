// k6 burst test — hammers one user key to prove the limiter holds
// Run: k6 run k6/burst.js
// This is the test you show in interviews to prove the limiter works.

import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const denied = new Counter('burst_denied');
const allowed = new Counter('burst_allowed');
const enforced = new Rate('limit_enforced');

export const options = {
  // Single user, 300 requests in 1 second = 300 rps
  // With limit=100/s, ~200 should be denied
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 300,
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 100,
      maxVUs: 300,
    },
  },
  thresholds: {
    // We EXPECT high deny rate here — that's the point
    limit_enforced: ['rate>0.5'], // more than 50% denied proves limiter is working
  },
};

export default function () {
  const res = http.get('http://localhost:8080/api/hello', {
    headers: {
      'X-User-ID': 'burst-test-user', // single key — hits limit fast
    },
  });

  check(res, {
    'status is 200 or 429': (r) => r.status === 200 || r.status === 429,
  });

  if (res.status === 429) {
    denied.add(1);
    enforced.add(1);
  } else {
    allowed.add(1);
    enforced.add(0);
  }
}
