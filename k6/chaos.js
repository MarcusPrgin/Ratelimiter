// k6 chaos test — run this while toggling Redis on/off
// Step 1: k6 run k6/chaos.js
// Step 2: in another terminal: docker compose stop redis
// Step 3: watch the console — you should see fallback behaviour, no panics
// Step 4: docker compose start redis — watch recovery

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate } from 'k6/metrics';

const fallbackHits = new Counter('fallback_hits');
const serverErrors = new Counter('server_errors');
const recoveries = new Counter('recoveries');

export const options = {
  vus: 20,
  duration: '5m',
  thresholds: {
    // Server should NEVER return 5xx — 429 is fine, 500 is not
    http_req_failed: ['rate<0.01'],
  },
};

let wasDown = false;

export default function () {
  const res = http.get('http://localhost:8080/api/hello', {
    headers: { 'X-User-ID': `chaos-user-${__VU}` },
    timeout: '3s',
  });

  const ok = check(res, {
    'no 5xx errors': (r) => r.status < 500,
    'responds at all': (r) => r.status > 0,
  });

  if (res.status >= 500) {
    serverErrors.add(1);
  }

  // detect recovery — was down, now up
  if (wasDown && res.status < 500) {
    recoveries.add(1);
    wasDown = false;
  }
  if (res.status >= 500) {
    wasDown = true;
    fallbackHits.add(1);
  }

  sleep(0.2);
}

export function handleSummary(data) {
  const total = data.metrics.http_reqs.values.count;
  const errors = data.metrics.server_errors?.values?.count || 0;
  const rec = data.metrics.recoveries?.values?.count || 0;

  return {
    stdout: `
╔══════════════════════════════════╗
║        Chaos Test Summary        ║
╠══════════════════════════════════╣
║ Total requests:  ${String(total).padStart(14)} ║
║ 5xx errors:      ${String(errors).padStart(14)} ║
║ Recoveries:      ${String(rec).padStart(14)} ║
╚══════════════════════════════════╝
`,
  };
}
