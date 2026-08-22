import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const TOPIC = __ENV.TOPIC || 'demo-messages';
const DURATION = __ENV.DURATION || '30s';
const VUS = parseInt(__ENV.VUS || '10', 10);

export const options = {
    scenarios: {
        constant_load: {
            executor: 'constant-vus',
            vus: VUS,
            duration: DURATION,
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],
        checks: ['rate>0.95'],
    },
};

const body = () => JSON.stringify({
    topic: TOPIC,
    key: `key-${Date.now()}-${Math.random().toString(36).substring(7)}`,
    value: JSON.stringify({
        content: `Load test message at ${new Date().toISOString()}`,
        vus: __VU,
        iteration: __ITER,
    }),
});

export default function () {
    const res = http.post(`${BASE_URL}/api/kafka/send`, body(), {
        headers: { 'Content-Type': 'application/json' },
    });

    check(res, {
        'status is 200': (r) => r.status === 200,
        'has partition': (r) => r.json('partition') !== undefined,
        'has offset': (r) => r.json('offset') !== undefined,
        'topic matches': (r) => r.json('topic') === TOPIC,
    });

    sleep(0.1);
}