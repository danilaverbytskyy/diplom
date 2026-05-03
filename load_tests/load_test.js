import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 1 },
    { duration: '20s', target: 5 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.05'],
    http_req_duration: ['p(95)<10000'],
  },
};

const baseUrl = __ENV.BASE_URL || 'http://nginx';
const titleId = __ENV.TITLE_ID || '1';

const endpoints = [
  '/api/titles/',
  '/api/titles/top/?min_votes=10000',
  '/api/titles/search/?q=matrix',
  `/api/titles/${titleId}/full/`,
];

export default function () {
  const endpoint = endpoints[Math.floor(Math.random() * endpoints.length)];

  const response = http.get(`${baseUrl}${endpoint}`, {
    timeout: '30s',
  });

  check(response, {
    'status is 200': (result) => result.status === 200,
  });

  sleep(1);
}