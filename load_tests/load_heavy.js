import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const discoverDuration = new Trend('endpoint_discover_duration');
const topGenresDuration = new Trend('endpoint_top_genres_duration');

export const options = {
  stages: [
    { duration: '10s', target: 1 },
    { duration: '20s', target: 3 },
    { duration: '10s', target: 0 },
  ],
  thresholds: {
    http_req_failed: ['rate<0.10'],
    http_req_duration: ['p(95)<30000'],
  },
};

const baseUrl = __ENV.BASE_URL || 'http://nginx';

const endpoints = [
  {
    name: 'discover',
    path: '/api/titles/discover/?genre=Drama&year_from=2000&ordering=-rating',
    metric: discoverDuration,
  },
  {
    name: 'top_genres',
    path: '/api/analytics/top-genres/',
    metric: topGenresDuration,
  },
];

export default function () {
  const endpoint = endpoints[Math.floor(Math.random() * endpoints.length)];

  const response = http.get(`${baseUrl}${endpoint.path}`, {
    tags: {
      endpoint: endpoint.name,
      scenario_type: 'heavy',
    },
    timeout: '60s',
  });

  endpoint.metric.add(response.timings.duration);

  check(response, {
    'status is 200': (result) => result.status === 200,
  });

  sleep(2);
}