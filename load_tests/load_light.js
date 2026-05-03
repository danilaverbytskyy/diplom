import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const titlesListDuration = new Trend('endpoint_titles_list_duration');
const topTitlesDuration = new Trend('endpoint_top_titles_duration');
const searchDuration = new Trend('endpoint_search_duration');
const fullTitleDuration = new Trend('endpoint_full_title_duration');

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
  {
    name: 'titles_list',
    path: '/api/titles/',
    metric: titlesListDuration,
  },
  {
    name: 'top_titles',
    path: '/api/titles/top/?min_votes=10000',
    metric: topTitlesDuration,
  },
  {
    name: 'search',
    path: '/api/titles/search/?q=matrix',
    metric: searchDuration,
  },
  {
    name: 'full_title',
    path: `/api/titles/${titleId}/full/`,
    metric: fullTitleDuration,
  },
];

export default function () {
  const endpoint = endpoints[Math.floor(Math.random() * endpoints.length)];

  const response = http.get(`${baseUrl}${endpoint.path}`, {
    tags: {
      endpoint: endpoint.name,
      scenario_type: 'light',
    },
    timeout: '30s',
  });

  endpoint.metric.add(response.timings.duration);

  check(response, {
    'status is 200': (result) => result.status === 200,
  });

  sleep(1);
}