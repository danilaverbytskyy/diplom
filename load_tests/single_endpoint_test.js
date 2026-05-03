import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 1,
  duration: '30s',
};

const baseUrl = __ENV.BASE_URL || 'http://nginx';

export default function () {
  const response = http.get(`${baseUrl}/api/titles/`, {
    timeout: '30s',
  });

  check(response, {
    'status is 200': (result) => result.status === 200,
  });

  sleep(1);
}