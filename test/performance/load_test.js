import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';

const errorRate = new Rate('errors');

export let options = {
  stages: [
    { duration: '2m', target: 10 },
    { duration: '5m', target: 50 }, 
    { duration: '2m', target: 100 },
    { duration: '5m', target: 100 },
    { duration: '2m', target: 200 },
    { duration: '5m', target: 200 },
    { duration: '2m', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<1000'],
    errors: ['rate<0.1'],
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

const samplePredictionData = {
  county: 'Los Angeles County',
  ten_year_age_groups: '75-84 years',
  gender: 'Female', 
  year: 2020,
  population: 50000,
  state: 'California'
};

const sampleBatchData = {
  data: Array(10).fill(samplePredictionData)
};

export default function() {
  const healthCheck = http.get(`${BASE_URL}/api/v1/health/`);
  check(healthCheck, {
    'health check status is 200': (r) => r.status === 200,
    'health check response time < 500ms': (r) => r.timings.duration < 500,
  }) || errorRate.add(1);

  const mortalityData = http.get(`${BASE_URL}/api/v1/mortality/data?limit=10`);
  check(mortalityData, {
    'mortality data status is 200': (r) => r.status === 200,
    'mortality data response time < 2s': (r) => r.timings.duration < 2000,
  }) || errorRate.add(1);

  const summaryStats = http.get(`${BASE_URL}/api/v1/mortality/statistics/summary`);
  check(summaryStats, {
    'summary stats status is 200': (r) => r.status === 200,
    'summary stats response time < 3s': (r) => r.timings.duration < 3000,
  }) || errorRate.add(1);

  const trendsData = http.get(`${BASE_URL}/api/v1/mortality/statistics/trends?start_year=2010&end_year=2020`);
  check(trendsData, {
    'trends data status is 200': (r) => r.status === 200,
    'trends data response time < 5s': (r) => r.timings.duration < 5000,
  }) || errorRate.add(1);

  const highRiskAreas = http.get(`${BASE_URL}/api/v1/mortality/high-risk-areas?limit=20`);
  check(highRiskAreas, {
    'high risk areas status is 200': (r) => r.status === 200,
    'high risk areas response time < 3s': (r) => r.timings.duration < 3000,
  }) || errorRate.add(1);

  if (__ITER % 10 === 0) {
    const prediction = http.post(
      `${BASE_URL}/api/v1/mortality/predict`,
      JSON.stringify(samplePredictionData),
      { headers: { 'Content-Type': 'application/json' } }
    );
    check(prediction, {
      'prediction status is 200 or 404': (r) => r.status === 200 || r.status === 404,
      'prediction response time < 2s': (r) => r.timings.duration < 2000,
    }) || errorRate.add(1);
  }

  if (__ITER % 50 === 0) {
    const batchPrediction = http.post(
      `${BASE_URL}/api/v1/mortality/predict/batch`,
      JSON.stringify(sampleBatchData),
      { headers: { 'Content-Type': 'application/json' } }
    );
    check(batchPrediction, {
      'batch prediction status is 200 or 404': (r) => r.status === 200 || r.status === 404,
      'batch prediction response time < 10s': (r) => r.timings.duration < 10000,
    }) || errorRate.add(1);
  }

  sleep(1);
}

export function handleSummary(data) {
  return {
    'performance-report.json': JSON.stringify(data, null, 2),
    stdout: '\n' + JSON.stringify(data.metrics, null, 2) + '\n\n',
  };
}