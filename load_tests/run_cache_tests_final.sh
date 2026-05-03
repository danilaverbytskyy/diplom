#!/usr/bin/env bash

set -euo pipefail

HOST_BASE_URL="${HOST_BASE_URL:-http://localhost:8000}"
K6_BASE_URL="${K6_BASE_URL:-http://nginx}"
TITLE_ID="${TITLE_ID:-1}"
RESULTS_DIR="${RESULTS_DIR:-results}"
COMPOSE_FILE="${COMPOSE_FILE:-../docker-compose.yml}"
WARMUP_ENABLED="${WARMUP_ENABLED:-1}"

mkdir -p "${RESULTS_DIR}"

log() {
  echo
  echo ">>> $1"
}

check_api_available() {
  log "Check API availability: ${HOST_BASE_URL}"

  curl -sSf "${HOST_BASE_URL}/api/cache/status/" \
    > "${RESULTS_DIR}/cache_status_initial.json"
}

set_cache_mode() {
  local mode="$1"
  local scenario="$2"

  log "Set cache mode: ${mode}"

  curl -sSf -X POST "${HOST_BASE_URL}/api/cache/mode/" \
    -H 'Content-Type: application/json' \
    -d '{"mode":"'"${mode}"'"}' \
    > "${RESULTS_DIR}/cache_status_${scenario}_${mode}_before_clear.json"
}

clear_cache() {
  local mode="$1"
  local scenario="$2"

  log "Clear all cache for scenario=${scenario}, mode=${mode}"

  curl -sSf -X POST "${HOST_BASE_URL}/api/cache/clear/" \
    > "${RESULTS_DIR}/cache_status_${scenario}_${mode}_after_clear.json"

  curl -sSf "${HOST_BASE_URL}/api/cache/status/" \
    > "${RESULTS_DIR}/cache_status_${scenario}_${mode}_after_version_sync.json"
}

show_cache_status() {
  local mode="$1"
  local scenario="$2"

  curl -sSf "${HOST_BASE_URL}/api/cache/status/" \
    > "${RESULTS_DIR}/cache_status_${scenario}_${mode}_after_test.json"
}

run_k6_warmup() {
  local mode="$1"
  local scenario="$2"
  local script="$3"

  if [ "${WARMUP_ENABLED}" != '1' ]; then
    log 'Warmup disabled'
    return
  fi

  log "Warmup cache: scenario=${scenario}, mode=${mode}"

  docker compose -f "${COMPOSE_FILE}" run --rm \
    -e BASE_URL="${K6_BASE_URL}" \
    -e TITLE_ID="${TITLE_ID}" \
    k6 run "/scripts/${script}" \
    > "${RESULTS_DIR}/warmup_${scenario}_${mode}.log" 2>&1 || true
}

run_k6_measurement() {
  local mode="$1"
  local scenario="$2"
  local script="$3"

  log "Run measurement: scenario=${scenario}, mode=${mode}"

  docker compose -f "${COMPOSE_FILE}" run --rm \
    -e BASE_URL="${K6_BASE_URL}" \
    -e TITLE_ID="${TITLE_ID}" \
    k6 run \
      --summary-export "/scripts/${RESULTS_DIR}/results_${scenario}_${mode}.json" \
      "/scripts/${script}" \
    | tee "${RESULTS_DIR}/run_${scenario}_${mode}.log"
}

run_mode() {
  local scenario="$1"
  local script="$2"
  local mode="$3"

  echo
  echo '=================================================='
  echo "Scenario: ${scenario}; cache mode: ${mode}"
  echo '=================================================='

  set_cache_mode "${mode}" "${scenario}"
  clear_cache "${mode}" "${scenario}"
  run_k6_warmup "${mode}" "${scenario}" "${script}"
  run_k6_measurement "${mode}" "${scenario}" "${script}"
  show_cache_status "${mode}" "${scenario}"
}

run_scenario() {
  local scenario="$1"
  local script="$2"

  for mode in off local redis multi
  do
    run_mode "${scenario}" "${script}" "${mode}"
  done
}

check_api_available

log 'Initial full cache clear'
curl -sSf -X POST "${HOST_BASE_URL}/api/cache/clear/" \
  > "${RESULTS_DIR}/cache_status_global_clear.json"

run_scenario 'light' 'load_light.js'
run_scenario 'heavy' 'load_heavy.js'

log 'All tests completed'