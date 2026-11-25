#!/usr/bin/env bash
set -euo pipefail
export COMPOSE_PARALLEL_LIMIT="${COMPOSE_PARALLEL_LIMIT:-1}"
cd "$(dirname "$0")/.."
project="cinema-hw5-test-$$"
compose=(docker compose -p "$project" -f docker-compose.yml -f docker-compose.test.yml)
cleanup() {
  result=$?
  if [ "$result" -ne 0 ]; then
    "${compose[@]}" logs --tail=100
  fi
  "${compose[@]}" down -v --remove-orphans
  exit "$result"
}
trap cleanup EXIT
"${compose[@]}" up -d --build --wait --wait-timeout 300
"${compose[@]}" run --rm --no-deps tests
"${compose[@]}" stop kafka-1
"${compose[@]}" run --rm --no-deps tests python -m tests.failover_check
"${compose[@]}" start kafka-1
"${compose[@]}" stop minio
"${compose[@]}" run --rm --no-deps tests python -m tests.export_recovery unavailable
"${compose[@]}" up -d --wait --no-deps minio
"${compose[@]}" run --rm --no-deps tests python -m tests.export_recovery recovered
echo 'All tests passed, including Kafka failover and S3 recovery.'
