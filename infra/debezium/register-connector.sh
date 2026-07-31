#!/bin/sh
# Registers the CDC connector with a running Kafka Connect worker. Waits
# for Connect's REST API to answer before posting, then treats the
# connector already existing (HTTP 409) as success so re-running this on
# every `docker compose up` restart never fails the step. Exits non-zero
# for anything else, including a Connect that never comes up within the
# wait budget.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTOR_CONFIG="${CONNECTOR_CONFIG:-/infra/debezium/connector.json}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-120}"

waited=0
echo "Waiting for Kafka Connect at ${CONNECT_URL}"
until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
  if [ "${waited}" -ge "${MAX_WAIT_SECONDS}" ]; then
    echo "Kafka Connect did not become reachable within ${MAX_WAIT_SECONDS}s"
    exit 1
  fi
  sleep 2
  waited=$((waited + 2))
done
echo "Kafka Connect is reachable, registering ${CONNECTOR_CONFIG}"

response_file=$(mktemp)
http_status=$(curl -s -o "${response_file}" -w "%{http_code}" \
  -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d @"${CONNECTOR_CONFIG}")

if [ "${http_status}" = "201" ]; then
  echo "Connector registered"
  exit 0
fi

if [ "${http_status}" = "409" ]; then
  echo "Connector already exists, treating as success"
  exit 0
fi

echo "Connector registration failed with HTTP ${http_status}:"
cat "${response_file}"
exit 1
