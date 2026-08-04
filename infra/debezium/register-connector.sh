#!/bin/sh
# Creates the CDC topic at the partition count this stack expects if it
# does not exist yet, verifies it landed at that count either way, then
# registers the CDC connector with a running Kafka Connect worker. Waits
# for Connect's REST API to answer before posting, then treats the
# connector already existing (HTTP 409) as success so re-running this on
# every `docker compose up` restart never fails the step. Exits non-zero
# for anything else, including a Connect that never comes up within the
# wait budget.
set -eu

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONNECTOR_CONFIG="${CONNECTOR_CONFIG:-/infra/debezium/connector.json}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
CDC_TOPIC="${CDC_TOPIC:-cdc.sourcedb.files}"
CDC_TOPIC_PARTITIONS="${CDC_TOPIC_PARTITIONS:-3}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-120}"

# Kafka auto-creates a topic at the broker's own default partition count
# the moment anything first produces to a name that does not exist yet.
# If the Debezium connector task ever won the race to be the first writer
# to this topic, it would silently revert to that default (often just 1)
# instead of the configured count, and a single row this consumer cannot
# resolve would then block every other row waiting behind it on the
# topic's only partition. Creating the topic here, before the connector
# is ever registered, wins that race outright rather than merely
# checking who won it; migrator-worker and migrator-coordinator each
# declare this same topic on their own startup too, but this script no
# longer depends on either one of them having done so first, so this
# step, and the connector registration after it, runs the same way
# whether or not that container happens to be up.
echo "Creating ${CDC_TOPIC} with ${CDC_TOPIC_PARTITIONS} partition(s) if it does not already exist"
kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
  --create --if-not-exists --topic "${CDC_TOPIC}" --partitions "${CDC_TOPIC_PARTITIONS}" --replication-factor 1

echo "Verifying ${CDC_TOPIC} exists with ${CDC_TOPIC_PARTITIONS} partition(s)"
if ! describe_output=$(kafka-topics --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --describe --topic "${CDC_TOPIC}" 2>&1); then
  echo "Failed to describe topic ${CDC_TOPIC}; it must already exist before the connector is registered:"
  echo "${describe_output}"
  exit 1
fi

actual_partitions=$(echo "${describe_output}" | head -n 1 | sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p')
if [ -z "${actual_partitions}" ]; then
  echo "Could not read a partition count out of the describe output for ${CDC_TOPIC}:"
  echo "${describe_output}"
  exit 1
fi
if [ "${actual_partitions}" != "${CDC_TOPIC_PARTITIONS}" ]; then
  echo "${CDC_TOPIC} has ${actual_partitions} partition(s), expected ${CDC_TOPIC_PARTITIONS}. Refusing to" \
    "register the connector against a topic that was auto-created at the wrong partition count."
  exit 1
fi
echo "${CDC_TOPIC} confirmed at ${actual_partitions} partition(s)"

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
