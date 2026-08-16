#!/usr/bin/env bash
set -euo pipefail

: "${CLUSTER_ID:?CLUSTER_ID is required}"
: "${KAFKA_NODE_ID:?KAFKA_NODE_ID is required}"
: "${KAFKA_CONTROLLER_QUORUM_VOTERS:?KAFKA_CONTROLLER_QUORUM_VOTERS is required}"
: "${KAFKA_LISTENERS:?KAFKA_LISTENERS is required}"
: "${KAFKA_ADVERTISED_LISTENERS:?KAFKA_ADVERTISED_LISTENERS is required}"
: "${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:?KAFKA_LISTENER_SECURITY_PROTOCOL_MAP is required}"

config_file=/opt/kafka/config/server.properties
log_dir=/tmp/kafka-logs

printf '%s\n' \
  'process.roles=broker,controller' \
  "node.id=${KAFKA_NODE_ID}" \
  "controller.quorum.voters=${KAFKA_CONTROLLER_QUORUM_VOTERS}" \
  "listeners=${KAFKA_LISTENERS}" \
  "advertised.listeners=${KAFKA_ADVERTISED_LISTENERS}" \
  "listener.security.protocol.map=${KAFKA_LISTENER_SECURITY_PROTOCOL_MAP}" \
  "inter.broker.listener.name=${KAFKA_INTER_BROKER_LISTENER_NAME:-INTERNAL}" \
  "controller.listener.names=${KAFKA_CONTROLLER_LISTENER_NAMES:-CONTROLLER}" \
  "log.dirs=${log_dir}" \
  "num.partitions=${KAFKA_NUM_PARTITIONS:-1}" \
  "offsets.topic.replication.factor=${KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR:-3}" \
  "transaction.state.log.replication.factor=${KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR:-3}" \
  "transaction.state.log.min.isr=${KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:-2}" \
  "default.replication.factor=${KAFKA_DEFAULT_REPLICATION_FACTOR:-3}" \
  "min.insync.replicas=${KAFKA_MIN_INSYNC_REPLICAS:-2}" \
  "auto.create.topics.enable=${KAFKA_AUTO_CREATE_TOPICS_ENABLE:-false}" \
  "log.message.timestamp.type=${KAFKA_LOG_MESSAGE_TIMESTAMP_TYPE:-LogAppendTime}" \
  "message.max.bytes=${KAFKA_MESSAGE_MAX_BYTES:-1000012}" \
  "replica.fetch.max.bytes=${KAFKA_REPLICA_FETCH_MAX_BYTES:-1048576}" \
  >"${config_file}"

mkdir -p "${log_dir}"
/opt/kafka/bin/kafka-storage.sh format --ignore-formatted -t "${CLUSTER_ID}" -c "${config_file}"
exec /opt/kafka/bin/kafka-server-start.sh "${config_file}"
