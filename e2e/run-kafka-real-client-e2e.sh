#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
gradle_user_home="${NEREUS_DELAY_KAFKA_GRADLE_USER_HOME:-/tmp/nereus-delay-kafka-e2e-gradle}"
compose_project="nereus-delay-kafka-e2e-$(date +%s)-$$"
compose_file="${script_dir}/docker-compose.kafka.yml"
compose=(docker compose -p "${compose_project}" -f "${compose_file}")
image="nereus-delay-kafka-k1:${compose_project}"
image_context="$(mktemp -d -t nereus-delay-k1-image.XXXXXX)"
base_port=$((19092 + ($$ % 500)))
broker_1_port="${KAFKA_BROKER_1_PORT:-${base_port}}"
broker_2_port="${KAFKA_BROKER_2_PORT:-$((base_port + 1))}"
broker_3_port="${KAFKA_BROKER_3_PORT:-$((base_port + 2))}"
cluster_id="${KAFKA_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
client_jar="${NEREUS_DELAY_KAFKA_CLIENT_JAR:-${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar}"
bootstrap_all="127.0.0.1:${broker_1_port},127.0.0.1:${broker_2_port},127.0.0.1:${broker_3_port}"
bootstrap_survivors="127.0.0.1:${broker_2_port},127.0.0.1:${broker_3_port}"
topic_1="${KAFKA_DELAY_E2E_TOPIC_1:-nereus-delay-k1-topic-1}"
source_topic="${KAFKA_DELAY_E2E_SOURCE_TOPIC:-nereus-delay-source-topic}"
worker_topic="${KAFKA_DELAY_E2E_WORKER_TOPIC:-nereus-delay-worker-topic}"
k2_target_topic="${KAFKA_DELAY_E2E_K2_TARGET_TOPIC:-nereus-delay-k2-target}"
k2_receipt_topic="${KAFKA_DELAY_E2E_K2_RECEIPT_TOPIC:-nereus-delay-k2-receipt}"

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  docker image rm "${image}" >/dev/null 2>&1 || true
  rm -rf "${image_context}"
}
trap cleanup EXIT INT TERM

require_clean_kafka_checkout() {
  test -z "$(git -C "${kafka_dir}" status --porcelain)"
}

wait_for_broker() {
  local service="$1"
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if "${compose[@]}" exec -T "${service}" bash -c \
        "echo > /dev/tcp/${service}/19092" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Kafka did not become ready: ${bootstrap}" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs >&2 || true
  return 1
}

cd "${delay_dir}"
require_clean_kafka_checkout
test -s "${client_jar}"
test -s "${kafka_dir}/core/build/libs/kafka_2.13-4.4.0-SNAPSHOT.jar"
test -s "${kafka_dir}/core/build/dependant-libs-2.13.18/kafka-server-4.4.0-SNAPSHOT.jar"
test -x "${delay_dir}/gradlew"

mkdir -p "${image_context}/core" "${image_context}/clients"
cp -R "${kafka_dir}/bin" "${image_context}/bin"
cp -R "${kafka_dir}/config" "${image_context}/config"
cp -R "${kafka_dir}/core/build" "${image_context}/core/build"
cp -R "${kafka_dir}/clients/build" "${image_context}/clients/build"
cp "${script_dir}/Dockerfile.kafka-k1" "${image_context}/Dockerfile"
cp "${script_dir}/kafka-k1-entrypoint.sh" "${image_context}/kafka-k1-entrypoint.sh"
docker build --pull=false -t "${image}" "${image_context}"
image_digest="$(docker image inspect "${image}" --format '{{.Id}}')"

export KAFKA_CLUSTER_ID="${cluster_id}"
export KAFKA_K1_IMAGE="${image}"
export KAFKA_BROKER_1_PORT="${broker_1_port}"
export KAFKA_BROKER_2_PORT="${broker_2_port}"
export KAFKA_BROKER_3_PORT="${broker_3_port}"

echo "K1 checkout: $(git -C "${kafka_dir}" rev-parse HEAD)"
echo "K1 client jar: ${client_jar}"
echo "K1 client SHA256: $(shasum -a 256 "${client_jar}" | awk '{print $1}')"
echo "K1 broker image ID: ${image_digest}"
echo "Compose project: ${compose_project}"
echo "Broker ports: ${broker_1_port},${broker_2_port},${broker_3_port}"

"${compose[@]}" up -d
wait_for_broker kafka-1

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaTopic="${topic_1}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSourceSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaSourceTopic="${source_topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaWorkerSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaWorkerTopic="${worker_topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaK2Smoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaTargetTopic="${k2_target_topic}" \
  -PkafkaReceiptTopic="${k2_receipt_topic}" \
  --no-daemon --console=plain

"${compose[@]}" stop kafka-1
wait_for_broker kafka-2
GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_survivors}" \
  -PkafkaTopic="${topic_1}" \
  -PsmokeMode=preserve \
  --no-daemon --console=plain

echo "Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence."
