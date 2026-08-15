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
mutation_topic="${KAFKA_DELAY_E2E_MUTATION_TOPIC:-nereus-delay-mutation-topic}"
mutation_worker_topic="${KAFKA_DELAY_E2E_MUTATION_WORKER_TOPIC:-nereus-delay-mutation-worker-topic}"
route_worker_topic="${KAFKA_DELAY_E2E_ROUTE_WORKER_TOPIC:-nereus-delay-route-worker-topic}"
worker_topic="${KAFKA_DELAY_E2E_WORKER_TOPIC:-nereus-delay-worker-topic}"
worker_destination_topic="${KAFKA_DELAY_E2E_WORKER_DESTINATION_TOPIC:-nereus-delay-worker-destination-topic}"
k2_target_topic="${KAFKA_DELAY_E2E_K2_TARGET_TOPIC:-nereus-delay-k2-target}"
k2_receipt_topic="${KAFKA_DELAY_E2E_K2_RECEIPT_TOPIC:-nereus-delay-k2-receipt}"
with_oxia="${NEREUS_DELAY_KAFKA_WITH_OXIA:-0}"
route_failover="${NEREUS_DELAY_KAFKA_ROUTE_FAILOVER:-0}"
route_failover_only="${NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY:-0}"
k2_failover="${NEREUS_DELAY_KAFKA_K2_FAILOVER:-0}"
k2_failover_only="${NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY:-0}"
oxia_checkout="${NEREUS_DELAY_KAFKA_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
oxia_port="${NEREUS_DELAY_KAFKA_OXIA_PORT:-$((16650 + ($$ % 100)))}"
oxia_compose_project="nereus-delay-kafka-oxia-e2e-$(date +%s)-$$"
oxia_compose_file="${script_dir}/docker-compose.oxia.yml"
oxia_compose=(docker compose -p "${oxia_compose_project}" -f "${oxia_compose_file}")
oxia_endpoint="127.0.0.1:${oxia_port}"

if [[ "${with_oxia}" != "0" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WITH_OXIA must be 0 or 1" >&2
  exit 1
fi
if [[ "${route_failover}" != "0" && "${route_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER must be 0 or 1" >&2
  exit 1
fi
if [[ "${route_failover_only}" != "0" && "${route_failover_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${k2_failover}" != "0" && "${k2_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_FAILOVER must be 0 or 1" >&2
  exit 1
fi
if [[ "${k2_failover_only}" != "0" && "${k2_failover_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${route_failover}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${route_failover_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${route_failover_only}" == "1" && "${route_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY requires NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1" >&2
  exit 1
fi
if [[ "${k2_failover_only}" == "1" && "${k2_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY requires NEREUS_DELAY_KAFKA_K2_FAILOVER=1" >&2
  exit 1
fi
if [[ "${route_failover_only}" == "1" && "${k2_failover_only}" == "1" ]]; then
  echo "route and K2 failover-only modes are mutually exclusive" >&2
  exit 1
fi

route_failover_dir="$(mktemp -d -t nereus-delay-kafka-route-failover.XXXXXX)"
route_failover_gate="${route_failover_dir}/release"
route_failover_log="${route_failover_dir}/route-worker.log"
k2_failover_dir="$(mktemp -d -t nereus-delay-kafka-k2-failover.XXXXXX)"
k2_failover_gate="${k2_failover_dir}/release"
k2_failover_ready="${k2_failover_dir}/ready"
k2_failover_log="${k2_failover_dir}/k2.log"
k2_failover_pid=""

cleanup() {
  if [[ -n "${k2_failover_pid}" ]]; then
    kill "${k2_failover_pid}" >/dev/null 2>&1 || true
    wait "${k2_failover_pid}" >/dev/null 2>&1 || true
  fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  docker image rm "${image}" >/dev/null 2>&1 || true
  if [[ "${with_oxia}" == "1" ]]; then
    "${oxia_compose[@]}" down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  fi
  rm -rf "${image_context}"
  rm -rf "${route_failover_dir}"
  rm -rf "${k2_failover_dir}"
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

start_oxia() {
  test -d "${oxia_checkout}"
  test -s "${oxia_checkout}/Dockerfile"
  export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}"
  export NEREUS_DELAY_OXIA_E2E_PORT="${oxia_port}"
  echo "Oxia checkout: $(git -C "${oxia_checkout}" rev-parse HEAD)"
  echo "Oxia Compose project: ${oxia_compose_project}"
  echo "Oxia endpoint: ${oxia_endpoint}"
  "${oxia_compose[@]}" up --build --detach
  local ready=0
  for attempt in $(seq 1 60); do
    if "${oxia_compose[@]}" exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
        >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != "1" ]]; then
    "${oxia_compose[@]}" logs oxia >&2 || true
    echo "Oxia did not become healthy" >&2
    return 1
  fi
}

run_worker_smoke() {
  local bootstrap_server="$1"
  local worker_topic_base="$2"
  local worker_mode="${3:-run}"
  if [[ "${with_oxia}" == "1" ]]; then
    NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaWorkerSmoke \
      -PkafkaClientJar="${client_jar}" \
      -PkafkaBootstrap="${bootstrap_server}" \
      -PkafkaWorkerTopic="${worker_topic_base}" \
      -PkafkaWorkerDestinationTopic="${worker_destination_topic}" \
      -PkafkaWorkerMode="${worker_mode}" \
      --no-daemon --console=plain
  else
    GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaWorkerSmoke \
      -PkafkaClientJar="${client_jar}" \
      -PkafkaBootstrap="${bootstrap_server}" \
      -PkafkaWorkerTopic="${worker_topic_base}" \
      -PkafkaWorkerDestinationTopic="${worker_destination_topic}" \
      -PkafkaWorkerMode="${worker_mode}" \
      --no-daemon --console=plain
  fi
}

run_mutation_worker_smoke() {
  local bootstrap_server="$1"
  if [[ "${with_oxia}" == "1" ]]; then
    NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaMutationWorkerSmoke \
      -PkafkaClientJar="${client_jar}" \
      -PkafkaBootstrap="${bootstrap_server}" \
      -PkafkaMutationWorkerTopic="${mutation_worker_topic}" \
      --no-daemon --console=plain
  else
    GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaMutationWorkerSmoke \
      -PkafkaClientJar="${client_jar}" \
      -PkafkaBootstrap="${bootstrap_server}" \
      -PkafkaMutationWorkerTopic="${mutation_worker_topic}" \
      --no-daemon --console=plain
  fi
}

run_route_worker_smoke() {
  local bootstrap_server="$1"
  if [[ "${with_oxia}" != "1" ]]; then
    return 0
  fi
  local route_command=(./gradlew runRealKafkaRouteWorkerSmoke
    "-PkafkaClientJar=${client_jar}"
    "-PkafkaBootstrap=${bootstrap_server}"
    "-PkafkaRouteWorkerTopic=${route_worker_topic}"
    --no-daemon --console=plain)
  if [[ "${route_failover}" != "1" ]]; then
    NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    GRADLE_USER_HOME="${gradle_user_home}" "${route_command[@]}"
    return 0
  fi

  NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
  NEREUS_DELAY_OXIA_NAMESPACE=default \
  GRADLE_USER_HOME="${gradle_user_home}" \
  NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_GATE="${route_failover_gate}" \
    "${route_command[@]}" >"${route_failover_log}" 2>&1 &
  local route_pid=$!
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if rg -F --quiet "Kafka Route Worker ready for accepted-route failover" "${route_failover_log}"; then
      break
    fi
    if ! kill -0 "${route_pid}" >/dev/null 2>&1; then
      wait "${route_pid}" || true
      cat "${route_failover_log}" >&2
      return 1
    fi
    sleep 1
  done
  if ! rg -F --quiet "Kafka Route Worker ready for accepted-route failover" "${route_failover_log}"; then
    echo "Kafka Route Worker did not reach the accepted-route failover gate" >&2
    cat "${route_failover_log}" >&2
    return 1
  fi
  "${compose[@]}" stop kafka-1
  wait_for_broker kafka-2
  touch "${route_failover_gate}"
  wait "${route_pid}"
  "${compose[@]}" start kafka-1
  wait_for_broker kafka-1
  sleep 10
  cat "${route_failover_log}"
}

run_k2_smoke() {
  local bootstrap_server="$1"
  local k2_command=(./gradlew runRealKafkaK2Smoke
    "-PkafkaClientJar=${client_jar}"
    "-PkafkaBootstrap=${bootstrap_server}"
    "-PkafkaTargetTopic=${k2_target_topic}"
    "-PkafkaReceiptTopic=${k2_receipt_topic}"
    --no-daemon --console=plain)
  if [[ "${k2_failover}" != "1" ]]; then
    GRADLE_USER_HOME="${gradle_user_home}" "${k2_command[@]}"
    return 0
  fi

  rm -f "${k2_failover_gate}" "${k2_failover_ready}" "${k2_failover_log}"
  NEREUS_DELAY_KAFKA_K2_COMMIT_GATE="${k2_failover_gate}" \
  NEREUS_DELAY_KAFKA_K2_COMMIT_READY="${k2_failover_ready}" \
  GRADLE_USER_HOME="${gradle_user_home}" "${k2_command[@]}" >"${k2_failover_log}" 2>&1 &
  k2_failover_pid=$!
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if [[ -f "${k2_failover_ready}" ]]; then
      break
    fi
    if ! kill -0 "${k2_failover_pid}" >/dev/null 2>&1; then
      if wait "${k2_failover_pid}"; then
        :
      else
        cat "${k2_failover_log}" >&2
        k2_failover_pid=""
        return 1
      fi
      cat "${k2_failover_log}" >&2
      k2_failover_pid=""
      return 1
    fi
    sleep 1
  done
  if [[ ! -f "${k2_failover_ready}" ]]; then
    echo "Kafka K2 smoke did not reach the broker failover gate" >&2
    cat "${k2_failover_log}" >&2
    return 1
  fi

  "${compose[@]}" stop kafka-1
  wait_for_broker kafka-2
  touch "${k2_failover_gate}"
  local k2_status=0
  if wait "${k2_failover_pid}"; then
    :
  else
    k2_status=$?
  fi
  k2_failover_pid=""
  "${compose[@]}" start kafka-1
  wait_for_broker kafka-1
  sleep 10
  cat "${k2_failover_log}"
  return "${k2_status}"
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

if [[ "${route_failover_only}" == "1" ]]; then
  start_oxia
  run_route_worker_smoke "${bootstrap_all}"
  echo "Kafka accepted-route broker failover E2E passed: Route-bound Worker applied and ACKed after broker-1 failover, then released its final checkpoint and Oxia assignment."
  exit 0
fi

if [[ "${k2_failover_only}" == "1" ]]; then
  run_k2_smoke "${bootstrap_all}"
  echo "Kafka K2 broker failover E2E passed: target-plus-receipt transaction crossed broker-1 failover with read_committed resolution."
  exit 0
fi

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

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaMutationSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaMutationTopic="${mutation_topic}" \
  --no-daemon --console=plain

if [[ "${with_oxia}" == "1" ]]; then
  start_oxia
fi
run_mutation_worker_smoke "${bootstrap_all}"
run_route_worker_smoke "${bootstrap_all}"
run_worker_smoke "${bootstrap_all}" "${worker_topic}"
restart_worker_topic="${KAFKA_DELAY_RESTART_WORKER_TOPIC:-${worker_topic}-broker-restart}"
run_worker_smoke "${bootstrap_all}" "${restart_worker_topic}" prepare

run_k2_smoke "${bootstrap_all}"

"${compose[@]}" stop kafka-1
wait_for_broker kafka-2
GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_survivors}" \
  -PkafkaTopic="${topic_1}" \
  -PsmokeMode=preserve \
  --no-daemon --console=plain

run_worker_smoke "${bootstrap_survivors}" "${restart_worker_topic}" resume

if [[ "${route_failover}" == "1" && "${with_oxia}" == "1" ]]; then
  echo "Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, accepted Route Worker apply across broker-1 failover, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence."
else
  echo "Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence."
fi
