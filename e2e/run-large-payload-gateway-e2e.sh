#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
oxia_checkout="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
gradle_user_home="${NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME:-/tmp/nereus-delay-large-payload-gradle}"
compose_project="nereus-delay-large-payload-e2e-$(date +%s)-$$"
compose_file="${script_dir}/docker-compose.large-payload.yml"
compose=(docker compose --project-name "${compose_project}" --file "${compose_file}")

kafka_image="nereus-delay-large-payload-k1:${compose_project}"
image_context="$(mktemp -d -t nereus-delay-large-payload-k1.XXXXXX)"
tls_dir="$(mktemp -d -t nereus-delay-large-payload-tls.XXXXXX)"
receipt_log="$(mktemp -t nereus-delay-large-payload-receipt.XXXXXX).log"
fault_proxy_log="$(mktemp -t nereus-delay-large-payload-fault-proxy.XXXXXX).log"
fault_proxy_pid=""

base_port=$((25000 + ($$ % 500)))
kafka_broker_1_port="${KAFKA_LARGE_PAYLOAD_BROKER_1_PORT:-${base_port}}"
kafka_broker_2_port="${KAFKA_LARGE_PAYLOAD_BROKER_2_PORT:-$((base_port + 1))}"
kafka_broker_3_port="${KAFKA_LARGE_PAYLOAD_BROKER_3_PORT:-$((base_port + 2))}"
oxia_port="${NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT:-$((26000 + ($$ % 500)))}"
minio_port="${NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT:-$((27000 + ($$ % 500)))}"
gateway_port="${NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT:-$((28000 + ($$ % 500)))}"
cluster_id="${KAFKA_LARGE_PAYLOAD_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
client_jar="${NEREUS_DELAY_KAFKA_CLIENT_JAR:-${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar}"
bootstrap="127.0.0.1:${kafka_broker_1_port},127.0.0.1:${kafka_broker_2_port},127.0.0.1:${kafka_broker_3_port}"
topic="${NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC:-nereus-delay-large-payload}"
destination_topic="${NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC:-}"
failover_mode="${NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER:-0}"
process_crash_mode="${NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_PROCESS_CRASH:-0}"
network_partition_mode="${NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_NETWORK_PARTITION:-0}"
network_partition_handoff_wait_seconds="${NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS:-45}"
multi_shard_mode="${NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD:-0}"

minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_region="${NEREUS_DELAY_MINIO_REGION:-us-east-1}"
minio_access_key="${NEREUS_DELAY_MINIO_ACCESS_KEY:-nereusdelaylarge}"
minio_secret_key="${NEREUS_DELAY_MINIO_SECRET_KEY:-nereus-delay-large-secret}"
minio_bucket="${NEREUS_DELAY_MINIO_BUCKET:-nereus-delay-large-payload-$(date +%s)-$$}"
minio_endpoint="http://127.0.0.1:${minio_port}"
minio_object_store_endpoint="${minio_endpoint}"
minio_fault_mode="${NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE:-NONE}"
minio_request_timeout_ms="${NEREUS_DELAY_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS:-60000}"
minio_fault_proxy_port="${NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT:-$((minio_port + 100))}"
minio_fault_proxy_endpoint="http://127.0.0.1:${minio_fault_proxy_port}"
oxia_endpoint="127.0.0.1:${oxia_port}"
failover_ready="${tls_dir}/kafka-large-payload-failover.ready"
failover_release="${failover_ready}.release"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "docker and docker compose are required" >&2
  exit 1
fi
for command_name in curl openssl shasum; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "${command_name} is required" >&2
    exit 1
  fi
done

for mode in "${failover_mode}" "${process_crash_mode}" "${network_partition_mode}"; do
  if [[ "${mode}" != "0" && "${mode}" != "1" ]]; then
    echo "large-payload fault modes must be 0 or 1" >&2
    exit 1
  fi
done
if [[ "${multi_shard_mode}" != "0" && "${multi_shard_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD must be 0 or 1" >&2
  exit 1
fi
if [[ "${process_crash_mode}" == "1" && "${failover_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_PROCESS_CRASH requires NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER=1" >&2
  exit 1
fi
if [[ "${network_partition_mode}" == "1" && "${failover_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_NETWORK_PARTITION requires NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER=1" >&2
  exit 1
fi
if [[ "${process_crash_mode}" == "1" && "${network_partition_mode}" == "1" ]]; then
  echo "large-payload process crash and network partition modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${multi_shard_mode}" == "1" && ("${failover_mode}" == "1" || "${process_crash_mode}" == "1"
    || "${network_partition_mode}" == "1") ]]; then
  echo "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD cannot be combined with Broker failover modes" >&2
  exit 1
fi
if [[ ! "${network_partition_handoff_wait_seconds}" =~ ^[0-9]+$ ]]; then
  echo "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS must be a non-negative integer" >&2
  exit 1
fi
if [[ "${minio_fault_mode}" != "NONE" && "${minio_fault_mode}" != "PUT_503_AFTER_COMMIT" \
    && "${minio_fault_mode}" != "PUT_TIMEOUT_AFTER_COMMIT" \
    && "${minio_fault_mode}" != "PUT_503_BEFORE_COMMIT" \
    && "${minio_fault_mode}" != "PUT_TIMEOUT_BEFORE_COMMIT" ]]; then
  echo "NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE must be NONE, PUT_503_AFTER_COMMIT, PUT_TIMEOUT_AFTER_COMMIT, PUT_503_BEFORE_COMMIT or PUT_TIMEOUT_BEFORE_COMMIT" >&2
  exit 1
fi
if [[ "${multi_shard_mode}" == "1" && "${minio_fault_mode}" != "NONE" ]]; then
  echo "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD currently requires MinIO fault mode NONE" >&2
  exit 1
fi
if [[ ! "${minio_request_timeout_ms}" =~ ^[1-9][0-9]*$ ]]; then
  echo "NEREUS_DELAY_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS must be a positive integer" >&2
  exit 1
fi
if [[ ! "${minio_fault_proxy_port}" =~ ^[0-9]+$ ]]; then
  echo "NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT must be numeric" >&2
  exit 1
fi
if [[ "${minio_fault_mode}" != "NONE" ]] && ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required for real MinIO fault injection" >&2
  exit 1
fi

if [[ ! "${minio_bucket}" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
  echo "NEREUS_DELAY_MINIO_BUCKET is not a canonical S3 bucket name: ${minio_bucket}" >&2
  exit 1
fi
if ! docker image inspect "${minio_image}" >/dev/null 2>&1; then
  echo "locked MinIO image is not present locally: ${minio_image}" >&2
  exit 1
fi
if ! docker image inspect --format '{{join .RepoDigests "\\n"}}' "${minio_image}" \
    | rg -F "@${minio_digest}" >/dev/null; then
  echo "local MinIO tag does not carry locked repository digest ${minio_digest}" >&2
  exit 1
fi

test -d "${kafka_dir}" && test -d "${oxia_checkout}"
test "$(git -C "${kafka_dir}" branch --show-current)" = "nereus/delay-guarded-producer-v1"
test -z "$(git -C "${kafka_dir}" status --porcelain)"
git -C "${kafka_dir}" merge-base --is-ancestor \
  c300006a7705c240642db6950b5a95fec982bfc5 HEAD
test -z "$(git -C "${oxia_checkout}" status --porcelain)"
test -s "${client_jar}"
test -s "${kafka_dir}/core/build/libs/kafka_2.13-4.4.0-SNAPSHOT.jar"
test -s "${kafka_dir}/core/build/dependant-libs-2.13.18/kafka-server-4.4.0-SNAPSHOT.jar"
test -x "${delay_dir}/gradlew"

if command -v lsof >/dev/null 2>&1; then
  for port in "${kafka_broker_1_port}" "${kafka_broker_2_port}" "${kafka_broker_3_port}" \
      "${oxia_port}" "${minio_port}" "${gateway_port}"; do
    if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "required E2E port is already listening: ${port}" >&2
      exit 1
    fi
  done
  if [[ "${minio_fault_mode}" != "NONE" ]] \
      && lsof -nP -iTCP:"${minio_fault_proxy_port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "required MinIO fault proxy port is already listening: ${minio_fault_proxy_port}" >&2
    exit 1
  fi
fi

cleanup() {
  local status=$?
  if [[ "${status}" != 0 ]]; then
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --no-color >&2 || true
  fi
  if [[ -n "${fault_proxy_pid}" ]]; then
    kill "${fault_proxy_pid}" >/dev/null 2>&1 || true
    wait "${fault_proxy_pid}" >/dev/null 2>&1 || true
  fi
  "${compose[@]}" down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  docker image rm "${kafka_image}" >/dev/null 2>&1 || true
  rm -rf "${image_context}" "${tls_dir}"
  rm -f "${fault_proxy_log}"
  echo "Large Payload E2E receipt log: ${receipt_log}" >&2
  exit "${status}"
}
trap cleanup EXIT INT TERM

generate_tls_material() {
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "${tls_dir}/ca.key" -out "${tls_dir}/ca.crt" -days 1 -sha256 \
    -subj "/CN=Nereus Delay Large Payload E2E CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes \
    -keyout "${tls_dir}/server.key" -out "${tls_dir}/server.csr" \
    -subj "/CN=localhost" >/dev/null 2>&1
  printf '%s\n' 'subjectAltName=IP:127.0.0.1' 'extendedKeyUsage=serverAuth' \
    >"${tls_dir}/server.ext"
  openssl x509 -req -in "${tls_dir}/server.csr" \
    -CA "${tls_dir}/ca.crt" -CAkey "${tls_dir}/ca.key" -CAcreateserial \
    -out "${tls_dir}/server.crt" -days 1 -sha256 -extfile "${tls_dir}/server.ext" \
    >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes \
    -keyout "${tls_dir}/client.key" -out "${tls_dir}/client.csr" \
    -subj "/CN=nereus-delay-large-payload-client" >/dev/null 2>&1
  printf '%s\n' 'extendedKeyUsage=clientAuth' >"${tls_dir}/client.ext"
  openssl x509 -req -in "${tls_dir}/client.csr" \
    -CA "${tls_dir}/ca.crt" -CAkey "${tls_dir}/ca.key" -CAcreateserial \
    -out "${tls_dir}/client.crt" -days 1 -sha256 -extfile "${tls_dir}/client.ext" \
    >/dev/null 2>&1
  chmod 600 "${tls_dir}"/*.key
}

wait_for_kafka() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if "${compose[@]}" exec --no-TTY kafka-1 bash -c \
        'echo >/dev/tcp/kafka-1/19092' >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Kafka did not become ready" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs kafka-1 kafka-2 kafka-3 >&2 || true
  return 1
}

wait_for_oxia() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if "${compose[@]}" exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Oxia did not become ready" >&2
  "${compose[@]}" logs oxia >&2 || true
  return 1
}

wait_for_minio() {
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${minio_endpoint}/minio/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "MinIO did not become ready" >&2
  "${compose[@]}" logs minio >&2 || true
  return 1
}

create_minio_bucket() {
  curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" --request PUT \
    --url "${minio_endpoint}/${minio_bucket}" >/dev/null
  curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" --request PUT \
    --header 'Content-Type: application/xml' \
    --data-binary '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' \
    --url "${minio_endpoint}/${minio_bucket}?versioning" >/dev/null
}

start_minio_fault_proxy() {
  if [[ "${minio_fault_mode}" == "NONE" ]]; then
    return 0
  fi
  python3 "${script_dir}/minio-fault-proxy.py" \
    --listen-port "${minio_fault_proxy_port}" --backend-port "${minio_port}" \
    >"${fault_proxy_log}" 2>&1 &
  fault_proxy_pid=$!
  for attempt in $(seq 1 30); do
    if curl --silent --fail "${minio_fault_proxy_endpoint}/__health" >/dev/null 2>&1; then
      curl --silent --show-error --fail --request POST --data "${minio_fault_mode}" \
        "${minio_fault_proxy_endpoint}/__fault" >/dev/null
      minio_object_store_endpoint="${minio_fault_proxy_endpoint}"
      return 0
    fi
    sleep 1
  done
  echo "MinIO fault proxy did not become ready" >&2
  cat "${fault_proxy_log}" >&2 || true
  return 1
}

mkdir -p "${image_context}/core" "${image_context}/clients"
cp -R "${kafka_dir}/bin" "${image_context}/bin"
cp -R "${kafka_dir}/config" "${image_context}/config"
cp -R "${kafka_dir}/core/build" "${image_context}/core/build"
cp -R "${kafka_dir}/clients/build" "${image_context}/clients/build"
cp "${script_dir}/Dockerfile.kafka-k1" "${image_context}/Dockerfile"
cp "${script_dir}/kafka-k1-entrypoint.sh" "${image_context}/kafka-k1-entrypoint.sh"
docker build --pull=false -t "${kafka_image}" "${image_context}"

export KAFKA_CLUSTER_ID="${cluster_id}"
export KAFKA_K1_IMAGE="${kafka_image}"
export KAFKA_BROKER_1_PORT="${kafka_broker_1_port}"
export KAFKA_BROKER_2_PORT="${kafka_broker_2_port}"
export KAFKA_BROKER_3_PORT="${kafka_broker_3_port}"
export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}"
export NEREUS_DELAY_OXIA_PORT="${oxia_port}"
export NEREUS_DELAY_MINIO_IMAGE="${minio_image}"
export NEREUS_DELAY_MINIO_PORT="${minio_port}"
export NEREUS_DELAY_MINIO_ACCESS_KEY="${minio_access_key}"
export NEREUS_DELAY_MINIO_SECRET_KEY="${minio_secret_key}"

generate_tls_material
"${compose[@]}" up --build --detach
wait_for_kafka
wait_for_oxia
wait_for_minio
create_minio_bucket
start_minio_fault_proxy

echo "Delay source: $(git -C "${delay_dir}" rev-parse HEAD)"
echo "Kafka source: $(git -C "${kafka_dir}" rev-parse HEAD)"
echo "Kafka client SHA256: $(shasum -a 256 "${client_jar}" | awk '{print $1}')"
echo "Kafka image ID: $(docker image inspect "${kafka_image}" --format '{{.Id}}')"
echo "Oxia source: $(git -C "${oxia_checkout}" rev-parse HEAD)"
echo "MinIO image: ${minio_image}@${minio_digest}"
echo "MinIO image ID: $(docker image inspect "${minio_image}" --format '{{.Id}}')"
echo "Compose project: ${compose_project}"
echo "Kafka bootstrap: ${bootstrap}"
echo "Oxia endpoint: ${oxia_endpoint}"
echo "MinIO endpoint/bucket: ${minio_endpoint}/${minio_bucket}"
echo "Object Store endpoint: ${minio_object_store_endpoint}"
echo "MinIO request timeout: ${minio_request_timeout_ms}ms"
if [[ "${minio_fault_mode}" != "NONE" ]]; then
  echo "MinIO fault mode/proxy: ${minio_fault_mode}/${minio_fault_proxy_endpoint}"
fi
echo "Gateway port: ${gateway_port}"
echo "Kafka destination topic: ${destination_topic:-<disabled>}"
echo "Kafka Large Payload shard mode: ${multi_shard_mode}"

smoke_environment=(
  "NEREUS_DELAY_OXIA_ENDPOINT=${oxia_endpoint}"
  "NEREUS_DELAY_OXIA_NAMESPACE=default"
  "NEREUS_DELAY_MINIO_ENDPOINT=${minio_object_store_endpoint}"
  "NEREUS_DELAY_MINIO_ACCESS_KEY=${minio_access_key}"
  "NEREUS_DELAY_MINIO_SECRET_KEY=${minio_secret_key}"
  "NEREUS_DELAY_MINIO_BUCKET=${minio_bucket}"
  "NEREUS_DELAY_MINIO_REGION=${minio_region}"
  "NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS=${minio_request_timeout_ms}"
  "NEREUS_DELAY_GATEWAY_PORT=${gateway_port}"
  "NEREUS_DELAY_GATEWAY_SERVER_CERT=${tls_dir}/server.crt"
  "NEREUS_DELAY_GATEWAY_SERVER_KEY=${tls_dir}/server.key"
  "NEREUS_DELAY_GATEWAY_CA_CERT=${tls_dir}/ca.crt"
  "NEREUS_DELAY_GATEWAY_CLIENT_CERT=${tls_dir}/client.crt"
  "NEREUS_DELAY_GATEWAY_CLIENT_KEY=${tls_dir}/client.key"
  "NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=${minio_fault_mode}"
  "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=${multi_shard_mode}"
  "GRADLE_USER_HOME=${gradle_user_home}"
)
if [[ "${failover_mode}" == "1" ]]; then
  smoke_environment+=(
    "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER_READY=${failover_ready}"
    "NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER_RELEASE=${failover_release}"
  )
fi
smoke_command=(
  "${delay_dir}/gradlew" runRealKafkaLargePayloadGatewaySmoke
  "-PkafkaClientJar=${client_jar}"
  "-PkafkaBootstrap=${bootstrap}"
  "-PkafkaLargePayloadTopic=${topic}"
  --no-daemon --console=plain
)

if [[ "${failover_mode}" == "1" ]]; then
  set +e
  env "${smoke_environment[@]}" "${smoke_command[@]}" >"${receipt_log}" 2>&1 &
  smoke_pid=$!
  set -e
  failover_deadline=$((SECONDS + 180))
  while [[ ! -f "${failover_ready}" ]]; do
    if ! kill -0 "${smoke_pid}" >/dev/null 2>&1; then
      set +e
      wait "${smoke_pid}"
      smoke_status=$?
      set -e
      cat "${receipt_log}"
      exit "${smoke_status:-1}"
    fi
    if (( SECONDS >= failover_deadline )); then
      kill "${smoke_pid}" >/dev/null 2>&1 || true
      set +e
      wait "${smoke_pid}"
      smoke_status=$?
      set -e
      cat "${receipt_log}"
      echo "Kafka large-payload Broker cut marker was not reached" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ "${process_crash_mode}" == "1" ]]; then
    echo "Kafka large-payload failover cut: SIGKILLing kafka-1"
    "${compose[@]}" kill --signal KILL kafka-1
  else
    network_name="$(docker network ls \
      --filter "label=com.docker.compose.project=${compose_project}" \
      --filter 'label=com.docker.compose.network=default' \
      --format '{{.Name}}' | head -n 1)"
    kafka_1_container="$("${compose[@]}" ps -q kafka-1)"
    if [[ -z "${network_name}" || -z "${kafka_1_container}" ]]; then
      echo "Kafka large-payload network partition could not resolve the exact network/container" >&2
      exit 1
    fi
    echo "Kafka large-payload failover cut: disconnecting kafka-1 from ${network_name}"
    docker network disconnect "${network_name}" "${kafka_1_container}"
    if docker network inspect --format '{{json .Containers}}' "${network_name}" \
        | rg -F --quiet "${kafka_1_container}"; then
      echo "Kafka large-payload network partition did not disconnect kafka-1" >&2
      exit 1
    fi
    echo "Kafka large-payload network partition: waiting ${network_partition_handoff_wait_seconds}s for broker handoff"
    sleep "${network_partition_handoff_wait_seconds}"
  fi
  touch "${failover_release}"
  set +e
  wait "${smoke_pid}"
  smoke_status=$?
  set -e
  cat "${receipt_log}"
else
  set +e
  env "${smoke_environment[@]}" "${smoke_command[@]}" 2>&1 | tee "${receipt_log}"
  smoke_status=${PIPESTATUS[0]}
  set -e
fi
if [[ "${smoke_status}" != 0 ]]; then
  exit "${smoke_status}"
fi

if [[ "${multi_shard_mode}" == "1" ]]; then
  echo "Kafka + Oxia + Gateway mTLS/JWT + one Worker fleet + real MinIO + two destination PUBLISHED outcomes two-shard Large Payload authority E2E passed"
elif [[ "${minio_fault_mode}" == "PUT_503_BEFORE_COMMIT" || "${minio_fault_mode}" == "PUT_TIMEOUT_BEFORE_COMMIT" ]]; then
  echo "Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload pre-commit fail-closed E2E passed"
elif [[ "${failover_mode}" == "1" && "${process_crash_mode}" == "1" ]]; then
  "${compose[@]}" start kafka-1
  wait_for_kafka
  echo "Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker process-crash failover E2E passed: kafka-1 was SIGKILLed after Gateway Commit/readback, the same source-applied physical Publish completed through kafka-2/kafka-3, and kafka-1 rejoined afterward"
elif [[ "${failover_mode}" == "1" ]]; then
  network_name="$(docker network ls \
    --filter "label=com.docker.compose.project=${compose_project}" \
    --filter 'label=com.docker.compose.network=default' \
    --format '{{.Name}}' | head -n 1)"
  kafka_1_container="$("${compose[@]}" ps -q kafka-1)"
  test -n "${network_name}" && test -n "${kafka_1_container}"
  docker network connect "${network_name}" "${kafka_1_container}"
  if ! docker network inspect --format '{{json .Containers}}' "${network_name}" \
      | rg -F --quiet "${kafka_1_container}"; then
    echo "Kafka large-payload network partition did not reconnect kafka-1" >&2
    exit 1
  fi
  wait_for_kafka
  echo "Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker network-partition failover E2E passed: kafka-1 stayed alive but lost its exact Compose network endpoint after Gateway Commit/readback, the same source-applied physical Publish completed through kafka-2/kafka-3, and kafka-1 rejoined afterward"
elif [[ -n "${destination_topic}" ]]; then
  echo "Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload + Kafka destination authority E2E passed"
else
  echo "Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed"
fi
