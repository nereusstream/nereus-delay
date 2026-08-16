#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_checkout="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
gradle_user_home="${NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME:-/tmp/nereus-delay-pulsar-large-payload-gradle}"
compose_project="nereus-delay-pulsar-large-e2e-$(date +%s)-$$"
compose_file="${script_dir}/docker-compose.pulsar-cluster.yml"
infra_file="${script_dir}/docker-compose.pulsar-large-payload-infra.yml"
compose=(docker compose -p "${compose_project}" -f "${compose_file}" -f "${infra_file}")
image="nereus-delay-pulsar-p1-large:${compose_project}"
oxia_image="${compose_project}-oxia"
image_context="$(mktemp -d -t nereus-delay-pulsar-large-image.XXXXXX)"
runtime_dir="$(mktemp -d -t nereus-delay-pulsar-large-runtime.XXXXXX)"
tls_dir="$(mktemp -d -t nereus-delay-pulsar-large-tls.XXXXXX)"
receipt_log="$(mktemp -t nereus-delay-pulsar-large-receipt.XXXXXX).log"
failover_mode="${NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER:-0}"
process_crash_mode="${NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH:-0}"
network_partition_mode="${NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION:-0}"
network_partition_handoff_wait_seconds="${NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS:-75}"
failover_marker="${runtime_dir}/gateway-commit.failover.ready"

base_port=$((29100 + ($$ % 300)))
broker_1_port="${PULSAR_LARGE_BROKER_1_PORT:-${base_port}}"
web_1_port="${PULSAR_LARGE_WEB_1_PORT:-$((base_port + 1))}"
broker_2_port="${PULSAR_LARGE_BROKER_2_PORT:-$((base_port + 2))}"
web_2_port="${PULSAR_LARGE_WEB_2_PORT:-$((base_port + 3))}"
oxia_port="${NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT:-$((base_port + 10))}"
minio_port="${NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT:-$((base_port + 11))}"
gateway_port="${NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT:-$((base_port + 12))}"
cluster_name="standalone"
source_topic="${PULSAR_LARGE_PAYLOAD_TOPIC:-pulsar-large-payload-source}"
destination_topic="${NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC:-pulsar-large-payload-destination-${compose_project##*-}}"
service_url="pulsar://127.0.0.1:${broker_1_port}"
admin_url="http://127.0.0.1:${web_1_port}"
admin_url_failover="http://127.0.0.1:${web_2_port}"
if [[ "${failover_mode}" == "1" ]]; then
  service_url="pulsar://127.0.0.1:${broker_1_port},127.0.0.1:${broker_2_port}"
fi
tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${pulsar_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
pulsar_client_cp="${pulsar_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${pulsar_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${pulsar_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
IFS=: read -r -a pulsar_client_artifacts <<< "${pulsar_client_cp}"

minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_region="${NEREUS_DELAY_MINIO_REGION:-us-east-1}"
minio_access_key="${NEREUS_DELAY_MINIO_ACCESS_KEY:-nereusdelaypulsarlarge}"
minio_secret_key="${NEREUS_DELAY_MINIO_SECRET_KEY:-nereus-delay-pulsar-large-secret}"
minio_bucket="${NEREUS_DELAY_MINIO_BUCKET:-nereus-delay-pulsar-large-${compose_project##*-}}"
minio_endpoint="http://127.0.0.1:${minio_port}"

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "docker and docker compose are required" >&2
  exit 1
fi
if [[ "${failover_mode}" != "0" && "${failover_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER must be 0 or 1" >&2
  exit 1
fi
if [[ "${process_crash_mode}" != "0" && "${process_crash_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH must be 0 or 1" >&2
  exit 1
fi
if [[ "${network_partition_mode}" != "0" && "${network_partition_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION must be 0 or 1" >&2
  exit 1
fi
if [[ ! "${network_partition_handoff_wait_seconds}" =~ ^[0-9]+$ ]]; then
  echo "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS must be a non-negative integer" >&2
  exit 1
fi
if [[ "${process_crash_mode}" == "1" && "${failover_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH requires NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1" >&2
  exit 1
fi
if [[ "${network_partition_mode}" == "1" && "${failover_mode}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION requires NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1" >&2
  exit 1
fi
if [[ "${network_partition_mode}" == "1" && "${process_crash_mode}" == "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION cannot be combined with NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH" >&2
  exit 1
fi
for command_name in curl openssl shasum; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "${command_name} is required" >&2
    exit 1
  fi
done
test -d "${pulsar_dir}" && test -d "${oxia_checkout}" && test -s "${tarball}"
test "$(git -C "${pulsar_dir}" branch --show-current)" = "nereus/delay-resource-guard-v1"
test -z "$(git -C "${pulsar_dir}" status --porcelain)"
git -C "${pulsar_dir}" merge-base --is-ancestor 8dae0236c0a0d405ed7f8303081080520fe91551 HEAD
test -z "$(git -C "${oxia_checkout}" status --porcelain)"
for artifact in "${pulsar_client_artifacts[@]}"; do
  test -s "${artifact}"
done
test -x "${delay_dir}/gradlew"
if ! docker image inspect "${minio_image}" >/dev/null 2>&1; then
  echo "locked MinIO image is not present locally: ${minio_image}" >&2
  exit 1
fi
if ! docker image inspect --format '{{join .RepoDigests "\n"}}' "${minio_image}" \
    | rg -F "@${minio_digest}" >/dev/null; then
  echo "local MinIO tag does not carry locked repository digest ${minio_digest}" >&2
  exit 1
fi
tar -xzf "${tarball}" -C "${runtime_dir}" --strip-components=1 "apache-pulsar-5.0.0-M1/lib"
test -n "$(find "${runtime_dir}/lib" -type f -name '*.jar' -print -quit)"

cleanup() {
  local status=$?
  if [[ "${status}" != 0 ]]; then
    "${compose[@]}" ps >&2 || true
    "${compose[@]}" logs --no-color >&2 || true
  fi
  "${compose[@]}" down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  docker image rm "${image}" >/dev/null 2>&1 || true
  docker image rm "${oxia_image}" >/dev/null 2>&1 || true
  rm -rf "${image_context}" "${runtime_dir}" "${tls_dir}"
  echo "Pulsar large-payload E2E receipt log: ${receipt_log}" >&2
  exit "${status}"
}
trap cleanup EXIT INT TERM

wait_for_admin() {
  local url="${1:-${admin_url}}"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${url}/admin/v2/brokers/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Pulsar large-payload broker did not become ready: ${url}" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs pulsar-broker-1 pulsar-broker-2 >&2 || true
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
  echo "Pulsar large-payload Oxia did not become ready" >&2
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
  echo "Pulsar large-payload MinIO did not become ready" >&2
  "${compose[@]}" logs minio >&2 || true
  return 1
}

generate_tls_material() {
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "${tls_dir}/ca.key" -out "${tls_dir}/ca.crt" \
    -days 1 -sha256 -subj "/CN=Nereus Delay Pulsar Large Payload E2E CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${tls_dir}/server.key" -out "${tls_dir}/server.csr" \
    -subj "/CN=localhost" >/dev/null 2>&1
  printf '%s\n' 'subjectAltName=IP:127.0.0.1' 'extendedKeyUsage=serverAuth' >"${tls_dir}/server.ext"
  openssl x509 -req -in "${tls_dir}/server.csr" -CA "${tls_dir}/ca.crt" -CAkey "${tls_dir}/ca.key" \
    -CAcreateserial -out "${tls_dir}/server.crt" -days 1 -sha256 -extfile "${tls_dir}/server.ext" \
    >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${tls_dir}/client.key" -out "${tls_dir}/client.csr" \
    -subj "/CN=nereus-delay-pulsar-large-client" >/dev/null 2>&1
  printf '%s\n' 'extendedKeyUsage=clientAuth' >"${tls_dir}/client.ext"
  openssl x509 -req -in "${tls_dir}/client.csr" -CA "${tls_dir}/ca.crt" -CAkey "${tls_dir}/ca.key" \
    -CAcreateserial -out "${tls_dir}/client.crt" -days 1 -sha256 -extfile "${tls_dir}/client.ext" \
    >/dev/null 2>&1
  chmod 600 "${tls_dir}"/*.key
}

mkdir -p "${image_context}"
cp "${tarball}" "${image_context}/apache-pulsar-5.0.0-M1-bin.tar.gz"
cp "${script_dir}/Dockerfile.pulsar-p1" "${image_context}/Dockerfile"
cp "${script_dir}/pulsar-p1-entrypoint.sh" "${image_context}/pulsar-p1-entrypoint.sh"
cp "${script_dir}/pulsar-p1-cluster-entrypoint.sh" "${image_context}/pulsar-p1-cluster-entrypoint.sh"
docker build --pull=false -t "${image}" "${image_context}"

export PULSAR_P1_IMAGE="${image}"
export PULSAR_CLUSTER_NAME="${cluster_name}"
export PULSAR_BROKER_1_PORT="${broker_1_port}"
export PULSAR_WEB_1_PORT="${web_1_port}"
export PULSAR_BROKER_2_PORT="${broker_2_port}"
export PULSAR_WEB_2_PORT="${web_2_port}"
export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}"
export NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT="${oxia_port}"
export NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT="${minio_port}"
export NEREUS_DELAY_MINIO_IMAGE="${minio_image}"
export NEREUS_DELAY_MINIO_ACCESS_KEY="${minio_access_key}"
export NEREUS_DELAY_MINIO_SECRET_KEY="${minio_secret_key}"

generate_tls_material
"${compose[@]}" up --build --detach
wait_for_admin "${admin_url}"
if [[ "${failover_mode}" == "1" ]]; then
  wait_for_admin "${admin_url_failover}"
fi
wait_for_oxia
wait_for_minio
curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
  --user "${minio_access_key}:${minio_secret_key}" --request PUT \
  --url "${minio_endpoint}/${minio_bucket}" >/dev/null
curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
  --user "${minio_access_key}:${minio_secret_key}" --request PUT --header 'Content-Type: application/xml' \
  --data-binary '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' \
  --url "${minio_endpoint}/${minio_bucket}?versioning" >/dev/null

echo "Delay source: $(git -C "${delay_dir}" rev-parse HEAD)"
echo "P1 source: $(git -C "${pulsar_dir}" rev-parse HEAD)"
echo "P1 distribution SHA256: $(shasum -a 256 "${tarball}" | awk '{print $1}')"
echo "P1 image ID: $(docker image inspect "${image}" --format '{{.Id}}')"
echo "Oxia source: $(git -C "${oxia_checkout}" rev-parse HEAD)"
echo "MinIO image: ${minio_image}@${minio_digest}"
echo "Compose project: ${compose_project}"
echo "Pulsar service/admin: ${service_url}/${admin_url}"
echo "Oxia/MinIO/Gateway: 127.0.0.1:${oxia_port}/127.0.0.1:${minio_port}/127.0.0.1:${gateway_port}"
echo "Destination topic: ${destination_topic}"

smoke_environment=(
  "NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:${oxia_port}"
  "NEREUS_DELAY_OXIA_NAMESPACE=default"
  "NEREUS_DELAY_MINIO_ENDPOINT=${minio_endpoint}"
  "NEREUS_DELAY_MINIO_ACCESS_KEY=${minio_access_key}"
  "NEREUS_DELAY_MINIO_SECRET_KEY=${minio_secret_key}"
  "NEREUS_DELAY_MINIO_BUCKET=${minio_bucket}"
  "NEREUS_DELAY_MINIO_REGION=${minio_region}"
  "NEREUS_DELAY_GATEWAY_PORT=${gateway_port}"
  "NEREUS_DELAY_GATEWAY_SERVER_CERT=${tls_dir}/server.crt"
  "NEREUS_DELAY_GATEWAY_SERVER_KEY=${tls_dir}/server.key"
  "NEREUS_DELAY_GATEWAY_CA_CERT=${tls_dir}/ca.crt"
  "NEREUS_DELAY_GATEWAY_CLIENT_CERT=${tls_dir}/client.crt"
  "NEREUS_DELAY_GATEWAY_CLIENT_KEY=${tls_dir}/client.key"
  "NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=${destination_topic}"
  "NEREUS_DELAY_PULSAR_LISTENER_NAME=external"
  "GRADLE_USER_HOME=${gradle_user_home}"
)
if [[ "${failover_mode}" == "1" ]]; then
  smoke_environment+=("NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER_MARKER=${failover_marker}")
fi
# Topic metadata/physical materialization starts through the broker named by
# cluster initialization.  Once the physical topic exists, resource-guard
# administration may need the exact successor Broker because P1 ownership can
# redirect a PUT; the client service URL still contains both Brokers.
smoke_admin_url="${admin_url}"
if [[ "${failover_mode}" == "1" ]]; then
  smoke_admin_url="${admin_url},${admin_url_failover}"
fi
smoke_arguments=(
  runRealPulsarLargePayloadGatewaySmoke
  "-PpulsarClientClasspath=${pulsar_client_cp}"
  "-PpulsarRuntimeDir=${runtime_dir}/lib"
  "-PpulsarServiceUrl=${service_url}"
  "-PpulsarAdminUrl=${smoke_admin_url}"
  "-PpulsarLargePayloadTopic=${source_topic}"
  --no-daemon
  --console=plain
)

if [[ "${failover_mode}" == "1" ]]; then
  set +e
  env "${smoke_environment[@]}" "${delay_dir}/gradlew" "${smoke_arguments[@]}" \
    >"${receipt_log}" 2>&1 &
  smoke_pid=$!
  set -e
  failover_deadline=$((SECONDS + 180))
  while [[ ! -f "${failover_marker}" ]]; do
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
      echo "Pulsar Gateway large-payload failover cut marker was not reached" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ "${process_crash_mode}" == "1" ]]; then
    echo "Pulsar Gateway large-payload failover cut: SIGKILLing broker-1"
    "${compose[@]}" kill --signal KILL pulsar-broker-1
  elif [[ "${network_partition_mode}" == "1" ]]; then
    network_name="$(docker network ls \
      --filter "label=com.docker.compose.project=${compose_project}" \
      --filter 'label=com.docker.compose.network=pulsar-cluster' \
      --format '{{.Name}}' | head -n 1)"
    broker_1_container="$("${compose[@]}" ps -q pulsar-broker-1)"
    if [[ -z "${network_name}" || -z "${broker_1_container}" ]]; then
      echo "Pulsar Gateway large-payload network partition could not resolve the exact network/container" >&2
      exit 1
    fi
    echo "Pulsar Gateway large-payload failover cut: disconnecting broker-1 from ${network_name}"
    docker network disconnect "${network_name}" "${broker_1_container}"
    if docker network inspect --format '{{json .Containers}}' "${network_name}" \
        | rg -F --quiet "${broker_1_container}"; then
      echo "Pulsar Gateway large-payload network partition did not disconnect broker-1" >&2
      exit 1
    fi
    echo "Pulsar Gateway large-payload network partition: waiting ${network_partition_handoff_wait_seconds}s for the disconnected broker ownership lease to expire"
    sleep "${network_partition_handoff_wait_seconds}"
  else
    echo "Pulsar Gateway large-payload failover cut: stopping broker-1"
    "${compose[@]}" stop pulsar-broker-1
  fi
  touch "${failover_marker}.release"
  set +e
  wait "${smoke_pid}"
  smoke_status=$?
  set -e
  cat "${receipt_log}"
else
  set +e
  env "${smoke_environment[@]}" "${delay_dir}/gradlew" "${smoke_arguments[@]}" \
    2>&1 | tee "${receipt_log}"
  smoke_status=${PIPESTATUS[0]}
  set -e
fi
if [[ "${smoke_status}" != 0 ]]; then
  exit "${smoke_status}"
fi
if [[ "${process_crash_mode}" == "1" ]]; then
  "${compose[@]}" start pulsar-broker-1
  wait_for_admin "${admin_url}"
  echo "Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker process-crash failover E2E passed: broker-1 was SIGKILLed after Gateway Commit/readback, the same source-applied physical Publish completed through broker-2, and broker-1 rejoined afterward"
elif [[ "${network_partition_mode}" == "1" ]]; then
  network_name="$(docker network ls \
    --filter "label=com.docker.compose.project=${compose_project}" \
    --filter 'label=com.docker.compose.network=pulsar-cluster' \
    --format '{{.Name}}' | head -n 1)"
  broker_1_container="$("${compose[@]}" ps -q pulsar-broker-1)"
  test -n "${network_name}" && test -n "${broker_1_container}"
  docker network connect "${network_name}" "${broker_1_container}"
  if ! docker network inspect --format '{{json .Containers}}' "${network_name}" \
      | rg -F --quiet "${broker_1_container}"; then
    echo "Pulsar Gateway large-payload network partition did not reconnect broker-1" >&2
    exit 1
  fi
  wait_for_admin "${admin_url}"
  echo "Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker network-partition failover E2E passed: broker-1 stayed alive but lost its exact Compose network endpoint after Gateway Commit/readback, the same source-applied physical Publish completed through broker-2, and broker-1 rejoined afterward"
elif [[ "${failover_mode}" == "1" ]]; then
  echo "Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload multi-Broker failover E2E passed: broker-1 stopped after Gateway Commit/readback and the same source-applied physical Publish completed through broker-2"
else
  echo "Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed"
fi
