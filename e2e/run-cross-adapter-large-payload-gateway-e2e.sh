#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
kafka_client_jar="${NEREUS_DELAY_KAFKA_CLIENT_JAR:-${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar}"
pulsar_tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${pulsar_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
pulsar_client_cp="${pulsar_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${pulsar_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${pulsar_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"

artifact_dir="${NEREUS_DELAY_CROSS_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-cross.XXXXXX)}"
mkdir -p "${artifact_dir}"
artifact_dir="$(cd "${artifact_dir}" && pwd)"

suffix="$(date +%s)-$$"
kafka_project="nereus-delay-cross-kafka-${suffix}"
pulsar_project="nereus-delay-cross-pulsar-${suffix}"
kafka_image="nereus-delay-cross-kafka:${suffix}"
pulsar_image="nereus-delay-cross-pulsar:${suffix}"
oxia_container="${kafka_project}-oxia"
minio_container="${kafka_project}-minio"

base_port=$((25000 + ($$ % 300)))
kafka_broker_1_port="${NEREUS_DELAY_CROSS_KAFKA_BROKER_1_PORT:-${base_port}}"
kafka_broker_2_port="${NEREUS_DELAY_CROSS_KAFKA_BROKER_2_PORT:-$((base_port + 1))}"
kafka_broker_3_port="${NEREUS_DELAY_CROSS_KAFKA_BROKER_3_PORT:-$((base_port + 2))}"
pulsar_broker_1_port="${NEREUS_DELAY_CROSS_PULSAR_BROKER_1_PORT:-$((base_port + 10))}"
pulsar_web_1_port="${NEREUS_DELAY_CROSS_PULSAR_WEB_1_PORT:-$((base_port + 11))}"
pulsar_broker_2_port="${NEREUS_DELAY_CROSS_PULSAR_BROKER_2_PORT:-$((base_port + 12))}"
pulsar_web_2_port="${NEREUS_DELAY_CROSS_PULSAR_WEB_2_PORT:-$((base_port + 13))}"
oxia_port="${NEREUS_DELAY_CROSS_OXIA_PORT:-$((base_port + 20))}"
minio_port="${NEREUS_DELAY_CROSS_MINIO_PORT:-$((base_port + 21))}"
gateway_port="${NEREUS_DELAY_CROSS_GATEWAY_PORT:-$((base_port + 22))}"

kafka_cluster_id="${NEREUS_DELAY_CROSS_KAFKA_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
oxia_image="nereus/oxia-o1:37a17bef1720"
minio_access_key="${NEREUS_DELAY_CROSS_MINIO_ACCESS_KEY:-nereusdelaycross}"
minio_secret_key="${NEREUS_DELAY_CROSS_MINIO_SECRET_KEY:-nereus-delay-cross-secret}"
minio_bucket="${NEREUS_DELAY_CROSS_MINIO_BUCKET:-nereus-delay-cross-${suffix}}"
minio_region="${NEREUS_DELAY_MINIO_REGION:-us-east-1}"

kafka_compose=(docker compose --project-name "${kafka_project}" --file "${script_dir}/docker-compose.large-payload.yml")
pulsar_compose=(docker compose --project-name "${pulsar_project}" --file "${script_dir}/docker-compose.pulsar-cluster.yml")
kafka_bootstrap="127.0.0.1:${kafka_broker_1_port},127.0.0.1:${kafka_broker_2_port},127.0.0.1:${kafka_broker_3_port}"
pulsar_service_url="pulsar://127.0.0.1:${pulsar_broker_1_port},127.0.0.1:${pulsar_broker_2_port}"
pulsar_admin_url_1="http://127.0.0.1:${pulsar_web_1_port}"
pulsar_admin_url_2="http://127.0.0.1:${pulsar_web_2_port}"
minio_endpoint="http://127.0.0.1:${minio_port}"
oxia_endpoint="127.0.0.1:${oxia_port}"

tls_dir="${artifact_dir}/tls"
pulsar_runtime_dir="${artifact_dir}/pulsar-runtime"
kafka_context="${artifact_dir}/kafka-image-context"
pulsar_context="${artifact_dir}/pulsar-image-context"
gradle_user_home="${NEREUS_DELAY_CROSS_GRADLE_USER_HOME:-${artifact_dir}/gradle-cache}"
mkdir -p "${tls_dir}" "${pulsar_runtime_dir}" "${kafka_context}/core" "${kafka_context}/clients" "${pulsar_context}"

for required in docker curl openssl shasum; do
  command -v "${required}" >/dev/null 2>&1 || {
    echo "${required} is required" >&2
    exit 1
  }
done
docker compose version >/dev/null 2>&1
test -s "${kafka_client_jar}"
test -s "${pulsar_tarball}"
test -d "${kafka_dir}" && test -d "${pulsar_dir}" && test -d "${oxia_dir}"
test "$(git -C "${kafka_dir}" branch --show-current)" = "nereus/delay-guarded-producer"
test "$(git -C "${pulsar_dir}" branch --show-current)" = "nereus/delay-resource-guard"
test -z "$(git -C "${kafka_dir}" status --porcelain)"
test -z "$(git -C "${pulsar_dir}" status --porcelain)"
test -z "$(git -C "${oxia_dir}" status --porcelain)"
docker image inspect "${minio_image}" >/dev/null 2>&1
docker image inspect --format '{{join .RepoDigests "\n"}}' "${minio_image}" \
  | rg -F "@${minio_digest}" >/dev/null
docker image inspect "${oxia_image}" >/dev/null 2>&1

for port in "${kafka_broker_1_port}" "${kafka_broker_2_port}" "${kafka_broker_3_port}" \
            "${pulsar_broker_1_port}" "${pulsar_web_1_port}" "${pulsar_broker_2_port}" \
            "${pulsar_web_2_port}" "${oxia_port}" "${minio_port}" "${gateway_port}"; do
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "required cross E2E port is already listening: ${port}" >&2
    exit 1
  fi
done

generate_tls_material() {
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "${tls_dir}/ca.key" \
    -out "${tls_dir}/ca.crt" -days 1 -sha256 -subj "/CN=Nereus Delay Cross E2E CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${tls_dir}/server.key" \
    -out "${tls_dir}/server.csr" -subj "/CN=localhost" >/dev/null 2>&1
  printf '%s\n' 'subjectAltName=IP:127.0.0.1' 'extendedKeyUsage=serverAuth' \
    >"${tls_dir}/server.ext"
  openssl x509 -req -in "${tls_dir}/server.csr" -CA "${tls_dir}/ca.crt" \
    -CAkey "${tls_dir}/ca.key" -CAcreateserial -out "${tls_dir}/server.crt" \
    -days 1 -sha256 -extfile "${tls_dir}/server.ext" >/dev/null 2>&1
  openssl req -newkey rsa:2048 -nodes -keyout "${tls_dir}/client.key" \
    -out "${tls_dir}/client.csr" -subj "/CN=nereus-delay-cross-client" >/dev/null 2>&1
  printf '%s\n' 'extendedKeyUsage=clientAuth' >"${tls_dir}/client.ext"
  openssl x509 -req -in "${tls_dir}/client.csr" -CA "${tls_dir}/ca.crt" \
    -CAkey "${tls_dir}/ca.key" -CAcreateserial -out "${tls_dir}/client.crt" \
    -days 1 -sha256 -extfile "${tls_dir}/client.ext" >/dev/null 2>&1
  chmod 600 "${tls_dir}"/*.key
}

copy_build_contexts() {
  cp -R "${kafka_dir}/bin" "${kafka_context}/bin"
  cp -R "${kafka_dir}/config" "${kafka_context}/config"
  cp -R "${kafka_dir}/core/build" "${kafka_context}/core/build"
  cp -R "${kafka_dir}/clients/build" "${kafka_context}/clients/build"
  cp "${script_dir}/Dockerfile.kafka-k1" "${kafka_context}/Dockerfile"
  cp "${script_dir}/kafka-k1-entrypoint.sh" "${kafka_context}/kafka-k1-entrypoint.sh"
  cp "${pulsar_tarball}" "${pulsar_context}/apache-pulsar-5.0.0-M1-bin.tar.gz"
  cp "${script_dir}/Dockerfile.pulsar-p1" "${pulsar_context}/Dockerfile"
  cp "${script_dir}/pulsar-p1-entrypoint.sh" "${pulsar_context}/pulsar-p1-entrypoint.sh"
  cp "${script_dir}/pulsar-p1-cluster-entrypoint.sh" "${pulsar_context}/pulsar-p1-cluster-entrypoint.sh"
}

write_metadata() {
  {
    echo "artifact_dir=${artifact_dir}"
    echo "kafka_project=${kafka_project}"
    echo "pulsar_project=${pulsar_project}"
    echo "kafka_bootstrap=${kafka_bootstrap}"
    echo "pulsar_service_url=${pulsar_service_url}"
    echo "pulsar_admin_urls=${pulsar_admin_url_1},${pulsar_admin_url_2}"
    echo "oxia_endpoint=${oxia_endpoint}"
    echo "minio_endpoint=${minio_endpoint}"
    echo "minio_bucket=${minio_bucket}"
    echo "gateway_port=${gateway_port}"
    echo "Delay_HEAD=$(git -C "${delay_dir}" rev-parse HEAD)"
    echo "Kafka_HEAD=$(git -C "${kafka_dir}" rev-parse HEAD)"
    echo "Pulsar_HEAD=$(git -C "${pulsar_dir}" rev-parse HEAD)"
    echo "Oxia_HEAD=$(git -C "${oxia_dir}" rev-parse HEAD)"
    echo "Kafka_client_SHA256=$(shasum -a 256 "${kafka_client_jar}" | awk '{print $1}')"
    echo "Pulsar_distribution_SHA256=$(shasum -a 256 "${pulsar_tarball}" | awk '{print $1}')"
    echo "MinIO_image=${minio_image}@${minio_digest}"
    echo "Oxia_image=${oxia_image}"
  } >"${artifact_dir}/metadata.txt"
}

wait_for_kafka() {
  local container
  container="$("${kafka_compose[@]}" ps -q kafka-1)"
  test -n "${container}"
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if docker exec "${container}" bash -c 'echo >/dev/tcp/kafka-1/19092' >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_for_pulsar() {
  local deadline=$((SECONDS + 240))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${pulsar_admin_url_1}/admin/v2/brokers/ready" >/dev/null 2>&1 \
        && curl --silent --fail "${pulsar_admin_url_2}/admin/v2/brokers/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_for_minio() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${minio_endpoint}/minio/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_oxia() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if docker exec "${oxia_container}" oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

cleanup() {
  local status=$?
  set +e
  if [[ "${status}" != 0 ]]; then
    "${kafka_compose[@]}" ps >"${artifact_dir}/kafka-ps.txt" 2>&1
    "${kafka_compose[@]}" logs --no-color >"${artifact_dir}/kafka-docker.log" 2>&1
    "${pulsar_compose[@]}" ps >"${artifact_dir}/pulsar-ps.txt" 2>&1
    "${pulsar_compose[@]}" logs --no-color >"${artifact_dir}/pulsar-docker.log" 2>&1
    docker logs "${oxia_container}" >"${artifact_dir}/oxia-docker.log" 2>&1
    docker logs "${minio_container}" >"${artifact_dir}/minio-docker.log" 2>&1
  fi
  "${pulsar_compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1
  "${kafka_compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1
  docker rm -f "${oxia_container}" "${minio_container}" >/dev/null 2>&1
  docker image rm "${kafka_image}" "${pulsar_image}" >/dev/null 2>&1
  echo "Cross-adapter artifact directory: ${artifact_dir}" >&2
  exit "${status}"
}
trap cleanup EXIT INT TERM

copy_build_contexts
generate_tls_material
write_metadata

docker build --pull=false -t "${kafka_image}" "${kafka_context}" >"${artifact_dir}/kafka-image-build.log" 2>&1
docker build --pull=false -t "${pulsar_image}" "${pulsar_context}" >"${artifact_dir}/pulsar-image-build.log" 2>&1
tar -xzf "${pulsar_tarball}" -C "${pulsar_runtime_dir}" --strip-components=1 \
  "apache-pulsar-5.0.0-M1/lib"
test -n "$(find "${pulsar_runtime_dir}/lib" -type f -name '*.jar' -print -quit)"

export KAFKA_CLUSTER_ID="${kafka_cluster_id}"
export KAFKA_K1_IMAGE="${kafka_image}"
export KAFKA_BROKER_1_PORT="${kafka_broker_1_port}"
export KAFKA_BROKER_2_PORT="${kafka_broker_2_port}"
export KAFKA_BROKER_3_PORT="${kafka_broker_3_port}"
export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}"
export NEREUS_DELAY_OXIA_PORT="${oxia_port}"
export NEREUS_DELAY_MINIO_IMAGE="${minio_image}"
export NEREUS_DELAY_MINIO_PORT="${minio_port}"
export NEREUS_DELAY_MINIO_ACCESS_KEY="${minio_access_key}"
export NEREUS_DELAY_MINIO_SECRET_KEY="${minio_secret_key}"
"${kafka_compose[@]}" up --detach kafka-1 kafka-2 kafka-3 >"${artifact_dir}/kafka-up.log" 2>&1

docker run --detach --name "${oxia_container}" --label "nereus.delay.cross=true" \
  -p "127.0.0.1:${oxia_port}:6648" -v "${artifact_dir}/oxia-data:/tmp/nereus-delay-cross-oxia" \
  "${oxia_image}" oxia standalone --public-addr 0.0.0.0:6648 --metrics-addr 0.0.0.0:8080 \
  --wal-dir /tmp/nereus-delay-cross-oxia/wal --data-dir /tmp/nereus-delay-cross-oxia/db \
  >"${artifact_dir}/oxia-run.log"
docker run --detach --name "${minio_container}" --label "nereus.delay.cross=true" \
  -e "MINIO_ROOT_USER=${minio_access_key}" -e "MINIO_ROOT_PASSWORD=${minio_secret_key}" \
  -p "127.0.0.1:${minio_port}:9000" -v "${artifact_dir}/minio-data:/data" \
  "${minio_image}" server /data --console-address :9001 >"${artifact_dir}/minio-run.log"

export PULSAR_P1_IMAGE="${pulsar_image}"
export PULSAR_CLUSTER_NAME="standalone"
export PULSAR_BROKER_1_PORT="${pulsar_broker_1_port}"
export PULSAR_WEB_1_PORT="${pulsar_web_1_port}"
export PULSAR_BROKER_2_PORT="${pulsar_broker_2_port}"
export PULSAR_WEB_2_PORT="${pulsar_web_2_port}"
"${pulsar_compose[@]}" up --detach >"${artifact_dir}/pulsar-up.log" 2>&1

wait_for_kafka
wait_for_oxia
wait_for_minio
wait_for_pulsar
curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
  --user "${minio_access_key}:${minio_secret_key}" --request PUT \
  --url "${minio_endpoint}/${minio_bucket}" >/dev/null
curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
  --user "${minio_access_key}:${minio_secret_key}" --request PUT \
  --header 'Content-Type: application/xml' \
  --data-binary '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' \
  --url "${minio_endpoint}/${minio_bucket}?versioning" >/dev/null

run_direction() {
  local direction="$1"
  local log_file="${artifact_dir}/${direction}.log"
  set +e
  env NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    NEREUS_DELAY_MINIO_ENDPOINT="${minio_endpoint}" \
    NEREUS_DELAY_MINIO_ACCESS_KEY="${minio_access_key}" \
    NEREUS_DELAY_MINIO_SECRET_KEY="${minio_secret_key}" \
    NEREUS_DELAY_MINIO_BUCKET="${minio_bucket}" \
    NEREUS_DELAY_MINIO_REGION="${minio_region}" \
    NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS=60000 \
    NEREUS_DELAY_GATEWAY_PORT="${gateway_port}" \
    NEREUS_DELAY_GATEWAY_SERVER_CERT="${tls_dir}/server.crt" \
    NEREUS_DELAY_GATEWAY_SERVER_KEY="${tls_dir}/server.key" \
    NEREUS_DELAY_GATEWAY_CA_CERT="${tls_dir}/ca.crt" \
    NEREUS_DELAY_GATEWAY_CLIENT_CERT="${tls_dir}/client.crt" \
    NEREUS_DELAY_GATEWAY_CLIENT_KEY="${tls_dir}/client.key" \
    NEREUS_DELAY_PULSAR_ADMIN_URLS="${pulsar_admin_url_1},${pulsar_admin_url_2}" \
    GRADLE_USER_HOME="${gradle_user_home}" \
    "${delay_dir}/gradlew" runRealCrossLargePayloadGatewaySmoke \
    "-PkafkaClientJar=${kafka_client_jar}" \
    "-PpulsarClientClasspath=${pulsar_client_cp}" \
    "-PpulsarRuntimeDir=${pulsar_runtime_dir}/lib" \
    "-PkafkaBootstrap=${kafka_bootstrap}" \
    "-PpulsarServiceUrl=${pulsar_service_url}" \
    "-PpulsarAdminUrl=${pulsar_admin_url_1}" \
    "-PcrossDirection=${direction}" --no-daemon --console=plain 2>&1 | tee "${log_file}"
  local status=${PIPESTATUS[0]}
  set -e
  if [[ "${status}" != 0 ]]; then
    return "${status}"
  fi
}

run_direction K_TO_P
run_direction P_TO_K
echo "CROSS_ADAPTER_LARGE_PAYLOAD_GATEWAY_E2E=PASS_CERTIFIED"
echo "K_TO_P_LOG=${artifact_dir}/K_TO_P.log"
echo "P_TO_K_LOG=${artifact_dir}/P_TO_K.log"
