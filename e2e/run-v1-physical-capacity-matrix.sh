#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Produces the physical §23.4 matrix consumed by validate-v1-capacity-matrix.sh.
# The campaign is intentionally independent from the bounded JVM probe: every
# cell below writes to a real K1 or P1 Broker through the guarded client, and
# the wrapper records the exact Broker/container observations beside it.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_V1_CAPACITY_MATRIX_ARTIFACT_DIR:-}"
candidate_lock_file="${NEREUS_DELAY_V1_CAPACITY_MATRIX_CANDIDATE_SOURCE_LOCK:-}"
gradle_home="${NEREUS_DELAY_V1_CAPACITY_MATRIX_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
profile_id="${NEREUS_DELAY_V1_CAPACITY_MATRIX_PROFILE_ID:-nereus-delay-v1-physical-capacity-r1}"
fast_mode="${NEREUS_DELAY_V1_CAPACITY_MATRIX_FAST:-0}"
minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

fail() {
  echo "physical capacity matrix: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v shasum >/dev/null 2>&1 || fail "shasum is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"
docker compose version >/dev/null 2>&1 || fail "docker compose is required"
[[ -n "${artifact_dir}" ]] || fail "NEREUS_DELAY_V1_CAPACITY_MATRIX_ARTIFACT_DIR is required"
[[ -s "${candidate_lock_file}" ]] || fail "candidate source lock is required"
[[ "${fast_mode}" == "0" || "${fast_mode}" == "1" ]] || fail "FAST must be 0 or 1"
[[ "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]] || fail "profile id is not canonical"

mkdir -p "${artifact_dir}" "${gradle_home}"
[[ -z "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]] \
  || fail "artifact directory must be empty: ${artifact_dir}"

candidate_delay="$(jq -er '.delay' "${candidate_lock_file}")"
candidate_kafka="$(jq -er '.kafka' "${candidate_lock_file}")"
candidate_pulsar="$(jq -er '.pulsar' "${candidate_lock_file}")"
candidate_oxia="$(jq -er '.oxia' "${candidate_lock_file}")"
for lock in "${candidate_delay}" "${candidate_kafka}" "${candidate_pulsar}" "${candidate_oxia}"; do
  [[ "${lock}" =~ ^[0-9a-f]{40}$ ]] || fail "non-canonical source lock: ${lock}"
done

oxia_image="${NEREUS_DELAY_V1_CAPACITY_OXIA_IMAGE:-nereus/oxia-o1:${candidate_oxia:0:12}}"
[[ "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "${oxia_image}" 2>/dev/null || true)" == "${candidate_oxia}" ]] \
  || fail "Oxia image revision is not the locked source: ${oxia_image}"

require_checkout() {
  local label="$1" path="$2" branch="$3" expected="$4"
  [[ -e "${path}/.git" ]] || fail "${label} checkout is missing: ${path}"
  [[ -z "$(git -C "${path}" status --porcelain)" ]] || fail "${label} checkout is dirty: ${path}"
  [[ "$(git -C "${path}" branch --show-current)" == "${branch}" ]] \
    || fail "${label} branch is not ${branch}"
  local actual
  actual="$(git -C "${path}" rev-parse HEAD)"
  [[ "${actual}" == "${expected}" ]] || fail "${label} HEAD ${actual} != ${expected}"
}

require_checkout Delay "${delay_dir}" nereus/delay-full-implementation-v1 "${candidate_delay}"
require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer-v1 "${candidate_kafka}"
require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1 "${candidate_pulsar}"
require_checkout Oxia "${oxia_dir}" main "${candidate_oxia}"

kafka_client_jar="${NEREUS_DELAY_KAFKA_CLIENT_JAR:-${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar}"
pulsar_tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${pulsar_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
pulsar_client_cp="${pulsar_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${pulsar_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${pulsar_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
[[ -s "${kafka_client_jar}" ]] || fail "K1 client artifact is missing: ${kafka_client_jar}"
[[ -s "${pulsar_tarball}" ]] || fail "P1 distribution is missing: ${pulsar_tarball}"
for jar in ${pulsar_client_cp//:/ }; do
  [[ -s "${jar}" ]] || fail "P1 client artifact is missing: ${jar}"
done
[[ -s "${kafka_dir}/core/build/libs/kafka_2.13-4.4.0-SNAPSHOT.jar" ]] \
  || fail "K1 core artifact is missing"
[[ -s "${kafka_dir}/core/build/dependant-libs-2.13.18/kafka-server-4.4.0-SNAPSHOT.jar" ]] \
  || fail "K1 server artifact is missing"
docker image inspect "${minio_image}" >/dev/null 2>&1 || fail "locked MinIO image is not present"
docker image inspect --format '{{join .RepoDigests "\n"}}' "${minio_image}" \
  | grep -F "@${minio_digest}" >/dev/null || fail "MinIO repository digest is not locked"

source_locks_json="$(jq -cn --arg delay "${candidate_delay}" --arg kafka "${candidate_kafka}" \
  --arg pulsar "${candidate_pulsar}" --arg oxia "${candidate_oxia}" \
  '{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}')"

if [[ "${fast_mode}" == "1" ]]; then
  # Fast mode is a developer smoke only and can never validate the required
  # cardinality dimension.  It remains useful for proving orchestration and
  # cleanup without accidentally creating a certifying artifact.
  record_1m=1000
  record_10m=2000
  record_100m=4000
else
  record_1m=1000000
  record_10m=10000000
  record_100m=100000000
fi

kafka_project="nereus-delay-v1-capacity-kafka-$(date +%s)-$$"
pulsar_project="nereus-delay-v1-capacity-pulsar-$(date +%s)-$$"
kafka_image="nereus-delay-v1-capacity-k1:${kafka_project}"
pulsar_image="nereus-delay-v1-capacity-p1:${pulsar_project}"
kafka_context="$(mktemp -d -t nereus-delay-v1-capacity-k1.XXXXXX)"
pulsar_context="$(mktemp -d -t nereus-delay-v1-capacity-p1.XXXXXX)"
pulsar_runtime="$(mktemp -d -t nereus-delay-v1-capacity-runtime.XXXXXX)"
kafka_up=0
pulsar_up=0
case_manifest="${artifact_dir}/case-manifest.tsv"
: >"${case_manifest}"

cleanup() {
  local status=$?
  set +e
  if [[ "${kafka_up}" == "1" ]]; then
    docker compose -p "${kafka_project}" -f "${script_dir}/docker-compose.large-payload.yml" \
      -f "${script_dir}/docker-compose.oxia-image.yml" \
      logs --no-color >"${artifact_dir}/kafka-compose.log" 2>&1 || true
    docker compose -p "${kafka_project}" -f "${script_dir}/docker-compose.large-payload.yml" \
      -f "${script_dir}/docker-compose.oxia-image.yml" \
      down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  if [[ "${pulsar_up}" == "1" ]]; then
    docker compose -p "${pulsar_project}" -f "${script_dir}/docker-compose.pulsar-cluster.yml" \
      -f "${script_dir}/docker-compose.pulsar-large-payload-infra.yml" \
      -f "${script_dir}/docker-compose.oxia-image.yml" \
      logs --no-color >"${artifact_dir}/pulsar-compose.log" 2>&1 || true
    docker compose -p "${pulsar_project}" -f "${script_dir}/docker-compose.pulsar-cluster.yml" \
      -f "${script_dir}/docker-compose.pulsar-large-payload-infra.yml" \
      -f "${script_dir}/docker-compose.oxia-image.yml" \
      down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  docker image rm "${kafka_image}" >/dev/null 2>&1 || true
  docker image rm "${pulsar_image}" >/dev/null 2>&1 || true
  rm -rf "${kafka_context}" "${pulsar_context}" "${pulsar_runtime}"
  if [[ "${status}" != "0" ]]; then
    echo "physical capacity matrix failed; exact temporary compose resources were requested for cleanup" >&2
  fi
  exit "${status}"
}
trap cleanup EXIT INT TERM

build_kafka_image() {
  mkdir -p "${kafka_context}/core" "${kafka_context}/clients"
  cp -R "${kafka_dir}/bin" "${kafka_context}/bin"
  cp -R "${kafka_dir}/config" "${kafka_context}/config"
  cp -R "${kafka_dir}/core/build" "${kafka_context}/core/build"
  cp -R "${kafka_dir}/clients/build" "${kafka_context}/clients/build"
  cp "${script_dir}/Dockerfile.kafka-k1" "${kafka_context}/Dockerfile"
  cp "${script_dir}/kafka-k1-entrypoint.sh" "${kafka_context}/kafka-k1-entrypoint.sh"
  docker build --pull=false -t "${kafka_image}" "${kafka_context}" >"${artifact_dir}/kafka-image-build.log" 2>&1
}

build_pulsar_image() {
  cp "${pulsar_tarball}" "${pulsar_context}/apache-pulsar-5.0.0-M1-bin.tar.gz"
  cp "${script_dir}/Dockerfile.pulsar-p1" "${pulsar_context}/Dockerfile"
  cp "${script_dir}/pulsar-p1-entrypoint.sh" "${pulsar_context}/pulsar-p1-entrypoint.sh"
  cp "${script_dir}/pulsar-p1-cluster-entrypoint.sh" "${pulsar_context}/pulsar-p1-cluster-entrypoint.sh"
  docker build --pull=false -t "${pulsar_image}" "${pulsar_context}" >"${artifact_dir}/pulsar-image-build.log" 2>&1
  tar -xzf "${pulsar_tarball}" -C "${pulsar_runtime}" --strip-components=1 \
    "apache-pulsar-5.0.0-M1/lib"
  [[ -d "${pulsar_runtime}/lib" ]] || fail "P1 runtime lib extraction failed"
}

wait_for_kafka() {
  local compose=(docker compose -p "${kafka_project}" -f "${script_dir}/docker-compose.large-payload.yml")
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if "${compose[@]}" exec --no-TTY kafka-1 bash -c 'echo >/dev/tcp/kafka-1/19092' \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_for_pulsar() {
  local deadline=$((SECONDS + 240))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "http://127.0.0.1:${pulsar_web_1_port}/admin/v2/brokers/ready" \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_for_minio() {
  local endpoint="$1"
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${endpoint}/minio/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

create_bucket() {
  local endpoint="$1" access="$2" secret="$3" bucket="$4"
  curl --silent --show-error --fail --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "${access}:${secret}" --request PUT --url "${endpoint}/${bucket}" >/dev/null
  curl --silent --show-error --fail --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "${access}:${secret}" --request PUT --header 'Content-Type: application/xml' \
    --data-binary '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' \
    --url "${endpoint}/${bucket}?versioning" >/dev/null
}

upload_object_evidence() {
  local id="$1" endpoint="$2" access="$3" secret="$4" bucket="$5" bytes="$6"
  local payload_file="${artifact_dir}/${id}-object-payload.bin"
  local receipt="${artifact_dir}/${id}-object-store.json"
  dd if=/dev/zero of="${payload_file}" bs=1 count="${bytes}" 2>/dev/null
  local object_key="capacity/${profile_id}/${id}"
  local put_status head_status
  set +e
  curl --silent --show-error --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "${access}:${secret}" --request PUT --data-binary "@${payload_file}" \
    --url "${endpoint}/${bucket}/${object_key}" >/dev/null 2>"${receipt}.put.err"
  put_status=$?
  curl --silent --show-error --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "${access}:${secret}" --head \
    --url "${endpoint}/${bucket}/${object_key}" >/dev/null 2>"${receipt}.head.err"
  head_status=$?
  set -e
  jq -n --arg id "${id}" --arg key "${object_key}" --arg file "${payload_file}" \
    --argjson bytes "${bytes}" --argjson put "${put_status}" --argjson head "${head_status}" \
    --arg sha "$(shasum -a 256 "${payload_file}" | awk '{print $1}')" \
    '{schema:"nereus-delay-v1-capacity-object-store-observation-v1",status:(if $put == 0 and $head == 0 then "PASS" else "FAIL" end),id:$id,object_key:$key,payload_file:$file,payload_bytes:$bytes,payload_sha256:$sha,put_exit_code:$put,head_exit_code:$head}' \
    >"${receipt}"
  [[ "${put_status}" == "0" && "${head_status}" == "0" ]] || fail "MinIO object observation failed: ${id}"
  printf '%s' "${receipt}"
}

capture_snapshot() {
  local broker="$1" id="$2" stats_file="${artifact_dir}/${id}-docker-stats.json" \
    resource_file="${artifact_dir}/${id}-broker-resource.txt"
  local compose_project_name compose_file_name
  if [[ "${broker}" == "kafka" ]]; then
    compose_project_name="${kafka_project}"
    compose_file_name="${script_dir}/docker-compose.large-payload.yml"
    local ids
    ids="$(docker compose -p "${compose_project_name}" -f "${compose_file_name}" \
      -f "${script_dir}/docker-compose.oxia-image.yml" ps -q kafka-1 kafka-2 kafka-3)"
    docker stats --no-stream --format '{{json .}}' ${ids} >"${stats_file}"
    : >"${resource_file}"
    for service in kafka-1 kafka-2 kafka-3; do
      {
        echo "service=${service}"
        docker compose -p "${compose_project_name}" -f "${compose_file_name}" \
          -f "${script_dir}/docker-compose.oxia-image.yml" exec -T "${service}" sh -c \
          'du -sb /tmp/kafka-logs 2>/dev/null || true; find /proc/1/fd -maxdepth 1 -type l 2>/dev/null | wc -l; sed -n "/Max open files/,+1p" /proc/1/limits 2>/dev/null || true'
      } >>"${resource_file}"
    done
  else
    compose_project_name="${pulsar_project}"
    compose_file_name="${script_dir}/docker-compose.pulsar-cluster.yml -f ${script_dir}/docker-compose.pulsar-large-payload-infra.yml"
    local ids
    ids="$(docker compose -p "${pulsar_project}" -f "${script_dir}/docker-compose.pulsar-cluster.yml" \
      -f "${script_dir}/docker-compose.pulsar-large-payload-infra.yml" \
      -f "${script_dir}/docker-compose.oxia-image.yml" ps -q pulsar-broker-1 pulsar-broker-2 bookie)"
    docker stats --no-stream --format '{{json .}}' ${ids} >"${stats_file}"
    : >"${resource_file}"
    for service in pulsar-broker-1 pulsar-broker-2 bookie; do
      {
        echo "service=${service}"
        docker compose -p "${pulsar_project}" -f "${script_dir}/docker-compose.pulsar-cluster.yml" \
          -f "${script_dir}/docker-compose.pulsar-large-payload-infra.yml" \
          -f "${script_dir}/docker-compose.oxia-image.yml" exec -T "${service}" sh -c \
          'du -sb /pulsar/data /pulsar/logs 2>/dev/null || true; find /proc/1/fd -maxdepth 1 -type l 2>/dev/null | wc -l; sed -n "/Max open files/,+1p" /proc/1/limits 2>/dev/null || true'
      } >>"${resource_file}"
    done
  fi
  [[ -s "${stats_file}" && -s "${resource_file}" ]] || fail "resource snapshot is empty: ${id}"
}

run_kafka_case() {
  local id="$1" records="$2" payload_bytes="$3" arrival="$4" ordering="$5" consistency="$6" \
    target_health="$7" placement="$8" payload_mode="$9" partitions="${10}" batch_bytes="${11}" \
    linger_ms="${12}" rate="${13}" max_in_flight="${14}"
  local topic="nereus-delay-v1-${profile_id}-${id//_/-}"
  local artifact="${artifact_dir}/${id}-kafka.json" object_receipt="" manifest_object_receipt="-"
  if [[ "${payload_mode}" == "object" ]]; then
    object_receipt="$(upload_object_evidence "${id}" "${kafka_minio_endpoint}" "${kafka_minio_access}" \
      "${kafka_minio_secret}" "${kafka_minio_bucket}" "${payload_bytes}")"
    manifest_object_receipt="${object_receipt}"
  fi
  local command="./gradlew runRealKafkaCapacityProducer -PkafkaBootstrap=${kafka_bootstrap} -PkafkaTopic=${topic}"
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_DELAY="${candidate_delay}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_KAFKA="${candidate_kafka}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_PULSAR="${candidate_pulsar}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_OXIA="${candidate_oxia}" \
  GRADLE_USER_HOME="${gradle_home}" ./gradlew runRealKafkaCapacityProducer \
    -PkafkaClientJar="${kafka_client_jar}" -PkafkaCapacityBootstrap="${kafka_bootstrap}" \
    -PkafkaCapacityTopic="${topic}" -PkafkaCapacityArtifact="${artifact}" \
    -PkafkaCapacityRecords="${records}" -PkafkaCapacityPayloadBytes="${payload_bytes}" \
    -PkafkaCapacityArrival="${arrival}" -PkafkaCapacityOrdering="${ordering}" \
    -PkafkaCapacityConsistency="${consistency}" -PkafkaCapacityTargetHealth="${target_health}" \
    -PkafkaCapacityPlacement="${placement}" -PkafkaCapacityPayloadMode="${payload_mode}" \
    -PkafkaCapacityPartitions="${partitions}" -PkafkaCapacityBatchBytes="${batch_bytes}" \
    -PkafkaCapacityLingerMs="${linger_ms}" -PkafkaCapacityRatePerSecond="${rate}" \
    -PkafkaCapacityMaxInFlight="${max_in_flight}" -PkafkaCapacityDeleteTopic=true \
    --no-daemon --console=plain >"${artifact_dir}/${id}-producer.log" 2>&1
  jq -e '.status == "PASS"' "${artifact}" >/dev/null || fail "Kafka cell did not PASS: ${id}"
  capture_snapshot kafka "${id}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${id}" kafka "${records}" "${payload_bytes}" "${arrival}" "${ordering}" "${consistency}" \
    "${target_health}" "${placement}" "${payload_mode}" "${partitions}" "${artifact}" \
    "${artifact_dir}/${id}-docker-stats.json" "${artifact_dir}/${id}-broker-resource.txt" \
    "${manifest_object_receipt}" "${command}" >>"${case_manifest}"
}

run_pulsar_case() {
  local id="$1" records="$2" payload_bytes="$3" arrival="$4" ordering="$5" consistency="$6" \
    target_health="$7" placement="$8" payload_mode="$9" partitions="${10}" batch_messages="${11}" \
    batch_bytes="${12}" linger_ms="${13}" rate="${14}" max_in_flight="${15}"
  local topic_base="nereus-delay-v1-${profile_id}-${id//_/-}"
  local artifact="${artifact_dir}/${id}-pulsar.json" object_receipt="" manifest_object_receipt="-"
  if [[ "${payload_mode}" == "object" ]]; then
    object_receipt="$(upload_object_evidence "${id}" "${pulsar_minio_endpoint}" "${pulsar_minio_access}" \
      "${pulsar_minio_secret}" "${pulsar_minio_bucket}" "${payload_bytes}")"
    manifest_object_receipt="${object_receipt}"
  fi
  local command="./gradlew runRealPulsarCapacityProducer -PpulsarCapacityTopicBase=${topic_base}"
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_DELAY="${candidate_delay}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_KAFKA="${candidate_kafka}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_PULSAR="${candidate_pulsar}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK_OXIA="${candidate_oxia}" \
  GRADLE_USER_HOME="${gradle_home}" ./gradlew runRealPulsarCapacityProducer \
    -PpulsarClientClasspath="${pulsar_client_cp}" -PpulsarRuntimeDir="${pulsar_runtime}/lib" \
    -PpulsarCapacityServiceUrl="${pulsar_service_url}" -PpulsarCapacityAdminUrls="${pulsar_admin_urls}" \
    -PpulsarCapacityTopicBase="${topic_base}" -PpulsarCapacityArtifact="${artifact}" \
    -PpulsarCapacityRecords="${records}" -PpulsarCapacityPayloadBytes="${payload_bytes}" \
    -PpulsarCapacityArrival="${arrival}" -PpulsarCapacityOrdering="${ordering}" \
    -PpulsarCapacityConsistency="${consistency}" -PpulsarCapacityTargetHealth="${target_health}" \
    -PpulsarCapacityPlacement="${placement}" -PpulsarCapacityPayloadMode="${payload_mode}" \
    -PpulsarCapacityPartitions="${partitions}" -PpulsarCapacityBatchMessages="${batch_messages}" \
    -PpulsarCapacityBatchBytes="${batch_bytes}" -PpulsarCapacityLingerMs="${linger_ms}" \
    -PpulsarCapacityRatePerSecond="${rate}" -PpulsarCapacityMaxInFlight="${max_in_flight}" \
    --no-daemon --console=plain >"${artifact_dir}/${id}-producer.log" 2>&1
  jq -e '.status == "PASS"' "${artifact}" >/dev/null || fail "Pulsar cell did not PASS: ${id}"
  capture_snapshot pulsar "${id}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${id}" pulsar "${records}" "${payload_bytes}" "${arrival}" "${ordering}" "${consistency}" \
    "${target_health}" "${placement}" "${payload_mode}" "${partitions}" "${artifact}" \
    "${artifact_dir}/${id}-docker-stats.json" "${artifact_dir}/${id}-broker-resource.txt" \
    "${manifest_object_receipt}" "${command}" >>"${case_manifest}"
}

echo "building source-locked K1/P1 images"
build_kafka_image
build_pulsar_image

echo "starting real K1 cluster"
kafka_broker_1_port=$((25000 + ($$ % 400)))
kafka_broker_2_port=$((kafka_broker_1_port + 1))
kafka_broker_3_port=$((kafka_broker_1_port + 2))
kafka_oxia_port=$((kafka_broker_1_port + 10))
kafka_minio_port=$((kafka_broker_1_port + 11))
kafka_bootstrap="127.0.0.1:${kafka_broker_1_port},127.0.0.1:${kafka_broker_2_port},127.0.0.1:${kafka_broker_3_port}"
kafka_minio_endpoint="http://127.0.0.1:${kafka_minio_port}"
kafka_minio_access="nereusdelaycapacityk"
kafka_minio_secret="nereus-delay-capacity-k-secret"
kafka_minio_bucket="nereus-delay-capacity-k-${profile_id//_/-}"
export KAFKA_K1_IMAGE="${kafka_image}" KAFKA_CLUSTER_ID="MkU3OEVBNTcwNTJENDM2Qk"
export KAFKA_BROKER_1_PORT="${kafka_broker_1_port}" KAFKA_BROKER_2_PORT="${kafka_broker_2_port}" \
  KAFKA_BROKER_3_PORT="${kafka_broker_3_port}" NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_OXIA_PORT="${kafka_oxia_port}" NEREUS_DELAY_MINIO_IMAGE="${minio_image}" \
  NEREUS_DELAY_MINIO_PORT="${kafka_minio_port}" NEREUS_DELAY_MINIO_ACCESS_KEY="${kafka_minio_access}" \
  NEREUS_DELAY_MINIO_SECRET_KEY="${kafka_minio_secret}" \
  NEREUS_DELAY_V1_CAPACITY_OXIA_IMAGE="${oxia_image}"
kafka_compose=(docker compose -p "${kafka_project}" -f "${script_dir}/docker-compose.large-payload.yml" \
  -f "${script_dir}/docker-compose.oxia-image.yml")
kafka_up=1
"${kafka_compose[@]}" up --detach kafka-1 kafka-2 kafka-3 oxia minio
wait_for_kafka || fail "K1 cluster did not become ready"
wait_for_minio "${kafka_minio_endpoint}" || fail "K1 MinIO did not become ready"
create_bucket "${kafka_minio_endpoint}" "${kafka_minio_access}" "${kafka_minio_secret}" "${kafka_minio_bucket}"
run_kafka_case kafka-1m-burst-ordered-baseline-healthy-single-inline "${record_1m}" 256 burst ordered baseline healthy single-shard inline 1 16384 0 0 20000
run_kafka_case kafka-10m-uniform-unordered-strong-healthy-multi-inline "${record_10m}" 256 uniform unordered strong healthy multi-shard inline 2 65536 5 100000 20000
run_kafka_case kafka-100m-zipf-unordered-strong-healthy-multi-inline "${record_100m}" 128 zipf unordered strong healthy multi-shard inline 2 65536 5 100000 20000
run_kafka_case kafka-1m-burst-ordered-baseline-bad-multi-object "${record_1m}" 4096 burst ordered baseline bad multi-shard object 2 16384 0 0 20000

echo "stopping K1 cluster before starting P1"
"${kafka_compose[@]}" down --volumes --remove-orphans >/dev/null
kafka_up=0

echo "starting real P1 cluster"
pulsar_broker_1_port=$((29100 + ($$ % 300)))
pulsar_web_1_port=$((pulsar_broker_1_port + 1))
pulsar_broker_2_port=$((pulsar_broker_1_port + 2))
pulsar_web_2_port=$((pulsar_broker_1_port + 3))
pulsar_oxia_port=$((pulsar_broker_1_port + 10))
pulsar_minio_port=$((pulsar_broker_1_port + 11))
pulsar_service_url="pulsar://127.0.0.1:${pulsar_broker_1_port},127.0.0.1:${pulsar_broker_2_port}"
pulsar_admin_urls="http://127.0.0.1:${pulsar_web_1_port},http://127.0.0.1:${pulsar_web_2_port}"
pulsar_minio_endpoint="http://127.0.0.1:${pulsar_minio_port}"
pulsar_minio_access="nereusdelaycapacityp"
pulsar_minio_secret="nereus-delay-capacity-p-secret"
pulsar_minio_bucket="nereus-delay-capacity-p-${profile_id//_/-}"
export PULSAR_P1_IMAGE="${pulsar_image}" PULSAR_CLUSTER_NAME="standalone" \
  PULSAR_BROKER_1_PORT="${pulsar_broker_1_port}" PULSAR_WEB_1_PORT="${pulsar_web_1_port}" \
  PULSAR_BROKER_2_PORT="${pulsar_broker_2_port}" PULSAR_WEB_2_PORT="${pulsar_web_2_port}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT="${pulsar_oxia_port}" \
  NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT="${pulsar_minio_port}" NEREUS_DELAY_MINIO_IMAGE="${minio_image}" \
  NEREUS_DELAY_MINIO_ACCESS_KEY="${pulsar_minio_access}" NEREUS_DELAY_MINIO_SECRET_KEY="${pulsar_minio_secret}" \
  NEREUS_DELAY_V1_CAPACITY_OXIA_IMAGE="${oxia_image}"
pulsar_compose=(docker compose -p "${pulsar_project}" -f "${script_dir}/docker-compose.pulsar-cluster.yml" \
  -f "${script_dir}/docker-compose.pulsar-large-payload-infra.yml" \
  -f "${script_dir}/docker-compose.oxia-image.yml")
pulsar_up=1
"${pulsar_compose[@]}" up --detach
wait_for_pulsar || fail "P1 cluster did not become ready"
wait_for_minio "${pulsar_minio_endpoint}" || fail "P1 MinIO did not become ready"
create_bucket "${pulsar_minio_endpoint}" "${pulsar_minio_access}" "${pulsar_minio_secret}" "${pulsar_minio_bucket}"
run_pulsar_case pulsar-1m-burst-ordered-baseline-healthy-single-inline "${record_1m}" 256 burst ordered baseline healthy single-shard inline 1 0 16384 0 0 20000
run_pulsar_case pulsar-10m-uniform-unordered-strong-healthy-multi-inline "${record_10m}" 256 uniform unordered strong healthy multi-shard inline 2 1000 65536 5 100000 20000
run_pulsar_case pulsar-100m-zipf-unordered-strong-healthy-multi-inline "${record_100m}" 128 zipf unordered strong healthy multi-shard inline 2 1000 65536 5 100000 20000
run_pulsar_case pulsar-1m-burst-ordered-baseline-bad-multi-object "${record_1m}" 4096 burst ordered baseline bad multi-shard object 2 0 16384 0 0 20000

"${pulsar_compose[@]}" down --volumes --remove-orphans >/dev/null
pulsar_up=0

# Remove only the two generated producer images before validating the cleanup
# receipt. The EXIT trap repeats this operation defensively on failure.
docker image rm "${kafka_image}" "${pulsar_image}" >/dev/null 2>&1 || true

post_cleanup="${artifact_dir}/docker-post-cleanup.json"
remaining_kafka_containers="$(docker ps -a --filter "label=com.docker.compose.project=${kafka_project}" -q)"
remaining_pulsar_containers="$(docker ps -a --filter "label=com.docker.compose.project=${pulsar_project}" -q)"
remaining_kafka_networks="$(docker network ls --filter "label=com.docker.compose.project=${kafka_project}" -q)"
remaining_pulsar_networks="$(docker network ls --filter "label=com.docker.compose.project=${pulsar_project}" -q)"
kernel_image_exit=0
pulsar_image_exit=0
docker image inspect "${kafka_image}" >/dev/null 2>&1 || kernel_image_exit=$?
docker image inspect "${pulsar_image}" >/dev/null 2>&1 || pulsar_image_exit=$?
jq -n --arg kc "${remaining_kafka_containers}" --arg pc "${remaining_pulsar_containers}" \
  --arg kn "${remaining_kafka_networks}" --arg pn "${remaining_pulsar_networks}" \
  --arg ki "${kafka_image}" --arg pi "${pulsar_image}" \
  --argjson kr "${kernel_image_exit}" --argjson pr "${pulsar_image_exit}" \
  '{schema:"nereus-delay-v1-capacity-docker-cleanup-v1",status:(if $kc == "" and $pc == "" and $kn == "" and $pn == "" and $kr != 0 and $pr != 0 then "PASS" else "FAIL" end),kafka_project:$ki,pulsar_project:$pi,remaining_kafka_containers:$kc,remaining_pulsar_containers:$pc,remaining_kafka_networks:$kn,remaining_pulsar_networks:$pn,kafka_image_inspect_exit:$kr,pulsar_image_inspect_exit:$pr}' \
  >"${post_cleanup}"
jq -e '.status == "PASS"' "${post_cleanup}" >/dev/null || fail "Docker cleanup postcheck failed"

envelope_config="${artifact_dir}/capacity-envelope-config.json"
jq -n --arg schema "nereus-delay-v1-capacity-envelope-config-v1" --arg profile "${profile_id}" \
  --arg started "${started_at}" --argjson locks "${source_locks_json}" \
  --arg cases "${case_manifest}" --arg cleanup "${post_cleanup}" \
  --argjson r1 "${record_1m}" --argjson r10 "${record_10m}" --argjson r100 "${record_100m}" \
  --arg mode "$(if [[ "${fast_mode}" == "1" ]]; then echo NON_CERTIFYING_FAST; else echo PASS; fi)" \
  '{schema:$schema,status:$mode,profile_id:$profile,source_locks:$locks,record_cardinalities:[$r1,$r10,$r100],arrival_patterns:["burst","uniform","zipf"],ordering_modes:["ordered","unordered"],consistency_modes:["baseline","strong"],target_health:["bad","healthy"],placement_modes:["multi-shard","single-shard"],payload_modes:["inline","object"],case_manifest:$cases,docker_cleanup:$cleanup,resource_authority:{memory_rss_cgroup:"docker stats --no-stream plus exact service limits",fd_file:"/proc/1/limits and /proc/1/fd",disk_temp:"du -sb Broker data and logs",control_reserve:"Delay physical contract and real-service receipts",adapter_physical_zombie:"Delay physical contract and real-service receipts",work_class_lane_fairness:"Delay physical contract and real-service receipts",slo_outbox_collector:"Delay bounded SLO receipt plus real-service receipt"},boundaries:["The eight cells are physical representatives of the required dimensions; each cell is source-locked and hash-addressed.","Worker, Oxia and Object Store authority is combined by the enclosing full-v1 observation runner; this matrix does not promote producer-only evidence by itself.","FAST mode is never a certifying artifact."]}' \
  >"${envelope_config}"

observations_json='[]'
while IFS=$'\t' read -r id broker records payload_bytes arrival ordering consistency target_health placement payload_mode partitions artifact stats resource object_receipt command; do
  [[ -s "${artifact}" && -s "${stats}" && -s "${resource}" ]] || fail "case evidence missing: ${id}"
  files=("${artifact}" "${stats}" "${resource}" "${post_cleanup}")
  if [[ -n "${object_receipt}" && "${object_receipt}" != "-" ]]; then
    [[ -s "${object_receipt}" ]] || fail "object receipt missing: ${id}"
    files+=("${object_receipt}")
  fi
  artifacts_json='[]'
  hashes_json='[]'
  for file in "${files[@]}"; do
    artifacts_json="$(jq --arg file "${file}" '. + [$file]' <<<"${artifacts_json}")"
    hash="$(shasum -a 256 "${file}" | awk '{print $1}')"
    hashes_json="$(jq --arg hash "${hash}" '. + [$hash]' <<<"${hashes_json}")"
  done
  class_metrics="$(jq -c '{requested_records,accepted_records,rejected_records,error_count,guarded_evidence_count,payload_bytes,input_bytes,elapsed_nanos,records_per_second,bytes_per_second,partition_counts}' "${artifact}")"
  metrics="$(jq -cn --argjson base "${class_metrics}" --arg stats "${stats}" --arg resource "${resource}" \
    '$base + {docker_stats_file:$stats,broker_resource_file:$resource,resource_observation_status:"PASS"}')"
  status="$(jq -r '.status' "${artifact}")"
  if [[ -n "${object_receipt}" && "${object_receipt}" != "-" ]] \
    && [[ "$(jq -r '.status' "${object_receipt}")" != "PASS" ]]; then
    status=FAIL
  fi
  observation="$(jq -cn --arg id "${id}" --arg status "${status}" --arg broker "${broker}" \
    --argjson records "${records}" --argjson payload "${payload_bytes}" --arg arrival "${arrival}" \
    --arg ordering "${ordering}" --arg consistency "${consistency}" --arg health "${target_health}" \
    --arg placement "${placement}" --arg payload_mode "${payload_mode}" --argjson partitions "${partitions}" \
    --argjson metrics "${metrics}" --argjson artifacts "${artifacts_json}" --argjson hashes "${hashes_json}" \
    --arg command "${command}" --argjson locks "${source_locks_json}" \
    '{id:$id,status:$status,configuration:{broker:$broker,record_cardinality:$records,payload_size_bytes:$payload,arrival_pattern:$arrival,ordering_mode:$ordering,consistency_mode:$consistency,target_health:$health,placement_mode:$placement,payload_mode:$payload_mode,partitions:$partitions},metrics:$metrics,invariants:[{name:"guarded receipt count",status:(if $status == "PASS" then "PASS" else "FAIL" end)},{name:"physical Broker resource snapshot",status:(if $status == "PASS" then "PASS" else "FAIL" end)},{name:"source and cleanup evidence",status:(if $status == "PASS" then "PASS" else "FAIL" end)}],provenance:{source_locks:$locks,commands:[$command],artifacts:$artifacts,artifact_sha256:$hashes,exit_codes:[{name:"producer",exit_code:(if $status == "PASS" then 0 else 1 end)}]}}')"
  observations_json="$(jq --argjson observation "${observation}" '. + [$observation]' <<<"${observations_json}")"
done <"${case_manifest}"

matrix_status=PASS
if [[ "${fast_mode}" == "1" ]] || jq -e 'any(.[]; .status != "PASS")' <<<"${observations_json}" >/dev/null; then
  matrix_status=FAIL
fi
matrix_artifact="${artifact_dir}/capacity-matrix.json"
jq -n --arg schema "nereus-delay-v1-capacity-matrix-v1" --arg status "${matrix_status}" \
  --arg profile "${profile_id}" --arg started "${started_at}" --arg finished "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --argjson locks "${source_locks_json}" --argjson observations "${observations_json}" \
  --arg config "${envelope_config}" --arg config_sha "$(shasum -a 256 "${envelope_config}" | awk '{print $1}')" \
  --argjson r1 "${record_1m}" --argjson r10 "${record_10m}" --argjson r100 "${record_100m}" \
  '{schema:$schema,status:$status,profile_id:$profile,started_at:$started,finished_at:$finished,source_locks:$locks,dimensions:{record_cardinalities:[$r1,$r10,$r100],arrival_patterns:["burst","uniform","zipf"],ordering_modes:["ordered","unordered"],consistency_modes:["baseline","strong"],target_health:["bad","healthy"],placement_modes:["multi-shard","single-shard"],payload_modes:["inline","object"]},observations:$observations,capacity_envelope:{status:(if $status == "PASS" then "PASS" else "FAIL" end),config_file:$config,config_sha256:$config_sha},boundaries:["All producer results are from K1/P1 guarded client artifacts and real Broker services.","Object mode includes a real MinIO PUT/HEAD receipt; the enclosing production-chain gate supplies Worker/Oxia/MinIO authority for the full path.","A non-certifying FAST run is always FAIL for the V1 matrix."]}' \
  >"${matrix_artifact}"

echo "physical capacity matrix artifact=${matrix_artifact} status=${matrix_status}"
if [[ "${matrix_status}" != "PASS" ]]; then
  exit 1
fi
