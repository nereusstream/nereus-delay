#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
gradle_user_home="${NEREUS_DELAY_PULSAR_GRADLE_USER_HOME:-/tmp/nereus-delay-pulsar-multi-broker-gradle}"
oxia_checkout="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
with_oxia="${NEREUS_DELAY_PULSAR_WITH_OXIA:-0}"
broker_process_crash="${NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH:-0}"
compose_project="nereus-delay-pulsar-multi-e2e-$(date +%s)-$$"
oxia_project="nereus-delay-pulsar-multi-oxia-e2e-${compose_project#nereus-delay-pulsar-multi-e2e-}"
compose_file="${script_dir}/docker-compose.pulsar-cluster.yml"
compose=(docker compose -p "${compose_project}" -f "${compose_file}")
oxia_compose_file="${script_dir}/docker-compose.oxia.yml"
oxia_compose=(docker compose -p "${oxia_project}" -f "${oxia_compose_file}")
image="nereus-delay-pulsar-p1:${compose_project}"
oxia_image="${oxia_project}-oxia"
image_context="$(mktemp -d -t nereus-delay-p1-cluster-image.XXXXXX)"
runtime_dir="$(mktemp -d -t nereus-delay-p1-cluster-runtime.XXXXXX)"
base_port=$((21900 + ($$ % 200)))
broker_1_port="${PULSAR_BROKER_1_PORT:-${base_port}}"
web_1_port="${PULSAR_WEB_1_PORT:-$((base_port + 1))}"
broker_2_port="${PULSAR_BROKER_2_PORT:-$((base_port + 2))}"
web_2_port="${PULSAR_WEB_2_PORT:-$((base_port + 3))}"
oxia_port="${NEREUS_DELAY_PULSAR_OXIA_PORT:-16663}"
cluster_name="standalone"
service_url_before="pulsar://127.0.0.1:${broker_1_port}"
service_url_failover="pulsar://127.0.0.1:${broker_1_port},127.0.0.1:${broker_2_port}"
admin_url_before="http://127.0.0.1:${web_1_port}"
admin_url_after="http://127.0.0.1:${web_2_port}"
restart_topic="${PULSAR_DELAY_MULTI_BROKER_RESTART_TOPIC:-p1-multi-worker-${compose_project##*-}}"
destination_topic="${PULSAR_DELAY_MULTI_BROKER_DESTINATION_TOPIC:-p1-multi-destination-${compose_project##*-}}"
broker_recovery_state_dump_dir="${NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_DUMP_DIR:-}"
broker_recovery_admin_endpoint_before="${NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT_BEFORE:-${admin_url_before}}"
broker_recovery_admin_endpoint_after="${NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT_AFTER:-${admin_url_after}}"
tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${pulsar_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
pulsar_client_cp="${pulsar_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${pulsar_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${pulsar_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
IFS=: read -r -a pulsar_client_artifacts <<< "${pulsar_client_cp}"

if [[ "${broker_process_crash}" != "0" && "${broker_process_crash}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH must be 0 or 1" >&2
  exit 1
fi
if [[ "${broker_process_crash}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH requires NEREUS_DELAY_PULSAR_WITH_OXIA=1" >&2
  exit 1
fi
if [[ -n "${broker_recovery_state_dump_dir}" ]]; then
  mkdir -p "${broker_recovery_state_dump_dir}"
  rm -f "${broker_recovery_state_dump_dir}/before-process-crash.json" \
    "${broker_recovery_state_dump_dir}/after-fresh-process.json"
fi

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  if [[ "${with_oxia}" == "1" ]]; then
    "${oxia_compose[@]}" down --remove-orphans >/dev/null 2>&1 || true
  fi
  docker image rm "${image}" >/dev/null 2>&1 || true
  docker image rm "${oxia_image}" >/dev/null 2>&1 || true
  rm -rf "${image_context}"
  rm -rf "${runtime_dir}"
}
trap cleanup EXIT INT TERM

require_clean_pulsar_checkout() {
  test -z "$(git -C "${pulsar_dir}" status --porcelain)"
  test "$(git -C "${pulsar_dir}" branch --show-current)" = "nereus/delay-resource-guard-v1"
  git -C "${pulsar_dir}" merge-base --is-ancestor \
    8dae0236c0a0d405ed7f8303081080520fe91551 HEAD
}

wait_for_admin() {
  local url="$1"
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${url}/admin/v2/brokers/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Pulsar broker did not become ready: ${url}" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs >&2 || true
  return 1
}

wait_for_admin_stable() {
  local url="$1"
  local deadline=$((SECONDS + 120))
  local consecutive=0
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${url}/admin/v2/brokers/ready" >/dev/null 2>&1; then
      consecutive=$((consecutive + 1))
      if (( consecutive >= 3 )); then
        return 0
      fi
    else
      consecutive=0
    fi
    sleep 2
  done
  echo "Pulsar broker did not become stably ready: ${url}" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs >&2 || true
  return 1
}

wait_for_oxia() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if "${oxia_compose[@]}" exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
        >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Oxia did not become ready: ${oxia_project}" >&2
  "${oxia_compose[@]}" ps >&2 || true
  "${oxia_compose[@]}" logs >&2 || true
  return 1
}

run_worker() {
  local service_url="$1"
  local admin_url="$2"
  local mode="$3"
  local worker_environment=(env "GRADLE_USER_HOME=${gradle_user_home}"
    NEREUS_DELAY_PULSAR_LISTENER_NAME=external)
  local worker_gradle_args=(
    -PpulsarClientClasspath="${pulsar_client_cp}"
    -PpulsarRuntimeDir="${runtime_dir}/lib"
    -PpulsarServiceUrl="${service_url}"
    -PpulsarAdminUrl="${admin_url}"
    -PpulsarTopic="${restart_topic}"
    -PpulsarWorkerMode="${mode}"
    -PpulsarWorkerDestinationTopic="${destination_topic}"
    --no-daemon --console=plain
  )
  if [[ "${with_oxia}" == "1" ]]; then
    worker_environment+=("NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:${oxia_port}")
    worker_gradle_args+=("-PpulsarWithOxia=true")
  fi
  "${worker_environment[@]}" ./gradlew runRealPulsarWorkerSmoke "${worker_gradle_args[@]}"
}

run_broker_recovery_state_smoke() {
  local admin_url="$1"
  local endpoint_label="$2"
  local phase="$3"
  if [[ -z "${broker_recovery_state_dump_dir}" ]]; then
    return 0
  fi
  NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_CELL=pulsar-multi-broker-process-crash \
  NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_PHASE="${phase}" \
  NEREUS_DELAY_PULSAR_BROKER_RECOVERY_STATE_DUMP_DIR="${broker_recovery_state_dump_dir}" \
  NEREUS_DELAY_PULSAR_BROKER_RECOVERY_ADMIN_ENDPOINT="${endpoint_label}" \
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarBrokerRecoveryStateSmoke \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarBrokerRecoveryTopic="${restart_topic}" \
    --no-daemon --console=plain
}

cd "${delay_dir}"
require_clean_pulsar_checkout
test -s "${tarball}"
tar -xzf "${tarball}" -C "${runtime_dir}" --strip-components=1 "apache-pulsar-5.0.0-M1/lib"
test -n "$(find "${runtime_dir}/lib" -type f -name '*.jar' -print -quit)"
for artifact in "${pulsar_client_artifacts[@]}"; do
  test -s "${artifact}"
done
test -x "${delay_dir}/gradlew"

cp "${tarball}" "${image_context}/apache-pulsar-5.0.0-M1-bin.tar.gz"
cp "${script_dir}/Dockerfile.pulsar-p1" "${image_context}/Dockerfile"
cp "${script_dir}/pulsar-p1-entrypoint.sh" "${image_context}/pulsar-p1-entrypoint.sh"
cp "${script_dir}/pulsar-p1-cluster-entrypoint.sh" "${image_context}/pulsar-p1-cluster-entrypoint.sh"
docker build --pull=false -t "${image}" "${image_context}"
image_id="$(docker image inspect "${image}" --format '{{.Id}}')"

export PULSAR_P1_IMAGE="${image}"
export PULSAR_CLUSTER_NAME="${cluster_name}"
export PULSAR_BROKER_1_PORT="${broker_1_port}"
export PULSAR_WEB_1_PORT="${web_1_port}"
export PULSAR_BROKER_2_PORT="${broker_2_port}"
export PULSAR_WEB_2_PORT="${web_2_port}"

echo "P1 checkout: $(git -C "${pulsar_dir}" rev-parse HEAD)"
echo "P1 distribution SHA256: $(shasum -a 256 "${tarball}" | awk '{print $1}')"
echo "P1 client SHA256:"
for artifact in "${pulsar_client_artifacts[@]}"; do
  shasum -a 256 "${artifact}"
done
echo "P1 image ID: ${image_id}"
echo "Compose project: ${compose_project}"
echo "P1 ports: broker-1=${broker_1_port},web-1=${web_1_port},broker-2=${broker_2_port},web-2=${web_2_port}"
echo "Pulsar Worker Oxia authority: ${with_oxia}"

"${compose[@]}" up -d
wait_for_admin "${admin_url_before}"
wait_for_admin_stable "${admin_url_after}"

if [[ "${with_oxia}" == "1" ]]; then
  test -d "${oxia_checkout}"
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}" NEREUS_DELAY_OXIA_E2E_PORT="${oxia_port}" \
    "${oxia_compose[@]}" up --build -d
  wait_for_oxia
fi

run_worker "${service_url_before}" "${admin_url_after}" prepare
run_broker_recovery_state_smoke "${admin_url_before}" \
  "${broker_recovery_admin_endpoint_before}" before
if [[ "${broker_process_crash}" == "1" ]]; then
  "${compose[@]}" kill --signal KILL pulsar-broker-1
else
  "${compose[@]}" stop pulsar-broker-1
fi
wait_for_admin_stable "${admin_url_after}"
run_broker_recovery_state_smoke "${admin_url_after}" \
  "${broker_recovery_admin_endpoint_after}" after
run_worker "${service_url_failover}" "${admin_url_after}" resume
"${compose[@]}" start pulsar-broker-1
wait_for_admin "${admin_url_before}"

if [[ "${broker_process_crash}" == "1" ]]; then
  echo "Pulsar Broker process-crash failover E2E passed: broker-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through broker-2 with real Oxia authority, and broker-1 rejoined afterward."
else
  echo "Pulsar multi-Broker failover E2E passed: same-topic guarded Worker resumed through broker-2 after broker-1 stop, applied the source record, completed provider-driven physical Publish, ACKed the source and released its final checkpoint and owner assignment."
fi
