#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
gradle_user_home="${NEREUS_DELAY_PULSAR_GRADLE_USER_HOME:-/tmp/nereus-delay-pulsar-e2e-gradle}"
with_oxia="${NEREUS_DELAY_PULSAR_WITH_OXIA:-0}"
destination_response_loss="${NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS:-0}"
destination_response_loss_only="${NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_ONLY:-0}"
source_ack_response_loss="${NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS:-0}"
source_ack_response_loss_only="${NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_ONLY:-0}"
worker_destination_response_loss="${NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS:-0}"
worker_destination_response_loss_only="${NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_ONLY:-0}"
oxia_checkout="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
compose_project="nereus-delay-pulsar-e2e-$(date +%s)-$$"
oxia_project="nereus-delay-pulsar-oxia-e2e-${compose_project#nereus-delay-pulsar-e2e-}"
compose_file="${script_dir}/docker-compose.pulsar.yml"
compose=(docker compose -p "${compose_project}" -f "${compose_file}")
oxia_compose_file="${script_dir}/docker-compose.oxia.yml"
oxia_compose=(docker compose -p "${oxia_project}" -f "${oxia_compose_file}")
image="nereus-delay-pulsar-p1:${compose_project}"
oxia_image="${oxia_project}-oxia"
image_context="$(mktemp -d -t nereus-delay-p1-image.XXXXXX)"
runtime_dir="$(mktemp -d -t nereus-delay-p1-runtime.XXXXXX)"
base_port=$((19650 + ($$ % 300)))
broker_port="${PULSAR_BROKER_PORT:-${base_port}}"
web_port="${PULSAR_WEB_PORT:-$((base_port + 1))}"
oxia_port="${NEREUS_DELAY_PULSAR_OXIA_PORT:-16657}"
tarball="${NEREUS_DELAY_PULSAR_TARBALL:-${pulsar_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz}"
topic="${PULSAR_DELAY_E2E_TOPIC:-p1-real-client-${compose_project##*-}}"
mutation_topic="${PULSAR_DELAY_MUTATION_TOPIC:-p1-mutation-${compose_project##*-}}"
mutation_worker_topic="${PULSAR_DELAY_MUTATION_WORKER_TOPIC:-p1-mutation-worker-${compose_project##*-}}"
route_worker_topic="${PULSAR_DELAY_ROUTE_WORKER_TOPIC:-p1-route-worker-${compose_project##*-}}"
destination_topic="${PULSAR_DELAY_DESTINATION_TOPIC:-p1-destination-${compose_project##*-}}"
worker_destination_topic="${PULSAR_DELAY_WORKER_DESTINATION_TOPIC:-p1-worker-destination-${compose_project##*-}}"
service_url="pulsar://127.0.0.1:${broker_port}"
admin_url="http://127.0.0.1:${web_port}"
pulsar_client_cp="${pulsar_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:${pulsar_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:${pulsar_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
IFS=: read -r -a pulsar_client_artifacts <<< "${pulsar_client_cp}"

if [[ "${destination_response_loss}" != "0" && "${destination_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS must be 0 or 1" >&2
  exit 1
fi
if [[ "${destination_response_loss_only}" != "0" && "${destination_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${destination_response_loss_only}" == "1" && "${destination_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_ONLY requires NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${source_ack_response_loss}" != "0" && "${source_ack_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS must be 0 or 1" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" != "0" && "${source_ack_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" == "1" && "${source_ack_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_ONLY requires NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss}" != "0" && "${worker_destination_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_only}" != "0" && "${worker_destination_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_only}" == "1" && "${worker_destination_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_ONLY requires NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1" >&2
  exit 1
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

wait_for_service() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl --silent --fail "${admin_url}/admin/v2/clusters" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Pulsar standalone did not become ready: ${admin_url}" >&2
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
export PULSAR_BROKER_PORT="${broker_port}"
export PULSAR_WEB_PORT="${web_port}"

echo "P1 checkout: $(git -C "${pulsar_dir}" rev-parse HEAD)"
echo "P1 distribution SHA256: $(shasum -a 256 "${tarball}" | awk '{print $1}')"
echo "P1 client SHA256:"
for artifact in "${pulsar_client_artifacts[@]}"; do
  shasum -a 256 "${artifact}"
done
echo "P1 image ID: ${image_id}"
echo "P1 runtime library count: $(find "${runtime_dir}/lib" -type f -name '*.jar' | wc -l | tr -d ' ')"
echo "Compose project: ${compose_project}"
echo "P1 ports: broker=${broker_port},web=${web_port}"
echo "Pulsar Worker Oxia authority: ${with_oxia}"

"${compose[@]}" up -d
wait_for_service

if [[ "${with_oxia}" == "1" ]]; then
  test -d "${oxia_checkout}"
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}" NEREUS_DELAY_OXIA_E2E_PORT="${oxia_port}" \
    "${oxia_compose[@]}" up --build -d
  wait_for_oxia
fi

if [[ "${destination_response_loss}" == "1" ]]; then
  export NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1
fi
if [[ "${source_ack_response_loss}" == "1" ]]; then
  export NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1
fi
if [[ "${worker_destination_response_loss}" == "1" ]]; then
  export NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1
fi

if [[ "${destination_response_loss_only}" == "1" ]]; then
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarDestinationSmoke \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarDestinationTopic="${destination_topic}" \
    --no-daemon --console=plain
  echo "Pulsar destination committed response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and exact guarded payload readback."
  exit 0
fi

if [[ "${source_ack_response_loss_only}" == "1" ]]; then
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarWorkerSmoke \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${topic}" \
    -PpulsarWorkerMode=run \
    -PpulsarWorkerDestinationTopic= \
    --no-daemon --console=plain
  echo "Pulsar Worker source ACK response-loss E2E passed: real ACK response loss was retried on the same source record and the bounded Worker vertical completed."
  exit 0
fi

if [[ "${worker_destination_response_loss_only}" == "1" ]]; then
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarWorkerSmoke \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarTopic="${topic}" \
    -PpulsarWorkerMode=run \
    -PpulsarWorkerDestinationTopic="${worker_destination_topic}" \
    --no-daemon --console=plain
  echo "Pulsar Worker destination response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and the source-applied Outcome completed."
  exit 0
fi

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarServiceSmoke \
  -PpulsarClientClasspath="${pulsar_client_cp}" \
  -PpulsarRuntimeDir="${runtime_dir}/lib" \
  -PpulsarServiceUrl="${service_url}" \
  -PpulsarAdminUrl="${admin_url}" \
  -PpulsarTopic="${topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarDestinationSmoke \
  -PpulsarClientClasspath="${pulsar_client_cp}" \
  -PpulsarRuntimeDir="${runtime_dir}/lib" \
  -PpulsarServiceUrl="${service_url}" \
  -PpulsarAdminUrl="${admin_url}" \
  -PpulsarDestinationTopic="${destination_topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarSourceSmoke \
  -PpulsarClientClasspath="${pulsar_client_cp}" \
  -PpulsarRuntimeDir="${runtime_dir}/lib" \
  -PpulsarServiceUrl="${service_url}" \
  -PpulsarAdminUrl="${admin_url}" \
  -PpulsarTopic="${topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealPulsarMutationSmoke \
  -PpulsarClientClasspath="${pulsar_client_cp}" \
  -PpulsarRuntimeDir="${runtime_dir}/lib" \
  -PpulsarServiceUrl="${service_url}" \
  -PpulsarAdminUrl="${admin_url}" \
  -PpulsarMutationTopic="${mutation_topic}" \
  --no-daemon --console=plain

run_mutation_worker_smoke() {
  local worker_topic="$1"
  local worker_environment=(env "GRADLE_USER_HOME=${gradle_user_home}")
  local worker_gradle_args=(
    -PpulsarClientClasspath="${pulsar_client_cp}"
    -PpulsarRuntimeDir="${runtime_dir}/lib"
    -PpulsarServiceUrl="${service_url}"
    -PpulsarAdminUrl="${admin_url}"
    -PpulsarMutationWorkerTopic="${worker_topic}"
  )
  if [[ "${with_oxia}" == "1" ]]; then
    worker_environment+=("NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:${oxia_port}")
    worker_gradle_args+=("-PpulsarWithOxia=true")
  fi

  "${worker_environment[@]}" ./gradlew runRealPulsarMutationWorkerSmoke \
    "${worker_gradle_args[@]}" \
    --no-daemon --console=plain
}

run_mutation_worker_smoke "${mutation_worker_topic}"

run_route_worker_smoke() {
  if [[ "${with_oxia}" != "1" ]]; then
    return 0
  fi
  local route_environment=(env "GRADLE_USER_HOME=${gradle_user_home}"
    "NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:${oxia_port}")
  "${route_environment[@]}" ./gradlew runRealPulsarRouteWorkerSmoke \
    -PpulsarClientClasspath="${pulsar_client_cp}" \
    -PpulsarRuntimeDir="${runtime_dir}/lib" \
    -PpulsarServiceUrl="${service_url}" \
    -PpulsarAdminUrl="${admin_url}" \
    -PpulsarRouteWorkerTopic="${route_worker_topic}" \
    -PpulsarWithOxia=true \
    --no-daemon --console=plain
}

run_route_worker_smoke

run_worker_smoke() {
  local worker_topic="$1"
  local worker_mode="$2"
  local worker_environment=(env "GRADLE_USER_HOME=${gradle_user_home}")
  local worker_gradle_args=(
    -PpulsarClientClasspath="${pulsar_client_cp}"
    -PpulsarRuntimeDir="${runtime_dir}/lib"
    -PpulsarServiceUrl="${service_url}"
    -PpulsarAdminUrl="${admin_url}"
    -PpulsarTopic="${worker_topic}"
    -PpulsarWorkerMode="${worker_mode}"
    -PpulsarWorkerDestinationTopic="${worker_destination_topic}"
  )
  if [[ "${with_oxia}" == "1" ]]; then
    worker_environment+=("NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:${oxia_port}")
    worker_gradle_args+=("-PpulsarWithOxia=true")
  fi

  "${worker_environment[@]}" ./gradlew runRealPulsarWorkerSmoke \
    "${worker_gradle_args[@]}" \
    --no-daemon --console=plain
}

run_worker_smoke "${topic}" run

restart_topic="${PULSAR_DELAY_RESTART_TOPIC:-p1-worker-restart-${compose_project##*-}}"
run_worker_smoke "${restart_topic}" prepare
"${compose[@]}" restart pulsar
wait_for_service
run_worker_smoke "${restart_topic}" resume

echo "Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, source-bound typed destination SEND ACK/payload readback, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, source-applied physical publish with typed Outcome and payload readback, ACK handoff, and broker-restart resume."
