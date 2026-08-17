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
worker_destination_response_loss_process_crash_only="${NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY:-0}"
worker_admission_response_loss="${NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS:-0}"
worker_admission_response_loss_only="${NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY:-0}"
worker_admission_response_loss_process_crash_only="${NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY:-0}"
worker_process_crash="${NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH:-0}"
worker_process_crash_only="${NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY:-0}"
multi_shard_only="${NEREUS_DELAY_PULSAR_MULTI_SHARD_ONLY:-0}"
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
if [[ "${worker_destination_response_loss_process_crash_only}" != "0" && "${worker_destination_response_loss_process_crash_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_process_crash_only}" == "1" && "${worker_destination_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY requires NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss}" != "0" && "${worker_admission_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss_only}" != "0" && "${worker_admission_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss_only}" == "1" && "${worker_admission_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY requires NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss_process_crash_only}" != "0" && "${worker_admission_response_loss_process_crash_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss_process_crash_only}" == "1" && "${worker_admission_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY requires NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${worker_process_crash}" != "0" && "${worker_process_crash}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" != "0" && "${worker_process_crash_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" == "1" && "${worker_process_crash}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY requires NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH=1" >&2
  exit 1
fi
if [[ "${multi_shard_only}" != "0" && "${multi_shard_only}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_MULTI_SHARD_ONLY must be 0 or 1" >&2
  exit 1
fi
focused_response_loss_modes=(
  "${destination_response_loss_only}"
  "${source_ack_response_loss_only}"
  "${worker_destination_response_loss_only}"
  "${worker_destination_response_loss_process_crash_only}"
  "${worker_admission_response_loss_only}"
  "${worker_admission_response_loss_process_crash_only}"
)
focused_response_loss_count=0
for focused_response_loss_mode in "${focused_response_loss_modes[@]}"; do
  if [[ "${focused_response_loss_mode}" == "1" ]]; then
    focused_response_loss_count=$((focused_response_loss_count + 1))
  fi
done
if (( focused_response_loss_count > 1 )); then
  echo "Pulsar response-loss focused modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${multi_shard_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_MULTI_SHARD_ONLY requires NEREUS_DELAY_PULSAR_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${multi_shard_only}" == "1" && "${focused_response_loss_count}" != "0" ]]; then
  echo "NEREUS_DELAY_PULSAR_MULTI_SHARD_ONLY cannot be combined with response-loss focused mode" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY requires NEREUS_DELAY_PULSAR_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" == "1" && "${focused_response_loss_count}" != "0" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY cannot be combined with response-loss focused mode" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" == "1" && "${multi_shard_only}" == "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY cannot be combined with multi-shard mode" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_process_crash_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY requires NEREUS_DELAY_PULSAR_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_process_crash_only}" == "1" && "${worker_process_crash_only}" == "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY cannot be combined with NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss_process_crash_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY requires NEREUS_DELAY_PULSAR_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss_process_crash_only}" == "1" && "${worker_process_crash_only}" == "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY cannot be combined with NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY" >&2
  exit 1
fi
if [[ "${worker_admission_response_loss_process_crash_only}" == "1" && "${multi_shard_only}" == "1" ]]; then
  echo "NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY cannot be combined with multi-shard mode" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_process_crash_only}" == "1" && "${worker_admission_response_loss_process_crash_only}" == "1" ]]; then
  echo "Pulsar Worker destination and admission response-loss process-crash modes are mutually exclusive" >&2
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
  if [[ -n "${worker_process_crash_dir:-}" ]]; then
    rm -rf "${worker_process_crash_dir}"
  fi
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
if [[ "${worker_destination_response_loss_process_crash_only}" == "1" ]]; then
  export NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY=1
fi
if [[ "${worker_admission_response_loss}" == "1" ]]; then
  export NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1
fi

run_focused_worker_smoke() {
  local worker_topic="$1"
  local worker_destination="$2"
  local worker_mode="${3:-run}"
  local worker_environment=(env "GRADLE_USER_HOME=${gradle_user_home}")
  local worker_gradle_args=(
    -PpulsarClientClasspath="${pulsar_client_cp}"
    -PpulsarRuntimeDir="${runtime_dir}/lib"
    -PpulsarServiceUrl="${service_url}"
    -PpulsarAdminUrl="${admin_url}"
    -PpulsarTopic="${worker_topic}"
    -PpulsarWorkerMode="${worker_mode}"
    -PpulsarWorkerDestinationTopic="${worker_destination}"
  )
  if [[ "${with_oxia}" == "1" ]]; then
    worker_environment+=("NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:${oxia_port}")
    worker_gradle_args+=("-PpulsarWithOxia=true")
  fi

  "${worker_environment[@]}" ./gradlew runRealPulsarWorkerSmoke \
    "${worker_gradle_args[@]}" \
    --no-daemon --console=plain
}

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

if [[ "${multi_shard_only}" == "1" ]]; then
  export NEREUS_DELAY_PULSAR_ROUTE_WORKER_SHARDS=2
  run_route_worker_smoke
  echo "Pulsar native multi-shard Worker fleet E2E passed: one signed Route covered two guarded SUBSCRIBE barriers, two real Oxia Assignment/Owner Lease CAS paths admitted two native source consumers, one fair fleet applied/ACKed both partitions, and both final checkpoints/assignments were released."
  exit 0
fi

if [[ "${worker_destination_response_loss_process_crash_only}" == "1" ]]; then
  worker_process_crash_dir="$(mktemp -d -t nereus-delay-pulsar-worker-destination-process-crash.XXXXXX)"
  worker_process_crash_gate="${worker_process_crash_dir}/cut"
  worker_process_crash_pid_file="${worker_process_crash_dir}/pid"
  worker_process_crash_log="${worker_process_crash_dir}/crash.log"
  worker_process_crash_resume_log="${worker_process_crash_dir}/resume.log"
  worker_process_crash_root="${worker_process_crash_dir}/state"
  worker_process_crash_state_dump_dir="${NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_STATE_DUMP_DIR:-${worker_process_crash_dir}/state-dumps}"
  mkdir -p "${worker_process_crash_state_dump_dir}"
  rm -f "${worker_process_crash_state_dump_dir}/before-process-crash.json" \
    "${worker_process_crash_state_dump_dir}/after-fresh-process.json"
  export NEREUS_DELAY_PULSAR_WORKER_ROOT="${worker_process_crash_root}"
  export NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR="${worker_process_crash_state_dump_dir}"
  export NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1
  export NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY=1
  export NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_GATE="${worker_process_crash_gate}"
  export NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_PID_FILE="${worker_process_crash_pid_file}"
  rm -f "${worker_process_crash_gate}" "${worker_process_crash_pid_file}"
  run_focused_worker_smoke "${topic}" "" prepare
  set +e
  run_focused_worker_smoke "${topic}" "${worker_destination_topic}" crash-wait >"${worker_process_crash_log}" 2>&1 &
  worker_process_crash_launcher_pid=$!
  set -e
  crash_gate_deadline=$((SECONDS + 180))
  while (( SECONDS < crash_gate_deadline )); do
    if [[ -f "${worker_process_crash_gate}" && -s "${worker_process_crash_pid_file}" ]]; then
      break
    fi
    if ! kill -0 "${worker_process_crash_launcher_pid}" >/dev/null 2>&1; then
      wait "${worker_process_crash_launcher_pid}" || true
      cat "${worker_process_crash_log}" >&2
      echo "Pulsar Worker destination response-loss process-crash JVM exited before its cut gate" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ ! -f "${worker_process_crash_gate}" || ! -s "${worker_process_crash_pid_file}" ]]; then
    cat "${worker_process_crash_log}" >&2
    echo "Pulsar Worker destination response-loss process-crash JVM did not reach its cut gate" >&2
    exit 1
  fi
  worker_process_pid="$(<"${worker_process_crash_pid_file}")"
  if [[ ! "${worker_process_pid}" =~ ^[0-9]+$ ]] || ! kill -0 "${worker_process_pid}" >/dev/null 2>&1; then
    cat "${worker_process_crash_log}" >&2
    echo "Pulsar Worker destination response-loss process-crash JVM PID is not alive at the cut gate" >&2
    exit 1
  fi
  rg -F "Pulsar Worker destination response-loss process-crash cut reached" "${worker_process_crash_log}"
  kill -KILL "${worker_process_pid}"
  rm -f "${worker_process_crash_gate}"
  set +e
  wait "${worker_process_crash_launcher_pid}"
  worker_process_crash_status=$?
  set -e
  worker_process_crash_launcher_pid=""
  if [[ "${worker_process_crash_status}" == "0" ]]; then
    echo "Pulsar Worker destination response-loss process-crash JVM unexpectedly returned success after SIGKILL" >&2
    exit 1
  fi
  if ! jq -e '
        .schema == "nereus-delay-chaos-durable-state-dump-v1"
        and .cell == "pulsar-worker-destination-response-loss-process-crash"
        and .phase == "DESTINATION_RESPONSE_LOSS_PERSISTED"
        and .attempt_state == "PUBLISHING"
        and .outcome_applied == false
        and .durable_store_read == true
        and .dump_forced == true
        and (.publish_attempt_id | (type == "string" and length > 0))
        and (.message_id | (type == "string" and length > 0))
      ' "${worker_process_crash_state_dump_dir}/before-process-crash.json" >/dev/null; then
    cat "${worker_process_crash_state_dump_dir}/before-process-crash.json" >&2
    echo "Pulsar Worker destination response-loss pre-crash durable state dump failed validation" >&2
    exit 1
  fi
  unset NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS
  resume_attempts="${NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_RESUME_ATTEMPTS:-90}"
  for attempt in $(seq 1 "${resume_attempts}"); do
    set +e
    run_focused_worker_smoke "${topic}" "${worker_destination_topic}" resume >"${worker_process_crash_resume_log}" 2>&1
    resume_status=$?
    set -e
    if [[ "${resume_status}" == "0" ]]; then
      cat "${worker_process_crash_resume_log}"
      break
    fi
    if [[ "${attempt}" == "${resume_attempts}" ]]; then
      cat "${worker_process_crash_resume_log}" >&2
      echo "Pulsar Worker destination response-loss fresh-process recovery did not reacquire the real Oxia lease" >&2
      exit 1
    fi
    sleep 1
  done
  rg -F --quiet "Pulsar Worker destination response-loss fresh-process recovery passed" \
    "${worker_process_crash_resume_log}" \
    || { cat "${worker_process_crash_resume_log}" >&2; exit 1; }
  if ! jq -e '
        .schema == "nereus-delay-chaos-durable-state-dump-v1"
        and .cell == "pulsar-worker-destination-response-loss-process-crash"
        and .phase == "RECOVERED_AFTER_FRESH_PROCESS"
        and .attempt_state == "PUBLISHED"
        and .outcome_applied == true
        and .durable_store_read == true
        and .dump_forced == true
        and (.publish_attempt_id | (type == "string" and length > 0))
        and (.message_id | (type == "string" and length > 0))
      ' "${worker_process_crash_state_dump_dir}/after-fresh-process.json" >/dev/null; then
    cat "${worker_process_crash_state_dump_dir}/after-fresh-process.json" >&2
    echo "Pulsar Worker destination response-loss post-recovery durable state dump failed validation" >&2
    exit 1
  fi
  echo "Pulsar Worker destination response-loss fresh-process recovery E2E passed: durable PUBLISH_OUTCOME survived SIGKILL, a fresh Worker applied it, and the exact destination payload was not resent."
  exit 0
fi

if [[ "${worker_admission_response_loss_process_crash_only}" == "1" ]]; then
  worker_process_crash_dir="$(mktemp -d -t nereus-delay-pulsar-worker-admission-process-crash.XXXXXX)"
  worker_process_crash_gate="${worker_process_crash_dir}/cut"
  worker_process_crash_pid_file="${worker_process_crash_dir}/pid"
  worker_process_crash_log="${worker_process_crash_dir}/crash.log"
  worker_process_crash_resume_log="${worker_process_crash_dir}/resume.log"
  worker_process_crash_root="${worker_process_crash_dir}/state"
  worker_process_crash_state_dump_dir="${NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_STATE_DUMP_DIR:-${worker_process_crash_dir}/state-dumps}"
  mkdir -p "${worker_process_crash_state_dump_dir}"
  rm -f "${worker_process_crash_state_dump_dir}/before-process-crash.json" \
    "${worker_process_crash_state_dump_dir}/after-fresh-process.json"
  export NEREUS_DELAY_PULSAR_WORKER_ROOT="${worker_process_crash_root}"
  export NEREUS_DELAY_PULSAR_CHAOS_STATE_DUMP_DIR="${worker_process_crash_state_dump_dir}"
  export NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1
  export NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY=1
  export NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_GATE="${worker_process_crash_gate}"
  export NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_PID_FILE="${worker_process_crash_pid_file}"
  rm -f "${worker_process_crash_gate}" "${worker_process_crash_pid_file}"
  run_focused_worker_smoke "${topic}" "" prepare
  set +e
  run_focused_worker_smoke "${topic}" "${worker_destination_topic}" crash-wait >"${worker_process_crash_log}" 2>&1 &
  worker_process_crash_launcher_pid=$!
  set -e
  crash_gate_deadline=$((SECONDS + 180))
  while (( SECONDS < crash_gate_deadline )); do
    if [[ -f "${worker_process_crash_gate}" && -s "${worker_process_crash_pid_file}" ]]; then
      break
    fi
    if ! kill -0 "${worker_process_crash_launcher_pid}" >/dev/null 2>&1; then
      wait "${worker_process_crash_launcher_pid}" || true
      cat "${worker_process_crash_log}" >&2
      echo "Pulsar Worker admission response-loss process-crash JVM exited before its cut gate" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ ! -f "${worker_process_crash_gate}" || ! -s "${worker_process_crash_pid_file}" ]]; then
    cat "${worker_process_crash_log}" >&2
    echo "Pulsar Worker admission response-loss process-crash JVM did not reach its cut gate" >&2
    exit 1
  fi
  worker_process_pid="$(<"${worker_process_crash_pid_file}")"
  if [[ ! "${worker_process_pid}" =~ ^[0-9]+$ ]] || ! kill -0 "${worker_process_pid}" >/dev/null 2>&1; then
    cat "${worker_process_crash_log}" >&2
    echo "Pulsar Worker admission response-loss process-crash JVM PID is not alive at the cut gate" >&2
    exit 1
  fi
  cat "${worker_process_crash_log}"
  kill -KILL "${worker_process_pid}"
  rm -f "${worker_process_crash_gate}"
  set +e
  wait "${worker_process_crash_launcher_pid}"
  worker_process_crash_status=$?
  set -e
  worker_process_crash_launcher_pid=""
  if [[ "${worker_process_crash_status}" == "0" ]]; then
    echo "Pulsar Worker admission response-loss process-crash JVM unexpectedly returned success after SIGKILL" >&2
    exit 1
  fi
  if [[ ! -s "${worker_process_crash_state_dump_dir}/before-process-crash.json" ]]; then
    echo "Pulsar Worker admission response-loss process-crash did not emit the pre-crash durable state dump" >&2
    exit 1
  fi
  if ! jq -e '
        .schema == "nereus-delay-chaos-durable-state-dump-v1"
        and .cell == "pulsar-worker-admission-response-loss-process-crash"
        and .phase == "ADMISSION_RESPONSE_LOSS_PERSISTED"
        and .attempt_state == "PUBLISHING"
        and .outcome_applied == false
        and .durable_store_read == true
        and .dump_forced == true
        and (.publish_attempt_id | (type == "string" and length > 0))
        and (.attempt_source_position | (type == "string" and length > 0))
      ' "${worker_process_crash_state_dump_dir}/before-process-crash.json" >/dev/null; then
    cat "${worker_process_crash_state_dump_dir}/before-process-crash.json" >&2
    echo "Pulsar Worker admission response-loss pre-crash durable state dump failed validation" >&2
    exit 1
  fi
  unset NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS
  resume_attempts="${NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_RESUME_ATTEMPTS:-90}"
  for attempt in $(seq 1 "${resume_attempts}"); do
    set +e
    run_focused_worker_smoke "${topic}" "${worker_destination_topic}" resume >"${worker_process_crash_resume_log}" 2>&1
    resume_status=$?
    set -e
    if [[ "${resume_status}" == "0" ]]; then
      cat "${worker_process_crash_resume_log}"
      break
    fi
    if [[ "${attempt}" == "${resume_attempts}" ]]; then
      cat "${worker_process_crash_resume_log}" >&2
      echo "Pulsar Worker admission response-loss fresh-process recovery did not reacquire the real Oxia lease" >&2
      exit 1
    fi
    sleep 1
  done
  rg -F --quiet "Pulsar Worker vertical smoke passed" "${worker_process_crash_resume_log}" \
    || { cat "${worker_process_crash_resume_log}" >&2; exit 1; }
  if [[ ! -s "${worker_process_crash_state_dump_dir}/after-fresh-process.json" ]]; then
    echo "Pulsar Worker admission response-loss fresh process did not emit the post-recovery durable state dump" >&2
    exit 1
  fi
  if ! jq -e '
        .schema == "nereus-delay-chaos-durable-state-dump-v1"
        and .cell == "pulsar-worker-admission-response-loss-process-crash"
        and .phase == "RECOVERED_AFTER_FRESH_PROCESS"
        and .attempt_state == "PUBLISHED"
        and .outcome_applied == true
        and .durable_store_read == true
        and .dump_forced == true
        and (.applied_source_position | (type == "string" and length > 0))
      ' "${worker_process_crash_state_dump_dir}/after-fresh-process.json" >/dev/null; then
    cat "${worker_process_crash_state_dump_dir}/after-fresh-process.json" >&2
    echo "Pulsar Worker admission response-loss post-recovery durable state dump failed validation" >&2
    exit 1
  fi
  echo "Pulsar Worker Publish Admission response-loss fresh-process recovery E2E passed: durable PUBLISHING dump survived SIGKILL, a fresh Worker JVM reread it, and the final PUBLISHED state was durably observed."
  exit 0
fi

if [[ "${worker_process_crash_only}" == "1" ]]; then
  worker_process_crash_dir="$(mktemp -d -t nereus-delay-pulsar-worker-process-crash.XXXXXX)"
  worker_process_crash_gate="${worker_process_crash_dir}/cut"
  worker_process_crash_pid_file="${worker_process_crash_dir}/pid"
  worker_process_crash_log="${worker_process_crash_dir}/crash.log"
  worker_process_crash_resume_log="${worker_process_crash_dir}/resume.log"
  worker_process_crash_root="${worker_process_crash_dir}/state"
  export NEREUS_DELAY_PULSAR_WORKER_ROOT="${worker_process_crash_root}"
  export NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_GATE="${worker_process_crash_gate}"
  export NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_PID_FILE="${worker_process_crash_pid_file}"
  rm -f "${worker_process_crash_gate}" "${worker_process_crash_pid_file}"
  run_focused_worker_smoke "${topic}" "" prepare
  set +e
  run_focused_worker_smoke "${topic}" "" crash-wait >"${worker_process_crash_log}" 2>&1 &
  worker_process_crash_launcher_pid=$!
  set -e
  crash_gate_deadline=$((SECONDS + 180))
  while (( SECONDS < crash_gate_deadline )); do
    if [[ -f "${worker_process_crash_gate}" && -s "${worker_process_crash_pid_file}" ]]; then
      break
    fi
    if ! kill -0 "${worker_process_crash_launcher_pid}" >/dev/null 2>&1; then
      wait "${worker_process_crash_launcher_pid}" || true
      cat "${worker_process_crash_log}" >&2
      echo "Pulsar Worker process-crash JVM exited before its cut gate" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ ! -f "${worker_process_crash_gate}" || ! -s "${worker_process_crash_pid_file}" ]]; then
    cat "${worker_process_crash_log}" >&2
    echo "Pulsar Worker process-crash JVM did not reach its cut gate" >&2
    exit 1
  fi
  worker_process_pid="$(<"${worker_process_crash_pid_file}")"
  if ! kill -0 "${worker_process_pid}" >/dev/null 2>&1; then
    cat "${worker_process_crash_log}" >&2
    echo "Pulsar Worker process-crash JVM PID is not alive at the cut gate" >&2
    exit 1
  fi
  cat "${worker_process_crash_log}"
  kill -KILL "${worker_process_pid}"
  rm -f "${worker_process_crash_gate}"
  set +e
  wait "${worker_process_crash_launcher_pid}"
  worker_process_crash_status=$?
  set -e
  worker_process_crash_launcher_pid=""
  if [[ "${worker_process_crash_status}" == "0" ]]; then
    echo "Pulsar Worker process-crash JVM unexpectedly returned success after SIGKILL" >&2
    exit 1
  fi
  for attempt in $(seq 1 90); do
    set +e
    run_focused_worker_smoke "${topic}" "" resume >"${worker_process_crash_resume_log}" 2>&1
    resume_status=$?
    set -e
    if [[ "${resume_status}" == "0" ]]; then
      cat "${worker_process_crash_resume_log}"
      break
    fi
    if [[ "${attempt}" == "90" ]]; then
      cat "${worker_process_crash_resume_log}" >&2
      echo "Pulsar Worker process-crash recovery did not reacquire the real Oxia lease" >&2
      exit 1
    fi
    sleep 1
  done
  rg -F --quiet "Pulsar Worker vertical smoke passed" "${worker_process_crash_resume_log}" \
    || { cat "${worker_process_crash_resume_log}" >&2; exit 1; }
  echo "Pulsar Worker process-crash recovery E2E passed: a real Worker JVM was SIGKILLed after opening the guarded source/runtime with the next record unACKed, and a fresh JVM reopened the exact local Store, reacquired the real Oxia lease, replayed and ACKed the source record, and published the final checkpoint."
  exit 0
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
  run_focused_worker_smoke "${topic}" ""
  echo "Pulsar Worker source ACK response-loss E2E passed: real ACK response loss was retried on the same source record and the bounded Worker vertical completed."
  exit 0
fi

if [[ "${worker_destination_response_loss_only}" == "1" ]]; then
  run_focused_worker_smoke "${topic}" "${worker_destination_topic}"
  echo "Pulsar Worker destination response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and the source-applied Outcome completed."
  exit 0
fi

if [[ "${worker_admission_response_loss_only}" == "1" ]]; then
  run_focused_worker_smoke "${topic}" "${worker_destination_topic}"
  echo "Pulsar Worker Publish Admission response-loss E2E passed: the real Shard Log mutation was persisted, its append response was discarded, and exact source replay recovered the PUBLISHING admission."
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
