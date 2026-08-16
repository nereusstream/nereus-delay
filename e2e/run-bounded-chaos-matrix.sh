#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-bounded-chaos.XXXXXX)}"
mkdir -p "${artifact_dir}"

delay_base="2dfc3289ffdbe9cf9d7f4d0de1d701493d1b49a6"
kafka_base="c300006a7705c240642db6950b5a95fec982bfc5"
pulsar_base="8dae0236c0a0d405ed7f8303081080520fe91551"
matrix_status=0
declare -a cell_results=()
declare -a observed_projects=()

require_checkout() {
  local path="$1"
  local branch="$2"
  local base="$3"
  local label="$4"
  test -d "${path}"
  test -z "$(git -C "${path}" status --porcelain)"
  test "$(git -C "${path}" branch --show-current)" = "${branch}"
  git -C "${path}" merge-base --is-ancestor "${base}" HEAD
  echo "${label} source: $(git -C "${path}" rev-parse HEAD)"
  echo "${label} branch: ${branch} (base ${base})"
}

require_checkout "${delay_dir}" "nereus/delay-full-implementation-v1" "${delay_base}" "Delay"
require_checkout "${kafka_dir}" "nereus/delay-guarded-producer-v1" "${kafka_base}" "Kafka"
require_checkout "${pulsar_dir}" "nereus/delay-resource-guard-v1" "${pulsar_base}" "Pulsar"
require_checkout "${oxia_dir}" "main" "$(git -C "${oxia_dir}" rev-parse HEAD)" "Oxia"

echo "Bounded chaos matrix artifact directory: ${artifact_dir}"
echo "Matrix scope: current-source focused cuts only; this is not a V1 release gate by itself."

run_cell() {
  local name="$1"
  shift
  local log="${artifact_dir}/${name}.log"
  local result_file="${artifact_dir}/${name}.result"
  echo
  echo "===== CELL ${name} ====="
  set +e
  "$@" 2>&1 | tee "${log}"
  local status=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "${status}" >"${result_file}"
  rg -n "(E2E passed|smoke passed|BUILD SUCCESSFUL|source-applied physical publish passed|authority passed)" "${log}" \
    >"${artifact_dir}/${name}.summary" || true
  cell_results+=("${name}=${status}")
  if [[ "${status}" != 0 ]]; then
    matrix_status=1
    echo "CELL ${name}: FAIL (${status})"
  else
    echo "CELL ${name}: PASS (bounded receipt; not release certification)"
  fi
}

run_cell kafka-broker-process-crash env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${artifact_dir}/kafka-broker-process-crash-gradle" \
  KAFKA_BROKER_1_PORT=31200 KAFKA_BROKER_2_PORT=31201 KAFKA_BROKER_3_PORT=31202 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31210 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-worker-ack-process-crash env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${artifact_dir}/kafka-worker-ack-process-crash-gradle" \
  KAFKA_BROKER_1_PORT=31220 KAFKA_BROKER_2_PORT=31221 KAFKA_BROKER_3_PORT=31222 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31230 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-broker-tcp-cut env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${artifact_dir}/kafka-broker-tcp-cut-gradle" \
  KAFKA_BROKER_1_PORT=31240 KAFKA_BROKER_2_PORT=31241 KAFKA_BROKER_3_PORT=31242 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31250 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell pulsar-worker-process-crash env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH=1 \
  NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${artifact_dir}/pulsar-worker-process-crash-gradle" \
  PULSAR_BROKER_PORT=31260 PULSAR_WEB_PORT=31261 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31270 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell pulsar-worker-admission-response-loss env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1 \
  NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY=1 \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${artifact_dir}/pulsar-worker-admission-response-loss-gradle" \
  PULSAR_BROKER_PORT=31280 PULSAR_WEB_PORT=31281 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31290 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell checkpoint-reaping env \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_E2E_GRADLE_USER_HOME="${artifact_dir}/checkpoint-reaping-gradle" \
  NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31300 \
  NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31301 \
  "${script_dir}/run-oxia-minio-checkpoint-e2e.sh"

printf '\n===== MATRIX SUMMARY =====\n'
for result in "${cell_results[@]}"; do
  echo "${result}"
done
echo "artifact_dir=${artifact_dir}"
echo "matrix_status=${matrix_status}"

if [[ "${matrix_status}" != 0 ]]; then
  exit 1
fi
