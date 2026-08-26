#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_CERTIFIED_OPERATIONS_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-certified-operations.XXXXXX)}"
gradle_home="${NEREUS_DELAY_CERTIFIED_OPERATIONS_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
soak_artifact="${NEREUS_DELAY_CERTIFIED_OPERATIONS_SOAK_ARTIFACT:-}"
profile_id="${NEREUS_DELAY_CERTIFIED_OPERATIONS_PROFILE_ID:-nereus-delay-rc1-operations-r1}"
expected_branch="nereus/delay-full-implementation"
bounded_dir="${artifact_dir}/bounded-operations"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if ! command -v jq >/dev/null 2>&1; then
  echo "certified operations drills require jq" >&2
  exit 1
fi
if [[ -e "${artifact_dir}" && -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "certified operations artifact directory must be empty: ${artifact_dir}" >&2
  exit 1
fi
mkdir -p "${artifact_dir}" "${gradle_home}"

require_checkout() {
  local label="$1"
  local path="$2"
  local branch="$3"
  if [[ ! -e "${path}/.git" ]]; then
    echo "${label} checkout is missing: ${path}" >&2
    return 1
  fi
  if [[ -n "$(git -C "${path}" status --porcelain)" ]]; then
    echo "${label} checkout is dirty: ${path}" >&2
    return 1
  fi
  if [[ "$(git -C "${path}" branch --show-current)" != "${branch}" ]]; then
    echo "${label} checkout has an unexpected branch" >&2
    return 1
  fi
  git -C "${path}" rev-parse HEAD
}

source_status="PASS"
delay_source="unknown"
kafka_source="unknown"
pulsar_source="unknown"
oxia_source="unknown"
if ! delay_source="$(require_checkout Delay "${delay_dir}" "${expected_branch}")"; then source_status="BLOCKED"; fi
if ! kafka_source="$(require_checkout Kafka "${kafka_dir}" "nereus/delay-guarded-producer")"; then source_status="BLOCKED"; fi
if ! pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" "nereus/delay-resource-guard")"; then source_status="BLOCKED"; fi
if ! oxia_source="$(require_checkout Oxia "${oxia_dir}" main)"; then source_status="BLOCKED"; fi

bounded_status="BLOCKED"
bounded_exit=1
bounded_artifact="${bounded_dir}/operations-drills.json"
set +e
NEREUS_DELAY_OPERATIONS_DRILLS_ARTIFACT_DIR="${bounded_dir}" \
NEREUS_DELAY_OPERATIONS_DRILLS_GRADLE_USER_HOME="${artifact_dir}/child-gradle-user-home" \
NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
bash "${script_dir}/run-bounded-operations-drills.sh" \
  >"${artifact_dir}/bounded-operations.log" 2>&1
bounded_exit=$?
set -e
cat "${artifact_dir}/bounded-operations.log"
if [[ "${bounded_exit}" == "0" && -s "${bounded_artifact}" ]] \
    && jq -e --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
      --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
      '.status == "PASS_BOUNDED"
       and (.source_locks.delay == $delay)
       and (.source_locks.kafka == $kafka)
       and (.source_locks.pulsar == $pulsar)
       and (.source_locks.oxia == $oxia)
       and (.probes | length == 2)
       and (all(.probes[]; .status == "PASS"))
       and (.docker_cleanup.status == "PASS")' "${bounded_artifact}" >/dev/null 2>&1; then
  bounded_status="PASS"
fi

authority_log="${bounded_dir}/real-oxia-minio-checkpoint.log"
authority_status="FAIL"
fresh_process_status="FAIL"
if [[ "${bounded_status}" == "PASS" && -s "${authority_log}" ]] \
    && rg -Fq "Oxia external control/protocol/Route/recovery authority E2E passed" "${authority_log}"; then
  authority_status="PASS"
fi
if [[ "${bounded_status}" == "PASS" && -s "${authority_log}" ]] \
    && rg -Fq "Oxia fresh-process control/recovery authority E2E passed" "${authority_log}"; then
  fresh_process_status="PASS"
fi

soak_status="MISSING"
if [[ -n "${soak_artifact}" && -s "${soak_artifact}" ]] \
    && jq -e --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
      --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
      '.schema == "nereus-delay-certified-production-chain-soak"
       and .status == "PASS_CERTIFIED"
       and .execution == "strict-sequential"
       and (.source_locks.delay == $delay)
       and (.source_locks.kafka == $kafka)
       and (.source_locks.pulsar == $pulsar)
       and (.source_locks.oxia == $oxia)
       and (.observations.case_invariants == "PASS")
       and (.observations.docker_cleanup_status == "PASS")' "${soak_artifact}" >/dev/null 2>&1; then
  soak_status="PASS_CERTIFIED"
fi

operations_status="BLOCKED"
if [[ "${source_status}" == "PASS" && "${bounded_status}" == "PASS" \
    && "${authority_status}" == "PASS" && "${fresh_process_status}" == "PASS" \
    && "${soak_status}" == "PASS_CERTIFIED" ]]; then
  operations_status="PASS_CERTIFIED"
fi

jq -n \
  --arg schema "nereus-delay-certified-operations-drills" \
  --arg status "${operations_status}" \
  --arg profile_id "${profile_id}" \
  --arg execution "strict-sequential" \
  --arg artifact "${artifact_dir}" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  --arg bounded_status "${bounded_status}" --argjson bounded_exit "${bounded_exit}" \
  --arg bounded_artifact "${bounded_artifact}" \
  --arg authority_status "${authority_status}" \
  --arg fresh_process_status "${fresh_process_status}" \
  --arg soak_status "${soak_status}" --arg soak_artifact "${soak_artifact}" \
  --argjson source_status "$(if [[ "${source_status}" == "PASS" ]]; then echo true; else echo false; fi)" \
  '{
    schema: $schema,
    status: $status,
    profile_id: $profile_id,
    execution: $execution,
    artifact_dir: $artifact,
    started_at: $started_at,
    finished_at: $finished_at,
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    source_checks_pass: $source_status,
    bounded_operations: {
      status: $bounded_status,
      exit_code: $bounded_exit,
      artifact: $bounded_artifact,
      docker_cleanup: (if ($bounded_status == "PASS") then "PASS" else "UNKNOWN" end)
    },
    evidence: {
      operator_authorization: $authority_status,
      capability_before_activation_marker: $authority_status,
      fresh_process_recovery: $fresh_process_status
    },
    multi_worker_soak: {status: $soak_status, artifact: $soak_artifact},
    boundaries: [
      "PASS_CERTIFIED is emitted only after the bounded real Oxia/MinIO probe passes with exact current source locks, signed control/Route/capability authority evidence and separate WRITE/READ Gradle JVM recovery.",
      "The multi-Worker production-chain soak is an independently source-locked PASS_CERTIFIED input; this receipt never promotes a bounded soak or changes its status.",
      "Upgrade/downgrade packaging and disaster/host fault coverage remain separate release gates.",
      "Docker cleanup is inherited only from the exact bounded child run; no global prune is performed."
    ]
  }' >"${artifact_dir}/operations-drills.json"

jq -e --arg status "${operations_status}" \
  '.status == $status and (.source_locks | type == "object")' \
  "${artifact_dir}/operations-drills.json" >/dev/null
echo "Certified operations drills artifact: ${artifact_dir}/operations-drills.json"
echo "status=${operations_status}"

if [[ "${operations_status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
