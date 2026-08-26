#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_CERTIFIED_ACTIVATION_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-certified-activation.XXXXXX)}"
gradle_home="${NEREUS_DELAY_CERTIFIED_ACTIVATION_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
expected_branch="nereus/delay-full-implementation"
profile_id="${NEREUS_DELAY_CERTIFIED_ACTIVATION_PROFILE_ID:-nereus-delay-rc1-activation-r1}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
local_dir="${artifact_dir}/local-activation"
local_artifact="${local_dir}/protocol-activation-cutover.json"
real_log="${artifact_dir}/real-oxia-protocol-authority.log"

if ! command -v jq >/dev/null 2>&1 || ! command -v docker >/dev/null 2>&1; then
  echo "certified activation requires jq and docker" >&2
  exit 1
fi
if [[ -e "${artifact_dir}" && -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "certified activation artifact directory must be empty: ${artifact_dir}" >&2
  exit 1
fi
mkdir -p "${artifact_dir}" "${gradle_home}"

require_checkout() {
  local label="$1"
  local path="$2"
  local branch="$3"
  if [[ ! -e "${path}/.git" ]] || [[ -n "$(git -C "${path}" status --porcelain)" ]] \
      || [[ "$(git -C "${path}" branch --show-current)" != "${branch}" ]]; then
    echo "${label} checkout is not a clean expected worktree: ${path}" >&2
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

local_status="BLOCKED"
local_exit=1
set +e
NEREUS_DELAY_PROTOCOL_ACTIVATION_ARTIFACT_DIR="${local_dir}" \
NEREUS_DELAY_PROTOCOL_ACTIVATION_GRADLE_USER_HOME="${gradle_home}" \
bash "${script_dir}/run-protocol-activation-cutover-smoke.sh" >"${artifact_dir}/local-activation.log" 2>&1
local_exit=$?
set -e
cat "${artifact_dir}/local-activation.log"
if [[ "${local_exit}" == "0" && -s "${local_artifact}" ]] \
    && jq -e --arg delay "${delay_source}" \
      '.status == "PASS_BOUNDED" and .source_lock == $delay and (.tests | length == 6)
       and any(.tests[]; .name == "com.nereusstream.delay.protocol.ProtocolActivationCutoverContractTest")
       and any(.tests[]; .name == "com.nereusstream.delay.store.CheckpointRestoreCoordinatorTest")' \
      "${local_artifact}" >/dev/null 2>&1; then
  local_status="PASS"
fi

set +e
NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
NEREUS_DELAY_OXIA_PROTOCOL_AUTHORITY_GRADLE_USER_HOME="${gradle_home}" \
NEREUS_DELAY_OXIA_PROTOCOL_AUTHORITY_LOG="${real_log}" \
NEREUS_DELAY_OXIA_PROTOCOL_AUTHORITY_PORT="31510" \
bash "${script_dir}/run-oxia-protocol-authority-e2e.sh"
real_exit=$?
set -e
real_status="FAIL"
if [[ "${real_exit}" == "0" && -s "${real_log}" ]] \
    && rg -Fq "Oxia protocol activation authority E2E passed" "${real_log}"; then
  real_status="PASS"
fi

compose_project="$(sed -n 's/^Compose project: //p' "${real_log}" | tail -1 || true)"
cleanup_status="FAIL"
if [[ -n "${compose_project}" ]]; then
  leftover_containers="$(docker ps -aq --filter "label=com.docker.compose.project=${compose_project}" || true)"
  leftover_networks="$(docker network ls -q --filter "label=com.docker.compose.project=${compose_project}" || true)"
  leftover_volumes="$(docker volume ls -q --filter "label=com.docker.compose.project=${compose_project}" || true)"
  generated_image="${compose_project}-oxia"
  if [[ -z "${leftover_containers}" && -z "${leftover_networks}" && -z "${leftover_volumes}" ]] \
      && ! docker image inspect "${generated_image}" >/dev/null 2>&1; then
    cleanup_status="PASS"
  fi
fi

activation_status="BLOCKED"
if [[ "${source_status}" == "PASS" && "${local_status}" == "PASS" \
    && "${real_status}" == "PASS" && "${cleanup_status}" == "PASS" ]]; then
  activation_status="PASS_CERTIFIED"
fi

jq -n \
  --arg schema "nereus-delay-certified-protocol-activation-cutover" \
  --arg status "${activation_status}" \
  --arg profile_id "${profile_id}" \
  --arg execution "strict-sequential" \
  --arg artifact "${artifact_dir}" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  --arg local_status "${local_status}" --argjson local_exit "${local_exit}" \
  --arg local_artifact "${local_artifact}" \
  --arg real_status "${real_status}" --argjson real_exit "${real_exit}" \
  --arg real_log "${real_log}" --arg compose_project "${compose_project}" \
  --arg cleanup_status "${cleanup_status}" \
  --argjson source_checks_pass "$(if [[ "${source_status}" == "PASS" ]]; then echo true; else echo false; fi)" \
  '{
    schema: $schema,
    status: $status,
    profile_id: $profile_id,
    execution: $execution,
    artifact_dir: $artifact,
    started_at: $started_at,
    finished_at: $finished_at,
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    source_checks_pass: $source_checks_pass,
    local_projection: {status: $local_status, exit_code: $local_exit, artifact: $local_artifact},
    external_authority: {
      status: $real_status,
      exit_code: $real_exit,
      log: $real_log,
      compose_project: $compose_project,
      cleanup_status: $cleanup_status,
      capability_before_marker: (if ($real_status == "PASS") then "PASS" else "FAIL" end),
      fresh_session_recovery: (if ($real_status == "PASS") then "PASS" else "FAIL" end)
    },
    assertions: [
      "Local key-14 activation projection round-trips canonical marker evidence and survives Store restart.",
      "Real Oxia rereads all eligible Worker capability declarations before the activation marker is materialized.",
      "The activation evidence hash binds sorted Worker identity, declaration revision/digest and Oxia session identity.",
      "Missing or withdrawn eligible capability fails closed before activation authorization.",
      "Writer-before-reader rollout is rejected until every eligible reader supports the exact tuple.",
      "Downgrade packaging is canonical, binary-digest bound and cannot delete an activated marker.",
      "Same payload bytes under different protocol versions remain distinct command identities.",
      "Checkpoint restore and recovery fencing are exercised with a fresh Store incarnation."
    ],
    boundaries: []
  }' >"${artifact_dir}/protocol-activation-cutover.json"

jq -e --arg status "${activation_status}" \
  '.status == $status and (.source_locks | type == "object")' \
  "${artifact_dir}/protocol-activation-cutover.json" >/dev/null
echo "Certified protocol activation/cutover artifact: ${artifact_dir}/protocol-activation-cutover.json"
echo "status=${activation_status}"

if [[ "${activation_status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
