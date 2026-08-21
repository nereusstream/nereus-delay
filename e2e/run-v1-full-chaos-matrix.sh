#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Full V1 chaos matrix.  The older certified wrapper is a useful source of
# fourteen real durable cells, but it is not itself the §23.3 matrix.  This
# runner keeps the full nineteen-cell contract, adds the real provider and
# failover children, and refuses to promote a cell unless its child exposes
# deterministic injection, durable before/after state, a fresh process and an
# independent invariant audit.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_FULL_CHAOS_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-v1-full-chaos.XXXXXX)}"
gradle_home="${NEREUS_DELAY_FULL_CHAOS_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
candidate_lock_file="${NEREUS_DELAY_FULL_CHAOS_CANDIDATE_SOURCE_LOCK:-${NEREUS_DELAY_RELEASE_GATE_CANDIDATE_SOURCE_LOCK:-}}"
profile_id="${NEREUS_DELAY_FULL_CHAOS_PROFILE_ID:-nereus-delay-v1-full-chaos-r1}"
run_external="${NEREUS_DELAY_FULL_CHAOS_RUN_EXTERNAL:-1}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

required_cells=(
  sigkill long-gc network-partition half-open enospc fsync-error sst-corruption
  broker-leader-failover oxia-session-expiry object-store-5xx object-store-timeout
  storage-provider-fault config-drift target-isolation disaster-host-fault
  kafka-response-loss lso-retention-floor pulsar-multibroker-failover
  credential-binding-drift
)

fail() {
  echo "full chaos matrix: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
[[ -n "${candidate_lock_file}" && -s "${candidate_lock_file}" ]] \
  || fail "NEREUS_DELAY_FULL_CHAOS_CANDIDATE_SOURCE_LOCK is required"
jq empty "${candidate_lock_file}" >/dev/null 2>&1 \
  || fail "candidate source lock is not valid JSON"
[[ "${run_external}" == "0" || "${run_external}" == "1" ]] \
  || fail "NEREUS_DELAY_FULL_CHAOS_RUN_EXTERNAL must be 0 or 1"
[[ "${artifact_dir}" != "/" && "${artifact_dir}" != "/private/tmp" ]] \
  || fail "artifact directory is too broad"
if [[ -e "${artifact_dir}" && -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
mkdir -p "${artifact_dir}" "${gradle_home}"

candidate_delay="$(jq -er '.delay' "${candidate_lock_file}")"
candidate_kafka="$(jq -er '.kafka' "${candidate_lock_file}")"
candidate_pulsar="$(jq -er '.pulsar' "${candidate_lock_file}")"
candidate_oxia="$(jq -er '.oxia' "${candidate_lock_file}")"
for lock in "${candidate_delay}" "${candidate_kafka}" "${candidate_pulsar}" "${candidate_oxia}"; do
  [[ "${lock}" =~ ^[0-9a-f]{40}$ ]] || fail "non-canonical candidate SHA: ${lock}"
done

require_checkout() {
  local path="$1" branch="$2" expected="$3"
  [[ -e "${path}/.git" ]] || return 1
  [[ -z "$(git -C "${path}" status --porcelain)" ]] || return 1
  [[ "$(git -C "${path}" branch --show-current)" == "${branch}" ]] || return 1
  [[ "$(git -C "${path}" rev-parse HEAD)" == "${expected}" ]] || return 1
}

source_status="PASS"
require_checkout "${delay_dir}" "nereus/delay-full-implementation-v1" "${candidate_delay}" \
  || source_status="BLOCKED"
require_checkout "${kafka_dir}" "nereus/delay-guarded-producer-v1" "${candidate_kafka}" \
  || source_status="BLOCKED"
require_checkout "${pulsar_dir}" "nereus/delay-resource-guard-v1" "${candidate_pulsar}" \
  || source_status="BLOCKED"
require_checkout "${oxia_dir}" "main" "${candidate_oxia}" || source_status="BLOCKED"

source_locks_json="$(jq -cn --arg delay "${candidate_delay}" --arg kafka "${candidate_kafka}" \
  --arg pulsar "${candidate_pulsar}" --arg oxia "${candidate_oxia}" \
  '{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}')"
required_json="$(printf '%s\n' "${required_cells[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"

bounded_dir="${artifact_dir}/certified-fourteen-cell"
bounded_artifact="${bounded_dir}/certified-chaos-matrix.json"
bounded_exit=1
if [[ "${source_status}" == "PASS" ]]; then
  set +e
  NEREUS_DELAY_CERTIFIED_CHAOS_ARTIFACT_DIR="${bounded_dir}" \
  NEREUS_DELAY_CERTIFIED_CHAOS_GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_CERTIFIED_CHAOS_PROFILE_ID="${profile_id}-bounded" \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-certified-chaos-matrix.sh" \
      >"${artifact_dir}/certified-fourteen-cell.log" 2>&1
  bounded_exit=$?
  set -e
else
  echo "source boundary blocked; fourteen-cell child was not started" \
    >"${artifact_dir}/certified-fourteen-cell.log"
fi

bounded_status="MISSING"
if [[ -s "${bounded_artifact}" ]] && jq empty "${bounded_artifact}" >/dev/null 2>&1; then
  bounded_status="$(jq -r '.status // "UNKNOWN"' "${bounded_artifact}")"
fi

cells_json='{}'
blocked_reasons=()

add_cell() {
  local cell_json="$1"
  cells_json="$(jq -c --argjson cell "${cell_json}" '. + $cell' <<<"${cells_json}")"
}

blocked_cell() {
  local name="$1" point="$2" reason="$3"
  local cell_json
  cell_json="$(jq -cn --arg name "${name}" --arg point "${point}" --arg reason "${reason}" \
    '{($name): {
      status:"BLOCKED",
      injection:{status:"BLOCKED",point:$point},
      before_after:{status:"BLOCKED",reason:$reason},
      fresh_process_recovery:"BLOCKED",
      invariant_audit:"BLOCKED",
      evidence:{reason:$reason}
    }}')"
  add_cell "${cell_json}"
  blocked_reasons+=("${name}: ${reason}")
}

bounded_cell() {
  local name="$1" point="$2" child_name="${3:-$1}"
  local audit
  if [[ ! -s "${bounded_artifact}" ]]; then
    blocked_cell "${name}" "${point}" "fourteen-cell child artifact is missing"
    return
  fi
  audit="$(jq -c --arg name "${child_name}" '.cells[$name].audit // empty' "${bounded_artifact}" 2>/dev/null || true)"
  if [[ -z "${audit}" || "${audit}" == "null" ]]; then
    blocked_cell "${name}" "${point}" "fourteen-cell child has no exact audit for ${child_name}"
    return
  fi
  local child_status audit_status durable fresh invariant
  child_status="$(jq -r --arg name "${child_name}" '.cells[$name].status // 1' "${bounded_artifact}")"
  audit_status="$(jq -r '.audit_status // "FAIL"' <<<"${audit}")"
  durable="$(jq -r '.durable_state_dump.status // "FAIL"' <<<"${audit}")"
  fresh="$(jq -r '.fresh_process_recovery // "FAIL"' <<<"${audit}")"
  invariant="$(jq -r '.invariant_audit.status // "FAIL"' <<<"${audit}")"
  local status="BLOCKED"
  [[ "${child_status}" == "0" && "${audit_status}" == "PASS" \
      && "${durable}" == "CAPTURED_AND_VERIFIED" \
      && "${fresh}" == "PASS" && "${invariant}" == "INDEPENDENT_FIELDS_PASS" ]] \
    && status="PASS"
  local before_status="FAIL" invariant_status="FAIL"
  [[ "${durable}" == "CAPTURED_AND_VERIFIED" ]] && before_status="PASS"
  [[ "${invariant}" == "INDEPENDENT_FIELDS_PASS" ]] && invariant_status="PASS"
  local cell_json
  cell_json="$(jq -cn --arg name "${name}" --arg point "${point}" \
      --arg status "${status}" --arg before "${before_status}" \
      --arg fresh "${fresh}" --arg invariant "${invariant_status}" \
      --argjson audit "${audit}" \
      '{($name): {
        status:$status,
        injection:{status:(if $status == "PASS" then "PASS" else "FAIL" end),point:$point},
        before_after:{status:$before,audit:$audit.durable_state_dump},
        fresh_process_recovery:$fresh,
        invariant_audit:$invariant,
        evidence:$audit.evidence
      }}')"
  add_cell "${cell_json}"
  [[ "${status}" == "PASS" ]] || blocked_reasons+=("${name}: child audit did not satisfy full-cell contract")
}

# The bounded child has exact durable/fresh/invariant receipts for these
# source cuts.  The full names below deliberately retain the broader design
# contract instead of pretending that one bounded cell closes every cut.
bounded_cell sigkill "SIGKILL Worker/Broker after a durable pre-ACK boundary" kafka-broker-process-crash
bounded_cell network-partition "remove one real Broker from the exact Compose network" kafka-broker-network-partition
bounded_cell kafka-response-loss "discard read_committed Fetch response and reopen the group" kafka-fetch-response-loss
bounded_cell lso-retention-floor "advance real retention and reopen stale/current source floor" kafka-retention-floor
bounded_cell pulsar-multibroker-failover "stop or SIGKILL one real Pulsar Broker and resume on survivor" pulsar-multi-broker-process-crash
bounded_cell oxia-session-expiry "stop the real Oxia authority while the old Gateway session is live" gateway-oxia-session-churn

if [[ "${run_external}" == "1" && "${source_status}" == "PASS" ]]; then
  minio_dir="${artifact_dir}/minio-fault"
  set +e
  NEREUS_DELAY_MINIO_FAULT_ARTIFACT_DIR="${minio_dir}" \
  NEREUS_DELAY_E2E_GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-minio-fault-e2e.sh" >"${artifact_dir}/minio-fault.log" 2>&1
  minio_exit=$?
  set -e
else
  minio_exit=1
  echo "real MinIO child not run" >"${artifact_dir}/minio-fault.log"
fi
minio_artifact="${artifact_dir}/minio-fault/minio-fault-e2e.json"
if [[ "${minio_exit}" == "0" && -s "${minio_artifact}" ]] \
    && jq -e '.status == "PASS" and .test_exit_code == 0 and .docker_cleanup.status == "PASS"' \
      "${minio_artifact}" >/dev/null 2>&1; then
  minio_reason="real MinIO child passed, but it does not yet expose independent durable before/after dumps"
else
  minio_reason="real MinIO 5xx/timeout/configuration-drift child did not pass"
fi
blocked_cell object-store-5xx "inject PUT_503_AFTER_COMMIT and PUT_503_BEFORE_COMMIT at the real MinIO proxy" "${minio_reason}"
blocked_cell object-store-timeout "inject PUT_TIMEOUT_AFTER_COMMIT at the real MinIO proxy" "${minio_reason}"
blocked_cell storage-provider-fault "inject provider failure before immutable checkpoint Commit" "${minio_reason}"
blocked_cell config-drift "replace the provider credential configuration with a drifted binding" "${minio_reason}"
blocked_cell credential-binding-drift "rotate the credential head across a protected use lease" "credential lease rotation still needs an independent durable before/after fault receipt"

# These cuts are intentionally not synthesized from a marker-only unit test.
# They remain explicit blockers until their real injection and recovery child
# is added with a durable dump that a fresh process can reopen.
blocked_cell long-gc "pause the Worker at the long-GC admission boundary" "no deterministic long-GC fresh-process dump child"
blocked_cell half-open "hold a half-open native connection past the channel deadline" "no real half-open transport dump child"
blocked_cell enospc "fill the exact Store/checkpoint filesystem to the ENOSPC boundary" "no safe exact ENOSPC fixture with durable before/after dump"
blocked_cell fsync-error "fail directory/WAL fsync after the accepted WriteBatch boundary" "no current-source fsync fault child artifact"
blocked_cell sst-corruption "corrupt one copied SST after checkpoint publication" "no current-source SST corruption fresh-process child"
blocked_cell broker-leader-failover "move the source leader to a surviving Broker" "leader-placement child is not yet wired into this full matrix"
blocked_cell target-isolation "starve target A while target B remains schedulable" "no full target-isolation durable fairness dump child"
blocked_cell disaster-host-fault "terminate the host-side Worker process and restore from the exact floor" "no disaster-host fresh-process child artifact"

all_pass="PASS"
for name in "${required_cells[@]}"; do
  status="$(jq -r --arg name "${name}" '.[$name].status // "BLOCKED"' <<<"${cells_json}")"
  [[ "${status}" == "PASS" ]] || all_pass="BLOCKED"
done
[[ "${source_status}" == "PASS" && "${bounded_exit}" == "0" \
    && "${bounded_status}" == "PASS_CERTIFIED" ]] || all_pass="BLOCKED"

if [[ "${all_pass}" == "PASS" ]]; then
  observed_json="${required_json}"
  evidence_exit=0
  coverage_status="PASS"
  independent_audit="PASS"
  boundaries_json='[]'
else
  observed_json="$(jq -c 'to_entries | map(select(.value.status == "PASS")) | map(.key)' <<<"${cells_json}")"
  evidence_exit=1
  coverage_status="BLOCKED"
  independent_audit="BLOCKED"
  boundaries_json="$(printf '%s\n' "${blocked_reasons[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
fi

jq -n \
  --arg schema "nereus-delay-v1-full-gate-input-v1" \
  --arg status "${all_pass}" --arg profile_id "${profile_id}" \
  --arg artifact "${artifact_dir}" --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --argjson locks "${source_locks_json}" --argjson required "${required_json}" \
  --argjson observed "${observed_json}" --argjson cells "${cells_json}" \
  --argjson boundaries "${boundaries_json}" --argjson test_exit_code "${evidence_exit}" \
  --arg coverage_status "${coverage_status}" --arg independent_audit "${independent_audit}" \
  --arg bounded_artifact "${bounded_artifact}" --arg minio_artifact "${minio_artifact}" \
  --argjson bounded_exit "${bounded_exit}" --arg bounded_status "${bounded_status}" \
  '{
    schema:$schema,status:(if $status == "PASS" then "PASS_CERTIFIED" else "BLOCKED" end),
    profile_id:$profile_id,scope:"full-v1",complete_v1:($status == "PASS"),
    gate:"chaos",execution:"strict-sequential",artifact_dir:$artifact,
    started_at:$started_at,finished_at:$finished_at,source_locks:$locks,
    coverage:{complete_v1:($status == "PASS"),required:$required,observed:$observed,exclusions:[]},
    evidence:{test_exit_code:$test_exit_code,source_lock_status:(if $locks then "PASS" else "BLOCKED" end),
      coverage_status:$coverage_status,independent_audit:$independent_audit,
      bounded_child:{status:$bounded_status,exit_code:$bounded_exit,artifact:$bounded_artifact},
      minio_child:$minio_artifact},
    cells:$cells,boundaries:$boundaries
  }' >"${artifact_dir}/full-chaos-matrix.json"

echo "V1 full chaos matrix artifact: ${artifact_dir}/full-chaos-matrix.json"
echo "status=$(jq -r '.status' "${artifact_dir}/full-chaos-matrix.json") bounded=${bounded_status} minio_exit=${minio_exit}"
jq -r '.boundaries[]? // empty' "${artifact_dir}/full-chaos-matrix.json" | head -n 40
[[ "${all_pass}" == "PASS" ]]
