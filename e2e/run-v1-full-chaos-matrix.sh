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
reuse_certified_dir="${NEREUS_DELAY_FULL_CHAOS_REUSE_CERTIFIED_DIR:-}"
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
if [[ -n "${reuse_certified_dir}" ]]; then
  bounded_dir="${reuse_certified_dir}"
  bounded_artifact="${bounded_dir}/certified-chaos-matrix.json"
  bounded_exit=0
elif [[ "${source_status}" == "PASS" ]]; then
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
bounded_locks_match="FAIL"
if [[ -s "${bounded_artifact}" ]] && jq -e --argjson locks "${source_locks_json}" \
    '.source_locks == $locks' "${bounded_artifact}" >/dev/null 2>&1; then
  bounded_locks_match="PASS"
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
  local before_status="BLOCKED" invariant_status="BLOCKED"
  [[ "${durable}" == "CAPTURED_AND_VERIFIED" ]] && before_status="PASS"
  [[ "${invariant}" == "INDEPENDENT_FIELDS_PASS" ]] && invariant_status="INDEPENDENT_FIELDS_PASS"
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

leader_dir="${artifact_dir}/broker-leader-failover"
leader_state_dir="${leader_dir}/state"
leader_exit=1
leader_cell_status="BLOCKED"
if [[ "${run_external}" == "1" && "${source_status}" == "PASS" ]]; then
  mkdir -p "${leader_dir}" "${leader_state_dir}"
  set +e
  NEREUS_DELAY_KAFKA_LEADER_PLACEMENT_ONLY=1 \
  NEREUS_DELAY_KAFKA_LEADER_PLACEMENT_STATE_DUMP_DIR="${leader_state_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_E2E_ARTIFACT_DIR="${leader_dir}" \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_KAFKA_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-kafka-real-client-e2e.sh" >"${leader_dir}/run.log" 2>&1
  leader_exit=$?
  set -e
fi
leader_before="${leader_state_dir}/before-process-crash.json"
leader_after="${leader_state_dir}/after-fresh-process.json"
if [[ "${leader_exit}" == "0" && -s "${leader_before}" && -s "${leader_after}" ]] \
    && jq -n --slurpfile before "${leader_before}" --slurpfile after "${leader_after}" \
      '($before | length) == 1 and ($after | length) == 1
       and $before[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
       and $after[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
       and $before[0].cell == "kafka-broker-leader-failover"
       and $after[0].cell == "kafka-broker-leader-failover"
       and $before[0].phase == "BROKER_LEADER_FAILOVER_READY"
       and $after[0].phase == "RECOVERED_AFTER_BROKER_LEADER_FAILOVER"
       and $before[0].dump_forced == true and $after[0].dump_forced == true
       and $before[0].durable_broker_read == true and $after[0].durable_broker_read == true
       and ($before[0].process_pid != $after[0].process_pid)
       and $before[0].cluster_id == $after[0].cluster_id
       and $before[0].topic_id == $after[0].topic_id
       and $before[0].leader_id == 1 and $after[0].leader_id == 2
       and ($before[0].live_broker_ids | sort) == [1,2,3]
       and ($after[0].live_broker_ids | sort) == [1,2,3]
       and ($before[0].isr_ids | sort) == [1,2,3]
       and ($after[0].isr_ids | sort) == [1,2,3]
       and $after[0].leader_moved_without_broker_loss == true
       and $after[0].end_offset >= $before[0].end_offset' >/dev/null 2>&1; then
  leader_cell_status="PASS"
fi
if [[ "${leader_cell_status}" == "PASS" ]]; then
  add_cell "$(jq -cn --arg before "${leader_before}" --arg after "${leader_after}" --arg log "${leader_dir}/run.log" \
    '{"broker-leader-failover": {
      status:"PASS",
      injection:{status:"PASS",point:"reassign replicated source and consumer-coordinator partitions to Broker-2 while Broker-1 remains live"},
      before_after:{status:"PASS",audit:{status:"CAPTURED_AND_VERIFIED",before_dump:$before,after_dump:$after}},
      fresh_process_recovery:"PASS",
      invariant_audit:"INDEPENDENT_FIELDS_PASS",
      evidence:{before_dump:$before,after_dump:$after,run_log:$log}
    }}')"
else
  blocked_cell broker-leader-failover "move the source leader to a surviving Broker while the old Broker remains live" \
    "real Kafka leader-placement child failed or its independent before/after audit did not pass (exit=${leader_exit})"
fi

half_open_dir="${artifact_dir}/half-open"
half_open_state_dir="${half_open_dir}/state"
mkdir -p "${half_open_dir}" "${half_open_state_dir}"
half_open_exit=1
if [[ "${run_external}" == "1" && "${source_status}" == "PASS" ]]; then
  set +e
  NEREUS_DELAY_KAFKA_HALF_OPEN_ONLY=1 \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_HALF_OPEN_STATE_DUMP_DIR="${half_open_state_dir}" \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_KAFKA_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-kafka-real-client-e2e.sh" \
      >"${half_open_dir}/run.log" 2>&1
  half_open_exit=$?
  set -e
else
  echo "source boundary blocked or external execution disabled; half-open child was not started" \
    >"${half_open_dir}/run.log"
fi

half_open_status="BLOCKED"
half_open_before="${half_open_state_dir}/before-process-crash.json"
half_open_after="${half_open_state_dir}/after-fresh-process.json"
half_open_transport="${half_open_state_dir}/half-open-e2e.json"
half_open_worker_after="${half_open_state_dir}/worker-state/after-fresh-process.json"
if [[ "${half_open_exit}" == "0" && -s "${half_open_transport}" \
    && -s "${half_open_before}" && -s "${half_open_after}" \
    && -s "${half_open_state_dir}/hold-ack" \
    && -s "${half_open_state_dir}/release-observed" \
    && -s "${half_open_state_dir}/post-release-forward" \
    && -s "${half_open_worker_after}" ]] \
    && jq -e --arg delay "${candidate_delay}" \
      '.schema == "nereus-delay-half-open-transport-e2e-v1"
       and .status == "PASS"
       and .cell == "kafka-half-open"
       and .real_socket_hold == true
       and .release_observed == true
       and .post_release_forward == true
       and .fresh_worker_recovery == true
       and .hold_crossed_channel_deadline == true
       and (.hold_duration_ms | type == "number")
       and (.hold_duration_ms >= .channel_deadline_ms)' \
      "${half_open_transport}" >/dev/null 2>&1 \
    && rg -F --quiet "hold_active=true" "${half_open_state_dir}/hold-ack" \
    && rg -F --quiet "release_observed=true" "${half_open_state_dir}/release-observed" \
    && rg -F --quiet "forwarded_after_release=true" "${half_open_state_dir}/post-release-forward" \
    && jq -n --slurpfile before "${half_open_before}" --slurpfile after "${half_open_after}" \
      --slurpfile worker "${half_open_worker_after}" \
      '($before | length) == 1 and ($after | length) == 1 and ($worker | length) == 1
       and $before[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
       and $after[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
       and $worker[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
       and $before[0].cell == "kafka-half-open" and $after[0].cell == "kafka-half-open"
       and $worker[0].cell == "kafka-worker-process-crash"
       and $before[0].phase == "HALF_OPEN_READY"
       and $after[0].phase == "RECOVERED_AFTER_HALF_OPEN"
       and $worker[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
       and $before[0].dump_forced == true and $after[0].dump_forced == true
       and $worker[0].dump_forced == true
       and $before[0].durable_broker_read == true and $after[0].durable_broker_read == true
       and $worker[0].durable_store_read == true
       and ($before[0].process_pid | type == "number")
       and ($after[0].process_pid | type == "number")
       and ($worker[0].process_pid | type == "number")
       and $before[0].process_pid != $after[0].process_pid
       and $before[0].topic == $after[0].topic
       and $before[0].topic_id == $after[0].topic_id
       and $before[0].cluster_id == $after[0].cluster_id
       and ($before[0].replica_ids | sort) == [1,2,3]
       and ($after[0].replica_ids | sort) == [1,2,3]
       and ($before[0].isr_ids | sort) == [1,2,3]
       and ($after[0].isr_ids | sort) == [1,2,3]
       and ($before[0].live_broker_ids | sort) == [1,2,3]
       and ($after[0].live_broker_ids | sort) == [1,2,3]
       and $before[0].leader_id == 1 and $after[0].leader_id == 1
       and $after[0].end_offset >= $before[0].end_offset
       and $worker[0].topic == $before[0].topic
       and $worker[0].topic_id == $before[0].topic_id
       and $worker[0].cluster_id == $before[0].cluster_id
       and $worker[0].applied_offset == 1
       and $worker[0].store_write_batch_durable == true
       and $worker[0].source_ack_committed == true' \
      >/dev/null 2>&1; then
  half_open_status="PASS"
fi
if [[ "${half_open_status}" == "PASS" ]]; then
  add_cell "$(jq -cn --arg transport "${half_open_transport}" \
    --arg before "${half_open_before}" --arg after "${half_open_after}" \
    --arg worker "${half_open_worker_after}" --arg log "${half_open_dir}/run.log" \
    '{"half-open": {
      status:"PASS",
      injection:{status:"PASS",point:"hold a real Broker response on an open TCP channel beyond the configured channel deadline"},
      before_after:{status:"PASS",audit:{status:"CAPTURED_AND_VERIFIED",before_dump:$before,after_dump:$after,transport:$transport}},
      fresh_process_recovery:"PASS",
      invariant_audit:"INDEPENDENT_FIELDS_PASS",
      evidence:{transport:$transport,before_dump:$before,after_dump:$after,worker_after_dump:$worker,run_log:$log}
    }}')"
else
  blocked_cell half-open "hold a half-open native connection past the channel deadline" \
    "real Kafka half-open transport/Worker child failed or its independent audit did not pass (exit=${half_open_exit})"
fi

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
minio_state_dir=""
minio_child_ready="FAIL"
if [[ "${minio_exit}" == "0" && -s "${minio_artifact}" ]] \
    && jq -e --arg source "${candidate_delay}" \
      '.status == "PASS" and .test_exit_code == 0
       and .first_test_exit_code == 0 and .recovery_test_exit_code == 0
       and .docker_cleanup.status == "PASS"
       and .fresh_process_recovery.required == true
       and .source_lock == $source' "${minio_artifact}" >/dev/null 2>&1; then
  minio_state_dir="$(jq -r '.minio.state_dump_dir // empty' "${minio_artifact}")"
  [[ -n "${minio_state_dir}" ]] && minio_child_ready="PASS"
fi

minio_cell() {
  local name="$1" point="$2" state_cell="$3" expected_fault="$4"
  local expected_intent="$5" expected_resource_before="$6" expected_action="$7"
  if [[ "${minio_child_ready}" != "PASS" || ! -d "${minio_state_dir}/${state_cell}" ]]; then
    blocked_cell "${name}" "${point}" "real MinIO child or its state-dump directory did not pass"
    return
  fi
  local before_dump="${minio_state_dir}/${state_cell}/before.json"
  local after_dump="${minio_state_dir}/${state_cell}/after.json"
  local pair_status="FAIL"
  if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
      && jq -n --slurpfile before "${before_dump}" --slurpfile after "${after_dump}" \
        --arg cell "${state_cell}" --arg fault "${expected_fault}" \
        --arg intent "${expected_intent}" --arg action "${expected_action}" \
        --argjson resource_before "${expected_resource_before}" \
        '($before | length) == 1 and ($after | length) == 1
         and $before[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
         and $after[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
         and $before[0].cell == $cell and $after[0].cell == $cell
         and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
         and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
         and $before[0].fault == $fault and $after[0].fault == "NONE"
         and $before[0].dump_forced == true and $after[0].dump_forced == true
         and $before[0].durable_store_read == true and $after[0].durable_store_read == true
         and ($before[0].process_pid != $after[0].process_pid)
         and ($before[0].manifest_sha256 == $after[0].manifest_sha256)
         and $before[0].intent_state == $intent
         and $before[0].resource_present == $resource_before
         and $after[0].resource_present == false
         and $after[0].recovery_action == $action' >/dev/null 2>&1; then
    pair_status="PASS"
  fi
  if [[ "${pair_status}" != "PASS" ]]; then
    blocked_cell "${name}" "${point}" "real MinIO before/after dumps failed independent fresh-process validation"
    return
  fi
  local cell_json
  cell_json="$(jq -cn --arg name "${name}" --arg point "${point}" \
      --arg child "${minio_artifact}" --arg before "${before_dump}" --arg after "${after_dump}" \
      '{($name): {
        status:"PASS",
        injection:{status:"PASS",point:$point},
        before_after:{status:"PASS",audit:{status:"CAPTURED_AND_VERIFIED",before_dump:$before,after_dump:$after}},
        fresh_process_recovery:"PASS",
        invariant_audit:"INDEPENDENT_FIELDS_PASS",
        evidence:{child_artifact:$child,before_dump:$before,after_dump:$after}
      }}')"
  add_cell "${cell_json}"
}

minio_cell object-store-5xx "inject PUT_503_AFTER_COMMIT at the real MinIO proxy" \
  object-store-5xx PUT_503_AFTER_COMMIT PUBLISHED true DOWNLOAD_EXACT_READBACK_DELETE_EXACT_VERSION
minio_cell object-store-timeout "inject PUT_TIMEOUT_AFTER_COMMIT at the real MinIO proxy" \
  object-store-timeout PUT_TIMEOUT_AFTER_COMMIT PUBLISHED true DOWNLOAD_EXACT_READBACK_DELETE_EXACT_VERSION
minio_cell storage-provider-fault "inject provider failure before immutable checkpoint Commit" \
  storage-provider-fault PUT_503_BEFORE_COMMIT PENDING_UPLOAD false EXACT_PREFIX_SWEEP_AFTER_PRECOMMIT_FAILURE
minio_cell config-drift "replace the provider credential configuration with a drifted binding" \
  config-drift CREDENTIAL_CONFIGURATION_DRIFT PENDING_UPLOAD false EXACT_PREFIX_SWEEP_AFTER_PRECOMMIT_FAILURE

credential_dir="${artifact_dir}/credential-binding-drift"
credential_exit=1
if [[ "${run_external}" == "1" && "${source_status}" == "PASS" ]]; then
  set +e
  NEREUS_DELAY_CREDENTIAL_CHAOS_ARTIFACT_DIR="${credential_dir}" \
  NEREUS_DELAY_CREDENTIAL_CHAOS_GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-credential-binding-drift-e2e.sh" \
      >"${artifact_dir}/credential-binding-drift-child.log" 2>&1
  credential_exit=$?
  set -e
fi
credential_artifact="${credential_dir}/credential-binding-drift-e2e.json"
credential_before="${credential_dir}/before.json"
credential_after="${credential_dir}/after.json"
credential_status="BLOCKED"
if [[ "${credential_exit}" == "0" && -s "${credential_artifact}" \
    && -s "${credential_before}" && -s "${credential_after}" ]] \
    && jq -e --arg delay "${candidate_delay}" --arg oxia "${candidate_oxia}" \
      --slurpfile before "${credential_before}" --slurpfile after "${credential_after}" \
      '.status == "PASS" and .cell == "credential-binding-drift"
       and .source_locks.delay == $delay and .source_locks.oxia == $oxia
       and .fresh_process_recovery == true
       and ($before | length) == 1 and ($after | length) == 1
       and $before[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
       and $after[0].schema == "nereus-delay-chaos-durable-state-dump-v1"
       and $before[0].cell == "credential-binding-drift"
       and $after[0].cell == "credential-binding-drift"
       and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
       and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
       and $before[0].fault == "CREDENTIAL_BINDING_ROTATION"
       and $after[0].fault == "CREDENTIAL_BINDING_ROTATION"
       and $before[0].dump_forced == true and $after[0].dump_forced == true
       and $before[0].durable_oxia_read == true and $after[0].durable_oxia_read == true
       and ($before[0].process_pid != $after[0].process_pid)
       and $before[0].head_generation == 2 and $after[0].head_generation == 2
       and $before[0].head_revision == 2 and $after[0].head_revision == 2
       and $before[0].old_lease_generation == 1 and $after[0].old_lease_generation == 1
       and $after[0].stale_fingerprint_rejected == true
       and $after[0].fresh_lease_generation == 2
       and $after[0].fresh_protection_until >= $after[0].old_lease_valid_until' \
      "${credential_artifact}" >/dev/null 2>&1; then
  credential_status="PASS"
fi
if [[ "${credential_status}" == "PASS" ]]; then
  add_cell "$(jq -cn --arg child "${credential_artifact}" --arg before "${credential_before}" \
    --arg after "${credential_after}" --arg log "${artifact_dir}/credential-binding-drift-child.log" \
    '{"credential-binding-drift": {
      status:"PASS",
      injection:{status:"PASS",point:"rotate the real Oxia credential Head across a protected generation-1 Object Store lease"},
      before_after:{status:"PASS",audit:{status:"CAPTURED_AND_VERIFIED",before_dump:$before,after_dump:$after}},
      fresh_process_recovery:"PASS",
      invariant_audit:"INDEPENDENT_FIELDS_PASS",
      evidence:{child_artifact:$child,before_dump:$before,after_dump:$after,run_log:$log}
    }}')"
else
  blocked_cell credential-binding-drift "rotate the credential head across a protected use lease" \
    "real Oxia credential rotation child failed or its independent before/after audit did not pass (exit=${credential_exit})"
fi

target_isolation_dir="${artifact_dir}/target-isolation"
target_isolation_before_exit=1
target_isolation_after_exit=1
if [[ "${source_status}" == "PASS" ]]; then
  mkdir -p "${target_isolation_dir}"
  set +e
  NEREUS_DELAY_TARGET_ISOLATION_ARTIFACT_DIR="${target_isolation_dir}" \
  NEREUS_DELAY_TARGET_ISOLATION_PHASE=before \
  GRADLE_USER_HOME="${gradle_home}" \
    ./gradlew test --no-daemon --console=plain --rerun-tasks \
      --tests com.nereusstream.delay.scheduler.TargetIsolationDurableChaosTest \
      >"${target_isolation_dir}/before-process.log" 2>&1
  target_isolation_before_exit=$?
  set -e
  if [[ "${target_isolation_before_exit}" == "0" ]]; then
    set +e
    NEREUS_DELAY_TARGET_ISOLATION_ARTIFACT_DIR="${target_isolation_dir}" \
    NEREUS_DELAY_TARGET_ISOLATION_PHASE=after \
    GRADLE_USER_HOME="${gradle_home}" \
      ./gradlew test --no-daemon --console=plain --rerun-tasks \
        --tests com.nereusstream.delay.scheduler.TargetIsolationDurableChaosTest \
        >"${target_isolation_dir}/after-process.log" 2>&1
    target_isolation_after_exit=$?
    set -e
  fi
fi

target_isolation_cell_status="FAIL"
target_isolation_manifest_sha256=""
if [[ -s "${target_isolation_dir}/manifest.json" ]]; then
  target_isolation_manifest_sha256="$(shasum -a 256 "${target_isolation_dir}/manifest.json" | awk '{print $1}')"
fi
if [[ "${target_isolation_before_exit}" == "0" && "${target_isolation_after_exit}" == "0" \
    && -s "${target_isolation_dir}/before.json" && -s "${target_isolation_dir}/after.json" \
    && -n "${target_isolation_manifest_sha256}" ]] \
    && jq -n --slurpfile before "${target_isolation_dir}/before.json" \
      --slurpfile after "${target_isolation_dir}/after.json" \
      --arg manifest "${target_isolation_manifest_sha256}" \
      '($before | length) == 1 and ($after | length) == 1
       and $before[0].schema == "nereus-delay-target-isolation-durable-state-dump-v1"
       and $after[0].schema == "nereus-delay-target-isolation-durable-state-dump-v1"
       and $before[0].cell == "target-isolation" and $after[0].cell == "target-isolation"
       and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
       and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
       and $before[0].fault == "TARGET_ISOLATION" and $after[0].fault == "TARGET_ISOLATION"
       and $before[0].dump_forced == true and $after[0].dump_forced == true
       and $before[0].durable_store_read == true and $after[0].durable_store_read == true
       and ($before[0].process_pid | type == "number")
       and ($after[0].process_pid | type == "number")
       and ($before[0].process_pid != $after[0].process_pid)
       and $before[0].manifest_sha256 == $manifest
       and $after[0].manifest_sha256 == $manifest
       and $before[0].bad_target_state == "BLOCKED"
       and $after[0].bad_target_state == "BLOCKED"
       and $before[0].healthy_target_state == "READY"
       and $after[0].healthy_target_state == "READY"
       and ($before[0].bad_pending | type == "number" and . >= 1)
       and $after[0].bad_pending == $before[0].bad_pending
       and $after[0].healthy_progress > $before[0].healthy_progress
       and $after[0].healthy_pending <= $before[0].healthy_pending
       and $after[0].recovery_action == "FRESH_PROCESS_REOPENED_AND_HEALTHY_PROGRESS_CONTINUED"' \
      >/dev/null 2>&1; then
  target_isolation_cell_status="PASS"
fi
if [[ "${target_isolation_cell_status}" == "PASS" ]]; then
  target_isolation_cell_json="$(jq -cn \
    --arg before "${target_isolation_dir}/before.json" \
    --arg after "${target_isolation_dir}/after.json" \
    --arg manifest "${target_isolation_dir}/manifest.json" \
    --arg before_log "${target_isolation_dir}/before-process.log" \
    --arg after_log "${target_isolation_dir}/after-process.log" \
    '{"target-isolation": {
      status:"PASS",
      injection:{status:"PASS",point:"block target A at the Worker outer-ring admission boundary"},
      before_after:{status:"PASS",audit:{status:"CAPTURED_AND_VERIFIED",before_dump:$before,after_dump:$after,manifest:$manifest}},
      fresh_process_recovery:"PASS",
      invariant_audit:"INDEPENDENT_FIELDS_PASS",
      evidence:{before_dump:$before,after_dump:$after,manifest:$manifest,before_log:$before_log,after_log:$after_log}
    }}')"
  add_cell "${target_isolation_cell_json}"
else
  blocked_cell target-isolation "block target A while target B remains schedulable" \
    "target-isolation durable before/after child failed (before=${target_isolation_before_exit}, after=${target_isolation_after_exit})"
fi

storage_dir="${artifact_dir}/local-storage-chaos"
storage_artifact="${storage_dir}/local-storage-chaos-e2e.json"
storage_exit=1
if [[ "${source_status}" == "PASS" ]]; then
  set +e
  NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR="${storage_dir}" \
  NEREUS_DELAY_STORAGE_CHAOS_GRADLE_USER_HOME="${gradle_home}" \
    bash "${script_dir}/run-local-storage-chaos-e2e.sh" \
      >"${artifact_dir}/local-storage-chaos-child.log" 2>&1
  storage_exit=$?
  set -e
else
  echo "source boundary blocked; local storage child was not started" \
    >"${artifact_dir}/local-storage-chaos-child.log"
fi
storage_child_ready="FAIL"
if [[ "${storage_exit}" == "0" && -s "${storage_artifact}" ]] \
    && jq -e --arg delay "${candidate_delay}" \
      '.schema == "nereus-delay-local-storage-chaos-e2e-v1"
       and .status == "PASS"
       and .source_locks.delay == $delay
       and .fresh_process_recovery == true
       and .docker_cleanup.status == "PASS"
       and ([.cells[].name] | sort) == ["disaster-host-fault", "enospc", "fsync-error", "sst-corruption"]
       and ([.cells[].status] | all(. == "PASS"))' \
      "${storage_artifact}" >/dev/null 2>&1; then
  storage_child_ready="PASS"
fi

storage_cell() {
  local name="$1" point="$2"
  local cell_dir="${storage_dir}/${name}"
  local before_dump="${cell_dir}/before.json"
  local after_dump="${cell_dir}/after.json"
  local kill_receipt="${cell_dir}/kill-receipt.json"
  local pair_status="FAIL"
  if [[ "${storage_child_ready}" == "PASS" && -s "${before_dump}" && -s "${after_dump}" ]]; then
    case "${name}" in
      fsync-error)
        if jq -n --slurpfile before "${before_dump}" --slurpfile after "${after_dump}" \
          '($before | length) == 1 and ($after | length) == 1
           and $before[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
           and $after[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
           and $before[0].cell == "fsync-error" and $after[0].cell == "fsync-error"
           and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
           and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
           and $before[0].fault == "FSYNC_ERROR" and $after[0].fault == "FSYNC_ERROR"
           and $before[0].dump_forced == true and $after[0].dump_forced == true
           and $before[0].durable_store_read == true and $after[0].durable_store_read == true
           and $before[0].flush_sync_failure_observed == true
           and $before[0].write_outcome_uncertain == true
           and $after[0].write_outcome_uncertain == false
           and $after[0].value_recovered_exactly == true
           and $before[0].key_sha256 == $after[0].key_sha256
           and $before[0].value_sha256 == $after[0].value_sha256
           and ($before[0].process_pid | type == "number")
           and ($after[0].process_pid | type == "number")
           and $before[0].process_pid != $after[0].process_pid
           and $after[0].recovery_action == "FRESH_PROCESS_REOPENED_AFTER_FSYNC_FAILURE"' \
          >/dev/null 2>&1; then
          pair_status="PASS"
        fi
        ;;
      sst-corruption)
        if jq -n --slurpfile before "${before_dump}" --slurpfile after "${after_dump}" \
          '($before | length) == 1 and ($after | length) == 1
           and $before[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
           and $after[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
           and $before[0].cell == "sst-corruption" and $after[0].cell == "sst-corruption"
           and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
           and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
           and $before[0].fault == "SST_CORRUPTION" and $after[0].fault == "SST_CORRUPTION"
           and $before[0].dump_forced == true and $after[0].dump_forced == true
           and $before[0].durable_store_read == true and $after[0].durable_store_read == true
           and $before[0].clean_checkpoint_present == true
           and ($before[0].clean_checkpoint_file_count | type == "number" and . >= 1)
           and $after[0].corruption_rejected == true
           and $after[0].clean_restore_exact == true
           and $before[0].clean_checkpoint_inventory_sha256
               != $after[0].corrupt_checkpoint_inventory_sha256
           and $before[0].key_sha256 == $after[0].key_sha256
           and $before[0].value_sha256 == $after[0].value_sha256
           and ($before[0].process_pid | type == "number")
           and ($after[0].process_pid | type == "number")
           and $before[0].process_pid != $after[0].process_pid
           and $after[0].recovery_action
               == "FRESH_PROCESS_REJECTED_CORRUPT_SST_AND_RESTORED_CLEAN_CHECKPOINT"' \
          >/dev/null 2>&1; then
          pair_status="PASS"
        fi
        ;;
      enospc)
        if jq -n --slurpfile before "${before_dump}" --slurpfile after "${after_dump}" \
          '($before | length) == 1 and ($after | length) == 1
           and $before[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
           and $after[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
           and $before[0].cell == "enospc" and $after[0].cell == "enospc"
           and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
           and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
           and $before[0].fault == "ENOSPC" and $after[0].fault == "ENOSPC"
           and $before[0].dump_forced == true and $after[0].dump_forced == true
           and $before[0].durable_store_read == true and $after[0].durable_store_read == true
           and $before[0].enospc_observed == true
           and $before[0].enospc_status == "NO_SPACE_LEFT_ON_DEVICE"
           and $before[0].headroom_file_present == true
           and ($before[0].filler_records_attempted | type == "number" and . >= 1)
           and $after[0].enospc_recovered == true
           and $after[0].space_released_before_recovery == true
           and $after[0].value_recovered_exactly == true
           and $after[0].write_outcome_uncertain == false
           and $before[0].key_sha256 == $after[0].key_sha256
           and $before[0].value_sha256 == $after[0].value_sha256
           and ($before[0].process_pid | type == "number")
           and ($after[0].process_pid | type == "number")
           and $before[0].process_pid != $after[0].process_pid
           and $after[0].recovery_action
               == "FRESH_PROCESS_REOPENED_AFTER_ENOSPC_AND_HEADROOM_RELEASE"' \
          >/dev/null 2>&1; then
          pair_status="PASS"
        fi
        ;;
      disaster-host-fault)
        if [[ -s "${kill_receipt}" ]] \
          && jq -n --slurpfile before "${before_dump}" --slurpfile after "${after_dump}" \
            --slurpfile kill "${kill_receipt}" \
            '($before | length) == 1 and ($after | length) == 1 and ($kill | length) == 1
             and $before[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and $after[0].schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and $kill[0].schema == "nereus-delay-storage-chaos-kill-receipt-v1"
             and $before[0].cell == "disaster-host-fault" and $after[0].cell == "disaster-host-fault"
             and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
             and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
             and $before[0].fault == "DISASTER_HOST_FAULT"
             and $after[0].fault == "DISASTER_HOST_FAULT"
             and $before[0].dump_forced == true and $after[0].dump_forced == true
             and $before[0].durable_store_read == true and $after[0].durable_store_read == true
             and $before[0].host_fault_pending == true
             and $before[0].store_left_open_for_host_fault == true
             and $kill[0].fault == "DISASTER_HOST_FAULT"
             and $kill[0].signal == "SIGKILL" and $kill[0].signal_number == 9
             and $kill[0].exact_target == true
             and $kill[0].target_process_pid == $before[0].process_pid
             and $after[0].host_fault_pending == false
             and $after[0].host_fault_signal == "SIGKILL"
             and $after[0].value_recovered_exactly == true
             and $before[0].key_sha256 == $after[0].key_sha256
             and $before[0].value_sha256 == $after[0].value_sha256
             and ($before[0].process_pid | type == "number")
             and ($after[0].process_pid | type == "number")
             and $before[0].process_pid != $after[0].process_pid
             and $after[0].recovery_action
                 == "FRESH_PROCESS_REOPENED_AFTER_SIGKILL_AND_REPLAYED_DURABLE_STORE"' \
            >/dev/null 2>&1; then
          pair_status="PASS"
        fi
        ;;
    esac
  fi
  if [[ "${pair_status}" == "PASS" ]]; then
    add_cell "$(jq -cn --arg name "${name}" --arg point "${point}" \
      --arg child "${storage_artifact}" --arg before "${before_dump}" --arg after "${after_dump}" \
      --arg kill "${kill_receipt}" \
      '{($name): {
        status:"PASS",
        injection:{status:"PASS",point:$point},
        before_after:{status:"PASS",audit:{status:"CAPTURED_AND_VERIFIED",before_dump:$before,after_dump:$after}},
        fresh_process_recovery:"PASS",
        invariant_audit:"INDEPENDENT_FIELDS_PASS",
        evidence:(
          {child_artifact:$child,before_dump:$before,after_dump:$after}
          + (if $name == "disaster-host-fault" then {kill_receipt:$kill} else {} end)
        )
      }}')"
  else
    blocked_cell "${name}" "${point}" \
      "local storage before/after child failed independent fresh-process validation (exit=${storage_exit})"
  fi
}

storage_cell fsync-error "fail the directory/WAL flush boundary after an accepted synchronous WriteBatch"
storage_cell sst-corruption "corrupt one copied SST after clean checkpoint publication and restore the clean image"
storage_cell enospc "fill an exact 128 MiB mounted Store filesystem until native ENOSPC, then release headroom"
storage_cell disaster-host-fault "SIGKILL the exact host-side Worker JVM after durable local Store read"

long_gc_dir="${artifact_dir}/long-gc"
long_gc_artifact="${long_gc_dir}/long-gc-e2e.json"
long_gc_exit=1
if [[ "${source_status}" == "PASS" ]]; then
  mkdir -p "${long_gc_dir}"
  set +e
  NEREUS_DELAY_LONG_GC_E2E_ARTIFACT_DIR="${long_gc_dir}" \
  NEREUS_DELAY_LONG_GC_GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_LONG_GC_SOURCE_LOCK="${candidate_lock_file}" \
    bash "${script_dir}/run-long-gc-chaos-e2e.sh" \
      >"${artifact_dir}/long-gc-child.log" 2>&1
  long_gc_exit=$?
  set -e
else
  echo "source boundary blocked; long-GC child was not started" \
    >"${artifact_dir}/long-gc-child.log"
fi

long_gc_status="BLOCKED"
long_gc_before="${long_gc_dir}/before.json"
long_gc_after="${long_gc_dir}/after.json"
if [[ "${long_gc_exit}" == "0" && -s "${long_gc_artifact}" \
    && -s "${long_gc_before}" && -s "${long_gc_after}" ]] \
    && jq -e --arg delay "${candidate_delay}" \
      '.schema == "nereus-delay-long-gc-e2e-v1"
       and .status == "PASS"
       and .cell == "long-gc"
       and .source_locks.delay == $delay
       and .before_process.exit_code == 0
       and .after_process.exit_code == 0
       and .fresh_process_recovery == true
       and .invariant_audit == "INDEPENDENT_FIELDS_PASS"
       and .docker_cleanup.status == "PASS"' \
      "${long_gc_artifact}" >/dev/null 2>&1 \
    && jq -n --slurpfile before "${long_gc_before}" --slurpfile after "${long_gc_after}" \
      '($before | length) == 1 and ($after | length) == 1
       and $before[0].schema == "nereus-delay-long-gc-durable-state-dump-v1"
       and $after[0].schema == "nereus-delay-long-gc-durable-state-dump-v1"
       and $before[0].cell == "long-gc" and $after[0].cell == "long-gc"
       and $before[0].phase == "BEFORE_FRESH_PROCESS_RECOVERY"
       and $after[0].phase == "RECOVERED_AFTER_FRESH_PROCESS"
       and $before[0].fault == "LONG_GC" and $after[0].fault == "LONG_GC"
       and $before[0].dump_forced == true and $after[0].dump_forced == true
       and $before[0].durable_store_read == true and $after[0].durable_store_read == true
       and $before[0].gc_pause_observed == true
       and ($before[0].gc_collection_count_delta | type == "number" and . > 0)
       and ($before[0].gc_collection_time_delta_ms | type == "number" and . >= 50)
       and $before[0].scheduler_pending_before == 1
       and $after[0].durable_store_value_recovered_exactly == true
       and $after[0].scheduler_item_served == true
       and $after[0].scheduler_pending_after == 0
       and ($before[0].process_pid | type == "number")
       and ($after[0].process_pid | type == "number")
       and $before[0].process_pid != $after[0].process_pid
       and $before[0].key_sha256 == $after[0].key_sha256
       and $before[0].value_sha256 == $after[0].value_sha256
       and $before[0].due_item_sha256 == $after[0].due_item_sha256' \
      >/dev/null 2>&1; then
  long_gc_status="PASS"
fi
if [[ "${long_gc_status}" == "PASS" ]]; then
  add_cell "$(jq -cn --arg child "${long_gc_artifact}" \
    --arg before "${long_gc_before}" --arg after "${long_gc_after}" \
    --arg log "${artifact_dir}/long-gc-child.log" \
    '{"long-gc": {
      status:"PASS",
      injection:{status:"PASS",point:"induce real JVM heap pressure and a measured long GC at the due-admission boundary"},
      before_after:{status:"PASS",audit:{status:"CAPTURED_AND_VERIFIED",before_dump:$before,after_dump:$after}},
      fresh_process_recovery:"PASS",
      invariant_audit:"INDEPENDENT_FIELDS_PASS",
      evidence:{child_artifact:$child,before_dump:$before,after_dump:$after,run_log:$log}
    }}')"
else
  blocked_cell long-gc "pause the Worker at the long-GC admission boundary" \
    "real long-GC before/after child failed or its independent audit did not pass (exit=${long_gc_exit})"
fi

all_pass="PASS"
for name in "${required_cells[@]}"; do
  status="$(jq -r --arg name "${name}" '.[$name].status // "BLOCKED"' <<<"${cells_json}")"
  [[ "${status}" == "PASS" ]] || all_pass="BLOCKED"
  invariant="$(jq -r --arg name "${name}" '.[$name].invariant_audit // "BLOCKED"' <<<"${cells_json}")"
  [[ "${invariant}" == "INDEPENDENT_FIELDS_PASS" ]] || all_pass="BLOCKED"
  durable="$(jq -r --arg name "${name}" '.[$name].before_after.audit.status // "BLOCKED"' <<<"${cells_json}")"
  [[ "${durable}" == "CAPTURED_AND_VERIFIED" ]] || all_pass="BLOCKED"
done
[[ "${source_status}" == "PASS" && "${bounded_exit}" == "0" \
    && "${bounded_status}" == "PASS_CERTIFIED" && "${bounded_locks_match}" == "PASS" ]] || all_pass="BLOCKED"

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
  if ((${#blocked_reasons[@]} > 0)); then
    boundaries_json="$(printf '%s\n' "${blocked_reasons[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
  else
    boundaries_json='[]'
  fi
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
  --arg bounded_locks_match "${bounded_locks_match}" \
  '{
    schema:$schema,status:(if $status == "PASS" then "PASS_CERTIFIED" else "BLOCKED" end),
    profile_id:$profile_id,scope:"full-v1",complete_v1:($status == "PASS"),
    gate:"chaos",execution:"strict-sequential",artifact_dir:$artifact,
    started_at:$started_at,finished_at:$finished_at,source_locks:$locks,
    coverage:{complete_v1:($status == "PASS"),required:$required,observed:$observed,exclusions:[]},
    evidence:{test_exit_code:$test_exit_code,source_lock_status:(if $locks then "PASS" else "BLOCKED" end),
      coverage_status:$coverage_status,independent_audit:$independent_audit,
      bounded_child:{status:$bounded_status,exit_code:$bounded_exit,artifact:$bounded_artifact,source_locks_match:$bounded_locks_match},
      minio_child:$minio_artifact},
    cells:$cells,boundaries:$boundaries
  }' >"${artifact_dir}/full-chaos-matrix.json"

echo "V1 full chaos matrix artifact: ${artifact_dir}/full-chaos-matrix.json"
echo "status=$(jq -r '.status' "${artifact_dir}/full-chaos-matrix.json") bounded=${bounded_status} bounded_locks=${bounded_locks_match} minio_exit=${minio_exit}"
jq -r '.boundaries[]? // empty' "${artifact_dir}/full-chaos-matrix.json" | head -n 40
[[ "${all_pass}" == "PASS" ]]
