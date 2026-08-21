#!/usr/bin/env bash
set -Eeuo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
artifact_dir="${NEREUS_DELAY_LONG_GC_E2E_ARTIFACT_DIR:-/private/tmp/nereus-delay-v1-long-gc-$(date +%Y%m%d-%H%M%S)}"
gradle_home="${NEREUS_DELAY_LONG_GC_GRADLE_USER_HOME:-${GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}}"
source_lock_file="${NEREUS_DELAY_LONG_GC_SOURCE_LOCK:-}"
java_tool_options="${NEREUS_DELAY_LONG_GC_JAVA_TOOL_OPTIONS:--Xmx512m -XX:+UseSerialGC}"
class_name="io.nereusstream.delay.scheduler.LongGcDurableChaosTest"
schema="nereus-delay-long-gc-e2e-v1"

fail() {
  echo "long-GC E2E: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
[[ "${artifact_dir}" != "/" && "${artifact_dir}" != "/private/tmp" ]] \
  || fail "artifact directory is too broad"
if [[ -e "${artifact_dir}" && -n "$(find "${artifact_dir}" -mindepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
mkdir -p "${artifact_dir}" "${gradle_home}"

actual_delay="$(git -C "${delay_dir}" rev-parse HEAD)"
[[ "${actual_delay}" =~ ^[0-9a-f]{40}$ ]] || fail "non-canonical Delay source SHA"
if [[ -n "${source_lock_file}" ]]; then
  [[ -s "${source_lock_file}" ]] || fail "source lock does not exist: ${source_lock_file}"
  jq empty "${source_lock_file}" >/dev/null 2>&1 || fail "source lock is not valid JSON"
  expected_delay="$(jq -er '.delay' "${source_lock_file}")"
  [[ "${expected_delay}" == "${actual_delay}" ]] \
    || fail "Delay source lock mismatch: expected=${expected_delay} actual=${actual_delay}"
else
  expected_delay="${actual_delay}"
fi

run_phase() {
  local phase="$1" log_file="$2"
  set +e
  JAVA_TOOL_OPTIONS="${java_tool_options}" \
  GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_LONG_GC_ARTIFACT_DIR="${artifact_dir}" \
  NEREUS_DELAY_LONG_GC_PHASE="${phase}" \
    "${delay_dir}/gradlew" test --no-daemon --console=plain --rerun-tasks \
      --tests "${class_name}" >"${log_file}" 2>&1
  local exit_code=$?
  set -e
  return "${exit_code}"
}

before_exit=1
after_exit=1
set +e
run_phase before "${artifact_dir}/before-process.log"
before_exit=$?
set -e
if [[ "${before_exit}" == "0" ]]; then
  set +e
  run_phase after "${artifact_dir}/after-process.log"
  after_exit=$?
  set -e
fi

before_dump="${artifact_dir}/before.json"
after_dump="${artifact_dir}/after.json"
status="BLOCKED"
if [[ "${before_exit}" == "0" && "${after_exit}" == "0" \
    && -s "${before_dump}" && -s "${after_dump}" ]] \
    && jq -n --slurpfile before "${before_dump}" --slurpfile after "${after_dump}" \
      --arg expected_delay "${expected_delay}" \
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
       and ($before[0].gc_memory_max_bytes | type == "number" and . > 0)
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
  status="PASS"
fi

docker_cleanup_status="PASS"
jq -n \
  --arg schema "${schema}" \
  --arg status "${status}" \
  --arg artifact_dir "${artifact_dir}" \
  --arg delay "${actual_delay}" \
  --arg expected_delay "${expected_delay}" \
  --arg before_dump "${before_dump}" \
  --arg after_dump "${after_dump}" \
  --arg before_log "${artifact_dir}/before-process.log" \
  --arg after_log "${artifact_dir}/after-process.log" \
  --arg java_tool_options "${java_tool_options}" \
  --argjson before_exit "${before_exit}" \
  --argjson after_exit "${after_exit}" \
  --arg docker_cleanup_status "${docker_cleanup_status}" \
  '{schema:$schema,status:$status,cell:"long-gc",artifact_dir:$artifact_dir,
    source_locks:{delay:$delay},source_lock_expected_delay:$expected_delay,
    java_tool_options:$java_tool_options,
    before_process:{exit_code:$before_exit,dump:$before_dump,log:$before_log},
    after_process:{exit_code:$after_exit,dump:$after_dump,log:$after_log},
    fresh_process_recovery:($status == "PASS"),
    invariant_audit:(if $status == "PASS" then "INDEPENDENT_FIELDS_PASS" else "BLOCKED" end),
    docker_cleanup:{status:$docker_cleanup_status,containers_started:0,remaining_related_containers:0}}' \
  >"${artifact_dir}/long-gc-e2e.json"

echo "long-GC E2E artifact: ${artifact_dir}/long-gc-e2e.json"
echo "status=${status} before_exit=${before_exit} after_exit=${after_exit}"
[[ "${status}" == "PASS" ]]
