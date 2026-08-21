#!/usr/bin/env bash
set -Eeuo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
artifact_dir="${NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR:-/private/tmp/nereus-delay-v1-local-storage-chaos-$(date +%Y%m%d-%H%M%S)}"
gradle_home="${NEREUS_DELAY_STORAGE_CHAOS_GRADLE_USER_HOME:-${GRADLE_USER_HOME:-}}"
if [[ -z "${gradle_home}" ]]; then
  gradle_home="${artifact_dir}/gradle-user-home"
fi

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

mkdir -p "${artifact_dir}"
class_name="io.nereusstream.delay.store.LocalStorageDurableChaosTest"
cells=(fsync-error sst-corruption disaster-host-fault)
cell_records=()

ensure_empty_cell() {
  local cell_dir="$1"
  if [[ -e "${cell_dir}" ]]; then
    [[ -d "${cell_dir}" ]] || fail "cell artifact is not a directory: ${cell_dir}"
    if [[ -n "$(find "${cell_dir}" -mindepth 1 -print -quit)" ]]; then
      fail "cell artifact must be a fresh exact directory: ${cell_dir}"
    fi
  else
    mkdir -p "${cell_dir}"
  fi
}

run_gradle_phase() {
  local cell="$1" phase="$2" cell_dir="$3" log_file="$4"
  set +e
  NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR="${cell_dir}" \
  NEREUS_DELAY_STORAGE_CHAOS_CELL="${cell}" \
  NEREUS_DELAY_STORAGE_CHAOS_PHASE="${phase}" \
  NEREUS_DELAY_STORAGE_CHAOS_HOLD_FILE="${cell_dir}/hold" \
  GRADLE_USER_HOME="${gradle_home}" \
    "${delay_dir}/gradlew" test --no-daemon --console=plain --rerun-tasks \
      --tests "${class_name}" >"${log_file}" 2>&1
  local exit_code=$?
  set -e
  return "${exit_code}"
}

run_regular_cell() {
  local cell="$1"
  local cell_dir="${artifact_dir}/${cell}"
  ensure_empty_cell "${cell_dir}"
  local before_log="${artifact_dir}/${cell}-before-process.log"
  local after_log="${artifact_dir}/${cell}-after-process.log"
  local before_exit=1 after_exit=1
  set +e
  run_gradle_phase "${cell}" before "${cell_dir}" "${before_log}"
  before_exit=$?
  set -e
  if [[ "${before_exit}" == "0" ]]; then
    set +e
    run_gradle_phase "${cell}" after "${cell_dir}" "${after_log}"
    after_exit=$?
    set -e
  fi
  validate_cell "${cell}" "${before_exit}" "${after_exit}" "${cell_dir}"
}

run_disaster_cell() {
  local cell="disaster-host-fault"
  local cell_dir="${artifact_dir}/${cell}"
  ensure_empty_cell "${cell_dir}"
  local hold_file="${cell_dir}/hold"
  : >"${hold_file}"
  local before_log="${artifact_dir}/${cell}-before-process.log"
  local after_log="${artifact_dir}/${cell}-after-process.log"
  set +e
  NEREUS_DELAY_STORAGE_CHAOS_ARTIFACT_DIR="${cell_dir}" \
  NEREUS_DELAY_STORAGE_CHAOS_CELL="${cell}" \
  NEREUS_DELAY_STORAGE_CHAOS_PHASE=before \
  NEREUS_DELAY_STORAGE_CHAOS_HOLD_FILE="${hold_file}" \
  GRADLE_USER_HOME="${gradle_home}" \
    "${delay_dir}/gradlew" test --no-daemon --console=plain --rerun-tasks \
      --tests "${class_name}" >"${before_log}" 2>&1 &
  local before_job=$!
  set -e

  local ready=0
  for _ in $(seq 1 120); do
    if [[ -s "${cell_dir}/ready" && -s "${cell_dir}/before.json" ]]; then
      ready=1
      break
    fi
    if ! kill -0 "${before_job}" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  [[ "${ready}" == "1" ]] || fail "disaster before phase did not publish a ready durable dump"

  local before_pid
  before_pid="$(jq -er '.process_pid | select(type == "number")' "${cell_dir}/before.json")"
  kill -0 "${before_pid}" 2>/dev/null || fail "before test JVM is not alive at the kill boundary: ${before_pid}"
  set +e
  kill -KILL "${before_pid}"
  local kill_exit=$?
  set -e
  jq -n --argjson pid "${before_pid}" --arg signal "SIGKILL" --argjson signal_number 9 \
    --argjson kill_exit "${kill_exit}" \
    '{schema:"nereus-delay-storage-chaos-kill-receipt-v1",fault:"DISASTER_HOST_FAULT",
      target_process_pid:$pid,signal:$signal,signal_number:$signal_number,kill_exit:$kill_exit,exact_target:true}' \
    >"${cell_dir}/kill-receipt.json"
  rm -f "${hold_file}"
  set +e
  wait "${before_job}"
  local before_exit=$?
  set -e

  local after_exit=1
  if [[ -s "${cell_dir}/before.json" && -s "${cell_dir}/kill-receipt.json" ]]; then
    set +e
    run_gradle_phase "${cell}" after "${cell_dir}" "${after_log}"
    after_exit=$?
    set -e
  fi
  validate_cell "${cell}" "${before_exit}" "${after_exit}" "${cell_dir}"
}

validate_cell() {
  local cell="$1" before_exit="$2" after_exit="$3" cell_dir="$4"
  local status="BLOCKED"
  if [[ -s "${cell_dir}/before.json" && -s "${cell_dir}/after.json" ]]; then
    case "${cell}" in
      fsync-error)
        if [[ "${before_exit}" == "0" && "${after_exit}" == "0" ]] \
          && jq -e \
            '.schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and .cell == "fsync-error"
             and .phase == "BEFORE_FRESH_PROCESS_RECOVERY"
             and .fault == "FSYNC_ERROR"
             and .dump_forced == true
             and .durable_store_read == true
             and .flush_sync_failure_observed == true
             and .write_outcome_uncertain == true
             and .value_written_before_fault == true' "${cell_dir}/before.json" >/dev/null \
          && jq -e \
            '.schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and .cell == "fsync-error"
             and .phase == "RECOVERED_AFTER_FRESH_PROCESS"
             and .fault == "FSYNC_ERROR"
             and .dump_forced == true
             and .durable_store_read == true
             and .value_recovered_exactly == true
             and .recovery_action == "FRESH_PROCESS_REOPENED_AFTER_FSYNC_FAILURE"
             and (.process_pid != $before_pid)' \
            --argjson before_pid "$(jq -er '.process_pid' "${cell_dir}/before.json")" \
            "${cell_dir}/after.json" >/dev/null; then
          status="PASS"
        fi
        ;;
      sst-corruption)
        if [[ "${before_exit}" == "0" && "${after_exit}" == "0" ]] \
          && jq -e \
            '.schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and .cell == "sst-corruption"
             and .phase == "BEFORE_FRESH_PROCESS_RECOVERY"
             and .fault == "SST_CORRUPTION"
             and .dump_forced == true
             and .durable_store_read == true
             and .clean_checkpoint_present == true
             and (.clean_checkpoint_file_count | type == "number" and . >= 1)' \
            "${cell_dir}/before.json" >/dev/null \
          && jq -e \
            '.schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and .cell == "sst-corruption"
             and .phase == "RECOVERED_AFTER_FRESH_PROCESS"
             and .fault == "SST_CORRUPTION"
             and .dump_forced == true
             and .durable_store_read == true
             and .corruption_rejected == true
             and .clean_restore_exact == true
             and .clean_checkpoint_inventory_sha256 != .corrupt_checkpoint_inventory_sha256
             and (.process_pid != $before_pid)' \
            --argjson before_pid "$(jq -er '.process_pid' "${cell_dir}/before.json")" \
            "${cell_dir}/after.json" >/dev/null; then
          status="PASS"
        fi
        ;;
      disaster-host-fault)
        if [[ "${after_exit}" == "0" && -s "${cell_dir}/kill-receipt.json" ]] \
          && jq -e \
            '.schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and .cell == "disaster-host-fault"
             and .phase == "BEFORE_FRESH_PROCESS_RECOVERY"
             and .fault == "DISASTER_HOST_FAULT"
             and .dump_forced == true
             and .durable_store_read == true
             and .host_fault_pending == true
             and .store_left_open_for_host_fault == true' "${cell_dir}/before.json" >/dev/null \
          && jq -e \
            '.schema == "nereus-delay-storage-chaos-kill-receipt-v1"
             and .fault == "DISASTER_HOST_FAULT"
             and .signal == "SIGKILL"
             and .signal_number == 9
             and .exact_target == true
             and .target_process_pid == $before_pid' \
            --argjson before_pid "$(jq -er '.process_pid' "${cell_dir}/before.json")" \
            "${cell_dir}/kill-receipt.json" >/dev/null \
          && jq -e \
            '.schema == "nereus-delay-storage-chaos-durable-state-dump-v1"
             and .cell == "disaster-host-fault"
             and .phase == "RECOVERED_AFTER_FRESH_PROCESS"
             and .fault == "DISASTER_HOST_FAULT"
             and .dump_forced == true
             and .durable_store_read == true
             and .host_fault_pending == false
             and .host_fault_signal == "SIGKILL"
             and .value_recovered_exactly == true
             and .recovery_action == "FRESH_PROCESS_REOPENED_AFTER_SIGKILL_AND_REPLAYED_DURABLE_STORE"
             and (.process_pid != $before_pid)' \
            --argjson before_pid "$(jq -er '.process_pid' "${cell_dir}/before.json")" \
            "${cell_dir}/after.json" >/dev/null; then
          status="PASS"
        fi
        ;;
    esac
  fi
  local record
  record="$(jq -cn --arg name "${cell}" --arg status "${status}" \
    --arg artifact "${cell_dir}" --argjson before_exit "${before_exit}" \
    --argjson after_exit "${after_exit}" \
    '{name:$name,status:$status,artifact:$artifact,before_exit:$before_exit,after_exit:$after_exit}')"
  cell_records+=("${record}")
  echo "${cell}: ${status} (before=${before_exit}, after=${after_exit})"
}

run_regular_cell fsync-error
run_regular_cell sst-corruption
run_disaster_cell

cells_json="$(printf '%s\n' "${cell_records[@]}" | jq -s '.')"
overall_status="PASS"
if jq -e 'any(.[]; .status != "PASS")' <<<"${cells_json}" >/dev/null; then
  overall_status="BLOCKED"
fi
delay_sha="$(git -C "${delay_dir}" rev-parse HEAD)"
summary_tmp="${artifact_dir}/local-storage-chaos-e2e.json.tmp"
jq -n --arg status "${overall_status}" --arg artifact "${artifact_dir}" \
  --arg delay "${delay_sha}" --argjson cells "${cells_json}" \
  '{
    schema:"nereus-delay-local-storage-chaos-e2e-v1",
    status:$status,
    source_locks:{delay:$delay},
    scope:"fsync-error,sst-corruption,disaster-host-fault",
    fresh_process_recovery:true,
    docker_cleanup:{status:"PASS",scope:"no Docker resources created"},
    artifact_dir:$artifact,
    cells:$cells
  }' >"${summary_tmp}"
mv "${summary_tmp}" "${artifact_dir}/local-storage-chaos-e2e.json"
[[ "${overall_status}" == "PASS" ]] || exit 1
