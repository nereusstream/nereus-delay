#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Full V1 release gate.  Bounded RC1 receipts are intentionally not accepted:
# every input must use the full-v1 contract and the frozen candidate source
# lock.  A later Delay documentation overlay is allowed only on the six
# evidence-ledger paths enforced by verify-v1-evidence-manifest.sh.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-v1-release-gate.XXXXXX)}"
gradle_home="${NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
candidate_lock_file="${NEREUS_DELAY_RELEASE_GATE_CANDIDATE_SOURCE_LOCK:-}"
run_check="${NEREUS_DELAY_RELEASE_GATE_RUN_CHECK:-1}"
allow_not_ready="${NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY:-0}"

gate_names=(
  protocol-golden chaos real-service no-early benchmark capacity soak
  upgrade-downgrade operations patch-distribution
)

if ! command -v jq >/dev/null 2>&1; then
  echo "V1 full release gate requires jq" >&2
  exit 1
fi
if ! command -v git >/dev/null 2>&1; then
  echo "V1 full release gate requires git" >&2
  exit 1
fi
[[ "${run_check}" == "0" || "${run_check}" == "1" ]] \
  || { echo "NEREUS_DELAY_RELEASE_GATE_RUN_CHECK must be 0 or 1" >&2; exit 1; }
[[ "${allow_not_ready}" == "0" || "${allow_not_ready}" == "1" ]] \
  || { echo "NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY must be 0 or 1" >&2; exit 1; }
[[ -n "${candidate_lock_file}" ]] \
  || { echo "NEREUS_DELAY_RELEASE_GATE_CANDIDATE_SOURCE_LOCK is required for full V1" >&2; exit 1; }
[[ -s "${candidate_lock_file}" ]] \
  || { echo "candidate source-lock file is missing or empty: ${candidate_lock_file}" >&2; exit 1; }
jq empty "${candidate_lock_file}" >/dev/null 2>&1 \
  || { echo "candidate source-lock file is not valid JSON: ${candidate_lock_file}" >&2; exit 1; }
mkdir -p "${artifact_dir}" "${gradle_home}"

checks_jsonl="${artifact_dir}/.release-gate-checks.jsonl"
: >"${checks_jsonl}"

add_check() {
  local name="$1" status="$2" detail="$3" artifact="${4:-}"
  jq -n --arg name "${name}" --arg status "${status}" --arg detail "${detail}" \
    --arg artifact "${artifact}" \
    '{name:$name,status:$status,detail:$detail,artifact:$artifact}' >>"${checks_jsonl}"
}

candidate_delay="$(jq -er '.delay' "${candidate_lock_file}")"
candidate_kafka="$(jq -er '.kafka' "${candidate_lock_file}")"
candidate_pulsar="$(jq -er '.pulsar' "${candidate_lock_file}")"
candidate_oxia="$(jq -er '.oxia' "${candidate_lock_file}")"
for lock in "${candidate_delay}" "${candidate_kafka}" "${candidate_pulsar}" "${candidate_oxia}"; do
  [[ "${lock}" =~ ^[0-9a-f]{40}$ ]] \
    || { echo "candidate source lock is not a canonical SHA: ${lock}" >&2; exit 1; }
done

current_delay=unknown current_kafka=unknown current_pulsar=unknown current_oxia=unknown
source_failures=0
check_checkout() {
  local label="$1" path="$2" expected_branch="$3" expected_source="$4"
  if [[ ! -e "${path}/.git" ]]; then
    add_check "source-${label}" BLOCKED "checkout missing: ${path}"
    source_failures=$((source_failures + 1)); return
  fi
  if [[ -n "$(git -C "${path}" status --porcelain)" ]]; then
    add_check "source-${label}" BLOCKED "worktree is dirty: ${path}"
    source_failures=$((source_failures + 1)); return
  fi
  local branch source
  branch="$(git -C "${path}" branch --show-current)"
  source="$(git -C "${path}" rev-parse HEAD)"
  case "${label}" in
    delay) current_delay="${source}";;
    kafka) current_kafka="${source}";;
    pulsar) current_pulsar="${source}";;
    oxia) current_oxia="${source}";;
  esac
  if [[ "${branch}" != "${expected_branch}" ]]; then
    add_check "source-${label}" BLOCKED "expected branch ${expected_branch}, got ${branch}"
    source_failures=$((source_failures + 1)); return
  fi
  if [[ "${source}" != "${expected_source}" ]]; then
    add_check "source-${label}" BLOCKED "HEAD ${source} does not match candidate ${expected_source}"
    source_failures=$((source_failures + 1)); return
  fi
  add_check "source-${label}" PASS "clean ${expected_branch}@${source} matches candidate lock"
}

check_checkout delay "${delay_dir}" nereus/delay-full-implementation-v1 "${candidate_delay}"
check_checkout kafka "${kafka_dir}" nereus/delay-guarded-producer-v1 "${candidate_kafka}"
check_checkout pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1 "${candidate_pulsar}"
check_checkout oxia "${oxia_dir}" main "${candidate_oxia}"

# A final evidence overlay may advance only Delay and may change exactly the
# six ledgers.  Convert the expected source mismatch into a pass only after
# proving that ancestry and the exact allowlist both hold.
if [[ "${current_delay}" != unknown && "${current_delay}" != "${candidate_delay}" ]]; then
  tmp_checks="${checks_jsonl}.tmp"
  jq -c 'select(.name != "source-delay")' "${checks_jsonl}" >"${tmp_checks}"
  mv "${tmp_checks}" "${checks_jsonl}"
  actual_paths="$(git -c core.quotePath=false -C "${delay_dir}" diff --name-only "${candidate_delay}" "${current_delay}" | sort -u)"
  expected_paths="$(printf '%s\n' \
    'docs/IMPLEMENTATION-STATUS.md' \
    'docs/Nereus Delay V1 设计.md' \
    'docs/V1-DESIGN-AUDIT.md' \
    'docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md' \
    'docs/V1-OPERATIONS-RUNBOOK.md' 'e2e/README.md' | sort -u)"
  if git -C "${delay_dir}" merge-base --is-ancestor "${candidate_delay}" "${current_delay}" \
      && diff -u <(printf '%s\n' "${expected_paths}") <(printf '%s\n' "${actual_paths}") >/dev/null; then
    add_check source-delay PASS "clean documentation-only evidence overlay ${current_delay} descends from candidate ${candidate_delay}"
    source_failures=$((source_failures - 1))
  else
    add_check source-delay BLOCKED "Delay HEAD ${current_delay} is not an exact six-ledger overlay over candidate ${candidate_delay}"
  fi
fi

if [[ "${source_failures}" == 0 ]]; then
  set +e
  bash "${script_dir}/validate-cross-repo-contracts.sh" >"${artifact_dir}/cross-repo-validator.log" 2>&1
  validator_status=$?
  set -e
  if [[ "${validator_status}" == 0 ]]; then
    add_check cross-repo-contracts PASS "validator passed" "${artifact_dir}/cross-repo-validator.log"
  else
    add_check cross-repo-contracts BLOCKED "validator exited ${validator_status}" "${artifact_dir}/cross-repo-validator.log"
  fi
else
  add_check cross-repo-contracts BLOCKED "candidate/current source boundary is not trustworthy"
fi

if [[ "${run_check}" == 1 && "${source_failures}" == 0 ]]; then
  set +e
  GRADLE_USER_HOME="${gradle_home}" ./gradlew check --no-daemon --console=plain >"${artifact_dir}/gradle-check.log" 2>&1
  gradle_status=$?
  set -e
  if [[ "${gradle_status}" == 0 ]]; then
    add_check gradle-check PASS "full Gradle check passed" "${artifact_dir}/gradle-check.log"
  else
    add_check gradle-check BLOCKED "Gradle check exited ${gradle_status}" "${artifact_dir}/gradle-check.log"
  fi
elif [[ "${run_check}" == 0 ]]; then
  add_check gradle-check SKIPPED "NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=0"
else
  add_check gradle-check BLOCKED "candidate/current source boundary is not trustworthy"
fi

candidate_locks_json="$(jq -cn --arg delay "${candidate_delay}" --arg kafka "${candidate_kafka}" \
  --arg pulsar "${candidate_pulsar}" --arg oxia "${candidate_oxia}" \
  '{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}')"

required_for_gate() {
  case "$1" in
    protocol-golden) printf '%s\n' ndl1 crc enums version-bound-hash signature identity protobuf-golden jcs uint64 key-ordering state-golden kafka-lso-boundary kafka-empty-boundary pulsar-inclusive-boundary pulsar-strictness model-property-interleavings kafka-guarded-golden pulsar-guarded-golden;;
    # The design calls this the nineteen-cell matrix: credential binding
    # drift is a separate cell from generic configuration drift because the
    # former exercises the rotation/lease fence while the latter exercises
    # static provider or Broker configuration validation.
    chaos) printf '%s\n' sigkill long-gc network-partition half-open enospc fsync-error sst-corruption broker-leader-failover oxia-session-expiry object-store-5xx object-store-timeout storage-provider-fault config-drift target-isolation disaster-host-fault kafka-response-loss lso-retention-floor pulsar-multibroker-failover credential-binding-drift;;
    real-service) printf '%s\n' kafka-to-kafka kafka-to-pulsar pulsar-to-kafka pulsar-to-pulsar gateway-mtls-jwt real-oxia real-minio real-worker activation-cutover kafka-lso-open-tx-aborted-marker-gap pulsar-batching-exclusive-inclusive pulsar-dedup-reconnect-attempt-journal mapping-before-send;;
    no-early) printf '%s\n' worker-clock-bound target-clock-bound pulsar-strict-delivery empty-partition boundary-arithmetic uncertainty-bound;;
    benchmark) printf '%s\n' command-throughput payload-throughput batch-writebatch-fsync ordered-unordered baseline-strong healthy-target-bad-target inline-object single-shard-multi-shard;;
    capacity) printf '%s\n' broker-throughput lane-distribution lane-fairness multi-worker-placement control-reserve adapter-physical-bound adapter-zombie-bound work-class-fairness checkpoint-restore inline-object bad-target-isolation slo-envelope command-payload-batch-writebatch-fsync;;
    soak) printf '%s\n' longest-checkpoint-period recovery-floor retry-period uncertainty-period gc-period source-continuity counter-continuity bounded-memory bounded-fd aged-uncertainty checkpoint-reopen;;
    upgrade-downgrade) printf '%s\n' writer-before-reader upgrade reader-before-writer downgrade same-bytes-different-version-dedupe backup-restore-fence;;
    operations) printf '%s\n' restore fence dlq uncertain-override disaster-recovery credential-rotation checkpoint-recovery oxia-recovery broker-recovery;;
    patch-distribution) printf '%s\n' kafka-full-rollout kafka-partial-rollout pulsar-full-rollout pulsar-partial-rollout binary-digest typed-rejection delete-recreate stock-client-rejection name-fallback-rejection old-protocol-rejection;;
    *) return 1;;
  esac
}

gate_artifact_path() {
  case "$1" in
    protocol-golden) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_PROTOCOL_GOLDEN_ARTIFACT:-}";;
    chaos) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_CHAOS_FULL_ARTIFACT:-}";;
    real-service) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_REAL_SERVICE_ARTIFACT:-}";;
    no-early) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_NO_EARLY_ARTIFACT:-}";;
    benchmark) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_BENCHMARK_ARTIFACT:-}";;
    capacity) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_CAPACITY_FULL_ARTIFACT:-}";;
    soak) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_SOAK_FULL_ARTIFACT:-}";;
    upgrade-downgrade) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_UPGRADE_DOWNGRADE_ARTIFACT:-}";;
    operations) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_OPERATIONS_FULL_ARTIFACT:-}";;
    patch-distribution) printf '%s' "${NEREUS_DELAY_RELEASE_GATE_PATCH_DISTRIBUTION_ARTIFACT:-}";;
    *) return 1;;
  esac
}

check_full_gate_artifact() {
  local gate="$1" path="$2"
  if [[ -z "${path}" ]]; then
    add_check "gate-${gate}" BLOCKED "no full-v1 artifact supplied; PASS_CERTIFIED is required"; return
  fi
  if [[ ! -s "${path}" ]] || ! jq empty "${path}" >/dev/null 2>&1; then
    add_check "gate-${gate}" BLOCKED "missing or invalid JSON artifact" "${path}"; return
  fi
  if [[ "${source_failures}" != 0 ]]; then
    add_check "gate-${gate}" BLOCKED "candidate/current source boundary is not trustworthy" "${path}"; return
  fi
  local required_json filter
  required_json="$(required_for_gate "${gate}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
  filter='(.schema == "nereus-delay-v1-full-gate-input-v1")
    and (.status == "PASS_CERTIFIED") and (.scope == "full-v1")
    and (.complete_v1 == true) and (.gate == $gate)
    and (.execution == "strict-sequential") and (.source_locks == $locks)
    and (.coverage | type == "object") and (.coverage.complete_v1 == true)
    and ((.coverage.exclusions | type) == "array") and ((.coverage.exclusions | length) == 0)
    and ((.coverage.required | sort | unique) == ($required | sort | unique))
    and ((.coverage.observed | sort | unique) == ($required | sort | unique))
    and (.evidence | type == "object") and (.evidence.test_exit_code == 0)
    and (.evidence.source_lock_status == "PASS") and (.evidence.coverage_status == "PASS")
    and (.evidence.independent_audit == "PASS") and ((.boundaries | type) == "array")
    and ((.boundaries | length) == 0)'
  case "${gate}" in
    chaos) filter="${filter}
      and (.cells | type == \"object\")
      and ((.cells | keys | sort | unique) == (\$required | sort | unique))
      and (. as \$root | all(\$required[]; . as \$cell | \$root.cells[\$cell].status == \"PASS\"
        and \$root.cells[\$cell].injection.status == \"PASS\"
        and \$root.cells[\$cell].before_after.status == \"PASS\"
        and \$root.cells[\$cell].before_after.audit.status == \"CAPTURED_AND_VERIFIED\"
        and \$root.cells[\$cell].fresh_process_recovery == \"PASS\"
        and \$root.cells[\$cell].invariant_audit == \"INDEPENDENT_FIELDS_PASS\"))";;
    real-service) filter="${filter}
      and (.observations.activation_cutover == \"PASS\")
      and (.observations.cross_route_paths == \"PASS\")
      and (.observations.real_services == \"PASS\")";;
    no-early) filter="${filter}
      and (.observations.worker_clock_bound_status == \"PASS\")
      and (.observations.target_clock_bound_status == \"PASS\")
      and (.observations.pulsar_strictness_status == \"PASS\")
      and (.observations.max_early_ms | type == \"number\" and . >= 0)
      and (.observations.clock_error_bound_ms | type == \"number\" and . >= 0)
      and (.observations.max_early_ms <= .observations.clock_error_bound_ms)";;
    benchmark) filter="${filter}
      and (.observations.required_configurations_status == \"PASS\")
      and (.observations.throughput_status == \"PASS\")
      and (.observations.slo_status == \"PASS\")
      and (.observations.full_v1_matrix_status == \"PASS\")";;
    capacity) filter="${filter}
      and (.observations.resource_control_reserve == \"PASS\")
      and (.observations.adapter_physical_bounds == \"PASS\")
      and (.observations.adapter_zombie_bounds == \"PASS\")
      and (.observations.lane_fairness == \"PASS\")
      and (.observations.slo_envelope == \"PASS\")
      and (.observations.required_configurations_status == \"PASS\")
      and (.observations.full_v1_matrix_status == \"PASS\")";;
    soak) filter="${filter}
      and (.policy.required_cycles | type == \"number\" and . >= 2 and . == floor)
      and (.policy.required_duration_seconds | type == \"number\" and . >= 1)
      and (.policy.longest_configured_period_seconds | type == \"number\" and . >= 1)
      and (.policy.required_duration_seconds >= .policy.longest_configured_period_seconds)
      and (.observations.source_continuity == \"PASS\")
      and (.observations.counter_continuity == \"PASS\")
      and (.observations.resource_bounds == \"PASS\")
      and (.observations.aged_uncertainty == \"PASS\")";;
    upgrade-downgrade) filter="${filter}
      and (.observations.writer_before_reader == \"PASS\")
      and (.observations.downgrade == \"PASS\")
      and (.observations.same_bytes_different_version_dedupe == \"PASS\")
      and (.observations.backup_restore_fence == \"PASS\")
      and (.observations.external_authority == \"PASS_CERTIFIED\")
      and (.child_evidence.real_child.status == \"PASS_CERTIFIED\")
      and (.child_evidence.real_child.exit_code == 0)";;
    operations) filter="${filter}
      and (.observations.restore == \"PASS\") and (.observations.fence == \"PASS\")
      and (.observations.dlq == \"PASS\") and (.observations.uncertain_override == \"PASS\")
      and (.observations.disaster_recovery == \"PASS\")";;
    patch-distribution) filter="${filter}
      and (.observations.full_rollout == \"PASS\") and (.observations.partial_rollout == \"PASS\")
      and (.observations.binary_digest == \"PASS\") and (.observations.typed_rejection == \"PASS\")
      and (.observations.delete_recreate == \"PASS\")
      and (.observations.stock_name_old_protocol_rejection == \"PASS\")";;
    protocol-golden) :;;
  esac
  if jq -e --arg gate "${gate}" --argjson locks "${candidate_locks_json}" \
      --argjson required "${required_json}" "${filter}" "${path}" >/dev/null 2>&1; then
    add_check "gate-${gate}" PASS "full-v1 PASS_CERTIFIED artifact is exact-source and complete" "${path}"
  else
    add_check "gate-${gate}" BLOCKED "artifact is not a complete full-v1 PASS_CERTIFIED input for ${gate}" "${path}"
  fi
}

for gate in "${gate_names[@]}"; do
  check_full_gate_artifact "${gate}" "$(gate_artifact_path "${gate}")"
done

checks_artifact="${artifact_dir}/release-gate-checks.json"
jq -s '.' "${checks_jsonl}" >"${checks_artifact}"
release_artifact="${artifact_dir}/v1-release-candidate-gate.json"
release_status="$(jq -r 'if all(.[]; .status == "PASS") then "PASS" else "NOT_READY" end' "${checks_artifact}")"
overlay_status=none
[[ "${current_delay}" != "${candidate_delay}" ]] && overlay_status="${current_delay}"
jq -n --arg schema nereus-delay-v1-release-gate-v2 --arg scope full-v1 \
  --arg status "${release_status}" --arg candidate_delay "${candidate_delay}" \
  --arg candidate_kafka "${candidate_kafka}" --arg candidate_pulsar "${candidate_pulsar}" \
  --arg candidate_oxia "${candidate_oxia}" --arg delay "${current_delay}" \
  --arg kafka "${current_kafka}" --arg pulsar "${current_pulsar}" --arg oxia "${current_oxia}" \
  --arg overlay "${overlay_status}" --arg artifact "${artifact_dir}" \
  --slurpfile checks "${checks_artifact}" \
  '{schema:$schema,scope:$scope,release_status:$status,artifact_dir:$artifact,
    candidate_source_lock:{delay:$candidate_delay,kafka:$candidate_kafka,pulsar:$candidate_pulsar,oxia:$candidate_oxia},
    source_locks:{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia},
    evidence_overlay_delay_commit:$overlay,
    required_gates:["protocol-golden","chaos","real-service","no-early","benchmark","capacity","soak","upgrade-downgrade","operations","patch-distribution"],
    checks:$checks[0],
    boundaries:["Only the ten main-design full-v1 gates can produce PASS.",
      "Bounded, partial, historical, skipped, missing or boundary-excluding artifacts never satisfy PASS_CERTIFIED.",
      "Candidate source locks are frozen before a documentation-only evidence overlay; the external manifest verifies the final bytes."]}' >"${release_artifact}"
rm -f "${checks_jsonl}"

echo "V1 full release gate artifact: ${release_artifact}"
echo "release_status=${release_status}"
jq -r '.checks[] | "\(.name)=\(.status): \(.detail)"' "${release_artifact}"
if [[ "${release_status}" != PASS && "${allow_not_ready}" != 1 ]]; then
  echo "V1 full release gate is NOT_READY; set NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 only to retain the audit artifact" >&2
  exit 1
fi
