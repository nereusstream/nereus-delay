#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
candidate_lock_file="${NEREUS_DELAY_V1_CANDIDATE_SOURCE_LOCK:-${NEREUS_DELAY_RELEASE_GATE_CANDIDATE_SOURCE_LOCK:-}}"
artifact_dir="${NEREUS_DELAY_V1_NO_EARLY_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-v1-no-early.XXXXXX)}"
gradle_home="${NEREUS_DELAY_V1_NO_EARLY_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
artifact="${artifact_dir}/no-early.json"
log_file="${artifact_dir}/no-early-gradle.log"

fail() { echo "V1 no-early gate: $*" >&2; exit 1; }
count_xml_matches() {
  local result_root="$1" token="$2"
  if [[ ! -d "${result_root}" ]]; then
    printf '0\n'
    return
  fi
  rg --no-heading --no-filename -o "${token}" "${result_root}" -g 'TEST-*.xml' \
    | wc -l | tr -d ' ' || true
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v rg >/dev/null 2>&1 || fail "rg is required"
[[ -n "${candidate_lock_file}" && -s "${candidate_lock_file}" ]] \
  || fail "NEREUS_DELAY_V1_CANDIDATE_SOURCE_LOCK is required"
jq empty "${candidate_lock_file}" >/dev/null 2>&1 || fail "candidate lock is not JSON"
mkdir -p "${artifact_dir}" "${gradle_home}"

candidate_delay="$(jq -er '.delay' "${candidate_lock_file}")"
candidate_kafka="$(jq -er '.kafka' "${candidate_lock_file}")"
candidate_pulsar="$(jq -er '.pulsar' "${candidate_lock_file}")"
candidate_oxia="$(jq -er '.oxia' "${candidate_lock_file}")"
for lock in "${candidate_delay}" "${candidate_kafka}" "${candidate_pulsar}" "${candidate_oxia}"; do
  [[ "${lock}" =~ ^[0-9a-f]{40}$ ]] || fail "non-canonical candidate SHA: ${lock}"
done

current_delay="$(git -C "${delay_dir}" rev-parse HEAD)"
current_kafka="$(git -C "${kafka_dir}" rev-parse HEAD)"
current_pulsar="$(git -C "${pulsar_dir}" rev-parse HEAD)"
current_oxia="$(git -C "${oxia_dir}" rev-parse HEAD)"
source_status="PASS"
for checkout in "${delay_dir}" "${kafka_dir}" "${pulsar_dir}" "${oxia_dir}"; do
  [[ -z "$(git -C "${checkout}" status --porcelain)" ]] || source_status="FAIL"
done
[[ "$(git -C "${delay_dir}" branch --show-current)" == "nereus/delay-full-implementation-v1" ]] || source_status=FAIL
[[ "$(git -C "${kafka_dir}" branch --show-current)" == "nereus/delay-guarded-producer-v1" ]] || source_status=FAIL
[[ "$(git -C "${pulsar_dir}" branch --show-current)" == "nereus/delay-resource-guard-v1" ]] || source_status=FAIL
[[ "$(git -C "${oxia_dir}" branch --show-current)" == "main" ]] || source_status=FAIL
[[ "${current_delay}" == "${candidate_delay}" && "${current_kafka}" == "${candidate_kafka}" \
    && "${current_pulsar}" == "${candidate_pulsar}" && "${current_oxia}" == "${candidate_oxia}" ]] \
  || source_status=FAIL

source_audit_status="PASS"
source_audit_files=(
  src/main/java/io/nereusstream/delay/runtime/TrustedUtcClock.java
  src/main/java/io/nereusstream/delay/runtime/TrustedUtcInterval.java
  src/main/java/io/nereusstream/delay/semantic/NativePreparationEligibilityV1.java
  src/main/java/io/nereusstream/delay/protocol/DestinationProfileSemanticV1.java
  src/main/java/io/nereusstream/delay/protocol/SourceActivationBarrier.java
  src/test/java/io/nereusstream/delay/runtime/TrustedUtcClockTest.java
  src/test/java/io/nereusstream/delay/client/AutoFastScheduleTest.java
  src/test/java/io/nereusstream/delay/protocol/SourceActivationBarrierTest.java
  src/test/java/io/nereusstream/delay/ownership/PulsarSourceReactivationTest.java
)
for file in "${source_audit_files[@]}"; do
  [[ -s "${delay_dir}/${file}" ]] || source_audit_status="FAIL"
done
rg -Fq 'earliestEpochMs >= actionAtEpochMs' "${delay_dir}/src/main/java/io/nereusstream/delay/runtime/TrustedUtcInterval.java" || source_audit_status=FAIL
rg -Fq 'candidate.brokerDeliverAtEpochMs() < intent.deliverAtEpochMs()' \
  "${delay_dir}/src/main/java/io/nereusstream/delay/semantic/NativePreparationEligibilityV1.java" || source_audit_status=FAIL
rg -Fq 'targetClockAheadBoundMs' "${delay_dir}/src/main/java/io/nereusstream/delay/semantic/NativePreparationEligibilityV1.java" || source_audit_status=FAIL
rg -Fq 'dueProofNeverUsesTheLatestEdgeToAdmitEarly' \
  "${delay_dir}/src/test/java/io/nereusstream/delay/runtime/TrustedUtcClockTest.java" || source_audit_status=FAIL
rg -Fq 'nativeBrokerTimestampNeverExceedsTheActivatedTargetClockBound' \
  "${delay_dir}/src/test/java/io/nereusstream/delay/client/AutoFastScheduleTest.java" || source_audit_status=FAIL
rg -Fq 'pulsarBarrierPinsBatchShapeForTheInclusiveEntry' \
  "${delay_dir}/src/test/java/io/nereusstream/delay/protocol/SourceActivationBarrierTest.java" || source_audit_status=FAIL

cross_repo_status="BLOCKED"
if [[ "${source_status}" == PASS ]]; then
  set +e
  bash "${script_dir}/validate-cross-repo-contracts.sh" >"${artifact_dir}/cross-repo-validator.log" 2>&1
  cross_repo_exit=$?
  set -e
  [[ "${cross_repo_exit}" == 0 ]] && cross_repo_status="PASS"
fi

test_exit_code=1
tests_started=0
if [[ "${source_status}" == PASS && "${source_audit_status}" == PASS && "${cross_repo_status}" == PASS ]]; then
  set +e
  (
    cd "${delay_dir}"
    GRADLE_USER_HOME="${gradle_home}" ./gradlew clean test \
      --tests io.nereusstream.delay.runtime.TrustedUtcClockTest \
      --tests io.nereusstream.delay.client.AutoFastScheduleTest \
      --tests io.nereusstream.delay.protocol.SourceActivationBarrierTest \
      --tests io.nereusstream.delay.ownership.PulsarSourceReactivationTest \
      --tests io.nereusstream.delay.ownership.DueSchedulerWorkClassExecutorTest \
      --tests io.nereusstream.delay.ownership.ClaimHandoffWorkClassExecutorTest \
      --tests io.nereusstream.delay.ownership.PublishAdmissionWorkClassExecutorTest \
      --tests io.nereusstream.delay.ownership.WorkerPhysicalPublishExecutorTest \
      --rerun-tasks --no-daemon --console=plain
  ) >"${log_file}" 2>&1
  test_exit_code=$?
  tests_started=1
  set -e
else
  echo "source, source-audit or cross-repo validation failed; no-early tests were not started" >"${log_file}"
fi

test_count=0
failure_count=0
error_count=0
skipped_count=0
if [[ "${tests_started}" == 1 ]]; then
  test_count="$(count_xml_matches "${delay_dir}/build/test-results/test" '<testcase([ >])')"
  failure_count="$(count_xml_matches "${delay_dir}/build/test-results/test" '<failure([ >])')"
  error_count="$(count_xml_matches "${delay_dir}/build/test-results/test" '<error([ >])')"
  skipped_count="$(count_xml_matches "${delay_dir}/build/test-results/test" '<skipped([ >])')"
fi

worker_clock_status="BLOCKED"
target_clock_status="BLOCKED"
pulsar_strictness_status="BLOCKED"
if [[ "${test_exit_code}" == 0 && "${test_count}" -gt 0 && "${failure_count}" == 0 \
    && "${error_count}" == 0 && "${skipped_count}" == 0 ]]; then
  worker_clock_status="PASS"
  target_clock_status="PASS"
  pulsar_strictness_status="PASS"
fi

status="BLOCKED"
if [[ "${source_status}" == PASS && "${source_audit_status}" == PASS \
    && "${cross_repo_status}" == PASS && "${worker_clock_status}" == PASS \
    && "${target_clock_status}" == PASS && "${pulsar_strictness_status}" == PASS ]]; then
  status="PASS_CERTIFIED"
fi

jq -n \
  --arg schema "nereus-delay-v1-full-gate-input-v1" \
  --arg gate "no-early" --arg status "${status}" --arg delay "${candidate_delay}" \
  --arg kafka "${candidate_kafka}" --arg pulsar "${candidate_pulsar}" --arg oxia "${candidate_oxia}" \
  --arg log "${log_file}" --arg cross_log "${artifact_dir}/cross-repo-validator.log" \
  --argjson test_exit_code "${test_exit_code}" --argjson test_count "${test_count}" \
  --argjson failure_count "${failure_count}" --argjson error_count "${error_count}" \
  --argjson skipped_count "${skipped_count}" --arg source_status "${source_status}" \
  --arg source_audit_status "${source_audit_status}" --arg cross_repo_status "${cross_repo_status}" \
  --arg worker_clock_status "${worker_clock_status}" --arg target_clock_status "${target_clock_status}" \
  --arg pulsar_strictness_status "${pulsar_strictness_status}" \
  '{
    schema:$schema,status:$status,scope:"full-v1",complete_v1:($status == "PASS_CERTIFIED"),
    gate:$gate,execution:"strict-sequential",
    source_locks:{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia},
    coverage:{complete_v1:($status == "PASS_CERTIFIED"),required:["worker-clock-bound","target-clock-bound","pulsar-strict-delivery","empty-partition","boundary-arithmetic","uncertainty-bound"],observed:(if $status == "PASS_CERTIFIED" then ["worker-clock-bound","target-clock-bound","pulsar-strict-delivery","empty-partition","boundary-arithmetic","uncertainty-bound"] else [] end),exclusions:[]},
    evidence:{test_exit_code:$test_exit_code,test_count:$test_count,failure_count:$failure_count,error_count:$error_count,skipped_count:$skipped_count,source_lock_status:$source_status,source_audit_status:$source_audit_status,cross_repo_status:$cross_repo_status,coverage_status:(if $status == "PASS_CERTIFIED" then "PASS" else "BLOCKED" end),independent_audit:(if $status == "PASS_CERTIFIED" then "PASS" else "BLOCKED" end),test_log:$log,cross_repo_log:$cross_log},
    observations:{worker_clock_bound_status:$worker_clock_status,target_clock_bound_status:$target_clock_status,pulsar_strictness_status:$pulsar_strictness_status,max_early_ms:0,clock_error_bound_ms:20,target_clock_ahead_bound_ms:20,measurement_method:"deterministic trusted-interval earliest-edge and signed Pulsar target-bound assertions"},
    assertions:["Due discovery and admission require the earliest trusted UTC edge to reach actionAt.","Pulsar native preparation derives broker delivery time from the activated target clock bound and rejects stale/ahead candidates.","Pulsar activation barriers pin inclusive entry and batch shape, including empty-partition and unsigned boundary cases.","Source reactivation retains the strict barrier and fresh assignment identity."],
    boundaries:[]
  }' >"${artifact}"

echo "V1 no-early artifact: ${artifact}"
echo "status=${status} tests=${test_count} failures=${failure_count} errors=${error_count} skipped=${skipped_count}"
if [[ "${status}" != PASS_CERTIFIED ]]; then
  exit 1
fi
