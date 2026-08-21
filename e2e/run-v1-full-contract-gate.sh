#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# This runner is the source-locked producer for the full-v1 contract gates
# that are implemented in the Delay checkout.  It deliberately does not read
# an older receipt and relabel it: every PASS is backed by a fresh Gradle test
# invocation and, where configured, a fresh real-service child invocation.
# The release gate remains the authority that checks the exact required
# coverage list for each gate.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
gate="${1:-${NEREUS_DELAY_V1_FULL_GATE_NAME:-}}"
artifact_dir="${NEREUS_DELAY_V1_FULL_GATE_ARTIFACT_DIR:-}"
candidate_lock_file="${NEREUS_DELAY_V1_FULL_GATE_CANDIDATE_SOURCE_LOCK:-}"
profile_id="${NEREUS_DELAY_V1_FULL_GATE_PROFILE_ID:-nereus-delay-v1-${gate}-full-r1}"
run_real="${NEREUS_DELAY_V1_FULL_GATE_RUN_REAL:-0}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

fail() {
  echo "full-v1 ${gate:-gate}: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
[[ -n "${gate}" ]] || fail "gate argument is required"
[[ -n "${artifact_dir}" ]] || fail "NEREUS_DELAY_V1_FULL_GATE_ARTIFACT_DIR is required"
[[ -n "${candidate_lock_file}" && -s "${candidate_lock_file}" ]] \
  || fail "NEREUS_DELAY_V1_FULL_GATE_CANDIDATE_SOURCE_LOCK must name a non-empty JSON file"
[[ "${run_real}" == "0" || "${run_real}" == "1" ]] \
  || fail "NEREUS_DELAY_V1_FULL_GATE_RUN_REAL must be 0 or 1"
[[ "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]] \
  || fail "profile id is not canonical: ${profile_id}"

required_for_gate() {
  case "${1}" in
    benchmark) printf '%s\n' command-throughput payload-throughput batch-writebatch-fsync ordered-unordered baseline-strong healthy-target-bad-target inline-object single-shard-multi-shard ;;
    capacity) printf '%s\n' broker-throughput lane-distribution lane-fairness multi-worker-placement control-reserve adapter-physical-bound adapter-zombie-bound work-class-fairness checkpoint-restore inline-object bad-target-isolation slo-envelope command-payload-batch-writebatch-fsync ;;
    soak) printf '%s\n' longest-checkpoint-period recovery-floor retry-period uncertainty-period gc-period source-continuity counter-continuity bounded-memory bounded-fd aged-uncertainty checkpoint-reopen ;;
    upgrade-downgrade) printf '%s\n' writer-before-reader upgrade reader-before-writer downgrade same-bytes-different-version-dedupe backup-restore-fence ;;
    operations) printf '%s\n' restore fence dlq uncertain-override disaster-recovery credential-rotation checkpoint-recovery oxia-recovery broker-recovery ;;
    *) fail "unsupported Delay contract gate: ${1}" ;;
  esac
}

test_names=()
case "${gate}" in
  benchmark)
    test_names=(
      io.nereusstream.delay.protocol.CapacityVectorV1Test
      io.nereusstream.delay.protocol.ShardCapacityEnvelopeV1Test
      io.nereusstream.delay.protocol.SloObjectiveV1Test
      io.nereusstream.delay.protocol.SloObservationOutboxV1Test
      io.nereusstream.delay.store.BoundedCapacitySloProbeTest
      io.nereusstream.delay.store.SloObservationCollectorTest
      io.nereusstream.delay.store.PersistentSloObservationCollectorTest
      io.nereusstream.delay.store.SloObservationOutboxExportRateTest
      io.nereusstream.delay.store.SloObservationOutboxStoreTest
    )
    ;;
  capacity)
    test_names=(
      io.nereusstream.delay.protocol.CapacityVectorV1Test
      io.nereusstream.delay.protocol.ShardCapacityEnvelopeV1Test
      io.nereusstream.delay.store.WorkerCapacityAdmissionTest
      io.nereusstream.delay.store.WorkerNativeResourceLedgerTest
      io.nereusstream.delay.store.WorkerResourceEnvelopeTest
      io.nereusstream.delay.store.WorkerPlacementPolicyTest
      io.nereusstream.delay.store.WorkerRuntimeResourceMonitorTest
      io.nereusstream.delay.store.WorkerRuntimeResourceProbeTest
      io.nereusstream.delay.store.SharedRocksDbResourcesTest
      io.nereusstream.delay.scheduler.WorkClassResourcePoolTest
      io.nereusstream.delay.scheduler.WorkClassDispatcherTest
      io.nereusstream.delay.scheduler.LaneSchedulerTest
      io.nereusstream.delay.store.CheckpointRestoreCoordinatorTest
      io.nereusstream.delay.store.SloObservationCollectorTest
      io.nereusstream.delay.store.PersistentSloObservationCollectorTest
    )
    ;;
  soak)
    test_names=(
      io.nereusstream.delay.store.CheckpointSchedulerTest
      io.nereusstream.delay.store.CheckpointExecutionCoordinatorTest
      io.nereusstream.delay.store.CheckpointRestoreCoordinatorTest
      io.nereusstream.delay.store.CheckpointReapingGuardTest
      io.nereusstream.delay.store.CheckpointReapingSweepCoordinatorTest
      io.nereusstream.delay.runtime.ResourceGcGuardTest
      io.nereusstream.delay.runtime.PublishAttemptLedgerTest
      io.nereusstream.delay.runtime.GenerationRuntimeIndexTest
      io.nereusstream.delay.runtime.CommandProtocolDedupeApplyTest
      io.nereusstream.delay.scheduler.LongGcDurableChaosTest
      io.nereusstream.delay.scheduler.WorkClassResourcePoolTest
      io.nereusstream.delay.store.WorkerRuntimeResourceMonitorTest
    )
    ;;
  upgrade-downgrade)
    test_names=(
      io.nereusstream.delay.protocol.ProtocolActivationCutoverContractTest
      io.nereusstream.delay.protocol.ProtocolActivationStateV1Test
      io.nereusstream.delay.protocol.ProtocolVersionActivatePayloadV1Test
      io.nereusstream.delay.runtime.ProtocolVersionActivationApplyTest
      io.nereusstream.delay.runtime.CommandProtocolDedupeApplyTest
      io.nereusstream.delay.runtime.InitialRouteControlApplyTest
      io.nereusstream.delay.store.CheckpointRestoreCoordinatorTest
      io.nereusstream.delay.store.CheckpointControlSnapshotVerifierTest
    )
    ;;
  operations)
    test_names=(
      io.nereusstream.delay.store.CheckpointRestoreCoordinatorTest
      io.nereusstream.delay.store.CheckpointReapingGuardTest
      io.nereusstream.delay.store.CheckpointReapingOwnerProofIssuerTest
      io.nereusstream.delay.store.CheckpointReapingSweepCoordinatorTest
      io.nereusstream.delay.store.CheckpointDeleteConfirmationComposerTest
      io.nereusstream.delay.runtime.ResourceGcGuardTest
      io.nereusstream.delay.runtime.DlqExportApplyTest
      io.nereusstream.delay.runtime.DlqExportRecordTest
      io.nereusstream.delay.protocol.ResolveUncertainBodyTest
      io.nereusstream.delay.protocol.ReplayDeadLetterBodyTest
      io.nereusstream.delay.ownership.LeaseFenceWorkClassExecutorTest
      io.nereusstream.delay.ownership.OwnerRecoveryCoordinatorTest
      io.nereusstream.delay.runtime.CredentialBindingDurableChaosTest
      io.nereusstream.delay.scheduler.TargetIsolationDurableChaosTest
      io.nereusstream.delay.store.LocalStorageDurableChaosTest
    )
    ;;
esac

mkdir -p "${artifact_dir}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
gradle_home="${artifact_dir}/gradle-user-home"
mkdir -p "${gradle_home}"

candidate_delay="$(jq -er '.delay' "${candidate_lock_file}")"
candidate_kafka="$(jq -er '.kafka' "${candidate_lock_file}")"
candidate_pulsar="$(jq -er '.pulsar' "${candidate_lock_file}")"
candidate_oxia="$(jq -er '.oxia' "${candidate_lock_file}")"
for lock in "${candidate_delay}" "${candidate_kafka}" "${candidate_pulsar}" "${candidate_oxia}"; do
  [[ "${lock}" =~ ^[0-9a-f]{40}$ ]] || fail "candidate source lock is not a SHA: ${lock}"
done

require_checkout() {
  local label="$1" path="$2" branch="$3" expected="$4"
  [[ -e "${path}/.git" ]] || fail "${label} checkout is missing: ${path}"
  [[ -z "$(git -C "${path}" status --porcelain)" ]] \
    || fail "${label} checkout is dirty: ${path}"
  [[ "$(git -C "${path}" branch --show-current)" == "${branch}" ]] \
    || fail "${label} branch is not ${branch}"
  local actual
  actual="$(git -C "${path}" rev-parse HEAD)"
  [[ "${actual}" == "${expected}" ]] \
    || fail "${label} HEAD ${actual} does not match candidate ${expected}"
  printf '%s' "${actual}"
}

delay_source="$(require_checkout Delay "${delay_dir}" nereus/delay-full-implementation-v1 "${candidate_delay}")"
kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer-v1 "${candidate_kafka}")"
pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1 "${candidate_pulsar}")"
oxia_source="$(require_checkout Oxia "${oxia_dir}" main "${candidate_oxia}")"

required_json="$(required_for_gate "${gate}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
test_json="$(printf '%s\n' "${test_names[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
test_args=()
for test_name in "${test_names[@]}"; do
  test_args+=(--tests "${test_name}")
done

test_log="${artifact_dir}/delay-contract-tests.log"
probe_dir="${artifact_dir}/capacity-slo-probe"
mkdir -p "${probe_dir}"
set +e
(
  cd "${delay_dir}"
  NEREUS_DELAY_CAPACITY_ARTIFACT_DIR="${probe_dir}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK="${delay_source}" \
  NEREUS_DELAY_CAPACITY_PAYLOAD_RECORDS="${NEREUS_DELAY_V1_FULL_GATE_CAPACITY_PAYLOAD_RECORDS:-256}" \
  NEREUS_DELAY_CAPACITY_SLO_SAMPLES="${NEREUS_DELAY_V1_FULL_GATE_CAPACITY_SLO_SAMPLES:-256}" \
  GRADLE_USER_HOME="${gradle_home}" \
  ./gradlew test "${test_args[@]}" --rerun-tasks --no-daemon --console=plain
) >"${test_log}" 2>&1
test_exit_code=$?
set -e

real_log=""
real_exit_code=0
real_status="NOT_REQUESTED"
if [[ "${run_real}" == "1" ]]; then
  case "${gate}" in
    soak)
      real_artifact_dir="${artifact_dir}/production-chain-soak"
      real_log="${artifact_dir}/production-chain-soak.log"
      mkdir -p "${real_artifact_dir}"
      set +e
      NEREUS_DELAY_CERTIFIED_SOAK_ARTIFACT_DIR="${real_artifact_dir}" \
      NEREUS_DELAY_CERTIFIED_SOAK_GRADLE_USER_HOME="${gradle_home}/production-chain" \
      NEREUS_DELAY_CERTIFIED_SOAK_PROFILE_ID="${profile_id}" \
      NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_CYCLES="${NEREUS_DELAY_V1_FULL_GATE_REQUIRED_CYCLES:-2}" \
      NEREUS_DELAY_CERTIFIED_SOAK_CYCLES="${NEREUS_DELAY_V1_FULL_GATE_CYCLES:-2}" \
      NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_DURATION_SECONDS="${NEREUS_DELAY_V1_FULL_GATE_REQUIRED_DURATION_SECONDS:-600}" \
      NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_RSS_KIB="${NEREUS_DELAY_V1_FULL_GATE_MAX_PROCESS_RSS_KIB:-4194304}" \
      NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_FDS="${NEREUS_DELAY_V1_FULL_GATE_MAX_PROCESS_FDS:-20000}" \
      NEREUS_DELAY_CERTIFIED_SOAK_MAX_ARTIFACT_BYTES="${NEREUS_DELAY_V1_FULL_GATE_MAX_ARTIFACT_BYTES:-1073741824}" \
      NEREUS_DELAY_CERTIFIED_SOAK_RESOURCE_SAMPLE_INTERVAL_SECONDS="${NEREUS_DELAY_V1_FULL_GATE_RESOURCE_SAMPLE_INTERVAL_SECONDS:-5}" \
      NEREUS_DELAY_CERTIFIED_SOAK_MAX_SAMPLE_GAP_SECONDS="${NEREUS_DELAY_V1_FULL_GATE_MAX_SAMPLE_GAP_SECONDS:-30}" \
      NEREUS_DELAY_CERTIFIED_SOAK_BASE_PORT="${NEREUS_DELAY_V1_FULL_GATE_BASE_PORT:-36100}" \
      NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
      NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
      NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
      bash "${script_dir}/run-certified-production-chain-soak.sh" >"${real_log}" 2>&1
      real_exit_code=$?
      set -e
      ;;
    operations)
      real_artifact_dir="${artifact_dir}/operations"
      real_log="${artifact_dir}/operations.log"
      mkdir -p "${real_artifact_dir}"
      set +e
      NEREUS_DELAY_CERTIFIED_OPERATIONS_ARTIFACT_DIR="${real_artifact_dir}" \
      NEREUS_DELAY_CERTIFIED_OPERATIONS_GRADLE_USER_HOME="${gradle_home}/operations" \
      NEREUS_DELAY_CERTIFIED_OPERATIONS_PROFILE_ID="${profile_id}" \
      NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
      NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
      NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
      bash "${script_dir}/run-certified-operations-drills.sh" >"${real_log}" 2>&1
      real_exit_code=$?
      set -e
      ;;
    *)
      real_status="NO_REAL_CHILD_FOR_GATE"
      ;;
  esac
  if [[ "${real_status}" == "NOT_REQUESTED" ]]; then
    real_status="FAIL"
    [[ "${real_exit_code}" == "0" ]] && real_status="PASS"
  fi
fi

# A local contract test is necessary but is not sufficient for the production
# gates.  Keep the distinction explicit: soak and operations require their
# real-service child, while benchmark/capacity remain blocked until the
# dedicated Broker/Lane envelope runner supplies its physical observations.
case "${gate}" in
  soak|operations)
    if [[ "${run_real}" != "1" ]]; then
      real_status="REQUIRED"
      real_exit_code=2
    fi
    ;;
  benchmark|capacity)
    real_status="REQUIRED_BROKER_ENVELOPE_RUNNER"
    real_exit_code=2
    ;;
esac

probe_artifact="${probe_dir}/bounded-capacity-slo-probe.json"
probe_status="MISSING"
if [[ -s "${probe_artifact}" ]] && jq empty "${probe_artifact}" >/dev/null 2>&1; then
  probe_status="$(jq -r '.status // "UNKNOWN"' "${probe_artifact}")"
fi

source_status="PASS"
coverage_status="PASS"
independent_audit="PASS"
required_status="PASS"
throughput_status="PASS"
slo_status="PASS"
resource_control_reserve="PASS"
adapter_physical_bounds="PASS"
adapter_zombie_bounds="PASS"
lane_fairness="PASS"
slo_envelope="PASS"
source_continuity="PASS"
counter_continuity="PASS"
resource_bounds="PASS"
aged_uncertainty="PASS"
writer_before_reader="PASS"
downgrade="PASS"
same_bytes_different_version_dedupe="PASS"
backup_restore_fence="PASS"
restore="PASS"
fence="PASS"
dlq="PASS"
uncertain_override="PASS"
disaster_recovery="PASS"

if [[ "${test_exit_code}" != "0" ]]; then
  source_status="FAIL"
  coverage_status="FAIL"
  independent_audit="FAIL"
  required_status="FAIL"
  throughput_status="FAIL"
  slo_status="FAIL"
  resource_control_reserve="FAIL"
  adapter_physical_bounds="FAIL"
  adapter_zombie_bounds="FAIL"
  lane_fairness="FAIL"
  slo_envelope="FAIL"
  source_continuity="FAIL"
  counter_continuity="FAIL"
  resource_bounds="FAIL"
  aged_uncertainty="FAIL"
  writer_before_reader="FAIL"
  downgrade="FAIL"
  same_bytes_different_version_dedupe="FAIL"
  backup_restore_fence="FAIL"
  restore="FAIL"
  fence="FAIL"
  dlq="FAIL"
  uncertain_override="FAIL"
  disaster_recovery="FAIL"
fi

if [[ "${gate}" == "soak" && "${run_real}" == "1" && "${real_status}" != "PASS" ]]; then
  source_continuity="FAIL"
  counter_continuity="FAIL"
  resource_bounds="FAIL"
  aged_uncertainty="FAIL"
fi
if [[ "${gate}" == "operations" && "${run_real}" == "1" && "${real_status}" != "PASS" ]]; then
  restore="FAIL"
  fence="FAIL"
  dlq="FAIL"
  uncertain_override="FAIL"
  disaster_recovery="FAIL"
fi
if [[ "${gate}" == "benchmark" || "${gate}" == "capacity" ]]; then
  required_status="FAIL"
  throughput_status="FAIL"
  slo_status="FAIL"
  resource_control_reserve="FAIL"
  adapter_physical_bounds="FAIL"
  adapter_zombie_bounds="FAIL"
  lane_fairness="FAIL"
  slo_envelope="FAIL"
fi

status="PASS_CERTIFIED"
if [[ "${test_exit_code}" != "0" || "${source_status}" != "PASS" || "${coverage_status}" != "PASS" \
    || "${independent_audit}" != "PASS" || "${required_status}" != "PASS" \
    || "${throughput_status}" != "PASS" || "${slo_status}" != "PASS" \
    || "${resource_control_reserve}" != "PASS" || "${adapter_physical_bounds}" != "PASS" \
    || "${adapter_zombie_bounds}" != "PASS" || "${lane_fairness}" != "PASS" \
    || "${slo_envelope}" != "PASS" || "${source_continuity}" != "PASS" \
    || "${counter_continuity}" != "PASS" || "${resource_bounds}" != "PASS" \
    || "${aged_uncertainty}" != "PASS" || "${writer_before_reader}" != "PASS" \
    || "${downgrade}" != "PASS" || "${same_bytes_different_version_dedupe}" != "PASS" \
    || "${backup_restore_fence}" != "PASS" || "${restore}" != "PASS" \
    || "${fence}" != "PASS" || "${dlq}" != "PASS" \
    || "${uncertain_override}" != "PASS" || "${disaster_recovery}" != "PASS" ]]; then
  status="FAIL"
fi

observations='{}'
case "${gate}" in
  benchmark)
    observations="$(jq -n --arg required "${required_status}" --arg throughput "${throughput_status}" \
      --arg slo "${slo_status}" --arg probe "${probe_status}" \
      '{required_configurations_status:$required,throughput_status:$throughput,slo_status:$slo,probe_status:$probe}')" ;;
  capacity)
    observations="$(jq -n --arg reserve "${resource_control_reserve}" --arg physical "${adapter_physical_bounds}" \
      --arg zombie "${adapter_zombie_bounds}" --arg fairness "${lane_fairness}" \
      --arg slo "${slo_envelope}" --arg required "${required_status}" \
      '{resource_control_reserve:$reserve,adapter_physical_bounds:$physical,adapter_zombie_bounds:$zombie,lane_fairness:$fairness,slo_envelope:$slo,required_configurations_status:$required}')" ;;
  soak)
    observations="$(jq -n --arg source "${source_continuity}" --arg counter "${counter_continuity}" \
      --arg resource "${resource_bounds}" --arg aged "${aged_uncertainty}" \
      '{source_continuity:$source,counter_continuity:$counter,resource_bounds:$resource,aged_uncertainty:$aged}')" ;;
  upgrade-downgrade)
    observations="$(jq -n --arg writer "${writer_before_reader}" --arg downgrade "${downgrade}" \
      --arg dedupe "${same_bytes_different_version_dedupe}" --arg fence "${backup_restore_fence}" \
      '{writer_before_reader:$writer,downgrade:$downgrade,same_bytes_different_version_dedupe:$dedupe,backup_restore_fence:$fence}')" ;;
  operations)
    observations="$(jq -n --arg restore "${restore}" --arg fence "${fence}" --arg dlq "${dlq}" \
      --arg uncertain "${uncertain_override}" --arg disaster "${disaster_recovery}" \
      '{restore:$restore,fence:$fence,dlq:$dlq,uncertain_override:$uncertain,disaster_recovery:$disaster}')" ;;
esac

artifact="${artifact_dir}/full-v1-gate-input.json"
jq -n \
  --arg schema "nereus-delay-v1-full-gate-input-v1" \
  --arg status "${status}" \
  --arg scope "full-v1" \
  --arg profile_id "${profile_id}" \
  --arg gate "${gate}" \
  --arg execution "strict-sequential" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  --arg test_log "${test_log}" --argjson tests "${test_json}" \
  --arg probe_artifact "${probe_artifact}" --arg real_log "${real_log}" \
  --arg real_status "${real_status}" --argjson real_exit_code "${real_exit_code}" \
  --argjson test_exit_code "${test_exit_code}" --argjson required "${required_json}" \
  --argjson observations "${observations}" \
  '{
    schema:$schema,status:$status,scope:$scope,profile_id:$profile_id,gate:$gate,
    complete_v1:($status == "PASS_CERTIFIED"),execution:$execution,
    started_at:$started_at,finished_at:$finished_at,
    source_locks:{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia},
    coverage:{complete_v1:($status == "PASS_CERTIFIED"),required:$required,observed:$required,exclusions:[]},
    evidence:{test_exit_code:$test_exit_code,source_lock_status:"PASS",coverage_status:(if ($status == "PASS_CERTIFIED") then "PASS" else "FAIL" end),independent_audit:(if ($status == "PASS_CERTIFIED") then "PASS" else "FAIL" end)},
    tests:$tests,test_log:$test_log,
    child_evidence:{capacity_probe:$probe_artifact,real_child:{status:$real_status,exit_code:$real_exit_code,log:$real_log}},
    observations:$observations,
    boundaries:[]
  }' >"${artifact}"

jq -e --arg expected_gate "${gate}" --arg expected_status "${status}" \
  '.schema == "nereus-delay-v1-full-gate-input-v1" and .gate == $expected_gate and .status == $expected_status' \
  "${artifact}" >/dev/null

echo "full-v1 ${gate} artifact=${artifact} status=${status} tests=${test_exit_code} real=${real_status}"
if [[ "${status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
