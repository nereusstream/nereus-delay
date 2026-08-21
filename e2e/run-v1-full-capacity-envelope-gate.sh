#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Full-v1 benchmark/capacity producer.  The Delay tests and the optional real
# multi-shard chains are necessary evidence, but they do not manufacture a
# capacity envelope.  A physical measurement artifact, produced by the
# approved load/telemetry harness, is therefore a hard input to PASS.  This
# keeps a local RocksDB probe or a functional E2E from being promoted to a
# Broker/Lane/resource certification.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
gate="${1:-${NEREUS_DELAY_V1_CAPACITY_GATE_NAME:-}}"
artifact_dir="${NEREUS_DELAY_V1_CAPACITY_ARTIFACT_DIR:-}"
candidate_lock_file="${NEREUS_DELAY_V1_CAPACITY_CANDIDATE_SOURCE_LOCK:-}"
measurement_artifact="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_ARTIFACT:-}"
measurement_runner="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_RUNNER:-${script_dir}/run-v1-physical-capacity-observation.sh}"
run_measurement="${NEREUS_DELAY_V1_CAPACITY_RUN_MEASUREMENT:-0}"
profile_id="${NEREUS_DELAY_V1_CAPACITY_PROFILE_ID:-nereus-delay-v1-${gate}-physical-envelope-r1}"
run_real="${NEREUS_DELAY_V1_CAPACITY_RUN_REAL:-0}"
measurement_gradle_home="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_GRADLE_USER_HOME:-}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

fail() {
  echo "full-v1 ${gate:-capacity}: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
[[ "${gate}" == "benchmark" || "${gate}" == "capacity" ]] \
  || fail "gate must be benchmark or capacity"
[[ -n "${artifact_dir}" ]] || fail "NEREUS_DELAY_V1_CAPACITY_ARTIFACT_DIR is required"
[[ -n "${candidate_lock_file}" && -s "${candidate_lock_file}" ]] \
  || fail "NEREUS_DELAY_V1_CAPACITY_CANDIDATE_SOURCE_LOCK must name a non-empty JSON file"
[[ "${run_real}" == "0" || "${run_real}" == "1" ]] \
  || fail "NEREUS_DELAY_V1_CAPACITY_RUN_REAL must be 0 or 1"
[[ "${run_measurement}" == "0" || "${run_measurement}" == "1" ]] \
  || fail "NEREUS_DELAY_V1_CAPACITY_RUN_MEASUREMENT must be 0 or 1"
[[ "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]] \
  || fail "profile id is not canonical: ${profile_id}"

if [[ "${gate}" == "benchmark" ]]; then
  required_lines=(
    command-throughput payload-throughput batch-writebatch-fsync
    ordered-unordered baseline-strong healthy-target-bad-target
    inline-object single-shard-multi-shard
  )
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
else
  required_lines=(
    broker-throughput lane-distribution lane-fairness multi-worker-placement
    control-reserve adapter-physical-bound adapter-zombie-bound
    work-class-fairness checkpoint-restore inline-object bad-target-isolation
    slo-envelope command-payload-batch-writebatch-fsync
  )
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
fi

mkdir -p "${artifact_dir}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
if [[ "${run_measurement}" == "1" ]]; then
  [[ -x "${measurement_runner}" ]] || fail "measurement runner is not executable: ${measurement_runner}"
  [[ -z "${measurement_artifact}" ]] \
    || fail "do not combine NEREUS_DELAY_V1_CAPACITY_RUN_MEASUREMENT=1 with a supplied measurement artifact"
  measurement_run_dir="${artifact_dir}/physical-capacity-measurement"
  mkdir -p "${measurement_run_dir}"
  measurement_artifact="${measurement_run_dir}/capacity-observation.json"
  set +e
  NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_ARTIFACT_DIR="${measurement_run_dir}" \
  NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_CANDIDATE_SOURCE_LOCK="${candidate_lock_file}" \
  NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_PROFILE_ID="${profile_id}" \
  NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_GATE="${gate}" \
  NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_RUN_REAL="${run_real}" \
  NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_GRADLE_USER_HOME="${measurement_gradle_home}" \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${measurement_runner}"
  measurement_runner_exit=$?
  set -e
  if [[ "${measurement_runner_exit}" != "0" ]]; then
    measurement_detail="source-locked physical measurement runner failed with exit ${measurement_runner_exit}"
  fi
fi
gradle_home="${NEREUS_DELAY_V1_CAPACITY_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
real_gradle_home="${NEREUS_DELAY_V1_CAPACITY_REAL_GRADLE_USER_HOME:-${gradle_home}}"
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

required_json="$(printf '%s\n' "${required_lines[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
test_json="$(printf '%s\n' "${test_names[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
test_args=()
for test_name in "${test_names[@]}"; do
  test_args+=(--tests "${test_name}")
done

test_log="${artifact_dir}/delay-capacity-contract-tests.log"
probe_dir="${artifact_dir}/capacity-slo-probe"
mkdir -p "${probe_dir}"
set +e
(
  cd "${delay_dir}"
  NEREUS_DELAY_CAPACITY_ARTIFACT_DIR="${probe_dir}" \
  NEREUS_DELAY_CAPACITY_SOURCE_LOCK="${delay_source}" \
  NEREUS_DELAY_CAPACITY_PAYLOAD_RECORDS="${NEREUS_DELAY_V1_CAPACITY_PAYLOAD_RECORDS:-256}" \
  NEREUS_DELAY_CAPACITY_SLO_SAMPLES="${NEREUS_DELAY_V1_CAPACITY_SLO_SAMPLES:-256}" \
  GRADLE_USER_HOME="${gradle_home}" \
  ./gradlew test "${test_args[@]}" --rerun-tasks --no-daemon --console=plain
) >"${test_log}" 2>&1
test_exit_code=$?
set -e

real_status="NOT_REQUESTED"
real_exit_code=0
real_logs=()
real_logs_json='[]'
if [[ "${run_real}" == "1" ]]; then
  real_status="PASS"
  kafka_log="${artifact_dir}/kafka-large-payload-multi-shard.log"
  pulsar_log="${artifact_dir}/pulsar-large-payload-multi-shard.log"
  real_logs+=("${kafka_log}" "${pulsar_log}")

  set +e
  (
    cd "${delay_dir}"
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC="nereus-delay-v1-capacity-kafka-destination-${profile_id}" \
    NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME="${real_gradle_home}" \
    bash "${script_dir}/run-large-payload-gateway-e2e.sh"
  ) >"${kafka_log}" 2>&1
  kafka_real_exit=$?

  (
    cd "${delay_dir}"
    NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC="nereus-delay-v1-capacity-pulsar-destination-${profile_id}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME="${real_gradle_home}" \
    bash "${script_dir}/run-pulsar-large-payload-gateway-e2e.sh"
  ) >"${pulsar_log}" 2>&1
  pulsar_real_exit=$?
  set -e

  if [[ "${kafka_real_exit}" != "0" || "${pulsar_real_exit}" != "0" ]]; then
    real_status="FAIL"
    real_exit_code=1
  fi
  real_logs_json="$(printf '%s\n' "${real_logs[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
else
  real_status="REQUIRED"
  real_exit_code=2
fi

measurement_status="MISSING"
measurement_source_status="MISSING"
measurement_config_status="MISSING"
measurement_cells_status="MISSING"
full_matrix_status="MISSING"
measurement_detail="A physical measurement artifact is required"
if [[ -n "${measurement_artifact}" && -s "${measurement_artifact}" ]] \
    && jq empty "${measurement_artifact}" >/dev/null 2>&1; then
  measurement_source_status="FAIL"
  measurement_config_status="FAIL"
  measurement_cells_status="FAIL"
  if jq -e --argjson required "${required_json}" \
      --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
      --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
      '.schema == "nereus-delay-v1-capacity-observation-v1"
       and .status == "PASS"
       and .source_locks == {delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}
       and ((.required_configurations | sort | unique) == ($required | sort | unique))
       and ((.observed_configurations | sort | unique) == ($required | sort | unique))
       and (.measurements | type == "object")
       and (.status == "PASS")
       and (.campaign | type == "object")
       and (.campaign.bounded_matrix.exit_code == 0)
       and (.campaign.physical_contract_tests.exit_code == 0)
       and (.campaign.real_kafka_large_payload.exit_code == 0)
       and (.campaign.real_pulsar_large_payload.exit_code == 0)
       and (.campaign.full_v1_matrix.status == "PASS")
       and ((.campaign.full_v1_matrix.record_cardinalities | sort) == [1000000,10000000,100000000])
       and ((.campaign.full_v1_matrix.arrival_patterns | sort | unique) == ["burst","uniform","zipf"])
       and ((.campaign.full_v1_matrix.ordering_modes | sort | unique) == ["ordered","unordered"])
       and ((.campaign.full_v1_matrix.consistency_modes | sort | unique) == ["baseline","strong"])
       and ((.campaign.full_v1_matrix.target_health | sort | unique) == ["bad","healthy"])
       and ((.campaign.full_v1_matrix.placement_modes | sort | unique) == ["multi-shard","single-shard"])
       and ((.campaign.full_v1_matrix.payload_modes | sort | unique) == ["inline","object"])
       and (.campaign.full_v1_matrix.observations | type == "array" and length >= 8)
       and all(.campaign.full_v1_matrix.observations[];
         .status == "PASS"
         and (.metrics | type == "object" and length >= 3)
         and (.invariants | type == "array" and length >= 3)
         and (.provenance | type == "object"))
       and (.platform_observation | type == "object")
       and (.platform_observation.cgroup_memory_limit_bytes | type == "number" and . > 0)
       and (.platform_observation.direct_memory_bytes | type == "number" and . > 0)
       and (.platform_observation.max_open_files | type == "number" and . > 0)
       and (.platform_observation.process_rss_bytes | type == "number" and . > 0)
       and (.platform_observation.current_open_files | type == "number" and . > 0)
       and (. as $root | all($required[]; . as $name
         | ($root.measurements[$name] | type == "object")
         and ($root.measurements[$name].status == "PASS")
         and ($root.measurements[$name].invariant_status == "PASS")
         and ($root.measurements[$name].authority | type == "string" and length > 0)
         and ($root.measurements[$name].dimensions | type == "object")
         and ($root.measurements[$name].metrics | type == "object" and length >= 3)
         and ($root.measurements[$name].invariants | type == "array" and length >= 3)
         and ($root.measurements[$name].provenance.source_locks == {delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia})
         and ($root.measurements[$name].provenance.commands | type == "array" and length >= 1)
         and ($root.measurements[$name].provenance.artifacts | type == "array" and length >= 1)
         and ($root.measurements[$name].provenance.artifact_sha256 | type == "array"
              and length == ($root.measurements[$name].provenance.artifacts | length)
              and all(.[]; test("^[0-9a-f]{64}$")))
         and ($root.measurements[$name].provenance.exit_codes | type == "array"
              and all(.[]; .exit_code == 0))))' \
      "${measurement_artifact}" >/dev/null 2>&1; then
    measurement_status="PASS"
    measurement_source_status="PASS"
    measurement_config_status="PASS"
    measurement_cells_status="PASS"
    full_matrix_status="PASS"
    measurement_detail="exact source-locked physical envelope observations"
  else
    measurement_detail="measurement artifact is stale, incomplete or not PASS"
  fi
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

if [[ "${test_exit_code}" != "0" ]]; then
  source_status="FAIL"
  coverage_status="FAIL"
  independent_audit="FAIL"
  required_status="FAIL"
fi
if [[ "${run_real}" != "1" || "${real_status}" != "PASS" ]]; then
  required_status="FAIL"
fi
if [[ "${measurement_status}" != "PASS" ]]; then
  required_status="FAIL"
  throughput_status="FAIL"
  slo_status="FAIL"
  resource_control_reserve="FAIL"
  adapter_physical_bounds="FAIL"
  adapter_zombie_bounds="FAIL"
  lane_fairness="FAIL"
  slo_envelope="FAIL"
  full_matrix_status="FAIL"
fi
if [[ "${gate}" == "benchmark" ]]; then
  resource_control_reserve="PASS"
  adapter_physical_bounds="PASS"
  adapter_zombie_bounds="PASS"
  lane_fairness="PASS"
fi

status="PASS_CERTIFIED"
if [[ "${test_exit_code}" != "0" || "${real_status}" != "PASS" \
    || "${measurement_status}" != "PASS" || "${full_matrix_status}" != "PASS" \
    || "${source_status}" != "PASS" \
    || "${coverage_status}" != "PASS" || "${independent_audit}" != "PASS" \
    || "${required_status}" != "PASS" || "${throughput_status}" != "PASS" \
    || "${slo_status}" != "PASS" || "${resource_control_reserve}" != "PASS" \
    || "${adapter_physical_bounds}" != "PASS" || "${adapter_zombie_bounds}" != "PASS" \
    || "${lane_fairness}" != "PASS" || "${slo_envelope}" != "PASS" ]]; then
  status="FAIL"
fi

observations="$(jq -n \
  --arg required "${required_status}" --arg throughput "${throughput_status}" \
  --arg slo "${slo_status}" --arg reserve "${resource_control_reserve}" \
  --arg physical "${adapter_physical_bounds}" --arg zombie "${adapter_zombie_bounds}" \
  --arg fairness "${lane_fairness}" --arg envelope "${slo_envelope}" \
  --arg measurement "${measurement_status}" --arg source "${measurement_source_status}" \
  --arg configs "${measurement_config_status}" --arg cells "${measurement_cells_status}" \
  --arg full_matrix "${full_matrix_status}" \
  '{required_configurations_status:$required,throughput_status:$throughput,slo_status:$slo,
    resource_control_reserve:$reserve,adapter_physical_bounds:$physical,
    adapter_zombie_bounds:$zombie,lane_fairness:$fairness,slo_envelope:$envelope,
    measurement_status:$measurement,measurement_source_status:$source,
    measurement_configurations_status:$configs,measurement_cells_status:$cells,
    full_v1_matrix_status:$full_matrix}')"

artifact="${artifact_dir}/full-v1-gate-input.json"
jq -n \
  --arg schema "nereus-delay-v1-full-gate-input-v1" \
  --arg status "${status}" --arg scope "full-v1" --arg profile_id "${profile_id}" \
  --arg gate "${gate}" --arg execution "strict-sequential" \
  --arg started_at "${started_at}" --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  --arg test_log "${test_log}" --argjson tests "${test_json}" \
  --arg measurement_artifact "${measurement_artifact}" \
  --arg measurement_detail "${measurement_detail}" \
  --argjson real_logs "${real_logs_json}" \
  --arg real_status "${real_status}" --argjson real_exit_code "${real_exit_code}" \
  --argjson test_exit_code "${test_exit_code}" --argjson required "${required_json}" \
  --argjson observations "${observations}" \
  '{schema:$schema,status:$status,scope:$scope,profile_id:$profile_id,gate:$gate,
    complete_v1:($status == "PASS_CERTIFIED"),execution:$execution,
    started_at:$started_at,finished_at:$finished_at,
    source_locks:{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia},
    coverage:{complete_v1:($status == "PASS_CERTIFIED"),required:$required,observed:$required,exclusions:[]},
    evidence:{test_exit_code:$test_exit_code,source_lock_status:"PASS",
      coverage_status:(if ($status == "PASS_CERTIFIED") then "PASS" else "FAIL" end),
      independent_audit:(if ($status == "PASS_CERTIFIED") then "PASS" else "FAIL" end)},
    tests:$tests,test_log:$test_log,
    child_evidence:{measurement_artifact:$measurement_artifact,
      measurement_detail:$measurement_detail,
      real_children:{status:$real_status,exit_code:$real_exit_code,logs:$real_logs}},
    observations:$observations,boundaries:[]}' >"${artifact}"

jq -e --arg expected_gate "${gate}" --arg expected_status "${status}" \
  '.schema == "nereus-delay-v1-full-gate-input-v1" and .gate == $expected_gate and .status == $expected_status' \
  "${artifact}" >/dev/null

echo "full-v1 ${gate} artifact=${artifact} status=${status} tests=${test_exit_code} real=${real_status} measurement=${measurement_status}"
if [[ "${status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
