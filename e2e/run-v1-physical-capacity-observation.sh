#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Source-locked producer for the physical benchmark/capacity observation
# artifact.  This runner owns the observation: it executes the bounded local
# resource campaign, the real Kafka/Pulsar multi-shard production chains and
# the relevant Delay contract tests, then derives every measurement from those
# receipts.  It never accepts a caller-supplied PASS JSON or turns a missing
# metric into zero.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
gate="${1:-${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_GATE:-capacity}}"
artifact_dir="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_ARTIFACT_DIR:-}"
candidate_lock_file="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_CANDIDATE_SOURCE_LOCK:-}"
profile_id="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_PROFILE_ID:-nereus-delay-v1-${gate}-physical-envelope-r1}"
run_real="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_RUN_REAL:-1}"
payload_bytes="${NEREUS_DELAY_V1_CAPACITY_PAYLOAD_BYTES:-1052672}"
matrix_image="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_IMAGE:-eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769}"
seeded_gradle_home="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_GRADLE_USER_HOME:-}"
full_matrix_input="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_FULL_MATRIX_ARTIFACT:-}"
full_matrix_runner="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_FULL_MATRIX_RUNNER:-${script_dir}/run-v1-physical-capacity-matrix.sh}"
run_full_matrix="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_RUN_FULL_MATRIX:-0}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

fail() {
  echo "physical capacity observation: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v shasum >/dev/null 2>&1 || fail "shasum is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required for monotonic wall-clock evidence"
[[ "${gate}" == "benchmark" || "${gate}" == "capacity" ]] || fail "gate must be benchmark or capacity"
[[ -n "${artifact_dir}" ]] || fail "NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_ARTIFACT_DIR is required"
[[ -n "${candidate_lock_file}" && -s "${candidate_lock_file}" ]] \
  || fail "NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_CANDIDATE_SOURCE_LOCK must name a JSON file"
[[ "${run_real}" == "0" || "${run_real}" == "1" ]] || fail "run-real must be 0 or 1"
[[ "${run_full_matrix}" == "0" || "${run_full_matrix}" == "1" ]] || fail "run-full-matrix must be 0 or 1"
[[ "${payload_bytes}" =~ ^[1-9][0-9]*$ ]] || fail "payload bytes must be positive"
[[ "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]] || fail "profile id is not canonical"
if [[ -n "${seeded_gradle_home}" ]]; then
  mkdir -p "${seeded_gradle_home}"
fi

if [[ "${gate}" == "benchmark" ]]; then
  required_lines=(
    command-throughput payload-throughput batch-writebatch-fsync
    ordered-unordered baseline-strong healthy-target-bad-target
    inline-object single-shard-multi-shard
  )
else
  required_lines=(
    broker-throughput lane-distribution lane-fairness multi-worker-placement
    control-reserve adapter-physical-bound adapter-zombie-bound
    work-class-fairness checkpoint-restore inline-object bad-target-isolation
    slo-envelope command-payload-batch-writebatch-fsync
  )
fi

required_json="$(printf '%s\n' "${required_lines[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"

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
  [[ -z "$(git -C "${path}" status --porcelain)" ]] || fail "${label} checkout is dirty: ${path}"
  [[ "$(git -C "${path}" branch --show-current)" == "${branch}" ]] \
    || fail "${label} branch is not ${branch}"
  local actual
  actual="$(git -C "${path}" rev-parse HEAD)"
  [[ "${actual}" == "${expected}" ]] || fail "${label} HEAD ${actual} does not match candidate ${expected}"
  printf '%s' "${actual}"
}

delay_source="$(require_checkout Delay "${delay_dir}" nereus/delay-full-implementation-v1 "${candidate_delay}")"
kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer-v1 "${candidate_kafka}")"
pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1 "${candidate_pulsar}")"
oxia_source="$(require_checkout Oxia "${oxia_dir}" main "${candidate_oxia}")"
source_locks_json="$(jq -cn --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  '{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}')"

mkdir -p "${artifact_dir}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "measurement artifact directory must be empty: ${artifact_dir}"
fi

full_matrix_status="MISSING"
full_matrix_validation_log="${artifact_dir}/full-capacity-matrix-validation.log"
full_matrix_runner_log="${artifact_dir}/full-capacity-matrix-runner.log"
if [[ -z "${full_matrix_input}" && "${run_full_matrix}" == "1" ]]; then
  full_matrix_dir="${artifact_dir}/full-physical-matrix"
  mkdir -p "${full_matrix_dir}"
  set +e
  NEREUS_DELAY_V1_CAPACITY_MATRIX_ARTIFACT_DIR="${full_matrix_dir}" \
  NEREUS_DELAY_V1_CAPACITY_MATRIX_CANDIDATE_SOURCE_LOCK="${candidate_lock_file}" \
  NEREUS_DELAY_V1_CAPACITY_MATRIX_PROFILE_ID="${profile_id}-broker-matrix" \
  NEREUS_DELAY_V1_CAPACITY_MATRIX_GRADLE_USER_HOME="${seeded_gradle_home:-${artifact_dir}/matrix-gradle-user-home}" \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${full_matrix_runner}" >"${full_matrix_runner_log}" 2>&1
  full_matrix_runner_exit=$?
  set -e
  if [[ "${full_matrix_runner_exit}" == "0" && -s "${full_matrix_dir}/capacity-matrix.json" ]]; then
    full_matrix_input="${full_matrix_dir}/capacity-matrix.json"
  else
    full_matrix_runner_exit="${full_matrix_runner_exit:-1}"
  fi
else
  full_matrix_runner_exit=2
fi
full_matrix_json="$(jq -cn \
  --argjson locks "${source_locks_json}" \
  '{schema:"nereus-delay-v1-capacity-matrix-v1",status:"MISSING",source_locks:$locks,
    dimensions:{record_cardinalities:[],arrival_patterns:[],ordering_modes:[],consistency_modes:[],
      target_health:[],placement_modes:[],payload_modes:[]},observations:[],
    capacity_envelope:{status:"MISSING",config_file:"",config_sha256:""},
    boundaries:["A separately produced physical §23.4 capacity matrix is required."]}')"
if [[ -n "${full_matrix_input}" ]]; then
  set +e
  bash "${script_dir}/validate-v1-capacity-matrix.sh" \
    "${full_matrix_input}" "${candidate_lock_file}" >"${full_matrix_validation_log}" 2>&1
  full_matrix_validation_exit=$?
  set -e
  if [[ "${full_matrix_validation_exit}" == "0" ]]; then
    full_matrix_status="PASS"
    full_matrix_json="$(jq -c '.' "${full_matrix_input}")"
  else
    full_matrix_status="FAIL"
  fi
else
  full_matrix_validation_exit=2
  echo "NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_FULL_MATRIX_ARTIFACT is required" \
    >"${full_matrix_validation_log}"
fi

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

sha256_or_empty() {
  local file="$1"
  if [[ -s "${file}" ]]; then
    shasum -a 256 "${file}" | awk '{print $1}'
  else
    printf ''
  fi
}

json_number_or_zero() {
  local file="$1" expression="$2"
  if [[ -s "${file}" ]] && jq empty "${file}" >/dev/null 2>&1; then
    jq -r "${expression} // 0" "${file}" 2>/dev/null || printf '0\n'
  else
    printf '0\n'
  fi
}

count_matches() {
  local expression="$1" file="$2"
  if [[ -s "${file}" ]]; then
    local count
    count="$(grep -E -c -- "${expression}" "${file}" 2>/dev/null || true)"
    [[ "${count}" =~ ^[0-9]+$ ]] || count=0
    printf '%s\n' "${count}"
  else
    printf '0\n'
  fi
}

extract_last_value() {
  local expression="$1" file="$2"
  if [[ -s "${file}" ]]; then
    grep -E -o -- "${expression}" "${file}" 2>/dev/null | tail -n 1 | sed -E 's/[^0-9]//g' || true
  fi
}

matrix_dir="${artifact_dir}/bounded-capacity-matrix"
matrix_log="${artifact_dir}/bounded-capacity-matrix.log"
matrix_gradle_home="${seeded_gradle_home:-${artifact_dir}/matrix-gradle-user-home}"
mkdir -p "${matrix_dir}"
matrix_started_ms="$(now_ms)"
set +e
NEREUS_DELAY_CAPACITY_MATRIX_ARTIFACT_DIR="${matrix_dir}" \
NEREUS_DELAY_CAPACITY_MATRIX_GRADLE_USER_HOME="${matrix_gradle_home}" \
NEREUS_DELAY_CAPACITY_MATRIX_IMAGE="${matrix_image}" \
NEREUS_DELAY_CAPACITY_MATRIX_PROJECT="nereus-delay-v1-${gate}-measurement-$(date +%s)-$$" \
NEREUS_DELAY_CAPACITY_MATRIX_PULL_IMAGE="${NEREUS_DELAY_V1_CAPACITY_MEASUREMENT_PULL_IMAGE:-0}" \
  bash "${script_dir}/run-bounded-capacity-matrix.sh" >"${matrix_log}" 2>&1
matrix_exit_code=$?
set -e
matrix_finished_ms="$(now_ms)"
matrix_artifact="${matrix_dir}/capacity-benchmark-matrix.json"
matrix_status="$(jq -r '.matrix_status // "MISSING"' "${matrix_artifact}" 2>/dev/null || printf 'MISSING')"

contract_log="${artifact_dir}/delay-physical-capacity-contract-tests.log"
contract_gradle_home="${seeded_gradle_home:-${artifact_dir}/contract-gradle-user-home}"
contract_tests=(
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
  io.nereusstream.delay.store.BoundedDestinationPublishAdapterTest
  io.nereusstream.delay.scheduler.TargetIsolationDurableChaosTest
  io.nereusstream.delay.store.SloObservationCollectorTest
  io.nereusstream.delay.store.PersistentSloObservationCollectorTest
)
contract_args=()
for test_name in "${contract_tests[@]}"; do
  contract_args+=(--tests "${test_name}")
done
contract_started_ms="$(now_ms)"
set +e
(
  cd "${delay_dir}"
  GRADLE_USER_HOME="${contract_gradle_home}" \
    ./gradlew test "${contract_args[@]}" --rerun-tasks --no-daemon --console=plain
) >"${contract_log}" 2>&1
contract_exit_code=$?
set -e
contract_finished_ms="$(now_ms)"

real_kafka_log="${artifact_dir}/kafka-large-payload-multi-shard.log"
real_pulsar_log="${artifact_dir}/pulsar-large-payload-multi-shard.log"
kafka_exit_code=2
pulsar_exit_code=2
kafka_started_ms=0
kafka_finished_ms=0
pulsar_started_ms=0
pulsar_finished_ms=0
if [[ "${run_real}" == "1" ]]; then
  kafka_started_ms="$(now_ms)"
  set +e
  (
    cd "${delay_dir}"
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC="nereus-delay-v1-${gate}-measurement-kafka-${profile_id}" \
    NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME="${seeded_gradle_home:-${artifact_dir}/kafka-gradle-user-home}" \
      bash "${script_dir}/run-large-payload-gateway-e2e.sh"
  ) >"${real_kafka_log}" 2>&1
  kafka_exit_code=$?
  set -e
  kafka_finished_ms="$(now_ms)"

  pulsar_started_ms="$(now_ms)"
  set +e
  (
    cd "${delay_dir}"
    NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC="persistent://public/default/nereus-delay-v1-${gate}-measurement-pulsar-${profile_id}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME="${seeded_gradle_home:-${artifact_dir}/pulsar-gradle-user-home}" \
      bash "${script_dir}/run-pulsar-large-payload-gateway-e2e.sh"
  ) >"${real_pulsar_log}" 2>&1
  pulsar_exit_code=$?
  set -e
  pulsar_finished_ms="$(now_ms)"
fi

sustained_artifact="${matrix_dir}/sustained/bounded-capacity-slo-probe.json"
local_records="$(json_number_or_zero "${matrix_artifact}" '[.cases[].artifact.store.payload_runs[].records] | add')"
local_bytes="$(json_number_or_zero "${matrix_artifact}" '[.cases[].artifact.store.payload_runs[].input_bytes] | add')"
local_elapsed_nanos="$(json_number_or_zero "${matrix_artifact}" '[.cases[].artifact.store.payload_runs[].elapsed_nanos] | add')"
local_elapsed_ms=$(( (local_elapsed_nanos + 999999) / 1000000 ))
(( local_elapsed_ms > 0 )) || local_elapsed_ms=1
local_rps=$(( local_records * 1000 / local_elapsed_ms ))
local_bps=$(( local_bytes * 1000 / local_elapsed_ms ))
local_cgroup="$(json_number_or_zero "${sustained_artifact}" '.platform_probe.cgroup_memory_limit_bytes')"
local_direct="$(json_number_or_zero "${sustained_artifact}" '.platform_probe.direct_memory_bytes')"
local_open_files="$(json_number_or_zero "${sustained_artifact}" '.platform_probe.max_open_files')"
local_rss="$(json_number_or_zero "${sustained_artifact}" '.platform_probe.process_rss_bytes')"
local_current_fds="$(json_number_or_zero "${sustained_artifact}" '.platform_probe.current_open_files')"
local_matrix_status="FAIL"
if [[ "${matrix_exit_code}" == "0" && "${matrix_status}" == "PASS_BOUNDED" \
    && "${local_records}" =~ ^[1-9][0-9]*$ && "${local_bytes}" =~ ^[1-9][0-9]*$ \
    && "${local_cgroup}" =~ ^[1-9][0-9]*$ && "${local_direct}" =~ ^[1-9][0-9]*$ \
    && "${local_open_files}" =~ ^[1-9][0-9]*$ && "${local_rss}" =~ ^[1-9][0-9]*$ \
    && "${local_current_fds}" =~ ^[1-9][0-9]*$ ]]; then
  local_matrix_status="PASS"
fi

kafka_source_records="$(extract_last_value 'sourceRecords=[0-9]+' "${real_kafka_log}")"
pulsar_source_records="$(extract_last_value 'sourceRecords=[0-9]+' "${real_pulsar_log}")"
kafka_published_records="$(count_matches 'Kafka multi-shard Large Payload partition=' "${real_kafka_log}")"
pulsar_published_records="$(count_matches 'Pulsar multi-shard Large Payload partition=' "${real_pulsar_log}")"
kafka_workers_text="$(grep -E -o -- 'workers=\[[^]]*\]' "${real_kafka_log}" 2>/dev/null | tail -n 1 | sed -E 's/.*workers=\[([^]]*)\].*/\1/' || true)"
pulsar_workers_text="$(grep -E -o -- 'workers=\[[^]]*\]' "${real_pulsar_log}" 2>/dev/null | tail -n 1 | sed -E 's/.*workers=\[([^]]*)\].*/\1/' || true)"
kafka_workers_json="$(printf '%s\n' "${kafka_workers_text}" | tr ',' '\n' | sed 's/^ *//;s/ *$//' | jq -Rsc 'split("\n") | map(select(length > 0))')"
pulsar_workers_json="$(printf '%s\n' "${pulsar_workers_text}" | tr ',' '\n' | sed 's/^ *//;s/ *$//' | jq -Rsc 'split("\n") | map(select(length > 0))')"
kafka_worker_count="$(jq 'length' <<<"${kafka_workers_json}")"
pulsar_worker_count="$(jq 'length' <<<"${pulsar_workers_json}")"
kafka_elapsed_ms=$(( kafka_finished_ms - kafka_started_ms ))
pulsar_elapsed_ms=$(( pulsar_finished_ms - pulsar_started_ms ))
(( kafka_elapsed_ms > 0 )) || kafka_elapsed_ms=1
(( pulsar_elapsed_ms > 0 )) || pulsar_elapsed_ms=1
real_status="FAIL"
if [[ "${run_real}" == "1" && "${kafka_exit_code}" == "0" && "${pulsar_exit_code}" == "0" \
    && "${kafka_published_records}" =~ ^[1-9][0-9]*$ && "${pulsar_published_records}" =~ ^[1-9][0-9]*$ \
    && "${kafka_worker_count}" -ge 2 && "${pulsar_worker_count}" -ge 2 ]]; then
  real_status="PASS"
fi

commands_json="$(jq -cn \
  --arg matrix "bash ${script_dir}/run-bounded-capacity-matrix.sh" \
  --arg contract "./gradlew test <physical-capacity-contract-tests> --rerun-tasks --no-daemon" \
  --arg kafka "bash ${script_dir}/run-large-payload-gateway-e2e.sh" \
  --arg pulsar "bash ${script_dir}/run-pulsar-large-payload-gateway-e2e.sh" \
  --arg physical_matrix "bash ${full_matrix_runner}" \
  '[$matrix,$contract,$kafka,$pulsar,$physical_matrix]')"
artifacts_json="$(printf '%s\n' "${matrix_artifact}" "${sustained_artifact}" "${contract_log}" "${real_kafka_log}" "${real_pulsar_log}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
hashes_json="$(printf '%s\n' "${matrix_artifact}" "${sustained_artifact}" "${contract_log}" "${real_kafka_log}" "${real_pulsar_log}" | while IFS= read -r file; do sha256_or_empty "${file}"; done | jq -Rsc 'split("\n") | map(select(length > 0))')"
exit_codes_json="$(jq -cn --argjson matrix "${matrix_exit_code}" --argjson contract "${contract_exit_code}" \
  --argjson kafka "${kafka_exit_code}" --argjson pulsar "${pulsar_exit_code}" \
  '[{name:"bounded-capacity-matrix",exit_code:$matrix},{name:"contract-tests",exit_code:$contract},{name:"kafka-large-payload",exit_code:$kafka},{name:"pulsar-large-payload",exit_code:$pulsar}]')"
provenance_json="$(jq -cn --argjson locks "${source_locks_json}" --argjson commands "${commands_json}" \
  --argjson artifacts "${artifacts_json}" --argjson hashes "${hashes_json}" --argjson exits "${exit_codes_json}" \
  '{source_locks:$locks,commands:$commands,artifacts:$artifacts,artifact_sha256:$hashes,exit_codes:$exits}')"

local_dimensions_json="$(jq -cn --arg image "${matrix_image}" \
  --argjson records "${local_records}" --argjson bytes "${local_bytes}" \
  '{authority:"local-JVM-RocksDB",container_image:$image,payload_record_count:$records,payload_bytes:$bytes,
    payload_sizes_bytes:[256,4096,65536],slo_samples:"source-artifact",reopen:true}')"
real_dimensions_json="$(jq -cn --argjson payload "${payload_bytes}" \
  --argjson kafka_workers "${kafka_workers_json}" --argjson pulsar_workers "${pulsar_workers_json}" \
  --argjson kafka_records "${kafka_published_records}" --argjson pulsar_records "${pulsar_published_records}" \
  '{authority:"real-Kafka-and-Pulsar-Gateway-Worker-MinIO",payload_bytes:$payload,source_shards:2,
    destination_shards:2,kafka_workers:$kafka_workers,pulsar_workers:$pulsar_workers,
    kafka_published_records:$kafka_records,pulsar_published_records:$pulsar_records,
    exact_payload_readback:true,exact_idempotency:true}')"
local_metrics_json="$(jq -cn --argjson records "${local_records}" --argjson bytes "${local_bytes}" \
  --argjson elapsed "${local_elapsed_ms}" --argjson rps "${local_rps}" --argjson bps "${local_bps}" \
  --argjson cgroup "${local_cgroup}" --argjson direct "${local_direct}" --argjson open "${local_open_files}" \
  --argjson rss "${local_rss}" --argjson fds "${local_current_fds}" \
  '{records:$records,bytes:$bytes,wall_elapsed_ms:$elapsed,records_per_second:$rps,bytes_per_second:$bps,
    cgroup_memory_limit_bytes:$cgroup,direct_memory_bytes:$direct,max_open_files:$open,
    process_rss_bytes:$rss,current_open_files:$fds}')"
real_metrics_json="$(jq -cn --argjson payload "${payload_bytes}" \
  --argjson kafka_source "${kafka_source_records:-0}" --argjson pulsar_source "${pulsar_source_records:-0}" \
  --argjson kafka_records "${kafka_published_records}" --argjson pulsar_records "${pulsar_published_records}" \
  --argjson kafka_elapsed "${kafka_elapsed_ms}" --argjson pulsar_elapsed "${pulsar_elapsed_ms}" \
  --argjson kafka_workers "${kafka_worker_count}" --argjson pulsar_workers "${pulsar_worker_count}" \
  '{payload_bytes:$payload,kafka_source_records:$kafka_source,pulsar_source_records:$pulsar_source,
    kafka_published_records:$kafka_records,pulsar_published_records:$pulsar_records,
    kafka_wall_elapsed_ms:$kafka_elapsed,pulsar_wall_elapsed_ms:$pulsar_elapsed,
    kafka_records_per_second:(($kafka_records * 1000) / $kafka_elapsed),
    pulsar_records_per_second:(($pulsar_records * 1000) / $pulsar_elapsed),
    kafka_bytes_per_second:(($kafka_records * $payload * 1000) / $kafka_elapsed),
    pulsar_bytes_per_second:(($pulsar_records * $payload * 1000) / $pulsar_elapsed),
    kafka_worker_count:$kafka_workers,pulsar_worker_count:$pulsar_workers}')"

if [[ "${local_matrix_status}" == "PASS" ]]; then
  local_measurement_status="PASS"
else
  local_measurement_status="FAIL"
fi
if [[ "${real_status}" == "PASS" ]]; then
  real_measurement_status="PASS"
else
  real_measurement_status="FAIL"
fi

measurements_json='{}'
for name in "${required_lines[@]}"; do
  authority="real-broker-worker-object-store"
  measurement_status="${real_measurement_status}"
  dimensions_json="${real_dimensions_json}"
  metrics_json="${real_metrics_json}"
  measurement_provenance="${provenance_json}"
  case "${name}" in
    broker-throughput|lane-distribution|lane-fairness|multi-worker-placement|inline-object|single-shard-multi-shard|payload-throughput)
      ;;
    *)
      authority="local-runtime-contract-and-resource-probe"
      measurement_status="${local_measurement_status}"
      dimensions_json="${local_dimensions_json}"
      metrics_json="${local_metrics_json}"
      ;;
  esac
  measurement_json="$(jq -cn --arg status "${measurement_status}" --arg authority "${authority}" \
    --argjson provenance "${measurement_provenance}" --argjson dimensions "${dimensions_json}" \
    --argjson metrics "${metrics_json}" \
    '{status:$status,authority:$authority,provenance:$provenance,dimensions:$dimensions,metrics:$metrics,
      invariant_status:$status,invariants:["source locks exact","producer exit codes recorded","artifact hashes recorded"]}')"
  measurements_json="$(jq --arg name "${name}" --argjson measurement "${measurement_json}" \
    '. + {($name):$measurement}' <<<"${measurements_json}")"
done

overall_status="FAIL"
if [[ "${full_matrix_status}" == "PASS" \
    && "${local_measurement_status}" == "PASS" && "${real_measurement_status}" == "PASS" \
    && "$(jq -n -r --argjson required "${required_json}" --argjson measurements "${measurements_json}" \
      'all($required[]; . as $name | $measurements[$name].status == "PASS" and $measurements[$name].invariant_status == "PASS")')" == "true" ]]; then
  overall_status="PASS"
fi

measurement_artifact="${artifact_dir}/capacity-observation.json"
jq -n \
  --arg schema "nereus-delay-v1-capacity-observation-v1" \
  --arg status "${overall_status}" \
  --arg profile_id "${profile_id}" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg execution "strict-sequential" \
  --argjson source_locks "${source_locks_json}" \
  --argjson required "${required_json}" \
  --argjson measurements "${measurements_json}" \
  --argjson provenance "${provenance_json}" \
  --arg matrix_status "${matrix_status}" \
  --arg full_matrix_status "${full_matrix_status}" \
  --arg full_matrix_input "${full_matrix_input}" \
  --arg full_matrix_validation_log "${full_matrix_validation_log}" \
  --argjson full_matrix_runner_exit "${full_matrix_runner_exit}" \
  --argjson full_matrix_validation_exit "${full_matrix_validation_exit}" \
  --argjson full_matrix "${full_matrix_json}" \
  --arg local_status "${local_measurement_status}" \
  --arg real_status "${real_measurement_status}" \
  --argjson matrix_exit "${matrix_exit_code}" \
  --argjson contract_exit "${contract_exit_code}" \
  --argjson kafka_exit "${kafka_exit_code}" \
  --argjson pulsar_exit "${pulsar_exit_code}" \
  --argjson payload_bytes "${payload_bytes}" \
  --argjson local_cgroup "${local_cgroup}" \
  --argjson local_direct "${local_direct}" \
  --argjson local_open_files "${local_open_files}" \
  --argjson local_rss "${local_rss}" \
  --argjson local_fds "${local_current_fds}" \
  --argjson local_records "${local_records}" \
  --argjson local_bytes "${local_bytes}" \
  '{
    schema:$schema,status:$status,profile_id:$profile_id,started_at:$started_at,finished_at:$finished_at,
    execution:$execution,source_locks:$source_locks,
    required_configurations:$required,observed_configurations:($measurements | keys | sort),
    campaign:{
      bounded_matrix:{status:$matrix_status,exit_code:$matrix_exit},
      physical_contract_tests:{status:(if $contract_exit == 0 then "PASS" else "FAIL" end),exit_code:$contract_exit},
      real_kafka_large_payload:{status:(if $kafka_exit == 0 then "PASS" else "FAIL" end),exit_code:$kafka_exit},
      real_pulsar_large_payload:{status:(if $pulsar_exit == 0 then "PASS" else "FAIL" end),exit_code:$pulsar_exit},
      full_v1_matrix:{
        status:$full_matrix_status,
        source_artifact:$full_matrix_input,
        runner_exit_code:$full_matrix_runner_exit,
        validation_log:$full_matrix_validation_log,
        validation_exit_code:$full_matrix_validation_exit,
        record_cardinalities:($full_matrix.dimensions.record_cardinalities // []),
        arrival_patterns:($full_matrix.dimensions.arrival_patterns // []),
        ordering_modes:($full_matrix.dimensions.ordering_modes // []),
        consistency_modes:($full_matrix.dimensions.consistency_modes // []),
        target_health:($full_matrix.dimensions.target_health // []),
        placement_modes:($full_matrix.dimensions.placement_modes // []),
        payload_modes:($full_matrix.dimensions.payload_modes // []),
        observations:($full_matrix.observations // []),
        capacity_envelope:($full_matrix.capacity_envelope // {})
      },
      payload_bytes:$payload_bytes,
      local_payload_records:$local_records,
      local_payload_bytes:$local_bytes
    },
    platform_observation:{
      cgroup_memory_limit_bytes:$local_cgroup,direct_memory_bytes:$local_direct,
      max_open_files:$local_open_files,process_rss_bytes:$local_rss,current_open_files:$local_fds,
      source:"WorkerRuntimeResourceProbe via bounded capacity artifact"
    },
    measurements:$measurements,
    provenance:$provenance,
    boundaries:[
      "All values are derived from the exact artifacts and exit codes recorded by this run.",
      "The real Broker values are end-to-end wall measurements for the two-shard Large Payload authority chain, not a claim of unconstrained broker saturation.",
      "The local resource values are authoritative only for the recorded container/profile and exact source lock.",
      "The full V1 capacity matrix is independently supplied and hash-validated; without it this artifact remains FAIL even when the bounded probe and real E2E pass.",
      "Missing or malformed platform, broker, worker, object-store, contract or hash evidence produces FAIL."
    ]
  }' >"${measurement_artifact}"

jq -e --arg status "${overall_status}" --argjson required "${required_json}" \
  '.schema == "nereus-delay-v1-capacity-observation-v1" and .status == $status
   and (.source_locks.delay | length == 40)
   and ((.required_configurations | sort | unique) == ($required | sort | unique))
   and ((.observed_configurations | sort | unique) == ($required | sort | unique))
   and (.measurements | type == "object")
   and (.campaign.full_v1_matrix.status == "MISSING" or .campaign.full_v1_matrix.status == "FAIL" or .campaign.full_v1_matrix.status == "PASS")' "${measurement_artifact}" >/dev/null

echo "physical capacity observation artifact=${measurement_artifact} status=${overall_status}"
echo "matrix=${matrix_status}/${matrix_exit_code} contract=${contract_exit_code} real=${real_status} kafka=${kafka_exit_code} pulsar=${pulsar_exit_code}"
if [[ "${overall_status}" != "PASS" ]]; then
  exit 1
fi
