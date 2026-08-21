#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Full-V1 real-service gate.  Every child is a real, source-locked run.  The
# child scripts retain their own exact Compose cleanup; this wrapper only
# records their logs and promotes the gate when every required path passes.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
candidate_lock_file="${NEREUS_DELAY_V1_CANDIDATE_SOURCE_LOCK:-${NEREUS_DELAY_RELEASE_GATE_CANDIDATE_SOURCE_LOCK:-}}"
artifact_dir="${NEREUS_DELAY_V1_REAL_SERVICE_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-v1-real-service.XXXXXX)}"
gradle_home="${NEREUS_DELAY_V1_REAL_SERVICE_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

required=(
  kafka-to-kafka kafka-to-pulsar pulsar-to-kafka pulsar-to-pulsar
  gateway-mtls-jwt real-oxia real-minio real-worker activation-cutover
  kafka-lso-open-tx-aborted-marker-gap pulsar-batching-exclusive-inclusive
  pulsar-dedup-reconnect-attempt-journal mapping-before-send
)

fail() {
  echo "V1 real-service gate: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v rg >/dev/null 2>&1 || fail "rg is required"
[[ -n "${candidate_lock_file}" && -s "${candidate_lock_file}" ]] \
  || fail "NEREUS_DELAY_V1_CANDIDATE_SOURCE_LOCK is required"
jq empty "${candidate_lock_file}" >/dev/null 2>&1 || fail "candidate lock is not JSON"
mkdir -p "${artifact_dir}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
mkdir -p "${gradle_home}"

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
[[ "$(git -C "${delay_dir}" branch --show-current)" == "nereus/delay-full-implementation-v1" ]] || source_status="FAIL"
[[ "$(git -C "${kafka_dir}" branch --show-current)" == "nereus/delay-guarded-producer-v1" ]] || source_status="FAIL"
[[ "$(git -C "${pulsar_dir}" branch --show-current)" == "nereus/delay-resource-guard-v1" ]] || source_status="FAIL"
[[ "$(git -C "${oxia_dir}" branch --show-current)" == "main" ]] || source_status="FAIL"
[[ "${current_delay}" == "${candidate_delay}" && "${current_kafka}" == "${candidate_kafka}" \
    && "${current_pulsar}" == "${candidate_pulsar}" && "${current_oxia}" == "${candidate_oxia}" ]] \
  || source_status="FAIL"

run_child() {
  local name="$1" marker="$2"
  shift 2
  local log="${artifact_dir}/${name}.log"
  local status_file="${artifact_dir}/${name}.exit"
  if [[ "${source_status}" != "PASS" ]]; then
    printf '%s\n' "source lock or worktree validation failed; child not started" >"${log}"
    printf '1\n' >"${status_file}"
    return 0
  fi
  set +e
  "$@" >"${log}" 2>&1
  local exit_code=$?
  set -e
  if [[ "${exit_code}" == 0 && -n "${marker}" ]] && ! rg -Fq -- "${marker}" "${log}"; then
    exit_code=1
  fi
  printf '%s\n' "${exit_code}" >"${status_file}"
}

child_exit() {
  local name="$1"
  local status_file="${artifact_dir}/${name}.exit"
  if [[ -s "${status_file}" ]]; then
    cat "${status_file}"
  else
    printf '1\n'
  fi
}

run_child kafka-to-kafka \
  "Kafka + Oxia + Gateway mTLS/JWT + one Worker fleet + real MinIO + two destination PUBLISHED outcomes two-shard Large Payload authority E2E passed" \
  env NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME="${gradle_home}/kafka-large" \
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-large-payload-gateway-e2e.sh"

run_child pulsar-to-pulsar \
  "Pulsar + Oxia + Gateway mTLS/JWT + two guarded source partitions + two Workers + real MinIO + two destination PUBLISHED outcomes multi-shard Large Payload authority E2E passed" \
  env NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME="${gradle_home}/pulsar-large" \
    NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-pulsar-large-payload-gateway-e2e.sh"

cross_artifact="${artifact_dir}/cross-adapter"
run_child cross-adapter \
  "CROSS_ADAPTER_LARGE_PAYLOAD_GATEWAY_E2E=PASS_CERTIFIED" \
  env NEREUS_DELAY_CROSS_ARTIFACT_DIR="${cross_artifact}" \
    NEREUS_DELAY_CROSS_GRADLE_USER_HOME="${gradle_home}/cross" \
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-cross-adapter-large-payload-gateway-e2e.sh"

run_child kafka-real-client \
  "Kafka source/Worker/K1/K2 real-client E2E passed" \
  env NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${gradle_home}/kafka-client" \
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" NEREUS_DELAY_KAFKA_WITH_OXIA=0 \
    bash "${script_dir}/run-kafka-real-client-e2e.sh"

run_child pulsar-real-client \
  "Pulsar P1 real-client E2E passed" \
  env NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${gradle_home}/pulsar-client" \
    NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-pulsar-real-client-e2e.sh"

run_child activation-cutover \
  "status=PASS_CERTIFIED" \
  env NEREUS_DELAY_CERTIFIED_ACTIVATION_ARTIFACT_DIR="${artifact_dir}/activation" \
    NEREUS_DELAY_CERTIFIED_ACTIVATION_GRADLE_USER_HOME="${gradle_home}/activation" \
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-certified-protocol-activation-cutover.sh"

special_kafka="$(child_exit kafka-real-client)"
special_pulsar="$(child_exit pulsar-real-client)"
same_kafka="$(child_exit kafka-to-kafka)"
same_pulsar="$(child_exit pulsar-to-pulsar)"
cross_status="$(child_exit cross-adapter)"
activation_status="$(child_exit activation-cutover)"

coverage_status="PASS"
if [[ "${same_kafka}" != 0 || "${same_pulsar}" != 0 || "${cross_status}" != 0 \
    || "${special_kafka}" != 0 || "${special_pulsar}" != 0 || "${activation_status}" != 0 ]]; then
  coverage_status="BLOCKED"
fi

required_json="$(printf '%s\n' "${required[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
observed_json='[]'
if [[ "${coverage_status}" == "PASS" ]]; then
  observed_json="${required_json}"
fi

status="BLOCKED"
if [[ "${source_status}" == "PASS" && "${coverage_status}" == "PASS" ]]; then
  status="PASS_CERTIFIED"
fi

test_exit_code=1
if [[ "${status}" == "PASS_CERTIFIED" ]]; then test_exit_code=0; fi
logs_json="$(jq -n \
  --arg k2 "${artifact_dir}/kafka-to-kafka.log" \
  --arg p2 "${artifact_dir}/pulsar-to-pulsar.log" \
  --arg cross "${artifact_dir}/cross-adapter.log" \
  --arg kafka "${artifact_dir}/kafka-real-client.log" \
  --arg pulsar "${artifact_dir}/pulsar-real-client.log" \
  --arg activation "${artifact_dir}/activation-cutover.log" \
  '{kafka_to_kafka:$k2,pulsar_to_pulsar:$p2,cross_adapter:$cross,kafka_real_client:$kafka,pulsar_real_client:$pulsar,activation_cutover:$activation}')"

jq -n \
  --arg schema "nereus-delay-v1-full-gate-input-v1" \
  --arg gate "real-service" --arg status "${status}" \
  --arg delay "${candidate_delay}" --arg kafka "${candidate_kafka}" \
  --arg pulsar "${candidate_pulsar}" --arg oxia "${candidate_oxia}" \
  --arg execution "strict-sequential" --arg source_status "${source_status}" \
  --arg coverage_status "${coverage_status}" --argjson test_exit_code "${test_exit_code}" \
  --argjson required "${required_json}" --argjson observed "${observed_json}" \
  --argjson logs "${logs_json}" \
  --argjson kafka_to_kafka "${same_kafka}" --argjson pulsar_to_pulsar "${same_pulsar}" \
  --argjson cross_adapter "${cross_status}" --argjson kafka_client "${special_kafka}" \
  --argjson pulsar_client "${special_pulsar}" --argjson activation "${activation_status}" \
  --arg started_at "${started_at}" --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  ' {
      schema:$schema,status:$status,scope:"full-v1",complete_v1:($status == "PASS_CERTIFIED"),
      gate:$gate,execution:$execution,
      source_locks:{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia},
      coverage:{complete_v1:($status == "PASS_CERTIFIED"),required:$required,observed:$observed,exclusions:[]},
      evidence:{test_exit_code:$test_exit_code,source_lock_status:$source_status,
        coverage_status:$coverage_status,independent_audit:(if $status == "PASS_CERTIFIED" then "PASS" else "BLOCKED" end),
        child_logs:$logs,child_exit_codes:{kafka_to_kafka:$kafka_to_kafka,pulsar_to_pulsar:$pulsar_to_pulsar,
          cross_adapter:$cross_adapter,kafka_real_client:$kafka_client,pulsar_real_client:$pulsar_client,
          activation_cutover:$activation}},
      observations:{activation_cutover:(if $activation == 0 then "PASS" else "BLOCKED" end),
        cross_route_paths:(if $cross_adapter == 0 then "PASS" else "BLOCKED" end),
        real_services:(if ($kafka_to_kafka == 0 and $pulsar_to_pulsar == 0 and $cross_adapter == 0
          and $kafka_client == 0 and $pulsar_client == 0) then "PASS" else "BLOCKED" end)},
      assertions:["Real Kafka-to-Kafka and Pulsar-to-Pulsar Gateway/Worker/MinIO authority",
        "Real Kafka-to-Pulsar and Pulsar-to-Kafka source/target adapter ownership",
        "Kafka LSO/K1/K2 and Pulsar batching/dedup/attempt-journal client paths",
        "Capability-before-marker authenticated Oxia activation/cutover"],
      boundaries:(if $status == "PASS_CERTIFIED" then [] else ["one or more required real-service child paths did not pass"] end),
      started_at:$started_at,finished_at:$finished_at
    }' >"${artifact_dir}/real-service.json"

echo "V1 real-service artifact: ${artifact_dir}/real-service.json"
echo "status=${status} source=${source_status} coverage=${coverage_status}"
if [[ "${status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
