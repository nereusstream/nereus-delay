#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-bounded-chaos.XXXXXX)}"
mkdir -p "${artifact_dir}"
matrix_gradle_home="${NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
mkdir -p "${matrix_gradle_home}"

if ! command -v jq >/dev/null 2>&1; then
  echo "bounded chaos matrix requires jq to write the canonical JSON receipt" >&2
  exit 1
fi

delay_base="2dfc3289ffdbe9cf9d7f4d0de1d701493d1b49a6"
kafka_base="c300006a7705c240642db6950b5a95fec982bfc5"
pulsar_base="8dae0236c0a0d405ed7f8303081080520fe91551"
matrix_status=0
declare -a cell_results=()

require_checkout() {
  local path="$1"
  local branch="$2"
  local base="$3"
  local label="$4"
  test -d "${path}"
  test -z "$(git -C "${path}" status --porcelain)"
  test "$(git -C "${path}" branch --show-current)" = "${branch}"
  git -C "${path}" merge-base --is-ancestor "${base}" HEAD
  echo "${label} source: $(git -C "${path}" rev-parse HEAD)"
  echo "${label} branch: ${branch} (base ${base})"
}

require_checkout "${delay_dir}" "nereus/delay-full-implementation-v1" "${delay_base}" "Delay"
require_checkout "${kafka_dir}" "nereus/delay-guarded-producer-v1" "${kafka_base}" "Kafka"
require_checkout "${pulsar_dir}" "nereus/delay-resource-guard-v1" "${pulsar_base}" "Pulsar"
require_checkout "${oxia_dir}" "main" "$(git -C "${oxia_dir}" rev-parse HEAD)" "Oxia"

delay_source="$(git -C "${delay_dir}" rev-parse HEAD)"
kafka_source="$(git -C "${kafka_dir}" rev-parse HEAD)"
pulsar_source="$(git -C "${pulsar_dir}" rev-parse HEAD)"
oxia_source="$(git -C "${oxia_dir}" rev-parse HEAD)"

echo "Bounded chaos matrix artifact directory: ${artifact_dir}"
echo "Matrix Gradle cache: ${matrix_gradle_home}"
echo "Matrix scope: current-source focused cuts only; this is not a V1 release gate by itself."
echo "Matrix audit: every cell must emit its deterministic injection and recovery evidence markers; durable state dumps and independent invariant audits remain explicit release requirements."

audit_cell() {
  local name="$1"
  local log="$2"
  local result_status="$3"
  local injection_point
  local expected_state
  local duplicate_boundary
  local fresh_process_recovery
  local authority_evidence
  local target_evidence
  local source_evidence
  local durable_state_dump_status="NOT_CAPTURED"
  local invariant_audit_status="MARKER_ONLY"
  local -a required_markers=()

  case "${name}" in
    kafka-broker-process-crash)
      injection_point="SIGKILL kafka-1 after guarded Worker preparation"
      expected_state="survivor Brokers resume the same source and the crashed Broker rejoins"
      duplicate_boundary="one source-applied physical publish and one typed receipt; replay must not duplicate the outcome"
      fresh_process_recovery="PASS"
      source_evidence="Kafka Broker process-crash recovery E2E passed"
      target_evidence="Kafka Worker source-applied physical publish passed"
      authority_evidence="Kafka Worker authority smoke passed"
      required_markers=("Kafka Broker process-crash recovery E2E passed" "Kafka Worker source-applied physical publish passed" "Kafka Worker authority smoke passed")
      ;;
    kafka-worker-ack-process-crash)
      injection_point="SIGKILL Worker after durable Store WriteBatch and before Kafka commitSync ACK"
      expected_state="fresh Worker replays the unACKed source record, dedupes the durable apply and commits the final checkpoint"
      duplicate_boundary="durable apply and source ACK are each idempotent; no second physical outcome"
      fresh_process_recovery="PASS"
      source_evidence="Kafka Worker ACK process-crash recovery E2E passed"
      target_evidence="Kafka Worker vertical smoke passed"
      authority_evidence="Kafka Worker authority smoke passed"
      required_markers=("Kafka Worker ACK process-crash recovery E2E passed" "Kafka Worker vertical smoke passed" "Kafka Worker authority smoke passed")
      ;;
    kafka-broker-tcp-cut)
      injection_point="one-shot raw TCP rejection of Broker-1 endpoint"
      expected_state="source and group coordinator placement remains on Broker-2 and a fresh Worker resumes through the bootstrap list"
      duplicate_boundary="the same source position is applied once after endpoint cut; no replacement request changes the guarded identity"
      fresh_process_recovery="PASS"
      source_evidence="Kafka Worker raw TCP Broker-endpoint cut recovery E2E passed"
      target_evidence="Kafka Worker vertical smoke passed"
      authority_evidence="Kafka Worker authority smoke passed"
      required_markers=("Kafka Worker raw TCP Broker-endpoint cut recovery E2E passed" "Kafka Worker vertical smoke passed" "Kafka Worker authority smoke passed")
      ;;
    kafka-broker-network-partition)
      injection_point="remove kafka-1 from the exact Compose network while keeping the process alive"
      expected_state="survivor leaders continue source/target progress and kafka-1 reconnects without changing the source identity"
      duplicate_boundary="partition recovery cannot create a second source-applied outcome"
      fresh_process_recovery="PASS"
      source_evidence="Kafka Broker network-partition recovery E2E passed"
      target_evidence="Kafka Worker vertical smoke passed"
      authority_evidence="Kafka Worker authority smoke passed"
      required_markers=("Kafka Broker network-partition recovery E2E passed" "Kafka Worker vertical smoke passed" "Kafka Worker authority smoke passed")
      ;;
    pulsar-worker-process-crash)
      injection_point="SIGKILL the Worker after guarded source open and before source ACK"
      expected_state="fresh Worker reopens the exact Store, reacquires Oxia ownership and ACKs the unACKed source record"
      duplicate_boundary="source replay preserves the same guarded record and produces one final checkpoint"
      fresh_process_recovery="PASS"
      source_evidence="Pulsar Worker process-crash recovery E2E passed"
      target_evidence="Pulsar Worker vertical smoke passed"
      authority_evidence="Pulsar Worker authority smoke passed"
      required_markers=("Pulsar Worker process-crash recovery E2E passed" "Pulsar Worker vertical smoke passed" "Pulsar Worker authority smoke passed")
      ;;
    pulsar-multi-broker-process-crash)
      injection_point="SIGKILL Pulsar broker-1 after guarded Worker preparation"
      expected_state="broker-2 serves the same topic and broker-1 rejoins after the Worker completes recovery"
      duplicate_boundary="broker failover preserves the source position and typed destination receipt"
      fresh_process_recovery="PASS"
      source_evidence="Pulsar Broker process-crash failover E2E passed"
      target_evidence="Pulsar Worker source-applied physical publish passed"
      authority_evidence="Pulsar Worker authority smoke passed"
      required_markers=("Pulsar Broker process-crash failover E2E passed" "Pulsar Worker source-applied physical publish passed" "Pulsar Worker authority smoke passed")
      ;;
    pulsar-worker-admission-response-loss)
      injection_point="discard the Pulsar Shard Log Publish Admission append response after durable mutation"
      expected_state="exact source replay recovers PUBLISHING and resolves one typed destination outcome"
      duplicate_boundary="response loss cannot append a second admission or physical destination publish"
      fresh_process_recovery="NOT_COVERED"
      source_evidence="Pulsar Worker Publish Admission response-loss E2E passed"
      target_evidence="Pulsar Worker source-applied physical publish passed"
      authority_evidence="Pulsar Worker authority smoke passed"
      required_markers=("Pulsar Worker Publish Admission response-loss E2E passed" "Pulsar Worker source-applied physical publish passed" "Pulsar Worker authority smoke passed")
      ;;
    checkpoint-reaping)
      injection_point="checkpoint upload/catalog/reaping competition against real Oxia and MinIO"
      expected_state="only the authorized checkpoint candidate is published/reaped and immutable object identity is preserved"
      duplicate_boundary="late provider work cannot delete or activate a fenced checkpoint"
      fresh_process_recovery="NOT_COVERED"
      source_evidence="Oxia + MinIO Worker checkpoint publication and REAPING E2E passed"
      target_evidence="Oxia + MinIO Worker checkpoint publication and REAPING E2E passed"
      authority_evidence="Oxia + MinIO Worker checkpoint publication and REAPING E2E passed"
      required_markers=("Oxia + MinIO Worker checkpoint publication and REAPING E2E passed")
      ;;
    kafka-fetch-response-loss)
      injection_point="discard a real read_committed guarded Fetch v13 response before source ACK"
      expected_state="replay starts at the same offset, observes the same LSO and commits the next source position"
      duplicate_boundary="Fetch response loss must not advance the source cursor or duplicate the record"
      fresh_process_recovery="NOT_COVERED"
      source_evidence="Kafka source Fetch response-loss E2E passed"
      target_evidence="Kafka source Fetch response-loss smoke passed"
      authority_evidence="Kafka source Fetch response-loss smoke passed"
      required_markers=("Kafka source Fetch response-loss E2E passed" "Kafka source Fetch response-loss smoke passed")
      ;;
    kafka-retention-floor)
      injection_point="advance real Broker retention beyond a stale guarded source offset"
      expected_state="stale source position is rejected and the current retention floor remains readable"
      duplicate_boundary="retention rejection cannot silently remap an old source position to a new record"
      fresh_process_recovery="NOT_COVERED"
      source_evidence="Kafka source retention-floor E2E passed"
      target_evidence="Kafka source retention-floor smoke passed"
      authority_evidence="Kafka source retention-floor smoke passed"
      required_markers=("Kafka source retention-floor E2E passed" "Kafka source retention-floor smoke passed")
      ;;
    pulsar-destination-response-loss)
      injection_point="discard the real Pulsar SEND response after the exact payload was persisted"
      expected_state="typed PULSAR_SEND_ACK evidence resolves the existing publish and source application finishes"
      duplicate_boundary="response loss must reread evidence and never send a second physical payload"
      fresh_process_recovery="NOT_COVERED"
      source_evidence="Pulsar destination committed response-loss E2E passed"
      target_evidence="Pulsar committed response-loss smoke passed"
      authority_evidence="Pulsar committed response-loss smoke passed"
      required_markers=("Pulsar destination committed response-loss E2E passed" "Pulsar committed response-loss smoke passed")
      ;;
    pulsar-source-ack-response-loss)
      injection_point="discard the real Pulsar source ACK response after Broker acceptance"
      expected_state="the next bounded Worker turn retries the same source ACK and closes the source record"
      duplicate_boundary="ACK response loss cannot cause a second source apply or destination publish"
      fresh_process_recovery="NOT_COVERED"
      source_evidence="Pulsar Worker source ACK response-loss E2E passed"
      target_evidence="Pulsar Worker source ACK response-loss smoke passed"
      authority_evidence="Pulsar Worker authority smoke passed"
      required_markers=("Pulsar Worker source ACK response-loss E2E passed" "Pulsar Worker source ACK response-loss smoke passed" "Pulsar Worker authority smoke passed")
      ;;
    gateway-oxia-session-churn)
      injection_point="stop and restart the real Oxia session during Gateway durable session use"
      expected_state="stale durable session fails closed and Gateway rereads one exact outcome after recovery"
      duplicate_boundary="session churn cannot resurrect stale Gateway state or duplicate the idempotency result"
      fresh_process_recovery="NOT_COVERED"
      source_evidence="Gateway Oxia session churn E2E passed"
      target_evidence="Gateway Oxia session churn E2E passed"
      authority_evidence="Dockerized Gateway Oxia session churn smoke passed"
      required_markers=("Gateway Oxia session churn E2E passed" "Dockerized Gateway Oxia session churn smoke passed")
      ;;
    *)
      echo "no chaos audit contract for cell ${name}" >&2
      return 1
      ;;
  esac

  local marker_status="PASS"
  local missing_markers=""
  local marker
  for marker in "${required_markers[@]}"; do
    if ! rg -Fq -- "${marker}" "${log}"; then
      marker_status="FAIL"
      missing_markers+="${marker}"$'\n'
    fi
  done
  local audit_status="PASS"
  if [[ "${result_status}" != "0" || "${marker_status}" != "PASS" ]]; then
    audit_status="FAIL"
  fi

  jq -n \
    --arg audit_status "${audit_status}" \
    --arg injection_point "${injection_point}" \
    --arg expected_state "${expected_state}" \
    --arg duplicate_boundary "${duplicate_boundary}" \
    --arg fresh_process_recovery "${fresh_process_recovery}" \
    --arg source_evidence "${source_evidence}" \
    --arg target_evidence "${target_evidence}" \
    --arg authority_evidence "${authority_evidence}" \
    --arg marker_status "${marker_status}" \
    --arg missing_markers "${missing_markers}" \
    --argjson required_marker_count "${#required_markers[@]}" \
    --argjson result_status "${result_status}" \
    --arg durable_state_dump_status "${durable_state_dump_status}" \
    --arg invariant_audit_status "${invariant_audit_status}" \
    '{
      audit_status: $audit_status,
      result_status: $result_status,
      deterministic_injection: {status: "DECLARED", point: $injection_point},
      expected_state: {status: "DECLARED_ONLY", description: $expected_state},
      duplicate_boundary: {status: "DECLARED_ONLY", description: $duplicate_boundary},
      evidence: {
        source: $source_evidence,
        target: $target_evidence,
        authority: $authority_evidence,
        required_marker_count: $required_marker_count,
        marker_status: $marker_status,
        missing_markers: ($missing_markers | split("\n") | map(select(length > 0)))
      },
      fresh_process_recovery: $fresh_process_recovery,
      durable_state_dump: {status: $durable_state_dump_status, note: "bounded runner has no canonical durable state dump"},
      invariant_audit: {status: $invariant_audit_status, note: "child assertions are marker evidence, not an independent state-dump audit"}
    }' >"${artifact_dir}/${name}.audit.json"
}

run_cell() {
  local name="$1"
  shift
  local log="${artifact_dir}/${name}.log"
  local result_file="${artifact_dir}/${name}.result"
  echo
  echo "===== CELL ${name} ====="
  set +e
  "$@" 2>&1 | tee "${log}"
  local status=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "${status}" >"${result_file}"
  rg -n "(E2E passed|smoke passed|BUILD SUCCESSFUL|source-applied physical publish passed|authority passed)" "${log}" \
    >"${artifact_dir}/${name}.summary" || true
  audit_cell "${name}" "${log}" "${status}"
  local audit_status
  audit_status="$(jq -r '.audit_status' "${artifact_dir}/${name}.audit.json")"
  if [[ "${audit_status}" != "PASS" ]]; then
    matrix_status=1
    echo "CELL ${name}: audit FAIL (required recovery evidence marker missing or child failed)"
  fi
  cell_results+=("${name}=${status}")
  if [[ "${status}" != 0 ]]; then
    matrix_status=1
    echo "CELL ${name}: FAIL (${status})"
  else
    echo "CELL ${name}: PASS (bounded receipt; not release certification)"
  fi
}

run_cell kafka-broker-process-crash env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31200 KAFKA_BROKER_2_PORT=31201 KAFKA_BROKER_3_PORT=31202 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31210 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-worker-ack-process-crash env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31220 KAFKA_BROKER_2_PORT=31221 KAFKA_BROKER_3_PORT=31222 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31230 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-broker-tcp-cut env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31240 KAFKA_BROKER_2_PORT=31241 KAFKA_BROKER_3_PORT=31242 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31250 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-broker-network-partition env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31400 KAFKA_BROKER_2_PORT=31401 KAFKA_BROKER_3_PORT=31402 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31410 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell pulsar-worker-process-crash env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH=1 \
  NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${matrix_gradle_home}" \
  PULSAR_BROKER_PORT=31260 PULSAR_WEB_PORT=31261 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31270 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell pulsar-multi-broker-process-crash env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH=1 \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${matrix_gradle_home}" \
  PULSAR_BROKER_1_PORT=31420 PULSAR_WEB_1_PORT=31421 \
  PULSAR_BROKER_2_PORT=31422 PULSAR_WEB_2_PORT=31423 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31430 \
  "${script_dir}/run-pulsar-multi-broker-failover-e2e.sh"

run_cell pulsar-worker-admission-response-loss env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1 \
  NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY=1 \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${matrix_gradle_home}" \
  PULSAR_BROKER_PORT=31280 PULSAR_WEB_PORT=31281 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31290 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell checkpoint-reaping env \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_E2E_GRADLE_USER_HOME="${matrix_gradle_home}" \
  NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31300 \
  NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31301 \
  "${script_dir}/run-oxia-minio-checkpoint-e2e.sh"

run_cell kafka-fetch-response-loss env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31320 KAFKA_BROKER_2_PORT=31321 KAFKA_BROKER_3_PORT=31322 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31330 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-retention-floor env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31340 KAFKA_BROKER_2_PORT=31341 KAFKA_BROKER_3_PORT=31342 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31350 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell pulsar-destination-response-loss env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1 \
  NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_ONLY=1 \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${matrix_gradle_home}" \
  PULSAR_BROKER_PORT=31360 PULSAR_WEB_PORT=31361 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31370 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell pulsar-source-ack-response-loss env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1 \
  NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_ONLY=1 \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${matrix_gradle_home}" \
  PULSAR_BROKER_PORT=31380 PULSAR_WEB_PORT=31381 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31390 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell gateway-oxia-session-churn env \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN=1 \
  NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_PAUSE_SECONDS=5 \
  NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME="${matrix_gradle_home}" \
  NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT=31440 \
  NEREUS_DELAY_GATEWAY_PORT=31450 \
  "${script_dir}/run-gateway-real-e2e.sh"

printf '\n===== MATRIX SUMMARY =====\n'
for result in "${cell_results[@]}"; do
  echo "${result}"
done
echo "artifact_dir=${artifact_dir}"
echo "matrix_status=${matrix_status}"

cells_json="${artifact_dir}/.bounded-chaos-cells.json"
printf '{}\n' >"${cells_json}"
for result in "${cell_results[@]}"; do
  cell_name="${result%%=*}"
  cell_status="${result#*=}"
  jq --arg name "${cell_name}" --argjson status "${cell_status}" \
    --slurpfile audit "${artifact_dir}/${cell_name}.audit.json" \
    '. + {($name): {status: $status, bounded: true, audit: $audit[0]}}' "${cells_json}" >"${cells_json}.tmp"
  mv "${cells_json}.tmp" "${cells_json}"
done
chaos_artifact="${artifact_dir}/bounded-chaos-matrix.json"
chaos_artifact_tmp="${chaos_artifact}.tmp"
if [[ "${matrix_status}" == "0" ]]; then
  matrix_result="PASS_BOUNDED"
else
  matrix_result="FAIL"
fi
jq -n \
  --arg schema "nereus-delay-bounded-chaos-matrix-v1" \
  --arg status "${matrix_result}" \
  --arg delay "${delay_source}" \
  --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" \
  --arg oxia "${oxia_source}" \
  --arg artifact "${artifact_dir}" \
  --slurpfile cells "${cells_json}" \
  '{
    schema: $schema,
    matrix_status: $status,
    artifact_dir: $artifact,
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    cells: $cells[0],
    audit_summary: {
      deterministic_injection: "DECLARED_AND_MARKER_CHECKED",
      source_target_authority_evidence: "MARKER_CHECKED",
      expected_state_and_duplicate_boundary: "DECLARED_ONLY",
      durable_state_dump: "NOT_CAPTURED",
      fresh_process_recovery: "CELL_SPECIFIC; NOT_COVERED CELLS REMAIN",
      invariant_audit: "MARKER_ONLY",
      release_certification: "OPEN"
    },
    boundaries: [
      "This is a bounded current-source fault matrix, not V1 release certification.",
      "Each bounded cell must emit its declared injection and required recovery markers; missing markers fail the bounded artifact.",
      "This runner does not capture canonical durable state dumps or an independent invariant audit, and therefore cannot satisfy the full §23.3 release matrix.",
      "A release gate must additionally prove required benchmark/capacity, certified soak, activation/cutover, operations and external authority evidence."
    ]
  }' >"${chaos_artifact_tmp}"
mv "${chaos_artifact_tmp}" "${chaos_artifact}"
rm -f "${cells_json}"
echo "canonical chaos artifact=${chaos_artifact}"

if [[ "${matrix_status}" != 0 ]]; then
  exit 1
fi
