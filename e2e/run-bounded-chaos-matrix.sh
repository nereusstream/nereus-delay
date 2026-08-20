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
  local durable_state_dump_note="bounded runner has no canonical durable state dump"
  local invariant_audit_note="child assertions are marker evidence, not an independent state-dump audit"
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
      required_markers=("Kafka Broker process-crash recovery E2E passed"
        "Kafka Broker process-crash durable state dump passed: phase=before"
        "Kafka Broker process-crash durable state dump passed: phase=after"
        "Kafka Worker source-applied physical publish passed" "Kafka Worker authority smoke passed")
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
      required_markers=("Kafka Worker raw TCP Broker-endpoint cut recovery E2E passed"
        "Kafka Broker kafka-broker-tcp-cut durable state dump passed: phase=before"
        "Kafka Broker kafka-broker-tcp-cut durable state dump passed: phase=after"
        "Kafka Worker vertical smoke passed" "Kafka Worker authority smoke passed")
      ;;
    kafka-broker-network-partition)
      injection_point="remove kafka-1 from the exact Compose network while keeping the process alive"
      expected_state="survivor leaders continue source/target progress and kafka-1 reconnects without changing the source identity"
      duplicate_boundary="partition recovery cannot create a second source-applied outcome"
      fresh_process_recovery="PASS"
      source_evidence="Kafka Broker network-partition recovery E2E passed"
      target_evidence="Kafka Worker vertical smoke passed"
      authority_evidence="Kafka Worker authority smoke passed"
      required_markers=("Kafka Broker network-partition recovery E2E passed"
        "Kafka Broker kafka-broker-network-partition durable state dump passed: phase=before"
        "Kafka Broker kafka-broker-network-partition durable state dump passed: phase=after"
        "Kafka Worker vertical smoke passed" "Kafka Worker authority smoke passed")
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
      expected_state="a fresh Worker reopens the durable PUBLISHING admission and resolves one typed destination outcome"
      duplicate_boundary="response loss cannot append a second admission or physical destination publish"
      fresh_process_recovery="PASS"
      source_evidence="Pulsar Worker Publish Admission response-loss fresh-process recovery E2E passed"
      target_evidence="Pulsar Worker source-applied physical publish passed"
      authority_evidence="Pulsar Worker authority smoke passed"
      required_markers=("Pulsar Worker Publish Admission response-loss smoke passed" "Pulsar Worker Publish Admission response-loss fresh-process recovery E2E passed" "Pulsar Worker source-applied physical publish passed" "Pulsar Worker authority smoke passed")
      ;;
    pulsar-worker-destination-response-loss)
      injection_point="discard the real Pulsar SEND response after PULSAR_SEND_ACK evidence is resolved and before source apply"
      expected_state="a fresh Worker replays the durable PUBLISH_OUTCOME, applies PUBLISHED and reads the exact destination payload without a second SEND"
      duplicate_boundary="the durable Outcome is the only recovery authority; fresh process recovery must not resend the physical payload"
      fresh_process_recovery="PASS"
      source_evidence="Pulsar Worker destination response-loss fresh-process recovery E2E passed"
      target_evidence="Pulsar Worker vertical smoke passed"
      authority_evidence="Pulsar Worker authority smoke passed"
      required_markers=("Pulsar Worker destination response-loss fresh-process recovery passed" "Pulsar Worker destination response-loss fresh-process recovery E2E passed" "Pulsar Worker vertical smoke passed" "Pulsar Worker authority smoke passed")
      ;;
    checkpoint-reaping)
      injection_point="checkpoint upload/catalog/reaping competition against real Oxia and MinIO"
      expected_state="only the authorized checkpoint candidate is published/reaped and immutable object identity is preserved"
      duplicate_boundary="late provider work cannot delete or activate a fenced checkpoint"
      fresh_process_recovery="PASS"
      source_evidence="Oxia + MinIO Worker checkpoint publication and REAPING E2E passed"
      target_evidence="Oxia + MinIO Worker checkpoint publication and REAPING E2E passed"
      authority_evidence="Oxia + MinIO Worker checkpoint publication and REAPING E2E passed"
      required_markers=("Oxia + MinIO Worker checkpoint publication and REAPING E2E passed"
        "Oxia + MinIO checkpoint REAPING fresh-process recovery E2E passed")
      ;;
    kafka-fetch-response-loss)
      injection_point="discard a real read_committed guarded Fetch v13 response, then reopen the same group in a fresh JVM"
      expected_state="replay starts at the same offset, observes the same LSO and commits the next source position"
      duplicate_boundary="Fetch response loss must not advance the source cursor or duplicate the record"
      fresh_process_recovery="PASS"
      source_evidence="Kafka source Fetch response-loss fresh-process recovery E2E passed"
      target_evidence="Kafka source Fetch response-loss process-crash cut reached"
      authority_evidence="Kafka source Fetch response-loss fresh-process recovery E2E passed"
      required_markers=("Kafka source Fetch response-loss process-crash cut reached" "Kafka source Fetch response-loss fresh-process recovery E2E passed")
      ;;
    kafka-retention-floor)
      injection_point="advance real Broker retention beyond a stale guarded source offset, then reopen the current floor in a fresh JVM"
      expected_state="stale source position is rejected and the current retention floor remains readable"
      duplicate_boundary="retention rejection cannot silently remap an old source position to a new record"
      fresh_process_recovery="PASS"
      source_evidence="Kafka source retention-floor fresh-process recovery E2E passed"
      target_evidence="Kafka source retention-floor process-crash cut reached"
      authority_evidence="Kafka source retention-floor fresh-process recovery E2E passed"
      required_markers=("Kafka source retention-floor process-crash cut reached" "Kafka source retention-floor fresh-process recovery E2E passed")
      ;;
    pulsar-destination-response-loss)
      injection_point="discard the real Pulsar SEND response after the exact payload was persisted"
      expected_state="a fresh JVM revalidates typed PULSAR_SEND_ACK evidence and reads exactly one existing publish without a second SEND"
      duplicate_boundary="response loss must reread evidence and never send a second physical payload"
      fresh_process_recovery="PASS"
      source_evidence="Pulsar destination committed response-loss fresh-process E2E passed"
      target_evidence="Pulsar destination committed response-loss fresh-process READ passed"
      authority_evidence="Pulsar destination committed response-loss fresh-process READ passed"
      required_markers=("Pulsar destination committed response-loss fresh-process E2E passed"
        "Pulsar destination committed response-loss fresh-process WRITE passed"
        "Pulsar destination committed response-loss fresh-process READ passed")
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
  if [[ "${name}" == "kafka-broker-process-crash" ]]; then
    local state_dump_dir="${artifact_dir}/kafka-broker-process-crash-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "kafka-broker-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "BROKER_PROCESS_CRASH_READY"
              and $post.phase == "RECOVERED_AFTER_BROKER_REJOIN"
              and ($pre.topic == $post.topic)
              and ($pre.cluster_id == $post.cluster_id)
              and ($pre.topic_id == $post.topic_id)
              and ($pre.partition == 0 and $post.partition == $pre.partition)
              and ($pre.leader_id | (type == "number" and . > 0))
              and ($pre.replica_ids == [1, 2, 3])
              and ($post.replica_ids == $pre.replica_ids)
              and (($pre.live_broker_ids | index(1)) != null)
              and (($pre.isr_ids | index(1)) != null)
              and (($post.live_broker_ids | index(1)) != null)
              and (($post.isr_ids | index(1)) != null)
              and ($pre.end_offset | (type == "number" and . >= 1))
              and ($post.end_offset | (type == "number" and . > $pre.end_offset))
              and $pre.broker_1_rejoined == false
              and $post.broker_1_rejoined == true
              and $pre.durable_broker_read == true
              and $post.durable_broker_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced Kafka metadata dumps before SIGKILL and after Broker-1 ISR rejoin"
      invariant_audit_note="topic/cluster identity, replica and ISR membership, live Broker-1 rejoin, end offset and process identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required Kafka Broker pre-crash and post-rejoin dumps were missing or failed cross-process validation"
      invariant_audit_note="independent Broker metadata and rejoin-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "kafka-broker-tcp-cut" || "${name}" == "kafka-broker-network-partition" ]]; then
    local state_dump_dir
    local before_dump=""
    local after_dump=""
    local before_phase=""
    local after_phase=""
    if [[ "${name}" == "kafka-broker-tcp-cut" ]]; then
      state_dump_dir="${artifact_dir}/kafka-broker-tcp-cut-state"
      before_phase="BROKER_TCP_CUT_READY"
      after_phase="RECOVERED_AFTER_BROKER_TCP_CUT"
    else
      state_dump_dir="${artifact_dir}/kafka-broker-network-partition-state"
      before_phase="BROKER_NETWORK_PARTITION_READY"
      after_phase="RECOVERED_AFTER_BROKER_NETWORK_REJOIN"
    fi
    before_dump="${state_dump_dir}/before-process-crash.json"
    after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --arg cell "${name}" \
          --arg before_phase "${before_phase}" \
          --arg after_phase "${after_phase}" \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == $cell
              and $post.cell == $pre.cell
              and $pre.phase == $before_phase
              and $post.phase == $after_phase
              and ($pre.topic == $post.topic)
              and ($pre.cluster_id == $post.cluster_id)
              and ($pre.topic_id == $post.topic_id)
              and ($pre.partition == 0 and $post.partition == $pre.partition)
              and ($pre.leader_id | (type == "number" and . > 0))
              and ($post.leader_id | (type == "number" and . > 0))
              and ($pre.replica_ids == [1, 2, 3])
              and ($post.replica_ids == $pre.replica_ids)
              and ($pre.live_broker_ids == [1, 2, 3])
              and ($pre.isr_ids == [1, 2, 3])
              and ($post.live_broker_ids == [1, 2, 3])
              and ($post.isr_ids == [1, 2, 3])
              and ($pre.end_offset | (type == "number" and . >= 1))
              and ($post.end_offset | (type == "number" and . > $pre.end_offset))
              and $pre.broker_1_recovery_observed == false
              and $post.broker_1_recovery_observed == true
              and $pre.durable_broker_read == true
              and $post.durable_broker_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced Kafka metadata dumps before and after ${name} recovery"
      invariant_audit_note="topic/cluster identity, replica/ISR/live membership, Broker-1 recovery, end offset and process identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required Kafka ${name} pre-cut and post-recovery dumps were missing or failed cross-process validation"
      invariant_audit_note="independent Kafka Broker metadata and recovery-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "kafka-worker-process-crash" ]]; then
    local state_dump_dir="${artifact_dir}/kafka-worker-process-crash-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "kafka-worker-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "WORKER_PROCESS_CRASH_READY"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and ($pre.topic == $post.topic)
              and ($pre.cluster_id == $post.cluster_id)
              and ($pre.topic_id == $post.topic_id)
              and ($pre.route_uuid == $post.route_uuid)
              and ($pre.partition == $post.partition)
              and $pre.applied_offset == 0
              and $post.applied_offset == 1
              and ($pre.applied_source_position | (type == "string" and length > 0))
              and ($post.applied_source_position | (type == "string" and length > 0))
              and ($pre.store_root == $post.store_root)
              and ($pre.store_incarnation == $post.store_incarnation)
              and ($pre.db_identity == $post.db_identity)
              and $pre.store_write_batch_durable == true
              and $post.store_write_batch_durable == true
              and $pre.source_ack_committed == false
              and $post.source_ack_committed == true
              and $pre.durable_store_read == true
              and $post.durable_store_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced Store metadata/source-position dump before SIGKILL and after fresh-process recovery"
      invariant_audit_note="topic, Kafka identity, Store identity, source offsets, ACK boundary and process identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required Worker pre-crash and post-recovery Store dumps were missing or failed cross-process validation"
      invariant_audit_note="independent Worker durable-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "kafka-worker-ack-process-crash" ]]; then
    local state_dump_dir="${artifact_dir}/kafka-worker-ack-process-crash-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "kafka-worker-ack-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "WORKER_ACK_PROCESS_CRASH_READY"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and ($pre.topic == $post.topic)
              and ($pre.cluster_id == $post.cluster_id)
              and ($pre.topic_id == $post.topic_id)
              and ($pre.route_uuid == $post.route_uuid)
              and ($pre.partition == $post.partition)
              and ($pre.applied_source_position == $post.applied_source_position)
              and ($pre.applied_source_position | (type == "string" and length > 0))
              and ($pre.applied_offset | (type == "number" and . >= 0))
              and ($post.applied_offset == $pre.applied_offset)
              and ($pre.store_root == $post.store_root)
              and ($pre.store_incarnation == $post.store_incarnation)
              and ($pre.db_identity == $post.db_identity)
              and $pre.store_write_batch_durable == true
              and $post.store_write_batch_durable == true
              and $pre.source_ack_committed == false
              and $post.source_ack_committed == true
              and $pre.durable_store_read == true
              and $post.durable_store_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced Store WriteBatch/source-ACK boundary dump before SIGKILL and after fresh-process recovery"
      invariant_audit_note="topic, Kafka identity, Store identity, exact applied source position, ACK boundary and process identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required ACK-cut pre-crash and post-recovery Store dumps were missing or failed cross-process validation"
      invariant_audit_note="independent ACK-cut durable-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "pulsar-worker-process-crash" ]]; then
    local state_dump_dir="${artifact_dir}/pulsar-worker-process-crash-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "pulsar-worker-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "PULSAR_WORKER_PROCESS_CRASH_READY"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and ($pre.physical_topic == $post.physical_topic)
              and ($pre.cluster_id == $post.cluster_id)
              and ($pre.route_uuid == $post.route_uuid)
              and ($pre.shard == $post.shard)
              and ($pre.partition == $post.partition)
              and ($pre.store_root == $post.store_root)
              and ($pre.store_incarnation == $post.store_incarnation)
              and ($pre.db_identity == $post.db_identity)
              and $pre.source_record_prepared == true
              and $post.source_record_prepared == true
              and $pre.source_record_applied == false
              and $post.source_record_applied == true
              and $pre.source_ack_committed == false
              and $post.source_ack_committed == true
              and ($post.applied_source_position | (type == "string" and length > 0))
              and $pre.durable_store_read == true
              and $post.durable_store_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced Store identity/source-apply dump before SIGKILL and after fresh-process recovery"
      invariant_audit_note="topic, shard, Store identity, source-apply/ACK boundary and process identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required Worker pre-crash and post-recovery dumps were missing or failed cross-process validation"
      invariant_audit_note="independent Worker durable-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "pulsar-worker-admission-response-loss" ]]; then
    local state_dump_dir="${artifact_dir}/pulsar-worker-admission-response-loss-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "pulsar-worker-admission-response-loss-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "ADMISSION_RESPONSE_LOSS_PERSISTED"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and $pre.attempt_state == "PUBLISHING"
              and $post.attempt_state == "PUBLISHED"
              and $pre.outcome_applied == false
              and $post.outcome_applied == true
              and $pre.durable_store_read == true
              and $post.durable_store_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.store_root == $post.store_root)
              and ($pre.shard == $post.shard)
              and ($pre.store_incarnation == $post.store_incarnation)
              and ($pre.db_identity == $post.db_identity)
              and ($pre.publish_attempt_id == $post.publish_attempt_id)
              and ($pre.attempt_source_position == $post.attempt_source_position)
              and ($pre.publish_attempt_id | (type == "string" and length > 0))
              and ($pre.attempt_source_position | (type == "string" and length > 0))
              and ($pre.physical_schedule_position | (type == "string" and length > 0))
              and ($post.applied_source_position | (type == "string" and length > 0))
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced JSON dump before SIGKILL and after fresh-process recovery"
      invariant_audit_note="pre/post durable fields, Store identity and Publish Attempt identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required pre-crash and post-recovery dumps were missing or failed cross-process validation"
      invariant_audit_note="independent durable-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "pulsar-worker-destination-response-loss" ]]; then
    local state_dump_dir="${artifact_dir}/pulsar-worker-destination-response-loss-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "pulsar-worker-destination-response-loss-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "DESTINATION_RESPONSE_LOSS_PERSISTED"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and $pre.attempt_state == "PUBLISHING"
              and $post.attempt_state == "PUBLISHED"
              and $pre.outcome_applied == false
              and $post.outcome_applied == true
              and $pre.durable_store_read == true
              and $post.durable_store_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.store_root == $post.store_root)
              and ($pre.shard == $post.shard)
              and ($pre.store_incarnation == $post.store_incarnation)
              and ($pre.db_identity == $post.db_identity)
              and ($pre.physical_schedule_position == $post.physical_schedule_position)
              and ($pre.publish_attempt_id == $post.publish_attempt_id)
              and ($pre.message_id == $post.message_id)
              and ($pre.publish_attempt_id | (type == "string" and length > 0))
              and ($pre.message_id | (type == "string" and length > 0))
              and ($pre.physical_schedule_position | (type == "string" and length > 0))
              and ($post.applied_source_position | (type == "string" and length > 0))
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced JSON dump before SIGKILL and after fresh-process recovery"
      invariant_audit_note="pre/post durable fields, Store identity, Publish Attempt identity and Message identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required pre-crash and post-recovery dumps were missing or failed cross-process validation"
      invariant_audit_note="independent durable-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "kafka-fetch-response-loss" ]]; then
    local state_dump_dir="${artifact_dir}/kafka-fetch-response-loss-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "kafka-fetch-response-loss-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "FETCH_RESPONSE_LOSS_PERSISTED"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and ($pre.topic == $post.topic)
              and ($pre.group_id == $post.group_id)
              and ($pre.topic_id == $post.topic_id)
              and ($pre.route_uuid == $post.route_uuid)
              and ($pre.partition == $post.partition)
              and ($pre.replay_offset | (type == "number" and . >= 0))
              and ($pre.last_stable_offset | (type == "number" and . > $pre.replay_offset))
              and ($post.replay_offset == $pre.replay_offset)
              and ($post.second_offset | (type == "number" and . > $post.replay_offset))
              and ($post.committed_offset == ($post.second_offset + 1))
              and $pre.response_discarded_after_fetch == true
              and $post.response_discarded_after_fetch == true
              and $pre.durable_broker_read == true
              and $post.durable_broker_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced JSON dump before and after fresh-group JVM recovery"
      invariant_audit_note="topic, group, topic identity, Route identity, source offsets, LSO and committed offset were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required pre-crash and post-recovery Fetch dumps were missing or failed cross-process validation"
      invariant_audit_note="independent Fetch/LSO/commit comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "kafka-retention-floor" ]]; then
    local state_dump_dir="${artifact_dir}/kafka-retention-floor-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "kafka-retention-floor-process-crash"
              and $post.cell == $pre.cell
              and $pre.phase == "RETENTION_FLOOR_REJECTED"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and ($pre.topic == $post.topic)
              and ($pre.topic_id == $post.topic_id)
              and ($pre.route_uuid == $post.route_uuid)
              and ($pre.partition == $post.partition)
              and $pre.old_offset == 0
              and ($pre.retention_floor | (type == "number" and . > 0))
              and ($pre.end_offset | (type == "number" and . > $pre.retention_floor))
              and $pre.stale_offset_rejected == true
              and ($post.retention_floor | (type == "number" and . > 0))
              and ($post.end_offset | (type == "number" and . > $post.retention_floor))
              and ($post.floor_fetch_offset | (type == "number" and . >= $post.retention_floor))
              and ($post.fetch_lso | (type == "number" and . > $post.floor_fetch_offset))
              and $post.stale_offset_rejected == true
              and $pre.durable_broker_read == true
              and $post.durable_broker_read == true
              and $pre.dump_forced == true
              and $post.dump_forced == true
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced JSON dump after stale-offset rejection and after fresh-floor recovery"
      invariant_audit_note="topic identity, Route identity, retention floor, end offset, stale rejection and LSO were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required retention pre-crash and post-recovery dumps were missing or failed cross-process validation"
      invariant_audit_note="independent retention-floor comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "checkpoint-reaping" ]]; then
    local state_dump_dir="${artifact_dir}/checkpoint-reaping-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "checkpoint-reaping"
              and $post.cell == $pre.cell
              and $pre.phase == "REAPING_READY"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and ($pre.authority_prefix == $post.authority_prefix)
              and ($pre.pending_intent_digest_base64 == $post.pending_intent_digest_base64)
              and ($pre.route_uuid == $post.route_uuid)
              and ($pre.partition == $post.partition)
              and ($pre.recovery_lineage_id_base64 == $post.recovery_lineage_id_base64)
              and ($pre.checkpoint_id_base64 == $post.checkpoint_id_base64)
              and ($pre.source_store_incarnation_base64 == $post.source_store_incarnation_base64)
              and ($pre.owner_released == true)
              and ($pre.provider_ownership_closed == true)
              and ($pre.object_versions_present == true)
              and ($post.owner_current_absent == true)
              and ($post.provider_quiescence_proof_bound == true)
              and ($pre.durable_store_read == true)
              and ($post.durable_store_read == true)
              and ($pre.dump_forced == true)
              and ($post.dump_forced == true)
              and ($post.reaping_intent_state == "REAPING")
              and ($pre.expected_version_count | tonumber) == ($post.listed_version_count | tonumber)
              and ($post.listed_version_count | tonumber) == ($post.deleted_version_count | tonumber)
              and ($post.prefix_empty == true)
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced Oxia Intent/Owner/Object Store dump before and after separate REAPING JVM"
      invariant_audit_note="intent digest, Route/checkpoint/store identities, owner absence, exact version counts, empty prefix and process identity were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required checkpoint REAPING pre-process and post-process dumps were missing or failed cross-process validation"
      invariant_audit_note="independent Intent/Owner/Object Store reaping-field comparison failed"
      audit_status="FAIL"
    fi
  fi
  if [[ "${name}" == "pulsar-destination-response-loss" ]]; then
    local state_dump_dir="${artifact_dir}/pulsar-destination-response-loss-state"
    local before_dump="${state_dump_dir}/before-process-crash.json"
    local after_dump="${state_dump_dir}/after-fresh-process.json"
    if [[ -s "${before_dump}" && -s "${after_dump}" ]] \
        && jq -e \
          --slurpfile before "${before_dump}" \
          --slurpfile after "${after_dump}" \
          -n '
            ($before[0]) as $pre
            | ($after[0]) as $post
            | $pre.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $post.schema == "nereus-delay-chaos-durable-state-dump-v1"
              and $pre.cell == "pulsar-destination-response-loss"
              and $post.cell == $pre.cell
              and $pre.phase == "DESTINATION_RESPONSE_LOSS_READY"
              and $post.phase == "RECOVERED_AFTER_FRESH_PROCESS"
              and ($pre.physical_topic == $post.physical_topic)
              and ($pre.authenticated_cluster == $post.authenticated_cluster)
              and ($pre.resource_incarnation_base64 == $post.resource_incarnation_base64)
              and ($pre.topic_creation_timestamp == $post.topic_creation_timestamp)
              and ($pre.partition == $post.partition)
              and ($pre.publish_attempt_id_base64 == $post.publish_attempt_id_base64)
              and ($pre.prepared_hash_base64 == $post.prepared_hash_base64)
              and ($pre.payload_base64 == $post.payload_base64)
              and ($pre.ledger_id == $post.ledger_id)
              and ($pre.entry_id == $post.entry_id)
              and ($pre.sequence_id == $post.sequence_id)
              and ($pre.broker_entry_timestamp == $post.broker_entry_timestamp)
              and ($pre.authenticated_response_sha256_base64 == $post.authenticated_response_sha256_base64)
              and ($pre.physical_send_count == "1")
              and ($post.physical_send_count == $pre.physical_send_count)
              and ($pre.send_committed == true)
              and ($pre.response_discarded == true)
              and ($post.payload_readback_exact == true)
              and ($post.duplicate_payload_count | tonumber) == 0
              and ($post.evidence_verified == true)
              and ($pre.durable_broker_read == true)
              and ($post.durable_broker_read == true)
              and ($pre.dump_forced == true)
              and ($post.dump_forced == true)
              and ($pre.process_pid != $post.process_pid)
          ' >/dev/null; then
      durable_state_dump_status="CAPTURED_AND_VERIFIED"
      invariant_audit_status="INDEPENDENT_FIELDS_PASS"
      durable_state_dump_note="external fsync-forced SEND evidence dump before and after separate Pulsar payload-read JVM"
      invariant_audit_note="physical topic/guard identity, publish attempt, prepared hash, broker position, evidence digest, payload and single-send boundary were independently compared"
    else
      fresh_process_recovery="FAIL"
      durable_state_dump_status="FAIL"
      invariant_audit_status="FAIL"
      durable_state_dump_note="required Pulsar destination response-loss pre-process and post-process dumps were missing or failed cross-process validation"
      invariant_audit_note="independent SEND evidence and duplicate-boundary comparison failed"
      audit_status="FAIL"
    fi
  fi
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
    --arg durable_state_dump_note "${durable_state_dump_note}" \
    --arg invariant_audit_status "${invariant_audit_status}" \
    --arg invariant_audit_note "${invariant_audit_note}" \
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
      durable_state_dump: {status: $durable_state_dump_status, note: $durable_state_dump_note},
      invariant_audit: {status: $invariant_audit_status, note: $invariant_audit_note}
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
  NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_STATE_DUMP_DIR="${artifact_dir}/kafka-broker-process-crash-state" \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31200 KAFKA_BROKER_2_PORT=31201 KAFKA_BROKER_3_PORT=31202 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31210 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-worker-ack-process-crash env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_STATE_DUMP_DIR="${artifact_dir}/kafka-worker-ack-process-crash-state" \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31220 KAFKA_BROKER_2_PORT=31221 KAFKA_BROKER_3_PORT=31222 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31230 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-broker-tcp-cut env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY=1 \
  NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_STATE_DUMP_DIR="${artifact_dir}/kafka-broker-tcp-cut-state" \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31240 KAFKA_BROKER_2_PORT=31241 KAFKA_BROKER_3_PORT=31242 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31250 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-broker-network-partition env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY=1 \
  NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_STATE_DUMP_DIR="${artifact_dir}/kafka-broker-network-partition-state" \
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
  NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_STATE_DUMP_DIR="${artifact_dir}/pulsar-worker-process-crash-state" \
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
  NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_STATE_DUMP_DIR="${artifact_dir}/pulsar-worker-admission-response-loss-state" \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${matrix_gradle_home}" \
  PULSAR_BROKER_PORT=31280 PULSAR_WEB_PORT=31281 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31290 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell pulsar-worker-destination-response-loss env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1 \
  NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_STATE_DUMP_DIR="${artifact_dir}/pulsar-worker-destination-response-loss-state" \
  NEREUS_DELAY_PULSAR_GRADLE_USER_HOME="${matrix_gradle_home}" \
  PULSAR_BROKER_PORT=31300 PULSAR_WEB_PORT=31301 \
  NEREUS_DELAY_PULSAR_OXIA_PORT=31310 \
  "${script_dir}/run-pulsar-real-client-e2e.sh"

run_cell checkpoint-reaping env \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_E2E_GRADLE_USER_HOME="${matrix_gradle_home}" \
  NEREUS_DELAY_CHECKPOINT_REAPING_FRESH_PROCESS=1 \
  NEREUS_DELAY_CHECKPOINT_REAPING_STATE_DUMP_DIR="${artifact_dir}/checkpoint-reaping-state" \
  NEREUS_DELAY_CHECKPOINT_REAPING_PREFIX="nereus-delay-chaos-reaping/$(date +%s)-$$" \
  NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31300 \
  NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31301 \
  "${script_dir}/run-oxia-minio-checkpoint-e2e.sh"

run_cell kafka-fetch-response-loss env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
  NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_STATE_DUMP_DIR="${artifact_dir}/kafka-fetch-response-loss-state" \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31320 KAFKA_BROKER_2_PORT=31321 KAFKA_BROKER_3_PORT=31322 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31330 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell kafka-retention-floor env \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
  NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
  NEREUS_DELAY_KAFKA_RETENTION_FLOOR_PROCESS_CRASH_ONLY=1 \
  NEREUS_DELAY_KAFKA_RETENTION_FLOOR_STATE_DUMP_DIR="${artifact_dir}/kafka-retention-floor-state" \
  NEREUS_DELAY_KAFKA_GRADLE_USER_HOME="${matrix_gradle_home}" \
  KAFKA_BROKER_1_PORT=31340 KAFKA_BROKER_2_PORT=31341 KAFKA_BROKER_3_PORT=31342 \
  NEREUS_DELAY_KAFKA_OXIA_PORT=31350 \
  "${script_dir}/run-kafka-real-client-e2e.sh"

run_cell pulsar-destination-response-loss env \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
  NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1 \
  NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_FRESH_PROCESS=1 \
  NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_STATE_DUMP_DIR="${artifact_dir}/pulsar-destination-response-loss-state" \
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
admission_audit_status="$(jq -r '."pulsar-worker-admission-response-loss".audit.audit_status' "${cells_json}")"
admission_dump_status="$(jq -r '."pulsar-worker-admission-response-loss".audit.durable_state_dump.status' "${cells_json}")"
admission_invariant_status="$(jq -r '."pulsar-worker-admission-response-loss".audit.invariant_audit.status' "${cells_json}")"
destination_audit_status="$(jq -r '."pulsar-worker-destination-response-loss".audit.audit_status' "${cells_json}")"
destination_dump_status="$(jq -r '."pulsar-worker-destination-response-loss".audit.durable_state_dump.status' "${cells_json}")"
destination_recovery_status="$(jq -r '."pulsar-worker-destination-response-loss".audit.fresh_process_recovery' "${cells_json}")"
destination_invariant_status="$(jq -r '."pulsar-worker-destination-response-loss".audit.invariant_audit.status' "${cells_json}")"
admission_recovery_status="$(jq -r '."pulsar-worker-admission-response-loss".audit.fresh_process_recovery' "${cells_json}")"
fetch_dump_status="$(jq -r '."kafka-fetch-response-loss".audit.durable_state_dump.status' "${cells_json}")"
fetch_recovery_status="$(jq -r '."kafka-fetch-response-loss".audit.fresh_process_recovery' "${cells_json}")"
fetch_invariant_status="$(jq -r '."kafka-fetch-response-loss".audit.invariant_audit.status' "${cells_json}")"
retention_dump_status="$(jq -r '."kafka-retention-floor".audit.durable_state_dump.status' "${cells_json}")"
retention_recovery_status="$(jq -r '."kafka-retention-floor".audit.fresh_process_recovery' "${cells_json}")"
retention_invariant_status="$(jq -r '."kafka-retention-floor".audit.invariant_audit.status' "${cells_json}")"
broker_dump_status="$(jq -r '."kafka-broker-process-crash".audit.durable_state_dump.status' "${cells_json}")"
broker_recovery_status="$(jq -r '."kafka-broker-process-crash".audit.fresh_process_recovery' "${cells_json}")"
broker_invariant_status="$(jq -r '."kafka-broker-process-crash".audit.invariant_audit.status' "${cells_json}")"
broker_tcp_dump_status="$(jq -r '."kafka-broker-tcp-cut".audit.durable_state_dump.status' "${cells_json}")"
broker_tcp_recovery_status="$(jq -r '."kafka-broker-tcp-cut".audit.fresh_process_recovery' "${cells_json}")"
broker_tcp_invariant_status="$(jq -r '."kafka-broker-tcp-cut".audit.invariant_audit.status' "${cells_json}")"
broker_network_dump_status="$(jq -r '."kafka-broker-network-partition".audit.durable_state_dump.status' "${cells_json}")"
broker_network_recovery_status="$(jq -r '."kafka-broker-network-partition".audit.fresh_process_recovery' "${cells_json}")"
broker_network_invariant_status="$(jq -r '."kafka-broker-network-partition".audit.invariant_audit.status' "${cells_json}")"
worker_ack_dump_status="$(jq -r '."kafka-worker-ack-process-crash".audit.durable_state_dump.status' "${cells_json}")"
worker_ack_recovery_status="$(jq -r '."kafka-worker-ack-process-crash".audit.fresh_process_recovery' "${cells_json}")"
worker_ack_invariant_status="$(jq -r '."kafka-worker-ack-process-crash".audit.invariant_audit.status' "${cells_json}")"
worker_process_dump_status="$(jq -r '."pulsar-worker-process-crash".audit.durable_state_dump.status' "${cells_json}")"
worker_process_recovery_status="$(jq -r '."pulsar-worker-process-crash".audit.fresh_process_recovery' "${cells_json}")"
worker_process_invariant_status="$(jq -r '."pulsar-worker-process-crash".audit.invariant_audit.status' "${cells_json}")"
durable_state_dump_summary="CELL_SPECIFIC; kafka-broker-process-crash ${broker_dump_status}; kafka-broker-tcp-cut ${broker_tcp_dump_status}; kafka-broker-network-partition ${broker_network_dump_status}; kafka-worker-ack-process-crash ${worker_ack_dump_status}; pulsar-worker-process-crash ${worker_process_dump_status}; kafka-fetch-response-loss ${fetch_dump_status}; kafka-retention-floor ${retention_dump_status}; pulsar-worker-admission-response-loss ${admission_dump_status}; pulsar-worker-destination-response-loss ${destination_dump_status}; other cells NOT_CAPTURED"
fresh_process_recovery_summary="CELL_SPECIFIC; kafka-broker-process-crash ${broker_recovery_status}; kafka-broker-tcp-cut ${broker_tcp_recovery_status}; kafka-broker-network-partition ${broker_network_recovery_status}; kafka-worker-ack-process-crash ${worker_ack_recovery_status}; pulsar-worker-process-crash ${worker_process_recovery_status}; kafka-fetch-response-loss ${fetch_recovery_status}; kafka-retention-floor ${retention_recovery_status}; pulsar-worker-admission-response-loss ${admission_recovery_status}; pulsar-worker-destination-response-loss ${destination_recovery_status}; other cells NOT_COVERED"
invariant_audit_summary="CELL_SPECIFIC; kafka-broker-process-crash ${broker_invariant_status}; kafka-broker-tcp-cut ${broker_tcp_invariant_status}; kafka-broker-network-partition ${broker_network_invariant_status}; kafka-worker-ack-process-crash ${worker_ack_invariant_status}; pulsar-worker-process-crash ${worker_process_invariant_status}; kafka-fetch-response-loss ${fetch_invariant_status}; kafka-retention-floor ${retention_invariant_status}; pulsar-worker-admission-response-loss ${admission_invariant_status}; pulsar-worker-destination-response-loss ${destination_invariant_status}; other cells MARKER_ONLY"
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
  --arg durable_state_dump_summary "${durable_state_dump_summary}" \
  --arg fresh_process_recovery_summary "${fresh_process_recovery_summary}" \
  --arg invariant_audit_summary "${invariant_audit_summary}" \
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
      durable_state_dump: $durable_state_dump_summary,
      fresh_process_recovery: $fresh_process_recovery_summary,
      invariant_audit: $invariant_audit_summary,
      release_certification: "OPEN"
    },
    boundaries: [
      "This is a bounded current-source fault matrix, not V1 release certification.",
      "Each bounded cell must emit its declared injection and required recovery markers; missing markers fail the bounded artifact.",
      "The Kafka Broker process/network/TCP cuts, Kafka Worker ACK, Pulsar Worker process-crash, Fetch/retention-floor and Pulsar Worker Publish Admission/destination response-loss cells capture and independently audit external durable state dumps; the remaining cells still require their own §23.3 durable evidence.",
      "A release gate must additionally prove required benchmark/capacity, certified soak, activation/cutover, operations and external authority evidence."
    ]
  }' >"${chaos_artifact_tmp}"
mv "${chaos_artifact_tmp}" "${chaos_artifact}"
rm -f "${cells_json}"
echo "canonical chaos artifact=${chaos_artifact}"

if [[ "${matrix_status}" != 0 ]]; then
  exit 1
fi
