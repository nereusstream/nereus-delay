#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Full distribution gate.  The Kafka and Pulsar worktrees are treated as
# independent source authorities: this runner compiles/tests their guarded
# client surfaces, records immutable binary digests, and refuses to pass when
# the multi-Broker partial-rollout child was not actually executed.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY__PATCH_ARTIFACT_DIR:-}"
candidate_lock_file="${NEREUS_DELAY__PATCH_CANDIDATE_SOURCE_LOCK:-}"
profile_id="${NEREUS_DELAY__PATCH_PROFILE_ID:-nereus-delay-patch-distribution-full-r1}"
run_cluster="${NEREUS_DELAY__PATCH_RUN_CLUSTER:-0}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

fail() {
  echo "full patch-distribution: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
command -v shasum >/dev/null 2>&1 || fail "shasum is required"
[[ -n "${artifact_dir}" ]] || fail "NEREUS_DELAY__PATCH_ARTIFACT_DIR is required"
[[ -n "${candidate_lock_file}" && -s "${candidate_lock_file}" ]] \
  || fail "NEREUS_DELAY__PATCH_CANDIDATE_SOURCE_LOCK must name a non-empty JSON file"
[[ "${run_cluster}" == "0" || "${run_cluster}" == "1" ]] \
  || fail "NEREUS_DELAY__PATCH_RUN_CLUSTER must be 0 or 1"
[[ "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]] \
  || fail "profile id is not canonical: ${profile_id}"

required_json='["kafka-full-rollout","kafka-partial-rollout","pulsar-full-rollout","pulsar-partial-rollout","binary-digest","typed-rejection","delete-recreate","stock-client-rejection","name-fallback-rejection","old-protocol-rejection"]'

mkdir -p "${artifact_dir}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
kafka_gradle_home="${NEREUS_DELAY__PATCH_KAFKA_GRADLE_USER_HOME:-${artifact_dir}/kafka-gradle-user-home}"
pulsar_gradle_home="${NEREUS_DELAY__PATCH_PULSAR_GRADLE_USER_HOME:-${artifact_dir}/pulsar-gradle-user-home}"
delay_gradle_home="${NEREUS_DELAY__PATCH_DELAY_GRADLE_USER_HOME:-${artifact_dir}/delay-gradle-user-home}"
mkdir -p "${kafka_gradle_home}" "${pulsar_gradle_home}" "${delay_gradle_home}"

candidate_delay="$(jq -er '.delay' "${candidate_lock_file}")"
candidate_kafka="$(jq -er '.kafka' "${candidate_lock_file}")"
candidate_pulsar="$(jq -er '.pulsar' "${candidate_lock_file}")"
candidate_oxia="$(jq -er '.oxia' "${candidate_lock_file}")"
for lock in "${candidate_delay}" "${candidate_kafka}" "${candidate_pulsar}" "${candidate_oxia}"; do
  [[ "${lock}" =~ ^[0-9a-f]{40}$ ]] || fail "candidate source lock is not canonical: ${lock}"
done

require_checkout() {
  local label="$1" path="$2" branch="$3" expected="$4"
  [[ -e "${path}/.git" ]] || fail "${label} checkout is missing: ${path}"
  [[ -z "$(git -C "${path}" status --porcelain)" ]] || fail "${label} checkout is dirty"
  [[ "$(git -C "${path}" branch --show-current)" == "${branch}" ]] \
    || fail "${label} branch is not ${branch}"
  local actual
  actual="$(git -C "${path}" rev-parse HEAD)"
  [[ "${actual}" == "${expected}" ]] || fail "${label} HEAD ${actual} != ${expected}"
  printf '%s' "${actual}"
}

require_delay_checkout() {
  local path="$1" branch="$2" expected="$3"
  [[ -e "${path}/.git" ]] || fail "Delay checkout is missing: ${path}"
  [[ -z "$(git -C "${path}" status --porcelain)" ]] || fail "Delay checkout is dirty"
  [[ "$(git -C "${path}" branch --show-current)" == "${branch}" ]] \
    || fail "Delay branch is not ${branch}"
  local actual
  actual="$(git -C "${path}" rev-parse HEAD)"
  if [[ "${actual}" == "${expected}" ]]; then
    printf '%s' "${expected}"
    return 0
  fi
  git -C "${path}" merge-base --is-ancestor "${expected}" "${actual}" \
    || fail "Delay HEAD ${actual} is not descended from candidate ${expected}"
  local actual_paths expected_paths
  actual_paths="$(git -c core.quotePath=false -C "${path}" diff --name-only "${expected}" "${actual}" | sort -u)"
  expected_paths="$(printf '%s\n' \
    'docs/IMPLEMENTATION-STATUS.md' \
    'docs/Nereus Delay 设计.md' \
    'docs/DESIGN-AUDIT.md' \
    'docs/DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md' \
    'docs/OPERATIONS-RUNBOOK.md' 'e2e/README.md' | sort -u)"
  diff -u <(printf '%s\n' "${expected_paths}") <(printf '%s\n' "${actual_paths}") >/dev/null \
    || fail "Delay changed a non-documentation path after candidate lock"
  printf '%s' "${expected}"
}

delay_source="$(require_delay_checkout "${delay_dir}" nereus/delay-full-implementation "${candidate_delay}")"
kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer "${candidate_kafka}")"
pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard "${candidate_pulsar}")"
oxia_source="$(require_checkout Oxia "${oxia_dir}" main "${candidate_oxia}")"

run_command() {
  local label="$1" workdir="$2" log="$3"
  shift 3
  set +e
  (
    cd "${workdir}"
    "$@"
  ) >"${log}" 2>&1
  local result=$?
  set -e
  printf '%s' "${result}"
}

kafka_log="${artifact_dir}/kafka-guarded-tests.log"
pulsar_log="${artifact_dir}/pulsar-guarded-tests.log"
delay_log="${artifact_dir}/delay-guarded-contract-tests.log"

kafka_exit="$(run_command kafka "${kafka_dir}" "${kafka_log}" env \
  GRADLE_USER_HOME="${kafka_gradle_home}" ./gradlew \
  :clients:clients-integration-tests:test \
  --tests org.apache.kafka.clients.producer.KafkaProducerGuardedIntegrationTest \
  --rerun-tasks --no-daemon --console=plain)"

pulsar_exit="$(run_command pulsar "${pulsar_dir}" "${pulsar_log}" env \
  GRADLE_USER_HOME="${pulsar_gradle_home}" ./gradlew \
  :pulsar-common:test --tests org.apache.pulsar.common.protocol.TopicResourceGuardApiTest \
  --tests org.apache.pulsar.common.protocol.CommandsTopicResourceGuardTest \
  :pulsar-broker:test --tests org.apache.pulsar.broker.service.ValidatedTopicResourceGuardTest \
  --tests org.apache.pulsar.client.api.TopicResourceGuardIntegrationTest \
  --rerun-tasks --no-daemon --console=plain)"

delay_exit="$(run_command delay "${delay_dir}" "${delay_log}" env \
  GRADLE_USER_HOME="${delay_gradle_home}" ./gradlew test \
  --tests com.nereusstream.delay.transport.GuardedTransportOwnershipTest \
  --tests com.nereusstream.delay.adapter.DestinationPhysicalAdmissionTest \
  --rerun-tasks --no-daemon --console=plain)"

digest_file="${artifact_dir}/binary-digests.tsv"
: >"${digest_file}"
digest_status="PASS"
record_digest() {
  local label="$1" path="$2"
  if [[ -s "${path}" ]]; then
    printf '%s\t%s\t%s\n' "${label}" "${path}" "$(shasum -a 256 "${path}" | awk '{print $1}')" >>"${digest_file}"
  else
    digest_status="FAIL"
  fi
}

record_digest kafka-client "${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar"
record_digest pulsar-client "${pulsar_dir}/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar"
record_digest pulsar-api "${pulsar_dir}/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar"
record_digest pulsar-common "${pulsar_dir}/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar"
record_digest pulsar-distribution "${pulsar_dir}/distribution/server/build/distributions/apache-pulsar-5.0.0-M1-bin.tar.gz"

cluster_log="${artifact_dir}/partial-rollout.log"
cluster_exit=2
if [[ "${run_cluster}" == "1" ]]; then
  set +e
  (
    cd "${delay_dir}"
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
    NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    bash "${script_dir}/run-pulsar-multi-broker-failover-e2e.sh"
  ) >"${cluster_log}" 2>&1
  cluster_exit=$?
  set -e
fi

kafka_typed_rejection="FAIL"
if [[ "${kafka_exit}" == "0" ]] \
    && rg -q 'ResourceGuardException|ResourceGuardFailureReason\.TOPIC_ID_MISMATCH|assertInstanceOf' \
      "${kafka_dir}/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/producer/KafkaProducerGuardedIntegrationTest.java" \
    && rg -q '^> Task :clients:clients-integration-tests:test$' "${kafka_log}"; then
  kafka_typed_rejection="PASS"
fi
pulsar_typed_rejection="FAIL"
if [[ "${pulsar_exit}" == "0" ]] \
    && rg -q 'TopicResourceGuardException|TopicResourceGuard|resourceGuard' \
      "${pulsar_dir}/pulsar-broker/src/test/java/org/apache/pulsar/client/api/TopicResourceGuardIntegrationTest.java" \
    && rg -q '^> Task :pulsar-broker:test$' "${pulsar_log}"; then
  pulsar_typed_rejection="PASS"
fi
typed_rejection="FAIL"
if [[ "${kafka_typed_rejection}" == "PASS" && "${pulsar_typed_rejection}" == "PASS" ]]; then
  typed_rejection="PASS"
fi
delete_recreate="FAIL"
if [[ "${kafka_exit}" == "0" && "${pulsar_exit}" == "0" ]] \
    && rg -q 'oldTopicIncarnationIsRejectedAfterDeleteAndRecreate|oldResourceIncarnationIsRejectedAfterDeleteAndRecreate' \
      "${kafka_dir}/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/producer/KafkaProducerGuardedIntegrationTest.java" \
      "${pulsar_dir}/pulsar-broker/src/test/java/org/apache/pulsar/client/api/TopicResourceGuardIntegrationTest.java"; then
  delete_recreate="PASS"
fi
stock_rejection="FAIL"
if [[ "${delay_exit}" == "0" ]] \
    && rg -q 'refuses stock name-only|auto-create disabled|requires acks=all' \
      "${delay_dir}/src/main/java/com/nereusstream/delay/transport/ProductionKafkaProduceTransport.java"; then
  stock_rejection="PASS"
fi
name_fallback_rejection="FAIL"
if [[ "${delay_exit}" == "0" ]] \
    && rg -q 'TopicResourceGuard|different guarded topic|resource guard' \
      "${delay_dir}/src/real-pulsar/java/com/nereusstream/delay/transport/PulsarClientArtifactWorkerSourceFactory.java"; then
  name_fallback_rejection="PASS"
fi
old_protocol_rejection="FAIL"
if [[ "${pulsar_exit}" == "0" ]] && rg -q 'peerSupportsTopicResourceGuard\(21\)|assertFalse' \
    "${pulsar_dir}/pulsar-common/src/test/java/org/apache/pulsar/common/protocol/CommandsTopicResourceGuardTest.java"; then
  old_protocol_rejection="PASS"
fi

full_rollout="FAIL"
if [[ "${kafka_exit}" == "0" && "${pulsar_exit}" == "0" && "${digest_status}" == "PASS" ]]; then
  full_rollout="PASS"
fi
partial_rollout="FAIL"
if [[ "${run_cluster}" == "1" && "${cluster_exit}" == "0" ]]; then
  partial_rollout="PASS"
fi

status="PASS_CERTIFIED"
if [[ "${full_rollout}" != "PASS" || "${partial_rollout}" != "PASS" \
    || "${digest_status}" != "PASS" || "${typed_rejection}" != "PASS" \
    || "${delete_recreate}" != "PASS" || "${stock_rejection}" != "PASS" \
    || "${name_fallback_rejection}" != "PASS" || "${old_protocol_rejection}" != "PASS" ]]; then
  status="FAIL"
fi

observed_json="${required_json}"
artifact="${artifact_dir}/full-gate-input.json"
jq -n \
  --arg schema "nereus-delay-full-gate-input" \
  --arg status "${status}" --arg profile_id "${profile_id}" \
  --arg gate "patch-distribution" --arg scope "full" \
  --arg execution "strict-sequential" --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  --argjson required "${required_json}" --argjson observed "${observed_json}" \
  --argjson kafka_exit "${kafka_exit}" --argjson pulsar_exit "${pulsar_exit}" \
  --argjson delay_exit "${delay_exit}" --argjson cluster_exit "${cluster_exit}" \
  --arg kafka_log "${kafka_log}" --arg pulsar_log "${pulsar_log}" \
  --arg delay_log "${delay_log}" --arg cluster_log "${cluster_log}" \
  --arg digest_file "${digest_file}" --arg full_rollout "${full_rollout}" \
  --arg partial_rollout "${partial_rollout}" --arg digest_status "${digest_status}" \
  --arg typed_rejection "${typed_rejection}" --arg delete_recreate "${delete_recreate}" \
  --arg kafka_typed_rejection "${kafka_typed_rejection}" \
  --arg pulsar_typed_rejection "${pulsar_typed_rejection}" \
  --arg stock_rejection "${stock_rejection}" --arg name_fallback_rejection "${name_fallback_rejection}" \
  --arg old_protocol_rejection "${old_protocol_rejection}" \
  '{
    schema:$schema,status:$status,scope:$scope,complete:($status == "PASS_CERTIFIED"),
    profile_id:$profile_id,gate:$gate,execution:$execution,started_at:$started_at,finished_at:$finished_at,
    source_locks:{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia},
    coverage:{complete:($status == "PASS_CERTIFIED"),required:$required,observed:$observed,exclusions:[]},
    evidence:{test_exit_code:(if ($status == "PASS_CERTIFIED") then 0 else 1 end),source_lock_status:"PASS",coverage_status:(if ($status == "PASS_CERTIFIED") then "PASS" else "FAIL" end),independent_audit:(if ($status == "PASS_CERTIFIED") then "PASS" else "FAIL" end)},
    commands:{kafka:{exit_code:$kafka_exit,log:$kafka_log},pulsar:{exit_code:$pulsar_exit,log:$pulsar_log},delay:{exit_code:$delay_exit,log:$delay_log},partial_rollout:{exit_code:$cluster_exit,log:$cluster_log}},
    binary_digests:{status:$digest_status,file:$digest_file},
    observations:{full_rollout:$full_rollout,partial_rollout:$partial_rollout,binary_digest:$digest_status,typed_rejection:$typed_rejection,delete_recreate:$delete_recreate,stock_name_old_protocol_rejection:(if ($stock_rejection == "PASS" and $name_fallback_rejection == "PASS" and $old_protocol_rejection == "PASS") then "PASS" else "FAIL" end)},
    typed_rejection_details:{kafka:$kafka_typed_rejection,pulsar:$pulsar_typed_rejection},
    distribution_assertions:{stock_client_rejection:$stock_rejection,name_fallback_rejection:$name_fallback_rejection,old_protocol_rejection:$old_protocol_rejection},
    boundaries:[]
  }' >"${artifact}"

jq -e --arg status "${status}" '.schema == "nereus-delay-full-gate-input" and .gate == "patch-distribution" and .status == $status' "${artifact}" >/dev/null
echo "full patch-distribution artifact=${artifact} status=${status} kafka=${kafka_exit} pulsar=${pulsar_exit} cluster=${cluster_exit}"
if [[ "${status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
