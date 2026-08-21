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
artifact_dir="${NEREUS_DELAY_V1_PROTOCOL_GOLDEN_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-v1-protocol-golden.XXXXXX)}"
gradle_home="${NEREUS_DELAY_V1_PROTOCOL_GOLDEN_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
log_file="${artifact_dir}/protocol-golden-gradle.log"
artifact="${artifact_dir}/protocol-golden.json"

fail() { echo "V1 protocol golden gate: $*" >&2; exit 1; }
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
if [[ -n "$(git -C "${delay_dir}" status --porcelain)" || -n "$(git -C "${kafka_dir}" status --porcelain)" \
    || -n "$(git -C "${pulsar_dir}" status --porcelain)" || -n "$(git -C "${oxia_dir}" status --porcelain)" ]]; then
  source_status="FAIL"
fi
[[ "$(git -C "${delay_dir}" branch --show-current)" == "nereus/delay-full-implementation-v1" ]] || source_status=FAIL
[[ "$(git -C "${kafka_dir}" branch --show-current)" == "nereus/delay-guarded-producer-v1" ]] || source_status=FAIL
[[ "$(git -C "${pulsar_dir}" branch --show-current)" == "nereus/delay-resource-guard-v1" ]] || source_status=FAIL
[[ "$(git -C "${oxia_dir}" branch --show-current)" == "main" ]] || source_status=FAIL
[[ "${current_delay}" == "${candidate_delay}" && "${current_kafka}" == "${candidate_kafka}" \
    && "${current_pulsar}" == "${candidate_pulsar}" && "${current_oxia}" == "${candidate_oxia}" ]] || source_status=FAIL

required=(
  ndl1 crc enums version-bound-hash signature identity protobuf-golden jcs uint64
  key-ordering state-golden kafka-lso-boundary kafka-empty-boundary pulsar-inclusive-boundary
  pulsar-strictness model-property-interleavings
)
test_patterns=(
  'io.nereusstream.delay.protocol.*'
  'io.nereusstream.delay.store.KeyCodecTest'
  'io.nereusstream.delay.store.ValueEnvelopeTest'
  'io.nereusstream.delay.store.CheckpointManifestTest'
  'io.nereusstream.delay.runtime.TrustedUtcClockTest'
  'io.nereusstream.delay.client.AutoFastScheduleTest'
  'io.nereusstream.delay.runtime.ProfileCatalogV1ScheduleResolverTest'
)

source_audit_status="PASS"
source_audit_files=(
  "src/test/java/io/nereusstream/delay/protocol/ProtocolCodecTest.java"
  "src/test/java/io/nereusstream/delay/protocol/CanonicalProtobufTest.java"
  "src/test/java/io/nereusstream/delay/protocol/CommandProtocolTupleTest.java"
  "src/test/java/io/nereusstream/delay/protocol/SourceActivationBarrierTest.java"
  "src/test/java/io/nereusstream/delay/store/KeyCodecTest.java"
  "src/test/java/io/nereusstream/delay/store/ValueEnvelopeTest.java"
  "src/test/java/io/nereusstream/delay/store/CheckpointManifestTest.java"
  "src/test/java/io/nereusstream/delay/runtime/TrustedUtcClockTest.java"
  "src/test/java/io/nereusstream/delay/client/AutoFastScheduleTest.java"
)
for file in "${source_audit_files[@]}"; do
  [[ -s "${delay_dir}/${file}" ]] || source_audit_status="FAIL"
done
rg -Fq 'frameZeroVectorMatchesRegistry' "${delay_dir}/src/test/java/io/nereusstream/delay/protocol/ProtocolCodecTest.java" || source_audit_status=FAIL
rg -Fq 'receiptFrameZeroVectorMatchesRegistry' "${delay_dir}/src/test/java/io/nereusstream/delay/protocol/ProtocolCodecTest.java" || source_audit_status=FAIL
rg -Fq 'uint64BitsRoundTripsTheHighBitPattern' "${delay_dir}/src/test/java/io/nereusstream/delay/protocol/CanonicalProtobufTest.java" || source_audit_status=FAIL
rg -Fq 'pulsarBarrierPinsBatchShapeForTheInclusiveEntry' "${delay_dir}/src/test/java/io/nereusstream/delay/protocol/SourceActivationBarrierTest.java" || source_audit_status=FAIL
rg -Fq 'commandHashBindsTheProtocolTuple' "${delay_dir}/src/test/java/io/nereusstream/delay/protocol/CommandProtocolTupleTest.java" || source_audit_status=FAIL
rg -Fq 'canonical JCS' "${delay_dir}/src/main/java/io/nereusstream/delay/store/CheckpointManifest.java" || source_audit_status=FAIL

cross_repo_status="BLOCKED"
if [[ "${source_status}" == PASS ]]; then
  set +e
  bash "${script_dir}/validate-cross-repo-contracts.sh" >"${artifact_dir}/cross-repo-validator.log" 2>&1
  cross_repo_exit=$?
  set -e
  [[ "${cross_repo_exit}" == 0 ]] && cross_repo_status=PASS
fi

test_exit_code=1
tests_started=0
if [[ "${source_status}" == PASS && "${source_audit_status}" == PASS && "${cross_repo_status}" == PASS ]]; then
  test_args=()
  for pattern in "${test_patterns[@]}"; do test_args+=(--tests "${pattern}"); done
  set +e
  (
    cd "${delay_dir}"
    GRADLE_USER_HOME="${gradle_home}" ./gradlew clean test "${test_args[@]}" --rerun-tasks --no-daemon --console=plain
  ) >"${log_file}" 2>&1
  test_exit_code=$?
  tests_started=1
  set -e
else
  echo "source, source-audit or cross-repo validation failed; protocol tests were not started" >"${log_file}"
fi

test_count=0
failure_count=0
error_count=0
skipped_count=0
if [[ "${tests_started}" == 1 && -d "${delay_dir}/build/test-results/test" ]]; then
  test_count="$(rg -h -o '<testcase([ >])' "${delay_dir}/build/test-results/test" -g 'TEST-*.xml' | wc -l | tr -d ' ')"
  failure_count="$(rg -h -o '<failure([ >])' "${delay_dir}/build/test-results/test" -g 'TEST-*.xml' | wc -l | tr -d ' ')"
  error_count="$(rg -h -o '<error([ >])' "${delay_dir}/build/test-results/test" -g 'TEST-*.xml' | wc -l | tr -d ' ')"
  skipped_count="$(rg -h -o '<skipped([ >])' "${delay_dir}/build/test-results/test" -g 'TEST-*.xml' | wc -l | tr -d ' ')"
fi

status="BLOCKED"
if [[ "${source_status}" == PASS && "${source_audit_status}" == PASS && "${cross_repo_status}" == PASS \
    && "${test_exit_code}" == 0 && "${test_count}" -gt 0 && "${failure_count}" == 0 \
    && "${error_count}" == 0 && "${skipped_count}" == 0 ]]; then
  status="PASS_CERTIFIED"
fi
required_json="$(printf '%s\n' "${required[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
jq -n \
  --arg schema "nereus-delay-v1-full-gate-input-v1" --arg gate "protocol-golden" \
  --arg status "${status}" --arg delay "${candidate_delay}" --arg kafka "${candidate_kafka}" \
  --arg pulsar "${candidate_pulsar}" --arg oxia "${candidate_oxia}" --arg log "${log_file}" \
  --argjson test_exit_code "${test_exit_code}" --argjson test_count "${test_count}" \
  --argjson failure_count "${failure_count}" --argjson error_count "${error_count}" \
  --argjson skipped_count "${skipped_count}" --argjson required "${required_json}" \
  --arg source_status "${source_status}" --arg source_audit_status "${source_audit_status}" \
  --arg cross_repo_status "${cross_repo_status}" \
  '{
    schema:$schema,status:$status,scope:"full-v1",complete_v1:($status == "PASS_CERTIFIED"),
    gate:$gate,execution:"strict-sequential",
    source_locks:{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia},
    coverage:{complete_v1:($status == "PASS_CERTIFIED"),required:$required,observed:(if $status == "PASS_CERTIFIED" then $required else [] end),exclusions:[]},
    evidence:{test_exit_code:$test_exit_code,test_count:$test_count,failure_count:$failure_count,error_count:$error_count,skipped_count:$skipped_count,source_lock_status:$source_status,source_audit_status:$source_audit_status,cross_repo_status:$cross_repo_status,coverage_status:(if $status == "PASS_CERTIFIED" then "PASS" else "BLOCKED" end),independent_audit:(if $status == "PASS_CERTIFIED" then "PASS" else "BLOCKED" end),test_log:$log},
    assertions:["NDL1/NDR1 CRC and enum/version checks","canonical Protobuf and JCS/uint64 vectors","identity and registered key ordering","Kafka LSO/empty boundaries","Pulsar inclusive MessageId/batch and strictness boundaries","state/property interleaving and activation vectors"],
    boundaries:[]
  }' >"${artifact}"

echo "V1 protocol golden artifact: ${artifact}"
echo "status=${status} tests=${test_count} failures=${failure_count} errors=${error_count} skipped=${skipped_count}"
if [[ "${status}" != PASS_CERTIFIED ]]; then
  exit 1
fi
