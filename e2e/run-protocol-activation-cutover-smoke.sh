#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
artifact_dir="${NEREUS_DELAY_PROTOCOL_ACTIVATION_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-protocol-activation.XXXXXX)}"
gradle_home="${NEREUS_DELAY_PROTOCOL_ACTIVATION_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
expected_branch="nereus/delay-full-implementation"

if ! command -v jq >/dev/null 2>&1; then
  echo "protocol activation smoke requires jq" >&2
  exit 1
fi
if [[ ! -e "${delay_dir}/.git" ]]; then
  echo "Delay checkout is missing: ${delay_dir}" >&2
  exit 1
fi
if [[ -n "$(git -C "${delay_dir}" status --porcelain)" ]]; then
  echo "protocol activation smoke requires a clean Delay worktree" >&2
  exit 1
fi
branch="$(git -C "${delay_dir}" branch --show-current)"
if [[ "${branch}" != "${expected_branch}" ]]; then
  echo "expected ${expected_branch}, got ${branch}" >&2
  exit 1
fi
if [[ -e "${artifact_dir}" && -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "activation artifact directory must be empty: ${artifact_dir}" >&2
  exit 1
fi

mkdir -p "${artifact_dir}" "${gradle_home}"
source_lock="$(git -C "${delay_dir}" rev-parse HEAD)"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log_file="${artifact_dir}/protocol-activation-cutover-gradle.log"
artifact="${artifact_dir}/protocol-activation-cutover.json"

test_names=(
  "com.nereusstream.delay.protocol.ProtocolActivationStateTest"
  "com.nereusstream.delay.protocol.ProtocolActivationCutoverContractTest"
  "com.nereusstream.delay.runtime.InitialRouteControlApplyTest"
  "com.nereusstream.delay.runtime.ProtocolVersionActivationApplyTest"
  "com.nereusstream.delay.runtime.CommandProtocolDedupeApplyTest"
  "com.nereusstream.delay.store.CheckpointRestoreCoordinatorTest"
)
test_args=()
for test_name in "${test_names[@]}"; do
  test_args+=(--tests "${test_name}")
done

echo "Delay source: ${branch}@${source_lock}"
echo "Artifact directory: ${artifact_dir}"
echo "Gradle user home: ${gradle_home}"
echo "Tests: ${test_names[*]}"

set +e
(
  cd "${delay_dir}"
  GRADLE_USER_HOME="${gradle_home}" ./gradlew test "${test_args[@]}" --rerun-tasks --no-daemon --console=plain
) >"${log_file}" 2>&1
test_status=$?
set -e
cat "${log_file}"

if [[ "${test_status}" == "0" ]]; then
  status="PASS_BOUNDED"
  detail="source-ordered local activation projection and restart/cutover tests passed"
else
  status="BLOCKED"
  detail="focused activation tests exited ${test_status}"
fi

jq -n \
  --arg schema "nereus-delay-protocol-activation-cutover" \
  --arg status "${status}" \
  --arg source "${source_lock}" \
  --arg branch "${branch}" \
  --arg started_at "${started_at}" \
  --arg detail "${detail}" \
  --arg log "${log_file}" \
  --argjson tests "$(printf '%s\n' "${test_names[@]}" | jq -R . | jq -s .)" \
  '{
    schema: $schema,
    status: $status,
    source_lock: $source,
    source_ref: $branch,
    started_at: $started_at,
    tests: ($tests | map({name: ., status: $status})),
    assertions: [
      "Initial Route kind-14 creates the empty activation projection atomically with its control snapshot and source position.",
      "A kind-1 marker records tuple, reader-set evidence, source position and mutation identity in canonical state.",
      "A non-baseline tuple is rejected before its marker and accepted after its source-ordered marker.",
      "The activation projection survives a Store restart with canonical bytes and digest validation.",
      "Writer-before-reader activation fails closed until every eligible Worker publishes the exact tuple.",
      "Downgrade packaging binds the activated marker, binary digests and fallback tuple without deleting the marker.",
      "The same payload bytes with a different protocol tuple remain a command conflict, not a dedupe hit.",
      "Checkpoint restore installs a new fenced Store incarnation and rejects stale restore authority."
    ],
    test_log: $log,
    detail: $detail,
    boundaries: [
      "PASS_BOUNDED is a local Delay Store projection receipt, not PASS_CERTIFIED.",
      "This smoke does not prove authenticated external Oxia Worker eligibility or Broker/Pulsar cutover.",
      "No Docker or external service is used by this runner; Docker image cleanup is not applicable."
    ]
  }' >"${artifact}"

if [[ "${test_status}" == "0" ]]; then
  jq -e --arg source "${source_lock}" \
    '.status == "PASS_BOUNDED" and .source_lock == $source and (.tests | length == 6)' \
    "${artifact}" >/dev/null
fi

echo "Protocol activation/cutover smoke artifact: ${artifact}"
echo "status=${status}"
if [[ "${test_status}" != "0" ]]; then
  exit "${test_status}"
fi
