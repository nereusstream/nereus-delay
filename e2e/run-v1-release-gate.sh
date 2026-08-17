#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-v1-release-gate.XXXXXX)}"
gradle_home="${NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
run_check="${NEREUS_DELAY_RELEASE_GATE_RUN_CHECK:-1}"
allow_not_ready="${NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY:-0}"
chaos_artifact="${NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT:-}"
capacity_artifact="${NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT:-}"
soak_artifact="${NEREUS_DELAY_RELEASE_GATE_SOAK_ARTIFACT:-}"
activation_artifact="${NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT:-}"
operations_artifact="${NEREUS_DELAY_RELEASE_GATE_OPERATIONS_ARTIFACT:-}"

if ! command -v jq >/dev/null 2>&1; then
  echo "V1 release gate requires jq" >&2
  exit 1
fi
if [[ "${run_check}" != "0" && "${run_check}" != "1" ]]; then
  echo "NEREUS_DELAY_RELEASE_GATE_RUN_CHECK must be 0 or 1" >&2
  exit 1
fi
if [[ "${allow_not_ready}" != "0" && "${allow_not_ready}" != "1" ]]; then
  echo "NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY must be 0 or 1" >&2
  exit 1
fi
mkdir -p "${artifact_dir}" "${gradle_home}"

checks_jsonl="${artifact_dir}/.release-gate-checks.jsonl"
: >"${checks_jsonl}"

add_check() {
  local name="$1"
  local status="$2"
  local detail="$3"
  local artifact="${4:-}"
  jq -n --arg name "${name}" --arg status "${status}" --arg detail "${detail}" \
    --arg artifact "${artifact}" \
    '{name: $name, status: $status, detail: $detail, artifact: $artifact}' >>"${checks_jsonl}"
}

checkout_source="unknown"
kafka_source="unknown"
pulsar_source="unknown"
oxia_source="unknown"
source_failures=0
check_checkout() {
  local label="$1"
  local path="$2"
  local expected_branch="$3"
  if [[ ! -e "${path}/.git" ]]; then
    add_check "source-${label}" "BLOCKED" "checkout missing: ${path}"
    source_failures=$((source_failures + 1))
    return
  fi
  local dirty
  dirty="$(git -C "${path}" status --porcelain)"
  if [[ -n "${dirty}" ]]; then
    add_check "source-${label}" "BLOCKED" "worktree is dirty: ${path}"
    source_failures=$((source_failures + 1))
    return
  fi
  local branch
  branch="$(git -C "${path}" branch --show-current)"
  if [[ "${branch}" != "${expected_branch}" ]]; then
    add_check "source-${label}" "BLOCKED" "expected branch ${expected_branch}, got ${branch}"
    source_failures=$((source_failures + 1))
    return
  fi
  local source
  source="$(git -C "${path}" rev-parse HEAD)"
  case "${label}" in
    delay) checkout_source="${source}" ;;
    kafka) kafka_source="${source}" ;;
    pulsar) pulsar_source="${source}" ;;
    oxia) oxia_source="${source}" ;;
  esac
  add_check "source-${label}" "PASS" "clean ${expected_branch}@${source}"
}

check_checkout delay "${delay_dir}" nereus/delay-full-implementation-v1
check_checkout kafka "${kafka_dir}" nereus/delay-guarded-producer-v1
check_checkout pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1
check_checkout oxia "${oxia_dir}" main

if [[ "${source_failures}" == "0" ]]; then
  set +e
  bash "${script_dir}/validate-cross-repo-contracts.sh" >"${artifact_dir}/cross-repo-validator.log" 2>&1
  validator_status=$?
  set -e
  if [[ "${validator_status}" == "0" ]]; then
    add_check "cross-repo-contracts" "PASS" "validator passed" "${artifact_dir}/cross-repo-validator.log"
  else
    add_check "cross-repo-contracts" "BLOCKED" "validator exited ${validator_status}" "${artifact_dir}/cross-repo-validator.log"
  fi
else
  add_check "cross-repo-contracts" "BLOCKED" "source locks are not trustworthy"
fi

if [[ "${run_check}" == "1" && "${source_failures}" == "0" ]]; then
  set +e
  GRADLE_USER_HOME="${gradle_home}" ./gradlew check --no-daemon --console=plain \
    >"${artifact_dir}/gradle-check.log" 2>&1
  gradle_status=$?
  set -e
  if [[ "${gradle_status}" == "0" ]]; then
    add_check "gradle-check" "PASS" "full Gradle check passed" "${artifact_dir}/gradle-check.log"
  else
    add_check "gradle-check" "BLOCKED" "Gradle check exited ${gradle_status}" "${artifact_dir}/gradle-check.log"
  fi
elif [[ "${run_check}" == "0" ]]; then
  add_check "gradle-check" "SKIPPED" "NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=0"
else
  add_check "gradle-check" "BLOCKED" "source locks are not trustworthy"
fi

check_certified_artifact() {
  local name="$1"
  local path="$2"
  if [[ -z "${path}" ]]; then
    add_check "${name}" "BLOCKED" "no artifact supplied; PASS_CERTIFIED is required"
    return
  fi
  if [[ ! -s "${path}" ]] || ! jq empty "${path}" >/dev/null 2>&1; then
    add_check "${name}" "BLOCKED" "missing or invalid JSON artifact" "${path}"
    return
  fi
  local status
  status="$(jq -r '.status // .matrix_status // "UNKNOWN"' "${path}")"
  if [[ "${status}" == "PASS_CERTIFIED" ]]; then
    add_check "${name}" "PASS" "artifact is PASS_CERTIFIED" "${path}"
  else
    add_check "${name}" "BLOCKED" "artifact status is ${status}; PASS_CERTIFIED is required" "${path}"
  fi
}

check_certified_artifact benchmark-capacity "${capacity_artifact}"
check_certified_artifact certified-soak "${soak_artifact}"
check_certified_artifact activation-cutover "${activation_artifact}"
check_certified_artifact operations-drills "${operations_artifact}"

if [[ -z "${chaos_artifact}" ]]; then
  add_check "chaos-matrix" "BLOCKED" "no canonical chaos artifact supplied; PASS_CERTIFIED is required"
elif [[ ! -s "${chaos_artifact}" ]] || ! jq empty "${chaos_artifact}" >/dev/null 2>&1; then
  add_check "chaos-matrix" "BLOCKED" "missing or invalid JSON artifact" "${chaos_artifact}"
else
  chaos_status="$(jq -r '.matrix_status // .status // "UNKNOWN"' "${chaos_artifact}")"
  if [[ "${chaos_status}" == "PASS_CERTIFIED" ]]; then
    add_check "chaos-matrix" "PASS" "artifact is PASS_CERTIFIED" "${chaos_artifact}"
  else
    add_check "chaos-matrix" "BLOCKED" "artifact status is ${chaos_status}; PASS_CERTIFIED is required" "${chaos_artifact}"
  fi
fi

checks_artifact="${artifact_dir}/release-gate-checks.json"
jq -s '.' "${checks_jsonl}" >"${checks_artifact}"
release_artifact="${artifact_dir}/v1-release-candidate-gate.json"
jq -n \
  --arg schema "nereus-delay-v1-release-candidate-gate-v1" \
  --arg status "$(jq -r 'if all(.[]; .status == "PASS") then "PASS" else "NOT_READY" end' "${checks_artifact}")" \
  --arg delay "${checkout_source}" \
  --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" \
  --arg oxia "${oxia_source}" \
  --arg artifact "${artifact_dir}" \
  --slurpfile checks "${checks_artifact}" \
  '{
    schema: $schema,
    release_status: $status,
    artifact_dir: $artifact,
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    checks: $checks[0],
    boundaries: [
      "All release evidence is source-locked and must be fresh for the same release candidate.",
      "PASS_BOUNDED, PARTIAL, SKIPPED and missing artifacts never satisfy PASS_CERTIFIED.",
      "A NOT_READY result is an intentional fail-closed release decision, not a failure of the bounded slices already recorded."
    ]
  }' >"${release_artifact}"
rm -f "${checks_jsonl}"

release_status="$(jq -r '.release_status' "${release_artifact}")"
echo "V1 release-candidate gate artifact: ${release_artifact}"
echo "release_status=${release_status}"
jq -r '.checks[] | "\(.name)=\(.status): \(.detail)"' "${release_artifact}"
if [[ "${release_status}" != "PASS" && "${allow_not_ready}" != "1" ]]; then
  echo "V1 release gate is NOT_READY; set NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 only to retain the audit artifact" >&2
  exit 1
fi
