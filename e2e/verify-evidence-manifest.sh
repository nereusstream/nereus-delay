#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# The candidate source lock is frozen before the final evidence run.  A later
# documentation-only overlay may record the resulting receipts, but it is
# bound by this manifest: only the six evidence ledgers may differ, their
# current bytes are hashed, and every artifact is hashed and source-checked.
# The manifest itself must live outside the Delay checkout so it cannot hash
# itself through a documentation commit.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
manifest="${NEREUS_DELAY_EVIDENCE_MANIFEST:-}"

fail() {
  echo " evidence manifest: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v git >/dev/null 2>&1 || fail "git is required"
command -v shasum >/dev/null 2>&1 || fail "shasum is required"
[[ -n "${manifest}" ]] || fail "NEREUS_DELAY_EVIDENCE_MANIFEST is required"
[[ -s "${manifest}" ]] || fail "manifest is missing or empty: ${manifest}"
jq empty "${manifest}" >/dev/null 2>&1 || fail "manifest is not valid JSON: ${manifest}"

[[ "$(jq -r '.schema // empty' "${manifest}")" == "nereus-delay-evidence-manifest" ]] \
  || fail "unsupported manifest schema"

candidate_delay="$(jq -er '.candidate_source_lock.delay' "${manifest}")"
candidate_kafka="$(jq -er '.candidate_source_lock.kafka' "${manifest}")"
candidate_pulsar="$(jq -er '.candidate_source_lock.pulsar' "${manifest}")"
candidate_oxia="$(jq -er '.candidate_source_lock.oxia' "${manifest}")"

[[ "${candidate_delay}" =~ ^[0-9a-f]{40}$ ]] || fail "candidate Delay SHA is not canonical"
[[ "${candidate_kafka}" =~ ^[0-9a-f]{40}$ ]] || fail "candidate Kafka SHA is not canonical"
[[ "${candidate_pulsar}" =~ ^[0-9a-f]{40}$ ]] || fail "candidate Pulsar SHA is not canonical"
[[ "${candidate_oxia}" =~ ^[0-9a-f]{40}$ ]] || fail "candidate Oxia SHA is not canonical"

expected_docs=(
  "docs/IMPLEMENTATION-STATUS.md"
  "docs/Nereus Delay 设计.md"
  "docs/DESIGN-AUDIT.md"
  "docs/DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md"
  "docs/OPERATIONS-RUNBOOK.md"
  "e2e/README.md"
)

assert_clean_checkout() {
  local label="$1"
  local checkout="$2"
  local branch="$3"
  [[ -e "${checkout}/.git" ]] || fail "${label} checkout is not a Git worktree: ${checkout}"
  [[ -z "$(git -C "${checkout}" status --porcelain)" ]] \
    || fail "${label} checkout is dirty: ${checkout}"
  [[ "$(git -C "${checkout}" branch --show-current)" == "${branch}" ]] \
    || fail "${label} branch is not ${branch}"
}

assert_clean_checkout delay "${delay_dir}" nereus/delay-full-implementation
assert_clean_checkout kafka "${kafka_dir}" nereus/delay-guarded-producer
assert_clean_checkout pulsar "${pulsar_dir}" nereus/delay-resource-guard
assert_clean_checkout oxia "${oxia_dir}" main

current_delay="$(git -C "${delay_dir}" rev-parse HEAD)"
current_kafka="$(git -C "${kafka_dir}" rev-parse HEAD)"
current_pulsar="$(git -C "${pulsar_dir}" rev-parse HEAD)"
current_oxia="$(git -C "${oxia_dir}" rev-parse HEAD)"

[[ "${current_kafka}" == "${candidate_kafka}" ]] \
  || fail "Kafka HEAD ${current_kafka} differs from candidate ${candidate_kafka}"
[[ "${current_pulsar}" == "${candidate_pulsar}" ]] \
  || fail "Pulsar HEAD ${current_pulsar} differs from candidate ${candidate_pulsar}"
[[ "${current_oxia}" == "${candidate_oxia}" ]] \
  || fail "Oxia HEAD ${current_oxia} differs from candidate ${candidate_oxia}"

overlay_delay="$(jq -r '.evidence_overlay.delay_commit // empty' "${manifest}")"
if [[ -z "${overlay_delay}" ]]; then
  [[ "${current_delay}" == "${candidate_delay}" ]] \
    || fail "Delay HEAD ${current_delay} differs from candidate ${candidate_delay}"
else
  [[ "${overlay_delay}" =~ ^[0-9a-f]{40}$ ]] || fail "overlay Delay SHA is not canonical"
  [[ "${current_delay}" == "${overlay_delay}" ]] \
    || fail "Delay HEAD ${current_delay} differs from evidence overlay ${overlay_delay}"
  git -C "${delay_dir}" merge-base --is-ancestor "${candidate_delay}" "${overlay_delay}" \
    || fail "evidence overlay is not a descendant of the candidate source lock"

  actual_paths="$(git -c core.quotePath=false -C "${delay_dir}" diff --name-only "${candidate_delay}" "${overlay_delay}" | sort -u)"
  expected_paths="$(jq -er '.evidence_overlay.allowed_paths | if type == "array" then .[] else error end' "${manifest}" | sort -u)"
  if ! diff -u <(printf '%s\n' "${expected_paths}") <(printf '%s\n' "${actual_paths}") >/dev/null; then
    fail "Delay evidence overlay changed a non-allowlisted path"
  fi
fi

actual_paths="$(jq -er '.evidence_overlay.allowed_paths | if type == "array" then .[] else error end' "${manifest}" | sort -u)"
expected_paths="$(printf '%s\n' "${expected_docs[@]}" | sort -u)"
if ! diff -u <(printf '%s\n' "${expected_paths}") <(printf '%s\n' "${actual_paths}") >/dev/null; then
  fail "evidence_overlay.allowed_paths must be exactly the six evidence ledgers"
fi

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

for doc in "${expected_docs[@]}"; do
  expected_sha="$(jq -er --arg doc "${doc}" '.evidence_overlay.docs_sha256[$doc]' "${manifest}")"
  [[ "${expected_sha}" =~ ^[0-9a-f]{64}$ ]] || fail "invalid documentation SHA for ${doc}"
  doc_path="${delay_dir}/${doc}"
  [[ -f "${doc_path}" ]] || fail "evidence ledger is missing: ${doc}"
  actual_sha="$(sha256_file "${doc_path}")"
  [[ "${actual_sha}" == "${expected_sha}" ]] \
    || fail "evidence ledger bytes changed after manifest freeze: ${doc}"
done

actual_doc_keys="$(jq -er '.evidence_overlay.docs_sha256 | if type == "object" then keys[] else error end' "${manifest}" | sort -u)"
expected_doc_keys="$(printf '%s\n' "${expected_docs[@]}" | sort -u)"
if ! diff -u <(printf '%s\n' "${expected_doc_keys}") <(printf '%s\n' "${actual_doc_keys}") >/dev/null; then
  fail "evidence_overlay.docs_sha256 must contain exactly the six evidence ledgers"
fi

lock_json="$(jq -cn \
  --arg delay "${candidate_delay}" \
  --arg kafka "${candidate_kafka}" \
  --arg pulsar "${candidate_pulsar}" \
  --arg oxia "${candidate_oxia}" \
  '{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}')"

verify_artifact() {
  local label="$1"
  local path="$2"
  local expected_sha="$3"
  local expected_status="$4"
  local expected_lock_kind="$5"
  local expected_locks="${lock_json}"

  [[ -f "${path}" ]] || fail "${label} artifact is missing: ${path}"
  [[ "$(sha256_file "${path}")" == "${expected_sha}" ]] \
    || fail "${label} artifact bytes changed: ${path}"
  jq empty "${path}" >/dev/null 2>&1 || fail "${label} artifact is not JSON: ${path}"
  actual_status="$(jq -r 'if .status != null then .status elif .release_status != null then .release_status else empty end' "${path}")"
  [[ "${actual_status}" == "${expected_status}" ]] \
    || fail "${label} status ${actual_status} != ${expected_status}"
  if [[ "${expected_lock_kind}" == "overlay" ]]; then
    [[ -n "${overlay_delay}" ]] || fail "${label} requests overlay lock without an evidence overlay"
    expected_locks="$(jq -cn --arg delay "${overlay_delay}" --arg kafka "${current_kafka}" --arg pulsar "${current_pulsar}" --arg oxia "${current_oxia}" '{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}')"
  elif [[ "${expected_lock_kind}" != "candidate" ]]; then
    fail "${label} has unsupported source_lock_kind ${expected_lock_kind}"
  fi
  jq -e --argjson expected "${expected_locks}" \
    '.source_locks == $expected' "${path}" >/dev/null 2>&1 \
    || fail "${label} source_locks do not match ${expected_lock_kind} lock"

  if [[ "${label}" == gate:* ]]; then
    gate_name="${label#gate:}"
    jq -e --arg gate "${gate_name}" \
      '.schema == "nereus-delay-full-gate-input"
       and .status == "PASS_CERTIFIED"
       and .scope == "full"
       and .complete == true
       and .gate == $gate
       and .execution == "strict-sequential"
       and .coverage.complete == true
       and (.coverage.exclusions | type == "array" and length == 0)
       and .evidence.test_exit_code == 0
       and .evidence.source_lock_status == "PASS"
       and .evidence.coverage_status == "PASS"
       and .evidence.independent_audit == "PASS"
       and (.boundaries | type == "array" and length == 0)' "${path}" >/dev/null 2>&1 \
      || fail "${label} is not a complete full PASS_CERTIFIED artifact"
  fi
}

gate_count="$(jq -er '.gates | if type == "object" then length else error end' "${manifest}")"
[[ "${gate_count}" == "10" ]] || fail "manifest must contain exactly ten certified gate inputs"
required_gates=(
  protocol-golden
  chaos
  real-service
  no-early
  benchmark
  capacity
  soak
  upgrade-downgrade
  operations
  patch-distribution
)
for gate in "${required_gates[@]}"; do
  gate_path="$(jq -er --arg gate "${gate}" '.gates[$gate].artifact_path' "${manifest}")"
  gate_sha="$(jq -er --arg gate "${gate}" '.gates[$gate].sha256' "${manifest}")"
  gate_status="$(jq -er --arg gate "${gate}" '.gates[$gate].expected_status' "${manifest}")"
  gate_lock_kind="$(jq -r --arg gate "${gate}" '.gates[$gate].source_lock_kind // "candidate"' "${manifest}")"
  [[ "${gate_status}" == "PASS_CERTIFIED" ]] || fail "${gate} must require PASS_CERTIFIED"
  verify_artifact "gate:${gate}" "${gate_path}" "${gate_sha}" "${gate_status}" "${gate_lock_kind}"
done

final_path="$(jq -er '.final_release_gate.artifact_path' "${manifest}")"
final_sha="$(jq -er '.final_release_gate.sha256' "${manifest}")"
final_status="$(jq -er '.final_release_gate.expected_status' "${manifest}")"
final_lock_kind="$(jq -r '.final_release_gate.source_lock_kind // "overlay"' "${manifest}")"
[[ "${final_status}" == "PASS" ]] || fail "final release gate must require PASS"
verify_artifact "final-release-gate" "${final_path}" "${final_sha}" "${final_status}" "${final_lock_kind}"
jq -e \
  '.schema == "nereus-delay-release-gate"
   and .scope == "full"
   and .release_status == "PASS"
   and ((.required_gates | type) == "array")
   and ((.required_gates | length) == 10)
   and (([.checks[] | select(.status != "PASS")] | length) == 0)' "${final_path}" >/dev/null 2>&1 \
  || fail "final release gate is not the ten-gate full PASS artifact"

echo " evidence manifest PASS"
echo "candidate_source_lock.delay=${candidate_delay}"
echo "evidence_overlay.delay_commit=${overlay_delay:-none}"
echo "certified_gate_inputs=${gate_count}"
echo "final_release_status=${final_status}"
