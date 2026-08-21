#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# Validate the physical §23.4 matrix produced by an approved load/telemetry
# harness.  This is intentionally separate from the bounded JVM probe: a
# caller cannot turn a small local Store run into a V1 capacity envelope by
# changing labels in the wrapper receipt.

artifact="${1:-}"
candidate_lock_file="${2:-}"

fail() {
  echo "V1 capacity matrix: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v shasum >/dev/null 2>&1 || fail "shasum is required"
[[ -s "${artifact}" ]] || fail "matrix artifact is missing or empty: ${artifact}"
[[ -s "${candidate_lock_file}" ]] || fail "candidate source lock is missing or empty: ${candidate_lock_file}"
jq empty "${artifact}" >/dev/null 2>&1 || fail "matrix artifact is invalid JSON"
jq empty "${candidate_lock_file}" >/dev/null 2>&1 || fail "candidate source lock is invalid JSON"

delay_source="$(jq -er '.delay' "${candidate_lock_file}")"
kafka_source="$(jq -er '.kafka' "${candidate_lock_file}")"
pulsar_source="$(jq -er '.pulsar' "${candidate_lock_file}")"
oxia_source="$(jq -er '.oxia' "${candidate_lock_file}")"
for lock in "${delay_source}" "${kafka_source}" "${pulsar_source}" "${oxia_source}"; do
  [[ "${lock}" =~ ^[0-9a-f]{40}$ ]] || fail "candidate source lock is not canonical: ${lock}"
done

locks_json="$(jq -cn --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  '{delay:$delay,kafka:$kafka,pulsar:$pulsar,oxia:$oxia}')"

jq -e --argjson locks "${locks_json}" \
  '.schema == "nereus-delay-v1-capacity-matrix-v1"
   and .status == "PASS"
   and .source_locks == $locks
   and (.profile_id | type == "string" and length > 0)
   and (.dimensions | type == "object")
   and ((.dimensions.record_cardinalities | sort) == [1000000,10000000,100000000])
   and ((.dimensions.arrival_patterns | sort | unique) == ["burst","uniform","zipf"])
   and ((.dimensions.ordering_modes | sort | unique) == ["ordered","unordered"])
   and ((.dimensions.consistency_modes | sort | unique) == ["baseline","strong"])
   and ((.dimensions.target_health | sort | unique) == ["bad","healthy"])
   and ((.dimensions.placement_modes | sort | unique) == ["multi-shard","single-shard"])
   and ((.dimensions.payload_modes | sort | unique) == ["inline","object"])
   and (.observations | type == "array" and length >= 8)
   and all(.observations[];
     .status == "PASS"
     and (.id | type == "string" and length > 0)
     and (.configuration | type == "object")
     and (.metrics | type == "object" and length >= 3)
     and (.invariants | type == "array" and length >= 3)
     and all(.invariants[]; .status == "PASS" or . == "PASS")
     and (.provenance | type == "object")
     and (.provenance.source_locks == $locks)
     and (.provenance.commands | type == "array" and length >= 1)
     and (.provenance.artifacts | type == "array" and length >= 1)
     and (.provenance.artifact_sha256 | type == "array"
          and length == (.provenance.artifacts | length)
          and all(.[]; test("^[0-9a-f]{64}$")))
     and (.provenance.exit_codes | type == "array"
          and length >= 1 and all(.[]; .exit_code == 0)))
   and (.capacity_envelope.status == "PASS")
   and (.capacity_envelope.config_file | type == "string" and length > 0)
   and (.capacity_envelope.config_sha256 | test("^[0-9a-f]{64}$"))' \
  "${artifact}" >/dev/null 2>&1 \
  || fail "matrix does not satisfy the source-locked §23.4 contract"

# The JSON shape is not enough: every referenced receipt must exist and match
# its recorded digest.  This also catches a copied measurement with a changed
# source log or a missing config file.
while IFS=$'\t' read -r path expected_hash; do
  [[ -n "${path}" && "${path}" != "null" ]] || fail "matrix contains an empty artifact path"
  [[ -s "${path}" ]] || fail "matrix evidence artifact is missing or empty: ${path}"
  actual_hash="$(shasum -a 256 "${path}" | awk '{print $1}')"
  [[ "${actual_hash}" == "${expected_hash}" ]] \
    || fail "matrix evidence hash mismatch: ${path}"
done < <(jq -r '.observations[] | .provenance as $p | range(0; ($p.artifacts | length)) | [$p.artifacts[.], $p.artifact_sha256[.]] | @tsv' "${artifact}")

config_file="$(jq -er '.capacity_envelope.config_file' "${artifact}")"
config_hash="$(jq -er '.capacity_envelope.config_sha256' "${artifact}")"
[[ -s "${config_file}" ]] || fail "capacity envelope config is missing or empty: ${config_file}"
actual_config_hash="$(shasum -a 256 "${config_file}" | awk '{print $1}')"
[[ "${actual_config_hash}" == "${config_hash}" ]] \
  || fail "capacity envelope config hash mismatch: ${config_file}"

echo "V1 capacity matrix validated: ${artifact}"
echo "profile=$(jq -r '.profile_id' "${artifact}") observations=$(jq -r '.observations | length' "${artifact}")"
