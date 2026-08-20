#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# This wrapper is intentionally stricter than the bounded matrix.  It never
# promotes marker-only cells: every required cell must carry a durable
# before/after dump, fresh-process recovery and an independent invariant audit
# before the named chaos profile can become PASS_CERTIFIED.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_CERTIFIED_CHAOS_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-certified-chaos.XXXXXX)}"
bounded_dir="${artifact_dir}/bounded-chaos"
gradle_home="${NEREUS_DELAY_CERTIFIED_CHAOS_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
profile_id="${NEREUS_DELAY_CERTIFIED_CHAOS_PROFILE_ID:-}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if ! command -v jq >/dev/null 2>&1; then
  echo "certified chaos matrix requires jq" >&2
  exit 1
fi
if [[ -z "${profile_id}" ]]; then
  echo "NEREUS_DELAY_CERTIFIED_CHAOS_PROFILE_ID is required" >&2
  exit 1
fi
if [[ ! "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]]; then
  echo "certified chaos profile id is not canonical: ${profile_id}" >&2
  exit 1
fi
if [[ -e "${artifact_dir}" && -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "certified chaos artifact directory must be empty: ${artifact_dir}" >&2
  exit 1
fi
mkdir -p "${artifact_dir}" "${bounded_dir}" "${gradle_home}"

required_cells=(
  kafka-broker-process-crash
  kafka-worker-ack-process-crash
  kafka-broker-tcp-cut
  kafka-broker-network-partition
  pulsar-worker-process-crash
  pulsar-multi-broker-process-crash
  pulsar-worker-admission-response-loss
  pulsar-worker-destination-response-loss
  checkpoint-reaping
  kafka-fetch-response-loss
  kafka-retention-floor
  pulsar-destination-response-loss
  pulsar-source-ack-response-loss
  gateway-oxia-session-churn
)

require_checkout() {
  local label="$1"
  local path="$2"
  local branch="$3"
  if [[ ! -e "${path}/.git" ]] \
      || [[ -n "$(git -C "${path}" status --porcelain)" ]] \
      || [[ "$(git -C "${path}" branch --show-current)" != "${branch}" ]]; then
    return 1
  fi
  git -C "${path}" rev-parse HEAD
}

source_status="PASS"
delay_source="unknown"
kafka_source="unknown"
pulsar_source="unknown"
oxia_source="unknown"
if ! delay_source="$(require_checkout Delay "${delay_dir}" nereus/delay-full-implementation-v1)"; then source_status="BLOCKED"; fi
if ! kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer-v1)"; then source_status="BLOCKED"; fi
if ! pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1)"; then source_status="BLOCKED"; fi
if ! oxia_source="$(require_checkout Oxia "${oxia_dir}" main)"; then source_status="BLOCKED"; fi

bounded_artifact="${bounded_dir}/bounded-chaos-matrix.json"
bounded_exit=1
if [[ "${source_status}" == "PASS" ]]; then
  set +e
  NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR="${bounded_dir}" \
  NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME="${gradle_home}" \
  NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
  NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
  NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
  bash "${script_dir}/run-bounded-chaos-matrix.sh" \
    >"${artifact_dir}/bounded-chaos.log" 2>&1
  bounded_exit=$?
  set -e
else
  echo "certified chaos: source checks blocked; bounded child was not started" \
    >"${artifact_dir}/bounded-chaos.log"
fi

bounded_status="MISSING"
bounded_locks_match="FAIL"
marker_status="FAIL"
durable_status="FAIL"
fresh_process_status="FAIL"
invariant_status="FAIL"
if [[ -s "${bounded_artifact}" ]] && jq empty "${bounded_artifact}" >/dev/null 2>&1; then
  bounded_status="$(jq -r '.matrix_status // "UNKNOWN"' "${bounded_artifact}")"
  if jq -e --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
      --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
      '.source_locks.delay == $delay
       and .source_locks.kafka == $kafka
       and .source_locks.pulsar == $pulsar
       and .source_locks.oxia == $oxia' "${bounded_artifact}" >/dev/null 2>&1; then
    bounded_locks_match="PASS"
  fi
  if jq -e --argjson required "$(printf '%s\n' "${required_cells[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')" \
      '(.cells | type == "object")
       and ([.cells | keys[]] | sort == ($required | sort))
       and (. as $root
         | all($required[]; . as $cell
           | $root.cells[$cell].status == 0
           and $root.cells[$cell].audit.audit_status == "PASS"))' \
      "${bounded_artifact}" >/dev/null 2>&1; then
    marker_status="PASS"
  fi
  if jq -e --argjson required "$(printf '%s\n' "${required_cells[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')" \
      '. as $root
       | all($required[]; . as $cell
         | $root.cells[$cell].audit.durable_state_dump.status == "CAPTURED_AND_VERIFIED")' \
      "${bounded_artifact}" >/dev/null 2>&1; then
    durable_status="PASS"
  fi
  if jq -e --argjson required "$(printf '%s\n' "${required_cells[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')" \
      '. as $root
       | all($required[]; . as $cell
         | $root.cells[$cell].audit.fresh_process_recovery == "PASS")' \
      "${bounded_artifact}" >/dev/null 2>&1; then
    fresh_process_status="PASS"
  fi
  if jq -e --argjson required "$(printf '%s\n' "${required_cells[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')" \
      '. as $root
       | all($required[]; . as $cell
         | $root.cells[$cell].audit.invariant_audit.status == "INDEPENDENT_FIELDS_PASS")' \
      "${bounded_artifact}" >/dev/null 2>&1; then
    invariant_status="PASS"
  fi
fi

json_lines() {
  if [[ -z "$1" ]]; then
    printf '[]\n'
  else
    printf '%s\n' "$1" | jq -Rsc 'split("\n") | map(select(length > 0))'
  fi
}

docker_containers_after="$(docker ps -a --format '{{.Names}} {{.Label "com.docker.compose.project"}}' \
  | rg 'nereus-delay-(kafka|pulsar|gateway|oxia).*e2e' || true)"
docker_networks_after="$(docker network ls --format '{{.Name}} {{.Label "com.docker.compose.project"}}' \
  | rg 'nereus-delay-(kafka|pulsar|gateway|oxia).*e2e' || true)"
docker_volumes_after="$(docker volume ls --format '{{.Name}} {{.Label "com.docker.compose.project"}}' \
  | rg 'nereus-delay-(kafka|pulsar|gateway|oxia).*e2e' || true)"
docker_images_after="$(docker image ls --format '{{.Repository}}:{{.Tag}} {{.ID}}' \
  | rg 'nereus-delay-(kafka|pulsar|gateway|oxia).*e2e|nereus-delay-(kafka-k1|pulsar-p1|gateway)' || true)"
docker_cleanup_status="PASS"
if [[ -n "${docker_containers_after}${docker_networks_after}${docker_volumes_after}${docker_images_after}" ]]; then
  docker_cleanup_status="FAIL"
fi

certified_status="BLOCKED"
if [[ "${source_status}" == "PASS" && "${bounded_exit}" == "0" \
    && "${bounded_status}" == "PASS_BOUNDED" && "${bounded_locks_match}" == "PASS" \
    && "${marker_status}" == "PASS" && "${durable_status}" == "PASS" \
    && "${fresh_process_status}" == "PASS" && "${invariant_status}" == "PASS" \
    && "${docker_cleanup_status}" == "PASS" ]]; then
  certified_status="PASS_CERTIFIED"
fi

required_cells_json="$(printf '%s\n' "${required_cells[@]}" | jq -Rsc 'split("\n") | map(select(length > 0))')"
jq -n \
  --arg schema "nereus-delay-certified-chaos-matrix-v1" \
  --arg status "${certified_status}" \
  --arg profile_id "${profile_id}" \
  --arg artifact "${artifact_dir}" \
  --arg bounded_artifact "${bounded_artifact}" \
  --arg bounded_log "${artifact_dir}/bounded-chaos.log" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
  --arg source_status "${source_status}" \
  --arg bounded_status "${bounded_status}" --argjson bounded_exit "${bounded_exit}" \
  --arg bounded_locks_match "${bounded_locks_match}" \
  --arg marker_status "${marker_status}" \
  --arg durable_status "${durable_status}" \
  --arg fresh_process_status "${fresh_process_status}" \
  --arg invariant_status "${invariant_status}" \
  --arg docker_cleanup_status "${docker_cleanup_status}" \
  --argjson required_cells "${required_cells_json}" \
  --slurpfile bounded "${bounded_artifact}" \
  --argjson docker_containers_after "$(json_lines "${docker_containers_after}")" \
  --argjson docker_networks_after "$(json_lines "${docker_networks_after}")" \
  --argjson docker_volumes_after "$(json_lines "${docker_volumes_after}")" \
  --argjson docker_images_after "$(json_lines "${docker_images_after}")" \
  '{
    schema: $schema,
    status: $status,
    profile_id: $profile_id,
    execution: "strict-sequential",
    artifact_dir: $artifact,
    started_at: $started_at,
    finished_at: $finished_at,
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    source_checks_pass: ($source_status == "PASS"),
    required_cells: $required_cells,
    cells: (if ($bounded | length) > 0 then ($bounded[0].cells // {}) else {} end),
    bounded_matrix: {
      status: $bounded_status,
      exit_code: $bounded_exit,
      artifact: $bounded_artifact,
      log: $bounded_log,
      source_locks_match: $bounded_locks_match,
      marker_status: $marker_status
    },
    evidence: {
      durable_state_dump: $durable_status,
      fresh_process_recovery: $fresh_process_status,
      invariant_audit: $invariant_status
    },
    docker_postcheck: {
      status: $docker_cleanup_status,
      containers: $docker_containers_after,
      networks: $docker_networks_after,
      volumes: $docker_volumes_after,
      generated_images: $docker_images_after
    },
    boundaries: [
      "PASS_CERTIFIED requires all fourteen declared cells, not only the cells currently backed by durable dumps.",
      "Each cell must independently prove deterministic injection, durable before/after state, fresh-process recovery and invariant comparison.",
      "This wrapper does not add long-GC, ENOSPC, fsync, SST, storage-provider, target-isolation or disaster evidence that the child matrix has not captured.",
      "Generated Docker resources are checked by exact Nereus Delay prefixes; no global Docker prune is performed."
    ]
  }' >"${artifact_dir}/certified-chaos-matrix.json"

jq -e --arg status "${certified_status}" \
  '.status == $status and .schema == "nereus-delay-certified-chaos-matrix-v1"' \
  "${artifact_dir}/certified-chaos-matrix.json" >/dev/null
echo "Certified chaos matrix artifact: ${artifact_dir}/certified-chaos-matrix.json"
echo "status=${certified_status} bounded_status=${bounded_status} durable=${durable_status} fresh_process=${fresh_process_status} invariant=${invariant_status} docker_cleanup=${docker_cleanup_status}"

if [[ "${certified_status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
