#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# This runner is deliberately policy-driven.  It never invents a soak profile:
# the caller must provide the release profile id, minimum cycle count, minimum
# elapsed duration and measurable process/disk bounds.  The underlying chain
# remains strictly sequential so a PASS_CERTIFIED receipt cannot hide port,
# Gradle, Docker or source-authority cross-talk.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_CERTIFIED_SOAK_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-certified-soak.XXXXXX)}"
bounded_artifact_dir="${artifact_dir}/bounded-production-chain"
gradle_home="${NEREUS_DELAY_CERTIFIED_SOAK_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
profile_id="${NEREUS_DELAY_CERTIFIED_SOAK_PROFILE_ID:-}"
required_cycles="${NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_CYCLES:-}"
cycles="${NEREUS_DELAY_CERTIFIED_SOAK_CYCLES:-${required_cycles}}"
required_duration_seconds="${NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_DURATION_SECONDS:-}"
max_process_rss_kib="${NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_RSS_KIB:-}"
max_process_fds="${NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_FDS:-}"
max_artifact_bytes="${NEREUS_DELAY_CERTIFIED_SOAK_MAX_ARTIFACT_BYTES:-}"
resource_sample_interval="${NEREUS_DELAY_CERTIFIED_SOAK_RESOURCE_SAMPLE_INTERVAL_SECONDS:-5}"
base_port="${NEREUS_DELAY_CERTIFIED_SOAK_BASE_PORT:-35100}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
started_epoch="$(date +%s)"

delay_base="2dfc3289ffdbe9cf9d7f4d0de1d701493d1b49a6"
kafka_base="c300006a7705c240642db6950b5a95fec982bfc5"
pulsar_base="8dae0236c0a0d405ed7f8303081080520fe91551"

fail() {
  echo "certified production-chain soak: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v docker >/dev/null 2>&1 || fail "docker is required"
docker compose version >/dev/null 2>&1 || fail "docker compose is required"
command -v pgrep >/dev/null 2>&1 || fail "pgrep is required for process-tree evidence"
command -v ps >/dev/null 2>&1 || fail "ps is required for process-tree evidence"
command -v lsof >/dev/null 2>&1 || fail "lsof is required for FD evidence"
command -v df >/dev/null 2>&1 || fail "df is required for disk evidence"
command -v du >/dev/null 2>&1 || fail "du is required for disk evidence"
command -v rg >/dev/null 2>&1 || fail "rg is required for Docker resource filtering"

[[ -n "${profile_id}" ]] || fail "NEREUS_DELAY_CERTIFIED_SOAK_PROFILE_ID is required"
[[ "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]] \
  || fail "certified soak profile id is not canonical: ${profile_id}"
[[ "${required_cycles}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_CYCLES must be a positive integer"
[[ "${cycles}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_CYCLES must be a positive integer"
(( cycles >= required_cycles )) \
  || fail "configured cycles ${cycles} are below required profile cycles ${required_cycles}"
[[ "${required_duration_seconds}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_DURATION_SECONDS must be positive"
[[ "${max_process_rss_kib}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_RSS_KIB must be positive"
[[ "${max_process_fds}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_FDS must be positive"
[[ "${max_artifact_bytes}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_MAX_ARTIFACT_BYTES must be positive"
[[ "${resource_sample_interval}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_RESOURCE_SAMPLE_INTERVAL_SECONDS must be positive"
[[ "${base_port}" =~ ^[1-9][0-9]*$ ]] \
  || fail "NEREUS_DELAY_CERTIFIED_SOAK_BASE_PORT must be positive"
(( base_port + (cycles - 1) * 1000 + 313 <= 65535 )) \
  || fail "base port leaves insufficient range for configured cycles"

mkdir -p "${artifact_dir}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
mkdir -p "${gradle_home}"
mkdir -p "${bounded_artifact_dir}"

require_checkout() {
  local label="$1"
  local path="$2"
  local expected_branch="$3"
  local required_base="$4"
  [[ -e "${path}/.git" ]] || fail "${label} checkout is not a Git worktree: ${path}"
  [[ -z "$(git -C "${path}" status --porcelain)" ]] \
    || fail "${label} checkout is dirty: ${path}"
  [[ "$(git -C "${path}" branch --show-current)" == "${expected_branch}" ]] \
    || fail "${label} checkout has unexpected branch: $(git -C "${path}" branch --show-current)"
  git -C "${path}" merge-base --is-ancestor "${required_base}" HEAD \
    || fail "${label} checkout does not contain required base ${required_base}"
  git -C "${path}" rev-parse HEAD
}

delay_source="$(require_checkout Delay "${delay_dir}" nereus/delay-full-implementation-v1 "${delay_base}")"
kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer-v1 "${kafka_base}")"
pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1 "${pulsar_base}")"
oxia_source="$(require_checkout Oxia "${oxia_dir}" main "$(git -C "${oxia_dir}" rev-parse HEAD)")"

resource_samples="${artifact_dir}/resource-samples.tsv"
docker_stats_log="${artifact_dir}/docker-stats.log"
child_log="${artifact_dir}/bounded-production-chain.log"
printf 'epoch_seconds\tprocess_rss_kib\tprocess_fds\tartifact_bytes\n' >"${resource_samples}"
: >"${docker_stats_log}"

collect_descendants() {
  local pid="$1"
  printf '%s\n' "${pid}"
  local child
  while IFS= read -r child; do
    [[ -n "${child}" ]] || continue
    collect_descendants "${child}"
  done < <(pgrep -P "${pid}" 2>/dev/null || true)
}

sample_resources() {
  local sample_epoch rss fds artifact_bytes pid line
  sample_epoch="$(date +%s)"
  rss=0
  fds=0
  if [[ -n "${child_pid:-}" ]] && kill -0 "${child_pid}" >/dev/null 2>&1; then
    while IFS= read -r pid; do
      [[ "${pid}" =~ ^[0-9]+$ ]] || continue
      line="$(ps -o rss= -p "${pid}" 2>/dev/null | awk '{print $1; exit}' || true)"
      [[ "${line}" =~ ^[0-9]+$ ]] && rss=$((rss + line))
      line="$(lsof -p "${pid}" 2>/dev/null | awk 'NR > 1 {count++} END {print count + 0}' || true)"
      [[ "${line}" =~ ^[0-9]+$ ]] && fds=$((fds + line))
    done < <(collect_descendants "${child_pid}" | sort -n -u)
  fi
  artifact_bytes=$(( $(du -sk "${artifact_dir}" 2>/dev/null | awk '{print $1}') * 1024 ))
  printf '%s\t%s\t%s\t%s\n' "${sample_epoch}" "${rss}" "${fds}" "${artifact_bytes}" >>"${resource_samples}"
  docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}\t{{.PIDs}}' 2>/dev/null \
    | rg 'nereus-delay-(large-payload|pulsar-large)' \
    | sed "s#^#${sample_epoch}\t#" >>"${docker_stats_log}" || true
}

monitor_resources() {
  while kill -0 "${child_pid}" >/dev/null 2>&1; do
    sample_resources
    sleep "${resource_sample_interval}"
  done
  sample_resources
}

echo "Certified production-chain soak profile: ${profile_id}"
echo "Required cycles/duration: ${required_cycles}/${required_duration_seconds}s"
echo "Configured cycles/base port: ${cycles}/${base_port}"
echo "Process RSS/FD/artifact limits: ${max_process_rss_kib}KiB/${max_process_fds}/${max_artifact_bytes}B"
echo "Source locks: Delay=${delay_source} Kafka=${kafka_source} Pulsar=${pulsar_source} Oxia=${oxia_source}"

child_started_epoch="$(date +%s)"
set +e
NEREUS_DELAY_PRODUCTION_SOAK_ARTIFACT_DIR="${bounded_artifact_dir}" \
NEREUS_DELAY_PRODUCTION_SOAK_GRADLE_USER_HOME="${gradle_home}" \
NEREUS_DELAY_PRODUCTION_SOAK_CYCLES="${cycles}" \
NEREUS_DELAY_PRODUCTION_SOAK_BASE_PORT="${base_port}" \
bash "${script_dir}/run-bounded-production-chain-soak.sh" >"${child_log}" 2>&1 &
child_pid=$!
set -e
monitor_resources &
monitor_pid=$!
set +e
wait "${child_pid}"
child_status=$?
set -e
child_finished_epoch="$(date +%s)"
set +e
wait "${monitor_pid}" >/dev/null 2>&1
monitor_status=$?
set -e
finished_epoch="$(date +%s)"
finished_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
elapsed_seconds=$((finished_epoch - started_epoch))
child_elapsed_seconds=$((child_finished_epoch - child_started_epoch))
minimum_resource_samples=$((child_elapsed_seconds / resource_sample_interval))
(( minimum_resource_samples >= 2 )) || minimum_resource_samples=2

process_peak_rss_kib="$(awk 'NR > 1 {if ($2 > max) max=$2} END {print max + 0}' "${resource_samples}")"
process_peak_fds="$(awk 'NR > 1 {if ($3 > max) max=$3} END {print max + 0}' "${resource_samples}")"
artifact_peak_bytes="$(awk 'NR > 1 {if ($4 > max) max=$4} END {print max + 0}' "${resource_samples}")"
resource_sample_count="$(awk 'NR > 1 {count++} END {print count + 0}' "${resource_samples}")"
artifact_bytes_after="$(du -sk "${artifact_dir}" | awk '{print $1 * 1024}')"

bounded_artifact="${bounded_artifact_dir}/production-chain-soak.json"
cases_artifact="${bounded_artifact_dir}/production-chain-cases.json"
bounded_status="MISSING"
if [[ -s "${bounded_artifact}" ]] && jq empty "${bounded_artifact}" >/dev/null 2>&1; then
  bounded_status="$(jq -r '.status // "UNKNOWN"' "${bounded_artifact}")"
fi

expected_cases=$((cycles * 4))
case_invariants="FAIL"
if [[ "${child_status}" == 0 && "${bounded_status}" == "PASS_BOUNDED" \
    && -s "${bounded_artifact}" && -s "${cases_artifact}" ]] \
    && jq -e \
      --arg delay "${delay_source}" --arg kafka "${kafka_source}" \
      --arg pulsar "${pulsar_source}" --arg oxia "${oxia_source}" \
      '.source_locks.delay == $delay and .source_locks.kafka == $kafka
       and .source_locks.pulsar == $pulsar and .source_locks.oxia == $oxia' \
      "${bounded_artifact}" >/dev/null 2>&1 \
    && jq -e \
      --argjson expected "${expected_cases}" \
      'length == $expected
       and all(.[]; .status == "PASS" and .exit_code == 0
         and .docker_cleanup.status == "PASS"
         and (.docker_cleanup.containers_after | length) == 0
         and (.docker_cleanup.networks_after | length) == 0
         and (.docker_cleanup.volumes_after | length) == 0
         and (.docker_cleanup.generated_images_after | length) == 0)
       and ([.[].name | capture("^cycle-(?<cycle>[0-9]+)-(?<case>.+)$") | .cycle] | unique | length == ($expected / 4))
       and ([.[].mode] | sort | unique == ["minio-503-after-commit", "minio-timeout-after-commit", "multi-shard-destination"])
       and ([.[].mode] | sort | group_by(.) | map(length) | sort == [($expected / 4), ($expected / 4), ($expected / 2)])' \
      "${cases_artifact}" >/dev/null 2>&1; then
  case_invariants="PASS"
fi

process_resource_status="FAIL"
if (( monitor_status == 0 )) \
    && (( resource_sample_count >= minimum_resource_samples )) \
    && (( process_peak_rss_kib <= max_process_rss_kib )) \
    && (( process_peak_fds <= max_process_fds )) \
    && (( artifact_peak_bytes <= max_artifact_bytes )) \
    && (( artifact_bytes_after <= max_artifact_bytes )); then
  process_resource_status="PASS"
fi

duration_status="FAIL"
if (( elapsed_seconds >= required_duration_seconds )); then
  duration_status="PASS"
fi

docker_containers_after="$(docker ps -a --format '{{.Names}} {{.Label "com.docker.compose.project"}}' \
  | rg 'nereus-delay-(large-payload|pulsar-large)' || true)"
docker_networks_after="$(docker network ls --format '{{.Name}} {{.Label "com.docker.compose.project"}}' \
  | rg 'nereus-delay-(large-payload|pulsar-large)' || true)"
docker_volumes_after="$(docker volume ls --format '{{.Name}} {{.Label "com.docker.compose.project"}}' \
  | rg 'nereus-delay-(large-payload|pulsar-large)' || true)"
docker_images_after="$(docker image ls --format '{{.Repository}}:{{.Tag}} {{.ID}}' \
  | rg 'nereus-delay-(large-payload|pulsar-large)' || true)"
docker_cleanup_status="PASS"
if [[ -n "${docker_containers_after}${docker_networks_after}${docker_volumes_after}${docker_images_after}" ]]; then
  docker_cleanup_status="FAIL"
fi

soak_status="PASS_CERTIFIED"
if [[ "${child_status}" != 0 || "${monitor_status}" != 0 || "${bounded_status}" != "PASS_BOUNDED" \
    || "${case_invariants}" != "PASS" || "${process_resource_status}" != "PASS" \
    || "${duration_status}" != "PASS" || "${docker_cleanup_status}" != "PASS" ]]; then
  soak_status="FAIL"
fi

json_lines() {
  if [[ -z "$1" ]]; then
    printf '[]\n'
  else
    printf '%s\n' "$1" | jq -Rsc 'split("\n") | map(select(length > 0))'
  fi
}

soak_artifact="${artifact_dir}/certified-production-chain-soak.json"
jq -n \
  --arg schema "nereus-delay-certified-production-chain-soak-v1" \
  --arg status "${soak_status}" \
  --arg profile_id "${profile_id}" \
  --arg artifact "${artifact_dir}" \
  --arg bounded_artifact "${bounded_artifact}" \
  --arg child_log "${child_log}" \
  --arg resource_samples "${resource_samples}" \
  --arg docker_stats_log "${docker_stats_log}" \
  --arg started_at "${started_at}" \
  --arg finished_at "${finished_at}" \
  --argjson elapsed_seconds "${elapsed_seconds}" \
  --argjson child_elapsed_seconds "${child_elapsed_seconds}" \
  --argjson minimum_resource_samples "${minimum_resource_samples}" \
  --argjson required_cycles "${required_cycles}" \
  --argjson cycles "${cycles}" \
  --argjson required_duration_seconds "${required_duration_seconds}" \
  --argjson max_process_rss_kib "${max_process_rss_kib}" \
  --argjson max_process_fds "${max_process_fds}" \
  --argjson max_artifact_bytes "${max_artifact_bytes}" \
  --argjson process_peak_rss_kib "${process_peak_rss_kib}" \
  --argjson process_peak_fds "${process_peak_fds}" \
  --argjson artifact_peak_bytes "${artifact_peak_bytes}" \
  --argjson resource_sample_count "${resource_sample_count}" \
  --argjson artifact_bytes_after "${artifact_bytes_after}" \
  --arg child_status "${child_status}" \
  --arg monitor_status "${monitor_status}" \
  --arg bounded_status "${bounded_status}" \
  --arg case_invariants "${case_invariants}" \
  --arg process_resource_status "${process_resource_status}" \
  --arg duration_status "${duration_status}" \
  --arg docker_cleanup_status "${docker_cleanup_status}" \
  --arg delay "${delay_source}" \
  --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" \
  --arg oxia "${oxia_source}" \
  --argjson docker_containers_after "$(json_lines "${docker_containers_after}")" \
  --argjson docker_networks_after "$(json_lines "${docker_networks_after}")" \
  --argjson docker_volumes_after "$(json_lines "${docker_volumes_after}")" \
  --argjson docker_images_after "$(json_lines "${docker_images_after}")" \
  '{
    schema: $schema,
    status: $status,
    profile_id: $profile_id,
    artifact_dir: $artifact,
    bounded_artifact: $bounded_artifact,
    child_log: $child_log,
    resource_samples: $resource_samples,
    docker_stats_log: $docker_stats_log,
    started_at: $started_at,
    finished_at: $finished_at,
    elapsed_seconds: $elapsed_seconds,
    child_elapsed_seconds: $child_elapsed_seconds,
    execution: "strict-sequential",
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    policy: {
      required_cycles: $required_cycles,
      configured_cycles: $cycles,
      required_duration_seconds: $required_duration_seconds,
      max_process_rss_kib: $max_process_rss_kib,
      max_process_fds: $max_process_fds,
      max_artifact_bytes: $max_artifact_bytes
    },
    observations: {
      child_exit_code: ($child_status | tonumber),
      monitor_exit_code: ($monitor_status | tonumber),
      bounded_status: $bounded_status,
      expected_cases: ($cycles * 4),
      case_invariants: $case_invariants,
      process_resource_status: $process_resource_status,
      resource_sample_count: $resource_sample_count,
      duration_status: $duration_status,
      docker_cleanup_status: $docker_cleanup_status,
      process_peak_rss_kib: $process_peak_rss_kib,
      process_peak_fds: $process_peak_fds,
      artifact_peak_bytes: $artifact_peak_bytes,
      minimum_resource_samples: $minimum_resource_samples,
      artifact_bytes_after: $artifact_bytes_after
    },
    docker_postcheck: {
      containers: $docker_containers_after,
      networks: $docker_networks_after,
      volumes: $docker_volumes_after,
      generated_images: $docker_images_after
    },
    boundaries: [
      "PASS_CERTIFIED applies only to the explicitly recorded soak profile and this exact four-repository source lock.",
      "The underlying cases are the real Kafka/Pulsar/Oxia/Worker/MinIO production chain and remain strictly serial.",
      "This artifact does not certify capacity, full chaos, activation/cutover, operations authorization, upgrade/downgrade or disaster continuity; those release gates remain independent.",
      "A missing policy field, failed invariant, resource limit, duration or exact Docker postcheck produces FAIL and never PASS_CERTIFIED."
    ]
  }' >"${soak_artifact}"

jq -e --arg status "${soak_status}" '.status == $status and (.source_locks.delay | length == 40)' \
  "${soak_artifact}" >/dev/null
echo "certified production-chain soak artifact=${soak_artifact}"
echo "status=${soak_status}"
echo "child_status=${child_status} monitor_status=${monitor_status} bounded_status=${bounded_status} case_invariants=${case_invariants}"
echo "resource_status=${process_resource_status} duration_status=${duration_status} docker_cleanup=${docker_cleanup_status}"

if [[ "${soak_status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
