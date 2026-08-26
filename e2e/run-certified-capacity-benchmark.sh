#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# This wrapper certifies only an explicitly named, bounded capacity profile.
# It does not turn the local Store/SLO probe into the §23.4  capacity
# envelope.  In particular, Broker throughput, Lane fairness, placement,
# adapter/zombie reservations, restore throughput and long-cycle soak remain
# independent release evidence.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_CERTIFIED_CAPACITY_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-certified-capacity.XXXXXX)}"
bounded_artifact_dir="${artifact_dir}/bounded-capacity-matrix"
gradle_home="${NEREUS_DELAY_CERTIFIED_CAPACITY_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
profile_id="${NEREUS_DELAY_CERTIFIED_CAPACITY_PROFILE_ID:-}"
required_case_count="${NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_CASE_COUNT:-}"
required_payload_records_total="${NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_PAYLOAD_RECORDS_TOTAL:-}"
required_slo_samples_total="${NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_SLO_SAMPLES_TOTAL:-}"
required_cgroup_memory_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_CGROUP_MEMORY_BYTES:-}"
required_direct_memory_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_DIRECT_MEMORY_BYTES:-}"
required_max_open_files="${NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_MAX_OPEN_FILES:-}"
max_case_process_rss_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_PROCESS_RSS_BYTES:-}"
max_case_current_open_files="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_CURRENT_OPEN_FILES:-}"
max_case_store_local_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_LOCAL_BYTES:-}"
max_case_store_wal_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_WAL_BYTES:-}"
max_case_store_sst_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_SST_BYTES:-}"
max_case_slo_outbox_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_SLO_OUTBOX_BYTES:-}"
max_case_slo_collector_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_SLO_COLLECTOR_BYTES:-}"
max_artifact_bytes="${NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_ARTIFACT_BYTES:-}"
image="${NEREUS_DELAY_CERTIFIED_CAPACITY_IMAGE:-eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769}"
pull_image="${NEREUS_DELAY_CERTIFIED_CAPACITY_PULL_IMAGE:-0}"
project_prefix="${NEREUS_DELAY_CERTIFIED_CAPACITY_PROJECT:-nereus-delay-certified-capacity-$(date +%s)-$$}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

fail() {
  echo "certified capacity benchmark: $*" >&2
  exit 1
}

command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v rg >/dev/null 2>&1 || fail "rg is required for exact Docker postchecks"

[[ -n "${profile_id}" ]] || fail "NEREUS_DELAY_CERTIFIED_CAPACITY_PROFILE_ID is required"
[[ "${profile_id}" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{1,127}$ ]] \
  || fail "capacity profile id is not canonical: ${profile_id}"
[[ "${project_prefix}" =~ ^[a-z0-9][a-z0-9_.-]*$ ]] \
  || fail "capacity project prefix contains unsupported characters: ${project_prefix}"
[[ "${pull_image}" == "0" || "${pull_image}" == "1" ]] \
  || fail "NEREUS_DELAY_CERTIFIED_CAPACITY_PULL_IMAGE must be 0 or 1"

positive_integer() {
  local name="$1"
  local value="$2"
  [[ "${value}" =~ ^[1-9][0-9]*$ ]] || fail "${name} must be a positive integer"
}

positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_CASE_COUNT "${required_case_count}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_PAYLOAD_RECORDS_TOTAL "${required_payload_records_total}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_SLO_SAMPLES_TOTAL "${required_slo_samples_total}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_CGROUP_MEMORY_BYTES "${required_cgroup_memory_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_DIRECT_MEMORY_BYTES "${required_direct_memory_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_MAX_OPEN_FILES "${required_max_open_files}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_PROCESS_RSS_BYTES "${max_case_process_rss_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_CURRENT_OPEN_FILES "${max_case_current_open_files}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_LOCAL_BYTES "${max_case_store_local_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_WAL_BYTES "${max_case_store_wal_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_SST_BYTES "${max_case_store_sst_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_SLO_OUTBOX_BYTES "${max_case_slo_outbox_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_SLO_COLLECTOR_BYTES "${max_case_slo_collector_bytes}"
positive_integer NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_ARTIFACT_BYTES "${max_artifact_bytes}"

[[ "${required_case_count}" == "3" ]] \
  || fail "the bounded capacity matrix currently requires exactly three cases"
[[ "${required_payload_records_total}" == "3888" ]] \
  || fail "the bounded capacity matrix currently requires payload total 3888"
[[ "${required_slo_samples_total}" == "664" ]] \
  || fail "the bounded capacity matrix currently requires SLO sample total 664"

mkdir -p "${artifact_dir}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  fail "artifact directory must be empty: ${artifact_dir}"
fi
mkdir -p "${bounded_artifact_dir}" "${gradle_home}"

require_checkout() {
  local label="$1"
  local path="$2"
  local expected_branch="$3"
  [[ -e "${path}/.git" ]] || fail "${label} checkout is not a Git worktree: ${path}"
  [[ -z "$(git -C "${path}" status --porcelain)" ]] \
    || fail "${label} checkout is dirty: ${path}"
  [[ "$(git -C "${path}" branch --show-current)" == "${expected_branch}" ]] \
    || fail "${label} checkout has unexpected branch: $(git -C "${path}" branch --show-current)"
  git -C "${path}" rev-parse HEAD
}

delay_source="$(require_checkout Delay "${delay_dir}" nereus/delay-full-implementation)"
kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer)"
pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard)"
oxia_source="$(require_checkout Oxia "${oxia_dir}" main)"

case_policy='[
  {"name":"smoke","payload_records_per_size":16,"slo_samples":24},
  {"name":"burst","payload_records_per_size":256,"slo_samples":128},
  {"name":"sustained","payload_records_per_size":1024,"slo_samples":512}
]'

case_names=("${project_prefix}-smoke" "${project_prefix}-burst" "${project_prefix}-sustained")
child_log="${artifact_dir}/bounded-capacity-matrix.log"
matrix_artifact="${bounded_artifact_dir}/capacity-benchmark-matrix.json"
image_id_before="$(docker image inspect "${image}" --format '{{.Id}}' 2>/dev/null || true)"

cleanup_exact_containers() {
  set +e
  for container_name in "${case_names[@]}"; do
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
  done
}
trap cleanup_exact_containers EXIT INT TERM

echo "Certified bounded capacity profile: ${profile_id}"
echo "Source locks: Delay=${delay_source} Kafka=${kafka_source} Pulsar=${pulsar_source} Oxia=${oxia_source}"
echo "Case policy: payload_records_total=${required_payload_records_total} slo_samples_total=${required_slo_samples_total}"
echo "Resource policy: cgroup=${required_cgroup_memory_bytes} direct=${required_direct_memory_bytes} open_files=${required_max_open_files}"

set +e
NEREUS_DELAY_CAPACITY_MATRIX_ARTIFACT_DIR="${bounded_artifact_dir}" \
NEREUS_DELAY_CAPACITY_MATRIX_GRADLE_USER_HOME="${gradle_home}" \
NEREUS_DELAY_CAPACITY_MATRIX_IMAGE="${image}" \
NEREUS_DELAY_CAPACITY_MATRIX_PROJECT="${project_prefix}" \
NEREUS_DELAY_CAPACITY_MATRIX_PULL_IMAGE="${pull_image}" \
bash "${script_dir}/run-bounded-capacity-matrix.sh" >"${child_log}" 2>&1
child_status=$?
set -e

matrix_status="MISSING"
if [[ -s "${matrix_artifact}" ]] && jq empty "${matrix_artifact}" >/dev/null 2>&1; then
  matrix_status="$(jq -r '.matrix_status // "UNKNOWN"' "${matrix_artifact}")"
fi

artifact_bytes_after="$(( $(du -sk "${artifact_dir}" 2>/dev/null | awk '{print $1 + 0}') * 1024 ))"

docker_resource_json() {
  local kind="$1"
  shift
  local lines=""
  local name
  for name in "$@"; do
    if [[ "${kind}" == "containers" ]]; then
      if docker ps -a --format '{{.Names}}' | rg -Fxq -- "${name}"; then lines+="${name}"$'\n'; fi
    elif [[ "${kind}" == "networks" ]]; then
      if docker network ls --format '{{.Name}}' | rg -Fxq -- "${name}"; then lines+="${name}"$'\n'; fi
    else
      if docker volume ls --format '{{.Name}}' | rg -Fxq -- "${name}"; then lines+="${name}"$'\n'; fi
    fi
  done
  printf '%s' "${lines}" | jq -Rsc 'split("\n") | map(select(length > 0))'
}

docker_containers_after="$(docker_resource_json containers "${case_names[@]}")"
docker_networks_after="$(docker_resource_json networks "${case_names[@]}")"
docker_volumes_after="$(docker_resource_json volumes "${case_names[@]}")"
docker_images_after="$(docker image ls --format '{{.Repository}}:{{.Tag}} {{.ID}}' \
  | { rg -F -- "${project_prefix}" || true; } \
  | jq -Rsc 'split("\n") | map(select(length > 0))')"
docker_cleanup_status="PASS"
if [[ "$(jq 'length' <<<"${docker_containers_after}")" != "0" \
   || "$(jq 'length' <<<"${docker_networks_after}")" != "0" \
   || "$(jq 'length' <<<"${docker_volumes_after}")" != "0" \
   || "$(jq 'length' <<<"${docker_images_after}")" != "0" ]]; then
  docker_cleanup_status="FAIL"
fi

image_id_after="$(docker image inspect "${image}" --format '{{.Id}}' 2>/dev/null || true)"
image_cleanup_status="PASS"
if [[ -z "${image_id_before}" && "${pull_image}" == "1" && -n "${image_id_after}" ]]; then
  image_cleanup_status="FAIL"
fi

case_invariants="FAIL"
resource_policy_status="FAIL"
artifact_size_status="FAIL"
if [[ "${child_status}" == "0" && -s "${matrix_artifact}" ]] \
    && jq -e \
      --arg delay "${delay_source}" \
      --argjson expected_cases "${required_case_count}" \
      --argjson expected_payload_total "${required_payload_records_total}" \
      --argjson expected_slo_total "${required_slo_samples_total}" \
      --argjson required_cgroup "${required_cgroup_memory_bytes}" \
      --argjson required_direct "${required_direct_memory_bytes}" \
      --argjson required_open_files "${required_max_open_files}" \
      --argjson max_rss "${max_case_process_rss_bytes}" \
      --argjson max_current_open_files "${max_case_current_open_files}" \
      --argjson max_local "${max_case_store_local_bytes}" \
      --argjson max_wal "${max_case_store_wal_bytes}" \
      --argjson max_sst "${max_case_store_sst_bytes}" \
      --argjson max_outbox "${max_case_slo_outbox_bytes}" \
      --argjson max_collector "${max_case_slo_collector_bytes}" \
      --argjson case_policy "${case_policy}" \
      '
        .schema == "nereus-delay-bounded-capacity-benchmark-matrix"
        and .status == "PARTIAL"
        and .matrix_status == "PASS_BOUNDED"
        and .source_lock == $delay
        and (.cases | type == "array" and length == $expected_cases)
        and ([.cases[] | {name: .name,
            payload_records_per_size: .artifact.configuration.payload_records_per_size,
            slo_samples: .artifact.configuration.slo_samples}] | sort_by(.name) == ($case_policy | sort_by(.name)))
        and ([.cases[].artifact.configuration.payload_records_per_size] | add) * 3 == $expected_payload_total
        and ([.cases[].artifact.configuration.slo_samples] | add) == $expected_slo_total
        and all(.cases[];
          .artifact.schema == "nereus-delay-bounded-capacity-slo-probe"
          and .artifact.status == "PARTIAL"
          and .artifact.source_lock == $delay
          and (.artifact.configuration.payload_sizes_bytes == [256, 4096, 65536])
          and (.artifact.platform_probe.status == "AVAILABLE")
          and (.artifact.platform_probe.authority == "WorkerRuntimeResourceProbe")
          and (.artifact.platform_probe.cgroup_memory_limit_bytes == $required_cgroup)
          and (.artifact.platform_probe.direct_memory_bytes == $required_direct)
          and (.artifact.platform_probe.max_open_files == $required_open_files)
          and (.artifact.platform_probe.process_rss_bytes | type == "number" and . <= $max_rss)
          and (.artifact.platform_probe.current_open_files | type == "number" and . <= $max_current_open_files)
          and (.artifact.store.reopen_verified == true)
          and (.artifact.store.usage_after.local_bytes | type == "number" and . <= $max_local)
          and (.artifact.store.usage_after.wal_bytes | type == "number" and . <= $max_wal)
          and (.artifact.store.usage_after.live_sst_bytes | type == "number" and . <= $max_sst)
          and ([.artifact.store.payload_runs[].payload_bytes] | sort == [256, 4096, 65536])
          and all(.artifact.store.payload_runs[];
            .records == .verified_records
            and .input_bytes == (.payload_bytes * .records))
          and (.artifact.slo.durable_start_final_samples == .artifact.configuration.slo_samples)
          and (.artifact.slo.outbox_record_count == .artifact.configuration.slo_samples)
          and (.artifact.slo.exported_records == .artifact.configuration.slo_samples)
          and (.artifact.slo.outbox_encoded_bytes | type == "number" and . <= $max_outbox)
          and (.artifact.slo.collector_state_bytes | type == "number" and . <= $max_collector)
          and (.artifact.slo.collector_reopen_verified == true)
        )
      ' "${matrix_artifact}" >/dev/null 2>&1; then
  case_invariants="PASS"
  resource_policy_status="PASS"
fi

if (( artifact_bytes_after <= max_artifact_bytes )); then
  artifact_size_status="PASS"
fi

if [[ ! -s "${matrix_artifact}" ]] || ! jq empty "${matrix_artifact}" >/dev/null 2>&1; then
  jq -n '{schema: "missing-capacity-benchmark-matrix", status: "MISSING"}' >"${matrix_artifact}"
fi

capacity_status="PASS_CERTIFIED"
if [[ "${child_status}" != "0" || "${matrix_status}" != "PASS_BOUNDED" \
    || "${case_invariants}" != "PASS" || "${resource_policy_status}" != "PASS" \
    || "${artifact_size_status}" != "PASS" || "${docker_cleanup_status}" != "PASS" \
    || "${image_cleanup_status}" != "PASS" ]]; then
  capacity_status="FAIL"
fi

jq -n \
  --arg schema "nereus-delay-certified-capacity-benchmark" \
  --arg status "${capacity_status}" \
  --arg profile_id "${profile_id}" \
  --arg artifact_dir "${artifact_dir}" \
  --arg matrix_artifact "${matrix_artifact}" \
  --arg child_log "${child_log}" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg image "${image}" \
  --arg image_id_before "${image_id_before}" \
  --arg image_id_after "${image_id_after}" \
  --arg delay "${delay_source}" \
  --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" \
  --arg oxia "${oxia_source}" \
  --argjson child_status "${child_status}" \
  --arg matrix_status "${matrix_status}" \
  --arg case_invariants "${case_invariants}" \
  --arg resource_policy_status "${resource_policy_status}" \
  --arg artifact_size_status "${artifact_size_status}" \
  --arg docker_cleanup_status "${docker_cleanup_status}" \
  --arg image_cleanup_status "${image_cleanup_status}" \
  --argjson artifact_bytes_after "${artifact_bytes_after}" \
  --argjson required_case_count "${required_case_count}" \
  --argjson required_payload_records_total "${required_payload_records_total}" \
  --argjson required_slo_samples_total "${required_slo_samples_total}" \
  --argjson required_cgroup_memory_bytes "${required_cgroup_memory_bytes}" \
  --argjson required_direct_memory_bytes "${required_direct_memory_bytes}" \
  --argjson required_max_open_files "${required_max_open_files}" \
  --argjson max_case_process_rss_bytes "${max_case_process_rss_bytes}" \
  --argjson max_case_current_open_files "${max_case_current_open_files}" \
  --argjson max_case_store_local_bytes "${max_case_store_local_bytes}" \
  --argjson max_case_store_wal_bytes "${max_case_store_wal_bytes}" \
  --argjson max_case_store_sst_bytes "${max_case_store_sst_bytes}" \
  --argjson max_case_slo_outbox_bytes "${max_case_slo_outbox_bytes}" \
  --argjson max_case_slo_collector_bytes "${max_case_slo_collector_bytes}" \
  --argjson max_artifact_bytes "${max_artifact_bytes}" \
  --argjson case_policy "${case_policy}" \
  --argjson docker_containers_after "${docker_containers_after}" \
  --argjson docker_networks_after "${docker_networks_after}" \
  --argjson docker_volumes_after "${docker_volumes_after}" \
  --argjson docker_images_after "${docker_images_after}" \
  --slurpfile matrix "${matrix_artifact}" \
  '
    {
      schema: $schema,
      status: $status,
      profile_id: $profile_id,
      execution: "strict-sequential",
      artifact_dir: $artifact_dir,
      matrix_artifact: $matrix_artifact,
      child_log: $child_log,
      started_at: $started_at,
      finished_at: $finished_at,
      container_image: $image,
      container_image_id_before: $image_id_before,
      container_image_id_after: $image_id_after,
      source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
      policy: {
        case_profiles: $case_policy,
        required_case_count: $required_case_count,
        required_payload_records_total: $required_payload_records_total,
        required_slo_samples_total: $required_slo_samples_total,
        required_cgroup_memory_bytes: $required_cgroup_memory_bytes,
        required_direct_memory_bytes: $required_direct_memory_bytes,
        required_max_open_files: $required_max_open_files,
        max_case_process_rss_bytes: $max_case_process_rss_bytes,
        max_case_current_open_files: $max_case_current_open_files,
        max_case_store_local_bytes: $max_case_store_local_bytes,
        max_case_store_wal_bytes: $max_case_store_wal_bytes,
        max_case_store_sst_bytes: $max_case_store_sst_bytes,
        max_case_slo_outbox_bytes: $max_case_slo_outbox_bytes,
        max_case_slo_collector_bytes: $max_case_slo_collector_bytes,
        max_artifact_bytes: $max_artifact_bytes
      },
      observations: {
        child_exit_code: $child_status,
        matrix_status: $matrix_status,
        case_invariants: $case_invariants,
        resource_policy_status: $resource_policy_status,
        artifact_size_status: $artifact_size_status,
        artifact_bytes_after: $artifact_bytes_after,
        docker_cleanup_status: $docker_cleanup_status,
        image_cleanup_status: $image_cleanup_status
      },
      docker_postcheck: {
        containers: $docker_containers_after,
        networks: $docker_networks_after,
        volumes: $docker_volumes_after,
        generated_images: $docker_images_after
      },
      boundaries: [
        "PASS_CERTIFIED applies only to this explicitly named bounded Store/SLO profile and exact four-repository source lock.",
        "The child cases are source-locked Linux local RocksDB payload writes, readback, durable SLO outbox merge and persistent reopen.",
        "This is not the §23.4  capacity envelope: it does not certify Broker throughput, Lane distributions or fairness, multi-Worker placement, Control Reserve, Adapter physical/zombie bounds, checkpoint restore throughput, inline/object flow, upgrade/downgrade or long-cycle soak.",
        "A missing policy field, failed case/resource invariant, artifact limit, image cleanup or exact Docker postcheck produces FAIL and never PASS_CERTIFIED.",
        "The pinned JDK image is removed only when this run pulled it; pre-existing base images are retained. No global Docker prune is permitted."
      ]
    }
  ' >"${artifact_dir}/certified-capacity-benchmark.json"

jq -e --arg status "${capacity_status}" \
  '.schema == "nereus-delay-certified-capacity-benchmark" and .status == $status and (.source_locks.delay | length == 40)' \
  "${artifact_dir}/certified-capacity-benchmark.json" >/dev/null

echo "certified capacity benchmark artifact=${artifact_dir}/certified-capacity-benchmark.json"
echo "status=${capacity_status} child_status=${child_status} matrix_status=${matrix_status}"
echo "case_invariants=${case_invariants} resource_policy=${resource_policy_status} artifact_size=${artifact_size_status}"
echo "docker_cleanup=${docker_cleanup_status} image_cleanup=${image_cleanup_status}"

if [[ "${capacity_status}" != "PASS_CERTIFIED" ]]; then
  exit 1
fi
