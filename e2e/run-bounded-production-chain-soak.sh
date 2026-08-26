#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

# This harness deliberately runs one real Large Payload production chain at a
# time.  The two provider runners share the Delay checkout/build output, so
# parallel execution would make the receipt ambiguous and could cross-wire
# ports, Gradle state or generated Docker resources.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_PRODUCTION_SOAK_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-production-soak.XXXXXX)}"
gradle_home="${NEREUS_DELAY_PRODUCTION_SOAK_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
cycles="${NEREUS_DELAY_PRODUCTION_SOAK_CYCLES:-1}"
base_port="${NEREUS_DELAY_PRODUCTION_SOAK_BASE_PORT:-34100}"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

delay_base="2dfc3289ffdbe9cf9d7f4d0de1d701493d1b49a6"
kafka_base="c300006a7705c240642db6950b5a95fec982bfc5"
pulsar_base="8dae0236c0a0d405ed7f8303081080520fe91551"

if ! command -v jq >/dev/null 2>&1; then
  echo "bounded production-chain soak requires jq" >&2
  exit 1
fi
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "bounded production-chain soak requires docker and docker compose" >&2
  exit 1
fi
if [[ ! "${cycles}" =~ ^[1-9][0-9]*$ || "${cycles}" -gt 8 ]]; then
  echo "NEREUS_DELAY_PRODUCTION_SOAK_CYCLES must be an integer from 1 through 8" >&2
  exit 1
fi
if [[ ! "${base_port}" =~ ^[1-9][0-9]*$ ]]; then
  echo "NEREUS_DELAY_PRODUCTION_SOAK_BASE_PORT must be a positive integer" >&2
  exit 1
fi
if (( base_port + (cycles - 1) * 1000 + 313 > 65535 )); then
  echo "NEREUS_DELAY_PRODUCTION_SOAK_BASE_PORT leaves insufficient TCP port range" >&2
  exit 1
fi

mkdir -p "${artifact_dir}" "${gradle_home}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "production-chain soak artifact directory must be empty: ${artifact_dir}" >&2
  exit 1
fi

require_checkout() {
  local label="$1"
  local path="$2"
  local expected_branch="$3"
  local required_base="$4"
  if [[ ! -e "${path}/.git" ]]; then
    echo "${label} checkout is not a Git worktree: ${path}" >&2
    exit 1
  fi
  if [[ -n "$(git -C "${path}" status --porcelain)" ]]; then
    echo "${label} checkout is dirty: ${path}" >&2
    exit 1
  fi
  if [[ "$(git -C "${path}" branch --show-current)" != "${expected_branch}" ]]; then
    echo "${label} checkout has unexpected branch: $(git -C "${path}" branch --show-current)" >&2
    exit 1
  fi
  if [[ -n "${required_base}" ]]; then
    git -C "${path}" merge-base --is-ancestor "${required_base}" HEAD
  fi
  git -C "${path}" rev-parse HEAD
}

delay_source="$(require_checkout Delay "${delay_dir}" nereus/delay-full-implementation "${delay_base}")"
kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer "${kafka_base}")"
pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard "${pulsar_base}")"
oxia_source="$(require_checkout Oxia "${oxia_dir}" main "")"

test -x "${delay_dir}/gradlew"
test -f "${script_dir}/run-large-payload-gateway-e2e.sh"
test -f "${script_dir}/run-pulsar-large-payload-gateway-e2e.sh"

echo "Bounded production-chain soak artifact directory: ${artifact_dir}"
echo "Production-chain cycles: ${cycles}"
echo "Production-chain base port: ${base_port}"
echo "Production-chain execution: strict sequential"
echo "Scope: current-source real Gateway + Oxia + Broker + Worker + MinIO; PASS_BOUNDED only"

cases_jsonl="${artifact_dir}/.production-chain-cases.jsonl"
: >"${cases_jsonl}"
matrix_status=0

json_lines() {
  local lines="$1"
  if [[ -z "${lines}" ]]; then
    printf '[]\n'
  else
    printf '%s\n' "${lines}" | jq -Rsc 'split("\n") | map(select(length > 0))'
  fi
}

cleanup_case_docker() {
  local provider="$1"
  local project="$2"
  case_cleanup_status="PASS"
  case_cleanup_detail="exact Compose project cleanup passed; generated provider images removed; locked MinIO base retained"
  case_removed_images=""
  case_removed_containers=""

  if [[ -z "${project}" ]]; then
    case_cleanup_status="FAIL"
    case_cleanup_detail="runner did not emit a recoverable Compose project"
    case_containers_after=""
    case_networks_after=""
    case_volumes_after=""
    case_generated_images_after=""
    return 0
  fi

  generated_refs=()
  if [[ "${provider}" == "kafka" ]]; then
    generated_refs+=("nereus-delay-large-payload-k1:${project}" "${project}-oxia")
  else
    generated_refs+=("nereus-delay-pulsar-p1-large:${project}" "${project}-oxia")
  fi
  related_containers="$(docker ps -aq --filter "label=com.docker.compose.project=${project}" || true)"
  related_networks="$(docker network ls -q --filter "label=com.docker.compose.project=${project}" || true)"
  related_volumes="$(docker volume ls -q --filter "label=com.docker.compose.project=${project}" || true)"
  related_generated_images=""
  for image_ref in "${generated_refs[@]}"; do
    if docker image inspect "${image_ref}" >/dev/null 2>&1; then
      related_generated_images="${related_generated_images}${image_ref}"$'\n'
    fi
  done

  set +e
  if [[ -n "${related_containers}" || -n "${related_networks}" || -n "${related_volumes}" \
      || -n "${related_generated_images}" ]]; then
    if [[ "${provider}" == "kafka" ]]; then
      docker compose --project-name "${project}" --file "${script_dir}/docker-compose.large-payload.yml" \
        down --volumes --remove-orphans --rmi local >/dev/null 2>&1
    else
      docker compose --project-name "${project}" \
        --file "${script_dir}/docker-compose.pulsar-cluster.yml" \
        --file "${script_dir}/docker-compose.pulsar-large-payload-infra.yml" \
        down --volumes --remove-orphans --rmi local >/dev/null 2>&1
    fi
    compose_status=$?
  else
    # The provider runner already executes this exact project cleanup in its
    # EXIT trap.  Avoid a second Compose interpolation pass after all
    # resources are gone; the compose files intentionally require runtime
    # image/port variables that are scoped to the child runner.
    compose_status=0
  fi
  if [[ "${compose_status}" != 0 ]]; then
    case_cleanup_status="FAIL"
    case_cleanup_detail="exact Compose cleanup returned ${compose_status}"
  fi

  if [[ -n "${related_containers}" ]]; then
    while IFS= read -r container; do
      [[ -z "${container}" ]] && continue
      docker rm --force "${container}" >/dev/null 2>&1
      remove_status=$?
      if [[ "${remove_status}" == 0 ]]; then
        case_removed_containers="${case_removed_containers}${container}"$'\n'
      else
        case_cleanup_status="FAIL"
        case_cleanup_detail="a related Compose container could not be removed: ${container}"
      fi
    done <<<"${related_containers}"
  fi

  if [[ -n "${related_networks}" ]]; then
    while IFS= read -r network; do
      [[ -z "${network}" ]] && continue
      docker network rm "${network}" >/dev/null 2>&1
      remove_status=$?
      if [[ "${remove_status}" != 0 ]]; then
        case_cleanup_status="FAIL"
        case_cleanup_detail="a related Compose network could not be removed: ${network}"
      fi
    done <<<"${related_networks}"
  fi

  if [[ -n "${related_volumes}" ]]; then
    while IFS= read -r volume; do
      [[ -z "${volume}" ]] && continue
      docker volume rm "${volume}" >/dev/null 2>&1
      remove_status=$?
      if [[ "${remove_status}" != 0 ]]; then
        case_cleanup_status="FAIL"
        case_cleanup_detail="a related Compose volume could not be removed: ${volume}"
      fi
    done <<<"${related_volumes}"
  fi

  for image_ref in "${generated_refs[@]}"; do
    if docker image inspect "${image_ref}" >/dev/null 2>&1; then
      image_id="$(docker image inspect --format '{{.Id}}' "${image_ref}" || true)"
      docker image rm "${image_ref}" >/dev/null 2>&1
      remove_status=$?
      if [[ "${remove_status}" == 0 ]]; then
        case_removed_images="${case_removed_images}${image_ref}@${image_id}"$'\n'
      else
        case_cleanup_status="FAIL"
        case_cleanup_detail="a generated provider image could not be removed: ${image_ref}"
      fi
    fi
  done
  set -e

  case_containers_after="$(docker ps -aq --filter "label=com.docker.compose.project=${project}" || true)"
  case_networks_after="$(docker network ls -q --filter "label=com.docker.compose.project=${project}" || true)"
  case_volumes_after="$(docker volume ls -q --filter "label=com.docker.compose.project=${project}" || true)"
  case_generated_images_after=""
  for image_ref in "${generated_refs[@]}"; do
    if docker image inspect "${image_ref}" >/dev/null 2>&1; then
      case_generated_images_after="${case_generated_images_after}${image_ref}"$'\n'
    fi
  done
  if [[ -n "${case_containers_after}" || -n "${case_networks_after}" || -n "${case_volumes_after}" \
      || -n "${case_generated_images_after}" ]]; then
    case_cleanup_status="FAIL"
    case_cleanup_detail="exact project cleanup left related Docker resources or generated images"
  fi
}

run_case() {
  local name="$1"
  local provider="$2"
  local mode="$3"
  shift 3
  local log="${artifact_dir}/${name}.log"
  local marker_log="${artifact_dir}/${name}.markers"
  local started_epoch="${SECONDS}"

  echo
  echo "===== PRODUCTION CHAIN CASE ${name} (${provider}/${mode}) ====="
  set +e
  "$@" 2>&1 | tee "${log}"
  runner_status="${PIPESTATUS[0]}"
  set -e

  project="$(sed -n 's/^Compose project: //p' "${log}" | tail -1 || true)"
  if [[ -n "${project}" && ! "${project}" =~ ^[a-z0-9][a-z0-9_.-]*$ ]]; then
    project=""
    project_parse_detail="runner emitted an invalid Compose project name"
  else
    project_parse_detail="Compose project parsed from the runner receipt"
  fi
  cleanup_case_docker "${provider}" "${project}"

  rg -n "(E2E passed|authority E2E passed|PUBLISHED outcomes|BUILD SUCCESSFUL|source-applied physical publish passed)" \
    "${log}" >"${marker_log}" || true
  receipt_markers="$(json_lines "$(cat "${marker_log}")")"
  duration_seconds=$((SECONDS - started_epoch))
  case_status="PASS"
  if [[ "${runner_status}" != 0 || "${case_cleanup_status}" != "PASS" ]]; then
    case_status="FAIL"
    matrix_status=1
  fi

  jq -n \
    --arg name "${name}" \
    --arg provider "${provider}" \
    --arg mode "${mode}" \
    --arg status "${case_status}" \
    --argjson exit_code "${runner_status}" \
    --argjson duration_seconds "${duration_seconds}" \
    --arg log "${log}" \
    --arg project "${project}" \
    --arg project_parse_detail "${project_parse_detail}" \
    --arg cleanup_status "${case_cleanup_status}" \
    --arg cleanup_detail "${case_cleanup_detail}" \
    --argjson receipt_markers "${receipt_markers}" \
    --argjson removed_images "$(json_lines "${case_removed_images}")" \
    --argjson removed_containers "$(json_lines "${case_removed_containers}")" \
    --argjson containers_after "$(json_lines "${case_containers_after}")" \
    --argjson networks_after "$(json_lines "${case_networks_after}")" \
    --argjson volumes_after "$(json_lines "${case_volumes_after}")" \
    --argjson generated_images_after "$(json_lines "${case_generated_images_after}")" \
    '{
      name: $name,
      provider: $provider,
      mode: $mode,
      status: $status,
      exit_code: $exit_code,
      duration_seconds: $duration_seconds,
      log: $log,
      compose_project: $project,
      compose_project_receipt: $project_parse_detail,
      receipt_markers: $receipt_markers,
      docker_cleanup: {
        status: $cleanup_status,
        detail: $cleanup_detail,
        removed_images: $removed_images,
        removed_containers: $removed_containers,
        containers_after: $containers_after,
        networks_after: $networks_after,
        volumes_after: $volumes_after,
        generated_images_after: $generated_images_after
      }
    }' >>"${cases_jsonl}"

  if [[ "${case_status}" == "PASS" ]]; then
    echo "CASE ${name}: PASS (bounded; exact Docker cleanup ${case_cleanup_status})"
  else
    echo "CASE ${name}: FAIL (runner=${runner_status}, cleanup=${case_cleanup_status})" >&2
  fi
}

for cycle in $(seq 1 "${cycles}"); do
  cycle_base=$((base_port + (cycle - 1) * 1000))
  cycle_suffix="c${cycle}"

  run_case "cycle-${cycle}-kafka-multi-shard" kafka "multi-shard-destination" env \
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_KAFKA_CLIENT_JAR="${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar" \
    NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME="${gradle_home}/kafka" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC="nereus-delay-soak-kafka-source-${cycle_suffix}" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC="nereus-delay-soak-kafka-destination-${cycle_suffix}" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER=0 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_PROCESS_CRASH=0 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_NETWORK_PARTITION=0 \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=NONE \
    NEREUS_DELAY_MINIO_BUCKET="nereus-delay-soak-kafka-ms-${cycle_suffix}" \
    KAFKA_LARGE_PAYLOAD_BROKER_1_PORT="${cycle_base}" \
    KAFKA_LARGE_PAYLOAD_BROKER_2_PORT="$((cycle_base + 1))" \
    KAFKA_LARGE_PAYLOAD_BROKER_3_PORT="$((cycle_base + 2))" \
    NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT="$((cycle_base + 10))" \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT="$((cycle_base + 11))" \
    NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT="$((cycle_base + 12))" \
    bash "${script_dir}/run-large-payload-gateway-e2e.sh"

  run_case "cycle-${cycle}-pulsar-multi-shard" pulsar "multi-shard-destination" env \
    NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME="${gradle_home}/pulsar" \
    PULSAR_LARGE_PAYLOAD_TOPIC="nereus-delay-soak-pulsar-source-${cycle_suffix}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC="nereus-delay-soak-pulsar-destination-${cycle_suffix}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=0 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH=0 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION=0 \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=NONE \
    NEREUS_DELAY_MINIO_BUCKET="nereus-delay-soak-pulsar-ms-${cycle_suffix}" \
    PULSAR_LARGE_BROKER_1_PORT="$((cycle_base + 100))" \
    PULSAR_LARGE_WEB_1_PORT="$((cycle_base + 101))" \
    PULSAR_LARGE_BROKER_2_PORT="$((cycle_base + 102))" \
    PULSAR_LARGE_WEB_2_PORT="$((cycle_base + 103))" \
    NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT="$((cycle_base + 110))" \
    NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT="$((cycle_base + 111))" \
    NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT="$((cycle_base + 112))" \
    bash "${script_dir}/run-pulsar-large-payload-gateway-e2e.sh"

  run_case "cycle-${cycle}-kafka-minio-after-commit" kafka "minio-timeout-after-commit" env \
    NEREUS_DELAY_KAFKA_CHECKOUT="${kafka_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_KAFKA_CLIENT_JAR="${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar" \
    NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME="${gradle_home}/kafka" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC="nereus-delay-soak-kafka-fault-source-${cycle_suffix}" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC="nereus-delay-soak-kafka-fault-destination-${cycle_suffix}" \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=0 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER=0 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_PROCESS_CRASH=0 \
    NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_NETWORK_PARTITION=0 \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_TIMEOUT_AFTER_COMMIT \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS=1000 \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT="$((cycle_base + 213))" \
    NEREUS_DELAY_MINIO_BUCKET="nereus-delay-soak-kafka-fault-${cycle_suffix}" \
    KAFKA_LARGE_PAYLOAD_BROKER_1_PORT="$((cycle_base + 200))" \
    KAFKA_LARGE_PAYLOAD_BROKER_2_PORT="$((cycle_base + 201))" \
    KAFKA_LARGE_PAYLOAD_BROKER_3_PORT="$((cycle_base + 202))" \
    NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT="$((cycle_base + 210))" \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT="$((cycle_base + 211))" \
    NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT="$((cycle_base + 212))" \
    bash "${script_dir}/run-large-payload-gateway-e2e.sh"

  run_case "cycle-${cycle}-pulsar-minio-after-commit" pulsar "minio-503-after-commit" env \
    NEREUS_DELAY_PULSAR_CHECKOUT="${pulsar_dir}" \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME="${gradle_home}/pulsar" \
    PULSAR_LARGE_PAYLOAD_TOPIC="nereus-delay-soak-pulsar-fault-source-${cycle_suffix}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC="nereus-delay-soak-pulsar-fault-destination-${cycle_suffix}" \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=0 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=0 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH=0 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION=0 \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_503_AFTER_COMMIT \
    NEREUS_DELAY_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS=1000 \
    NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT="$((cycle_base + 313))" \
    NEREUS_DELAY_MINIO_BUCKET="nereus-delay-soak-pulsar-fault-${cycle_suffix}" \
    PULSAR_LARGE_BROKER_1_PORT="$((cycle_base + 300))" \
    PULSAR_LARGE_WEB_1_PORT="$((cycle_base + 301))" \
    PULSAR_LARGE_BROKER_2_PORT="$((cycle_base + 302))" \
    PULSAR_LARGE_WEB_2_PORT="$((cycle_base + 303))" \
    NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT="$((cycle_base + 310))" \
    NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT="$((cycle_base + 311))" \
    NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT="$((cycle_base + 312))" \
    bash "${script_dir}/run-pulsar-large-payload-gateway-e2e.sh"
done

cases_artifact="${artifact_dir}/production-chain-cases.json"
jq -s '.' "${cases_jsonl}" >"${cases_artifact}"
expected_cases=$((cycles * 4))
soak_status="PASS_BOUNDED"
if [[ "${matrix_status}" != 0 ]] \
    || ! jq -e --argjson expected "${expected_cases}" \
      'length == $expected and all(.[]; .status == "PASS" and .exit_code == 0 and .docker_cleanup.status == "PASS" and (.docker_cleanup.containers_after | length) == 0 and (.docker_cleanup.networks_after | length) == 0 and (.docker_cleanup.volumes_after | length) == 0 and (.docker_cleanup.generated_images_after | length) == 0)' \
      "${cases_artifact}" >/dev/null; then
  soak_status="FAIL"
fi

soak_artifact="${artifact_dir}/production-chain-soak.json"
jq -n \
  --arg schema "nereus-delay-bounded-production-chain-soak" \
  --arg status "${soak_status}" \
  --arg artifact "${artifact_dir}" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --argjson cycles "${cycles}" \
  --argjson expected_cases "${expected_cases}" \
  --arg delay "${delay_source}" \
  --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" \
  --arg oxia "${oxia_source}" \
  --slurpfile cases "${cases_artifact}" \
  '{
    schema: $schema,
    status: $status,
    artifact_dir: $artifact,
    started_at: $started_at,
    finished_at: $finished_at,
    execution: "strict-sequential",
    cycles: $cycles,
    expected_cases: $expected_cases,
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    cases: $cases[0],
    docker_policy: "Only the exact Compose projects and generated provider image tags emitted by these cases are eligible for removal; the locked MinIO base and unrelated images are retained; no global Docker prune is performed.",
    boundaries: [
      "PASS_BOUNDED is repeated current-source production-chain evidence, not  release certification.",
      "The cases cover Kafka and Pulsar two-shard Large Payload destination egress plus one Kafka MinIO timeout-after-commit and one Pulsar MinIO 503-after-commit uncertainty path per cycle.",
      "This bounded harness does not prove the full section 23.3 fresh-process chaos matrix, the complete 23.4 capacity envelope, certified memory/FD/disk/aged-uncertainty soak, upgrade or disaster continuity gates.",
      "The release gate must continue to require independently source-locked PASS_CERTIFIED capacity, soak, activation/cutover, operations and chaos evidence."
    ]
  }' >"${soak_artifact}"
rm -f "${cases_jsonl}"

jq -e --arg status "${soak_status}" \
  '.status == $status and (.cases | length > 0) and (.source_locks.delay | length == 40)' \
  "${soak_artifact}" >/dev/null
echo "canonical production-chain soak artifact=${soak_artifact}"
echo "status=${soak_status}"
echo "related Docker cleanup: exact per-project containers/networks/volumes/generated images checked; locked MinIO base retained; no global prune"

if [[ "${soak_status}" != "PASS_BOUNDED" ]]; then
  exit 1
fi
