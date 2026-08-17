#!/usr/bin/env bash
set -euo pipefail

export LC_ALL=C
export LANG=C

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
pulsar_dir="${NEREUS_DELAY_PULSAR_CHECKOUT:-${delay_dir}/../../pulsar-worktrees/nereus-delay-p1}"
oxia_dir="${NEREUS_DELAY_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
artifact_dir="${NEREUS_DELAY_OPERATIONS_DRILLS_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-bounded-operations.XXXXXX)}"
gradle_home="${NEREUS_DELAY_OPERATIONS_DRILLS_GRADLE_USER_HOME:-${artifact_dir}/gradle-user-home}"
checkpoint_oxia_port="${NEREUS_DELAY_OPERATIONS_CHECKPOINT_OXIA_PORT:-31500}"
checkpoint_minio_port="${NEREUS_DELAY_OPERATIONS_CHECKPOINT_MINIO_PORT:-31501}"
checkpoint_compose_file="${script_dir}/docker-compose.oxia.yml"
minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if ! command -v jq >/dev/null 2>&1; then
  echo "operations drills require jq" >&2
  exit 1
fi
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "operations drills require docker and docker compose" >&2
  exit 1
fi
if [[ ! "${checkpoint_oxia_port}" =~ ^[0-9]+$ || ! "${checkpoint_minio_port}" =~ ^[0-9]+$ ]]; then
  echo "operations checkpoint ports must be numeric" >&2
  exit 1
fi
if [[ "${checkpoint_oxia_port}" == "${checkpoint_minio_port}" ]]; then
  echo "operations checkpoint ports must differ" >&2
  exit 1
fi
if ! docker image inspect "${minio_image}" >/dev/null 2>&1; then
  echo "locked MinIO image is not present locally: ${minio_image}" >&2
  exit 1
fi
if ! docker image inspect --format '{{join .RepoDigests "\n"}}' "${minio_image}" \
    | rg -F "@${minio_digest}" >/dev/null; then
  echo "local MinIO tag does not carry locked repository digest ${minio_digest}" >&2
  exit 1
fi

mkdir -p "${artifact_dir}" "${gradle_home}"
if [[ -n "$(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "operations drill artifact directory must be empty: ${artifact_dir}" >&2
  exit 1
fi

require_checkout() {
  local label="$1"
  local path="$2"
  local expected_branch="$3"
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
  git -C "${path}" rev-parse HEAD
}

delay_source="$(require_checkout Delay "${delay_dir}" nereus/delay-full-implementation-v1)"
kafka_source="$(require_checkout Kafka "${kafka_dir}" nereus/delay-guarded-producer-v1)"
pulsar_source="$(require_checkout Pulsar "${pulsar_dir}" nereus/delay-resource-guard-v1)"
oxia_source="$(require_checkout Oxia "${oxia_dir}" main)"

if command -v lsof >/dev/null 2>&1; then
  for port in "${checkpoint_oxia_port}" "${checkpoint_minio_port}"; do
    if lsof -nP -iTCP:"${port}" -sTCP:LISTEN >/dev/null 2>&1; then
      echo "required operations checkpoint port is already listening: ${port}" >&2
      exit 1
    fi
  done
fi
preexisting_minio_port_containers="$(docker ps -aq --filter "publish=${checkpoint_minio_port}" || true)"
if [[ -n "${preexisting_minio_port_containers}" ]]; then
  echo "operations checkpoint MinIO port is already owned by Docker containers: ${preexisting_minio_port_containers}" >&2
  exit 1
fi

probes_jsonl="${artifact_dir}/.operations-probes.jsonl"
: >"${probes_jsonl}"
cleanup_removed_images="${artifact_dir}/.cleanup-removed-images.json"
cleanup_removed_containers="${artifact_dir}/.cleanup-removed-containers.json"
printf '[]\n' >"${cleanup_removed_images}"
printf '[]\n' >"${cleanup_removed_containers}"

run_probe() {
  local name="$1"
  shift
  local log="${artifact_dir}/${name}.log"
  echo
  echo "===== OPERATIONS PROBE ${name} ====="
  set +e
  "$@" 2>&1 | tee "${log}"
  local status=${PIPESTATUS[0]}
  set -e
  jq -n --arg name "${name}" --arg status "$(if [[ "${status}" == 0 ]]; then echo PASS; else echo FAIL; fi)" \
    --argjson exit_code "${status}" --arg log "${log}" \
    '{name: $name, status: $status, exit_code: $exit_code, log: $log}' >>"${probes_jsonl}"
  if [[ "${status}" == 0 ]]; then
    echo "OPERATIONS PROBE ${name}: PASS"
  else
    echo "OPERATIONS PROBE ${name}: FAIL (${status})" >&2
  fi
}

run_local_operations() {
  cd "${delay_dir}"
  env GRADLE_USER_HOME="${gradle_home}/local" ./gradlew test \
    --tests io.nereusstream.delay.store.CheckpointRestoreCoordinatorTest \
    --tests io.nereusstream.delay.store.RecoveryCatalogTest \
    --tests 'io.nereusstream.delay.store.ShardStoreTest.checkpointUsesTemporaryNamespaceAndRejectsExistingTarget' \
    --tests 'io.nereusstream.delay.store.ShardStoreTest.checkpointIdentityIsCopiedWithTheDbAndFailedAttemptRollsBackProjection' \
    --tests 'io.nereusstream.delay.store.ShardStoreTest.completeCheckpointRestoresIntoFreshStoreIncarnation' \
    --tests 'io.nereusstream.delay.store.ShardStoreTest.restoreCanReplaceAnOrphanIncarnationWhenActivePointerWasNotInstalled' \
    --tests 'io.nereusstream.delay.store.ShardStoreTest.failedStagedRestoreCleansRuntimeValidationTree' \
    --tests 'io.nereusstream.delay.store.ShardStoreTest.failedActivePointerInstallRemovesUnpublishedDb' \
    --tests 'io.nereusstream.delay.store.ShardStoreTest.catalogBoundRestoreRejectsPinDriftBeforeActivePublication' \
    --tests io.nereusstream.delay.ownership.OwnerRecoveryCoordinatorTest \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.strictCatchupCasPublishesTheAuthoritativeLifecycleBeforeReplay' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.strictCatchupAcceptsOnlyAnExactResponseLossReread' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.strictReplayFencesBeforeApplyingAfterAuthoritativeOwnerReplacement' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.activationRequeuesRestoredClaimBeforeOpeningCommandGate' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.ownerCannotApplyBeforeRestoreAndCatchUpBarriers' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.catchupReplayAppliesCommandsBeforeActivationAndAdvancesOnlyAfterCommit' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.boundedCatchupTurnRetainsTheCursorForTheNextTurn' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.sourceCursorFailureFencesEveryReplayPathBeforeApplyingOrAdvancing' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.v1CatchupPinsTheAdapterSuccessorAndRejectsAKafkaGapBeforeApplyingIt' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.authorityGatedActivationKeepsLocalGateClosedDuringLeaseCas' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.authorityGatedDrainRequiresTheExactLeaseSuccessor' \
    --tests 'io.nereusstream.delay.ownership.OwnerLeaseTest.authorityGatedDrainFailsClosedWhenLeaseIsExpired' \
    --tests 'io.nereusstream.delay.runtime.DelayShardTest.replayDeadLetterCreatesNextGenerationAndRetainsOldTerminalSummary' \
    --tests 'io.nereusstream.delay.runtime.DelayShardTest.terminalSummaryRetainsASecondOpenObligationAndReopensSafely' \
    --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainPublishedEvidenceSettlesExactObligation' \
    --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainNotPublishedEvidenceNormalizesDefinitiveRetry' \
    --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainRetryMaterializesControlOverrideTimeline' \
    --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainTerminalizesPossibleDeliveryAndRetainsObligation' \
    --rerun-tasks --no-daemon --console=plain
}

run_real_checkpoint() {
  env \
    NEREUS_DELAY_OXIA_CHECKOUT="${oxia_dir}" \
    NEREUS_DELAY_E2E_GRADLE_USER_HOME="${gradle_home}/real-checkpoint" \
    NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT="${checkpoint_oxia_port}" \
    NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT="${checkpoint_minio_port}" \
    "${script_dir}/run-oxia-minio-checkpoint-e2e.sh"
}

run_probe local-state-machine run_local_operations
run_probe real-oxia-minio-checkpoint run_real_checkpoint

checkpoint_log="${artifact_dir}/real-oxia-minio-checkpoint.log"
checkpoint_project="$(sed -n 's/^Compose project: //p' "${checkpoint_log}" | tail -1 || true)"
if [[ -n "${checkpoint_project}" && ! "${checkpoint_project}" =~ ^[a-z0-9][a-z0-9_.-]*$ ]]; then
  echo "unexpected checkpoint compose project in receipt: ${checkpoint_project}" >&2
  checkpoint_project=""
fi

cleanup_status="PASS"
cleanup_detail="exact project/container/image cleanup attempted; locked MinIO base image retained"
record_list_item() {
  local file="$1"
  local value="$2"
  jq --arg value "${value}" '. + [$value]' "${file}" >"${file}.tmp"
  mv "${file}.tmp" "${file}"
}

cleanup_related_docker() {
  set +e
  if [[ -n "${checkpoint_project}" ]]; then
    docker compose --project-name "${checkpoint_project}" --file "${checkpoint_compose_file}" \
      down --remove-orphans --volumes --rmi local >/dev/null 2>&1
    compose_status=$?
    if [[ "${compose_status}" != 0 ]]; then
      cleanup_status="FAIL"
      cleanup_detail="exact checkpoint compose cleanup returned ${compose_status}"
    fi

    leftover_compose_containers="$(docker ps -aq --filter "label=com.docker.compose.project=${checkpoint_project}" || true)"
    if [[ -n "${leftover_compose_containers}" ]]; then
      while IFS= read -r container; do
        [[ -z "${container}" ]] && continue
        docker rm --force "${container}" >/dev/null 2>&1
        remove_status=$?
        if [[ "${remove_status}" == 0 ]]; then
          record_list_item "${cleanup_removed_containers}" "${container}"
        else
          cleanup_status="FAIL"
          cleanup_detail="a related Compose container could not be removed: ${container}"
        fi
      done <<<"${leftover_compose_containers}"
    fi

    generated_image="${checkpoint_project}-oxia"
    if docker image inspect "${generated_image}" >/dev/null 2>&1; then
      generated_image_id="$(docker image inspect --format '{{.Id}}' "${generated_image}" || true)"
      docker image rm "${generated_image}" >/dev/null 2>&1
      remove_status=$?
      if [[ "${remove_status}" == 0 ]]; then
        record_list_item "${cleanup_removed_images}" "${generated_image}@${generated_image_id}"
      else
        cleanup_status="FAIL"
        cleanup_detail="related generated image could not be removed: ${generated_image}"
      fi
    fi

    leftover_networks="$(docker network ls -q --filter "label=com.docker.compose.project=${checkpoint_project}" || true)"
    leftover_volumes="$(docker volume ls -q --filter "label=com.docker.compose.project=${checkpoint_project}" || true)"
    if [[ -n "${leftover_networks}" || -n "${leftover_volumes}" ]]; then
      cleanup_status="FAIL"
      cleanup_detail="related Compose networks or volumes remain after exact cleanup"
    fi
  else
    cleanup_status="FAIL"
    cleanup_detail="checkpoint Compose project was not recoverable from its receipt"
  fi

  leftover_minio_containers="$(docker ps -aq --filter "publish=${checkpoint_minio_port}" || true)"
  if [[ -n "${leftover_minio_containers}" ]]; then
    while IFS= read -r container; do
      [[ -z "${container}" ]] && continue
      docker rm --force "${container}" >/dev/null 2>&1
      remove_status=$?
      if [[ "${remove_status}" == 0 ]]; then
        record_list_item "${cleanup_removed_containers}" "${container}"
      else
        cleanup_status="FAIL"
        cleanup_detail="related MinIO container could not be removed: ${container}"
      fi
    done <<<"${leftover_minio_containers}"
  fi
  set -e
}

cleanup_related_docker

probes_artifact="${artifact_dir}/operations-probes.json"
jq -s '.' "${probes_jsonl}" >"${probes_artifact}"
operations_artifact="${artifact_dir}/operations-drills.json"
operations_status="PASS_BOUNDED"
if jq -e 'any(.[]; .status != "PASS")' "${probes_artifact}" >/dev/null 2>&1 || [[ "${cleanup_status}" != "PASS" ]]; then
  operations_status="FAIL"
fi

jq -n \
  --arg schema "nereus-delay-bounded-operations-drills-v1" \
  --arg status "${operations_status}" \
  --arg artifact "${artifact_dir}" \
  --arg started_at "${started_at}" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg delay "${delay_source}" \
  --arg kafka "${kafka_source}" \
  --arg pulsar "${pulsar_source}" \
  --arg oxia "${oxia_source}" \
  --arg compose_project "${checkpoint_project}" \
  --arg cleanup_status "${cleanup_status}" \
  --arg cleanup_detail "${cleanup_detail}" \
  --arg minio_image "${minio_image}@${minio_digest}" \
  --slurpfile probes "${probes_artifact}" \
  --slurpfile removed_images "${cleanup_removed_images}" \
  --slurpfile removed_containers "${cleanup_removed_containers}" \
  '{
    schema: $schema,
    status: $status,
    artifact_dir: $artifact,
    started_at: $started_at,
    finished_at: $finished_at,
    source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
    probes: $probes[0],
    docker_cleanup: {
      status: $cleanup_status,
      detail: $cleanup_detail,
      checkpoint_compose_project: $compose_project,
      removed_images: $removed_images[0],
      removed_containers: $removed_containers[0],
      retained_locked_images: [$minio_image],
      policy: "Only this run's checkpoint Compose resources and generated Oxia image are eligible for removal; no global Docker prune is performed."
    },
    boundaries: [
      "PASS_BOUNDED is an operations/state-machine receipt, not V1 release certification.",
      "The local probes cover restore fencing, catalog ancestry/floor validation, Owner recovery/drain gates, DLQ replay and source-ordered UNCERTAIN resolution.",
      "The real service probe covers the current Oxia/MinIO checkpoint publication and exact REAPING path; it does not prove external operator authorization, fresh-process disaster continuity or a multi-Worker production soak.",
      "PASS_CERTIFIED still requires the release gate's exact current four-repository source locks and independent operator/soak evidence."
    ]
  }' >"${operations_artifact}"
rm -f "${probes_jsonl}"

jq -e --arg status "${operations_status}" \
  '.status == $status and (.probes | length == 2) and (.docker_cleanup.status == "PASS")' \
  "${operations_artifact}" >/dev/null
echo "Bounded operations drills artifact: ${operations_artifact}"
echo "status=${operations_status}"
echo "related Docker cleanup: status=${cleanup_status}, compose_project=${checkpoint_project:-unknown}, locked MinIO image retained"

if [[ "${operations_status}" != "PASS_BOUNDED" ]]; then
  exit 1
fi
