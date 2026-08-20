#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}
oxia_port=${NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT:-16719}
minio_port=${NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT:-16729}
gradle_user_home=${NEREUS_DELAY_E2E_GRADLE_USER_HOME:-/tmp/nereus-delay-oxia-minio-checkpoint-gradle}
compose_file="$e2e_root/docker-compose.oxia.yml"
compose_project="nereus-delay-oxia-minio-checkpoint-e2e-$(date +%s)-$$"
minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_access_key=${NEREUS_DELAY_MINIO_ACCESS_KEY:-nereusdelay}
minio_secret_key=${NEREUS_DELAY_MINIO_SECRET_KEY:-nereus-delay-secret}
minio_region=${NEREUS_DELAY_MINIO_REGION:-us-east-1}
minio_bucket=${NEREUS_DELAY_MINIO_BUCKET:-nereus-delay-checkpoints-$(date +%s)-$$}
minio_container="nereus-delay-checkpoint-minio-e2e-$(date +%s)-$$"
minio_fault_mode=${NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_MODE:-NONE}
minio_request_timeout_ms=${NEREUS_DELAY_CHECKPOINT_MINIO_REQUEST_TIMEOUT_MS:-60000}
fresh_process_authority=${NEREUS_DELAY_FRESH_PROCESS_AUTHORITY:-0}
minio_fault_proxy_port=${NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_PROXY_PORT:-$((minio_port + 100))}
minio_fault_proxy_endpoint="http://127.0.0.1:${minio_fault_proxy_port}"
fault_proxy_log=$(mktemp -t nereus-delay-checkpoint-fault-proxy.XXXXXX).log
fault_proxy_seed="${fault_proxy_log%.log}"
fault_proxy_pid=""
minio_started=0
oxia_image="${compose_project}-oxia"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required" >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose is required" >&2
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required" >&2
    exit 1
fi
if [[ "${minio_fault_mode}" != "NONE" ]] && ! command -v python3 >/dev/null 2>&1; then
    echo "python3 is required for real MinIO checkpoint fault injection" >&2
    exit 1
fi
if ! git -C "$oxia_checkout" rev-parse --verify HEAD >/dev/null 2>&1; then
    echo "NEREUS_DELAY_OXIA_CHECKOUT is not a Git checkout: $oxia_checkout" >&2
    exit 1
fi
if [[ ! "$oxia_port" =~ ^[0-9]+$ || ! "$minio_port" =~ ^[0-9]+$ ]]; then
    echo "checkpoint E2E ports must be numeric" >&2
    exit 1
fi
if [[ "${minio_fault_mode}" != "NONE" && "${minio_fault_mode}" != "PUT_503_AFTER_COMMIT" \
    && "${minio_fault_mode}" != "PUT_TIMEOUT_AFTER_COMMIT" \
    && "${minio_fault_mode}" != "PUT_503_BEFORE_COMMIT" \
    && "${minio_fault_mode}" != "PUT_TIMEOUT_BEFORE_COMMIT" ]]; then
    echo "NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_MODE must be NONE, PUT_503_AFTER_COMMIT, PUT_TIMEOUT_AFTER_COMMIT, PUT_503_BEFORE_COMMIT or PUT_TIMEOUT_BEFORE_COMMIT" >&2
    exit 1
fi
if [[ ! "${minio_request_timeout_ms}" =~ ^[1-9][0-9]*$ ]]; then
    echo "NEREUS_DELAY_CHECKPOINT_MINIO_REQUEST_TIMEOUT_MS must be a positive integer" >&2
    exit 1
fi
if [[ "${fresh_process_authority}" != "0" && "${fresh_process_authority}" != "1" ]]; then
    echo "NEREUS_DELAY_FRESH_PROCESS_AUTHORITY must be 0 or 1" >&2
    exit 1
fi
if [[ "${fresh_process_authority}" == "1" && "${minio_fault_mode}" != "NONE" ]]; then
    echo "fresh-process authority evidence requires NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_MODE=NONE" >&2
    exit 1
fi
if [[ ! "${minio_fault_proxy_port}" =~ ^[0-9]+$ ]]; then
    echo "NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_PROXY_PORT must be numeric" >&2
    exit 1
fi
if [[ "${minio_fault_mode}" != "NONE" && "${minio_fault_proxy_port}" == "${minio_port}" ]]; then
    echo "MinIO and checkpoint fault proxy ports must differ" >&2
    exit 1
fi
if [[ ! "$minio_bucket" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    echo "NEREUS_DELAY_MINIO_BUCKET is not a canonical S3 bucket name: $minio_bucket" >&2
    exit 1
fi
if ! docker image inspect "$minio_image" >/dev/null 2>&1; then
    echo "the locked MinIO image is not present locally: $minio_image" >&2
    exit 1
fi

repo_digest=$(docker image inspect --format '{{json .RepoDigests}}' "$minio_image" \
    | rg -F "@$minio_digest" || true)
if [[ -z "$repo_digest" ]]; then
    echo "the local MinIO tag does not carry the expected repository digest: $minio_digest" >&2
    exit 1
fi
if [[ "${minio_fault_mode}" != "NONE" ]] && command -v lsof >/dev/null 2>&1 \
    && lsof -nP -iTCP:"${minio_fault_proxy_port}" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "required checkpoint fault proxy port is already listening: ${minio_fault_proxy_port}" >&2
    exit 1
fi

compose() {
    docker compose --project-name "$compose_project" --file "$compose_file" "$@"
}

wait_for_oxia_health() {
    local ready=0
    for attempt in $(seq 1 60); do
        if compose exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
            >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done
    if [[ "$ready" != 1 ]]; then
        compose logs oxia >&2 || true
        echo "Oxia did not become healthy" >&2
        return 1
    fi
}

cleanup() {
    local status=$?
    if [[ "$status" != 0 ]]; then
        if [[ "$minio_started" == 1 ]]; then
            docker logs "$minio_container" >&2 || true
        fi
        compose logs oxia >&2 || true
    fi
    if [[ -n "$fault_proxy_pid" ]]; then
        kill "$fault_proxy_pid" >/dev/null 2>&1 || true
        wait "$fault_proxy_pid" >/dev/null 2>&1 || true
    fi
    compose down --remove-orphans --volumes >/dev/null 2>&1 || true
    if [[ "$minio_started" == 1 ]]; then
        docker rm --force "$minio_container" >/dev/null 2>&1 || true
    fi
    if docker image inspect "$oxia_image" >/dev/null 2>&1; then
        docker image rm "$oxia_image" >/dev/null 2>&1 || true
    fi
    rm -f "$fault_proxy_log" "$fault_proxy_seed"
    exit "$status"
}
trap cleanup EXIT INT TERM

set_fault_mode() {
    local mode="$1"
    local response
    response=$(curl --silent --show-error --fail --request POST --data "$mode" \
        "$minio_fault_proxy_endpoint/__fault")
    [[ "$response" == "mode=${mode}" ]]
}

start_fault_proxy() {
    if [[ "${minio_fault_mode}" == "NONE" ]]; then
        return 0
    fi
    python3 "$e2e_root/minio-fault-proxy.py" --listen-port "$minio_fault_proxy_port" \
        --backend-port "$minio_port" >"$fault_proxy_log" 2>&1 &
    fault_proxy_pid=$!
    for attempt in $(seq 1 30); do
        if curl --silent --fail "$minio_fault_proxy_endpoint/__health" >/dev/null 2>&1; then
            set_fault_mode "$minio_fault_mode"
            return 0
        fi
        sleep 1
    done
    cat "$fault_proxy_log" >&2 || true
    echo "MinIO checkpoint fault proxy did not become ready" >&2
    return 1
}

oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
echo "Oxia checkout: $oxia_checkout@$oxia_sha"
echo "Compose project: $compose_project"
echo "Oxia endpoint: 127.0.0.1:$oxia_port"
echo "MinIO image: $minio_image@$minio_digest"
echo "MinIO bucket: $minio_bucket"
echo "MinIO fault mode: ${minio_fault_mode}"
echo "MinIO request timeout: ${minio_request_timeout_ms}ms"

docker run --detach --name "$minio_container" \
    --publish "127.0.0.1:${minio_port}:9000" \
    --env MINIO_ROOT_USER="$minio_access_key" \
    --env MINIO_ROOT_PASSWORD="$minio_secret_key" \
    "$minio_image" server /data --console-address :9001 >/dev/null
minio_started=1

minio_endpoint="http://127.0.0.1:${minio_port}"
ready=0
for attempt in $(seq 1 60); do
    if curl --silent --fail "$minio_endpoint/minio/health/ready" >/dev/null; then
        ready=1
        break
    fi
    sleep 1
done
if [[ "$ready" != 1 ]]; then
    echo "MinIO did not become ready" >&2
    exit 1
fi

curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" --request PUT \
    --url "${minio_endpoint}/${minio_bucket}" >/dev/null
curl --silent --show-error --fail --aws-sigv4 "aws:amz:${minio_region}:s3" \
    --user "${minio_access_key}:${minio_secret_key}" --request PUT \
    --header 'Content-Type: application/xml' \
    --data-binary '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>' \
    --url "${minio_endpoint}/${minio_bucket}?versioning" >/dev/null

start_fault_proxy

minio_object_store_endpoint="$minio_endpoint"
if [[ "${minio_fault_mode}" != "NONE" ]]; then
    minio_object_store_endpoint="$minio_fault_proxy_endpoint"
    echo "MinIO fault proxy endpoint: ${minio_object_store_endpoint}"
fi

export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
export NEREUS_DELAY_OXIA_E2E_PORT="$oxia_port"
compose up --build --detach
wait_for_oxia_health

smoke_environment=(
    "NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:$oxia_port"
    "NEREUS_DELAY_MINIO_ENDPOINT=$minio_object_store_endpoint"
    "NEREUS_DELAY_MINIO_ACCESS_KEY=$minio_access_key"
    "NEREUS_DELAY_MINIO_SECRET_KEY=$minio_secret_key"
    "NEREUS_DELAY_MINIO_BUCKET=$minio_bucket"
    "NEREUS_DELAY_MINIO_REGION=$minio_region"
    "GRADLE_USER_HOME=$gradle_user_home"
)
if [[ "${minio_fault_mode}" != "NONE" ]]; then
    smoke_environment+=(
        "NEREUS_DELAY_MINIO_FAULT_CONTROL=${minio_fault_proxy_endpoint}/__fault"
        "NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS=${minio_request_timeout_ms}"
    )
fi

run_smoke() {
    env "${smoke_environment[@]}" "$delay_root/gradlew" test --tests "$1" \
        --rerun-tasks --no-daemon --console=plain
}

run_fresh_process_authority() {
    local phase="$1"
    env "${smoke_environment[@]}" \
        "NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE=${phase}" \
        "NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PREFIX=${fresh_process_prefix}" \
        "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest.freshProcessPhaseReopensDurableControlAuthority \
        --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest.freshProcessPhaseReopensDurableRecoveryAuthorities \
        --rerun-tasks --no-daemon --console=plain
}

if [[ "${minio_fault_mode}" == "NONE" ]]; then
    fresh_process_prefix="${NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PREFIX:-nereus-delay-real-fresh-process/$(date +%s)-$$}"
    if [[ "${fresh_process_authority}" == "1" ]]; then
        run_fresh_process_authority WRITE
        run_fresh_process_authority READ
        echo "Oxia fresh-process control/recovery authority E2E passed: separate WRITE/READ Gradle JVMs"
    fi
    env "${smoke_environment[@]}" "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest \
        --tests io.nereusstream.delay.ownership.OxiaRealProtocolCapabilitySmokeTest \
        --tests io.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest \
        --tests io.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest \
        --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
        --tests io.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimePublishesToRealMinioAndOxia \
        --tests io.nereusstream.delay.store.OxiaRealCheckpointReapingSmokeTest.realOxiaOwnerAbandonmentReapsExactMinioCheckpointPrefix \
        --tests io.nereusstream.delay.store.OxiaRealObjectStoreCredentialRenewalSmokeTest.renewsRealOxiaLeaseAndFencesTheLiveAdapterAtHeadRotation \
        --rerun-tasks --no-daemon --console=plain
    echo "Oxia external control/protocol/Route/recovery authority E2E passed"
elif [[ "${minio_fault_mode}" == "PUT_503_BEFORE_COMMIT" \
    || "${minio_fault_mode}" == "PUT_TIMEOUT_BEFORE_COMMIT" ]]; then
    run_smoke io.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimeRemainsPendingWhenMinioCommitFailsBeforeProviderWrite
    set_fault_mode NONE
else
    run_smoke io.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimePublishesToRealMinioAndOxia
    set_fault_mode NONE
    set_fault_mode "$minio_fault_mode"
    run_smoke io.nereusstream.delay.store.OxiaRealCheckpointReapingSmokeTest.realOxiaOwnerAbandonmentReapsExactMinioCheckpointPrefix
    set_fault_mode NONE
fi

if [[ "${minio_fault_mode}" == "PUT_503_BEFORE_COMMIT" \
    || "${minio_fault_mode}" == "PUT_TIMEOUT_BEFORE_COMMIT" ]]; then
    echo "Oxia + MinIO Worker checkpoint pre-commit fail-closed E2E passed: real Oxia Intent/Owner authority and real MinIO partial-prefix cleanup (fault=${minio_fault_mode})"
else
    echo "Oxia + MinIO Worker checkpoint publication and REAPING E2E passed: real Oxia Intent/Catalog/Owner authority and real MinIO immutable objects (fault=${minio_fault_mode})"
fi
