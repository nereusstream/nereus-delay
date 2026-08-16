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
if ! git -C "$oxia_checkout" rev-parse --verify HEAD >/dev/null 2>&1; then
    echo "NEREUS_DELAY_OXIA_CHECKOUT is not a Git checkout: $oxia_checkout" >&2
    exit 1
fi
if [[ ! "$oxia_port" =~ ^[0-9]+$ || ! "$minio_port" =~ ^[0-9]+$ ]]; then
    echo "checkpoint E2E ports must be numeric" >&2
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
    compose down --remove-orphans --volumes >/dev/null 2>&1 || true
    if [[ "$minio_started" == 1 ]]; then
        docker rm --force "$minio_container" >/dev/null 2>&1 || true
    fi
    if docker image inspect "$oxia_image" >/dev/null 2>&1; then
        docker image rm "$oxia_image" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
echo "Oxia checkout: $oxia_checkout@$oxia_sha"
echo "Compose project: $compose_project"
echo "Oxia endpoint: 127.0.0.1:$oxia_port"
echo "MinIO image: $minio_image@$minio_digest"
echo "MinIO bucket: $minio_bucket"

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

export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
export NEREUS_DELAY_OXIA_E2E_PORT="$oxia_port"
compose up --build --detach
wait_for_oxia_health

NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
NEREUS_DELAY_MINIO_ENDPOINT="$minio_endpoint" \
NEREUS_DELAY_MINIO_ACCESS_KEY="$minio_access_key" \
NEREUS_DELAY_MINIO_SECRET_KEY="$minio_secret_key" \
NEREUS_DELAY_MINIO_BUCKET="$minio_bucket" \
NEREUS_DELAY_MINIO_REGION="$minio_region" \
GRADLE_USER_HOME="$gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest.workerCheckpointRuntimePublishesToRealMinioAndOxia \
        --tests io.nereusstream.delay.store.OxiaRealCheckpointReapingSmokeTest.realOxiaOwnerAbandonmentReapsExactMinioCheckpointPrefix \
        --rerun-tasks \
        --no-daemon --console=plain

echo "Oxia + MinIO Worker checkpoint publication and REAPING E2E passed: real Oxia Intent/Catalog/Owner authority and real MinIO immutable objects"
