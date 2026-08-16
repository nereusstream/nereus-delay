#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_region=${NEREUS_DELAY_MINIO_REGION:-us-east-1}
minio_access_key=${NEREUS_DELAY_MINIO_ACCESS_KEY:-nereusdelay}
minio_secret_key=${NEREUS_DELAY_MINIO_SECRET_KEY:-nereus-delay-secret}
minio_bucket=${NEREUS_DELAY_MINIO_BUCKET:-nereus-delay-checkpoints-$(date +%s)-$$}
gradle_user_home=${NEREUS_DELAY_E2E_GRADLE_USER_HOME:-/tmp/nereus-delay-minio-gradle}
container_name="nereus-delay-minio-e2e-$(date +%s)-$$"
minio_port_override=${NEREUS_DELAY_MINIO_E2E_PORT:-}
container_started=0

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required" >&2
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required" >&2
    exit 1
fi
if ! docker image inspect "$minio_image" >/dev/null 2>&1; then
    echo "the locked MinIO image is not present locally: $minio_image" >&2
    exit 1
fi
if [[ -n "$minio_port_override" && ! "$minio_port_override" =~ ^[0-9]+$ ]]; then
    echo "NEREUS_DELAY_MINIO_E2E_PORT must be a numeric host port" >&2
    exit 1
fi
if [[ ! "$minio_bucket" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
    echo "NEREUS_DELAY_MINIO_BUCKET is not a canonical S3 bucket name: $minio_bucket" >&2
    exit 1
fi

repo_digest=$(docker image inspect --format '{{join .RepoDigests "\n"}}' "$minio_image" \
    | rg "@$minio_digest$" || true)
if [[ -z "$repo_digest" ]]; then
    echo "the local MinIO tag does not carry the expected repository digest: $minio_digest" >&2
    exit 1
fi

cleanup() {
    local status=$?
    if [[ "$status" != 0 && "$container_started" == 1 ]]; then
        docker logs "$container_name" >&2 || true
    fi
    if [[ "$container_started" == 1 ]]; then
        docker rm --force "$container_name" >/dev/null 2>&1 || true
    fi
    exit "$status"
}
trap cleanup EXIT INT TERM

publish_args=(--publish 127.0.0.1::9000)
if [[ -n "$minio_port_override" ]]; then
    publish_args=(--publish "127.0.0.1:${minio_port_override}:9000")
fi

docker run --detach --name "$container_name" "${publish_args[@]}" \
    --env MINIO_ROOT_USER="$minio_access_key" \
    --env MINIO_ROOT_PASSWORD="$minio_secret_key" \
    "$minio_image" server /data --console-address :9001 >/dev/null
container_started=1

mapping=""
for attempt in $(seq 1 30); do
    mapping=$(docker port "$container_name" 9000/tcp 2>/dev/null || true)
    if [[ -n "$mapping" ]]; then
        break
    fi
    sleep 1
done
if [[ -z "$mapping" ]]; then
    echo "MinIO did not publish its S3 port" >&2
    exit 1
fi
minio_port=${mapping##*:}
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

echo "MinIO image: $minio_image"
echo "MinIO image ID: $(docker image inspect --format '{{.Id}}' "$minio_image")"
echo "MinIO image digest: $minio_digest"
echo "MinIO container: $container_name"
echo "MinIO endpoint: $minio_endpoint"
echo "MinIO bucket: $minio_bucket"

NEREUS_DELAY_MINIO_ENDPOINT="$minio_endpoint" \
NEREUS_DELAY_MINIO_ACCESS_KEY="$minio_access_key" \
NEREUS_DELAY_MINIO_SECRET_KEY="$minio_secret_key" \
NEREUS_DELAY_MINIO_BUCKET="$minio_bucket" \
NEREUS_DELAY_MINIO_REGION="$minio_region" \
GRADLE_USER_HOME="$gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest \
        --rerun-tasks \
        --no-daemon --console=plain

echo "Dockerized MinIO S3-compatible checkpoint smoke passed for $minio_image@$minio_digest"
