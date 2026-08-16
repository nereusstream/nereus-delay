#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
minio_image="quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z"
minio_digest="sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
minio_region=${NEREUS_DELAY_MINIO_REGION:-us-east-1}
minio_access_key=${NEREUS_DELAY_MINIO_ACCESS_KEY:-nereusdelayfault}
minio_secret_key=${NEREUS_DELAY_MINIO_SECRET_KEY:-nereus-delay-fault-secret}
minio_bucket=${NEREUS_DELAY_MINIO_BUCKET:-$(date +%s)-fault-$$}
gradle_user_home=${NEREUS_DELAY_E2E_GRADLE_USER_HOME:-/tmp/nereus-delay-minio-fault-gradle}
minio_port=${NEREUS_DELAY_MINIO_FAULT_MINIO_PORT:-31621}
proxy_port=${NEREUS_DELAY_MINIO_FAULT_PROXY_PORT:-31622}
container_name="nereus-delay-minio-fault-e2e-$(date +%s)-$$"
proxy_log=$(mktemp -t nereus-delay-minio-fault-proxy.XXXXXX).log
proxy_pid=""
container_started=0

if ! command -v docker >/dev/null 2>&1 || ! command -v curl >/dev/null 2>&1 \
    || ! command -v python3 >/dev/null 2>&1; then
    echo "docker, curl and python3 are required" >&2
    exit 1
fi
if [[ ! "$minio_port" =~ ^[0-9]+$ || ! "$proxy_port" =~ ^[0-9]+$ ]]; then
    echo "MinIO fault E2E ports must be numeric" >&2
    exit 1
fi
if [[ "$minio_port" == "$proxy_port" ]]; then
    echo "MinIO and fault proxy ports must differ" >&2
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
if ! docker image inspect --format '{{join .RepoDigests "\n"}}' "$minio_image" \
    | rg -F "@$minio_digest" >/dev/null; then
    echo "the local MinIO tag does not carry the expected repository digest: $minio_digest" >&2
    exit 1
fi
if command -v lsof >/dev/null 2>&1; then
    for port in "$minio_port" "$proxy_port"; do
        if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
            echo "required fault E2E port is already listening: $port" >&2
            exit 1
        fi
    done
fi

cleanup() {
    local status=$?
    if [[ "$status" != 0 ]]; then
        if [[ -n "$proxy_pid" ]]; then
            cat "$proxy_log" >&2 || true
        fi
        if [[ "$container_started" == 1 ]]; then
            docker logs "$container_name" >&2 || true
        fi
    fi
    if [[ -n "$proxy_pid" ]]; then
        kill "$proxy_pid" >/dev/null 2>&1 || true
        wait "$proxy_pid" >/dev/null 2>&1 || true
    fi
    if [[ "$container_started" == 1 ]]; then
        docker rm --force "$container_name" >/dev/null 2>&1 || true
    fi
    rm -f "$proxy_log"
    exit "$status"
}
trap cleanup EXIT INT TERM

docker run --detach --name "$container_name" \
    --publish "127.0.0.1:${minio_port}:9000" \
    --env MINIO_ROOT_USER="$minio_access_key" \
    --env MINIO_ROOT_PASSWORD="$minio_secret_key" \
    "$minio_image" server /data --console-address :9001 >/dev/null
container_started=1

minio_endpoint="http://127.0.0.1:${minio_port}"
proxy_endpoint="http://127.0.0.1:${proxy_port}"
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

python3 "$e2e_root/minio-fault-proxy.py" --listen-port "$proxy_port" \
    --backend-port "$minio_port" >"$proxy_log" 2>&1 &
proxy_pid=$!
ready=0
for attempt in $(seq 1 30); do
    if curl --silent --fail "$proxy_endpoint/__health" >/dev/null; then
        ready=1
        break
    fi
    sleep 1
done
if [[ "$ready" != 1 ]]; then
    echo "MinIO fault proxy did not become ready" >&2
    exit 1
fi

echo "Delay source: $(git -C "$delay_root" rev-parse HEAD)"
echo "MinIO image: $minio_image@$minio_digest"
echo "MinIO image ID: $(docker image inspect --format '{{.Id}}' "$minio_image")"
echo "MinIO container: $container_name"
echo "MinIO endpoint: $minio_endpoint"
echo "Fault proxy endpoint: $proxy_endpoint"
echo "MinIO bucket: $minio_bucket"

NEREUS_DELAY_MINIO_ENDPOINT="$proxy_endpoint" \
NEREUS_DELAY_MINIO_FAULT_CONTROL="$proxy_endpoint/__fault" \
NEREUS_DELAY_MINIO_ACCESS_KEY="$minio_access_key" \
NEREUS_DELAY_MINIO_SECRET_KEY="$minio_secret_key" \
NEREUS_DELAY_MINIO_BUCKET="$minio_bucket" \
NEREUS_DELAY_MINIO_REGION="$minio_region" \
GRADLE_USER_HOME="$gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioFiveHundredAfterCommitResolvesByExactReadback \
        --tests io.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioFiveHundredBeforeCommitRemainsFailClosed \
        --tests io.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioTimeoutAfterCommitResolvesByExactReadback \
        --tests io.nereusstream.delay.store.S3CompatibleMinioRealSmokeTest.realMinioCredentialConfigurationDriftFailsClosed \
        --rerun-tasks --no-daemon --console=plain

echo "Real MinIO Object Store 5xx/timeout/config-drift fault E2E passed: exact immutable read-back resolved post-commit 503/timeout and real MinIO rejected drifted credentials"
