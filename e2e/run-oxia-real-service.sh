#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}
oxia_port=${NEREUS_DELAY_OXIA_E2E_PORT:-16649}
delay_gradle_user_home=${NEREUS_DELAY_E2E_GRADLE_USER_HOME:-/tmp/nereus-delay-oxia-docker-gradle}
compose_file="$e2e_root/docker-compose.oxia.yml"
compose_project="nereus-delay-v1-oxia-e2e-$(date +%s)-$$"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required" >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose is required" >&2
    exit 1
fi
if ! git -C "$oxia_checkout" rev-parse --verify HEAD >/dev/null 2>&1; then
    echo "NEREUS_DELAY_OXIA_CHECKOUT is not a Git checkout: $oxia_checkout" >&2
    exit 1
fi

oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
echo "Oxia checkout: $oxia_checkout@$oxia_sha"
echo "Compose project: $compose_project"
echo "Host endpoint: 127.0.0.1:$oxia_port"

compose() {
    docker compose --project-name "$compose_project" --file "$compose_file" "$@"
}

cleanup() {
    compose down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
export NEREUS_DELAY_OXIA_E2E_PORT="$oxia_port"
compose up --build --detach

ready=0
for attempt in $(seq 1 60); do
    if compose exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [[ "$ready" != 1 ]]; then
    compose logs oxia
    echo "Oxia did not become healthy" >&2
    exit 1
fi

NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
NEREUS_DELAY_OXIA_NAMESPACE=default \
GRADLE_USER_HOME="$delay_gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.ownership.OxiaRealServiceSmokeTest \
        --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest \
        --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
        --tests io.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest \
        --tests io.nereusstream.delay.gateway.OxiaRealGatewayAuditSinkSmokeTest \
        --tests io.nereusstream.delay.gateway.OxiaRealGatewayAdmissionSmokeTest \
        --no-daemon --console=plain

echo "Dockerized Oxia real-service smoke passed for $oxia_sha"
