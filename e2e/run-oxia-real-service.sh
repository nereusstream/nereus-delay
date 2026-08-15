#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}
oxia_port=${NEREUS_DELAY_OXIA_E2E_PORT:-16649}
delay_gradle_user_home=${NEREUS_DELAY_E2E_GRADLE_USER_HOME:-/tmp/nereus-delay-oxia-docker-gradle}
compose_file="$e2e_root/docker-compose.oxia.yml"
compose_project="nereus-delay-v1-oxia-e2e-$(date +%s)-$$"
route_restart=${NEREUS_DELAY_OXIA_ROUTE_RESTART:-0}
route_restart_only=${NEREUS_DELAY_OXIA_ROUTE_RESTART_ONLY:-0}
route_restart_notifications=${NEREUS_DELAY_OXIA_ROUTE_RESTART_NOTIFICATIONS:-0}
route_restart_pause_seconds=${NEREUS_DELAY_OXIA_ROUTE_RESTART_PAUSE_SECONDS:-5}

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
if [[ "$route_restart" != 0 && "$route_restart" != 1 ]]; then
    echo "NEREUS_DELAY_OXIA_ROUTE_RESTART must be 0 or 1" >&2
    exit 1
fi
if [[ "$route_restart_only" != 0 && "$route_restart_only" != 1 ]]; then
    echo "NEREUS_DELAY_OXIA_ROUTE_RESTART_ONLY must be 0 or 1" >&2
    exit 1
fi
if [[ "$route_restart_only" == 1 && "$route_restart" != 1 ]]; then
    echo "NEREUS_DELAY_OXIA_ROUTE_RESTART_ONLY requires NEREUS_DELAY_OXIA_ROUTE_RESTART=1" >&2
    exit 1
fi
if [[ "$route_restart_notifications" != 0 && "$route_restart_notifications" != 1 ]]; then
    echo "NEREUS_DELAY_OXIA_ROUTE_RESTART_NOTIFICATIONS must be 0 or 1" >&2
    exit 1
fi
if [[ "$route_restart_notifications" == 1 && "$route_restart" != 1 ]]; then
    echo "NEREUS_DELAY_OXIA_ROUTE_RESTART_NOTIFICATIONS requires NEREUS_DELAY_OXIA_ROUTE_RESTART=1" >&2
    exit 1
fi
if [[ ! "$route_restart_pause_seconds" =~ ^[0-9]+$ ]]; then
    echo "NEREUS_DELAY_OXIA_ROUTE_RESTART_PAUSE_SECONDS must be a non-negative integer" >&2
    exit 1
fi

route_restart_dir=$(mktemp -d -t nereus-delay-oxia-route-restart.XXXXXX)
route_restart_gate="$route_restart_dir/release"
route_restart_ready="$route_restart_dir/ready"
route_restart_log="$route_restart_dir/route-restart.log"
route_restart_pid=""

oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
echo "Oxia checkout: $oxia_checkout@$oxia_sha"
echo "Compose project: $compose_project"
echo "Host endpoint: 127.0.0.1:$oxia_port"

compose() {
    docker compose --project-name "$compose_project" --file "$compose_file" "$@"
}

cleanup() {
    if [[ -n "$route_restart_pid" ]]; then
        kill "$route_restart_pid" >/dev/null 2>&1 || true
        wait "$route_restart_pid" >/dev/null 2>&1 || true
    fi
    compose down --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$route_restart_dir"
}
trap cleanup EXIT INT TERM

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
        compose logs oxia
        echo "Oxia did not become healthy" >&2
        return 1
    fi
}

run_route_restart_smoke() {
    local route_restart_test="io.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteProviderRecoversAfterRealOxiaRestart"
    if [[ "$route_restart_notifications" == 1 ]]; then
        route_restart_test="io.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest.signedRouteNotificationsRecoverAfterRealOxiaRestart"
    fi
    NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    NEREUS_DELAY_OXIA_ROUTE_RESTART_GATE="$route_restart_gate" \
    NEREUS_DELAY_OXIA_ROUTE_RESTART_READY="$route_restart_ready" \
    GRADLE_USER_HOME="$delay_gradle_user_home" \
        "$delay_root/gradlew" test \
            --tests "$route_restart_test" \
            --rerun-tasks \
            --no-daemon --console=plain >"$route_restart_log" 2>&1 &
    route_restart_pid=$!
    local ready=0
    for attempt in $(seq 1 120); do
        if [[ -f "$route_restart_ready" ]]; then
            ready=1
            break
        fi
        if ! kill -0 "$route_restart_pid" >/dev/null 2>&1; then
            wait "$route_restart_pid" || true
            cat "$route_restart_log" >&2 || true
            return 1
        fi
        sleep 1
    done
    if [[ "$ready" != 1 ]]; then
        echo "Oxia Route provider did not reach the restart gate" >&2
        cat "$route_restart_log" >&2 || true
        return 1
    fi

    compose stop oxia
    if [[ "$route_restart_notifications" == 1 ]]; then
        sleep "$route_restart_pause_seconds"
    fi
    compose start oxia
    wait_for_oxia_health
    touch "$route_restart_gate"
    local test_status=0
    wait "$route_restart_pid" || test_status=$?
    route_restart_pid=""
    cat "$route_restart_log"
    if [[ "$test_status" != 0 ]]; then
        return "$test_status"
    fi
}

export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
export NEREUS_DELAY_OXIA_E2E_PORT="$oxia_port"
compose up --build --detach
wait_for_oxia_health

if [[ "$route_restart" == 1 ]]; then
    run_route_restart_smoke
    if [[ "$route_restart_only" == 1 ]]; then
        if [[ "$route_restart_notifications" == 1 ]]; then
            echo "Dockerized Oxia Route notification restart smoke passed: session rotation and notification stream recovery"
            exit 0
        fi
        echo "Dockerized Oxia Route restart smoke passed: provider session recovery and signed Route cache rebuild"
        exit 0
    fi
fi

NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
NEREUS_DELAY_OXIA_NAMESPACE=default \
GRADLE_USER_HOME="$delay_gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.ownership.OxiaRealServiceSmokeTest \
        --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest \
        --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
        --tests io.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest \
        --tests io.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest \
        --tests io.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest \
        --tests io.nereusstream.delay.gateway.OxiaRealGatewayAuditSinkSmokeTest \
        --tests io.nereusstream.delay.gateway.OxiaRealGatewayAdmissionSmokeTest \
        --no-daemon --console=plain

echo "Dockerized Oxia real-service smoke passed for $oxia_sha"
