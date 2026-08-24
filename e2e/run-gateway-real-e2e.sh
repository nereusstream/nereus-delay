#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}
oxia_port=${NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT:-16667}
gateway_port=${NEREUS_DELAY_GATEWAY_PORT:-22349}
delay_gradle_user_home=${NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME:-/tmp/nereus-delay-gateway-e2e-gradle}
compose_file="$e2e_root/docker-compose.oxia.yml"
compose_project="nereus-delay-gateway-e2e-$(date +%s)-$$"
oxia_image="${compose_project}-oxia"
tls_dir=$(mktemp -d -t nereus-delay-gateway-tls.XXXXXX)
session_churn=${NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN:-0}
session_churn_pause_seconds=${NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_PAUSE_SECONDS:-5}
session_churn_dir=$(mktemp -d -t nereus-delay-gateway-session-churn.XXXXXX)
session_churn_gate="$session_churn_dir/release"
session_churn_ready="$session_churn_dir/ready"
session_churn_recovery_gate="$session_churn_dir/recovery-release"
session_churn_recovery_ready="$session_churn_dir/recovery-ready"
session_churn_log="$session_churn_dir/session-churn.log"
session_churn_state_dump_dir=${NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_STATE_DUMP_DIR:-"$session_churn_dir/state"}
session_churn_pid=""

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required" >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "docker compose is required" >&2
    exit 1
fi
if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required" >&2
    exit 1
fi
if ! git -C "$oxia_checkout" rev-parse --verify HEAD >/dev/null 2>&1; then
    echo "NEREUS_DELAY_OXIA_CHECKOUT is not a Git checkout: $oxia_checkout" >&2
    exit 1
fi
if [[ "$session_churn" != 0 && "$session_churn" != 1 ]]; then
    echo "NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN must be 0 or 1" >&2
    exit 1
fi
if [[ ! "$session_churn_pause_seconds" =~ ^[0-9]+$ ]]; then
    echo "NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_PAUSE_SECONDS must be a non-negative integer" >&2
    exit 1
fi

oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
echo "Oxia checkout: $oxia_checkout@$oxia_sha"
echo "Compose project: $compose_project"
echo "Oxia endpoint: 127.0.0.1:$oxia_port"
echo "Gateway endpoint: 127.0.0.1:$gateway_port"

compose() {
    docker compose --project-name "$compose_project" --file "$compose_file" "$@"
}

cleanup() {
    if [[ -n "$session_churn_pid" ]]; then
        kill "$session_churn_pid" >/dev/null 2>&1 || true
        wait "$session_churn_pid" >/dev/null 2>&1 || true
    fi
    compose down --remove-orphans >/dev/null 2>&1 || true
    docker image rm "${oxia_image}:latest" >/dev/null 2>&1 || true
    rm -rf "$tls_dir"
    rm -rf "$session_churn_dir"
}
trap cleanup EXIT INT TERM

generate_tls_material() {
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$tls_dir/ca.key" -out "$tls_dir/ca.crt" -days 1 -sha256 \
        -subj "/CN=Nereus Delay Gateway E2E CA" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        >/dev/null 2>&1

    openssl req -newkey rsa:2048 -nodes \
        -keyout "$tls_dir/server.key" -out "$tls_dir/server.csr" \
        -subj "/CN=localhost" >/dev/null 2>&1
    printf '%s\n' \
        'subjectAltName=IP:127.0.0.1' \
        'extendedKeyUsage=serverAuth' >"$tls_dir/server.ext"
    openssl x509 -req -in "$tls_dir/server.csr" \
        -CA "$tls_dir/ca.crt" -CAkey "$tls_dir/ca.key" -CAcreateserial \
        -out "$tls_dir/server.crt" -days 1 -sha256 -extfile "$tls_dir/server.ext" \
        >/dev/null 2>&1

    openssl req -newkey rsa:2048 -nodes \
        -keyout "$tls_dir/client.key" -out "$tls_dir/client.csr" \
        -subj "/CN=nereus-delay-gateway-client" >/dev/null 2>&1
    printf '%s\n' 'extendedKeyUsage=clientAuth' >"$tls_dir/client.ext"
    openssl x509 -req -in "$tls_dir/client.csr" \
        -CA "$tls_dir/ca.crt" -CAkey "$tls_dir/ca.key" -CAcreateserial \
        -out "$tls_dir/client.crt" -days 1 -sha256 -extfile "$tls_dir/client.ext" \
        >/dev/null 2>&1

    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$tls_dir/rotated-ca.key" -out "$tls_dir/rotated-ca.crt" -days 1 -sha256 \
        -subj "/CN=Nereus Delay Gateway Rotated E2E CA" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" \
        >/dev/null 2>&1

    openssl req -newkey rsa:2048 -nodes \
        -keyout "$tls_dir/rotated-server.key" -out "$tls_dir/rotated-server.csr" \
        -subj "/CN=localhost" >/dev/null 2>&1
    printf '%s\n' \
        'subjectAltName=IP:127.0.0.1' \
        'extendedKeyUsage=serverAuth' >"$tls_dir/rotated-server.ext"
    openssl x509 -req -in "$tls_dir/rotated-server.csr" \
        -CA "$tls_dir/rotated-ca.crt" -CAkey "$tls_dir/rotated-ca.key" \
        -CAcreateserial -CAserial "$tls_dir/rotated-ca.srl" \
        -out "$tls_dir/rotated-server.crt" -days 1 -sha256 -extfile "$tls_dir/rotated-server.ext" \
        >/dev/null 2>&1

    openssl req -newkey rsa:2048 -nodes \
        -keyout "$tls_dir/rotated-client.key" -out "$tls_dir/rotated-client.csr" \
        -subj "/CN=nereus-delay-gateway-rotated-client" >/dev/null 2>&1
    printf '%s\n' 'extendedKeyUsage=clientAuth' >"$tls_dir/rotated-client.ext"
    openssl x509 -req -in "$tls_dir/rotated-client.csr" \
        -CA "$tls_dir/rotated-ca.crt" -CAkey "$tls_dir/rotated-ca.key" \
        -CAserial "$tls_dir/rotated-ca.srl" \
        -out "$tls_dir/rotated-client.crt" -days 1 -sha256 -extfile "$tls_dir/rotated-client.ext" \
        >/dev/null 2>&1
    chmod 600 "$tls_dir"/*.key
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
        compose logs oxia
        echo "Oxia did not become healthy" >&2
        return 1
    fi
}

run_session_churn_smoke() {
    NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    NEREUS_DELAY_GATEWAY_PORT="$gateway_port" \
    NEREUS_DELAY_GATEWAY_SERVER_CERT="$tls_dir/server.crt" \
    NEREUS_DELAY_GATEWAY_SERVER_KEY="$tls_dir/server.key" \
    NEREUS_DELAY_GATEWAY_CA_CERT="$tls_dir/ca.crt" \
    NEREUS_DELAY_GATEWAY_CLIENT_CERT="$tls_dir/client.crt" \
    NEREUS_DELAY_GATEWAY_CLIENT_KEY="$tls_dir/client.key" \
    NEREUS_DELAY_GATEWAY_SESSION_CHURN_GATE="$session_churn_gate" \
    NEREUS_DELAY_GATEWAY_SESSION_CHURN_READY="$session_churn_ready" \
    NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_GATE="$session_churn_recovery_gate" \
    NEREUS_DELAY_GATEWAY_SESSION_CHURN_RECOVERY_READY="$session_churn_recovery_ready" \
    NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_STATE_DUMP_DIR="$session_churn_state_dump_dir" \
    GRADLE_USER_HOME="$delay_gradle_user_home" \
        "$delay_root/gradlew" test \
            --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.gatewayDurableRecordsRecoverAfterOxiaSessionChurn \
            --rerun-tasks \
            --no-daemon --console=plain >"$session_churn_log" 2>&1 &
    session_churn_pid=$!
    local ready=0
    for attempt in $(seq 1 120); do
        if [[ -f "$session_churn_ready" ]]; then
            ready=1
            break
        fi
        if ! kill -0 "$session_churn_pid" >/dev/null 2>&1; then
            wait "$session_churn_pid" || true
            cat "$session_churn_log" >&2 || true
            return 1
        fi
        sleep 1
    done
    if [[ "$ready" != 1 ]]; then
        echo "Gateway Oxia session churn test did not reach the restart gate" >&2
        cat "$session_churn_log" >&2 || true
        return 1
    fi

    compose stop oxia
    sleep "$session_churn_pause_seconds"

    # Keep Oxia unavailable while the test exercises the old handles. Oxia
    # persists session metadata across a process restart, so restarting before
    # this assertion would allow the old client to resume the same session and
    # would not prove the Gateway outage fence. The test signals when its
    # stale-handle assertions are complete; only then do we restore authority.
    touch "$session_churn_gate"
    local recovery_ready=0
    for attempt in $(seq 1 120); do
        if [[ -f "$session_churn_recovery_ready" ]]; then
            recovery_ready=1
            break
        fi
        if ! kill -0 "$session_churn_pid" >/dev/null 2>&1; then
            wait "$session_churn_pid" || true
            cat "$session_churn_log" >&2 || true
            return 1
        fi
        sleep 1
    done
    if [[ "$recovery_ready" != 1 ]]; then
        echo "Gateway Oxia session churn test did not reach the recovery gate" >&2
        cat "$session_churn_log" >&2 || true
        return 1
    fi

    compose start oxia
    wait_for_oxia_health
    touch "$session_churn_recovery_gate"
    local test_status=0
    wait "$session_churn_pid" || test_status=$?
    session_churn_pid=""
    cat "$session_churn_log"
    if [[ "$test_status" != 0 ]]; then
        return "$test_status"
    fi
    if [[ ! -s "$session_churn_state_dump_dir/before-oxia-restart.json" \
        || ! -s "$session_churn_state_dump_dir/after-oxia-restart.json" ]]; then
        echo "Gateway Oxia session churn durable state dumps are missing" >&2
        return 1
    fi
    jq -e '
        .schema == "nereus-delay-chaos-durable-state-dump-v1"
        and .cell == "gateway-oxia-session-churn"
        and .phase == "BEFORE_OXIA_RESTART"
        and .durable_store_read == true
        and .dump_forced == true
    ' "$session_churn_state_dump_dir/before-oxia-restart.json" >/dev/null
    jq -e '
        .schema == "nereus-delay-chaos-durable-state-dump-v1"
        and .cell == "gateway-oxia-session-churn"
        and .phase == "RECOVERED_AFTER_OXIA_RESTART"
        and .stale_session_failed_closed == true
        and .exact_outcome_recovered == true
        and .oxia_process_restarted == true
        and .durable_store_read == true
        and .dump_forced == true
    ' "$session_churn_state_dump_dir/after-oxia-restart.json" >/dev/null
}

generate_tls_material
export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
export NEREUS_DELAY_OXIA_E2E_PORT="$oxia_port"
compose up --build --detach
wait_for_oxia_health

if [[ "$session_churn" == 1 ]]; then
    run_session_churn_smoke
    echo "Gateway Oxia session churn E2E passed: stale durable sessions failed closed and recovery reread one exact outcome"
    echo "Dockerized Gateway Oxia session churn smoke passed for Oxia $oxia_sha"
    exit 0
fi

NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
NEREUS_DELAY_OXIA_NAMESPACE=default \
NEREUS_DELAY_GATEWAY_PORT="$gateway_port" \
NEREUS_DELAY_GATEWAY_SERVER_CERT="$tls_dir/server.crt" \
NEREUS_DELAY_GATEWAY_SERVER_KEY="$tls_dir/server.key" \
NEREUS_DELAY_GATEWAY_CA_CERT="$tls_dir/ca.crt" \
NEREUS_DELAY_GATEWAY_CLIENT_CERT="$tls_dir/client.crt" \
NEREUS_DELAY_GATEWAY_CLIENT_KEY="$tls_dir/client.key" \
NEREUS_DELAY_GATEWAY_ROTATED_SERVER_CERT="$tls_dir/rotated-server.crt" \
NEREUS_DELAY_GATEWAY_ROTATED_SERVER_KEY="$tls_dir/rotated-server.key" \
NEREUS_DELAY_GATEWAY_ROTATED_CA_CERT="$tls_dir/rotated-ca.crt" \
NEREUS_DELAY_GATEWAY_ROTATED_CLIENT_CERT="$tls_dir/rotated-client.crt" \
NEREUS_DELAY_GATEWAY_ROTATED_CLIENT_KEY="$tls_dir/rotated-client.key" \
GRADLE_USER_HOME="$delay_gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests com.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest \
        --rerun-tasks \
        --no-daemon --console=plain

echo "Gateway mTLS/RS256 network E2E passed: authenticated Schedule and invalid JWT rejection"
echo "Gateway restart/idempotency E2E passed: server restarted and returned the exact durable outcome without a second attempt"
echo "Gateway certificate rotation E2E passed: old mTLS client rejected and new certificate reread the exact durable outcome"
echo "Gateway two-server CAS race E2E passed: independent Gateway servers converged on one durable physical attempt"
echo "Gateway Oxia durable E2E passed: admission released, one idempotency attempt, and two digest-only audit events"
echo "Dockerized Gateway real-service smoke passed for Oxia $oxia_sha"
