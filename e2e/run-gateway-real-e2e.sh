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
tls_dir=$(mktemp -d -t nereus-delay-gateway-tls.XXXXXX)

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

oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
echo "Oxia checkout: $oxia_checkout@$oxia_sha"
echo "Compose project: $compose_project"
echo "Oxia endpoint: 127.0.0.1:$oxia_port"
echo "Gateway endpoint: 127.0.0.1:$gateway_port"

compose() {
    docker compose --project-name "$compose_project" --file "$compose_file" "$@"
}

cleanup() {
    compose down --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$tls_dir"
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

generate_tls_material
export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
export NEREUS_DELAY_OXIA_E2E_PORT="$oxia_port"
compose up --build --detach
wait_for_oxia_health

NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$oxia_port" \
NEREUS_DELAY_OXIA_NAMESPACE=default \
NEREUS_DELAY_GATEWAY_PORT="$gateway_port" \
NEREUS_DELAY_GATEWAY_SERVER_CERT="$tls_dir/server.crt" \
NEREUS_DELAY_GATEWAY_SERVER_KEY="$tls_dir/server.key" \
NEREUS_DELAY_GATEWAY_CA_CERT="$tls_dir/ca.crt" \
NEREUS_DELAY_GATEWAY_CLIENT_CERT="$tls_dir/client.crt" \
NEREUS_DELAY_GATEWAY_CLIENT_KEY="$tls_dir/client.key" \
GRADLE_USER_HOME="$delay_gradle_user_home" \
    "$delay_root/gradlew" test \
        --tests io.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest \
        --rerun-tasks \
        --no-daemon --console=plain

echo "Gateway mTLS/RS256 network E2E passed: authenticated Schedule and invalid JWT rejection"
echo "Gateway restart/idempotency E2E passed: server restarted and returned the exact durable outcome without a second attempt"
echo "Gateway two-server CAS race E2E passed: independent Gateway servers converged on one durable physical attempt"
echo "Gateway Oxia durable E2E passed: admission released, one idempotency attempt, and two digest-only audit events"
echo "Dockerized Gateway real-service smoke passed for Oxia $oxia_sha"
