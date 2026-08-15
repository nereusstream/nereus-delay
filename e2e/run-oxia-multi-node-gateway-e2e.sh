#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
oxia_checkout=${NEREUS_DELAY_OXIA_CHECKOUT:-"$delay_root/../../oxia"}
coordinator_1_port=${NEREUS_DELAY_OXIA_COORDINATOR_1_PORT:-16691}
coordinator_2_port=${NEREUS_DELAY_OXIA_COORDINATOR_2_PORT:-16692}
coordinator_3_port=${NEREUS_DELAY_OXIA_COORDINATOR_3_PORT:-16693}
data_server_1_port=${NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT:-16681}
data_server_2_port=${NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT:-16682}
data_server_3_port=${NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT:-16683}
gateway_port=${NEREUS_DELAY_GATEWAY_PORT:-22358}
delay_gradle_user_home=${NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME:-/tmp/nereus-delay-oxia-cluster-gateway-gradle}
compose_file="$e2e_root/docker-compose.oxia-cluster.yml"
compose_project="nereus-delay-oxia-cluster-gateway-e2e-$(date +%s)-$$"
tls_dir=$(mktemp -d -t nereus-delay-oxia-cluster-gateway-tls.XXXXXX)
failover_dir=$(mktemp -d -t nereus-delay-oxia-cluster-gateway-failover.XXXXXX)
failover_gate="$failover_dir/release"
failover_ready="$failover_dir/ready"
failover_log="$failover_dir/failover.log"
failover_pid=""

for required_command in docker openssl jq; do
    if ! command -v "$required_command" >/dev/null 2>&1; then
        echo "$required_command is required" >&2
        exit 1
    fi
done
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
echo "Coordinator endpoints: 127.0.0.1:$coordinator_1_port, 127.0.0.1:$coordinator_2_port, 127.0.0.1:$coordinator_3_port"
echo "Data-server endpoints: 127.0.0.1:$data_server_1_port, 127.0.0.1:$data_server_2_port, 127.0.0.1:$data_server_3_port"

compose() {
    docker compose --project-name "$compose_project" --file "$compose_file" "$@"
}

cleanup() {
    if [[ -n "$failover_pid" ]]; then
        kill "$failover_pid" >/dev/null 2>&1 || true
        wait "$failover_pid" >/dev/null 2>&1 || true
    fi
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
    rm -rf "$tls_dir" "$failover_dir"
}
trap cleanup EXIT INT TERM

generate_tls_material() {
    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$tls_dir/ca.key" -out "$tls_dir/ca.crt" -days 1 -sha256 \
        -subj "/CN=Nereus Delay Gateway multi-node E2E CA" \
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
        -subj "/CN=nereus-delay-gateway-cluster-client" >/dev/null 2>&1
    printf '%s\n' 'extendedKeyUsage=clientAuth' >"$tls_dir/client.ext"
    openssl x509 -req -in "$tls_dir/client.csr" \
        -CA "$tls_dir/ca.crt" -CAkey "$tls_dir/ca.key" -CAcreateserial \
        -out "$tls_dir/client.crt" -days 1 -sha256 -extfile "$tls_dir/client.ext" \
        >/dev/null 2>&1
    chmod 600 "$tls_dir"/*.key
}

wait_for_health() {
    local service=$1
    local port=$2
    local ready=0
    for attempt in $(seq 1 90); do
        if compose exec --no-TTY "$service" oxia health --host 127.0.0.1 --port "$port" --timeout 2s \
            >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done
    if [[ "$ready" != 1 ]]; then
        compose logs "$service" >&2 || true
        echo "$service did not become healthy" >&2
        return 1
    fi
}

admin() {
    local coordinator output
    for coordinator in coordinator-1 coordinator-2 coordinator-3; do
        if output=$(compose exec --no-TTY "$coordinator" oxia admin \
            --admin-address "$coordinator:6651" "$@" 2>/dev/null); then
            printf '%s\n' "$output"
            return 0
        fi
    done
    return 1
}

bootstrap_cluster() {
    wait_for_health coordinator-1 6651
    wait_for_health coordinator-2 6651
    wait_for_health coordinator-3 6651
    wait_for_health data-server-1 6648
    wait_for_health data-server-2 6648
    wait_for_health data-server-3 6648

    local dataserver_attempt created
    created=0
    for dataserver_attempt in $(seq 1 90); do
        if admin dataserver create ds-1 --public "127.0.0.1:$data_server_1_port" \
            --internal data-server-1:6649 -o json >/dev/null 2>&1; then
            created=1
            break
        fi
        sleep 1
    done
    if [[ "$created" != 1 ]]; then
        echo "failed to register ds-1" >&2
        return 1
    fi
    created=0
    for dataserver_attempt in $(seq 1 90); do
        if admin dataserver create ds-2 --public "127.0.0.1:$data_server_2_port" \
            --internal data-server-2:6649 -o json >/dev/null 2>&1; then
            created=1
            break
        fi
        sleep 1
    done
    if [[ "$created" != 1 ]]; then
        echo "failed to register ds-2" >&2
        return 1
    fi
    created=0
    for dataserver_attempt in $(seq 1 90); do
        if admin dataserver create ds-3 --public "127.0.0.1:$data_server_3_port" \
            --internal data-server-3:6649 -o json >/dev/null 2>&1; then
            created=1
            break
        fi
        sleep 1
    done
    if [[ "$created" != 1 ]]; then
        echo "failed to register ds-3" >&2
        return 1
    fi
    if ! admin namespace create default --initial-shards 1 --replication-factor 3 \
        --notifications --key-sorting hierarchical -o json >/dev/null 2>&1; then
        echo "failed to create the default namespace" >&2
        admin namespace get default -o json >&2 || true
        return 1
    fi
}

namespace_view() {
    admin namespace get default -o json 2>/dev/null
}

wait_for_namespace_leader() {
    local expected_absent=${1:-}
    local view leader
    for attempt in $(seq 1 120); do
        view=$(namespace_view || true)
        leader=$(printf '%s' "$view" | jq -r '.namespace_status.shards["0"].leader.name // empty' 2>/dev/null || true)
        if [[ -n "$leader" && "$leader" != "$expected_absent" ]]; then
            printf '%s\n' "$leader"
            return 0
        fi
        sleep 1
    done
    echo "namespace did not expose a stable leader" >&2
    printf '%s\n' "$view" >&2
    return 1
}

run_failover_smoke() {
    local initial_leader
    initial_leader=$(wait_for_namespace_leader)
    local stopped_service survivor_port
    case "$initial_leader" in
        ds-1)
            stopped_service=data-server-1
            survivor_port=$data_server_2_port
            ;;
        ds-2)
            stopped_service=data-server-2
            survivor_port=$data_server_1_port
            ;;
        ds-3)
            stopped_service=data-server-3
            survivor_port=$data_server_1_port
            ;;
        *)
            echo "unexpected Oxia leader identity: $initial_leader" >&2
            return 1
            ;;
    esac
    echo "Initial Oxia shard leader: $initial_leader ($stopped_service)"
    echo "Gateway bootstrap survivor: 127.0.0.1:$survivor_port"

    NEREUS_DELAY_OXIA_ENDPOINT="127.0.0.1:$survivor_port" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    NEREUS_DELAY_GATEWAY_PORT="$gateway_port" \
    NEREUS_DELAY_GATEWAY_SERVER_CERT="$tls_dir/server.crt" \
    NEREUS_DELAY_GATEWAY_SERVER_KEY="$tls_dir/server.key" \
    NEREUS_DELAY_GATEWAY_CA_CERT="$tls_dir/ca.crt" \
    NEREUS_DELAY_GATEWAY_CLIENT_CERT="$tls_dir/client.crt" \
    NEREUS_DELAY_GATEWAY_CLIENT_KEY="$tls_dir/client.key" \
    NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_GATE="$failover_gate" \
    NEREUS_DELAY_GATEWAY_MULTI_NODE_FAILOVER_READY="$failover_ready" \
    GRADLE_USER_HOME="$delay_gradle_user_home" \
        "$delay_root/gradlew" test \
            --tests io.nereusstream.delay.gateway.OxiaRealGatewayGrpcSmokeTest.gatewayRecoversAcrossRealOxiaDataServerFailover \
            --rerun-tasks \
            --no-daemon --console=plain >"$failover_log" 2>&1 &
    failover_pid=$!

    local ready=0
    for attempt in $(seq 1 180); do
        if [[ -f "$failover_ready" ]]; then
            ready=1
            break
        fi
        if ! kill -0 "$failover_pid" >/dev/null 2>&1; then
            wait "$failover_pid" || true
            cat "$failover_log" >&2 || true
            return 1
        fi
        sleep 1
    done
    if [[ "$ready" != 1 ]]; then
        echo "Gateway multi-node failover test did not reach its cut gate" >&2
        cat "$failover_log" >&2 || true
        return 1
    fi

    compose stop "$stopped_service"
    local successor
    successor=$(wait_for_namespace_leader "$initial_leader")
    echo "Oxia shard successor leader: $successor"
    touch "$failover_gate"

    local test_status=0
    wait "$failover_pid" || test_status=$?
    failover_pid=""
    cat "$failover_log"
    if [[ "$test_status" != 0 ]]; then
        compose logs "$stopped_service" >&2 || true
        return "$test_status"
    fi
}

generate_tls_material
export NEREUS_DELAY_OXIA_CHECKOUT="$oxia_checkout"
compose up --build --detach
bootstrap_cluster
run_failover_smoke

echo "Oxia multi-node Gateway failover E2E passed: session-bound Gateway reread the exact durable outcome after leader stop"
echo "Dockerized Oxia multi-node Gateway failover smoke passed for Oxia $oxia_sha"
