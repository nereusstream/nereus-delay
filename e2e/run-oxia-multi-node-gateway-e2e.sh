#!/usr/bin/env bash
set -euo pipefail

e2e_root=$(cd "$(dirname "$0")" && pwd)
delay_root=$(cd "$e2e_root/.." && pwd)
artifact_dir=${NEREUS_DELAY_OXIA_MULTI_NODE_GATEWAY_ARTIFACT_DIR:-$(mktemp -d -t nereus-delay-oxia-multi-node-gateway.XXXXXX)}
artifact="$artifact_dir/oxia-multi-node-gateway-e2e.json"
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
mkdir -p "$artifact_dir"
if [[ -n "$(find "$artifact_dir" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "artifact directory must be empty: $artifact_dir" >&2
    exit 1
fi

delay_branch=$(git -C "$delay_root" branch --show-current 2>/dev/null || true)
kafka_checkout=${NEREUS_DELAY_KAFKA_CHECKOUT:-"$delay_root/../../kafka-worktrees/nereus-delay-k1"}
pulsar_checkout=${NEREUS_DELAY_PULSAR_CHECKOUT:-"$delay_root/../../pulsar-worktrees/nereus-delay-p1"}
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
failover_log="$artifact_dir/failover.log"
failover_pid=""
delay_sha="unknown"
kafka_sha="unknown"
pulsar_sha="unknown"
oxia_sha="unknown"
initial_leader="unknown"
successor_leader="unknown"
stopped_service="unknown"
survivor_port="unknown"
cut_gate_reached=false
test_status=1
cleanup_status="PASS"
cleanup_detail="exact Compose project cleanup has not run yet"
compose_image_ids_before='[]'
compose_image_ids_after='[]'
removed_container_ids='[]'
removed_image_ids='[]'
printf '[]\n' >"$artifact_dir/.removed-containers.json"
printf '[]\n' >"$artifact_dir/.removed-images.json"

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
require_source() {
    local name=$1 path=$2 branch=$3
    if ! git -C "$path" rev-parse --verify HEAD >/dev/null 2>&1; then
        echo "$name checkout is not a Git checkout: $path" >&2
        return 1
    fi
    if [[ -n "$(git -C "$path" status --porcelain)" ]]; then
        echo "$name checkout is dirty: $path" >&2
        return 1
    fi
    if [[ "$(git -C "$path" branch --show-current)" != "$branch" ]]; then
        echo "$name checkout is not on $branch: $(git -C "$path" branch --show-current)" >&2
        return 1
    fi
}

require_source Delay "$delay_root" nereus/delay-full-implementation-v1
require_source Kafka "$kafka_checkout" nereus/delay-guarded-producer-v1
require_source Pulsar "$pulsar_checkout" nereus/delay-resource-guard-v1
require_source Oxia "$oxia_checkout" main
delay_sha=$(git -C "$delay_root" rev-parse HEAD)
kafka_sha=$(git -C "$kafka_checkout" rev-parse HEAD)
pulsar_sha=$(git -C "$pulsar_checkout" rev-parse HEAD)
oxia_sha=$(git -C "$oxia_checkout" rev-parse HEAD)
echo "Delay checkout: $delay_root@$delay_sha"
echo "Kafka checkout: $kafka_checkout@$kafka_sha"
echo "Pulsar checkout: $pulsar_checkout@$pulsar_sha"
echo "Oxia checkout: $oxia_checkout@$oxia_sha"
echo "Compose project: $compose_project"
echo "Coordinator endpoints: 127.0.0.1:$coordinator_1_port, 127.0.0.1:$coordinator_2_port, 127.0.0.1:$coordinator_3_port"
echo "Data-server endpoints: 127.0.0.1:$data_server_1_port, 127.0.0.1:$data_server_2_port, 127.0.0.1:$data_server_3_port"

compose() {
    docker compose --project-name "$compose_project" --file "$compose_file" "$@"
}

json_array_from_lines() {
    local lines=${1:-}
    if [[ -z "$lines" ]]; then
        printf '[]'
    else
        printf '%s\n' "$lines" | jq -R -s 'split("\n") | map(select(length > 0)) | unique'
    fi
}

capture_compose_image_ids() {
    json_array_from_lines "$(compose images --quiet 2>/dev/null | sort -u || true)"
}

record_json_item() {
    local file=$1 value=$2
    jq --arg value "$value" '. + [$value]' "$file" >"$file.tmp"
    mv "$file.tmp" "$file"
}

write_artifact() {
    local exit_status=$1
    local status=FAIL
    if [[ "$exit_status" == 0 && "$test_status" == 0 && "$cleanup_status" == PASS ]]; then
        status=PASS
    fi
    jq -n \
        --arg schema "nereus-delay-oxia-multi-node-gateway-e2e-v1" \
        --arg status "$status" \
        --arg artifact_dir "$artifact_dir" \
        --arg started_at "$started_at" \
        --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --arg delay_branch "$delay_branch" --arg kafka_branch "nereus/delay-guarded-producer-v1" \
        --arg pulsar_branch "nereus/delay-resource-guard-v1" --arg oxia_branch "main" \
        --arg delay "$delay_sha" --arg kafka "$kafka_sha" --arg pulsar "$pulsar_sha" --arg oxia "$oxia_sha" \
        --arg compose_project "$compose_project" --arg compose_file "$compose_file" \
        --argjson coordinator_ports "[$coordinator_1_port, $coordinator_2_port, $coordinator_3_port]" \
        --argjson data_server_ports "[$data_server_1_port, $data_server_2_port, $data_server_3_port]" \
        --arg gateway_port "$gateway_port" --arg initial_leader "$initial_leader" \
        --arg successor_leader "$successor_leader" --arg stopped_service "$stopped_service" \
        --arg survivor_port "$survivor_port" --argjson cut_gate_reached "$cut_gate_reached" \
        --argjson test_exit_code "$test_status" --arg failover_log "$failover_log" \
        --arg cleanup_status "$cleanup_status" --arg cleanup_detail "$cleanup_detail" \
        --argjson images_before "$compose_image_ids_before" \
        --argjson images_after "$compose_image_ids_after" \
        --argjson removed_containers "$removed_container_ids" \
        --argjson removed_images "$removed_image_ids" \
        '{
          schema: $schema,
          status: $status,
          artifact_dir: $artifact_dir,
          started_at: $started_at,
          finished_at: $finished_at,
          source_refs: {delay: $delay_branch, kafka: $kafka_branch, pulsar: $pulsar_branch, oxia: $oxia_branch},
          source_locks: {delay: $delay, kafka: $kafka, pulsar: $pulsar, oxia: $oxia},
          runtime: {
            compose_project: $compose_project,
            compose_file: $compose_file,
            coordinator_ports: $coordinator_ports,
            data_server_ports: $data_server_ports,
            gateway_port: ($gateway_port | tonumber),
            oxia_image_ids_before_cleanup: $images_before,
            oxia_image_ids_after_cleanup: $images_after
          },
          failover: {
            initial_leader: $initial_leader,
            stopped_service: $stopped_service,
            survivor_port: ($survivor_port | tonumber? // $survivor_port),
            successor_leader: $successor_leader,
            cut_gate_reached: $cut_gate_reached,
            test_exit_code: $test_exit_code,
            log: $failover_log
          },
          docker_cleanup: {
            status: $cleanup_status,
            detail: $cleanup_detail,
            removed_containers: $removed_containers,
            removed_images: $removed_images,
            remaining_compose_image_ids: $images_after,
            policy: "Only this exact Compose project and its generated Oxia images are eligible for removal; no global Docker prune is performed."
          },
          boundaries: [
            "PASS is a real three-DataServer Oxia shard-leader-stop plus Gateway durable-outcome reread receipt.",
            "The test uses one Oxia namespace shard and one Gateway process; it does not certify Gateway HA, coordinator failover, storage-service failover, placement churn or disaster continuity.",
            "This artifact is source-locked runtime evidence and is not a PASS_CERTIFIED V1 release artifact by itself."
          ]
        }' >"$artifact"
}

cleanup() {
    local exit_status=$?
    set +e
    if [[ -n "$failover_pid" ]]; then
        kill "$failover_pid" >/dev/null 2>&1 || true
        wait "$failover_pid" >/dev/null 2>&1 || true
    fi
    compose_image_ids_before=$(capture_compose_image_ids)
    compose down --volumes --remove-orphans --rmi local >/dev/null 2>&1
    local compose_down_status=$?
    if [[ "$compose_down_status" != 0 ]]; then
        cleanup_status=FAIL
        cleanup_detail="exact Compose down returned $compose_down_status"
    fi
    local leftover_containers
    leftover_containers=$(docker ps -aq --filter "label=com.docker.compose.project=$compose_project" || true)
    while IFS= read -r container; do
        [[ -z "$container" ]] && continue
        docker rm --force "$container" >/dev/null 2>&1
        if [[ $? == 0 ]]; then
            record_json_item "$artifact_dir/.removed-containers.json" "$container"
        else
            cleanup_status=FAIL
            cleanup_detail="a related Compose container could not be removed: $container"
        fi
    done <<<"$leftover_containers"
    local leftover_images
    leftover_images=$(docker image ls -q --filter "label=com.docker.compose.project=$compose_project" | sort -u || true)
    while IFS= read -r image_id; do
        [[ -z "$image_id" ]] && continue
        docker image rm "$image_id" >/dev/null 2>&1
        if [[ $? == 0 ]]; then
            record_json_item "$artifact_dir/.removed-images.json" "$image_id"
        else
            cleanup_status=FAIL
            cleanup_detail="a related generated Oxia image could not be removed: $image_id"
        fi
    done <<<"$leftover_images"
    if [[ -s "$artifact_dir/.removed-containers.json" ]]; then
        removed_container_ids=$(cat "$artifact_dir/.removed-containers.json")
    fi
    if [[ -s "$artifact_dir/.removed-images.json" ]]; then
        removed_image_ids=$(cat "$artifact_dir/.removed-images.json")
    fi
    local remaining_containers remaining_networks remaining_volumes remaining_images
    remaining_containers=$(docker ps -aq --filter "label=com.docker.compose.project=$compose_project" || true)
    remaining_networks=$(docker network ls -q --filter "label=com.docker.compose.project=$compose_project" || true)
    remaining_volumes=$(docker volume ls -q --filter "label=com.docker.compose.project=$compose_project" || true)
    remaining_images=$(docker image ls -q --filter "label=com.docker.compose.project=$compose_project" | sort -u || true)
    if [[ -n "$remaining_containers$remaining_networks$remaining_volumes$remaining_images" ]]; then
        cleanup_status=FAIL
        cleanup_detail="exact Compose postcheck found remaining project resources"
    elif [[ "$compose_down_status" == 0 ]]; then
        cleanup_detail="exact Compose project/container/network/volume/generated-image cleanup passed; locked bases retained"
    fi
    compose_image_ids_after=$(json_array_from_lines "$remaining_images")
    rm -f "$artifact_dir/.removed-containers.json" "$artifact_dir/.removed-images.json"
    rm -rf "$tls_dir" "$failover_dir"
    write_artifact "$exit_status"
    set -e
    exit "$exit_status"
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
    initial_leader=$(wait_for_namespace_leader)
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
    cut_gate_reached=true

    compose stop "$stopped_service"
    successor=$(wait_for_namespace_leader "$initial_leader")
    successor_leader="$successor"
    echo "Oxia shard successor leader: $successor"
    touch "$failover_gate"

    test_status=0
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
compose_image_ids_before=$(capture_compose_image_ids)
bootstrap_cluster
run_failover_smoke

echo "Oxia multi-node Gateway failover E2E passed: session-bound Gateway reread the exact durable outcome after leader stop"
echo "Dockerized Oxia multi-node Gateway failover smoke passed for Oxia $oxia_sha"
