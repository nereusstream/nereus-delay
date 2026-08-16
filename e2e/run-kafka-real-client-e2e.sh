#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
delay_dir="$(cd "${script_dir}/.." && pwd)"
kafka_dir="${NEREUS_DELAY_KAFKA_CHECKOUT:-${delay_dir}/../../kafka-worktrees/nereus-delay-k1}"
gradle_user_home="${NEREUS_DELAY_KAFKA_GRADLE_USER_HOME:-/tmp/nereus-delay-kafka-e2e-gradle}"
compose_project="nereus-delay-kafka-e2e-$(date +%s)-$$"
compose_file="${script_dir}/docker-compose.kafka.yml"
compose=(docker compose -p "${compose_project}" -f "${compose_file}")
image="nereus-delay-kafka-k1:${compose_project}"
image_context="$(mktemp -d -t nereus-delay-k1-image.XXXXXX)"
base_port=$((19092 + ($$ % 500)))
broker_1_port="${KAFKA_BROKER_1_PORT:-${base_port}}"
broker_2_port="${KAFKA_BROKER_2_PORT:-$((base_port + 1))}"
broker_3_port="${KAFKA_BROKER_3_PORT:-$((base_port + 2))}"
cluster_id="${KAFKA_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
client_jar="${NEREUS_DELAY_KAFKA_CLIENT_JAR:-${kafka_dir}/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar}"
bootstrap_all="127.0.0.1:${broker_1_port},127.0.0.1:${broker_2_port},127.0.0.1:${broker_3_port}"
bootstrap_survivors="127.0.0.1:${broker_2_port},127.0.0.1:${broker_3_port}"
topic_1="${KAFKA_DELAY_E2E_TOPIC_1:-nereus-delay-k1-topic-1}"
source_topic="${KAFKA_DELAY_E2E_SOURCE_TOPIC:-nereus-delay-source-topic}"
mutation_topic="${KAFKA_DELAY_E2E_MUTATION_TOPIC:-nereus-delay-mutation-topic}"
mutation_worker_topic="${KAFKA_DELAY_E2E_MUTATION_WORKER_TOPIC:-nereus-delay-mutation-worker-topic}"
route_worker_topic="${KAFKA_DELAY_E2E_ROUTE_WORKER_TOPIC:-nereus-delay-route-worker-topic}"
worker_topic="${KAFKA_DELAY_E2E_WORKER_TOPIC:-nereus-delay-worker-topic}"
worker_destination_topic="${KAFKA_DELAY_E2E_WORKER_DESTINATION_TOPIC:-nereus-delay-worker-destination-topic}"
k2_target_topic="${KAFKA_DELAY_E2E_K2_TARGET_TOPIC:-nereus-delay-k2-target}"
k2_receipt_topic="${KAFKA_DELAY_E2E_K2_RECEIPT_TOPIC:-nereus-delay-k2-receipt}"
with_oxia="${NEREUS_DELAY_KAFKA_WITH_OXIA:-0}"
route_failover="${NEREUS_DELAY_KAFKA_ROUTE_FAILOVER:-0}"
route_failover_only="${NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY:-0}"
multi_shard_only="${NEREUS_DELAY_KAFKA_MULTI_SHARD_ONLY:-0}"
k2_failover="${NEREUS_DELAY_KAFKA_K2_FAILOVER:-0}"
k2_failover_only="${NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY:-0}"
k2_response_loss="${NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS:-0}"
k2_response_loss_only="${NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY:-0}"
worker_destination_response_loss="${NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS:-0}"
worker_destination_response_loss_only="${NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS_ONLY:-0}"
source_ack_response_loss="${NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS:-0}"
source_ack_response_loss_only="${NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS_ONLY:-0}"
fetch_response_loss_only="${NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY:-0}"
retention_floor_only="${NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY:-0}"
process_crash_only="${NEREUS_DELAY_KAFKA_PROCESS_CRASH_ONLY:-0}"
worker_process_crash_only="${NEREUS_DELAY_KAFKA_WORKER_PROCESS_CRASH_ONLY:-0}"
worker_ack_process_crash_only="${NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY:-0}"
broker_process_crash_only="${NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY:-0}"
broker_network_partition_only="${NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY:-0}"
broker_tcp_cut_only="${NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY:-0}"
if [[ "${broker_tcp_cut_only}" == "1" ]]; then
  broker_1_bind_port="${KAFKA_BROKER_1_BIND_PORT:-$((broker_1_port + 100))}"
else
  broker_1_bind_port="${KAFKA_BROKER_1_BIND_PORT:-${broker_1_port}}"
fi
oxia_checkout="${NEREUS_DELAY_KAFKA_OXIA_CHECKOUT:-${delay_dir}/../../oxia}"
oxia_port="${NEREUS_DELAY_KAFKA_OXIA_PORT:-$((16650 + ($$ % 100)))}"
oxia_compose_project="nereus-delay-kafka-oxia-e2e-$(date +%s)-$$"
oxia_compose_file="${script_dir}/docker-compose.oxia.yml"
oxia_compose=(docker compose -p "${oxia_compose_project}" -f "${oxia_compose_file}")
oxia_endpoint="127.0.0.1:${oxia_port}"

if [[ "${with_oxia}" != "0" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WITH_OXIA must be 0 or 1" >&2
  exit 1
fi
if [[ "${route_failover}" != "0" && "${route_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER must be 0 or 1" >&2
  exit 1
fi
if [[ "${route_failover_only}" != "0" && "${route_failover_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${multi_shard_only}" != "0" && "${multi_shard_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_MULTI_SHARD_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${k2_failover}" != "0" && "${k2_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_FAILOVER must be 0 or 1" >&2
  exit 1
fi
if [[ "${k2_failover_only}" != "0" && "${k2_failover_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${k2_response_loss}" != "0" && "${k2_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS must be 0 or 1" >&2
  exit 1
fi
if [[ "${k2_response_loss_only}" != "0" && "${k2_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss}" != "0" && "${worker_destination_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_only}" != "0" && "${worker_destination_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${source_ack_response_loss}" != "0" && "${source_ack_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS must be 0 or 1" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" != "0" && "${source_ack_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${fetch_response_loss_only}" != "0" && "${fetch_response_loss_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${retention_floor_only}" != "0" && "${retention_floor_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${process_crash_only}" != "0" && "${process_crash_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_PROCESS_CRASH_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" != "0" && "${worker_process_crash_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WORKER_PROCESS_CRASH_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${worker_ack_process_crash_only}" != "0" && "${worker_ack_process_crash_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${broker_process_crash_only}" != "0" && "${broker_process_crash_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${broker_network_partition_only}" != "0" && "${broker_network_partition_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY must be 0 or 1" >&2
  exit 1
fi
if [[ "${broker_tcp_cut_only}" != "0" && "${broker_tcp_cut_only}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY must be 0 or 1" >&2
  exit 1
fi
if (( broker_1_bind_port <= 0 || broker_1_bind_port > 65535 )); then
  echo "KAFKA_BROKER_1_BIND_PORT must be 1..65535" >&2
  exit 1
fi
if [[ "${broker_process_crash_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WORKER_PROCESS_CRASH_ONLY requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${worker_ack_process_crash_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${broker_network_partition_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${route_failover}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${route_failover_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${route_failover_only}" == "1" && "${route_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY requires NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1" >&2
  exit 1
fi
if [[ "${multi_shard_only}" == "1" && "${with_oxia}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_MULTI_SHARD_ONLY requires NEREUS_DELAY_KAFKA_WITH_OXIA=1" >&2
  exit 1
fi
if [[ "${multi_shard_only}" == "1" && ("${route_failover}" == "1" || "${route_failover_only}" == "1") ]]; then
  echo "NEREUS_DELAY_KAFKA_MULTI_SHARD_ONLY cannot be combined with accepted-route failover mode" >&2
  exit 1
fi
if [[ "${k2_failover_only}" == "1" && "${k2_failover}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY requires NEREUS_DELAY_KAFKA_K2_FAILOVER=1" >&2
  exit 1
fi
if [[ "${k2_response_loss_only}" == "1" && "${k2_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY requires NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_only}" == "1" && "${worker_destination_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS_ONLY requires NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" == "1" && "${source_ack_response_loss}" != "1" ]]; then
  echo "NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS_ONLY requires NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS=1" >&2
  exit 1
fi
if [[ "${k2_response_loss}" == "1" && "${k2_failover}" == "1" ]]; then
  echo "K2 response-loss and broker-failover modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${route_failover_only}" == "1" && "${k2_failover_only}" == "1" ]]; then
  echo "route and K2 failover-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${route_failover_only}" == "1" && "${k2_response_loss_only}" == "1" ]]; then
  echo "route and K2 response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${k2_failover_only}" == "1" && "${k2_response_loss_only}" == "1" ]]; then
  echo "K2 failover and response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_only}" == "1" && "${route_failover_only}" == "1" ]]; then
  echo "route and Kafka Worker destination response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_only}" == "1" && "${k2_failover_only}" == "1" ]]; then
  echo "K2 failover and Kafka Worker destination response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${worker_destination_response_loss_only}" == "1" && "${k2_response_loss_only}" == "1" ]]; then
  echo "K2 response-loss and Kafka Worker destination response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" == "1" && "${route_failover_only}" == "1" ]]; then
  echo "route and Kafka Worker source ACK response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" == "1" && "${k2_failover_only}" == "1" ]]; then
  echo "K2 failover and Kafka Worker source ACK response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" == "1" && "${k2_response_loss_only}" == "1" ]]; then
  echo "K2 response-loss and Kafka Worker source ACK response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${source_ack_response_loss_only}" == "1" && "${worker_destination_response_loss_only}" == "1" ]]; then
  echo "Kafka Worker destination and source ACK response-loss-only modes are mutually exclusive" >&2
  exit 1
fi
if [[ "${fetch_response_loss_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1"
        || "${retention_floor_only}" == "1") ]]; then
  echo "Kafka Fetch response-loss-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi
if [[ "${retention_floor_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1") ]]; then
  echo "Kafka retention-floor-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi
if [[ "${process_crash_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1"
        || "${fetch_response_loss_only}" == "1"
        || "${retention_floor_only}" == "1") ]]; then
  echo "Kafka process-crash-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi
if [[ "${broker_process_crash_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1"
        || "${fetch_response_loss_only}" == "1"
        || "${retention_floor_only}" == "1" || "${process_crash_only}" == "1"
        || "${route_failover}" == "1" || "${k2_failover}" == "1"
        || "${k2_response_loss}" == "1" || "${worker_destination_response_loss}" == "1"
        || "${source_ack_response_loss}" == "1") ]]; then
  echo "Kafka Broker process-crash-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi
if [[ "${worker_process_crash_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1"
        || "${fetch_response_loss_only}" == "1"
        || "${retention_floor_only}" == "1" || "${process_crash_only}" == "1"
        || "${broker_process_crash_only}" == "1" || "${broker_network_partition_only}" == "1"
        || "${route_failover}" == "1" || "${k2_failover}" == "1"
        || "${k2_response_loss}" == "1" || "${worker_destination_response_loss}" == "1"
        || "${source_ack_response_loss}" == "1") ]]; then
  echo "Kafka Worker process-crash-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi
if [[ "${worker_ack_process_crash_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1"
        || "${fetch_response_loss_only}" == "1"
        || "${retention_floor_only}" == "1" || "${process_crash_only}" == "1"
        || "${worker_process_crash_only}" == "1" || "${broker_process_crash_only}" == "1"
        || "${broker_network_partition_only}" == "1"
        || "${route_failover}" == "1" || "${k2_failover}" == "1"
        || "${k2_response_loss}" == "1" || "${worker_destination_response_loss}" == "1"
        || "${source_ack_response_loss}" == "1") ]]; then
  echo "Kafka Worker ACK process-crash-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi
if [[ "${broker_network_partition_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1"
        || "${fetch_response_loss_only}" == "1"
        || "${retention_floor_only}" == "1" || "${process_crash_only}" == "1"
        || "${broker_process_crash_only}" == "1"
        || "${route_failover}" == "1" || "${k2_failover}" == "1"
        || "${k2_response_loss}" == "1" || "${worker_destination_response_loss}" == "1"
        || "${source_ack_response_loss}" == "1") ]]; then
  echo "Kafka Broker network-partition-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi
if [[ "${broker_tcp_cut_only}" == "1" && ("${route_failover_only}" == "1"
        || "${k2_failover_only}" == "1" || "${k2_response_loss_only}" == "1"
        || "${worker_destination_response_loss_only}" == "1"
        || "${source_ack_response_loss_only}" == "1"
        || "${fetch_response_loss_only}" == "1"
        || "${retention_floor_only}" == "1" || "${process_crash_only}" == "1"
        || "${worker_process_crash_only}" == "1" || "${worker_ack_process_crash_only}" == "1"
        || "${broker_process_crash_only}" == "1" || "${broker_network_partition_only}" == "1"
        || "${route_failover}" == "1" || "${k2_failover}" == "1"
        || "${k2_response_loss}" == "1" || "${worker_destination_response_loss}" == "1"
        || "${source_ack_response_loss}" == "1") ]]; then
  echo "Kafka Broker raw TCP cut-only mode is mutually exclusive with other focused modes" >&2
  exit 1
fi

if [[ "${worker_destination_response_loss}" == "1" ]]; then
  export NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS=1
fi
if [[ "${source_ack_response_loss}" == "1" ]]; then
  export NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS=1
fi

route_failover_dir="$(mktemp -d -t nereus-delay-kafka-route-failover.XXXXXX)"
route_failover_gate="${route_failover_dir}/release"
route_failover_log="${route_failover_dir}/route-worker.log"
k2_failover_dir="$(mktemp -d -t nereus-delay-kafka-k2-failover.XXXXXX)"
k2_failover_gate="${k2_failover_dir}/release"
k2_failover_ready="${k2_failover_dir}/ready"
k2_failover_log="${k2_failover_dir}/k2.log"
k2_failover_pid=""
process_crash_dir="$(mktemp -d -t nereus-delay-kafka-process-crash.XXXXXX)"
process_crash_log="${process_crash_dir}/crash.log"
worker_process_crash_dir="$(mktemp -d -t nereus-delay-kafka-worker-process-crash.XXXXXX)"
worker_process_crash_log="${worker_process_crash_dir}/crash.log"
worker_process_crash_resume_log="${worker_process_crash_dir}/resume.log"
worker_process_crash_gate="${worker_process_crash_dir}/release"
worker_process_crash_pid_file="${worker_process_crash_dir}/worker.pid"
worker_process_crash_launcher_pid=""
worker_ack_process_crash_dir="$(mktemp -d -t nereus-delay-kafka-worker-ack-process-crash.XXXXXX)"
worker_ack_process_crash_log="${worker_ack_process_crash_dir}/crash.log"
worker_ack_process_crash_resume_log="${worker_ack_process_crash_dir}/resume.log"
worker_ack_process_crash_gate="${worker_ack_process_crash_dir}/release"
worker_ack_process_crash_pid_file="${worker_ack_process_crash_dir}/worker.pid"
worker_ack_process_crash_launcher_pid=""
broker_tcp_cut_dir="$(mktemp -d -t nereus-delay-kafka-broker-tcp-cut.XXXXXX)"
broker_tcp_cut_log="${broker_tcp_cut_dir}/proxy.log"
broker_tcp_cut_file="${broker_tcp_cut_dir}/cut"
broker_tcp_release_file="${broker_tcp_cut_dir}/release"
broker_tcp_stop_file="${broker_tcp_cut_dir}/stop"
broker_tcp_ready_file="${broker_tcp_cut_dir}/ready"
broker_tcp_ack_file="${broker_tcp_cut_dir}/cut-ack"
broker_tcp_pre_cut_file="${broker_tcp_cut_dir}/pre-cut-forward"
broker_tcp_post_cut_file="${broker_tcp_cut_dir}/post-cut-rejection"
broker_tcp_post_cut_handoff_file="${broker_tcp_cut_dir}/post-cut-handoff"
broker_tcp_proxy_pid=""

cleanup() {
  if [[ -n "${worker_process_crash_launcher_pid}" ]]; then
    kill "${worker_process_crash_launcher_pid}" >/dev/null 2>&1 || true
    wait "${worker_process_crash_launcher_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${worker_ack_process_crash_launcher_pid}" ]]; then
    kill "${worker_ack_process_crash_launcher_pid}" >/dev/null 2>&1 || true
    wait "${worker_ack_process_crash_launcher_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${broker_tcp_proxy_pid}" ]]; then
    touch "${broker_tcp_stop_file}" >/dev/null 2>&1 || true
    wait "${broker_tcp_proxy_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${k2_failover_pid}" ]]; then
    kill "${k2_failover_pid}" >/dev/null 2>&1 || true
    wait "${k2_failover_pid}" >/dev/null 2>&1 || true
  fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  docker image rm "${image}" >/dev/null 2>&1 || true
  if [[ "${with_oxia}" == "1" ]]; then
    "${oxia_compose[@]}" down --volumes --remove-orphans --rmi local >/dev/null 2>&1 || true
  fi
  rm -rf "${image_context}"
  rm -rf "${route_failover_dir}"
  rm -rf "${k2_failover_dir}"
  rm -rf "${process_crash_dir}"
  rm -rf "${worker_process_crash_dir}"
  rm -rf "${worker_ack_process_crash_dir}"
  rm -rf "${broker_tcp_cut_dir}"
}
trap cleanup EXIT INT TERM

require_clean_kafka_checkout() {
  test -z "$(git -C "${kafka_dir}" status --porcelain)"
}

wait_for_broker() {
  local service="$1"
  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if "${compose[@]}" exec -T "${service}" bash -c \
        "echo > /dev/tcp/${service}/19092" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "Kafka did not become ready: ${bootstrap}" >&2
  "${compose[@]}" ps >&2 || true
  "${compose[@]}" logs >&2 || true
  return 1
}

start_oxia() {
  test -d "${oxia_checkout}"
  test -s "${oxia_checkout}/Dockerfile"
  export NEREUS_DELAY_OXIA_CHECKOUT="${oxia_checkout}"
  export NEREUS_DELAY_OXIA_E2E_PORT="${oxia_port}"
  echo "Oxia checkout: $(git -C "${oxia_checkout}" rev-parse HEAD)"
  echo "Oxia Compose project: ${oxia_compose_project}"
  echo "Oxia endpoint: ${oxia_endpoint}"
  "${oxia_compose[@]}" up --build --detach
  local ready=0
  for attempt in $(seq 1 60); do
    if "${oxia_compose[@]}" exec --no-TTY oxia oxia health --host 127.0.0.1 --port 6648 --timeout 2s \
        >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != "1" ]]; then
    "${oxia_compose[@]}" logs oxia >&2 || true
    echo "Oxia did not become healthy" >&2
    return 1
  fi
}

run_worker_smoke() {
  local bootstrap_server="$1"
  local worker_topic_base="$2"
  local worker_mode="${3:-run}"
  local destination_topic="${4-${worker_destination_topic}}"
  local worker_command=(./gradlew runRealKafkaWorkerSmoke
    "-PkafkaClientJar=${client_jar}"
    "-PkafkaBootstrap=${bootstrap_server}"
    "-PkafkaWorkerTopic=${worker_topic_base}"
    "-PkafkaWorkerMode=${worker_mode}")
  if [[ -n "${destination_topic}" ]]; then
    worker_command+=("-PkafkaWorkerDestinationTopic=${destination_topic}")
  fi
  if [[ "${with_oxia}" == "1" ]]; then
    NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    GRADLE_USER_HOME="${gradle_user_home}" "${worker_command[@]}" \
      --no-daemon --console=plain
  else
    GRADLE_USER_HOME="${gradle_user_home}" "${worker_command[@]}" \
      --no-daemon --console=plain
  fi
}

start_broker_tcp_fault_proxy() {
  rm -f "${broker_tcp_ready_file}" "${broker_tcp_ack_file}" "${broker_tcp_pre_cut_file}" \
    "${broker_tcp_post_cut_file}" "${broker_tcp_post_cut_handoff_file}" "${broker_tcp_stop_file}"
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaTcpFaultProxy \
    "-PkafkaClientJar=${client_jar}" \
    "-PkafkaProxyListenPort=${broker_1_port}" \
    "-PkafkaProxyTargetHost=127.0.0.1" \
    "-PkafkaProxyTargetPort=${broker_1_bind_port}" \
    "-PkafkaProxyPostCutTargetHost=127.0.0.1" \
    "-PkafkaProxyPostCutTargetPort=${broker_2_port}" \
    "-PkafkaProxyCutFile=${broker_tcp_cut_file}" \
    "-PkafkaProxyReleaseFile=${broker_tcp_release_file}" \
    "-PkafkaProxyStopFile=${broker_tcp_stop_file}" \
    "-PkafkaProxyReadyFile=${broker_tcp_ready_file}" \
    "-PkafkaProxyCutAckFile=${broker_tcp_ack_file}" \
    "-PkafkaProxyPreCutFile=${broker_tcp_pre_cut_file}" \
    "-PkafkaProxyPostCutFile=${broker_tcp_post_cut_file}" \
    "-PkafkaProxyPostCutHandoffFile=${broker_tcp_post_cut_handoff_file}" \
    --no-daemon --console=plain >"${broker_tcp_cut_log}" 2>&1 &
  broker_tcp_proxy_pid=$!
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if [[ -f "${broker_tcp_ready_file}" ]]; then
      return 0
    fi
    if ! kill -0 "${broker_tcp_proxy_pid}" >/dev/null 2>&1; then
      cat "${broker_tcp_cut_log}" >&2
      echo "Kafka raw TCP fault proxy exited before readiness" >&2
      return 1
    fi
    sleep 1
  done
  cat "${broker_tcp_cut_log}" >&2
  echo "Kafka raw TCP fault proxy did not become ready" >&2
  return 1
}

run_mutation_worker_smoke() {
  local bootstrap_server="$1"
  if [[ "${with_oxia}" == "1" ]]; then
    NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaMutationWorkerSmoke \
      -PkafkaClientJar="${client_jar}" \
      -PkafkaBootstrap="${bootstrap_server}" \
      -PkafkaMutationWorkerTopic="${mutation_worker_topic}" \
      --no-daemon --console=plain
  else
    GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaMutationWorkerSmoke \
      -PkafkaClientJar="${client_jar}" \
      -PkafkaBootstrap="${bootstrap_server}" \
      -PkafkaMutationWorkerTopic="${mutation_worker_topic}" \
      --no-daemon --console=plain
  fi
}

run_route_worker_smoke() {
  local bootstrap_server="$1"
  if [[ "${with_oxia}" != "1" ]]; then
    return 0
  fi
  local route_command=(./gradlew runRealKafkaRouteWorkerSmoke
    "-PkafkaClientJar=${client_jar}"
    "-PkafkaBootstrap=${bootstrap_server}"
    "-PkafkaRouteWorkerTopic=${route_worker_topic}"
    --no-daemon --console=plain)
  if [[ "${route_failover}" != "1" ]]; then
    NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
    NEREUS_DELAY_OXIA_NAMESPACE=default \
    GRADLE_USER_HOME="${gradle_user_home}" "${route_command[@]}"
    return 0
  fi

  NEREUS_DELAY_OXIA_ENDPOINT="${oxia_endpoint}" \
  NEREUS_DELAY_OXIA_NAMESPACE=default \
  GRADLE_USER_HOME="${gradle_user_home}" \
  NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_GATE="${route_failover_gate}" \
    "${route_command[@]}" >"${route_failover_log}" 2>&1 &
  local route_pid=$!
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if rg -F --quiet "Kafka Route Worker ready for accepted-route failover" "${route_failover_log}"; then
      break
    fi
    if ! kill -0 "${route_pid}" >/dev/null 2>&1; then
      wait "${route_pid}" || true
      cat "${route_failover_log}" >&2
      return 1
    fi
    sleep 1
  done
  if ! rg -F --quiet "Kafka Route Worker ready for accepted-route failover" "${route_failover_log}"; then
    echo "Kafka Route Worker did not reach the accepted-route failover gate" >&2
    cat "${route_failover_log}" >&2
    return 1
  fi
  "${compose[@]}" stop kafka-1
  wait_for_broker kafka-2
  touch "${route_failover_gate}"
  wait "${route_pid}"
  "${compose[@]}" start kafka-1
  wait_for_broker kafka-1
  sleep 10
  cat "${route_failover_log}"
}

run_k2_smoke() {
  local bootstrap_server="$1"
  local k2_command=(./gradlew runRealKafkaK2Smoke
    "-PkafkaClientJar=${client_jar}"
    "-PkafkaBootstrap=${bootstrap_server}"
    "-PkafkaTargetTopic=${k2_target_topic}"
    "-PkafkaReceiptTopic=${k2_receipt_topic}"
    --no-daemon --console=plain)
  if [[ "${k2_response_loss}" == "1" ]]; then
    NEREUS_DELAY_KAFKA_K2_COMMITTED_RESPONSE_LOSS=1 \
    GRADLE_USER_HOME="${gradle_user_home}" "${k2_command[@]}"
    return 0
  fi
  if [[ "${k2_failover}" != "1" ]]; then
    GRADLE_USER_HOME="${gradle_user_home}" "${k2_command[@]}"
    return 0
  fi

  rm -f "${k2_failover_gate}" "${k2_failover_ready}" "${k2_failover_log}"
  NEREUS_DELAY_KAFKA_K2_COMMIT_GATE="${k2_failover_gate}" \
  NEREUS_DELAY_KAFKA_K2_COMMIT_READY="${k2_failover_ready}" \
  GRADLE_USER_HOME="${gradle_user_home}" "${k2_command[@]}" >"${k2_failover_log}" 2>&1 &
  k2_failover_pid=$!
  local deadline=$((SECONDS + 180))
  while (( SECONDS < deadline )); do
    if [[ -f "${k2_failover_ready}" ]]; then
      break
    fi
    if ! kill -0 "${k2_failover_pid}" >/dev/null 2>&1; then
      if wait "${k2_failover_pid}"; then
        :
      else
        cat "${k2_failover_log}" >&2
        k2_failover_pid=""
        return 1
      fi
      cat "${k2_failover_log}" >&2
      k2_failover_pid=""
      return 1
    fi
    sleep 1
  done
  if [[ ! -f "${k2_failover_ready}" ]]; then
    echo "Kafka K2 smoke did not reach the broker failover gate" >&2
    cat "${k2_failover_log}" >&2
    return 1
  fi

  "${compose[@]}" stop kafka-1
  wait_for_broker kafka-2
  touch "${k2_failover_gate}"
  local k2_status=0
  if wait "${k2_failover_pid}"; then
    :
  else
    k2_status=$?
  fi
  k2_failover_pid=""
  "${compose[@]}" start kafka-1
  wait_for_broker kafka-1
  sleep 10
  cat "${k2_failover_log}"
  return "${k2_status}"
}

cd "${delay_dir}"
require_clean_kafka_checkout
test -s "${client_jar}"
test -s "${kafka_dir}/core/build/libs/kafka_2.13-4.4.0-SNAPSHOT.jar"
test -s "${kafka_dir}/core/build/dependant-libs-2.13.18/kafka-server-4.4.0-SNAPSHOT.jar"
test -x "${delay_dir}/gradlew"

mkdir -p "${image_context}/core" "${image_context}/clients"
cp -R "${kafka_dir}/bin" "${image_context}/bin"
cp -R "${kafka_dir}/config" "${image_context}/config"
cp -R "${kafka_dir}/core/build" "${image_context}/core/build"
cp -R "${kafka_dir}/clients/build" "${image_context}/clients/build"
cp "${script_dir}/Dockerfile.kafka-k1" "${image_context}/Dockerfile"
cp "${script_dir}/kafka-k1-entrypoint.sh" "${image_context}/kafka-k1-entrypoint.sh"
docker build --pull=false -t "${image}" "${image_context}"
image_digest="$(docker image inspect "${image}" --format '{{.Id}}')"

export KAFKA_CLUSTER_ID="${cluster_id}"
export KAFKA_K1_IMAGE="${image}"
export KAFKA_BROKER_1_PORT="${broker_1_port}"
export KAFKA_BROKER_1_BIND_PORT="${broker_1_bind_port}"
export KAFKA_BROKER_2_PORT="${broker_2_port}"
export KAFKA_BROKER_3_PORT="${broker_3_port}"
if [[ "${retention_floor_only}" == "1" ]]; then
  export KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=1000
fi

echo "K1 checkout: $(git -C "${kafka_dir}" rev-parse HEAD)"
echo "K1 client jar: ${client_jar}"
echo "K1 client SHA256: $(shasum -a 256 "${client_jar}" | awk '{print $1}')"
echo "K1 broker image ID: ${image_digest}"
echo "Compose project: ${compose_project}"
echo "Broker ports: ${broker_1_port},${broker_2_port},${broker_3_port}"

"${compose[@]}" up -d
if [[ "${broker_tcp_cut_only}" == "1" ]]; then
  start_broker_tcp_fault_proxy
fi
wait_for_broker kafka-1

if [[ "${route_failover_only}" == "1" ]]; then
  start_oxia
  run_route_worker_smoke "${bootstrap_all}"
  echo "Kafka accepted-route broker failover E2E passed: Route-bound Worker applied and ACKed after broker-1 failover, then released its final checkpoint and Oxia assignment."
  exit 0
fi

if [[ "${multi_shard_only}" == "1" ]]; then
  start_oxia
  export NEREUS_DELAY_KAFKA_ROUTE_WORKER_SHARDS=2
  run_route_worker_smoke "${bootstrap_all}"
  echo "Kafka native multi-shard Worker fleet E2E passed: one signed Route covered two guarded Fetch barriers, two real Oxia Assignment/Owner Lease CAS paths admitted two native source consumers, one fair fleet applied/ACKed both partitions, and both final checkpoints/assignments were released."
  exit 0
fi

if [[ "${k2_failover_only}" == "1" ]]; then
  run_k2_smoke "${bootstrap_all}"
  echo "Kafka K2 broker failover E2E passed: target-plus-receipt transaction crossed broker-1 failover with read_committed resolution."
  exit 0
fi

if [[ "${k2_response_loss_only}" == "1" ]]; then
  run_k2_smoke "${bootstrap_all}"
  echo "Kafka K2 committed response-loss E2E passed: real EndTxn commit was followed by local response loss and exact read_committed typed receipt resolution."
  exit 0
fi

if [[ "${worker_destination_response_loss_only}" == "1" ]]; then
  if [[ "${with_oxia}" == "1" ]]; then
    start_oxia
  fi
  response_loss_worker_topic="${KAFKA_DELAY_WORKER_RESPONSE_LOSS_TOPIC:-${worker_topic}-destination-response-loss}"
  run_worker_smoke "${bootstrap_all}" "${response_loss_worker_topic}"
  echo "Kafka Worker destination response-loss E2E passed: real EndTxn response loss resolved through typed read_committed KAFKA_TRANSACTIONAL_RECEIPT evidence and the source-applied Outcome completed."
  exit 0
fi

if [[ "${source_ack_response_loss_only}" == "1" ]]; then
  if [[ "${with_oxia}" == "1" ]]; then
    start_oxia
  fi
  response_loss_source_topic="${KAFKA_DELAY_SOURCE_ACK_RESPONSE_LOSS_TOPIC:-${worker_topic}-source-ack-response-loss}"
  run_worker_smoke "${bootstrap_all}" "${response_loss_source_topic}" run ""
  echo "Kafka Worker source ACK response-loss E2E passed: real commitSync ACK response loss was retried on the same source record and the bounded Worker vertical completed."
  exit 0
fi

if [[ "${fetch_response_loss_only}" == "1" ]]; then
  fetch_response_loss_topic="${KAFKA_DELAY_FETCH_RESPONSE_LOSS_TOPIC:-${source_topic}-fetch-response-loss}"
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaFetchResponseLossSmoke \
    -PkafkaClientJar="${client_jar}" \
    -PkafkaBootstrap="${bootstrap_all}" \
    -PkafkaFetchResponseLossTopic="${fetch_response_loss_topic}" \
    --no-daemon --console=plain
  echo "Kafka source Fetch response-loss E2E passed: real read_committed Fetch v13 response was discarded before ACK, exact source replay and LSO coverage were recovered."
  exit 0
fi

if [[ "${retention_floor_only}" == "1" ]]; then
  retention_floor_topic="${KAFKA_DELAY_RETENTION_FLOOR_TOPIC:-${source_topic}-retention-floor}"
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaRetentionFloorSmoke \
    -PkafkaClientJar="${client_jar}" \
    -PkafkaBootstrap="${bootstrap_all}" \
    -PkafkaRetentionFloorTopic="${retention_floor_topic}" \
    --no-daemon --console=plain
  echo "Kafka source retention-floor E2E passed: real Broker retention advanced the earliest offset, stale source offset was rejected, and the current floor remained readable through guarded Fetch v13 with LSO."
  exit 0
fi

if [[ "${process_crash_only}" == "1" ]]; then
  process_crash_topic="${KAFKA_DELAY_PROCESS_CRASH_TOPIC:-${source_topic}-process-crash}"
  set +e
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaProcessCrashRecoverySmoke \
    -PkafkaClientJar="${client_jar}" \
    -PkafkaBootstrap="${bootstrap_all}" \
    -PkafkaProcessCrashTopic="${process_crash_topic}" \
    -PkafkaProcessCrashMode=crash \
    --no-daemon --console=plain >"${process_crash_log}" 2>&1
  crash_status=$?
  set -e
  if [[ "${crash_status}" == "0" ]]; then
    cat "${process_crash_log}" >&2
    echo "Kafka process-crash cut unexpectedly returned success" >&2
    exit 1
  fi
  rg -F --quiet "exit value 86" "${process_crash_log}" \
    || { cat "${process_crash_log}" >&2; echo "Kafka process-crash cut did not halt with exit 86" >&2; exit 1; }
  rg -F --quiet "Kafka source process-crash cut reached" "${process_crash_log}" \
    || { cat "${process_crash_log}" >&2; exit 1; }
  rg -F "Kafka source process-crash cut reached" "${process_crash_log}"
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaProcessCrashRecoverySmoke \
    -PkafkaClientJar="${client_jar}" \
    -PkafkaBootstrap="${bootstrap_all}" \
    -PkafkaProcessCrashTopic="${process_crash_topic}" \
    -PkafkaProcessCrashMode=resume \
    --no-daemon --console=plain
  echo "Kafka source process-crash recovery E2E passed: the crashed JVM fetched exact guarded records without ACK, and a fresh same-group process replayed offsets 0 and 1 before committing offset 2."
  exit 0
fi

if [[ "${broker_process_crash_only}" == "1" ]]; then
  start_oxia
  broker_crash_topic="${KAFKA_DELAY_BROKER_PROCESS_CRASH_TOPIC:-${worker_topic}-broker-process-crash}"
  run_worker_smoke "${bootstrap_all}" "${broker_crash_topic}" prepare
  "${compose[@]}" kill --signal KILL kafka-1
  wait_for_broker kafka-2
  run_worker_smoke "${bootstrap_survivors}" "${broker_crash_topic}" resume
  "${compose[@]}" start kafka-1
  wait_for_broker kafka-1
  echo "Kafka Broker process-crash recovery E2E passed: kafka-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia authority, and kafka-1 rejoined afterward."
  exit 0
fi

if [[ "${worker_process_crash_only}" == "1" ]]; then
  start_oxia
  worker_process_crash_topic="${KAFKA_DELAY_WORKER_PROCESS_CRASH_TOPIC:-${worker_topic}-worker-process-crash}"
  export NEREUS_DELAY_KAFKA_WORKER_ROOT="${worker_process_crash_dir}/state"
  export NEREUS_DELAY_KAFKA_WORKER_CRASH_GATE="${worker_process_crash_gate}"
  export NEREUS_DELAY_KAFKA_WORKER_CRASH_PID_FILE="${worker_process_crash_pid_file}"
  rm -f "${worker_process_crash_gate}" "${worker_process_crash_pid_file}"
  set +e
  run_worker_smoke "${bootstrap_all}" "${worker_process_crash_topic}" crash-wait "" \
    >"${worker_process_crash_log}" 2>&1 &
  worker_process_crash_launcher_pid=$!
  set -e
  crash_gate_deadline=$((SECONDS + 180))
  while (( SECONDS < crash_gate_deadline )); do
    if [[ -f "${worker_process_crash_gate}" && -s "${worker_process_crash_pid_file}" ]]; then
      break
    fi
    if ! kill -0 "${worker_process_crash_launcher_pid}" >/dev/null 2>&1; then
      wait "${worker_process_crash_launcher_pid}" || true
      cat "${worker_process_crash_log}" >&2
      echo "Kafka Worker process-crash JVM exited before its cut gate" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ ! -f "${worker_process_crash_gate}" || ! -s "${worker_process_crash_pid_file}" ]]; then
    cat "${worker_process_crash_log}" >&2
    echo "Kafka Worker process-crash JVM did not reach its cut gate" >&2
    exit 1
  fi
  worker_process_pid="$(<"${worker_process_crash_pid_file}")"
  if ! kill -0 "${worker_process_pid}" >/dev/null 2>&1; then
    cat "${worker_process_crash_log}" >&2
    echo "Kafka Worker process-crash JVM PID is not alive at the cut gate" >&2
    exit 1
  fi
  cat "${worker_process_crash_log}"
  kill -KILL "${worker_process_pid}"
  rm -f "${worker_process_crash_gate}"
  set +e
  wait "${worker_process_crash_launcher_pid}"
  worker_process_crash_status=$?
  set -e
  worker_process_crash_launcher_pid=""
  if [[ "${worker_process_crash_status}" == "0" ]]; then
    echo "Kafka Worker process-crash JVM unexpectedly returned success after SIGKILL" >&2
    exit 1
  fi
  for attempt in $(seq 1 90); do
    set +e
    run_worker_smoke "${bootstrap_all}" "${worker_process_crash_topic}" resume "" \
      >"${worker_process_crash_resume_log}" 2>&1
    resume_status=$?
    set -e
    if [[ "${resume_status}" == "0" ]]; then
      cat "${worker_process_crash_resume_log}"
      break
    fi
    if [[ "${attempt}" == "90" ]]; then
      cat "${worker_process_crash_resume_log}" >&2
      echo "Kafka Worker process-crash recovery did not reacquire the real Oxia lease" >&2
      exit 1
    fi
    sleep 1
  done
  rg -F --quiet "Kafka Worker vertical smoke passed" "${worker_process_crash_resume_log}" \
    || { cat "${worker_process_crash_resume_log}" >&2; exit 1; }
  echo "Kafka Worker process-crash recovery E2E passed: a real Worker JVM was SIGKILLed after opening the guarded source/runtime with the next record unACKed, and a fresh JVM reopened the exact local Store, reacquired the real Oxia lease, replayed and ACKed the source record, and published the final checkpoint."
  exit 0
fi

if [[ "${worker_ack_process_crash_only}" == "1" ]]; then
  start_oxia
  worker_ack_process_crash_topic="${KAFKA_DELAY_WORKER_ACK_PROCESS_CRASH_TOPIC:-${worker_topic}-worker-ack-process-crash}"
  export NEREUS_DELAY_KAFKA_WORKER_ROOT="${worker_ack_process_crash_dir}/state"
  export NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_GATE="${worker_ack_process_crash_gate}"
  export NEREUS_DELAY_KAFKA_WORKER_ACK_CRASH_PID_FILE="${worker_ack_process_crash_pid_file}"
  rm -f "${worker_ack_process_crash_gate}" "${worker_ack_process_crash_pid_file}"
  set +e
  run_worker_smoke "${bootstrap_all}" "${worker_ack_process_crash_topic}" ack-crash-wait "" \
    >"${worker_ack_process_crash_log}" 2>&1 &
  worker_ack_process_crash_launcher_pid=$!
  set -e
  ack_crash_gate_deadline=$((SECONDS + 180))
  while (( SECONDS < ack_crash_gate_deadline )); do
    if [[ -f "${worker_ack_process_crash_gate}" && -s "${worker_ack_process_crash_pid_file}" ]]; then
      break
    fi
    if ! kill -0 "${worker_ack_process_crash_launcher_pid}" >/dev/null 2>&1; then
      wait "${worker_ack_process_crash_launcher_pid}" || true
      cat "${worker_ack_process_crash_log}" >&2
      echo "Kafka Worker ACK process-crash JVM exited before its cut gate" >&2
      exit 1
    fi
    sleep 1
  done
  if [[ ! -f "${worker_ack_process_crash_gate}" || ! -s "${worker_ack_process_crash_pid_file}" ]]; then
    cat "${worker_ack_process_crash_log}" >&2
    echo "Kafka Worker ACK process-crash JVM did not reach its cut gate" >&2
    exit 1
  fi
  worker_ack_process_pid="$(<"${worker_ack_process_crash_pid_file}")"
  if ! kill -0 "${worker_ack_process_pid}" >/dev/null 2>&1; then
    cat "${worker_ack_process_crash_log}" >&2
    echo "Kafka Worker ACK process-crash JVM PID is not alive at the cut gate" >&2
    exit 1
  fi
  rg -F "Kafka Worker ACK process-crash cut reached" "${worker_ack_process_crash_log}"
  kill -KILL "${worker_ack_process_pid}"
  rm -f "${worker_ack_process_crash_gate}"
  set +e
  wait "${worker_ack_process_crash_launcher_pid}"
  worker_ack_process_crash_status=$?
  set -e
  worker_ack_process_crash_launcher_pid=""
  if [[ "${worker_ack_process_crash_status}" == "0" ]]; then
    echo "Kafka Worker ACK process-crash JVM unexpectedly returned success after SIGKILL" >&2
    exit 1
  fi
  for attempt in $(seq 1 90); do
    set +e
    run_worker_smoke "${bootstrap_all}" "${worker_ack_process_crash_topic}" resume "" \
      >"${worker_ack_process_crash_resume_log}" 2>&1
    resume_status=$?
    set -e
    if [[ "${resume_status}" == "0" ]]; then
      cat "${worker_ack_process_crash_resume_log}"
      break
    fi
    if [[ "${attempt}" == "90" ]]; then
      cat "${worker_ack_process_crash_resume_log}" >&2
      echo "Kafka Worker ACK process-crash recovery did not reacquire the real Oxia lease" >&2
      exit 1
    fi
    sleep 1
  done
  rg -F --quiet "Kafka Worker vertical smoke passed" "${worker_ack_process_crash_resume_log}" \
    || { cat "${worker_ack_process_crash_resume_log}" >&2; exit 1; }
  echo "Kafka Worker ACK process-crash recovery E2E passed: the Worker Store WriteBatch was durable before SIGKILL and before Kafka commitSync ACK, and a fresh JVM replayed the exact source record through real Oxia authority, dedupe, ACK and final checkpoint."
  exit 0
fi

if [[ "${broker_network_partition_only}" == "1" ]]; then
  start_oxia
  broker_network_partition_topic="${KAFKA_DELAY_BROKER_NETWORK_PARTITION_TOPIC:-${worker_topic}-broker-network-partition}"
  run_worker_smoke "${bootstrap_all}" "${broker_network_partition_topic}" prepare
  network_name="${compose_project}_default"
  kafka_1_container="$("${compose[@]}" ps -q kafka-1)"
  test -n "${kafka_1_container}"
  docker network disconnect "${network_name}" "${kafka_1_container}"
  if docker network inspect --format '{{json .Containers}}' "${network_name}" | rg -F --quiet "${kafka_1_container}"; then
    echo "Kafka Broker network partition did not disconnect kafka-1" >&2
    exit 1
  fi
  wait_for_broker kafka-2
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSurvivorLeaderRecoverySmoke \
    -PkafkaClientJar="${client_jar}" \
    -PkafkaBootstrap="${bootstrap_survivors}" \
    -PkafkaSurvivorTopics="${broker_network_partition_topic},${worker_destination_topic},${worker_destination_topic}-receipt" \
    --no-daemon --console=plain
  run_worker_smoke "${bootstrap_survivors}" "${broker_network_partition_topic}" resume ""
  docker network connect "${network_name}" "${kafka_1_container}"
  if ! docker network inspect --format '{{json .Containers}}' "${network_name}" | rg -F --quiet "${kafka_1_container}"; then
    echo "Kafka Broker network partition did not reconnect kafka-1" >&2
    exit 1
  fi
  wait_for_broker kafka-1
  echo "Kafka Broker network-partition recovery E2E passed: kafka-1 stayed alive but was disconnected from the Compose network after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia Worker authority and source apply/ACK/checkpoint, and kafka-1 reconnected afterward."
  exit 0
fi

if [[ "${broker_tcp_cut_only}" == "1" ]]; then
  start_oxia
  broker_tcp_cut_topic="${KAFKA_DELAY_BROKER_TCP_CUT_TOPIC:-${worker_topic}-broker-tcp-cut}"
  broker_tcp_cut_group="${broker_tcp_cut_topic}-group"
  run_worker_smoke "${bootstrap_all}" "${broker_tcp_cut_topic}" prepare
  if [[ ! -s "${broker_tcp_pre_cut_file}" ]]; then
    cat "${broker_tcp_cut_log}" >&2
    echo "Kafka raw TCP fault proxy did not forward the pre-cut Worker connection" >&2
    exit 1
  fi
  GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaLeaderPlacementSmoke \
    "-PkafkaClientJar=${client_jar}" \
    "-PkafkaBootstrap=${bootstrap_all}" \
    "-PkafkaLeaderPlacementTopic=${broker_tcp_cut_topic}" \
    "-PkafkaLeaderPlacementGroup=${broker_tcp_cut_group}" \
    --no-daemon --console=plain
  touch "${broker_tcp_cut_file}"
  cut_deadline=$((SECONDS + 60))
  while [[ ! -f "${broker_tcp_ack_file}" ]]; do
    if (( SECONDS >= cut_deadline )); then
      cat "${broker_tcp_cut_log}" >&2
      echo "Kafka raw TCP fault proxy did not acknowledge the endpoint cut" >&2
      exit 1
    fi
    sleep 1
  done
  NEREUS_DELAY_KAFKA_WORKER_GROUP_ID="${broker_tcp_cut_group}" \
    run_worker_smoke "${bootstrap_all}" "${broker_tcp_cut_topic}" resume ""
  touch "${broker_tcp_release_file}"
  if [[ ! -s "${broker_tcp_post_cut_file}" ]]; then
    cat "${broker_tcp_cut_log}" >&2
    echo "Kafka raw TCP fault proxy did not reject a post-cut Broker-1 connection" >&2
    exit 1
  fi
  if [[ ! -s "${broker_tcp_post_cut_handoff_file}" ]]; then
    cat "${broker_tcp_cut_log}" >&2
    echo "Kafka raw TCP fault proxy did not forward a later post-cut connection to Broker-2" >&2
    exit 1
  fi
  echo "Kafka Worker raw TCP Broker-endpoint cut recovery E2E passed: Broker-1 remained alive, the source and selected group-coordinator partitions were explicitly placed on Broker-2, the raw proxy rejected Broker-1 once and handed later connections to Broker-2, and a fresh Worker resumed the same source through the full bootstrap list with real Oxia authority and source apply/ACK/checkpoint."
  exit 0
fi

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaTopic="${topic_1}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSourceSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaSourceTopic="${source_topic}" \
  --no-daemon --console=plain

GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaMutationSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_all}" \
  -PkafkaMutationTopic="${mutation_topic}" \
  --no-daemon --console=plain

if [[ "${with_oxia}" == "1" ]]; then
  start_oxia
fi
run_mutation_worker_smoke "${bootstrap_all}"
run_route_worker_smoke "${bootstrap_all}"
run_worker_smoke "${bootstrap_all}" "${worker_topic}"
restart_worker_topic="${KAFKA_DELAY_RESTART_WORKER_TOPIC:-${worker_topic}-broker-restart}"
run_worker_smoke "${bootstrap_all}" "${restart_worker_topic}" prepare

run_k2_smoke "${bootstrap_all}"

"${compose[@]}" stop kafka-1
wait_for_broker kafka-2
GRADLE_USER_HOME="${gradle_user_home}" ./gradlew runRealKafkaSmoke \
  -PkafkaClientJar="${client_jar}" \
  -PkafkaBootstrap="${bootstrap_survivors}" \
  -PkafkaTopic="${topic_1}" \
  -PsmokeMode=preserve \
  --no-daemon --console=plain

run_worker_smoke "${bootstrap_survivors}" "${restart_worker_topic}" resume

if [[ "${route_failover}" == "1" && "${with_oxia}" == "1" ]]; then
  echo "Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, accepted Route Worker apply across broker-1 failover, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence."
else
  echo "Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence."
fi
