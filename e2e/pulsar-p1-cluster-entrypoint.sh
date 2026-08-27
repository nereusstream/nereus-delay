#!/usr/bin/env bash
set -euo pipefail

component="${PULSAR_COMPONENT:?PULSAR_COMPONENT is required}"
broker_conf=/opt/pulsar/conf/broker.conf
bookkeeper_conf=/opt/pulsar/conf/bookkeeper.conf
zookeeper_conf=/opt/pulsar/conf/zookeeper.conf

replace_property() {
  local file="$1"
  local key="$2"
  local value="$3"
  if grep -q "^${key}=" "${file}"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "${file}"
  elif grep -q "^# ${key}=" "${file}"; then
    sed -i "s|^# ${key}=.*|${key}=${value}|" "${file}"
  else
    printf '%s\n' "${key}=${value}" >> "${file}"
  fi
}

resolve_zk_metadata_store() {
  local zk_host="${PULSAR_ZOOKEEPER_HOST:-zk}"
  local zk_ip
  zk_ip="$(getent hosts "${zk_host}" | awk 'NR == 1 {print $1}')"
  if [[ -z "${zk_ip}" ]]; then
    echo "could not resolve ZooKeeper host: ${zk_host}" >&2
    return 1
  fi
  printf 'zk:%s:2181' "${zk_ip}"
}

wait_for_zookeeper() {
  local zk_host="${PULSAR_ZOOKEEPER_HOST:-zk}"
  until (echo >/dev/tcp/${zk_host}/2181) 2>/dev/null; do
    sleep 1
  done
}

case "${component}" in
  zookeeper)
    replace_property "${zookeeper_conf}" dataDir /pulsar/data/zookeeper
    exec /opt/pulsar/bin/pulsar zookeeper
    ;;
  init)
    : "${PULSAR_CLUSTER_NAME:?PULSAR_CLUSTER_NAME is required}"
    : "${PULSAR_METADATA_STORE:?PULSAR_METADATA_STORE is required}"
    : "${PULSAR_CONFIGURATION_STORE:?PULSAR_CONFIGURATION_STORE is required}"
    : "${PULSAR_WEB_SERVICE_URL:?PULSAR_WEB_SERVICE_URL is required}"
    : "${PULSAR_BROKER_SERVICE_URL:?PULSAR_BROKER_SERVICE_URL is required}"
    wait_for_zookeeper
    metadata_store="$(resolve_zk_metadata_store)"
    until /opt/pulsar/bin/pulsar initialize-cluster-metadata \
        --cluster "${PULSAR_CLUSTER_NAME}" \
        --metadata-store "${metadata_store}" \
        --configuration-metadata-store "${metadata_store}" \
        --web-service-url "${PULSAR_WEB_SERVICE_URL}" \
        --broker-service-url "${PULSAR_BROKER_SERVICE_URL}"; do
      sleep 2
    done
    ;;
  bookie)
    : "${PULSAR_ZOOKEEPER_SERVERS:?PULSAR_ZOOKEEPER_SERVERS is required}"
    replace_property "${bookkeeper_conf}" zkServers "${PULSAR_ZOOKEEPER_SERVERS}"
    replace_property "${bookkeeper_conf}" advertisedAddress "${PULSAR_BOOKIE_ADVERTISED_ADDRESS:-bookie}"
    replace_property "${bookkeeper_conf}" useHostNameAsBookieID "${PULSAR_BOOKIE_USE_HOSTNAME_AS_ID:-true}"
    replace_property "${bookkeeper_conf}" allowLoopback "${PULSAR_BOOKIE_ALLOW_LOOPBACK:-false}"
    if [[ -n "${PULSAR_BOOKIE_ID:-}" ]]; then
      sed -i "s|^# bookieId=.*|bookieId=${PULSAR_BOOKIE_ID}|" "${bookkeeper_conf}"
    fi
    replace_property "${bookkeeper_conf}" journalDirectory /pulsar/data/bookkeeper/journal
    replace_property "${bookkeeper_conf}" ledgerDirectories /pulsar/data/bookkeeper/ledgers
    replace_property "${bookkeeper_conf}" prometheusStatsHttpPort "${PULSAR_BOOKIE_PROMETHEUS_PORT:-8000}"
    replace_property "${bookkeeper_conf}" httpServerPort "${PULSAR_BOOKIE_PROMETHEUS_PORT:-8000}"
    exec /opt/pulsar/bin/pulsar bookie
    ;;
  broker)
    : "${PULSAR_CLUSTER_NAME:?PULSAR_CLUSTER_NAME is required}"
    : "${PULSAR_ZOOKEEPER_SERVERS:?PULSAR_ZOOKEEPER_SERVERS is required}"
    : "${PULSAR_METADATA_STORE:?PULSAR_METADATA_STORE is required}"
    : "${PULSAR_CONFIGURATION_STORE:?PULSAR_CONFIGURATION_STORE is required}"
    : "${PULSAR_BROKER_PORT:?PULSAR_BROKER_PORT is required}"
    : "${PULSAR_WEB_PORT:?PULSAR_WEB_PORT is required}"
    : "${PULSAR_ADVERTISED_ADDRESS:?PULSAR_ADVERTISED_ADDRESS is required}"
    metadata_store="$(resolve_zk_metadata_store)"
    replace_property "${broker_conf}" clusterName "${PULSAR_CLUSTER_NAME}"
    replace_property "${broker_conf}" zookeeperServers "${PULSAR_ZOOKEEPER_SERVERS}"
    replace_property "${broker_conf}" metadataStoreUrl "${metadata_store}"
    replace_property "${broker_conf}" configurationMetadataStoreUrl "${metadata_store}"
    replace_property "${broker_conf}" brokerServicePort "${PULSAR_BROKER_PORT}"
    replace_property "${broker_conf}" webServicePort "${PULSAR_WEB_PORT}"
    replace_property "${broker_conf}" advertisedAddress "${PULSAR_ADVERTISED_ADDRESS}"
    replace_property "${broker_conf}" internalListenerName "${PULSAR_INTERNAL_LISTENER_NAME:-internal}"
    if [[ -n "${PULSAR_ADVERTISED_LISTENERS:-}" ]]; then
      replace_property "${broker_conf}" advertisedListeners "${PULSAR_ADVERTISED_LISTENERS}"
    fi
    if [[ -n "${PULSAR_BIND_ADDRESSES:-}" ]]; then
      replace_property "${broker_conf}" bindAddresses "${PULSAR_BIND_ADDRESSES}"
    fi
    replace_property "${broker_conf}" bindAddress 0.0.0.0
    replace_property "${broker_conf}" managedLedgerDefaultEnsembleSize 1
    replace_property "${broker_conf}" managedLedgerDefaultWriteQuorum 1
    replace_property "${broker_conf}" managedLedgerDefaultAckQuorum 1
    replace_property "${broker_conf}" webSocketServiceEnabled false
    replace_property "${broker_conf}" functionsWorkerEnabled false
    # Keep pre-provisioned guarded destinations present across the bounded
    # failover handoff window.  Inactive-topic deletion would recreate the
    # topic without its resource-guard tuple and turn a broker handoff into a
    # false ResourceIncarnationMismatch.
    replace_property "${broker_conf}" brokerDeleteInactiveTopicsEnabled \
      "${PULSAR_BROKER_DELETE_INACTIVE_TOPICS_ENABLED:-false}"
    if [[ -n "${PULSAR_DELAYED_DELIVERY_STRICT:-}" ]]; then
      replace_property "${broker_conf}" isDelayedDeliveryDeliverAtTimeStrict \
        "${PULSAR_DELAYED_DELIVERY_STRICT}"
    fi
    printf '%s\n' \
      'brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor,org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor' \
      'exposingBrokerEntryMetadataToClientEnabled=true' >> "${broker_conf}"
    exec /opt/pulsar/bin/pulsar broker
    ;;
  *)
    echo "unknown PULSAR_COMPONENT: ${component}" >&2
    exit 2
    ;;
esac
