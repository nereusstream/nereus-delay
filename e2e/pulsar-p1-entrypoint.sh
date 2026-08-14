#!/usr/bin/env bash
set -euo pipefail

: "${PULSAR_BROKER_PORT:?PULSAR_BROKER_PORT is required}"
: "${PULSAR_WEB_PORT:?PULSAR_WEB_PORT is required}"

sed -i "s/^brokerServicePort=.*/brokerServicePort=${PULSAR_BROKER_PORT}/" /opt/pulsar/conf/standalone.conf
sed -i "s/^webServicePort=.*/webServicePort=${PULSAR_WEB_PORT}/" /opt/pulsar/conf/standalone.conf
sed -i "/^webServicePort=.*/a brokerEntryMetadataInterceptors=org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor,org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor" /opt/pulsar/conf/standalone.conf
sed -i "/^brokerEntryMetadataInterceptors=.*/a exposingBrokerEntryMetadataToClientEnabled=true" /opt/pulsar/conf/standalone.conf
exec /opt/pulsar/bin/pulsar standalone --advertised-address 127.0.0.1
