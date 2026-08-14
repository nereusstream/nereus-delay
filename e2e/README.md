# Delay V1 isolated Oxia E2E

`run-oxia-real-service.sh` builds the locked Oxia checkout in a unique Docker
Compose project, starts one standalone Oxia shard on a temporary host port,
waits for the gRPC health service, and runs the Delay opt-in real-service
smokes against that container. The smoke set covers Oxia Owner Lease, Control,
Recovery, signed Route publication/refresh and Gateway audit. The compose
service uses only its container filesystem; it does not reuse existing
containers, ports, or volumes.

From the Delay checkout:

```text
./e2e/run-oxia-real-service.sh
```

Use `NEREUS_DELAY_OXIA_CHECKOUT=/absolute/path/to/oxia` and
`NEREUS_DELAY_OXIA_E2E_PORT=<unused-port>` to override the defaults. The
result is Dockerized Oxia plus host-side Delay authority/audit smoke evidence.
It is not a complete Kafka/Pulsar broker, source-consumer, Worker scheduling,
HA or release E2E; those require the locked upstream client/broker artifacts
and separate lifecycle gates.

## Kafka K1 real-client E2E

`run-kafka-real-client-e2e.sh` builds a temporary broker image from the locked
K1 Kafka worktree, starts a unique three-broker KRaft Compose project, and uses
the matching K1 client jar from that same worktree. The smoke checks the
canonical topic UUID fence across delete/recreate, then stops broker 1 and
reuses the surviving topic through brokers 2 and 3. It records the K1 source
SHA, client-jar SHA256, locally built broker image ID and allocated ports, and
cleans up only its own Compose project, volumes, temporary image and staging
directory. The Kafka checkout must be clean; ignored build outputs are read
only.

```text
./e2e/run-kafka-real-client-e2e.sh
```

Use `NEREUS_DELAY_KAFKA_CHECKOUT`, `NEREUS_DELAY_KAFKA_CLIENT_JAR`, or the
`KAFKA_BROKER_*_PORT` variables to override local paths and ports. This is a
K1 produce/resource-incarnation smoke; it does not claim K2 target-plus-receipt
transactions, Fetch/ACK source recovery, or the full Worker vertical.

## Pulsar P1 real-client service E2E

`run-pulsar-real-client-e2e.sh` builds a temporary Pulsar server image from the
locked P1 distribution tarball, enables the broker entry-timestamp/index
interceptors required by the P1 guarded-send contract, and runs the real
Pulsar client binding against a unique single-node Compose project. The smoke
creates a persistent topic with the exact three resource-guard properties,
sends with the old guard and checks evidenced persistence, closes and deletes
the topic, recreates the same name with a new guard, requires old-guard
producer creation to fail with typed `definitelyNotPersisted`, and sends with
the replacement guard. It extracts the distribution `lib/*.jar` files into a
temporary host directory for the client runtime; the client/server jars and
distribution all come from the same locked P1 checkout.

```text
./e2e/run-pulsar-real-client-e2e.sh
```

The harness requires a clean
`nereus/delay-resource-guard-v1` checkout descended from
`8dae0236c0a0d405ed7f8303081080520fe91551`. It records the P1 source SHA,
distribution and client-jar SHA-256 values, image ID and allocated ports, and
cleans only its own Compose project, volumes, temporary image and staging
directories. The current run passed with P1
`7eebd41d5b0917a0dfe5ea26ef3062a39f70a6d9`, distribution SHA-256
`d4b9e8aa6b44582c383262007217980793ec41bdf7fa3a1a4285e220407fef32`, image
`sha256:f377aeddd73913830a1004287e14eae910e739f39793a96fe41d38f2e5aca264`,
Compose project `nereus-delay-pulsar-e2e-1786737555-46201`, ports
`19651,19652`, and output
`initial=PERSISTED, stale=DEFINITIVELY_NOT_PERSISTED, replacement=PERSISTED`.
The exit check found no matching container, network, volume or temporary
image.

This closes only the single-node real P1 client/broker delete-recreate cut. It
does not claim unload, multi-broker failover, old-peer proxy compatibility,
guarded source Fetch/ACK/rewind, D3 Direct SDK integration or the Worker
vertical.

## Optional source-locked client bindings

The shared Gradle build never puts upstream Kafka or Pulsar classes on the
normal `main` source set. The real bindings are explicit source sets and fail
closed when their artifact paths are omitted:

```text
./gradlew compileRealKafka \
  -PkafkaClientJar=/absolute/path/to/kafka-clients-4.4.0-SNAPSHOT.jar

./gradlew runRealKafkaSmoke \
  -PkafkaClientJar=/absolute/path/to/kafka-clients-4.4.0-SNAPSHOT.jar \
  -PkafkaBootstrap=127.0.0.1:9092 -PkafkaTopic=nereus-delay-k1-topic

./gradlew runRealPulsarSmoke \
  -PpulsarClientClasspath=/absolute/path/pulsar-client.jar:/absolute/path/pulsar-client-api.jar:/absolute/path/pulsar-common.jar

./gradlew runRealPulsarServiceSmoke \
  -PpulsarClientClasspath=/absolute/path/pulsar-client.jar:/absolute/path/pulsar-client-api.jar:/absolute/path/pulsar-common.jar \
  -PpulsarRuntimeDir=/absolute/path/extracted-pulsar/lib \
  -PpulsarServiceUrl=pulsar://127.0.0.1:6650 \
  -PpulsarAdminUrl=http://127.0.0.1:8080 \
  -PpulsarTopic=nereus-delay-p1-topic
```

The Kafka binding uses `GuardedProducer.sendGuarded` and maps only verified
K1 response evidence into a Delay result. The Pulsar binding requires a
`TopicResourceGuard` producer and a `GuardedMessageId`; its API smoke covers
success, resource-incarnation mismatch and typed error evidence. These opt-in
checks do not silently fall back to stock/name-only clients and do not by
themselves establish the K2/D3 transaction, source-consumer or release gates.
