# Delay V1 isolated Oxia E2E

`run-oxia-real-service.sh` builds the locked Oxia checkout in a unique Docker
Compose project, starts one standalone Oxia shard on a temporary host port,
waits for the gRPC health service, and runs the Delay opt-in real-service
smokes against that container. The smoke set covers Oxia Owner Lease, Control,
Recovery, signed Route publication/refresh, Gateway audit and Gateway
tenant-admission CAS. The compose
service uses only its container filesystem; it does not reuse existing
containers, ports, or volumes.

From the Delay checkout:

```text
./e2e/run-oxia-real-service.sh
```

Use `NEREUS_DELAY_OXIA_CHECKOUT=/absolute/path/to/oxia` and
`NEREUS_DELAY_OXIA_E2E_PORT=<unused-port>` to override the defaults. The
result is Dockerized Oxia plus host-side Delay authority, audit and admission
smoke evidence.
It is not a complete Kafka/Pulsar broker, source-consumer, Worker scheduling,
HA or release E2E; those require the locked upstream client/broker artifacts
and separate lifecycle gates.

The latest admission-inclusive run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786746636-41339` and host port `16651`; the selected
real-service tests passed and the matching Compose container/network were
cleaned up.

## Kafka K1/K2 real-client E2E

`run-kafka-real-client-e2e.sh` builds a temporary broker image from the locked
K1/K2 Kafka worktree, starts a unique three-broker KRaft Compose project, and
uses the matching client jar from that same worktree. The K1 smoke checks the
canonical topic UUID fence across delete/recreate. The source handoff smoke
then writes two NDL1/V1 records through the guarded K1 producer, reads them
through an explicitly source-locked Kafka binding, and verifies exact
Topic-UUID/offset/leader-epoch/LogAppendTime positions, same-group replay
before ACK, `commitSync`-after-ACK and an empty replay after both ACKs. The
Worker smoke then recovers one pre-activation record through the no-ACK
recovery cursor, activates the owned shard at the exact barrier, applies the
next guarded record through `WorkerShardRuntime` and RocksDB `WriteBatch`, and
checks the committed group offset after synchronous ACK plus exact owner-lease
release on drain. The default owner authority is deliberately deterministic;
when `NEREUS_DELAY_OXIA_ENDPOINT` is set, the same Worker smoke can acquire
the assignment through a real Oxia ephemeral session-bound lease. This remains
an owner-authority smoke, not Route placement or publication evidence. The K2 smoke
creates
separate target and receipt topics, sends both through one transaction-v2
guarded producer transaction, checks commit and abort with `read_committed`
consumers, verifies the exact target payload and canonical receipt key/value,
rejects a stale target TopicId after same-name delete/recreate, and commits
against the replacement target. The harness then stops broker 1 and reuses the
surviving K1 topic through brokers 2 and 3. It records source SHA, client-jar
SHA256, broker image ID and allocated ports, and cleans only its own Compose
project, volumes, temporary image and staging directory. The Kafka checkout
must be clean; ignored build outputs are read only.

```text
./e2e/run-kafka-real-client-e2e.sh
```

Use `NEREUS_DELAY_KAFKA_CHECKOUT`, `NEREUS_DELAY_KAFKA_CLIENT_JAR`, or the
`KAFKA_BROKER_*_PORT` variables to override local paths and ports. The latest
run passed with Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Compose project
`nereus-delay-kafka-e2e-1786757667-58603`, ports `19195,19196,19197`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`.
The source smoke reported source Topic UUID `ay9r1XMxQUycBBCYwxqqvg`, first
offset `0`, second offset `1`, and `committedAfterRestart=empty`. The source
binding now creates only a `GuardedConsumer`, binds cluster/topic/TopicId/
partition, and validates Fetch v13+ evidence before exposing each record;
the recovery cursor uses the same proof and never commits. The Worker smoke
reported:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch and commitSync ACK
```

This is current guarded source-handoff plus real-Kafka local recovery/apply/ACK
opt-in evidence. The optional Oxia mode additionally proves the network
session-bound owner lease for the smoke-created assignment. It does not claim
EndTxn response-loss, Fetch response-loss, LSO/retention-floor recovery, Route
assignment publication, placement authority, ACK-failure injection,
due/Lane/publish/checkpoint production wiring or the complete Worker vertical.

The latest optional real-Oxia run used Delay
`a7fd5fa7dd35d5d8535d3c63e577208d29fc2c5`, Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Kafka Compose project
`nereus-delay-kafka-e2e-1786759086-73769` on ports `19300,19301,19302`, and
Oxia Compose project `nereus-delay-kafka-oxia-e2e-1786759086-73769` on port
`16656`. It reported:

```text
Kafka Worker authority smoke passed: real Oxia session-bound lease
```

The harness exited successfully and removed its matching Kafka/Oxia Docker
resources. Run it with:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_OXIA_CHECKOUT=/absolute/path/to/oxia \
./e2e/run-kafka-real-client-e2e.sh
```

## Pulsar P1 real-client service E2E

`run-pulsar-real-client-e2e.sh` builds a temporary Pulsar server image from the
locked P1 distribution tarball, enables the broker entry-timestamp/index
interceptors required by the P1 guarded-send contract, and runs the real
Pulsar client binding against a unique single-node Compose project. The writer
smoke creates a persistent topic with the exact three resource-guard
properties, sends with the old guard and checks evidenced persistence, closes
and deletes the topic, recreates the same name with a new guard, requires
old-guard producer creation to fail with typed `definitelyNotPersisted`, and
sends with the replacement guard. The source smoke then uses guarded
`SUBSCRIBE`, exact physical-topic/guard validation and connection generations:
it replays an unacknowledged record after a client reconnect, ACKs only after
the synchronous broker ACK call, and confirms that the committed record is not
replayed. It extracts the distribution `lib/*.jar` files into a temporary host
directory for the client runtime; the client/server jars and distribution all
come from the same locked P1 checkout.

```text
./e2e/run-pulsar-real-client-e2e.sh
```

The harness requires a clean
`nereus/delay-resource-guard-v1` checkout descended from
`8dae0236c0a0d405ed7f8303081080520fe91551`. It records the P1 source SHA,
distribution and client-jar SHA-256 values, image ID and allocated ports, and
cleans only its own Compose project, volumes, temporary image and staging
directories. The current run passed with P1
`f813c96687cc19e6fca1c82d3d161cf3e045c86b`, distribution SHA-256
`bfe0c479c60db1a7a56f4548bd821d218c4c284dceb7c112d92f425606adec37`, image
`sha256:735e2a6b952e2f7d4c8fc4c7a7b0d4ec2a852a9f4a9b21e82b076477cf19669f`,
Compose project `nereus-delay-pulsar-e2e-1786743812-11877`, ports
`19827,19828`, and writer output
`initial=PERSISTED, stale=DEFINITIVELY_NOT_PERSISTED, replacement=PERSISTED`.
The source output was
`firstLedger=11, firstEntry=0, secondLedger=11, secondEntry=1,
firstConnectionGeneration=1, secondConnectionGeneration=2`, followed by an
empty poll after the ACK. The client artifacts were
`pulsar-client-original` SHA-256
`a636470f7d3f04af18980b84703a2b90f240a4bb58f77f8c19c1fd05b5bb40b2`,
`pulsar-client-api` SHA-256
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`pulsar-common` SHA-256
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`.
The exit check found no matching container, network, volume or temporary
image.

This closes only the single-node real P1 client/broker delete-recreate and
guarded source replay/ACK cut. It does not claim unload, multi-broker failover,
old-peer proxy compatibility, source unload/failover/session ownership,
guarded Fetch/rewind, D3 Direct SDK integration or the Worker vertical.

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

./gradlew runRealKafkaK2Smoke \
  -PkafkaClientJar=/absolute/path/to/kafka-clients-4.4.0-SNAPSHOT.jar \
  -PkafkaBootstrap=127.0.0.1:9092 \
  -PkafkaTargetTopic=nereus-delay-k2-target \
  -PkafkaReceiptTopic=nereus-delay-k2-receipt

./gradlew runRealKafkaSourceSmoke \
  -PkafkaClientJar=/absolute/path/to/kafka-clients-4.4.0-SNAPSHOT.jar \
  -PkafkaBootstrap=127.0.0.1:9092 \
  -PkafkaSourceTopic=nereus-delay-source-topic

./gradlew runRealPulsarSmoke \
  -PpulsarClientClasspath=/absolute/path/pulsar-client.jar:/absolute/path/pulsar-client-api.jar:/absolute/path/pulsar-common.jar

./gradlew runRealPulsarServiceSmoke \
  -PpulsarClientClasspath=/absolute/path/pulsar-client.jar:/absolute/path/pulsar-client-api.jar:/absolute/path/pulsar-common.jar \
  -PpulsarRuntimeDir=/absolute/path/extracted-pulsar/lib \
  -PpulsarServiceUrl=pulsar://127.0.0.1:6650 \
  -PpulsarAdminUrl=http://127.0.0.1:8080 \
  -PpulsarTopic=nereus-delay-p1-topic

./gradlew runRealPulsarSourceSmoke \
  -PpulsarClientClasspath=/absolute/path/pulsar-client.jar:/absolute/path/pulsar-client-api.jar:/absolute/path/pulsar-common.jar \
  -PpulsarRuntimeDir=/absolute/path/extracted-pulsar/lib \
  -PpulsarServiceUrl=pulsar://127.0.0.1:6650 \
  -PpulsarAdminUrl=http://127.0.0.1:8080 \
  -PpulsarTopic=nereus-delay-p1-topic
```

The Kafka K1 binding uses `GuardedProducer.sendGuarded`; the K2 binding uses
`GuardedTransactionalProducer.sendGuardedInTransaction` and
`KafkaTransactionalDestinationAdapter`, mapping only verified guarded evidence
into a Delay result. The Pulsar binding requires a
`TopicResourceGuard` producer and a `GuardedMessageId`; its API smoke covers
success, resource-incarnation mismatch and typed error evidence. These opt-in
checks do not silently fall back to stock/name-only clients and do not by
themselves establish the remaining K2/D3 receipt/source-consumer or release
gates.
