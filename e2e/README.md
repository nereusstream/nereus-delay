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
come from the same locked P1 checkout. The Worker smoke then reuses the
post-seek guarded consumer through no-ACK recovery, applies the next record
through `WorkerShardRuntime` and RocksDB `WriteBatch`, performs synchronous ACK
and drains the exact owner lease.

```text
./e2e/run-pulsar-real-client-e2e.sh
```

The harness requires a clean
`nereus/delay-resource-guard-v1` checkout descended from
`8dae0236c0a0d405ed7f8303081080520fe91551`. It records the P1 source SHA,
distribution and client-jar SHA-256 values, image ID and allocated ports, and
cleans only its own Compose project, volumes, temporary image and staging
directories. The current run passed with P1
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`, image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
Compose project `nereus-delay-pulsar-e2e-1786760203-85592`, ports
`19930,19931`, and writer output
`initial=PERSISTED, stale=DEFINITIVELY_NOT_PERSISTED, replacement=PERSISTED`.
The source output was
`firstLedger=11, firstEntry=0, secondLedger=11, secondEntry=1,
firstConnectionGeneration=5, secondConnectionGeneration=6`, followed by an
empty poll after the ACK; positioned recovery skipped `11/0` and returned the
second command after a stable proof. The client artifacts were
`pulsar-client-original` SHA-256
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`pulsar-client-api` SHA-256
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`pulsar-common` SHA-256
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`.
The exit check found no matching container, network, volume or temporary
image.

The Worker output was:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=15/0, active apply ledger/entry=15/1, guarded SUBSCRIBE, RocksDB WriteBatch and ACK
```

This closes only the single-node real P1 client/broker delete-recreate,
guarded source replay/ACK and Worker recovery/apply/ACK cuts. It does not
claim unload, multi-broker failover, old-peer proxy compatibility, source
unload/failover/session ownership, guarded Fetch/rewind, network Oxia
session/placement, D3 Direct SDK integration or production multi-shard Worker
wiring.

The Worker can also use a real network Oxia owner-lease authority. This is
opt-in; without the environment variable the deterministic in-memory
authority above remains in use:

```text
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=16657 \
PULSAR_BROKER_PORT=19940 PULSAR_WEB_PORT=19941 \
./e2e/run-pulsar-real-client-e2e.sh
```

The verified run used P1
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Pulsar Compose project
`nereus-delay-pulsar-e2e-1786761304-98904`, Oxia Compose project
`nereus-delay-pulsar-oxia-e2e-1786761304-98904`, ports `19940,19941` and
`16657`, P1 image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
and Oxia image
`sha256:4fdba6125c3f3ceca0d5ebe0224464ec83eb815e91999e1910660c60416231ca`.
It printed:

```text
Pulsar Worker authority smoke passed: real Oxia session-bound lease
```

The matching containers, networks and volumes were removed; the locally
built Oxia image remains. This proves session-bound owner authority around
one smoke-created assignment, not Oxia placement, Route publication, Broker
source ownership, multi-broker failover, production multi-shard Worker
wiring, due/Lane/publish/checkpoint paths or crash gates.

## Worker assignment publication and acceptance

The real Kafka and Pulsar Worker smokes now run the local
`WorkerPlacementPolicy` scorer through `WorkerAssignmentCoordinator`. The
selected assignment is encoded canonically, published through revision-CAS
authority, and reread at the exact revision before native source setup. The
default mode uses the deterministic in-memory authority; the existing Oxia
opt-in uses the session-bound `OxiaSyncWorkerAssignmentBackend`.

The fresh default Kafka run used project
`nereus-delay-kafka-e2e-1786763617-28066` on `19420,19421,19422`; the fresh
default Pulsar run used project `nereus-delay-pulsar-e2e-1786763739-29494` on
`19970,19971`. They printed:

```text
Kafka Worker assignment publication/acceptance passed: revision=1, worker=kafka-worker, authority=in-memory
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=in-memory
```

The optional network-authority runs used Kafka/Oxia projects
`nereus-delay-kafka-e2e-1786763887-31303` /
`nereus-delay-kafka-oxia-e2e-1786763887-31303` on
`19430,19431,19432` / `16658`, and Pulsar/Oxia projects
`nereus-delay-pulsar-e2e-1786764116-34287` /
`nereus-delay-pulsar-oxia-e2e-1786764116-34287` on
`19980,19981` / `16659`. Both printed the same placement line with
`authority=real Oxia session-bound`; the Pulsar run also printed:

```text
Pulsar Worker authority smoke passed: real Oxia session-bound lease
```

Delay source-lock commit: `759c4a49b54395211c8ee02c2705006525288fe3`.
Oxia source-lock commit: `37a17bef17202d5fd6e23282da5fd26d94865484`.
The matching Compose containers, networks and volumes were removed. This
closes per-shard authoritative publication/acceptance and exact Worker
pre-wiring reread for the smoke-created assignments; it does not claim
catalog-driven multi-shard placement, signed RouteSnapshot publication,
source ownership transfer/reconnect, capacity-envelope authority,
due/Lane/publish/checkpoint production wiring, failover or crash gates.

## Signed Route publication to Worker assignment authority

`RouteWorkerAssignmentCoordinator` binds Worker assignment identity to the
signed `RouteSnapshotV1.snapshotDigest`. It reads the active or exact
historical Route through the tenant-authorized provider, publishes the
route-bound canonical assignment through revision-CAS authority, and rereads
the same Route incarnation, partition barrier and digest before acceptance.
The real Oxia harness includes
`OxiaRealRouteWorkerAssignmentSmokeTest`, which uses separate session-fenced
Route publisher/provider and Worker assignment-authority clients.

The verified run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786765353-47776`, host port `16660`, and image
`sha256:7001f39d94a8d21d74928aad06e7666fcf4bcf3879ef6d27940c9a7ef8db702f`.
The selected real-service Gradle suite passed and the matching container and
network were removed; the local image remains. This closes the signed Route
event/head → Route cache → route-bound assignment CAS cut for one assignment,
not catalog-driven multi-shard placement, capacity-envelope authority,
Broker source ownership transfer/reconnect, due/Lane/publish/checkpoint
production wiring, failover or crash gates.

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
