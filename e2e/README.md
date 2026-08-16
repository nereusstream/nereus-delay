# Delay V1 isolated Oxia E2E

`run-oxia-real-service.sh` builds the locked Oxia checkout in a unique Docker
Compose project, starts one standalone Oxia shard on a temporary host port,
waits for the gRPC health service, and runs the Delay opt-in real-service
smokes against that container. The smoke set covers Oxia Owner Lease, Control,
Recovery Catalog, atomic checkpoint intent/catalog publication, signed Route
publication/refresh and assignment, Gateway audit and Gateway tenant-admission
CAS. The compose
service uses only its container filesystem; it does not reuse existing
containers, ports, or volumes.

From the Delay checkout:

```text
./e2e/run-oxia-real-service.sh
```

Use `NEREUS_DELAY_OXIA_CHECKOUT=/absolute/path/to/oxia` and
`NEREUS_DELAY_OXIA_E2E_PORT=<unused-port>` to override the defaults. The
result is Dockerized Oxia plus host-side Delay authority, checkpoint, audit and
admission smoke evidence.
It is not a complete Kafka/Pulsar broker, source-consumer, Worker scheduling,
HA or release E2E; those require the locked upstream client/broker artifacts
and separate lifecycle gates.

The latest admission/checkpoint-inclusive run used Delay
`ac72e43803806b9c309b62150c0aa54b43f8a3ea`, Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786787138-90186` and host port `16675`; the selected
real-service tests passed with `BUILD SUCCESSFUL in 11s` and the matching
Compose container/network were cleaned up. The filesystem checkpoint adapter
is a provider-side crash-durable seam; remote Object Store credentials and
quiescence remain outside this receipt.

## Real Oxia + MinIO Worker checkpoint publication E2E

`run-oxia-minio-checkpoint-e2e.sh` composes the Worker checkpoint scheduler
with a real Oxia session-bound Owner Lease and the real S3-compatible MinIO
provider. The focused test creates the exact PENDING_UPLOAD intent, runs the
Worker checkpoint work-class turn, verifies Oxia's canonical PUBLISHED
Intent/Catalog projection, downloads the exact provider resource, and checks
the restored file inventory against the manifest. MinIO bucket versioning is
enabled before the test starts.

From the Delay checkout:

```text
./e2e/run-oxia-minio-checkpoint-e2e.sh
```

The runner uses a unique Compose project and bucket. Override the Oxia and
MinIO ports with `NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT` and
`NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT`; override the Oxia checkout with
`NEREUS_DELAY_OXIA_CHECKOUT`. The locked provider is
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` at digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The source-bound run used Delay
`7a2b7b7461dd56ff5c3ebbc0e5471756d148ad18`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, MinIO image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`,
Compose project `nereus-delay-oxia-minio-checkpoint-e2e-1786886192-18395`,
and ports `16719/16729`. It reported:

```text
Oxia + MinIO Worker checkpoint publication passed: atomic Intent/Catalog=true, immutable object upload/download=true, checkpoint=00000000000000000000000000000003
BUILD SUCCESSFUL
Oxia + MinIO Worker checkpoint publication E2E passed: real Oxia Intent/Catalog authority and real MinIO immutable objects
```

After the run, exact-name checks found no project containers, network, volume
or temporary Oxia image. The locked MinIO base image is retained for other
real-service runs. This receipt does not prove real-Oxia REAPING/RecoveryPin
competition, provider quiescence/consistency attestation, late-PUT or delete
response-loss handling, restore activation, multi-shard placement, raw chaos
or V1 release readiness.

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
release on drain. The Worker smoke also runs the bounded final local RocksDB
checkpoint and checks its file inventory before that release. The default
owner authority is deliberately deterministic;
when `NEREUS_DELAY_OXIA_ENDPOINT` is set, the same Worker smoke can acquire
the assignment through a real Oxia ephemeral session-bound lease. This remains
an owner-authority smoke, not Route placement or publication evidence. The K2 smoke
creates
separate target and receipt topics, sends both through one transaction-v2
guarded producer transaction, checks commit and abort with `read_committed`
consumers, verifies the exact target payload and canonical receipt key/value,
rejects a stale target TopicId after same-name delete/recreate, and commits
against the replacement target. The harness then stops broker 1 and reuses the
surviving K1 topic through brokers 2 and 3. Before the stop, the harness
prepares one exact Worker restart topic and persists one guarded record; after
the stop, a new Worker JVM resumes that same topic through the survivor
bootstrap. It records source SHA, client-jar SHA256, broker image ID and
allocated ports, and cleans only its own Compose project, volumes, temporary
image and staging directory. The Kafka checkout must be clean; ignored build
outputs are read only.

The same-topic resume line recovers offset 0, applies offset 1 through guarded
Fetch v13 and RocksDB `WriteBatch`, ACKs with synchronous `commitSync`, writes
the final local checkpoint and releases the exact Owner lease. This is
survivor-bootstrap plus fresh-process same-topic recovery evidence for one
partition, not in-flight ownership transfer or complete crash/failover release
evidence.

```text
./e2e/run-kafka-real-client-e2e.sh
```

Use `NEREUS_DELAY_KAFKA_CHECKOUT`, `NEREUS_DELAY_KAFKA_CLIENT_JAR`, or the
`KAFKA_BROKER_*_PORT` variables to override local paths and ports. The latest
run passed with Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Delay commit `3ca85c74`, Compose
project `nereus-delay-kafka-e2e-1786771524-17482`, ports `19270,19271,19272`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`.
The source smoke reported source Topic UUID `26IyMNTRSAiF0HFe6pPM2w`, first
offset `0`, second offset `1`, and `committedAfterRestart=empty`. The source
binding now creates only a `GuardedConsumer`, binds cluster/topic/TopicId/
partition, and validates Fetch v13+ evidence before exposing each record;
the recovery cursor uses the same proof and never commits. The Worker smoke
reported:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch and commitSync ACK
```

The same run also printed:

```text
Kafka Worker restart preparation passed: one guarded record persisted before broker failover
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
```

The second line is from a new Worker JVM bootstrapped through
`127.0.0.1:19271,127.0.0.1:19272` after broker 1 was stopped. The full harness
exited successfully and removed its matching Docker resources.

This is current guarded source-handoff plus real-Kafka local recovery/apply/ACK
opt-in evidence. The optional Oxia mode additionally proves the network
session-bound owner lease for the smoke-created assignment. It does not claim
EndTxn response-loss, Fetch response-loss, LSO/retention-floor recovery, Route
assignment publication, placement authority, ACK-failure injection,
due/Lane/publish/checkpoint production wiring or the complete Worker vertical.

### Kafka K2 broker failover-only cut

Set `NEREUS_DELAY_KAFKA_K2_FAILOVER=1` and
`NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY=1` to run a dedicated real-broker cut.
The K2 client pauses immediately before the guarded transaction-v2
`commitTransaction`/`EndTxn` boundary; the harness stops `kafka-1`, waits for a
surviving broker, releases the gate, and verifies the target-plus-keyed-receipt
pair with `read_committed` counts and exact records. The failover-only smoke
ends after that proof; normal K2 mode continues to cover abort and
same-name delete/recreate fencing.

The source-locked receipt used Delay `6912b940`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786790805-40581`, and ports
`19795,19796,19797`:

```bash
NEREUS_DELAY_KAFKA_K2_FAILOVER=1 \
NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY=1 \
KAFKA_BROKER_1_PORT=19795 KAFKA_BROKER_2_PORT=19796 KAFKA_BROKER_3_PORT=19797 \
./e2e/run-kafka-real-client-e2e.sh
```

It printed:

```text
K2 broker failover commit returned PUBLISHED: read_committed target+receipt pair
K2 broker failover smoke passed: target-plus-receipt transaction crossed broker-1 failover and exact read_committed records were verified
BUILD SUCCESSFUL in 10s
Kafka K2 broker failover E2E passed: target-plus-receipt transaction crossed broker-1 failover with read_committed resolution.
```

The client trace showed coordinator `19795/id=1` before the stop and
rediscovery at `19797/id=3` after broker-1 failed over. This run observed
`PUBLISHED`, so it does not close a lost `EndTxn` response; generic
response-loss resolution, Fetch/LSO/retention-floor ambiguity and crash gates
remain open. The receipt is source-bound integration evidence, not a V1 release
PASS.

### Kafka K2 committed response-loss-only cut

Set `NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS=1` and
`NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY=1` for the dedicated cut. The
test-only producer wrapper delegates the real guarded K2
`commitTransaction()`/`EndTxn`, then discards the local result. The transport
must recover through its normal commit-uncertainty path and a fresh
`read_committed` consumer; the run requires the exact target-plus-keyed-receipt
pair and typed `KAFKA_TRANSACTIONAL_RECEIPT` evidence before returning
`PUBLISHED`. This is a controlled client-side post-commit response cut, not raw
socket packet-loss or Broker failover injection.

The source-locked receipt used Delay `376252bae0faf6f2d5120e223886b3af8a54e636`,
Kafka `nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786828912-64477`, and ports
`19569,19570,19571`:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS=1 \
NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY=1 \
KAFKA_BROKER_1_PORT=19569 KAFKA_BROKER_2_PORT=19570 KAFKA_BROKER_3_PORT=19571 \
./e2e/run-kafka-real-client-e2e.sh
```

It printed:

```text
K2 committed response-loss smoke passed: real EndTxn committed the exact target-plus-receipt pair, the local response was discarded, and typed read_committed evidence resolved PUBLISHED
Kafka K2 committed response-loss E2E passed: real EndTxn commit was followed by local response loss and exact read_committed typed receipt resolution.
```

The receipt does not promote generic Kafka crash/response-loss, Fetch
response-loss, LSO/retention-floor recovery, or V1 release readiness.

### Kafka Shard Log signed mutation append/replay/ACK

The same harness now runs `runRealKafkaMutationSmoke` after the ordinary
source smoke. It creates one LogAppendTime topic with replication factor 3,
builds a signed `TIME_FENCE` System Mutation, appends its canonical frame with
the K1 `GuardedProducer` and exact `ProducerResourceGuard`, then reads the
same frame through the no-ACK recovery cursor and active guarded source. The
active source acknowledges only after `commitSync`; the smoke also rejects a
second visible entry. The appender maps missing/ambiguous response evidence
to `UNKNOWN` and does not fabricate a Produce-response leader epoch; replay
may enrich that optional field from Fetch metadata while route, resource,
offset and append-time identity must remain the same.

The fresh run used Delay `02f4b458`, Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786773092-35482`, and ports
`19570,19571,19572`. It printed:

```text
Kafka Shard Log mutation append/replay/ACK smoke passed: topicId=7jN1ZJcgRPSZILqxxNLqVw, offset=0, record=TIME_FENCE, guarded Producer, ordered mutation replay, commitSync ACK
```

This closes one source-locked Kafka partition's append → recovery replay →
active-source ACK path. It does not claim mutation apply, signature trust
authorization, automatic Publish or external Claim prerequisites, Pulsar
mutation support, response-loss/crash coverage, multi-shard production wiring
or release PASS. Local durable V1 Claim materialization is covered by the
focused Gradle tests, not by this Kafka transport smoke.

### Kafka mutation-to-Store Worker vertical

The full harness now also runs `runRealKafkaMutationWorkerSmoke`. Two signed
`TIME_FENCE` records are appended through the guarded Producer; strict recovery
applies offset `0`, the active Worker applies offset `1` through guarded Fetch
v13 and the shared RocksDB WriteBatch, then performs `commitSync` and final
checkpoint. The source-locked receipt used Delay mutation Worker commit
`eee022bd`, Kafka `05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and Compose project `nereus-delay-kafka-e2e-1786779783-8472` on
`19700,19701,19702`. It printed:

```text
Kafka mutation Worker vertical smoke passed: recovery TIME_FENCE offset=0, active Store apply TIME_FENCE offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
```

The network-authority rerun used Kafka/Oxia projects
`nereus-delay-kafka-e2e-1786781272-24697` /
`nereus-delay-kafka-oxia-e2e-1786781272-24697` on
`19710,19711,19712` / `16671`, and printed:

```text
Kafka mutation Worker assignment publication/acceptance passed: revision=1, worker=kafka-mutation-worker, authority=real Oxia session-bound
Kafka mutation Worker vertical smoke passed: recovery TIME_FENCE offset=0, active Store apply TIME_FENCE offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka mutation Worker authority smoke passed: real Oxia session-bound lease
```

Both full runs exited successfully; the Oxia run also completed the existing
K1/K2 and broker-failover/resume cuts. This closes the bounded Kafka
mutation-to-Store Worker plus network owner-session cut, not response-loss or
crash recovery, catalog placement, Route source ownership or release PASS.

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

### Kafka signed Route barrier to Worker assignment

When `NEREUS_DELAY_KAFKA_WITH_OXIA=1`, the same harness also runs
`runRealKafkaRouteWorkerSmoke`. It appends one guarded K1 record, requires the
Fetch v13/TopicId/LSO proof, signs a Kafka Route with the exact activation
barrier, publishes the Route event/head through a real session-fenced Oxia
authority, publishes and rereads a route-bound Worker assignment, then
recovers the pre-Route record into the Worker Store and ACKs it after the
RocksDB apply, then appends a second record and starts the guarded Worker
source at the signed barrier before applying and `commitSync` ACKing it. The
Worker drains through its final local checkpoint and releases the Oxia owner
lease and assignment.

The source-locked receipt used Delay `7e0abb87fff8db1c1d2d2f73ffdd44a0c6097112`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Kafka/Oxia projects `nereus-delay-kafka-e2e-1786785694-74566` /
`nereus-delay-kafka-oxia-e2e-1786785694-74566`, ports
`19730,19731,19732` / `16673`. It printed:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=1, commitSync ACK, final checkpoint
```

This proves one source-locked topic/partition and one Oxia
assignment/Worker-apply/checkpoint/session cut. It does not prove catalog-driven multi-shard placement, session
reconnect/churn, Broker failover with an accepted Route, native eligibility,
production source ownership transfer, Object Store checkpoint publication or
release PASS.

### Kafka accepted Route broker failover cut

The accepted-Route failover cut is opt-in and intentionally standalone. Set
both `NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1` and
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY=1` together with
`NEREUS_DELAY_KAFKA_WITH_OXIA=1`. The route Worker pauses after the signed
Route/assignment acceptance; the harness stops `kafka-1`, waits for a
surviving broker, releases the gate, and requires the next record to reach the
same Worker Store, exact Kafka position check and `commitSync` ACK before the
final local checkpoint and Oxia assignment release.

The source-locked run used Delay
`7e94d0f8a3e374832a111dbd2f741be5f20795d5`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, Kafka/Oxia projects
`nereus-delay-kafka-e2e-1786787846-2966` /
`nereus-delay-kafka-oxia-e2e-1786787846-2966`, broker ports
`19750,19751,19752`, and Oxia port `16677`:

```bash
JAVA_TOOL_OPTIONS='-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' \
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1 \
NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16677 \
KAFKA_BROKER_1_PORT=19750 KAFKA_BROKER_2_PORT=19751 KAFKA_BROKER_3_PORT=19752 \
./e2e/run-kafka-real-client-e2e.sh
```

The bounded receipt was:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=2, commitSync ACK, accepted-route broker failover, final checkpoint
Kafka accepted-route broker failover E2E passed: Route-bound Worker applied and ACKed after broker-1 failover, then released its final checkpoint and Oxia assignment.
```

This is one source-locked topic/partition and is not the aggregate Kafka/K1/K2
E2E or a release PASS. Catalog-driven placement, Route session churn, native
eligibility, production source ownership, remote Object Store authority,
response-loss/crash cuts at every Store boundary, Pulsar multi-broker failover
and automatic Claim/Publish remain open.

### Kafka default full real-client revalidation

After the Route Worker harness change, the default path was rerun with
`NEREUS_DELAY_KAFKA_WITH_OXIA=1`; the two Route failover variables were not
set, so they remained `0`. The source-locked run used Delay branch HEAD
`52da04a3b14c56fcbe769f64836e1311e11956a7` (runtime slice
`7e94d0f8a3e374832a111dbd2f741be5f20795d5`), Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, Kafka/Oxia projects
`nereus-delay-kafka-e2e-1786788428-10652` /
`nereus-delay-kafka-oxia-e2e-1786788428-10652`, broker ports
`19763,19764,19765`, and Oxia port `16679`:

```bash
JAVA_TOOL_OPTIONS='-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' \
NEREUS_DELAY_KAFKA_WITH_OXIA=1 NEREUS_DELAY_KAFKA_OXIA_PORT=16679 \
KAFKA_BROKER_1_PORT=19763 KAFKA_BROKER_2_PORT=19764 KAFKA_BROKER_3_PORT=19765 \
./e2e/run-kafka-real-client-e2e.sh
```

The Route and aggregate receipts were:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=1, commitSync ACK, final checkpoint
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

This revalidates the default aggregate path; it is not a release PASS and does
not expand the bounded Route evidence into catalog placement, session churn,
native eligibility, production source ownership, remote Object Store
authority, response-loss/crash coverage, Pulsar multi-broker failover or
automatic Claim/Publish.

### Pulsar signed Route barrier to Worker assignment

When `NEREUS_DELAY_PULSAR_WITH_OXIA=1`, the same harness also runs
`runRealPulsarRouteWorkerSmoke`. It creates a native one-partition topic,
stamps the physical `-partition-0` guard through the P1 dedicated Resource
Controller endpoint, captures a guarded SUBSCRIBE position and stable
attestation, signs the Pulsar Route activation barrier, publishes the Route
event/head through session-fenced Oxia, publishes and rereads a route-bound
Worker assignment by revision CAS, recovers the pre-Route record into the
Worker Store before ACK, and hands the same guarded connection to the Worker.
The Worker applies and ACKs the post-barrier record, publishes a bounded local
RocksDB checkpoint, and releases the owner lease and assignment. The generic
topic-properties mutation remains fail-closed for the guard tuple.

The assignment-only receipt at Delay
`a73faf3e836ada67931f709d46214dde7caf3ad0` is historical provenance. The
current source-locked receipt uses Delay `bf858b089b927fcf65129214d8ed5a7fc5300deb`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` from
`8dae0236c0a0d405ed7f8303081080520fe91551`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`. It used P1 client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
distribution `373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`,
and base image
`eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`.
The command was:

```bash
JAVA_TOOL_OPTIONS='-Dorg.slf4j.simpleLogger.defaultLogLevel=warn' \
NEREUS_DELAY_PULSAR_WITH_OXIA=1 NEREUS_DELAY_PULSAR_OXIA_PORT=16674 \
PULSAR_BROKER_PORT=20020 PULSAR_WEB_PORT=20021 \
./e2e/run-pulsar-real-client-e2e.sh
```

The bounded Route receipt was:

```text
Pulsar signed Route -> guarded SUBSCRIBE barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: generation=15, barrier=20/0, routeRevision=1, assignmentRevision=1, source=20/1, ACK, final checkpoint
```

The same run ended with:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

This is one source-locked native partition and one Oxia
assignment/Worker-apply/checkpoint/session cut. It does not prove
catalog-driven multi-shard placement, session reconnect/churn, multi-broker
failover with an accepted Route, native eligibility, production source
ownership transfer, Object Store checkpoint publication or release PASS.

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
through `WorkerShardRuntime` and RocksDB `WriteBatch`, performs synchronous ACK,
runs the bounded final local RocksDB checkpoint, verifies its file inventory
and drains the exact owner lease.

```text
./e2e/run-pulsar-real-client-e2e.sh
```

The harness requires a clean
`nereus/delay-resource-guard-v1` checkout descended from
`8dae0236c0a0d405ed7f8303081080520fe91551`. It records the P1 source SHA,
distribution and client-jar SHA-256 values, image ID and allocated ports, and
cleans only its own Compose project, volumes, temporary image and staging
directories. The latest run passed with P1
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5a1d137d`, image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
Compose project `nereus-delay-pulsar-e2e-1786775596-63266`, ports `19916,19917`,
and writer output
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

The same harness runs `runRealPulsarMutationSmoke` after the source smoke. It
creates a guarded P1 Producer and a proof-bound source consumer, appends one
signed `TIME_FENCE` frame, replays it through the no-ACK recovery cursor, then
exposes and ACKs the mutation through the active guarded source. The appender
keeps an ambiguous or changed source proof as `UNKNOWN`; only a typed P1 guard
rejection with evidence is definitive non-persistence. The latest run used
Compose project `nereus-delay-pulsar-e2e-1786775596-63266`, ports `19916,19917`,
and printed:

```text
Pulsar Shard Log mutation append/replay/ACK smoke passed: physicalTopic=persistent://public/default/p1-mutation-63266-9fbffc7c-1863-4fdf-92df-9060f24b7538, ledger=15, entry=0, record=TIME_FENCE, guarded Producer, ordered mutation replay, ack receipt ACK
```

The standard Worker output was:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=18/0, active apply ledger/entry=18/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
```

The same run then used Worker `prepare` to persist one guarded record on the
named Pulsar volume, restarted the broker container, and launched a new Worker
JVM in `resume` mode against that same topic:

```text
Pulsar Worker restart preparation passed: one guarded record persisted before broker restart
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=20/0, active apply ledger/entry=27/0, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
```

This closes the single-node real P1 client/broker delete-recreate, guarded
source replay/ACK, Worker recovery/apply/ACK and one volume-backed broker
restart/resume cut. It does not claim unload, multi-broker failover, old-peer
proxy compatibility, live source ownership transfer, ACK response-loss,
guarded Fetch/rewind, network Oxia session/placement, D3 Direct SDK
integration or production multi-shard Worker wiring.

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

### Pulsar mutation-to-Store Worker vertical

The full harness now also runs `runRealPulsarMutationWorkerSmoke`. Two signed
`TIME_FENCE` records are appended through the guarded P1 Producer; strict
recovery applies ledger/entry `18/0`, the active Worker applies `18/1` through
guarded SUBSCRIBE and the shared RocksDB WriteBatch, then ACKs and publishes a
final checkpoint. The source-locked receipt used Delay mutation Worker commit
`016288b1`, P1 `358ce4a1033bd566faebcd3465c3ba4606f3c83f`, distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`, image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
and Compose project `nereus-delay-pulsar-e2e-1786780346-14394` on
`19930,19931`. It printed:

```text
Pulsar mutation Worker vertical smoke passed: recovery TIME_FENCE ledger/entry=18/0, active Store apply TIME_FENCE ledger/entry=18/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
```

The network-authority rerun used Pulsar/Oxia projects
`nereus-delay-pulsar-e2e-1786781139-23243` /
`nereus-delay-pulsar-oxia-e2e-1786781139-23243` on
`19950,19951` / `16670`, and printed:

```text
Pulsar mutation Worker assignment publication/acceptance passed: revision=1, worker=pulsar-mutation-worker, authority=real Oxia session-bound
Pulsar mutation Worker vertical smoke passed: recovery TIME_FENCE ledger/entry=18/0, active Store apply TIME_FENCE ledger/entry=18/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar mutation Worker authority smoke passed: real Oxia session-bound lease
```

Both runs exited successfully; the Oxia run also completed the existing source,
Worker and broker-restart/resume cuts. This closes the bounded Pulsar
mutation-to-Store Worker plus network owner-session cut, not multi-broker
failover, response-loss or crash recovery, catalog placement, Route source
ownership or release PASS.

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

## Oxia Route provider restart/revalidation

The restart-only mode validates a live Route cache against a real Oxia service.
The test publishes and caches one signed Route, pauses at a file gate, and the
harness stops/starts the same Oxia container. After the health check passes,
the gate is released and explicit provider `refresh()` must recover the
session-backed read path, replay the persisted head/event and retain the exact
signed snapshot.

Run it with:

```bash
NEREUS_DELAY_OXIA_ROUTE_RESTART=1 \
NEREUS_DELAY_OXIA_ROUTE_RESTART_ONLY=1 \
NEREUS_DELAY_OXIA_E2E_PORT=16684 \
./e2e/run-oxia-real-service.sh
```

The source-locked run used Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786789198-22565`, host port `16684`, and image
`sha256:1ea8324636e65d92bf6f0767062e58078fd617767c9c74540443c5b6a2c1293d`.
It passed with:

```text
Oxia Route provider restart recovery passed: revision=1, session revalidated, cache healthy
Dockerized Oxia Route restart smoke passed: provider session recovery and signed Route cache rebuild
```

This covers one real Oxia restart/revalidation cut. The session identity was
reusable across this stop/start; marker expiry/rotation, notification-stream
churn, multi-node failover, catalog placement, remote Object Store authority
and release PASS remain outside the receipt.

## Oxia Route notification recovery after session rotation

The notification-recovery mode exercises the stricter restart cut. It uses
two-second session timeouts and pauses the stopped Oxia container for five
seconds so the publisher and provider markers expire. After the service is
healthy again, the test reconnects the publisher and calls provider `refresh()`
once. That refresh replaces the separate watch client and registers the same
callback on a new offset-tracked notification stream. The test then publishes
revision 2 without another provider refresh; receiving the retired snapshot
proves the recovered stream, while the changed session identity proves marker
rotation.

Run it with:

```bash
NEREUS_DELAY_OXIA_ROUTE_RESTART=1 \
NEREUS_DELAY_OXIA_ROUTE_RESTART_ONLY=1 \
NEREUS_DELAY_OXIA_ROUTE_RESTART_NOTIFICATIONS=1 \
NEREUS_DELAY_OXIA_ROUTE_RESTART_PAUSE_SECONDS=5 \
NEREUS_DELAY_OXIA_E2E_PORT=16675 \
./e2e/run-oxia-real-service.sh
```

The source-locked run used Delay commit `6a64ca894928a9a6f210129e2567b02f7df1329f`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:05d66cf3117d24b358baee21fb87caa001c99bec2f734ea9ce2549f7675d085a`,
Compose project `nereus-delay-v1-oxia-e2e-1786822655-96457`, and port `16675`.
It printed:

```text
Oxia Route notification restart recovery passed: revision=2, session rotated, notification stream resumed without a second provider refresh
Dockerized Oxia Route notification restart smoke passed: session rotation and notification stream recovery
```

This closes only the bounded single-node restart/session-rotation and
notification-resume cut. Multi-node Oxia failover, partial placement, catalog
authority, native eligibility, live Broker transport, crash/response-loss and
release PASS remain open.

## Worker final checkpoint-on-drain evidence

Delay commit `2dd2cfff83f4d029972cf7fbeb569fbf4538c026` makes the real Kafka and
Pulsar Worker smokes pass an exact checkpoint identity to the drain coordinator.
The smoke executes the bounded `CHECKPOINT` work class, requires a non-empty
`CheckpointFileInventory`, and verifies that the exact owner lease is empty
only after checkpoint creation and Store close.

The fresh default runs used Kafka Compose project
`nereus-delay-kafka-e2e-1786765675-51303` on ports `19440,19441,19442` and
Pulsar Compose project `nereus-delay-pulsar-e2e-1786765675-51304` on ports
`19990,19991`. Their Worker lines were:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=15/0, active apply ledger/entry=15/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
```

Both scripts exited successfully and removed their matching containers,
networks and volumes. This proves only bounded local checkpoint/inventory and
checkpoint-before-lease-release ordering in these two source-locked smokes; it
does not prove object-store checkpoint publication, due/Lane/publish
orchestration, crash recovery, multi-broker failover or production multi-shard
Worker wiring.

The same checkpoint code can be run with the network authority enabled. The
fresh Kafka/Oxia run used projects
`nereus-delay-kafka-e2e-1786766242-57688` /
`nereus-delay-kafka-oxia-e2e-1786766242-57688` on
`19450,19451,19452` / `16661`; the fresh Pulsar/Oxia run used projects
`nereus-delay-pulsar-e2e-1786766242-57687` /
`nereus-delay-pulsar-oxia-e2e-1786766242-57687` on `20000,20001` / `16662`.
Both printed `final checkpoint` and `real Oxia session-bound lease`, exited
successfully and removed matching containers, networks and volumes. The
optional Pulsar run emitted multiple-provider SLF4J warnings from its combined
runtime classpath. This mode proves checkpoint-before-release with network
owner authority only; it does not prove object-store publication, due/Lane/
publish orchestration, crash recovery, failover or production multi-shard
Worker wiring.

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

./gradlew runRealKafkaMutationSmoke \
  -PkafkaClientJar=/absolute/path/to/kafka-clients-4.4.0-SNAPSHOT.jar \
  -PkafkaBootstrap=127.0.0.1:9092 \
  -PkafkaMutationTopic=nereus-delay-mutation-topic

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

## Gateway real mTLS/RS256 and Oxia durability E2E

Run the Gateway network receipt with an isolated Oxia port and Gateway port:

```bash
NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT=16668 \
NEREUS_DELAY_GATEWAY_PORT=22350 \
./e2e/run-gateway-real-e2e.sh
```

The checked-in harness creates an ephemeral test CA/server/client
certificate, starts `GatewayGrpcServer.mutualTls`, and calls the generated
Schedule RPC through a real Netty gRPC channel. The RS256 JWT is bound to the
client certificate through `cnf.x5t#S256`; a mutated signature must be
rejected before preparation. Two identical authenticated requests must leave
one prepared-byte/idempotency attempt, a released Oxia admission lease and
two deduplicated digest-only audit records. The source-locked receipt used
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786820415-72294`, Oxia port `16668` and Gateway
port `22350`.

This is a bounded Gateway/authentication and durable-record receipt. The test
uses deterministic Semantic-Core/submission doubles and a local definite
non-submission outcome; live Kafka/Pulsar publish, certificate
deployment/rotation, admission HA, load, crash cuts and release gates remain
open.

The current revalidation is commit `232ce29d`: after the first authenticated
request the harness restarts `GatewayGrpcServer` on the same port and sends a
new mTLS request. It requires the exact durable outcome without a second
preparation or submission attempt. The accepted run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786820937-77983`, Oxia port `16669` and Gateway
port `22351`, and printed:

```text
Gateway restart/idempotency E2E passed: server restarted and returned the exact durable outcome without a second attempt
```

The current harness also races two independent Gateway compositions, backed
by separate Oxia client sessions, against the same durable idempotency key.
The accepted run used ports `22353,22354` and required one durable physical
attempt:

```text
Gateway two-server CAS race E2E passed: independent Gateway servers converged on one durable physical attempt
```

## Gateway certificate replacement and channel revalidation

The Gateway harness also exercises a bounded deployment replacement. It
generates independent old and rotated CA/server/client sets. The old server
persists the first request. The replacement server uses the rotated server
certificate and trusts only the rotated CA; an old client trusts the rotated
server but presents the old client certificate and must fail at mTLS. A new
client certificate and JWT with matching `cnf.x5t#S256` must receive the exact
durable outcome without another preparation or physical attempt.

The source-locked run used:

```bash
NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT=16677 \
NEREUS_DELAY_GATEWAY_PORT=22356 \
./e2e/run-gateway-real-e2e.sh
```

It used Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:054bb7d13cd9c3d7a6c4dd0b70d5820b6ece2115e840cf47ff8ea0e679a9248c`,
Compose project `nereus-delay-gateway-e2e-1786823102-1813`, and printed:

```text
Gateway certificate rotation E2E passed: old mTLS client rejected and new certificate reread the exact durable outcome
```

This closes only bounded same-port certificate replacement and authenticated
channel revalidation. Hot reload, staged rollback, revocation/CRL or OCSP,
multi-process Gateway HA, load, crash/response-loss and release PASS remain
open.

## Gateway durable admission/idempotency recovery after Oxia session churn

Run the gated stop/start receipt with a short old session and a five-second
Oxia outage:

```bash
NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT=16678 \
NEREUS_DELAY_GATEWAY_PORT=22357 \
NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN=1 \
NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_PAUSE_SECONDS=5 \
./e2e/run-gateway-real-e2e.sh
```

The implementation commit is `f9fa48b7`; the gated test/harness commit is
`241068fd`. The old Gateway composition uses two-second Oxia sessions and
remains live while the harness stops Oxia. Session-bound admission and
idempotency I/O fail closed, and the old mTLS request receives `UNAVAILABLE`.
After Oxia restarts, a new three-handle composition rereads the same durable
prefix and returns the exact prior outcome without another preparation or
physical attempt. The scans require one quiescent idempotency attempt, zero
admission leases and two digest-only audit records.

The accepted source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:e36ced8f25cff4ea67e61a1dd392668d53b5ac79ffe992587d49548cf038a059`,
Compose project `nereus-delay-gateway-e2e-1786824181-13578`, and printed:

```text
Gateway Oxia session churn E2E passed: stale durable sessions failed closed and recovery reread the exact durable outcome
Dockerized Gateway Oxia session churn smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

This closes only the bounded single-node Oxia session-rotation,
fail-closed/recomposition cut. Transparent automatic reconnect, multi-node
Oxia failover, production Gateway HA, crash/response-loss, load, live
Kafka/Pulsar publication and release PASS remain open.

## Gateway recovery across a real multi-node Oxia DataServer leader stop

Run the independent three-Coordinator/three-DataServer receipt:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/absolute/path/to/oxia \
NEREUS_DELAY_OXIA_COORDINATOR_1_PORT=16691 \
NEREUS_DELAY_OXIA_COORDINATOR_2_PORT=16692 \
NEREUS_DELAY_OXIA_COORDINATOR_3_PORT=16693 \
NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT=16681 \
NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT=16682 \
NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT=16683 \
NEREUS_DELAY_GATEWAY_PORT=22358 \
./e2e/run-oxia-multi-node-gateway-e2e.sh
```

The harness builds the source-locked Oxia checkout, starts a three-node Raft
Coordinator set and three DataServers, registers a three-replica `default`
namespace through the admin API, and discovers the actual shard leader. It
starts the Gateway test against a surviving DataServer, then stops the leader
while the same Gateway process and all three session-bound durable wrappers
remain live. The old handles must preserve their session markers; the second
authenticated request must return the exact prior outcome without a second
preparation or physical attempt. Final scans require one quiescent
idempotency attempt, zero admission leases and two audit records.

The accepted run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Delay commit
`43493a709e4041e94c7f4f270a25b2725534ab59`, Compose project
`nereus-delay-oxia-cluster-gateway-e2e-1786825431-27266`, host ports
`16691,16692,16693` and `16681,16682,16683`, and Gateway port `22358`:

```text
Oxia shard successor leader: ds-1
Gateway multi-node Oxia failover E2E passed: session-bound clients preserved the exact durable outcome after the shard leader stopped
Oxia multi-node Gateway failover E2E passed: session-bound Gateway reread the exact durable outcome after leader stop
Dockerized Oxia multi-node Gateway failover smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

This is a bounded real multi-node DataServer leader-stop/session-preserving
cut. It does not prove total-outage automatic reconnect, partial placement or
quorum-loss behavior, production Gateway HA, crash/response-loss resolution,
load, live Kafka/Pulsar publication or release PASS.

## Gateway STARTED CAS response-loss recovery

The durable Gateway state machine also covers the cut where the Oxia CAS
response is lost after the `STARTED` attempt has already committed. The
production stores never recreate the one-shot physical-attempt permit from a
reread. A caller before `uncertaintyAtEpochMs` observes the active attempt; a
same-key caller at or after that trusted deadline CASes the exact persisted
attempt to `UNCERTAIN`/`QUIESCENT` using the original prepared bytes. A failed
recovery CAS is reread and cannot authorize a duplicate physical send.

Run the deterministic regression with:

```bash
GRADLE_USER_HOME=/tmp/nereus-delay-gateway-started-recovery-gradle \
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --no-daemon --console=plain
```

The accepted source tree uses commits `a120b6bd` and `7adb95f0`; the tests are
`attemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit` and
`retryAttemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit`.
This is local committed-then-lost CAS evidence; it is not a real Oxia network
fault injection, physical Kafka/Pulsar response-loss receipt, transparent
Gateway reconnect, HA/load evidence or release PASS.

## Gateway STARTED CAS response-loss recovery against real Oxia

Run the Gateway class against a source-locked real Oxia service:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/absolute/path/to/oxia \
NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT=16695 \
NEREUS_DELAY_GATEWAY_PORT=22360 \
NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME=/tmp/nereus-delay-gateway-response-loss-gradle \
./e2e/run-gateway-real-e2e.sh
```

Test commit `1ce8b7e604ca969adabd7372e80ce04f96e5b45a` adds
`gatewayRecoversAfterCommittedOxiaAttemptResponseLoss`. The test uses the
real mTLS/RS256 Gateway and Oxia record clients, then throws from a test-only
wrapper after the real `STARTED` CAS has committed. The first request returns
managed `ENQUEUE_UNCERTAIN`; after the exact trusted deadline, the repeated
request returns byte-identical output, with one preparation and zero physical
submissions. Final scans require one quiescent idempotency record with one
`UNCERTAIN` attempt and an aggregate, zero admission leases and four audit
records.

The accepted source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:db1b0409c36cbf16bc21d63f74932a0f9f188f5d0101b9b398e6b90de3e01cc`,
Compose project `nereus-delay-gateway-e2e-1786827281-47103`, Oxia port
`16695` and Gateway port `22360`. The six-test class ended with
`BUILD SUCCESSFUL in 1m 14s` and two opt-in tests skipped, and emitted:

```text
Gateway Oxia STARTED response-loss E2E passed: committed attempt was reread after deadline as exact UNCERTAIN without a second physical submission
```

This is real-Oxia durable post-commit recovery with controlled client-side
response loss. It is not raw socket fault injection or physical Kafka/Pulsar
response-loss/crash evidence; transparent Gateway reconnect/HA, load,
multi-shard placement and release PASS remain open.

## Gateway RETRY_UNCERTAIN response-loss recovery against real Oxia

Run the explicit-retry receipt against a source-locked real Oxia service:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/absolute/path/to/oxia \
NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT=16697 \
NEREUS_DELAY_GATEWAY_PORT=22362 \
NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME=/tmp/nereus-delay-gateway-retry-loss-real-gradle \
./e2e/run-gateway-real-e2e.sh
```

Commit `bcac733ae7e48776ce7d427d66643d21a6dd2a7d` clears the previous
uncertain aggregate whenever a new explicit retry becomes `ACTIVE`. The real
`gatewayRecoversAfterCommittedOxiaRetryAttemptResponseLoss` test loses the
response after Oxia commits both the initial and retry `STARTED` CAS writes.
It recovers the initial attempt, issues `RetryUncertain` with the exact prior
attempt ID, and checks byte-identical retry responses before and after the
deadline, one Semantic preparation and zero physical submissions. Final scans
require one quiescent idempotency record with two `UNCERTAIN` attempts, an
aggregate, zero admission leases and eight audit records.

The accepted source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:ba41122a1fa21cdcfb1c2680e81a3f14519d6f1f9213f82dfa284fb3e792428d`,
Compose project `nereus-delay-gateway-e2e-1786828250-57299`, Oxia port
`16697` and Gateway port `22362`. The seven-test class ended with
`BUILD SUCCESSFUL in 16s` and two opt-in tests skipped, and emitted:

```text
Gateway Oxia RETRY_UNCERTAIN response-loss E2E passed: committed retry attempt was reread after deadline as exact UNCERTAIN without a second physical submission
```

This is real-Oxia durable explicit-retry recovery with controlled client-side
post-commit response loss. It is not raw socket fault injection or physical
Kafka/Pulsar response-loss/crash evidence; transparent Gateway reconnect/HA,
load, multi-shard placement and release PASS remain open.

## Pulsar committed SEND response-loss receipt

Run the dedicated source-bound P1 destination cut with:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-p1-response-loss-real-gradle \
NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_ONLY=1 \
PULSAR_BROKER_PORT=21885 PULSAR_WEB_PORT=21886 \
./e2e/run-pulsar-real-client-e2e.sh
```

The run is locked to Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose project `nereus-delay-pulsar-e2e-1786829967-75545`, ports `21885` and
`21886`, and Delay implementation commit `12334f63`.

It printed:

```text
Pulsar committed response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved PUBLISHED
Pulsar destination committed response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and exact guarded payload readback.
```

The implementation uses an optional source-bound provider to accept only exact
typed `PULSAR_SEND_ACK` evidence after the guarded SEND completion becomes
uncertain. The test proxy loses the local completion after the real Broker has
persisted the message, and the smoke confirms exact guarded payload readback.
The run is controlled client-side response loss, not raw socket fault
injection. It does not cover `PulsarAttemptJournal` durability, in-flight
process/Broker crash, multi-Broker failover, generic transport response-loss
or V1 release PASS.

## Pulsar Worker source ACK response-loss receipt

Run the dedicated bounded Worker source-ACK cut with:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-p1-response-loss-real-gradle \
NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_ONLY=1 \
PULSAR_BROKER_PORT=21887 PULSAR_WEB_PORT=21888 \
./e2e/run-pulsar-real-client-e2e.sh
```

The source-locked run used Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose project `nereus-delay-pulsar-e2e-1786830626-82754`, ports `21887` and
`21888`, and Delay commit `31145cc8`.

It printed:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker source ACK response-loss smoke passed: real ACK was accepted before the local response was discarded, and the same source record was ACKed on the next bounded Worker turn
Pulsar Worker source ACK response-loss E2E passed: real ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

The wrapper loses only the local response after the Broker receipt; the
pending source record remains the retry authority and the already applied
outcome is reused. This is not raw network loss, process/consumer/Broker crash
recovery, multi-Broker failover or a complete D6/V1 release receipt.

## Pulsar Worker source-applied destination response-loss receipt

Run the Worker physical-publish cut with:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-p1-response-loss-real-gradle \
NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_ONLY=1 \
PULSAR_BROKER_PORT=21889 PULSAR_WEB_PORT=21890 \
./e2e/run-pulsar-real-client-e2e.sh
```

The source-locked run used Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose project `nereus-delay-pulsar-e2e-1786830983-86815`, ports `21889` and
`21890`, and Delay commit `c903fe34`.

It printed:

```text
Pulsar Worker destination response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved the source-applied PUBLISHED Outcome
Pulsar Worker source-applied physical publish passed: Admission source ledger=9/3, typed PULSAR_SEND_ACK target ledger/entry=10/0, Outcome source ledger=9/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker destination response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and the source-applied Outcome completed.
```

The test-only wrapper loses the local completion after the real destination
SEND, while the Worker bridge validates exact typed evidence before appending
the source-applied Outcome. This is not raw network loss, process/Broker crash
between physical persistence and Outcome, multi-Broker failover or a complete
D6/V1 release receipt.

## Kafka Worker destination response-loss receipt

Run the focused Worker physical-publish cut with:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-gradle \
NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS=1 \
NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS_ONLY=1 \
KAFKA_BROKER_1_PORT=19669 KAFKA_BROKER_2_PORT=19670 KAFKA_BROKER_3_PORT=19671 \
./e2e/run-kafka-real-client-e2e.sh
```

The source-locked run used Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786831579-93599`, ports `19669,19670,19671`,
and Delay commit `e95d1c0cbaf4b94c8523d6fd9994b6487102f400`.

It printed:

```text
Kafka Worker destination response-loss smoke passed: real EndTxn committed the exact target-plus-receipt pair, the local response was discarded, and typed read_committed KAFKA_TRANSACTIONAL_RECEIPT evidence resolved the source-applied PUBLISHED Outcome
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, and final checkpoint
Kafka Worker destination response-loss E2E passed: real EndTxn response loss resolved through typed read_committed KAFKA_TRANSACTIONAL_RECEIPT evidence and the source-applied Outcome completed.
```

The test-only proxy loses the local response after real transaction commit;
the typed receipt provider and source-applied Outcome validate the exact
publication. This is controlled client-side response loss, not raw network
loss, process/Broker crash recovery, multi-Broker failover or a V1 release
receipt.

## Kafka Worker source ACK response-loss receipt

Run the focused Worker source-ACK cut with:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-gradle \
NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS=1 \
NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS_ONLY=1 \
KAFKA_BROKER_1_PORT=19679 KAFKA_BROKER_2_PORT=19680 KAFKA_BROKER_3_PORT=19681 \
./e2e/run-kafka-real-client-e2e.sh
```

The source-locked run used Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786832218-928`, ports `19679,19680,19681`,
and Delay commit `d165e73e457834be55af58d238980be65c2054c7`.

It printed:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker source ACK response-loss smoke passed: real commitSync ACK was accepted before the local response was discarded, and the same source record was ACKed on the next bounded Worker turn
Kafka Worker source ACK response-loss E2E passed: real commitSync ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

The test-only proxy loses the local response after the real `commitSync`
returns. The source adapter retains the same in-flight record and the Worker
retries its pending ACK without reapplying the Store mutation. This is
controlled client-side response loss, not raw network loss, process/consumer/
Broker crash recovery, multi-Broker failover or a V1 release receipt.

## S3-compatible checkpoint adapter focused receipt

The local adapter test exercises the provider-shaped checkpoint boundary without
claiming a real cloud or MinIO deployment:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

Delay commit `e01d3ee8708a53487747b0ef721d1f0d107ff677` adds
`S3CompatibleCheckpointObjectStoreAdapter`. The test fixture is a raw local
HTTP server and verifies Profile endpoint/credential-scope drift rejection,
SigV4 and `If-None-Match: *` headers, deterministic object identity,
manifest-last upload, manifest PUT response loss followed by exact reread,
bounded full restore and same-key immutable conflict rejection.

The focused test passed with `BUILD SUCCESSFUL`. This is local adapter evidence,
not real S3/MinIO conformance, credential-use lease or rotation evidence,
provider quiescence/consistency attestation, version-aware deletion,
multi-shard RecoveryPin/catalog authority, process/network chaos or V1 release
PASS.

## Object Store credential-use lease gate focused receipt

The local gate and its adapter wiring are covered by:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.ObjectStoreCredentialUseLeaseGateTest \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

Delay commit `078c66ce141a17a3e757aabb88bae5140d1d297a` adds
`ObjectStoreCredentialUseLeaseGate` and the lease-gated S3-compatible adapter
constructor. Each upload/download rechecks the exact binding/protection/lease
identity, configured TTL and attestation age, current local trusted time and
loaded credential fingerprint before HTTP. The focused tests passed with
`BUILD SUCCESSFUL`, and the full Gradle check passed. Existing ungated adapter
constructors remain a local provider-shaped seam, not credential-authority
evidence. Oxia Head/protection CAS, trust-set/secret authority, rotation, real
S3/MinIO, provider quiescence, deletion, chaos and release gates remain open.

## Object Store authority-to-adapter activation focused receipt

The activation composition is covered by:

```bash
./gradlew test \
  --tests io.nereusstream.delay.runtime.OxiaObjectStoreCredentialLeaseActivatorTest \
  --tests io.nereusstream.delay.runtime.OxiaSyncProfileCatalogBackendTest \
  --no-daemon --console=plain
```

Delay commit `138c1c0e5e0e9af9c3b8e93b223da5b3e322a6bb` adds
`CredentialProfileAuthority` and `OxiaObjectStoreCredentialLeaseActivator`.
The focused tests passed with `BUILD SUCCESSFUL`: exact activation resolves
one Profile/Head/Binding, fingerprint drift is rejected before lease issuance,
and a lease whose returned Protection revision is not proven by the reread is
rejected. This is activation-time composition evidence only; no real secret
manager, trust-set/actor authorization, automatic renewal, provider
rotation/quiescence, real S3/MinIO, deletion, chaos or release gate is closed.

## Oxia credential Profile Head/Protection/lease authority receipt

Run the Dockerized real-service authority slice with:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-profile-gradle \
NEREUS_DELAY_OXIA_E2E_PORT=16693 \
./e2e/run-oxia-real-service.sh
```

The source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786835835-39861`, and host port `16693`. The
script includes
`OxiaRealProfileCatalogSmokeTest.profileHeadProtectionLeaseAndRotationReopenAgainstRealService`;
its report recorded one test, zero skips, zero failures and zero errors. The
run ended with `BUILD SUCCESSFUL` and
`Dockerized Oxia real-service smoke passed for 37a17bef17202d5fd6e23282da5fd26d94865484`.

The receipt proves one Profile's generation-1 publication, exact Head-bound
Protection/lease issuance, reopen and checked generation rotation through
single-record Oxia CAS. It is not secret-provider resolution, attestation
trust-set or actor authorization, source ordering, retained-generation GC,
cross-record Owner/Route/session transaction, multi-node failover for this
authority, provider credential rotation/quiescence, real S3/MinIO or V1
release evidence.

## Credential attestation trust-set focused receipt

The deterministic trust-set and Profile-authority regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.runtime.CredentialAttestationTrustSetTest \
  --tests io.nereusstream.delay.runtime.OxiaSyncProfileCatalogBackendTest \
  --no-daemon --console=plain
```

Delay commit `f758d010b4d75f9c53d1f6e2cf01d573d655fd1c` adds the immutable
`CredentialAttestationTrustSet`. The focused tests passed with `BUILD
SUCCESSFUL`; they cover exact verifier tuple/signature/window acceptance,
unknown verifier rejection, out-of-window rejection, and Profile CAS
publication/rotation/reopen/lease integration. The full Gradle check passed.

The real-service receipt used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786837306-55484`, host port `16694`, and
`OxiaRealProfileCatalogSmokeTest.profileHeadProtectionLeaseAndRotationReopenAgainstRealService`.
The report recorded one test, zero skips, zero failures and zero errors; the
run ended with `BUILD SUCCESSFUL` and
`Dockerized Oxia real-service smoke passed for 37a17bef17202d5fd6e23282da5fd26d94865484`.

This is local trust-set/signature/window evidence integrated with one
single-record Oxia authority. It does not prove source-ordered trust-set
publication/rotation, actor authorization, secret-manager resolution,
automatic renewal, multi-node authority failover, provider rotation/
quiescence, real S3/MinIO, chaos or release readiness.

## Same-generation Object Store lease renewal focused receipt

The local renewal and gate-replacement regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapterTest \
  --tests io.nereusstream.delay.store.ObjectStoreCredentialUseLeaseGateTest \
  --no-daemon --console=plain
```

Delay commit `8307d690351af1699a6a9cb69e2cfe9bfe26a4a2` adds
`RenewableS3CompatibleCheckpointObjectStoreAdapter`. The focused tests passed
with `BUILD SUCCESSFUL`; they cover no authority read outside the renewal
window, protected lease/protection revision advancement inside the window and
Head rotation rejection before Provider I/O. The full Gradle check passed.

This is bounded same-generation, single-process renewal evidence. It does not
prove scheduled multi-process renewal ownership, source-ordered rotation or
provider quiescence, secret-manager resolution, multi-node Profile authority
failover, real S3/MinIO, deletion, chaos or release readiness.

## Verified credential material cache focused receipt

The local cache-boundary regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.VerifiedCredentialMaterialCacheTest \
  --tests io.nereusstream.delay.runtime.CredentialAttestationTrustSetTest \
  --no-daemon --console=plain
```

Delay commit `d9b713a9159a8b2672a2b0aea5bd5243ca798c3e` adds
`VerifiedCredentialMaterialCache`. The focused tests passed with `BUILD
SUCCESSFUL`; they cover exact Profile/generation/binding/reference/fingerprint
lookup, generation miss, fingerprint drift, foreign attestation rejection and
atomic failed replacement. The full Gradle check passed.

This is local verified-cache evidence only. It does not prove an external
secret-manager reader, source-ordered cache refresh/publication, actor
authorization, multi-node authority failover, credential rotation/quiescence,
real S3/MinIO, chaos or release readiness.

## MinIO S3-compatible checkpoint real-service receipt

Run the locked, opt-in MinIO provider smoke from the Delay checkout with:

```text
./e2e/run-minio-real-e2e.sh
```

The harness uses the locally available
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` image at repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
It starts one isolated container on a dynamic host port, creates a generated
bucket with curl AWS SigV4, runs
`S3CompatibleMinioRealSmokeTest` with Gradle `--rerun-tasks`, and cleans only
the matching container. `NEREUS_DELAY_MINIO_E2E_PORT` can pin a host port;
access key, secret key, region and bucket can be overridden with the
`NEREUS_DELAY_MINIO_*` variables.

The source-locked receipt used Delay commit `31ba5661`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`,
container `nereus-delay-minio-e2e-1786839150-77162`, endpoint
`http://127.0.0.1:62159` and bucket
`nereus-delay-checkpoints-1786839150-77162`. The JUnit report recorded
`tests=1 skipped=0 failures=0 errors=0`; the run ended with `BUILD SUCCESSFUL`
and:

```text
Dockerized MinIO S3-compatible checkpoint smoke passed for quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e
```

The test proves real MinIO upload of immutable checkpoint objects and the
manifest, same-key idempotent retry, and download restore of both files. It is
one-provider bounded evidence, not generic S3 compatibility, credential
authority/renewal/rotation, provider deletion/versioning, consistency
attestation, network/process chaos, multi-node failover or V1 release
evidence.

## Exact provider-version MinIO receipt

The MinIO harness enables bucket versioning before it creates the checkpoint.
The adapter now requires `x-amz-version-id` for every successful or reread
object response because the frozen Object Store Profile requires exact
version deletion. The local negative test
`S3CompatibleCheckpointObjectStoreAdapterTest.rejectsProviderThatOmitsExactVersionHeaders`
proves that a provider without this header fails closed instead of producing a
`sha256-*` production identity.

The source-locked run used Delay implementation `b971cd3f` plus test receipt
`2981a269`, container `nereus-delay-minio-e2e-1786840003-88209`, endpoint
`http://127.0.0.1:64830`, bucket
`nereus-delay-checkpoints-1786840003-88209`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and image digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The JUnit report recorded `tests=1 skipped=0 failures=0 errors=0` and
`MinIO checkpoint manifest provider version=780f1e1f-c7da-4dc1-ae4e-a7b9be4f801c`;
the run ended with `BUILD SUCCESSFUL`.

This receipt proves only exact provider-version response identity for the
locked MinIO path. Full version-aware checkpoint deletion, source-ordered
retire/delete authority, Recovery Floor/Pin release, provider consistency,
credential rotation, chaos, failover and V1 release evidence remain open.

## Catalog-bound manifest version readback receipt

The adapter's download path now requests the manifest with the exact
catalog-bound `versionId` and signs that query through SigV4. The fake-provider
regression rejects a different query version, and the real MinIO run used
Delay commit `d7f51441`, container `nereus-delay-minio-e2e-1786840389-93104`,
endpoint `http://127.0.0.1:49401`, bucket
`nereus-delay-checkpoints-1786840389-93104`, and the locked image digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The JUnit report recorded `tests=1 skipped=0 failures=0 errors=0` and
`MinIO checkpoint manifest provider version=ac201fe8-ba70-4bcb-a49c-a75a6657be55`;
the run ended with `BUILD SUCCESSFUL`.

This is manifest readback evidence only. File-object version capture,
complete version-aware checkpoint deletion, source-ordered retire/delete
authority, Recovery Floor/Pin release, provider consistency, credential
rotation, chaos, failover and V1 release evidence remain open.

## Exact checkpoint object-set deletion receipt

`CheckpointDeleteAdapter` is the direct provider boundary for deleting one
catalog-bound checkpoint. The S3-compatible implementation preflights the
exact manifest/resource identity and every deterministic file object's
length/SHA-256, captures the provider version for each object, deletes the
files by signed `versionId` and deletes the catalog-bound manifest version
last. It requires a matching `x-amz-version-id` and nonblank
`x-amz-request-id` on every successful DELETE, and returns aggregate request
and response hashes only after the complete set succeeds.

The focused regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

The locked real-provider run was:

```text
./e2e/run-minio-real-e2e.sh
```

It used container `nereus-delay-minio-e2e-1786841029-825`, endpoint
`http://127.0.0.1:51386`, bucket
`nereus-delay-checkpoints-1786841029-825`, MinIO image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The JUnit report recorded `tests=1 skipped=0 failures=0 errors=0`, system-out
recorded manifest provider version
`e223584d-2863-45a1-8471-9b378c0899c5`, and the harness ended with
`BUILD SUCCESSFUL` after the post-delete restore read failed as expected.

This is one locked MinIO provider's direct deletion evidence. It does not
close `ALREADY_ABSENT` partial-reconciliation, final prefix sweeps,
retire/Floor/Pin authorization, provider consistency/quiescence, credential
rotation, generic provider compatibility, chaos, failover or release gates.

## Checkpoint delete retry-convergence receipt

Delete probes now retain provider request-ID/response evidence for exact
manifest and file GETs. A retry after a partial response loss deletes only
the remaining verified file versions and the manifest exact version last; a
retry after the complete object set is absent returns `ALREADY_ABSENT` with
the absence-probe aggregates. A manifest-absent/file-present mixed state is
rejected.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

The source-locked implementation is Delay commit `220fc98a`. The MinIO
rerun used container `nereus-delay-minio-e2e-1786841861-10565`, endpoint
`http://127.0.0.1:54320`, bucket
`nereus-delay-checkpoints-1786841861-10565`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `1a81631c-3bd9-41e6-a132-8abe1da7ea2e`, and the
harness ended with `BUILD SUCCESSFUL`.

This closes direct delete retry convergence for the locked provider seam. It
does not close final prefix sweeping, source-ordered retire/Floor/Pin
authorization, provider consistency/quiescence, credential rotation,
generic provider compatibility, chaos, failover or release gates.

## Checkpoint prefix sweep receipt

The provider seam now exposes a bounded exact checkpoint-prefix sweep. It
lists one non-truncated `ListObjectVersions` page for the derived
`checkpoints/<lineage>/<checkpoint>/` prefix, rejects malformed or escaped
entries, deletes every listed version with the exact version ID and performs
a final list that must be empty. The caller must already hold the exact
Object Store Profile and checkpoint identity; this receipt does not stand in
for external REAPING, retire, Recovery Floor/Pin or Owner authorization.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

The source-locked implementation is Delay commit
`c32a98f328400c71346b98188930a6efa80da7c9`. The locked MinIO rerun used
container `nereus-delay-minio-e2e-1786842572-18888`, endpoint
`http://127.0.0.1:56466`, bucket
`nereus-delay-checkpoints-1786842572-18888`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `f905db1e-1a7e-455c-bb32-5fa90bb7ed1f`, and the
harness ended with `BUILD SUCCESSFUL` after final empty-prefix proof.

This closes direct prefix-sweep evidence for one locked provider only. It
does not close external lifecycle authorization, multi-page policy, provider
consistency/quiescence, credential rotation, generic provider compatibility,
chaos, failover or release gates.

## Checkpoint REAPING sweep coordination receipt

`CheckpointReapingSweepCoordinator` first wins the exact
`PENDING_UPLOAD -> REAPING` intent CAS, rereads the REAPING successor, and
only then invokes the bounded exact-prefix provider sweep. If the provider
response is lost, the intent remains REAPING and the same pending identity
and trusted evidence can retry; a catalog/pin protection decision prevents
provider I/O. This is not proof of old-Owner abandonment, session loss,
provider quiescence or external delete-confirmed mutation.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.CheckpointReapingSweepCoordinatorTest \
  --no-daemon --console=plain
```

The source-locked implementation is Delay commit
`b9fcd2aa846329ed13986b122d287375a441b2fd`. The locked MinIO rerun used
container `nereus-delay-minio-e2e-1786843326-27711`, endpoint
`http://127.0.0.1:58388`, bucket
`nereus-delay-checkpoints-1786843326-27711`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `f5404da4-4944-4581-a75d-80dccdad92c3`, and the
harness ended with `BUILD SUCCESSFUL` after the coordinator-driven final
empty-prefix proof.

This closes only the bounded REAPING-to-provider composition for the locked
seam. External lifecycle authorization, provider ownership/quiescence,
source-ordered retire/delete confirmation, Floor/Pin/Owner transactions,
provider breadth, chaos, failover and release gates remain open.

## Checkpoint REAPING quiescence proof receipt

The coordinator now requires an immutable quiescence proof before its
provider call. The proof binds the pending intent and reaping evidence,
checks `requestQuiescenceHorizon >= maximumProviderOwnershipLifetime +
maximumTrustedUtcIntervalWidth`, and requires the observed trusted interval
to be after the reaping boundary and both old-owner/provider closure horizons.
Opaque evidence digests identify those external attestations; this local
slice does not issue them.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.CheckpointReapingSweepCoordinatorTest \
  --no-daemon --console=plain
```

The source-locked implementation is Delay commit
`7b8b73885c5ec26dfc96c1b5b8a1a6ab8ec0d1d9`. The locked MinIO rerun used
container `nereus-delay-minio-e2e-1786843920-34723`, endpoint
`http://127.0.0.1:59954`, bucket
`nereus-delay-checkpoints-1786843920-34723`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `9c4dcab9-c03c-4860-81de-07e62302d30e`, and the
harness ended with `BUILD SUCCESSFUL` after the proof-gated coordinator
sweep.

This closes only the local proof/ordering gate. External attestation issuers,
Owner/session loss, provider quiescence, source-ordered delete confirmation,
Floor/Pin/Owner transactions, provider breadth, chaos, failover and release
gates remain open.

## Checkpoint REAPING Owner proof receipt

The reaping coordinator now requires a typed Owner proof before it can win the
`PENDING_UPLOAD -> REAPING` CAS. `CheckpointReapingOwnerProof` binds the exact
pending intent digest, Owner identity, Store Incarnation and session-bound
recorded lease. `CheckpointReapingOwnerProofIssuer` distinguishes exact Owner
abandonment (release plus absence reread) from another actor observing that
the recorded lease is no longer current; both require trusted UTC at or after
the upload deadline. The quiescence receipt binds its old-owner digest to the
Owner proof digest.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.CheckpointReapingOwnerProofIssuerTest \
  --tests io.nereusstream.delay.store.CheckpointReapingSweepCoordinatorTest \
  --rerun-tasks --no-daemon --console=plain
```

The source-locked implementation is Delay commit
`44cd3230709f5e87742cd94cd9a8b7bce314a184`. The locked MinIO rerun used
container `nereus-delay-minio-e2e-1786845031-48170`, endpoint
`http://127.0.0.1:62715`, bucket
`nereus-delay-checkpoints-1786845031-48170`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `ea89d80e-e63e-4980-b225-94b070d3c36b`, and the
harness ended with `BUILD SUCCESSFUL`.

This receipt proves only the local typed Owner-proof composition and its
binding to the provider-call gate. The issuer is not the production
cross-record intent/Owner/session/catalog authority; certified session-loss,
provider quiescence, source-ordered delete confirmation, Floor/Pin/Owner
transactions, provider breadth, chaos, failover and release gates remain
open.

## Checkpoint provider-owned request horizon ledger receipt

Delay commit `cc97c7654cb19f88c69045cd3c33a4d970a9fed3` adds a local
`ObjectStoreProviderOwnershipTracker` around the S3-compatible checkpoint
adapter. It keeps each upload/download/delete/sweep operation active through
all nested HTTP calls and streamed response bodies, retains a bounded
uncertainty horizon after an ambiguous failure, and exposes a canonical local
observation only after a one-way new-operation fence, active-operation drain
and elapsed horizon. The renewable wrapper rejects renewal after the fence,
and every `HttpClient.send` rechecks the local credential-use lease.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.ObjectStoreProviderOwnershipTrackerTest \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

JUnit recorded tracker `tests=3 skipped=0 failures=0 errors=0` and adapter
`tests=9 skipped=0 failures=0 errors=0`; the full check returned 0. The
locked MinIO rerun used container
`nereus-delay-minio-e2e-1786846128-60582`, endpoint
`http://127.0.0.1:49215`, bucket
`nereus-delay-checkpoints-1786846128-60582`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The JUnit system-out recorded provider manifest version
`1b904a10-2104-46eb-a6fd-0bd2afe24524`, and the harness ended with
`BUILD SUCCESSFUL`.

This receipt closes local adapter operation accounting and admission fencing
only. It does not attest remote provider request completion or provider
quiescence and does not close certified REAPING provider evidence,
source-ordered delete confirmation, Recovery Floor/Pin/Owner transactions,
provider breadth, chaos, failover or V1 release gates.

## Checkpoint delete-confirmation mutation composition receipt

Delay commit `70e5f0da` adds the pure local
`CheckpointDeleteConfirmationComposer`. It binds a complete
`CheckpointDeleteResult` to the exact canonical identity and identity hash in
an already-applied `ResourceRetireIntentRecord`, preserves the
`DELETED`/`ALREADY_ABSENT` evidence rules, and requires the trusted
confirmation interval to start at or after the observation interval's latest
bound. It derives the mutation shard from the retire Source Position and
signs the canonical `RESOURCE_DELETE_CONFIRMED_V1` body using the service
author and retire mutation ID.

The focused regression was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.CheckpointDeleteConfirmationComposerTest \
  --no-daemon --console=plain
```

JUnit recorded `tests=4 skipped=0 failures=0 errors=0`, and the full
`./gradlew check --no-daemon --console=plain --quiet` returned 0. This is a
local composition receipt only: it does not claim provider-side deletion,
GC Owner/Floor/Pin authorization, Shard Log append, mutation apply, or V1
release readiness.

## Delete-confirmation temporal evidence fence receipt

Delay commit `a26c6816` moves the confirmation-time causal fence into both
the canonical `ResourceDeleteConfirmedBody` parser and the durable
`ResourceDeleteConfirmedRecord`. Every `RESOURCE_DELETE_CONFIRMED_V1` path
now requires `confirmedAt.earliestEpochMs()` to be at least the complete
provider-observation interval's `latestEpochMs()`, so a manually constructed
or differently composed signed mutation cannot bypass the evidence ordering
rule.

The focused body/record/GC/composer regression recorded 26 passing tests; the
full `./gradlew check --no-daemon --console=plain --quiet` remains the
repository gate. This receipt is local evidence-shape coverage only and does
not claim provider completion, deletion authorization, Floor/Pin/Owner
transition, Shard Log append/apply, or release readiness.

## Source-ordered GC confirmation handoff receipt

Delay commit `b225cef9` adds a typed `GcWorkClassExecutor` handoff for
`RESOURCE_DELETE_CONFIRMED_V1`. The handoff binds the nested retire reference
to the exact `ResourceRetireIntentRecord` supplied by the caller and only
returns `PERSISTED` when the external append result is strictly later than the
retire Source Position on the same authenticated physical source. A regressed
or foreign returned position becomes `UNKNOWN` and fences the local Owner.

The focused `GcWorkClassExecutorTest` passed both the valid-later and
regressed-position cases; the full
`./gradlew check --no-daemon --console=plain --quiet` returned 0. This is a
local interpretation fence for an external append receipt, not provider
delete evidence, source position allocation, tombstone apply, lifecycle
authorization or release evidence.

## Oxia Recovery Pin session-bound CAS receipt

Delay commit `dedd03a94fb2ab1e8d12f19ba993408646426578` adds a separate
session-bound ephemeral Oxia record for the active `RecoveryPinV1`. The
catalog record remains the single CAS authority for manifests/resources and
scalar/typed Floor state. Pin create uses the exact canonical recovery-pin
key with `IfRecordDoesNotExist` and `AsEphemeralRecord`, requires the caller's
connected-session digest, validates the returned key/version/session metadata,
and rereads the exact canonical pin bytes. It also checks the observed catalog
generation before and after the pin CAS. Release uses exact version CAS and
accepts response loss only after an exact absent reread.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.OxiaSyncRecoveryCatalogBackendTest \
  --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
  --no-daemon --console=plain
```

The deterministic catalog suite passed 17 tests and the full check returned
0. The real-service smoke methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured. This receipt proves only the
single-pin record/CAS boundary and its local response-loss tests; it does not
prove an Oxia cross-record transaction, Owner/session-loss authority,
multi-worker activation, provider deletion, GC authorization, chaos,
failover or V1 release readiness.

## Atomic publication Recovery Pin CAS receipt

Delay commit `04976375` composes the shared
`OxiaSessionBoundRecoveryPinStore` into the atomic publication authority.
`OxiaSyncCheckpointPublicationBackend` keeps PUBLISHED Upload Intent plus
Catalog manifest in its one canonical `/publication` CAS record and stores
the active pin in a separate `/recovery-pin` ephemeral record. The
identity-bearing constructor supplies the connected Oxia session digest;
create validates the current publication catalog projection, exact key/version
and session-derived digest, rereads canonical bytes and rechecks catalog
generation. Release uses exact version CAS and exact absence reread.

The local regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.OxiaSyncRecoveryCatalogBackendTest \
  --tests io.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackendTest \
  --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
  --tests io.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest \
  --no-daemon --console=plain
```

JUnit recorded 17 deterministic Recovery Catalog tests and 4 deterministic
atomic-publication tests with zero failures; the four real-service methods
were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the
full check returned 0. This receipt proves reusable single-pin record/CAS
semantics only. It does not prove an atomic Intent/Catalog/Pin transaction,
Owner/session-loss authority, provider completion, GC authorization, chaos,
failover or V1 release readiness.

## Oxia Control Operation session-bound CAS receipt

Delay commit `cc8001b528bb9943a2f683c6ad14728c426cb8f2` adds the
`OxiaSyncControlOperationBackend(ClientHandle, keyPrefix)` path. It fences
each control-operation record read and CAS write with the exact connected Oxia
session marker, including the response-loss path. A marker change after a
committed write prevents the backend from performing an authorized exact
reread, so the caller receives a fence failure rather than a guessed CURRENT
result.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.ownership.OxiaSyncControlOperationBackendTest \
  --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest \
  --no-daemon --console=plain
```

The deterministic backend suite passed 5 tests. The two real-service methods
were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the
full `./gradlew check --no-daemon --console=plain --quiet` returned 0. This is
single-record session-bound CAS evidence only; source-ordered control routing,
actor/scope authorization, cross-record target/state transactionality,
automatic session recovery, production query routing, chaos and V1 release
gates remain open.

## Oxia Control Target Registration session-bound CAS receipt

Delay commit `50435a1364d2e8f7d823cc05faa18e4766f5cbd6` adds the
`OxiaSyncControlTargetRegistrationBackend(ClientHandle, keyPrefix)` path. It
checks the exact connected Oxia session marker before and after each target
registration record read or `IfRecordDoesNotExist` write. A marker change
after a committed write prevents the backend from reporting
`RECORDED`/`ALREADY_RECORDED` through a guessed response-loss result; lookup
and mutation-validation reads use the same fence.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.ownership.OxiaSyncControlTargetRegistrationBackendTest \
  --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest \
  --no-daemon --console=plain
```

The deterministic target-registration suite passed 4 tests. The two
real-service methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not
configured, and the full `./gradlew check --no-daemon --console=plain --quiet`
returned 0. This receipt proves only per-record session-bound CAS; atomic
Control Operation plus target registration, actor/source authority, automatic
session recovery, production routing, chaos and V1 release gates remain open.

## Oxia credential Profile catalog session-bound CAS receipt

Delay commit `89020c97c29f99d98f7f3259ab7b27131644adcd` adds the
`OxiaSyncProfileCatalogBackend(ClientHandle, ...)` path. It checks the exact
connected Oxia session marker before and after each Profile catalog read or
version CAS write, covering publication, equivalent-secret rotation,
protection-before-lease issuance, resolution and response-loss rereads. A
marker change after a committed Profile write prevents a guessed publication,
rotation or credential-lease success.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.runtime.OxiaSyncProfileCatalogBackendTest \
  --tests io.nereusstream.delay.runtime.OxiaRealProfileCatalogSmokeTest \
  --no-daemon --console=plain
```

The deterministic Profile catalog suite passed 4 tests. The real-service
method was skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured,
and the full `./gradlew check --no-daemon --console=plain --quiet` returned 0.
This receipt proves only the single-record Profile session fence; secret
resolution, source/actor authority, retained-generation GC, cross-record
transactions, automatic reconnect, provider rotation/quiescence, chaos and
V1 release gates remain open.

## Oxia Recovery Catalog session-bound CAS receipt

Delay commit `f04f58d15588662b71be68809e1a11a627baf540` adds the
`OxiaSyncRecoveryCatalogBackend(ClientHandle, ...)` path. It checks the exact
connected Oxia session marker before and after every catalog read/version CAS
write. The original receipt's pin store still received the raw record client,
so pin session identity and ephemeral CAS were covered, but current-marker
fencing of pin I/O was not proven by that source revision.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.OxiaSyncRecoveryCatalogBackendTest \
  --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
  --no-daemon --console=plain
```

The deterministic Recovery Catalog suite passed 18 tests. The three
real-service methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not
configured, and the full `./gradlew check --no-daemon --console=plain --quiet`
returned 0. This receipt proves only catalog-record single-record session
fencing; Catalog/Pin/Upload-Intent transactionality, source ordering,
Owner/session recovery, provider publication/deletion, chaos and V1 release
gates remain open.

## Oxia Checkpoint Publication session-bound CAS receipt

Delay commit `ffe0e5e15894ba377248068258444a1484bfb7f2` adds the
`OxiaSyncCheckpointPublicationBackend(ClientHandle, ...)` path. The combined
PUBLISHED Upload Intent and Recovery Catalog manifest remain in the canonical
`/publication` record, while every publication-record read and version-CAS
write on the handle-bound path checks the exact connected Oxia session marker
before and after the call. If a committed CAS is followed by marker loss, the
caller receives a fence failure instead of a guessed publication result; the
sibling ephemeral Recovery Pin remains a separate session-identity-bound
record.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackendTest \
  --tests io.nereusstream.delay.store.OxiaRealCheckpointPublicationSmokeTest \
  --no-daemon --console=plain
```

The deterministic Publication suite passed 5 tests. The two real-service
methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured,
and the full `./gradlew check --no-daemon --console=plain --quiet` returned 0.
This receipt proves only the single canonical publication-record session
fence; it does not prove an atomic Intent/Catalog/Pin transaction, provider
completion, source/evidence replay, Owner/session recovery, chaos, failover or
V1 release readiness.

## Oxia Checkpoint Upload Intent session-bound CAS receipt

Delay commit `0a1e6020` adds the
`OxiaSyncCheckpointUploadIntentBackend(ClientHandle, ...)` path. The independent
`/intent` record retains exact canonical intent and version-CAS semantics, but
the handle-bound constructor checks the exact connected Oxia session marker
before and after every read/write. A committed intent successor followed by
marker loss is therefore fenced instead of being returned as a guessed
create/publish/reaping success; the combined `/publication` authority remains
a separate canonical record surface.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.OxiaSyncCheckpointUploadIntentBackendTest \
  --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
  --no-daemon --console=plain
```

The deterministic Upload Intent suite passed 4 tests. The three real-service
methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured,
and the full `./gradlew check --no-daemon --console=plain --quiet` returned 0.
This receipt proves only the independent upload-intent single-record session
fence; it does not prove cross-record Intent/Catalog/Pin transactionality,
Owner/session recovery, provider completion, source/evidence replay, chaos,
failover or V1 release readiness.

## Oxia Worker assignment session-bound CAS receipt

Delay commit `cca59a92df395c11cfdda23d24bb27a8b5269cca` strengthens the
`OxiaSyncWorkerAssignmentBackend(ClientHandle, ...)` path. The durable desired
assignment record checks the exact connected Oxia session marker before and
after every read, version-CAS write and exact-version withdrawal. A committed
assignment followed by marker loss is fenced instead of being returned as a
guessed publication result; the unbound constructor remains an explicit
deterministic/external seam.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackendTest \
  --tests io.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest \
  --no-daemon --console=plain
```

The deterministic Worker assignment suite passed 5 tests. The real route-
worker smoke method was skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not
configured, and the full `./gradlew check --no-daemon --console=plain --quiet`
returned 0. This receipt proves only desired-assignment single-record session
fencing; it does not prove Assignment/Owner/Route transactionality, placement
authority, session recovery, source/evidence replay, chaos, failover or V1
release readiness.

## Oxia Owner Lease session-bound CAS receipt

Delay commit `7a76a3af61ea16bceb81cc566462c078ca8de2a5` strengthens the
connected `OxiaSyncOwnerLeaseBackend` path. The owner epoch and ephemeral
lease records check the exact connected Oxia session marker before and after
every read, version-CAS write and exact-version delete. A committed lease
followed by marker loss is fenced instead of being returned as a guessed
acquire/renewal/transition/release result; the unbound constructor remains an
explicit deterministic/external seam.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackendTest \
  --tests io.nereusstream.delay.ownership.OxiaRealServiceSmokeTest \
  --no-daemon --console=plain
```

The deterministic owner-lease suite passed 14 tests. The real-service smoke
method was skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured,
and the full `./gradlew check --no-daemon --console=plain --quiet` returned 0.
This receipt proves only the per-record owner epoch/lease session fence; it
does not prove Assignment/Owner/Route transactionality, placement authority,
automatic session recovery, source ordering, chaos, failover or V1 release
readiness.

## Oxia Route authority session-bound I/O fence receipt

Delay commit `57e466786aea596cfdbd75020e48310415da0335` strengthens the
`OxiaRouteAuthoritySession` record/watch surface. Route `get`, `put`,
notification registration and range-scan creation check the exact ephemeral
marker before and after the delegated call; lazy range iteration checks the
marker around each `hasNext`, `next` and `remove`. A committed Route head
followed by marker loss is fenced instead of being returned as a guessed
publication result.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.route.OxiaSignedRouteSnapshotProviderTest \
  --tests io.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest \
  --tests io.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest \
  --no-daemon --console=plain
```

The deterministic Route provider/session suite passed 6 tests. Four real Route
authority methods and one real Route-worker method were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full
`./gradlew check --no-daemon --console=plain --quiet` returned 0. This receipt
proves only per-operation Route session fencing and lazy range protection; it
does not prove event/head transactionality, automatic reconnect, multi-node
failover, placement/source ownership, chaos or V1 release readiness.

## Atomic checkpoint publication authority pairing fence receipt

Delay commit `920197ad41aaa6f0b88871f5ddf631f6899a53d3` makes
`CheckpointPublicationCoordinator` reject a split atomic intent/catalog pair
regardless of which supplied side implements
`CheckpointAtomicPublicationAuthority`. The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.CheckpointUploadCoordinatorTest \
  --no-daemon --console=plain
```

The focused checkpoint coordinator suite passed. This receipt proves only
constructor-time authority pairing; it does not prove an Intent/Catalog/Pin
multi-record transaction, provider evidence, Owner/session recovery, source
ordering, chaos or V1 release readiness.

## Recovery Pin session-fenced client wiring correction receipt

Delay commit `f0e45cbdf6eb30d730c6678e71c4c19d34e06072` passes the
session-wrapped catalog/publication `RecordClient` into
`OxiaSessionBoundRecoveryPinStore` in both Oxia authorities. Pin reads,
ephemeral creates and exact-version releases now check the connected marker
before and after the operation.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.OxiaSyncRecoveryCatalogBackendTest \
  --tests io.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackendTest \
  --no-daemon --console=plain
```

The deterministic Recovery Catalog and Publication suites passed. This
correction proves only the per-record Recovery Pin session fence and does not
prove Catalog/Pin/Upload-Intent transactionality, Owner/session recovery,
provider evidence, source ordering, chaos or V1 release readiness.

## Oxia Route notification reconnect session fence receipt

Delay commit `de203e4dc14de32746ce73da75381843152af922` adds the current
session-marker fence to `OxiaRouteAuthoritySession.reconnectNotifications`.
Replacement notification registration is checked before the replacement is
created and immediately before and after the callback is registered. A marker
change after registration closes the replacement client, restores the previous
client reference and returns a failed operation instead of exposing a stale
reconnect as successful.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.route.OxiaSignedRouteSnapshotProviderTest \
  --tests io.nereusstream.delay.route.OxiaRealRouteAuthoritySmokeTest \
  --tests io.nereusstream.delay.route.OxiaRealRouteWorkerAssignmentSmokeTest \
  --no-daemon --console=plain
```

The deterministic Route provider/session suite passed 8 tests. Four real Route
authority methods and one real Route-worker method were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured. This receipt proves only the
replacement notification registration fence; it does not prove automatic
reconnect, event/head transactionality, multi-node failover, placement/source
ownership, raw chaos or V1 release readiness.

## Oxia Route provider start retry after notification fence receipt

Delay commit `d241246eefc284fea9719c8e162afa8e2a8e4828` fixes the provider
state after a replacement notification registration commits but the Route
session marker changes before the call can return. A non-healthy started
provider no longer treats a repeated `start()` as a no-op; it reuses explicit
`refresh()`, reconnects the Route session, installs the replacement notification
client and rebuilds the signed Route cache.

The deterministic regression
`OxiaSignedRouteSnapshotProviderTest.startRetriesNotificationRegistrationAfterACommittedRegistrationIsFenced`
passed as part of the 9-test Route provider/session suite. The receipt proves
only this retry state transition after a fenced registration; it does not prove
transparent automatic reconnect, event/head transactionality, multi-node
failover, placement/source ownership, raw chaos or V1 release readiness.

## Oxia Route initial-refresh notification restoration receipt

Delay commit `22780082d24e2011d44ead6ca62c38251a03633b` closes the gap where a
provider whose first Route replay failed could later rebuild a healthy cache
without registering a notification stream. `refresh()` now establishes the
initial callback after the repaired authority replay; a later registration
fence uses the replacement path on the next explicit retry.

`OxiaSignedRouteSnapshotProviderTest.refreshAfterAnInitialRouteGapRestoresTheNotificationStream`
passed as part of the 10-test Route provider/session suite. This receipt proves
only initial-refresh notification restoration; it does not prove transparent
automatic reconnect, event/head transactionality, multi-node failover,
placement/source ownership, raw chaos or V1 release readiness.

## Fleet and Route resource close aggregation receipt

Delay commit `eb47cb807ceb45d68a9f8db5f53ef3a7cc6ead4e` makes the local fleet
and session close paths attempt every independently owned resource after an
earlier close failure. The first failure remains the primary exception and
later failures are suppressed; the fleet is not marked closed while a shard
still requires its owner-drain retry.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.ownership.WorkerShardFleetRuntimeTest \
  --tests io.nereusstream.delay.route.OxiaSignedRouteSnapshotProviderTest \
  --no-daemon --console=plain
```

The fleet suite passed 2 tests and the Route provider/session suite passed 11
tests, with zero failures/skips/errors. This is deterministic local teardown
evidence only; it does not prove owner-drain bypass, automatic Oxia recovery,
Route transactionality, placement/source ownership, chaos, failover or V1
release readiness.

## Worker source close retry receipt

Delay commit `874fccb4fc521ad51b7954236ec5e37c1591e011` keeps the source loop
open when its native `SourceRecordConsumer.close()` fails, allowing the exact
owner-drain close boundary to retry. The loop becomes closed only after a
successful native close.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.ownership.SourceApplyCoordinatorTest \
  --no-daemon --console=plain
```

The deterministic source/apply suite passed 8 tests, including the native
close-failure retry regression. This is local lifecycle evidence only; it does
not prove pending-ACK bypass, Broker reconnect/ACK durability, crash/chaos,
failover or V1 release readiness.

## Route client teardown retry receipt

Delay commit `9f24b2f38ba4f21962bebdaa2455d7f86ba0cd1b` keeps the Route session
and provider close paths fenced but retryable. A failed authority/client close
does not allow new Route I/O, yet the next explicit close retries all still
owned clients and marks completion only after success.

The deterministic Route provider/session suite passed 12 tests, including
session and provider client-close retry regressions. This receipt proves only
local teardown retryability; it does not prove automatic Oxia recovery, Route
transactionality, placement/source ownership, chaos, failover or V1 release
readiness.

## Direct SDK client teardown retry receipt

Delay commit `677026b3` keeps `DefaultDelayClient` closed to new operations
while its owned child resources remain explicitly retryable. The first close
attempts the outbox, query client and optional transport registry even when the
outbox fails; the next close reaches all three children again and completes
only after success.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.client.DefaultDelayClientTest \
  --no-daemon --console=plain
```

The deterministic Direct SDK client suite passed 11 tests, including
`closeRetriesEveryChildAfterTheFirstCloseFailure`. This receipt proves only
local SDK teardown retryability; it does not prove provider/session recovery,
transport delivery, durable outbox authority, crash/chaos, failover or V1
release readiness.

## Route connect prefix validation receipt

Delay commit `4da7bcf46b0ab9350adebf1f614590851a1fadd8` validates the canonical
Route key prefix before creating the authority and notification Oxia clients.
The constructor receives the validated canonical value, so malformed prefixes
fail at the connect boundary instead of after external client creation.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.route.OxiaRouteAuthoritySessionTest \
  --no-daemon --console=plain
```

The deterministic Route session construction suite passed 1 test, including
`connectRejectsAnInvalidKeyPrefixBeforeCreatingOxiaClients`. This receipt
proves only local connect-input/resource-ordering validation; it does not
prove Oxia recovery, Route transactionality, placement/source ownership,
chaos, failover or V1 release readiness.

## Worker monitor teardown retry receipt

Delay commit `2f7d9d667547380355a27517ea2c1e4941962693` keeps both Worker
resource monitors fenced but retryable during executor teardown. The first
injected `shutdownNow()` failure is retained while cancellation/shutdown
actions continue; a second explicit monitor close reaches the executor again.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.WorkerRuntimeResourceMonitorTest \
  --tests io.nereusstream.delay.store.WorkerRocksDbUsageMonitorTest \
  --no-daemon --console=plain
```

The deterministic monitor suites passed 12 tests, including the shutdown
retry regression in both monitor classes. This receipt proves only local
Worker monitor teardown retryability; it does not prove native process
recovery, production resource authority, Owner/Oxia, chaos, failover or V1
release readiness.

## In-memory command transport registry teardown retry receipt

Delay commit `0378e9a7585397e6f5e71a301f58c6d00835f2a0` fences the deterministic
transport registry immediately while preserving failed child transports for
an explicit retry. Successful transports are removed after their first close;
the next registry close reaches only the still-owned failure.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.transport.InMemoryCommandTransportRegistryTest \
  --no-daemon --console=plain
```

The deterministic registry suite passed 1 test,
`closeRetriesOnlyTheTransportThatFailedTheFirstTeardown`. This receipt proves
only local registry teardown retryability; it does not prove production
Kafka/Pulsar client lifecycle, transport delivery, Broker failover, chaos or
V1 release readiness.

## Guarded Pulsar transport teardown aggregation receipt

Delay commit `9d164037f9ba3832cd1f83846813b44de18967ab` attempts both managed
and native Pulsar sender closes after the close boundary is entered. A managed
sender failure remains primary while the native sender is still attempted;
the enclosing retry gate can then repeat the failed child.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.transport.GuardedTransportOwnershipTest \
  --no-daemon --console=plain
```

The deterministic guarded transport suite passed 4 tests, including
`pulsarCloseAttemptsNativeSenderAfterManagedSenderFailure`. This receipt proves
only local Pulsar teardown aggregation; it does not prove native/managed
Broker delivery, client authority, failover, chaos or V1 release readiness.

## Owner connect prefix validation receipt

Delay commit `499e8439f2fe0f1b1c1114dbfd1bb7e55a06c43c` validates the canonical
Owner authority key prefix before creating the external Oxia client. The
backend receives the already validated prefix, so malformed input fails before
session/client setup.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackendTest \
  --no-daemon --console=plain
```

The deterministic Owner backend suite passed 15 tests, including
`connectRejectsAnInvalidKeyPrefixBeforeCreatingAnOxiaClient`. This receipt
proves only local Owner connect-input/resource-ordering validation; it does
not prove Owner/Oxia recovery, lease authority, placement, chaos, failover or
V1 release readiness.

## Gateway admission lease release retry receipt

Delay commit `d5384b954e4d99ad291b2aea004910e1b1666ec8` leaves the durable
Gateway admission lease handle retryable when its bounded release CAS does not
converge. The exact handle can perform the same release again; only a
successful durable removal marks it closed.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayAdmissionControllerTest \
  --no-daemon --console=plain
```

The deterministic Gateway admission suite passed 6 tests, including
`leaseCloseRemainsRetryableAfterReleaseCasDoesNotConverge`. This receipt proves
only local admission-lease release retryability; it does not prove
distributed Gateway authority, session recovery, transport delivery,
failover, chaos or V1 release readiness.

## Gateway idempotency evidence monotonicity receipt

Delay commit `b19f998ffe811d0a6dee1051491eae6c61131712` binds each durable
Gateway outcome to the exact prepared branch and physical attempt identity.
Identical terminal evidence is idempotent; conflicting terminal evidence is
rejected without overwrite. Aggregate selection keeps the first queued
receipt, retains the highest unresolved attempt when any uncertainty remains,
and permits a later retry from that unresolved attempt even if a newer retry
has a definitive non-queued result.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --tests io.nereusstream.delay.gateway.GatewayScheduleServiceTest \
  --no-daemon --console=plain
```

The deterministic Gateway suites passed 13 tests with zero failures/skips/
errors. The full `./gradlew check` passed 1532 tests with 24 skips and zero
failures/errors. This receipt proves only local durable idempotency evidence
ordering; it does not prove distributed Gateway authority, transport delivery,
Broker failover, chaos or V1 release readiness.

## Gateway prepared-expiry fence and aggregate replay receipt

Delay commit `66508783f5e8230ace8bae37ff04c28dfb353653` checks prepared
retention at the durable `startAttempt()` boundary in both the Oxia and
in-memory stores. An expired prepared record remains attempt-free and does not
call the submission coordinator. A record with an installed aggregate bypasses
the request deadline and returns the exact stored outcome on a later duplicate
RPC.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.GatewayScheduleServiceTest \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --no-daemon --console=plain
```

The deterministic Gateway suites passed 16 tests with zero failures/skips/
errors. The full `./gradlew check` passed 1535 tests with 24 skips and zero
failures/errors. This receipt proves only local prepared-expiry fencing and
durable aggregate replay; it does not prove distributed Gateway authority,
transport delivery, Broker failover, chaos or V1 release readiness.

## Gateway attempt projection integrity fence receipt

Delay commit `52c6ed1c604a98b56668e510a3cf84ad364ec9cc` makes persisted Gateway
attempt/record projections fail closed on impossible lifecycle shapes. The
attempt codec binds evidence presence to `STARTED` versus terminal state; the
record codec binds attempt numbering, physical/retry identity uniqueness,
phase, and aggregate presence before a record is accepted.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --no-daemon --console=plain
```

The deterministic idempotency suite passed 10 tests with zero
failures/skips/errors. The full `./gradlew check` passed 1536 tests with 24
skips and zero failures/errors. This receipt proves only local durable
projection integrity; it does not prove distributed Gateway authority,
transport delivery, Broker failover, chaos or V1 release readiness.

## Gateway stored evidence binding receipt

Delay commit `380e279725e9ac5d31f98ad49ee711cd15c5b25c` makes the Gateway
idempotency record reject semantically foreign stored evidence. Terminal
outcomes must match the prepared managed/native branch, prepared command or
native reference, physical attempt identity and persisted state; the stored
aggregate must equal the deterministic aggregate recomputed from the attempt
history.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --no-daemon --console=plain
```

The deterministic idempotency suite passed 11 tests with zero
failures/skips/errors, including
`gatewayProjectionRejectsOutcomeStateAndAggregateMismatches`. The full
`./gradlew check` passed 1537 tests with 24 skips and zero failures/errors.
This receipt proves only local stored-evidence binding and aggregate
recomputation; it does not prove distributed Gateway authority, transport
delivery, Broker failover, chaos or V1 release readiness.

## Gateway retry evidence hash binding receipt

Delay commit `5e1bd9f6b3e2bcf24972e7b9ecdd78db49520734` makes each stored retry
request hash bind to the current gateway key and an earlier physical attempt.
Canonical bytes with a foreign retry hash are rejected before durable Gateway
state-machine use. The validator permits the earlier attempt to be in any
final state so a late queued/definitive callback after retry creation remains
valid.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --no-daemon --console=plain
```

The deterministic idempotency suite passed 11 tests with zero
failures/skips/errors. The full `./gradlew check` passed 1537 tests with 24
skips and zero failures/errors. This receipt proves only local retry-evidence
hash binding; it does not prove distributed Gateway authority, transport
delivery, Broker failover, chaos or V1 release readiness.

## Gateway operation/prepared binding receipt

Delay commit `f27800424a7cde3b8496b4fbbb4d4586cbeb07ca` binds the stored
Gateway operation to the prepared command type. Managed command types map to
the matching Schedule, PrepareLarge, CommitLarge, Cancel or Reschedule
operation; native prepared delivery is accepted only for Schedule. A
canonical but mismatched operation record is rejected before durable attempt
processing.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.GatewayGrpcServiceTest \
  --tests io.nereusstream.delay.gateway.GatewayScheduleServiceTest \
  --no-daemon --console=plain
```

The focused Gateway suites passed 10 tests with zero failures/skips/errors.
The full `./gradlew check` passed 1537 tests with 24 skips and zero
failures/errors. This receipt proves only local operation/prepared binding;
it does not prove distributed Gateway authority, transport delivery, Broker
failover, chaos or V1 release readiness.

## Gateway audit phase evidence receipt

Delay commit `745da182c72af27dff09a8fb55db6cc15a4f20e3` requires the local
Gateway audit event union to carry an outcome digest exactly when its phase is
`COMPLETED`. `RECEIVED` and `FAILED` events with a digest, and `COMPLETED`
events without one, are rejected before durable audit storage.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayAuditSinkTest \
  --no-daemon --console=plain
```

The focused audit suite passed 4 tests with zero failures/skips/errors. The
full `./gradlew check` passed 1538 tests with 24 skips and zero
failures/errors. This receipt proves only local audit phase/digest shape
validation; it does not prove distributed Gateway authority, transport
delivery, Broker failover, chaos or V1 release readiness.

## Gateway active attempt tail fence receipt

Delay commit `a1a85f99471743c48126943fad92fbb80ce6be34` requires an ACTIVE
Gateway idempotency projection to contain exactly one final `STARTED` attempt.
Multiple `STARTED` attempts and a non-final `STARTED` followed by terminal
evidence are rejected before durable state-machine use.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --no-daemon --console=plain
```

The deterministic idempotency suite passed 11 tests with zero
failures/skips/errors. The full `./gradlew check` passed 1538 tests with 24
skips and zero failures/errors. This receipt proves only local active-attempt
projection integrity; it does not prove distributed Gateway authority,
transport delivery, Broker failover, chaos or V1 release readiness.

## Gateway attempt timing/retry shape receipt

Delay commit `e0d5bc9761fea57103518819165d54eb60662b99` enforces the physical
attempt timing and retry identity union: both time boundaries must be after
the start, ownership expiry must not exceed uncertainty, the first attempt
must omit retry identity, and later attempts must carry it.

The focused receipt command was:

```bash
./gradlew test \
  --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest \
  --no-daemon --console=plain
```

The deterministic idempotency suite passed 11 tests with zero
failures/skips/errors. The full `./gradlew check` passed 1538 tests with 24
skips and zero failures/errors. This receipt proves only local physical-
attempt temporal/retry-shape validation; it does not prove distributed
Gateway authority, transport delivery, Broker failover, chaos or V1 release
readiness.

## Pulsar large-payload Gateway-to-destination authority E2E receipt

The checked-in `run-pulsar-large-payload-gateway-e2e.sh` composes the Delay
Pulsar P1 binding with a real two-Broker Pulsar topology, real Oxia, Gateway
mTLS/JWT, Worker source apply/ACK and a locked versioned MinIO service. Run it
from the Delay worktree with:

```bash
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The source-bound receipt passed at Delay
`accdc7074bfd38aed2cfd7c696a8c3ff62a972ba`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1 distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Its isolated Compose project was
`nereus-delay-pulsar-large-e2e-1786879186-27914`; the destination was
`pulsar-large-payload-destination-27914`.

The positive output was:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=2/4, typed PULSAR_SEND_ACK target ledger/entry=3/0, Outcome source ledger=2/5, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
BUILD SUCCESSFUL
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed
```

This proves the bounded real chain through versioned MinIO upload/attestation,
exact 1 MiB + 4 KiB destination readback (`1,052,672` bytes), duplicate
Prepare byte identity, typed Pulsar SEND evidence, source `PUBLISH_OUTCOME`,
final local checkpoint and Oxia Owner release. It does not cover combined
Gateway-plus-multi-Broker failover, multi-shard placement, raw
crash/network/proxy/process chaos, Kafka LSO/retention recovery, Object Store
checkpoint publication or V1 release readiness.

### Clean revalidation at Delay `667458b9`

The same normal authority path was rerun from clean Delay commit
`667458b98bd5adcec04eae53e2d2fe7da157be8c` after the guarded source reconnect
replay, recovered `UNKNOWN` Publish Admission handling and exact Compose
cleanup changes:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-revalidation-20260816 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The run was locked to P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1
distribution `373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and the locked MinIO image
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The isolated Compose project was
`nereus-delay-pulsar-large-e2e-1786884946-97580`.

The clean output was:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/4, typed PULSAR_SEND_ACK target ledger/entry=4/0, Outcome source ledger=3/5, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=3/2, commit=3/3, exactGatewayIdempotency=true, sourceRecords=6
BUILD SUCCESSFUL in 57s
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed
```

The post-run exact-name cleanup check found no containers for the project and
no temporary P1/Oxia images. This is a normal-path revalidation only; it does
not claim combined multi-Broker failover, the untriggered recovered `UNKNOWN`
response-loss branch, multi-shard placement, chaos or V1 release readiness.

## Pulsar Worker destination response-loss with real Oxia

The focused Worker destination response-loss mode now passes the real Oxia
endpoint into the Worker when `NEREUS_DELAY_PULSAR_WITH_OXIA=1`. Run it with:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-worker-destination-response-loss-oxia-20260816 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The source-bound run was Delay
`b647176ed92491fd96514eed2b87098454078a79`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
artifact SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, P1
image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Pulsar Compose project
`nereus-delay-pulsar-e2e-1786885612-9737` used `19787/19788`; the Oxia project
was `nereus-delay-pulsar-oxia-e2e-1786885612-9737` on `16657`.

The source-bound output was:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker destination response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved the source-applied PUBLISHED Outcome
Pulsar Worker source-applied physical publish passed: Admission source ledger=9/3, typed PULSAR_SEND_ACK target ledger/entry=10/0, Outcome source ledger=9/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
BUILD SUCCESSFUL in 1m 1s
Pulsar Worker destination response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and the source-applied Outcome completed.
```

This proves the bounded real-Oxia Worker destination SEND response-loss path,
typed evidence resolution, source-applied `PUBLISHED` Outcome and final
checkpoint. Exact cleanup checks found no containers, images, volumes or
networks for either project. The run used normal `ENQUEUED` admission; it does
not claim the recovered `UNKNOWN` Publish Admission branch, raw socket/process
loss, combined Gateway multi-Broker failover, checkpoint publication,
multi-shard placement or V1 release readiness.

## Pulsar Worker UNKNOWN Publish Admission response-loss with real Oxia

The focused Worker admission response-loss mode injects one bounded
client-side cut after the real guarded Shard Log producer has persisted the
admission mutation. It discards only that first local `PERSISTED` result, so
the Worker must recover the exact mutation from source replay; the later
`PUBLISH_OUTCOME` append is left intact.

Run it with:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-worker-admission-response-loss-oxia-20260816 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The source-bound run was Delay `88d58c02`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
artifacts
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, P1
image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Pulsar Compose project
`nereus-delay-pulsar-e2e-1786886923-27929` used `19679/19680`; the Oxia
project was `nereus-delay-pulsar-oxia-e2e-1786886923-27929` on `16657`.

The source-bound output was:

```text
Pulsar Worker recovered UNKNOWN Publish Admission from exact source mutation: PulsarSourcePosition[shardId=ShardId[routeIncarnation=85d32917c2004c0ca801400cc3da8572, partition=0], brokerResourceIncarnation=[B@69a2b3b6, physicalTopic=persistent://public/default/p1-real-client-27929-worker-4e3cfaad-300e-437f-befa-1e3205c2d2a2, ledgerId=9, entryId=3, normalizedBatchIndex=0, batchSize=1, entryKind=NON_BATCH, brokerEntryTimestampEpochMs=1786886946158]
Pulsar Worker Publish Admission response-loss smoke passed: the real Shard Log mutation was persisted, its local append response was discarded, and exact source replay recovered the PUBLISHING admission
Pulsar Worker source-applied physical publish passed: Admission source ledger=9/3, typed PULSAR_SEND_ACK target ledger/entry=10/0, Outcome source ledger=9/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker authority smoke passed: real Oxia session-bound lease
BUILD SUCCESSFUL in 15s
Pulsar Worker Publish Admission response-loss E2E passed: the real Shard Log mutation was persisted, its append response was discarded, and exact source replay recovered the PUBLISHING admission.
```

This proves the bounded real-Broker/real-Oxia recovered `UNKNOWN` Publish
Admission branch and source-applied physical publish. It is controlled
client-side response loss after real Broker persistence, not raw socket,
process/Broker crash, multi-Broker reactivation, combined Gateway failover,
multi-shard, checkpoint REAPING, chaos or V1 release evidence. The runner's
exact cleanup checks found no containers, images, volumes or networks for the
isolated projects.

## Route-driven multi-shard Worker placement with real Oxia

The real Oxia Route/Assignment smoke also covers two Route partitions and two
workers. It publishes partition 0 to `worker-a`, reflects that committed
capacity before placing partition 1 on `worker-b`, rereads both exact
assignment records through the signed Route projection, and withdraws both
records by identity.

Run it with:

```bash
NEREUS_DELAY_OXIA_E2E_PORT=16659 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-multishard-20260816 \
  bash e2e/run-oxia-real-service.sh
```

The source-bound run was Delay `e629a404`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, project
`nereus-delay-v1-oxia-e2e-1786887413-34183`, endpoint `127.0.0.1:16659`, and
temporary image
`sha256:e05630a933783a3925150ad1a1ca38869249d06a9983a6a7c4ed1e0bef98c460`.
The test XML recorded two tests with zero skips/failures/errors:

```text
Oxia signed Route -> Worker assignment smoke passed: routeRevision=1, assignmentRevision=1, session-bound CAS
Oxia signed Route -> multi-shard Worker placement smoke passed: routeRevision=1, shards=2, workers=2, session-bound CAS
BUILD SUCCESSFUL in 1m 20s
Dockerized Oxia real-service smoke passed for 37a17bef17202d5fd6e23282da5fd26d94865484
```

This is positive evidence for real Oxia Route-driven multi-shard placement and
per-shard assignment CAS. It is not a full two-shard native Kafka/Pulsar
Worker runtime receipt: per-shard source ownership, Owner Lease/catch-up/ACK,
scheduler fairness, raw chaos and V1 release readiness remain open. The exact
temporary Oxia image was removed after the run; no project container/network
remained.

## Kafka source Fetch response-loss receipt

The focused source fault mode composes the locked K1 client with a real
three-Broker KRaft cluster:

```bash
NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-gradle \
  bash e2e/run-kafka-real-client-e2e.sh
```

The source-bound receipt passed at Delay
`8f1116abad2bd77e2f384c04411dabaeb70b4f72`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:b0fcef7eb6f8350af6c22d333de889155acf4b1ec157887266568fc78beada0e`,
and Compose project `nereus-delay-kafka-e2e-1786879840-36136` on ports
`19228,19229,19230`.

It printed:

```text
Kafka source Fetch response-loss smoke passed: responseDiscardedAfterFetch=true, replayOffset=0, secondOffset=1, fetchLso=2, committedAfterReplay=2
BUILD SUCCESSFUL
Kafka source Fetch response-loss E2E passed: real read_committed Fetch v13 response was discarded before ACK, exact source replay and LSO coverage were recovered.
```

This closes only controlled client-side response loss after a real Fetch: the
same group replayed exact offset 0, then ACKed offsets 0 and 1 through group
offset 2 with LSO 2. It does not cover raw socket loss, retention-floor
recovery, coordinator/process/Broker crash cuts, multi-shard placement,
checkpoint publication, chaos or V1 release readiness.

## Kafka source retention-floor receipt

The focused retention mode composes the locked K1 client with a real
three-Broker KRaft cluster:

```bash
NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-retention-floor-gradle-2 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The post-commit receipt passed at Delay `d8dc5f45`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
and Compose project `nereus-delay-kafka-e2e-1786880647-45643` on ports
`19235,19236,19237`.

It printed:

```text
Kafka source retention-floor smoke passed: oldOffset=0, retentionFloor=20, endOffset=21, staleOffsetRejected=true, floorFetchOffset=20, fetchLso=21
BUILD SUCCESSFUL
Kafka source retention-floor E2E passed: real Broker retention advanced the earliest offset, stale source offset was rejected, and the current floor remained readable through guarded Fetch v13 with LSO.
```

This closes the bounded real-Broker retention-floor recovery slice: twenty
guarded records were produced, Broker retention advanced the earliest offset
to `20`, a stale offset `0` Fetch failed closed with typed
`OFFSET_OUT_OF_RANGE`, and the fresh floor record remained readable with LSO
`21`. Raw socket loss, coordinator/process/Broker crash cuts, multi-shard
placement, checkpoint publication, chaos and V1 release readiness remain
outside this receipt.

## Kafka source process-crash recovery receipt

The focused process-crash mode composes the locked K1 client with a real
three-Broker KRaft cluster:

```bash
NEREUS_DELAY_KAFKA_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-process-crash-e2e-receipt \
  bash e2e/run-kafka-real-client-e2e.sh
```

The post-commit receipt passed at Delay
`2bcaff5e0c0b15b819cbc614c166c47e19571be3`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
and Compose project `nereus-delay-kafka-e2e-1786881618-58469` on ports
`19561,19562,19563`.

It printed:

```text
Kafka source process-crash cut reached: fetchedOffsets=0,1, fetchLso=2, responseAcked=false, consumerClosed=false
Kafka source process-crash recovery smoke passed: crashExit=86, replayOffset=0, secondOffset=1, committedAfterRecovery=2
BUILD SUCCESSFUL in 5s
Kafka source process-crash recovery E2E passed: the crashed JVM fetched exact guarded records without ACK, and a fresh same-group process replayed offsets 0 and 1 before committing offset 2.
```

This closes the bounded isolated JVM process-crash recovery slice: the crash
process fetched exact guarded records through real Fetch v13 and halted before
ACK/close; the fresh same-group process replayed offsets `0` and `1`, then
committed offset `2`. It does not cover raw network/proxy/socket loss,
consumer-coordinator or Broker crash/leader-failover cuts, Worker crash during
apply/publish, multi-shard placement, checkpoint publication, chaos or V1
release readiness.

## Checkpoint REAPING with real Oxia and MinIO

The isolated checkpoint runner now executes the scheduled publication receipt
and a bounded real REAPING handoff:

```bash
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=16739 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=16749 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

The source-bound Delay commit is `d58ca4d7038c994c4415898b91362760a01896d0`;
Oxia is `37a17bef17202d5fd6e23282da5fd26d94865484`; MinIO is locked to
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`;
and the run used project `nereus-delay-oxia-minio-checkpoint-e2e-1786888377-45712`
on `16739/16749`.

The real REAPING method recorded:

```text
Oxia + MinIO checkpoint REAPING authority passed: real Owner abandonment=true, real Intent PENDING_UPLOAD->REAPING=true, exact-version prefix sweep=2, finalEmptyPrefix=true, localProviderOwnershipClosed=true
```

This closes the bounded real-Oxia Owner-abandonment/Intent-CAS to real-MinIO
exact-version prefix-sweep path. The two deleted versions are the checkpoint
file and manifest, and the adapter performs a final empty-prefix listing. The
local provider-generation fence is evidence for the configured local horizon,
not provider-side quiescence/consistency attestation. RecoveryPin competition,
production cross-record transaction, source-ordered delete confirmation,
response-loss retry, multi-shard runtime, raw chaos and V1 release gates remain
open. The runner removes its exact containers, network, volume and temporary
Oxia image; the locked MinIO base image remains for reuse.

## Kafka Broker SIGKILL Worker recovery receipt

The focused Broker process-crash mode uses a real three-Broker KRaft cluster,
real Oxia Worker authority, and a same-topic survivor resume:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16679 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-kafka-real-client-e2e.sh
```

The source-bound run was Delay
`2a560a9d3f288b08bd02e139c52f4cfe6fda8ff3`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and projects
`nereus-delay-kafka-e2e-1786888793-51634` /
`nereus-delay-kafka-oxia-e2e-1786888793-51634`.

The runner killed `kafka-1` with `SIGKILL` after guarded Worker preparation,
resumed the same topic through `kafka-2,kafka-3` with real Oxia authority, and
started `kafka-1` again. It printed:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Broker process-crash recovery E2E passed: kafka-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia authority, and kafka-1 rejoined afterward.
```

This is a bounded Broker-process recovery receipt, not raw network/proxy/socket
chaos, controller/coordinator leader-failover evidence, Worker crash recovery,
production multi-shard runtime or V1 release readiness. Exact post-run checks
found no containers, networks, volumes or temporary Kafka/Oxia images for the
two isolated projects.

## Kafka Broker network-partition Worker recovery receipt

The focused mode uses a real three-Broker KRaft cluster and cuts only the
Docker network membership of the still-running `kafka-1` container:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16689 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-kafka-real-client-e2e.sh
```

The source-bound run was Delay
`5460746c74b2a4cc05f9ecfb71c5d2a285828380`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and projects
`nereus-delay-kafka-e2e-1786889717-63599` /
`nereus-delay-kafka-oxia-e2e-1786889717-63599`.

The runner verified survivor topic leaders with the Java Admin client, then
completed source-only Worker recovery/apply/ACK/final-checkpoint through
`kafka-2,kafka-3` and reconnected `kafka-1`. It printed:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Broker network-partition recovery E2E passed: kafka-1 stayed alive but was disconnected from the Compose network after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia Worker authority and source apply/ACK/checkpoint, and kafka-1 reconnected afterward.
```

This is a bounded real Docker-network partition receipt. The survivor run
deliberately skips physical destination publish, so raw packet/proxy/socket
chaos, destination egress under partition, controller/coordinator leader
proof, production multi-shard runtime and V1 release readiness remain open.
Exact post-run checks found no containers, networks, volumes or temporary
Kafka/Oxia images for the two isolated projects.
