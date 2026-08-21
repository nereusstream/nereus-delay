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

## 2026-08-21 Pulsar multi-Broker failover and Worker admission response-loss receipts

The following focused cuts were rerun after Delay commits
`16c3792e`, `2f57b5f8` and `b7b156e6`. The current Delay source lock for the
next full matrix is `b7b156e6`.

The real Pulsar multi-Broker process-crash receipt is:

```text
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r7-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r7-state/after-fresh-process.json
```

The before dump was read from survivor Admin `31481`, with topic
`persistent://public/default/p1-multi-worker-72521`, ledger IDs `[-1,3]`, one
entry, confirmed position `3:0` and Broker PID `73216`. After Broker-1 was
SIGKILLed, the fresh Worker recovered through Broker-2/Admin `31483`; the
after dump retained the same topic, cluster and confirmed position, showed
ledger IDs `[-1,3,4]`, one entry and Broker PID `73337`, and Broker-1 rejoined.
Ledger `4` is an allowed managed-ledger extension during failover; the audit
requires the pre-failure ledger set to remain a subset of the post-failure
set. The Worker then completed source-applied physical publish, real Oxia
authority and the vertical smoke.

The real Pulsar Worker Publish Admission response-loss process-crash receipt
is:

```text
/private/tmp/nereus-delay-pulsar-worker-admission-response-loss-20260821-r1-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-worker-admission-response-loss-20260821-r1-state/after-fresh-process.json
```

Before SIGKILL, the forced durable dump reported `PUBLISHING` with
`durable_store_read=true`, `dump_forced=true` and `outcome_applied=false`.
After a fresh Worker JVM (`75056 -> 75161`), the same
`publish_attempt_id`, message ID, DB identity and Store incarnation were
reopened; the state became `PUBLISHED`, `outcome_applied=true`, and the exact
typed destination receipt, source-applied Outcome and payload readback
completed. An independent field comparison returned
`INDEPENDENT_FIELDS_PASS`.

These receipts close the two focused durable/fresh-process cuts at the stated
boundary. They do not by themselves promote the bounded matrix to certified
release evidence, and they do not close multi-shard placement, full broker
response-loss/retention recovery or the V1 release gate.

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

## Kafka Worker JVM SIGKILL recovery receipt

The focused Worker-process cut uses a real three-Broker KRaft cluster and real
Oxia. The first Worker JVM opens the guarded source/runtime and announces a
gate while the next source record is still unACKed. The harness sends `SIGKILL`
to the exact recorded PID, then starts a fresh JVM against the same local Store
root and the same source topic:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_WORKER_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16709 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-worker-process-crash-gradle-20260816 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The source-bound Delay commit was `d35dce96`; Kafka is locked to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
with client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3` and
broker image
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`.
Oxia is `37a17bef17202d5fd6e23282da5fd26d94865484`. The isolated projects
were `nereus-delay-kafka-e2e-1786890291-72188` /
`nereus-delay-kafka-oxia-e2e-1786890291-72188` on
`19280,19281,19282/16709`; the temporary Oxia image was
`sha256:803fdb3a48af0411170bc96e81bcb39bd5674c8766a105973dfed8cc46bcc449`.

It reported:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Worker process-crash recovery E2E passed: a real Worker JVM was SIGKILLed after opening the guarded source/runtime with the next record unACKed, and a fresh JVM reopened the exact local Store, reacquired the real Oxia lease, replayed and ACKed the source record, and published the final checkpoint.
```

This receipt is limited to OS-process Worker source replay/ACK/checkpoint
recovery. It does not prove a crash during destination publish, raw
packet/proxy/socket chaos, controller/coordinator leader failover,
production multi-shard runtime or V1 release readiness. The named Compose
containers, networks, volumes and temporary images were absent after cleanup;
base images were retained and no global `docker prune` was run.

## Kafka Worker durable-apply-before-ACK SIGKILL recovery receipt

The focused ACK-boundary cut uses Delay `2cfc207f`, a real three-Broker K1
cluster and real Oxia. The first Worker JVM opens the guarded source/runtime and
reaches a test-only gate after its local RocksDB WriteBatch is durable but
before Kafka `commitSync` starts. The harness sends `SIGKILL` to the exact
recorded PID, then starts a fresh JVM against the same local Store root and
source topic. The replacement reacquires the real Oxia lease, replays and
deduplicates the source record, ACKs it and publishes the final checkpoint.

Run command:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16719 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-worker-ack-process-crash-gradle-20260816 \
KAFKA_DELAY_WORKER_ACK_PROCESS_CRASH_TOPIC=nereus-delay-worker-ack-crash-live-20260816 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The live receipt used K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, projects
`nereus-delay-kafka-e2e-1786890684-77735` /
`nereus-delay-kafka-oxia-e2e-1786890684-77735`, ports
`19327,19328,19329/16719`, and temporary Oxia image
`sha256:6b8082d3b205230306c243b332a02c1c9d3ecd9c4286ae22b90743a0fc80d26c`.

It reported:

```text
Kafka Worker ACK process-crash cut reached: pid=78620, storeWriteBatchDurable=true, kafkaCommitSyncStarted=false
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Worker ACK process-crash recovery E2E passed: the Worker Store WriteBatch was durable before SIGKILL and before Kafka commitSync ACK, and a fresh JVM replayed the exact source record through real Oxia authority, dedupe, ACK and final checkpoint.
```

This receipt is limited to the Store-durable-before-source-ACK Worker replay
boundary. It does not prove a crash during destination publish, raw
packet/proxy/socket chaos, controller/coordinator leader failover, production
multi-shard runtime or V1 release readiness. The named Compose containers,
networks, volumes and temporary images were absent after exact cleanup; base
images were retained and no global `docker prune` was run.

## Kafka raw TCP Broker endpoint-cut Worker recovery

Run the focused real-service slice with:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16769 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-broker-tcp-cut-gradle-20260816-r5 \
KAFKA_DELAY_BROKER_TCP_CUT_TOPIC=nereus-delay-worker-broker-tcp-cut-live-20260816-r5 \
  bash e2e/run-kafka-real-client-e2e.sh
```

This starts three real K1 Brokers and real Oxia. Before the cut, the local raw
proxy forwards Broker-1's public endpoint to its bound listener. The placement
smoke then moves the one-partition source leader and the exact
`__consumer_offsets` partition for the fixed Worker group to
`[2,3,1]`/leader `2`, without stopping Broker-1. The cut closes existing
sockets, rejects one new Broker-1 endpoint connection, and forwards later
connections on that endpoint to Broker-2. The fresh Worker must complete the
real Oxia assignment/Owner Lease path, guarded source replay/apply, ACK and
final checkpoint.

The successful receipt recorded source `leader=2, replicas=[2, 3, 1]`,
coordinator `offsetsPartition=15, leader=2, replicas=[2, 3, 1]`, the vertical
Worker receipt and the raw endpoint-cut E2E receipt. The runner fails closed
unless pre-cut forwarding, cut acknowledgement, post-cut rejection and
post-cut handoff marker files all exist.

This is an explicit endpoint fault/handoff harness. It does not prove automatic
Kafka controller/coordinator failover, Broker crash recovery, Docker network
partition recovery, production proxy behavior, multi-shard runtime, the full
chaos matrix or V1 release readiness. Cleanup removes only the exact Compose
projects, temporary networks/volumes and run-created Kafka/Oxia images; reusable
base images are retained and no global `docker prune` is performed.

## Pulsar Gateway large-payload multi-Broker reactivation

Run the source-bound combined Gateway/Oxia/Worker/MinIO slice with:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-reactivation-gradle-20260816-r2 \
  ./e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The authoritative receipt locks Delay to
`49665a75041ea05cd7b47e887c9e28fa08647b9`, P1 to
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1 distribution SHA-256 to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image to `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO to
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The run uses one physical source partition. After Gateway Commit/readback it
stops Broker-1, opens a fresh guarded source connection, seeks after the
committed source position, fences/quiesces the old Owner/runtime, and publishes
and acquires a canonical successor Assignment/Owner before the Worker resumes.

Required live markers include:

```text
Pulsar source reactivation successor accepted: oldGeneration=2, newGeneration=3, assignmentRevision=2, ownerEpoch=2
Pulsar Worker source-applied physical publish passed: Admission source ledger=6/0, typed PULSAR_SEND_ACK target ledger/entry=4/0, Outcome source ledger=6/1, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=3/2, commit=3/3, exactGatewayIdempotency=true, sourceRecords=6
```

This is bounded reactivation evidence, not automatic Pulsar controller/
coordinator failover, multi-shard evidence or V1 release approval. Cleanup is
exactly scoped to the Compose project, its networks/volumes/orphans and
run-created temporary images; reusable base images remain and no global Docker
prune is run. The runner emitted a topic-delete cleanup warning, but post-run
checks found no Docker resources or matching temporary images for the run.

## Kafka current Large-payload Gateway-to-destination authority

Run the current-source Kafka destination slice with:

```bash
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-large-payload-gradle-20260816-r2 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-payload-destination-20260816-r2 \
  ./e2e/run-large-payload-gateway-e2e.sh
```

The receipt locks Delay to `eb8e4a9df859316253202ba3abfb48236bf64196`, Kafka
to `05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO to
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
It uses one physical source partition and proves real Gateway/Oxia/Worker/
MinIO authority plus Kafka destination typed receipt and exact payload
readback. The successful markers are:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=4, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=5, exact payload readback
Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload + Kafka destination authority E2E passed
```

The runner removes only the exact Compose project, its temporary networks,
volumes and run-created Kafka/Oxia images; reusable base images remain and no
global Docker prune is run.

## Current Oxia Route-driven multi-shard placement

Refresh the current real Oxia placement receipt with:

```bash
NEREUS_DELAY_OXIA_E2E_PORT=16659 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-multishard-20260816-current \
  bash e2e/run-oxia-real-service.sh
```

The source lock is Delay `b059d99aef1793f56c4b33d4293ec141e20c4d96` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The run proves two signed Route
partitions placed across two workers with session-bound Assignment CAS and
exact withdrawal. It is not yet a two-native-source Worker fleet proof;
per-shard source ownership/catch-up/ACK, scheduler fairness, raw chaos and
V1 release readiness remain open.

The runner's project/container/network cleanup was exact. Its run-created
Oxia image required one explicit `docker image rm` after the runner, and was
then absent; reusable base images remain and no global Docker prune is run.

## Kafka native multi-shard Worker fleet

Run the source-bound Kafka two-shard Worker path with the real K1 Broker
cluster and real Oxia authority:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_MULTI_SHARD_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-multishard-20260816-c6b2d0ea \
GRADLE_USER_HOME=/tmp/nereus-delay-kafka-multishard-20260816-c6b2d0ea \
  bash e2e/run-kafka-real-client-e2e.sh
```

At Delay `c6b2d0ea`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, the runner published one
signed two-partition Route, crossed two guarded Fetch barriers, admitted two
real Oxia Assignment/Owner paths and attached two native guarded source
consumers to one `WorkerShardFleetRuntime`. The current receipt is:

```text
Kafka signed Route -> two guarded Fetch barriers -> Oxia multi-shard Assignment/Owner -> one Worker fleet -> RocksDB apply/ACK/checkpoint smoke passed: fetchPartitions=2, routeRevision=1, assignmentRevisions=[1, 1], workers=[kafka-route-worker-a, kafka-route-worker-b], sourceBarriers=[1, 1]
Kafka native multi-shard Worker fleet E2E passed: one signed Route covered two guarded Fetch barriers, two real Oxia Assignment/Owner Lease CAS paths admitted two native source consumers, one fair fleet applied/ACKed both partitions, and both final checkpoints/assignments were released.
```

This is positive source-bound Kafka evidence for two native Worker shards,
including per-shard recovery/apply/ACK/checkpoint and exact withdrawal. It does
not claim native Pulsar multi-shard production, multiple Worker processes, raw
chaos completeness or V1 release readiness. The run used isolated Kafka and
Oxia Compose projects; post-run checks found no containers, networks, volumes
or matching temporary images. Reusable base images remain and no global Docker
prune is used.

## Pulsar native multi-shard Worker fleet

Run the source-bound Pulsar two-shard Worker path with the real P1 Broker and
real Oxia authority:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_MULTI_SHARD_ONLY=1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-multishard-20260817-r2 \
GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-multishard-20260817-r2 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

At Delay `c2003627`, Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, the runner published one
signed two-partition Route, crossed two guarded SUBSCRIBE barriers, admitted
two real Oxia Assignment/Owner paths and attached two native guarded source
consumers to one `WorkerShardFleetRuntime`. The current receipt is:

```text
Pulsar signed Route -> two guarded SUBSCRIBE barriers -> Oxia multi-shard Assignment/Owner -> one Worker fleet -> RocksDB apply/ACK/checkpoint smoke passed: subscribePartitions=2, routeRevision=1, assignmentRevisions=[1, 1], workers=[pulsar-route-worker-b, pulsar-route-worker-a], sourceBarriers=[9/0, 10/0]
Pulsar native multi-shard Worker fleet E2E passed: one signed Route covered two guarded SUBSCRIBE barriers, two real Oxia Assignment/Owner Lease CAS paths admitted two native source consumers, one fair fleet applied/ACKed both partitions, and both final checkpoints/assignments were released.
```

This is positive source-bound Pulsar evidence for two native Worker shards on
one real P1 Broker, including per-shard recovery/apply/ACK/checkpoint and exact
withdrawal. It does not claim Pulsar multi-Broker failover, multiple Worker
processes, raw chaos completeness or V1 release readiness. The isolated
Pulsar/Oxia Compose projects left no containers, networks, volumes or matching
temporary images; reusable base images remain and no global Docker prune is
used.

## Kafka Broker network-partition Worker recovery receipt (current source)

Run the current-source Kafka Broker network-partition cut with real Oxia:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16761 \
KAFKA_DELAY_BROKER_NETWORK_PARTITION_TOPIC=nereus-delay-broker-network-partition-20260817 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-network-partition-20260817 \
GRADLE_USER_HOME=/tmp/nereus-delay-kafka-network-partition-20260817 \
  bash e2e/run-kafka-real-client-e2e.sh
```

At Delay `35745db08672f1bf2e3178419422a46741da20d1`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, the real three-Broker
KRaft run disconnected live `kafka-1` from the Compose network, verified
survivor topic leaders through the Admin API, resumed the same guarded Worker
source through `kafka-2,kafka-3`, and reconnected `kafka-1`. The current output
was:

```text
Kafka survivor topic leader recovery passed: leaders={nereus-delay-broker-network-partition-20260817=2, nereus-delay-worker-destination-topic=3, nereus-delay-worker-destination-topic-receipt=2}
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Broker network-partition recovery E2E passed: kafka-1 stayed alive but was disconnected from the Compose network after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia Worker authority and source apply/ACK/checkpoint, and kafka-1 reconnected afterward.
```

This is a bounded real Docker-network partition receipt for source Worker
recovery. It does not claim destination egress during the survivor window, raw
packet/proxy/socket chaos, controller/coordinator failover beyond the topic
leader check, production multi-shard chaos or V1 release readiness. The exact
Kafka project was `nereus-delay-kafka-e2e-1786896942-56285` and the exact Oxia
project was `nereus-delay-kafka-oxia-e2e-1786896942-56285`; post-run checks
found no containers, networks, volumes or matching temporary images, reusable
base images were retained and no global Docker prune was used.

## Kafka source Fetch response-loss receipt (current source)

Run the focused receipt against the current Delay source:

```bash
NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-gradle-20260817-r1 \
KAFKA_DELAY_E2E_SOURCE_TOPIC=nereus-delay-fetch-response-loss-live-20260817-r1 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The current-source run locks Delay to
`a3bb8462edc3d4e32006f5d98af958d1c8d7ef18`, Kafka to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
the K1 client SHA-256 to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
the broker image ID to
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`.
The exact Kafka project was
`nereus-delay-kafka-e2e-1786898037-72717` on ports
`19309,19310,19311`.

The receipt was:

```text
Kafka source Fetch response-loss smoke passed: responseDiscardedAfterFetch=true, replayOffset=0, secondOffset=1, fetchLso=2, committedAfterReplay=2
BUILD SUCCESSFUL in 50s
Kafka source Fetch response-loss E2E passed: real read_committed Fetch v13 response was discarded before ACK, exact source replay and LSO coverage were recovered.
```

This is a bounded controlled client-side response-loss receipt after a real
Fetch. It does not cover raw socket loss, retention-floor recovery,
coordinator/Broker crash, multi-shard placement, checkpoint publication,
chaos or V1 release readiness. Exact post-run checks found no matching
containers, networks, volumes or temporary image tag/image ID; reusable base
images were retained and no global Docker prune was run.

## Kafka source retention-floor receipt (current source)

Run the focused retention receipt:

```bash
NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-gradle-20260817-r1 \
KAFKA_DELAY_E2E_SOURCE_TOPIC=nereus-delay-retention-floor-live-20260817-r2 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The first attempt stopped before Gradle because a new wrapper cache hit a
transient TLS EOF; the exact runner cleanup still completed. The successful
retry used the populated cache and locked Delay to
`a3bb8462edc3d4e32006f5d98af958d1c8d7ef18`, Kafka to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
the K1 client SHA-256 to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
the broker image ID to
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`.
The exact Kafka project was
`nereus-delay-kafka-e2e-1786898140-73898` on ports
`19490,19491,19492`.

The successful receipt was:

```text
Kafka source retention-floor smoke passed: oldOffset=0, retentionFloor=20, endOffset=21, staleOffsetRejected=true, floorFetchOffset=20, fetchLso=21
BUILD SUCCESSFUL in 30s
Kafka source retention-floor E2E passed: real Broker retention advanced the earliest offset, stale source offset was rejected, and the current floor remained readable through guarded Fetch v13 with LSO.
```

This is bounded real-Broker retention-floor recovery with test-accelerated
retention. It does not claim disk ENOSPC, raw socket/process chaos,
controller/coordinator failover, multi-shard placement, checkpoint
publication or V1 release readiness. Exact post-run checks found no matching
containers, networks, volumes or temporary image tag/image ID; reusable base
images were retained and no global Docker prune was run.

## Kafka Worker durable-apply-before-ACK process-cut receipt (current source)

Run the current-source Worker ACK process cut:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY=1 \
KAFKA_BROKER_1_PORT=19661 \
KAFKA_BROKER_2_PORT=19662 \
KAFKA_BROKER_3_PORT=19663 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16763 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-worker-ack-crash-20260817 \
GRADLE_USER_HOME=/tmp/nereus-delay-kafka-worker-ack-crash-20260817 \
KAFKA_DELAY_WORKER_ACK_PROCESS_CRASH_TOPIC=nereus-delay-worker-ack-crash-live-20260817 \
  bash e2e/run-kafka-real-client-e2e.sh
```

At Delay `ade0c813bb8919793eecdd2e07cf76073432237f`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, the first Worker JVM
reached a gate after the local WriteBatch was durable and before Kafka
`commitSync`; the harness SIGKILLed it and a fresh JVM reopened the exact Store
root under the real Oxia lease. The receipt was:

```text
Kafka Worker ACK process-crash cut reached: pid=65541, storeWriteBatchDurable=true, kafkaCommitSyncStarted=false
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker ACK process-crash recovery E2E passed: the Worker Store WriteBatch was durable before SIGKILL and before Kafka commitSync ACK, and a fresh JVM replayed the exact source record through real Oxia authority, dedupe, ACK and final checkpoint.
```

This is a bounded Worker process-cut receipt for durable apply before source
ACK. It does not claim destination-publish crash recovery, raw network chaos,
controller/coordinator failover, production multi-shard fault coverage or V1
release readiness. The exact Kafka project was
`nereus-delay-kafka-e2e-1786897528-64796` and exact Oxia project was
`nereus-delay-kafka-oxia-e2e-1786897528-64796`; post-run checks found no
containers, networks, volumes or matching temporary images, reusable base
images were retained and no global Docker prune was used.

## Kafka Broker process-crash recovery receipt (current source)

Run the current-source Broker process-crash slice:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY=1 \
KAFKA_BROKER_1_PORT=19761 \
KAFKA_BROKER_2_PORT=19762 \
KAFKA_BROKER_3_PORT=19763 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16764 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-broker-crash-20260817 \
GRADLE_USER_HOME=/tmp/nereus-delay-kafka-broker-crash-20260817 \
KAFKA_DELAY_BROKER_PROCESS_CRASH_TOPIC=nereus-delay-worker-broker-crash-live-20260817 \
  bash e2e/run-kafka-real-client-e2e.sh
```

At Delay `13857e57cee134c2bc0fcf20a4d8b988fbe0f02a`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, the harness SIGKILLed
`kafka-1` after guarded Worker preparation. The survivor run through
`kafka-2,kafka-3` completed real Oxia authority, source apply/ACK, physical
Kafka destination publish and typed receipt/readback, then restarted and
readiness-checked `kafka-1`:

```text
Kafka Worker restart preparation passed: one guarded record persisted before broker failover
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, and final checkpoint
Kafka Broker process-crash recovery E2E passed: kafka-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia authority, and kafka-1 rejoined afterward.
```

This is a bounded current-source Broker-process crash receipt covering the
Worker source/destination path. It does not claim raw endpoint/network fault
injection, controller/coordinator failover, production multi-shard chaos or V1
release readiness. The exact Kafka project was
`nereus-delay-kafka-e2e-1786897707-67896` and exact Oxia project was
`nereus-delay-kafka-oxia-e2e-1786897707-67896`; post-run checks found no
containers, networks, volumes or matching temporary images, reusable base
images were retained and no global Docker prune was used.

## Kafka raw TCP Broker endpoint-cut Worker recovery receipt (current source)

Run the current-source raw TCP endpoint-cut slice:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY=1 \
KAFKA_BROKER_1_PORT=19461 \
KAFKA_BROKER_2_PORT=19462 \
KAFKA_BROKER_3_PORT=19463 \
KAFKA_BROKER_1_BIND_PORT=19561 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16762 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-broker-tcp-cut-20260817 \
GRADLE_USER_HOME=/tmp/nereus-delay-kafka-broker-tcp-cut-20260817 \
KAFKA_DELAY_BROKER_TCP_CUT_TOPIC=nereus-delay-worker-broker-tcp-cut-live-20260817 \
  bash e2e/run-kafka-real-client-e2e.sh
```

At Delay `47fa6620e7816dbd13ea393b42891a53286009ec`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, the placement smoke put
the source leader and selected group coordinator partition on Broker-2 with
replicas `[2,3,1]` while Broker-1 stayed alive. The raw proxy then rejected one
Broker-1 endpoint connection and handed a later connection to Broker-2. The
fresh Worker resumed through the full bootstrap list under real Oxia authority
and completed source apply/ACK/final checkpoint:

```text
Kafka raw TCP cut source leader placement passed: topic=nereus-delay-worker-broker-tcp-cut-live-20260817, leader=2, replicas=[2, 3, 1], broker1Alive=true
Kafka raw TCP cut group coordinator placement passed: group=nereus-delay-worker-broker-tcp-cut-live-20260817-group, offsetsPartition=6, leader=2, replicas=[2, 3, 1], broker1Alive=true
Kafka Worker raw TCP Broker-endpoint cut recovery E2E passed: Broker-1 remained alive, the source and selected group-coordinator partitions were explicitly placed on Broker-2, the raw proxy rejected Broker-1 once and handed later connections to Broker-2, and a fresh Worker resumed the same source through the full bootstrap list with real Oxia authority and source apply/ACK/checkpoint.
```

The runner also required pre-cut forwarding, cut acknowledgement, post-cut
rejection and post-cut handoff marker files. This is a bounded raw endpoint
fault/handoff receipt; it does not claim controller/coordinator automatic
failover, Broker crash recovery, Docker network partition, destination egress
under the cut, production multi-shard chaos or V1 release readiness. The exact
Kafka project was `nereus-delay-kafka-e2e-1786897339-61592` and exact Oxia
project was `nereus-delay-kafka-oxia-e2e-1786897339-61592`; post-run checks
found no containers, networks, volumes or matching temporary images, reusable
base images were retained and no global Docker prune was used.

## Pulsar multi-Broker Worker failover receipt (current source)

Run the current-source two-Broker Worker failover slice:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=16765 \
PULSAR_BROKER_1_PORT=22080 \
PULSAR_WEB_1_PORT=22081 \
PULSAR_BROKER_2_PORT=22082 \
PULSAR_WEB_2_PORT=22083 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-reactivation-gradle-20260816-r2 \
PULSAR_DELAY_MULTI_BROKER_RESTART_TOPIC=nereus-delay-pulsar-multi-broker-live-20260817-r1 \
PULSAR_DELAY_MULTI_BROKER_DESTINATION_TOPIC=nereus-delay-pulsar-multi-destination-live-20260817-r1 \
  bash e2e/run-pulsar-multi-broker-failover-e2e.sh
```

The current-source run locks Delay to
`19577006e4c104b2934617719b711aa5d549ed27`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution SHA-256 to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
the P1 image ID to
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`.
The exact P1 project was
`nereus-delay-pulsar-multi-e2e-1786898570-79450` on ports
`22080,22081,22082,22083`; the exact Oxia project was
`nereus-delay-pulsar-multi-oxia-e2e-1786898570-79450` at
`127.0.0.1:16765`.

The receipt was:

```text
Pulsar Worker restart preparation passed: one guarded record persisted before broker restart
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/2, typed PULSAR_SEND_ACK target ledger/entry=4/0, Outcome source ledger=3/3, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=2/0, active apply ledger/entry=3/0, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker authority smoke passed: real Oxia session-bound lease
Pulsar multi-Broker failover E2E passed: same-topic guarded Worker resumed through broker-2 after broker-1 stop, applied the source record, completed provider-driven physical Publish, ACKed the source and released its final checkpoint and owner assignment.
```

This is bounded two-Broker P1 failover evidence with real Oxia authority and
physical destination readback. It does not claim automatic Pulsar
controller/coordinator failover, raw socket/network chaos, Gateway ingress,
multi-shard production placement, the full chaos matrix or V1 release
readiness. Exact post-run checks found no P1/Oxia project containers,
networks or volumes and no matching P1 image; the run-created Oxia image tag
and ID were explicitly removed, reusable base images were retained and no
global Docker prune was run.

## Kafka current-source Large-payload Gateway-to-destination authority

Run the current-source Kafka production-authority composition with:

```bash
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-large-payload-gradle \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=25130 \
KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=25131 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=25132 \
NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=26100 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=27100 \
NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=28100 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC=nereus-delay-large-payload-live-20260817-r2 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-payload-destination-live-20260817-r2 \
  bash e2e/run-large-payload-gateway-e2e.sh
```

The current-source receipt locks Delay to
`f3adc8cba4c78479f2daa883f0605136dc085f50`, K1 to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
and the isolated project to
`nereus-delay-large-payload-e2e-1786898894-84130`. The real Kafka/Oxia/Gateway/
MinIO path passed:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=4, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=5, exact payload readback
Kafka + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: activationOffset=0, barrierOffset=2, prepareOffset=KafkaSourcePosition[shardId=ShardId[routeIncarnation=0a18766bd5b24b43ae29a62e8b7e8df1, partition=0], authenticatedClusterId=MkU3OEVBNTcwNTJENDM2Qk, nativeTopicUuid=81c2e553-92d4-4ba7-954a-83fb227d3cce, offset=2, leaderEpoch=null, brokerLogAppendTimeEpochMs=1786898913930], commitOffset=KafkaSourcePosition[shardId=ShardId[routeIncarnation=0a18766bd5b24b43ae29a62e8b7e8df1, partition=0], authenticatedClusterId=MkU3OEVBNTcwNTJENDM2Qk, nativeTopicUuid=81c2e553-92d4-4ba7-954a-83fb227d3cce, offset=3, leaderEpoch=null, brokerLogAppendTimeEpochMs=1786898914695], providerVersion=295e66ce-feec-467c-a7cf-6db22e473dbf, exactGatewayIdempotency=true
Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload + Kafka destination authority E2E passed
```

This is one physical source partition and bounded production-authority
evidence, not the V1 release gate. Exact post-run checks found no project
containers, networks, volumes or temporary Kafka/Oxia images; the locked
MinIO base image was retained and no global Docker prune was run.

## Pulsar Gateway large-payload multi-Broker failover receipt (current source)

Run the current-source combined Gateway/Oxia/Worker/MinIO failover slice with:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-gradle \
PULSAR_LARGE_BROKER_1_PORT=29200 \
PULSAR_LARGE_WEB_1_PORT=29201 \
PULSAR_LARGE_BROKER_2_PORT=29202 \
PULSAR_LARGE_WEB_2_PORT=29203 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=29210 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=29211 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=29212 \
PULSAR_LARGE_PAYLOAD_TOPIC=nereus-delay-pulsar-large-payload-live-20260817-r1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-pulsar-large-destination-live-20260817-r1 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The current-source receipt locks Delay to
`f3adc8cba4c78479f2daa883f0605136dc085f50`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
and the isolated project to
`nereus-delay-pulsar-large-e2e-1786898952-84840`. The receipt was:

```text
Pulsar source reactivation successor accepted: oldGeneration=2, newGeneration=3, assignmentRevision=2, ownerEpoch=2
Pulsar Worker source-applied physical publish passed: Admission source ledger=5/0, typed PULSAR_SEND_ACK target ledger/entry=7/0, Outcome source ledger=5/1, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload multi-Broker failover E2E passed: broker-1 stopped after Gateway Commit/readback and the same source-applied physical Publish completed through broker-2
```

This is bounded two-Broker, one-physical-source-partition reactivation
evidence. The stop is a harness action, not automatic controller/coordinator
leader failover; Profile/Oxia credential authority, checkpoint REAPING/GC, the
full chaos matrix and V1 release readiness remain open. Exact post-run checks
found no P1/Oxia project containers, networks, volumes or temporary images;
the locked MinIO base image was retained and no global Docker prune was run.

## Current-source Checkpoint REAPING with real Oxia and MinIO

Run the current-source real checkpoint publication plus REAPING handoff with:

```bash
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=26300 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=27300 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-checkpoint-20260817 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

The current-source run locks Delay to
`f3adc8cba4c78479f2daa883f0605136dc085f50`, Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`, and the locked MinIO digest to
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The exact project was
`nereus-delay-oxia-minio-checkpoint-e2e-1786899309-90091`; it used Oxia
`26300`, MinIO `27300`, and run-created Oxia image ID
`sha256:2a9b1dcff9c104121084556f300b7222eeeb6f2493d1056a6454560e041b4353`.

The receipt was:

```text
Oxia + MinIO Worker checkpoint publication and REAPING E2E passed: real Oxia Intent/Catalog/Owner authority and real MinIO immutable objects
```

The test proves real Owner abandonment, Intent `PENDING_UPLOAD -> REAPING`,
provider quiescence, exact-version prefix deletion and empty-prefix reread.
It is bounded real-service evidence, not multi-worker disaster takeover,
external secret-manager rotation, full chaos or V1 release readiness. Exact
post-run checks found no project resources, temporary Oxia image or standalone
MinIO container; the locked MinIO base was retained and no global Docker prune
was run.

## Current-source Oxia Profile/Route authority and notification restart

Run the current-source main Oxia authority smoke with:

```bash
NEREUS_DELAY_OXIA_E2E_PORT=26420 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-real-20260817 \
  bash e2e/run-oxia-real-service.sh
```

The run locks Delay to
`d521aeb41c13d396716f8ac726a63bf4f96db4db`, Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`, and project
`nereus-delay-v1-oxia-e2e-1786899760-96148`. It passed the real Profile,
Owner/control/recovery, Route/Assignment and Gateway authority tests with
zero failures/errors; three opt-in tests were explicitly skipped.

Run the current-source restart cut with:

```bash
NEREUS_DELAY_OXIA_E2E_PORT=26410 \
NEREUS_DELAY_OXIA_ROUTE_RESTART=1 \
NEREUS_DELAY_OXIA_ROUTE_RESTART_ONLY=1 \
NEREUS_DELAY_OXIA_ROUTE_RESTART_NOTIFICATIONS=1 \
NEREUS_DELAY_OXIA_ROUTE_RESTART_PAUSE_SECONDS=2 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-real-20260817 \
  bash e2e/run-oxia-real-service.sh
```

The exact restart project was
`nereus-delay-v1-oxia-e2e-1786899721-95680`; the receipt was:

```text
Dockerized Oxia Route notification restart smoke passed: session rotation and notification stream recovery
```

This is bounded real Oxia session recovery evidence. The old-marker-visible
restart bug is fixed by `d521aeb41c13d396716f8ac726a63bf4f96db4db`, with
deterministic coverage in `OxiaSignedRouteSnapshotProviderTest`. Exact
post-run checks found no named containers/networks/volumes or temporary Oxia
images; no global Docker prune was used.

## V1 release-gate audit (2026-08-17)

Current source locks are Delay
`9e29af8e70fa4d84725d624959f377c271d9f319` for the current-source Gateway /
Broker revalidation (the Gate 8 tuple-bound dedupe implementation is
`59e085ed643e7e16658004aa73761079d6c036ae`), K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The prior audit snapshot was
synchronized at documentation commit `ea134e6acdd28f333e4d87444f020d6e2ca623f6`;
the current source-lock check and cross-repo validator pass. The release gate
is `NOT READY`.

Current real E2E evidence makes gates 1--4 and 10 partial only; the current
source Kafka and Pulsar Gateway/Broker large-payload revalidations pass. Gate 8 is now
partial: tuple-bound command hashing and durable dedupe conflict evidence pass,
but authenticated writer-before-reader assignment, cutover/downgrade and the
release artifact remain open. Gates 5--7 and 9 remain open for benchmark
matrix, capacity/SLO artifact, certified soak and operational
restore/fence/DLQ/uncertain/disaster drills; the repository now has a
fail-closed runbook draft and bounded local drills, but no release-candidate
operational certification. Full chaos and external
credential/provider failover are also open; positive bounded receipts must not
be used as release substitutes.

## Gateway Oxia session-churn recovery (current source)

Run the real Gateway/Oxia session-churn cut with:

```bash
NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN=1 \
NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_PAUSE_SECONDS=2 \
NEREUS_DELAY_OXIA_GATEWAY_E2E_PORT=26500 \
NEREUS_DELAY_GATEWAY_PORT=28500 \
NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME=/tmp/nereus-delay-gateway-e2e-20260817 \
  bash e2e/run-gateway-real-e2e.sh
```

The current-source run locks Delay to
`262254fcefea86f34cc153282706cfb2b16ad222` and Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`. The exact project was
`nereus-delay-gateway-e2e-1786900154-5135`; the real Oxia and Gateway ports
were `26500` and `28500`. The one-test report passed with zero failures and
zero errors:

```text
Gateway Oxia session churn E2E passed: stale admission/idempotency sessions failed closed and new sessions reread the exact durable outcome
```

The old Gateway process stayed alive across an Oxia stop/start. Stale
admission, idempotency and audit sessions failed closed after expiry; three
new session-bound clients reread the exact durable outcome with one
preparation, one physical attempt, zero live admission leases and two
digest-only audit records. The run-created Oxia image
`sha256:15ca9bafe5206cc9709255955a99a6b7761c85916163831ea248c350dea3335`
was removed after exact project cleanup. No matching project resources
remain; the locked MinIO base was retained and no global Docker prune was
used.

This is bounded single-node session-churn/recomposition evidence. Transparent
automatic reconnect, production multi-process Gateway HA, load, complete
crash/response-loss resolution, external credential/provider authority and
V1 release gates remain open.

## Object Store credential renewal with real Oxia and MinIO (current source)

Run the combined checkpoint publication, REAPING and credential-renewal
receipt with:

```bash
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=26320 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=27320 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-renewal-20260817 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

The current-source run locks Delay to
`e6d28a5b0fecc6c20daded998b1d324990fe95c2`, Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484` and MinIO to
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The exact project was
`nereus-delay-oxia-minio-checkpoint-e2e-1786900763-13173`; Oxia used `26320`,
MinIO `27320`, and the bucket was
`nereus-delay-checkpoints-1786900763-13173`.

All three selected real-service tests passed with zero failures and zero
errors:

```text
Oxia + MinIO Worker checkpoint publication passed: atomic Intent/Catalog=true, immutable object upload/download=true, checkpoint=00000000000000000000000000000003
Oxia + MinIO checkpoint REAPING authority passed: real Owner abandonment=true, real Intent PENDING_UPLOAD->REAPING=true, exact-version prefix sweep=2, finalEmptyPrefix=true, localProviderOwnershipClosed=true
Oxia + MinIO Object Store credential renewal E2E passed: real Profile Head/protection CAS renewed the exact lease and fenced the live adapter at secret rotation
```

The renewal test is control-plane evidence: it constructs the lease-gated
adapter against the locked MinIO Profile, renews the exact real-Oxia lease,
and rejects a live adapter renewal after Head rotation. It does not claim
external secret-manager resolution or a provider upload after renewal. The
runner removed its project resources, MinIO container and temporary Oxia
image; the locked MinIO base remained and no global Docker prune was used.

## Pulsar Worker Publish Admission response-loss (current source)

Run the focused real P1 Broker + real Oxia response-loss cut with:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=29410 \
PULSAR_BROKER_PORT=29400 \
PULSAR_WEB_PORT=29401 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-worker-admission-response-loss-oxia-20260817-r1 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The current-source receipt locks Delay to
`ef8ad3fcdb0765565b93036f901a45781f163bb0`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`. The isolated projects
were `nereus-delay-pulsar-e2e-1786901196-18866` and
`nereus-delay-pulsar-oxia-e2e-1786901196-18866`, using Pulsar
`29400/29401` and Oxia `29410`.

The focused task passed with `BUILD SUCCESSFUL`, zero failures and zero
errors:

```text
Pulsar Worker Publish Admission response-loss E2E passed: the real Shard Log mutation was persisted, its append response was discarded, and exact source replay recovered the PUBLISHING admission.
```

This is controlled client-side response loss after real Pulsar persistence.
Exact source replay resolves the same admission identity and continues
`PUBLISHING`; no retry mutation is appended. It is not raw socket loss or
process/Broker crash evidence and does not close multi-Broker, multi-shard,
REAPING, full chaos or V1 release gates. Exact postchecks found no project
containers, networks, volumes or matching P1/Oxia images. The locked MinIO
base remained and no global Docker prune was used.

## Pulsar Worker source ACK response-loss (current source)

Run the real P1 Broker + real Oxia Worker source-ACK response-loss cut with:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=29430 \
PULSAR_BROKER_PORT=29420 \
PULSAR_WEB_PORT=29421 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-worker-source-ack-response-loss-oxia-20260817-r1 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The current-source receipt locks Delay to
`75f451758c30c6eafc50b252bffdcef22f0137b4`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`. Projects were
`nereus-delay-pulsar-e2e-1786901489-23214` and
`nereus-delay-pulsar-oxia-e2e-1786901489-23214` on `29420/29421` and `29430`.

The run passed with zero failures/errors:

```text
Pulsar Worker source ACK response-loss E2E passed: real ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

Real ACK was accepted before the local response was discarded; the same
source record was ACKed on the next bounded Worker turn without a second
physical publish. This is controlled response loss, not raw socket or
process/Broker crash evidence. Exact project/image cleanup was empty; no
global Docker prune was used.

## Pulsar guarded destination SEND response-loss (current source)

Run the P1-only guarded destination response-loss cut with:

```bash
NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_DESTINATION_RESPONSE_LOSS_ONLY=1 \
PULSAR_BROKER_PORT=29440 \
PULSAR_WEB_PORT=29441 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-destination-response-loss-20260817-r1 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The current-source receipt locks Delay to
`75f451758c30c6eafc50b252bffdcef22f0137b4`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
project `nereus-delay-pulsar-e2e-1786901571-24129` on `29440/29441`.

The run passed with zero failures/errors:

```text
Pulsar destination committed response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and exact guarded payload readback.
```

This proves the exact guarded payload and typed destination evidence converge
after local SEND response loss. It does not claim Worker/Oxia authority, raw
socket or process/Broker crash recovery, multi-Broker, multi-shard, REAPING or
V1 release coverage. Exact project/image cleanup was empty; no global Docker
prune was used.

## Pulsar Worker JVM process-crash recovery (current source)

Run the current-source real P1 Broker + real Oxia Worker crash cut with:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH=1 \
NEREUS_DELAY_PULSAR_WORKER_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=29470 \
PULSAR_BROKER_PORT=29460 \
PULSAR_WEB_PORT=29461 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The receipt locks Delay to
`fdee96ca5e402bd725ff1454c1086b249e0ce8da`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`. Projects were
`nereus-delay-pulsar-e2e-1786902001-29815` and
`nereus-delay-pulsar-oxia-e2e-1786902001-29815` on `29460/29461` and `29470`.

The runner persisted one guarded record, opened the exact Store/runtime,
SIGKILLed the JVM at `sourceRuntimeReady=true,
nextSourceRecordUnacked=true`, then resumed with a fresh JVM and the same
Store root:

```text
Pulsar Worker process-crash recovery E2E passed: a real Worker JVM was SIGKILLed after opening the guarded source/runtime with the next record unACKed, and a fresh JVM reopened the exact local Store, reacquired the real Oxia lease, replayed and ACKed the source record, and published the final checkpoint.
```

This is bounded Worker JVM crash/reopen evidence. It does not cover a crash
during physical destination publish, raw socket/network chaos,
Broker/controller failover, multi-Worker placement, REAPING or V1 release
gates. Exact postchecks found no project resources, temporary images or crash
state; no global Docker prune was used.

## Pulsar multi-Broker Worker process-crash failover (current source)

Run the real P1 two-Broker + real Oxia process-crash cut with:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=29490 \
PULSAR_BROKER_1_PORT=29480 \
PULSAR_WEB_1_PORT=29481 \
PULSAR_BROKER_2_PORT=29482 \
PULSAR_WEB_2_PORT=29483 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 \
  bash e2e/run-pulsar-multi-broker-failover-e2e.sh
```

The current-source receipt locks Delay to
`123ffe6e6f70c7779a5712012f1836f8d792b43b`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`. The exact projects were
`nereus-delay-pulsar-multi-e2e-1786902614-37701` and
`nereus-delay-pulsar-multi-oxia-e2e-1786902614-37701`; the brokers used
`29480/29481` and `29482/29483`, Oxia used `29490`, P1 image was
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
and the temporary Oxia image was
`sha256:d4808a1f1860d744ec8d12539d1a85daf583114589b36a70c62aaffcae7819e6`.

The run passed with both Worker invocations reporting `BUILD SUCCESSFUL`:

```text
Pulsar Broker process-crash failover E2E passed: broker-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through broker-2 with real Oxia authority, and broker-1 rejoined afterward.
```

This is bounded two-Broker process-crash evidence using one ZooKeeper and one
BookKeeper. It does not close raw network/socket cuts, controller or storage
failover, Gateway-plus-Broker failover, multi-shard placement, the full
crash/chaos matrix or V1 release gates. Exact postchecks found no project
containers, networks, volumes or matching P1/Oxia images; the locked Oxia
base remained and no global Docker prune was used.

## Pulsar Gateway + Broker process-crash large-payload failover (current source)

Run the current-source combined Gateway/Oxia/Worker/MinIO process-crash slice:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 \
PULSAR_LARGE_BROKER_1_PORT=29520 \
PULSAR_LARGE_WEB_1_PORT=29521 \
PULSAR_LARGE_BROKER_2_PORT=29522 \
PULSAR_LARGE_WEB_2_PORT=29523 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=29530 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=29531 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=29532 \
PULSAR_LARGE_PAYLOAD_TOPIC=nereus-delay-pulsar-large-payload-process-crash-20260817-r4 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-pulsar-large-destination-process-crash-20260817-r4 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The receipt locks Delay to
`888c0513c433234282a12eff6e401aa4a8a40116`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`. It uses the locked MinIO
image
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
P1 image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
and project `nereus-delay-pulsar-large-e2e-1786903675-50550` on Pulsar
`29520/29521,29522/29523`, Oxia `29530`, MinIO `29531` and Gateway `29532`.

The run passed with `BUILD SUCCESSFUL`:

```text
Pulsar source reactivation successor accepted: oldGeneration=2, newGeneration=3, assignmentRevision=2, ownerEpoch=2
Pulsar Worker source-applied physical publish passed: Admission source ledger=5/0, typed PULSAR_SEND_ACK target ledger/entry=3/0, Outcome source ledger=5/1, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker process-crash failover E2E passed: broker-1 was SIGKILLed after Gateway Commit/readback, the same source-applied physical Publish completed through broker-2, and broker-1 rejoined afterward
```

The runner keeps the strict equal-generation rejection. Since the current P1
generation allocator is Broker-process local, an equal candidate is closed
and retried with a fresh guarded SUBSCRIBE; no equal proof is accepted and no
cluster-global generation monotonicity is claimed. This is bounded
two-Broker/one-physical-source-partition evidence, not automatic Pulsar
controller/coordinator failover, raw socket/network chaos,
ZooKeeper/BookKeeper/storage failover, multi-shard placement, full chaos or
V1 release evidence. Exact postchecks found no project containers, networks,
volumes, P1 image or run-created Oxia image; the locked MinIO base remained
and no global Docker prune was used.

## Pulsar Gateway + Broker network-partition large-payload failover (current source)

Run the current-source combined Gateway/Oxia/Worker/MinIO network-partition
slice with:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS=75 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 \
PULSAR_LARGE_BROKER_1_PORT=30440 \
PULSAR_LARGE_WEB_1_PORT=30441 \
PULSAR_LARGE_BROKER_2_PORT=30442 \
PULSAR_LARGE_WEB_2_PORT=30443 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=30450 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=30451 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=30452 \
PULSAR_LARGE_PAYLOAD_TOPIC=nereus-delay-pulsar-large-payload-networkcut-20260817-r14 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-pulsar-large-destination-networkcut-20260817-r14 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

This current-source receipt locks Delay to `f95c8a5468d6a1ee6df0bc1bd99000dc769d8797`,
P1 to `nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1 image
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`, Oxia
to `37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO to the locked digest
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The isolated project was `nereus-delay-pulsar-large-e2e-1786906721-85706` on
Pulsar `30440/30441,30442/30443`, Oxia `30450`, MinIO `30451` and Gateway
`30452`.

The receipt was:

```text
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906846912
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906861998
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906877097
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906892146
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906907254
Pulsar source reactivation successor accepted: oldGeneration=2, newGeneration=3, assignmentRevision=2, ownerEpoch=2
Pulsar Worker source-applied physical publish passed: Admission source ledger=5/0, typed PULSAR_SEND_ACK target ledger/entry=7/0, Outcome source ledger=5/1, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker network-partition failover E2E passed: broker-1 stayed alive but lost its exact Compose network endpoint after Gateway Commit/readback, the same source-applied physical Publish completed through broker-2, and broker-1 rejoined afterward
BUILD SUCCESSFUL in 2m 5s
```

The harness disconnects only the exact Docker Compose network membership of the
still-running Broker-1 after Gateway Commit/readback, waits 75 seconds for the
disconnected ownership lease to expire while renewing the active lease, then
performs strict successor guarded SUBSCRIBE reactivation and reconnects
Broker-1 after the same large-payload physical Publish completes through
Broker-2. The post-seek proof uses a bounded quiet window so a P1 consumer
replacement cannot invalidate the Route barrier immediately after `seekAfter`.
This is bounded two-Broker/one-physical-source-partition evidence, not all
packet/proxy/socket failure shapes, automatic controller/coordinator or
ZooKeeper/BookKeeper/storage failover, multi-shard production placement, the
full crash/chaos matrix or V1 release readiness.

Exact post-run checks found no containers, networks, volumes, P1 image or
run-created Oxia image for the project. The locked MinIO base image was
retained; no global `docker prune` was run.

## Kafka K2 committed response-loss (current source)

Run the focused current-source three-Broker K2 response-loss cut with:

```bash
NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS=1 \
NEREUS_DELAY_KAFKA_K2_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-k2-response-loss-20260817-r2 \
KAFKA_BROKER_1_PORT=30740 \
KAFKA_BROKER_2_PORT=30741 \
KAFKA_BROKER_3_PORT=30742 \
KAFKA_DELAY_E2E_K2_TARGET_TOPIC=nereus-delay-k2-target-response-loss-20260817-r2 \
KAFKA_DELAY_E2E_K2_RECEIPT_TOPIC=nereus-delay-k2-receipt-response-loss-20260817-r2 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The current-source receipt locks Delay to
`9c6afd5e93621320da2b1c952553f6ffd28b364f`, K1 to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
the client artifact to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, the
temporary broker image to
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
and the Compose project to
`nereus-delay-kafka-e2e-1786909364-22320`.

The run passed with `BUILD SUCCESSFUL in 48s` and printed:

```text
K2 committed response-loss smoke passed: real EndTxn committed the exact target-plus-receipt pair, the local response was discarded, and typed read_committed evidence resolved PUBLISHED
Kafka K2 committed response-loss E2E passed: real EndTxn commit was followed by local response loss and exact read_committed typed receipt resolution.
```

This is controlled local post-commit response loss, not raw socket loss,
Broker crash/failover, the full Kafka LSO/retention matrix or a release gate.
The runner's exact postchecks found no project containers, networks, volumes or
temporary K1 image; no global Docker prune was used.

## Kafka native multi-shard Worker fleet (current source)

Run the focused current-source two-shard Kafka Worker composition with:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_MULTI_SHARD_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-multishard-20260817-r1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=30760 \
KAFKA_BROKER_1_PORT=30750 \
KAFKA_BROKER_2_PORT=30751 \
KAFKA_BROKER_3_PORT=30752 \
KAFKA_DELAY_E2E_ROUTE_WORKER_TOPIC=nereus-delay-kafka-route-multishard-20260817-r1 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The current-source receipt locks Delay to
`cd0b90bd52d5db00cbccdf42be24bdcf41375dbc`, K1 to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
the client artifact to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, K1
image to
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`, and the isolated projects
to `nereus-delay-kafka-e2e-1786909915-30675` and
`nereus-delay-kafka-oxia-e2e-1786909915-30675`.

The run passed with `BUILD SUCCESSFUL in 54s` and printed:

```text
Kafka signed Route -> two guarded Fetch barriers -> Oxia multi-shard Assignment/Owner -> one Worker fleet -> RocksDB apply/ACK/checkpoint smoke passed: fetchPartitions=2, routeRevision=1, assignmentRevisions=[1, 1], workers=[kafka-route-worker-a, kafka-route-worker-b], sourceBarriers=[1, 1]
Kafka native multi-shard Worker fleet E2E passed: one signed Route covered two guarded Fetch barriers, two real Oxia Assignment/Owner Lease CAS paths admitted two native source consumers, one fair fleet applied/ACKed both partitions, and both final checkpoints/assignments were released.
```

This proves the bounded two-physical-partition Worker fleet path. It does not
close production placement/eligibility authority, multi-shard large-payload
egress, arbitrary multi-shard chaos or a release gate. Exact postchecks found
no project containers, networks, volumes or temporary Kafka/Oxia images; no
global Docker prune was used.

## Kafka Broker network-partition Worker recovery (current source)

Run the focused current-source three-Broker Docker-network partition cut with:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=30780 \
KAFKA_BROKER_1_PORT=30770 \
KAFKA_BROKER_2_PORT=30771 \
KAFKA_BROKER_3_PORT=30772 \
KAFKA_DELAY_BROKER_NETWORK_PARTITION_TOPIC=nereus-delay-broker-network-partition-20260817-r1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-network-partition-20260817-r1 \
KAFKA_DELAY_E2E_WORKER_TOPIC=nereus-delay-worker-network-partition-20260817-r1 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The receipt locks Delay to
`499169a116fa401cb902a60bb805f9c72173ab69`, K1 to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
the client artifact to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, K1
image to
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`, and the isolated projects
to `nereus-delay-kafka-e2e-1786910072-33614` and
`nereus-delay-kafka-oxia-e2e-1786910072-33614`.

The run passed with the Worker stages reporting `BUILD SUCCESSFUL` and printed:

```text
Kafka survivor topic leader recovery passed: leaders={nereus-delay-broker-network-partition-20260817-r1=2, nereus-delay-worker-destination-topic=2, nereus-delay-worker-destination-topic-receipt=3}
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Broker network-partition recovery E2E passed: kafka-1 stayed alive but was disconnected from the Compose network after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia Worker authority and source apply/ACK/checkpoint, and kafka-1 reconnected afterward.
```

This is bounded source-Worker recovery evidence only; it does not cover
destination egress during the survivor window, raw packet/proxy/socket chaos,
automatic controller/coordinator failover beyond the topic-leader check,
multi-shard chaos or release readiness. Exact postchecks found no project
containers, networks, volumes or temporary Kafka/Oxia images; no global Docker
prune was used.

## Kafka Fetch response-loss and LSO (current source)

Run the focused current-source guarded Fetch response-loss cut with:

```bash
NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-20260817-r2 \
KAFKA_BROKER_1_PORT=30790 \
KAFKA_BROKER_2_PORT=30791 \
KAFKA_BROKER_3_PORT=30792 \
KAFKA_DELAY_FETCH_RESPONSE_LOSS_TOPIC=nereus-delay-fetch-response-loss-20260817-r2 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The receipt locks Delay to
`4800b3b269c623061149a398e9799adc8aa7c449`, K1 to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
the client artifact to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, K1
image to
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
and project `nereus-delay-kafka-e2e-1786910277-37004`.

The run passed with `BUILD SUCCESSFUL in 56s`:

```text
Kafka source Fetch response-loss smoke passed: responseDiscardedAfterFetch=true, replayOffset=0, secondOffset=1, fetchLso=2, committedAfterReplay=2
Kafka source Fetch response-loss E2E passed: real read_committed Fetch v13 response was discarded before ACK, exact source replay and LSO coverage were recovered.
```

This is controlled Fetch response loss after real Broker persistence, not raw
socket loss, coordinator/Broker crash recovery, the full chaos matrix or a
release gate. Exact postchecks found no project containers, networks, volumes
or temporary K1 image; no global Docker prune was used.

## Kafka retention floor (current source)

Run the focused current-source accelerated-retention floor cut with:

```bash
NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-retention-floor-20260817-r2 \
KAFKA_BROKER_1_PORT=30800 \
KAFKA_BROKER_2_PORT=30801 \
KAFKA_BROKER_3_PORT=30802 \
KAFKA_DELAY_RETENTION_FLOOR_TOPIC=nereus-delay-retention-floor-20260817-r2 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The receipt uses the same Delay/K1/client/image locks as the Fetch cut and
project `nereus-delay-kafka-e2e-1786910277-37005`.

The run passed with `BUILD SUCCESSFUL in 1m 4s`:

```text
Kafka source retention-floor smoke passed: oldOffset=0, retentionFloor=4, endOffset=21, staleOffsetRejected=true, floorFetchOffset=4, fetchLso=21
Kafka source retention-floor E2E passed: real Broker retention advanced the earliest offset, stale source offset was rejected, and the current floor remained readable through guarded Fetch v13 with LSO.
```

This is deterministic accelerated-retention evidence, not disk ENOSPC, raw
socket/coordinator chaos, multi-shard placement or a release gate. Exact
postchecks found no project containers, networks, volumes or temporary K1
image; no global Docker prune was used.

## Pulsar multi-Broker Worker process-crash failover (current source)

Run the current-source real P1 two-Broker + Oxia process-crash cut with:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=30820 \
PULSAR_BROKER_1_PORT=30810 \
PULSAR_WEB_1_PORT=30811 \
PULSAR_BROKER_2_PORT=30812 \
PULSAR_WEB_2_PORT=30813 \
PULSAR_DELAY_MULTI_BROKER_RESTART_TOPIC=p1-multi-worker-20260817-r1 \
PULSAR_DELAY_MULTI_BROKER_DESTINATION_TOPIC=p1-multi-destination-20260817-r1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-multi-broker-20260817-r1 \
  bash e2e/run-pulsar-multi-broker-failover-e2e.sh
```

The receipt locks Delay to
`e690ee06951bfcf6a614fee82c9d772873bedf0b`, P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, the
three client artifacts to the SHA-256 values recorded in the status receipt,
P1 image to
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`, and the isolated projects
to `nereus-delay-pulsar-multi-e2e-1786910460-40274` and
`nereus-delay-pulsar-multi-oxia-e2e-1786910460-40274`.

The run passed with both Worker invocations reporting `BUILD SUCCESSFUL`:

```text
Pulsar Worker restart preparation passed: one guarded record persisted before broker restart
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/3, typed PULSAR_SEND_ACK target ledger/entry=4/0, Outcome source ledger=3/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=3/0, active apply ledger/entry=3/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Broker process-crash failover E2E passed: broker-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through broker-2 with real Oxia authority, and broker-1 rejoined afterward.
```

This is bounded two-Broker process-crash evidence for one physical source
topic. It does not cover raw network/socket cuts, controller or storage
failover, Gateway-plus-Broker combined failover, multi-shard chaos or release
readiness. Exact postchecks found no project containers, networks, volumes,
P1 image or temporary Oxia image; no global Docker prune was used.

## Bounded local capacity and SLO probe

`run-bounded-capacity-slo-probe.sh` is the first source-locked evidence
producer for the benchmark/capacity/SLO closure work. It requires a clean Delay
worktree, records the exact `HEAD`, uses no Docker, writes three bounded
payload sizes (256, 4096 and 65536 bytes) through real synchronous RocksDB
`WriteBatch` calls, verifies readback after each run, persists bounded SLO
Start/Final records through `SloObservationOutboxStore`, scans the outbox,
persists the collector projection and verifies both the Store and collector
after a fresh reopen. The report is JSON and remains `PARTIAL` by design.

Run it with:

```bash
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-capacity-gradle \
  bash e2e/run-bounded-capacity-slo-probe.sh
```

The current-source receipt used Delay
`nereus/delay-full-implementation-v1@5248326726f89b761facbe7d872cf36abcc4a181`,
16 records per payload size and 24 SLO samples. It passed with `BUILD SUCCESSFUL`
and produced
`/var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T//nereus-delay-capacity.p2hcUY/bounded-capacity-slo-probe.json`:

```text
payload 256: 16 records, 4096 input bytes, 16 readback records
payload 4096: 16 records, 65536 input bytes, 16 readback records
payload 65536: 16 records, 1048576 input bytes, 16 readback records
SLO: 24 durable Start/Final samples, 24 exported records, 12528 encoded bytes
reopen: Store payload/SLO readback=true, persistent collector=true
```

The host is Darwin, so `WorkerRuntimeResourceProbe` correctly reported
`UNAVAILABLE` because the test JVM had no explicit `MaxDirectMemorySize` and
the host does not expose the Linux procfs/cgroup authority expected by that
probe. Host observations must not be substituted for certified cgroup, rlimit,
RSS, FD or disk envelope values. This receipt therefore does not close Gates 5
or 6, and it does not cover broker batching/linger, 1M/10M/100M records, Lane
distributions, multi-Worker placement, checkpoint restore throughput, or the
long-cycle soak required by Gate 7.

## Gateway + real Oxia + Worker + MinIO large payload (current source)

Run the Kafka production-authority chain with the isolated K1 worktree and a
real Kafka destination readback:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-large-payload-kafka-20260817-r1 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=31400 \
KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=31401 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=31402 \
NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=31410 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=31411 \
NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=31412 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC=nereus-delay-large-payload-20260817-r1 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-destination-20260817-r1 \
  bash e2e/run-large-payload-gateway-e2e.sh
```

The current-source run used Delay `53a1eb71b480d3d1ecff1a14d6c1f76d675fe4d8`,
Kafka K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, K1 image
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and the locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Project `nereus-delay-large-payload-e2e-1786919558-57250` passed in 1m 6s:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=4, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=5, exact payload readback
Kafka + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: activationOffset=0, barrierOffset=2, prepareOffset=... offset=2 ..., commitOffset=... offset=3 ..., providerVersion=895ef48b-def6-4789-bd3c-875139095322, exactGatewayIdempotency=true
Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload + Kafka destination authority E2E passed
```

Run the corresponding P1 chain with a bare source topic name (the smoke task
adds the `persistent://public/default/` prefix) and a real Pulsar destination:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-large-payload-pulsar-20260817-r2 \
PULSAR_LARGE_BROKER_1_PORT=31440 \
PULSAR_LARGE_WEB_1_PORT=31441 \
PULSAR_LARGE_BROKER_2_PORT=31442 \
PULSAR_LARGE_WEB_2_PORT=31443 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=31450 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=31451 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=31452 \
PULSAR_LARGE_PAYLOAD_TOPIC=nereus-delay-large-payload-20260817-r2 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-destination-20260817-r2 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The current-source run used the same Delay/Oxia locks, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
and project `nereus-delay-pulsar-large-e2e-1786919804-59806`. It passed in
1m 12s:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/4, typed PULSAR_SEND_ACK target ledger/entry=4/0, Outcome source ledger=3/5, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=3/2, commit=3/3, exactGatewayIdempotency=true, sourceRecords=6
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed
```

Both runs used real 1 MiB payloads, source-ordered apply, typed destination
evidence, exact payload readback and real MinIO-backed checkpoint publication.
They strengthen the bounded Gateway/Broker/Worker/Object Store authority chain
and destination coverage, but do not close the full fault matrix, benchmark or
soak gates, authenticated activation-state/cutover, multi-shard production
placement, or V1 release readiness. The runners removed their exact project
containers, networks, volumes and temporary images; no global Docker prune was
used.

## Current-source twelve-cell bounded chaos rerun

The bounded matrix runner can be rerun as one source-locked command:

```bash
env NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR="$(mktemp -d -t nereus-delay-chaos-current.XXXXXX)" \
  NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-current-20260817-r1 \
  bash e2e/run-bounded-chaos-matrix.sh
```

At Delay `0d4fdbbc899c0af0d9edee20d939b207dc3721a5`, the current-source run
completed all twelve cells with `matrix_status=0`: four Kafka Broker/Worker
cuts; Pulsar Worker crash, two-Broker crash/failover and Publish Admission
response loss; checkpoint REAPING; Kafka Fetch response loss and retention
floor; and Pulsar destination SEND and source ACK response loss. It used K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, the locked K1/P1 image and
artifact digests documented in `docs/IMPLEMENTATION-STATUS.md`, and a unique
Compose project per cell.

The run is bounded evidence, not full §23.3 or V1 release certification.
After completion, exact-name checks found no matrix containers, networks or
volumes and no temporary broker/Pulsar/Oxia images. The locked Oxia and MinIO
base images were retained for later source-locked runs; no global Docker
prune or unrelated image deletion was performed.

## Current-source Oxia + MinIO credential renewal

The real Profile/Head/protection and renewable Object Store lease smoke can be
run with:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31500 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31501 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-renewal-20260817-r2 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

At Delay `7675ea75c8f51f07a0898986e529c9035ee51528`, the three selected
real-service tests passed: checkpoint publication, checkpoint REAPING, and
credential lease renewal. The exact renewal receipt was:

```text
Oxia + MinIO Object Store credential renewal E2E passed: real Profile Head/protection CAS renewed the exact lease and fenced the live adapter at secret rotation
```

The exact Compose project was
`nereus-delay-oxia-minio-checkpoint-e2e-1786921039-77113`; postchecks found no
project resources or temporary Oxia image. The locked Oxia and MinIO bases
remain intentionally retained. This is bounded renewal/fencing evidence, not
external secret-manager, provider-side quiescence/attestation, multi-node
failover or V1 release certification.

## Current-source large-payload Broker failover matrix

The large-payload Gateway runner also supports bounded Broker process-crash and
network-partition cuts. Each command below uses an isolated K1/P1 checkout,
real Oxia, real MinIO and a unique topic/project. The failover cut is injected
only after Gateway Commit/readback; the receipt then requires the same
source-applied physical Publish, typed destination evidence, exact payload
readback and Broker rejoin.

Kafka process crash:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-large-payload-kafka-crash-20260817-r1 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_PROCESS_CRASH=1 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=31510 KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=31511 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=31512 NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=31520 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=31521 NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=31522 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC=nereus-delay-large-payload-kafka-crash-20260817 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-destination-kafka-crash-20260817 \
  bash e2e/run-large-payload-gateway-e2e.sh
```

Kafka network partition uses the same runner with
`NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_NETWORK_PARTITION=1` and ports
`31530/31531/31532`, Oxia `31540`, MinIO `31541`, Gateway `31542`. The current
receipts were projects `nereus-delay-large-payload-e2e-1786921218-80232` and
`nereus-delay-large-payload-e2e-1786921333-81544`, both using Delay
`5e721b878bcd2ef81f53a035f0aa74b14220fb9e`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, K1 image
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Both passed with exact payload readback and survivor completion; the network
receipt also recorded `BUILD SUCCESSFUL in 1m 39s`.

Pulsar process crash:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-crash-20260817-r1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_PROCESS_CRASH=1 \
PULSAR_LARGE_BROKER_1_PORT=31550 PULSAR_LARGE_WEB_1_PORT=31551 \
PULSAR_LARGE_BROKER_2_PORT=31552 PULSAR_LARGE_WEB_2_PORT=31553 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=31560 NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=31561 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=31562 \
PULSAR_LARGE_PAYLOAD_TOPIC=nereus-delay-pulsar-large-crash-20260817 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-pulsar-destination-crash-20260817 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

Pulsar network partition uses the same runner with
`NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION=1` and ports
`31590/31591`, `31592/31593`, Oxia `31600`, MinIO `31601`, Gateway `31602`.
The final current-source project was
`nereus-delay-pulsar-large-e2e-1786922018-88960`; it used Delay
`3536cd42fa6234fe461bf4beb687375463814daa`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1 distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1 image
`sha256:a2c76925f2504337a55c1b88d0a83cc80147d563189041514b63bc1e347cf9d3`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and the same locked MinIO
digest. It passed with `BUILD SUCCESSFUL in 2m 22s`, source apply
`ledger=3/4`, outcome `ledger=3/5`, exact payload readback and broker-1 rejoin.

The first Pulsar network attempt, project
`nereus-delay-pulsar-large-e2e-1786921724-85688`, is explicitly not a PASS:
the 75-second handoff wait let inactive-topic deletion remove the
pre-provisioned guarded destination, so auto-creation on broker-2 was rejected
with `TopicResourceGuardException`. Commit `3536cd42` makes the P1 cluster E2E
entrypoint disable inactive-topic deletion by default during this bounded
handoff window. That keeps the exact guard tuple alive and preserves fail-closed
guarded producer validation; it does not add a retry or allow an unguarded
destination.

Every successful runner removed its exact Compose containers, networks, volumes
and temporary K1/P1/Oxia images. Final related-image inspection retained only
`nereus/oxia-o1:37a17bef1720` (local ID
`sha256:5aa715e4f19091931743e5af489af5f8d6ee15efcce6430a908c6f65cc6d6516`)
and the locked MinIO base (local ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`).
No global Docker prune or unrelated image deletion was performed. These are
bounded one-physical-partition failover receipts, not controller/coordinator/
BookKeeper storage failover, full §23.3 completion or V1 release approval.

## Current-source Object Store ambiguous PUT fault slice

The adapter's deterministic fault boundary can be run without Docker:

```bash
GRADLE_USER_HOME=/tmp/nereus-delay-object-store-faults-20260817-r1 \
  ./gradlew test \
    --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
    --no-daemon --console=plain
```

At Delay `5b64004b6a4fe2b07ac67b504be05cd57b10b2e2`, all 12 tests passed. The
fake S3 provider injects a 503 after storing the exact object, a 503 before
storing it, and a response timeout after storing it. The adapter exact-reads
the object before resolving a 5xx/timeout; an absent object remains a failure
with local uncertainty and no invented success. The same test class retains
the endpoint/credential-scope drift-before-HTTP assertion.

For post-change real-provider regression, use the normal Oxia + MinIO runner:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31610 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31611 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-fault-regression-20260817-r1 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

The current run used project
`nereus-delay-oxia-minio-checkpoint-e2e-1786923070-2608`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The three selected real-service tests passed in `1m 13s`; exact postchecks
found no project resources, temporary Oxia image or dangling image. Only the
locked Oxia and MinIO base images were retained, and no global Docker prune or
unrelated image deletion was used.

The local tests are not a real MinIO fault-injection receipt. A production
Worker run with real MinIO 5xx/timeout or credential/config failure injection
is still required before closing that §23.3/Object Store authority cell or
the V1 release gate.

## Current-source real MinIO provider-fault E2E

The committed runner starts the locked MinIO base, places a deterministic
HTTP proxy in front of it, and runs four real adapter tests:

```bash
NEREUS_DELAY_MINIO_FAULT_MINIO_PORT=31651 \
NEREUS_DELAY_MINIO_FAULT_PROXY_PORT=31652 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-minio-fault-real-20260817-r1 \
  bash e2e/run-minio-fault-e2e.sh
```

The runner supports these proxy modes for the first manifest PUT:

- `PUT_503_AFTER_COMMIT`: real MinIO commits, then the proxy returns HTTP 503;
  exact immutable GET must resolve the provider version.
- `PUT_503_BEFORE_COMMIT`: the proxy returns HTTP 503 without forwarding; the
  adapter must remain fail-closed.
- `PUT_TIMEOUT_AFTER_COMMIT`: real MinIO commits, then the proxy holds the
  response beyond the adapter timeout; exact immutable GET must resolve it.
- credential configuration drift: the adapter sends a wrong secret to real
  MinIO and must receive HTTP 403.

At Delay `ef794947c16557a9f677e51d39413c25b8f1d479`, the run passed with
`BUILD SUCCESSFUL in 16s`. The exact project/container was
`nereus-delay-minio-fault-e2e-1786924060-14028`; bucket
`1786924060-fault-14028`; MinIO/proxy ports `31651/31652`; and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The runner removed the exact container and proxy, and postchecks found no
matching network, volume, listener, dangling image or Python cache. No new
Docker image was built; the locked MinIO base remains intentionally retained.
No global Docker prune or unrelated image deletion was performed.

The previous fresh-Gradle-home attempt failed before test execution during an
`os-maven-plugin-1.7.1.jar` TLS handshake and is not a PASS receipt; its exact
resources were cleaned. This E2E proves real MinIO provider behavior through
the adapter boundary. It does not yet drive the fault proxy through the full
Gateway/Oxia/Worker large-payload Checkpoint Intent/Catalog/REAPING production
chain, so the production Object Store fault matrix and V1 release gate remain
open.

## Current-source full large-payload Gateway/Broker/Worker fault E2E

The large-payload runners can place the same deterministic proxy in front of
the real MinIO used by Gateway and Worker. `PUT_503_AFTER_COMMIT` applies to
the first payload object PUT; the proxy then forwards the exact GET and HEAD
used by Worker readback and Gateway attestation. The adapter exact-reads the
immutable identity, so a matching object resolves the ambiguous 503 while an
absent object remains fail-closed.

Kafka source-locked invocation:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-full-large-payload-fault-20260817-r2 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_503_AFTER_COMMIT \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31773 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=31760 KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=31761 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=31762 NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=31770 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=31771 NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=31772 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC=kafka-large-payload-minio-fault-20260817-r2 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=kafka-large-payload-destination-minio-fault-20260817-r2 \
  bash e2e/run-large-payload-gateway-e2e.sh
```

Pulsar source-locked invocation:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-minio-fault-20260817-r2 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_503_AFTER_COMMIT \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31793 \
PULSAR_LARGE_BROKER_1_PORT=31780 PULSAR_LARGE_WEB_1_PORT=31781 \
PULSAR_LARGE_BROKER_2_PORT=31782 PULSAR_LARGE_WEB_2_PORT=31783 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=31790 NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=31791 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=31792 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-minio-fault-20260817-r2 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=pulsar-large-payload-destination-minio-fault-20260817-r2 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

At Delay `9bea4b2408db3302d68ec0ef0bb3b9613cee4d18`, Kafka project
`nereus-delay-large-payload-e2e-1786925109-27016` passed in `1m 7s` and Pulsar
project `nereus-delay-pulsar-large-e2e-1786925224-28335` passed in `1m 13s`.
Both receipts include real Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`,
K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Kafka passed source offsets `4`/`5`, typed `KAFKA_TRANSACTIONAL_RECEIPT`,
exact destination payload readback and `exactGatewayIdempotency=true`; Pulsar
passed source ledgers `3/4` and `3/5`, typed `PULSAR_SEND_ACK` target `4/0`,
exact destination payload readback and `exactGatewayIdempotency=true`.

The runners remove their exact Compose containers, networks, volumes, proxy
processes and per-run images. Only locked base images remain intentionally;
do not use a global Docker prune. This receipt closes only the real MinIO
503-after-commit large-payload production path. Full-chain pre-commit and
timeout faults, Checkpoint Intent/Catalog/REAPING fault injection, target
isolation, the remaining §23.3 matrix, multi-shard placement, benchmark/soak
and V1 release proof remain open.

## Current-source full large-payload timeout-after-commit E2E

The same full runners support `PUT_TIMEOUT_AFTER_COMMIT`. The proxy forwards
the first immutable payload PUT to real MinIO, holds the response for three
seconds, and the smoke process uses a `1000ms` MinIO request timeout. Exact
immutable GET must resolve the committed object; a pre-commit timeout is not
treated as success and remains fail-closed.

Kafka source-locked invocation:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-full-large-payload-timeout-20260817-r1 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_TIMEOUT_AFTER_COMMIT \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS=1000 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31813 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=31800 KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=31801 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=31802 NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=31810 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=31811 NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=31812 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_TOPIC=kafka-large-payload-minio-timeout-20260817-r1 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=kafka-large-payload-destination-minio-timeout-20260817-r1 \
  bash e2e/run-large-payload-gateway-e2e.sh
```

Pulsar source-locked invocation:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-timeout-20260817-r1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_TIMEOUT_AFTER_COMMIT \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS=1000 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31833 \
PULSAR_LARGE_BROKER_1_PORT=31820 PULSAR_LARGE_WEB_1_PORT=31821 \
PULSAR_LARGE_BROKER_2_PORT=31822 PULSAR_LARGE_WEB_2_PORT=31823 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=31830 NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=31831 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=31832 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-minio-timeout-20260817-r1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=pulsar-large-payload-destination-minio-timeout-20260817-r1 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

At Delay `0bc0741a4a813b2403becea0f4aa23b1785bab09`, the Kafka project
`nereus-delay-large-payload-e2e-1786925957-37227` passed in `1m 7s`, and the
Pulsar project `nereus-delay-pulsar-large-e2e-1786926140-39112` passed in
`1m 12s`. Kafka passed source offsets `4`/`5`, typed
`KAFKA_TRANSACTIONAL_RECEIPT`, exact destination readback,
`providerVersion=a2de8a49-5173-4f58-b5a5-0b06edd8a002` and
`exactGatewayIdempotency=true`; Pulsar passed source ledgers `3/4` and `3/5`,
typed `PULSAR_SEND_ACK` target `4/0`, exact destination readback and
`exactGatewayIdempotency=true` with `sourceRecords=6`. Both receipts used
real Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The runners remove exact Compose containers, networks, volumes, proxy
processes and per-run images. Only the locked Oxia and MinIO base images are
retained; no global Docker prune or unrelated image deletion is appropriate.
This receipt closes only the full real-MinIO timeout-after-commit payload path;
Checkpoint Intent/Catalog/REAPING fault injection, pre-commit fail-closed
evidence, target isolation, the remaining §23.3 matrix, multi-shard
placement, benchmark/soak and V1 release proof remain open.

## Current-source real MinIO Checkpoint Intent/Catalog/REAPING fault E2E

The checkpoint runner can inject a fault into the first immutable
`manifest.json` PUT while using real Oxia and version-enabled MinIO. It runs
the publication and REAPING tests as separate JVMs and resets the proxy between
them, so both authority paths receive the selected fault. Supported modes are
`PUT_503_AFTER_COMMIT`, `PUT_TIMEOUT_AFTER_COMMIT`, `PUT_503_BEFORE_COMMIT` and
`PUT_TIMEOUT_BEFORE_COMMIT`.

503-after-commit invocation:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31910 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31911 \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_MODE=PUT_503_AFTER_COMMIT \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_PROXY_PORT=31912 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-checkpoint-fault-20260817-r1 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

Timeout-after-commit invocation:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31920 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31921 \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_MODE=PUT_TIMEOUT_AFTER_COMMIT \
NEREUS_DELAY_CHECKPOINT_MINIO_REQUEST_TIMEOUT_MS=1000 \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_PROXY_PORT=31922 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-checkpoint-timeout-20260817-r1 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

At Delay `c930413d146879b68b06f9f313eef3f290c63e1e`, the 503 project
`nereus-delay-oxia-minio-checkpoint-e2e-1786926546-44708` passed publication in
`1m 17s` and REAPING in `13s`; the timeout project
`nereus-delay-oxia-minio-checkpoint-e2e-1786926652-46178` passed publication in
`1m 17s` and REAPING in `14s`. The real REAPING receipt proves Owner
abandonment, `PENDING_UPLOAD -> REAPING`, exact-version prefix sweep `2`,
`finalEmptyPrefix=true` and `localProviderOwnershipClosed=true`. Both runs
used Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The runner removes exact MinIO/Oxia containers, Compose resources, proxy
processes and per-run Oxia images. Only locked Oxia/MinIO bases remain; no
global Docker prune or unrelated image deletion is appropriate. This receipt
closes only post-commit ambiguity through Checkpoint Intent/Catalog/REAPING;
pre-commit remains fail-closed, and provider-side quiescence/consistency,
multi-worker takeover, target isolation, the remaining §23.3 matrix,
multi-shard placement, benchmark/soak and V1 release proof remain open.

### Checkpoint pre-commit fail-closed modes

These invocations use the same real Oxia + version-enabled MinIO runner, but
run only the Worker publication failure test. The proxy does not forward the
first `manifest.json` PUT, so a timeout is deliberately an error and cannot be
promoted to PUBLISHED.

HTTP 503 before provider commit:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31950 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31951 \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_MODE=PUT_503_BEFORE_COMMIT \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_PROXY_PORT=31952 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-checkpoint-fault-20260817-r1 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

Timeout before provider commit:

```bash
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_OXIA_CHECKPOINT_E2E_PORT=31960 \
NEREUS_DELAY_MINIO_CHECKPOINT_E2E_PORT=31961 \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_MODE=PUT_TIMEOUT_BEFORE_COMMIT \
NEREUS_DELAY_CHECKPOINT_MINIO_REQUEST_TIMEOUT_MS=1000 \
NEREUS_DELAY_CHECKPOINT_MINIO_FAULT_PROXY_PORT=31962 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-minio-checkpoint-fault-20260817-r1 \
  bash e2e/run-oxia-minio-checkpoint-e2e.sh
```

At Delay `33714d9a5470edf50aed57bc8a2aefe5cfb52b5c`, the 503 project
`nereus-delay-oxia-minio-checkpoint-e2e-1786927303-54007` passed in `13s` and
the timeout project `nereus-delay-oxia-minio-checkpoint-e2e-1786927391-54902`
passed in `14s`. Both proved exact Oxia `PENDING_UPLOAD` retention, absent
PUBLISHED Catalog/manifest, scheduler/Owner cleanup and exact deletion of the
partial checkpoint prefix. They used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The runner removes exact containers, networks, volumes, proxy processes and
per-run Oxia images. Only the locked Oxia/MinIO bases remain; no global Docker
prune or unrelated image deletion is appropriate. Full Gateway/large-payload
pre-commit, provider-side quiescence/consistency, multi-shard placement, the
remaining fault matrix and V1 release proof remain open.

### Full Gateway/large-payload pre-commit fail-closed modes

The full Kafka and Pulsar large-payload runners also support
`PUT_503_BEFORE_COMMIT` and `PUT_TIMEOUT_BEFORE_COMMIT`. In both modes the
fault proxy does not forward the first payload PUT. The real Gateway/Oxia/
Broker/Worker/MinIO chain must leave Prepare `RESERVED`, omit Commit, attest
the payload as absent, drain the Worker checkpoint and release the Owner.

Kafka 503-before-commit:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_503_BEFORE_COMMIT \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31853 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=31840 KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=31841 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=31842 NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=31850 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=31851 NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=31852 \
  bash e2e/run-large-payload-gateway-e2e.sh
```

Kafka timeout-before-commit:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_TIMEOUT_BEFORE_COMMIT \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS=1000 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31873 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=31860 KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=31861 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=31862 NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=31870 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=31871 NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=31872 \
  bash e2e/run-large-payload-gateway-e2e.sh
```

Pulsar 503-before-commit:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_503_BEFORE_COMMIT \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31893 \
PULSAR_LARGE_BROKER_1_PORT=31880 PULSAR_LARGE_WEB_1_PORT=31881 \
PULSAR_LARGE_BROKER_2_PORT=31882 PULSAR_LARGE_WEB_2_PORT=31883 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=31890 NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=31891 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=31892 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

Pulsar timeout-before-commit:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_MODE=PUT_TIMEOUT_BEFORE_COMMIT \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_REQUEST_TIMEOUT_MS=1000 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MINIO_FAULT_PROXY_PORT=31913 \
PULSAR_LARGE_BROKER_1_PORT=31900 PULSAR_LARGE_WEB_1_PORT=31901 \
PULSAR_LARGE_BROKER_2_PORT=31902 PULSAR_LARGE_WEB_2_PORT=31903 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=31910 NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=31911 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=31912 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

At Delay `2a0db42290da0fa47a28356a1d4bcb6bcf2123b8`, the four source-locked
projects were `nereus-delay-large-payload-e2e-1786928021-63153`,
`nereus-delay-large-payload-e2e-1786928059-63650`,
`nereus-delay-pulsar-large-e2e-1786928197-65083` and
`nereus-delay-pulsar-large-e2e-1786928269-65870`; all reported the explicit
pre-commit fail-closed receipt and exact postchecks found no run-created
containers, networks, volumes, listeners, fault proxies or per-run images.
Only the locked Oxia/MinIO bases remain. This closes the bounded full-chain
pre-commit fault cells, not response-loss/LSO/retention recovery, Pulsar
multi-Broker failover, multi-shard placement, the remaining chaos matrix or
V1 release proof.

## Current-source Kafka Fetch/LSO/retention and Pulsar multi-Broker receipt refresh

These focused runs refresh the release-critical broker evidence at Delay
`883352e2bdc4f376cbf892020b0e8f02e8319797`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

Kafka Fetch response-loss + LSO:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-20260817-r3 \
KAFKA_BROKER_1_PORT=31940 KAFKA_BROKER_2_PORT=31941 KAFKA_BROKER_3_PORT=31942 \
KAFKA_DELAY_FETCH_RESPONSE_LOSS_TOPIC=nereus-delay-fetch-response-loss-20260817-r3 \
  bash e2e/run-kafka-real-client-e2e.sh
```

Receipt: project `nereus-delay-kafka-e2e-1786928641-71203`,
`BUILD SUCCESSFUL in 49s`; `responseDiscardedAfterFetch=true`,
`replayOffset=0`, `secondOffset=1`, `fetchLso=2`,
`committedAfterReplay=2`.

Kafka retention floor + LSO:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-20260817-r3 \
KAFKA_BROKER_1_PORT=31950 KAFKA_BROKER_2_PORT=31951 KAFKA_BROKER_3_PORT=31952 \
KAFKA_DELAY_RETENTION_FLOOR_TOPIC=nereus-delay-retention-floor-20260817-r3 \
  bash e2e/run-kafka-real-client-e2e.sh
```

Receipt: project `nereus-delay-kafka-e2e-1786928713-71988`,
`BUILD SUCCESSFUL in 30s`; `oldOffset=0`, `retentionFloor=20`,
`endOffset=21`, `staleOffsetRejected=true`, `floorFetchOffset=20`,
`fetchLso=21`.

Pulsar multi-Broker Worker process-crash failover:

```bash
NEREUS_DELAY_PULSAR_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_PULSAR_WITH_OXIA=1 NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH=1 \
NEREUS_DELAY_PULSAR_OXIA_PORT=31980 \
PULSAR_BROKER_1_PORT=31970 PULSAR_WEB_1_PORT=31971 \
PULSAR_BROKER_2_PORT=31972 PULSAR_WEB_2_PORT=31973 \
PULSAR_DELAY_MULTI_BROKER_RESTART_TOPIC=p1-multi-worker-20260817-r3 \
PULSAR_DELAY_MULTI_BROKER_DESTINATION_TOPIC=p1-multi-destination-20260817-r3 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-multi-broker-20260817-r3 \
  bash e2e/run-pulsar-multi-broker-failover-e2e.sh
```

Receipt: Compose project `nereus-delay-pulsar-multi-e2e-1786928804-72884`,
Oxia project `nereus-delay-pulsar-multi-oxia-e2e-1786928804-72884`,
preparation `BUILD SUCCESSFUL in 52s`, recovery `BUILD SUCCESSFUL in 37s`.
broker-1 was SIGKILLed after guarded preparation; broker-2 recovered the real
Oxia assignment/Owner, guarded SUBSCRIBE, apply/ACK and destination readback,
then broker-1 rejoined.

The three runs left no matching containers, networks, volumes, listeners or
per-run images. Only the locked Oxia/MinIO bases remain; do not use a global
Docker prune for this cleanup. These are bounded current-source receipts and
do not close coordinator/controller or storage failover, multi-shard
placement, the remaining chaos matrix or V1 release proof.

## Current-source bounded capacity/SLO probe refresh

Run the local source-locked probe from a clean Delay worktree:

```bash
NEREUS_DELAY_CAPACITY_ARTIFACT_DIR=/tmp/nereus-delay-capacity-current-20260817-r1 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-capacity-gradle-current-20260817-r1 \
  bash e2e/run-bounded-capacity-slo-probe.sh
```

At Delay `c12c23c248adfb9f19238c4315a58b2eb6613d22`, the test reported
`BUILD SUCCESSFUL in 1m 15s` and wrote
`/tmp/nereus-delay-capacity-current-20260817-r1/bounded-capacity-slo-probe.json`.
The artifact status is `PARTIAL`: the platform probe was `UNAVAILABLE` because
`MaxDirectMemorySize` was not explicitly bounded. The available store receipt
verified 16 records at each payload size `256/4096/65536`, persistent reopen,
and the available SLO receipt exported and reopened 24 durable samples.

This is bounded local evidence only. It does not close the required benchmark
campaign, production resource authority, multi-Worker placement, restore
throughput, fairness/SLO denominator, long-cycle soak or V1 release gate.

## Current-source Initial Route control apply regression

The source-ordered kind-14 apply boundary is covered without Docker by the
focused test and the full Gradle check:

```bash
GRADLE_USER_HOME=/Users/liusinan/.gradle \
  ./gradlew test --tests io.nereusstream.delay.runtime.InitialRouteControlApplyTest \
  --no-daemon --console=plain

GRADLE_USER_HOME=/Users/liusinan/.gradle \
  ./gradlew check --no-daemon --console=plain --quiet
```

At Delay `f6b7c4ee`, the focused test covers first apply, exact replay,
same-snapshot stale handling, divergent/tampered rejection and restart
readback. The first apply uses the existing `meta/FIXED` key 10 rather than a
new metadata key and shares one Store WriteBatch with the mutation result and
Source Position. This is not a real Broker/Oxia/Gateway E2E and does not close
kind-1 protocol activation, eligible-reader cutover, downgrade or V1 release
gates. No Docker resources or images are created by this regression.

## Current-source bounded Linux platform capacity probe

The bounded capacity/SLO probe can also run inside a pinned Linux container so
the platform portion sees real cgroup v2, procfs, rlimit and filesystem
authority:

```bash
NEREUS_DELAY_CAPACITY_CONTAINER_ARTIFACT_DIR=/tmp/nereus-delay-capacity-linux-20260817-r2 \
NEREUS_DELAY_CAPACITY_CONTAINER_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-bounded-capacity-slo-container-probe.sh
```

At Delay `84003e7aa55b7a5278cab45b606b941cdef3bcec`, the runner used
`eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769`
on Linux aarch64 with a 2 GiB memory cgroup, 2 CPUs, a 65536 FD limit and an
`exec` 4 GiB temporary filesystem. It completed with `BUILD SUCCESSFUL in
18s` and wrote a JSON-valid artifact at
`/tmp/nereus-delay-capacity-linux-20260817-r2/bounded-capacity-slo-probe.json`.

The platform probe was `AVAILABLE`: JVM heap `536870912`, direct memory
`268435456`, process RSS `216797184`, cgroup memory `2147483648`, max open
files `65536`, current open files `97`, filesystem `4294967296` and usable
filesystem `4279943168`. The store still verified 16 records at each of
`256/4096/65536` bytes and persistent reopen; the SLO outbox exported and
reopened 24 durable samples. The artifact remains `PARTIAL` by design because
this is a bounded platform receipt, not the required size/burst/Lane/shard/
restore benchmark campaign or long-cycle soak. Gate 6 still requires the
complete certified envelope, reserve/fairness, adapter/zombie and durable SLO
capacity evidence.

The run used no application network, Broker, Oxia or MinIO resources. The
temporary JDK image was removed after the receipt's exact postcheck; the
locked Oxia and MinIO bases were retained. No global Docker prune was used.

## Current-source Kafka and Pulsar two-shard Worker fleet receipts

The native two-shard Route/Assignment/Owner path was rerun against the current
clean source with the isolated K1 and P1 checkouts. Kafka used broker ports
`32000/32001/32002`, Oxia `32010`, project
`nereus-delay-kafka-e2e-1786930684-99099` and Oxia project
`nereus-delay-kafka-oxia-e2e-1786930684-99099`; Pulsar used broker/web
`32020/32021`, Oxia `32030`, project
`nereus-delay-pulsar-e2e-1786930684-99098` and Oxia project
`nereus-delay-pulsar-oxia-e2e-1786930684-99098`.

Both source-locked commands used `*_WITH_OXIA=1` and `*_MULTI_SHARD_ONLY=1`.
Kafka completed with `BUILD SUCCESSFUL in 1m 8s` and printed
`fetchPartitions=2, routeRevision=1, assignmentRevisions=[1, 1],
workers=[kafka-route-worker-a, kafka-route-worker-b], sourceBarriers=[1, 1]`;
Pulsar completed with `BUILD SUCCESSFUL in 1m 4s` and printed
`subscribePartitions=2, routeRevision=1, assignmentRevisions=[1, 1],
workers=[pulsar-route-worker-b, pulsar-route-worker-a],
sourceBarriers=[10/0, 9/0]`:

The Kafka K1 image was `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`;
the P1 image was `sha256:a2c76925f2504337a55c1b88d0a83cc80147d563189041514b63bc1e347cf9d3`.
The real Oxia Assignment/Owner CAS paths admitted both guarded native source
consumers, one shared Worker fleet applied and ACKed both partitions, and both
final checkpoints/assignments were released.

This is a current-source bounded placement receipt, not catalog-driven
production placement or release certification. Multi-shard large-payload
egress, arbitrary placement churn, controller/coordinator/storage failover,
target isolation, full chaos, benchmark/soak and V1 release gates remain open.
Exact postchecks found no project containers, networks, volumes, listeners or
temporary images; locked Oxia/MinIO bases were retained and no global Docker
prune was used.

## Current-source Gateway/Oxia session-expiry chaos cell

The bounded chaos runner now includes `gateway-oxia-session-churn` as its
thirteenth focused cell. The current source-locked run used Delay
`f6acacdca87b6e91a953030f5a523e39df5ed314`, Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786931032-4431`, Oxia port `32060` and Gateway port
`32070`. The session was stopped for two seconds and restarted while the old
Gateway process remained alive.

The smoke completed with `BUILD SUCCESSFUL in 21s` and printed:

```text
Gateway Oxia session churn E2E passed: stale durable sessions failed closed and recovery reread one exact outcome
Dockerized Gateway Oxia session churn smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

This closes the bounded single-node Oxia session-expiry/stale-session cell
only. It does not certify transparent reconnect, Gateway HA, coordinator or
provider failover, target isolation, full chaos, benchmark/soak or V1 release
readiness. The runner cleanup now removes its exact generated Oxia image as
well as the Compose project; postchecks found no project resource, listener or
temporary image, and no global Docker prune was used.

## Current-source 13-cell bounded chaos matrix

The clean current-source run used Delay `80fdb63d3512be8fcb3af51c7f9e0aa5bba9382f`, K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Running `e2e/run-bounded-chaos-matrix.sh` produced `/tmp/nereus-delay-chaos-current-20260817-r2` and returned `matrix_status=0` for all 13 cells:

```text
kafka-broker-process-crash=0
kafka-worker-ack-process-crash=0
kafka-broker-tcp-cut=0
kafka-broker-network-partition=0
pulsar-worker-process-crash=0
pulsar-multi-broker-process-crash=0
pulsar-worker-admission-response-loss=0
checkpoint-reaping=0
kafka-fetch-response-loss=0
kafka-retention-floor=0
pulsar-destination-response-loss=0
pulsar-source-ack-response-loss=0
gateway-oxia-session-churn=0
matrix_status=0
```

This is a bounded current-source chaos receipt, not release certification. It covers real Broker/Worker cuts, response-loss replay, Kafka LSO/retention, Pulsar multi-Broker failover, Checkpoint REAPING and Gateway/Oxia session expiry. Catalog placement, target isolation, controller/coordinator/storage/provider failover, full large-payload fault coverage, benchmark/soak, activation/cutover and V1 release gates remain open.

Postchecks found no matrix containers, networks, project volumes, listeners or generated related images. Locked Oxia and MinIO images were retained as reusable bases; pre-existing unlabelled `pulsarconf`/`pulsardata` volumes were left untouched because they were outside this run's ownership. No global Docker prune was used.

## Current-source Linux bounded capacity/benchmark matrix

The clean run used Delay `4713a54c983a025bbd1bda64dd25831416642fe1` and the pinned image `eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769`. `e2e/run-bounded-capacity-matrix.sh` produced `/tmp/nereus-delay-capacity-matrix-current-20260817-r4/capacity-benchmark-matrix.json` with `matrix_status=PASS_BOUNDED`:

```text
smoke: records/size=16, SLO samples=24, payload sizes=256/4096/65536, records/s=6125/24230/5479
burst: records/size=256, SLO samples=128, payload sizes=256/4096/65536, records/s=39448/54007/6800
sustained: records/size=1024, SLO samples=512, payload sizes=256/4096/65536, records/s=70346/56423/7610
```

All cases reported `store.reopen_verified=true` and `collector_reopen_verified=true`; the Linux platform authority was available under the bounded 2 GiB/256 MiB/65536-nofile/4 GiB-tmpfs container. This remains a bounded local capacity receipt, not the required Broker throughput, Lane/shard, compaction/restore, inline-object, soak or V1 release campaign. The exact generated JDK image was removed and no matrix resource remained.

## Current-source Kafka two-shard Large Payload Object Store authority

Run the opt-in two-shard authority cell from the Delay checkout:

```bash
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=32545 \
KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=32546 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=32547 \
NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=32645 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=32745 \
NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=32845 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
./e2e/run-large-payload-gateway-e2e.sh
```

This mode creates a two-partition source topic, appends source-ordered
activation/pre-route records, proves guarded Fetch/LSO barriers, publishes one
signed Route, places two Oxia Assignment/Owner leases, and drives both shards
through one Worker fleet. Each partition then completes Gateway mTLS/JWT
Prepare, real MinIO upload/attest/Commit/readback, exact Prepare idempotency,
Worker apply/ACK and final checkpoint/Owner release. The current source-locked
receipt is Delay `048b4d8f220557d510ced088999f94077bc253d4`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Kafka client source
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The run used project `nereus-delay-large-payload-e2e-1786934597-44345` and
reported `fetchPartitions=2`, `routeRevision=1`,
`workers=[kafka-large-payload-worker-b, kafka-large-payload-worker-a]`,
`sourceBarriers=[2, 2]`, `exactGatewayIdempotency=true`; both partitions had
Prepare/Commit offsets `2/3`. Destination egress is intentionally disabled in
this mode, and MinIO/Broker failover modes are rejected. Pulsar multi-shard
Large Payload, multi-shard destination egress, placement churn/failover,
benchmark/soak and V1 release certification remain open.

The runner removes its exact Compose containers, networks, volumes, listeners,
temporary Kafka image and generated TLS/receipt files after the run. Locked
Oxia/MinIO bases are retained; no global Docker prune is used.

## Current-source canonical bounded chaos and V1 release-candidate gate

Run the canonical 13-cell matrix from the clean Delay checkout:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-release-20260817-r1 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-release-gradle-20260817-r1 \
bash e2e/run-bounded-chaos-matrix.sh
```

With Delay `fe62065750f86b607d4c395afd52197e3cb31008`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, the canonical JSON index is
`/tmp/nereus-delay-chaos-release-20260817-r1/bounded-chaos-matrix.json` and
reports `matrix_status=PASS_BOUNDED` for all 13 cells. This is bounded
evidence, not release certification.

Run the fail-closed release-candidate audit with the bounded artifacts:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/tmp/nereus-delay-v1-release-gate-20260817-r1 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/tmp/nereus-delay-v1-release-gradle-20260817-r1 \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/tmp/nereus-delay-chaos-release-20260817-r1/bounded-chaos-matrix.json \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/tmp/nereus-delay-capacity-matrix-current-20260817-r4/capacity-benchmark-matrix.json \
NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=1 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
bash e2e/run-v1-release-gate.sh
```

The resulting `/tmp/nereus-delay-v1-release-gate-20260817-r1/v1-release-candidate-gate.json`
has `release_status=NOT_READY`: source checks, contract validation and full
Gradle `check` pass, but `PARTIAL`/`PASS_BOUNDED` do not satisfy the required
`PASS_CERTIFIED` status, and certified soak, activation/cutover and operations
artifacts are missing. `ALLOW_NOT_READY=1` is only an audit-mode escape for
recording the negative result.

Both runners remove their exact project resources and generated images. The
canonical rerun left no related containers, networks or volumes and no
run-created Kafka/Pulsar/Oxia/Gateway image IDs; locked Oxia/MinIO bases remain.
Do not use a global Docker prune for this workflow.

## Source-ordered Protocol Version activation projection

Delay commit `1c924f479c284161771c24b013622f645c4fab06` implements the local
source-ordered kind-1 activation projection. `ProtocolActivationStateV1` is
stored at `meta/FIXED` key 14 with ValueEnvelope type 11 and retains the exact
tuple, schema hash, compatible-reader-set evidence hash, marker source
position and System Mutation ID. Kind-14 Initial Route control creates the
empty projection atomically with the key-10 control snapshot, result and
source cursor.

The focused test command is:

```bash
GRADLE_USER_HOME=/tmp/nereus-delay-protocol-activation-gradle-20260817-r1 \
./gradlew test \
  --tests io.nereusstream.delay.protocol.ProtocolActivationStateV1Test \
  --tests io.nereusstream.delay.runtime.InitialRouteControlApplyTest \
  --tests io.nereusstream.delay.runtime.ProtocolVersionActivationApplyTest \
  --no-daemon --console=plain
```

The current receipt passed state canonical round-trip, source-ordered marker
apply, pre-marker `UNACTIVATED_PROTOCOL_VERSION`, post-marker command apply,
and restart recovery. This is a local activation/cutover binding, not a
certified external Worker rollout or downgrade/release artifact. The V1 gate
continues to require `PASS_CERTIFIED` activation, soak, benchmark and
operations evidence.

The latest source-locked gate receipt is
`/tmp/nereus-delay-v1-release-gate-20260817-r3/v1-release-candidate-gate.json`
at Delay `7835a4c4bb5ac8e083c73885047c4165918cbdab`, with K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source/contract/full-Gradle
checks pass and the gate remains `NOT_READY` for the bounded/partial/missing
certification inputs. The first fresh-cache attempt encountered an external
Maven Central TLS handshake failure during Checkstyle dependency resolution;
the known-good-cache rerun passed.

## Reproducible protocol activation/cutover smoke

Run it from the clean Delay full-v1 checkout:

```bash
NEREUS_DELAY_PROTOCOL_ACTIVATION_ARTIFACT_DIR=/tmp/nereus-delay-protocol-activation-cutover-20260817-r1 \
NEREUS_DELAY_PROTOCOL_ACTIVATION_GRADLE_USER_HOME=/tmp/nereus-delay-protocol-activation-full-check-20260817-r1 \
bash e2e/run-protocol-activation-cutover-smoke.sh
```

The current source lock is `b0d2f757716d24cbf148a6990daeaf555cfa1369` and the
receipt is
`/tmp/nereus-delay-protocol-activation-cutover-20260817-r1/protocol-activation-cutover.json`
with `status=PASS_BOUNDED`. It runs the canonical activation-state, Initial
Route and source-ordered marker/restart tests and checks the resulting JSON.
The receipt is local projection evidence only: external Oxia eligible-reader
rollout, writer-before-reader orchestration, downgrade/release packaging and
real Broker/Pulsar cutover remain open. This runner uses no Docker resources,
so there are no related images to clean.

The current-source release-gate rerun is:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/tmp/nereus-delay-v1-release-gate-20260817-r4 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/tmp/nereus-delay-protocol-activation-full-check-20260817-r1 \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/tmp/nereus-delay-chaos-release-20260817-r1/bounded-chaos-matrix.json \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/tmp/nereus-delay-capacity-matrix-current-20260817-r4/capacity-benchmark-matrix.json \
NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT=/tmp/nereus-delay-protocol-activation-cutover-20260817-r1/protocol-activation-cutover.json \
NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=1 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
bash e2e/run-v1-release-gate.sh
```

It produced `release_status=NOT_READY` at Delay
`3e21eb072f41014ed893ef5799817f2f8cb305cb`: source/contract/full-check PASS,
activation `PASS_BOUNDED` recorded but blocked by the `PASS_CERTIFIED` rule,
capacity `PARTIAL`, chaos bounded, and soak/operations missing.

## Current-source Pulsar Large Payload production-authority rerun

Run the single-shard complete authority path with an isolated port set:

```bash
PULSAR_LARGE_BROKER_1_PORT=32900 \
PULSAR_LARGE_WEB_1_PORT=32901 \
PULSAR_LARGE_BROKER_2_PORT=32902 \
PULSAR_LARGE_WEB_2_PORT=32903 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=32910 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=32911 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=32912 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-activation-20260817-r1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-activation-20260817-r1 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

At Delay `7ca4cd89d6f2f7fc5a4309dc3a383e5f34f736a6`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and MinIO
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
the run passed in `1m 15s`. It completed guarded recovery, Gateway mTLS/JWT
Prepare, real MinIO upload/attest/Commit/readback, Worker apply/ACK, typed
`PULSAR_SEND_ACK`, source-applied physical Publish, exact destination payload
readback, final checkpoint and Owner release. Prepare/Commit were `3/2` and
`3/3`, typed target `4/0`, and exact Prepare replay kept source record count
at `6`.

This is a current-source single-shard PASS; Pulsar multi-shard Large Payload,
full infrastructure failover, benchmark/soak and V1 release certification
remain open. The runner removed project
`nereus-delay-pulsar-large-e2e-1786937594-84236`, all its volumes/networks,
listeners and generated P1/Oxia images. The locked MinIO base was retained;
no global Docker prune was used.

## Release artifact source-lock enforcement

The release runner now requires every `PASS_CERTIFIED` artifact to include
exact current `source_locks.delay`, `.kafka`, `.pulsar` and `.oxia`; missing or
stale locks are blocked. Commit `41b66de37980ecca624c0f2d69cbd52307d8d452`
was verified with:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/tmp/nereus-delay-v1-release-gate-20260817-r5 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/tmp/nereus-delay-protocol-activation-full-check-20260817-r1 \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/tmp/nereus-delay-chaos-release-20260817-r1/bounded-chaos-matrix.json \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/tmp/nereus-delay-capacity-matrix-current-20260817-r4/capacity-benchmark-matrix.json \
NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT=/tmp/nereus-delay-protocol-activation-cutover-20260817-r1/protocol-activation-cutover.json \
NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=1 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
bash e2e/run-v1-release-gate.sh
```

The receipt is `release_status=NOT_READY`: source/contract/full-check PASS,
but bounded/partial/missing inputs remain blocked. This runner does not use
Docker.

## Current-source bounded operations drills

Run the source-locked operations receipt from a clean full-v1 checkout:

```bash
NEREUS_DELAY_OPERATIONS_DRILLS_ARTIFACT_DIR=/tmp/nereus-delay-operations-20260817-r2 \
NEREUS_DELAY_OPERATIONS_DRILLS_GRADLE_USER_HOME=/tmp/nereus-delay-protocol-activation-full-check-20260817-r1 \
NEREUS_DELAY_OPERATIONS_CHECKPOINT_OXIA_PORT=31510 \
NEREUS_DELAY_OPERATIONS_CHECKPOINT_MINIO_PORT=31511 \
bash e2e/run-bounded-operations-drills.sh
```

The current artifact is
`/tmp/nereus-delay-operations-20260817-r2/operations-drills.json` with
`status=PASS_BOUNDED`. It locks Delay
`441a148ba4570ba0af3b6c2cfb7af3d324690954`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The local probe passed restore,
catalog, Owner recovery/drain, DLQ replay and source-ordered UNCERTAIN tests;
the real Oxia + MinIO probe passed checkpoint publication and exact REAPING.

This is bounded operations evidence, not `PASS_CERTIFIED`: external operator
authorization, fresh-process disaster continuity and certified multi-Worker
soak remain open. The runner performs exact cleanup of this run's Compose
resources and generated Oxia image, verifies ports `31510/31511` are free and
retains only the locked MinIO/Oxia bases. No global Docker prune is used.

The release gate can record this artifact, but it must remain blocked until the
artifact is independently promoted to `PASS_CERTIFIED` with exact current
four-repository source locks and the required operations authority evidence.

The current-source gate rerun with this bounded operations artifact is:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/tmp/nereus-delay-v1-release-gate-20260817-r6 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/tmp/nereus-delay-protocol-activation-full-check-20260817-r1 \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/tmp/nereus-delay-chaos-release-20260817-r1/bounded-chaos-matrix.json \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/tmp/nereus-delay-capacity-matrix-current-20260817-r4/capacity-benchmark-matrix.json \
NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT=/tmp/nereus-delay-protocol-activation-cutover-20260817-r1/protocol-activation-cutover.json \
NEREUS_DELAY_RELEASE_GATE_OPERATIONS_ARTIFACT=/tmp/nereus-delay-operations-20260817-r2/operations-drills.json \
NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=1 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
bash e2e/run-v1-release-gate.sh
```

It produced `release_status=NOT_READY` at Delay
`d405d2fa00bcaf99a0d34c892291ea0a425d4c47`: source/contract/full-check PASS,
operations recorded but blocked as `PASS_BOUNDED`, capacity `PARTIAL`, chaos
bounded, activation bounded and certified soak missing. The gate used no
Docker and `ALLOW_NOT_READY=1` only retained the negative audit.

## Current-source Pulsar Large Payload Broker failover

Run the combined Gateway/Oxia/Worker/MinIO failover cut with an isolated
two-Broker port set:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
PULSAR_LARGE_BROKER_1_PORT=33000 \
PULSAR_LARGE_WEB_1_PORT=33001 \
PULSAR_LARGE_BROKER_2_PORT=33002 \
PULSAR_LARGE_WEB_2_PORT=33003 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=33010 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=33011 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=33012 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-failover-20260817-r1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-activation-20260817-r1 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The clean Delay source was `11728ea29b6b27d8a314b0afc1c7805cd0af4e1f`, with
P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and the locked MinIO digest. The
run passed in 56 seconds: broker-1 stopped after Gateway Commit/readback, the
same source-applied physical Publish completed through broker-2, and exact
payload readback passed. Admission was `2/4`, typed target `7/0`, Outcome
`2/5`, Prepare/Commit were `2/2` and `2/3`, and exact Prepare replay left six
source records.

This is single-shard failover evidence, not Pulsar multi-shard production,
controller/storage/provider failover, soak or V1 release certification. The
runner removed project `nereus-delay-pulsar-large-e2e-1786938863-99638`, its
containers/networks/volumes/listeners and generated P1/Oxia images; locked
MinIO/Oxia bases remain and no global Docker prune was used.

The current-source release-gate rerun after this receipt is
`/tmp/nereus-delay-v1-release-gate-20260817-r7/v1-release-candidate-gate.json`
at Delay `9ec909d95b890dd227b572396091e500a9c72299`; source/contract/full-check
PASS, capacity `PARTIAL`, activation/operations/chaos bounded, certified soak
missing, and overall `release_status=NOT_READY`.

## Pulsar Large Payload network-partition failover receipt

The source-locked two-Broker run used:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS=75 \
PULSAR_LARGE_BROKER_1_PORT=33100 \
PULSAR_LARGE_WEB_1_PORT=33101 \
PULSAR_LARGE_BROKER_2_PORT=33102 \
PULSAR_LARGE_WEB_2_PORT=33103 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=33110 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=33111 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=33112 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-network-failover-20260817-r2 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-activation-20260817-r1 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

At Delay `fc004146b807087fcd72ee7188419eaa8f6eac06`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
project `nereus-delay-pulsar-large-e2e-1786939347-6325` passed in 2m 7s.
After Gateway Commit/readback, broker-1 stayed alive but was disconnected from
the exact Compose network; after the 75-second ownership handoff broker-2
completed the same source-applied physical Publish and broker-1 rejoined.
Admission source was `5/0`, typed `PULSAR_SEND_ACK` target `3/0`, Outcome
source `5/1`, `prepare=2/2`, `commit=2/3`, `sourceRecords=6`, and exact 1 MiB
payload readback plus `exactGatewayIdempotency=true` passed.

This closes only the bounded single-shard Pulsar Broker network-partition cell;
multi-shard Large Payload, controller/storage/provider failover, certified soak
and V1 release certification remain open. Exact postchecks found no matching
containers, networks, volumes, listeners or generated P1/Oxia images. Locked
Oxia/MinIO bases were retained; no global Docker prune or unrelated image
deletion was performed.

## Current-source release-gate rerun after network-partition receipt

The current artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r8/v1-release-candidate-gate.json`
at Delay `54759958b0c7af41ffa2374d835831ec7df72d13`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source/contract/full-check all
passed. The fail-closed result is `release_status=NOT_READY`: capacity is
`PARTIAL`, certified soak is missing, and activation, operations and chaos are
`PASS_BOUNDED` and remain blocked by the `PASS_CERTIFIED` rule.

## Current-source Pulsar Large Payload multi-shard authority

The opt-in multi-shard receipt uses two real Pulsar physical source
partitions, two Oxia Assignment/Owner records and one fair Worker fleet. It
proves the Object Store authority path through Gateway mTLS/JWT and real
MinIO, but intentionally does not run destination egress in the same receipt.
The topic base must not contain `-partition-` because Pulsar reserves that
pattern for generated physical partition names.

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
PULSAR_LARGE_BROKER_1_PORT=33400 \
PULSAR_LARGE_WEB_1_PORT=33401 \
PULSAR_LARGE_BROKER_2_PORT=33402 \
PULSAR_LARGE_WEB_2_PORT=33403 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=33410 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=33411 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=33412 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-multi-20260817-r3 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-multi-20260817-r3 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The current PASS receipt is Delay
`801e5be6a931f0dc4c5e991b79f099fdc6fd1b02`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
It passed barriers `3/1` and `4/1`, Prepare/Commit `3/2,3/3` and
`4/2,4/3`, exact object versions
`59ecd3d5-60c0-43e7-a583-a6f78e9c7d49` and
`ace5c3a2-a148-4d2a-afc8-5b5872012f9f`, exact Gateway idempotency, source
record count `4` per partition and final checkpoint/Owner release. Gradle
reported `BUILD SUCCESSFUL in 1m 1s`.

The runner removes its exact Compose containers, networks, volumes, listeners
and generated P1/Oxia images. The locked Oxia/MinIO images are retained; no
global Docker prune or unrelated image deletion is part of this receipt.

## Current-source bounded fault matrix and release gate

The current-source matrix command is:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-release-20260817-r2 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-release-20260817-r2/gradle-user-home \
bash e2e/run-bounded-chaos-matrix.sh
```

It produced
`/tmp/nereus-delay-chaos-release-20260817-r2/bounded-chaos-matrix.json` with
all 13 cells passing and `matrix_status=PASS_BOUNDED`, source-locked to Delay
`3370bfbeb03a26186156528507e379dcb1dd3021`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The matching gate artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r11/v1-release-candidate-gate.json`:
source, cross-repository and full Gradle checks pass, but
`release_status=NOT_READY` because capacity is `PARTIAL`, certified soak is
missing and bounded activation/operations/chaos evidence cannot satisfy
`PASS_CERTIFIED`. Matrix cleanup is exact and run-scoped; no global Docker
prune or unrelated image deletion is allowed.

## Current-source Pulsar multi-shard Large Payload destination egress receipt

The opt-in multi-shard runner now includes guarded destination egress. It
creates two destination physical partitions and carries the explicit
partition through Lane activation, admission, transport, PUBLISH_OUTCOME and
consumer readback:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
PULSAR_LARGE_BROKER_1_PORT=34300 \
PULSAR_LARGE_WEB_1_PORT=34301 \
PULSAR_LARGE_BROKER_2_PORT=34302 \
PULSAR_LARGE_WEB_2_PORT=34303 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=34310 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=34311 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=34312 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=pulsar-large-payload-multi-egress-20260817-r12 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-gradle-r12 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The current-source r12 receipt used Delay
`ee292f4090e23a3f26f949aa54ac075b8ed94a78`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1 distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image `sha256:a2c76925f2504337a55c1b88d0a83cc80147d563189041514b63bc1e347cf9d3`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Project `nereus-delay-pulsar-large-e2e-1786945120-74832` passed:

```text
partition=0 barrier=3/1 prepare=3/2 commit=3/3 objectVersion=994e6c4a-d337-4d7f-99a5-4b99ea353252 destinationEgress=true sourceRecords=6
partition=1 barrier=4/1 prepare=4/2 commit=4/3 objectVersion=2be1f1ad-68ef-4717-be73-379dcaaa8964 destinationEgress=true sourceRecords=6
typed PULSAR_SEND_ACK targets=5/0 and 6/0; Outcome sources=3/5 and 4/5; exact payload readback on both destination partitions
exactGatewayIdempotency=true
BUILD SUCCESSFUL in 1m 34s
```

The matching release-gate artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r13/v1-release-candidate-gate.json`.
Source, cross-repository and full Gradle checks pass, but the fail-closed
result remains `release_status=NOT_READY`: capacity is `PARTIAL`, certified
soak is absent and activation/cutover, operations and chaos are bounded rather
than `PASS_CERTIFIED`. The exact r12 Compose resources and generated P1/Oxia
images were removed; locked MinIO and unrelated pre-existing images were
retained, with no global Docker prune.

## Current-source Kafka multi-shard Large Payload destination egress receipt

The Kafka multi-shard runner now exercises the complete destination boundary
when `NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC` is set. It creates a
two-partition target topic and a matching two-partition `-receipt` topic, then
fences target and receipt partitions through the same guarded transaction:

```bash
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=34700 \
KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=34701 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=34702 \
NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=34710 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=34711 \
NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=34712 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=kafka-large-payload-multi-egress-20260817-r3 \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-large-payload-gradle-r1 \
bash e2e/run-large-payload-gateway-e2e.sh
```

The current-source r3 receipt used Delay
`b641fc714db779787054811f7229709b1a3fa0ba`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, K1 client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, K1
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Project `nereus-delay-large-payload-e2e-1786946121-90342` passed:

```text
partition=0 barrier=2 prepare=2 commit=3 typedReceipt=0 admission=4 outcome=5 objectVersion=7626b7fe-8deb-404a-9e24-af68a69dbc3c destinationEgress=true sourceRecords=6
partition=1 barrier=2 prepare=2 commit=3 typedReceipt=0 admission=4 outcome=5 objectVersion=34f981ac-b7f1-444a-a2ea-546b2e69e3a0 destinationEgress=true sourceRecords=6
exact payload readback on destination partitions 0 and 1; exactGatewayIdempotency=true
BUILD SUCCESSFUL in 44s
```

The matching release gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r15/v1-release-candidate-gate.json`.
Source, cross-repository and full Gradle checks pass, but the fail-closed
result remains `release_status=NOT_READY`: capacity is `PARTIAL`, certified
soak is absent and activation/cutover, operations and chaos are bounded rather
than `PASS_CERTIFIED`. Exact r3 Compose resources and generated K1/Oxia images
were removed; locked MinIO and unrelated pre-existing images were retained,
with no global Docker prune.

## Current-source Gateway/Oxia session-fence chaos refresh

The current matrix uses Delay
`56f39ff80ee32ff46ce7086895a3b875d7284134`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-release-20260817-r4 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-release-20260817-r4/gradle-user-home \
bash e2e/run-bounded-chaos-matrix.sh
```

The canonical artifact
`/tmp/nereus-delay-chaos-release-20260817-r4/bounded-chaos-matrix.json` is
`PASS_BOUNDED` with all 13 cells passing. The Gateway cell holds Oxia down
while old session-bound handles fail closed, then starts Oxia after the
recovery-ready barrier so fresh sessions reread the exact durable outcome.
This is bounded evidence, not V1 release certification.

The matching gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r17/v1-release-candidate-gate.json`;
source, cross-repository and full Gradle checks pass, but the result remains
`NOT_READY` because capacity is `PARTIAL`, certified soak is missing and
activation/cutover, operations and chaos are `PASS_BOUNDED`.

Postchecks found no run-owned Compose containers, volumes, networks or
generated images. The locked MinIO base and unrelated `alpine:3.17` were
retained; no global Docker prune or unrelated image deletion was used.

## Current-source bounded capacity refresh and r20 gate

At Delay `0f04415e3c8abcf17952ae3f5c5e4796bb797831`,
`e2e/run-bounded-capacity-matrix.sh` produced
`/tmp/nereus-delay-capacity-matrix-current-20260817-r9/capacity-benchmark-matrix.json`.
The pinned Linux JDK image was explicitly pulled, used for smoke/burst/
sustained Store/SLO cases, and removed by exact runner cleanup. The artifact
is `PARTIAL/PASS_BOUNDED`; all three cases reopened Store and the durable SLO
collector. It does not cover real Broker throughput, placement, large-scale
records, compaction/restore, inline/object flow or soak.

The current-source gate
`/tmp/nereus-delay-v1-release-gate-20260817-r20/v1-release-candidate-gate.json`
passes source/contract/Gradle checks but remains `NOT_READY`: capacity is
`PARTIAL`, certified soak is missing and activation/cutover, operations and
chaos are still bounded rather than `PASS_CERTIFIED`.

## Current-source production-chain rerun and r19 gate

The clean current candidate reran the Kafka and Pulsar two-shard Large Payload
destination paths with Delay `59abbde18ad2b0b5551e4ea59c5fc146db068982`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Both runners crossed the real
Gateway mTLS/JWT, Oxia Assignment/Owner, guarded source, Worker,
MinIO upload/attest/Commit/readback, typed destination, Outcome and
checkpoint/release path for both partitions. They exited zero and exact
postchecks found no run-owned containers, networks, volumes or generated
Kafka/Pulsar/Oxia images. The locked MinIO base and unrelated images were
retained; no global Docker prune was used.

The subsequent current-source release artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r19/v1-release-candidate-gate.json`.
Source, cross-repository and full Gradle checks pass, but the result remains
`NOT_READY`: capacity is `PARTIAL`, certified soak is missing, and bounded
activation, operations and chaos artifacts cannot satisfy `PASS_CERTIFIED`.

## 20. Current sequential bounded fault matrix and release gate

The canonical current-source matrix was run with:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-current-20260817-r7 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-gradle-current-20260817-r7 \
bash e2e/run-bounded-chaos-matrix.sh
```

`/tmp/nereus-delay-chaos-current-20260817-r7/bounded-chaos-matrix.json` is
`matrix_status=PASS_BOUNDED`; all 13 cells passed: Kafka broker process crash,
Kafka Worker ACK process crash, Kafka broker TCP cut, Kafka broker network
partition, Pulsar Worker process crash, Pulsar multi-Broker process crash,
Pulsar Worker admission response loss, checkpoint reaping, Kafka Fetch
response loss, Kafka retention floor, Pulsar destination response loss,
Pulsar source ACK response loss and Gateway/Oxia session churn. The artifact
locks Delay `1dd68005e18d3a7422a2fae653750372a5841421`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The activation artifact
`/tmp/nereus-delay-protocol-activation-current-20260817-r2/protocol-activation-cutover.json`
and operations artifact
`/tmp/nereus-delay-operations-current-20260817-r4/operations-drills.json`
are both `PASS_BOUNDED`. Activation covers the local source-ordered projection
and restart digest; operations covers local recovery drills and the real
Oxia/MinIO checkpoint plus exact `REAPING` path. Neither is a certified
operator, rollout or production-soak receipt.

The matching release artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r22/v1-release-candidate-gate.json`.
Source cleanliness, cross-repository validation and full Gradle `check` pass,
but the explicit decision is `release_status=NOT_READY`: capacity is
`PARTIAL`, certified soak is missing, and bounded activation, operations and
chaos evidence cannot satisfy `PASS_CERTIFIED`. The receipts are source-locked
to the runtime candidate above; this documentation append does not refresh
those locks.

### Exact Docker cleanup

Completed runs removed their exact Compose resources and generated K1/P1/Oxia/
Gateway images. The one interrupted Pulsar run was cleaned explicitly by
removing image
`nereus-delay-pulsar-p1:nereus-delay-pulsar-multi-e2e-1786950538-53376`,
container
`4afc75a5ddc037ec77841f4ab0e90009abaf374bf941ffe66a858a1bb20c1fa3` and
volume `nereus-delay-pulsar-multi-e2e-1786950538-53376_zk-data`.

Final exact postchecks found no `nereus-delay-*` containers, networks, volumes
or generated images. Retain the locked MinIO base
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
and unrelated images; do not use global Docker prune or delete unrelated
images.

## 21. Current-source real MinIO 5xx/timeout/config-drift fault E2E

The current fault runner produces a canonical JSON receipt and exact cleanup:

```bash
NEREUS_DELAY_MINIO_FAULT_ARTIFACT_DIR=/tmp/nereus-delay-minio-fault-current-20260817-r3 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-minio-fault-current-20260817-r1 \
NEREUS_DELAY_MINIO_FAULT_MINIO_PORT=31651 \
NEREUS_DELAY_MINIO_FAULT_PROXY_PORT=31652 \
NEREUS_DELAY_MINIO_BUCKET=nereus-delay-fault-current-r3 \
bash e2e/run-minio-fault-e2e.sh
```

`/tmp/nereus-delay-minio-fault-current-20260817-r3/minio-fault-e2e.json` is
`status=PASS`, source-locked to Delay `b982f423e0f6f3d7627e6f0fabfbed1e36c85498`,
and uses MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The four real tests cover 503 after Commit, 503 before Commit, timeout after
Commit and credential configuration drift. Exact postchecks confirm the
fault-run container is gone and no generated `nereus-delay-*` image remains.

This is a bounded real Object Store fault receipt. It does not satisfy the
broader capacity/soak/production chaos `PASS_CERTIFIED` requirements.

## Real Oxia multi-node Gateway leader failover with canonical receipt

`run-oxia-multi-node-gateway-e2e.sh` builds the locked Oxia checkout into a
three-coordinator/three-DataServer Compose project, creates one replicated
namespace shard, and runs the Gateway mTLS/Oxia client test across an injected
DataServer leader stop. It now emits a canonical JSON receipt and removes only
the exact project and generated images.

The current clean run was:

```bash
NEREUS_DELAY_OXIA_MULTI_NODE_GATEWAY_ARTIFACT_DIR=/tmp/nereus-delay-oxia-multi-node-gateway-current-20260817-r2 \
NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-multi-node-gateway-gradle-current-20260817-r1 \
NEREUS_DELAY_OXIA_COORDINATOR_1_PORT=35191 \
NEREUS_DELAY_OXIA_COORDINATOR_2_PORT=35192 \
NEREUS_DELAY_OXIA_COORDINATOR_3_PORT=35193 \
NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT=35181 \
NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT=35182 \
NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT=35183 \
NEREUS_DELAY_GATEWAY_PORT=35158 \
bash e2e/run-oxia-multi-node-gateway-e2e.sh
```

Receipt: `/tmp/nereus-delay-oxia-multi-node-gateway-current-20260817-r2/oxia-multi-node-gateway-e2e.json`.
It reports `status=PASS`, Delay `53c9fc0c7b1609ba37109536326dad330d994ebb`,
K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The observed transition was
`ds-2 -> ds-1`; the Gateway test exited zero with `BUILD SUCCESSFUL in 16s`.
The artifact retains six generated image IDs before cleanup and proves empty
exact Compose container/network/volume/image postchecks.

This is bounded real Oxia leader-stop/Gateway recovery evidence. It does not
certify Gateway HA, coordinator/storage failover, placement churn, disaster
continuity, certified soak or V1 `PASS_CERTIFIED` release readiness. Locked
reusable bases remain; no global Docker prune is used.

## Current-source r25 release-gate refresh

The source-locked gate after the real Oxia/Gateway receipt is
`/tmp/nereus-delay-v1-release-gate-20260817-r25/v1-release-candidate-gate.json`.
Delay `6a5cd494d7122a01d666cd681a3dac7fe6e11769`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` are clean; source, contract and
full Gradle checks pass. The gate remains `release_status=NOT_READY` because
capacity is `PARTIAL`, certified soak is absent, and activation/operations/
chaos are `PASS_BOUNDED`, not `PASS_CERTIFIED`. The Oxia/Gateway artifact is
bounded runtime evidence and is not a substitute for those certification
inputs; this README append does not refresh the r25 source lock.

## 24. Strictly sequential bounded production-chain soak

`run-bounded-production-chain-soak.sh` composes the real Gateway, Oxia,
Kafka/Pulsar, Worker and MinIO links into one bounded current-source receipt.
It runs cases serially and records source locks, child logs, receipt markers,
exit codes, exact Compose projects and post-cleanup checks.

The current receipt is:

```text
/tmp/nereus-delay-production-chain-soak-current-20260817-r2/production-chain-soak.json
```

It is `status=PASS_BOUNDED` for one cycle of four cases: Kafka multi-shard
destination, Pulsar multi-shard destination, Kafka MinIO timeout-after-Commit,
and Pulsar MinIO 503-after-Commit. All four child runners exited 0 and passed
the real authority/readback assertions. The receipt locks Delay
`57a02095e51bf6c143aef57c330b415f95b61e96`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

Each case requires exact Docker cleanup. Only its Compose project, labeled
resources and generated provider images are eligible for removal; the locked
MinIO base and unrelated images are retained. The current postcheck found no
matching run containers, networks, volumes or generated images. No global
Docker prune was used. This bounded artifact is functional/runtime evidence,
not `PASS_CERTIFIED` release evidence; the full fresh-process fault matrix,
capacity envelope, certified long-cycle soak, rollout compatibility and
disaster-continuity requirements remain outside this runner.

## Current-source r28 release-gate refresh

The matching gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r28/v1-release-candidate-gate.json`.
Source locks, the cross-repository contract validator and full Gradle `check`
passed; the gate remains `release_status=NOT_READY` because capacity,
certified soak, activation, operations and chaos require separate
`PASS_CERTIFIED` artifacts. The bounded production-chain receipt is not a
promotion input. This README append does not refresh the r28 source lock.

## Current-source r29 release-gate refresh

The final gate artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r29/v1-release-candidate-gate.json`.
Source locks, the cross-repository validator and full Gradle `check` pass for
Delay `830fce40c77c52a3a8b25d657355db9abee851c4`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The result remains
`release_status=NOT_READY`; bounded soak evidence is not a
`PASS_CERTIFIED` substitute. This README append does not refresh the r29
source lock.

## Certified production-chain soak harness

`run-certified-production-chain-soak.sh` wraps the bounded four-case runner
with an explicit profile, strict sequential execution, process RSS/FD and
artifact-size observations, measured sample coverage, duration and exact
Docker postchecks. It refuses to start when the profile or numeric policy is
missing, and it emits `PASS_CERTIFIED` only for the exact recorded profile and
four-repository source lock.

The current harness-integration receipt is:

```text
/tmp/nereus-delay-certified-soak-harness-20260817-r5/certified-production-chain-soak.json
```

It reports `PASS_CERTIFIED` for profile
`harness-integration-production-chain-r1`, with Delay
`8f6fddd4c3e626a90bbe73be1360398c78114065`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. One cycle passed Kafka and
Pulsar multi-shard destination egress plus Kafka timeout-after-Commit and
Pulsar 503-after-Commit real MinIO cases. Child runtime was 269 seconds with
36 samples and an 8-second maximum sample gap; process peak RSS was
`1003392 KiB`, peak FD count `1151`, and all exact cleanup arrays were empty.

This profile is explicitly harness integration evidence, not V1 release
approval. The release gate requires the certified schema, policy/coverage
observations and `NEREUS_DELAY_RELEASE_GATE_CERTIFIED_SOAK_PROFILE_ID` to
match an approved release profile. The §23.5 longest checkpoint/floor/retry/
uncertainty/GC soak, capacity, full chaos, activation, operations,
upgrade/downgrade and disaster gates remain separate. Generated run images
are removed; the locked MinIO base and canonical Oxia image are retained, and
no global Docker prune is used.

## Current-source r30 release-gate refresh

The final gate artifact for the documented source is
`/tmp/nereus-delay-v1-release-gate-20260817-r30/v1-release-candidate-gate.json`.
Delay `b9a7fa9994542b9bc9630d7b12c63ade2fc1c57b`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` passed source,
cross-repository and full Gradle checks. The gate remains
`release_status=NOT_READY`; certified capacity, soak, activation,
operations and chaos inputs are absent, and the bounded harness receipt is
not a release substitute. This README append does not refresh the r30 source
lock.

## 2026-08-21 Evidence-manifest protocol for complete V1

The certified-chaos r19 receipt and release-gate r20 receipt are retained as
historical artifacts at Delay cec7641b96a57d3108723c8cb27eb51594846543:

~~~text
/private/tmp/nereus-delay-v1-certified-chaos-20260821-r19/certified-chaos-matrix.json
/private/tmp/nereus-delay-v1-release-gate-20260821-r20/v1-release-candidate-gate.json
~~~

They do not certify complete V1. The final process freezes a four-repository
candidate source lock first, runs ten independent exact-source certified gate
inputs, then permits one documentation-only overlay across the six evidence
ledgers. An external manifest records the candidate lock, overlay commit,
SHA-256 bytes for every ledger and artifact, all ten gate statuses, and the
final release-gate status. Validate it with:

~~~bash
NEREUS_DELAY_EVIDENCE_MANIFEST=/private/tmp/<final>/v1-evidence-manifest.json \
  bash e2e/verify-v1-evidence-manifest.sh
~~~

The verifier fails closed for dirty worktrees, non-allowlisted source changes,
post-freeze ledger edits, missing or changed receipts, non-certified gate
inputs, and any source-lock mismatch. Because the manifest is outside the
checkout, the documentation overlay does not recursively hash the manifest
that describes it.

The stable release command is full-scope and requires the external candidate
lock plus one artifact for each of the ten main-design gates:

~~~bash
NEREUS_DELAY_RELEASE_GATE_CANDIDATE_SOURCE_LOCK=/private/tmp/<candidate>/source-lock.json \
NEREUS_DELAY_RELEASE_GATE_PROTOCOL_GOLDEN_ARTIFACT=/private/tmp/<final>/protocol-golden.json \
NEREUS_DELAY_RELEASE_GATE_CHAOS_FULL_ARTIFACT=/private/tmp/<final>/chaos.json \
NEREUS_DELAY_RELEASE_GATE_REAL_SERVICE_ARTIFACT=/private/tmp/<final>/real-service.json \
NEREUS_DELAY_RELEASE_GATE_NO_EARLY_ARTIFACT=/private/tmp/<final>/no-early.json \
NEREUS_DELAY_RELEASE_GATE_BENCHMARK_ARTIFACT=/private/tmp/<final>/benchmark.json \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_FULL_ARTIFACT=/private/tmp/<final>/capacity.json \
NEREUS_DELAY_RELEASE_GATE_SOAK_FULL_ARTIFACT=/private/tmp/<final>/soak.json \
NEREUS_DELAY_RELEASE_GATE_UPGRADE_DOWNGRADE_ARTIFACT=/private/tmp/<final>/upgrade-downgrade.json \
NEREUS_DELAY_RELEASE_GATE_OPERATIONS_FULL_ARTIFACT=/private/tmp/<final>/operations.json \
NEREUS_DELAY_RELEASE_GATE_PATCH_DISTRIBUTION_ARTIFACT=/private/tmp/<final>/patch-distribution.json \
  bash e2e/run-v1-release-gate.sh
~~~

Every input must be `nereus-delay-v1-full-gate-input-v1` with
`scope=full-v1`, `complete_v1=true`, `PASS_CERTIFIED`, exact candidate locks,
empty exclusions and empty boundaries. Existing bounded RC1 receipts are
rejected by construction; until the full ten-gate set exists the result is
expected to remain `NOT_READY`.

## Current-source r32 release-gate refresh

The current gate artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r32/v1-release-candidate-gate.json`.
Delay `5d282244524de0d002cc7122ebf389150a4fd9f2`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` passed source,
cross-repository and full Gradle checks. The result remains
`release_status=NOT_READY`; bounded chaos is not a `PASS_CERTIFIED`
release input, and the other four certification inputs are absent.

## Current-source 13-cell bounded chaos refresh

The clean rerun is recorded at
`/tmp/nereus-delay-chaos-current-20260817-r3/bounded-chaos-matrix.json`.
It is `matrix_status=PASS_BOUNDED` with all 13 focused cells returning zero,
source-locked to Delay `8cfa6acc97a7a966e76b0ce086572c53cd731f7d`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The matrix covers Broker/Worker crashes, TCP/network cuts, response loss,
Kafka Fetch/retention, Pulsar failover and ACK paths, checkpoint REAPING and
Gateway/Oxia session churn. Exact run-scoped Docker resources and generated
images were removed; only locked Oxia/MinIO bases remain. This receipt is
bounded evidence, not §23.3 completion or V1 release certification; long GC,
half-open, ENOSPC, fsync/SST, target isolation and controller/storage/provider
cuts remain outside the runner.

## Current-source audited 13-cell bounded chaos r5

The audited current-source receipt is
`/tmp/nereus-delay-chaos-current-20260817-r5/bounded-chaos-matrix.json`.
It reports `PASS_BOUNDED` with all 13 cells returning zero, locked to Delay
`75b347da58a4086d19df912ca82f974401432f44`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

Unlike a plain exit-code matrix, the runner now checks each cell's required
source/target/authority markers and records its injection point, expected
state, duplicate boundary and fresh-process recovery status. All 13 audits
passed. Six crash/network cells are `fresh_process_recovery=PASS`; the other
seven response-loss/checkpoint/session cells are explicitly `NOT_COVERED`.
`durable_state_dump=NOT_CAPTURED` and `invariant_audit=MARKER_ONLY` remain
visible in every cell, so this is bounded evidence rather than §23.3 completion
or `PASS_CERTIFIED` release evidence. Exact cleanup left only the locked Oxia
and MinIO bases; no global Docker prune was used.

## Certified bounded-capacity benchmark

`run-certified-capacity-benchmark.sh` wraps
`run-bounded-capacity-matrix.sh` with a named profile, fixed three-case policy,
Worker resource observations, Store/SLO reopen assertions and exact Docker
cleanup. It writes the integration receipt at:

```text
/tmp/nereus-delay-certified-capacity-harness-20260817-r3/certified-capacity-benchmark.json
```

The recorded profile is
`harness-integration-bounded-capacity-r1`. It covers smoke/burst/sustained
payload runs of 16/256/1024 records per payload size and 24/128/512 durable SLO
samples. The child matrix remains `PARTIAL`/`PASS_BOUNDED`; wrapper
`PASS_CERTIFIED` is scoped to the named profile and exact four-repository
source locks, not to V1 capacity approval.

The release gate accepts the strict schema only when
`NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CAPACITY_PROFILE_ID` is supplied and
matches the receipt. The wrapper removes only the exact temporary JDK image it
pulled; pre-existing Oxia/MinIO bases are retained, and global Docker prune is
forbidden. Broker/Lane throughput, placement/fairness, Control Reserve,
Adapter/zombie, restore, inline/object, upgrade/downgrade and long-cycle soak
remain outside this bounded profile.

## Current-source bounded-chaos r6

The 13-cell current-source matrix is run with:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-current-20260817-r6 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-gradle-current-20260817-r6 \
bash e2e/run-bounded-chaos-matrix.sh
```

The receipt is
`/tmp/nereus-delay-chaos-current-20260817-r6/bounded-chaos-matrix.json`.
It reports `matrix_status=PASS_BOUNDED` and all 13 cells exit zero. The
`pulsar-worker-admission-response-loss` cell additionally captures and
independently audits a forced durable dump before SIGKILL and a fresh-process
recovery dump after restart: `PUBLISHING`/`outcome_applied=false` becomes
`PUBLISHED`/`outcome_applied=true` with the same Store and Publish Attempt
identity.

The other cells remain explicitly bounded marker evidence; durable dumps and
independent invariant audits are not implied by a zero exit code. Matrix-level
release certification is open and the V1 gate remains `NOT_READY`.

The runner performs exact run-scoped cleanup. Verify that generated
Delay/Kafka/Pulsar/Oxia/MinIO/Gateway containers, networks, volumes and images
are absent afterward. The canonical Oxia and locked MinIO bases may remain for
reuse. No global Docker prune is allowed, and unrelated images must be left
untouched.

## Current HEAD release gate r37

The current-source fail-closed gate receipt is:

```text
/tmp/nereus-delay-v1-release-gate-20260817-r37/v1-release-candidate-gate.json
```

It records clean source checkouts, passing cross-repository validation and a
passing full Gradle check, but `release_status=NOT_READY` because this gate
invocation did not receive approved certified capacity, soak,
activation/cutover, operations or release-certified chaos artifacts. A
`PASS_BOUNDED` receipt is not a release certificate.

The completed cleanup removed stale top-level `/private/tmp/nereus-delay*`
logs, retries and build caches only. Five canonical evidence directories were
kept so the latest recovery, chaos, capacity, soak and gate receipts remain
available. No source worktree, Git metadata, or unrelated Docker resource was
deleted.

## Current-source Gateway large-payload multi-shard authority E2E

Kafka requires an explicit destination topic in multi-shard mode:

```bash
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-payload-destination-current-20260817 \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/private/tmp/nereus-delay-large-payload-kafka-gradle-current-20260817 \
bash e2e/run-large-payload-gateway-e2e.sh
```

Pulsar multi-shard mode:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/private/tmp/nereus-delay-pulsar-large-payload-gateway-gradle-current-20260817 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The current successful logs are
`/tmp/nereus-delay-large-payload-gateway-current-20260817-r1/kafka-multi-shard.log`
and
`/tmp/nereus-delay-large-payload-gateway-current-20260817-r1/pulsar-multi-shard.log`.
They prove the two-protocol signed-Route → guarded source barriers → real
Oxia Assignment/Owner → Gateway mTLS/JWT → Worker → real MinIO
upload/attestation/commit/readback → two destination `PUBLISHED` outcomes →
checkpoint chain. The exact Compose resources and generated images are empty
after cleanup; the Gradle homes are disposable run caches and are not retained.

This is functional E2E evidence, not `PASS_CERTIFIED` release evidence. The
full chaos/capacity/soak/activation/operations/upgrade/disaster gates remain
independent.

## Current HEAD gate r38

The latest fail-closed receipt is
`/tmp/nereus-delay-v1-release-gate-20260817-r38/v1-release-candidate-gate.json`.
Source, cross-repository and full Gradle checks pass at the current HEAD, but
`release_status=NOT_READY` because approved capacity, soak,
activation/cutover, operations and release-certified chaos artifacts were not
supplied. Delete the r38 Gradle user home after the run; retain the receipt.

## Current-source bounded-chaos r7

The 14-cell current-source matrix is run with:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/private/tmp/nereus-delay-chaos-current-20260817-r7 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/private/tmp/nereus-delay-chaos-gradle-current-20260817-r7 \
bash e2e/run-bounded-chaos-matrix.sh
```

The receipt is
`/private/tmp/nereus-delay-chaos-current-20260817-r7/bounded-chaos-matrix.json`.
It reports `matrix_status=PASS_BOUNDED` and all 14 cells exit zero under
Delay `9a6f171ab817607ff59d18a4e963ae0a8504e281`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The `pulsar-worker-admission-response-loss` and
`pulsar-worker-destination-response-loss` cells independently audit durable
before/after dumps: `PUBLISHING` with `outcome_applied=false` becomes fresh
process `PUBLISHED` with `outcome_applied=true`, matching Store/DB/attempt/
message identities. The destination cell also proves exact payload readback
and no second SEND. Both report `CAPTURED_AND_VERIFIED` and
`INDEPENDENT_FIELDS_PASS`; the other 12 cells retain their explicit
marker-only/not-captured boundary.

This is bounded fault evidence, not `PASS_CERTIFIED` release evidence. The
matrix release slot is still open and the V1 gate remains `NOT_READY`. After
the run, generated run-scoped Docker resources must be empty; keep the r7
receipt and two state-dump directories, remove only the disposable Gradle
cache, and do not use global Docker prune.

## Current HEAD release gate r39

The latest fail-closed receipt is:

```text
/private/tmp/nereus-delay-v1-release-gate-20260817-r39/v1-release-candidate-gate.json
```

It records clean source checkouts, passing cross-repository validation and a
passing full Gradle check at Delay
`a783c5e292dde247b2a79f04078e122057917ad4`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. It remains
`release_status=NOT_READY`: approved capacity, soak, activation/cutover,
operations and release-certified chaos artifacts were not supplied. The r7
`PASS_BOUNDED` receipt is scoped evidence and is not a release certificate.
The r39 Gradle user home is disposable and was removed after the check.

## RC1 source-locked evidence refresh (2026-08-20)

The RC1 candidate locks are the exact four-repository `source_locks` recorded
in each canonical receipt and in the final gate; use those fields as the
authority instead of copying a hash into a command or narrative.

The fresh bounded capacity and production-chain soak commands produce these
canonical receipts:

```text
/private/tmp/nereus-delay-v1-rc1-capacity-20260820-r2/certified-capacity-benchmark.json
/private/tmp/nereus-delay-v1-rc1-soak-20260820-r1/certified-production-chain-soak.json
```

Both report `PASS_CERTIFIED` only for their named profiles and exact locks.
Capacity covers the fixed three-case Store/SLO policy (3,888 payload records
and 664 SLO samples). Soak covers four serial real Kafka/Pulsar/Oxia/Worker/
MinIO cases with passing invariants, duration and resource coverage. Neither
receipt is the complete V1 capacity or release-soak certificate.

The current-source activation, operations and chaos receipts are:

```text
/private/tmp/nereus-delay-v1-rc1-activation-20260820-r1/protocol-activation-cutover.json
/private/tmp/nereus-delay-v1-rc1-operations-20260820-r1/operations-drills.json
/private/tmp/nereus-delay-v1-rc1-chaos-20260820-r2/bounded-chaos-matrix.json
```

Activation and operations remain `PASS_BOUNDED`. Chaos is
`matrix_status=PASS_BOUNDED` with 14/14 zero cells. Its Pulsar Admission and
destination response-loss cells independently audit durable before/after
state; the other 12 cells retain marker-only and/or not-captured boundaries.

Run the fail-closed gate with the exact artifacts:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/private/tmp/nereus-delay-v1-rc1-gate-20260820-r2 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/private/tmp/nereus-delay-v1-rc1-chaos-gradle-20260820-r2 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-capacity-20260820-r2/certified-capacity-benchmark.json \
NEREUS_DELAY_RELEASE_GATE_SOAK_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-soak-20260820-r1/certified-production-chain-soak.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CAPACITY_PROFILE_ID=nereus-delay-v1-rc1-bounded-capacity-r1 \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_SOAK_PROFILE_ID=nereus-delay-v1-rc1-production-chain-soak-r1 \
NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-activation-20260820-r1/protocol-activation-cutover.json \
NEREUS_DELAY_RELEASE_GATE_OPERATIONS_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-operations-20260820-r1/operations-drills.json \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-chaos-20260820-r2/bounded-chaos-matrix.json \
bash e2e/run-v1-release-gate.sh
```

The canonical gate is
`/private/tmp/nereus-delay-v1-rc1-gate-20260820-r2/v1-release-candidate-gate.json`.
Source, cross-repository and full Gradle checks pass; activation, operations
and certified chaos remain blocked by bounded status, so the result is
`release_status=NOT_READY`. Exact Docker postchecks are empty; retain only
the canonical receipt directories and locked base images, and remove
disposable Gradle homes without touching source worktrees.

## Current-source Kafka source durable chaos slice and certified wrapper (2026-08-20)

The focused Kafka Fetch response-loss and retention-floor runners now support
an explicit prepare/resume split. The first JVM persists the real Broker
response-loss or stale-offset-rejection observation, the second JVM reopens the
same topic identity and Route/partition boundary, and both write a forced
`nereus-delay-chaos-durable-state-dump-v1` JSON record. The bounded matrix
compares the exact fields instead of accepting a log marker: source offsets and
LSO/retention floor, topic identity, group/Route identity, commit/floor Fetch,
`durable_broker_read`, `dump_forced` and distinct process PIDs.

The diagnostic focused run was recorded under
`/private/tmp/nereus-delay-v1-kafka-durable-20260820-r1/`; its Fetch and
retention directories are implementation evidence, not a release artifact,
because the run preceded the final source-lock commit. The generated K1
Compose projects and images were removed by each runner's exact trap, and the
postcheck was empty.

`e2e/run-certified-chaos-matrix.sh` is now the fail-closed certification
boundary. It requires an explicit profile and current four-repository locks,
then accepts `PASS_CERTIFIED` only when all fourteen declared cells have
marker PASS, durable before/after dumps, fresh-process recovery,
`INDEPENDENT_FIELDS_PASS`, and empty run-scoped Docker postchecks. The current
matrix has only four cells at that evidence level, so the wrapper must remain
`BLOCKED`; `e2e/run-v1-release-gate.sh` validates this schema and profile rather
than promoting `PASS_BOUNDED` evidence.

## 2026-08-21 RC1 source-lock refresh and cleanup boundary

This is the current evidence handoff. Earlier receipt sections are frozen
history; use only the receipts listed here for the final gate. The refresh is
performed after the documentation commit containing this section, so the
Delay SHA is read from each receipt's `source_locks.delay`. The remaining
source locks are K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

Canonical receipts:

```text
/private/tmp/nereus-delay-v1-rc1-capacity-20260821-r4/certified-capacity-benchmark.json
/private/tmp/nereus-delay-v1-rc1-soak-20260821-r7/certified-production-chain-soak.json
/private/tmp/nereus-delay-v1-rc1-activation-20260821-r5/protocol-activation-cutover.json
/private/tmp/nereus-delay-v1-rc1-operations-20260821-r4/operations-drills.json
/private/tmp/nereus-delay-v1-certified-chaos-20260821-r5/certified-chaos-matrix.json
/private/tmp/nereus-delay-v1-rc1-release-gate-20260821-r4/v1-release-candidate-gate.json
```

Capacity, production-chain soak, activation/cutover and operations are
expected to report `PASS_CERTIFIED`. The certified chaos wrapper remains
`BLOCKED` because only 4/14 cells currently have durable state dumps, fresh
process recovery and independent-field invariants; its 14/14 bounded child
cells may report `PASS_BOUNDED` without becoming release certification. The
fail-closed V1 gate is consequently `release_status=NOT_READY`.

Run the final gate with:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/private/tmp/nereus-delay-v1-rc1-release-gate-20260821-r4 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/private/tmp/nereus-delay-v1-rc1-soak-20260820-r3/gradle-user-home/kafka \
NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=1 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-capacity-20260821-r4/certified-capacity-benchmark.json \
NEREUS_DELAY_RELEASE_GATE_SOAK_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-soak-20260821-r7/certified-production-chain-soak.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CAPACITY_PROFILE_ID=nereus-delay-v1-rc1-bounded-capacity-r1 \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_SOAK_PROFILE_ID=nereus-delay-v1-rc1-production-chain-soak-r1 \
NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-activation-20260821-r5/protocol-activation-cutover.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_ACTIVATION_PROFILE_ID=nereus-delay-v1-rc1-activation-r1 \
NEREUS_DELAY_RELEASE_GATE_OPERATIONS_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-operations-20260821-r4/operations-drills.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_OPERATIONS_PROFILE_ID=nereus-delay-v1-rc1-operations-r1 \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/private/tmp/nereus-delay-v1-certified-chaos-20260821-r5/certified-chaos-matrix.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CHAOS_PROFILE_ID=nereus-delay-v1-rc1-chaos-r1 \
bash e2e/run-v1-release-gate.sh
```

Before accepting the receipt, require full Gradle, all source-lock checks and
the cross-repository validator to pass. The run-scoped Docker containers,
networks, volumes and generated images must be empty afterwards. Keep only
the canonical receipts, `nereus/oxia-o1:37a17bef1720` and
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`; move exact disposable
Gradle/diagnostic paths to Trash. Do not use a global Docker prune, broad
globs or recursive deletion against a source/worktree root.

## 2026-08-21 current-source chaos slice and post-documentation receipt

The implementation slice pushed immediately before this section is
`71068209dff3915e17ac2d81324154d79074e6f5` (`test: wait for all Kafka brokers
before chaos`). Together with `9a55403e1f493fad8db73956db9dcd50c4429964`, it
adds the Kafka Worker ACK crash cell's durable before/after Store dumps,
fresh-JVM recovery and native Kafka `commitSync` proof. The recovery test
reuses the durable Store root and real Oxia Owner Lease, but its local
Recovery Catalog/Floor authority remains an explicit test seam; this is not a
claim of full production Recovery Catalog authority.

The pre-documentation current-source wrapper receipt is:

```text
/private/tmp/nereus-delay-v1-certified-chaos-20260821-r9/certified-chaos-matrix.json
```

Its exact source locks are Delay `71068209dff3915e17ac2d81324154d79074e6f5`,
K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The bounded child is
`PASS_BOUNDED` with 14/14 cells passing in this rerun, while the wrapper is
`BLOCKED`: durable-state, fresh-process and invariant evidence is still
`FAIL`, and only 5/14 cells have the required durable dumps. Docker postcheck
is `PASS`. An earlier same-source r8 run recorded a timing-sensitive Kafka
network-partition resume timeout; r9 passed the bounded child, so this history
must not be promoted to certified stability.

The matching pre-documentation gate receipt is:

```text
/private/tmp/nereus-delay-v1-rc1-release-gate-20260821-r5/v1-release-candidate-gate.json
```

It records source, cross-repository and full Gradle checks as `PASS`, but
`release_status=NOT_READY` because the older capacity/soak/activation/
operations receipts are not source-locked to this implementation and chaos is
not `PASS_CERTIFIED`.

After this documentation commit, regenerate the source-locked follow-up
receipts before using them as current gate inputs:

```bash
NEREUS_DELAY_CERTIFIED_CHAOS_ARTIFACT_DIR=/private/tmp/nereus-delay-v1-certified-chaos-20260821-r10 \
NEREUS_DELAY_CERTIFIED_CHAOS_GRADLE_USER_HOME=/private/tmp/nereus-delay-chaos-gradle-20260821 \
NEREUS_DELAY_CERTIFIED_CHAOS_PROFILE_ID=nereus-delay-v1-rc1-chaos-r2 \
bash e2e/run-certified-chaos-matrix.sh

NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/private/tmp/nereus-delay-v1-rc1-release-gate-20260821-r6 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/private/tmp/nereus-delay-chaos-gradle-20260821 \
NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=1 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-capacity-20260821-r4/certified-capacity-benchmark.json \
NEREUS_DELAY_RELEASE_GATE_SOAK_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-soak-20260821-r7/certified-production-chain-soak.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CAPACITY_PROFILE_ID=nereus-delay-v1-rc1-bounded-capacity-r1 \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_SOAK_PROFILE_ID=nereus-delay-v1-rc1-production-chain-soak-r1 \
NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-activation-20260821-r5/protocol-activation-cutover.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_ACTIVATION_PROFILE_ID=nereus-delay-v1-rc1-activation-r1 \
NEREUS_DELAY_RELEASE_GATE_OPERATIONS_ARTIFACT=/private/tmp/nereus-delay-v1-rc1-operations-20260821-r4/operations-drills.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_OPERATIONS_PROFILE_ID=nereus-delay-v1-rc1-operations-r1 \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/private/tmp/nereus-delay-v1-certified-chaos-20260821-r10/certified-chaos-matrix.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CHAOS_PROFILE_ID=nereus-delay-v1-rc1-chaos-r2 \
bash e2e/run-v1-release-gate.sh
```

The post-documentation receipts are authoritative only through their own
`source_locks`; the expected fail-closed result remains certified chaos
`BLOCKED` and V1 `NOT_READY` until all fourteen cells independently provide
durable state, fresh-process recovery and invariant evidence. Keep only
canonical receipts and locked base images; move superseded exact paths to
Trash after checking that no process is using them. Source worktrees and Git
metadata are never cleanup targets.

## 2026-08-21 Pulsar Worker process-crash evidence slice

The next chaos slice is pushed as
`83a47900ef3de4cfa110f7ca43d13fcde1376628` (`test: certify Pulsar Worker
process crash recovery`). It adds external fsync-forced before/after Store
dumps for the existing real Pulsar Worker process-crash path and independently
compares the physical topic, Route/shard identity, Store incarnation, DB
identity, source apply/ACK boundary and fresh JVM PID. The focused receipt is:

```text
/private/tmp/nereus-delay-v1-pulsar-worker-process-crash-20260821-r1/before-process-crash.json
/private/tmp/nereus-delay-v1-pulsar-worker-process-crash-20260821-r1/after-fresh-process.json
```

The focused real-broker E2E passed with Store/DB identity unchanged, PID
`750 -> 847`, source apply/ACK `false -> true`, real Oxia Owner Lease and final
checkpoint. The existing r10 certified wrapper and r6 gate are now historical
pre-slice receipts because their Delay source lock predates this commit. After
this documentation update, regenerate the authoritative current-source
wrapper and gate at r11 and r7; until then the release remains fail-closed.

## 2026-08-21 current-source Large Payload production-authority soak

Before this documentation change, the current implementation passed the
strict-sequential bounded production-chain soak at:

```text
/private/tmp/nereus-delay-v1-production-chain-soak-20260821-r1/production-chain-soak.json
```

The receipt has schema `nereus-delay-bounded-production-chain-soak-v1`, status
`PASS_BOUNDED`, one cycle and four of four cases `PASS`. Its exact source locks
are Delay `d5dfa990c22f7659ebdb68f84e800646f34e7d46`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The four real production-authority cases were:

- Kafka two-shard destination: signed Route, Oxia Assignment/Owner, one Worker
  fleet, Gateway mTLS/JWT, real MinIO, two destination `PUBLISHED` outcomes,
  exact payload readback and object versions
  `6f92b8cb-7c08-4cf2-818f-7a9b5e43342b` and
  `7d88d85d-52bc-4109-8b0c-8fb6e35369d9`.
- Pulsar two-shard destination: two guarded source barriers, Oxia
  multi-shard Assignment/Owner, two Workers, real MinIO, two destination
  `PUBLISHED` outcomes, exact payload readback and object versions
  `5cbbf2ca-44a4-4993-bcc5-360ef1be6903` and
  `9dbda3ce-6eef-45db-adfe-93dfb2b99b6c`.
- Kafka `PUT_TIMEOUT_AFTER_COMMIT`: the after-commit uncertainty path closed
  with source apply/ACK, destination readback and exact Gateway idempotency.
- Pulsar `PUT_503_AFTER_COMMIT`: the after-commit uncertainty path closed with
  source apply/ACK, destination readback and exact Gateway idempotency.

Every case reports exact per-Compose cleanup `PASS`: no run-scoped container,
network, volume or generated provider image remained; the locked MinIO base was
retained. This is current-source functional and bounded production-chain
evidence, not release certification. It does not satisfy the fourteen-cell
fresh-process chaos matrix, certified capacity/soak, activation/cutover,
operations, upgrade or disaster-continuity gates.

Because this documentation commit changes the Delay source lock, r1 is a
pre-documentation receipt. Regenerate the current handoff after the commit:

```bash
NEREUS_DELAY_PRODUCTION_SOAK_ARTIFACT_DIR=/private/tmp/nereus-delay-v1-production-chain-soak-20260821-r2 \
NEREUS_DELAY_PRODUCTION_SOAK_GRADLE_USER_HOME=/private/tmp/nereus-delay-production-chain-gradle-20260821-r2 \
NEREUS_DELAY_PRODUCTION_SOAK_CYCLES=1 \
NEREUS_DELAY_PRODUCTION_SOAK_BASE_PORT=36100 \
bash e2e/run-bounded-production-chain-soak.sh
```

Only r2's exact `source_locks` may be used as the post-documentation
production-chain handoff. Keep the release gate fail-closed until the
independently source-locked certified inputs and all fourteen chaos cells pass.

## 2026-08-21 checkpoint-reaping fresh-process evidence slice

Delay commit `6e163de1` adds a real two-JVM checkpoint REAPING cut to the
existing Oxia+MinIO harness. The WRITE JVM creates the durable
`PENDING_UPLOAD` intent, uploads the exact versioned prefix, explicitly
abandons the session-bound Owner and fsync-forces the pre-recovery dump. The
READ JVM reconnects to real Oxia, proves the Owner is absent, wins the
`PENDING_UPLOAD -> REAPING` CAS and performs the exact MinIO version sweep.

Focused evidence:

```text
/private/tmp/nereus-delay-checkpoint-reaping-fresh-20260821-r1/before-process-crash.json
/private/tmp/nereus-delay-checkpoint-reaping-fresh-20260821-r1/after-fresh-process.json
```

The receipt passed with distinct JVM PIDs `35845` and `35997`. Intent digest,
Route/partition, recovery lineage, checkpoint ID and source-store incarnation
were unchanged; the after dump recorded `REAPING`, `listed=2`, `deleted=2` and
`prefix_empty=true`, with Owner absence and forced durable reads. The run-scoped
Compose resources and generated Oxia image were cleaned; locked base images
remain retained. This closes the `checkpoint-reaping` cell boundary only; the
14-cell chaos union and V1 release gate remain fail-closed until a new
source-locked matrix run.

## 2026-08-21 Pulsar destination response-loss fresh-process slice

Delay commit `b42135d4` adds a two-JVM real P1 destination response-loss cut.
The WRITE JVM performs exactly one guarded SEND, discards the completion after
the broker has committed it, and fsync-forces the full request plus guarded
SEND evidence. The READ JVM creates no Producer and sends nothing: it
reconstructs and validates the typed `PULSAR_SEND_ACK`, then reads the same
real topic and checks the exact payload count.

Focused evidence:

```text
/private/tmp/nereus-delay-pulsar-destination-response-loss-fresh-20260821-r1/before-process-crash.json
/private/tmp/nereus-delay-pulsar-destination-response-loss-fresh-20260821-r1/after-fresh-process.json
```

The receipt passed with PIDs `42487 -> 42581`, broker position `ledger=9,
entry=0, sequence=0`, equal topic/guard/attempt/prepared-hash/payload fields,
and `physical_send_count=1`, `duplicate_payload_count=0`. The P1 and Oxia
Compose resources and generated images were cleaned; locked base images remain.
This closes the direct `pulsar-destination-response-loss` cell only. It does
not replace the separate Worker destination-response-loss cell, the complete
chaos union or the V1 release gate.

## 2026-08-21 Kafka Broker process-crash durable rejoin evidence

Delay commit `75a008fc` adds an independently auditable durable-state path to
the existing real Kafka Broker process-crash cut. After guarded Worker
preparation, a separate Java Admin JVM force-dumps the real topic/cluster
identity, replicas, ISR, live Broker IDs and end offset. The runner then
SIGKILLs `kafka-1`, waits for survivor leader convergence, resumes the Worker
through `kafka-2,kafka-3`, starts `kafka-1`, waits for its port and ISR rejoin,
and finally runs a fresh Admin JVM for the post-rejoin dump.

Run the focused cell with:

```bash
artifact_dir=/private/tmp/nereus-delay-kafka-broker-process-crash-20260821-r3
mkdir -p "${artifact_dir}"
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_OXIA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/oxia \
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_STATE_DUMP_DIR="${artifact_dir}/state" \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/Users/liusinan/.gradle \
KAFKA_BROKER_1_PORT=31540 KAFKA_BROKER_2_PORT=31541 KAFKA_BROKER_3_PORT=31542 \
NEREUS_DELAY_KAFKA_OXIA_PORT=31550 \
KAFKA_DELAY_BROKER_PROCESS_CRASH_TOPIC=nereus-delay-broker-crash-20260821-r3 \
bash e2e/run-kafka-real-client-e2e.sh
```

The focused receipt is:

```text
/private/tmp/nereus-delay-kafka-broker-process-crash-20260821-r3/state/before-process-crash.json
/private/tmp/nereus-delay-kafka-broker-process-crash-20260821-r3/state/after-fresh-process.json
```

The real run passed with Delay `75a008fc`, K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`,
P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The before dump observed leader
`3`, replicas/ISR/live `[1,2,3]` and end offset `1`; after survivor leader
convergence, real Worker source apply/ACK, typed `KAFKA_TRANSACTIONAL_RECEIPT`
readback and Broker-1 restart, the after dump observed ISR/live `[1,2,3]`,
end offset `5`, `broker_1_rejoined=true` and distinct JVM PIDs `51328 -> 51612`.
The independent audit checks topic/cluster/topic identity, replica and ISR
membership, Broker-1 rejoin, offset advancement, forced durable reads and
process identity. It deliberately does not require Broker-1 to have been the
topic leader: the real assignment selected Broker-3, so the evidence records
the actual leader rather than imposing an unobserved ownership claim.

This closes the durable/fresh-process/invariant boundary for the
`kafka-broker-process-crash` cell only. The current 14-cell wrapper and V1
release gate must be regenerated after this commit; historical receipts whose
Delay source lock predates `75a008fc` remain historical. Keep only canonical
dumps and locked Oxia/MinIO base images after the run; exact run-scoped
Kafka/Oxia containers, networks, volumes and generated images must be absent.
Never delete a source worktree, `.git` directory or code path.

## 2026-08-21 Typed durable chaos state evidence contract

Delay commit `33b546f6` fixes the durable-state evidence writer used by the
checkpoint REAPING and direct Pulsar destination response-loss cells. Boolean
fields are now emitted as JSON booleans rather than quoted strings, while the
fresh-process reader accepts the scalar representation used by the state
schema. This is an evidence-schema correction; it does not broaden the
runtime recovery claim.

The focused current-source checks passed with Delay `33b546f6`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`:

```text
/private/tmp/nereus-delay-checkpoint-reaping-20260821-r13-state/before-process-crash.json
/private/tmp/nereus-delay-checkpoint-reaping-20260821-r13-state/after-fresh-process.json
/private/tmp/nereus-delay-pulsar-destination-response-loss-20260821-r13-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-destination-response-loss-20260821-r13-state/after-fresh-process.json
```

Both runs used separate WRITE/READ JVMs against real Oxia plus real MinIO or
Pulsar. The checkpoint audit verified the REAPING transition, exact version
counts and empty prefix; the direct Pulsar audit verified typed
`PULSAR_SEND_ACK`, exact payload readback, `physical_send_count=1` and zero
duplicates. Focused cleanup left no scoped containers, networks or volumes.
Rerun the complete current-source chaos wrapper after this documentation
commit; neither focused receipt is a V1 release certificate.

## 2026-08-21 Current-source certified chaos r13 and release gate r10

After Delay `4be08ee917e045b1466046aa69318645ac689ea5`, the strict-sequential
14-cell child matrix completed with every child exit code `0` and
`matrix_status=PASS_BOUNDED`. Nine cells now have independently verified
durable before/after dumps, fresh-process recovery and invariant audits:
Kafka Broker crash, Kafka Worker ACK crash, Pulsar Worker crash, Pulsar Worker
admission response loss, Pulsar Worker destination response loss, checkpoint
REAPING, Kafka Fetch response loss, Kafka retention floor and direct Pulsar
destination response loss. The remaining five cells are explicitly
marker-only or not covered: Kafka TCP cut, Kafka network partition, Pulsar
multi-Broker crash, Pulsar source ACK response loss and Gateway/Oxia session
churn.

The canonical receipts are:

```text
/private/tmp/nereus-delay-v1-certified-chaos-20260821-r13/bounded-chaos/bounded-chaos-matrix.json
/private/tmp/nereus-delay-v1-certified-chaos-20260821-r13/certified-chaos-matrix.json
/private/tmp/nereus-delay-v1-rc1-release-gate-20260821-r10/v1-release-candidate-gate.json
```

The r13 source locks are Delay `4be08ee917e045b1466046aa69318645ac689ea5`,
K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Certified status remains
`BLOCKED`; Docker postcheck is `PASS` with no scoped containers, networks,
volumes or generated images. Gate r10 is intentionally
`release_status=NOT_READY`: source checks, cross-repo validation and full
Gradle check passed, while the supplied certified capacity/soak/activation/
operations artifacts have older Delay source locks and the chaos artifact is
`BLOCKED`. `ALLOW_NOT_READY=1` only preserves this fail-closed audit.

## 2026-08-21 Kafka TCP cut and network-partition durable evidence

Delay commit `ae10068e` generalizes the Kafka Broker state smoke and bounded
audit to the real TCP-cut and Compose-network-partition cells. Each cell now
captures a forced durable Admin dump before the fault and from a separate JVM
after recovery, and the shell audit compares the schema, cell/phase, topic /
cluster / topic identity, replica/ISR/live membership, offset advancement,
Broker-1 recovery observation and process identity.

The focused current-source receipts are:

```text
/private/tmp/nereus-delay-kafka-broker-tcp-cut-20260821-r1-state/before-process-crash.json
/private/tmp/nereus-delay-kafka-broker-tcp-cut-20260821-r1-state/after-fresh-process.json
/private/tmp/nereus-delay-kafka-broker-network-partition-20260821-r1-state/before-process-crash.json
/private/tmp/nereus-delay-kafka-broker-network-partition-20260821-r1-state/after-fresh-process.json
```

Both real three-Broker runs passed the full Worker/Oxia recovery chain. TCP
cut recorded end offset `1 -> 2`, and network partition recorded `1 -> 2`;
both preserved replicas/ISR/live `[1,2,3]`, with distinct before/after JVM
PIDs and forced durable reads. The raw proxy rejected the Broker-1 endpoint
once, while the network case removed the live Broker-1 container from the
exact Compose network and later reconnected it. The focused Docker postchecks
left no scoped containers or networks and retained only locked base images.

This advances the independently audited bounded union to 11 of 14 cells. The
full current-source chaos wrapper and certified release gate still need a
fresh rerun after the remaining Pulsar multi-Broker, Pulsar source-ACK and
Gateway/Oxia session-churn slices; this focused receipt is not release
certification.

## 2026-08-21 Pulsar multi-Broker process-crash durable failover evidence

Delay commit `a48cd33a00ecd566d149ddb300efa22cf670747a` adds an independent
durable-state collector and audit for `pulsar-multi-broker-process-crash`.
The focused r4 run used two real Pulsar Brokers with ZooKeeper and
BookKeeper, real Oxia authority, and the real Worker chain. After guarded
preparation, Broker-1 was SIGKILLed; Broker-2 supplied the survivor Admin read
before a fresh Worker resumed source apply, typed destination publish and
ACK/checkpoint work, and Broker-1 was then restarted and observed rejoined.

Focused receipts:

```text
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r4-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r4-state/after-process-crash.json
```

The independent jq audit passed the common durable-state schema, identical
topic/physical-topic/cluster and ledger identity, non-decreasing entry and
confirmed position, distinct survivor Admin endpoints
(`31741 -> 31743`), distinct collector JVM PIDs (`12676 -> 12738`),
`durable_broker_read=true` and `dump_forced=true`. Both state dumps read
`internalStats?metadata=true` and reported ledger IDs `[-1,2]`, one entry,
confirmed position `2:0` and `LedgerOpened`. The source locks were Delay
`a48cd33a00ecd566d149ddb300efa22cf670747a`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The Admin state is intentionally captured after survivor readiness and before
the fresh Worker closes the source consumer: in this setup, querying
`internalStats`/`internal-info` after that consumer close can return 404/500.
This receipt therefore proves the survivor Broker durable read and subsequent
Worker recovery chain without claiming a post-Worker Admin query. The
independently audited bounded union is now 12 of 14. Pulsar source-ACK
response loss and Gateway/Oxia session churn remain open; the full wrapper and
V1 release gate must be regenerated at the new source lock.

## 2026-08-21 Pulsar source-ACK response-loss and Gateway/Oxia churn evidence

Delay commit `63b72ee9944995a88b0cfe4505ede2051e4392f` closes the two remaining
focused durable-evidence slices. The Pulsar source-ACK cell now forces the
RocksDB Store/WAL boundary before cutting the local ACK response, SIGKILLs the
Worker JVM, and lets a fresh Worker accept the persisted source position when
there is no old record to replay. The Gateway cell writes forced before/after
durable-record dumps around a real Oxia process restart and independently
checks stale-session fail-closed behavior and exact outcome reuse.

Focused receipts:

```text
/private/tmp/nereus-delay-pulsar-source-ack-response-loss-20260821-r3-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-source-ack-response-loss-20260821-r3-state/after-fresh-process.json
/private/tmp/nereus-delay-gateway-oxia-session-churn-20260821-r1-state/before-oxia-restart.json
/private/tmp/nereus-delay-gateway-oxia-session-churn-20260821-r1-state/after-oxia-restart.json
```

The real Pulsar/Oxia source-ACK run preserved the same topic, Route/Store
identity, `store_incarnation=87e9c9ecfb3a42499970b75927cfb661` and
`db_identity=77fa231eacc87edc595e5ef82567bbf7b579ff7cc32adf325bf2a4e6bed2405e`.
The before dump recorded `source_ack_source_position == applied_source_position`,
`source_apply_durable=true`, `source_ack_committed=true` and an intentionally
lost ACK response. Fresh JVM PIDs were `31393 -> 31470`; the after dump kept the
ACK position, reported `recovery_replayed_entries=0`,
`recovery_replayed_ack_source=false` and `duplicate_source_apply_observed=false`,
then completed the active vertical path and final checkpoint.

The Gateway/Oxia receipt preserved one admission record, one QUIESCENT
idempotency record with one attempt and one aggregate outcome, two audit
records, zero active leases, one prepare call and one submit call. The before
and after response SHA-256 was
`ebac652cf85c75330d79e523b1cd46d52116a0611c7d3128448749cd78cec2c3`; the
stale old sessions failed closed while the new sessions reread that exact
outcome after Oxia restarted. This is an Oxia-process/session-churn cell, not
full Gateway HA or provider failover.

These two receipts bring the independently audited focused durable union to
14/14. The complete current-source matrix and certified release gate still
must be regenerated at this source lock; V1 remains fail-closed until the
approved capacity, soak, activation/cutover, operations and chaos inputs pass.
Focused cleanup moved the unused Pulsar r2 diagnostic directory to recoverable
Trash, removed run-scoped Docker resources, retained only locked base images,
and did not touch source worktrees or the pre-existing unlabelled
`pulsarconf`/`pulsardata` volumes.

## Current-source 14-cell bounded chaos r15

After Delay `d14d9a6a7e55d77bd1a3a42ea3f2e30291896b61` corrected the Kafka
process-crash marker audit, the strict-sequential wrapper was regenerated.
The canonical receipt is:

```text
/private/tmp/nereus-delay-chaos-current-20260821-r15/bounded-chaos-matrix.json
```

It reports `matrix_status=PASS_BOUNDED`; all fourteen child processes return
zero, and every cell has marker PASS,
`CAPTURED_AND_VERIFIED` durable state, fresh-process recovery PASS and
`INDEPENDENT_FIELDS_PASS` invariant audit. The source locks are Delay
`d14d9a6a7e55d77bd1a3a42ea3f2e30291896b61`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

This is the canonical bounded current-source fault receipt, not V1 release
certification. The certified chaos wrapper and release gate must be
regenerated from the post-documentation source; capacity, soak,
activation/cutover and operations remain separate release inputs.

## 2026-08-21 Current-source bounded chaos r17 and Kafka TCP-cut rerun

The complete strict-sequential matrix was rerun after Delay documentation
commit `257161a203090fdf5657acdea896d6b8b5777040`. The canonical artifact is:

```text
/private/tmp/nereus-delay-chaos-current-20260821-r17/bounded-chaos-matrix.json
```

It reports `matrix_status=PASS_BOUNDED`; all fourteen child processes returned
zero, and every cell independently passed
`audit_status=PASS`,
`durable_state_dump=CAPTURED_AND_VERIFIED`,
`fresh_process_recovery=PASS` and
`invariant_audit=INDEPENDENT_FIELDS_PASS`. The four source locks are Delay
`257161a203090fdf5657acdea896d6b8b5777040`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The previous r16 run is retained as a diagnostic because its Kafka
`broker-tcp-cut` producer hit a transient `TimeoutException` and had no after
dump. The same current source was independently rerun at:

```text
/private/tmp/nereus-delay-kafka-broker-tcp-cut-20260821-r2-state/
```

The focused r2 receipt passed fresh-process recovery, changed PID, same
topic/cluster/topic ID, monotonic end offset, Broker-1 recovery and
`INDEPENDENT_FIELDS_PASS`. r17 is the canonical bounded receipt, not a
certified-chaos or V1 release PASS; run those gates against the final source
lock.

## Current-source protocol-golden PASS_CERTIFIED

The current exact-source protocol gate completed independently at:

```text
/private/tmp/nereus-delay-v1-protocol-golden-run-20260821-f.1N9Xji/protocol-golden.json
sha256=e144407304580231c879ff3ed9f4c84951f85f537bcda2f06a9f101b1f375365
```

Candidate locks were Delay `dc37d2c2093eb46d3bf85f2bd964d5055a086194`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The runner executed 392 Delay
tests, 17 Kafka guarded tests and 8 Pulsar guarded tests. All test exits were
zero with zero failures, errors and skips, so the artifact is
`PASS_CERTIFIED` for Gate 1 (`protocol-golden`). It does not satisfy the other
nine full-V1 gates and does not change the historical status of r19/r20.

## Current-source no-early PASS_CERTIFIED

The no-early runner produced the exact-source receipt:

```text
/private/tmp/nereus-delay-v1-no-early-20260821-a.bOg67w/no-early.json
sha256=91692a7301b5e4fc99605ef6698c0c9208a12ea1379f7123d9db928ae7138d37
```

Delay `f82e914d22c5b7d84f618e0ca31fa378a27bf3a2` plus the fixed Kafka/Pulsar/Oxia
locks passed 34 focused tests with zero failures, errors or skips. The receipt
is `PASS_CERTIFIED` for no-early, records `max_early_ms=0`, and exposes the
20 ms worker and target clock bounds. It does not close the remaining eight
full-V1 gate inputs or the final release gate.
