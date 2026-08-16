# Nereus Delay V1 Design Audit

状态：PASS / design semantics closed  
Spec revision：`V1-FROZEN-2026-08-13`
审计日期：2026-08-15
性质：验收证据索引；不覆盖主设计、Protocol Registry 或 Accepted ADR

## 结论

V1 的业务语义、线性化点、fencing 范围、物理持久边界、故障隔离、恢复/GC 保护关系、公开错误模型和发布停止条件已经闭合。审计未留下需要实现自行选择的语义分支。

**Open semantic questions: none.**

The 2026-08-13 revision accepts ADR 0043/0044 and the code-level
[`Direct SDK / Delay Gateway / Guarded Transport design`](V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md).
It closes the previously incomplete entry-shape choice as two optional
production compositions over one Semantic Core, and replaces Pulsar's
plugin-string Producer proof with a first-class typed v22 create/per-SEND
guard. Protocol Registry §6.5 freezes the Gateway wrappers and crash-safe
single-record idempotency projection; §6.6 freezes the signed Ingress Route
snapshot bytes. Existing self-routing IDs, tenant
authority, NDL1/NDR1, Worker state, Source Position and RocksDB keys do not
change.

This is design closure, not release approval. The Delay repository now has
local implementation evidence for the Semantic Core/value seam, explicit
Direct SDK composition, the shared submission coordinator, an in-memory
Gateway Schedule/idempotency conformance path, generated Java/gRPC Gateway API
descriptors, Schedule/RetryUncertain/PrepareLargeSchedule/CommitLargeSchedule/
Cancel/Reschedule handlers, receipt-bound upload handlers behind a
digest-audited ingress, and query/await/message handlers behind an explicitly
injected query authority,
and a transport-neutral Worker source-consumer handoff that retains one exact
record until ACK-after-sync. The current branch also has an explicit
source-locked Kafka source poll/ACK handoff smoke, source-locked K1/K2 Kafka
and P1 Pulsar client bindings, plus three-broker K1/K2 Docker smokes. It still
lacks a Route activation-barrier/session-reconnect-complete real Oxia
service gate, issuer/catalog native eligibility authority, certificate deployment/rotation and live Gateway authority,
the full K2 receipt-read/response-loss gate, D3/source verticals, or a production Worker
vertical. Those rows remain open release blockers even though the design status
is Accepted.

Commit `17b4d7e6` adds the source-bound Kafka guarded Fetch cut. It creates a
`GuardedConsumer` only through an immutable cluster/topic/TopicId/partition
guard, requires Fetch v13+ and carries the broker response proof through the
Classic and Async consumer paths. The proof includes correlation/broker/session
identity, fetch and returned offset range, high watermark, last stable offset
and a SHA-256 response-body digest. Delay source and recovery adapters reject
stock consumers, validate every record against that proof, and keep the active
record in flight until `commitSync` succeeds; recovery never commits. The
focused Kafka tests, Delay source compile/checkstyle and full local `check`
passed. The three-broker run used Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
broker image
`sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`.
This is opt-in source Fetch/ACK evidence, not Fetch response-loss,
LSO/retention-floor, assignment/session, ACK-failure, Store-apply or Worker
production evidence.

Commit `72d4accf` closes the local native Pulsar recovery-positioning gap
without promoting the Worker vertical. `PulsarClientArtifactRecoverySourcePositioner`
validates the guarded consumer before seek, maps a durable position to the
physical-topic native MessageId, waits for the seek-triggered SUBSCRIBE
generation to stabilize, and makes the post-seek proof the only source of the
new activation barrier. P1 commit `358ce4a103` returns the permit consumed by a
non-batch seek-filter path, so the guarded queue-size-one source can advance
past the filtered target. The focused P1 client test and distribution build
passed; the rebuilt client, common and distribution SHA-256 values are
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8` and
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`.
The latest Docker run used P1 image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
project `nereus-delay-pulsar-e2e-1786753971-20261` and ports `19811,19812`;
it asserted `skipped=11/0`, returned the second command, reported source
generations `5` and `6`, and ended with `Pulsar P1 real-client E2E passed`.
Delay source compilation and full `check` passed as well. This closes only
native cursor positioning and the P1 small-queue permit boundary. Durable
Store cursor selection, Owner assignment/Lease CAS, RocksDB apply, activation
and the full Worker catch-up/ACK/recovery vertical remain OPEN.

Commit `5c53f866` adds the source-set
`KafkaClientArtifactWorkerSourceFactory` and
`PulsarClientArtifactWorkerSourceFactory` composition boundaries. They reject
an assignment that is not the Owner's exact accepted identity or an Owner that
is not `ACTIVE_FOR_COMMANDS`; Kafka seeks to the projected exclusive barrier,
while Pulsar requires the current guarded SUBSCRIBE proof to match the exact
resource, topic, partition, generation and attestation digest in the barrier.
Both factories close the native source if common Worker runtime construction
fails. The locked K1 and P1 source sets compile successfully. This is a
post-activation composition seam only: assignment publication, Owner Lease
CAS, source catch-up, recovery, production RocksDB scheduling and broker
ACK/rewind E2E remain open.

Commit `bbbc3160a6674b04a90b48a1f00865c079313bc7` adds the native no-ACK
replay inputs `KafkaClientArtifactRecoverySourceCursor` and
`PulsarClientArtifactRecoverySourceCursor`. Each retains one decoded entry
until the recovery caller advances it after a proven Store apply. Kafka uses
the exact assignment/barrier and a caller-supplied durable start offset;
Pulsar verifies the guarded subscription's resource, attestation digest and
connection generation, while the caller remains responsible for positioning
the subscription at the durable recovery cursor. The source smokes verify
exact first/second replay and look-ahead retention without ACKing recovery
records. The latest locked-artifact Docker reruns passed Kafka source/K1/K2
and Pulsar guarded writer/source checks. Kafka used project
`nereus-delay-kafka-e2e-1786750940-86542` on `19134,19135,19136`; Pulsar used
project `nereus-delay-pulsar-e2e-1786751009-87322` on `19672,19673` and
reported source generations `2` and `3`. This closes only native replay-input
composition; Owner assignment publication, Lease CAS, Store-positioned
Pulsar recovery, RocksDB apply and the complete D6 Worker vertical remain
OPEN.

Commit `412441c47cce4e61d3cc015b95c7d3cffcab2f7f` adds the real Kafka source
handoff cut. Against the locked K1 client artifact, the smoke decodes exact
NDL1/V1 source frames, preserves Kafka Topic UUID/offset/leader-epoch/
LogAppendTime in `KafkaSourcePosition`, replays an unacknowledged record after
same-group consumer restart, and advances the group offset only after
`commitSync` succeeds. The final three-broker run used Compose project
`nereus-delay-kafka-e2e-1786741055-82662` on ports `19254,19255,19256`,
passed source/K1/K2/failover checks and cleaned its Docker resources. This is
not guarded Kafka Fetch, source assignment/session authority, ACK-failure
injection, RocksDB apply, or the D6 production Worker vertical.

The 2026-08-14 Delay worktree milestones `532f8ad5`,
`402b27fa0dced95c2312bfedc0678af03463f2d5`,
`67ef3de3ab6f69ae992c3ccb70c7cb65cad47613` and
`c42405ce6c69aef8ae0f8a9a63158c917410309f`, `62a9438967112f96e65b8daa7b2b86d52a103b10`,
`e276bec3ffff7f5015367bed55f5b8d63c080e21` and
`69d89839e4e80326e5317a4f5066667e270a7136`,
`a06ab232a5608ec0e7c9152ef80fc72c06966e66`,
`1dc28eaf391429f2dc9221f416af968d36575dff`,
`5cc955e1306e1f54db06a06a2bb2b84f232c2a7b`,
`1bee5b45e4df697770f7bca99a572167bb869526`,
`bcf2f0a883cd3090ae96250453dabaa71f3945c5`,
`9695eba7ca384d99cd28ece238f6cbfe1bcd08be`,
`724fdad95971dd096e116056f8e5da1a7ba76d14` and
`44bffea6063ef68ce36f8fb49527ee00a9bfa36b`, and
`59d492041ac42b79a632ebddfb56a7608b2d7283`, and
`4f606fec86aaeb74472f6575e5ee7ddcb8dc8f82` verify the canonical
signed Route
value, exact Kafka/Pulsar resource projections, UUIDv7/independent command
identities, zero-I/O preparation, exact historical-route plan resolution,
one-shot transport ownership, NDR1 projection, local Gateway idempotency
body/attempt behavior, contiguous signed-cache watch fencing, Oxia event/head
CAS Route publication/refresh, explicit uncertain retry CAS and Oxia Gateway
single-record CAS. The full local `check` passes at
`4f606fec86aaeb74472f6575e5ee7ddcb8dc8f82`; the Route provider/publisher
focused tests pass at `62a94389`, and the Gateway CAS focused tests pass at
`e276bec3`; the route-cache/Gateway focused tests pass at their respective
commits. This historical evidence does not establish activation-barrier,
notification-stream or session-timeout/reconnect-complete real-service Oxia
Route behavior, production Gateway authority/authentication,
HA durable idempotency, real Kafka/Pulsar transport artifacts, Worker wiring or
real-Broker correctness; those release rows remain OPEN.

The separately owned Kafka K1 worktree has since produced commit
`95d48e89e7e8a4e6d8718e44d424ffef8f17829f` from the locked
`trunk@c300006a7705c240642db6950b5a95fec982bfc5`. Its focused client/mock
evidence covers the generic guarded producer API, exact TopicId/v13 request,
batch isolation, leader retry, response evidence hashes and the
`UNKNOWN_TOPIC_ID`/ambiguity boundary. Its real KRaft integration test also
passes same-name delete/recreate rejection and leader-failover success, and
locks Kafka's legal `CreateTime` `logAppendTimeMs=-1` sentinel. Artifact/source
digest capture, D2 Nereus transport and production release approval remain
OPEN.

The separately owned Pulsar P1 worktree has since produced
`19c97bf836d521f0e6103c542819723e70ccdbab`,
`be226fe6c88634e9a94ba5c6a0f5859bc510cb66` and
`7eebd41d5b0917a0dfe5ea26ef3062a39f70a6d9`, followed by
`f813c96687cc19e6fca1c82d3d161cf3e045c86b` on
`nereus/delay-resource-guard-v1`, based on the locked
`5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551`. The commits add the
v22 guard wire contract, immutable guard/evidence API, strict broker property
view, create/per-SEND checks, guarded SUBSCRIBE, source attestation and
connection-generation correlation. The writer path snapshots the guard before
asynchronous broker topic work and makes create-time incarnation mismatch
typed and non-retriable; the source path validates the current broker view
before allocating a generation.
The current real in-process broker test passes guarded SEND evidence and
same-name delete/recreate (old incarnation rejected before persistence,
replacement accepted); the affected-module checkstyle also passes with
`GRADLE_USER_HOME=/tmp/nereus-pulsar-delay-gradle`.
This is isolated upstream module evidence plus a Delay-side single-node
real-client Docker cut: unload, multi-broker failover, old-peer proxy cuts,
source session/rewind, complete artifact attestation and D3 Nereus transport
evidence remain OPEN, so this audit does not promote P1 or V1 production
status.

The post-permit live-service audit on 2026-08-12 ran from document commit
`b45045b` with a temporary standalone Oxia service built from source commit
`37a17bef17202d5fd6e23282da5fd26d94865484`:
```text
NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:6648 \
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check --rerun-tasks --console=plain
```
The command completed with 1205 tests, zero failures,
errors or skips. All five opt-in real-Oxia methods executed successfully.
`checkDocumentation` was the first verification task in the same live run.
That live-service evidence is pinned to `b45045b`. Subsequent local commits
`3a4914f`, `07751ef` and `227b91b` add only the Worker event-loop/resource
composition seam, its package-local admission hook and the auto-closing local
bounded-turn executor. A full `clean check --rerun-tasks` after `227b91b`
also passed with 1205 tests and zero failures/errors; the five opt-in
real-Oxia methods were skipped because the endpoint was unset. External
cross-record Oxia transaction/session, Broker transport,
provider authority and release-scale evidence gates remain open.

The later local Worker source-consumer slice is pinned to
`nereus/delay-full-implementation-v1@1bee5b45`. `SourceRecordConsumer` and
`WorkerSourceApplyLoop` now provide one-record poll retention, explicit idle
poll handling, identity-bound ACK and ACK-after-Store ordering; the full local
`check` passed with the five real-Oxia smoke methods skipped because no endpoint
was configured. This is local composition evidence only: real Kafka/Pulsar
Fetch/ACK/commit/rewind, source session/ownership authority, dynamic
WriteBatch/IO admission, due/publish/checkpoint/recovery wiring, Docker cuts
and real-Broker correctness remain OPEN.

Commit `decb965e3991264ac243eb68c62ba0827759e616` adds the local
`WorkerShardRuntime` composition over that source loop and
`OwnerDrainCoordinator`. It rejects drain before the Owner Lease transition
when an exact source record still has an unconfirmed ACK, pauses new source
turns once drain starts, preserves checkpoint retry state, and closes the
source only after Store close and exact lease release. The full local `check`
passed at this commit. This closes a local lifecycle ordering seam only; it
does not establish native Kafka/Pulsar consumers, Oxia placement/session
authority, due/publish/checkpoint/recovery scheduling, or real Broker ACK and
rewind evidence.

Commit `3d0bf7ea081ae7b652e3a0ca4b66003bc4b23618` adds an isolated Docker
Compose harness for the Oxia real-service smokes. On 2026-08-15,
`./e2e/run-oxia-real-service.sh` built Oxia source
`37a17bef17202d5fd6e23282da5fd26d94865484`, started unique Compose project
`nereus-delay-v1-oxia-e2e-1786729940-65321` on host endpoint
`127.0.0.1:16649`, and passed the Owner, Control, Recovery and Gateway audit
real-service test classes with Gradle `BUILD SUCCESSFUL`. The exit trap
removed the matching container and network. This is isolated Dockerized Oxia
authority/audit evidence; it does not promote Route activation/session
reconnect, real Kafka/Pulsar transport, Worker vertical integration, HA or
release status.

Commits `becfb1a35fc05cbf7ae7c77816f91bd72e546566` and
`3d45dcd7bc457d0ab308b51b9dee4abf5de6adf4` add the real Oxia Route
publication/refresh smoke, move notification refresh work off the Oxia
callback thread, and isolate the notification stream on a separate client
from the session-fenced Route client. On 2026-08-15, the updated
`./e2e/run-oxia-real-service.sh` built source
`37a17bef17202d5fd6e23282da5fd26d94865484` in Compose project
`nereus-delay-v1-oxia-e2e-1786732310-90387` at `127.0.0.1:16649`; eight
selected real-service methods passed with `BUILD SUCCESSFUL`, including
signed Route event/head revisions, separate publisher/provider sessions,
explicit session-fenced refresh and notification-driven refresh. The matching
container and network were removed by the exit trap. Session timeout/reconnect,
cache-staleness recovery, activation barrier, native eligibility and release
status remain open.

Follow-up commit `a71a0667` adds explicit `reconnectSession()` to the Route
record surface. A fenced publisher operation remains fail-closed; the
provider's explicit `refresh()` or a caller's reconnect request rotates the
ephemeral marker, derives the new session identity and then rereads the
authority before cache I/O. The focused Route tests cover marker expiry,
identity rotation, restored reads and provider refresh. This is deterministic
local reconnect evidence, not a live Oxia timeout/connection-loss cut,
activation-barrier publication, native eligibility or production deployment.

Commit `9a805f2ef879ce7e9c78168d4fff31a973f7c186` adds the deployable Gateway
gRPC composition: `GatewayGrpcContext` binds transport-owned peer metadata and
attributes per call, `GatewayGrpcServer.mutualTls` configures shaded Netty
with mandatory client certificates, and
`MutualTlsJwtGatewayTenantAuthority` requires both the peer certificate and a
Bearer token before invoking an explicit `GatewayJwtVerifier`. Delay commit
`19099e2e` adds the new
`RsaSha256GatewayJwtVerifier` implements the concrete RS256 policy with exact
issuer/audience/key-id and NumericDate checks, duplicate-free strict JSON,
signed tenant/routing scope digests and mTLS `cnf.x5t#S256` certificate
binding. `GatewaySecurityCompositionTest`,
`RsaSha256GatewayJwtVerifierTest`, the Gateway API test and main Checkstyle
passed. This is verifier-policy evidence, not certificate issuance/rotation
deployment, distributed quota/control reserve, Gateway HA crash cuts or
release readiness.

Commit `4a4cb9424ec731a59bb117028ae631557c907e2f` adds the read-only
`e2e/validate-cross-repo-contracts.sh` audit. It passed on 2026-08-15 and
verified clean isolated worktrees, the Kafka/Pulsar source-lock ancestry and
exact implementation heads, Oxia source `37a17bef17202d5fd6e23282da5fd26d94865484`,
and the corresponding Delay identity, Kafka `sendGuarded`/TopicId and Pulsar
v22 resource-guard/result symbols. The shared roots retain their unrelated
pre-existing edits and are intentionally outside this clean-worktree check.
This is static contract/source-lock evidence only; it does not promote the
open real Broker, Route activation/session, Worker vertical, HA or release
gates.

Commit `3f76e836964d818360d5affc122515ccbac04717` adds explicit `realKafka`
and `realPulsar` source sets to the Delay worktree. The K1 binding constructs
`ProducerResourceGuard`, invokes only `GuardedProducer.sendGuarded`, and maps
verified cluster/topic/TopicId/partition/time/evidence into the existing
Kafka result union; a missing broker timestamp remains `UNKNOWN`. The P1
binding creates a BYTES `TopicResourceGuard` producer, requires an exact
`GuardedMessageId`, and maps typed v22 rejection evidence without storing
payload or credentials. Both compile paths require explicit upstream artifact
paths, so a stock or name-only client cannot silently enter the normal build.

The source-bound API checks passed. The P1 smoke returned
`PERSISTED`, `UNKNOWN` for an incarnation mismatch and
`DEFINITIVELY_NOT_PERSISTED` for typed error evidence. The Kafka Docker smoke
ran from K1 `95d48e89e7e8a4e6d8718e44d424ffef8f17829f` and used client SHA-256
`722b09de1a6d79eba867ceda4baac085af0a59897f9e003b58f167ac13e35c24`; it
started Compose project `nereus-delay-kafka-e2e-1786735980-29312` on
`19404,19405,19406`, proved three-replica produce, delete/recreate old-TopicId
rejection, replacement acceptance and broker-1 failover through brokers 2/3,
then removed its container, network and temporary image. The broker image was
built from base
`eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`
and local image ID
`sha256:8ef999e7f4151005ceaf570bb15989932628e040037e4e8056213ca4270f4b0b`.

Delay commit `62ea85e8` adds and passes the real P1 client service Docker cut
from P1 `7eebd41d5b0917a0dfe5ea26ef3062a39f70a6d9`. The temporary image uses
distribution SHA-256
`d4b9e8aa6b44582c383262007217980793ec41bdf7fa3a1a4285e220407fef32`, image
ID `sha256:f377aeddd73913830a1004287e14eae910e739f39793a96fe41d38f2e5aca264`,
and Compose project `nereus-delay-pulsar-e2e-1786737555-46201` on
`19651,19652`. With broker timestamp/index metadata enabled and exposed, the
smoke returned `initial=PERSISTED`,
`stale=DEFINITIVELY_NOT_PERSISTED`, `replacement=PERSISTED`; its exit cleanup
left no matching container, network, volume or temporary image. This closes
only single-node P1 client/broker delete-recreate evidence. Unload,
multi-broker failover, old-peer proxy compatibility, connected stale SEND
without typed response evidence, D3 source/ACK and Worker production wiring
remain OPEN.

This is opt-in writer/client-binding evidence, not release promotion. Kafka
K2 target-plus-receipt transactions, Pulsar D3 unload/multi-broker/reconnect,
guarded source Fetch/ACK/rewind, deployed Gateway authority, production
Worker wiring and the remaining §23.5 artifact/chaos/SLO gates remain OPEN.

Delay commit `ca9134ec8a1922e68f76f69ae0aa9bdd6e7180d5` now adds the opt-in K2
target-plus-receipt binding. The isolated Kafka branch is
`nereus/delay-guarded-producer-v1@8bd66fbb26eae1b0e4c5867e61f41900c3f5e318`;
its generic `GuardedTransactionalProducer` requires an active transaction,
transaction-v2 capability and partition registration before the guarded send.
The source-locked client jar used by Delay has SHA-256
`4b6362d10146568c7ef78629ad678e50f164a750fdbb362ba0899dc49b815656`.

`LC_ALL=C LANG=C ./e2e/run-kafka-real-client-e2e.sh` passed on 2026-08-15
against Compose project `nereus-delay-kafka-e2e-1786739311-64581` on
`19173,19174,19175`, using broker image
`sha256:3116a80efc9d4a9399ca225c1de4288abde253659fd6fad2292af7727a2e9505`.
The K2 smoke proved atomic target-plus-receipt commit, abort, exact target
payload and canonical receipt key/value reads, stale target TopicId rejection
after same-name delete/recreate, and replacement commit. Commit
`7b64de0f9648815df55893d3dad3673093deabb7` adds the exact-record assertion;
its Docker cleanup found no matching resources. This is partial K2 evidence:
EndTxn response-loss, Fetch v13/LSO/contiguous replay, retention-floor recovery
and independent target/receipt failover remain open,
so the atomic-target-receipt profile is not activated. D2 source/ACK, D3,
Worker production wiring and release gates remain OPEN.

The clean cross-repository audit at the current evidence state passed with
Delay `d93143f1b4eb2546ed326b2c3a5ef683352ec1fe`, Kafka
`8bd66fbb26eae1b0e4c5867e61f41900c3f5e318`, Pulsar
`f813c96687cc19e6fca1c82d3d161cf3e045c86b` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`; both upstream branches were
verified descended from their locked bases and all four audited worktrees were
clean.

Commit `bcf2f0a883cd3090ae96250453dabaa71f3945c5` also closes the local Direct
SDK outbox-Final ambiguity branch: a completion-evidence write failure keeps
the exact prepared branch and physical attempt and returns `ENQUEUE_UNCERTAIN`.
`DefaultDelayClientTest` covers this projection; outbox restart durability and
Broker outcome evidence remain OPEN.

Commit `9695eba7ca384d99cd28ece238f6cbfe1bcd08be` extends the local Gateway
conformance path with Cancel and Reschedule. It proves canonical
`MessagePreconditionV1`/self-routing-ID decode, Semantic Core control
preparation, the same tenant/admission/audit ingress, idempotency CAS and
one-shot physical-attempt/NDR1 projection used by Schedule. The full local
`check` passed at the commit; five real-Oxia smoke methods were skipped because
no endpoint was configured. This remains generated-service evidence only;
remaining RPCs, deployable authentication, distributed quota/audit, HA
durability, real Broker transports and production Worker wiring remain OPEN.

Commit `724fdad95971dd096e116056f8e5da1a7ba76d14` extends the same local
Gateway evidence to PrepareLargeSchedule and CommitLargeSchedule. It covers
canonical route/profile/trust-set references, reservation receipt and signed
payload proof decoding, Semantic Core large-payload preparation, idempotency
CAS and one-shot attempt/NDR1 projection. The full local `check` passed at the
commit; five real-Oxia smoke methods were skipped because no endpoint was
configured. Upload/attestation, query/await/message RPCs, deployable auth,
distributed quota/audit, HA durability and real transport/Worker evidence
remain OPEN.

Commit `44bffea6063ef68ce36f8fb49527ee00a9bfa36b` adds the local
receipt-bound upload-handle/attestation ingress. Generated gRPC handlers
strictly decode canonical receipt and opaque-handle bytes, propagate the
authenticated tenant to an explicitly injected `GatewayPayloadAuthority`,
and retain control admission plus digest-only audit. The full local `check`
passed at the commit; five real-Oxia smoke methods were skipped because no
endpoint was configured. This is authority/ingress composition only: actual
Object Store credential and reservation registration authority, remote
immutability, proof-key custody and payload durability remain OPEN.

Commit `59d492041ac42b79a632ebddfb56a7608b2d7283` adds the local Gateway
query composition. It strictly decodes the frozen receipt/CommandId/
DelayMessageId locators, propagates the authenticated tenant through
`GatewayQueryIngressService`, records digest-only admission/audit events, and
bounds `AwaitApplied` canonical response streaming. The full local `check`
passed at this commit; five real-Oxia smoke methods were skipped because no
endpoint was configured. The generated handlers dispatch only with an explicit
`GatewayQueryAuthority`; receipt-to-source/store binding, retention/deadline
policy, production query routing, deployable authentication, HA durability,
real transports and Worker wiring remain OPEN.

Delay worktree commit `39744ac70cae21a3ad4a5401da33805d9221dec7` adds
`OxiaGatewayAuditSink`, an append-only Oxia
composition for the frozen digest-only `GatewayAuditEventV1`. The
event-content-derived key and `IfRecordDoesNotExist` write make exact repeats
idempotent; a response-loss path succeeds only after an exact
key/version/value reread, and same-key byte drift is rejected.
`OxiaGatewayAuditSinkTest` covers these three boundaries, and its focused test
plus main Checkstyle commands pass. This is durable audit storage evidence
only; it does not establish mTLS/JWT authentication, distributed quota,
Gateway HA/transactional idempotency or release readiness.

The follow-up commit `8e0ed49b706dda2a6cb0d7d011c72d2a9270157b` adds
`OxiaRealGatewayAuditSinkSmokeTest`. A standalone Oxia service built from
source commit `37a17bef17202d5fd6e23282da5fd26d94865484` served
`127.0.0.1:16648` on 2026-08-15; the Owner Lease, Control, Recovery and
Gateway audit real-service smoke classes all passed. The Gateway smoke wrote
the same event twice and range-scanned exactly one record with the original
canonical bytes. This is live single-record Oxia evidence, not Route
activation/session-reconnect, cross-record transaction, deployed-auth,
real-Broker or production-Worker evidence.

Commit `4f606fec86aaeb74472f6575e5ee7ddcb8dc8f82` adds the local Route
session-fenced authority composition. `OxiaRouteAuthoritySession` creates an
ephemeral marker, derives session identity from Oxia version metadata, and
rereads exact marker/value/version/session metadata before Route event/head or
cache I/O. Its response-loss path succeeds only after exact reread, while
marker expiry or identity drift fences both publisher and provider. The full
local `check` passed at this commit; five real-Oxia smoke methods were skipped
because no endpoint was configured. Real session-timeout/reconnect and
cache-staleness cuts, activation-barrier publication, native eligibility,
cross-record transactions and production transport/Worker wiring remain OPEN.

The Oxia transaction gate was rechecked against the locked Oxia source and the
resolved `oxia-client:0.9.0` API.  Public client methods are single-record
`put`/CAS calls.  The internal `WriteBatch`/`WriteRequest` path is a
per-Oxia-shard request batch, not a public cross-record transaction; server
`ProcessWrite` atomically commits only the records that already belong to
that one shard.  Therefore it cannot safely bind separately keyed Owner Lease,
Upload Intent, Catalog and Recovery Pin records.  The production adapters stay
fail-closed rather than treating concurrent puts, internal reflection or
coincidental shard co-location as V1 transaction evidence.

The Gradle `checkDocumentation` verification task also passed in this run. It
checks the required authority files, the document-map reference to the main
design, and a single frozen spec revision across the main design, Registry,
ADR index, Status and Audit. This is governance evidence only; it does not
turn the remaining external release gates into PASS.

The local event-loop/resource composition gap is now closed without changing
the V1 contract: `WorkClassEventLoop` uses the scheduler's atomic
before-removal hook to acquire one exact record/byte lease per selected task,
restores the queue/fairness snapshot and releases earlier leases when a later
task is rejected, and requires the returned bounded `Turn` to close before a
second poll. Its `runTurn` helper executes the callback sequence outside the
event-loop monitor, checks borrowed-hold validity around each callback and
closes every lease before rethrowing callback or close failures.
Borrowed-hold and monotonic-clock failures are reported after all leases are
released. `WorkClassEventLoopTest` covers the rollback, one-turn fence,
idempotent release, hold-time failure and callback-failure cleanup. This is
local evidence only; dynamic RocksDB attribution, WriteBatch/IO admission and
production Worker wiring remain open release gates.

After `e531eb8`, that cleanup guarantee also covers fatal `Error`, not only
`RuntimeException`. A fatal hold-clock sample or lease close cannot stop later
lease-release attempts or strand the event loop's active Turn; the first
failure is rethrown after the Turn is closed and later cleanup failures remain
suppressed. `WorkClassEventLoopTest.fatalHoldCheckStillReleasesEveryLeaseAndClosesTheTurn`
proves the fatal-clock branch leaves the shared pool with zero active leases
and reopens the next-poll gate. This closes a local resource-leak/fairness path;
it does not establish production WriteBatch/IO admission or Worker authority.

After `e297176`, `Turn.isClosed()` is a volatile lock-free observation, so
`poll()` does not enter the Turn monitor while holding the event-loop monitor.
This removes the inverse ordering with `Turn.close()`, which holds the Turn
monitor while clearing `activeTurn`. The focused concurrent regression blocks
close inside the resource hold check and proves the next poll immediately
rejects the still-open Turn rather than deadlocking; after release, close
finishes with zero active leases. This is local concurrency evidence and does
not replace production Worker or external IO authority.

After `c44368c`, an ordinary handler `RuntimeException` is aggregated rather
than aborting the selected callback sequence. Later tasks already removed for
that bounded Turn still reach their handlers while the resource hold is valid;
all leases then close and the first handler failure is rethrown. Fatal `Error`
or a hold-boundary failure still terminates callback execution and proceeds to
cleanup. The two-task dispatcher regression proves the trailing selected task
is invoked rather than silently lost. This is local event-loop isolation
evidence; it does not provide production handler durability or IO authority.

After `70d65c2`, a fatal handler or borrowed-hold stop returns the exact
never-invoked suffix of the selected Turn to the fronts of its class queues in
original selection order. The handler-started task is not implicitly requeued,
while selected record/byte capacity remains reserved against concurrent offers
until the active Turn closes. The focused dispatcher and event-loop regressions
cover fatal suffix retention, a pre-handler hold failure and capacity
reservation. This is local queue-integrity evidence; durable production task
identity, handler side-effect recovery and Worker IO authority remain open.

After `9663927`, every exact `PUBLISHED` successor observed after acquiring the
Worker checkpoint-upload slot is subjected to the same bounded-resource and
canonical-manifest binding checks as the initial read. A concurrent publisher
therefore suppresses duplicate provider I/O only when lineage, checkpoint,
Object Store Profile, manifest length and SHA-256 all match. The focused
`CheckpointUploadCoordinatorTest.rereadAfterUploadSlotRejectsPublishedResourceThatDoesNotBindTheManifest`
regression covers the mismatched-length race. This is local fail-closed
publication integrity evidence, not proof of production Object Store or Oxia
cross-record transaction authority.

After `5175a68`, the checkpoint execution coordinator has direct evidence for
the combined body/completion failure cut: the body error remains primary, the
completion error is suppressed, and a completion that cannot be proven leaves
the exact scheduler claim in flight. A later valid completion using that same
handle succeeds. This confirms local claim consistency only; the process-local
scheduler is not checkpoint durability or external publication authority.

After `b4ba86e`, the eight-class runtime can no longer be assembled from a
partial or implicit policy set. `WorkClassRuntimeConfig` requires exact class
coverage, positive class-delay/borrowed-hold/shared-pool bounds, nonzero
record/byte minima for the six correctness/progress classes, checked aggregate
fit, and `LEASE_FENCE`-only preemption before constructing scheduler and pool
from one monotonic clock. Its focused tests execute all eight classes and reject
missing, oversubscribed or semantically invalid policies. This closes the local
configuration/construction seam only; benchmark values, shard handlers and
dynamic WriteBatch/IO authority remain release evidence gaps.

After `bb09b2c`, the local handler-composition seam has an inspectable exact
action lifecycle. `WorkClassExecutionRegistry` registers the complete
class/task/byte-charge identity before queue admission, rolls the registration
back when admission rejects it, removes only successful actions, and retains a
started runtime or fatal failure as `FAILED` for an explicit exact retry.
Never-started trailing actions remain `QUEUED` when fatal/hold termination
causes the event loop to restore the same task suffix. Its four focused tests
cover all eight classes, ordinary failure/retry, fatal suffix retention and
admission rollback with identity-drift rejection. This is process-local
evidence and deliberately does not replace restart rediscovery, shard-specific
handlers, durable source authority or dynamic WriteBatch/IO admission.

After `e20409d` and `d595324`, the independent `CHECKPOINT` class is no longer
only a configured queue: `CheckpointWorkClassExecutor` is the concrete local bridge
from an exact `CheckpointScheduler` claim to
`CheckpointExecutionCoordinator`. It validates the claim and pending
intent/Store identity before admission, starts no directory/provider work when
the queue rejects the action, and repeats the fence after the queue wait. The
caller can no longer provide `workClassBytes`: the exact Shard, claim due time,
normalized absolute checkpoint directory, complete canonical pending intent
and upload time form one canonical value identity. Its domain-separated hash
is the task ID and its exact length is the static queue charge; malformed
negative upload time fails before admission. The
direct preflight/physical execution methods are package-private, leaving the
bounded executor as the cross-package production composition entrypoint.
Ordinary attempt failure is returned as a checkpoint-owned outcome because the
execution coordinator already completed/rescheduled or retained that claim;
fatal `Error` is still rethrown into WorkClass cleanup/fencing. The focused
regression proves queue rejection is side-effect free, an ordinary failed
attempt leaves no stale generic action, and the next exact claim executes
through the bounded turn. This closes one concrete class wiring, not the
remaining shard handlers, dynamic checkpoint-file/upload/WriteBatch I/O admission, Owner Lease/session
or external publication authority.

After `6921ad8`, `SOURCE_APPLY` also has a concrete bounded action rather than
only a configured queue. `SourceApplyWorkClassExecutor` binds task identity and
charge to exact canonical position/frame bytes, performs a side-effect-free
strict assignment/barrier/guard preflight, and rereads the execution-time Owner
Lease/session before either active Command or signed System Mutation reaches
the Shard WriteBatch path. Ordinary failure returns a source-owned outcome and
removes the process action instead of competing with the Broker cursor as a
second retry authority; fatal `Error` remains visible to event-loop cleanup.
The direct active apply methods are package-private. The focused regression
also proves queue rejection is mutation-free, both record kinds receive exact
charges/current-position outcomes, and lease expiry fences without retaining a
generic failed action. This is local composition evidence, not real Broker
consumer/ACK, production Oxia session or dynamic WriteBatch/IO authority.

After `d21983b`, `SourceApplyCoordinator` closes the local source handoff
ordering gap around that executor. One exact caller-owned look-ahead record is
retained across queue rejection, apply failure, ACK loss/unknown and definite
non-ACK; the cursor advances only after an external `ACKED` result and an
exact position/frame/guard/generation re-check. A cursor backing-iterator or
advance failure fences the Owner before the error escapes. The focused
`SourceApplyCoordinatorTest` proves ACK-after-apply, unknown-ACK retry without
duplicating the WriteBatch, and queue rejection without consumption. This
does not claim a real Kafka/Pulsar consumer, Broker commit/rewind, pinned
resource/session authority or dynamic production WriteBatch/IO admission;
those release gates remain OPEN.

After `a49b19a`, the independent expiry path has the corresponding local
`EXPIRY` work-class handoff. `ExpiryWorkClassExecutor` accepts one candidate
already validated by the durable `timeline_cf/EXPIRY` discovery boundary,
prepares and signs the exact `EXPIRE_GENERATION_V1` body before queue
admission, and binds task identity/bytes to that exact mutation. It performs
no local expiry apply and never allocates a Source Position; after queue wait
it rereads the strict Owner Lease, shard identity and Trusted-UTC boundary,
then calls only `ShardLogMutationAppender`. Persisted append positions are
checked against the active source barrier, definite non-persistence and
unknown remain distinct, and append/runtime failure fences the local Owner.
`ExpiryWorkClassExecutorTest` covers persisted, definite, unknown, queue
rejection and failure/fencing outcomes, proving the queue boundary is
side-effect free. This closes only the local candidate-to-log composition
seam; production expiry scanner scheduling, Broker append/ACK, Oxia/clock
authority and source-ordered `EXPIRE_GENERATION` apply remain release gates.

After `45b99dd` and `5f1eae3`, the candidate-discovery side is bounded as well.
`ExpiryDiscoveryWorkClassExecutor` binds Shard identity, canonical Trusted-UTC
evidence and the complete record/byte/elapsed scan envelope before `EXPIRY`
admission; queue rejection reads no Oxia, clock or Store state. Execution rereads
the strict Owner Lease and uses a separate monotonic scan clock. One shared
`BoundedReadBudget` charges actual key/value bytes for both the durable EXPIRY
entry and its dependent Message projection, so a candidate cannot be admitted by
count alone. An individually oversized candidate fails closed, while a later
candidate that exhausts remaining budget is left for a later turn. Discovery does
not mutate Message state and each returned candidate still requires the exact
signed append handoff above. Focused discovery/append/Store tests, compilation and
Checkstyle passed; production scanner scheduling, Trusted-Time/Oxia authority,
Broker append/ACK and source replay remain open.

After `8eb97e4`, the independent reservation-expiry path has a bounded
`GC`-class materializer. `ReservationExpiryWorkClassExecutor` binds the exact
reservation id, Message id, expiry and state version before queue admission,
performs no System Mutation or Source Position allocation, and rereads the
strict Owner Lease and execution clock before touching RocksDB. The shard
materializer atomically updates the reservation, `RESERVATION_EXPIRY` index and
reservation quota only when the candidate still names the same `RESERVED`
projection and the source-ordered `TIME_FENCE` watermark covers it; a newer
Commit/Cancel/Lane Close projection is reported as `STALE` or
`ALREADY_TERMINAL`. Queue rejection is mutation-free. The focused
`ReservationExpiryWorkClassExecutorTest` covers success, rejection, stale
candidate and Owner-expiry fencing. This closes only the local scanner-to-store
composition seam; production GC scheduling, Oxia authority, Recovery-Floor and
Object Store deletion evidence remain open.

After `f2c9334`, the scanner side is bounded by the same class as well.
`ReservationExpiryDiscoveryWorkClassExecutor` admits the exact Shard and full
record/byte/elapsed envelope without reading Oxia, clocks or RocksDB. Execution
rereads strict Owner authority and derives its cutoff exclusively from persisted
source-ordered `closedIngressDeadlineThrough`; no wall-clock input can create a
second expiry decision. One shared `BoundedReadBudget` charges actual index and
dependent Reservation key/value bytes plus elapsed time. Oversized individual
candidates fail closed and later candidates that exhaust the remaining envelope
stay durable for a later turn. Discovery is state-neutral and every byte-identical
candidate still uses the materializer above. Focused discovery/materialization,
Message-expiry and full `DelayShardTest` suites plus compilation and Checkstyle
passed; production TIME_FENCE scheduling, Oxia, Recovery-Floor, Object Store and
replay authority remain open.

After `5eedd9d`, Lane-close cursor advancement has the matching strict
`GC`-class bridge. `LaneCloseWorkClassExecutor` binds the canonical cursor and
bounded batch size before queue admission, rereads the Owner Lease before the
turn, and delegates only the existing source-marker materializer. Queue
rejection is side-effect free; a cursor that was advanced or removed while
queued is reported as `STALE` or `NOT_FOUND` and cannot be applied to another
close version. The focused regression covers successful message terminalization,
rejection, stale cursor and Owner-expiry fencing. This is local evidence only;
production close scheduling, admitted-obligation/object cleanup, Oxia authority
and Recovery-Floor protection remain open.

After `6c3e8ad`, Lane-close cursor discovery is also a strict bounded `GC`
action. `LaneCloseDiscoveryWorkClassExecutor` binds Shard identity and the full
record/byte/elapsed scan envelope before admission; rejection reads no Oxia,
clock or Store state. Execution rereads Owner authority and uses a separate
monotonic clock. One shared `BoundedReadBudget` charges actual key/value bytes
for both the SYSTEM cursor and dependent Lane projection, so an oversized cursor
fails closed and a later cursor that exhausts remaining budget stays durable.
Discovery does not advance cursors or mutate Messages; each result still enters
the exact materializer above. Focused Lane-close discovery/materialization and
full `DelayShardTest` suites plus compilation and Checkstyle passed. Production
scheduling, Oxia, admitted-obligation retirement, Object Store quiescence and
Recovery-Floor authority remain open.

After `3ba6fb6`, persistent READY discovery is also behind a concrete
`DUE_SCHEDULER` action. The task identity binds the exact Shard, canonical
trusted-UTC evidence and all scan-budget fields; its charge reserves canonical
request bytes plus the complete configured scan-byte envelope. Submission
preflight is local and mutation-free, while execution rereads the exact Owner
Lease/session and uses the evidence earliest bound as inclusive due-through.
`PersistentLaneScheduler` remains responsible for bounded READY decoding and
atomic rollback of queue, cursor, ring and fairness projections. Queue
rejection therefore cannot consume READY work, and a failed selected action
fences the Owner and remains exact failed process evidence rather than
advancing the durable cursor. Success exposes only newly promoted due heads;
it does not establish Claim, materialization, permit or Publish Admission.
The focused regression proves rejection without scheduler mutation, exact
charge, due-boundary success and execution-time lease expiry. This closes the
local discovery-class bridge only; Claim/Admission handoff, production
trusted-time issuance, dynamic IO attribution and external destination/Oxia
authority remain release gates. The record-count-only legacy timeline
`discoverDue` scan is package-local and cannot serve as a public production
shortcut around these boundaries. Count-only expiry, reservation-expiry and
Lane-close discovery are package-local for the same reason; only strict
budget/clock overloads remain available to the cross-package owner/work-class
composition. The count-only `DelayShard.discoverReady` query is package-local
as well; production READY scanning is the scheduler evidence+budget path behind
the work-class executor.

After `1759405f`, due/Claim composition also binds the physical Store identity.
The shared `OwnedDelayShard.requireDueSchedulerSubmission` preflight compares
the scheduler and owned runtime's exact ShardId, complete Owner identity and
byte-equal Store Incarnation. A same-Shard/same-Owner scheduler backed by a
different DB incarnation is rejected before action registration and before any
scheduler/Store read or write; Claim handoff inherits the same fence. The
foreign-Store regression preserves its RocksDB sequence and leaves the action
registry empty. Focused due/Claim tests and the complete six-task local gate
passed on 2026-08-13; five real-Oxia smokes were skipped. Production scheduler
assembly, Oxia/trusted-time authority and dynamic I/O attribution remain OPEN.

After `120f462`, the active-owner discovery bridge passes the complete trusted
interval into the persistent scanner. Production discovery requires typed
`ActiveLaneStateV1`, exact current scheduler Owner and Store Incarnation,
`evidence.earliest >= certificate.issuedAt.latest` and the strict
`evidence.latest < certificate.validUntil` boundary before promotion. Wrong
Owner and exact-expiry evidence both fail before a head is offered; scanner
rollback leaves the queue empty, and `OwnedDelayShard` fences the local Owner.
This is a discovery authorization fence, not Claim, permit, full channel/
credential-generation validation or Publish Admission authority.

After `666f56a`, the Registry distinction between outer `AuthorIdentityV1` and
nested `OwnerIdentityV1` is executable rather than prose-only. Admission body
field 10, Claim precondition field 14 and Ready Certificate field 2 now parse
and retain bare `OwnerIdentityV1`; Claim records and canonical attempt ledgers
carry the same bytes. The signed envelope alone retains the tagged Owner author
branch, and apply/replay compares its typed nested projection to the body.
Focused Admission/Certificate/Claim/runtime tests plus the full 1232-test local
suite pass; five real-Oxia smoke methods remain skipped without the endpoint.

After `9240f60`, the local READY-to-Claim handoff is no longer only a documented
future bridge. `ClaimHandoffWorkClassExecutor` takes an exact scheduler-polled
head, binds typed materialization/deadline/charge and trusted evidence into a
bounded `DUE_SCHEDULER` action, and repeats the READY/Message/Timeline/typed
Lane/Ready-Certificate checks after queue wait. `ClaimExecutionAdmission` proves
the local Worker/Shard/Lane message-and-byte caps, READY-lane minima and exact
message-generation reservation identity. Known queue/prerequisite/permit
deferrals restore the exact head; unexpected failures fence the Owner rather
than inventing a local retry authority. The focused Claim, permit, scheduler
and typed-Lane regressions pass, including the legal post-Claim state where a
READY certificate remains but the consumed physical key and timing projection
are absent. This closes only the shard-local composition seam; external
Profile/catalog, Object Store, Adapter serialization/size, channel/credential
generations, real Oxia authority and Publish Admission/Producer remain open.

After `ecf65c30`, the process-local Worker cap cannot be multiplied by injecting
a different `ClaimExecutionAdmission` into each shard executor. One
`WorkClassExecutionRegistry` binds one exact permit-pool instance; Claim and
Publish Admission construction must reuse it. Publish Admission additionally
verifies that the supplied Reservation was created by that exact pool before
action registration or external append. Registry rebinding and foreign-pool
Reservation regressions, focused Claim/Publish Admission suites and the
complete six-task local gate passed on 2026-08-13; five real-Oxia smokes were
skipped. This proves only a process-local composition invariant, not Oxia
capacity grants, production Worker assembly or dynamic I/O authority.

After `1601053`, the local Claim→`PUBLISH_ADMISSION` seam is executable through
`PublishAdmissionWorkClassExecutor` in the bounded `OUTCOME_AND_CONTROL` class.
The executor binds the exact Claim/reservation, descriptor, Ready Certificate,
Trusted-UTC evidence and canonical signed mutation before queue admission, then
rereads owner/ownerEpoch and Claim after queue wait and calls only the external
`ShardLogMutationAppender`. It does not allocate a Source Position or invoke
`DelayShard.applySystemMutation`; persisted positions are checked against the
assignment/physical activation barrier and Pulsar source-generation/guard
attestation when applicable. Definite non-persistence, prerequisite deferral and
unknown outcomes retain the exact reservation until source-ordered apply or
explicit revoke; unknown outcomes fence the Owner. The focused executor test
passes. This is local composition evidence, not production Shard Log
append/ACK/cursor, Oxia, source-session, Profile/Object Store, Producer or
source-ordered outcome evidence, so those release gates remain OPEN.

After `187c18c`, `OutcomeWorkClassExecutor` closes the corresponding local
queue boundary for already prepared `PUBLISH_OUTCOME`, `EVIDENCE_RESOLUTION`,
`CLAIM_RESULT` and `DLQ_EXPORT_RESULT` mutations. It binds the exact canonical
frame and charge, rejects wrong type/shard/author/owner-epoch submissions before
queue admission, rereads the strict Owner Lease before the external append,
and preserves `PERSISTED`, `DEFINITIVELY_NOT_PERSISTED` and `UNKNOWN` as separate
outcomes. It does not generate callback semantics, apply RocksDB state or
allocate a local Source Position; a writer/proof failure fences the local
Owner. The focused test proves queue rejection is side-effect free, persisted
handoff does not locally apply the mutation, and expired ownership never calls
the appender. This is still local composition evidence: Broker append/ACK,
callback/evidence authority, Oxia session, source replay and signing-key
history remain OPEN.

After `aed3352`, resource retirement has a separate strict `GC`-class bridge in
`GcWorkClassExecutor`. It accepts only exact signed `RESOURCE_RETIRE_INTENT` or
`RESOURCE_DELETE_CONFIRMED` mutations, binds their canonical frame/charge,
checks retire identity, protection-source shard and service author before queue
admission, then rereads the Owner Lease before the external Shard Log append.
Queue rejection is side-effect free; the bridge does not call provider delete,
write `gc_cf`, apply RocksDB state or allocate a local Source Position. Persisted
positions are barrier-checked and non-persistence/unknown remain distinct;
writer or proof failure fences the Owner. The focused test covers persisted
retire with no local tombstone, definitive non-persistence, queue rejection and
expired ownership. External delete/quiescence, Recovery Floor, source-ordered
tombstone application, quota release and compaction remain OPEN.

After `77f1587`, control mutations have a separate strict bridge in
`ControlWorkClassExecutor`. The bridge accepts only exact signed
`APPLY_SHARD_CONTROL`, `REPLAY_DEAD_LETTER`, `RESOLVE_UNCERTAIN` or `TIME_FENCE`
frames, checks control/body identity and the time-fence proof before queue
admission, rereads Owner Lease/clock before external append, and preserves the
three append outcomes. It does not register targets, apply RocksDB state or
allocate a local Source Position; queue rejection is side-effect free and
writer/proof failure fences the Owner. The focused test proves a persisted
TIME_FENCE is not locally applied and expired ownership never calls the
appender. Control authorization/registration, Broker/Oxia/source replay and
signing-key history remain OPEN.

`LeaseFenceWorkClassExecutor` now closes the local `LEASE_FENCE` handoff
boundary. The task identity includes the complete observed Owner Lease bytes;
execution rereads the authoritative lease and clock, does not fence when the
exact lease is still valid, and fences/stops only the matching local Owner when
the lease is expired or replaced. A queued task from an old Owner cannot fence
a replacement local identity, and queue rejection is side-effect free.
`LeaseFenceWorkClassExecutorTest` covers the expiry/replacement, live-owner and
rejection paths. This evidence is process-local: Oxia session-watch delivery,
Broker consumer pause/rewind, scheduler shutdown and cross-Worker stop
authority remain release gates.

After `fc0cf8a`, `QueryWorkClassExecutor` closes the local read-only `QUERY`
handoff. It binds exact Shard/request bytes, performs no authority or Store
read before queue admission, rereads the Owner Lease/clock around one injected
bounded read, and discards the result when ownership changes. The focused test
covers successful read linearization, ownership loss, expired-owner fencing and
queue rejection without invoking the callback. This does not claim production
Gateway routing, tenant authorization, retention calculation, cross-worker
forwarding or observability authority.

The synchronized full local gate after `666f56a` and this documentation update
passed on 2026-08-12 with 1232 reported
tests, zero failures/errors and five skipped opt-in real-Oxia methods because
the endpoint was unset. `checkDocumentation` and `checkstyleMain` passed in the
same `clean check --rerun-tasks` run. This verifies the repository-local change
set; it does not convert skipped real-service or external authority gates into
PASS.

After `fe7d484`, this local seam covers all eight V1 work classes, including
the independent `CHECKPOINT` execution queue. `CheckpointScheduler` remains
the process-local due-time/claim layer; a claimed checkpoint must still enter
the bounded `CHECKPOINT` turn and resource path before physical checkpoint I/O.

After `5f90f38`, the full local gate
`GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
--rerun-tasks --console=plain` completed with 1210 tests and zero
failures/errors. The five opt-in real-Oxia methods were skipped because the
endpoint was unset; this is local regression evidence, not a production
Oxia or external-service PASS.

After `0081e9d`, those five opt-in methods were rerun against a temporary
standalone Oxia service built from source commit
`37a17bef17202d5fd6e23282da5fd26d94865484`. The focused Gradle run completed
with 5 tests, zero failures/errors/skips, and the service was stopped after
the run. This confirms the implemented single-record Oxia smoke boundaries;
it does not prove the V1-required cross-record Owner Lease/Upload
Intent/Catalog/RecoveryPin transaction or production authority.

After `9d37ad9`, `WorkClassDispatcher` closes the local handler-composition
gap: all eight V1 classes must have a handler before work can be accepted,
and each selected task still runs through `WorkClassEventLoop` resource and
bounded-turn checks. The focused dispatcher regression covers missing-class
rejection, `CHECKPOINT` routing and handler-failure cleanup. This is a local
composition boundary; it does not claim shard-specific WriteBatch/IO or
external Worker authority.

After `2f78bd9`, the complete local `clean check --rerun-tasks` gate reported
1213 test cases with zero failures/errors; five real-Oxia methods were skipped
because the endpoint was unset. The dispatcher tests are included in this
gate, which is local regression evidence only and not a production Worker or
external-service PASS.

After `da9a8ac`, the mixed replay path validates the exact typed outcome
projection before advancing either the caller-owned Source Replay cursor or
the local last-position projection. A malformed post-WriteBatch projection
now fences the Owner without creating a position-ahead-of-cursor state. The
focused owner replay/recovery regressions and `checkstyleMain` passed. This
closes a local ordering gap only; Broker source continuity and Oxia
lease/session evidence remain external release gates.

After `9405f7c`, `replayCatchupTurn` and
`replaySystemMutationsTurn` now perform the same physical-entry result
projection before advancing their shared cursor. Logical duplicate results
remain durable at the first Source Position, while returned type-specific
results are anchored to the current physical record; a malformed projection
fences the Owner. The focused `OwnerLeaseTest` covers both branches. This
closes the local typed-replay ordering gap only and leaves Broker continuity,
Oxia lease/session and production Worker authority as release gates.

After `6bc3135`, active apply and all bounded replay branches also fence on
unexpected `RuntimeException`, not only typed RocksDB/native failures. This
keeps an unproven local state or commit boundary from retaining command or
replay authority; deterministic business rejections remain typed results.
`OwnerLeaseTest` covers a regressed active source position and confirms the
message was not applied. The change closes only the local fail-closed fence;
Broker/Oxia authority and release-scale evidence remain open.

After `c4391ca`, checkpoint restore admission covers the complete local
download-to-install interval: `CheckpointRestoreCoordinator` acquires a
Worker-wide idempotent permit before provider I/O, and the same permit remains
held through inventory validation and `ShardStore` Store-Incarnation
installation.  The focused coordinator regression rejects a nested second
download-slot acquisition from inside the provider callback.  This closes the
local process-level concurrency boundary only; it does not close remote Object
Store, Owner Lease/session, Source Assignment, Source Log replay or real-broker
release gates.

After `dc7a300`, the full local Gradle gate completed successfully with 1205
tests and zero failures/errors; 5 opt-in real-Oxia methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset.  This confirms the checkpoint-download
permit change across the repository regression suite but does not convert the
remaining external Object Store, broker, authority, chaos, benchmark, soak or
upgrade gates into PASS evidence.

The 2026-08-12 transport-source check is intentionally recorded as blocker
evidence rather than a PASS claim.  Kafka source
`76f62f3b83e882105219b6c7687dbde594a8b8a2` exposes Produce v13 topic-ID wire
fields and Nereus broker-side exact-topic-ID checks, but its stock producer
path remains name-keyed and may emit a zero UUID before metadata is available.
Pulsar source `11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9` exposes broker-side
Nereus topic-open/write-fence hooks, but not the Delay client-side guarded
resource-incarnation and send-evidence contract.  Consequently the audit's
Kafka/Pulsar real-service gate remains open until pinned transports, response
classification, source assignment/barrier proof and real-broker tests exist.

An earlier real-service check on 2026-08-12 built Oxia source commit
`37a17bef17202d5fd6e23282da5fd26d94865484`, started a temporary standalone
service, and ran:

```text
NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:6648 \
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle \
./gradlew clean check --rerun-tasks --console=plain
BUILD SUCCESSFUL in 1m 1s
```

The five opt-in real-Oxia methods all executed with zero skips, failures and
errors: Owner Lease/ephemeral session; Control Target/Operation CAS; Recovery
Catalog/Floor plus local-reuse validation; and Checkpoint Upload Intent
create/publish/reopen. This upgrades the Oxia single-record smoke evidence
from environment-skipped to live-service PASS, but remains only repository
evidence. It does not close cross-record authority transactions, Object Store
publication, Kafka/Pulsar transport, chaos, benchmark, soak or upgrade gates.

The newer 2026-08-15 check additionally ran the real Gateway audit sink
smoke from `8e0ed49b706dda2a6cb0d7d011c72d2a9270157b` against the same
standalone service shape. It confirms the Oxia-backed digest-only audit
record is durable and exactly deduplicated at the live client boundary; it
does not broaden the single-record result into a Gateway HA or cross-record
transaction claim.

After `1aca36a`, the repository also has a concrete local provider seam:
`FilesystemCheckpointUploadAdapter` streams the complete checkpoint inventory
to deterministic immutable object paths, writes the manifest last through an
atomic temporary-file boundary, and verifies exact bytes/hashes on retry. Its
regressions cover response-loss-style idempotency, immutable-object conflict and
source symlink rejection. This is useful evidence for the local physical upload
ordering only; it does not change the audit result for remote credentials,
provider quiescence/attestation/deletion, Owner Lease/session, cross-record
Upload Intent/Catalog transaction or real Object Store conformance.

After `4b1c2b0`, the matching local download boundary is explicit:
`FilesystemCheckpointDownloadAdapter` validates the catalog-bound manifest
object/resource identity, streams every immutable object into a private
temporary tree, re-inventories the complete tree, and publishes the requested
directory only after an atomic rename. Corrupt objects, path/symlink attacks and
existing targets fail closed without leaving a partial restore tree. This closes
the local provider-to-restore physical ordering gap only; it does not change the
open gates for remote Object Store authority, RecoveryPin/Oxia transactions,
source replay or real-service conformance.

After `f9e8583`, `CheckpointRestoreCoordinator` connects that provider boundary
to the existing finite-limit `ShardStore` restore protocol. It rejects provider
paths outside a per-attempt staging directory, verifies the complete downloaded
inventory before opening RocksDB, and deletes the provider tree only after the
new Store Incarnation is installed. The real-checkpoint regression covers both
the round trip and the out-of-bound return; this remains local orchestration
evidence and does not close Owner Lease/Source Assignment, RecoveryPin/Oxia
transaction, Source Log replay or real Object Store gates.

After `465d6de`, the large-payload local adapter also has a durable filesystem
byte backend:
`FilesystemPayloadObjectStore` preserves the existing reservation/handle/proof
contract while publishing immutable payload bytes through no-follow reads,
private temporary files, fsync and atomic rename. Its restart regression proves
that re-registering the same source-ordered reservation reproduces the exact
handle and proof; corruption, conflicting bytes and a symlinked root fail
closed. This is physical local/test evidence only and does not close remote
Object Store credentials, provider quiescence/attestation/deletion, Oxia
protection or source-ordered reservation authority.

After `da3146c`, the full Gradle gate completed successfully (`clean check
--rerun-tasks`, 1205 tests, no failures). Five opt-in real-Oxia smoke methods
were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this confirms the
local regression suite only and does not close the external Oxia, Object Store,
Kafka/Pulsar, chaos, benchmark, soak or upgrade gates.

After `6efd89f`, the local checkpoint execution boundary is explicit rather
than implicit: `CheckpointExecutionCoordinator` requires the exact scheduler
claim before filesystem/provider I/O, creates or safely reuses the fixed-ID
physical checkpoint, verifies the physical checkpoint's source/mutation,
evidence and control projections against the canonical manifest, and completes
the same claim after either outcome. Its response-loss regression reuses the
same local image and PUBLISHED intent without invoking the provider twice.
`CheckpointWorkClassExecutor` now adds the previously missing bounded
execution admission: it preflights the same claim/intent without I/O, queues an
exact `CHECKPOINT` action, repeats the fence at execution, and leaves queue
rejection side-effect free. The underlying execution method is package-private,
so cross-package composition cannot bypass that public entrypoint. Ordinary
attempt failure remains owned by the checkpoint claim/outcome path rather than
creating a second WorkClass retry.
This closes a local orchestration/documentation gap in §16.2; it does not
upgrade the audit to a production checkpoint PASS because the Owner
Lease/session + Upload Intent + Catalog transaction, Object Store
attestation/quiescence, Source Assignment and real transport evidence remain
open gates.

After `4e2cf94`, local recovery reuse no longer rewrites a Store's runtime or
recovery projection merely by opening the ACTIVE DB. `ShardStore` first passes
the persisted projection (including the prior `CLOSED_CLEAN` install state in
the clean-reopen case) to the catalog/Floor validator; only after that proof
returns does a synchronous WriteBatch publish the new `OPEN` marker. The
focused `ShardStoreTest.localRecoveryReuseOpensOnlyCatalogValidatedActiveStore`
regression and the full `./gradlew clean check --rerun-tasks --console=plain`
gate passed on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). This closes the local
open-phase ordering drift described by the main design; it does not claim
Owner Lease/session, source replay or final activation authority.

After `a9321b4`, the strict local catch-up entrypoint also CASes the exact
context-bound Owner Lease to `CATCHING_UP` before exposing the replay gate.
`OwnedDelayShard.markCatchingUp` accepts only an exact successor or an exact
response-loss reread with unchanged fencing/assignment/session identity and a
non-regressed expiry; a contextless compatibility lease is rejected before
the authority transition. `OwnerLeaseTest` covers both the authoritative
state publication and the fail-closed legacy path. Commit `38f6a60` adds a
response-loss backend regression that drops the successful CAS response and
verifies that only an exact authority reread is accepted. This aligns the
local catch-up lifecycle with section 9.2, while source-assignment
publication, session orchestration, cross-record activation/pin authority and
real Broker replay remain release blockers.

After `6583eac`, strict catch-up replay rereads the same Oxia lease before
each bounded turn and source record, requiring exact fencing/assignment/session
identity, `CATCHING_UP` state and a non-regressed valid expiry. The new
`OwnerLeaseTest` replacement-owner regression proves that the next record is
not applied after authority changes; the authority-read failure regression
proves the authoritative apply path fences before mutation. This closes only
the local replay guard; production session orchestration, assignment/barrier
publication and real Broker replay remain open.

After `b387648`, the control-snapshot activation entrypoint rejects a
contextless lease before local Store writes or the authoritative lifecycle CAS;
only a context-bound strict catch-up window can open `ACTIVE_FOR_COMMANDS` on
that path. `OwnerLeaseTest` covers the strict snapshot success and unchanged
`ACQUIRING` authority on rejection. The legacy activation overloads remain
explicit embedded seams, and the production control catalog plus atomic
RecoveryPin/lease transaction are still open.

After `45ec559`, strict activation rereads the exact `CATCHING_UP` lease before
any local owner-open or Claim-recovery write. The replacement-owner regression
confirms that an authority change fences before `lastOpenedOwnerEpoch` is
persisted, closing the local pre-CAS projection gap while leaving the
production atomic control-catalog/RecoveryPin transaction and real session
authority open.

The planned-drain boundary now carries the same context requirement. A strict
owner must use `OwnedDelayShard.beginDrainStrict`, which validates the exact
assignment/session-bound lease and accepted Source Assignment before the
`ACTIVE_FOR_COMMANDS -> DRAINING` CAS. `OwnerDrainCoordinator` chooses this
path for strict owners; contextless assignment-only owners remain an explicit
embedded compatibility seam. `OwnerLeaseTest.strictDrainRequiresTheContextBoundCatchupLease`
and `OwnerLeaseTest.strictDrainPreservesAssignmentAndSessionFenceThroughAuthorityCas`
cover the rejection and successful identity-preservation paths. This closes a
local lifecycle ordering gap, not the production Oxia session/assignment or
cross-worker drain orchestration gate.

Authoritative Command mutation now has an equivalent strict entrypoint:
`OwnedDelayShard.applyAuthoritativelyStrict` rejects a contextless lease before
the Delegate and then rereads the exact active lease before applying. The
context-bound success and contextless no-mutation regressions are in
`OwnerLeaseTest.strictAuthoritativeApplyUsesTheContextBoundOwnerLease` and
`OwnerLeaseTest.strictAuthoritativeApplyRejectsAContextlessCompatibilityLease`.
This closes the local mutation gate, while production source-writer wiring and
real assignment/session authority remain open.

The checkpoint upload boundary now rereads the exact `PENDING_UPLOAD` intent
after acquiring the Worker upload slot and immediately before provider I/O.
If a concurrent caller already committed the exact `PUBLISHED` successor, the
coordinator returns it without invoking the adapter; any other revision/state
change fails closed. `CheckpointUploadCoordinatorTest.rereadsIntentAfterUploadSlotBeforeProviderIo`
covers the race. This closes a local stale-provider-I/O window only; Object
Store quiescence/attestation, owner-abandonment proof and the Oxia
intent/catalog transaction remain release gates.

The post-`3527c89` local verification `./gradlew clean check --rerun-tasks
--console=plain` passed on 2026-08-12. Five opt-in real-Oxia methods were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is repository
evidence only and does not change the external transport/Oxia/Object Store,
chaos, benchmark or soak release gates.

The journal Source Position identity-fence follow-up (`22b9409`, `5d22d8a`)
was rechecked with the same full command on 2026-08-12 and passed
(`BUILD SUCCESSFUL`, 5 tasks). The real-Oxia methods remained skipped because
the endpoint was unset, so this is only local regression evidence.

The local GC protection projection now closes the public `ProtectionRef`
construction boundary as well: source-bearing kinds canonical-decode
`minimumSourcePosition`, reject incompatible optional fields and require the
supplied canonical bytes to match the typed fields. The regression is
`ResourceRetireIntentBodyTest.protectionRefConstructorRequiresCanonicalSourceAndKindSpecificFields`;
Recovery-Floor ancestry and external catalog authority remain open.

The enclosing retire identity values now have equivalent direct-construction
guards: `ExactResourceIdentity` revalidates its branch and identity hash, and
`ProtectionSet` revalidates strict reference ordering, uniqueness, digest and
canonical bytes. The focused regression is
`ResourceRetireIntentBodyTest.exactIdentityAndProtectionSetConstructorsRequireCanonicalDigestsAndOrdering`;
provider and Recovery-Floor authority are still external gates.

The persisted GC intent boundary now decodes the complete canonical
`ProtectionSet` before writing `gc_cf/TASK`, and `DelayShard` checks every
source-bearing protection position against its current Shard. A foreign-Shard
anchor therefore becomes a source-ordered stale rejection without a durable
retire record. Focused evidence is
`DelayShardTest.resourceRetireIntentRejectsForeignProtectionSourceBeforePersistence`;
catalog/Floor and provider deletion authority remain open.

After the retire identity/protection constructor and persistence fences
(`18629dd`, `2a84e88`, `4a81566`, `7180153`, `ac635b6`, `ede3ff3`), the full
`./gradlew clean check --rerun-tasks --console=plain` recheck also passed on
2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks); the real-Oxia methods were still
skipped because no endpoint was configured.

The physical `dedupe_cf/POSITION` key now accepts only a canonical decoded
Source Position, so a malformed or trailing-byte input cannot create a
look-alike audit locator. The focused evidence is
`KeyCodecTest.dedupePositionRequiresCanonicalSourcePositionBytes` in
`0259ffb`; this closes the local key-codec boundary without claiming source
assignment, Broker receipt or external durability authority.

After `0259ffb`, `./gradlew clean check --rerun-tasks --console=plain` passed
again on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The five real-Oxia methods
remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset, so this is
local regression evidence rather than live-service or release evidence.

`SourceReplayRecord` and `SourceReplayMutation` now reject entries whose
command or signed mutation is bound to a different Shard than the supplied
Source Position; `SourceReplayEntryTest` covers both constructor fences. The
local typed replay boundary is therefore aligned with the one-shard Source
Log model, while adapter assignment and continuity proofs remain release gates.

After `19dede3`, `./gradlew clean check --rerun-tasks --console=plain` passed
again on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The same five real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset.

The Recovery Pin persistence audit now preserves the intended historical
protection window: creating a session-bound pin records the exact Floor it
observed, but later Floor advancement must not make a still-active pin
unreopenable.  Catalog snapshot validation therefore checks that historical
Floor manifest and the candidate's complete parent-hash ancestry remain
published, instead of requiring the pin to equal the current Floor.  The
cross-restart regression is
`PersistentRecoveryCatalogTest.historicalRecoveryPinSurvivesReopenAfterFloorAdvances`.
Restore still revalidates the pin against the current Floor before staging and
before ACTIVE publication, so a candidate superseded by Floor advancement is
protected from deletion but cannot be installed through a stale pin.

The local physical-boundary audit now also covers the DB open call itself:
every descendant of the configured Worker root is created and rechecked as a
real directory before RocksDB is opened, including a fresh Store Incarnation
or restore staging parent. A concurrent `FileAlreadyExists` is accepted only
after the component passes the same `NOFOLLOW_LINKS` directory check, closing
the `Files.createDirectories(dbPath)` symlink-following window. This is local
path-safety evidence; it does not replace the external ownership/placement
authority or real-service recovery gates.

The checkpoint manifest audit now rejects duplicate `(objectKey,
objectVersion)` identities and checksum reuse with conflicting file lengths at
the immutable `CheckpointManifest` boundary. Same-length checksum reuse remains
allowed, while a duplicate provider identity can no longer make two manifest
entries address the same immutable object. The focused regressions are
`CheckpointManifestTest.duplicateObjectIdentityIsRejectedBeforePublication` and
`CheckpointManifestTest.checksumWithConflictingLengthsIsRejectedButSameLengthReuseIsAllowed`;
provider upload/attestation and catalog publication remain external gates.

The checkpoint inventory audit now binds length and SHA-256 to one
`NOFOLLOW_LINKS` file channel instead of checking a path and reopening it for
the hash. A path replacement cannot redirect the checksum read to a symlink,
and a length change during hashing fails closed. The existing regression is
`CheckpointManifestTest.inventoryRejectsSymlinkedCheckpointFiles`; provider
immutability and external attestation remain release gates.

The physical checkpoint boundary now also covers directory creation before the
RocksDB checkpoint/copy call: checkpoint parent, `checkpoint-tmp`, restore
staging descendants and the installed parent are created one component at a
time and rechecked without following a local symlink. A leading deployment
symlink before the nearest existing anchor remains allowed, while a symlink at
the shard-owned temporary boundary fails closed; the focused regression is
`ShardStoreTest.checkpointRejectsSymbolicParentComponentBeforeCreatingOutsideFiles`.
This closes local path handling only; provider and process-recovery authority
remain release blockers.

The same local path boundary now covers the crash-durable Recovery Catalog,
Checkpoint Upload Intent and SLO collector projections. Their state-file
parents are created and rechecked component by component with `NOFOLLOW_LINKS`,
so a missing intermediate directory cannot follow a pre-planted or raced
symlink before the lock/temp/state file is created. The focused regressions are
`PersistentRecoveryCatalogTest.rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary`,
`CheckpointUploadIntentStoreTest.rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary`
and `PersistentSloObservationCollectorTest.rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary`.
This is local crash-durable evidence, not external Oxia/Object Store/collector
authority.

The same guard now covers the local Control Operation and Owner Lease roots,
with focused regressions
`PersistentControlOperationAuthorityTest.rejectsSymbolicParentComponentBeforeCreatingControlStateOutsideBoundary`
and `PersistentOwnerLeaseStoreTest.rejectsSymbolicParentComponentBeforeCreatingLeaseStateOutsideBoundary`.
This removes the remaining local root-initialization path that could have
followed an intermediate symlink; production Oxia/session fencing remains an
external release gate.

Crash-durable state reads now bind the bounded size check and byte read to one
`NOFOLLOW_LINKS` file handle. Recovery Catalog, Upload Intent, SLO Collector,
Control Operation, Owner Lease and the shard `ACTIVE` pointer no longer use a
path check followed by `Files.readAllBytes`; symlink/directory replacement,
disappearance, size change and short reads fail closed. The focused helper
regression is `LocalStatePathGuardTest`. This is local physical-file evidence,
not external Oxia/Object Store authority or process-recovery evidence.

The per-shard RocksDB physical-usage probe now rejects a symbolic-link DB root,
symbolic links and non-regular entries under the DB path, and observes each
file size through one `NOFOLLOW_LINKS` handle. A deceptive or raced entry
therefore fences usage evidence instead of being silently omitted. The focused regression is
`ShardStoreTest.physicalUsageFailsClosedOnADeceptiveSymbolicFile`; process-wide
disk/quota authority remains an external gate.

ACTIVE pointer publication now writes the fixed `ACTIVE.tmp` path through a
`NOFOLLOW_LINKS` channel, forces the exact pointer bytes, and only then performs
the atomic rename. A raced replacement cannot redirect the pointer write via a
symlink; the focused temporary-path regression is
`ShardStoreTest.checkpointAndActivePointerTemporaryPathsRejectSymbolicLinks`.

The Lane-control audit now checks the closed Resume result union at the runtime
projection boundary: `OPEN` is `ALREADY_OPEN`, `ORDERING_BROKEN` and `CLOSED`
retain their stable rejection codes, and a same-key terminal guard is
`LANE_TERMINALLY_CLOSED`. Resolve retry and Dead Letter replay also distinguish
an ordinary closed Lane from an irreversibly retired one. The focused evidence
is `DelayShardTest`'s idempotent-Resume and terminal-guard-Resume cases. This
does not claim authenticated control registration or production Oxia authority.

The scheduler registry now rejects unregister for an ordinary `CLOSED` Lane;
only `RETIRED`—after the same-key terminal guard has been installed—can remove
the process-local ring, deficit and last-served projections. The focused
`LaneSchedulerTest` covers the non-terminal rejection, exact incarnation fence
and persistent cleanup. Recovery-Floor, adapter quiescence and terminal-guard
authority remain separate release evidence.

The local `LaneRecord` compatibility transition also retains the Close
`laneControlVersion` across `CLOSED -> RETIRED`, advancing only the runtime
projection. This prevents physical retirement from creating a synthetic
source-ordered management version; `LaneRecordTest` covers the invariant.

The terminal-guard audit now includes active payload reservations: the bounded
retirement proof refuses to install a guard while a `RESERVED` or `COMMITTED`
reservation still names the Lane. A stale large-payload Commit observes an
already installed same-key terminal guard before inspecting reservation state,
returns `LANE_TERMINALLY_CLOSED`, and cannot recreate an ACTIVE Lane value.
Activation/quota rebuild also rejects a live message or reservation that names
a retired Lane instead of reconstructing live quota usage.
`DelayShardTest.largePayloadCommitCannotResurrectTerminalLaneGuard` is the
focused regression. This closes a local resurrection and reopen path; external
close materialization, Floor/retention and adapter authority remain release
gates.

The Control target-registration audit now separates an authoritative binding
mismatch from an unavailable registry boundary.  Missing registration remains
an explicit `UNAUTHORIZED_SYSTEM_MUTATION` position result, but a lookup or
validation `RuntimeException` is retained at the Source Position instead of
being misclassified as an authorization rejection.  This preserves the
main-design rule that Oxia transient/unproven absence must stop replay; the
local regression is
`DelayShardTest.controlRegistrationAuthorityFailureRetainsSourcePositionForRetry`.
Production Oxia response classification and source replay authority remain
release gates.

The receipt-bound payload audit now separates a non-enumerating reservation
denial from local binding integrity failure.  Unknown/foreign shard or
service-owned receipt mismatch remains `NOT_FOUND_OR_NOT_AUTHORIZED`; a shard
read, deterministic adapter registration or service-owned receipt projection
exception is returned as closed `INTEGRITY_ERROR` instead of falsely proving
object absence.  `EmbeddedDelayServiceTest.payloadFacadeMapsLocalReservationBindingFailureAsIntegrityError`
covers the pinned trust-set mismatch vector.  A real provider or credential
failure must still be projected by the external adapter as
`OBJECT_STORE_UNAVAILABLE_RETRYABLE`.  Negative `nowEpochMs` is rejected before
adapter authority as closed `INTEGRITY_ERROR`, preserving the main design's
non-negative observation-time rule; the no-adapter regression also covers that
branch, while the deterministic adapter's direct typed seam is covered by
`InMemoryPayloadObjectStoreTest.negativeObservationTimeReturnsTypedIntegrityOutcome`.

The Message Query audit now maps local snapshot/read and public binding/DLQ
projection failures to closed `INTEGRITY_ERROR` responses instead of leaking
exceptional Futures; cross-shard identity remains the explicit
`RECEIPT_MISMATCH` branch.  `EmbeddedDelayServiceTest.messageQueryMapsPublicProjectionDriftToClosedIntegrityError`
covers both facade overloads, and null Message IDs are now `INVALID_RECEIPT`
through the same facade; the direct projector remains a throwing internal
validation seam.

The Command Query audit now keeps the same response boundary: null locators are
`INVALID_RECEIPT`, local POSITION/result/projection failures are
`INTEGRITY_ERROR`, and a proved command identity/position/hash mismatch remains
`RECEIPT_MISMATCH`.  This closes exceptional-Future leakage without changing
pending, barrier or retention semantics.

The Control Operation Query audit now applies the closed union at both the
authority seams and the embedded facade: null receipts or negative observation
times are `INVALID_RECEIPT`, while an unproven local authority read or response
binding is `INTEGRITY_ERROR`.  Complete receipt matching, fixed retention expiry
and CURRENT/error projection semantics are unchanged.

The uncertain-Store drain audit now also covers the race where native Store
close was started by another caller before the coordinator observed the
unproven write boundary.  An already-started close does not prove source or
scheduler quiescence: the coordinator still fences the local Owner and invokes
`stopSourceAndScheduling` exactly once, retaining that completion across
retryable close/release teardown.  The normal ACTIVE-to-DRAINING path records
the same completion before its authority CAS, so a later close/release retry
cannot invoke the callback a second time.  The regression is
`OwnerDrainCoordinatorTest.uncertainStoreWithExternalCloseStillStopsSourceBeforeRelease`.
The normal close-retry count is covered by
`OwnerDrainCoordinatorTest.storeCloseFailureLeavesDrainingStateForRetryableTeardown`.
This remains local evidence; external source quiescence and Oxia lease release
are still release gates.

The Owner replay audit now covers the previously asymmetric pure System
Mutation path: Command, System Mutation and mixed replay all fence the local
Owner on `RocksDbWriteFailure` or fatal `Error` before rethrowing, and retain the
source record/cursor for a fresh Store incarnation. The focused regression is
`OwnerLeaseTest.fatalSystemMutationReplayFencesOwnerAndRetainsSourceCursor`.
This closes only the local owner gate; source continuity, fresh-incarnation
recovery and Oxia lease authority remain release gates.

The same replay boundary now covers failures from the backing source iterator:
`hasNext`, look-ahead `peek` and cursor advancement are fenced before a
`RuntimeException`/`Error` escapes, so an unavailable or malformed source
cannot leave an Owner in `CATCHING_UP` with a continuity proof it no longer
holds. `OwnerLeaseTest.sourceCursorFailureFencesEveryReplayPathBeforeApplyingOrAdvancing`
covers Command, System Mutation and mixed replay; the failed read occurs
before any record is applied and `lastCatchupPosition` remains unchanged.
This is local fail-closed evidence only; real broker consumer continuity and
fresh-incarnation replay remain release gates.

The source-successor audit now also closes the deterministic gap branch:
wrong-source, regression, conflicting canonical identity and non-successor
positions are represented by a typed local proof and move the Owner to
`FAILED(SOURCE_GAP)` rather than leaving it in `CATCHING_UP`. The offending
record and `lastCatchupPosition` remain unchanged. A continuity proof that
throws an unrelated runtime or fatal error, or a record that cannot be
canonically bounded, fences the Owner instead because no durable gap has been
proven. The focused regression is
`OwnerLeaseTest.v1CatchupPinsTheAdapterSuccessorAndRejectsAKafkaGapBeforeApplyingIt`;
Oxia shard-status/reason publication and real Broker continuity remain release
gates.

The activation audit also closes the metadata/requeue asymmetry: once either
activation path starts its owner-open marker and Claim-requeue projection, any
Store/recovery `RuntimeException` or fatal `Error` fences the Owner before the
authoritative `ACTIVE_FOR_COMMANDS` CAS. The regression is
`OwnerLeaseTest.activationMetadataIntegrityFailureFencesBeforeAuthorityCas`.
An unreached source barrier, missing control snapshot or expired lease remains a
precondition failure in `CATCHING_UP` (or the existing lease fence), not a
post-start Store failure.

The owner-drain audit now closes the uncertain-Store callback ordering gap:
when a native commit boundary is already unproven, the local Owner is fenced
before `stopSourceAndScheduling` is invoked. A callback `RuntimeException` or
fatal `Error` can no longer leave that Store under an
`ACTIVE_FOR_COMMANDS` gate; teardown retains the existing retryable exact-lease
and native-close rules. The focused regression is
`OwnerDrainCoordinatorTest.uncertainStoreFencesBeforeStopCallbackFailureCanEscape`.
This remains local fail-closed evidence; external source quiescence, Oxia
release and fresh-incarnation recovery are release gates.

The checkpoint scheduler audit also rejects a time-reversed completion: a
`completedAt` earlier than the exact claim's `dueAt` is not allowed to clear the
in-flight marker or reschedule the shard. This prevents stale callback timestamps
from moving the next due time backwards; the claim remains retryable and the
focused regression is
`CheckpointSchedulerTest.completionBeforeClaimDueFailsClosedAndKeepsClaimInFlight`.
The scheduler remains process-local; durable upload/catalog publication is still
external authority.

The drain durability audit now also covers `flushAndSync()`: native,
JNI/runtime and fatal failures mark the Store `writeOutcomeUncertain` before the
exception escapes. The shard remains `DRAINING`; a later retry must enter the
uncertain-Store branch and perform only local fencing, native close and exact
lease release, never reuse that incarnation or rerun Claim/callback/checkpoint
work. `ShardStoreTest.flushAndSyncFailureFencesStoreUntilReopen` is the local
regression. Device/WAL durability and external source/Oxia recovery evidence
remain release gates.

The Kafka receipt and Pulsar Attempt Journal mapping-before-send seams now
reject a target sender that returns `null CompletionStage` with a typed
integrity/divergence failure. The mapping is already durable and remains the
unresolved lower-sequence fence; the malformed stage cannot be treated as a
non-persistence proof, release the next sequence, or leak an unclassified
`NullPointerException`. Focused evidence is
`KafkaReceiptJournalTest.nullTargetStageFailsClosedAndKeepsTheMappedAttemptUnresolved`
and
`PulsarAttemptJournalTest.nullTargetStageFailsClosedAndKeepsTheMappedAttemptUnresolved`.
This is local transport-SPI evidence only; the production caller still must
classify the physical operation as UNKNOWN/evidence-pending until Broker
completion or a certified teardown proof exists.

The physical publish wrapper also releases the reservation when its Adapter
executor synchronously throws a fatal `Error` before accepting the delegate
task, then rethrows that failure. The boundary is pre-ownership, so it cannot
leave an active request charge or produce a target-side `UNKNOWN`; focused
evidence is `BoundedDestinationPublishAdapterTest.fatalExecutorRejectionReleasesPhysicalChargeBeforeRethrowing`.
The wrapper now records task start before invoking the delegate, so an inline or
custom executor that throws after accepting the task cannot be mistaken for the
pre-ownership rejection path: the unknown physical operation remains fenced as
zombie/in-flight. `BoundedDestinationPublishAdapterTest.inlineDelegateFatalFailureRetainsPhysicalChargeAfterTaskWasAccepted`
covers this accepted-task boundary.
The wrapper now installs its release observer before `Executor.execute` as well.
If a custom executor runs the delegate task and then throws while returning
from `execute`, the delegate CompletionStage callback can still release the
retained zombie charge; `BoundedDestinationPublishAdapterTest.executorFatalAfterAcceptedTaskRetainsUntilDelegateCompletion`
covers that post-accept executor-failure window.
Production executor admission and target ownership remain external release
gates.

The local checkpoint reaping and resource-GC guards now classify fatal `Error`
from catalog/Floor/RecoveryPin reads the same as an unavailable authority
response. They return the existing protection decisions and cannot authorize
reaping or tombstone compaction after a broken read boundary. Focused evidence
is `CheckpointReapingGuardTest.failsClosedWhenCatalogReadThrowsFatalError`,
`CheckpointReapingGuardTest.failsClosedWhenRecoveryPinReadThrowsFatalError`,
`ResourceGcGuardTest.checkpointGcFailsClosedWhenRecoveryFloorReadThrowsFatalError`,
`ResourceGcGuardTest.checkpointGcFailsClosedWhenFloorCoverageThrowsFatalError`
and `ResourceGcGuardTest.checkpointGcFailsClosedWhenRecoveryPinReadThrowsFatalError`.
This remains local fail-closed evidence; Oxia CAS, provider ownership and
quiescence are external release gates.

The local activation path now records the current `ownerEpoch` in the
Store's non-decreasing `lastOpenedOwnerEpoch` projection before opening the
Command gate, for both embedded and authoritative activation. A failed
metadata/requeue batch cannot advance the source cursor or expose
`ACTIVE_FOR_COMMANDS`; a later loss of the authority CAS leaves only a
conservative owner-open marker. `OwnerLeaseTest` covers the authoritative
activation branch. This closes the local metadata ordering seam, not the
production Oxia lease/session transaction, RecoveryPin deletion or source
replay authority.

The local checkpoint seam now gives every physical image a fresh non-zero
16-byte `checkpointId`, including the convenience creation path. The ID is
written before the RocksDB snapshot and survives restore; response-loss retry
callers must use the explicit-ID overload so the same identity is reused.
`ShardStoreTest.convenienceCheckpointAllocatesIdentityBeforeSnapshot` covers
the copied-image boundary. This is local identity evidence only; Oxia
upload-intent/catalog CAS and Object Store publication remain release gates.

The inner Lane DRR now closes the weight-downgrade cap gap: registering an
existing Lane with a lower scheduler weight recomputes the cap across all
registered Lanes and immediately clamps historical deficit credit. The
regression `LaneSchedulerTest.weightDowngradeRecomputesDeficitCapAndClampsExistingCredit`
proves that a `10`-byte quantum cannot retain the former `8 * quantum` credit
after a downgrade to weight `1`. This aligns the inner scheduler with the
outer Worker cap rule and ADR-0032; certified capacity limits, placement and
real service fairness evidence remain release gates.

The local Kafka receipt appender now computes the checked exclusive successor
before advancing its cursor. At the all-ones raw u64 offset, the append stays
exhausted and fails with the typed Journal integrity error; a failed position
construction can no longer wrap the seam to offset zero. The regression
`KafkaReceiptJournalTest.localAppenderFailsClosedBeforeUnsignedOffsetExhaustionCanWrap`
covers the repeated-failure boundary. This closes only the local appender
overflow seam; production Kafka transaction, read-committed LSO and retention
authority remain external release gates.

The Pulsar activation-barrier compatibility seam is now internally consistent:
the deprecated constructor can represent a non-empty legacy barrier without a
known batch size, while the identity, source-connection guard and inclusive
batch-index checks remain active. The full V1 constructor still requires and
pins a positive batch size, so this compatibility repair does not weaken the
V1 source-assignment contract. Focused evidence is
`SourceActivationBarrierTest.legacyPulsarBarrierAllowsUnknownBatchShapeWithoutWeakeningIdentityFence`;
real source assignment and Broker guard authority remain release gates.

The physical-admission wrapper now fails closed for a generic delegate that
throws before returning a stage or returns a null stage: neither condition is
proof that Producer ownership was avoided. The wrapper preserves logical
`UNKNOWN` and retains the physical request/byte charge until the returned
`PublishCall` has a certified completion or fenced-teardown release; only the
closed-gate and executor-rejection paths release before a delegate invocation.
`BoundedDestinationPublishAdapterTest.failedOrNullDelegateStageRetainsChargeUntilExplicitRelease`
covers both regressions. This is local admission evidence, not authenticated
Broker ownership or provider teardown proof. `DestinationPhysicalAdmission`
also rejects a candidate before transport when the complete currently-active
request/byte charge plus that candidate cannot fit the Lane's potential-zombie
envelope, so a later timeout cannot strand an admitted request outside its
zombie budget. The request and byte boundary regressions are
`DestinationPhysicalAdmissionTest.admissionReservesZombieRequestCapacityForAllOutstandingRequests`
and `DestinationPhysicalAdmissionTest.admissionReservesZombieByteCapacityForAllOutstandingRequests`.
The same wrapper now handles an asynchronous delegate or callback-registration
`Error` by completing the logical call as `UNKNOWN` and retaining the physical
charge before rethrowing the fatal failure; this prevents an executor task from
leaving an indefinitely pending `PublishCall`. Focused regressions are
`BoundedDestinationPublishAdapterTest.asynchronousDelegateErrorCompletesUnknownBeforeFatalFailureEscapes`
and `BoundedDestinationPublishAdapterTest.asynchronousCallbackRegistrationErrorCompletesUnknownBeforeFatalFailureEscapes`.

The local Owner Lease model now rejects a renewal that would shorten the
currently published expiry, matching the remote adapter's response fence and
leaving the previous lease unchanged. `OwnerLeaseTest.renewalCannotMoveTheLiveExpiryBackwards`
covers this CAS-parity edge; it does not replace Oxia session/ephemeral-record
authority or a live lease-loss test.

Open Attempt lookup/listing now uses the larger of the pending-Message and
outcome-reserve-record bounds. V1 permits one Message to retain multiple
unresolved Attempt obligations, so a scan limited only by `maxPendingMessages`
could falsely fence a healthy shard; `DelayShardTest.openAttemptLookupUsesOutcomeReserveBoundInsteadOfMessageBound`
covers the exact boundary. This remains local recovery evidence, not external
Producer ownership or outcome authority; activation reconciliation and
Lane-retirement scans apply the same bounded distinction.

The queued-receipt audit now has an executable local binding for the existing
Registry rule: `QueuedReceiptQueryPolicy` computes `receipt_query_until` from
Broker persistence time with checked arithmetic, and policy-bound Kafka/Pulsar
ingress plus managed submission paths reject missing or drifting Route snapshots
before Producer ownership. A boundary overflow after a persisted Broker result
is retained as `ENQUEUE_UNCERTAIN`/integrity evidence rather than a false
non-persistence proof. The legacy absolute-boundary overloads remain only as a
compatibility seam and are compared with the bound policy when configured;
Route publication, source-time authority and concrete production transports are
still release gates. Focused evidence is in `AdapterIngressTest` and
`NativeSubmissionAdapterTest`.

The same source-time rule is now executable for full command-result retention:
`CommandResultRetentionPolicy` derives the boundary from the applied result
Source Position with checked arithmetic, and the embedded query/await/applied
receipt bridges expose policy-bound overloads. A boundary overflow becomes a
bounded integrity error; callers cannot change the strict path by supplying an
absolute timestamp. Legacy absolute-boundary methods remain compatibility
seams only. Focused evidence is
`EmbeddedDelayServiceTest.embeddedQueryDerivesFullResultRetentionFromAppliedSourceTime`
and `CommandResultRetentionPolicyTest`; external policy publication and query
authority remain release gates.

The Control Operation receipt boundary now has the same executable local fence:
`ControlOperationQueryPolicy` must match the policy version carried by the
Prepared Control Operation and derives `queryUntil` from the trusted
registration upper bound before target registration. Version drift and checked
addition overflow fail closed without a partial local registration. The raw
absolute-window compatibility entrypoint now performs the same projection and
binding validation before publishing its target record, so an invalid window or
registration evidence cannot leave a target-only operation behind. Immutable
policy publication and the production Oxia registration transaction remain
external release gates. Focused evidence is
`ControlOperationQueryPolicyTest`,
`EmbeddedDelayServiceTest.strictPreparedControlRegistrationRejectsPolicyDriftAndOverflowBeforeRegistration`
and
`EmbeddedDelayServiceTest.compatibilityPreparedControlRegistrationValidatesBeforeTargetRegistration`.

The result-retention wire boundary now enforces both sides of the source-time
contract: `fullResultRetainUntilEpochMs` in
`CommandAppliedReceiptV1`, `PublicCommandResultV1` and
`CompactCommandResultV1` must be at least the result Source Position's Broker
persistence time. Direct construction and decoded values that place the
deadline before persistence are rejected before any query projection can use
them. `ProtocolCodecTest` covers all three malformed lower-bound cases; this
closes a local wire-integrity gap while source-time authority and retention
publication remain external release gates.

The embedded absolute-boundary query compatibility seam now checks that same
lower bound after reading the durable result Source Position and maps an early
boundary or malformed local projection to typed `INTEGRITY_ERROR`. It no longer
leaks a result-constructor exception to the caller. The strict policy path still
derives its boundary from source time; `EmbeddedDelayServiceTest` covers the
compatibility-path regression. This is local fail-closed evidence only.

The Kafka receipt-journal replay boundary now compares reconstructed mapped
positions by canonical bytes. This preserves the Registry requirement that
response-loss/replay of the same durable mapping is an exact idempotent no-op;
the generated Java equality of the embedded receipt-hash array is not used as
an identity proof. `KafkaReceiptJournalTest`
`reconstructedMappedRecordReplayUsesCanonicalPositionBytes` covers the decoded
position case. It does not replace the production Kafka transaction,
`read_committed`, LSO or retention evidence gates.

The Pulsar Attempt Journal now uses the same canonical-byte identity for
replayed `MAPPED` and `RETIRED_NOT_PUBLISHED` positions. Both reconstructed
position branches remain exact idempotent no-ops instead of depending on Java
record/object equality. `PulsarAttemptJournalTest`
`reconstructedMappedAndRetirementReplayUsesCanonicalPositionBytes` covers the
two branches; the real Journal topic, ExclusiveWithFencing and Broker evidence
gates remain external.

The local Pulsar Journal appender now closes its raw u64 `entryId` domain
without wraparound: the all-ones entry is accepted once, then every later
append fails with `INTEGRITY_ERROR` before any local Journal state changes.
`PulsarAttemptJournalTest.localAppenderFailsClosedAfterUnsignedEntryExhaustionWithoutWrapping`
proves repeated failure after the boundary. This closes only the deterministic
appender seam; Broker Journal ownership, fencing and durability evidence remain
release gates.

The generic bounded destination wrapper now rejects a malformed
`CompletionStage` whose `whenComplete(...)` registration returns `null` (also
when its fallback future view does so). The logical result is `UNKNOWN` and
the physical reservation remains `ZOMBIE` until an explicit release, so a
broken callback cannot leave an in-flight operation pending without a bounded
ownership state. `BoundedDestinationPublishAdapterTest.nullWhenCompleteReturnIsTreatedAsUnobservedCompletion`
proves the local fence; destination Broker completion/teardown attestation
remains an external release gate.

The Pulsar Journal resolver now preserves the same fail-closed evidence rule
after `RETIRED_NOT_PUBLISHED`: the marker cannot suppress the Broker
last-sequence check or the two retention predicates. A late sequence at or
above the retired mapping is a typed evidence divergence, and only a lower or
absent sequence with both proofs yields `NOT_PUBLISHED`.
`PulsarAttemptJournalTest.retiredMappingStillRequiresRetentionProofAndFencesLateBrokerPublication`
covers the regression; external sequence/fencing/retention authority remains a
release gate.

The UNKNOWN-outcome audit now closes a Store-integrity edge at the same
ownership boundary: before `PUBLISH_OUTCOME_V1(UNKNOWN)` can materialize an
uncertain retry or update READY, the current Message Lane, durable Lane record,
and exact `laneIncarnation` must match the admitted attempt ledger. A missing
or drifted Lane fails before the WriteBatch; the generic projection path cannot
silently create `LaneRecord.initial(...)` and advance the source cursor. The
focused regression is
`DelayShardTest.unknownOutcomeFailsClosedWithoutRecreatingAMissingLaneProjection`.
This is local crash/corruption evidence only; Owner lease, source replay and
external evidence authority remain release gates.

The same audit boundary now covers existing-message `RESCHEDULE` and `CANCEL`,
and large-payload `COMMIT`: each command requires a durable, identity-matching
Lane before any generic quota/READY projection is built. A missing Lane fails
before the WriteBatch, leaving the Message or Reservation and source position
unchanged; `LaneRecord.initial(...)` is reserved for first Schedule/Prepare
creation. The focused regressions are
`DelayShardTest.rescheduleFailsClosedWithoutRecreatingAMissingLaneProjection`,
`DelayShardTest.reservedPayloadCancelFailsClosedWithoutRecreatingAMissingLaneProjection`
and `DelayShardTest.largePayloadCommitFailsClosedWithoutRecreatingAMissingLaneProjection`.
This is local Store-integrity evidence and does not close the external ownership,
replay or production authority gates.

The audit now applies the same fence to existing publish obligations: definitive
or retry `PUBLISH_OUTCOME`, `EVIDENCE_RESOLUTION`, `RESOLVE_UNCERTAIN`,
`CLAIM_RESULT` and `EXPIRE_GENERATION` require the durable Lane before any
stale-result, quota/READY projection or source cursor can be persisted.
Canonical V1 ledgers additionally bind the exact Lane incarnation; missing Lane
state fails closed instead of being treated as a business retry/expiry result.
`DelayShardTest.notPublishedOutcomeFailsClosedWithoutRecreatingAMissingLaneProjection`
is the focused representative regression, alongside the existing UNKNOWN and
message/reservation command tests. External ownership and Broker/evidence
authority remain release gates.

The self-routing identity audit now closes the logical locator shape: the
fixed-width bytes between the Route/partition prefix and CRC must carry UUID
version 7 and RFC variant `10`. `SelfRoutingId.decode` rejects a CRC-valid
non-v7/non-RFC-variant locator before it can become a `CommandId` or
`DelayMessageId`, with `ProtocolCodecTest`
`selfRoutingIdRejectsNonUuidV7LogicalLocatorsEvenWithValidCrc` covering both
negative vectors. Timestamp age/future-skew remains a Route policy and is not
claimed by this byte-level evidence.

The local Message identity-reclamation projection now has a closed branch
instead of treating a reclaimed `id_cf/MESSAGE` key as absent: the same
valueType-1 envelope carries payload version 5
`RETIRED_IDENTITY(messageIdentityReuseUntil, retirementMutationSequence,
appliedSourcePosition)`. `retireMessageIdentity` atomically removes bounded
terminal generations and local DLQ export records, while every Message-range
rebuild/close/activation scan validates and skips the branch. The query seam
returns `IDENTITY_RETIRED`, and a same-ID first Schedule remains
`DELAY_MESSAGE_ID_CONFLICT` while the tombstone exists. The focused evidence
is `RetiredMessageIdentityRecordTest` plus
`EmbeddedDelayServiceTest.retiredMessageIdentitySurvivesQueryAndFreshProcessReopen`.
The positive delete/reuse boundary is also covered by
`DelayShardTest.retiredMessageIdentityCompactsOnlyAfterFenceAndFloorThenExpiresOldId`.
`compactRetiredMessageIdentity` is deliberately conservative: it requires a
closed ingress deadline and a Recovery Catalog Floor coverage proof, and
returns a non-compacting decision when either authority is unavailable. This
is local Store evidence only; Route freshness/retention policy, Oxia CAS,
provider quiescence and production GC remain release blockers.

After `a0d97cc`, the four physical local GC mutation methods are compiler-hidden
from cross-package production composition: `retireMessageIdentity`,
`compactRetiredMessageIdentity`, `compactResourceDeleteConfirmation` and
`retireLaneWithTerminalGuard` are package-local. A reflection regression locks all
four names to non-public visibility, and the one embedded cross-package fixture uses
a test-only bridge absent from the main artifact. Complete `DelayShardTest` and
`EmbeddedDelayServiceTest` suites plus compilation and Checkstyle passed. This
removes an API-authority mismatch only; Route retention, Oxia, provider quiescence,
grant release and Recovery-Floor coordinators are still release gates.

After `ce21a99`, the same compiler boundary includes
`DelayShard.rebuildReadyIndexes()`. This recovery-only algorithm scans Lane and
Timeline projections and rewrites all READY keys, so exposing it without a fenced
Owner/Oxia coordinator would create a direct mutation bypass. It is now
runtime-package-only and covered by the existing non-public reflection regression;
the full `DelayShardTest`, compilation and Checkstyle passed. This visibility change
does not implement the still-open production repair coordinator or dynamic I/O
admission.

After `ae1224f`, the raw mutation boundary is narrower again. The overloads that
revoke one Claim by ID, materialize one reservation by ID, or advance Lane-close by
Lane ID are package-local. Production reservation/Lane-close composition must retain
the exact discovered candidate and pass through the strict Owner/work-class wrapper;
it cannot replace that identity with an unguarded ID after queue wait. Drain and
recovery may still use the package-local Claim primitive internally. Signature-level
reflection checks plus complete `DelayShardTest`, Lane-close/Reservation strict
executor suites, compilation and Checkstyle passed. Exact-candidate public primitive
visibility remains a separate audit item.

After `03b6031`, the compiler boundary now covers eleven more local writes that had
no main-source caller: Lane gate mutation, four control/system-writer reserve
operations, three Attempt Journal updates, and direct admission/unknown/published
Publish state transitions. They remain available only to source-ordered/runtime
algorithms and tests. The reflection regression enumerates every exact signature;
complete `DelayShardTest`, compilation and Checkstyle passed. This prevents a
cross-package Worker from treating local Store integrity checks as Control, Broker,
adapter or capacity authority. The corresponding production coordinators and
dynamic reserve attribution remain release gates.

After `581faba`, `DelayShard.updateLaneReadiness` is no longer a cross-package
mutation surface. The repository has the durable projection algorithm but no
production Lane activator that proves pinned Profile/capability/credential state,
fences the Lane channel, crosses the evidence barrier and rereads Owner authority.
The method is therefore runtime-package-only; a test-classpath bridge supports
ownership fixtures without entering the main artifact. The shared reflection
regression and affected Owner/Claim/Admission suites, complete `DelayShardTest`,
compilation and Checkstyle passed. Production readiness authority remains OPEN.

The command/runtime projection audit also closes a `RESCHEDULE` drift: the
apply path and its persistence normalization now use the prior generation's
same pinned `actionAt` (or re-derive the pinned Profile handoff boundary),
construct the new generation's exact timeline key, and retain the matching
`TimelineWorkRef`. A same-`deliverAt` Reschedule can no longer fall through a
legacy constructor and become `actionAt=deliverAt`, which would disagree with
the previously certified handoff. The focused regression is
`DelayShardTest.reschedulePreservesPinnedActionAtInPersistedRuntimeProjection`;
this is local replay/projection evidence and does not add external Profile or
Broker authority.

The retry projection closes the corresponding `PUBLISHING`/`UNCERTAIN` seam.
Those branches have no current timeline reference, so a retry rebuild recovers
`actionAt` from the canonical `PublishAdmission` descriptor retained by the
bounded open-attempt ledger; opaque legacy ledgers remain compatibility-only.
A conflicting or timing-mismatched canonical Admission is rejected rather than
guessed. The focused regression is
`DelayShardTest.uncertainRetryPreservesPinnedActionAtWithoutProfileCatalog`,
which verifies the early-action value through uncertain retry and fresh-process
reopen. This is local state-projection evidence, not production
Profile/Admission or Broker handoff authority.

The Claim rollback audit now covers every local path that reconstructs a
timeline after `CLAIMED`: explicit revoke, source-ordered Lane Pause/Close and
capacity-gated Admission all restore the canonical `sourceTimelineWork`
retained by the Claim. This keeps an early `actionAt` and retry eligibility
stable without depending on a process-local resolver/catalog, while source
work kind, encoded key and retry gate are checked before persistence. The
shared action resolver consults that live Claim before opaque open-attempt or
Profile fallback, covering evidence settlement that converts a claimed
uncertain retry into a definitive retry. The focused regression is
`DelayShardTest.sourceOrderedLanePauseRestoresClaimPinnedActionAtWithoutResolver`;
legacy Claims without the optional source projection remain compatibility-only.
This is local rollback/projection evidence, not external Owner or control
authority.

The Evidence Cursor audit now treats the Pulsar Attempt Journal's
`physicalTopic` and `physicalTopicCreationTimestamp` as part of the full
stream identity. `EvidenceCursorV1.sameIdentity` and `dominates` reject a
replacement topic or creation identity even when the resource token and
generation are reused, so local Recovery Floor/parent-cursor coverage cannot
splice two physical Journals. `EvidenceCursorV1Test`
`pulsarCursorIdentityIncludesPhysicalTopicCreationIdentity` is the focused
regression; Broker resource-token issuance and retention proof remain external
gates.

The adapter outcome audit also closes the malformed-Stage return path: a
transport `CompletionStage` whose `handle(...)` returns null is treated as an
unobserved completion, not leaked as a null client result. Managed ingress and
submission wrappers converge to their existing uncertain branches; pinned
destination adapters preserve the unobserved marker so physical admission
retains the zombie/in-flight charge. The focused regressions are
`AdapterIngressTest.kafkaNullHandledStageIsUncertain`,
`AdapterIngressTest.pulsarNullHandledStageIsUncertain`,
`NativeSubmissionAdapterTest.preparedSubmissionWrapperNullHandledStageRemainsManagedUncertain`
and `BoundedDestinationPublishAdapterTest.pinnedAdapterNullHandledStageRetainsPhysicalCharge`.
This remains local transport-SPI evidence, not Broker-side completion or
non-persistence proof.

The public query-error audit also rejects a malformed optional `retryAt` field
before decoding its value: only the Registry varint wire type reaches the
stable-code projection, while length-delimited and fixed-width variants fail
closed with `IllegalArgumentException`. The focused regression is
`ProtocolCodecTest.queryErrorResponsesKeepClosedResultTagsAndRetryPresence`;
this remains local codec evidence and does not prove gateway or external query
authority.

The lane-control audit now also routes ACK presence and close order-break reads
through `AcknowledgementSetV1` and the typed varint helper. Direct inspection
cannot bypass canonical field order, known ACK kinds, or wire-type validation;
`PayloadProofControlPayloadV1Test.laneAcknowledgementQueryRejectsMalformedAckWireType`
covers the malformed direct-query path. This remains local projection evidence;
source-ordered authenticated control authority is still external.

The local codec audit additionally verified that implemented V1 key-version
fields honor the Registry's full unsigned `uint32` domain rather than silently
narrowing to positive Java `int`: signed System Mutation, Native Capability,
credential attestation, Evidence Verifier, Payload Commit and payload-proof
trust-set/control paths preserve raw high-bit values, compare trust-set keys
unsigned, and use the same bits in signature/proof preimages. Focused protocol
tests cover the high-bit round trips. This is byte-level evidence only; the
activated trust set, key rotation and authenticated writer authority remain
release gates.

The local SLO codec audit also preserves the Registry's complete unsigned
`uint64` domain for objective threshold/ratio/window/envelope fields and Final
measurement/revision fields. Canonical decode retains raw high-bit values;
interval validation, evidence revision selection and `AT_MOST`/`AT_LEAST`
conservative merges compare them unsigned. Focused SLO tests cover round-trip
and merge vectors; the due-admission identity also preserves its raw `uint32`
generation bits. The due-admission identity now rejects a negative
`path_start_epoch_ms`, exposes typed path/start fields, and `SloSampleStartV1`
requires exact identity/path and semantic fixed-epoch agreement; the focused
SLO tests cover path/time drift and the negative-time vector. This remains
shard-local protocol evidence, not proof of production catalog publication, but
the local pair validator now requires both the exact Start-bound ALL_ACCEPTED
objective and its matching HEALTHY due companion before an exclusion can be
merged. It checks the full companion policy rather than only membership of the
exclusion reason; legacy pair-less overloads fail closed for excluded Finals.
A different payload cannot regress observation revision. Start reconstruction,
production catalog/collector merge-export, and production SLO evidence remain
release gates.

The ownership/recovery audit now covers the local reversible-Claim activation
boundary: before either embedded or authoritative `OwnedDelayShard` activation
opens the command gate, `DelayShard.requeueClaimsForRecovery()` scans the full
bounded `inflight_cf/CLAIMED` namespace and restores each Claim through one
atomic timeline/Message/READY/quota WriteBatch. The restored timeline keeps
the semantic work digest while using a new checked runtime instance; malformed
or over-bound Claim state fails closed, and `PUBLISHING`/`UNCERTAIN` ledgers are
not rewritten by this helper. This closes only the local Claim recovery seam;
Source Log successor proof, Oxia lease/session CAS, adapter materialization and
external Producer authority remain release evidence.

The shared `uint32` decode boundary also rejects a varint outside the unsigned
32-bit domain instead of narrowing a high-bit `uint64` into a `uint32`; the
queued receipt path rejects high-bit signed timing values while retaining raw
Pulsar physical-topic creation identity. The closed Control and Scheduler
projection validators apply the same fence to local `uint32` and boolean
fields. `ProtocolCodecTest` covers these malformed-input fences.

Public Command/Message query responses, public result/error views, terminal
views, applied receipts and the Client Command envelope now use the same
bounded runtime projection for enum/status/command tags. A wire `uint32` above
the local signed range is rejected as `IllegalArgumentException` before enum
dispatch, rather than leaking an arithmetic narrowing exception or producing a
negative runtime tag. `ProtocolCodecTest.publicClosedUnionTagsRejectHighBitUint32AsInvalidInput`
covers the public query union boundary; this remains a local codec fence and
does not narrow the Registry wire domain.

The canonical Protobuf length-delimited reader rejects high-bit `uint64`
length prefixes and lengths above the bounded Java `int` payload limit before
the value is narrowed for slicing. This closes the local malformed-input path
where a `2^63`-class length could otherwise become zero after a cast;
`ProtocolCodecTest.canonicalReaderRejectsHighBitLengthPrefixes` is the focused
regression evidence. It is parser-safety evidence only and does not claim an
external transport or object-store size limit.

The five `meta/SCHEDULER` projection codecs likewise retain complete raw
`uint64` generation/version/deficit bits and keep only the bounded `next_index`
`uint32` in local range. The focused projection vector covers discovery cursor,
ring, deficit map, round and last-served map; runtime capacity/placement and
Oxia scheduler authority remain separate release evidence.

The Registry-shaped `ActiveLaneStateV1` projection also preserves raw unsigned
`uint64` lane-control/lane-version/scheduler-weight/failure fields and keeps
the separate epoch fields as nonnegative `int64`. `DelayShard` now recognizes
that direct typed ACTIVE branch during reopen, rebuild and same-key projection
updates, maps only the bounded compatibility fields to its local `LaneRecord`,
and preserves immutable Profile/tuple/certificate/retirement data plus the
projected per-Lane quota usage. A malformed typed value, missing BLOCKED
reason/READY key/certificate or out-of-range compatibility field fails closed;
there is no silent downgrade to the legacy adapter. This is still local typed
runtime evidence; complete Profile/Lane activation, quota authority and Oxia
ownership remain release evidence.

The local `LaneRecord.withGate` transition helper now matches the frozen
irreversibility graph: `ORDERING_BROKEN` can only move to `CLOSED`, and a
`CLOSED` Lane cannot be reopened through the generic gate API. The focused
`LaneRecordTest.controlTransitionsAreExplicitAndIrreversibleWhereRequired`
regression covers the former reopen paths. This is a local lifecycle fence;
source-ordered control authorization and external terminal-guard retirement
remain release evidence.
Before activation, every typed ACTIVE Lane now also has to carry field-14
`lane_usage` byte-equal to the matching `(laneId, laneIncarnation)` entry in the
persisted class-3 `meta_cf/QUOTA` map. Missing-map and usage-drift cases fail
closed, and the focused `DelayShardTest` regressions cover both branches; this
closes the local state/map projection fence without claiming typed runtime cutover
or external revision authority.
Activation also recomputes typed `encoded_ready_key` from the Lane ID,
`laneVersion` and `nextEligibleAt`, rejecting a key whose fields or gate/readiness
branch drift. `DelayShardTest.typedActiveLaneStateRejectsReadyKeyDriftBeforeActivation`
covers that local identity fence; it does not prove physical READY existence,
certificate authority or scheduler recovery.

The nested `ChargeVectorV1` now preserves the complete raw `uint64` domain at
the wire boundary. Its embedded signed-capacity projection is guarded
explicitly, so a high-bit charge cannot enter local reserve/vector arithmetic
as a negative value; it is capacity-gated instead. This is local codec and
fail-closed admission evidence, not an external grant-authority proof.

Profile and Retry Policy semantic versions and their reference values likewise
retain complete nonzero `uint64` bits in canonical bytes and semantic-hash
preimages. This closes their local immutable-value codec boundary; catalog
publication, activation history and source authority remain release evidence.
`PublishOutcomeBody` and `DlqExportResultBody` now share the strict
`RetryPolicyRefV1` decoder, so the high-bit version boundary is preserved on
both outcome paths; `PublishOutcomeBodyTest` covers the cross-path regression.
The Admission target-partition hash now uses the same raw `uint64` Profile
version bits (`PublishAdmissionBody.requireHashedPartition`), so a high-bit
Destination Profile reference is not rejected while recomputing the Registry
`TARGET_PARTITION_HASH_V1` digest. `PublishAdmissionBodyTest` covers the
high-bit hash vector; target catalog/publication and external partition
authority remain release evidence. Admission descriptor/materialization Profile
references now also reuse `ProfileRefV1.decode`, so malformed semantic-hash
lengths cannot pass the projection validator; the negative vector is covered by
`PublishAdmissionBodyTest`.
The replay-stable Claim Result materialization uses the same decoder, so its
high-bit Destination/Delivery-Capability Profile versions are not rejected by
the Claim path; `ClaimResultBodyTest` covers that boundary.
Its nested Broker resource, committed Object Store descriptor and adapter
metadata now reuse the typed Registry codecs as well, so NFC/UTF-8, branch,
Profile-kind and fixed identity semantics cannot be weakened by the Claim
projection's local parser. Negative vectors for a non-canonical Broker identity
and a committed descriptor with the wrong Profile kind are in
`ClaimResultBodyTest`.

The runtime Claim seam now has a strict `DelayShard.claimForPublishV1` entrypoint.
Before Claim persistence it compares the typed materialization with the current
Message's identity, generation, delivery window, timeline `actionAt` and
inline/object payload reference. When the Message has a durable
`V1ScheduleBinding`, it additionally re-decodes the accepted Schedule/Prepare
body and requires exact Destination Profile, business metadata and delivery
window equality. Ordinary Schedule requires the complete original payload
branch/descriptor; Prepare→Commit requires the complete pinned Object Store
ProfileRef plus expected length and SHA-256. Therefore a foreign Profile
identity cannot pass merely by reusing the same semantic hash.
The same binding's exact canonical Lane tuple is parsed before Claim persistence
and must project byte-identical Destination/Capability Profile refs,
Kafka/Pulsar Broker target resource and physical partition. Kafka's duplicated
native-topic UUID and physical-topic identity must also agree. This is an
immutable Schedule-time identity check, not evidence that the current Profile,
credential lease or Broker resource remains live.
`ClaimMaterializationRuntimeTest` covers both payload branches and ordinary
Schedule Profile substitution; the large-payload regression in
`DelayShardTest.registryPrepareCannotDowngradeTrustSetAuthorityWithLegacyCommitBody`
covers the post-Commit Prepare binding. The raw byte-array primitive is now
package-local; `DelayShardTest.physicalGcMutationPrimitivesAreNotPublicProductionApis`
locks that visibility, and the only cross-package compatibility access lives in
test sources for recovery-fixture construction. This is a local durable-command
binding and API-surface proof only: live Profile/credential/resource authority,
Object Store fetch/immutability, Adapter serialization/size/channel lease
certification and Producer ownership/recovery remain release gates.

`PublishAdmissionBody` now parses its descriptor through the complete canonical
`PreparedPublishDescriptorV1`; `Descriptor.value()` returns that same typed
projection, including reserved metadata and prepared-publish hash. The Admission
tests cover byte round-trip, fail-closed reserved-shard relabeling and reserved
Profile-hash drift. This removes the local raw-descriptor parser/accessor gap
without claiming external channel or Producer authority.

Both Claim and Publish Admission materialization parsers also enforce the
Registry slot kinds: field 1 is `DESTINATION` and field 2 is
`DELIVERY_CAPABILITY`. This prevents a validly encoded but semantically wrong
Profile reference from crossing the replay-stable Claim/Admission boundary;
the negative vectors are in `ClaimResultBodyTest` and `PublishAdmissionBodyTest`.

The replay-stable nested values now have one shared local implementation:
`PayloadForPublishV1` owns the inline/object union and length/SHA-256 equality,
and `ClaimMaterializationV1` owns the complete 11-field canonical projection,
uint32 partition/generation range, target-resource/metadata branch agreement,
nonnegative timing constraints and domain-separated digest. `ClaimResultBody`,
`PublishAdmissionBody` and the Prepared Publish Descriptor all decode through
these types, while `ClaimRecord` exposes the typed projection only after its
durable Claim precondition has passed. This is stronger local codec evidence,
not proof of Profile/catalog, Object Store, Adapter or Producer authority.
The canonical typed descriptor and its compatibility `PublishAdmissionBody.Descriptor`
now retain the complete raw uint32 generation/attempt bit patterns. The local
Message, Claim, ledger, timeline/terminal key, Adapter request, retry-jitter and
public Message/Command view projections use the same raw-bit representation;
generation ordering is explicitly unsigned and checked successors fence the
all-ones value instead of wrapping. Focused coverage includes
`PublishAdmissionBodyTest.preservesHighBitUint32GenerationAndAttemptBits`,
`PublishAttemptLedgerTest.v1LedgerPreservesHighBitGenerationAndAttemptBits`,
`MessageRecordTest.scalarMessageRecordPreservesHighBitGenerationBits` and
`KeyCodecTest.timelineAndTerminalKeysPreserveUnsignedGenerationBits`, with
`GenerationRuntimeIndexTest.attemptObligationPreservesUnsignedGenerationBits`
and the ordering/overflow helper covered by `UnsignedInt32Test`. The legacy
`CommandResult` rejected-result absence sentinel remains a separately
documented compatibility boundary; applied results use the presence of
`stateVersion/messageStatus` to distinguish a real all-ones generation from
that absence sentinel (`DurableResultTest.appliedCommandResultPreservesMaxUint32GenerationAndProjectsIt`).
The same raw-bit rule now covers `RetryDecisionV1.completed_attempt_no` and
the DLQ physical attempt in both outcome codecs: values above `0xffffffff` are
rejected, high-bit values round-trip, and DLQ PENDING/UNCERTAIN transitions
use an unsigned checked successor/comparison rather than signed arithmetic
(`PublishOutcomeBodyTest`, `DlqExportResultBodyTest`, `DlqExportRecordTest`,
`DelayShardTest`).

The `ExactResourceIdentityV1` retirement projection now applies the same
branch-specific Object Store Profile fence as the committed/checkpoint
resource codecs. Payload-object and checkpoint lengths follow the Registry's
`uint64` non-negative domain, so a valid zero-byte object or manifest is not
rejected before the GC intent can be persisted. `ResourceRetireIntentBodyTest`
covers both zero-length branches and rejects a payload identity carrying a
Destination Profile.
The seven Registry retirement branches are now exposed as typed protocol values:
`PayloadObjectResourceV1`, `CheckpointResourceV1`, `DlqExportResourceV1`,
`KafkaReceiptSlotResourceV1`, `PulsarJournalGenerationResourceV1`,
`LaneChannelResourceV1` and `LocalStoreResourceV1`. Each value owns its direct
canonical codec and exact `ResourceKind` wrapper; `ExternalResourceBranchTest`
covers the newly added payload/DLQ/lane/local-store values as well as raw
unsigned Kafka/Pulsar fields. `KafkaReceiptResource` and
`PulsarJournalResource` project their physical identities into the same typed
Registry branches, and the Journal resource preserves high-bit physical
partition values as raw `uint32` rather than applying a Java signed-range
check. This closes only the local resource-value boundary, not slot
allocation, external retirement, Broker evidence or Oxia ownership.

Payload-proof trust-set semantic/reference versions now also preserve complete
raw bits, while the source-ordered activation projection compares versions as
unsigned values and rejects regression. This is local control-state evidence;
trust-set publication and authenticated source authority remain external.
The legacy and typed payload commit proof projections now preserve the same
nonzero raw trust-set version bits through proof IDs, signatures and canonical
decode, closing the local semantic-ref-to-attestation mismatch. Provider proof
issuance and source-ordered trust authority remain release evidence.
The compatibility `LargeScheduleIntent` reserve projection preserves the same
raw version through its fixed binary encode/decode path as well.

`ApplyShardControlBody.semantic_version` now follows the same immutable
semantic-version boundary as a raw nonzero `uint64` through the System
Mutation body validator and typed parser. The optional
`expected_prior_control_version` and Lane target control versions remain
bounded local CAS values.

CapacityGrant source versions, QuotaGrant reference versions and
ShardCapacityEnvelope versions likewise preserve their complete nonzero
`uint64` bit patterns through canonical bytes, nested decode and quota
semantic-hash preimages. The separate `CapacityVectorV1` amount projection
remains a checked signed local capacity envelope; this audit closes only the
independent version-identity codec boundary.

The per-Lane quota usage entry now retains the complete nonzero `uint64`
usage-revision bit pattern in its digest and canonical bytes as well. This
closes the map-entry revision codec boundary only; same-batch quota coupling
and external quota authority remain separate release evidence.

Checkpoint summary, catalog and control-result projections now retain complete
nonzero `uint64 catalog_generation` bits and compare repeated summaries with
unsigned generation ordering. This is public checkpoint projection evidence;
Recovery Floor/upload authority and local generation-increment rules remain
separate release evidence.

The checkpoint upload-intent codec likewise retains complete nonzero
`base_catalog_generation` and checked `state_revision` bits, and the local
intent successor advances the latter across the signed Java high-bit boundary
until the all-ones pattern. Provider/Oxia publication and CAS authority remain
external release evidence.

Typed Recovery Floor and session-bound Pin references now also retain the
complete nonzero `catalog_generation` bits through their digests and exact
cross-object binding. The local catalog successor and the Oxia response adapter
now compare catalog generations as unsigned values and reject only zero or the
all-ones successor exhaustion; Recovery catalog freshness, ancestry and session
CAS remain external authority evidence. This closes the local arithmetic and
response boundary as well as the canonical reference codec boundary.

`QuotaTransferPlanRefV1` likewise retains the complete nonzero tenant-policy
version `uint64` bit pattern in the canonical control request value. This is
wire-value evidence only; policy authorization and source-ordered transfer
authority remain external.

Lane retirement progress and terminal-guard projections likewise retain the
complete nonzero raw `uint64` mutation-sequence pattern in their digests and
canonical bytes. These values are identity/fencing projections only; the
runtime source-mutation and lane-control counters remain bounded local
successors, and terminal-guard/Oxia retirement authority remains release
evidence.

尚未填写的数值不是开放设计问题：它们必须由 §23 的 benchmark、capacity proof、real-service conformance 和 chaos evidence 产生，并装入已经冻结的 required config/schema。任何实现若要改变字段、状态、时序、不变量或停止条件，必须发布新的 spec/protocol revision，不能把 benchmark 输出当作协议修订。

## 权威材料

| 材料 | 责任边界 |
|---|---|
| [`Nereus Delay V1 设计.md`](<Nereus Delay V1 设计.md>) | V1 行为、状态、时序、配置和验收语义 |
| [`V1-PROTOCOL-REGISTRY.md`](V1-PROTOCOL-REGISTRY.md) | 唯一 numeric/byte registry：frame、field、enum、hash、code、key、manifest 和 golden vector |
| [`CONTEXT.md`](../CONTEXT.md) | 统一术语和禁止混用的近义概念 |
| [`adr/README.md`](adr/README.md) | 42 个 Accepted 决策及其治理索引 |
| 本文件 | 交叉审计结果和 release-evidence checklist；非新 authority |

若前三类规范材料互相冲突，release gate 失败；实现不得自行选一个解释。

## 契约覆盖

| V1 契约 | 已冻结的证据入口 |
|---|---|
| `deliverAt` 是消费者最早可见时间 | 主设计 §2/§3/§12.4/§13.7；ADR 0001、0021 |
| 默认 managed；`AUTO_FAST` 显式 opt-in 且先 prepare 后 I/O | 主设计 §3/§6；ADR 0002、0031 |
| queued、applied 和 destination outcome 是不同事实 | 主设计 §6/§8；ADR 0005、0006 |
| Command identity 在 I/O 前固定；Source Position 是唯一 shard order | 主设计 §7/§8；ADR 0006、0007、0026、0034 |
| 一个 Delay Shard 对应一个 DB、checkpoint、restore 和 migration 单元 | 主设计 §5/§9/§10/§16；ADR 0004、0025、0027 |
| `ownerEpoch` 只 fence Nereus 本地 authority | 主设计 §9/§11；ADR 0003、0017 |
| 坏 Destination Lane 不暂停 Command apply、也不无限饿死健康 Lane | 主设计 §4/§12/§18/§20/§23；ADR 0008、0032、0035、0036 |
| Destination/Profile/partition/order domain 都被 immutable binding pin 住 | 主设计 §5/§12/§13；ADR 0009、0014、0016、0038 |
| Publish Admission 是 Producer call 前的 durable point of no return | 主设计 §11；ADR 0013、0039 |
| remote accepted/unknown 不冒充 failure；多 attempt obligation 可闭合定位 | 主设计 §11/§15；Registry 的 `GenerationRuntimeIndexV1`、`AttemptObligationRefV1`；ADR 0022 |
| 大 payload 使用 reserve/upload/attest/commit | 主设计 §14；ADR 0010、0042 |
| Recovery Set/Floor、lineage、pin 和 upload intent 共同约束恢复与 GC | 主设计 §15.5/§16；ADR 0011、0027、0040 |
| quota、Control Reserve、Worker physical envelope 和 placement 不超卖 | 主设计 §18/§21；ADR 0019、0028 |
| Query、Control Operation、RBAC 和 public-safe projection 是 closed union | 主设计 §17/§19；Registry §6.3；ADR 0018、0020、0029 |
| 每个 SLO sample 可持久恢复且 conservative merge | 主设计 §20；ADR 0041 |

## Authority 与线性化点

Registry credential-control-plane canonical values are locally closed by
`CredentialEquivalenceAttestationV1`, `CredentialBindingV1`,
`CredentialBindingHeadV1` and `CredentialBindingProtectionV1`. Their digests,
candidate tuple agreement, Trusted-UTC interval ordering and Ed25519 signature
projection are verified before a value is accepted. This is not an Oxia
linearization receipt: activated trust-set membership, provider-side immutable
version resolution, configured proof-age bounds, Head/protection monotonic
CAS and durable reread remain external authority gates. `CredentialUseLeaseV1`
locally requires the matching Profile kind, binding/fingerprint tuple and
kind-specific protection-until bound; it does not reread Oxia for each provider
call.

The Profile publication/deprecation/equivalent-rotation request values are
also canonical and fail closed on envelope/binding identity, generation
successor, private-reference digest, attestation candidate and expected Head
revision. They remain request-value codecs, not authenticated actor/target
authorization or source-ordered Oxia mutation receipts.

`ProfileCatalog`/`InMemoryProfileCatalog` now provide a strict local lookup
projection for exact Profile semantic bytes, generation-1 and rotated private
bindings, Head/protection records and deprecation intent. The projection is
useful for recovery and tests, but it is not the Profile publication authority,
does not create per-shard activation markers, and does not replace authenticated
Oxia CAS or retained-generation policy.
`ProfileCatalogV1ScheduleResolver` uses that seam only as a local exact-profile
and Head prerequisite gate, and now exact-resolves the referenced Delivery
Capability semantic with the same Adapter before delegating Lane resolution;
missing/wrong-kind/mismatched capability is fail-closed. `DelayShard` applies
this decorator automatically when a raw resolver and exact Profile catalog are
provided. An already decorated resolver is preserved only when it is paired
with the exact same catalog instance; missing/foreign catalog injection and a
nested second Profile decorator are rejected before Store projection reads.
This prevents Schedule actionAt and later Admission/recovery timing validation
from observing independently mutable Profile semantic sources. It does not turn
a catalog lookup into a source position activation receipt.
When a persisted V1 binding is revisited for Commit, Reschedule or recovery,
the same exact Profile/Capability lookup fence protects `actionAt`; a missing
or mismatched catalog entry is `ROUTE_SNAPSHOT_UNAVAILABLE`, never an ordinary
`deliverAt` fallback. `DelayShardTest.catalogBackedActionAtDerivationFailsClosedWhenPinnedProfileDisappears`
covers the local boundary.
The Control Operation request union now has local canonical codecs for all
fifteen Registry branches, including the evidence/acknowledgement matrix. It
remains a request-value boundary and does not authenticate an actor/resource,
construct source mutations or produce an Oxia registration receipt.
The prepared envelope also retains the complete raw `control_query_policy_version`
bits in its digest and signature preimage; this immutable policy reference is
distinct from the bounded local operation-revision CAS counter.
Credential binding Head/protection revisions now follow the same complete raw
`uint64` rule across Head, protection, lease, rotation request/target and public
result projections. Zero remains invalid, and the local catalog stops its
unsigned checked successor at the all-ones pattern; this is codec/CAS-value
evidence only, not an Oxia transaction or provider-authority proof.
The Control target value layer closes the six branch shapes and field-22
digest locally. `PreparedControlOperationV1` now additionally enforces the
closed operation-specific target counts/kinds, mutation-identity presence and
Profile/Quota request-to-target identity rules before hashing/signing; source
mutation construction and authenticated target authority remain outside this
local codec boundary. `ControlTargetMutationBindingV1` then validates a
completed source mutation's ControlRef, logical identity, computed ID/hash,
target Shard/Message and the covered Replay/Resolve/Lane body fields before
external registration; it does not replace Oxia or writer authentication.
`ControlOperationAuthorizationV1` additionally checks the authenticator's
actor/role/scope hash projection and the minimum V1 role matrix before that
step; its explicit scope proof remains an external authorization input.
`ControlRegistrationBindingV1` binds all three local registration outcome
branches back to the exact Prepared operation and rejects receipt/request,
scope, target-snapshot or revision drift; transport classification and Oxia
persistence proof remain external.
The local `InMemoryControlTargetRegistrationAuthority` is only an idempotent
exact-byte target-registration model; it does not make the Oxia CAS/transaction
or production target lookup available.
Its bytes are not an Oxia registration receipt and do not establish actor/role
authority.
The local operation authority now also applies the closed monotonic operation
and target-marker transition graph before its in-memory revision CAS; this is
projection validation only and does not provide durable Oxia operation state.
`OxiaControlTargetRegistrationAuthority` applies the same exact Prepared-byte
and operation-ID checks around its injected backend; it is a validation
adapter, not a real Oxia client or transport classifier. Lookup buffers are
copied into separate backend and validation snapshots, so a backend cannot
rewrite the operation identity used by the response check.
`OxiaSyncControlTargetRegistrationBackend` 现在把每个 operation ID 的 exact
Prepared bytes 放进一个 canonical Oxia record，以 `IfRecordDoesNotExist` CAS
完成 immutable registration；response loss 只有 exact reread 成功，record
corruption、same-ID/different-bytes 和 operation-ID drift 均 fail closed。
`OxiaSyncControlTargetRegistrationBackendTest` 覆盖真实 Oxia Java client
record surface 的 deterministic seam、reopen、冲突和损坏边界。这只闭合
per-operation registration record，不等于 actor/target authorization、target
existence、source-ordered mutation transaction 或 production transport。
`OxiaRealControlAuthoritySmokeTest` 同时在真实服务上验证了该 immutable
registration 的首次写入、精确重开读取；它仍不构成 target existence 或
authenticated mutation authority 证据。
`ControlSystemMutationFactoryV1` now centralizes the signed envelope and
logical-identity derivation, while operation-specific body encoding and
service-key trust remain outside this local seam.
The initial operation projection now covers every target and preserves the
Registry uint32 target index; this is a local codec/projection guarantee, not
an Oxia registration or target-existence proof.
`ControlRegistrationProjectionV1` keeps the receipt and that initial CURRENT
projection together, but still does not claim the Oxia one-transaction
registration boundary.
The query-retention helper derives the boundary from trusted registration
evidence with checked addition; policy publication and Oxia persistence remain
external.
The embedded service now runs the local target-registration plus operation-CAS
path end to end. Its configured `DelayShard` also consumes the same local
registration authority: `APPLY_SHARD_CONTROL`, `REPLAY_DEAD_LETTER` and
`RESOLVE_UNCERTAIN` markers are rejected at their Source Position before any
handler effect unless the exact Prepared target and mutation identity are
registered; the matching exact-registration path is also covered by the
`DelayShardTest` positive/negative vectors. This proves the fail-closed local
boundary only; it remains a test model and does not provide gateway
authentication, target existence or a production Oxia transaction.
The registration outcome/proof union is likewise local evidence projection:
only authenticated Oxia response evidence can construct the conditional
rejection branch, while timeout/session ambiguity remains `RECORD_UNCERTAIN`.

| 事件 | 唯一 authority / 线性化点 | 明确不构成 authority 的事件 |
|---|---|---|
| Command 准备 | canonical Prepared Command bytes/hash 在首次 I/O 前完成 | Producer request、Broker position、wall clock |
| Command queued | ingress Broker durable receipt | SDK Future 创建、local buffer acceptance |
| Command applied/rejected | state/result/dedupe/Source Position 的 WAL-enabled RocksDB WriteBatch sync | queued receipt、source ACK、cache observation |
| source ACK/commit | 只能发生在对应 DB sync 后 | ahead-of-DB consumer offset |
| source-ordered control 生效 | signed System Mutation 在目标 shard 的 RocksDB apply | Oxia request、watch delivery、Control API return |
| Claim | local reversible Claim WriteBatch | destination ownership、delivery attempt |
| Publish Admission | exact Admission/ledger/ref/charge 的 RocksDB WriteBatch sync | Claim、Producer call、callback |
| destination outcome | Outcome/Evidence System Mutation 的 source-ordered apply | raw callback、timeout、Future cancellation |
| expiration | qualified `EXPIRE_GENERATION_V1` apply | raw timer、unqualified Worker clock |
| checkpoint 可恢复 | immutable manifest/files 已校验并成功进入 Oxia catalog | local checkpoint、partial upload、object listing |
| shard command-active | restore/replay 到 typed Activation Barrier 后的 guarded `ACTIVE_FOR_COMMANDS` CAS | DB open、Source Assignment、Owner Lease 单独存在 |
| Lane READY | exact Ready Certificate、channel/evidence/credential prerequisites 和 READY key 同步成立 | target 健康猜测、admin OPEN 单独存在 |
| GC/delete 完成 | source-ordered retire intent、保护集、external delete confirmation 和 Recovery Floor 全满足 | delete request、timeout、listing absence 单独存在 |

`EmbeddedDelayService` 的本地 ingress seam 现在在分配 Source Position 前同时
限制 pending command count 和 canonical frame bytes；缓冲区满时返回 Registry
的 `SDK_BACKPRESSURE_NOT_SUBMITTED` definitive local rejection，且不消耗嵌入式
source offset，drain 后释放精确 byte charge；`close()` 会先同步 drain，且
队头只有在 `DelayShard.apply` 返回后才释放。证据是
`EmbeddedDelayServiceTest.sdkBackpressureRejectsBeforeSourcePositionAndByteBudgetAreConsumed`。
以及 `EmbeddedDelayServiceTest.closeDrainsQueuedCommandsBeforeClosingTheShardDb`。
`DelayClient.enqueueBatch` / `EmbeddedDelayService.enqueueBatch` 现在逐条复用同一
本地 ingress admission，返回与输入顺序一致的独立 `EnqueueOutcome`；它明确不承诺
跨命令原子性，混合 queued 与 definitive local rejection 的结果仍可逐条处理。
`DelayClient.getCommandResult` 与 `getMessage` 也已把这两个 bounded local query
projection 暴露在客户端契约中；它们仍要求 queued receipt/source barrier 与调用方
提供的 binding/evidence/retention policy，不能被解释为跨 Worker routing 或 production
authorization-safe lookup。
严格 V1 的 Schedule/PrepareLarge/Cancel/Reschedule prepare 入口也已进入同一
`DelayClient` 契约；它们只构造 Registry-shaped canonical bytes，不做网络 I/O，
`EmbeddedDelayServiceTest.delayClientPreparesStrictV1CommandsWithoutIo` 覆盖其
body round-trip。legacy prepare 仍是兼容桥，不能伪装成 V1 managed submission。
对应的 `enqueueV1`/`enqueueBatchV1` 在 pending admission 前调用 strict V1 frame
validator；legacy body 得到 `INVALID_PREPARED_COMMAND` definitive branch，不消耗
Source Position 或 byte budget，且 batch 仍逐条保持输入顺序。
`DelayClient.awaitAppliedV1` 在同一边界上补齐等待语义：先验证 pinned embedded
receipt，再对 PENDING 结果 drain 并 reread；不合法 receipt 直接返回
`RECEIPT_MISMATCH`，不会推进 source。
`DelayClient.prepareManagedSubmissionV1` 现在把严格 V1 frame 包装为不可变的
managed `PreparedSubmissionV1`，其 `submit` bridge 在嵌入式路径先验证
nonzero physical attempt，再分配 Source Position 并投影为 managed
`SubmissionOutcomeMessageV1`。注入 `PreparedSubmissionAdapter` 时由该 adapter
保持 exact managed/native branch dispatch；未配置真实 native adapter 的 embedded
路径返回 typed `AUTO_FAST_PREREQUISITE_UNAVAILABLE`，不会把 native prepared
submission 悄悄改走 managed。`DelayClient.prepareAutoFast` 现在补齐零 I/O
selection seam：它消费调用方提供的 immutable Profile semantic envelopes、已签名
`NativeCapabilitySnapshotV1` 和 pinned issuer key，在签名/expiry/target/partition/
payload/metadata/timing/direct-authority 检查全部满足时生成新的 nonzero native
delivery identity，并把 shifted Broker timestamp 固定进 native prepared bytes；任何
native 不满足项都返回同一 strict managed frame；batch 逐项独立选择并保持输入顺序。
`AutoFastScheduleTest` 覆盖 eligible native、exact managed fallback、batch 和 no Source
Position admission。`HASH_ONLY` 及未命中显式集合的 `EXPLICIT_OR_HASH` 现在不再只信任
signed snapshot 的分区字段；`EmbeddedDelayService` 从 exact managed Command 的
adapter metadata/Delay Message ID 按共享 `TargetPartitionHashV1` 重算目标分区，
错配在 Producer 前稳定回退 managed。Snapshot issuer、
Oxia protection-before-rotation、Broker guard、Producer ownership 和 production
response evidence 仍是外部 release gate。Native adapter 若配置
`CredentialFingerprintProvider`，会在 Producer ownership 前解析当前凭据指纹并
与 signed capability snapshot 中的 immutable digest 做 constant-time 比较；漂移
返回 `CREDENTIAL_BINDING_DRIFT`，解析不可用、null、长度错误或抛异常返回
`AUTO_FAST_PREREQUISITE_UNAVAILABLE`，两者都不会调用 Pulsar transport。旧构造器
故意不配置该 provider，仅作为兼容性 seam，不能被解释为生产 credential authority
或 durable rotation protection。
同一 pre-ownership gate 对注入时钟也 fail closed：`clock.millis()` 抛异常或返回负的
epoch millisecond 时返回 `AUTO_FAST_PREREQUISITE_UNAVAILABLE`，不把本地时间源故障
解释成 expiry 或 Producer ownership；`NativeSubmissionAdapterTest` 覆盖这两个分支。
时钟认证和生产 Broker-time authority 仍是 release gate。
Native physical partition remains a raw `uint32` projection through AUTO_FAST
selection; `AutoFastSchedule` and `EmbeddedDelayService` compare it as unsigned
values, and `AutoFastScheduleTest` covers the high-bit `0x80000000` case. This
closes the local representation boundary only; the Broker transport and guard
authority remain pending external gates.
`PulsarAttemptJournal` 进一步把 ADR 0037 的本地可验证部分落成独立 seam：Producer
key 固定在一个 Shard，sequence 严格递增，mapping append 必须先拿到 Journal position，
精确重放幂等；replay 也会在首条和后续 mapping 安装前验证严格的 Producer sequence
successor，首条或后续跳号都以 `INTEGRITY_ERROR` 拒绝而不留下部分 state；对某个
Lane-scoped Producer，`evidenceCursor` 可将最新本地 Journal position 投影成带显式
Journal resource/creation/partition、batch cursor、generation 和最大本地 Broker 时间的
typed `EvidenceCursorV1`（旧构造器未提供 Journal identity 时仅保留 target-derived
compatibility seam）；`publishedEvidence` 进一步构造 Registry 的
`PULSAR_ATTEMPT_JOURNAL` PUBLISHED branch，绑定 exact Attempt owner、prepared hash、
producer-name hash、sequence、mapping-record hash 和可选 target-ack evidence。两者都
仍只是本地 canonical value projection，不是 contiguous Broker reader、retention、
authenticated Broker ACK/guard 或 publication 证明；
durable `RETIRED_NOT_PUBLISHED` 之后，`notPublishedEvidence` 也构造 Registry 的
`PULSAR_JOURNAL_ABSENCE` VERIFIED_NOT_PUBLISHED branch，严格绑定 fenced Pulsar
dedup channel 的 Lane/target/evidence-resource/partition/generation、显式
`PulsarJournalResource`、Attempt/prepared/
producer identity、sequence、typed cursor 和 retirement-barrier digest。该 channel 与
barrier 仍是调用方提供的 local seam 输入，不是已认证的 ExclusiveWithFencing、contiguous
reader 或 retention proof，因此不能单独打开生产 absence capability；
`appendOrReuse`/重载 `sendAfterMapped` 对同一 exact attempt
重试复用原 mapping/sequence，且在 mapping append 失败时不进入 target sender；
未 retirement 的 lower sequence 阻塞后续 Admission，
`RETIRED_NOT_PUBLISHED` 之后才允许下一个 sequence，`sendAfterMapped` 不接受未 durable
或已 retirement 的 mapping。Broker sequence 超过 Journal 最大值，或 lower sequence 缺少
inactivity-horizon 与 producer-snapshot 两项证明，统一返回
`PULSAR_EVIDENCE_DIVERGENCE`；`PulsarAttemptJournalTest` 覆盖这些分支。该类的 injected
appender 只是本地协议测试，不替代 Nereus-owned topic、ExclusiveWithFencing、guarded
reader/reconnect、Recovery-Floor retention 或真实 Broker evidence。
`KafkaReceiptJournal` 现在补齐对应的本地 transactional-receipt seam：
`KafkaReceiptResource` 显式绑定 receipt cluster/topic UUID、Route/Shard、slot
generation 以及 `shardPartition * K + receiptLaneSlot`；journal 以一个
transactional-channel key 为边界，在 target transaction sender 之前持久化 exact
mapping，严格阻塞 unresolved lower sequence，支持 mapping/retirement replay 幂等，
并投影 typed `KAFKA_RECEIPT_CONTIGUOUS` cursor。它还构造 Registry 的
`KAFKA_TRANSACTIONAL_RECEIPT` PUBLISHED branch 以及 durable retirement 后的
`KAFKA_RECEIPT_ABSENCE` branch，绑定 target/receipt UUID、partition、generation、
transaction identity、prepared hash 和 receipt/barrier digest；
`ReceiptObservation`/`Resolution` 还会对 receipt cursor identity、attempt/
prepared/record hash 漂移 fail closed，并要求独立的 retirement、LSO barrier
和 retention predicate 才能得到本地 `NOT_PUBLISHED`；
`MAPPED` 和 `RETIRED_NOT_PUBLISHED` 现在都经过注入 appender 并取得 non-null
journal position；retirement position 会推进本地 cursor/replay watermark，append
失败则继续保留 lower sequence 的 unresolved fence。`KafkaReceiptJournalTest` 覆盖这些
本地顺序、回放、cursor、resolver 与 identity fence。
注入的 appender 和调用方提供的 fenced channel 仍只是 canonical value seam，不能
证明真实 target+receipt Kafka transaction、`read_committed` Fetch/LSO contiguous
replay、ExclusiveWithFencing、retention/Floor 或 slot authority，故这些仍是 release
blocker。
`PublishAttemptLedger` 现在保留兼容的 V1/V2 bytes，并提供不改变线上
`PublishAdmissionV1` 的可选 V3 local Journal projection：先记录 adapter 分配的
sequence，再写入 exact acknowledged Journal position，definitive absence 期间设置
`retirementPending`，确认 `RETIRED_NOT_PUBLISHED` 后再清除该 fence。
package-local `DelayShard.recordAttemptJournalMapping`、`markAttemptJournalRetirementPending` 和
`recordAttemptJournalRetirement` 只更新同一 `inflight_cf` value，不推进 Shard source
cursor；因此它们必须由已持有 exact Producer/Attempt identity 的 fenced adapter event
loop 调用，不能被解释为 source-ordered Outcome 或真实 Broker durability。
`PublishAttemptLedgerTest` 与 `DelayShardTest.attemptJournalProjectionIsDurableWithoutAdvancingShardSourcePosition`
覆盖 round-trip、identity/retirement 顺序、重启恢复和 source-position 不变性。
同步 prepare 入口的本地参数/strict-frame 校验现在统一投影为
`PreparationFailure`，其 `StableErrorV1.stage` 固定为 `PREPARATION`，同时保持
`IllegalArgumentException` 兼容性；`AutoFastScheduleTest` 覆盖稳定错误的 canonical
round-trip。它不把网络或 Broker 结果伪装成本地 preparation failure。
调用注入 adapter 时不持有 embedded service monitor，阻塞的 transport 不会
阻止 close fence；仅本地 embedded admission 在 monitor 内完成。
close 还会在 DB close 失败后继续尝试释放共享 RocksDB 资源，并把后续失败作为
suppressed exception 聚合；成功 drain 后服务立即进入 closed 状态，避免部分关闭
时继续触碰已关闭的 Store。该清理顺序仍只属于 embedded seam，不等于真实
Producer close-drain 或 Broker response drain。
构造阶段如果 DB 已打开但 source identity/metadata 校验失败，也会按同一
fail-closed 生命周期执行 Store 与共享 native resource 清理；
`EmbeddedDelayServiceTest.failedEmbeddedConstructionClosesStoreAfterSourceIdentityMismatch`
随后重新打开同一 DB，证明失败的接管尝试不会遗留 DB lock 或资源 slot。
关闭后的 embedded service 也不再暴露底层 `DelayShard` 或 pending-buffer
诊断读取；`EmbeddedDelayServiceTest.closedEmbeddedServiceDoesNotExposeShardOrBufferState`
覆盖该 facade 生命周期 fence。
这只证明本地 SDK seam；Producer buffer、batch/linger、request/delivery timeout、
close drain 以及真实 Broker response 仍属于真实适配器 release gate。

## 故障域闭合

| 故障域 | 允许影响 | 不允许传播为 |
|---|---|---|
| 单 Destination Lane/topic/credential/circuit/Producer | 该 Lane `BLOCKED`、backoff、quota reject、due lag | source pause、其它 Lane permit/queue starvation |
| target ACK 丢失或 callback 超时 | exact attempt `UNCERTAIN`，按 capability/policy 解析 | definitive failure、远端 fencing 成功 |
| Owner Lease/session 丢失 | 关闭本地 source/Admission/event gate，旧 attempt 保守解析 | 撤回已被 Broker 接管的请求 |
| 单 shard DB compaction/checkpoint/L0 storm | 该 DB 的有界 slowdown/placement repair | 消耗其它 DB 的 correctness/due/expiry minima |
| Worker 共享内存、FD、磁盘安全域失效 | 同一真实 shared domain 的 acquisition/Claim/Admission/source safety gate | 事后假装成一个 Lane 的业务拒绝 |
| Object Store credential/provider failure | handle/attest retry、Claim revoke、restore wait、GC 保留保护 | destination-backlog source pause、object-absence proof |
| Oxia/cache/watch failure | 在 exact authority 无法证明时 fail closed 或等待 | cache miss 变业务 not-found、watch 变线性化点 |
| 时钟不确定或 step | 停止新的时间敏感 Admission/expiry，等待 qualified interval | 提前 delivery、提前 expiry、wall-clock replay drift |
| source/evidence retention gap | shard/profile fail closed | newest checkpoint 猜测、空日志猜测 |

## 关键闭合关系

| 关系 | V1 不变量 |
|---|---|
| Shard/DB | `Ingress Route Partition = ownership/ownerEpoch = one RocksDB DB = Source Position atomic commit = checkpoint/restore/delete/migration` |
| Message runtime | 每 generation 零或一个 current TIMELINE/CLAIMED/PUBLISHING work；另有有界 canonical attempt-obligation refs；完整 terminal/history 回收后，`id_cf/MESSAGE` 可保留经校验的 version-5 `RETIRED_IDENTITY` branch，不计入 live runtime |
| Attempt lookup | ref 携 exact inflight key、Owner Epoch、tag、generation、state、hash/digest；ledger 与 locator 双向一一对应 |
| Terminal/Replay | current terminal runtime 与 terminal summary byte-equal；Replay 新 generation 从空 obligation set 开始，旧 ref 只留旧 terminal summary |
| Retry | `BOUNDED_RETRY_POSSIBLE_DUPLICATE` 仅 unordered `BEST_EFFORT`，且 `0 < maxUncertainRetries < maxPublishAdmissions` |
| Digest | timeline semantic digest 排除 local runtime revision；instance digest 包含它；source replay 不比较新 Owner/Store/runtime instance |
| Cancel/Reschedule | 任一 UNCERTAIN obligation 存在即 `TOO_LATE`，不能因 current work 可逆而恢复管理权 |
| Lane/READY | 同一 `meta_cf/LANE` key 是 ACTIVE 或 TERMINAL_GUARD；只有 `OPEN + READY + schedulable` 有一个 exact READY key |
| Lane retirement | source-ordered gate 先到 `CLOSED`，清理与 Floor 条件满足后，same-key replacement 为 `finalGate=RETIRED` guard；retirement progress 与 terminal guard 在 equal order token 时要求 canonical Source Position 完全一致；本地 terminal-guard replacement 在同一 WriteBatch 释放 shard 的 `laneCount` slot，并在重启后保留该记账，`maxLanes=1` 可由回归测试证明复用；外部 grant/Oxia 释放与完整 terminal-guard authority 仍是 blocker |
| Scheduler | Lane-first timeline + one READY head + persisted inner DRR；Worker outer DRR 从有限 shard DB 集合重建，不跨 DB 假装原子 |
| Quota | active/pending 每非终态 generation 一次；inflight 每 Claim/attempt obligation 一次；payload ownership 不随 attempt 倍增 |
| Recovery | candidate 必须是 exact Recovery Floor descendant；session-bound Recovery Pin 覆盖选择到 activation；ACTIVE CAS 删除 pin |
| Checkpoint upload | PENDING/PUBLISHED/REAPING Upload Intent CAS；catalog 只接受 complete immutable manifest |
| Time | Admission 使用 frozen decision interval + Broker persistence inequality；replay 不采样新墙钟 |

这里的 `meta/CONTROL_RESERVE` reserveClass=3--6 与 Registry
`meta/QUOTA` quotaClass=4/5 是两个不同命名空间。后者分别表示
retained/object usage 与 grandfathered transfer state，但目前只有 key subtype，
没有 V1 value schema、digest 或 source-ordered accounting transition。因此不能把
control-reserve 的本地算术证据解释成 retained/object 或 grandfathered-transfer
quota 已实现；`DelayShard` 对这两个 class 的任意非空持久值在 activation 时
fail closed，而不是把它们当成空 projection。Registry 必须先冻结完整编码和
转移规则，之后实现才可写入或恢复这两个 class。

Durable `MessageRecord` values use checked fixed-width decoding for every
version-specific field. Any strict prefix of a canonical value is rejected as
codec validation rather than leaking a buffer-underflow exception; the local
evidence is `MessageRecordTest`.
The persisted Claim value applies the same guard before LP32 length prefixes
and u64/u32 fields; `ClaimRecordTest` covers every strict prefix of a valid
Claim and confirms that the native decoder exception is not exposed.
The publish-attempt ledger applies the same fixed-width guards and uses only
the actual minimum framing prefix, so short valid LP32 values remain readable;
`PublishAttemptLedgerTest` is the local evidence for both cases.
The large-payload reservation value applies the same guards to its post-intent
numeric and presence fields; `PayloadReservationTest` covers strict-prefix
rejection before payload-reference decoding.
Its current and receipt-anchor Source Position fields are now decoded through
`SourcePositionCodec` at construction time, require exact canonical bytes and
the reservation Shard identity, and reject arbitrary non-empty or
foreign-Shard values before they can become source-order, receipt or GC
anchors. `PayloadReservationTest.sourcePositionsMustBeCanonicalAndBelongToReservationShard`
covers the malformed and cross-Shard fences; this remains local binding
evidence, not authenticated ingress/source authority.
The same source boundary now covers the durable Message, terminal-generation
and publish-attempt values: their source-position fields are decoded with
`SourcePositionCodec` and re-encoded to exact canonical bytes before the value
can be persisted. Existing `DelayShard` reads still apply the separate
current-Shard identity fence, while adapter Journal positions are not Source
Positions and remain opaque bounded evidence. The focused regressions are
`MessageRecordTest.sourcePositionMustBeCanonicalBeforeMessageValueConstruction`,
`TerminalGenerationRecordTest.sourcePositionMustBeCanonicalBeforeTerminalValueConstruction`
and `PublishAttemptLedgerTest.sourcePositionMustBeCanonicalBeforeAttemptValueConstruction`.
The compact post-GC `RetiredMessageIdentityRecord` now applies the same
canonical decode before retaining its source anchor; `DelayShard` still checks
that decoded anchor against the current Shard before exposing identity-reuse
state. `RetiredMessageIdentityRecordTest` covers canonical round-trip and
malformed/trailing-byte rejection. This is local retention evidence only;
Recovery-Floor and Route identity-retention authority remain release gates.
The local Kafka Receipt Journal and Pulsar Attempt Journal now canonicalize
their `AttemptIdentity` Source Position and require both the DelayMessageId and
decoded source position to match the journal Shard before constructing a
mapping. Focused `KafkaReceiptJournalTest` and `PulsarAttemptJournalTest`
vectors cover malformed source bytes and cross-Shard mapping rejection. This
does not claim Broker receipt/journal durability or authenticated transport
evidence.
Its `COMMITTED` branch also rejects a payload reference whose length or
SHA-256 differs from the Prepare intent, so a damaged durable value cannot
cross the reservation-to-message boundary as a false object binding. The
focused evidence is
`PayloadReservationTest.committedPayloadMustMatchPrepareLengthAndDigest`;
provider/Object Store authority remains a release gate.
`DelayClient.prepareLargePayloadCommit` now closes the client-side preparation
boundary as well: the embedded path refuses a proof whose reservation/message,
shard, object identity, payload digest/length, trust-set version or expiry
drifts from the durable reservation receipt before constructing the canonical
`CommitLargeScheduleV1` body. This is still a local pre-I/O binding check;
source-ordered reservation lookup, proof-key authority and production Object
Store verification remain release gates.
The client facade also exposes receipt-bound `issuePayloadUploadHandle` and
`attestPayloadUpload` when a deterministic local Object Store adapter is
injected. It rereads/registers the exact shard reservation and returns the
closed retryable unavailable branch when no adapter is configured; it does not
claim Oxia handle-protection CAS, provider credentials or remote immutability.
The positive local reserve-to-handle-to-attestation path is covered by
`EmbeddedDelayServiceTest.receiptBoundPayloadFacadeRereadsTheShardReservation`.
The local adapter now keeps the first registered Prepare reservation as the
immutable receipt anchor while accepting only the same reservation identity's
legal source-ordered lifecycle advance. The shard-local reservation value
persists the anchor state version and Source Position, so a newly constructed
adapter reconstructs the original receipt after reopening a v2 `COMMITTED`,
`ABANDONED` or materialized `EXPIRED` value. A strict legacy-v1 decode/upgrade
path remains for older local values; because those values never carried an
anchor, their fallback anchor is the current state and cannot invent missing
historical Prepare data. A receipt observed before a later lifecycle transition therefore reaches
the corresponding typed closed outcome instead of being reported as
`INTEGRITY_ERROR`; arbitrary identity/state drift remains rejected. The
focused evidence is
`InMemoryPayloadObjectStoreTest.receiptAnchorSurvivesSourceOrderedReservationLifecycleTransitions`
and `EmbeddedDelayServiceTest.payloadFacadeMapsSourceOrderedReservationCloseToTypedOutcome`.
This closes only the local durable binding projection; source-position trust
authority, external Object Store/Oxia binding and guarded object retention
remain release gates.
Terminal generation history uses the same guarded reads for source and
obligation framing in both legacy and v2 branches; the local prefix evidence
is `TerminalGenerationRecordTest`. Direct `DelayShard` history reads also
compare the embedded `messageId/generation` with the requested terminal key;
`DelayShardTest.terminalGenerationLookupRejectsKeyValueIdentityMismatch`
covers the misplaced-value fence before a query or runtime summary can use it.
System Mutation result reads apply the same mutationId/key check, so a
misplaced `dedupe_cf/SYSTEM_MUTATION` result cannot be returned as another
mutation's outcome.
The embedded Kafka source counter treats offsets as an unsigned 64-bit
sequence: the raw Java `-1L` bit pattern is accepted as the final valid
offset, then a separate exhaustion flag prevents a successor from being
allocated.  This avoids using a valid offset as a pre-enqueue sentinel and
keeps a failed post-exhaustion enqueue from mutating source state;
`EmbeddedDelayServiceTest.embeddedSourceAcceptsUnsignedMaximumOffsetThenExhausts`
and `EmbeddedDelayServiceTest.embeddedSourceOffsetExhaustionFailsBeforeMutatingOffset`
prove both boundaries.
Persisted Delay Shard mutation and Claim sequence metadata now preserves the
Registry's complete raw `uint64` domain through activation, checkpoint barriers
and Claim identity derivation. The local evidence is
`DelayShardTest.acceptsCompleteUnsignedPersistedShardSequences`.
Every source-position WriteBatch computes its successor through one checked
unsigned helper, including the applied sequence captured by resource
retire/delete records. The successful post-write update stores that checked
successor in the in-memory projection; `0x7fff... -> 0x8000...` is valid and
the all-ones pattern is the only exhausted value. At exhaustion the next
WriteBatch fails before any authoritative command or position state is committed;
`DelayShardTest.mutationSequenceExhaustionFailsClosedBeforeCommandMutation`
covers the near-maximum and exhausted boundaries.

Checkpoint manifest `lineageGeneration`/`shardMutationSequence` and both scalar
and typed Recovery Floor projections now use the same full-width unsigned
representation. Canonical JSON/Protobuf/floor-digest bytes retain high-bit
patterns, catalog ancestry uses checked unsigned lineage successors, and Floor
coverage compares mutation sequences unsigned. `CheckpointManifestTest`,
`RecoveryFloorRefV1Test` and `RecoveryCatalogTest.catalogComparesManifestMutationSequenceAsUnsigned`
cover the local boundary; Oxia CAS, object-store publication and source replay
remain release evidence.
The durable Command/System Mutation result values apply the same source-anchor
boundary independently: their constructors and decoders require canonical
Source Position bytes, so an empty, truncated or non-canonical result cannot
become a local projection before a shard-specific lookup runs. The evidence is
`DurableResultTest`.
The Client Command position audit now also closes the replay boundary for
position-level outcomes: its exact `commandId[41]` value is read when the same
Source Position is delivered again, allowing a previously persisted fence
rejection or `COMMAND_ID_CONFLICT` to return the same result without creating a
logical Command Result or appending another audit. The same-hash duplicate
Command path validates that locator both at the first logical result position
and after restart at a later physical position; a missing or cross-shard
POSITION value remains fail-closed. A later same-hash Command whose Broker
persistence time is outside its retry window returns only a position-level
`COMMAND_RETRY_WINDOW_EXPIRED` result, leaves the first logical result
unchanged, and replays that position-level result after restart. The local
evidence is `DelayShardTest.laterDuplicateOutsideBrokerRetryWindowReturnsPositionRejectionWithoutChangingLogicalResult`
alongside the exact-replay and missing-audit tests.
The System Mutation dedupe path applies the complementary rule: an exact
already-verified mutation at its first or a later Source Position must match
the physical audit; an in-window later duplicate advances only the durable
applied position, and replay of that same later position returns the stored
first-result without re-running the mutation. A duplicate whose Broker
persistence time is outside its signed `mutationRetryUntil`, or whose retry
window was already closed by `TIME_FENCE`, produces only a position-level
`SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED` result and leaves the first logical
dedupe value unchanged; exact replay returns that same position-level result.
This avoids treating a valid post-commit duplicate as a source-position
conflict while preventing a closed retry window from regaining mutation
authority. The POSITION value is now a closed command/system identity union:
every System Mutation WriteBatch records its mutation ID, later duplicates
replace only the physical locator, and a replay at an already-advanced
position must match that locator. A command audit, a different mutation audit,
missing evidence, or a cross-shard identity is rejected instead of being
mistaken for the duplicate. The local evidence is
`DelayShardTest.systemMutationDuplicateOutsideRetryWindowKeepsFirstResultAndUsesPositionAuditOnly`,
`DelayShardTest.timeFenceMonotonicallyClosesIngressWithoutOverwritingCommandIdentity`
plus the existing source-ordered System Mutation duplicate tests.
Kafka's exclusive activation LSO uses the same fail-closed boundary handling:
an applied offset at the unsigned-64 maximum proves an exclusive barrier
without wrapping the successor calculation. The local evidence is
`SourceActivationBarrierTest.KafkaBarrierAcceptsUnsignedMaximumExclusiveOffset`.
The embedded source counter also saturates while reconstructing its next offset
from persisted state, so restart after the unsigned maximum remains a
controlled exhaustion rather than an arithmetic-open failure; the evidence is
`EmbeddedDelayServiceTest.reopenedEmbeddedServiceKeepsUnsignedMaximumSourceOffsetExhaustion`.
Canonical protocol writing now rejects values outside the unsigned 32-bit
range, while the Registry's resource-state version is treated as a raw
unsigned `uint64` across the retire body, delete-confirmation reference,
logical identity hash, GC key and durable intent record. The System Mutation
body validator has an explicit raw-`uint64` exception only for that registered
field; local mutation/claim sequences remain bounded non-negative counters.
The nested `ProtectionRefV1.protection_generation` and its `gc_cf/PROTECTION`
locator likewise retain the complete raw `uint64` pattern; external
protection authority and guarded release remain release evidence.
The same retire-identity parser now preserves complete nonzero raw `uint64`
bits for nested `ProfileRefV1` versions and Kafka receipt-slot/Pulsar journal
external generations. Its nested Pulsar Broker resource validation also keeps
the physical-topic creation timestamp byte-exact instead of applying a signed
host-range check. Object/manifest lengths remain subject to the separate
bounded local admission envelopes rather than being widened into unbounded
runtime arithmetic.
The Admission body’s duplicated nested ProfileRef/Broker-resource validators
now use that same boundary, keeping a high-bit Profile version accepted in the
descriptor and ClaimMaterialization projections instead of rejecting it in a
shallow parser. The local evidence is
`PublishAdmissionBodyTest.preservesUnsignedProfileReferenceVersionsAcrossAdmissionMaterialization`.
The local evidence is `ResourceRetireIntentBodyTest.preservesUnsignedResourceStateVersionBits`,
`ResourceDeleteConfirmedBodyTest.intentPreservesFullUnsignedResourceStateVersion`,
`KeyCodecTest.gcRetireIntentKeyPreservesUnsignedResourceStateVersionBits`,
`ResourceGcGuardTest.durableGcRecordsPreserveUnsignedResourceStateVersionBits`
and the high-bit `DelayShardTest.resourceRetireIntentIsSourceOrderedDurableAndVersionFenced`.
The descriptor and ClaimMaterialization branches now also reuse the typed
Broker-resource, committed Object Store descriptor and adapter-metadata codecs,
keeping Admission and Claim Result on one strict NFC/branch/Profile-kind
boundary rather than maintaining divergent shallow validators.
The canonical-protobuf reader also rejects field numbers outside the Registry's
`1..0x1fff` range before closed-union dispatch; the local regression is
`CanonicalProtobufTest.readerRejectsFieldNumbersOutsideRegistryRange`.

当前 `PersistentLaneScheduler.rebuildFromAuthoritativeReady` 已提供 fenced 的本地
恢复桥：它从 bounded `timeline_cf/READY` 扫描开始，严格校验对应的
`meta_cf/LANE` incarnation/version/gate/readiness、`id_cf/MESSAGE` 的当前
generation/status，并从 Message 的 source position 重算 exact timeline key、
digest 与 timeline value，再一次性替换内存 pending heads 与 active DRR ring；旧、
孤儿、重复或非 schedulable projection 会 fail closed。`ReadyDiscoveryCursor` 的
`lastScannedReadyKey`、`wrapGeneration` 和 active-ring generation 也会在同一组
SCHEDULER projection 中持久化，恢复不会把 stale Lane 重新加入 ring。恢复时还会
比较 `SchedulerRoundV1.owner` 与当前 Owner；Owner/Store 更换会重新进入
`recovery_first_pass`，由 `LaneScheduler` 跟踪已服务 Lane，保证第一轮对每个当前
eligible Lane 至多取一条记录，直到所有已发现 Lane 都获得机会；fenced READY rebuild
同样重启该 pass。恢复扫描本身现在从 READY namespace 的起点执行完整 bounded pass，
不会把 rotating discovery cursor 当成已消费的条目；因此超过 `maxReadyEntries` 的
READY 集合必然 fail closed，而不会因为 cursor 被丢弃而静默漏掉一个 head。
`LaneSchedulerTest.fencedRecoveryUsesCompleteReadyPassDespitePersistedDiscoveryCursor`
覆盖该边界。正常运行的 `discoverReady` 现在从同一 cursor 读取 bounded slice，
把 exact READY/Lane/Message/timeline projection 提升到 active ring；已经 poll 但仍
等待 Claim 的 head 由进程内 identity fence 抑制重复 offer，READY head 改变后才允许
同 Lane 的 successor 进入队列。inclusive cursor 读取额外一个条目，保证 `limit=1`
在首项上也能继续到 successor 或 wrap；`LaneSchedulerTest.rotatingReadyDiscoveryDoesNotReofferPolledHeadAndFindsSuccessorAfterWrap`
覆盖该边界。该实现证明
的是单 DB 内的本地物理边界和确定性恢复顺序。Typed
`ActiveLaneStateV1` 的 direct read、bounded compatibility projection and
same-key immutable-field preservation now have local evidence, but this still
does not equal Oxia owner/session fencing or real Lane certificate/adapter
activation.
Worker 外层 DRR 也只把至少含有一个 schedulable pending head 的 shard 纳入 visit，
空 shard 不会消耗外层 deficit；其 cursor/round 仍是跨独立 shard DB 的 bounded
process state，不伪造跨 DB 的持久原子 ring。Worker 外层 scheduler 在新建、
restore 或 READY 集合重新激活后进入 recovery first pass：当前 eligible shard
在重复服务前各获得一次 outer visit，首轮每 shard 最多取一条 work item；新增/恢复
的 shard 不会被旧 hot shard 直接越过。外层 visit 会读取当前最小 schedulable
Lane head；当该合法 record 大于 outer deficit cap 时，只在全局 byte budget 足够的
情况下临时放宽本次 shard budget，因此大记录不会被 cap 永久饿死。
`WorkerSchedulerTest.outerDeficitCapDoesNotMakeLargeHeadUnserviceable` 覆盖该边界。
ownership loss 后，`WorkerScheduler.unregisterShard` 还要求 outer shard 已 blocked
且其所有本地 Lane queue 已排空，随后才从进程内 ring/registry 移除、按剩余 shard
重算 deficit cap 并截断保留的 deficit；`WorkerSchedulerTest.shardUnregisterRequiresBlockedAndDrainedLocalQueue`
与 `WorkerSchedulerTest.unregisteringHighestWeightShardRecomputesOuterDeficitCap`
覆盖这一生命周期 fence。它只回收可重建 outer process state，不宣称跨 shard DB
的持久原子更新，也不替代 source-ordered terminal guard、Store close 或 Oxia
ownership authority。
两级 scheduler 现在还对
`weight * quantum` 与 deficit cap 做 checked arithmetic，并对运行时 deficit 累加做
saturating arithmetic；配置、注册或恢复导致的整数溢出不会 wrap 成可调度的错误预算。
恢复 snapshot 时也会立即把每个已注册 Lane/Shard 的 deficit 截断到当前 cap，
所以旧 quantum/weight 配置留下的超 cap 值不会在空闲期间继续存在；
`LaneSchedulerTest.saturatesRestoredDeficitBeforeServing` 与
`WorkerSchedulerTest.saturatesRestoredDeficitBeforeServing` 同时检查恢复后的
projection 和第一次 service。
两级 cap 至少提升到当前注册的 `weight * quantum`，因此 weight 大于四时不会被
固定 4×quantum cap 静默截成错误的长期服务比例；
`LaneSchedulerTest.highWeightRetainsItsConfiguredDeficitQuantum` 与
`WorkerSchedulerTest.highWeightRetainsItsConfiguredOuterDeficitQuantum` 覆盖该边界。
本轮还把 trusted due-through 明确传入两级 scheduler：`LaneScheduler` 在 inner
poll 前检查 `eligibleAtEpochMs`，`PersistentLaneScheduler` 的 READY discovery
只把不晚于该边界的新 head 返回，同时允许 future projection 留在本地队列并由
后续 poll 继续 fence；durable cursor 只推进到本轮最后一个 eligible key，future-only
切片在重启后仍可重新发现。`WorkerScheduler` 将同一边界传给每个 shard，等值按
inclusive 语义可服务。`LaneSchedulerTest.duePollUsesAnInclusiveEligibilityBoundary`、
`LaneSchedulerTest.persistentReadyDiscoveryAndPollFenceFutureDeliverAt` 和
`WorkerSchedulerTest.workerPollCarriesTheInclusiveDueThroughBoundaryToShardScheduler`
覆盖这一 local scheduler seam。无时间参数的 overload 仍是兼容接口，不是生产
Trusted UTC/Owner/Oxia 证据；真实 time authority、Broker visibility 与 production
Claim/Admission wiring 仍是 release blocker。
恢复首轮的 eligible 集合也按同一 trusted due-through 重新计算，而不是把所有
pending head 都算作当前机会；inner/outer 的最小 head byte budget 同样只看当前
due head。否则一个远期 future Lane/Shard 会让 `recovery_first_pass` 永不收敛，
阻塞其它已经 due 的 work。`LaneSchedulerTest.persistentRecoveryFirstPassIgnoresFutureLaneForDueFairness`
与 `WorkerSchedulerTest.futureShardDoesNotHoldRecoveryFirstPassOpenForDueWork` 覆盖
该本地公平性边界；生产 Trusted UTC、placement 和 Owner/Oxia authority 仍不由此
证明。

本轮还把调用方的全局 byte budget 纳入 recovery-first-pass 的机会集合：due
head 大于当前 budget 时不会 Claim，也不会让 inner/outer 的恢复首轮等待该
不可服务的 head；它会留在 pending projection，等后续拥有足够预算的 turn
再被尝试。`LaneSchedulerTest.persistentRecoveryFirstPassDoesNotWaitForAnOversizedHead`
与 `WorkerSchedulerTest.oversizedHeadDoesNotHoldRecoveryFirstPassOpenForSmallerShard`
覆盖这一边界，证明一个过大的 due head 不会阻塞当前预算内可服务的健康 work。
Legacy client receipt handling now closes the remaining local locator gap:
`CommandQueuedReceipt` binds `commandId`, `delayMessageId`, and Source Position
to the same Shard, and embedded `awaitApplied` validates its pinned Kafka source
and exact durable-or-pending physical locator before draining. A queued command
without a durable POSITION audit is admitted only when the pending record has the
same command/message/source tuple; after drain the audit is reread before the
result is returned. The exact pending `DelayShard.apply` result is retained for
that tuple, so a position-level `COMMAND_ID_CONFLICT` or fence rejection cannot
be collapsed into the first logical result (or become `null`) by a commandId-only
lookup. `EmbeddedDelayServiceTest.awaitAppliedRejectsForeignSourceBeforeDraining`
and `EmbeddedDelayServiceTest.awaitAppliedRejectsSameShardReceiptWithWrongPhysicalPositionBeforeDraining`
show that foreign or forged receipts are rejected without applying queued work,
while `EmbeddedDelayServiceTest.awaitAppliedReturnsTheExactPendingPhysicalConflictResult`
and `EmbeddedDelayServiceTest.queuedReceiptRejectsMessageIdFromAnotherShard` cover
the exact pending result and legacy constructor identity fences. This is a local
API/conformance guard; gateway authorization, production routing and durable
receipt-retention authority remain release blockers.
An explicit embedded `drain()` also retains physical apply outcomes within the
bounded `EmbeddedDelayServiceConfig.maxPendingCommandCount` window, so a later
legacy await can still return a position-level conflict or fence result. If
that local evidence is evicted and the durable logical result is anchored at a
different position, the seam fails closed instead of returning the wrong
logical result or `null`; this does not add outcome bytes to `dedupe/POSITION`.

§12.4 的本地时钟 guard 现在由 `TrustedUtcClock` 提供：它只从批准的
`TrustedUtcIntervalEvidence` 和注入的 monotonic reading 推导保守 interval，
对 uncertainty、sample age、wall/monotonic step 和 stabilization window
fail closed，并只允许 qualified interval 使用严格的 due/pre-expiry 谓词；
`TrustedUtcClockTest` 覆盖这些状态切换。同步源签名、Broker-time 认证以及
生产 Worker/Admission 接线仍是 release blocker。配置还必须让
`maxUncertainty` 覆盖 projection 两侧的 divergence 扩展，否则在 guard
激活前直接拒绝不可能 qualified 的 timing budget。
Placement 的 dominant-resource score 现在使用候选的 projected
`committedCapacity + required`，而不是只评分新 shard 的增量；因此 equal telemetry
下已接近 hard capacity 的 worker 不会因字典序 tie-break 抢到新 shard。
`WorkerPlacementPolicyTest.projectedCommittedCapacityBreaksEqualTelemetryTie`
覆盖该本地 placement fence。
`LaneSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` 与
`WorkerSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` 是本地回归证据，
`WorkerSchedulerTest.recoveryFirstPassServesEveryEligibleShardBeforeRepeatingOne` 与
`WorkerSchedulerTest.restoreStartsANewOuterFirstPass` 覆盖 outer recovery fairness；
不等于 production placement/authority 已完成。Lane runtime/control version 的
checked increment 也在 `Long.MAX_VALUE` fail closed，避免 READY key 或管理 CAS
版本回绕；`LaneRecordTest.versionCountersFailClosedBeforeLongOverflow` 覆盖该本地边界。
Message Control Version (`stateVersion`) 与本地 generation successor 在
Cancel/Reschedule 及其 replay projection 中也使用 checked increment；达到
Java 表示上限时先以 `INVALID_COMMAND` 持久拒绝，不会把负值写入 Message 或
timeline。`DelayShardTest.messageGenerationAndStateVersionOverflowFailClosedBeforeMutation`
覆盖这两个边界。
Scheduler round generations and persisted service-gap counters now saturate at
`Long.MAX_VALUE`, and inner scheduler byte accumulation is checked; the local
regressions are `LaneSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`
and `WorkerSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`。
`PersistentLaneScheduler` also computes ring and READY-cursor wrap generations
locally and commits them to memory only after the complete five-value scheduler
`WriteBatch` succeeds. A failed projection write or malformed READY projection
cannot leave the process advertising a generation that was never durable; the
local regressions are
`LaneSchedulerTest.failedSchedulerProjectionWriteDoesNotAdvanceGenerationInMemory`
and `LaneSchedulerTest.failedReadyProjectionDecodeDoesNotAdvanceWrapGenerationInMemory`。
The same durable boundary now covers every turn that mutates process state,
including a fenced READY rebuild:
failed `poll` writes put returned heads back at the front of their Lane queues;
failed READY discovery removes only the tails it appended and restores the
active ring, cursor, discovery heads and recovery bookkeeping; failed blocked/
ready projection writes restore the previous schedulable state before rethrowing.
`LaneSchedulerTest.failedPollProjectionWriteRestoresThePolledHeadInMemory` and
`LaneSchedulerTest.failedReadinessProjectionWriteRestoresThePreviousGateProjection`
cover these rollback paths, while `LaneSchedulerTest.queueSnapshotRestoresExactFifoProjection`
covers the exact FIFO snapshot used by fenced rebuild rollback. A durable READY
head cannot be lost merely because its projection batch failed.
The full READY queue replacement path now performs the same all-items-first
validation: a later unknown/non-schedulable item leaves the original FIFO intact
(`LaneSchedulerTest.failedPendingReplacementKeepsTheOriginalQueues`).
Lane/Shard counter restore now validates the complete registered subset and
rejects duplicate identities before publishing any counter or blocked-state
change; a malformed later snapshot entry or ambiguous repeated identity cannot
leave an earlier Lane/Shard partially restored (`LaneSchedulerTest.invalidLaterRestoreEntryDoesNotPartiallyApplyEarlierCounters`,
`LaneSchedulerTest.duplicateLaneRestoreIdentityDoesNotPartiallyApplyEarlierCounters`,
`WorkerSchedulerTest.duplicateWorkerRestoreIdentityDoesNotPartiallyApplyEarlierCounters`).
Persistent projection restore applies the same rule across the active ring: the
semantic generation/counter snapshot is validated before replacing the ring,
and a malformed persisted generation rolls the registered scheduler back to its
exact pre-restore ring and counters (`LaneSchedulerTest.malformedPersistedSchedulerGenerationDoesNotPartiallyApplyTheActiveRing`).
Deficit entries are also checked against the currently registered Lane
incarnation and `observedLaneVersion`; a same-key Lane version change cannot
inherit stale credits, while the incarnation-level last-served history remains
available for the service-gap projection (`LaneSchedulerTest.stalePersistedDeficitVersionDoesNotRestoreCreditsToARevisedLane`).
The inner and outer two-rotation visit limits also widen `ring.size() * 2`
before comparison, with `LaneSchedulerTest.ringVisitLimitUsesWideArithmetic`
and `WorkerSchedulerTest.outerVisitLimitUsesWideArithmetic` covering the
large-ring arithmetic boundary. This closes only the local integer-wrap path;
the production population, time-share and fairness bounds still require the
capacity/chaos evidence listed below.
Lane registration now also fences `laneIncarnation` as an immutable part of a
registered `destinationLaneId`: a different incarnation is rejected before
the in-memory queue or `PersistentLaneScheduler` registration map can be
replaced, while same-incarnation state updates remain supported. The local
regression is
`LaneSchedulerTest.rejectsLaneIncarnationChangeWithoutMutatingSchedulerState`;
this does not replace source-ordered Lane lifecycle or terminal-guard
authority.
Worker shard registration applies the same no-partial-update rule: weight
arithmetic is checked before the outer deficit cap can change, so a conflicting
duplicate Shard registration cannot influence subsequent service gaps. The
local regression is
`WorkerSchedulerTest.conflictingShardRegistrationDoesNotMutateOuterDeficitCap`.
Persisted inner fairness counters are also restored for registered Lanes that
are temporarily outside the active ring; a BLOCKED/paused Lane no longer loses
its `lastServedRound` or deficit across owner/store restart. The local
regression is
`LaneSchedulerTest.fairnessCountersSurviveRestartForLaneOutsideActiveRing`.
Scheduler recovery additionally rejects digest-valid cross-value generation
drift: the discovery cursor, active ring and round must describe the same
ring/round generations before the local scheduler is opened. The regression is
`LaneSchedulerTest.schedulerRestoreRejectsCrossProjectionGenerationDrift`.

当前代码已把 Lane 的 same-key ACTIVE/TERMINAL 分支和保守本地退休证明接入
`DelayShard`；并已补齐 Registry-shaped `ActiveLaneStateV1`、
`LaneQuotaUsageEntryV1/MapV1`、`ReadyCertificateV1`、`ActivationBarrierV1` 与
`EvidenceCursorV1` 的独立 canonical codec 与交叉校验，checkpoint manifest 也能
严格 round-trip 非空的 Kafka/Pulsar typed evidence-cursor 数组；
`RecoveryFloorRefV1` 已补齐 lineage/checkpoint/source/typed-cursor-array 的
canonical floor reference codec，`RecoveryCandidateRefV1` 与 `RecoveryPinV1`
也已补齐 candidate branch、lineage binding 和 session-identity digest 的
canonical value codecs；`EvidenceCursorV1` 现在还提供同 generation
identity/dominance 校验（Kafka offset/LSO、Pulsar inclusive member 和
Broker-time anchor），跨 generation 保持 incomparable；本地 `RecoveryCatalog` 现在对同一 shard 提供
typed Floor advancement/current-FloorRef 和同一 shard 的 Floor/catalog generation
绑定的单 active-pin create/idempotent reread/release 投影；typed Floor 还要求
传入 cursor 集合与候选 checkpoint manifest 的 evidence-cursor 数组 byte-equal，
再执行同 generation dominance，避免把 checkpoint 未覆盖的 evidence 推进到
Floor；legacy scalar Floor 创建 typed Recovery Pin 时也执行相同的 manifest
cursor 绑定；checkpoint parent lineage 还要求每个父 cursor 在子 manifest
中保持同一 identity 并单调 dominate，不能通过 source position 前进掩盖
evidence cursor 回退。但这仍不是 Oxia
Owner Lease/session CAS；`CheckpointResourceV1` 与
`CheckpointUploadIntentV1`
也已补齐 manifest-object identity 和 PENDING/PUBLISHED/REAPING 的 canonical
state branches；`CheckpointUploadIntentStore` 还提供了 exact-value create
idempotency 与本地 PENDING_UPLOAD -> PUBLISHED/REAPING revision CAS 投影。
它的 `Path` 构造还把完整 canonical intent 持久化到 checksum/atomic-rename/
directory-fsync 的 state file，并用 JVM/on-disk lock 保护跨实例 CAS；重启、
exact PUBLISHED reread 和损坏状态 fail-closed 已由
`CheckpointUploadIntentStoreTest` 覆盖。无参构造仍只是内存 projection。
`CheckpointUploadIntentAuthority` 现在把 local store、upload coordinator 与
`OxiaSyncCheckpointUploadIntentBackend` 统一到同一 value-transition surface；
后者以 shard/checkpoint identity 为 key，把完整 intent 放进一个 canonical
Oxia record，用 version CAS 完成 PENDING_UPLOAD -> PUBLISHED/REAPING，并在
response loss 后只接受 exact successor reread。`OxiaSyncCheckpointUploadIntentBackendTest`
覆盖 pending/published/reaping、deadline、reopen、corruption 和 response-loss。
这闭合的是 per-intent durable CAS，不等于 Owner Lease/session、catalog
publication、Object Store attestation、owner-abandonment 或 reaping/quiescence
的跨 record authority。
`PersistentRecoveryCatalog(Path)` 现在把同一 shard 的已发布 manifest、immutable
manifest-object identity、scalar/typed Floor 和 active Recovery Pin 保存为排序的
canonical snapshot，并用 checksum、临时文件、atomic rename、directory fsync 与
JVM/on-disk lock 保护重启和跨实例 CAS；`PersistentRecoveryCatalogTest` 覆盖
manifest/Floor/Pin/ancestry 重开、generation CAS、scalar Floor 严格解码和 checksum
损坏 fail-closed。它只是本地 crash-durable projection，不能替代 Oxia
Owner Lease/session、catalog/Floor transaction 或 Object Store authority。
其中 REAPING 竞争在 response loss 后可用相同 pending identity 和
`reapingStartedAt` evidence 精确重读同一 successor；不同 evidence 仍 fail
closed，且 evidence 的 earliest trusted time 必须达到 upload deadline；
deadline 前的 reaper 证据不会推进状态。新增的
`CheckpointReapingGuard` 在进入 REAPING 前还检查 published catalog
protection 和同 lineage/checkpoint 的 active `RecoveryPinV1`；catalog/pin
读取失败（包括 fatal `Error`）也 fail closed。该边界不等于 owner abandonment/lease-loss
authority、quiescence、exact-version Object Store delete/final prefix sweep
或 Oxia transaction。
`ShardStore` 还提供 pin-aware restore overload：它在下载/暂存校验后、原子
移动新 Store Incarnation 前，以及正式打开并 fsync 新 incarnation 后、发布
checksummed `ACTIVE` pointer 前，都会 reread exact active `RecoveryPinV1`。pin
缺失或值漂移，以及 Floor 已越过 candidate，都会 fail closed；若新 incarnation
已经移动但尚未发布 pointer，清理路径只删除这个未发布目录并保留原始错误。
`ShardStoreTest.catalogBoundRestoreRejectsPinDriftBeforeActivePublication` 覆盖
后一个 late-session-drift 窗口。这把本地安装边界接上了 pin/Floor 语义，但不
冒充 production Oxia 的 Owner Lease/session 同事务 CAS。
`OxiaRecoveryCatalog` 的 response boundary 现在会在 scalar/typed Floor CAS
后 reread exact published manifest，并拒绝 lineage、manifest hash、source
position、mutation sequence 或 typed evidence-cursor drift；typed 返回还必须
与请求的 cursor 集合 byte-equal，缺失 manifest 也 fail closed。这只是远端
响应验证，不等同于已经实现 Oxia transaction。`currentFloor`/
`currentFloorRef` 和 `proveFloorCoverage` 的只读响应也会绑定已发布
manifest、candidate/Floor identity、请求的 mutation/source boundary 与
ancestry 末端，拒绝漂移或缺失 Floor；equal order token 仍要求 canonical
Source Position 完全一致。
Publication 及 upload-intent publication 返回的可选 Floor 也会绑定到其已发布
manifest、同一 shard，并要求 Floor catalog generation 不晚于 publication
generation；因此 catalog publication response 不能夹带另一条 shard 或更高代的
伪造 Floor。这仍只是 adapter response fence。
现在 `OxiaSyncRecoveryCatalogBackend` 把单 shard 的 manifest、immutable
manifest-resource、scalar/typed Floor 投影编码为一个 bounded canonical Oxia
record，并用单次 version CAS 做 publication/Floor mutation；response loss 只有
在 exact snapshot reread 后才会被视为成功，malformed/non-canonical snapshot
会 fail closed。`OxiaSyncRecoveryCatalogBackendTest` 覆盖真实 Oxia Java client
record surface 的 deterministic seam、reopen、corruption 和 response-loss
边界。这闭合的是 catalog/Floor 单 record CAS，不等于 upload-intent 与 catalog
的跨 record transaction，也不等于 Owner Lease/session-bound RecoveryPin；这两类
能力仍由 backend 明确拒绝。新增的 `OxiaRealRecoveryAuthoritySmokeTest` 在
2026-08-12 对 Oxia source `a45e38cf2b8c815499fda4c1b59e017db769142f` 的真实
endpoint 通过了 catalog publication、typed Floor CAS、reopen/local-reuse
validation，以及 Upload Intent PENDING_UPLOAD -> PUBLISHED CAS/reopen；这仍是
single-record service evidence，不扩大为跨 record transaction、Owner
Lease/session-bound RecoveryPin、multi-worker 或 Object Store publication
evidence，后四者仍是 release gates。
同一 backend 现在还会在复用本地 Store 前，对当前远端 catalog/Floor snapshot
执行只读校验：要求 shard、published lineage/manifest、typed Floor generation
以及 install-state/store-incarnation tuple 全部 exact match；stale Floor 或非
descendant projection 会 fail closed。这只是 catalog-side reuse proof，不推断
Owner Lease/session，也不替代跨 record activation transaction。
`ShardStore.openForLocalRecoveryReuse` 现在把这一条本地 proof 接到打开边界：
它只解析已有的 checksummed `ACTIVE` incarnation，不在没有可复用 Store 时
创建 fresh DB；打开后先执行 catalog/Floor reuse validation，失败会关闭 Store
并释放 Worker DB/owned-shard/physical-usage 注册，再把错误交给上层选择
checkpoint restore。对应回归为
`ShardStoreTest.localRecoveryReuseOpensOnlyCatalogValidatedActiveStore` 和
`ShardStoreTest.localRecoveryReuseDoesNotCreateAFreshDbWithoutActiveIncarnation`。
这仍只是本地复用入口，不能替代 Owner Lease/session、Recovery Pin 的
跨记录 CAS、source replay 或最终 `ACTIVE_FOR_COMMANDS` activation。
这仍不是 Oxia 的 Owner Lease/session、lineage-head、catalog-generation
transaction，也不执行 Object Store upload/attestation/delete。现有 `DelayShard` 仍
通过兼容 `LaneRecord` 写入 ACTIVE 分支，因此这不被误报为已经完成 full
ActiveLaneState persistence 或外部 quota-map revision authority、Oxia target
registration、Oxia Recovery Pin/Floor CAS、source/evidence replay 或
Recovery-Floor/retention gate。`LaneRecordEnvelopeV1` 现在还提供 Registry
field-10 直接承载 typed `ActiveLaneStateV1` 的构造、严格解码和
legacy-adapter 区分，并对 malformed typed bytes fail closed；不过当前
`ScheduleIntent` 只带 `destinationLaneId`，无法无损提供完整 active state 所需的
immutable Profile refs、canonical tuple、READY certificate 和 quota 输入，所以
`DelayShard` 仍明确停留在兼容 adapter 路径，不能把这一步误报成运行时 cutover。
Typed `ActiveLaneStateV1` 与 `LaneTerminalGuardV1` 现在还会解析
Registry-shaped canonical Lane tuple，要求两个 immutable Profile 槽位按
id/version/semantic-hash byte-project，并在 tuple 截断、未知分支、尾随 bytes 或
adapter/resource/ordering 不一致时 fail closed。新增的
`CanonicalLaneTupleV1` 及其 Active/Terminal 回归测试只证明本地 canonical shape
和 projection fence；Profile resolver/catalog、Oxia ownership 与 Broker authority
仍是 release evidence，兼容 adapter 也仍未切换为 typed runtime persistence。
在 typed Lane 的本地 scheduler projection 中，READY 现在同时要求
`earliest_action_at`、`next_eligible_at`、exact key 和 certificate；候选 head 的
action boundary 会随同 field 16/key 在同一 Lane value 中更新，不再沿用旧 field 15。
READY discovery 也会把这两个时间与当前 `TimelineWorkRef` 交叉校验；不一致时
fail closed；`PersistentLaneScheduler` 的 fenced recovery 也执行同一交叉校验，
避免调度器恢复路径绕过 typed Lane projection。这只证明 shard-local projection 与 Registry 字段的一致性，不证明
physical READY 恢复、certificate authority 或外部 Profile/Oxia revision authority。
Active state 还要求嵌套 `ReadyCertificateV1` 的 Lane ID/incarnation 与自身完全
一致，避免“证书内部有效但挂错 Lane”的值通过本地恢复；这仍不是外部
certificate issuer 或 Oxia activation authority。`next_eligible_at` 还必须不早于
当前保留 action、OPEN circuit、Lane backoff 与 executor retry gate 的最大值，
避免 typed projection 在本地先于显式故障隔离窗口重新进入 READY；`READY` 同时
强制 `admission_gate=OPEN`，暂停、断序或关闭状态不能携带可调度 READY 证明。

协议边界也已开始按 Registry 收敛：`ScheduleIntentV1` 及其
`RetryPolicyRefV1`、`AdapterMetadataV1`、`KafkaMetadataV1`、
`CommittedPayloadDescriptorV1` 已提供严格 canonical value codec，覆盖
Schedule 的 Profile/Retry/时间/Delivery/Ordering、inline-versus-committed
payload、Kafka/Pulsar metadata、可选 business/event 字段和 quota version；
`forPrepare` 明确表示 PrepareLargeSchedule 的无 payload 形态。该增量只证明
wire/value 校验；`ScheduleCommandBodyV1` 与 `PrepareLargeScheduleBodyV1` 现在也
按 Registry 写入 Client common fields 1–3，`CommandBodies.*V1` 只作为显式迁移
seam；`PreparedCommand`/`CommandCodec.*V1` 还会把这些 fields 与 outer
message/type/retry identity 逐项比较。`DelayShard` 已把五类 body 分成明确
的运行时边界：Cancel/Reschedule V1 直接进入原子状态迁移；Schedule/Prepare
V1 必须经过显式 `V1ScheduleResolver`，校验 tuple 派生 Lane、payload 投影，
并把 canonical body/tuple 写入 `V1ScheduleBinding` sidecar。缺少 resolver 时
固定返回 `ROUTE_SNAPSHOT_UNAVAILABLE`，不会降级到旧 body；旧
`ScheduleIntent`/`LargeScheduleIntent` 只服务于非 V1 兼容命令。这个 resolver
仍是本地 authority seam，不等于 Profile/Policy/Oxia/真实 Adapter 已接入。
`ProfileBindingActivatePayloadV1`/`ProfileNewBindingClosePayloadV1` 和
`ProfileBindingControlState` 现在也提供了 source-ordered first-binding marker
投影；catalog-backed V1 路径在尚未应用首个 Profile activation marker 时就
会 fail closed，已有 Profile marker 时则在 resolver 前按 activation/close
边界返回对应稳定码，marker 与 System Mutation result 在同一 WriteBatch
持久化并可在 reopen 后恢复。没有 Profile catalog 的旧构造器仍是显式的
legacy compatibility seam。`InMemoryProfileCatalog` 现在
提供 exact immutable semantic/binding/head/protection lookup；签名 control
target、source-ordered activation routing、历史 binding retention 与 provider
verification 仍是 release blocker。
CommitLargeSchedule V1 也有独立 canonical body 和嵌套
`PayloadCommitProofV1` codec，校验 reservation/message identity、typed Object
Store Profile、tenant scope、optional etag presence、proof ID/signature 后，
通过统一 proof view 复用现有 reservation commit 状态机。Prepare V1 现在还用
required field 15 固定 exact `OBJECT_STORE ProfileRefV1`，并把它随 canonical
body 持久化到 `V1ScheduleBinding`；typed proof/receipt 必须匹配完整 ref，legacy
proof 只能匹配 semantic hash，首次 Commit 与已提交重试都不能绕过该绑定。新增的
`InMemoryPayloadObjectStore` 是一个明确标注的 deterministic local seam：它
只接受 canonical `PayloadReservation`，为同一预约固定 service-owned
container/key/version，按配置的 max-handle-lifetime 与 reservation expiry
的较小值约束短期 handle，再按 Object Store Profile 的 max-bytes、expected
length/SHA-256 和 immutable-if-absent 规则处理上传；旧 handle 过期后，
同一 reservation/kind 才允许重签新的 handle，并把 response-loss 重试
固定为仍在有效期内的同一 opaque handle 与同一签名 `PayloadCommitProofV1`。
`reservationReceipt` 及 receipt-bound handle/upload/attestation 路径还会
校验完整的 service-owned object identity、Source Position、state version 和
trust-set reference，不能仅凭 bare reservation ID 冒充客户端授权；
`InMemoryPayloadObjectStoreTest` 还用 `PayloadProofTrustSet` 验证 proof。
这闭合的是本地预约绑定、handle/上传/attestation 的可测试协议形状，不是
真实 provider credentials、远端 if-absent/immutable 语义、Object Store
availability/ownership evidence、Oxia source-order authority 或完整
reservation migration；这些仍是 release blockers。
`RetryPolicySemanticV1`
现在也能按 Registry 公式重算 semantic hash、生成 typed ref，并拒绝 uncertain/
DLQ 分支和 backoff arithmetic 漂移；`RetryPolicyCatalog` 接入后，V1
Schedule/Prepare 会在 resolver 前校验 exact ref/hash 的 source-position 可见性，
并执行 ordering-mode guard，缺失语义返回
`RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION`。这仍只是 authority seam；policy
publication/source-position activation authority 之外，已接受 binding 的后续
Admission、UNCERTAIN retry 和 reopen 也会在 catalog 可用时重新使用其 immutable
budget，而不会回退到较宽的 shard default；历史 policy binding retention、
Profile/Adapter 运行时绑定和真实 ingress 迁移仍是 release blocker。
本地 source-history catalog 在同一 order token 时还要求完整 canonical
position bytes 一致，避免同一 offset/ledger-entry-batch 的 metadata 变体提前
获得 policy visibility。
业务 `PublishOutcomeBody` 与 evidence-resolution 现在还会在 catalog 可用时
保留 full-shape `RetryDecisionV1` 的 policy/cause/domain，并在 source-ordered
状态变更前重算 exact policy ref、checked retry deadline 和
`RETRY_JITTER_V1(MESSAGE_PUBLISH)`；错误 jitter 被拒绝，正确值才进入本地
timeline/state transition。旧的 opaque `UNKNOWN` placeholder 仍保留兼容路径。
Canonical V1 Admission bytes 已随 attempt ledger 保留，并为该重算提供
trusted `firstAttemptAt`。新 source-applied Admission 现在同时写入 V2
attempt ledger 的独立 typed `firstAttemptAt`/`retryDeadline`；catalog 可用时
该 deadline 再与 immutable Retry Policy 重算值交叉校验，catalog-less shard
至少以 message expiry 作为安全上界。旧 opaque V1 ledger 仍只走 structural
compatibility path，不能在没有 authoritative Admission replay 时补写窗口。
DLQ domain、外部 policy publication/activation 和历史 retention 仍是
release blocker。
Payload proof trust-set 也已补齐 canonical verifier-key list、semantic
hash/ref、Ed25519 raw-key projection 和本地 source-time validity-window
校验；`PayloadProofTrustSetControlState` 现在保留严格 source-ordered
activation/issuance-close markers，在 close 后阻止 first-seen issuance、同时
保留 historical verification 语义，`DelayShard` 以 catalog seam 校验 semantic
ref 后把 marker/result/cursor 原子写入 `meta_cf`。Oxia control authority、
trust-set catalog durability、签名 key/ACL 和历史 key retention 仍不能由该
local projection 自行推断。
对应的 `ControlReasonV1`、trust-set activate/issuance-close payload branches
也已按 Registry 严格解码；这些本地 marker apply 仍没有被误报成已经接入
Oxia control authority。
Lane PAUSE/RESUME/BREAK/CLOSE 的 ApplyShardControl projection 现在也复用
canonical `ControlReasonV1` 与 `AcknowledgementSetV1` decoder，不再用只检查
字段长度/哈希长度的浅层校验接受未知 reason kind 或 malformed optional
entries；`PayloadProofControlPayloadV1Test` 覆盖该 fail-closed 路径。

Managed Kafka/Pulsar ingress 在 Producer ownership 前拒绝无效 physical attempt；
transport exception、空结果、failed stage、CompletionStage callback
registration 失败或 malformed receipt projection 现在统一映射为 Registry 的
`ENQUEUE_RESULT_UNCERTAIN`；共享 transport 若误传
`NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED` 或 `NATIVE_ENQUEUE_RESULT_UNCERTAIN`，
也会在 managed projection 边界归一化为 managed stable-code family。只有
`PinnedPulsarNativeSubmissionAdapter` 使用 `NATIVE_ENQUEUE_RESULT_UNCERTAIN`。
`KafkaProduceResult`/`PulsarSendResult` 还在入口关闭
`PERSISTED`/non-persisted 的 stable-code、position 和 canonical identity 组合，
并要求 `DEFINITIVELY_NOT_PERSISTED` disposition 与已登记的
`BROKER_DEFINITIVE_NOT_PERSISTED`/`NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED`
stable code 配对；未知或错配 code 只产生 `ENQUEUE_UNCERTAIN`/
`NATIVE_ENQUEUE_UNCERTAIN`，绝不构造 proof，
避免把开放 result 变成 queued/proof。该分支映射由 `AdapterIngressTest` 覆盖，
包括 query-boundary projection failure、canonical identity/physical-attempt
rejection、managed
null-result 和 native-code 泄漏，避免把 managed Command 的 retry contract 误标成
native submission；malformed result 只作为 bounded `INTEGRITY_ERROR` diagnostic，
不是 non-persistence proof。错配 definitive-code 的 Kafka、Pulsar 和 native
回归向量以及 Kafka/Pulsar callback-registration failure 的 uncertain 回归也已覆盖。
managed callback 内部若在已返回的 `PERSISTED` result 上发现 malformed
position/identity，也会收敛为 `ENQUEUE_UNCERTAIN`，不会把异常泄漏成
exceptional Future；该保护与 wire projection 的 malformed-result 降级保持一致。
Submission union 的本地构造边界也已固定：managed/native
`DefinitelyNotQueued` 与 `EnqueueUncertain` 只接受
`StableErrorV1.stage=ENQUEUE`，不能把其它 stage 当作 ingress 结果；公共
`StableErrorV1` 拒绝 `OK` 成功码。`ProtocolCodecTest.stableErrorPinsRegistryRetryabilityAndPreparedRefPresence`
覆盖错误 stage、`OK` 和四个 managed/native branch 构造的 fail-closed 回归，保留
各 branch 对 prepared ref、retryability 与 proof 的既有绑定。真实 Broker proof
和 Producer ownership 仍是外部 release blocker。
Native submission 的 `CompletionStage` callback registration 失败也已收敛为
`NATIVE_ENQUEUE_RESULT_UNCERTAIN`，保留原始 physical attempt；这类异常不能证明
Broker 在 Producer ownership 后没有持久化。

Kafka/Pulsar destination adapter 的 `CompletionStage.handle(...)` 注册失败也
已收敛为 `UNKNOWN / DESTINATION_OUTCOME_UNKNOWN`；若 `toCompletableFuture()`
fallback 也无法注册，则结果带有“physical completion 未观察”标记。它发生在目标
Producer 可能已经取得 ownership 之后，不能泄漏为 exceptional Future，更不能伪造
`DEFINITIVELY_NOT_PUBLISHED`；外层 `BoundedDestinationPublishAdapter` 会因此保留
对应的 zombie/in-flight charge，直到 `PublishCall` 获得物理释放证明。
`DestinationAdapterTest` 的 Kafka/Pulsar callback-registration 回归和
`BoundedDestinationPublishAdapterTest.pinnedAdapterRegistrationFailureRetainsPhysicalCharge`
覆盖这条组合边界。这仍只是 adapter transport-SPI 证据，真实 Broker side-effect
evidence 与 absence classifier 仍是发布门禁。

`PreparedSubmissionAdapter` 也在 managed wrapper 层保留该 fail-closed 语义：
managed adapter 返回 null、同步抛出、异步 exceptional completion，或其
`CompletionStage.handle(...)` 注册失败时，结果固定收敛为 managed
`ENQUEUE_UNCERTAIN`，并保留原始
Prepared Command 与 physical attempt id。包装层不能把可能已经进入 Producer
ownership 的 managed 调用泄漏为 exceptional Future，也不能切换到 native
branch；`NativeSubmissionAdapterTest.preparedSubmissionWrapperRegistrationFailureRemainsManagedUncertain`
和 `NativeSubmissionAdapterTest.preparedSubmissionWrapperExceptionalStageRemainsManagedUncertain`
覆盖注册失败与异步完成失败两条边界。若 physical attempt 本身无效，即使 wrapper 正在异常路径上，
也固定回到本地 `INVALID_PREPARED_COMMAND` definitive rejection，而不构造
缺少 attempt identity 的 uncertain branch；`NativeSubmissionAdapterTest.preparedSubmissionWrapperInvalidAttemptRemainsLocalDefinite`
覆盖该优先级。wrapper 自身现在也在 close 请求上 fence managed branch；close
之后不会再调用注入的 managed transport，而是返回本地 `CLIENT_CLOSED`，native
branch 仍交给其 pinned close gate，且失败 teardown 可由后续 close 重试。
`NativeSubmissionAdapterTest.preparedSubmissionAdapterFencesManagedSubmissionAfterClose`
覆盖该 branch/lifecycle 边界。这仍是本地 transport-SPI 证据，不等于真实
Broker response attestation。

`EmbeddedDelayService.enqueueOutcomeV1` 对 queued 与 uncertain 两个需要
physical attempt 的分支也先执行同一 nonzero 16-byte 校验；null、长度错误或全零
输入固定映射为本地 `DEFINITELY_NOT_QUEUED(INVALID_PREPARED_COMMAND)`，不会让
`CommandQueuedReceiptV1`/`EnqueueUncertainV1` 构造器异常穿透，也不会产生无 attempt
identity 的 uncertain union。Queued receipt 的 query-boundary/ack projection
失败则保留同一 physical attempt，收敛为 `ENQUEUE_UNCERTAIN`，并只携带 bounded
`INTEGRITY_ERROR` diagnostic；已经进入本地队列的命令不能因 receipt projection
失败而伪造 definitive rejection。`EmbeddedDelayServiceTest.embeddedIngressProjectsAllManagedOutcomeBranches`
覆盖 queued/uncertain、invalid attempt 和 malformed boundary 投影。这仍只属于
embedded conformance seam。

V1 managed submission 现在还在 Producer ownership 前强制执行
`CommandCodec.encodeFrameV1/decodeFrameV1`：`PinnedKafkaCommandIngress`、
`PinnedPulsarCommandIngress` 的 `enqueueOutcomeV1` 和
`PreparedSubmissionV1` 不再接受 legacy compatibility body；
`AdapterIngressTest.kafkaV1WireRejectsLegacyBodyBeforeTransportOwnership` 与
`PreparedCommandV1Test.managedPreparedSubmissionRejectsACompatibilityBody` 覆盖该
fail-closed 边界；同一边界由
`AdapterIngressTest.pulsarV1WireRejectsLegacyBodyBeforeTransportOwnership` 对
Pulsar 复核。`CommandQueuedReceiptV1.PreparedCommandRef` 也只从
`encodeFrameV1` 派生 frame digest；`ProtocolCodecTest.commandQueuedReceiptRejectsCompatibilityCommandBody`
证明 legacy body 不能被标成 V1 `ProtocolTuple`。旧 `enqueue()` 路径仍明确保留为
compatibility seam，不被计入 V1 submission/receipt 证据。

Target publish 的本地 transport 结果现在也在 adapter 边界执行 closed-product
校验：`PUBLISHED` 必须携带非空 delivery identity、非空 side-effect evidence、
`StableCode.OK` 和非负 Broker persistence time；若返回 pinned Broker resource，
必须同时携带完整 `uint32` physical partition；`UNKNOWN`/`DEFINITIVELY_NOT_PUBLISHED`
不得携带成功码、delivery identity、persistence time 或 Broker resource。证据是
`DestinationPublishResult` 与 `DestinationAdapterTest` 的非法组合回归。这只收紧
transport result 的本地输入边界；physical evidence journal、Lane/Worker/target
cluster admission 和真实 Broker outcome proof 仍是 release blocker。

Kafka/Pulsar target resource 现在也在 request 之前拒绝非 canonical cluster/topic
identity，避免把无法成为合法 `BrokerResourceIdentityV1` 的文本交给 Producer；
`DestinationAdapterTest.targetResourcesRejectNonCanonicalBrokerIdentityText` 覆盖
该构造期边界。

公开的 Kafka/Pulsar ingress、managed target 和 native target request value 现在在
自身构造期重复执行同一 canonical UTF-8/NFC identity fence，不能通过直接构造绕过
resource/profile 边界把 decomposed 或 malformed text 交给 transport。Pulsar native
request 还拒绝全零 `nativeDeliveryId`；`AdapterRequestIdentityTest` 覆盖五个 request
branch 的 direct-constructor rejection。

物理 admission registry 现在对同一 target cluster identity 使用相同的
canonical UTF-8/NFC fence；非 canonical 文本不能先进入 Worker/cluster 容量域，
避免 Unicode 等价字符串被拆成两个独立的 hard-cap accounting key。
`DestinationPhysicalAdmissionTest.targetClusterIdentityRejectsNonCanonicalText`
覆盖该构造期边界。

After `f4899d47`, the Worker physical cap has an executable composition
identity. The registry's closed `WorkerSingleton` set binds one exact
`DESTINATION_PHYSICAL_ADMISSION` instance, and the only public
`BoundedDestinationPublishAdapter` constructor requires that shared registry,
the exact pool and a caller-owned executor. The no-registry/default-executor
constructors are adapter-package test seams. Reflection, same-pool reuse and
foreign-pool rejection regressions prove that a Worker cannot multiply its
request/byte cap by constructing one full-cap pool per adapter through the
cross-package production API. This remains local identity/accounting evidence;
real bounded executor provisioning, Producer/channel ownership, teardown
attestation, Oxia grants and benchmark limits remain OPEN.

Kafka destination request 还在自身构造边界固定
`actionAt=deliverAt`。提前 action 只属于有固定 lead、Broker visibility guard
和能力位证明的 Pulsar handoff；`KafkaDestinationRequest` 遇到提前 action 会在
Producer ownership 前拒绝，`DestinationAdapterTest.kafkaDestinationDoesNotInvokeTransportForEarlyActionAt`
证明不会调用 transport。这与主设计的“`deliverAt` 是消费者最早可见时间”及
Kafka managed 时间关系保持一致；真实 Broker timing/target authority 仍需
release conformance evidence。

Pulsar managed target adapter 现在也默认只接受 ordinary `actionAt=deliverAt`。
只有显式传入 `PulsarDestinationTimingPolicy.certifiedHandoff(fixedLead)` 时，才会在
Producer ownership 前接受唯一的 `actionAt=deliverAt-fixedLead`；错误 lead、下溢和普通
policy 下的提前 action 都归一化为 `DEFINITIVELY_NOT_PUBLISHED/INVALID_METADATA`，且不调用
transport。这个 policy 只是低层 adapter 的 fail-closed guard，不能替代上游 immutable
Destination/Delivery Capability Profile、Broker visibility guard 或发布 authority；
`DestinationAdapterTest.pulsarDefaultTimingPolicyRejectsEarlyActionBeforeTransport` 和
`DestinationAdapterTest.pulsarCertifiedHandoffRequiresTheExactFixedLead` 覆盖了该局部边界。

`DelayShard` 的 Schedule projection 现在也保留了同一条 immutable timing 关系：
`ProfileCatalogV1ScheduleResolver` 从 exact Destination Profile/Delivery Capability
推导固定 Pulsar handoff 的 `actionAt`，`MessageRecord` 和
`TimelineWorkRef` 持久化该值，`TimelineCandidate` 的 READY projection 使用
`max(actionAt,retryEligibility)`，而 `ORDERED` key 继续使用业务 `deliverAt`。
catalog-less、普通 managed 和旧 embedded resolver 仍明确归一化为
`actionAt=deliverAt`；`ProfileCatalogV1ScheduleResolverTest`
`derivesCertifiedPulsarActionAtFromImmutableProfileAndCapability` 与
`DelayShardTest.resolvedActionAtIsEarlierThanDeliverAtButOrderedKeyKeepsBusinessVisibilityOrder`
、`DelayShardTest.resolvedActionAtScratchIsBoundToTheScheduleMessageDuringReadyProjection`
覆盖本地 projection。该项不替代 authenticated Profile publication、Broker
visibility guard 或真实 Producer/target timing evidence。

本地 `DestinationPhysicalAdmission`/`BoundedDestinationPublishAdapter` 现在把
target 请求的 physical request/byte charge 作为显式 reservation：Worker 和 target
cluster hard cap、每 Lane cap 以及所有其它 READY Lane 的 committed minimum 都在
同一 gate 中检查；每次 Admission 还必须能纳入当前 active charge 全部变成
zombie 的 request/byte 最坏向量，否则在调用 delegate 前以 `ZOMBIE_CAPACITY`
拒绝；logical callback 超时只能把 reservation 标为 `ZOMBIE`，达到
Lane zombie cap 立即阻止该 Lane 的新 Admission，直到 physical release 后显式清除
block。delegate stage 完成（包括 `UNKNOWN`）才释放 request/byte charge；capacity
拒绝不会调用 delegate。Release 现在先检查 Lane/cluster/Worker/zombie 四个 accounting
bucket，再一次性扣减；underflow 会保留 reservation active，回归证据为
`DestinationPhysicalAdmissionTest.zombieReleaseUnderflowDoesNotPartiallyDecrementActiveCharge`。
`DestinationPhysicalAdmissionTest` 与
`BoundedDestinationPublishAdapterTest` 覆盖 READY minimum、跨层 cap、identity、
zombie 和 response completion；开启一个尚未 READY 的 Lane 时，其候选
READY minimum 现在只计入一次，`openingLaneCountsItsReadyMinimumExactlyOnce`
覆盖恰好填满 Worker/target-cluster 最小保护容量的边界。`BoundedDestinationPublishAdapter`
不再在 adapter monitor 内同步调用 delegate，而是把调用提交到注入的
Lane/Adapter executor（默认构造器使用 Java 21 virtual-thread executor）；因此同一
adapter 上一个永久阻塞的同步 metadata/send 调用不会阻塞另一个健康 Lane，且
`blockingDelegateCallDoesNotBlockHealthyLane` 覆盖了该隔离边界；executor 拒绝会在
delegate 尚未取得 ownership 前归一化为 `UNKNOWN` 并释放 reservation。Pinned
delegate completion 现在先释放 physical reservation，再完成逻辑 outcome，因而
调用方观察到完成结果时，`activeRequests`/byte accounting 已经同步收敛；同一
回归也覆盖 callback-registration 已安装后报告失败的幂等 release race。
如果执行器已经接受任务后在内联 delegate 或回调路径抛出 fatal `Error`，wrapper
通过 task-start 围栏保留该未知 physical charge；不能把这种 accepted-task failure
误判为 pre-ownership rejection。回归证据为
`BoundedDestinationPublishAdapterTest.inlineDelegateFatalFailureRetainsPhysicalChargeAfterTaskWasAccepted`。
destination adapter 对同步 transport exception 或空 stage 也不假定 ownership
已结束，而是返回带“physical completion 未观察”标记的逻辑 `UNKNOWN`。若底层
`CompletionStage` 的 callback registration（包括其 `toCompletableFuture()` fallback）
都失败，则只归一化逻辑结果为 `UNKNOWN`，同时把 reservation 保留为
`ZOMBIE`（zombie cap 已耗尽时保留为 in-flight），直到 `PublishCall` 的物理释放
证明或 fenced teardown；不能把 registration failure 当成 physical completion。该组件只是进程内可重建的资源闸门，尚未接入持久
`ActiveLaneState`/`ReadyCertificate`、Owner/Lease/Oxia authority、真实 channel
teardown 或 Broker evidence journal，因此不能宣称 production admission 已闭合。

Adapter close 也已按同一 fail-closed 原则收敛：第一次 close 请求立即阻断
新的 ingress/submission/publish，但底层 channel/Producer close 抛错时仍保留
“未完成”状态，后续生命周期调用可重试 native teardown；只有底层成功后 close
才成为永久幂等。`CloseGuardTest` 与
`DestinationAdapterTest.destinationCloseFailureCanBeRetriedWhileAdapterRemainsFenced`
覆盖该本地 fence/retry 证据。它不等于 Broker-side cancellation、physical charge
release 或生产 channel quiescence；这些仍由外部 teardown/evidence gate 证明。

该 close gate 现在还把每次同步 transport invocation 的接受与 close 请求放在同一
线性化边界：`CloseGuard.invokeIfOpen` 在同一 monitor 内登记 accepted invocation，
然后才在 monitor 外执行 transport，不再允许独立读取 `isClosed()` 后在 gate 外调用
transport。close 前已接受的 invocation 可以在 close 请求后完成，并继续按
UNKNOWN/physical-charge 规则处理；close 线性化后则不能再开始新的 transport call。
`CloseGuardTest.acceptedInvocationDoesNotLetCloseAdmitASecondTransportCall` 覆盖
accepted-before-close 的生命周期。gate 不持有 adapter monitor 执行同步 transport，
因此永久阻塞的调用不会阻止 close fence 或 teardown retry；Bounded wrapper 仍把
阻塞 transport 放在 Lane/Adapter executor。

本地 physical-admission registry 另有明确的 Lane teardown 边界：只有 READY
已关闭、所有 physical/zombie charge 已清零且 exact `laneIncarnation` 通过 fencing
时才可 unregister；旧 channel 的迟到 callback 不能删除新 registration。
`DestinationPhysicalAdmissionTest` 覆盖 READY/残留 charge/stale incarnation
拒绝和 quiescent replacement。该证据只覆盖进程内可重建资源回收，不替代
source-ordered retirement、Oxia grant、terminal guard、Recovery Floor 或真实
channel teardown authority。

Scheduler registry 也已补齐同一生命周期证据：terminal gate、empty queue 和
exact Lane incarnation 通过后才允许移除；`PersistentLaneScheduler` 在同一个
projection WriteBatch 清理 ring/fairness/discovery 状态，写失败会恢复内存
registration 和精确的 active-ring membership，原先处于 ring 外的
BLOCKED/terminal Lane 不会被回滚注册步骤重新激活。`LaneSchedulerTest` 覆盖
stale identity、pending work、restart 后的空 projection，以及
`failedPersistentUnregisterDoesNotReactivatePreviouslyInactiveLane`。该证据只证明
bounded local index 回收，不替代 terminal guard、Oxia 或 source-ordered retirement
authority。

`PublishOutcomeBody.encodeInitial` 和
`PublishOutcomeBody.encodeEvidenceResolution` 现在复用 Registry 的 common fields
1–3，并在返回前执行本地 decode round-trip；初始 Outcome 的
`PUBLISHED`/`NOT_PUBLISHED`/`UNKNOWN` side-effect/disposition/stable-code/retry
组合因此不会由调用方随意拼出，definitive transfer 必须是 canonical
`ChargeVectorV1`，Evidence Resolution 的 cursor 必须是 typed canonical
`EvidenceCursorV1`。Apply 现在还要求 definitive Outcome/Resolution 的 transfer 与
对应 Admission ledger 保留的 charge 做 canonical byte-equality；不一致写入
`REJECTED(STALE_SYSTEM_MUTATION)`，不改变 attempt、message、timeline 或 quota，
而 `UNKNOWN` transfer 仍不参与 release。`PublishOutcomeBodyTest` 与 `DelayShardTest`
覆盖编码器、非规范 ChargeVector、typed cursor、transfer mismatch 以及
source-ordered close/requeue；这仍只是 canonical
body codec 和本地 transition seam；当前 local transition 还验证了已 admitted
generation 在 Close marker 后收到 definitive `NOT_PUBLISHED` 时固定写入
`LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED` 并停止 retry。它不等于签名服务、真实
Broker evidence、strong-capability retirement 或 production outcome authority 已完成。
Activation 还从 durable `PUBLISHING`/`UNCERTAIN` ledgers 重建 Registry
`meta/QUOTA` class-2 的 canonical aggregate 与 exact 66 维 outcome vector；
per-Lane class-3 map 提供 dimensions 1--17，open-attempt ledger 提供 outcome
dimensions 9--15，外部/物理 dimensions 18--66 在本地兼容投影中保持 zero。
兼容 shard 即使没有 immutable capacity envelope 也不会在重启后丢失这个本地
vector，绑定 envelope 时再校验其持久 grant-bound projection。旧 class-1
`ShardQuota` 与旧 class-2 `OutcomeReserveUsage` 只读用于迁移/校验，新 mutation
写 canonical class 2 并清除 stale class 1。这仍不等于外部 reserve authority。
Apply 同时验证 operation-specific logical identity：initial Publish Outcome 必须使用
body 中的 `PublishAttemptId`，Evidence Resolution 必须使用
`SHA-256("nereus-delay-evidence-resolution-logical-id-v1\0" || PublishAttemptId || evidenceId)`；
身份不匹配写入 `REJECTED(UNAUTHORIZED_SYSTEM_MUTATION)`，并在 source-ordered
路径中保持 attempt、message、timeline 与 quota 不变。`DelayShardTest` 覆盖 Publish
Outcome 与 Evidence Resolution 的错误 identity fence 以及后续正确 identity 的
source-ordered apply。
Initial Publish Outcome 现在还把 admitted ledger 中的完整 canonical
`OwnerIdentityV1` 与外层 Owner 做 byte-equality fence；同 epoch 但 deployment、worker
或 lease-fencing digest 不同的作者不会仅凭 epoch 命中旧 attempt。若新 Owner epoch
无法直接命中旧的 inflight key，bounded lookup 只为 Registry 规定的
`UNKNOWN + OWNER_FENCED + RECOVERY_FIRST_SEND_UNCERTAIN + UNCERTAIN_HOLD` recovery
tuple 重新绑定原 admitted ledger，并保留原 epoch key。当前 Owner 的 Oxia/guarded
recovery 资格仍在本地 shard 之外验证；任何跨 Owner definitive 或 scheduled Outcome
都拒绝。
同一关闭边界下的 `UNKNOWN` 结果保留 `UNCERTAIN` obligation 且不创建新的
`UNCERTAIN_RETRY` timeline；后续 Resolve retry 仍由 closed-Lane gate 拒绝。

Durable attempt-ledger charge projection now has a canonical-shape fence: when
retained `admissionBytes` starts with the Registry common-body field-1 tag
(`0x0a`) but `PublishAdmissionBody.decode` fails, `DelayShard` fails closed;
only arbitrary pre-V1 synthetic adapter bytes use the bounded zero-charge
compatibility projection. `DelayShardTest.malformedCanonicalAdmissionLedgerDoesNotDowngradeToZeroCharge`
proves that the malformed canonical-looking ledger does not create `PUBLISHING`
state or an open attempt. This is local integrity evidence, not authenticated
source-ordered Admission authority or external grant evidence.

The embedded Admission boundary now applies the same canonical fence to valid
V2 ledgers: before a `PUBLISHING` WriteBatch, the retained body is compared
with the ledger's attempt/generation/message/Claim/Lane/Owner/Store/prepared
hash/attempt number, owner generation and Message timing, and its Lane
incarnation must equal the current durable Lane. The regression
`DelayShardTest.canonicalAttemptLedgerRejectsStaleLaneIncarnationBeforePersistence`
shows that a body/ledger pair from a stale Lane is rejected without advancing
the Source Position or creating an attempt. Opaque V1 ledgers remain a
compatibility seam and are not locally upgraded without authoritative
source-ordered replay; external source and Owner authority remain release
gates.

同一 logical-identity fence 现在也覆盖三个此前容易被签名 envelope 掩盖的入口：
`PUBLISH_ADMISSION_V1` 必须使用 body 的 `PublishAttemptId`，
`CLAIM_RESULT_V1` 必须使用 body 的 `ClaimId`，而 `EXPIRE_GENERATION_V1`
必须使用 Registry §4.8 规定的
`SHA-256("nereus-delay-expiry-logical-id-v1" || DelayMessageId || u32be(generation) || i64be(expireAt))`。
任一身份不匹配都会在 handler 状态变化前持久化
`REJECTED(UNAUTHORIZED_SYSTEM_MUTATION)` 和该 Source Position；Admission、
Claim、expiry 的 message/timeline/quota 不被改变。`DelayShardTest` 的
`sourceOrderedPublishAdmissionPersistsAttemptAndMutationResultTogether`、
`sourceOrderedExpireMutationAtomicallyClosesScheduledGenerationAndDedupes` 和
`sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline` 覆盖错误身份
拒绝以及随后正确 mutation 的 source-ordered apply。

DLQ Export Result 的 canonical body 已校验 `ChargeVectorV1`，而 configured
`DlqExportRecord` 现在持久化 policy-derived retained charge projection；
`NOT_CONFIGURED` terminal record 固定保留 all-zero projection，legacy v1 record
也按 zero-charge 兼容解码。Source-ordered apply 要求每个 callback 的 transfer
与该 outbox projection canonical byte-equal；不一致写入
`REJECTED(STALE_SYSTEM_MUTATION)`，不改变 outbox state 或 source position
边界，且 `UNKNOWN` 仍不会释放 charge。`DlqExportApplyTest` 覆盖 non-zero
retained charge 的成功/不匹配结果、legacy decode 与 outbox 原子推进。真实
policy publication、external charge authority、adapter evidence 和 Object Store
ownership 仍是 release blocker。成功应用还保留 body 的 `stable_code` 到
`SystemMutationResult`；`DLQ_EXPORT_OUTCOME_UNKNOWN` 的 source-ordered 回归
覆盖该结果可见性。

DLQ `terminalRevision` now follows the Registry's complete nonzero `uint64`
identity boundary through `DlqExportRecord` IDs/bytes and
`DlqExportResultBody` parsing; zero remains invalid while high-bit patterns are
preserved. `DlqExportRecordTest` and `DlqExportResultBodyTest` cover the
high-bit vectors. Local terminal-state arithmetic remains bounded separately;
external DLQ policy and provider authority remain release evidence.

The same `DLQ_EXPORT_RESULT_V1` parser now closes the nested retry boundary:
`RetryDecisionV1` must use the exact canonical field sequence and its nested
`RetryPolicyRefV1` is decoded (not merely accepted as nonempty bytes), including
the complete raw nonzero policy version and 32-byte semantic hash. The parser
also enforces the local `first_attempt_at <= next_retry_at <= retry_deadline`
interval (and rejects a deadline before the first attempt), so a structurally
valid retry cannot schedule outside its own decision window. Negative vectors
are in `DlqExportResultBodyTest`; policy publication and source-ordered
authority remain external evidence. The typed decision is now retained by the
body codec, and a `DelayShard` with an exact source-position Retry Policy
catalog rechecks the `DLQ_EXPORT` domain, pinned ref, terminalization
`firstExportAt`, checked deadline, physical-attempt budget, duplicate permission
and deterministic next-retry jitter before persisting the outbox transition;
catalog-less and legacy bindings remain structural compatibility seams.
`DlqExportApplyTest.catalogBackedDlqOutcomeRecomputesPinnedPolicyBeforePersisting`
covers the exact-ref rejection followed by a valid source-ordered outcome.

The business `PublishOutcomeBody` retry decoder applies the same exact
`RetryDecisionV1` field sequence and first-attempt/deadline interval fence as
the DLQ parser. Unknown nested fields and an out-of-window `next_retry_at`
therefore fail closed on both outcome paths; `PublishOutcomeBodyTest` covers
the two rejection vectors.

`PublishEvidenceV1`/`ExternalDeliveryIdentityV1` 进一步把 Registry 的
`PublishEvidenceV1` 公共字段、kind 对应 oneof 分支、verification-status 语义、
owner identity 和 domain-separated `evidence_id` 固定在一个共享 codec 中；
`PUBLISH_OUTCOME_V1` 与 `DLQ_EXPORT_RESULT_V1` 不再只接受任意非空 nested bytes。
`ChannelResourceIdentityV1`/`CredentialUseLeaseV1` 现在把 channel-bearing
absence/non-submission 分支的 Adapter/target branch、strong-capability evidence
resource presence、producer digest、credential binding and destination-channel
holder-scope checks 收敛到同一 canonical implementation；Publish Admission、
Ready Certificate 和 Evidence codec 复用该边界，且 Admission/Ready Certificate
拒绝 credential binding drift 或 certificate 超过 protected Channel lease 的
有效期。当前分支检查覆盖 canonical shape、typed cursor、Broker/Channel/Profile
nested identity 和 owner 匹配，但
真实 adapter 的 authenticated response、lease protection CAS/TTL authority、
retention barrier、external proof ownership 仍是 release blocker。
其中 `OperatorAttestationEvidenceV1` 的 verifier 槽位也已强制为
`ProfileKindV1.EVIDENCE_VERIFIER`，并由 `PublishEvidenceV1Test` 覆盖错误
Profile kind 的负向回归；Profile 注册、key activation 与签名 authority
仍属于外部 release gate。
`LaneTerminalGuardV1` 的两个 Profile 槽位也已在构造和 decode 路径强制为
`DESTINATION`、`DELIVERY_CAPABILITY`，由 `LaneTerminalGuardV1Test` 覆盖
双向错误 kind；typed Active/Terminal 路径还由
`CanonicalLaneTupleV1` 强制 tuple 的 Registry shape 和 Profile
id/version/semantic-hash byte projection。该 parser 不代替 resolver 或外部
authority，兼容层仍可保存 resolver 提供的 opaque bytes。

`ChannelResourceIdentityV1` now preserves the complete raw unsigned patterns for
its physical `channel_generation`, optional `evidence_generation`, and
`credential_binding_generation` fields, matching the raw `EvidenceCursorV1`
generation boundary. The same full-width rule now runs through credential
attestation/binding, Head/protection, use lease, Ready Certificate, native
capability snapshot, rotation request/result, and Profile control projections:
zero is invalid, but a host signed-integer high bit is valid. Head/protection
revisions are nonzero complete raw `uint64` identity values; other local
control versions remain bounded positive counters. The independent Broker
resource-guard configuration generation also preserves raw nonzero `uint64`
bits. High-bit coverage is provided by
`CredentialBindingV1Test`, `ProfileControlRequestV1Test`,
`ControlResultCodecTest`, `ChannelResourceIdentityV1Test`,
`ProtocolCodecTest`, and the Publish Admission/Ready Certificate fixtures.

The nested Ready Certificate Broker attestation and config generations now
also preserve complete raw `uint64` patterns through the shared
Admission/certificate decoder; `ReadyCertificateV1Test` covers both high-bit
values. The external Broker rollout remains the authority for their meaning.
The shared decoder also parses the nested `ActivationBarrierV1` and every
repeated `EvidenceCursorV1`, rather than treating those fields as opaque
non-empty bytes. It requires at least one cursor and rejects non-canonical
nested branches plus unsorted/duplicate cursor identities before either the
Admission body or the public `ReadyCertificateV1` wrapper can expose the
certificate; direct-parser rejection vectors are in `ReadyCertificateV1Test`.

`PublishAdmissionBody` 还把 `PreparedPublishDescriptorV1` 的 adapter kind、固定
adapter encoding version（并要求 immutable Destination Profile 也 pin 版本 `1`）、target resource、physical partition 与嵌套
`ChannelResourceIdentityV1` 做 exact equality，要求 `business_metadata` branch
与 adapter 一致，并要求 descriptor Destination
Profile 与 channel credential lease 的 ProfileRef 一致；descriptor 的两个 Profile
kind 也按 Registry 字段位置固定。`PublishAdmissionBodyTest` 的
`rejectsDescriptorAdapterIdentityDrift` 覆盖哈希有效但 adapter identity 漂移的
fail-closed 路径。这是本地跨对象解析证据，不替代 Profile semantic publication、
authenticated channel registration 或真实 Producer admission authority。

Descriptor timing 的结构解析现在允许 `actionAt <= deliverAt`，但不把提前值当作
许可：没有 Profile catalog 的兼容 shard 只接受 ordinary managed，配置 exact
catalog 的 shard 才会按 Destination/Delivery Capability semantic bytes 验证固定
Pulsar handoff lead、capability bit、target resource、target partition policy 和
checked subtraction。
`PublishAdmissionBodyTest.retainsCertifiedHandoffTimingForProfileSemanticValidation`
与 `acceptsOnlyThePinnedPulsarHandoffLead` 覆盖这一分层边界；真实 Profile 发布、
Broker guard attestation 和 Producer admission authority 仍是 release blocker。

Admission 的 Broker-time 边界也已有本地闭合证据：`DelayShardConfig` 暴露
`maxIngressBrokerTimestampDivergenceMs` 与 `maximumAdmissionMutationEnqueueAgeMs`，
`DelayShard` 在 source-ordered apply/replay 调用 `PublishAdmissionBody.requireBrokerTiming`，
检查 `bp + divergence < min(expireAt, readyCertificate.validUntil)` 及 decision interval
距离上界，并对加减溢出 fail closed；`PublishAdmissionBodyTest` 覆盖距离、expiry、
divergence 和溢出拒绝，`DelayShardConfigTest` 覆盖正式字段与兼容构造器。该证据只证明
本地状态机和算术 fence，不能替代真实 Broker timestamp certification、capacity artifact
发布或外部 Admission authority。

同一 apply/replay 路径还会在 timing/profile fence 失败时撤销 exact matching live Claim，
并持久化 `STALE_SYSTEM_MUTATION`；`DelayShardTest.publishAdmissionTimingFailureRevokesMatchingClaimBeforePersistingStaleMutation`
覆盖该 no-attempt/no-Producer 前置状态。该测试仍属于本地状态机证据，不代表外部 authority 已部署。

`ResolveUncertainBody` 的 `ATTACH_PUBLISHED_EVIDENCE`/
`ATTACH_NOT_PUBLISHED_EVIDENCE` 分支也不再接受任意 opaque nested bytes，
而是要求 typed evidence 的 Publish Attempt owner 和 verification status
匹配。`ATTACH_PUBLISHED_EVIDENCE` 现在在 source order 中校验 exact current
`UNCERTAIN` obligation；若当前代仍有 timeline/Claim work，会先删除该可逆
work 后 terminalize；若另有 current PUBLISHING attempt，则只移除目标
obligation 并保留该发送；也支持旧代 `terminal_cf` summary 中仍开放的 exact
obligation。当前代与 published terminal、ledger、pending quota、outcome
reserve、mutation result 和 source position 一起原子提交，旧代只更新
terminal summary、ledger、duplicate-risk 和 outcome reserve，不能修改新代；
重复 mutation 返回已持久化结果。`ATTACH_NOT_PUBLISHED_EVIDENCE` 现在也
校验 exact UNCERTAIN obligation 并按 remaining-obligation/all-absent 规则
保留未决工作、保留另一个 current PUBLISHING；若当前是 stale `CLAIMED`，
则与目标 ledger 一起原子撤销为 `UNCERTAIN/NONE` 并推进 Message state version，
或在无剩余 obligation 时
原子化为 definitive retry
及其 closed-lane/budget/expiry terminal 分支；当前 `CLAIMED` work 的撤销与
definitive retry 也由
`DelayShardTest.sourceOrderedNotPublishedEvidenceRevokesClaimAndNormalizesDefinitiveRetry`
以及
`DelayShardTest.sourceOrderedNotPublishedEvidenceRevokesClaimWhenAnotherUncertainObligationRemains`
覆盖；外部 authenticated control/evidence authority 和完整 retry/charge
policy 仍未完成。

此外，Claim 从 `UNCERTAIN_RETRY` timeline 创建时现在冻结实际的
`sourceWorkKind=UNCERTAIN_RETRY`，不再根据 retry timestamp 误判为
`DEFINITIVE_RETRY`；完整 Claim materialization/recovery 仍是独立的待完成边界。

`ClaimResultBody` 现在还要求 field 20 `ChargeVectorV1 transfer` 与
`ClaimPreconditionV1.claimed_charge` 做 canonical byte-equality。这样签名的
permanent pre-send callback 不能在 source-ordered apply 时偷偷替换 Claim 的
reversible charge projection；`DelayShardTest.sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline`
覆盖不一致 vector 在 parser 和 source-ordered apply 层的 fail-closed 拒绝，且确认
Shard 状态、quota 和 timeline 不被改变。该项只是本地 replay-stable charge fence，
不能冒充完整的 grant policy、external charge authority 或 materialization/recovery
accounting。

System Mutation 统一入口现在也把 handler 的 checked-arithmetic overflow
转换为 `STALE_SYSTEM_MUTATION`，持久化该 mutation 的拒绝结果和 Source
Position，而不是让异常逸出并留下可重复应用的 source gap；
`DelayShardTest.systemMutationStateVersionOverflowPersistsStaleResult` 用
`Message.stateVersion=Long.MAX_VALUE` 覆盖这一边界。该路径仍只保护本地
source-ordered state machine，不能替代外部 writer/lease authority。

Evidence branch validation also checks the adapter-specific target resource
and cursor/channel branch, so a Kafka evidence envelope cannot carry a Pulsar
resource (or vice versa); authenticated Broker response and external proof
ownership remain separate release gates.

`OwnedDelayShard` 现在还提供了带 assignment/barrier/source-connection 校验的
统一 `replay` seam，以及兼容性的 `replayCatchup`/`replaySystemMutations`：
Command 和 signed System Mutation 通过 `SourceReplayEntry` 在同一个
source-order stream 中选择分支，每条记录先走同一 shard WriteBatch，成功后才
推进 catch-up cursor，并返回带分支类型的 `SourceReplayOutcome`。对于同一
Command/Mutation 在后续 Source Position 的合法 logical duplicate，outcome 只把
首次 durable result 投影到当前 physical position；RocksDB 中的首次结果锚点不被
改写，canonical position metadata 不一致仍 fail closed。它仍不等同于
真实 Kafka/Pulsar consumer、Oxia session/ephemeral authority、broker assignment/
guard 或 production activation transaction。

为了闭合主设计 §8.4 的 source-turn 约束，当前 seam 还提供
`SourceReplayCursor`、`ReplayTurnBudget` 和 `SourceReplayTurn`：
`replayCatchupTurn`、`replaySystemMutationsTurn` 与混合 `replayTurn` 在每次
写入前同时检查 record count、canonical bytes 和 monotonic elapsed-time cap。
canonical-byte 计费使用 exact source-position bytes 加 canonical NDL1/System
Mutation frame；单项超出 byte cap 会在消费/WriteBatch 前 fail closed。cursor
只保留一条 look-ahead，因此 yield 不会丢失下一条 source record；旧的完整
`Iterable` overload 明确使用 unbounded compatibility budget，不能作为生产
source consumer 的 bounded turn。实现先 `peek()` 并完成 shard WriteBatch，
成功返回后才 `next()`；校验、fencing 或存储失败会让 exact look-ahead record
留在 cursor 上，下一轮可以原样重试。`OwnerLeaseTest` 已覆盖 record-cap 后的
cursor continuation、single-record byte overflow，以及 source-gap 后的 cursor
保留。

The direct replay visibility is now closed as well: all
`OwnedDelayShard.replayCatchup*`, `replaySystemMutations*` and mixed `replay*`
overloads are package-local ownership seams, including fixed-time, bounded and
whole-iterable compatibility forms. `OwnerLeaseTest.directReplaySeamsAreNotPublicProductionApi`
guards that none is re-exposed as a public API. Cross-package production source
and recovery composition must use `SourceApplyWorkClassExecutor`.

Commit `43e61c6` adds `OwnerRecoveryCoordinator`/`OwnerRecoveryTurn` to compose
that local replay seam with the takeover lifecycle. The coordinator starts
only after a caller supplies an already selected and locally validated Store,
performs the context-bound `CATCHING_UP` CAS once, yields exactly one bounded
mixed replay turn per call, and performs strict control-snapshot activation only
after the cursor is exhausted. `OwnerRecoveryCoordinatorTest` proves that a
record-cap yield leaves the authority in `CATCHING_UP`, that the next turn is
the one that activates, and that a clock failure before the first CAS fences
the local Owner. This closes local ordering/orchestration drift; it is not a
claim of Source Assignment publication, Oxia session creation, checkpoint or
Object Store selection, Broker guard, Lane evidence, or production Worker
integration.

The follow-up `0798f73` closes the remaining local queue-boundary drift in that
seam: recovery no longer invokes direct mixed replay. Each `CATCHING_UP`
entry is an exact shared `SOURCE_APPLY` action; a fairness wait is surfaced as
a retained task, and cursor/`lastCatchupPosition` advancement occurs only
after the action outcome and look-ahead identity are proven. The focused
coordinator regression covers the occupied-class wait before continuation.
This is still local orchestration evidence; it does not promote the external
source, Oxia, checkpoint, Object Store or Worker integration blockers to PASS.

V1 的 assignment 接管路径现在还会显式 pin `SourceReplaySuccessor`：同一
canonical Source Position 的 broker redelivery 可以由 durable apply 幂等处理，
但任何后继位置都必须由 adapter proof 判定为 immediate successor；内置的
`strictKafka()` 在 offset gap 处 fail closed，`strictPulsarBatchMember()` 只覆盖
同一 batch entry，跨 entry 仍要求真实 Pulsar adapter 提供 successor 证明。
`SourceReplaySuccessorTest` 和
`OwnerLeaseTest.v1CatchupPinsTheAdapterSuccessorAndRejectsAKafkaGapBeforeApplyingIt`
证明跳过的 Kafka record 不会被静默重放。旧的 assignment-only overload 保留为
兼容性 monotonic seam，不能作为 V1 source-gap evidence。
接管 replay 还提供 live-clock overload，并在每条记录前重新检查 lease；长时间
catch-up 中途过期会在下一条记录前 fence，cursor 保留在最后已提交的位置，
不会继续用旧 owner 写入。固定 `nowEpochMs` overload 仅保留给确定性兼容调用，
`OwnerLeaseTest.liveCatchupClockFencesBeforeApplyingAfterLeaseExpiry` 覆盖该边界。
时钟读取本身也是 lease-validity proof：若 injected clock 抛异常或返回负的
epoch-ms，Command、System Mutation 和 mixed 三条 bounded replay 路径都会在
读取 source 之前把 Owner 置为 `FENCED`，保留 look-ahead cursor 和
`lastCatchupPosition`。`OwnerLeaseTest.replayClockFailureFencesEveryReplayPathBeforeReadingSource`
覆盖该 pre-read fence；这仍是本地 fail-closed 证据，trusted-clock、Broker
assignment 和 Oxia lease/session authority 仍是 release gate。
正常 active source apply 现在使用 `SourceApplyWorkClassExecutor`：Command 与
System Mutation 的 task identity 绑定 exact canonical position/frame hash，资源 byte
charge 是两者长度的 checked sum。queue admission 只做本地 strict
assignment/barrier/guard preflight，不读 Oxia、不写 Store；bounded action 运行时才
读取时钟并 reread exact Oxia lease/session，同 identity 续租可更新本地 expiry，
而 owner/epoch/token/session、状态或 expiry 回退都会在 WriteBatch 前 fence。普通
failure 由 Submission outcome 返回，不留下与 Broker cursor 竞争的 generic retry。
`OwnedDelayShard` 的 direct active apply overload 和原始 `DelayShard` delegate accessor 现都是
ownership 包内可见的本地测试/组合 seam；包外调用方不能绕过 work-class、
owner lifecycle 和 lease gate。该收口仍不是真实 Broker consumer/ACK 或 Oxia session 证据。

After `0798f73`, the takeover path is also inside the same `SOURCE_APPLY`
work-class boundary. `OwnerRecoveryCoordinator` no longer calls the direct
mixed `replayTurn`; it submits each exact recovery entry through
`SourceApplyWorkClassExecutor.submitRecovery`, which requires `CATCHING_UP`,
rereads the context-bound lease/clock and source guard before the WriteBatch,
and returns the physical-position-projected outcome. A source cursor is
advanced only after the action is selected and succeeds and the caller-owned
look-ahead still has the same position/frame/guard/generation identity. If
fairness selects another class, the coordinator returns an explicit waiting
task without consuming the source entry; recovery never sends a Broker ACK or
creates a generic process retry. `OwnerRecoveryCoordinatorTest` covers the
waiting and continuation path. This removes the local active/recovery queue
drift; it remains local evidence and does not prove production Broker,
Oxia-session, checkpoint/Object Store, Lane evidence or dynamic IO authority.

After `0261cb5`, the local checkpoint restore path also has an explicit bounded
work-class boundary. `CheckpointRestoreWorkClassExecutor` admits the exact
manifest/resource/optional-pin identity and checked request bytes before the
`CHECKPOINT` queue, repeats the pure validation after queue wait, and keeps the
provider download, complete inventory validation and Store-Incarnation install
inside the selected action. The focused coordinator regression proves that queue
rejection does not call the provider or create staging, while success returns an
explicit restore-owned outcome containing the installed Store. The direct
`CheckpointRestoreCoordinator.restore` seam is package-local for tests/composition;
cross-package Worker production code cannot bypass the queue. This closes local
restore work-class drift only and leaves RecoveryPin/Oxia CAS, Object Store,
Source Assignment/replay and production dynamic IO authority as release blockers.

After `de5601b`, planned-drain final checkpoint creation is also inside the
shared `CHECKPOINT` boundary. `CheckpointDrainWorkClassExecutor` binds the
normalized path, fixed checkpoint identity, exact DRAINING Owner Lease/session
identity, owner clock and deadline before admission, and queue rejection leaves
the Store, checkpoint projection and filesystem untouched. `OwnerDrainCoordinator`
returns an explicit pending task when fairness selects another class, retains the
same DRAINING lease and checkpoint identity, and resumes only the checkpoint/close
continuation on the next call. The selected action rereads the lease before and
after the physical RocksDB image; outcome failure is lease-proven before it is
returned, and close/release is allowed only after success. The focused
`OwnerDrainCoordinatorTest` covers fairness wait, queue rejection plus exact retry,
successful continuation and lease replacement during checkpoint. The old
package-local four-argument constructor is now a no-final-checkpoint compatibility
seam; cross-package Worker production composition must supply the shared registry.
This closes local drain/checkpoint work-class drift only; source quiescence, Oxia
session fencing, external publication and production dynamic I/O attribution remain
release gates.

After `f05290d`, the bottom-most physical checkpoint seams are no longer public:
all `ShardStore.createCheckpoint(...)` and `ShardStore.restoreFromCheckpoint(...)`
overloads are package-local to `io.nereusstream.delay.store`. This makes the design's
cross-package rule compiler-enforced: Worker composition must use the scheduled,
planned-drain or restore `CHECKPOINT` executor rather than calling a RocksDB primitive
directly. `ShardStoreTest` reflects over every declared method with those names and
fails if public visibility returns; the focused RocksDB suite, compilation and
Checkstyle passed. This proves the local API boundary only, not external Oxia/Object
Store authority, Source Assignment integration or production dynamic I/O charging.

After `4b0489e`, the same compiler-enforced boundary covers the scheduled
checkpoint pipeline above the physical primitive. The direct `execute`, `publish` and
`upload` actions on `CheckpointExecutionCoordinator`,
`CheckpointPublicationCoordinator` and `CheckpointUploadCoordinator` are package-local;
only `CheckpointWorkClassExecutor` remains the cross-package production handoff. The
external adapter and authority interfaces remain public and replaceable. A reflection
regression requires all three named methods to exist and be non-public, and the focused
execution/upload tests, compilation and Checkstyle passed. This removes the local API
bypass but does not close real Object Store/Oxia authority or dynamic I/O evidence.

After `d943e0b`, the legacy whole-turn Lane-close materializer can no longer be
called from cross-package Worker code without the strict `GC` handoff. Both the
`LaneCloseMaterializer` class and `runTurn(...)` are package-local; production-facing
composition must submit one exact candidate through `LaneCloseWorkClassExecutor`,
which binds the cursor/batch identity and rereads Owner authority before the Store
primitive. The focused visibility regression plus materializer/executor tests,
compilation and Checkstyle passed. This removes the local no-authority bypass only;
production scanner, Floor protection and Oxia orchestration remain open gates.

After `45b99dd` and `5f1eae3`, Message expiry discovery no longer relies on a
cross-package unbounded `DelayShard.discoverExpiry` call. The strict public
composition path is `ExpiryDiscoveryWorkClassExecutor`: pure local preflight before
queueing, execution-time Owner reread, separate monotonic scan clock and one
record/actual-byte/elapsed budget shared across index and dependent Message reads.
The compatibility count-only overload remains for local callers/tests, but it is not
the production Worker boundary. Returned candidates remain state-neutral and must
pass through `ExpiryWorkClassExecutor`; no new Source Position or competing mutation
authority was introduced. This closes local discovery/work-class drift only and
does not close real Trusted-Time, Oxia, Broker or replay gates.

After `f2c9334`, the same no-bypass rule covers Reservation expiry discovery.
The strict public composition path is `ReservationExpiryDiscoveryWorkClassExecutor`;
it has no external time-cutoff parameter and runs only through `GC` admission with
execution-time Owner reread. `DelayShard` derives the scan boundary from the
persisted TIME_FENCE watermark and shares one actual-byte/elapsed budget across the
index and dependent Reservation reads. Its explicit-cutoff overload remains a
compatibility/test seam for deterministic projection validation, not a production
clock authority. Discovery does not write state or quota; materialization remains a
second exact `ReservationExpiryWorkClassExecutor` action. This closes local scanner
work-class drift only, not production TIME_FENCE/Oxia/Floor/Object Store authority.

After `6c3e8ad`, Lane-close production discovery no longer calls the count-only
`DelayShard.discoverLaneCloseMaterialization(int)` outside the queue. The strict
public path is `LaneCloseDiscoveryWorkClassExecutor`, with pure local preflight,
execution-time Owner reread, a monotonic scan clock and one actual-byte/elapsed
budget shared across cursor and Lane reads. The count-only overload remains a
compatibility/test seam that intentionally fails if more than its declared cursor
bound exists; the production bounded overload leaves additional valid work durable.
Discovery is state-neutral and materialization remains a separate exact
`LaneCloseWorkClassExecutor` action. This closes local cursor-discovery drift only,
not the external close scheduler, Oxia, Object Store or Floor authorities.

After `a0d97cc`, `DelayShard` no longer exposes uncoordinated physical GC writes to
cross-package Worker code. Message identity retirement/compaction, resource
delete-confirmation compaction and Lane terminal-guard replacement remain usable by
runtime-package algorithms/tests only. The visibility regression prevents these
methods from becoming public again without an explicit design change. No replacement
production authority was invented: strict Route retention, Oxia/session, provider
quiescence, grant release, bounded admission and Recovery-Floor orchestration remain
OPEN and must precede any future public GC coordinator.

After `ae1224f`, direct ID-based materialization/revocation also cannot be called from
cross-package Worker code. The strict candidate-based Lane-close and reservation
executors remain the production-facing paths; their queue-time identity and
execution-time Owner checks are no longer bypassable by calling the simpler overload.
This is an API-boundary change only, not new Oxia, Floor or drain authority.

After `03b6031`, cross-package composition also cannot invoke raw Lane gate,
capacity-reserve, Attempt Journal or Publish state transitions. Exact source handlers
inside `DelayShard` continue to use the algorithms, but public production composition
must enter through the existing Control/Publish work-class and future adapter/capacity
coordinators. This removes API drift without claiming the missing external authority.

After `581faba`, the same rule applies to runtime readiness. Cross-package code can
no longer make a Lane schedulable by writing `READY` directly; only a future strict
Lane activator may expose that transition after proving all pinned prerequisites.
This closes a misleading API seam, not the activator implementation itself.

Worker 资源侧现在还提供了本地 `WorkerLoadVector` 与
`WorkerPlacementPolicy`：它们先按完整 committed capacity、固定/transition
demand 以及 owned/open DB slots 做 hard filter，再以 dominant-resource/load
分数处理 stale telemetry、minimum residence、hysteresis 和 checkpoint/replay
movement cost；这只是可复现的评分 seam，不是 Kafka cooperative assignor、Oxia
desired-placement plan 或 Owner Lease authority。

`ShardStoreConfig.maxWriteBufferBytesPerDb` 现在是显式的 Worker 配置，
`ShardStore` 会把它绑定到每个 Column Family 的 RocksDB
`ColumnFamilyOptions.setWriteBufferSize`，并同时绑定到每个 DB 的
`DBOptions.setDbWriteBufferSize`；前者只是单 CF ceiling，后者才是八个
physical CF 的聚合 DB ceiling。进程级 `WriteBufferManager` 继续约束共享
memtable 总预算。共享 `Env` 明确承载进程级 background pool；每个 DB 另外绑定
`maxBackgroundJobsPerDb` 以及非零 `reservedFlushJobs`/`maxCompactionJobs` split，
并在配置不满足 split 时 fail closed。这由
`ShardStoreTest.perDbWriteBufferCeilingMustBePositive`、
`ShardStoreTest.perDbWriteBufferCeilingIsBoundAtRocksDbDbLevel` 和
`ShardStoreTest.backgroundJobSplitMustFitPerDbCeiling` 证明；但不把 DB
memtable option 或每个 DB 的 job split 误报成聚合 WAL/SST/temp 运行时记账。
work-class reserve、真实 JVM/cgroup/rlimit 和 Oxia placement authority 仍是
外部资源证据。

资源审计现在还有一个可复用的本地物理观测边界：`RocksDbUsageSnapshot`
从打开的单 shard DB 读取 live SST、WAL、MANIFEST、L0、compaction-pending
以及不跟随符号链接的实际 DB 文件总量；`RocksDbUsageLimits` 按 shard identity
拒绝重复观测，逐 DB 校验上限，再用 checked addition 校验 Worker 聚合上限和
`FileStore` 对应卷的最小可用空间。`ShardStore.physicalUsage()`/
`requirePhysicalUsageWithin` 与 `RocksDbUsageLimitsTest`、
`ShardStoreTest.physicalUsageProbeAndGuardObserveOneShardDb` 证明了这个本地
guard。`SharedRocksDbResources.startRocksDbUsageMonitor` 现在注册每个打开
Store 的 identity-bound physical source，按 fixed delay 聚合所有 shard DB，并在
Store native teardown 前移除 source；缺失、身份不一致、逐 DB 或 Worker 聚合
上限超限以及 filesystem floor 证据都会复用同一个 sticky runtime gate。
`NativeResourceUsage`/`WorkerNativeResourceLedger` 又提供了互斥的
RocksDB-native bucket attribution（shared block cache、memtable、table-reader
metadata、pinned blocks/iterators、flush/compaction scratch）和独立的
other-native bucket；`SharedRocksDbResources` 在 JNI 创建前预留 shared cache/WBM，
并在对应 native close 后释放。Ledger release 现在先 checked 计算两个 successor
bucket，再移除 allocation identity；underflow 会保留 active reservation 供后续
重试，回归证据为 `WorkerNativeResourceLedgerTest.underflowingReleaseLeavesReservationForRetry`。
Shared resource close 还会在 cache/WBM native teardown 成功后独立重试尚未释放的
shared reservation，并以 `WorkerNativeResourceLedgerTest.sharedResourceCloseRetriesReservationsAfterReleaseFailure`
证明只有两个 reservation 都释放后才完成 Worker close。
`WorkClassScheduler` 对八个冻结 work class
提供 bounded queue/turn record-byte-time caps、`LEASE_FENCE` 首个 bounded turn
抢占和跨小预算 poll 的 preemption-debt yield（连续 fence queue 不会饿死
普通 class），以及 stale-class
选择；`WorkClassResourcePool` 还按 class 保护 non-borrowable record/byte
minimum、把 acquisition checked-sum overflow 转成 closed rejection，并限制
borrowed hold time。`WorkClassEventLoop` 现在把两者绑定在 bounded-turn
边界：任务仍在 queue 中时不占共享 token，任务即将移出 queue 时才取得
exact lease；后续任务 admission 失败会恢复 scheduler projection 并释放前面
已取得的 lease，上一 Turn 未关闭时下一次 poll 会 fail closed。`runTurn` 还在
不持有 event-loop monitor 的情况下执行一个 bounded callback sequence，并在
每个 callback 前后检查 borrowed hold、无论 callback 成功还是失败都关闭所有
lease。`WorkClassDispatcher` 在这一层之上要求八类 handler 完整覆盖，并把
selected task 路由到对应 handler；已经进入 handler 的 task 不做隐式 requeue，
fatal/hold stop 之后从未进入 handler 的 exact suffix 则按原 selection order 回到
各 class 队首。active Turn 还保留 selected queue capacity，避免并发 offer 挤掉
该安全 requeue 空间。`WorkClassExecutionRegistry` 还在 queue admission
之前把 complete class/task/byte-charge identity 与 exact action 绑定：admission
拒绝会撤销注册，成功 action 才删除，已开始的 runtime/fatal failure 保留为
`FAILED` 并只允许 exact explicit retry，从未开始的 fatal suffix 则保持
`QUEUED`。该 registry 只是可从 shard/source/checkpoint authority 重建的进程内
projection，不冒充 durable retry authority。`WorkClassScheduler`
现在会在任何 queue head removal、deficit 扣减或 fairness counter 推进之前
读取用于 `lastServed` 的单调时钟样本；负值/回拨样本只会 fail closed，不会丢掉
仍由 bounded queue 支持的 head，回归证据为
`WorkClassSchedulerTest.invalidClockSampleDoesNotDropHeadBeforeTurnMutation` 和
`WorkClassSchedulerTest.continuousPreemptiveQueueYieldsAcrossSmallPolls`。
一个 bounded `poll` 还把 queue、queued bytes、credits、cursor、last-served 和
preemption-debt 当作一个内存边界；如果在已选中 head 后的后续 clock、选择或
checked arithmetic 失败，整轮 projection 会恢复，未返回的 task 不会丢失。
`WorkClassSchedulerTest.clockFailureAfterAHeadWasSelectedRollsBackTheWholePoll`
覆盖该回滚；clock high-water 可以保守保留，但不会把中断的 poll 记作已服务。
`WorkerRuntimeSafetyGate` 还把新鲜的 JVM/cgroup/FD/filesystem
observation 接入一个 sticky `ACTIVE -> DRAIN_OR_MIGRATE` 门；共享资源的
ownership/restore slots 和 embedded Claim 在门未恢复前 fail closed，只有
显式 empty-drain activation 才能重新开放。Staged envelope 也必须先显式
进入 `DRAIN_OR_MIGRATE`，不能从 `STAGED` 直接激活；
`WorkerRuntimeSafetyGateTest.stagedEnvelopeRequiresExplicitDrainTransition`
覆盖该状态栅栏。`WorkerRuntimeResourceMonitor`
现在提供可关闭的 fixed-delay envelope probe scheduler，并把 probe 异常和
envelope mismatch 路由回同一个 sticky gate；`WorkerRocksDbUsageMonitor`
覆盖了本地 per-DB physical usage observation，但不提供 WriteBatch 的
work-class reserve admission，也不替代真实 checkpoint/compaction 调度和 Oxia
placement authority；这些继续保持 release blocker。两个 monitor 都由
`SharedRocksDbResources` 持有并在 native teardown 前关闭，补上了本地 monitor
生命周期边界。

Control Reserve 的本地投影也已覆盖 Registry 的 class 6：
`meta_cf/CONTROL_RESERVE` 以 `CapacityVectorV1` 持久化 Broker system-writer
reservation，绑定 `NON_OUTCOME_CONTROL` grant identity；class 6 只接受维度
51–53，class 3 排除这些维度，二者合计必须被同一 immutable grant 覆盖。
`DelayShard` 的同步 charge/release 和重开校验已有
`DelayShardTest.systemWriterReserveProjectionIsPartitionedAndPersistsAcrossReopen`
及错误维度 fail-closed 测试。该证据只闭合 shard-local projection，不证明
Route Broker/source-writer 的远端 quota authority、跨 shard placement 或
实际 operation charge。
首次 capacity-envelope binding 也已延后到所有 reserve/quota/obligation
projection 校验成功之后；错误维度导致的失败 open 不会遗留 binding marker，
回归由 `DelayShardTest.systemWriterReserveProjectionRejectsWrongPersistedDimensions`
覆盖。这仍只是本地 open/recovery 原子性证据，不替代外部 grant/placement authority。

当前 `DelayShard` 还把 Registry class-3 `meta_cf/QUOTA` 接入为一个本地兼容投影：
`LaneQuotaUsageProjection` 按 Lane incarnation/usage revision 记录可精确重建的
message、reservation、Lane-slot 以及每个 Claim/attempt obligation 的
`inflight_messages`/`inflight_bytes` 维度；命令及 source-ordered system mutation
与 Registry class-2 canonical aggregate、class-3 map 在同一 WriteBatch 更新，打开/恢复时从
`id_cf`/`meta_cf` 和 `inflight_cf` 的 durable Claim/attempt ledger 重建并逐字节校验，
同时复算 aggregate 的 pending/reservation/Lane-cardinality counts；已存在但数值漂移的
aggregate 会 fail closed，缺失的 aggregate 只在内存回填并等待下一次 source-ordered mutation
持久化。旧 class-1 `ShardQuota` 与旧 class-2 `OutcomeReserveUsage` 只读用于迁移/校验，
新 mutation 会清除 stale class 1。零 field-7
charge 的 legacy/synthetic ledger 按一个 durable record 计数，canonical admission
中的 field-8 attempt bytes 则保留；若运行时发现旧 map 缺失对应 ledger，释放路径
会先从 durable ledger 重建再在同一批次修复。`LaneQuotaUsageProjectionTest` 与
`DelayShardTest` 覆盖 checked arithmetic、Claim/admission、close/terminal/replay/
retirement 路径和 reopen fence。Typed ACTIVE Lane activation now also checks
field-14 `lane_usage` byte equality against the matching class-3 map entry;
missing-map and usage-drift cases fail closed, while the typed-state and map
updates remain coupled in the same source-ordered batch. This closes the local
state/map projection fence. execution beyond local attempt bytes、retained、
evidence 与外部 adapter 维度仍未接入，因此这是 map 的 local compatibility subset，
不是完整 ActiveLaneState、grant revision coupling、Route Broker authority 或多
shard placement proof。退休前会扫描完整 17 维 usage vector；即使未来投影开始
记录 retained/control/evidence 维度而当前 adapter 尚未理解，仍会 fail closed，
不会把带残留 usage 的 Lane 变成 terminal guard，同时原子释放该 Lane 的普通
和 strong-capability cardinality slots。

本地回归还覆盖了同一 `DestinationLaneId` 同时保留旧/新 incarnation 的过渡投影：
`LaneQuotaUsageProjection` 按完整 tuple 精确查找目标 entry，不会因排序中更早的
foreign incarnation 提前报错或误更新错误一代（`LaneQuotaUsageProjectionTest`
`findsExactIncarnationWhenSameLaneRetainsAForeignEntry`）。

Owner Lease 的本地 CAS 投影现在还按 V1 lifecycle graph 拒绝回退状态和
`FENCED -> ACTIVE_FOR_COMMANDS` 复活；允许的前向 acquisition/activation
跳转、fence 和 fenced recycle 都保留。续租响应若改变期望的 lifecycle
state 也会 fail closed，即使 fencing/assignment/session identity 相同，避免
把状态漂移误当作成功续租。`OxiaSyncOwnerLeaseBackend` 现在使用真实 Oxia
Java client 的 durable epoch record、ephemeral lease record 和 version CAS；其
deterministic record surface 由 `OxiaSyncOwnerLeaseBackendTest` 覆盖。
`OxiaRealServiceSmokeTest` 是显式 opt-in 的真实服务验证：在
`NEREUS_DELAY_OXIA_ENDPOINT=host:port` 下，2026-08-12 对 Oxia standalone
source `a45e38cf2b8c815499fda4c1b59e017db769142f` 通过了真实 gRPC session、
durable epoch CAS、ephemeral lease create/close、renew、lifecycle transition、
release 和第二客户端递增 epoch 接管。该测试没有把本地 fake seam 或
clean-close 证据扩大为故障切换证明；authenticated assignment publication、
response-loss、multi-worker chaos 以及生产服务配置仍是 release gate。
内存 authority 也在同一 CAS 边界拒绝携带 stale lifecycle state 的 renewal；
若已有 `ACQUIRING -> RESTORING` successor，旧 lease 不能把它续租写回
`ACQUIRING`，证据为 `OwnerLeaseTest.renewalCannotRewindAConcurrentLifecycleTransition`。
内存 Owner Lease 测试 authority 的 epoch successor 也按完整 raw
`uint64` 域递增：`0x7fff... -> 0x8000...` 合法，只有全 1 值耗尽时
fail closed；这与 `OwnerIdentityV1`、Store runtime metadata 和 inflight key
的 unsigned fencing 语义一致，不把 Java signed overflow 当作协议边界。
证据为 `OwnerLeaseTest.ownerEpochSuccessorUsesTheCompleteUnsignedDomain`；
lease 的 expiry checked-add 也在 epoch 写入之前执行，时间溢出不会消耗下一次
接管的 epoch，证据为 `OwnerLeaseTest.overflowingAcquireExpiryDoesNotConsumeOwnerEpoch`；
候选 lease 的 owner/context 校验同样先于 epoch/lease map 更新，非法身份输入不会
消耗 fencing epoch，证据为 `OwnerLeaseTest.invalidAcquireValueDoesNotConsumeOwnerEpoch`；
Oxia owner-epoch allocation is now implemented by the concrete backend; the
real service response-loss and multi-worker evidence gates remain external.
`PersistentOwnerLeaseStore` 现在把同一边界扩展到一个 crash-durable 的本地
conformance projection：每个 shard 保留已消费的 epoch history，lease 的
assignment/session context、lifecycle、expiry 和 token 通过 canonical bounded
snapshot、checksum、atomic rename、directory fsync 及 JVM/on-disk lock 一起恢复。
`PersistentOwnerLeaseStoreTest` 覆盖 reopen、过期接管、stale release、context/shard
identity drift 和 corruption fail-closed。它只证明本地重启/response-loss 的投影
一致性，不能替代 Oxia session/ephemeral ownership、cross-worker CAS 或 production
assignment authority。
Activation 的本地 Oxia adapter 还会在 CAS response loss 后仅接受同一
fencing/assignment/session identity 的 exact `ACTIVE_FOR_COMMANDS` 重读；
`transitionOrRead` 在 lifecycle graph 禁止的请求上不会执行重读，因此
非法 transition 不会被 coincidental current state 掩盖。
Response-loss reread 还拒绝同 identity 但 expiry 变短的 successor；本地证据为
`OxiaOwnerLeaseStoreTest.transitionOrReadRejectsAResponseLossSuccessorWithShorterExpiry`。
Assignment/session-bound acquisition 的 interface default 现在也 fail closed；它
不再把未实现 context CAS 的 backend 降级成 shard-only live lease，避免在发现
上下文丢失前先占用一个无法用于 V1 activation 的租约。本地证据为
`OwnerLeaseTest.shardOnlyOwnerLeaseStoreCannotFallbackForContextBoundAssignment`
和 `OxiaOwnerLeaseStoreTest.backendWithoutContextBoundAcquireCannotAllocateAShardOnlyLease`。
Oxia adapter 还要求 acquire 成功值保持 `ACQUIRING`；后端若直接返回
`RESTORING`/`ACTIVE_FOR_COMMANDS` 会被拒绝，避免跳过 Worker 可验证的 lifecycle
CAS，证据为 `OxiaOwnerLeaseStoreTest.rejectsBackendAcquireResultThatSkipsAcquiringState`。
V1 assignment acceptance also rejects a non-null compatibility lease context
with assignment epoch `0`; only a positive exact assignment epoch may bind the
catch-up window。证据为 `OwnerLeaseTest.legacyZeroEpochLeaseContextCannotAuthorizeV1Assignment`。
Activation of `OwnedDelayShard` now leaves the local lifecycle in
`CATCHING_UP` while the authority performs the `ACTIVE_FOR_COMMANDS` CAS; the
local gate opens only after the exact successor is validated. The regression is
`OwnerLeaseTest.authorityGatedActivationKeepsLocalGateClosedDuringLeaseCas`。
严格 V1 activation 还提供
`OwnedDelayShard.activateForCommandsWithControlSnapshot(...)`，在 embedded 或
authority-gated activation 前要求 `meta/FIXED` key 10 中的
`CompatibleControlSnapshotV1` 与调用方提供的完整 snapshot exact match；缺失或
漂移不会打开本地 command gate。旧 overload 仍是 embedded compatibility seam，证据为
`OwnerLeaseTest.strictActivationRequiresThePersistedShardControlSnapshot`。
`OwnedDelayShard.beginDrain(OxiaOwnerLeaseStore, nowEpochMs)` 现在对
`ACTIVE_FOR_COMMANDS -> DRAINING` 使用同一 exact-successor CAS 规则；response
loss 只有在 owner/epoch/token/assignment/session 完全一致且在观测时刻仍有效的
successor 被重读时才算成功，否则本地视图转为 `FENCED`。
`OwnerLeaseTest.authorityGatedDrainRequiresTheExactLeaseSuccessor` 与
`OwnerLeaseTest.authorityGatedDrainFailsClosedWhenLeaseIsExpired` 覆盖该本地边界。
`DelayShard.revokeClaimsForOwner` 现在还提供 bounded 的 local `CLAIMED`
rollback：在单写锁内按 exact Owner Epoch 扫描并逐 Claim 原子恢复
timeline/Message/READY；Claim v2 还保留 `TimelineWorkRef`，因此
`UNCERTAIN_RETRY` 的 ControlRef/Source Position authority 不会在 revoke 时丢失。
超 bound fail closed，重复执行返回零；证据为
`DelayShardTest.localClaimIsDurableAndRevokeRestoresTimelineAtomically` 及
`DelayShardTest.sourceOrderedNotPublishedEvidenceRevokesClaimWhenAnotherUncertainObligationRemains`。
它只
关闭新的 command admission 并撤销可逆 Claim；in-flight publish quiescence、final
checkpoint 和 lease release 仍是生产 drain gate。
Close marker 现在额外把未 admitted 的 message/reservation quota 在同一个
WriteBatch 一次性转移，并写入已注册的 `timeline/SYSTEM` kind-2
`LaneCloseMaterializationCursor`。`materializeClosedLane` 按 canonical `id_cf`
key 顺序分 message/reservation bounded batch，重启从 cursor 继续；只有空
admitted-obligation set 的 generation 才会物化为
`LANE_CLOSED_BEFORE_ADMISSION`，`PUBLISHING`/`UNCERTAIN` 保留。证据为
`LaneCloseMaterializationCursorTest` 与
`DelayShardTest.closeTransfersUnadmittedQuotaAndResumesBoundedMaterializationCursor`。
`DelayShard.discoverLaneCloseMaterialization` 还严格校验
`timeline/SYSTEM(kind=2)` 的 key/value/Lane identity，`LaneCloseMaterializer`
提供不作新语义选择的 bounded local turn；这让本地调度器可以从持久 cursor
继续执行而不会把它混入 due-publish scan。它仍不证明 close-owned Claim 标记、
admitted outcome retirement、对象句柄 quiescence/GC、Recovery-Floor retention
或 owner/Oxia 负责的生产 materializer 编排已经闭合。
单个 close-materialization result 的 message/reservation 计数和一个 bounded
turn 的跨 Lane 聚合现在都使用 checked addition；`LaneCloseMaterializerTest`
覆盖 `int` wrap 会被拒绝，而不是伪造未超界的物化结果。
直接的 Lane 读取也会校验 `meta_cf/LANE` 值内 Lane id，以及 close cursor 的
Lane id/incarnation/control version/source shard；错挂的管理投影在暴露给调度或
物化器前 fail closed，回归证据为 `DelayShardTest` 的 key/value identity tests。
`id_cf/MESSAGE` 的直接读取及 activation/Close/retirement bounded scans 还会
校验 self-routing key 的 Shard 与值内 `scheduleSourcePosition` 的 Shard；跨
Shard 错挂的当前 Message 不会被当作本地工作，证据为
`DelayShardTest.messageLookupRejectsForeignSourcePosition`。
Command result、terminal history、reservation、open attempt、DLQ export 与
GC projection 的 direct reads 也做相同的 Source Position shard 检查；带有
message/command locator 的值同时检查其 self-routing Shard。跨 Shard 的历史
不能成为本地 query、drain 或 compaction 输入。
Lane 退休的最后一轮 inflight 扫描也复用 Claim/attempt 的 key/value 与 Source
Position 校验，错挂的 ledger 会直接 fence 退休，而不会只被当作普通 pending
work；`DelayShardTest.laneRetirementRejectsInflightKeyValueMismatchBeforeRetiring`
覆盖这一边界。
`PersistentLaneScheduler` 的 READY 恢复还会在重算 timeline key 前检查 READY
消息的 self-routing Shard 与 `scheduleSourcePosition` Shard，避免 scheduler-only
recovery 把跨 Shard 的 READY head 放入本地公平 ring；证据为
`LaneSchedulerTest.fencedRecoveryRejectsReadyMessageFromAnotherShard`。
稳态 `discoverReady` 还会把物理的、带 laneVersion 的 READY key 与 work item
一起记入进程内 discovered-head fence；因此相同 message/generation 只有在
work identity 和 READY key 都未改变时才会被抑制，合法的 Claim/READY
transition 即使保留相同 work item 也会重新进入发现队列，证据为
`LaneSchedulerTest.readyTransitionWithSameWorkUsesNewReadyKey`。
发现预算也没有“第一条 oversized READY”例外：serialized key/value
projection 超过本次 visit 的 byte cap 会直接 fail closed，证据为
`LaneSchedulerTest.readyDiscoveryRejectsFirstEntryThatExceedsByteBudget`；
这与激活时必须证明最大 admitted record 能同时适配所有 scheduler cap 的
V1/ADR 约束一致。
同一 discovery turn 的 elapsed cap 也在首条 projection decode 前检查；已耗尽
的 turn 不会因为尚无已收集 projection 而放行一条记录，证据为
`LaneSchedulerTest.readyDiscoveryStopsBeforeFirstEntryWhenTimeBudgetIsElapsed`。
内部 `dedupe_cf/COMMAND` replay lookup 也检查 command key 的 Shard 与结果的
Source Position；Claim lookup/scan 则检查其 `DelayMessageId` 的 self-routing
Shard，避免跨 Shard 的旧去重结果或 Claim 进入 source replay、owner drain 或
admission；证据为 `DelayShardTest.commandDedupeLookupRejectsForeignSourcePosition`
与 `DelayShardTest.claimLookupRejectsForeignMessageShard`。
SLO outbox 的 direct `get(sampleId)` 也与 bounded scan 使用同一
`meta_cf/SLO_OUTBOX` key/value sample-id fence，错挂的 Start 不会进入 Final
merge；`SloObservationOutboxStoreTest.scanRejectsKeyValueSampleIdentityMismatch`
覆盖 direct 与 scan 两条读取路径。

SLO Final 还按 Registry 的 success-event 分支绑定 endpoint kind：
`COMMAND_QUEUED_LATENCY` 与 `NATIVE_HANDOFF_ACK_LAG` 的 `SUCCESS` 必须来自
`BROKER_PERSISTENCE`，其它内部 WAL/barrier/probe 成功事件必须来自
`TRUSTED_OBSERVATION`；`SEMANTIC_FIXED_EPOCH` 只能作为 Start 端点，不能被
重复当成完成时间。`SloObservationOutboxV1Test` 覆盖 semantic-start-as-success
拒绝。该校验只关闭本地 endpoint-kind 混淆，真实 Broker receipt、Admission
WAL/evidence authority 与生产 Final 重建仍是 release blocker。

Close-materialization discovery 也会在返回 scheduler work 前重验 cursor 的
embedded close Source Position Shard，与 direct cursor query 保持同一边界；
`DelayShardTest.laneCloseMaterializationDiscoveryRejectsForeignSourcePosition`
覆盖该 scheduler-only 路径。
`discoverDue` 与 `discoverExpiry` 现在还会把每条 timeline projection 与当前
`id_cf/MESSAGE` 逐字段互证：status、generation、Lane、expiry 以及 exact
derived key 必须一致。orphan、terminal、旧 generation 或错挂的 DUE/EXPIRY
不会变成 publish/expiry work，而是直接 fail closed；Close 物化前仍合法的
`SCHEDULED`/`CLAIMED` generation 继续可发现。证据为
`DelayShardTest.timelineDiscoveryRejectsOrphanDueAndExpiryEntries`。
package-local `rebuildReadyIndexes` 在恢复每个 Lane 的 READY head 时也要求 candidate 的
timeline key 是当前 `MESSAGE` 的 exact derived key；仅有当前 message 而时间、
source token 或 generation key 被篡改，不能使 READY 恢复成功。证据为
`DelayShardTest.readyRebuildRejectsTimelineKeyThatDiffersFromCurrentMessage`。
Direct `discoverReady` 也会在返回 scheduler work 前确认对应 timeline entry
仍存在且 key/value identity 正确；READY projection 不能在 timeline 被删除后
继续作为独立指针。证据为 `DelayShardTest.readyDiscoveryRejectsMissingTimelineEntry`。
当前实现还把物理 value 边界收紧到 Registry 的 `TimelineWorkRefV1`：新的
DUE/ORDERED 及 paired EXPIRY 写入直接保存 canonical work projection，读取时校验
embedded timeline key、rich READY 的 `max(actionAt,retryEligibility)`，并在当前
runtime projection 存在时要求与 `GenerationRuntimeIndexV1.timeline` byte-identical。旧 `TimelineEntry` 只作为
read-only migration seam 接受，不能再由 writer 产生；缺失 runtime projection 的
legacy `MessageRecord` 只按 scalar schedule fields 受限读取。物理 rich value、
`actionAt` 和 key equality 由
`DelayShardTest.resolvedActionAtIsEarlierThanDeliverAtButOrderedKeyKeepsBusinessVisibilityOrder`
和 `LaneSchedulerTest.fencedRecoveryAcceptsCanonicalTimelineWorkRefValue` 覆盖。
同一边界现在也约束物理时间：DUE key 的时间必须严格等于
`max(actionAt,retryEligibilityAt)`，ORDERED key 不能早于 rich eligibility，且
`UNCERTAIN_RETRY` 不能进入 ordered namespace；`DelayShard.timelineKey`、
`PersistentLaneScheduler` rebuild 与 `TimelineWorkRef` 构造/解码使用同一规则。
`GenerationRuntimeIndexTest.timelineWorkFencesPhysicalEligibilityAndOrderedUncertainRetry`
覆盖不一致 key、过早 ORDERED key 和 ordered uncertain retry 的 fail-closed 分支。
`CONTROL_OVERRIDE` 的 nested ControlRef/Source Position 也经过 canonical typed
decode；`GenerationRuntimeIndexTest.controlOverrideTimelineRequiresCanonicalTypedNestedValues`
覆盖 malformed control/source bytes。这个 codec fence 不替代 authenticated
control/evidence authority。与此同时，Control Override 的 Source Position 必须
属于 timeline key 内 self-routing `DelayMessageId` 的 Shard，避免一个跨 Shard
的控制重试值进入本地 timeline；`GenerationRuntimeIndexTest.controlOverrideTimelineRejectsSourcePositionFromAnotherShard`
覆盖该 fail-closed 边界。
同一 runtime projection 现在还校验 aggregate status 与 current-work oneof 的
一致性：非 terminal `NONE` 只能带完整 UNCERTAIN obligation set，timeline work
必须与其 work kind/aggregate 相容，且任何 UNCERTAIN obligation 都把 aggregate
固定为 `UNCERTAIN`；terminal branch 可以保留 open obligation，但不能保留 current
send work。`GenerationRuntimeIndexTest.runtimeIndexFencesAggregateAndCurrentWorkProjectionDrift`
覆盖 drift vectors。为兼容旧 scalar-only `MessageRecord`，读取路径保留显式 legacy
v3 placeholder；它不是 canonical V1 runtime value，下一次 typed mutation 必须替换，
而 v4 decoder 会拒绝同形的 `NONE + non-terminal` projection。唯一保留的 typed
管理投影是旧 UNCERTAIN obligation 撤销 current work 后的
`SCHEDULED + NONE + UNCERTAIN`，它仍由 TOO_LATE gate 保护。这个兼容 seam 只解决
本地历史值迁移，不降低新写入或 source-ordered recovery 的 fail-closed 要求。带有
旧 UNCERTAIN obligation 的 current timeline 还必须是 `UNCERTAIN_RETRY`；typed
`MessageRecord` 同时拒绝 status 与 aggregate/current-work 不一致的 v4 projection，
`MessageRecordTest.typedRuntimeCannotDisagreeWithMessageStatus` 覆盖 terminal drift。
Registry 已声明的 `HANDED_OFF` 也已接入本地 status projection（追加 wire value 10，
不重排既有 status）：固定 early Pulsar handoff 的 Admission 只有在证据 kind 为
`PULSAR_SEND_ACK` 时才从 wire success `PUBLISHED` 派生为 terminal
`HANDED_OFF`；ordinary managed 和 opaque legacy Admission 仍为 `PUBLISHED`。两种
终态共用 `GenerationAggregateState`/v4 runtime/terminal summary fence，early timing
与证据不匹配会持久化 `STALE_SYSTEM_MUTATION`，不会静默接受为普通 publish。覆盖为
`MessageRecordTest.handedOffStatusUsesTheRegisteredTerminalAggregateProjection` 及
`DelayShardTest` 的 source-ordered published/evidence-resolution regressions；真实
Pulsar guard、Broker ACK authentication 和 handoff responsibility 仍是外部 release
gate。
在升级为 `HANDED_OFF` 前，`PublishEvidenceV1` 还逐字段校验 `PULSAR_SEND_ACK` 的
target resource、physical partition、prepared hash 与 retained Admission 的
`ChannelResourceIdentityV1`/prepared hash 完全一致；外部目标、分区或 payload hash
漂移会 fail closed 为 stale。该 local binding 的回归为
`PublishEvidenceV1Test.certifiedPulsarHandoffBindsTargetPartitionAndPreparedHashToAdmission`；
它不替代真实 Pulsar ACK authentication、guard 和责任证明。
`id_cf/V1_SCHEDULE_BINDING` 的 direct lookup 也会在读取 sidecar 前校验
`DelayMessageId` 的 self-routing Shard；即使当前 DB 只有错挂的 sidecar，也不会
把它暴露给 Registry 路径。证据为
`DelayShardTest.scheduleBindingLookupRejectsForeignMessageShard`。
Message、Command-result、Terminal、DLQ-export 和 message-based Claim 的
route-keyed lookup 现在也在 RocksDB 读取前执行同一 self-routing fence；foreign
ID 即使在当前 DB 没有对应记录，也不会被降级为 not-found。证据为
`DelayShardTest.routeKeyLookupsRejectForeignMessageShardBeforeMissingRead`。
READY rebuild 的 candidate scan 现在按 pending bound 多读一个条目并在溢出时
fail closed，不会静默丢掉同一 Lane 的后续 timeline head；证据为
`DelayShardTest.readyRebuildRejectsTimelineCandidateScanOverflow`。
Retired Lane guard 的直接读取也校验其 terminal Source Position 属于当前
Shard；错挂的退休证明不会通过 `getLaneTerminalGuard` 暴露。
`ShardStore.flushAndSync` 还提供 drain 的物理 flush/WAL-sync 原语，重开回归为
`ShardStoreTest.flushAndSyncMakesTheShardBoundaryExplicit`；它不替代远端 callback
quiescence 或 final checkpoint publication。
Store close 会先持久化 clean-close marker，再 fence 所有 public Store 操作；随后
关闭默认 Column Family、命名 handles、DB/options，只有完整 native teardown 后才
释放 Worker slots。每一项 teardown 都独立记账，native close 失败不会把未完成项丢掉，
后续 close 只重试未完成项；因此 read、scan、write、flush 和 sequence-number API
全部 fail closed，同时不会把 `maxOpenShardDbs`/`maxOwnedShards` slot 永久吞掉，
也不会让 shared native resources 在仍有活跃 handle 时提前关闭。
`ShardStoreTest.closedShardStoreFailsClosedForAllRocksDbOperations` 覆盖该本地生命周期
边界；共享 rate limiter、WriteBufferManager 和 block cache 采用同一 retryable
teardown 规则。`EmbeddedDelayService.close()` 在 final drain 后保持 fenced-but-retryable，
不会因 Store/Worker 首次 teardown 失败而永久吞掉后续 close。
`DelayShard.listOpenPublishAttempts` 还提供 bounded 的
`PUBLISHING`/`UNCERTAIN` ledger view，供 drain 等待 admitted callback 的本地轮询
使用；重复 attempt identity 或超 bound 都 fail closed，不能把未知 obligation
当作已清空。
`OwnerDrainCoordinator` 将 source/scheduler stop、authority-gated `DRAINING`、
Claim revoke、bounded callback poll、lease/deadline reread、flush/sync、可选
final checkpoint、Store close 和 exact release 串成一个可重试的本地顺序；
它还在 `OwnedDelayShard` 上持有 shard-local drain-attempt gate，避免仅靠
Worker-wide drain semaphore 时两个 coordinator 并发关闭或释放同一 shard
DB/lease；失败后 gate 可释放并保留 `DRAINING` 供重试。回归证据为
`OwnerDrainCoordinatorTest.duplicateCoordinatorCannotDrainTheSameShardConcurrently`。
如果 caller 提供 final checkpoint 的 exact 16-byte identity，coordinator 会把
它提交到共享 `CHECKPOINT` work class，由选中的
`CheckpointDrainWorkClassExecutor` 传入 `ShardStore.createCheckpoint`，让完整
镜像携带对应 `lastCheckpointId`；队列拒绝或尚未公平选中时不会进行物理 I/O，
也不会提前 close 或 release；
`flushAndSync` 后的可选 `commitSourceHint` 只收到最后已持久化的
`SourcePosition`，callback 返回后还会重新检查 draining lease；该 hint 仍不是
recovery authority。物理 final checkpoint 安装完成后也会再次检查 lease，只有
这条检查通过才会关闭 Store 和执行 exact release，从而把 checkpoint 期间的
lease 丢失转换为本地 fence，而不会让旧 owner 继续操作新 owner 的状态；
`OwnerDrainCoordinatorTest` 覆盖该边界。
如果 Store close 本身失败，local shard 保持 `DRAINING` 并保留
authoritative lease；后续 drain 只重试 Store native/slot teardown，不重复
Claim revoke、callback poll、flush 或 checkpoint，也不会在 DB 未确认关闭时
把 local state 置为 `FENCED` 而丢失重试入口。Store 已关闭但 exact lease release
响应未确认时也必须保持同一可重试的 `DRAINING` 状态，不能由 close-success
的 finally 路径提前改成 `FENCED`；后续 drain 只重试 lease release。只有 Store
完整关闭并确认 exact lease release 后才进入 `FENCED`。`OwnerDrainCoordinatorTest`
`storeCloseFailureLeavesDrainingStateForRetryableTeardown` 与
`unconfirmedLeaseReleaseKeepsClosedDrainRetryable` 覆盖这两个边界。
外部先启动 Store close 的路径现在也会闭合：若 Owner 仍为
`ACTIVE_FOR_COMMANDS`，先完成同一 identity 的 `DRAINING` CAS、停止
source/scheduling，再只重试 Store close 与 exact release，不会留下 active local gate
和无法释放的 lease。`OwnerDrainCoordinatorTest`
`externallyStartedStoreCloseEntersDrainAndReleasesTheMatchingLease` 覆盖该本地
emergency-drain 路径；source quiescence、Oxia CAS 与 fresh-incarnation recovery
仍是 release gate。
callback/source quiescence 仍由调用方和真实 transport 提供，超时保持
`DRAINING` 而不伪造成功。`OwnerDrainCoordinatorTest` 覆盖成功与 deadline
失败边界。
本地 Worker envelope 还会在创建 JNI 资源前检查显式 shared block cache 与
WriteBufferManager 预算之和不超过认证的 RocksDB native 桶；
`NativeResourceUsage`/`WorkerNativeResourceLedger` 将 block cache、memtable、
table-reader metadata、pinned blocks/iterators、flush/compaction scratch
保持互斥，并以 exact allocation identity 做 checked reserve/release；聚焦
回归覆盖拒绝、重复 identity 和释放路径。`WorkerRuntimeResourceProbe` 现在从 JVM、procfs、cgroup v1/v2
和 rootPath 对应的精确 FileStore 读取有限 runtime observation，并读取真实
`/proc/self/fd` entry count；`max`、缺失、非真实 FD 目录或 malformed limit
均 fail closed，`WorkerResourceEnvelope.validate` 再以 checked arithmetic
检查 `currentProcessOpenFiles + fdHeadroom <= maxProcessOpenFiles` 以及
heap/direct/RSS/cgroup/FD/filesystem 交叉边界。`WorkerRuntimeSafetyGate`
提供 fresh observation 的 sticky drain/migrate 状态和 explicit empty-drain
activation；`WorkerRuntimeResourceMonitor` 将固定间隔 probe 接入同一 gate，
probe 异常或 envelope mismatch 都会进入 drain/migrate，并可显式关闭调度器；
`WorkerRuntimeResourceProbeTest`、`WorkerRuntimeSafetyGateTest` 与
`WorkerRuntimeResourceMonitorTest` 覆盖解析、envelope rejection、周期探针
生命周期和共享资源 ownership fencing；`WorkClassSchedulerTest` 覆盖八个
work-class 的 bounded queue/turn caps、lease/fence 抢占和 stale-class 选择。
`WorkClassResourcePoolTest` 还覆盖 non-borrowable minimum、borrowed hold
bound 和 acquisition overflow rejection，`WorkClassEventLoopTest` 覆盖组合层
的 rollback、close、hold-time 和 callback-failure 边界。每 DB 的动态 RocksDB
attribution、WriteBatch/IO reserve admission 和生产 Worker wiring 仍是
release gate。
Lease validity additionally rejects negative observation times even when a
caller reaches `OwnerLease.validAt` directly rather than through an authority
request; `OwnerLeaseTest.negativeClockCannotMakeOwnerLeaseValid` covers the
fail-closed local gate.
Kafka source records now reject an unexpected Pulsar connection proof instead
of silently ignoring it.

Kafka Source Position ordering now uses the physical partition offset only;
leader epoch and append time remain authenticated metadata rather than a
second order dimension.  A replay that reuses the same offset/ledger-entry-
batch token with different canonical metadata is rejected, so one physical
record cannot be interpreted as a later Shard Log position or silently reuse a
Command result.
The shared `SourcePositionCodec` also requires decoded bytes to round-trip
exactly; malformed UTF-8 or a replacement-character variant cannot enter a
persisted position field. Kafka/Pulsar Source Position constructors apply the
same nonblank, NFC and UTF-8 check before producing identity bytes, so callers
cannot bypass the canonical boundary by constructing a position in memory;
`ProtocolCodecTest.sourcePositionsRejectNonCanonicalTextAtConstruction` covers
that direct-construction path. Decoder length prefixes and fixed-width fields
also fail with the closed validation error instead of leaking a buffer-underflow
or arithmetic exception; `ProtocolCodecTest.sourcePositionDecoderRejectsTruncatedLengthAndFixedFields`
covers the malformed-byte path.
The same strict UTF-8 round-trip fence now applies to direct construction of
Kafka/Pulsar broker resource identities, Pulsar `EvidenceCursorV1` physical
topics, and both managed queued-receipt `SafeBrokerAck` branches. These values
also become canonical identity bytes, so accepting an unpaired surrogate and
letting the JDK silently encode it as U+FFFD would otherwise make an in-memory
identity differ from its wire identity; `ProtocolCodecTest`
`brokerEvidenceAndQueuedAckIdentitiesRejectNonCanonicalUtf8AtConstruction`
covers all of those constructor paths.
The same exact-position check is applied to the owner catch-up cursor before
activation, not only to the subsequent Command/System Mutation WriteBatch.
An empty Pulsar activation barrier still validates a non-null persisted cursor's
resource incarnation and physical topic before declaring the barrier reached;
an old DB from another Pulsar resource therefore cannot bypass source identity
validation merely because no replay record is required.
The canonical empty Pulsar barrier also requires the guarded source connection
generation and resource-guard attestation digest as a pair; an unguarded empty
Pulsar barrier cannot enter a Ready Certificate.
The embedded queued-receipt query and applied-receipt projection apply the same
canonical-position fence at a reached barrier: a same Kafka offset or Pulsar
ledger/entry/batch token with different metadata is an integrity failure rather
than a successful query or applied frame.
`CommandQueuedReceiptV1` additionally binds the `PreparedCommandRef` shard to
the Source Position shard in the shared constructor used by both create and
decode, so a command-from-A/source-from-B receipt is rejected before barrier
evaluation; `ProtocolCodecTest.commandQueuedReceiptRejectsACommandAndSourceFromDifferentShards`
covers this self-routing identity fence.

本地 `RecoveryCatalog.publishUploadedCheckpoint` 现在要求 PUBLISHED intent
与完整 manifest 的 shard、lineage、checkpoint、manifest hash/length、owner
和 store incarnation 完全一致后才接受 catalog projection；同一 checkpoint
的 exact manifest 在 response-loss 重试中会作为幂等 reread 返回，即使 catalog
generation 已被其它操作推进；同 ID 不同 manifest hash 或 Object Store
container/key/version/profile 仍 fail closed。这仍不等于
Object Store 真实性或 Oxia transaction；Oxia validation adapter 同样允许
generation 相等的 exact reread，但拒绝 generation 回退。它现在在调用外部
upload-intent CAS 前复用同一组 state、base generation、对象身份、owner、store
incarnation 和 parent identity 校验；非法请求不会先触达 Oxia。Oxia 返回的
candidate、Floor 和 ancestry manifest 还必须与已发布值的完整 canonical JSON
字节投影一致，不能只依赖部分字段比较；checkpoint ID、evidence digest、
typed cursor 和 coverage positions 在调用 backend 前也会复制，校验快照与
backend 请求缓冲区彼此隔离，避免可变调用方或 adapter 在 CAS 期间改变请求内容。
恢复候选入口也会先 reread 同 checkpoint 的已发布 manifest 并做完整 canonical
投影校验，再调用 backend 的 floor/recovery-set 验证；同 ID 漂移候选不会把远端
验证当作第一次发现错误的边界。

Legacy/typed local Recovery Floor CAS 也支持 exact successor reread（含
checkpoint、manifest、source/mutation 和 evidence/cursor identity），response
loss 不会重复推进 Floor；不同 Floor 或 identity drift 仍 fail closed。
Floor coverage 与本地 GC guard 在 order token 相等时还要求 Source Position
canonical bytes 完全一致；同一 Kafka offset 或 Pulsar ledger/entry/batch 的
metadata 变体不能被当作已覆盖的 retention boundary。Oxia coverage response
还必须逐项对应已发布 manifest、无重复 checkpoint，并沿着 parent id/hash、
lineage generation、source position、mutation sequence 和 evidence cursor
逐边验证到 candidate；跳过中间 parent 或伪造 ancestry 会 fail closed。

Checkpoint GC 的 catalog-backed guard 现在会在 source/sequence/ancestry
证明之后 reread 当前 `RecoveryPinV1`。活动 pin 若保护待删 checkpoint 的
candidate 或 observed Floor，返回 `RECOVERY_PIN_PROTECTS_RESOURCE`；pin
读取失败返回 `RECOVERY_PIN_STATE_UNAVAILABLE`，两者都禁止本地 tombstone
compact。这只闭合了本地 pin-aware necessary condition，仍不等于 Oxia
session CAS、provider delete attestation 或完整的 external GC orchestration。
当前 Floor 读取或 ancestry coverage proof 任一 authority 异常也会返回
`FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING`，不会把异常当作无保护而删除 tombstone。

`DelayShard` 的本地 `gc_cf/TASK` lookup 还会把 requested resource kind、identity
hash 和 expected version 与嵌入的 retire intent 逐项比对；delete confirmation 的
nested intent 也必须匹配同一 key。错挂的 GC value 会在 query/compaction 前 fail closed，
回归证据为 `DelayShardTest.gcRetireIntentLookupRejectsKeyValueIdentityMismatch`。
`ResourceGcGuard` 对 nested intent 进一步执行 canonical record-byte equality，
因此 protection set、applied mutation sequence 或 applied Source Position 的漂移
也会返回 `INTENT_REFERENCE_MISMATCH`，不能仅凭 mutation/resource/version 子集
获得 tombstone compaction 资格。
对 `PAYLOAD_OBJECT` 和 `CHECKPOINT`，`DelayShard` 的删除确认路径还会把 `DELETED` outcome 与嵌入 retire intent 的完整不变版本绑定：缺失 immutable version，或 payload 在 pinned identity 存在 etag 时缺失该 etag，都不能证明是精确被 pin 的对象，因而 fail closed。同一校验也在恢复读取 `ResourceDeleteConfirmedRecord` 时执行，损坏 tombstone 不会绕过 GC guard；`ALREADY_ABSENT` 的 identity 禁止保持不变。回归证据是 `ResourceDeleteConfirmedBodyTest.deletedObjectEvidenceMustCarryThePinnedImmutableIdentity` 和 `ResourceGcGuardTest.durableDeletedCheckpointEvidenceCannotOmitPinnedVersion`。这个本地合同仍不代替真实 provider delete attestation、Oxia CAS 或 external GC orchestration。

普通 local catalog publish 对已存在的 exact manifest 也先做 identity reread，
因此 catalog generation 推进不会把一次已成功的 checkpoint insert 误报为冲突。
`OxiaRecoveryCatalog.validateLocalStoreRecovery` 现在也把 local Store 的
recovery-reuse proof 明确转发到 CAS backend，并在发起远端读取前拒绝缺失
Floor/base/install-state 的不完整 projection；embedded delegating backend
复用 `RecoveryCatalog` 的 exact typed Floor、manifest ancestry、Shard 与
Store-Incarnation 校验。这个 adapter 仍不伪装成 current Oxia
Owner Lease/session transaction，证据为
`RecoveryCatalogTest.OxiaBoundaryForwardsLocalStoreRecoveryValidation`。

`ShardStore.createCheckpoint` 现在先把完整 RocksDB 镜像写入同文件系统的
`checkpoint-tmp` 命名空间，完成后才通过 atomic rename 安装到目标路径；已有
目标会被拒绝；如果目标已经移动但后续目录 durability 校验失败，失败路径
也会删除该自有目标并回滚本地 projection，失败 staging 会清理。这闭合的是本地物理 checkpoint 边界，
不代表 Object Store 上传、manifest publication 或 Oxia CAS 已完成。
`checkpoint-tmp` parent/staging directory 现在必须是 `NOFOLLOW_LINKS` 下的真实目录，
而 `ACTIVE.tmp` 在写入前拒绝符号链接和非 regular file；因此临时路径不能把
checkpoint 或 ACTIVE pointer 写到外部目标。`ShardStoreTest.checkpointAndActivePointerTemporaryPathsRejectSymbolicLinks`
验证了 staging symlink fail-closed 以及 pointer target bytes 保持不变。
带 manifest 的 restore 在 staged DB 打开后还会逐项比较镜像中的
`lastCheckpointId`、`appliedShardLogPosition`、`shardMutationSequence` 和
typed evidence-cursor projection；文件 checksum 正确但运行时状态与 manifest
不一致时仍会在 install 前 fail closed。这样 manifest 的物理文件边界与其
恢复状态描述保持一致，而不把 source replay 或外部 catalog authority 假定为
本地 RocksDB 校验已经完成。
`ShardStore.openAtPathWithSlot` 也把 RocksDB 成功打开后的 metadata decode、
format/identity validation 和 install-mode write 放在同一个失败清理边界内；
任一失败都会先关闭 DB、Column Family handles 和 options，再释放 Worker slot。
`ShardStoreTest.malformedExistingMetadataDoesNotLeaveRocksDbOpen` 随后用 raw
RocksDB reopen 证明不会遗留 native 文件锁。失败 cleanup 现在会逐项尝试
所有 Column Family handle、DB/options，并把 close runtime failure 作为
suppressed diagnostic 保留，而不是在第一项失败时短路。
固定的本地 `shards/<routeIncarnation>/<partition>` 祖先目录也不再通过
`Files.createDirectories` 隐式跟随符号链接：open/restore 会逐级创建并用
`NOFOLLOW_LINKS` 验证 `shards`、route 和 partition 目录，任一 ownership-boundary
组件为符号链接都会在 RocksDB 创建或 restore staging 前 fail closed。这样
一 shard 一 DB 的物理目录不会被重定向到配置 root 之外；回归证据为
`ShardStoreTest.openRejectsSymbolicShardPathAncestors`。
Restore 在把 staged DB 原子移动到新 incarnation 后，会先以正式 active path
打开并验证 DB，再写入 `ACTIVE`；任一 pointer 安装失败都会关闭已打开句柄，
在关闭成功时删除未被 `ACTIVE` 指向的自有 incarnation 目录；关闭失败会有界
重试并保留无法证明已关闭的 orphan 目录供离线修复，同时保留原始 pointer I/O
错误。`ShardStoreTest.failedActivePointerInstallRemovesUnpublishedDb` 覆盖
`ACTIVE.tmp` 故障路径。
在这次 pointer 切换之前，restore 还会对新 incarnation 的父目录执行
directory fsync；因此 staged DB 的 atomic rename 已先取得目录级持久性，crash
不会出现 `ACTIVE` 已发布但对应 incarnation 目录项仍只存在于 page cache 的窗口。
restore 的 checkpoint copy 还会先 fsync 每个拷贝文件，再按子目录到根目录
fsync staged tree；因此该窗口同时覆盖 DB 文件内容和目录项，而不只覆盖 rename。
Normal `ShardStore.open` 也在发布 `ACTIVE` 前 fsync DB 目录及同一 Store
Incarnation 父目录，包括接管没有 `ACTIVE` 指针的 orphan incarnation；fresh-
open 与 restore 因此共享同一 pointer-before-directory-durability 顺序，并覆盖
open 阶段新建的 WAL/MANIFEST 目录项。
Restore admission 只把 checksum-validated `ACTIVE` pointer 指向的
incarnation 视为 live DB；pointer 尚未切换时留下的 orphan incarnation 不会
阻塞新的 atomic restore，且不会被悄悄当作 active 覆盖。
如果 `ACTIVE` 本身存在但指向缺失或非目录 DB，restore 现在会把它视为
store-integrity failure 并 fail closed，不会把损坏指针伪装成“无 active DB”后
覆盖；只有不存在 `ACTIVE` 指针时才允许安装新的 incarnation。回归证据为
`ShardStoreTest.restoreRejectsAnActivePointerWhoseDbIsMissing`。
Normal `ShardStore.open` 也对 `ACTIVE`、incarnation、DB 目录和 `CURRENT`
使用 `NOFOLLOW_LINKS` 并拒绝符号链接，因此 open 与 restore 对
live-incarnation pointer 使用同一 fail-closed 边界；restore 也不会把
有效 `ACTIVE` 指向的符号链接 incarnation/DB 当作 live DB。
已有 RocksDB 如果缺少 `meta_cf` 的 shard-identity marker 也不再被当成 fresh
DB 初始化；只有没有 `CURRENT` 的真正新目录才允许写入初始 metadata，已有目录
缺 marker 会在 activation 前 fail closed，`ShardStoreTest` 覆盖该重开路径。
正常 incarnation 目录还会把路径 UUID 与 metadata 的 `storeIncarnation` 做交叉
校验，并拒绝全零 Store/DB identity；restore-tmp 在 install-mode 完成新 identity
写入前不套用这条路径检查，安装到 `incarnations/` 后再由正常 open 验证。
staged open/metadata validation 的 runtime failure 也会清理 private
`restore-tmp`，而 download-slot 尚未取得时仍保留原始 bounded-concurrency
错误。Restore 的 staged validation、install-mode probe 和正式 installed open
现在由显式 Store 生命周期管理；失败清理会有界重试可重试的 native/slot
teardown，只有在 staged/prepared/installed Store 都确认完成关闭后才删除
`restore-tmp` 或未发布 incarnation。若仍有 Store 无法证明已关闭，相关目录
会保留供离线修复，避免把 native handle 仍在使用的目录删除。

`meta/FIXED` 的 immutable key 1/2 也已与 Registry §7 的物理约束对齐：
store format 的 payload 是 canonical u32 `1`，shard/Store identity 的
payload 是 canonical `StoreMetadata`，两者都通过 fixed-key ValueEnvelope
写入并在 open、restore staging 和 install-mode 重读时执行 type/version/
length/CRC 校验；不再存在裸 format 或裸 identity value。回归证据为
`ShardStoreTest.fixedFormatAndIdentityValuesUseRegisteredValueEnvelope`。
`ValueEnvelope` 的 numeric discriminator 也已在本地 codec 中限制为 Registry
注册的 1--11；§7 明确记录了 context-specific mapping，而不是把 GC 的
retire/delete union 或 fixed control states 错误地压成同一个 payload schema。
`ValueEnvelopeTest` 覆盖上界、未知 type、长度和 CRC 的 fail-closed 行为。
`ShardStore.open` 现在也在 activation 边界校验固定 key 3 的 Source Position
属于当前 Shard、key 5/11 的 non-negative fixed-width sequence，以及 key
12/13 的 registered non-empty control-state envelope；错误不会等到
`DelayShard` 构造后才暴露。`ShardStoreTest.fixedControlMetadataIsValidatedBeforeShardActivation`
覆盖 type mismatch 和失败后 native DB 可重开的证据。
`DelayShard` 构造时还会逐 marker 校验 key 12/13 内 Profile/Trust-Set
source-ordered history 的 Shard，并再次确认 key 3 的 applied Position；错挂的
历史不会进入 compare/replay 运行时，证据为
`DelayShardTest.activationRejectsForeignAppliedSourcePosition` 与
`DelayShardTest.activationRejectsForeignSourcePositionInProfileControlState`。

主设计 §10.1 要求的可变 Store 元数据现在也有独立的本地投影：
`StoreRuntimeMetadata` 在注册的 `meta/FIXED` key 4/6/7/8/9 中 canonical 持久
`lastIngressFenceProofId`、typed `evidenceCursors`、`lastCheckpointId`、单调的
`lastOpenedOwnerEpoch` 和 `cleanCloseMarker`；key 4 的单一
`IngressFenceState` 同时承载 close deadline 与 proof identity，避免 DelayShard
与 Store projection 争用同一 fixed key；不再把这些字段打包写入已经保留给
compatible control snapshot 的 key 10。key 10 现在由 canonical
`CompatibleControlSnapshotV1` 占用，严格绑定 shard subject、ProtocolTuple/Profile/
initial grant 集合及 digest；打开/恢复时逐项解码、校验 digest 和 shard identity，
并由 `CompatibleControlSnapshotV1Test`、
`ShardStoreTest.compatibleControlSnapshotIsPersistedAndRevalidatedForItsShard` 覆盖。
该 projection 只证明本地已取得的 control input，不替代 Oxia catalog/session
和版本读取 authority。打开时逐项严格解码并清除 clean marker，
正常 close 通过同步 WriteBatch 写回 marker；fence/checkpoint/owner/evidence 更新也
沿用同一 WAL-sync 边界。`StoreRuntimeMetadataTest` 和
`ShardStoreTest.malformedRuntimeMetadataDoesNotLeaveRocksDbOpen` 覆盖注册 key、
codec、生命周期与失败清理。该投影只证明本地 Store 事实，不能替代 Oxia
lease/catalog 或真实 Broker fence authority。`TIME_FENCE` 的 apply 还会读取
`DelayShardConfig.timeFenceSafetyMarginMs`，以 checked-add 后的
`proof.earliestEpochMs` 下界拒绝不足余量的 proof；verified proof ID 现在与
mutation result/source position 在同一 batch 原子落盘，重开回归也验证该 proof
identity。
`meta/RECOVERY` 的四个注册 key 现在也有独立的本地投影：key 1 持久
`RecoveryCandidateRefV1` lineage/base，key 2 持久完整 typed `RecoveryFloorRefV1`，
key 3 持久与 Floor generation 绑定的 raw `uint64` catalog generation，key 4 持久
带 digest 的 `RecoveryInstallStateV1` install/open phase。Shard Store 在 open、
install-mode、normal close 与恢复候选安装时用 WAL-synchronised batch 更新这些值，
并拒绝 foreign-shard Floor、LOCAL_STORE candidate/DB identity 漂移和 install-state
Store Incarnation 漂移；restore 会为新 Store Incarnation 记录 LOCAL_STORE candidate
及 pin 观察到的 Floor。`StoreRecoveryMetadataTest`、
`RecoveryInstallStateV1Test` 和 `ShardStoreTest.catalogBoundRestoreRejectsPinDriftBeforeActivePublication`
覆盖 canonical round-trip、key/value projection、reopen 与 restore。该投影只闭合
本地 recovery-reuse facts；`hasReusableRecoveryProof()` 不证明 ancestry/Floor
coverage，也不替代 Oxia Owner Lease/session/catalog authority。
其中 `StoreRecoveryMetadata.catalogGeneration` 保留完整非零 `uint64` 位模式，
包括 Java signed high-bit 值；高位 typed Floor generation 在 `meta/RECOVERY`
持久化后可重开并继续参与 exact Floor 校验，回归由
`StoreRecoveryMetadataTest.reopensRecoveryProjectionWithHighBitCatalogGeneration`
覆盖。
本地 `RecoveryCatalog.validateLocalStoreRecovery` 现在把这些 facts 与 exact typed
current Floor、published base manifest、parent-hash ancestry 以及 Store
Incarnation/install-state identity 绑定，拒绝 stale Floor、跨 shard 或跨 lineage
重用；它仍是只读 local proof seam，不是 Oxia transaction 或 Owner Lease/session
authority。
manifest restore 在 staged DB install 前还会把 `meta/RECOVERY` 的 lineage/base
identity（lineage、checkpoint、manifest hash 与 `LOCAL_STORE` 的 source Store
Incarnation）、observed-Floor lineage 和 install-state checkpoint identity 与
checkpoint manifest 做 exact 比较，拒绝把一份合法 RocksDB 文件镜像与另一条
recovery projection 拼接；`ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionLineageDrift`、
`ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionCheckpointDrift` 和
`ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionManifestHashDrift` 覆盖
这些 fail-closed 边界。它仍不等于真实 Object Store/Oxia catalog、Owner
Lease/session 或 source replay activation 证据。
同一 staged restore 边界现在还会在物理 DB 已有 `meta/FIXED` key 10 时，把
`CompatibleControlSnapshotV1.snapshotDigest` 与 manifest 的
`controlStateDigest` 做 exact 比较；不一致会在 install 前 fail closed。
缺少 key 10 的旧本地 DB 仍保留为兼容 seam，但不能据此证明 V1
`ACTIVE_FOR_COMMANDS` 的 control prerequisite，回归为
`ShardStoreTest.restoreWithManifestRejectsControlStateDigestDrift`。
已有 DB 在任何 open-phase 重写前也会验证 install-state checkpoint 与
lineage/base 一致；没有 lineage/base 的 install state 不能带 checkpoint，避免
原始投影漂移被新的 `OPEN` marker 掩盖。`ShardStoreTest.recoveryInstallStateDriftDoesNotLeaveRocksDbOpen`
覆盖该失败清理边界。
带 identity 的 `ShardStore.createCheckpoint(path, checkpointId)` 会先把 exact
16-byte checkpoint identity 写入 live DB，再拍摄完整镜像；物理失败会同步恢复
旧 projection，恢复后的 DB 因而保留它所代表的 checkpoint identity。无 identity
的兼容性调用仍只证明本地物理镜像，不冒充 manifest/catalog publication。

`CheckpointUploadCoordinator` 现在在本地上传边界内先校验完整 checkpoint
inventory、intent deadline 和 shard/lineage/owner/store/parent identity，取得
Worker upload slot 后才调用 typed adapter；对可识别的 RocksDB image，还会在
provider I/O 前读取固定 format/store identity 和 key 10，校验
`StoreMetadata` 的 shard、`dbIdentity`、Store Incarnation、format，以及
`CompatibleControlSnapshotV1` 的 shard/digest 与 manifest 一致；adapter 返回的
manifest object length/SHA-256/profile/lineage/checkpoint identity 不匹配时保持 PENDING_UPLOAD，
只有校验通过才执行本地 PENDING_UPLOAD -> PUBLISHED CAS。这仍不是 provider
attestation、Object Store immutability 或 Oxia intent/catalog transaction；
同一 pending intent 在 response-loss 重试中会先精确重读已提交的
PUBLISHED successor，且不再次调用 adapter。
`CheckpointFileInventory` 与 restore 的 `copyTree` 现在都会拒绝符号链接及目录
之外的非 regular 文件，避免把 checkpoint 中未知的物理文件静默丢弃后仍继续
恢复；显式 `CheckpointManifestLimits` 还把文件数、单文件/总 bytes、路径和
manifest bytes、evidence cursor 数以及 file/manifest object identity 字段长度绑定到
inventory、canonical decode、upload 和 restore 边界，原始 manifest byte cap 在
JSON parse 前检查，file/evidence array bound 在 parser materialize 时检查，其余
超限在本地 hash/provider I/O 前 fail closed；inventory 与 manifest 的文件名排序
使用 normalized UTF-8 unsigned byte order，而不是 Java UTF-16 order；restore 的
`copyTree` 通过 streaming iterator 复制，不再把整棵路径树 materialize，inventory
也在任何 file hash 前拒绝非 canonical path；源目录通过 admission inventory 后，
restore-tmp 副本还会按同一 manifest 再做一次完整 inventory/checksum 校验，避免
复制截断或 copy-time source mutation 仅因 RocksDB 仍可打开而进入 install；
restore-tmp cleanup 使用
post-order `walkFileTree`，不再建立 sorted whole-tree list；inventory 和 manifest
limits 的 total-byte checked addition 溢出也会统一 fail closed，而不是泄漏
`ArithmeticException`。无 limits
overload 保留为 embedded compatibility seam，不能作为 production activated
limits；这仍只是本地文件完整性边界，不替代 Object Store 内容证明。

After `eca483b`, `CheckpointPublicationCoordinator` 进一步把本地调用顺序固定为先完成 exact
Upload Intent 的 `PUBLISHED` successor，再把相同 manifest/resource identity
提交给 `RecoveryCatalogAuthority.publishUploadedCheckpoint`；请求在 provider I/O
前还必须匹配 intent 的 `baseCatalogGeneration`。它允许本地 Catalog 对已存在的
byte-identical manifest 做幂等重试，但明确不把 Upload Intent、Owner
Lease/session 与 Catalog 的多个单记录 CAS 当成跨记录原子事务。生产 Oxia
publication transaction、Object Store attestation/quiescence 和 owner-abandonment
authority 仍保持 release blocker。

`SharedRocksDbResources` 现在也把 checkpoint create/upload slot 纳入进程级
关闭保护；后台 checkpoint 或上传操作持有 slot 时，资源 close 会 fail closed。
`ShardStore.createCheckpoint(checkpointId)` 还会在写入 live Store 的 checkpoint
identity projection 之前取得 create slot；并发预算已满时不会产生需要补偿的
metadata WriteBatch，回归证据为
`ShardStoreTest.checkpointCreateSlotRejectionDoesNotMutateCheckpointProjection`。
一旦关闭获准，rate limiter、shared WriteBufferManager 和 block cache 会逐项尝试
释放；某个 native close 抛出 runtime failure 时，后续资源仍会被尝试，异常以
suppressed 形式保留，避免进程级资源被首个失败短路。
长期 owned-shard slot 现在还绑定 exact `ShardId` identity，而不只是一个计数器。
同一 Worker 对同一 Shard 的第二次 `ShardStore.open` 会在 native DB open/create
之前 fail closed；这也封住了两个 no-`ACTIVE` 并发 open 各自选择新 incarnation、
最后覆盖 `ACTIVE` 而留下一个仍可写的非 active DB 的窗口。identity 只在对应 Store
close 时释放，`ShardStoreTest.duplicateOwnedShardOpenIsRejectedBeforeCreatingAnotherDb`
证明重复打开不产生第二个 `CURRENT`，释放后仍可正常 reopen。
Shard open/restore 的短生命周期 acquisition 阶段也有独立的
`maxConcurrentAcquiresPerWorker` slot；它在 native DB 打开或失败清理完成后立即
释放，不会把 acquisition 并发额度错误地当成长期 owned/DB capacity。已有
acquisition slot 被占用时，`ShardStore.open` 在创建 native handle 前 fail closed，
回归证据为 `ShardStoreTest.workerAcquireSlotIsReleasedAfterOpenAndFailsBeforeOpeningWhenHeld`。
同一进程的 restore/download staging 也有独立的 Worker 级 slot，并在
manifest/file 校验、临时目录复制、验证打开和 ACTIVE 安装完成后释放；真实
restore 回归会在返回的 DB 仍保持打开时重新取得该 slot，证明不会把恢复并发
额度错误地绑定到 DB 生命周期。`CheckpointScheduler` 则以确定性 shard
jitter、due claim 上限和 in-flight fence 提供错峰调度；completion 必须带回
`claimDue` 返回的 exact process-local 句柄，并以对象身份而非
`shardId + dueAt` 值相等进行校验，重建的 value-equal handle、shard-only
或迟到旧 claim 都 fail closed。它是 process-local 调度器，不冒充
checkpoint manifest、Upload Intent 或 Oxia catalog authority。
同一资源封套现在还提供 `maxConcurrentDrainsPerWorker` 的独立 drain slot，
争用和资源 close 保护由 `ShardStoreTest.drainSlotIsWorkerBoundedAndCloseProtected`
覆盖；这只证明进程级 drain 并发限额，不能替代 claim quiescence、final checkpoint
和 lease release 的生产编排。

`CheckpointScheduler` 的 jitter 百分比计算现在先除后乘，避免近
`Long.MAX_VALUE` 的合法 interval 在中间乘法处误报溢出；`CheckpointSchedulerTest.largeIntervalJitterUsesCheckedWideArithmetic`
覆盖可接受的最大 jitter span 和应拒绝的更大 span。该证据仍只属于本地
错峰调度器，不替代真实 Worker checkpoint/capacity/chaos 证据。

同一 scheduler 的 next-due 计算现在对 interval、jitter 和完成时间的
`int64` 边界采用饱和：不可表示的 future due 固定为 `Long.MAX_VALUE`，并
清除已经完成的 in-flight claim，避免异常路径把本地 claim 永久卡住。
`CheckpointSchedulerTest.completionAtEpochBoundarySaturatesWithoutStrandingClaim`
覆盖该回归；这仍不代表 durable Upload Intent/Catalog 的时间 authority。

查询层也已补齐 `CheckpointSummaryV1`/`CheckpointCatalogResultV1` 的
canonical checkpoint-catalog projection，包含 shard identity、Floor identity
和严格排序的 summary array；它仍只是 public query value codec，不代表
durable control-operation query routing 或 Oxia catalog authority 已完成。
`CheckpointControlResultV1` 也已补齐 checkpoint-control typed result 的
shard/checkpoint/manifest/generation projection；其余 control result branches
现已补齐 Lane/Shard/Profile/Quota/Message/Route/Secret 的纯值 codecs 和
枚举/presence 校验，`ControlTypedResultV1` 也会按 branch 调用对应 codec，拒绝
tag/payload 漂移。本地 `ControlOperationAuthority` 现在把完整 receipt 作为
唯一 locator，覆盖幂等 register、严格 revision CAS 和固定 `queryUntil` 边界；
`PersistentControlOperationAuthority` 还把 exact receipt/current pair 以 checksum、
atomic rename 和 directory fsync 持久化，重启、同 bytes response-loss 重试以及
损坏/截断状态均 fail closed；`OxiaControlOperationAuthority` 对 backend 的 CURRENT
响应执行 operation/request/scope identity 与 revision 不回退校验。这闭合了本地
crash-durable CAS seam，durable control-operation query state 的生产 routing、
authorization、session ownership 和真实 Oxia authority 仍未完成。
现在 `OxiaSyncControlOperationBackend` 将同一 Control Operation 的完整 receipt 与
CURRENT projection 写入一个 checksummed canonical Oxia record，并用 version CAS
执行 register/advance；response loss 只有在 exact successor reread 后才成功，
malformed record、identity drift、非法 state/target transition 都 fail closed。
`OxiaSyncControlOperationBackendTest` 覆盖真实 Oxia Java client record surface 的
deterministic seam、reopen、revision/retention fence、corruption 和 response-loss。
这只是 per-operation durable record CAS，不等于 actor/scope authorization、
source-ordered routing、session ownership 或生产 Oxia service/chaos evidence。
`OxiaRealControlAuthoritySmokeTest` 在 2026-08-12 对 Oxia source
`a45e38cf2b8c815499fda4c1b59e017db769142f` 的真实 endpoint 通过了
Control Operation register/advance/query/reopen 和 immutable Prepared target
registration/reopen；这扩大了 single-record service evidence，但不改变上述
生产授权、source ordering、mutation transaction 和 chaos gates。
`EmbeddedDelayService` 已将该 seam 暴露为本地 register/advance/query 入口，便于
conformance tests 验证 register 和 exact advance response-loss 后的精确 receipt
重读；`OxiaControlOperationAuthority.advance` 不接受更高或不同状态的后续
CURRENT 来冒充目标 revision 的成功。它不改变上述生产边界。
该 adapter 还在远端调用前校验 receipt identity、register revision 和
`expectedRevision + 1` 的连续性，避免把非法请求交给 authority；本地 authority
还以显式 checked-successor predicate 拒绝 `Long.MAX_VALUE` 的 expected revision，
不会依赖 Java `long` 回绕来产生拒绝（`ControlOperationAuthorityTest`
`revisionSuccessorFailsClosedBeforeLongWraparound`）。

嵌入式 Command query 现在还会在 barrier 之后把 receipt 的 `commandHash`
与 shard `dedupe_cf` 保留的命令身份核对；同一 `commandId` 但不同 hash 的
receipt 返回 `RECEIPT_MISMATCH`，applied-receipt 路径也拒绝该 locator。
`EmbeddedDelayServiceTest.embeddedQueryBindsReceiptCommandHashToDurableDedupeIdentity`
提供本地证据。该检查闭合了“不能只按 ID 暴露结果”的 shard 边界，但不等于
生产 Gateway 的租户授权、Oxia 路由或真实 Broker barrier。

同一物理 locator 还必须通过 `DelayShard.matchesCommandPosition` 读取精确
`dedupe_cf/POSITION` 审计并确认其命名 receipt 的 `commandId`；因此同 shard
的较早或伪造 Source Position 即使已经跨过 barrier，也不会借用另一条命令的
逻辑结果。`EmbeddedDelayServiceTest.embeddedQueryBindsReceiptToExactPhysicalPositionAudit`
覆盖 query 与 applied-receipt 两条路径；缺失或跨类型 POSITION 审计均 fail closed。

The local `DeliveryCapabilitySemanticV1` value codec now closes the Registry
baseline/strong outcome branches and Kafka/Pulsar evidence-resource and timing
compatibility checks. This is only semantic-value evidence; immutable Profile
publication/catalog resolution and authenticated Broker prerequisite authority
remain release blockers.
The four Profile semantic body codecs and `ProfileSemanticEnvelopeV1` now add
closed branch/kind checks, destination partition-policy validation, mandatory
Object Store safety flags, verifier validity bounds and the Registry
domain-separated semantic hash. They remain pure values; publication,
credential-binding protection and catalog/authority transactions are not
claimed complete.

Kafka offset and Pulsar ledger/entry Source Position fields now preserve the
complete unsigned-64 raw bit pattern through `SourcePositionCodec`, direct
constructors, activation barriers, evidence cursors, queued receipts, adapter
result values, canonical protobuf helpers and checkpoint-manifest JSON. The
position partition, leader epoch and Pulsar batch fields now preserve their
complete unsigned-32 raw bit patterns through the same local protocol paths;
checkpoint manifest JSON decoding also uses the unsigned parser for Pulsar
evidence-cursor `batchIndex`/`batchSize`, so `0x80000000` and `0xffffffff`
round-trip instead of being rejected by a signed parser. The boundary vector is
covered by `CheckpointManifestTest.manifestRoundTripsUnsignedSourceAndEvidencePositions`.
comparators and the strict Kafka successor use unsigned order. Boundary vectors
cover high-bit position fields, receipt/evidence round-trips and manifest
round-trips. The local `KafkaReceiptJournal` now follows that same boundary for
receipt positions and exact matches: it compares offsets and
`lastStableOffsetExclusive` unsigned, permits sign-bit crossing, and treats only
the all-ones raw offset as exhausted. `KafkaReceiptJournalTest`
`receiptJournalPreservesUnsignedHighBitOffsetsAndOrdering` covers mapping,
retirement and contiguous-cursor ordering across `Long.MAX_VALUE ->
Long.MIN_VALUE`. This closes the local receipt-journal/source-codec width and
ordering drift; Kafka transaction, read-committed LSO and contiguous replay
authority remain release evidence. The three `TrustedUtcIntervalEvidenceV1` uint64 counters and its
`sourceKeyVersion:uint32` also preserve their raw bit patterns through the
canonical codec and checkpoint `createdAt` JSON; high-bit signed-time evidence
is covered by `ProtocolCodecTest.trustedUtcEvidencePreservesUnsignedSourceKeyVersionBits`
and `CheckpointManifestTest.manifestRoundTripsUnsignedSourceAndEvidencePositions`.
Registry Pulsar `physicalTopicCreationTimestamp:u64be`
also preserves its raw bits through broker identity, queued ACK, evidence
cursor, managed/native adapter and checkpoint-manifest projections, with
high-bit vectors in the protocol, adapter and manifest suites. Other
auxiliary uint64/time fields, real Broker assignment/barrier proof and
production adapter authority remain release blockers, so the source-order
row below claims the implemented local u64/u32 position paths rather than full
production Source Position authority. Ownership/fencing `ownerEpoch` and
writer generations now preserve raw uint64 bit patterns through
`OwnerIdentityV1`, the OWNER/FENCE/SERVICE `AuthorIdentity` branches,
`ShardControlResultV1`, StoreRuntimeMetadata/inflight keys, Claim and
PublishAttempt records, and checkpoint `createdBy` JSON. High-bit vectors cover
the typed identities, runtime metadata, local inflight projections and
manifest JSON; this closes the local width/encoding path, not Oxia lease or
production ownership authority. The guarded Pulsar source-connection
generation is likewise preserved as a raw uint64 through the canonical
activation barrier, runtime barrier, replay-entry validation and
`OwnedDelayShard`; high-bit fence vectors cover both the wire and catch-up
paths.
`NativeCapabilitySnapshotV1` likewise preserves the complete nonzero raw
`resource_guard_config_generation` in its signed digest and canonical bytes;
the guarded Broker rollout attestation remains external evidence.

## Source locks

| 依赖 | 审计锁 |
|---|---|
| Delay current Kafka source process-crash recovery receipt | `nereus/delay-full-implementation-v1@2bcaff5e0c0b15b819cbc614c166c47e19571be3` (source-bound live three-Broker KRaft process-crash cut fetched exact offsets `0,1` with LSO `2` and no ACK/close, then a fresh same-group process replayed offsets `0,1` and committed group offset `2`; K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256 `1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`, Compose project `nereus-delay-kafka-e2e-1786881618-58469` on `19561,19562,19563`; raw network/proxy/socket, coordinator/Broker cuts, placement and release gates remain open) |
| Delay current Kafka retention-floor receipt | `nereus/delay-full-implementation-v1@d8dc5f45` (source-bound live three-Broker KRaft retention floor advanced from offset `0` to `20`, stale guarded Fetch was rejected with typed `OFFSET_OUT_OF_RANGE`, and the fresh floor record at offset `20` was read with LSO `21`; K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256 `1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`; raw cuts, placement and release gates remain open) |
| Delay current Pulsar multi-Broker Worker failover receipt | `nereus/delay-full-implementation-v1@afbb2e30511b53b2f44adc620767685753acb48e` (source-bound live two-Broker P1 Worker failover with real Oxia Assignment/Owner authority; P1 `nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution SHA-256 `373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`; broker-1 stop and broker-2 Worker resume passed, while one ZooKeeper/BookKeeper, no Gateway ingress, raw cuts, D3, placement and release gates remain explicit) |
| Delay current live large-payload authority receipt | `nereus/delay-full-implementation-v1@33ff7a4b` (source-bound live three-node Kafka source/destination/receipt + real Oxia Route/Assignment/Owner + mTLS/RS256 Gateway + Worker + versioned MinIO receipt; provider version `483877e3-06e8-4b8d-81fa-5983b42a2cba`; Admission source offset `4`, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset `0`, Outcome source offset `5`, exact payload readback and exact Gateway idempotency; one partition per topic and local trust-catalog semantic resolution are explicit boundaries; response-loss/LSO/retention, Pulsar combined egress, multi-shard placement, raw fault matrix and release gates remain open) |
| Delay historical large-payload control/object receipt | `nereus/delay-full-implementation-v1@ca9e9623b082f1c7df575e7316fb806226860ee5` (historical source-bound live three-node Kafka + real Oxia + mTLS/RS256 Gateway + Worker + versioned MinIO control/object receipt; provider version `5777ce46-f28c-4a68-ab76-71241ab5fd43`; retained as provenance for the pre-destination composition) |
| Delay current implementation slice | `nereus/delay-full-implementation-v1@33ff7a4b` (Large Payload Kafka destination authority composition; 2 MiB producer/broker/fetch sizing, source-only Worker bootstrap followed by durable typed-Lane binding, real destination transaction/receipt readback, and fail-closed cleanup) |
| Delay local implementation slice | `nereus/delay-full-implementation-v1@d9b713a9159a8b2672a2b0aea5bd5243ca798c3e` (latest runtime slice: the single-record Oxia Profile catalog requires an immutable credential-attestation trust set during publication, rotation, canonical decode/reopen and bounded lease issuance; the verified material cache now installs only exact Object Store Profile/generation/binding/reference/fingerprint values after trust-set and scope checks, returns null on miss and atomically preserves its previous snapshot on failed replacement; the renewable S3 adapter renews only inside an explicit window, rechecks the exact same Head/Binding/material fingerprint, atomically replaces the local gate after a protected lease reread and rejects Head generation rotation as a quiescence boundary; exact verifier tuple, Ed25519 signature and retained key window are checked before the existing activation composition resolves Head/Binding/Protection once, obtains private Object Store material through a resolver/cache and constructs the lease-gated S3 adapter; Provider calls do not reread Oxia unless renewal is due; it builds on the exact CAS/response-loss and real single-node Oxia Profile smoke plus the S3-compatible checkpoint adapter/local lease gate and source-bound Kafka/Pulsar physical Publish/Outcome, Gateway/Oxia response-loss, real multi-node Oxia Gateway failover and bounded Kafka/Pulsar Worker failover slices; external secret-manager resolution/source-ordered refresh, trust-set publication, secret/actor authority, source ordering, retained-generation GC, cross-record session transactions, scheduled multi-process renewal ownership, multi-node failover for this authority, provider quiescence, raw network/process cuts, production placement/eligibility and release gates remain open) |
| Delay current implementation head | `nereus/delay-full-implementation-v1@0868119c` (current receipt head; commit `0868119c` records the Large-payload Gateway/Oxia/Kafka/Worker/MinIO vertical harness receipt; implementation source is `44657691`, predecessor current Gateway/Kafka evidence receipt is `cfc3e8f0e9ab011846badfe02f8583403419a3d8` with implementation source `df2d021fc7e8c5586b062870325efa71835b6d3b`, predecessor recovered publish evidence identity fence receipt is `435ec3a65eaf88aa34adfd319d98621b1ab12911` with implementation source `c6b6a5f9b52e8f5c358e047218ee606ea58aed3f`, predecessor Kafka client metadata identity fence receipt is `a8dcd5254fbde85a4c51d8c7b13d473859bbff5d` with implementation source `1ab1d53fa4e14235fbb510035f2afaeea1ff3605`, predecessor Pulsar managed SEND evidence identity fence receipt is `9de24f3b6333803238c7a8d032f3097275c7c8ae` with implementation source `4ed28c89f6cf9e20c12f1ee226752327f05f7953`, predecessor Gateway queued aggregate tail fence receipt is `f81c9782fbeae4d1919348e0c9fb962abd7aa5f2` with implementation source `5b4d99e39725b79ae2c1aad6de9009b8df546ab5`, predecessor Gateway attempt timing/retry shape receipt is `4515f9c9d39ed8be6faad9e43e549e919e805765` with implementation source `e0d5bc9761fea57103518819165d54eb60662b99`, predecessor Gateway active attempt tail fence receipt is `554cf4cc34131c17d83943fb985e51116eab6ccb` with implementation source `a1a85f99471743c48126943fad92fbb80ce6be34`, predecessor Gateway audit phase evidence receipt is `a4a9e63ef807b9bce6a7c9488c885564eec9a4d3` with implementation source `745da182c72af27dff09a8fb55db6cc15a4f20e3`, predecessor Gateway operation/prepared binding receipt is `1392eeab354a4cc8970c6120c7cfb8d667d5e632` with implementation source `f27800424a7cde3b8496b4fbbb4d4586cbeb07ca`, predecessor Gateway retry evidence hash binding receipt is `ee243d65028911197d3ba4f429761c436069be01` with implementation source `5e1bd9f6b3e2bcf24972e7b9ecdd78db49520734`, predecessor Gateway stored evidence binding receipt is `ac1f09af2e8d7390705d9c88c3c69287828c8628` with implementation source `380e279725e9ac5d31f98ad49ee711cd15c5b25c`, predecessor Gateway attempt projection integrity receipt is `ccbaf45ec1e74541e80c2a123c0dba63f1a111a2` with implementation source `52c6ed1c604a98b56668e510a3cf84ad364ec9cc`, predecessor Gateway prepared-expiry fence receipt is `634cc8c0c46daedfd2725ae0a27d7b6c5f7d3f0a` with implementation source `66508783f5e8230ace8bae37ff04c28dfb353653`, predecessor Gateway idempotency evidence monotonicity receipt is `2708ee9e36bd7a14d5eb6e1d0e1ff5ac895bc71c` with implementation source `b19f998ffe811d0a6dee1051491eae6c61131712`, predecessor Gateway admission lease release retry receipt is `ed612260` with implementation source `d5384b954e4d99ad291b2aea004910e1b1666ec8`, predecessor Owner connect prefix validation receipt is `c14f69247754f9702b097d7dc3b6a834453af675` with implementation source `499e8439f2fe0f1b1c1114dbfd1bb7e55a06c43c`, predecessor Guarded Pulsar transport teardown aggregation receipt is `9cf460e910f920fb275d9f66537e094014a1ae05` with implementation source `9d164037f9ba3832cd1f83846813b44de18967ab`, predecessor in-memory command transport registry teardown retry receipt is `0d7497dffe9e1af935f1102a194659ceddbe71da` with implementation source `0378e9a7585397e6f5e71a301f58c6d00835f2a0`, predecessor Worker monitor teardown retry receipt is `b577c2b53b798bc2ce857d2bd8bb01aadc29e4ea` with implementation source `2f7d9d667547380355a27517ea2c1e4941962693`, predecessor Route connect prefix validation receipt is `0864a9d86295df512f230571d713ef787439fd12` with implementation source `4da7bcf46b0ab9350adebf1f614590851a1fadd8`, predecessor Direct SDK client teardown retry receipt is `2ee753b4b01b9eb55471e8ab69de5a0e492de47f` with implementation source `677026b33bcb7aacc5e3e8b2338f4167eaf0d952`, predecessor Route client teardown retry receipt is `eae1d2195ac617eedc667fc4631eafd9fc4dbe7f` with implementation source `9f24b2f38ba4f21962bebdaa2455d7f86ba0cd1b`, predecessor Worker source close retry receipt is `c82642d68cad2fdd558e8e5ab41e8510e4a968c1` with implementation source `874fccb4fc521ad51b7954236ec5e37c1591e011`, predecessor fleet and Route resource close aggregation receipt is `2a43c78cc0bdfbc4ae319cdb971be70d065f9a77` with implementation source `eb47cb807ceb45d68a9f8db5f53ef3a7cc6ead4e`, predecessor initial Route watch recovery receipt is `aeaa87be52cafeaffaa05e6f0b646b16a3ad3e27` with implementation source `22780082d24e2011d44ead6ca62c38251a03633b`, predecessor Route provider start-retry receipt is `2ea88d5a4ee8c1d3ba283bd6eed1fb3bba549040` with implementation source `d241246eefc284fea9719c8e162afa8e2a8e4828`, predecessor Route notification reconnect receipt is `7558dcbd154f505e1c6378631c4547168b6cbb22` with implementation source `de203e4dc14de32746ce73da75381843152af922`, predecessor Route authority session-bound receipt is `8ac9c03b92b327b1c371f59cfb173d7a77f23124` with implementation source `57e466786aea596cfdbd75020e48310415da0335`, predecessor Recovery Pin wiring receipt is `4fbae7e7984262ed97949fadc9a24d44601316fb` with implementation source `f0e45cbdf6eb30d730c6678e71c4c19d34e06072`, predecessor atomic publication authority pairing receipt is `21290feae32927297b1641decc44c70ea21a492e` with implementation source `920197ad41aaa6f0b88871f5ddf631f6899a53d3`, predecessor Owner Lease session-bound receipt is `65b30118f7dc2e4729c4472d32e9c382de9cd751` with implementation source `7a76a3af61ea16bceb81cc566462c078ca8de2a5`, predecessor Worker assignment session-bound receipt is `b68bc045f1b38a89a9377d4df0a4414fffd966b9` with implementation source `cca59a92df395c11cfdda23d24bb27a8b5269cca`, predecessor Checkpoint Upload Intent session-bound receipt is `2a68c3eee6fcc2618994463972f6bfedef9b98d4` with implementation source `0a1e6020290ae6a5759d6dad7bad4f0ccd677830`, predecessor Checkpoint Publication session-bound receipt is `c6b3612d7e919f86f883c006d41f745db080a607` with implementation source `ffe0e5e15894ba377248068258444a1484bfb7f2`, predecessor Recovery Catalog session-bound receipt is `d5317749c33216877530c87721b415fb92ad7898` with implementation source `f04f58d15588662b71be68809e1a11a627baf540`, and earlier source locks remain recorded below; certified external Owner/session authority, provider quiescence attestation, REAPING/Floor/Pin/Owner authorization, source-ordered retire/delete confirmation, secret-manager resolution/source-ordered refresh, trust-set publication, secret/actor authority, catalog-driven multi-shard placement, native eligibility, production Worker authority, scheduled renewal ownership, raw chaos and release gates remain open) |
| Delay historical source-lock anchors | Historical provenance retained but not current: Recovery Pin correction `4fbae7e7984262ed97949fadc9a24d44601316fb` / `dbe8ae91` / `f0e45cbdf6eb30d730c6678e71c4c19d34e06072`; atomic publication pairing `21290feae32927297b1641decc44c70ea21a492e` / `920197ad41aaa6f0b88871f5ddf631f6899a53d3`; Owner Lease `65b30118f7dc2e4729c4472d32e9c382de9cd751` / `7a76a3af61ea16bceb81cc566462c078ca8de2a5`; Worker assignment `b68bc045f1b38a89a9377d4df0a4414fffd966b9` / `cca59a92df395c11cfdda23d24bb27a8b5269cca`; Upload Intent `2a68c3eee6fcc2618994463972f6bfedef9b98d4` / `0a1e6020290ae6a5759d6dad7bad4f0ccd677830`; Publication `c6b3612d7e919f86f883c006d41f745db080a607` / `ffe0e5e15894ba377248068258444a1484bfb7f2`; Recovery Catalog `d5317749c33216877530c87721b415fb92ad7898` / `f04f58d15588662b71be68809e1a11a627baf540`; credential Profile `1b33f2715d700c0128f1bce6bc3fa6b2b268a8c9` / `89020c97c29f99d98f7f3259ab7b27131644adcd`; Control Target Registration `5e48d1d4aa8467a2047b86b0c5b94d2057c1c42e` / `50435a1364d2e8f7d823cc05faa18e4766f5cbd6`; Control Operation `cd1b880901a329dc4c011edafb29bbe9ba0ee111` / `cc8001b528bb9943a2f683c6ad14728c426cb8f2`; atomic Recovery Pin `58ab9cb4f83b858ab2251c51e37fd61fe39df38f` / `04976375`; Recovery Pin `d4b8f6811ed96d0ea93656c6e78ee9527ea5888f` / `dedd03a94fb2ab1e8d12f19ba993408646426578`; source-ordered GC `d3aa4dc32dcffb448e0667890cc9f8f46f9e21f7` / `b225cef9`; temporal evidence `32bfa4abec90f1857efa7390f3b0b3941c8eaf93` / `a26c6816`; composition `1c7c362213d74fe6542b17da3727b3d9f8f50088` / `70e5f0da`; provider horizon `fa9a5affa1a1ffde47a7def2a312b83849bde848` / `cc97c7654cb19f88c69045cd3c33a4d970a9fed3`; MinIO manifest `1b904a10-2104-46eb-a6fd-0bd2afe24524`; Owner proof `410d17edc81cb7885333bb9f1f728db2ece7b454` / `44cd3230709f5e87742cd94cd9a8b7bce314a184`; quiescence `065a233a48f07ee561e78d4d35fa35f82b8af0da` / `7b8b73885c5ec26dfc96c1b5b8a1a6ab8ec0d1d9`. |
| Delay MinIO provider smoke slice | `nereus/delay-full-implementation-v1@31ba5661` (`S3CompatibleMinioRealSmokeTest` plus `e2e/run-minio-real-e2e.sh`; the harness locks the local MinIO image tag/repository digest, creates only its own temporary bucket through curl SigV4, runs the real adapter with `--rerun-tasks`, and removes only its own container; the receipt proves one MinIO endpoint's immutable checkpoint upload/idempotent retry/download path, while generic S3/provider breadth, credential authority/renewal, deletion, chaos and release gates remain open) |
| Delay exact provider-version slice | `nereus/delay-full-implementation-v1@2981a269` (implementation `b971cd3f` makes missing `x-amz-version-id` fail closed for the mandatory exact-version Object Store Profile; the versioned MinIO receipt records provider manifest version `780f1e1f-c7da-4dc1-ae4e-a7b9be4f801c`; complete version-aware deletion, retire/Floor/Pin authority, provider consistency and release gates remain open) |
| Delay exact manifest-version readback slice | `nereus/delay-full-implementation-v1@d7f51441` (download signs and requests the catalog-bound manifest `versionId` and rejects a different response version; the real MinIO receipt records `ac201fe8-ba70-4bcb-a49c-a75a6657be55`; complete object-set deletion and GC authority remain open) |
| Delay exact checkpoint object-set deletion slice | `nereus/delay-full-implementation-v1@3bfe030a` (the S3-compatible adapter preflights every manifest/file identity, captures provider versions, deletes files by exact `versionId` and the catalog-bound manifest last, and requires matching delete response version plus provider request ID; the locked MinIO receipt records `e223584d-2863-45a1-8471-9b378c0899c5`; `ALREADY_ABSENT` reconciliation, final prefix sweep and retire/Floor/Pin authority remain open) |
| Delay checkpoint delete retry-convergence slice | `nereus/delay-full-implementation-v1@220fc98a` (404 presence probes now carry provider request/response evidence; partial response loss resumes from the remaining verified object set and a completely absent set returns `ALREADY_ABSENT`; final prefix sweep and retire/Floor/Pin authority remain open) |
| Delay checkpoint prefix sweep slice | `nereus/delay-full-implementation-v1@c32a98f328400c71346b98188930a6efa80da7c9` (bounded one-page `ListObjectVersions` over the exact checkpoint prefix, secure/version-complete parsing, exact-version deletes and a final empty-prefix listing; the locked MinIO receipt records `f905db1e-1a7e-455c-bb32-5fa90bb7ed1f`; external REAPING/Floor/Pin/Owner authorization and lifecycle state transition remain open) |
| Delay checkpoint REAPING sweep coordination slice | `nereus/delay-full-implementation-v1@b9fcd2aa846329ed13986b122d287375a441b2fd` (exact PENDING_UPLOAD -> REAPING CAS and successor reread precede the provider sweep; response loss retries the same REAPING identity/prefix; the locked MinIO receipt records `f5404da4-4944-4581-a75d-80dccdad92c3`; old-Owner/session authority, quiescence and external delete confirmation remain open) |
| Delay checkpoint REAPING quiescence proof slice | `nereus/delay-full-implementation-v1@7b8b73885c5ec26dfc96c1b5b8a1a6ab8ec0d1d9` (immutable proof binds pending/reaping evidence, enforces the provider-lifetime plus trusted-clock-width horizon, and requires old-owner/provider closure horizons before the coordinator calls the provider; the locked MinIO receipt records `9c4dcab9-c03c-4860-81de-07e62302d30e`; external attestation issuers, Owner/session loss and delete confirmation remain open) |
| Delay checkpoint REAPING Owner proof slice | `nereus/delay-full-implementation-v1@44cd3230709f5e87742cd94cd9a8b7bce314a184` (typed proof binds pending/Owner/Store/session lease identity, distinguishes explicit abandonment from a recorded lease no longer current, and requires trusted UTC after the upload deadline; the locked MinIO receipt records `ea89d80e-e63e-4980-b225-94b070d3c36b`; the issuer is local composition, not the production cross-record intent/Owner/catalog authority) |
| Delay Object Store provider ownership horizon slice | `nereus/delay-full-implementation-v1@cc97c7654cb19f88c69045cd3c33a4d970a9fed3` (local tracker spans complete upload/download/delete/sweep operations, retains response-loss uncertainty through a bounded horizon, fences new operations and rechecks the credential-use lease before each HTTP send; the locked MinIO receipt records `1b904a10-2104-46eb-a6fd-0bd2afe24524`; remote provider execution/quiescence attestation remains external) |
| Delay checkpoint delete-confirmation composition slice | `nereus/delay-full-implementation-v1@70e5f0da` (pure local `CheckpointDeleteConfirmationComposer` binds `CheckpointDeleteResult` to the exact durable checkpoint retire identity, requires confirmation earliest time at or after the observation latest time, and composes a signed `RESOURCE_DELETE_CONFIRMED_V1`; provider-side deletion, retire/Floor/Pin/Owner authorization, Shard Log append and mutation apply remain external) |
| Delay delete-confirmation temporal evidence fence slice | `nereus/delay-full-implementation-v1@a26c6816` (`ResourceDeleteConfirmedBody` and `ResourceDeleteConfirmedRecord` both require confirmation earliest time at or after the complete provider-observation interval, so manual/signed callers cannot bypass the composer; provider execution and lifecycle authorization remain external) |
| Delay source-ordered GC confirmation handoff slice | `nereus/delay-full-implementation-v1@b225cef9` (typed `GcWorkClassExecutor.submitDeleteConfirmation` binds the exact retire record and reports `PERSISTED` only for a strictly later position in the same physical source; the executor does not append, apply, or authorize the lifecycle) |
| Kafka contract/patch source | `76f62f3b83e882105219b6c7687dbde594a8b8a2` |
| Pulsar contract/guard source | `50fc70fe4620febcf0fd31d97ff7d2be447af3d4` |
| Kafka guarded-client implementation base inspected for ADR 0044 | `trunk@c300006a7705c240642db6950b5a95fec982bfc5` |
| Pulsar first-class-guard implementation base inspected for ADR 0044 | `5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551` |
| Kafka isolated K1/K2 implementation | `nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9` |
| Pulsar isolated P1 implementation | `nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7` (dedicated admin/ownership-checked Resource Controller guard endpoint; generic topic-properties mutation remains fail-closed) |

主设计 R12–R37 的 Kafka/Pulsar correctness-critical 链接全部使用上述 immutable commit。发布包还必须记录实际 patch/binary digest、Broker rollout attestation 和 delete/recreate cuts；仅有文档 source lock 不等于实现已通过。

## 机械审计结果

| 检查 | 结果 |
|---|---:|
| Markdown UTF-8、fence 配对、relative links | PASS（53 个 Markdown 输入） |
| 主配置 YAML parse + duplicate-key walk | PASS（1 个完整 config block） |
| Stable code numeric/symbol uniqueness | PASS（103） |
| non-default retryability sets | PASS（5 个集合，互斥且只引用 registered code） |
| `CapacityDimensionV1` | PASS（1–66 连续、完整） |
| ADR file/index sequence | PASS（0001–0044） |
| `*V1` cross-document references | Prior frozen audit PASS（336 个 unique refs）；2026-08-13 delta 已登记 Gateway/Route wire types，Java-only implementation class names 不计入 Registry；完整生成器复跑仍是 release gate |
| RocksDB CF namespace | PASS（只出现七个 CF 的 registered tags） |
| `RETRY_JITTER_V1` independent recomputation | PASS（`dd78e75…d339`，first64 `15958759676622330853`） |
| stale placeholder/unfinished-decision markers | PASS（0） |
| Kafka/Pulsar correctness links use source lock | PASS |

## Release artifact matrix

这些是 implementation/release 的必交付物，不是允许延后决定的语义：

| Artifact | 固定输入 | 通过条件 |
|---|---|---|
| Generated IDL/descriptors | Registry field/enum/tag/version | 全语言 descriptor digest 一致，unknown/negative vectors fail closed |
| Shared entry conformance | one Semantic Core, Direct SDK, Delay Gateway, exact RouteSnapshot and Prepared bytes | same authenticated intent yields byte-identical NDL1/branch/outcome; crash never re-prepares against a new Route |
| Guarded client patches | Kafka trunk and Pulsar 5.0.0-M1 implementation bases from ADR 0044 plus explicit Delay K1/K2/P1 source sets | generic API, exact response evidence, ambiguity monotonicity, no name/old-protocol fallback, focused upstream-module tests, Delay artifact compile/API smoke and K1/K2 three-broker/delete-recreate evidence; the current K2 target-plus-receipt commit/abort/fence subset, real committed-EndTxn response-loss reread, and bounded Kafka/Pulsar Worker source-ACK/destination response-loss receipts are proven, while raw network/process cuts, Fetch/LSO/retention, source/placement authority and production Worker gates remain open |
| Golden vector bundle | ID、frame、canonical body/hash、key、cursor、manifest、signature、retry jitter | 每个 registered branch 有 positive/negative vector |
| Semantic catalogs | Stable errors、Profile/capability、Retry Policy、SLO、quota/capacity | catalog digest 绑定本 revision；无自由字符串扩展 |
| Benchmark config | §21 所有 `required` 数值 | §23.4 矩阵产出可复现 capacity envelope，而非单一 TPS |
| Capacity proof | dimensions 1–66、Control Reserve、RSS/cgroup、FD/disk/temp、Adapter zombie、DRR bound | checked sums 全部成立；无 double-count/oversell |
| Real-service conformance | Kafka/Pulsar/Oxia/Object Store 的 exact locked contract | §23.2 全通过，不能 stock/name fallback |
| Chaos evidence | §22 每一 failure cut + §23.3 target-isolation gates | deterministic cut、durable dump、external evidence、fresh-process recovery、invariant audit 齐全 |
| Binary/source attestation | locked commits、patches、guard/client binary digest、Broker coverage | 全 Broker rollout attested，delete/recreate 和 downgrade cuts 通过 |
| SLO evidence package | objective/load-envelope/outbox/collector schemas | timeout 不丢样本，HEALTHY 配同事件 ALL_ACCEPTED，merge 只保守变差 |
| Runbooks | restore、fence、checkpoint、DLQ replay、uncertain override、disaster boundary | 在 release candidate 上完成演练并留 evidence |
| Soak/upgrade report | 最长 retry/checkpoint/floor/GC 周期和 protocol rollout | 无 source gap、counter drift、unbounded resource 或 reader-before-writer |

`ShardQuota` 的单条 message/reservation add、remove 和 commit 入口现在先拒绝
负 bytes，再执行 checked arithmetic；`ShardQuotaTest.singleChargeOperationsRejectNegativeBytes`
覆盖了原本可能把扣费参数解释成加费的输入边界。这是 shard-local quota
完整性证据，仍不替代 Route Broker charge authority 或多 shard placement proof。

本地 `KafkaActivationBarrier` 现在在构造时复用 Source Position 的 canonical
UTF-8/NFC identity fence；无效 cluster identity 不会进入 assignment。`PulsarActivationBarrier` 现在把 non-empty inclusive cursor 的
`batchSize` 与最后 member index 一起固定；同一 ledger/entry 的 batch-shape 漂移在
`validatePosition/reachedBy` 即 fail closed，而不是把另一种 batch 形状判为已越过
barrier。兼容构造器仍明确不计入 V1 source-assignment 证据。

本地 `SloObservationOutboxStore` 已把 `meta_cf/SLO_OUTBOX` 的扫描边界收紧为
key/value `sampleId` 必须 byte-identical，并支持 record/ValueEnvelope byte
budget；首条超预算记录直接 fail closed，后续记录留给下一次 export turn。
错挂的 key 不会被导出为另一个样本，
而 collector acknowledgement 仍必须匹配当前完整 record digest 才能删除。回归
证据为 `SloObservationOutboxStoreTest`。`SloObservationOutboxV1` 现在还按 closed
objective branch 校验 Final 的 unit 与 merge direction，错误语义不会进入 durable
outbox；`SloObservationOutboxV1Test.rejectsFinalUnitAndMergeDirectionThatDisagreeWithObjective`
覆盖该边界；同一 severity 的重复 Final 还按较新 `observationRevision` 选择
evidence，避免 revision 与 evidence 混配。`SloObservationOutboxV1Test.mergeUsesNewestEvidenceWhenOutcomeSeverityIsEqual`
覆盖该边界；两个重复 Final 若携带不同的 non-null due exclusion reason
则 fail closed，而不是静默保留旧 reason，`SloObservationOutboxV1Test.mergeRejectsConflictingDueExclusionReasons`
覆盖 mutually-exclusive reason 的完整性边界。这只补足 shard-local 持久化完整性，不能
替代 SLO Start 重建、collector merge/export 或生产观测 authority。
持久化 store 的方向-only merge 入口现在还会拒绝带 exclusion 的新旧投影；
只有显式传入 exact `HEALTHY`/`ALL_ACCEPTED` companion pair 的 overload 才能写入
excluded ALL_ACCEPTED Final：`ALL_ACCEPTED` objective digest 必须与 durable Start
一致，且 `HEALTHY.validateDueCompanion(ALL_ACCEPTED)` 必须通过；旧的 pair-less
overload 对 exclusion fail closed。`SloObservationOutboxStoreTest`
`excludedFinalRequiresPairedHealthyObjectiveAtDurableBoundary` 与
`SloObservationOutboxV1Test.excludedFinalRejectsAHealthyObjectiveFromAnotherCatalogPair`
覆盖该 catalog-pair 边界。

SLO outbox 还增加了显式的 `SloObservationOutboxLimits` 本地容量 envelope。带
limits 的 store 在写入新 Start 或替换 Final 前，按严格 key/value 解码统计当前
record 数和 `ValueEnvelope` 编码字节，使用 checked arithmetic 检查替换后的总占用，
并以 `Usage` 暴露同一 bounded projection；超出 record/byte 上限的写入和过大的
export scan 都 fail closed。回归证据为
`SloObservationOutboxStoreTest.configuredCapacityBoundsRecordsAndEncodedBytesBeforeWrite`。
这仍只是 shard-local capacity guard；超过 certified envelope 后如何 durable 记录
`BAD_EVIDENCE_GAP`、从 Message/Admission authority 重建缺失 Start、collector merge/
export 和生产观测 authority 仍是 release blocker。旧的无 limits 构造器只保留给嵌入式
兼容调用，生产 wiring 必须提供 §21 的 required envelope。
`SloObservationOutboxExportRate` 现在再为每个 export turn 施加 process-local
monotonic one-second record/encoded-byte budget；拒绝只停止本次 scan，不删除或
修改 durable Start/Final。回归证据为
`SloObservationOutboxStoreTest.exportRateBoundsEachScanWindowAndResetsAfterOneSecond`
和 `SloObservationOutboxExportRateTest`。该 rate state 在进程重启后不伪装成持久
collector authority，生产配额、merge/export 和 evidence-gap projection 仍须外部
证据闭合。
其窗口初始化改为显式状态位，不再把 `Long.MIN_VALUE` 当作时钟哨兵；因此合法的
`Long.MIN_VALUE` 首个单调时间戳不会在下一次 export turn 重置已用预算。回归用例为
`SloObservationOutboxExportRateTest.treatsLongMinimumAsARealWindowStart`。
本地 `SloObservationCollector` 还把相同 `(sampleId,startDigest)` 的 at-least-once
记录合并为 deterministic projection：Start bytes 漂移 fail closed，Final 继续
使用 Registry 的 direction-aware conservative merge；配置
`SloObservationCollectorLimits` 后，sample 数和当前 canonical projection bytes
也有 checked 上限，超限不会修改已有 projection。`SloObservationCollectorTest`
覆盖 bad-evidence 单调性、Start drift、snapshot 排序以及 sample/byte capacity
rejection。它不替代生产 collector 的持久化、完整 merge/export history、授权和
metric publication。

`PersistentSloObservationCollector(Path)` 现在把该 projection 持久化为按 sample ID
排序的 canonical snapshot，并用 checksum、atomic rename、directory fsync 与
JVM/on-disk lock 保护重启、跨实例重读和 response-loss merge；
`PersistentSloObservationCollectorTest` 覆盖 conservative Final 重开、identity
failure rollback、capacity fence 与 checksum corruption。它不替代生产 collector
的 rolling-window/late-finalization retention、授权、完整 merge/export history 或
metric publication。

`SloObservationOutboxStore.reconcileDurableStarts(...)` 现在补足了本地恢复物化的
边界：它只接受调用方已经从 Message/Admission/Lane/Recovery authority 得到的 exact
`SloSampleStartV1`，按 sample ID 确定性排序并折叠 byte-identical 重复，冲突 Start
在写入前 fail closed，容量预检通过后以一个同步 batch 写入全部缺失 Start，并保留
已有 Final。`SloObservationOutboxStoreTest` 覆盖排序、重试保留 Final、冲突回滚和
容量回滚。该 seam 仍不等于 source-ordered Start reconstruction、接管时的 authority
认证或 evidence-gap `BAD_EVIDENCE_GAP` 记录；这些仍是 production release gate。

同一 store 现在还提供 `reconcileDurableStartsInBatch(...)`：source-apply caller 可以把
完整 authority-derived Start 集追加到自己的 `ShardStore.Batch`，在同一同步 WriteBatch
提交业务 projection 与 SLO denominator。该入口在追加前完成冲突/容量预检，并明确要求
一个 apply batch 只调用一次，且 batch identity 必须属于同一个 ShardStore；
`SloObservationOutboxStoreTest.reconcileInCallerBatchSharesBusinessCommitAndRollsBackTogether`
与 `reconcileInCallerBatchRejectsABatchFromAnotherShardStore` 证明 caller batch 的 joint
commit/joint abort 与 cross-store rejection。它关闭的是本地 WriteBatch 窗口，不是
生产 source-order authority、Message/Admission reconstruction 或 evidence-gap 计数。

`SloAuthoritativeStartFactory` 进一步把两条 Shard-derived reconstruction 输入固定为
typed fields：`commandApplied(...)` 使用 Registry `SourcePositionV1` canonical bytes、
Broker persistence time 和对应 SHA-256；`dueAdmission(...)` 强制完整 unsigned-32
generation、managed path、ordinary `deliverAt`/handoff `actionAt` 及调用方提供的
semantic evidence digest；store convenience entry 还验证这些 identity 属于当前 Shard。
两条路径都重新计算 sample ID、Start digest 和 checked timeout；
`SloAuthoritativeStartFactoryTest` 覆盖 Source Position identity、high-bit generation、
managed-path/time mismatch 与非法输入，store convenience test 覆盖幂等物化和 foreign
Shard 拒绝。此证据关闭的是本地 typed projection，不是生产 Message/Admission authority、
source-ordered recovery 编排或 evidence-gap 计数。

`DelayShard` 现在把 `COMMAND_APPLIED_LATENCY` 的本地 Start 物化接到真实 client
Command apply：注入外部 immutable objective 后，正常结果、stable rejection、命令冲突、
position-only dedupe 和 retry-window fence 都在各自的业务 `ShardStore.Batch` 中调用
`SloAuthoritativeStartFactory.commandApplied(...)` +
`reconcileDurableStartsInBatch(...)`，与 Message/result/Source Position 一起同步提交；
同一已提交 position 的 replay 只做 byte-identical repair。容量预检失败发生在
`db.write` 之前，`DelayShardTest.commandAppliedStartsShareClientCommandBatchesAndReplayIsIdempotent`
与 `DelayShardTest.commandAppliedOutboxCapacityAbortsTheBusinessBatch` 证明 joint commit、
replay 和 joint abort。该实现仍只证明 shard-local atomicity；objective/catalog authentication、
Message/Admission authority 驱动的 due Start、source-order recovery 编排和 production
collector/export 仍是 release gate。

该 repair 现在还按 objective 分支 fail closed：只配置 `DUE_ADMISSION_LAG` 的 shard
重放已提交 Command 时不会把空的 command-applied objective 传入 factory，也不会凭空
创建 command-applied sample。`DelayShardTest.commandReplayWithOnlyDueAdmissionObjectiveDoesNotMaterializeCommandAppliedStart`
覆盖该配置边界；它仍只是本地 replay 证据，不替代生产 SLO authority。

The source-ordered `PUBLISH_ADMISSION_V1` seam now accepts an immutable
`DUE_ADMISSION_LAG` objective only for `ALL_ACCEPTED`. Typed Admission descriptor fields plus
the local Profile/timing/shard-state gate determine the message ID, unsigned generation,
ordinary managed or managed Pulsar handoff path, and `deliverAt/actionAt`; the implementation
uses the descriptor canonical bytes as a local semantic-evidence digest and materializes the
Start in the same batch as both successful Admission and `ADMISSION_CAPACITY_GATED`.
`SloStartMaterializationException` deliberately bypasses the stale-result compatibility catch:
capacity/integrity failure leaves the source position, message and System Mutation result
unadvanced rather than producing `STALE_SYSTEM_MUTATION`. The evidence is covered by
`DelayShardTest.sourceOrderedPublishAdmissionPersistsAttemptAndMutationResultTogether` and
`DelayShardTest.dueAdmissionSloCapacityFailureDoesNotBecomeStaleMutation`. This closes only
the local typed atomicity seam; the descriptor digest is not external eligibility authority,
and immediate accepted-due Starts, HEALTHY/full-interval proof, production Profile/Oxia/Broker
authority and collector/export remain release gates.

The same stale-result catch is now fenced against native storage errors: `ShardStore.write`
exposes a typed `RocksDbWriteFailure`, and the Admission apply path propagates it before any
fallback result or source-position advance. If the native call may have committed but the
post-write ingress-fence reread/decoding check fails, the Store enters local
`WRITE_OUTCOME_UNCERTAIN`, rejects further reads/writes, skips the clean-close marker and
requires a fresh reopen from the durable incarnation; `ShardStoreTest.postWriteVerificationFailureFencesStoreUntilReopen`
covers this committed-but-unverifiable boundary. `ShardStoreTest.nativeWriteFailureHasATypeDistinctFromSemanticStaleness`
covers the pre-write callback failure classification. This preserves the design invariant
that a failed or unverifiable synchronous WriteBatch stops before Source ACK; it is local
failure classification, not evidence of a production RocksDB or source-consumer deployment.

`OwnedDelayShard` now maps that typed storage failure to local `FENCED` for
command, System Mutation, mixed replay and Claim-recovery paths before
rethrowing. The caller-owned source cursor therefore remains on the exact
physical record while the owner gate is closed; worker-level close, lease
retention and fresh-incarnation replay remain orchestration evidence still
required outside this local seam.

The local orchestration seam now closes that gap for the failure path:
`OwnerDrainCoordinator` treats `FENCED` plus `WRITE_OUTCOME_UNCERTAIN` as an
emergency teardown, stops source/scheduling once, closes the exact Store, and
checks the current Oxia lease identity before releasing it. Close failure or
release response loss is retryable without repeating business drain decisions;
an identity change is a hard no-release fence. The regression set is
`OwnerDrainCoordinatorTest.uncertainStoreClosesAndReleasesOnlyTheMatchingOwnerLease`,
`OwnerDrainCoordinatorTest.uncertainStoreNeverReleasesAReplacementOwnerLease`,
and `OwnerDrainCoordinatorTest.uncertainStoreCloseFailureRetainsAReproducibleTeardownRetry`.
This is local evidence only; production source quiescence, Oxia session
fencing and restart/restore orchestration remain outside the implementation
claim.

SLO 文档与 Registry 的 native 时间字段也已对齐：`native_handoff_ack_lag` 的起点是
`NativePreparedDelivery` field 10 的未平移 business `deliverAt`，field 11 的 shifted
Broker `deliverAt` 只用于 Broker visibility 语义；V1 native wire 没有独立的
`actionAt` 字段，不能在实现或审计中凭空引入该 API 名称。

Large-payload reservation 的本地读取也采用同一条组合身份边界：`id_cf/RESERVATION`
key 中的 reservationId 必须与 `PayloadReservation` 值一致，值中的 ShardId 必须与
当前 Delay Shard 一致；按 messageId 的 bounded lookup 在超界或发现多个 reservation
时直接 fail closed；`RESERVATION_EXPIRY` timeline entry 还必须与当前 `id_cf` 记录
byte-identical。`DelayShardTest` 覆盖错挂 key、重复 reservation 和 stale expiry projection，
避免 Cancel/Commit/expiry materializer 从不完整投影中猜测唯一预约。Object Store、Oxia
和 source-ordered reservation authority 仍不由此本地检查替代。

Typed `ActiveLaneStateV1` now rejects a present field-22 READY key unless its
bytes exactly equal the Registry `timeline/READY` projection of field 2 Lane ID,
field 8 runtime Lane version and field 16 `nextEligibleAt`. The direct codec
fence is covered by `ActiveLaneStateV1Test.readyKeyMustBeTheExactLaneVersionAndEligibilityProjection`
and `DelayShardTest.typedActiveLaneStateRejectsReadyKeyDriftAtConstruction`;
`DelayShard` and `PersistentLaneScheduler` still repeat the identity check
against the physical READY entry and current Timeline head. The scheduler
recovery/discovery boundary additionally requires typed READY/OPEN state,
non-empty encoded key and a decodable `ReadyCertificateV1` whose key fields
match the physical index; legacy adapter Lanes retain their explicit
compatibility path. This removes the possibility of rebuilding a claimable
typed READY head from only a partial key/time projection, but it remains local
integrity evidence rather than external readiness/capability authority.

The local Lane runtime projection now enforces the frozen readiness transition
graph: `RECOVERING_EVIDENCE -> READY|BLOCKED`, `READY -> BLOCKED|RECOVERING_EVIDENCE`,
and `BLOCKED -> RECOVERING_EVIDENCE`. A capability-blocked Lane can therefore
never be marked READY without an intervening evidence-recovery state; same-state
updates are idempotent and do not advance `laneVersion`. Covered by
`LaneRecordTest.runtimeReadinessMustPassThroughRecoveryBeforeBecomingReadyAgain`.
The legacy runtime constructor applies the matching cross-axis invariant and
rejects a direct `READY` projection with a non-`OPEN` admission gate; this is
covered by `LaneRecordTest.directProjectionCannotPersistReadyLaneBehindAClosedAdmissionGate`.
This closes only the local state-machine fence; activator evidence and external
readiness authority remain release gates.

The Worker outer ring now has the matching isolation behavior: blocked Shards
are removed before polling and restore filters them, while READY reactivation
rejoins the ring. The one-visit starvation regression is
`WorkerSchedulerTest.blockedShardLeavesOuterRingBeforeAOneVisitBudgetCanStarveHealthyWork`.
This is local scheduler evidence only; placement and Owner/Oxia authority
remain release gates.

`WorkerScheduler.poll` now also rolls back the complete bounded outer turn:
the snapshot covers the outer ring/cursor, Shard deficit and round/recovery
state, and every registered shard's inner scheduler counters; heads removed by
the turn are requeued in reverse order. If a later local clock, selection or
checked-arithmetic check fails after an inner head has been removed, the exact
two-level FIFO/fairness projection is restored before the exception is returned.
The focused evidence is
`WorkerSchedulerTest.clockFailureAfterAHeadWasSelectedRollsBackTheWholeWorkerPoll`.
`LaneScheduler.poll` independently restores its removed heads and inner
counters before propagating an exception, so an outer caller never relies on a
partially returned inner list for correctness.
All three local scheduler layers now use a monotonic, non-negative injected
clock guard: `LaneScheduler` reads it before head removal, `WorkerScheduler`
fences a backward/negative outer sample, and `PersistentLaneScheduler` applies
the same rule before decoding or advancing a bounded READY discovery cursor.
`LaneSchedulerTest.clockRegressionAfterAHeadWasSelectedRollsBackTheWholeLanePoll`
and `WorkerSchedulerTest.clockRegressionAfterAHeadWasSelectedRollsBackTheWholeWorkerPoll`
cover the rollback boundary. This remains an elapsed-time safety check only;
Trusted UTC, Owner/Oxia and production scheduler timing authority remain
release gates.
This closes a local process-state loss path only; it does not replace Trusted
UTC, Owner/Oxia fencing or production placement evidence.

The process-local scheduler now mirrors that graph: `markReady` cannot bypass
`RECOVERING_EVIDENCE`, and `PersistentLaneScheduler` rolls back the exact
readiness enum after a failed projection write instead of restoring only a
schedulable/not-schedulable bit; blocked/recovering Lanes leave the active ring
until READY reactivation. The regressions are
`LaneSchedulerTest.blockedLaneMustRecoverEvidenceBeforeBecomingReady` and
`LaneSchedulerTest.nonReadyRegistrationStaysOutsideActiveRingUntilRecoveryCompletes`,
alongside
`LaneSchedulerTest.failedReadyProjectionRestoresEvidenceRecoveryStateExactly`.
This is local projection evidence only; activator evidence and Owner/Oxia
readiness authority remain release gates.

The shared-resource teardown boundary now also preserves the process-level
cleanup invariant when a runtime monitor close fails: monitor failures are
retained as retryable close errors while RateLimiter, WriteBufferManager,
block-cache and native reservation teardown continue independently. The Store
open wrapper similarly releases every slot acquired by that invocation if an
`Error` escapes after acquisition, preventing an unrecoverable native/JVM path
from leaving phantom Worker capacity. This remains local teardown evidence;
fresh-incarnation recovery and production process supervision are external
release gates.

The checkpoint-create rollback boundary now also fences the Store when the
compensating runtime-metadata WriteBatch fails. The original checkpoint
failure remains primary, but subsequent operations require a fresh Store
incarnation rather than trusting an in-memory `lastCheckpointId` projection
whose durable state is uncertain. This closes the local compensation fence;
catalog publication and restart orchestration remain external release gates.

Restore failure cleanup now applies the same conservative rule to `Error`
escapes from Store teardown as to ordinary retryable close failures: the exact
Store is retried once, its directory remains preserved while teardown is
unconfirmed, and the original restore failure is kept as the primary error.
This closes the local directory-safety edge only; process supervision and
fresh-incarnation recovery remain external release gates.

The Store close/open boundary now aggregates both runtime failures and JVM/JNI
`Error` escapes across every teardown item. Handles, RocksDB, options, usage
registration and each Worker slot are attempted independently; a failed DB
slot release no longer suppresses the owned-shard or acquisition-slot release,
and the Store remains retryable until native teardown plus all capacity
transitions are confirmed. Shared Worker resource close follows the same
policy for monitors, RateLimiter, WriteBufferManager, block cache and native
reservations. Restore failures that occur after download admission now enter
the existing directory-preserving cleanup even when the primary exception is
an `Error`. This closes local lifecycle accounting only; it is not evidence of
safe recovery from a process-fatal JVM/native condition.

The embedded facade and local adapter wrappers now preserve the same
independent-close rule for `Error` as for runtime failures. The facade attempts
submission adapter, Store and shared Worker teardown in sequence, and the
prepared/bounded wrappers attempt both their delegate/native resource and
owned executor before `CloseGuard` exposes the failure for a later retry. This
keeps the local fenced-but-retryable contract aligned across Store, Worker and
client seams; it does not claim external Producer/Broker quiescence.

The pre-restore control-snapshot verifier now also aggregates `RuntimeException`
and `Error` across every read-only Column Family handle and Options close. A
failed verifier teardown no longer prevents later handles from being attempted;
the original failure remains visible and restore stays fail-closed. This is
local checkpoint-resource evidence only, not proof of external checkpoint
publication or provider recovery.

The owner-drain retry branches now preserve the same rule for a JVM/native
`Error` as for a runtime close failure. A failed Store close keeps local state
`DRAINING`, does not release the identity-bound Oxia lease, and can be retried
without replaying Claim revocation, callback polling, flush or checkpoint
steps. This is local drain sequencing evidence; external source quiescence,
lease CAS and callback/evidence authority remain release gates.

The Store open path now also handles failures after native construction. If
the short-lived acquisition slot cannot be released, the already-open Store is
closed and retried first; DB/owned-shard slots are not released by the outer
catch while that close remains unconfirmed. This prevents an open native DB
from being hidden behind phantom-free Worker capacity, while preserving the
fail-closed requirement for a fatal teardown.

Checkpoint creation now applies the same compensation fence to JVM/native
`Error` escapes: the prior runtime metadata is restored when possible, a
failed compensation enters `WRITE_OUTCOME_UNCERTAIN`, and temporary files plus
the create-concurrency slot are still attempted independently. This keeps the
checkpoint identity projection fail-closed before a fresh Store incarnation
is required.

Checkpoint upload now preserves its primary provider/intent failure while
independently attempting the Worker upload-slot release. If the upload body
succeeds but the slot transition fails, the coordinator returns a failure
instead of claiming a complete upload lifecycle; response-loss retry remains
bound to the exact pending/published intent.

Restore now preserves the same boundary for checkpoint-download admission. A
release failure after the ACTIVE pointer is published closes the installed
Store before surfacing the slot failure, avoiding a returned-error/open-DB
leak while leaving the durable incarnation available for reopen. A release
failure on an already-failed restore is suppressed onto the primary restore
or cleanup error. This closes local restore teardown accounting only; it is
not evidence of external download, object-store, or process-recovery safety.

Owner drain finalization now attempts the local fence, shard-attempt gate
release and Worker drain-slot release independently. A failed finalizer no
longer prevents later capacity release, and cleanup diagnostics are suppressed
onto the original drain/lease error; after a successful body, any unconfirmed
finalizer transition fails the call closed. The owner remains in the existing
retryable state until Store teardown and lease release are proven. This closes
the local drain-capacity accounting edge only; source/Callback quiescence and
Oxia authority remain release blockers.

The four crash-durable local projections now apply the same primary/suppressed
rule to temporary-file deletion: Recovery Catalog, SLO collector, Checkpoint
Upload Intent and Control Operation state preserve the original write/rename/
directory-fsync failure and retain cleanup failures as diagnostics. A cleanup
failure is surfaced on its own only after an otherwise successful replacement.
This closes local atomic-replacement error reporting, not external Oxia,
Object Store or production durability authority.

Both Worker resource monitors now convert probe `Error` escapes into recorded
evidence and a sticky `DRAIN_OR_MIGRATE` gate instead of allowing a scheduled
probe thread to terminate silently. Callback failures are retained as
secondary diagnostics. This closes the local monitor fail-closed edge; it is
not evidence that a process-fatal JVM/native condition can be recovered
without supervision and a fresh process/Store incarnation.

The persistent Recovery Catalog and SLO collector now restore their exact
pre-mutation in-memory snapshots when an `Error` escapes an action or durable
replacement. I/O failures keep the existing wrapped state-file error, while
runtime/JVM failures remain primary after rollback. This closes local
projection/read-after-failed-write drift only; external authority and fresh
process recovery remain release gates.

The local ingress implementation now has an explicit strict identity-policy
mode aligned with the main design's first-seen UUID/Broker checks. With a
configured Route snapshot, a Command's `retryUntil` is byte/value-bound to its
UUIDv7 timestamp and fixed retry window; first-seen Command and initial Schedule
Message identities are checked with checked lower/upper bounds around the
authoritative Broker persistence time. Invalid timing/identity input receives
an atomic `INVALID_COMMAND` result and cannot create Message state, while
existing dedupe records retain the prior conflict/no-op semantics. The local
compatibility constructors intentionally disable this mode until authenticated
Route policy is present, so they are not production ingress evidence. The
focused evidence is
`DelayShardTest.strictFirstSeenIdentityTimingBindsRetryDeadlineAndUuidAge` and
`DelayShardConfigTest.strictIdentityPolicyRequiresCommandAndPreparationWindowsTogether`;
Route policy publication, Broker-time authority and real transport integration
remain release gates.

The persistent Lane scheduler now applies the same Throwable-level rollback
boundary to restore/discovery/poll/readiness/unregister operations. An
`Error` no longer leaves the process-local ring, queue, deficit, discovery
cursor or readiness projection ahead of the last durable scheduler batch; the
original failure remains primary and rollback failures are retained as
suppressed diagnostics. This is local scheduler consistency evidence only and
does not replace process supervision, Store re-open or authoritative placement.

The Store/owner gate now closes the corresponding fatal native boundary. A
runtime/JNI `Error` from the RocksDB write call, post-write ingress-fence
verification, or the native WriteBatch/WriteOptions teardown after a write
attempt marks the Store `WRITE_OUTCOME_UNCERTAIN`; the original `Error` is
re-thrown and a fresh Store incarnation is required before replay. Direct
owner apply, Command/System Mutation/mixed replay and Claim-recovery
activation map that failure to `FENCED`, while Oxia activation and drain CAS
`Error` escapes also fence the local owner view. The deterministic authority
vectors are `OwnerLeaseTest.activationFatalAuthorityFailureFencesTheLocalOwnerGate`
and `OwnerLeaseTest.drainFatalAuthorityFailureFencesTheLocalOwnerGate`.
This closes only the local fail-closed edge; it does not prove process-fatal
JVM/native recovery or production Oxia/source quiescence.

`SystemMutationResult.from` now decodes the applied Source Position and rejects
it when the position belongs to a different Shard than the signed System
Mutation. The durable result factory therefore preserves the same source
identity boundary as replay and Command-result projections; the focused
`DurableResultTest.systemMutationResultFactoryBindsSourcePositionToMutationShard`
covers both paths. This is local construction evidence only and does not
replace source assignment, authenticated ingress, or production Oxia authority.

After `f7d3f74`, the checked-in wrapper completed
`./gradlew clean check --rerun-tasks --console=plain` on 2026-08-12 with
`BUILD SUCCESSFUL` and five executed tasks. The two control-authority methods,
the real owner-lease method, and the two recovery-authority methods were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; no production Oxia
evidence is claimed.

`PublishAttemptLedger` now applies the same shard-identity check to its
canonical Source Position that the Message identity implies. Because all
attempt lifecycle projections use the shared constructor, an UNCERTAIN or
Journal-mapping successor cannot introduce a foreign source anchor; the focused
`PublishAttemptLedgerTest.sourcePositionMustBelongToAttemptMessageShard` covers
the rejection. This remains local durable-value evidence, not production
source assignment or target-adapter authority.

After `7e7c971`, the full checked-in `clean check --rerun-tasks` gate passed
again on 2026-08-12 with five executed tasks. The five real-Oxia methods were
still skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset, so this is a
local revalidation and not production-service evidence.

`TerminalGenerationRecord`, `RetiredMessageIdentityRecord` and
`DlqExportRecord` now reject a canonical Source Position whose Shard differs
from the embedded Message identity before the value is retained. This closes
the durable terminal-history, retired-identity and DLQ-outbox construction
boundary; focused record tests and the adjusted `DelayShardTest` cover the
foreign-shard path. It remains local projection evidence and does not claim
external source assignment or provider authority.

After `7b5c30f`, the full wrapper `clean check --rerun-tasks` gate passed on
2026-08-12 with five executed tasks. The five real-Oxia methods remained
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; no production-service
evidence is claimed.

`TimelineWorkRef` now binds a `CONTROL_OVERRIDE` retry's canonical Source
Position to the self-routing `DelayMessageId` embedded in its DUE/ORDERED
timeline key. A foreign-Shard control source therefore fails before it can
become a durable timeline projection; the focused
`GenerationRuntimeIndexTest.controlOverrideTimelineRejectsSourcePositionFromAnotherShard`
covers this boundary. This strengthens local source identity only and does not
replace authenticated control authority or production source assignment.

`ResourceDeleteConfirmedRecord` 现在还把 confirmation 的 Source Position 与嵌入的
retire intent 绑定到同一个 Shard Log source identity：Shard、Kafka topic identity
或 Pulsar resource/topic identity 任一不一致，构造和 decode 都 fail closed。这样
脱离 `DelayShard` apply 路径读取的本地 GC tombstone 也不能接受 foreign-shard 或
replacement-source 的删除确认。回归证据为
`ResourceGcGuardTest.deleteConfirmationSourcePositionMustMatchRetireIntentSource`；
confirmation 还必须严格晚于 retire intent，回归证据为
`ResourceGcGuardTest.deleteConfirmationSourcePositionMustFollowRetireIntent`。这仍只是
本地 tombstone integrity proof，不替代 provider delete attestation、Oxia CAS 或
external GC orchestration。

`ResourceRetireIntentRecord` 的 durable `ProtectionSet` 也会逐个检查其中带
minimum Source Position 的 Recovery-Floor/time-bound reference，确保它们属于
retire record 的 applied Source Position Shard；foreign-shard protection 在 canonical
bytes 被保留前即拒绝。回归证据为
`ResourceGcGuardTest.retireIntentRecordRejectsProtectionSourceFromAnotherShard`。
这只闭合本地 value integrity，不替代 source-resource authority 或 external Floor
publication。

After `93147c4`, `./gradlew clean check --rerun-tasks --console=plain` passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five real-Oxia
 methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this
 is local revalidation, not real-service or release evidence.

Kafka `ReceiptJournal.AttemptIdentity` 与 Pulsar `AttemptJournal.AttemptIdentity`
现在在构造时就把 canonical Source Position 绑定到嵌入的 `DelayMessageId` Shard，
不再允许 foreign-shard 的中间 attempt identity 先存在、等到 `Mapping.create` 才
被拒绝。回归证据为
`KafkaReceiptJournalTest.attemptIdentityRequiresCanonicalSourcePositionAndMatchingShard`
和 `PulsarAttemptJournalTest.attemptIdentityRequiresCanonicalSourcePositionAndMatchingShard`；
这仍只是本地 adapter identity proof，不替代 authenticated broker assignment 或
real journal authority。

`ClaimRecord` 现在会解析保留的 DUE/ORDERED timeline key，并要求其中的 lane、
`DelayMessageId`、generation 三元组同时匹配 Claim 值和 precondition。即使只重绑
`originalTimelineKeySha256`，也不能让 Claim 指向另一条 Message；回归证据为
`ClaimRecordTest.claimRejectsTimelineKeyForAnotherMessageAfterPreconditionHashIsRebound`。
这只是本地 Claim value integrity proof，完整 Claim materialization/recovery authority
仍是 release blocker。

`RecoveryPinV1` 现在在 value 构造和 decode 边界把显式 `ShardSubjectV1` 与
`observedFloor.appliedSourcePosition` 的 Shard 绑定。一个带有 foreign-Shard
Recovery Floor 的 pin 不能先作为看似有效的中间值被序列化，之后才等 Catalog
authority 拒绝；`RecoveryPinV1Test.rejectsLineageGenerationAndDigestDrift` 覆盖了
foreign-Shard 构造失败，并保留 lineage、generation、digest 的已有回归。这里闭合的
仍是本地 recovery value integrity，不替代 Oxia session/CAS、Floor publication 或
source replay authority。

`SloObservationOutboxStore` 的通用 `ensureStart/reconcile` 路径以及
`get/scan/usage` 读取现在也复用 typed `COMMAND_APPLIED`/`DUE_ADMISSION`
Start 的 Shard fence。有效但属于其他 Shard 的 Source Position 或 self-routing
`DelayMessageId` 不能绕过专用入口写入本地 `SLO_OUTBOX`，读取侧的错挂值同样
fail closed；旧的 opaque synthetic Due fixture 仍明确保留为 compatibility seam。
证据为 `SloObservationOutboxStoreTest` 的 generic admission/reconcile/read
corruption 回归。该边界只证明本地 outbox 完整性，真实 receipt/evidence
authority、collector 路由与 production observability 仍是 release blocker。

`RecoveryCatalog` 的 snapshot install 现在拒绝 generation 为零却包含已发布
manifest/Floor/pin 的状态，也拒绝没有 manifest 却带有非零 catalog generation 的
状态。active RecoveryPin 还要求当前 Floor 沿 pinned candidate 的同一 ancestry
branch 推进；因此允许 Floor 在同一分支上越过 candidate 的历史 pin 保留，但拒绝
从 observed Floor 分叉到 sibling checkpoint 的损坏快照。`RecoveryCatalogTest` 的
两条 snapshot 回归覆盖该边界。这是本地 crash-durable projection proof，不是
Oxia pin/Floor cross-record CAS 或 source replay authority。

`StoreRecoveryMetadata` 现在还拒绝没有 `lastObservedFloor` 却携带非零
`catalogGeneration` 的 `meta/RECOVERY` 投影。该 generation 只允许表示 exact
observed typed Floor；dangling generation 会在构造和 shard DB reopen 时
fail closed。`StoreRecoveryMetadataTest.rejectsCatalogGenerationWithoutObservedFloor`
和 `ShardStoreTest.danglingRecoveryCatalogGenerationDoesNotLeaveRocksDbOpen`
分别覆盖构造与 native reopen 的损坏边界。这仍是本地 projection integrity proof，
不替代 Oxia Floor、Owner Lease/session 或 source replay authority。

After `ebb2bc5`, the full checked-in `./gradlew clean check --rerun-tasks
--console=plain` gate passed on 2026-08-12 (`BUILD SUCCESSFUL`, five executed
tasks). The five opt-in real-Oxia methods remained skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is local evidence, not
real-service or release evidence.

The Oxia single-record Recovery Catalog now rejects an over-limit manifest
count before encoding as well as while decoding. The focused regression is
`OxiaSyncRecoveryCatalogBackendTest.rejectsManifestCountAboveBoundBeforeEncodingSnapshot`;
this is a local serialization-bound proof, not evidence for the still-open
upload-intent/catalog transaction or Object Store publication gates.

After `72e31da`, the focused `OxiaSyncRecoveryCatalogBackendTest` run and the
full `./gradlew clean check --rerun-tasks --console=plain` gate both passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this
remains local evidence only.

The local directory-path guard now walks and validates every component rather
than checking only the nearest existing ancestor. An existing intermediate
symlink that redirects outside its lexical parent is rejected before any
state, lock or temporary directory can be created, while deployment-managed
system links such as macOS `/var` remain usable;
`LocalStatePathGuardTest.directoryPathRejectsExistingIntermediateSymlinkEvenWhenTargetExists`
is the focused regression. This closes a local physical-boundary case only and
does not claim external filesystem, Oxia or Object Store authority.

After this path-guard change, `./gradlew clean check --rerun-tasks
--console=plain` passed on 2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks).
The same five opt-in real-Oxia methods remained skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset; this remains local evidence only.

The concrete Oxia owner-lease backend now rereads an exact committed
single-record successor after response loss for acquire, renewal and
lifecycle transition. It accepts the reread only when shard, owner, epoch,
token, assignment/session context, state and expiry match the requested value;
otherwise it preserves the unknown/fenced outcome. The focused regressions are
`OxiaSyncOwnerLeaseBackendTest.acquireResponseLossRereadsExactCommittedEphemeralLease`,
`renewalResponseLossRereadsExactCommittedSuccessor` and
`transitionResponseLossRereadsExactCommittedSuccessor`. This is per-record CAS
evidence, not proof of source assignment, session orchestration or the required
cross-record activation/pin transaction.

The Recovery Catalog response-loss reread now applies the same exact-record
rule: catalog key and Oxia version must be valid and the complete snapshot
bytes must match. A same-bytes response for a different key is not success;
`OxiaSyncRecoveryCatalogBackendTest.responseLossWithWrongRereadRecordIdentityDoesNotBecomeSuccess`
covers the case. This is still a single-record CAS fence, not cross-record
catalog/upload-intent/Owner Lease authority.

Owner-lease release now rereads after a delete response loss and accepts
success only when the exact lease record is absent. A still-present identity or
failed reread stays unknown, and a replacement identity is not reported as
released; `OxiaSyncOwnerLeaseBackendTest.releaseResponseLossRereadsAbsenceAfterCommittedDelete`
covers the committed-delete case. This is per-record CAS evidence, not a
session/assignment or cross-record activation proof.

Recovery Catalog reads now require both the exact record key and a non-null
Oxia version before returning a snapshot. A valid-looking snapshot without a
CAS version is rejected at the boundary by
`OxiaSyncRecoveryCatalogBackendTest.catalogReadRejectsARecordWithoutAnOxiaVersion`;
this remains local single-record integrity evidence.

Owner-lease epoch and lease reads now require the exact Oxia record key,
non-null value and non-null version before decoding; malformed or redirected
single-record responses fail closed. The focused evidence is
`OxiaSyncOwnerLeaseBackendTest.epochReadRejectsARecordWithoutExactIdentityOrVersion`
and `leaseReadRejectsAResponseForAnotherRecordKey`. This closes only the local
single-record identity boundary and does not claim source-assignment,
session-orchestration or cross-record activation/pin correctness.

After this owner-lease identity fence, the focused owner-lease test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is
local regression evidence, not release evidence.

The single-record Recovery Catalog snapshot now applies the manifest-resource
count bound in both directions and validates each encoded resource against a
published manifest's checkpoint, lineage and manifest hash. Duplicate or
orphan resource identities fail before bytes are emitted; the focused
regression is
`OxiaSyncRecoveryCatalogBackendTest.rejectsResourceCountAboveBoundBeforeEncodingSnapshot`.
This is local snapshot integrity evidence only, not Object Store or
upload-intent/catalog transaction evidence.

After this catalog resource-bound change, the focused catalog backend test and
the full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is
local regression evidence, not release evidence.

The public single-record Recovery Catalog backend now rejects a checkpoint ID
with any width other than the Registry's 16 bytes before issuing a direct
manifest or Floor-coverage read. This closes the concrete backend boundary in
addition to the wrapper validation; the focused regressions are
`OxiaSyncRecoveryCatalogBackendTest.directManifestReadRejectsCheckpointIdentityWithWrongWidth`
and `directFloorCoverageReadRejectsCandidateIdentityWithWrongWidth`. The
change is local input validation and does not claim cross-record Oxia,
upload-intent, Object Store or recovery-session authority.

After this direct-backend identity fence, the focused catalog test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this
is local evidence only.

Owner Lease ephemeral writes now validate the exact lease key as well as the
Oxia version and session metadata. If the write response is malformed, the
backend accepts it only after an exact successor reread; a wrong-key response
combined with a wrong-key reread remains an integrity failure. The focused
regression is
`OxiaSyncOwnerLeaseBackendTest.leaseWriteRejectsWrongResponseAndWrongRereadIdentity`.
This is per-record Oxia evidence only and does not claim source-assignment,
session-orchestration or cross-record activation correctness.

After this ephemeral-write identity fence, the focused owner-lease test and
the full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this
is local evidence only.

Owner Epoch create/update response loss is now resolved only by an exact
reread of the expected durable epoch value under the exact key with a valid
Oxia version. The focused regressions are
`OxiaSyncOwnerLeaseBackendTest.epochCreateResponseLossUsesOnlyAnExactCommittedReread`
and `epochUpdateResponseLossUsesOnlyAnExactCommittedReread`; malformed or
redirected rereads remain failures. This closes a single-record availability
case and does not claim source-assignment, session or activation transaction
authority.

After this epoch response-loss fence, the focused owner-lease test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this
is local evidence only.

Recovery Catalog encoding now rejects duplicate checkpoint IDs before bytes
are emitted, so the direct encoder and canonical decoder enforce the same
identity boundary. The focused regression is
`OxiaSyncRecoveryCatalogBackendTest.rejectsDuplicateManifestIdentityBeforeEncodingSnapshot`.
This is local snapshot integrity evidence only; Object Store publication and
cross-record catalog/upload-intent authority remain open.

After this duplicate-identity fence, the focused catalog test and the full
`./gradlew clean check --rerun-tasks --console=plain` gate passed on 2026-08-12
(`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia methods
remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this remains
local evidence only.

The Recovery Catalog encoder now rejects a `manifestResources` map whose key
does not equal the resource's canonical checkpoint ID, preventing an alias key
from being silently normalized away. The focused regression is
`OxiaSyncRecoveryCatalogBackendTest.rejectsResourceMapKeyThatDoesNotMatchCheckpointIdentityBeforeEncodingSnapshot`.
This is local canonical-snapshot evidence only; external catalog, Object Store
and upload-intent authority remain open.

After this resource-map identity fence, the focused catalog test and the full
`./gradlew clean check --rerun-tasks --console=plain` gate passed on 2026-08-12
(`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia methods
remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this remains
local evidence only.

The Recovery Catalog encoder now rejects any snapshot with an active
`RecoveryPin`. This backend has no supported single-record representation for
the pin's create/release semantics, and encoding it as a pin-less snapshot
would silently discard the recovery fence. The focused regression is
`OxiaSyncRecoveryCatalogBackendTest.rejectsUnsupportedRecoveryPinBeforeEncodingSnapshot`;
this is a fail-closed serialization boundary, not evidence that RecoveryPin
CAS or cross-record recovery authority is implemented.

After this unsupported-RecoveryPin fence, the focused catalog test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this remains local evidence only.

The Catalog encoder now validates the complete local snapshot structure
before emitting bytes: manifest presence must carry the matching shard
identity, and the existing local validator checks manifest ancestry, Floor
projections and generation relationships. A cross-shard `catalogShard` is
rejected by `OxiaSyncRecoveryCatalogBackendTest`
`rejectsCatalogShardIdentityThatDoesNotMatchManifestBeforeEncodingSnapshot`.
This prevents a direct encoder caller from producing a record that cannot be
reopened by the canonical decoder; it does not claim external Oxia recovery
authority.

After this complete snapshot-structure fence, the focused catalog test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this remains local evidence only.

The local crash-durable Recovery Catalog encoder now validates the complete
Snapshot projection before serialization, matching the fail-closed intent of
the single-record Oxia encoder. Resource-map aliases and a catalog shard that
does not match its manifests are rejected by
`PersistentRecoveryCatalogTest.snapshotEncoderRejectsResourceMapAliasBeforeEmittingBytes`
and `snapshotEncoderRejectsForeignCatalogShardBeforeEmittingBytes`. This is a
local serialization-integrity improvement only; it does not close the
session-bound RecoveryPin, upload-intent/catalog transaction, Object Store or
real-service recovery gates.

Manifest-backed local restore now fails closed when `meta_cf/FIXED` key 10 is
absent, and it checks the persisted `CompatibleControlSnapshotV1` digest
against `CheckpointManifest.controlStateDigest` before staging can proceed to
activation. `ShardStoreTest.restoreWithManifestRejectsMissingControlSnapshot`
covers the missing projection; success/late-failure fixtures were updated to
carry a matching snapshot so RecoveryPin and lineage assertions still exercise
their intended boundary. This is local checkpoint/activation evidence only and
does not claim external catalog, Owner Lease transaction or Object Store
authority.

After this manifest control-snapshot fence, the focused `ShardStoreTest` and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is
local evidence only.

The checkpoint upload verifier now requires the same key-10 control projection
that manifest-backed restore requires: a recognized RocksDB checkpoint without
`CompatibleControlSnapshotV1` is rejected before the upload coordinator can
advance PENDING_UPLOAD to PUBLISHED. The focused regression is
`CheckpointControlSnapshotVerifierTest.rejectsMissingControlSnapshotFromRecognizedRocksDbImage`.
The no-MANIFEST legacy fixture seam is still explicit; this is local upload
integrity evidence and does not close Object Store, catalog or Oxia transaction
authority.

After this checkpoint-upload control-snapshot fence, the focused verifier and
upload-coordinator tests and the full `./gradlew clean check --rerun-tasks
--console=plain` gate passed on 2026-08-12 (`BUILD SUCCESSFUL`, five executed
tasks). The five opt-in real-Oxia methods remained skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is local evidence only.

The activation call-site audit found no production (`src/main`) caller of the
legacy `OwnedDelayShard.activateForCommands(...)` overloads. They remain only
for embedded ownership tests; V1 Worker/recovery wiring must use
`activateForCommandsWithControlSnapshot(...)`, which proves the exact persisted
shard control snapshot before exposing `ACTIVE_FOR_COMMANDS`.

Checkpoint upload now binds the recognized RocksDB image's fixed store-format
marker and `StoreMetadata` (shard identity, DB identity and Store Incarnation)
to the corresponding `CheckpointManifest` before provider I/O. A file-complete
image can no longer be published with a manifest for another DB identity or
Store Incarnation; `CheckpointControlSnapshotVerifierTest`
`rejectsManifestWhenCheckpointStoreIdentityDrifts` covers the local fence.
This remains an upload-integrity check and does not claim Object Store or
upload-intent/catalog transaction authority.

The raw-byte Claim mutation is no longer a public runtime seam. Main production
sources call only typed `DelayShard.claimForPublishV1(...)` through
`OwnedDelayShard`; the package-local primitive remains solely as its
implementation detail and for same-package/test-classpath fixture construction.
The reflection regression in `DelayShardTest` prevents accidental public
re-exposure. Focused Claim/ownership coverage and the complete local Gradle gate
passed after the change; the five real-Oxia smoke methods remained opt-in and
skipped without `NEREUS_DELAY_OXIA_ENDPOINT`, so external authority is still
unverified.

The legacy timeline `DelayShard.discoverDue(...)` scan is no longer public.
There is no production main-source caller, and its record-only limit cannot
meet the active READY path's trusted-time evidence, execution-time Owner reread
or byte/elapsed budget. A reflection regression locks package-local visibility;
same-package compatibility tests remain unchanged. Focused scheduler/runtime
coverage and the complete local Gradle gate passed, while the five real-Oxia
smoke tests stayed opt-in and skipped without an endpoint.

The remaining count-only discovery overloads for message expiry, reservation
expiry and Lane-close materialization are also no longer public. Their default
`Long.MAX_VALUE` byte/elapsed envelopes were useful for same-package semantics
tests but did not satisfy the V1 bounded-turn contract. The strict
`SchedulerBudget` plus monotonic-clock forms remain public and are called by
`OwnedDelayShard`; test-only bridges construct exact materializer fixtures
without enlarging the main artifact API. The reflection regression, focused
discovery/materialization suites and full local gate passed; external Oxia
evidence remains open.

The legacy count-only `DelayShard.discoverReady(...)` index scan is no longer
public. No main production source calls it; unlike the persistent scheduler
path, it has no byte/elapsed envelope or complete trusted-time/Owner/certificate
binding. Same-package runtime tests continue to exercise READY projection
invariants, and one ownership recovery fixture uses the test-only bridge. The
reflection regression and focused runtime/ownership/scheduler tests plus the
complete local gate passed. This API fence does not close real Oxia or trusted
clock evidence.

The persistent scheduler API now mirrors the same boundary: only
`discoverReady(TrustedUtcIntervalEvidence, SchedulerBudget)` remains public.
The no-evidence and scalar-time overloads are package-local because they omit
the certificate lifetime, exact scheduler Owner and Store Incarnation binding
required by production discovery. `LaneSchedulerTest` locks their visibility;
focused scheduler/due/Claim suites and the complete local gate passed. External
trusted-time and Oxia authority remain open.

The persistent scheduler's untimed `poll(SchedulerBudget)` overload is now
package-local too. It maps to `Long.MAX_VALUE` and is valid only for historical
same-package algorithm tests; production `WorkerScheduler` passes an explicit
due-through boundary. The reflection regression prevents re-exposure. Focused
Lane/Worker scheduler, due and Claim tests plus the complete local gate passed;
real trusted time and Oxia remain external evidence.

The Worker-level untimed poll is package-local too. It previously translated
to `Long.MAX_VALUE` before delegating through DRR, allowing an external caller
to remove future work despite the shard-level time fence. Only
`poll(dueThroughEpochMs, budget)` remains public. The Worker reflection
regression, focused scheduler/due/Claim tests and complete local gate passed;
production trusted-time issuance remains an external gate.

The innermost Lane scheduler now follows the same rule: untimed poll is
package-local, while its public poll requires an explicit due-through value.
All three scheduler layers therefore reject API-level omission of the time
boundary. The reflection regression, focused scheduler/due/Claim suites and
complete local gate passed; this still does not prove production trusted-clock
authority.

All three scheduler `offer(ScheduleWorkItem)` methods are now package-local.
They had no cross-package production caller and accepted caller-constructed
work without independently proving the READY index, typed certificate, Owner
or Store identity. Authoritative discovery still injects internally after the
complete persistent validation, while scheduler tests retain package access.
The reflection regressions, focused scheduler/due/Claim suites and complete
local gate passed on 2026-08-13; external trusted time and Oxia remain open.

The persistent scheduler's raw lifecycle methods are no longer public:
registry setup, persisted restore, fenced rebuild, readiness changes, terminal
unregister, direct requeue and explicit persist all mutate rebuildable process
and projection state without themselves proving Owner or source-ordered Lane
authority. Main production sources had no cross-package caller. A test-only
bridge preserves registration/rebuild fixtures without enlarging the artifact
API, and reflection tests lock the visibility. Full runtime/scheduler/ownership
coverage and the complete local gate passed on 2026-08-13; dedicated production
recovery/readiness/retirement coordination remains OPEN.

The same public-surface fence now covers inner `LaneScheduler` and outer
`WorkerScheduler` lifecycle mutation. Their registration, readiness/block,
ring rebuild, snapshot restore, direct requeue/replacement and unregister
methods had no main cross-package caller and could otherwise be used to bypass
the persistent/ownership coordinator. Constructors, timed poll and read-only
projections remain public. Reflection regressions and the complete local gate
passed on 2026-08-13; the production Worker scheduler coordinator is still an
explicit OPEN integration item.

The deterministic embedded-owner scheduler factory is no longer public. Its
fixed `embedded-scheduler` identity and epoch 1 can support local fixtures but
must not be mistaken for current Oxia ownership or used to mint production
READY certificate bindings. Cross-package tests use a test-only factory; the
public scheduler constructor requires explicit `OwnerIdentityV1`. Reflection
coverage and the complete local gate passed on 2026-08-13; authenticated Owner
construction remains OPEN.

The local Owner composition boundary now rejects epoch-only equivalence.
`OwnedDelayShard`'s public constructor requires a non-null full
`OwnerIdentityV1`, validates the exact Shard and lease epoch, and the strict
due/Claim, expiry, Publish Admission and Owner-authored outcome paths compare
that complete identity with the scheduler, Claim or mutation author. The
two-argument constructor is package-local and cannot authorize those paths.
Regression coverage includes null/epoch/Shard constructor drift and a
same-epoch scheduler with different deployment, worker and fencing digest.
Focused suites and the complete six-task local Gradle gate passed on
2026-08-13; five real-Oxia smokes remained skipped. This closes local
same-epoch identity substitution, but authenticated Oxia lease/session to
protocol-Owner construction in a production Worker remains OPEN.

Raw `OwnedDelayShard` lifecycle mutation is no longer a public composition
surface. Lease replacement, catch-up admission/cursor recording, activation
and drain transitions are package-local; public callers compose strict
takeover and shutdown through `OwnerRecoveryCoordinator` and
`OwnerDrainCoordinator`. Read-only projections remain public, and `fence()` is
intentionally public because it can only remove local authority. Reflection
coverage locks every overload; focused ownership/recovery/drain/source-apply
tests and the complete six-task local gate passed on 2026-08-13, with five
real-Oxia smokes skipped. This prevents API-level lifecycle bypass but does not
provide production Worker, source-quiescence, placement or Oxia evidence.

Recovery composition now uses one dependency authority graph:
`OwnerRecoveryCoordinator` constructs its recovery
`SourceApplyWorkClassExecutor` from the exact owned shard, Oxia authority,
verification key and shared WorkClass registry already passed to the
coordinator. Callers can no longer inject an executor bound to a different
shard, authority, key or queue, and recovery-only submission is package-local.
Reflection and focused recovery/source-apply coverage plus the complete
six-task local gate passed on 2026-08-13; five real-Oxia smokes were skipped.
Real Broker cursor ownership, Worker event-loop wiring and external session
authority remain OPEN.

The active source composition now also has a single dependency graph.
`SourceApplyCoordinator` internally creates its executor from the exact owned
shard, Oxia authority, verification key and shared WorkClass registry supplied
to the coordinator, so a caller cannot pair ACK/cursor handling with an
executor bound to another owner, key, authority or queue. Constructor-shape,
ACK retention and executor regressions plus the complete six-task local gate
passed on 2026-08-13; five real-Oxia smokes were skipped. The lower-level
active executor remains a bounded action API, while real Broker consumer,
ACK/commit/rewind and session evidence remain OPEN.

Drain composition now binds the exact physical runtime. The public
`OwnerDrainCoordinator` constructor requires a non-null shared WorkClass
registry, while the no-registry compatibility constructor is package-local;
an optional final checkpoint therefore cannot silently bypass the bounded
`CHECKPOINT` class from package-external production wiring. Construction also
requires the owned runtime and supplied Store to have the same ShardId and
byte-equal Store Incarnation, in addition to the existing exact shared-resource
instance check. Same-Shard foreign-Store rejection, focused drain/checkpoint
coverage and the complete six-task local gate passed on 2026-08-13; five
real-Oxia smokes skipped. External callbacks, Object Store/catalog publication
and production Worker drain authority remain OPEN.

The physical Worker resource graph now has exact configuration identity.
Every Store open, local reuse and restore path requires the complete
`ShardStoreConfig` to equal the config that created its
`SharedRocksDbResources`; mismatch fails before directory mutation, slot
acquisition or provider download. Checkpoint execution also requires its
publication/upload coordinator to use the exact same resource-envelope
instance as the active Store, so upload concurrency and rate limiting cannot
be charged to another Worker. Open/restore zero-side-effect and foreign
publication-envelope regressions, focused suites and the complete six-task
local gate passed on 2026-08-13; five real-Oxia smokes skipped. Benchmark-backed
production limits and external Object Store/Oxia evidence remain OPEN.

Final owner-drain checkpoints now bind the physical Store's persisted runtime
Owner projection. `CheckpointDrainWorkClassExecutor` requires exact equality
between `lastOpenedOwnerEpoch` and the DRAINING lease epoch both before queue
admission and again at execution. A current same-Shard lease therefore cannot
checkpoint a Store last opened by another epoch; the normal drain coordinator
persists the exact epoch before submission. Direct zero-filesystem rejection,
normal drain/checkpoint coverage and the complete six-task local gate passed
on 2026-08-13; five real-Oxia smokes skipped. Real session, catalog and Object
Store authority remain OPEN.

Scheduled checkpoint admission now derives its static accounting from the
request rather than trusting the caller. `CheckpointWorkClassExecutor` encodes
the exact Shard route incarnation/partition, scheduler due time, normalized
absolute checkpoint directory, complete canonical pending upload intent and
upload time into one value identity. `taskId` is the domain-separated SHA-256
of those bytes and the work-class charge is their exact length;
`ExecutionRequest` has no `workClassBytes` component and rejects negative upload
time before queueing. Reflection/API-shape and bounded rejection/execution
coverage plus the complete six-task local Gradle gate passed on 2026-08-13;
five real-Oxia smokes were skipped. This closes caller-controlled static queue
undercharging only: actual checkpoint-file, temp-headroom and Object Store
upload bytes/latency still require production dynamic I/O authority and remain
OPEN.

The active READY/Claim runtime now rejects physical scheduler drift. A public
`PersistentLaneScheduler.storeIncarnation()` exposes only a defensive 16-byte
identity projection, and the shared due/Claim preflight compares it with the
owned `DelayShard` Store Incarnation in addition to ShardId and full Owner
identity. A scheduler for the same logical Shard and Owner but a different DB
cannot discover READY in one Store and write a Claim into another. The
zero-action/unchanged-sequence regression, focused due/Claim suites and the
complete six-task local Gradle gate passed on 2026-08-13; five real-Oxia smokes
were skipped. External Worker construction, trusted-time and Oxia authority
remain OPEN.

Claim capacity is now bound to one process-local Worker graph. A
`WorkClassExecutionRegistry` accepts only one exact `ClaimExecutionAdmission`
instance, and both Claim and Publish Admission executors bind to it. Publish
Admission also rejects an otherwise identity-matching Reservation from another
pool before task registration or Shard Log append. Exact-pool/rebinding
regressions, focused suites and the complete six-task local Gradle gate passed
on 2026-08-13; five real-Oxia smokes were skipped. Cluster-wide/Oxia capacity,
production Worker construction and dynamic I/O admission remain OPEN.

Destination physical capacity now shares the same exact Worker graph boundary.
`WorkClassExecutionRegistry.WorkerSingleton` binds one physical-admission pool,
and cross-package `BoundedDestinationPublishAdapter` construction must supply
that registry plus a caller-owned executor. A second pool fails at construction
before transport or charge; no-registry constructors are package-local tests.
Visibility/exact-instance regressions and the complete six-task local Gradle
gate passed on 2026-08-13; five real-Oxia smokes were skipped. Production
executor sizing, Producer/channel evidence, Oxia capacity and benchmarks remain
OPEN.

Worker execution and physical resource composition are now identity-bound in
both directions. Every owner-side work-class executor and the scheduled,
restore and final-drain checkpoint executors trace through their exact Store or
coordinator and bind its `SharedRocksDbResources` to the supplied
`WorkClassExecutionRegistry`. One resources object graph rejects a second
registry, and one registry rejects a second Store resource envelope before
queue admission or I/O. `ClaimExecutionAdmission` and
`DestinationPhysicalAdmission` also reject reuse by a second registry, closing
the reverse direction that registry-only singleton checks did not cover.
`SharedRocksDbResourcesTest`,
`WorkClassExecutionRegistryTest.oneClaimAdmissionPoolCannotMultiplyCapacityAcrossWorkerRegistries`
and
`BoundedDestinationPublishAdapterTest.onePhysicalAdmissionPoolCannotMultiplyCapacityAcrossWorkerRegistries`
cover both directions. Code commit `52ba3091` and the complete six-task local
Gradle gate passed on 2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. This proves an existing object graph
cannot fork its queue/resource authorities; production bootstrap uniqueness,
JVM-wide root construction, dynamic I/O authority and external capacity remain
OPEN.

Profile timing composition now has one exact local authority graph as well.
`ProfileCatalogV1ScheduleResolver` rejects nested Profile decorators and exposes
only a package-local exact-instance check. `DelayShard` automatically decorates
a raw resolver when a catalog is supplied, but a pre-decorated resolver is
accepted only with the exact same shard `ProfileCatalog`; no catalog or a
foreign catalog fails before any Store projection read. The focused
`ProfileCatalogV1ScheduleResolverTest.decoratorCannotHideAnotherProfileCatalog`
and `DelayShardTest.decoratedScheduleResolverRequiresTheExactShardProfileCatalog`
regressions plus the complete six-task local Gradle gate passed at code commit
`832bec13` on 2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. This closes local split-brain semantic
injection between Schedule actionAt and Admission/recovery timing only;
authenticated Profile publication, source-ordered activation and external Oxia
authority remain OPEN.

Large-payload Commit verification no longer lets the current body encoding
choose a weaker authority than the durable Prepare. A Registry V1 Prepare pins
the exact `PayloadProofTrustSetRefV1(version, semanticHash)` in its
`V1ScheduleBinding`; a later legacy Commit body is still checked against that
semantic and its source-ordered activation/issuance-close state. Only a legacy
Prepare without the V1 binding can use the version-only compatibility verifier.
`DelayShardTest.registryPrepareCannotDowngradeTrustSetAuthorityWithLegacyCommitBody`
uses two different verifier keys with the same trust-set/key versions and
proves that the fallback key is rejected while reservation/quota state remains
unchanged. Code commit `897c0eec` and the complete six-task local Gradle gate
passed on 2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. This closes the local mixed-body
downgrade only; authenticated trust-set publication, catalog durability,
signer ACL and Recovery-Floor historical key retention remain OPEN.

The adjacent receipt/handle/attestation path now reuses the same exact Prepare
authority instead of letting an adapter synthesize its own trust-set ref.
`EmbeddedDelayService` reloads the durable `V1ScheduleBinding` before adapter
registration, and `InMemoryPayloadObjectStore` rejects a full semantic-ref
mismatch before it records the reservation or issues receipt/handle/proof
state. The end-to-end regression applies a V1 Prepare, closes and reopens the
same shard DB under the embedded source identity, then injects an adapter with
the same trust-set/key versions but a different semantic hash; the facade
returns `INTEGRITY_ERROR` and the foreign adapter remains unregistered. Code
commit `3c2ed49a` and the complete six-task local Gradle gate passed on
2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. This is local authority-composition
evidence only; real provider credentials, authenticated service routing and
Oxia publication remain OPEN.

The Object Store half of the Prepare authority is now explicit on the wire as
well. `PrepareLargeScheduleV1` required field 15 carries the complete
`OBJECT_STORE ProfileRefV1`; catalog-backed apply resolves its immutable
semantic/current credential Head, and the embedded adapter registration must
match both this Profile ref and the pinned trust-set ref before recording any
reservation or issuing object authority. Typed Commit proofs require full-ref
equality, while the legacy proof adapter is limited to semantic-hash equality.
The check runs before both the initial Commit transition and the
`ALREADY_COMMITTED` fast path, so reusing the same semantic hash under another
Profile identity fails closed. Protocol round-trip/kind rejection, same-hash
foreign-Profile, legacy hash mismatch, accepted Commit, committed retry and
close/reopen adapter-drift regressions passed. Code commit `5747e833` and the
complete six-task local Gradle gate passed on 2026-08-13; five real-Oxia smokes
were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. This is a
pre-activation incompatible schema correction: incomplete Prepare bytes are
not migrated or reinterpreted. Authenticated Profile/trust-set publication,
provider credentials, remote immutability and Oxia source authority remain
OPEN.

The committed projection now retains the Registry descriptor's ReservationId
and accepted ProofId instead of collapsing it to Object Store bytes alone.
`PayloadReference` writes a new local value version and continues to decode the
prior identity-less value. Registry `ALREADY_COMMITTED` requires exact accepted
ProofId/object identity and a valid signature under the retained historical
key; a source-ordered issuer close still permits that historical retry but
blocks a new first-seen proof. Both typed and legacy proof verification treat
malformed 64-byte Ed25519 signatures as verification failure rather than a
runtime exception. Value-codec, accepted-proof persistence, issuer-close exact
retry, forged signature and same-hash foreign-Profile regressions passed. Code
commit `db22d2e7` and the complete six-task local Gradle gate passed on
2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. Legacy identity-less committed local
values remain readable but cannot claim Registry historical-proof idempotency
or satisfy a typed committed-descriptor/Claim identity check; an external
migration/activation policy for such state remains OPEN.

`FilesystemPayloadObjectStore` now mirrors the strict Prepare registration
boundary of its in-memory delegate instead of exposing only the legacy
one-argument registration surface. Its restart/proof-stability test supplies
the exact pinned Profile/trust-set refs, while a dedicated regression rejects
same-version foreign trust semantics and same-hash foreign Profile identity
before any reservation, handle or regular payload file exists. Code commit
`c4af3096` and the complete six-task local Gradle gate passed on 2026-08-13;
five real-Oxia smokes were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset. This closes local adapter API parity only; the filesystem class remains
a test seam outside the embedded facade and does not prove production provider
credentials, remote immutability or Oxia authority.

Typed Claim creation now closes the adjacent compact-reference substitution
gap. `PayloadReference` is intentionally compact and therefore cannot by itself
distinguish two complete ProfileRefs that share one semantic hash. Before the
Claim WriteBatch, `DelayShard` now reloads `V1ScheduleBinding`: ordinary
Schedule requires exact Destination Profile, business metadata, delivery
window and payload branch/descriptor equality; Prepare→Commit requires the
complete Object Store ProfileRef pinned by Prepare plus expected length and
SHA-256, while the Message reference still fences object coordinates and
ReservationId/ProofId. Focused regressions reject same-hash foreign Destination
and Object Store Profile identities on both ordinary Schedule and committed
Prepare paths and prove no Claim state transition. Code commit `abc6fec1` and
the complete six-task local Gradle gate passed on 2026-08-13; five real-Oxia
smokes were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. Delivery
Capability/target/partition immutable identity is covered by the tuple
projection below; current Profile/credential/resource continuity, Object Store
fetch, Adapter/Producer and recovery authority remain OPEN.

Typed Claim now also closes the immutable Lane-identity half of materialization.
`V1ScheduleBinding` retains the exact §4.1 canonical tuple, and the shared
parser reconstructs Destination/Delivery Capability refs, Kafka or Pulsar
Broker target identity and physical partition. Claim persistence requires all
four projections to match; Kafka additionally requires its two UUID
projections to agree. `CanonicalLaneTupleV1Test` covers Kafka and Pulsar plus
the internally inconsistent Kafka tuple, while `ClaimMaterializationRuntimeTest`
covers Capability, target and partition substitution before any Claim state
change. Code commit `dc5cc765` and the complete six-task local Gradle gate
passed on 2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. Current Profile semantics, credential
lease/resource continuity, Object Store fetch, Adapter/channel, Producer and
recovery authority remain OPEN.

The bounded Claim→Admission executor now enforces the missing causal lower
bounds before action registration. `decision_time.earliest` must be at least
descriptor `actionAt` and at least `ready_certificate.issued_at.latest`; the
existing certificate-validity, expiry, Claim-deadline and retry-until upper
bounds remain unchanged. Focused regressions prove that both an early business
decision and a decision that predates certificate issuance leave zero queued
actions and zero Shard Log append calls. Code commit `c568a041` and the complete
six-task local Gradle gate passed on 2026-08-13; five real-Oxia smokes were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. The injected
Profile/payload/channel prerequisite authorities, real Broker append and
Producer ownership remain OPEN.

The large-payload timing audit found no production-code split between inline
Schedule and Prepare→Commit, but it did find a missing direct recovery proof.
Commit does not consume `lastResolvedPrepare`; after reopen it decodes the
durable Prepare body from `V1ScheduleBinding`, resolves the exact pinned
Destination and Delivery Capability semantics, and re-derives the certified
fixed-lead `actionAt`, failing closed if either semantic is unavailable. The new
end-to-end regression applies source-ordered Profile and trust-set activation,
persists Prepare, closes the DB, opens a new shard instance and commits a signed
object proof. It verifies `deliverAt=3000`, `actionAt=2500`, no discovery at
2499 and discovery at 2500. Code commit `59c4e6de` and the complete six-task
local Gradle gate passed on 2026-08-13 in 1m13s; five real-Oxia smokes were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. No wire, key or value
format changed, so no Registry or ADR update is required. Real Profile/Oxia
publication, Object Store provider behavior, Broker visibility guard and
Producer authority remain OPEN.

The same audit exposed a late-rejection gap for invalid certified timing.
Schedule already ran `expectedActionAt` during resolution, while Prepare only
checked that the Profile existed; `deliverAt < fixedLead` could therefore
reserve quota and authorize an object upload before Commit rejected the
underflow. `resolvePrepare` now validates the fixed lead before delegating Lane
resolution, using the same once-resolved Destination/Capability pair as
Schedule. The shard-level regression proves stable
`INVALID_DELIVERY_WINDOW`, zero delegate calls, no Reservation, Lane or
`V1ScheduleBinding`, and unchanged reservation quota before a valid Prepare.
Code commit `c9600447` and the complete six-task local Gradle gate passed on
2026-08-13 in 1m13s; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. Registry §1234 already freezes
underflow rejection; no wire/key/value or ADR change was needed. Real upload
and external source authority remain OPEN.

The adjacent immutable-Profile audit found four more source-apply bypasses.
The embedded/native facade checked some size fields, but replay or another
producer could reach the shard without proving that metadata matched the
Destination Adapter, ordering was allowed, payload/metadata fitted the
Destination limits, or a committed/large object fitted its exact Object Store
Profile. `ProfileCatalogV1ScheduleResolver` now rejects those Destination
violations before delegate resolution; `DelayShard` separately requires
source-ordered activation, exact semantic/current Head and `maxObjectBytes` for
the Object Store Profile used by direct committed Schedule and Prepare.
Regressions cover all stable codes and prove no Message, Reservation, Lane,
`V1ScheduleBinding` or reservation quota side effect. Code commit `8955510c`
and the complete six-task local Gradle gate passed on 2026-08-13 in 1m11s;
five real-Oxia smokes were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset. This deliberately does not claim `maxTargetRecordBytes`: the exact
target-record limit requires final Adapter serialization plus reserved metadata
and remains OPEN with the live prerequisite/Producer path. No wire/key/value or
ADR change was required; real Profile/Oxia/Object Store authority remains OPEN.

## 2026-08-15 guarded Pulsar source evidence

Delay commit `a85a91d8dfd44e8d871673f9244356ba8356c062` adds the source-side
binding `PulsarClientArtifactSourceRecordConsumer`. It consumes only through
the locked P1 `GuardedConsumer<byte[]>` API, checks the exact guard,
physical topic, attestation and non-zero connection generation, decodes the
exact `MessageIdAdv` source position and Broker entry timestamp, retains one
record until the synchronous ACK returns, and refuses silent fallback on
proof or identity failure. Commit `b5ce0fb8` only adds the second-generation
value to the smoke output.

The P1 source lock is
`nereus/delay-resource-guard-v1@f813c96687cc19e6fca1c82d3d161cf3e045c86b`
from `5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551`. The locked client,
client-api and common artifacts have SHA-256 values
`a636470f7d3f04af18980b84703a2b90f240a4bb58f77f8c19c1fd05b5bb40b2`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`.
The rebuilt distribution is
`bfe0c479c60db1a7a56f4548bd821d218c4c284dceb7c112d92f425606adec37` and the
temporary image is
`sha256:735e2a6b952e2f7d4c8fc4c7a7b0d4ec2a852a9f4a9b21e82b076477cf19669f`.

The latest isolated Docker run used project
`nereus-delay-pulsar-e2e-1786743812-11877` on ports `19827,19828`. The writer
returned `initial=PERSISTED, stale=DEFINITIVELY_NOT_PERSISTED,
replacement=PERSISTED`. The source path replayed an unacknowledged `11/0`
record across connection generations `1` and `2`, delivered the next record
at `11/1`, and observed no record after both synchronous ACKs. Cleanup found
no matching Compose resources.

This is current single-node guarded `SUBSCRIBE`/replay/ACK evidence. It does
not promote P1 or D3 to release status: unload, multi-broker failover,
old-peer proxy compatibility, source session ownership/rewind, Direct SDK
integration, Worker production wiring and the remaining D3/D6 artifact and
chaos gates remain OPEN.

## 2026-08-15 Gateway Oxia admission CAS evidence

Delay commit `de1da743` adds the durable admission composition
`OxiaGatewayAdmissionController` over a strict canonical
`GatewayAdmissionRecordV1`. The record is keyed by authenticated tenant-scope
digest and stores expiring leases for independent schedule, retry-uncertain
and control pools; schedule bytes are accounted in the same CAS successor.
Reserve/release retries are bounded, expired leases are reclaimed at the
trusted-clock fence, and a lost response is accepted only after exact lease
identity reread. `OxiaGatewayAdmissionControllerTest` covers canonical
tampering, pool isolation, hard byte quota, expiry, tenant separation and
reserve/release response-loss recovery.

The full local `check` passed at `de1da743` in 21 actionable tasks, while the
five opt-in real-Oxia methods were skipped without an endpoint. Follow-up
commit `b6154072` adds `OxiaRealGatewayAdmissionSmokeTest` to the Docker
harness. The 2026-08-15 run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786746636-41339` on host port `16651`, and the
selected real-service tests reported `BUILD SUCCESSFUL`; it reread one exact
canonical admission record with zero leases after expiry/release. This is
live single-node Oxia evidence, not quota-rate/load proof, deployment/HA
observability, cross-record Gateway transactionality or production Gateway
wiring; those release boundaries stay OPEN.

## 2026-08-15 native eligibility and Route barrier audit

Commit `8e404a30` adds `VerifiedNativePreparationSnapshotCache` as a local
cache boundary. Installation verifies canonical Profile/Snapshot bytes and
the configured issuer signature; `NativePreparationEligibilityV1` then binds
the candidate to the authenticated principal, signed active Route, exact
Pulsar AUTO_FAST capability, target partition policy, clock bound, time
window and record limits. The shared Core returns the already-prepared
managed bytes when no candidate is eligible, and
`prepareScheduleSubmissionV1(..., SubmissionModeV1)` prevents a Direct SDK
caller from injecting a target or native credential candidate.

Commit `57d6dfd7` adds `ActivationBarrierV1.toSourceBarrier` and
`RouteSourceAssignmentFactory`, so the Worker source assignment can preserve
the signed Route incarnation, physical resource, partition and Pulsar guard
connection evidence. The full local gate at this code head was:

```text
GRADLE_USER_HOME=/tmp/nereus-delay-full-gradle \
  ./gradlew check --no-daemon --console=plain
```

It passed with `BUILD SUCCESSFUL` in 21 actionable tasks; the five opt-in
real-Oxia tests were skipped without an endpoint. Focused tests also passed
for `VerifiedNativePreparationSnapshotCacheTest`, `ActivationBarrierV1Test`
and `SourceAssignmentTest`.

This is local conformance evidence only. The configured public key is not an
issuer/catalog authority, and the projection is not activation publication.
Live Broker probes, native credential resolution, Oxia activation/session
authority, guarded source ownership and the production Worker vertical
remain OPEN release gates.

## 2026-08-15 Route-authorized Worker assignment audit

Commit `f8ffaff9` adds `RouteSourceAssignmentResolver`, which obtains the
current or exact historical Route only through the tenant-scoped
`RouteSnapshotProvider` before projecting the selected partition's signed
barrier. Missing/unauthorized historical Route data fails closed. The
resolver does not publish an assignment, perform the Oxia Owner Lease CAS, or
establish Broker source ownership; those are still external activation gates.

`RouteSourceAssignmentResolverTest` and `SourceAssignmentTest` passed. The
full local gate at the current code head `1cd64b72` passed with
`BUILD SUCCESSFUL` in 21 actionable tasks; five opt-in real-Oxia tests were
skipped without an endpoint. The separate JWT test change in `1cd64b72` makes
its signature-mutation negative vector deterministic and is not evidence of
live certificate deployment.

## 2026-08-15 native capability issuance audit

Commit `3bae4a6b` adds the issuance-side boundary. It verifies the signed
Route bytes and exact catalog Profile/Binding/Head, checks the signed
credential-equivalence attestation, obtains principal-scoped Broker guard
evidence, bounds snapshot expiry by every supplied prerequisite, and requires
the external authority to return exact credential protection through that
expiry before exposing the issuer-signed snapshot.

The focused issuer tests passed. This closes the local ordering and
validation seam only. It does not provide the production Oxia protection
transaction, live Pulsar guard authority, credential resolver, key rotation or
real issuer/catalog deployment evidence; those release gates remain OPEN.

## 2026-08-15 guarded Kafka Worker vertical audit

Delay commit `c72cac90` adds the opt-in `KafkaClientArtifactWorkerSmoke`.
It uses the locked Kafka K1 guarded Producer and Consumer to publish one
record before activation, recover offset 0 through
`KafkaClientArtifactRecoverySourceCursor` and `OwnerRecoveryCoordinator`,
activate the `OwnedDelayShard` at the exact exclusive barrier, then consume
offset 1 through `KafkaClientArtifactWorkerSourceFactory` and
`WorkerShardRuntime`. The smoke proves Fetch v13 evidence, exact Kafka source
identity, RocksDB `WriteBatch` apply before native `commitSync`, committed group
offset 2 and exact owner-lease release during drain.

The isolated three-broker Docker run used Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786757667-58603`, and ports
`19195,19196,19197`. The source and Worker output was:

```text
Kafka source ACK smoke passed: topicId=ay9r1XMxQUycBBCYwxqqvg, firstOffset=0, secondOffset=1, committedAfterRestart=empty
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch and commitSync ACK
```

K1 delete/recreate and survivor-broker failover plus K2 target/receipt
commit/abort/replacement fencing passed in the same run; the harness cleaned
its matching Docker resources. This is an opt-in real-Kafka integration
evidence cut. Its owner authority is a deterministic in-memory backend wrapped
by `OxiaOwnerLeaseStore`, so it does not prove network Oxia session authority,
Route assignment publication, placement, Broker source ownership, real
Pulsar Worker apply/ACK, due/Lane/publish/checkpoint production wiring or
failure-injection/crash recovery. Those remain release blockers.

## 2026-08-15 Kafka Worker real Oxia authority audit

Delay commit `a7fd5fa7dd35d5d8535d3c63e577208d29fc2c5` adds the optional
`NEREUS_DELAY_KAFKA_WITH_OXIA=1` path to the Kafka real-client harness. The
Worker smoke connects to Oxia with a unique client/key prefix, establishes an
ephemeral session marker, derives the `OwnerLeaseContext` identity from the
returned session metadata, and verifies the marker before the assignment
acquire and subsequent drain/release path. The normal command remains the
deterministic in-memory mode when the endpoint is absent.

The integrated source-locked run used Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, Kafka Compose project
`nereus-delay-kafka-e2e-1786759086-73769` with ports `19300,19301,19302`, and
Oxia Compose project `nereus-delay-kafka-oxia-e2e-1786759086-73769` with port
`16656`. It passed the guarded source ACK/restart, Worker recovery at offset
0, RocksDB apply at offset 1, synchronous ACK, exact drain release, K1/K2
delete/recreate/failover cuts, and printed:

```text
Kafka Worker authority smoke passed: real Oxia session-bound lease
```

The run proves only this real Oxia session-bound authority cut around a
single smoke-created assignment. It does not prove RouteSnapshot publication,
placement, catalog-driven assignment, production multi-shard Worker wiring,
real Pulsar Worker apply/ACK, or D6 crash/failure-injection gates.

## 2026-08-15 guarded Pulsar Worker vertical audit

Delay commit `202368d46fedfe12ae414edaa9c3db32cc8e5073` adds
`PulsarClientArtifactWorkerSmoke` and the `runRealPulsarWorkerSmoke` E2E task.
The smoke captures a post-seek P1 guard proof, uses the exact same guarded
consumer for no-ACK `OwnerRecoveryCoordinator` replay and the active source
loop, applies the next record through `WorkerShardRuntime` and RocksDB
`WriteBatch`, then performs the synchronous source ACK and owner drain/release.

The real-client run used P1
`nereus/delay-resource-guard-v1@358ce4a1033bd566faebcd3465c3ba4606f3c83f`,
distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`, image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
Compose project `nereus-delay-pulsar-e2e-1786760203-85592`, and ports
`19930,19931`. It passed the existing guarded writer/delete-recreate and
source replay/ACK smokes and printed:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=15/0, active apply ledger/entry=15/1, guarded SUBSCRIBE, RocksDB WriteBatch and ACK
```

This is single-node, non-batch, smoke-created assignment evidence with a
deterministic in-memory owner authority. Network Oxia session/placement,
Route publication, multi-broker failover, production multi-shard Worker
wiring and D6 crash/failure-injection gates remain open.

## 2026-08-15 Pulsar Worker Oxia authority audit

Delay commit `10e21cbf0e6f741f10b353c56a316a0b57b71b9d` adds the opt-in
`NEREUS_DELAY_OXIA_ENDPOINT` path to `PulsarClientArtifactWorkerSmoke`; the
default Worker path remains in-memory. Commit
`0da18a7b4d6040eeb6700195a1132ee224087ffa` makes the optional Worker Gradle
arguments safe under `set -u`.

The real Oxia run locked P1 at
`nereus/delay-resource-guard-v1@358ce4a1033bd566faebcd3465c3ba4606f3c83f`
from `8dae0236c0a0d405ed7f8303081080520fe91551`, Oxia at
`37a17bef17202d5fd6e23282da5fd26d94865484`, P1 image at
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
and Oxia image at
`sha256:4fdba6125c3f3ceca0d5ebe0224464ec83eb815e91999e1910660c60416231ca`.
Pulsar Compose project
`nereus-delay-pulsar-e2e-1786761304-98904` used `19940,19941`; Oxia Compose
project `nereus-delay-pulsar-oxia-e2e-1786761304-98904` used `16657`. It
printed:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=15/0, active apply ledger/entry=15/1, guarded SUBSCRIBE, RocksDB WriteBatch and ACK
Pulsar Worker authority smoke passed: real Oxia session-bound lease
```

The matching containers, networks and volumes were absent after cleanup; the
locally built Oxia image remained. This is real session-bound owner authority
around one smoke-created assignment. It is not placement, RouteSnapshot
publication, Broker source ownership, multi-broker failover, production
multi-shard Worker wiring or D6 crash/failure-injection evidence.

## 2026-08-15 authoritative Worker placement publication audit

Delay commit `759c4a49b54395211c8ee02c2705006525288fe3` adds
`WorkerAssignment`, strict `SourceAssignment`/assignment codecs,
`WorkerAssignmentAuthority`, the in-memory CAS authority,
`OxiaSyncWorkerAssignmentBackend` and `WorkerAssignmentCoordinator`. The
coordinator is deliberately two-stage: `WorkerPlacementPolicy` scores local
candidate telemetry, then the selected assignment is published through the
authority and must be reread at the exact revision and canonical bytes by
`requireAccepted` before native Kafka or Pulsar source setup. Oxia publication
uses a revisioned canonical record, expected-revision CAS, exact idempotence,
placement-epoch monotonicity and response-loss reread; the connected-client
constructor preserves the Oxia session fence.

The focused codec/authority/coordinator/Oxia tests passed, as did the isolated
real Kafka and real P1 compilation/checkstyle gates. The fresh default Kafka
run used Compose project `nereus-delay-kafka-e2e-1786763617-28066` on
`19420,19421,19422` and printed:

```text
Kafka Worker assignment publication/acceptance passed: revision=1, worker=kafka-worker, authority=in-memory
```

The corresponding fresh default Pulsar run used Compose project
`nereus-delay-pulsar-e2e-1786763739-29494` on `19970,19971` and printed:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=in-memory
```

The opt-in network-authority runs used Kafka/Oxia projects
`nereus-delay-kafka-e2e-1786763887-31303` /
`nereus-delay-kafka-oxia-e2e-1786763887-31303` on `19430,19431,19432` /
`16658`, and Pulsar/Oxia projects
`nereus-delay-pulsar-e2e-1786764116-34287` /
`nereus-delay-pulsar-oxia-e2e-1786764116-34287` on `19980,19981` / `16659`.
Both printed the same placement line with `authority=real Oxia
session-bound`; the Pulsar run also printed
`Pulsar Worker authority smoke passed: real Oxia session-bound lease` and the
final P1 E2E line. Matching containers, networks and volumes were absent after
cleanup.

This closes the authoritative per-shard publication/acceptance and exact
Worker pre-wiring reread cut for smoke-created assignments. It does not close
catalog-driven multi-shard placement, capacity-envelope authority, signed
RouteSnapshot-to-authority publication, source ownership transfer/reconnect,
multi-broker failover, due/Lane/publish/checkpoint production wiring or D6
crash/failure-injection gates.

## 2026-08-15 signed Route publication to Worker assignment audit

Delay commit `e173cf0e02e701229f07c37ccac926416ea5c3cb` binds the canonical
Worker assignment to `RouteSnapshotV1.snapshotDigest` and adds
`RouteWorkerAssignmentCoordinator`. Active and historical Route resolution
still goes through the tenant-scoped provider; publication then uses the same
assignment CAS authority, and acceptance rereads the exact historical Route
projection before native Worker setup. The negative path rejects assignments
that were created without a Route binding or whose snapshot digest no longer
matches the authorized historical Route.

`OxiaRealRouteWorkerAssignmentSmokeTest` was added to
`e2e/run-oxia-real-service.sh`. The selected real-service suite passed with
`BUILD SUCCESSFUL` against Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786765353-47776`, port `16660`, and image
`sha256:7001f39d94a8d21d74928aad06e7666fcf4bcf3879ef6d27940c9a7ef8db702f`.
It exercised separate publisher, provider and assignment-authority sessions;
matching container/network cleanup passed.

This closes the real Oxia signed Route event/head → Route cache → route-bound
assignment CAS cut for one assignment. It does not prove catalog-driven
multi-shard placement, capacity-envelope authority, Broker source ownership
transfer/reconnect, multi-broker failover, due/Lane/publish/checkpoint wiring
or D6 crash/failure-injection gates.

## 2026-08-15 Worker final checkpoint-on-drain audit

Delay commit `2dd2cfff83f4d029972cf7fbeb569fbf4538c026` wires an exact
checkpoint identity into the real Kafka and Pulsar Worker drain paths. Each
smoke invokes the bounded `CHECKPOINT` work class, verifies the local
`CheckpointFileInventory` is non-empty, and checks that the session-bound owner
lease is released only after the final checkpoint and Store close. This is a
local RocksDB checkpoint evidence cut, not an object-store manifest or
publication claim.

The fresh Kafka run used project `nereus-delay-kafka-e2e-1786765675-51303` on
ports `19440,19441,19442`, Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
broker image
`sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`.
It printed:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
```

The fresh Pulsar run used project `nereus-delay-pulsar-e2e-1786765675-51304`
on ports `19990,19991`, P1 source
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`, and P1
image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`.
It printed:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=15/0, active apply ledger/entry=15/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
```

Both harnesses exited successfully and removed their matching containers,
networks and volumes. This closes the bounded local checkpoint/inventory and
checkpoint-before-lease-release ordering for these two real-client smokes. It
does not close due/Lane/publish orchestration, object-store checkpoint
publication, crash recovery, multi-broker failover or production multi-shard
Worker wiring.

The same code was rerun with network Oxia authority. The Kafka/Oxia projects
were `nereus-delay-kafka-e2e-1786766242-57688` /
`nereus-delay-kafka-oxia-e2e-1786766242-57688` on
`19450,19451,19452` / `16661`, with Oxia image
`sha256:22ec5f247796cce0bd78017bc78f110e1f865d26b3af5ce74f7a45886532efa4`.
The Pulsar/Oxia projects were `nereus-delay-pulsar-e2e-1786766242-57687` /
`nereus-delay-pulsar-oxia-e2e-1786766242-57687` on `20000,20001` / `16662`,
with Oxia image
`sha256:fb3da338d4b1ff5e0974b74837c094775c708aaa9e5470650cdd487185493f55`.
Both printed the final-checkpoint Worker line and
`real Oxia session-bound lease`, then exited successfully with matching
containers, networks and volumes absent. The optional Pulsar classpath emitted
multiple-provider SLF4J warnings; this does not change the successful exit.
These runs close only checkpoint-before-release under network owner authority,
not object-store publication, due/Lane/publish orchestration, crash recovery,
multi-broker failover or production multi-shard Worker wiring.

## 2026-08-15 active-owner Lane/READY scheduling audit

Commit `c124b216` adds the missing Worker-side composition around the already
implemented strict scheduler actions. `WorkerSchedulingRuntime` restores the
accepted active Lane set and persisted fairness projections, performs a fenced
authoritative READY rebuild, submits due discovery to `DUE_SCHEDULER`, and
exposes only a strict READY poll for the next Claim handoff. The regression
executes both the queued discovery action and the Owner-reread poll through
that composition.

The new entrypoint does not promote the release state: Claim materialization
and permits, external Profile/Object Store/credential readiness, signed
Publish Admission append/ACK evidence, checkpoint Intent/Catalog/Object Store
publication, multi-shard Worker scheduling, crash cuts and broker failover
remain open. This is a local composition audit with no Docker or network
service evidence.

## 2026-08-15 Worker scheduling lifecycle audit

Commit `579ad3ba` binds `WorkerSchedulingRuntime` to the optional
`WorkerShardRuntime` composition. All scheduling entrypoints use the shared
runtime resource admission and are rejected after the drain callback fences
source/scheduling; this closes the local lifecycle ordering gap between source
quiescence and new due/Claim/Publish/Checkpoint admission.

The change remains below the release gate. It does not establish automatic
Claim materialization, external readiness, Shard Log outcome evidence,
Object Store checkpoint publication, multi-shard scheduling or crash/failover
evidence.

## 2026-08-15 Worker checkpoint queue and atomic Oxia publication audit

Delay commit `d9b06f5e` composes `claimDue` → bounded `CHECKPOINT` admission →
`CheckpointExecutionCoordinator` through `WorkerCheckpointRuntime`. The
execution-time prerequisite gate is deliberately injected: it rereads the
Owner/session and exact pending-intent/catalog prerequisites after queue wait.
When that gate fails before physical I/O, the exact scheduler claim is
completed without creating a checkpoint directory or calling the upload
adapter, so a later due turn can retry the same schedule boundary.

Delay commit `bdcd4ddb` adds the production Oxia publication authority for the
client surface available in this checkout. `OxiaSyncCheckpointPublicationBackend`
stores the catalog snapshot and upload-intent projections in one canonical
record and commits the provider resource identity, PUBLISHED intent and
catalog manifest in one version CAS. `CheckpointUploadCoordinator` enters this
path only when the intent authority is the combined backend, while
`CheckpointPublicationCoordinator` rejects an atomic backend paired with a
different catalog authority. The existing split Oxia backends remain
per-record CAS seams and still reject the unsupported cross-record operation.

The Dockerized real-Oxia suite included
`OxiaRealCheckpointPublicationSmokeTest` and passed at Oxia
`37a17bef17202d5fd6e232da5fd26d94865484` with project
`nereus-delay-v1-oxia-e2e-1786768622-85502`, port `16663`, and image
`sha256:2d133d6ff493f0fffd4ac744a448d735d84f7ed08df42db9bd1df7b630477d03`.
The evidence is one shard, real Oxia intent/catalog CAS plus a local
filesystem upload adapter. It is not remote Object Store, Owner-session gate,
multi-shard production wiring, automatic Claim/Publish orchestration, crash
recovery, broker failover or release PASS.

## 2026-08-15 Worker Claim/Publish command composition audit

Delay commit `a025fade` adds `WorkerCommandRuntime` and attaches its optional
Claim/Publish graph to `WorkerShardRuntime`. Command submissions and bounded
turns use the same `SharedRocksDbResources` admission gate as source and
scheduling; the drain callback therefore fences new Claim and Publish
admission together with source and due/READY scheduling.

The wrapper carries either exact caller-prepared materialization or the
derived V1 materialization from the accepted durable binding, current Message
and canonical Lane tuple. The derived path runs behind the same strict
Owner/READY fence before queue admission and then enters the existing fenced
executor. It does not manufacture external readiness, payload serialization,
Claim charge, Publish descriptor/Ready Certificate, Profile/Object Store state,
Broker append evidence or Source Position. This is local composition evidence,
not a real Shard Log append/ACK, automatic Publish pipeline, multi-shard,
crash, failover or release PASS.

## 2026-08-15 Oxia checkpoint Owner/session gate audit

Delay commit `8918891a` extends `OxiaRealCheckpointPublicationSmokeTest` with
an assignment-bound lease from the same real Oxia session used by the
publication backend. The execution-time checkpoint gate rereads that lease
after queue admission and checks exact assignment/session identity, active
lifecycle state and expiry before local checkpoint I/O. The smoke then
releases the exact lease and verifies that the authority is empty.

The fresh run passed the selected real-service tests at Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, project
`nereus-delay-v1-oxia-e2e-1786769822-98671`, port `16664`, with
`BUILD SUCCESSFUL`. This upgrades the previous one-shard publication smoke
with Owner/session evidence only; it remains local filesystem Object Store
adapter evidence and is not remote provider durability, multi-shard,
crash/failover or release PASS.

## 2026-08-15 Kafka Worker survivor-broker failover audit

Delay commit `7a839678` makes the real Kafka harness run the complete Worker
vertical once more after `kafka-1` is stopped. The second run bootstraps only
from brokers 2 and 3, creates a fresh source topic, recovers offset 0,
applies offset 1 through RocksDB before synchronous `commitSync`, and drains
through the final local checkpoint.

The fresh run passed with Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
project `nereus-delay-kafka-e2e-1786769898-99544`, ports
`19136,19137,19138`, and survivor bootstrap `19137,19138`. The all-broker and
survivor Worker lines both passed and the harness cleaned its resources.

This is survivor-bootstrap evidence for one smoke-created partition. It does
not prove in-flight source ownership transfer, ACK response-loss recovery,
Pulsar multi-broker failover, multi-shard placement, crash cuts or release
PASS.

## 2026-08-15 Kafka Worker same-topic failover/resume audit

Delay commit `3ca85c74` adds explicit `prepare` and `resume` modes to the K1
Worker harness. One exact restart topic is persisted before broker 1 is
stopped; a new Worker JVM then bootstraps only from brokers 2 and 3, recovers
the prepared offset 0 record, applies the next record at offset 1 through the
guarded Fetch v13 and RocksDB WriteBatch path, ACKs with `commitSync`, writes
the bounded final checkpoint and releases the exact Owner lease.

The fresh run used Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
project `nereus-delay-kafka-e2e-1786771524-17482`, ports
`19270,19271,19272`, and survivor bootstrap `19271,19272`. The preparation,
resume and complete harness exited successfully and cleaned their matching
Docker resources. The resumed Worker line was:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
```

This is one source-locked same-topic fresh-process recovery cut. It does not
prove same-process source ownership transfer, ACK/Fetch/commit response-loss,
Pulsar multi-broker failover, multi-shard placement, crash cuts at every
WriteBatch boundary or release PASS.

## 2026-08-15 Pulsar Worker broker-restart resume audit

Delay commit `fe8879b3` adds a two-process cut to the P1 Worker harness. The
first process persists one guarded record on the standalone broker's named
volume; the harness restarts the broker container; a new Worker JVM then
resumes the same topic, recovers the prepared record, applies and ACKs the
next record, writes the final local checkpoint and releases the exact lease.

The fresh run passed with P1 source
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
project `nereus-delay-pulsar-e2e-1786770623-7577`, ports `19950,19951`, and
resume recovery/apply positions `17/0` → `24/0`. The preparation, resume and
full harness exited `BUILD SUCCESSFUL` and cleaned their matching resources.

This is single-node broker-process restart/resume evidence with the
deterministic in-memory Owner authority. It does not prove Pulsar
multi-broker failover, live source ownership transfer, ACK response-loss,
crash cuts at every WriteBatch boundary, multi-shard placement or release
PASS.

## 2026-08-15 Kafka Shard Log System Mutation append/replay/ACK audit

Delay commit `02f4b458` closes a bounded Kafka-side mutation transport seam.
The canonical System Mutation body is the sole source of the replay logical
identity, and the Kafka decoder preserves the ordered Client Command/System
Mutation frame union. Both the no-ACK recovery cursor and the active source
carry the same signed mutation object and source-position guard; active ACK
still occurs only after the guarded consumer's `commitSync` succeeds.

The guarded K1 appender accepts only response evidence bound to the exact
cluster, TopicId, topic and partition, with a nonnegative offset and Broker
append time. It maps timeout, ordinary failure and incomplete evidence to
`UNKNOWN`; it does not convert a missing Produce response leader epoch into
fabricated evidence. Fetch replay can supply the optional epoch later while
the route/resource/offset/append-time identity remains fixed.

The source-locked fresh run used Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
Delay `02f4b458`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786773092-35482`, ports
`19570,19571,19572`, mutation TopicId `7jN1ZJcgRPSZILqxxNLqVw`, and mutation
offset `0`. It printed the guarded append/replay/ACK success line, and the
full source/Worker/K1/K2 harness exited successfully with cleanup complete.

This is one source-locked partition and one `TIME_FENCE` mutation. It is not
evidence of signature trust-set authorization, mutation-to-RocksDB apply,
automatic Claim/Publish orchestration, Pulsar mutation apply, response-loss
recovery, crash-boundary coverage, multi-shard placement or release PASS.

## 2026-08-15 Pulsar Shard Log System Mutation append/replay/ACK audit

Delay commit `54c58557` closes the corresponding bounded P1 transport seam.
`PulsarClientArtifactSourceRecordDecoder` preserves the ordered Client
Command/System Mutation frame union. The guarded appender uses the P1
resource-guarded Producer and binds its result to a stable guarded SUBSCRIBE
proof; malformed or changed producer response identity, ledger/entry, batch
shape, attestation, Broker timestamp or source proof is `UNKNOWN`. Only the
typed P1 guard rejection with response evidence can establish definitive
non-persistence.

The source-locked fresh run used P1
`nereus/delay-resource-guard-v1@358ce4a1033bd566faebcd3465c3ba4606f3c83f`,
Delay `54c58557`, distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5a1d137d`, image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
Compose project `nereus-delay-pulsar-e2e-1786775596-63266`, ports
`19916,19917`, and the three locked client artifacts recorded in the
implementation status. It printed the guarded append/replay/ACK success line
for `ledger=15, entry=0`; the complete source/Worker/restart harness exited
`BUILD SUCCESSFUL` and cleaned its resources.

This is one source-locked single-node partition and one `TIME_FENCE` mutation.
It is not mutation-to-RocksDB apply, signature trust-set authorization,
Pulsar multi-broker failover, response-loss/crash recovery, automatic
Claim/Publish orchestration, multi-shard production wiring or release PASS.

## 2026-08-15 automatic V1 Claim materialization and Worker handoff audit

Delay commit `5dbd0874` makes the durable Claim projection derivable at the
production handoff boundary. `CanonicalLaneTupleV1.project` parses the exact
Registry tuple; `DelayShard.resolveClaimMaterializationV1` combines its
Destination/Capability Profile refs, Broker target and physical partition
with the current Message generation, Timeline `actionAt`, adapter metadata and
the accepted Schedule/Prepare payload branch. A committed Prepare payload
reconstructs the descriptor from the persisted Object Store reference and
retains reservation/proof identity. Missing V1 binding, timeline or commit
proof is a fail-closed error.

`OwnedDelayShard`, `ClaimHandoffWorkClassExecutor`, `WorkerCommandRuntime` and
`WorkerShardRuntime` expose the derived path while retaining the explicit path
for compatibility. The same external prerequisite gate still receives the
derived bytes, and trusted time, charge/deadline, live Profile/credential/
channel authority, payload serialization, Producer ownership and Publish
descriptor/Ready Certificate remain independent inputs. Focused tests prove
inline and committed-object equality with the strict Claim validator.

This closes durable V1 Claim materialization and local handoff composition for
one shard. It does not promote the result to automatic channel/Ready
Certificate preparation, external provider authority, Pulsar mutation apply,
response-loss/crash evidence, multi-shard Worker production wiring or release
PASS.

## 2026-08-15 Claim-derived Publish descriptor handoff audit

Delay commit `4865ba4f` adds the derived Publish Admission path. It takes the
exact Claim and an externally-authorized `ChannelResourceIdentityV1`, derives
the adapter/lane/resource/profile/payload/timing projection, the next
replay-stable attempt identity and Reserved Publish metadata, then feeds the
result into the existing canonical signed `PUBLISH_ADMISSION` builder. The
explicit descriptor path remains available and the focused executor test
compares its canonical descriptor bytes with the derived path.

The channel is intentionally not inferred: producer or transactional
identity, resource-guard attestation, credential binding/use lease and channel
generation are external authority. Ready Certificate, trusted decision time,
retry deadline, signing key and live prerequisite checks also remain explicit.
This closes local Claim → descriptor composition, not automatic channel/Ready
Certificate preparation, Broker append/ACK, response-loss/crash evidence,
Pulsar mutation apply, multi-shard production wiring or release PASS.

## 2026-08-15 bounded READY-to-derived-Claim Worker wiring audit

Delay commit `e7495086` adds a one-head `WorkerShardRuntime` entrypoint. It
narrows the scheduler poll budget to one message, applies the active
Owner/Store fence, and immediately submits the exact head through the derived
Claim handoff. Existing Claim executor queue-rejection and pre-commit failure
paths retain the scheduler's exact requeue identity; no batch rollback guess is
introduced.

The entrypoint still requires trusted UTC evidence, Claim deadline/charge and
the configured external prerequisite gate. It does not synthesize channel,
Ready Certificate, Publish descriptor, Broker append or external authority.
Commit `d413869b` moves the Claim handoff regression to a real V1 binding and
the derived Worker command overload, including queue deferral, permit
rejection and successful Claim coverage. This closes only the local one-shard
DUE/READY → Claim queue wiring and is not multi-shard, automatic Ready/Publish
preparation, response-loss/crash or release evidence.

## 2026-08-15 Claim-result-bound Publish handoff audit

Delay commit `e5828f40` adds a typed Claim-result → Publish handoff. The
runtime accepts only a successful Claim result and carries its exact
reservation into Publish Admission; a separate Claim or reservation cannot be
silently substituted. The external Channel Resource Identity, Ready
Certificate, trusted decision interval, retry limit and signing key remain
caller-supplied authority inputs. Focused Publish and Claim regressions pass.

This is local queue composition only. It does not prepare channel/credential
authority, execute Broker append/ACK, classify response loss, or prove
multi-shard/failover/crash/release gates.

## 2026-08-15 bounded due-to-Claim Worker turn audit

Delay commit `ce4bc2d5` composes one due-discovery work-class action with the
strict one-head READY → derived Claim handoff in `WorkerShardRuntime`. The
returned Claim action is still queued, not executed; the caller must run the
shared command turn. This keeps the separate Claim prerequisite/permit and
source-ordered local mutation boundaries intact while removing an orchestration
caller from the normal DUE/READY path. The focused Claim regression covers
queue, prerequisite and permit deferrals plus the successful Claim result.

The slice is local one-shard DUE/READY → Claim composition. Automatic channel
and Ready-Certificate preparation, Publish Admission, Broker append/ACK,
multi-shard assignment, response-loss/crash evidence and release PASS remain
open.

## 2026-08-15 checkpoint preflight and bounded multi-shard dispatch audit

Delay commit `ad5020f0` fixes the checkpoint claim lifecycle at the
pre-queue boundary. If the exact Owner/intent prerequisite or Store/intent
identity check fails before a `CHECKPOINT` action is registered, the executor
completes that same scheduler claim and preserves the primary rejection. A
queue-capacity rejection still leaves the claim current for exact retry. The
focused regression proves no checkpoint directory or provider call is created
by a preflight failure and the next schedule is claimable.

Delay commit `d0fe7158` adds `WorkerShardFleetRuntime`, a process-local
round-robin dispatcher for already accepted shard runtimes. Admission binds
unique Shard identities to one WorkClass registry and one shared RocksDB
resource envelope; source, scheduling and Claim/Publish command turns are
bounded per selected shard, while an absent optional graph yields no invented
action. Closing the fleet still requires each shard's own drain/lease/source
ordering.

## 2026-08-15 recurring checkpoint Worker wiring audit

Delay commit `46ca2b1e` binds a recurring checkpoint graph to one
`WorkerShardRuntime` only after exact WorkClass-registry, Shard-Store and
shared-resource identity checks. Register, due-claim, submit and run operations
reuse the shard's source lifecycle and runtime-business-admission fences. The
fleet adds a bounded checkpoint turn and round-robin cursor; it does not create
a checkpoint graph for a source-only shard.

The focused fleet/checkpoint tests and main checkstyle passed. This remains
local recurring-checkpoint dispatch: remote Object Store/provider/catalog
authority, checkpoint-on-drain completeness, catalog assignment, native source
ownership transfer, broker failover, crash evidence and release PASS remain
unproven.

## 2026-08-15 checkpoint claim-to-Store shard fence audit

Delay commit `c8d85e66` binds Worker-facing recurring checkpoint registration
and due-claim operations to the exact `ShardId` of the attached Store. Shared
process schedules now offer a targeted claim operation, so a selected shard
cannot hand another Store's checkpoint handle to its own execution coordinator.
The focused scheduler/runtime regressions and main Checkstyle passed.

This closes only a local cross-shard identity hazard. Durable schedule
authority, remote publication, assignment/failover, crash-boundary evidence and
release PASS remain open.

## 2026-08-15 recurring checkpoint drain fence audit

Delay commit `6f1f6d25` adds a drain preflight for the attached recurring
checkpoint runtime. An in-flight exact schedule claim rejects drain before the
Owner/Store/lease sequence starts; an idle registered schedule is unregistered
before that sequence, so a successfully drained shard does not retain a stale
local registration. The fleet/checkpoint focused tests and main Checkstyle
passed.

This is a local lifecycle fence only. Remote provider quiescence, durable
checkpoint publication, assignment/failover, crash-boundary evidence and
release PASS remain unproven.

## 2026-08-15 recurring checkpoint claim-to-submit wiring audit

Delay commit `5dacd6f3` adds a one-head Worker checkpoint entrypoint that keeps
the exact `ScheduledCheckpoint` capability through request construction and
into `CheckpointWorkClassExecutor`. A factory failure before queue admission
now releases that exact handle through the checkpoint execution boundary;
foreign value-equal handles are rejected, while queue rejection still leaves
the original handle current for exact retry. The corresponding Worker shard
wrapper keeps the source-running and runtime-resource admission fences.

The focused checkpoint/fleet tests and main Checkstyle passed. This advances
only local recurring-checkpoint retryability and request wiring. Durable
schedule authority, remote publication, automatic Ready/Publish preparation,
assignment/failover, crash-boundary evidence and release PASS remain open.

The focused checkpoint and fleet tests plus main checkstyle passed. This
advances only local checkpoint retryability and event-loop multi-shard
composition. It is not catalog-driven placement or assignment publication,
automatic Ready/Publish preparation, remote Object Store durability, native
source ownership transfer, broker failover, crash-boundary evidence or a
release PASS.

## 2026-08-15 Kafka System Mutation Worker apply audit

Delay commit `eee022bd` closes the previously open Kafka mutation-to-Store
Worker cut. The real Kafka smoke appends two signed `TIME_FENCE` mutations,
replays offset 0 through strict Owner recovery, activates at the offset-1
barrier, and applies offset 1 through the active mixed source loop. It checks
the mutation id/hash/type, `SystemMutationResult=APPLIED/OK`, exact Kafka
Source Position, RocksDB WriteBatch completion, `commitSync` ACK and final
checkpoint before reporting success. The source set is locked to Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`; the fresh three-broker receipt
used Compose project `nereus-delay-kafka-e2e-1786779783-8472`, ports
`19700,19701,19702`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
broker image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`.

The focused real-client task and the surrounding Kafka source/Worker/K1/K2
and broker-failover harness all passed. This is a bounded Kafka integration
receipt using local/in-memory owner authority, not evidence of production
assignment/session CAS, response-loss or crash recovery, Pulsar mutation
apply, automatic Claim/Publish authority, multi-shard placement or the V1
release gates.

## 2026-08-15 Pulsar System Mutation Worker apply audit

Delay commit `016288b1` closes the previously open Pulsar mutation-to-Store
Worker cut. The real P1 smoke appends two signed `TIME_FENCE` mutations,
replays ledger/entry `18/0` through strict Owner recovery, activates at the
exclusive `18/1` barrier, and applies `18/1` through the active mixed source
loop. It checks the mutation id/hash/type, `SystemMutationResult=APPLIED/OK`,
exact Pulsar Source Position, RocksDB WriteBatch completion, guarded
SUBSCRIBE ACK and final checkpoint before reporting success. The append,
recovery and active paths retain the same cluster/incarnation/topic
attestation and connection-generation proof.

The source lock is P1
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`; the receipt used client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5a1d137d`, and P1
image `sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`.
The full Docker receipt used Compose project
`nereus-delay-pulsar-e2e-1786780346-14394` on ports `19930,19931`; a second
full receipt on `19940,19941` reproduced the mutation Worker ledger/entry
pair. Both used local/in-memory owner authority.

This closes only the bounded Pulsar mutation append → recovery → active Store
apply → ACK → local checkpoint integration cut. It is not evidence of real
Oxia assignment/session CAS, multi-broker failover, native source ownership
transfer, response-loss or crash recovery at every WriteBatch boundary,
automatic Claim/Publish authority, catalog placement or §23.5 release
completion.

## 2026-08-15 real Oxia mutation Worker authority audit

The Kafka and Pulsar mutation Worker receipts were rerun with network Oxia
enabled. Kafka used Compose projects
`nereus-delay-kafka-e2e-1786781272-24697` /
`nereus-delay-kafka-oxia-e2e-1786781272-24697`, broker ports
`19710,19711,19712`, Oxia port `16671`, Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, and Oxia source
`37a17bef17202d5fd6e23282da5fd26d94865484`. It reported guarded mutation
recovery/apply/ACK plus `authority=real Oxia session-bound` and
`real Oxia session-bound lease`. Pulsar used projects
`nereus-delay-pulsar-e2e-1786781139-23243` /
`nereus-delay-pulsar-oxia-e2e-1786781139-23243`, broker/web ports
`19950,19951`, Oxia port `16670`, P1 source
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, and the same Oxia source; it
reported the equivalent guarded mutation recovery/apply/ACK and real Oxia
session-bound lease receipts.

This is stronger authority evidence for one bounded shard and one Worker
session. It does not establish catalog-driven multi-shard placement, signed
Route activation/source ownership transfer, Pulsar multi-broker failover,
response-loss or crash recovery at every WriteBatch boundary, automatic
Claim/Publish authority or §23.5 release completion.

## 2026-08-15 Kafka guarded Fetch to signed Route Worker assignment audit

Delay commit `7e0abb87fff8db1c1d2d2f73ffdd44a0c6097112` adds a real-client
Route activation cut rather than
only reusing the deterministic Route/assignment tests. With K1 source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, the smoke requires Fetch v13
evidence and uses its exact `lastStableOffset`/record range to build the Kafka
activation barrier. A real Oxia session publishes the signed Route event/head;
the provider refreshes from that authority; `RouteWorkerAssignmentCoordinator`
then publishes and rereads a route-bound Worker assignment by revision CAS.
The pre-Route record is recovered into the real Worker Store and ACKed only
after its RocksDB apply. The second Kafka record is observed only after the
signed exclusive barrier, applied through the accepted Worker assignment and
ACKed by `commitSync`; the Worker then publishes its final local checkpoint
and releases the Oxia owner lease and assignment.

The earlier assignment-only receipt is retained as historical provenance at
Delay `1550347f`; it is superseded by the current Worker Store apply cut
below, not rewritten as current evidence.

The receipt used Delay `7e0abb87fff8db1c1d2d2f73ffdd44a0c6097112`, Kafka client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Kafka/Oxia projects `nereus-delay-kafka-e2e-1786785694-74566` /
`nereus-delay-kafka-oxia-e2e-1786785694-74566`, ports
`19730,19731,19732` / `16673`, and Oxia source
`37a17bef17202d5fd6e232da5fd26d94865484`. It printed:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=1, commitSync ACK, final checkpoint
```

This is source-locked, one-partition integration evidence for signed Route
publication, exact barrier projection, Oxia assignment CAS, pre-Route Store
recovery/apply, post-barrier Worker Store apply/source ACK, final local
checkpoint and owner/assignment release. It does not promote the result to
production Route activation:
session churn/reconnect, accepted-Route broker failover, catalog placement,
native eligibility, source ownership transfer, Object Store checkpointing,
automatic Claim/Publish authority and §23.5 release gates remain open.

## 2026-08-15 Kafka accepted Route broker failover audit

Delay commit `7e94d0f8a3e374832a111dbd2f741be5f20795d5` adds a separately gated
real-client cut after the signed Route and route-bound Oxia assignment have
been accepted. The harness stops `kafka-1` while the Worker is held at the
accepted assignment, waits for a surviving broker, releases the gate, and
requires the same Worker Store to apply and `commitSync` ACK the next Kafka
record at `barrierOffset + 1`. It then restarts `kafka-1`, drains, publishes
the bounded local checkpoint and proves assignment/owner release.

The source lock is Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
Oxia `37a17bef17202d5fd6e232da5fd26d94865484`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Kafka/Oxia projects `nereus-delay-kafka-e2e-1786787846-2966` /
`nereus-delay-kafka-oxia-e2e-1786787846-2966`, broker ports
`19750,19751,19752`, and Oxia port `16677`. The Delay harness was run with
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1` and
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY=1`; it printed:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=2, commitSync ACK, accepted-route broker failover, final checkpoint
Kafka accepted-route broker failover E2E passed: Route-bound Worker applied and ACKed after broker-1 failover, then released its final checkpoint and Oxia assignment.
```

This is source-locked evidence for one accepted Route and one Kafka
topic/partition. The `ROUTE_FAILOVER_ONLY` mode is deliberately not promoted
to the aggregate E2E or release gate. Catalog placement, Route session
reconnect/churn, native eligibility, production source ownership transfer,
remote Object Store authority, response-loss/crash cuts, Pulsar multi-broker
failover, automatic Claim/Publish authority and §23.5 release gates remain
open.

## 2026-08-15 Kafka default full real-client revalidation audit

The default Kafka harness was rerun after the Route Worker script change with
the accepted-Route failover flags at their default `0` values. The successful
receipt used Delay branch HEAD
`52da04a3b14c56fcbe769f64836e1311e11956a7` (runtime slice
`7e94d0f8a3e374832a111dbd2f741be5f20795d5`), Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
Oxia `37a17bef17202d5fd6e232da5fd26d94865484`, Kafka/Oxia projects
`nereus-delay-kafka-e2e-1786788428-10652` /
`nereus-delay-kafka-oxia-e2e-1786788428-10652`, broker ports
`19763,19764,19765`, and Oxia port `16679`. It printed:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=1, commitSync ACK, final checkpoint
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

This is a current default-path revalidation, not a new production authority
claim. The bounded accepted-Route broker failover remains the separately gated
one-topic/one-partition evidence above; catalog placement, Route session
reconnect/churn, native eligibility, production source ownership, remote Object
Store authority, response-loss/crash boundaries, Pulsar multi-broker failover,
automatic Claim/Publish authority and §23.5 release gates remain open.

## 2026-08-15 Pulsar guarded SUBSCRIBE to signed Route Worker assignment audit

Delay commit `bf858b089b927fcf65129214d8ed5a7fc5300deb` adds a real-client
Route activation cut for the locked P1 source. P1 commit
`0a2536484cd3932801a98dc88ff112b2df88a1c7` supplies the dedicated
admin/ownership-checked Resource Controller endpoint required to stamp the
guard tuple on a native partitioned physical topic; the generic topic
properties API remains fail-closed. The smoke creates one native partition,
captures the guarded SUBSCRIBE position and stable attestation, derives
`ActivationBarrierV1.pulsar`, publishes the signed Route event/head through a
session-fenced real Oxia client, publishes and rereads a revision-CAS
route-bound Worker assignment, recovers the pre-Route record into the real
Worker Store before ACK, and hands the same guarded connection to the Worker.
The Worker applies/ACKs the post-barrier record, creates the bounded local
RocksDB checkpoint, and releases the owner lease and assignment.

The preceding bounded Worker evidence remains historically pinned to Delay
`nereus/delay-full-implementation-v1@2dd2cfff83f4d029972cf7fbeb569fbf4538c026`;
that frozen receipt is not the current implementation lock below.

The preceding assignment-only Route receipt remains historical provenance at
Delay `nereus/delay-full-implementation-v1@a73faf3e836ada67931f709d46214dde7caf3ad0`;
it is superseded by the current Worker Store apply/checkpoint cut below.

The receipt is source-locked to P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7`
from `8dae0236c0a0d405ed7f8303081080520fe91551`, Delay
`bf858b089b927fcf65129214d8ed5a7fc5300deb`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`. It records P1 client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
distribution `373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`,
base image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d5`,
Compose project `nereus-delay-pulsar-e2e-1786786327-81328`, broker/web ports
`20020,20021`, and Oxia port `16674`. The exact Route receipt was:

```text
Pulsar signed Route -> guarded SUBSCRIBE barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: generation=15, barrier=20/0, routeRevision=1, assignmentRevision=1, source=20/1, ACK, final checkpoint
```

The same run ended with the aggregate receipt `Pulsar P1 real-client E2E
passed: guarded send, stale resource rejection, guarded source replay, signed
mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker
timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.`

This is source-locked, one-native-partition integration evidence for the
guarded source position, signed Pulsar barrier, Oxia assignment CAS, Worker
Store recovery/apply, post-barrier source ACK, bounded local checkpoint and
owner/assignment release. It does not promote the result to production Route
activation: session reconnect/churn, multi-broker failover with an accepted
Route, catalog placement, native eligibility, source ownership transfer,
Object Store checkpointing, automatic Claim/Publish authority and §23.5
release gates remain open.

## 2026-08-15 Dockerized Oxia authority and checkpoint publication audit

The latest isolated Oxia authority run used Delay
`nereus/delay-full-implementation-v1@ac72e43803806b9c309b62150c0aa54b43f8a3ea`,
Oxia `37a17bef17202d5fd6e232da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786787138-90186`, and host port `16675`. The
selected real-service tests passed with `BUILD SUCCESSFUL in 11s`, including
the real Oxia Owner Lease/Control/Recovery Catalog, atomic checkpoint
intent/catalog publication, Route publication/refresh/assignment, Gateway
audit and Gateway tenant-admission CAS methods. The harness printed:

```text
Dockerized Oxia real-service smoke passed for 37a17bef17202d5fd6e232da5fd26d94865484
```

This is real Oxia authority evidence. The provider-side checkpoint adapter in
this smoke is the crash-durable filesystem object seam, so remote Object Store
credentials/quiescence, session-bound RecoveryPin transaction, multi-shard
placement and the release cross-entry gate remain open.

## 2026-08-15 Oxia Route provider restart/revalidation audit

Delay commit `164597c39f1da6fc403c5283494b1f0c6b132802` adds a gated real-service
restart cut for `OxiaSignedRouteSnapshotProvider`. After a signed Route is
published and cached, the isolated harness stops and starts the same Oxia
container, waits for health, releases the test gate, and requires explicit
provider refresh to rebuild the persisted head/event cache with the exact
revision and signed snapshot intact.

The receipt is locked to Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, image
`sha256:1ea8324636e65d92bf6f0767062e58078fd617767c9c74540443c5b6a2c1293d`,
Compose project `nereus-delay-v1-oxia-e2e-1786789198-22565`, and port `16684`.
The selected test passed with `BUILD SUCCESSFUL in 10s` and recorded:

```text
Oxia Route provider restart recovery passed: revision=1, session revalidated, cache healthy
Dockerized Oxia Route restart smoke passed: provider session recovery and signed Route cache rebuild
```

This is one real Oxia service restart/revalidation cut. The observed reusable
session identity did not rotate across this stop/start, so ephemeral marker
expiry/rotation, notification-stream churn, multi-node failover, catalog
placement, remote Object Store authority and §23.5 release gates remain open.

## 2026-08-15 Kafka K2 broker failover commit-boundary audit

Delay commit `6912b940` adds an opt-in file gate immediately before the guarded
K2 transaction's `EndTxn`/`commitTransaction` call. The dedicated
`K2_FAILOVER_ONLY` mode starts the locked three-broker Kafka image, pauses the
real client at that gate, stops `kafka-1`, releases the gate, and requires the
transaction result plus exact `read_committed` target/receipt records before
the smoke exits. The normal K2 path remains the receipt for abort and
delete/recreate fencing.

The source lock was Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from `c300006a7705c240642db6950b5a95fec982bfc5`, with client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3` and
broker image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`.
The successful project was `nereus-delay-kafka-e2e-1786790805-40581` on
`19795,19796,19797`. The client trace recorded coordinator
`127.0.0.1:19795 (id: 1)` before the stop and rediscovered
`127.0.0.1:19797 (id: 3)` after broker-1 was stopped. It printed:

```text
K2 broker failover commit returned PUBLISHED: read_committed target+receipt pair
K2 broker failover smoke passed: target-plus-receipt transaction crossed broker-1 failover and exact read_committed records were verified
```

This is a bounded real broker failover receipt for one target-plus-receipt
transaction and coordinator rediscovery. Because the observed commit returned
`PUBLISHED`, it does not close lost `EndTxn` response resolution;
LSO/retention-floor recovery, crash cuts and release gates remain open.

## 2026-08-15 strict typed Lane activation and complete Worker graph audit

Delay commit `defce755` adds the next D6 local composition slice. A canonical
V1 Schedule/Prepare resolver tuple now creates a typed `ActiveLaneStateV1`
projection with exact Profile refs, tuple identity and typed readiness. Legacy
or malformed resolver bytes remain compatibility state; no Profile identity is
inferred from arbitrary bytes. `ReadyCertificateV1` exposes the certificate's
activation barrier and evidence cursors for exact proof binding.

`LaneActivationCoordinator` requires the owned shard to be in
`CATCHING_UP`, captures Lane/Owner/Store identity, and asks an injected
prerequisite authority for the Channel Resource, Ready Certificate, evidence
cursors and trusted interval. `OwnedDelayShard` rereads the context-bound
Oxia catch-up lease before `DelayShard.activateLaneReadiness` atomically
installs the certificate-backed READY projection and READY index. The path
rejects foreign Owner, Store, Lane, tuple, barrier or evidence identity and
allows only an exact-certificate retry after READY. The old readiness setter
cannot activate a typed Lane without a certificate.

Kafka and Pulsar source factories now have an overload that accepts the same
registry-bound scheduling, command and checkpoint runtimes, so the guarded
source can be constructed as part of the complete local Worker graph while
retaining the existing `WorkerShardRuntime` identity/resource fences.

The source tree was validated with:

```text
GRADLE_USER_HOME=/tmp/nereus-delay-lane-activation-full-gradle ./gradlew check --no-daemon --console=plain
BUILD SUCCESSFUL in 1m 14s; 21 actionable tasks
GRADLE_USER_HOME=/tmp/nereus-delay-lane-activation-full-gradle ./gradlew checkstyleMain --rerun-tasks --no-daemon --console=plain
BUILD SUCCESSFUL in 22s
GRADLE_USER_HOME=/tmp/nereus-delay-real-kafka-worker-factory-gradle ./gradlew compileRealKafka -PkafkaClientJar=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1/clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar --no-daemon --console=plain
BUILD SUCCESSFUL in 4s
GRADLE_USER_HOME=/tmp/nereus-delay-real-pulsar-worker-factory-gradle ./gradlew compileRealPulsar -PpulsarClientClasspath=/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1/pulsar-client/build/libs/pulsar-client-original-5.0.0-M1.jar:/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1/pulsar-client-api/build/libs/pulsar-client-api-5.0.0-M1.jar:/Users/liusinan/apps/ideaproject/nereusstream/pulsar-worktrees/nereus-delay-p1/pulsar-common/build/libs/pulsar-common-5.0.0-M1.jar --no-daemon --console=plain
BUILD SUCCESSFUL in 4s
```

The full check's real Oxia smoke tests were skipped by their normal opt-in
gates. This slice is local typed activation and Worker graph evidence only;
there is still no live Profile/credential/Broker prerequisite authority, real
due-to-Claim-to-Publish E2E, multi-shard or transport production wiring,
crash/response-loss proof, or release PASS.

## 2026-08-15 typed Lane scheduling bootstrap audit

Delay commit `7a48f85b` adds `WorkerSchedulingRuntime.openForActiveOwnerFromTypedLanes`.
The bootstrap rereads each requested Lane from the same Store, requires an
`ActiveLaneStateV1` with `OPEN` admission, `READY` runtime state and a
certificate, and checks that the derived `LaneRecord` has the same incarnation
and version. Missing/legacy state, duplicate identities and projection drift
are rejected before scheduler construction. The existing strict active
Owner/Shard/Store checks and authoritative READY-index rebuild then remain in
force.

The due-worker regression now exercises this typed bootstrap. It verifies that
a persisted READY head is restored before the due turn, that no duplicate new
head is reported, and that the exact restored item remains available to strict
READY polling. `check` passed with the typed Lane activation and due/Claim
regressions; the full check's real Oxia smoke tests were skipped by their
normal opt-in gates.

This closes only the local typed activation → scheduler bootstrap boundary.
It does not establish live Profile/credential/Broker prerequisite authority,
automatic due-to-Claim-to-Publish execution, multi-shard or transport E2E,
crash/response-loss evidence, or release PASS.

## 2026-08-15 bounded due-to-Claim-to-Publish Worker composition audit

Delay commit `5305748b02965f171ac751615bb00b4dda8a9eb0` extends the local
one-shard Worker entrypoint to observe the exact Claim task through bounded
fair shared-command turns and, when an injected typed
`PublishPreparationProvider` supplies preparation, to submit and observe the
exact Publish task as well. The Claim result and active reservation remain
bound; an empty preparation result preserves the reservation for retry or
revoke, a provider exception fences the Owner, and Publish `UNKNOWN` leaves
the reservation active until source-ordered resolution or explicit release.

The focused Claim-only and Claim-plus-Publish regressions passed, followed by
the full `check` (`BUILD SUCCESSFUL in 1m 13s`, 21 actionable tasks). This is
bounded local orchestration evidence only. The provider is still caller
authority: no live Profile/credential/Broker prerequisite, physical append or
ACK, automatic preparation, response-loss/crash resolution, multi-shard
placement, remote Object Store authority or release gate is proven.

## 2026-08-15 current-source Kafka and Pulsar revalidation audit

The current Delay commit `efa422a9ec16cb370376e0c5a72b18bbbdb3a906` passed
both locked real-client transport harnesses. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786795881-97477`, ports
`19835,19836,19837`, and Oxia `37a17bef17202d5fd6e232da5fd26d94865484` in
`nereus-delay-kafka-oxia-e2e-1786795881-97477` on `16686`. Its final receipt
was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786796050-99359`, broker/web
`20135,20136`, and Oxia in `nereus-delay-pulsar-oxia-e2e-1786796050-99359` on
`16687`. Its final receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

This revalidates the current transport and Worker source locks after the typed
Lane/scheduler changes. It is not evidence that the typed activation boundary
has live Profile/credential/Broker authority, nor does it close automatic
due-to-Claim-to-Publish execution, multi-shard placement, Pulsar multi-broker
failover, remote Object Store checkpointing, crash/response-loss resolution or
the §23.5 release gates. Temporary Docker projects and images were cleaned up
by the harness traps.

## 2026-08-15 current-source Kafka and Pulsar revalidation after bounded Worker composition

The fresh receipts use Delay source lock
`5305748b02965f171ac751615bb00b4dda8a9eb0`, after the bounded
due-to-Claim-to-Publish composition was implemented. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from base `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786797371-14292`, ports
`19845,19846,19847`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484` in project
`nereus-delay-kafka-oxia-e2e-1786797371-14292` on `16696`. The receipt was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from base
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786797371-14293`, broker/web
`20145,20146`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484` in project
`nereus-delay-pulsar-oxia-e2e-1786797371-14293` on `16697`, with image
`sha256:b8e9f6e6497308be5e1c1cb937a6af96be10d8b258cb660696f605cdf0b495e3`.
The receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

These are current-source transport/Worker receipts only. The harnesses do not
invoke `runDueClaimPublishTurn`, so they do not prove provider-driven live
preparation or physical Publish append/ACK. Kafka covers the checked-in
three-broker K1/K2 cuts; Pulsar covers the checked-in standalone-broker
restart path, not multi-broker failover. Live Profile/credential/Broker
authority, multi-shard placement, remote Object Store checkpointing,
crash/response-loss resolution and §23.5 release gates remain open. Temporary
Compose resources were removed; the Kafka Oxia image was removed by cleanup.

## 2026-08-15 typed Worker publish-preparation coordinator audit

Delay commit `2a8c198328e5a8879db9c23faf6e805b6d7ea819` adds
`WorkerPublishPreparationCoordinator` to bind the external Publish preparation
callback to the exact typed READY Lane that produced a successful Claim. The
coordinator authoritatively rereads Owner/Claim admission, then requires the
persisted Lane to be `OPEN`/`READY` with a decodable `ReadyCertificateV1`.
Owner, Store, Lane incarnation, materialization target/partition, activation
barrier and both typed Profile refs are compared before the callback can run.
The callback's non-empty result must retain the exact persisted channel and
certificate; an empty result retains the Claim reservation for retry.

The external authority remains responsible for live channel/credential
resolution, signing keys and trusted publish timing. The focused regression
also rejects a foreign Store Certificate. The focused Claim test and full
`check` passed (`BUILD SUCCESSFUL in 1m 13s`, 21 actionable tasks; real Oxia
smokes skipped by their opt-in gates).

This is a local Claim-to-preparation identity audit, not proof of live
prerequisite authority, automatic due-to-Claim-to-Publish execution, physical
append/ACK, response-loss/crash recovery, multi-shard placement, remote Object
Store authority or §23.5 release readiness.

## 2026-08-15 current-source Kafka and Pulsar revalidation after typed preparation coordinator

The current Delay source lock is
`2a8c198328e5a8879db9c23faf6e805b6d7ea819`. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786798539-27043`, ports
`19855,19856,19857`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-kafka-oxia-e2e-1786798539-27043` on `16698`. The receipt was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786798539-27042`, broker/web
`20155,20156`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-pulsar-oxia-e2e-1786798539-27042` on `16699`, with image
`sha256:58c9302be462dc5f16ba593c289b17373a14d85cead7b0526b0bc02cfa2ee575`.
The receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

The receipts are transport/Worker evidence after the new local coordinator,
not coordinator coverage: neither checked-in harness constructs it or invokes
provider-driven `runDueClaimPublishTurn`. Kafka covers three-broker K1/K2
failover; Pulsar covers a standalone-broker restart, not multi-broker failover.
Live prerequisite authority, multi-shard placement, remote Object Store
checkpointing, crash/response-loss resolution and §23.5 release gates remain
open. Temporary Compose resources were removed; Kafka's temporary Oxia image
was removed by cleanup.

## 2026-08-15 typed Worker preparation provider binding audit

Delay commit `d02d81201d0cff3f9fa5fb3c8bba912721de5575` carries the
`PublishPreparationProvider` from complete Worker graph construction into
`WorkerShardRuntime`. The no-provider `runDueClaimPublishTurn(...)` entrypoint
uses the bound provider and fails closed when the graph has none; the narrow
explicit-provider seam remains for composition tests. Kafka and Pulsar complete
`WorkerSourceFactory.create` overloads pass the provider through, while the
legacy complete-graph overloads intentionally remain unbound compatibility
paths. The focused test exercises both the bound coordinator path and the
unbound failure.

This is an implementation-boundary audit, not a live authority audit. The
provider is still supplied by the caller and the slice does not create Profile,
credential, Broker, channel, signing-key, physical append/ACK or checkpoint
authority. The focused test, exact real Kafka/Pulsar compile gates and full
`check` passed; real Oxia smokes remain opt-in.

## 2026-08-15 current-source Kafka and Pulsar revalidation after typed Worker provider binding

The current Delay source lock is
`d02d81201d0cff3f9fa5fb3c8bba912721de5575`. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from base `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786799494-38038`, ports
`19865,19866,19867`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-kafka-oxia-e2e-1786799494-38038` on `16700`. The receipt was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from base
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
runtime library count `341`, Compose project
`nereus-delay-pulsar-e2e-1786799494-38039`, broker/web ports `20165,20166`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-pulsar-oxia-e2e-1786799494-38039` on `16701`. The receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

Neither harness constructs the bound provider or invokes
`runDueClaimPublishTurn`; these are transport/Worker revalidations after
`d02d8120`, not live preparation or physical Publish append/ACK evidence.
Kafka covers three-broker K1/K2 failover; Pulsar covers a standalone-broker
restart, not multi-broker failover. Live prerequisite authority, multi-shard
placement, remote Object Store checkpointing, crash/response-loss resolution
and §23.5 release gates remain open. Temporary Compose resources were removed;
the Kafka Oxia image was removed by cleanup and the Pulsar Oxia image remained
locally with digest `sha256:71d69981a5b9dd458158a8c440fb6d90642450d96b0e92e59f7c49745bbc498c`.

## 2026-08-15 fleet-level due-to-Claim-to-Publish dispatch audit

Delay commit `d5672a46c9558aa4417f744f30cccd79518adde0` adds the multi-shard
`WorkerShardFleetRuntime.runNextDueClaimPublishTurn(...)` boundary. It selects
only a runtime with both scheduling and command graphs, preserves fair
round-robin selection, and invokes the runtime's already-bound preparation
provider without exposing a replacement callback. Source-only or partially
composed runtimes therefore return no combined turn; a selected runtime without
the provider remains fail-closed.

The Claim handoff test now proves the successful bounded turn through this fleet
boundary, and the full `check` passed. This audit closes only the common fleet
dispatch composition. It does not prove catalog-driven placement, live
Profile/credential/Broker authority, physical Publish append/ACK, checkpoint
publication, crash/response-loss resolution, real fleet E2E or §23.5 release
readiness. No real-client harness was rerun for this common-only change; its
latest source-qualified receipts remain bound to `d02d8120`.

## 2026-08-15 bounded PUBLISHING physical adapter and Outcome bridge audit

Delay commit `9a9c14827d01f94b36820e2e4381373725cec7fa` adds
`WorkerPhysicalPublishExecutor` and the bounded adapter preflight hook. A
`PUBLISHING` ledger is decoded and identity-checked against its canonical
`PUBLISH_ADMISSION`, Claim, READY Lane, message, attempt and payload
projection before a `DestinationPublishRequest` is admitted. The injected
`PhysicalPublishGate` is evaluated both before physical admission and in the
last pre-delegate slot; deferred or definitive gate outcomes do not invoke the
target. A physical result, including `UNKNOWN`, is handed to the external
signed `PublishOutcomeMutationFactory` through the bounded outcome executor.

`WorkerPhysicalPublishExecutorTest` covers allowed handoff, deferred admission,
the late-gate race and opaque-admission rejection; the focused Worker tests and
full `check` passed. This audit closes only the common local bridge. It does
not prove live prerequisite/payload authority, signed typed outcome/evidence
construction, physical Kafka/Pulsar Broker append/ACK, source-factory binding,
adapter evidence journals/classifiers, crash/response-loss resolution,
multi-shard placement, checkpoint/quiescence or §23.5 release readiness. The
real-client harnesses were not changed or rerun; their source-qualified
receipts remain bound to `d02d8120`.

## 2026-08-15 typed signed PUBLISH_OUTCOME factory audit

`WorkerPublishOutcomeMutationFactory` now supplies the common typed signing
path behind the physical bridge. It requires exact PUBLISHING ledger/request
identity, maps physical disposition to the closed Outcome side-effect branch,
validates attempt-owned Publish Evidence, rejects typed UNKNOWN evidence that
would be dropped, and emits a canonical Ed25519-signed `PUBLISH_OUTCOME`
mutation. `WorkerPublishOutcomeMutationFactoryTest` proves the published
canonical body/signature and fail-closed mismatched context cases.

The factory consumes an external retry/charge/time `OutcomeContextProvider`; it
does not create live authority, Broker evidence or source ordering. Physical
Kafka/Pulsar append/ACK, source-factory wiring, adapter journals,
response-loss/crash resolution, real-client E2E and §23.5 release readiness
remain open.

## 2026-08-15 source-bound physical adapter invocation audit

Delay commit `5222c9bbb9f64e6a0fe58009ccf143ca8ec59636` carries the durable
attempt's canonical Source Position and prepared Publish hash through the
bounded physical adapter. The late gate and zombie-release behavior remain
shared with ordinary invocation, while `KafkaTransactionalDestinationAdapter`
can now be reached through its source-aware target-plus-receipt overload.

The focused Worker/Kafka tests and full `check` passed. This closes only the
common source-bound composition. It does not prove source-ordered lookup and
application of a live PUBLISHING ledger, Source Assignment/ACK authority,
physical K2 Broker append, response-loss/LSO resolution, real Worker E2E or
§23.5 release readiness.

## 2026-08-15 Pulsar source-bound physical adapter invocation audit

Delay commit `1d969cb8fa15430faaf8b38ae1e34390ce5e7769` extends the common
source-bound physical call to `PinnedPulsarDestinationAdapter`. The adapter
requires a Pulsar Source Position for the request's Shard and forwards the
canonical Source Position and prepared hash to a source-aware transport,
while legacy request-only transports remain compatible.

The focused Pulsar adapter/Worker tests and full `check` passed. This is still
local transport composition only: source-applied PUBLISHING lookup, live
Pulsar assignment/ACK authority, Broker append, reconnect/rewind,
response-loss/crash resolution, real Worker E2E and §23.5 release readiness
remain open.

## 2026-08-15 persisted PUBLISHING attempt lookup audit

Delay commit `e9cfde1415e2c389c8587b1d72ed7f42afa47b79` adds the persisted
attempt-ID entrypoint on `WorkerShardRuntime`. It reloads from the owned
shard's bounded inflight scan, rejects missing or non-PUBLISHING state before
any physical adapter or Outcome path, and then delegates to the existing
canonical request/source-bound checks.

`WorkerShardRuntimePhysicalLookupTest` proves the missing-attempt fail-closed
boundary and the full `check` passed. This remains local lookup evidence only;
source application, live Owner/Assignment authority, Object Store payload
resolution, Broker append/ACK, response-loss/crash recovery, real Worker E2E
and §23.5 release readiness remain open.

## 2026-08-15 source-applied PUBLISHING ledger physical dispatch audit

Delay commit `ada1d2aa80bbdaf73293e46203fcb7dfd4f0a93d` adds a bounded
source-bound Worker dispatch. It uses the exact Admission Source Position as
the replay target, waits through the common source apply/ACK coordinator, then
reloads the attempt by logical ID from the owned shard before allowing the
physical adapter. A persisted Source Position mismatch, missing ledger after
the matching source apply, non-`PUBLISHING` state or unavailable payload is
returned as a closed boundary; none of those branches calls the destination.

The runtime also exposes a bound due/Claim/Publish → source apply → physical
composition, a fair fleet dispatch, and optional physical-executor binding in
the Kafka/Pulsar source factories. The payload provider remains external and
the physical executor still owns late gate, bounded adapter and signed Outcome
handoff semantics.

`WorkerShardRuntimePhysicalLookupTest` covers the bounded source-wait path;
the focused test, full `check`, `compileRealKafka` and `compileRealPulsar`
checks passed. This audit closes only local source-position/ledger
orchestration. It does not prove live Object Store or channel authority,
destination Broker append/ACK, source-ordered `PUBLISH_OUTCOME` application,
response-loss/LSO/crash recovery, multi-broker or real Worker E2E evidence, or
§23.5 release readiness.

## 2026-08-15 typed Kafka K2 read-committed receipt evidence audit

Delay commit `3c7128eb6caecc50f3d6f4865ed2cdfa2838ad8a` closes the positive
typed K2 evidence boundary at the real Kafka adapter. The shared builder
rejects a foreign lane/incarnation, TopicId, partition, cursor kind,
zero-generation cursor, uncovered receipt offset/LSO or mismatched exact
receipt-record digest. The source-locked provider uses a fresh
`read_committed` guarded consumer, validates Fetch v13 evidence and the exact
producer receipt metadata, requires the returned LSO to cover the receipt, and
only then emits `KAFKA_TRANSACTIONAL_RECEIPT` with
`VERIFIED_PUBLISHED`. The transport decodes the returned evidence and checks
the kind, status and Publish Attempt owner before returning `PUBLISHED`;
response loss without that reread remains `UNKNOWN`.

The focused test, full `check` and exact Kafka client compile passed. The
dedicated failover-only Docker receipt is source-qualified to Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786806083-13395`, and ports
`19985,19986,19987`. Its exact lines were:

```text
K2 broker failover commit returned PUBLISHED: typed KAFKA_TRANSACTIONAL_RECEIPT evidence and read_committed target+receipt pair
K2 broker failover smoke passed: target-plus-receipt transaction crossed broker-1 failover and exact read_committed records were verified
Kafka K2 broker failover E2E passed: target-plus-receipt transaction crossed broker-1 failover with read_committed resolution.
```

This is direct adapter evidence, not a full V1 or Worker production PASS. At
the time of this 2026-08-15 audit snapshot, the real Worker harness still did
not source-apply a PUBLISHING ledger through the bound physical
executor/provider. Fetch response-loss, retention-floor
and crash resolution, live prerequisite/channel/Object Store authority,
source-ordered `PUBLISH_OUTCOME`, Pulsar multi-broker parity, placement,
checkpoint/quiescence and §23.5 release gates remain open.

## 2026-08-15 typed Pulsar SEND ACK evidence audit

Delay commit `4f2297e1dc593f8b5e16f7733e6ed1109544cb4a` adds
`PulsarSendAckEvidence` and the source-locked P1
`PulsarClientArtifactDestinationTransport`. The builder covers Registry
branch fields 1–11 and binds the target resource/partition, ledger/entry,
normalized batch index, broker time, producer-name hash, sequence, exact
Publish Attempt, prepared hash and authenticated response digest. The real
transport validates the P1 `GuardedMessageId`/`TopicResourceGuard` and
`MessageIdAdv` identity before returning a typed `PUBLISHED`; request-only
publication is conservatively unavailable. Invalid or incomplete physical
proof remains `UNKNOWN`.

The focused evidence test, full `check`, and exact P1 compile passed. The
source-qualified E2E used P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256 `373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`,
client SHAs `57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, image
`sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d5`,
Compose project `nereus-delay-pulsar-e2e-1786807647-30858`, and ports
`20305,20306`. Its exact typed receipt was:

```text
Pulsar destination typed-evidence smoke passed: topic=persistent://public/default/p1-destination-30858, ledger=11, entry=0, batchIndex=0, sequence=0, brokerPersistenceTime=1786807670952
```

The same run read the exact destination payload back through a guarded P1
consumer and passed the existing source, mutation, Worker and restart smokes.
This is a positive direct-adapter evidence cut, not a release PASS: at that
2026-08-15 direct-destination snapshot, the real Worker E2E did not invoke the
source-applied physical destination transport, and typed guard rejection,
response-loss/crash resolution,
Pulsar multi-broker failover, live prerequisite authority, placement,
checkpoint/quiescence and §23.5 gates remain open.

## 2026-08-16 source-applied Pulsar Worker physical Publish and typed Outcome audit

Delay commit `cb309d82` extends the real P1 Worker smoke through a bounded
source-ordered physical Publish path. The smoke obtains a real guarded source
position for a physical Schedule, applies that Schedule through the Worker
source loop, then appends a signed `PUBLISH_ADMISSION` with a bounded Claim,
READY certificate and typed Lane projection. The Worker source-bound physical
entrypoint reloads the resulting `PUBLISHING` ledger and invokes the guarded
P1 destination transport. Its typed `PULSAR_SEND_ACK` is signed into a
source-log `PUBLISH_OUTCOME`; replay applies the Outcome, closes the attempt
as `PUBLISHED`, and a guarded consumer verifies the exact destination payload.

Verification passed with `./gradlew check`, exact `compileRealPulsar`, and the
current-source E2E on P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, image
`sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786809927-56224`, and ports
`20515,20516`. The run recorded source/target/source positions `22/3 → 23/0
→ 22/4` and, after Broker restart, `33/2 → 34/0 → 33/3`, with exact payload
readback in both cases.

This is a positive source-applied Worker vertical only. The Claim and
readiness inputs remain bounded smoke authority; the live due/Claim/Object
Store provider graph, `runDueClaimPublishPhysicalTurn`, typed guard rejection,
crash/response-loss resolution, Pulsar multi-Broker failover, Oxia Route
authority and §23.5 release gates remain open. The receipt ran with
`NEREUS_DELAY_PULSAR_WITH_OXIA=0`.

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=22/3, typed PULSAR_SEND_ACK target ledger/entry=23/0, Outcome source ledger=22/4, exact payload readback
Pulsar Worker source-applied physical publish passed: Admission source ledger=33/2, typed PULSAR_SEND_ACK target ledger/entry=34/0, Outcome source ledger=33/3, exact payload readback
```

## 2026-08-16 source-applied Kafka Worker physical Publish and typed Outcome audit

Delay commit `112522e6` extends the current K1 real-client Worker smoke through
a bounded source-ordered physical Publish path. The smoke obtains a guarded
Kafka source position for a physical Schedule and verifies it with exact
Fetch-v13 readback, including the broker append time and any available leader
epoch. The Worker applies that Schedule, derives the typed Lane projection,
appends signed `PUBLISH_ADMISSION`, and calls
`WorkerShardRuntime.runSourceBoundPhysicalPublish(...)`. The runtime reloads
the persisted `PUBLISHING` ledger and invokes the guarded transactional
destination adapter with the exact source position and prepared hash. Typed
`KAFKA_TRANSACTIONAL_RECEIPT` evidence is read in `read_committed`, carried by
a signed source `PUBLISH_OUTCOME`, source-applied to `PUBLISHED`, and checked
with exact destination payload readback.

Verification passed with `./gradlew check`, exact `compileRealKafka` against
the locked client artifact, and the full three-broker K1/K2 E2E. Locks: K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786812109-79794`, and ports
`21092,21093,21094`.

```text
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=2, Outcome source offset=4, exact payload readback
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

This is positive source-applied Worker and three-broker failover evidence only.
The Claim, readiness, credential and payload inputs are bounded smoke
authority; the E2E does not invoke the live due/Claim/Object Store provider
graph or `runDueClaimPublishPhysicalTurn`, and Oxia Route authority was
disabled. Crash/response-loss coverage beyond the exercised K2 read-committed
receipt, multi-shard placement, checkpoint/quiescence and §23.5 release gates
remain open.

## 2026-08-16 provider-driven Kafka and Pulsar Worker physical Publish audit

The earlier source-applied entries are historical snapshots. Delay commits
`e5cae7b8e7d9988cc6dca516212d011d49fea5fa` (Kafka) and
`3c6e605a33cea2de85fce473af740b5e05fcf74e` (Pulsar) now bind each real Worker
smoke to the active-owner typed scheduling, Claim Handoff, Publish Admission
and preparation-provider graph. Each smoke invokes
`runDueClaimPublishPhysicalTurn(...)`, source-applies the provider-driven
Claim/Admission, then completes the source-bound physical Publish and typed
Outcome path.

Kafka verification passed with `./gradlew check`, exact locked-client
`compileRealKafka`, and the three-broker E2E using K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, image
`sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786814042-841`, ports `21492,21493,21494`:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar verification passed with `./gradlew check`, exact locked-client
`compileRealPulsar`, and the P1 E2E using source
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, image
`sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose `nereus-delay-pulsar-e2e-1786814719-7983`, ports `21515,21516`:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=22/3, typed PULSAR_SEND_ACK target ledger/entry=23/0, Outcome source ledger=22/4, exact payload readback
Pulsar Worker source-applied physical publish passed: Admission source ledger=33/2, typed PULSAR_SEND_ACK target ledger/entry=34/0, Outcome source ledger=33/3, exact payload readback
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, source-bound typed destination SEND ACK/payload readback, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, source-applied physical publish with typed Outcome and payload readback, ACK handoff, and broker-restart resume.
```

This is positive provider-driven Worker evidence. The provider graph still
uses bounded in-memory authority and deterministic smoke preparation; it does
not establish live Profile/credential/Object Store/catalog authority, crash or
response-loss resolution, Pulsar multi-Broker failover, placement,
checkpoint/quiescence or §23.5 release readiness. Pulsar ran with
`NEREUS_DELAY_PULSAR_WITH_OXIA=0`; neither E2E is a runtime, milestone or
release PASS.

## 2026-08-16 real Oxia authority provider-driven Pulsar audit

The provider-driven P1 Worker E2E was rerun with
`NEREUS_DELAY_PULSAR_WITH_OXIA=1`. Assignment publication, session-bound
ownership and the provider-driven Claim/Admission/physical Publish graph used
the real Oxia backend. Locks were Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, image
`sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose `nereus-delay-pulsar-e2e-1786815185-13398`, Pulsar ports
`21615,21616`, and Oxia port `16658`:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker source-applied physical publish passed: Admission source ledger=24/3, typed PULSAR_SEND_ACK target ledger/entry=25/0, Outcome source ledger=24/4, exact payload readback
Pulsar Worker source-applied physical publish passed: Admission source ledger=35/2, typed PULSAR_SEND_ACK target ledger/entry=36/0, Outcome source ledger=35/3, exact payload readback
Pulsar Worker authority smoke passed: real Oxia session-bound lease
Pulsar signed Route -> guarded SUBSCRIBE barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: generation=16, barrier=22/0, routeRevision=1, assignmentRevision=1, source=22/1, ACK, final checkpoint
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, source-bound typed destination SEND ACK/payload readback, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, source-applied physical publish with typed Outcome and payload readback, ACK handoff, and broker-restart resume.
```

This is positive real-Oxia authority evidence across a standalone Broker
restart. It does not prove multi-Broker Pulsar failover, Oxia failover or
partition behavior, crash/response-loss resolution, live
Profile/credential/Object Store/catalog authority, placement,
checkpoint/quiescence or §23.5 release readiness.

## 2026-08-16 real Oxia authority provider-driven Kafka audit

The provider-driven Kafka Worker E2E was rerun with
`NEREUS_DELAY_KAFKA_WITH_OXIA=1`. Worker assignment/ownership and the
due-to-Claim-to-physical-Publish graph used the real Oxia backend. Locks were
K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Compose project was
`nereus-delay-kafka-e2e-1786815566-17636`, with Kafka ports
`21792,21793,21794` and Oxia port `16659`:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=2, Outcome source offset=4, exact payload readback
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

This is positive real-Oxia authority evidence for the provider-driven Kafka
Worker path across a three-broker K1/K2 run and broker-1 survivor cut. It does
not prove Oxia failover or partition behavior, crash/response-loss resolution,
live Profile/credential/Object Store/catalog authority, multi-shard placement,
checkpoint/quiescence or §23.5 release readiness.

## 2026-08-16 real Oxia accepted-Route Kafka failover audit

The accepted-Route Kafka Worker failover-only E2E used
`NEREUS_DELAY_KAFKA_WITH_OXIA=1`,
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1` and
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY=1`. Locks were K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Compose project was
`nereus-delay-kafka-e2e-1786815918-21809`, with Kafka ports
`21892,21893,21894` and Oxia port `16660`:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=2, commitSync ACK, accepted-route broker failover, final checkpoint
Kafka accepted-route broker failover E2E passed: Route-bound Worker applied and ACKed after broker-1 failover, then released its final checkpoint and Oxia assignment.
```

This is positive real-Oxia accepted-Route Worker evidence across a
three-broker broker-1 failover cut. It remains a failover-only single-shard
receipt and does not prove Oxia failover or partition behavior,
crash/response-loss resolution, live Profile/credential/Object Store/catalog
authority, multi-shard placement, physical delayed Publish,
checkpoint/quiescence or §23.5 release readiness.

## 2026-08-16 locked P1 two-Broker Worker failover audit

The checked-in `e2e/run-pulsar-multi-broker-failover-e2e.sh` harness ran a
same-topic Worker through a real Broker-1 stop and Broker-2 resume. Its
bounded topology was one ZooKeeper, one BookKeeper and two P1 Brokers, with
internal bridge-network forwarding separated from the host-facing external
listener. The Worker used the P1 `listenerName=external` lookup path and the
Admin client followed owner redirects.

The accepted run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, image
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
Compose project `nereus-delay-pulsar-multi-e2e-1786819171-58253`, Pulsar
ports `21985,21986,21987,21988`, and Oxia port `16666`:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/3, typed PULSAR_SEND_ACK target ledger/entry=5/0, Outcome source ledger=3/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=3/0, active apply ledger/entry=3/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker authority smoke passed: real Oxia session-bound lease
Pulsar multi-Broker failover E2E passed: same-topic guarded Worker resumed through broker-2 after broker-1 stop, applied the source record, completed provider-driven physical Publish, ACKed the source and released its final checkpoint and owner assignment.
```

This is positive real-Oxia same-topic Worker failover evidence. It is bounded
to a single BookKeeper, single ZooKeeper, two Brokers and one single-shard
topic; it does not prove production metadata/storage HA, Oxia failover or
partition behavior, crash/response-loss resolution, multi-shard placement,
live Profile/credential/Object Store/catalog authority,
checkpoint/quiescence or §23.5 release readiness.

## 2026-08-16 real Gateway mTLS/RS256 network and Oxia durability audit

Delay commit `9a170837` adds the opt-in
`OxiaRealGatewayGrpcSmokeTest` and `e2e/run-gateway-real-e2e.sh`. The test
starts the actual `GatewayGrpcServer.mutualTls` composition, connects with a
real client certificate, signs an RS256 JWT with the exact issuer/audience/kid
policy and certificate confirmation, and sends the generated Schedule RPC
over a Netty gRPC channel. Repeating the exact request proves one Semantic
preparation and one Oxia idempotency attempt; direct range scans prove the
admission lease is released and the two digest-only audit phases are exactly
deduplicated. A mutated signature is rejected as `UNAUTHENTICATED` before
preparation.

The source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786820415-72294`, Oxia port `16668`, Gateway port
`22350`, and ended with `BUILD SUCCESSFUL` after `11 actionable tasks: 11
executed`:

```text
Gateway mTLS/RS256 network E2E passed: authenticated Schedule and invalid JWT rejection
Gateway Oxia durable E2E passed: admission released, one idempotency attempt, and two digest-only audit events
Dockerized Gateway real-service smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

Audit boundary: this is one real Gateway network/authentication and durable
record composition. It uses deterministic Semantic-Core/submission doubles
and the bounded local `SDK_BACKPRESSURE_NOT_SUBMITTED` branch, so it does not
promote live Kafka/Pulsar publish, certificate operations, HA/session churn,
load, crash cuts or V1 release readiness.

## 2026-08-16 Gateway server-restart idempotency audit

Commit `232ce29d` reruns the Gateway network receipt across a real server
restart. The first Gateway instance handles the authenticated Schedule and is
closed; a second instance on the same port then serves the exact request with
the same mTLS/JWT policy and Oxia records. The returned outcome is byte
identical, while Semantic-Core preparation and submission remain one. The
durable scans still prove one released admission record, one idempotency
attempt and two deduplicated audit events.

The source-locked revalidation used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786820937-77983`, Oxia port `16669`, Gateway port
`22351`, and ended with `BUILD SUCCESSFUL` after `11 actionable tasks: 11
executed`:

```text
Gateway mTLS/RS256 network E2E passed: authenticated Schedule and invalid JWT rejection
Gateway restart/idempotency E2E passed: server restarted and returned the exact durable outcome without a second attempt
Gateway Oxia durable E2E passed: admission released, one idempotency attempt, and two digest-only audit events
Dockerized Gateway real-service smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

Audit boundary: this proves one Gateway process restart/reconnect path, not
multi-process HA, admission session churn, certificate deployment/rotation,
load, crash/response-loss cuts, live Kafka/Pulsar publication or V1 release
readiness.

## 2026-08-16 Gateway two-server Oxia CAS race audit

Commit `1213650b` adds two independent Gateway server compositions with
separate Oxia client sessions and separate durable wrapper instances. They
receive concurrent identical mTLS/JWT requests against one tenant-scoped
durable key prefix. The race converged on one physical attempt; the losing
request stayed within the defined in-flight/uncertain branch, and a settled
request reread the durable aggregate. Admission was empty after both leases
closed, and the idempotency scan contained one attempt.

The source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786821521-84089`, Oxia port `16670`, Gateway ports
`22353,22354`, and ended with `BUILD SUCCESSFUL` after `11 actionable tasks:
11 executed`:

```text
Gateway mTLS/RS256 network E2E passed: authenticated Schedule and invalid JWT rejection
Gateway restart/idempotency E2E passed: server restarted and returned the exact durable outcome without a second attempt
Gateway two-server CAS race E2E passed: independent Gateway servers converged on one durable physical attempt
Gateway Oxia durable E2E passed: admission released, one idempotency attempt, and two digest-only audit events
Dockerized Gateway real-service smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

Audit boundary: this is independent-client concurrency in one test JVM, not
production multi-process HA, Oxia failover, session churn, certificate
operations, load, crash/response-loss resolution, live Kafka/Pulsar
publication or V1 release readiness.

## 2026-08-16 Oxia Route notification recovery audit

Delay commit `6a64ca894928a9a6f210129e2567b02f7df1329f` closes the missing
session-rotation notification cut for the session-fenced Route provider. On a
started provider, `refresh()` revalidates or rotates the main ephemeral marker,
replaces the separate notification client and registers the same callback on a
fresh offset-tracked Oxia notification manager before replaying the signed
head/event stream. Raw clients keep the native Oxia retry path without a second
callback registration.

The gated real-service run used two-second Route sessions, a five-second
stop window, Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:05d66cf3117d24b358baee21fb87caa001c99bec2f734ea9ce2549f7675d085a`,
Compose `nereus-delay-v1-oxia-e2e-1786822655-96457`, and port `16675`. After
the restart, the provider identity changed, one explicit refresh restored the
cache, and a later revision reached the provider without a second refresh:

```text
Oxia Route notification restart recovery passed: revision=2, session rotated, notification stream resumed without a second provider refresh
Dockerized Oxia Route notification restart smoke passed: session rotation and notification stream recovery
```

Audit boundary: this is a bounded single-node Oxia restart/session-rotation
receipt. It does not establish multi-node Oxia failover, partial-placement
behavior, multi-shard activation, native eligibility, live resource/profile
authority, production Worker transport or V1 release readiness.

## 2026-08-16 Gateway certificate replacement audit

Commit `cbe895e1` adds an independent certificate set to the real Gateway
network harness. The first server uses the original CA, server certificate and
client certificate. The replacement server uses a new CA, server certificate
and client certificate on the same port. An old client trusts the new CA but
still presents the old client certificate and is rejected during mTLS; the new
client presents a JWT bound to its new certificate fingerprint and rereads the
same Oxia idempotency result. Counters and scans require one attempt, one
quiescent aggregate, released admission and two audit records.

The source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:054bb7d13cd9c3d7a6c4dd0b70d5820b6ece2115e840cf47ff8ea0e679a9248c`,
Compose `nereus-delay-gateway-e2e-1786823102-1813`, Oxia port `16677` and
Gateway port `22356`:

```text
Gateway certificate rotation E2E passed: old mTLS client rejected and new certificate reread the exact durable outcome
```

Audit boundary: this is bounded same-port server replacement and channel
revalidation. It does not establish hot reload, staged rollback, certificate
revocation, multi-process Gateway HA, load, crash/response-loss handling,
live Kafka/Pulsar publication or V1 release readiness.

## 2026-08-16 Gateway durable admission/idempotency session-churn audit

Commit `f9fa48b7` binds Gateway admission, idempotency and audit I/O to the
exact Oxia ephemeral session marker through
`SessionBoundOxiaGatewayRecordClient`. The wrapper verifies the marker before
and after every `get`/`put`; marker loss is surfaced as a fail-closed session
unavailable boundary. Commit `241068fd` adds the real gated stop/start test
`gatewayDurableRecordsRecoverAfterOxiaSessionChurn`, which keeps the old
Gateway server alive across a five-second Oxia outage after two-second sessions
expire, rejects stale admission/idempotency operations, and then composes three
new sessions against the same durable prefix.

The source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:e36ced8f25cff4ea67e61a1dd392668d53b5ac79ffe992587d49548cf038a059`,
Compose `nereus-delay-gateway-e2e-1786824181-13578`, Oxia port `16678` and
Gateway port `22357`:

```text
Gateway Oxia session churn E2E passed: stale durable sessions failed closed and recovery reread the exact durable outcome
Dockerized Gateway Oxia session churn smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

Audit boundary: this is one single-node Oxia session-rotation and controlled
recomposition receipt, not transparent automatic reconnect, multi-node Oxia or
Gateway HA, crash/response-loss resolution, load, live Kafka/Pulsar publication
or V1 release readiness.

## 2026-08-16 Gateway multi-node Oxia DataServer failover audit

Commit `43493a709e4041e94c7f4f270a25b2725534ab59` adds the isolated
three-Coordinator/three-DataServer deployment and the real Gateway cut
`gatewayRecoversAcrossRealOxiaDataServerFailover`. The harness uses the real
Oxia admin API to create a three-replica namespace, identifies the actual
shard leader, stops that DataServer and waits for a successor before releasing
the test gate. The old Gateway process and its three session-bound Oxia
handles remain in place; the test explicitly checks each session marker after
the leader transition.

The source-locked receipt used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-oxia-cluster-gateway-e2e-1786825431-27266`, Coordinator ports
`16691,16692,16693`, DataServer ports `16681,16682,16683`, Gateway port
`22358`, and the six built image IDs recorded in
`docs/IMPLEMENTATION-STATUS.md`. The cut started with `ds-3`, elected `ds-1`
after `data-server-3` stopped, and ended with:

```text
Oxia shard successor leader: ds-1
Gateway multi-node Oxia failover E2E passed: session-bound clients preserved the exact durable outcome after the shard leader stopped
Oxia multi-node Gateway failover E2E passed: session-bound Gateway reread the exact durable outcome after leader stop
Dockerized Oxia multi-node Gateway failover smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

The checks require byte-identical idempotent outcome, one Semantic preparation,
one physical attempt, zero admission leases, one quiescent idempotency record
and two audit records. This is real multi-node DataServer leader-stop and
session-preserving recovery evidence. It does not establish total-outage
reconnect, partial-placement/quorum-loss behavior, production Gateway HA,
crash/response-loss resolution, load, live Kafka/Pulsar publication or V1
release readiness.

## 2026-08-16 Gateway STARTED CAS response-loss recovery audit

Commits `a120b6bd` and `7adb95f0` close the state-machine recovery required
after a successful Gateway `STARTED` CAS whose response is lost. The implementation never derives
an ownership permit from a reread. Before `uncertaintyAtEpochMs`, a same-key
caller sees the active attempt without an aggregate; at or after that trusted
deadline, it decodes the persisted prepared bytes and CASes the exact attempt
to `UNCERTAIN`/`QUIESCENT` with the canonical aggregate. A failed recovery CAS
is reread and cannot authorize a second physical send. Explicit retry attempts
use the same recovery and retain their retry identity through outcome
completion.

The focused test
`OxiaGatewayIdempotencyStoreTest.attemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit`
passed with the committed-then-lost response injection. It verifies no permit
before or after reread, no aggregate before the deadline, and one persisted
uncertain attempt after the deadline. The companion
`retryAttemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit`
verifies the retry path remains `EXISTING_RETRY` after recovery. Full
`./gradlew check` also passed.

This is deterministic local CAS evidence, not a real Oxia network fault
injection. Physical Kafka/Pulsar response-loss or crash resolution, Gateway
transparent reconnect/HA, multi-node placement and V1 release readiness
remain open.

## 2026-08-16 real Oxia Gateway STARTED CAS response-loss receipt audit

Test commit `1ce8b7e604ca969adabd7372e80ce04f96e5b45a` adds
`gatewayRecoversAfterCommittedOxiaAttemptResponseLoss`. The test uses a real
Oxia service and the normal mTLS/RS256 Gateway path. A test-only wrapper around
the idempotency record client throws after the real Oxia `STARTED` CAS has
committed, modeling a committed-then-lost client response without fabricating
the durable record. The first Schedule returns managed `ENQUEUE_UNCERTAIN`;
after the trusted uncertainty deadline, the same request rereads and converges
to the exact byte-identical outcome without a second physical submission.

The receipt is locked to Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:db1b0409c36cbf16bc21d63f74932a0f9f188f5d0101b9b398e6b90de3e01cc`,
Compose `nereus-delay-gateway-e2e-1786827281-47103`, Oxia `16695` and Gateway
`22360`. The real scans require one admission record with zero leases, one
quiescent idempotency record with one `UNCERTAIN` attempt and a non-null
aggregate, and four audit records because the two authenticated calls use
different trusted timestamps. The run ended with:

```text
Gateway Oxia STARTED response-loss E2E passed: committed attempt was reread after deadline as exact UNCERTAIN without a second physical submission
```

This is source-bound real-Oxia durability and recovery evidence with controlled
post-commit response loss. It is not raw network packet-loss or Broker
transport response-loss evidence; physical Kafka/Pulsar crash/response-loss
resolution, transparent reconnect/HA, load, multi-shard placement and V1
release readiness remain open.

## 2026-08-16 real Oxia Gateway RETRY_UNCERTAIN response-loss receipt audit

Commit `bcac733ae7e48776ce7d427d66643d21a6dd2a7d` fixes the retry transition
so `GatewayIdempotencyRecordV1.withAttempt` clears the prior uncertain
aggregate while an explicit retry is `ACTIVE`. The deterministic retry CAS
test asserts that boundary. The source-bound
`gatewayRecoversAfterCommittedOxiaRetryAttemptResponseLoss` test then loses
the response after real Oxia commits both the initial `STARTED` CAS and the
explicit retry `STARTED` CAS. It recovers the first attempt, starts the retry
only from the exact prior attempt, and verifies byte-identical retry output
before and after the retry uncertainty deadline with no physical submission.

The receipt is locked to Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:ba41122a1fa21cdcfb1c2680e81a3f14519d6f1f9213f82dfa284fb3e792428d`,
Compose `nereus-delay-gateway-e2e-1786828250-57299`, Oxia `16697` and Gateway
`22362`. Final scans require one admission record with zero leases, one
quiescent idempotency record with two `UNCERTAIN` attempts and an aggregate,
and eight digest-only audit records. The run ended with:

```text
Gateway Oxia RETRY_UNCERTAIN response-loss E2E passed: committed retry attempt was reread after deadline as exact UNCERTAIN without a second physical submission
```

This is source-bound real-Oxia explicit-retry durability and recovery with a
controlled post-commit response cut. It is not raw network packet-loss or
Broker transport response-loss evidence; physical Kafka/Pulsar crash/response-
loss resolution, transparent reconnect/HA, load, multi-shard placement and
V1 release readiness remain open.

## 2026-08-16 Kafka K2 committed EndTxn response-loss receipt audit

Commit `376252bae0faf6f2d5120e223886b3af8a54e636` adds the response-loss-only
real-client cut. A test-only `GuardedTransactionalProducer` proxy calls the
real Kafka producer's guarded transaction commit first and throws only after
that call returns. The transport then exercises its normal
`resolveCommitUncertainty` branch. A separate `read_committed` consumer and
the source-bound receipt provider prove the exact target and keyed receipt,
Fetch/LSO evidence, and typed `KAFKA_TRANSACTIONAL_RECEIPT` before returning
`PUBLISHED`.

The receipt is locked to Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786828912-64477`, ports `19569,19570,19571`,
and Delay `376252bae0faf6f2d5120e223886b3af8a54e636`:

```text
K2 committed response-loss smoke passed: real EndTxn committed the exact target-plus-receipt pair, the local response was discarded, and typed read_committed evidence resolved PUBLISHED
K2 committed response-loss E2E passed: real EndTxn commit was followed by local response loss and exact read_committed typed receipt resolution.
```

This is a source-bound real-Broker durable post-commit response-loss receipt
with controlled client-side response loss. It is not raw socket fault
injection and does not establish Broker crash/failover, generic Fetch response
loss, LSO/retention-floor recovery, or V1 release readiness.

## 2026-08-16 Pulsar committed SEND response-loss receipt audit

Commit `12334f63` adds an optional source-bound recovery provider to
`PulsarClientArtifactDestinationTransport`. The provider is invoked only after
the guarded Producer completion is exceptional or otherwise uncertain and may
return `PULSAR_SEND_ACK` evidence. The transport accepts `PUBLISHED` only for
typed verified evidence whose business mutation is bound to the same
`publishAttemptId`; empty, malformed or divergent provider output remains
`UNKNOWN`.

The real smoke uses a test-only dynamic proxy around `sendAsync()`. It allows
the guarded client and Broker to complete the SEND, stores the real
`GuardedMessageId`, and then returns an exceptional completion to the
transport. The provider checks the exact `TopicResourceGuard`, physical topic,
partition, `MessageIdAdv` ledger/entry/batch values, and
`GuardedSendSuccessEvidence` attestation before constructing the typed ACK.
The smoke verifies the result through a guarded consumer and exact payload
readback. Commit `12334f63` also copies the cluster entrypoint into the
single-node Docker build context because the shared Pulsar Dockerfile declares
it.

The receipt is locked to Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose `nereus-delay-pulsar-e2e-1786829967-75545`, host ports `21885` and
`21886`, and Delay `12334f63`. The dedicated run printed:

```text
Pulsar committed response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved PUBLISHED
Pulsar destination committed response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and exact guarded payload readback.
```

This is a source-bound real-Pulsar durable post-send receipt with a controlled
client-side completion cut. It is not raw network loss, process/Broker crash
recovery, multi-Broker failover, Attempt Journal completion recovery or a V1
release PASS.

## 2026-08-16 Pulsar Worker source ACK response-loss receipt audit

Commit `31145cc8` adds a focused Worker source-ACK response-loss mode. A
test-only proxy calls the real receipt-enabled Pulsar `acknowledge` operation
and throws only after it returns. The source adapter reports `ACK_UNKNOWN`
while retaining the exact in-flight record; the Worker smoke now retries that
bounded turn, and `SourceApplyCoordinator` reuses the already applied outcome
instead of applying the record again. Only the second ACK clears the pending
record and allows the source cursor/Worker drain to complete.

The receipt is locked to Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose `nereus-delay-pulsar-e2e-1786830626-82754`, host ports `21887` and
`21888`, and Delay `31145cc8`. The run printed:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker source ACK response-loss smoke passed: real ACK was accepted before the local response was discarded, and the same source record was ACKed on the next bounded Worker turn
Pulsar Worker source ACK response-loss E2E passed: real ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

This is a controlled post-receipt client response cut. It is not raw network
loss, process/consumer/Broker crash recovery, multi-Broker failover or a
complete D6 source-ACK/crash or V1 release receipt.

## 2026-08-16 Pulsar Worker source-applied destination response-loss audit

Commit `c903fe34` wires the source-bound destination recovery provider into
the real Pulsar Worker physical bridge. The test-only Producer proxy lets the
real guarded SEND complete, captures its exact `GuardedMessageId`, and then
returns a failed local completion. The provider validates the exact resource
guard, topic, partition, ledger/entry, batch coordinates and attestation before
returning typed `PULSAR_SEND_ACK`. The Worker then source-applies the typed
`PUBLISH_OUTCOME`, closes the matching publish attempt as `PUBLISHED`, and
reads back the exact destination payload.

The receipt is locked to Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose `nereus-delay-pulsar-e2e-1786830983-86815`, host ports `21889` and
`21890`, and Delay `c903fe34`. The run printed:

```text
Pulsar Worker destination response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved the source-applied PUBLISHED Outcome
Pulsar Worker source-applied physical publish passed: Admission source ledger=9/3, typed PULSAR_SEND_ACK target ledger/entry=10/0, Outcome source ledger=9/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker destination response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and the source-applied Outcome completed.
```

This is a controlled client-side post-SEND response cut in one source-applied
Worker process. It is not raw network loss, process/Broker crash recovery,
multi-Broker failover, Attempt Journal completion recovery or a V1 release
PASS.

## 2026-08-16 Kafka Worker source-applied destination response-loss receipt audit

Commit `e95d1c0cbaf4b94c8523d6fd9994b6487102f400` adds the focused Worker
destination response-loss receipt. A test-only producer proxy lets real
`EndTxn` commit the exact target-plus-keyed-receipt pair and then discards only
the local response. The existing source-bound `read_committed` provider
resolves typed `KAFKA_TRANSACTIONAL_RECEIPT` evidence; the Worker then
source-applies the matching `PUBLISH_OUTCOME`, closes the publish attempt and
verifies exact payload readback.

The receipt is locked to Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786831579-93599`, ports `19669,19670,19671`,
and Delay `e95d1c0cbaf4b94c8523d6fd9994b6487102f400`. The run printed:

```text
Kafka Worker destination response-loss smoke passed: real EndTxn committed the exact target-plus-receipt pair, the local response was discarded, and typed read_committed KAFKA_TRANSACTIONAL_RECEIPT evidence resolved the source-applied PUBLISHED Outcome
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka Worker destination response-loss E2E passed: real EndTxn response loss resolved through typed read_committed KAFKA_TRANSACTIONAL_RECEIPT evidence and the source-applied Outcome completed.
```

This is controlled client-side post-commit response-loss evidence in one
source-applied Worker process. It is not raw network fault injection, crash
recovery, multi-Broker failover, Attempt Journal recovery or a V1 release PASS.

## 2026-08-16 Kafka Worker source ACK response-loss receipt audit

Commit `d165e73e457834be55af58d238980be65c2054c7` adds the focused source ACK
receipt. A test-only guarded-consumer proxy delegates the real `commitSync`
operation and then discards only its local response. The Kafka source adapter
retains the exact in-flight record, the Worker reports `ACK_UNKNOWN`, and the
next bounded turn retries the same source record without repeating Store apply.
The final committed offset and checkpoint complete only after the retry ACK.

The receipt is locked to Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786832218-928`, ports `19679,19680,19681`,
and Delay `d165e73e457834be55af58d238980be65c2054c7`. The run printed:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker source ACK response-loss smoke passed: real commitSync ACK was accepted before the local response was discarded, and the same source record was ACKed on the next bounded Worker turn
Kafka Worker source ACK response-loss E2E passed: real commitSync ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

This is controlled client-side post-ACK response-loss evidence. It is not raw
network fault injection, process/consumer/Broker crash recovery,
multi-Broker failover, coordinator recovery or a V1 release PASS.

## 2026-08-16 S3-compatible checkpoint Object Store adapter audit

Delay runtime commit `e01d3ee8708a53487747b0ef721d1f0d107ff677` adds the
Profile-bound `S3CompatibleCheckpointObjectStoreAdapter`. Constructor checks
the exact `OBJECT_STORE` semantic body, provider kind, endpoint configuration
digest and non-secret credential authorization-scope digest before HTTP. The
adapter uses SigV4 over canonical path-style S3 requests, reuses the existing
checkpoint object identity function, sends `If-None-Match: *`, writes the
manifest last, and verifies the remote object byte-for-byte after a successful
file PUT as well as after HTTP precondition conflict or transport ambiguity.
The download path validates the exact resource/profile and manifest, streams
bounded objects into a private staging tree, re-inventories all files and
atomically publishes the target directory.

The focused local receipt is
`S3CompatibleCheckpointObjectStoreAdapterTest`, run with:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

It passed with `BUILD SUCCESSFUL`; the raw local HTTP fixture covers the
SigV4/conditional headers, endpoint/credential drift before I/O, manifest
response loss followed by exact reread, complete restore and same-key
immutable conflict. This closes only the provider adapter's bounded identity
and response-loss seam. It does not close real S3/MinIO conformance,
credential-use leases or rotation, provider quiescence/consistency
attestation, version-aware deletion, multi-shard RecoveryPin/catalog
transaction, process/network chaos, or the V1 release matrix.

## 2026-08-16 Object Store credential-use lease gate audit

Delay runtime commit `078c66ce141a17a3e757aabb88bae5140d1d297a` adds
`ObjectStoreCredentialUseLeaseGate`. The lease-gated S3-compatible adapter
constructor checks that the gate is bound to the same Profile, and `upload` and
`download` invoke the gate before any HTTP request. The gate validates the
exact binding/protection/lease projection, configured lease TTL and attestation
age, current local trusted time and the loaded immutable credential
fingerprint; its test covers a current lease, expiry, fingerprint drift and a
protection horizon shorter than the lease.

The focused command and full check both passed:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.ObjectStoreCredentialUseLeaseGateTest \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
./gradlew check --no-daemon --console=plain --quiet
```

This is a local call-order and identity recheck receipt. It does not prove
Oxia Head/protection CAS, attestation trust-set verification, secret
resolution, credential rotation, real S3/MinIO conformance, provider
quiescence, version-aware deletion, catalog transaction, chaos or V1 release
readiness.

## 2026-08-16 Oxia credential Profile CAS authority audit

Delay runtime commit `37d8efb49876e8eb95b9d214f0ad9ec1afe48595` adds
`OxiaSyncProfileCatalogBackend`. Its one-record state binds the exact Profile
semantic bytes, immutable generation bindings, mutable Head and all matching
Protection projections under a final digest. Generation-1 publication and
`RotateEquivalentSecretRequestV1` use exact version CAS with idempotent retries;
lease issuance checks Head/binding/fingerprint/attestation/TTL inputs and
raises the corresponding protection horizon in the same CAS before returning
the lease. `OxiaSyncProfileCatalogBackendTest` covers reopen, rotation,
protection-bound lease issuance, response-loss exact reread, Head fencing and
semantic collision. The opt-in real test passed against Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` with one test, zero skips and zero
failures/errors in Compose project `nereus-delay-v1-oxia-e2e-1786835835-39861`
on port `16693`.

This is single-Profile, single-record Oxia authority evidence. It does not
prove secret resolution, attestation trust-set verification, actor or
source-order authorization, retained-generation quota/GC, cross-record
Owner/Route/session transactions, multi-node failover for this authority,
provider credential rotation/quiescence, real Object Store conformance, chaos
or V1 release readiness.

## 2026-08-16 Object Store adapter activation binding audit

Delay runtime commit `138c1c0e5e0e9af9c3b8e93b223da5b3e322a6bb` adds the
`CredentialProfileAuthority` activation seam and
`OxiaObjectStoreCredentialLeaseActivator`. It resolves the exact Profile,
Head, Binding and post-issuance Protection once, obtains private material from
an injected resolver, rejects a fingerprint mismatch before calling the lease
authority, and passes the resulting lease gate into
`S3CompatibleCheckpointObjectStoreAdapter`. The focused composition test
covers exact activation, resolver fingerprint drift and a lease whose
protection revision is not proven by the reread projection; the full check
passed.

This closes only local activation wiring. It does not prove a real secret
manager, trust-set or actor authorization, automatic renewal, multi-node
activation failover, provider credential rotation/quiescence, real S3/MinIO,
deletion, chaos or V1 release readiness.

## 2026-08-16 Credential attestation trust-set verification audit

Delay runtime commit `f758d010b4d75f9c53d1f6e2cf01d573d655fd1c` adds the
immutable `CredentialAttestationTrustSet` and makes
`OxiaSyncProfileCatalogBackend` require it for every credential binding
publication, equivalent-secret rotation, canonical state decode/reopen and
bounded lease issuance. The trust set orders and deduplicates the exact
verifier-version/verifier-id/signing-key tuple, retains canonical Ed25519
public-key bytes with an explicit verification window, and exposes a stable
semantic digest. A binding is accepted only when the tuple is present, the
attestation interval fits the retained key window and the attestation's
Ed25519 signature verifies.

The deterministic regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.runtime.CredentialAttestationTrustSetTest \
  --tests io.nereusstream.delay.runtime.OxiaSyncProfileCatalogBackendTest \
  --no-daemon --console=plain
```

It passed with `BUILD SUCCESSFUL`; the full Gradle check also passed. The
Dockerized real-service receipt used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786837306-55484`, host port `16694`, and
`OxiaRealProfileCatalogSmokeTest.profileHeadProtectionLeaseAndRotationReopenAgainstRealService`.
The report recorded `tests=1`, `skipped=0`, `failures=0`, `errors=0`; the run
ended with `BUILD SUCCESSFUL` and
`Dockerized Oxia real-service smoke passed for 37a17bef17202d5fd6e23282da5fd26d94865484`.

This closes only local trust-set selection/signature/window verification and
its integration with one Profile's single-record CAS authority. It does not
prove source-ordered trust-set publication/rotation, actor authorization,
secret-manager resolution, cross-record Owner/Route/session transactions,
multi-node failover for this authority, automatic renewal, provider
rotation/quiescence, real S3/MinIO, deletion, chaos or V1 release readiness.

## 2026-08-16 Same-generation Object Store lease renewal audit

Delay runtime commit `8307d690351af1699a6a9cb69e2cfe9bfe26a4a2` adds
`RenewableS3CompatibleCheckpointObjectStoreAdapter` and atomic projection
replacement to `ObjectStoreCredentialUseLeaseGate`. The renewable wrapper
checks the local lease expiry before each upload/download and reads the
authority only inside its explicit renewal window. It requires the exact
Profile, unchanged Head generation and byte-identical immutable Binding,
re-resolves private material and its attested fingerprint, obtains fresh
trusted-time evidence, issues a bounded lease and verifies the reread
Protection before atomically replacing the local gate. Any Head rotation is
treated as an adapter-quiescence boundary and fails before Provider I/O.

The deterministic regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.RenewableS3CompatibleCheckpointObjectStoreAdapterTest \
  --tests io.nereusstream.delay.store.ObjectStoreCredentialUseLeaseGateTest \
  --no-daemon --console=plain
```

It passed with `BUILD SUCCESSFUL`; the full Gradle check also passed. The
focused tests prove no authority read outside the renewal window, lease and
Protection revision advancement inside the window, and fail-closed rejection
of a rotated Head before Provider I/O.

This closes only same-generation opportunistic renewal and local gate
replacement. It does not prove a scheduled multi-process renewal owner,
source-ordered rotation/quiescence, secret-manager resolution, cross-record
Owner/Route/session transactions, multi-node authority failover, real
S3/MinIO, provider consistency/deletion, chaos or V1 release readiness.

## 2026-08-16 Verified credential material cache audit

Delay runtime commit `d9b713a9159a8b2672a2b0aea5bd5243ca798c3e` adds
`VerifiedCredentialMaterialCache`, the local cache implementation for the
activation resolver seam. Cache installation checks the exact Object Store
Profile and binding, authorization scope, configured attestation trust set and
resolved fingerprint before private material is installed. The immutable-view
snapshot is keyed by Profile ref, secret generation, binding digest and
secret-reference hash; a miss is null and cannot select another generation or
perform Oxia/Vault/Provider I/O. A failed exact install or batch replacement
does not publish a partial snapshot.

The deterministic regression is:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.VerifiedCredentialMaterialCacheTest \
  --tests io.nereusstream.delay.runtime.CredentialAttestationTrustSetTest \
  --no-daemon --console=plain
```

It passed with `BUILD SUCCESSFUL`; the full Gradle check also passed. This
closes only local verified-cache identity and fail-closed lookup. It does not
prove an external secret-manager reader, source-ordered cache publication or
refresh, actor authorization, cross-record Owner/Route/session transactions,
multi-node authority failover, credential rotation/quiescence, real S3/MinIO,
chaos or V1 release readiness.

## 2026-08-16 MinIO S3-compatible checkpoint provider audit

Delay implementation commit `31ba5661` adds the opt-in
`S3CompatibleMinioRealSmokeTest` and `e2e/run-minio-real-e2e.sh`. The harness
uses the local MinIO image
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` at repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
and observed image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`.
The source-locked run used container
`nereus-delay-minio-e2e-1786839150-77162`, host endpoint
`http://127.0.0.1:62159` and bucket
`nereus-delay-checkpoints-1786839150-77162`. It created the bucket through
curl SigV4, executed the real adapter with Gradle `--rerun-tasks`, and cleaned
the matching container.

The JUnit report recorded `tests=1 skipped=0 failures=0 errors=0`; the run
ended with `BUILD SUCCESSFUL` and verified canonical file/manifest upload,
same-key immutable retry and complete two-file download restore through
MinIO. This is bounded one-provider evidence only. Generic S3/provider
compatibility, credential authority/renewal/rotation, provider deletion and
consistency attestation, cross-record failover, chaos and V1 release readiness
remain open.

## 2026-08-16 Exact Object Store provider-version audit

Delay commits `b971cd3f` and `2981a269` make the mandatory exact-version
deletion profile requirement fail closed in
`S3CompatibleCheckpointObjectStoreAdapter`. Successful PUTs, conflict or
ambiguous-response rereads and download GETs now require the provider's
`x-amz-version-id`; a missing header cannot become a `sha256-*` production
identity. The focused negative test is
`S3CompatibleCheckpointObjectStoreAdapterTest.rejectsProviderThatOmitsExactVersionHeaders`.

The source-locked MinIO run used container
`nereus-delay-minio-e2e-1786840003-88209`, host endpoint
`http://127.0.0.1:64830`, bucket
`nereus-delay-checkpoints-1786840003-88209`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Its JUnit report recorded `tests=1 skipped=0 failures=0 errors=0`, system-out
recorded provider version
`780f1e1f-c7da-4dc1-ae4e-a7b9be4f801c`, and the run ended with
`BUILD SUCCESSFUL`.

This is exact provider-version evidence for the locked MinIO path. It does
not establish complete version-aware checkpoint deletion, source-ordered
retire/delete authorization, Recovery Floor/Pin release, provider
consistency, credential rotation, cross-provider compatibility, chaos,
failover or V1 release readiness.

## 2026-08-16 Catalog-bound manifest version readback audit

Delay commit `d7f51441` changes S3-compatible checkpoint download to request
the catalog-bound manifest provider version as the exact `versionId` query,
including that query in the SigV4 canonical request. The response's
`x-amz-version-id` remains byte-equal to the `CheckpointResourceV1` identity;
the local fake provider rejects a different requested version.

The source-locked MinIO run used container
`nereus-delay-minio-e2e-1786840389-93104`, endpoint
`http://127.0.0.1:49401`, bucket
`nereus-delay-checkpoints-1786840389-93104`, and image digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The JUnit report recorded `tests=1 skipped=0 failures=0 errors=0`, provider
manifest version `ac201fe8-ba70-4bcb-a49c-a75a6657be55`, and `BUILD SUCCESSFUL`.

This is exact manifest readback evidence only. It does not establish file
object version capture, complete version-aware checkpoint deletion,
source-ordered retire/delete authorization, Recovery Floor/Pin release,
provider consistency, credential rotation, chaos, failover or V1 release
readiness.

## 2026-08-16 Exact checkpoint object-set deletion audit

Delay commit `3bfe030a` adds the direct provider delete boundary through
`CheckpointDeleteAdapter`, `CheckpointDeleteRequest` and
`CheckpointDeleteResult`. Before any delete, the S3-compatible adapter
validates the catalog-bound manifest/resource identity, fetches the exact
manifest provider version, streams and hashes every deterministic file object,
and records each current provider version. It then signs one exact
`versionId` DELETE per file and deletes the manifest version last. A successful
operation requires the provider response version to equal the requested
version and requires a nonblank `x-amz-request-id`; aggregate request-ID and
response hashes are retained for the canonical external delete evidence.
Missing version headers, request IDs, transport ambiguity, non-2xx status or
response-version drift fail closed.

The focused fake-provider test covers exact version query paths,
manifest-last ordering, complete object-set removal and an omitted delete
version response. The locked MinIO run used container
`nereus-delay-minio-e2e-1786841029-825`, endpoint `http://127.0.0.1:51386`,
bucket `nereus-delay-checkpoints-1786841029-825`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `e223584d-2863-45a1-8471-9b378c0899c5`, and the
harness ended with `BUILD SUCCESSFUL`.

This is bounded direct deletion evidence for one locked MinIO provider. It
does not establish `ALREADY_ABSENT` reconciliation after partial deletion,
final empty-prefix sweeping, source-ordered retire authorization, Recovery
Floor/Pin release, provider consistency/quiescence, credential rotation,
generic cross-provider compatibility, chaos, failover or release readiness.

## 2026-08-16 Checkpoint delete retry-convergence audit

Delay commit `220fc98a` closes the response-loss retry seam for the bounded
checkpoint object-set adapter. Manifest and file GET probes now retain
provider request-ID/response hashes, and a missing exact file is a known
absence rather than an instruction to issue a name-based delete. A manifest
that remains present allows deletion of the remaining verified file versions
followed by the manifest; when the manifest and every file are absent, the
adapter returns `ALREADY_ABSENT` only after all absence probes provide request
IDs and response hashes. A manifest-absent/file-present mixed state remains
rejected.

The local regression intentionally drops the first delete response after the
fake provider removes that version, then proves completion on retry and
`ALREADY_ABSENT` on the following retry. The locked MinIO rerun used container
`nereus-delay-minio-e2e-1786841861-10565`, endpoint `http://127.0.0.1:54320`,
bucket `nereus-delay-checkpoints-1786841861-10565`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `1a81631c-3bd9-41e6-a132-8abe1da7ea2e`, and the
harness ended with `BUILD SUCCESSFUL`.

This closes bounded direct delete retry convergence. Final empty-prefix
sweeping, source-ordered retire authorization, Recovery Floor/Pin release,
provider consistency/quiescence, credential rotation, generic provider
compatibility, chaos, failover and release readiness remain open.

## 2026-08-16 Checkpoint prefix sweep audit

Delay commit `c32a98f328400c71346b98188930a6efa80da7c9` adds the narrow
provider-facing prefix sweep seam. `CheckpointPrefixSweepRequest` binds the
exact Object Store Profile and nonzero lineage/checkpoint identities to a
single bounded page. `S3CompatibleCheckpointObjectStoreAdapter.sweep` signs
`ListObjectVersions` at the bucket endpoint, rejects a truncated or malformed
version listing and any key outside the exact checkpoint prefix, deletes each
listed version with the existing exact-version/request-ID checks, then lists
again and refuses to return unless the prefix is empty. The receipt hashes
both listings and every delete operation.

The fake-provider regression proves the three-object manifest/file prefix,
exact `prefix`/`versions` query, exact-version deletes and the final empty
listing. The locked MinIO run used container
`nereus-delay-minio-e2e-1786842572-18888`, endpoint
`http://127.0.0.1:56466`, bucket
`nereus-delay-checkpoints-1786842572-18888`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `f905db1e-1a7e-455c-bb32-5fa90bb7ed1f`, and the
harness ended with `BUILD SUCCESSFUL`.

This is bounded direct provider evidence only. It does not authorize or
perform the external REAPING transition, source-ordered retire, Recovery
Floor/Pin/Owner checks, multi-page continuation, provider consistency or
quiescence, credential rotation, generic provider compatibility, chaos,
failover or V1 release readiness.

## 2026-08-16 Checkpoint REAPING sweep coordination audit

Delay commit `b9fcd2aa846329ed13986b122d287375a441b2fd` adds the bounded
`CheckpointReapingSweepCoordinator`. It accepts only a PENDING_UPLOAD intent,
uses the existing exact `beginReaping` CAS with trusted evidence and the
catalog/pin guard, rereads the exact REAPING successor immediately before
provider I/O, and derives the `CheckpointPrefixSweepRequest` from that
successor. If the provider response is lost, the durable REAPING state is
retained and the same pending identity/evidence can retry the same prefix;
catalog protection is proven to block provider invocation.

`CheckpointReapingSweepCoordinatorTest` covers CAS-before-provider order,
response-loss retry and catalog protection. The source-locked MinIO run used
container `nereus-delay-minio-e2e-1786843326-27711`, endpoint
`http://127.0.0.1:58388`, bucket
`nereus-delay-checkpoints-1786843326-27711`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `f5404da4-4944-4581-a75d-80dccdad92c3`, and the
harness ended with `BUILD SUCCESSFUL`.

This is a local orchestration composition, not the missing lifecycle
authority. Old-Owner abandonment/session loss, provider ownership and
quiescence horizons, source-ordered retire/delete confirmation, Recovery
Floor/Pin/Owner transactions, multi-page policy, provider breadth, chaos,
failover and V1 release readiness remain open.

## 2026-08-16 Checkpoint REAPING quiescence proof audit

Delay commit `7b8b73885c5ec26dfc96c1b5b8a1a6ab8ec0d1d9` adds the immutable
`CheckpointReapingQuiescenceProof` and pure
`CheckpointReapingQuiescenceGuard`. The proof binds the exact pending intent
digest and reaping evidence, limits every trusted-time interval, and rejects
a configured request horizon shorter than the certified provider-ownership
lifetime plus maximum trusted-clock width. Before prefix listing, the gate
requires the observed trusted interval to be after the reaping boundary and
after both old-owner local-guard and provider-ownership closure evidence.

The focused regression covers provider-horizon rejection and the arithmetic
bound; the locked MinIO run used container
`nereus-delay-minio-e2e-1786843920-34723`, endpoint
`http://127.0.0.1:59954`, bucket
`nereus-delay-checkpoints-1786843920-34723`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, system-out recorded
manifest provider version `9c4dcab9-c03c-4860-81de-07e62302d30e`, and the
harness ended with `BUILD SUCCESSFUL`.

This is a local proof gate only: the opaque old-owner/provider evidence
digests still need certified external issuers. Owner/session loss detection,
provider quiescence attestation, source-ordered delete confirmation,
Recovery Floor/Pin/Owner transactions, provider breadth, chaos, failover and
release readiness remain open.

## 2026-08-16 Checkpoint REAPING Owner proof audit

Delay commit `44cd3230709f5e87742cd94cd9a8b7bce314a184` adds
`CheckpointReapingOwnerProof`, `CheckpointReapingOwnerProofGuard` and
`CheckpointReapingOwnerProofIssuer`. The typed proof carries the exact
pending intent digest, Owner identity, Store Incarnation and a complete
session-bound recorded `OwnerLease`; its closed kind is either
`EXACT_OWNER_EXPLICIT_ABANDON` or `RECORDED_OWNER_NOT_CURRENT`. The issuer
uses the Oxia Owner Lease adapter for exact release/reread or current-lease
replacement observation and requires trusted UTC at or after the upload
deadline. The coordinator consumes the proof before entering REAPING, and
the quiescence guard binds the old-owner evidence digest to its canonical
proof digest.

Focused Owner-proof/coordinator tests passed with 3 and 6 tests respectively;
the full `./gradlew check --no-daemon --console=plain --quiet` returned 0.
The source-locked MinIO rerun used container
`nereus-delay-minio-e2e-1786845031-48170`, endpoint
`http://127.0.0.1:62715`, bucket
`nereus-delay-checkpoints-1786845031-48170`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, provider manifest
version `ea89d80e-e63e-4980-b225-94b070d3c36b`, and `BUILD SUCCESSFUL`.

This closes only the local typed Owner-proof composition and its binding to
the provider-call gate. The proof issuer is not the production cross-record
intent/Owner/catalog transaction; certified external session-loss authority,
provider quiescence, source-ordered delete confirmation, Floor/Pin/Owner
transactions, provider breadth, chaos, failover and release readiness remain
open.

## 2026-08-16 Checkpoint provider-owned request horizon audit

Delay commit `cc97c7654cb19f88c69045cd3c33a4d970a9fed3` adds the local
`ObjectStoreProviderOwnershipTracker` and wires it through the
S3-compatible checkpoint adapter and renewable wrapper. The tracker keeps a
complete upload/download/delete/sweep operation active through nested HTTP
responses and streamed bodies, retains a bounded uncertainty horizon after
ambiguous failure, and exposes a canonical local observation only after a
one-way admission fence, operation drain and elapsed horizon. The adapter
also rechecks the local credential-use lease immediately before each
`HttpClient.send`, so a multi-object operation cannot rely only on its entry
check.

Focused tracker/adapter tests passed with 3 and 9 tests respectively; the
full `./gradlew check --no-daemon --console=plain --quiet` returned 0. The
source-locked MinIO rerun used container
`nereus-delay-minio-e2e-1786846128-60582`, endpoint
`http://127.0.0.1:49215`, bucket
`nereus-delay-checkpoints-1786846128-60582`, image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`
and repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
JUnit recorded `tests=1 skipped=0 failures=0 errors=0`, provider manifest
version `1b904a10-2104-46eb-a6fd-0bd2afe24524`, and `BUILD SUCCESSFUL`.

This closes only local adapter ownership accounting and admission fencing.
The observation cannot attest remote provider request completion or provider
quiescence, and it does not close certified REAPING provider evidence,
source-ordered deletion, Floor/Pin/Owner transactions, provider breadth,
chaos, failover or V1 release gates.

The historical bounded Pulsar Route/Worker receipt remains provenance at
`nereus/delay-full-implementation-v1@bf858b089b927fcf65129214d8ed5a7fc5300deb`.
Earlier source-lock provenance remains recorded for the Delay implementation
composition at `d9b713a9159a8b2672a2b0aea5bd5243ca798c3e`, manifest-version
readback at `87b44d77344e564b46d9c5515472a581cad733ba`, exact object-set
deletion at `fecfd1cf7283a007efb7c8618bb8ae1f6f468bd8`, delete
retry-convergence at `660a3d0c4d909dd02e412f0153dd9e701c27bbdd`, and prefix
sweep receipt at `e0402eef46026c2ee91e4fe59337bb0e40cac723`; the earlier
REAPING coordination implementation/source pair remains
`83bf17cea70b37fa42a507832693a0c43ed4d9fb` and
`b9fcd2aa846329ed13986b122d287375a441b2fd`.

## 2026-08-16 Checkpoint delete-confirmed source mutation composition audit

Delay commit `70e5f0da` adds the local
`CheckpointDeleteConfirmationComposer`. The composer consumes only the
durable retire-intent record and the typed provider result; it fails closed
unless the checkpoint's complete `ExactResourceIdentity` bytes and
domain-separated hash match, the outcome-specific immutable version is
valid, and the confirmation interval begins no earlier than the observation
interval's latest bound. It then derives the Shard subject from the retire
Source Position and signs the canonical `RESOURCE_DELETE_CONFIRMED_V1`
mutation whose logical identity is the retire mutation ID.

The focused composer test passed 4 tests, and the full
`./gradlew check --no-daemon --console=plain --quiet` returned 0. This is
strictly local evidence-to-mutation composition. It does not prove the
provider performed the delete, authorize the retire intent, evaluate
Recovery Floor/Pin/Owner coverage, append or apply the source mutation, or
promote the GC lifecycle to PASS.

## 2026-08-16 Delete-confirmed temporal evidence audit

Delay commit `a26c6816` moves the confirmation-time causal check into both
the canonical `ResourceDeleteConfirmedBody` parser and the durable
`ResourceDeleteConfirmedRecord`. A `RESOURCE_DELETE_CONFIRMED_V1` body or
tombstone is accepted only when its confirmation interval's earliest trusted
time is no earlier than the complete provider-observation interval's latest
trusted time. The regression covers direct body decode, durable record
construction, existing GC handoff fixtures and the checkpoint composer.

This closes a local evidence-ordering bypass, not provider or lifecycle
authority. It does not attest remote deletion, authorize retire/Floor/Pin/Owner
state, append or apply the source mutation, or promote GC to PASS.

## 2026-08-16 Source-ordered GC confirmation handoff audit

Delay commit `b225cef9` adds a typed `GcWorkClassExecutor.submitDeleteConfirmation`
boundary. It binds the confirmation body to the exact retire record supplied by
the caller and interprets a persisted append as valid only when the returned
Source Position is from the same authenticated physical source and is strictly
later than the retire position. A regressed or foreign returned position
fences the local Owner and is reported as UNKNOWN.

The focused handoff test covered both valid later and regressed positions; the
full Gradle check returned 0. This is only a local interpretation fence for
an external append receipt; it does not allocate Source Positions, perform
provider deletion, write/apply tombstones, or authorize the GC lifecycle.

## 2026-08-16 Oxia Recovery Pin session-bound CAS audit

Delay commit `dedd03a94fb2ab1e8d12f19ba993408646426578` adds the missing
single-pin record seam to `OxiaSyncRecoveryCatalogBackend`. The durable
catalog snapshot continues to encode only manifest/resource and scalar/typed
Floor projections; an active `RecoveryPinV1` is held in a separate canonical
Oxia record using `IfRecordDoesNotExist` and `AsEphemeralRecord`. The public
identity-bearing constructor requires the session digest supplied by the
connected Oxia client owner, while the catalog-only constructor fails closed
for pin create/release.

Create first validates the exact pin through the current local catalog
projection and its observed generation, then validates the exact ephemeral
put response, rereads canonical pin bytes and the Oxia version-derived
session identity, and rechecks catalog generation. Release binds the complete
pin value to an exact version delete and accepts a lost delete response only
after an exact absent reread. The deterministic suite covers 17 tests,
including singleton conflict, response loss, session binding and the
catalog-only fail-closed boundary; the full Gradle check returned 0.

This is a per-pin Oxia record and generation-fence slice, not a cross-record
transaction. A catalog update can still race after the final generation read,
and no claim is made for atomic upload-intent/catalog/pin activation, Owner
Lease/session-loss attestation, multi-worker placement, provider deletion or
GC release. The opt-in real-service smoke is wired to the same session
identity constructor but was skipped in this run because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset.

## 2026-08-16 Atomic publication Recovery Pin CAS audit

Delay commit `04976375` composes the shared
`OxiaSessionBoundRecoveryPinStore` into `OxiaSyncCheckpointPublicationBackend`.
The publication record still atomically CASes the PUBLISHED Upload Intent
and Catalog manifest together, while the active `RecoveryPinV1` is a sibling
ephemeral record bound to the connected Oxia session. The identity-bearing
publication constructor is required for pin create/release; the existing
constructor remains catalog/publication-only and fails closed for those calls.

The callback validates the pin against the current publication record's
catalog projection on every CAS attempt. The shared store then validates
exact key, canonical value, non-null Oxia version, version-derived session
digest and singleton `IfRecordDoesNotExist`/`AsEphemeralRecord` semantics;
post-CAS catalog-generation reread and exact-version delete/absence reread
retain the response-loss boundary. Four publication-backend tests passed in
addition to the 17 recovery-catalog tests; the full Gradle check returned 0.

This closes reuse of the single-pin record contract across both local Oxia
authorities. It does not serialize the pin into `PublicationState` and does
not provide the missing cross-record Intent/Catalog/Pin transaction, Owner
session-loss authority, provider evidence, source ordering or GC lifecycle
authorization. The two opt-in real publication smoke methods were skipped
because `NEREUS_DELAY_OXIA_ENDPOINT` was unset.

## 2026-08-16 Oxia Control Operation session-bound CAS audit

Delay commit `cc8001b528bb9943a2f683c6ad14728c426cb8f2` adds the missing
per-operation session fence to `OxiaSyncControlOperationBackend`. The
`ClientHandle` constructor composes the existing connected Oxia session marker;
`SessionBoundRecordClient` verifies that marker before and after every record
read or CAS write. Expected `KeyAlreadyExistsException` and
`UnexpectedVersionIdException` remain ordinary CAS races only after the
post-operation marker check succeeds. A response-loss path whose marker has
changed cannot perform a valid exact reread and therefore fails closed rather
than returning CURRENT.

`OxiaSyncControlOperationBackendTest.sessionFenceRejectsACommittedWriteAfterTheMarkerChanges`
covers a write that is committed by the fake record service before the session
check fails; reopening through the unbound deterministic seam proves the value
was durable without treating the fenced caller as successful. The focused
deterministic suite passed 5 tests, the two real-service methods were skipped
because `NEREUS_DELAY_OXIA_ENDPOINT` was unset, and the full Gradle check
returned 0. The unbound constructor remains an explicit external/test seam.

This audit closes only the single-record Oxia I/O/session boundary. It does
not establish authenticated actor/scope authorization, source-ordered control
mutation routing, atomic target/state registration across records, automatic
session reconnection, production query routing, chaos evidence or V1 release
readiness.

## 2026-08-16 Oxia Control Target Registration session-bound CAS audit

Delay commit `50435a1364d2e8f7d823cc05faa18e4766f5cbd6` adds the
session-bound constructor to `OxiaSyncControlTargetRegistrationBackend`. Its
`SessionBoundRecordClient` checks the exact connected Oxia session marker before
and after every target-registration record read and `IfRecordDoesNotExist`
write. This covers `register`, `find` and the durable lookup used by mutation
validation. A target record committed before session loss is not exposed as a
successful registration when the post-write marker check or exact reread is
fenced.

`OxiaSyncControlTargetRegistrationBackendTest.sessionFenceRejectsACommittedRegistrationAfterTheMarkerChanges`
covers that boundary by committing the fake record, fencing the caller, and
then reopening through the explicit unbound deterministic seam to prove the
bytes are durable but the fenced operation did not return a registration
result. The focused deterministic suite passed 4 tests, the two real-service
methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset, and the
full Gradle check returned 0.

This audit closes only target-registration single-record session fencing. It
does not establish atomic Control Operation plus target registration,
actor/scope authorization, source-ordered mutation routing, automatic session
reconnection, production control-query routing, chaos evidence or V1 release
readiness.

## 2026-08-16 Oxia credential Profile catalog session-bound CAS audit

Delay commit `89020c97c29f99d98f7f3259ab7b27131644adcd` adds the
session-bound constructor to `OxiaSyncProfileCatalogBackend`. The private
`SessionBoundRecordClient` checks the exact connected Oxia session marker
before and after every Profile catalog read or version CAS write. This fences
generation-one publication, equivalent-secret rotation, protection-before-use
lease issuance, immutable binding/Head/Protection reads and response-loss
rereads with the same session boundary.

`OxiaSyncProfileCatalogBackendTest.sessionFenceRejectsACommittedPublicationAfterTheMarkerChanges`
commits the canonical Profile record in the fake service, fences the session
before the put response returns, asserts the backend fails, and then reopens
the exact Profile through the explicit unbound deterministic seam. Four
deterministic Profile tests passed; the real-service method was skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset, and the full Gradle check returned 0.

This audit closes only the single-record Profile authority's Oxia I/O/session
boundary. It does not establish secret-manager resolution, source-ordered
Profile publication, actor/target authorization, retained-generation GC,
cross-record Owner/Route/session transactions, automatic session reconnect,
provider rotation/quiescence, chaos evidence or V1 release readiness.

## 2026-08-16 Oxia Recovery Catalog session-bound CAS audit

Delay commit `f04f58d15588662b71be68809e1a11a627baf540` adds the
session-bound `ClientHandle` constructor to `OxiaSyncRecoveryCatalogBackend`.
Its private record wrapper checks the connected Oxia session marker before and
after every catalog `get` and version-CAS `put`. The original pin-store
construction still received the raw record client, so the pin's session
identity digest and ephemeral CAS were covered, but current-marker fencing of
pin `get`/put/delete was not proven by that commit.

`OxiaSyncRecoveryCatalogBackendTest.sessionFenceRejectsACommittedCatalogPublicationAfterTheMarkerChanges`
commits the catalog snapshot in the fake service, fences the session before
the put response returns, asserts the backend fails, and then reopens the exact
manifest through the explicit unbound deterministic seam. The deterministic
Recovery Catalog suite passed 18 tests, the three real-service methods were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset, and the full Gradle
check returned 0.

This audit closes catalog-record single-record I/O/session fencing only. It does
not establish an atomic Catalog/RecoveryPin/Upload-Intent transaction,
source-ordered activation, Owner/session recovery, immutable Object Store
publication, provider deletion, source/evidence replay, chaos evidence or V1
release readiness.

## 2026-08-16 Oxia Checkpoint Publication session-bound CAS audit

Delay commit `ffe0e5e15894ba377248068258444a1484bfb7f2` adds the
session-bound `ClientHandle` constructor to
`OxiaSyncCheckpointPublicationBackend`. Its canonical `/publication` record
now uses a private `SessionBoundRecordClient` that checks the connected Oxia
session marker before and after every publication-record read or version CAS
write. A committed combined Upload Intent/Catalog value whose response arrives
after marker loss therefore cannot be returned as a successful publication;
the explicit unbound constructors remain the narrow external/deterministic
seams.

`OxiaSyncCheckpointPublicationBackendTest.sessionFenceRejectsACommittedPublicationAfterTheMarkerChanges`
commits the canonical value in the fake service, fences the marker before the
put response returns, asserts failure, and reopens the exact manifest through
the unbound seam. The deterministic Publication suite passed 5 tests, the two
real-service methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset, and the full Gradle check returned 0.

This audit closes only the single-record publication I/O/session boundary. The
Recovery Pin remains a sibling ephemeral record and is not folded into the
publication snapshot; no Intent/Catalog/Pin cross-record transaction,
provider/source evidence, Owner/session recovery, chaos evidence or V1 release
readiness is claimed.

## 2026-08-16 Oxia Checkpoint Upload Intent session-bound CAS audit

Delay commit `0a1e6020` adds the session-bound `ClientHandle` constructor to
`OxiaSyncCheckpointUploadIntentBackend`. The independent `/intent` record
surface checks the connected Oxia session marker before and after every
upload-intent read and version-CAS write. If a PENDING_UPLOAD successor is
committed but the marker changes before the response or exact reread, the
backend fails closed instead of reporting a guessed create/publish/reaping
result; the unbound constructor remains the explicit deterministic/external
seam.

`OxiaSyncCheckpointUploadIntentBackendTest.sessionFenceRejectsACommittedIntentAfterTheMarkerChanges`
commits the fake intent, fences the session before the put response returns,
asserts failure and reopens the exact intent through the unbound seam. The
deterministic Upload Intent suite passed 4 tests, all three real-service
methods were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset, and the
full Gradle check returned 0.

This closes only independent single-record upload-intent session fencing. It
does not turn the separate intent, catalog and pin records into one
transaction, and it does not claim Owner/session recovery, provider evidence,
source ordering, chaos evidence or V1 release readiness.

## 2026-08-16 Oxia Worker assignment session-bound CAS audit

Delay commit `cca59a92df395c11cfdda23d24bb27a8b5269cca` strengthens the
handle-bound `OxiaSyncWorkerAssignmentBackend` path. Its private
`SessionBoundRecordClient` checks the connected Oxia marker before and after
every desired-assignment record read, version-CAS write and exact-version
withdrawal. A committed assignment whose marker changes before the response or
exact reread therefore fails closed rather than being exposed as a successful
publication or withdrawal.

`OxiaSyncWorkerAssignmentBackendTest.sessionFenceRejectsACommittedAssignmentAfterTheMarkerChanges`
commits the fake assignment, fences the session before the put response
returns, asserts failure and reopens the exact assignment through the unbound
seam. The deterministic Worker assignment suite passed 5 tests, the real
route-worker smoke method was skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset, and the full Gradle check returned 0.

This closes only desired-assignment single-record session fencing. It does not
establish an Assignment/Owner/Route transaction, placement authority,
automatic session recovery, source ordering, chaos evidence or V1 release
readiness.

## 2026-08-16 Oxia Owner Lease session-bound CAS audit

Delay commit `7a76a3af61ea16bceb81cc566462c078ca8de2a5` strengthens the
connected `OxiaSyncOwnerLeaseBackend` path. Its private
`SessionBoundRecordClient` checks the exact connected Oxia marker before and
after every owner-epoch read/version-CAS write and every ephemeral lease
read/version-CAS write/exact-version delete. A committed lease whose marker
changes before the response or exact reread therefore fails closed rather than
being exposed as a successful acquire, renewal, lifecycle transition or
release.

`OxiaSyncOwnerLeaseBackendTest.sessionFenceRejectsACommittedLeaseAfterTheMarkerChanges`
commits the fake lease, fences the session before the ephemeral put response
returns, asserts failure and reopens the exact lease through the unbound seam.
The deterministic Owner Lease suite passed 14 tests, the real owner-service
smoke method was skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset, and
the full Gradle check returned 0.

This audit closes only per-record owner epoch/lease session fencing. It does
not establish Assignment/Owner/Route transactionality, placement authority,
automatic session recovery, source ordering, raw chaos, failover or V1
release readiness.

## 2026-08-16 Oxia Route authority session-bound I/O fence audit

Delay commit `57e466786aea596cfdbd75020e48310415da0335` strengthens the
`OxiaRouteAuthoritySession` record/watch surface. Synchronous Route `get` and
`put`, notification registration and range-scan creation now check the exact
ephemeral marker before and after the delegated call. A private
`SessionBoundIterable` checks the marker around each lazy range iterator
`hasNext`, `next` and `remove`. A Route head that commits before marker loss
therefore fails closed when the head response is fenced instead of being
exposed as a successful publication.

`OxiaSignedRouteSnapshotProviderTest.sessionFenceRejectsACommittedRouteHeadAfterTheMarkerChanges`
commits the fake Route head, expires the marker before the head response
returns, asserts failure and rereads the exact head through the raw fake seam.
The deterministic Route provider/session suite passed 6 tests, four real Route
authority methods and one real Route-worker method were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset, and the full Gradle check returned 0.

This audit closes only per-operation Route session fencing and lazy range
protection. It does not establish event/head transactionality, automatic
reconnect, multi-node failover, placement/source ownership, raw chaos or V1
release readiness.

## 2026-08-16 Atomic checkpoint publication authority pairing fence audit

Delay commit `920197ad41aaa6f0b88871f5ddf631f6899a53d3` closes the reverse
constructor-wiring hole in `CheckpointPublicationCoordinator`. The coordinator
now rejects a split intent/catalog pair when either authority implements
`CheckpointAtomicPublicationAuthority`; the same object must own both sides of
the combined publication record. The previous one-sided check could allow an
atomic catalog paired with an independent intent backend and defer the failure
until after provider work.

`CheckpointUploadCoordinatorTest.rejectsMismatchedAtomicAuthorityRegardlessOfWhichSideDeclaresIt`
covers both atomic-on-intent and atomic-on-catalog mismatch directions. The
focused `CheckpointUploadCoordinatorTest` suite passed. This audit closes only
the constructor-time authority identity fence; the single-record publication
boundary remains subject to its existing provider, Owner/session, source,
chaos and release limitations, and no Intent/Catalog/Pin multi-record
transaction is claimed.

## 2026-08-16 Recovery Pin session-fenced client wiring correction audit

Delay commit `f0e45cbdf6eb30d730c6678e71c4c19d34e06072` corrects both Oxia
authority constructors: `OxiaSessionBoundRecoveryPinStore` now receives the
already session-wrapped catalog/publication `RecordClient`. Pin `get`,
`AsEphemeralRecord` `put` and exact-version `delete` therefore check the
connected marker before and after the operation, including response-loss
handling.

`OxiaSyncRecoveryCatalogBackendTest` and
`OxiaSyncCheckpointPublicationBackendTest` each add create and release tests.
The focused Recovery Catalog and Publication suites passed. These tests also
correct the scope of the earlier `f04f58d1` catalog receipt: that receipt did
not prove the pin I/O marker fence because the pin store still used the raw
client at that source revision. This correction closes only per-record pin
session fencing; no Catalog/Pin/Upload-Intent transaction, Owner/session
recovery, provider evidence, source ordering, chaos or V1 release readiness is
claimed.

## 2026-08-16 Oxia Route notification reconnect session fence audit

Delay commit `de203e4dc14de32746ce73da75381843152af922` closes the missing
session-boundary path in `OxiaRouteAuthoritySession.reconnectNotifications`.
The current Route marker is checked before replacement-client creation and
again immediately before and after replacement notification registration. If
the registration commits and the marker then changes, the replacement client
is closed, the previous client reference is restored and the call fails closed.

`OxiaSignedRouteSnapshotProviderTest.notificationReconnectRequiresTheCurrentSessionBeforeRegistration`
covers the pre-registration fence, and
`OxiaSignedRouteSnapshotProviderTest.notificationReconnectRejectsACommittedRegistrationAfterTheMarkerChanges`
covers the committed-registration/marker-loss boundary. The deterministic
Route provider/session suite passed 8 tests; four real Route authority methods
and one real Route-worker method were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. This audit closes only replacement
notification registration fencing. It does not establish event/head
transactionality, automatic reconnect, multi-node failover,
placement/source ownership, raw chaos or V1 release readiness.

## 2026-08-16 Oxia Route provider start retry after notification fence audit

Delay commit `d241246eefc284fea9719c8e162afa8e2a8e4828` fixes the provider
state transition after a notification registration commits and the connected
Route marker is then fenced. A started provider now returns immediately only
when its cache is healthy; a non-healthy started provider reuses explicit
`refresh()`, which reconnects the session, replaces the notification client
and rebuilds the signed Route cache.

`OxiaSignedRouteSnapshotProviderTest.startRetriesNotificationRegistrationAfterACommittedRegistrationIsFenced`
forces the post-registration marker loss, verifies the first start fails with
`WATCH_GAP`, then requires the next `start()` to restore `HEALTHY` state and a
replacement registration. The deterministic Route provider/session suite
passed 9 tests. This audit closes only the retry state transition; it does not
establish transparent automatic reconnect, event/head transactionality,
multi-node failover, placement/source ownership, raw chaos or V1 release
readiness.

## 2026-08-16 Oxia Route initial-refresh notification restoration audit

Delay commit `22780082d24e2011d44ead6ca62c38251a03633b` closes the provider
subscription gap after an incomplete first start. If the first Route replay
fails before the provider is marked started, a later `refresh()` now rebuilds
the cache and registers the initial notification callback before returning.
The started state is set before registration so a post-registration marker
fence is recovered through the existing replacement path on the next retry.

`OxiaSignedRouteSnapshotProviderTest.refreshAfterAnInitialRouteGapRestoresTheNotificationStream`
forces the missing-event failure, repairs the exact event/head pair and
requires revision 2 plus one notification registration after `refresh()`. The
deterministic Route provider/session suite passed 10 tests. This audit closes
only initial-refresh notification restoration; it does not establish
transparent automatic reconnect, event/head transactionality, multi-node
failover, placement/source ownership, raw chaos or V1 release readiness.

## 2026-08-16 fleet and Route resource close aggregation audit

Delay commit `eb47cb807ceb45d68a9f8db5f53ef3a7cc6ead4e` closes two local
teardown aggregation gaps. `WorkerShardFleetRuntime.close()` attempts every
admitted shard runtime after a close failure, retains the first failure and
attaches later failures as suppressed, while leaving the fleet retryable until
all shard closes succeed. `OxiaRouteAuthoritySession.close()` independently
attempts its authority and notification clients, so a failed authority-client
close no longer prevents the separate watch client from being closed.

`WorkerShardFleetRuntimeTest.closeAttemptsEveryShardAndRetainsTheFirstDrainFailure`
proves both shard close attempts and first/suppressed failure ordering.
`OxiaSignedRouteSnapshotProviderTest.sessionCloseAttemptsTheIndependentWatchClientAfterAuthorityCloseFails`
proves watch-client cleanup after authority close failure. The focused receipt
passed 2 fleet tests and 11 Route provider/session tests with zero
failures/skips/errors.

This closes only local close-attempt aggregation. Per-shard owner drain remains
mandatory; no automatic Oxia session retry, Route event/head transactionality,
placement/source ownership, raw chaos, failover or V1 release readiness is
claimed.

## 2026-08-16 Worker source close retry boundary audit

Delay commit `874fccb4fc521ad51b7954236ec5e37c1591e011` fixes the source-loop
teardown state transition. `WorkerSourceApplyLoop.close()` now marks the loop
closed only after the native `SourceRecordConsumer.close()` succeeds. If the
native close throws, the loop remains open for the exact owner-drain retry and
cannot be mistaken for a completed source teardown.

`SourceApplyCoordinatorTest.workerSourceLoopRetriesNativeCloseAfterAReleaseFailure`
forces the first native close to fail, verifies one attempted call, retries the
same loop close successfully and confirms later source turns are fenced. The
focused source/apply suite passed 8 tests with zero failures/skips/errors.

This closes only local source close retryability. Pending ACK and owner-drain
ordering remain mandatory; Broker reconnect/ACK durability, crash/chaos,
failover, production ownership and V1 release readiness are not claimed.

## 2026-08-16 Route client teardown retry boundary audit

Delay commit `9f24b2f38ba4f21962bebdaa2455d7f86ba0cd1b` separates the Route
close fence from close completion. `OxiaRouteAuthoritySession` rejects further
session I/O immediately after close is requested, but retries its authority and
notification client close calls until both succeed. The provider similarly
keeps its CLOSED health fence while retrying its owned notification executor
and Route client, aggregating failures without losing the later attempt.

`OxiaSignedRouteSnapshotProviderTest.sessionCloseAttemptsTheIndependentWatchClientAfterAuthorityCloseFails`
now proves a failed session close can be retried, and
`OxiaSignedRouteSnapshotProviderTest.routeProviderRetriesClientCloseAfterAReleaseFailure`
proves the provider's second close reaches the client. The deterministic Route
provider/session suite passed 12 tests with zero failures/skips/errors.

This closes only local Route teardown retryability. It does not establish
automatic session recovery, event/head transactionality, placement/source
ownership, raw chaos, failover or V1 release readiness.

## 2026-08-16 Direct SDK client teardown retry boundary audit

Delay commit `677026b3` separates the Direct SDK close fence from close
completion. `DefaultDelayClient.close()` fences new submissions immediately,
attempts the outbox, query client and optional transport registry even when an
earlier child close fails, and retains the first failure with later failures
suppressed. The client becomes terminally closed only after all owned child
resources close successfully, so a later explicit close can retry the same
teardown boundary.

`DefaultDelayClientTest.closeRetriesEveryChildAfterTheFirstCloseFailure`
forces the first outbox close to fail, verifies that all three children were
attempted, then requires the second client close to reach every child again.
The deterministic Direct SDK client suite passed 11 tests with zero
failures/skips/errors.

This closes only local Direct SDK teardown retryability. It does not establish
provider/session recovery, transport delivery, durable outbox authority,
crash/chaos, failover or V1 release readiness.

## 2026-08-16 Route connect prefix validation boundary audit

Delay commit `4da7bcf46b0ab9350adebf1f614590851a1fadd8` moves canonical
`keyPrefix` validation ahead of both `OxiaClientBuilder` calls in
`OxiaRouteAuthoritySession.connect()`. A malformed Route prefix therefore
fails before authority or notification client creation, and the constructor
receives the already validated canonical prefix when external client setup is
successful.

`OxiaRouteAuthoritySessionTest.connectRejectsAnInvalidKeyPrefixBeforeCreatingOxiaClients`
passes an invalid trailing-slash prefix with an unusable endpoint and proves
the deterministic `IllegalArgumentException` input fence without entering
client creation. The focused Route session construction suite passed 1 test
with zero failures/skips/errors.

This closes only local connect-input/resource-ordering validation. It does not
establish Oxia session recovery, Route transactionality, placement/source
ownership, raw chaos, failover or V1 release readiness.

## 2026-08-16 Worker monitor teardown retry boundary audit

Delay commit `2f7d9d667547380355a27517ea2c1e4941962693` repairs the monitor
side of the shared-resource cleanup invariant. Both
`WorkerRuntimeResourceMonitor` and `WorkerRocksDbUsageMonitor` set their close
fence before teardown but retain a separate completion state; cancellation and
executor shutdown are attempted independently, and a failed shutdown leaves a
later explicit close able to retry the executor.

`WorkerRuntimeResourceMonitorTest.closeRetriesExecutorShutdownAfterTheFirstFailure`
and
`WorkerRocksDbUsageMonitorTest.closeRetriesExecutorShutdownAfterTheFirstFailure`
force the first injected `shutdownNow()` call to fail and require the second
monitor close to invoke shutdown again. The deterministic monitor suites
passed 12 tests with zero failures/skips/errors.

This closes only local Worker monitor teardown retryability. It does not turn
a failed native process into safe recovery or provide production resource,
Owner/Oxia, raw chaos, failover or V1 release readiness.

## 2026-08-16 In-memory command transport registry teardown retry audit

Delay commit `0378e9a7585397e6f5e71a301f58c6d00835f2a0` repairs the local
registry teardown state. `InMemoryCommandTransportRegistry` fences new
registration and lookup immediately, attempts every snapshot entry, removes a
transport only after its close succeeds, and leaves failed entries available
for the next explicit close while retaining first/suppressed failure order.

`InMemoryCommandTransportRegistryTest.closeRetriesOnlyTheTransportThatFailedTheFirstTeardown`
forces one transport to fail once, verifies that a healthy sibling is not
closed again, and requires the second registry close to reach the failed
transport. The deterministic registry suite passed 1 test with zero
failures/skips/errors.

This closes only the local registry lifecycle. It does not establish
production Kafka/Pulsar client teardown, transport delivery, Broker failover,
raw chaos or V1 release readiness.

## 2026-08-16 Guarded Pulsar transport teardown aggregation audit

Delay commit `9d164037f9ba3832cd1f83846813b44de18967ab` closes the child-order
gap in `GuardedPulsarCommandTransport`. Its managed sender and native sender
are now attempted independently; the first failure remains primary with
later failures suppressed, and the existing outer retry gate can invoke the
transport close again after a partial release.

`GuardedTransportOwnershipTest.pulsarCloseAttemptsNativeSenderAfterManagedSenderFailure`
forces the managed sender to fail once, proves the native sender was still
attempted, then requires a second close to reach both senders. The
deterministic guarded transport suite passed 4 tests with zero
failures/skips/errors.

This closes only local Pulsar transport teardown aggregation. It does not
establish native or managed Broker delivery, client lifecycle authority,
failover, raw chaos or V1 release readiness.

## 2026-08-16 Owner connect prefix validation boundary audit

Delay commit `499e8439f2fe0f1b1c1114dbfd1bb7e55a06c43c` moves canonical
`keyPrefix` validation ahead of the `OxiaClientBuilder` call in
`OxiaSyncOwnerLeaseBackend.connect()`. The validated namespace, client
identifier and prefix are then reused for construction; malformed Owner
authority input cannot reach external client creation.

`OxiaSyncOwnerLeaseBackendTest.connectRejectsAnInvalidKeyPrefixBeforeCreatingAnOxiaClient`
passes an invalid trailing-slash prefix with an unusable endpoint and proves
the deterministic `IllegalArgumentException` input fence. The deterministic
Owner backend suite passed 15 tests with zero failures/skips/errors.

This closes only local Owner connect-input/resource-ordering validation. It
does not establish Owner/Oxia session recovery, lease authority, placement,
raw chaos, failover or V1 release readiness.

## 2026-08-16 Gateway admission lease release retry boundary audit

Delay commit `d5384b954e4d99ad291b2aea004910e1b1666ec8` fixes the local handle
state transition around durable Gateway admission release. A lease remains
open while its owner retries the exact CAS removal; it is marked closed only
after release returns successfully, so a failed release cannot strand an
active admission record behind a terminal in-memory handle.

`OxiaGatewayAdmissionControllerTest.leaseCloseRemainsRetryableAfterReleaseCasDoesNotConverge`
forces five release CAS failures, verifies the durable lease remains present,
then retries the same handle and requires the lease to disappear. The
deterministic Gateway admission suite passed 6 tests with zero
failures/skips/errors.

This closes only local durable admission-lease release retryability. It does
not establish distributed Gateway authority, session recovery, transport
delivery, failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway idempotency evidence monotonicity audit

Delay commit `b19f998ffe811d0a6dee1051491eae6c61131712` closes the durable
Gateway evidence-ordering gap. `GatewayIdempotencyRecordV1` verifies every
terminal outcome against the one stored managed/native prepared submission and
the callback's physical attempt ID. A byte-identical terminal replay returns
the existing record without a new Oxia put; different terminal evidence for a
terminal attempt is an integrity conflict and leaves the stored value intact.

The aggregate transition is monotonic across the full attempt history: the
first queued receipt remains selected, a queued attempt cannot be downgraded,
and any unresolved attempt keeps the aggregate uncertain. Retry admission
selects the highest unresolved attempt as the CAS precondition, so a newer
definitive non-queued attempt cannot make an older unresolved obligation
disappear. The Oxia and in-memory stores share the same record transition
rules.

The focused Oxia/idempotency and Gateway schedule suites passed 13 tests with
zero failures/skips/errors. The full `./gradlew check` passed 1532 tests with
24 skips and zero failures/errors. This closes local durable evidence
monotonicity and exact-replay behavior only; it does not establish distributed
Gateway authority, transport delivery, Broker failover, raw chaos or V1
release readiness.

## 2026-08-16 Gateway prepared-expiry fence and aggregate replay audit

Delay commit `66508783f5e8230ace8bae37ff04c28dfb353653` closes the expiry
ordering gap between the Gateway handler and durable idempotency record. Both
stores refuse to create an attempt when a `PREPARED` record has reached its
retention fence; the record remains `PREPARED` with no permit, attempt or
aggregate. The handler now asks the store for the current attempt state before
applying the request deadline, so a previously installed aggregate remains the
exact replay result after expiry.

The focused Gateway suites passed 16 tests with zero failures/skips/errors,
and the full `./gradlew check` passed 1535 tests with 24 skips and zero
failures/errors. This closes only local prepared-expiry and replay ordering;
it does not establish distributed Gateway authority, transport delivery,
Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway attempt projection integrity fence audit

Delay commit `52c6ed1c604a98b56668e510a3cf84ad364ec9cc` adds a fail-closed
projection boundary for the one-value Gateway idempotency record. A physical
attempt cannot be `STARTED` with terminal evidence or terminal without evidence;
record construction and decode reject non-source-ordered attempts, duplicate
physical/retry identities, phase/attempt disagreement and impossible aggregate
presence.

The deterministic idempotency suite passed 10 tests with zero
failures/skips/errors, and the full `./gradlew check` passed 1536 tests with
24 skips and zero failures/errors. This closes only local durable projection
integrity; it does not establish distributed Gateway authority, transport
delivery, Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway stored evidence binding audit

Delay commit `380e279725e9ac5d31f98ad49ee711cd15c5b25c` extends the Gateway
projection audit from lifecycle shape to semantic binding. On construction
and decode, every terminal attempt outcome is decoded and checked against the
stored prepared submission's managed/native branch, prepared command or native
reference, physical attempt identity and persisted lifecycle state. The
aggregate bytes are independently recomputed from the attempt history and
must match the stored aggregate exactly, so a canonical but foreign outcome
cannot be accepted as the record summary.

`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsOutcomeStateAndAggregateMismatches`
covers both state/outcome disagreement and a foreign aggregate. The
deterministic idempotency suite passed 11 tests with zero failures/skips/errors;
the full `./gradlew check` passed 1537 tests with 24 skips and zero
failures/errors. This closes only local stored-evidence binding and aggregate
integrity; it does not establish distributed Gateway authority, transport
delivery, Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway retry evidence hash binding audit

Delay commit `5e1bd9f6b3e2bcf24972e7b9ecdd78db49520734` closes the remaining
syntactic-only retry identity gap in the durable Gateway projection. For each
attempt carrying `retryRequestId/retryRequestHash`, construction and decode
recompute the contract hash from the record gateway key, that retry ID and an
earlier physical attempt ID. A canonical but foreign hash is therefore
rejected before the state machine consumes the record.

The validator intentionally accepts any earlier physical attempt as the hash
candidate because a later terminal callback can change the prior attempt from
UNCERTAIN to QUEUED/DEFINITELY_NOT_QUEUED after the retry was installed; the
stored projection has no transition-history field with which to reconstruct
that historical state. The deterministic idempotency suite passed 11 tests
with zero failures/skips/errors, and the full `./gradlew check` passed 1537
tests with 24 skips and zero failures/errors. This closes only local retry
evidence hash binding; it does not establish distributed Gateway authority,
transport delivery, Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway operation/prepared binding audit

Delay commit `f27800424a7cde3b8496b4fbbb4d4586cbeb07ca` closes the semantic
gap between the record operation tag and its frozen prepared bytes. The
record validator maps each managed `CommandType` to the matching
`GatewayOperationKindV1`, and rejects every other pairing. Native prepared
bytes are limited to the Schedule operation because the native AUTO_FAST
composition is produced only from Schedule preparation.

The regression in
`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsImpossibleAttemptAndRecordShapes`
rejects a Schedule prepared command stored under `CANCEL`. The focused
Gateway gRPC and schedule suites passed 10 tests with zero
failures/skips/errors; the full `./gradlew check` passed 1537 tests with 24
skips and zero failures/errors. This closes only local operation/prepared
semantic binding; it does not establish distributed Gateway authority,
transport delivery, Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway audit phase evidence audit

Delay commit `745da182c72af27dff09a8fb55db6cc15a4f20e3` closes the local
phase/digest union gap in `GatewayAuditEventV1`: a `COMPLETED` event must
carry `outcomeHash`, and `RECEIVED` or `FAILED` events must not carry one.
The constructor rejects both digest-only non-completed events and completed
events without an outcome digest before they are persisted by the Oxia audit
sink.

`OxiaGatewayAuditSinkTest.auditOutcomeDigestIsPresentOnlyForCompletedEvents`
passed in the focused 4-test audit suite with zero failures/skips/errors; the
full `./gradlew check` passed 1538 tests with 24 skips and zero
failures/errors. This closes only local audit phase/digest shape validation;
it does not establish distributed Gateway authority, transport delivery,
Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway active attempt tail fence audit

Delay commit `a1a85f99471743c48126943fad92fbb80ce6be34` closes the remaining
local lifecycle-shape gap in `GatewayIdempotencyRecordV1`. The durable
projection now rejects more than one `STARTED` attempt and rejects any
`STARTED` attempt that is not the final source-ordered entry, while retaining
the valid case where earlier terminal evidence coexists with one final
`STARTED` attempt.

The multiple-started and non-final-started regressions in
`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsImpossibleAttemptAndRecordShapes`
passed in the 11-test deterministic idempotency suite with zero
failures/skips/errors. The full `./gradlew check` passed 1538 tests with 24
skips and zero failures/errors. This closes only local active-attempt
projection integrity; it does not establish distributed Gateway authority,
transport delivery, Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Gateway attempt timing/retry shape audit

Delay commit `e0d5bc9761fea57103518819165d54eb60662b99` closes the local
physical-attempt field-shape gap. A valid attempt now has strictly positive
time distance from start to uncertainty and ownership expiry, keeps ownership
expiry at or before uncertainty, omits retry identity on attempt 1, and
requires retry identity on every later attempt.

The timing-bound and retry-presence regressions in
`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsImpossibleAttemptAndRecordShapes`
passed in the 11-test deterministic idempotency suite with zero
failures/skips/errors. The full `./gradlew check` passed 1538 tests with 24
skips and zero failures/errors. This closes only local physical-attempt
temporal/retry-shape validation; it does not establish distributed Gateway
authority, transport delivery, Broker failover, raw chaos or V1 release
readiness.

## 2026-08-16 Gateway queued aggregate tail fence audit

Delay commit `5b4d99e3` closes a local lifecycle gap left after the temporal
attempt checks. `GatewayIdempotencyRecordV1` now tracks whether a source-ordered
attempt has already reached `QUEUED` and rejects every later attempt. This
matches the Registry's sticky first-persisted-receipt rule while preserving the
valid case where an earlier unresolved uncertain attempt remains retryable even
after a newer definitive attempt.

The queued-tail regression in
`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsImpossibleAttemptAndRecordShapes`
passed in the 11-test deterministic idempotency suite with zero
failures/skips/errors. The full `./gradlew check` passed 1538 tests with 24
skips and zero failures/errors. This closes only local sticky-queued projection
integrity; it does not establish distributed Gateway authority, transport
delivery, Broker failover, raw chaos or V1 release readiness.

## 2026-08-16 Pulsar managed SEND evidence identity fence audit

Delay commit `4ed28c89f6cf9e20c12f1ee226752327f05f7953` closes an opt-in
real-client evidence mismatch that could otherwise produce a false positive
`PERSISTED` result. The SEND transport now binds
`GuardedSendSuccessEvidence.attestation()` to the expected guard, exact
physical topic and partition, then binds evidence ledger/entry/timestamp to
the returned native MessageId and broker timestamp. Contradictory evidence is
classified as `UNKNOWN/INTEGRITY_ERROR`.

The source-locked `compileRealPulsar` task and `runRealPulsarSmoke` passed; the
smoke records `persisted=PERSISTED, mismatch=UNKNOWN,
rejection=DEFINITIVELY_NOT_PERSISTED`. This is local opt-in binding evidence
only. It does not establish Broker rollout, multi-broker failover, source/ACK
integration, Worker production wiring or V1 release readiness.

## 2026-08-16 Kafka client metadata identity fence audit

Delay commit `1ab1d53fa4e14235fbb510035f2afaeea1ff3605` closes the matching
opt-in Kafka client gap. Direct produce now rejects a successful callback when
native `RecordMetadata.topic()` or `partition()` differs from the pinned
request. K2 validates the same metadata identity separately for the target and
receipt before committing the transaction result, so a contradictory callback
cannot become `PUBLISHED`.

The source-locked `compileRealKafka` task passed against the K1 client artifact
`kafka-clients-4.4.0-SNAPSHOT.jar`. No local Kafka broker was listening, so
broker-dependent smoke was not run. This is local opt-in identity evidence
only; it does not establish Broker rollout, multi-broker failover,
read-committed receipt authority, source/ACK integration, Worker production
wiring or V1 release readiness.

## 2026-08-16 recovered publish evidence identity fence audit

Delay commit `c6b6a5f9b52e8f5c358e047218ee606ea58aed3f` closes a false-positive
promotion path after destination response loss. K2 provider evidence is now
checked against the exact cursor channel, receipt offset, prepared hash,
target resource/partition, transactional identity and receipt bytes. Pulsar
provider evidence is checked against the exact target/partition, prepared
hash, producer identity hash, publish attempt and broker persistence time.
Owner identity alone is therefore insufficient to return `PUBLISHED`.

The focused evidence tests and source-locked K1/P1 compile tasks passed. No
Broker was listening for the external smoke, so this remains local
recovered-evidence binding only; live read-committed/Pulsar reread authority,
Broker rollout/failover, source/ACK integration, Worker production wiring and
V1 release readiness remain open. Follow-up commit
`df2d021fc7e8c5586b062870325efa71835b6d3b` retains the explicit K2 owner
check required by the cross-repository contract audit.

## 2026-08-16 Large-payload production-authority vertical live audit

Delay commit `ca9e9623b082f1c7df575e7316fb806226860ee5` adds the isolated
`e2e/run-large-payload-gateway-e2e.sh` and
`e2e/docker-compose.large-payload.yml` harness, plus the
`KafkaClientArtifactLargePayloadGatewaySmoke` source-set smoke. The
source-locked composition is Kafka K1 guarded source/command ingress, real
Oxia Route/Assignment/Owner authority, a real mTLS/RS256 Gateway network with
Oxia admission/idempotency/audit records, the Worker source-apply path, and a
versioned MinIO Object Store.

The source-bound live command passed:

```bash
./e2e/run-large-payload-gateway-e2e.sh
```

The receipt is locked to Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, Delay
`ca9e9623b082f1c7df575e7316fb806226860ee5`, and locked MinIO repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
(image ID `sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`).
The isolated Compose project was
`nereus-delay-large-payload-e2e-1786873193-51922`, with Kafka host ports
`25422,25423,25424`, Oxia `26422`, MinIO `27422`, and Gateway `28422`.

The receipt reported `activationOffset=0`, `barrierOffset=2`, a Prepare
`KafkaSourcePosition` at offset `2`, a Commit `KafkaSourcePosition` at offset
`3`, provider version
`5777ce46-f28c-4a68-ab76-71241ab5fd43`, and
`exactGatewayIdempotency=true`, followed by `BUILD SUCCESSFUL`. The positive
assertions cover source-ordered trust activation and guarded barrier Fetch,
Gateway Prepare, Worker `RESERVED`, receipt-bound upload-handle issuance, a
1 MiB + 4 KiB versioned-MinIO upload, provider-issued immutable attestation,
Gateway Commit, Worker `COMMITTED`/`SCHEDULED`, exact Object Store
proof/readback, byte-identical duplicate Prepare without an extra Kafka
record, final local checkpoint and exact Oxia Owner/Assignment release.

This is a live production-authority large-payload control/object path through
Worker Commit, not a full V1 release PASS. The source topic has one partition;
`InMemoryPayloadProofTrustSetCatalog` remains the local semantic resolver even
though the trust activation is source-ordered on real Kafka; and the final
checkpoint is local rather than Object Store checkpoint publication. The
separate Kafka/Pulsar destination egress receipts remain separate. Multi-shard
placement, Kafka response-loss/LSO/retention recovery, Pulsar multi-Broker
failover, raw chaos and V1 release readiness remain open.

## 2026-08-16 Large-payload Gateway-to-Kafka destination authority audit

Delay commit `33ff7a4b` closes the previously separate-harness boundary for the
positive large-payload destination path. Its opt-in source set composes the
real three-node Kafka source/destination/receipt topology, real Oxia
Route/Assignment/Owner and Gateway admission/idempotency/audit authorities,
mTLS plus RS256/JWT Gateway authentication, the source-ordered Worker graph,
and versioned MinIO. The Worker first applies the Gateway Prepare/Commit source
records with no guessed physical Lane incarnation, then binds the exact
durable typed Lane before due/Claim/Publish execution.

The source-bound command passed after the implementation commit:

```bash
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-payload-destination \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-full-v1-kafka-composition-compile \
  ./e2e/run-large-payload-gateway-e2e.sh
```

It used Delay `33ff7a4b`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:b0fcef7eb6f8350af6c22d333de889155acf4b1ec157887266568fc78beada0e`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The isolated Compose project was
`nereus-delay-large-payload-e2e-1786876116-89475` on Kafka
`25475,25476,25477`, Oxia `26475`, MinIO `27475`, and Gateway `28475`.

The live receipt proved `Admission source offset=4`, typed
`KAFKA_TRANSACTIONAL_RECEIPT` at receipt offset `0`, `PUBLISH_OUTCOME` at
source offset `5`, exact 1 MiB + 4 KiB payload readback, provider version
`483877e3-06e8-4b8d-81fa-5983b42a2cba`, and exact Gateway idempotency. The
source-ordered route is activation `0`, barrier `2`, Prepare `2`, Commit `3`;
the destination target and receipt are committed by the same real Kafka
transaction, and the final local checkpoint and Oxia Owner/Assignment release
also passed.

This is a bounded positive production-authority E2E, not V1 release approval.
It has one source, destination and receipt partition, retains the local
`InMemoryPayloadProofTrustSetCatalog` semantic resolver, and uses a local final
checkpoint. Kafka response-loss/LSO/retention recovery, Pulsar's combined
Gateway chain, multi-shard placement, raw crash/chaos and all release gates
remain OPEN.

## 2026-08-16 Pulsar multi-Broker Worker failover live audit

The checked-in `e2e/run-pulsar-multi-broker-failover-e2e.sh` was rerun with
real Oxia authority using:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-multi-live-gradle \
  ./e2e/run-pulsar-multi-broker-failover-e2e.sh
```

The source lock is Delay
`nereus/delay-full-implementation-v1@afbb2e30511b53b2f44adc620767685753acb48e`,
P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`,
client artifact SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, P1
image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Compose project
`nereus-delay-pulsar-multi-e2e-1786873557-57433` used broker-1 ports
`21933`/`21934`, broker-2 ports `21935`/`21936`, and Oxia port `16663`.

Broker-1 was stopped after the first Worker preparation. The same-topic
Worker resumed through broker-2, completed guarded SUBSCRIBE and source
apply, published through the provider-driven physical P1 path, ACKed the
source and released the final checkpoint and Oxia owner assignment. The
source-bound output was:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker source-applied physical publish passed: Admission source ledger=4/2, typed PULSAR_SEND_ACK target ledger/entry=5/0, Outcome source ledger=4/3, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=3/0, active apply ledger/entry=4/0, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker authority smoke passed: real Oxia session-bound lease
Pulsar multi-Broker failover E2E passed: same-topic guarded Worker resumed through broker-2 after broker-1 stop, applied the source record, completed provider-driven physical Publish, ACKed the source and released its final checkpoint and owner assignment.
```

This upgrades the evidence row from an implementation/design assertion to a
current bounded real two-Broker Worker failover receipt. It does not satisfy
the V1 release gate: the run still has one ZooKeeper and one BookKeeper, does
not include Gateway ingress or a combined Gateway-to-destination chain, and
does not cover raw network/proxy/process cuts, full Pulsar D3, Oxia
failover/partition, catalog-driven multi-shard placement, checkpoint
publication/quiescence or the remaining crash/response-loss matrix.

## 2026-08-16 Pulsar Large-payload Gateway-to-destination authority audit

Delay implementation commit
`accdc7074bfd38aed2cfd7c696a8c3ff62a972ba` adds the isolated
`PulsarClientArtifactLargePayloadGatewaySmoke` production-authority
composition and its `runRealPulsarLargePayloadGatewaySmoke` task. The source-
locked command was:

```bash
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The receipt locked Delay to `accdc7074bfd38aed2cfd7c696a8c3ff62a972ba`, P1
to `0a2536484cd3932801a98dc88ff112b2df88a1c7`, the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image to
`sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO to
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The isolated Compose project was
`nereus-delay-pulsar-large-e2e-1786879186-27914` with Pulsar service/admin
ports `29114/29115`, broker-2 `29116/29117`, Oxia `29124`, MinIO `29125` and
Gateway `29126`.

The live output was:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=2/4, typed PULSAR_SEND_ACK target ledger/entry=3/0, Outcome source ledger=2/5, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
BUILD SUCCESSFUL
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed
```

This closes the positive combined chain through real Pulsar, Oxia
Route/Assignment/Owner, Gateway mTLS/JWT, Worker source apply/ACK, versioned
MinIO upload/attestation/readback, due/Claim/Publish Admission/
`PUBLISHING`, typed `PULSAR_SEND_ACK`, source `PUBLISH_OUTCOME`, `PUBLISHED`,
exact 1 MiB + 4 KiB destination readback and final local checkpoint/Owner
release. It also proves exact duplicate Gateway Prepare bytes and a six-record
source log, so the idempotent retry did not append a second command.

The audit remains bounded: one physical source partition, one ZooKeeper and
one BookKeeper; no combined Gateway-plus-multi-Broker failover cut, no
multi-shard placement, no raw crash/network/proxy/process chaos, no Kafka
response-loss/LSO/retention recovery, no Object Store checkpoint publication
and no V1 release approval.

## 2026-08-16 Kafka source Fetch response-loss audit

Delay commit `8f1116abad2bd77e2f384c04411dabaeb70b4f72` adds the focused
`KafkaClientArtifactFetchResponseLossSmoke` and the source-bound E2E mode:

```bash
NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-gradle \
  bash e2e/run-kafka-real-client-e2e.sh
```

The receipt locked Kafka to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:b0fcef7eb6f8350af6c22d333de889155acf4b1ec157887266568fc78beada0e`,
and Delay `8f1116abad2bd77e2f384c04411dabaeb70b4f72`. Compose project
`nereus-delay-kafka-e2e-1786879840-36136` used ports `19228,19229,19230`.

The live output was:

```text
Kafka source Fetch response-loss smoke passed: responseDiscardedAfterFetch=true, replayOffset=0, secondOffset=1, fetchLso=2, committedAfterReplay=2
BUILD SUCCESSFUL
Kafka source Fetch response-loss E2E passed: real read_committed Fetch v13 response was discarded before ACK, exact source replay and LSO coverage were recovered.
```

This is positive evidence for a real three-Broker KRaft Fetch/LSO/source ACK
cut. It proves that a client-side loss after the real Fetch does not advance
the source cursor: the same group replayed the exact offset-0 command, then
ACKed offsets 0 and 1 and committed group offset 2. It does not prove raw
socket loss, consumer-coordinator or process/Broker crash recovery,
multi-shard placement, checkpoint publication, chaos or V1 release approval.

## 2026-08-16 Kafka source retention-floor audit

Delay commit `d8dc5f45` adds `KafkaClientArtifactRetentionFloorSmoke` and the
source-bound mode:

```bash
NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-retention-floor-gradle-2 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The live receipt used K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Delay `d8dc5f45`, and Compose project
`nereus-delay-kafka-e2e-1786880647-45643` on `19235,19236,19237`.

The output was:

```text
Kafka source retention-floor smoke passed: oldOffset=0, retentionFloor=20, endOffset=21, staleOffsetRejected=true, floorFetchOffset=20, fetchLso=21
BUILD SUCCESSFUL
Kafka source retention-floor E2E passed: real Broker retention advanced the earliest offset, stale source offset was rejected, and the current floor remained readable through guarded Fetch v13 with LSO.
```

This closes the bounded retention-floor recovery obligation: real Broker
retention removed the old source history, the guarded client failed closed on
offset `0` with typed `ConsumerResourceGuardException`/`OFFSET_OUT_OF_RANGE`,
and the current floor remained readable with an LSO covering the tail. It is a
controlled retention test, not raw network loss, disk exhaustion,
consumer-coordinator/process/Broker crash recovery, multi-shard placement,
checkpoint publication, chaos or V1 release approval.

## 2026-08-16 Kafka source process-crash recovery audit

Delay commit
`2bcaff5e0c0b15b819cbc614c166c47e19571be3` adds
`KafkaClientArtifactProcessCrashRecoverySmoke` and the source-bound mode:

```bash
NEREUS_DELAY_KAFKA_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-process-crash-e2e-receipt \
  bash e2e/run-kafka-real-client-e2e.sh
```

The live receipt used K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Delay `2bcaff5e0c0b15b819cbc614c166c47e19571be3`, and Compose project
`nereus-delay-kafka-e2e-1786881618-58469` on `19561,19562,19563`.

The output was:

```text
Kafka source process-crash cut reached: fetchedOffsets=0,1, fetchLso=2, responseAcked=false, consumerClosed=false
Kafka source process-crash recovery smoke passed: crashExit=86, replayOffset=0, secondOffset=1, committedAfterRecovery=2
BUILD SUCCESSFUL in 5s
Kafka source process-crash recovery E2E passed: the crashed JVM fetched exact guarded records without ACK, and a fresh same-group process replayed offsets 0 and 1 before committing offset 2.
```

This is positive evidence for the source cursor's bounded fresh-process
recovery rule: the crash cut occurs after a real guarded Fetch response but
before ACK/close, so the same group must replay the exact records and only
then commit offset `2`. It does not promote the result to raw socket loss,
consumer-coordinator or Broker crash recovery, Worker apply/publish crash
coverage, multi-shard placement, checkpoint publication, chaos completion or
V1 release approval.

## Final gate

设计审计通过不代表实现发布通过。实现只有在上述 artifact matrix 和主设计 §23.5 十项 release gate 全部完成后才可宣称 V1 release-ready；缺少数值、binary、benchmark 或 chaos evidence 的状态是“实现证据未完成”，不是“设计可自行解释”。
