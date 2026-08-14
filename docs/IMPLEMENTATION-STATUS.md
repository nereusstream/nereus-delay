# V1 Implementation Status

Spec revision: `V1-FROZEN-2026-08-13`

This file records implementation evidence. It does not relax or replace the
normative requirements in [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md),
the [`V1 Protocol Registry`](V1-PROTOCOL-REGISTRY.md), or the Accepted ADRs.
An unchecked item is not an implementation permission; it is a release blocker.

The `V1-FROZEN-2026-08-13` revision accepts ADR 0043/0044 and the code-level
[`Direct SDK / Delay Gateway / Guarded Transport design`](V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md).
The repository is still a single Gradle module. It now contains a local
`DelaySemanticCore`, signed Route value/verifier plus Oxia event/head-CAS
composition, Direct SDK facade, transport ownership/coordinator seam and an
in-memory Gateway Schedule conformance composition. These are local
implementation evidence, not claims of activation-barrier/session-fenced real
Oxia authority, generated Gateway descriptors, real Broker transport, or
production/release readiness. The isolated Kafka and Pulsar
upstream worktrees recorded below remain separately owned implementation
evidence; their patches are not copied into this repository.

## 2026-08-14 D1 local semantic-core slice

Delay worktree commit `532f8ad5b0a087d272c3f93e37c0b28c81576f96` on
`nereus/delay-full-implementation-v1` adds the first locally executable D1
seam: canonical signed `RouteSnapshotV1` resources/policies, Ed25519
digest/signature verification, UUIDv7-backed independent Message/Command
identities, exact `ROUTING_HASH_V1`, and a zero-I/O `DefaultDelaySemanticCore`
for managed schedule, large-payload preparation, cancel, reschedule and
historical-route preparation. The AUTO_FAST type seam is present, but it only
accepts an already verified local native-preparation snapshot; it does not
create or resolve Broker credentials or Route authority.

Evidence from the same worktree and independent Gradle user home:

```text
GRADLE_USER_HOME=/tmp/nereus-delay-full-gradle \
  ./gradlew check --no-daemon --console=plain
```

The gate passed, including `checkDocumentation`, `checkstyleMain`, the full
local test task, and the five opt-in real-Oxia methods remained skipped because
no endpoint was configured. Focused coverage is
`RouteSnapshotV1Test` and `DefaultDelaySemanticCoreTest`; it proves canonical
round trips, tamper rejection, tenant/lifecycle fencing, exact physical
Pulsar partition identity, byte-equivalent managed preparation, deterministic
routing and no transport call from preparation.

This remains local D1 evidence. The Delay worktree now also contains a local
Oxia event-stream/head-CAS Route authority composition, but activation-barrier
publication, session fencing, real credential/native eligibility authority,
production transport artifacts, Worker integration and real-Broker evidence
remain open.

## 2026-08-14 D1/D4/D5 local composition slice

Delay worktree commits `402b27fa0dced95c2312bfedc0678af03463f2d5`,
`67ef3de3ab6f69ae992c3ccb70c7cb65cad47613`,
`c42405ce6c69aef8ae0f8a9a63158c917410309f` and
`62a9438967112f96e65b8daa7b2b86d52a103b10`,
`e276bec3ffff7f5015367bed55f5b8d63c080e21` and
`69d89839e4e80326e5317a4f5066667e270a7136` on
`nereus/delay-full-implementation-v1` adds the first shared post-preparation
composition. `DefaultSubmissionCoordinator` resolves an exact historical
Route-bound plan, performs one exact projector/transport lookup, transfers a
non-serializable one-shot `TransportOwnershipPermit` immediately before the
guarded client call, and maps persisted/definite/unknown results into the
existing NDR1 managed/native union. `DefaultDelayClient` exposes explicit
tenant, semantic-core, coordinator, query, admission and outbox dependencies;
legacy production entry points fail closed instead of accepting caller-chosen
native authority.

The same slice adds `LocalTransportOwnershipPermit`,
`GatewayAttemptOwnershipPermit`, exact Kafka/Pulsar transport keys and guarded
bridges, strict `ProductionKafkaProduceTransport`/
`ProductionPulsarSendTransport` configuration seams, and an in-memory Gateway
Schedule idempotency record/service. Gateway canonical request hashing,
prepared bytes before ownership, single-attempt CAS, body conflict, aggregate
outcome, explicit uncertain retry CAS and the source-only gRPC proto are covered by
`GatewayScheduleServiceTest`; one-shot transfer and pre-ownership rejection
are covered by `GuardedTransportOwnershipTest`.

The Gateway follow-up adds the shared `GatewayIdempotencyStore`, strict
`GatewayPhysicalAttemptV1`/`GatewayIdempotencyRecordV1` decoders and
`OxiaGatewayIdempotencyStore`. Each Oxia transition uses one version-CAS
record; response-loss rereads may return the exact current aggregate but never
recreate an ownership permit. `OxiaGatewayIdempotencyStoreTest` covers reopen,
canonical record round-trip and a CAS-success/response-loss retry.

The follow-up `InMemorySignedRouteSnapshotProvider` and
`OxiaSignedRouteSnapshotProvider` supply local signed-snapshot authority/cache
conformance. `OxiaSignedRouteSnapshotPublisher` writes immutable canonical
events and advances an Oxia head with version CAS; the provider rebuilds from
that head, re-verifies Ed25519/digest bytes, applies contiguous revisions,
refreshes from Oxia notifications, freezes on gaps/signature errors and
quarantines same-incarnation immutable drift. Exact historical reads remain
tenant-scoped. This is an Oxia client composition and deterministic fake-client
evidence, not yet an activation-barrier/session-fenced real-service gate.

Evidence from this commit and its independent Gradle user home:

```text
GRADLE_USER_HOME=/tmp/nereus-delay-full-gradle \
  ./gradlew check --no-daemon --console=plain
```

The command passed, including `checkDocumentation`, the full local test task
and main Checkstyle at branch SHA
`2e0109c6da808f0681de75b137e531620c2ed6a7`; the Gateway-CAS focused tests and
Checkstyle pass at `e276bec3`, and the operation registry compiles at
`69d89839`. The Route slice's focused provider/publisher tests and Checkstyle
pass at `62a94389`. The five
real-Oxia methods remained skipped because no endpoint was configured. The
Delay worktree has no Docker compose or Broker
lifecycle harness; the Kafka/Pulsar compose files belong to their upstream test
suites and contain no Nereus guarded integration. This slice does not claim
generated gRPC descriptors,
Gateway HA/transactional durability beyond the single-record CAS composition,
late authenticated evidence/aggregate promotion,
activation-barrier or session-fenced real Oxia Route authority, Kafka/Pulsar client artifact integration, Worker ACK-after-sync
evidence, Docker lifecycle cuts or any real-Broker PASS.

## 2026-08-14 K1 isolated Kafka guarded-client slice

The isolated Kafka worktree now contains the first K1 client implementation at
`d1810fa3466e1378a33c5c6327c7f401cec03d07` on
`nereus/delay-guarded-producer-v1`, based directly on the locked Kafka
`trunk@c300006a7705c240642db6950b5a95fec982bfc5`. The implementation is
generic Kafka client code and does not import Nereus types. It adds the
`GuardedProducer`/`ProducerResourceGuard` API, non-transactional guarded
`sendGuarded`, exact expected TopicId Produce v13 handling, guard-separated
accumulator batches, retry/split guard retention, request/response/child/value
SHA-256 evidence, typed response failures, the K1 `UNKNOWN_TOPIC_ID` definitive
allowlist and ambiguity fencing.

Focused evidence from that worktree uses its independent Gradle user home:

```text
GRADLE_USER_HOME=/tmp/nereus-kafka-delay-gradle \
  ./gradlew :clients:test \
    --tests org.apache.kafka.clients.producer.GuardedProducerApiTest \
    --tests org.apache.kafka.clients.producer.KafkaProducerGuardedPreflightTest \
    --tests org.apache.kafka.clients.producer.internals.GuardedSenderTest \
    --no-daemon --console=plain
```

The API/preflight/guarded-Sender focused tests pass, including public future
completion, v13/TopicId binding, guarded-versus-ordinary batch separation,
leader retry, disconnect ambiguity, definitive `UNKNOWN_TOPIC_ID` and
non-allowlisted rejection. Existing `ProducerBatchTest`, `RecordAccumulatorTest`
and `SenderTest` regressions also pass in the same worktree. This is isolated
client/mock evidence only: the K1 completion gate still lacks the real Kafka
delete/recreate and leader-failover cuts, artifact/source digest capture, and
the Nereus D2 transport. No Kafka patch was copied into this Delay repository,
and no real-broker or production promotion claim is made.

The implementation blueprint was checked against Kafka
`trunk@c300006a7705c240642db6950b5a95fec982bfc5` and Pulsar
`5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551`. The independent branches
are `nereus/delay-guarded-producer-v1` from Kafka `trunk` and
`nereus/delay-resource-guard-v1` from Pulsar `5.0.0-M1`; neither patch is copied
into this Delay repository. Their source locks, binary digests, rollout and
real-Broker cuts remain release blockers; a design ACCEPTED label is not
implementation PASS.

## 2026-08-14 P1 isolated Pulsar guarded-client slice

The isolated Pulsar worktree now contains two independently reviewable commits
on `nereus/delay-resource-guard-v1`, based directly on the locked
`5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551`:

```text
19c97bf836d521f0e6103c542819723e70ccdbab  Add Pulsar v22 topic resource guard contract
be226fe6c88634e9a94ba5c6a0f5859bc510cb66  Enforce Pulsar topic resource guards
```

The first commit adds ProtocolVersion v22, ResourceIncarnationMismatch=26,
the guarded producer/success/receipt wire fields, immutable public guard and
evidence values, the backward-compatible `ProducerBuilder.resourceGuard`
seam, and protocol/API tests. The second adds strict managed-ledger property
validation, the INVALID/VALID atomic topic guard view, a resource-controller-only
ordered property update path, exact create and pre-admission SEND checks,
broker entry timestamp receipt echo, connection-generation/identity checks,
typed success/error evidence hashes, and fail-closed old-peer behavior.

Post-commit evidence was run with the worktree's independent Gradle user home:

```text
GRADLE_USER_HOME=/tmp/nereus-pulsar-delay-gradle \
  ./gradlew :pulsar-common:test \
    --tests org.apache.pulsar.common.protocol.CommandsTopicResourceGuardTest \
    --tests org.apache.pulsar.common.protocol.TopicResourceGuardApiTest \
    :pulsar-broker:test \
    --tests org.apache.pulsar.broker.service.ValidatedTopicResourceGuardTest \
    :pulsar-client-api:checkstyleMain \
    :pulsar-client-original:checkstyleMain \
    :pulsar-common:checkstyleMain \
    :pulsar-broker:checkstyleMain \
    :pulsar-common:checkstyleTest \
    :pulsar-broker:checkstyleTest \
    --no-daemon --console=plain -PtestRetryCount=0
```

This command passed at `be226fe6c8`; the cross-module compile
`:pulsar-client-original:compileJava :pulsar-broker:compileJava` also passed.
The focused result is upstream module/mock evidence only. Real Pulsar
delete/recreate, unload, failover, old-broker proxy compatibility, artifact
digest capture, Docker lifecycle and the D3 Nereus transport are still OPEN.
The `pulsar-client-original` full test source was not used as a gate because
the checkout has unrelated pre-existing generated-test compilation failures;
no full client or real-broker PASS is claimed.

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
real-Oxia methods were skipped because the endpoint was unset. The remaining
incomplete rows below require cross-record Oxia
transactions, Broker transports, provider authority, or release-scale
evidence.

After `fe7d484`, the local `WorkClass` registry now includes the independent
`CHECKPOINT` class required by the main design, ADR 0032 and the scheduler
configuration. `CheckpointScheduler` remains the process-local due-time and
exact-claim schedule; the `CHECKPOINT` class is the bounded execution queue
and turn/resource boundary, so scheduling a due shard cannot bypass the same
queue, byte/record caps or shared-minimum protection as other Worker work.

After `5f90f38`, `GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle
./gradlew clean check --rerun-tasks --console=plain` completed successfully:
1210 tests ran with zero failures/errors; the five opt-in real-Oxia methods
were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. The new
`WorkClassSchedulerTest.checkpointHasItsOwnBoundedWorkClassQueue` is included
in this full gate; the skipped external methods remain release evidence gaps.

After `0081e9d`, the five opt-in real-Oxia smoke methods were rerun against a
temporary standalone service built from Oxia source commit
`37a17bef17202d5fd6e23282da5fd26d94865484`:
```text
NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:6648 \
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle \
./gradlew test --tests io.nereusstream.delay.ownership.OxiaRealServiceSmokeTest \
  --tests io.nereusstream.delay.ownership.OxiaRealControlAuthoritySmokeTest \
  --tests io.nereusstream.delay.store.OxiaRealRecoveryAuthoritySmokeTest \
  --console=plain
```
All 5 test cases completed with zero failures, errors or skips. The service
used only temporary local data and was stopped after the run; this is current
single-record Oxia smoke evidence, not cross-record transaction or production
authority evidence.

After `9d37ad9`, `WorkClassDispatcher` provides the local production
composition boundary for the eight frozen classes. It rejects a handler map
with a missing or extra class before accepting work, routes each selected task
through the existing bounded `WorkClassEventLoop`, and preserves its exact
lease release and callback-failure semantics. `WorkClassDispatcherTest`
covers complete-class validation, `CHECKPOINT` dispatch and failure cleanup;
actual shard-specific handlers, dynamic WriteBatch attribution and external
Worker authority remain release gates.

After `2f78bd9`, the full `GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle
./gradlew clean check --rerun-tasks --console=plain` gate completed with 1213
reported test cases, zero failures/errors, and five skipped real-Oxia methods
because the endpoint was unset. This includes the dispatcher regression and
the complete repository check; skipped external evidence remains a release
gap.

After `da9a8ac`, mixed Source Log replay now constructs and validates the
typed `SourceReplayOutcome` before advancing the caller-owned cursor or the
in-memory `lastCatchupPosition`. A malformed post-WriteBatch command or
System Mutation projection fences the local Owner instead of leaving a
position projection ahead of the continuity cursor. The focused
`OwnerLeaseTest` and `OwnerRecoveryCoordinatorTest` replay/recovery suites and
`checkstyleMain` passed; this remains local fail-closed evidence and does not
replace production source-adapter continuity or Oxia lease/session authority.

After `9405f7c`, the type-specific bounded replay paths apply the same
post-WriteBatch physical-position projection used by mixed replay. A duplicate
Command or System Mutation therefore returns a result anchored to the current
physical Source Position while the durable logical result remains anchored at
its first application. The projection is validated before the caller-owned
cursor and `lastCatchupPosition` advance; malformed projection fences the
Owner. `OwnerLeaseTest` covers both duplicate branches and verifies the
first-position durable result. This is local replay evidence only; it does not
replace Broker source continuity or Oxia lease/session authority.

After `6bc3135`, `OwnedDelayShard` fences on unexpected `RuntimeException` as
well as native `RocksDbWriteFailure`/`Error` across active apply and all three
bounded replay paths. Deterministic business rejections still arrive as typed
`CommandResult`/`SystemMutationResult`; an untyped runtime failure is treated
as an unproven local projection or commit boundary and cannot leave the Owner
in `ACTIVE_FOR_COMMANDS` or `CATCHING_UP`. `OwnerLeaseTest` proves a regressed
source apply fences the Owner and leaves the rejected message unapplied. This
is local fail-closed evidence only; it does not replace production Broker or
Oxia authority.

After `e531eb8`, `WorkClassEventLoop` treats fatal `Error` as part of the same
cleanup boundary as `RuntimeException`. A fatal borrowed-hold/clock check or
lease close no longer aborts the cleanup loop: every lease is attempted, the
active Turn is cleared and marked closed, the first failure remains primary,
and later cleanup failures are attached as suppressed evidence. The focused
`WorkClassEventLoopTest.fatalHoldCheckStillReleasesEveryLeaseAndClosesTheTurn`
regression proves that a fatal hold check leaves zero active leases and permits
the next bounded poll. This is process-local resource cleanup evidence; dynamic
RocksDB WriteBatch attribution, checkpoint/compaction I/O authority and
production Worker wiring remain release gates.

After `e297176`, the active-Turn observation is a volatile lock-free read.
`poll()` no longer holds the event-loop monitor while trying to enter the Turn
monitor, which was the reverse of `Turn.close()` clearing `activeTurn` while
holding the Turn monitor. The deterministic
`WorkClassEventLoopTest.concurrentCloseAndPollDoNotInvertEventLoopAndTurnLocks`
blocks close inside its hold check, starts a concurrent poll, and proves the
poll immediately rejects the still-open Turn before close is released; close
then completes and releases the lease. This closes a process-local deadlock and
overlapping-turn path only; production Worker handler/IO authority remains a
release gate.

After `c44368c`, a normal handler `RuntimeException` no longer discards later
tasks that were already selected and removed for the same bounded Turn.
`WorkClassEventLoop.runTurn` records the first handler failure, continues the
remaining callbacks while every borrowed-hold check remains valid, closes all
leases, then rethrows the original failure with later failures suppressed.
Fatal `Error` and hold-boundary failures still stop callback execution and take
the cleanup path. `WorkClassDispatcherTest.handlerRuntimeFailureDoesNotDropLaterTasksAlreadySelectedForTheTurn`
proves a two-task Turn invokes the second handler after the first fails and
leaves the queue empty without masking the first failure. This closes a local
selected-task loss/isolation path; durable production handler identities and
Worker IO authority remain release gates.

After `70d65c2`, fatal handler and borrowed-hold failures no longer discard the
exact suffix of a bounded Turn whose handlers were never invoked. The event
loop returns only that unstarted suffix to the fronts of its class queues in
original selection order; the task whose handler may already have side effects
is not requeued. While a Turn is active, its selected record/byte capacity is
reserved so concurrent offers cannot consume the space required by that safe
requeue. `WorkClassDispatcherTest.fatalHandlerFailureRequeuesOnlyTrailingTasksThatWereNeverStarted`,
`WorkClassEventLoopTest.holdFailureBeforeTheFirstHandlerRequeuesTheWholeUnstartedTurn`
and `activeTurnReservesEnoughQueueCapacityForUnstartedTasks` cover the three
boundaries. This is process-local task retention evidence; production durable
handler identities and Worker IO authority remain release gates.

After `9663927`, the `CheckpointUploadCoordinator` no longer trusts an exact
`PUBLISHED` successor merely because it appeared on the second intent reread
after the Worker upload slot was acquired. It reapplies the bounded resource
validation and binds lineage, checkpoint, Object Store Profile, manifest
length and manifest SHA-256 to the canonical manifest before returning without
provider I/O. `CheckpointUploadCoordinatorTest.rereadAfterUploadSlotRejectsPublishedResourceThatDoesNotBindTheManifest`
proves a concurrent publication with a mismatched manifest length fails closed
and does not invoke the provider. This is a local publication-race integrity
fence; it does not provide the production Oxia multi-record transaction or
Object Store authority.

After `5175a68`, the checkpoint execution failure/completion combination has a
direct regression. If the execution body fails and the completion clock or
exact-claim completion also fails, the execution error remains primary, the
completion error is suppressed, and the scheduler retains the in-flight claim.
`CheckpointExecutionCoordinatorTest.executionFailureRemainsPrimaryWhenClaimCompletionAlsoFails`
also proves that the exact claim can be completed later with a valid timestamp.
This is local scheduler consistency evidence, not durable production scheduling
or external checkpoint authority.

After `b4ba86e`, `WorkClassRuntimeConfig` is the required construction boundary
for the local eight-class scheduler/resource runtime. It has no unverified
defaults: all classes, class-delay/borrowed-hold bounds and shared record/byte
capacity must be supplied together. Only `LEASE_FENCE` may be preemptive; the
six correctness/progress classes require nonzero record and byte minima; the
checked aggregate must fit the shared pool; and per-turn limits must fit each
queue. The same injected monotonic clock constructs both scheduler and resource
pool. `WorkClassRuntimeConfigTest` covers all-eight-class execution, missing or
misclassified policies, minima oversubscription and invalid queue/turn bounds.
Actual shard-specific handlers, dynamic WriteBatch attribution and release
benchmark values remain open.

After `bb09b2c`, `WorkClassExecutionRegistry` binds every accepted queue
identity to one exact synchronous action before admission. Duplicate
class/task identities fail closed; queue rejection rolls the registration
back; selection moves the action from `QUEUED` to `RUNNING`; success removes
it; and a started `RuntimeException` or fatal `Error` retains the exact action
as `FAILED`. Only an explicit retry carrying the same complete
`WorkClassTask`, including its byte charge, can requeue that action. Fatal
turn termination leaves never-started trailing actions `QUEUED` while the
event loop restores their exact tasks. `WorkClassExecutionRegistryTest`
covers all eight successful classes, ordinary failure followed by exact
retry, fatal suffix retention and admission rollback/identity drift. This is
a process-local execution projection only; restart rediscovery, actual shard
handlers and dynamic WriteBatch/IO authority remain open release gates.

After `e20409d` and `d595324`, a scheduled checkpoint can no longer reach the
local physical executor without an explicit `CHECKPOINT` work-class action.
`CheckpointWorkClassExecutor` preflights the exact scheduler claim and pending
intent before queue admission, performs no filesystem/provider I/O on queue
rejection, and repeats the same claim/Store/intent fence when the bounded turn
runs. After `34dac408`, `ExecutionRequest` no longer accepts caller-reported
`workClassBytes`: a canonical value identity binds the exact Shard, claim due
time, normalized absolute checkpoint directory, complete pending intent and
upload time. The task ID is its domain-separated hash and its exact length is
the static queue charge; negative upload time is rejected during request
construction. The direct preflight/physical execution methods are package-private, so
cross-package Worker composition cannot bypass this public entrypoint. Ordinary
attempt failures are captured in the submission outcome because
`CheckpointExecutionCoordinator` already owns exact-claim completion or
retention; this avoids creating a second generic `FAILED` retry authority. A
later physical retry requires the next exact scheduler claim, while fatal
`Error` remains visible to the WorkClass fatal-stop path. The regression
fills the `CHECKPOINT` queue, proves rejection leaves the claim and filesystem
unchanged, captures an ordinary failed attempt with no stale action, then
executes the next exact claim through the bounded turn. This closes the local
checkpoint-class wiring only; Owner Lease/session, external Object Store/Oxia
authority, the other shard-specific work classes and dynamic WriteBatch/IO
attribution remain release gates. The derived static request charge is not
evidence for actual checkpoint-file or upload-byte I/O charging.

After `6921ad8`, active Client Command and signed System Mutation records have
one concrete `SOURCE_APPLY` entrypoint. `SourceApplyWorkClassExecutor` hashes
the exact canonical Source Position and NDL1 frame into the task identity and
derives its byte charge from their checked exact length, then preflights the
strict assignment/activation/guard identity before queue admission. The
bounded action rereads the execution-time Owner Lease/session and repeats the
source fence before the synchronous Shard mutation. Queue rejection leaves the
Store, lease and source record unchanged. Ordinary lease/apply failure is
returned as a source-owned outcome and removes the process action, because the
physical Broker record/cursor remains the only retry authority; fatal `Error`
is still rethrown. Direct active apply methods are now package-private, leaving
the bounded executor as the cross-package path. The focused mixed-record
regression covers rejection before I/O, exact Command/System Mutation charges,
current-position result projection and expired-lease fencing without a stale
generic retry action. Real source consumer/ACK, Oxia session authority and
dynamic WriteBatch/IO reserve attribution remain release gates.

After `d21983b`, `SourceApplyCoordinator` provides the local one-record source
handoff around the existing `SOURCE_APPLY` work-class executor. It retains the
exact caller-owned look-ahead record until an injected external
`SourceAcknowledgement` returns `ACKED`; only then does it re-check the
position/frame/guard identity and advance the source cursor. Definitive
non-ACK, unknown ACK, queue rejection, apply failure and acknowledgement
exceptions retain that same physical record, while source cursor read or
advance failures fence the local Owner before escaping. The focused
`SourceApplyCoordinatorTest` covers ACK-after-apply, unknown-ACK retry without
reapplying the WriteBatch, and queue rejection without source consumption;
`SourceApplyWorkClassExecutorTest` remains green as well. This is a local
adapter/composition boundary only: real Kafka/Pulsar Fetch/ACK/commit,
rewind, pinned resource/session authority and dynamic production IO admission
remain release blockers.

After `0798f73`, strict owner takeover replay no longer bypasses the
`SOURCE_APPLY` work-class boundary. `SourceApplyWorkClassExecutor.submitRecovery`
uses the same exact Source Position/frame task identity and checked byte charge,
but admits only a `CATCHING_UP` shard and performs the recovery-specific
context-bound lease/clock/barrier/guard reread before the WriteBatch. The
`OwnerRecoveryCoordinator` retains the caller-owned look-ahead entry, returns a
work-class waiting result when another class is selected, and advances the
cursor/`lastCatchupPosition` only after the exact action outcome and look-ahead
identity are proven. Recovery never emits a Broker ACK or a generic retry task;
the source cursor remains the retry authority. `OwnerRecoveryCoordinatorTest`
covers waiting, turn continuation, strict activation after exhaustion and clock
fencing. This closes local active/recovery queue drift; Source Assignment
publication, real Oxia session authority, checkpoint/Object Store selection,
Broker consumer integration, Lane evidence and dynamic production IO attribution
remain release blockers.

After `0261cb5`, checkpoint restore/download is also admitted through the bounded
`CHECKPOINT` work-class instead of being callable as an unbounded cross-package
physical operation. `CheckpointRestoreWorkClassExecutor` binds the exact canonical
manifest/resource/optional-pin request identity and checked byte charge, performs
side-effect-free local validation before queue admission, and repeats the checks
inside the action. Queue rejection leaves catalog/provider/filesystem state
untouched; the selected action owns the complete provider download, inventory
validation and Store-Incarnation install interval and returns either a caller-owned
open `ShardStore` or restore failure evidence. `CheckpointRestoreCoordinator.restore`
is now package-local for tests/composition. `CheckpointRestoreCoordinatorTest` covers
successful queued restore and rejection before provider or staging I/O. This closes
the local restore work-class boundary only; RecoveryPin/Oxia CAS, Object Store
authority, Source Assignment/replay and production dynamic IO attribution remain
release blockers.

After `de5601b`, the planned-drain final checkpoint no longer has a coordinator-level
physical bypass. `CheckpointDrainWorkClassExecutor` binds the normalized target path,
fixed 16-byte checkpoint identity, exact DRAINING Owner Lease/session identity, owner
clock and drain deadline into a domain-separated `CHECKPOINT` task before queue
admission. Queue rejection performs no Store/filesystem I/O and leaves the shard in
retryable `DRAINING`; if another class wins the bounded turn, the coordinator returns
the exact pending task without repeating Claim revoke, callback polling or flush. The
selected action rereads the exact lease before and after `ShardStore.createCheckpoint`,
and a failed outcome is lease-proven before it is exposed. Only after checkpoint success,
post-action lease proof, Store close and exact release does the local Owner become
`FENCED`. `OwnerDrainCoordinatorTest` covers successful continuation, fairness wait,
queue-capacity rejection with exact retry and lease loss during checkpoint. The
four-argument coordinator constructor is package-local and cannot create a final
checkpoint; cross-package Worker composition must provide the shared registry. This
closes the local drain/checkpoint queue boundary only; source quiescence, Oxia session
authority, Object Store publication and production dynamic I/O attribution remain
release blockers.

After `f05290d`, the physical `ShardStore.createCheckpoint(...)` and every
`ShardStore.restoreFromCheckpoint(...)` overload are package-local primitives rather
than public production APIs. Scheduled checkpoint, planned-drain checkpoint and restore
composition must therefore enter through their bounded `CHECKPOINT` executors instead
of exposing a cross-package direct RocksDB seam. `ShardStoreTest` includes a reflection
regression over every declared method with either physical primitive name, and the
focused RocksDB test plus main/test compilation and Checkstyle passed. This is a local
Java visibility guarantee; it does not supply the still-missing production Oxia,
Object Store, Source Assignment or dynamic I/O authority.

After `4b0489e`, the remaining scheduled-checkpoint action seams are also
package-local: `CheckpointExecutionCoordinator.execute(...)`,
`CheckpointPublicationCoordinator.publish(...)` and
`CheckpointUploadCoordinator.upload(...)`. Cross-package Worker composition can no
longer start RocksDB creation, provider upload, upload-intent CAS or catalog binding
outside `CheckpointWorkClassExecutor`; the adapter and authority interfaces remain
public replacement boundaries. `CheckpointExecutionCoordinatorTest` asserts that all
three named actions exist and are non-public, while the focused execution/upload suites,
compilation and Checkstyle passed. This closes a local API bypass only and does not
prove real Object Store or Oxia authority, Source Assignment integration or production
dynamic I/O attribution.

After `d943e0b`, the authority-free whole-turn Lane-close helper is no longer a
cross-package production surface. `LaneCloseMaterializer` and its `runTurn(...)`
action are package-local to the runtime algorithm/tests, while the production-facing
handoff remains `LaneCloseWorkClassExecutor`: one exact cursor candidate, bounded
record count, strict Owner Lease reread and `GC` queue admission. The single-candidate
`DelayShard` primitive remains available to the strict `OwnedDelayShard` wrapper.
`LaneCloseMaterializerTest` reflects over the helper class/action visibility, and the
focused materializer/executor suites, main/test compilation and Checkstyle passed.
Production cursor discovery, Recovery Floor protection and Oxia owner orchestration
remain release blockers.

After `6c3e8ad`, Lane-close cursor discovery itself also runs in a strict bounded
`GC` action. `LaneCloseDiscoveryWorkClassExecutor` binds the exact Shard and full
record/byte/elapsed scan envelope before admission and charges the canonical request
identity plus the full byte envelope; rejection reads no Oxia, clock or Store state.
Execution rereads the Owner Lease and uses a separate monotonic scan clock. One
`BoundedReadBudget` charges actual key/value bytes across both the durable SYSTEM
cursor and dependent Lane projection. An individually oversized cursor fails closed,
while a later cursor that does not fit stays durable for another turn. Discovery
does not advance cursors or mutate Messages; each exact cursor still goes through
`LaneCloseWorkClassExecutor`. The legacy multi-Lane materializer remains package-local.
Focused Lane-close discovery/materialization and full `DelayShardTest` suites plus
compilation and Checkstyle passed. Production scheduling, Oxia authority, admitted
obligation retirement, Object Store quiescence and Recovery-Floor proof remain open.

After `45b99dd` and `5f1eae3`, Message expiry discovery itself has a concrete
bounded `EXPIRY` entrypoint rather than relying on an external unbounded RocksDB
scan. `ExpiryDiscoveryWorkClassExecutor` binds the exact Shard, canonical
Trusted-UTC evidence and record/byte/elapsed budget, and charges the canonical
request identity plus the full scan-byte envelope. Submission rejection reads
neither Oxia, either clock nor RocksDB. The action rereads the execution-time Owner
Lease and uses a separate monotonic scan clock; `BoundedReadBudget` is shared across
each `timeline_cf/EXPIRY` entry and its dependent `id_cf/MESSAGE` projection, charging
their actual key/value bytes. A single candidate larger than the envelope fails
closed, while a later candidate that does not fit the remaining envelope is left for
another turn. Discovery returns candidates without changing Message state; each
candidate still goes through `ExpiryWorkClassExecutor` for the exact signed append.
The focused discovery, append and RocksDB tests plus compilation and Checkstyle
passed. Production scanner scheduling, Trusted-Time/Oxia authority, Broker append/ACK
and source replay remain release blockers.

After `a49b19a`, the durable `timeline_cf/EXPIRY` candidate has a matching
bounded local handoff in `ExpiryWorkClassExecutor`. The caller supplies one
candidate from `DelayShard.discoverExpiry` plus the certified UTC interval;
the executor prepares and signs the exact `EXPIRE_GENERATION_V1` mutation
before queue admission, charges the encoded mutation frame, and performs no
local state transition or Source Position allocation. Execution rereads the
strict Owner Lease/assignment fence and calls only the external
`ShardLogMutationAppender`. A persisted append must carry a matching source
position; definite non-persistence and unknown append outcomes retain the
exact mutation distinction, while an append exception fences the local Owner.
`ExpiryWorkClassExecutorTest` covers persisted, definite, unknown, queue
rejection and failure/fencing branches, including proof that queue rejection
does not consume the expiry projection. This is the shard-local handoff seam;
production expiry scanning/Trusted-Time issuance, Broker append/ACK, Oxia
authority and source-ordered replay remain release blockers.

After `8eb97e4`, the source-fence-derived `timeline_cf/RESERVATION_EXPIRY`
candidate has a bounded `GC`-class materializer. `ReservationExpiryWorkClassExecutor`
binds the exact reservation id, Message id, expiry and reservation state version
into the task identity and byte charge before queue admission; it creates no
System Mutation and allocates no Source Position. Execution rereads the strict
Owner Lease and clock, then atomically materializes only the still-`RESERVED`
projection whose persisted `closedIngressDeadlineThrough` covers the candidate,
updating `id_cf`, the expiry index and reservation quota in one Store batch.
Source-ordered Commit/Cancel/Lane Close races return explicit
`ALREADY_TERMINAL`/`STALE`/`NOT_FOUND` results instead of applying to a newer
projection. `ReservationExpiryWorkClassExecutorTest` covers successful quota
release, queue rejection without Store mutation, stale-candidate fencing and
expired-owner fencing. This is still a local GC composition seam: production
`TIME_FENCE`/scanner scheduling, Oxia Owner authority, Recovery-Floor barriers,
Object Store deletion evidence and end-to-end source replay remain release
blockers.

After `f2c9334`, reservation-expiry candidate discovery is also inside a strict
`GC` work-class turn. `ReservationExpiryDiscoveryWorkClassExecutor` binds the exact
Shard and full record/byte/elapsed scan envelope before admission; rejection reads
no Oxia, clock or RocksDB state. It deliberately accepts no external time cutoff:
execution rereads the Owner Lease and uses only the persisted source-ordered
`closedIngressDeadlineThrough`. One `BoundedReadBudget` charges actual key/value
bytes and elapsed time across both `timeline_cf/RESERVATION_EXPIRY` and the dependent
`id_cf/RESERVATION` projection. An individually oversized candidate fails closed;
a later candidate that does not fit remains for another turn. Discovery neither
materializes nor releases quota, and its byte-identical candidate still passes
through `ReservationExpiryWorkClassExecutor`. Focused discovery/materialization,
Message-expiry and the complete `DelayShardTest` suite plus compilation and
Checkstyle passed. Production `TIME_FENCE` scheduling, Oxia authority,
Recovery-Floor/Object Store GC evidence and source replay remain release blockers.

After `5eedd9d`, the source-ordered Lane-close cursor also has a strict
`GC`-class handoff in `LaneCloseWorkClassExecutor`. It binds the exact
`LaneCloseMaterializationCursor` bytes and per-turn record bound before queue
admission, performs no Store I/O on rejection, then rereads the Owner Lease and
executes one bounded `materializeClosedLane` turn. A cursor removed or advanced
while queued returns `NOT_FOUND`/`STALE` rather than applying the batch to a
different close version; success only advances the already-persisted cursor and
terminal/history projections. `LaneCloseWorkClassExecutorTest` covers success,
rejection, stale cursor and expired-owner fencing. This remains a local
composition seam: production close scheduling, Oxia session authority,
admitted-obligation retirement, Object Store quiescence and Recovery-Floor GC
proof remain release blockers.

After `3ba6fb6`, persistent READY discovery has a concrete active-owner
`DUE_SCHEDULER` entrypoint. `DueSchedulerWorkClassExecutor` binds the exact
Shard, canonical trusted-UTC evidence and complete scan budget into the task
identity, and charges the canonical request bytes plus the configured maximum
scan-byte envelope. Its preflight checks only strict local lifecycle, Shard and
scheduler Owner identity. After `1759405f`, the same preflight also requires the
scheduler Store Incarnation to equal the owned runtime Store Incarnation, so a
same-Shard/same-Owner scheduler over another physical DB fails before action
registration or scheduler/Store access. Claim handoff reuses this preflight.
Queue rejection therefore cannot read Oxia/RocksDB, offer a
Lane head or advance the durable cursor/ring. The selected action rereads the
execution-time Owner Lease/session and passes `evidence.earliestEpochMs` as the
inclusive due-through boundary to `PersistentLaneScheduler`; the existing
persistent scanner retains its complete queue/fairness/cursor rollback. A
successful result contains only newly promoted due heads and does not Claim or
prepare Publish Admission. Discovery/authority failure fences the Owner and
retains the exact generic action as failed process evidence rather than
claiming cursor advancement. The focused regression covers rejection without
scheduler mutation, exact charge, due-boundary success and execution-time
lease expiry. Claim/materialization/Admission handoff, production trusted-time
issuance, dynamic RocksDB/IO attribution and real destination authority remain
release blockers. The old `DelayShard.discoverDue(earliest, limit)` timeline
scan is package-local because it has only a record cap and cannot satisfy the
trusted-time, Owner-reread or byte/elapsed-budget contract; production callers
cannot use it as a shortcut around this executor. The same visibility fence now
applies to count-only expiry, reservation-expiry and Lane-close discovery;
their strict `SchedulerBudget`/monotonic-clock overloads remain the
cross-package primitives used by the owner/work-class composition. The
count-only `DelayShard.discoverReady` index query is package-local too;
production READY scanning occurs in `PersistentLaneScheduler` under complete
trusted evidence and `SchedulerBudget`, submitted only through the executor.

After `120f462`, that production discovery path passes the complete
`TrustedUtcIntervalEvidenceV1` into `PersistentLaneScheduler` instead of
reducing it to one due-through scalar. Strict discovery now requires a typed
`ActiveLaneStateV1`, byte-equal current scheduler Owner and Store Incarnation,
rejects evidence that predates certificate issuance, and requires
`evidence.latest < certificate.validUntil` before a READY head can be promoted.
Failure remains inside the scanner rollback and then fences the active
`OwnedDelayShard`. The focused regression covers the exact expiry boundary,
wrong scheduler Owner, zero pending work after rejection, the successful typed
path and the pre-existing execution-time lease fence. It still does not Claim,
materialize, acquire a publish permit, validate every external certificate
generation or prepare Publish Admission.

After `666f56a`, the previously detected Owner encoding drift is closed across
`PublishAdmissionV1`, `ClaimPreconditionV1`, `ReadyCertificateV1`, local
`ClaimRecord` and canonical Publish Attempt ledgers. Nested Registry fields now
store and parse bare `OwnerIdentityV1`; only the signed System Mutation envelope
uses the tagged `AuthorIdentityV1.owner` branch. Apply/replay compares the outer
Owner's typed nested value to the body bytes, while Claim/Outcome authorization
and recovery-unknown fencing retain the full deployment/worker/epoch/lease
tuple. The canonical projection regression rejects treating the bare nested
value as an outer Author wrapper. A full `test checkstyleMain --rerun-tasks`
run passed with 1232 tests, zero failures/errors and five skipped opt-in
real-Oxia methods because the endpoint was unset; the synchronized clean gate
is recorded below after this documentation update.

After `9240f60`, the READY-to-Claim handoff has a concrete bounded local seam.
`ClaimHandoffWorkClassExecutor` accepts only an already-polled exact head, binds
the shard/Lane/message generation, trusted-time evidence, deadline, typed
`ClaimMaterializationV1` and canonical charge into a `DUE_SCHEDULER` task, then
rereads READY, Message, Timeline, typed Lane and Ready Certificate after queue
wait. `ClaimExecutionAdmission` enforces process-local Worker/Shard/Lane
message-and-byte caps, READY-lane minima and duplicate message-generation
reservations. After `ecf65c30`, one `WorkClassExecutionRegistry` binds one exact
permit-pool instance; every Claim and Publish Admission executor on that Worker
registry must reuse it, and Publish Admission rejects a Reservation created by
another pool before action registration or append. Queue rejection, prerequisite deferral and permit rejection
restore the exact scheduler head; an unexpected exception fences the Owner and
does not create a second local retry authority. A successful local Claim keeps
the reservation active and only then releases the scheduler's retained head.
`ActiveLaneStateV1` now permits a READY certificate to remain after the current
physical READY head is consumed while requiring the key/timing projection to be
absent. Focused Claim, permit, scheduler and typed-Lane tests pass. This is
local composition evidence only: Profile/catalog, Object Store, Adapter
serialization/size, channel/credential generations, real Oxia authority and
Publish Admission/Producer integration remain release blockers.

After `1601053`, the local Claim→`PUBLISH_ADMISSION` handoff has a concrete
bounded `OUTCOME_AND_CONTROL` executor. `PublishAdmissionWorkClassExecutor`
prepares and signs the exact canonical mutation before queue admission, binding
the exact Claim/reservation, descriptor, Ready Certificate, Trusted-UTC evidence
and task identity. Preparation now also requires decision earliest to be no
earlier than descriptor `actionAt` or certificate `issuedAt.latest`; both
causality failures are rejected before work-class registration or append
(`c568a041`). After queue wait it rereads the Oxia owner/ownerEpoch and
Claim and applies an injected prerequisite gate before calling the external-only
`ShardLogMutationAppender`. There is no local Source Position allocator and no
`DelayShard.applySystemMutation` path; a persisted position is checked against
the shard assignment/activation barrier and, for Pulsar, the source connection
generation and guard attestation. Definite non-persistence, prerequisite
deferral and unknown outcomes retain the exact Claim/reservation until
source-ordered Admission apply or explicit revoke, while unknown outcomes fence
the Owner. `PublishAdmissionWorkClassExecutorTest` passes and verifies the exact
body/signature/task binding. This closes only the local Claim→Admission seam;
real Broker append/ACK/cursor, source adapter session, Oxia authority, external
Profile/Object Store/channel credentials, Producer call and source-ordered
outcome apply remain release blockers.

After `187c18c`, the remaining result-mutation callbacks have a shared bounded
`OUTCOME_AND_CONTROL` handoff in `OutcomeWorkClassExecutor`. It accepts only an
already prepared and signed `PUBLISH_OUTCOME`, `EVIDENCE_RESOLUTION`,
`CLAIM_RESULT` or `DLQ_EXPORT_RESULT`, binds the complete canonical mutation
frame to task identity and byte charge, and performs strict local Shard/
AuthorIdentity/Owner-epoch validation before queue admission. Execution rereads
the Owner Lease/clock and calls only the external `ShardLogMutationAppender`;
it never applies the mutation locally or allocates a Source Position. Persisted
positions are checked against the active source assignment/barrier, while
definitive non-persistence and unknown outcomes remain distinct and append or
proof failures fence the Owner with the exact mutation retained for recovery.
`OutcomeWorkClassExecutorTest` covers persisted-without-local-apply,
definitive/unknown append outcomes, queue rejection and expired-owner fencing.
This is a local callback-to-log composition seam only; real callback/evidence
authority, Broker append/ACK/cursor, Oxia session, signing-key history and
source-ordered outcome replay remain release blockers.

After `aed3352`, resource GC mutations have their own bounded `GC` handoff in
`GcWorkClassExecutor`, rather than sharing the result-callback class. It accepts
only an already prepared and signed `RESOURCE_RETIRE_INTENT` or
`RESOURCE_DELETE_CONFIRMED`, validates canonical body identity, protection
Source-Position shard and service author branch before queue admission, then
rereads Owner Lease/clock before calling the external-only
`ShardLogMutationAppender`. It never performs provider deletion, writes local
`gc_cf`, applies the mutation or allocates a Source Position. Persisted,
definitively-not-persisted and unknown append outcomes remain distinct;
append/position-proof failure fences the Owner and retains the exact mutation.
`GcWorkClassExecutorTest` covers persisted retire without local GC apply,
definitive delete non-persistence, queue rejection and expired-owner fencing.
Provider ownership/quiescence, Recovery Floor, source-ordered tombstone apply,
quota release and compaction remain release blockers.

After `77f1587`, the four control-plane mutation types have a dedicated
bounded `OUTCOME_AND_CONTROL` handoff in `ControlWorkClassExecutor`. It accepts
only an already prepared and signed `APPLY_SHARD_CONTROL`, `REPLAY_DEAD_LETTER`,
`RESOLVE_UNCERTAIN` or `TIME_FENCE`, validates canonical control/body identity
and the `TIME_FENCE` proof before queue admission, then rereads Owner Lease/
clock before calling the external-only `ShardLogMutationAppender`. It never
registers control targets, applies local state, writes a control projection or
allocates a Source Position. Persisted, definitively-not-persisted and unknown
outcomes remain distinct; append/proof failure fences the Owner and retains the
exact mutation. `ControlWorkClassExecutorTest` covers persisted TIME_FENCE
without local apply, queue rejection and expired-owner fencing. Real control
registration/authorization, Broker append/ACK/cursor, Oxia session, source
replay and signing-key history remain release blockers.

After `096f461`, `LeaseFenceWorkClassExecutor` provides the
concrete `LEASE_FENCE` preemptive entrypoint. It binds the complete observed
Owner Lease identity into the task bytes before queue admission, rereads the
Oxia lease and owner clock after the queue wait, returns `OWNER_STILL_VALID`
without fencing when the exact lease is still live, and fences/stops only when
the queued identity is still the local identity but the authoritative lease has
expired or been replaced. A delayed old-owner task cannot fence a replacement
local Owner; clock/authority/stop failures remain `UNKNOWN`, and queue
rejection has no stop or Store side effect. `LeaseFenceWorkClassExecutorTest`
covers expiry/replacement, still-valid, and queue-rejection branches. This is a
local preemptive handoff only; Oxia session watch, Broker pause/rewind,
scheduler shutdown and cross-Worker stop authority remain release blockers.

After `fc0cf8a`, `QueryWorkClassExecutor` provides the concrete read-only
`QUERY` action. It binds the exact Shard and canonical request envelope into
the task identity and charges the actual envelope bytes before queue admission;
the preflight performs no Oxia/RocksDB read. The bounded action rereads the
Owner Lease/clock before and after the injected shard-local read, returns the
value only when the same owner remains authoritative, and discards a snapshot
on ownership loss as `SHARD_TRANSITIONING`. Queue rejection and expired-owner
paths do not invoke the read callback. `QueryWorkClassExecutorTest` covers
successful linearization, ownership loss, expiry and rejection. This is a
local query composition seam; Gateway routing, tenant authorization,
retention authority, cross-worker forwarding and observability remain release
blockers.

After `666f56a` and the corresponding design/status/audit synchronization, the full
`GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
--rerun-tasks --console=plain` gate passed on 2026-08-12: 1232 tests were
reported with zero failures/errors and five skipped opt-in real-Oxia methods
because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. `checkDocumentation` and
`checkstyleMain` passed in the same run. This is current local repository
evidence; the skipped real-service and other external release gates remain
open.

The Oxia transaction question was checked against the locked source and the
Gradle-resolved `oxia-client:0.9.0` API.  Its public `SyncOxiaClient` and
`AsyncOxiaClient` expose one-key `put`/CAS operations only.  The internal
client-side `WriteBatch` emits a `WriteRequest` containing multiple puts, but
the factory/implementation is not a public application API and the request is
only a per-Oxia-shard batch; it is not a cross-record transaction.  Oxia's
server `ProcessWrite` commits that request as one local database batch, which
does not prove that independently keyed Owner Lease, Upload Intent, Catalog or
Recovery Pin records share a shard or can be submitted atomically.  We keep
`OxiaSyncRecoveryCatalogBackend.publishUploadedCheckpoint` and the
session-bound pin/activation paths fail-closed until a supported transaction
or an equivalent single-record authority is available; no reflection or
best-effort concurrent puts are used to manufacture V1 atomicity.

The Gradle `checkDocumentation` task is now part of `check`. It verifies that
the V1 authority documents exist, the document map still points at the main
design, and the main design, Protocol Registry, ADR index, Status and Audit
carry the same frozen spec revision. The task passed in the same gate; it is a
document-governance check and does not relax any semantic or release gate.

The local Worker event-loop composition seam is now explicit in
`WorkClassEventLoop`. It keeps queue admission bounded without holding shared
record/byte tokens while work waits, acquires one exact `WorkClassResourcePool`
lease per task immediately before the task leaves the queue, and returns those
leases in an owned `Turn` that must close before another bounded poll. If a
later task cannot acquire capacity, `WorkClassScheduler` restores the complete
queue/fairness snapshot and the already acquired leases are released. The
`runTurn` helper executes one bounded callback sequence outside the event-loop
monitor, checks borrowed-hold validity before and after each callback, and
closes every lease before rethrowing callback or close failures.
`WorkClassEventLoopTest` covers rejection rollback, one-open-turn fencing,
idempotent close, borrowed-hold failure and callback-failure cleanup. This is
still a local event-loop/resource seam; dynamic RocksDB WriteBatch admission,
checkpoint/compaction I/O authority and production Worker wiring remain release
gates.

Commit `c4391ca` closes the local checkpoint-download admission gap.  The
`CheckpointRestoreCoordinator` now acquires one idempotent Worker-wide
`CheckpointDownloadPermit` before invoking the provider and holds it through
provider materialization, complete inventory validation and
`ShardStore` Store-Incarnation installation.  The restore helper consumes the
same permit without double-acquiring it, and the coordinator regression proves
that a provider callback cannot acquire a second download slot while the
first operation is active.  This is process-local concurrency evidence; remote
Object Store authority, Owner Lease/session, Source Assignment and source
replay remain release blockers.

After `dc7a300`, `GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle
./gradlew clean check --rerun-tasks --console=plain` completed successfully:
1205 tests ran with zero failures/errors and 5 opt-in real-Oxia tests were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset.  The gate verifies the
permit change against the complete local regression suite; the skipped external
smoke tests remain release evidence gaps.

Commit `465d6de` adds a crash-durable byte backend to the local large-payload
seam:
`FilesystemPayloadObjectStore` reuses the exact reservation/handle/proof state
machine while persisting service-owned payload bytes below a guarded root. It
uses no-follow reads, private temporary files, fsync/atomic publication and
immutable byte-identical retry; a restarted adapter can re-register the exact
reservation and reproduce the same handle/proof over the existing bytes.
`FilesystemPayloadObjectStoreTest` covers restart stability, immutable conflict,
corruption fencing and symlinked-root rejection. Reservation metadata remains
source-ordered in the shard and remote Object Store credentials, quiescence,
attestation, Oxia protection and deletion authority remain release blockers.

After `da3146c`, the full repository gate was rerun with
`GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
--rerun-tasks --console=plain`: `BUILD SUCCESSFUL` in 1m 1s, 1205 tests
completed with no failures, and 5 opt-in real-Oxia tests skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. The skipped external smoke tests remain
release evidence gaps rather than local test failures.

Commit `f9e8583` adds `CheckpointDownloadAdapter` and
`CheckpointRestoreCoordinator`. The coordinator allocates a per-attempt local
download staging boundary, rejects a provider path outside that boundary,
rechecks the complete downloaded inventory, invokes the existing finite-limit
manifest/pin-aware `ShardStore.restoreFromCheckpoint` path, and removes the
provider tree only after the new Store Incarnation is installed. The focused
`CheckpointRestoreCoordinatorTest` covers a real RocksDB checkpoint round trip
and an out-of-bound provider return. This remains local orchestration evidence;
Owner Lease/session, Source Assignment, RecoveryPin/Oxia transaction and Source
Log replay are still external release gates.

Commit `4b1c2b0` adds `CheckpointDownloadRequest` and
`FilesystemCheckpointDownloadAdapter` as the matching local/test restore seam.
It verifies the catalog-bound manifest object before file I/O, streams every
immutable object through a no-follow handle into a private temporary tree,
re-inventories the complete result against the manifest, and atomically
publishes the target directory only after all checks pass. The focused
`FilesystemCheckpointDownloadAdapterTest` covers complete restore, corrupt
object cleanup and existing-target protection. This remains local provider and
filesystem evidence; remote Object Store credentials/attestation/quiescence,
RecoveryPin/Oxia transaction and Source Log replay remain release blockers.

Commit `1aca36a` adds `FilesystemCheckpointUploadAdapter` as a bounded local/test
Object Store seam. It streams every manifest file into a deterministic immutable
object path derived from the opaque object key/version, verifies length and
SHA-256, writes the manifest last through a durable temporary-file/atomic-rename
boundary, and accepts retries only when existing bytes are identical. The focused
`FilesystemCheckpointUploadAdapterTest` covers complete-file publication,
response-loss-style idempotent retry, immutable-object conflict and source
symlink rejection. This is provider-local evidence only; credentials,
quiescence/attestation, remote consistency/deletion, Owner Lease/session,
Upload Intent/Catalog transaction and real Object Store conformance remain
release blockers.

Commit `6efd89f` adds `CheckpointExecutionCoordinator` as the local bounded
execution seam from an exact `CheckpointScheduler` claim through fixed-ID
RocksDB checkpoint creation, manifest-vs-Store identity verification and the
existing upload/catalog coordinator. It reuses an existing local checkpoint
directory only when the Store's persisted `lastCheckpointId` matches the
pending intent, and it always completes the same exact scheduler claim after
success or failure. `CheckpointControlSnapshotVerifier` now also rereads the
physical image's checkpoint ID, applied Source Position, mutation sequence and
evidence-cursor projection when a complete manifest is supplied. The focused
`CheckpointExecutionCoordinatorTest.retriesSamePhysicalCheckpointAfterCatalogResponseLoss`
and the checkpoint/recovery regression set passed. This is local ordering and
response-loss evidence only; Owner Lease/session, Source Assignment, Object
Store attestation/quiescence and the production Oxia cross-record transaction
remain release blockers.

The locally available broker source trees were inspected on 2026-08-12 as
evidence for the remaining transport gate, not as a substitute for a Delay
transport implementation.  The locked Kafka checkout
`/Users/liusinan/apps/ideaproject/nereusstream/kafka` is at
`76f62f3b83e882105219b6c7687dbde594a8b8a2`; its `ProduceRequest` schema
supports topic-ID-only version 13, and its Nereus broker log requires an exact
non-zero topic ID.  Its producer `Sender` still obtains IDs from ordinary
name-keyed metadata and falls back to the zero UUID when metadata is absent;
the Delay project therefore has no safe pinned request transport merely by
using stock `KafkaProducer`.  The locked Pulsar checkout
`/Users/liusinan/apps/ideaproject/nereusstream/pulsar` is at
`11d7ab15291ca4bbc9cc29dedd7878c4e1311ec9`; it contains broker-side Nereus
topic-open and write-fence integration, but no client-side Delay adapter that
returns the authenticated physical topic incarnation/creation identity and
guarded send evidence required by V1.  These source inspections keep the
Kafka/Pulsar rows below as release blockers until concrete pinned transports
and real-broker evidence are added.

Latest real-service verification on 2026-08-12 ran the repository gate with a
temporary Oxia standalone service built from source commit
`37a17bef17202d5fd6e23282da5fd26d94865484`:

```text
NEREUS_DELAY_OXIA_ENDPOINT=127.0.0.1:6648 \
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle \
./gradlew clean check --rerun-tasks --console=plain
BUILD SUCCESSFUL in 1m 1s
```

All five opt-in real-Oxia methods ran rather than being assumed or skipped:
Owner Lease/ephemeral session (1), Control Target/Operation CAS (2),
Recovery Catalog/Floor and local-reuse validation (1), and Checkpoint Upload
Intent create/publish/reopen (1); every method reported `skipped=0`,
`failures=0`, `errors=0`. The Oxia process used only a temporary local data
directory and was stopped after the run. This closes the current real-Oxia
smoke evidence for the implemented single-record authorities, but it does
not close the cross-record Owner Lease/catalog/pin transaction, Object Store,
Kafka/Pulsar, chaos, benchmark, soak or upgrade release gates.

Commit `43e61c6` adds `OwnerRecoveryCoordinator` and `OwnerRecoveryTurn` as the
local bounded takeover orchestration seam. After the caller has selected,
opened and locally validated a Store Incarnation, one `runTurn()` performs the
context-bound `CATCHING_UP` Owner Lease CAS, delegates exactly one bounded mixed
source replay turn, and only on cursor exhaustion performs the strict persisted
control-snapshot/`ACTIVE_FOR_COMMANDS` CAS. The regression
`OwnerRecoveryCoordinatorTest.runsOneBoundedTurnAndActivatesOnlyAfterTheCursorIsExhausted`
covers turn continuation, authority state and idempotent completion; the clock
failure test proves the local Owner is fenced before catch-up begins. This is
only local orchestration evidence: Source Assignment publication, Oxia session
creation, checkpoint/Object Store selection and download, Broker guards, Lane
evidence and production Worker scheduling remain release blockers.

Commit `eca483b` adds `CheckpointPublicationCoordinator` as the matching local
checkpoint publication seam. It checks the pending intent's catalog generation
before provider I/O, reuses the exact `PUBLISHED` intent on retry, and then
binds that manifest/resource identity through
`RecoveryCatalogAuthority.publishUploadedCheckpoint`. The focused
`CheckpointUploadCoordinatorTest.publicationCoordinatorBindsPublishedIntentToCatalogAndRetriesCatalogResponseLoss`
regression covers the catalog binding and confirms a retry does not call the
provider again. This remains local ordering/idempotency evidence; the
cross-record Oxia transaction, Object Store attestation/quiescence and owner
abandonment proof remain release blockers.

Commit `4e2cf94` tightens the local recovery-reuse open boundary: an ACTIVE
Store is opened without rewriting its runtime/recovery OPEN projection, the
persisted recovery metadata is passed to `RecoveryCatalogAuthority` for
read-only validation, and only a successful proof may publish the OPEN marker
in a synchronous WriteBatch. A rejected proof therefore cannot be hidden by
an eager open-phase rewrite. `ShardStoreTest.localRecoveryReuseOpensOnlyCatalogValidatedActiveStore`
now asserts that validation observes the prior `CLOSED_CLEAN` projection and
that OPEN is published only after validation returns. The focused test and
the full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). This remains a local ordering fence;
Owner Lease/session fencing, source replay and final activation are still
release blockers.

Commit `a9321b4` adds the strict authority-bound catch-up seam: after local
Store identity/recovery validation, `OwnedDelayShard.markCatchingUp` with an
`OxiaOwnerLeaseStore`, exact `SourceAssignment`, live time and pinned
`SourceReplaySuccessor` CASes the same context-bound Owner Lease to
`CATCHING_UP` before opening the local replay gate. A response-loss reread is
accepted only for the same fencing identity, assignment/session context,
state and non-regressed expiry; contextless legacy leases are rejected before
any authority transition. `OwnerLeaseTest` covers the authoritative state
publication and the contextless fail-closed path. Commit `38f6a60` adds an
explicit backend that drops the successful transition response; the regression
proves that only an exact authority reread can open the local replay gate. This
closes only the local per-record lifecycle CAS ordering; source assignment
publication, Oxia session orchestration, cross-record activation/pin authority
and real Broker replay remain release blockers.

Commit `6583eac` binds the strict catch-up replay window to that same
`OxiaOwnerLeaseStore`: every bounded turn and every record rechecks exact
owner/epoch/token, assignment/session context, `CATCHING_UP` state and
non-regressed live expiry before source look-ahead or application. A replaced
authoritative owner therefore fences the local shard before the next record;
an Oxia lease-read failure also fences `applyAuthoritatively` instead of
leaving the local command gate usable. `OwnerLeaseTest` covers both the owner
replacement and authority-read failure paths. Compatibility assignment-only
replay remains explicitly local-only, and real Oxia session orchestration,
source assignment publication and Broker replay remain release blockers.

Commit `b387648` closes the matching activation seam: the
`activateForCommandsWithControlSnapshot(OxiaOwnerLeaseStore, ...)` entrypoint
now requires a context-bound lease and a strict catch-up replay authority before
it can validate the persisted control snapshot or write the local owner-open
projection. A contextless compatibility lease is rejected before either local
mutation or the `ACTIVE_FOR_COMMANDS` authority CAS; the focused
`OwnerLeaseTest` covers both the strict success path and unchanged `ACQUIRING`
authority on rejection. The no-snapshot/assignment-only activation overloads
remain embedded compatibility seams, while the production control catalog and
atomic RecoveryPin/lease transaction remain external blockers.

Commit `45ec559` adds the strict activation pre-CAS fence: after the control
snapshot and strict context checks, the entrypoint rereads the same
authority-bound `CATCHING_UP` lease before writing `lastOpenedOwnerEpoch` or
requeueing restored Claims. Replacement-owner, assignment/session drift and
authority-read failure therefore fence without publishing an old Owner's local
activation projection; `OwnerLeaseTest` verifies the replacement path leaves a
fresh Store epoch at zero. This is still a local ordering fence; the atomic
production control-catalog/RecoveryPin transaction and real session authority
remain external blockers.

The strict owner lifecycle boundary now continues through planned drain.
`OwnedDelayShard.beginDrainStrict` requires the context-bound lease, accepted
Source Assignment and strict catch-up authority before the authoritative
`ACTIVE_FOR_COMMANDS -> DRAINING` CAS. `OwnerDrainCoordinator` selects this
entrypoint automatically for strict owners, while assignment-only owners keep
an explicit embedded compatibility path. `OwnerLeaseTest` covers both the
contextless rejection (with no authority transition) and the exact
assignment/session identity preserved by a successful drain CAS. This is still
local lifecycle evidence; Oxia session orchestration, source assignment
publication and production planned-drain routing remain release blockers.

The same strict context now covers authoritative Command mutation. The new
`OwnedDelayShard.applyAuthoritativelyStrict` entrypoint requires the
context-bound catch-up state before rereading the exact `ACTIVE_FOR_COMMANDS`
lease and invoking the Delegate; a contextless compatibility lease is rejected
without a local mutation. `OwnerLeaseTest` covers both rejection and a
successful context-bound apply. The ordinary authoritative overload remains an
explicit embedded seam; production source-writer wiring and real Broker
assignment/session authority remain release blockers.

After the terminal-Lane reservation/Commit, publish-charge ordering and typed
READY recovery fencing fixes (`c619b38`, `f771f64`, `3527c89`), the repository
verification command `./gradlew clean check --rerun-tasks --console=plain`
passed on 2026-08-12. The five opt-in real-Oxia methods remained skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured; this local PASS does not close
the real-service, transport, Object Store, chaos or benchmark release gates.

After the journal Source Position identity fences (`22b9409`, `5d22d8a`), the
same full command passed again on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks).
The five real-Oxia methods were still skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset; this recheck confirms local regression
health only and does not close any external release gate.

The typed READY recovery boundary now validates the complete projection at the
physical scheduler index: a typed ACTIVE Lane must be READY/OPEN, carry both
the exact encoded READY key and a decodable `ReadyCertificateV1`, and match the
physical key's Lane/version/eligibility fields before recovery or discovery can
rebuild a claimable head. Legacy adapter Lanes retain their explicit
compatibility path. Focused coverage is
`DelayShardTest.typedReadyProjectionRefreshesEarliestActionBoundaryFromCurrentHead`;
this is local projection evidence, not external capability or transport
authority.

`PayloadReservation` now validates both the current lifecycle Source Position
and the receipt-anchor Source Position through `SourcePositionCodec` before a
reservation value can be constructed or persisted. The decoded position must
be canonical and belong to the reservation's Shard; arbitrary non-empty bytes
and a foreign-Shard anchor therefore fail closed instead of entering the
source-order, receipt or GC-protection path. `PayloadReservationTest` covers
malformed, foreign-Shard and canonical round-trip cases. This is local durable
binding evidence only; authenticated source assignment and external receipt/
Object Store authority remain release gates.

The same pre-persistence canonical Source Position fence now applies to
`MessageRecord.scheduleSourcePosition`, `TerminalGenerationRecord`'s applied
position and `PublishAttemptLedger.sourcePosition`: construction and decode
run `SourcePositionCodec` and retain only exact canonical bytes, so malformed
or non-canonical values cannot enter `id_cf`, `terminal_cf` or `inflight_cf`.
`DelayShard` continues to validate the decoded position against its current
Shard when reading key/value projections; journal-position bytes remain a
separate adapter-local evidence field. Focused coverage is
`MessageRecordTest.sourcePositionMustBeCanonicalBeforeMessageValueConstruction`,
`TerminalGenerationRecordTest.sourcePositionMustBeCanonicalBeforeTerminalValueConstruction`
and `PublishAttemptLedgerTest.sourcePositionMustBeCanonicalBeforeAttemptValueConstruction`.

The post-GC `RetiredMessageIdentityRecord` now applies the same canonical
decode before persisting its retained source anchor. Its Shard identity is
still checked by `DelayShard` when the compact identity is read, so a malformed
or foreign source cannot authorize Message identity reuse. The focused
coverage is `RetiredMessageIdentityRecordTest`, including canonical round-trip,
malformed-source and trailing-byte rejection. This remains local retention
evidence; Recovery-Floor and Route identity-retention authority remain release
gates.

The local Kafka Receipt Journal and Pulsar Attempt Journal now apply the same
Source Position canonical decode to `AttemptIdentity`; creating a Mapping also
requires the DelayMessageId and decoded source position to belong to the
journal Shard. This prevents a valid-looking Journal mapping from carrying a
foreign or malformed ingress anchor. `KafkaReceiptJournalTest` and
`PulsarAttemptJournalTest` cover malformed identities and cross-Shard mapping
rejection. These are local adapter invariants only; authenticated Broker
receipt/journal durability, response classification and real transport proofs
remain release blockers.

`ResourceRetireIntentBody.ProtectionRef` now applies the same constructor
fence to source-bearing protection kinds: `minimumSourcePosition` is
canonical-decoded, kind-specific field presence is checked, and the supplied
canonical bytes must exactly re-encode those fields. Directly constructed
Recovery-Floor, query-retention or replay-window references therefore cannot
smuggle malformed source anchors into a protection set. The focused coverage is
`ResourceRetireIntentBodyTest.protectionRefConstructorRequiresCanonicalSourceAndKindSpecificFields`;
Recovery-Floor ancestry and external catalog authority remain release gates.

The same public-construction boundary now covers the surrounding
`ExactResourceIdentity` and `ProtectionSet` values: identity branches and
identity hashes must agree, while protection references must be strictly
sorted/unique and match their set digest and canonical bytes. Direct callers
cannot bypass the decode-only integrity checks before a retire body is
persisted. `ResourceRetireIntentBodyTest.exactIdentityAndProtectionSetConstructorsRequireCanonicalDigestsAndOrdering`
covers the mismatch, duplicate and digest failures; external resource and
Recovery-Floor authority remain release gates.

The durable `ResourceRetireIntentRecord` now decodes and stores only a
canonical `ProtectionSet`, rather than accepting arbitrary non-empty bytes.
`DelayShard` additionally checks every source-bearing protection reference
against the applying Shard before the retire WriteBatch; a valid Source
Position from another Shard is rejected as `STALE_SYSTEM_MUTATION` and no GC
intent is persisted. `ResourceGcGuardTest.retireIntentRecordRequiresCanonicalProtectionSet`,
`ResourceRetireIntentBodyTest.protectionSourcePositionsMustBelongToApplyingShard`
and `DelayShardTest.resourceRetireIntentRejectsForeignProtectionSourceBeforePersistence`
cover the local boundary. Recovery-Floor/catalog authority and external GC
orchestration remain release blockers.

After the retire identity/protection constructor and persistence fences
(`18629dd`, `2a84e88`, `4a81566`, `7180153`, `ac635b6`, `ede3ff3`),
`./gradlew clean check --rerun-tasks --console=plain` passed again on
2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The five real-Oxia methods remained
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is local
regression evidence only.

The `dedupe_cf/POSITION` key boundary now canonical-decodes its Source
Position before constructing the physical-position audit locator. Malformed or
trailing-byte positions therefore cannot create look-alike dedupe keys from a
direct caller; `KeyCodecTest.dedupePositionRequiresCanonicalSourcePositionBytes`
covers the valid and fail-closed paths (`0259ffb`). This is local key-codec
evidence only; source assignment and Broker receipt authority remain release
gates.

After `0259ffb`, `./gradlew clean check --rerun-tasks --console=plain` passed
again on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The five real-Oxia methods
remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this confirms
the local regression suite only and is not live-service or release evidence.

`SourceReplayRecord` and `SourceReplayMutation` now reject a replay entry when
its command or signed mutation belongs to a different Shard than the supplied
Source Position. `SourceReplayEntryTest` covers both constructor fences. This
keeps the typed replay cursor aligned with the one-shard Source Log boundary;
adapter assignment and continuity proofs remain external release gates.

After `19dede3`, `./gradlew clean check --rerun-tasks --console=plain` passed
again on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The same five real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset.

The local `TIME_FENCE_V1` apply path now carries an explicit
`DelayShardConfig.timeFenceSafetyMarginMs` input and checks the Trusted-UTC
proof with checked addition before advancing the ingress watermark. A proof one
millisecond below the configured boundary is rejected as
`UNAUTHORIZED_SYSTEM_MUTATION`, while the exact boundary is accepted; legacy
config constructors retain a zero-margin compatibility seam. Production Route
configuration still must supply the benchmarked nonzero margin and authenticated
fence-writer authority.

The owner-lease boundary now has a concrete Oxia Java client backend in
`OxiaSyncOwnerLeaseBackend`. It stores the fencing epoch in a durable Oxia
record, allocates it with version CAS, creates the lease as an Oxia ephemeral
record, binds context-bound acquisition to Oxia session/client metadata, and
uses version CAS for renew, lifecycle transition, and release. Epoch gaps after
a lost race are intentional and safe; reuse is never allowed. The deterministic
record surface is covered by `OxiaSyncOwnerLeaseBackendTest`. An opt-in
`OxiaRealServiceSmokeTest` can now run the same boundary against a real Oxia
gRPC service (`NEREUS_DELAY_OXIA_ENDPOINT=host:port ./gradlew test
--tests io.nereusstream.delay.ownership.OxiaRealServiceSmokeTest
--rerun-tasks`); on 2026-08-12 it passed against the standalone Oxia source at
`a45e38cf2b8c815499fda4c1b59e017db769142f`, covering durable epoch CAS,
ephemeral-session creation/removal, renew, lifecycle transition, release and
reacquisition with an incremented epoch. The test is skipped when no endpoint
is configured. Authenticated assignment publication, response-loss and
multi-worker chaos evidence remain release gates.

The Recovery Catalog now also has a concrete single-record Oxia CAS backend in
`OxiaSyncRecoveryCatalogBackend`. One shard's published manifests, immutable
manifest-object identities, scalar Floor and typed Floor reference are stored
as one bounded canonical snapshot, so publication and Floor advancement use one
Oxia version CAS rather than a sequence of puts that would only look atomic.
Exact response-loss reread, canonical decode, snapshot corruption rejection and
reopen behavior are covered by `OxiaSyncRecoveryCatalogBackendTest`. This slice
does not claim the stronger cross-record transaction needed to bind an upload
intent or session-bound RecoveryPin to the Owner Lease; those operations remain
explicitly fail-closed and are still release gates, together with real Oxia
service/session and multi-worker evidence.

The same Oxia catalog backend now performs the read-only local Store recovery
reuse check against the current remote catalog/Floor snapshot. It validates the
exact shard, published lineage/manifest, observed Floor generation and
install-state/store-incarnation tuple before a caller may reuse a local DB;
stale Floor or descendant drift fails closed. This does not infer Owner
Lease/session authority and does not replace the required cross-record
activation transaction.

The checkpoint manifest projection now rejects duplicate file object identities
(`objectKey` plus immutable `objectVersion`) and rejects reuse of one SHA-256
checksum across files with conflicting lengths before a manifest can be
serialized or published. Reusing the same checksum at the same length remains
valid; this matches Protocol Registry §10's object-identity and checksum
consistency rule. `CheckpointManifestTest.duplicateObjectIdentityIsRejectedBeforePublication`
and `CheckpointManifestTest.checksumWithConflictingLengthsIsRejectedButSameLengthReuseIsAllowed`
cover the boundary. Object Store publication and immutable provider attestation
remain external release gates.

Checkpoint file inventory now opens each regular file once with
`NOFOLLOW_LINKS`, hashes that same channel and verifies the channel length is
stable for the scan. This closes the check-then-open symlink replacement window
between the physical-file test and checksum read; the existing
`CheckpointManifestTest.inventoryRejectsSymlinkedCheckpointFiles` regression
continues to cover the fail-closed boundary. This is local checkpoint
integrity evidence and does not replace provider immutability or attestation.

The shard checkpoint/restore filesystem paths now use the same component-by-
component real-directory guard before creating `checkpoint-tmp`, `restore-tmp`,
staged DB descendants, or the final checkpoint parent. The guard starts at the
nearest already-existing deployment path (so a deployment-managed leading
symlink is not mistaken for a shard-owned component), but rejects a symlink at
the local temporary boundary and rechecks a concurrent create. The existing
temporary-path regression plus `ShardStoreTest.checkpointRejectsSymbolicParentComponentBeforeCreatingOutsideFiles`
cover the local boundary; this remains local evidence rather than external
filesystem/quota authority.

The crash-durable local Recovery Catalog, Checkpoint Upload Intent and SLO
collector projections now use the same component-by-component real-directory
guard before creating their state, lock and temporary-file parents. A missing
intermediate component is created with `CREATE_DIRECTORY` semantics and every
concurrent create is rechecked with `NOFOLLOW_LINKS`; a symbolic parent cannot
redirect a projection outside its configured state boundary. The focused
regressions are `PersistentRecoveryCatalogTest.rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary`,
`CheckpointUploadIntentStoreTest.rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary`
and `PersistentSloObservationCollectorTest.rejectsSymbolicParentComponentBeforeCreatingStateOutsideBoundary`.
This closes local projection path handling only; it does not replace external
Oxia, Object Store or collector authority.

The same shared guard now covers the local Control Operation and Owner Lease
state roots. Their missing root descendants are created one component at a time
and existing/raced components are rechecked without following symlinks. The
focused regressions are
`PersistentControlOperationAuthorityTest.rejectsSymbolicParentComponentBeforeCreatingControlStateOutsideBoundary`
and `PersistentOwnerLeaseStoreTest.rejectsSymbolicParentComponentBeforeCreatingLeaseStateOutsideBoundary`.
This keeps all crash-durable local authority projections on one physical path
policy; it remains an embedded seam, not production Oxia authority.

Crash-durable state reads now use one bounded `NOFOLLOW_LINKS` file handle for
the size check and byte read instead of checking a path and reopening it with
`Files.readAllBytes`. Recovery Catalog, Upload Intent, SLO Collector, Control
Operation and Owner Lease projections share this helper; the shard `ACTIVE`
pointer uses it as well. A symlink, directory, disappearance, size change or
short read fails closed. `LocalStatePathGuardTest` covers the bounded read and
target-type fences. This closes the local state-file check-then-open window
only; it does not replace external authority or process supervision.

The per-shard physical-usage probe now treats a symbolic-link DB root, a
symbolic link, or any non-regular entry inside an open DB as missing physical
evidence and fails closed instead of silently skipping it. File sizes are
observed through one `NOFOLLOW_LINKS` handle, so a replacement or disappearance
cannot be counted as zero bytes.
`ShardStoreTest.physicalUsageFailsClosedOnADeceptiveSymbolicFile` covers the
local monitor boundary; process-wide disk/quota authority remains external.

ACTIVE pointer publication now writes the fixed `ACTIVE.tmp` path through a
`NOFOLLOW_LINKS` channel and forces the exact bytes before atomic rename. A
raced replacement of the prechecked temporary path cannot redirect pointer
contents through a symlink; the existing
`ShardStoreTest.checkpointAndActivePointerTemporaryPathsRejectSymbolicLinks`
regression covers the fail-closed temporary boundary.

The source-ordered Lane control projection now preserves the V1 closed result
union for Resume: an already `OPEN` Lane returns `ALREADY_OPEN`, an
`ORDERING_BROKEN`/`CLOSED` Lane returns its corresponding stable code, and a
terminal guard returns `LANE_TERMINALLY_CLOSED` instead of the previous generic
`TOO_LATE`. Resolve retry and Dead Letter replay use the same distinction for a
closed versus irreversibly retired Lane. `DelayShardTest` covers the OPEN
idempotent Resume and terminal-guard Resume paths. This is a local source-log
projection; authenticated control registration and production Oxia authority
remain release gates.

The scheduler unregister boundary now requires `AdmissionGate.RETIRED`, which
is the local projection of an installed same-key `LaneTerminalGuardV1`; an
ordinary source-ordered `CLOSED` Lane remains registered until the
Recovery-Floor/adapter retirement protocol completes. `LaneSchedulerTest` now
covers both the closed rejection and the exact-incarnation retired cleanup,
including persistent fairness-state removal. This only releases process-local
scheduler state and does not authorize physical Lane retirement.

The compatibility `LaneRecord` transition now preserves the final
`laneControlVersion` when projecting `CLOSED -> RETIRED`; only the runtime
version advances. This matches the terminal-guard rule that physical
retirement is not a new source-ordered management CAS. `LaneRecordTest` covers
the retained control version; the durable retirement method already applies
the same rule and still requires the external Floor/adapter proof.

The terminal-guard path now also scans the bounded reservation namespace before
replacing an active Lane, so an unmaterialized `RESERVED`/`COMMITTED` payload
reservation cannot be bypassed by physical retirement. Large-payload Commit
checks the same-key `RETIRED` guard before lifecycle/proof handling and returns
`LANE_TERMINALLY_CLOSED` without projecting a Message or rewriting the compact
terminal value as ACTIVE. Activation/quota rebuild also rejects a current
message or reservation that names a retired Lane instead of reconstructing a
live quota entry. `DelayShardTest.largePayloadCommitCannotResurrectTerminalLaneGuard`
covers the local retirement fence, stale-Commit branch and reopen fail-closed
path. This is local state-machine evidence only; close-cursor completion,
Recovery Floor, adapter quiescence and authenticated terminal-guard authority
remain release gates.

`OxiaRealRecoveryAuthoritySmokeTest` now exercises both single-record recovery
authorities against a real Oxia endpoint when
`NEREUS_DELAY_OXIA_ENDPOINT=host:port` is set: Recovery Catalog publication,
typed Floor CAS, reopen and local-store reuse validation, plus Upload Intent
PENDING_UPLOAD -> PUBLISHED CAS and exact reopen. The 2026-08-12 run passed
against Oxia source `a45e38cf2b8c815499fda4c1b59e017db769142f`; these tests are
still opt-in and do not claim the deliberately unsupported cross-record
Owner Lease/session + catalog transaction or Object Store attestation.

The Control Operation state now has a concrete per-operation Oxia CAS backend
in `OxiaSyncControlOperationBackend`. It stores the complete receipt and
CURRENT projection in one checksummed canonical record, uses version CAS for
register/advance, and accepts a response-loss retry only after an exact
successor reread. `OxiaSyncControlOperationBackendTest` covers idempotent
registration, revision/state fencing, retention-bound query, reopen and
response-loss behavior. This is only the durable record surface: authenticated
scope/actor authorization, source-ordered routing, session ownership and
production Oxia service evidence remain release gates. The opt-in
`OxiaRealControlAuthoritySmokeTest` additionally passed real Oxia
register/advance/query/reopen for Control Operation state and immutable
Prepared Control target registration on 2026-08-12 against source
`a45e38cf2b8c815499fda4c1b59e017db769142f`; authorization, source ordering and
mutation transaction semantics remain outside this single-record evidence.

Checkpoint Upload Intent now has an exact-authority interface shared by the
local store, `CheckpointUploadCoordinator` and
`OxiaSyncCheckpointUploadIntentBackend`. The Oxia implementation stores one
shard/checkpoint intent record, uses version CAS for PENDING_UPLOAD to
PUBLISHED/REAPING, validates deadline evidence and accepts response loss only
after an exact successor reread. `OxiaSyncCheckpointUploadIntentBackendTest`
covers pending/published/reaping transitions, reopen, response loss and
corruption. The backend still does not claim the cross-record Owner
Lease/session + catalog publication transaction or Object Store attestation.

Immutable Control target registration now has a concrete per-operation Oxia
backend in `OxiaSyncControlTargetRegistrationBackend`. It stores the exact
Prepared bytes in one canonical record, performs `IfRecordDoesNotExist`
registration, and treats a response-loss retry as idempotent only after an
exact reread. `OxiaSyncControlTargetRegistrationBackendTest` covers reopen,
conflict and corruption behavior. This remains a durable registration record,
not actor authorization, target existence, source ordering or an Oxia
transaction joining registration to mutation application.

The configured Control target-registration gate now distinguishes an
authoritative binding mismatch from an unavailable authority.  A missing
registration still produces the bounded `UNAUTHORIZED_SYSTEM_MUTATION`
position result, while a registry lookup/validation `RuntimeException` is no
longer converted into that rejection: the Source Position remains pending for
retry (and the Owner replay gate can fence the local Store).  This matches the
main design's Oxia rule that transient or unproven target-registration absence
must stop at the position.  `DelayShardTest.controlRegistrationAuthorityFailureRetainsSourcePositionForRetry`
covers the local boundary; Oxia response classification and durable source
replay remain external release gates.

The receipt-bound payload facade now distinguishes a caller-visible missing or
foreign reservation from a local shard/adapter binding failure.  A missing
reservation, shard mismatch or service-owned receipt mismatch remains the
non-enumerating `NOT_FOUND_OR_NOT_AUTHORIZED` branch, while a RocksDB read,
local Object Store registration or receipt-projection exception is projected as
closed `INTEGRITY_ERROR` rather than being misreported as object absence.
`EmbeddedDelayServiceTest.payloadFacadeMapsLocalReservationBindingFailureAsIntegrityError`
covers a pinned trust-set mismatch in the deterministic adapter.  Real
provider/credential unavailability still belongs to the external adapter's
`OBJECT_STORE_UNAVAILABLE_RETRYABLE` branch.  The facade also rejects a
negative observation time as `INTEGRITY_ERROR` before either local or external
Object Store authority; the negative-time branches are covered by
`EmbeddedDelayServiceTest.payloadClientWithoutLocalObjectStoreReturnsTypedRetryableOutcome`
and `InMemoryPayloadObjectStoreTest.negativeObservationTimeReturnsTypedIntegrityOutcome`.

The embedded Message Query bridge now keeps the closed response union intact
when a durable snapshot/read or caller-supplied binding/DLQ projection cannot
be proven: those failures return `INTEGRITY_ERROR`, while a cross-shard
message identity remains `RECEIPT_MISMATCH`.  The regression
`EmbeddedDelayServiceTest.messageQueryMapsPublicProjectionDriftToClosedIntegrityError`
covers both public query overloads; null Message IDs now return
`INVALID_RECEIPT` through both the local and `CompletionStage` entrypoints; the
projector's direct throwing seam stays an internal validation boundary.

The Command Query bridge now applies the same closed-union rule to its local
POSITION/result reads: a null receipt returns `INVALID_RECEIPT`, while a
RocksDB/read or projection exception returns `INTEGRITY_ERROR`; a proven
command/position/hash mismatch remains `RECEIPT_MISMATCH`.  The null-receipt
regression is included in `EmbeddedDelayServiceTest.embeddedQueryUsesQueuedReceiptAsSourceBarrier`.

The Control Operation Query bridge now closes the same public boundary: a null
receipt or negative observation time returns `INVALID_RECEIPT`, while an
authority read or response-binding exception is projected as `INTEGRITY_ERROR`.
Complete receipt identity, fixed retention expiry and CURRENT/error branches
remain unchanged.  The local regressions are covered by
`ControlOperationAuthorityTest` and the control-query assertions in
`EmbeddedDelayServiceTest.embeddedControlOperationEntryPointsPreserveReceiptBoundCas`.

The uncertain-Store drain path now applies the source/scheduler stop fence even
when a caller started native Store close before the coordinator observed the
unproven write boundary.  External close is not evidence of source quiescence:
the coordinator fences the local Owner, invokes `stopSourceAndScheduling` once,
and retains that completion across close/release retries.  The normal
`ACTIVE_FOR_COMMANDS -> DRAINING` path records the same completion before its
authority CAS, so a later close/release retry cannot invoke the callback again.
The focused
regression is
`OwnerDrainCoordinatorTest.uncertainStoreWithExternalCloseStillStopsSourceBeforeRelease`.
The retry-count assertion is also covered by
`OwnerDrainCoordinatorTest.storeCloseFailureLeavesDrainingStateForRetryableTeardown`.
This is local ordering evidence; production source quiescence and Oxia lease
authority remain release gates.

The local Owner replay gate now applies the same fencing rule to the pure
System Mutation path as to Command and mixed replay: a `RocksDbWriteFailure` or
fatal `Error` from delegate application moves the Owner to `FENCED` before the
failure escapes, without advancing the source cursor or `lastCatchupPosition`.
`OwnerLeaseTest.fatalSystemMutationReplayFencesOwnerAndRetainsSourceCursor`
covers a fatal control-registration boundary. This is local fail-closed evidence;
fresh Store-incarnation recovery, source continuity and Oxia ownership remain
release gates.

The replay successor boundary now distinguishes a deterministic continuity gap
from an unavailable or malformed continuity proof. `SourceReplaySuccessor`
emits a closed `SourceReplayGapException` for a wrong-source, regressed,
conflicting or non-successor position; `OwnedDelayShard` projects that proof as
`FAILED(SOURCE_GAP)` with the registered `ShardFailureReason`, retains the
offending source record and refuses further catch-up. Other validation or
canonical-bounding failures fence the local Owner instead of leaving it in
`CATCHING_UP`. `OwnerLeaseTest.v1CatchupPinsTheAdapterSuccessorAndRejectsAKafkaGapBeforeApplyingIt`
covers the command replay path; the mixed/type-specific paths use the same
central validation helper. Durable Oxia shard-status/reason publication and
production source continuity remain release gates.

The same activation fence now covers both embedded and authoritative activation:
after the owner-open metadata/requeue phase begins, a Store/recovery
`RuntimeException` or fatal `Error` (including an owner-epoch metadata
regression) fences the local Owner before the Oxia `ACTIVE_FOR_COMMANDS` CAS can
run. `OwnerLeaseTest.activationMetadataIntegrityFailureFencesBeforeAuthorityCas`
covers the persisted newer-epoch case. Missing activation prerequisites such as
an unreached source barrier still remain `CATCHING_UP`; they are not Store
activation failures. External lease/session and recovery authority remain
release blockers.

The uncertain-Store drain branch now fences the local Owner before invoking the
source/scheduler stop callback. A callback `RuntimeException` or fatal `Error`
therefore cannot leave an unproven native commit boundary behind an
`ACTIVE_FOR_COMMANDS` gate; the exact Store/lease teardown remains retryable.
`OwnerDrainCoordinatorTest.uncertainStoreFencesBeforeStopCallbackFailureCanEscape`
covers this ordering. This is local fail-closed evidence; source quiescence,
Oxia release and fresh-incarnation recovery remain release gates.

The drain durability boundary now treats a native/JNI/runtime/fatal failure from
`ShardStore.flushAndSync()` as an unproven Store outcome. The Store marks
`writeOutcomeUncertain` before propagating the failure, so a retry cannot reuse
the same incarnation for source or ownership decisions; `ShardStoreTest`
`flushAndSyncFailureFencesStoreUntilReopen` covers the local fence. The
coordinator keeps the shard `DRAINING` and the next retry performs only the
uncertain-Store fence/close/exact-lease-release path. Real WAL/device durability,
source quiescence and fresh-incarnation recovery remain release gates.

The process-local `CheckpointScheduler` now saturates a next-due epoch at
`Long.MAX_VALUE` when interval/jitter/completion arithmetic would overflow, then
clears the exact completed claim. `CheckpointSchedulerTest`
`completionAtEpochBoundarySaturatesWithoutStrandingClaim` covers the boundary;
without it, a valid near-epoch-end completion could leave an in-flight claim that
no later callback could release. This remains only a local scheduling safeguard;
durable Upload Intent/Catalog and checkpoint timing evidence remain external.

The local Kafka receipt and Pulsar Attempt Journal mapping-before-send seams now
fail closed when an injected target sender returns a `null CompletionStage`.
That malformed result is not a non-persistence proof: the exact durable mapping
remains the unresolved lower-sequence fence, and the caller receives a typed
Journal integrity/divergence failure instead of an unclassified `NullPointerException`.
`KafkaReceiptJournalTest.nullTargetStageFailsClosedAndKeepsTheMappedAttemptUnresolved`
and
`PulsarAttemptJournalTest.nullTargetStageFailsClosedAndKeepsTheMappedAttemptUnresolved`
cover the boundary. Real target ownership, Broker evidence and subsequent
UNKNOWN/outcome mutation remain release gates.

The physical publish wrapper now also releases a reservation when its injected
Adapter executor synchronously throws a fatal `Error` before accepting the
delegate task, then rethrows that failure. This is the same pre-ownership
boundary as ordinary executor rejection; it cannot strand an active request
charge and does not manufacture a target-side `UNKNOWN`. The focused regression
is `BoundedDestinationPublishAdapterTest.fatalExecutorRejectionReleasesPhysicalChargeBeforeRethrowing`.
The wrapper also records whether the submitted task has started, so an inline
or custom executor cannot be mistaken for pre-ownership rejection when a
delegate/registration `Error` escapes after task acceptance: that path retains
the physical charge as zombie/in-flight. The regression is
`BoundedDestinationPublishAdapterTest.inlineDelegateFatalFailureRetainsPhysicalChargeAfterTaskWasAccepted`.
The release observer is now installed before `Executor.execute`: if a custom
executor runs the task and then throws a fatal `Error` while returning from
`execute`, a later delegate completion still releases the retained zombie
charge instead of leaving an unowned reservation behind. The regression is
`BoundedDestinationPublishAdapterTest.executorFatalAfterAcceptedTaskRetainsUntilDelegateCompletion`.
When a delegate completion is observed, the wrapper now releases the physical
reservation before completing the logical `PublishCall` outcome. This keeps
`activeRequests`/byte accounting drained at the point callers observe a
completed result, including the callback-registration race where the callback
was installed before registration reported failure. The ordering is covered by
`BoundedDestinationPublishAdapterTest.blockingDelegateCallDoesNotBlockHealthyLane`;
the release remains idempotent for the already-armed outcome observer.
Production executor admission and target ownership evidence remain external
release gates.

The local checkpoint reaping and resource-GC safety predicates now also treat a
fatal `Error` while reading catalog/Floor/RecoveryPin authority as unavailable
protection state. They return the existing fail-closed decisions instead of
allowing a caller to interpret a broken authority boundary as safe to reap or
compact. `CheckpointReapingGuardTest.failsClosedWhenCatalogReadThrowsFatalError`,
`CheckpointReapingGuardTest.failsClosedWhenRecoveryPinReadThrowsFatalError`,
`ResourceGcGuardTest.checkpointGcFailsClosedWhenRecoveryFloorReadThrowsFatalError`,
`ResourceGcGuardTest.checkpointGcFailsClosedWhenFloorCoverageThrowsFatalError`
and `ResourceGcGuardTest.checkpointGcFailsClosedWhenRecoveryPinReadThrowsFatalError`
cover the local boundary. Oxia CAS, provider ownership and quiescence remain
external release gates.

The shard-local identity-reclamation seam now has an explicit compact branch:
`DelayShard.retireMessageIdentity(...)` removes every bounded terminal
generation and local DLQ export for a fully terminal Message in one
WriteBatch, then retains the same `id_cf/MESSAGE` key as a type-1
`RETIRED_IDENTITY` payload. `getMessage` and all activation/rebuild/close
scans validate and skip that branch, `getRetiredMessageIdentity` exposes it
for the bounded query path, and `EmbeddedDelayService` returns
`IDENTITY_RETIRED`. A first Schedule with the same identity remains
`DELAY_MESSAGE_ID_CONFLICT`; after the tombstone is deleted, the local
closed-ingress freshness check can return `DELAY_MESSAGE_ID_EXPIRED`. The
conservative `compactRetiredMessageIdentity` helper requires the closed
source fence and a Recovery Floor coverage proof, but does not claim the
external Route identity-retention policy, Oxia CAS, provider quiescence or
full production GC authority. Because this embedded implementation has no
separate Route catalog, its local compatibility horizon is checked addition
of UUIDv7 time plus `DelayShardConfig.maxMessageLifetimeMs`; production Route
policy and UUID age/future-skew evidence remain release gates. Focused local
evidence is `RetiredMessageIdentityRecordTest` and
`EmbeddedDelayServiceTest.retiredMessageIdentitySurvivesQueryAndFreshProcessReopen`,
plus the positive fence/Floor/delete/reuse boundary in
`DelayShardTest.retiredMessageIdentityCompactsOnlyAfterFenceAndFloorThenExpiresOldId`.

After `a0d97cc`, this local evidence can no longer be mistaken for a production
GC authority. `retireMessageIdentity`, `compactRetiredMessageIdentity`,
`compactResourceDeleteConfirmation` and `retireLaneWithTerminalGuard` are
package-local to the runtime algorithms/tests. A reflection regression requires
all four named methods to exist and remain non-public; the only cross-package
fixture uses a test-classpath bridge that is absent from the main artifact. The
complete `DelayShardTest` and `EmbeddedDelayServiceTest` suites, compilation and
Checkstyle passed. Production Route retention, Oxia CAS/session, provider
ownership/quiescence, grant release and Recovery-Floor orchestration remain OPEN;
no strict replacement coordinator is claimed by this visibility change.

After `ce21a99`, the recovery-only READY rewrite is also hidden from cross-package
production composition. `DelayShard.rebuildReadyIndexes()` remains a package-local
deterministic repair/test algorithm, and the existing reflection regression now
requires this fifth method to remain non-public. Complete `DelayShardTest`,
compilation and Checkstyle passed. The READY key/value/timeline integrity checks are
unchanged; a future public repair coordinator still needs fenced lifecycle,
Owner/Oxia authority and record/actual-byte/elapsed I/O admission.

After `ae1224f`, three raw local mutation overloads are no longer cross-package
production APIs: single-Claim `revokeClaim(claimId, ownerEpoch)`, reservation expiry
materialization by reservation ID, and Lane-close materialization by Lane ID. They
remain package-local algorithms for drain/recovery/runtime tests. Cross-package
reservation and Lane-close paths retain only the exact-candidate overloads used by
their strict Owner/work-class wrappers, including stale cursor/projection rechecks.
The visibility regression covers all three exact signatures; complete
`DelayShardTest`, both strict materialization suites, compilation and Checkstyle
passed. This does not yet hide the exact-candidate primitives themselves or replace
the documented embedded drain/recovery compatibility seams.

After `03b6031`, eleven additional authority-free mutation helpers are
runtime-package-only: Lane gate CAS; class-3/4/5/6 and system-writer reserve/release;
Attempt Journal mapping, retirement-pending and retirement acknowledgement; and the
direct Publish Admission, UNKNOWN outcome and verified-published outcome methods.
Their source-ordered handlers and runtime tests retain local access, while the main
artifact can no longer let cross-package Worker code substitute these WriteBatches
for Control/Publish source authority, fenced adapter single-writer execution or
immutable capacity-grant admission. Signature-level visibility regression, complete
`DelayShardTest`, compilation and Checkstyle passed. Production coordinators for
adapter journal authority and dynamic reserve charging remain OPEN.

After `581faba`, the raw Lane runtime-readiness setter is also package-local.
No main-source Lane activator/capability coordinator currently exists, so leaving
`updateLaneReadiness` public would incorrectly imply that any cross-package Worker
could establish `READY` without pinned Profile/capability/credential generations,
channel fencing, evidence barrier, Owner authority or event-loop admission. The
non-public signature is covered by the shared reflection regression; ownership
fixtures use a test-classpath-only bridge absent from the main artifact. Complete
`DelayShardTest`, the affected Owner/Claim/Admission suites, compilation and
Checkstyle passed. The production Lane activator remains an explicit release blocker.

The local command projection now preserves the pinned timeline action boundary
across `RESCHEDULE`. Apply-time validation and the later persistence
normalization derive the new generation's `actionAt` from the prior runtime
projection (or its pinned Profile binding), build the matching DUE/ORDERED key,
and write the corresponding `TimelineWorkRef` instead of letting the
compatibility constructor reset `actionAt` to `deliverAt`. This covers the
same-`deliverAt` early-action case with
`DelayShardTest.reschedulePreservesPinnedActionAtInPersistedRuntimeProjection`;
the main design's business-visible `deliverAt` boundary remains unchanged.

The same action boundary is now retained when a generation leaves the timeline
projection for `PUBLISHING`/`UNCERTAIN` and later returns to a retry timeline.
Those runtime branches do not carry a current `TimelineWorkRef`, so the local
rebuild scans the bounded open-attempt ledgers and decodes the canonical
`PublishAdmission` descriptor as the immutable source of `actionAt`; legacy
opaque ledgers remain on the ordinary compatibility path. Conflicting or
mismatched canonical Admission timing fails closed. The regression
`DelayShardTest.uncertainRetryPreservesPinnedActionAtWithoutProfileCatalog`
covers a V1 early-action Schedule, canonical Admission projection, uncertain
retry, and fresh-process reopen. This is shard-local evidence; production
Profile/Admission authority and Broker handoff certification remain release
gates.

Claim rollback now uses the Claim's retained `sourceTimelineWork` as the
replay-stable timeline projection whenever a `CLAIMED` message returns to
`SCHEDULED`. Lane Pause/Close and capacity-gated `PUBLISH_ADMISSION` share the
same checked restore helper as explicit `revokeClaim`, preserving the pinned
`actionAt`, retry gate, work kind, candidate attempt and uncertain-retry
authority even when the current Worker has no Profile Catalog or resolver.
The shared `actionAt` resolver also consults a live Claim before open-attempt
or Profile fallback, so evidence settlement that converts a claimed uncertain
retry into a definitive retry cannot depend on an opaque older ledger.
The helper rejects a source-work/key/retry-gate mismatch before the batch, and
legacy Claims without a source projection retain only their documented
compatibility fallback. `DelayShardTest.sourceOrderedLanePauseRestoresClaimPinnedActionAtWithoutResolver`
covers the early-action Pause path; real Owner/Lane control authority and
cross-process replay remain release gates.

The certified early-Pulsar handoff projection now has a narrow evidence-binding
fence: before a verified `PULSAR_SEND_ACK` can project local `HANDED_OFF`, its
target resource, physical partition and prepared hash must match the retained
`PublishAdmission` channel and prepared hash byte-for-byte. A mismatch remains
`STALE_SYSTEM_MUTATION`; ordinary `PUBLISHED` and opaque legacy compatibility
paths are unchanged. Local regression evidence is
`PublishEvidenceV1Test.certifiedPulsarHandoffBindsTargetPartitionAndPreparedHashToAdmission`;
real Broker ACK authentication and visibility-guard responsibility remain
release blockers.

The owner activation seam now persists the current `ownerEpoch` into the
Store's monotonic `lastOpenedOwnerEpoch` metadata before either the embedded
or authoritative path exposes `ACTIVE_FOR_COMMANDS`. A failed metadata or
Claim-requeue WriteBatch fences the local owner and leaves the source cursor
unchanged; an authority CAS that is lost after the marker write remains
conservative for the next owner. `OwnerLeaseTest` verifies the authoritative
activation marker. This closes the local owner-open metadata boundary only;
Oxia lease/session CAS, recovery-pin transaction and source replay authority
remain release blockers.

`OwnedDelayShard.activateForCommandsWithControlSnapshot(...)` now provides the
strict V1 activation entrypoint: it requires the exact shard-bound
`CompatibleControlSnapshotV1` to already be persisted at `meta/FIXED` key 10
before the local gate (and, in the authority overload, the same Owner Lease
CAS) can expose `ACTIVE_FOR_COMMANDS`. The older activation overloads remain
embedded compatibility seams and do not prove the control prerequisite;
`OwnerLeaseTest.strictActivationRequiresThePersistedShardControlSnapshot`
covers the missing-then-matching boundary.

`ShardStore.createCheckpoint` now guarantees that every physical checkpoint
image carries a fresh non-zero 16-byte `checkpointId`, including the embedded
convenience overload that previously allowed a missing identity. The identity
is persisted before RocksDB snapshots the files and is restored from the
copied image; callers that need response-loss retries must use the explicit-ID
overload and reuse the same bytes. `ShardStoreTest.convenienceCheckpointAllocatesIdentityBeforeSnapshot`
covers the local image/restore boundary. This does not replace the external
Oxia upload-intent/catalog CAS or Object Store publication protocol.

`LaneScheduler.register(existingLane)` now recomputes the process-local
deficit cap after an existing Lane's scheduler weight changes and clamps any
historical credit to the largest currently registered Lane increment. This
keeps a weight downgrade from leaving an idle Lane above the V1 capped-DRR
bound. `LaneSchedulerTest.weightDowngradeRecomputesDeficitCapAndClampsExistingCredit`
covers the deterministic `weight=8 -> weight=1` transition; the outer Worker
scheduler already applies the same recomputation rule. This remains local
scheduler evidence and does not replace the certified capacity artifact or
production placement telemetry.

The local Kafka receipt appender now computes the checked exclusive successor
before advancing its cursor. At the raw u64 all-ones offset, the append fails
with the stable Journal integrity error and the cursor remains exhausted,
instead of wrapping to offset zero after a failed `ReceiptPosition` build.
`KafkaReceiptJournalTest.localAppenderFailsClosedBeforeUnsignedOffsetExhaustionCanWrap`
covers the repeated-failure boundary. This is local seam evidence only; the
production Kafka receipt partition, read-committed LSO and retention proofs
remain external release blockers.

The deprecated `PulsarActivationBarrier` compatibility constructor now actually
supports its documented unknown-batch-shape form: a non-empty legacy barrier
may carry `batchSize=0`, skips only the same-entry batch-shape check, and still
fences the shard, physical resource, topic and inclusive batch-index boundary.
V1 source adapters must continue to use the full constructor with a positive
batch size. `SourceActivationBarrierTest`
`legacyPulsarBarrierAllowsUnknownBatchShapeWithoutWeakeningIdentityFence`
covers the compatibility path; this does not replace production Pulsar source
assignment or Broker guard evidence.

The generic `BoundedDestinationPublishAdapter` now treats a delegate that
throws synchronously or returns no `CompletionStage` as an unobserved physical
operation. It returns logical `UNKNOWN` while retaining the request/byte charge
as `ZOMBIE`/in-flight until the caller's `PublishCall` receives certified
completion or fenced-teardown release; only a closed gate or executor
rejection is a pre-ownership release. `BoundedDestinationPublishAdapterTest`
`failedOrNullDelegateStageRetainsChargeUntilExplicitRelease` covers both
unobserved branches. The physical-admission gate also reserves the complete
potential-zombie request/byte envelope: a candidate is rejected with
`ZOMBIE_CAPACITY` before transport when all currently active charges plus that
candidate could not simultaneously become zombies. Request and byte boundaries
are covered by `DestinationPhysicalAdmissionTest`'s
`admissionReservesZombieRequestCapacityForAllOutstandingRequests` and
`admissionReservesZombieByteCapacityForAllOutstandingRequests`. This local
wrapper now also binds its Worker accounting identity: after `f4899d47`, only
the four-argument cross-package constructor is public, and it requires the
shared `WorkClassExecutionRegistry`, exact `DestinationPhysicalAdmission` and
caller-owned executor. A Worker registry accepts only one exact physical pool;
the no-registry constructors are adapter-package test seams. Reflection and
foreign-pool construction regressions prove rejection before transport or
charge. This local wrapper evidence does not replace the production adapter's ownership,
cancellation or teardown attestation. An asynchronous delegate or callback
registration `Error` now completes the logical call as `UNKNOWN`, retains the
reservation as `ZOMBIE`/in-flight, and then rethrows the fatal failure so the
executor/process supervisor still sees it; the regressions are
`BoundedDestinationPublishAdapterTest.asynchronousDelegateErrorCompletesUnknownBeforeFatalFailureEscapes`
and `BoundedDestinationPublishAdapterTest.asynchronousCallbackRegistrationErrorCompletesUnknownBeforeFatalFailureEscapes`.
The same wrapper distinguishes an executor that rejects before task start from
an inline/custom executor that throws after task acceptance; the latter keeps
the physical charge fenced even though `submit` rethrows the fatal failure.
`BoundedDestinationPublishAdapterTest.inlineDelegateFatalFailureRetainsPhysicalChargeAfterTaskWasAccepted`
covers that accepted-task boundary.

The deterministic `InMemoryOwnerLeaseStore` now applies the same monotonic
renewal fence as `OxiaOwnerLeaseStore`: a valid lease renewal cannot move the
live expiry backwards, and a rejected shorter renewal leaves the current lease
unchanged. `OwnerLeaseTest.renewalCannotMoveTheLiveExpiryBackwards` covers the
local CAS parity; Oxia session/ephemeral-record authority remains an external
release gate.
The same in-memory CAS now rejects a renewal carrying a stale lifecycle state
after a concurrent `ACQUIRING -> RESTORING` transition, so renewal cannot
rewrite the current state backward; `OwnerLeaseTest.renewalCannotRewindAConcurrentLifecycleTransition`
covers the fence. Persistent local projection and Oxia production CAS keep the
same state-preservation requirement.

Open publish-attempt lookup and listing now bound Attempt-only scans by the
maximum of `maxPendingMessages` and `maxOutcomeReserveRecords`. This preserves
the V1 case where one Message owns multiple unresolved Attempt ledgers; using
only the message count could fence a valid shard during admission/outcome
recovery. `DelayShardTest.openAttemptLookupUsesOutcomeReserveBoundInsteadOfMessageBound`
covers three live ledgers with one pending Message slot. Mixed Claim+Attempt
scans, activation runtime-index reconciliation and Lane-retirement proof use
matching conservative bounds, and external producer/evidence authority is
unchanged.

The local queued-receipt adapter seam now has a strict Route-policy path:
`QueuedReceiptQueryPolicy` derives `receipt_query_until` only as checked addition
of the authenticated Broker persistence time and the immutable policy window.
`PolicyBoundWireCommandIngressAdapter`, the pinned Kafka/Pulsar ingress adapters,
`PreparedSubmissionAdapter` and the embedded facade reject an absent or mismatched
policy before transport ownership; a post-persistence overflow or malformed
projection remains `ENQUEUE_UNCERTAIN` with an integrity diagnostic. The older
absolute-boundary overloads remain a compatibility seam for existing callers and
are checked against a bound policy when one is present; they are not the strict
V1 client contract. `AdapterIngressTest` and `NativeSubmissionAdapterTest`
cover policy derivation, overflow and managed-branch binding. Route policy
publication, source-time authority and production adapter wiring remain external
release gates.

The local command-result query seam now has the matching strict retention path:
`CommandResultRetentionPolicy` derives `full_result_retain_until` from the
applied result Source Position's Broker persistence time with checked addition.
`EmbeddedDelayService`, `DelayClient` and `BoundedLocalQueryProjector` expose
policy-bound query, await and applied-receipt overloads; a policy overflow is an
integrity error rather than a wrapped or caller-selected deadline. Existing
absolute-boundary overloads remain compatibility-only. The focused evidence is
`EmbeddedDelayServiceTest.embeddedQueryDerivesFullResultRetentionFromAppliedSourceTime`
and `CommandResultRetentionPolicyTest`; external retention-policy publication,
source-time authority and production query routing remain release gates.

The embedded Control registration seam now has the corresponding strict policy
entrypoint: `ControlOperationQueryPolicy` must match the nonzero
`controlQueryPolicyVersion` frozen in `PreparedControlOperationV1`, derives
`queryUntil` from `registeredAt.latest` with checked addition, and performs that
derivation before target registration. Policy-version drift or overflow fails
closed without a partial local registration. The existing absolute-window
overload remains a compatibility seam, but it now constructs and validates the
same receipt/current projection before publishing the target registration, so a
negative window or invalid registration evidence cannot leave a target-only
record. Immutable policy distribution and the production Oxia registration
transaction remain external release gates. Focused evidence is
`ControlOperationQueryPolicyTest`,
`EmbeddedDelayServiceTest.strictPreparedControlRegistrationRejectsPolicyDriftAndOverflowBeforeRegistration`
and
`EmbeddedDelayServiceTest.compatibilityPreparedControlRegistrationValidatesBeforeTargetRegistration`.

The command-result wire constructors now enforce the lower retention bound as
well: `fullResultRetainUntilEpochMs` must not precede the result Source
Position's Broker persistence time. This guard applies to
`CommandAppliedReceiptV1`, `PublicCommandResultV1` and
`CompactCommandResultV1`, so direct construction and decoded malformed values
fail closed instead of representing an impossible retained result. Focused
coverage is in `ProtocolCodecTest`, alongside the existing policy-derived
upper-bound checks; this is local wire-integrity evidence and does not replace
external source-time authority or retention-policy publication gates.

The embedded compatibility query overload now applies the same lower-bound
fence before projecting a result. A caller-supplied absolute retention boundary
that precedes the applied Source Position's Broker persistence time, or a
malformed local result/projection, returns typed `INTEGRITY_ERROR` rather than
leaking a constructor exception; strict policy-bound queries continue to derive
the boundary from source time. `EmbeddedDelayServiceTest` covers the malformed
absolute-boundary path. This remains local fail-closed behavior, not production
query routing or retention authority.

The local Kafka transactional receipt journal now defines mapped-record replay
idempotence by canonical `ReceiptPosition` bytes rather than Java record object
identity. A reconstructed position carries a copied receipt-hash array, so the
generated array `equals()` path could falsely classify an exact decoded replay
as a mapping conflict. `KafkaReceiptJournalTest`
`reconstructedMappedRecordReplayUsesCanonicalPositionBytes` covers the exact
reconstructed replay. This is local journal evidence only; Kafka
`read_committed`/LSO/retention proof and transactional Broker authority remain
release blockers.

The Pulsar Attempt Journal applies the same canonical replay rule to both
`MAPPED` and `RETIRED_NOT_PUBLISHED` records. Reconstructed `JournalPosition`
values are compared by their exact canonical bytes, so response-loss replay
cannot depend on Java object identity. `PulsarAttemptJournalTest`
`reconstructedMappedAndRetirementReplayUsesCanonicalPositionBytes` covers both
branches. This remains a local journal seam; the Nereus-owned Pulsar topic,
ExclusiveWithFencing writer and Broker evidence authority remain release
blockers.

The local Pulsar Journal appender now treats raw u64 all-ones as the final
usable `entryId`: it writes that entry once and then remains permanently
exhausted. A subsequent retirement fails with typed `INTEGRITY_ERROR` before
the Journal state or record list changes, rather than allowing an internal
entry cursor to wrap toward zero. `PulsarAttemptJournalTest`
`localAppenderFailsClosedAfterUnsignedEntryExhaustionWithoutWrapping` covers
repeated failure after exhaustion. This is local appender evidence only; the
real Pulsar Journal position and Broker durability authority remain release
blockers.

The generic bounded destination adapter now treats a `CompletionStage` that
returns `null` from `whenComplete(...)` (or from the fallback future view) as
an unobserved physical operation. It returns `UNKNOWN`, marks the reservation
as `ZOMBIE`, and keeps the request/byte charge until explicit physical release
instead of leaving the logical outcome pending forever. `BoundedDestinationPublishAdapterTest`
`nullWhenCompleteReturnIsTreatedAsUnobservedCompletion` covers this malformed
callback-registration boundary. This is local wrapper evidence only; a real
destination adapter must still provide Broker completion or teardown proof.

Pulsar Journal resolution now also fails closed after a local retirement marker:
`RETIRED_NOT_PUBLISHED` does not bypass the Broker last-sequence observation or
the inactivity-horizon plus producer-snapshot retention predicates. A late
Broker sequence at or above the retired mapping is classified as
`PULSAR_EVIDENCE_DIVERGENCE`, while a lower/absent sequence is `NOT_PUBLISHED`
only with both proofs. `PulsarAttemptJournalTest`
`retiredMappingStillRequiresRetentionProofAndFencesLateBrokerPublication`
covers these branches. This is local evidence classification; real Broker
sequence, fencing and retention authority remain release blockers.

The source-ordered `PUBLISH_OUTCOME(UNKNOWN)` projection now verifies the
current Message Lane identity, durable Lane presence, and exact Lane
incarnation against the `PUBLISHING` attempt ledger before constructing any
READY projection. Missing or drifted Lane state fails closed before the
WriteBatch, so `LaneRecord.initial(...)` cannot resurrect a corrupt/misplaced
Lane and the Message, attempt, quota, and source position remain unchanged.
`DelayShardTest.unknownOutcomeFailsClosedWithoutRecreatingAMissingLaneProjection`
covers the missing-record regression. This is local Store-integrity evidence;
it does not replace the external Owner/source/evidence recovery gates.

The same missing-Lane fence now covers existing-message `RESCHEDULE` and
`CANCEL`, plus large-payload `COMMIT`: each path requires the durable Lane
before it can construct a result or enter the generic quota/READY projection.
Missing Lane state throws before the WriteBatch, so the Message or Reservation,
quota and source position remain unchanged rather than being silently repaired by
`LaneRecord.initial(...)`. Focused regressions are
`DelayShardTest.rescheduleFailsClosedWithoutRecreatingAMissingLaneProjection`,
`DelayShardTest.reservedPayloadCancelFailsClosedWithoutRecreatingAMissingLaneProjection`
and `DelayShardTest.largePayloadCommitFailsClosedWithoutRecreatingAMissingLaneProjection`.
This closes a local Store-integrity boundary only; production ownership, replay
and external authority evidence remain release gates.

The same durable-Lane requirement now guards source-ordered existing-obligation
transitions: definitive/retry `PUBLISH_OUTCOME`, `EVIDENCE_RESOLUTION`,
`RESOLVE_UNCERTAIN`, `CLAIM_RESULT` and `EXPIRE_GENERATION` fail before a
stale-result, quota/READY projection or source-position write can hide a missing
Lane. Canonical V1 publish ledgers also require the exact Lane incarnation;
legacy opaque ledgers retain only the historical incarnation compatibility seam.
The representative regression is
`DelayShardTest.notPublishedOutcomeFailsClosedWithoutRecreatingAMissingLaneProjection`;
the existing UNKNOWN/Cancel/Reschedule/Commit regressions cover the adjacent
branches. This remains local Store-integrity evidence, not external recovery or
Broker authority evidence.

The embedded Admission seam now validates every canonical V2 attempt ledger
before its `PUBLISHING` WriteBatch: the retained body must match the ledger's
attempt, generation, message, Claim, Lane/Lane incarnation, Owner/Store,
prepared hash and attempt number; the owner generation and Message timing are
checked too, and the body Lane incarnation must match the current durable
Lane. `DelayShardTest.canonicalAttemptLedgerRejectsStaleLaneIncarnationBeforePersistence`
covers the stale-Lane regression. Legacy opaque V1 ledgers remain a bounded
compatibility path and are intentionally not locally upgraded without an
authoritative source-ordered Admission replay. This is local ledger-integrity
evidence only; external source, Owner and catalog authority remain release
gates.

The local Lane quota projection now scans the complete `(LaneId, LaneIncarnation)`
identity tuple instead of stopping at the first foreign incarnation for a Lane ID.
This preserves access to a replacement while an older terminal/retired incarnation
is still retained in the Registry-shaped map. The regression is
`LaneQuotaUsageProjectionTest.findsExactIncarnationWhenSameLaneRetainsAForeignEntry`;
the class-3 map's revision coupling and external retirement authority remain
release gates.

The self-routing ID decoder now enforces the complete logical-locator shape:
after the fixed format byte, route incarnation, partition and CRC framing are
parsed, bytes 21--36 must carry UUID version 7 with RFC variant `10`. A valid
CRC no longer makes arbitrary 128-bit logical bytes acceptable as a
`commandId` or `delayMessageId`. `ProtocolCodecTest`
`selfRoutingIdRejectsNonUuidV7LogicalLocatorsEvenWithValidCrc` covers both the
version and variant rejection paths. UUIDv7 timestamp age/future checks remain
Route-policy concerns; this local decoder evidence does not establish the
production preparation-age authority.

The adapter callback fence now also treats a malformed `CompletionStage` whose
`handle(...)` returns null as unobserved transport completion: managed Kafka/Pulsar
ingress and native/managed submission wrappers return the same uncertain branch,
while pinned destination adapters return the internal unobserved marker so the
bounded physical-admission layer retains the zombie/in-flight charge. Focused
regressions are `AdapterIngressTest.kafkaNullHandledStageIsUncertain`,
`AdapterIngressTest.pulsarNullHandledStageIsUncertain`,
`NativeSubmissionAdapterTest.preparedSubmissionWrapperNullHandledStageRemainsManagedUncertain`
and `BoundedDestinationPublishAdapterTest.pinnedAdapterNullHandledStageRetainsPhysicalCharge`.
This is local transport-SPI evidence only; it does not establish Broker-side
completion or non-persistence proof.

The public query-error codec now validates the optional `retryAt` field's
varint wire type before reading its unsigned value. A length-delimited or
fixed-width replacement is rejected as `IllegalArgumentException` before
stable-code projection; `ProtocolCodecTest.queryErrorResponsesKeepClosedResultTagsAndRetryPresence`
covers the malformed-field regression. This is local canonical-wire evidence
only and does not establish gateway routing or external query authority.

The local ownership seam now closes the reversible-Claim part of activation
recovery: `DelayShard.requeueClaimsForRecovery()` performs a bounded complete
`inflight_cf/CLAIMED` scan before a recovered `OwnedDelayShard` opens
`ACTIVE_FOR_COMMANDS`. Each Claim is restored through the existing atomic
timeline/Message/READY/quota WriteBatch with the same semantic work digest and
a checked successor runtime instance; `PUBLISHING`/`UNCERTAIN` obligations are
left for source-ordered outcome/evidence recovery. This is local crash/replay
evidence only and does not claim Oxia lease/session, Source Log successor,
materialization adapter, or external Producer authority.

The local `StoreRecoveryMetadata` projection now preserves the complete
nonzero `uint64` `catalogGeneration` bit pattern, including Java values with
the sign bit set. A high-bit typed Floor generation is persisted in
`meta/RECOVERY` and survives a normal shard DB reopen; the focused
`StoreRecoveryMetadataTest.reopensRecoveryProjectionWithHighBitCatalogGeneration`
regression prevents recovery reuse from failing only because a valid wire
generation was narrowed to a signed non-negative value.

The managed `PreparedSubmissionAdapter` now also handles an asynchronously
exceptional managed `CompletionStage` (and a null stage value) with the same
fail-closed projection as synchronous adapter throws and callback-registration
failures: managed `ENQUEUE_UNCERTAIN` retains the original Prepared Command and
physical attempt, while an invalid attempt remains a local
`INVALID_PREPARED_COMMAND` rejection. `NativeSubmissionAdapterTest`
`preparedSubmissionWrapperExceptionalStageRemainsManagedUncertain` covers this
ownership boundary; the wrapper never leaks an exceptional Future or switches
the prepared branch to native.

The embedded managed-outcome bridge now also converts a queued receipt
projection failure (for example, a query boundary earlier than the authenticated
embedded Broker persistence time) into managed `ENQUEUE_UNCERTAIN` with the
same physical attempt and bounded `INTEGRITY_ERROR` diagnostic. The command may
already be durably admitted, so this path cannot leak a constructor exception
or claim `DEFINITELY_NOT_QUEUED`; `EmbeddedDelayServiceTest`
`embeddedIngressProjectsAllManagedOutcomeBranches` covers the malformed-boundary
regression.

The local `KafkaReceiptJournal` now uses the same raw `uint64` offset contract as
Source Position and evidence codecs: receipt positions and exact receipt matches
retain high-bit offset patterns, ordering and `lastStableOffsetExclusive` checks
are unsigned, and successor allocation crosses the Java sign bit until the
all-ones offset is exhausted. This keeps mapping-before-send, retirement and
contiguous receipt-cursor evidence aligned at the `Long.MAX_VALUE ->
Long.MIN_VALUE` boundary; `KafkaReceiptJournalTest`
`receiptJournalPreservesUnsignedHighBitOffsetsAndOrdering` covers the local
regression. Mapping/producer sequence numbers remain separate bounded local
counters, and this does not claim Kafka broker or transaction authority.

The local `EvidenceCursorV1.sameIdentity` fence now includes the complete
Pulsar Attempt Journal physical identity, not only the resource token: a
different `physicalTopic` or `physicalTopicCreationTimestamp` is an
incomparable replacement stream even when the token, Lane, partition and
generation match. `dominates`, typed Recovery Floor coverage and parent
cursor checks therefore cannot promote a replacement Journal as a successor;
`EvidenceCursorV1Test.pulsarCursorIdentityIncludesPhysicalTopicCreationIdentity`
covers both drift branches. This remains local cursor/restore evidence and
does not claim Broker resource-token or retention authority.

The replay-stable Claim materialization subset is now a shared typed protocol
projection: `PayloadForPublishV1` validates the inline/object union and exact
length/SHA-256, while `ClaimMaterializationV1` validates the two Profile slot
kinds, Broker resource/metadata branch, uint32 partition/generation and timing
fields, and computes the Registry digest. `ClaimResultBody`, `PublishAdmissionBody`
and `PublishAdmissionBody.Descriptor` all reuse these codecs; `ClaimRecord` exposes
the validated projection without performing live catalog or Object Store I/O.
This closes local canonical parsing only. Full materialization ownership,
catalog/profile authority, adapter serialization/size limits and Producer
recovery remain release blockers.

The runtime now also exposes strict `DelayShard.claimForPublishV1`: before the
Claim WriteBatch it binds message identity, generation, delivery window,
timeline `actionAt` and inline/object payload identity to the current
`MessageRecord`. The legacy byte-array Claim primitive is now package-local and
cannot be called as a production API; a test-classpath-only bridge exists only
to construct recovery fixtures. `claimForPublishV1` is therefore the sole
public `DelayShard` Claim-creation entrypoint, while production still enters
through `ClaimHandoffWorkClassExecutor` and `OwnedDelayShard`. Profile/catalog,
adapter and Producer authority remain external gates.

`PublishAdmissionBody` now uses `PreparedPublishDescriptorV1.decode` as its main
descriptor parser, while `Descriptor.value()` exposes the same exact typed
projection including `ReservedPublishMetadataV1`; canonical round-trip,
prepared-hash derivation and reserved identity equality therefore cannot drift
between the body parser and the accessor. This is local descriptor identity
evidence only; channel, Profile/catalog, Adapter and Producer authority remain
external release gates.

The local RecoveryCatalog now also validates a persisted `StoreRecoveryMetadata`
reuse projection against the exact typed current Floor, published base-manifest
identity, parent-hash ancestry and Store Incarnation/install-state identity.
This is a read-only local proof seam; `OxiaRecoveryCatalog` still requires the
production catalog/Floor and Owner Lease/session transaction.

Manifest restore now also validates the staged DB's persisted `meta/RECOVERY`
lineage/base identity (lineage, checkpoint, manifest hash and LOCAL_STORE source
Store Incarnation), observed-Floor lineage and install-state checkpoint identity
against the manifest before install. This closes the local DB/projection splice
boundary; `ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionLineageDrift`,
`ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionCheckpointDrift` and
`ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionManifestHashDrift`
cover the fail-closed paths. Existing DB open also checks install-state/base
checkpoint coherence before writing a new OPEN projection, with
`ShardStoreTest.recoveryInstallStateDriftDoesNotLeaveRocksDbOpen` covering the
native-handle cleanup path. It remains local evidence, not production catalog,
Owner Lease/session or source-replay activation evidence.

The client contract now exposes the bounded V1 query bridge through
`DelayClient.getCommandResult` and `DelayClient.getMessage`. The embedded
implementation delegates to the existing receipt/source-barrier and
caller-supplied binding/evidence/retention projections; it does not claim
cross-Worker routing, tenant authorization, or production query authority.

The same client contract now exposes `prepareLargePayloadCommit`. The embedded
preparation path binds the reservation receipt to the typed proof's message,
reservation, shard, service-owned object identity, payload digest/length,
trust-set version and proof expiry before producing the canonical V1 command;
proof signature verification and source-ordered reservation authority remain
apply-time/production gates.

`DelayClient.issuePayloadUploadHandle` and `attestPayloadUpload` now expose the
deterministic local Object Store seam when an `InMemoryPayloadObjectStore` is
injected into `EmbeddedDelayService`. The facade rereads the exact shard
reservation, registers it in the adapter, and compares the service-owned
receipt before delegating; an unconfigured adapter returns the closed typed
`OBJECT_STORE_UNAVAILABLE_RETRYABLE` branch. Provider credentials, Oxia
protection CAS and remote Object Store authority remain release blockers;
`EmbeddedDelayServiceTest.receiptBoundPayloadFacadeRereadsTheShardReservation`
covers the positive reserve-to-handle-to-attestation path.

`DelayClient` also exposes the strict V1 Schedule/PrepareLarge/Cancel/Reschedule
preparation methods already backed by the Registry-shaped `PreparedCommand`
constructors. They remain zero-I/O and are covered by
`EmbeddedDelayServiceTest.delayClientPreparesStrictV1CommandsWithoutIo`; the
legacy preparation methods remain compatibility bridges and cannot be labeled
as V1 managed submission by themselves. Synchronous preparation validation now
uses `PreparationFailure`, an `IllegalArgumentException`-compatible exception
that carries a canonical `StableErrorV1(stage=PREPARATION)`; malformed legacy
frames cannot be wrapped as V1 managed submissions, and payload-proof/timing
preparation failures retain their stable code. `AutoFastScheduleTest` covers
the typed error and its canonical round trip.

Strict `enqueueV1`/`enqueueBatchV1` now validate the V1 frame before charging
pending bytes or allocating an embedded Source Position; legacy bodies return
the closed `INVALID_PREPARED_COMMAND` definitive outcome. The batch form keeps
the same independent input-order semantics as legacy batch enqueue, covered by
`EmbeddedDelayServiceTest.strictV1IngressRejectsLegacyBodiesBeforeSourceAdmission`.

`DelayClient.awaitAppliedV1` now performs a bounded local wait by validating the
embedded V1 receipt first, draining only a valid pending source prefix, and
re-querying the closed union. Invalid/forged receipts return
`RECEIPT_MISMATCH` without a drain side effect; the evidence is
`EmbeddedDelayServiceTest.awaitAppliedV1DrainsOnlyAfterReceiptValidation`.

The client seam now also exposes `prepareManagedSubmissionV1` and the exact
prepared-branch `submit` operation. Managed submissions are wrapped from the
strict V1 frame before any transport I/O; the embedded fallback validates the
nonzero physical attempt before source admission and returns the managed
`SubmissionOutcomeMessageV1` projection. A caller-injected
`PreparedSubmissionAdapter` owns managed/native transport dispatch when real
adapters are available. Without that adapter, an embedded native branch stays
typed as `AUTO_FAST_PREREQUISITE_UNAVAILABLE` rather than silently selecting
managed; `DelayClient.prepareAutoFast` now supplies the zero-I/O selection
seam: it consumes caller-supplied immutable Profile semantic envelopes, a
signed `NativeCapabilitySnapshotV1`, and a pinned issuer key; only when the
signature, expiry, target/partition, payload/metadata, timing, and direct
authority checks pass does it generate a nonzero native delivery identity and
freeze the shifted Broker timestamp into native prepared bytes. Any native
ineligibility returns the exact strict managed frame. `AutoFastScheduleTest`
covers eligible native, exact managed fallback, independent input-ordered batch
selection, and no Source Position admission. Snapshot issuance, Oxia protection-before-rotation, Broker guard,
Producer ownership, and production response evidence remain external release
gates. The focused evidence is
`EmbeddedDelayServiceTest.managedPreparedSubmissionKeepsStrictBranchAndAttemptFence`.
The embedded facade does not hold its service monitor while invoking an
injected adapter, so a blocked transport cannot prevent the close fence.

The local in-memory Owner Lease authority advances the raw `uint64` epoch
domain through the high-bit boundary and fails only when the all-ones value is
exhausted (`OwnerLeaseTest.ownerEpochSuccessorUsesTheCompleteUnsignedDomain`).
Lease expiry is computed before publishing the next epoch, so a checked time
overflow leaves the epoch allocator unchanged
(`OwnerLeaseTest.overflowingAcquireExpiryDoesNotConsumeOwnerEpoch`).
The candidate `OwnerLease` and assignment/session context are also fully
validated before the epoch/lease maps are updated, so malformed identity input
cannot consume a fencing epoch (`OwnerLeaseTest.invalidAcquireValueDoesNotConsumeOwnerEpoch`).
Production Oxia sequence allocation remains an external authority gate.

The latest protocol pass also preserves the complete raw `uint32` bit pattern
for every currently implemented V1 signing/verifier/proof key version and
verifier version: System Mutation envelopes, Native Capability snapshots,
credential-equivalence attestations, Evidence Verifier/Profile values,
Payload Commit proofs and payload-proof trust-set controls. Java `int` accessors
remain source-compatible, but zero is the only invalid value and canonical
encoding/decoding uses unsigned-bit helpers; trust-set ordering compares these
versions as unsigned values. TIME_FENCE apply uses the same full-range decode
and proof-id preimage. `ProtocolCodecTest`, `CredentialBindingV1Test` and
`PayloadProofTrustSetSemanticV1Test` cover high-bit round trips. This closes the
local codec boundary only; activated key-set membership, rotation and writer
trust remain external release gates.

The same unsigned-bit audit now covers the SLO Registry's `uint64` fields:
objective thresholds/ratios/windows/envelope versions and Final measured
intervals/observation revisions preserve complete raw 64-bit values through
canonical encode/decode, unsigned interval ordering and conservative merge.
The due-admission identity's `uint32 generation` uses the same complete-bit
validation.
`SloObjectiveV1Test.objectiveRoundTripsCompleteUnsigned64BitFields` and
`SloObservationOutboxV1Test.finalRoundTripsAndMergesCompleteUnsigned64BitFields`
cover the high-bit vectors. This closes only the local SLO wire/merge boundary;
`SloObjectiveV1.validateDueCompanion` and the paired Final validation now also
fence a due ALL_ACCEPTED exclusion to the exact closed HEALTHY/ALL_ACCEPTED
companion pair: the ALL_ACCEPTED objective must validate the durable Start
digest, while the HEALTHY objective must match every companion policy field.
The pair-mismatch and legacy-overload rejection vectors are covered by
`SloObservationOutboxV1Test.excludedFinalRejectsAHealthyObjectiveFromAnotherCatalogPair`.
Final merge rejects a different payload that regresses observation revision;
exact-byte replay remains idempotent.
`SloDueAdmissionIdentityV1` now rejects a negative `path_start_epoch_ms`,
exposes the typed path/start fields, and `SloSampleStartV1` requires exact
identity/path and semantic fixed-epoch agreement before it can be persisted;
`SloObjectiveV1Test` covers path/time drift and negative-time vectors. Start
reconstruction, production collector merge/export and production evidence
authority remain release blockers; the local collector now has an explicit
projection capacity envelope. `SloSampleFinalV1.validateAgainst` also binds a
`SUCCESS` Final to the endpoint kind of its closed objective success event:
queued/native handoff require `BROKER_PERSISTENCE`, while internal/barrier/
probe objectives require `TRUSTED_OBSERVATION`; a semantic fixed epoch cannot
be reused as a completion observation. `SloObservationOutboxV1Test` covers the
semantic-start-as-success rejection. This remains an endpoint-kind fence, not
the production Broker/Admission/evidence authority.

The shared `uint32` decoder now rejects any varint outside the unsigned
32-bit domain, including a malformed `uint64` value whose Java `long` view has
the sign bit set; it no longer truncates such input into a valid-looking
partition, key version or target index. Queued receipt decoding applies the
same signed-range check to epoch/timing fields while keeping Pulsar physical
topic creation identity on the raw `uint64` path. `ProtocolCodecTest` covers
both overflow and high-bit timing rejection; the closed Control and Scheduler
projection validators use the same negative-value fence for local `uint32` and
boolean fields.

The canonical Protobuf reader also rejects a length-delimited prefix whose
unsigned `uint64` value has the sign bit set, as well as any length above the
bounded Java `int` payload limit, before narrowing it to an array offset. This
prevents a malformed `2^63`-class length from being truncated to zero and
accepted as an empty payload. `ProtocolCodecTest.canonicalReaderRejectsHighBitLengthPrefixes`
covers this local parser boundary; it does not change the Registry's bounded
length-field semantics or establish an external transport limit.

The five persisted `meta/SCHEDULER` projection codecs now preserve the
Registry's complete raw `uint64` generation/version/deficit bit patterns and
use zero-only checks for nonzero fields; `next_index` remains a bounded local
`uint32`. `SchedulerProjectionsV1Test.schedulerUint64FieldsPreserveCompleteRawBitPatterns`
covers all five projection values. This is wire/reopen evidence only; the
runtime scheduler still operates inside its certified signed Java capacity
envelope.

The Registry-shaped `ActiveLaneStateV1` codec now does the same for its raw
`uint64` lane-control/lane-version/scheduler-weight/failure fields; only zero
is invalid, while its epoch-time fields retain their nonnegative `int64`
semantics. `ActiveLaneStateV1Test.preservesUnsignedLaneVersionWeightAndFailureBits`
covers the high-bit projection. `DelayShard` now recognizes the direct typed
ACTIVE branch on read, rebuild, scheduler projection and quota rebuild paths;
the bounded local adapter is used only to expose fields that the existing
runtime can represent. Typed updates preserve Profile refs, canonical tuple,
certificate/retirement data and the exact projected per-Lane usage, while a
missing BLOCKED reason, READY key/certificate or out-of-range local field fails
closed instead of downgrading the value. `DelayShardTest.typedActiveLaneStateIsReadAndUpdatedWithoutLegacyDowngrade`
covers reopen/read and same-key update. The typed state remains a local
persistence/runtime bridge until full Lane/Profile/Oxia activation is available.
Activation now also requires every typed ACTIVE state's field-14 `lane_usage`
to be byte-equal to the matching `(laneId, laneIncarnation)` entry in the
persisted class-3 `meta_cf/QUOTA` map; a missing map or usage drift fails closed
before the shard becomes active. `DelayShardTest.typedActiveLaneStateRequiresPersistedPerLaneQuotaProjection`
and `DelayShardTest.typedActiveLaneStateRejectsUsageDriftFromPerLaneQuotaProjection`
cover the two rejection branches, while the existing typed reopen/update test
covers the matching branch. This closes the local cross-projection fence; it
does not claim full typed runtime cutover or external revision authority.
The same activation fence recomputes typed `encoded_ready_key` from the exact
Lane ID, `laneVersion` and `nextEligibleAt` and rejects a key whose fields or
gate/readiness branch do not match; the direct constructor fence is covered by
`DelayShardTest.typedActiveLaneStateRejectsReadyKeyDriftAtConstruction`. This
is key-identity evidence only; physical READY existence, certificate authority
and scheduler recovery remain separate gates.
For the typed projection, READY also requires `earliest_action_at` and
`next_eligible_at`; the exact candidate action boundary is refreshed together
with field 16 and the READY key instead of retaining a stale field-15 value,
and discovery rejects a typed state whose times disagree with the current
TimelineWorkRef (`DelayShardTest.typedReadyProjectionRefreshesEarliestActionBoundaryFromCurrentHead`).
The same cross-check is enforced by `PersistentLaneScheduler` recovery, so a
second scheduler path cannot bypass the typed Lane time projection.
The typed constructor also requires the nested ReadyCertificate's Lane ID and
incarnation to byte-match the active state, with the drift regression in
`ActiveLaneStateV1Test.readyRequiresCertificateAndRejectsReadyKeyDigestTampering`.
Its typed `next_eligible_at` projection also cannot precede the retained
action, open-circuit, Lane backoff or executor retry gate; an early value fails
closed in `ActiveLaneStateV1Test.rejectsTupleIdentityAndCircuitInvariantViolations`.
The typed constructor also rejects `READY` paired with any non-`OPEN` admission
gate, before key/certificate projection can reach persistence.
The typed state and terminal-guard constructors now also parse the
Registry-shaped canonical Lane tuple and require exact byte projection of both
immutable Profile slots; malformed tuple structure or Profile id/version/hash
drift fails closed. `ActiveLaneStateV1Test.rejectsProfileProjectionDriftInTypedState`
and `LaneTerminalGuardV1Test.guardRejectsProfileProjectionDrift` cover the two
typed branches. This is local canonical-shape evidence only; resolver/catalog,
Profile activation and Oxia ownership authority remain outside the codec.

`LaneRecord.withGate` now encodes the frozen Lane lifecycle instead of allowing
the generic local gate helper to reopen an ordering-broken or closed Lane:
`OPEN` may pause/break/close, `ADMIN_PAUSED` may resume/break/close,
`ORDERING_BROKEN` may only close, and `CLOSED` may only enter the separate
terminal-guard retirement path. `LaneRecordTest.controlTransitionsAreExplicitAndIrreversibleWhereRequired`
covers the illegal `ORDERING_BROKEN -> ADMIN_PAUSED/OPEN` and
`CLOSED -> OPEN` attempts. Source-ordered control authority and Oxia retirement
remain external release gates.

The nested Registry `ChargeVectorV1` codec now preserves all seventeen raw
`uint64` fields, including high-bit values, through canonical encode/decode.
Before the embedded runtime performs signed capacity or Outcome Reserve
arithmetic it calls the explicit local-range guard; an out-of-range charge is
capacity-gated rather than narrowed or wrapped. `PublishAdmissionBodyTest`
covers the wire round trip and local fail-closed projection. The separate
`CapacityVectorV1` grant representation remains the certified signed local
capacity envelope.

Profile and Retry Policy semantic versions and their reference wrappers now
also preserve the complete nonzero `uint64` bit pattern through semantic-hash
preimages and canonical decode. Equality remains byte/version based; catalog
publication and source-ordered activation authority are still external.
`ProfileSemanticEnvelopeV1Test` and `RetryPolicySemanticV1Test` cover the
high-bit references. `PublishOutcomeBody` and `DlqExportResultBody` reuse the
same strict `RetryPolicyRefV1` decoder, so a high-bit policy reference cannot
be accepted by the catalog and rejected by an outcome parser; the regression
is covered by `PublishOutcomeBodyTest`.
Admission target-partition hashing now feeds the exact raw Destination Profile
version bits into the Registry digest instead of the signed `u64` helper;
`PublishAdmissionBodyTest.hashedPartitionValidationPreservesHighBitDestinationProfileVersion`
covers the boundary, and the Admission projection now reuses the strict
`ProfileRefV1` decoder, including its fixed 32-byte semantic-hash check. This
is local hash/codec evidence only; Profile catalog,
partition assignment and external Broker authority remain release blockers.

The Claim Result replay-stable `ClaimPrecondition` projection now reuses the
same `ProfileRefV1` decoder, preserving complete raw Profile versions instead
of applying a signed nonnegative fence. `ClaimResultBodyTest` covers high-bit
Destination and Delivery-Capability versions; Claim source authority and
external placement remain release blockers.

The same Claim materialization projection now delegates its nested Broker
resource, committed Object Store descriptor and adapter-metadata branches to
their typed codecs. This closes NFC/UTF-8, branch semantics, Object Store
Profile-kind and fixed identity checks that a shallow non-empty-byte validator
could bypass. `ClaimResultBodyTest` covers non-canonical Broker identity and a
committed payload carrying the wrong Profile kind; full Claim materialization
and external Object Store authority remain release blockers.

Claim materialization Profile slots now also enforce the Registry union kinds:
the first reference must be `DESTINATION` and the second must be
`DELIVERY_CAPABILITY`. The same slot fence is applied by both
`ClaimResultBody` and `PublishAdmissionBody`, with negative coverage in
`ClaimResultBodyTest.claimMaterializationRequiresDestinationAndCapabilityProfileSlots`
and `PublishAdmissionBodyTest.rejectsClaimMaterializationProfileSlotKindDrift`.

`PublishAdmissionBody` now applies the same typed nested codecs to both its
Prepared Publish descriptor and Claim materialization projections, so Admission
cannot accept a Broker or metadata branch that Claim Result would reject, nor
an Object Store descriptor with the wrong Profile kind. Existing Admission
cross-object and profile/timing tests remain green; authenticated catalog and
Object Store authority are still external gates.

The business `PublishOutcomeBody` retry decoder now shares the DLQ retry
boundary: it requires the exact `RetryDecisionV1` field sequence and rejects a
deadline before first-attempt or an optional `next_retry_at` outside the
first/deadline interval. Unknown nested fields can no longer be smuggled in by
the fixed-count parser; `PublishOutcomeBodyTest` covers both rejection paths.

Source-ordered business `PublishOutcomeBody` and evidence-resolution apply now
retain the typed policy reference, cause and retry domain from a full
`RetryDecisionV1`. When a source-pinned `RetryPolicyCatalog` is present,
`DelayShard` revalidates the exact policy reference, attempt number, checked
`retryDeadline` and Registry `RETRY_JITTER_V1` (`MESSAGE_PUBLISH`) against the
trusted observed interval before changing durable state. A wrong-jitter
outcome is rejected while the matching decision applies, and the legacy opaque
`UNKNOWN` placeholder remains a compatibility seam. Newly source-applied
canonical Admissions now write a V2 attempt ledger that independently persists
`firstAttemptAt` and `retryDeadline` in the same WriteBatch; the ledger checks
those fields against the canonical Admission and, when available, the pinned
Retry Policy. Catalog-less shards still use the message expiry as their only
safe local deadline and validate the typed fields even without a catalog.
Legacy V1 opaque ledgers remain structurally compatible but cannot be upgraded
without an authoritative Admission replay. Catalog-backed DLQ-domain
revalidation is now local; catalog-less/legacy DLQ handling remains structural,
and external policy publication/authority remains a release blocker.

DLQ `terminalRevision` now preserves complete nonzero `uint64` bits through
`DlqExportRecord` identity/bytes and `DlqExportResultBody` parsing; zero remains
invalid. `DlqExportRecordTest` and `DlqExportResultBodyTest` cover the high-bit
vectors. Local terminal-state arithmetic remains bounded separately, while
external DLQ policy/provider authority remains a release blocker.

`DLQ_EXPORT_RESULT_V1` now also validates the nested `RetryDecisionV1` field
order/presence and decodes its exact `RetryPolicyRefV1` (including canonical
bytes, nonzero raw version and semantic-hash length) before accepting the
result. It also enforces the local first-attempt/deadline interval for an
optional `next_retry_at`, so a structurally valid retry cannot schedule
outside its own decision window. A malformed nested policy reference or
timing interval cannot be reduced to an opaque nonempty byte string;
`DlqExportResultBodyTest` covers both rejection paths. Policy publication,
source ordering and external DLQ authority remain release blockers.

Payload-proof trust-set semantic/ref versions now follow the same full-width
rule, and source-ordered activation compares them with unsigned ordering;
historical key-version ordering remains unsigned as well. The semantic and
control-state tests cover high-bit round trips and version regression fencing.
Both the legacy `PayloadCommitProof` adapter and typed `PayloadCommitProofV1`
now carry the same nonzero raw trust-set version bits through proof IDs,
signatures and canonical decode, so a high-bit trust-set reference cannot be
accepted by the catalog but rejected by payload attestation. The trust-set
semantic test verifies both proof projections through the local verifier
adapter; provider attestation and source-ordered trust authority remain
external.
The legacy `LargeScheduleIntent` reserve path carries that same raw version
through its fixed binary projection and reopen decode.

`ApplyShardControlBody.semantic_version` now follows the immutable semantic
version boundary as a raw nonzero `uint64` through the System Mutation body
validator and typed control-body parser. Its `expected_prior_control_version`
and Lane target control versions remain bounded local CAS values; high-bit
semantic-version coverage is in `PayloadProofControlPayloadV1Test`.

CapacityGrant source versions, QuotaGrant reference versions and
ShardCapacityEnvelope versions now preserve their complete nonzero `uint64`
bit patterns in canonical bytes, nested decoding and quota semantic-hash
preimages. The local `CapacityVectorV1` amount envelope remains intentionally
bounded to signed Java capacity arithmetic; only the independent version
identity fields use the full wire domain. Focused capacity-vector and envelope
tests cover high-bit round trips.

The Registry-shaped per-Lane quota usage entry now also preserves the complete
nonzero `uint64` usage-revision bit pattern through its digest and canonical
codec. This is the map-entry identity/version boundary; quota arithmetic and
same-batch revision coupling remain local signed/runtime and external
authority gates respectively. `LaneQuotaUsageMapV1Test` covers the high-bit
entry round trip.

Checkpoint summary/catalog/control-result projections now preserve nonzero
`uint64 catalog_generation` values as raw bit patterns and sort catalog
summaries with unsigned generation ordering. This closes the public checkpoint
projection codec boundary; Recovery Floor/upload authority and local
generation increment arithmetic remain separate recovery gates.

`CheckpointUploadIntentV1` now preserves the nonzero `base_catalog_generation`
and checked `state_revision` raw `uint64` patterns through its canonical digest
and state branches. The local intent CAS advances `state_revision` with an
unsigned-bit successor and fails only at the all-ones pattern; Object Store and
Oxia catalog authority remain external. Protocol and intent-store tests cover
the high-bit boundary.

Typed `RecoveryFloorRefV1` and session-bound `RecoveryPinV1` now preserve the
nonzero `catalog_generation` raw `uint64` pattern through floor/pin digests and
cross-object equality. The local catalog and Oxia CAS remain the authority for
generation freshness, ancestry and session ownership; their local successor and
response checks now compare catalog generations as unsigned values and stop at
the all-ones pattern. These tests close the local arithmetic/response boundary
as well as the canonical recovery-reference wire boundary; production Oxia
transaction and session authority remain external.

The quota-control `QuotaTransferPlanRefV1` now preserves its nonzero tenant
policy-version `uint64` as a raw bit pattern in the canonical control request
value. Policy authorization and source-ordered transfer authority remain
external; `ControlRequestSupportCodecTest` covers the high-bit round trip.

Lane retirement progress and terminal-guard projections now preserve the
complete nonzero raw `uint64` mutation-sequence pattern through their digests
and canonical codecs. The sequence is only an identity/fencing value here;
the runtime's local source-mutation counter and lane-control successor remain
bounded local counters. `LaneTerminalGuardV1Test` covers the high-bit progress
and guard round trip.

Registry class-3 `meta_cf/QUOTA` now has a local per-Lane compatibility projection.
`LaneQuotaUsageProjection` fences the message, reservation, per-Lane slot and
per-Claim/per-attempt `inflight_messages`/`inflight_bytes` dimensions with Lane
incarnation and usage revision; command and source-ordered system-mutation paths
persist it beside the Registry class-2 aggregate `CapacityVectorV1` in the same RocksDB WriteBatch. Open/recovery
rebuilds the projection from `id_cf`/`meta_cf` and the durable `inflight_cf` Claim and
attempt ledgers, then compares canonical bytes and recomputes the aggregate pending,
reservation and Lane-cardinality counts. A present aggregate that disagrees with
durable state fails activation; a legacy store with no aggregate is backfilled in
memory until its next source-ordered mutation. Legacy or synthetic ledgers with a
zero field-7 charge are conservatively counted as one durable inflight record, while
canonical field-8 attempt bytes are retained when available. Typed ACTIVE Lane
activation additionally checks field-14 `lane_usage` byte equality against the
matching class-3 map entry; a missing map or usage drift fails closed, and the
typed-state and class-3 writes remain coupled in the same source-ordered batch.
This closes the local cross-projection fence, while full typed runtime cutover,
revision authority and external placement remain release blockers. Execution beyond those
local attempt bytes, retained, evidence and external-adapter dimensions remain zero in
this subset, so full ActiveLaneState/grant authority and Route Broker placement are
still release blockers. Lane retirement now releases both per-Lane cardinality slots
and rejects any nonzero non-cardinality dimension in the complete 17-field vector,
including dimensions that this compatibility adapter does not yet populate, so a
future projection cannot silently retire a Lane with retained or control usage still
attached.

Attempt-ledger charge projection now distinguishes canonical-looking bytes from
the pre-V1 synthetic adapter seam: if `admissionBytes` begins with the Registry
common-body field-1 tag (`0x0a`) and `PublishAdmissionBody.decode` fails,
`DelayShard` fails closed instead of treating the ledger as zero-charge; arbitrary
legacy fixture bytes remain the explicitly bounded compatibility case.
`DelayShardTest.malformedCanonicalAdmissionLedgerDoesNotDowngradeToZeroCharge`
proves that no `PUBLISHING` message or attempt ledger is written. This is local
integrity evidence only; it does not replace authenticated source-ordered Admission
authority or external charge/grant evidence.

Registry class-2 `meta_cf/QUOTA` is now the canonical aggregate
`CapacityVectorV1` for the dimensions this compatibility runtime can rebuild
exactly: the per-Lane map contributes dimensions 1--17 and open
`PUBLISHING`/`UNCERTAIN` attempt ledgers contribute outcome dimensions 9--15.
External and physical dimensions 18--66 remain explicit zero until their durable
ledgers are wired. Activation compares a present class-2 vector with this
reconstructed aggregate and fails closed on drift; a missing vector is backfilled
in memory until the next source-ordered mutation. The old class-1 `ShardQuota`
and old class-2 scalar `OutcomeReserveUsage` values are read only for compatibility
validation/migration; new writes emit class-2 canonical bytes and delete the stale
class-1 projection. The exact 66-dimensional outcome vector is rebuilt from the
same canonical Admission charges for every shard, including compatibility shards
without an immutable capacity envelope; when an envelope is pinned, the persisted
grant-bound vector is additionally compared and bounded. This closes local
record/byte/vector accounting only and still does not claim external reserve
authority.
When an immutable capacity envelope is first bound, `DelayShard` now defers the
binding marker until all persisted reserve, quota and obligation projections
have passed open-time validation; a failed open therefore leaves no partial
envelope identity. `DelayShardTest.systemWriterReserveProjectionRejectsWrongPersistedDimensions`
covers the no-partial-binding boundary.

The Registry's `meta/QUOTA` quotaClass=4 (retained/object usage) remains
unimplemented: the current Registry defines its key subtype but does not yet
define a V1 value payload/accounting ledger for that class. References below to
checked reserve classes 3--6 mean the separate `meta/CONTROL_RESERVE` namespace;
they must not be read as retained/object quotaClass=4 support. Adding that quota
class requires a Registry revision before code can persist or restore it.

The typed `ActivationBarrierV1` codec now enforces the Registry rule that an
empty Pulsar barrier must carry the guarded source-connection generation and
resource-guard attestation digest together; an unguarded empty Pulsar barrier
is rejected before it can enter a Ready Certificate.

The Registry-shaped `OutcomeCapabilityV1`, `TimingCapabilityV1` and
`DeliveryCapabilitySemanticV1` codecs now enforce baseline versus strong
Kafka/Pulsar evidence-resource branches, timing-bit adapter compatibility,
nonzero prerequisite digests and canonical field ordering. This closes the
local semantic-value boundary only; Profile publication/catalog authority and
authenticated Broker prerequisite attestations remain release blockers.

The four Registry §5.1.1 Profile semantic bodies now have strict canonical
codecs, and `ProfileSemanticEnvelopeV1` binds their closed branch, kind, ID,
version and domain-separated semantic hash. Destination partition policy,
Object Store safety booleans and Evidence Verifier validity bounds fail closed
locally. `ProfileCatalog` and `InMemoryProfileCatalog` now provide an exact
local lookup seam for immutable semantic bytes, generation-1 bindings, Head,
protection and deprecation intent; authenticated publication, source-ordered
activation, retained-generation policy and Oxia authority remain external.
`ProfileCatalogV1ScheduleResolver` can decorate the existing V1 resolver to
fail closed with `ROUTE_SNAPSHOT_UNAVAILABLE` until the exact Destination
semantic reference, matching credential Head, and the referenced Delivery
Capability semantic with the same Adapter are present. `DelayShard` applies
this decorator automatically when both a raw V1 resolver and an exact Profile
catalog are supplied. An already decorated resolver is reused only when it is
paired with the byte-identical object authority of its exact same catalog;
missing/foreign catalog injection and nested Profile decorators fail before
Store projection reads (`DelayShardTest.decoratedScheduleResolverRequiresTheExactShardProfileCatalog`,
`ProfileCatalogV1ScheduleResolverTest.decoratorCannotHideAnotherProfileCatalog`). Shard-local source
activation/deprecation markers still run before this resolver gate. A missing,
wrong-kind or adapter-mismatched capability is rejected before the delegated
Lane projection (`ProfileCatalogV1ScheduleResolverTest.failsClosedWhenReferencedDeliveryCapabilityIsMissing`).
The same exact lookup fence is used when a persisted V1 binding later derives
`actionAt` for Commit/Reschedule/recovery; a missing or mismatched pinned
Profile/Capability cannot silently fall back to `deliverAt`
(`DelayShardTest.catalogBackedActionAtDerivationFailsClosedWhenPinnedProfileDisappears`).

Control Operation request values now close the Registry §6.3 operation-kind
enum, all fifteen request branches and their canonical outer oneof dispatch,
including acknowledgement/evidence presence matrices and quota transfer-plan
references. Prepared operations now also enforce the closed local
operation-kind/target-kind/presence matrix; authenticated actor/resource
authority, source-ordered System Mutation construction/registration and Oxia
operation state are still pending. `ControlTargetMutationBindingV1` now closes
the local pre-registration binding check once a caller has constructed a
mutation; it does not construct the body or authenticate the external writer.

The prepared Control Operation envelope now preserves the complete raw
`uint64` `control_query_policy_version` through its prepared digest, canonical
decode and signature preimage. This immutable policy reference is separate
from the local bounded `operation_revision` CAS counter; catalog lookup and
authenticated Oxia registration remain external. `PreparedControlOperationV1Test`
covers the high-bit round trip.

Credential binding Head/protection revisions and the corresponding rotation
target, request, lease and public-result projections now preserve complete raw
nonzero `uint64` bit patterns as well. Their local identity and equality checks
use zero-only validation; the in-memory catalog advances a revision with an
unsigned checked successor and stops at the all-ones pattern. This keeps a
high-bit CAS value from being rejected by one nested projection while accepted
by another; Oxia Head/protection CAS and provider authority remain external.
`CredentialBindingV1Test`, `ProfileControlRequestV1Test`,
`ControlTargetRefV1Test` and `ControlResultCodecTest` cover the high-bit
round trips.

The closed Control target value layer now also has canonical codecs for Shard,
Lane, Message, Route, Profile and Quota Grant target branches, including the
optional expected-mutation identity pair and a digest over fields 1--21. This
layer verifies branch identity and local bytes only; operation-specific target
presence is enforced by the prepared-operation boundary, while source-mutation
construction and authenticated target registration remain authority
responsibilities.

`PreparedControlOperationV1` now freezes the pre-I/O control envelope: it
derives the Registry request hash and target-snapshot hash, enforces the
operation-specific target count/kind and mutation-identity matrix, enforces a
strictly-sorted target list, derives the prepared digest over fields 1--10,
and signs that digest with Ed25519. Decode verifies all local hash equalities
and canonical bytes; it does not claim authenticated actor/resource
authorization, Oxia registration, or external actor/role authentication.

`ControlRoleSetV1`, `ControlAuthorizationContextV1` and
`ControlOperationAuthorizationV1` now provide the local RBAC/scope gate for a
prepared operation: the authenticated actor hash, canonical role-set digest
and resource-scope hash must exactly match `ControlAuthorV1`, and the minimum
role matrix is enforced before registration. The context and target-scope
proof are authenticator inputs; this code does not authenticate mTLS/OAuth,
resolve target existence or perform Oxia CAS.

The registration outcome layer now closes the three public branches around
`ControlOperationReceiptV1`: `ControlNonPersistenceProofV1` distinguishes
pre-Oxia ownership from authenticated conditional rejection and forbids
evidence on the local branch; the definitive/uncertain wrappers bind the
prepared digest and CONTROL-stage error; and
`ControlRegistrationOutcomeMessageV1` enforces outcome-to-branch tags. A
timeout or session ambiguity still cannot be projected as a definitive proof.
`ControlRegistrationBindingV1` now validates that each recorded, definitive
rejection or uncertain outcome is bound to the exact Prepared operation,
request hash, target snapshot, scope and initial revision before callers
project it as a registration result. It does not classify transport failures
or prove Oxia persistence.

`ControlTargetRegistrationAuthority` and
`InMemoryControlTargetRegistrationAuthority` now provide a local idempotent
exact-Prepared registration seam: a repeated operation ID is accepted only
for byte-identical Prepared bytes, and an unregistered or changed registration
cannot validate a source mutation. The local seam is not the Oxia transaction,
transport classifier or production target lookup.

`OxiaControlTargetRegistrationAuthority` now wraps the external registration
CAS surface with exact Prepared-byte reread and operation-ID/lookup identity
checks; lookup request and validation identity use independent byte snapshots.
Its injected `CasBackend` remains the production Oxia transaction and transport
boundary; the adapter does not claim to implement that client.

`ControlSystemMutationFactoryV1` now derives the closed Control mutation type
and logical identity from the Prepared target, signs the supplied canonical
body, and reruns the full target binding check before the mutation can leave
the process. It intentionally does not construct operation-specific body
fields or authenticate the service signing-key trust set.

`ControlTargetStateViewV1` now preserves the Registry's full unsigned uint32
`target_index` instead of narrowing it to a signed Java int. The Prepared
operation also exposes one canonical revision-1 `PENDING` projection covering
every target, so registration callers do not hand-build an incomplete initial
state.

`ControlRegistrationProjectionV1` pairs that initial projection with the
receipt fields and rejects operation/request/scope/revision drift before a
local operation authority is called. It is a deterministic value factory;
the one-transaction Oxia registration and response classifier remain
external.

`ControlOperationReceiptV1.createWithQueryWindow` and the matching projection
factory now compute `queryUntil` from the trusted registration upper bound via
checked addition and reject negative windows/overflow. The existing explicit
deadline constructor remains available only when an outer immutable policy
has already performed that calculation.

`EmbeddedDelayService.registerPreparedControlOperation` now exercises the
complete local registration path: exact Prepared target registration,
receipt/current projection construction, outcome binding and operation CAS.
The embedded service wires that same local registration authority into its
`DelayShard`; configured shards now fail closed with
`UNAUTHORIZED_SYSTEM_MUTATION` before any `APPLY_SHARD_CONTROL`,
`REPLAY_DEAD_LETTER` or `RESOLVE_UNCERTAIN` handler can run unless the exact
Prepared target registration and mutation bytes are present. This is an
embedded/conformance guard, not a production Oxia transaction: authenticated
gateway checks, target existence and the source-to-Oxia registration CAS still
remain release blockers.

`ControlOperationStateTransitionV1` now closes the local monotonic projection
guard for the Registry's operation and target-marker state graphs. The
in-memory operation authority rejects terminal rollback, failure after an
effective/in-progress state, disappearing target indexes and marker revisions
that do not advance. This remains a local projection check; source-ordered
mutation application and Oxia CAS are still the production authorities.

The Registry credential-control-plane values now also have strict canonical
codecs: `CredentialEquivalenceAttestationV1` binds the candidate generation,
secret-reference digest, immutable authorization scope, verifier evidence,
Trusted-UTC acceptance interval, domain-separated attestation digest and
Ed25519 signature; `CredentialBindingV1` binds the private reference to that
attestation and derives the immutable generation digest; and
`CredentialBindingHeadV1` / `CredentialBindingProtectionV1` close the current
pointer and monotonic protection projections. These codecs validate only local
bytes, digest relationships, candidate agreement and signatures. Activated
verifier trust, provider resolution, maximum proof age, Oxia Head/protection
CAS and durable post-CAS observation remain external authority and release
gates; `CredentialUseLeaseV1` now also checks its Profile-kind branch, exact
binding/fingerprint projection and kind-specific protection lifetime locally,
without performing an Oxia read per call. No private reference or verifier
evidence is projected to public data.

Registry §6.3 Profile control request values now have strict canonical codecs:
`PublishDestinationProfileRequestV1` requires a Destination envelope and its
exact generation-1 `CredentialBindingV1`; `DeprecateDestinationProfileRequestV1`
requires a Destination `ProfileRefV1` and typed `ControlReasonV1`; and
`RotateEquivalentSecretRequestV1` checks the checked generation successor,
private-reference hash, attestation candidate tuple, expected binding digest and
Head revision, and can derive the exact new immutable binding. These are request
value boundaries only; authenticated actor/target authorization, source-ordered
System Mutation routing and Oxia CAS remain external authority gates.

The System Mutation outcome subset now also has explicit canonical body encoders:
`PublishOutcomeBody.encodeInitial` closes the initial
`PUBLISHED`/`NOT_PUBLISHED`/`UNKNOWN` combinations, while
`encodeEvidenceResolution` requires a typed canonical `EvidenceCursorV1`.
The shared `PublishEvidenceV1`/`ExternalDeliveryIdentityV1` codec now closes
the evidence kind/status/id/branch envelope and evidence digest, and is reused
by Publish Outcome and DLQ Export Result. Definitive transfers are checked as
canonical `ChargeVectorV1` values before the encoder's decode round-trip. The
definitive Outcome/Resolution apply path now also requires transfer to be
canonical byte-identical to the retained Admission charge; a mismatch persists
`REJECTED(STALE_SYSTEM_MUTATION)` and advances source position without changing
the attempt, message, timeline, or quota, while `UNKNOWN` transfer remains opaque.
Initial Publish Outcome apply also requires the body-derived `PublishAttemptId` as
the logical operation identity. Evidence Resolution apply requires the domain-separated
identity `SHA-256("nereus-delay-evidence-resolution-logical-id-v1\0" ||
PublishAttemptId || evidenceId)`. A mismatch persists
`REJECTED(UNAUTHORIZED_SYSTEM_MUTATION)` without changing the attempt, message,
timeline or quota; source-ordered regressions cover both wrong-identity fences and
subsequent valid application.
Initial Publish Outcome apply now also compares the complete canonical admitted
`OwnerIdentityV1`, not just its epoch; a different deployment/worker/lease digest at
the same epoch is rejected before the attempt changes. When a recovery Owner has a
new epoch, the bounded attempt lookup permits only the Registry's exact
`UNKNOWN + OWNER_FENCED + RECOVERY_FIRST_SEND_UNCERTAIN + UNCERTAIN_HOLD` tuple and
applies it against the original admitted ledger key. The current guarded-recovery
Owner/Oxia proof remains an external authority gate; definitive or policy-retry
cross-Owner Outcomes stay unauthorized.
The same operation-identity check now covers the remaining message-bearing
handlers: `PUBLISH_ADMISSION_V1` requires `PublishAttemptId`,
`CLAIM_RESULT_V1` requires `ClaimId`, and `EXPIRE_GENERATION_V1` requires the
Registry-derived expiry identity over `DelayMessageId`, generation and
`expireAt`. Each mismatch persists `REJECTED(UNAUTHORIZED_SYSTEM_MUTATION)`
before any handler state transition; `DelayShardTest` covers the three rejection
paths and the later valid source-ordered mutation. This closes the local identity
fence only; signed-writer trust, source routing and external authority remain
release gates.
`ChannelKindV1`, `CredentialUseKindV1`, `CredentialUseLeaseV1` and
`ChannelResourceIdentityV1` now provide the shared canonical channel/lease
identity checks: adapter/target branch, strong-capability evidence resource
presence, producer digest, binding generation/digests and destination-channel
holder scope. `PublishEvidenceV1` uses this codec for channel-bearing absence
and non-submission branches. `ChannelResourceIdentityV1Test`,
`PublishEvidenceV1Test`, `PublishOutcomeBodyTest`, `DlqExportResultBodyTest`,
`DlqExportApplyTest` and the updated `DelayShardTest` provide local evidence;
provider ownership, authenticated Broker proofs, signing, Shard Log routing,
lease protection CAS/TTL configuration and evidence retention remain outside
this codec boundary.
The `OperatorAttestationEvidenceV1` branch now also requires its verifier slot
to use `ProfileKindV1.EVIDENCE_VERIFIER`; `PublishEvidenceV1Test` covers the
wrong-kind rejection. This closes the local Profile union only; verifier
registration, key activation and signature authority remain external gates.
`LaneTerminalGuardV1` now applies the Registry slot fence (`DESTINATION` then
`DELIVERY_CAPABILITY`) in both construction and decode; `LaneTerminalGuardV1Test`
covers both invalid slot directions and the typed constructor/decode paths now
parse the canonical tuple enough to reject malformed structure and Profile
id/version/hash projection drift. The compatibility adapter may still retain
resolver-provided opaque bytes, while complete resolver/catalog/Oxia authority
remains an external gate.
Physical channel/evidence generations and the credential binding generation in
`ChannelResourceIdentityV1` now preserve raw unsigned `uint64` bit patterns
(zero is still rejected), matching the typed `EvidenceCursorV1` generation.
The same full-width rule now covers credential attestation/binding,
Head/protection, use lease, Ready Certificate, native capability snapshot,
rotation request/result, and Profile control projections. Head/protection
revisions are nonzero complete raw `uint64` identity values; other local
control versions remain bounded positive counters. The independent Broker
resource-guard configuration generation also preserves raw nonzero `uint64`
bits. High-bit coverage is provided by
`CredentialBindingV1Test`, `ProfileControlRequestV1Test`,
`ControlResultCodecTest`, `ChannelResourceIdentityV1Test`, `ProtocolCodecTest`,
and the Publish Admission/Ready Certificate fixtures.
`ReadyCertificateV1` and `PublishAdmissionBody` additionally reject a
certificate whose credential binding drifts from the Channel or whose expiry
outlives the protected Channel lease.
`ResolveUncertainBody` now has a canonical encoder for every closed resolution
shape, round-trips its output through the strict decoder, validates the
evidence-attachment owner/status through the typed `PublishEvidenceV1` codec,
and rejects a `messageId` whose self-routing Shard differs from the mutation
subject. The shared System Mutation body helper now applies the same
self-routing check to Publish Admission, Claim Result, DLQ Export Result and
Expire Generation bodies, so a valid outer signature cannot redirect a
message-bearing mutation to another Shard. The source-ordered control applier
now settles `ATTACH_PUBLISHED_EVIDENCE` against the exact current `UNCERTAIN`
obligation or an exact open obligation retained in an older `terminal_cf`
generation summary. Current-generation settlement commits Message, terminal
summary, ledger, pending-schedule quota, outcome reserve, System Mutation result
and Source Position in one WriteBatch; historical settlement updates only the
retained terminal summary, ledger, duplicate-risk and outcome reserve, so it
cannot mutate a newer generation. Both paths return the stored result on
duplicate mutation replay. `DelayShard` also treats a duplicate System Mutation
at a later Source Position as a position-only advance; if that later batch was
committed before the source ACK was lost, exact replay returns the stored result
instead of reporting a first-position mismatch. `DelayShardTest` covers this
post-commit replay boundary.
`ATTACH_NOT_PUBLISHED_EVIDENCE` now settles the exact typed not-published
obligation and applies remaining-obligation/all-absent normalization,
including revoking a live `CLAIMED` branch before creating the definitive
retry timeline; `DelayShardTest` covers that source-ordered path. The
authenticated external evidence/control authority and complete retry/charge
proof remain release blockers.
Publish evidence branches also enforce Kafka/Pulsar target-resource and
EvidenceCursor/Channel adapter alignment instead of validating each nested
identity independently.

The bounded local Control Operation authority also rereads an exact CURRENT
advance after response loss; it does not infer success for a later or
different revision. `PersistentControlOperationAuthority` now stores the
complete receipt and current projection in a checksummed per-operation file,
uses a JVM plus on-disk lock for cross-instance local CAS, and publishes each
replacement with a temporary file, atomic rename and directory fsync. Reopen,
same-revision response-loss retry, identity mismatch and corrupt/truncated
state are covered by `PersistentControlOperationAuthorityTest`; this closes
the embedded crash-durable state boundary but not production Oxia routing,
authorization or session ownership.

The local Recovery Catalog likewise rereads an exact legacy or typed Floor
successor after a lost CAS response without advancing the Floor twice.
Typed Floor advancement now also requires the supplied evidence-cursor set to
be byte-equal to the candidate checkpoint manifest's cursor set; a cursor not
covered by that checkpoint cannot be promoted into the Floor projection.
The legacy scalar-Floor compatibility path applies the same manifest-cursor
check when creating a typed Recovery Pin, so compatibility does not create a
weaker evidence boundary.
Published checkpoint ancestry also keeps every parent evidence cursor present
at the same identity in the child and requires the child cursor to dominate it;
cursor disappearance or generation replacement is rejected before catalog
generation advances.
Floor coverage and local GC proofs likewise require canonical Source Position
bytes when the covered and required order tokens are equal; a same-offset or
same-ledger/entry/batch metadata variant cannot satisfy a retention barrier.

The local `gc_cf/TASK` readers also compare the requested resource kind,
identity hash and expected resource version with the embedded retire intent
(including the nested intent in a delete confirmation); a misplaced GC value
is rejected before compaction or query code can use it. `DelayShardTest`
covers the key/value identity fence.
`ResourceGcGuard` now additionally requires the nested delete-confirmation
intent to be byte-identical to the current retire-intent record, including its
protection set, applied mutation sequence and applied Source Position; a
matching mutation/resource tuple with altered retention fields returns
`INTENT_REFERENCE_MISMATCH` instead of authorizing compaction.

Exact already-published manifests are similarly reread before generation CAS;
same-checkpoint hash drift remains an integrity conflict.

Source replay rejects a connection-generation/guard proof on Kafka positions;
that proof is reserved for the guarded Pulsar source branch.

`SourcePositionCodec` now requires every decoded Kafka/Pulsar position to
round-trip byte-for-byte through its canonical encoding; malformed UTF-8 or
replacement-character input is rejected before a position can become persisted
metadata, receipt evidence or checkpoint state. `ProtocolCodecTest` covers both
adapter branches with non-canonical UTF-8 vectors.

Kafka offset and Pulsar ledger/entry Source Position fields now preserve the
complete unsigned-64 raw bit pattern through the fixed-width codec, canonical
protobuf Source Position/receipt/evidence/barrier paths, adapter result values
and checkpoint-manifest JSON. Source Position partition, leader-epoch and
Pulsar batch fields likewise preserve the complete unsigned-32 raw bit pattern
through direct codecs, comparisons, receipts, barriers, evidence cursors and
manifest JSON. Checkpoint manifest decoding specifically uses the unsigned
parser for Pulsar evidence-cursor `batchIndex`/`batchSize`, including the
high-bit and all-ones values. Their comparisons and Kafka successor use
unsigned order,
including the `0x7fff... -> 0x8000...` boundary; high-bit position,
receipt/evidence/manifest round-trips are covered by `ProtocolCodecTest`,
`ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `SourceReplaySuccessorTest`
and `CheckpointManifestTest`. The three Registry
`TrustedUtcIntervalEvidenceV1` uint64 counters and its `sourceKeyVersion:uint32`
now also preserve raw bits through the canonical codec and checkpoint
`createdAt` JSON; high-bit signed-time evidence is covered by
`ProtocolCodecTest.trustedUtcEvidencePreservesUnsignedSourceKeyVersionBits` and
`CheckpointManifestTest.manifestRoundTripsUnsignedSourceAndEvidencePositions`.
The Registry Pulsar `physicalTopicCreationTimestamp:u64be` identity likewise preserves its
raw bits through broker identities, queued ACKs, evidence cursors, managed and
native adapter request/result values, and checkpoint-manifest JSON; the high-bit
boundary is covered by `ProtocolCodecTest`, `EvidenceCursorV1Test`,
`CheckpointManifestTest`, `AdapterIngressTest`, `DestinationAdapterTest` and
`NativeSubmissionAdapterTest`. Other auxiliary uint64/time fields plus real
Broker adapter proof remain release blockers. Ownership/fencing `ownerEpoch`
and writer generations now preserve their raw uint64 bit patterns through
`OwnerIdentityV1`, the OWNER/FENCE/SERVICE `AuthorIdentity` branches,
`ShardControlResultV1`, StoreRuntimeMetadata/inflight keys, Claim and
PublishAttempt records, and checkpoint `createdBy` JSON. The high-bit boundary
is covered by `ProtocolCodecTest`, `StoreRuntimeMetadataTest`, `KeyCodecTest`,
`ClaimRecordTest`, `PublishAttemptLedgerTest` and `CheckpointManifestTest`.
The guarded Pulsar source-connection generation is also preserved as a raw
uint64 through `ActivationBarrierV1`, the runtime Pulsar barrier, replay-entry
validation and `OwnedDelayShard`; `ActivationBarrierV1Test`,
`SourceActivationBarrierTest` and `OwnerLeaseTest` cover its high-bit fence
path.
`NativeCapabilitySnapshotV1` now preserves the complete nonzero raw
`resource_guard_config_generation` in its signed digest and canonical decode,
matching the already raw credential-binding generation. The snapshot test
covers both high-bit fields; guarded Broker rollout attestation remains an
external authority gate.
`AutoFastSchedule` and the embedded AUTO_FAST preparation path likewise keep
the native physical partition as raw `uint32` bits: selection and range checks
use unsigned comparisons, so a valid high-bit partition is not rejected or
miscompared before Producer ownership. `AutoFastScheduleTest` covers this
projection with `0x80000000`.
The nested `ReadyCertificateV1` Broker attestation and config generations now
also preserve their complete raw `uint64` patterns through the shared
Admission/certificate decoder; `ReadyCertificateV1Test` covers both high-bit
values.
The same shared decoder now parses the nested `ActivationBarrierV1` and every
repeated `EvidenceCursorV1` instead of accepting opaque non-empty bytes. It
requires at least one cursor and rejects non-canonical nested branches or
unsorted/duplicate cursor identities before either the Admission body or the
public `ReadyCertificateV1` wrapper can expose the certificate;
`ReadyCertificateV1Test` covers the direct Admission-parser rejection paths.

`TrustedUtcClock` now provides the local Worker timing guard that was missing
behind the evidence codec: it advances an approved interval from an injected
monotonic reading, rejects over-wide or stale samples, detects wall/monotonic
step drift, requires the configured stabilization window, and exposes only a
qualified interval for the strict `earliest >= actionAt` and
`latest < expireAt` gates. It never reads raw wall time itself. The evidence is
`TrustedUtcClockTest`; approved time-source/signature authority and wiring this
guard into production Broker/Worker activation remain release blockers. The
configured uncertainty cap must also cover both conservative divergence sides,
so an impossible timing budget is rejected before the guard can activate.

Destination Profile target partition count/allow-list values and Native
capability/prepared/public binding physical partitions now preserve complete
unsigned-32 bit patterns through their canonical protobuf codecs and local
publish-admission checks. `ProfileSemanticEnvelopeV1Test`, `ProtocolCodecTest`
and the existing Native/Publish Admission suites cover high-bit values; Profile
publication, target existence and authenticated Broker authority remain
external.

`DelayShard` now uses checked increments for the Message Control Version
(`stateVersion`) and the locally represented generation successor in
Cancel/Reschedule transitions and their replay projections. At the maximum
representable value the command is persisted as `INVALID_COMMAND` before any
Message or timeline mutation; it cannot wrap into a negative version or
generation. `DelayShardTest.messageGenerationAndStateVersionOverflowFailClosedBeforeMutation`
covers both boundaries.

`MessageRecord.decode` now bounds every fixed-width read, including the
version-specific retry, payload and runtime-index fields. A truncated durable
value therefore raises the codec's `IllegalArgumentException` instead of
leaking a `BufferUnderflowException`; `MessageRecordTest` checks every strict
prefix of a canonical v4 record.

`ClaimRecord.decode` applies the same bound before every fixed-width and
length-prefixed field. Claim record v1 remains readable, while v2 retains the
canonical source `TimelineWorkRef` needed to restore an exact retry authority
on revoke. A truncated Claim value cannot consume a missing LP32 length or
numeric suffix and leak a native buffer exception; `ClaimRecordTest` exercises
every strict prefix of a persisted Claim.

`PublishAttemptLedger.decode` now guards its u32/u64 suffixes before reading
them and no longer rejects valid minimal non-empty LP32 fields through an
over-large aggregate minimum. `PublishAttemptLedgerTest` covers every strict
prefix and a canonical short-field ledger.

`PayloadReservation.decode` now bounds its version, partition, expiry,
state-version and presence-byte reads after the fixed intent projection;
truncated reservation values fail as validation errors before a missing
committed-payload branch can be consumed. `PayloadReservationTest` covers
every strict prefix.

The same durable reservation codec now binds a `COMMITTED` payload reference's
length and SHA-256 to the Prepare intent before the value can be constructed or
decoded. A damaged or mis-bound committed reservation therefore fails closed
instead of exposing an object identity whose bytes disagree with the frozen
reservation. `PayloadReservationTest.committedPayloadMustMatchPrepareLengthAndDigest`
covers the local mismatch fence; provider/Object Store authority remains a
release blocker.

Reservation reads now also fence the `id_cf/RESERVATION` key against the
embedded reservation identity and the current shard identity. Message-based
reservation lookup uses a bounded `maxPendingMessages + 1` scan, rejects a
scan that cannot prove completeness, and fails closed when one message has
multiple reservation records. `RESERVATION_EXPIRY` discovery additionally
requires the timeline projection to byte-match the current `id_cf` record;
`DelayShardTest` covers misplaced-key, duplicate-reservation and stale-expiry
projection recovery paths.

The deterministic local Object Store adapter now retains the first registered
Prepare reservation as the receipt anchor and accepts only the same immutable
reservation identity's legal source-ordered lifecycle transition. The
shard-local `PayloadReservation` value persists that anchor's state version and
Source Position, so a newly constructed adapter can reconstruct the original
Prepare receipt after reopening a v2 `COMMITTED`, `ABANDONED` or materialized
`EXPIRED` reservation. A strict v1 decode/upgrade path remains for older local
values; because those values never carried an anchor, their fallback anchor is
the value's current state and cannot invent missing historical Prepare data. A previously
issued receipt consequently maps to `RESERVATION_EXPIRED`,
`RESERVATION_ABANDONED` or `RESERVATION_CLOSED` after the corresponding local
state projection, while arbitrary state/version/source drift remains rejected.
`InMemoryPayloadObjectStoreTest.receiptAnchorSurvivesSourceOrderedReservationLifecycleTransitions`
and `EmbeddedDelayServiceTest.payloadFacadeMapsSourceOrderedReservationCloseToTypedOutcome`
cover the lifecycle and typed outcome behavior; the durable value is also
round-tripped before the reopened-adapter assertion. Source-position trust
authority, real provider/Oxia binding and external object retention remain
release blockers.

`TerminalGenerationRecord.decode` now bounds the terminal source and
obligation count/length fields through named fixed-width readers. Its legacy
and v2 branches both reject strict-prefix truncation as codec validation;
`TerminalGenerationRecordTest` covers the canonical v2 path.

Direct terminal-history reads now also compare the requested
`messageId/generation` with the embedded value before exposing the summary;
an entry written under the wrong terminal key is rejected instead of being
used by query or runtime-summary paths. `DelayShardTest` covers this
key/value identity fence.

The same direct-read fence now covers `dedupe_cf/SYSTEM_MUTATION` results,
`meta_cf/LANE` active/terminal values and the `timeline/SYSTEM` Lane-close
cursor: requested mutation/Lane/cursor identity, Lane incarnation/control
version and source-position shard are checked before callers receive the
projection. Misplaced values fail closed in `DelayShardTest`.

`id_cf/MESSAGE` direct reads and the bounded runtime-index/Lane scans now also
check the self-routing message-key shard and decode the embedded
`scheduleSourcePosition` before exposing the current projection. A value copied
from another Shard therefore cannot be treated as local work after a live DB
write; the same fence is applied during activation reconciliation and close or
retirement scans. `DelayShardTest.messageLookupRejectsForeignSourcePosition`
covers the cross-Shard value case.

The same local Source Position shard fence now covers command results, terminal
generation history, open publish-attempt ledgers, reservation projections,
system-mutation results, DLQ export records and GC retire/delete projections.
Their embedded message/command routing identity is checked where the value
contains one; foreign history cannot become a local query, drain or compaction
input. `DelayShardTest` covers command-result, terminal-generation and
publish-attempt foreign-position lookups.

Activation also rereads the persisted applied Source Position and every
source-ordered Profile/Trust-Set control marker after decoding their registered
`meta_cf` envelopes. A DB that was physically copied with a foreign source
history therefore cannot enter the local runtime before replay; the boundary is
covered by `DelayShardTest.activationRejectsForeignAppliedSourcePosition` and
`DelayShardTest.activationRejectsForeignSourcePositionInProfileControlState`.

Terminal Lane guard reads now apply the same check to the guard's terminal
Source Position before returning a retired Lane projection; a foreign retirement
proof is rejected even when its Lane tuple and guard digest are otherwise valid.
`DelayShardTest.laneTerminalGuardLookupRejectsForeignSourcePosition` covers this
direct-read path.

Lane retirement's final bounded inflight scan now reuses the same Claim and
publish-attempt key/value decoders as normal recovery, so a misplaced key or
foreign attempt Source Position cannot be mistaken for merely pending Lane
work. `DelayShardTest.laneRetirementRejectsInflightKeyValueMismatchBeforeRetiring`
covers the fail-closed boundary.

`PersistentLaneScheduler` READY recovery now also checks both the READY message
self-routing Shard and its embedded `scheduleSourcePosition` Shard before
recomputing the timeline key. A copied READY projection therefore cannot enter
the local fairness ring through the scheduler-only recovery path;
`LaneSchedulerTest.fencedRecoveryRejectsReadyMessageFromAnotherShard` covers it.
Steady-state discovery retains the physical READY key together with the work
item, so a same-message/generation head is suppressed only while both its
work identity and its exact lane-versioned READY key remain unchanged;
`LaneSchedulerTest.readyTransitionWithSameWorkUsesNewReadyKey` covers the
transition fence. Discovery also rejects a READY key/value projection whose
serialized bytes exceed the supplied visit byte budget instead of granting a
first-entry exception; `LaneSchedulerTest.readyDiscoveryRejectsFirstEntryThatExceedsByteBudget`
covers the cap. The elapsed-time cap is likewise checked before the first
projection is decoded, so a scan that has already exhausted its turn returns
without admitting work; `LaneSchedulerTest.readyDiscoveryStopsBeforeFirstEntryWhenTimeBudgetIsElapsed`
covers that boundary.

The internal `dedupe_cf/COMMAND` replay lookup now applies the same command-key
Shard and result Source Position checks as the public result query, and Claim
lookups/scans reject a `DelayMessageId` routed to another Shard before owner
drain or admission logic can use it. `DelayShardTest` covers both
`commandDedupeLookupRejectsForeignSourcePosition` and
`claimLookupRejectsForeignMessageShard`.

Route-keyed Message and Command-result reads now perform that self-routing
Shard check before the RocksDB lookup, so a foreign ID with no local record
cannot be silently reported as not-found. Terminal-history, DLQ-export and
message-based Claim lookups share the same pre-read fence; the missing-record
case is covered by `DelayShardTest.routeKeyLookupsRejectForeignMessageShardBeforeMissingRead`.

`SloObservationOutboxStore.get` now applies the same sample-id key/value fence
as its bounded scan, so a misplaced `meta_cf/SLO_OUTBOX` record cannot be
consumed by Start/Final merge logic; `SloObservationOutboxStoreTest` covers
both direct and scan reads.

Close-materialization discovery now revalidates each cursor's embedded close
Source Position shard before returning scheduler work, matching the direct
cursor query path; `DelayShardTest.laneCloseMaterializationDiscoveryRejectsForeignSourcePosition`
covers the scheduler-only read.

`discoverDue` and `discoverExpiry` now cross-check each timeline projection
against the current `id_cf/MESSAGE` record: status, generation, Lane, expiry
and the exact derived key must all match. Orphan, terminal, stale-generation or
misplaced DUE/EXPIRY entries therefore fail closed instead of becoming publish
or expiry work; valid close-owned `SCHEDULED`/`CLAIMED` generations remain
discoverable until their normal materialization/admission path removes them.
`DelayShardTest.timelineDiscoveryRejectsOrphanDueAndExpiryEntries` covers both
discovery namespaces.
The same exact-key requirement is enforced by the package-local `rebuildReadyIndexes` while it
reconstructs the per-Lane READY head; a current scheduled `MESSAGE` cannot
legitimize a timeline entry whose eligibility/source token/generation key was
altered. `DelayShardTest.readyRebuildRejectsTimelineKeyThatDiffersFromCurrentMessage`
covers the recovery-only path.
Direct `discoverReady` now makes the same final existence and key/value identity
check before returning scheduler work; a READY projection cannot survive as a
standalone pointer after its timeline entry is deleted. The missing-entry case
is covered by `DelayShardTest.readyDiscoveryRejectsMissingTimelineEntry`.
`id_cf/V1_SCHEDULE_BINDING` direct reads also reject a message ID routed to a
different Shard before looking up the sidecar; `DelayShardTest.scheduleBindingLookupRejectsForeignMessageShard`
covers the sidecar-only case.
READY rebuild candidate scans now read one entry past the configured pending
bound and fail closed on overflow instead of silently dropping a Lane's later
timeline heads; `DelayShardTest.readyRebuildRejectsTimelineCandidateScanOverflow`
covers the bounded recovery case.

The embedded Kafka ingress now treats Source Position offsets as an unsigned
64-bit sequence. It validates the next position before advancing state, accepts
the raw Java `-1L` bit pattern as the final valid offset, and records exhaustion
in a separate flag so that a valid all-ones offset is not rejected as a sentinel
or incremented into an invalid successor. The failed-enqueue and final-offset
boundaries are covered by
`EmbeddedDelayServiceTest.embeddedSourceOffsetExhaustionFailsBeforeMutatingOffset`
and `EmbeddedDelayServiceTest.embeddedSourceAcceptsUnsignedMaximumOffsetThenExhausts`.

`EmbeddedDelayService` now applies an explicit bounded client buffer through
`EmbeddedDelayServiceConfig`: pending command count and canonical frame bytes
are checked before allocating a Source Position. A full buffer returns
`DEFINITELY_NOT_QUEUED(SDK_BACKPRESSURE_NOT_SUBMITTED)` without advancing the
embedded source offset; draining releases the exact byte charge. The embedded
defaults are conformance-harness values, not production Broker defaults. Real
adapters still need equivalent Producer buffer, batch/linger,
request/delivery-timeout and close-drain configuration. The embedded `close()`
now synchronously drains accepted records before closing the shard DB, and the
drain queue keeps its head charged until `DelayShard.apply` returns. The count,
byte, offset, release and close-drain behavior is covered by
`EmbeddedDelayServiceTest.sdkBackpressureRejectsBeforeSourcePositionAndByteBudgetAreConsumed`.
`EmbeddedDelayServiceTest.closeDrainsQueuedCommandsBeforeClosingTheShardDb`
also verifies that a reopened service can read the applied result. After a
successful drain the service enters the closed state before resource teardown;
DB-close and shared-resource-close failures are both attempted and aggregated
with suppressed exceptions, so a failed native close cannot skip the process
resource release attempt. This remains an embedded lifecycle guarantee rather
than production Producer/Broker close-drain evidence. The facade also fences
post-close `shard()` and pending-buffer access, covered by
`EmbeddedDelayServiceTest.closedEmbeddedServiceDoesNotExposeShardOrBufferState`,
so callers cannot bypass the client lifecycle and mutate a closed Store.
Every explicit `drain()` retains the applied physical result in a bounded local
window of `EmbeddedDelayServiceConfig.maxPendingCommandCount`, allowing a later
legacy await to return an exact position-level conflict/fence result. If that
evidence is evicted and the only durable result is anchored at another source
position, `awaitApplied` fails closed rather than returning the wrong logical
dedupe result or `null`; this is local conformance evidence and does not change
the durable `dedupe_cf/POSITION` value schema.
The constructor now treats a post-open source-identity/metadata mismatch as a
failed acquisition: it closes the just-opened `ShardStore` and its private
shared RocksDB resource envelope, aggregating cleanup failures as suppressed
exceptions. `EmbeddedDelayServiceTest.failedEmbeddedConstructionClosesStoreAfterSourceIdentityMismatch`
reopens the same DB after the rejected construction to prove that a fail-closed
startup path does not strand the DB lock or native resource slots. This is still
an embedded startup guarantee; production owner acquisition and source
assignment authority remain external.

`DelayShard` now preserves the complete raw `uint64` domain for persisted
mutation and Claim sequences on reopen, through checkpoint barriers and Claim
identity derivation. `DelayShardTest.acceptsCompleteUnsignedPersistedShardSequences`
covers the high-bit/all-ones reopen boundary. The next mutation sequence is
computed through one checked unsigned helper before every source-position
WriteBatch; resource-retirement and delete-confirmed records use the same helper.
The successful post-write update assigns that checked successor back to the
in-memory projection, so `0x7fff... -> 0x8000...` is valid and only the all-ones
pattern is exhausted. At exhaustion, the next batch is rejected before any
command, result, or source-position state is written.
`DelayShardTest.mutationSequenceExhaustionFailsClosedBeforeCommandMutation`
covers both boundaries.

Checkpoint manifest `lineageGeneration`/`shardMutationSequence` and scalar/typed
Recovery Floor projections now preserve full-width unsigned values in their
canonical JSON, Protobuf and digest bytes. Local catalog ancestry uses checked
unsigned lineage successors, and Floor coverage compares mutation sequences
unsigned. `CheckpointManifestTest`, `RecoveryFloorRefV1Test` and
`RecoveryCatalogTest.catalogComparesManifestMutationSequenceAsUnsigned` cover
the boundary; Oxia CAS, object-store publication and source replay remain
release gates.

Durable `CommandResult` and `SystemMutationResult` values now validate their
embedded Source Position through the canonical decoder at construction and
decode time. Empty, truncated or non-canonical source bytes therefore cannot
enter a result projection and wait for a later shard-specific query to reject
them; `DurableResultTest` covers both result types and the trailing-byte
adversary.

`KafkaActivationBarrier` now treats the raw `-1L` bit pattern as the
unsigned-64 maximum and saturates the exclusive next-readable offset there; a
source record at that maximum no longer wraps while proving an already-reached
LSO. `SourceActivationBarrierTest.KafkaBarrierAcceptsUnsignedMaximumExclusiveOffset`
covers this physical-offset boundary.

`EmbeddedDelayService` applies the same unsigned-maximum saturation when
reconstructing its next Kafka offset from persisted shard state. Reopening
after a record at the raw unsigned maximum now succeeds and keeps enqueue
fail-closed at the exhausted boundary;
`EmbeddedDelayServiceTest.reopenedEmbeddedServiceKeepsUnsignedMaximumSourceOffsetExhaustion`
covers the restart path.

Canonical protocol writers now enforce the unsigned 32-bit range instead of
silently encoding a wider value through the `uint32` helper. The Registry's
resource-state version is a raw unsigned `uint64` across the retire body,
delete-confirmation reference, logical identity hash, GC key and durable
intent record; only this registered System Mutation field bypasses the normal
non-negative scalar guard. Local mutation/Claim sequences now use the complete
raw `uint64` domain and checked all-ones exhaustion. High-bit coverage is provided by
`ResourceRetireIntentBodyTest.preservesUnsignedResourceStateVersionBits`,
`ResourceDeleteConfirmedBodyTest.intentPreservesFullUnsignedResourceStateVersion`,
`KeyCodecTest.gcRetireIntentKeyPreservesUnsignedResourceStateVersionBits`,
`ResourceGcGuardTest.durableGcRecordsPreserveUnsignedResourceStateVersionBits`
and `DelayShardTest.resourceRetireIntentIsSourceOrderedDurableAndVersionFenced`.
The canonical-protobuf reader now applies the same `1..0x1fff` field-number
bound as the writer, so an out-of-registry tag cannot enter a closed union
decoder. `CanonicalProtobufTest.readerRejectsFieldNumbersOutsideRegistryRange`
covers the reader boundary.

The Registry-shaped `ScheduleIntentV1` value is now implemented as a strict
canonical codec: it binds the destination `ProfileRefV1`, `RetryPolicyRefV1`,
delivery/order fields, the closed inline-versus-committed payload union,
Kafka/Pulsar adapter metadata, optional business/event fields and quota
accounting version. `ScheduleIntentV1.forPrepare` is the explicit no-payload
form for `PrepareLargeScheduleV1`; ordinary creation requires exactly one
payload branch. Schedule/Prepare now have an explicit
`V1ScheduleResolver` seam: `DelayShard` requires that resolver for Registry
bodies, validates the tuple-derived Lane and payload projection, and persists a
`V1ScheduleBinding` sidecar with the canonical body in the same WriteBatch.
Without a resolver the source record is rejected with
`ROUTE_SNAPSHOT_UNAVAILABLE`; it is never downgraded to the legacy adapter.
The resolver is a local authority seam only. When a `RetryPolicyCatalog` is
supplied, Schedule/Prepare additionally require the exact policy semantic ref
at the command Source Position and apply its ordering-mode guard; a missing or
mismatched value returns a stable fail-closed code. Policy/Profile activation,
production adapter validation and the object-payload optional-etag projection
remain release blockers. The Cancel/Reschedule V1 bodies are applied by
`DelayShard` with their independently present generation/state-version
preconditions.

`PreparedCommand.scheduleV1`/`prepareLargeV1` and the matching
`CommandCodec.encode/decode*V1` seam now bind body common fields back to the
outer command identity/type/retry deadline. The default no-suffix encode/decode
path remains backward-compatible and does not claim V1 body validation.

`PrepareLargeScheduleV1` now carries required field 15, an exact
`OBJECT_STORE ProfileRefV1`, in addition to its pinned trust-set. The complete
canonical Prepare remains in `V1ScheduleBinding`; `DelayShard` resolves the
exact immutable Object Store semantic/current credential Head and applies the
source-ordered first-binding gate before accepting a catalog-backed Prepare.
The embedded receipt/handle/attestation facade reloads that durable binding and
requires both the adapter Profile ref and trust-set ref to match before any
adapter registration or object authority is created.

`CommitLargeScheduleV1` has an explicit canonical body with the same common
fields 1–3, reservation identity and nested typed `PayloadCommitProofV1`. The
proof codec covers the Registry's typed Object Store Profile, tenant scope,
optional etag presence, proof-id/signature digests and Ed25519 verification.
`DelayShard` consumes both the typed V1 proof view and the legacy proof adapter
through the same trust-set/commit state machine. A typed proof must equal the
full Profile ref pinned by Prepare; a legacy proof must equal its semantic
hash, and both checks occur before the first transition and an already-committed
retry fast path. The local `PayloadReference` value now retains the Registry
descriptor's ReservationId and accepted ProofId while reading its prior
identity-less version for compatibility. `ALREADY_COMMITTED` requires exact
ProofId/object identity plus signature verification under the retained
historical key; issuer close no longer conflicts with safe retry, and malformed
Ed25519 signature bytes return false rather than escaping as a source-stalling
runtime exception. Source-position trust-set authority and production Object
Store attestation/ownership remain release blockers.
`InMemoryPayloadObjectStore` now provides a deterministic local
adapter seam that binds an exact `PayloadReservation` to a service-owned
immutable object identity, projects and validates a receipt-bound object
identity/source/state/trust-set contract, and issues response-loss-idempotent
opaque handles,
enforces the configured max-handle-lifetime/reservation-expiry minimum plus
the profile's max-bytes/length/SHA-256 and immutable-if-absent rules,
and returns a cached, trust-set-verifiable `PayloadCommitProofV1`; it keeps
capability/payload bytes in memory, re-signs a new handle only after the old
capability expires, and does not claim provider credentials, remote
immutability, Oxia authority or production availability evidence.

`RetryPolicySemanticV1` now supplies the Registry semantic fields, the
domain-separated semantic hash and a typed `RetryPolicyRefV1` projection. It
rejects invalid uncertain/DLQ branch combinations and checked backoff-budget
overflow, and rejects possible-duplicate retry for FIFO ordering. When a
`RetryPolicyCatalog` is supplied, accepted V1 Schedule/Prepare bindings are
revalidated at later source-ordered Admission/uncertain-retry and reopen turns;
the immutable semantic budgets are used instead of silently widening or
weakening them to local defaults. `InMemoryRetryPolicyCatalog` now provides a
deterministic source-history test authority with exact reference/hash lookup
and visibility fences; authenticated activation authority and durable
historical retention remain external-authority blockers.
The local source-history gate also rejects a same-token visibility lookup or
publication when the canonical Source Position metadata differs.

Scheduler round generations and Lane/Shard `lastServedRound` now saturate at
`Long.MAX_VALUE`, while inner scheduler byte-budget accumulation uses checked
addition; a long-running or restored scheduler cannot wrap its service-gap
evidence into a negative value. `LaneSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`
and `WorkerSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`
cover the two scheduler levels.

Both scheduler restore paths now clamp a persisted Lane/Shard deficit to the
current registered cap before exposing the in-memory projection. A stale
snapshot from a larger historical quantum or weight therefore cannot leave an
idle scheduler with an unbounded deficit until its next poll;
`LaneSchedulerTest.saturatesRestoredDeficitBeforeServing` and
`WorkerSchedulerTest.saturatesRestoredDeficitBeforeServing` assert the
post-restore projection as well as the first service.

`PersistentLaneScheduler` now computes the next ring and READY-cursor wrap
generations as local projections and advances the in-memory counters only after
the five-value scheduler `WriteBatch` succeeds. A failed projection write or
READY decode therefore cannot advertise a generation that was never durable;
`LaneSchedulerTest.failedSchedulerProjectionWriteDoesNotAdvanceGenerationInMemory`
and `LaneSchedulerTest.failedReadyProjectionDecodeDoesNotAdvanceWrapGenerationInMemory`
cover both local failure boundaries. Poll, READY discovery, fenced READY
rebuild and blocked/ready transitions now use the same rollback boundary: a
failed write restores polled heads, newly offered tails, rebuilt queue state,
active-ring/cursor/fairness state, discovery heads and recovery-first-pass
bookkeeping before rethrowing. The regressions
`LaneSchedulerTest.failedPollProjectionWriteRestoresThePolledHeadInMemory` and
`LaneSchedulerTest.failedReadinessProjectionWriteRestoresThePreviousGateProjection`
cover the queue and readiness cases; `LaneSchedulerTest.queueSnapshotRestoresExactFifoProjection`
covers the exact FIFO snapshot used by fenced rebuild rollback. Full READY
replacement now validates and assembles every item before clearing prior
queues, so a later malformed or non-schedulable item cannot leave a partial FIFO
(`LaneSchedulerTest.failedPendingReplacementKeepsTheOriginalQueues`). Failed terminal
unregister also restores the exact prior active-ring membership, so a Lane that
was already outside the ring cannot be reactivated by the rollback registration
step (`LaneSchedulerTest.failedPersistentUnregisterDoesNotReactivatePreviouslyInactiveLane`).
Lane/Shard counter restore validates the complete registered subset and rejects
duplicate identities before applying any entry, so a malformed later counter or
ambiguous repeated entry cannot partially restore an earlier Lane/Shard
(`LaneSchedulerTest.invalidLaterRestoreEntryDoesNotPartiallyApplyEarlierCounters`,
`LaneSchedulerTest.duplicateLaneRestoreIdentityDoesNotPartiallyApplyEarlierCounters`,
`WorkerSchedulerTest.duplicateWorkerRestoreIdentityDoesNotPartiallyApplyEarlierCounters`).

The inner and outer two-rotation visit caps now widen `ring.size() * 2` before
comparison, so a large in-memory ring cannot turn the bounded loop limit into a
negative `int` through arithmetic wrap. `LaneSchedulerTest.ringVisitLimitUsesWideArithmetic`
and `WorkerSchedulerTest.outerVisitLimitUsesWideArithmetic` cover the boundary;
this is local scheduler arithmetic evidence and does not replace the required
capacity proof for a production worker's maximum Lane/shard population.

Lane registration now treats `laneIncarnation` as immutable for a registered
`destinationLaneId`. A second registration with a different incarnation fails
before the existing queue, deficit, ring position or persistent registration
projection can be replaced; same-incarnation registrations remain the normal
gate/readiness/weight update path. `LaneSchedulerTest.rejectsLaneIncarnationChangeWithoutMutatingSchedulerState`
covers the local fence. This protects the scheduler's identity boundary only;
source-ordered Lane lifecycle and terminal-guard authority remain in
`DelayShard`/the external Registry.

Worker shard registration now performs the complete weight arithmetic check
before changing the outer deficit cap or accepting a duplicate Shard identity.
A conflicting scheduler/weight registration therefore cannot leave a larger
cap behind for later visits; `WorkerSchedulerTest.conflictingShardRegistrationDoesNotMutateOuterDeficitCap`
covers the failed-registration path.

`PersistentLaneScheduler.restorePersistedState` now restores deficit and
`lastServedRound` for every registered Lane, including a BLOCKED/paused Lane
that is absent from the active ring. Its counters are therefore not reset when
the Lane later becomes READY after restart;
`LaneSchedulerTest.fairnessCountersSurviveRestartForLaneOutsideActiveRing`
covers this recovery boundary.

Scheduler projection loading also verifies the cross-value generation fence:
the discovery cursor's active-ring generation must equal the ActiveRing
generation, and ActiveRing/round generations must agree before any scheduler
state is exposed. `LaneSchedulerTest.schedulerRestoreRejectsCrossProjectionGenerationDrift`
covers a digest-valid but cross-projection-inconsistent value.

`ProfileBindingActivatePayloadV1` and `ProfileNewBindingClosePayloadV1` now
close the Registry control branches for Profile first-binding lifecycle.
`ProfileBindingControlState` persists strictly source-ordered activation and
close markers. Catalog-backed V1 Schedule/Prepare paths fail closed until the
first activation marker is durably applied, then return the distinct
pre-activation or post-close stable code; legacy constructors without a
Profile catalog remain an explicitly bounded compatibility seam. Exact
duplicate commands still reuse their durable first result. `InMemoryProfileCatalog` supplies exact
immutable Profile/binding/head/protection lookup and checked local deprecation
state; signed control target authority, source-ordered activation routing,
historical retention and provider verification remain external blockers.
Profile marker comparisons also reject equal order tokens with conflicting
canonical Source Position metadata.

`PayloadProofTrustSetSemanticV1` and `PayloadProofVerifierKeyV1` now provide
the canonical sorted verifier-key list, semantic hash/ref and Ed25519 raw-key
projection; `PayloadProofTrustSet.fromSemantic` is an explicit local adapter
and applies each key's source-time validity window during proof verification.
`PayloadProofTrustSetControlState` now provides the replay-stable local marker
projection: activation versions and Source Positions are strictly ordered,
exact marker replay is idempotent, issuance-close is keyed by the pinned trust
set and proof-key version, first-seen issuance closes at the marker while
historical verification remains allowed, and the complete marker state has a
canonical repeated-field value encoding for `meta_cf` persistence.
Trust-set marker comparisons reject equal order tokens with conflicting
canonical Source Position metadata.
`InMemoryPayloadProofTrustSetCatalog` now resolves exact local semantic
references without fallback. Authenticated source-ordered control authority
and Recovery-Floor historical verifier retention remain external blockers; the
state projection does not invent those authorities.

Large-payload Commit verification now selects its authority from the durable
Prepare binding rather than from the current Commit body's encoding. A
Registry V1 Prepare therefore pins its exact trust-set version/semantic hash
and source-ordered issuance state even when the later command uses the legacy
Commit body; only a legacy Prepare without a `V1ScheduleBinding` may use the
legacy version-only verifier. Missing or mismatched V1 semantic authority fails
closed instead of falling back. The mixed-body regression
`DelayShardTest.registryPrepareCannotDowngradeTrustSetAuthorityWithLegacyCommitBody`
uses different keys under the same trust-set version and verifies that the
legacy key is rejected without consuming the reservation or quota. External
catalog durability, authenticated control publication and Recovery-Floor key
retention remain release blockers.

The local handle/attestation facade now uses the same durable authority before
Object Store registration. `EmbeddedDelayService` reloads the reservation's
`V1ScheduleBinding`, decodes the exact Prepare trust-set ref and calls the
adapter's strict registration entry; `InMemoryPayloadObjectStore` rejects an
adapter semantic that shares the version but not the semantic hash before it
registers the reservation or issues a receipt/handle/proof. Legacy Prepare
without a V1 binding retains the version-only compatibility entry. The direct
adapter regression and the close/reopen facade regression cover both the zero-
registration side effect and the public `INTEGRITY_ERROR` projection. Real
provider credentials, Oxia publication and service authentication remain
external blockers.

The closed control-payload branches for trust-set activation and issuance
close now have typed `ControlReasonV1`/payload codecs with strict branch and
optional-field ordering, and `ApplyShardControlBody` exposes their typed
decoders. `DelayShard` now resolves the referenced semantic value through an
explicit catalog seam and persists the marker state, mutation result and
source position in one WriteBatch; reopen restores the same activation and
issuance-close projection. The catalog itself, authenticated control
authority, and Recovery-Floor historical verifier retention remain external
blockers, so the local marker apply does not claim production trust authority.
Lane PAUSE/RESUME/BREAK/CLOSE projection now also delegates its nested
`ControlReasonV1` and `AcknowledgementSetV1` values to the canonical codecs;
unknown reason kinds, malformed optional hashes and non-canonical acknowledgement
entries cannot pass the manual lane-target projection. The negative coverage is
in `PayloadProofControlPayloadV1Test`.

`ApplyShardControlBody.hasAcknowledgement` and `allowOrderBreak` now reuse the
same typed/canonical ACK and varint decoders as `laneTarget`; callers cannot
observe a malformed ACK wire type through a leaked `IllegalStateException` or
an unchecked field read. `PayloadProofControlPayloadV1Test.laneAcknowledgementQueryRejectsMalformedAckWireType`
covers the direct query boundary. This is still local marker projection
evidence, not authenticated Oxia control authority.

The local Owner Lease adapters now enforce the closed lifecycle transition
matrix, including fail-closed backward transitions and fenced-lease
non-reactivation; renewal also rejects a response that changes the expected
lifecycle state, rather than silently accepting a same-identity lease in a
different state. `transitionOrRead` only performs response-loss rereads for a
transition allowed by that lifecycle graph, so an illegal request cannot be
turned into success by a coincidental current state. The real Oxia ephemeral
session/CAS authority remains a release blocker. Activation also rereads an
exact same-identity `ACTIVE_FOR_COMMANDS` successor after a lost transition
response, and that reread rejects a successor whose lease expiry moved
backwards. `OxiaOwnerLeaseStoreTest.transitionOrReadRejectsAResponseLossSuccessorWithShorterExpiry`
covers the monotonic-expiry fence.
The context-bound acquisition defaults in both `OwnerLeaseStore` and
`OxiaOwnerLeaseStore.LeaseCasBackend` now fail closed instead of delegating to
shard-only acquisition. A backend that has not implemented the atomic
assignment/session CAS can therefore not allocate an unbound live lease (or
leave one behind before the adapter notices the missing context); the local
regressions are `OwnerLeaseTest.shardOnlyOwnerLeaseStoreCannotFallbackForContextBoundAssignment`
and `OxiaOwnerLeaseStoreTest.backendWithoutContextBoundAcquireCannotAllocateAShardOnlyLease`.
The Oxia adapter also requires every successful acquire response to remain in
`ACQUIRING`; a backend cannot skip the lifecycle CAS into `RESTORING` or
`ACTIVE_FOR_COMMANDS` while the Worker has not yet established the next
authority boundary. `OxiaOwnerLeaseStoreTest.rejectsBackendAcquireResultThatSkipsAcquiringState`
covers this response fence.
The V1 assignment gate also rejects a non-null legacy lease context whose
assignment epoch is zero; it cannot silently authorize a positive-epoch
assignment. `OwnerLeaseTest.legacyZeroEpochLeaseContextCannotAuthorizeV1Assignment`
covers this compatibility-boundary fence.
Authority-gated activation now keeps the local shard in `CATCHING_UP` until
the exact `ACTIVE_FOR_COMMANDS` lease CAS (or its validated response-loss
reread) succeeds; `OwnerLeaseTest.authorityGatedActivationKeepsLocalGateClosedDuringLeaseCas`
covers the state observed inside the authority callback.
The authority-gated `OwnedDelayShard.beginDrain(OxiaOwnerLeaseStore, nowEpochMs)`
now uses the same exact-successor CAS boundary for
`ACTIVE_FOR_COMMANDS -> DRAINING`; response loss is accepted only for a same
owner/epoch/token/assignment/session successor that is still valid at the
observation time, and a failed, expired or identity-changing transition fences
the local view. `OwnerLeaseTest.authorityGatedDrainRequiresTheExactLeaseSuccessor`
and `OwnerLeaseTest.authorityGatedDrainFailsClosedWhenLeaseIsExpired` cover the
local projection. This only closes new command admission; claim revocation,
publish callback quiescence, final checkpoint and lease release still require
the surrounding worker drain orchestration and real Oxia authority.
`OwnerLease.validAt` also requires a non-negative observation time, so a
directly reached local shard gate cannot turn a negative clock into ownership
authority; `OwnerLeaseTest.negativeClockCannotMakeOwnerLeaseValid` covers this
fail-closed boundary.

`PersistentOwnerLeaseStore` now provides a separate crash-durable local
projection for embedded/conformance runs. It persists the latest consumed
owner epoch even after release, binds context-bound leases to the exact
assignment/session bytes, and publishes every acquire/renew/transition/release
CAS through a bounded canonical snapshot with checksum, atomic rename,
directory fsync and JVM/on-disk locking. Reopen, stale-release, expiry
replacement, context mismatch, shard-identity mismatch and corruption
fail-closed cases are covered by `PersistentOwnerLeaseStoreTest`. This is only
local restart/response-loss evidence; it does not implement the production
Oxia session/ephemeral record or cross-worker CAS.

The V1 catch-up overload now pins an adapter-owned `SourceReplaySuccessor` in
the accepted assignment window. Exact canonical redelivery remains
idempotently admissible, while every later source position must be proven as
the immediate physical successor; `SourceReplaySuccessor.strictKafka()` rejects
an offset gap before the skipped record can reach `DelayShard.apply`, and the
Pulsar batch-member helper intentionally leaves cross-entry succession to the
broker adapter. The assignment-only compatibility overload remains
monotonic-only and is not V1 source-gap evidence. `SourceReplaySuccessorTest`
and `OwnerLeaseTest.v1CatchupPinsTheAdapterSuccessorAndRejectsAKafkaGapBeforeApplyingIt`
cover the local fence; real broker fetch/assignment continuity evidence is
still a release blocker.
`OwnedDelayShard` also exposes live-clock overloads for catch-up and mixed
source replay. They reread lease validity before every record, fence the local
view on mid-replay expiry, and leave the durable source cursor at the last
committed record; the fixed-time overloads remain deterministic compatibility
seams. `OwnerLeaseTest.liveCatchupClockFencesBeforeApplyingAfterLeaseExpiry`
covers the command replay boundary.
The live clock is itself part of that lease-validity proof: if the injected
clock throws or returns a negative epoch-ms, all three bounded replay paths
(Command, System Mutation and mixed) fence the local Owner before the failure
escapes, without consuming their look-ahead cursor or changing
`lastCatchupPosition`. `OwnerLeaseTest.replayClockFailureFencesEveryReplayPathBeforeReadingSource`
covers this pre-read boundary. This is local fail-closed evidence; production
trusted-clock and Oxia lease/session authority remain release gates.
`OwnedDelayShard.applyAuthoritatively` adds the corresponding per-command
authority reread for the post-activation path: a missing, changed, non-active,
expired or regressed-expiry Oxia lease fences before the delegate WriteBatch;
only an exact same-identity expiry extension is adopted locally. Its raw
`DelayShard` delegate accessor is package-private and limited to ownership
drain/inspection code, preventing external callers from bypassing the owner
lifecycle and lease gates.

The source replay seam now also exposes bounded `replayCatchupTurn`,
`replaySystemMutationsTurn` and mixed `replayTurn` APIs. Each turn is capped by
record count, canonical replay bytes (the exact source-position bytes plus the
canonical NDL1/System Mutation frame) and elapsed monotonic time. A
caller-owned `SourceReplayCursor` keeps one look-ahead record, so a byte cap
never consumes the first record of the next turn; `SourceReplayTurn` reports
whether another turn is needed. The legacy whole-`Iterable` methods remain
compatibility conveniences and use an explicitly unbounded budget; they are
not the production source-consumer boundary. Each replay branch advances the
cursor only after the shard WriteBatch returns successfully, so validation,
fencing or storage failure leaves the exact physical record available for
retry. `OwnerLeaseTest` covers cursor continuation, fail-closed single-record
byte overflow, and retention of the exact look-ahead record after a source-gap
failure. Mixed `SourceReplayOutcome` results additionally project a logical
duplicate's result to the current physical Source Position while leaving the
durable first-result anchor unchanged; `OwnerLeaseTest` covers command and
System Mutation duplicates plus canonical-metadata mismatch rejection.

All `replayCatchup*`, `replaySystemMutations*` and mixed `replay*` methods are
package-local ownership seams, including their fixed-time and whole-iterable
compatibility overloads; the direct-replay API visibility regression in
`OwnerLeaseTest.directReplaySeamsAreNotPublicProductionApi` prevents a future
public bypass of `SOURCE_APPLY`. Cross-package production composition must use
`SourceApplyWorkClassExecutor`.

`CheckpointScheduler` now provides a bounded process-local schedule for each
owned shard: interval and deterministic per-shard jitter are validated, due
claims are sorted and capped, an in-flight shard cannot be claimed twice, and
completion reschedules from the observed completion time only when the exact
claim handle returned by `claimDue` is supplied. Completion compares that
handle by object identity, so a value-equal handle reconstructed from
`shardId + dueAt` is rejected. A shard-only completion is a fail-closed
compatibility trap, so a late callback from an earlier claim cannot reset a newer
attempt; `CheckpointSchedulerTest` covers reconstructed-handle, stale-claim
and shard-only paths. Jitter percentage calculation divides before multiplying,
so a valid percentage of a near-maximum interval does not fail on an
intermediate `long` overflow; `CheckpointSchedulerTest.largeIntervalJitterUsesCheckedWideArithmetic`
covers the accepted boundary and the rejected jitter span. Completion timestamps
before the claimed `dueAt` are rejected before clearing the in-flight marker, so a
misordered callback cannot move the next schedule backwards;
`CheckpointSchedulerTest.completionBeforeClaimDueFailsClosedAndKeepsClaimInFlight`
covers that fence. It is only a local
worker scheduling primitive;
checkpoint manifests, upload intents and Oxia catalog publication remain the
durability authority.

`ShardStore.createCheckpoint(checkpointId)` now acquires the Worker-level
checkpoint-create slot before mutating `meta_cf` checkpoint identity. A full
create budget therefore rejects before any metadata WriteBatch; rollback of the
previous identity is reserved for failures after that mutation was attempted.
`ShardStoreTest.checkpointCreateSlotRejectionDoesNotMutateCheckpointProjection`
covers the pre-admission side-effect fence.

`CheckpointManifestLimits` now provides the explicit physical guard required by
the manifest protocol: file count, individual/total file bytes, normalized path
bytes, canonical manifest bytes, evidence-cursor count and file/manifest-object
identity lengths are checked before local file hashing or provider I/O; the raw
manifest byte cap is enforced before JSON parsing, and the activated file/evidence
array bound is enforced while the canonical parser is materializing the array.
`CheckpointFileInventory`, canonical manifest decode, upload coordination and
the finite-limit restore overload all use the same limit set; restore also
re-inventories the private copied tree against the manifest before staged open.
Legacy no-limits
overloads remain compatibility seams and are not production activated limits.
Inventory and manifest file ordering compares normalized names by unsigned
UTF-8 bytes, matching the Registry rather than Java UTF-16 string order.
The inventory and manifest limit aggregators also convert total-byte `long`
overflow into the same fail-closed validation error instead of leaking
wrapping arithmetic (`CheckpointManifestTest.manifestTotalFileBytesOverflowFailsAsValidationError`).
Restore `copyTree` also consumes the source walk through a streaming iterator,
so validated checkpoint restore does not materialize the entire path tree again;
inventory canonicalizes and rejects path names before hashing any file, and
the copied `restore-tmp` tree is re-inventoried against the same manifest before
the staged RocksDB open/install boundary. This closes the copy-time truncation
or source-mutation window rather than relying on RocksDB readability alone;
restore-tmp cleanup uses post-order `walkFileTree` deletion without a sorted
whole-tree list.

Shared RocksDB resources also retain checkpoint create/upload/download slot
counts and reject close while any bounded worker operation is still in flight.
Shard open/restore acquisition now has a separate short-lived
`maxConcurrentAcquiresPerWorker` slot, released immediately after native DB
open or failure cleanup; `ShardStoreTest.workerAcquireSlotIsReleasedAfterOpenAndFailsBeforeOpeningWhenHeld`
proves both the pre-open rejection and the post-open release without confusing
acquisition concurrency with long-lived owned/DB capacity.
Checkpoint restore/download staging now holds its own Worker-level slot across
manifest/file validation, restore-tmp copy, validation opens, and atomic
installation; it is released only after the active DB is opened or cleanup
completes. `ShardStoreTest.completeCheckpointRestoresIntoFreshStoreIncarnation`
also reacquires that slot immediately after a real restore returns, proving
the slot is released before the caller closes the restored DB.
`ShardStore.close()` now writes the clean-close marker before fencing public
operations, closes the default Column Family as well as every named handle and
DB/options, and records each item independently. DB/owned-shard slots are
released only after the full native teardown is complete, so shared resources
cannot pass their in-flight check while this Store still owns a native handle.
An earlier JNI close failure is aggregated with later failures, but a
successful item is not repeated or released twice; a later `close()` retries
only unfinished teardown and marks the Store permanently closed only after
every handle and slot is complete. A failed native shutdown therefore cannot
strand capacity or cause premature shared-resource destruction.
`SharedRocksDbResources.close()` applies the same retryable all-resources rule
to the process-level rate limiter, shared write-buffer manager and block cache:
a close failure fences new acquisitions without discarding the unfinished
resource state, and a later close retries it while preserving the first
failure/suppressed diagnostics.
`EmbeddedDelayService.close()` now fences client operations only after its
final synchronous drain and keeps the fenced service retryable until both the
Store and shared Worker resources finish teardown; a first close failure cannot
make a later close a no-op.
The long-lived owned-shard slot is also bound to the exact `ShardId`, not only
to a numeric semaphore count. A second `ShardStore.open` for the same Shard
therefore fails before it can open or create another RocksDB incarnation; this
also closes the race where two no-`ACTIVE` opens could otherwise install
different incarnations and leave one still-open DB outside the active pointer.
The identity is released only with the corresponding Store close, and
`ShardStoreTest.duplicateOwnedShardOpenIsRejectedBeforeCreatingAnotherDb`
covers rejection, single-DB preservation and reopen after release.
The same process-level resource envelope now exposes
`maxConcurrentDrainsPerWorker` and a shared drain slot. It is bounded
independently from DB and checkpoint slots and prevents
`SharedRocksDbResources` from closing while an owner-drain window is still
registered; `ShardStoreTest.drainSlotIsWorkerBoundedAndCloseProtected` covers
contention, release and close protection. The slot is a limiter for worker
orchestration; the full claim-quiescence/final-checkpoint/lease-release
sequence remains a separate production drain blocker.
`DelayShard.revokeClaimsForOwner` now supplies the bounded local drain step for
reversible `CLAIMED` work: it scans the exact Owner Epoch under the shard
single-writer lock and restores each Claim through the existing atomic
timeline/Message/READY rollback. An over-bound scan fails closed, and a second
pass is idempotent; `DelayShardTest.localClaimIsDurableAndRevokeRestoresTimelineAtomically`
covers the persisted path. This does not revoke already-admitted
`PUBLISHING`/`UNCERTAIN` obligations or prove callback quiescence.
`ShardStore.flushAndSync` now makes the planned drain persistence boundary
explicit by waiting for all Column Family flushes and then synchronizing the
WAL; `ShardStoreTest.flushAndSyncMakesTheShardBoundaryExplicit` verifies the
value after a close/reopen. It is a local physical primitive, not proof that
all remote callbacks have quiesced.
`DelayShard.listOpenPublishAttempts` provides the corresponding bounded local
view of live `PUBLISHING`/`UNCERTAIN` ledgers for drain/recovery polling; it
rejects duplicate attempt identities and over-bound scans instead of guessing.
The admission regression asserts the exact ledger is visible in this view.
`OwnerDrainCoordinator` now composes the local planned-drain order: caller-owned
source/scheduler stop, authority-gated `DRAINING` CAS, owner Claim rollback,
bounded callback polling, lease/deadline rereads, flush/WAL sync, optional
physical final checkpoint, Store close and exact lease release (an empty
current-lease reread is accepted only as release response-loss evidence).
The coordinator also acquires a shard-local drain-attempt gate in
`OwnedDelayShard`; the Worker-wide drain semaphore alone cannot prevent two
coordinators from concurrently closing or releasing the same shard DB/lease.
The gate is released on both success and fail-closed retry paths, while a
second coordinator receives `owner drain is already in progress for this shard`.
`OwnerDrainCoordinatorTest.duplicateCoordinatorCannotDrainTheSameShardConcurrently`
covers this same-shard boundary.
When the caller supplies the final checkpoint's exact 16-byte identity,
`OwnerDrainCoordinator` submits it through the shared `CHECKPOINT` work class;
the selected `CheckpointDrainWorkClassExecutor` passes it into
`ShardStore.createCheckpoint`, so the identity is present in the copied DB
metadata. The package-local four-argument compatibility seam cannot create a
final checkpoint without a shared registry.
After `flushAndSync`, an optional `commitSourceHint` callback receives only the
last persisted `SourcePosition`; the coordinator rereads the draining lease
after that transport-owned callback before continuing.  The hint is never the
recovery authority.  It also rereads the lease after the physical final
checkpoint has been installed, before Store close or exact release, so a lease
loss during a long RocksDB checkpoint cannot make the old owner close or
release a newer owner's state. `OwnerDrainCoordinatorTest` covers this
post-checkpoint fence.
If Store close itself fails, the coordinator leaves the local shard in
`DRAINING` and keeps the authoritative lease instead of releasing a lease
whose DB shutdown was not confirmed. The next drain call detects the fenced
Store and retries only native/slot teardown; it does not repeat Claim revoke,
callback polling, flush or checkpoint decisions. If Store close succeeds but
exact lease release is not confirmed, the same `DRAINING` state is retained so
the next call retries only the release; the close-success cleanup path does not
fence the shard early and strand the lease. Only confirmed Store close and
exact lease release move the shard to `FENCED`. The deterministic
`OwnerDrainCoordinatorTest.storeCloseFailureLeavesDrainingStateForRetryableTeardown`
and `OwnerDrainCoordinatorTest.unconfirmedLeaseReleaseKeepsClosedDrainRetryable`
regressions cover these boundaries.
If Store teardown was started externally while the Owner was still
`ACTIVE_FOR_COMMANDS`, the coordinator now performs the matching authority
transition to `DRAINING`, invokes the stop callback, and enters the same
close/release-only retry branch instead of throwing forever with the lease held.
`OwnerDrainCoordinatorTest.externallyStartedStoreCloseEntersDrainAndReleasesTheMatchingLease`
covers the local emergency-drain path.
Callback quiescence and source hint commit remain caller/transport boundaries;
timeout leaves the DB and lease in visible `DRAINING` for a safe retry rather
than claiming completion. `OwnerDrainCoordinatorTest` covers success and
deadline-failure sequencing.
`WorkerResourceEnvelope.validate` also requires the explicit shared block cache
plus WriteBufferManager budgets to fit inside the certified
`maxRocksDbNativeBytes` bucket before JNI resources are created;
`WorkerResourceEnvelopeTest.rejectsSharedRocksDbBudgetsOutsideTheCertifiedNativeBucket`
covers the fail-closed boundary. `WorkerRuntimeResourceProbe` now reads the
actual JVM heap limit, an explicitly bounded `MaxDirectMemorySize`, procfs
RSS/RLIMIT values, the live `/proc/self/fd` descriptor count, cgroup v2/v1
memory limits and the exact root filesystem's total/usable bytes; missing,
unlimited or malformed platform values fail closed. `WorkerResourceEnvelope.validate(config, observation)`
also checks the current descriptor count plus configured FD headroom against
the certified process limit. It compares those values against the certified
envelope, and `SharedRocksDbResources.withRuntimeProbe` exposes the startup
wiring. `WorkerRuntimeSafetyGate` now provides the explicit
sticky `ACTIVE -> DRAIN_OR_MIGRATE` transition for a failed fresh observation,
plus `STAGED -> DRAIN_OR_MIGRATE -> ACTIVE` only after the old DB/ownership
boundary is empty; shared shard acquisition/restore slots and the embedded
Claim helper consult the gate before admitting new work. Direct activation from
`STAGED` is rejected until the caller explicitly enters `DRAIN_OR_MIGRATE`;
`WorkerRuntimeSafetyGateTest.stagedEnvelopeRequiresExplicitDrainTransition`
covers that lifecycle fence. Parser, envelope and gate regressions are covered
by `WorkerRuntimeResourceProbeTest`, `WorkerRuntimeSafetyGateTest` and
`WorkerResourceEnvelopeTest`. This remains an
explicit probe/wiring seam. `WorkerRuntimeResourceMonitor` now supplies a
daemon, fixed-delay runtime probe that can be started after startup validation;
probe exceptions and envelope mismatches call the same sticky safety gate,
record bounded failure evidence, and fence new ownership/Claim admission until
an explicit empty-drain activation. Its close lifecycle and both failure paths
are covered by `WorkerRuntimeResourceMonitorTest`. Per-work-class reserve
enforcement and full write-time admission authority remain separate release
gates. `NativeResourceUsage` and
`WorkerNativeResourceLedger` now provide the local disjoint RocksDB-native
bucket attribution seam (block cache, memtable, table-reader metadata, pinned
blocks/iterators and flush/compaction scratch) plus a separate other-native
bucket; every allocation has an exact identity, checked aggregate and
idempotent release. `SharedRocksDbResources` reserves the configured shared
block-cache and WriteBufferManager budgets through that ledger before opening
the JNI resources, and releases them only after the corresponding native close
succeeds. `SharedRocksDbResources.startRocksDbUsageMonitor` now owns a
closeable fixed-delay observer over every registered open shard DB; each
observation validates per-DB and Worker WAL/MANIFEST/SST/compaction/file and
filesystem floors through `RocksDbUsageLimits`, and probe failures fence the
same sticky runtime gate. Registration is removed before Store native teardown,
so a closing DB cannot be mistaken for a zero-usage observation. Production
write-time attribution and authoritative checkpoint/compaction admission still
remain release gates. `WorkClass`, `WorkClassPolicy`,
`WorkClassTask` and `WorkClassScheduler` now provide an eight-class local
queue/turn seam with bounded queue records/bytes, per-turn records/bytes/time,
lease/fence preemption and stale-class selection. The service timestamp used
to mark a class served is read before any head removal or fairness mutation, so
an invalid monotonic-clock sample cannot drop a queued head
(`WorkClassSchedulerTest.invalidClockSampleDoesNotDropHeadBeforeTurnMutation`).
One bounded `poll` is also an in-memory mutation boundary: it snapshots the
queue, queued bytes, credits, cursor, last-served values and preemption debt;
if a later clock, selection or checked-arithmetic failure occurs after a head
was selected, the exact turn projection is restored instead of dropping a task
whose result was never returned. `WorkClassSchedulerTest.clockFailureAfterAHeadWasSelectedRollsBackTheWholePoll`
covers this rollback; the monotonic clock high-water may remain conservative,
but an interrupted poll is not recorded as served.
`LaneScheduler` now owns the same injected monotonic/non-negative clock guard
and reads it before removing a head; `LaneSchedulerTest.clockRegressionAfterAHeadWasSelectedRollsBackTheWholeLanePoll`
covers the regression rollback. `WorkerScheduler` rejects negative or backward
samples at the outer boundary, and `PersistentLaneScheduler` applies the guard
to bounded READY discovery before decoding or advancing its cursor. These are
local elapsed-time guards only; Trusted UTC and production Owner/Oxia clocks
remain release authorities.
`WorkClassResourcePool`
additionally protects other classes' non-borrowable record/byte minima and
bounds borrowed lease holds, covered by `WorkClassSchedulerTest` and
`WorkClassResourcePoolTest`. Production Worker event-loop wiring, chunk-level
token reacquisition and write-time authority remain release gates.
`SharedRocksDbResources.startRuntimeResourceMonitor` and
`startRocksDbUsageMonitor` own their monitor instances and close them before
native resource teardown, so neither local monitor lifecycle can outlive the
shared Worker resource owner.
Restore admission only treats a checksum-validated `ACTIVE` pointer target as
the live incarnation; an orphan incarnation left before pointer installation
does not block a new atomic restore and remains available for later repair.
Normal `ShardStore.open` applies the same `NOFOLLOW_LINKS` rule to the
`ACTIVE` pointer, incarnation directory, DB directory and `CURRENT` marker;
any symbolic path is rejected before it can be opened, so open and restore
cannot disagree about the live-incarnation boundary; restore admission also
rejects a symbolic incarnation or DB path behind a valid `ACTIVE` pointer.
The fixed worker-owned `shards/<routeIncarnation>/<partition>` ancestors are
now created and checked one component at a time as real directories as well;
an operational symlink at any of those ownership-boundary components fails
closed before RocksDB creation or restore staging, so a shard cannot redirect
its DB outside the configured root namespace. `ShardStoreTest`
`openRejectsSymbolicShardPathAncestors` covers the `shards`, route and
partition ancestors, and confirms no external `CURRENT` marker is created.
The DB open path now applies the same component-by-component check to every
descendant below the configured root, including a newly created Store
Incarnation or restore staging parent; a raced `FileAlreadyExists` is accepted
only after the component is rechecked as a real directory. This closes the
remaining `Files.createDirectories(dbPath)` symlink-following window before
RocksDB opens the physical DB.
If the `ACTIVE` pointer itself names a missing or non-directory DB, restore
now fails closed instead of treating the corrupt pointer as an orphan and
overwriting it; `ShardStoreTest.restoreRejectsAnActivePointerWhoseDbIsMissing`
covers this store-integrity boundary. An absent `ACTIVE` pointer remains the
only case in which an orphan incarnation can be replaced by a new restore.
Runtime validation failures after staging begins now remove the private
`restore-tmp` tree as well as releasing the download slot; after the staged DB
is atomically moved, restore opens it through the formal active path before
writing `ACTIVE`, and pointer-install failure closes the DB and removes only
the unreferenced incarnation it owns when teardown completes (an unreadable
pointer or an incomplete retryable close is preserved for offline repair).
Pointer-install cleanup retries the opened Store once and preserves the
original I/O failure with suppressed teardown diagnostics.
`ShardStoreTest.failedActivePointerInstallRemovesUnpublishedDb` covers the
`ACTIVE.tmp` failure path; a pre-acquisition concurrency rejection keeps its
original bounded-resource error.
After RocksDB itself has opened, the normal shard-open path has one failure
cleanup boundary around metadata reads/decoding, format and identity checks,
and install-mode writes: every DB/Column Family handle and options object is
closed before Worker DB/owned-shard slots are released.  The malformed-metadata
reopen regression `ShardStoreTest.malformedExistingMetadataDoesNotLeaveRocksDbOpen`
proves that the same physical DB can be opened again after this failure path,
so a local validation error cannot leave a native RocksDB file lock behind.
The open-failure cleanup now attempts every Column Family handle, DB/options
object, and the original pointer-install Store close even when one native close
reports a runtime failure; later retryable close failures are aggregated rather
than masking the original I/O or validation error.
Restore's staged validation, install-mode probe, and formal installed open now
also use explicit Store lifetime management. Failure cleanup makes a bounded
retry of retryable native/slot teardown and deletes `restore-tmp` or an
unpublished incarnation only after every staged/prepared/installed Store is
confirmed fully closed. If any Store cannot be proven closed, its directory is
retained for offline repair instead of deleting a path that a native handle may
still be using.

`StoreRuntimeMetadata` now provides the remaining local `meta_cf` runtime
projection required by the design: optional `lastIngressFenceProofId` and
`lastCheckpointId`, a non-decreasing `lastOpenedOwnerEpoch`, canonically sorted
typed `evidenceCursors`, and a `cleanCloseMarker`.  The physical projection is
stored at the registered `meta/FIXED` keys 4, 6, 7, 8 and 9, each with the
fixed-key ValueEnvelope type; key 4 uses one canonical `IngressFenceState`
containing both the source-ordered close deadline and proof identity, so the
DelayShard fence and Store projection cannot overwrite one another.  Key 10
now stores a bounded canonical `CompatibleControlSnapshotV1` in the fixed-key
ValueEnvelope type: it binds the shard subject, non-empty ProtocolTuple set,
sorted ProfileRef set and initial QuotaGrantRef to a domain-separated digest.
Open/restore strictly decodes the value, verifies the digest and rejects a
foreign shard identity; `ShardStoreTest.compatibleControlSnapshotIsPersistedAndRevalidatedForItsShard`
and `CompatibleControlSnapshotV1Test` cover the projection and codec.  This is
only the local copy of an already obtained control input; Oxia catalog/session
and version-read authority remain external blockers.  The Java projection and
evidence-cursor array have bounded canonical encoding and strict decode/round-trip checks.  Opening a DB validates every fixed-key value
and synchronously clears the clean-close marker; normal close persists it
synchronously, while explicit owner/fence/checkpoint/evidence updates use the
same WAL-synced WriteBatch boundary.  This is only local Store evidence: it
does not create Oxia lease/catalog authority or certify a remote Broker fence.
`StoreRuntimeMetadataTest` covers the registered physical keys and lifecycle
behavior, and `ShardStoreTest.malformedRuntimeMetadataDoesNotLeaveRocksDbOpen`
covers the activation failure cleanup path.
The immutable `meta/FIXED` format and shard-identity values at keys 1 and 2
now use the same fixed-key ValueEnvelope as the mutable projection: the format
payload is the canonical u32 value `1`, and the identity payload is the
canonical `StoreMetadata` bytes.  Open and restore-install paths decode and
CRC-check those envelopes before accepting a DB, and
`ShardStoreTest.fixedFormatAndIdentityValuesUseRegisteredValueEnvelope`
asserts the physical representation.  This closes the Registry requirement
that no `meta/FIXED` value is stored as an unframed raw byte sequence.
`ValueEnvelope` now also rejects numeric payload types outside the closed V1
range 1--11; the Registry records the context-specific mapping, including the
two GC task union branches and fixed control states, so a payload discriminator
cannot silently grow an unregistered schema.
`KeyCodec` now applies the same closed-subtype rule at key construction for
`timeline/SYSTEM` kinds 1--4 and `meta/QUOTA` classes 1--5; generic RocksDB
fixtures that need arbitrary scratch bytes use an explicitly raw test key
instead of widening the production Registry entry point. `gc/TASK` resource
kinds are bounded to the registered 1--10 range, and `dedupe/POSITION` rejects
an empty canonical Source Position payload. Its value type 3 is now the closed
command/system audit union (`commandId[41]` or `systemMutationId[32]`); the
System Mutation path writes and validates its branch at both the first and
later physical positions, so a duplicate can be replayed after restart without
confusing it with another record at the same physical position. The same-hash
Client Command path likewise validates its `commandId[41]` locator at the first
logical result position and after restart before reusing the first logical
result at a later physical position; `DelayShardTest` covers the missing-audit
and `laterDuplicateCommandReplayAfterRestartUsesPositionAudit` boundaries.
`ShardStore.open` now validates the remaining fixed-key activation boundary as
well: key 3's persisted Source Position must belong to this Shard, keys 5 and
11 must be non-negative fixed-width sequences, and keys 12/13 must carry their
registered non-empty control-state envelope types before the Store is exposed.
`ShardStoreTest.fixedControlMetadataIsValidatedBeforeShardActivation` covers
the type-mismatch path and confirms the native DB can be reopened after the
failed activation.
`ShardStore.createCheckpoint(path, checkpointId)` additionally writes the exact
16-byte identity before taking the RocksDB image, so a restored checkpoint
retains the identity it represents; a failed physical attempt restores the
previous projection.  The legacy path without an identity remains a local
physical primitive only and does not claim manifest/catalog publication.
The source-ordered `TIME_FENCE` apply path now writes its verified
`lastIngressFenceProofId` in that same batch as the mutation result and source
position, including the idempotent lower-watermark branch; the time-fence
reopen regression checks the proof identity.

The checkpoint code now covers the local physical boundary: create the complete
RocksDB image under the same-filesystem `checkpoint-tmp` namespace, atomically
rename it into the requested checkpoint path, checksum the full directory,
emit the closed manifest JSON projection, and install a validated checkpoint
into a new local Store Incarnation without merging into an open DB; restore
fsyncs each copied file and the staged directory tree before the incarnation
rename. The local
`checkpoint-tmp` parent is now required to be a real non-symbolic directory,
and `ACTIVE.tmp` is rejected before writing when it is a symbolic link or a
non-regular file; a failed restore/pointer install therefore cannot overwrite
an external target through a temporary path. `ShardStoreTest`
`checkpointAndActivePointerTemporaryPathsRejectSymbolicLinks` covers both
boundaries and verifies the external target bytes remain unchanged.
Normal `ShardStore.open` now applies the same directory-durability ordering as
restore: it fsyncs the DB directory and Store Incarnation parent after open and
before publishing the checksummed `ACTIVE` pointer, including when it adopts an
orphan incarnation.
`CheckpointUploadCoordinator` now inventories the exact local file set before
provider I/O, requires the RocksDB `CURRENT` marker, charges the Worker upload
slot, validates returned manifest
object identity and, for a recognized RocksDB image, verifies the physical
key-10 `CompatibleControlSnapshotV1` shard/digest against the manifest before
provider I/O; only then does it advance the exact pending intent to PUBLISHED;
provider failure or identity mismatch leaves the intent pending for retry. The
manifest `createdAt` projection now accepts only the four Registry time-evidence
source symbols and applies the signed-source key/signature presence rule before
canonical JSON is emitted. The
local store uses an `ACTIVE` checksummed pointer and an
`incarnations/<storeIncarnation>/db` directory. Typed
`CheckpointResourceV1`/`CheckpointUploadIntentV1` codecs now close the
manifest-object identity and PENDING/PUBLISHED/REAPING branch rules. The
local `CheckpointUploadIntentStore` also rereads an exact PENDING_UPLOAD ->
REAPING successor after a lost transition response; a different reaping
evidence value or pending identity remains a CAS conflict. When constructed
with a dedicated state file, the same store persists the complete canonical
intent with a checksum, temporary file, atomic rename, directory fsync and
JVM/on-disk lock; reopen, cross-instance CAS and checksum-corruption
fail-closed paths are covered by `CheckpointUploadIntentStoreTest`. The
no-argument constructor stays an in-memory compatibility seam. This only
closes the local intent projection; quiescence, exact-version Object Store
deletion, final prefix sweep and Oxia authority remain pending.
The local REAPING transition now also requires the trusted UTC interval's
earliest bound to be at least `uploadDeadlineEpochMs`; evidence before the
deadline leaves the intent PENDING. Owner abandonment/lease-loss authority,
provider quiescence and deletion remain external blockers. The guarded
`beginReaping(..., RecoveryCatalogAuthority)` overload additionally refuses a
published catalog entry, an active pin protecting the same lineage/checkpoint,
or an unavailable catalog/pin read (including a fatal `Error` from that
authority boundary); `CheckpointReapingGuardTest` covers the fail-closed
branches. This is still a local necessary-condition projection, not the atomic
Oxia reaper CAS or provider-owned request horizon.
embedded `RecoveryCatalog` now selects and validates a published floor-eligible
ancestry before local restore. Upload-intent catalog projection also accepts
an exact same-checkpoint/manifest/object-identity reread after publication
response loss while rejecting same-ID manifest-hash or object-version drift.
The `OxiaRecoveryCatalog` response boundary now rereads the exact published
manifest after scalar or typed Floor CAS and rejects returned lineage,
manifest-hash, source-position, mutation-sequence, or evidence-cursor drift;
typed responses must also be byte-equal to the requested cursor set. A missing
manifest or malformed Floor response therefore fails closed instead of being
accepted as a successful remote CAS. Read-only `currentFloor`/
`currentFloorRef` responses and `proveFloorCoverage` results apply the same
manifest binding, candidate/floor identity, requested mutation/source-boundary
coverage, and ancestry-endpoint checks; equal source order tokens still require
canonical Source Position equality.
`validatePublishedRestoreCandidate` also rereads the exact published manifest
and rejects a same-ID canonical projection drift before invoking the backend's
floor/recovery-set validation.
Typed `RecoveryCandidateRefV1` and
`RecoveryPinV1` codecs now close the candidate branch and session-bound pin
projection, but they are still local value codecs: immutable object publication,
durable Oxia catalog/session pin CAS, and Kafka/Pulsar source replay remain
release blockers below.
`PersistentRecoveryCatalog(Path)` now adds a crash-durable local projection for
the same catalog boundary: it stores sorted canonical manifests, immutable
manifest-object identities, scalar/typed Floors and the active Recovery Pin in
a bounded checksummed snapshot, publishes replacements with temp-file write,
atomic rename and directory fsync, and serializes JVM/on-disk access. Reopen,
cross-instance generation CAS, Floor/Pin/ancestry recovery, scalar Floor
canonical decoding and checksum corruption fail-closed are covered by
`PersistentRecoveryCatalogTest`; this remains an embedded projection and does
not replace Oxia Owner Lease/session or catalog authority.
`ShardStore.restoreFromCheckpoint(..., catalog, pin)` now adds the exact active
RecoveryPin to the local install boundary: it validates the pin against the
candidate and rereads the current Floor-bounded ancestry plus the same pin
after staged DB validation, immediately before moving the new Store Incarnation
into place, and once more after the formally opened incarnation is fsynced,
immediately before publishing the checksummed `ACTIVE` pointer. A Floor that
has advanced beyond the candidate, or a missing or changed pin, leaves no
published pointer and only recoverable private/orphan state; this is a local
fail-closed guard, not the production Oxia Lease/session transaction. The
late-session-drift path is covered by
`ShardStoreTest.catalogBoundRestoreRejectsPinDriftBeforeActivePublication`.
When a manifest is supplied, staged restore now also compares the physical
image's persisted `lastCheckpointId`, `appliedShardLogPosition`,
`shardMutationSequence` and typed evidence-cursor projection against the exact
manifest values before install. A complete file inventory with a mismatched
runtime state is therefore rejected rather than restored as if it represented
that manifest. If the staged image contains the fixed-key
`CompatibleControlSnapshotV1`, restore also compares its canonical digest with
`CheckpointManifest.controlStateDigest` and rejects a mismatch before install;
an image without key 10 remains only a legacy local compatibility seam and
cannot prove the V1 `ACTIVE_FOR_COMMANDS` control prerequisite. The focused
regression is `ShardStoreTest.restoreWithManifestRejectsControlStateDigestDrift`;
`ShardStoreTest.restoreWithManifestRejectsRuntimeStateDrift` and
`ShardStoreTest.catalogBoundRestoreRejectsPinDriftBeforeActivePublication`
cover the other rejection and matching paths. Source replay after restore and
the external catalog/object-store authority remain release blockers.

`EvidenceCursorV1` now also exposes the Registry cursor identity and
same-generation dominance rules: Kafka requires non-regressing offset/LSO/time
watermarks, while Pulsar compares the inclusive ledger/entry/batch member and
the same Broker-time anchor. Cross-generation cursors remain incomparable.

The AUTO_FAST native submission boundary now has a local, identity-pinned
Pulsar transport SPI. `PinnedPulsarNativeSubmissionAdapter` verifies the
signed capability snapshot, expiry, prepared target and physical attempt
before Producer ownership, then maps persisted, guard-rejected, uncertain and
pre-ownership failures to the closed native submission union. This is an
adapter contract and deterministic test seam only; it does not claim a real
Pulsar Broker transport, durable guard/credential protection, or production
response attestation. A `DEFINITIVELY_NOT_PERSISTED` transport disposition is
now proof-bearing only with the registered Broker/guard rejection stable code;
an unknown or mismatched code is downgraded to `NATIVE_ENQUEUE_UNCERTAIN` with
an integrity diagnostic. A transport `CompletionStage` that rejects callback
registration is treated as the same post-ownership uncertainty and returns
`NATIVE_ENQUEUE_RESULT_UNCERTAIN` with the original physical attempt id rather
than leaking an exceptional Future. The same disposition/code binding is enforced for
managed Kafka/Pulsar wire projections, so a malformed shared transport result
cannot become a non-persistence proof. `AdapterIngressTest` and
`NativeSubmissionAdapterTest` cover these downgrade vectors.

When the native adapter is constructed with a `CredentialFingerprintProvider`,
it resolves the immutable credential fingerprint before Producer ownership and
compares it with the digest bound into the signed capability snapshot. A
mismatch returns `CREDENTIAL_BINDING_DRIFT`; an unavailable, null, malformed or
throwing resolver returns `AUTO_FAST_PREREQUISITE_UNAVAILABLE`, and neither
branch calls the Pulsar transport. The legacy constructors intentionally leave
the provider unset as compatibility seams; they do not claim a production
credential authority or durable rotation protection.

The same native pre-ownership gate now treats a clock that throws or returns a
negative epoch millisecond value as an unavailable AUTO_FAST prerequisite,
returning `AUTO_FAST_PREREQUISITE_UNAVAILABLE` without invoking the transport.
This prevents local time-source failure or an invalid epoch from becoming an
expiry decision or Producer ownership; `NativeSubmissionAdapterTest` covers
both cases. Clock certification and production Broker-time authority remain
external release gates.

`PulsarAttemptJournal` now provides a local mapping-before-send seam for the
strong Pulsar dedup branch. It scopes a stable Producer key to one Shard, allocates
strictly increasing sequences, rejects a second unresolved lower mapping, makes
exact mapping append replay idempotent, and exposes `appendOrReuse` plus the
identity-bound `sendAfterMapped(producer, attempt, sender)` entry point so an
exact attempt retransmission reuses its original mapping/sequence. The target
sender is never invoked until the Journal append/replay gate returns a durable
position, and a Journal append failure cannot be projected as a target send.
The seam persists
`RETIRED_NOT_PUBLISHED` before the next sequence is admitted. Recovery replay
rebuilds the same mapping/retirement projection, while a Broker sequence above
the Journal maximum or a lower sequence without both retention proofs returns
`PULSAR_EVIDENCE_DIVERGENCE`. `PulsarAttemptJournalTest` covers these local
ordering and fail-closed branches. The injected appender is only a deterministic
test seam; the Nereus-owned Pulsar topic, ExclusiveWithFencing writer, guarded
reader, retention/Floor proof and production Broker evidence remain release
blockers.

The same seam now projects the Registry `PULSAR_JOURNAL_ABSENCE` branch only
after an exact mapping has a durable `RETIRED_NOT_PUBLISHED` record. It binds the
explicit `PulsarJournalResource`-backed Journal cursor, a caller-supplied fenced Pulsar dedup channel, exact
Attempt/prepared/producer identity, sequence and a fixed retirement-barrier
digest; Lane, target, evidence-resource, partition and generation drift are
rejected before the evidence value is returned. This is still a local canonical
projection: the supplied channel/barrier are not an authenticated
`ExclusiveWithFencing` response, contiguous reader result or remote retention
proof, and the production absence capability remains blocked on those authorities.

`KafkaReceiptJournal` now provides the corresponding local seam for the
transactional-receipt capability. `KafkaReceiptResource` makes the receipt
cluster, topic UUID, Route/Shard, slot generation and
`shardPartition * receiptLaneSlotsPerShard + receiptLaneSlot` partition explicit;
the journal keeps one transactional-channel key per Shard, maps an exact
Publish Attempt before the target transaction sender, rejects unresolved lower
sequences, replays mapping/retirement records idempotently, and projects the
typed `KAFKA_RECEIPT_CONTIGUOUS` cursor. It also constructs the Registry
`KAFKA_TRANSACTIONAL_RECEIPT` PUBLISHED branch and the post-retirement
`KAFKA_RECEIPT_ABSENCE` branch, binding target/receipt UUIDs, partitions,
generation, transaction identity, prepared hash and receipt/barrier digests.
`ReceiptObservation`/`Resolution` now fail closed on receipt cursor identity,
attempt/prepared/record-hash drift, and require independent retirement, LSO
barrier and retention predicates for local `NOT_PUBLISHED` classification;
both `MAPPED` and `RETIRED_NOT_PUBLISHED` records now pass through the injected
appender with a non-null position, retirement advances the local cursor and
replay watermark, and an append failure leaves the lower sequence unresolved;
`KafkaReceiptJournalTest` covers the local ordering, replay, cursor, resolver
and identity-fencing vectors. The injected appender and caller-supplied fenced
channel remain only local canonical projections: real target-plus-receipt
Kafka transactions, `read_committed` Fetch/LSO proofs, ExclusiveWithFencing,
receipt retention/Floor coverage and authenticated Broker tests remain release
blockers.

The current source-ordered control increment is deliberately bounded: the
`RESOLVE_UNCERTAIN_V1(RETRY_ALLOW_POSSIBLE_DUPLICATE)` branch now validates a
canonical `ControlRefV1`, its Resolve logical identity, lane incarnation,
acknowledgement hash, current-generation UNCERTAIN obligation and source
position, then materializes one `UNCERTAIN_RETRY(CONTROL_OVERRIDE)` timeline
work item without consuming the Admission counter. A Claim created from that
timeline now freezes `sourceWorkKind=UNCERTAIN_RETRY` instead of deriving a
definitive-retry kind from the retry timestamp. Resolve evidence attachment
and authenticated Oxia target registration remain release blockers; the
`ATTACH_PUBLISHED_EVIDENCE` branch is now locally covered for an exact current
UNCERTAIN obligation even when the generation still has reversible timeline or
Claim work: verified success removes that work, terminalizes the generation,
marks possible duplicate, and releases the exact pending schedule quota
atomically; a different current PUBLISHING attempt remains open and is updated
without terminalizing it. Historical terminal-summary settlement is also
covered. The `ATTACH_NOT_PUBLISHED_EVIDENCE` branch now validates the exact
UNCERTAIN obligation and typed not-published evidence, settles old/terminal
summaries, preserves remaining uncertainty or another current PUBLISHING
attempt, and atomically revokes a stale current Claim to `UNCERTAIN/NONE`
when another obligation remains, checked-incrementing the Message state
version for that public status change. It normalizes an all-absent current generation
to definitive retry; `DelayShardTest` covers both the all-absent current
Claim path and the remaining-obligation Claim revocation path
(`sourceOrderedNotPublishedEvidenceRevokesClaimWhenAnotherUncertainObligationRemains`)
(or a closed-lane, budget-exhausted, or expired terminal outcome) in one batch;
its external authenticated evidence authority and full policy/charge proof
remain release blockers, while the
possible-delivery terminal branch is locally covered by retaining the exact
UNCERTAIN obligation while terminalizing the generation and releasing its
active pending quota.

The bounded replay increment now covers `REPLAY_DEAD_LETTER_V1` after a
`DEAD_LETTER` terminal decision: it checks the exact generation/state-version
precondition, terminal summary and duplicate acknowledgement rule, lane gate,
timing and shard quota, then atomically creates the next generation's
`INITIAL_SCHEDULE` timeline while retaining the old summary and obligations.
`ReplayDeadLetterBody.encode` now constructs the complete canonical mutation
body (including the optional duplicate acknowledgement), round-trips it through
the strict decoder, decodes field 16 as a typed canonical `RetryPolicyRefV1`,
and rejects a `messageId` whose self-routing Shard differs from the mutation
subject before the body can be signed. Immutable
RetryPolicy/Profile binding, replay-window/fence proofs, Oxia target
registration and full DLQ/replay retention remain pending.

The local `TIME_FENCE_V1` increment now validates the exact proof ID, fence key
version and the configured `DelayShardConfig.timeFenceSafetyMarginMs` Trusted-UTC
lower bound (`proof.earliestEpochMs >= checkedAdd(closeThrough, margin)`),
monotonically persists
`closedIngressDeadlineThrough`, and rejects later commands at the position level
without overwriting an existing command identity/result. The POSITION audit now
also makes an exact replay of a fence rejection or command-ID conflict
idempotent after a successful RocksDB batch with a lost source ACK; it returns
the same position-level result without creating a logical Command Result. Both
Command and System Mutation exact replays fail closed when the matching
POSITION audit is missing; the same-hash duplicate path also validates that
locator after restart before reusing the first logical result at a later
physical position. A later same-hash Command whose Broker persistence time
is outside its retry window now returns only position-level
`COMMAND_RETRY_WINDOW_EXPIRED`, keeps the first logical result unchanged,
and returns that same position-level result on exact replay and after
restart; `DelayShardTest` covers both replay paths. Reservation-expiry
watermark overlay now makes still-RESERVED payload reservations immediately
appear `EXPIRED` to Commit/Cancel/Query, while the bounded
`RESERVATION_EXPIRY` cursor materializes the state and releases reservation
quota without making a new source-log decision. Source-protected fence-key
history and full retention/GC proofs remain pending.

The source-ordered `APPLY_SHARD_CONTROL_V1` increment now covers the bounded
Lane `PAUSE_DESTINATION_LANE`/`RESUME_DESTINATION_LANE`,
`BREAK_ORDERING_DOMAIN`, and `CLOSE_DESTINATION_LANE` gate subset. Apply
verifies the ControlRef logical identity, Lane incarnation and control-version
CAS; validates the close policy and required `ORDER_LOSS` plus
`POSSIBLE_DUPLICATE` acknowledgements for strict work; and one WriteBatch
updates the gate/readiness/READY projection, reverses live Claims, restores
their exact timeline keys, persists the System Mutation result, and advances
the source position. Profile/grant activation, authenticated Oxia target
registration and production terminal-guard authority remain release blockers;
the same-key local terminal-guard replacement is implemented below.

The bounded local Lane-retirement path now releases the shard's physical
`laneCount` slot in the same WriteBatch that replaces the active Lane value
with its terminal guard. `ShardQuota.removeLane()` rejects underflow, the
quota projection survives reopen, and
`DelayShardTest.laneRetirementAtomicallyReplacesActiveValueWithTerminalGuard`
proves that a `maxLanes=1` shard can reuse the slot only after retirement.
This closes local slot accounting; external grant/Oxia release and the full
terminal-guard authority protocol remain release blockers.

The local query increment now exposes bounded read-only
`MessageQuerySnapshot` and `ReservationQuerySnapshot` projections. They derive
the exact current runtime/terminal state, state version, timing, duplicate-risk
bit and safe payload-availability category without exposing payload bytes,
destination lane identity, object-store keys, command hashes or receipt data.
The durable DLQ export state is part of the message snapshot and is the only
accepted source for the public DLQ projection; non-dead-letter generations must
remain `NOT_CONFIGURED`, and the compatibility projector overload rejects a
caller-supplied state that disagrees with the snapshot. Configured DLQ outboxes
persist a canonical policy-derived retained charge (with legacy v1 records
decoded as zero), and source-ordered result apply requires transfer equality
against that projection before advancing the outbox.
The wire-level closed unions are now also encoded: `CommandQueryResponseV1`
and `MessageQueryResponseV1` cover every Registry public view and error branch,
including source-barrier pending views, safe destination binding, retired
identity and evidence-reference projections. The codecs do not fabricate those
views from the local snapshots: receipt/barrier routing, authorization policy,
real binding/evidence lookup and source-derived retention decisions remain
release blockers.

The embedded conformance service now exposes the local Control Operation
register/advance/query entry points over the receipt-bound authority. They
preserve idempotent registration, revision CAS and the fixed query retention
boundary for tests. The Oxia validation adapter now requires an exact
`CurrentControlOperationV1` (identity, revision, state, targets and typed
result) for `advance` after response loss; a later or different CURRENT is not
accepted as proof of that CAS. It also rejects receipt-identity drift and
non-consecutive register/advance revisions before invoking the backend. The
in-memory authority uses an explicit checked-successor predicate: a
`Long.MAX_VALUE` expected revision has no representable successor and is
rejected before Java `long` wraparound (`ControlOperationAuthorityTest
revisionSuccessorFailsClosedBeforeLongWraparound`). They do not provide
production Oxia routing, authorization, or crash-durable control state.

The embedded query bridge now also compares a queued receipt's `commandHash`
with the immutable hash retained in the shard `dedupe_cf` record before
projecting a durable result or emitting an applied receipt. A same-`commandId`
receipt with a different command body/hash returns `RECEIPT_MISMATCH` (and is
rejected by the applied-receipt path), rather than exposing the result by ID
alone. `EmbeddedDelayServiceTest.embeddedQueryBindsReceiptCommandHashToDurableDedupeIdentity`
covers the fence; this remains a local identity check, not production gateway
authorization or routing.

The embedded query bridge also reads the exact `dedupe_cf/POSITION` audit through
`DelayShard.matchesCommandPosition` before projecting a retained command result
or emitting an applied receipt. A same-shard receipt forged with an earlier or
otherwise different physical position now returns `RECEIPT_MISMATCH` even after
the source barrier has advanced, and the applied-receipt path rejects it;
`EmbeddedDelayServiceTest.embeddedQueryBindsReceiptToExactPhysicalPositionAudit`
covers both paths. This is the local physical-locator fence, not production
gateway authorization, Oxia routing, or broker evidence.

`CommandQueuedReceiptV1` now also binds the `PreparedCommandRef` shard to the
receipt Source Position shard in its shared constructor, so both local creation
and canonical decode reject a command-from-A/source-from-B receipt. The
regression is covered by
`ProtocolCodecTest.commandQueuedReceiptRejectsACommandAndSourceFromDifferentShards`;
this closes the receipt's self-routing identity before any barrier/query path.

The local Admission increment now decodes the closed 17-dimensional
`ChargeVectorV1` and persists the non-borrowable outcome reserve usage in the
shard DB. Admission, its `PUBLISHING` ledger and the reserve projection share
one WriteBatch; when the configured records/bytes cap cannot fit the charge,
the source position advances with `ADMISSION_CAPACITY_GATED` and any
reversible Claim is restored to `SCHEDULED`. Definitive or verified terminal
settlement releases the exact charge atomically, and restart rebuilds the local
record/byte projection from durable `PUBLISHING`/`UNCERTAIN` ledgers before using
it for admission. A drifted present aggregate fails activation and a missing legacy
aggregate is backfilled in memory; the Registry class-2 aggregate vector and
class-3 per-Lane map are written together on the next source-ordered mutation,
while the old class-1/class-2 scalar projections are migration-only. The closed
protocol codec for the 66-dimensional
`CapacityVectorV1`, `CapacityGrantV1`, `QuotaGrantRefV1` and
`ShardCapacityEnvelopeV1` is also covered by canonical round-trip and
rejection tests, including component-grant projection and checked sums. This
is still only the local envelope/projection boundary: when an immutable
envelope is supplied at `DelayShard` activation, its outcome grant identity
and canonical envelope are bound under `meta/CONTROL_RESERVE` class 1, the
exact 66-dimensional charged outcome vector is persisted under class 2, and
restart or envelope rotation fails closed on identity, digest, projection or
grant-capacity drift. The legacy no-envelope constructor keeps the prior
records/bytes projection for compatibility, while `meta/QUOTA` class 2 remains
the canonical local aggregate described above. The `SLO_OUTBOX=08` key shape is
now also wired to a synchronous `SloObservationOutboxStore`: it persists an
immutable Start before ownership loss and atomically replaces the conservative
merged Final under the shard's `meta_cf` ValueEnvelope/CRC boundary. This is
also a bounded key-order scan plus exact key/value sample-identity and
record-digest delete-after-ACK boundary, with a caller-supplied
record/ValueEnvelope byte budget, so an exporter can retry unchanged
bytes without accepting a mis-keyed observation or deleting a newer one. The
identity fence is covered by `SloObservationOutboxStoreTest`. Final merge now also
revalidates the closed objective branch's required unit and direction before a
durable replacement; `SloObservationOutboxV1Test.rejectsFinalUnitAndMergeDirectionThatDisagreeWithObjective`
covers the semantic fence.
The durable store entry point now requires an explicit paired HEALTHY objective
when an ALL_ACCEPTED due Final carries an exclusion; the direction-only entry
rejects both a new excluded Final and a previously excluded projection instead
of allowing callers to bypass the catalog pair. The pair-aware overload also
requires the exact ALL_ACCEPTED objective whose digest is bound into the
durable Start; the older three-argument overload is a fail-closed compatibility
trap for excluded Finals. `SloObservationOutboxStoreTest`
`excludedFinalRequiresPairedHealthyObjectiveAtDurableBoundary` covers the
positive and bypass-rejection boundaries.
The source-position audit now closes the complementary System Mutation replay
boundary: `dedupe_cf/POSITION` value type 3 accepts only the registered
`commandId[41]` or `systemMutationId[32]` branch, and every durable System
Mutation WriteBatch records its mutation identity there. A verified duplicate
at a later physical position records the new locator while retaining the first
logical Source Position; an in-window duplicate reuses the first logical
result, while a duplicate outside its signed retry deadline or after a
`TIME_FENCE` produces only position-level
`SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED` and never overwrites the first
`dedupe/SYSTEM_MUTATION` value. Replay at that already-advanced position
requires the matching audit and returns the same position-level result;
command/system or different-mutation collisions remain fail-closed.
`DelayShardTest.systemMutationDuplicateOutsideRetryWindowKeepsFirstResultAndUsesPositionAuditOnly`,
`DelayShardTest.timeFenceMonotonicallyClosesIngressWithoutOverwritingCommandIdentity`
and the source-ordered System Mutation dedupe tests cover the boundary.
Equal-severity Finals now select the newest observation revision's evidence as
well as the conservative measurement; `SloObservationOutboxV1Test.mergeUsesNewestEvidenceWhenOutcomeSeverityIsEqual`
covers that replay boundary. This is
still local evidence only. Repeated due Finals with two different non-null
mutually-exclusive exclusion reasons now fail closed instead of silently keeping
the older reason; `SloObservationOutboxV1Test.mergeRejectsConflictingDueExclusionReasons`
covers that integrity boundary. Multi-shard placement/Oxia authority,
production non-outcome/recovery/emergency/source-writer reserve authority,
transfer protocol, SLO
identity-specific reconstruction/collector export and full GC accounting
remain release blockers. Activation now also scans reserve classes 3–6 and
rejects stale, over-capacity or cross-dimension non-outcome/recovery/emergency
projections instead of silently ignoring them. The local class-6 projection is
now implemented as a canonical `CapacityVectorV1` keyed by the
`NON_OUTCOME_CONTROL` grant ID: only dimensions 51–53 are accepted, class 3
cannot consume those dimensions, and their combined usage is checked against
the immutable grant. `DelayShard.systemWriterReserveUsage`,
package-local `reserveSystemWriterCapacity` and `releaseSystemWriterCapacity` persist the
projection synchronously. This still does not implement Route Broker
source-writer quota authority, remote reservation/charge, or multi-shard
placement, which remain release blockers.
`BoundedLocalQueryProjector` is the explicit local bridge when those policy
inputs have already been supplied; it does not perform the missing authority
steps itself. `EmbeddedDelayService` now connects its canonical queued receipt
to the fixed embedded source barrier, durable command-result lookup, retention
branch, bounded message snapshot projection, and distinct applied-receipt
frame. This is a local conformance
bridge only: it is not production receipt routing, tenant authorization,
Oxia ownership lookup, or a real Broker adapter.

The bounded `RESOURCE_RETIRE_INTENT_V1` increment now validates the closed
`ExactResourceIdentityV1` branches and canonical `ProtectionSetV1`, checks the
registered retire logical identity, and atomically persists an immutable
`gc_cf` intent with its applied shard mutation sequence and source position.
The expected resource-state version now remains a raw unsigned `uint64` from
canonical body decoding through logical identity, GC locator, durable record,
lookup and local compaction APIs; this does not relax the separate bounded
mutation-sequence counter. It deliberately does not perform an external
delete, apply `RESOURCE_DELETE_CONFIRMED_V1`, or infer Recovery Floor release.
The local same-key Lane terminal-guard replacement is implemented as the package-local
`DelayShard.retireLaneWithTerminalGuard` algorithm/test seam; external deletion/confirmation,
Oxia grant release and Recovery-Floor authority remain release blockers.
Protection-set `ProtectionRefV1.protection_generation` now follows the same
full-width rule through nested canonical bytes and the `gc_cf/PROTECTION` key;
high-bit protection references are covered by `ResourceRetireIntentBodyTest`
and `KeyCodecTest`. External protection authority and guarded GC release
remain outside this local key/value boundary.
The closed `ExactResourceIdentityV1` parser now keeps immutable `ProfileRefV1`
versions and Kafka receipt-slot/Pulsar journal external generations as raw
nonzero `uint64` values; its nested Pulsar Broker identity validator likewise
does not reject a high-bit physical-topic creation timestamp. These values are
identity inputs, not local counters, and high-bit coverage is included in
`ResourceRetireIntentBodyTest`; payload/checkpoint byte lengths remain bounded
by the local object/manifest admission envelope.
All seven Registry retirement branches now have typed protocol values with
canonical construction, decode and ExactResourceIdentity wrappers:
`PayloadObjectResourceV1`, `CheckpointResourceV1`, `DlqExportResourceV1`,
`KafkaReceiptSlotResourceV1`, `PulsarJournalGenerationResourceV1`,
`LaneChannelResourceV1` and `LocalStoreResourceV1`. `ExternalResourceBranchTest`
covers payload optional-etag handling, typed Broker/channel/shard identities,
raw unsigned Kafka/Pulsar fields and branch rejection, while
`KafkaReceiptResource.protocolResource()` exposes the slot geometry through the
same Registry identity and `PulsarJournalResource.protocolResource()` exposes
the Journal resource/generation identity. `PulsarJournalResource` now preserves
the complete raw `uint32` physical-partition bit pattern instead of rejecting a
Java signed high bit; `PulsarAttemptJournalTest.journalResourcePreservesUnsignedPhysicalPartitionBits`
covers the typed projection and canonical decode. This remains a local identity codec;
slot allocation, retirement authority and Broker/Oxia ownership are still
external.
`PublishAdmissionBody`'s nested ProfileRef and Broker-resource validators now
apply the same raw-version/physical-identity rule, so a canonical Admission
cannot reject a Profile version that the standalone Registry codec accepts.
`PublishAdmissionBodyTest.preservesUnsignedProfileReferenceVersionsAcrossAdmissionMaterialization`
covers both descriptor and ClaimMaterialization projections.

The bounded `RESOURCE_DELETE_CONFIRMED_V1` increment now validates the exact
Retire Intent reference, delete outcome, provider evidence and Trusted-UTC
interval, then source-orders a local `gc_cf/TASK` tombstone that retains the
full intent and confirmation evidence. It does not claim that the provider
delete happened merely because this mutation was accepted: local payload and
checkpoint identity/version fields are compared when present, while real
adapter ownership/attestation, Recovery Floor barriers, quota release and
Lane terminal-guard replacement remain pending.

`OxiaRecoveryCatalog.validateLocalStoreRecovery` now forwards the documented
local reuse proof to the Oxia CAS backend after rejecting an incomplete
`StoreRecoveryMetadata` projection locally. The delegating embedded backend
uses the same exact typed Floor, published base manifest, ancestry and
Store-Incarnation/install-state checks as `RecoveryCatalog`; the remote backend
remains responsible for binding that read to the current catalog and Owner
Lease/session transaction. `RecoveryCatalogTest.OxiaBoundaryForwardsLocalStoreRecoveryValidation`
covers the forwarding and incomplete/wrong-shard fences.

`ResourceGcGuard` now exposes both the scalar necessary-condition result and a
catalog-backed variant. The latter requires exact parent-hash ancestry from a
published candidate through the current local Recovery Floor, in addition to
the applied mutation sequence and Source Position checks. For `CHECKPOINT`
resources it also rereads the active `RecoveryPinV1`: a pin protecting either
the candidate checkpoint or the observed Floor returns
`RECOVERY_PIN_PROTECTS_RESOURCE`, and an unavailable pin read returns
`RECOVERY_PIN_STATE_UNAVAILABLE`. `DelayShard` can compact the local
`gc_cf/TASK` tombstone only after that proof. This remains a local proof: it
does not perform the Oxia CAS, provider ownership/attestation, or external
delete.

## Current repository shape

The repository is currently a single Gradle Java 21 library while the design's
multi-module target is being implemented incrementally. Package boundaries map
to the intended modules:

| Package | Current responsibility | Design target |
|---|---|---|
| `io.nereusstream.delay.protocol` | IDs, source positions, canonical hash, NDL1 frame, command envelope and body codec | `delay-api` / `delay-client-core` |
| `io.nereusstream.delay.store` | One RocksDB instance per shard, seven application CFs, value envelope, shared process resources and checkpoints | `delay-store-rocksdb` |
| `io.nereusstream.delay.runtime` | Deterministic Shard Log application, message state machine and Lane gate projection | `delay-core` |
| `io.nereusstream.delay.scheduler` | Lane-local failure isolation and bounded weighted DRR | `delay-core` |
| `io.nereusstream.delay.ownership` | Owner Lease CAS boundary and local ownerEpoch fencing | `delay-server` / `delay-metadata-oxia` |
| `io.nereusstream.delay.client` | Strict preparation, zero-I/O AUTO_FAST branch selection, immutable managed/native submission bridge, ordered enqueue outcomes, bounded command/message queries, receipt-bound large-payload operations and embedded conformance service | `delay-api` / `delay-client-core` / `delay-testkit` |
| `io.nereusstream.delay.adapter` | Broker/destination interfaces and test adapters | ingress/adapter modules |
| local monolith packages exist | shared zero-I/O preparation and verified Route cache boundary (`semantic`, `route`) | `delay-semantic-core` / `delay-route-spi` / `delay-route-oxia` extraction and Oxia authority remain pending |
| local monolith packages exist | transport registry, ownership permits, guarded Kafka/Pulsar bridges and strict production configuration seams | `delay-transport-spi` / `delay-client-kafka` / `delay-client-pulsar` extraction and real client artifacts remain pending |
| local source proto + in-memory Schedule conformance exist | optional multi-language auth/idempotency/quota/audit entry (`gateway`) | `delay-gateway-api` / `delay-gateway` generated modules, auth, quota, audit and HA durability remain pending |

## Evidence matrix

Latest local delta for the target-publish boundary: physical admission now
checks each candidate against the full currently-active potential-zombie
request/byte envelope before invoking the delegate; requests that cannot all
become zombies within the Lane budget are rejected as `ZOMBIE_CAPACITY`.
Focused coverage is
`DestinationPhysicalAdmissionTest.admissionReservesZombieRequestCapacityForAllOutstandingRequests`
and `DestinationPhysicalAdmissionTest.admissionReservesZombieByteCapacityForAllOutstandingRequests`.
Asynchronous delegate and callback-registration `Error` paths complete
`UNKNOWN`, retain the physical charge, and rethrow after the logical result is
available; focused coverage is in
`BoundedDestinationPublishAdapterTest.asynchronousDelegateErrorCompletesUnknownBeforeFatalFailureEscapes`
and `BoundedDestinationPublishAdapterTest.asynchronousCallbackRegistrationErrorCompletesUnknownBeforeFatalFailureEscapes`.
Observed delegate completion releases the physical reservation before the
logical outcome is completed, so a caller that sees a completed result also
sees drained local admission accounting; the healthy-lane regression
`BoundedDestinationPublishAdapterTest.blockingDelegateCallDoesNotBlockHealthyLane`
covers this ordering.

| Area | Status | Evidence |
|---|---|---|
| Shared Semantic Core and signed immutable RouteSnapshot | Partial (local deterministic core plus Oxia event/head-CAS authority composition; production gates open) | `RouteSnapshotV1`, `DefaultDelaySemanticCore`, `InMemorySignedRouteSnapshotProvider`, `OxiaSignedRouteSnapshotProvider`, `OxiaSignedRouteSnapshotPublisher`, `RouteSnapshotCompatibilityV1`, `DefaultDelayClient`, `RouteBoundSubmissionTransportPlanResolver`, `RouteSnapshotV1Test`, `DefaultDelaySemanticCoreTest`, `InMemorySignedRouteSnapshotProviderTest`, `OxiaSignedRouteSnapshotProviderTest`; canonical signature/digest, contiguous replay, head CAS, notification refresh, same-incarnation immutable-drift quarantine, tenant-scoped historical resolution and zero-I/O preparation are covered. Activation-barrier publication, session fencing, real Oxia service evidence, native eligibility authority, package split and production cross-entry gate remain open |
| Delay Gateway and Gateway idempotency | Partial (local Schedule/RetryUncertain plus Oxia single-record CAS composition) | `GatewayScheduleRequestV1`, `GatewayRetryUncertainRequestV1`, `GatewayIdempotencyStore`, `GatewayIdempotencyHashV1`, `GatewayIdempotencyRecordV1`, `GatewayPhysicalAttemptV1`, `InMemoryGatewayIdempotencyStore`, `OxiaGatewayIdempotencyStore`, `GatewayScheduleService`, source proto, `GatewayScheduleServiceTest` and `OxiaGatewayIdempotencyStoreTest`; exact body conflict, prepared-before-ownership, one-shot attempt, strict record decoding, uncertain expected-prior/retry-ID CAS, response-loss no-permit behavior and outcome replay are local evidence. Generated gRPC/API modules, mTLS/JWT authentication, quota/audit, HA/transactional durability, late authenticated evidence promotion, crash cuts and multi-language vectors remain open |
| Kafka generic guarded Producer patch | Implemented in isolated upstream worktree (real-service gate open) | Kafka branch `nereus/delay-guarded-producer-v1@d1810fa3466e1378a33c5c6327c7f401cec03d07` from locked `trunk@c300006a7705c240642db6950b5a95fec982bfc5`; focused client/mock and regression evidence pass. Delete/recreate, leader-failover, artifact/source digest and Delay D2 transport remain open; K2 target-plus-receipt transaction is separate |
| Pulsar v22 first-class resource guard | Implemented in isolated upstream worktree (real-service gate open) | Pulsar branch `nereus/delay-resource-guard-v1@be226fe6c88634e9a94ba5c6a0f5859bc510cb66` from locked `5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551`; focused common/broker and checkstyle evidence pass. Delete/recreate, unload/failover, proxy compatibility, artifact/source digest and Delay D3 transport remain open |
| Queued receipt Route-policy boundary | Implemented (local strict adapter seam; Route authority pending) | `QueuedReceiptQueryPolicy`, `PolicyBoundWireCommandIngressAdapter`, `PinnedKafkaCommandIngress`, `PinnedPulsarCommandIngress`, `PreparedSubmissionAdapter`, `EmbeddedDelayService`, `AdapterIngressTest`, `NativeSubmissionAdapterTest`; strict paths derive `receipt_query_until` from authenticated Broker persistence time with checked addition, reject missing/drifting policy snapshots before transport ownership, and retain post-persistence overflow as `ENQUEUE_UNCERTAIN`/integrity evidence; absolute-boundary overloads are compatibility-only and checked against a bound policy; Route policy publication, source-time authority and concrete production transports remain release blockers |
| Full command-result retention boundary | Implemented (local strict query seam; retention authority pending) | `CommandResultRetentionPolicy`, `DelayClient`, `EmbeddedDelayService`, `BoundedLocalQueryProjector`, `EmbeddedDelayServiceTest.embeddedQueryDerivesFullResultRetentionFromAppliedSourceTime`, `CommandResultRetentionPolicyTest`; strict query/await/applied-receipt projections derive `full_result_retain_until` from the applied Source Position Broker persistence time with checked addition, while absolute-boundary overloads remain compatibility-only; policy publication, source-time authority and production query routing remain release blockers |
| Strict typed Claim runtime binding | Implemented (local Message plus durable V1 command/Lane-tuple binding and public-API fence; live authority pending) | `DelayShard.claimForPublishV1`, `V1ScheduleBinding.requireClaimLaneProjection`, `CanonicalLaneTupleV1Test`, `DelayShardTest.physicalGcMutationPrimitivesAreNotPublicProductionApis`, `DelayShardTest.registryPrepareCannotDowngradeTrustSetAuthorityWithLegacyCommitBody`, `ClaimMaterializationRuntimeTest`; strict Claim entrypoint binds message identity, generation, delivery window, timeline `actionAt` and inline/object payload reference before persistence, then, when a `V1ScheduleBinding` exists, exactly rebinds Destination Profile, business metadata, delivery window and the original Schedule payload branch or Prepare Object Store Profile/length/SHA-256. It also parses the exact durable canonical Lane tuple and requires byte-identical Destination/Capability Profiles, Kafka/Pulsar Broker target resource and physical partition; same-hash foreign Profile identities, target or partition drift are rejected before Claim state changes. The legacy byte-array primitive is package-local and reachable across packages only from the test-classpath bridge. Production Claim creation remains routed through `ClaimHandoffWorkClassExecutor`/`OwnedDelayShard`; live Profile/credential/resource authority, Object Store fetch, Adapter serialization/size certification, channel lease, Producer ownership and crash recovery remain release blockers |
| Gradle Java 21 build | Implemented | `gradle compileJava`, `gradle test` |
| Self-routing IDs and CRC32C | Implemented | `ProtocolCodecTest` |
| `commandId + commandHash` prepared before I/O | Implemented | `PreparedCommand`, `CommandHash`, `ProtocolCodecTest` |
| NDL1 frame and canonical Client Command envelope | Implemented | `ShardLogFrame`, `CommandCodec`, registry frame vector test |
| Registry-shaped `ScheduleIntentV1` | Implemented (canonical codec plus resolver/catalog-backed local apply; external authority pending) | `ScheduleIntentV1`, `ScheduleCommandBodyV1`, `CommandBodies.scheduleV1/decodeScheduleV1`, `PreparedCommand.scheduleV1`, `CommandCodec.encode/decode*V1`, `V1ScheduleResolver`, `RetryPolicyCatalog`, `V1CommandResolutionException`, `V1ScheduleBinding`, `KeyCodec.idV1ScheduleBinding`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `PayloadReference.fromDescriptor`, `DelayShard`, `ScheduleIntentV1Test`, `ClientCommandBodyV1Test`, `PreparedCommandV1Test`, `V1ScheduleBindingTest`, `ProfileBindingControlStateTest`, `PayloadReferenceTest`, `DelayShardTest`; exact field order/oneof/presence, common fields 1–3, outer/body message identity/type/retry binding, tuple-derived Lane, resolver-required fail-closed route snapshot, source-position Retry Policy semantic ref/hash lookup and ordering guard, inline/object payload projection including absent optional etag, source-ordered Profile first-binding activation/close gating, canonical body/tuple binding persistence and reopen checks are covered; immutable Profile/Retry publication/catalog authority, signed control target authority, full historical binding and production adapter authority remain pending |
| Registry-shaped `PrepareLargeScheduleV1` | Implemented (canonical codec plus resolver/catalog-backed reservation apply; external authority pending) | `PrepareLargeScheduleBodyV1`, `CommandBodies.prepareLargeV1/decodePrepareLargeV1`, `V1ScheduleResolver`, `RetryPolicyCatalog`, `V1ScheduleBinding`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `DelayShard`, `InMemoryPayloadObjectStore`, `EmbeddedDelayService`, `ClientCommandBodyV1Test`, `V1ScheduleBindingTest`, `ProfileBindingControlStateTest`, `InMemoryPayloadObjectStoreTest.registryRegistrationRequiresTheExactPinnedTrustSetSemantic`, `EmbeddedDelayServiceTest.payloadFacadeRejectsAdapterSemanticDriftFromDurableV1PrepareBinding`, `DelayShardTest`; common fields 1–3, intent-without-payload, expected length/SHA-256, reservation TTL, exact trust-set ref and required field 15 exact `OBJECT_STORE ProfileRefV1`, resolver-derived Lane, exact Object Store semantic/current credential Head, source-position Retry Policy semantic lookup/ordering guard, source-ordered Profile first-binding gate and atomic binding sidecar persistence are covered; the durable binding selects both Commit verification and handle/attestation adapter Profile/trust semantics regardless of a later body or adapter configuration; immutable Profile/Retry/trust-set publication/catalog authority and production Object Store authority remain pending |
| Registry-shaped `CommitLargeScheduleV1` | Implemented (canonical body plus typed proof/outer-identity apply; external authority pending) | `PayloadCommitProofView`, `PayloadCommitProofV1`, `CommitLargeScheduleBodyV1`, `CommandBodies.commitLargeV1/decodeCommitLargeV1`, `PreparedCommand.commitLargeV1`, `CommandCodec.encode/decode*V1`, `PayloadReference`, `PayloadProofTrustSet`, `DelayShard`, `InMemoryPayloadObjectStore`, `CommitLargeScheduleBodyV1Test`, `PayloadReferenceTest`, `PayloadProofTrustSetSemanticV1Test`, `InMemoryPayloadObjectStoreTest`, `DelayShardTest.registryPrepareCannotDowngradeTrustSetAuthorityWithLegacyCommitBody`; common fields 1–3, reservation identity, typed Object Store Profile/tenant scope, optional etag presence, proof-id/signature digests, strict canonical field order, outer/body identity fencing, local exact-reservation handle issuance/upload/attestation, bounded handle expiry/re-sign and trust-set verification, durable Prepare receipt-anchor reconstruction across reopen, V1-Prepare/legacy-Commit trust downgrade rejection, typed full-Profile/legacy semantic-hash matching, committed ReservationId/ProofId retention, pre-`ALREADY_COMMITTED` Profile fencing, exact historical-proof/signature verification after issuer close, and malformed-signature stable rejection are covered; source-position trust-set publication authority and real Object Store credential/provider ownership remain pending |
| Registry-shaped `CancelV1` / `RescheduleV1` | Implemented (canonical body/outer identity plus local runtime application) | `MessagePreconditionV1`, `CancelCommandBodyV1`, `RescheduleCommandBodyV1`, `CommandBodies.cancelV1/decodeCancelV1`, `CommandBodies.rescheduleV1/decodeRescheduleV1`, `PreparedCommand.cancelV1/rescheduleV1`, `CommandCodec.encode/decode*V1`, `DelayShard`, `RemainingClientCommandBodyV1Test`, `RemainingPreparedCommandV1Test`, `DelayShardTest`; independently present generation/state-version preconditions are checked against current messages/reservations and persisted through the same atomic cancellation/reschedule transition, with common fields 1–3, outer/body identity/type/retry binding, canonical timing and strict field order covered |
| Registry-shaped `RetryPolicySemanticV1` | Implemented (canonical value/hash codec, source-position catalog gate and catalog-backed business/DLQ retry revalidation) | `RetryPolicySemanticV1`, `RetryPolicyRefV1`, `RetryJitterV1`, `PublishOutcomeBody`, `DlqExportResultBody`, `PublishAttemptLedger`, `RetryPolicyCatalog`, `InMemoryRetryPolicyCatalog`, `UncertainPolicyV1`, `DlqExportModeV1`, `RetryPolicySemanticV1Test`, `PolicyCatalogTest`, `PublishOutcomeBodyTest`, `DlqExportResultBodyTest`, `PublishAttemptLedgerTest`, `DlqExportApplyTest.catalogBackedDlqOutcomeRecomputesPinnedPolicyBeforePersisting`, `DelayShardTest`; fields 1/4–18, domain-separated semantic hash, typed ref projection, checked exponential cap and Registry jitter scaling, uncertain/DLQ presence rules, FIFO possible-duplicate guard, exact catalog ref/hash matching, source publication visibility fences, V2 Admission-ledger retry-window persistence, canonical V2 ledger/body/Lane identity revalidation at the embedded admission boundary, catalog-less typed-window validation, canonical Admission first-attempt extraction, source-ordered business Outcome/Evidence retry ref/deadline/jitter revalidation, and catalog-backed DLQ policy ref/terminalization-time/deadline/attempt-budget/duplicate/jitter revalidation are covered; legacy opaque-ledger upgrade, authenticated activation authority and durable historical retention remain pending |
| Registry §6.3 Profile control requests | Implemented (canonical request values; authority pending) | `PublishDestinationProfileRequestV1`, `DeprecateDestinationProfileRequestV1`, `RotateEquivalentSecretRequestV1`, `ProfileControlRequestV1Test`; exact Profile envelope/binding identity, generation-1 publication, typed deprecation reason, checked equivalent-generation successor, private-reference SHA-256, attestation candidate tuple, expected binding digest/head revision and derived new binding are covered; authenticated actor/target authorization, source-ordered mutation routing, immutable catalog publication and Oxia CAS remain release blockers |
| Local immutable Profile/binding catalog seam | Implemented (exact local projection and resolver/catalog identity graph; authority pending) | `ProfileCatalog`, `InMemoryProfileCatalog`, `ProfileCatalogV1ScheduleResolver`, `ProfileCatalogTest`, `ProfileCatalogV1ScheduleResolverTest`, `DelayShardTest.catalogBackedRegistryScheduleRejectsBeforeFirstProfileActivationMarker`, `DelayShardTest.catalogBackedActionAtDerivationFailsClosedWhenPinnedProfileDisappears`, `DelayShardTest.decoratedScheduleResolverRequiresTheExactShardProfileCatalog`; exact Profile semantic reference lookup, generation-1 binding/head/protection creation, checked equivalent rotation with response-loss idempotency, immutable generation retention, deprecation intent, semantic-reference collision rejection, fail-closed Schedule/Prepare resolver gating, the initial-marker gate for catalog-backed V1 paths and the persisted-binding actionAt lookup fence are covered; a pre-decorated resolver must be paired with its exact same shard catalog and nested/foreign/missing catalog composition fails before Store reads, so Schedule timing and later Admission/recovery lookups cannot use two independently mutable semantic sources; authenticated Profile publication, source-ordered activation routing/target authority, retained-generation quota and Oxia CAS remain external |
| Local Control Operation state projection guard | Implemented (crash-durable local authority; per-operation Oxia CAS backend; source/session authority pending) | `ControlOperationStateTransitionV1`, `InMemoryControlOperationAuthority`, `PersistentControlOperationAuthority`, `OxiaSyncControlOperationBackend`, `ControlOperationAuthorityTest`, `PersistentControlOperationAuthorityTest`, `OxiaSyncControlOperationBackendTest`; closed operation and target-marker transition graph, immutable target-index set and target-revision monotonicity are enforced before local/CAS writes, the revision successor check rejects `Long.MAX_VALUE` before wraparound, the persistent authority stores the exact receipt/current pair with checksum, atomic replacement, directory fsync and cross-instance local locking, and the Oxia backend stores the same pair in one checksummed record with version CAS and exact response-loss reread; source-ordered mutation application, production routing/authorization, session ownership and authenticated Oxia service evidence remain release blockers |
| Control Operation initial projection and uint32 target index | Implemented (local protocol projection) | `PreparedControlOperationV1.initialCurrentOperation`, `ControlTargetStateViewV1`, `ControlOperationStateTransitionV1Test`; revision-one PENDING target states cover every Prepared target and preserve the full 0..0xffffffff target-index range in canonical bytes |
| Control registration receipt/current projection | Implemented (local value pairing; Oxia CAS pending) | `ControlRegistrationProjectionV1`, `ControlRegistrationProjectionV1Test`; receipt identity/scope/request target snapshot, revision-one binding and all-target PENDING projection are constructed and checked together; production transaction/response classification remains external |
| Control receipt retention boundary | Implemented (strict local policy binding; policy authority pending) | `ControlOperationQueryPolicy`, `EmbeddedDelayService.registerPreparedControlOperation`, `ControlOperationReceiptV1.createWithQueryWindow`, `ControlRegistrationProjectionV1.initialWithQueryWindow`, `ControlOperationQueryPolicyTest`, `EmbeddedDelayServiceTest.strictPreparedControlRegistrationRejectsPolicyDriftAndOverflowBeforeRegistration`, `ControlRegistrationProjectionV1Test`; strict registration binds the policy version from `PreparedControlOperationV1`, derives trusted `registered_at.latest + controlOperationQueryWindow` with checked arithmetic before target registration, and rejects drift/overflow; the explicit deadline/window constructors remain compatibility-only and immutable policy distribution remains external |
| Embedded Prepared Control registration path | Implemented (local conformance only) | `EmbeddedDelayService.registerPreparedControlOperation`, `ControlOperationQueryPolicy`, `EmbeddedDelayServiceTest`; strict policy/version binding, target registration, receipt/current pairing, registration binding and operation-authority CAS are exercised together; production authenticated gateway/Oxia transaction remains a blocker |
| Registry §6.3 Control Operation request union | Implemented (canonical request branches; authority pending) | `ControlOperationKindV1`, `ControlOperationRequestV1`, `ControlOperationRequestBranchV1`, `StopNewSchedulesRequestV1`, `LaneGateRequestV1`, `CloseLaneRequestV1`, `BreakOrderingRequestV1`, `DrainShardRequestV1`, `FenceShardRequestV1`, `ForceCheckpointRequestV1`, `GetCheckpointCatalogRequestV1`, `ReplayDeadLetterRequestV1`, `ResolveUncertainRequestV1`, `PublishQuotaGrantRequestV1`, existing Profile request branches, `ControlOperationRequestV1Test`, `ControlRequestSupportCodecTest`; all fifteen outer tags, exact branch field order, empty catalog branch, acknowledgement/evidence/boolean matrices, fixed hash/scope fields, retry timing and quota-plan identity/version/hash canonical round-trips/rejection vectors are covered; authenticated actor/resource authority, source-ordered registration, operation state and Oxia CAS remain release blockers |
| Registry §6.3 Control target value layer | Implemented (canonical target branches; preparation matrix, local mutation binding and local immutable registration seam enforced) | `ControlTargetKindV1`, `ControlTargetRefV1`, `LaneControlTargetV1`, `ControlMessageTargetV1`, `ProfileControlTargetV1`, `ControlTargetRefV1Test`, `PreparedControlOperationV1`, `ControlTargetMutationBindingV1`, `ControlTargetRegistrationAuthority`, `InMemoryControlTargetRegistrationAuthority`; all six target branches, Profile rotation all-or-none precondition tuple, optional expected System Mutation ID/hash pair, branch-kind matching, target digest, operation-specific target counts/kinds, prepared-target membership, ControlRef/logical identity, mutation ID/hash, target Shard/Message, Replay/Resolve body and Lane marker matching, canonical tamper rejection, and exact-byte idempotent Prepared registration are covered; source-mutation construction, actor/resource authorization, authenticated target existence and Oxia CAS remain release blockers |
| Oxia Control target registration validation adapter | Implemented (per-operation canonical Oxia record plus exact reread validation; authorization/transport pending) | `OxiaControlTargetRegistrationAuthority`, `OxiaSyncControlTargetRegistrationBackend`, `OxiaControlTargetRegistrationAuthorityTest`, `OxiaSyncControlTargetRegistrationBackendTest`; backend registration outcome, exact Prepared reread, operation-ID lookup identity and mutation binding are checked, and the concrete backend uses one `IfRecordDoesNotExist` CAS record with corruption/response-loss fencing; authenticated actor/target authorization, source-ordered mutation transaction, target existence and real transport remain release blockers |
| DelayShard Control marker registration gate | Implemented (configured local authority; Oxia transaction pending) | `DelayShard` eight-argument constructor, `ControlTargetRegistrationAuthority`, `DelayShardTest.configuredControlRegistrationRejectsUnregisteredMarkerBeforeHandler`, `DelayShardTest.configuredControlRegistrationAppliesExactRegisteredMarker`; configured shards extract the body `ControlRef`, require the exact registered Prepared target and validate mutation identity before applying the three source-ordered Control marker types; missing, malformed or drifting registration is persisted as `UNAUTHORIZED_SYSTEM_MUTATION` with no handler effect, while an exact registered target reaches the normal handler; production Oxia registration/lookup and authenticated writer authority remain release blockers |
| Control System Mutation construction seam | Implemented (signed envelope and Prepared-target binding; body/authentication pending) | `ControlSystemMutationFactoryV1`, `ControlSystemMutationFactoryV1Test`; operation-specific mutation type, `ControlRef` logical identity, signed System Mutation envelope and expected ID/hash binding are checked before return; body encoders, signing-key trust/ACL and source Broker registration remain release blockers |
| Registry §6.3 Prepared Control Operation envelope | Implemented (canonical pre-I/O envelope, target matrix, mutation binding and local RBAC gate; registration authority pending) | `ControlAuthorV1`, `ControlRoleV1`, `ControlRoleSetV1`, `ControlAuthorizationContextV1`, `ControlOperationAuthorizationV1`, `PreparedControlOperationV1`, `PreparedControlOperationV1Test`, `ControlOperationAuthorizationV1Test`, `ControlTargetMutationBindingV1`; fixed operation ID/version, request-kind binding, request hash, operation-specific target counts/kinds and request-to-target Profile/Quota identity, strictly sorted repeated targets, target-snapshot hash, query-policy/retry fields, prepared digest, Ed25519 signing/verification, completed source-mutation ControlRef/identity/body binding, actor/role/scope hash equality and minimum role matrix are covered; authenticator implementation, target existence/tenant authorization, source-mutation construction, Oxia registration outcome, non-persistence proof and durable control-operation state remain release blockers |
| Registry §6.3 Control registration outcome union | Implemented (canonical outcome/proof values, prepared-operation binding and local exact-Prepared target registration; Oxia transport pending) | `ControlRegistrationOutcomeV1`, `ControlNonPersistenceProofKindV1`, `ControlNonPersistenceProofV1`, `ControlDefinitelyNotRecordedV1`, `ControlRecordUncertainV1`, `ControlRegistrationOutcomeMessageV1`, `ControlRegistrationBindingV1`, `ControlTargetRegistrationAuthority`, `InMemoryControlTargetRegistrationAuthority`, `ControlRegistrationOutcomeCodecTest`, `ControlRegistrationBindingV1Test`, `ControlTargetRegistrationAuthorityTest`; proof branch evidence matrix, operation/prepared digest binding, exact receipt request/scope/target identity and initial revision, CONTROL-stage error fencing, recorded/definitive/uncertain outer tags, canonical round-trip and timeout-proof rejection vectors, and idempotent byte-identical Prepared registration are covered; authenticated Oxia transaction/response classifier, real registration transport, retry/query state and durable operation authority remain release blockers |
| Registry-shaped `PayloadProofTrustSetSemanticV1` | Implemented (canonical verifier-key/hash codec, exact local catalog and source-ordered marker projection; authority pending) | `PayloadProofVerifierKeyV1`, `PayloadProofTrustSetSemanticV1`, `PayloadProofTrustSetRefV1`, `PayloadProofTrustSet.fromSemantic`, `PayloadProofTrustSetControlState`, `InMemoryPayloadProofTrustSetCatalog`, `PayloadProofTrustSetSemanticV1Test`, `PayloadProofTrustSetControlStateTest`, `PolicyCatalogTest`; sorted/unique Ed25519 raw keys, validity bounds, semantic hash/ref, exact local reference resolution, canonical round-trip/tamper rejection, source-time verification windows, strictly ordered activation markers, idempotent marker replay, first-seen issuance close versus historical verification, canonical marker-state encoding, and `DelayShard` atomic marker/result/source-position persistence with reopen are covered; authenticated source-ordered control authority and Recovery-Floor historical retention remain pending |
| NDR1 receipt frame | Implemented (queued/applied/reservation/control/native receipt/prepared payload subset) | `ReceiptFrame`, `ReceiptKind`, `CommandQueuedReceiptV1`, `CommandAppliedReceiptV1`, `PayloadReservationReceiptV1`, `PayloadProofTrustSetRefV1`, `ControlOperationReceiptV1`, `PulsarBrokerResourceIdentityV1`, `NativeCapabilitySnapshotV1`, `PulsarMetadataV1`, `NativePreparedDeliveryV1`, `NativePreparedRefV1`, `NativeDeliveryReceiptV1`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1`, `WireCommandIngressAdapter`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter`, `NativeSubmissionAdapterTest`; registry zero-payload vector, canonical PreparedCommandRef/ProtocolTuple, Kafka/Pulsar Source Position and SafeBrokerAck agreement, queued-to-applied digest/source fencing, barrier-gated applied-frame emission, apply-status/message-field consistency and generation/state-version/binding presence fencing, object-store profile/object identity/trust-set pinning, control operation/scope/target/evidence/query-boundary pinning, Pulsar-only native target/ACK identity matching, signed capability snapshot canonical digest/Trusted-UTC binding/Ed25519 verification, strict optional Pulsar metadata and key-sorted unique property encoding, native snapshot projection/expiry/attestation matching, unsigned high-bit native physical-partition and Pulsar physical-topic-creation-timestamp projection, submission-hash and prepared-ref byte projection, capability bits and physical-attempt/digest checks, query boundary/capability/physical-attempt/digest checks, Kafka queued ACK and definitive rejection proof projection, Pulsar batch-aware queued ACK and guard rejection proof projection, native persisted/guard-rejection/uncertain/local-fence projection, and flags/length/kind/CRC/Base64url rejection tests; durable guard/credential protection and real Broker response transports remain pending |
| Query response closed unions | Implemented (wire codec plus bounded local bridge) | `ProfileRefV1`, `PublicDestinationBindingViewV1`, `PublicEvidenceRefV1`, `CheckpointSummaryV1`, `CheckpointCatalogResultV1`, `CheckpointControlResultV1`, `LaneControlResultV1`, `ShardControlResultV1`, `ProfileControlResultV1`, `QuotaControlResultV1`, `MessageControlResultV1`, `RouteControlResultV1`, `SecretRotationResultV1`, all Registry Message/Command view classes, `CommandQueryResponseV1`, `MessageQueryResponseV1`, `ControlOperationQueryResponseV1`, `CurrentControlOperationV1`, `ControlTypedResultV1`, `PublicQueryErrorV1`, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queryCommand/queryMessage`, `DelayShard.matchesCommandHash`, `ProtocolCodecTest`, `CheckpointCatalogResultV1Test`, `CheckpointControlResultV1Test`, `ControlResultCodecTest`, `ControlOperationQueryResponseV1Test`, `EmbeddedDelayServiceTest`; exact branch tags/field order, Source Position barrier ordering, durable `dedupe_cf` command-hash binding (`RECEIPT_MISMATCH` on same-ID hash drift), state/status agreement, command-view optional presence fencing, safe NFC alias and payload/DLQ/evidence enum checks, canonical checkpoint-catalog shard/Floor/sorted-summary validation, checkpoint-control identity validation, all nine control-result branch field/presence/identity codecs with strict branch-to-payload dispatch and round-trip/rejection vectors, fixed-source queued-receipt barrier and retention projection, and canonical Control Operation CURRENT/error/target/revision/typed-result projection; production receipt routing, authorization-safe lookup, source-derived retention, Oxia ownership, durable control-operation query state and observability remain pending |
| System Mutation envelope, type registry, canonical hash/ID and Ed25519 signature | Implemented (bounded control plus admission/expiry/outcome/evidence/claim-result/resource-retire/delete-confirmed subset) | `SystemMutationType`, `SystemMutation`, `ShardSubjectV1`, `SystemMutationBodyCodec`, `ApplyShardControlBody`, `ReplayDeadLetterBody`, `ResolveUncertainBody`, `ControlRef`, `ControlReasonKindV1`, `ControlReasonV1`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `PayloadProofTrustSetActivatePayloadV1`, `PayloadProofIssuanceClosePayloadV1`, `PayloadProofTrustSetControlState`, `PayloadProofTrustSetControlCatalog`, `PublishAdmissionBody`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `PublishOutcomeBody`, `ClaimResultBody`, `ResourceRetireIntentBody`, `ResourceRetireIntentRecord`, `ResourceDeleteConfirmedBody`, `ResourceDeleteConfirmedRecord`, `TrustedUtcIntervalEvidence`, `SystemMutationResult`, `AuthorIdentity`, `ClaimRecord`, `GenerationRuntimeIndex`, `DelayShard`, `ProtocolCodecTest`, `ShardSubjectV1Test`, `ReplayDeadLetterBodyTest`, `ResolveUncertainBodyTest`, `PublishAdmissionBodyTest`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `ResourceRetireIntentBodyTest`, `ResourceDeleteConfirmedBodyTest`, `GenerationRuntimeIndexTest`, `PayloadProofControlPayloadV1Test`, `ProfileBindingControlStateTest`, `PayloadProofTrustSetControlStateTest`, `DelayShardTest`; canonical owner/control/fence/service branches, strict canonical ShardSubject route/partition decoding shared by envelope and body checks, body fields 1–3, required/optional operation fields, wire widths, bool bounds, canonical nested bytes, canonical typed `RetryPolicyRefV1` Replay field, canonical Replay/Resolve encoding and shared message-bearing body self-routing checks for Admission/Claim/DLQ/Expire, durable `dedupe_cf/SYSTEM_MUTATION` with key/value mutation identity and Source Position shard fencing, explicit signature verification, source-ordered Profile activation/close and trust-set activation/issuance-close with semantic-reference checks and atomic marker-state persistence/reopen, source-ordered Lane PAUSE/RESUME/BREAK/CLOSE with ControlRef/identity/incarnation/CAS, close-policy/acknowledgement checks and atomic Claim/READY rollback, source-ordered TIME_FENCE watermark and reservation-expiry overlay/materialization, source-ordered `EXPIRE_GENERATION_V1`, `PUBLISH_ADMISSION_V1` descriptor/Ready Certificate/Claim identity projection plus adapter/encoding/target/partition/channel-Profile cross-object fences, replay-stable timeline-key/semantic-digest/counter/obligation-set preconditions, checked attempt-number and uncertain-retry counter projection, definitive `PUBLISH_OUTCOME_V1/NOT_PUBLISHED` disposition/retry-shape subset, verified-published `ATTACH_PUBLISHED_EVIDENCE`, `ATTACH_NOT_PUBLISHED_EVIDENCE` all-absent normalization, and verified `EVIDENCE_RESOLUTION_V1` transition subsets; successful canonical fixed-lead Pulsar handoff evidence now projects `HANDED_OFF` while ordinary/legacy outcomes remain `PUBLISHED` (`MessageStatus`, `GenerationAggregateState`, `MessageRecord`, `DelayShard`, `MessageRecordTest`, `DelayShardTest`), with malformed/unsupported early evidence rejected as stale; replay-stable permanent `CLAIM_RESULT_V1` ClaimPrecondition/timeline/claimed-charge terminalization subset with canonical transfer equality and source-apply rejection (`DelayShardTest.sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline`), uniform handler arithmetic-overflow fencing (`DelayShardTest.systemMutationStateVersionOverflowPersistsStaleResult`), closed ExactResourceIdentity/ProtectionSet parsing with branch-specific Object Store Profile kind and zero-length payload/manifest support, plus registered logical-identity verification and atomic `gc_cf/TASK` retire-intent persistence, exact RetireIntentRef/DeleteOutcome/ExternalDeleteEvidence matching and source-ordered local tombstone persistence, local durable `SCHEDULED -> CLAIMED -> revoke/Admission/ClaimResult/Cancel/Reschedule/expiry` transitions, and v4 `id_cf/MESSAGE` runtime-index writes are covered; immutable Profile/catalog and authenticated source control authority, source-protected signing-key trust/ACL, immutable Oxia target registration, Recovery Floor barriers, full ActiveLaneState persistence, obligation-set quota/recovery accounting and full Claim materialization/recovery model remain pending |
| Kafka/Pulsar source order token and source identity fencing | Implemented (u64/u32 local protocol paths; external authority pending) | `SourcePosition.sourceOrderToken`, `SourcePositionCodec` byte-round-trip canonical decode with explicit truncated length/fixed-field rejection, Kafka offset/Pulsar ledger-entry-batch order, exact canonical-position fencing for same physical token, physical-resource comparison guard, unsigned high-bit comparison/successor, canonical protobuf receipt/evidence/barrier paths, queued-receipt PreparedCommandRef-to-Source-Position shard binding in the shared constructor/decode path (`ProtocolCodecTest.commandQueuedReceiptRejectsACommandAndSourceFromDifferentShards`), unsigned checkpoint-manifest source/evidence fields, direct Source Position construction rejects malformed/non-NFC text before identity bytes are produced (`ProtocolCodecTest.sourcePositionsRejectNonCanonicalTextAtConstruction`, `ProtocolCodecTest.sourcePositionDecoderRejectsTruncatedLengthAndFixedFields`, `ProtocolCodecTest.sourcePositionsRoundTripUnsignedHighBitOffsetsThroughReceiptAndEvidenceCodecs`, `ProtocolCodecTest.sourcePositionsPreserveUnsignedPartitionLeaderAndBatchFields`, `ProtocolCodecTest.trustedUtcEvidencePreservesUnsignedCounterBitPatterns`, `ActivationBarrierV1Test.preservesUnsignedPartitionAndBatchFields`, `EvidenceCursorV1Test.preservesUnsignedPartitionAndBatchFields`, `SourceReplaySuccessorTest.strictKafkaAcceptsTheUnsignedHighBitBoundarySuccessor`, `CheckpointManifestTest.manifestRoundTripsUnsignedSourceAndEvidencePositions`), including Registry Pulsar `physicalTopicCreationTimestamp:u64be` through broker identity/queued ACK, evidence, adapter and manifest projections; `KafkaReceiptJournal` receipt positions/matches and contiguous-cursor successor/order use the same unsigned offset domain, covered by `KafkaReceiptJournalTest.receiptJournalPreservesUnsignedHighBitOffsetsAndOrdering`; `DelayShardTest`; remaining auxiliary uint64/time fields, real Broker assignment/barrier adapters and production authority remain release blockers |
| Pinned Kafka/Pulsar command ingress outcome mapping | Implemented (transport SPI plus Kafka/Pulsar wire projection) | `PinnedKafkaCommandIngress`, `PinnedPulsarCommandIngress`, `WireCommandIngressAdapter`, `WireIngressOutcomeSupport`, `KafkaIngressResource`, `PulsarIngressResource`, `KafkaProduceRequest`, `PulsarSendRequest`, `KafkaProduceResult`, `PulsarSendResult`, `AdapterIngressTest`, `AdapterRequestIdentityTest`; Kafka topic UUID and Pulsar resource token plus physical topic creation identity are carried at the request boundary, ingress resources and direct request/result transport identities enforce canonical UTF-8/NFC text, invalid 16-byte physical attempts are rejected locally before Producer ownership, result dispositions form a closed matrix (`PERSISTED` + `OK` versus non-persisted stable code with no success position), persisted Pulsar results fence all pinned identity fields, managed Kafka/Pulsar transport failures, null results and malformed receipt projections use `ENQUEUE_RESULT_UNCERTAIN` without leaking an exceptional Future, managed result projection normalizes native-only guard/uncertain codes while only the native adapter uses `NATIVE_ENQUEUE_RESULT_UNCERTAIN`, and both managed adapters can project queued/definitive-proof/uncertain NDR1 outcomes with evidence fail-closed; concrete pinned request transports, authenticated production rejection classifiers and source assignment pending |
| Pulsar Attempt Journal mapping-before-send seam | Implemented (local deterministic ordering/evidence seam plus optional durable ledger projection) | `PulsarAttemptJournal`, `PulsarAttemptJournalTest`, `PublishAttemptLedger`, `PublishAttemptLedgerTest`, `DelayShard`, `DelayShardTest.attemptJournalProjectionIsDurableWithoutAdvancingShardSourcePosition`; one-Shard Producer key, strictly increasing sequence allocation, durable-appender position gate before target sender invocation, `appendOrReuse`/identity-bound `sendAfterMapped` exact-attempt retransmission reuse, mapping identity-drift fencing, exact mapping idempotency, unresolved lower-sequence blocking, durable `RETIRED_NOT_PUBLISHED` retirement, null target `CompletionStage` fail-closed handling that retains the unresolved mapping fence, replay reconstruction with first/later Producer-sequence-gap rejection before state installation, a typed local `EvidenceCursorV1` projection for the latest Lane Producer Journal position, exact local `PULSAR_ATTEMPT_JOURNAL` PUBLISHED evidence-branch projection (including sequence and mapping-record hash), and Broker last-sequence/retention-proof divergence classification are covered. Publish Attempt ledgers remain V1/V2 compatible and now have an optional V3 local projection for allocated sequence, latest acknowledged Journal position and `retirementPending`; `DelayShard` persists those updates without changing the Shard source-position cursor and reloads them fail-closed through the same inflight key. The adapter must still supply the exact Producer/Attempt identity and invoke these updates only after the corresponding Journal append; the Journal cursor/evidence branch are local value projections and are not a new Registry Admission field, a contiguous Broker-reader proof, authenticated Broker ACK/guard evidence, or an atomic substitute for the source-ordered outcome mutation. Nereus-owned non-compacted Pulsar topic, ExclusiveWithFencing writer, guarded reader/reconnect, Recovery-Floor retention and production Broker evidence remain release blockers |
| Kafka transactional receipt mapping-before-send seam | Implemented (local deterministic ordering/evidence seam; external transaction/evidence authority pending) | `KafkaReceiptResource`, `KafkaReceiptJournal`, `KafkaReceiptJournalTest`; explicit receipt cluster/topic UUID, Route/Shard and slot/partition geometry, one transactional-channel key per Shard, exact mapping-before-send and retransmission reuse, unresolved lower-sequence blocking, durable retirement/replay, typed `KAFKA_RECEIPT_CONTIGUOUS` cursor, `ReceiptObservation`/`Resolution` fail-closed matching of exact attempt/prepared/receipt-record hashes plus independent retirement/LSO/retention predicates, local `KAFKA_TRANSACTIONAL_RECEIPT` PUBLISHED branch and post-retirement `KAFKA_RECEIPT_ABSENCE` branch with target/receipt identity and generation fencing. This is a local appender/value projection only: the real same-cluster target-plus-receipt transaction, pinned `read_committed` Fetch/LSO and contiguous replay, ExclusiveWithFencing, retention/Floor proof, slot allocation authority and real-broker evidence remain release blockers |
| Target publish side-effect outcome boundary | Implemented (identity-fenced transport SPI, Worker-bound local physical admission seam and durable attempt/outcome subset) | `DestinationPublishAdapter`, `DestinationPublishResult`, `PinnedKafkaDestinationAdapter`, `PinnedPulsarDestinationAdapter`, `PulsarDestinationTimingPolicy`, `KafkaTargetResource`, `PulsarTargetResource`, `KafkaDestinationRequest`, `PulsarDestinationRequest`, `PulsarNativeSendRequest`, `DestinationPhysicalAdmission`, `BoundedDestinationPublishAdapter`, `WorkClassExecutionRegistry`, `PublishAttemptLedger`, `PublishOutcomeBody`, `DelayShard`, `DestinationAdapterTest`, `AdapterRequestIdentityTest`, `DestinationPhysicalAdmissionTest`, `BoundedDestinationPublishAdapterTest`, `WorkClassExecutionRegistryTest`, `DelayShardTest`; PUBLISHED results now require non-empty delivery identity/evidence, use stable code `OK`, and pair an optional pinned `BrokerResourceIdentityV1` with a uint32 physical partition; target resources, direct request values and the physical-admission target-cluster registry enforce canonical UTF-8/NFC cluster/topic identity before request construction or capacity accounting, and native request values reject a zero delivery identity; Kafka destination requests additionally require `actionAt=deliverAt`, so the early-action branch cannot reach transport; the Pulsar destination adapter defaults to the same ordinary timing relationship and accepts an early action only through an explicit fixed-lead `PulsarDestinationTimingPolicy`, which is a local pre-transport guard rather than Profile/Capability authority (`DestinationAdapterTest.pulsarDefaultTimingPolicyRejectsEarlyActionBeforeTransport`, `DestinationAdapterTest.pulsarCertifiedHandoffRequiresTheExactFixedLead`); local admission protects Worker and target-cluster request/byte caps, READY Lane minimums, Lane caps and zombie charges, counts a not-yet-ready candidate Lane's protected minimum exactly once when opening READY, and release validates every active/zombie bucket before decrementing so an accounting underflow cannot partially release a charge (`DestinationPhysicalAdmissionTest.zombieReleaseUnderflowDoesNotPartiallyDecrementActiveCharge`); one Worker work-class registry binds one exact physical pool, and that pool rejects a second registry before transport or charge, all cross-package bounded adapters must use the public registry-aware/caller-executor constructor, and no-registry constructors are package-local test seams (`BoundedDestinationPublishAdapterTest.productionCompositionBindsOneWorkerPhysicalAdmissionPool`, `BoundedDestinationPublishAdapterTest.onePhysicalAdmissionPoolCannotMultiplyCapacityAcrossWorkerRegistries`, `WorkClassExecutionRegistryTest.workerSingletonBindingUsesExactInstancePerResourceKind`); `BoundedDestinationPublishAdapter` dispatches delegate calls through an injected/default asynchronous Lane/Adapter executor instead of holding the adapter monitor across a synchronous transport call, and `blockingDelegateCallDoesNotBlockHealthyLane` covers same-adapter Lane isolation; executor rejection remains a pre-ownership release, while Pinned destination adapters mark synchronous transport exceptions, null stages, or unobservable callback registration as logical `UNKNOWN` without a physical-completion proof, and the wrapper retains those charges as `ZOMBIE`/in-flight until `PublishCall.releasePhysicalCharge()` follows certified completion or fenced teardown (`BoundedDestinationPublishAdapterTest.callbackRegistrationFailureRetainsPhysicalChargeUntilExplicitRelease`, `BoundedDestinationPublishAdapterTest.pinnedAdapterRegistrationFailureRetainsPhysicalCharge`, `BoundedDestinationPublishAdapterTest.pinnedAdapterTransportExceptionRetainsPhysicalCharge`, `DestinationAdapterTest.kafkaDestinationDoesNotInvokeTransportForEarlyActionAt`); the adapter-package default virtual-thread executor is only a local seam and production bounded executor, physical adapter evidence journal, durable ActiveLaneState/ReadyCertificate admission authority, authenticated non-persistence classifiers, and remaining outcome/evidence mutations remain pending; definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable state transitions, service-authored verified `EVIDENCE_RESOLUTION`, and Resolve `ATTACH_PUBLISHED_EVIDENCE`/`ATTACH_NOT_PUBLISHED_EVIDENCE` obligation settlement are covered locally |
| One Delay Shard = one RocksDB DB | Implemented | `ShardStore`, `StoreMetadata`, `StoreRuntimeMetadata`, `StoreRecoveryMetadata`, `RecoveryInstallStateV1`, `CompatibleControlSnapshotV1`, `SharedRocksDbResources`, `ShardStoreTest`, `StoreRuntimeMetadataTest`, `StoreRecoveryMetadataTest`, `RecoveryInstallStateV1Test`, `CompatibleControlSnapshotV1Test`; existing DBs missing the `meta_cf` shard-identity marker or carrying a store incarnation that disagrees with `incarnations/<storeIncarnation>/db` now fail closed instead of being initialized or opened as a different store; the local `meta/FIXED` projection persists ingress-fence/checkpoint identity at keys 4/7, owner-open epoch at key 8, typed evidence cursors at key 6, clean-close state at key 9 and the bounded shard-bound compatible control snapshot at key 10; the local `meta/RECOVERY` projection now persists typed lineage/base, last-observed Floor, catalog generation and install/open state at keys 1–4, updates them in the same WAL-synchronised batch as install/close boundaries, rejects foreign-shard Floor or mismatched Store Incarnation values, and records a new LOCAL_STORE candidate plus pinned Floor during restore; `hasReusableRecoveryProof()` remains only a local minimum-facts check and never proves current Floor ancestry or Oxia authority; owner-open epochs use raw uint64 encoding and unsigned monotonic comparison, including the high-bit boundary; the shared owned-shard semaphore is identity-bound to `ShardId`, so duplicate opens fail before a second DB incarnation can be created or installed and the exact identity is released only when its Store closes (`ShardStoreTest.duplicateOwnedShardOpenIsRejectedBeforeCreatingAnotherDb`); all values use bounded canonical validation and post-open metadata/format/install failures close every DB/Column Family handle and options object before slots release, covered by `ShardStoreTest.malformedExistingMetadataDoesNotLeaveRocksDbOpen` and `ShardStoreTest.malformedRuntimeMetadataDoesNotLeaveRocksDbOpen`; after the clean-close marker is durably written, all RocksDB read, scan, write, flush and sequence-number APIs fail closed instead of touching a closed native handle, covered by `ShardStoreTest.closedShardStoreFailsClosedForAllRocksDbOperations` |
| Worker DB/checkpoint resource limits | Implemented (config-envelope plus runtime shard/DB/acquire/restore slots, startup and fixed-delay runtime probes, sticky runtime safety gate, local native bucket ledger, placement scorer and physical usage guard seam) | `ShardStoreConfig`, `SharedRocksDbResources`, `ShardStore`, `WorkerResourceEnvelope`, `WorkerRuntimeResourceObservation`, `WorkerRuntimeResourceProbe`, `WorkerRuntimeResourceMonitor`, `WorkerRuntimeSafetyGate`, `NativeResourceUsage`, `WorkerNativeResourceLedger`, `WorkerCapacityAdmission`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `RocksDbUsageSnapshot`, `RocksDbUsageLimits`, `WorkerResourceEnvelopeTest`, `WorkerRuntimeResourceProbeTest`, `WorkerRuntimeResourceMonitorTest`, `WorkerRuntimeSafetyGateTest`, `WorkerNativeResourceLedgerTest`, `WorkerCapacityAdmissionTest`, `WorkerPlacementPolicyTest`, `RocksDbUsageLimitsTest`, `ShardStoreTest`; checked memory/FD/disk cross-bucket inequalities fail closed before resource creation, including the live process descriptor count plus configured FD headroom, the disjoint native ledger attributes shared block-cache/WBM reservations before JNI creation with exact allocation identity and checked release, and a release computes both successor buckets before removing the identity so an underflow remains retryable (`WorkerNativeResourceLedgerTest.underflowingReleaseLeavesReservationForRetry`); shared-resource teardown separately retries each native reservation after the corresponding cache/WBM close and does not mark the Worker closed until both reservations are released (`WorkerNativeResourceLedgerTest.sharedResourceCloseRetriesReservationsAfterReleaseFailure`); logical `maxOwnedShards` and physical `maxOpenShardDbs` slots are tracked independently, short-lived shard-open/restore acquisition has an independent `maxConcurrentAcquiresPerWorker` slot released after native open or cleanup, checkpoint restore/download staging has an independent bounded slot released after atomic installation, the process-shared RocksDB `Env` background pool is explicitly configured from `maxBackgroundJobs`, each DB binds `maxBackgroundJobsPerDb` plus nonzero `reservedFlushJobs`/`maxCompactionJobs` split, each CF keeps the explicit `maxWriteBufferBytesPerDb` per-CF ceiling and each DB now also binds the same value as the aggregate `db_write_buffer_size` (`ShardStoreTest.perDbWriteBufferCeilingIsBoundAtRocksDbDbLevel`), while the process-wide `WriteBufferManager` remains the shared memtable budget, shared RocksDB resources refuse premature close, local Worker admission sums distinct shard `committed` vectors plus fixed/transition demand once with checked per-dimension arithmetic, and the local placement seam rejects over-capacity candidates before applying dominant-resource/load scoring with stale-telemetry penalty, minimum residence, hysteresis and movement cost; `WorkerRuntimeResourceProbe` reads bounded JVM heap/direct-memory, procfs RSS/RLIMIT, live process FD count, cgroup v2/v1 and exact root filesystem total/usable limits, `WorkerResourceEnvelope.validate(config, observation)` rejects any certified envelope that exceeds them, and `WorkerRuntimeSafetyGate` fences new ownership/restore plus the embedded Claim path after a failed fresh observation until explicit empty-drain activation; `WorkerRuntimeResourceMonitor` runs a closeable fixed-delay probe and routes probe/validation failures into the same sticky drain state; `RocksDbUsageSnapshot` observes live SST/WAL/MANIFEST/L0/compaction-pending and exact DB file totals, while `RocksDbUsageLimits` checks duplicate shard identity, per-DB caps, checked Worker totals and the exact root filesystem free-space floor; `ShardStoreTest.physicalUsageProbeAndGuardObserveOneShardDb`, `ShardStoreTest.perDbWriteBufferCeilingIsBoundAtRocksDbDbLevel`, `WorkerRuntimeResourceProbeTest`, `WorkerRuntimeResourceMonitorTest`, `WorkerRuntimeSafetyGateTest`, `WorkerNativeResourceLedgerTest` and `RocksDbUsageLimitsTest` cover the local probe/ledger/guard; production per-work-class write-time reserve enforcement, cooperative assignment or Oxia placement capacity authority remain pending |
| Seven application CFs plus empty `default` CF | Implemented | `ShardStore` descriptor validation |
| Worker dynamic per-DB physical-usage observer | Implemented (local monitor lifecycle and aggregate guard; authority pending) | `WorkerRocksDbUsageMonitor`, `SharedRocksDbResources.startRocksDbUsageMonitor`, `ShardStore`, `WorkerRocksDbUsageMonitorTest`; every open shard Store registers one identity-bound physical source, the source is removed before native teardown, fixed-delay observations validate per-DB and Worker WAL/MANIFEST/SST/L0/compaction/file caps plus filesystem free-space floor, and missing/invalid/over-capacity evidence fences the shared sticky runtime gate; production write-time reserve attribution, checkpoint/compaction scheduling authority and Oxia placement capacity remain release gates |
| Worker event-loop work-class queue/turn seam | Implemented (local scheduler/resource/config/action lifecycle and exact resource-graph binding; production authority pending) | `WorkClass`, `WorkClassPolicy`, `WorkClassRuntimeConfig`, `WorkClassTask`, `WorkClassScheduler`, `WorkClassEventLoop`, `WorkClassDispatcher`, `WorkClassExecutionRegistry`, `SharedRocksDbResources`, `WorkClassSchedulerTest`, `WorkClassRuntimeConfigTest`, `WorkClassEventLoopTest`, `WorkClassDispatcherTest`, `WorkClassExecutionRegistryTest`, `SharedRocksDbResourcesTest`; all eight V1 classes require positive weights and bounded queue/turn records, bytes and time, including the independent `CHECKPOINT` execution class; the runtime config has no fallback defaults, requires exact eight-class coverage, only permits `LEASE_FENCE` preemption, requires nonzero minima for the six correctness/progress classes, checks aggregate minima against the shared record/byte pool and constructs scheduler/resource pool from one monotonic clock; `LEASE_FENCE` gets the first bounded preemptive turn but carries a cross-poll preemption debt so a continuously queued fence class yields to a runnable ordinary class (`WorkClassSchedulerTest.continuousPreemptiveQueueYieldsAcrossSmallPolls`), and stale queued classes are selected after the configured maximum delay; `WorkClassEventLoop` acquires one exact resource lease per task immediately before queue removal, restores the scheduler snapshot and releases earlier leases on a later admission rejection, fences a second poll until the prior turn closes, reserves the selected queue capacity against concurrent offers, and `runTurn` executes the bounded callback sequence outside the monitor while closing all leases on success or failure; a fatal/hold stop requeues only the exact trailing tasks whose handlers were never invoked, while an invoked task is not requeued; `WorkClassDispatcher` rejects incomplete class coverage and routes selected tasks through those same boundaries; `WorkClassExecutionRegistry` binds the complete task identity and byte charge before queue admission, rolls registration back on rejection, removes successful actions, retains started failures for exact explicit retry and leaves unstarted fatal suffix actions queued; every owner-side executor and all three checkpoint-side production executors bind through their Store/coordinator to one exact `SharedRocksDbResources`, and the resources/registry exact-identity fence is bidirectional so neither one resources object graph can accept a second registry nor one registry span two Store resource envelopes (`SharedRocksDbResourcesTest`); this proves one object graph cannot fork, while production bootstrap uniqueness, dynamic RocksDB WriteBatch admission and WriteBatch/IO authority remain release gates |
| Worker work-class reserve token seam | Implemented (local checked pool plus bounded-turn binding; production wiring pending) | `WorkClassResourcePool`, `WorkClassEventLoop`, `WorkClassResourcePoolTest`, `WorkClassEventLoopTest`; exact record/byte leases protect every other class' configured non-borrowable minima, convert acquisition-sum overflow into a closed resource rejection, mark borrowed capacity, bound borrowed hold time, release idempotently and are reacquired per bounded turn rather than held while queueing; production dynamic RocksDB attribution and WriteBatch/IO admission authority remain release gates |
| Active and recovery source-apply work-class admission | Implemented (local active/recovery handler wiring; production source authority pending) | `SourceApplyWorkClassExecutor`, `OwnerRecoveryCoordinator`, `OwnedDelayShard`, `SourceReplayEntry`, `OwnerRecoveryCoordinatorTest`, `SourceApplyWorkClassExecutorTest`; active Command and signed System Mutation tasks, plus strict `CATCHING_UP` replay actions, bind a domain-separated identity to exact canonical Source Position/NDL1 frame bytes and charge their checked exact length, perform a mutation-free assignment/barrier/guard preflight before `SOURCE_APPLY` admission, reread execution-time Owner Lease/session before the Shard WriteBatch, return results projected to the current physical position, and advance the recovery cursor only after the exact action outcome and look-ahead identity are proven; ordinary failure remains source-owned and does not create a competing generic retry authority; direct active/recovery apply methods are ownership-package-only, while real Broker consumer/ACK, production Oxia session and dynamic WriteBatch/IO reserve attribution remain release gates |
| Active READY-discovery and Claim-handoff work-class admission | Implemented (local bounded discovery, exact Store/Owner/Worker-permit binding, live-certificate handler, exact Claim rollback and logical permit seam; external authority pending) | `DueSchedulerWorkClassExecutor`, `ClaimHandoffWorkClassExecutor`, `PublishAdmissionWorkClassExecutor`, `ClaimExecutionAdmission`, `WorkClassExecutionRegistry`, `OwnedDelayShard`, `PersistentLaneScheduler`, `ActiveLaneStateV1`, `DueSchedulerWorkClassExecutorTest`, `ClaimHandoffWorkClassExecutorTest`, `PublishAdmissionWorkClassExecutorTest`, `ClaimExecutionAdmissionTest`, `WorkClassExecutionRegistryTest`; discovery retains its exact Shard/canonical trusted-time/scan-budget identity and full rollback. Shared due/Claim preflight requires the persistent scheduler and owned runtime to have the same ShardId, complete Owner identity and byte-equal Store Incarnation; a same-Shard/same-Owner scheduler over another DB is rejected before action registration or Store access. One Worker registry binds one exact Claim permit pool, all Claim/Publish Admission executors reuse it, the pool rejects a second registry, and foreign-pool Reservations fail before action registration/append (`WorkClassExecutionRegistryTest.oneClaimAdmissionPoolCannotMultiplyCapacityAcrossWorkerRegistries`). The Claim action accepts only a previously polled head, binds typed materialization/deadline/charge, rereads READY/Message/Timeline/typed Lane/Ready Certificate after queue wait, enforces Worker/Shard/Lane message-and-byte caps plus READY minima, exact-requeues known deferrals, and fences unknown failures. A successful local Claim consumes READY before releasing the retained scheduler identity; a READY certificate may remain with no physical key/current timing projection after the head is consumed. Profile/catalog, Object Store, Adapter serialization/size, channel/credential live generations, Publish Admission/Producer, production trusted-time/Oxia capacity authority and dynamic IO attribution remain release blockers |
| Message expiry discovery and exact-mutation handoff | Implemented (local record/actual-byte/elapsed bounded discovery plus append handoff; external authority pending) | `ExpiryDiscoveryWorkClassExecutor`, `ExpiryWorkClassExecutor`, `BoundedReadBudget`, `OwnedDelayShard`, `DelayShard`, `ShardStore`, `ExpiryDiscoveryWorkClassExecutorTest`, `ExpiryWorkClassExecutorTest`, `ShardStoreTest`; discovery binds the exact Shard/canonical Trusted-UTC evidence/full scan envelope before `EXPIRY` admission, rejection reads no Oxia/clock/Store state, execution rereads strict Owner authority, and one shared budget charges actual `timeline_cf/EXPIRY` plus dependent `id_cf/MESSAGE` key/value bytes and elapsed time. An individually oversized candidate fails closed; a later candidate that does not fit remains durable for another turn. Discovery is state-neutral, while each exact candidate is independently signed and handed to the external Shard Log appender without local Source Position allocation; production scheduling, Trusted-Time/Oxia authority, Broker append/ACK and source replay remain release blockers |
| Scheduled checkpoint work-class admission | Implemented (local concrete handler wiring and derived static request charge; production authority pending) | `CheckpointWorkClassExecutor`, `CheckpointExecutionCoordinator`, `WorkClassExecutionRegistry`, `CheckpointExecutionCoordinatorTest.checkpointWorkClassRejectsBeforeIoThenExecutesTheExactClaim`; exact scheduler claim and pending intent are fenced before `CHECKPOINT` queue admission and repeated before I/O, normalized path/claim/pending-intent/upload-time bytes form one canonical identity whose domain-separated hash is the task ID and whose exact length is the queue byte charge, caller-supplied `workClassBytes` is absent, negative upload time fails before admission, direct preflight/execution methods are package-private so cross-package composition must use the bounded entrypoint, queue rejection leaves the current claim and filesystem/provider state unchanged, ordinary attempt failure returns a checkpoint-owned outcome without a stale generic work action, and only the next exact scheduler claim can start the next physical attempt; Owner Lease/session, Source Assignment, external Object Store/Oxia authority, the other shard-specific handlers and dynamic checkpoint-file/upload/WriteBatch I/O attribution remain release gates |
| Shard identity and local Store Incarnation validation | Implemented | `StoreMetadata`, `ShardStoreTest` |
| Synchronous atomic WriteBatch | Implemented | `ShardStore.write`, `ShardStoreTest` |
| Native RocksDB checkpoint creation | Implemented | `ShardStore.createCheckpoint`, `ShardStoreTest`; checkpoint creation is staged under the same-filesystem `checkpoint-tmp` namespace, rejects an existing target, installs only through an atomic rename, and removes a target that was moved but failed the subsequent parent-directory durability step, with failed-stage cleanup |
| Scheduled checkpoint execution | Implemented (local orchestration; production authority pending) | `CheckpointExecutionCoordinator`, `CheckpointExecutionCoordinatorTest`; exact scheduler claim is fenced before I/O and completed with the same handle after success/failure, fixed intent checkpoint identity is passed to `ShardStore.createCheckpoint`, an existing directory is reusable only with the matching persisted `lastCheckpointId`, and manifest identity/source/mutation/evidence/control projections are checked before upload/catalog publication; Owner Lease/session, Source Assignment, Object Store attestation/quiescence and cross-record Oxia transaction remain release blockers |
| Crash-durable local checkpoint Upload Intent | Implemented (local projection plus per-intent Oxia CAS backend; cross-record authority pending) | `CheckpointUploadIntentAuthority`, `CheckpointUploadIntentStore(Path)`, `OxiaSyncCheckpointUploadIntentBackend`, `CheckpointUploadIntentStoreTest`, `OxiaSyncCheckpointUploadIntentBackendTest`; the local state file and concrete Oxia record both store the complete canonical intent, enforce exact PENDING_UPLOAD -> PUBLISHED/REAPING revision CAS, deadline evidence and exact response-loss reread; local reopen/checksum and Oxia corruption/reopen paths are covered. The coordinator accepts either authority, while Owner Lease/session + catalog publication transaction, Object Store attestation, owner-abandonment and reaping/quiescence remain external |
| Checkpoint file inventory and canonical manifest projection | Implemented (local physical upload/download seams and CAS projection; external publication pending) | `CheckpointFileInventory`, `CheckpointManifestLimits`, `CheckpointManifest`, `CheckpointManifestJson`, `CheckpointResourceV1`, `CheckpointUploadStateV1`, `CheckpointUploadIntentV1`, `CheckpointDownloadRequest`, `CheckpointUploadIntentStore`, `CheckpointReapingGuard`, `CheckpointUploadAdapter`, `CheckpointUploadRequest`, `CheckpointUploadCoordinator`, `CheckpointExecutionCoordinator`, `FilesystemCheckpointUploadAdapter`, `FilesystemCheckpointDownloadAdapter`, `CheckpointControlSnapshotVerifier`, `CheckpointScheduler`, `CheckpointManifestTest`, `CheckpointResourceV1Test`, `CheckpointUploadIntentV1Test`, `CheckpointUploadIntentStoreTest`, `CheckpointReapingGuardTest`, `CheckpointUploadCoordinatorTest`, `CheckpointExecutionCoordinatorTest`, `FilesystemCheckpointUploadAdapterTest`, `FilesystemCheckpointDownloadAdapterTest`, `CheckpointControlSnapshotVerifierTest`, `CheckpointSchedulerTest`; inventory and the filesystem adapters stream SHA-256 over each file without loading an SST into heap, reject symbolic links or non-regular files before copy, enforce explicit manifest limits, derive deterministic immutable object paths from opaque object key/version, write the manifest last through temporary-file/atomic-rename, verify identical existing bytes on retry, validate catalog-bound manifest/resource identity before download, re-inventory the complete private restore tree and publish it only through an atomic rename; the manifest decoder enforces the closed field order/types, Kafka/Pulsar typed `EvidenceCursorV1` branches, strict cursor identity ordering and byte-identical canonical JSON round trip; the published-manifest Object Store identity and upload-intent state branches have closed canonical codecs, and the local coordinators verify exact pending intent, deadline, shard/lineage/owner/store/parent identity, complete local file inventory, recognized RocksDB format/store identity (`StoreMetadata` shard/`dbIdentity`/Store Incarnation) plus key-10 control snapshot, Worker upload/download slots, then reread the exact intent immediately before provider I/O, and validate returned manifest length/SHA-256 before PENDING_UPLOAD -> PUBLISHED revision CAS; the local scheduler validates interval/jitter, spreads owned shards deterministically, fences duplicate in-flight claims, and requires the exact returned claim handle for completion so stale callbacks cannot reset a newer attempt; an exact PUBLISHED successor is reread after response loss without another adapter call, and a concurrent post-slot publication/change is handled without invoking the adapter (`CheckpointUploadCoordinatorTest.rereadsIntentAfterUploadSlotBeforeProviderIo`); the guarded local PENDING_UPLOAD -> REAPING overload also rejects published catalog protection, same-checkpoint active RecoveryPin protection, unavailable catalog/pin reads, and Floor/coverage authority failures, while legacy no-limits overloads remain compatibility seams and real Oxia lease/session/catalog CAS, Object Store credentials/quiescence/attestation/publication/deletion, owner-abandonment proof and reaping/quiescence remain pending |
| Checkpoint restore into a new Store Incarnation | Implemented (local download-to-restore orchestration and bounded `CHECKPOINT` admission; external authority pending) | `CheckpointDownloadRequest`, `CheckpointDownloadAdapter`, `FilesystemCheckpointDownloadAdapter`, `CheckpointRestoreCoordinator`, `CheckpointRestoreWorkClassExecutor`, `ShardStore.restoreFromCheckpoint`, `CheckpointManifest.decodeCanonicalJson`, `CheckpointManifestLimits`, `FilesystemCheckpointDownloadAdapterTest`, `CheckpointRestoreCoordinatorTest`, `ShardStoreTest`; the local downloader verifies the catalog-bound manifest object/resource identity, streams immutable file objects into a private temporary tree, re-inventories every file before atomic target publication and removes failed staging; `CheckpointRestoreWorkClassExecutor` binds the exact manifest/resource/optional-pin request identity and checked byte charge before `CHECKPOINT` admission, performs no catalog/provider/filesystem I/O on queue rejection, and runs the complete download → inventory validation → Store-Incarnation install action before returning a caller-owned open Store or restore failure evidence; `CheckpointRestoreCoordinator` constrains provider output to a per-attempt staging boundary, validates the complete downloaded inventory and only then invokes `ShardStore`'s finite-limit manifest/pin-aware restore, deleting the provider tree after Store Incarnation installation; `ShardStore` raw-decodes and catalog-validates canonical manifest bytes, enforces the raw manifest byte cap before JSON parsing and the finite limit set before complete source-file verification/copy, re-inventories the private restore-tmp copy against the same manifest before staged open/install, compares staged DB identity plus persisted `lastCheckpointId`, `appliedShardLogPosition`, `shardMutationSequence`, evidence cursors, required key-10 `CompatibleControlSnapshotV1.snapshotDigest` and `meta/RECOVERY` lineage/base/Floor/install projections to the exact manifest before install (including candidate checkpoint/hash and LOCAL_STORE source Store Incarnation), directory-fsyncs the newly renamed Store Incarnation parent before the checksummed `ACTIVE` pointer can publish it, and the pin-aware overload rereads the exact active RecoveryPin after staging, immediately before the atomic Store Incarnation move, and again immediately before ACTIVE publication; `FilesystemCheckpointDownloadAdapterTest`, `CheckpointRestoreCoordinatorTest.restoreQueueRejectionDoesNotCallProviderOrCreateStaging`, `ShardStoreTest.catalogBoundRestoreRejectsPinDriftBeforeActivePublication`, `ShardStoreTest.restoreWithManifestRejectsMissingControlSnapshot`, `ShardStoreTest.restoreWithManifestRejectsControlStateDigestDrift`, `ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionLineageDrift`, `ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionCheckpointDrift` and `ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionManifestHashDrift` cover the projection-splice, object-integrity, staging-boundary and late-session fences; legacy no-limits restore overloads remain compatibility seams, while Oxia Recovery Pin/Floor CAS, Source Assignment and source replay remain pending |
| Recovery catalog, lineage and Floor selection | Implemented (typed local codecs, crash-durable local projection, typed cursor-dominant Floor projection, manifest-bound evidence cursors, upload-intent-bound publication and bounded pin authority; single-record Oxia catalog CAS plus read-only local-reuse validation) | `RecoveryFloorRefV1`, `RecoveryCandidateRefV1`, `RecoveryPinV1`, `EvidenceCursorV1`, `RecoveryCatalog`, `PersistentRecoveryCatalog`, `RecoveryCatalogAuthority`, `OxiaRecoveryCatalog`, `OxiaSyncRecoveryCatalogBackend`, `RecoveryFloor`, `RecoveryFloorRefV1Test`, `RecoveryCandidateRefV1Test`, `RecoveryPinV1Test`, `EvidenceCursorV1Test`, `RecoveryCatalogTest`, `PersistentRecoveryCatalogTest`, `OxiaSyncRecoveryCatalogBackendTest`; the typed references canonically bind lineage/checkpoint/manifest, catalog generation, Source Position, mutation sequence, sorted evidence cursors, candidate branch and session identity digest; the local catalog still binds one shard, rejects non-zero genesis lineage, enforces floor ancestry, requires the Floor cursor set to byte-match the candidate manifest and then enforces same-generation cursor dominance, exposes candidate validation/selection and `proveFloorCoverage`, independently fences requested mutation/source boundary coverage with canonical Source Position equality on equal order tokens, requires a PUBLISHED `CheckpointUploadIntentV1` plus exact manifest/object/owner/store/parent identity for the local publication projection, validates the same request binding before an Oxia upload-intent CAS, copies mutable checkpoint/digest/cursor/coverage inputs before backend CAS, validates optional Oxia publication Floors and all candidate/ancestry manifests against their complete canonical JSON projection, shard and publication generation, and validates every Oxia coverage ancestry edge against published parent id/hash, lineage/source/mutation progression and evidence-cursor dominance; the single-record Oxia backend persists one bounded manifest/resource/scalar-Floor/typed-Floor snapshot with version CAS and validates reusable local Store lineage/Floor/install projections against it; `PersistentRecoveryCatalog(Path)` persists sorted canonical manifests, resource identities, scalar/typed Floors and one active pin with checksum, atomic replacement, directory fsync and cross-instance locking, and reloads the exact snapshot before each read/CAS; a session-bound pin is also reopen-safe after the current Floor advances because its historical observed Floor and full candidate ancestry are retained and revalidated (`PersistentRecoveryCatalogTest.historicalRecoveryPinSurvivesReopenAfterFloorAdvances`); durable Oxia Owner Lease/session and cross-record catalog/pin CAS, real Object Store publication/attestation, and evidence-cursor retention/dominance enforcement remain pending |
| Command applied/rejected state machine | Implemented (embedded core) | `DelayShard`, `DelayShardTest`, `DurableResultTest` |
| DUE/ORDERED/READY/EXPIRY timeline namespaces | Implemented (embedded core subset) | `DelayShard`, `MessageRecord`, `TimelineWorkRef`, `GenerationRuntimeIndex`, `ClaimRecord`, `ReadyIndexValue`, `KeyCodec`, `GenerationRuntimeIndexTest`, `KeyCodecTest`, `DelayShardTest`; READY key/value, laneVersion fencing, retry eligibility for unordered definitive retry, canonical timeline semantic/instance digests, v4 runtime-index persistence, atomic affected-lane updates, durable Claim removal/restoration including source-ordered Lane Pause rollback, fenced rebuild/discovery, replay-stable Claim Result timeline-key/semantic-digest checks, current `id_cf/MESSAGE` reads and activation/close/retirement scans fenced to the self-routing key shard and embedded schedule Source Position shard, Cancel/Reschedule fencing whenever an UNCERTAIN obligation survives a current-work projection, the pinned-policy `UNKNOWN` scheduling plus `UNCERTAIN_RETRY` Admission subset that materializes timeline work while retaining the old obligation, ProfileCatalog-backed `ResolvedSchedule.actionAtEpochMs` persistence and replay derivation, `headEligibilityAt=max(actionAt,retryEligibility)` READY projection while ORDERED keys remain deliverAt-based, and exact constructors for the registered FENCE, GC protection, Producer and Recovery key layouts are covered; new DUE/ORDERED and paired EXPIRY writes now store direct canonical `TimelineWorkRef` values, readers validate the embedded key, current runtime projection and rich READY eligibility, and legacy `TimelineEntry` values are accepted only as a read-only migration seam; DUE rich values now require the physical key timestamp to equal `max(actionAt,retryEligibilityAt)`, ORDERED values cannot precede that eligibility and `UNCERTAIN_RETRY` cannot use an ordered namespace; `CONTROL_OVERRIDE` nested ControlRef/Source Position values are canonical-decoded before becoming durable and must belong to the self-routing Shard encoded in the timeline key; canonical v4 `GenerationRuntimeIndex` now rejects aggregate/current-work drift, uncertain obligations attached to a non-`UNCERTAIN_RETRY` timeline and non-terminal `NONE` placeholders, while old scalar-only `MessageRecord` values round-trip only through an explicit legacy v3 compatibility seam and must be replaced before a new typed write; typed `MessageRecord` status/runtime drift is also rejected; `DelayShardTest.resolvedActionAtIsEarlierThanDeliverAtButOrderedKeyKeepsBusinessVisibilityOrder`, `GenerationRuntimeIndexTest.timelineWorkFencesPhysicalEligibilityAndOrderedUncertainRetry`, `GenerationRuntimeIndexTest.controlOverrideTimelineRequiresCanonicalTypedNestedValues`, `GenerationRuntimeIndexTest.controlOverrideTimelineRejectsSourcePositionFromAnotherShard`, `GenerationRuntimeIndexTest.runtimeIndexFencesAggregateAndCurrentWorkProjectionDrift`, `MessageRecordTest.typedRuntimeCannotDisagreeWithMessageStatus`, `MessageRecordTest.handedOffStatusUsesTheRegisteredTerminalAggregateProjection` and `LaneSchedulerTest.fencedRecoveryAcceptsCanonicalTimelineWorkRefValue` cover the local boundary; authenticated control authority and policy-bound timeline materialization beyond this local budget remain pending |
| `CLAIMED`/`PUBLISHING`/`UNCERTAIN` attempt ledger and obligation locator | Implemented (durable local Claim plus source-ordered attempt subset) | `ClaimRecord`, `PublishAttemptLedger`, `AttemptObligationRef`, `GenerationRuntimeIndex`, `PublishAdmissionBody`, `PublishOutcomeBody`, `RetryPolicyCatalog`, `DelayShard`, `DelayShardTest`; local Claim sequence/key/value and exact precondition/instance digest are persisted atomically, registry-shaped runtime index and canonical obligation-set digest are persisted with v4 Message records, source-ordered `PUBLISH_ADMISSION_V1` checks replay-stable timeline key/semantic/counter/obligation projections and descriptor attempt number, including the `UNCERTAIN_RETRY` source-work branch, reconstructs the same PUBLISHING attempt when the reversible Claim is absent but source state matches, retains admission counters across definitive retry timelines, structurally bounds descriptor `actionAt <= deliverAt`, and when the exact Profile catalog is supplied validates ordinary timing or the pinned certified Pulsar handoff lead before admission; when the catalog is supplied it also revalidates the pinned immutable policy budgets at Admission, uncertain retry, and reopen; shard activation now performs bounded bidirectional reconciliation between every current/terminal runtime obligation ref and its exact PUBLISHING/UNCERTAIN ledger, and between every live ledger/Claim and the current Message branch, failing closed on an orphan, missing counterpart, persisted total-admission overflow, or terminal/runtime summary mismatch; source-ordered `PUBLISH_OUTCOME_V1` UNKNOWN atomically migrates to UNCERTAIN, can materialize the pinned-policy `UNCERTAIN_RETRY` timeline subset without consuming the retry counter, but a closed Lane keeps the generation in UNCERTAIN with no retry timeline, verified-success closes the ledger, definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable outcomes atomically requeue, terminalize, or block the lane, and a definitive not-published outcome on a closed Lane is terminalized as `LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED` without retry; definitive Outcome/Resolution transfer now must be canonical byte-identical to the retained Admission charge, with mismatch persisted as `REJECTED(STALE_SYSTEM_MUTATION)` without changing the attempt/message/timeline/quota, while UNKNOWN transfer remains opaque; late outcomes from either the current or an older generation settle only the exact ledger/terminal summary while preserving terminal decision state and monotonically raising duplicate risk for verified success; service-authored verified `EVIDENCE_RESOLUTION_V1` can close or requeue the exact UNCERTAIN ledger; `RetryDecisionV1.completed_attempt_no` and DLQ physical attempts are parsed as complete `uint32` values, with high-bit round-trip, upper-bound rejection, and unsigned checked DLQ successor/comparison coverage in `PublishOutcomeBodyTest`, `DlqExportResultBodyTest` and `DelayShardTest`; `meta_cf/QUOTA quotaClass=3` now also records each durable Claim and attempt obligation's `inflight_messages/inflight_bytes` by Lane, rebuilds from `inflight_cf` and repairs a missing legacy map on release; full Profile/Adapter/evidence/capacity validation, immutable RetryPolicy publication/authority, multi-obligation terminal-summary lifecycle beyond current-generation settlement, uncertain-retry ControlRef/policy enforcement beyond the local automatic budget, logical/retained/evidence quota dimensions, Recovery Floor/source replay and full Claim materialization/recovery semantics remain pending |
| Claim/Prepared Publish materialization projection | Implemented (typed local canonical codec and raw-bit runtime projection; external authority pending) | `PayloadForPublishV1`, `ClaimMaterializationV1`, `ClaimResultBody`, `PublishAdmissionBody`, `PublishAdmissionBody.Descriptor`, `ClaimRecord`, `PublishAttemptLedger`, `MessageRecord`, `KeyCodec`, `UnsignedInt32`, `PayloadForPublishV1Test`, `ClaimMaterializationV1Test`, `ClaimResultBodyTest`, `PublishAdmissionBodyTest`, `PublishAttemptLedgerTest`, `MessageRecordTest`, `KeyCodecTest`, `UnsignedInt32Test`; inline/object payload union, exact length/SHA-256, Profile slot kinds, Broker resource and adapter-metadata branch agreement, complete uint32 partition/generation, nonnegative timing/order constraints, canonical bytes and `nereus-delay-claim-materialization-v1` digest are shared across Claim Result, Admission and Descriptor parsing; compatibility runtime fields preserve raw high-bit generation/attempt bits, key/value encoders use bit-preserving u32 writes, generation comparison and checked successor paths use unsigned order, and public Message/Command projections round-trip the complete domain; the legacy `CommandResult` rejected-result absence sentinel remains a separate compatibility boundary and is never interpreted as a valid generation; typed accessors do not perform live catalog/Object Store/Producer calls, so full materialization authority, adapter serialization/target-size certification and crash recovery remain blockers |
| Reversible Claim activation recovery | Implemented (bounded local seam; external authority pending) | `DelayShard.requeueClaimsForRecovery`, `OwnedDelayShard.activateForCommands`, `DelayShardTest.localClaimIsDurableAndRecoveryRequeueRestoresSemanticTimelineAtomically`, `OwnerLeaseTest.activationRequeuesRestoredClaimBeforeOpeningCommandGate`; before either activation path opens `ACTIVE_FOR_COMMANDS`, all bounded `inflight_cf/CLAIMED` records are scanned and restored through atomic timeline/Message/READY/quota batches with the same semantic work digest and a checked successor runtime instance; malformed/over-bound claims fail closed, while `PUBLISHING`/`UNCERTAIN` remain for source-ordered outcome/evidence recovery. Source successor, Oxia lease/session, adapter materialization, full obligation accounting and external Producer authority remain release blockers |
| Publish Admission timing/profile gate | Implemented (local semantic gate and exact catalog composition; publication authority pending) | `PublishAdmissionBody.requireTimingPolicy`, `PublishAdmissionBody.requireBrokerTiming`, `DelayShardConfig`, `DelayShard` optional `ProfileCatalog`, `ProfileCatalogV1ScheduleResolver`, `ShardStore.RocksDbWriteFailure`, `PublishAdmissionBodyTest`, `DelayShardConfigTest`, `ShardStoreTest.nativeWriteFailureHasATypeDistinctFromSemanticStaleness`, `DelayShardTest.publishAdmissionTimingFailureRevokesMatchingClaimBeforePersistingStaleMutation`, `DelayShardTest.catalogBackedActionAtDerivationFailsClosedWhenPinnedProfileDisappears`, `DelayShardTest.decoratedScheduleResolverRequiresTheExactShardProfileCatalog`; descriptor structure permits only `actionAt <= deliverAt`, catalogued shards validate exact Destination/Delivery Capability refs, fixed V1 adapter encoding and metadata branch, ordinary managed timing, fixed certified Pulsar handoff lead/capability, target resource and explicit/hash partition policy, and every apply/replay checks source Broker persistence time against configured `maxIngressBrokerTimestampDivergenceMs`/`maximumAdmissionMutationEnqueueAgeMs` with checked expiry and decision-interval distance arithmetic; a pre-decorated Schedule resolver is accepted only with the exact same catalog instance used by later Publish Admission and actionAt-recovery lookups, while nested/foreign/missing catalog composition fails before Store reads; timing/profile failures revoke an exact matching live Claim before persisting `STALE_SYSTEM_MUTATION`, so no attempt or Producer state is created; persisted V1 binding actionAt derivation also fails closed on missing/mismatched Profile/Capability rather than falling back to `deliverAt`; a native RocksDB WriteBatch/sync reread failure is typed and propagated instead of being downgraded to stale or advancing the source position; catalog-less compatibility shards fail closed to `actionAt=deliverAt`; formal Broker-time certification, Profile publication, Broker guard attestation and production Producer authority remain release blockers |
| Terminal generation history | Implemented (current/older-generation open-obligation summary and late-settlement subset) | `TerminalGenerationRecord`, `ClaimRecord`, `DelayShard`, `DelayShardTest`, `TerminalGenerationRecordTest`; Claim Result, publish outcome, expiry and command terminalization persist the exact terminal state version together with remaining obligation refs and duplicate-risk projection in a versioned terminal record, direct reads fence embedded `messageId/generation` against the `terminal_cf` key and require the applied Source Position to belong to the current Shard, activation reconciles both current and older-generation summaries against their exact inflight ledgers, and late verified or definitive not-published outcomes remove only their exact old ledger/ref without changing terminal status/code/time; legacy v1 terminal records decode as empty summaries; older-generation callbacks after full Dead Letter Replay, Replay retention and guarded GC remain pending |
| Large-payload reservation/proof/commit | Implemented (embedded core plus typed wire proof/response and deterministic local Object Store seams) | `LargeScheduleIntent`, legacy `PayloadCommitProof`, `PayloadCommitProofView`, `PayloadCommitProofV1`, `PayloadProofTrustSet`, `PayloadReference`, `PayloadReservation`, `PayloadReservationReceiptV1`, `OpaquePayloadUploadHandleV1`, `PayloadUploadHandleResponseV1`, `PayloadAttestationResponseV1`, `InMemoryPayloadObjectStore`, `FilesystemPayloadObjectStore`, `PayloadReferenceTest`, `PayloadProofTrustSetSemanticV1Test`, `InMemoryPayloadObjectStoreTest`, `FilesystemPayloadObjectStoreTest`, object-backed `MessageRecord`, `DelayShardTest`, `ReservationExpiryDiscoveryWorkClassExecutor`, `ReservationExpiryWorkClassExecutor`, `ReservationExpiryDiscoveryWorkClassExecutorTest`, `ReservationExpiryWorkClassExecutorTest`, `CommitLargeScheduleBodyV1Test`, `ProtocolCodecTest`; Prepare/Commit reservation quota, durable immutable Prepare receipt-anchor state/source, strict v1-to-v2 reservation-value upgrade, source-ordered TIME_FENCE overlay, record/actual-byte/elapsed bounded `RESERVATION_EXPIRY` discovery that uses only persisted `closedIngressDeadlineThrough`, byte-identical `id_cf` projection fencing, separate bounded GC-class discovery/materialization handoffs with queue-rejection and execution-time Owner fencing, bounded and key/value-identity-checked reservation lookup with duplicate detection plus Source Position shard fencing, guarded local quota release, exact reservation-bound handle issuance, configured lifetime/reservation-expiry bound and post-expiry re-sign, receipt-derived service-owned container/key with exact source/state/trust-set/object-identity validation before handle/upload/attestation, immutable-if-absent upload, typed Object Store proof Profile/tenant-scope/optional-etag/proof-id/signature validation, committed ReservationId/accepted-ProofId local retention, exact historical-signature idempotency after issuer close, malformed-signature stable rejection, cached attestation, receipt-anchor preservation across legal local `ABANDONED`/`EXPIRED`/`CLOSED` lifecycle updates, fixed payload-scoped error branches and typed attestation/commit response round-trips, and crash-durable filesystem payload bytes with no-follow reads, fsync/atomic publication, immutable retry and corruption fencing are covered; source-position trust authority, real provider credentials/availability/immutability, external Object Store/Oxia binding, fence-key history and guarded GC remain pending |
| Source assignment, typed Activation Barrier and Owner Lease | Implemented (local bounded mixed command/System Mutation replay seam; production authority pending) | `SourceAssignment`, `SourceReplayEntry`, `SourceReplayRecord`, `SourceReplayMutation`, `SourceReplayOutcome`, `SourceReplayCursor`, `SourceReplayTurn`, `ReplayTurnBudget`, `SourceReplaySuccessor`, `KafkaActivationBarrier`, `PulsarActivationBarrier`, barrier-gated `OwnedDelayShard`, `OwnerLease`, `OwnerLeaseContext`, `OwnerLeaseStore`, `InMemoryOwnerLeaseStore`, `PersistentOwnerLeaseStore`, `OxiaOwnerLeaseStore`, `OwnerDrainCoordinator`, `OwnerLeaseTest`, `PersistentOwnerLeaseStoreTest`, `OwnerDrainCoordinatorTest`, `SourceReplaySuccessorTest`, `OxiaOwnerLeaseStoreTest`, `SourceActivationBarrierTest`; catch-up now requires an explicit non-zero assignment identity/epoch bound to the typed barrier, assignment/barrier equality compares array-backed resource and guard identities by value, Kafka barrier cluster identity is canonical NFC/UTF-8 at construction, Pulsar barrier resource/guard identities reject all-zero placeholders, the runtime Pulsar barrier pins the inclusive entry `batchSize` and rejects same-entry batch-shape drift before apply, every catch-up cursor and post-activation apply record is checked against the typed physical source identity and rejects same offset/ledger-entry-batch tokens with conflicting canonical metadata before replay; the V1 overload pins an adapter-defined `SourceReplaySuccessor` for the entire catch-up window, accepts only exact canonical redelivery or the proven immediate successor, and the strict Kafka helper rejects offset gaps before applying the skipped record (Pulsar batch-member strictness is available while entry transitions remain adapter-defined); empty Kafka/Pulsar barriers are covered, and an empty Pulsar barrier validates any non-null persisted cursor before allowing activation; the unified bounded `replayTurn` seam and type-specific turn APIs cap record count, canonical frame/position bytes and elapsed monotonic time while preserving the caller cursor across turns, and apply mixed Command/System Mutation entries in one source order through the shard's atomic WriteBatch before advancing the cursor (the whole-`Iterable` methods and assignment-only overload remain explicitly compatibility conveniences); backing source iterator `RuntimeException`/`Error` during cursor look-ahead or advancement now fences the Owner before the failure escapes, retaining the source continuity proof for a fresh Store incarnation; Pulsar replay/catch-up/apply paths additionally require a positive guarded source-connection generation and exact resource-guard attestation digest; context-bound lease acquisition carries assignment identity plus assignment epoch and exact session identity, renewal preserves them and the lifecycle state, stale state transitions fail CAS, strict activation additionally requires the exact persisted `CompatibleControlSnapshotV1` before the authority CAS opens `ACTIVE_FOR_COMMANDS`, while legacy activation remains a compatibility seam; `PersistentOwnerLeaseStore` adds a checksummed, atomic, cross-restart local lease projection and retains consumed epoch history, but it is not a production owner authority; `OwnerDrainCoordinator` composes the locally provable drain order and leaves callback/source quiescence and production lease/session integration explicit; real Kafka/Pulsar consumer replay, Oxia session/ephemeral records, broker assignment/guard and production activation transaction remain pending |
| Queued vs applied client outcomes | Implemented (embedded core) | `DelayClient.enqueueBatch`, `EmbeddedDelayService.enqueueBatch`, `EmbeddedDelayService.queuedReceiptV1`, `appliedReceiptV1`, `enqueueOutcomeV1`, `awaitApplied`, `queryCommand`, `queryMessage`, `CommandQueuedReceipt`, `DelayShard.matchesCommandHash`, `DelayShard.matchesCommandPosition`, `EmbeddedDelayServiceTest`, `ProtocolCodecTest`; queued receipt stays distinct from applied result, its Command/Message/source identities must name the same Shard in both legacy and V1 paths, the embedded `awaitApplied` gate validates an exact durable-or-pending physical locator before draining and rereads POSITION after drain, and a rejected/foreign/forged receipt has no apply side effect; `enqueueBatch` returns one independent outcome per input command in input order, with no cross-command atomicity, so mixed queued and definitive local rejection results remain individually actionable; the closed managed enqueue union preserves queued/definitely-not-queued/uncertain states, applied frame is emitted only after the source barrier and retains the queued digest, query and applied-receipt barrier checks reject same physical offset/ledger-entry-batch tokens with conflicting canonical source metadata, same-command-id command-hash drift, or a mismatched POSITION audit, pending is source-barrier based, full/compact/evidence-expired branches are bounded, and message projections require caller-supplied safe policy inputs; real Broker response adapters and production routing remain pending |
| Destination Lane gate/readiness projection | Implemented (core plus closed same-key terminal branch and typed ActiveLaneStateV1/quota/certificate/barrier codecs) | `LaneRecord`, `LaneRecordEnvelopeV1`, `ActiveLaneStateV1`, `LaneCircuitStateV1`, `LaneRuntimeBlockReasonV1`, `LaneQuotaUsageEntryV1`, `LaneQuotaUsageMapV1`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `LaneRetirementProgressV1`, `LaneTerminalGuardV1`, `LaneRecordTest`, `LaneRecordEnvelopeV1Test`, `ActiveLaneStateV1Test`, `LaneQuotaUsageMapV1Test`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `LaneTerminalGuardV1Test`, `ApplyShardControlBody`, `ControlRef`, `DelayShard`, `DelayShardTest`; schedulable lanes maintain a versioned READY head and readiness/gate CAS transitions remove/recreate it atomically; source-ordered signed PAUSE/RESUME/BREAK/CLOSE applies exact Lane incarnation/control-version fencing, strict-lane acknowledgement checks and atomically revokes/restores reversible Claims; the same `meta_cf/LANE` key now persists a closed ACTIVE/TERMINAL branch, direct Lane reads fence the embedded Lane id, and close-cursor reads fence Lane id/incarnation/control version/source shard before exposing materialization work; the bounded local retirement path conservatively proves no current message/timeline/inflight work before atomically installing a tuple/profile/source/digest-checked terminal guard that survives reopen and rejects reuse; retirement progress and terminal-guard source checks reject equal order tokens with conflicting canonical metadata; runtime and control version increments fail closed at `Long.MAX_VALUE` (`LaneRecordTest.versionCountersFailClosedBeforeLongOverflow`), while the Registry-shaped ActiveLaneStateV1 codec now covers tuple identity/digest, gate/readiness block-reason rules, 17-dimensional lane charge, circuit/backoff counters, ready-key digest, canonical ReadyCertificateV1 wrapper and retirement progress, and the per-Lane quota entry/map codec enforces sorted identity keys and usage/map digests; `LaneRecordEnvelopeV1` now emits typed field-10 ACTIVE values as direct canonical `ActiveLaneStateV1`, rejects malformed typed values instead of downgrading them to legacy, and still reads the old adapter sub-message; `DelayShard` reads typed ACTIVE values during reopen/rebuild/scheduler projection, preserves immutable Profile/tuple/certificate/retirement fields and exact projected quota usage on same-key updates, and fails closed when the compatibility runtime cannot provide a required block reason, READY key/certificate or bounded numeric field (`DelayShardTest.typedActiveLaneStateIsReadAndUpdatedWithoutLegacyDowngrade`). Activation now also fences typed field-14 usage against the exact class-3 map entry and recomputes typed READY-key identity (`DelayShardTest.typedActiveLaneStateRequiresPersistedPerLaneQuotaProjection`, `DelayShardTest.typedActiveLaneStateRejectsUsageDriftFromPerLaneQuotaProjection`, `DelayShardTest.typedActiveLaneStateRejectsReadyKeyDriftBeforeActivation`). Legacy persistence remains on the compatibility adapter because current Schedule inputs do not carry the immutable Profile/tuple/certificate inputs required for a lossless cutover; external revision authority, Oxia target registration and Recovery-Floor/retention guard remain release blockers |
| Lane Close materialization cursor | Implemented (local source-marker overlay, bounded cursor discovery and strict GC-class materialization) | `LaneCloseMaterializationCursor`, `LaneCloseMaterializer`, `LaneCloseDiscoveryWorkClassExecutor`, `LaneCloseWorkClassExecutor`, `LaneCloseWorkClassExecutorTest`, `DelayShard`, `ShardQuota`, `LaneCloseMaterializationCursorTest`, `LaneCloseMaterializerTest`, `DelayShardTest.closeTransfersUnadmittedQuotaAndResumesBoundedMaterializationCursor`; Close marker transfers unadmitted message/reservation quota once in the marker WriteBatch and persists canonical `timeline/SYSTEM` kind-2 cursor state. The strict discovery action binds full record/actual-byte/elapsed bounds, rereads Owner Lease, charges both cursor and Lane projections and returns identity-checked candidates without advancing state; the separate strict materialization handoff binds exact cursor bytes and batch bounds, rejects admission without Store I/O, rereads Owner Lease and returns stale/not-found rather than applying an old cursor to a newer close state. Materialization freezes only generations with an empty admitted-obligation set as `DEAD_LETTER(LANE_CLOSED_BEFORE_ADMISSION)`, closes uncommitted reservations as `ABANDONED`, and resumes message then reservation scans after restart; the package-local multi-Lane helper makes no new semantic decision and uses checked aggregate counts. Closed-lane Cancel/Reschedule/Commit paths return stable frozen outcomes before cursor completion; PUBLISHING/UNCERTAIN obligations are retained and full close-owned Claim tagging, admitted-outcome retirement, object-handle/quiescence GC, Recovery-Floor protection and owner/Oxia orchestration remain release blockers |
| Destination Lane isolation and bounded weighted DRR | Implemented (scheduler core plus fenced READY recovery seam) | `LaneScheduler`, `PersistentLaneScheduler`, `ReadyIndexValue`, `TimelineEntry`, `LaneSchedulerTest`, `SchedulerProjectionsV1Test`; lane-local work is rebuilt from bounded `timeline_cf/READY` plus `meta_cf/LANE`/`id_cf/MESSAGE` identity checks, recomputes the exact timeline key and digest, verifies the timeline value, rejects a READY message whose self-routing key or embedded schedule Source Position belongs to another Shard, stale/orphan/multiple-head projections fail closed, and the recovered heads replace the active ring and queues before scheduling resumes; scheduler quantum/weight/cap multiplication is checked at configuration and lane registration, while runtime deficit accumulation saturates instead of wrapping; `LaneSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow`, `LaneSchedulerTest.saturatesRestoredDeficitBeforeServing` and `LaneSchedulerTest.fencedRecoveryRejectsReadyMessageFromAnotherShard` cover the local fence; production Lane certificate/activation authority remains pending |
| Persistent scheduler fairness counters | Implemented (local closed projection plus owner-bound recovery first pass and rotating READY cursor) | `PersistentLaneScheduler`, `LaneScheduler`, `WorkerScheduler`, `SchedulerProjectionsV1`, `OwnerIdentityV1`, `LaneSchedulerTest`, `WorkerSchedulerTest`, `SchedulerProjectionsV1Test`; all five `meta_cf/SCHEDULER` values are written in one batch, persisted successor order is restored without re-adding discarded lanes, the persisted `SchedulerRoundV1.owner` is compared with the current owner and an owner/store change restarts `recovery_first_pass`, and the first recovery rotation serves at most one item per eligible Lane until every currently discovered Lane has received an opportunity; the opportunity set is also bounded by the caller's global byte budget, so an oversized due head remains pending without blocking smaller healthy work (`LaneSchedulerTest.persistentRecoveryFirstPassDoesNotWaitForAnOversizedHead`, `WorkerSchedulerTest.oversizedHeadDoesNotHoldRecoveryFirstPassOpenForSmallerShard`); fenced READY rebuild performs a complete bounded scan from the READY namespace rather than consuming the rotating discovery cursor, while `discoverReady` promotes a bounded rotating slice into the active ring, validates exact Lane/Message/timeline identity, requires typed ACTIVE Lanes to be READY/OPEN with an exact encoded READY key and decodable `ReadyCertificateV1` matching the physical index, retains the physical lane-versioned READY key alongside each discovered work item, suppresses re-offering a polled head only while both work identity and READY key remain unchanged, rejects a projection that exceeds the visit byte cap without a first-entry exception, stops before decoding when the elapsed cap is already exhausted, and reads one extra inclusive cursor entry so a one-entry turn can wrap (`LaneSchedulerTest.fencedRecoveryUsesCompleteReadyPassDespitePersistedDiscoveryCursor`, `LaneSchedulerTest.rotatingReadyDiscoveryDoesNotReofferPolledHeadAndFindsSuccessorAfterWrap`, `LaneSchedulerTest.readyTransitionWithSameWorkUsesNewReadyKey`, `LaneSchedulerTest.readyDiscoveryRejectsFirstEntryThatExceedsByteBudget`, `LaneSchedulerTest.readyDiscoveryStopsBeforeFirstEntryWhenTimeBudgetIsElapsed`, `DelayShardTest.typedReadyProjectionRefreshesEarliestActionBoundaryFromCurrentHead`); failed scheduler projection writes now roll back polled/offered work, fenced-rebuild queue state, ring/cursor/fairness state, discovery heads and readiness before rethrowing (`LaneSchedulerTest.failedPollProjectionWriteRestoresThePolledHeadInMemory`, `LaneSchedulerTest.failedReadinessProjectionWriteRestoresThePreviousGateProjection`, `LaneSchedulerTest.queueSnapshotRestoresExactFifoProjection`); failed terminal unregister restores exact prior active-ring membership, including a Lane that was already outside the ring (`LaneSchedulerTest.failedPersistentUnregisterDoesNotReactivatePreviouslyInactiveLane`); Lane/Shard counter restore validates the complete registered subset and rejects duplicate identities before applying any entry (`LaneSchedulerTest.invalidLaterRestoreEntryDoesNotPartiallyApplyEarlierCounters`, `LaneSchedulerTest.duplicateLaneRestoreIdentityDoesNotPartiallyApplyEarlierCounters`, `WorkerSchedulerTest.duplicateWorkerRestoreIdentityDoesNotPartiallyApplyEarlierCounters`); persisted semantic generation validation also happens before active-ring replacement, and a malformed later projection restores the exact pre-restore ring/counters (`LaneSchedulerTest.malformedPersistedSchedulerGenerationDoesNotPartiallyApplyTheActiveRing`); deficit entries are additionally fenced by the current Lane incarnation and observed version, so a same-key Lane version change cannot inherit stale credits (`LaneSchedulerTest.stalePersistedDeficitVersionDoesNotRestoreCreditsToARevisedLane`); Lane incarnation/version and message-generation checks remain enforced; full Oxia-fenced activation and typed ActiveLaneState cutover remain release blockers |
| Worker Trusted UTC interval guard | Implemented (local deterministic guard; authority/wiring pending) | `TrustedUtcClock`, `TrustedUtcInterval`, `TrustedUtcClockTest`; injected monotonic projection, maximum uncertainty/sample age, wall/monotonic divergence fencing, stabilization window, conservative interval widening and strict due/pre-expiry predicates are covered; approved synchronization/signature source, Broker-time certification and production Worker/Admission wiring remain release blockers |
| Worker-to-shard-to-lane bounded DRR | Implemented (core snapshot plus READY-aware outer filtering, recovery first-pass, large-head service, fenced shard unregister and local placement scoring) | `WorkerScheduler`, `LaneScheduler`, `WorkerSchedulerTest`, `LaneSchedulerTest`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `WorkerPlacementPolicyTest`; outer and inner caps retain at least the current registered `weight * quantum` so weights above four are not silently clipped to a 4:1 long-run share, visits only shards with at least one schedulable Lane head, starts a new process/restore/READY recovery pass that serves each currently eligible shard at most once before repeating one, excludes due heads larger than the caller's global byte budget from the recovery opportunity set so they cannot block smaller healthy work (`WorkerSchedulerTest.oversizedHeadDoesNotHoldRecoveryFirstPassOpenForSmallerShard`), widens a shard visit to its smallest schedulable head when that head exceeds the outer deficit cap (still bounded by the caller's global byte budget), checks shard weight/quantum/cap arithmetic before registration and saturates runtime deficit accumulation, and the local placement seam hard-filters full committed capacity/DB slots before scoring projected `committed + required` dominant utilization plus unequal observed load; after ownership loss `WorkerScheduler.unregisterShard` requires a blocked shard with an empty local queue, removes it from the bounded outer ring, recomputes the cap from the remaining shard set and clamps retained deficits to that cap; `WorkerSchedulerTest.recoveryFirstPassServesEveryEligibleShardBeforeRepeatingOne`, `WorkerSchedulerTest.restoreStartsANewOuterFirstPass`, `WorkerSchedulerTest.outerDeficitCapDoesNotMakeLargeHeadUnserviceable`, `WorkerSchedulerTest.highWeightRetainsItsConfiguredOuterDeficitQuantum`, `WorkerSchedulerTest.shardUnregisterRequiresBlockedAndDrainedLocalQueue`, `WorkerSchedulerTest.unregisteringHighestWeightShardRecomputesOuterDeficitCap`, `LaneSchedulerTest.highWeightRetainsItsConfiguredDeficitQuantum`, `WorkerPlacementPolicyTest.projectedCommittedCapacityBreaksEqualTelemetryTie`, `WorkerSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` and `WorkerSchedulerTest.saturatesRestoredDeficitBeforeServing` cover the local fence; the outer ring is intentionally not durable across independent shard DBs, while broker assignors, Oxia desired-placement plans and authoritative placement weights remain pending |
| Closed Stable Code registry | Implemented | `StableCode`, `FailureStageV1`, `RetryabilityV1`, `StableErrorV1`, `ProtocolCodecTest`; all Registry stable codes including activated-protocol/capacity-fence codes, code-derived retryability, retry-at presence, rejection of `OK` in error projections, ENQUEUE-stage fencing for managed/native submission errors, mutually exclusive managed/native prepared refs, bounded diagnostic code and canonical round-trip/rejection checks |
| Non-persistence proof and Broker identity | Implemented (codec boundary) | `KafkaBrokerResourceIdentityV1`, `PulsarBrokerResourceIdentityV1`, `BrokerResourceIdentityV1`, `NonPersistenceProofKindV1`, `NonPersistenceProofV1`, `ProtocolCodecTest.brokerEvidenceAndQueuedAckIdentitiesRejectNonCanonicalUtf8AtConstruction`; closed Kafka/Pulsar identity branches, kind-specific attempt/resource/request/response presence, pre-ownership evidence prohibition, adapter-proof version and fields 1–7 digest, and strict direct-construction UTF-8 round-trip fencing for broker identities; authenticated adapter rejection classifiers and real non-persistence attestations remain pending |
| Managed/native submission outcome unions | Implemented (wire codec plus embedded/Kafka/Pulsar managed and native transport-SPI bridges) | `EnqueueOutcomeKindV1`, `DefinitelyNotQueuedV1`, `EnqueueUncertainV1`, `EnqueueOutcomeMessageV1`, `SubmissionOutcomeKindV1`, `NativeDefinitelyNotQueuedV1`, `NativeEnqueueUncertainV1`, `SubmissionOutcomeMessageV1`, `PreparedSubmissionV1`, `PreparedSubmissionAdapter`, `CommandQueuedReceiptV1.PreparedCommandRef`, `TargetPartitionHashV1`, `EmbeddedDelayService.enqueueOutcomeV1`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter.submit`, `WireIngressOutcomeSupport`, `ProtocolCodecTest`, `PreparedCommandV1Test`, `EmbeddedDelayServiceTest`, `AutoFastScheduleTest.hashOnlyNativeSelectionRecomputesTheSignedPartition`, `AdapterIngressTest`, `NativeSubmissionAdapterTest`; closed branch tags, exact prepared/ref hash binding, retryability and physical-attempt checks, canonical managed NDL1/native prepared branches, exact managed/native branch dispatch without reselection, V1 managed submission strict `encodeFrameV1/decodeFrameV1` validation before transport ownership, strict V1 frame digest derivation for `PreparedCommandRefV1`, compatibility-body rejection in both submission and receipt projections, embedded queued/definite/uncertain projection, Kafka/Pulsar queued ACK and authenticated definitive-rejection proof projection, native capability signature/expiry and pinned-resource checks before transport ownership, native `HASH_ONLY`/unlisted `EXPLICIT_OR_HASH` target-partition recomputation shared with Admission, native receipt/guard-proof/uncertain/local-definite projection, managed wrapper projection of null/throw/exceptional-stage/registration failures, and conservative downgrade when evidence is absent; durable guard/credential protection, authenticated Producer ownership and production response evidence remain pending |
| Hard shard quota admission | Implemented (core subset) | `ShardQuota`, `ShardQuotaTest`, `OutcomeReserveUsage`, `LaneQuotaUsageProjection`, `PublishAdmissionBody.ChargeVector`, `CapacityDimensionV1`, `CapacityVectorV1`, `CapacityGrantV1`, `QuotaGrantRefV1`, `ShardCapacityEnvelopeV1`, `DelayShardConfig`, `DelayShard`, `KeyCodecTest`, `DelayShardTest`, `CapacityVectorV1Test`, `ShardCapacityEnvelopeV1Test`; shard-local class-2 aggregate, per-Lane class-3 map and outcome reserve records/bytes are durable and source-ordered, with `ADMISSION_CAPACITY_GATED` Claim rollback, admission charge and definitive/verified settlement release in one WriteBatch; activation rebuilds and compares the canonical local aggregate, reads old class-1 `ShardQuota`/class-2 scalar values only for migration, and removes the stale class-1 projection on the next mutation; single message/reservation add/remove/commit entries reject negative bytes before applying checked arithmetic; an activation-supplied immutable envelope additionally binds the exact outcome grant and persists the full 66-dimensional outcome usage under registered `meta/CONTROL_RESERVE` keys, with restart/rotation checks; class-3/4/5/6 reserve/release now has checked grant-bounded local persistence, class 3/6 are dimension-disjoint under the shared `NON_OUTCOME_CONTROL` grant, and class-6 restart/invalid-dimension tests are covered; source-writer charge integration, Route Broker authority, multi-shard placement/Oxia authority and GC accounting remain pending |
| Kafka/Pulsar ingress and target adapters | In progress (identity-pinned transport SPI) | release blocker until concrete pinned transports, authenticated non-persistence classifiers/proofs, target publish/evidence channels, production response evidence and real-broker tests exist |
| Recovery Set/Floor, catalog and restore replay | In progress (typed and crash-durable local catalog/Floor subset; single-record Oxia catalog CAS backend) | release blocker; `OxiaSyncRecoveryCatalogBackend` now stores one shard's manifest/resource/scalar-Floor/typed-Floor snapshot with canonical decode and version CAS, and validates reusable local Store lineage/Floor/install projections against that snapshot. Oxia session pin, upload-intent transaction, immutable Object Store publication, source/evidence replay and activation CAS remain |
| Large payload, quota grants, control reserve and GC | In progress (reservation/commit, shard hard-quota, 66-dimensional vector/grant/envelope codec, canonical class-2 local aggregate plus per-Lane map, bound outcome-reserve usage, checked `meta/CONTROL_RESERVE` class-3/4/5/6 reserve arithmetic, disjoint system-writer projection, retire-intent, delete-confirmed, retired-Message identity branch and catalog-backed local compaction subsets) | release blocker; `CapacityVectorV1`/`CapacityGrantV1`/`QuotaGrantRefV1`/`ShardCapacityEnvelopeV1` enforce the closed dimension registry, zero-explicit ordered amounts, grant/envelope digests, logical charge projection, component-grant projection and checked arithmetic locally. The bound `DelayShard` path persists class-1 envelope identity and class-2 exact outcome usage under `meta/CONTROL_RESERVE`; `meta/QUOTA` class 2 now carries the canonical aggregate for locally accounted dimensions, class 3 carries the per-Lane map, and old class-1/class-2 scalar values are migration-only. It exposes synchronous grant-bounded reserve/release for `meta/CONTROL_RESERVE` classes 3–6, and enforces the class-3/class-6 dimension partition plus combined `NON_OUTCOME_CONTROL` grant bound; it scans those reserve classes during activation and rejects stale/unknown, over-capacity or cross-partition projections instead of ignoring them. Because the Registry has not frozen value schemas for `meta/QUOTA` classes 4 (`retained/object usage`) and 5 (`grandfathered transfer state`), `DelayShard` now rejects non-empty values for those classes during activation rather than treating them as empty; a Registry revision is still required before they can be persisted/restored. `ResourceRetireIntentBody`/`ResourceRetireIntentRecord` plus `ResourceDeleteConfirmedBody`/`ResourceDeleteConfirmedRecord` provide canonical source-ordered `gc_cf/TASK` intent/tombstone persistence with applied mutation sequence; `DelayShard.retireMessageIdentity` additionally removes bounded terminal/DLQ projections and retains a type-1 version-5 `RETIRED_IDENTITY` key branch, while `compactRetiredMessageIdentity` requires source-fence and Floor coverage before deleting it; direct GC reads fence resource kind/hash/version against the embedded retire intent, including nested delete-confirmation intents; `RecoveryCatalogAuthority`/`OxiaRecoveryCatalog` plus `ResourceGcGuard` enforce local ancestry/source/sequence coverage and fail closed when an active RecoveryPin protects a checkpoint resource or its pin state cannot be read, `DelayShard.compactResourceDeleteConfirmation` removes only a covered unpinned local tombstone, and local payload/checkpoint version/etag comparison is enforced, but Route Broker source-writer operation charging/authority, Object Store/Oxia publication, multi-shard grant placement/authority, real provider delete attestation/ownership, durable catalog/Floor barrier, Route identity-retention policy, Lane terminal guard and full guarded GC remain |
| Query, control operations, DLQ and observability | In progress (wire unions plus bounded local receipt/barrier/DLQ/SLO bridge) | `MessageQuerySnapshot`, `ReservationQuerySnapshot`, `DlqExportRecord`, `DlqExportResultBody`, source-ordered `DelayShard` DLQ export apply, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1/queryCommand/queryMessage/registerControlOperation/advanceControlOperation/queryControlOperation`, `ControlOperationQueryResponseV1`/`CurrentControlOperationV1`/`ControlTargetStateViewV1`/`ControlTypedResultV1`, `ControlOperationAuthority`, `InMemoryControlOperationAuthority`, `OxiaControlOperationAuthority`, `SloObjectiveV1`/`SloSampleEventIdentityV1`/`SloSampleStartV1`/`SloSampleFinalV1`/`SloObservationOutboxV1`/`SloObservationOutboxStore`/`SloObservationOutboxExportRate`/`SloObservationCollector`/`SloObservationCollectorLimits` and closed SLO enum/time codecs, all V1 Command/Message query view codecs, `DelayShardTest`, `DlqExportRecordTest`, `DlqExportApplyTest`, `ControlOperationQueryResponseV1Test`, `ControlOperationAuthorityTest`, `SloObjectiveV1Test`, `SloObservationOutboxV1Test`, `SloObservationOutboxStoreTest`, `SloObservationOutboxExportRateTest`, `SloObservationCollectorTest`, `ShardStoreTest`, `EmbeddedDelayServiceTest`, `ProtocolCodecTest`; Dead Letter terminalization writes the deterministic `terminal_cf/DLQ_EXPORT` `NOT_CONFIGURED` record atomically; configured local outboxes can now apply signed `DLQ_EXPORT_RESULT_V1` transitions with checked attempt succession, PENDING next-attempt advancement, terminal monotonicity and mutation dedupe, preserving body `stable_code` in the applied System Mutation result, but configured `DlqExportRecord` now persists the canonical policy-derived retained charge (legacy v1 records decode as zero), and apply requires callback transfer byte-equality with that projection; mismatches remain `REJECTED(STALE_SYSTEM_MUTATION)` without advancing the outbox state; the Control Operation query response union now has canonical CURRENT/error/target-marker/revision/typed-result wire validation, while the local authority and embedded entry points add receipt-bound idempotent registration, strict revision CAS and fixed retention-bound queries; a reclaimed Message identity is now projected as `IDENTITY_RETIRED` through the type-1 version-5 `id_cf/MESSAGE` branch and is covered by the focused EmbeddedDelayService regression; SLO objective digest, direction/unit/population/exclusion semantics, all 14 objective branch tags/common identity field-shape checks, Start threshold timeout, exact sample/start/final digests, start matching, exact due identity/path/start consistency, `meta_cf/SLO_OUTBOX` key/value-envelope persistence, key/value sample identity fencing, deterministic authority-provided Start reconciliation, conservative AT_MOST/AT_LEAST Final merge, bounded outbox capacity and process-local bounded export-rate accounting are locally covered; Real target/evidence adapter ownership, production receipt/barrier routing, authorization-safe binding/evidence/retention lookup, durable Oxia control-operation state/routing, Start reconstruction from Message/Admission authority, production collector merge/export and observability remain release blockers |
| Crash-durable local SLO collector merge projection | Implemented (embedded crash/replay seam; production authority pending) | `PersistentSloObservationCollector`, `PersistentSloObservationCollectorTest`; sorted canonical sample snapshots, `(sampleId,startDigest)` identity fence, direction-aware conservative Final merge, bounded sample/file state, checksum, atomic replacement, directory fsync and JVM/on-disk locking are covered; production rolling-window/late-finalization retention, authorization, ACK/export and metric publication remain release blockers |
The query/control matrix's local durable operation state is now covered by
`PersistentControlOperationAuthority`; the remaining durable-operation blocker
is the production Oxia routing/authorization/session boundary and source-ordered
marker authority, not the embedded file recovery seam.
| Real-service, chaos, benchmark, soak and upgrade evidence | Not started | release blocker |

The local DLQ bridge now retains a typed `RetryDecisionV1` alongside the raw
body bytes. When an exact source-position Retry Policy catalog and V1 schedule
binding are present, `DelayShard` recomputes the `DLQ_EXPORT` policy reference,
terminalization `firstExportAt`, checked deadline, physical-attempt budget,
possible-duplicate permission and deterministic next-retry jitter before the
outbox WriteBatch. Catalog-less and legacy bindings remain structural-only
compatibility seams; policy publication, target evidence and provider
ownership are still release blockers. `DlqExportApplyTest`
`catalogBackedDlqOutcomeRecomputesPinnedPolicyBeforePersisting` covers exact
ref rejection followed by a valid source-ordered outcome.

`SystemMutationResult.from` now decodes and binds its applied Source Position to
the signed mutation's Shard before constructing the durable result. A foreign
Source Position can no longer enter the System Mutation result projection
through the shared factory, while direct value decode still retains its
canonical-source fence. `DurableResultTest.systemMutationResultFactoryBindsSourcePositionToMutationShard`
covers the accepted and rejected paths. This is local source-identity evidence;
source assignment and production ingress authority remain release blockers.

After `f7d3f74`, `./gradlew clean check --rerun-tasks --console=plain` passed
on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The same five real-Oxia smoke
methods remain skipped because `NEREUS_DELAY_OXIA_ENDPOINT` is unset; this is
local build evidence only, not real-service or release evidence.

`PublishAttemptLedger` now decodes its Source Position before construction and
requires that position to belong to the `DelayMessageId` routing Shard. The
same fence applies to PUBLISHING, UNCERTAIN and Journal-mapping successors
because they share the constructor path. `PublishAttemptLedgerTest.sourcePositionMustBelongToAttemptMessageShard`
covers the foreign-shard rejection; source assignment and target authority
remain external release blockers.

After `7e7c971`, `./gradlew clean check --rerun-tasks --console=plain` passed
again on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The five real-Oxia smoke
methods remain skipped because `NEREUS_DELAY_OXIA_ENDPOINT` is unset; this
revalidates the local suite only.

`TerminalGenerationRecord`, `RetiredMessageIdentityRecord` and
`DlqExportRecord` now apply the same Source Position-to-Message Shard fence at
construction/decode time. Their source anchors are canonicalized only after
the identity check, so terminal history, retired-identity retention and DLQ
outbox projections cannot carry an external-shard position. Focused record
tests plus `DelayShardTest.terminalGenerationConstructionRejectsForeignSourcePosition`
cover the boundary; lookup and Store recovery remain local evidence while
external source authority is pending.

After `7b5c30f`, `./gradlew clean check --rerun-tasks --console=plain` passed
on 2026-08-12 (`BUILD SUCCESSFUL`, 5 tasks). The same five real-Oxia smoke
methods remain skipped because `NEREUS_DELAY_OXIA_ENDPOINT` is unset; this is
local projection/build evidence only.

`ResourceRetireIntentBody` 的 outcome-aware delete-evidence seam 已对 `PAYLOAD_OBJECT` 和 `CHECKPOINT` 闭合了精确身份证明：`DELETED` 必须携带 retire intent 中的 immutable version，并在 payload identity 存在 pinned etag 时同时携带该 etag；`ALREADY_ABSENT` 仍禁止任何 identity 字段。同一校验也在 `ResourceDeleteConfirmedRecord` 构造/解码时执行，因此损坏的本地 tombstone 不会绕过 GC guard。这个本地回归不取代真实 provider delete attestation、Oxia CAS、Floor barrier 或 external GC orchestration。

The shard-local SLO outbox now also has an explicit `SloObservationOutboxLimits`
envelope. The limit-aware store checks the strict key/value projection before
each new Start or Final replacement, accounts encoded `ValueEnvelope` bytes with
checked arithmetic, exposes bounded `Usage`, and refuses scans above the
configured record/byte budget; `SloObservationOutboxStoreTest.configuredCapacityBoundsRecordsAndEncodedBytesBeforeWrite`
covers the guard. A separate `SloObservationOutboxExportRate` applies a
process-local monotonic one-second record/encoded-byte budget to export scans;
rate denial leaves the durable record untouched and is covered by
`SloObservationOutboxStoreTest.exportRateBoundsEachScanWindowAndResetsAfterOneSecond`
and `SloObservationOutboxExportRateTest`. The legacy constructors remain
embedded compatibility seams; production wiring still must supply the required
§21 capacity and export-rate envelope, and evidence-gap classification, Start
reconstruction from Message/Admission authority, production collector
merge/export and metric publication remain release blockers.

The export-rate state now uses an explicit initialized bit rather than a clock
value sentinel, so `Long.MIN_VALUE` is a valid first monotonic timestamp and
cannot reset the window on the next call. `SloObservationOutboxExportRateTest`
`treatsLongMinimumAsARealWindowStart` covers the boundary while preserving the
same checked regression and per-window budget fences.

`SloObservationOutboxStore.reconcileDurableStarts(...)` now provides the local recovery
materialization seam for an authority-supplied exact Start set. It sorts by sample ID,
collapses byte-identical duplicate inputs, rejects conflicting Starts before mutation,
preserves any existing Final, preflights the configured record/byte envelope, and writes
all missing Starts in one synchronous batch. `SloObservationOutboxStoreTest` covers
deterministic ordering, idempotent retry with Final preservation, conflict rollback and
capacity rollback. This is not Message/Admission/Lane/Recovery authority or source-ordered
reconstruction: production activation must still obtain and authenticate that exact Start
set, and must record `BAD_EVIDENCE_GAP` rather than shrink the denominator when the
authority or evidence is unavailable.

`SloObservationOutboxStore.reconcileDurableStartsInBatch(...)` now exposes the same
preflight/materialization logic on a caller-owned `ShardStore.Batch`. It appends all missing
Starts only after conflict and configured record/byte checks pass, allowing a source-apply
caller to commit its Message/Admission/Source Position projection and SLO denominator in one
synchronous WriteBatch. The caller must provide the complete Start set once per batch because
RocksDB does not expose uncommitted reads to the projection, and the batch is identity-bound to
the same ShardStore; the rollback/foreign-batch tests
`SloObservationOutboxStoreTest.reconcileInCallerBatchSharesBusinessCommitAndRollsBackTogether`
and `reconcileInCallerBatchRejectsABatchFromAnotherShardStore` prove both joint commit/joint
abort and cross-store rejection. This is still a local atomicity seam, not production
source-order orchestration or Message/Admission authority.

`DelayShard` now wires that seam into the client-command source-ordered apply path when an
external immutable `COMMAND_APPLIED_LATENCY` objective is supplied, with an optional
`SloObservationOutboxLimits` envelope. Normal results, stable rejections, Command-ID
conflicts, position-only dedupe and retry-window fences all materialize the typed Start in
the same business `ShardStore.Batch`; replay of an already committed source position only
performs a byte-identical idempotent repair. Capacity preflight fails before RocksDB
`db.write`, so a rejected Start cannot leave a partial Message/result/position projection.
`DelayShardTest.commandAppliedStartsShareClientCommandBatchesAndReplayIsIdempotent` and
`DelayShardTest.commandAppliedOutboxCapacityAbortsTheBusinessBatch` cover the joint commit,
rejection, replay and rollback paths. Objective/catalog authentication, due-admission Start
materialization from Message/Admission authority, and production collector/export remain
release blockers; constructors without an objective remain compatibility seams.

The replay repair is now explicitly objective-gated: a shard configured only with
`DUE_ADMISSION_LAG` does not call the command-applied Start factory with a null objective
when an already committed client Command is replayed. `DelayShardTest`
`commandReplayWithOnlyDueAdmissionObjectiveDoesNotMaterializeCommandAppliedStart` covers
that configuration boundary; it keeps the due-admission outbox empty and the replay result
idempotent. This is local configuration/replay evidence, not production SLO authority.

The source-ordered `PUBLISH_ADMISSION_V1` seam now also accepts an immutable
`DUE_ADMISSION_LAG` objective restricted to `ALL_ACCEPTED`. After the typed Admission
descriptor and local Profile/timing/shard-state checks establish the message identity,
generation, managed path and `deliverAt/actionAt`, `DelayShard` calls
`SloAuthoritativeStartFactory.dueAdmission(...)` and appends the Start to the same business
batch for both successful Admission and `ADMISSION_CAPACITY_GATED`. The descriptor digest is
only local semantic evidence; it is not a substitute for Schedule/eligibility authority.
`SloStartMaterializationException` is kept outside the stale-result compatibility catch, so
SLO capacity or integrity failure aborts the source turn instead of advancing a stale System
Mutation result. `DelayShardTest.sourceOrderedPublishAdmissionPersistsAttemptAndMutationResultTogether`
and `DelayShardTest.dueAdmissionSloCapacityFailureDoesNotBecomeStaleMutation` cover the
positive, replay-idempotent and joint-abort paths. Immediate accepted-due reconstruction,
HEALTHY/full-interval proof, production Profile/Oxia/Broker authority and collector/export
remain release blockers.

`SloAuthoritativeStartFactory` now provides the typed local reconstruction projection for the
two Shard-derived branches. `commandApplied(...)` uses the Registry `SourcePositionV1`
canonical bytes, the Source Position Broker-persistence timestamp and its exact SHA-256;
`dueAdmission(...)` requires the caller to provide the Delay Message ID, complete unsigned
32-bit generation, managed path, exact ordinary `deliverAt`/handoff `actionAt`, and semantic
evidence digest. Both branches recompute the sample identity and checked timeout, and the
outbox exposes matching `ensureCommandAppliedStart(...)` and
`ensureDueAdmissionStart(...)` convenience methods. `SloAuthoritativeStartFactoryTest` and
`SloObservationOutboxStoreTest.typedAuthorityConvenienceUsesExactFactoryBranches` cover
canonical round trips, high-bit generation preservation, current-Shard identity/path/time/
evidence fencing and idempotent materialization. These are typed local seams only: they do not discover or
authenticate Message/Admission authority, and they do not close the production source-order
recovery or evidence-gap gates.

`SloObservationCollector` now provides the local deterministic collector merge
projection: it keys by canonical sample ID, rejects a different Start for that
ID, delegates repeated Final observations to the direction-aware conservative
merge, and optionally enforces a sample/canonical-byte projection envelope.
`SloObservationCollectorTest` covers bad-evidence monotonicity, Start drift,
deterministic snapshot order and capacity rejection. This is not yet the
durable/authorized production collector or metric publisher. The new
`PersistentSloObservationCollector(Path)` stores the same sorted canonical
sample projection behind a bounded checksum/atomic-rename/directory-fsync file
and JVM/on-disk lock; reopen, cross-instance latest-merge visibility, identity
failure rollback, conservative Final replay, configured capacity rejection and
checksum corruption are covered by `PersistentSloObservationCollectorTest`.
It remains an embedded crash/replay seam, not the production collector's
rolling-window retention, authorization, ACK/export or metric authority.

The pinned Kafka/Pulsar ingress adapters also catch a transport
`CompletionStage` whose callback registration itself throws; after Producer
ownership that path is conservatively projected as `ENQUEUE_UNCERTAIN` for both
the managed and NDR1 wire APIs. `AdapterIngressTest.kafkaCompletionStageRegistrationFailureIsUncertain`
and `AdapterIngressTest.pulsarCompletionStageRegistrationFailureIsUncertain`
cover this adapter-boundary failure. This remains transport-SPI evidence, not
an authenticated Broker response classifier. The managed callbacks also catch
runtime failures while projecting a returned `PERSISTED` result, so a malformed
result cannot escape as an exceptional Future or become a definitive rejection;
it is projected as `ENQUEUE_UNCERTAIN`, matching the existing wire-projection
downgrade for a malformed result.

The pinned Kafka/Pulsar destination adapters now apply the same fail-closed rule
to callback-registration failure: if `CompletionStage.handle(...)` itself throws
after the target Producer may have taken ownership, the adapter returns
`UNKNOWN / DESTINATION_OUTCOME_UNKNOWN` rather than an exceptional Future or a
definitive non-publication result. Synchronous transport exceptions and null
stages take the same conservative path. A standard `toCompletableFuture()` view
is used as a second observation path; if both registrations fail, the completed
logical UNKNOWN carries an internal “physical completion unobserved” marker so
`BoundedDestinationPublishAdapter` retains the reservation as zombie/in-flight.
`DestinationAdapterTest.kafkaCallbackRegistrationFailureRemainsUnknown` and
`DestinationAdapterTest.pulsarCallbackRegistrationFailureRemainsUnknown` cover
the result branch, while
`BoundedDestinationPublishAdapterTest.pinnedAdapterRegistrationFailureRetainsPhysicalCharge`
covers the composed physical-charge fence. This is local transport-SPI evidence
only; it does not establish a Broker-side absence proof or durable destination
evidence.

The local adapter close gate now fences new work on the first close request but
does not turn a failed native teardown into a permanent no-op. `CloseGuard`
keeps the adapter logically closed while allowing a later lifecycle call to
retry the underlying Kafka/Pulsar channel or Producer close; completion becomes
terminal only after that operation succeeds. The same guard is used by the
bounded physical-admission wrapper, which still shuts down its owned executor
and aggregates delegate/teardown failures. `CloseGuardTest` and
`DestinationAdapterTest.destinationCloseFailureCanBeRetriedWhileAdapterRemainsFenced`
cover the retry/fence boundary. This is local teardown evidence only: it does
not prove Broker-side cancellation, physical-charge release, or production
channel quiescence.

`CloseGuard.invokeIfOpen` now increments an accepted-invocation count while
holding the same monitor used by `close()`, then releases the monitor before
running potentially blocking transport code. This removes the check-then-call
window: an invocation accepted before the close fence may finish afterward, but
no new ingress/submission/publish transport call can begin after the fence.
`CloseGuardTest.acceptedInvocationDoesNotLetCloseAdmitASecondTransportCall`
covers the accepted-before-close lifecycle; the implementation still does not
claim production Broker teardown or physical-charge quiescence.

The same gate now linearizes acceptance of each synchronous ingress, submission
or publish transport invocation with the close request. The adapters no longer
perform a standalone `isClosed()` check followed by a transport call outside
that boundary: an invocation accepted before close may finish after close and
remains subject to the UNKNOWN/physical-charge rules, while no new transport
call can begin after the close linearization point. The gate itself is not held
while transport code runs, so a blocking call cannot prevent close from fencing
future work or retrying teardown; the bounded wrapper continues to dispatch
such calls on its Lane/Adapter executor.

`PreparedSubmissionAdapter` now preserves the same boundary at the managed
submission wrapper: a null stage, adapter throw, asynchronous exceptional
completion, or `CompletionStage.handle` callback-registration failure is
projected as managed `ENQUEUE_UNCERTAIN` with the original Prepared Command and
physical attempt id. The wrapper must not leak an exceptional Future or
silently switch to the native branch after managed transport ownership may have
begun. `NativeSubmissionAdapterTest`
`preparedSubmissionWrapperRegistrationFailureRemainsManagedUncertain` and
`preparedSubmissionWrapperExceptionalStageRemainsManagedUncertain` cover the
registration and asynchronous completion cases. If the physical attempt id is invalid while
that wrapper is failing, the result instead remains a local definitive
`INVALID_PREPARED_COMMAND` rejection with no attempt/proof of Producer
ownership; `preparedSubmissionWrapperInvalidAttemptRemainsLocalDefinite` covers
that precedence. The wrapper now owns a close fence as well: a managed
submission after `close()` is converted locally to `CLIENT_CLOSED` without
invoking the injected managed transport, while native submissions remain on
the native branch and use its pinned close gate; subsequent close calls can
retry either teardown. `NativeSubmissionAdapterTest.preparedSubmissionAdapterFencesManagedSubmissionAfterClose`
covers this branch/lifecycle boundary. This remains local transport-SPI
evidence only.

The embedded managed-outcome bridge applies the same physical-attempt rule to
its queued and uncertain projections: a null, wrong-length, or all-zero attempt
is returned as local `DEFINITELY_NOT_QUEUED(INVALID_PREPARED_COMMAND)` rather
than leaking a constructor exception or emitting an uncertain union without a
valid attempt identity. If a queued receipt's query boundary or ACK projection
is malformed after local admission, the bridge retains that attempt and returns
`ENQUEUE_UNCERTAIN` with a bounded `INTEGRITY_ERROR` diagnostic instead of
claiming non-persistence. `EmbeddedDelayServiceTest.embeddedIngressProjectsAllManagedOutcomeBranches`
covers queued/uncertain, invalid-attempt and malformed-boundary projections; the
embedded service remains a conformance seam, not a real Broker adapter.

The local physical-admission lifecycle now has an explicit Lane teardown
boundary: after the channel/Producer generation is fenced, READY is closed and
all physical/zombie reservations have quiesced, the caller may unregister with
the exact Lane incarnation. READY registrations, residual charges and stale
incarnations fail closed, so a late teardown callback cannot remove a newer
registration. This only reclaims process-local, rebuildable registry state; it
does not claim source-ordered retirement, Oxia grant release, terminal-guard,
Recovery-Floor or production channel-teardown authority.

The shard-local scheduler now has the matching terminal lifecycle: an exact
incarnation, terminal gate and empty queue are required before unregistering a
Lane; `PersistentLaneScheduler` removes the active ring, deficit, last-served
and discovery entries in the same projection WriteBatch, and restores the
in-memory registration if that write fails. `LaneSchedulerTest` covers the
terminal/identity/queue fence and persistent projection removal after reopen.
This bounds the rebuildable scheduler index; it does not replace the durable
terminal guard or external retirement authority.

The scheduler boundary now carries an explicit trusted due-through time through
`LaneScheduler`, `PersistentLaneScheduler` and `WorkerScheduler`.  Inclusive
eligibility (`eligibleAtEpochMs <= dueThroughEpochMs`) is enforced before a
work item can be returned for Claim; a future READY projection may be retained
in the process-local queue, but its poll remains fenced.  The durable discovery
cursor advances only through eligible READY keys, so a future-only slice can be
rediscovered after restart.  `LaneSchedulerTest.duePollUsesAnInclusiveEligibilityBoundary`,
`LaneSchedulerTest.persistentReadyDiscoveryAndPollFenceFutureDeliverAt` and
`WorkerSchedulerTest.workerPollCarriesTheInclusiveDueThroughBoundaryToShardScheduler`
cover the local time-boundary and restart seam.  The no-time overloads remain
compatibility seams only; Trusted UTC production wiring, Broker-time evidence
and Owner/Oxia scheduling authority are still release blockers.

Recovery fairness now uses the same due-through boundary when building its
eligible Lane/Shard sets and when choosing the minimum head byte budget.  A
future-only Lane or Shard therefore cannot keep `recovery_first_pass` open and
starve newly due work elsewhere.  The regressions are
`LaneSchedulerTest.persistentRecoveryFirstPassIgnoresFutureLaneForDueFairness`
and `WorkerSchedulerTest.futureShardDoesNotHoldRecoveryFirstPassOpenForDueWork`;
this remains local scheduler evidence rather than production placement or
Trusted UTC authority.

Recovery fairness also filters the first-pass opportunity set by the caller's
global byte budget. A due head larger than the current budget is retained in
the pending projection but is not claimable or counted as a first-pass
opportunity, so it cannot hold the recovery pass open and block a smaller
healthy Lane/Shard. The regressions are
`LaneSchedulerTest.persistentRecoveryFirstPassDoesNotWaitForAnOversizedHead`
and `WorkerSchedulerTest.oversizedHeadDoesNotHoldRecoveryFirstPassOpenForSmallerShard`;
the next turn may retry the retained head with a larger global budget.

The embedded client receipt boundary now validates the legacy queued locator's
`commandId`, `delayMessageId`, and Source Position against one Shard, and pins
`awaitApplied` to the embedded Kafka source and exact durable-or-pending physical
locator before it drains any queued records. If a command is still pending and
has no POSITION audit, only an exact pending `(commandId, delayMessageId,
Source Position)` tuple is accepted; the audit is reread after drain before the
result is returned. The exact pending `DelayShard.apply` result is retained for
that tuple, so a position-level `COMMAND_ID_CONFLICT` or fence rejection cannot
be collapsed into the first logical result (or become `null`) by a commandId-only
lookup. `EmbeddedDelayServiceTest.awaitAppliedRejectsForeignSourceBeforeDraining`
and `EmbeddedDelayServiceTest.awaitAppliedRejectsSameShardReceiptWithWrongPhysicalPositionBeforeDraining`
prove that foreign or forged receipts have no pending-command side effect, while
`EmbeddedDelayServiceTest.awaitAppliedReturnsTheExactPendingPhysicalConflictResult`
and `EmbeddedDelayServiceTest.queuedReceiptRejectsMessageIdFromAnotherShard` cover
the exact pending result and legacy constructor identity fences. This is local
receipt/query evidence; it does not replace gateway authorization, cross-worker
routing, or production receipt retention authority.

The public query/result codec boundary now routes closed union tags, stable
codes and Client Command types through an explicit bounded runtime projection.
For the legacy `CommandResult` projection, a rejected result with no message
fields still uses `generation=-1` as the documented absence sentinel, while an
applied result with state-version/status fields may carry the real all-ones
uint32 generation; `CommandResult.hasGeneration()` keeps that distinction
explicit and `DurableResultTest.appliedCommandResultPreservesMaxUint32GenerationAndProjectsIt`
covers the boundary.
High-bit wire `uint32` values fail with `IllegalArgumentException` before enum
dispatch instead of leaking arithmetic narrowing; the local regression is
`ProtocolCodecTest.publicClosedUnionTagsRejectHighBitUint32AsInvalidInput`.
This keeps the full Registry wire domain distinct from the current signed
runtime compatibility projection.

`ShardStore.write` now treats a native WriteBatch failure, or a post-write
ingress-fence reread/decoding failure, as a storage boundary rather than a
semantic rejection. The Store enters local `WRITE_OUTCOME_UNCERTAIN`, rejects
all further reads/writes, skips the clean-close marker, and requires a fresh
reopen from the durable incarnation; source replay therefore retains the
physical record and cannot advance an in-memory projection after an
unverifiable commit. `ShardStoreTest.nativeWriteFailureHasATypeDistinctFromSemanticStaleness`
and `ShardStoreTest.postWriteVerificationFailureFencesStoreUntilReopen` cover
the pre-write and committed-but-unverifiable branches. This is local
RocksDB/source-cursor evidence only; it does not claim production consumer
restart orchestration.

`OwnedDelayShard` now maps the typed `RocksDbWriteFailure` from command,
System Mutation, mixed replay and Claim-recovery batches to local
`FENCED` before rethrowing; the caller-owned source cursor remains on the
physical record. This closes the owner command gate at the same local failure
boundary, while the surrounding worker still must close the failed Store,
retain the lease/recovery evidence safely, and reopen a fresh incarnation
before replay.

`OwnerDrainCoordinator` now has a separate emergency teardown branch for that
local `FENCED` + `WRITE_OUTCOME_UNCERTAIN` combination. It stops source and
scheduling once, closes the exact Store before any lease release, and releases
only an Oxia lease whose full fencing identity still matches the captured
Owner. Native close failure and unconfirmed release remain retryable without
re-running Claim/callback/flush/checkpoint decisions; a replacement lease is
never released. `OwnerDrainCoordinatorTest.uncertainStoreClosesAndReleasesOnlyTheMatchingOwnerLease`,
`OwnerDrainCoordinatorTest.uncertainStoreNeverReleasesAReplacementOwnerLease`
and `OwnerDrainCoordinatorTest.uncertainStoreCloseFailureRetainsAReproducibleTeardownRetry`
cover the matching, replacement-identity and close-retry paths. This remains
local orchestration evidence; production source stop, Oxia session fencing and
worker restart coordination are still release blockers.

The typed `ActiveLaneStateV1` constructor now applies the Registry READY-key
projection itself: when field 22 is present it must be the exact
`timeline/READY` key derived from the Lane ID, runtime Lane version and field
16 `nextEligibleAt`; arbitrary non-empty bytes are rejected before the state
can be wrapped in `meta_cf/LANE`. `ActiveLaneStateV1Test.readyKeyMustBeTheExactLaneVersionAndEligibilityProjection`
and `DelayShardTest.typedActiveLaneStateRejectsReadyKeyDriftAtConstruction`
cover the direct codec boundary, while shard and persistent-scheduler
discovery continue to revalidate the same bytes against the physical READY
index and current Timeline head. This closes a typed-value integrity gap only;
it does not replace source-ordered readiness authority or real adapter evidence.

`LaneRecord.withReadiness` now encodes the Registry runtime-readiness graph:
`RECOVERING_EVIDENCE` may enter `READY` or `BLOCKED`, `READY` may enter
`BLOCKED` or `RECOVERING_EVIDENCE`, and `BLOCKED` must first return to
`RECOVERING_EVIDENCE` before it can become `READY`. A direct `BLOCKED -> READY`
transition would bypass evidence/capability reacquisition and is rejected;
repeating the same readiness value is an idempotent local no-op. The focused
regression is `LaneRecordTest.runtimeReadinessMustPassThroughRecoveryBeforeBecomingReadyAgain`.
The legacy `LaneRecord` constructor now applies the same cross-axis fence as the
typed `ActiveLaneStateV1` constructor: a direct projection cannot persist
`runtimeReadiness=READY` behind a non-`OPEN` admission gate. The direct-construction
regression is `LaneRecordTest.directProjectionCannotPersistReadyLaneBehindAClosedAdmissionGate`.
This is a local lifecycle fence and does not prove the external activator's
evidence or Owner/Oxia readiness authority.

The process-local `LaneScheduler` readiness projection now applies the same
evidence-recovery fence as the authoritative `LaneRecord`: `markReady` is
idempotent for READY, accepts only `RECOVERING_EVIDENCE -> READY`, and rejects
`BLOCKED -> READY`; `markRecoveringEvidence` is the explicit recovery step.
`PersistentLaneScheduler` exposes and persists that step, and its rollback now
captures the full readiness enum rather than only the schedulable boolean, so a
failed projection write cannot silently turn a recovery state into BLOCKED or
READY; blocked/recovering Lanes also leave the active ring until READY
reactivation. `LaneSchedulerTest.blockedLaneMustRecoverEvidenceBeforeBecomingReady`
and `LaneSchedulerTest.nonReadyRegistrationStaysOutsideActiveRingUntilRecoveryCompletes`
plus `LaneSchedulerTest.failedReadyProjectionRestoresEvidenceRecoveryStateExactly`
cover the local scheduler and rollback boundaries. External activator evidence
and Owner/Oxia readiness authority remain release gates.

The Worker outer scheduler now removes a blocked Shard from its active ring and
re-adds it only on `markShardReady`; restore also rebuilds the ring without
blocked Shards. This makes the isolation claim hold under a small outer visit
budget instead of merely skipping a blocked entry after spending a visit on it.
`WorkerSchedulerTest.blockedShardLeavesOuterRingBeforeAOneVisitBudgetCanStarveHealthyWork`
covers the fairness boundary. Placement/ownership authority remains external.

`WorkerScheduler.poll` now treats the complete bounded outer turn as one local
mutation boundary. Its pre-poll snapshot includes the outer ring/cursor,
Shard fairness counters and recovery-first-pass set plus every registered
shard's inner scheduler counters, while the heads removed during this turn are
requeued in reverse order on failure. If a later local clock, selection or
checked-arithmetic failure occurs after an inner head was removed, the exact
two-level projection is restored before the exception is rethrown; the caller
never observes a partially returned result. The regression is
`WorkerSchedulerTest.clockFailureAfterAHeadWasSelectedRollsBackTheWholeWorkerPoll`.
`LaneScheduler.poll` itself applies the same lightweight rollback to its own
removed heads and inner counters before propagating an exception.
This is elapsed-time/process-state evidence only and does not provide Trusted
UTC, Owner or Oxia authority.

Shared Worker resource teardown now treats runtime-monitor shutdown as a
retryable close item rather than an escape hatch: if either monitor reports a
runtime failure, the shared RateLimiter, WriteBufferManager, block cache and
their native reservations are still all attempted, with the first failure
preserved and later failures suppressed. A Store open/restore invocation also
releases the acquire, owned-shard and DB slots when a post-acquisition `Error`
escapes the native-open/metadata path, so an unrecoverable JVM/native failure
cannot strand the Worker capacity envelope. This is teardown accounting only;
it does not turn a failed native process into a safe retry without a fresh
Store incarnation.

Checkpoint compensation now fails closed if restoring the prior runtime
metadata after a physical snapshot failure itself cannot be proven: the Store
enters `WRITE_OUTCOME_UNCERTAIN` and rejects subsequent operations until a
fresh incarnation reopens the durable projection. A failed checkpoint can no
longer leave an unverified `lastCheckpointId` projection available to later
source application.

Restore cleanup now treats both runtime/native exceptions and `Error` escapes
from a staged or installed Store close as unconfirmed teardown: it records the
failure, retries the exact close boundary once, and leaves the private
`restore-tmp`/unpublished incarnation in place unless every handle and slot is
known closed. The original restore failure remains the surfaced cause, so an
unrecoverable JVM/native path cannot turn into an unsafe directory deletion.

The Store teardown boundary now applies that same rule to every native close
and capacity transition: default and named Column Family handles, RocksDB,
options, physical-usage registration, DB slots, owned-shard slots and
acquisition slots are attempted independently, and a `RuntimeException` or
`Error` is retained for a later retry without releasing an unfinished native
capacity. Shared Worker resources likewise continue monitor, RateLimiter,
WriteBufferManager, block-cache and native-reservation teardown after an
`Error`, and only mark the Worker closed after all required pieces succeed.
The restore entrypoint routes post-download-slot `Error` failures through its
existing conservative cleanup, while a failed open releases every slot it
actually acquired and preserves secondary cleanup failures as suppressed
diagnostics. This is local fail-closed lifecycle evidence; it does not replace
process supervision or fresh-incarnation recovery after a fatal JVM condition.

The same typed failure aggregation now reaches the embedded facade and local
adapter wrappers: `EmbeddedDelayService` still attempts the Store and shared
Worker close after a submission-adapter `Error`, while
`PreparedSubmissionAdapter` and `BoundedDestinationPublishAdapter` attempt
both delegate/native teardown and owned-executor shutdown before rethrowing
the first failure. Their `CloseGuard` therefore remains retryable even when a
JVM/native `Error` interrupts one teardown item. This is still local lifecycle
evidence; it does not attest Broker-side producer quiescence or recovery from
a process-fatal condition.

`CheckpointControlSnapshotVerifier` now applies the same restore-side cleanup
rule to its read-only RocksDB probe: every opened Column Family handle and
Options object is attempted independently for both runtime failures and
`Error`, with the first failure rethrown after all cleanup diagnostics are
retained. This keeps checkpoint identity validation from leaking a temporary
native handle before the real Store restore path begins.

`OwnerDrainCoordinator` now catches both runtime teardown failures and
`Error` in its normal and retry-only Store-close branches. The coordinator
still leaves the shard in `DRAINING`, keeps the exact Oxia lease unreleased
until Store closure is confirmed, and exposes the original failure so the same
coordinator can retry native/slot teardown; no Claim revoke, callback poll,
flush or final checkpoint is repeated on the retry branch.

`ShardStore.openAtPath` now treats a post-open short-lived acquire-slot
release failure as a Store teardown path: it retries closing that exact Store
before any outer slot cleanup. DB and owned-shard capacity is released only by
a confirmed Store close; if native teardown remains uncertain, those outer
cleanup flags are cleared deliberately so capacity cannot be released beneath
a live handle. This closes the open-failure ownership boundary locally; a
process restart is still required to recover from an unrecoverable native
teardown.

`ShardStore.createCheckpoint` now routes `Error` as well as ordinary snapshot
and metadata failures through the compensating `lastCheckpointId` restore. A
failed compensation marks the live Store `WRITE_OUTCOME_UNCERTAIN`; temporary
directory deletion and checkpoint-create-slot release are attempted afterward,
with cleanup failures suppressed onto the original error. The checkpoint path
therefore cannot leave a live Store advertising an unproven checkpoint merely
because the primary failure was a JVM/native `Error`.

`CheckpointUploadCoordinator` now records the primary adapter/intent failure
before releasing its Worker upload slot. A release failure is suppressed onto
that primary failure, while a release failure after an otherwise successful
upload is surfaced explicitly instead of returning a result with an
unconfirmed Worker-capacity transition.

The restore download-slot boundary now follows the same primary/suppressed
policy. A slot-release failure after a successful install first closes the
Store that would otherwise be returned, then surfaces the release failure;
the already-published ACTIVE incarnation is preserved for a later reopen. If
restore or cleanup already failed, the release failure is retained as
suppressed diagnostic evidence instead of masking the original failure. This
is local resource-lifecycle evidence only; it does not replace process
supervision or external checkpoint/download authority.

`OwnerDrainCoordinator` now finalizes its local boundaries independently:
successful Store/lease completion attempts the local fence, shard drain-attempt
release and Worker drain-slot release even when an earlier finalizer throws.
The first drain/lease failure remains primary and later fence/slot failures are
suppressed; when the drain body succeeds, a finalizer failure is surfaced
instead of returning a result with unconfirmed capacity. The owner remains
retryable whenever teardown or lease release is not proven. This is local
drain-lifecycle evidence only; Oxia CAS, source quiescence and callback
authority remain external release gates.

Crash-durable local projections now preserve primary write failures when their
temporary-file cleanup fails. Recovery Catalog, SLO collector, Checkpoint
Upload Intent and Control Operation writes record the body/rename/fsync failure
before attempting temporary deletion, suppress cleanup diagnostics onto that
primary failure, and surface cleanup failure directly only when the write body
otherwise succeeded. This keeps atomic replacement and retry evidence
diagnosable; it remains local filesystem evidence and does not replace Oxia or
provider durability authority.

`WorkerRuntimeResourceMonitor` and `WorkerRocksDbUsageMonitor` now treat
`Error` from a probe as missing resource evidence, record it, and fence the
sticky Worker safety gate just like a runtime probe failure. A scheduled task
therefore cannot silently die while the Worker remains admissible; secondary
failure from the fence callback is retained on the primary probe error. This
is fail-closed monitor evidence only, and process supervision/fresh restart
remain required for process-fatal JVM/native conditions.

The crash-durable Recovery Catalog and SLO collector now also roll their
in-memory delegate back to the pre-mutation snapshot when an `Error` escapes
the mutation or persistence path. Ordinary I/O is still wrapped as the typed
state-file failure, while runtime and JVM/native failures retain their
original type after rollback. This prevents a surviving local process from
serving a projection that was never durably published; process restart remains
the recovery boundary for unrecoverable native failures.

The strict first-seen ingress identity seam now closes the UUID/Broker timing
boundary from the V1 design when a Route policy is supplied to
`DelayShardConfig`: `retryUntil` must equal the Command UUIDv7 timestamp plus
the configured command retry window, and both the first-seen `commandId` and a
new Schedule's `delayMessageId` must fall within the checked
`[brokerPersistedAt - maximumPreparationAge, brokerPersistedAt +
maximumUuidFutureSkew]` interval. A drifted deadline or out-of-window identity
is persisted as `INVALID_COMMAND` before any business mutation; an existing
dedupe identity continues to use its original conflict/no-op rules. The
configured preparation window also drives `messageIdentityReuseUntil`. Legacy
embedded constructors leave these three Route-owned bounds at zero because
they have no authenticated Route policy snapshot; production activation must
use the strict fields rather than treating that compatibility seam as ingress
authority. `DelayShardTest.strictFirstSeenIdentityTimingBindsRetryDeadlineAndUuidAge`
and `DelayShardConfigTest.strictIdentityPolicyRequiresCommandAndPreparationWindowsTogether`
cover the local boundary. Route publication, authoritative Broker timestamp,
and production ingress wiring remain release blockers.

`PersistentLaneScheduler` now treats an `Error` during restore, READY discovery,
poll, readiness transition or terminal unregister the same as a runtime
projection failure: it restores the exact scheduler snapshot, queue/ring,
discovery cursor, fairness counters and readiness projection before rethrowing
the original failure, while rollback diagnostics are suppressed onto it. This
keeps a surviving process from serving an in-memory Lane schedule that was
never durably written after a JVM/native failure. The focused scheduler suite
continues to pass; process supervision and a fresh Store incarnation remain
the recovery boundary for unrecoverable native failures.

`ShardStore.write` now fences the Store when the native `db.write` boundary or
the subsequent WriteBatch/WriteOptions teardown throws a runtime/JNI `Error`,
not only when RocksDB returns its checked native failure. A post-write
ingress-fence reread/decoding `Error` is likewise marked
`WRITE_OUTCOME_UNCERTAIN`; the original failure is rethrown and no later
source record may use the in-memory projection before a fresh Store reopen.
`OwnedDelayShard` maps the same fatal boundary to local `FENCED` across direct
apply, Command/System Mutation/mixed replay and Claim-recovery activation, and
also fences when the Oxia activation or drain transition throws an `Error`.
`OwnerLeaseTest.activationFatalAuthorityFailureFencesTheLocalOwnerGate` and
`OwnerLeaseTest.drainFatalAuthorityFailureFencesTheLocalOwnerGate` cover the
authority-side fatal fence. This is local fail-closed evidence only; process
supervision, native recovery and production Oxia/source orchestration remain
release blockers.

`TimelineWorkRef` now binds a `CONTROL_OVERRIDE` retry's canonical Source
Position to the self-routing `DelayMessageId` embedded in its DUE/ORDERED
timeline key. A cross-Shard control source is rejected before the runtime
projection can be persisted or decoded;
`GenerationRuntimeIndexTest.controlOverrideTimelineRejectsSourcePositionFromAnotherShard`
covers the new fence. This is local timeline/source identity evidence only;
authenticated control authority and production source assignment remain
release blockers.

`ResourceDeleteConfirmedRecord` now binds its confirmation Source Position to
the nested retire intent's exact Shard Log identity: both the Shard and the
authenticated Kafka topic or Pulsar resource identity must match before a
delete-confirmation tombstone can be constructed or decoded, and the
confirmation position must be strictly later than the retire-intent position.
This prevents a foreign-shard, replacement-source or source-reordered
confirmation from being accepted when the record is handled outside the
`DelayShard` apply path; `ResourceGcGuardTest.deleteConfirmationSourcePositionMustMatchRetireIntentSource`
and `ResourceGcGuardTest.deleteConfirmationSourcePositionMustFollowRetireIntent`
cover the fences. The check is local tombstone integrity only;
provider delete attestation, Oxia CAS and external GC orchestration remain
release blockers.

`ResourceRetireIntentRecord` now performs the same shard-boundary check for
source-bearing `ProtectionSet` references: every Recovery-Floor or time-bound
minimum Source Position must belong to the retire record's applied Source
Position Shard before the canonical protection bytes are retained. This keeps
the durable value safe even when decoded outside the `DelayShard` apply helper;
`ResourceGcGuardTest.retireIntentRecordRejectsProtectionSourceFromAnotherShard`
covers the foreign-shard branch. Source-resource authority and external Floor
publication remain release blockers.

After `93147c4`, the full wrapper `./gradlew clean check --rerun-tasks
--console=plain` gate passed on 2026-08-12 (`BUILD SUCCESSFUL`, five executed
tasks). The same five real-Oxia methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset; this revalidates the local code and
does not claim real-service or release evidence.

Kafka `ReceiptJournal.AttemptIdentity` and Pulsar `AttemptJournal.AttemptIdentity`
now bind their canonical Source Position to the embedded `DelayMessageId` Shard
at construction time, before a transport mapping is created. This closes the
intermediate-value gap in which a foreign-shard attempt identity could exist
until `Mapping.create`; `KafkaReceiptJournalTest.attemptIdentityRequiresCanonicalSourcePositionAndMatchingShard`
and `PulsarAttemptJournalTest.attemptIdentityRequiresCanonicalSourcePositionAndMatchingShard`
cover the direct-construction rejection. The checks remain local adapter
identity evidence; authenticated broker assignment and real journal authority
remain release blockers.

`ClaimRecord` now parses the retained DUE/ORDERED timeline key and requires its
lane, `DelayMessageId` and generation tuple to equal the Claim value and its
precondition. Rebinding only `originalTimelineKeySha256` can no longer make a
Claim point at another Message; `ClaimRecordTest.claimRejectsTimelineKeyForAnotherMessageAfterPreconditionHashIsRebound`
covers this local value-integrity fence. Full Claim materialization/recovery
authority remains a release blocker.

`RecoveryPinV1` now binds its explicit `ShardSubjectV1` to the observed
`RecoveryFloorRefV1.appliedSourcePosition` Shard during value construction and
decode. A pin carrying a valid Floor from another Shard can no longer be
serialized as a signed-looking intermediate value before the Catalog authority
rejects it; `RecoveryPinV1Test.rejectsLineageGenerationAndDigestDrift` covers
the foreign-Shard constructor path alongside the existing lineage and digest
checks. This is local recovery-value integrity evidence only; Oxia session/CAS,
Floor publication and source replay remain release blockers.

`SloObservationOutboxStore` now applies the same Shard fence to the generic
`ensureStart/reconcile` paths and to direct `get/scan/usage` reads for the typed
`COMMAND_APPLIED` and `DUE_ADMISSION` Start branches. A valid Source Position or
self-routing `DelayMessageId` from another Shard cannot be smuggled through the
generic outbox API; legacy opaque synthetic Due fixtures remain an explicit
compatibility seam. `SloObservationOutboxStoreTest` covers generic admission,
reconciliation and read-side corruption. This is local SLO outbox integrity
only; production receipt/evidence authority and collector routing remain
release blockers.

`RecoveryCatalog` snapshot installation now rejects a non-empty catalog at
generation zero and a non-empty generation without any published manifest.
Active RecoveryPin restore also requires the current Floor to remain on the
same ancestry branch as the pinned candidate (while allowing the documented
historical pin to survive a later descendant Floor). `RecoveryCatalogTest`
covers both malformed-snapshot cases; this is crash-durable local projection
integrity, not the missing Oxia cross-record pin/Floor transaction.

`StoreRecoveryMetadata` now rejects a nonzero `meta/RECOVERY` catalog
generation when no `lastObservedFloor` is present. The generation is only a
projection of that exact typed Floor, so a dangling generation must fail closed
on construction/reopen rather than remain as apparently valid local metadata;
`StoreRecoveryMetadataTest.rejectsCatalogGenerationWithoutObservedFloor` and
`ShardStoreTest.danglingRecoveryCatalogGenerationDoesNotLeaveRocksDbOpen`
cover the constructor and native-reopen corruption boundaries. This is local
projection integrity only and does not provide the external Oxia Floor/Owner
Lease transaction.

After `ebb2bc5`, the full checked-in `./gradlew clean check --rerun-tasks
--console=plain` gate passed on 2026-08-12 (`BUILD SUCCESSFUL`, five executed
tasks). The five opt-in real-Oxia methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is local regression evidence only,
not real-service or release evidence.

The single-record Oxia Recovery Catalog now applies the same bounded manifest
count on both decode and encode. A locally constructed snapshot with more than
100,000 manifests fails closed before canonical bytes are produced;
`OxiaSyncRecoveryCatalogBackendTest.rejectsManifestCountAboveBoundBeforeEncodingSnapshot`
covers the boundary. This closes only the local catalog serialization bound;
the production upload-intent/catalog transaction and immutable Object Store
publication gates remain open.

After `72e31da`, the focused `OxiaSyncRecoveryCatalogBackendTest` run and the
full `./gradlew clean check --rerun-tasks --console=plain` gate both passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset.

The shared `LocalStatePathGuard.ensureRealDirectoryPath` now validates every
existing and newly-created path component from the filesystem root. An
intermediate symbolic link that redirects a state/lock/temp directory outside
its lexical parent is rejected even when the final target already exists;
deployment-managed system links such as macOS `/var` remain usable;
`LocalStatePathGuardTest.directoryPathRejectsExistingIntermediateSymlinkEvenWhenTargetExists`
covers that previously untested branch. This is local physical-path evidence
only; it does not replace deployment ownership, Oxia authority or provider
filesystem guarantees.

After this path-guard change, `./gradlew clean check --rerun-tasks
--console=plain` passed on 2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks).
The same five opt-in real-Oxia methods remained skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset.

`OxiaSyncOwnerLeaseBackend` now handles Oxia response loss at the actual
single-record CAS boundary. Acquire, renew and lifecycle transition reread the
same shard/owner/epoch/token/context candidate and accept success only when
the committed state and expiry are byte-equivalent; a stale value, replacement
owner or failed reread remains unknown. `OxiaSyncOwnerLeaseBackendTest` covers
exact committed acquire, renewal and transition response-loss paths. This
closes only the per-record Oxia lease CAS seam; assignment publication,
session lifecycle orchestration and the cross-record activation/pin
transaction remain release blockers.

The Recovery Catalog CAS response-loss path now also requires the reread to
carry the exact catalog key and a non-null Oxia version, in addition to
byte-equal snapshot contents. `OxiaSyncRecoveryCatalogBackendTest`
`responseLossWithWrongRereadRecordIdentityDoesNotBecomeSuccess` covers a
same-bytes/wrong-key response; the committed catalog remains durable but the
caller does not receive a false success.

Owner-lease release now applies the same response-loss rule: a delete is
confirmed only by rereading the exact lease key as absent; a still-present
same-identity lease or a failed reread remains unknown, while a replacement
owner is reported as not released. `OxiaSyncOwnerLeaseBackendTest`
`releaseResponseLossRereadsAbsenceAfterCommittedDelete` covers the committed
delete/response-loss path. This remains per-record Oxia evidence only.

`OxiaSyncRecoveryCatalogBackend.decodeCatalog` now rejects a remote catalog
record whose Oxia response has no version, before exposing the snapshot to
read or mutation callers; `OxiaSyncRecoveryCatalogBackendTest.catalogReadRejectsARecordWithoutAnOxiaVersion`
covers the malformed record. This keeps the single-record CAS projection
fail-closed and does not add the missing cross-record transaction.

The concrete Oxia owner-lease backend now validates the exact record identity
at every owner-epoch and lease read boundary. Epoch CAS reads/writes reject a
wrong key, null value or missing version, and lease reads reject the same
malformed response before decoding or using the ephemeral-session metadata.
`OxiaSyncOwnerLeaseBackendTest.epochReadRejectsARecordWithoutExactIdentityOrVersion`
and `leaseReadRejectsAResponseForAnotherRecordKey` cover the two failure
classes. This is a local single-record response-integrity fence; assignment
publication, session orchestration and cross-record activation/pin authority
remain release blockers.

After this owner-lease identity fence, the focused
`OxiaSyncOwnerLeaseBackendTest` run and the full
`./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this is local regression evidence only, not real-service or release
evidence.

The single-record Oxia Recovery Catalog now bounds manifest-resource entries
as well as manifests during both snapshot encoding and decoding. Encoding
also rejects duplicate resource identities or a resource whose checkpoint,
lineage or manifest hash does not match a published manifest;
`OxiaSyncRecoveryCatalogBackendTest.rejectsResourceCountAboveBoundBeforeEncodingSnapshot`
covers the oversized input. This closes only the local catalog snapshot
serialization boundary; Object Store publication and upload-intent/catalog
transaction gates remain open.

After this catalog resource-bound change, the focused
`OxiaSyncRecoveryCatalogBackendTest` run and the full
`./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this is local regression evidence only, not release evidence.

The concrete public `OxiaSyncRecoveryCatalogBackend` now validates the fixed
16-byte checkpoint identity before direct `manifest` and `proveFloorCoverage`
calls reach the Oxia read path. The outer `OxiaRecoveryCatalog` already had
this fence, but the backend itself is also a public `CasBackend` implementation
and must fail closed when used directly. The focused regressions are
`OxiaSyncRecoveryCatalogBackendTest.directManifestReadRejectsCheckpointIdentityWithWrongWidth`
and `directFloorCoverageReadRejectsCandidateIdentityWithWrongWidth`. This is a
local API-boundary check only; it does not add upload-intent transactions,
Object Store publication or external recovery authority.

After this direct-backend identity fence, the focused catalog test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this remains local regression evidence only.

Ephemeral Owner Lease acquire, renew and lifecycle-transition writes now also
require the exact lease record key, a non-null Oxia version and the expected
session metadata in their `PutResult`; a malformed write response is accepted
only if the subsequent reread proves the exact candidate. The focused
regression `OxiaSyncOwnerLeaseBackendTest.leaseWriteRejectsWrongResponseAndWrongRereadIdentity`
covers the fail-closed path and cleanup. This is a per-record response fence;
source assignment publication, session orchestration and cross-record
activation authority remain release blockers.

After this ephemeral-write identity fence, the focused owner-lease test and
the full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this remains local regression evidence only.

Owner Epoch creation and increment CAS now handle a lost Oxia write response
without inventing success: the backend rereads the exact epoch key and accepts
the operation only when the expected canonical eight-byte value and a valid
Oxia version are present. A missing, redirected or malformed reread preserves
the original failure. `OxiaSyncOwnerLeaseBackendTest`
`epochCreateResponseLossUsesOnlyAnExactCommittedReread` and
`epochUpdateResponseLossUsesOnlyAnExactCommittedReread` cover both CAS forms;
epoch gaps remain safe and never permit reuse. This improves availability at a
single-record boundary only; source assignment and cross-record activation
authority remain release blockers.

After this epoch response-loss fence, the focused owner-lease test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this remains local regression evidence only.

The Recovery Catalog snapshot encoder now rejects duplicate checkpoint
identities before emitting canonical bytes, matching the decoder's existing
duplicate-field fence. `OxiaSyncRecoveryCatalogBackendTest.rejectsDuplicateManifestIdentityBeforeEncodingSnapshot`
covers a locally constructed invalid snapshot. This is a local
serialization-integrity bound; it does not add the external upload-intent,
Object Store or catalog transaction authority.

After this duplicate-identity fence, the focused catalog test and the full
`./gradlew clean check --rerun-tasks --console=plain` gate passed on 2026-08-12
(`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia methods
remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this remains
local regression evidence only.

Recovery Catalog encoding now also requires each `manifestResources` map key
to equal the resource's canonical checkpoint identity. An alias key would
otherwise be silently discarded while producing bytes; the focused regression
is `OxiaSyncRecoveryCatalogBackendTest.rejectsResourceMapKeyThatDoesNotMatchCheckpointIdentityBeforeEncodingSnapshot`.
This remains a local snapshot-integrity fence and does not claim external
catalog or Object Store authority.

After this resource-map identity fence, the focused catalog test and the full
`./gradlew clean check --rerun-tasks --console=plain` gate passed on 2026-08-12
(`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia methods
remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this remains
local regression evidence only.

The Oxia single-record Recovery Catalog encoder now fails closed when a
snapshot contains an active `RecoveryPin`. The current backend cannot encode
or atomically persist the pin semantics, so silently dropping it would lose
recovery protection; `OxiaSyncRecoveryCatalogBackendTest`
`rejectsUnsupportedRecoveryPinBeforeEncodingSnapshot` verifies that the
encoder rejects the snapshot before emitting bytes. This preserves the
unsupported RecoveryPin boundary and does not claim RecoveryPin CAS,
session-fencing or cross-record catalog authority.

After this unsupported-RecoveryPin fence, the focused catalog test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this remains local regression evidence only.

The Recovery Catalog snapshot encoder now also rejects a non-null manifest set
whose `catalogShard` does not match its manifests, and rejects a non-empty
manifest set without a shard identity. After the explicit resource/identity
checks, it reuses the local snapshot validator so ancestry, scalar/typed Floor,
generation and shard relationships cannot be encoded into bytes that the
decoder would refuse. `OxiaSyncRecoveryCatalogBackendTest`
`rejectsCatalogShardIdentityThatDoesNotMatchManifestBeforeEncodingSnapshot`
covers the cross-shard case. This remains a local serialization-integrity
fence; it does not add external recovery authority.

After this complete snapshot-structure fence, the focused catalog test and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in
real-Oxia methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was
unset; this remains local regression evidence only.

The crash-durable local `PersistentRecoveryCatalog` encoder now applies the
same complete Snapshot projection validation before emitting bytes. It rejects
resource-map aliases and foreign catalog-shard identities instead of silently
normalizing them into a different local state; `PersistentRecoveryCatalogTest`
`snapshotEncoderRejectsResourceMapAliasBeforeEmittingBytes` and
`snapshotEncoderRejectsForeignCatalogShardBeforeEmittingBytes` cover the two
direct-encoder fences. This strengthens only the local catalog serialization
boundary; session-bound RecoveryPin, upload-intent/catalog transaction,
Object Store publication and external recovery authority remain release
blockers.

Manifest-backed `ShardStore.restoreFromCheckpoint` now requires the complete
`meta_cf/FIXED` key 10 `CompatibleControlSnapshotV1` and requires its digest to
equal `CheckpointManifest.controlStateDigest`; a missing snapshot can no longer
silently pass restore validation. `ShardStoreTest.restoreWithManifestRejectsMissingControlSnapshot`
covers the missing-key path, while the existing catalog/lineage restore fixtures
now carry a matching snapshot so their later pin and recovery-integrity checks
remain targeted. This closes the local restore activation boundary only; Oxia
catalog/Owner Lease transaction authority, Object Store publication and real
service evidence remain open.

After this manifest control-snapshot fence, the focused `ShardStoreTest` and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, five executed tasks). The five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is
local regression evidence only.

`CheckpointControlSnapshotVerifier` now requires key 10 to be present in a
recognized RocksDB checkpoint before `CheckpointUploadCoordinator` can publish
the manifest. The previous optional path could publish a complete DB image
without the control projection; `CheckpointControlSnapshotVerifierTest`
`rejectsMissingControlSnapshotFromRecognizedRocksDbImage` covers the fail-closed
case. Directories without a RocksDB MANIFEST remain an explicit legacy fixture
seam, while real checkpoint upload/publication authority remains external.

After this checkpoint-upload control-snapshot fence, the focused verifier and
upload-coordinator tests and the full `./gradlew clean check --rerun-tasks
--console=plain` gate passed on 2026-08-12 (`BUILD SUCCESSFUL`, five executed
tasks). The five opt-in real-Oxia methods remained skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset; this is local regression evidence only.

The activation call-site audit found no production (`src/main`) caller of the
legacy `OwnedDelayShard.activateForCommands(...)` overloads. They remain only
for embedded ownership tests; V1 Worker/recovery wiring must use
`activateForCommandsWithControlSnapshot(...)`, which proves the exact persisted
shard control snapshot before exposing `ACTIVE_FOR_COMMANDS`.

`CheckpointUploadCoordinator` now also validates the recognized RocksDB image's
fixed store-format marker and `StoreMetadata` against the manifest's shard,
`dbIdentity` and `sourceStoreIncarnation`, before provider I/O. The focused
`CheckpointControlSnapshotVerifierTest.rejectsManifestWhenCheckpointStoreIdentityDrifts`
regression proves that a file-complete image cannot be published with a foreign
DB identity. This is local upload-integrity evidence only; Object Store
publication and upload-intent/catalog transaction authority remain blockers.

The strict local reuse entrypoint `ShardStore.openForLocalRecoveryReuse` now resolves
only an existing checksummed `ACTIVE` Store Incarnation, opens it with the normal
one-shard DB/resource fences, and calls `RecoveryCatalogAuthority.validateLocalStoreRecovery`
before returning the Store. A missing ACTIVE incarnation never creates a fresh DB;
catalog/Floor rejection closes the native Store and releases its Worker slots before
the error escapes. `ShardStoreTest.localRecoveryReuseOpensOnlyCatalogValidatedActiveStore`
and `ShardStoreTest.localRecoveryReuseDoesNotCreateAFreshDbWithoutActiveIncarnation`
cover the success, rejection/close and no-fresh-DB paths. This is only the local
catalog/Floor reuse gate; Owner Lease/session fencing, source replay and final
`ACTIVE_FOR_COMMANDS` activation remain external.

The legacy raw-byte `DelayShard.claimForPublish(...)` primitive is now
package-local. Production main sources have no caller of that overload: the
public typed `claimForPublishV1(...)` entry is reached through
`OwnedDelayShard`, while `ClaimHandoffWorkClassExecutor` supplies the bounded
work-class and authority checks. The only cross-package compatibility bridge
lives in test sources to create an activation-recovery fixture, and
`DelayShardTest.physicalGcMutationPrimitivesAreNotPublicProductionApis` locks
the visibility. After this API fence, focused Claim/ownership tests and the
full `./gradlew clean check --rerun-tasks --console=plain` gate passed on
2026-08-12 (`BUILD SUCCESSFUL`, six executed tasks). Five opt-in real-Oxia
methods remained skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset; this
is local API-boundary evidence only.

The legacy timeline `DelayShard.discoverDue(...)` scan is now package-local.
It has no production main-source caller and lacks the trusted-UTC evidence,
execution-time Owner reread and byte/elapsed scan envelope required by the
active READY-discovery contract. `DelayShardTest.legacyRecordOnlyDiscoveryOverloadsAreNotPublicProductionApis`
locks the visibility while existing same-package tests retain its compatibility
semantics. Production discovery remains exclusively routed through
`DueSchedulerWorkClassExecutor`, `OwnedDelayShard` and
`PersistentLaneScheduler`. Focused runtime/scheduler tests and the full local
Gradle gate passed after the change; five opt-in real-Oxia smoke methods were
still skipped without `NEREUS_DELAY_OXIA_ENDPOINT`.

The count-only compatibility overloads for message expiry, reservation expiry
and Lane-close discovery are now package-local as well. They previously filled
missing byte/elapsed fields with effectively unbounded values, so a production
caller could have bypassed the exact envelope enforced by the corresponding
discovery work-class. Strict overloads taking `SchedulerBudget` and a monotonic
clock remain public for `OwnedDelayShard`; cross-package executor fixtures use
only a test-classpath bridge. The reflection regression locks all four
record-only discovery seams. Focused discovery/materialization tests and the
complete local Gradle gate passed; the five real-Oxia methods remained opt-in
and skipped without an endpoint.

`DelayShard.discoverReady(earliest, limit)` is now package-local. It has no
production main-source caller and provides neither actual-byte/elapsed charging
nor the typed trusted-time/Owner/certificate composition required for active
scheduling. Existing runtime tests retain it as an index-semantics oracle, with
the only cross-package recovery assertion using the test-classpath bridge.
Production READY discovery remains `DueSchedulerWorkClassExecutor` ->
`OwnedDelayShard` -> `PersistentLaneScheduler.discoverReady(evidence, budget)`.
The reflection API fence, runtime/ownership/scheduler focused suites and full
local Gradle gate passed; five real-Oxia smoke methods remained skipped without
an endpoint.

`PersistentLaneScheduler` now exposes only the complete
`discoverReady(TrustedUtcIntervalEvidence, SchedulerBudget)` form across package
boundaries. The no-evidence and scalar-due-through overloads have no main-source
caller outside the scheduler and are package-local compatibility surfaces;
otherwise a caller could promote READY work without the certificate-issued-at,
certificate-expiry, exact Owner and Store Incarnation checks. A reflection
regression locks both overloads. Focused scheduler, due-work and Claim-handoff
tests plus the full local Gradle gate passed; five real-Oxia smoke methods
remained opt-in and skipped.

The untimed `PersistentLaneScheduler.poll(SchedulerBudget)` compatibility
overload is now package-local. It delegated with `dueThrough=Long.MAX_VALUE`
and could therefore remove future work from the active ring if used as a
production shortcut. `WorkerScheduler` already calls the explicit
`poll(dueThroughEpochMs, budget)` form; same-package scheduler tests retain the
compatibility overload. The scheduler reflection fence, focused Lane/Worker,
due-executor and Claim-handoff tests, and the full local Gradle gate passed;
five real-Oxia smoke methods remained skipped without an endpoint.

The untimed `WorkerScheduler.poll(SchedulerBudget)` overload is now
package-local as well. It also mapped to `Long.MAX_VALUE`, so exposing it would
let a Worker-level caller bypass due eligibility before DRR delegates to each
shard scheduler. The explicit `poll(dueThroughEpochMs, budget)` form remains
public and propagates the same inclusive boundary to shard-local polling.
`WorkerSchedulerTest.untimedWorkerPollIsNotPublicProductionApi`, the focused
Worker/Lane/due/Claim suites and the full local Gradle gate passed; five
real-Oxia smoke methods remained opt-in and skipped.

The innermost `LaneScheduler.poll(SchedulerBudget)` overload is package-local
too. Its only main production caller was already the explicit due-through form
from `PersistentLaneScheduler`; the untimed overload remains solely for
same-package algorithm tests. Lane, persistent-shard and Worker scheduler
public polling now uniformly requires a due-through timestamp. The shared
scheduler reflection fence, focused Lane/Worker/due/Claim tests and complete
local Gradle gate passed; five real-Oxia smoke methods remained skipped.

Direct `ScheduleWorkItem` injection is no longer public at any scheduler layer:
`LaneScheduler.offer`, `PersistentLaneScheduler.offer` and
`WorkerScheduler.offer` are package-local. Production main sources never call
these methods across the scheduler package; authoritative READY discovery
validates the persisted key/value, Message, Timeline, typed Lane/certificate,
Owner and Store identity before the persistent scheduler offers internally.
Scheduler algorithm tests retain package access. Reflection fences, focused
Lane/Worker/due/Claim coverage and the full local Gradle gate passed on
2026-08-13; five real-Oxia smoke methods remained opt-in and skipped without
an endpoint.

Raw `PersistentLaneScheduler` lifecycle mutation is now package-local:
registration, persisted-projection restore, fenced READY rebuild, readiness
transitions, terminal unregister, direct requeue and explicit persist are not
public Worker APIs. No production main source called those methods across the
scheduler package. Cross-package ownership/runtime tests use a test-classpath
bridge only for registration and fenced-rebuild fixtures; the bridge is absent
from the main artifact. Strict discovery and Claim primitives used by
`OwnedDelayShard` remain public. The reflection fence, complete `DelayShardTest`
and focused scheduler/ownership suites plus the full local Gradle gate passed
on 2026-08-13; five real-Oxia smoke methods remained skipped. Production
recovery/readiness/retirement coordinators and their external authority remain
release work, not implied by these local primitives.

Raw lifecycle mutation on `LaneScheduler` and `WorkerScheduler` is now
package-local too: registration, readiness/block transitions, ring rebuild,
snapshot restore, direct requeue, READY replacement and terminal/shard
unregister cannot be called from a future Worker package as authority-free
wiring. No current main source outside the scheduler package depended on these
methods. Constructors, explicit timed poll and read-only snapshots/queries
remain public. Reflection tests lock the two surfaces; focused scheduler and
ownership tests plus the full local Gradle gate passed on 2026-08-13, with five
real-Oxia smoke methods still skipped. A production Worker scheduler
coordinator remains OPEN and must expose only authority-checked composition.

The embedded-owner `PersistentLaneScheduler(store, delegate)` constructor and
`defaults(store)` factory are now package-local. They derive a deterministic
`embedded-scheduler` owner at epoch 1, which is suitable for local scheduler
fixtures but cannot prove the current Owner Lease or bind production READY
certificates. Cross-package tests obtain it through a test-only factory; the
public constructor requires an explicit `OwnerIdentityV1`. Reflection coverage,
focused runtime/scheduler/ownership tests and the full local Gradle gate passed
on 2026-08-13; five real-Oxia smokes remained skipped. Actual scheduler Owner
construction from authenticated lease/session state remains integration work.

`OwnedDelayShard` 的公开生产构造现在要求显式、非空的完整
`OwnerIdentityV1`，并在构造时校验 Shard identity 与 lease `ownerEpoch`。旧两参数
构造已降为 ownership 包内兼容 seam；它没有协议 Owner identity，因此不能通过
strict due/Claim、expiry、Publish Admission 或 Owner-authored outcome gate。上述实时
路径不再只比较 epoch，而是把 scheduler、Claim 或 mutation author 与绑定的完整
Owner identity 做 equality fence；Claim author 也只从该绑定 identity 生成。测试覆盖
null/epoch/Shard 构造拒绝及“相同 epoch、不同 deployment/worker/fencing digest”的
scheduler 拒绝。聚焦 ownership/due/Claim/expiry/outcome/admission 测试及完整
`clean check --rerun-tasks` 在 2026-08-13 通过（6 个任务执行）；5 个真实 Oxia smoke
仍因未设置 `NEREUS_DELAY_OXIA_ENDPOINT` 跳过。认证 lease/session 到协议 Owner 的
生产 Worker 组装仍是 OPEN release blocker。

`OwnedDelayShard` 的 lease renewal、所有 catch-up admission/cursor、activation 与
drain transition primitive 现均为 ownership 包内可见；包外公开面只保留完整 Owner
identity 构造、只读 lease/assignment/lifecycle/failure projection，以及只能收权的
`fence()`。`OwnerRecoveryCoordinator` 和 `OwnerDrainCoordinator` 仍是公开的 bounded
组合入口，包内 WorkClass executors 继续使用所需的精确校验原语。反射回归锁定所有
重载，聚焦 ownership/recovery/drain/source-apply 测试及完整 6-task Gradle 门禁于
2026-08-13 通过；5 个真实 Oxia smoke 仍因缺少 endpoint 跳过。此项只关闭 API-level
authority bypass，生产 Worker、真实 source quiescence 和 placement/Oxia 集成仍 OPEN。

`OwnerRecoveryCoordinator` 不再接受调用方预构造的
`SourceApplyWorkClassExecutor`。它从同一组 exact owned shard、Oxia authority、
verification key 和 shared WorkClass registry 内部构造恢复 executor，消除了 coordinator
与 executor 绑定到不同 Shard/authority/key/queue 的混装面；`submitRecovery(...)` 同时
降为 ownership 包内入口，包外活动 source `submit(...)` 保持公开。构造器形状与恢复
入口可见性由反射测试锁定，聚焦 recovery/source-apply 测试和完整 6-task Gradle 门禁
于 2026-08-13 通过；5 个真实 Oxia smoke 跳过。真实 Broker cursor、Worker event loop
和 session-bound authority 仍是 OPEN 集成证据。

`SourceApplyCoordinator` 现在与恢复协调器采用同一依赖闭合方式：公开构造器显式接收
owned shard、Oxia authority、verification key 和 shared WorkClass registry，并在内部
构造 active `SourceApplyWorkClassExecutor`；调用方不能混装另一套 owner/authority/key/
queue。构造器形状由反射测试锁定，ACK-after-sync、unknown-ACK retention、queue
rejection 和低层 executor 聚焦测试及完整 6-task Gradle 门禁于 2026-08-13 通过；5 个
真实 Oxia smoke 跳过。低层 active executor 仍是公开 bounded action，但真实
Kafka/Pulsar consumer 必须由 coordinator 绑定 ACK/cursor/rewind；这些外部证据仍 OPEN。

`OwnerDrainCoordinator` 的公开生产构造现在要求非空 shared WorkClass registry，不能
以 `null` 静默退化到无 bounded final-checkpoint executor；无 registry 的四参数兼容
构造保持 package-local。构造器还把 owned runtime 的 Store Incarnation 与传入
`ShardStore` 做 exact equality fence，除已有 ShardId 和 shared-resource 实例校验外，
同 Shard 的另一 DB incarnation 也会在任何 close/lease 操作前被拒绝。聚焦
owner-drain/checkpoint 测试和完整 6-task Gradle 门禁于 2026-08-13 通过；5 个真实
Oxia smoke 跳过。Object Store upload/catalog publication、真实 callback quiescence 与
Worker drain wiring 仍为 OPEN release evidence。

Store 与 Worker resource envelope 现在有统一 exact-config fence：`ShardStore.open`、
local recovery reuse、全部 restore helper、内部 `openAtPath` 以及
`CheckpointRestoreCoordinator` 都要求调用 config 与创建 `SharedRocksDbResources` 的
完整 `ShardStoreConfig` equality。混用另一 root/limits 的请求在文件系统、slot 或
provider I/O 前失败。`CheckpointExecutionCoordinator` 还要求 publication/upload
coordinator 的 `SharedRocksDbResources` 与 active Store 的资源实例相同，防止上传槽位/
rate limit 计入另一 Worker。测试覆盖 open 零文件系统副作用、restore 零 provider
调用及 foreign publication envelope；一个历史 Outcome fixture 也改为使用同一 config。
聚焦 Store/restore/checkpoint/outcome 测试和完整 6-task Gradle 门禁于 2026-08-13
通过；5 个真实 Oxia smoke 跳过。生产 envelope 数值、基准测试与真实 Object Store/
Oxia authority 仍 OPEN。

`CheckpointDrainWorkClassExecutor` 现在在 queue admission 与执行时 authority reread 前
后都校验 `store.runtimeMetadata.lastOpenedOwnerEpoch == expectedLease.ownerEpoch`。
因此公开 bounded executor 即使拿到同 Shard 的 DRAINING lease，也不能对尚未由该
Owner epoch 打开的旧 Store 创建 final checkpoint；正常 `OwnerDrainCoordinator` 会在
checkpoint 前持久化 exact epoch。直接调用拒绝用例证明零 checkpoint 文件副作用，
正常 drain/checkpoint 聚焦套件和完整 6-task Gradle 门禁于 2026-08-13 通过；5 个真实
Oxia smoke 跳过。该 Store projection fence 不替代真实 session-bound lease、catalog
或 Object Store publication authority。

Large-payload proof authority is now fixed by the durable Prepare projection.
`DelayShard` consults the exact `PayloadProofTrustSetRefV1` in a Registry V1
Prepare's `V1ScheduleBinding` regardless of whether the later Commit body is V1
or legacy; only a reservation created by legacy Prepare can use the old
version-only verifier. The regression deliberately gives the pinned semantic
and legacy fallback the same trust-set/key versions but different public keys,
and proves that a legacy Commit cannot consume the reservation through the
fallback key. Code commit `897c0eec` and the complete six-task local Gradle gate
passed on 2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. Authenticated trust-set publication,
catalog durability and Recovery-Floor historical key retention remain OPEN.

The local payload adapter registration boundary now consumes that same exact
Prepare authority. `EmbeddedDelayService` reloads the durable
`V1ScheduleBinding` before registering a reservation, and
`InMemoryPayloadObjectStore` requires its full semantic ref—not only its
version—to equal the pinned `PayloadProofTrustSetRefV1`. A close/reopen
regression proves that an adapter with the same version/key version but a
different semantic hash returns the existing public `INTEGRITY_ERROR` and
retains zero reservation/handle state. Code commit `3c2ed49a` and the complete
six-task local Gradle gate passed on 2026-08-13; five real-Oxia smokes were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. Production Object Store
credentials, authenticated facade routing and Oxia semantic publication remain
OPEN.

`PrepareLargeScheduleV1` now closes the adjacent Object Store authority gap:
required canonical field 15 carries the complete `OBJECT_STORE ProfileRefV1`,
catalog-backed apply resolves its exact immutable semantic/current credential
Head, and the durable `V1ScheduleBinding` becomes the common authority for
receipt/handle/attestation registration and later Commit verification. The
strict adapter path requires exact Profile and trust-set refs before it records
a reservation. Typed Commit proofs require complete Profile-ref equality;
legacy proofs can match only the pinned semantic hash. The same check precedes
both the initial Commit and the `ALREADY_COMMITTED` fast path, preventing a
same-hash/different-Profile identity from receiving an idempotent success.
Protocol round-trip/kind rejection, typed and legacy Profile downgrade,
accepted Commit, committed identity persistence, issuer-close historical retry,
forged-signature rejection, exact adapter registration and close/reopen
facade-drift regressions passed. Code commits `5747e833` and `db22d2e7`; the complete
six-task local Gradle gate passed on 2026-08-13; five real-Oxia smokes were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. The missing-field bytes
are treated as a pre-activation incompatible schema omission and are not
migrated. Likewise, prior local `PayloadReference` values without
ReservationId/ProofId remain decodable but cannot satisfy Registry historical
idempotency or typed committed-descriptor/Claim identity; their external
migration/activation policy remains OPEN. Production Profile/trust-set
publication, Object Store credential and provider authority, Oxia source
ordering and external service evidence remain OPEN.

The crash-durable filesystem payload seam now exposes the same strict Registry
registration boundary as the in-memory adapter. A caller can supply the exact
trust-set and Object Store Profile refs reloaded from durable Prepare state;
same-version/different-semantic trust sets and same-hash/different-identity
Profiles fail before reservation registration, handle issuance or payload-file
creation. The existing one-argument registration remains explicitly legacy/
local compatibility only. Restart/proof stability now exercises the strict
path, and the negative regression proves zero handle and regular-file state.
Code commit `c4af3096` and the complete six-task local Gradle gate passed on
2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. `FilesystemPayloadObjectStore` remains
a local test seam and is not yet the embedded facade type or a production
provider/credential/Oxia authority.

Typed Claim persistence now reuses the durable V1 command sidecar instead of
trusting an authority-supplied materialization merely because its compact
`PayloadReference` matches. For ordinary Registry Schedule, `DelayShard`
requires exact Destination Profile, business metadata, delivery window and
inline/committed payload descriptor equality. For Prepare→Commit, it requires
the complete Object Store ProfileRef pinned by Prepare plus the expected length
and SHA-256; the committed ReservationId/ProofId/object coordinates remain
checked by the current Message reference. Regressions reject same-semantic-hash
foreign Destination and Object Store Profile identities for ordinary Schedule
and reject the equivalent Profile substitution after a signed large-payload
Commit, without changing `SCHEDULED` state. Code commit `abc6fec1` and the
complete six-task local Gradle gate passed on 2026-08-13; five real-Oxia smokes
were skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. Delivery Capability,
target resource and partition immutable identity are covered by the tuple
projection below; live Profile/credential/resource availability, Object Store
fetch/immutability, Adapter serialization/size and Producer/recovery authority
remain OPEN.

The same typed Claim boundary now consumes the complete immutable Lane identity
already stored in `V1ScheduleBinding.canonicalLaneTuple`. The shared parser
reconstructs the exact Destination and Delivery Capability Profile refs,
Kafka/Pulsar Broker target resource and physical partition, then rejects any
materialization drift before the Claim batch. Kafka's repeated native-topic
UUID and physical-topic identity are also cross-checked. Protocol tests cover
both Kafka and Pulsar tuple reconstruction plus an internally inconsistent
Kafka tuple; runtime tests independently cover Capability, target and partition
substitution. Code commit `dc5cc765` and the complete six-task local Gradle gate
passed on 2026-08-13; five real-Oxia smokes were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. This proves only the immutable
Schedule-time identity: live Profile/credential/resource availability, Object
Store fetch, Adapter serialization/channel lease, Producer and recovery
authority remain OPEN.

Publish Admission preparation now enforces both lower time bounds that precede
its existing expiry/certificate/deadline upper bounds. A decision interval
whose earliest instant is before descriptor `actionAt`, or before the depended-on
Ready Certificate finished issuance, is rejected synchronously before a
work-class action exists and before `ShardLogMutationAppender` can run.
`PublishAdmissionWorkClassExecutorTest` proves zero registered actions and zero
append calls for both cases. Code commit `c568a041` and the complete six-task
local Gradle gate passed on 2026-08-13; five real-Oxia smokes were skipped
because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. Profile/payload/channel
prerequisite implementations, real Broker append and Producer ownership remain
OPEN.

Large-payload certified timing now has explicit restart evidence. After a
catalog-backed V1 Prepare has durably stored its `V1ScheduleBinding`, the test
closes the shard DB, reopens a fresh `DelayShard`, and applies the signed Commit
without any Prepare-turn resolver scratch. Commit reloads the exact Destination
Profile from the Prepare body, resolves the pinned Delivery Capability through
the same catalog, and persists `deliverAt=3000` with the fixed-lead
`actionAt=2500`; ready discovery rejects 2499 and accepts 2500.
`DelayShardTest.largeCommitAfterReopenRecoversCertifiedActionAtFromDurablePrepareBinding`
passed in code commit `59c4e6de`; the complete six-task local Gradle gate passed
on 2026-08-13 in 1m13s, with five real-Oxia smokes skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was unset. This closes the local durable-binding
recovery proof only; real Profile/Oxia publication, Object Store provider,
Broker visibility guard and Producer authority remain OPEN.

Certified large-payload timing is also rejected at the earliest stateful
boundary. `ProfileCatalogV1ScheduleResolver.resolvePrepare` now uses the same
single resolved Destination/Capability semantic pair as Schedule and runs the
fixed-lead `expectedActionAt` check before invoking the Lane resolver. An
underflow therefore returns stable `INVALID_DELIVERY_WINDOW` before any
Reservation, Lane, `V1ScheduleBinding` or reservation-quota projection exists;
the subsequent valid Prepare still succeeds. Resolver and `DelayShard.apply`
regressions passed in code commit `c9600447`; the complete six-task local
Gradle gate passed on 2026-08-13 in 1m13s, with five real-Oxia smokes skipped
because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. Real upload/provider and
external source authority remain OPEN.

Source-ordered Registry Schedule/Prepare now revalidate every command field
that is decidable from the immutable Profile before Lane resolution or shard
state creation. The exact Destination semantic gates Adapter metadata branch,
allowed ordering-mode bit, business payload length and canonical adapter
metadata length. Committed Schedule and Prepare additionally require the exact
source-activated Object Store Profile/current credential Head and enforce its
`maxObjectBytes`. Stable failures are `INVALID_METADATA`,
`ORDERING_CAPABILITY_UNAVAILABLE` or `PAYLOAD_TOO_LARGE`, with zero delegate
calls and no Message, Reservation, Lane, binding or quota projection. Resolver
and shard-level regressions passed in code commit `8955510c`; the complete
six-task local Gradle gate passed on 2026-08-13 in 1m11s, with five real-Oxia
smokes skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was unset. Final
`maxTargetRecordBytes` validation remains in the still-OPEN Adapter
serialization prerequisite because it depends on the complete prepared record,
not raw payload length; real Profile/Oxia/Object Store authority also remains
OPEN.

## Verification command

Use the checked-in Gradle Wrapper and an isolated cache on hosts where the
default Gradle native cache is not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
