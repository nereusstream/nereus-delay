# V1 Implementation Status

Spec revision: `V1-FROZEN-2026-08-01`

This file records implementation evidence. It does not relax or replace the
normative requirements in [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md),
the [`V1 Protocol Registry`](V1-PROTOCOL-REGISTRY.md), or the Accepted ADRs.
An unchecked item is not an implementation permission; it is a release blocker.

The checkpoint code now covers the local physical boundary: checksum the full
RocksDB directory, emit the closed manifest JSON projection, and install a
validated checkpoint into a new local Store Incarnation without merging into an
open DB. The local store uses an `ACTIVE` checksummed pointer and an
`incarnations/<storeIncarnation>/db` directory. The embedded `RecoveryCatalog`
now selects and validates a published floor-eligible ancestry before local
restore. Immutable object publication, durable Oxia catalog/session pins, and
Kafka/Pulsar source replay remain release blockers below.

The current source-ordered control increment is deliberately bounded: the
`RESOLVE_UNCERTAIN_V1(RETRY_ALLOW_POSSIBLE_DUPLICATE)` branch now validates a
canonical `ControlRefV1`, its Resolve logical identity, lane incarnation,
acknowledgement hash, current-generation UNCERTAIN obligation and source
position, then materializes one `UNCERTAIN_RETRY(CONTROL_OVERRIDE)` timeline
work item without consuming the Admission counter. Resolve evidence attachment,
possible-delivery terminalization, authenticated Oxia target registration and
the remaining control-operation matrix are still release blockers.

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
| `io.nereusstream.delay.client` | Queued/applied outcome contract and embedded conformance service | `delay-api` / `delay-client-core` / `delay-testkit` |
| `io.nereusstream.delay.adapter` | Broker/destination interfaces and test adapters | ingress/adapter modules |

## Evidence matrix

| Area | Status | Evidence |
|---|---|---|
| Gradle Java 21 build | Implemented | `gradle compileJava`, `gradle test` |
| Self-routing IDs and CRC32C | Implemented | `ProtocolCodecTest` |
| `commandId + commandHash` prepared before I/O | Implemented | `PreparedCommand`, `CommandHash`, `ProtocolCodecTest` |
| NDL1 frame and canonical Client Command envelope | Implemented | `ShardLogFrame`, `CommandCodec`, registry frame vector test |
| System Mutation envelope, type registry, canonical hash/ID and Ed25519 signature | Implemented (admission/expiry/outcome/evidence/claim-result subset) | `SystemMutationType`, `SystemMutation`, `SystemMutationBodyCodec`, `PublishAdmissionBody`, `PublishOutcomeBody`, `ClaimResultBody`, `TrustedUtcIntervalEvidence`, `SystemMutationResult`, `AuthorIdentity`, `ClaimRecord`, `GenerationRuntimeIndex`, `DelayShard`, `ProtocolCodecTest`, `PublishAdmissionBodyTest`, `GenerationRuntimeIndexTest`, `DelayShardTest`; canonical owner/control/fence/service branches, body fields 1–3, required/optional operation fields, wire widths, bool bounds, canonical nested bytes, durable `dedupe_cf/SYSTEM_MUTATION`, explicit signature verification, source-ordered `EXPIRE_GENERATION_V1`, `PUBLISH_ADMISSION_V1` descriptor/Ready Certificate/Claim identity projection, replay-stable timeline-key/semantic-digest/counter/obligation-set preconditions, checked attempt-number and uncertain-retry counter projection, definitive `PUBLISH_OUTCOME_V1/NOT_PUBLISHED` disposition/retry-shape subset, verified published/not-published `EVIDENCE_RESOLUTION_V1` transition subset, replay-stable permanent `CLAIM_RESULT_V1` ClaimPrecondition/timeline terminalization subset, local durable `SCHEDULED -> CLAIMED -> revoke/Admission/ClaimResult/Cancel/Reschedule/expiry` transitions, and v4 `id_cf/MESSAGE` runtime-index writes are covered; full Profile/Adapter/evidence/capacity semantics, source-protected signing-key trust/ACL, obligation-set quota/recovery reconciliation and full Claim materialization/recovery model remain pending |
| Kafka/Pulsar source order token and source identity fencing | Implemented (core codec) | `SourcePosition.sourceOrderToken`, physical-resource comparison guard, `ProtocolCodecTest`; broker assignment/barrier adapters pending |
| Pinned Kafka/Pulsar command ingress outcome mapping | Implemented (transport SPI) | `PinnedKafkaCommandIngress`, `PinnedPulsarCommandIngress`, `AdapterIngressTest`; concrete pinned request transports and source assignment pending |
| Target publish side-effect outcome boundary | Implemented (transport SPI plus durable attempt/outcome subset) | `DestinationPublishAdapter`, `PinnedKafkaDestinationAdapter`, `PinnedPulsarDestinationAdapter`, `PublishAttemptLedger`, `PublishOutcomeBody`, `DelayShard`, `DestinationAdapterTest`, `DelayShardTest`; physical adapter evidence journal, full admission gate and remaining outcome/evidence mutations pending; definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable state transitions and service-authored verified evidence resolution are covered locally |
| One Delay Shard = one RocksDB DB | Implemented | `ShardStore`, `ShardStoreTest` |
| Worker DB/checkpoint resource limits | Implemented (config-envelope subset) | `ShardStoreConfig`, `SharedRocksDbResources`, `WorkerResourceEnvelope`, `WorkerResourceEnvelopeTest`; checked memory/FD/disk cross-bucket inequalities fail closed before resource creation; live JVM/cgroup/rlimit probes, WAL/SST/temp accounting, per-work-class reserves and placement capacity artifact pending |
| Seven application CFs plus empty `default` CF | Implemented | `ShardStore` descriptor validation |
| Shard identity and local Store Incarnation validation | Implemented | `StoreMetadata`, `ShardStoreTest` |
| Synchronous atomic WriteBatch | Implemented | `ShardStore.write`, `ShardStoreTest` |
| Native RocksDB checkpoint creation | Implemented | `ShardStore.createCheckpoint`, `ShardStoreTest` |
| Checkpoint file inventory and canonical manifest projection | Implemented (local/object publication boundary pending) | `CheckpointFileInventory`, `CheckpointManifest`, `CheckpointManifestTest` |
| Checkpoint restore into a new Store Incarnation | Implemented (local manifest/catalog-validated path) | `ShardStore.restoreFromCheckpoint`, `ShardStoreTest`; exact published floor-eligible catalog candidate is checked before local manifest/file validation; Oxia Recovery Pin/Floor CAS and source replay pending |
| Recovery catalog, lineage and Floor selection | Implemented (in-memory core subset) | `RecoveryCatalog`, `RecoveryFloor`, `RecoveryCatalogTest`; catalog binds one shard, rejects non-zero genesis lineage, enforces floor ancestry and exposes candidate validation/selection; durable Oxia catalog/session pins, Object Store publication and evidence cursors pending |
| Command applied/rejected state machine | Implemented (embedded core) | `DelayShard`, `DelayShardTest` |
| DUE/ORDERED/READY/EXPIRY timeline namespaces | Implemented (embedded core subset) | `DelayShard`, `MessageRecord`, `TimelineWorkRef`, `GenerationRuntimeIndex`, `ClaimRecord`, `ReadyIndexValue`, `KeyCodec`, `GenerationRuntimeIndexTest`, `DelayShardTest`; READY key/value, laneVersion fencing, retry eligibility for unordered definitive retry, canonical timeline semantic/instance digests, v4 runtime-index persistence, atomic affected-lane updates, durable Claim removal/restoration, fenced rebuild/discovery, replay-stable Claim Result timeline-key/semantic-digest checks, Cancel/Reschedule fencing whenever an UNCERTAIN obligation survives a current-work projection, and the pinned-policy `UNKNOWN` scheduling plus `UNCERTAIN_RETRY` Admission subset that materializes timeline work while retaining the old obligation are covered; ControlRef validation and policy-bound timeline materialization beyond this local budget remain pending |
| `CLAIMED`/`PUBLISHING`/`UNCERTAIN` attempt ledger and obligation locator | Implemented (durable local Claim plus source-ordered attempt subset) | `ClaimRecord`, `PublishAttemptLedger`, `AttemptObligationRef`, `GenerationRuntimeIndex`, `PublishAdmissionBody`, `PublishOutcomeBody`, `DelayShard`, `DelayShardTest`; local Claim sequence/key/value and exact precondition/instance digest are persisted atomically, registry-shaped runtime index and canonical obligation-set digest are persisted with v4 Message records, source-ordered `PUBLISH_ADMISSION_V1` checks replay-stable timeline key/semantic/counter/obligation projections and descriptor attempt number, including the `UNCERTAIN_RETRY` source-work branch, reconstructs the same PUBLISHING attempt when the reversible Claim is absent but source state matches, retains admission counters across definitive retry timelines, and fails closed at the configured per-generation `maxPublishAdmissions`/automatic `maxUncertainRetries` bounds; shard activation now performs bounded bidirectional reconciliation between every current/terminal runtime obligation ref and its exact PUBLISHING/UNCERTAIN ledger, and between every live ledger/Claim and the current Message branch, failing closed on an orphan, missing counterpart, persisted total-admission overflow, or terminal/runtime summary mismatch; source-ordered `PUBLISH_OUTCOME_V1` UNKNOWN atomically migrates to UNCERTAIN, can materialize the pinned-policy `UNCERTAIN_RETRY` timeline subset without consuming the retry counter, verified-success closes the ledger, definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable outcomes atomically requeue, terminalize, or block the lane, and late outcomes from either the current or an older generation settle only the exact ledger/terminal summary while preserving terminal decision state and monotonically raising duplicate risk for verified success; service-authored verified `EVIDENCE_RESOLUTION_V1` can close or requeue the exact UNCERTAIN ledger; full Profile/Adapter/evidence/capacity validation, immutable RetryPolicy binding, multi-obligation terminal-summary lifecycle beyond current-generation settlement, uncertain-retry ControlRef/policy enforcement beyond the local automatic budget, obligation-set quota/recovery accounting beyond local reconciliation, Recovery Floor/source replay and full Claim materialization/recovery semantics remain pending |
| Terminal generation history | Implemented (current/older-generation open-obligation summary and late-settlement subset) | `TerminalGenerationRecord`, `ClaimRecord`, `DelayShard`, `DelayShardTest`, `TerminalGenerationRecordTest`; Claim Result, publish outcome, expiry and command terminalization persist the exact remaining obligation refs and duplicate-risk projection in a versioned terminal record, activation reconciles both current and older-generation summaries against their exact inflight ledgers, and late verified or definitive not-published outcomes remove only their exact old ledger/ref without changing terminal status/code/time; legacy v1 terminal records decode as empty summaries; older-generation callbacks after full Dead Letter Replay, Replay retention and guarded GC remain pending |
| Large-payload reservation/proof/commit | Implemented (embedded core subset) | `LargeScheduleIntent`, `PayloadCommitProof`, `PayloadReservation`, object-backed `MessageRecord`, `DelayShardTest`; Object Store handles/attestation, source-ordered trust controls, Time Fence overlay and guarded GC pending |
| Source assignment, typed Activation Barrier and Owner Lease | Implemented (local boundary subset) | `KafkaActivationBarrier`, `PulsarActivationBarrier`, barrier-gated `OwnedDelayShard`, `OwnerLeaseStore`; renewal keeps owner identity/token/epoch and monotonic expiry; pinned broker assignment/guard, Oxia adapter and activation CAS pending |
| Queued vs applied client outcomes | Implemented (embedded core) | `EmbeddedDelayServiceTest` |
| Destination Lane gate/readiness projection | Implemented (core projection) | `LaneRecord`, `LaneRecordTest`, `DelayShard`; schedulable lanes maintain a versioned READY head and readiness/gate CAS transitions remove/recreate it atomically; source-ordered signed control mutation and full `LaneRecordV1` certificate fields pending |
| Destination Lane isolation and bounded weighted DRR | Implemented (scheduler core) | `LaneSchedulerTest`; lane work discovery and exact five-value registry projection pending |
| Persistent scheduler fairness counters | Implemented (core subset) | `PersistentLaneScheduler`, `LaneSchedulerTest`; full `meta_cf/SCHEDULER` closed projections pending |
| Worker-to-shard-to-lane bounded DRR | Implemented (core snapshot) | `WorkerScheduler`, `WorkerSchedulerTest`; durable outer scheduler projections and placement weights pending |
| Closed Stable Code registry | Implemented | `StableCode`, `ProtocolCodecTest` |
| Hard shard quota admission | Implemented (core subset) | `ShardQuota`, `DelayShardTest`; atomic multi-shard grants, control reserve and GC accounting pending |
| Kafka/Pulsar ingress and target adapters | In progress (ingress SPI only) | release blocker until concrete pinned transports, target publish/evidence channels and real-broker tests exist |
| Recovery Set/Floor, catalog and restore replay | In progress (local catalog/Floor subset) | release blocker; Oxia catalog/session pin, immutable publication, source/evidence replay and activation CAS remain |
| Large payload, quota grants, control reserve and GC | In progress (reservation/commit and shard hard-quota subsets) | release blocker; Object Store/Oxia publication, multi-shard grants, control reserve, Time Fence overlay and guarded GC remain |
| Query, control operations, DLQ and observability | Not started | release blocker |
| Real-service, chaos, benchmark, soak and upgrade evidence | Not started | release blocker |

## Verification command

Use the checked-in Gradle Wrapper and an isolated cache on hosts where the
default Gradle native cache is not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
