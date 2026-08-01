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
and authenticated Oxia target registration remain release blockers; the
possible-delivery terminal branch is now locally covered by retaining the exact
UNCERTAIN obligation while terminalizing the generation and releasing its
active pending quota.

The bounded replay increment now covers `REPLAY_DEAD_LETTER_V1` after a
`DEAD_LETTER` terminal decision: it checks the exact generation/state-version
precondition, terminal summary and duplicate acknowledgement rule, lane gate,
timing and shard quota, then atomically creates the next generation's
`INITIAL_SCHEDULE` timeline while retaining the old summary and obligations.
Immutable RetryPolicy/Profile binding, replay-window/fence proofs, Oxia target
registration and full DLQ/replay retention remain pending.

The local `TIME_FENCE_V1` increment now validates the exact proof ID, fence key
version and Trusted-UTC lower bound, monotonically persists
`closedIngressDeadlineThrough`, and rejects later commands at the position level
without overwriting an existing command identity/result. Reservation-expiry
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
registration and the Lane terminal guard remain release blockers.

The local query increment now exposes bounded read-only
`MessageQuerySnapshot` and `ReservationQuerySnapshot` projections. They derive
the exact current runtime/terminal state, state version, timing, duplicate-risk
bit and safe payload-availability category without exposing payload bytes,
destination lane identity, object-store keys, command hashes or receipt data.
The wire-level closed unions are now also encoded: `CommandQueryResponseV1`
and `MessageQueryResponseV1` cover every Registry public view and error branch,
including source-barrier pending views, safe destination binding, retired
identity and evidence-reference projections. The codecs do not fabricate those
views from the local snapshots: receipt/barrier routing, authorization policy,
real binding/evidence lookup and source-derived retention decisions remain
release blockers.

The bounded `RESOURCE_RETIRE_INTENT_V1` increment now validates the closed
`ExactResourceIdentityV1` branches and canonical `ProtectionSetV1`, checks the
registered retire logical identity, and atomically persists an immutable
`gc_cf` intent with its applied shard mutation sequence and source position. It
deliberately does not perform an external delete, apply
`RESOURCE_DELETE_CONFIRMED_V1`, replace a Lane with its terminal guard, or
infer Recovery Floor release; those remain release blockers.

The bounded `RESOURCE_DELETE_CONFIRMED_V1` increment now validates the exact
Retire Intent reference, delete outcome, provider evidence and Trusted-UTC
interval, then source-orders a local `gc_cf/TASK` tombstone that retains the
full intent and confirmation evidence. It does not claim that the provider
delete happened merely because this mutation was accepted: local payload and
checkpoint identity/version fields are compared when present, while real
adapter ownership/attestation, Recovery Floor barriers, quota release and
Lane terminal-guard replacement remain pending.

`ResourceGcGuard` now exposes both the scalar necessary-condition result and a
catalog-backed variant. The latter requires exact parent-hash ancestry from a
published candidate through the current local Recovery Floor, in addition to
the applied mutation sequence and Source Position checks. `DelayShard` can
compact the local `gc_cf/TASK` tombstone only after that proof. This remains a
local proof: it does not perform the Oxia CAS, provider ownership/attestation,
or external delete.

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
| NDR1 receipt frame | Implemented (queued/applied/reservation payload subset) | `ReceiptFrame`, `ReceiptKind`, `CommandQueuedReceiptV1`, `CommandAppliedReceiptV1`, `PayloadReservationReceiptV1`, `PayloadProofTrustSetRefV1`; registry zero-payload vector, canonical PreparedCommandRef/ProtocolTuple, Kafka/Pulsar Source Position and SafeBrokerAck agreement, queued-to-applied digest/source fencing, apply-status/message-field consistency, object-store profile/object identity/trust-set pinning, query boundary/capability/physical-attempt/digest checks, and flags/length/kind/CRC/Base64url rejection tests; native/control payload schemas and real Broker response adapters remain pending |
| Query response closed unions | Implemented (wire codec subset) | `ProfileRefV1`, `PublicDestinationBindingViewV1`, `PublicEvidenceRefV1`, all Registry Message/Command view classes, `CommandQueryResponseV1`, `MessageQueryResponseV1`, `PublicQueryErrorV1`, `ProtocolCodecTest`; exact branch tags/field order, Source Position barrier ordering, state/status agreement, safe NFC alias and payload/DLQ/evidence enum checks, canonical round-trip and rejection vectors; real receipt/barrier routing, authorization-safe lookup, retention decisions and service integration remain pending |
| System Mutation envelope, type registry, canonical hash/ID and Ed25519 signature | Implemented (bounded control plus admission/expiry/outcome/evidence/claim-result/resource-retire/delete-confirmed subset) | `SystemMutationType`, `SystemMutation`, `SystemMutationBodyCodec`, `ApplyShardControlBody`, `ControlRef`, `PublishAdmissionBody`, `PublishOutcomeBody`, `ClaimResultBody`, `ResourceRetireIntentBody`, `ResourceRetireIntentRecord`, `ResourceDeleteConfirmedBody`, `ResourceDeleteConfirmedRecord`, `TrustedUtcIntervalEvidence`, `SystemMutationResult`, `AuthorIdentity`, `ClaimRecord`, `GenerationRuntimeIndex`, `DelayShard`, `ProtocolCodecTest`, `PublishAdmissionBodyTest`, `ResourceRetireIntentBodyTest`, `ResourceDeleteConfirmedBodyTest`, `GenerationRuntimeIndexTest`, `DelayShardTest`; canonical owner/control/fence/service branches, body fields 1–3, required/optional operation fields, wire widths, bool bounds, canonical nested bytes, durable `dedupe_cf/SYSTEM_MUTATION`, explicit signature verification, source-ordered Lane PAUSE/RESUME/BREAK/CLOSE with ControlRef/identity/incarnation/CAS, close-policy/acknowledgement checks and atomic Claim/READY rollback, source-ordered TIME_FENCE watermark and reservation-expiry overlay/materialization, source-ordered `EXPIRE_GENERATION_V1`, `PUBLISH_ADMISSION_V1` descriptor/Ready Certificate/Claim identity projection, replay-stable timeline-key/semantic-digest/counter/obligation-set preconditions, checked attempt-number and uncertain-retry counter projection, definitive `PUBLISH_OUTCOME_V1/NOT_PUBLISHED` disposition/retry-shape subset, verified published/not-published `EVIDENCE_RESOLUTION_V1` transition subset, replay-stable permanent `CLAIM_RESULT_V1` ClaimPrecondition/timeline terminalization subset, closed ExactResourceIdentity/ProtectionSet parsing plus registered logical-identity verification and atomic `gc_cf/TASK` retire-intent persistence, exact RetireIntentRef/DeleteOutcome/ExternalDeleteEvidence matching and source-ordered local tombstone persistence, local durable `SCHEDULED -> CLAIMED -> revoke/Admission/ClaimResult/Cancel/Reschedule/expiry` transitions, and v4 `id_cf/MESSAGE` runtime-index writes are covered; real adapter/provider delete attestation and ownership, source-protected signing-key trust/ACL, immutable Oxia target registration, Recovery Floor barriers, Lane terminal guards, obligation-set quota/recovery reconciliation and full Claim materialization/recovery model remain pending |
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
| Recovery catalog, lineage and Floor selection | Implemented (in-memory core subset) | `RecoveryCatalog`, `RecoveryFloor`, `RecoveryCatalogTest`; catalog binds one shard, rejects non-zero genesis lineage, enforces floor ancestry, exposes candidate validation/selection and `proveFloorCoverage` evidence; durable Oxia catalog/session pins, Object Store publication and evidence cursors pending |
| Command applied/rejected state machine | Implemented (embedded core) | `DelayShard`, `DelayShardTest` |
| DUE/ORDERED/READY/EXPIRY timeline namespaces | Implemented (embedded core subset) | `DelayShard`, `MessageRecord`, `TimelineWorkRef`, `GenerationRuntimeIndex`, `ClaimRecord`, `ReadyIndexValue`, `KeyCodec`, `GenerationRuntimeIndexTest`, `DelayShardTest`; READY key/value, laneVersion fencing, retry eligibility for unordered definitive retry, canonical timeline semantic/instance digests, v4 runtime-index persistence, atomic affected-lane updates, durable Claim removal/restoration including source-ordered Lane Pause rollback, fenced rebuild/discovery, replay-stable Claim Result timeline-key/semantic-digest checks, Cancel/Reschedule fencing whenever an UNCERTAIN obligation survives a current-work projection, and the pinned-policy `UNKNOWN` scheduling plus `UNCERTAIN_RETRY` Admission subset that materializes timeline work while retaining the old obligation are covered; ControlRef validation and policy-bound timeline materialization beyond this local budget remain pending |
| `CLAIMED`/`PUBLISHING`/`UNCERTAIN` attempt ledger and obligation locator | Implemented (durable local Claim plus source-ordered attempt subset) | `ClaimRecord`, `PublishAttemptLedger`, `AttemptObligationRef`, `GenerationRuntimeIndex`, `PublishAdmissionBody`, `PublishOutcomeBody`, `DelayShard`, `DelayShardTest`; local Claim sequence/key/value and exact precondition/instance digest are persisted atomically, registry-shaped runtime index and canonical obligation-set digest are persisted with v4 Message records, source-ordered `PUBLISH_ADMISSION_V1` checks replay-stable timeline key/semantic/counter/obligation projections and descriptor attempt number, including the `UNCERTAIN_RETRY` source-work branch, reconstructs the same PUBLISHING attempt when the reversible Claim is absent but source state matches, retains admission counters across definitive retry timelines, and fails closed at the configured per-generation `maxPublishAdmissions`/automatic `maxUncertainRetries` bounds; shard activation now performs bounded bidirectional reconciliation between every current/terminal runtime obligation ref and its exact PUBLISHING/UNCERTAIN ledger, and between every live ledger/Claim and the current Message branch, failing closed on an orphan, missing counterpart, persisted total-admission overflow, or terminal/runtime summary mismatch; source-ordered `PUBLISH_OUTCOME_V1` UNKNOWN atomically migrates to UNCERTAIN, can materialize the pinned-policy `UNCERTAIN_RETRY` timeline subset without consuming the retry counter, verified-success closes the ledger, definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable outcomes atomically requeue, terminalize, or block the lane, and late outcomes from either the current or an older generation settle only the exact ledger/terminal summary while preserving terminal decision state and monotonically raising duplicate risk for verified success; service-authored verified `EVIDENCE_RESOLUTION_V1` can close or requeue the exact UNCERTAIN ledger; full Profile/Adapter/evidence/capacity validation, immutable RetryPolicy binding, multi-obligation terminal-summary lifecycle beyond current-generation settlement, uncertain-retry ControlRef/policy enforcement beyond the local automatic budget, obligation-set quota/recovery accounting beyond local reconciliation, Recovery Floor/source replay and full Claim materialization/recovery semantics remain pending |
| Terminal generation history | Implemented (current/older-generation open-obligation summary and late-settlement subset) | `TerminalGenerationRecord`, `ClaimRecord`, `DelayShard`, `DelayShardTest`, `TerminalGenerationRecordTest`; Claim Result, publish outcome, expiry and command terminalization persist the exact remaining obligation refs and duplicate-risk projection in a versioned terminal record, activation reconciles both current and older-generation summaries against their exact inflight ledgers, and late verified or definitive not-published outcomes remove only their exact old ledger/ref without changing terminal status/code/time; legacy v1 terminal records decode as empty summaries; older-generation callbacks after full Dead Letter Replay, Replay retention and guarded GC remain pending |
| Large-payload reservation/proof/commit | Implemented (embedded core subset) | `LargeScheduleIntent`, `PayloadCommitProof`, `PayloadReservation`, object-backed `MessageRecord`, `DelayShardTest`; Prepare/Commit reservation quota, source-ordered TIME_FENCE overlay, bounded `RESERVATION_EXPIRY` discovery/materialization and guarded local quota release are covered; Object Store handles/attestation, source-ordered trust controls, fence-key history and guarded GC remain pending |
| Source assignment, typed Activation Barrier and Owner Lease | Implemented (local boundary subset) | `KafkaActivationBarrier`, `PulsarActivationBarrier`, barrier-gated `OwnedDelayShard`, `OwnerLeaseStore`; renewal keeps owner identity/token/epoch and monotonic expiry; pinned broker assignment/guard, Oxia adapter and activation CAS pending |
| Queued vs applied client outcomes | Implemented (embedded core) | `EmbeddedDelayServiceTest` |
| Destination Lane gate/readiness projection | Implemented (core plus bounded source control) | `LaneRecord`, `LaneRecordTest`, `ApplyShardControlBody`, `ControlRef`, `DelayShard`, `DelayShardTest`; schedulable lanes maintain a versioned READY head and readiness/gate CAS transitions remove/recreate it atomically; source-ordered signed PAUSE/RESUME/BREAK/CLOSE applies exact Lane incarnation/control-version fencing, strict-lane acknowledgement checks and atomically revokes/restores reversible Claims; full `LaneRecordV1` certificate fields, Oxia target registration and terminal guard remain pending |
| Destination Lane isolation and bounded weighted DRR | Implemented (scheduler core) | `LaneSchedulerTest`; lane work discovery and exact five-value registry projection pending |
| Persistent scheduler fairness counters | Implemented (core subset) | `PersistentLaneScheduler`, `LaneSchedulerTest`; full `meta_cf/SCHEDULER` closed projections pending |
| Worker-to-shard-to-lane bounded DRR | Implemented (core snapshot) | `WorkerScheduler`, `WorkerSchedulerTest`; durable outer scheduler projections and placement weights pending |
| Closed Stable Code registry | Implemented | `StableCode`, `ProtocolCodecTest` |
| Hard shard quota admission | Implemented (core subset) | `ShardQuota`, `DelayShardTest`; atomic multi-shard grants, control reserve and GC accounting pending |
| Kafka/Pulsar ingress and target adapters | In progress (ingress SPI only) | release blocker until concrete pinned transports, target publish/evidence channels and real-broker tests exist |
| Recovery Set/Floor, catalog and restore replay | In progress (local catalog/Floor subset) | release blocker; Oxia catalog/session pin, immutable publication, source/evidence replay and activation CAS remain |
| Large payload, quota grants, control reserve and GC | In progress (reservation/commit, shard hard-quota, retire-intent, delete-confirmed and catalog-backed local compaction subsets) | release blocker; `ResourceRetireIntentBody`/`ResourceRetireIntentRecord` and `ResourceDeleteConfirmedBody`/`ResourceDeleteConfirmedRecord` provide canonical source-ordered `gc_cf/TASK` intent/tombstone persistence with applied mutation sequence, `RecoveryCatalog.proveFloorCoverage` plus `ResourceGcGuard` enforce local ancestry/source/sequence coverage, `DelayShard.compactResourceDeleteConfirmation` removes only a covered local tombstone, and local payload/checkpoint version/etag comparison is enforced, but Object Store/Oxia publication, multi-shard grants, control reserve, real provider delete attestation/ownership, durable catalog/Floor barrier, Lane terminal guard and full guarded GC remain |
| Query, control operations, DLQ and observability | In progress (wire unions plus bounded local Message/Reservation projection) | `MessageQuerySnapshot`, `ReservationQuerySnapshot`, `DelayShard.queryMessageSnapshot`, `DelayShard.queryReservationSnapshot`, all V1 Command/Message query view codecs, `DelayShardTest`, `ProtocolCodecTest`; receipt/barrier routing, authorization-safe binding/evidence/retention lookup, control-operation query, DLQ and observability remain release blockers |
| Real-service, chaos, benchmark, soak and upgrade evidence | Not started | release blocker |

## Verification command

Use the checked-in Gradle Wrapper and an isolated cache on hosts where the
default Gradle native cache is not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
