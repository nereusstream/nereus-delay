# V1 Implementation Status

Spec revision: `V1-FROZEN-2026-08-01`

This file records implementation evidence. It does not relax or replace the
normative requirements in [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md),
the [`V1 Protocol Registry`](V1-PROTOCOL-REGISTRY.md), or the Accepted ADRs.
An unchecked item is not an implementation permission; it is a release blocker.

The bounded local Control Operation authority also rereads an exact CURRENT
advance after response loss; it does not infer success for a later or
different revision.

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

Exact already-published manifests are similarly reread before generation CAS;
same-checkpoint hash drift remains an integrity conflict.

Source replay rejects a connection-generation/guard proof on Kafka positions;
that proof is reserved for the guarded Pulsar source branch.

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

`CommitLargeScheduleV1` now has an explicit canonical body with the same
common fields 1–3, reservation identity and nested typed `PayloadCommitProofV1`.
The proof codec covers the Registry's typed Object Store Profile, tenant scope,
optional etag presence, proof-id/signature digests and Ed25519 verification.
`DelayShard` consumes both the typed V1 proof view and the legacy proof adapter
through the same trust-set/commit state machine; source-position trust-set
authority, Object Store attestation/ownership and reservation binding remain
release blockers.

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

`ProfileBindingActivatePayloadV1` and `ProfileNewBindingClosePayloadV1` now
close the Registry control branches for Profile first-binding lifecycle.
`ProfileBindingControlState` persists strictly source-ordered activation and
close markers; when any Profile markers are present, V1 Schedule/Prepare
first-binding admission is source-position gated and returns the distinct
pre-activation or post-close stable code. Exact duplicate commands still
reuse their durable first result. The immutable Profile catalog, signed
control target authority, and historical binding lookup remain external
blockers.

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
`InMemoryPayloadProofTrustSetCatalog` now resolves exact local semantic
references without fallback. Authenticated source-ordered control authority
and Recovery-Floor historical verifier retention remain external blockers; the
state projection does not invent those authorities.

The closed control-payload branches for trust-set activation and issuance
close now have typed `ControlReasonV1`/payload codecs with strict branch and
optional-field ordering, and `ApplyShardControlBody` exposes their typed
decoders. `DelayShard` now resolves the referenced semantic value through an
explicit catalog seam and persists the marker state, mutation result and
source position in one WriteBatch; reopen restores the same activation and
issuance-close projection. The catalog itself, authenticated control
authority, and Recovery-Floor historical verifier retention remain external
blockers, so the local marker apply does not claim production trust authority.

The local Owner Lease adapters now enforce the closed lifecycle transition
matrix, including fail-closed backward transitions and fenced-lease
non-reactivation; renewal also rejects a response that changes the expected
lifecycle state, rather than silently accepting a same-identity lease in a
different state. The real Oxia ephemeral session/CAS authority remains a
release blocker. Activation also rereads an exact same-identity
`ACTIVE_FOR_COMMANDS` successor after a lost transition response.

`CheckpointScheduler` now provides a bounded process-local schedule for each
owned shard: interval and deterministic per-shard jitter are validated, due
claims are sorted and capped, an in-flight shard cannot be claimed twice, and
completion reschedules from the observed completion time. It is only a local
worker scheduling primitive; checkpoint manifests, upload intents and Oxia
catalog publication remain the durability authority.

Shared RocksDB resources also retain checkpoint create/upload/download slot
counts and reject close while any bounded worker operation is still in flight.
Checkpoint restore/download staging now holds its own Worker-level slot across
manifest/file validation, restore-tmp copy, validation opens, and atomic
installation; it is released only after the active DB is opened or cleanup
completes. `ShardStoreTest.completeCheckpointRestoresIntoFreshStoreIncarnation`
also reacquires that slot immediately after a real restore returns, proving
the slot is released before the caller closes the restored DB.
Restore admission only treats a checksum-validated `ACTIVE` pointer target as
the live incarnation; an orphan incarnation left before pointer installation
does not block a new atomic restore and remains available for later repair.
Normal `ShardStore.open` applies the same `NOFOLLOW_LINKS` rule to the
`ACTIVE` pointer, incarnation directory, DB directory and `CURRENT` marker;
any symbolic path is rejected before it can be opened, so open and restore
cannot disagree about the live-incarnation boundary; restore admission also
rejects a symbolic incarnation or DB path behind a valid `ACTIVE` pointer.
Runtime validation failures after staging begins now remove the private
`restore-tmp` tree as well as releasing the download slot; a pre-acquisition
concurrency rejection keeps its original bounded-resource error.

The checkpoint code now covers the local physical boundary: create the complete
RocksDB image under the same-filesystem `checkpoint-tmp` namespace, atomically
rename it into the requested checkpoint path, checksum the full directory,
emit the closed manifest JSON projection, and install a validated checkpoint
into a new local Store Incarnation without merging into an open DB. The local
`CheckpointUploadCoordinator` now inventories the exact local file set before
provider I/O, charges the Worker upload slot, validates returned manifest
object identity and only then advances the exact pending intent to PUBLISHED;
provider failure or identity mismatch leaves the intent pending for retry. The
local store uses an `ACTIVE` checksummed pointer and an
`incarnations/<storeIncarnation>/db` directory. Typed
`CheckpointResourceV1`/`CheckpointUploadIntentV1` codecs now close the
manifest-object identity and PENDING/PUBLISHED/REAPING branch rules. The
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
manifest binding, candidate/floor identity, and ancestry-endpoint checks.
Typed `RecoveryCandidateRefV1` and
`RecoveryPinV1` codecs now close the candidate branch and session-bound pin
projection, but they are still local value codecs: immutable object publication,
durable Oxia catalog/session pin CAS, and Kafka/Pulsar source replay remain
release blockers below.

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
response attestation.

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
The durable DLQ export state is part of the message snapshot and is the only
accepted source for the public DLQ projection; non-dead-letter generations must
remain `NOT_CONFIGURED`, and the compatibility projector overload rejects a
caller-supplied state that disagrees with the snapshot.
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
accepted as proof of that CAS. They do not provide production Oxia routing,
authorization, or crash-durable control state.

The local Admission increment now decodes the closed 17-dimensional
`ChargeVectorV1` and persists the non-borrowable outcome reserve usage in the
shard DB. Admission, its `PUBLISHING` ledger and the reserve projection share
one WriteBatch; when the configured records/bytes cap cannot fit the charge,
the source position advances with `ADMISSION_CAPACITY_GATED` and any
reversible Claim is restored to `SCHEDULED`. Definitive or verified terminal
settlement releases the exact charge atomically, and restart reloads the
projection. The closed protocol codec for the 66-dimensional
`CapacityVectorV1`, `CapacityGrantV1`, `QuotaGrantRefV1` and
`ShardCapacityEnvelopeV1` is also covered by canonical round-trip and
rejection tests, including component-grant projection and checked sums. This
is still only the local envelope/projection boundary: when an immutable
envelope is supplied at `DelayShard` activation, its outcome grant identity
and canonical envelope are bound under `meta/CONTROL_RESERVE` class 1, the
exact 66-dimensional charged outcome vector is persisted under class 2, and
restart or envelope rotation fails closed on identity, digest, projection or
grant-capacity drift. The legacy no-envelope constructor keeps the prior
records/bytes projection for compatibility. The `SLO_OUTBOX=08` key shape is
now also wired to a synchronous `SloObservationOutboxStore`: it persists an
immutable Start before ownership loss and atomically replaces the conservative
merged Final under the shard's `meta_cf` ValueEnvelope/CRC boundary. This is
also a bounded key-order scan plus exact-record-digest delete-after-ACK
boundary, so an exporter can retry unchanged bytes without deleting a newer
observation. This is
still local evidence only; multi-shard placement/Oxia authority,
non-outcome/recovery/emergency reserve accounting, transfer protocol, SLO
identity-specific reconstruction/collector export and full GC accounting
remain release blockers. Activation now also scans reserve classes 3–6 and
rejects a stale or over-capacity non-outcome/recovery/emergency projection
instead of silently ignoring it; class-6 Broker system-writer accounting is
explicitly rejected until its authority and charge model are implemented.
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
| Registry-shaped `ScheduleIntentV1` | Implemented (canonical codec plus resolver/catalog-backed local apply; external authority pending) | `ScheduleIntentV1`, `ScheduleCommandBodyV1`, `CommandBodies.scheduleV1/decodeScheduleV1`, `PreparedCommand.scheduleV1`, `CommandCodec.encode/decode*V1`, `V1ScheduleResolver`, `RetryPolicyCatalog`, `V1CommandResolutionException`, `V1ScheduleBinding`, `KeyCodec.idV1ScheduleBinding`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `PayloadReference.fromDescriptor`, `DelayShard`, `ScheduleIntentV1Test`, `ClientCommandBodyV1Test`, `PreparedCommandV1Test`, `V1ScheduleBindingTest`, `ProfileBindingControlStateTest`, `PayloadReferenceTest`, `DelayShardTest`; exact field order/oneof/presence, common fields 1–3, outer/body message identity/type/retry binding, tuple-derived Lane, resolver-required fail-closed route snapshot, source-position Retry Policy semantic ref/hash lookup and ordering guard, inline/object payload projection including absent optional etag, source-ordered Profile first-binding activation/close gating, canonical body/tuple binding persistence and reopen checks are covered; immutable Profile/Retry publication/catalog authority, signed control target authority, full historical binding and production adapter authority remain pending |
| Registry-shaped `PrepareLargeScheduleV1` | Implemented (canonical codec plus resolver/catalog-backed reservation apply; external authority pending) | `PrepareLargeScheduleBodyV1`, `CommandBodies.prepareLargeV1/decodePrepareLargeV1`, `V1ScheduleResolver`, `RetryPolicyCatalog`, `V1ScheduleBinding`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `DelayShard`, `ClientCommandBodyV1Test`, `V1ScheduleBindingTest`, `ProfileBindingControlStateTest`, `DelayShardTest`; common fields 1–3, intent-without-payload, expected length/SHA-256, reservation TTL/trust-set reference, resolver-derived Lane, source-position Retry Policy semantic lookup and ordering guard, source-ordered Profile first-binding gate and atomic binding sidecar persistence are covered; immutable Profile/Retry/trust-set publication/catalog authority, Object Store handle/attestation and full reservation binding migration remain pending |
| Registry-shaped `CommitLargeScheduleV1` | Implemented (canonical body plus typed proof/outer-identity apply; external authority pending) | `PayloadCommitProofView`, `PayloadCommitProofV1`, `CommitLargeScheduleBodyV1`, `CommandBodies.commitLargeV1/decodeCommitLargeV1`, `PreparedCommand.commitLargeV1`, `CommandCodec.encode/decode*V1`, `DelayShard`, `CommitLargeScheduleBodyV1Test`, `DelayShardTest`; common fields 1–3, reservation identity, typed Object Store Profile/tenant scope, optional etag presence, proof-id/signature digests, strict canonical field order, outer/body identity fencing and trust-set verification are covered; source-position trust-set authority, Object Store attestation/ownership and full reservation binding migration remain pending |
| Registry-shaped `CancelV1` / `RescheduleV1` | Implemented (canonical body/outer identity plus local runtime application) | `MessagePreconditionV1`, `CancelCommandBodyV1`, `RescheduleCommandBodyV1`, `CommandBodies.cancelV1/decodeCancelV1`, `CommandBodies.rescheduleV1/decodeRescheduleV1`, `PreparedCommand.cancelV1/rescheduleV1`, `CommandCodec.encode/decode*V1`, `DelayShard`, `RemainingClientCommandBodyV1Test`, `RemainingPreparedCommandV1Test`, `DelayShardTest`; independently present generation/state-version preconditions are checked against current messages/reservations and persisted through the same atomic cancellation/reschedule transition, with common fields 1–3, outer/body identity/type/retry binding, canonical timing and strict field order covered |
| Registry-shaped `RetryPolicySemanticV1` | Implemented (canonical value/hash codec plus source-position catalog gate and deterministic local history) | `RetryPolicySemanticV1`, `RetryPolicyRefV1`, `RetryPolicyCatalog`, `InMemoryRetryPolicyCatalog`, `UncertainPolicyV1`, `DlqExportModeV1`, `RetryPolicySemanticV1Test`, `PolicyCatalogTest`, `DelayShardTest`; fields 1/4–18, domain-separated semantic hash, typed ref projection, uncertain/DLQ presence rules, FIFO possible-duplicate guard and checked backoff budgets, exact catalog ref/hash matching, source publication visibility fences and stable pre-apply rejection for an unavailable source-position value are covered; authenticated activation authority, durable historical retention and full retry binding remain pending |
| Registry-shaped `PayloadProofTrustSetSemanticV1` | Implemented (canonical verifier-key/hash codec, exact local catalog and source-ordered marker projection; authority pending) | `PayloadProofVerifierKeyV1`, `PayloadProofTrustSetSemanticV1`, `PayloadProofTrustSetRefV1`, `PayloadProofTrustSet.fromSemantic`, `PayloadProofTrustSetControlState`, `InMemoryPayloadProofTrustSetCatalog`, `PayloadProofTrustSetSemanticV1Test`, `PayloadProofTrustSetControlStateTest`, `PolicyCatalogTest`; sorted/unique Ed25519 raw keys, validity bounds, semantic hash/ref, exact local reference resolution, canonical round-trip/tamper rejection, source-time verification windows, strictly ordered activation markers, idempotent marker replay, first-seen issuance close versus historical verification, and canonical marker-state encoding are covered; authenticated source-ordered control apply, durable shard wiring and Recovery-Floor historical retention remain pending |
| NDR1 receipt frame | Implemented (queued/applied/reservation/control/native receipt/prepared payload subset) | `ReceiptFrame`, `ReceiptKind`, `CommandQueuedReceiptV1`, `CommandAppliedReceiptV1`, `PayloadReservationReceiptV1`, `PayloadProofTrustSetRefV1`, `ControlOperationReceiptV1`, `PulsarBrokerResourceIdentityV1`, `NativeCapabilitySnapshotV1`, `PulsarMetadataV1`, `NativePreparedDeliveryV1`, `NativePreparedRefV1`, `NativeDeliveryReceiptV1`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1`, `WireCommandIngressAdapter`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter`, `NativeSubmissionAdapterTest`; registry zero-payload vector, canonical PreparedCommandRef/ProtocolTuple, Kafka/Pulsar Source Position and SafeBrokerAck agreement, queued-to-applied digest/source fencing, barrier-gated applied-frame emission, apply-status/message-field consistency and generation/state-version/binding presence fencing, object-store profile/object identity/trust-set pinning, control operation/scope/target/evidence/query-boundary pinning, Pulsar-only native target/ACK identity matching, signed capability snapshot canonical digest/Trusted-UTC binding/Ed25519 verification, strict optional Pulsar metadata and key-sorted unique property encoding, native snapshot projection/expiry/attestation matching, submission-hash and prepared-ref byte projection, capability bits and physical-attempt/digest checks, query boundary/capability/physical-attempt/digest checks, Kafka queued ACK and definitive rejection proof projection, Pulsar batch-aware queued ACK and guard rejection proof projection, native persisted/guard-rejection/uncertain/local-fence projection, and flags/length/kind/CRC/Base64url rejection tests; durable guard/credential protection and real Broker response transports remain pending |
| Query response closed unions | Implemented (wire codec plus bounded local bridge) | `ProfileRefV1`, `PublicDestinationBindingViewV1`, `PublicEvidenceRefV1`, `CheckpointSummaryV1`, `CheckpointCatalogResultV1`, `CheckpointControlResultV1`, `LaneControlResultV1`, `ShardControlResultV1`, `ProfileControlResultV1`, `QuotaControlResultV1`, `MessageControlResultV1`, `RouteControlResultV1`, `SecretRotationResultV1`, all Registry Message/Command view classes, `CommandQueryResponseV1`, `MessageQueryResponseV1`, `ControlOperationQueryResponseV1`, `CurrentControlOperationV1`, `ControlTypedResultV1`, `PublicQueryErrorV1`, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queryCommand/queryMessage`, `ProtocolCodecTest`, `CheckpointCatalogResultV1Test`, `CheckpointControlResultV1Test`, `ControlResultCodecTest`, `ControlOperationQueryResponseV1Test`, `EmbeddedDelayServiceTest`; exact branch tags/field order, Source Position barrier ordering, state/status agreement, command-view optional presence fencing, safe NFC alias and payload/DLQ/evidence enum checks, canonical checkpoint-catalog shard/Floor/sorted-summary validation, checkpoint-control identity validation, all nine control-result branch field/presence/identity codecs with strict branch-to-payload dispatch and round-trip/rejection vectors, fixed-source queued-receipt barrier and retention projection, and canonical Control Operation CURRENT/error/target/revision/typed-result projection; production receipt routing, authorization-safe lookup, source-derived retention, Oxia ownership, durable control-operation query state and observability remain pending |
| System Mutation envelope, type registry, canonical hash/ID and Ed25519 signature | Implemented (bounded control plus admission/expiry/outcome/evidence/claim-result/resource-retire/delete-confirmed subset) | `SystemMutationType`, `SystemMutation`, `ShardSubjectV1`, `SystemMutationBodyCodec`, `ApplyShardControlBody`, `ControlRef`, `ControlReasonKindV1`, `ControlReasonV1`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `PayloadProofTrustSetActivatePayloadV1`, `PayloadProofIssuanceClosePayloadV1`, `PayloadProofTrustSetControlState`, `PayloadProofTrustSetControlCatalog`, `PublishAdmissionBody`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `PublishOutcomeBody`, `ClaimResultBody`, `ResourceRetireIntentBody`, `ResourceRetireIntentRecord`, `ResourceDeleteConfirmedBody`, `ResourceDeleteConfirmedRecord`, `TrustedUtcIntervalEvidence`, `SystemMutationResult`, `AuthorIdentity`, `ClaimRecord`, `GenerationRuntimeIndex`, `DelayShard`, `ProtocolCodecTest`, `ShardSubjectV1Test`, `PublishAdmissionBodyTest`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `ResourceRetireIntentBodyTest`, `ResourceDeleteConfirmedBodyTest`, `GenerationRuntimeIndexTest`, `PayloadProofControlPayloadV1Test`, `ProfileBindingControlStateTest`, `PayloadProofTrustSetControlStateTest`, `DelayShardTest`; canonical owner/control/fence/service branches, strict canonical ShardSubject route/partition decoding shared by envelope and body checks, body fields 1–3, required/optional operation fields, wire widths, bool bounds, canonical nested bytes, durable `dedupe_cf/SYSTEM_MUTATION`, explicit signature verification, source-ordered Profile activation/close and trust-set activation/issuance-close with semantic-reference checks and atomic marker-state persistence/reopen, source-ordered Lane PAUSE/RESUME/BREAK/CLOSE with ControlRef/identity/incarnation/CAS, close-policy/acknowledgement checks and atomic Claim/READY rollback, source-ordered TIME_FENCE watermark and reservation-expiry overlay/materialization, source-ordered `EXPIRE_GENERATION_V1`, `PUBLISH_ADMISSION_V1` descriptor/Ready Certificate/Claim identity projection, replay-stable timeline-key/semantic-digest/counter/obligation-set preconditions, checked attempt-number and uncertain-retry counter projection, definitive `PUBLISH_OUTCOME_V1/NOT_PUBLISHED` disposition/retry-shape subset, verified published/not-published `EVIDENCE_RESOLUTION_V1` transition subset, replay-stable permanent `CLAIM_RESULT_V1` ClaimPrecondition/timeline terminalization subset, closed ExactResourceIdentity/ProtectionSet parsing plus registered logical-identity verification and atomic `gc_cf/TASK` retire-intent persistence, exact RetireIntentRef/DeleteOutcome/ExternalDeleteEvidence matching and source-ordered local tombstone persistence, local durable `SCHEDULED -> CLAIMED -> revoke/Admission/ClaimResult/Cancel/Reschedule/expiry` transitions, and v4 `id_cf/MESSAGE` runtime-index writes are covered; immutable Profile/catalog and authenticated source control authority, source-protected signing-key trust/ACL, immutable Oxia target registration, Recovery Floor barriers, full ActiveLaneState persistence, obligation-set quota/recovery reconciliation and full Claim materialization/recovery model remain pending |
| Kafka/Pulsar source order token and source identity fencing | Implemented (core codec) | `SourcePosition.sourceOrderToken`, physical-resource comparison guard, `ProtocolCodecTest`; broker assignment/barrier adapters pending |
| Pinned Kafka/Pulsar command ingress outcome mapping | Implemented (transport SPI plus Kafka/Pulsar wire projection) | `PinnedKafkaCommandIngress`, `PinnedPulsarCommandIngress`, `WireCommandIngressAdapter`, `WireIngressOutcomeSupport`, `KafkaIngressResource`, `PulsarIngressResource`, `PulsarSendRequest`, `PulsarSendResult`, `AdapterIngressTest`; Kafka topic UUID and Pulsar resource token plus physical topic creation identity are carried at the request boundary, persisted Pulsar results fence all pinned identity fields, and both adapters can project queued/definitive-proof/uncertain NDR1 outcomes with evidence fail-closed; concrete pinned request transports, authenticated production rejection classifiers and source assignment pending |
| Target publish side-effect outcome boundary | Implemented (identity-fenced transport SPI plus durable attempt/outcome subset) | `DestinationPublishAdapter`, `DestinationPublishResult`, `PinnedKafkaDestinationAdapter`, `PinnedPulsarDestinationAdapter`, `PublishAttemptLedger`, `PublishOutcomeBody`, `DelayShard`, `DestinationAdapterTest`, `DelayShardTest`; PUBLISHED results now carry and are checked against the pinned `BrokerResourceIdentityV1` and physical partition, including Pulsar creation identity; physical adapter evidence journal, full admission gate and remaining outcome/evidence mutations pending; definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable state transitions and service-authored verified evidence resolution are covered locally |
| One Delay Shard = one RocksDB DB | Implemented | `ShardStore`, `ShardStoreTest` |
| Worker DB/checkpoint resource limits | Implemented (config-envelope plus runtime shard/DB/restore slots and local placement scorer) | `ShardStoreConfig`, `SharedRocksDbResources`, `ShardStore`, `WorkerResourceEnvelope`, `WorkerCapacityAdmission`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `WorkerResourceEnvelopeTest`, `WorkerCapacityAdmissionTest`, `WorkerPlacementPolicyTest`, `ShardStoreTest`; checked memory/FD/disk cross-bucket inequalities fail closed before resource creation, logical `maxOwnedShards` and physical `maxOpenShardDbs` slots are tracked independently, checkpoint restore/download staging has an independent bounded slot released after atomic installation, shared RocksDB resources refuse premature close, local Worker admission sums distinct shard `committed` vectors plus fixed/transition demand once with checked per-dimension arithmetic, and the local placement seam rejects over-capacity candidates before applying dominant-resource/load scoring with stale-telemetry penalty, minimum residence, hysteresis and movement cost; live JVM/cgroup/rlimit probes, WAL/SST/temp accounting, per-work-class reserves, cooperative assignment and Oxia placement capacity authority remain pending |
| Seven application CFs plus empty `default` CF | Implemented | `ShardStore` descriptor validation |
| Shard identity and local Store Incarnation validation | Implemented | `StoreMetadata`, `ShardStoreTest` |
| Synchronous atomic WriteBatch | Implemented | `ShardStore.write`, `ShardStoreTest` |
| Native RocksDB checkpoint creation | Implemented | `ShardStore.createCheckpoint`, `ShardStoreTest`; checkpoint creation is staged under the same-filesystem `checkpoint-tmp` namespace, rejects an existing target, and installs only through an atomic rename with failed-stage cleanup |
| Checkpoint file inventory and canonical manifest projection | Implemented (local CAS projection, bounded upload coordinator and deterministic schedule seam; external publication pending) | `CheckpointFileInventory`, `CheckpointManifest`, `CheckpointManifestJson`, `CheckpointResourceV1`, `CheckpointUploadStateV1`, `CheckpointUploadIntentV1`, `CheckpointUploadIntentStore`, `CheckpointUploadAdapter`, `CheckpointUploadRequest`, `CheckpointUploadCoordinator`, `CheckpointScheduler`, `CheckpointManifestTest`, `CheckpointResourceV1Test`, `CheckpointUploadIntentV1Test`, `CheckpointUploadIntentStoreTest`, `CheckpointUploadCoordinatorTest`, `CheckpointSchedulerTest`; inventory streams SHA-256 over each file without loading an SST into heap and rejects symbolic links before restore/copy, while the manifest decoder enforces the closed field order/types, Kafka/Pulsar typed `EvidenceCursorV1` branches, strict cursor identity ordering and byte-identical canonical JSON round trip; the published-manifest Object Store identity and upload-intent state branches have closed canonical codecs, and the local coordinator verifies exact pending intent, deadline, shard/lineage/owner/store/parent identity, complete local file inventory, Worker upload slot and returned manifest length/SHA-256 before PENDING_UPLOAD -> PUBLISHED revision CAS; the local scheduler validates interval/jitter, spreads owned shards deterministically, and fences duplicate in-flight claims; an exact PUBLISHED successor is reread after response loss without another adapter call, while real Oxia lease/session/catalog CAS, Object Store upload/attestation/publication and reaping/quiescence remain pending |
| Checkpoint restore into a new Store Incarnation | Implemented (local manifest/catalog-validated path) | `ShardStore.restoreFromCheckpoint`, `CheckpointManifest.decodeCanonicalJson`, `ShardStoreTest`; raw canonical manifest bytes are decoded and catalog-validated before local file verification, then installed as a new Store Incarnation; Oxia Recovery Pin/Floor CAS and source replay pending |
| Recovery catalog, lineage and Floor selection | Implemented (typed local codecs, typed cursor-dominant Floor projection, manifest-bound evidence cursors, upload-intent-bound publication and bounded in-memory pin authority; Oxia CAS pending) | `RecoveryFloorRefV1`, `RecoveryCandidateRefV1`, `RecoveryPinV1`, `EvidenceCursorV1`, `RecoveryCatalog`, `RecoveryCatalogAuthority`, `OxiaRecoveryCatalog`, `RecoveryFloor`, `RecoveryFloorRefV1Test`, `RecoveryCandidateRefV1Test`, `RecoveryPinV1Test`, `EvidenceCursorV1Test`, `RecoveryCatalogTest`; the typed references canonically bind lineage/checkpoint/manifest, catalog generation, Source Position, mutation sequence, sorted evidence cursors, candidate branch and session identity digest; the local catalog still binds one shard, rejects non-zero genesis lineage, enforces floor ancestry, requires the Floor cursor set to byte-match the candidate manifest and then enforces same-generation cursor dominance, exposes candidate validation/selection and `proveFloorCoverage`, requires a PUBLISHED `CheckpointUploadIntentV1` plus exact manifest/object/owner/store identity for the local publication projection, and supports one exact active-pin create/idempotent-reread/release projection with typed Floor equality checks; durable Oxia Owner Lease/session and catalog/Floor CAS, real Object Store publication/attestation, and evidence-cursor retention/dominance enforcement remain pending |
| Command applied/rejected state machine | Implemented (embedded core) | `DelayShard`, `DelayShardTest` |
| DUE/ORDERED/READY/EXPIRY timeline namespaces | Implemented (embedded core subset) | `DelayShard`, `MessageRecord`, `TimelineWorkRef`, `GenerationRuntimeIndex`, `ClaimRecord`, `ReadyIndexValue`, `KeyCodec`, `GenerationRuntimeIndexTest`, `DelayShardTest`; READY key/value, laneVersion fencing, retry eligibility for unordered definitive retry, canonical timeline semantic/instance digests, v4 runtime-index persistence, atomic affected-lane updates, durable Claim removal/restoration including source-ordered Lane Pause rollback, fenced rebuild/discovery, replay-stable Claim Result timeline-key/semantic-digest checks, Cancel/Reschedule fencing whenever an UNCERTAIN obligation survives a current-work projection, and the pinned-policy `UNKNOWN` scheduling plus `UNCERTAIN_RETRY` Admission subset that materializes timeline work while retaining the old obligation are covered; ControlRef validation and policy-bound timeline materialization beyond this local budget remain pending |
| `CLAIMED`/`PUBLISHING`/`UNCERTAIN` attempt ledger and obligation locator | Implemented (durable local Claim plus source-ordered attempt subset) | `ClaimRecord`, `PublishAttemptLedger`, `AttemptObligationRef`, `GenerationRuntimeIndex`, `PublishAdmissionBody`, `PublishOutcomeBody`, `RetryPolicyCatalog`, `DelayShard`, `DelayShardTest`; local Claim sequence/key/value and exact precondition/instance digest are persisted atomically, registry-shaped runtime index and canonical obligation-set digest are persisted with v4 Message records, source-ordered `PUBLISH_ADMISSION_V1` checks replay-stable timeline key/semantic/counter/obligation projections and descriptor attempt number, including the `UNCERTAIN_RETRY` source-work branch, reconstructs the same PUBLISHING attempt when the reversible Claim is absent but source state matches, retains admission counters across definitive retry timelines, and when the catalog is supplied revalidates the pinned immutable policy budgets at Admission, uncertain retry, and reopen; shard activation now performs bounded bidirectional reconciliation between every current/terminal runtime obligation ref and its exact PUBLISHING/UNCERTAIN ledger, and between every live ledger/Claim and the current Message branch, failing closed on an orphan, missing counterpart, persisted total-admission overflow, or terminal/runtime summary mismatch; source-ordered `PUBLISH_OUTCOME_V1` UNKNOWN atomically migrates to UNCERTAIN, can materialize the pinned-policy `UNCERTAIN_RETRY` timeline subset without consuming the retry counter, verified-success closes the ledger, definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable outcomes atomically requeue, terminalize, or block the lane, and late outcomes from either the current or an older generation settle only the exact ledger/terminal summary while preserving terminal decision state and monotonically raising duplicate risk for verified success; service-authored verified `EVIDENCE_RESOLUTION_V1` can close or requeue the exact UNCERTAIN ledger; full Profile/Adapter/evidence/capacity validation, immutable RetryPolicy publication/authority, multi-obligation terminal-summary lifecycle beyond current-generation settlement, uncertain-retry ControlRef/policy enforcement beyond the local automatic budget, obligation-set quota/recovery accounting beyond local reconciliation, Recovery Floor/source replay and full Claim materialization/recovery semantics remain pending |
| Terminal generation history | Implemented (current/older-generation open-obligation summary and late-settlement subset) | `TerminalGenerationRecord`, `ClaimRecord`, `DelayShard`, `DelayShardTest`, `TerminalGenerationRecordTest`; Claim Result, publish outcome, expiry and command terminalization persist the exact remaining obligation refs and duplicate-risk projection in a versioned terminal record, activation reconciles both current and older-generation summaries against their exact inflight ledgers, and late verified or definitive not-published outcomes remove only their exact old ledger/ref without changing terminal status/code/time; legacy v1 terminal records decode as empty summaries; older-generation callbacks after full Dead Letter Replay, Replay retention and guarded GC remain pending |
| Large-payload reservation/proof/commit | Implemented (embedded core plus typed wire proof/response subset) | `LargeScheduleIntent`, legacy `PayloadCommitProof`, `PayloadCommitProofView`, `PayloadCommitProofV1`, `PayloadReservation`, `OpaquePayloadUploadHandleV1`, `PayloadUploadHandleResponseV1`, `PayloadAttestationResponseV1`, object-backed `MessageRecord`, `DelayShardTest`, `CommitLargeScheduleBodyV1Test`, `ProtocolCodecTest`; Prepare/Commit reservation quota, source-ordered TIME_FENCE overlay, bounded `RESERVATION_EXPIRY` discovery/materialization, guarded local quota release, bounded opaque upload handle, typed Object Store proof Profile/tenant-scope/optional-etag/proof-id/signature validation, fixed payload-scoped error branches and typed attestation/commit response round-trips are covered; Object Store handle issuance/attestation ownership, source-ordered trust controls, fence-key history and guarded GC remain pending |
| Source assignment, typed Activation Barrier and Owner Lease | Implemented (local mixed command/System Mutation replay seam; production authority pending) | `SourceAssignment`, `SourceReplayEntry`, `SourceReplayRecord`, `SourceReplayMutation`, `SourceReplayOutcome`, `KafkaActivationBarrier`, `PulsarActivationBarrier`, barrier-gated `OwnedDelayShard`, `OwnerLease`, `OwnerLeaseContext`, `OwnerLeaseStore`, `OxiaOwnerLeaseStore`, `OwnerLeaseTest`, `OxiaOwnerLeaseStoreTest`; catch-up now requires an explicit non-zero assignment identity/epoch bound to the typed barrier, assignment/barrier equality compares array-backed resource and guard identities by value, Pulsar barrier resource/guard identities reject all-zero placeholders, every catch-up and post-activation apply record is checked against the typed physical source identity before monotonic replay, including empty Kafka/Pulsar barriers, and the unified `replay` seam applies mixed Command/System Mutation entries in one source order through the shard's atomic WriteBatch before advancing the cursor (the type-specific methods remain compatibility conveniences); Pulsar replay/catch-up/apply paths additionally require a positive guarded source-connection generation and exact resource-guard attestation digest; context-bound lease acquisition carries assignment identity plus assignment epoch and exact session identity, renewal preserves them and the lifecycle state, stale state transitions fail CAS, and the activation overload requires the authority to CAS the same lease to `ACTIVE_FOR_COMMANDS` before opening the local gate; real Kafka/Pulsar consumer replay, Oxia session/ephemeral records, broker assignment/guard and production activation transaction remain pending |
| Queued vs applied client outcomes | Implemented (embedded core) | `EmbeddedDelayService.queuedReceiptV1`, `appliedReceiptV1`, `enqueueOutcomeV1`, `queryCommand`, `queryMessage`, `EmbeddedDelayServiceTest`; queued receipt stays distinct from applied result, the closed managed enqueue union preserves queued/definitely-not-queued/uncertain states, applied frame is emitted only after the source barrier and retains the queued digest, pending is source-barrier based, full/compact/evidence-expired branches are bounded, and message projections require caller-supplied safe policy inputs; real Broker response adapters and production routing remain pending |
| Destination Lane gate/readiness projection | Implemented (core plus closed same-key terminal branch and typed ActiveLaneStateV1/quota/certificate/barrier codecs) | `LaneRecord`, `LaneRecordEnvelopeV1`, `ActiveLaneStateV1`, `LaneCircuitStateV1`, `LaneRuntimeBlockReasonV1`, `LaneQuotaUsageEntryV1`, `LaneQuotaUsageMapV1`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `LaneRetirementProgressV1`, `LaneTerminalGuardV1`, `LaneRecordTest`, `LaneRecordEnvelopeV1Test`, `ActiveLaneStateV1Test`, `LaneQuotaUsageMapV1Test`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `LaneTerminalGuardV1Test`, `ApplyShardControlBody`, `ControlRef`, `DelayShard`, `DelayShardTest`; schedulable lanes maintain a versioned READY head and readiness/gate CAS transitions remove/recreate it atomically; source-ordered signed PAUSE/RESUME/BREAK/CLOSE applies exact Lane incarnation/control-version fencing, strict-lane acknowledgement checks and atomically revokes/restores reversible Claims; the same `meta_cf/LANE` key now persists a closed ACTIVE/TERMINAL branch, and the bounded local retirement path conservatively proves no current message/timeline/inflight work before atomically installing a tuple/profile/source/digest-checked terminal guard that survives reopen and rejects reuse; the Registry-shaped ActiveLaneStateV1 codec now covers tuple identity/digest, gate/readiness block-reason rules, 17-dimensional lane charge, circuit/backoff counters, ready-key digest, canonical ReadyCertificateV1 wrapper and retirement progress, while the per-Lane quota entry/map codec enforces sorted identity keys and usage/map digests; `LaneRecordEnvelopeV1` now emits typed field-10 ACTIVE values as direct canonical `ActiveLaneStateV1`, rejects malformed typed values instead of downgrading them to legacy, and still reads the old adapter sub-message. Legacy `DelayShard` persistence remains on the compatibility adapter because current Schedule inputs do not carry the immutable Profile/tuple/certificate inputs required for a lossless cutover; quota-map revision coupling, Oxia target registration and Recovery-Floor/retention guard remain release blockers |
| Destination Lane isolation and bounded weighted DRR | Implemented (scheduler core plus fenced READY recovery seam) | `LaneScheduler`, `PersistentLaneScheduler`, `ReadyIndexValue`, `TimelineEntry`, `LaneSchedulerTest`, `SchedulerProjectionsV1Test`; lane-local work is rebuilt from bounded `timeline_cf/READY` plus `meta_cf/LANE`/`id_cf/MESSAGE` identity checks, recomputes the exact timeline key and digest, verifies the timeline value, stale/orphan/multiple-head projections fail closed, and the recovered heads replace the active ring and queues before scheduling resumes; production Lane certificate/activation authority remains pending |
| Persistent scheduler fairness counters | Implemented (local closed projection plus rotating READY cursor) | `PersistentLaneScheduler`, `SchedulerProjectionsV1`, `OwnerIdentityV1`, `LaneSchedulerTest`, `SchedulerProjectionsV1Test`; all five `meta_cf/SCHEDULER` values are written in one batch, persisted successor order is restored without re-adding discarded lanes, and fenced rebuild advances the bounded rotating discovery cursor/wrap generation while preserving Lane incarnation/version and message-generation checks; full Oxia-fenced activation and typed ActiveLaneState cutover remain release blockers |
| Worker-to-shard-to-lane bounded DRR | Implemented (core snapshot plus READY-aware outer filtering and local placement scoring) | `WorkerScheduler`, `WorkerSchedulerTest`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `WorkerPlacementPolicyTest`; outer DRR persists/restores deficit, cursor/round and blocked-shard isolation state, visits only shards with at least one schedulable Lane head, and the local placement seam hard-filters full committed capacity/DB slots before deterministically scoring unequal observed load; durable outer scheduler projections, broker assignors, Oxia desired-placement plans and authoritative placement weights remain pending |
| Closed Stable Code registry | Implemented | `StableCode`, `FailureStageV1`, `RetryabilityV1`, `StableErrorV1`, `ProtocolCodecTest`; all Registry stable codes including activated-protocol/capacity-fence codes, code-derived retryability, retry-at presence, mutually exclusive managed/native prepared refs, bounded diagnostic code and canonical round-trip/rejection checks |
| Non-persistence proof and Broker identity | Implemented (codec boundary) | `KafkaBrokerResourceIdentityV1`, `PulsarBrokerResourceIdentityV1`, `BrokerResourceIdentityV1`, `NonPersistenceProofKindV1`, `NonPersistenceProofV1`, `ProtocolCodecTest`; closed Kafka/Pulsar identity branches, kind-specific attempt/resource/request/response presence, pre-ownership evidence prohibition, adapter-proof version and fields 1–7 digest; authenticated adapter rejection classifiers and real non-persistence attestations remain pending |
| Managed/native submission outcome unions | Implemented (wire codec plus embedded/Kafka/Pulsar managed and native transport-SPI bridges) | `EnqueueOutcomeKindV1`, `DefinitelyNotQueuedV1`, `EnqueueUncertainV1`, `EnqueueOutcomeMessageV1`, `SubmissionOutcomeKindV1`, `NativeDefinitelyNotQueuedV1`, `NativeEnqueueUncertainV1`, `SubmissionOutcomeMessageV1`, `PreparedSubmissionV1`, `PreparedSubmissionAdapter`, `EmbeddedDelayService.enqueueOutcomeV1`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter.submit`, `WireIngressOutcomeSupport`, `ProtocolCodecTest`, `EmbeddedDelayServiceTest`, `AdapterIngressTest`, `NativeSubmissionAdapterTest`; closed branch tags, exact prepared/ref hash binding, retryability and physical-attempt checks, canonical managed NDL1/native prepared branches, exact managed/native branch dispatch without reselection, embedded queued/definite/uncertain projection, Kafka/Pulsar queued ACK and authenticated definitive-rejection proof projection, native capability signature/expiry and pinned-resource checks before transport ownership, native receipt/guard-proof/uncertain/local-definite projection, and conservative downgrade when evidence is absent; durable guard/credential protection, authenticated Producer ownership and production response evidence remain pending |
| Hard shard quota admission | Implemented (core subset) | `ShardQuota`, `OutcomeReserveUsage`, `PublishAdmissionBody.ChargeVector`, `CapacityDimensionV1`, `CapacityVectorV1`, `CapacityGrantV1`, `QuotaGrantRefV1`, `ShardCapacityEnvelopeV1`, `DelayShardConfig`, `DelayShard`, `KeyCodecTest`, `DelayShardTest`, `CapacityVectorV1Test`, `ShardCapacityEnvelopeV1Test`; shard-local outcome reserve records/bytes are durable and source-ordered, with `ADMISSION_CAPACITY_GATED` Claim rollback, admission charge and definitive/verified settlement release in one WriteBatch; an activation-supplied immutable envelope additionally binds the exact outcome grant and persists the full 66-dimensional outcome usage under registered `meta/CONTROL_RESERVE` keys, with restart/rotation checks; class-3/4/5 reserve/release now has checked grant-bounded local persistence, while source-writer charge integration, class-6 system-writer schema, multi-shard placement/Oxia authority and GC accounting remain pending |
| Kafka/Pulsar ingress and target adapters | In progress (identity-pinned transport SPI) | release blocker until concrete pinned transports, authenticated non-persistence classifiers/proofs, target publish/evidence channels, production response evidence and real-broker tests exist |
| Recovery Set/Floor, catalog and restore replay | In progress (local catalog/Floor subset) | release blocker; Oxia catalog/session pin, immutable publication, source/evidence replay and activation CAS remain |
| Large payload, quota grants, control reserve and GC | In progress (reservation/commit, shard hard-quota, 66-dimensional vector/grant/envelope codec, bound outcome-reserve usage, checked class-3/4/5 reserve arithmetic, reserve projection drift guard, retire-intent, delete-confirmed and catalog-backed local compaction subsets) | release blocker; `CapacityVectorV1`/`CapacityGrantV1`/`QuotaGrantRefV1`/`ShardCapacityEnvelopeV1` enforce the closed dimension registry, zero-explicit ordered amounts, grant/envelope digests, logical charge projection, component-grant projection and checked arithmetic locally. The bound `DelayShard` path now persists class-1 envelope identity and class-2 exact outcome usage, exposes synchronous grant-bounded reserve/release for classes 3–5, scans classes 3–6 during activation and rejects stale/unknown or over-capacity projections instead of ignoring them; the legacy `OutcomeReserveUsage` projection remains as a compatibility aggregate. `ResourceRetireIntentBody`/`ResourceRetireIntentRecord` plus `ResourceDeleteConfirmedBody`/`ResourceDeleteConfirmedRecord` provide canonical source-ordered `gc_cf/TASK` intent/tombstone persistence with applied mutation sequence; `RecoveryCatalogAuthority`/`OxiaRecoveryCatalog` plus `ResourceGcGuard` enforce local ancestry/source/sequence coverage, `DelayShard.compactResourceDeleteConfirmation` removes only a covered local tombstone, and local payload/checkpoint version/etag comparison is enforced, but source-writer operation charging, class-6 system-writer grant/schema, Object Store/Oxia publication, multi-shard grant placement/authority, real provider delete attestation/ownership, durable catalog/Floor barrier, Lane terminal guard and full guarded GC remain |
| Query, control operations, DLQ and observability | In progress (wire unions plus bounded local receipt/barrier/DLQ/SLO bridge) | `MessageQuerySnapshot`, `ReservationQuerySnapshot`, `DlqExportRecord`, `DlqExportResultBody`, source-ordered `DelayShard` DLQ export apply, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1/queryCommand/queryMessage/registerControlOperation/advanceControlOperation/queryControlOperation`, `ControlOperationQueryResponseV1`/`CurrentControlOperationV1`/`ControlTargetStateViewV1`/`ControlTypedResultV1`, `ControlOperationAuthority`, `InMemoryControlOperationAuthority`, `OxiaControlOperationAuthority`, `SloObjectiveV1`/`SloSampleEventIdentityV1`/`SloSampleStartV1`/`SloSampleFinalV1`/`SloObservationOutboxV1`/`SloObservationOutboxStore` and closed SLO enum/time codecs, all V1 Command/Message query view codecs, `DelayShardTest`, `DlqExportRecordTest`, `DlqExportApplyTest`, `ControlOperationQueryResponseV1Test`, `ControlOperationAuthorityTest`, `SloObjectiveV1Test`, `SloObservationOutboxV1Test`, `ShardStoreTest`, `EmbeddedDelayServiceTest`, `ProtocolCodecTest`; Dead Letter terminalization writes the deterministic `terminal_cf/DLQ_EXPORT` `NOT_CONFIGURED` record atomically; configured local outboxes can now apply signed `DLQ_EXPORT_RESULT_V1` transitions with checked attempt succession, PENDING next-attempt advancement, terminal monotonicity and mutation dedupe; the Control Operation query response union now has canonical CURRENT/error/target-marker/revision/typed-result wire validation, while the local authority and embedded entry points add receipt-bound idempotent registration, strict revision CAS and fixed retention-bound queries; SLO objective digest, direction/unit/population/exclusion semantics, all 14 objective branch tags/common identity field-shape checks, Start threshold timeout, exact sample/start/final digests, start matching, `meta_cf/SLO_OUTBOX` key/value-envelope persistence and conservative AT_MOST/AT_LEAST Final merge are locally covered. Real target/evidence adapter ownership, production receipt/barrier routing, authorization-safe binding/evidence/retention lookup, durable Oxia control-operation state/routing, crash reconstruction, collector merge/export and observability remain release blockers |
| Real-service, chaos, benchmark, soak and upgrade evidence | Not started | release blocker |

## Verification command

Use the checked-in Gradle Wrapper and an isolated cache on hosts where the
default Gradle native cache is not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
