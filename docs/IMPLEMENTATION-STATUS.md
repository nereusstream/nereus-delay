# V1 Implementation Status

Spec revision: `V1-FROZEN-2026-08-01`

This file records implementation evidence. It does not relax or replace the
normative requirements in [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md),
the [`V1 Protocol Registry`](V1-PROTOCOL-REGISTRY.md), or the Accepted ADRs.
An unchecked item is not an implementation permission; it is a release blocker.

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
semantic reference and matching credential Head are present; shard-local
source activation/deprecation markers still run before this resolver gate.

Control Operation request values now close the Registry §6.3 operation-kind
enum, all fifteen request branches and their canonical outer oneof dispatch,
including acknowledgement/evidence presence matrices and quota transfer-plan
references. Prepared operations now also enforce the closed local
operation-kind/target-kind/presence matrix; authenticated actor/resource
authority, source-ordered System Mutation construction/registration and Oxia
operation state are still pending. `ControlTargetMutationBindingV1` now closes
the local pre-registration binding check once a caller has constructed a
mutation; it does not construct the body or authenticate the external writer.

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
canonical `ChargeVectorV1` values before the encoder's decode round-trip.
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
duplicate mutation replay.
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
Floor coverage and local GC proofs likewise require canonical Source Position
bytes when the covered and required order tokens are equal; a same-offset or
same-ledger/entry/batch metadata variant cannot satisfy a retention barrier.

The local `gc_cf/TASK` readers also compare the requested resource kind,
identity hash and expected resource version with the embedded retire intent
(including the nested intent in a delete confirmation); a misplaced GC value
is rejected before compaction or query code can use it. `DelayShardTest`
covers the key/value identity fence.

Exact already-published manifests are similarly reread before generation CAS;
same-checkpoint hash drift remains an integrity conflict.

Source replay rejects a connection-generation/guard proof on Kafka positions;
that proof is reserved for the guarded Pulsar source branch.

`SourcePositionCodec` now requires every decoded Kafka/Pulsar position to
round-trip byte-for-byte through its canonical encoding; malformed UTF-8 or
replacement-character input is rejected before a position can become persisted
metadata, receipt evidence or checkpoint state. `ProtocolCodecTest` covers both
adapter branches with non-canonical UTF-8 vectors.

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

Reservation reads now also fence the `id_cf/RESERVATION` key against the
embedded reservation identity and the current shard identity. Message-based
reservation lookup uses a bounded `maxPendingMessages + 1` scan, rejects a
scan that cannot prove completeness, and fails closed when one message has
multiple reservation records. `RESERVATION_EXPIRY` discovery additionally
requires the timeline projection to byte-match the current `id_cf` record;
`DelayShardTest` covers misplaced-key, duplicate-reservation and stale-expiry
projection recovery paths.

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
The same exact-key requirement is enforced by `rebuildReadyIndexes` while it
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

The embedded Kafka ingress now checks source-offset exhaustion before creating
the next queued position and advances the counter only after the position has
validated successfully. `EmbeddedDelayServiceTest` covers the
`Long.MAX_VALUE` boundary, so a failed enqueue cannot wrap the in-memory
offset into a negative value.

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
also verifies that a reopened service can read the applied result.

`DelayShard` now rejects negative persisted mutation and Claim sequence values
on reopen. These counters are encoded as checked non-negative u64 values;
accepting a high-bit-set value would turn corrupt metadata into a wrapped local
sequence and could poison resource-retirement or Claim identity derivation.
`DelayShardTest.rejectsNegativePersistedShardSequences` covers both sequence
metadata keys and verifies the shard fails closed before activation.
The next mutation sequence is computed through one checked helper before every
source-position WriteBatch; resource-retirement and delete-confirmed records use
the same helper. At `Long.MAX_VALUE`, the batch is rejected before any command,
result, or source-position state is written, so the in-memory and persisted
sequence cannot diverge or wrap. `DelayShardTest.mutationSequenceExhaustionFailsClosedBeforeCommandMutation`
covers the exhausted boundary.

Durable `CommandResult` and `SystemMutationResult` values now validate their
embedded Source Position through the canonical decoder at construction and
decode time. Empty, truncated or non-canonical source bytes therefore cannot
enter a result projection and wait for a later shard-specific query to reject
them; `DurableResultTest` covers both result types and the trailing-byte
adversary.

`KafkaActivationBarrier` now saturates the exclusive next-readable offset at
`Long.MAX_VALUE`; a source record at the largest representable offset no
longer causes an arithmetic exception while proving an already-reached LSO.
`SourceActivationBarrierTest.KafkaBarrierSaturatesExclusiveNextOffsetAtLongMaximum`
covers this physical-offset boundary.

`EmbeddedDelayService` applies the same saturation when reconstructing its
next Kafka offset from persisted shard state. Reopening after a record at
`Long.MAX_VALUE` now succeeds and keeps enqueue fail-closed at the exhausted
boundary; `EmbeddedDelayServiceTest.reopenedEmbeddedServiceSaturatesPersistedSourceOffsetExhaustion`
covers the restart path.

Canonical protocol writers now enforce the unsigned 32-bit range instead of
silently encoding a wider value through the `uint32` helper. The
`RetireIntentRefV1` resource-state version is emitted as `uint64` in
`ResourceDeleteConfirmedBody`, and the corresponding retire-intent fixture
uses the same width. `CanonicalProtobufTest` covers both uint32 boundaries;
`ResourceDeleteConfirmedBodyTest.intentPreservesFullUnsignedResourceStateVersion`
covers a value above 2^32.
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
The local source-history gate also rejects a same-token visibility lookup or
publication when the canonical Source Position metadata differs.

Scheduler round generations and Lane/Shard `lastServedRound` now saturate at
`Long.MAX_VALUE`, while inner scheduler byte-budget accumulation uses checked
addition; a long-running or restored scheduler cannot wrap its service-gap
evidence into a negative value. `LaneSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`
and `WorkerSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`
cover the two scheduler levels.

The inner and outer two-rotation visit caps now widen `ring.size() * 2` before
comparison, so a large in-memory ring cannot turn the bounded loop limit into a
negative `int` through arithmetic wrap. `LaneSchedulerTest.ringVisitLimitUsesWideArithmetic`
and `WorkerSchedulerTest.outerVisitLimitUsesWideArithmetic` cover the boundary;
this is local scheduler arithmetic evidence and does not replace the required
capacity proof for a production worker's maximum Lane/shard population.

`ProfileBindingActivatePayloadV1` and `ProfileNewBindingClosePayloadV1` now
close the Registry control branches for Profile first-binding lifecycle.
`ProfileBindingControlState` persists strictly source-ordered activation and
close markers; when any Profile markers are present, V1 Schedule/Prepare
first-binding admission is source-position gated and returns the distinct
pre-activation or post-close stable code. Exact duplicate commands still
reuse their durable first result. `InMemoryProfileCatalog` supplies exact
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
different state. `transitionOrRead` only performs response-loss rereads for a
transition allowed by that lifecycle graph, so an illegal request cannot be
turned into success by a coincidental current state. The real Oxia ephemeral
session/CAS authority remains a release blocker. Activation also rereads an
exact same-identity `ACTIVE_FOR_COMMANDS` successor after a lost transition
response, and that reread rejects a successor whose lease expiry moved
backwards. `OxiaOwnerLeaseStoreTest.transitionOrReadRejectsAResponseLossSuccessorWithShorterExpiry`
covers the monotonic-expiry fence.
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
`OwnedDelayShard.applyAuthoritatively` adds the corresponding per-command
authority reread for the post-activation path: a missing, changed, non-active,
expired or regressed-expiry Oxia lease fences before the delegate WriteBatch;
only an exact same-identity expiry extension is adopted locally.

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
failure.

`CheckpointScheduler` now provides a bounded process-local schedule for each
owned shard: interval and deterministic per-shard jitter are validated, due
claims are sorted and capped, an in-flight shard cannot be claimed twice, and
completion reschedules from the observed completion time only when the exact
claim handle returned by `claimDue` is supplied. A shard-only completion is a
fail-closed compatibility trap, so a late callback from an earlier claim cannot
reset a newer attempt; `CheckpointSchedulerTest` covers both the stale-claim
and shard-only paths. Jitter percentage calculation divides before multiplying,
so a valid percentage of a near-maximum interval does not fail on an
intermediate `long` overflow; `CheckpointSchedulerTest.largeIntervalJitterUsesCheckedWideArithmetic`
covers the accepted boundary and the rejected jitter span. It is only a local
worker scheduling primitive;
checkpoint manifests, upload intents and Oxia catalog publication remain the
durability authority.

`CheckpointManifestLimits` now provides the explicit physical guard required by
the manifest protocol: file count, individual/total file bytes, normalized path
bytes, canonical manifest bytes, evidence-cursor count and file/manifest-object
identity lengths are checked before local file hashing or provider I/O; the raw
manifest byte cap is enforced before JSON parsing, and the activated file/evidence
array bound is enforced while the canonical parser is materializing the array.
`CheckpointFileInventory`, canonical manifest decode, upload coordination and
the finite-limit restore overload all use the same limit set. Legacy no-limits
overloads remain compatibility seams and are not production activated limits.
Inventory and manifest file ordering compares normalized names by unsigned
UTF-8 bytes, matching the Registry rather than Java UTF-16 string order.
The inventory and manifest limit aggregators also convert total-byte `long`
overflow into the same fail-closed validation error instead of leaking
wrapping arithmetic (`CheckpointManifestTest.manifestTotalFileBytesOverflowFailsAsValidationError`).
Restore `copyTree` also consumes the source walk through a streaming iterator,
so validated checkpoint restore does not materialize the entire path tree again;
inventory canonicalizes and rejects path names before hashing any file, and
restore-tmp cleanup uses post-order `walkFileTree` deletion without a sorted
whole-tree list.

Shared RocksDB resources also retain checkpoint create/upload/download slot
counts and reject close while any bounded worker operation is still in flight.
Checkpoint restore/download staging now holds its own Worker-level slot across
manifest/file validation, restore-tmp copy, validation opens, and atomic
installation; it is released only after the active DB is opened or cleanup
completes. `ShardStoreTest.completeCheckpointRestoresIntoFreshStoreIncarnation`
also reacquires that slot immediately after a real restore returns, proving
the slot is released before the caller closes the restored DB.
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
`OwnerDrainCoordinator` passes it into `ShardStore.createCheckpoint`; the
identity is therefore present in the copied DB metadata, while the legacy
path without an identity remains local-only.
After `flushAndSync`, an optional `commitSourceHint` callback receives only the
last persisted `SourcePosition`; the coordinator rereads the draining lease
after that transport-owned callback before continuing.  The hint is never the
recovery authority.  It also rereads the lease after the physical final
checkpoint has been installed, before Store close or exact release, so a lease
loss during a long RocksDB checkpoint cannot make the old owner close or
release a newer owner's state. `OwnerDrainCoordinatorTest` covers this
post-checkpoint fence.
If Store close itself fails, the coordinator now fences locally and leaves the
authoritative lease in visible `DRAINING` instead of releasing a lease whose DB
shutdown was not confirmed; this preserves a safe retry boundary.
Callback quiescence and source hint commit remain caller/transport boundaries;
timeout leaves the DB and lease in visible `DRAINING` for a safe retry rather
than claiming completion. `OwnerDrainCoordinatorTest` covers success and
deadline-failure sequencing.
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
If the `ACTIVE` pointer itself names a missing or non-directory DB, restore
now fails closed instead of treating the corrupt pointer as an orphan and
overwriting it; `ShardStoreTest.restoreRejectsAnActivePointerWhoseDbIsMissing`
covers this store-integrity boundary. An absent `ACTIVE` pointer remains the
only case in which an orphan incarnation can be replaced by a new restore.
Runtime validation failures after staging begins now remove the private
`restore-tmp` tree as well as releasing the download slot; after the staged DB
is atomically moved, restore opens it through the formal active path before
writing `ACTIVE`, and pointer-install failure closes the DB and removes only
the unreferenced incarnation it owns (an unreadable pointer is preserved for
offline repair). `ShardStoreTest.failedActivePointerInstallRemovesUnpublishedDb`
covers the `ACTIVE.tmp` failure path; a pre-acquisition concurrency rejection
keeps its original bounded-resource error.
After RocksDB itself has opened, the normal shard-open path has one failure
cleanup boundary around metadata reads/decoding, format and identity checks,
and install-mode writes: every DB/Column Family handle and options object is
closed before Worker DB/owned-shard slots are released.  The malformed-metadata
reopen regression `ShardStoreTest.malformedExistingMetadataDoesNotLeaveRocksDbOpen`
proves that the same physical DB can be opened again after this failure path,
so a local validation error cannot leave a native RocksDB file lock behind.

`StoreRuntimeMetadata` now provides the remaining local `meta_cf` runtime
projection required by the design: optional `lastIngressFenceProofId` and
`lastCheckpointId`, a non-decreasing `lastOpenedOwnerEpoch`, canonically sorted
typed `evidenceCursors`, and a `cleanCloseMarker`.  The physical projection is
stored at the registered `meta/FIXED` keys 4, 6, 7, 8 and 9, each with the
fixed-key ValueEnvelope type; key 4 uses one canonical `IngressFenceState`
containing both the source-ordered close deadline and proof identity, so the
DelayShard fence and Store projection cannot overwrite one another.  Key 10 remains reserved for the compatible
control snapshot and is rejected until that snapshot is implemented.  The
Java projection and evidence-cursor array have bounded canonical encoding and
strict decode/round-trip checks.  Opening a DB validates every fixed-key value
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
an empty canonical Source Position payload.
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
into a new local Store Incarnation without merging into an open DB. The local
`checkpoint-tmp` parent is now required to be a real non-symbolic directory,
and `ACTIVE.tmp` is rejected before writing when it is a symbolic link or a
non-regular file; a failed restore/pointer install therefore cannot overwrite
an external target through a temporary path. `ShardStoreTest`
`checkpointAndActivePointerTemporaryPathsRejectSymbolicLinks` covers both
boundaries and verifies the external target bytes remain unchanged.
`CheckpointUploadCoordinator` now inventories the exact local file set before
provider I/O, requires the RocksDB `CURRENT` marker, charges the Worker upload
slot, validates returned manifest
object identity and only then advances the exact pending intent to PUBLISHED;
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
evidence value or pending identity remains a CAS conflict. This only closes
the local intent projection; quiescence, exact-version Object Store deletion,
final prefix sweep and Oxia authority remain pending.
The local REAPING transition now also requires the trusted UTC interval's
earliest bound to be at least `uploadDeadlineEpochMs`; evidence before the
deadline leaves the intent PENDING. Owner abandonment/lease-loss authority,
provider quiescence and deletion remain external blockers. The guarded
`beginReaping(..., RecoveryCatalogAuthority)` overload additionally refuses a
published catalog entry, an active pin protecting the same lineage/checkpoint,
or an unavailable catalog/pin read; `CheckpointReapingGuardTest` covers the
fail-closed branches. This is still a local necessary-condition projection,
not the atomic Oxia reaper CAS or provider-owned request horizon.
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
`ShardStore.restoreFromCheckpoint(..., catalog, pin)` now adds the exact active
RecoveryPin to the local install boundary: it validates the pin against the
candidate and rereads the current Floor-bounded ancestry plus the same pin
after staged DB validation, immediately before moving the new Store Incarnation
into place. A Floor that has advanced beyond the candidate, or a missing or
changed pin, leaves only restore-tmp state; this is a local fail-closed guard,
not the production Oxia Lease/session transaction.
When a manifest is supplied, staged restore now also compares the physical
image's persisted `lastCheckpointId`, `appliedShardLogPosition`,
`shardMutationSequence` and typed evidence-cursor projection against the exact
manifest values before install. A complete file inventory with a mismatched
runtime state is therefore rejected rather than restored as if it represented
that manifest. `ShardStoreTest.restoreWithManifestRejectsRuntimeStateDrift`
and `ShardStoreTest.catalogBoundRestoreRequiresPublishedFloorEligibleManifest`
cover the rejection and matching paths; source replay after restore and the
external catalog/object-store authority remain release blockers.

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
also a bounded key-order scan plus exact key/value sample-identity and
record-digest delete-after-ACK boundary, so an exporter can retry unchanged
bytes without accepting a mis-keyed observation or deleting a newer one. The
identity fence is covered by `SloObservationOutboxStoreTest`. This is
still local evidence only; multi-shard placement/Oxia authority,
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
`reserveSystemWriterCapacity` and `releaseSystemWriterCapacity` persist the
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
| Registry §6.3 Profile control requests | Implemented (canonical request values; authority pending) | `PublishDestinationProfileRequestV1`, `DeprecateDestinationProfileRequestV1`, `RotateEquivalentSecretRequestV1`, `ProfileControlRequestV1Test`; exact Profile envelope/binding identity, generation-1 publication, typed deprecation reason, checked equivalent-generation successor, private-reference SHA-256, attestation candidate tuple, expected binding digest/head revision and derived new binding are covered; authenticated actor/target authorization, source-ordered mutation routing, immutable catalog publication and Oxia CAS remain release blockers |
| Local immutable Profile/binding catalog seam | Implemented (exact local projection; authority pending) | `ProfileCatalog`, `InMemoryProfileCatalog`, `ProfileCatalogV1ScheduleResolver`, `ProfileCatalogTest`, `ProfileCatalogV1ScheduleResolverTest`; exact Profile semantic reference lookup, generation-1 binding/head/protection creation, checked equivalent rotation with response-loss idempotency, immutable generation retention, deprecation intent, semantic-reference collision rejection and fail-closed Schedule/Prepare resolver gating are covered; authenticated Profile publication, source-ordered activation markers, retained-generation quota and Oxia CAS remain external |
| Local Control Operation state projection guard | Implemented (monotonic local guard; source/Oxia authority pending) | `ControlOperationStateTransitionV1`, `InMemoryControlOperationAuthority`, `ControlOperationAuthorityTest`; closed operation and target-marker transition graph, immutable target-index set and target-revision monotonicity are enforced before local CAS, and the revision successor check rejects `Long.MAX_VALUE` before wraparound; source-ordered mutation application, durable operation state and Oxia CAS remain release blockers |
| Control Operation initial projection and uint32 target index | Implemented (local protocol projection) | `PreparedControlOperationV1.initialCurrentOperation`, `ControlTargetStateViewV1`, `ControlOperationStateTransitionV1Test`; revision-one PENDING target states cover every Prepared target and preserve the full 0..0xffffffff target-index range in canonical bytes |
| Control registration receipt/current projection | Implemented (local value pairing; Oxia CAS pending) | `ControlRegistrationProjectionV1`, `ControlRegistrationProjectionV1Test`; receipt identity/scope/request target snapshot, revision-one binding and all-target PENDING projection are constructed and checked together; production transaction/response classification remains external |
| Control receipt retention boundary | Implemented (checked local window projection; policy authority pending) | `ControlOperationReceiptV1.createWithQueryWindow`, `ControlRegistrationProjectionV1.initialWithQueryWindow`, `ControlRegistrationProjectionV1Test`; trusted `registered_at.latest + controlOperationQueryWindow` checked-add and overflow/negative-window rejection are covered; immutable policy distribution remains external |
| Embedded Prepared Control registration path | Implemented (local conformance only) | `EmbeddedDelayService.registerPreparedControlOperation`, `EmbeddedDelayServiceTest`; target registration, receipt/current pairing, registration binding and operation-authority CAS are exercised together; production authenticated gateway/Oxia transaction remains a blocker |
| Registry §6.3 Control Operation request union | Implemented (canonical request branches; authority pending) | `ControlOperationKindV1`, `ControlOperationRequestV1`, `ControlOperationRequestBranchV1`, `StopNewSchedulesRequestV1`, `LaneGateRequestV1`, `CloseLaneRequestV1`, `BreakOrderingRequestV1`, `DrainShardRequestV1`, `FenceShardRequestV1`, `ForceCheckpointRequestV1`, `GetCheckpointCatalogRequestV1`, `ReplayDeadLetterRequestV1`, `ResolveUncertainRequestV1`, `PublishQuotaGrantRequestV1`, existing Profile request branches, `ControlOperationRequestV1Test`, `ControlRequestSupportCodecTest`; all fifteen outer tags, exact branch field order, empty catalog branch, acknowledgement/evidence/boolean matrices, fixed hash/scope fields, retry timing and quota-plan identity/version/hash canonical round-trips/rejection vectors are covered; authenticated actor/resource authority, source-ordered registration, operation state and Oxia CAS remain release blockers |
| Registry §6.3 Control target value layer | Implemented (canonical target branches; preparation matrix, local mutation binding and local immutable registration seam enforced) | `ControlTargetKindV1`, `ControlTargetRefV1`, `LaneControlTargetV1`, `ControlMessageTargetV1`, `ProfileControlTargetV1`, `ControlTargetRefV1Test`, `PreparedControlOperationV1`, `ControlTargetMutationBindingV1`, `ControlTargetRegistrationAuthority`, `InMemoryControlTargetRegistrationAuthority`; all six target branches, Profile rotation all-or-none precondition tuple, optional expected System Mutation ID/hash pair, branch-kind matching, target digest, operation-specific target counts/kinds, prepared-target membership, ControlRef/logical identity, mutation ID/hash, target Shard/Message, Replay/Resolve body and Lane marker matching, canonical tamper rejection, and exact-byte idempotent Prepared registration are covered; source-mutation construction, actor/resource authorization, authenticated target existence and Oxia CAS remain release blockers |
| Oxia Control target registration validation adapter | Implemented (backend seam and exact reread validation; real Oxia transport pending) | `OxiaControlTargetRegistrationAuthority`, `OxiaControlTargetRegistrationAuthorityTest`; backend registration outcome, exact Prepared reread, operation-ID lookup identity and mutation binding are checked; real Oxia transaction, response classifier, target existence and transport remain release blockers |
| DelayShard Control marker registration gate | Implemented (configured local authority; Oxia transaction pending) | `DelayShard` eight-argument constructor, `ControlTargetRegistrationAuthority`, `DelayShardTest.configuredControlRegistrationRejectsUnregisteredMarkerBeforeHandler`, `DelayShardTest.configuredControlRegistrationAppliesExactRegisteredMarker`; configured shards extract the body `ControlRef`, require the exact registered Prepared target and validate mutation identity before applying the three source-ordered Control marker types; missing, malformed or drifting registration is persisted as `UNAUTHORIZED_SYSTEM_MUTATION` with no handler effect, while an exact registered target reaches the normal handler; production Oxia registration/lookup and authenticated writer authority remain release blockers |
| Control System Mutation construction seam | Implemented (signed envelope and Prepared-target binding; body/authentication pending) | `ControlSystemMutationFactoryV1`, `ControlSystemMutationFactoryV1Test`; operation-specific mutation type, `ControlRef` logical identity, signed System Mutation envelope and expected ID/hash binding are checked before return; body encoders, signing-key trust/ACL and source Broker registration remain release blockers |
| Registry §6.3 Prepared Control Operation envelope | Implemented (canonical pre-I/O envelope, target matrix, mutation binding and local RBAC gate; registration authority pending) | `ControlAuthorV1`, `ControlRoleV1`, `ControlRoleSetV1`, `ControlAuthorizationContextV1`, `ControlOperationAuthorizationV1`, `PreparedControlOperationV1`, `PreparedControlOperationV1Test`, `ControlOperationAuthorizationV1Test`, `ControlTargetMutationBindingV1`; fixed operation ID/version, request-kind binding, request hash, operation-specific target counts/kinds and request-to-target Profile/Quota identity, strictly sorted repeated targets, target-snapshot hash, query-policy/retry fields, prepared digest, Ed25519 signing/verification, completed source-mutation ControlRef/identity/body binding, actor/role/scope hash equality and minimum role matrix are covered; authenticator implementation, target existence/tenant authorization, source-mutation construction, Oxia registration outcome, non-persistence proof and durable control-operation state remain release blockers |
| Registry §6.3 Control registration outcome union | Implemented (canonical outcome/proof values, prepared-operation binding and local exact-Prepared target registration; Oxia transport pending) | `ControlRegistrationOutcomeV1`, `ControlNonPersistenceProofKindV1`, `ControlNonPersistenceProofV1`, `ControlDefinitelyNotRecordedV1`, `ControlRecordUncertainV1`, `ControlRegistrationOutcomeMessageV1`, `ControlRegistrationBindingV1`, `ControlTargetRegistrationAuthority`, `InMemoryControlTargetRegistrationAuthority`, `ControlRegistrationOutcomeCodecTest`, `ControlRegistrationBindingV1Test`, `ControlTargetRegistrationAuthorityTest`; proof branch evidence matrix, operation/prepared digest binding, exact receipt request/scope/target identity and initial revision, CONTROL-stage error fencing, recorded/definitive/uncertain outer tags, canonical round-trip and timeout-proof rejection vectors, and idempotent byte-identical Prepared registration are covered; authenticated Oxia transaction/response classifier, real registration transport, retry/query state and durable operation authority remain release blockers |
| Registry-shaped `PayloadProofTrustSetSemanticV1` | Implemented (canonical verifier-key/hash codec, exact local catalog and source-ordered marker projection; authority pending) | `PayloadProofVerifierKeyV1`, `PayloadProofTrustSetSemanticV1`, `PayloadProofTrustSetRefV1`, `PayloadProofTrustSet.fromSemantic`, `PayloadProofTrustSetControlState`, `InMemoryPayloadProofTrustSetCatalog`, `PayloadProofTrustSetSemanticV1Test`, `PayloadProofTrustSetControlStateTest`, `PolicyCatalogTest`; sorted/unique Ed25519 raw keys, validity bounds, semantic hash/ref, exact local reference resolution, canonical round-trip/tamper rejection, source-time verification windows, strictly ordered activation markers, idempotent marker replay, first-seen issuance close versus historical verification, canonical marker-state encoding, and `DelayShard` atomic marker/result/source-position persistence with reopen are covered; authenticated source-ordered control authority and Recovery-Floor historical retention remain pending |
| NDR1 receipt frame | Implemented (queued/applied/reservation/control/native receipt/prepared payload subset) | `ReceiptFrame`, `ReceiptKind`, `CommandQueuedReceiptV1`, `CommandAppliedReceiptV1`, `PayloadReservationReceiptV1`, `PayloadProofTrustSetRefV1`, `ControlOperationReceiptV1`, `PulsarBrokerResourceIdentityV1`, `NativeCapabilitySnapshotV1`, `PulsarMetadataV1`, `NativePreparedDeliveryV1`, `NativePreparedRefV1`, `NativeDeliveryReceiptV1`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1`, `WireCommandIngressAdapter`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter`, `NativeSubmissionAdapterTest`; registry zero-payload vector, canonical PreparedCommandRef/ProtocolTuple, Kafka/Pulsar Source Position and SafeBrokerAck agreement, queued-to-applied digest/source fencing, barrier-gated applied-frame emission, apply-status/message-field consistency and generation/state-version/binding presence fencing, object-store profile/object identity/trust-set pinning, control operation/scope/target/evidence/query-boundary pinning, Pulsar-only native target/ACK identity matching, signed capability snapshot canonical digest/Trusted-UTC binding/Ed25519 verification, strict optional Pulsar metadata and key-sorted unique property encoding, native snapshot projection/expiry/attestation matching, submission-hash and prepared-ref byte projection, capability bits and physical-attempt/digest checks, query boundary/capability/physical-attempt/digest checks, Kafka queued ACK and definitive rejection proof projection, Pulsar batch-aware queued ACK and guard rejection proof projection, native persisted/guard-rejection/uncertain/local-fence projection, and flags/length/kind/CRC/Base64url rejection tests; durable guard/credential protection and real Broker response transports remain pending |
| Query response closed unions | Implemented (wire codec plus bounded local bridge) | `ProfileRefV1`, `PublicDestinationBindingViewV1`, `PublicEvidenceRefV1`, `CheckpointSummaryV1`, `CheckpointCatalogResultV1`, `CheckpointControlResultV1`, `LaneControlResultV1`, `ShardControlResultV1`, `ProfileControlResultV1`, `QuotaControlResultV1`, `MessageControlResultV1`, `RouteControlResultV1`, `SecretRotationResultV1`, all Registry Message/Command view classes, `CommandQueryResponseV1`, `MessageQueryResponseV1`, `ControlOperationQueryResponseV1`, `CurrentControlOperationV1`, `ControlTypedResultV1`, `PublicQueryErrorV1`, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queryCommand/queryMessage`, `DelayShard.matchesCommandHash`, `ProtocolCodecTest`, `CheckpointCatalogResultV1Test`, `CheckpointControlResultV1Test`, `ControlResultCodecTest`, `ControlOperationQueryResponseV1Test`, `EmbeddedDelayServiceTest`; exact branch tags/field order, Source Position barrier ordering, durable `dedupe_cf` command-hash binding (`RECEIPT_MISMATCH` on same-ID hash drift), state/status agreement, command-view optional presence fencing, safe NFC alias and payload/DLQ/evidence enum checks, canonical checkpoint-catalog shard/Floor/sorted-summary validation, checkpoint-control identity validation, all nine control-result branch field/presence/identity codecs with strict branch-to-payload dispatch and round-trip/rejection vectors, fixed-source queued-receipt barrier and retention projection, and canonical Control Operation CURRENT/error/target/revision/typed-result projection; production receipt routing, authorization-safe lookup, source-derived retention, Oxia ownership, durable control-operation query state and observability remain pending |
| System Mutation envelope, type registry, canonical hash/ID and Ed25519 signature | Implemented (bounded control plus admission/expiry/outcome/evidence/claim-result/resource-retire/delete-confirmed subset) | `SystemMutationType`, `SystemMutation`, `ShardSubjectV1`, `SystemMutationBodyCodec`, `ApplyShardControlBody`, `ReplayDeadLetterBody`, `ResolveUncertainBody`, `ControlRef`, `ControlReasonKindV1`, `ControlReasonV1`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `PayloadProofTrustSetActivatePayloadV1`, `PayloadProofIssuanceClosePayloadV1`, `PayloadProofTrustSetControlState`, `PayloadProofTrustSetControlCatalog`, `PublishAdmissionBody`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `PublishOutcomeBody`, `ClaimResultBody`, `ResourceRetireIntentBody`, `ResourceRetireIntentRecord`, `ResourceDeleteConfirmedBody`, `ResourceDeleteConfirmedRecord`, `TrustedUtcIntervalEvidence`, `SystemMutationResult`, `AuthorIdentity`, `ClaimRecord`, `GenerationRuntimeIndex`, `DelayShard`, `ProtocolCodecTest`, `ShardSubjectV1Test`, `ReplayDeadLetterBodyTest`, `ResolveUncertainBodyTest`, `PublishAdmissionBodyTest`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `ResourceRetireIntentBodyTest`, `ResourceDeleteConfirmedBodyTest`, `GenerationRuntimeIndexTest`, `PayloadProofControlPayloadV1Test`, `ProfileBindingControlStateTest`, `PayloadProofTrustSetControlStateTest`, `DelayShardTest`; canonical owner/control/fence/service branches, strict canonical ShardSubject route/partition decoding shared by envelope and body checks, body fields 1–3, required/optional operation fields, wire widths, bool bounds, canonical nested bytes, canonical typed `RetryPolicyRefV1` Replay field, canonical Replay/Resolve encoding and shared message-bearing body self-routing checks for Admission/Claim/DLQ/Expire, durable `dedupe_cf/SYSTEM_MUTATION` with key/value mutation identity and Source Position shard fencing, explicit signature verification, source-ordered Profile activation/close and trust-set activation/issuance-close with semantic-reference checks and atomic marker-state persistence/reopen, source-ordered Lane PAUSE/RESUME/BREAK/CLOSE with ControlRef/identity/incarnation/CAS, close-policy/acknowledgement checks and atomic Claim/READY rollback, source-ordered TIME_FENCE watermark and reservation-expiry overlay/materialization, source-ordered `EXPIRE_GENERATION_V1`, `PUBLISH_ADMISSION_V1` descriptor/Ready Certificate/Claim identity projection plus adapter/encoding/target/partition/channel-Profile cross-object fences, replay-stable timeline-key/semantic-digest/counter/obligation-set preconditions, checked attempt-number and uncertain-retry counter projection, definitive `PUBLISH_OUTCOME_V1/NOT_PUBLISHED` disposition/retry-shape subset, verified-published `ATTACH_PUBLISHED_EVIDENCE`, `ATTACH_NOT_PUBLISHED_EVIDENCE` all-absent normalization, and verified `EVIDENCE_RESOLUTION_V1` transition subsets, replay-stable permanent `CLAIM_RESULT_V1` ClaimPrecondition/timeline terminalization subset, closed ExactResourceIdentity/ProtectionSet parsing plus registered logical-identity verification and atomic `gc_cf/TASK` retire-intent persistence, exact RetireIntentRef/DeleteOutcome/ExternalDeleteEvidence matching and source-ordered local tombstone persistence, local durable `SCHEDULED -> CLAIMED -> revoke/Admission/ClaimResult/Cancel/Reschedule/expiry` transitions, and v4 `id_cf/MESSAGE` runtime-index writes are covered; immutable Profile/catalog and authenticated source control authority, source-protected signing-key trust/ACL, immutable Oxia target registration, Recovery Floor barriers, full ActiveLaneState persistence, obligation-set quota/recovery accounting and full Claim materialization/recovery model remain pending |
| Kafka/Pulsar source order token and source identity fencing | Implemented (core codec) | `SourcePosition.sourceOrderToken`, `SourcePositionCodec` byte-round-trip canonical decode with explicit truncated length/fixed-field rejection, Kafka offset/Pulsar ledger-entry-batch order, exact canonical-position fencing for same physical token, physical-resource comparison guard, direct Source Position construction rejects malformed/non-NFC text before identity bytes are produced (`ProtocolCodecTest.sourcePositionsRejectNonCanonicalTextAtConstruction`, `ProtocolCodecTest.sourcePositionDecoderRejectsTruncatedLengthAndFixedFields`), `ProtocolCodecTest`, `DelayShardTest`; broker assignment/barrier adapters pending |
| Pinned Kafka/Pulsar command ingress outcome mapping | Implemented (transport SPI plus Kafka/Pulsar wire projection) | `PinnedKafkaCommandIngress`, `PinnedPulsarCommandIngress`, `WireCommandIngressAdapter`, `WireIngressOutcomeSupport`, `KafkaIngressResource`, `PulsarIngressResource`, `PulsarSendRequest`, `PulsarSendResult`, `AdapterIngressTest`; Kafka topic UUID and Pulsar resource token plus physical topic creation identity are carried at the request boundary, ingress resources and persisted transport identities enforce canonical UTF-8/NFC text, invalid 16-byte physical attempts are rejected locally before Producer ownership, result dispositions form a closed matrix (`PERSISTED` + `OK` versus non-persisted stable code with no success position), persisted Pulsar results fence all pinned identity fields, managed Kafka/Pulsar transport failures, null results and malformed receipt projections use `ENQUEUE_RESULT_UNCERTAIN` without leaking an exceptional Future, managed result projection normalizes native-only guard/uncertain codes while only the native adapter uses `NATIVE_ENQUEUE_RESULT_UNCERTAIN`, and both managed adapters can project queued/definitive-proof/uncertain NDR1 outcomes with evidence fail-closed; concrete pinned request transports, authenticated production rejection classifiers and source assignment pending |
| Target publish side-effect outcome boundary | Implemented (identity-fenced transport SPI, local physical admission seam and durable attempt/outcome subset) | `DestinationPublishAdapter`, `DestinationPublishResult`, `PinnedKafkaDestinationAdapter`, `PinnedPulsarDestinationAdapter`, `KafkaTargetResource`, `PulsarTargetResource`, `DestinationPhysicalAdmission`, `BoundedDestinationPublishAdapter`, `PublishAttemptLedger`, `PublishOutcomeBody`, `DelayShard`, `DestinationAdapterTest`, `DestinationPhysicalAdmissionTest`, `BoundedDestinationPublishAdapterTest`, `DelayShardTest`; PUBLISHED results now require non-empty delivery identity/evidence, use stable code `OK`, and pair an optional pinned `BrokerResourceIdentityV1` with a non-negative physical partition; target resources enforce canonical UTF-8/NFC cluster/topic identity before request construction; local admission protects Worker and target-cluster request/byte caps, READY Lane minimums, Lane caps and zombie charges, counts a not-yet-ready candidate Lane's protected minimum exactly once when opening READY, and releases only on delegate-stage completion; pinned Kafka/Pulsar adapters still verify returned identity, including Pulsar creation identity; physical adapter evidence journal, durable ActiveLaneState/ReadyCertificate admission authority, authenticated non-persistence classifiers, and remaining outcome/evidence mutations pending; definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable state transitions, service-authored verified `EVIDENCE_RESOLUTION`, and Resolve `ATTACH_PUBLISHED_EVIDENCE`/`ATTACH_NOT_PUBLISHED_EVIDENCE` obligation settlement are covered locally |
| One Delay Shard = one RocksDB DB | Implemented | `ShardStore`, `StoreMetadata`, `StoreRuntimeMetadata`, `ShardStoreTest`, `StoreRuntimeMetadataTest`; existing DBs missing the `meta_cf` shard-identity marker or carrying a store incarnation that disagrees with `incarnations/<storeIncarnation>/db` now fail closed instead of being initialized or opened as a different store; the local `meta/FIXED` runtime projection persists ingress-fence/checkpoint identity at keys 4/7, owner-open epoch at key 8, typed evidence cursors at key 6 and clean-close state at key 9, while reserved control-snapshot key 10 is not consumed; all values use bounded canonical validation and post-open metadata/format/install failures close every DB/Column Family handle and options object before slots release, covered by `ShardStoreTest.malformedExistingMetadataDoesNotLeaveRocksDbOpen` and `ShardStoreTest.malformedRuntimeMetadataDoesNotLeaveRocksDbOpen` |
| Worker DB/checkpoint resource limits | Implemented (config-envelope plus runtime shard/DB/restore slots and local placement scorer) | `ShardStoreConfig`, `SharedRocksDbResources`, `ShardStore`, `WorkerResourceEnvelope`, `WorkerCapacityAdmission`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `WorkerResourceEnvelopeTest`, `WorkerCapacityAdmissionTest`, `WorkerPlacementPolicyTest`, `ShardStoreTest`; checked memory/FD/disk cross-bucket inequalities fail closed before resource creation, logical `maxOwnedShards` and physical `maxOpenShardDbs` slots are tracked independently, checkpoint restore/download staging has an independent bounded slot released after atomic installation, the process-shared RocksDB `Env` background pool is explicitly configured from `maxBackgroundJobs`, each DB binds `maxBackgroundJobsPerDb` plus nonzero `reservedFlushJobs`/`maxCompactionJobs` split, every Column Family receives the explicit `maxWriteBufferBytesPerDb` write-buffer option while the process-wide `WriteBufferManager` remains the shared memtable budget, shared RocksDB resources refuse premature close, local Worker admission sums distinct shard `committed` vectors plus fixed/transition demand once with checked per-dimension arithmetic, and the local placement seam rejects over-capacity candidates before applying dominant-resource/load scoring with stale-telemetry penalty, minimum residence, hysteresis and movement cost; `ShardStoreTest.perDbWriteBufferCeilingMustBePositive` and `ShardStoreTest.backgroundJobSplitMustFitPerDbCeiling` cover invalid configuration; the per-CF option and background-job split are not aggregate WAL/SST/temp runtime accounting proofs, and live JVM/cgroup/rlimit probes, WAL/SST/temp accounting, per-work-class reserves, cooperative assignment and Oxia placement capacity authority remain pending |
| Seven application CFs plus empty `default` CF | Implemented | `ShardStore` descriptor validation |
| Shard identity and local Store Incarnation validation | Implemented | `StoreMetadata`, `ShardStoreTest` |
| Synchronous atomic WriteBatch | Implemented | `ShardStore.write`, `ShardStoreTest` |
| Native RocksDB checkpoint creation | Implemented | `ShardStore.createCheckpoint`, `ShardStoreTest`; checkpoint creation is staged under the same-filesystem `checkpoint-tmp` namespace, rejects an existing target, installs only through an atomic rename, and removes a target that was moved but failed the subsequent parent-directory durability step, with failed-stage cleanup |
| Checkpoint file inventory and canonical manifest projection | Implemented (local CAS projection, bounded upload coordinator and deterministic schedule seam; external publication pending) | `CheckpointFileInventory`, `CheckpointManifestLimits`, `CheckpointManifest`, `CheckpointManifestJson`, `CheckpointResourceV1`, `CheckpointUploadStateV1`, `CheckpointUploadIntentV1`, `CheckpointUploadIntentStore`, `CheckpointReapingGuard`, `CheckpointUploadAdapter`, `CheckpointUploadRequest`, `CheckpointUploadCoordinator`, `CheckpointScheduler`, `CheckpointManifestTest`, `CheckpointResourceV1Test`, `CheckpointUploadIntentV1Test`, `CheckpointUploadIntentStoreTest`, `CheckpointReapingGuardTest`, `CheckpointUploadCoordinatorTest`, `CheckpointSchedulerTest`; inventory streams SHA-256 over each file without loading an SST into heap and rejects symbolic links or non-regular files before restore/copy, while an explicit `CheckpointManifestLimits` boundary fails closed on file count, individual/total bytes, path/manifest bytes, evidence-cursor count and file/manifest-object identity lengths before local hashing or provider I/O (the raw manifest byte cap is enforced before JSON parsing and the file/evidence array bound while parsing); inventory and manifest file names are ordered by unsigned normalized UTF-8 bytes, and total-byte addition overflow is converted to a fail-closed validation error (`CheckpointManifestTest.manifestTotalFileBytesOverflowFailsAsValidationError`); the manifest decoder enforces the closed field order/types, Kafka/Pulsar typed `EvidenceCursorV1` branches, strict cursor identity ordering and byte-identical canonical JSON round trip; the published-manifest Object Store identity and upload-intent state branches have closed canonical codecs, and the local coordinator verifies exact pending intent, deadline, shard/lineage/owner/store/parent identity, complete local file inventory, Worker upload slot and returned manifest length/SHA-256 before PENDING_UPLOAD -> PUBLISHED revision CAS; the local scheduler validates interval/jitter, spreads owned shards deterministically, fences duplicate in-flight claims, and requires the exact returned claim handle for completion so stale callbacks cannot reset a newer attempt; an exact PUBLISHED successor is reread after response loss without another adapter call; the guarded local PENDING_UPLOAD -> REAPING overload also rejects published catalog protection, same-checkpoint active RecoveryPin protection, unavailable catalog/pin reads, and Floor/coverage authority failures, while legacy no-limits overloads remain compatibility seams and real Oxia lease/session/catalog CAS, Object Store upload/attestation/publication, owner-abandonment proof and reaping/quiescence remain pending |
| Checkpoint restore into a new Store Incarnation | Implemented (local manifest/catalog/pin-validated path) | `ShardStore.restoreFromCheckpoint`, `CheckpointManifest.decodeCanonicalJson`, `CheckpointManifestLimits`, `ShardStoreTest`; raw canonical manifest bytes are decoded and catalog-validated before local file verification, the raw manifest byte cap is enforced before JSON parsing and the finite limit set is then applied to the decoded manifest and complete file inventory before copy/install, staged DB identity plus persisted `lastCheckpointId`, `appliedShardLogPosition`, `shardMutationSequence` and evidence cursors are compared to the exact manifest before install, and the pin-aware overload rereads the exact active RecoveryPin before staging and before atomic Store Incarnation installation; legacy no-limits restore overloads remain compatibility seams, while Oxia Recovery Pin/Floor CAS and source replay remain pending |
| Recovery catalog, lineage and Floor selection | Implemented (typed local codecs, typed cursor-dominant Floor projection, manifest-bound evidence cursors, upload-intent-bound publication and bounded in-memory pin authority; Oxia CAS pending) | `RecoveryFloorRefV1`, `RecoveryCandidateRefV1`, `RecoveryPinV1`, `EvidenceCursorV1`, `RecoveryCatalog`, `RecoveryCatalogAuthority`, `OxiaRecoveryCatalog`, `RecoveryFloor`, `RecoveryFloorRefV1Test`, `RecoveryCandidateRefV1Test`, `RecoveryPinV1Test`, `EvidenceCursorV1Test`, `RecoveryCatalogTest`; the typed references canonically bind lineage/checkpoint/manifest, catalog generation, Source Position, mutation sequence, sorted evidence cursors, candidate branch and session identity digest; the local catalog still binds one shard, rejects non-zero genesis lineage, enforces floor ancestry, requires the Floor cursor set to byte-match the candidate manifest and then enforces same-generation cursor dominance, exposes candidate validation/selection and `proveFloorCoverage`, independently fences requested mutation/source boundary coverage with canonical Source Position equality on equal order tokens, requires a PUBLISHED `CheckpointUploadIntentV1` plus exact manifest/object/owner/store/parent identity for the local publication projection, validates the same request binding before an Oxia upload-intent CAS, copies mutable checkpoint/digest/cursor/coverage inputs before backend CAS, validates optional Oxia publication Floors and all candidate/ancestry manifests against their complete canonical JSON projection, shard and publication generation, and validates every Oxia coverage ancestry edge against published parent id/hash, lineage/source/mutation progression and evidence-cursor dominance; supports one exact active-pin create/idempotent-reread/release projection with typed Floor equality checks; durable Oxia Owner Lease/session and catalog/Floor CAS, real Object Store publication/attestation, and evidence-cursor retention/dominance enforcement remain pending |
| Command applied/rejected state machine | Implemented (embedded core) | `DelayShard`, `DelayShardTest`, `DurableResultTest` |
| DUE/ORDERED/READY/EXPIRY timeline namespaces | Implemented (embedded core subset) | `DelayShard`, `MessageRecord`, `TimelineWorkRef`, `GenerationRuntimeIndex`, `ClaimRecord`, `ReadyIndexValue`, `KeyCodec`, `GenerationRuntimeIndexTest`, `KeyCodecTest`, `DelayShardTest`; READY key/value, laneVersion fencing, retry eligibility for unordered definitive retry, canonical timeline semantic/instance digests, v4 runtime-index persistence, atomic affected-lane updates, durable Claim removal/restoration including source-ordered Lane Pause rollback, fenced rebuild/discovery, replay-stable Claim Result timeline-key/semantic-digest checks, current `id_cf/MESSAGE` reads and activation/close/retirement scans fenced to the self-routing key shard and embedded schedule Source Position shard, Cancel/Reschedule fencing whenever an UNCERTAIN obligation survives a current-work projection, the pinned-policy `UNKNOWN` scheduling plus `UNCERTAIN_RETRY` Admission subset that materializes timeline work while retaining the old obligation, and exact constructors for the registered FENCE, GC protection, Producer and Recovery key layouts are covered; ControlRef validation and policy-bound timeline materialization beyond this local budget remain pending |
| `CLAIMED`/`PUBLISHING`/`UNCERTAIN` attempt ledger and obligation locator | Implemented (durable local Claim plus source-ordered attempt subset) | `ClaimRecord`, `PublishAttemptLedger`, `AttemptObligationRef`, `GenerationRuntimeIndex`, `PublishAdmissionBody`, `PublishOutcomeBody`, `RetryPolicyCatalog`, `DelayShard`, `DelayShardTest`; local Claim sequence/key/value and exact precondition/instance digest are persisted atomically, registry-shaped runtime index and canonical obligation-set digest are persisted with v4 Message records, source-ordered `PUBLISH_ADMISSION_V1` checks replay-stable timeline key/semantic/counter/obligation projections and descriptor attempt number, including the `UNCERTAIN_RETRY` source-work branch, reconstructs the same PUBLISHING attempt when the reversible Claim is absent but source state matches, retains admission counters across definitive retry timelines, structurally bounds descriptor `actionAt <= deliverAt`, and when the exact Profile catalog is supplied validates ordinary timing or the pinned certified Pulsar handoff lead before admission; when the catalog is supplied it also revalidates the pinned immutable policy budgets at Admission, uncertain retry, and reopen; shard activation now performs bounded bidirectional reconciliation between every current/terminal runtime obligation ref and its exact PUBLISHING/UNCERTAIN ledger, and between every live ledger/Claim and the current Message branch, failing closed on an orphan, missing counterpart, persisted total-admission overflow, or terminal/runtime summary mismatch; source-ordered `PUBLISH_OUTCOME_V1` UNKNOWN atomically migrates to UNCERTAIN, can materialize the pinned-policy `UNCERTAIN_RETRY` timeline subset without consuming the retry counter, but a closed Lane keeps the generation in UNCERTAIN with no retry timeline, verified-success closes the ledger, definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable outcomes atomically requeue, terminalize, or block the lane, and a definitive not-published outcome on a closed Lane is terminalized as `LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED` without retry; late outcomes from either the current or an older generation settle only the exact ledger/terminal summary while preserving terminal decision state and monotonically raising duplicate risk for verified success; service-authored verified `EVIDENCE_RESOLUTION_V1` can close or requeue the exact UNCERTAIN ledger; full Profile/Adapter/evidence/capacity validation, immutable RetryPolicy publication/authority, multi-obligation terminal-summary lifecycle beyond current-generation settlement, uncertain-retry ControlRef/policy enforcement beyond the local automatic budget, obligation-set quota/recovery accounting beyond local reconciliation, Recovery Floor/source replay and full Claim materialization/recovery semantics remain pending |
| Publish Admission timing/profile gate | Implemented (local semantic gate; publication authority pending) | `PublishAdmissionBody.requireTimingPolicy`, `PublishAdmissionBody.requireBrokerTiming`, `DelayShardConfig`, `DelayShard` optional `ProfileCatalog`, `PublishAdmissionBodyTest`, `DelayShardConfigTest`, `DelayShardTest.publishAdmissionTimingFailureRevokesMatchingClaimBeforePersistingStaleMutation`; descriptor structure permits only `actionAt <= deliverAt`, catalogued shards validate exact Destination/Delivery Capability refs, fixed V1 adapter encoding and metadata branch, ordinary managed timing, fixed certified Pulsar handoff lead/capability, target resource and explicit/hash partition policy, and every apply/replay checks source Broker persistence time against configured `maxIngressBrokerTimestampDivergenceMs`/`maximumAdmissionMutationEnqueueAgeMs` with checked expiry and decision-interval distance arithmetic; timing/profile failures revoke an exact matching live Claim before persisting `STALE_SYSTEM_MUTATION`, so no attempt or Producer state is created; catalog-less compatibility shards fail closed to `actionAt=deliverAt`; formal Broker-time certification, Profile publication, Broker guard attestation and production Producer authority remain release blockers |
| Terminal generation history | Implemented (current/older-generation open-obligation summary and late-settlement subset) | `TerminalGenerationRecord`, `ClaimRecord`, `DelayShard`, `DelayShardTest`, `TerminalGenerationRecordTest`; Claim Result, publish outcome, expiry and command terminalization persist the exact terminal state version together with remaining obligation refs and duplicate-risk projection in a versioned terminal record, direct reads fence embedded `messageId/generation` against the `terminal_cf` key and require the applied Source Position to belong to the current Shard, activation reconciles both current and older-generation summaries against their exact inflight ledgers, and late verified or definitive not-published outcomes remove only their exact old ledger/ref without changing terminal status/code/time; legacy v1 terminal records decode as empty summaries; older-generation callbacks after full Dead Letter Replay, Replay retention and guarded GC remain pending |
| Large-payload reservation/proof/commit | Implemented (embedded core plus typed wire proof/response subset) | `LargeScheduleIntent`, legacy `PayloadCommitProof`, `PayloadCommitProofView`, `PayloadCommitProofV1`, `PayloadReservation`, `OpaquePayloadUploadHandleV1`, `PayloadUploadHandleResponseV1`, `PayloadAttestationResponseV1`, object-backed `MessageRecord`, `DelayShardTest`, `CommitLargeScheduleBodyV1Test`, `ProtocolCodecTest`; Prepare/Commit reservation quota, source-ordered TIME_FENCE overlay, bounded `RESERVATION_EXPIRY` discovery/materialization with byte-identical `id_cf` projection fencing, bounded and key/value-identity-checked reservation lookup with duplicate detection plus Source Position shard fencing, guarded local quota release, bounded opaque upload handle, typed Object Store proof Profile/tenant-scope/optional-etag/proof-id/signature validation, fixed payload-scoped error branches and typed attestation/commit response round-trips are covered; Object Store handle issuance/attestation ownership, source-ordered trust controls, fence-key history and guarded GC remain pending |
| Source assignment, typed Activation Barrier and Owner Lease | Implemented (local bounded mixed command/System Mutation replay seam; production authority pending) | `SourceAssignment`, `SourceReplayEntry`, `SourceReplayRecord`, `SourceReplayMutation`, `SourceReplayOutcome`, `SourceReplayCursor`, `SourceReplayTurn`, `ReplayTurnBudget`, `SourceReplaySuccessor`, `KafkaActivationBarrier`, `PulsarActivationBarrier`, barrier-gated `OwnedDelayShard`, `OwnerLease`, `OwnerLeaseContext`, `OwnerLeaseStore`, `OxiaOwnerLeaseStore`, `OwnerDrainCoordinator`, `OwnerLeaseTest`, `OwnerDrainCoordinatorTest`, `SourceReplaySuccessorTest`, `OxiaOwnerLeaseStoreTest`, `SourceActivationBarrierTest`; catch-up now requires an explicit non-zero assignment identity/epoch bound to the typed barrier, assignment/barrier equality compares array-backed resource and guard identities by value, Kafka barrier cluster identity is canonical NFC/UTF-8 at construction, Pulsar barrier resource/guard identities reject all-zero placeholders, the runtime Pulsar barrier pins the inclusive entry `batchSize` and rejects same-entry batch-shape drift before apply, every catch-up cursor and post-activation apply record is checked against the typed physical source identity and rejects same offset/ledger-entry-batch tokens with conflicting canonical metadata before replay; the V1 overload pins an adapter-defined `SourceReplaySuccessor` for the entire catch-up window, accepts only exact canonical redelivery or the proven immediate successor, and the strict Kafka helper rejects offset gaps before applying the skipped record (Pulsar batch-member strictness is available while entry transitions remain adapter-defined); empty Kafka/Pulsar barriers are covered, and an empty Pulsar barrier validates any non-null persisted cursor before allowing activation; the unified bounded `replayTurn` seam and type-specific turn APIs cap record count, canonical frame/position bytes and elapsed monotonic time while preserving the caller cursor across turns, and apply mixed Command/System Mutation entries in one source order through the shard's atomic WriteBatch before advancing the cursor (the whole-`Iterable` methods and assignment-only overload remain explicitly compatibility conveniences); Pulsar replay/catch-up/apply paths additionally require a positive guarded source-connection generation and exact resource-guard attestation digest; context-bound lease acquisition carries assignment identity plus assignment epoch and exact session identity, renewal preserves them and the lifecycle state, stale state transitions fail CAS, and the activation overload requires the authority to CAS the same lease to `ACTIVE_FOR_COMMANDS` before opening the local gate; `OwnerDrainCoordinator` composes the locally provable drain order and leaves callback/source quiescence and production lease/session integration explicit; real Kafka/Pulsar consumer replay, Oxia session/ephemeral records, broker assignment/guard and production activation transaction remain pending |
| Queued vs applied client outcomes | Implemented (embedded core) | `EmbeddedDelayService.queuedReceiptV1`, `appliedReceiptV1`, `enqueueOutcomeV1`, `queryCommand`, `queryMessage`, `DelayShard.matchesCommandHash`, `EmbeddedDelayServiceTest`, `ProtocolCodecTest`; queued receipt stays distinct from applied result, the closed managed enqueue union preserves queued/definitely-not-queued/uncertain states, applied frame is emitted only after the source barrier and retains the queued digest, query and applied-receipt barrier checks reject same physical offset/ledger-entry-batch tokens with conflicting canonical source metadata and same-command-id command-hash drift, pending is source-barrier based, full/compact/evidence-expired branches are bounded, and message projections require caller-supplied safe policy inputs; real Broker response adapters and production routing remain pending |
| Destination Lane gate/readiness projection | Implemented (core plus closed same-key terminal branch and typed ActiveLaneStateV1/quota/certificate/barrier codecs) | `LaneRecord`, `LaneRecordEnvelopeV1`, `ActiveLaneStateV1`, `LaneCircuitStateV1`, `LaneRuntimeBlockReasonV1`, `LaneQuotaUsageEntryV1`, `LaneQuotaUsageMapV1`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `LaneRetirementProgressV1`, `LaneTerminalGuardV1`, `LaneRecordTest`, `LaneRecordEnvelopeV1Test`, `ActiveLaneStateV1Test`, `LaneQuotaUsageMapV1Test`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `LaneTerminalGuardV1Test`, `ApplyShardControlBody`, `ControlRef`, `DelayShard`, `DelayShardTest`; schedulable lanes maintain a versioned READY head and readiness/gate CAS transitions remove/recreate it atomically; source-ordered signed PAUSE/RESUME/BREAK/CLOSE applies exact Lane incarnation/control-version fencing, strict-lane acknowledgement checks and atomically revokes/restores reversible Claims; the same `meta_cf/LANE` key now persists a closed ACTIVE/TERMINAL branch, direct Lane reads fence the embedded Lane id, and close-cursor reads fence Lane id/incarnation/control version/source shard before exposing materialization work; the bounded local retirement path conservatively proves no current message/timeline/inflight work before atomically installing a tuple/profile/source/digest-checked terminal guard that survives reopen and rejects reuse; retirement progress and terminal-guard source checks reject equal order tokens with conflicting canonical metadata; runtime and control version increments fail closed at `Long.MAX_VALUE` (`LaneRecordTest.versionCountersFailClosedBeforeLongOverflow`), while the Registry-shaped ActiveLaneStateV1 codec now covers tuple identity/digest, gate/readiness block-reason rules, 17-dimensional lane charge, circuit/backoff counters, ready-key digest, canonical ReadyCertificateV1 wrapper and retirement progress, and the per-Lane quota entry/map codec enforces sorted identity keys and usage/map digests; `LaneRecordEnvelopeV1` now emits typed field-10 ACTIVE values as direct canonical `ActiveLaneStateV1`, rejects malformed typed values instead of downgrading them to legacy, and still reads the old adapter sub-message. Legacy `DelayShard` persistence remains on the compatibility adapter because current Schedule inputs do not carry the immutable Profile/tuple/certificate inputs required for a lossless cutover; quota-map revision coupling, Oxia target registration and Recovery-Floor/retention guard remain release blockers |
| Lane Close materialization cursor | Implemented (local source-marker overlay, bounded cursor and discovery/materializer bridge) | `LaneCloseMaterializationCursor`, `LaneCloseMaterializer`, `DelayShard`, `ShardQuota`, `LaneCloseMaterializationCursorTest`, `LaneCloseMaterializerTest`, `DelayShardTest.closeTransfersUnadmittedQuotaAndResumesBoundedMaterializationCursor`; Close marker transfers unadmitted message/reservation quota once in the marker WriteBatch, persists canonical `timeline/SYSTEM` kind-2 cursor state, strictly discovers only cursor entries whose key/value/Lane identity still agree, freezes only generations with an empty admitted-obligation set as `DEAD_LETTER(LANE_CLOSED_BEFORE_ADMISSION)`, closes uncommitted reservations as `ABANDONED`, and resumes message then reservation scans after restart; the local materializer runs bounded turns over the discovered cursors without making a new semantic decision, and its per-result and cross-Lane aggregate counts use checked addition; closed-lane Cancel/Reschedule/Commit paths return the stable frozen outcomes before cursor completion; PUBLISHING/UNCERTAIN obligations are retained and full close-owned Claim tagging, admitted-outcome retirement, object-handle/quiescence GC, Recovery-Floor protection and owner/Oxia materializer orchestration remain release blockers |
| Destination Lane isolation and bounded weighted DRR | Implemented (scheduler core plus fenced READY recovery seam) | `LaneScheduler`, `PersistentLaneScheduler`, `ReadyIndexValue`, `TimelineEntry`, `LaneSchedulerTest`, `SchedulerProjectionsV1Test`; lane-local work is rebuilt from bounded `timeline_cf/READY` plus `meta_cf/LANE`/`id_cf/MESSAGE` identity checks, recomputes the exact timeline key and digest, verifies the timeline value, rejects a READY message whose self-routing key or embedded schedule Source Position belongs to another Shard, stale/orphan/multiple-head projections fail closed, and the recovered heads replace the active ring and queues before scheduling resumes; scheduler quantum/weight/cap multiplication is checked at configuration and lane registration, while runtime deficit accumulation saturates instead of wrapping; `LaneSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow`, `LaneSchedulerTest.saturatesRestoredDeficitBeforeServing` and `LaneSchedulerTest.fencedRecoveryRejectsReadyMessageFromAnotherShard` cover the local fence; production Lane certificate/activation authority remains pending |
| Persistent scheduler fairness counters | Implemented (local closed projection plus owner-bound recovery first pass and rotating READY cursor) | `PersistentLaneScheduler`, `LaneScheduler`, `SchedulerProjectionsV1`, `OwnerIdentityV1`, `LaneSchedulerTest`, `SchedulerProjectionsV1Test`; all five `meta_cf/SCHEDULER` values are written in one batch, persisted successor order is restored without re-adding discarded lanes, the persisted `SchedulerRoundV1.owner` is compared with the current owner and an owner/store change restarts `recovery_first_pass`, and the first recovery rotation serves at most one item per eligible Lane until every currently discovered Lane has received an opportunity; fenced READY rebuild also restarts that pass, while Lane incarnation/version and message-generation checks remain enforced; full Oxia-fenced activation and typed ActiveLaneState cutover remain release blockers |
| Worker-to-shard-to-lane bounded DRR | Implemented (core snapshot plus READY-aware outer filtering, recovery first-pass and local placement scoring) | `WorkerScheduler`, `WorkerSchedulerTest`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `WorkerPlacementPolicyTest`; outer DRR persists/restores deficit, cursor/round and blocked-shard isolation state, visits only shards with at least one schedulable Lane head, starts a new process/restore/READY recovery pass that serves each currently eligible shard at most once before repeating one, checks shard weight/quantum/cap arithmetic before registration and saturates runtime deficit accumulation, and the local placement seam hard-filters full committed capacity/DB slots before deterministically scoring unequal observed load; `WorkerSchedulerTest.recoveryFirstPassServesEveryEligibleShardBeforeRepeatingOne`, `WorkerSchedulerTest.restoreStartsANewOuterFirstPass`, `WorkerSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` and `WorkerSchedulerTest.saturatesRestoredDeficitBeforeServing` cover the local fence; durable outer scheduler projections, broker assignors, Oxia desired-placement plans and authoritative placement weights remain pending |
| Closed Stable Code registry | Implemented | `StableCode`, `FailureStageV1`, `RetryabilityV1`, `StableErrorV1`, `ProtocolCodecTest`; all Registry stable codes including activated-protocol/capacity-fence codes, code-derived retryability, retry-at presence, mutually exclusive managed/native prepared refs, bounded diagnostic code and canonical round-trip/rejection checks |
| Non-persistence proof and Broker identity | Implemented (codec boundary) | `KafkaBrokerResourceIdentityV1`, `PulsarBrokerResourceIdentityV1`, `BrokerResourceIdentityV1`, `NonPersistenceProofKindV1`, `NonPersistenceProofV1`, `ProtocolCodecTest`; closed Kafka/Pulsar identity branches, kind-specific attempt/resource/request/response presence, pre-ownership evidence prohibition, adapter-proof version and fields 1–7 digest; authenticated adapter rejection classifiers and real non-persistence attestations remain pending |
| Managed/native submission outcome unions | Implemented (wire codec plus embedded/Kafka/Pulsar managed and native transport-SPI bridges) | `EnqueueOutcomeKindV1`, `DefinitelyNotQueuedV1`, `EnqueueUncertainV1`, `EnqueueOutcomeMessageV1`, `SubmissionOutcomeKindV1`, `NativeDefinitelyNotQueuedV1`, `NativeEnqueueUncertainV1`, `SubmissionOutcomeMessageV1`, `PreparedSubmissionV1`, `PreparedSubmissionAdapter`, `CommandQueuedReceiptV1.PreparedCommandRef`, `EmbeddedDelayService.enqueueOutcomeV1`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter.submit`, `WireIngressOutcomeSupport`, `ProtocolCodecTest`, `PreparedCommandV1Test`, `EmbeddedDelayServiceTest`, `AdapterIngressTest`, `NativeSubmissionAdapterTest`; closed branch tags, exact prepared/ref hash binding, retryability and physical-attempt checks, canonical managed NDL1/native prepared branches, exact managed/native branch dispatch without reselection, V1 managed submission strict `encodeFrameV1/decodeFrameV1` validation before transport ownership, strict V1 frame digest derivation for `PreparedCommandRefV1`, compatibility-body rejection in both submission and receipt projections, embedded queued/definite/uncertain projection, Kafka/Pulsar queued ACK and authenticated definitive-rejection proof projection, native capability signature/expiry and pinned-resource checks before transport ownership, native receipt/guard-proof/uncertain/local-definite projection, and conservative downgrade when evidence is absent; durable guard/credential protection, authenticated Producer ownership and production response evidence remain pending |
| Hard shard quota admission | Implemented (core subset) | `ShardQuota`, `ShardQuotaTest`, `OutcomeReserveUsage`, `PublishAdmissionBody.ChargeVector`, `CapacityDimensionV1`, `CapacityVectorV1`, `CapacityGrantV1`, `QuotaGrantRefV1`, `ShardCapacityEnvelopeV1`, `DelayShardConfig`, `DelayShard`, `KeyCodecTest`, `DelayShardTest`, `CapacityVectorV1Test`, `ShardCapacityEnvelopeV1Test`; shard-local outcome reserve records/bytes are durable and source-ordered, with `ADMISSION_CAPACITY_GATED` Claim rollback, admission charge and definitive/verified settlement release in one WriteBatch; single message/reservation add/remove/commit entries reject negative bytes before applying checked arithmetic; an activation-supplied immutable envelope additionally binds the exact outcome grant and persists the full 66-dimensional outcome usage under registered `meta/CONTROL_RESERVE` keys, with restart/rotation checks; class-3/4/5/6 reserve/release now has checked grant-bounded local persistence, class 3/6 are dimension-disjoint under the shared `NON_OUTCOME_CONTROL` grant, and class-6 restart/invalid-dimension tests are covered; source-writer charge integration, Route Broker authority, multi-shard placement/Oxia authority and GC accounting remain pending |
| Kafka/Pulsar ingress and target adapters | In progress (identity-pinned transport SPI) | release blocker until concrete pinned transports, authenticated non-persistence classifiers/proofs, target publish/evidence channels, production response evidence and real-broker tests exist |
| Recovery Set/Floor, catalog and restore replay | In progress (local catalog/Floor subset) | release blocker; Oxia catalog/session pin, immutable publication, source/evidence replay and activation CAS remain |
| Large payload, quota grants, control reserve and GC | In progress (reservation/commit, shard hard-quota, 66-dimensional vector/grant/envelope codec, bound outcome-reserve usage, checked class-3/4/5/6 reserve arithmetic, disjoint system-writer projection, retire-intent, delete-confirmed and catalog-backed local compaction subsets) | release blocker; `CapacityVectorV1`/`CapacityGrantV1`/`QuotaGrantRefV1`/`ShardCapacityEnvelopeV1` enforce the closed dimension registry, zero-explicit ordered amounts, grant/envelope digests, logical charge projection, component-grant projection and checked arithmetic locally. The bound `DelayShard` path persists class-1 envelope identity and class-2 exact outcome usage, exposes synchronous grant-bounded reserve/release for classes 3–6, and enforces the class-3/class-6 dimension partition plus combined `NON_OUTCOME_CONTROL` grant bound; it scans classes 3–6 during activation and rejects stale/unknown, over-capacity or cross-partition projections instead of ignoring them. The legacy `OutcomeReserveUsage` projection remains as a compatibility aggregate. `ResourceRetireIntentBody`/`ResourceRetireIntentRecord` plus `ResourceDeleteConfirmedBody`/`ResourceDeleteConfirmedRecord` provide canonical source-ordered `gc_cf/TASK` intent/tombstone persistence with applied mutation sequence; direct GC reads fence resource kind/hash/version against the embedded retire intent, including nested delete-confirmation intents; `RecoveryCatalogAuthority`/`OxiaRecoveryCatalog` plus `ResourceGcGuard` enforce local ancestry/source/sequence coverage and fail closed when an active RecoveryPin protects a checkpoint resource or its pin state cannot be read, `DelayShard.compactResourceDeleteConfirmation` removes only a covered unpinned local tombstone, and local payload/checkpoint version/etag comparison is enforced, but Route Broker source-writer operation charging/authority, Object Store/Oxia publication, multi-shard grant placement/authority, real provider delete attestation/ownership, durable catalog/Floor barrier, Lane terminal guard and full guarded GC remain |
| Query, control operations, DLQ and observability | In progress (wire unions plus bounded local receipt/barrier/DLQ/SLO bridge) | `MessageQuerySnapshot`, `ReservationQuerySnapshot`, `DlqExportRecord`, `DlqExportResultBody`, source-ordered `DelayShard` DLQ export apply, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1/queryCommand/queryMessage/registerControlOperation/advanceControlOperation/queryControlOperation`, `ControlOperationQueryResponseV1`/`CurrentControlOperationV1`/`ControlTargetStateViewV1`/`ControlTypedResultV1`, `ControlOperationAuthority`, `InMemoryControlOperationAuthority`, `OxiaControlOperationAuthority`, `SloObjectiveV1`/`SloSampleEventIdentityV1`/`SloSampleStartV1`/`SloSampleFinalV1`/`SloObservationOutboxV1`/`SloObservationOutboxStore` and closed SLO enum/time codecs, all V1 Command/Message query view codecs, `DelayShardTest`, `DlqExportRecordTest`, `DlqExportApplyTest`, `ControlOperationQueryResponseV1Test`, `ControlOperationAuthorityTest`, `SloObjectiveV1Test`, `SloObservationOutboxV1Test`, `SloObservationOutboxStoreTest`, `ShardStoreTest`, `EmbeddedDelayServiceTest`, `ProtocolCodecTest`; Dead Letter terminalization writes the deterministic `terminal_cf/DLQ_EXPORT` `NOT_CONFIGURED` record atomically; configured local outboxes can now apply signed `DLQ_EXPORT_RESULT_V1` transitions with checked attempt succession, PENDING next-attempt advancement, terminal monotonicity and mutation dedupe; the Control Operation query response union now has canonical CURRENT/error/target-marker/revision/typed-result wire validation, while the local authority and embedded entry points add receipt-bound idempotent registration, strict revision CAS and fixed retention-bound queries; SLO objective digest, direction/unit/population/exclusion semantics, all 14 objective branch tags/common identity field-shape checks, Start threshold timeout, exact sample/start/final digests, start matching, `meta_cf/SLO_OUTBOX` key/value-envelope persistence, key/value sample identity fencing and conservative AT_MOST/AT_LEAST Final merge are locally covered. Real target/evidence adapter ownership, production receipt/barrier routing, authorization-safe binding/evidence/retention lookup, durable Oxia control-operation state/routing, crash reconstruction, collector merge/export and observability remain release blockers |
| Real-service, chaos, benchmark, soak and upgrade evidence | Not started | release blocker |

## Verification command

Use the checked-in Gradle Wrapper and an isolated cache on hosts where the
default Gradle native cache is not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
