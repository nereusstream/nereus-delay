# V1 Implementation Status

Spec revision: `V1-FROZEN-2026-08-01`

This file records implementation evidence. It does not relax or replace the
normative requirements in [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md),
the [`V1 Protocol Registry`](V1-PROTOCOL-REGISTRY.md), or the Accepted ADRs.
An unchecked item is not an implementation permission; it is a release blocker.

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
fence a due ALL_ACCEPTED exclusion to the closed HEALTHY companion set.
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
without an authoritative Admission replay. DLQ-domain application and external
policy publication/authority remain release blockers.

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
canonical field-8 attempt bytes are retained when available. Execution beyond those
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
Capability semantic with the same Adapter are present; shard-local source
activation/deprecation markers still run before this resolver gate. A missing,
wrong-kind or adapter-mismatched capability is rejected before the delegated
Lane projection (`ProfileCatalogV1ScheduleResolverTest.failsClosedWhenReferencedDeliveryCapabilityIsMissing`).

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
covers both invalid slot directions. The canonical Lane tuple remains an
externally resolved opaque byte projection in this compatibility layer, so full
tuple/profile byte projection remains a resolver/authority gate rather than an
invented local parser.
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

`CommitLargeScheduleV1` now has an explicit canonical body with the same
common fields 1–3, reservation identity and nested typed `PayloadCommitProofV1`.
The proof codec covers the Registry's typed Object Store Profile, tenant scope,
optional etag presence, proof-id/signature digests and Ed25519 verification.
`DelayShard` consumes both the typed V1 proof view and the legacy proof adapter
through the same trust-set/commit state machine; source-position trust-set
authority and production Object Store attestation/ownership remain release
blockers. `InMemoryPayloadObjectStore` now provides a deterministic local
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

`PersistentLaneScheduler` now computes the next ring and READY-cursor wrap
generations as local projections and advances the in-memory counters only after
the five-value scheduler `WriteBatch` succeeds. A failed projection write or
READY decode therefore cannot advertise a generation that was never durable;
`LaneSchedulerTest.failedSchedulerProjectionWriteDoesNotAdvanceGenerationInMemory`
and `LaneSchedulerTest.failedReadyProjectionDecodeDoesNotAdvanceWrapGenerationInMemory`
cover both local failure boundaries.

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
Lane PAUSE/RESUME/BREAK/CLOSE projection now also delegates its nested
`ControlReasonV1` and `AcknowledgementSetV1` values to the canonical codecs;
unknown reason kinds, malformed optional hashes and non-canonical acknowledgement
entries cannot pass the manual lane-target projection. The negative coverage is
in `PayloadProofControlPayloadV1Test`.

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
RSS/RLIMIT values, cgroup v2/v1 memory limits and the exact root filesystem's
total/usable bytes; missing, unlimited or malformed platform values fail closed.
`WorkerResourceEnvelope.validate(config, observation)` compares those values
against the certified envelope, and `SharedRocksDbResources.withRuntimeProbe`
exposes the startup wiring. `WorkerRuntimeSafetyGate` now provides the explicit
sticky `ACTIVE -> DRAIN_OR_MIGRATE` transition for a failed fresh observation,
plus `STAGED -> DRAIN_OR_MIGRATE -> ACTIVE` only after the old DB/ownership
boundary is empty; shared shard acquisition/restore slots and the embedded
Claim helper consult the gate before admitting new work. Parser, envelope and
gate regressions are covered by `WorkerRuntimeResourceProbeTest`,
`WorkerRuntimeSafetyGateTest` and `WorkerResourceEnvelopeTest`. This remains an
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
`WorkClassTask` and `WorkClassScheduler` now provide a seven-class local
queue/turn seam with bounded queue records/bytes, per-turn records/bytes/time,
lease/fence preemption and stale-class selection. `WorkClassResourcePool`
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
without overwriting an existing command identity/result. The POSITION audit now
also makes an exact replay of a fence rejection or command-ID conflict
idempotent after a successful RocksDB batch with a lost source ACK; it returns
the same position-level result without creating a logical Command Result. Both
Command and System Mutation exact replays fail closed when the matching
POSITION audit is missing; the same-hash duplicate path also validates that
locator after restart before reusing the first logical result at a later
physical position. `DelayShardTest` covers both replay paths. Reservation-expiry
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
of allowing callers to bypass the catalog pair. `SloObservationOutboxStoreTest`
`excludedFinalRequiresPairedHealthyObjectiveAtDurableBoundary` covers this
boundary.
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
`gc_cf` intent with its applied shard mutation sequence and source position.
The expected resource-state version now remains a raw unsigned `uint64` from
canonical body decoding through logical identity, GC locator, durable record,
lookup and local compaction APIs; this does not relax the separate bounded
mutation-sequence counter. It deliberately does not perform an external
delete, apply
`RESOURCE_DELETE_CONFIRMED_V1`, replace a Lane with its terminal guard, or
infer Recovery Floor release; those remain release blockers.
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
| Registry-shaped `PrepareLargeScheduleV1` | Implemented (canonical codec plus resolver/catalog-backed reservation apply; external authority pending) | `PrepareLargeScheduleBodyV1`, `CommandBodies.prepareLargeV1/decodePrepareLargeV1`, `V1ScheduleResolver`, `RetryPolicyCatalog`, `V1ScheduleBinding`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `DelayShard`, `ClientCommandBodyV1Test`, `V1ScheduleBindingTest`, `ProfileBindingControlStateTest`, `DelayShardTest`; common fields 1–3, intent-without-payload, expected length/SHA-256, reservation TTL/trust-set reference, resolver-derived Lane, source-position Retry Policy semantic lookup and ordering guard, source-ordered Profile first-binding gate and atomic binding sidecar persistence are covered; immutable Profile/Retry/trust-set publication/catalog authority, production Object Store authority and full reservation binding migration remain pending |
| Registry-shaped `CommitLargeScheduleV1` | Implemented (canonical body plus typed proof/outer-identity apply; external authority pending) | `PayloadCommitProofView`, `PayloadCommitProofV1`, `CommitLargeScheduleBodyV1`, `CommandBodies.commitLargeV1/decodeCommitLargeV1`, `PreparedCommand.commitLargeV1`, `CommandCodec.encode/decode*V1`, `DelayShard`, `InMemoryPayloadObjectStore`, `CommitLargeScheduleBodyV1Test`, `InMemoryPayloadObjectStoreTest`, `DelayShardTest`; common fields 1–3, reservation identity, typed Object Store Profile/tenant scope, optional etag presence, proof-id/signature digests, strict canonical field order, outer/body identity fencing, local exact-reservation handle issuance/upload/attestation, bounded handle expiry/re-sign and trust-set verification are covered; source-position trust-set authority, real Object Store credential/provider ownership and full reservation binding migration remain pending |
| Registry-shaped `CancelV1` / `RescheduleV1` | Implemented (canonical body/outer identity plus local runtime application) | `MessagePreconditionV1`, `CancelCommandBodyV1`, `RescheduleCommandBodyV1`, `CommandBodies.cancelV1/decodeCancelV1`, `CommandBodies.rescheduleV1/decodeRescheduleV1`, `PreparedCommand.cancelV1/rescheduleV1`, `CommandCodec.encode/decode*V1`, `DelayShard`, `RemainingClientCommandBodyV1Test`, `RemainingPreparedCommandV1Test`, `DelayShardTest`; independently present generation/state-version preconditions are checked against current messages/reservations and persisted through the same atomic cancellation/reschedule transition, with common fields 1–3, outer/body identity/type/retry binding, canonical timing and strict field order covered |
| Registry-shaped `RetryPolicySemanticV1` | Implemented (canonical value/hash codec, source-position catalog gate and catalogued business retry revalidation) | `RetryPolicySemanticV1`, `RetryPolicyRefV1`, `RetryJitterV1`, `PublishOutcomeBody`, `PublishAttemptLedger`, `RetryPolicyCatalog`, `InMemoryRetryPolicyCatalog`, `UncertainPolicyV1`, `DlqExportModeV1`, `RetryPolicySemanticV1Test`, `PolicyCatalogTest`, `PublishOutcomeBodyTest`, `PublishAttemptLedgerTest`, `DelayShardTest`; fields 1/4–18, domain-separated semantic hash, typed ref projection, checked exponential cap and Registry jitter scaling, uncertain/DLQ presence rules, FIFO possible-duplicate guard, exact catalog ref/hash matching, source publication visibility fences, V2 Admission-ledger retry-window persistence, catalog-less typed-window validation, canonical Admission first-attempt extraction, and source-ordered business Outcome/Evidence retry ref/deadline/jitter revalidation are covered; legacy opaque-ledger upgrade, DLQ-domain application, authenticated activation authority and durable historical retention remain pending |
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
| NDR1 receipt frame | Implemented (queued/applied/reservation/control/native receipt/prepared payload subset) | `ReceiptFrame`, `ReceiptKind`, `CommandQueuedReceiptV1`, `CommandAppliedReceiptV1`, `PayloadReservationReceiptV1`, `PayloadProofTrustSetRefV1`, `ControlOperationReceiptV1`, `PulsarBrokerResourceIdentityV1`, `NativeCapabilitySnapshotV1`, `PulsarMetadataV1`, `NativePreparedDeliveryV1`, `NativePreparedRefV1`, `NativeDeliveryReceiptV1`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1`, `WireCommandIngressAdapter`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter`, `NativeSubmissionAdapterTest`; registry zero-payload vector, canonical PreparedCommandRef/ProtocolTuple, Kafka/Pulsar Source Position and SafeBrokerAck agreement, queued-to-applied digest/source fencing, barrier-gated applied-frame emission, apply-status/message-field consistency and generation/state-version/binding presence fencing, object-store profile/object identity/trust-set pinning, control operation/scope/target/evidence/query-boundary pinning, Pulsar-only native target/ACK identity matching, signed capability snapshot canonical digest/Trusted-UTC binding/Ed25519 verification, strict optional Pulsar metadata and key-sorted unique property encoding, native snapshot projection/expiry/attestation matching, unsigned high-bit native physical-partition and Pulsar physical-topic-creation-timestamp projection, submission-hash and prepared-ref byte projection, capability bits and physical-attempt/digest checks, query boundary/capability/physical-attempt/digest checks, Kafka queued ACK and definitive rejection proof projection, Pulsar batch-aware queued ACK and guard rejection proof projection, native persisted/guard-rejection/uncertain/local-fence projection, and flags/length/kind/CRC/Base64url rejection tests; durable guard/credential protection and real Broker response transports remain pending |
| Query response closed unions | Implemented (wire codec plus bounded local bridge) | `ProfileRefV1`, `PublicDestinationBindingViewV1`, `PublicEvidenceRefV1`, `CheckpointSummaryV1`, `CheckpointCatalogResultV1`, `CheckpointControlResultV1`, `LaneControlResultV1`, `ShardControlResultV1`, `ProfileControlResultV1`, `QuotaControlResultV1`, `MessageControlResultV1`, `RouteControlResultV1`, `SecretRotationResultV1`, all Registry Message/Command view classes, `CommandQueryResponseV1`, `MessageQueryResponseV1`, `ControlOperationQueryResponseV1`, `CurrentControlOperationV1`, `ControlTypedResultV1`, `PublicQueryErrorV1`, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queryCommand/queryMessage`, `DelayShard.matchesCommandHash`, `ProtocolCodecTest`, `CheckpointCatalogResultV1Test`, `CheckpointControlResultV1Test`, `ControlResultCodecTest`, `ControlOperationQueryResponseV1Test`, `EmbeddedDelayServiceTest`; exact branch tags/field order, Source Position barrier ordering, durable `dedupe_cf` command-hash binding (`RECEIPT_MISMATCH` on same-ID hash drift), state/status agreement, command-view optional presence fencing, safe NFC alias and payload/DLQ/evidence enum checks, canonical checkpoint-catalog shard/Floor/sorted-summary validation, checkpoint-control identity validation, all nine control-result branch field/presence/identity codecs with strict branch-to-payload dispatch and round-trip/rejection vectors, fixed-source queued-receipt barrier and retention projection, and canonical Control Operation CURRENT/error/target/revision/typed-result projection; production receipt routing, authorization-safe lookup, source-derived retention, Oxia ownership, durable control-operation query state and observability remain pending |
| System Mutation envelope, type registry, canonical hash/ID and Ed25519 signature | Implemented (bounded control plus admission/expiry/outcome/evidence/claim-result/resource-retire/delete-confirmed subset) | `SystemMutationType`, `SystemMutation`, `ShardSubjectV1`, `SystemMutationBodyCodec`, `ApplyShardControlBody`, `ReplayDeadLetterBody`, `ResolveUncertainBody`, `ControlRef`, `ControlReasonKindV1`, `ControlReasonV1`, `ProfileBindingActivatePayloadV1`, `ProfileNewBindingClosePayloadV1`, `ProfileBindingControlState`, `PayloadProofTrustSetActivatePayloadV1`, `PayloadProofIssuanceClosePayloadV1`, `PayloadProofTrustSetControlState`, `PayloadProofTrustSetControlCatalog`, `PublishAdmissionBody`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `PublishOutcomeBody`, `ClaimResultBody`, `ResourceRetireIntentBody`, `ResourceRetireIntentRecord`, `ResourceDeleteConfirmedBody`, `ResourceDeleteConfirmedRecord`, `TrustedUtcIntervalEvidence`, `SystemMutationResult`, `AuthorIdentity`, `ClaimRecord`, `GenerationRuntimeIndex`, `DelayShard`, `ProtocolCodecTest`, `ShardSubjectV1Test`, `ReplayDeadLetterBodyTest`, `ResolveUncertainBodyTest`, `PublishAdmissionBodyTest`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `ResourceRetireIntentBodyTest`, `ResourceDeleteConfirmedBodyTest`, `GenerationRuntimeIndexTest`, `PayloadProofControlPayloadV1Test`, `ProfileBindingControlStateTest`, `PayloadProofTrustSetControlStateTest`, `DelayShardTest`; canonical owner/control/fence/service branches, strict canonical ShardSubject route/partition decoding shared by envelope and body checks, body fields 1–3, required/optional operation fields, wire widths, bool bounds, canonical nested bytes, canonical typed `RetryPolicyRefV1` Replay field, canonical Replay/Resolve encoding and shared message-bearing body self-routing checks for Admission/Claim/DLQ/Expire, durable `dedupe_cf/SYSTEM_MUTATION` with key/value mutation identity and Source Position shard fencing, explicit signature verification, source-ordered Profile activation/close and trust-set activation/issuance-close with semantic-reference checks and atomic marker-state persistence/reopen, source-ordered Lane PAUSE/RESUME/BREAK/CLOSE with ControlRef/identity/incarnation/CAS, close-policy/acknowledgement checks and atomic Claim/READY rollback, source-ordered TIME_FENCE watermark and reservation-expiry overlay/materialization, source-ordered `EXPIRE_GENERATION_V1`, `PUBLISH_ADMISSION_V1` descriptor/Ready Certificate/Claim identity projection plus adapter/encoding/target/partition/channel-Profile cross-object fences, replay-stable timeline-key/semantic-digest/counter/obligation-set preconditions, checked attempt-number and uncertain-retry counter projection, definitive `PUBLISH_OUTCOME_V1/NOT_PUBLISHED` disposition/retry-shape subset, verified-published `ATTACH_PUBLISHED_EVIDENCE`, `ATTACH_NOT_PUBLISHED_EVIDENCE` all-absent normalization, and verified `EVIDENCE_RESOLUTION_V1` transition subsets, replay-stable permanent `CLAIM_RESULT_V1` ClaimPrecondition/timeline/claimed-charge terminalization subset with canonical transfer equality and source-apply rejection (`DelayShardTest.sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline`), uniform handler arithmetic-overflow fencing (`DelayShardTest.systemMutationStateVersionOverflowPersistsStaleResult`), closed ExactResourceIdentity/ProtectionSet parsing with branch-specific Object Store Profile kind and zero-length payload/manifest support, plus registered logical-identity verification and atomic `gc_cf/TASK` retire-intent persistence, exact RetireIntentRef/DeleteOutcome/ExternalDeleteEvidence matching and source-ordered local tombstone persistence, local durable `SCHEDULED -> CLAIMED -> revoke/Admission/ClaimResult/Cancel/Reschedule/expiry` transitions, and v4 `id_cf/MESSAGE` runtime-index writes are covered; immutable Profile/catalog and authenticated source control authority, source-protected signing-key trust/ACL, immutable Oxia target registration, Recovery Floor barriers, full ActiveLaneState persistence, obligation-set quota/recovery accounting and full Claim materialization/recovery model remain pending |
| Kafka/Pulsar source order token and source identity fencing | Implemented (u64/u32 local protocol paths; external authority pending) | `SourcePosition.sourceOrderToken`, `SourcePositionCodec` byte-round-trip canonical decode with explicit truncated length/fixed-field rejection, Kafka offset/Pulsar ledger-entry-batch order, exact canonical-position fencing for same physical token, physical-resource comparison guard, unsigned high-bit comparison/successor, canonical protobuf receipt/evidence/barrier paths, queued-receipt PreparedCommandRef-to-Source-Position shard binding in the shared constructor/decode path (`ProtocolCodecTest.commandQueuedReceiptRejectsACommandAndSourceFromDifferentShards`), unsigned checkpoint-manifest source/evidence fields, direct Source Position construction rejects malformed/non-NFC text before identity bytes are produced (`ProtocolCodecTest.sourcePositionsRejectNonCanonicalTextAtConstruction`, `ProtocolCodecTest.sourcePositionDecoderRejectsTruncatedLengthAndFixedFields`, `ProtocolCodecTest.sourcePositionsRoundTripUnsignedHighBitOffsetsThroughReceiptAndEvidenceCodecs`, `ProtocolCodecTest.sourcePositionsPreserveUnsignedPartitionLeaderAndBatchFields`, `ProtocolCodecTest.trustedUtcEvidencePreservesUnsignedCounterBitPatterns`, `ActivationBarrierV1Test.preservesUnsignedPartitionAndBatchFields`, `EvidenceCursorV1Test.preservesUnsignedPartitionAndBatchFields`, `SourceReplaySuccessorTest.strictKafkaAcceptsTheUnsignedHighBitBoundarySuccessor`, `CheckpointManifestTest.manifestRoundTripsUnsignedSourceAndEvidencePositions`), including Registry Pulsar `physicalTopicCreationTimestamp:u64be` through broker identity/queued ACK, evidence, adapter and manifest projections; `DelayShardTest`; remaining auxiliary uint64/time fields, real Broker assignment/barrier adapters and production authority remain release blockers |
| Pinned Kafka/Pulsar command ingress outcome mapping | Implemented (transport SPI plus Kafka/Pulsar wire projection) | `PinnedKafkaCommandIngress`, `PinnedPulsarCommandIngress`, `WireCommandIngressAdapter`, `WireIngressOutcomeSupport`, `KafkaIngressResource`, `PulsarIngressResource`, `KafkaProduceRequest`, `PulsarSendRequest`, `KafkaProduceResult`, `PulsarSendResult`, `AdapterIngressTest`, `AdapterRequestIdentityTest`; Kafka topic UUID and Pulsar resource token plus physical topic creation identity are carried at the request boundary, ingress resources and direct request/result transport identities enforce canonical UTF-8/NFC text, invalid 16-byte physical attempts are rejected locally before Producer ownership, result dispositions form a closed matrix (`PERSISTED` + `OK` versus non-persisted stable code with no success position), persisted Pulsar results fence all pinned identity fields, managed Kafka/Pulsar transport failures, null results and malformed receipt projections use `ENQUEUE_RESULT_UNCERTAIN` without leaking an exceptional Future, managed result projection normalizes native-only guard/uncertain codes while only the native adapter uses `NATIVE_ENQUEUE_RESULT_UNCERTAIN`, and both managed adapters can project queued/definitive-proof/uncertain NDR1 outcomes with evidence fail-closed; concrete pinned request transports, authenticated production rejection classifiers and source assignment pending |
| Target publish side-effect outcome boundary | Implemented (identity-fenced transport SPI, local physical admission seam and durable attempt/outcome subset) | `DestinationPublishAdapter`, `DestinationPublishResult`, `PinnedKafkaDestinationAdapter`, `PinnedPulsarDestinationAdapter`, `PulsarDestinationTimingPolicy`, `KafkaTargetResource`, `PulsarTargetResource`, `KafkaDestinationRequest`, `PulsarDestinationRequest`, `PulsarNativeSendRequest`, `DestinationPhysicalAdmission`, `BoundedDestinationPublishAdapter`, `PublishAttemptLedger`, `PublishOutcomeBody`, `DelayShard`, `DestinationAdapterTest`, `AdapterRequestIdentityTest`, `DestinationPhysicalAdmissionTest`, `BoundedDestinationPublishAdapterTest`, `DelayShardTest`; PUBLISHED results now require non-empty delivery identity/evidence, use stable code `OK`, and pair an optional pinned `BrokerResourceIdentityV1` with a uint32 physical partition; target resources, direct request values and the physical-admission target-cluster registry enforce canonical UTF-8/NFC cluster/topic identity before request construction or capacity accounting, and native request values reject a zero delivery identity; Kafka destination requests additionally require `actionAt=deliverAt`, so the early-action branch cannot reach transport; the Pulsar destination adapter defaults to the same ordinary timing relationship and accepts an early action only through an explicit fixed-lead `PulsarDestinationTimingPolicy`, which is a local pre-transport guard rather than Profile/Capability authority (`DestinationAdapterTest.pulsarDefaultTimingPolicyRejectsEarlyActionBeforeTransport`, `DestinationAdapterTest.pulsarCertifiedHandoffRequiresTheExactFixedLead`); local admission protects Worker and target-cluster request/byte caps, READY Lane minimums, Lane caps and zombie charges, counts a not-yet-ready candidate Lane's protected minimum exactly once when opening READY, and releases only on delegate-stage completion; `BoundedDestinationPublishAdapter` dispatches delegate calls through an injected/default asynchronous Lane/Adapter executor instead of holding the adapter monitor across a synchronous transport call, and `blockingDelegateCallDoesNotBlockHealthyLane` covers same-adapter Lane isolation; executor rejection remains a pre-ownership release, while Pinned destination adapters mark synchronous transport exceptions, null stages, or unobservable callback registration as logical `UNKNOWN` without a physical-completion proof, and the wrapper retains those charges as `ZOMBIE`/in-flight until `PublishCall.releasePhysicalCharge()` follows certified completion or fenced teardown (`BoundedDestinationPublishAdapterTest.callbackRegistrationFailureRetainsPhysicalChargeUntilExplicitRelease`, `BoundedDestinationPublishAdapterTest.pinnedAdapterRegistrationFailureRetainsPhysicalCharge`, `BoundedDestinationPublishAdapterTest.pinnedAdapterTransportExceptionRetainsPhysicalCharge`, `DestinationAdapterTest.kafkaDestinationDoesNotInvokeTransportForEarlyActionAt`); the default virtual-thread executor is only a local seam and production bounded executor, physical adapter evidence journal, durable ActiveLaneState/ReadyCertificate admission authority, authenticated non-persistence classifiers, and remaining outcome/evidence mutations remain pending; definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable state transitions, service-authored verified `EVIDENCE_RESOLUTION`, and Resolve `ATTACH_PUBLISHED_EVIDENCE`/`ATTACH_NOT_PUBLISHED_EVIDENCE` obligation settlement are covered locally |
| One Delay Shard = one RocksDB DB | Implemented | `ShardStore`, `StoreMetadata`, `StoreRuntimeMetadata`, `SharedRocksDbResources`, `ShardStoreTest`, `StoreRuntimeMetadataTest`; existing DBs missing the `meta_cf` shard-identity marker or carrying a store incarnation that disagrees with `incarnations/<storeIncarnation>/db` now fail closed instead of being initialized or opened as a different store; the local `meta/FIXED` runtime projection persists ingress-fence/checkpoint identity at keys 4/7, owner-open epoch at key 8, typed evidence cursors at key 6 and clean-close state at key 9, while reserved control-snapshot key 10 is not consumed; owner-open epochs use raw uint64 encoding and unsigned monotonic comparison, including the high-bit boundary; the shared owned-shard semaphore is identity-bound to `ShardId`, so duplicate opens fail before a second DB incarnation can be created or installed and the exact identity is released only when its Store closes (`ShardStoreTest.duplicateOwnedShardOpenIsRejectedBeforeCreatingAnotherDb`); all values use bounded canonical validation and post-open metadata/format/install failures close every DB/Column Family handle and options object before slots release, covered by `ShardStoreTest.malformedExistingMetadataDoesNotLeaveRocksDbOpen` and `ShardStoreTest.malformedRuntimeMetadataDoesNotLeaveRocksDbOpen`; after the clean-close marker is durably written, all RocksDB read, scan, write, flush and sequence-number APIs fail closed instead of touching a closed native handle, covered by `ShardStoreTest.closedShardStoreFailsClosedForAllRocksDbOperations` |
| Worker DB/checkpoint resource limits | Implemented (config-envelope plus runtime shard/DB/acquire/restore slots, startup and fixed-delay runtime probes, sticky runtime safety gate, local native bucket ledger, placement scorer and physical usage guard seam) | `ShardStoreConfig`, `SharedRocksDbResources`, `ShardStore`, `WorkerResourceEnvelope`, `WorkerRuntimeResourceObservation`, `WorkerRuntimeResourceProbe`, `WorkerRuntimeResourceMonitor`, `WorkerRuntimeSafetyGate`, `NativeResourceUsage`, `WorkerNativeResourceLedger`, `WorkerCapacityAdmission`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `RocksDbUsageSnapshot`, `RocksDbUsageLimits`, `WorkerResourceEnvelopeTest`, `WorkerRuntimeResourceProbeTest`, `WorkerRuntimeResourceMonitorTest`, `WorkerRuntimeSafetyGateTest`, `WorkerNativeResourceLedgerTest`, `WorkerCapacityAdmissionTest`, `WorkerPlacementPolicyTest`, `RocksDbUsageLimitsTest`, `ShardStoreTest`; checked memory/FD/disk cross-bucket inequalities fail closed before resource creation, the disjoint native ledger attributes shared block-cache/WBM reservations before JNI creation with exact allocation identity and checked release, logical `maxOwnedShards` and physical `maxOpenShardDbs` slots are tracked independently, short-lived shard-open/restore acquisition has an independent `maxConcurrentAcquiresPerWorker` slot released after native open or cleanup, checkpoint restore/download staging has an independent bounded slot released after atomic installation, the process-shared RocksDB `Env` background pool is explicitly configured from `maxBackgroundJobs`, each DB binds `maxBackgroundJobsPerDb` plus nonzero `reservedFlushJobs`/`maxCompactionJobs` split, every Column Family receives the explicit `maxWriteBufferBytesPerDb` write-buffer option while the process-wide `WriteBufferManager` remains the shared memtable budget, shared RocksDB resources refuse premature close, local Worker admission sums distinct shard `committed` vectors plus fixed/transition demand once with checked per-dimension arithmetic, and the local placement seam rejects over-capacity candidates before applying dominant-resource/load scoring with stale-telemetry penalty, minimum residence, hysteresis and movement cost; `WorkerRuntimeResourceProbe` reads bounded JVM heap/direct-memory, procfs RSS/RLIMIT, cgroup v2/v1 and exact root filesystem total/usable limits, `WorkerResourceEnvelope.validate(config, observation)` rejects any certified envelope that exceeds them, and `WorkerRuntimeSafetyGate` fences new ownership/restore plus the embedded Claim path after a failed fresh observation until explicit empty-drain activation; `WorkerRuntimeResourceMonitor` runs a closeable fixed-delay probe and routes probe/validation failures into the same sticky drain state; `RocksDbUsageSnapshot` observes live SST/WAL/MANIFEST/L0/compaction-pending and exact DB file totals, while `RocksDbUsageLimits` checks duplicate shard identity, per-DB caps, checked Worker totals and the exact root filesystem free-space floor; `ShardStoreTest.physicalUsageProbeAndGuardObserveOneShardDb`, `WorkerRuntimeResourceProbeTest`, `WorkerRuntimeResourceMonitorTest`, `WorkerRuntimeSafetyGateTest`, `WorkerNativeResourceLedgerTest` and `RocksDbUsageLimitsTest` cover the local probe/ledger/guard; per-DB dynamic attribution, per-work-class reserve enforcement, cooperative assignment or Oxia placement capacity authority remain pending |
| Seven application CFs plus empty `default` CF | Implemented | `ShardStore` descriptor validation |
| Worker dynamic per-DB physical-usage observer | Implemented (local monitor lifecycle and aggregate guard; authority pending) | `WorkerRocksDbUsageMonitor`, `SharedRocksDbResources.startRocksDbUsageMonitor`, `ShardStore`, `WorkerRocksDbUsageMonitorTest`; every open shard Store registers one identity-bound physical source, the source is removed before native teardown, fixed-delay observations validate per-DB and Worker WAL/MANIFEST/SST/L0/compaction/file caps plus filesystem free-space floor, and missing/invalid/over-capacity evidence fences the shared sticky runtime gate; production write-time reserve attribution, checkpoint/compaction scheduling authority and Oxia placement capacity remain release gates |
| Worker event-loop work-class queue/turn seam | Implemented (local bounded scheduler; production wiring pending) | `WorkClass`, `WorkClassPolicy`, `WorkClassTask`, `WorkClassScheduler`, `WorkClassSchedulerTest`; all seven V1 classes require positive weights and bounded queue/turn records, bytes and time, `LEASE_FENCE` is preemptive, and stale queued classes are selected after the configured maximum delay; production Worker event-loop integration, non-borrowable reserve-token enforcement/borrowing and write-time admission authority remain release gates |
| Worker work-class reserve token seam | Implemented (local checked pool; production wiring pending) | `WorkClassResourcePool`, `WorkClassResourcePoolTest`; exact record/byte leases protect every other class' configured non-borrowable minima, mark borrowed capacity, bound borrowed hold time and release idempotently; production chunk-level reacquisition and WriteBatch/IO admission authority remain release gates |
| Shard identity and local Store Incarnation validation | Implemented | `StoreMetadata`, `ShardStoreTest` |
| Synchronous atomic WriteBatch | Implemented | `ShardStore.write`, `ShardStoreTest` |
| Native RocksDB checkpoint creation | Implemented | `ShardStore.createCheckpoint`, `ShardStoreTest`; checkpoint creation is staged under the same-filesystem `checkpoint-tmp` namespace, rejects an existing target, installs only through an atomic rename, and removes a target that was moved but failed the subsequent parent-directory durability step, with failed-stage cleanup |
| Checkpoint file inventory and canonical manifest projection | Implemented (local CAS projection, bounded upload coordinator and deterministic schedule seam; external publication pending) | `CheckpointFileInventory`, `CheckpointManifestLimits`, `CheckpointManifest`, `CheckpointManifestJson`, `CheckpointResourceV1`, `CheckpointUploadStateV1`, `CheckpointUploadIntentV1`, `CheckpointUploadIntentStore`, `CheckpointReapingGuard`, `CheckpointUploadAdapter`, `CheckpointUploadRequest`, `CheckpointUploadCoordinator`, `CheckpointScheduler`, `CheckpointManifestTest`, `CheckpointResourceV1Test`, `CheckpointUploadIntentV1Test`, `CheckpointUploadIntentStoreTest`, `CheckpointReapingGuardTest`, `CheckpointUploadCoordinatorTest`, `CheckpointSchedulerTest`; inventory streams SHA-256 over each file without loading an SST into heap and rejects symbolic links or non-regular files before restore/copy, while an explicit `CheckpointManifestLimits` boundary fails closed on file count, individual/total bytes, path/manifest bytes, evidence-cursor count and file/manifest-object identity lengths before local hashing or provider I/O (the raw manifest byte cap is enforced before JSON parsing and the file/evidence array bound while parsing); inventory and manifest file names are ordered by unsigned normalized UTF-8 bytes, and total-byte addition overflow is converted to a fail-closed validation error (`CheckpointManifestTest.manifestTotalFileBytesOverflowFailsAsValidationError`); the manifest decoder enforces the closed field order/types, Kafka/Pulsar typed `EvidenceCursorV1` branches, strict cursor identity ordering and byte-identical canonical JSON round trip; the published-manifest Object Store identity and upload-intent state branches have closed canonical codecs, and the local coordinator verifies exact pending intent, deadline, shard/lineage/owner/store/parent identity, complete local file inventory, Worker upload slot and returned manifest length/SHA-256 before PENDING_UPLOAD -> PUBLISHED revision CAS; the local scheduler validates interval/jitter, spreads owned shards deterministically, fences duplicate in-flight claims, and requires the exact returned claim handle for completion so stale callbacks cannot reset a newer attempt; an exact PUBLISHED successor is reread after response loss without another adapter call; the guarded local PENDING_UPLOAD -> REAPING overload also rejects published catalog protection, same-checkpoint active RecoveryPin protection, unavailable catalog/pin reads, and Floor/coverage authority failures, while legacy no-limits overloads remain compatibility seams and real Oxia lease/session/catalog CAS, Object Store upload/attestation/publication, owner-abandonment proof and reaping/quiescence remain pending |
| Checkpoint restore into a new Store Incarnation | Implemented (local manifest/catalog/pin-validated path) | `ShardStore.restoreFromCheckpoint`, `CheckpointManifest.decodeCanonicalJson`, `CheckpointManifestLimits`, `ShardStoreTest`; raw canonical manifest bytes are decoded and catalog-validated before local file verification, the raw manifest byte cap is enforced before JSON parsing and the finite limit set is then applied to the decoded manifest and complete source file inventory before copy, the private restore-tmp copy is re-inventoried against the same manifest before staged open/install, staged DB identity plus persisted `lastCheckpointId`, `appliedShardLogPosition`, `shardMutationSequence` and evidence cursors are compared to the exact manifest before install, the newly renamed Store Incarnation parent is directory-fsynced before the checksummed `ACTIVE` pointer can publish it, and the pin-aware overload rereads the exact active RecoveryPin before staging and before atomic Store Incarnation installation; legacy no-limits restore overloads remain compatibility seams, while Oxia Recovery Pin/Floor CAS and source replay remain pending |
| Recovery catalog, lineage and Floor selection | Implemented (typed local codecs, typed cursor-dominant Floor projection, manifest-bound evidence cursors, upload-intent-bound publication and bounded in-memory pin authority; Oxia CAS pending) | `RecoveryFloorRefV1`, `RecoveryCandidateRefV1`, `RecoveryPinV1`, `EvidenceCursorV1`, `RecoveryCatalog`, `RecoveryCatalogAuthority`, `OxiaRecoveryCatalog`, `RecoveryFloor`, `RecoveryFloorRefV1Test`, `RecoveryCandidateRefV1Test`, `RecoveryPinV1Test`, `EvidenceCursorV1Test`, `RecoveryCatalogTest`; the typed references canonically bind lineage/checkpoint/manifest, catalog generation, Source Position, mutation sequence, sorted evidence cursors, candidate branch and session identity digest; the local catalog still binds one shard, rejects non-zero genesis lineage, enforces floor ancestry, requires the Floor cursor set to byte-match the candidate manifest and then enforces same-generation cursor dominance, exposes candidate validation/selection and `proveFloorCoverage`, independently fences requested mutation/source boundary coverage with canonical Source Position equality on equal order tokens, requires a PUBLISHED `CheckpointUploadIntentV1` plus exact manifest/object/owner/store/parent identity for the local publication projection, validates the same request binding before an Oxia upload-intent CAS, copies mutable checkpoint/digest/cursor/coverage inputs before backend CAS, validates optional Oxia publication Floors and all candidate/ancestry manifests against their complete canonical JSON projection, shard and publication generation, and validates every Oxia coverage ancestry edge against published parent id/hash, lineage/source/mutation progression and evidence-cursor dominance; supports one exact active-pin create/idempotent-reread/release projection with typed Floor equality checks; durable Oxia Owner Lease/session and catalog/Floor CAS, real Object Store publication/attestation, and evidence-cursor retention/dominance enforcement remain pending |
| Command applied/rejected state machine | Implemented (embedded core) | `DelayShard`, `DelayShardTest`, `DurableResultTest` |
| DUE/ORDERED/READY/EXPIRY timeline namespaces | Implemented (embedded core subset) | `DelayShard`, `MessageRecord`, `TimelineWorkRef`, `GenerationRuntimeIndex`, `ClaimRecord`, `ReadyIndexValue`, `KeyCodec`, `GenerationRuntimeIndexTest`, `KeyCodecTest`, `DelayShardTest`; READY key/value, laneVersion fencing, retry eligibility for unordered definitive retry, canonical timeline semantic/instance digests, v4 runtime-index persistence, atomic affected-lane updates, durable Claim removal/restoration including source-ordered Lane Pause rollback, fenced rebuild/discovery, replay-stable Claim Result timeline-key/semantic-digest checks, current `id_cf/MESSAGE` reads and activation/close/retirement scans fenced to the self-routing key shard and embedded schedule Source Position shard, Cancel/Reschedule fencing whenever an UNCERTAIN obligation survives a current-work projection, the pinned-policy `UNKNOWN` scheduling plus `UNCERTAIN_RETRY` Admission subset that materializes timeline work while retaining the old obligation, and exact constructors for the registered FENCE, GC protection, Producer and Recovery key layouts are covered; ControlRef validation and policy-bound timeline materialization beyond this local budget remain pending |
| `CLAIMED`/`PUBLISHING`/`UNCERTAIN` attempt ledger and obligation locator | Implemented (durable local Claim plus source-ordered attempt subset) | `ClaimRecord`, `PublishAttemptLedger`, `AttemptObligationRef`, `GenerationRuntimeIndex`, `PublishAdmissionBody`, `PublishOutcomeBody`, `RetryPolicyCatalog`, `DelayShard`, `DelayShardTest`; local Claim sequence/key/value and exact precondition/instance digest are persisted atomically, registry-shaped runtime index and canonical obligation-set digest are persisted with v4 Message records, source-ordered `PUBLISH_ADMISSION_V1` checks replay-stable timeline key/semantic/counter/obligation projections and descriptor attempt number, including the `UNCERTAIN_RETRY` source-work branch, reconstructs the same PUBLISHING attempt when the reversible Claim is absent but source state matches, retains admission counters across definitive retry timelines, structurally bounds descriptor `actionAt <= deliverAt`, and when the exact Profile catalog is supplied validates ordinary timing or the pinned certified Pulsar handoff lead before admission; when the catalog is supplied it also revalidates the pinned immutable policy budgets at Admission, uncertain retry, and reopen; shard activation now performs bounded bidirectional reconciliation between every current/terminal runtime obligation ref and its exact PUBLISHING/UNCERTAIN ledger, and between every live ledger/Claim and the current Message branch, failing closed on an orphan, missing counterpart, persisted total-admission overflow, or terminal/runtime summary mismatch; source-ordered `PUBLISH_OUTCOME_V1` UNKNOWN atomically migrates to UNCERTAIN, can materialize the pinned-policy `UNCERTAIN_RETRY` timeline subset without consuming the retry counter, but a closed Lane keeps the generation in UNCERTAIN with no retry timeline, verified-success closes the ledger, definitive `NOT_PUBLISHED` retriable/permanent/lane-unavailable outcomes atomically requeue, terminalize, or block the lane, and a definitive not-published outcome on a closed Lane is terminalized as `LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED` without retry; definitive Outcome/Resolution transfer now must be canonical byte-identical to the retained Admission charge, with mismatch persisted as `REJECTED(STALE_SYSTEM_MUTATION)` without changing the attempt/message/timeline/quota, while UNKNOWN transfer remains opaque; late outcomes from either the current or an older generation settle only the exact ledger/terminal summary while preserving terminal decision state and monotonically raising duplicate risk for verified success; service-authored verified `EVIDENCE_RESOLUTION_V1` can close or requeue the exact UNCERTAIN ledger; `meta_cf/QUOTA quotaClass=3` now also records each durable Claim and attempt obligation's `inflight_messages/inflight_bytes` by Lane, rebuilds from `inflight_cf` and repairs a missing legacy map on release; full Profile/Adapter/evidence/capacity validation, immutable RetryPolicy publication/authority, multi-obligation terminal-summary lifecycle beyond current-generation settlement, uncertain-retry ControlRef/policy enforcement beyond the local automatic budget, logical/retained/evidence quota dimensions, Recovery Floor/source replay and full Claim materialization/recovery semantics remain pending |
| Publish Admission timing/profile gate | Implemented (local semantic gate; publication authority pending) | `PublishAdmissionBody.requireTimingPolicy`, `PublishAdmissionBody.requireBrokerTiming`, `DelayShardConfig`, `DelayShard` optional `ProfileCatalog`, `PublishAdmissionBodyTest`, `DelayShardConfigTest`, `DelayShardTest.publishAdmissionTimingFailureRevokesMatchingClaimBeforePersistingStaleMutation`; descriptor structure permits only `actionAt <= deliverAt`, catalogued shards validate exact Destination/Delivery Capability refs, fixed V1 adapter encoding and metadata branch, ordinary managed timing, fixed certified Pulsar handoff lead/capability, target resource and explicit/hash partition policy, and every apply/replay checks source Broker persistence time against configured `maxIngressBrokerTimestampDivergenceMs`/`maximumAdmissionMutationEnqueueAgeMs` with checked expiry and decision-interval distance arithmetic; timing/profile failures revoke an exact matching live Claim before persisting `STALE_SYSTEM_MUTATION`, so no attempt or Producer state is created; catalog-less compatibility shards fail closed to `actionAt=deliverAt`; formal Broker-time certification, Profile publication, Broker guard attestation and production Producer authority remain release blockers |
| Terminal generation history | Implemented (current/older-generation open-obligation summary and late-settlement subset) | `TerminalGenerationRecord`, `ClaimRecord`, `DelayShard`, `DelayShardTest`, `TerminalGenerationRecordTest`; Claim Result, publish outcome, expiry and command terminalization persist the exact terminal state version together with remaining obligation refs and duplicate-risk projection in a versioned terminal record, direct reads fence embedded `messageId/generation` against the `terminal_cf` key and require the applied Source Position to belong to the current Shard, activation reconciles both current and older-generation summaries against their exact inflight ledgers, and late verified or definitive not-published outcomes remove only their exact old ledger/ref without changing terminal status/code/time; legacy v1 terminal records decode as empty summaries; older-generation callbacks after full Dead Letter Replay, Replay retention and guarded GC remain pending |
| Large-payload reservation/proof/commit | Implemented (embedded core plus typed wire proof/response and deterministic local Object Store seam) | `LargeScheduleIntent`, legacy `PayloadCommitProof`, `PayloadCommitProofView`, `PayloadCommitProofV1`, `PayloadReservation`, `PayloadReservationReceiptV1`, `OpaquePayloadUploadHandleV1`, `PayloadUploadHandleResponseV1`, `PayloadAttestationResponseV1`, `InMemoryPayloadObjectStore`, `InMemoryPayloadObjectStoreTest`, object-backed `MessageRecord`, `DelayShardTest`, `CommitLargeScheduleBodyV1Test`, `ProtocolCodecTest`; Prepare/Commit reservation quota, source-ordered TIME_FENCE overlay, bounded `RESERVATION_EXPIRY` discovery/materialization with byte-identical `id_cf` projection fencing, bounded and key/value-identity-checked reservation lookup with duplicate detection plus Source Position shard fencing, guarded local quota release, exact reservation-bound handle issuance, configured lifetime/reservation-expiry bound and post-expiry re-sign, receipt-derived service-owned container/key with exact source/state/trust-set/object-identity validation before handle/upload/attestation, immutable-if-absent upload, typed Object Store proof Profile/tenant-scope/optional-etag/proof-id/signature validation, cached attestation, fixed payload-scoped error branches and typed attestation/commit response round-trips are covered; source-position trust authority, real provider credentials/availability/immutability, full reservation binding migration, fence-key history and guarded GC remain pending |
| Source assignment, typed Activation Barrier and Owner Lease | Implemented (local bounded mixed command/System Mutation replay seam; production authority pending) | `SourceAssignment`, `SourceReplayEntry`, `SourceReplayRecord`, `SourceReplayMutation`, `SourceReplayOutcome`, `SourceReplayCursor`, `SourceReplayTurn`, `ReplayTurnBudget`, `SourceReplaySuccessor`, `KafkaActivationBarrier`, `PulsarActivationBarrier`, barrier-gated `OwnedDelayShard`, `OwnerLease`, `OwnerLeaseContext`, `OwnerLeaseStore`, `OxiaOwnerLeaseStore`, `OwnerDrainCoordinator`, `OwnerLeaseTest`, `OwnerDrainCoordinatorTest`, `SourceReplaySuccessorTest`, `OxiaOwnerLeaseStoreTest`, `SourceActivationBarrierTest`; catch-up now requires an explicit non-zero assignment identity/epoch bound to the typed barrier, assignment/barrier equality compares array-backed resource and guard identities by value, Kafka barrier cluster identity is canonical NFC/UTF-8 at construction, Pulsar barrier resource/guard identities reject all-zero placeholders, the runtime Pulsar barrier pins the inclusive entry `batchSize` and rejects same-entry batch-shape drift before apply, every catch-up cursor and post-activation apply record is checked against the typed physical source identity and rejects same offset/ledger-entry-batch tokens with conflicting canonical metadata before replay; the V1 overload pins an adapter-defined `SourceReplaySuccessor` for the entire catch-up window, accepts only exact canonical redelivery or the proven immediate successor, and the strict Kafka helper rejects offset gaps before applying the skipped record (Pulsar batch-member strictness is available while entry transitions remain adapter-defined); empty Kafka/Pulsar barriers are covered, and an empty Pulsar barrier validates any non-null persisted cursor before allowing activation; the unified bounded `replayTurn` seam and type-specific turn APIs cap record count, canonical frame/position bytes and elapsed monotonic time while preserving the caller cursor across turns, and apply mixed Command/System Mutation entries in one source order through the shard's atomic WriteBatch before advancing the cursor (the whole-`Iterable` methods and assignment-only overload remain explicitly compatibility conveniences); Pulsar replay/catch-up/apply paths additionally require a positive guarded source-connection generation and exact resource-guard attestation digest; context-bound lease acquisition carries assignment identity plus assignment epoch and exact session identity, renewal preserves them and the lifecycle state, stale state transitions fail CAS, and the activation overload requires the authority to CAS the same lease to `ACTIVE_FOR_COMMANDS` before opening the local gate; `OwnerDrainCoordinator` composes the locally provable drain order and leaves callback/source quiescence and production lease/session integration explicit; real Kafka/Pulsar consumer replay, Oxia session/ephemeral records, broker assignment/guard and production activation transaction remain pending |
| Queued vs applied client outcomes | Implemented (embedded core) | `EmbeddedDelayService.queuedReceiptV1`, `appliedReceiptV1`, `enqueueOutcomeV1`, `awaitApplied`, `queryCommand`, `queryMessage`, `CommandQueuedReceipt`, `DelayShard.matchesCommandHash`, `DelayShard.matchesCommandPosition`, `EmbeddedDelayServiceTest`, `ProtocolCodecTest`; queued receipt stays distinct from applied result, its Command/Message/source identities must name the same Shard in both legacy and V1 paths, the embedded `awaitApplied` gate validates an exact durable-or-pending physical locator before draining and rereads POSITION after drain, and a rejected/foreign/forged receipt has no apply side effect; the closed managed enqueue union preserves queued/definitely-not-queued/uncertain states, applied frame is emitted only after the source barrier and retains the queued digest, query and applied-receipt barrier checks reject same physical offset/ledger-entry-batch tokens with conflicting canonical source metadata, same-command-id command-hash drift, or a mismatched POSITION audit, pending is source-barrier based, full/compact/evidence-expired branches are bounded, and message projections require caller-supplied safe policy inputs; real Broker response adapters and production routing remain pending |
| Destination Lane gate/readiness projection | Implemented (core plus closed same-key terminal branch and typed ActiveLaneStateV1/quota/certificate/barrier codecs) | `LaneRecord`, `LaneRecordEnvelopeV1`, `ActiveLaneStateV1`, `LaneCircuitStateV1`, `LaneRuntimeBlockReasonV1`, `LaneQuotaUsageEntryV1`, `LaneQuotaUsageMapV1`, `ReadyCertificateV1`, `ActivationBarrierV1`, `EvidenceCursorV1`, `LaneRetirementProgressV1`, `LaneTerminalGuardV1`, `LaneRecordTest`, `LaneRecordEnvelopeV1Test`, `ActiveLaneStateV1Test`, `LaneQuotaUsageMapV1Test`, `ReadyCertificateV1Test`, `ActivationBarrierV1Test`, `EvidenceCursorV1Test`, `LaneTerminalGuardV1Test`, `ApplyShardControlBody`, `ControlRef`, `DelayShard`, `DelayShardTest`; schedulable lanes maintain a versioned READY head and readiness/gate CAS transitions remove/recreate it atomically; source-ordered signed PAUSE/RESUME/BREAK/CLOSE applies exact Lane incarnation/control-version fencing, strict-lane acknowledgement checks and atomically revokes/restores reversible Claims; the same `meta_cf/LANE` key now persists a closed ACTIVE/TERMINAL branch, direct Lane reads fence the embedded Lane id, and close-cursor reads fence Lane id/incarnation/control version/source shard before exposing materialization work; the bounded local retirement path conservatively proves no current message/timeline/inflight work before atomically installing a tuple/profile/source/digest-checked terminal guard that survives reopen and rejects reuse; retirement progress and terminal-guard source checks reject equal order tokens with conflicting canonical metadata; runtime and control version increments fail closed at `Long.MAX_VALUE` (`LaneRecordTest.versionCountersFailClosedBeforeLongOverflow`), while the Registry-shaped ActiveLaneStateV1 codec now covers tuple identity/digest, gate/readiness block-reason rules, 17-dimensional lane charge, circuit/backoff counters, ready-key digest, canonical ReadyCertificateV1 wrapper and retirement progress, and the per-Lane quota entry/map codec enforces sorted identity keys and usage/map digests; `LaneRecordEnvelopeV1` now emits typed field-10 ACTIVE values as direct canonical `ActiveLaneStateV1`, rejects malformed typed values instead of downgrading them to legacy, and still reads the old adapter sub-message; `DelayShard` reads typed ACTIVE values during reopen/rebuild/scheduler projection, preserves immutable Profile/tuple/certificate/retirement fields and exact projected quota usage on same-key updates, and fails closed when the compatibility runtime cannot provide a required block reason, READY key/certificate or bounded numeric field (`DelayShardTest.typedActiveLaneStateIsReadAndUpdatedWithoutLegacyDowngrade`). Legacy persistence remains on the compatibility adapter because current Schedule inputs do not carry the immutable Profile/tuple/certificate inputs required for a lossless cutover; quota-map revision coupling, Oxia target registration and Recovery-Floor/retention guard remain release blockers |
| Lane Close materialization cursor | Implemented (local source-marker overlay, bounded cursor and discovery/materializer bridge) | `LaneCloseMaterializationCursor`, `LaneCloseMaterializer`, `DelayShard`, `ShardQuota`, `LaneCloseMaterializationCursorTest`, `LaneCloseMaterializerTest`, `DelayShardTest.closeTransfersUnadmittedQuotaAndResumesBoundedMaterializationCursor`; Close marker transfers unadmitted message/reservation quota once in the marker WriteBatch, persists canonical `timeline/SYSTEM` kind-2 cursor state, strictly discovers only cursor entries whose key/value/Lane identity still agree, freezes only generations with an empty admitted-obligation set as `DEAD_LETTER(LANE_CLOSED_BEFORE_ADMISSION)`, closes uncommitted reservations as `ABANDONED`, and resumes message then reservation scans after restart; the local materializer runs bounded turns over the discovered cursors without making a new semantic decision, and its per-result and cross-Lane aggregate counts use checked addition; closed-lane Cancel/Reschedule/Commit paths return the stable frozen outcomes before cursor completion; PUBLISHING/UNCERTAIN obligations are retained and full close-owned Claim tagging, admitted-outcome retirement, object-handle/quiescence GC, Recovery-Floor protection and owner/Oxia materializer orchestration remain release blockers |
| Destination Lane isolation and bounded weighted DRR | Implemented (scheduler core plus fenced READY recovery seam) | `LaneScheduler`, `PersistentLaneScheduler`, `ReadyIndexValue`, `TimelineEntry`, `LaneSchedulerTest`, `SchedulerProjectionsV1Test`; lane-local work is rebuilt from bounded `timeline_cf/READY` plus `meta_cf/LANE`/`id_cf/MESSAGE` identity checks, recomputes the exact timeline key and digest, verifies the timeline value, rejects a READY message whose self-routing key or embedded schedule Source Position belongs to another Shard, stale/orphan/multiple-head projections fail closed, and the recovered heads replace the active ring and queues before scheduling resumes; scheduler quantum/weight/cap multiplication is checked at configuration and lane registration, while runtime deficit accumulation saturates instead of wrapping; `LaneSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow`, `LaneSchedulerTest.saturatesRestoredDeficitBeforeServing` and `LaneSchedulerTest.fencedRecoveryRejectsReadyMessageFromAnotherShard` cover the local fence; production Lane certificate/activation authority remains pending |
| Persistent scheduler fairness counters | Implemented (local closed projection plus owner-bound recovery first pass and rotating READY cursor) | `PersistentLaneScheduler`, `LaneScheduler`, `SchedulerProjectionsV1`, `OwnerIdentityV1`, `LaneSchedulerTest`, `SchedulerProjectionsV1Test`; all five `meta_cf/SCHEDULER` values are written in one batch, persisted successor order is restored without re-adding discarded lanes, the persisted `SchedulerRoundV1.owner` is compared with the current owner and an owner/store change restarts `recovery_first_pass`, and the first recovery rotation serves at most one item per eligible Lane until every currently discovered Lane has received an opportunity; fenced READY rebuild performs a complete bounded scan from the READY namespace rather than consuming the rotating discovery cursor, while `discoverReady` promotes a bounded rotating slice into the active ring, validates exact Lane/Message/timeline identity, retains the physical lane-versioned READY key alongside each discovered work item, suppresses re-offering a polled head only while both work identity and READY key remain unchanged, rejects a projection that exceeds the visit byte cap without a first-entry exception, stops before decoding when the elapsed cap is already exhausted, and reads one extra inclusive cursor entry so a one-entry turn can wrap (`LaneSchedulerTest.fencedRecoveryUsesCompleteReadyPassDespitePersistedDiscoveryCursor`, `LaneSchedulerTest.rotatingReadyDiscoveryDoesNotReofferPolledHeadAndFindsSuccessorAfterWrap`, `LaneSchedulerTest.readyTransitionWithSameWorkUsesNewReadyKey`, `LaneSchedulerTest.readyDiscoveryRejectsFirstEntryThatExceedsByteBudget`, `LaneSchedulerTest.readyDiscoveryStopsBeforeFirstEntryWhenTimeBudgetIsElapsed`); Lane incarnation/version and message-generation checks remain enforced; full Oxia-fenced activation and typed ActiveLaneState cutover remain release blockers |
| Worker Trusted UTC interval guard | Implemented (local deterministic guard; authority/wiring pending) | `TrustedUtcClock`, `TrustedUtcInterval`, `TrustedUtcClockTest`; injected monotonic projection, maximum uncertainty/sample age, wall/monotonic divergence fencing, stabilization window, conservative interval widening and strict due/pre-expiry predicates are covered; approved synchronization/signature source, Broker-time certification and production Worker/Admission wiring remain release blockers |
| Worker-to-shard-to-lane bounded DRR | Implemented (core snapshot plus READY-aware outer filtering, recovery first-pass, large-head service and local placement scoring) | `WorkerScheduler`, `LaneScheduler`, `WorkerSchedulerTest`, `LaneSchedulerTest`, `WorkerLoadVector`, `WorkerPlacementPolicy`, `WorkerPlacementPolicyTest`; outer and inner caps retain at least the current registered `weight * quantum` so weights above four are not silently clipped to a 4:1 long-run share, visits only shards with at least one schedulable Lane head, starts a new process/restore/READY recovery pass that serves each currently eligible shard at most once before repeating one, widens a shard visit to its smallest schedulable head when that head exceeds the outer deficit cap (still bounded by the caller's global byte budget), checks shard weight/quantum/cap arithmetic before registration and saturates runtime deficit accumulation, and the local placement seam hard-filters full committed capacity/DB slots before scoring projected `committed + required` dominant utilization plus unequal observed load; `WorkerSchedulerTest.recoveryFirstPassServesEveryEligibleShardBeforeRepeatingOne`, `WorkerSchedulerTest.restoreStartsANewOuterFirstPass`, `WorkerSchedulerTest.outerDeficitCapDoesNotMakeLargeHeadUnserviceable`, `WorkerSchedulerTest.highWeightRetainsItsConfiguredOuterDeficitQuantum`, `LaneSchedulerTest.highWeightRetainsItsConfiguredDeficitQuantum`, `WorkerPlacementPolicyTest.projectedCommittedCapacityBreaksEqualTelemetryTie`, `WorkerSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` and `WorkerSchedulerTest.saturatesRestoredDeficitBeforeServing` cover the local fence; durable outer scheduler projections, broker assignors, Oxia desired-placement plans and authoritative placement weights remain pending |
| Closed Stable Code registry | Implemented | `StableCode`, `FailureStageV1`, `RetryabilityV1`, `StableErrorV1`, `ProtocolCodecTest`; all Registry stable codes including activated-protocol/capacity-fence codes, code-derived retryability, retry-at presence, mutually exclusive managed/native prepared refs, bounded diagnostic code and canonical round-trip/rejection checks |
| Non-persistence proof and Broker identity | Implemented (codec boundary) | `KafkaBrokerResourceIdentityV1`, `PulsarBrokerResourceIdentityV1`, `BrokerResourceIdentityV1`, `NonPersistenceProofKindV1`, `NonPersistenceProofV1`, `ProtocolCodecTest.brokerEvidenceAndQueuedAckIdentitiesRejectNonCanonicalUtf8AtConstruction`; closed Kafka/Pulsar identity branches, kind-specific attempt/resource/request/response presence, pre-ownership evidence prohibition, adapter-proof version and fields 1–7 digest, and strict direct-construction UTF-8 round-trip fencing for broker identities; authenticated adapter rejection classifiers and real non-persistence attestations remain pending |
| Managed/native submission outcome unions | Implemented (wire codec plus embedded/Kafka/Pulsar managed and native transport-SPI bridges) | `EnqueueOutcomeKindV1`, `DefinitelyNotQueuedV1`, `EnqueueUncertainV1`, `EnqueueOutcomeMessageV1`, `SubmissionOutcomeKindV1`, `NativeDefinitelyNotQueuedV1`, `NativeEnqueueUncertainV1`, `SubmissionOutcomeMessageV1`, `PreparedSubmissionV1`, `PreparedSubmissionAdapter`, `CommandQueuedReceiptV1.PreparedCommandRef`, `EmbeddedDelayService.enqueueOutcomeV1`, `PinnedKafkaCommandIngress.enqueueOutcomeV1`, `PinnedPulsarCommandIngress.enqueueOutcomeV1`, `PinnedPulsarNativeSubmissionAdapter.submit`, `WireIngressOutcomeSupport`, `ProtocolCodecTest`, `PreparedCommandV1Test`, `EmbeddedDelayServiceTest`, `AdapterIngressTest`, `NativeSubmissionAdapterTest`; closed branch tags, exact prepared/ref hash binding, retryability and physical-attempt checks, canonical managed NDL1/native prepared branches, exact managed/native branch dispatch without reselection, V1 managed submission strict `encodeFrameV1/decodeFrameV1` validation before transport ownership, strict V1 frame digest derivation for `PreparedCommandRefV1`, compatibility-body rejection in both submission and receipt projections, embedded queued/definite/uncertain projection, Kafka/Pulsar queued ACK and authenticated definitive-rejection proof projection, native capability signature/expiry and pinned-resource checks before transport ownership, native receipt/guard-proof/uncertain/local-definite projection, and conservative downgrade when evidence is absent; durable guard/credential protection, authenticated Producer ownership and production response evidence remain pending |
| Hard shard quota admission | Implemented (core subset) | `ShardQuota`, `ShardQuotaTest`, `OutcomeReserveUsage`, `LaneQuotaUsageProjection`, `PublishAdmissionBody.ChargeVector`, `CapacityDimensionV1`, `CapacityVectorV1`, `CapacityGrantV1`, `QuotaGrantRefV1`, `ShardCapacityEnvelopeV1`, `DelayShardConfig`, `DelayShard`, `KeyCodecTest`, `DelayShardTest`, `CapacityVectorV1Test`, `ShardCapacityEnvelopeV1Test`; shard-local class-2 aggregate, per-Lane class-3 map and outcome reserve records/bytes are durable and source-ordered, with `ADMISSION_CAPACITY_GATED` Claim rollback, admission charge and definitive/verified settlement release in one WriteBatch; activation rebuilds and compares the canonical local aggregate, reads old class-1 `ShardQuota`/class-2 scalar values only for migration, and removes the stale class-1 projection on the next mutation; single message/reservation add/remove/commit entries reject negative bytes before applying checked arithmetic; an activation-supplied immutable envelope additionally binds the exact outcome grant and persists the full 66-dimensional outcome usage under registered `meta/CONTROL_RESERVE` keys, with restart/rotation checks; class-3/4/5/6 reserve/release now has checked grant-bounded local persistence, class 3/6 are dimension-disjoint under the shared `NON_OUTCOME_CONTROL` grant, and class-6 restart/invalid-dimension tests are covered; source-writer charge integration, Route Broker authority, multi-shard placement/Oxia authority and GC accounting remain pending |
| Kafka/Pulsar ingress and target adapters | In progress (identity-pinned transport SPI) | release blocker until concrete pinned transports, authenticated non-persistence classifiers/proofs, target publish/evidence channels, production response evidence and real-broker tests exist |
| Recovery Set/Floor, catalog and restore replay | In progress (local catalog/Floor subset) | release blocker; Oxia catalog/session pin, immutable publication, source/evidence replay and activation CAS remain |
| Large payload, quota grants, control reserve and GC | In progress (reservation/commit, shard hard-quota, 66-dimensional vector/grant/envelope codec, canonical class-2 local aggregate plus per-Lane map, bound outcome-reserve usage, checked `meta/CONTROL_RESERVE` class-3/4/5/6 reserve arithmetic, disjoint system-writer projection, retire-intent, delete-confirmed and catalog-backed local compaction subsets) | release blocker; `CapacityVectorV1`/`CapacityGrantV1`/`QuotaGrantRefV1`/`ShardCapacityEnvelopeV1` enforce the closed dimension registry, zero-explicit ordered amounts, grant/envelope digests, logical charge projection, component-grant projection and checked arithmetic locally. The bound `DelayShard` path persists class-1 envelope identity and class-2 exact outcome usage under `meta/CONTROL_RESERVE`; `meta/QUOTA` class 2 now carries the canonical aggregate for locally accounted dimensions, class 3 carries the per-Lane map, and old class-1/class-2 scalar values are migration-only. It exposes synchronous grant-bounded reserve/release for `meta/CONTROL_RESERVE` classes 3–6, and enforces the class-3/class-6 dimension partition plus combined `NON_OUTCOME_CONTROL` grant bound; it scans those reserve classes during activation and rejects stale/unknown, over-capacity or cross-partition projections instead of ignoring them. `meta/QUOTA` quotaClass=4 retained/object usage still lacks a Registry value schema and remains pending. `ResourceRetireIntentBody`/`ResourceRetireIntentRecord` plus `ResourceDeleteConfirmedBody`/`ResourceDeleteConfirmedRecord` provide canonical source-ordered `gc_cf/TASK` intent/tombstone persistence with applied mutation sequence; direct GC reads fence resource kind/hash/version against the embedded retire intent, including nested delete-confirmation intents; `RecoveryCatalogAuthority`/`OxiaRecoveryCatalog` plus `ResourceGcGuard` enforce local ancestry/source/sequence coverage and fail closed when an active RecoveryPin protects a checkpoint resource or its pin state cannot be read, `DelayShard.compactResourceDeleteConfirmation` removes only a covered unpinned local tombstone, and local payload/checkpoint version/etag comparison is enforced, but Route Broker source-writer operation charging/authority, Object Store/Oxia publication, multi-shard grant placement/authority, real provider delete attestation/ownership, durable catalog/Floor barrier, Lane terminal guard and full guarded GC remain |
| Query, control operations, DLQ and observability | In progress (wire unions plus bounded local receipt/barrier/DLQ/SLO bridge) | `MessageQuerySnapshot`, `ReservationQuerySnapshot`, `DlqExportRecord`, `DlqExportResultBody`, source-ordered `DelayShard` DLQ export apply, `BoundedLocalQueryProjector`, `EmbeddedDelayService.queuedReceiptV1/appliedReceiptV1/queryCommand/queryMessage/registerControlOperation/advanceControlOperation/queryControlOperation`, `ControlOperationQueryResponseV1`/`CurrentControlOperationV1`/`ControlTargetStateViewV1`/`ControlTypedResultV1`, `ControlOperationAuthority`, `InMemoryControlOperationAuthority`, `OxiaControlOperationAuthority`, `SloObjectiveV1`/`SloSampleEventIdentityV1`/`SloSampleStartV1`/`SloSampleFinalV1`/`SloObservationOutboxV1`/`SloObservationOutboxStore`/`SloObservationOutboxExportRate`/`SloObservationCollector`/`SloObservationCollectorLimits` and closed SLO enum/time codecs, all V1 Command/Message query view codecs, `DelayShardTest`, `DlqExportRecordTest`, `DlqExportApplyTest`, `ControlOperationQueryResponseV1Test`, `ControlOperationAuthorityTest`, `SloObjectiveV1Test`, `SloObservationOutboxV1Test`, `SloObservationOutboxStoreTest`, `SloObservationOutboxExportRateTest`, `SloObservationCollectorTest`, `ShardStoreTest`, `EmbeddedDelayServiceTest`, `ProtocolCodecTest`; Dead Letter terminalization writes the deterministic `terminal_cf/DLQ_EXPORT` `NOT_CONFIGURED` record atomically; configured local outboxes can now apply signed `DLQ_EXPORT_RESULT_V1` transitions with checked attempt succession, PENDING next-attempt advancement, terminal monotonicity and mutation dedupe, preserving body `stable_code` in the applied System Mutation result, but configured `DlqExportRecord` now persists the canonical policy-derived retained charge (legacy v1 records decode as zero), and apply requires callback transfer byte-equality with that projection; mismatches remain `REJECTED(STALE_SYSTEM_MUTATION)` without advancing the outbox state; the Control Operation query response union now has canonical CURRENT/error/target-marker/revision/typed-result wire validation, while the local authority and embedded entry points add receipt-bound idempotent registration, strict revision CAS and fixed retention-bound queries; SLO objective digest, direction/unit/population/exclusion semantics, all 14 objective branch tags/common identity field-shape checks, Start threshold timeout, exact sample/start/final digests, start matching, exact due identity/path/start consistency, `meta_cf/SLO_OUTBOX` key/value-envelope persistence, key/value sample identity fencing, conservative AT_MOST/AT_LEAST Final merge, bounded outbox capacity and process-local bounded export-rate accounting are locally covered; local collector merge now also rejects a different Start and fails closed on sample/canonical-byte capacity overflow without mutating its prior projection. Real target/evidence adapter ownership, production receipt/barrier routing, authorization-safe binding/evidence/retention lookup, durable Oxia control-operation state/routing, Start reconstruction from Message/Admission authority, production collector merge/export and observability remain release blockers |
| Real-service, chaos, benchmark, soak and upgrade evidence | Not started | release blocker |

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

`SloObservationCollector` now provides the local deterministic collector merge
projection: it keys by canonical sample ID, rejects a different Start for that
ID, delegates repeated Final observations to the direction-aware conservative
merge, and optionally enforces a sample/canonical-byte projection envelope.
`SloObservationCollectorTest` covers bad-evidence monotonicity, Start drift,
deterministic snapshot order and capacity rejection. This is not yet the
durable/authorized production collector or metric publisher.

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
submission wrapper: a null stage, adapter throw, or `thenApply` callback
registration failure is projected as managed `ENQUEUE_UNCERTAIN` with the
original Prepared Command and physical attempt id. The wrapper must not leak an
exceptional Future or silently switch to the native branch after managed
transport ownership may have begun. `NativeSubmissionAdapterTest`
`preparedSubmissionWrapperRegistrationFailureRemainsManagedUncertain` covers
the callback-registration case. If the physical attempt id is invalid while
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
valid attempt identity. `EmbeddedDelayServiceTest.embeddedIngressProjectsAllManagedOutcomeBranches`
covers both queued and uncertain invalid-attempt projections; the embedded
service remains a conformance seam, not a real Broker adapter.

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

## Verification command

Use the checked-in Gradle Wrapper and an isolated cache on hosts where the
default Gradle native cache is not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
