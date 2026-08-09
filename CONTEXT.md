# Nereus Delay

Nereus Delay provides a common language for scheduling when messages may first become visible to consumers across destination messaging systems.

## Delivery Timing

**Delivery Time (`deliverAt`)**:
The earliest instant at which a destination consumer may become eligible to receive a delayed message. It is neither the time publishing starts nor a guarantee of exact-time visibility.
_Avoid_: Publish time, send time, execution time

**Action Time (`actionAt`)**:
The earliest instant at which Nereus Delay may start the destination action needed to satisfy a message's Delivery Time.
_Avoid_: Delivery time, visibility time

**Trusted UTC Interval**:
The scheduler's bounded estimate `[earliestUtcNow, latestUtcNow]` of current UTC, derived from monitored clock synchronization and monotonic elapsed time. V1 admits not-before actions only from the lower bound and expiration decisions from both bounds.
_Avoid_: Raw `currentTimeMillis`, client timestamp

**Target Early-Delivery Bound**:
The certified maximum by which a destination Broker's clock or delayed-delivery implementation could otherwise make a message visible early. Native delayed timestamps are shifted by this bound.
_Avoid_: Scheduler tick, lateness SLO

## Delivery Management

**Managed Delivery**:
A delayed delivery for which Nereus Delay retains management authority and provides query, cancellation, and rescheduling within its documented lifecycle boundaries.
_Avoid_: Native delivery, direct delivery

**Automatic Fast Selection Mode (`AUTO_FAST`)**:
An explicit caller permission for the SDK to choose, before any I/O, either a managed Prepared Command or a direct certified Pulsar `NativePreparedDelivery`. The chosen serializable branch is returned before submission and never changes on retry. Only the native branch lacks server query, cancel, or reschedule.
_Avoid_: One-shot request that can reselect after uncertainty, transparent optimization

**Native Capability Snapshot**:
A short-lived signed authorization lease embedded in a `NativePreparedDelivery`. It binds exact Destination/Capability semantics, Pulsar resource/partition, resource-guard rollout, Credential Binding generation/digest/fingerprint, SDK principal scope and Trusted-UTC expiry. Issuance is protected durably before exposure; equivalent credential rotation does not retroactively revoke the lease, while Producer ownership after emergency revocation remains an uncertain boundary.
_Avoid_: Hash without a schema, secret reference in prepared bytes, Shard Ready Certificate

**Submission Outcome**:
A sealed submission outcome whose concrete type states whether an already prepared managed branch was queued, or an already prepared native Producer operation was acknowledged, definitely not queued, or uncertain. “Receipt” is reserved for acknowledged concrete results.
_Avoid_: One receipt with optional ambiguous fields, treating uncertainty as a receipt

## Commands and Messages

**Command**:
An immutable request to schedule, cancel, or reschedule a delayed message. A Command being queued is distinct from Nereus Delay authoritatively applying it.
_Avoid_: Delayed message, applied operation

**Command Identity (`commandId`)**:
The stable identity of one logical Command across all of its physical enqueue attempts. Reusing it for different command semantics is a protocol conflict.
_Avoid_: Delay message identity, enqueue attempt ID

**Self-Routing Identity**:
A versioned opaque identifier that carries Route Incarnation, physical ingress partition, random logical identity, and an accidental-corruption checksum. It conveys no tenant authority.
_Avoid_: Authorization token, Source Position

**Delay Message Identity (`delayMessageId`)**:
The stable identity of one Delayed Message across its Schedule, Cancel, Reschedule, and publish lifecycle. A different initial Schedule cannot reuse it to replace an existing message.
_Avoid_: Command identity, Message Generation

**Retired Message Identity**:
A compact `id_cf` tombstone that replaces reclaimable full message history and remains until a source-ordered time fence closes that ID's maximum first-seen preparation deadline. It bridges GC to deterministic rejection of later identity reuse.
_Avoid_: Full terminal history, deferred Cancel tombstone

**Identity-Retired Query (`IDENTITY_RETIRED`)**:
A Message query outcome proving that a Delay Message Identity is still occupied by a compact tombstone although its full reservation/generation/terminal details have expired. It does not reconstruct or guess those details.
_Avoid_: Unknown identity, retained terminal history

**Prepared Command**:
An immutable, serializable Command whose identities, route, partition, canonical bytes, and Command Hash are fixed before any network I/O. Every retry of the same logical operation reuses the same Prepared Command.
_Avoid_: Producer request, enqueue attempt

**Command Hash**:
A digest of every field that defines a Command's business meaning, excluding transport-attempt metadata. It distinguishes an idempotent retry from conflicting reuse of a Command Identity.
_Avoid_: Payload hash, checksum of an enqueue attempt

**Canonical Command Body**:
The version-specific, byte-for-byte canonical encoding of a Command's semantics that is fixed by preparation, carried unchanged by every enqueue attempt, and covered by Command Hash.
_Avoid_: Arbitrary valid Protobuf encoding, Broker record envelope

**V1 Protocol Registry**:
The normative numeric and byte-level registry for the frozen spec revision: Shard Log and public receipt frames, enum/body/receipt fields, version-bound hash/signature preimages, stable result codes, RocksDB key tags/widths, cursor ordering, checkpoint JSON, and conformance vectors. Prose does not authorize unregistered extensions.
_Avoid_: Implementation-local enum, an informal field list, generic Protobuf deterministic mode

**Shard Log Frame (`NDL1`)**:
The bounded, checksummed Broker value framing that identifies a Client Command or signed System Mutation before canonical envelope parsing. Its version and record kind are part of the activated protocol tuple and hash/signature domains.
_Avoid_: Broker header, opaque arbitrary Protobuf bytes

**Command Retry Window**:
The fixed interval during which physical enqueue attempts of one Prepared Command may be durably appended to its Ingress Route. It does not limit how long an already-queued record may wait before application.
_Avoid_: Command result retention, message lifetime

**Ingress Time Fence**:
A canonical system-signed source record whose deterministic Proof ID binds the exact shard, `closeThrough`, signing-key version and immutable Trusted-UTC interval proof. It monotonically advances `closedIngressDeadlineThrough`, proving that later records cannot reopen deadlines at or below that boundary even if Broker timestamps regress.
_Avoid_: Unsigned tenant record, decreasing watermark, wall-clock-only TTL

**Ingress Route**:
A configured, partitioned command stream through which Prepared Commands enter Nereus Delay. Every command for a Delayed Message remains on the route and physical partition selected by its initial Schedule.
_Avoid_: Destination route, dynamically selected retry route

**Route Incarnation**:
The immutable identity of one physical Ingress Route creation and protocol/configuration epoch. Recreating a Broker topic or changing partition topology requires a new incarnation.
_Avoid_: Route display name, Destination Profile version

**Security Domain**:
The set of callers intentionally sharing one Broker ACL and one derived tenant authority on an Ingress Route. V1 does not recover an individual producer principal from consumed Command records.
_Avoid_: Self-declared tenant field, destination credential

**Authenticated Tenant Context**:
The tenant and roles derived from a trusted API authenticator or the registered Security Domain of an Ingress Route, never from caller-controlled Command payload.
_Avoid_: Receipt field, message property

**Source Position**:
The ingress Broker position that orders physical Client Command and System Mutation records within one Ingress Route partition. Positions from different routes, topics, or partitions are not comparable.
_Avoid_: Client sequence, global sequence, application timestamp

**Source Entry**:
One Broker storage entry that may contain one Kafka record or a Pulsar batch of Shard Log records. A Pulsar entry is acknowledged only after every decoded batch member through its end is durably applied or quarantined.
_Avoid_: Prepared Command, RocksDB WriteBatch

**Queued Command (`QUEUED`)**:
A Command durably accepted by the configured ingress Broker but not yet authoritatively applied by its Delay Shard. It carries no promise that validation or the requested operation will succeed.
_Avoid_: Accepted message, scheduled message

**Definitely Not Queued (`DEFINITELY_NOT_QUEUED`)**:
An enqueue outcome for which the client Adapter can prove that the ingress Broker did not persist the Prepared Command.
_Avoid_: Timeout, unknown enqueue result

**Uncertain Enqueue (`ENQUEUE_UNCERTAIN`)**:
An enqueue outcome for which a Prepared Command reached the Producer or network but the client cannot determine whether the ingress Broker persisted it. Recovery reuses the original Prepared Command rather than creating a new identity.
_Avoid_: Failed enqueue, definitely not queued

**Applied Command (`APPLIED`)**:
A Command that its Delay Shard has authoritatively processed and whose operation outcome has been durably recorded.
_Avoid_: Queued command, Broker acknowledgement

**Rejected Command (`REJECTED`)**:
A Command that its Delay Shard has deterministically refused for a stable business, policy, or protocol reason and whose rejection result has been durably recorded. Transient infrastructure failures are not rejections.
_Avoid_: Transient failure, unknown result

**Quarantined Source Record**:
A position-level durable result for a malformed or unsupported ingress record whose Command Identity cannot be trusted or decoded. It advances source progress without creating a Delayed Message.
_Avoid_: Rejected Command, silently skipped record

**Command Applied Receipt**:
The immutable result returned only after a Delay Shard has durably applied or rejected a Command. It is distinct from a Command Queued Receipt.
_Avoid_: Broker acknowledgement, message delivery receipt

**Command Queued Receipt**:
An immutable receipt proving that one Prepared Command was durably accepted at an authenticated ingress Source Position. Its query boundary is anchored to Broker persistence time plus the immutable Route window; it never proves Command application.
_Avoid_: Applied receipt, delivery receipt

**Full Command Result Retention**:
The policy-derived boundary through which the complete applied or rejected Command result may be returned, measured from the first result Source Position's Broker persistence time.
_Avoid_: Worker apply TTL, SDK-supplied deadline

**Control Operation Query Policy**:
The immutable policy snapshot bound to a Prepared Control Operation. Its version must match `controlQueryPolicyVersion`, and its window is added to `registeredAt.latest` with checked arithmetic to derive the receipt's fixed `queryUntil` boundary before registration.
_Avoid_: caller-selected absolute Control receipt deadline, response-time extension

**Deterministic Command Application**:
The requirement that replaying a source record from any permitted checkpoint produces the same authoritative result from canonical Command bytes, Broker source metadata, preceding durable shard state, immutable referenced versions, and preceding source-ordered control markers. Live destination state, Worker clock, cache timing, and physical disk size are not business-result inputs.
_Avoid_: Best-effort replay, current-environment validation

**Shard Control Marker**:
A service-signed `APPLY_SHARD_CONTROL_V1` System Mutation in one shard's Shard Log partition that activates a quota, profile-acceptance, admission, or Lane-control version at an exact Source Position. Its durable RocksDB application, not Oxia request acceptance or watch delivery, is the control boundary.
_Avoid_: Tenant Client Command, watch event, mutable flag, unsigned marker

**System Mutation**:
A service-only, canonical and signed Shard Log record with a deterministic 32-byte identity, version-bound hash, bounded retry deadline, exact shard subject and stable dedupe result. It serializes control, Replay/Resolve, Publish Admission/outcome, permanent Claim result, DLQ export result, expiration, evidence, and resource-retirement effects with Client Commands without entering the Command query namespace.
_Avoid_: Unsigned privileged Command, direct callback write, UUIDv7 Command identity

**Query Barrier**:
A Source Position through which an `ACTIVE_FOR_COMMANDS` shard must have durably applied Commands before answering a read-after-command query conclusively. Query availability does not require every Destination Lane to be `READY`.
_Avoid_: Consumer-group committed offset, destination publish position

**Delayed Message**:
A managed logical message whose Schedule Command has been successfully applied. A queued or rejected Schedule Command does not by itself create a Delayed Message.
_Avoid_: Schedule command, queued request

## Payloads

**Opaque Payload**:
The exact application-serialized bytes scheduled by V1. Nereus Delay does not deserialize a domain object or consult a schema registry at delivery time.
_Avoid_: Java object, Broker wire batch

**Payload Reservation**:
A durable, time-bounded reservation of identity and capacity for an out-of-line payload before it becomes a Delayed Message. It owns the only valid upload location and can be committed or abandoned.
_Avoid_: Scheduled message, arbitrary object upload

**Reservation Expiration Fence**:
The source-ordered effect of a signed Ingress Time Fence whose `closeThrough` reaches a still-open Payload Reservation's deadline. It immediately makes the reservation effectively `RESERVATION_EXPIRED`; a bounded expiry cursor only materializes that already-decided state and cannot reopen or reorder it.
_Avoid_: Wall-clock timer write, best-effort expiry scan

**Payload Object**:
An immutable, service-scoped object containing the bytes of an out-of-line message payload and identified by its exact length and checksum.
_Avoid_: Mutable blob, caller-selected object key

**Payload Commit Proof**:
A canonical, deterministic service-signed attestation that an exact reservation-scoped immutable Payload Object was verified before a Commit Command was prepared. It is non-secret, replayable, and locally verifiable during Command application.
_Avoid_: Upload handle, caller assertion, live Object Store HEAD during apply

**Payload Commit**:
The authoritative Command that verifies a Payload Commit Proof and reservation state without Object Store I/O, then converts the reservation into a scheduled Delayed Message.
_Avoid_: Upload completion, Command queuing

## Recovery

**Recovery Checkpoint**:
An immutable, complete physical snapshot of one Delay Shard database whose verified manifest has been authoritatively published. Uploaded files or manifests that are not published in the shard's checkpoint catalog are not recovery state.
_Avoid_: Local snapshot, uncommitted upload

**Recovery Set**:
The bounded, ordered set of published Recovery Checkpoints from which shard recovery is allowed to choose.
_Avoid_: Every object under the checkpoint prefix, orphan upload

**Recovery Floor**:
The oldest Recovery Checkpoint still permitted by the Recovery Set. State or external objects required by any checkpoint at or above this floor remain protected.
_Avoid_: Latest checkpoint, Command Topic retention time

**Recovery Pin**:
An Oxia session-bound ephemeral record that protects one exact recovery candidate and observed Recovery Floor while an Owner verifies, downloads, installs, and activates it. Presence, not a client-clock timeout, is authoritative; activation deletes the exact pin atomically with the Owner Lease transition to `ACTIVE_FOR_COMMANDS`.
_Avoid_: Download lock, local timer, permanent checkpoint catalog entry

**Checkpoint Upload Intent**:
A persistent Oxia state machine for one preassigned checkpoint ID and unique object prefix. `PENDING_UPLOAD -> PUBLISHED` and `PENDING_UPLOAD -> REAPING` are competing CAS transitions; REAPING waits for old-Owner/provider request quiescence and a final prefix sweep before deletion.
_Avoid_: Uploaded file inventory, cataloged Recovery Checkpoint, object-list discovery

**Checkpoint Safety Barrier**:
Proof that the Recovery Floor includes a state mutation and that no permitted older recovery image can require the mutation's retired resources.
_Avoid_: Checkpoint upload completion, terminal timestamp

## Destination Isolation

**Destination Profile**:
A pre-registered, immutable semantic definition of an allowed destination system, endpoint/resource scope, topic policy, partition policy, Ordering Domain policy, and credential authorization-scope policy. A Schedule cannot define an arbitrary destination connection. The current service-owned secret reference is a separate Credential Binding and is not part of the Profile hash.
_Avoid_: Per-message connection config, plaintext credentials

**Credential Binding**:
A private control-plane family containing immutable secret-reference generations, a checked current Head, and monotonic protection high-watermarks for exposed capabilities. Rotation is equivalent only when a platform verifier attests the exact immutable authorization scope; it invalidates affected READY certificates/channels without changing Profile semantic identity, Destination Lane ID, or existing Destination Bindings. Object Store failure remains operation-scoped, and a protected Native Capability Snapshot may outlive an equivalent rotation until its signed expiry.
_Avoid_: Credential field inside immutable Profile, operator equivalence boolean, secret generation in Schedule

**Credential Use Lease**:
A bounded local-call authorization issued only after an immutable secret reference is resolved and one Oxia transaction compares the current Credential Binding Head while extending that exact generation's protection horizon. A Destination Channel or Object Store Adapter validates its lease, holder scope, expiry, and loaded fingerprint locally before library/provider ownership; ordinary data calls do not perform per-message or per-provider Oxia reads. Equivalent rotation prevents renewal but does not retroactively revoke a protected unexpired lease, so this is not emergency revocation or remote Broker fencing.
_Avoid_: Per-call Oxia lookup, mutable credential cache entry, remote-fencing token

**Control Operation**:
An idempotent, authenticated, audited administrative mutation prepared with stable identity/canonical hash/scope/targets/CAS before registration. Registration outcome, per-target semantic marker state, and overall execution state are separate closed types.
_Avoid_: Repeated imperative RPC, treating marker apply as whole-operation completion

**Destination Binding**:
The immutable semantic destination, Broker Resource Incarnation, and physical partition resolved from a specific Destination Profile version when a Schedule Command is applied. Later Profile changes do not silently reroute the Delayed Message.
_Avoid_: Live mutable profile, credential material

**Broker Resource Incarnation**:
The verifiable lifetime identity of one Broker cluster/topic resource, distinct from its reusable name. Kafka uses cluster ID plus native topic UUID; Pulsar uses an administrator-protected Nereus token plus physical-topic creation identity. V1 binds it at the actual Broker request boundary, not only during activation.
_Avoid_: Topic name, Profile display name

**Pinned Kafka Topic-ID Channel (`PINNED_TOPIC_ID_V1`)**:
A Kafka Fetch/Produce channel that puts the immutable Route/Profile native topic UUID into every v13-or-newer wire request, allows metadata to update only the pinned UUID's leader, and forbids name-based protocol fallback or substitution of a same-name replacement UUID.
_Avoid_: Stock name-routed Producer, activation-only topic ID check

**Pulsar Resource Guard (`PULSAR_RESOURCE_GUARD_V1`)**:
A cluster-certified BrokerInterceptor installed on every eligible Pulsar Broker that compares each Nereus Producer's expected resource token with the actual persistent Topic token before `handleSend` and persistence. A typed guard rejection proves non-publication; a lost response remains uncertain.
_Avoid_: `producerCreated` callback, pre-send admin HEAD, client-only topic-name check

**Source Connection Generation**:
One initial or reconnected Pulsar Command consumer connection whose records remain gated until the Ingress Adapter verifies the exact physical topic incarnation. Records and callbacks from an uncertified or superseded generation cannot be applied or acknowledged.
_Avoid_: Consumer object lifetime without reconnect fencing, Source Position

**Ordering Domain**:
A stable `(tenant, Destination Profile version, orderingKey)` group of destination messages whose relative publish order must be preserved. Nereus Delay makes no global publish-order promise across different Ordering Domains or a Profile/Route migration boundary.
_Avoid_: Delay Shard, global order

**Delivery-Time FIFO**:
The V1 ordered-delivery rule that compares Message Generations by `(deliverAt, effective Schedule Source Position, delayMessageId)` and proves destination-Broker durable append or handoff order inside one Ordering Domain. Extending it to consumer receive requires an explicit downstream ordering certificate; processing-completion order is excluded. Reschedule gives a generation a new effective Schedule position.
_Avoid_: Original client call order, global FIFO, consumer processing order

**Physical Destination Partition**:
The immutable concrete Kafka or Pulsar topic partition selected when Schedule is applied. Publication does not recompute it from later Broker metadata.
_Avoid_: Ingress partition, mutable partitioner result

**Destination Lane**:
A stable, bounded group of messages within one Delay Shard that share destination, tenancy, and Ordering Domain characteristics for publish fairness, capacity, retry, and fault isolation. It is not an ownership, checkpoint, or recovery unit.
_Avoid_: Delay Shard, per-message queue

**Closed Destination Lane**:
An irreversible Lane lifecycle state created by a source-ordered close marker. It forbids every new Publish Admission, freezes then-unadmitted work, and lets already-admitted attempts finish only as success, definitive closed-after-admission non-publication, or retained uncertainty.
_Avoid_: Paused Lane, retryable circuit-open Lane

**Ordering-Broken Lane**:
A distinct irreversible admission-closed state recording that the old strict Ordering Domain was explicitly abandoned. It does not by itself freeze or release pending messages; an exact Close or Resolve action is still required.
_Avoid_: Closed Destination Lane, healthy ordered Lane

**Lane Incarnation**:
A replay-deterministic identity for the one lifecycle instance created for a Destination Lane ID, derived from that Lane ID and its creating Source Position. A broken/closed/retired tuple is terminally guarded and cannot reopen merely by choosing another incarnation; continued traffic must produce a different Lane ID through a new Profile, Ordering Domain, or resource incarnation.
_Avoid_: Destination Lane ID, Owner Epoch, reopen token

**Strong Capability Channel**:
A bounded Kafka transactional or Pulsar producer-sequence evidence domain scoped to one Destination Lane and exact target identity. An unresolved channel cannot be shared in a way that blocks unrelated Lanes.
_Avoid_: Worker-global producer sequence, cross-Lane transaction slot

**Pulsar Attempt Journal**:
The persistent non-compacted evidence log that maps a Lane-scoped Pulsar producer sequence to an exact Publish Attempt, Message Generation, and Prepared Publish before target send, and records definitive non-publication retirement before a later Admission may advance the sequence.
_Avoid_: Broker last sequence without mapping, Command Topic

**Adapter Channel**:
The Lane-scoped, bounded local submission and buffering domain through which a Destination Adapter invokes its client library. Sharing is allowed only when per-Lane capacity and non-blocking isolation are proven.
_Avoid_: Unbounded Producer queue, Worker-global send buffer

**Schedule Admission Control**:
The authoritative capacity decision that applies persistent Lane, tenant, and shard limits to a new Schedule Command. Exceeding a hard limit rejects that Schedule without stopping other Command application.
_Avoid_: Source pause, publish backpressure

**Shard Quota Grant**:
A versioned slice of a tenant's hard capacity statically assigned to one Delay Shard and consumed atomically with message state. The sum of grants, not independent local guesses, defines a V1 tenant-wide hard limit.
_Avoid_: Runtime metric, dynamically borrowed capacity

**Logical Resource Charge**:
The checked, persisted `QUOTA_ACCOUNTING_V1` resource vector derived from exact payload length and canonical record/Adapter metadata encodings. It controls deterministic quota and fairness decisions; physical RocksDB or filesystem size is a separate safety signal.
_Avoid_: SST compressed size, current disk usage, recomputed charge under a new version

**Control Capacity Reserve**:
Disjoint disk/write/Broker-writer capacity withheld from new Schedule and Publish Admission so already charged outcomes, fencing/close, rejection/audit, terminalization, capacity-releasing GC, and recovery metadata can still make progress. Its per-shard Outcome, non-Outcome, recovery, and emergency pools cannot be double-counted or consumed by compaction temporary files.
_Avoid_: General pending-message capacity

**Shard Safety Backpressure**:
A suspension of Command application at an exact Source Position because the relevant shard/process/filesystem failure domain can no longer durably or recoverably commit authoritative state. A shared guard closes every shard sharing it unless a truly independent hard per-shard limit exists. Destination failure or Lane publish saturation alone is not Shard Safety Backpressure.
_Avoid_: Lane quota, destination outage

**Shard Capacity Envelope**:
An immutable version/digest bound to a Shard Grant and placement that reserves the shard's full logical maximum, physical amplification, DB/file/memory/Adapter minima and Control Capacity partition from assignment acceptance through physical release. Current low usage does not make committed capacity free.
_Avoid_: Telemetry score, observed usage, best-effort placement weight

**Delivery Capability Profile**:
A named, versioned contract describing which destination-side fencing, outcome resolution, and duplicate-suppression properties an Adapter can prove under explicit Broker, topic, producer, and consumer prerequisites. Capability loss never silently downgrades a bound message.
_Avoid_: Adapter type, best-effort feature flag, exactly-once claim

**Publish Outcome**:
An Adapter result with two orthogonal closed dimensions for one exact Publish Attempt: side effect (`PUBLISHED`, `NOT_PUBLISHED`, or `UNKNOWN`) and disposition (`NONE`, message-retriable, message-permanent, Lane-unavailable, Owner-fenced, or Adapter-bug). The side-effect value is backed by the evidence required by its Delivery Capability Profile.
_Avoid_: One enum that loses failure scope, Producer callback alone, local state guess

**Dead Letter Record**:
The internal terminal record whose immutable decision core explains why one Message Generation exhausted or cannot continue its delivery policy. An auxiliary open-attempt/evidence/charge summary may only shrink or mark duplicate risk; it cannot rewrite that decision. The record is authoritative even when an optional external DLQ export has not succeeded.
_Avoid_: External DLQ message, retry queue

**DLQ Export**:
An optional, independently tracked outbox publication of a Dead Letter Record to an operator-facing destination. Its failure or uncertainty does not undo the internal terminal transition.
_Avoid_: Dead Letter Record, business delivery

**Dead Letter Replay**:
An explicit authenticated Control Operation whose exact target is emitted as signed `REPLAY_DEAD_LETTER_V1` System Mutation and uses retained payload/binding to create a new Message Generation under fresh timing and retry bounds.
_Avoid_: Tenant Client Command, retrying the old generation, deleting the audit record

## Ownership and Publication

**Delay Shard**:
The smallest unit of command ordering, ownership, durable recovery, and scheduling. In V1, one Delay Shard corresponds to exactly one partition of an Ingress Route.
_Avoid_: Worker, Worker database, arbitrary key range

**Store Incarnation**:
One immutable local installation of a Delay Shard RocksDB created by fresh initialization or a specific restore. An atomic active pointer selects it only after full identity and integrity verification.
_Avoid_: Owner Epoch, DB identity

**Owner Epoch (`ownerEpoch`)**:
A monotonically increasing generation that authorizes a Delay Shard owner to claim work, admit publish attempts, and record outcomes inside Nereus Delay. It does not fence requests already accepted by a destination Broker.
_Avoid_: Broker epoch, delivery identity, remote fencing token

**Source Assignment**:
The ingress Broker coordinator's current authorization for one Worker to consume an Ingress Route partition. It is necessary but insufficient for Delay Shard publication authority.
_Avoid_: Owner lease, committed source position

**Owner Lease**:
The Oxia-session-bound, single-holder record that combines a Delay Shard, Worker run, and Owner Epoch. A Worker must hold both Source Assignment and a locally valid Owner Lease guard to mutate or publish shard state.
_Avoid_: Source Assignment, remote Broker fencing

**Command-active Shard (`ACTIVE_FOR_COMMANDS`)**:
A shard whose current Owner has restored and caught up through its typed Activation Barrier, so Command application and authoritative queries may run. It does not imply that every Destination Lane may publish.
_Avoid_: Lane Ready, destination healthy

**Ready Destination Lane (`READY`)**:
A Lane whose current Owner has completed that Lane's capability-channel fencing, evidence replay, and local activation checks. This is runtime readiness only; scanning, Claim, and Publish Admission additionally require the source-ordered Lane Admission Gate to be `OPEN`.
_Avoid_: Command-active Shard, target guaranteed healthy, administratively open Lane

**Ready Certificate**:
A persisted, expiring proof binding READY to the exact Owner, Store, Lane incarnation, Adapter channel generation, evidence barrier/cursors, Broker-resource attestation generation, and a Head-compared/protected Credential Use Lease. Claim, Admission preparation, and first Producer call require a live certificate/lease and matching locally loaded fingerprint. An Admission record retains its embedded copy as historical decision evidence, so later runtime generations cannot rewrite an on-time replay result. Invalidation removes the READY key before closing the old channel.
_Avoid_: Unbounded boolean, one-time activation probe, source-ordered Lane gate

**Lane Admission Gate**:
The source-ordered, replayable management axis `OPEN`, `ADMIN_PAUSED`, `ORDERING_BROKEN`, `CLOSED`, or `RETIRED`. It is independent of runtime capability readiness; Resume can only change `ADMIN_PAUSED` back to `OPEN`.
_Avoid_: Capability circuit, runtime Ready state

**Activation Barrier**:
A typed source cursor captured after Owner Lease acquisition that bounds the prefix a restoring shard must catch up before it may become `ACTIVE_FOR_COMMANDS`. Kafka uses an exclusive read-committed LSO offset; Pulsar uses an inclusive batch-aware last MessageId; an empty source has an explicit empty variant. Destination publication additionally requires that Lane to become `READY`.
_Avoid_: Untyped Source Position, latest position forever, committed consumer offset

**Evidence Cursor**:
A closed Kafka-receipt or Pulsar-Attempt-Journal contiguous replay boundary keyed by evidence kind, Lane and Lane incarnation, resource incarnation, physical partition, and evidence generation. Dominance is defined only for an identical full key; old and new generations may coexist while recovery protects both.
_Avoid_: Untyped offset, cross-generation maximum, newest-only evidence guess

**Message Generation (`generation`)**:
A version of a delayed message created by scheduling or rescheduling it. `delayMessageId + generation` identifies one logical delivery for idempotency.
_Avoid_: Owner epoch, publish attempt

**Publish Attempt**:
One attempt to publish a Message Generation to its destination Broker. Multiple Publish Attempts may represent the same logical delivery.
_Avoid_: Message generation, logical delivery

**Prepared Publish**:
The immutable, fully materialized destination record and exact target/capability/channel/payload/metadata descriptor constructed before Publish Admission. The Admission record carries that full descriptor and its canonical hash, so crash recovery never reconstructs it from current configuration.
_Avoid_: Delayed Message record, Producer future

**Definitive Non-publication**:
Adapter evidence that one exact Publish Attempt did not and cannot become durable at the destination. Only this evidence permits a retry without the uncertainty caveat.
_Avoid_: Timeout, lost acknowledgement, classified exception alone

**Uncertain Publish (`UNCERTAIN`)**:
A Publish Attempt for which Nereus Delay cannot determine whether the destination Broker persisted the message. It is neither a confirmed failure nor a confirmed success. Only an unordered BEST_EFFORT Lane may start a pinned, bounded possible-duplicate retry while it remains unresolved; ordered delivery holds the head pending evidence or an explicit acknowledged domain break/close.
_Avoid_: Failed publish, canceled publish

**Generation Runtime Index**:
The single `id_cf/MESSAGE` record that separates a Message Generation's public aggregate state from its zero-or-one current send work and its bounded canonical set of still-open admitted Publish Attempt obligations. Any unresolved attempt keeps the aggregate `UNCERTAIN` even when current work is a timeline retry, Claim, or newer PUBLISHING attempt.
_Avoid_: One physical locator, current attempt only

**Attempt Obligation**:
An admitted PUBLISHING or UNCERTAIN Publish Attempt whose exact outcome, evidence, physical resource charge, or retirement remains open. Its canonical reference carries the full inflight key (including the admitting Owner Epoch), key hash, generation, ledger state, and digest, so recovery can locate it without an Owner-Epoch range scan. Several obligations may coexist for a baseline possible-duplicate retry and may outlive the Generation's terminal decision; each is closed independently.
_Avoid_: Pending Message count, current work

**Timeline Work**:
The one reversible current send candidate for a Generation, typed as initial schedule, definitive retry, or unordered uncertain retry. An uncertain retry is bound either to its pinned automatic policy or to an exact source-ordered, acknowledged Control override. Timeline Work is distinct from public aggregate state and from historical Attempt Obligations.
_Avoid_: Publish Attempt, aggregate `UNCERTAIN`

**Claim**:
A durable, bounded, and still-reversible reservation of a scheduled record and publish capacity before any destination side effect is authorized.
_Avoid_: Publish, delivery, point of no return

**Publish Admission**:
The durable authorization for one exact Publish Attempt after which Nereus Delay may invoke the destination Producer. It is the V1 cancellation and rescheduling point of no return.
_Avoid_: Claim, Producer callback

**Message Control Version (`stateVersion`)**:
The client-visible per-message version advanced by successful Schedule, Cancel, Reschedule, and Dead Letter Replay operations. Internal publish and retry revisions, and Lane management versions, use separate tokens.
_Avoid_: Lane Control Version, runtime revision, Message Generation

**Lane Control Version (`laneControlVersion`)**:
The source-ordered CAS version for one Lane Incarnation's management gate. Successful Pause, Resume, Break, and Close advance it; runtime readiness, circuit, Ready-index churn, and guarded physical retirement do not.
_Avoid_: Message stateVersion, runtime laneVersion, Lane Incarnation

**Lane Runtime Version (`laneVersion`)**:
The internal revision used to invalidate Ready-index keys, Claims, and runtime Lane snapshots. It may advance without any operator-visible Lane control change.
_Avoid_: Lane Control Version, Message stateVersion

**Expiration Time (`expireAt`)**:
The exclusive latest Broker-persistence boundary at which a new Publish Admission may qualify for a Delayed Message. Worker apply/replay may occur later without changing an on-time record. It does not revoke an already-admitted request or guarantee that visibility completes before that time.
_Avoid_: Delivery Time, payload retention time

**Durable SLO Evidence**:
A versioned Start/Final sample whose identity and objective are fixed before the measured operation can be lost, or are reconstructible from an existing authority, and whose replay merge can only preserve or worsen the result. It is observational and never authorizes or rewrites Command or Publish state.
_Avoid_: Best-effort metrics scrape, replay that drops slow samples
