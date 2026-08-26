# Use immutable control versions and narrow operator actions

Nereus Delay stores Route Incarnations, Destination Profiles, Retry Policies, quota policies, and protocol capabilities as immutable semantic versions in Oxia. Lifecycle changes and runtime blocks are separate versioned records.Nereus Delay allows versions to become `DEPRECATED` for new use but does not physically delete them; long-lived messages and permitted checkpoints must always be able to reconstruct their pinned meaning.

## Version behavior

- Oxia publication makes a Profile version immutable but does not authorize it at an arbitrary Shard Log position. Every authorized Route partition persists `profileAcceptance(profileId, version) = ABSENT | ACTIVE_FOR_FIRST_BINDING | CLOSED_FOR_FIRST_BINDING`. A service-signed `APPLY_SHARD_CONTROL(PROFILE_BINDING_ACTIVATE)` marker binds the exact Profile semantic hash and Control Operation target; only a first-seen Schedule/Prepare ordered after it may select that Profile. A pre-marker first use is `PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION`.
- Deprecation first disables SDK selection and then source-orders `PROFILE_NEW_BINDING_CLOSE` on the frozen target set. A first binding after the marker is `PROFILE_DEPRECATED_FOR_NEW_USE`. Exact duplicate Commands first seen earlier keep their first result; Commit of an already-created reservation, commands on an existing Message Identity, and authorized replay preserving the existing binding remain valid. Emergency inability to use an existing binding is a runtime `BLOCKED` overlay, not retroactive deprecation.
- A new Route cannot open tenant produce until all initially allowed Profile acceptance markers are applied. A deprecated version can never be activated on a later Route; continued new use requires a new immutable version.
- Endpoint, cluster, topic scope, routing/ordering, capability, object-store identity, or credential authorization-scope changes create a new version. The current secret reference/generation is a separate private Credential Binding. Rotation is allowed only by expected-generation CAS after a platform verifier proves exact equivalence to the immutable scope; it invalidates affected READY certificates/channels, whose identities pin binding generation/digest and resolved immutable credential-version fingerprint, but does not alter the Profile hash, Lane ID, or applied bindings.
- Route lifecycle, Profile publication/acceptance/deprecation, grant publication, and policy default changes are Oxia CAS operations with stable Control Operation IDs. Lost responses are resolved by exact reread; a retry never creates a second semantic version accidentally.
- Workers activate only with a complete compatible config snapshot and persist all pinned IDs/versions needed for recovery. Config watch notifications are hints; every mutating transition validates expected versions.
- Profile acceptance/deprecation, grant activation, `StopNewSchedules`, and any Lane pause/resume requiring an exact Command boundary are represented by signed `APPLY_SHARD_CONTROL` System Mutations in each affected shard's Shard Log partition. The marker's RocksDB apply, not its Oxia request or watch delivery, is the boundary. Route-wide completion waits for every target shard.
- Lane management has its own source-ordered `laneControlVersion`, initialized to 1 and checked-incremented by successful Pause, Resume, Break, and Close. Runtime readiness, circuit, ready-index changes, and recovery-safe `CLOSED -> RETIRED` use separate runtime/storage revisions; message Schedule/Cancel/Reschedule use `stateVersion`. None substitutes for another in a CAS, and retirement retains the final close control version.

## Narrow administrative controls

Nereus Delay provides separate actions rather than one ambiguous ingress pause:

- `StopNewSchedules` makes Schedule/reservation/replay admission reject while Cancel, Reschedule, payload abandon/commit policy, query, terminalization, and source progress continue.
- `PauseDestinationLane` changes only the source-ordered Lane `admissionGate` from `OPEN` to `ADMIN_PAUSED`, atomically removes its READY key, advances runtime `laneVersion`, revokes reversible Claims, and releases their executor permits. `ResumeDestinationLane` changes only exact-`laneControlVersion` `ADMIN_PAUSED -> OPEN`; it cannot clear runtime capability `BLOCKED` or reopen `ORDERING_BROKEN/CLOSED`.
- `CloseDestinationLane` is a source-ordered irreversible transition for one exact Lane Incarnation/`expectedLaneControlVersion`. A strict Lane's same request must include explicit order-loss/duplicate acknowledgement, so direct Close and Break+Close have one unambiguous audit boundary. Its marker atomically closes Admission, removes READY, semantically owns/revokes Claims, freezes all truly unadmitted messages and uncommitted payload reservations, transfers state-split aggregate counters once, and starts a restartable quota-neutral canonical materialization cursor. Any Claim still physically present is tagged close-owned and never requeued. An abandoned reservation stops new upload/attestation authority, a later Commit gets a stable closed result, and already-issued upload handles remain protected through their closed deadline/quiescence GC protocol. Attempts already admitted—or any Generation retaining an older unknown attempt—are not declared absent: success completes normally, definitive non-publication becomes a closed-after-admission terminal after required evidence retirement, and unknown remains in possible-delivery escrow; retry is forbidden because the Lane is closed.
- `BreakOrderingDomain` moves one exact Lane Incarnation/`expectedLaneControlVersion` to distinct `ORDERING_BROKEN` with an explicit duplicate/order-loss acknowledgement and audit. It atomically removes READY/revokes reversible Claims, but does not perform `CloseDestinationLane`'s bulk freeze or counter transfer, migrate, or silently release old pending messages; those require a later exact Close/Resolve action. Continued ordered traffic requires a new Profile/Ordering Domain.
- `DrainShard` performs the ownership drain protocol and transfers placement.
- `FenceShardForMaintenance` intentionally stops mutation after revoking source/lease authority and is required for offline verification or repair.
- `ForceCheckpoint` requests bounded work but cannot bypass ownership, source-retention, integrity, or catalog-CAS checks.

No administrative “force” can publish without durable Admission, restore below Recovery Floor, treat missing source data as empty, delete protected payload/checkpoints, or silently change a Destination Binding.

After runtime Lane metadata is reclaimed, a compact terminal control guard keyed by `DestinationLaneId` remains. It prevents Schedule/Prepare/Commit/Replay from reopening the same old Profile/Ordering Domain tuple under a fresh Lane Incarnation.

Dead Letter Replay and Uncertain resolution are signed service System Mutations referencing an authenticated immutable Control Operation, so they are ordered with Client Commands and cannot be forged by a principal that only has tenant Route produce access.

## Preparation, registration, and lifecycle

An authenticated gateway prepares a serializable `PreparedControlOperation` before any control write or Shard Log enqueue. It fixes operation ID, canonical request/hash, actor/scope, exact target snapshot and indices, expected versions, acknowledgement flags, and the expected signed mutation ID/hash for each source-ordered target. Registration returns exactly `RECORDED(ControlOperationReceipt)`, `DEFINITELY_NOT_RECORDED`, or `RECORD_UNCERTAIN`; uncertain recovery only rereads/retries that exact prepared object.

Per-target and aggregate state are distinct closed types:

```text
TargetMarkerState:
  PENDING | ENQUEUE_UNCERTAIN | QUEUED | EFFECTIVE
  | MATERIALIZING | COMPLETED | REJECTED | FAILED_BEFORE_EFFECT

ControlOperationState:
  PENDING | DISPATCHING | PARTIALLY_EFFECTIVE | IN_PROGRESS
  | SUCCEEDED | SUCCEEDED_WITH_OUTSTANDING
  | REJECTED | FAILED_BEFORE_EFFECT
```

`EFFECTIVE` means the signed marker was durably applied at its Source Position; it does not mean Close materialization, admitted-attempt resolution, drain, checkpoint upload, or every target has completed. `PARTIALLY_EFFECTIVE` and `IN_PROGRESS` are non-terminal and must roll forward under the same operation. `REJECTED`/`FAILED_BEFORE_EFFECT` are legal only when no target became effective. Operation-specific terminal typed result supplies close/outstanding/checkpoint details; generic state never fabricates them.

## Uncertain outcome control

An authorized operator may resolve a stuck `UNCERTAIN` generation only through an audited command that chooses one explicit action: attach externally verifiable success evidence, retry with `allowPossibleDuplicate=true`, or terminalize with the possible-delivery flag. Cancel and Reschedule remain `TOO_LATE`. Operator assertion without verifiable evidence is labelled as an override in all query/DLQ/audit results and never upgrades the advertised Adapter guarantee.

## Audit

Each Control Operation records authenticated actor, tenant/scope, request hash, expected and resulting versions, timestamp source, reason/ticket, outcome, and any override evidence reference. The authoritative control mutation is never considered absent merely because export to an external audit sink is uncertain; audit export follows a stable outbox ID and exposes its own status.
