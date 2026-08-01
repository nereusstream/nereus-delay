# Make Command application deterministic under replay

Nereus Delay V1 requires the authoritative result of each Shard Log record to be a deterministic function of its Canonical Client Command or signed System Mutation body, Broker Source Position and persistence time, the preceding durable shard state, exact immutable configuration versions, and preceding source-ordered controls. Restoring the same permitted checkpoint and replaying the same Shard Log prefix must reconstruct the same Command results, Admission/outcome/expiry order, message state, quota counters, bindings, and control boundary regardless of Worker, wall clock, cache timing, local disk size, or live destination condition. A remote request with no logged Outcome remains `UNCERTAIN`; that evidence boundary does not permit recomputing the old local order.

## Pinned semantic inputs

- Schedule-like Commands carry exact Destination Profile and Retry Policy versions; the server never resolves `latest` during application or replay.
- Route Incarnation, Broker-time acceptance cutoff, canonicalization version, routing version, tenant Security Domain, and validation limits are immutable inputs.
- Timing validation uses the record's Broker persistence time. Current Trusted UTC may prepare a signed `EXPIRE_GENERATION_V1`, but expiration becomes authoritative only at that System Mutation's Source Position and replay validates the persisted interval evidence.
- Destination Profile publication alone is not apply authority. Per-shard signed `PROFILE_BINDING_ACTIVATE` and `PROFILE_NEW_BINDING_CLOSE` controls create the exact Source Position window for first binding. A pre-activation or post-close first-seen Schedule is stably rejected even if a live Oxia watch later changes; a duplicate first seen within the window keeps its original result. Existing Message/Reservation binding and replay that preserves it are unaffected. A live `BLOCKED` overlay pauses publishing without rewriting bound semantics.
- Large-payload Commit references one immutable, protected object identity through a canonical service-signed `PayloadCommitProof`. Object verification and transient I/O happen in the authenticated attestation API before the Commit is prepared; source application verifies the pinned public-key version and proof locally and never contacts Object Store.
- Retry-window and reservation reclamation use only a canonical system-signed `TIME_FENCE` ordered in the same source partition. Its `closeThrough` monotonically advances durable `closedIngressDeadlineThrough`; ordinary records and live wall clock cannot advance or decrease that boundary.

## Source-ordered control markers

Profile binding activation/closure, quota-grant activation, `StopNewSchedules`, and any Lane pause/resume requiring an exact Command boundary are applied through signed `APPLY_SHARD_CONTROL_V1` System Mutations in the same physical Command Topic partition. The record references an immutable authenticated Oxia Control Operation and includes its exact target, mutation ID/hash, scope, semantic version/hash, expected prior version and request hash. A tenant that merely has produce access cannot forge privileged control.

The marker's durable RocksDB WriteBatch is the control linearization point. Commands before it use the preceding control version; Commands after it use the new version. A route-wide operation is complete only after every target shard has applied its marker. Initial control/grant markers are applied before the Route accepts tenant traffic.

The marker carries the canonical control payload while the Oxia record authenticates actor, scope, and hash. That record, every referenced semantic version, and required authorization evidence remain protected for every Recovery Floor/source window that can replay the marker.

Message-scoped privileged mutations such as Dead Letter Replay and Uncertain resolution travel as signed System Mutations bound to exact Control Operation targets. Admission, callback outcome, trusted-time expiration, evidence resolution, and resource retirement/deletion use the same authenticated System Mutation envelope. Tenant Schedule/Prepare/Commit/Cancel/Reschedule remain Client Commands. Both outer record kinds share the same physical order.

## Transient and physical conditions

Command application does not contact a destination Broker and does not turn live topic, authorization, capability, throttle, circuit, or target-cluster state into APPLIED/REJECTED decisions. Those conditions affect the pinned Destination Lane after the message is applied.

An unproven config cache miss, Oxia outage, RocksDB write failure, source gap, or physical disk-safety breach stops before Source ACK. Logical persistent grants reject new Schedule deterministically before certified physical capacity is exhausted. If actual disk pressure exceeds that certified envelope, the shard enters safety backpressure instead of emitting an environment-dependent rejection that could replay differently. Object Store outage may block upload attestation, Claim materialization, checkpointing, or GC, but is not an input to Command APPLIED/REJECTED.

## Duplicate and batch behavior

The same `commandId + commandHash` at a later Source Position preserves the first result and only advances position audit. Reuse with a different hash records a position-level `COMMAND_ID_CONFLICT` without replacing the first identity result.

A WriteBatch may apply a bounded contiguous Shard Log prefix. It stops at the first record whose deterministic inputs cannot be proven and cannot apply a later position. Release tests replay identical Client/System interleavings with different batching, cache arrival, process boundaries, Worker capacity, callback timing, and timer timing and compare a canonical logical-state digest.
