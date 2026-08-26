# Use atomic Shard Quota Grants and reserve control capacity

Nereus Delay makes Schedule capacity a durable shard-local decision. Tenant-wide hard limits are pre-divided into versioned Shard Quota Grants whose sum cannot exceed the tenant policy; a shard atomically updates its grant usage, Lane usage, message state, Command result, and Source Position in one RocksDB WriteBatch. Nereus Delay does not pretend that independent per-shard checks form an exact global counter, and it does not dynamically borrow hard capacity across shards.

## Accounted resources

Each shard persists counters for active messages and logical bytes, retained logical bytes, payload reservations, Lane count, per-Lane active messages/bytes, inflight messages/bytes, command-idempotency records, terminal/DLQ records, and outbox records. Active/pending counts each nonterminal Message Generation once even when its aggregate is UNCERTAIN and it also has current work plus multiple attempt ledgers. Inflight independently counts every reversible Claim and every admitted attempt obligation, with execution/attempt bytes that do not duplicate payload ownership. A terminal Generation can therefore release active/pending while open attempt obligations remain charged inflight. Worker-level guards separately cover open DBs, memtables, block cache, file descriptors, physical local disk, executors, and checkpoint I/O.

All authoritative byte decisions use fixed `QUOTA_ACCOUNTING`. Payload ownership is the exact inline length or immutable object length. Publish cost is payload length plus the canonical Adapter-metadata encoding and a fixed versioned envelope charge. Logical state cost is canonical key length plus canonical typed-value length and a fixed record charge. The checked resource-charge vector is persisted with each reservation, Message Generation, and retained record; payload ownership is counted once per Message Identity and Reschedule does not duplicate it. Each counter names one charge class, duplicates reuse the first result, and release uses the stored charge rather than recomputing under a new binary.

RocksDB compression, WAL/SST layout, memtable shape, compaction amplification, filesystem allocation, and Object Store billing size never decide a durable Command result. They belong to physical safety admission and deployment certification. An unknown accounting version stops source application at that record rather than guessing a charge.

- Inline Schedule consumes active and retained capacity.
- `PrepareLargeSchedule` consumes reservation and retained-capacity headroom; Payload Commit atomically transfers reservation usage to active message usage without double counting.
- Reschedule inthe current design cannot change payload or destination and therefore moves timing state without releasing and reacquiring message bytes. Replay consumes a fresh active-message slot but reuses already retained payload.
- Cancel, publish, expiration, and dead-letter transition release active backlog and inflight permits, but retained logical/object ownership charges remain until guarded physical GC actually removes the state or payload.
- Duplicate Commands and lost-response retries reuse the original accounting result. Counter underflow, overflow, or mismatch is shard corruption and fails closed.

## Hard grants and policy changes

The control plane creates static `(tenantId, routeId, partitionId, grantVersion)` allocations for pending messages, pending bytes, reservations, and Lane cardinality. The deployment validator proves that their sums fit the tenant-wide policy. Grant growth is safe only through the serialized shrink-first transfer below; a reduction below current usage is grandfathered and blocks new admission without evicting existing messages. Nereus Delay has no opportunistic online borrowing outside that protocol.

“Admission” in the preceding rule means only capacity-increasing Command application: first Schedule, `PrepareLargeSchedule`, and Dead Letter Replay. A below-usage shrink never blocks Claim, Publish Admission, outcome application, Cancel, terminalization or guarded GC for already charged work; otherwise the shard could not drain below the new limit.

Current quota redistribution is a serialized, shrink-first Control Operation, not dynamic borrowing:

1. Oxia CAS creates one immutable `QuotaTransferPlan(operationId, requestHash, tenantPolicyVersion, oldGrantSet, newGrantSet)`. Every new grant version is staged inactive and the validator proves both final totals and checked arithmetic.
2. Source-ordered signed `APPLY_SHARD_CONTROL(GRANT_DECREASE_OR_HOLD)` markers are appended to every shard whose allowance falls. Each marker makes the lower admission ceiling effective; when usage is already above it, only the capacity-increasing operations listed above are blocked. The grandfathered excess remains charged to both the tenant hard envelope and the donor Worker's committed physical envelope.
3. A decrease does not create transferable credit until every affected resource dimension satisfies `persistedUsage <= newGrant`. The donor then source-orders `GRANT_SHRINK_DRAINED(planHash, counterDigest, usageVector)`; apply recomputes the counters and rejects a false claim. The operation waits for all such markers and exact plan-bound results. Failure or response loss resumes the same plan; no increase is sent speculatively.
4. Before any recipient increase, placement reserves the full physical-envelope delta on its target Worker and binds that reservation to the plan. Only then may source-ordered `GRANT_INCREASE_ACTIVATE` markers activate increases. At every mixed state, `sum(max(effectiveGrant, grandfatheredUsage))` for every resource dimension remains at most the tenant hard policy.

Concurrent plans for the same tenant/policy are rejected by CAS. Abort before step 4 leaves the safe decreased allocation; after any increase marker is emitted the operation must roll forward under the same plan. The design does not lend unused capacity outside this protocol.

An unavailable policy service does not revoke already pinned grants, but it prevents activation with an unknown grant version and prevents grant expansion. Destination outage cannot consume unbounded capacity because each affected Lane and shard has a hard grant.

## Disk-pressure behavior

Every Worker holds a configured Control Capacity Reserve plus compaction and checkpoint headroom outside Schedule grants. It is explicitly partitioned:

```text
sum(shardOutcomeReserveGrant)
+ nonOutcomeControlReserve
+ recoveryWorkingReserve
+ emergencyControlHeadroom
<= controlReserveBytes
```

This is the byte projection; the same inequality is checked componentwise for the applicable `ReserveVector` records, Broker-writer records/bytes/rate, WAL and DB-result dimensions. The four terms are disjoint. `nonOutcomeControlReserve` contains separately metered fence-evidence, position/quarantine/control audit, terminal and GC metadata partitions. Compaction/checkpoint/restore temporary file bytes charge dedicated temp headroom outside Control Capacity Reserve and cannot consume outcome/fence/recovery guarantees. One unit cannot satisfy two classes.

- Logical grant limits are the replay-stable Schedule admission watermark. They reject new Schedule, payload reservation, and replay before certified physical capacity approaches the disk safety watermark; Cancel, eligible Reschedule, result writes, terminal transitions, DLQ progress, and GC continue.
- The capacity proof requires all logical grants plus worst-case write/compaction/checkpoint amplification to fit below the physical safety watermark while leaving Control Capacity Reserve and temporary headroom.
- Actual physical disk pressure outside that certified envelope is a shard-safety failure, not an input to an ACKed business result. The shard stops Claim/Admission and Source application at the current position rather than producing a rejection that could differ after restore on another disk.
- RocksDB corruption/stall beyond the safety deadline, source gap, recovery failure, or inability to durably record already-admitted outcomes likewise triggers Shard Safety Backpressure.
- At placement/Owner activation the Worker assigns the Protocol Registry's immutable per-shard `CapacityGrant(OUTCOME_RESERVE)`, bound to reserve-source version, grant ID and the complete zero-explicit `CapacityVector` digest. It is embedded as a projection of the full `ShardCapacityEnvelope`, stored in Oxia placement and `meta_cf`, and revalidated on every Owner/Store change. No online cross-DB observation or borrowing decides correctness, and a component grant is never added twice to the full envelope.
- The worst-case vector covers Publish Outcome/Evidence Resolution, permanent Claim Result, every policy-permitted numbered DLQ export attempt/evidence result, expiry/retire callback candidate outboxes, Shard Log System Mutation producer queue records/bytes, the Route Broker's non-borrowable records/bytes/rate system-writer quota, local WAL/DB result and retained recovery state. Max Admissions, DLQ retry policy and mutation retry windows bound concurrency. Tenant ingress cannot consume the system-writer quota; placement validates that Broker quota before Owner activation. Durable SLO observations use a separate bounded evidence budget and cannot be double-counted as outcome reserve.
- Successful Publish Admission atomically charges the exact attempt's vector against its shard partition in the same WriteBatch as `PUBLISH_ADMISSION`. Logical callback timeout does not release it. The charge is released or reduced only when the logged outcome/retirement state and its actual retained charge are durable and checkpoint-safe.
- If an already ordered Admission mutation cannot fit its full replay-stable shard vector, it applies as `ADMISSION_CAPACITY_GATED`, revokes the reversible Claim, restores timeline eligibility, advances Source Position, allocates no attempt and calls no Producer. It never blocks later Outcome/Resolution/Cancel/terminal/GC records that can free capacity. Only inability to durably apply an obligation that was already charged is Shard Safety Backpressure.
- New Admission is withheld when its shard partition cannot hold the worst-case result, even if another shard's partition appears idle. Repartitioning requires a fenced placement/control operation that first proves all outstanding charges.

Deployment certification maps logical grants to worst-case physical amplification and reconciles them against actual RocksDB live data and disk watermarks. Numeric defaults are benchmark outputs, but the accounting version, reserve, and every global guard are mandatory configuration.

## Abuse and reconciliation

Ingress Broker quotas and tenant-isolated Ingress Routes bound rejected-command and dedupe growth; Schedule rejection alone cannot protect a shared partition from an unlimited command-ID flood. Background audits compare persisted counters with record scans and emit corruption alerts. Repair is an explicit offline or fenced-shard operation, never an unjournaled counter overwrite on an active shard.
