# Journal Pulsar sequence mappings before target send

Pulsar Broker `lastSequenceId` alone is not enough to recover `PULSAR_BROKER_DEDUP` after local-disk loss. A checkpoint can predate several Publish Admissions, leaving the Broker high watermark without a durable mapping from sequence IDs to Delay Message Generations. therefore requires a Nereus-owned, persistent, non-compacted Pulsar Attempt Journal for this capability. Each target cluster has one journal per Route Incarnation, with partition count equal to the Route and journal partition equal to Delay Shard partition. A Nereus-only ACL restricts the service principal; topic-wide `ExclusiveWithFencing`, not ACL identity, makes the current shard Owner the single writer. The journal and target resources pin distinct protected incarnation tokens, and every Producer SEND passes `PULSAR_RESOURCE_GUARD`. A sequence-to-generation mapping is durably acknowledged in that journal before the target Producer may be invoked.

## Mapping protocol

For one Lane-scoped stable physical-partition Producer, Publish Admission allocates and persists one sequence ID plus `PUBLISHING(mappingDurable=false)`. It then appends a canonical `MAPPED` record containing:

```text
mappingId
Route / shard / Destination Lane / Lane Incarnation
stable producer-name hash and physical target
sequenceId
delayMessageId / generation / publishAttemptId
Prepared Publish hash
journal Broker entry timestamp and guarded Source Position
```

`mappingId` is a domain-separated hash of all those identity fields. Same ID/hash duplicates are idempotent; a different body is integrity failure. A timeout or lost acknowledgement while writing the journal never authorizes target send. The Worker retries the exact mapping until it gets a Broker-durable position, persists that evidence in the shard DB, and revalidates Owner Lease, Store, Generation, and attempt immediately before target send.

Client/network retransmissions inside that exact admitted attempt reuse its sequence, attempt identity, Prepared Publish hash, and bytes. If the Adapter proves it was not published, the Worker keeps the attempt in `PUBLISHING(retirementPending=true)`, appends and acknowledges `RETIRED_NOT_PUBLISHED`, and persists that journal position before moving the Generation to `RETRY_WAIT` or terminal state. A later Publish Admission—even for the same Generation—allocates the next sequence and a new exact mapping. At most one non-retired sequence is unresolved per Producer. disables Pulsar client batching for this capability.

## Recovery

The shard checkpoint records `EvidenceCursor.PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS`: exact Destination Lane/Lane Incarnation, journal token/physical-topic creation identity/partition, evidence generation, inclusive last-applied `(ledgerId, entryId, batchIndex, batchSize)`, and guarded maximum Broker persistence time through that cursor. After acquiring its lease, recovery first creates the physical journal-partition Producer with Pulsar `ExclusiveWithFencing`. This use is safe because the partition is Nereus-owned by exactly one shard, unlike a shared business topic. It fences old-writer late appends. Every Journal reader initial connect/reconnect then carries expected token/creation identity through `PULSAR_SUBSCRIBE_RESOURCE_GUARD`; the Broker validates the exact ManagedLedger before adding the Consumer. Only a certified generation may capture the batch-aware last MessageId and replay through that barrier. Recovery seeks the containing entry and skips applied members, rejects mapping conflicts, and reconstructs every sequence assignment/retirement that can postdate the checkpoint.

- Broker last sequence at or above a non-retired mapped sequence proves that mapped Publish Attempt was published, because no later sequence is sent until every lower mapping is published or durably retired.
- A lower Broker value proves non-publication only while the mapping's guarded Broker-time anchor is inside the inactivity horizon and a physical-topic producer-cardinality proof shows every required producer key fits strictly below `brokerDeduplicationMaxNumberOfProducers` with safety margin. Cap pressure, reload omission, or missing producer state is divergence, not absence.
- Broker sequence above the maximum mapped sequence, missing journal retention, conflicting mapping, or an unverifiable dedup horizon is `PULSAR_EVIDENCE_DIVERGENCE` and fail-closed.

The generic maximum from a partitioned Producer is never per-partition evidence. Journal records and cursor positions are protected by the Recovery Set/Floor retention formula.

Without this journal, stable producer names and exact-attempt sequence reuse may reduce duplicates while the local attempt mapping survives, but they remain baseline behavior and cannot register the recoverable `PULSAR_BROKER_DEDUP` outcome capability.

## Ordering and isolation

The Producer and sequence domain remain Lane-scoped. An ordered Lane never advances past an unresolved mapping. If evidence or the dedup horizon is permanently lost, runtime readiness remains `BLOCKED`; only an authenticated source-ordered Break/Close can enter `ORDERING_BROKEN/CLOSED`. An operator may create an explicit new Ordering Domain/incarnation with a visible order-break audit, but cannot continue the old strict-order claim.

Attempt Journal availability is a shared declared dependency for strong Pulsar Lanes. Lane-scoped Adapter admission prevents one Lane from consuming unbounded journal Producer buffers; outage removes READY and marks affected capability Lanes runtime `BLOCKED` without writing `ADMIN_PAUSED` or stopping Command application.
