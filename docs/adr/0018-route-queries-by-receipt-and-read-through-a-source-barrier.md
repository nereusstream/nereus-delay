# Route queries by receipt and read through a Source barrier

Nereus Delay V1 keeps authoritative Command and message results in the owning shard database and routes reads through the current Oxia Owner Lease. Public receipts carry Ingress Route, partition, identity, and—when Broker queuing was confirmed—Source Position. They may carry a tenant display hint, but authorization always comes from Authenticated Tenant Context and the Route Security Domain. A read-after-command request can use that position as a Query Barrier. V1 does not add a second global result log or eventually consistent search index to the correctness path.

## API outcomes

- `prepare` returns a serializable `PreparedCommand`; `enqueue` returns `CommandQueuedReceipt`, `DefinitelyNotQueued`, or `EnqueueUncertain`. Only the first contains a Broker-confirmed Source Position and `receiptQueryUntil = sourcePosition.brokerPersistenceTime + immutable queuedReceiptQueryWindow`; Worker apply time and SDK receipt time never move this boundary.
- `CommandLocator` is exactly `QueuedCommandLocator(CommandQueuedReceipt) | BareCommandLocator(CommandId)`. `awaitApplied` accepts only the queued form. A queued query may return `PENDING` before its barrier but cannot return `UNKNOWN` afterward; a bare query may return `UNKNOWN` but never `PENDING`.
- `MessageLocator` is exactly `ManagedMessageId(DelayMessageId) | ManagedMessageReceipt(CommandQueuedReceipt with MessageSubject)`. Native receipts are statically excluded.
- `CommandQueryResult` is `PENDING | APPLIED | REJECTED | RESULT_EXPIRED | RESULT_EVIDENCE_EXPIRED | UNKNOWN | INVALID_RECEIPT | RECEIPT_MISMATCH | NOT_FOUND_OR_NOT_AUTHORIZED | SHARD_TRANSITIONING | SHARD_UNAVAILABLE | INTEGRITY_ERROR`.
- `MessageQueryResult` is `RESERVED | ACTIVE | TERMINAL | IDENTITY_RETIRED | UNKNOWN | INVALID_RECEIPT | RECEIPT_MISMATCH | NOT_FOUND_OR_NOT_AUTHORIZED | SHARD_TRANSITIONING | SHARD_UNAVAILABLE | INTEGRITY_ERROR`. `IDENTITY_RETIRED` means the compact message-identity tombstone remains after full details expired. `UNKNOWN` does not prove nonexistence or authorize ID reuse; a source-closed UUID freshness deadline may be reported separately as first-Schedule-ineligible without claiming historical existence. It never treats `QUEUED` as a scheduled message and reports business state separately from DLQ Export state.
- `awaitApplied` is a bounded long-poll convenience over the same query. It does not bypass the Command Topic or change durability. Waiters are bounded per tenant and Worker, are not durable, and receive a retryable shard-transition result on owner loss.

## Routing and consistency

The gateway first validates bounded receipt syntax/type, then authorizes its Route Incarnation/tenant using trusted registry state and Authenticated Tenant Context, and only then resolves `(Route Incarnation, partition)` to an `ACTIVE_FOR_COMMANDS` Owner Lease. Invalid syntax/type is `INVALID_RECEIPT`; unknown-route and cross-tenant requests share the non-enumerating `NOT_FOUND_OR_NOT_AUTHORIZED` projection. It forwards with the observed Owner Epoch and refreshes once if the owner rejects that epoch. `ACQUIRING`, `RESTORING`, `CATCHING_UP`, `DRAINING`, absent ownership, and Oxia ambiguity return `SHARD_TRANSITIONING` or `SHARD_UNAVAILABLE`; the gateway never reads a stale local directory. Query availability does not wait for every Destination Lane to become `READY`.

For a queued receipt, the owner waits up to the request deadline until `appliedShardLogPosition >= receipt.sourcePosition`. Before that barrier it may return `PENDING` with current progress. At or beyond the barrier it returns the exact command result, a retained compact result, `RECEIPT_MISMATCH`, or `RESULT_EVIDENCE_EXPIRED`; it cannot return `UNKNOWN`. Caller-supplied route/partition/position/commandId/hash/subject mismatch is not a server `INTEGRITY_ERROR`, and safe mismatch details are exposed only after authorization. A bare identity without a position can return `UNKNOWN`; absence from RocksDB is not proof that an uncertain enqueue was absent from the Broker.

Wait registration follows “register, re-read durable state, then sleep” so application cannot race between the initial read and waiter installation. Completion notifications are only wake-up hints; every response is reconstructed from RocksDB under the current shard identity.

After satisfying the barrier, the Owner reads Command and message state from one consistent RocksDB snapshot under the same Owner Lease and Store Incarnation. The response linearizes at that snapshot. Lease or Store change during the read discards the data and returns a shard-transition outcome rather than serving a closed/stale database.

## Result retention and audit

Every applied Shard Log record writes a compact position audit alongside record dedupe, result, state mutation, and `appliedShardLogPosition` in one WriteBatch. Full results may expire before compact idempotency evidence. While compact evidence exists, query returns `RESULT_EXPIRED` plus stable status and reason; after all evidence is checkpoint-safe and reclaimed, a bare lookup is `UNKNOWN`.

A queued receipt's position audit remains protected until its fixed `receiptQueryUntil` has been closed by TIME_FENCE, source has passed that fence, a descendant Recovery Floor contains the audit mutation, and minimum audit retention has elapsed. Full-result retention is likewise derived from the first record's Broker persistence time, never replay-time wall clock. After contractual audit expiry, the queued result is `RESULT_EVIDENCE_EXPIRED`, never a fabricated `UNKNOWN` or mismatch.

Query and long-poll authorization is derived from authenticated tenant context, not from tenant fields in a caller-provided receipt. Receipt identity and Command Hash are validated against the stored record. Cross-tenant existence is never revealed.
