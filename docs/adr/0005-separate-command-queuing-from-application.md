# Separate Command queuing from authoritative application

Nereus Delay uses a two-stage Command contract. The default high-throughput API returns `CommandQueuedReceipt` only after the ingress Broker durably stores a Schedule, Cancel, or Reschedule Command; this confirms queuing, not validation or operation success. A Delay Shard later performs authoritative validation and durably records either `APPLIED` with the command-specific outcome or `REJECTED` with a stable reason code. A Delayed Message begins its lifecycle only when its Schedule Command is successfully applied.

## Consequences

- Queuing APIs return `CommandQueuedReceipt`; field names describe Broker persistence and never imply that a message was accepted or scheduled.
- Applied results are stored by `commandId` and are queryable by both `commandId` and `delayMessageId`. A rejected Schedule does not create active `timeline_cf` or `id_cf` state.
- The command-result and deduplication namespaces in `dedupe_cf`, message-state changes, and `appliedShardLogPosition` are committed in the same durable shard `WriteBatch` before the source position is acknowledged.
- Schedule, Cancel, and Reschedule all separate their queued receipt from their operation result. Optional `awaitApplied` and combined convenience APIs wait for the persisted result without bypassing the Command Topic.
- Query distinguishes `PENDING`, `APPLIED`, `REJECTED`, `UNKNOWN`, and `RESULT_EXPIRED`. A receipt's route, partition, and source position may prove that a known Command is still pending; an unknown bare `commandId` is not reported as definitively absent.
- SDK validation is limited to deterministic local checks. Authority, policy, live quota, destination, and current-state checks remain server-side.
- Deterministic business, policy, or protocol failures may be `REJECTED`. Transient infrastructure failures leave the source record unacknowledged and trigger retry/backpressure; an unparseable poison envelope is quarantined and audited rather than silently skipped.
