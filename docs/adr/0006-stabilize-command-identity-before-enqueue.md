# Stabilize Command identity before enqueue

Nereus Delay V1 fixes a Command's identity before any network I/O and models enqueue as `QUEUED`, `DEFINITELY_NOT_QUEUED`, or `ENQUEUE_UNCERTAIN`. A serializable, immutable `PreparedCommand` contains stable `commandId`, `delayMessageId`, route, partition, canonical command bytes, and a `commandHash` over the entire business meaning of the Command. Every retry of the same logical operation reuses this exact Prepared Command; a timeout, canceled wait, lost acknowledgement, or connection failure after Producer submission never authorizes creating a new identity.

## Consequences

- Preparation performs deterministic local validation, routing, canonical serialization, identity allocation, and hashing without Producer or server I/O. Prepared Commands may be persisted and retried after process restart.
- Expected transport ambiguity is represented as a normal `EnqueueOutcome`, not inferred from arbitrary exceptions. `DEFINITELY_NOT_QUEUED` requires Adapter-specific proof of non-persistence; otherwise the result is `ENQUEUE_UNCERTAIN`.
- Physical enqueue attempts receive separate trace identities, but cannot change the logical IDs, route, partition, canonical bytes, or Command Hash.
- The Delay Shard stores `commandId -> commandHash + authoritativeResult`. Within the fixed retry window, the same ID and hash is an idempotent no-op that preserves the original result; the same ID with a different hash is `COMMAND_ID_CONFLICT`. A physical record persisted after `retryUntil` receives a position-level `COMMAND_RETRY_WINDOW_EXPIRED` regardless of whether compact dedupe still exists.
- `delayMessageId` independently protects message-entity identity. Any different first-seen initial Schedule cannot overwrite an active or retained message and yields `DELAY_MESSAGE_ID_CONFLICT`; V1 has no separate “identical Schedule under a new Command ID” exception.
- Deduplication state, command result, message-state changes, and applied source position are atomically committed before acknowledging the source record. Duplicate source records only advance the applied source position.
- Querying an uncertain enqueue by bare `commandId` may return `UNKNOWN`; absence of an applied result does not prove absence from the ingress Topic. The reliable recovery action is to retry the original Prepared Command.
- V1 does not allow Cancel or Reschedule to overtake an unresolved initial Schedule. Callers first retry or resolve that Schedule; deferred cancellation tombstones would require a separate future design.
