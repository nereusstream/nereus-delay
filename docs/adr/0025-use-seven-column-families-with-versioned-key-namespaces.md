# Use seven Column Families with versioned key namespaces

Nereus Delay V1 keeps exactly seven **application** Column Families per shard database: `timeline_cf`, `id_cf`, `inflight_cf`, `dedupe_cf`, `terminal_cf`, `gc_cf`, and `meta_cf`. RocksDB's mandatory `default` Column Family is opened as an eighth physical CF, stays application-empty and read/write-forbidden, and is still included in descriptors, cache/memtable/file budgets, checkpoint manifests and open-time validation. Missing/unknown CFs or a non-empty default CF fail activation. New protocol concepts use versioned one-byte key namespaces inside the seven application CFs rather than creating another memtable/tuning surface. Every authoritative mutation uses one WAL-enabled RocksDB WriteBatch across them.

## Key encoding

Keys begin with record-type and key-format bytes. The normative [`V1 Protocol Registry`](../V1-PROTOCOL-REGISTRY.md) fixes every CF tag, component order/width, typed Source Position variant and conformance vector. Ordered numeric components are fixed-width unsigned big-endian; identities use their exact fixed canonical width; truly variable bytes are u32-length-prefixed and bounded. Values use the registry's typed version envelope with required-field validation and CRC32C in addition to RocksDB block/file checksums. Unknown key or required value versions fail shard activation. Java serialization, delimiter-concatenated strings, native-endian numbers, and wall-clock TTL compaction filters are forbidden.

## Column Family namespaces

- `timeline_cf`
  - unordered due key: `[DUE=0x01][v1=0x01][destinationLaneId][eligibleAt][sourceOrderToken][delayMessageId][generation]`;
  - ordered key: `[ORDERED=0x02][v1=0x01][destinationLaneId][deliverAt][sourceOrderToken][delayMessageId][generation]`, whose value carries canonical `actionAt`, current retry eligibility and head-blocking state. `deliverAt` selects the strict business-order head; the READY time is that head's `max(actionAt, retryEligibilityAt)`. Certified Pulsar handoff uses one fixed Profile-version lead per Lane, so `actionAt=deliverAt-handoffLead` is order-preserving; V1 forbids a per-message lead;
  - every DUE/ORDERED value is exact `TimelineWorkRefV1`, whose closed kind is initial schedule, definitive retry, or unordered uncertain retry; its full embedded key/hash, candidate attempt number, runtime revision and times must match the RocksDB key and `id_cf` current-work copy;
  - ready key: `[READY=0x03][v1=0x01][nextEligibleAt][destinationLaneId][laneVersion]`; Lane state stores its exact current key, and every replacement atomically deletes the old key and inserts the new one;
  - expiry key: `[EXPIRY=0x04][v1=0x01][expireAt][destinationLaneId][delayMessageId][generation]`, independent of Lane readiness/circuit and removed only by generation replacement or terminal/close;
  - reservation expiry key: `[RESERVATION_EXPIRY=0x05][v1=0x01][reservationExpireAt][reservationId]`; scanner never guesses reservation versus Message Generation from value/length;
  - bounded system-Lane keys for DLQ export and maintenance admission.
- `id_cf`
  - current message index by Delay Message Identity, including generation, Control Version, binding/payload references, and exact `GenerationRuntimeIndexV1`: public aggregate state, the zero-or-one current send work, canonical admitted-attempt obligation ref set, Admissions/uncertain-retry counters, duplicate risk and runtime digest. Each ref stores the exact inflight key/hash and ledger state for direct old-Owner lookup;
  - compact `RETIRED_IDENTITY` tombstone after full entity history is reclaimable and until the ID's maximum first-seen freshness deadline is source-fenced and checkpoint-safe;
  - payload reservation/reference by reservation identity;
  - immutable binding/value references needed to find active state without scanning.
- `inflight_cf`
  - full `CLAIMED`, `PUBLISHING`, and `UNCERTAIN` records, including owner/store identity, claim/admission deadlines, the complete canonical Prepared Publish descriptor plus hash, attempt number, capability evidence identity, and permit accounting;
  - one reversible current Claim may coexist with older UNCERTAIN ledgers; one current PUBLISHING ledger may likewise coexist with older UNCERTAIN ledgers. Every admitted ledger remains in the runtime index's bounded obligation set until exact outcome/evidence/charge retirement.
- `dedupe_cf`
  - `[COMMAND=0x01][v1=0x01][commandId]` compact Client Command identity/hash/outcome and retry window;
  - `[RESULT=0x02][v1=0x01][commandId]` full public query result;
  - `[POSITION=0x03][v1=0x01][typedCanonicalSourcePosition]` position audit/quarantine;
  - `[FENCE=0x04][v1=0x01][fenceProofId]` fence/reclamation evidence;
  - `[SYSTEM_MUTATION=0x05][v1=0x01][systemMutationId]` signed mutation hash/type/author/scope/deadline/stable result;
  - full query result with shorter retention;
  - Source Position and System Mutation namespaces are never exposed as bare Command IDs.
- `terminal_cf`
  - immutable per-generation `SUPERSEDED`, `PUBLISHED`, `HANDED_OFF`, `CANCELED`, `EXPIRED`, and `DEAD_LETTER` history;
  - independently updated DLQ Export outcome and retained target evidence.
- `gc_cf`
  - time-ordered guarded deletion tasks for payloads, terminal/result/dedupe/retired-identity state, checkpoints, and orphan uploads;
  - every task carries exact expected identity/version/checksum and Checkpoint Safety Barrier requirement.
- `meta_cf`
  - fixed shard/store/DB identity, format and schema versions, Route Incarnation and partition, applied Shard Log position and typed evidence cursors, checkpoint/catalog identity, last opened Owner Epoch, clean-close marker, and the checked per-Store next-Claim sequence used by the Protocol Registry's collision-free Claim/Publish Attempt ID derivation;
  - Lane state/version/circuit/fairness counters;
  - Shard Quota Grant usage and Worker reconciliation metadata;
  - destination producer sequences/channels and bounded GC/checkpoint progress.
  - bounded `SLO_OUTBOX=0x08` entries keyed by exact sample ID. These are observational, reconstructible/monotonic-conservative records outside the command-derived state digest; their budget is disjoint from correctness/outcome reserve.

Lane and fixed identifiers are collision-resistant canonical hashes, but their values retain enough canonical source identity to detect a hash collision before merging state.

## Atomic mutations

- Shard Log apply writes dedupe/result/position audit, message or reservation state, timeline/inflight/terminal changes, old/new READY replacement, Lane/quota counters, and final `appliedShardLogPosition` atomically.
- Claim moves one exact timeline record to `inflight_cf` and updates the ID locator and Lane permits atomically.
- Publish Admission replaces reversible claim state with exact `PUBLISHING` evidence before the Adapter call.
- Publish Admission also validates the Claim's source-work kind/counters/obligation-set digest, checked-increments Admissions and, only when an older UNCERTAIN ledger exists, uncertain-retry Admissions, then adds the new attempt to the canonical obligation set in that same batch.
- Callback/outcome, retry, terminal, Cancel, Reschedule, signed Replay mutation, payload commit, and GC compare their exact generation/runtime/owner/store or immutable attempt-ledger tokens and update every duplicate index in one batch. A retained prior `UNKNOWN` attempt can append evidence without pretending it is the current runtime revision. Late success deletes reversible current work but cannot erase another admitted attempt; terminal history retains that open obligation and later callbacks only retire its evidence/charge.

All correctness batches keep WAL enabled. `sync=true` is group-amortized for command-application batches and for Claim/Admission/outcome groups only where the state machine allows grouping; a Producer call never starts before the exact Admission batch is durably synced. Source acknowledgements likewise follow the command batch sync.

## Scan and consistency rules

The shard event loop is the single writer. Scheduler scans use bounded RocksDB snapshots and iterate Lane-ready/due prefixes with upper bounds; the event loop revalidates READY version, exact ID locator, and runtime revision before Claim, so an old key visible in a concurrent snapshot cannot publish. Persistent orphan/missing/version-mismatched READY keys fence scheduling and require deterministic rebuild rather than ordinary GC. Ordered Lane heads are selected by Delivery-Time FIFO and remain blocking through retry or uncertainty. Unordered Lanes scan by eligibility.

Invariant audits verify one runtime index for the Message Identity's current Generation or protected retired-identity tombstone; zero or one current TIMELINE/CLAIMED/PUBLISHING work; zero through pinned-maximum admitted attempt ledgers covered by that nonterminal/current-terminal runtime set or an older terminal open-obligation summary; matching work/attempt counters and digests; counter sums, Lane head/readiness, Source Position monotonicity, and terminal-decision immutability. A current terminal record mirrors retained PUBLISHING/UNCERTAIN obligations with current work NONE; after Replay, its summary is their sole Generation locator and the new runtime index starts empty. Any ambiguity or cross-generation reference is fail-closed; repair requires a fenced shard and deterministic rebuild from authoritative records.

## Local layout

Each installation lives at:

```text
<root>/shards/<routeIncarnation>/<partition>/
  ACTIVE
  incarnations/<storeIncarnation>/db/
  checkpoint-tmp/<checkpointId>/
  restore-tmp/<checkpointId>-<nonce>/db/
```

Restore populates a unique temp directory, verifies manifest/file checksums, DB and shard identity, format, and replay availability, then generates and WAL-syncs a fresh Store Incarnation. It closes install mode before atomically replacing and fsyncing the checksummed `ACTIVE` pointer, then opens normally. The checkpoint creator's Store token is never reused, files are never copied over an open database, and two shard identities never share one DB directory.
