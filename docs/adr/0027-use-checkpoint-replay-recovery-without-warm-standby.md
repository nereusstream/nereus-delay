# Use checkpoint/replay recovery without warm standby

Nereus Delay V1 recovers one shard from a verified immutable RocksDB checkpoint plus complete Shard Log replay and does not implement a warm standby. The physical Command Topic partition is that Shard Log: it interleaves tenant Commands with authenticated System Mutations for Publish Admission, outcomes, expiry, evidence resolution, and resource retirement/deletion. Reversible Claim/materialization and other derivable runtime state may be absent and is rebuilt. No second delta log exists.

## Checkpoint creation

- The active Owner creates a native RocksDB checkpoint of its one-shard DB. The checkpoint's own `meta_cf` is the authority for applied Shard Log Position, the canonical sorted `EvidenceCursorV1` array, DB/shard identity, format versions, quota/Lane state, and local runtime state. Evidence cursors use the main spec's tagged Kafka-receipt/Pulsar-Attempt-Journal variants; no untyped destination receipt cursor is accepted.
- Local creation uses a unique temporary directory and may hard-link immutable files on the same filesystem. The uploader writes to a unique immutable object prefix, records every relative filename, length, checksum, object version/etag, and total bytes, and reopens/verifies the checkpoint metadata before publication.
- Upload completion alone has no recovery meaning. The Worker revalidates Source Assignment, exact Owner Lease, store/DB incarnation, source replay margin, and base catalog version, then uses Oxia CAS to add the manifest to the Recovery Set. Lost CAS response is resolved by rereading the exact checkpoint identity.
- Creation and upload are staggered and bounded by per-Worker concurrency, bandwidth, temporary-disk, and object-request limits. Failure leaves an orphan candidate but never replaces the catalog.
- Scheduled creation and planned-drain final creation both enter the shared bounded `CHECKPOINT` work class. A drain request carries one fixed checkpoint identity and exact DRAINING Owner Lease; queue rejection has no Store/filesystem side effect, and a queued final checkpoint must be selected before the Owner may close or release its lease.

## Restore

The new Owner first prefers the Store Incarnation named by a verified checksummed local `ACTIVE` pointer. Otherwise it selects the newest valid catalog member, downloads it into a unique restore temp directory, verifies manifest/object/file checksums and versions, DB/shard/route identity, the checkpoint's source Store Incarnation, store format, source and receipt replay availability, and generates a fresh local Store Incarnation. It renames the directory into the shard's incarnation set, install-mode opens and WAL-syncs the new local identity, closes it, fsyncs the directory, then atomically replaces and fsyncs `ACTIVE` before normal open. It never reuses the checkpoint creator's Store token, overwrites an open DB, or merges files.

After opening, it:

1. seeks the Shard Log to the Adapter-defined replay successor of checkpoint `appliedShardLogPosition` (Kafka next offset; Pulsar containing entry plus batch-aware skip);
2. replays idempotently until the typed Activation Barrier predicate is met—Kafka exclusive LSO cursor from the pinned UUID Fetch response, or Pulsar inclusive batch-aware last MessageId bound to the same guarded source resource/partition/consumer-connection generation; name-only admin observations are not barriers;
3. requeues reversible `CLAIMED` state only when it is not covered by a source-ordered Close overlay, preserving semantic work/authority/candidate/digest and prior obligation set while issuing a new runtime revision/instance digest; resumes Close materialization cursors, prepares the exact source-ordered recovery-UNKNOWN Outcome for unresolved prior-owner `PUBLISHING` (which becomes `UNCERTAIN` only when that record applies), and marks each Lane `RECOVERING_EVIDENCE`;
4. validates shard/source invariants and becomes `ACTIVE_FOR_COMMANDS`;
5. independently fences/initializes each Lane's stronger channels and replays its Kafka receipt partition or Pulsar Attempt Journal through a post-lease barrier;
6. marks only that Lane `READY` for Claim/Admission when its evidence and capability checks pass.

A failed target, receipt partition, or Attempt Journal keeps only the affected Lane blocked. It cannot hold the shard's Command application or unrelated Lane activation behind destination recovery.

If the newest checkpoint is corrupt or incomplete, recovery may fall back only to another member at or above the Recovery Floor whose required source/evidence logs are retained. Failure of every permitted candidate is fail-closed.

## Recovery semantics

Command- and System-Mutation-derived state after the selected checkpoint replays in its original Source Position order, including quota releases and exact Cancel/Admission/expiry races. A valid Admission always rebuilds the same `PUBLISHING`. If the live first-send gate did not survive, recovery then appends the exact initial `UNKNOWN/OWNER_FENCED/RECOVERY_FIRST_SEND_UNCERTAIN/UNCERTAIN_HOLD` Outcome at a later Source Position; stronger evidence resolves it, while an unordered bounded baseline policy may retry with duplicate risk and an ordered Lane holds its head. Checkpoint cadence is therefore an RTO and uncertainty-window control, not a substitute for destination idempotency.

Planned drain may create a final checkpoint to reduce replay, but correctness assumes it can fail. Checkpoint Safety Barriers protect payload, terminal, dedupe, receipt, and checkpoint objects from being reclaimed while any permitted recovery image can still require them.

## Deferred standby

A future warm standby may consume the complete Shard Log only with an explicit independent replay subscription, snapshot cut, resource budget, and integrity protocol. It must remain non-publishing until it acquires both Source Assignment and a new Owner Lease. V1 implements none of that; neither joining the active source subscription twice nor copying live RocksDB files is a standby design.
