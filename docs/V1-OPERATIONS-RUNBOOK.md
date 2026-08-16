# Nereus Delay V1 Operations Runbook

Status: `DRAFT / NOT CERTIFIED` (2026-08-17)

This runbook is the operator-facing boundary for restore, fencing, Dead Letter
replay, uncertain publish resolution and disaster continuity. It is not an
authorization to edit RocksDB, Oxia records, Broker offsets or Object Store
objects directly. Every state-changing action must be a source-ordered,
authenticated V1 mutation with an exact identity and an auditable receipt.

## 1. Common preflight and stop conditions

Before any action, record:

- Route Incarnation, shard, source kind/native resource identity and exact
  source cursor;
- current Owner/Assignment generation, owner epoch, Store incarnation and
  Recovery Floor/catalog generation;
- current Route/control/profile/capability versions and trusted UTC interval;
- checkpoint ID/object versions, manifest hash, source/evidence cursors and
  any open `PUBLISHING`/`UNCERTAIN` attempt IDs;
- the exact operator identity, Control Operation ID and reason.

Stop immediately, preserve the source position, and page the owning service if
any of the following is true:

- source continuity, retention-floor coverage, checkpoint ancestry or Object
  Store version identity cannot be proven;
- Owner/Assignment/lease CAS is ambiguous, an old epoch can still write, or a
  channel/certificate binding is not exact;
- the current worker cannot decode an activated tuple, or a Broker resource
  identity/strict guard has drifted;
- SLO evidence, control reserve, recovery reserve or filesystem safety is
  outside the certified envelope;
- a proposed action would change a terminal result, release a protected
  payload, or bypass a source-ordered mutation.

The safe outcome is a bounded `FAILED`/`UNCERTAIN`/quarantine boundary and a
new incident, not a guessed success or a name-based fallback.

## 2. Restore drill

1. Close new admission for the affected Route and establish a source-ordered
   fence. Do not delete the old active Store or checkpoint objects.
2. Pin the Recovery Floor and catalog generation through the authority. Select
   only a descendant checkpoint whose Route/shard, DB identity, Store
   incarnation lineage, control snapshot, source cursor, evidence cursor,
   manifest hash and every file/object version match the pin.
3. Download into a fresh staging incarnation. Verify the signed manifest,
   object version, file length/checksum, control snapshot and recovery
   projection before opening it as a Worker Store.
4. If validation or installation fails before the ACTIVE pointer switch, leave
   the old ACTIVE pointer authoritative and remove only the failed staging
   tree. If the pointer has switched, validate the new incarnation before
   normal open; otherwise fail closed and create a new restore attempt.
5. Reacquire the Owner lease, replay from the exact checkpoint/evidence cursor,
   and prove no source gap, duplicate side effect, counter drift, open-object
   resurrection or stale Owner write before reopening admission.
6. Retain all checkpoint/object protection until the final reread and floor
   advancement are durable. Only then may REAPING/GC proceed.

Bounded local drill commands:

```bash
GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 ./gradlew test --no-daemon --console=plain \
  --tests io.nereusstream.delay.store.CheckpointRestoreCoordinatorTest \
  --tests io.nereusstream.delay.store.RecoveryCatalogTest \
  --tests io.nereusstream.delay.store.ShardStoreTest
```

The real Object Store/Oxia checkpoint boundary is exercised by
`bash e2e/run-oxia-minio-checkpoint-e2e.sh` with its locked MinIO digest and an
isolated project. A successful local or single-node receipt is not a
multi-worker disaster-restore certification.

## 3. Owner and channel fence

1. Stop new Gateway admission and mark the affected Assignment draining.
2. Perform the authoritative Oxia Owner/Assignment CAS. A local lease expiry
   is not proof that a remote old Worker is fenced.
3. Revoke the old Owner epoch and close the source/target event gate before
   releasing channel ownership. Require the successor assignment, lease,
   Store incarnation and Route/control snapshot to match exactly.
4. During catch-up, apply source records only after the clock, cursor, resource
   identity, protocol tuple and mutation signature gates pass. An activated but
   unsupported tuple stops the shard with `UNSUPPORTED_ACTIVATED_PROTOCOL`;
   an unactivated tuple is a position-level rejection.
5. Reopen physical egress only after the successor has replayed the required
   prefix, acquired the exact capability/credential binding and passed the
   monotonic-time gate. Old callbacks are audit-only after the epoch changes.

Bounded local drill commands:

```bash
GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 ./gradlew test --no-daemon --console=plain \
  --tests io.nereusstream.delay.ownership.OwnerLeaseTest \
  --tests io.nereusstream.delay.ownership.OwnerRecoveryCoordinatorTest
```

Real Kafka/Pulsar failover receipts prove only the named source/resource and
fault shape. Controller, storage, multi-worker and full chaos certification
require separate release evidence.

## 4. Dead Letter replay

1. Query the immutable terminal summary and exact generation. Confirm that the
   replay deadline, Route Incarnation, payload/history retention and every
   Recovery Floor covering the replay window are still valid.
2. Resolve the immutable retry policy and payload/profile/capability references
   by exact version/hash. Never use `latest` or mutate the old terminal
   generation.
3. Submit one authenticated `REPLAY_DEAD_LETTER_V1` operation with a new
   logical operation identity. The source-ordered result creates the next
   generation while retaining the old terminal summary and any still-open
   attempt obligations.
4. Replay the exact bytes on response loss. Same identity/bytes is a no-op;
   different bytes, generation, attempt number or evidence is a conflict and
   must not overwrite the first result.
5. After the new generation is definitively terminal and all old/new
   obligations, export protection and checkpoint barriers are covered, use
   the normal source-ordered GC path. Do not delete a payload merely because
   an external DLQ export succeeded.

Bounded local drill:

```bash
GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 ./gradlew test --no-daemon --console=plain \
  --tests 'io.nereusstream.delay.runtime.DelayShardTest.replayDeadLetterCreatesNextGenerationAndRetainsOldTerminalSummary' \
  --tests 'io.nereusstream.delay.runtime.DelayShardTest.terminalSummaryRetainsASecondOpenObligationAndReopensSafely'
```

## 5. Uncertain publish resolution

An `UNCERTAIN` state means the destination side effect is not proven absent or
present. It is not permission to retry blindly.

1. Identify the exact open attempt ledger, generation, Owner epoch, channel
   binding and evidence cursor. Hold the Lane and retain physical/zombie
   charge.
2. Prefer verified published or not-published evidence. Apply one
   source-ordered `RESOLVE_UNCERTAIN_V1` operation with an exact ControlRef and
   acknowledgement. An operator assertion alone is never evidence.
3. `RETRY_ALLOW_POSSIBLE_DUPLICATE` is limited to unordered `BEST_EFFORT` and
   remaining Admission/time/expiry/capacity bounds. It creates a
   `CONTROL_OVERRIDE` timeline marker; the later Admission, not the marker,
   consumes the attempt budget.
4. `TERMINALIZE_POSSIBLE_DELIVERY` retains the possible-delivery flag and
   obligation/evidence summary. It does not claim a successful publish.
5. If the lane is ordered, closed, broken, expired or budget-exhausted, do not
   create retry work. Preserve the exact source position and escalate.

Bounded local drills:

```bash
GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 ./gradlew test --no-daemon --console=plain \
  --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainPublishedEvidenceSettlesExactObligation' \
  --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainNotPublishedEvidenceNormalizesDefinitiveRetry' \
  --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainRetryMaterializesControlOverrideTimeline' \
  --tests 'io.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainTerminalizesPossibleDeliveryAndRetainsObligation'
```

## 6. Disaster boundary

If the active cell loses authoritative continuity, do not infer continuity
from Kafka offsets, Pulsar MessageIds, MirrorMaker offsets, a copied RocksDB
directory or a reachable but unproven Oxia session. Stop the Route, retain all
objects and receipts, and create a new Route Incarnation through the disaster
authority. The new incarnation must record whether the outcome is loss,
duplication risk or an explicitly accepted operator boundary. Old receipts must
not be presented as continuous management under the new incarnation.

The disaster drill is only certified when it includes fresh processes, the
actual authority/object/provider boundary, an explicit continuity proof or
new-incarnation decision, and post-drill evidence for source gap, duplicate
risk, Owner fencing, checkpoint/object retention and GC. The current local and
bounded real-service receipts do not yet satisfy that full certification.

## 7. Receipt and certification record

Every release-candidate drill must attach:

```text
run_id / incident_id:
source lock(s):
Route Incarnation / shard / native resource identity:
Owner assignment / owner epoch before and after:
Store incarnation / checkpoint ID / manifest and object versions:
source and evidence cursors before and after:
fault cut and exact process/container/resource identity:
mutation IDs, attempt IDs, ControlRef and acknowledgements:
result codes and physical destination evidence:
SLO/evidence-gap/capacity counters:
post-run resource and image cleanup:
operator sign-off / unresolved boundary:
```

Current status is `PARTIAL`: the state-machine tests and bounded real
checkpoint/failover receipts exist, but no single release-candidate run has
executed all five sections with fresh-process disaster continuity, external
operator authorization and signed evidence. Gate 9 therefore remains
`OPEN`, and this document must not be cited as V1 release approval.
