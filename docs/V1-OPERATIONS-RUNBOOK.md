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

## 8. Current-source bounded operations drill

The clean source-locked run of `e2e/run-bounded-operations-drills.sh` produced
`/tmp/nereus-delay-operations-20260817-r2/operations-drills.json` with
`status=PASS_BOUNDED`. It passed the local restore/catalog/Owner recovery and
drain suite, Dead Letter replay and all four source-ordered UNCERTAIN branches,
then passed real Oxia + MinIO checkpoint publication and exact REAPING on
ports `31510/31511`.

The exact source locks are Delay `441a148ba4570ba0af3b6c2cfb7af3d324690954`,
K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The receipt is bounded evidence
only. It does not prove fresh-process disaster continuity, external operator
authorization, cross-record production authority or certified multi-Worker
soak, so Gate 9 remains `OPEN` and V1 remains `NOT_READY`.

The exact Compose project was
`nereus-delay-oxia-minio-checkpoint-e2e-1786938487-94600`. Post-run checks found
no related containers, networks, volumes, listeners or generated Oxia image.
The locked MinIO/Oxia bases were retained. The runner performs only exact
run-scoped cleanup and never invokes global Docker prune.

The subsequent source-locked gate artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r6/v1-release-candidate-gate.json`
at Delay `d405d2fa00bcaf99a0d34c892291ea0a425d4c47`. Source, contract and full
Gradle checks passed, but the gate records this operations receipt as blocked
because `PASS_BOUNDED` is not `PASS_CERTIFIED`; Gate 9 remains `OPEN`.

Current status is `PARTIAL`: bounded state-machine and real checkpoint/failover
receipts now exist, but no single release-candidate run has executed all five
sections with fresh-process disaster continuity, external operator
authorization and signed evidence. Gate 9 therefore remains `OPEN`, and this
document must not be cited as V1 release approval.

## 9. Current-source Pulsar Large Payload network-partition drill

Run the real two-Broker network-partition cut with an isolated port set. The
source topic name must not contain Pulsar's reserved `-partition-` pattern.

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS=75 \
PULSAR_LARGE_BROKER_1_PORT=33100 \
PULSAR_LARGE_WEB_1_PORT=33101 \
PULSAR_LARGE_BROKER_2_PORT=33102 \
PULSAR_LARGE_WEB_2_PORT=33103 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=33110 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=33111 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=33112 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-network-failover-20260817-r2 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-activation-20260817-r1 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The current-source run used Delay `fc004146b807087fcd72ee7188419eaa8f6eac06`,
P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Broker-1 stayed alive but lost its exact Compose network endpoint after
Gateway Commit/readback; after the 75-second handoff, broker-2 completed the
same source-applied physical Publish and broker-1 rejoined. Exact payload
readback passed with Admission source `5/0`, typed target `3/0`, Outcome source
`5/1`, `prepare=2/2`, `commit=2/3`, `sourceRecords=6` and
`exactGatewayIdempotency=true`; Gradle reported `BUILD SUCCESSFUL in 2m 7s`.

This is a bounded single-shard Broker network-partition receipt. It does not
close multi-shard placement, controller/storage/provider failover, certified
soak or release certification. The exact project
`nereus-delay-pulsar-large-e2e-1786939347-6325` left no containers, networks,
volumes, listeners or generated P1/Oxia images. Locked Oxia/MinIO bases were
retained; the runner used exact run-scoped cleanup and no global Docker prune.

## 10. Current-source release-gate rerun after network-partition drill

The source-locked audit artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r8/v1-release-candidate-gate.json`
at Delay `54759958b0c7af41ffa2374d835831ec7df72d13`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source, cross-repository and full
Gradle checks passed. It remains `release_status=NOT_READY`: capacity is
`PARTIAL`, certified soak is absent, and activation, operations and chaos are
`PASS_BOUNDED`, not promotable `PASS_CERTIFIED` evidence.

## 11. Current-source Pulsar two-shard Large Payload authority drill

Run this receipt with a source topic base that does not contain Pulsar's
reserved `-partition-` pattern:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
PULSAR_LARGE_BROKER_1_PORT=33400 \
PULSAR_LARGE_WEB_1_PORT=33401 \
PULSAR_LARGE_BROKER_2_PORT=33402 \
PULSAR_LARGE_WEB_2_PORT=33403 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=33410 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=33411 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=33412 \
PULSAR_LARGE_PAYLOAD_TOPIC=pulsar-large-payload-multi-20260817-r3 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-multi-20260817-r3 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The source-locked r3 receipt passed with Delay
`801e5be6a931f0dc4c5e991b79f099fdc6fd1b02`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
It passed two guarded source barriers, two Assignment/Owner pairs, one Worker
fleet, per-shard MinIO upload/attest/Commit/readback, exact Gateway Prepare
replay and final checkpoint/Owner release. The output was:

```text
partition=0 barrier=3/1 prepare=3/2 commit=3/3 objectVersion=59ecd3d5-60c0-43e7-a583-a6f78e9c7d49
partition=1 barrier=4/1 prepare=4/2 commit=4/3 objectVersion=ace5c3a2-a148-4d2a-afc8-5b5872012f9f
subscribePartitions=2 routeRevision=1 exactGatewayIdempotency=true
BUILD SUCCESSFUL in 1m 1s
```

This is an Object Store authority drill. It deliberately does not exercise
destination egress, Broker failover, MinIO fault modes or certified soak.
After every run, verify the exact Compose project resources, ports and
generated images are gone. The runner performs only run-scoped cleanup with
`docker compose down --volumes --remove-orphans --rmi local` plus exact
generated P1/Oxia image removal. Retain only the locked Oxia/MinIO bases; do
not use global `docker image prune` or `docker system prune`.

## 12. Current-source 13-cell bounded chaos rerun

Run the executable bounded matrix from the Delay worktree with the isolated
artifact directory:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-release-20260817-r2 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-release-20260817-r2/gradle-user-home \
bash e2e/run-bounded-chaos-matrix.sh
```

The resulting
`/tmp/nereus-delay-chaos-release-20260817-r2/bounded-chaos-matrix.json` is
`PASS_BOUNDED`; all 13 cells passed at Delay
`3370bfbeb03a26186156528507e379dcb1dd3021`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The source-locked gate
`/tmp/nereus-delay-v1-release-gate-20260817-r11/v1-release-candidate-gate.json`
passes source, contract and full Gradle checks but remains `NOT_READY` because
`PASS_BOUNDED` is not `PASS_CERTIFIED`, capacity is `PARTIAL` and certified
soak is absent. Every cell's Compose resources and generated images were
removed by exact run-scoped cleanup; locked bases were retained and no global
prune was used.

## 13. Current-source Pulsar multi-shard Large Payload destination drill

Use the opt-in receipt below to exercise two real source partitions and two
guarded destination physical partitions through one Worker fleet:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
PULSAR_LARGE_BROKER_1_PORT=34300 \
PULSAR_LARGE_WEB_1_PORT=34301 \
PULSAR_LARGE_BROKER_2_PORT=34302 \
PULSAR_LARGE_WEB_2_PORT=34303 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=34310 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=34311 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=34312 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=pulsar-large-payload-multi-egress-20260817-r12 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-payload-gradle-r12 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The current receipt is source-locked to Delay
`ee292f4090e23a3f26f949aa54ac075b8ed94a78`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
It passed exact barriers `3/1` and `4/1`, Prepare/Commit `3/2,3/3` and
`4/2,4/3`, sourceRecords `6` per shard, exact payload readback on both
destination partitions, two PUBLISHED outcomes and exact Gateway idempotency.

Before treating the run as complete, verify that the exact Compose project
`nereus-delay-pulsar-large-e2e-1786945120-74832` has no containers, networks,
volumes or generated P1/Oxia images. Retain the locked MinIO/Oxia bases and
unrelated pre-existing images; do not use global `docker image prune` or
`docker system prune`. This is a bounded operations receipt, not a certified
multi-shard soak, failover or release gate.

## 14. Current-source Kafka multi-shard Large Payload destination drill

Run the Kafka counterpart with a non-empty destination topic. The harness
creates matching two-partition target and receipt topics:

```bash
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=34700 \
KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=34701 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=34702 \
NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=34710 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=34711 \
NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=34712 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=kafka-large-payload-multi-egress-20260817-r3 \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-large-payload-gradle-r1 \
bash e2e/run-large-payload-gateway-e2e.sh
```

The current receipt is source-locked to Delay
`b641fc714db779787054811f7229709b1a3fa0ba`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
It passed target/receipt transactional publication and read-committed typed
evidence on both partitions, exact payload readback, sourceRecords `6` per
partition and final checkpoint/Owner release. Verify exact project
`nereus-delay-large-payload-e2e-1786946121-90342` has no containers, networks,
volumes or generated K1/Oxia images. Retain locked MinIO/Oxia bases and
unrelated images; do not use global Docker prune. This remains bounded
evidence, not certified soak or release promotion.

## 15. Current-source Gateway/Oxia session-fence chaos refresh

Run the current-source bounded matrix with an isolated artifact/cache pair:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-release-20260817-r4 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-release-20260817-r4/gradle-user-home \
bash e2e/run-bounded-chaos-matrix.sh
```

The canonical receipt
`/tmp/nereus-delay-chaos-release-20260817-r4/bounded-chaos-matrix.json` is
`PASS_BOUNDED` with all 13 cells passing under Delay
`56f39ff80ee32ff46ce7086895a3b875d7284134`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The Gateway session-fence cell
keeps Oxia stopped through stale-handle assertions, then restores it through
an explicit recovery barrier; this avoids treating Oxia's persisted session
metadata after restart as a changed marker.

The source-locked r17 gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r17/v1-release-candidate-gate.json`.
It passes source, cross-repository and full Gradle checks but remains
`NOT_READY` because bounded evidence does not satisfy `PASS_CERTIFIED`,
capacity is `PARTIAL`, and certified soak is absent. Verify exact cleanup after
the run: no run-owned containers, volumes, networks or generated images may
remain. Retain only the locked MinIO base and unrelated pre-existing images;
do not use global Docker prune or delete unrelated images.

## 16. Current-source multi-shard production-chain revalidation

The current source-locked candidate rerun used Delay
`59abbde18ad2b0b5551e4ea59c5fc146db068982`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

Kafka:

```bash
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
KAFKA_LARGE_PAYLOAD_BROKER_1_PORT=34800 \
KAFKA_LARGE_PAYLOAD_BROKER_2_PORT=34801 \
KAFKA_LARGE_PAYLOAD_BROKER_3_PORT=34802 \
NEREUS_DELAY_LARGE_PAYLOAD_OXIA_PORT=34810 \
NEREUS_DELAY_LARGE_PAYLOAD_MINIO_PORT=34811 \
NEREUS_DELAY_LARGE_PAYLOAD_GATEWAY_PORT=34812 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=kafka-large-payload-current-r18-20260817 \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-large-payload-current-r18 \
bash e2e/run-large-payload-gateway-e2e.sh
```

Pulsar used the equivalent two-Broker run on ports `34900/34901/34902/34903`,
Oxia `34910`, MinIO `34911`, Gateway `34912`, destination topic
`pulsar-large-payload-current-r18-20260817` and Gradle cache
`/tmp/nereus-delay-pulsar-large-payload-current-r18`.

Both runs passed exact destination readback on source partitions 0 and 1,
Gateway idempotency replay, typed target evidence, source-ordered Outcome,
checkpoint and Owner release. Verify exact cleanup by project label before
retaining the receipt: neither project may have containers, networks or
volumes, and the per-run Kafka/Pulsar/Oxia images must be absent. Retain only
the locked MinIO base and pre-existing unrelated images. No global Docker
prune is allowed.

This is a production-chain revalidation, not a certified soak or release
approval. A separate Gateway deployment boundary, catalog/placement churn,
controller/storage/provider failover and the `PASS_CERTIFIED` capacity,
activation, operations and chaos artifacts remain required.

## 17. Current-source release-gate receipt

After the two-shard production-chain revalidation, run-scoped source and
Gradle checks produced
`/tmp/nereus-delay-v1-release-gate-20260817-r19/v1-release-candidate-gate.json`.
The exact locks are Delay `9f8b697ce5dbbeec79c70f237fa909172d2fccb3`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`; source, contract and full Gradle
checks are `PASS`.

The release decision remains `NOT_READY`. Capacity is `PARTIAL`, certified
soak is absent, and activation/cutover, operations and chaos are blocked by
the requirement for independently source-locked `PASS_CERTIFIED` artifacts.
`ALLOW_NOT_READY=1` only preserves this audit receipt and is not a promotion
override.

## 18. Current-source bounded capacity refresh

The current Delay source `0f04415e3c8abcf17952ae3f5c5e4796bb797831` produced
`/tmp/nereus-delay-capacity-matrix-current-20260817-r9/capacity-benchmark-matrix.json`
with `status=PARTIAL` / `matrix_status=PASS_BOUNDED`. The three locked Linux
cases passed Store/SLO readback and reopen. The exact pinned JDK image was
pulled for this run and removed afterward; no run container or image remained.

This receipt is not an operator capacity certification. It does not cover
Broker throughput, Lane/Worker placement, large-scale records,
compaction/restore, inline/object flow, adapter/zombie bounds or soak. The
r20 release gate therefore remains `NOT_READY` and the bounded result cannot
be promoted with `ALLOW_NOT_READY`.

## 19. Current bounded fault, activation and cleanup receipt

Run the current bounded fault matrix with a fresh artifact directory and an
isolated Gradle cache. The canonical run must remain sequential because the
individual cells share the Delay build output directory:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-current-20260817-r7 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-gradle-current-20260817-r7 \
bash e2e/run-bounded-chaos-matrix.sh
```

The resulting
`/tmp/nereus-delay-chaos-current-20260817-r7/bounded-chaos-matrix.json` is
`PASS_BOUNDED` with all 13 Kafka, Pulsar, checkpoint and Gateway cells at
status `0`. It is locked to Delay `1dd68005e18d3a7422a2fae653750372a5841421`,
K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The activation smoke is
`/tmp/nereus-delay-protocol-activation-current-20260817-r2/protocol-activation-cutover.json`
(`PASS_BOUNDED`), and the operations drill is
`/tmp/nereus-delay-operations-current-20260817-r4/operations-drills.json`
(`PASS_BOUNDED`). The latter's real-service probe used Compose project
`nereus-delay-oxia-minio-checkpoint-e2e-1786949637-42236` and retained the
locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

### Docker cleanup policy and final postcheck

Every run must remove only resources identified by its exact Compose project,
container labels and generated image tags. The interrupted Pulsar attempt was
cleaned explicitly by removing image
`nereus-delay-pulsar-p1:nereus-delay-pulsar-multi-e2e-1786950538-53376`,
container
`4afc75a5ddc037ec77841f4ab0e90009abaf374bf941ffe66a858a1bb20c1fa3` and
volume `nereus-delay-pulsar-multi-e2e-1786950538-53376_zk-data`.

After cleanup, verify that no `nereus-delay-*` run container, network, volume
or generated image remains. Retain the locked MinIO base and unrelated images;
do not run a global Docker prune. The final postcheck for this evidence set
found only the unrelated `wenjunxiao/mac-docker-connector` running container,
with no generated `nereus-delay-*` image left.

The source-locked gate
`/tmp/nereus-delay-v1-release-gate-20260817-r22/v1-release-candidate-gate.json`
passed source, contract and full Gradle checks but returned `NOT_READY` by
design. Capacity was `PARTIAL`, certified soak was absent, and bounded
activation, operations and chaos receipts were not eligible for
`PASS_CERTIFIED`. `ALLOW_NOT_READY=1` records this audit result only; it does
not authorize promotion.

## 20. Real MinIO provider fault drill

Run the Object Store fault drill with an empty artifact directory, isolated
ports and a populated isolated Gradle cache:

```bash
NEREUS_DELAY_MINIO_FAULT_ARTIFACT_DIR=/tmp/nereus-delay-minio-fault-current-20260817-r3 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-minio-fault-current-20260817-r1 \
NEREUS_DELAY_MINIO_FAULT_MINIO_PORT=31651 \
NEREUS_DELAY_MINIO_FAULT_PROXY_PORT=31652 \
NEREUS_DELAY_MINIO_BUCKET=nereus-delay-fault-current-r3 \
bash e2e/run-minio-fault-e2e.sh
```

The canonical receipt is
`/tmp/nereus-delay-minio-fault-current-20260817-r3/minio-fault-e2e.json`.
It is source-locked to Delay `b982f423e0f6f3d7627e6f0fabfbed1e36c85498`, uses
the locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
and records all four provider fault tests as PASS. The runner removes its
exact container in an EXIT trap and retains the locked base image; it does not
use global Docker prune.

The receipt is a real Object Store fault slice, not `PASS_CERTIFIED` capacity,
soak or release evidence. Preserve the fail-closed boundary for provider
errors before Commit and the exact-readback boundary after Commit.

## 22. Current-source real Oxia multi-node Gateway leader-failover drill

Run from the clean full-v1 checkout with an isolated artifact directory,
Gradle cache and ports:

```bash
NEREUS_DELAY_OXIA_MULTI_NODE_GATEWAY_ARTIFACT_DIR=/tmp/nereus-delay-oxia-multi-node-gateway-current-20260817-r2 \
NEREUS_DELAY_GATEWAY_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-multi-node-gateway-gradle-current-20260817-r1 \
NEREUS_DELAY_OXIA_COORDINATOR_1_PORT=35191 \
NEREUS_DELAY_OXIA_COORDINATOR_2_PORT=35192 \
NEREUS_DELAY_OXIA_COORDINATOR_3_PORT=35193 \
NEREUS_DELAY_OXIA_DATA_SERVER_1_PORT=35181 \
NEREUS_DELAY_OXIA_DATA_SERVER_2_PORT=35182 \
NEREUS_DELAY_OXIA_DATA_SERVER_3_PORT=35183 \
NEREUS_DELAY_GATEWAY_PORT=35158 \
bash e2e/run-oxia-multi-node-gateway-e2e.sh
```

The canonical receipt is
`/tmp/nereus-delay-oxia-multi-node-gateway-current-20260817-r2/oxia-multi-node-gateway-e2e.json`.
It records `ds-2 -> ds-1` leader succession, the exact Compose project and
six pre-cleanup generated image IDs, and reports `status=PASS` with
`docker_cleanup.status=PASS`. The source locks are Delay
`53c9fc0c7b1609ba37109536326dad330d994ebb`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The cleanup policy is exact: the runner removes only this Compose project,
its labeled resources and its generated Oxia images with `--rmi local`; the
locked Oxia/MinIO bases and unrelated images are retained. An empty project
container/network/volume/image postcheck is required. This drill proves one
namespace-shard DataServer leader stop and Gateway durable-outcome reread; it
does not prove Gateway HA, coordinator/storage failover, placement churn,
disaster continuity or `PASS_CERTIFIED` release readiness.

## 23. Current-source r25 release-gate audit

The post-receipt gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r25/v1-release-candidate-gate.json`.
Source cleanliness, cross-repository contract validation and full Gradle
`check` passed for the exact four-repository locks recorded in that artifact.
The decision is intentionally `release_status=NOT_READY`: `capacity=PARTIAL`,
certified soak is missing, and activation, operations and chaos are bounded
receipts. Do not use `ALLOW_NOT_READY=1` as a promotion switch. The gate
artifact predates this runbook append, so the append does not change its
source-qualified status.

## 24. Current-source bounded Large Payload production-chain soak

Run the four real production-chain cases strictly sequentially from the clean
full-v1 checkout. Use an empty artifact directory, an isolated Gradle cache and
an unused contiguous port range:

```bash
NEREUS_DELAY_PRODUCTION_SOAK_ARTIFACT_DIR=/tmp/nereus-delay-production-chain-soak-current-20260817-r2 \
NEREUS_DELAY_PRODUCTION_SOAK_GRADLE_USER_HOME=/tmp/nereus-delay-production-chain-soak-gradle-20260817-r1 \
NEREUS_DELAY_PRODUCTION_SOAK_CYCLES=1 \
NEREUS_DELAY_PRODUCTION_SOAK_BASE_PORT=35100 \
bash e2e/run-bounded-production-chain-soak.sh
```

The canonical receipt is
`/tmp/nereus-delay-production-chain-soak-current-20260817-r2/production-chain-soak.json`.
The run completed four cases with exit code 0 and `status=PASS_BOUNDED`:
Kafka multi-shard destination, Pulsar multi-shard destination, Kafka MinIO
timeout-after-Commit, and Pulsar MinIO 503-after-Commit. Check each case's
`receipt_markers` for the real Gateway/Oxia/Broker/Worker/MinIO evidence and
each `docker_cleanup.status` for exact cleanup.

The cleanup contract is narrow: remove only the case's exact Compose project,
labeled containers/networks/volumes and generated provider image tags. The
locked MinIO base remains retained at digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
After the run, verify there are no matching
`nereus-delay-large-payload`/`nereus-delay-pulsar-large` resources or images.
Do not use `docker system prune`, `docker image prune`, broad globs or remove
unrelated images. The bounded receipt is not a release certification; the
fresh-process chaos, capacity, certified soak, upgrade/downgrade and disaster
continuity gates remain separate.

## 25. Current-source r28 release-gate result

The current fail-closed release artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r28/v1-release-candidate-gate.json`.
It records source, cross-repository and full Gradle checks as `PASS` for the
four locked repositories, while `release_status=NOT_READY` because no
`PASS_CERTIFIED` capacity, soak, activation, operations or chaos inputs were
provided. Do not promote the bounded production-chain receipt into any of
those slots. The artifact predates this runbook append, which does not refresh
its source lock.

## 26. Current-source r29 release-gate result

The final gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r29/v1-release-candidate-gate.json`.
It reports clean source locks, passing cross-repository validation and a
passing full Gradle check for the four repositories. The decision remains
`release_status=NOT_READY`: no `PASS_CERTIFIED` capacity, soak, activation,
operations or chaos artifacts were supplied. The r29 artifact predates this
runbook append, so the append does not refresh its source lock.

## 27. Current-source certified production-chain harness integration

Run this wrapper only with an explicit policy. The following receipt is a
harness-integration profile, not an approved V1 release profile:

```bash
NEREUS_DELAY_CERTIFIED_SOAK_PROFILE_ID=harness-integration-production-chain-r1 \
NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_CYCLES=1 \
NEREUS_DELAY_CERTIFIED_SOAK_CYCLES=1 \
NEREUS_DELAY_CERTIFIED_SOAK_REQUIRED_DURATION_SECONDS=240 \
NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_RSS_KIB=16777216 \
NEREUS_DELAY_CERTIFIED_SOAK_MAX_PROCESS_FDS=262144 \
NEREUS_DELAY_CERTIFIED_SOAK_MAX_ARTIFACT_BYTES=8589934592 \
NEREUS_DELAY_CERTIFIED_SOAK_RESOURCE_SAMPLE_INTERVAL_SECONDS=5 \
NEREUS_DELAY_CERTIFIED_SOAK_MAX_SAMPLE_GAP_SECONDS=15 \
NEREUS_DELAY_CERTIFIED_SOAK_BASE_PORT=40100 \
NEREUS_DELAY_CERTIFIED_SOAK_ARTIFACT_DIR=/tmp/nereus-delay-certified-soak-harness-20260817-r5 \
NEREUS_DELAY_CERTIFIED_SOAK_GRADLE_USER_HOME=/tmp/nereus-delay-certified-soak-gradle-harness-r1 \
bash e2e/run-certified-production-chain-soak.sh
```

The canonical artifact is
`/tmp/nereus-delay-certified-soak-harness-20260817-r5/certified-production-chain-soak.json`.
It reports `PASS_CERTIFIED` for the named profile, locks Delay
`8f6fddd4c3e626a90bbe73be1360398c78114065`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, and covers all four serial
Kafka/Pulsar destination and MinIO post-Commit cases. Child runtime was 269
seconds; 36 samples were collected with an 8-second maximum gap, peak RSS was
`1003392 KiB`, peak FD count `1151`, and all exact cleanup/postcheck fields
were empty/pass.

The runner removes only matching Compose projects, labeled resources and
generated provider images. The locked MinIO base and canonical Oxia image are
retained for subsequent real-service runs. Do not run `docker system prune`,
`docker image prune`, broad image globs or unrelated-image deletion. The
release gate still requires an explicitly supplied approved profile id and
independent capacity, full chaos, activation, operations, upgrade/downgrade
and disaster receipts; this one-cycle harness profile does not close those
gates or the §23.5 longest-cycle requirement.

## 28. Current-source r30 release-gate result

The current fail-closed artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r30/v1-release-candidate-gate.json`.
It records source, cross-repository and full Gradle checks as `PASS` for
Delay `b9a7fa9994542b9bc9630d7b12c63ade2fc1c57b`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The release decision remains `NOT_READY`: benchmark capacity, certified
soak, activation/cutover, operations and chaos remain blocked without their
separate approved `PASS_CERTIFIED` artifacts. The harness-integration
receipt is not a release promotion input. This runbook append does not
refresh the r30 source lock.

## 30. Current-source r32 release-gate result

The current fail-closed artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r32/v1-release-candidate-gate.json`.
It records source, cross-repository and full Gradle checks as `PASS` for
Delay `5d282244524de0d002cc7122ebf389150a4fd9f2`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The decision remains `NOT_READY`: no approved `PASS_CERTIFIED` artifacts
were supplied for capacity, soak, activation/cutover, operations or chaos.
The bounded chaos run is a recovery receipt, not release authorization.

## 29. Current-source bounded chaos refresh

Run receipt:
`/tmp/nereus-delay-chaos-current-20260817-r3/bounded-chaos-matrix.json`.
The 13 sequential focused cells all returned zero under Delay
`8cfa6acc97a7a966e76b0ce086572c53cd731f7d`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The runner found no matching run containers, networks, volumes or generated
images after cleanup. Retain only the locked Oxia/MinIO bases. Treat this as
`PASS_BOUNDED`: it is useful operational recovery evidence, but it does not
authorize V1 promotion. The full §23.3 fault matrix and its durable state
dump/invariant requirements remain open; the release gate remains
`release_status=NOT_READY`.
