# Nereus Delay Operations Runbook

Status: `DRAFT / NOT CERTIFIED` (2026-08-17)

This runbook is the operator-facing boundary for restore, fencing, Dead Letter
replay, uncertain publish resolution and disaster continuity. It is not an
authorization to edit RocksDB, Oxia records, Broker offsets or Object Store
objects directly. Every state-changing action must be a source-ordered,
authenticated mutation with an exact identity and an auditable receipt.

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
  --tests com.nereusstream.delay.store.CheckpointRestoreCoordinatorTest \
  --tests com.nereusstream.delay.store.RecoveryCatalogTest \
  --tests com.nereusstream.delay.store.ShardStoreTest
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
  --tests com.nereusstream.delay.ownership.OwnerLeaseTest \
  --tests com.nereusstream.delay.ownership.OwnerRecoveryCoordinatorTest
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
3. Submit one authenticated `REPLAY_DEAD_LETTER` operation with a new
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
  --tests 'com.nereusstream.delay.runtime.DelayShardTest.replayDeadLetterCreatesNextGenerationAndRetainsOldTerminalSummary' \
  --tests 'com.nereusstream.delay.runtime.DelayShardTest.terminalSummaryRetainsASecondOpenObligationAndReopensSafely'
```

## 5. Uncertain publish resolution

An `UNCERTAIN` state means the destination side effect is not proven absent or
present. It is not permission to retry blindly.

1. Identify the exact open attempt ledger, generation, Owner epoch, channel
   binding and evidence cursor. Hold the Lane and retain physical/zombie
   charge.
2. Prefer verified published or not-published evidence. Apply one
   source-ordered `RESOLVE_UNCERTAIN` operation with an exact ControlRef and
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
  --tests 'com.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainPublishedEvidenceSettlesExactObligation' \
  --tests 'com.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainNotPublishedEvidenceNormalizesDefinitiveRetry' \
  --tests 'com.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainRetryMaterializesControlOverrideTimeline' \
  --tests 'com.nereusstream.delay.runtime.DelayShardTest.sourceOrderedResolveUncertainTerminalizesPossibleDeliveryAndRetainsObligation'
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
soak, so Gate 9 remains `OPEN` and remains `NOT_READY`.

The exact Compose project was
`nereus-delay-oxia-minio-checkpoint-e2e-1786938487-94600`. Post-run checks found
no related containers, networks, volumes, listeners or generated Oxia image.
The locked MinIO/Oxia bases were retained. The runner performs only exact
run-scoped cleanup and never invokes global Docker prune.

The subsequent source-locked gate artifact is
`/tmp/nereus-delay-release-gate-20260817-r6/release-candidate-gate.json`
at Delay `d405d2fa00bcaf99a0d34c892291ea0a425d4c47`. Source, contract and full
Gradle checks passed, but the gate records this operations receipt as blocked
because `PASS_BOUNDED` is not `PASS_CERTIFIED`; Gate 9 remains `OPEN`.

Current status is `PARTIAL`: bounded state-machine and real checkpoint/failover
receipts now exist, but no single release-candidate run has executed all five
sections with fresh-process disaster continuity, external operator
authorization and signed evidence. Gate 9 therefore remains `OPEN`, and this
document must not be cited as release approval.

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
`/tmp/nereus-delay-release-gate-20260817-r8/release-candidate-gate.json`
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
`/tmp/nereus-delay-release-gate-20260817-r11/release-candidate-gate.json`
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
`/tmp/nereus-delay-release-gate-20260817-r17/release-candidate-gate.json`.
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
`/tmp/nereus-delay-release-gate-20260817-r19/release-candidate-gate.json`.
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
`/tmp/nereus-delay-release-gate-20260817-r22/release-candidate-gate.json`
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

Run from the clean full checkout with an isolated artifact directory,
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
`/tmp/nereus-delay-release-gate-20260817-r25/release-candidate-gate.json`.
Source cleanliness, cross-repository contract validation and full Gradle
`check` passed for the exact four-repository locks recorded in that artifact.
The decision is intentionally `release_status=NOT_READY`: `capacity=PARTIAL`,
certified soak is missing, and activation, operations and chaos are bounded
receipts. Do not use `ALLOW_NOT_READY=1` as a promotion switch. The gate
artifact predates this runbook append, so the append does not change its
source-qualified status.

## 24. Current-source bounded Large Payload production-chain soak

Run the four real production-chain cases strictly sequentially from the clean
full checkout. Use an empty artifact directory, an isolated Gradle cache and
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
`/tmp/nereus-delay-release-gate-20260817-r28/release-candidate-gate.json`.
It records source, cross-repository and full Gradle checks as `PASS` for the
four locked repositories, while `release_status=NOT_READY` because no
`PASS_CERTIFIED` capacity, soak, activation, operations or chaos inputs were
provided. Do not promote the bounded production-chain receipt into any of
those slots. The artifact predates this runbook append, which does not refresh
its source lock.

## 26. Current-source r29 release-gate result

The final gate is
`/tmp/nereus-delay-release-gate-20260817-r29/release-candidate-gate.json`.
It reports clean source locks, passing cross-repository validation and a
passing full Gradle check for the four repositories. The decision remains
`release_status=NOT_READY`: no `PASS_CERTIFIED` capacity, soak, activation,
operations or chaos artifacts were supplied. The r29 artifact predates this
runbook append, so the append does not refresh its source lock.

## 27. Current-source certified production-chain harness integration

Run this wrapper only with an explicit policy. The following receipt is a
harness-integration profile, not an approved release profile:

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
`/tmp/nereus-delay-release-gate-20260817-r30/release-candidate-gate.json`.
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
`/tmp/nereus-delay-release-gate-20260817-r32/release-candidate-gate.json`.
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
authorize promotion. The full §23.3 fault matrix and its durable state
dump/invariant requirements remain open; the release gate remains
`release_status=NOT_READY`.

## 31. Current-source audited 13-cell bounded chaos r5

Run the matrix with an isolated artifact/cache pair:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-current-20260817-r5 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-gradle-current-20260817-r5 \
bash e2e/run-bounded-chaos-matrix.sh
```

The canonical receipt is
`/tmp/nereus-delay-chaos-current-20260817-r5/bounded-chaos-matrix.json`.
It is `PASS_BOUNDED`, with all 13 cells at status `0` and audit/marker status
`PASS`, under Delay `75b347da58a4086d19df912ca82f974401432f44`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The audit covers declared deterministic injection points, expected states,
duplicate boundaries and marker-checked source/target/authority evidence. Six
crash/network cells report fresh-process recovery `PASS`; the remaining seven
response-loss/checkpoint/session cells remain `NOT_COVERED`. Durable state
dumps are `NOT_CAPTURED` and invariant audits are `MARKER_ONLY`, so the matrix
is operational recovery evidence only and does not authorize promotion.

Exact postchecks are empty for matching containers, networks, volumes and
generated images. The only retained related images are the canonical Oxia
image and locked MinIO base. Do not use global Docker prune; the release gate
remains `release_status=NOT_READY`.

## 32. Certified bounded-capacity harness integration

Run the bounded capacity wrapper only with an explicit, reviewable profile and
an empty artifact directory. The following is a harness-integration profile,
not a release approval:

```bash
NEREUS_DELAY_CERTIFIED_CAPACITY_PROFILE_ID=harness-integration-bounded-capacity-r1 \
NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_CASE_COUNT=3 \
NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_PAYLOAD_RECORDS_TOTAL=3888 \
NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_SLO_SAMPLES_TOTAL=664 \
NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_CGROUP_MEMORY_BYTES=2147483648 \
NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_DIRECT_MEMORY_BYTES=268435456 \
NEREUS_DELAY_CERTIFIED_CAPACITY_REQUIRED_MAX_OPEN_FILES=65536 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_PROCESS_RSS_BYTES=1073741824 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_CURRENT_OPEN_FILES=4096 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_LOCAL_BYTES=268435456 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_WAL_BYTES=134217728 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_STORE_SST_BYTES=134217728 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_SLO_OUTBOX_BYTES=8388608 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_CASE_SLO_COLLECTOR_BYTES=8388608 \
NEREUS_DELAY_CERTIFIED_CAPACITY_MAX_ARTIFACT_BYTES=1073741824 \
NEREUS_DELAY_CERTIFIED_CAPACITY_PULL_IMAGE=1 \
NEREUS_DELAY_CERTIFIED_CAPACITY_ARTIFACT_DIR=/tmp/nereus-delay-certified-capacity-harness-20260817-r3 \
NEREUS_DELAY_CERTIFIED_CAPACITY_GRADLE_USER_HOME=/tmp/nereus-delay-certified-capacity-gradle-20260817-r3 \
NEREUS_DELAY_CERTIFIED_CAPACITY_PROJECT=nereus-delay-certified-capacity-20260817-r3 \
bash e2e/run-certified-capacity-benchmark.sh
```

The canonical receipt is
`/tmp/nereus-delay-certified-capacity-harness-20260817-r3/certified-capacity-benchmark.json`.
It must contain `PASS_CERTIFIED` only for the named profile, three child cases
with Store/SLO reopen evidence, passing resource policy and an empty exact
Docker postcheck. The wrapper removes the pinned JDK image only when this run
pulled it; it retains pre-existing base images and never runs a global prune.

This receipt is bounded Store/SLO harness evidence. It does not authorize
capacity or release promotion: Broker throughput, Lane fairness and placement,
Control Reserve, Adapter/zombie bounds, restore/inline/object capacity,
long-cycle soak and the other certification gates remain open. The release
gate requires `NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CAPACITY_PROFILE_ID` and
the strict capacity schema before this receipt can satisfy only the capacity
slot.

## 2026-08-17 Bounded-chaos r6 and exact Docker cleanup

Run the current-source bounded matrix with an isolated artifact directory:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/tmp/nereus-delay-chaos-current-20260817-r6 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/tmp/nereus-delay-chaos-gradle-current-20260817-r6 \
bash e2e/run-bounded-chaos-matrix.sh
```

The canonical receipt is
`/tmp/nereus-delay-chaos-current-20260817-r6/bounded-chaos-matrix.json`.
The run is `PASS_BOUNDED` with all 13 cells returning zero and locks Delay
`396220a7ecc01fbd317dcd96ce3155365b9280a9`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The Pulsar Worker Admission response-loss cell now includes a forced durable
pre-SIGKILL dump and a post-fresh-process dump. It proves the same durable
`PUBLISHING` attempt reaches `PUBLISHED` without a second Admission or
physical destination publish. This is cell-specific recovery evidence; the
other cells retain their declared marker-only or not-captured boundaries.

After the run, verify the exact project-scoped cleanup. There must be no
matching `nereus-delay-(kafka|pulsar|oxia|minio|gateway)` containers, generated
images, networks or volumes. Retain only the canonical Oxia and locked MinIO
base images needed by later runs. Do not run a global Docker prune or delete
unrelated images. This cleanup is evidence hygiene and does not change the
matrix's `PASS_BOUNDED` or the release gate's `NOT_READY` status.

## 2026-08-17 Current HEAD gate and temporary-directory cleanup

The current gate command was run with an isolated artifact directory and
`NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1` so that the fail-closed receipt
could be retained:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/tmp/nereus-delay-release-gate-20260817-r37 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/tmp/nereus-delay-release-gate-gradle-20260817-r37 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
bash e2e/run-release-gate.sh
```

Receipt:
`/tmp/nereus-delay-release-gate-20260817-r37/release-candidate-gate.json`.
Source, cross-repo and Gradle checks passed. Capacity, certified soak,
activation/cutover, operations and release-certified chaos were blocked
because no approved artifact/profile was supplied; therefore the only valid
release result is `release_status=NOT_READY`.

After all runs completed, `/private/tmp` was cleaned by exact top-level name
matching. 476 stale `nereus-delay*` entries were removed; the five canonical
receipt directories (Admission r8, chaos r6, certified capacity r3, certified
soak r5 and gate r37) were retained. Before deletion there were no matching
active processes, `.git` directories or source trees under the temporary
targets. Source worktrees and unrelated files were not touched.

## 2026-08-17 Gateway large-payload multi-shard authority runs

Run the current Kafka chain with an explicit destination topic:

```bash
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1 \
NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-large-payload-destination-current-20260817 \
NEREUS_DELAY_LARGE_PAYLOAD_GRADLE_USER_HOME=/private/tmp/nereus-delay-large-payload-kafka-gradle-current-20260817 \
bash e2e/run-large-payload-gateway-e2e.sh
```

Run the current Pulsar chain with two source partitions:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/private/tmp/nereus-delay-pulsar-large-payload-gateway-gradle-current-20260817 \
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The successful logs are retained at
`/tmp/nereus-delay-large-payload-gateway-current-20260817-r1/`. The runs
proved the complete functional chain through real Kafka/Pulsar, Oxia, Gateway
mTLS/JWT, Worker, MinIO and checkpoint, with two source barriers and two
destination `PUBLISHED` outcomes per protocol. The runners removed their
Compose containers, networks, volumes and generated images. The Gradle homes
are run caches only and must be removed after evidence capture; they are not
source or release artifacts.

The multi-shard baseline does not authorize release promotion. Broker
failover/chaos, capacity, approved soak, activation/cutover, operations,
upgrade/downgrade and disaster continuity remain separate gates.

## 2026-08-17 Gate r38 current HEAD receipt

The latest fail-closed gate receipt is
`/tmp/nereus-delay-release-gate-20260817-r38/release-candidate-gate.json`.
It records current source/cross-repository/full Gradle `PASS` and
`release_status=NOT_READY`; capacity, soak, activation, operations and
release-certified chaos remain blocked without their explicitly approved
artifacts. Remove the corresponding r38 Gradle user home after the run; keep
only the receipt.

## 2026-08-17 Current-source bounded-chaos r7

Run the 14-cell current-source matrix with isolated evidence and build-cache
directories:

```bash
NEREUS_DELAY_CHAOS_MATRIX_ARTIFACT_DIR=/private/tmp/nereus-delay-chaos-current-20260817-r7 \
NEREUS_DELAY_CHAOS_MATRIX_GRADLE_USER_HOME=/private/tmp/nereus-delay-chaos-gradle-current-20260817-r7 \
bash e2e/run-bounded-chaos-matrix.sh
```

The canonical receipt is
`/private/tmp/nereus-delay-chaos-current-20260817-r7/bounded-chaos-matrix.json`.
It is source-locked to Delay `9a6f171ab817607ff59d18a4e963ae0a8504e281`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`; the receipt reports
`PASS_BOUNDED` and 14 zero exit codes.

The two cell-specific durable recovery audits are
`pulsar-worker-admission-response-loss` and
`pulsar-worker-destination-response-loss`. Each retains before/after state
dumps, proves durable `PUBLISHING` to fresh-process `PUBLISHED` recovery and
reports `CAPTURED_AND_VERIFIED` / `INDEPENDENT_FIELDS_PASS`. The destination
cell additionally proves exact destination payload readback with no second
SEND after replaying the durable `PUBLISH_OUTCOME`. Do not infer this audit
level for the other 12 cells; they remain marker-only and/or not captured.

After the run, verify exact run-scoped Docker cleanup. There must be no
matching generated containers, networks, volumes or images. Retain the r7
receipt and both state-dump directories; remove only the r7 Gradle cache after
evidence capture. Never run a global Docker prune or remove unrelated images.
The bounded result remains `release_status=NOT_READY` at the gate.

## 2026-08-17 Current HEAD gate r39

Run the fail-closed gate with isolated receipt and Gradle-home paths:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/private/tmp/nereus-delay-release-gate-20260817-r39 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/private/tmp/nereus-delay-release-gate-gradle-20260817-r39 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
bash e2e/run-release-gate.sh
```

Receipt:
`/private/tmp/nereus-delay-release-gate-20260817-r39/release-candidate-gate.json`.
The four source checks, cross-repository validator and full Gradle check are
`PASS`. The result is still `release_status=NOT_READY` because certified
capacity, soak, activation/cutover, operations drills and release-certified
chaos were not supplied as approved `PASS_CERTIFIED` artifacts. Remove the
exact r39 Gradle home after the run; retain the receipt and logs. Do not
promote the bounded r7 receipt into a certified slot.

## 2026-08-20 RC1 evidence and cleanup handoff

Use the following exact source-locked receipts for the current RC1 candidate:

```text
/private/tmp/nereus-delay-rc1-capacity-20260820-r2/certified-capacity-benchmark.json
/private/tmp/nereus-delay-rc1-soak-20260820-r1/certified-production-chain-soak.json
/private/tmp/nereus-delay-rc1-activation-20260820-r1/protocol-activation-cutover.json
/private/tmp/nereus-delay-rc1-operations-20260820-r1/operations-drills.json
/private/tmp/nereus-delay-rc1-chaos-20260820-r2/bounded-chaos-matrix.json
/private/tmp/nereus-delay-rc1-gate-20260820-r2/release-candidate-gate.json
```

The four exact locks are carried by the `source_locks` fields in the listed
receipts and final gate. The capacity and named soak profiles are
`PASS_CERTIFIED` within their declared bounded policies. The
activation, operations and 14-cell chaos receipts are `PASS_BOUNDED`; the
release gate correctly reports `NOT_READY` and must not be overridden by an
operator flag.

The r2 chaos matrix is the canonical retry after a superseded r1 ACK-crash
startup failure. A focused real Kafka/Oxia reproduction and the complete r2
matrix both passed the ACK cut gate, fresh Worker replay, real Oxia authority,
dedupe, ACK and final checkpoint. Preserve the r1 failure only if a diagnostic
history is needed; it is not a release input.

Cleanup policy for this campaign:

- Verify no related process is alive before cleanup.
- Verify exact Compose postchecks are empty for containers, projects,
  networks, volumes and generated images.
- Retain only the canonical receipt directories listed above and the locked
  Oxia/MinIO base images needed for future evidence.
- Move disposable Gradle homes, superseded retry artifacts and focused
  diagnostic caches to Trash using exact paths. Do not use `docker system
  prune`, broad globs, or recursive deletion against a source/worktree root.

The RC1 gate is a release handoff, not a release approval: activation/cutover,
operations authorization and release-certified chaos still require the
separate `PASS_CERTIFIED` evidence defined by the gate.

## 2026-08-20 Kafka source durable-chaos and certification boundary

The current Delay source adds prepare/resume JVM modes to the Kafka Fetch
response-loss and retention-floor drills. Each mode writes a forced
`nereus-delay-chaos-durable-state-dump` record. The bounded audit checks
same topic/topic identity, Route/partition, source offset or retention floor,
LSO/commit result, `durable_broker_read`, `dump_forced` and distinct JVM PIDs.
The diagnostic output was
`/private/tmp/nereus-delay-kafka-durable-20260820-r1/`; it is not a release
input because it predates the source-lock commit.

Use `e2e/run-certified-chaos-matrix.sh` only with an explicit approved profile.
It fails closed unless all fourteen cells independently carry durable dumps,
fresh-process recovery and `INDEPENDENT_FIELDS_PASS`, with exact current
source locks and empty generated Docker resources. The current implementation
has four cells at that level, so certification remains `BLOCKED`; do not
promote the bounded matrix into the release gate.

## 2026-08-21 RC1 source-lock refresh and cleanup boundary

This section is the current evidence handoff. Earlier receipt sections are
frozen historical records and must not be used as the current release inputs.
After the documentation commit containing this section, regenerate the
receipts below without another source change. Each receipt's
`source_locks.delay` is the authoritative Delay SHA; the other locks remain K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The current canonical receipt paths are:

```text
/private/tmp/nereus-delay-rc1-capacity-20260821-r4/certified-capacity-benchmark.json
/private/tmp/nereus-delay-rc1-soak-20260821-r7/certified-production-chain-soak.json
/private/tmp/nereus-delay-rc1-activation-20260821-r5/protocol-activation-cutover.json
/private/tmp/nereus-delay-rc1-operations-20260821-r4/operations-drills.json
/private/tmp/nereus-delay-certified-chaos-20260821-r5/certified-chaos-matrix.json
/private/tmp/nereus-delay-rc1-release-gate-20260821-r4/release-candidate-gate.json
```

The expected source-locked results are `PASS_CERTIFIED` for capacity, the
four-case production-chain soak, protocol activation/cutover and operations
drills. The certified chaos receipt remains `BLOCKED`: all 14 bounded cells
must exit zero and therefore report `PASS_BOUNDED`, but only the independently
audited durable/fresh-process/invariant cells may enter the certified slot.
The current matrix has four such cells out of fourteen. The release gate must
therefore remain `release_status=NOT_READY`; no promotion or release
approval may be inferred from the bounded result.

The final gate command is:

```bash
NEREUS_DELAY_RELEASE_GATE_ARTIFACT_DIR=/private/tmp/nereus-delay-rc1-release-gate-20260821-r4 \
NEREUS_DELAY_RELEASE_GATE_GRADLE_USER_HOME=/private/tmp/nereus-delay-rc1-soak-20260820-r3/gradle-user-home/kafka \
NEREUS_DELAY_RELEASE_GATE_RUN_CHECK=1 \
NEREUS_DELAY_RELEASE_GATE_ALLOW_NOT_READY=1 \
NEREUS_DELAY_RELEASE_GATE_CAPACITY_ARTIFACT=/private/tmp/nereus-delay-rc1-capacity-20260821-r4/certified-capacity-benchmark.json \
NEREUS_DELAY_RELEASE_GATE_SOAK_ARTIFACT=/private/tmp/nereus-delay-rc1-soak-20260821-r7/certified-production-chain-soak.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CAPACITY_PROFILE_ID=nereus-delay-rc1-bounded-capacity-r1 \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_SOAK_PROFILE_ID=nereus-delay-rc1-production-chain-soak-r1 \
NEREUS_DELAY_RELEASE_GATE_ACTIVATION_ARTIFACT=/private/tmp/nereus-delay-rc1-activation-20260821-r5/protocol-activation-cutover.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_ACTIVATION_PROFILE_ID=nereus-delay-rc1-activation-r1 \
NEREUS_DELAY_RELEASE_GATE_OPERATIONS_ARTIFACT=/private/tmp/nereus-delay-rc1-operations-20260821-r4/operations-drills.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_OPERATIONS_PROFILE_ID=nereus-delay-rc1-operations-r1 \
NEREUS_DELAY_RELEASE_GATE_CHAOS_ARTIFACT=/private/tmp/nereus-delay-certified-chaos-20260821-r5/certified-chaos-matrix.json \
NEREUS_DELAY_RELEASE_GATE_CERTIFIED_CHAOS_PROFILE_ID=nereus-delay-rc1-chaos-r1 \
bash e2e/run-release-gate.sh
```

The full Gradle check, four source checks and cross-repository contract
validator must all pass before this gate result is accepted. After every run,
verify that the exact Compose postchecks are empty for containers, networks,
volumes and generated images. Retain only the canonical receipts above and
the locked Oxia base image `nereus/oxia-o1:37a17bef1720` and MinIO base image
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`; move disposable Gradle
caches, superseded receipts and diagnostics to Trash by exact path. Never use
a global Docker prune or remove unrelated images/worktrees.

## 2026-08-21 current-source chaos slice and post-documentation receipt

The current implementation slice is Delay commit
`71068209dff3915e17ac2d81324154d79074e6f5`, following the durable Kafka Worker
ACK recovery slice `9a55403e1f493fad8db73956db9dcd50c4429964`. The ACK cell now
proves durable Store reuse, a fresh JVM, real Oxia Owner Lease continuity and
the Kafka ACK transition. The test uses a local Recovery Catalog/Floor
authority seam, so it must not be described as full production Recovery
Catalog authority.

The current pre-documentation chaos receipt is
`/private/tmp/nereus-delay-certified-chaos-20260821-r9/certified-chaos-matrix.json`.
It is source-locked to Delay `71068209dff3915e17ac2d81324154d79074e6f5`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Its bounded child is
`PASS_BOUNDED` (14/14), Docker postcheck is `PASS`, but the certified wrapper
is `BLOCKED` because durable/fresh-process/invariant proof is `FAIL` and only
5/14 cells have durable dumps. The r8 network-partition timeout remains a
recorded timing-sensitive failure; r9 bounded PASS does not certify stability.

The corresponding pre-documentation release gate is
`/private/tmp/nereus-delay-rc1-release-gate-20260821-r5/release-candidate-gate.json`.
It has source, cross-repository and full Gradle checks `PASS`, while the final
status is `release_status=NOT_READY` because the older named receipts do not
match the current source lock and chaos is not `PASS_CERTIFIED`.

Once this section is committed, regenerate the post-documentation receipts at
`/private/tmp/nereus-delay-certified-chaos-20260821-r10/certified-chaos-matrix.json`
and
`/private/tmp/nereus-delay-rc1-release-gate-20260821-r6/release-candidate-gate.json`.
Only those receipts' exact `source_locks` may be used as the current handoff.
Until all fourteen cells independently satisfy durable state, fresh-process
recovery and invariant comparison, keep the certified chaos result blocked and
the release gate `NOT_READY`. Cleanup remains exact-path and recoverable via
Trash; never delete a source/worktree root or use global Docker prune.

## 2026-08-21 Pulsar Worker process-crash evidence slice

Delay commit `83a47900ef3de4cfa110f7ca43d13fcde1376628` adds the certified
durable-state path for the existing Pulsar Worker process-crash cell. Its
focused before/after dumps are
`/private/tmp/nereus-delay-pulsar-worker-process-crash-20260821-r1/`;
the real-broker E2E preserved Store/DB identity across fresh JVM recovery and
closed source apply/ACK under the real Oxia Owner Lease. The r10/r6 receipts
are pre-slice history and must not be used as current source-locked inputs.
After this section is committed, r11 chaos and r7 gate receipts become the
current handoff targets. Certified chaos and the gate remain fail-closed
until all fourteen cells satisfy the durable/fresh/invariant contract.

## 2026-08-21 current-source Large Payload production-chain receipt

The strict-sequential bounded production-chain soak completed before this
documentation change at
`/private/tmp/nereus-delay-production-chain-soak-20260821-r1/production-chain-soak.json`.
It is `nereus-delay-bounded-production-chain-soak`,
`status=PASS_BOUNDED`, one cycle, four expected cases and four `PASS` cases.
The source locks are Delay `d5dfa990c22f7659ebdb68f84e800646f34e7d46`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

It covers the real Gateway + Oxia + Broker + Worker + MinIO chain for Kafka
and Pulsar two-shard destination egress, plus Kafka
`PUT_TIMEOUT_AFTER_COMMIT` and Pulsar `PUT_503_AFTER_COMMIT` uncertainty.
All four cases reached source apply/ACK and exact destination payload readback;
the multi-shard cases also recorded exact MinIO object versions and the fault
cases recorded exact Gateway idempotency. Each case's Compose postcheck is
`PASS` with no run-scoped containers, networks, volumes or generated provider
images left behind. The locked MinIO base image remains retained.

This is bounded functional production-authority evidence. It does not certify
the release: the fourteen-cell fresh-process chaos union, certified capacity
and aged-uncertainty soak, activation/cutover, operations, upgrade and disaster
continuity gates remain separate and fail-closed. Since this section changes
the Delay source, r1 is pre-documentation evidence. Re-run the current-source
receipt after committing this section:

```bash
NEREUS_DELAY_PRODUCTION_SOAK_ARTIFACT_DIR=/private/tmp/nereus-delay-production-chain-soak-20260821-r2 \
NEREUS_DELAY_PRODUCTION_SOAK_GRADLE_USER_HOME=/private/tmp/nereus-delay-production-chain-gradle-20260821-r2 \
NEREUS_DELAY_PRODUCTION_SOAK_CYCLES=1 \
NEREUS_DELAY_PRODUCTION_SOAK_BASE_PORT=36100 \
bash e2e/run-bounded-production-chain-soak.sh
```

Use only r2's exact `source_locks` for the post-documentation handoff. Keep
the r1 receipt as historical provenance and move only superseded disposable
diagnostics/caches to Trash by exact path; never remove source worktrees or
use a global Docker prune.

## 2026-08-21 checkpoint-reaping fresh-process drill

Commit `6e163de1` adds the missing process boundary to the real checkpoint
REAPING drill. WRITE creates the Oxia-backed `PENDING_UPLOAD` intent, uploads
the exact MinIO versioned prefix, closes provider ownership, explicitly
abandons the session-bound Owner and fsync-forces the pre-process dump. A
separate READ JVM reconnects to Oxia, verifies no current Owner remains, CASes
the intent to `REAPING` and sweeps the exact prefix through the real MinIO
adapter.

Focused evidence:

```text
/private/tmp/nereus-delay-checkpoint-reaping-fresh-20260821-r1/before-process-crash.json
/private/tmp/nereus-delay-checkpoint-reaping-fresh-20260821-r1/after-fresh-process.json
```

The receipt passed with PIDs `35845 -> 35997`, identical intent/Route/
lineage/checkpoint/store identities, Owner absence after reconnect, and exact
version counts `2 listed / 2 deleted / empty prefix`. This is a focused PASS
for the checkpoint cell, not a 14-cell certified-chaos or release PASS.

## 2026-08-21 Pulsar destination response-loss fresh-process drill

Commit `b42135d4` makes the direct Pulsar destination response-loss drill
cross-process. WRITE performs one guarded SEND against the real P1 broker,
discards the post-commit response and force-dumps the request/evidence. READ
reopens the topic in a separate JVM without a Producer, validates the exact
`PULSAR_SEND_ACK` fields and reads back exactly one payload.

The focused dumps are:

```text
/private/tmp/nereus-delay-pulsar-destination-response-loss-fresh-20260821-r1/before-process-crash.json
/private/tmp/nereus-delay-pulsar-destination-response-loss-fresh-20260821-r1/after-fresh-process.json
```

PIDs were `42487 -> 42581`; the exact broker position was `9/0/0`, and the
after dump records one physical send, exact payload readback and zero duplicate
payloads. This is the direct destination adapter cell, not the separate
Worker destination response-loss process-crash drill. The full chaos and
release gates remain fail-closed.

## 2026-08-21 Kafka Broker process-crash recovery drill

The Kafka Broker process-crash runner now has a durable rejoin handoff in
Delay commit `75a008fc`. The operational sequence is: prepare one guarded
Worker record; fsync a real Admin metadata dump; SIGKILL `kafka-1`; wait for
survivor leader convergence; resume the Worker through Brokers 2 and 3;
restart Broker 1; wait for its port and ISR rejoin; then fsync a second dump
from a fresh Admin JVM. The Worker portion must retain real Oxia authority,
source apply/ACK, typed `KAFKA_TRANSACTIONAL_RECEIPT`, exact destination
readback and final checkpoint markers.

The focused evidence is:

```text
/private/tmp/nereus-delay-kafka-broker-process-crash-20260821-r3/state/before-process-crash.json
/private/tmp/nereus-delay-kafka-broker-process-crash-20260821-r3/state/after-fresh-process.json
```

It recorded topic/cluster identity unchanged, replicas and ISR `[1,2,3]`
before and after, survivor leader `3`, end offset `1 -> 5`,
`broker_1_rejoined=true` and distinct Admin JVM PIDs `51328 -> 51612`. The
first attempt also exposed why the survivor leader convergence step is
mandatory: a Worker can otherwise observe transient `UNKNOWN_LEADER_EPOCH`
immediately after the crashed Broker's port disappears. Treat the convergence
smoke as a required operational barrier, not as a sleep-based workaround.

This is a focused cell PASS, not a release approval. Regenerate the current
source-locked chaos wrapper and release gate after the implementation and
documentation commits. Cleanup must verify that run-scoped Kafka/Oxia
containers, networks, volumes and generated images are absent while retaining
only locked base images; do not prune unrelated Docker resources or delete
source/worktree paths.

## 2026-08-21 Durable chaos state dump type contract

The checkpoint REAPING and direct Pulsar destination response-loss drills now
emit boolean state fields as JSON booleans. Delay commit `33b546f6` also makes
the small fresh-process reader tolerate scalar state values, so the shell
audit and the JVM handoff validate the same durable-state schema.

The current focused receipts are:

```text
/private/tmp/nereus-delay-checkpoint-reaping-20260821-r13-state/before-process-crash.json
/private/tmp/nereus-delay-checkpoint-reaping-20260821-r13-state/after-fresh-process.json
/private/tmp/nereus-delay-pulsar-destination-response-loss-20260821-r13-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-destination-response-loss-20260821-r13-state/after-fresh-process.json
```

The first pair completes the real Oxia + MinIO REAPING sweep with exact
version deletion and empty prefix. The Pulsar pair performs one real guarded
SEND, discards the committed response, then uses a fresh JVM to revalidate
typed `PULSAR_SEND_ACK` and read one exact payload without a second SEND. The
complete chaos wrapper and gate still require a new current-source run.

## 2026-08-21 Certified chaos and release-gate handoff

The post-fix r13 run completed all fourteen child scenarios with exit code
`0`. Nine cells have durable/fresh-process/invariant evidence; the five
remaining cells are deliberately still marker-only or not covered. Use these
receipts for the handoff:

```text
/private/tmp/nereus-delay-certified-chaos-20260821-r13/certified-chaos-matrix.json
/private/tmp/nereus-delay-rc1-release-gate-20260821-r10/release-candidate-gate.json
```

The certified wrapper is `BLOCKED`, although its bounded child matrix is
`PASS_BOUNDED` and its Docker postcheck is clean. Gate r10 is
`release_status=NOT_READY`; source, cross-repository and full Gradle checks
passed, but exact source-lock validation blocks the older certified capacity,
soak, activation and operations receipts, and the chaos artifact itself is not
`PASS_CERTIFIED`. Do not promote the bounded matrix or partial nine-cell union.

## 2026-08-21 Kafka Broker TCP-cut and network-partition evidence

Delay commit `ae10068e` adds the same durable-state handoff used by the Kafka
Broker process-crash cell to the two remaining Kafka Broker cuts. For the TCP
cell, prepare the guarded Worker record, place the source and group-coordinator
partitions on Broker-2, force the before dump, cut the one-shot raw endpoint
proxy, let a fresh Worker resume through the bootstrap list, and force the
after dump. For the network cell, force the before dump, disconnect only
`kafka-1` from the exact Compose network, wait for survivor leader convergence,
resume the Worker through Brokers 2/3, reconnect `kafka-1`, and force the after
dump from a new Admin JVM.

Focused receipts:

```text
/private/tmp/nereus-delay-kafka-broker-tcp-cut-20260821-r1-state/before-process-crash.json
/private/tmp/nereus-delay-kafka-broker-tcp-cut-20260821-r1-state/after-fresh-process.json
/private/tmp/nereus-delay-kafka-broker-network-partition-20260821-r1-state/before-process-crash.json
/private/tmp/nereus-delay-kafka-broker-network-partition-20260821-r1-state/after-fresh-process.json
```

The source locks are Delay `ae10068e`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Both focused cells passed the
real Kafka/Oxia Worker chain and independent audit: end offset `1 -> 2`,
unchanged cluster/topic identity, replicas/ISR/live `[1,2,3]`, forced durable
reads and distinct JVM PIDs. These two cells therefore join the bounded
durable union, but the complete current-source matrix and certified gate have
not yet been rerun. The remaining durable evidence slices are Pulsar
multi-Broker failover, Pulsar source-ACK response loss and Gateway/Oxia
session churn. Cleanup remains exact-path and run-scoped; do not prune locked
base images or touch source worktrees.

## 2026-08-21 Pulsar multi-Broker process-crash runbook receipt

The focused r4 run for `pulsar-multi-broker-process-crash` passed with real
two-Broker Pulsar, ZooKeeper/BookKeeper, Oxia and Worker services. It killed
Broker-1 after guarded preparation, read durable state from the surviving
Broker-2, resumed the Worker through the survivor path, and then restarted
Broker-1 until rejoin was observed.

Receipt files:

```text
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r4-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r4-state/after-process-crash.json
```

The independent audit requires and passed: common schema and topic/physical
topic/cluster, ledger IDs `[-1,2]`, entries and confirmed position monotonic,
distinct Admin endpoints `31741 -> 31743`, distinct collector JVM PIDs
`12676 -> 12738`, `internalStats?metadata=true`, and explicit
`durable_broker_read=true` plus `dump_forced=true`. Source locks are Delay
`a48cd33a00ecd566d149ddb300efa22cf670747a`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

Operational boundary: capture the survivor Admin dump after Broker-2 is ready
but before fresh Worker resume. Once the source consumer is closed, Pulsar
Admin `internalStats`/`internal-info` may return 404/500 in this setup, so a
post-Worker Admin query is not part of this receipt. The focused Docker
cleanup left no scoped resources and retained locked base images. This slice
advances the bounded durable union to 12/14; rerun the complete source-locked
chaos wrapper before using it as release input.

## 2026-08-21 Pulsar source-ACK and Gateway/Oxia churn runbook receipt

Delay commit `63b72ee9944995a88b0cfe4505ede2051e4392f` supplies the final two
focused durable-state receipts for the bounded fault set. Run the Pulsar cell
with real Pulsar and Oxia, `NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS=1`,
`NEREUS_DELAY_PULSAR_SOURCE_ACK_RESPONSE_LOSS_PROCESS_CRASH_ONLY=1` and an
exact state-dump directory. The Worker forces the Store/WAL boundary, waits at
the Broker-accepted/response-lost cut, the runner SIGKILLs the recorded JVM,
and a fresh Worker resumes from the same Store root.

```text
/private/tmp/nereus-delay-pulsar-source-ack-response-loss-20260821-r3-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-source-ack-response-loss-20260821-r3-state/after-fresh-process.json
```

The r3 receipt preserved the same physical topic, Route/Store identity,
`store_incarnation=87e9c9ecfb3a42499970b75927cfb661` and DB identity across
Worker PIDs `31393 -> 31470`. Before SIGKILL, ACK source position equaled the
applied position and the dump recorded durable source apply, Broker ACK
acceptance and local response loss. After fresh recovery, the old ACK position
was retained, no source entry was replayed, no duplicate apply was observed,
and vertical smoke, Oxia lease and final checkpoint passed.

Run the Gateway cell with
`NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN=1` and
`NEREUS_DELAY_GATEWAY_OXIA_SESSION_CHURN_STATE_DUMP_DIR` pointing at an exact
artifact path:

```text
/private/tmp/nereus-delay-gateway-oxia-session-churn-20260821-r1-state/before-oxia-restart.json
/private/tmp/nereus-delay-gateway-oxia-session-churn-20260821-r1-state/after-oxia-restart.json
```

The real mTLS Gateway/Oxia run preserved one admission record, one QUIESCENT
idempotency record with one attempt and aggregate outcome, two audit records,
zero active leases and one prepare/submit pair. The response digest was
identical before and after the Oxia process restart; stale sessions failed
closed and fresh sessions reread the exact outcome. This is a session/process
churn receipt, not full Gateway HA/provider failover.

These two focused cells bring the independently audited durable union to
14/14. They do not make the historical matrix or release gate current. Rerun
the full strict-sequential wrapper at Delay
`63b72ee9944995a88b0cfe4505ede2051e4392f`, then regenerate the fail-closed
release gate. Keep the four canonical JSON files, move only confirmed failed
diagnostic directories to recoverable Trash, remove exact run-scoped Docker
resources, retain locked Oxia/MinIO base images, and never target source
worktrees or the pre-existing unlabelled `pulsarconf`/`pulsardata` volumes.

## 2026-08-21 Current-source 14-cell bounded chaos r15

After the Kafka process-crash marker audit was corrected in Delay
`d14d9a6a7e55d77bd1a3a42ea3f2e30291896b61`, the complete matrix was run with
an isolated artifact and Gradle cache. The canonical receipt is
`/private/tmp/nereus-delay-chaos-current-20260821-r15/bounded-chaos-matrix.json`.
It reports `PASS_BOUNDED`, fourteen zero child exits, and PASS for every
cell's marker, durable-state, fresh-process and independent-field audits. Its
locks are Delay `d14d9a6a7e55d77bd1a3a42ea3f2e30291896b61`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

This receipt is the current bounded fault baseline, not release authorization.
Before promotion, regenerate the certified chaos wrapper and the release gate
after this documentation commit, keep exact Docker postchecks empty, retain
only canonical evidence and locked base images, and move only confirmed
obsolete diagnostic directories to recoverable Trash. Never clean source
worktrees, Git metadata or the pre-existing unlabelled `pulsarconf` and
`pulsardata` volumes.

## 2026-08-21 Pulsar failover/admission recovery runbook receipts

The focused recovery fixes are Delay `16c3792e`, `2f57b5f8` and `b7b156e6`.
The multi-Broker process-crash receipt is:

```text
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r7-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r7-state/after-fresh-process.json
```

The runner now requires three consecutive survivor readiness probes before
capturing post-failover Admin state. Treat a new managed-ledger ID as valid
only when every pre-failure ID is retained, the post set is non-empty, and the
same topic/cluster/confirmed position and durable entry boundary are present.
The r7 receipt changed `[-1,3]` to `[-1,3,4]` and passed this retained-or-
extended invariant.

The Worker admission response-loss process-crash receipt is:

```text
/private/tmp/nereus-delay-pulsar-worker-admission-response-loss-20260821-r1-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-worker-admission-response-loss-20260821-r1-state/after-fresh-process.json
```

The before state is forced durable `PUBLISHING`; after SIGKILL and a fresh
Worker process, the same attempt is `PUBLISHED` with durable read flags and
the exact typed destination evidence. The independent audit is
`INDEPENDENT_FIELDS_PASS`. Keep both JSON pairs as canonical focused
receipts; do not delete them during run-scoped cleanup.

These focused receipts do not authorize release. After the final source and
documentation commit, rerun the complete strict-sequential matrix, then the
certified wrapper and fail-closed release gate. Remove only exact obsolete
diagnostic directories to recoverable Trash, keep source worktrees and Git
metadata untouched, and retain the locked Oxia/MinIO images.

## 2026-08-21 Current-source bounded chaos r17 and TCP-cut follow-up

The post-documentation strict-sequential bounded matrix is recorded at:

```text
/private/tmp/nereus-delay-chaos-current-20260821-r17/bounded-chaos-matrix.json
```

With Delay `257161a203090fdf5657acdea896d6b8b5777040`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, all fourteen cells returned zero
and passed marker, durable-state, fresh-process and independent-invariant
audits. The artifact is `PASS_BOUNDED`, not `PASS_CERTIFIED`.

r16 is retained as a diagnostic for a transient Kafka TCP-cut producer timeout
with no after dump. Before using that cell as a focused handoff, the same
source was rerun at:

```text
/private/tmp/nereus-delay-kafka-broker-tcp-cut-20260821-r2-state/
```

The focused r2 receipt passed the fresh process/PID, same topic/cluster/topic
ID, monotonic end offset, Broker-1 rejoin and
`INDEPENDENT_FIELDS_PASS` checks. Certified chaos and the release gate must
still be run against the final source lock. Cleanup remains exact-path and
recoverable: retain canonical JSON receipts and locked Oxia/MinIO base images;
move only confirmed disposable diagnostics to Trash; never use global Docker
prune or target source worktrees and the unlabelled `pulsarconf`/`pulsardata`
volumes.

## 2026-08-21 Candidate/evidence lock operating procedure

Keep r19 and r20 as historical, source-bound receipts at Delay
cec7641b96a57d3108723c8cb27eb51594846543:

~~~text
/private/tmp/nereus-delay-certified-chaos-20260821-r19/certified-chaos-matrix.json
/private/tmp/nereus-delay-release-gate-20260821-r20/release-candidate-gate.json
~~~

The final runbook sequence is: freeze the candidate four-repository source
lock; execute the ten independent gate inputs; make at most one documentation-
only evidence overlay; generate the external evidence manifest; and run the
final release gate against that manifest. The overlay may touch only:

~~~text
docs/IMPLEMENTATION-STATUS.md
docs/Nereus Delay 设计.md
docs/DESIGN-AUDIT.md
docs/DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md
docs/OPERATIONS-RUNBOOK.md
e2e/README.md
~~~

e2e/verify-evidence-manifest.sh checks clean branches, candidate-to-
overlay ancestry, the exact six-path diff, post-overlay document SHA-256,
artifact SHA-256/status and all four-repository source locks. A missing
manifest, changed ledger, non-allowlisted source edit or stale artifact is a
hard block. The manifest is outside the repository to avoid self-hashing; no
post-manifest documentation edit is permitted.

The stable runbook command is intentionally full-scope:

~~~bash
NEREUS_DELAY_RELEASE_GATE_CANDIDATE_SOURCE_LOCK=/private/tmp/<candidate>/source-lock.json \
  bash e2e/run-release-gate.sh
~~~

The ten artifact variables are `PROTOCOL_GOLDEN`, `CHAOS_FULL`,
`REAL_SERVICE`, `NO_EARLY`, `BENCHMARK`, `CAPACITY_FULL`, `SOAK_FULL`,
`UPGRADE_DOWNGRADE`, `OPERATIONS_FULL` and `PATCH_DISTRIBUTION`. The validator
emits `nereus-delay-release-gate`; absent or old bounded receipts are
reported as `NOT_READY` and are never promoted.

## Current-source protocol-golden execution

The first current-source full- gate receipt is:

```text
/private/tmp/nereus-delay-protocol-golden-run-20260821-f.1N9Xji/protocol-golden.json
sha256=e144407304580231c879ff3ed9f4c84951f85f537bcda2f06a9f101b1f375365
```

It locks Delay `dc37d2c2093eb46d3bf85f2bd964d5055a086194`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Delay returned 392 tests, Kafka
17 guarded tests, and Pulsar 6 common plus 2 broker guarded tests; each exit
code was zero and no failure/error/skip was observed. The receipt is
`PASS_CERTIFIED` only for `protocol-golden`; do not pass it as the complete
release gate while the other nine artifacts are absent.

## Current-source no-early execution

The source-locked no-early run is:

```text
/private/tmp/nereus-delay-no-early-20260821-a.bOg67w/no-early.json
sha256=91692a7301b5e4fc99605ef6698c0c9208a12ea1379f7123d9db928ae7138d37
```

It uses Delay `f82e914d22c5b7d84f618e0ca31fa378a27bf3a2` with the fixed K1/P1/Oxia
locks, and passed 34 focused tests with zero failures/errors/skips. The
artifact records `max_early_ms=0`, a 20 ms trusted worker uncertainty bound and
a 20 ms Pulsar target clock-ahead bound. It is `PASS_CERTIFIED` only for the
no-early gate; it cannot substitute for the remaining full- artifacts.

## Current-source Large Payload authority receipts

The current source `2f38677f491bd0b9071269dc27937ec691827c49` has completed the
real same-adapter Large Payload run for K1 and P1. The Kafka log is
`/private/tmp/nereus-delay-real-service-kafka-20260821-c.4owPvQ/run.log`,
SHA-256 `358271def7aeb50bc503c8a09f4eda430fbd7e4db8850f6775ba6d22de60f4d8`;
the Pulsar log is
`/private/tmp/nereus-delay-real-service-pulsar-20260821-a.WCUeKp/run.log`,
SHA-256 `84bf7f5171c0124463dd5efe40ca061ef7cea7bbc240bce14a569d77877c8d11`.
Both runs used real Oxia, locked MinIO, Gateway mTLS/JWT, real Broker
evidence, Worker apply/ACK, two destination `PUBLISHED` outcomes and exact
post-run Compose cleanup.

Operationally these are retained as named same-adapter receipts, not as the
full `real-service` gate input. The cross-adapter cells, activation cutover and
the remaining full- fault/capacity/soak/operations/patch receipts must be
present before the release command can return PASS.

## 2026-08-21 current-source cross-adapter Large Payload runbook receipt

The exact current-source cross-adapter run is retained at:

```text
/private/tmp/nereus-delay-cross-20260821-r29/
```

Source locks are Delay `6b5c357c207169f98ec78be7f7007e2ebf3c1209`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Kafka client SHA-256 is
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, Pulsar
distribution SHA-256 is
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and the
MinIO image is locked to
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

`K_TO_P.log` SHA-256 is
`02db290caafda6d4cc814f2e2397726c50dcd91a2a3f1e0d9f2b27cfcdd76f40`;
`P_TO_K.log` SHA-256 is
`44ffccb5e043f59ed15e60de6696e324359bd7d738a2276bbb816a259dee3608`.
Both directions passed real Broker ingress, Gateway mTLS/JWT, Oxia placement
and ownership, MinIO upload/attest/Commit/readback, Worker due→Claim→Admission,
target publish, source Outcome and exact idempotency. K→P resolved with
`PULSAR_SEND_ACK`; P→K resolved with `KAFKA_TRANSACTIONAL_RECEIPT`. The runner
returned `CROSS_ADAPTER_LARGE_PAYLOAD_GATEWAY_E2E=PASS_CERTIFIED` and removed
only its exact scoped runtime resources.

This receipt closes the two cross-adapter Large Payload cells, but it is not a
complete release input. The release runbook still requires independently
source-locked activation/cutover, full 19-cell chaos, capacity, soak,
upgrade/downgrade, operations/disaster-continuity and patch-distribution
artifacts.

## 2026-08-21 current-source closure audit and runbook boundary

The frozen candidate source lock is Delay
`e44a23ccd76e9976c49427ebf46240fda8410abd`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The current runbook receipts are:

```text
protocol-golden  PASS_CERTIFIED
/private/tmp/nereus-delay-protocol-golden-current-20260821-r2/protocol-golden.json
sha256=362f54f6cec0d6041be3be07f1b8ba6188322980f00fa853a4eae2fb4791d90c

no-early         PASS_CERTIFIED
/private/tmp/nereus-delay-no-early-current-20260821-r2/no-early.json
sha256=d424f5017a110ff884355b4d7f28c5367a2855d2562eac97606efce6054d1a3a

real-service     PASS_CERTIFIED
/private/tmp/nereus-delay-real-service-candidate-20260821/real-service-r6/real-service.json
sha256=db0297371961dbc8d3791a80f24940eaa07ca27da5938e6aa4fb547097e779c0
```

`real-service` is now the current full Gateway + real Oxia + real Kafka/Pulsar
Broker + Worker + MinIO Large Payload authority chain, including both
cross-adapter directions and activation cutover. Worker egress is complete;
the remaining work is release evidence, fault coverage and packaging, not a
new egress abstraction. Standalone activation is also certified at
`/private/tmp/nereus-delay-activation-current-20260821-r2/`.

The current capacity, soak and operations runs are deliberately retained as
bounded profiles. They pass their bounded policies, but their schemas and
boundaries do not satisfy the complete §23 capacity/soak/operations inputs.
The current 19-cell chaos receipt at
`/private/tmp/nereus-delay-full-chaos-20260821-r44/full-chaos-matrix.json`
has 11 passing cells and 8 blockers: credential-binding-drift, long-GC,
half-open, ENOSPC, fsync-error, SST corruption, broker-leader-failover and
disaster-host-fault.

The strict audit command produced
`/private/tmp/nereus-delay-release-gate-current-20260821-r6/release-candidate-gate.json`
with SHA-256
`bd64e1897210f834b6160223221c3b65360b74c7861fa6b37c874b0f202fd597` and
`release_status=NOT_READY`. It passed source locks, cross-repo contracts,
full Gradle check, protocol-golden, real-service and no-early. Benchmark,
chaos, complete capacity/soak, upgrade/downgrade, complete operations and
patch-distribution remain fail-closed; do not use bounded receipts to pass the
full release command.

## 2026-08-21 full gate runner procedure

For Delay-owned full contract evidence, use a fresh empty artifact
directory and an explicit four-repository lock:

\`\`\`bash
NEREUS_DELAY__FULL_GATE_ARTIFACT_DIR=/private/tmp/<run>/upgrade \\
NEREUS_DELAY__FULL_GATE_CANDIDATE_SOURCE_LOCK=/private/tmp/<run>/source-lock.json \\
NEREUS_DELAY__FULL_GATE_PROFILE_ID=nereus-delay-upgrade-r1 \\
bash e2e/run-full-contract-gate.sh upgrade-downgrade
\`\`\`

The runner checks branch, cleanliness and exact HEAD before running the fresh
test matrix. A successful local upgrade/downgrade receipt proves only its
declared full cells; capacity and benchmark must additionally use the
physical Broker/Lane envelope producer, while soak and operations must pass
their real-service child. Do not use \`NEREUS_DELAY__FULL_GATE_RUN_REAL=0\`
to turn those missing external authorities into a PASS.

For the guarded-client distribution gate, use the same candidate lock and an
empty artifact directory:

\`\`\`bash
NEREUS_DELAY__PATCH_ARTIFACT_DIR=/private/tmp/<run>/patch \\
NEREUS_DELAY__PATCH_CANDIDATE_SOURCE_LOCK=/private/tmp/<run>/source-lock.json \\
NEREUS_DELAY__PATCH_RUN_CLUSTER=1 \\
bash e2e/run-full-patch-distribution-gate.sh
\`\`\`

The runner retains the K1/P1 test logs and binary digest table. A source or
binary-only check is not a partial rollout; the multi-Broker child must run
and clean its exact Compose resources before the patch-distribution input can
pass.

## 2026-08-21 full benchmark/capacity run

Run the physical envelope producer with an empty artifact directory and a
candidate lock. `RUN_REAL=1` runs the functional multi-shard children;
`MEASUREMENT_ARTIFACT` must point to a separately collected, source-matching
physical observation file for a PASS:

```bash
NEREUS_DELAY__CAPACITY_ARTIFACT_DIR=/private/tmp/<run>/capacity \
NEREUS_DELAY__CAPACITY_CANDIDATE_SOURCE_LOCK=/private/tmp/<run>/source-lock.json \
NEREUS_DELAY__CAPACITY_GRADLE_USER_HOME=/private/tmp/<run>/gradle-user-home \
NEREUS_DELAY__CAPACITY_RUN_REAL=1 \
NEREUS_DELAY__CAPACITY_MEASUREMENT_ARTIFACT=/private/tmp/<run>/capacity-observation.json \
bash e2e/run-full-capacity-envelope-gate.sh capacity
```

The current base-source probe is retained at
`/private/tmp/nereus-delay-full-capacity-real-current-20260821-r3/` for
Delay `9ab82d11c0b1b8bd60547d94ea695403d2c73b1c`. Local contracts and both
real children passed, but the full input is intentionally `FAIL` because the
physical measurement artifact was not supplied. Related Docker postchecks
were empty; do not pass this functional result into the release gate as
benchmark or capacity evidence.

## 2026-08-22 current-source guarded patch-distribution certification

The gate fix is Delay `1631f8c1821116e8c7b3ef3f7166bab06c4b8a76`: K1, P1 and
Delay tests now use `--rerun-tasks`, so an up-to-date Gradle result cannot be
mistaken for fresh execution. The canonical artifact is
`/private/tmp/nereus-delay-patch-distribution-current-20260822-r3/full-gate-input.json`
with SHA-256
`c92104c707d208035aff782a3def37d84c409830bd2214bc543381e5eeab2ebb`.

It locks Kafka K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The artifact is
`PASS_CERTIFIED`, exclusion-free and source-lock exact: Kafka guarded producer
cases passed, Pulsar guarded common/broker tests and Delay guarded transport
tests were freshly executed, and the real two-Broker Pulsar partial-rollout
child passed broker stop/recovery, physical publish, ACK and checkpoint
release. Binary digests are in
`/private/tmp/nereus-delay-patch-distribution-current-20260822-r3/binary-digests.tsv`.

This certifies only the patch-distribution input, not the release. Capacity
measurement, complete chaos, soak, upgrade/downgrade, operations/disaster and
the remaining release inputs stay fail-closed. Exact scoped Docker postchecks
were empty; base images were retained.

## 2026-08-22 current-source release audit boundary

The final audit for candidate lock Delay
`1631f8c1821116e8c7b3ef3f7166bab06c4b8a76`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` is retained at
`/private/tmp/nereus-delay-release-gate-current-20260822-r1/release-candidate-gate.json`
with SHA-256
`6436c4279cf3be7e579cbd0bae5c48fa6a1684e857bc711691dca015cba0b3d0`.
The documentation-only overlay is Delay `03e285c7d2d99c1389cf6d8d73338a9e8f8205c0`.

Source checks, cross-repository contracts and the full Gradle `check` passed;
the patch-distribution input is also exact-source `PASS`. The nine other full
 inputs were absent and therefore `BLOCKED` by the validator, leaving the
strict release result `NOT_READY`. The Gradle run still skips opt-in external
Oxia/MinIO/chaos methods when their endpoints are unset; this audit does not
promote those skips or any bounded receipt into release PASS.

## 2026-08-22 current-source Large Payload and operations evidence refresh

The current documentation-overlay source is Delay `336f6586a7013938356eea6bd3093225a646d7b1`, with Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The source-locked physical-capacity runner produced
`/private/tmp/nereus-delay-full-capacity-current-20260822-r1/full-gate-input.json`
(SHA-256 `1e69acee2181ba87ec0d03bea9cc8689ed40951eda1db1ff5bb8ddd4361cba0d`).
Its Delay contract tests and both real children passed. Kafka's two-shard
Gateway mTLS/JWT -> Oxia Assignment/Owner -> Worker -> real MinIO -> destination
`PUBLISHED` receipt is recorded in
`kafka-large-payload-multi-shard.log` (SHA-256
`7e1a8ac79733a8b86e28ea1683787a01863ae3d5dfc757b0a449f9ade47311ad`); Pulsar's
corresponding two-guarded-partition/two-Worker chain is recorded in
`pulsar-large-payload-multi-shard.log` (SHA-256
`54617e4489106e56e183a771244af5bb8401a4df914dca66c8ced8a79c9ffdc8`).
The full capacity artifact remains `FAIL` with `measurement_status=MISSING`:
functional Large Payload E2E is now current-source evidence, but it is not a
Broker/Lane/resource capacity envelope.

The current-source operations retry is retained at
`/private/tmp/nereus-delay-operations-current-20260822-r2/full-gate-input.json`
(SHA-256 `69cc717120703bba10fdf0650f2187298a68b87fb52d9e6c0e32d99d4247af2a`).
Its bounded child is `PASS_BOUNDED` (SHA-256
`bca3dcdfb55fcb871396ca0af484a30a32ca389795086027398ea277d0acac59`): local
state-machine, real Oxia/MinIO checkpoint recovery, separate fresh-process
recovery and exact Docker cleanup all passed. The certified operations wrapper
remains `BLOCKED` only because the independent multi-Worker soak artifact is
missing; no operations or disaster-continuity release PASS is claimed.

For the candidate source lock itself, the upgrade/downgrade full artifact was
rerun in an isolated candidate clone at
`/private/tmp/nereus-delay-upgrade-downgrade-candidate-20260822-r1/full-gate-input.json`
(SHA-256 `023460f978fcc6a74c752419521e86e0869eb087fbb08aa4419e8af2547778a1`).
It is `PASS_CERTIFIED`, exclusion-free and covers all six required cells. The
capacity, soak, operations, chaos and remaining release obligations stay
fail-closed. Exact related Docker postchecks were empty; retained base images
were not globally pruned.

## 2026-08-22 release audit after candidate upgrade refresh

The strict audit artifact is
`/private/tmp/nereus-delay-release-gate-current-20260822-r2/release-candidate-gate.json`
with SHA-256 `6a3f7ff024933555613fd93c682d41d9b56b00c711e8d531947e086aac13c375`.
It used candidate Delay `1631f8c1821116e8c7b3ef3f7166bab06c4b8a76`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`; the audit-time documentation
overlay was Delay `ea3a76e24b7c7aa5e4bb20a3be50e0b101d13172`.

Source checks, cross-repository contracts and full Gradle `check` passed. The
candidate-source upgrade/downgrade and patch-distribution artifacts passed
exactly. Capacity and operations were present but rejected because they were
not complete `PASS_CERTIFIED` full inputs (`measurement_status=MISSING` and
missing independent soak, respectively); protocol-golden, chaos, real-service,
no-early, benchmark and soak had no complete full artifact. The resulting
release status is therefore `NOT_READY`; no complete ten-gate manifest exists.

## 2026-08-22 current operational evidence and cleanup boundary

Current locks are Delay `a40588bec6d363a4cfd2a4b7d3df5695649a0d79`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Operations r16 is a complete
`PASS_CERTIFIED` full receipt for restore, fence, DLQ, uncertain override
and disaster recovery. Soak r15 independently passed 3 cycles / 12 cases,
800 seconds and exact Docker cleanup, but release audit r17 keeps its wrapper
blocked until `policy.longest_configured_period_seconds` is emitted.

Large Payload receipt r3 passed both adapter directions with exact payload and
idempotency. Capacity r10 and benchmark r11 remain blocked by absent physical
Broker/Lane measurements. The fail-closed audit
`/private/tmp/nereus-delay-full-gates-20260822-r20/release/release-candidate-gate.json`
is `NOT_READY`.

Cleanup is recoverable and exact: only unused evidence/cache directories are
eligible for Trash after reference checks; no worktree or source checkout is
deleted. Docker cleanup uses exact generated project resources only. The locked
MinIO base and canonical Oxia image remain available; no global prune is used.

## 2026-08-22 current full- release handoff

The exact source lock for this handoff is:

```text
Delay  c448e52607c8ff8bf3206c443fed35137a0c4cdc
Kafka  05849884ca81fad767fda058444d1e17c7f9cbf9
Pulsar 0a2536484cd3932801a98dc88ff112b2df88a1c7
Oxia   37a17bef17202d5fd6e23282da5fd26d94865484
```

The strict ten-gate receipt is
`/private/tmp/nereus-delay-release-gate-20260822-r1/release-candidate-gate.json`
(`release_status=PASS`, SHA-256
`e25fcec81e766afb6d9ba8c2e68149439bd25ced902ab3b260d346be11e563e9`). It
passed source cleanliness/branch checks, the cross-repository validator and
full Gradle `check`, then validated exact-source `PASS_CERTIFIED` inputs for
all ten gates.

The soak profile used 3 configured cycles / 12 cases, a required duration of
600 seconds and `longest_configured_period_seconds=600`; this is recorded as
the run profile input. The operations command must receive that independently
certified soak artifact. Commit `c448e526` makes the runner pass it explicitly,
so omitting it remains a fail-closed error.

The post-run cleanup contract remains unchanged: generated Compose resources
are removed by each exact child, locked MinIO/Oxia base images are retained,
and no global Docker prune or source/worktree deletion is authorized. This
receipt is a candidate release handoff; target-branch merges and publication
still require their own explicit delivery steps.

## 2026-08-22 latest operational handoff — f4b7e005

The current source lock is Delay
`f4b7e005c217d938c26bdba1eaa107cadb355da`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The current strict handoff is
`/private/tmp/nereus-delay-release-gate-20260822-f4b7e005-rerun/release-candidate-gate.json`
with `release_status=NOT_READY`: all source/contract checks, real-service,
chaos, no-early, soak, operations, upgrade/downgrade and patch-distribution
inputs pass; only Benchmark and Capacity lack independent physical matrices.

The cleanup boundary is part of this handoff. After the runs, no generated
`nereus-delay` Docker resources remained. The pinned Oxia and MinIO base images
were retained, and no global prune was used. Sixty-three stale related
`/private/tmp` entries were moved out of the temporary directory into
`/Users/liusinan/.Trash/nereus-delay-cleanup-20260822-104500`; the retained f4
receipts, candidate source lock and current Gradle cache remain under
`/private/tmp`. No worktree, source checkout or `.git` directory was touched.

Do not publish or call this a full- release until the strict receipt changes
from `NOT_READY` to `PASS` after the independent §23.4 Benchmark/Capacity
artifacts are supplied and source-locked.

## 2026-08-22 physical-capacity runbook addition — a11d281c

The physical §23.4 runner is now available at
`e2e/run-physical-capacity-matrix.sh` in source commit
`a11d281cbc39416359c9a03085146c40d2142053`. It must be run with a newly
generated candidate source lock after all participating worktrees are clean.
It executes the real Kafka campaign before the real Pulsar campaign, captures
exact Docker/resource evidence, verifies MinIO object-mode evidence and
removes only the generated project resources. `NEREUS_DELAY__CAPACITY_MATRIX_FAST=1`
is an orchestration smoke mode and cannot satisfy a release gate.

No new physical run has yet been certified. The f4 receipts are retained as
historical provenance and must not be supplied to the current-source release
validator. Keep the pinned Oxia/MinIO images, avoid global Docker prune, and
continue to protect every source worktree during cleanup.

## 2026-08-22 physical-capacity producer lifecycle — 6209d824

Use candidate lock
`/private/tmp/nereus-delay-candidate-6209d824.json` for the next physical
run. K1 and P1 capacity producers now use 500,000-record epochs on the same
real topic, with epoch-qualified producer names and aggregate guarded evidence.
This keeps the full 1M/10M/100M campaign intact while bounding the client
lifecycle retained state observed during the earlier large Kafka probe.

The FAST smoke
`/private/tmp/nereus-delay-capacity-smoke-6209d824/capacity-matrix.json`
is a valid orchestration/cleanup check only: all 8 observations passed and the
exact generated Docker resources were removed, but its matrix status is
non-certifying `FAIL`. Run the default (FAST unset/0) serial campaign into a
new empty artifact directory for release evidence. Retain only the pinned
Oxia/MinIO images; remove generated images by exact tag and never use a global
Docker prune.

## 2026-08-22 current-source full certification — 6f9ab51c

The frozen candidate lock is Delay
`6f9ab51c392ea47dba46e0d6d67ff7f7d0aa0312`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The candidate receipt
`/private/tmp/nereus-delay-release-candidate-6f9ab51c/release-candidate-gate.json`
is `release_status=PASS`; full Gradle `check`, source validation and all ten
full- `PASS_CERTIFIED` inputs passed.

| gate | exact artifact | SHA-256 |
| --- | --- | --- |
| protocol-golden | `/private/tmp/nereus-delay-protocol-golden-6f9ab51c/protocol-golden.json` | `987f29b85496a296a7375d72eca5a3749335773a80f3d18e8b021e554e313253` |
| chaos | `/private/tmp/nereus-delay-full-chaos-6f9ab51c/full-chaos-matrix.json` | `49fb28741abafc12db03185b83a6d53b44c900d4ee4a16dca126b1876a91de80` |
| real-service | `/private/tmp/nereus-delay-real-service-6f9ab51c/real-service.json` | `2886a91d44f10900395c62fa821e435144c236c431295af74a6705b75a9cd43a` |
| no-early | `/private/tmp/nereus-delay-no-early-6f9ab51c/no-early.json` | `667de31953e8cdb665a2eb13b8e905c33dc3f10124c3767176e6f42e088e7c14` |
| benchmark | `/private/tmp/nereus-delay-benchmark-envelope-6f9ab51c/full-gate-input.json` | `bcf78ac3cc4584502f311b9102af9b34a888ac6e192b97859a44618797ea0bed` |
| capacity | `/private/tmp/nereus-delay-capacity-envelope-6f9ab51c/full-gate-input.json` | `cd9de96dc830a5d466c4a8679cf2b51ee5927545f75cc521c3ad66ba32139fb1` |
| soak | `/private/tmp/nereus-delay-soak-6f9ab51c/full-gate-input.json` | `fdf3c369bf2b1ce2b649a858d0654de13864bb82c95407b1e9d0f4a2a606fe96` |
| upgrade-downgrade | `/private/tmp/nereus-delay-upgrade-downgrade-6f9ab51c/full-gate-input.json` | `0f98682a7578fab55914f457cb33502bbb336cccddad2a8bd52196e1439c275f` |
| operations | `/private/tmp/nereus-delay-operations-6f9ab51c-r4/full-gate-input.json` | `5bebe0adec9b0c6cf6742f6a530cf67d5151e7e9f5ff68e1e86a6c373aa5f04a` |
| patch-distribution | `/private/tmp/nereus-delay-patch-distribution-6f9ab51c/full-gate-input.json` | `f763a9ea27e1bafc8009c91d895653e0d4a6002030e7ff392f83a3492b2672ab` |

The final post-overlay receipt will be written to
`/private/tmp/nereus-delay-release-final-6f9ab51c/release-candidate-gate.json`.
The certification boundary is the locked feature worktrees; it does not imply
merge, deployment or promotion into a target `main`.

Cleanup remains exact and recoverable: pinned Oxia, MinIO and the benchmark
`eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769`
image were retained, generated Nereus Docker resources were absent, and no
global prune was used. Eighty-eight unreferenced `/private/tmp/nereus-delay*`
directories were moved to
`/Users/liusinan/.Trash/nereus-delay-cleanup-20260822-full` after `.git`
checks. Current evidence and referenced historical receipts remain available.

## NDIP-1 current disposable and persistent certification entry points

Do not reuse the temporary receipt or source SHA in the historical section
below for a newer checkout. A current disposable run must use a retained,
operator-owned artifact base and its generation-3 verifier must bind all 24
cells plus `p1.compileRealPulsar`, `p1.h0`, and `p1.nativeCoordinator` to the
exact HEAD and directly verify every matrix/supporting log digest:

```bash
NEREUS_DELAY_DISPOSABLE_ARTIFACT_DIR=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-artifacts/ndip1-final \
NEREUS_DELAY_DISPOSABLE_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-disposable-local-certification.sh
```

Persistent staging additionally requires the exact disposable receipt and
SHA-256, an explicit signed `RESET_INTERNAL_ONLY` or
`CREATE_NEW_INTERNAL_ONLY` operator decision, and
`NEREUS_DELAY_STAGING_EXTERNAL_USER_DATA=false`. Its current result is read
only through
`nereus-delay-staging/local-docker-staging-ndip1/deployment/current.json` and
`e2e/validate-ndip1-persistent-certification.py`. A valid result includes G0,
13/13 Manifest readback, Gate C 41/41, SHADOW 0/0/0, AUTO_FAST 1/1/0,
Managed Handoff 1/1/1, and final DISABLED rollback. It remains local staging
evidence with `productionAuthority=false`.

The Managed canary must also reopen the same durable Attempt Journal
subscription and replay exactly `MAPPED / OWNERSHIP_STARTED / PUBLISHED` from
earliest retained data. Gate C carries digests for G0, its 13 observations,
the 41-row audit and Manifest readback; canary evidence uses closed
`{path,sha256}` references. Any missing/mutated sidecar or a binary Manifest
signature failure blocks the independent validator and prevents updating the
current deployment pointer.

## 2026-08-30 NDIP-1 disposable certification generation-3 contract

The current `e2e/run-disposable-local-certification.sh` emits
`receiptSchemaGeneration=3`. Every supporting check and every one of the 24
matrix cells carries both its evidence digest and `logSha256`; the independent
verifier rereads both paths and rejects any post-receipt mutation. Persistent
staging rejects generation-1/2 receipts even when their source field happens
to name the candidate.

The Oxia restart cell uses separate test/control logs. After the exact
data-server-1 stop/start it requires process health, a source-locked clean Oxia
CLI namespace read, and a 20-second session-expiry grace before releasing the
Route refresh gate. This prevents a port-ready process or a request that still
overlaps leader election from being interpreted as restart/reopen recovery.

Persistent staging binds `ROCKSDB_STORE` to the exact root passed as
`NEREUS_DELAY_PULSAR_WORKER_ROOT`; a nested marker-only directory is not an
acceptable substitute. The run creates incarnation markers in that root, the
real Worker writes its ShardStore below the same root, and STAGING smoke runs
retain it through the final 13-resource readback. A missing root, changed
marker, symlink, or Worker cleanup of that root blocks Gate C before any SHADOW
policy can be issued.

Final owner-drain checkpoints below a retained P1 Worker root are namespaced by
Route Incarnation, unsigned partition and owner epoch. The checkpoint identity
hash carries the same tuple. A retry of one drain therefore addresses the same
physical identity, while another Shard or ownership generation cannot collide
with or silently reuse its directory.

SHADOW evidence validation treats
`shadow/chaos/shadow-worker-ownership/worker-root` as a typed persistent-state
subtree. RocksDB WAL files happen to use the `.log` suffix but are binary and
must not be decoded as application logs. Outside that exact state root, every
`.log` remains required to be a regular, non-symlink, strict-UTF-8 evidence
file and is scanned for forbidden native-send markers. A decode error outside
the typed state tree is a certification failure; operators must not use
replacement decoding or ignore malformed files. Run `20260830091937-44496`
is the immutable blocked evidence that exposed this boundary after Gate C and
before any SHADOW receipt or ENABLED activation.

## 2026-08-28 NDIP-1 disposable local certification — da15290e (historical)

The current disposable entry point is
`e2e/run-disposable-local-certification.sh`. The source-bound run used Delay
`da15290e47b9255403c92e4ebba3c7d5189edb75`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`. Run it with a fresh artifact and an
exclusive resource prefix:

```bash
NEREUS_DELAY_DISPOSABLE_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-disposable-local-certification.sh
```

For a deliberately retained artifact, set
`NEREUS_DELAY_DISPOSABLE_ARTIFACT_DIR` to an owned output base outside the
repository before invoking the same runner. Without that variable the runner
uses the host temporary directory.

The current receipt is
`/var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/nereus-delay-ndip1-cert.XXXXXX.e3ypGoUrmz/disposable-local-certification-receipt.json`,
with SHA-256
`e8dfd5feef88afdfdbebd89b2118ae214833e318c50d2377adbc84a1890b5e61`.
It is `PASS`: all 24 closed cells are `EXECUTED_PASS`, with
`EXECUTED_FAIL=0`, `NOT_COVERED=0` and `skipped=0`. Coverage includes the
native Shared/Key_Shared strictness matrix, disabled delivery,
Exclusive/Failover native immediate behavior, TTL expiry, zero-retention
ledger trim, four response-loss cuts, real two-Broker failover, real Oxia
restart/ownership transfer, Oxia/MinIO checkpoint paths and RocksDB reopen.
Verify it from the exact bound source checkout with:

```bash
python3 -B scripts/verify-disposable-local-certification.py \
  --receipt /var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/nereus-delay-ndip1-cert.XXXXXX.e3ypGoUrmz/disposable-local-certification-receipt.json
```

This is a local disposable certification receipt/report, not a deployment
assessment or activation authority. It does not change the current G0
`NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT` state. A future
`EXISTING` or `STAGING` persistent environment must run the real G0 and its
independent Gate C path before any SHADOW or ENABLED operation.

Cleanup passed with the exact Compose project absent and empty remaining
containers, generated images, networks, processes, temporary credentials,
topics and volumes. The runner uses exact project/resource-prefix cleanup and
does not authorize global Docker prune or source/worktree deletion. A later
documentation-only commit does not rewrite this receipt's exact source
binding; historical verification must check out `da15290e` rather than editing
the receipt.
