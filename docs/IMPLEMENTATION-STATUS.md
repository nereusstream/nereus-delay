# V1 Implementation Status

Spec revision: `V1-FROZEN-2026-08-01`

This file records implementation evidence. It does not relax or replace the
normative requirements in [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md),
the [`V1 Protocol Registry`](V1-PROTOCOL-REGISTRY.md), or the Accepted ADRs.
An unchecked item is not an implementation permission; it is a release blocker.

The checkpoint code now covers the local physical boundary: checksum the full
RocksDB directory, emit the closed manifest JSON projection, and install a
validated checkpoint into a new local Store Incarnation without merging into an
open DB. The local store uses an `ACTIVE` checksummed pointer and an
`incarnations/<storeIncarnation>/db` directory. It does not yet publish
immutable objects, CAS an Oxia catalog, select a Recovery Set/Floor, or replay
Kafka/Pulsar source records; those remain release blockers below.

## Current repository shape

The repository is currently a single Gradle Java 21 library while the design's
multi-module target is being implemented incrementally. Package boundaries map
to the intended modules:

| Package | Current responsibility | Design target |
|---|---|---|
| `io.nereusstream.delay.protocol` | IDs, source positions, canonical hash, NDL1 frame, command envelope and body codec | `delay-api` / `delay-client-core` |
| `io.nereusstream.delay.store` | One RocksDB instance per shard, seven application CFs, value envelope, shared process resources and checkpoints | `delay-store-rocksdb` |
| `io.nereusstream.delay.runtime` | Deterministic Shard Log application, message state machine and Lane gate projection | `delay-core` |
| `io.nereusstream.delay.scheduler` | Lane-local failure isolation and bounded weighted DRR | `delay-core` |
| `io.nereusstream.delay.ownership` | Owner Lease CAS boundary and local ownerEpoch fencing | `delay-server` / `delay-metadata-oxia` |
| `io.nereusstream.delay.client` | Queued/applied outcome contract and embedded conformance service | `delay-api` / `delay-client-core` / `delay-testkit` |
| `io.nereusstream.delay.adapter` | Broker/destination interfaces and test adapters | ingress/adapter modules |

## Evidence matrix

| Area | Status | Evidence |
|---|---|---|
| Gradle Java 21 build | Implemented | `gradle compileJava`, `gradle test` |
| Self-routing IDs and CRC32C | Implemented | `ProtocolCodecTest` |
| `commandId + commandHash` prepared before I/O | Implemented | `PreparedCommand`, `CommandHash`, `ProtocolCodecTest` |
| NDL1 frame and canonical Client Command envelope | Implemented | `ShardLogFrame`, `CommandCodec`, registry frame vector test |
| Kafka/Pulsar source order token and source identity fencing | Implemented (core codec) | `SourcePosition.sourceOrderToken`, physical-resource comparison guard, `ProtocolCodecTest`; broker assignment/barrier adapters pending |
| Pinned Kafka/Pulsar command ingress outcome mapping | Implemented (transport SPI) | `PinnedKafkaCommandIngress`, `PinnedPulsarCommandIngress`, `AdapterIngressTest`; concrete pinned request transports and source assignment pending |
| Target publish side-effect outcome boundary | Implemented (transport SPI) | `DestinationPublishAdapter`, `PinnedKafkaDestinationAdapter`, `PinnedPulsarDestinationAdapter`, `DestinationAdapterTest`; Publish Admission/attempt ledger/evidence journal pending |
| One Delay Shard = one RocksDB DB | Implemented | `ShardStore`, `ShardStoreTest` |
| Worker DB/checkpoint resource limits | Implemented (config-envelope subset) | `ShardStoreConfig`, `SharedRocksDbResources`, `WorkerResourceEnvelope`, `WorkerResourceEnvelopeTest`; checked memory/FD/disk cross-bucket inequalities fail closed before resource creation; live JVM/cgroup/rlimit probes, WAL/SST/temp accounting, per-work-class reserves and placement capacity artifact pending |
| Seven application CFs plus empty `default` CF | Implemented | `ShardStore` descriptor validation |
| Shard identity and local Store Incarnation validation | Implemented | `StoreMetadata`, `ShardStoreTest` |
| Synchronous atomic WriteBatch | Implemented | `ShardStore.write`, `ShardStoreTest` |
| Native RocksDB checkpoint creation | Implemented | `ShardStore.createCheckpoint`, `ShardStoreTest` |
| Checkpoint file inventory and canonical manifest projection | Implemented (local/object publication boundary pending) | `CheckpointFileInventory`, `CheckpointManifest`, `CheckpointManifestTest` |
| Checkpoint restore into a new Store Incarnation | Implemented (local manifest/catalog-validated path) | `ShardStore.restoreFromCheckpoint`, `ShardStoreTest`; exact published floor-eligible catalog candidate is checked before local manifest/file validation; Oxia Recovery Pin/Floor CAS and source replay pending |
| Recovery catalog, lineage and Floor selection | Implemented (in-memory core subset) | `RecoveryCatalog`, `RecoveryFloor`, `RecoveryCatalogTest`; catalog binds one shard, rejects non-zero genesis lineage, enforces floor ancestry and exposes candidate validation/selection; durable Oxia catalog/session pins, Object Store publication and evidence cursors pending |
| Command applied/rejected state machine | Implemented (embedded core) | `DelayShard`, `DelayShardTest` |
| DUE/ORDERED/READY/EXPIRY timeline namespaces | Implemented (embedded core subset) | `DelayShard`, `ReadyIndexValue`, `KeyCodec`, `DelayShardTest`; READY key/value, laneVersion fencing, atomic affected-lane updates and fenced rebuild/discovery are covered; full registry `TimelineWorkRefV1`/GenerationRuntimeIndex pending |
| Terminal generation history | Implemented (Cancel/Reschedule subset) | `TerminalGenerationRecord`, `DelayShardTest`; publish/expiry/DLQ terminal obligations and GC retention pending |
| Large-payload reservation/proof/commit | Implemented (embedded core subset) | `LargeScheduleIntent`, `PayloadCommitProof`, `PayloadReservation`, object-backed `MessageRecord`, `DelayShardTest`; Object Store handles/attestation, source-ordered trust controls, Time Fence overlay and guarded GC pending |
| Source assignment, typed Activation Barrier and Owner Lease | Implemented (local boundary subset) | `KafkaActivationBarrier`, `PulsarActivationBarrier`, barrier-gated `OwnedDelayShard`, `OwnerLeaseStore`; renewal keeps owner identity/token/epoch and monotonic expiry; pinned broker assignment/guard, Oxia adapter and activation CAS pending |
| Queued vs applied client outcomes | Implemented (embedded core) | `EmbeddedDelayServiceTest` |
| Destination Lane gate/readiness projection | Implemented (core projection) | `LaneRecord`, `LaneRecordTest`, `DelayShard`; schedulable lanes maintain a versioned READY head and readiness/gate CAS transitions remove/recreate it atomically; source-ordered signed control mutation and full `LaneRecordV1` certificate fields pending |
| Destination Lane isolation and bounded weighted DRR | Implemented (scheduler core) | `LaneSchedulerTest`; lane work discovery and exact five-value registry projection pending |
| Persistent scheduler fairness counters | Implemented (core subset) | `PersistentLaneScheduler`, `LaneSchedulerTest`; full `meta_cf/SCHEDULER` closed projections pending |
| Worker-to-shard-to-lane bounded DRR | Implemented (core snapshot) | `WorkerScheduler`, `WorkerSchedulerTest`; durable outer scheduler projections and placement weights pending |
| Closed Stable Code registry | Implemented | `StableCode`, `ProtocolCodecTest` |
| Hard shard quota admission | Implemented (core subset) | `ShardQuota`, `DelayShardTest`; atomic multi-shard grants, control reserve and GC accounting pending |
| Kafka/Pulsar ingress and target adapters | In progress (ingress SPI only) | release blocker until concrete pinned transports, target publish/evidence channels and real-broker tests exist |
| Recovery Set/Floor, catalog and restore replay | In progress (local catalog/Floor subset) | release blocker; Oxia catalog/session pin, immutable publication, source/evidence replay and activation CAS remain |
| Large payload, quota grants, control reserve and GC | In progress (reservation/commit and shard hard-quota subsets) | release blocker; Object Store/Oxia publication, multi-shard grants, control reserve, Time Fence overlay and guarded GC remain |
| Query, control operations, DLQ and observability | Not started | release blocker |
| Real-service, chaos, benchmark, soak and upgrade evidence | Not started | release blocker |

## Verification command

Use the checked-in Gradle Wrapper and an isolated cache on hosts where the
default Gradle native cache is not writable:

```bash
GRADLE_USER_HOME=/private/tmp/nereus-delay-gradle ./gradlew clean check
```
