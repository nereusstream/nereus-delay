# Bound multi-DB Workers with shared resources and weighted placement

Nereus Delay V1 allows one Worker process to own multiple one-shard RocksDB instances, but every process has explicit hard admission and shared resource budgets. All shard DBs use the same RocksDB `Env`/background pools, block cache, WriteBufferManager, rate limiter, statistics, and process-level SST/file accounting. A Worker never accepts an unbounded number of assignments merely because each DB is individually configured.

## Mandatory Worker guards

Configuration must provide, and startup must cross-validate:

- `maxOwnedShards`, `maxOpenShardDbs`, and concurrent `ACQUIRING`/restore/drain limits;
- shared block-cache and total memtable budgets, per-DB write-buffer ceilings, background flush/compaction jobs, RocksDB rate limiter, and separate JVM heap/direct-buffer/RocksDB-native/other-native/RSS envelopes;
- per-DB and process-total open-file, WAL bytes/files, MANIFEST bytes/files, live SST and pinned cache/iterator limits plus OS headroom;
- local live-data, temporary checkpoint/restore, compaction-amplification, and Control Capacity Reserve disk watermarks;
- publish executor messages/bytes, per-Lane and per-cluster connections/producers/threads/physical requests/zombies, payload fetches, query waiters, and object requests;
- checkpoint create/upload/download and restore concurrency, bytes/sec, temporary bytes, and jitter.

Numeric defaults are release benchmark outputs and are not guessed in the design. Missing or internally inconsistent guards fail startup.

Memory buckets are disjoint. RocksDB-native includes cache, mutable/immutable memtables, table-reader metadata, pinned blocks/iterators and flush/compaction scratch, excluding bytes charged to direct/other-native. Startup reads actual JVM/cgroup limits and proves with checked arithmetic:

```text
actualXmx <= certifiedJvmHeapBytes
actualMaxDirectMemory <= maxDirectMemoryBytes
sharedBlockCacheBytes + sharedWriteBufferBudgetBytes <= maxRocksDbNativeBytes
certifiedJvmHeapBytes + maxDirectMemoryBytes
  + maxRocksDbNativeBytes + maxOtherNativeBytes
  + minInProcessControlHeadroomBytes <= maxProcessRssBytes
maxProcessRssBytes + minContainerHeadroomBytes
  <= effectiveCgroupMemoryLimitBytes
```

Unknown/unbounded limits fail startup. Per-DB/process WAL, MANIFEST, SST, pinned cache/iterator and FD file/byte sums are likewise checked against RLIMIT and the exact filesystem/quota holding `rootPath`. Checkpoint-create, restore and compaction temp headroom is disjoint from Control Capacity Reserve.

Shared totals alone are not fault isolation. Every `grantVersion` binds the Protocol Registry's immutable `ShardCapacityEnvelopeV1`: a complete zero-explicit 1–66 `CapacityVectorV1`, exact outcome/non-outcome/recovery/emergency component grants, logical Quota Grant, and release capacity-artifact digest. The full vector already includes worst-case write/compaction amplification, WAL/MANIFEST/FD budget, memtable ceiling, Adapter minima and Control Capacity partitions. The hard filter counts `sum(shard envelopes) + Worker fixed cost + transition temporary demand` exactly once. Placement may not count presently unused committed capacity as available to another shard or add one embedded component twice.

The envelope is charged from assignment acceptance until DB/channel physical release; ACQUIRING, RESTORING, CATCHING_UP and DRAINING are not free. Migration charges old and new Workers concurrently plus restore temporary demand. Owner Lease acquisition validates the exact envelope version/digest; unknown or mismatched identity fails closed.

Every CF of every DB receives the same shared cache and WriteBufferManager objects, but the Worker enforces per-shard ceilings and work-class reservations above RocksDB: correctness work (lease/fence, Shard Log WAL sync, admitted outcome and recovery metadata) has a non-borrowable minimum; due-index reads and expiry have a second minimum; flush/compaction/checkpoint/restore consume separately capped low-priority tokens. `maxBackgroundJobs` is split into reserved nonzero flush progress and bounded compaction/checkpoint shares, with per-DB concurrent-job ceilings. One DB hitting WriteBufferManager pressure, L0 slowdown, compaction debt or checkpoint upload cannot take another DB's WAL/due minimum. Every event-loop class has positive weight, bounded record/byte queue, per-turn record/byte/time caps and `maxEventLoopClassDelay`. Borrowed cache/I/O/job capacity is reacquired at each bounded chunk and becomes reclaimable within configured maximum hold time.

Process-level accounting includes native memory and pinned iterator/cache blocks, not just JVM heap. Certification injects one-shard write amplification/compaction storms and proves another shard's WAL sync, expiry and healthy-Lane service remain within bounds.

## Placement

Each shard publishes a bounded, low-cardinality load vector: active messages/bytes, command ingress rate, due/admitted publish rate, RocksDB live size, memtable and compaction-pending bytes, WAL/fsync and stall time, checkpoint size/age, source/due lag, Lane count/failures, and local-disk contribution. Workers publish available DB slots, memory/disk/FD/I/O/publish capacity and health.

A weighted, capacity-aware placement policy uses hard filters first and a normalized dominant-resource score second. The hard filter uses the sum of full committed shard grant envelopes, configured amplification, per-shard Control Capacity partitions, DB/WAL/MANIFEST/FD minima and Adapter channel minima; observed usage is used only for scoring and early movement, never to overbook a hard promise. It applies hysteresis, minimum residence time, movement cost proportional to checkpoint/replay size, and per-Worker/cluster migration concurrency limits. Stale telemetry is charged conservatively. Equal shard count is never treated as equal load.

Kafka uses a versioned cooperative weighted assignor that carries Worker capacity and recent owned-shard weights in group subscription metadata; unmeasured partitions use a conservative configured weight. Pulsar uses an Oxia desired-placement plan and one Broker-enforced Exclusive consumer per physical Command partition. Desired placement is not publication authority: successful source attachment and the separate Owner Lease are still required.

If an unexpected Pulsar assignment would exceed a hard guard, the Worker refuses Owner Lease acquisition and leaves it in the Oxia desired-placement repair path; it never opens one more DB. If no Kafka member fits a full envelope, the assignor returns a stable `UNASSIGNED(NO_CAPACITY)` set rather than choosing a least-overfull member; an unchanged membership/capacity/config epoch cannot self-trigger an assign/rejoin loop. An unexpectedly assigned partition is paused before fetch delivery and performs no DB open, lease, apply, ACK or commit while exact cooperative revocation/rejoin runs. If the group has not removed it by `overCapacityAssignmentDeadline`, the Worker explicitly leaves and does not rejoin until a material epoch change or configured backoff. Keeping an over-capacity assignment merely paused is forbidden because it can pin ownership and block redistribution. Rebalancing moves a bounded number of shards at a time.

## Pressure response

Persistent logical grants are sized so normal Schedule admission cannot consume the physical memory, FD, disk, compaction, and Control Capacity headroom. A breached process/filesystem/shared-WBM guard closes acquisition and Claim/Admission for every shard sharing that failure domain; only an independently hard-enforced per-shard limit may close one shard. If authoritative writes are no longer safe, all affected shards stop at their exact Source Positions rather than emit environment-dependent Schedule results. A Worker does not evict or close an actively owned shard as a cache policy; ownership is drained and transferred explicitly. A lower Worker/container envelope is staged inactive, rejects new ownership, and drains/migrates until all commitments fit before activation; externally imposed early shrink is a safety breach. Failure to preserve the Control Capacity Reserve fences affected shards.

V1 accepts the costs of independent WALs, MANIFESTs, file sets, memtables, and inability to group-sync across DBs. Only benchmark evidence may justify a future fixed `ShardBundle`, and that Bundle must become the explicit ownership/checkpoint/restore/migration unit.
