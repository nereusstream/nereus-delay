# Use one RocksDB database per Delay Shard

Nereus Delay V1 assigns each Delay Shard its own RocksDB directory and database instance. The ingress partition, Oxia ownership and `ownerEpoch`, source-position commit, checkpoint, restore, local deletion, and migration boundaries are therefore identical. A shard database uses separate `timeline_cf`, `id_cf`, `inflight_cf`, `dedupe_cf`, `terminal_cf`, `gc_cf`, and `meta_cf` Column Families, with one RocksDB `WriteBatch` providing atomic command-state and source-position updates across them.

## Consequences

- The shard root contains a checksummed `ACTIVE` pointer and immutable `incarnations/<storeIncarnation>/db` directories. Restore verifies files in `restore-tmp`, generates a new Store Incarnation rather than reusing the checkpoint creator's token, install-mode opens and syncs that identity, closes the DB, then crash-safely replaces and fsyncs `ACTIVE` before normal open. A crash before the pointer leaves only an orphan; a crash after it is restartable.
- Because a database contains only one shard, timeline keys omit route and partition prefixes. `meta_cf` records the shard identity and source position, which are verified whenever the database is opened or restored.
- A checkpoint manifest describes exactly one complete shard database. Checkpoint creation and upload are staggered and constrained by Worker-level concurrency and I/O budgets.
- Workers may open multiple shard databases, but share process-level block cache, write-buffer budget, RocksDB `Env`, background threads, and rate limiter. Workers enforce limits on owned/open shards, memory, files, disk, and checkpoint activity.
- The Worker-owned slot is bound to the exact `ShardId`, not only to a count. A second open of the same shard is rejected before RocksDB open/create, preventing concurrent no-`ACTIVE` opens from installing different writable incarnations behind one active pointer; the identity is released only when the owning Store closes.
- V1 does not implement key-range export/import or merge a Worker-wide checkpoint into a running database. If benchmarks justify a future `ShardBundle`, the bundle must explicitly become the physical ownership, checkpoint, restore, and migration unit rather than silently combining otherwise independent shards.
