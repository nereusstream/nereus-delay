# Use ancestry-bound Recovery Lineages and recovery pins

Nereus Delay does not use a free scalar mutation sequence as proof that one checkpoint contains another checkpoint's state. Every checkpoint and local Store Incarnation belongs to an explicit Recovery Lineage, and every recovery candidate must prove ancestry from the exact current Recovery Floor. A candidate is protected by an Oxia recovery pin while it is inspected, downloaded, installed, and activated.

## Lineage

Each checkpoint manifest and catalog entry contains:

- nonzero random 16-byte `recoveryLineageId`/`checkpointId` fixed before I/O, plus `lineageGeneration`; checkpoint retries reuse the same ID and only genesis allocates a lineage ID;
- exact `parentCheckpointId + parentManifestHash`, or a declared base;
- `restoredFromCheckpointId` for the Store that produced it;
- canonical `appliedShardLogPosition`, `shardMutationSequence`, and typed evidence cursors;
- the exact Store/DB/Route/shard identities and manifest hash.

`shardMutationSequence` is compared only inside a proven ancestry chain. The Recovery Floor names an exact `(lineageId, checkpointId, manifestHash, catalogGeneration, appliedShardLogPosition, includedMutationSequence, evidenceCursors)`. `EvidenceCursor` is the canonical tagged Kafka-receipt/Pulsar-journal union defined by the main spec and Protocol Registry; arrays sort strictly by kind/Lane/Lane-Incarnation/resource/partition/**evidence-generation**, and cursor dominance is legal only for that identical full key and its type-specific contiguous boundary. Protected old/new generations may coexist. “At or above the floor” means the candidate's parent-hash chain reaches that exact checkpoint and every required typed source/evidence cursor dominates it; a numerically larger sequence on another branch or incomparable resource identity proves nothing.

## Fallback

If a newest checkpoint is unusable, the Owner may choose only another valid descendant of the current Floor. Continuing from an earlier descendant creates a new lineage generation with that checkpoint as its declared base and CAS-marks later incompatible descendants `SUPERSEDED`; it never names a discarded descendant as parent or reuses its scalar sequence. If no candidate descends from the Floor, recovery fails closed. Disaster acceptance below the Floor requires a new Route Incarnation and explicit loss/duplicate boundary.

## Local Store reuse

A checksummed local `ACTIVE` pointer is only a hint. The DB must contain its lineage/base checkpoint, last observed Floor identity, source/evidence cursors, Store Incarnation, and clean/unclean state. Reuse is legal only if an invariant audit proves that its lineage includes the current Floor and its retained dependencies still satisfy every Floor cursor. A host that missed a newer Owner's floor-covered retirement or deletion cannot reopen its old DB.

## Recovery pin and final revalidation

Before reading a local candidate or downloading a checkpoint, the lease holder uses a transaction comparing the exact Owner Lease/session and catalog generation to create a `RecoveryPin` under that same Oxia session. It binds a random Pin ID, Owner, candidate, exact Floor, catalog generation and session digest. It has no client-clock expiry: while the record exists, checkpoint removal, supersession cleanup, and orphan reaping protect candidate and Floor objects. Create-response loss rereads the exact path/value; a different pin is not success.

Immediately before replacing the local `ACTIVE` pointer and again before `ACTIVE_FOR_COMMANDS`, the Owner rereads the exact pin, Floor, catalog generation, lineage head, source retention, and evidence retention. If the Floor advanced beyond the candidate, the session-bound pin disappeared, or any identity/cursor changed incompatibly, it closes/discards the installation and restarts selection. The final transaction both marks the exact Owner Lease `ACTIVE_FOR_COMMANDS` and deletes that pin; response loss rereads both. Pins improve mutual exclusion but never make an invalid candidate valid, and session loss removes them automatically.

## GC barrier

A resource mutation is deletable only when an exact Floor checkpoint is proven by ancestry to contain its `RESOURCE_RETIRE_INTENT` and reconstructible GC tombstone. Delete confirmation remains in the Shard Log and is retained until a later descendant Floor contains it. GC comparisons therefore use `(lineage, checkpoint ancestry, mutation sequence)`, never a free sequence from a different restored branch.
