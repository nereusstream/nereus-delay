# Scope strong destination channels to Destination Lanes

Nereus Delay scopes destination-side transactional or sequence evidence to a bounded Destination Lane rather than sharing one unresolved outcome domain across unrelated Lanes. A strong capability may use several fixed channel slots inside one Lane, but its transactional ID, producer name, sequence state, circuit, evidence cursor, and unresolved-order constraint cannot silently couple healthy Lanes.

## Kafka transactional receipt

Each `KAFKA_TRANSACTIONAL_RECEIPT` channel is identified by:

```text
(deploymentId, targetClusterId, routeIncarnation,
 shardPartition, receiptLaneSlot, receiptSlotGeneration,
 destinationLaneId, laneIncarnation, channelSlot)
```

The Route creates `K` exclusive receipt partitions per source shard. Strong-Lane creation persistently allocates `(receiptLaneSlot, receiptSlotGeneration)`, mapping to `shardPartition * K + slot`; slots are never shared concurrently. The cluster must have finalized `transaction.version >= 2`: transaction caps ProduceRequest at v11 and therefore cannot carry topic UUIDs. The transaction atomically writes the exact target record and a receipt to that partition, using the separately pinned native UUID of each topic in ProduceRequest v13+. A new Owner calls `initTransactions()` for a Lane's fixed channel slots before that Lane can publish, then replays only its receipt partition with `read_committed` through LSO using its pinned UUID. An unresolved transaction can hold back only that Lane's LSO. Strong-Lane, receipt-slot, and channel cardinality are hard-limited and included in placement/resource accounting.

Reusing a retired slot requires all old channels fenced/closed, no active or retained evidence, retention completion, and a Checkpoint Safety Barrier. Reallocation checked-increments `receiptSlotGeneration`, which is part of every transactional/evidence identity.

## Pulsar Broker dedup

Each `PULSAR_BROKER_DEDUP` producer name includes `destinationLaneId`, `laneIncarnation`, and one exact Physical Destination Partition. Sequence IDs are persisted and strictly increasing only within that producer/Lane domain. An unresolved lower sequence blocks subsequent sequences in the same Lane but never a different Lane.

The exact sequence-to-Generation mapping is first persisted in the Pulsar Attempt Journal; target send without that durable mapping is forbidden. Journal and target Producers both carry their own pinned incarnation token, and every reconnect/retransmitted SEND passes the Broker resource guard. The new Owner replays the journal, then reuses the exact producer name and physical partition. If the old same-name Producer is still live, the new Lane remains unavailable until it can connect; it does not choose another name or downgrade capability. Broker last sequence proves an outcome only for journaled mappings inside the certified dedup-retention horizon.

## Fault isolation and ordering

Lane-scoped channel state prevents a timeout, producer collision, transaction recovery, or uncertain sequence in one Lane from consuming another Lane's semantic evidence path. Per-Lane and per-shard message/byte/connection caps plus Worker reserve keep a hung Lane from occupying all executor capacity.

`DELIVERY_TIME_FIFO` promises the proven durable append/handoff order inside one Ordering Domain. Extending that claim to consumer receive requires a Destination Profile certificate for the exact Broker version, physical partition or key routing, subscription type, delayed-delivery implementation, and consumer ordering contract. Consumer processing-completion order is never part of the current design.

`laneIncarnation` is deterministically derived from the Lane ID and the Source Position that creates this instance. Retirement first logs a durable intent and compact terminal Lane guard. Runtime state and grants can be released only after no pending/inflight/ready state remains, all attempt/evidence retention and Adapter fencing are complete, and an ancestry-proven Recovery Floor includes that intent. Thus every permitted checkpoint sees either the old incarnation or its retirement guard. The same closed/broken tuple cannot recreate; continued traffic requires a new Profile/Ordering Domain and therefore a new Lane ID/incarnation, so a purged local sequence can never collide with an old Broker producer identity.
