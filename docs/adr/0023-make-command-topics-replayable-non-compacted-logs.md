# Make Command Topics replayable non-compacted logs

Nereus Delay V1 provisions every Ingress Route as a fixed-partition, non-compacted, durable Shard Log whose acknowledged Client Command and System Mutation history outlives the Recovery Set. Broker consumer progress is only a hint; the per-shard RocksDB `appliedShardLogPosition` is authoritative. Topic auto-creation, silent partition expansion, name-preserving recreation, and retention below the calculated recovery requirement are unsupported.

## Kafka route

- The topic uses `cleanup.policy=delete`, `message.timestamp.type=LogAppendTime`, fixed partition count, `acks=all`, idempotent Producers, and deployment-certified replication/minimum-ISR durability (normally RF at least three and minimum ISR at least two). Unclean leader election is disabled.
- SDK Command, system `TIME_FENCE`, and control writers use ProduceRequest v13+ with the Route's pinned native topic UUID. They cannot downgrade to a name-only request or turn a same-name replacement into a valid queued Command.
- Workers use a durable cooperative assignment group, `enable.auto.commit=false`, and `isolation.level=read_committed`, but V1 disables stock group OffsetCommit because its name-only request can touch a same-name replacement. A restoring shard seeks with the Adapter-defined successor. Its Kafka Activation Barrier comes from one pinned Fetch v13+ response partition block that simultaneously carries the exact Route topic UUID and `lastStableOffset`; `ListOffsets/endOffsets` is not authoritative. Catch-up is complete when the consumer's next position is at least that exclusive offset. Transaction markers and aborted records may create offset gaps and do not require synthetic Source Positions.
- Every fetch uses `PINNED_TOPIC_ID_V1`: FetchRequest v13+ carries the Route's immutable native topic UUID and cannot downgrade to or substitute a name-routed replacement. Unsupported request version, `UNKNOWN_TOPIC_ID`, or name-to-UUID drift leaves source activation blocked.
- Kafka Source Position contains Route Incarnation, authenticated cluster ID/native topic UUID, partition, record offset, optional leader epoch, and Broker append timestamp. Offset is the sole order token; leader epoch and resource incarnation detect truncation or recreation.
- RocksDB `appliedShardLogPosition` is authoritative. If a future source-locked topic-ID OffsetCommit v10+ hint is enabled, it occurs only after the corresponding WriteBatch and is ignored/rewound when ahead; stock name-based commit remains forbidden.

## Pulsar route

- The route is a persistent partitioned topic with fixed partitions, no compaction, durable retention of acknowledged entries, certified ensemble/write/ack quorum, and the Broker entry-timestamp interceptor required for per-Command retry/timing validation. Reclamation closure comes only from separately signed source-ordered `TIME_FENCE` records.
- SDK Command and system writers carry the Route's protected incarnation token in Producer metadata, and `PULSAR_RESOURCE_GUARD_V1` checks every SEND before persistence. Auto-topic-creation is disabled and only the resource controller can create/delete or mutate incarnation properties.
- Workers use one durable subscription with an Oxia desired-placement plan and one Broker-enforced Exclusive consumer per physical partition, with acknowledgements only after local durability. On recovery they seek from the database position; acknowledged history must remain readable under topic retention.
- Initial connect and every reconnect create an `UNCERTIFIED` source connection generation. The Ingress Adapter verifies the exact physical topic token and creation identity before records from that generation can reach Shard Runtime; mismatch closes the consumer, and callbacks from a superseded generation cannot apply or acknowledge.
- Pulsar Source Position contains Route Incarnation, physical topic partition, ledger ID, entry ID, batch index, batch size, and Broker entry timestamp. Batch members are ordered by batch index.
- Producer batching remains supported. The Worker may commit RocksDB per bounded group of batch members, but it advances the subscription cursor for a Pulsar Source Entry only after every member in that entry is durably applied or quarantined. A crash midway replays the whole entry; earlier members are idempotent duplicates. Checkpoint replay seeks to the containing entry boundary rather than assuming partial-batch cursor state.

## Retention and recovery invariant

For every shard, the earliest Broker-retained Source Position must be at or before the replay successor of the Recovery Floor checkpoint. Static time/size settings cover checkpoint interval and jitter, Recovery Set span, maximum checkpoint age, detection and outage budget, restore/download time, worst-case replay duration and rate, Broker clock uncertainty, and safety margin. Size retention is unlimited or provisioned from the certified peak ingress rate for the same interval.

Runtime monitors compare actual earliest position and timestamp with every checkpoint in the Recovery Set. Low margin blocks new Route production where possible and puts the shard into closed `ShardPauseReason.RECOVERY_RETENTION_RISK` at its current Source Position while forcing checkpoint/floor progress; it does not skip a Schedule to apply a later control System Mutation or emit a replay-unstable rejection. An actual gap is `FAILED(SOURCE_GAP)`, not a pause alias. No amount of consumer offset manipulation turns missing Broker data into a valid recovery.

## Route lifecycle

A Route Incarnation pins physical Broker Resource Incarnation, partition count, routing-hash version, protocol versions, Security Domain, and durability/retention policy. Kafka uses cluster ID plus native topic UUID; Pulsar uses an administrator-protected random Nereus topic token plus physical-partition creation identity. Expansion creates a new incarnation rather than changing modulo routing under existing ordered keys. Route states are `ACTIVE_FOR_NEW`, `CONTROL_ONLY`, `DRAINING`, and `RETIRED`: old message IDs keep sending Cancel/Reschedule/Replay to their original route while new Schedules use the active route.

An old route can retire only after it has no protected active/reservable messages, all command retry windows and result obligations are closed, its Recovery Set no longer needs the log, and ordered-domain cutover requirements are satisfied. Broker topic deletion/recreation requires a new Route Incarnation and cannot reuse old receipts.

## Continuous validation

Provisioning and Worker activation verify topic identity, partition count, compaction, timestamp mode/interceptor, durability, maximum record size, retention, subscription/group identity, request-level identity guard, and ACLs. Correctness-affecting configuration drift pauses affected route activation or source application and alerts operators; an exact Schedule-admission boundary requires a source-ordered control marker. Drift never silently weakens durability, resource identity, or time-fence semantics.
