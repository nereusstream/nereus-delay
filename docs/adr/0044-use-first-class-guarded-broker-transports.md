# Use first-class guarded Broker transports

Status: Accepted
Spec revision: `DESIGN-BASELINE-2026-08-25`

A Route or Destination Profile pins one physical Broker resource incarnation. A metadata lookup performed before send cannot prove that a later retry, reconnect, batch, or Broker request still targets that incarnation. The guard must survive into the real Kafka Produce or Pulsar SEND boundary, and the result must distinguish proven non-persistence from an ambiguous prior attempt.

## Decision

- Kafka and Pulsar client changes expose generic resource-guard APIs. They contain no Nereus Route, Schedule, receipt, or Worker type.
- Kafka `sendGuarded` binds authenticated cluster ID, canonical topic name, native topic UUID, and explicit partition to the record before accumulator ownership. The immutable guard is carried by `ProducerBatch`, participates in batch compatibility, survives split/retry/reenqueue, and supplies the exact TopicId in Produce v13-or-newer. Metadata may refresh the leader for that UUID but may not substitute the current UUID for the topic name. This first generic API is non-transactional because the selected transaction request path cannot satisfy the v13 floor. It is sufficient for a single Command record, but it does not satisfy ADR 0038's Kafka target-plus-receipt transaction; that channel stays blocked until a separate source-locked transactional v13 guarded path exists.
- Kafka returns typed guarded metadata/evidence. A pre-network mismatch or a registered authenticated Broker rejection with no earlier ambiguous attempt is definitive non-persistence. On the inspected K1 baseline the Broker-side definitive resource allowlist contains only Produce v13 `UNKNOWN_TOPIC_ID(100)`; other Produce errors are not promoted by a generic `error != NONE` branch. Timeout, disconnect, response loss, missing success evidence, or a mismatch after an ambiguous attempt remains unknown.
- Pulsar adds a first-class `TopicResourceGuard` to Producer creation on the source-locked `5.0.0-M1` baseline. Protocol v22 carries the expected cluster, 32-byte resource token, and service-owned physical-topic creation identity. The Broker validates the exact persistent physical Topic at Producer creation and again at the beginning of every SEND, before `startPublishOperation`/topic persistence, balances any connection-level pending admission on rejection, and replies with typed `ResourceIncarnationMismatch = 26` on mismatch.
- A guarded Pulsar success echoes the validated resource identity, physical topic/partition, Broker entry timestamp, and MessageId in the correlated Producer success/SEND receipt. Missing or mismatched success evidence is not success. Old Broker protocol, auto-created resources, partition auto-switching, transactions, unguarded fallback, or partial Broker rollout fail closed.
- Nereus fixes Pulsar batching off and one unresolved SEND per guarded channel so one typed response maps to one physical attempt. Bounded parallelism uses separate channel slots.
- Existing pinned Kafka/Pulsar Nereus adapters remain responsible for projecting generic transport results into NDR1. Client libraries never emit Nereus receipts.
- Source consumption remains independently gated by pinned Kafka Fetch v13 and guarded Pulsar SUBSCRIBE connection generations. Completing the writer guard does not establish the Worker source, ACK-after-sync, or release gate.

ADR 0038 remains the resource-identity contract and is amended in the same revision to replace its Pulsar plugin-only Producer enforcement with this first-class protocol path. A `BrokerInterceptor` string error may be retained for experiments or audit, but it is not production non-persistence evidence.

The exact classes, fields, call sites, failure classification, branches, and tests are specified in [`CurrentDIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md`](../DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md).

## Consequences and trade-offs

- The common path gains one nullable batch guard check in Kafka and one guarded property comparison per Pulsar SEND. Ordinary unguarded APIs retain their existing behavior.
- Delete-and-recreate safety no longer depends on a best-effort activation probe or mutable name metadata, at the cost of maintaining source-locked client/Broker patches and rollout attestations.
- Batching restrictions reduce per-channel Pulsar throughput for Nereus Delay. Capacity is recovered with bounded channel parallelism and measured configuration, not by weakening correlation.
- A typed rejection only proves that the rejected attempt did not persist. Historical network ambiguity is monotonic and cannot be erased by a later guard failure.
- Completing the non-transactional Kafka API is not evidence for a Kafka atomic target-plus-receipt channel. Such a Profile cannot activate from this patch and may not downgrade to name-only Produce v11.
- These client changes are necessary transport evidence, not proof that the Nereus production Route authority, Gateway, source adapter, Worker lifecycle, chaos, benchmark, or release gates are complete.
