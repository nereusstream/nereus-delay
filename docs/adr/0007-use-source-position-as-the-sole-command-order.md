# Use Source Position as the sole Command order

Nereus Delay removes `command_sequence` from the protocol and uses the physical Source Position within a fixed Ingress Route partition as the only authoritative order for Client Commands and command-affecting System Mutations. The Broker append position orders records; the durable shard `WriteBatch` containing the state change, record result, deduplication state, and new `appliedShardLogPosition` is the application linearization point.

## Consequences

- Source Positions are supplied by the Kafka or Pulsar Ingress Adapter and are comparable only within the same route, topic, and physical partition. Clients cannot provide them and the system constructs no cross-partition order.
- Every Command for a Delayed Message uses its original route and partition. A record delivered through another route or partition yields `INGRESS_ROUTE_MISMATCH`; neither scans partitions nor reorders records to repair it.
- Repeated physical records with the same identity and hash preserve the first result and only advance `appliedShardLogPosition`. A conflicting hash is audited without applying or overwriting the original record.
- Cancel and Reschedule may optionally carry `expectedStateVersion`. A matching version permits the operation, a stale version yields `VERSION_CONFLICT`, and an omitted version applies to the current state in Source Position order.
- Message Generation is assigned by the Delay Shard to distinguish scheduling versions; client timestamps, UUID ordering, trace data, and enqueue-attempt IDs are audit data only.
-Nereus Delay does not infer caller intent or defer a Command that overtakes an unresolved prerequisite. Dependencies are resolved by awaiting the preceding applied result; deferred-operation tombstones require a separate design.
- The removed Protobuf field number and name are reserved permanently and cannot be reused for another meaning.
