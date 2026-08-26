# Pin ordered delivery to one source and target partition

Nereus Delay offers no global FIFO. Its only strict order mode is `DELIVERY_TIME_FIFO` within one Ordering Domain, defined by `(deliverAt, effective Schedule Source Position, delayMessageId)`. An ordered domain is accepted only when all of its messages map to one Ingress Route partition, one immutable Physical Destination Partition, one Destination Lane, and a Delivery Capability Profile that can fence or resolve prior attempts. Baseline `AT_LEAST_ONCE` destinations expose `BEST_EFFORT` order because an old Owner or uncertain retry can produce duplicates or reorder observations.

The core guarantee is the proven destination-Broker durable append or handoff order. A Profile may extend it to consumer receive only when it certifies the exact partition/key routing, subscription type, Broker delayed-delivery implementation, and downstream consumer ordering contract. Consumer processing-completion order is never included.

## Routing and binding

- The initial Schedule carries an `orderingKey` when ordered delivery is requested. The SDK deterministically chooses its Ingress Route partition from exact `(Route Incarnation, tenantRoutingScope, orderingKey)` using `ROUTING_HASH`; `tenantRoutingScope` is the registry-bound 32-byte Security Domain value, not a caller tenant string. Every later Client Command uses the route/partition encoded in the ID, and signed control mutations use the exact shard subject; callers never recompute a mutable modulo mapping.
- Schedule application resolves and persists a Physical Destination Partition from the pinned Destination Profile version. The Profile contains the allowed explicit range or fixed `TARGET_PARTITION_HASH` input, partition-count snapshot, and hash version. Publishing addresses that physical partition directly rather than asking a future client or Broker metadata snapshot to repartition the key.
- Target partition expansion or a partitioner change creates a new Destination Profile version. Existing messages retain their old partition; Nereus Delay makes no ordering promise across Profile versions or an explicit migration boundary.
- Business key, Pulsar ordering key, and headers are preserved independently of physical routing. A user-supplied explicit partition is accepted only when the Profile permits and validates it.

## Scheduler and retry rules

- An ordered Lane has at most one unresolved head Publish Attempt. Later records cannot pass a head in `CLAIMED`, `PUBLISHING`, `UNCERTAIN`, or `RETRY_WAIT`; retry backoff therefore causes intentional head-of-line blocking only within that Ordering Domain.
- `UNCERTAIN_RETRY` is invalid for an ordered Lane. A policy that permits possible-duplicate retry can be bound only to `BEST_EFFORT`; strict delivery requires evidence or an explicit acknowledged Break/Close before any successor domain continues.
- Equal `deliverAt` values are ordered by the Source Position of the Schedule or successful Reschedule that created the generation, then by `delayMessageId` as a deterministic tie-breaker. Reschedule may move a generation to a new place in the order.
- Pulsar managed handoff uses a fixed, versioned handoff lead per Destination Binding, so `actionAt = deliverAt - handoffLead` remains monotonic. This is necessary but not sufficient for ordered consumer receive: the exact Broker/tracker/subscription path must pass an ordering certificate; otherwise an ordered Profile uses ordinary send at `deliverAt` or is rejected. The native branch selected by `AUTO_FAST` is outside this managed order contract.
- A proven permanent non-publication or expiration before any uncertain side effect may terminalize the head and unblock the next record. A strict Lane cannot use an unresolved-outcome timeout/override to continue the same Ordering Domain, because the old request may later appear out of order. Permanent evidence loss makes runtime readiness `BLOCKED`; an explicit signed Break/Close ends the old domain, after which a new Profile/Ordering Domain creates a different Lane ID. Replaying a dead letter creates a new generation at a new order position.
- Different Ordering Domains can interleave even when they share a physical Broker partition. Unordered Lanes may use multiple inflight sends subject to configured capacity.

## Admission and proof

An ordered Schedule is rejected with `ORDERING_CAPABILITY_UNAVAILABLE` when its Profile cannot pin a physical partition, its capability is only baseline at-least-once, or the domain-to-lane cardinality limit is exhausted. Tests must cover owner failover, lost acknowledgements, retries, equal timestamps, Reschedule, target partition expansion, and dead-letter unblocking without admitting a later ordered attempt before its unresolved head.
