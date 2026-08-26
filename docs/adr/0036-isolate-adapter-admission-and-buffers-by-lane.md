# Isolate Adapter admission and buffers by Destination Lane

Nereus Delay carries Destination Lane fault isolation through the Adapter submission boundary. Scheduler fairness and per-Lane RocksDB state are insufficient if unrelated Lanes share an unbounded Producer queue, metadata wait, transaction lock, callback executor, or client buffer. Every Lane therefore has a bounded Adapter Channel admission domain for tasks, messages, bytes, connections/producers, and deadlines.

## Submission boundary

- Scheduler visits and the shard event loop never wait for a destination Producer call or Broker Future.
- A claimed message obtains Lane, shard, and Worker logical task/byte permits **and** Lane-owned physical-outstanding request/byte permits before Publish Admission. Admission atomically charges the attempt against Lane, Worker and target-cluster connection/producer/request/physical-byte/buffer/thread envelopes; a logical timeout never returns that physical charge early.
- Synchronous Producer work, including metadata lookup, authentication, buffer acquisition and any client call that may block before returning a Future, runs in a Lane-bounded Adapter executor outside shard correctness threads. It has a configured `adapterSubmitDeadline` and per-Lane thread cap. Timeout after request ownership becomes `UNKNOWN`; a proven pre-ownership refusal can be `NOT_PUBLISHED`.
- `callbackDeadline` only bounds the logical waiter. The attempt keeps its physical-outstanding and zombie charges until one of three events is observed: exact completion, library-confirmed cancellation before remote ownership, or fenced close of the exact Producer/channel generation. Dropping a Future, interrupting a Java thread, or expiring a timer is not resource release.
- Every Lane, Worker and target cluster has hard caps for connections, producers, synchronous-call threads, physical outstanding requests/bytes, zombie requests/bytes, buffered messages and buffered bytes. Admission reserves the vector in which all current physical requests become zombies simultaneously. Reaching a Lane zombie cap changes only that Lane to `runtimeReadiness=BLOCKED`, fences/tears down its channel where the capability permits, and prevents further Admission there; it cannot consume another Lane's reserve.
- A Lane at its channel cap leaves additional records in RocksDB and updates its READY time; it does not enqueue unbounded tasks.

## Sharing rule

A Producer, connection pool, network event loop, or buffer may be shared only when the Adapter proves all of the following:

1. per-Lane message and byte reservations cannot be consumed by another Lane;
2. one topic's metadata/auth failure or full buffer cannot head-block another Lane's submission;
3. callbacks and circuit state are independently attributable;
4. blocking time is bounded and cannot exhaust every Worker execution thread;
5. physical outstanding and zombie ownership is metered to the originating Lane until actual release;
6. the Worker and each target cluster retain fixed connection, producer, thread, request and byte minima for other READY Lanes.

If the client library cannot provide that isolation, the Adapter uses a Lane-scoped bounded producer/channel. Kafka transactional and Pulsar sequence-evidence identities additionally follow their strong-capability Lane contracts. Channel cardinality consumes a persistent shard Lane grant; exhausting that grant rejects creation of a new Schedule/Lane deterministically. A Worker's instantaneous connection cap is a placement/runtime constraint: it removes READY and marks the Lane `BLOCKED(CAPACITY)`, or prevents assignment; it never writes `ADMIN_PAUSED` or changes a replayed Command result.

The Worker separately caps total Adapter connections/producers/threads/requests/physical bytes/zombies and per-cluster totals. For every resource dimension and both scopes, Admission proves `retainedPhysical + candidatePhysicalAndPotentialZombieCharge + sum(committed minima of every other READY Lane) <= hardCap`. A new channel is admitted only if its full Lane minimum envelope fits. Online borrowing may improve utilization, but revocation affects only future Admission; an outstanding borrowed physical charge is not free until exact release. A Lane whose full minimum cannot be committed is `BLOCKED(CAPACITY)` and cannot hold a current Ready Certificate.

## Guarantee boundary

A cluster-wide or shared-network outage can make every Lane using that dependency unavailable. The hard promise is that a failure confined to one topic, credential, Lane buffer, circuit, Producer, or unresolved attempt cannot consume unrelated Lane reserve or pause Command application. Tests inject permanently blocking synchronous metadata calls, dropped callbacks, Futures that ignore cancellation, full client buffers, metadata failure, and one-Lane connection churn. Repeated logical timeouts must converge at the offending Lane's zombie cap, while memory, threads, connections and requests stay bounded and a READY Lane on an independent target continues within its certified service-gap bound.
