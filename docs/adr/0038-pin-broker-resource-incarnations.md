# Pin Broker resource incarnations

Status: Accepted; Producer enforcement amended by [ADR 0044](0044-use-first-class-guarded-broker-transports.md) in `V1-FROZEN-2026-08-13`.

Nereus Delay V1 never treats a Broker cluster/topic name as sufficient continuity evidence. Deleting and recreating a Command, destination, receipt, or Attempt Journal topic under the same name creates a different resource incarnation. Routes and Destination Profiles pin an immutable Broker Resource Incarnation, and the actual Broker Fetch/Produce/SEND boundary carries or verifies that identity. An activation-time lookup alone is not publication authority. A mismatch fails closed; it never silently reads Commands from or sends old delayed data to the replacement topic.

## Identity contract

- Kafka identity is the authenticated cluster ID plus native topic UUID and fixed partition identity. Route/Profile registration reads and stores the topic UUID. Metadata refresh may update the pinned UUID's leader and epoch but cannot replace it with the current UUID for the same name.
- Pulsar identity is the authenticated cluster identity plus a cryptographically random Nereus-managed incarnation token stamped into every physical partition Topic's ManagedLedger properties under administrator-only ACL, together with Broker-reported creation identity for every physical partition used by the Route/Profile. Updating only partitioned-base metadata is insufficient because SEND/SUBSCRIBE guards inspect the actual physical Topic. The token is the runtime fencing field and cannot be copied to a recreated topic; the creation identity is a second registration and audit check. Registration creates or attests every partition marker; a deployment that cannot protect or verify them cannot register the resource for V1.
- Receipt topics and Pulsar Attempt Journals use the same rule and are additionally Nereus-owned. A recreated evidence topic is an evidence-retention gap, not an empty log.
- The exact incarnation enters Ingress Route or Destination Profile version, Destination Binding, Lane/channel identity, Prepared Publish hash, checkpoint semantic-version digest, metrics, and audit. Topic expansion or recreation requires a new immutable Profile or Route Incarnation.

Command application uses only the pinned immutable version and performs no live Broker lookup. Source ownership activation verifies the Ingress resource incarnation before opening `ACTIVE_FOR_COMMANDS`. Destination Lane activation verifies the exact target/evidence incarnation before `READY`; unavailable metadata is retryable Lane recovery, while a proven mismatch is `BLOCKED(DESTINATION_INCARNATION_MISMATCH)`.

Every READY transition writes `ReadyCertificateV1` with exact OwnerIdentity, Store Incarnation, Lane Incarnation, Adapter channel generation, evidence cursor/barrier, resource-guard attestation/config generation, protected Credential Use Lease generation/digest/fingerprint, and Trusted-UTC expiry. It is not a timeless boolean. Claim, Admission preparation, and first Producer call recheck the live certificate/lease; the first physical call validates the loaded fingerprint locally and reaches library ownership inside the configured local age bound. Admission apply/replay validates the embedded copy against its captured decision/Broker time and does not demand that its historical generations remain current. Resource attestation expiry, replacement, channel reconnect generation, credential rotation/fingerprint drift, Owner/store change or evidence discontinuity first removes the READY key and certificate before any new attempt can be prepared.

## Request-level enforcement

### Kafka

All Kafka V1 Command, source, destination, and evidence channels implement `PINNED_TOPIC_ID_V1`:

- Command and evidence readers issue FetchRequest v13 or newer with the exact Route/Profile topic UUID.
- Command, TIME_FENCE/control, target, receipt, DLQ, and evidence writers issue ProduceRequest v13 with the exact pinned UUID for each topic in the request or transaction.
- TLS endpoint identity/authentication and the connected metadata cluster ID must match the pinned cluster before any Fetch or Produce.
- No request may fall back to a version that identifies the topic only by name. Every possible Broker for the resource must support the required request version before the Route or Lane activates.
- `UNKNOWN_TOPIC_ID`, a metadata name-to-UUID change, or loss of a leader for the pinned UUID blocks the Route/Lane. It never causes the client to substitute the replacement UUID.
- Activation and receipt evidence LSO are captured from the `lastStableOffset` field in the same pinned Fetch v13+ response block that carries the exact topic UUID. Name-only ListOffsets/endOffsets is telemetry only and cannot establish a barrier.

The stock Kafka producer path is insufficient by itself because its Sender obtains topic IDs from current name-based metadata while building a request. V1 therefore requires a source-locked pinned-topic-id client patch or transport that puts the immutable expected UUID into the wire request. This applies to both records in a Kafka target-plus-receipt transaction.

### Pulsar

Every Broker that may own a V1 Command/fence/control writer, managed target, Pulsar Attempt Journal, DLQ Export, or `AUTO_FAST` target supports the source-locked first-class `PULSAR_RESOURCE_GUARD_V1` protocol v22. A trusted deployment controller signs a cluster capability attestation containing the guard protocol/version, Broker binary digest, complete Broker set, and configuration generation. Missing, expired, old-protocol, or partial coverage prevents Route/Profile registration and writer/Lane/native activation.

Each guarded Producer creation carries a typed expected cluster, physical topic/partition resource token, service-owned creation identity, and guard version. Before adding the Producer to the exact persistent physical Topic, the Broker compares those fields and the authenticated principal/cluster against the loaded ManagedLedger properties and returns a typed attestation in `CommandProducerSuccess`. On every `SEND`, including reconnect retransmission, `Producer.checkAndStartPublish` performs the same current-property comparison before `startPublishOperation` and `topic.publishMessage`, and a rejection balances any connection pending-send admission already charged by `ServerCnx`.

Mismatch or inability to prove identity returns `ServerError.ResourceIncarnationMismatch = 26`: `CommandError` for Producer creation and correlated `CommandSendError` for SEND. Success returns `CommandSendReceipt` with the validated guard attestation and Broker entry timestamp in addition to the existing MessageId. Receiving the exact typed SEND rejection is definitive nonpublication only when that physical attempt has no earlier ambiguous network write. Losing/mis-correlating the response, receiving a malformed/mismatched success receipt, or observing the rejection after an earlier disconnect remains `UNKNOWN`; uncertainty is monotonic. Every Nereus V1 guarded channel fixes `batching=false` and one unresolved SEND so sequence/pending-operation correlation is unambiguous; parallelism uses bounded channel slots.

An old Broker, ordinary unguarded Producer, transaction, automatic partition switch, plugin-only `NotAllowedError`, or exception-string classifier cannot satisfy this contract. `BrokerInterceptor.onPulsarCommand`, `producerCreated`, and `onMessagePublish` may remain experiment/audit hooks but are not V1 production non-persistence authority.

Auto-topic-creation is disabled for V1 resources. SDK and Worker principals cannot create, delete, or mutate incarnation properties; those operations belong only to the resource controller and every recreation generates a fresh token.

Pulsar consumption uses the independent source-locked `PULSAR_SUBSCRIBE_RESOURCE_GUARD_V1`: every initial/reconnect SUBSCRIBE carries the expected token, creation identity, physical topic/partition, and protocol. The Broker validates those fields synchronously against the exact `PersistentTopic`/ManagedLedger before adding the Consumer or returning success. Each connection generation begins `UNCERTIFIED`, sends no FLOW, and releases no record until guarded success. Mismatch closes the consumer/Source Assignment or evidence Lane, and queued callbacks from an older generation cannot apply/ACK. Admin lookups and `consumerCreated` are not Broker-bound proof. This contract covers Command source, Attempt Journal, and every Pulsar evidence/DLQ reader. Completing ADR 0044's Producer writer patch does not establish this source gate. Kafka consumption obtains the equivalent boundary directly from pinned Fetch requests.

## Permanent loss

Old messages are never rerouted to the replacement incarnation. An operator may keep the Lane blocked while restoring the original resource or issue an authenticated, source-ordered `CloseDestinationLane` for the exact Lane Incarnation and `expectedLaneControlVersion`. That operation:

- atomically moves the Lane to irreversible `CLOSED`, sets `laneCloseVersion/closedAtSourcePosition`, prevents new Admission, freezes truly unadmitted `SCHEDULED`/eligible `RETRY_WAIT`/`CLAIMED` records as `DEAD_LETTER(LANE_CLOSED_BEFORE_ADMISSION)` and uncommitted `PAYLOAD_RESERVED` as a closed reservation outcome, invalidates Claim/ready versions, and transfers state-split aggregate active/reservation counters once;
- leaves every marker-time `PUBLISHING`/`UNCERTAIN` attempt—and any Generation retaining an earlier unresolved admitted attempt regardless of its separate current-work kind—under its evidence contract and possible-delivery escrow; later success terminalizes normally, later definitive nonpublication retires the attempt then becomes `DEAD_LETTER(LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED)`, and unknown stays `UNCERTAIN`;
- forbids `ResolveUncertain(retry)` on the closed Lane because it would require a new Admission; only verified success or terminal-with-possible-delivery remains;
- records a durable canonical close cursor and marks any physically remaining Claim `closeOwnedByVersion`; restore never requeues it. Bounded terminal/reservation materialization is quota-neutral, and each record carries the close version that already transferred its counter;
- requires the same Close request to carry explicit order-loss acknowledgement for a strict Lane, whether or not a separate Break marker already ran;
- permits runtime retirement only after pending/inflight/evidence retention and the ancestry-bound Checkpoint Safety Barrier are complete, while a compact terminal Lane guard continues to reject the old tuple.

Continuing business on the replacement topic requires a new Destination Profile/resource incarnation and new Schedules. V1 does not migrate old messages across that boundary.
