# Treat payloads as opaque and metadata as Adapter-specific

Nereus Delay schedules Opaque Payload bytes. Serialization happens in the caller before Command preparation; delivery never loads application classes, calls a Schema Registry, upgrades an event schema, or interprets payload content. Destination-specific metadata is a closed Protobuf `oneof` selected by the pinned Adapter Profile rather than a misleading lowest-common-denominator map.

## Kafka record

The Kafka prepared-record projection is derived exactly from `PreparedPublishDescriptor`: value bytes/reference, optional key bytes, ordered repeated headers that preserve duplicates, and optional business event timestamp. It is not a second wire type. The Destination Binding supplies cluster/topic Broker Resource Incarnation and immutable Physical Destination Partition. Nereus uses a byte-array Producer; schema IDs or serialization envelopes already present in value/headers remain application bytes.

The Adapter adds reserved binary headers for Delay Message Identity, generation, `deliverAt`, Publish Attempt identity, Prepared Publish hash, capability, and diagnostic Owner Epoch. Caller headers under the reserved namespace are rejected. Every baseline or transactional send uses a v13+ `PINNED_TOPIC_ID` Produce channel with the Binding's UUID; a stock sender that replaces it from current topic-name metadata is not conforming. Baseline Producers enable Kafka idempotence and all-ISR acknowledgement but do not treat session idempotence as cross-restart deduplication; transactional channels follow their separate capability contract.

## Pulsar record

The Pulsar prepared-record projection is derived exactly from `PreparedPublishDescriptor`: value bytes/reference, optional partition key bytes/string encoding choice, optional ordering-key bytes, unique UTF-8 string properties, and optional event time. It is not a second wire type. It targets the pinned physical topic partition directly. Producers use `Schema.BYTES`; a Profile whose topic policy requires an incompatible typed schema is rejected at registration/application rather than discovered after a long delay.

The Adapter adds reserved properties equivalent to the Kafka identity metadata. A typed first-class `TopicResourceGuard` carries the exact cluster/resource token and physical-topic creation identity at Producer creation; the Broker binds it to the authenticated principal/physical topic, revalidates current ManagedLedger properties before every SEND persistence, and echoes typed success evidence. Certified delayed handoff additionally sets the guarded Broker deliver timestamp. Broker-dedup capability controls stable producer name/sequence ID and disables client batching so one sequence maps to one Publish Attempt; baseline Producers do not claim dedup but still require the resource guard.

## Prepared Publish

Claim materialization fetches and verifies out-of-line bytes, applies exact metadata validation, checks target record-size/config limits, and constructs an immutable `PreparedPublishTemplate`. Publish Admission allocates the exact attempt/channel/sequence fields, adds reserved metadata, and computes:

```text
SHA-256(
  "nereus-delay-prepared-publish" ||
  adapterEncodingVersion ||
  canonical binding/resource incarnation/physical target ||
  canonical business metadata ||
  payload length + SHA-256(payload) ||
  canonical reserved metadata except the hash field)
```

The durable descriptor contains inline bytes or an immutable payload reference/checksum and can reconstruct the same logical target record. Compression, protocol framing, and Broker batching may change transport encoding but not the prepared logical bytes. A mismatch between stored payload descriptor, downloaded checksum, or prepared hash is corruption or a definitive message failure before external side effects.

Profile registration probes target message-size, schema, header/property, partition, delayed-delivery, transaction/dedup, TTL, and authorization prerequisites. Runtime drift removes READY and marks the Lane runtime `BLOCKED`; it does not mutate the source-ordered administrative gate. Nereus Delay does not automatically convert Kafka headers to Pulsar properties, change payload encoding, or route one Schedule to multiple destinations.
