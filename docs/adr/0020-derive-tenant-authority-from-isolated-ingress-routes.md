# Derive tenant authority from isolated Ingress Routes

Nereus Delay assigns each Ingress Route to exactly one tenant Security Domain and derives Command authority from that registered route plus Broker ACL, never from a `tenantId`, destination URI, or credential in message payload. A Security Domain may represent several callers that deliberately share authority, but Nereus Delay does not claim per-producer identity after Broker consumption. Shared multi-tenant Command Topics would require a signed-command protocol and are out of scope.

## Data-plane authorization

- Client credentials receive produce-only access to their registered Command Topic and no access to other tenants' routes. Delay Workers receive consume/seek access; administrative creation, retention, and ACL changes use separate service identities.
- Route configuration binds the physical Broker Resource Incarnation, partitions, tenant ID, protocol versions, quota grants, and allowed Destination Profiles. The Worker verifies that incarnation and derives Authenticated Tenant Context from the Route binding before decoding business fields.
- Schedule application authorizes the exact pinned Destination Profile, topic scope, partition policy, delivery capability, payload size, delay horizon, and ordering mode. Arbitrary endpoints, secret material, presigned URLs, and plaintext credentials are invalid protocol.
- Cancel, Reschedule, query, and replay require the same tenant context as the original message. Guessed cross-tenant identities return a non-enumerating authorization failure.
- Destination and object-store credentials are service-owned secret references. Secret bytes never enter Commands, RocksDB, checkpoints, logs, metrics, receipts, or DLQ records. Semantic identity changes create new Profile versions; credential rotation behind an equivalent reference can be transparent.

## API and operator authorization

Production APIs require TLS and an authenticated principal through the deployment's mTLS/SPIFFE or OAuth2/JWT integration. The authorization layer maps principals to tenant and explicit roles such as command producer, query reader, dead-letter operator, tenant policy administrator, and platform operator. Platform actions—route pause, shard drain, force checkpoint, quota-grant change, Profile publication, and recovery override—require a platform role and an immutable audit event.

Control-plane mutations use versioned compare-and-set records and stable operation IDs. Response loss is resolved by rereading the exact operation identity. Audit publication uncertainty is tracked as an outbox failure; it never causes an unrecorded mutation to be repeated under a new identity.

## Untrusted record handling

The Worker validates the Protocol Registry's `NDL1` frame magic/framing-version/kind/zero-flags/bounded payload length/CRC32C, canonical envelope/body version tuple, route/partition binding, identity/hash, timing, and profile references before allocating proportional memory or reading external payload. A syntactically valid but unauthorized command becomes a durable `REJECTED` result; only framing/identity that cannot be trusted becomes a bounded quarantined source record.

If identity cannot be decoded or trusted, the shard writes a bounded Quarantined Source Record keyed by Source Position, including a content hash, reason, size, and truncated diagnostics, then advances `appliedShardLogPosition` in the same WriteBatch. Raw oversized or sensitive bytes are not copied into metrics or logs. Quarantine rate opens a tenant-route circuit and alerts operators but does not permanently poison the partition.

## Native fast path

The native branch selected by `AUTO_FAST` uses the application's own direct Pulsar Producer authority. It is not routed through the managed service, does not inherit service-side Profile authorization, and returns native outcomes with no managed query, cancel, reschedule, server quota, or audit claim. The managed branch remains an ordinary Prepared Command. The SDK cannot silently substitute native delivery for `MANAGED`.
