# Hash a versioned Canonical Command Body

Nereus Delay carries each operation as an exact Canonical Command Body. The normative [`Current Protocol Registry`](../PROTOCOL-REGISTRY.md) fixes the frame, enum/body fields and domain-separated preimages. `commandHash` binds framing/log/envelope/body versions, Command type, Command/Message identities, retry deadline and canonical body; `mutationHash` binds the same version tuple, mutation type, exact shard subject, mutation deadline and canonical body. Physical Broker headers, Source Position, producer/batch metadata, enqueue-attempt ID, tracing, authentication, and Broker timestamps remain outside those hashes. The same logical retry must reuse the original bytes and version tuple; semantic re-creation under the same Command Identity is not accepted.

## Envelope

The Broker value begins with the Protocol Registry's `NDL1` frame (`magic`, framing version, record kind, zero flags, bounded payload length and CRC32C). The canonical payload is a closed oneof. Client Commands and service System Mutations never share an unsigned privileged shape:

```protobuf
message ShardLogEnvelope {
  uint32 log_envelope_version = 1;      // exactly 1
  oneof record {
    DelayCommandEnvelope client_command = 2;
    ShardSystemMutationEnvelope system_mutation = 3;
  }
}

message ShardSubject {
  bytes route_incarnation_uuid = 1;     // exactly 16 bytes
  uint32 partition = 2;
}

message DelayCommandEnvelope {
  uint32 envelope_version = 1;        // exactly 1
  bytes command_id = 2;               // canonical ndc1 binary
  bytes delay_message_id = 3;         // message/reservation Client Commands
  reserved 4;
  reserved "tenant_id";
  CommandType command_type = 5;
  reserved 6;
  reserved "command_sequence";
  int64 retry_until_epoch_ms = 7;
  bytes canonical_body = 8;
  bytes command_hash_sha256 = 9;       // 32 bytes
  uint32 body_version = 10;            // exactly 1
  reserved 11;
  reserved "shard_subject";
}

message ShardSystemMutationEnvelope {
  uint32 envelope_version = 1;
  bytes system_mutation_id = 2;
  ShardSubject shard_subject = 3;
  SystemMutationType mutation_type = 4;
  int64 retry_until_epoch_ms = 5;
  bytes canonical_body = 6;
  bytes mutation_hash_sha256 = 7;
  uint32 body_version = 8;
  bytes author_identity = 9;
  uint32 signing_key_version = 10;
  bytes signature = 11;
}
```

Message/reservation Client Commands use `delay_message_id`; the former Client-envelope shard-subject number/name is reserved. Privileged shard controls, Replay/Resolve, fences and runtime mutations use the signed System Mutation envelope with exact shard subject; a message locator appears only inside its canonical mutation body. The body repeats identity-sensitive locator/type/deadline fields needed for independent validation; envelope and body must match. Tenant authority is deliberately absent. The removed `command_sequence` field number and name are permanently reserved.

## Canonicalization

Body is the closed Protobuf field registry in the Protocol Registry, not the prose list below. It has no maps, no groups, no `Any`, no floating point, no duplicate singular fields, no unknown fields, bounded lengths/counts, explicit optional presence, and fixed enum validation. Repeated headers preserve their declared order and duplicates; set-like repeated fields are sorted by their specified byte comparator during preparation. Text fields define UTF-8 and normalization requirements individually; opaque keys and payloads remain bytes.

Every SDK/system writer uses the normative encoder and published registry vectors. The server parses with recursion/size limits, rejects non-minimal or duplicate wire encodings, deterministically re-encodes the decoded value, requires byte equality with `canonical_body`, and then verifies the applicable exact hash/signature preimage. System signatures additionally bind frame/log/envelope/body versions, kind/type, mutation ID/hash, exact shard, deadline, author and key version. Protobuf's generic “deterministic” option alone is not treated as a cross-version canonicalization guarantee.

## Operation bodies

The field numbers, types, required presence, oneofs and bounds for every following body and nested type are frozen in the Protocol Registry. This section is a semantic index only and cannot be used to add implementation-defined fields.

- `Schedule` contains requested Destination Profile/ref, delivery and ordering modes, `deliverAt`, `expireAt`, inline payload or committed payload descriptor, business key/ordering key/headers/event timestamp, and pinned/requested Retry Policy.
- `PrepareLargeSchedule` contains the complete immutable Schedule intent plus expected payload length/checksum and reservation lifetime. `CommitLargeSchedule` references that reservation, exact uploaded object identity, and the canonical service-signed `PayloadCommitProof`; upload handles and credentials are excluded.
- `Cancel` contains optional expected generation/Control Version.
- `Reschedule` contains expected state preconditions and new `deliverAt`/`expireAt`; it preserves payload, binding, ordering mode, and Retry Policy in the current design.

The signed System Mutation body set is closed:

- `ApplyShardControl` contains exact Control Operation ID/request hash, target index, control kind, semantic version/hash, expected prior control version and a matching closed `ControlPayload`. Its own expected mutation ID/hash are deliberately excluded and their body field names/numbers reserved; the completed marker's computed ID/hash are registered externally in the immutable Oxia target record, avoiding a circular hash.
- `ReplayDeadLetter` contains exact Control Operation, expected terminal generation/Control Version, new timing, pinned Retry Policy and explicit possible-duplicate acknowledgement when required.
- `ResolveUncertain` contains exact Control Operation, Lane/generation/attempt, evidence or override, and duplicate/possible-delivery acknowledgement.
- `TimeFence` contains exact Route Incarnation/partition, `closeThrough`, fence-proof key version, canonical `TrustedUtcIntervalEvidence` and its deterministic proof ID.
- Admission contains the full `PreparedPublishDescriptor`, hash, Ready Certificate/channel and reserve charge; Outcome and Evidence Resolution contain typed evidence, Trusted-UTC observation, closed retry decision and charge transfer. `ClaimResult` serializes the sole permanent pre-send terminal result, and `DlqExportResult` serializes each numbered deterministic-outbox attempt outcome or evidence resolution. Expiry, Resource Retire Intent and Resource Delete Confirmation use the exact closed types in the Registry and ADR-0039.

System signature coverage includes the outer kind, mutation ID/hash, shard subject, body version/body/hash, retry deadline, typed author and signing-key version. `ApplyShardControl` additionally binds every exact Oxia target field enumerated above.

Reserved destination-property names under `nereus.delay.*` cannot be supplied as business headers. Adapter-produced records bind the exact Delay Message Identity, generation, attempt/capability metadata, and Prepared Publish hash without exposing credentials.

## Evolution and rolling upgrade

Envelope/body/System Mutation versions are activated as the exact `(framingVersion, logEnvelopeVersion, recordKind, envelopeVersion, bodyVersion)` tuple by signed source-ordered Route markers only after every eligible Worker advertises read/apply support. A well-framed record whose outer identity is trusted but whose tuple is absent from the then-active writable set deterministically becomes position-level `REJECTED(UNACTIVATED_PROTOCOL_VERSION)` (or the system-specific unactivated code) and source advances. Only bytes whose frame/identity cannot be trusted are quarantined. A tuple activated by a preceding authenticated marker but unsupported by the current eligible Worker makes the shard `FAILED(reason=UNSUPPORTED_ACTIVATED_PROTOCOL)` and stops at that position as a deployment invariant violation.

Additive semantics require a new body version because unknown fields are forbidden in canonical bytes. Hash/signature domains include the version tuple and dedupe stores/compares it, so equal body bytes under two versions can never alias as an old duplicate. Removed Protobuf field names and numbers remain reserved. Store-format and Command-version compatibility are independent; a Worker must prove both before activation. Downgrade is allowed only while no route has admitted records or shard stores requiring the newer versions.

## Public results

Batch enqueue returns one independent outcome per Prepared Command. `CommandQueuedReceipt`, `CommandAppliedReceipt`, and `NativeDeliveryReceipt` are distinct types. All error responses carry a stable stage, code, retryability, relevant identities, and optional safe diagnostics; exception class or free-form text is never the protocol contract.
