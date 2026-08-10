# Nereus Delay V1 Protocol Registry

状态：Accepted / normative  
Spec revision：`V1-FROZEN-2026-08-01`  
兼容范围：只适用于 Nereus Delay V1；任何未列出的 enum、tag、field 或 code 都是 unknown

本文是 V1 的唯一数值注册表。主设计定义语义，ADR 记录理由；本文件固定实现必须逐 byte 一致的 framing、enum、canonical preimage、body field、RocksDB key 与 closed result tag。修改既有数字或语义不属于兼容变更，必须分配新 version/domain。

## 1. 通用编码

```text
u8/u16be/u32be/u64be = unsigned fixed-width big-endian
i64be                = two's-complement signed fixed-width big-endian
lp32(x)               = u32be(byteLength(x)) || x
bool                  = Protobuf bool，canonical value 只能 0 或 1
sha256                = 32 bytes
uuid                  = 16 RFC 9562 network-order bytes
DelayMessageId        = 41 bytes
CommandId             = 41 bytes
DestinationLaneId     = 32 bytes
ReservationId         = 32 bytes
SystemMutationId      = 32 bytes
ClaimId               = 32 bytes
PublishAttemptId      = 32 bytes
PhysicalEnqueueAttemptId = 16 nonzero cryptographic-random bytes, per physical submit
ProofId               = 32 bytes
generation            = u32；overflow 拒绝，禁止 wrap
version/epoch/counter = u64，除非表中明确为 u32
epoch milliseconds    = nonnegative i64 at API/wire；key 中编码为 u64be
```

文本默认必须是 valid UTF-8、NFC、无 NUL，最大 byte length 由 immutable Route/Profile limit 固定。opaque payload、key、header value、hash 和 identity 永不做 Unicode normalization。所有长度/加法/乘法先 checked；overflow 是 stable validation/integrity failure，不截断。

Canonical Protobuf V1 禁止 group、map、`Any`、float/double、unknown field、duplicate singular field、non-minimal varint、out-of-order field、非法 enum 和超过 Route limit 的递归/bytes/count。field 按 field number 严格递增编码；set-like repeated values 按本文指定 byte comparator 严格递增，Kafka header 这类 ordered repeated 保留输入顺序和重复。parse 后用 V1 encoder 重编码，bytes 必须相等。

## 2. Shard Log frame

`ShardLogFrameV1` is the following exact non-Protobuf byte layout; it contains one canonical `ShardLogEnvelopeV1` payload and no optional extension area:

```text
offset  size  field
0       4     magic = 4e 44 4c 31 (ASCII NDL1)
4       1     framingVersion = 01
5       1     recordKind
6       2     flags = 0000
8       4     payloadLength:u32be
12      N     canonical ShardLogEnvelope
12+N    4     CRC32C(header[0..11] || payload), u32be
```

总长必须等于 `16 + payloadLength` 且不超过 Route `maxShardLogPayloadBytes + 16`。`recordKind` 必须与 envelope oneof 一致。

| `RecordKindV1` | value |
|---|---:|
| `INVALID` | `0x00` |
| `CLIENT_COMMAND` | `0x01` |
| `SYSTEM_MUTATION` | `0x02` |

### 2.1 Operation enum

| `CommandTypeV1` | value |
|---|---:|
| `INVALID` | 0 |
| `SCHEDULE` | 1 |
| `PREPARE_LARGE_SCHEDULE` | 2 |
| `COMMIT_LARGE_SCHEDULE` | 3 |
| `CANCEL` | 4 |
| `RESCHEDULE` | 5 |

| `SystemMutationTypeV1` | value |
|---|---:|
| `INVALID` | 0 |
| `APPLY_SHARD_CONTROL_V1` | 1 |
| `REPLAY_DEAD_LETTER_V1` | 2 |
| `RESOLVE_UNCERTAIN_V1` | 3 |
| `TIME_FENCE_V1` | 4 |
| `PUBLISH_ADMISSION_V1` | 5 |
| `PUBLISH_OUTCOME_V1` | 6 |
| `EXPIRE_GENERATION_V1` | 7 |
| `EVIDENCE_RESOLUTION_V1` | 8 |
| `RESOURCE_RETIRE_INTENT_V1` | 9 |
| `RESOURCE_DELETE_CONFIRMED_V1` | 10 |
| `CLAIM_RESULT_V1` | 11 |
| `DLQ_EXPORT_RESULT_V1` | 12 |

Supporting body enums:

| enum | values |
|---|---|
| `DeliveryModeV1` | 1 `MANAGED` |
| `OrderingModeV1` | 1 `BEST_EFFORT`, 2 `DELIVERY_TIME_FIFO` |
| `ControlKindV1` | 1 `PROTOCOL_VERSION_ACTIVATE`, 2 `PROFILE_BINDING_ACTIVATE`, 3 `PROFILE_NEW_BINDING_CLOSE`, 4 `STOP_NEW_SCHEDULES`, 5 `GRANT_DECREASE_OR_HOLD`, 6 `GRANT_SHRINK_DRAINED`, 7 `GRANT_INCREASE_ACTIVATE`, 8 `PAUSE_DESTINATION_LANE`, 9 `RESUME_DESTINATION_LANE`, 10 `BREAK_ORDERING_DOMAIN`, 11 `CLOSE_DESTINATION_LANE`, 12 `PAYLOAD_PROOF_TRUST_SET_ACTIVATE`, 13 `PAYLOAD_PROOF_ISSUANCE_CLOSE`, 14 `INITIAL_ROUTE_CONTROL_ACTIVATE` |
| `UncertainResolutionKindV1` | 1 `ATTACH_PUBLISHED_EVIDENCE`, 2 `ATTACH_NOT_PUBLISHED_EVIDENCE`, 3 `RETRY_ALLOW_POSSIBLE_DUPLICATE`, 4 `TERMINALIZE_POSSIBLE_DELIVERY` |
| `ResourceKindV1` | 1 payload object, 2 checkpoint, 3 DLQ export object, 4 Kafka receipt slot, 5 Pulsar Attempt Journal generation, 6 Lane channel/producer identity, 7 local Store incarnation |
| `DeleteOutcomeV1` | 1 `DELETED`, 2 `ALREADY_ABSENT` |
| `EvidenceVerificationStatusV1` | 1 `VERIFIED_PUBLISHED`, 2 `VERIFIED_NOT_PUBLISHED`, 3 `UNRESOLVED` |
| `ChannelKindV1` | 1 `BASELINE_PRODUCER`, 2 `KAFKA_TRANSACTIONAL_RECEIPT`, 3 `PULSAR_DEDUP_PRODUCER`, 4 `PULSAR_NATIVE_DELAYED`, 5 `DLQ_EXPORT` |
| `PublishEvidenceKindV1` | 1 `KAFKA_PRODUCE_ACK`, 2 `KAFKA_TRANSACTIONAL_RECEIPT`, 3 `KAFKA_RECEIPT_ABSENCE`, 4 `PULSAR_SEND_ACK`, 5 `PULSAR_ATTEMPT_JOURNAL`, 6 `PULSAR_JOURNAL_ABSENCE`, 7 `BROKER_RESOURCE_GUARD_REJECTION`, 8 `OPERATOR_ATTESTATION`, 9 `ADAPTER_NON_SUBMISSION`, 10 `BROKER_DEFINITIVE_REJECTION` |
| `RetryDecisionKindV1` | 1 `NONE`, 2 `SCHEDULED`, 3 `EXHAUSTED`, 4 `LANE_WAIT`, 5 `UNCERTAIN_HOLD` |
| `RetryDomainV1` | 1 `MESSAGE_PUBLISH`, 2 `DLQ_EXPORT` |
| `ClaimResultKindV1` | 1 `DEAD_LETTER`; zero and 2–255 invalid in V1 |
| `DlqExportEventKindV1` | 1 `ATTEMPT_OUTCOME`, 2 `EVIDENCE_RESOLUTION` |
| `AdapterNonSubmissionKindV1` | 1 `BEFORE_LIBRARY_OWNERSHIP`, 2 `LIBRARY_CERTIFIED_CANCELED_BEFORE_OWNERSHIP` |
| `TimeEvidenceSourceV1` | 1 `KAFKA_LOG_APPEND_TIME`, 2 `PULSAR_BROKER_ENTRY_TIME`, 3 `CERTIFIED_HOST_CLOCK`, 4 `SIGNED_TIME_SERVICE` |
| `ControlReasonKindV1` | 1 `OPERATOR_REQUEST`, 2 `POLICY_CHANGE`, 3 `CAPABILITY_REPLACEMENT`, 4 `INCIDENT`, 5 `TENANT_OFFBOARD`, 6 `QUOTA_REALLOCATION`, 7 `MAINTENANCE` |
| `AcknowledgementKindV1` | 1 `POSSIBLE_DUPLICATE`, 2 `POSSIBLE_DELIVERY`, 3 `ORDER_LOSS` |
| `ClosePolicyV1` | 1 `V1_FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED` |
| `GuardOperationV1` | 1 `INGRESS_PRODUCE`, 2 `INGRESS_SUBSCRIBE`, 3 `TARGET_PRODUCE`, 4 `EVIDENCE_SUBSCRIBE`, 5 `EVIDENCE_PRODUCE`, 6 `DELETE_OR_HEAD` |
| `ReceiptKindV1` | 1 `COMMAND_QUEUED`, 2 `COMMAND_APPLIED`, 3 `NATIVE_DELIVERY`, 4 `PAYLOAD_RESERVATION`, 5 `CONTROL_OPERATION` |
| `SafeBrokerAckKindV1` | 1 `KAFKA_PRODUCE_ACK`, 2 `PULSAR_SEND_RECEIPT` |
| `FailureStageV1` | 1 `PREPARATION`, 2 `ENQUEUE`, 3 `APPLICATION`, 4 `QUERY`, 5 `PAYLOAD`, 6 `CONTROL`, 7 `PUBLISH`, 8 `RECOVERY`, 9 `INTEGRITY` |
| `RetryabilityV1` | 1 `NEVER`, 2 `RETRY_EXACT_BYTES`, 3 `RETRY_EXACT_BYTES_AFTER_RETRY_AT`, 4 `NEW_PREPARATION_REQUIRED`, 5 `RETRY_EXACT_BYTES_AFTER_EXTERNAL_CHANGE`, 6 `REREAD_AFTER_REPAIR` |
| `ReceiptCapabilityBitV1` | `0x0001 QUERY`, `0x0002 CANCEL`, `0x0004 RESCHEDULE`, `0x0008 SERVER_QUOTA`, `0x0010 SERVER_AUDIT`; all other bits invalid |
| `NonPersistenceProofKindV1` | 1 `LOCAL_BEFORE_PRODUCER_OWNERSHIP`, 2 `KAFKA_DEFINITIVE_REJECTION`, 3 `PULSAR_GUARD_REJECTION`, 4 `LIBRARY_CERTIFIED_PRE_OWNERSHIP_CANCEL` |
| `ProtectionKindV1` | 1 `RECOVERY_FLOOR`, 2 `QUERY_OR_AUDIT_RETENTION`, 3 `ACTIVE_ATTEMPT_OR_READ`, 4 `REPLAY_OR_RETRY_WINDOW`, 5 `EXPORT_OBLIGATION`, 6 `CONTROL_OPERATION` |
| `ProfileKindV1` | 1 `DESTINATION`, 2 `DELIVERY_CAPABILITY`, 3 `OBJECT_STORE`, 4 `EVIDENCE_VERIFIER` |
| `TargetPartitionPolicyV1` | 1 `EXPLICIT_ONLY`, 2 `HASH_ONLY`, 3 `EXPLICIT_OR_HASH` |
| `TargetPartitionHashInputV1` | 1 `ORDERING_KEY`, 2 `ADAPTER_MESSAGE_KEY`, 3 `DELAY_MESSAGE_ID` |
| `OutcomeCapabilityV1` | 1 `AT_LEAST_ONCE`, 2 `KAFKA_TRANSACTIONAL_RECEIPT`, 3 `PULSAR_BROKER_DEDUP` |
| `TimingCapabilityV1` | bit `0x01 ORDINARY_MANAGED`, `0x02 PULSAR_GUARDED_HANDOFF`, `0x04 PULSAR_AUTO_FAST`; all other bits invalid |
| `ObjectStoreProviderKindV1` | 1 `S3`, 2 `GCS`, 3 `AZURE_BLOB`, 4 `S3_COMPATIBLE` |
| `UncertainPolicyV1` | 1 `HOLD_FOR_EVIDENCE`, 2 `BOUNDED_RETRY_POSSIBLE_DUPLICATE`, 3 `BOUNDED_TERMINAL_POSSIBLE_DELIVERY` |
| `DlqExportModeV1` | 1 `NOT_CONFIGURED`, 2 `BASELINE_AT_LEAST_ONCE` |
| `ControlOperationKindV1` | 1 `STOP_NEW_SCHEDULES`, 2 `PAUSE_DESTINATION_LANE`, 3 `RESUME_DESTINATION_LANE`, 4 `CLOSE_DESTINATION_LANE`, 5 `BREAK_ORDERING_DOMAIN`, 6 `DRAIN_SHARD`, 7 `FENCE_SHARD_FOR_MAINTENANCE`, 8 `FORCE_CHECKPOINT`, 9 `GET_CHECKPOINT_CATALOG`, 10 `REPLAY_DEAD_LETTER`, 11 `RESOLVE_UNCERTAIN`, 12 `PUBLISH_DESTINATION_PROFILE_VERSION`, 13 `DEPRECATE_DESTINATION_PROFILE_VERSION`, 14 `PUBLISH_QUOTA_GRANT`, 15 `ROTATE_EQUIVALENT_SECRET_REFERENCE` |
| `ControlTargetKindV1` | 1 `SHARD`, 2 `LANE`, 3 `MESSAGE`, 4 `ROUTE`, 5 `PROFILE`, 6 `QUOTA_GRANT` |
| `ControlNonPersistenceProofKindV1` | 1 `BEFORE_OXIA_OWNERSHIP`, 2 `OXIA_CONDITIONAL_REJECTION` |
| `CapacityGrantKindV1` | 1 `OUTCOME_RESERVE`, 2 `NON_OUTCOME_CONTROL`, 3 `RECOVERY_WORKING`, 4 `EMERGENCY_HEADROOM` |
| `CredentialUseKindV1` | 1 `DESTINATION_CHANNEL`, 2 `OBJECT_STORE_ADAPTER` |
| `RecoveryCandidateKindV1` | 1 `LOCAL_STORE`, 2 `CATALOG_CHECKPOINT` |
| `CheckpointUploadStateV1` | 1 `PENDING_UPLOAD`, 2 `PUBLISHED`, 3 `REAPING` |
| `TimelineWorkKindV1` | 1 `INITIAL_SCHEDULE`, 2 `DEFINITIVE_RETRY`, 3 `UNCERTAIN_RETRY` |
| `CurrentSendWorkKindV1` | 1 `NONE`, 2 `TIMELINE`, 3 `CLAIMED`, 4 `PUBLISHING` |
| `UncertainRetryAuthorityV1` | 1 `NONE`, 2 `PINNED_POLICY`, 3 `CONTROL_OVERRIDE` |
| `LaneRecordKindV1` | 1 `ACTIVE_LANE`, 2 `TERMINAL_GUARD` |
| `AttemptLedgerStateV1` | 1 `PUBLISHING`, 2 `UNCERTAIN` |
| `LaneCircuitStateV1` | 1 `CLOSED`, 2 `OPEN`, 3 `HALF_OPEN` |
| `LaneRuntimeBlockReasonV1` | 1 `CAPABILITY`, 2 `CREDENTIAL_BINDING_DRIFT`, 3 `DESTINATION_INCARNATION_MISMATCH`, 4 `EVIDENCE_GAP`, 5 `CAPACITY`, 6 `ADAPTER_SAFETY`, 7 `TARGET_POLICY_DRIFT` |

Zero/unlisted values are invalid. Boolean acknowledgement fields do not replace a required acknowledgement hash/ticket scope.

## 3. Envelope fields

`ShardLogEnvelopeV1`：field 1 `uint32 log_envelope_version=1`；oneof field 2 `DelayCommandEnvelopeV1 client_command` / field 3 `ShardSystemMutationEnvelopeV1 system_mutation`。

`ShardSubjectV1`：field 1 `bytes route_incarnation_uuid` exactly 16；field 2 `uint32 partition`。

`DelayCommandEnvelopeV1`：

| field | type | name | V1 rule |
|---:|---|---|---|
| 1 | `uint32` | `envelope_version` | exactly 1 |
| 2 | `bytes` | `command_id` | exactly 41 |
| 3 | `bytes` | `delay_message_id` | exactly 41 |
| 4 | reserved | `tenant_id` | number and name permanently reserved |
| 5 | enum | `command_type` | `CommandTypeV1` |
| 6 | reserved | `command_sequence` | number and name permanently reserved |
| 7 | `int64` | `retry_until_epoch_ms` | nonnegative, formula checked |
| 8 | `bytes` | `canonical_body` | exact body table below |
| 9 | `bytes` | `command_hash_sha256` | exactly 32 |
| 10 | `uint32` | `body_version` | exactly 1 |
| 11 | reserved | `shard_subject` | number and name permanently reserved |

`ShardSystemMutationEnvelopeV1`：

| field | type | name | V1 rule |
|---:|---|---|---|
| 1 | `uint32` | `envelope_version` | exactly 1 |
| 2 | `bytes` | `system_mutation_id` | exactly 32 |
| 3 | message | `shard_subject` | required exact shard |
| 4 | enum | `mutation_type` | `SystemMutationTypeV1` |
| 5 | `int64` | `retry_until_epoch_ms` | nonnegative bounded epoch-ms deadline |
| 6 | `bytes` | `canonical_body` | exact body table below |
| 7 | `bytes` | `mutation_hash_sha256` | exactly 32 |
| 8 | `uint32` | `body_version` | exactly 1 |
| 9 | `bytes` | `author_identity` | canonical `AuthorIdentityV1`, closed tagged identity |
| 10 | `uint32` | `signing_key_version` | nonzero activated key |
| 11 | `bytes` | `signature` | Ed25519, exactly 64 |

## 4. Canonical hash and signature preimages

```text
commandHash = SHA-256(
  "nereus-delay-command-hash-v1\0" ||
  u8(framingVersion) || u32be(logEnvelopeVersion) ||
  u32be(envelopeVersion) || u32be(bodyVersion) ||
  u16be(commandType) || lp32(commandId) || lp32(delayMessageId) ||
  i64be(retryUntilEpochMs) || lp32(canonicalBody)
)

mutationHash = SHA-256(
  "nereus-delay-system-mutation-hash-v1\0" ||
  u8(framingVersion) || u32be(logEnvelopeVersion) ||
  u32be(envelopeVersion) || u32be(bodyVersion) ||
  u16be(systemMutationType) ||
  routeIncarnationUuid[16] || u32be(partition) ||
  i64be(mutationRetryUntilEpochMs) || lp32(canonicalBody)
)

systemMutationId = SHA-256(
  "nereus-delay-system-mutation-id-v1" ||
  u16be(systemMutationType) || lp32(logicalOperationIdentity) ||
  routeIncarnationUuid[16] || u32be(partition) || mutationHash
)

signatureDigest = SHA-256(
  "nereus-delay-system-mutation-signature-v1\0" ||
  u32be(0x4e444c31) || u8(framingVersion) || u8(0x02) ||
  u32be(logEnvelopeVersion) || u32be(envelopeVersion) ||
  u32be(bodyVersion) || u16be(systemMutationType) ||
  lp32(systemMutationId) || routeIncarnationUuid[16] || u32be(partition) ||
  i64be(mutationRetryUntilEpochMs) || lp32(canonicalBody) ||
  lp32(mutationHash) || lp32(authorIdentity) || u32be(signingKeyVersion)
)
```

`logicalOperationIdentity` is closed by mutation type:

```text
Apply/Replay/Resolve control target = SHA-256(
  "nereus-delay-control-target-logical-id-v1\0" ||
  controlOperationId[32] || u32be(targetIndex) || u16be(applicableTypeOrControlKind)
)
Time Fence       = ProofId[32]
Publish Admission = PublishAttemptId[32]
initial Publish Outcome = PublishAttemptId[32]
Claim Result = ClaimId[32]
Evidence Resolution = SHA-256(
  "nereus-delay-evidence-resolution-logical-id-v1\0" ||
  PublishAttemptId[32] || evidenceId[32]
)
Expiry = SHA-256(
  "nereus-delay-expiry-logical-id-v1" || DelayMessageId[41] ||
  u32be(generation) || i64be(expireAt)
)
Retire = SHA-256(
  "nereus-delay-retire-logical-id-v1" || u8(resourceKind) ||
  resourceIdentityHash[32] || u64be(expectedResourceStateVersion)
)
Delete Confirmation = referenced Retire Intent mutation ID[32]
DLQ Export attempt outcome = SHA-256(
  "nereus-delay-dlq-export-attempt-logical-id-v1\0" ||
  DlqExportId[32] || u32be(physicalAttemptNo)
)
DLQ Export evidence resolution = SHA-256(
  "nereus-delay-dlq-export-evidence-logical-id-v1\0" ||
  DlqExportId[32] || evidenceId[32]
)
```

For Apply, `applicableTypeOrControlKind=u16be(ControlKindV1)`; Replay/Resolve use their `SystemMutationTypeV1` number. One immutable Control Operation must assign a distinct target index to every staged marker, including decrease/drained/increase on the same shard. `PUBLISH_OUTCOME_V1` is the one initial callback/deadline classification for a business Publish Attempt; later evidence always uses `EVIDENCE_RESOLUTION_V1`, whose evidence ID makes each evidence event distinct. Likewise one Claim has at most one `CLAIM_RESULT_V1`; an Admission for that Claim races it by Source Position. A DLQ Export has exactly one `ATTEMPT_OUTCOME` for each checked `physicalAttemptNo` and any later proof is an `EVIDENCE_RESOLUTION` bound to the exact evidence ID. A logical identity is never a random enqueue-attempt ID. Signature is Ed25519 over `signatureDigest`. Any version/type/subject/deadline/identity/body change changes hash or signature.

### 4.1 Routing and Lane identity

`ROUTING_HASH_V1`：

```text
digest = SHA-256(
  "nereus-delay-routing-v1" ||
  lp32(routeIncarnationUuid[16]) ||
  lp32(tenantRoutingScope[32]) ||
  lp32(routingKey)
)
partition = unsigned-u64be(digest[0..7]) mod partitionCount
```

`TARGET_PARTITION_HASH_V1` is:

```text
digest = SHA-256(
  "nereus-delay-target-partition-v1" ||
  lp32(destinationProfileId) || u64be(destinationProfileVersion) ||
  lp32(profileSelectedRoutingBytes)
)
physicalPartition = unsigned-u64be(digest[0..7]) mod targetPartitionCount
```

The immutable Profile fixes which one of ordering key, message key, or Delay Message UUID bytes supplies `profileSelectedRoutingBytes`; an empty selected value is encoded as `u32be(0)`, never omitted.

Self-routing ID binary is exactly `01 | routeUuid[16] | partition:u32be | logicalUuidV7[16] | crc32c:u32be`; CRC covers the first 37 bytes. `ndm1_`/`ndc1_` text is unpadded Base64url of all 41 bytes and decodes to the applicable nominal type; prefixes are not interchangeable.

`AdapterKindV1`: 1 Kafka, 2 Pulsar。`BrokerResourceKindV1`: 1 Kafka native-topic UUID, 2 Pulsar protected token + physical-topic creation timestamp。`OrderingLaneKindV1`: 1 ordered domain hash, 2 unordered bucket。

`canonicalLaneTupleV1` is the following exact concatenation:

```text
tenantRoutingScope[32]
adapterKind:u8
lp32(authenticatedTargetClusterId)
brokerResourceKind:u8
  Kafka: nativeTopicUuid[16]
  Pulsar: nereusResourceIncarnation[32] | physicalTopicCreationTimestamp:u64be
lp32(physicalTopicIdentity)       // Kafka UUID bytes; Pulsar canonical full physical-topic UTF-8
physicalPartition:u32be
lp32(destinationProfileId) | destinationProfileVersion:u64be | profileSemanticHash[32]
lp32(capabilityProfileId) | capabilityProfileVersion:u64be | capabilitySemanticHash[32]
orderingLaneKind:u8
  ORDERED: orderingDomainHash[32]
  UNORDERED: unorderedBucket:u32be
```

The first Profile slot is valid only for `ProfileKindV1.DESTINATION`; the second is valid only for `ProfileKindV1.DELIVERY_CAPABILITY`. Their domain-separated semantic hashes already bind those kinds, so the kind byte is not duplicated in this Lane-tuple revision. A ref with the wrong kind is rejected before tuple construction rather than being reinterpreted by position.

```text
destinationLaneId = SHA-256(
  "nereus-delay-destination-lane-v1" || 01 || canonicalLaneTupleV1
)
```

No display name, delimiter, current endpoint or credential reference enters the tuple. Same tuple always yields the same terminally guarded Lane ID; continued traffic after Break/Close must change a semantic tuple component and therefore the Lane ID. `laneIncarnation` remains the first 128 bits of the separately domain-separated hash over Lane ID and canonical creating Source Position.

```text
laneIncarnation = first128Bits(SHA-256(
  "nereus-delay-lane-incarnation-v1\0" ||
  destinationLaneId[32] || lp32(canonicalSourcePosition)
))

unorderedBucket = unsigned-u64be(SHA-256(
  "nereus-delay-lane-v1" || delayMessageId[41]
)[0..7]) mod unorderedLaneBucketCount

reservationId = SHA-256(
  "nereus-delay-reservation-id-v1\0" ||
  commandId[41] || delayMessageId[41] || commandHash[32]
)

claimId = SHA-256(
  "nereus-delay-claim-id-v1\0" || storeIncarnation[16] ||
  u64be(ownerEpoch) || u64be(claimSequence) ||
  delayMessageId[41] || u32be(generation) || u64be(laneVersion)
)

publishAttemptId = SHA-256(
  "nereus-delay-publish-attempt-id-v1\0" || claimId[32] ||
  delayMessageId[41] || u32be(generation) || u32be(attemptNo)
)

dlqExportId = SHA-256(
  "nereus-delay-dlq-export-id-v1\0" || delayMessageId[41] ||
  u32be(generation) || u64be(terminalRevision)
)

resourceIdentityHash = SHA-256(
  "nereus-delay-resource-identity-v1\0" ||
  canonicalProtobuf(ExactResourceIdentityV1)
)

retryJitterDigest = SHA-256(
  "nereus-delay-retry-v1" || u8(retryDomain) || delayMessageId[41] ||
  u32be(generation) || u32be(attemptNo)
)
```

`claimSequence` is a checked, monotonically increasing per-Store value reserved and WAL-synced with the reversible Claim; it never rolls back within one Store Incarnation. A capacity-gated/stale Admission revokes that Claim, so a later Claim has a new Claim ID and Publish Attempt ID even when `attemptNo` was not consumed. An uncertain enqueue retries the original Claim/Attempt/mutation bytes. Overflow fences the shard; no ID wraps. `payloadRefId` is the applicable `resourceIdentityHash`.

## 5. Body V1 field registry

每个 Client body 先固定 common fields：field 1 `bytes delay_message_id` exactly 41；field 2 `CommandTypeV1 command_type`；field 3 `int64 retry_until_epoch_ms`。每个 System body 先固定：field 1 `ShardSubjectV1 shard_subject`；field 2 `SystemMutationTypeV1 mutation_type`；field 3 `int64 retry_until_epoch_ms`。三者必须等于 outer envelope。operation-specific fields 从 10 开始；1–9 其余 numbers reserved。

### 5.1 Closed nested types

| Type | exact fields |
|---|---|
| `ProfileRefV1` | 1 `bytes profile_id`；2 `uint64 version`；3 `bytes semantic_hash`=32；4 `ProfileKindV1 profile_kind` |
| `RetryPolicyRefV1` | 1 `bytes policy_id`；2 `uint64 version`；3 `bytes semantic_hash`=32 |
| `PayloadProofTrustSetRefV1` | 1 `uint64 version`；2 `bytes semantic_hash`=32 |
| `MessagePreconditionV1` | optional 1 `uint32 expected_generation`；optional 2 `uint64 expected_state_version`；presence required independently |
| `ControlRefV1` | 1 `bytes operation_id`=32；2 `bytes request_hash`=32；3 `uint32 target_index`；field numbers/names 4 `expected_mutation_id` and 5 `expected_mutation_hash` permanently reserved |
| `ChargeVectorV1` | u64 fields: 1 active messages, 2 pending payload bytes, 3 logical state bytes, 4 retained bytes, 5 reservation messages, 6 reservation payload bytes, 7 inflight messages, 8 inflight bytes, 9 result records, 10 result bytes, 11 System Mutation records, 12 System Mutation bytes, 13 outcome WAL bytes, 14 evidence records, 15 evidence bytes, 16 Lane count, 17 strong-Lane count；all fields required, zero explicit |
| `SourcePositionV1` | closed oneof field 1 `KafkaSourcePositionV1 kafka` / field 2 `PulsarSourcePositionV1 pulsar`；exact members below |
| `EvidenceCursorV1` | exact common fields and closed oneof defined below；ordering from §8 |
| `PublicEvidenceRefV1` | 1 `PublishEvidenceKindV1 evidence_type`；2 `bytes evidence_id`=32；3 `EvidenceVerificationStatusV1 verification_status`；no endpoint/object/signature |
| `ExternalDeliveryIdentityV1` | closed oneof field 1 `bytes publish_attempt_id`=32 / field 2 `bytes dlq_export_id`=32 |
| `AdapterMetadataV1` | closed oneof field 1 `KafkaMetadataV1`, field 2 `PulsarMetadataV1` |
| `KafkaMetadataV1` | optional 1 `bytes key` with explicit presence；repeated ordered 2 `KafkaHeaderV1`; header fields 1 `bytes name_utf8_nfc` and 2 `bytes value`; duplicates/order preserved |
| `PulsarMetadataV1` | optional 1 `bytes partition_key`; optional 2 enum key encoding (1 UTF-8, 2 Base64 bytes); optional 3 `bytes ordering_key`; repeated 4 `PulsarPropertyV1`, strictly UTF-8-key byte sorted and unique; property fields 1 `bytes key_utf8_nfc`/2 `bytes value_utf8_nfc` |
| `CommittedPayloadDescriptorV1` | 1 `ProfileRefV1 object_store_profile`；2 `bytes bucket_or_container`；3 `bytes object_key`；4 `bytes immutable_object_version`；optional 5 `bytes etag` with presence；6 `uint64 length`；7 `bytes payload_sha256`=32；8 ReservationId[32]；9 ProofId[32] |
| `OwnerIdentityV1` | 1 `bytes deployment_id`；2 `bytes worker_run_id`；3 `uint64 owner_epoch`；4 `bytes lease_fencing_digest`=32 |
| `AuthorIdentityV1` | closed oneof: field 1 `OwnerIdentityV1 owner`; field 2 `ControlAuthorV1 control`; field 3 `FenceWriterV1 fence`; field 4 `ServiceWriterV1 service` |
| `ControlAuthorV1` | 1 `bytes operation_actor_id_hash`=32；2 `bytes authenticated_role_set_hash`=32；3 `bytes tenant_resource_scope_hash`=32 |
| `FenceWriterV1` | 1 `bytes writer_id`；2 `uint64 config_generation` |
| `ServiceWriterV1` | 1 `bytes service_id`；2 `bytes service_run_id`；3 `uint64 config_generation` |
| `TrustedUtcIntervalEvidenceV1` | 1 `int64 earliest_epoch_ms`；2 `int64 latest_epoch_ms`；3 `TimeEvidenceSourceV1 source`；4 `bytes source_id`；5 `uint64 source_config_generation`；6 `uint64 sample_sequence`；7 `uint64 monotonic_anchor_ns`；8 `bytes source_evidence_sha256`=32；9 `uint32 source_key_version`；optional 10 `bytes source_signature`=64 |

`TrustedUtcIntervalEvidenceV1` requires `0 <= earliest <= latest`, interval width at most the activated clock bound, and either `(source=SIGNED_TIME_SERVICE, source_key_version>0, signature present)` or `(source!=SIGNED_TIME_SERVICE, source_key_version=0, signature absent)`. The outer signed System Mutation attests host-derived evidence; replay never samples a new clock.

`ChargeVectorV1` accounting is closed: field 1 counts each nonterminal Message Generation exactly once regardless of current work/attempt count; field 2 owns that Generation's payload bytes once; field 5/6 count each uncommitted Payload Reservation and its reserved payload once. Field 7 counts each reversible `inflight_cf/CLAIMED` record plus each `AttemptObligationRefV1` in `GenerationRuntimeIndexV1.attempt_obligations`; field 8 is their persisted `QUOTA_ACCOUNTING_V1` execution/attempt-byte charge, not a second payload-ownership charge. A terminal Generation releases fields 1/2 but keeps each open attempt in fields 7/8 and moves required history/object ownership into retained fields 3/4 until guarded GC. Timeline work alone never increments fields 7/8. All other fields count the exact record/obligation class named by the registry; no byte or record may satisfy two fields of the same grant.

`CapacityDimensionV1` is the following closed registry. Every amount is an unsigned committed capacity in the unit named by the symbol; vectors are compared and summed only dimension-by-dimension.

| value | dimension |
|---:|---|
| 1–17 | `ACTIVE_MESSAGES`, `PENDING_PAYLOAD_BYTES`, `LOGICAL_STATE_BYTES`, `RETAINED_BYTES`, `RESERVATION_MESSAGES`, `RESERVATION_PAYLOAD_BYTES`, `INFLIGHT_MESSAGES`, `INFLIGHT_BYTES`, `RESULT_RECORDS`, `RESULT_BYTES`, `SYSTEM_MUTATION_RECORDS`, `SYSTEM_MUTATION_BYTES`, `OUTCOME_WAL_BYTES`, `EVIDENCE_RECORDS`, `EVIDENCE_BYTES`, `LANE_COUNT`, `STRONG_LANE_COUNT`, in exactly the same order/meaning as `ChargeVectorV1` |
| 18 | `DB_INSTANCES` |
| 19 | `OPEN_FILES` |
| 20 | `WAL_BYTES` |
| 21 | `WAL_FILES` |
| 22 | `MANIFEST_BYTES` |
| 23 | `MANIFEST_FILES` |
| 24 | `LIVE_SST_BYTES` |
| 25 | `SST_FILES` |
| 26 | `MEMTABLE_BYTES` |
| 27 | `RESERVED_BLOCK_CACHE_BYTES` |
| 28 | `PINNED_CACHE_BYTES` |
| 29 | `PINNED_ITERATOR_BYTES` |
| 30 | `COMPACTION_PENDING_BYTES` |
| 31 | `L0_FILES` |
| 32 | `CHECKPOINT_CREATE_TEMP_BYTES` |
| 33 | `RESTORE_TEMP_BYTES` |
| 34 | `COMPACTION_TEMP_BYTES` |
| 35 | `BACKGROUND_JOBS` |
| 36 | `RESERVED_FLUSH_JOBS` |
| 37 | `RESERVED_CORRECTNESS_IO_BYTES_PER_SECOND` |
| 38 | `RESERVED_DUE_READ_OPS_PER_SECOND` |
| 39 | `RESERVED_EXPIRY_READ_OPS_PER_SECOND` |
| 40 | `ADAPTER_CONNECTIONS` |
| 41 | `ADAPTER_PRODUCERS` |
| 42 | `ADAPTER_THREADS` |
| 43 | `PHYSICAL_REQUESTS` |
| 44 | `PHYSICAL_BYTES` |
| 45 | `ZOMBIE_REQUESTS` |
| 46 | `ZOMBIE_BYTES` |
| 47 | `BUFFERED_MESSAGES` |
| 48 | `BUFFERED_BYTES` |
| 49 | `PUBLISH_MESSAGES` |
| 50 | `PUBLISH_BYTES` |
| 51 | `SYSTEM_WRITER_RESERVED_RECORDS` |
| 52 | `SYSTEM_WRITER_RESERVED_BYTES` |
| 53 | `SYSTEM_WRITER_RESERVED_BYTES_PER_SECOND` |
| 54 | `CONTROL_RESERVE_BYTES` |
| 55 | `CONTROL_RESERVE_RECORDS` |
| 56 | `ROCKSDB_NATIVE_BYTES` |
| 57 | `DIRECT_BUFFER_BYTES` |
| 58 | `OTHER_NATIVE_BYTES` |
| 59 | `QUERY_WAITERS` |
| 60 | `PAYLOAD_FETCHES` |
| 61 | `CHECKPOINT_CREATES` |
| 62 | `CHECKPOINT_UPLOADS` |
| 63 | `CHECKPOINT_DOWNLOADS` |
| 64 | `CHECKPOINT_IO_BYTES_PER_SECOND` |
| 65 | `OBJECT_REQUESTS` |
| 66 | `OBJECT_BYTES` |

`CapacityAmountV1` exact fields are 1 `CapacityDimensionV1 dimension`; 2 `uint64 amount`. `CapacityVectorV1` exact fields are 1 `uint32 accounting_version`=1; 2 repeated `CapacityAmountV1 amounts`, containing each registered dimension 1–66 exactly once in numeric order with zero explicit; 3 vector digest[32]. Field 3 is SHA-256 of domain `nereus-delay-capacity-vector-v1\0` plus canonical Protobuf fields 1–2. Unknown/missing/duplicate dimensions, saturating arithmetic or cross-dimension conversion are invalid.

`CapacityGrantV1` exact fields are 1 `CapacityGrantKindV1 kind`; 2 nonzero grant ID[32]; 3 nonzero `uint64 reserve_source_version`; 4 `CapacityVectorV1 vector`; 5 grant digest[32]. Field 5 is SHA-256 of domain `nereus-delay-capacity-grant-v1\0` plus canonical Protobuf fields 1–4. A component grant uses zero for inapplicable dimensions and cannot be borrowed by another shard/GrantKind.

`ShardCapacityEnvelopeV1` exact fields are 1 envelope schema version=1; 2 nonzero envelope ID[32]; 3 nonzero `uint64 envelope_version`; 4 `QuotaGrantRefV1 logical_grant`; 5 full committed `CapacityVectorV1`; 6 outcome `CapacityGrantV1`; 7 non-outcome-control `CapacityGrantV1`; 8 recovery-working `CapacityGrantV1`; 9 emergency-headroom `CapacityGrantV1`; 10 release capacity-artifact SHA-256[32]; 11 envelope digest[32]. Grant kinds in fields 6–9 must match their field roles and their per-dimension checked sum must be less than or equal to field 5; they are projections already included in field 5 and are never added a second time. `ChargeVectorV1` limits and reserve charges must byte-project to dimensions 1–17. Field 11 is SHA-256 of domain `nereus-delay-shard-capacity-envelope-v1\0` plus canonical Protobuf fields 1–10. Oxia placement, Owner Lease and `meta_cf` bind fields 2–3/11; a mismatch or unavailable referenced artifact/grant fails ownership before DB open.

#### 5.1.1 Immutable semantic objects

`DestinationProfileSemanticV1` exact fields: 1 `AdapterKindV1`; 2 target `BrokerResourceIdentityV1`; 3 `uint32 target_partition_count`; 4 `TargetPartitionPolicyV1`; 5 `TargetPartitionHashInputV1`; repeated 6 allowed explicit partitions, strictly numeric-sorted/unique and `< field 3`; 7 delivery-capability `ProfileRefV1`; 8 `uint32 allowed_ordering_mode_bits` (`0x01 BEST_EFFORT`, `0x02 DELIVERY_TIME_FIFO` only); 9 `uint64 handoff_lead_ms`; 10 `uint64 target_clock_ahead_bound_ms`; 11 immutable credential-authorization-scope/policy digest[32]; 12 `uint32 credential_binding_protocol_version`=1; 13 `uint64 max_target_record_bytes`; 14 `uint64 max_adapter_metadata_bytes`; 15 `uint64 max_payload_bytes`; 16 `uint32 unordered_lane_bucket_count`; 17 bounded public-safe destination alias UTF-8 NFC; 18 `uint64 minimum_topic_ttl_ms`; 19 `uint64 minimum_topic_retention_ms`; 20 `uint32 adapter_encoding_version`; 21 prerequisite-policy digest[32]. Field 7 must be kind `DELIVERY_CAPABILITY`, its semantic Adapter must equal field 1, and every destination reference to this envelope must be kind `DESTINATION`. Partition count/bucket count/limits are nonzero. `EXPLICIT_ONLY` requires a nonempty field 6 and ignores field 5; `HASH_ONLY` requires field 6 empty; `EXPLICIT_OR_HASH` requires nonempty field 6 and uses field 5 only when no permitted explicit partition was supplied. Kafka forbids timing bits other than ordinary and requires fields 9–10 zero; Pulsar handoff/auto-fast bits require the source-locked guards and bounds in the main spec. Strict ordering requires a non-baseline capability certified for the selected Adapter. Field 11 covers the exact target/evidence resources and operations the credential may authorize; changing that scope or policy requires a new Profile version.

`DeliveryCapabilitySemanticV1` exact fields: 1 `AdapterKindV1`; 2 `OutcomeCapabilityV1`; 3 `uint32 timing_capability_bits` from `TimingCapabilityV1`; optional 4 evidence `BrokerResourceIdentityV1`; 5 `uint32 evidence_partition_count`; 6 `uint64 minimum_evidence_retention_ms`; 7 `uint64 minimum_dedup_horizon_ms`; 8 `uint64 maximum_certified_producer_keys`; 9 Broker prerequisite digest[32]; 10 source-lock digest[32]; 11 `uint32 adapter_conformance_version`; 12 `uint32 rejection_classifier_version`. Baseline forbids evidence resource/count and uses zero evidence/dedup values; strong branches require the Adapter-specific main-spec fields and nonzero conformance/source-lock values. Kafka transactional receipt requires a Kafka evidence resource; Pulsar dedup requires a Pulsar Attempt Journal resource.

`ObjectStoreProfileSemanticV1` exact fields: 1 `ObjectStoreProviderKindV1`; 2 private endpoint/config digest[32]; 3 immutable credential-authorization-scope/policy digest[32]; 4 `uint32 credential_binding_protocol_version`=1; 5 `bool require_if_absent_create`; 6 `bool require_immutable_version`; 7 `bool require_exact_version_delete`; 8 `bool require_sha256_verification`; 9 encryption-policy digest[32]; 10 `uint64 max_object_bytes`; 11 `uint32 allowed_upload_handle_bits` (`0x01 single PUT`, `0x02 multipart`); 12 `uint32 adapter_conformance_version`; 13 lifecycle-policy digest[32]. All four booleans are true in V1 and limits/bits/conformance are nonzero. Changing endpoint/object scope, permissions, provider identity or field 3 requires a new Profile version.

`EvidenceVerifierProfileSemanticV1` exact fields: 1 verifier-kind `uint32` (1 Ed25519 operator attestation); 2 `uint32 key_version`; 3 public key[32]; 4 authenticated scope hash[32]; 5 `int64 not_before_epoch_ms`; 6 `int64 not_after_epoch_ms`; 7 verifier-policy digest[32]. Require `0 <= not_before < not_after`.

`ProfileSemanticEnvelopeV1` exact fields: 1 envelope version=1; 2 `ProfileKindV1`; 3 bounded profile ID bytes; 4 nonzero `uint64 version`; 5 body schema version=1; closed oneof field 10 destination, 11 delivery capability, 12 object store, 13 evidence verifier; 20 semantic hash[32]. Kind/branch numbers agree. All nested Profile refs use the globally unique `(profile_kind,profile_id,version)` namespace and may not be `latest`.

```text
profileSemanticHash = SHA-256(
  "nereus-delay-profile-semantic-v1\0" ||
  u16be(profileKind) || lp32(profileId) || u64be(version) ||
  u32be(bodySchemaVersion) || lp32(canonicalProtobuf(selectedBody))
)
```

Envelope field 20 and every `ProfileRefV1.semantic_hash` must equal this value; ref kind/ID/version must equal the envelope. Lifecycle, secret plaintext/reference/current generation, runtime health, endpoint discovery and mutable credentials are never in the semantic object. Changing any semantic field requires a new Profile version.

`CredentialEquivalenceAttestationV1` exact fields are 1 `ProfileRefV1 profile`; 2 nonzero proposed raw `uint64 secret_generation` (the complete 64-bit pattern is preserved; zero is invalid); 3 proposed secret-reference SHA-256[32]; 4 immutable authorization-scope/policy digest[32]; 5 resolved immutable credential-version/public-fingerprint digest[32]; 6 nonzero `uint32 verifier_version`; 7 bounded verifier ID bytes; 8 `TrustedUtcIntervalEvidenceV1 verified_at`; 9 `int64 not_after_epoch_ms`; 10 verification-evidence SHA-256[32]; 11 attestation digest[32]; 12 nonzero signing-key version `uint32`; 13 Ed25519 signature[64]. Field 9 must be greater than `verified_at.latest_epoch_ms`, within the configured maximum attestation age, and the verifier/key must be in the activated trust set. Field 11 is SHA-256 of domain `nereus-delay-credential-equivalence-v1\0` plus canonical Protobuf fields 1–10. Field 13 signs `SHA-256("nereus-delay-credential-equivalence-signature-v1\0" || field11 || u32be(field12))`. Field 4 must equal the selected Profile body's immutable credential scope digest; fields 1–3 must equal the candidate binding. Fields 5/10 bind the exact immutable provider secret version or public credential fingerprint and the verifier's authenticated principal/resource/operation probe evidence; operator assertion or a successful TCP connection is not sufficient. `not_after` is the deadline for accepting this control-plane proof, not permission to mutate the referenced secret later.

`CredentialBindingV1` is a separate private control-plane object with exact fields: 1 `ProfileRefV1 profile`; 2 nonzero raw `uint64 secret_generation` (complete 64-bit pattern, zero invalid); 3 bounded nonempty private `bytes secret_reference`; 4 `bytes secret_reference_sha256`=32; 5 `CredentialEquivalenceAttestationV1 equivalence_attestation`; 6 `uint32 binding_protocol_version`=1; 7 `bytes binding_digest`=32. Field 4 equals SHA-256(field 3); attestation fields 1–3 must equal binding fields 1–2/4. Field 7 is:

```text
credentialBindingDigest = SHA-256(
  "nereus-delay-credential-binding-v1\0" ||
  lp32(canonicalProtobuf(fields 1..6))
)
```

Each `CredentialBindingV1` is immutable at `/credential-bindings/<profileKind>/<profileId>/<profileVersion>/generations/<secretGeneration>`. `CredentialBindingHeadV1` at the sibling `/head` is the only mutable current pointer, with exact fields: 1 `ProfileRefV1 profile`; 2 nonzero current raw `uint64 secret_generation`; 3 current binding digest[32]; 4 nonzero checked `uint64 head_revision`; 5 head digest[32] over domain `nereus-delay-credential-binding-head-v1\0` plus canonical fields 1–4. The head and selected generation must byte-agree. Initial publication atomically creates generation 1, head revision 1 and its protection record. Rotation atomically verifies the expected head/generation/digest, creates exactly the checked next immutable generation and protection record, and advances the head/revision; response loss rereads these exact bytes and never creates a second generation.

`CredentialBindingProtectionV1` lives at sibling `/protections/<secretGeneration>` and has fields: 1 `ProfileRefV1 profile`; 2 nonzero raw `uint64 secret_generation`; 3 binding digest[32]; 4 nonnegative `int64 managed_channel_protection_until_epoch_ms`; 5 nonnegative `int64 object_store_lease_protection_until_epoch_ms`; 6 nonnegative `int64 native_capability_protection_until_epoch_ms`; 7 nonnegative `int64 upload_handle_protection_until_epoch_ms`; 8 nonzero checked `uint64 protection_revision`; 9 protection digest[32] over domain `nereus-delay-credential-binding-protection-v1\0` plus canonical fields 1–8. Fields 1–3 must identify the immutable generation. A credential-use lease, native snapshot or upload-handle issuer runs one Oxia transaction that compares the current Head generation/binding-digest/revision triplet to the candidate and updates the corresponding field 4/5/6/7 by checked monotonic max-CAS; it then durably observes that exact protection record. Thus protection-before-rotation authorizes the bounded old-generation lease/capability, while rotation-before-protection rejects the stale issuer and requires the new generation. CAS uncertainty rereads/retries the exact transaction intent and maximum. Decreasing any protection time, deleting a current generation, or deleting a generation with a live channel/Adapter lease, unresolved provider/Producer ownership, unexpired protection, unelapsed `bindingQuiescenceHorizon`, or required audit retention is forbidden. Rotation first checks the hard retained-generation budget; exhaustion is `HARD_QUOTA_EXCEEDED`, never forced deletion.

`CredentialUseLeaseV1` exact fields are 1 lease version=1; 2 `ProfileRefV1 profile`; 3 `CredentialUseKindV1 kind`; 4 holder-scope digest[32]; 5 nonzero raw `uint64 secret_generation`; 6 credential-binding digest[32]; 7 resolved immutable credential-version/public-fingerprint digest[32]; 8 `TrustedUtcIntervalEvidenceV1 issued_at`; 9 `int64 valid_until_epoch_ms`; 10 nonzero resulting protection revision `uint64`; 11 lease digest[32] over domain `nereus-delay-credential-use-lease-v1\0` plus canonical fields 1–10. The issuer first resolves the immutable reference, then uses the Head-compare/protection transaction above and emits no lease until it durably observes field 10 and a protection-until at least field 9. Fields 5–7 must equal that immutable binding and resolution. Require `issued_at.latest_epoch_ms < field9 <= checkedAdd(issued_at.earliest_epoch_ms, configured kind-specific lease TTL)`. `DESTINATION_CHANNEL` holder scope is SHA-256 of domain `nereus-delay-credential-holder-destination-channel-v1\0` plus canonical enclosing `ChannelResourceIdentityV1` fields 1–13; `OBJECT_STORE_ADAPTER` holder scope is SHA-256 of domain `nereus-delay-credential-holder-object-store-v1\0` plus canonical Profile ref, length-prefixed deployment/worker-run IDs and nonzero adapter-instance generation. A local call is authorized only while Trusted-UTC `latest < valid_until`, the activated Adapter still reports the same loaded credential fingerprint, holder/kind match, and library/provider ownership occurs within its configured local authorization age. No Oxia read is performed per message/provider call. Head rotation prevents renewal/new leases for the old generation but does not retroactively revoke an already protected lease.

Only `DESTINATION` and `OBJECT_STORE` Profiles have this object. Initial generation is 1. Rotation requires the checked raw-`uint64` successor `new_generation = expected_generation + 1`; the all-ones raw pattern cannot be incremented, and zero remains invalid. Field 3 must be an immutable provider-version-qualified reference, never `latest` or a mutable alias. Runtime resolution must return the exact immutable version/public fingerprint digest in attestation field 5. A Destination mismatch blocks only affected Lanes with `CREDENTIAL_BINDING_DRIFT`. An Object Store mismatch never becomes a source-pause reason by itself: handle/attestation returns the retryable Object Store outcome, payload fetch revokes only its reversible Claim, checkpoint publication retries while recovery-margin gates remain authoritative, restore stays `RESTORING`, and GC retains protection/quota and retries without declaring deletion. Every service-owned channel/provider call uses a protected bounded `CredentialUseLeaseV1`; lease acquisition/renewal, not each data call, is the Head-linearized boundary. Field 3 and attestation verifier evidence are never present in tenant/public results, logs, metrics, receipts, shard DBs, checkpoints or DLQ; those surfaces may expose only binding fields 2, 4 and 7 where authorized, while an exact signed `NativeCapabilitySnapshotV1` may additionally expose only the attested field-5 digest to its authenticated SDK scope. Profile publication and platform Object Store catalog publication atomically create an already-verified generation-1 binding before lifecycle activation. Equivalent rotation changes only this object, not Profile semantic hash, Destination Lane ID or an applied Destination Binding.

`RetryPolicySemanticV1` exact fields: 1 envelope version=1; 2 bounded policy ID bytes; 3 nonzero `uint64 version`; 4 `uint64 initial_backoff_ms`; 5 `uint64 max_backoff_ms`; 6 nonzero `uint32 max_publish_admissions`; 7 `uint64 max_retry_duration_ms`; 8 `UncertainPolicyV1`; 9 `uint32 max_uncertain_retries`; 10 `DlqExportModeV1`; 11 `uint64 dlq_initial_backoff_ms`; 12 `uint64 dlq_max_backoff_ms`; 13 `uint32 dlq_max_attempts`; 14 `uint64 dlq_max_retry_duration_ms`; 15 `bool dlq_allow_possible_duplicate`; 16 jitter algorithm version=1; 17 terminal-policy digest[32]; 18 semantic hash[32]. Require initial <= max, every enabled duration/attempt count nonzero, disabled DLQ fields 11–15 zero/false, and checked exponent/deadline arithmetic. `BOUNDED_RETRY_POSSIBLE_DUPLICATE` requires `0 < max_uncertain_retries < max_publish_admissions`; every other uncertain policy requires field 9 to be zero. A Schedule/Replay binding this policy to `DELIVERY_TIME_FIFO` is invalid: V1 uncertain retry is authorized only for `BEST_EFFORT`. Field 9 is the automatic pinned-policy budget. Every new durable Admission applied while an older `UNCERTAIN` attempt ledger is still present increments the Generation's total uncertain-retry Admission count; a `PINNED_POLICY` Admission additionally requires the pre-increment count below field 9. A source-ordered, acknowledged `CONTROL_OVERRIDE` may exceed field 9 but never field 6 or the time/expiry/capacity gates. Timeout, timeline insertion, Claim, enqueue retry of the same Admission, and definitive retry after the set becomes empty consume neither count.

```text
retryPolicySemanticHash = SHA-256(
  "nereus-delay-retry-policy-semantic-v1\0" ||
  lp32(policyId) || u64be(version) ||
  lp32(canonicalProtobuf(fields 1 and 4..17 with original field numbers))
)
```

Field 18 and `RetryPolicyRefV1.semantic_hash` must match. `PayloadProofTrustSetSemanticV1` exact fields are 1 version `uint64`; repeated 2 `PayloadProofVerifierKeyV1` strictly key-version sorted/unique (fields 1 nonzero `uint32 key_version`, 2 Ed25519 public key[32], 3 `int64 verify_not_before`, 4 `int64 verify_not_after`); 3 semantic hash[32]. Its hash is SHA-256 of domain `nereus-delay-payload-trust-set-semantic-v1\0` plus version and canonical repeated key list; `PayloadProofTrustSetRefV1` must match. Issuance-open/close is source-ordered control state, not mutable inside the historical verifier set.

`QuotaGrantRefV1.grant_semantic_hash` equals SHA-256 of domain `nereus-delay-quota-grant-semantic-v1\0` plus grant ID[32], grant version and canonical `ChargeVectorV1 limit`. These formulas, not an Oxia serializer or display JSON, define semantic equality.

`KafkaSourcePositionV1` exact fields: 1 Route UUID[16]; 2 authenticated cluster ID bytes; 3 native topic UUID[16]; 4 partition `uint32`; 5 offset `uint64`; optional 6 leader epoch `uint32`; 7 broker log-append time `int64`. `PulsarSourcePositionV1`: 1 Route UUID[16]; 2 Broker resource incarnation bytes; 3 canonical physical-topic UTF-8 bytes; 4 partition `uint32`; 5 ledger ID `uint64`; 6 entry ID `uint64`; 7 normalized batch index `uint32`; 8 batch size `uint32`; 9 entry kind (1 non-batch, 2 batch); 10 broker entry timestamp `int64`. A decoded Source Position must byte-round-trip to the exact input; malformed UTF-8 or any non-canonical alternate encoding is rejected before persistence.

`ProtocolTupleV1` exact fields: 1 `uint32 framing_version`; 2 `uint32 log_envelope_version`; 3 `RecordKindV1 record_kind`; 4 `uint32 envelope_version`; 5 `uint32 body_version`.

`BrokerResourceIdentityV1` is a closed oneof: field 1 `KafkaResourceIdentityV1 kafka`, whose fields are 1 `bytes authenticated_cluster_id`, 2 `bytes native_topic_uuid`=16; or field 2 `PulsarResourceIdentityV1 pulsar`, whose fields are 1 `bytes authenticated_cluster_id`, 2 `bytes nereus_resource_incarnation`=32, 3 `bytes canonical_physical_topic_utf8`, 4 `uint64 physical_topic_creation_timestamp`. The selected adapter must match the branch.

`ActivationBarrierV1` is a closed oneof: field 1 `EmptyBarrierV1 empty`; field 2 `KafkaExclusiveOffsetV1 kafka`; field 3 `PulsarInclusiveMessageIdV1 pulsar`. `EmptyBarrierV1` fields are 1 `BrokerResourceIdentityV1 resource`, 2 `uint32 partition`, optional 3 `uint64 guarded_source_connection_generation`, optional 4 resource-guard attestation digest[32]; fields 3–4 are required together for Pulsar and forbidden for Kafka. `KafkaExclusiveOffsetV1` fields are 1 resource, 2 partition, 3 `uint64 next_offset_exclusive`, 4 `uint64 observed_lso_exclusive`. `PulsarInclusiveMessageIdV1` fields are 1 resource, 2 partition, 3 `uint64 ledger_id`, 4 `uint64 entry_id`, 5 `uint32 normalized_batch_index`, 6 `uint32 batch_size`, 7 `uint64 guarded_source_connection_generation`, 8 resource-guard attestation digest[32]. The Pulsar values must come from the same still-valid guarded consumer connection; a name-only/admin MessageId cannot populate this type.

`EvidenceCursorV1` fields are: 1 `EvidenceKindV1 evidence_kind`; 2 `bytes destination_lane_id`=32; 3 `bytes lane_incarnation`=16; 4 `bytes evidence_resource_incarnation`; 5 `uint32 physical_partition`; 6 `uint64 evidence_generation`; 7 `int64 max_broker_persisted_at_through_cursor`; oneof field 10 `KafkaReceiptCursorV1 kafka` / field 11 `PulsarJournalCursorV1 pulsar`. Kafka fields are 1 `bytes topic_uuid`=16, 2 `uint64 next_offset_exclusive`, 3 `uint64 last_observed_lso_exclusive`. Pulsar fields are 1 `bytes resource_token`=32, 2 `bytes canonical_physical_topic_utf8`, 3 `uint64 physical_topic_creation_timestamp`, 4 `uint64 ledger_id`, 5 `uint64 entry_id`, 6 `uint32 normalized_batch_index`, 7 `uint32 batch_size`. Kind and branch must agree; common resource bytes must equal the branch's canonical resource-incarnation encoding.

`ChannelResourceIdentityV1` exact fields: 1 `AdapterKindV1 adapter_kind`; 2 `ChannelKindV1 channel_kind`; 3 `bytes destination_lane_id`=32; 4 `bytes lane_incarnation`=16; 5 `BrokerResourceIdentityV1 target_resource`; 6 `uint32 physical_partition`; 7 `uint64 channel_generation`; 8 `uint32 channel_slot`; 9 `bytes producer_or_transactional_identity`; 10 `bytes producer_or_transactional_identity_sha256`=32; optional 11 `BrokerResourceIdentityV1 evidence_resource`; optional 12 `uint64 evidence_generation`; 13 `bytes resource_guard_attestation_digest`=32; 14 nonzero raw `uint64 credential_binding_generation`; 15 `bytes credential_binding_digest`=32; 16 `bytes resolved_credential_version_fingerprint_digest`=32; 17 `CredentialUseLeaseV1 credential_use_lease`. Fields 7, 12 and 14 carry complete raw unsigned 64-bit patterns; zero is invalid, but a host signed-integer high bit is not a decode error. Field 10 must equal SHA-256(field 9); evidence fields are both present exactly for a channel kind that requires them. Lease kind must be `DESTINATION_CHANNEL`; fields 14–16 must equal lease fields 5–7, the enclosing holder-scope formula must match, and the certificate validity cannot outlive the lease. Replacing/renewing field 17 requires a checked-incremented channel generation and a new entire identity; it can never mutate one generation in place. Before every first physical Producer call, the Worker validates the live lease/loaded fingerprint locally and closes the gate-to-library-ownership interval within `maximumCredentialAuthorizationToProducerCallAge`. A mismatch cannot reuse or silently relabel the channel: the Lane first loses READY and becomes `BLOCKED(CREDENTIAL_BINDING_DRIFT)`.

`PayloadForPublishV1` exact fields: 1 `uint64 length`; 2 `bytes payload_sha256`=32; closed oneof field 3 `bytes inline_payload` / field 4 `CommittedPayloadDescriptorV1 object`. Inline bytes must match length/hash; an object descriptor must match the same length/hash.

`ClaimMaterializationV1` exact fields: 1 destination `ProfileRefV1`; 2 capability `ProfileRefV1`; 3 `BrokerResourceIdentityV1 target_resource`; 4 physical partition `uint32`; 5 DelayMessageId[41]; 6 generation `uint32`; 7 `PayloadForPublishV1 payload`; 8 `AdapterMetadataV1 business_metadata`; 9 `int64 deliver_at`; 10 `int64 expire_at`; 11 `int64 action_at`. Its digest is `SHA-256("nereus-delay-claim-materialization-v1\0" || canonicalProtobuf(ClaimMaterializationV1))`.

`AttemptObligationRefV1` exact fields: 1 PublishAttemptId[32]; 2 `uint32 generation`; 3 `AttemptLedgerStateV1 ledger_state`; 4 `bytes encoded_inflight_key`; 5 `bytes inflight_key_sha256`=32; 6 `bytes ref_digest`=32. Field 4 is the exact §7 PUBLISHING or UNCERTAIN key, including admitted Owner Epoch and field-1 ID; its tag must match field 3, and field 5 is SHA-256(field 4). Field 6 is `SHA-256("nereus-delay-attempt-obligation-ref-v1\0" || canonicalProtobuf(fields 1–5))`. Sets are strictly byte-sorted/unique by `(publishAttemptId, encoded_inflight_key)` and forbid two refs with one ID.

`ClaimPreconditionV1` exact fields: 1 ClaimId[32]; 2 DelayMessageId[41]; 3 generation `uint32`; 4 message state version `uint64`; 5 DestinationLaneId[32]; 6 Lane Incarnation[16]; 7 lane control version `uint64`; 8 runtime lane version `uint64`; 9 original timeline-key SHA-256[32]; optional 10 `ClaimMaterializationV1 materialization`; optional 11 materialization digest[32]; 12 `ChargeVectorV1 claimed_charge`; 13 `int64 claim_deadline`; 14 `OwnerIdentityV1 owner`; 15 Store Incarnation[16]; 16 `TimelineWorkKindV1 source_work_kind`; 17 `uint32 expected_admissions_used`; 18 `uint32 expected_uncertain_retry_admissions_used`; 19 `bytes expected_attempt_obligation_set_digest`=32; 20 `bytes source_timeline_semantic_digest`=32. Fields 10–11 are present or absent together; when present, field 11 must match the formula. Admission requires them present and every duplicated value in Admission/descriptor byte-equal. A failed pre-send Claim Result may omit them only when failure occurred before complete materialization. Field 19 is `SHA-256("nereus-delay-attempt-obligation-set-v1\0" || concat(canonicalProtobuf(byte-sorted unique AttemptObligationRefV1)))`; fields 16–20 must equal the timeline semantic work and `GenerationRuntimeIndexV1` from which the Claim was taken. The Claim record additionally retains the source `TimelineWorkRefV1.work_instance_digest` for local snapshot fencing, but that local digest is deliberately absent from the source-replay precondition.

Fields 8 and 14–15 are historical Claim/live-gate identity, not apply-time current-state comparators. With the exact local Claim, local preparation/first-send checks all fields and the retained instance digest. If replay reconstructs after Owner/Store/runtime Lane version changed, it validates the historical signature/identity internally but compares current source-derived state only to fields 2–7, 9, 16–20 and the descriptor/materialization equalities; a current field-8/14/15 mismatch cannot reject an already Broker-persisted record. Any intervening source-ordered change to that replay-stable subset, counters or obligation set makes the record stale. A pure recovery/requeue that changes only Owner/Store/runtime Lane/work-instance identity does not.

`ReservedPublishMetadataV1` exact fields: 1 Route UUID[16]; 2 shard partition `uint32`; 3 DelayMessageId[41]; 4 generation `uint32`; 5 PublishAttemptId[32]; 6 destination Profile semantic hash[32]; 7 capability Profile semantic hash[32]; 8 `int64 deliver_at`; 9 `DeliveryModeV1 delivery_mode`. Caller metadata cannot contain a reserved name.

`PreparedPublishDescriptorV1` exact fields: 1 `uint32 descriptor_version`=1; 2 `AdapterKindV1 adapter_kind`; 3 `uint32 adapter_encoding_version`=1; 4 `bytes destination_lane_id`=32; 5 `bytes lane_incarnation`=16; 6 `ProfileRefV1 destination_profile`; 7 `ProfileRefV1 capability_profile`; 8 `BrokerResourceIdentityV1 target_resource`; 9 `uint32 physical_partition`; 10 `ChannelResourceIdentityV1 channel`; 11 DelayMessageId[41]; 12 generation `uint32`; 13 PublishAttemptId[32]; 14 `uint32 attempt_no`; 15 `PayloadForPublishV1 payload`; 16 `AdapterMetadataV1 business_metadata`; 17 `ReservedPublishMetadataV1 reserved_metadata_without_hash`; 18 `int64 deliver_at`; 19 `int64 expire_at`; 20 `int64 action_at`. Every duplicated identity/time field must be byte-equal across the descriptor and nested objects. Ordinary managed requires `action_at=deliver_at`; certified Pulsar handoff requires the exact pinned timing-policy derivation.

```text
preparedPublishHash = SHA-256(
  "nereus-delay-prepared-publish-v1\0" ||
  canonicalProtobuf(PreparedPublishDescriptorV1)
)
```

The Broker record appends the hash after computing it. Kafka appends, after caller headers, the following UTF-8 header names in this exact order with opaque values: `nereus.delay.route`=Route UUID[16], `nereus.delay.partition`=u32be, `nereus.delay.message_id`=41 bytes, `nereus.delay.generation`=u32be, `nereus.delay.attempt_id`=32 bytes, `nereus.delay.destination_profile_hash`=32 bytes, `nereus.delay.capability_profile_hash`=32 bytes, `nereus.delay.deliver_at`=i64be, `nereus.delay.prepared_hash`=32 bytes. Pulsar adds the same names as unique properties; byte values are unpadded Base64url and integer values are unsigned/signed canonical decimal. These are the only V1 `nereus.delay.*` fields.

`ReadyCertificateV1` exact fields: 1 `uint32 certificate_version`=1; 2 `OwnerIdentityV1 owner`; 3 Store Incarnation[16]; 4 DestinationLaneId[32]; 5 Lane Incarnation[16]; 6 `ChannelResourceIdentityV1 channel`; 7 `ActivationBarrierV1 activation_barrier`; 8 repeated `EvidenceCursorV1 evidence_cursors`, strictly sorted by §8; 9 `uint64 broker_resource_attestation_generation`; 10 `uint64 config_generation`; 11 `int64 valid_until_epoch_ms`; 12 `TrustedUtcIntervalEvidenceV1 issued_at`; 13 nonzero raw `uint64 credential_binding_generation`; 14 `bytes credential_binding_digest`=32; 15 `bytes resolved_credential_version_fingerprint_digest`=32; 16 `bytes certificate_digest`=32. Fields 13–15 must byte-equal channel fields 14–16, and field 11 must be no later than channel lease field 9. Field 16 is SHA-256 of domain `nereus-delay-ready-certificate-v1\0` plus canonical fields 1–15. For a new Claim, Admission preparation, or first Producer call, it is live-authorizing only while every referenced generation and the protected Credential Use Lease remain locally valid, the loaded credential fingerprint still equals field 15, and the applicable safe-time predicate holds. A later equivalent Head rotation does not retroactively revoke the bounded lease, though its notification removes READY early and prevents renewal. Once embedded in an Admission record, the certificate is historical decision evidence: apply/replay validates its digest, captured generations, decision interval, and Broker persistence-time inequalities, but a later Owner/channel/config/credential generation cannot retroactively invalidate that record.

`PublishEvidenceV1` exact fields: 1 `PublishEvidenceKindV1 evidence_kind`; 2 `EvidenceVerificationStatusV1 verification_status`; 3 `bytes evidence_id`=32; closed oneof fields 10–19 in the following table. `evidence_id = SHA-256("nereus-delay-publish-evidence-v1\0" || u16be(evidence_kind) || u16be(verification_status) || canonicalProtobuf(selected branch))`. A `VERIFIED_PUBLISHED`/`VERIFIED_NOT_PUBLISHED` status must match the branch semantics; `UNRESOLVED` is not valid in a mutation that claims a definitive side effect. The branch's `ExternalDeliveryIdentityV1` must select and equal the owning `PublishAttemptId` for business Outcome/Resolution or `DlqExportId` for DLQ Export Result; cross-kind evidence is invalid.

| oneof field | branch | exact branch fields |
|---:|---|---|
| 10 | `KafkaProduceAckEvidenceV1` | 1 target `BrokerResourceIdentityV1`; 2 partition `uint32`; 3 base offset `uint64`; optional 4 leader epoch `uint32`; 5 broker log-append time `int64`; 6 `ExternalDeliveryIdentityV1`; 7 exact prepared/export envelope hash[32]; 8 authenticated response SHA-256[32] |
| 11 | `KafkaTransactionalReceiptEvidenceV1` | 1 `EvidenceCursorV1`; 2 receipt offset `uint64`; 3 `ExternalDeliveryIdentityV1`; 4 exact prepared/export envelope hash[32]; 5 target `BrokerResourceIdentityV1`; 6 target partition `uint32`; 7 transactional identity SHA-256[32]; 8 canonical receipt-record SHA-256[32] |
| 12 | `KafkaReceiptAbsenceEvidenceV1` | 1 `EvidenceCursorV1`; 2 `ChannelResourceIdentityV1` after fencing; 3 `ExternalDeliveryIdentityV1`; 4 exact prepared/export envelope hash[32]; 5 exact fence-and-LSO barrier SHA-256[32] |
| 13 | `PulsarSendAckEvidenceV1` | 1 target `BrokerResourceIdentityV1`; 2 partition `uint32`; 3 ledger ID `uint64`; 4 entry ID `uint64`; 5 normalized batch index `uint32`; 6 broker persistence time `int64`; 7 producer-name SHA-256[32]; 8 sequence ID `uint64`; 9 `ExternalDeliveryIdentityV1`; 10 exact prepared/export envelope hash[32]; 11 authenticated response SHA-256[32] |
| 14 | `PulsarAttemptJournalEvidenceV1` | 1 `EvidenceCursorV1`; 2 journal ledger ID `uint64`; 3 journal entry ID `uint64`; 4 normalized batch index `uint32`; 5 `ExternalDeliveryIdentityV1`; 6 exact prepared/export envelope hash[32]; 7 producer-name SHA-256[32]; 8 sequence ID `uint64`; 9 mapping-record SHA-256[32]; optional 10 target-ack evidence ID[32] |
| 15 | `PulsarJournalAbsenceEvidenceV1` | 1 `EvidenceCursorV1`; 2 fenced `ChannelResourceIdentityV1`; 3 `ExternalDeliveryIdentityV1`; 4 exact prepared/export envelope hash[32]; 5 producer-name SHA-256[32]; 6 sequence ID `uint64`; 7 exact retirement-barrier SHA-256[32] |
| 16 | `BrokerResourceGuardRejectionEvidenceV1` | 1 target `BrokerResourceIdentityV1`; 2 partition `uint32`; 3 `GuardOperationV1 operation`; 4 attestation generation `uint64`; 5 attestation digest[32]; 6 exact request SHA-256[32]; 7 `StableCodeV1 guard_code`; 8 authenticated response SHA-256[32]; 9 `ExternalDeliveryIdentityV1`; 10 exact prepared/export envelope hash[32] |
| 17 | `OperatorAttestationEvidenceV1` | 1 verifier `ProfileRefV1`; 2 `ExternalDeliveryIdentityV1`; 3 exact prepared/export envelope hash[32]; 4 target `BrokerResourceIdentityV1`; 5 partition `uint32`; 6 verification status; 7 issued-at `int64`; 8 not-after `int64`; 9 payload SHA-256[32]; 10 key version `uint32`; 11 Ed25519 signature[64] |
| 18 | `AdapterNonSubmissionEvidenceV1` | 1 `ChannelResourceIdentityV1`; 2 `ExternalDeliveryIdentityV1`; 3 exact prepared/export envelope hash[32]; 4 `AdapterNonSubmissionKindV1`; 5 exact local request SHA-256[32]; 6 activated Adapter conformance version `uint32`; 7 `StableCodeV1`; valid only as `VERIFIED_NOT_PUBLISHED` |
| 19 | `BrokerDefinitiveRejectionEvidenceV1` | 1 `AdapterKindV1`; 2 target `BrokerResourceIdentityV1`; 3 partition `uint32`; 4 `ExternalDeliveryIdentityV1`; 5 exact prepared/export envelope hash[32]; 6 exact Broker request SHA-256[32]; 7 unsigned wire error code `uint32`; 8 authenticated response SHA-256[32]; 9 activated rejection-classifier version `uint32`; valid only as `VERIFIED_NOT_PUBLISHED` |

`RetryDecisionV1` exact fields: 1 `RetryDecisionKindV1 kind`; 2 `RetryPolicyRefV1 policy`; 3 `uint32 completed_attempt_no`; 4 `int64 first_attempt_at`; 5 `int64 retry_deadline`; optional 6 `int64 next_retry_at`; 7 `uint32 jitter_algorithm_version` (1 for `RETRY_JITTER_V1`); 8 `StableCodeV1 cause`; 9 `RetryDomainV1 retry_domain`. `SCHEDULED` and `LANE_WAIT` require field 6; `NONE`, `EXHAUSTED`, and `UNCERTAIN_HOLD` forbid it. Business Outcome/Resolution requires `MESSAGE_PUBLISH`; DLQ result requires `DLQ_EXPORT`. Apply recomputes the domain-separated V1 jitter/deadline from prior state and the accompanying trusted interval; mismatch is an integrity failure.

`TimelineWorkRefV1` exact fields: 1 `TimelineWorkKindV1 work_kind`; 2 `bytes encoded_timeline_key`; 3 `bytes timeline_key_sha256`=32; 4 `int64 action_at`; 5 `int64 retry_eligibility_at`; 6 nonzero `uint32 candidate_attempt_no`; 7 nonzero `uint64 runtime_revision`; 8 `bool ordered_head_blocking`; 9 `UncertainRetryAuthorityV1 uncertain_retry_authority`; optional 10 `ControlRefV1 uncertain_retry_control`; optional 11 `SourcePositionV1 uncertain_retry_control_position`; 12 `bytes semantic_work_digest`=32; 13 `bytes work_instance_digest`=32. Field 2 is exactly one complete §7 `timeline/DUE` or `timeline/ORDERED` key and field 3 equals its SHA-256. Field 12 is `SHA-256("nereus-delay-timeline-work-semantic-v1\0" || canonicalProtobuf(fields 1–6,8–11))`, deliberately excluding local field 7. Field 13 is `SHA-256("nereus-delay-timeline-work-instance-v1\0" || canonicalProtobuf(fields 1–12))` and therefore fences a particular runtime revision. `INITIAL_SCHEDULE` requires candidate 1, `retry_eligibility_at=action_at`, authority NONE and fields 10–11 absent. `DEFINITIVE_RETRY` requires the Generation's attempt-obligation set empty, authority NONE and fields 10–11 absent. `UNCERTAIN_RETRY` requires `BEST_EFFORT`, a nonempty set containing at least one `UNCERTAIN` ledger, and remaining Admission/time budgets. Its `PINNED_POLICY` branch forbids fields 10–11, requires remaining automatic uncertain budget and uses the applied `RetryDecisionV1.next_retry_at`; its `CONTROL_OVERRIDE` branch requires fields 10–11, remaining max-Admission budget, and `retry_eligibility_at=max(action_at, field11 Broker timestamp)`. The ControlRef must identify the exact authenticated `ResolveUncertainV1(RETRY_ALLOW_POSSIBLE_DUPLICATE)` record at field 11 with duplicate acknowledgement. `ordered_head_blocking` is true exactly for `timeline/ORDERED`, which therefore cannot be UNCERTAIN_RETRY. For DUE, the key eligibility equals `max(action_at,retry_eligibility_at)`; for ORDERED, the key business time remains `deliverAt` and Lane READY uses that same maximum from the value.

`GenerationRuntimeIndexV1` is the exact `id_cf/MESSAGE` current-generation runtime payload: 1 version=1; 2 `MessageGenerationStateV1 aggregate_state`; 3 `CurrentSendWorkKindV1 current_work_kind`; closed oneof 10 `TimelineWorkRefV1 timeline` / 11 ClaimId[32] / 12 PublishAttemptId[32]; 13 repeated `AttemptObligationRefV1 attempt_obligations`, canonically sorted/unique; 14 `uint32 admissions_used`; 15 `uint32 uncertain_retry_admissions_used`; 16 `bool possible_destination_duplicate`; 17 nonzero `uint64 runtime_revision`; 18 `bytes runtime_digest`=32. The selected oneof branch is absent for `NONE` and otherwise exactly matches field 3. Field 18 is `SHA-256("nereus-delay-generation-runtime-index-v1\0" || canonicalProtobuf(fields 1–17))`. Every ref in field 13 resolves by exact key to one byte-matching `inflight_cf/PUBLISHING|UNCERTAIN` attempt ledger of the same Message/generation/ID/state; a current PUBLISHING ID must have one matching ref. Its cardinality and fields 14–15 are bounded by the pinned `max_publish_admissions`, and field 15 cannot exceed field 14. Pinned-policy Admission additionally enforces field 15 below `max_uncertain_retries` before increment; a bound authenticated Control override may exceed that automatic limit but not the total Admission/time/capacity bounds.

For a nonterminal Generation, at most one new send work exists. With no `UNCERTAIN` ledger, aggregate state is the current work projection (`INITIAL_SCHEDULE→SCHEDULED`, `DEFINITIVE_RETRY→RETRY_WAIT`, `CLAIMED→CLAIMED`, `PUBLISHING→PUBLISHING`). If any historical attempt ledger is `UNCERTAIN`, aggregate state is `UNCERTAIN` even while the current work is TIMELINE, CLAIMED, or PUBLISHING. A terminal aggregate requires current work `NONE`, but field 13 may retain already-admitted PUBLISHING/UNCERTAIN obligations until their exact Outcome/evidence and guarded GC finish; while this terminal Generation remains the Message Identity's current generation, field 13 must byte-equal its `terminal_cf/GENERATION` open-obligation summary. Later records update only evidence, charges, and duplicate risk, never the immutable terminal decision. Logical active/pending quota counts the Generation once; each field-13 ledger keeps its own outcome/evidence/physical-resource charge.

Dead Letter Replay atomically replaces `id_cf/MESSAGE` with the next Generation's fresh runtime index; it does not copy old open obligations into the new Generation. The prior terminal record then becomes their sole Generation-level summary/locator. Every attempt ledger carries its own DelayMessageId/generation and must appear in the current runtime set when its Generation is nonterminal, in the exact terminal summary when terminal, and in both only while that terminal Generation is still the current one. A late Outcome/evidence for an older terminal Generation compares that ledger and terminal summary, never the newer runtime index, and cannot change or terminalize the newer Generation. Terminal history/identity and all referenced payload/evidence remain GC-protected while the summary is nonempty.

`ControlReasonV1` exact fields: 1 `ControlReasonKindV1 kind`; optional 2 `bytes ticket_reference_hash`=32; optional 3 `bytes bounded_detail_hash`=32. Raw free-form reason text is audit data outside the canonical marker and cannot alter apply semantics.

`AcknowledgementSetV1` contains repeated `AcknowledgementV1 acknowledgements`, each with 1 `AcknowledgementKindV1 kind`, 2 `bytes acknowledgement_hash`=32, 3 `bytes ticket_scope_hash`=32. Entries are strictly sorted by kind and unique; a required acknowledgement is satisfied only by the exact registered kind/hash/scope.

`QuotaGrantRefV1` exact fields: 1 `bytes grant_id`=32; 2 `uint64 grant_version`; 3 `bytes grant_semantic_hash`=32; 4 `ChargeVectorV1 limit`. `QuotaTransferPlanRefV1`: 1 Control Operation ID[32]; 2 request hash[32]; 3 tenant-policy version `uint64`; 4 plan hash[32]. `LaneControlTargetV1`: 1 DestinationLaneId[32]; 2 Lane Incarnation[16]; 3 `uint64 expected_lane_control_version`.

`LaneTerminalGuardV1` exact fields: 1 version=1; 2 DestinationLaneId[32]; 3 Lane Incarnation[16]; 4 `LaneAdmissionGateV1 final_gate`, exactly `RETIRED`; 5 nonzero `uint64 lane_control_version`; 6 `SourcePositionV1 terminal_source_position` of the last source-ordered Close/Break boundary; 7 destination `ProfileRefV1`; 8 capability `ProfileRefV1`; 9 bounded exact canonical Lane tuple bytes from §4.1; 10 `bytes canonical_lane_tuple_sha256`=32; 11 `bytes retirement_intent_id`=32; 12 nonzero `uint64 retirement_mutation_sequence`; 13 `bytes guard_digest`=32. Field 10 equals SHA-256(field 9), field 2 must recompute from field 9 under §4.1, and fields 7–8 must byte-project from that tuple. Field 13 is `SHA-256("nereus-delay-lane-terminal-guard-v1\0" || canonicalProtobuf(fields 1–12))`. Retaining the exact tuple prevents a hash collision from merging or reopening another Lane.

`LaneRetirementProgressV1` exact fields: 1 Resource-Retire System Mutation ID[32]; 2 nonzero applied shard-mutation sequence `uint64`; 3 `SourcePositionV1 intent_source_position`; 4 progress digest[32] over domain `nereus-delay-lane-retirement-progress-v1\0` plus canonical fields 1–3. It is absent before a Lane retirement intent applies and remains present until the active record is replaced by its terminal guard.

`ActiveLaneStateV1` exact fields: 1 version=1; 2 DestinationLaneId[32]; 3 Lane Incarnation[16]; 4 `LaneAdmissionGateV1 admission_gate`; 5 `LaneRuntimeReadinessV1 runtime_readiness`; optional 6 `LaneRuntimeBlockReasonV1 runtime_block_reason`; 7 nonzero `uint64 lane_control_version`; 8 nonzero `uint64 lane_version`; 9 destination `ProfileRefV1`; 10 capability `ProfileRefV1`; 11 bounded exact canonical Lane tuple bytes from §4.1; 12 tuple SHA-256[32]; 13 nonzero `uint64 scheduler_weight`; 14 `ChargeVectorV1 lane_usage`; optional 15 nonnegative `int64 earliest_action_at`; optional 16 nonnegative `int64 next_eligible_at`; 17 `LaneCircuitStateV1 circuit_state`; 18 nonnegative `int64 circuit_open_until`; 19 `uint64 consecutive_failures`; 20 nonnegative `int64 lane_retry_backoff_until`; 21 nonnegative `int64 executor_retry_at`; optional 22 `bytes encoded_ready_key`; optional 23 ready-key SHA-256[32]; optional 24 `ReadyCertificateV1 ready_certificate`; optional 25 `LaneRetirementProgressV1 retirement`; 26 state digest[32] over domain `nereus-delay-active-lane-state-v1\0` plus canonical fields 1–25.

`LaneQuotaUsageEntryV1` exact fields: 1 DestinationLaneId[32]; 2 Lane Incarnation[16]; 3 `ChargeVectorV1 usage`; 4 nonzero `uint64 usage_revision`; 5 entry digest[32] over domain `nereus-delay-lane-quota-usage-entry-v1\0` plus canonical fields 1–4. `LaneQuotaUsageMapV1`, the exact `meta_cf/QUOTA` quotaClass=3 value, has fields: 1 version=1; 2 repeated entries strictly sorted/unique by `(DestinationLaneId,Lane Incarnation)`; 3 map digest[32] over domain `nereus-delay-lane-quota-usage-map-v1\0` plus canonical fields 1–2.

Active-Lane fields 11–12 recompute field 2 and byte-project fields 9–10; field 4 is restricted to `OPEN|ADMIN_PAUSED|ORDERING_BROKEN|CLOSED`, because `RETIRED` is represented only by the terminal-guard branch. Field 14 is byte-equal to the matching `LaneQuotaUsageEntryV1.usage`, and the two revisions advance in the same batch. Field 6 is present exactly for `BLOCKED`; other readiness states forbid it. Fields 22–23 are present together. `READY` requires field 24, while other readiness states forbid it. A physical READY entry exists exactly when `admission_gate=OPEN`, `runtime_readiness=READY`, schedulable current work exists and fields 15–16/22–23 are present. Its full key must equal field 22, match §7 `timeline/READY`, encode fields 2/8/16, and hash to field 23. Otherwise fields 22–23 are absent; fields 15–16 may remain present only for retained work whose gate/readiness currently excludes scheduling. `OPEN` circuit requires field 18 greater than the decision time that opened it; `CLOSED` requires field 18=0; `HALF_OPEN` uses bounded probe ownership and cannot create more than the Profile limit. Every Lane mutation recomputes field 26 in the same WriteBatch as any READY/quota/scheduler projection it changes.

The `meta_cf/LANE` value is a closed `LaneRecordV1`: field 1 `LaneRecordKindV1`; oneof field 10 active `ActiveLaneStateV1` / field 11 `LaneTerminalGuardV1`. Kind and branch must agree. Physical retirement atomically replaces branch 10 with branch 11 at the same RocksDB key; V1 has no separate `LANE_TERMINAL_GUARD` tag/key namespace. A terminal guard cannot transition back to active and is deleted only under the referenced retirement/Recovery-Floor rule.

The five `meta_cf/SCHEDULER` values are also closed. `SchedulerReadyDiscoveryCursorV1` (kind 1) fields: 1 version=1; optional 2 exact last-scanned READY key; optional 3 its SHA-256[32], present with field 2; 4 `uint64 wrap_generation`; 5 `uint64 active_ring_generation`; 6 digest[32]. `SchedulerRingEntryV1` fields: 1 DestinationLaneId[32]; 2 Lane Incarnation[16]; 3 nonzero observed `lane_version`; entries are unique by fields 1–2. `SchedulerActiveRingV1` (kind 2) fields: 1 version=1; 2 nonzero ring generation; 3 `uint64 round_generation`; 4 `uint32 next_index`; 5 repeated ordered `SchedulerRingEntryV1`; 6 digest[32]; field 4 is zero for an empty ring and otherwise less than the entry count. `SchedulerDeficitEntryV1` fields: 1 DestinationLaneId[32]; 2 Lane Incarnation[16]; 3 `uint64 deficit_bytes`; 4 nonzero observed `lane_version`. `SchedulerDeficitMapV1` (kind 3) fields: 1 version=1; 2 repeated entries strictly sorted/unique by fields 1–2; 3 digest[32]. `SchedulerRoundV1` (kind 4) fields: 1 version=1; 2 `uint64 round_generation`; 3 `OwnerIdentityV1 owner`; 4 `bool recovery_first_pass`; 5 digest[32]. `SchedulerLastServedEntryV1` fields: 1 DestinationLaneId[32]; 2 Lane Incarnation[16]; 3 `uint64 last_served_round`; 4 `uint64 service_gap_generation`. `SchedulerLastServedMapV1` (kind 5) fields: 1 version=1; 2 repeated entries strictly sorted/unique by fields 1–2; 3 digest[32]. Fields named digest use, respectively, `SHA-256("nereus-delay-scheduler-ready-discovery-cursor-v1\0" || canonicalProtobuf(fields 1–5))`, `SHA-256("nereus-delay-scheduler-active-ring-v1\0" || canonicalProtobuf(fields 1–5))`, `SHA-256("nereus-delay-scheduler-deficit-map-v1\0" || canonicalProtobuf(fields 1–2))`, `SHA-256("nereus-delay-scheduler-round-v1\0" || canonicalProtobuf(fields 1–4))`, and `SHA-256("nereus-delay-scheduler-last-served-map-v1\0" || canonicalProtobuf(fields 1–2))`. Ring order is semantic successor order and is not sorted. Stale Lane incarnation/version entries are discarded only while fenced, then all five projections are atomically rebuilt from `meta_cf/LANE` plus READY keys before scheduling resumes.

Because one shard is one DB, these records persist only the inner Lane scheduler. The Worker-level outer shard DRR is bounded process state reconstructed from the finite set of open `ACTIVE_FOR_COMMANDS` shard DBs; after construction it offers every eligible shard once before repeating one. V1 never pretends to atomically persist one Worker ring across independent shard DBs, and the progress guarantee restarts at a new ownership interval.

`ControlRefV1` deliberately does not embed this marker's expected mutation ID/hash: those values are computed from the completed body and stored in the immutable Oxia target record. Embedding either value in the hashed body would be circular. Apply computes the outer ID/hash and requires exact equality with the externally registered target before granting control authority.

`ControlPayloadV1` is a closed oneof whose field number equals `ControlKindV1`; its selected branch must equal body field 11. Exact branches are:

| field / branch | exact fields |
|---|---|
| 1 `ProtocolVersionActivatePayloadV1` | 1 `ProtocolTupleV1 tuple`; 2 canonical schema SHA-256[32]; 3 compatible-reader-set evidence SHA-256[32] |
| 2 `ProfileBindingActivatePayloadV1` | 1 `ProfileRefV1 profile` |
| 3 `ProfileNewBindingClosePayloadV1` | 1 `ProfileRefV1 profile`; 2 `ControlReasonV1 reason` |
| 4 `StopNewSchedulesPayloadV1` | 1 `bool stop`, required true; 2 `ControlReasonV1 reason` |
| 5 `GrantDecreaseOrHoldPayloadV1` | 1 `QuotaTransferPlanRefV1 plan`; 2 `QuotaGrantRefV1 old_grant`; 3 `QuotaGrantRefV1 staged_new_grant` |
| 6 `GrantShrinkDrainedPayloadV1` | 1 `QuotaTransferPlanRefV1 plan`; 2 `QuotaGrantRefV1 staged_new_grant`; 3 persisted counter SHA-256[32]; 4 `ChargeVectorV1 observed_usage` |
| 7 `GrantIncreaseActivatePayloadV1` | 1 `QuotaTransferPlanRefV1 plan`; 2 `QuotaGrantRefV1 old_grant`; 3 `QuotaGrantRefV1 new_grant`; 4 placement reservation ID[32]; 5 placement reservation SHA-256[32] |
| 8 `PauseDestinationLanePayloadV1` | 1 `LaneControlTargetV1 lane`; 2 `ControlReasonV1 reason` |
| 9 `ResumeDestinationLanePayloadV1` | 1 `LaneControlTargetV1 lane`; 2 `ControlReasonV1 reason` |
| 10 `BreakOrderingDomainPayloadV1` | 1 `LaneControlTargetV1 lane`; 2 `AcknowledgementSetV1 acknowledgements`, containing `ORDER_LOSS` and `POSSIBLE_DUPLICATE` |
| 11 `CloseDestinationLanePayloadV1` | 1 `LaneControlTargetV1 lane`; 2 `ControlReasonV1 reason`; 3 `ClosePolicyV1 close_policy`=1; 4 `bool allow_order_break`; 5 `AcknowledgementSetV1 acknowledgements`; strict Lane requires `allow_order_break=true` plus `ORDER_LOSS` and `POSSIBLE_DUPLICATE` |
| 12 `PayloadProofTrustSetActivatePayloadV1` | 1 `PayloadProofTrustSetRefV1 trust_set` |
| 13 `PayloadProofIssuanceClosePayloadV1` | 1 `PayloadProofTrustSetRefV1 trust_set`; 2 `uint32 proof_key_version`; 3 `ControlReasonV1 reason` |
| 14 `InitialRouteControlActivatePayloadV1` | 1 repeated `ProtocolTupleV1` strictly byte-sorted/unique; 2 repeated `ProfileRefV1` strictly `(profile_id,version)` sorted/unique; 3 initial `QuotaGrantRefV1`; 4 immutable initial-control snapshot SHA-256[32] |

For Lane controls, outer field 14 `expected_prior_control_version`, when present, must equal `LaneControlTargetV1.expected_lane_control_version`. Profile/grant/trust-set references must equal the body semantic version/hash and the immutable Control Operation target. A mismatch is `UNAUTHORIZED_SYSTEM_MUTATION`, not an alternate interpretation.

`ExactResourceIdentityV1` is a closed oneof with these branches:

| field | resource branch | exact fields |
|---:|---|---|
| 1 | `PayloadObjectResourceV1` | 1 `ProfileRefV1 object_store_profile`; 2 container bytes; 3 object-key bytes; 4 immutable-version bytes; optional 5 etag bytes; 6 length `uint64`; 7 SHA-256[32] |
| 2 | `CheckpointResourceV1` | 1 Recovery Lineage ID[16]; 2 checkpoint ID[16]; 3 `ProfileRefV1 object_store_profile`; 4 container bytes; 5 object-key bytes; 6 immutable-version bytes; 7 manifest length `uint64`; 8 manifest SHA-256[32] |
| 3 | `DlqExportResourceV1` | 1 export ID[32]; 2 target `BrokerResourceIdentityV1`; 3 exact object/message identity bytes; 4 payload SHA-256[32] |
| 4 | `KafkaReceiptSlotResourceV1` | 1 cluster ID bytes; 2 receipt topic UUID[16]; 3 Route UUID[16]; 4 shard partition `uint32`; 5 slot `uint32`; 6 slot generation `uint64` |
| 5 | `PulsarJournalGenerationResourceV1` | 1 `BrokerResourceIdentityV1`; 2 partition `uint32`; 3 evidence generation `uint64` |
| 6 | `LaneChannelResourceV1` | 1 `ChannelResourceIdentityV1` |
| 7 | `LocalStoreResourceV1` | 1 `ShardSubjectV1`; 2 Store Incarnation[16]; 3 DB identity[32]; 4 absolute-root-policy digest[32] |

`ProtectionRefV1` exact fields: 1 `ProtectionKindV1 protection_kind`; 2 `bytes protected_resource_id`=32; 3 `uint64 protection_generation`; optional 4 `SourcePositionV1 minimum_source_position`; optional 5 Recovery Lineage ID[16]; optional 6 checkpoint ID[16]; optional 7 manifest hash[32]. Presence is kind-specific: `RECOVERY_FLOOR` requires fields 4–7; `QUERY_OR_AUDIT_RETENTION` and `REPLAY_OR_RETRY_WINDOW` require field 4 and forbid 5–7; the other kinds forbid 4–7 and bind their exact external generation through fields 2–3. `ProtectionSetV1` field 1 is a strictly canonical-byte-sorted unique repeated list of `ProtectionRefV1`; field 2 is SHA-256 of domain `nereus-delay-protection-set-v1\0` plus the canonical repeated list.

`RecoveryFloorRefV1` exact fields: 1 Recovery Lineage ID[16]; 2 checkpoint ID[16]; 3 manifest SHA-256[32]; 4 nonzero `uint64 catalog_generation`; 5 `SourcePositionV1 applied_source_position`; 6 `uint64 included_mutation_sequence`; 7 repeated `EvidenceCursorV1 evidence_cursors`, strictly sorted by §8; 8 floor digest[32]. Field 8 is SHA-256 of domain `nereus-delay-recovery-floor-ref-v1\0` plus canonical fields 1–7. A scalar position or mutation sequence without fields 1–4/8 never identifies a Floor.

`RecoveryCandidateRefV1` exact fields: 1 `RecoveryCandidateKindV1 kind`; 2 Recovery Lineage ID[16]; 3 checkpoint ID[16]; 4 manifest SHA-256[32]; optional 5 Store Incarnation[16]; 6 candidate digest[32]. `LOCAL_STORE` requires field 5 and identifies the exact catalog checkpoint/base recorded in that Store; `CATALOG_CHECKPOINT` forbids it. Field 6 is SHA-256 of domain `nereus-delay-recovery-candidate-ref-v1\0` plus canonical fields 1–5. Initial fresh-store creation before a Recovery Floor exists is not recovery reuse and creates no synthetic candidate.

`RecoveryPinV1` exact fields: 1 version=1; 2 nonzero random Pin ID[16]; 3 `ShardSubjectV1 shard`; 4 `OwnerIdentityV1 owner`; 5 `RecoveryCandidateRefV1 candidate`; 6 `RecoveryFloorRefV1 observed_floor`; 7 nonzero `uint64 observed_catalog_generation`, equal to floor field 4; 8 Oxia session-identity digest[32]; 9 pin digest[32]. Field 9 is SHA-256 of domain `nereus-delay-recovery-pin-v1\0` plus canonical fields 1–8. The pin is an Oxia session-bound ephemeral record created by one transaction that compares the exact current Owner Lease/session and catalog generation. It has no client-clock expiry field: while the exact record exists, checkpoint deletion, supersession cleanup and orphan reaping must protect candidate and Floor objects. Create-response loss rereads the exact path/value. Immediately before activation the Owner revalidates pin/Floor/catalog/source/evidence; the `ACTIVE_FOR_COMMANDS` Owner-Lease CAS deletes the exact pin in the same transaction. Abandonment deletes it only after the candidate DB/temp directory is closed; session loss removes it automatically.

`CheckpointUploadIntentV1` exact fields: 1 version=1; 2 `ShardSubjectV1 shard`; 3 Recovery Lineage ID[16]; 4 checkpoint ID[16]; 5 `OwnerIdentityV1 owner`; 6 source Store Incarnation[16]; 7 nonzero random upload token[32]; 8 nonzero `uint64 base_catalog_generation`; optional pair 9 parent checkpoint ID[16] / 10 parent manifest hash[32]; 11 Object Store `ProfileRefV1`; 12 `TrustedUtcIntervalEvidenceV1 checkpoint_created_at`; 13 nonnegative `int64 upload_deadline_epoch_ms`; 14 `CheckpointUploadStateV1 state`; 15 nonzero checked `uint64 state_revision`; optional 16 `CheckpointResourceV1 published_manifest`; optional 17 `TrustedUtcIntervalEvidenceV1 reaping_started_at`; 18 intent digest[32] over domain `nereus-delay-checkpoint-upload-intent-v1\0` plus canonical fields 1–17. `PENDING_UPLOAD` forbids 16–17; `PUBLISHED` requires 16 and forbids 17; `REAPING` requires 17 and forbids 16. Fields 3–4/12 must equal the draft/manifest; parent fields are both present exactly for a non-genesis candidate.

Intent creation compares the exact active Owner Lease/session/Store and base catalog. Publication is one Oxia transaction comparing state/revision/token, Owner Lease/session, lineage head and base catalog, then changing `PENDING_UPLOAD -> PUBLISHED` and adding field 16 to the Recovery Set. Reaping can win only through the competing `PENDING_UPLOAD -> REAPING` CAS: either the exact live Owner explicitly abandons, or another actor proves the recorded Owner Lease/session is no longer current and Trusted UTC has passed field 13. `REAPING` permanently forbids catalog publication. Object deletion waits until `earliestUtcNow >= checkedAdd(reaping_started_at.latest, checkpointUploadRequestQuiescenceHorizon)`, no `RecoveryPinV1` or published catalog entry protects the prefix, and the certified old-Owner local guard plus provider-ownership horizon excludes another late request. The reaper then enumerates the unique checkpoint prefix, exact-version deletes, and performs a final empty-prefix sweep; response loss restarts this same state, token and prefix. A mere upload deadline or missing callback never authorizes deletion while the old Owner Lease/session remains current.

`RetireIntentRefV1`: 1 System Mutation ID[32]; 2 mutation hash[32]; 3 exact resource identity SHA-256[32]; 4 expected resource-state version `uint64`. This `uint64` carries the complete 64-bit pattern; implementations must not reject the high bit merely because their host integer type is signed. `ExternalDeleteEvidenceV1`: 1 exact resource identity SHA-256[32]; 2 provider request ID hash[32]; 3 `DeleteOutcomeV1 outcome`; 4 optional exact observed immutable version; 5 optional exact observed etag; 6 authenticated HEAD/delete response SHA-256[32]; 7 `TrustedUtcIntervalEvidenceV1 observed_at`. `ALREADY_ABSENT` forbids version/etag presence; `DELETED` requires every provider-returned identity field.

`ScheduleIntentV1` exact fields：1 `ProfileRefV1 profile`；2 `RetryPolicyRefV1 retry_policy`；3 `int64 deliver_at`；4 `int64 expire_at`；5 `DeliveryModeV1 delivery_mode`；6 `OrderingModeV1 ordering_mode`；7 `bytes ordering_key`；closed oneof field 8 `bytes inline_payload` / field 9 `CommittedPayloadDescriptorV1 committed_payload`；10 `AdapterMetadataV1 adapter_metadata`；optional 11 `bytes business_key`；optional 12 `int64 event_time`；13 `uint32 quota_accounting_version=1`。Prepare body requires neither payload branch; ordinary Schedule requires exactly one. `nereus.delay.*` names are forbidden in caller metadata.

### 5.2 Client bodies

| Body | fields 10+ |
|---|---|
| `ScheduleV1` | 10 `ScheduleIntentV1 intent` |
| `PrepareLargeScheduleV1` | 10 `ScheduleIntentV1 intent_without_payload_object`；11 `uint64 expected_payload_length`；12 `bytes payload_sha256`=32；13 `uint64 reservation_ttl_ms`；14 `PayloadProofTrustSetRefV1 trust_set` |
| `CommitLargeScheduleV1` | 10 `bytes reservation_id`=32；11 `PayloadCommitProofV1 proof` |
| `CancelV1` | 10 `MessagePreconditionV1 precondition` |
| `RescheduleV1` | 10 `MessagePreconditionV1 precondition`；11 `int64 new_deliver_at`；12 `int64 new_expire_at` |

`PayloadCommitProofV1` exact fields: 1 `uint32 proof_version`=1; 2 ReservationId[32]; 3 tenantRoutingScope[32]; 4 Route UUID[16]; 5 partition `uint32`; 6 DelayMessageId[41]; 7 `ProfileRefV1 object_store_profile`; 8 trust-set version `uint64`; 9 proof-key version `uint32`; 10 exact bucket/container bytes; 11 exact object-key bytes; 12 exact immutable-object-version bytes; optional-presence 13 exact etag bytes; 14 length `uint64`; 15 payload SHA-256[32]; 16 `int64 not_after_epoch_ms`; 17 ProofId[32]; 18 Ed25519 signature[64].

```text
proofId = SHA-256(
  "nereus-delay-payload-proof-id-v1\0" ||
  canonicalProtobuf(fields 1..8 and 10..16 with original field numbers)
)
payloadProofSignatureDigest = SHA-256(
  "nereus-delay-payload-proof-signature-v1\0" ||
  canonicalProtobuf(fields 1..17)
)
```

Field 17 must equal `proofId`; field 18 is Ed25519 over `payloadProofSignatureDigest`. Thus ProofId excludes only signer field 9 plus identity/signature fields 17–18, while signature excludes only field 18. Every optional presence, Profile semantic hash and object identity field enters ProofId; provider display strings cannot replace exact values.

### 5.3 System Mutation bodies

| Body | fields 10+；全部 required，除非标 optional |
|---|---|
| `ApplyShardControlV1` | 10 `ControlRefV1`；11 `ControlKindV1 control_kind`；12 `uint64 semantic_version`；13 `bytes semantic_hash`=32；14 optional `uint64 expected_prior_control_version`；15 `ControlPayloadV1 payload` |
| `ReplayDeadLetterV1` | 10 `ControlRefV1`；11 `DelayMessageId`；12 `uint32 expected_generation`；13 `uint64 expected_state_version`；14 `int64 deliver_at`；15 `int64 expire_at`；16 `RetryPolicyRefV1`；17 `bool allow_possible_duplicate`；18 `bytes acknowledgement_hash`=32 when required |
| `ResolveUncertainV1` | 10 `ControlRefV1`；11 `DestinationLaneId`；12 `bytes lane_incarnation`=16；13 `DelayMessageId`；14 `uint32 generation`；15 `PublishAttemptId`；16 `UncertainResolutionKindV1`；17 optional `PublishEvidenceV1 evidence`；18 `bool allow_possible_duplicate`；19 `bool allow_possible_delivery_terminal`；20 optional `bytes acknowledgement_hash`=32 |
| `TimeFenceV1` | 10 `int64 close_through_epoch_ms`；11 `uint32 fence_proof_key_version`；12 `ProofId`；13 `TrustedUtcIntervalEvidenceV1 proof_time` |
| `PublishAdmissionV1` | 10 `OwnerIdentityV1`；11 `bytes store_incarnation`=16；12 ClaimId[32]；13 `DestinationLaneId`；14 `bytes lane_incarnation`=16；15 `DelayMessageId`；16 `uint32 generation`；17 `PublishAttemptId`；18 `bytes prepared_publish_hash`=32；19 `ChargeVectorV1 reserve_charge`；20 `bytes ready_certificate_digest`=32；21 `ChannelResourceIdentityV1 channel`；22 `PreparedPublishDescriptorV1 descriptor`；23 `ReadyCertificateV1 ready_certificate`；24 `TrustedUtcIntervalEvidenceV1 decision_time`；25 `ClaimPreconditionV1 claim_precondition` |
| `PublishOutcomeV1` | 10 `PublishAttemptId`；11 `PublishSideEffectV1 side_effect`；12 `PublishDispositionV1 disposition`；13 `StableCodeV1 stable_code`；14 optional `PublishEvidenceV1 evidence`；15 `ChargeVectorV1 transfer`；16 `TrustedUtcIntervalEvidenceV1 observed_at`；17 `RetryDecisionV1 retry_decision` |
| `ExpireGenerationV1` | 10 `DelayMessageId`；11 `uint32 generation`；12 `int64 expire_at`；13 `TrustedUtcIntervalEvidenceV1` |
| `EvidenceResolutionV1` | 10 `PublishAttemptId`；11 `EvidenceCursorV1 cursor`；12 `PublishEvidenceV1 evidence`；13 `StableCodeV1 stable_code`；14 `PublishSideEffectV1 side_effect`；15 `PublishDispositionV1 disposition`；16 `ChargeVectorV1 transfer`；17 `TrustedUtcIntervalEvidenceV1 observed_at`；18 `RetryDecisionV1 retry_decision` |
| `ResourceRetireIntentV1` | 10 `ResourceKindV1 resource_kind`；11 `ExactResourceIdentityV1 resource`；12 `uint64 expected_resource_state_version`；13 `ProtectionSetV1 protections` |
| `ResourceDeleteConfirmedV1` | 10 `RetireIntentRefV1 intent`；11 `DeleteOutcomeV1 outcome`；12 `ExternalDeleteEvidenceV1 evidence`；13 `TrustedUtcIntervalEvidenceV1 confirmed_at` |
| `ClaimResultV1` | 10 ClaimId[32]；11 DelayMessageId[41]；12 `uint32 generation`；13 DestinationLaneId[32]；14 Lane Incarnation[16]；15 `ClaimPreconditionV1 claim_precondition`；16 `ClaimResultKindV1 result_kind`；17 `StableCodeV1 stable_code`；18 `TrustedUtcIntervalEvidenceV1 observed_at`；field number/name 19 `retry_decision` permanently reserved；20 `ChargeVectorV1 transfer` |
| `DlqExportResultV1` | 10 DlqExportId[32]；11 DelayMessageId[41]；12 `uint32 generation`；13 `uint64 terminal_revision`；14 export envelope SHA-256[32]；15 `DlqExportEventKindV1 event_kind`；16 `PublishSideEffectV1 side_effect`；17 `PublishDispositionV1 disposition`；18 `StableCodeV1 stable_code`；19 optional `PublishEvidenceV1 evidence`；20 `ChargeVectorV1 transfer`；21 `TrustedUtcIntervalEvidenceV1 observed_at`；22 `RetryDecisionV1 retry_decision`；23 `DlqExportStateV1 resulting_state`；24 `uint32 physical_attempt_no` |

任何向上述 V1 table 增加 field、改变 required/presence、复用 reserved number 或赋予旧 bytes 新语义都要求新 `bodyVersion` 和独立 activated tuple。

Outer `AuthorIdentityV1` branch is closed by mutation type: `APPLY_SHARD_CONTROL_V1`, `REPLAY_DEAD_LETTER_V1`, and `RESOLVE_UNCERTAIN_V1` require `control`; `TIME_FENCE_V1` requires `fence`; `PUBLISH_ADMISSION_V1`, `PUBLISH_OUTCOME_V1`, `EXPIRE_GENERATION_V1`, and `CLAIM_RESULT_V1` require `owner`; `EVIDENCE_RESOLUTION_V1`, both resource mutations, and `DLQ_EXPORT_RESULT_V1` require `service`. Admission outer Owner must equal body field 10 and Claim precondition Owner; Claim Result outer Owner must equal precondition Owner. Publish Outcome's Owner is checked against the immutable attempt ledger (the recovery-unknown Outcome is authored by the new current Owner and uses the explicit recovery code). Every signing key and service/fence/control generation must be in the Route's source-protected historical accepted-writer set for that record, retained through its retry/Recovery-Floor replay window. Owner authorization is likewise historical: an accepted signature, typed Owner and exact lease-fencing digest/body equalities are validated, but apply-time current Owner mismatch alone cannot invalidate an earlier Admission. Current lease/Store/certificate state only gates a new physical call. Wrong branch, unproved generation or equality mismatch is `UNAUTHORIZED_SYSTEM_MUTATION`, never a fallback branch. V1 assumes accepted service writers are non-Byzantine; a compromised accepted signing key is outside the normal failure model.

For `PUBLISH_ADMISSION_V1`, descriptor fields 4–5/10–13 must equal body fields 13–17/21, and field 18 must equal the descriptor hash formula. Certificate fields 2–6/16 must equal body Owner/Store/Lane/channel/digest, and certificate fields 13–15 must equal channel fields 14–16. Claim-precondition fields 1–3/5–6/14–15 must equal body fields 12/15–16/13–14/10–11, and its materialization must be the exact projection of descriptor fields 6–9/11–12/15–16/18–20. Any mismatch is `STALE_SYSTEM_MUTATION`; a hash-valid descriptor is persisted in full before Producer authorization.

Admission timing is replay-stable. At preparation and live apply, `decision_time.earliest_epoch_ms >= actionAt`, `decision_time.latest_epoch_ms < expireAt`, and `decision_time.latest_epoch_ms < ready_certificate.valid_until_epoch_ms`. Let `bp` be this Admission record's Broker persistence time and `D=maxIngressBrokerTimestampDivergence`; require checked `bp + D < min(expireAt, certificate.validUntil)` and interval distance between `bp` and `decision_time` at most `maximumAdmissionMutationEnqueueAge + D`. Failure is `STALE_SYSTEM_MUTATION`, revokes any matching Claim, creates no attempt and never calls Producer. Apply/replay does not sample a new clock, so source lag cannot change the result.

If the exact local Claim is present, apply consumes it. If it is absent because a permitted checkpoint predates this record or Store/Owner changed, but the replay-stable subset of source-ordered Message/Lane control state still matches the signed Claim precondition, apply reconstructs the same `PUBLISHING` attempt from the body; it does not reject merely because reversible runtime state was not checkpointed. The descriptor `attempt_no` must equal `checkedAdd(GenerationRuntimeIndexV1.admissions_used,1)` and the original timeline-key hash, semantic-work digest, source work kind, both counters and attempt-obligation-set digest must match either the linked Claim or the exact reversible source state; current Owner/Store/runtime Lane version and local runtime revision/work-instance digest are not replay preconditions. Mismatch in the replay-stable subset is stale. Apply checked-increments `admissions_used`; it also checked-increments `uncertain_retry_admissions_used` exactly when an older `UNCERTAIN` ledger is present. That case requires `UNCERTAIN_RETRY`, `BEST_EFFORT`, remaining total Admission/time/capacity bounds, plus either remaining automatic budget for `PINNED_POLICY` or the exact source-ordered acknowledged ControlRef/position for `CONTROL_OVERRIDE`. It creates the new PUBLISHING ledger, inserts the exact PUBLISHING-key `AttemptObligationRefV1` into the canonical obligation set and sets current work PUBLISHING in the same batch. Admission apply always produces the same durable attempt state independent of the ephemeral locally-authored send token. Only a matching live Owner/Store/Claim/token/certificate/monotonic-time gate may make the first Producer call; the live gate validates the protected unexpired channel lease, requires binding generation/digest/fingerprint to equal channel fields 14–16 and certificate fields 13–15, checks the Adapter's loaded fingerprint, and closes the local gate-to-library-ownership interval within the configured bound. A detected failure while the exclusive send token is still `BEFORE_LIBRARY_OWNERSHIP` emits an initial `PUBLISH_OUTCOME_V1(NOT_PUBLISHED, LANE_UNAVAILABLE, code)` with `AdapterNonSubmissionEvidenceV1(BEFORE_LIBRARY_OWNERSHIP)` and a `MESSAGE_PUBLISH` Lane-wait retry decision: expired/wrong-kind/holder lease uses `CAPABILITY_UNAVAILABLE`, while binding or loaded-fingerprint mismatch uses `CREDENTIAL_BINDING_DRIFT`. A crash before that Outcome is durably logged follows the conservative recovery-unknown rule. This live time gate enforces `actionAt`/clock safety and certificate/lease validity but does not reapply `expireAt` to an already qualified Admission. Without a provable gate result, recovery subsequently logs the one initial `PUBLISH_OUTCOME_V1` with `side_effect=UNKNOWN`, `disposition=OWNER_FENCED`, `stable_code=RECOVERY_FIRST_SEND_UNCERTAIN`, no evidence, zero early physical-release claim, and `retry_decision=UNCERTAIN_HOLD`; it never mutates Admission replay directly into an implementation-dependent state. A later verified result uses `EVIDENCE_RESOLUTION_V1`, not a competing initial Outcome.

`TIME_FENCE_V1` requires `proof_time.earliest_epoch_ms >= checkedAdd(close_through_epoch_ms, timeFenceSafetyMargin)` and `fence_proof_key_version == outer signing_key_version`. Its ProofId is:

```text
proofId = SHA-256(
  "nereus-delay-time-fence-proof-v1\0" ||
  routeIncarnationUuid[16] || u32be(partition) ||
  i64be(closeThroughEpochMs) || u32be(fenceProofKeyVersion) ||
  lp32(canonicalProtobuf(proofTime))
)
```

Applying a valid fence monotonically advances `closedIngressDeadlineThrough`. That source-ordered watermark is also the authoritative logical transition for every still-`PAYLOAD_RESERVED` reservation with `reservation_expiry <= close_through_epoch_ms`: its effective state becomes `RESERVATION_EXPIRED` at the fence Source Position. A prior Commit, Cancel, or Lane Close wins by Source Position. The bounded `RESERVATION_EXPIRY` cursor only materializes the already-decided effective state, counter transfer and GC/tombstone work; replay and every API/Commit check the watermark overlay even when that cursor has not reached the reservation.

Outcome combination rules are closed: `PUBLISHED` requires `disposition=NONE`, verified-published evidence, `stable_code=OK`, and `retryDecision=NONE`; `NOT_PUBLISHED` requires verified-not-published evidence and may use `MESSAGE_RETRIABLE/MESSAGE_PERMANENT/LANE_UNAVAILABLE`; `UNKNOWN` forbids a definitive evidence status and requires `UNCERTAIN_HOLD` unless a baseline duplicate-authorized `SCHEDULED` decision is already fixed by policy. `OWNER_FENCED`/`ADAPTER_BUG` never turn `UNKNOWN` into `NOT_PUBLISHED`. A Publish Outcome authored by an Owner other than the attempt's admitted Owner is legal only when that author is the current guarded recovery Owner and the exact tuple is `UNKNOWN + OWNER_FENCED + RECOVERY_FIRST_SEND_UNCERTAIN + no evidence + UNCERTAIN_HOLD`; every other cross-Owner initial outcome is unauthorized/audit-only. `EvidenceResolutionV1` obeys the same evidence/side-effect matrix under its Service Writer branch. For definitive `PUBLISH_OUTCOME_V1` and `EVIDENCE_RESOLUTION_V1`, field `transfer` must be canonical byte-identical to the charge vector retained by the exact Admission ledger. A mismatch is `REJECTED(STALE_SYSTEM_MUTATION)`, advances the source position, and never changes the attempt, message, timeline, or quota; `UNKNOWN` transfer is opaque and never authorizes a definitive release.

Outcome/Resolution application updates the attempt ledger and `GenerationRuntimeIndexV1` in one WriteBatch. `UNKNOWN + SCHEDULED` atomically deletes the exact PUBLISHING key, writes the byte-equivalent identity/admission prefix plus Outcome under its exact UNCERTAIN key, replaces the PUBLISHING-key obligation ref with the UNCERTAIN-key ref, keeps aggregate `UNCERTAIN`, and inserts one `UNCERTAIN_RETRY` timeline work; it does not increment field 15 until the later Admission applies. `UNKNOWN + UNCERTAIN_HOLD` performs the same key/ref replacement and leaves current work NONE. Definitive nonpublication removes that exact ref only after required strong-capability retirement; if another UNCERTAIN ledger remains, any scheduled current work is `UNCERTAIN_RETRY` and the aggregate remains `UNCERTAIN`, otherwise it is `DEFINITIVE_RETRY`/`RETRY_WAIT`. When resolution empties the UNCERTAIN set while reversible TIMELINE/CLAIMED work exists, the event loop first revokes any Claim whose field-19 digest is stale, normalizes the work kind and aggregate, then permits a fresh Claim.

Any verified success terminalizes the Generation. Reversible TIMELINE or CLAIMED current work is removed in that batch. A different already-admitted PUBLISHING attempt cannot be revoked: its ledger and obligation ref remain, current work becomes NONE, `possible_destination_duplicate=true`, and its later Outcome/evidence may release only its own obligation and charges. Expiry and Lane Close similarly delete reversible current work but never erase an admitted obligation; with any possible-delivery ledger they retain aggregate `UNCERTAIN` or apply the pinned explicit possible-delivery terminal policy. Cancel/Reschedule are `TOO_LATE` whenever an UNCERTAIN obligation exists, even if current work is TIMELINE or CLAIMED. `UNCERTAIN_RETRY` is invalid for `DELIVERY_TIME_FIFO`, a closed/broken Lane, or exhausted budgets.

`ReplayDeadLetterV1` requires the current Message generation/state version to equal fields 12–13 and that Generation's immutable terminal decision to be `DEAD_LETTER`. Its own Broker persistence time must be at or before the retained replay deadline and not source-closed; timing/Profile/policy/quota/control preconditions are otherwise the exact main-spec rules. Field 17 plus field 18 acknowledgement are required if the old terminal decision has `possible_destination_duplicate=true` or its open-obligation summary is nonempty, and forbidden otherwise. Successful apply checked-increments generation/state version, writes the new Generation's `GenerationRuntimeIndexV1(INITIAL_SCHEDULE, admissions_used=0, uncertain_retry_admissions_used=0, empty obligation set)` and timeline/expiry records, while retaining the old terminal summary and ledgers under the old generation. An old obligation is never copied to the new index; later old Outcome/evidence addresses only its embedded old generation. All locator/counter/result/Source Position changes are one batch.

`ResolveUncertainV1` matrix is also closed: `ATTACH_PUBLISHED_EVIDENCE` requires verified-published evidence and all booleans false; `ATTACH_NOT_PUBLISHED_EVIDENCE` requires verified-not-published evidence and all booleans false; `RETRY_ALLOW_POSSIBLE_DUPLICATE` forbids evidence, requires field 18 plus acknowledgement field 20, and is illegal for ordered/broken/closed Lane; `TERMINALIZE_POSSIBLE_DELIVERY` forbids evidence, requires field 19 plus acknowledgement field 20. Other presence combinations are invalid. The retry target must be one exact UNCERTAIN attempt in the current obligation set, current work must be NONE, and `checkedAdd(admissions_used,1)` must fit the pinned total Admission/time/expiry bounds; otherwise the immutable target result is `TOO_LATE` (or the more specific Lane terminal code) and no timeline work is created. Success inserts one immediate `UNCERTAIN_RETRY(CONTROL_OVERRIDE)` whose ControlRef is this operation and whose eligibility is `max(actionAt, this record's authenticated Broker timestamp)`; it does not increment the uncertain count until the later Admission applies. Transient permits/capacity do not change this source-ordered result and are rechecked at Claim/Admission. Exact duplicate operation/mutation bytes return the first result. Evidence branches apply to the named attempt and follow the same success/absence normalization rules as `EvidenceResolutionV1`.

`RESOURCE_RETIRE_INTENT_V1` does not carry a guessed future `shardMutationSequence`; apply assigns that sequence in its atomic WriteBatch and stores it with the intent/tombstone. Resource kind must match the selected identity branch, and field 13 digest must validate. Delete Confirmation must reference the exact applied intent and byte-equal resource identity hash; outer and embedded delete outcomes must agree.

`CLAIM_RESULT_V1` is the only authority for a proven permanent materialization/serialization/size/payload failure that changes a Generation to `DEAD_LETTER`; V1 never converts a pre-send transient into message `RETRY_WAIT` or consumes a Publish Admission count. A transient failure only revokes the Claim to the same semantic timeline work/key/authority/candidate attempt and may update derivable Lane circuit/backoff runtime; the newly inserted `TimelineWorkRefV1` receives a checked-incremented runtime revision and recomputed instance digest, so stale snapshot/Claim tokens cannot revive. Body fields 10–14 must equal precondition fields 1–3/5–6, and field 17 is `CLAIM_PERMANENT_FAILURE`. The exact Claim may be consumed when present or reconstructed against its original timeline-key hash, semantic-work digest/kind, both counters and obligation-set digest when a permitted replay omitted it, exactly as for Admission. Earlier Cancel/Reschedule/Expiry/Close/Admission or any Outcome/Resolution that changed the semantic work or obligation set makes the result stale; a pure runtime-instance change does not. Callback code cannot write terminal state directly.
Field 20 `transfer` must be canonical byte-identical to precondition field 12 `claimed_charge`; a mismatch is a stale/integrity rejection and never becomes a new quota authority. The local apply may only release the Claim projection retained by the source-ordered state; complete grant-policy and external charge accounting remain separate V1 implementation boundaries.

`DLQ_EXPORT_RESULT_V1` applies only to the deterministic `dlqExportId` and immutable export-envelope hash created with the Dead Letter terminal record. The terminal outbox fixes `physical_attempt_no=1` and `first_export_at=terminalizing Shard Log record Broker persistence time` before the first external call. `ATTEMPT_OUTCOME` forbids reusing a number with different bytes and requires the next number to equal the outbox's checked successor; uncertain enqueue retries the same mutation bytes and a crash may repeat the same exact external envelope/attempt. A policy-scheduled retry is authorized only by the preceding applied result and uses `physical_attempt_no + 1`; overflow permanently fails the export with an integrity alert and no wrap. Its `RetryDecisionV1` uses `DLQ_EXPORT`, the DLQ fields of the pinned policy and that persisted first-export time. `EVIDENCE_RESOLUTION` requires field 19 with matching DLQ identity/hash; field 24 names the attempt proved by that evidence and uses the domain-separated evidence logical identity above. `PUBLISHED` requires verified-published evidence, `disposition=NONE`, `retryDecision=NONE`, and `resulting_state=PUBLISHED`. `NOT_PUBLISHED` requires verified-not-published evidence and yields `PENDING` with a scheduled retry or `FAILED_PERMANENT` with exhausted/none. `UNKNOWN` forbids definitive evidence and yields `UNCERTAIN` with `UNCERTAIN_HOLD` or a policy-authorized duplicate retry; each later physical retry has its own `ATTEMPT_OUTCOME`, so a second timeout is never smuggled in as evidence. `NOT_CONFIGURED` is created only with terminalization and is never a result target. There is no V1 `ABANDONED` branch. Apply recomputes every charge transfer, attempt-number transition and state transition from the retained outbox.

## 6. Closed state and result tags

### 6.1 Shard state

| `ShardLifecycleStateV1` | value |
|---|---:|
| `UNASSIGNED` | 1 |
| `ACQUIRING` | 2 |
| `RESTORING` | 3 |
| `CATCHING_UP` | 4 |
| `ACTIVE_FOR_COMMANDS` | 5 |
| `DRAINING` | 6 |
| `FENCED` | 7 |
| `FAILED` | 8 |

| `ShardPauseReasonV1` | value |
|---|---:|
| `NONE` | 0 |
| `OWNERSHIP_GUARD` | 1 |
| `RESTORE_IN_PROGRESS` | 2 |
| `ROCKSDB_WRITE_UNSAFE` | 3 |
| `DISK_SAFETY` | 4 |
| `CONTROL_INTEGRITY` | 5 |
| `TIME_FENCE_CAPACITY` | 6 |
| `INGRESS_ABUSE` | 7 |
| `PLACEMENT_NO_CAPACITY` | 8 |
| `RECOVERY_RETENTION_RISK` | 9 |

| `ShardFailureReasonV1` | value |
|---|---:|
| `NONE` | 0 |
| `SOURCE_GAP` | 1 |
| `STORE_CORRUPTION` | 2 |
| `CATALOG_OR_LINEAGE_INTEGRITY` | 3 |
| `UNSUPPORTED_ACTIVATED_PROTOCOL` | 4 |
| `CONTROL_PROTOCOL_INTEGRITY` | 5 |
| `UNRECOVERABLE_EVIDENCE_GAP` | 6 |

`FENCE_STALLED_CAPACITY` 是 pause reason 6 的 metric/audit alias，不是 lifecycle value。

### 6.2 Public/control union tag

`CommandApplyStatusV1`: 1 `APPLIED`, 2 `REJECTED`。  
`EnqueueOutcomeV1`: 1 `QUEUED`, 2 `DEFINITELY_NOT_QUEUED`, 3 `ENQUEUE_UNCERTAIN`。  
`ControlRegistrationOutcomeV1`: 1 `RECORDED`, 2 `DEFINITELY_NOT_RECORDED`, 3 `RECORD_UNCERTAIN`。  
`RouteLifecycleV1`: 1 `ACTIVE_FOR_NEW`, 2 `CONTROL_ONLY`, 3 `DRAINING`, 4 `RETIRED`。  
`SubmissionModeV1`: 1 `MANAGED`, 2 `AUTO_FAST`。  
`MessageGenerationStateV1`: 1 `SCHEDULED`, 2 `CLAIMED`, 3 `PUBLISHING`, 4 `RETRY_WAIT`, 5 `UNCERTAIN`, 6 `PUBLISHED`, 7 `HANDED_OFF`, 8 `CANCELED`, 9 `EXPIRED`, 10 `DEAD_LETTER`, 11 `SUPERSEDED`。  
`PayloadReservationStateV1`: 1 `PAYLOAD_RESERVED`, 2 `COMMITTED`, 3 `ABANDONED`, 4 `RESERVATION_EXPIRED`。  
`ActivationBarrierKindV1`: 1 `EMPTY_BARRIER`, 2 `KAFKA_EXCLUSIVE_OFFSET`, 3 `PULSAR_INCLUSIVE_MESSAGE_ID`。  
`CommandQueryResultV1`: 1 `PENDING`, 2 `APPLIED`, 3 `REJECTED`, 4 `RESULT_EXPIRED`, 5 `RESULT_EVIDENCE_EXPIRED`, 6 `UNKNOWN`, 7 `INVALID_RECEIPT`, 8 `RECEIPT_MISMATCH`, 9 `NOT_FOUND_OR_NOT_AUTHORIZED`, 10 `SHARD_TRANSITIONING`, 11 `SHARD_UNAVAILABLE`, 12 `INTEGRITY_ERROR`。  
`MessageQueryResultV1`: 1 `RESERVED`, 2 `ACTIVE`, 3 `TERMINAL`, 4 `IDENTITY_RETIRED`, 5 `UNKNOWN`, 6 `INVALID_RECEIPT`, 7 `RECEIPT_MISMATCH`, 8 `NOT_FOUND_OR_NOT_AUTHORIZED`, 9 `SHARD_TRANSITIONING`, 10 `SHARD_UNAVAILABLE`, 11 `INTEGRITY_ERROR`。  
`PayloadUploadHandleOutcomeV1`: 1 `ISSUED`, 2 `RESERVATION_EXPIRED`, 3 `RESERVATION_ABANDONED`, 4 `RESERVATION_CLOSED`, 5 `NOT_FOUND_OR_NOT_AUTHORIZED`, 6 `SHARD_TRANSITIONING`, 7 `SHARD_UNAVAILABLE`, 8 `INTEGRITY_ERROR`, 9 `OBJECT_STORE_UNAVAILABLE_RETRYABLE`。  
`PayloadAttestationOutcomeV1`: 1 `ATTESTED`, 2 `OBJECT_NOT_READY_RETRYABLE`, 3 `OBJECT_STORE_UNAVAILABLE_RETRYABLE`, 4 `OBJECT_IDENTITY_CONFLICT`, 5 `RESERVATION_EXPIRED`, 6 `RESERVATION_ABANDONED`, 7 `RESERVATION_CLOSED`, 8 `NOT_FOUND_OR_NOT_AUTHORIZED`, 9 `SHARD_TRANSITIONING`, 10 `SHARD_UNAVAILABLE`, 11 `INTEGRITY_ERROR`。  
`TargetMarkerStateV1`: 1 `PENDING`, 2 `ENQUEUE_UNCERTAIN`, 3 `QUEUED`, 4 `EFFECTIVE`, 5 `MATERIALIZING`, 6 `COMPLETED`, 7 `REJECTED`, 8 `FAILED_BEFORE_EFFECT`。  
`ControlOperationStateV1`: 1 `PENDING`, 2 `DISPATCHING`, 3 `PARTIALLY_EFFECTIVE`, 4 `IN_PROGRESS`, 5 `SUCCEEDED`, 6 `SUCCEEDED_WITH_OUTSTANDING`, 7 `REJECTED`, 8 `FAILED_BEFORE_EFFECT`。  
`ControlOperationQueryResultV1`: 1 `CURRENT`, 2 `INVALID_RECEIPT`, 3 `NOT_FOUND_OR_NOT_AUTHORIZED`, 4 `INTEGRITY_ERROR`。  
`SubmissionOutcomeKindV1`: 1 `MANAGED`, 2 `NATIVE_RECEIPT`, 3 `NATIVE_DEFINITELY_NOT_QUEUED`, 4 `NATIVE_ENQUEUE_UNCERTAIN`。  
`UploadHandleKindV1`: 1 `OPAQUE_SINGLE_PUT`, 2 `OPAQUE_MULTIPART`。  
`PublishSideEffectV1`: 1 `PUBLISHED`, 2 `NOT_PUBLISHED`, 3 `UNKNOWN`。  
`PublishDispositionV1`: 0 `NONE`, 1 `MESSAGE_RETRIABLE`, 2 `MESSAGE_PERMANENT`, 3 `LANE_UNAVAILABLE`, 4 `OWNER_FENCED`, 5 `ADAPTER_BUG`。
`LaneAdmissionGateV1`: 1 `OPEN`, 2 `ADMIN_PAUSED`, 3 `ORDERING_BROKEN`, 4 `CLOSED`, 5 `RETIRED`。  
`LaneRuntimeReadinessV1`: 1 `RECOVERING_EVIDENCE`, 2 `READY`, 3 `BLOCKED`。  
`ProfileAcceptanceV1`: 1 `ABSENT`, 2 `ACTIVE_FOR_FIRST_BINDING`, 3 `CLOSED_FOR_FIRST_BINDING`。  
`DlqExportStateV1`: 1 `NOT_CONFIGURED`, 2 `PENDING`, 3 `PUBLISHED`, 4 `UNCERTAIN`, 5 `FAILED_PERMANENT`；zero 和 6–255 在 V1 非法。
`PayloadAvailabilityV1`: 1 `UPLOAD_PENDING`, 2 `INLINE_RETAINED`, 3 `OBJECT_RETAINED`, 4 `PAYLOAD_RECLAIMED`, 5 `NOT_APPLICABLE`。  
`FirstScheduleEligibilityV1`: 1 `NOT_PROVEN`, 2 `EXPIRED_BY_SOURCE_FENCE`。  
`ControlResultKindV1`: 1 `LANE`, 2 `SHARD`, 3 `CHECKPOINT`, 4 `PROFILE`, 5 `QUOTA`, 6 `MESSAGE`, 7 `CHECKPOINT_CATALOG`, 8 `ROUTE`, 9 `SECRET_ROTATION`。
`SloPopulationV1`: 1 `HEALTHY`, 2 `ALL_ACCEPTED`。  
`DueExclusionReasonV1`: 1 `ADMIN_PAUSED`, 2 `ORDERING_BROKEN`, 3 `CLOSED`, 4 `RECOVERING_EVIDENCE`, 5 `CAPABILITY_BLOCKED`, 6 `CLOCK_GATED`, 7 `ORDER_HEAD_BLOCKED`, 8 `CAPACITY_GATED`, 9 `ADAPTER_LANE_FULL`。
`SloObjectiveNameV1`: 1 `COMMAND_QUEUED_LATENCY`, 2 `COMMAND_APPLIED_LATENCY`, 3 `DUE_ADMISSION_LAG`, 4 `NATIVE_HANDOFF_ACK_LAG`, 5 `QUERY_LATENCY`, 6 `OWNERSHIP_FAILOVER_RTO`, 7 `LOCAL_DISK_LOSS_RTO`, 8 `CHECKPOINT_AGE`, 9 `SOURCE_RETENTION_TIME_MARGIN`, 10 `SOURCE_RETENTION_BYTE_MARGIN`, 11 `POSSIBLE_DUPLICATE_WINDOW`, 12 `HEALTHY_LANE_DISCOVERY_AGE`, 13 `HEALTHY_LANE_SERVICE_GAP`, 14 `LANE_RECOVERY_READY_RTO`。  
`SloThresholdDirectionV1`: 1 `AT_MOST`, 2 `AT_LEAST`。 `SloThresholdUnitV1`: 1 `MILLISECONDS`, 2 `BYTES`, 3 `ROUNDS`。 `SloTimeoutTreatmentV1`: 1 `BAD`。
`SloPathV1`: 1 `NOT_APPLICABLE`, 2 `ORDINARY_MANAGED`, 3 `MANAGED_PULSAR_HANDOFF`, 4 `AUTO_FAST_NATIVE`。  
`SloTimeEndpointKindV1`: 1 `SEMANTIC_FIXED_EPOCH`, 2 `BROKER_PERSISTENCE`, 3 `TRUSTED_OBSERVATION`。  
`SloFinalOutcomeV1`: 1 `SUCCESS`, 2 `BAD_DEFINITIVE`, 3 `BAD_UNCERTAIN`, 4 `BAD_TIMEOUT`, 5 `BAD_UNQUALIFIED_TIME`, 6 `BAD_EVIDENCE_GAP`。

`SloObjectiveV1` exact fields: 1 `SloObjectiveNameV1 name`; 2 `SloPopulationV1 population`; 3 `SloThresholdDirectionV1 direction`; 4 `SloThresholdUnitV1 unit`; 5 `uint64 threshold`; 6 `uint64 objective_numerator`; 7 `uint64 objective_denominator`; 8 `uint64 rolling_window_ms`; 9 `uint64 minimum_samples`; 10 `SloTimeoutTreatmentV1 timeout_treatment`=BAD; 11 repeated `DueExclusionReasonV1 exclusions`, strictly numeric-sorted/unique; 12 `uint64 healthy_load_envelope_version`; 13 healthy-load envelope SHA-256[32]; 14 objective digest[32] over fields 1–13. Fractions require `0 < numerator <= denominator`; no float is serialized. Latency/RTO/age/duplicate-window/Lane-age use `AT_MOST+MILLISECONDS`; source margins use `AT_LEAST` with their respective unit. Lane rounds are a separate capacity gate using `AT_MOST+ROUNDS` when published. `DUE_ADMISSION_LAG` requires one paired `HEALTHY` and `ALL_ACCEPTED` objective; exclusions are nonempty only on its HEALTHY definition. `HEALTHY_LANE_DISCOVERY_AGE` and `HEALTHY_LANE_SERVICE_GAP` require population `HEALTHY` and an empty exclusion list. Every other name requires `ALL_ACCEPTED` and empty exclusions. Main-spec §20.3 fixes the sample identity/start/success event for each name; these are not configurable strings.

The closed identity branch messages are: `SloCommandQueuedIdentityV1` fields 1 CommandId[41], 2 command hash[32], 3 PhysicalEnqueueAttemptId[16]; `SloCommandAppliedIdentityV1` field 1 `SourcePositionV1`; `SloDueAdmissionIdentityV1` fields 1 DelayMessageId[41], 2 `uint32 generation`, 3 `int64 path_start_epoch_ms`, 4 `SloPathV1`; `SloNativeHandoffIdentityV1` fields 1 NativeDeliveryId[32], 2 submission hash[32]; `SloQueryIdentityV1` field 1 nonzero random request ID[16]; `SloOwnershipLossIdentityV1` fields 1 `ShardSubjectV1`, 2 `uint64 ownership_loss_epoch`; `SloLocalDiskLossIdentityV1` fields 1 `ShardSubjectV1`, 2 StoreIncarnation[16]; `SloCheckpointAgeIdentityV1` fields 1 `ShardSubjectV1`, 2 `uint64 recovery_set_generation`, 3 `uint64 durable_probe_sequence`; `SloSourceMarginIdentityV1` fields 1 `ShardSubjectV1`, 2 `uint64 recovery_set_generation`, 3 `uint64 broker_log_epoch`, 4 `uint64 durable_probe_sequence`; `SloPossibleDuplicateIdentityV1` field 1 PublishAttemptId[32]; `SloLaneDiscoveryIdentityV1` fields 1 DestinationLaneId[32], 2 LaneIncarnation[16], 3 `uint64 ready_generation` equal to the READY key's `laneVersion`; `SloLaneServiceGapIdentityV1` fields 1 DestinationLaneId[32], 2 LaneIncarnation[16], 3 checked `uint64 service_gap_generation` persisted with `lastServedRound`; `SloLaneRecoveryReadyIdentityV1` fields 1 `ShardSubjectV1`, 2 DestinationLaneId[32], 3 LaneIncarnation[16], 4 `uint64 ownership_loss_epoch`, 5 `uint64 lane_recovery_generation`.

`SloSampleEventIdentityV1` is a closed oneof whose branch number equals `SloObjectiveNameV1`: fields 1–8 select their same-number named type above; fields 9 and 10 each select `SloSourceMarginIdentityV1`; fields 11–14 select their same-number named type. Branch and objective name must agree; an identity message has no unknown fields.

```text
sloSampleId = SHA-256(
  "nereus-delay-slo-sample-v1\0" || objectiveDigest[32] ||
  lp32(canonicalProtobuf(SloSampleEventIdentityV1))
)
```

`SloTimeEndpointV1` exact fields: 1 `SloTimeEndpointKindV1 kind`; 2 `int64 earliest_epoch_ms`; 3 `int64 latest_epoch_ms`; 4 evidence SHA-256[32]. Require `0 <= earliest <= latest`; semantic fixed time uses an identity-bound semantic evidence digest, Broker time uses the exact Broker receipt/Source Position digest, and a trusted observation uses the exact `TrustedUtcIntervalEvidenceV1` digest.  
`SloSampleStartV1` exact fields: 1 version=1; 2 objective digest[32]; 3 `SloObjectiveNameV1`; 4 `SloPopulationV1`; 5 `SloPathV1`; 6 `SloSampleEventIdentityV1`; 7 sample ID[32]; 8 `SloTimeEndpointV1 start`; optional 9 `int64 timeout_at_epoch_ms`; 10 start digest[32] over fields 1–9. Field 7 must equal the formula above. `AT_MOST` samples require field 9 equal checked `start.earliest_epoch_ms + threshold`, the conservative deadline for the upper-bound SLI; `AT_LEAST` point samples forbid it.  
`SloSampleFinalV1` exact fields: 1 version=1; 2 sample ID[32]; 3 start digest[32]; 4 `SloFinalOutcomeV1`; 5 `SloThresholdUnitV1`; 6 `uint64 measured_lower`; 7 `uint64 measured_upper`; optional 8 `DueExclusionReasonV1 exclusion_reason`; 9 `SloTimeEndpointV1 final_observation`; 10 source-event evidence SHA-256[32]; 11 `uint64 observation_revision`; 12 final digest[32] over fields 1–11. Require lower <= upper. Exclusion is present only for the `ALL_ACCEPTED` companion of due Admission, must be a member of that event's paired HEALTHY objective field-11 exclusion set, and never removes the ALL_ACCEPTED sample; a HEALTHY final forbids it. `SUCCESS` must be backed by the objective's exact success event. A missing/uncertifiable endpoint is a bad outcome, never a zero-duration success.

For millisecond duration objectives, checked arithmetic fixes `measured_lower=max(0, final.earliest-start.latest)` and `measured_upper=max(0, final.latest-start.earliest)`. `CHECKPOINT_AGE` uses the selected checkpoint creation interval as Start and the durable probe interval as Final. Source-retention TIME/BYTE margins are point gauges: the exact Adapter probe evidence directly supplies a nonnegative `[lower,upper]`, and field 9 is the probe time rather than an operand; TIME uses milliseconds and BYTE uses bytes. Lane rounds, when published, use the durable DRR generation delta and set lower=upper. Overflow or an evidence/unit/objective mismatch is `BAD_UNQUALIFIED_TIME`/`BAD_EVIDENCE_GAP`, never saturation or zero. `SloPathV1` is `ORDINARY_MANAGED` or `MANAGED_PULSAR_HANDOFF` only for due Admission, `AUTO_FAST_NATIVE` only for native handoff, and `NOT_APPLICABLE` otherwise.

One component-local durable outbox record stores the exact Start and the conservative merged Final. Same sample ID with a different start digest is integrity failure. Repeated Finals merge monotonically: any `BAD_*` dominates `SUCCESS`; among `AT_MOST` observations keep maximum upper/lower, among `AT_LEAST` keep minimum lower/upper, and never remove an exclusion/evidence-gap marker. Observation revision checked-increments. Thus a replay may reproduce a later/worse observation but cannot improve or delete a previously bad sample. Export is at-least-once; the evidence collector dedupes by `(sampleId, startDigest)` and applies the same merge.

For `DUE_ADMISSION_LAG`, the `ALL_ACCEPTED` Start is materialized immediately from the durable Message/eligibility authority. The `HEALTHY` sample is not tentatively created and later deleted: only after the complete start-to-Final interval has durable evidence for every configured healthy predicate may the component reconstruct the byte-identical semantic Start under the paired HEALTHY objective digest and emit its Start/Final. A non-qualifying interval remains only in the ALL_ACCEPTED denominator with exactly one closed exclusion reason. The objective catalog binds each HEALTHY digest to one same-event ALL_ACCEPTED digest; missing companion, health-interval evidence, or exclusion evidence is `BAD_EVIDENCE_GAP`, never denominator shrinkage.

### 6.3 Public API and receipt fields

Serialized receipts use this exact frame and unpadded Base64url text prefix `ndr1_`:

```text
magic:u32be             = 4e445231 (ASCII NDR1)
receiptFramingVersion:u8 = 01
receiptKind:u8
flags:u16be             = 0000
payloadLength:u32be
canonical receipt payload
crc32c:u32be            = CRC32C(all preceding bytes)
```

Total length is `16 + payloadLength`; trailing bytes, unknown flags/kind, noncanonical payload, or CRC mismatch is `INVALID_RECEIPT`. CRC is accidental-corruption detection, not tenant authorization or result authority. The gateway authorizes the Route from authenticated context before resolving owner/position; the shard then compares the receipt to durable position/result evidence. A syntactically valid but unequal authorized locator is `RECEIPT_MISMATCH`.

`PreparedCommandRefV1` exact fields: 1 Route UUID[16]; 2 partition `uint32`; 3 CommandId[41]; 4 DelayMessageId[41]; 5 `CommandTypeV1`; 6 `ProtocolTupleV1`; 7 command hash[32]; 8 `int64 retry_until_epoch_ms`; 9 exact NDL1 frame SHA-256[32].

`SafeBrokerAckV1` is a closed oneof field 1 `KafkaQueuedAckV1 kafka` / field 2 `PulsarQueuedAckV1 pulsar`. Kafka fields: 1 `BrokerResourceIdentityV1`; 2 partition `uint32`; 3 offset `uint64`; optional 4 leader epoch `uint32`; 5 broker log-append time `int64`; 6 authenticated response SHA-256[32]. Pulsar fields: 1 resource; 2 partition; 3 ledger ID `uint64`; 4 entry ID `uint64`; 5 normalized batch index `uint32`; 6 batch size `uint32`; 7 broker entry timestamp `int64`; 8 SEND receipt SHA-256[32]. It contains no open metadata map.

`CommandQueuedReceiptV1` payload fields: 1 `uint32 receipt_version`=1; 2 `PreparedCommandRefV1 command`; 3 `SourcePositionV1 source_position`; 4 `SafeBrokerAckV1 broker_ack`; 5 `int64 receipt_query_until`; 6 `uint32 capability_bits` exactly `QUERY|CANCEL|RESCHEDULE|SERVER_QUOTA|SERVER_AUDIT`; 7 PhysicalEnqueueAttemptId[16]; 8 `bytes receipt_payload_digest`=32. Field 8 equals SHA-256 of domain `nereus-delay-command-queued-receipt-v1\0` plus canonical fields 1–7. Source Position and Broker ACK identities/position/timestamp must agree.

Field 5 must equal `checkedAdd(sourcePosition.brokerPersistenceTime, queuedReceiptQueryWindow)` from the immutable Route query-policy version. `CommandAppliedReceiptV1.full_result_retain_until` must equal `checkedAdd(firstSourcePosition.brokerPersistenceTime, fullCommandResultRetention)` from the version active for that first result. Neither boundary uses SDK receipt time or Worker apply wall clock; overflow makes the Route configuration uncertifiable.

`PublicDestinationBindingViewV1` exact fields: 1 destination `ProfileRefV1`; 2 capability `ProfileRefV1`; 3 `AdapterKindV1`; 4 policy-approved `bytes destination_alias_utf8_nfc`; 5 physical partition `uint32`; 6 `OrderingModeV1`. Field 4 is a bounded immutable safe display alias from the Profile policy, never the canonical topic, endpoint, cluster/resource token, object identity or credential reference. `BrokerResourceIdentityV1`, DestinationLaneId, private evidence and internal producer identity are forbidden from this public projection.

`CommandAppliedReceiptV1` payload fields: 1 `uint32 receipt_version`=1; 2 queued receipt payload digest[32]; 3 `CommandApplyStatusV1`; 4 `StableCodeV1`; 5 `SourcePositionV1 applied_source_position`; optional 6 generation `uint32`; optional 7 state version `uint64`; optional 8 `PublicDestinationBindingViewV1`; 9 `int64 full_result_retain_until`; 10 receipt payload digest[32], domain-separated over fields 1–9. Optional message fields are present only when that outcome has a real Message/Reservation generation; rejection/`NOT_FOUND` cannot fabricate them.

`PayloadReservationReceiptV1` payload fields: 1 version=1; 2 ReservationId[32]; 3 DelayMessageId[41]; 4 Route UUID[16]; 5 partition `uint32`; 6 applied `SourcePositionV1`; 7 state version `uint64`; 8 `ProfileRefV1 object_store_profile`; 9 exact container bytes; 10 exact service-owned object-key bytes; 11 `uint64 expected_length`; 12 payload SHA-256[32]; 13 `int64 reservation_expiry`; 14 `PayloadProofTrustSetRefV1`; 15 payload digest[32] over fields 1–14. It is emitted only by an APPLIED Prepare result.

`ControlOperationReceiptV1` payload fields: 1 version=1; 2 Control Operation ID[32]; 3 request hash[32]; 4 authenticated tenant/resource scope hash[32]; 5 exact target-snapshot hash[32]; 6 operation revision `uint64`; 7 `TrustedUtcIntervalEvidenceV1 registered_at`; 8 `int64 query_until`; 9 payload digest[32] over fields 1–8. Field 8 equals `checkedAdd(registered_at.latest_epoch_ms, controlOperationQueryWindow)` from the immutable control retention policy. Control operation ID is a nonzero 32-byte cryptographic random value fixed in `PreparedControlOperation` before I/O; it is never regenerated on an uncertain registration.

`ControlMessageTargetV1` exact fields: 1 DelayMessageId[41]; 2 `uint32 expected_generation`; 3 `uint64 expected_state_version`; optional 4 PublishAttemptId[32]. Replay targets the current Dead Letter Generation and forbids field 4. Resolve requires field 4. Its evidence-attachment branches may target either the current Generation or a retained older terminal Generation when that exact attempt remains in the terminal open-obligation summary; field 3 then equals that Generation's immutable control/state version. Retry and possible-delivery terminalization branches require the current nonterminal Generation. A newer current Generation therefore does not prevent closing old evidence, but old evidence can never authorize work or terminalize the newer Generation. `ProfileControlTargetV1` fields: 1 `ProfileRefV1`; optional 2 raw `uint64 expected_secret_generation` (nonzero); optional 3 expected credential-binding digest[32]; optional 4 nonzero checked raw `uint64 expected_binding_head_revision`. Fields 2–4 are present together exactly for secret rotation and forbidden together for other Profile operations. `ControlTargetRefV1` exact fields: 1 `uint32 target_index`; 2 `ControlTargetKindV1`; closed oneof field 10 `ShardSubjectV1 shard`, 11 `LaneControlTargetV1 lane`, 12 `ControlMessageTargetV1 message`, 13 Route UUID[16], 14 `ProfileControlTargetV1 profile`, 15 `QuotaGrantRefV1 quota_grant`; optional pair 20 expected System Mutation ID[32] / 21 expected mutation hash[32]; 22 target digest[32] over fields 1–21. Source-ordered control/message targets require fields 20–21 after exact mutation construction; Drain/Fence/ForceCheckpoint/GetCatalog/profile-storage/secret targets forbid them. Targets are strictly index-sorted, unique and immutable.

`ControlOperationRequestV1` is a closed oneof whose field number equals `ControlOperationKindV1`. Exact branches are: 1 `StopNewSchedulesRequestV1` fields 1 `ControlReasonV1`; 2/3 `LaneGateRequestV1` field 1 reason; 4 `CloseLaneRequestV1` fields 1 reason, 2 `ClosePolicyV1`=1, 3 `bool allow_order_break`, 4 `AcknowledgementSetV1`; 5 `BreakOrderingRequestV1` field 1 acknowledgements; 6 `DrainShardRequestV1` fields 1 reason, 2 `uint64 max_drain_wait_ms`, 3 `bool request_final_checkpoint`; 7 `FenceShardRequestV1` field 1 reason; 8 `ForceCheckpointRequestV1` field 1 reason; 9 `GetCheckpointCatalogRequestV1` empty; 10 `ReplayDeadLetterRequestV1` fields 1 `int64 deliver_at`, 2 `int64 expire_at`, 3 `RetryPolicyRefV1`, 4 `bool allow_possible_duplicate`, 5 `AcknowledgementSetV1`; 11 `ResolveUncertainRequestV1` fields 1 `UncertainResolutionKindV1`, optional 2 `PublishEvidenceV1`, 3 `bool allow_possible_duplicate`, 4 `bool allow_possible_delivery_terminal`, 5 `AcknowledgementSetV1`; 12 `PublishDestinationProfileRequestV1` fields 1 destination `ProfileSemanticEnvelopeV1`, 2 matching generation-1 `CredentialBindingV1`; 13 `DeprecateDestinationProfileRequestV1` fields 1 destination `ProfileRefV1`, 2 reason; 14 `PublishQuotaGrantRequestV1` fields 1 `QuotaGrantRefV1`, optional 2 `QuotaTransferPlanRefV1`; 15 `RotateEquivalentSecretRequestV1` fields 1 `ProfileRefV1`, 2 raw `uint64 expected_secret_generation`, 3 raw `uint64 new_secret_generation`, 4 bounded private new secret-reference bytes, 5 new secret-reference SHA-256[32], 6 `CredentialEquivalenceAttestationV1`, 7 expected credential-binding digest[32], 8 nonzero checked raw `uint64 expected_binding_head_revision`. Operation kind, request branch, target kinds, acknowledgement/evidence presence and required source-mutation targets must match the main-spec operation matrix. Rotation requires kind `DESTINATION` or `OBJECT_STORE`; request fields 2/7/8 must equal Profile target fields 2–4 and current Head fields 2–4; field 3 exactly checked-increments field 2 (the raw all-ones pattern is not incrementable); field 5 equals SHA-256(field 4); and request fields 1/3/5 equal attestation fields 1/2/3. Request fields 1 and 3–6 produce the new canonical `CredentialBindingV1`. Public/audit projections redact field 4 and the attestation's private verifier evidence.

Target presence is closed: Replay/Resolve have exactly one MESSAGE target; Lane operations have one or more LANE targets; Drain/Fence/ForceCheckpoint/GetCatalog have one or more SHARD targets; Stop has one ROUTE plus every frozen SHARD marker target; Profile publish/deprecate have one matching PROFILE target plus every frozen SHARD marker target; quota publication has one matching QUOTA_GRANT target plus all affected SHARD targets; secret rotation has exactly one PROFILE target with the expected generation/binding-digest/Head-revision triplet and no source mutation. Publish Profile requires a `DESTINATION` envelope plus its exact verified generation-1 binding; referenced capability/object semantics are already platform-approved. Secret rotation permits only `DESTINATION` or `OBJECT_STORE`. Any missing, extra or mixed target kind is invalid preparation, not a best-effort subset.

`PreparedControlOperationV1` exact fields: 1 version=1; 2 nonzero random Control Operation ID[32]; 3 `ControlOperationKindV1`; 4 `ControlAuthorV1`; 5 `ControlOperationRequestV1`; 6 request hash[32]; 7 repeated `ControlTargetRefV1` strictly index-sorted/unique; 8 target-snapshot hash[32]; 9 nonzero control-query-policy version `uint64`; 10 `int64 registration_retry_until`; 11 prepared digest[32]; 12 nonzero signing-key version `uint32`; 13 Ed25519 signature[64].

```text
controlRequestHash = SHA-256(
  "nereus-delay-control-request-v1\0" || u16be(operationKind) ||
  lp32(canonicalProtobuf(ControlOperationRequestV1))
)
controlTargetSnapshotHash = SHA-256(
  "nereus-delay-control-target-snapshot-v1\0" ||
  lp32(canonicalProtobuf(repeated ControlTargetRefV1))
)
preparedControlDigest = SHA-256(
  "nereus-delay-prepared-control-v1\0" ||
  canonicalProtobuf(fields 1..10 with original field numbers)
)
```

Fields 6/8/11 must match; field 13 signs field 11. Preparation fixes all bytes before Oxia or Shard Log I/O. `ControlNonPersistenceProofV1` fields: 1 `ControlNonPersistenceProofKindV1`; 2 operation ID[32]; 3 prepared digest[32]; optional 4 exact Oxia transaction SHA-256[32]; optional 5 authenticated response SHA-256[32]; 6 proof digest[32] over fields 1–5. The local branch forbids 4–5; conditional rejection requires them. Timeout/session ambiguity has no proof. `ControlDefinitelyNotRecordedV1` fields 1 prepared digest[32], 2 proof, 3 `StableErrorV1`; `ControlRecordUncertainV1` fields 1 operation ID[32], 2 prepared digest[32], 3 `StableErrorV1`. `ControlRegistrationOutcomeMessageV1` fields 1 `ControlRegistrationOutcomeV1`; closed oneof field 10 receipt, 11 definitely-not-recorded, 12 uncertain. Same prepared bytes/operation ID are mandatory on retry.

`NativeCapabilitySnapshotV1` exact fields: 1 snapshot version=1; 2 destination `ProfileRefV1`; 3 capability `ProfileRefV1`; 4 Pulsar `BrokerResourceIdentityV1 target`; 5 physical partition `uint32`; 6 resource-guard attestation SHA-256[32]; 7 nonzero resource-guard config generation `uint64`; 8 nonzero raw Credential Binding generation `uint64`; 9 credential-binding digest[32]; 10 resolved immutable credential-version/public-fingerprint digest[32]; 11 authenticated SDK principal-scope digest[32]; 12 `TrustedUtcIntervalEvidenceV1 issued_at`; 13 `int64 not_after_epoch_ms`; 14 nonzero issuer signing-key version `uint32`; 15 snapshot digest[32]; 16 Ed25519 signature[64]. Fields 2–5 must resolve to a registered Pulsar Destination/Capability pair whose timing bits include `PULSAR_AUTO_FAST`; fields 6–7 must identify the complete still-valid guarded Broker rollout; fields 8–10 must equal the current Destination `CredentialBindingV1` at issuance; and field 11 must be within both the immutable credential scope and Broker guard principal scope. Require `issued_at.latest_epoch_ms < field13 <= checkedAdd(issued_at.earliest_epoch_ms, maxNativeCapabilitySnapshotLifetime)` and field 13 no later than every bound guard/route/capability prerequisite. Field 15 is SHA-256 of domain `nereus-delay-native-capability-snapshot-v1\0` plus canonical fields 1–14. Field 16 signs `SHA-256("nereus-delay-native-capability-snapshot-signature-v1\0" || field15 || u32be(field14))` under the activated issuer trust set. Before exposing a snapshot, the issuer durably extends the exact binding generation's `nativeCapabilityProtectionUntil >= field13`; failed protection persistence exposes no snapshot.

Snapshot issuance is the native credential-authorization linearization point. An exact signed snapshot remains valid until field 13 even if an equivalent binding rotation occurs later; rotation is not remote revocation. `submit()` validates the full snapshot/signature/expiry, exact prepared projections, and that the SDK credential provider resolves to field 10 before Producer ownership. Expiry is `NATIVE_PREPARED_SUBMISSION_EXPIRED`; another prerequisite failure is `AUTO_FAST_PREREQUISITE_UNAVAILABLE`; credential-version mismatch is `CREDENTIAL_BINDING_DRIFT`. Each yields a definitive local non-persistence proof only before library ownership. Secret reference/plaintext is never in the snapshot or prepared bytes. Emergency stop requires resource-guard/credential revocation and treats a request already owned by the Producer as uncertain.

`NativePreparedRefV1` exact fields: 1 Native Delivery ID[32]; 2 submission hash[32]; 3 destination `ProfileRefV1`; 4 target `BrokerResourceIdentityV1`; 5 physical partition `uint32`; 6 capability snapshot digest[32]; 7 `int64 capability_expiry`; 8 exact prepared bytes SHA-256[32]. Native Delivery ID is a nonzero 32-byte cryptographic random value generated before I/O and stored with the prepared bytes.

`NativePreparedDeliveryV1` exact fields: 1 version=1; 2 Native Delivery ID[32]; 3 destination `ProfileRefV1`; 4 capability `ProfileRefV1`; 5 Pulsar `BrokerResourceIdentityV1`; 6 physical partition `uint32`; 7 inline payload bytes; 8 `PulsarMetadataV1`; optional 9 `int64 event_time`; 10 business `int64 deliver_at`; 11 shifted Broker `int64 deliver_at`; 12 `NativeCapabilitySnapshotV1 capability_snapshot`; 13 resource-guard attestation SHA-256[32]; 14 `int64 capability_expiry`; 15 `uint32 native_encoding_version`=1; 16 submission hash[32]. Fields 3–6 must equal snapshot fields 2–5; fields 13–14 equal snapshot fields 6/13. Field 16 is SHA-256 of domain `nereus-delay-native-submission-v1\0` plus canonical fields 1–15. Field 11 must be the certified shift derived by the exact timing-policy version in fields 3–4 and cannot be recomputed from current config after preparation. The canonical prepared bytes are this entire message; `NativePreparedRefV1` must project byte-equal fields, snapshot field 15, field 14, and the prepared bytes' SHA-256.

`NativeDeliveryReceiptV1` payload fields: 1 version=1; 2 `NativePreparedRefV1`; 3 `SafeBrokerAckV1`; 4 `uint32 capability_bits`=0; 5 PhysicalEnqueueAttemptId[16]; 6 payload digest[32] over fields 1–5. It can never be decoded as a managed Message/Command locator, and its Broker ACK branch must be Pulsar.

`StableErrorV1` exact fields: 1 `FailureStageV1 stage`; 2 `StableCodeV1 code`; 3 `RetryabilityV1 retryability`; optional 4 `int64 retry_at`; optional 5 `PreparedCommandRefV1 command`; optional 6 `NativePreparedRefV1 native`; optional 7 bounded allowlisted diagnostic code `uint32`. `RETRY_EXACT_BYTES_AFTER_RETRY_AT` requires field 4 and all other retryability values forbid it. `NEW_PREPARATION_REQUIRED` explicitly forbids reusing the rejected/expired prepared identity; `RETRY_EXACT_BYTES*` explicitly forbids generating a replacement identity. Free-form exception text is not serialized into this contract.

`NonPersistenceProofV1` exact fields: 1 `NonPersistenceProofKindV1 kind`; 2 `uint32 adapter_proof_version`=1; optional 3 PhysicalEnqueueAttemptId[16]; 4 exact prepared frame/submission SHA-256[32]; optional 5 `BrokerResourceIdentityV1 broker_resource`; optional 6 exact Broker request SHA-256[32]; optional 7 authenticated response SHA-256[32]; 8 proof digest[32] over fields 1–7. Local/pre-ownership branches forbid 5–7; Kafka/Pulsar rejection branches require 3 and 5–7; library-certified cancellation requires 3 plus an activated adapter conformance version encoded in field 2. A timeout, Future cancellation, connection loss, process exit, missing callback, or unverified exception has no legal proof branch.

Public unions are closed: `EnqueueOutcomeV1` oneof is queued receipt / definitely-not-queued `{PreparedCommandRefV1, NonPersistenceProofV1, StableErrorV1}` / enqueue-uncertain `{PreparedCommandRefV1, PhysicalEnqueueAttemptId[16], StableErrorV1}`; `SubmissionOutcomeV1` oneof is managed `EnqueueOutcomeV1` / native receipt / native-definitely-not-queued `{NativePreparedRefV1, NonPersistenceProofV1, StableErrorV1}` / native-uncertain `{NativePreparedRefV1, PhysicalEnqueueAttemptId[16], StableErrorV1}`. The SDK allocates the physical ID immediately before each Producer submission attempt; purely local rejection can therefore have no physical ID inside its proof. A physical ID never enters the Prepared Command/command hash and changes on a later physical retry. A batch returns one `EnqueueOutcomeV1` per input in the same order and has no cross-element atomic tag.

The exact messages are: `DefinitelyNotQueuedV1` fields 1 `PreparedCommandRefV1`, 2 `NonPersistenceProofV1`, 3 `StableErrorV1`; `EnqueueUncertainV1` fields 1 prepared ref, 2 PhysicalEnqueueAttemptId[16], 3 stable error; `EnqueueOutcomeMessageV1` fields 1 `EnqueueOutcomeV1 outcome_kind`, closed oneof field 10 `CommandQueuedReceiptV1 queued`, 11 `DefinitelyNotQueuedV1`, 12 `EnqueueUncertainV1`. `NativeDefinitelyNotQueuedV1` fields 1 `NativePreparedRefV1`, 2 proof, 3 error; `NativeEnqueueUncertainV1` fields 1 native ref, 2 PhysicalEnqueueAttemptId[16], 3 error. `SubmissionOutcomeMessageV1` fields 1 `SubmissionOutcomeKindV1`, closed oneof field 10 managed `EnqueueOutcomeMessageV1`, 11 `NativeDeliveryReceiptV1`, 12 `NativeDefinitelyNotQueuedV1`, 13 `NativeEnqueueUncertainV1`. `PreparedSubmissionV1` fields 1 version=1 and closed oneof field 2 exact managed NDL1 frame bytes / field 3 `NativePreparedDeliveryV1`; branch never changes after preparation. Kind/branch/stable code/retryability/proof presence must agree. Batch response is repeated field 1 `EnqueueOutcomeMessageV1` preserving input order, with no map or aggregate atomic status.

`OpaquePayloadUploadHandleV1` exact fields: 1 version=1; 2 ReservationId[32]; 3 `ProfileRefV1 object_store_profile`; 4 `UploadHandleKindV1`; 5 `int64 expires_at_epoch_ms`; 6 bounded sensitive `bytes capability_envelope`; 7 envelope SHA-256[32]. Field 6 is interpreted only by the activated Object Store Adapter/Profile version, must be authenticated/encrypted or provider-signed, and is forbidden from logs, metrics, receipts, DB and checkpoints. It cannot change the reservation's service-owned object identity.  
`PayloadUploadHandleResponseV1` exact fields: 1 `PayloadUploadHandleOutcomeV1 outcome_kind`; closed oneof field 10 `OpaquePayloadUploadHandleV1 issued`; fields 11–18 `StableErrorV1` for `RESERVATION_EXPIRED`, `RESERVATION_ABANDONED`, `RESERVATION_CLOSED`, `NOT_FOUND_OR_NOT_AUTHORIZED`, `SHARD_TRANSITIONING`, `SHARD_UNAVAILABLE`, `INTEGRITY_ERROR`, `OBJECT_STORE_UNAVAILABLE_RETRYABLE`. `PayloadAttestationResponseV1` fields: 1 `PayloadAttestationOutcomeV1`; closed oneof field 10 `PayloadCommitProofV1 attested`; fields 11–20 `StableErrorV1` in enum order for the ten failure outcomes. All error branches require `stage=PAYLOAD`, forbid prepared/native refs, and fix the matching stable code/retryability; retryable object/shard-transition branches carry `retry_at`, all others forbid it. Object Store Credential Binding unavailability/drift projects to `OBJECT_STORE_UNAVAILABLE_RETRYABLE` without exposing credential identity; a private operator metric distinguishes `CREDENTIAL_BINDING_DRIFT`. Branch and enum must agree.

#### 6.3.1 Public query response schemas

`EmptyResultV1` has no fields. `PublicQueryErrorV1` exact fields are 1 `StableCodeV1 code`; optional 2 `int64 retry_at_epoch_ms`. Only `SHARD_TRANSITIONING` requires field 2; every other query code forbids it. Each error branch below fixes its exact code and may not carry endpoint, identity, payload or free text.

`PendingCommandViewV1` exact fields: 1 awaited `SourcePositionV1`; optional 2 current applied `SourcePositionV1`; 3 `int64 retry_at_epoch_ms`. Current position, when present, must be strictly before awaited under the same source identity.  
`PublicCommandResultV1` exact fields: 1 `CommandApplyStatusV1`; 2 `StableCodeV1`; 3 applied `SourcePositionV1`; optional 4 `uint32 generation`; optional 5 `uint64 state_version`; optional 6 `PublicDestinationBindingViewV1`; 7 `int64 full_result_retain_until`. Presence equals `CommandAppliedReceiptV1` fields 6–8 but this query view has no queued-receipt digest and therefore works for a bare Command locator.  
`CompactCommandResultV1` exact fields: 1 `CommandApplyStatusV1`; 2 `StableCodeV1`; 3 first applied `SourcePositionV1`; 4 `int64 full_result_retain_until`. It contains no reconstructed message/binding fields.

`CommandQueryResponseV1` exact fields are 1 version=1; 2 `CommandQueryResultV1 result_kind`; closed oneof: field 10 `PendingCommandViewV1 pending`; 11 `PublicCommandResultV1 applied`; 12 `PublicCommandResultV1 rejected`; 13 `CompactCommandResultV1 result_expired`; 14 `EmptyResultV1 result_evidence_expired`; 15 `EmptyResultV1 unknown`; fields 16–21 `PublicQueryErrorV1` for, respectively, invalid receipt, receipt mismatch, not-found-or-not-authorized, shard transitioning, shard unavailable and integrity error. Field 11 requires status APPLIED, field 12 REJECTED; branch, enum and stable code must agree. Bare locators forbid branches 10/14/16/17.

`ReservedMessageViewV1` exact fields: 1 ReservationId[32]; 2 `uint64 state_version`; 3 `PayloadReservationStateV1`; 4 `int64 reservation_expiry`; 5 `PublicDestinationBindingViewV1`; 6 `PayloadAvailabilityV1`, which must be `UPLOAD_PENDING` while reserved.  
`ActiveMessageViewV1` exact fields: 1 `uint32 generation`; 2 `uint64 state_version`; 3 `MessageGenerationStateV1`; 4 `int64 deliver_at`; 5 `int64 expire_at`; 6 `PublicDestinationBindingViewV1`; 7 `PayloadAvailabilityV1`; 8 `bool possible_destination_duplicate`. State is restricted to `SCHEDULED/CLAIMED/PUBLISHING/RETRY_WAIT/UNCERTAIN`; `HANDED_OFF` is terminal for management/query projection.  
`TerminalMessageViewV1` exact fields: 1 `uint32 generation`; 2 `uint64 state_version`; 3 `MessageGenerationStateV1`; 4 `StableCodeV1 terminal_code`; 5 `PublicDestinationBindingViewV1`; 6 `PayloadAvailabilityV1`; 7 `DlqExportStateV1`; 8 `bool possible_destination_duplicate`; optional 9 `PublicEvidenceRefV1`. State is restricted to `PUBLISHED/HANDED_OFF/CANCELED/EXPIRED/DEAD_LETTER/SUPERSEDED`.  
`IdentityRetiredMessageViewV1` and `UnknownMessageViewV1` have, respectively, no fields and field 1 `FirstScheduleEligibilityV1`. Unknown uses `EXPIRED_BY_SOURCE_FENCE` only when the self-routing UUID freshness deadline is proven closed; otherwise it uses `NOT_PROVEN`.

`MessageQueryResponseV1` exact fields are 1 version=1; 2 `MessageQueryResultV1 result_kind`; closed oneof: fields 10 `ReservedMessageViewV1`, 11 `ActiveMessageViewV1`, 12 `TerminalMessageViewV1`, 13 `IdentityRetiredMessageViewV1`, 14 `UnknownMessageViewV1`; fields 15–20 `PublicQueryErrorV1` for invalid receipt, receipt mismatch, not-found-or-not-authorized, shard transitioning, shard unavailable and integrity error. Branch, result tag, state subset and stable code must agree. Bare Message ID forbids receipt-only branches 15–16.

`ControlTargetStateViewV1` exact fields: 1 `uint32 target_index`; 2 `TargetMarkerStateV1`; 3 `StableCodeV1`; 4 `uint64 target_revision`; optional 5 applied `SourcePositionV1`; 6 target-result digest[32]. Target array is strictly increasing by index and contains every immutable target exactly once.  
`LaneControlResultV1` fields: 1 DestinationLaneId[32], 2 LaneIncarnation[16], 3 `uint64 lane_control_version`, 4 `LaneAdmissionGateV1`, 5 `uint64 outstanding_attempts`, 6 `StableCodeV1`. `ShardControlResultV1`: 1 `ShardSubjectV1`, 2 `ShardLifecycleStateV1`, optional 3 `uint64 owner_epoch`, 4 `StableCodeV1`. `CheckpointControlResultV1`: 1 shard, 2 checkpoint ID[16], 3 manifest hash[32], 4 `uint64 catalog_generation`. `ProfileControlResultV1`: 1 `ProfileRefV1`, 2 `ProfileAcceptanceV1`, optional 3 nonzero raw `uint64 current_secret_generation` required exactly for `DESTINATION`/`OBJECT_STORE`. `QuotaControlResultV1`: 1 `QuotaGrantRefV1`, 2 persisted usage digest[32]. `MessageControlResultV1`: 1 DelayMessageId[41], 2 `uint32 generation`, 3 `uint64 state_version`, 4 `MessageGenerationStateV1`, 5 `StableCodeV1`, optional 6 `PublicEvidenceRefV1`. `CheckpointSummaryV1`: 1 checkpoint ID[16], 2 manifest hash[32], 3 applied `SourcePositionV1`, 4 `uint64 catalog_generation`, 5 `bool is_recovery_floor`. `CheckpointCatalogResultV1`: 1 `ShardSubjectV1`, 2 Recovery Lineage ID[16], 3 floor checkpoint ID[16], 4 floor manifest hash[32], 5 `uint64 catalog_generation`, 6 repeated `CheckpointSummaryV1` strictly `(catalog_generation,checkpoint_id)` sorted/unique. `RouteControlResultV1`: 1 Route UUID[16], 2 `RouteLifecycleV1`, 3 `uint64 control_version`. `SecretRotationResultV1`: 1 `ProfileRefV1`, 2 raw `uint64 secret_generation` (nonzero), 3 safe secret-reference digest[32], 4 credential-binding digest[32], 5 nonzero checked raw `uint64 binding_head_revision`, 6 binding-head digest[32].

`ControlTypedResultV1` is a closed oneof whose fields 1–9 and branch types are exactly the same-number `ControlResultKindV1` variants listed above. `CurrentControlOperationV1` exact fields: 1 Control Operation ID[32]; 2 request hash[32]; 3 authenticated scope hash[32]; 4 `ControlOperationStateV1`; 5 `uint64 operation_revision`; 6 repeated `ControlTargetStateViewV1`; optional 7 `ControlTypedResultV1`. Typed result is absent while no operation-specific result exists and required for `SUCCEEDED/SUCCEEDED_WITH_OUTSTANDING`; its branch must match the operation kind.  
`ControlOperationQueryResponseV1` exact fields: 1 version=1; 2 `ControlOperationQueryResultV1 result_kind`; closed oneof field 10 `CurrentControlOperationV1 current`; fields 11–13 `PublicQueryErrorV1` fixed respectively to `INVALID_RECEIPT`, `NOT_FOUND_OR_NOT_AUTHORIZED`, `INTEGRITY_ERROR`.

### 6.4 Stable code

`StableCodeV1` 是 `u32`。下面是 closed V1 表；diagnostic text 不影响 code/retryability。

| range / code | symbolic value | retryability |
|---|---|---|
| `0x0000` | `OK` | no |
| `0x0101` | `INVALID_COMMAND` | no-until-corrected |
| `0x0102` | `INVALID_PREPARED_COMMAND` | no |
| `0x0103` | `PAYLOAD_TOO_LARGE` | no-until-corrected |
| `0x0104` | `INVALID_METADATA` | no-until-corrected |
| `0x0105` | `UNSUPPORTED_DELIVERY_MODE` | no-until-corrected |
| `0x0106` | `ROUTE_SNAPSHOT_UNAVAILABLE` | yes-after-snapshot-refresh |
| `0x0107` | `PREPARED_COMMAND_EXPIRED` | no-for-same-prepared-command |
| `0x0108` | `CLIENT_CLOSED` | conditional-by-ownership-state |
| `0x0109` | `AUTO_FAST_PREREQUISITE_UNAVAILABLE` | retry-by-new-prepare-before-IO |
| `0x010a` | `NATIVE_PREPARED_SUBMISSION_EXPIRED` | no-for-same-native-prepared |
| `0x0201` | `SDK_BACKPRESSURE_NOT_SUBMITTED` | yes-same-bytes |
| `0x0202` | `BROKER_DEFINITIVE_NOT_PERSISTED` | yes-same-bytes |
| `0x0203` | `ENQUEUE_RESULT_UNCERTAIN` | retry-exact-bytes |
| `0x0204` | `PREPARED_SUBMISSION_MISMATCH` | no |
| `0x0205` | `NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED` | conditional-after-resource-change |
| `0x0206` | `NATIVE_ENQUEUE_RESULT_UNCERTAIN` | retry-exact-native-bytes |
| `0x0207` | `BROKER_RESOURCE_UNCERTIFIED` | conditional-after-attestation |
| `0x0208` | `PRODUCER_OWNERSHIP_UNKNOWN` | retry-exact-bytes |
| `0x1001` | `SCHEDULED` | no |
| `0x1002` | `CANCELED` | no |
| `0x1003` | `SUPERSEDED` | no |
| `0x1004` | `NOT_FOUND` | no |
| `0x1005` | `TOO_LATE` | no |
| `0x1006` | `ALREADY_PUBLISHED` | no |
| `0x1007` | `ALREADY_CANCELED` | no |
| `0x1008` | `VERSION_CONFLICT` | no |
| `0x1009` | `ALREADY_ABANDONED` | no |
| `0x100a` | `ALREADY_COMMITTED` | no |
| `0x100b` | `RESERVATION_NOT_COMMITTED` | no |
| `0x100c` | `ALREADY_EXPIRED` | no |
| `0x100d` | `ALREADY_DEAD_LETTERED` | no |
| `0x100e` | `GENERATION_SUPERSEDED` | no |
| `0x1101` | `INVALID_DELIVERY_WINDOW` | no |
| `0x1102` | `UNAUTHORIZED` | no |
| `0x1103` | `DESTINATION_NOT_ALLOWED` | no |
| `0x1104` | `HARD_QUOTA_EXCEEDED` | conditional-after-capacity-change |
| `0x1105` | `ROUTE_NOT_ACTIVE` | conditional-after-control-change |
| `0x1106` | `COMMAND_ID_CONFLICT` | no |
| `0x1107` | `COMMAND_RETRY_WINDOW_EXPIRED` | no |
| `0x1108` | `DELAY_MESSAGE_ID_CONFLICT` | no |
| `0x1109` | `DELAY_MESSAGE_ID_EXPIRED` | no |
| `0x110a` | `INGRESS_ROUTE_MISMATCH` | no |
| `0x110b` | `PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION` | no-for-same-record |
| `0x110c` | `PROFILE_DEPRECATED_FOR_NEW_USE` | no-for-new-binding |
| `0x110d` | `LANE_CLOSED` | no |
| `0x110e` | `LANE_TERMINALLY_CLOSED` | no |
| `0x110f` | `ORDERING_DOMAIN_BROKEN` | no |
| `0x1110` | `DESTINATION_LANE_LIMIT_EXCEEDED` | conditional-after-policy-change |
| `0x1111` | `ORDERING_CAPABILITY_UNAVAILABLE` | conditional-after-profile-change |
| `0x1112` | `SERVER_INVALID_COMMAND` | no-for-same-record |
| `0x1113` | `SERVER_PAYLOAD_TOO_LARGE` | no-for-same-record |
| `0x1114` | `SERVER_INVALID_METADATA` | no-for-same-record |
| `0x1115` | `SERVER_UNSUPPORTED_DELIVERY_MODE` | no-for-same-record |
| `0x1116` | `RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION` | no-for-same-record |
| `0x2001` | `UNACTIVATED_PROTOCOL_VERSION` | no-for-same-record |
| `0x2002` | `UNACTIVATED_SYSTEM_PROTOCOL_VERSION` | no-for-same-record |
| `0x2003` | `UNAUTHORIZED_SYSTEM_MUTATION` | no |
| `0x2004` | `SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED` | no |
| `0x2005` | `STALE_SYSTEM_MUTATION` | no |
| `0x2006` | `ADMISSION_CAPACITY_GATED` | retry-by-new-mutation-after-capacity |
| `0x2007` | `QUARANTINED_SOURCE_RECORD` | no-for-same-bytes |
| `0x3001` | `INVALID_RECEIPT` | no |
| `0x3002` | `RECEIPT_MISMATCH` | no |
| `0x3003` | `RESULT_EXPIRED` | no |
| `0x3004` | `RESULT_EVIDENCE_EXPIRED` | no |
| `0x3005` | `NOT_FOUND_OR_NOT_AUTHORIZED` | no-information |
| `0x3006` | `SHARD_TRANSITIONING` | yes-with-retry-after |
| `0x3007` | `SHARD_UNAVAILABLE` | conditional-after-repair |
| `0x3008` | `INTEGRITY_ERROR` | no-until-repair |
| `0x4001` | `PAYLOAD_OBJECT_CONFLICT` | no |
| `0x4002` | `PAYLOAD_COMMIT_CONFLICT` | no |
| `0x4003` | `PAYLOAD_PROOF_INVALID` | no |
| `0x4004` | `PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION` | no-for-same-record |
| `0x4005` | `RESERVATION_EXPIRED` | no |
| `0x4006` | `RESERVATION_ABANDONED` | no |
| `0x4007` | `PAYLOAD_RESERVATION_CLOSED` | no |
| `0x4008` | `OBJECT_NOT_READY_RETRYABLE` | yes |
| `0x4009` | `OBJECT_STORE_UNAVAILABLE_RETRYABLE` | yes |
| `0x400a` | `OBJECT_IDENTITY_CONFLICT` | no |
| `0x400b` | `PAYLOAD_RESERVATION_ABANDONED` | no |
| `0x5001` | `CAPABILITY_UNAVAILABLE` | conditional |
| `0x5002` | `RESOURCE_INCARNATION_MISMATCH` | no-for-old-resource |
| `0x5003` | `SOURCE_INCARNATION_MISMATCH` | no-for-old-resource |
| `0x5004` | `PULSAR_EVIDENCE_DIVERGENCE` | no-until-repair |
| `0x5005` | `RECOVERY_FIRST_SEND_UNCERTAIN` | no |
| `0x5006` | `CLOSED_WITH_OUTSTANDING_ATTEMPTS` | no |
| `0x5007` | `LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED` | no |
| `0x5008` | `LANE_CLOSED_BEFORE_ADMISSION` | no |
| `0x5009` | `REPLAY_WINDOW_EXPIRED` | no |
| `0x500a` | `ALREADY_OPEN` | no |
| `0x500b` | `CREDENTIAL_EQUIVALENCE_NOT_PROVEN` | no-for-same-binding |
| `0x500c` | `CREDENTIAL_BINDING_DRIFT` | conditional-after-secret-repair |
| `0x5101` | `CLAIM_PERMANENT_FAILURE` | no-client-retry |
| `0x5102` | `DESTINATION_DEFINITIVE_RETRIABLE` | no-client-retry |
| `0x5103` | `DESTINATION_DEFINITIVE_PERMANENT` | no-client-retry |
| `0x5104` | `DESTINATION_OUTCOME_UNKNOWN` | no-client-retry |
| `0x5105` | `DLQ_EXPORT_DEFINITIVE_RETRIABLE` | no-client-retry |
| `0x5106` | `DLQ_EXPORT_DEFINITIVE_PERMANENT` | no-client-retry |
| `0x5107` | `DLQ_EXPORT_OUTCOME_UNKNOWN` | no-client-retry |
| `0x6001` | `SOURCE_GAP` | no-until-repair |
| `0x6002` | `UNSUPPORTED_ACTIVATED_PROTOCOL` | no-until-compatible-deploy |
| `0x6003` | `FENCE_STALLED_CAPACITY` | yes-after-envelope-change |

The table's prose hint does not permit an implementation-local boolean. `StableErrorV1.retryability` is the following closed projection; every code not listed in a non-default set is `NEVER`:

- `RETRY_EXACT_BYTES`: `CLIENT_CLOSED`, `SDK_BACKPRESSURE_NOT_SUBMITTED`, `BROKER_DEFINITIVE_NOT_PERSISTED`, `ENQUEUE_RESULT_UNCERTAIN`, `NATIVE_ENQUEUE_RESULT_UNCERTAIN`, `PRODUCER_OWNERSHIP_UNKNOWN`.
- `RETRY_EXACT_BYTES_AFTER_RETRY_AT`: `SHARD_TRANSITIONING`, `OBJECT_NOT_READY_RETRYABLE`, `OBJECT_STORE_UNAVAILABLE_RETRYABLE`.
- `NEW_PREPARATION_REQUIRED`: `INVALID_COMMAND`, `INVALID_PREPARED_COMMAND`, `PAYLOAD_TOO_LARGE`, `INVALID_METADATA`, `UNSUPPORTED_DELIVERY_MODE`, `ROUTE_SNAPSHOT_UNAVAILABLE`, `PREPARED_COMMAND_EXPIRED`, `AUTO_FAST_PREREQUISITE_UNAVAILABLE`, `NATIVE_PREPARED_SUBMISSION_EXPIRED`, `PREPARED_SUBMISSION_MISMATCH`, `NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED`, `NOT_FOUND`, `VERSION_CONFLICT`, `RESERVATION_NOT_COMMITTED`, `INVALID_DELIVERY_WINDOW`, `UNAUTHORIZED`, `DESTINATION_NOT_ALLOWED`, `HARD_QUOTA_EXCEEDED`, `ROUTE_NOT_ACTIVE`, `COMMAND_ID_CONFLICT`, `COMMAND_RETRY_WINDOW_EXPIRED`, `DELAY_MESSAGE_ID_CONFLICT`, `DELAY_MESSAGE_ID_EXPIRED`, `INGRESS_ROUTE_MISMATCH`, `PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION`, `PROFILE_DEPRECATED_FOR_NEW_USE`, `LANE_CLOSED`, `LANE_TERMINALLY_CLOSED`, `ORDERING_DOMAIN_BROKEN`, `DESTINATION_LANE_LIMIT_EXCEEDED`, `ORDERING_CAPABILITY_UNAVAILABLE`, `SERVER_INVALID_COMMAND`, `SERVER_PAYLOAD_TOO_LARGE`, `SERVER_INVALID_METADATA`, `SERVER_UNSUPPORTED_DELIVERY_MODE`, `RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION`, `UNACTIVATED_PROTOCOL_VERSION`, `UNACTIVATED_SYSTEM_PROTOCOL_VERSION`, `UNAUTHORIZED_SYSTEM_MUTATION`, `SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED`, `STALE_SYSTEM_MUTATION`, `ADMISSION_CAPACITY_GATED`, `QUARANTINED_SOURCE_RECORD`, `PAYLOAD_OBJECT_CONFLICT`, `PAYLOAD_COMMIT_CONFLICT`, `PAYLOAD_PROOF_INVALID`, `PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION`, `RESERVATION_EXPIRED`, `RESERVATION_ABANDONED`, `PAYLOAD_RESERVATION_CLOSED`, `OBJECT_IDENTITY_CONFLICT`, `PAYLOAD_RESERVATION_ABANDONED`, `RESOURCE_INCARNATION_MISMATCH`, `SOURCE_INCARNATION_MISMATCH`, `REPLAY_WINDOW_EXPIRED`, `CREDENTIAL_EQUIVALENCE_NOT_PROVEN`.
- `RETRY_EXACT_BYTES_AFTER_EXTERNAL_CHANGE`: `BROKER_RESOURCE_UNCERTIFIED`, `CAPABILITY_UNAVAILABLE`, `FENCE_STALLED_CAPACITY`, `CREDENTIAL_BINDING_DRIFT`.
- `REREAD_AFTER_REPAIR`: `SHARD_UNAVAILABLE`, `INTEGRITY_ERROR`, `PULSAR_EVIDENCE_DIVERGENCE`, `SOURCE_GAP`, `UNSUPPORTED_ACTIVATED_PROTOCOL`.

The same stable code can appear as an immutable applied result, but its projection still describes the next legal client action, not permission to mutate that result. `NEW_PREPARATION_REQUIRED` always means a new logical operation/identity after correcting the stated prerequisite; it never retries the rejected identity under changed bytes.

Symbolic sub-outcomes not assigned a public stable code (`READY`, `BLOCKED`, etc.) use their own closed enum, never a free string. Adding a code uses a previously unused number and new spec revision; aliases are forbidden.

## 7. RocksDB CF and key codec

Physical CF set is exactly `default` plus seven application CFs. `default` has no application key tag and must remain empty.

| CF | tag assignments |
|---|---|
| `timeline_cf` | `DUE=01`, `ORDERED=02`, `READY=03`, `EXPIRY=04`, `RESERVATION_EXPIRY=05`, `SYSTEM=06` |
| `id_cf` | `MESSAGE=01`, `RESERVATION=02`, `PAYLOAD_REF=03` |
| `inflight_cf` | `CLAIMED=01`, `PUBLISHING=02`, `UNCERTAIN=03` |
| `dedupe_cf` | `COMMAND=01`, `RESULT=02`, `POSITION=03`, `FENCE=04`, `SYSTEM_MUTATION=05` |
| `terminal_cf` | `GENERATION=01`, `DLQ_EXPORT=02` |
| `gc_cf` | `TASK=01`, `PROTECTION=02` |
| `meta_cf` | `FIXED=01`, `LANE=02`, `QUOTA=03`, `PRODUCER=04`, `SCHEDULER=05`, `CONTROL_RESERVE=06`, `RECOVERY=07`, `SLO_OUTBOX=08` |

Closed subtype bytes used below:

- `meta/FIXED fixedKeyKind`: 1 store/schema format, 2 shard/Route/DB/Store identities, 3 applied Source Position, 4 ingress-fence state, 5 shard mutation sequence, 6 evidence cursor array, 7 checkpoint identity, 8 last-opened Owner, 9 clean/unclean marker, 10 compatible control snapshot, 11 next Claim sequence, 12 Payload Proof Trust-Set control state, 13 Profile binding control state.

`meta/FIXED` key 10 is a `CompatibleControlSnapshotV1` carried in the fixed-key
ValueEnvelope type 1. Its canonical fields are: 1 schema version=1; 2
`ShardSubjectV1 shard`; repeated 3 `ProtocolTupleV1` strictly canonical-byte
sorted/unique; repeated 4 `ProfileRefV1` strictly sorted by
`(profile_id,version)` and unique; 5 initial `QuotaGrantRefV1`; 6 snapshot
digest[32]. Field 6 is
`SHA-256("nereus-delay-compatible-control-snapshot-v1\\0" || canonicalProtobuf(fields 1-5))`.
The tuple list is non-empty and bounded; the profile list and canonical bytes
are bounded. Open/restore must decode the complete value, verify the digest,
and require field 2 to equal the DB's `meta/FIXED` shard identity. The local
projection records the exact control input used for activation; it does not
replace Oxia's authoritative Route/Profile/grant catalog or its session-bound
lease checks.
- `meta/QUOTA quotaClass`: 1 grant identity/version, 2 aggregate usage vector, 3 per-Lane usage, 4 retained/object usage, 5 grandfathered transfer state.
- `meta/SCHEDULER schedulerKeyKind`: 1 ready-discovery cursor, 2 active-ring descriptor, 3 capped deficits, 4 round generation, 5 last-served map.
- `meta/CONTROL_RESERVE reserveClass`: 1 grant identity/digest, 2 charged outcome, 3 non-outcome/fence, 4 recovery working, 5 emergency headroom, 6 Broker system-writer reservation.
- `meta/RECOVERY recoveryKeyKind`: 1 lineage/base, 2 last-observed Floor, 3 catalog generation, 4 install/open state.
- `gc resourceKind`: 1 payload object, 2 terminal history, 3 result, 4 Command/System dedupe, 5 retired identity, 6 checkpoint, 7 orphan upload, 8 DLQ export, 9 Lane runtime/terminal dependency, 10 evidence resource.
- `gc protectionKind`: 1 Recovery Floor, 2 query/audit retention, 3 active attempt/read, 4 replay/retry window, 5 export obligation, 6 Control Operation.
- `timeline/SYSTEM systemWorkKind`: 1 DLQ export, 2 Lane-close materialization, 3 evidence recovery, 4 capacity-releasing GC.

Zero and all other subtype values are invalid in V1. A later feature cannot allocate an unregistered subtype without a new registry revision and compatible rollout.

The V1 Registry currently names `meta/QUOTA` classes 4 (`retained/object usage`)
and 5 (`grandfathered transfer state`) but does not yet freeze their value
payload schemas or accounting transitions. A V1 implementation must therefore
reject a non-empty class-4 or class-5 value during shard activation; it must
not decode it as an empty projection or silently ignore it. A future Registry
revision must define the canonical value, digest, accounting owner and replay
rules before those classes can be persisted or restored.

`meta/CONTROL_RESERVE` values use the canonical `CapacityVectorV1` payload inside
the registered ValueEnvelope. Classes 3–5 and class 6 are keyed by their
component grant ID, but are disjoint projections: classes 3–5 must have zero
amounts in dimensions 51–53, while class 6 is keyed by the
`NON_OUTCOME_CONTROL` grant ID and may have nonzero amounts only in
`SYSTEM_WRITER_RESERVED_RECORDS`, `SYSTEM_WRITER_RESERVED_BYTES` and
`SYSTEM_WRITER_RESERVED_BYTES_PER_SECOND`. The class-3 plus class-6 checked
componentwise sum must fit the immutable `NON_OUTCOME_CONTROL` grant. Class 6 is the shard-local
durable projection of the Route Broker system-writer reservation; it is not
proof that an external Broker quota authority has granted or charged that
reservation.

`meta/RECOVERY` values use ValueEnvelope type `1` and are closed as follows:

- recoveryKeyKind `1` is the canonical `RecoveryCandidateRefV1` for the local
  lineage/base projection. A `LOCAL_STORE` candidate must carry the current
  Store Incarnation; a fresh store has no candidate and must not synthesize one
  from its directory or `ACTIVE` pointer.
- recoveryKeyKind `2` is the canonical `RecoveryFloorRefV1` last observed by
  the local Store. Its Source Position must belong to this Shard.
- recoveryKeyKind `3` is exactly eight big-endian bytes carrying a nonzero raw
  `uint64 catalog_generation`. When key `2` is present this value must equal
  the Floor's field 4; absence is encoded by an absent key, never by zero.
- recoveryKeyKind `4` is the canonical `RecoveryInstallStateV1`: fields 1
  `version=1`, 2 `RecoveryInstallPhaseV1 phase`, 3 Store Incarnation[16],
  optional 4 checkpoint ID[16], and 5 state digest[32]. The digest is
  `SHA-256("nereus-delay-recovery-install-state-v1\\0" || canonical fields
  1–4)`. `FRESH`, `STAGED`, `INSTALLED`, `OPEN` and `CLOSED_CLEAN` are the
  only phases. The value records physical install/open history; it is not an
  Owner Lease, Recovery Pin or catalog decision.

Keys 1–4 are updated in one WAL-synchronised WriteBatch whenever a Store
Incarnation is installed or its local recovery observation changes. A Store
may be considered for local reuse only when the lineage/base, observed Floor,
catalog generation, install state, DB identity and shard identity are all
present and internally consistent; current Floor ancestry and Oxia authority
remain external checks.

第二 byte 总是 key-format version `01`。布局：

```text
timeline/DUE:
  01 01 | laneId[32] | eligibleAt:u64be | sourceOrderToken | messageId[41] | generation:u32be
timeline/ORDERED:
  02 01 | laneId[32] | deliverAt:u64be | sourceOrderToken | messageId[41] | generation:u32be
timeline/READY:
  03 01 | nextEligibleAt:u64be | laneId[32] | laneVersion:u64be
timeline/EXPIRY:
  04 01 | expireAt:u64be | laneId[32] | messageId[41] | generation:u32be
timeline/RESERVATION_EXPIRY:
  05 01 | reservationExpireAt:u64be | reservationId[32]
timeline/SYSTEM:
  06 01 | systemWorkKind:u8 | nextEligibleAt:u64be | lp32(workId) | workVersion:u64be

id/MESSAGE:      01 01 | messageId[41]
id/RESERVATION:  02 01 | reservationId[32]
id/PAYLOAD_REF:  03 01 | payloadRefId[32]

inflight/CLAIMED|PUBLISHING|UNCERTAIN:
  tag | 01 | ownerEpoch:u64be | claimId-or-publishAttemptId[32]

dedupe/COMMAND:          01 01 | commandId[41]
dedupe/RESULT:           02 01 | commandId[41]
dedupe/POSITION:         03 01 | canonicalSourcePosition
dedupe/FENCE:            04 01 | proofId[32]
dedupe/SYSTEM_MUTATION:  05 01 | systemMutationId[32]

`dedupe/POSITION` uses value type 3 as a closed physical-record audit union:
the Client Command branch carries the exact `commandId[41]` payload, while the
System Mutation branch carries the exact `systemMutationId[32]` payload. It is
not a logical `CommandResult` or `SystemMutationResult`; the referenced dedupe
record remains the authority for the first result and its original Source
Position. An exact replay may use the matching locator together with the
source-ordered state to return that result without reapplying it or appending
another audit. Any other payload length, cross-shard identity, missing
System Mutation result, or source-position mismatch fails closed. A later
physical duplicate writes a new POSITION locator while retaining the first
Source Position in the logical result.

terminal/GENERATION:
  01 01 | messageId[41] | generation:u32be
terminal/DLQ_EXPORT:
  02 01 | dlqExportId[32]

gc/TASK:
  01 01 | notBefore:u64be | resourceKind:u8 | lp32(resourceId) | expectedVersion:u64be
gc/PROTECTION:
  02 01 | protectionKind:u8 | lp32(resourceId) | protectionGeneration:u64be

meta/FIXED:           01 01 | fixedKeyKind:u8
meta/LANE:            02 01 | laneId[32]
meta/QUOTA:           03 01 | quotaClass:u8
meta/PRODUCER:        04 01 | laneId[32] | physicalPartition:u32be | channelSlot:u32be
meta/SCHEDULER:       05 01 | schedulerKeyKind:u8
meta/CONTROL_RESERVE: 06 01 | reserveClass:u8 | lp32(grantId)
meta/RECOVERY:        07 01 | recoveryKeyKind:u8
meta/SLO_OUTBOX:      08 01 | sampleId[32]
```

For `timeline/SYSTEM systemWorkKind=2`, the ValueEnvelope payload is the
canonical `LaneCloseMaterializationCursorV1` (local value type 11): fields are
1 version=1; 2 `DestinationLaneId[32]`; 3 Lane Incarnation[16]; 4 nonzero
`closeVersion`; 5 canonical `SourcePositionV1 closeSourcePosition`; 6 phase
(`MESSAGES=1` or `RESERVATIONS=2`); optional 7 exclusive-resume `lastKey`; 8
transferred pending-message count; 9 transferred pending bytes; 10 transferred
reservation count; 11 transferred reservation bytes; 12 digest. The digest is
`SHA-256("nereus-delay-lane-close-cursor-v1\\0" || canonical fields 1–11)`.
The marker transfers counters once; cursor batches are quota-neutral and may
only materialize unadmitted records. A generation with any admitted
`PUBLISHING`/`UNCERTAIN` obligation is never converted by this cursor.

`sourceOrderToken` 是 closed self-delimiting fixed variant：Kafka `01 | offset:u64be`；Pulsar `02 | ledgerId:u64be | entryId:u64be | normalizedBatchIndex:u32be`。一个 Route adapter 固定一种 variant。

For `timeline/ORDERED`, `deliverAt` is the business-order component, not the wake timestamp. Its V1 value schema must retain canonical `actionAt`, current `retryEligibilityAt`, and head-blocking state; the Lane READY projection uses `headEligibilityAt=max(actionAt,retryEligibilityAt)`. Ordinary managed delivery requires `actionAt=deliverAt`. Certified Pulsar handoff requires the one fixed handoff lead of the pinned Profile version, so `actionAt=checkedSubtract(deliverAt,handoffLead)` is order-preserving inside that Lane; V1 rejects underflow and any per-message handoff lead. Changing this relationship requires a new ORDERED key/value version.

The following correctness-critical value payloads are closed in V1:

- `timeline_cf/DUE|ORDERED` stores one `TimelineWorkRefV1`; its embedded encoded key must byte-equal the RocksDB key. It is the current-work copy and must be byte-equal to `GenerationRuntimeIndexV1.timeline`.
- `id_cf/MESSAGE` stores immutable identity/control/binding/payload fields plus exactly one `GenerationRuntimeIndexV1` for the current Generation. The runtime digest covers only that nested type; the surrounding value has its own canonical schema/version and envelope CRC.
- `inflight_cf/CLAIMED` stores the exact `ClaimPreconditionV1`, original `TimelineWorkRefV1`, materialization state and reversible permit charge. Its Claim ID must equal runtime-index branch 11 while current, but a source-ordered terminal/revocation deletes it.
- `inflight_cf/PUBLISHING|UNCERTAIN` stores one versioned Publish Attempt ledger whose Admission/Prepared descriptor/hash, attempt number and owner/store/channel/certificate identity are immutable, while its initial Outcome, evidence-resolution set, retirement state and remaining charge advance only by the registered monotonic transitions. Its exact encoded key/ID/generation/state must match exactly one `AttemptObligationRefV1` in the applicable current runtime index or terminal summary until that obligation is definitively closed and charge-transferred. `PUBLISHING` means no initial Outcome has applied; `UNCERTAIN` means its initial or later authoritative result still permits destination side effect. A tag transition is a single-batch old-key delete/new-key put/ref replacement, never two live keys or an Owner-Epoch range scan.
- `terminal_cf/GENERATION` stores the immutable terminal decision plus the same canonically sorted `AttemptObligationRefV1` summary for any still-open attempts and `possible_destination_duplicate`; later attempt evidence may only replace a PUBLISHING ref with the exact UNCERTAIN-key ref, remove closed refs, release exact charges, and monotonically set duplicate risk, never change the terminal state/code/time. Before a newer Generation exists, this summary is byte-equal to the current terminal `GenerationRuntimeIndexV1` set; after Replay it is the sole Generation-level locator for those old obligations.
- `meta_cf/LANE` stores the closed `LaneRecordV1`; active-to-terminal retirement replaces the value at the same `[LANE=02][v1][laneId]` key. No unregistered terminal-guard key/tag exists.

The authoritative physical invariant is therefore not “one record per Generation.” It is one `id_cf/MESSAGE` runtime index for the Message Identity's current Generation, zero or one current TIMELINE/CLAIMED/PUBLISHING work, and zero through pinned-maximum admitted attempt ledgers per Generation. A current PUBLISHING record is both current work and an obligation; historical UNCERTAIN records are obligations only. Terminal history may coexist with retained PUBLISHING/UNCERTAIN ledgers and becomes their sole generation locator after Replay. Every duplicated reference/digest/counter changes in one WAL-enabled batch; an orphan, cross-generation reference, duplicate, missing, wrong-kind, or over-policy reference fences scheduling and requires deterministic repair.

`canonicalSourcePosition` for `dedupe/POSITION` is exact:

```text
Kafka:
  01 | routeUuid[16] | lp32(authenticatedClusterId) | nativeTopicUuid[16]
     | partition:u32be | offset:u64be
     | leaderEpochPresence:u8 | [leaderEpoch:u32be if present]
     | brokerLogAppendTime:u64be

Pulsar:
  02 | routeUuid[16] | lp32(brokerResourceIncarnation)
     | lp32(canonicalPhysicalTopicUtf8) | partition:u32be
     | ledgerId:u64be | entryId:u64be | normalizedBatchIndex:u32be
     | batchSize:u32be | entryKind:u8 | brokerEntryTimestamp:u64be
```

`leaderEpochPresence` is 0/1 only. `entryKind` is 1 non-batch, 2 batch; non-batch requires batch index 0 and batch size 1. The Broker timestamp is part of position audit identity/evidence but not its same-resource order comparator.

Value envelope：`4e56 | valueType:u8 | valueVersion:u8=01 | payloadLength:u32be | canonicalPayload | crc32c:u32be`，CRC 覆盖前面全部 bytes。`valueType` 是 V1 关闭的 payload-schema discriminator；它不等同于 key 的 namespace tag。完整 schema identity 是 `CF descriptor + key tag + valueType`，因此同一 numeric type 只有在对应的 CF/key context 中才有意义，不能把一个 context 的 payload 搬到另一个 context。V1 当前注册的 payload type/context 映射为：

| `valueType` | Registered payload contexts |
|---:|---|
| 1 | `meta/FIXED` kinds 1--9 and 11, `id/MESSAGE`, `timeline/DUE|ORDERED|EXPIRY`, `dedupe/COMMAND`, `terminal/GENERATION` |
| 2 | `id/RESERVATION`, `meta/LANE` |
| 3 | `timeline/READY`, `dedupe/POSITION` |
| 4 | `dedupe/SYSTEM_MUTATION` (`SystemMutationResult`) |
| 5 | `meta/SCHEDULER`, `timeline/RESERVATION_EXPIRY` |
| 6 | `gc/TASK` retire-intent payload |
| 7 | `meta/QUOTA`, `gc/TASK` delete-confirmed payload |
| 8 | `meta/CONTROL_RESERVE`, `inflight/PUBLISHING|UNCERTAIN`, `terminal/DLQ_EXPORT` |
| 9 | `inflight/CLAIMED`, `meta/FIXED` kind 12, `meta/SLO_OUTBOX` |
| 10 | `meta/FIXED` kind 13 |
| 11 | `timeline/SYSTEM` close-materialization cursor |

Unknown value types, unknown/duplicate/missing CF tags, unsupported key versions,
or a value envelope used outside its registered context fail activation or the
relevant read/write boundary. This explicit union mapping is why GC retire and
delete-confirmed records keep distinct value types even though they share the
`gc/TASK` key namespace.

## 8. Evidence cursor ordering

Canonical array sort key 精确为：

```text
evidenceKind:u8
|| destinationLaneId[32]
|| laneIncarnation[16]
|| lp32(evidenceResourceIncarnation)
|| physicalPartition:u32be
|| evidenceGeneration:u64be
```

`EvidenceKindV1`: 1 `KAFKA_RECEIPT_CONTIGUOUS`, 2 `PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS`。数组按上述 bytes 严格递增，duplicate full key 非法。Dominance 只在 full key 相同的 cursor 间比较；Kafka 比 `nextOffsetExclusive`，Pulsar 比 `(ledgerId,entryId,batchIndex)`。不同 generation 可同时存在，不能因前五项相同而覆盖旧 cursor。

## 9. Version activation

Route source-ordered writable tuple 是：

```text
(framingVersion, logEnvelopeVersion, recordKind, envelopeVersion, bodyVersion)
```

- malformed frame/identity：`QUARANTINED_SOURCE_RECORD`；
- well-framed tuple 未激活：position-level `UNACTIVATED_PROTOCOL_VERSION` 或 system-specific code；
- tuple 已由前置 marker 激活但 Worker 不支持：`FAILED(UNSUPPORTED_ACTIVATED_PROTOCOL)`；
- unknown key/value/store format：DB activation fail，不猜测；
- dedupe value 保存完整 tuple；same ID/hash 只有 tuple 也相等才是 no-op。

Writer-before-reader rollout 被禁止。新 field/enum/semantic 必须新 body version；改变 frame/hash/key 则分别新 framing/domain/key-format version。旧 number/name 永久 reserved。

## 10. Checkpoint manifest JSON

`CHECKPOINT_MANIFEST_JSON_V1` is a closed RFC 8785 JCS object. Unknown keys, duplicate keys, noncanonical JSON, BOM, comments, floats, exponential numbers, and JSON numbers for any `uint64`/epoch/length/offset are invalid. Text conventions are:

```text
uuidText    = lowercase RFC 9562 8-4-4-4-12 form
b64Bytes    = unpadded RFC 4648 Base64url; decode then re-encode must match
sha256Text  = 64 lowercase hexadecimal characters
u64Text     = "0" or [1-9][0-9]*, value <= 2^64-1
i64TimeText = "0" or [1-9][0-9]*, value <= 2^63-1
```

The top-level keys are all required and appear below in semantic order (JCS serialization itself sorts object keys):

| key | exact V1 value |
|---|---|
| `manifestVersion` | JSON integer `1` |
| `shardId` | object `{routeIncarnation:uuidText, partition:uint32 JSON integer}` |
| `checkpointId` | b64Bytes decoding to 16 nonzero bytes |
| `recoveryLineageId` | b64Bytes decoding to 16 nonzero bytes |
| `lineageGeneration` | u64Text |
| `parentCheckpoint` | `null` only for lineage genesis, otherwise `{checkpointId:b64Bytes[16], manifestSha256:sha256Text}` |
| `restoredFromCheckpointId` | `null` for a fresh lineage, otherwise b64Bytes[16] |
| `createdBy` | `{deploymentId:b64Bytes, workerRunId:b64Bytes, ownerEpoch:u64Text}` |
| `createdAt` | exact JSON projection of one `TrustedUtcIntervalEvidenceV1`, defined below |
| `dbIdentity` | b64Bytes of the exact bounded nonempty RocksDB DB identity bytes |
| `sourceStoreIncarnation` | uuidText |
| `storeFormatVersion` | JSON integer `1` |
| `shardMutationSequence` | u64Text |
| `appliedShardLogPosition` | exact tagged object below |
| `controlStateDigest` | sha256Text |
| `referencedSemanticVersionsDigest` | sha256Text |
| `evidenceCursors` | strictly registry-sort-ordered array of exact cursor objects below; empty array allowed |
| `files` | nonempty strictly normalized-name-byte-sorted array of exact file objects below |

Both ID byte strings are nonzero cryptographic-random 16-byte values fixed before any checkpoint/lineage I/O. All retries of one checkpoint reuse the exact `checkpointId`; only lineage genesis allocates a `recoveryLineageId`, and every normal descendant inherits it.

`createdAt` has exactly these keys: `earliestEpochMs:i64TimeText`, `latestEpochMs:i64TimeText`, `source` equal to one `TimeEvidenceSourceV1` symbol, `sourceId:b64Bytes`, `sourceConfigGeneration:u64Text`, `sampleSequence:u64Text`, `monotonicAnchorNs:u64Text`, `sourceEvidenceSha256:sha256Text`, `sourceKeyVersion:uint32 JSON integer`, and `sourceSignature:null|b64Bytes[64]`. It must round-trip byte-for-byte to the checkpoint creator's canonical `TrustedUtcIntervalEvidenceV1` and satisfy that type's interval/source/signature rules. The creator samples it exactly once immediately after the physical RocksDB checkpoint is successfully created and before upload; retry reuses it. `CHECKPOINT_AGE` uses this field as its authoritative Start, so omission, resampling, or an unverifiable projection rejects the manifest.

`appliedShardLogPosition` is one of these closed objects; branch-inapplicable keys are forbidden:

```json
{"kind":"KAFKA","routeIncarnation":"uuidText","clusterId":"b64Bytes","topicUuid":"uuidText","partition":0,"offset":"u64Text","leaderEpoch":null,"brokerLogAppendTime":"i64TimeText"}
{"kind":"PULSAR","routeIncarnation":"uuidText","resourceIncarnation":"b64Bytes","physicalTopic":"canonical UTF-8 NFC","partition":0,"ledgerId":"u64Text","entryId":"u64Text","batchIndex":0,"batchSize":1,"entryKind":"NON_BATCH","brokerEntryTimestamp":"i64TimeText"}
```

Kafka `leaderEpoch` is `null` or a uint32 JSON integer. Pulsar `entryKind` is exactly `NON_BATCH` or `BATCH`; non-batch requires index 0/size 1. These fields must encode the same `SourcePositionV1` as the database value and catalog entry.

An evidence-cursor object has required common keys `evidenceKind`, `destinationLaneId` (b64Bytes[32]), `laneIncarnation` (b64Bytes[16]), `evidenceResourceIncarnation` (b64Bytes), `physicalPartition` (uint32 integer), `evidenceGeneration` (u64Text), and `maxBrokerPersistedAtThroughCursor` (i64TimeText). It then has exactly one branch:

- `KAFKA_RECEIPT_CONTIGUOUS`: `topicUuid` uuidText, `nextOffsetExclusive` u64Text, `lastObservedLsoExclusive` u64Text.
- `PULSAR_ATTEMPT_JOURNAL_CONTIGUOUS`: `resourceToken` b64Bytes[32], `physicalTopic` canonical UTF-8 NFC, `physicalTopicCreationTimestamp` u64Text, `ledgerId` u64Text, `entryId` u64Text, `batchIndex` uint32 integer, `batchSize` uint32 integer.

Cursor arrays use §8's full binary sort key including evidence generation; JSON lexical order is not a substitute.

Each file object has exactly:

```text
name          canonical relative UTF-8 NFC path
length        u64Text
checksum      sha256Text of the complete file bytes
objectKey     b64Bytes of the exact provider object-key bytes
objectVersion b64Bytes of the exact immutable provider version bytes
etag          null or b64Bytes of the exact provider-returned etag bytes
```

`name` must be nonempty, use `/`, contain no empty/`.`/`..` segment, no leading `/`, backslash, NUL or Unicode normalization ambiguity, and satisfy the activated path/file/count/byte limits. Duplicate normalized names, object identities, or checksums with conflicting lengths are invalid. Object version is mandatory; etag never replaces SHA-256.

The final manifest cannot be finalized before file upload because object versions are manifest fields. The publication order is exact: build and checksum local file inventory; create `PENDING_UPLOAD`; upload every file to its unique key and capture immutable version/etag; verify each uploaded object; build the final JCS manifest; upload/verify that manifest; then catalog-CAS its exact key/version/length/SHA-256. A draft inventory is not a manifest and is never catalog-visible.

## 11. Published conformance vectors

### 11.1 Frame CRC

一个仅用于 frame-codec 的零 payload prefix（不是合法 envelope）：

```text
header hex: 4e444c310101000000000000
CRC32C:    519553ae
full hex:  4e444c310101000000000000519553ae
```

Receipt-frame codec zero-payload prefix (not a legal receipt payload):

```text
header hex: 4e4452310101000000000000
CRC32C:    2ad79a80
full hex:  4e44523101010000000000002ad79a80
```

### 11.2 Hash vectors

Vector `CLIENT-ZERO-1`：framing/log/envelope/body version 均 1；type `SCHEDULE=1`；CommandId 为 41 个 `00`；DelayMessageId 为 41 个 `11`；retryUntil=0；body empty（只测试 preimage helper，不是合法 Schedule body）。

```text
SHA-256 = c039cf07a4bdb4089aeef7ee97cddf2c4d8bdd8da2589da5cc53fda4b92f5b44
```

Vector `MUTATION-ZERO-1`：version 均 1；type `TIME_FENCE_V1=4`；Route UUID/partition/retryUntil 为 0；body empty（同样仅测试 preimage helper）。

```text
SHA-256 = 301b41cc39340312965717a62f105849b57957f88ef1c34176c2c1b9a527af00
```

### 11.3 Key vectors

```text
READY(next=0,laneId=32*00,laneVersion=0):
0301000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000

MESSAGE(messageId=41*00):
01010000000000000000000000000000000000000000000000000000000000000000000000000000000000
```

### 11.4 Routing, self-ID and Lane vectors

`ROUTING-ZERO-1` uses Route UUID=`16*00`, tenant scope=`32*00`, empty routing key:

```text
digest:      1430410397c4d723024e40b2442b927312a7aa6929f61d8629ea9a2e581e3ef7
partitionCount=16 -> partition 3
```

`SELF-ID-ZERO-1` uses format 1 and zero Route/partition/logical UUID:

```text
010000000000000000000000000000000000000000000000000000000000000000000000009f0a9589
```

`LANE-KAFKA-UNORDERED-1` uses scope=`32*00`, Kafka, cluster `cluster-a`, zero topic UUID, physical identity=the same 16 zero bytes, partition 3, Profile `profile-a` v1/hash zero, capability `baseline` v1/hash zero, unordered bucket 7:

```text
destinationLaneId:
9ab8f6eaf14154adb301ef83741ff236ae14a948c17ce4aefbdd72200a16a5fd
```

### 11.5 Derived-ID vectors

All unspecified fixed-width inputs are zero. `attemptNo=1`; DLQ `terminalRevision=1`.

```text
reservationId(commandId=41*00,messageId=41*00,commandHash=32*00):
81174b2bb8f2d9d81b82732f43d289d846502c6b75e2a04d817f8b015212afc9

claimId(store=16*00,ownerEpoch=0,claimSequence=0,message=41*00,generation=0,laneVersion=0):
954f48488178052b31203f69f466778f0fb8f5122dd6078c9b345026403cb01d

publishAttemptId(claimId=32*00,message=41*00,generation=0,attemptNo=1):
888d8b8e428f28ac7a090c9bdf872983d8b52e545022961a5fee2fafe3497509

dlqExportId(message=41*00,generation=0,terminalRevision=1):
eef6a63d92d6096d491258a4b1d1a0ceb868615572f390cc6e53448dfbd38408

laneIncarnation(laneId=32*00, canonical Kafka Source Position with zero identities,
empty cluster ID, partition/offset/time zero, no leader epoch):
af00186beffb82f4a816a7c9d45c5849

TARGET_PARTITION_HASH_V1(profileId empty,version=1,routingBytes empty):
digest=7ccafc4a57a353493a08438e1fbbe7623a710a979b66c66ffe4530058695cd42
partitionCount=16 -> partition 9

RETRY_JITTER_V1(domain=MESSAGE_PUBLISH=1,message=41*00,generation=0,attemptNo=1):
digest=dd78e75f6ce6dbe52cf55a19e91931f71423c4b32d6f1804d57f71b629b3d339
unsigned first64=15958759676622330853
```

Release artifacts must add cross-language positive/negative vectors for every body, enum boundary, ID CRC, routing/Lane hash, Source Position variant, key min/max, generation/version overflow, value CRC, manifest and signature.这些生成文件可以机器生成，但输入 schema/domain/expected digest 必须引用本 revision；缺失任一 registered variant 时 release gate 失败。
