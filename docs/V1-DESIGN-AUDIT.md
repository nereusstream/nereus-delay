# Nereus Delay V1 Design Audit

状态：PASS / design semantics closed  
Spec revision：`V1-FROZEN-2026-08-01`  
审计日期：2026-08-09
性质：验收证据索引；不覆盖主设计、Protocol Registry 或 Accepted ADR

## 结论

V1 的业务语义、线性化点、fencing 范围、物理持久边界、故障隔离、恢复/GC 保护关系、公开错误模型和发布停止条件已经闭合。审计未留下需要实现自行选择的语义分支。

**Open semantic questions: none.**

The local codec audit additionally verified that implemented V1 key-version
fields honor the Registry's full unsigned `uint32` domain rather than silently
narrowing to positive Java `int`: signed System Mutation, Native Capability,
credential attestation, Evidence Verifier, Payload Commit and payload-proof
trust-set/control paths preserve raw high-bit values, compare trust-set keys
unsigned, and use the same bits in signature/proof preimages. Focused protocol
tests cover the high-bit round trips. This is byte-level evidence only; the
activated trust set, key rotation and authenticated writer authority remain
release gates.

The local SLO codec audit also preserves the Registry's complete unsigned
`uint64` domain for objective threshold/ratio/window/envelope fields and Final
measurement/revision fields. Canonical decode retains raw high-bit values;
interval validation, evidence revision selection and `AT_MOST`/`AT_LEAST`
conservative merges compare them unsigned. Focused SLO tests cover round-trip
and merge vectors; the due-admission identity also preserves its raw `uint32`
generation bits. The due-admission identity now rejects a negative
`path_start_epoch_ms`, exposes typed path/start fields, and `SloSampleStartV1`
requires exact identity/path and semantic fixed-epoch agreement; the focused
SLO tests cover path/time drift and the negative-time vector. This remains
shard-local protocol evidence, not proof of production catalog publication, but
the local pair validator now requires a HEALTHY due objective and matching
ALL_ACCEPTED policy before an exclusion can be merged; a different payload
cannot regress observation revision. Start reconstruction, production collector
merge/export, and production SLO evidence remain release gates.

The ownership/recovery audit now covers the local reversible-Claim activation
boundary: before either embedded or authoritative `OwnedDelayShard` activation
opens the command gate, `DelayShard.requeueClaimsForRecovery()` scans the full
bounded `inflight_cf/CLAIMED` namespace and restores each Claim through one
atomic timeline/Message/READY/quota WriteBatch. The restored timeline keeps
the semantic work digest while using a new checked runtime instance; malformed
or over-bound Claim state fails closed, and `PUBLISHING`/`UNCERTAIN` ledgers are
not rewritten by this helper. This closes only the local Claim recovery seam;
Source Log successor proof, Oxia lease/session CAS, adapter materialization and
external Producer authority remain release evidence.

The shared `uint32` decode boundary also rejects a varint outside the unsigned
32-bit domain instead of narrowing a high-bit `uint64` into a `uint32`; the
queued receipt path rejects high-bit signed timing values while retaining raw
Pulsar physical-topic creation identity. The closed Control and Scheduler
projection validators apply the same fence to local `uint32` and boolean
fields. `ProtocolCodecTest` covers these malformed-input fences.

The canonical Protobuf length-delimited reader rejects high-bit `uint64`
length prefixes and lengths above the bounded Java `int` payload limit before
the value is narrowed for slicing. This closes the local malformed-input path
where a `2^63`-class length could otherwise become zero after a cast;
`ProtocolCodecTest.canonicalReaderRejectsHighBitLengthPrefixes` is the focused
regression evidence. It is parser-safety evidence only and does not claim an
external transport or object-store size limit.

The five `meta/SCHEDULER` projection codecs likewise retain complete raw
`uint64` generation/version/deficit bits and keep only the bounded `next_index`
`uint32` in local range. The focused projection vector covers discovery cursor,
ring, deficit map, round and last-served map; runtime capacity/placement and
Oxia scheduler authority remain separate release evidence.

The Registry-shaped `ActiveLaneStateV1` projection also preserves raw unsigned
`uint64` lane-control/lane-version/scheduler-weight/failure fields and keeps
the separate epoch fields as nonnegative `int64`. `DelayShard` now recognizes
that direct typed ACTIVE branch during reopen, rebuild and same-key projection
updates, maps only the bounded compatibility fields to its local `LaneRecord`,
and preserves immutable Profile/tuple/certificate/retirement data plus the
projected per-Lane quota usage. A malformed typed value, missing BLOCKED
reason/READY key/certificate or out-of-range compatibility field fails closed;
there is no silent downgrade to the legacy adapter. This is still local typed
runtime evidence; complete Profile/Lane activation, quota authority and Oxia
ownership remain release evidence.

The nested `ChargeVectorV1` now preserves the complete raw `uint64` domain at
the wire boundary. Its embedded signed-capacity projection is guarded
explicitly, so a high-bit charge cannot enter local reserve/vector arithmetic
as a negative value; it is capacity-gated instead. This is local codec and
fail-closed admission evidence, not an external grant-authority proof.

Profile and Retry Policy semantic versions and their reference values likewise
retain complete nonzero `uint64` bits in canonical bytes and semantic-hash
preimages. This closes their local immutable-value codec boundary; catalog
publication, activation history and source authority remain release evidence.
`PublishOutcomeBody` and `DlqExportResultBody` now share the strict
`RetryPolicyRefV1` decoder, so the high-bit version boundary is preserved on
both outcome paths; `PublishOutcomeBodyTest` covers the cross-path regression.
The Admission target-partition hash now uses the same raw `uint64` Profile
version bits (`PublishAdmissionBody.requireHashedPartition`), so a high-bit
Destination Profile reference is not rejected while recomputing the Registry
`TARGET_PARTITION_HASH_V1` digest. `PublishAdmissionBodyTest` covers the
high-bit hash vector; target catalog/publication and external partition
authority remain release evidence. Admission descriptor/materialization Profile
references now also reuse `ProfileRefV1.decode`, so malformed semantic-hash
lengths cannot pass the projection validator; the negative vector is covered by
`PublishAdmissionBodyTest`.
The replay-stable Claim Result materialization uses the same decoder, so its
high-bit Destination/Delivery-Capability Profile versions are not rejected by
the Claim path; `ClaimResultBodyTest` covers that boundary.
Its nested Broker resource, committed Object Store descriptor and adapter
metadata now reuse the typed Registry codecs as well, so NFC/UTF-8, branch,
Profile-kind and fixed identity semantics cannot be weakened by the Claim
projection's local parser. Negative vectors for a non-canonical Broker identity
and a committed descriptor with the wrong Profile kind are in
`ClaimResultBodyTest`.

The runtime Claim seam now has a strict `DelayShard.claimForPublishV1` entrypoint.
Before Claim persistence it compares the typed materialization with the current
Message's identity, generation, delivery window, timeline `actionAt` and
inline/object payload reference; `ClaimMaterializationRuntimeTest` covers both
payload branches and mismatch rejection. This is a local Message-binding
proof only: Profile/catalog, Adapter serialization/size certification and
Producer ownership/recovery remain release gates.

`PublishAdmissionBody` now parses its descriptor through the complete canonical
`PreparedPublishDescriptorV1`; `Descriptor.value()` returns that same typed
projection, including reserved metadata and prepared-publish hash. The Admission
tests cover byte round-trip, fail-closed reserved-shard relabeling and reserved
Profile-hash drift. This removes the local raw-descriptor parser/accessor gap
without claiming external channel or Producer authority.

Both Claim and Publish Admission materialization parsers also enforce the
Registry slot kinds: field 1 is `DESTINATION` and field 2 is
`DELIVERY_CAPABILITY`. This prevents a validly encoded but semantically wrong
Profile reference from crossing the replay-stable Claim/Admission boundary;
the negative vectors are in `ClaimResultBodyTest` and `PublishAdmissionBodyTest`.

The replay-stable nested values now have one shared local implementation:
`PayloadForPublishV1` owns the inline/object union and length/SHA-256 equality,
and `ClaimMaterializationV1` owns the complete 11-field canonical projection,
uint32 partition/generation range, target-resource/metadata branch agreement,
nonnegative timing constraints and domain-separated digest. `ClaimResultBody`,
`PublishAdmissionBody` and the Prepared Publish Descriptor all decode through
these types, while `ClaimRecord` exposes the typed projection only after its
durable Claim precondition has passed. This is stronger local codec evidence,
not proof of Profile/catalog, Object Store, Adapter or Producer authority.

The `ExactResourceIdentityV1` retirement projection now applies the same
branch-specific Object Store Profile fence as the committed/checkpoint
resource codecs. Payload-object and checkpoint lengths follow the Registry's
`uint64` non-negative domain, so a valid zero-byte object or manifest is not
rejected before the GC intent can be persisted. `ResourceRetireIntentBodyTest`
covers both zero-length branches and rejects a payload identity carrying a
Destination Profile.
The seven Registry retirement branches are now exposed as typed protocol values:
`PayloadObjectResourceV1`, `CheckpointResourceV1`, `DlqExportResourceV1`,
`KafkaReceiptSlotResourceV1`, `PulsarJournalGenerationResourceV1`,
`LaneChannelResourceV1` and `LocalStoreResourceV1`. Each value owns its direct
canonical codec and exact `ResourceKind` wrapper; `ExternalResourceBranchTest`
covers the newly added payload/DLQ/lane/local-store values as well as raw
unsigned Kafka/Pulsar fields. `KafkaReceiptResource` and
`PulsarJournalResource` project their physical identities into the same typed
Registry branches. This closes only the local resource-value boundary, not slot
allocation, external retirement, Broker evidence or Oxia ownership.

Payload-proof trust-set semantic/reference versions now also preserve complete
raw bits, while the source-ordered activation projection compares versions as
unsigned values and rejects regression. This is local control-state evidence;
trust-set publication and authenticated source authority remain external.
The legacy and typed payload commit proof projections now preserve the same
nonzero raw trust-set version bits through proof IDs, signatures and canonical
decode, closing the local semantic-ref-to-attestation mismatch. Provider proof
issuance and source-ordered trust authority remain release evidence.
The compatibility `LargeScheduleIntent` reserve projection preserves the same
raw version through its fixed binary encode/decode path as well.

`ApplyShardControlBody.semantic_version` now follows the same immutable
semantic-version boundary as a raw nonzero `uint64` through the System
Mutation body validator and typed parser. The optional
`expected_prior_control_version` and Lane target control versions remain
bounded local CAS values.

CapacityGrant source versions, QuotaGrant reference versions and
ShardCapacityEnvelope versions likewise preserve their complete nonzero
`uint64` bit patterns through canonical bytes, nested decode and quota
semantic-hash preimages. The separate `CapacityVectorV1` amount projection
remains a checked signed local capacity envelope; this audit closes only the
independent version-identity codec boundary.

The per-Lane quota usage entry now retains the complete nonzero `uint64`
usage-revision bit pattern in its digest and canonical bytes as well. This
closes the map-entry revision codec boundary only; same-batch quota coupling
and external quota authority remain separate release evidence.

Checkpoint summary, catalog and control-result projections now retain complete
nonzero `uint64 catalog_generation` bits and compare repeated summaries with
unsigned generation ordering. This is public checkpoint projection evidence;
Recovery Floor/upload authority and local generation-increment rules remain
separate release evidence.

The checkpoint upload-intent codec likewise retains complete nonzero
`base_catalog_generation` and checked `state_revision` bits, and the local
intent successor advances the latter across the signed Java high-bit boundary
until the all-ones pattern. Provider/Oxia publication and CAS authority remain
external release evidence.

Typed Recovery Floor and session-bound Pin references now also retain the
complete nonzero `catalog_generation` bits through their digests and exact
cross-object binding. The local catalog successor and the Oxia response adapter
now compare catalog generations as unsigned values and reject only zero or the
all-ones successor exhaustion; Recovery catalog freshness, ancestry and session
CAS remain external authority evidence. This closes the local arithmetic and
response boundary as well as the canonical reference codec boundary.

`QuotaTransferPlanRefV1` likewise retains the complete nonzero tenant-policy
version `uint64` bit pattern in the canonical control request value. This is
wire-value evidence only; policy authorization and source-ordered transfer
authority remain external.

Lane retirement progress and terminal-guard projections likewise retain the
complete nonzero raw `uint64` mutation-sequence pattern in their digests and
canonical bytes. These values are identity/fencing projections only; the
runtime source-mutation and lane-control counters remain bounded local
successors, and terminal-guard/Oxia retirement authority remains release
evidence.

尚未填写的数值不是开放设计问题：它们必须由 §23 的 benchmark、capacity proof、real-service conformance 和 chaos evidence 产生，并装入已经冻结的 required config/schema。任何实现若要改变字段、状态、时序、不变量或停止条件，必须发布新的 spec/protocol revision，不能把 benchmark 输出当作协议修订。

## 权威材料

| 材料 | 责任边界 |
|---|---|
| [`Nereus Delay V1 设计.md`](<Nereus Delay V1 设计.md>) | V1 行为、状态、时序、配置和验收语义 |
| [`V1-PROTOCOL-REGISTRY.md`](V1-PROTOCOL-REGISTRY.md) | 唯一 numeric/byte registry：frame、field、enum、hash、code、key、manifest 和 golden vector |
| [`CONTEXT.md`](../CONTEXT.md) | 统一术语和禁止混用的近义概念 |
| [`adr/README.md`](adr/README.md) | 42 个 Accepted 决策及其治理索引 |
| 本文件 | 交叉审计结果和 release-evidence checklist；非新 authority |

若前三类规范材料互相冲突，release gate 失败；实现不得自行选一个解释。

## 契约覆盖

| V1 契约 | 已冻结的证据入口 |
|---|---|
| `deliverAt` 是消费者最早可见时间 | 主设计 §2/§3/§12.4/§13.7；ADR 0001、0021 |
| 默认 managed；`AUTO_FAST` 显式 opt-in 且先 prepare 后 I/O | 主设计 §3/§6；ADR 0002、0031 |
| queued、applied 和 destination outcome 是不同事实 | 主设计 §6/§8；ADR 0005、0006 |
| Command identity 在 I/O 前固定；Source Position 是唯一 shard order | 主设计 §7/§8；ADR 0006、0007、0026、0034 |
| 一个 Delay Shard 对应一个 DB、checkpoint、restore 和 migration 单元 | 主设计 §5/§9/§10/§16；ADR 0004、0025、0027 |
| `ownerEpoch` 只 fence Nereus 本地 authority | 主设计 §9/§11；ADR 0003、0017 |
| 坏 Destination Lane 不暂停 Command apply、也不无限饿死健康 Lane | 主设计 §4/§12/§18/§20/§23；ADR 0008、0032、0035、0036 |
| Destination/Profile/partition/order domain 都被 immutable binding pin 住 | 主设计 §5/§12/§13；ADR 0009、0014、0016、0038 |
| Publish Admission 是 Producer call 前的 durable point of no return | 主设计 §11；ADR 0013、0039 |
| remote accepted/unknown 不冒充 failure；多 attempt obligation 可闭合定位 | 主设计 §11/§15；Registry 的 `GenerationRuntimeIndexV1`、`AttemptObligationRefV1`；ADR 0022 |
| 大 payload 使用 reserve/upload/attest/commit | 主设计 §14；ADR 0010、0042 |
| Recovery Set/Floor、lineage、pin 和 upload intent 共同约束恢复与 GC | 主设计 §15.5/§16；ADR 0011、0027、0040 |
| quota、Control Reserve、Worker physical envelope 和 placement 不超卖 | 主设计 §18/§21；ADR 0019、0028 |
| Query、Control Operation、RBAC 和 public-safe projection 是 closed union | 主设计 §17/§19；Registry §6.3；ADR 0018、0020、0029 |
| 每个 SLO sample 可持久恢复且 conservative merge | 主设计 §20；ADR 0041 |

## Authority 与线性化点

Registry credential-control-plane canonical values are locally closed by
`CredentialEquivalenceAttestationV1`, `CredentialBindingV1`,
`CredentialBindingHeadV1` and `CredentialBindingProtectionV1`. Their digests,
candidate tuple agreement, Trusted-UTC interval ordering and Ed25519 signature
projection are verified before a value is accepted. This is not an Oxia
linearization receipt: activated trust-set membership, provider-side immutable
version resolution, configured proof-age bounds, Head/protection monotonic
CAS and durable reread remain external authority gates. `CredentialUseLeaseV1`
locally requires the matching Profile kind, binding/fingerprint tuple and
kind-specific protection-until bound; it does not reread Oxia for each provider
call.

The Profile publication/deprecation/equivalent-rotation request values are
also canonical and fail closed on envelope/binding identity, generation
successor, private-reference digest, attestation candidate and expected Head
revision. They remain request-value codecs, not authenticated actor/target
authorization or source-ordered Oxia mutation receipts.

`ProfileCatalog`/`InMemoryProfileCatalog` now provide a strict local lookup
projection for exact Profile semantic bytes, generation-1 and rotated private
bindings, Head/protection records and deprecation intent. The projection is
useful for recovery and tests, but it is not the Profile publication authority,
does not create per-shard activation markers, and does not replace authenticated
Oxia CAS or retained-generation policy.
`ProfileCatalogV1ScheduleResolver` uses that seam only as a local exact-profile
and Head prerequisite gate, and now exact-resolves the referenced Delivery
Capability semantic with the same Adapter before delegating Lane resolution;
missing/wrong-kind/mismatched capability is fail-closed. It does not turn a
catalog lookup into a source position activation receipt.
The Control Operation request union now has local canonical codecs for all
fifteen Registry branches, including the evidence/acknowledgement matrix. It
remains a request-value boundary and does not authenticate an actor/resource,
construct source mutations or produce an Oxia registration receipt.
The prepared envelope also retains the complete raw `control_query_policy_version`
bits in its digest and signature preimage; this immutable policy reference is
distinct from the bounded local operation-revision CAS counter.
Credential binding Head/protection revisions now follow the same complete raw
`uint64` rule across Head, protection, lease, rotation request/target and public
result projections. Zero remains invalid, and the local catalog stops its
unsigned checked successor at the all-ones pattern; this is codec/CAS-value
evidence only, not an Oxia transaction or provider-authority proof.
The Control target value layer closes the six branch shapes and field-22
digest locally. `PreparedControlOperationV1` now additionally enforces the
closed operation-specific target counts/kinds, mutation-identity presence and
Profile/Quota request-to-target identity rules before hashing/signing; source
mutation construction and authenticated target authority remain outside this
local codec boundary. `ControlTargetMutationBindingV1` then validates a
completed source mutation's ControlRef, logical identity, computed ID/hash,
target Shard/Message and the covered Replay/Resolve/Lane body fields before
external registration; it does not replace Oxia or writer authentication.
`ControlOperationAuthorizationV1` additionally checks the authenticator's
actor/role/scope hash projection and the minimum V1 role matrix before that
step; its explicit scope proof remains an external authorization input.
`ControlRegistrationBindingV1` binds all three local registration outcome
branches back to the exact Prepared operation and rejects receipt/request,
scope, target-snapshot or revision drift; transport classification and Oxia
persistence proof remain external.
The local `InMemoryControlTargetRegistrationAuthority` is only an idempotent
exact-byte target-registration model; it does not make the Oxia CAS/transaction
or production target lookup available.
Its bytes are not an Oxia registration receipt and do not establish actor/role
authority.
The local operation authority now also applies the closed monotonic operation
and target-marker transition graph before its in-memory revision CAS; this is
projection validation only and does not provide durable Oxia operation state.
`OxiaControlTargetRegistrationAuthority` applies the same exact Prepared-byte
and operation-ID checks around its injected backend; it is a validation
adapter, not a real Oxia client or transport classifier. Lookup buffers are
copied into separate backend and validation snapshots, so a backend cannot
rewrite the operation identity used by the response check.
`ControlSystemMutationFactoryV1` now centralizes the signed envelope and
logical-identity derivation, while operation-specific body encoding and
service-key trust remain outside this local seam.
The initial operation projection now covers every target and preserves the
Registry uint32 target index; this is a local codec/projection guarantee, not
an Oxia registration or target-existence proof.
`ControlRegistrationProjectionV1` keeps the receipt and that initial CURRENT
projection together, but still does not claim the Oxia one-transaction
registration boundary.
The query-retention helper derives the boundary from trusted registration
evidence with checked addition; policy publication and Oxia persistence remain
external.
The embedded service now runs the local target-registration plus operation-CAS
path end to end. Its configured `DelayShard` also consumes the same local
registration authority: `APPLY_SHARD_CONTROL`, `REPLAY_DEAD_LETTER` and
`RESOLVE_UNCERTAIN` markers are rejected at their Source Position before any
handler effect unless the exact Prepared target and mutation identity are
registered; the matching exact-registration path is also covered by the
`DelayShardTest` positive/negative vectors. This proves the fail-closed local
boundary only; it remains a test model and does not provide gateway
authentication, target existence or a production Oxia transaction.
The registration outcome/proof union is likewise local evidence projection:
only authenticated Oxia response evidence can construct the conditional
rejection branch, while timeout/session ambiguity remains `RECORD_UNCERTAIN`.

| 事件 | 唯一 authority / 线性化点 | 明确不构成 authority 的事件 |
|---|---|---|
| Command 准备 | canonical Prepared Command bytes/hash 在首次 I/O 前完成 | Producer request、Broker position、wall clock |
| Command queued | ingress Broker durable receipt | SDK Future 创建、local buffer acceptance |
| Command applied/rejected | state/result/dedupe/Source Position 的 WAL-enabled RocksDB WriteBatch sync | queued receipt、source ACK、cache observation |
| source ACK/commit | 只能发生在对应 DB sync 后 | ahead-of-DB consumer offset |
| source-ordered control 生效 | signed System Mutation 在目标 shard 的 RocksDB apply | Oxia request、watch delivery、Control API return |
| Claim | local reversible Claim WriteBatch | destination ownership、delivery attempt |
| Publish Admission | exact Admission/ledger/ref/charge 的 RocksDB WriteBatch sync | Claim、Producer call、callback |
| destination outcome | Outcome/Evidence System Mutation 的 source-ordered apply | raw callback、timeout、Future cancellation |
| expiration | qualified `EXPIRE_GENERATION_V1` apply | raw timer、unqualified Worker clock |
| checkpoint 可恢复 | immutable manifest/files 已校验并成功进入 Oxia catalog | local checkpoint、partial upload、object listing |
| shard command-active | restore/replay 到 typed Activation Barrier 后的 guarded `ACTIVE_FOR_COMMANDS` CAS | DB open、Source Assignment、Owner Lease 单独存在 |
| Lane READY | exact Ready Certificate、channel/evidence/credential prerequisites 和 READY key 同步成立 | target 健康猜测、admin OPEN 单独存在 |
| GC/delete 完成 | source-ordered retire intent、保护集、external delete confirmation 和 Recovery Floor 全满足 | delete request、timeout、listing absence 单独存在 |

`EmbeddedDelayService` 的本地 ingress seam 现在在分配 Source Position 前同时
限制 pending command count 和 canonical frame bytes；缓冲区满时返回 Registry
的 `SDK_BACKPRESSURE_NOT_SUBMITTED` definitive local rejection，且不消耗嵌入式
source offset，drain 后释放精确 byte charge；`close()` 会先同步 drain，且
队头只有在 `DelayShard.apply` 返回后才释放。证据是
`EmbeddedDelayServiceTest.sdkBackpressureRejectsBeforeSourcePositionAndByteBudgetAreConsumed`。
以及 `EmbeddedDelayServiceTest.closeDrainsQueuedCommandsBeforeClosingTheShardDb`。
`DelayClient.enqueueBatch` / `EmbeddedDelayService.enqueueBatch` 现在逐条复用同一
本地 ingress admission，返回与输入顺序一致的独立 `EnqueueOutcome`；它明确不承诺
跨命令原子性，混合 queued 与 definitive local rejection 的结果仍可逐条处理。
`DelayClient.getCommandResult` 与 `getMessage` 也已把这两个 bounded local query
projection 暴露在客户端契约中；它们仍要求 queued receipt/source barrier 与调用方
提供的 binding/evidence/retention policy，不能被解释为跨 Worker routing 或 production
authorization-safe lookup。
严格 V1 的 Schedule/PrepareLarge/Cancel/Reschedule prepare 入口也已进入同一
`DelayClient` 契约；它们只构造 Registry-shaped canonical bytes，不做网络 I/O，
`EmbeddedDelayServiceTest.delayClientPreparesStrictV1CommandsWithoutIo` 覆盖其
body round-trip。legacy prepare 仍是兼容桥，不能伪装成 V1 managed submission。
对应的 `enqueueV1`/`enqueueBatchV1` 在 pending admission 前调用 strict V1 frame
validator；legacy body 得到 `INVALID_PREPARED_COMMAND` definitive branch，不消耗
Source Position 或 byte budget，且 batch 仍逐条保持输入顺序。
`DelayClient.awaitAppliedV1` 在同一边界上补齐等待语义：先验证 pinned embedded
receipt，再对 PENDING 结果 drain 并 reread；不合法 receipt 直接返回
`RECEIPT_MISMATCH`，不会推进 source。
`DelayClient.prepareManagedSubmissionV1` 现在把严格 V1 frame 包装为不可变的
managed `PreparedSubmissionV1`，其 `submit` bridge 在嵌入式路径先验证
nonzero physical attempt，再分配 Source Position 并投影为 managed
`SubmissionOutcomeMessageV1`。注入 `PreparedSubmissionAdapter` 时由该 adapter
保持 exact managed/native branch dispatch；未配置真实 native adapter 的 embedded
路径返回 typed `AUTO_FAST_PREREQUISITE_UNAVAILABLE`，不会把 native prepared
submission 悄悄改走 managed。`DelayClient.prepareAutoFast` 现在补齐零 I/O
selection seam：它消费调用方提供的 immutable Profile semantic envelopes、已签名
`NativeCapabilitySnapshotV1` 和 pinned issuer key，在签名/expiry/target/partition/
payload/metadata/timing/direct-authority 检查全部满足时生成新的 nonzero native
delivery identity，并把 shifted Broker timestamp 固定进 native prepared bytes；任何
native 不满足项都返回同一 strict managed frame；batch 逐项独立选择并保持输入顺序。
`AutoFastScheduleTest` 覆盖 eligible native、exact managed fallback、batch 和 no Source
Position admission。Snapshot issuer、
Oxia protection-before-rotation、Broker guard、Producer ownership 和 production
response evidence 仍是外部 release gate。Native adapter 若配置
`CredentialFingerprintProvider`，会在 Producer ownership 前解析当前凭据指纹并
与 signed capability snapshot 中的 immutable digest 做 constant-time 比较；漂移
返回 `CREDENTIAL_BINDING_DRIFT`，解析不可用、null、长度错误或抛异常返回
`AUTO_FAST_PREREQUISITE_UNAVAILABLE`，两者都不会调用 Pulsar transport。旧构造器
故意不配置该 provider，仅作为兼容性 seam，不能被解释为生产 credential authority
或 durable rotation protection。
同一 pre-ownership gate 对注入时钟也 fail closed：`clock.millis()` 抛异常或返回负的
epoch millisecond 时返回 `AUTO_FAST_PREREQUISITE_UNAVAILABLE`，不把本地时间源故障
解释成 expiry 或 Producer ownership；`NativeSubmissionAdapterTest` 覆盖这两个分支。
时钟认证和生产 Broker-time authority 仍是 release gate。
`PulsarAttemptJournal` 进一步把 ADR 0037 的本地可验证部分落成独立 seam：Producer
key 固定在一个 Shard，sequence 严格递增，mapping append 必须先拿到 Journal position，
精确重放幂等；replay 也会在首条和后续 mapping 安装前验证严格的 Producer sequence
successor，首条或后续跳号都以 `INTEGRITY_ERROR` 拒绝而不留下部分 state；对某个
Lane-scoped Producer，`evidenceCursor` 可将最新本地 Journal position 投影成带显式
Journal resource/creation/partition、batch cursor、generation 和最大本地 Broker 时间的
typed `EvidenceCursorV1`（旧构造器未提供 Journal identity 时仅保留 target-derived
compatibility seam）；`publishedEvidence` 进一步构造 Registry 的
`PULSAR_ATTEMPT_JOURNAL` PUBLISHED branch，绑定 exact Attempt owner、prepared hash、
producer-name hash、sequence、mapping-record hash 和可选 target-ack evidence。两者都
仍只是本地 canonical value projection，不是 contiguous Broker reader、retention、
authenticated Broker ACK/guard 或 publication 证明；
durable `RETIRED_NOT_PUBLISHED` 之后，`notPublishedEvidence` 也构造 Registry 的
`PULSAR_JOURNAL_ABSENCE` VERIFIED_NOT_PUBLISHED branch，严格绑定 fenced Pulsar
dedup channel 的 Lane/target/evidence-resource/partition/generation、显式
`PulsarJournalResource`、Attempt/prepared/
producer identity、sequence、typed cursor 和 retirement-barrier digest。该 channel 与
barrier 仍是调用方提供的 local seam 输入，不是已认证的 ExclusiveWithFencing、contiguous
reader 或 retention proof，因此不能单独打开生产 absence capability；
`appendOrReuse`/重载 `sendAfterMapped` 对同一 exact attempt
重试复用原 mapping/sequence，且在 mapping append 失败时不进入 target sender；
未 retirement 的 lower sequence 阻塞后续 Admission，
`RETIRED_NOT_PUBLISHED` 之后才允许下一个 sequence，`sendAfterMapped` 不接受未 durable
或已 retirement 的 mapping。Broker sequence 超过 Journal 最大值，或 lower sequence 缺少
inactivity-horizon 与 producer-snapshot 两项证明，统一返回
`PULSAR_EVIDENCE_DIVERGENCE`；`PulsarAttemptJournalTest` 覆盖这些分支。该类的 injected
appender 只是本地协议测试，不替代 Nereus-owned topic、ExclusiveWithFencing、guarded
reader/reconnect、Recovery-Floor retention 或真实 Broker evidence。
`KafkaReceiptJournal` 现在补齐对应的本地 transactional-receipt seam：
`KafkaReceiptResource` 显式绑定 receipt cluster/topic UUID、Route/Shard、slot
generation 以及 `shardPartition * K + receiptLaneSlot`；journal 以一个
transactional-channel key 为边界，在 target transaction sender 之前持久化 exact
mapping，严格阻塞 unresolved lower sequence，支持 mapping/retirement replay 幂等，
并投影 typed `KAFKA_RECEIPT_CONTIGUOUS` cursor。它还构造 Registry 的
`KAFKA_TRANSACTIONAL_RECEIPT` PUBLISHED branch 以及 durable retirement 后的
`KAFKA_RECEIPT_ABSENCE` branch，绑定 target/receipt UUID、partition、generation、
transaction identity、prepared hash 和 receipt/barrier digest；
`ReceiptObservation`/`Resolution` 还会对 receipt cursor identity、attempt/
prepared/record hash 漂移 fail closed，并要求独立的 retirement、LSO barrier
和 retention predicate 才能得到本地 `NOT_PUBLISHED`；
`KafkaReceiptJournalTest` 覆盖这些本地顺序、回放、cursor、resolver 与 identity fence。
注入的 appender 和调用方提供的 fenced channel 仍只是 canonical value seam，不能
证明真实 target+receipt Kafka transaction、`read_committed` Fetch/LSO contiguous
replay、ExclusiveWithFencing、retention/Floor 或 slot authority，故这些仍是 release
blocker。
`PublishAttemptLedger` 现在保留兼容的 V1/V2 bytes，并提供不改变线上
`PublishAdmissionV1` 的可选 V3 local Journal projection：先记录 adapter 分配的
sequence，再写入 exact acknowledged Journal position，definitive absence 期间设置
`retirementPending`，确认 `RETIRED_NOT_PUBLISHED` 后再清除该 fence。
`DelayShard.recordAttemptJournalMapping`、`markAttemptJournalRetirementPending` 和
`recordAttemptJournalRetirement` 只更新同一 `inflight_cf` value，不推进 Shard source
cursor；因此它们必须由已持有 exact Producer/Attempt identity 的 fenced adapter event
loop 调用，不能被解释为 source-ordered Outcome 或真实 Broker durability。
`PublishAttemptLedgerTest` 与 `DelayShardTest.attemptJournalProjectionIsDurableWithoutAdvancingShardSourcePosition`
覆盖 round-trip、identity/retirement 顺序、重启恢复和 source-position 不变性。
同步 prepare 入口的本地参数/strict-frame 校验现在统一投影为
`PreparationFailure`，其 `StableErrorV1.stage` 固定为 `PREPARATION`，同时保持
`IllegalArgumentException` 兼容性；`AutoFastScheduleTest` 覆盖稳定错误的 canonical
round-trip。它不把网络或 Broker 结果伪装成本地 preparation failure。
调用注入 adapter 时不持有 embedded service monitor，阻塞的 transport 不会
阻止 close fence；仅本地 embedded admission 在 monitor 内完成。
close 还会在 DB close 失败后继续尝试释放共享 RocksDB 资源，并把后续失败作为
suppressed exception 聚合；成功 drain 后服务立即进入 closed 状态，避免部分关闭
时继续触碰已关闭的 Store。该清理顺序仍只属于 embedded seam，不等于真实
Producer close-drain 或 Broker response drain。
构造阶段如果 DB 已打开但 source identity/metadata 校验失败，也会按同一
fail-closed 生命周期执行 Store 与共享 native resource 清理；
`EmbeddedDelayServiceTest.failedEmbeddedConstructionClosesStoreAfterSourceIdentityMismatch`
随后重新打开同一 DB，证明失败的接管尝试不会遗留 DB lock 或资源 slot。
关闭后的 embedded service 也不再暴露底层 `DelayShard` 或 pending-buffer
诊断读取；`EmbeddedDelayServiceTest.closedEmbeddedServiceDoesNotExposeShardOrBufferState`
覆盖该 facade 生命周期 fence。
这只证明本地 SDK seam；Producer buffer、batch/linger、request/delivery timeout、
close drain 以及真实 Broker response 仍属于真实适配器 release gate。

## 故障域闭合

| 故障域 | 允许影响 | 不允许传播为 |
|---|---|---|
| 单 Destination Lane/topic/credential/circuit/Producer | 该 Lane `BLOCKED`、backoff、quota reject、due lag | source pause、其它 Lane permit/queue starvation |
| target ACK 丢失或 callback 超时 | exact attempt `UNCERTAIN`，按 capability/policy 解析 | definitive failure、远端 fencing 成功 |
| Owner Lease/session 丢失 | 关闭本地 source/Admission/event gate，旧 attempt 保守解析 | 撤回已被 Broker 接管的请求 |
| 单 shard DB compaction/checkpoint/L0 storm | 该 DB 的有界 slowdown/placement repair | 消耗其它 DB 的 correctness/due/expiry minima |
| Worker 共享内存、FD、磁盘安全域失效 | 同一真实 shared domain 的 acquisition/Claim/Admission/source safety gate | 事后假装成一个 Lane 的业务拒绝 |
| Object Store credential/provider failure | handle/attest retry、Claim revoke、restore wait、GC 保留保护 | destination-backlog source pause、object-absence proof |
| Oxia/cache/watch failure | 在 exact authority 无法证明时 fail closed 或等待 | cache miss 变业务 not-found、watch 变线性化点 |
| 时钟不确定或 step | 停止新的时间敏感 Admission/expiry，等待 qualified interval | 提前 delivery、提前 expiry、wall-clock replay drift |
| source/evidence retention gap | shard/profile fail closed | newest checkpoint 猜测、空日志猜测 |

## 关键闭合关系

| 关系 | V1 不变量 |
|---|---|
| Shard/DB | `Ingress Route Partition = ownership/ownerEpoch = one RocksDB DB = Source Position atomic commit = checkpoint/restore/delete/migration` |
| Message runtime | 每 generation 零或一个 current TIMELINE/CLAIMED/PUBLISHING work；另有有界 canonical attempt-obligation refs |
| Attempt lookup | ref 携 exact inflight key、Owner Epoch、tag、generation、state、hash/digest；ledger 与 locator 双向一一对应 |
| Terminal/Replay | current terminal runtime 与 terminal summary byte-equal；Replay 新 generation 从空 obligation set 开始，旧 ref 只留旧 terminal summary |
| Retry | `BOUNDED_RETRY_POSSIBLE_DUPLICATE` 仅 unordered `BEST_EFFORT`，且 `0 < maxUncertainRetries < maxPublishAdmissions` |
| Digest | timeline semantic digest 排除 local runtime revision；instance digest 包含它；source replay 不比较新 Owner/Store/runtime instance |
| Cancel/Reschedule | 任一 UNCERTAIN obligation 存在即 `TOO_LATE`，不能因 current work 可逆而恢复管理权 |
| Lane/READY | 同一 `meta_cf/LANE` key 是 ACTIVE 或 TERMINAL_GUARD；只有 `OPEN + READY + schedulable` 有一个 exact READY key |
| Lane retirement | source-ordered gate 先到 `CLOSED`，清理与 Floor 条件满足后，same-key replacement 为 `finalGate=RETIRED` guard；retirement progress 与 terminal guard 在 equal order token 时要求 canonical Source Position 完全一致；本地 terminal-guard replacement 在同一 WriteBatch 释放 shard 的 `laneCount` slot，并在重启后保留该记账，`maxLanes=1` 可由回归测试证明复用；外部 grant/Oxia 释放与完整 terminal-guard authority 仍是 blocker |
| Scheduler | Lane-first timeline + one READY head + persisted inner DRR；Worker outer DRR 从有限 shard DB 集合重建，不跨 DB 假装原子 |
| Quota | active/pending 每非终态 generation 一次；inflight 每 Claim/attempt obligation 一次；payload ownership 不随 attempt 倍增 |
| Recovery | candidate 必须是 exact Recovery Floor descendant；session-bound Recovery Pin 覆盖选择到 activation；ACTIVE CAS 删除 pin |
| Checkpoint upload | PENDING/PUBLISHED/REAPING Upload Intent CAS；catalog 只接受 complete immutable manifest |
| Time | Admission 使用 frozen decision interval + Broker persistence inequality；replay 不采样新墙钟 |

这里的 `meta/CONTROL_RESERVE` reserveClass=3--6 与 Registry
`meta/QUOTA` quotaClass=4/5 是两个不同命名空间。后者分别表示
retained/object usage 与 grandfathered transfer state，但目前只有 key subtype，
没有 V1 value schema、digest 或 source-ordered accounting transition。因此不能把
control-reserve 的本地算术证据解释成 retained/object 或 grandfathered-transfer
quota 已实现；`DelayShard` 对这两个 class 的任意非空持久值在 activation 时
fail closed，而不是把它们当成空 projection。Registry 必须先冻结完整编码和
转移规则，之后实现才可写入或恢复这两个 class。

Durable `MessageRecord` values use checked fixed-width decoding for every
version-specific field. Any strict prefix of a canonical value is rejected as
codec validation rather than leaking a buffer-underflow exception; the local
evidence is `MessageRecordTest`.
The persisted Claim value applies the same guard before LP32 length prefixes
and u64/u32 fields; `ClaimRecordTest` covers every strict prefix of a valid
Claim and confirms that the native decoder exception is not exposed.
The publish-attempt ledger applies the same fixed-width guards and uses only
the actual minimum framing prefix, so short valid LP32 values remain readable;
`PublishAttemptLedgerTest` is the local evidence for both cases.
The large-payload reservation value applies the same guards to its post-intent
numeric and presence fields; `PayloadReservationTest` covers strict-prefix
rejection before payload-reference decoding.
`DelayClient.prepareLargePayloadCommit` now closes the client-side preparation
boundary as well: the embedded path refuses a proof whose reservation/message,
shard, object identity, payload digest/length, trust-set version or expiry
drifts from the durable reservation receipt before constructing the canonical
`CommitLargeScheduleV1` body. This is still a local pre-I/O binding check;
source-ordered reservation lookup, proof-key authority and production Object
Store verification remain release gates.
The client facade also exposes receipt-bound `issuePayloadUploadHandle` and
`attestPayloadUpload` when a deterministic local Object Store adapter is
injected. It rereads/registers the exact shard reservation and returns the
closed retryable unavailable branch when no adapter is configured; it does not
claim Oxia handle-protection CAS, provider credentials or remote immutability.
The positive local reserve-to-handle-to-attestation path is covered by
`EmbeddedDelayServiceTest.receiptBoundPayloadFacadeRereadsTheShardReservation`.
Terminal generation history uses the same guarded reads for source and
obligation framing in both legacy and v2 branches; the local prefix evidence
is `TerminalGenerationRecordTest`. Direct `DelayShard` history reads also
compare the embedded `messageId/generation` with the requested terminal key;
`DelayShardTest.terminalGenerationLookupRejectsKeyValueIdentityMismatch`
covers the misplaced-value fence before a query or runtime summary can use it.
System Mutation result reads apply the same mutationId/key check, so a
misplaced `dedupe_cf/SYSTEM_MUTATION` result cannot be returned as another
mutation's outcome.
The embedded Kafka source counter treats offsets as an unsigned 64-bit
sequence: the raw Java `-1L` bit pattern is accepted as the final valid
offset, then a separate exhaustion flag prevents a successor from being
allocated.  This avoids using a valid offset as a pre-enqueue sentinel and
keeps a failed post-exhaustion enqueue from mutating source state;
`EmbeddedDelayServiceTest.embeddedSourceAcceptsUnsignedMaximumOffsetThenExhausts`
and `EmbeddedDelayServiceTest.embeddedSourceOffsetExhaustionFailsBeforeMutatingOffset`
prove both boundaries.
Persisted Delay Shard mutation and Claim sequence metadata now preserves the
Registry's complete raw `uint64` domain through activation, checkpoint barriers
and Claim identity derivation. The local evidence is
`DelayShardTest.acceptsCompleteUnsignedPersistedShardSequences`.
Every source-position WriteBatch computes its successor through one checked
unsigned helper, including the applied sequence captured by resource
retire/delete records. The successful post-write update stores that checked
successor in the in-memory projection; `0x7fff... -> 0x8000...` is valid and
the all-ones pattern is the only exhausted value. At exhaustion the next
WriteBatch fails before any authoritative command or position state is committed;
`DelayShardTest.mutationSequenceExhaustionFailsClosedBeforeCommandMutation`
covers the near-maximum and exhausted boundaries.

Checkpoint manifest `lineageGeneration`/`shardMutationSequence` and both scalar
and typed Recovery Floor projections now use the same full-width unsigned
representation. Canonical JSON/Protobuf/floor-digest bytes retain high-bit
patterns, catalog ancestry uses checked unsigned lineage successors, and Floor
coverage compares mutation sequences unsigned. `CheckpointManifestTest`,
`RecoveryFloorRefV1Test` and `RecoveryCatalogTest.catalogComparesManifestMutationSequenceAsUnsigned`
cover the local boundary; Oxia CAS, object-store publication and source replay
remain release evidence.
The durable Command/System Mutation result values apply the same source-anchor
boundary independently: their constructors and decoders require canonical
Source Position bytes, so an empty, truncated or non-canonical result cannot
become a local projection before a shard-specific lookup runs. The evidence is
`DurableResultTest`.
The Client Command position audit now also closes the replay boundary for
position-level outcomes: its exact `commandId[41]` value is read when the same
Source Position is delivered again, allowing a previously persisted fence
rejection or `COMMAND_ID_CONFLICT` to return the same result without creating a
logical Command Result or appending another audit. The same-hash duplicate
Command path validates that locator both at the first logical result position
and after restart at a later physical position; a missing or cross-shard
POSITION value remains fail-closed. `DelayShardTest` covers exact replay with a
missing audit and `laterDuplicateCommandReplayAfterRestartUsesPositionAudit`.
The System Mutation dedupe path applies the complementary rule: an exact
already-verified mutation at its first or a later Source Position must match
the physical audit; an in-window later duplicate advances only the durable
applied position, and replay of that same later position returns the stored
first-result without re-running the mutation. A duplicate whose Broker
persistence time is outside its signed `mutationRetryUntil`, or whose retry
window was already closed by `TIME_FENCE`, produces only a position-level
`SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED` result and leaves the first logical
dedupe value unchanged; exact replay returns that same position-level result.
This avoids treating a valid post-commit duplicate as a source-position
conflict while preventing a closed retry window from regaining mutation
authority. The POSITION value is now a closed command/system identity union:
every System Mutation WriteBatch records its mutation ID, later duplicates
replace only the physical locator, and a replay at an already-advanced
position must match that locator. A command audit, a different mutation audit,
missing evidence, or a cross-shard identity is rejected instead of being
mistaken for the duplicate. The local evidence is
`DelayShardTest.systemMutationDuplicateOutsideRetryWindowKeepsFirstResultAndUsesPositionAuditOnly`,
`DelayShardTest.timeFenceMonotonicallyClosesIngressWithoutOverwritingCommandIdentity`
plus the existing source-ordered System Mutation duplicate tests.
Kafka's exclusive activation LSO uses the same fail-closed boundary handling:
an applied offset at the unsigned-64 maximum proves an exclusive barrier
without wrapping the successor calculation. The local evidence is
`SourceActivationBarrierTest.KafkaBarrierAcceptsUnsignedMaximumExclusiveOffset`.
The embedded source counter also saturates while reconstructing its next offset
from persisted state, so restart after the unsigned maximum remains a
controlled exhaustion rather than an arithmetic-open failure; the evidence is
`EmbeddedDelayServiceTest.reopenedEmbeddedServiceKeepsUnsignedMaximumSourceOffsetExhaustion`.
Canonical protocol writing now rejects values outside the unsigned 32-bit
range, while the Registry's resource-state version is treated as a raw
unsigned `uint64` across the retire body, delete-confirmation reference,
logical identity hash, GC key and durable intent record. The System Mutation
body validator has an explicit raw-`uint64` exception only for that registered
field; local mutation/claim sequences remain bounded non-negative counters.
The nested `ProtectionRefV1.protection_generation` and its `gc_cf/PROTECTION`
locator likewise retain the complete raw `uint64` pattern; external
protection authority and guarded release remain release evidence.
The same retire-identity parser now preserves complete nonzero raw `uint64`
bits for nested `ProfileRefV1` versions and Kafka receipt-slot/Pulsar journal
external generations. Its nested Pulsar Broker resource validation also keeps
the physical-topic creation timestamp byte-exact instead of applying a signed
host-range check. Object/manifest lengths remain subject to the separate
bounded local admission envelopes rather than being widened into unbounded
runtime arithmetic.
The Admission body’s duplicated nested ProfileRef/Broker-resource validators
now use that same boundary, keeping a high-bit Profile version accepted in the
descriptor and ClaimMaterialization projections instead of rejecting it in a
shallow parser. The local evidence is
`PublishAdmissionBodyTest.preservesUnsignedProfileReferenceVersionsAcrossAdmissionMaterialization`.
The local evidence is `ResourceRetireIntentBodyTest.preservesUnsignedResourceStateVersionBits`,
`ResourceDeleteConfirmedBodyTest.intentPreservesFullUnsignedResourceStateVersion`,
`KeyCodecTest.gcRetireIntentKeyPreservesUnsignedResourceStateVersionBits`,
`ResourceGcGuardTest.durableGcRecordsPreserveUnsignedResourceStateVersionBits`
and the high-bit `DelayShardTest.resourceRetireIntentIsSourceOrderedDurableAndVersionFenced`.
The descriptor and ClaimMaterialization branches now also reuse the typed
Broker-resource, committed Object Store descriptor and adapter-metadata codecs,
keeping Admission and Claim Result on one strict NFC/branch/Profile-kind
boundary rather than maintaining divergent shallow validators.
The canonical-protobuf reader also rejects field numbers outside the Registry's
`1..0x1fff` range before closed-union dispatch; the local regression is
`CanonicalProtobufTest.readerRejectsFieldNumbersOutsideRegistryRange`.

当前 `PersistentLaneScheduler.rebuildFromAuthoritativeReady` 已提供 fenced 的本地
恢复桥：它从 bounded `timeline_cf/READY` 扫描开始，严格校验对应的
`meta_cf/LANE` incarnation/version/gate/readiness、`id_cf/MESSAGE` 的当前
generation/status，并从 Message 的 source position 重算 exact timeline key、
digest 与 timeline value，再一次性替换内存 pending heads 与 active DRR ring；旧、
孤儿、重复或非 schedulable projection 会 fail closed。`ReadyDiscoveryCursor` 的
`lastScannedReadyKey`、`wrapGeneration` 和 active-ring generation 也会在同一组
SCHEDULER projection 中持久化，恢复不会把 stale Lane 重新加入 ring。恢复时还会
比较 `SchedulerRoundV1.owner` 与当前 Owner；Owner/Store 更换会重新进入
`recovery_first_pass`，由 `LaneScheduler` 跟踪已服务 Lane，保证第一轮对每个当前
eligible Lane 至多取一条记录，直到所有已发现 Lane 都获得机会；fenced READY rebuild
同样重启该 pass。恢复扫描本身现在从 READY namespace 的起点执行完整 bounded pass，
不会把 rotating discovery cursor 当成已消费的条目；因此超过 `maxReadyEntries` 的
READY 集合必然 fail closed，而不会因为 cursor 被丢弃而静默漏掉一个 head。
`LaneSchedulerTest.fencedRecoveryUsesCompleteReadyPassDespitePersistedDiscoveryCursor`
覆盖该边界。正常运行的 `discoverReady` 现在从同一 cursor 读取 bounded slice，
把 exact READY/Lane/Message/timeline projection 提升到 active ring；已经 poll 但仍
等待 Claim 的 head 由进程内 identity fence 抑制重复 offer，READY head 改变后才允许
同 Lane 的 successor 进入队列。inclusive cursor 读取额外一个条目，保证 `limit=1`
在首项上也能继续到 successor 或 wrap；`LaneSchedulerTest.rotatingReadyDiscoveryDoesNotReofferPolledHeadAndFindsSuccessorAfterWrap`
覆盖该边界。该实现证明
的是单 DB 内的本地物理边界和确定性恢复顺序。Typed
`ActiveLaneStateV1` 的 direct read、bounded compatibility projection and
same-key immutable-field preservation now have local evidence, but this still
does not equal Oxia owner/session fencing or real Lane certificate/adapter
activation.
Worker 外层 DRR 也只把至少含有一个 schedulable pending head 的 shard 纳入 visit，
空 shard 不会消耗外层 deficit；其 cursor/round 仍是跨独立 shard DB 的 bounded
process state，不伪造跨 DB 的持久原子 ring。Worker 外层 scheduler 在新建、
restore 或 READY 集合重新激活后进入 recovery first pass：当前 eligible shard
在重复服务前各获得一次 outer visit，首轮每 shard 最多取一条 work item；新增/恢复
的 shard 不会被旧 hot shard 直接越过。外层 visit 会读取当前最小 schedulable
Lane head；当该合法 record 大于 outer deficit cap 时，只在全局 byte budget 足够的
情况下临时放宽本次 shard budget，因此大记录不会被 cap 永久饿死。
`WorkerSchedulerTest.outerDeficitCapDoesNotMakeLargeHeadUnserviceable` 覆盖该边界。
ownership loss 后，`WorkerScheduler.unregisterShard` 还要求 outer shard 已 blocked
且其所有本地 Lane queue 已排空，随后才从进程内 ring/registry 移除、按剩余 shard
重算 deficit cap 并截断保留的 deficit；`WorkerSchedulerTest.shardUnregisterRequiresBlockedAndDrainedLocalQueue`
与 `WorkerSchedulerTest.unregisteringHighestWeightShardRecomputesOuterDeficitCap`
覆盖这一生命周期 fence。它只回收可重建 outer process state，不宣称跨 shard DB
的持久原子更新，也不替代 source-ordered terminal guard、Store close 或 Oxia
ownership authority。
两级 scheduler 现在还对
`weight * quantum` 与 deficit cap 做 checked arithmetic，并对运行时 deficit 累加做
saturating arithmetic；配置、注册或恢复导致的整数溢出不会 wrap 成可调度的错误预算。
两级 cap 至少提升到当前注册的 `weight * quantum`，因此 weight 大于四时不会被
固定 4×quantum cap 静默截成错误的长期服务比例；
`LaneSchedulerTest.highWeightRetainsItsConfiguredDeficitQuantum` 与
`WorkerSchedulerTest.highWeightRetainsItsConfiguredOuterDeficitQuantum` 覆盖该边界。
本轮还把 trusted due-through 明确传入两级 scheduler：`LaneScheduler` 在 inner
poll 前检查 `eligibleAtEpochMs`，`PersistentLaneScheduler` 的 READY discovery
只把不晚于该边界的新 head 返回，同时允许 future projection 留在本地队列并由
后续 poll 继续 fence；durable cursor 只推进到本轮最后一个 eligible key，future-only
切片在重启后仍可重新发现。`WorkerScheduler` 将同一边界传给每个 shard，等值按
inclusive 语义可服务。`LaneSchedulerTest.duePollUsesAnInclusiveEligibilityBoundary`、
`LaneSchedulerTest.persistentReadyDiscoveryAndPollFenceFutureDeliverAt` 和
`WorkerSchedulerTest.workerPollCarriesTheInclusiveDueThroughBoundaryToShardScheduler`
覆盖这一 local scheduler seam。无时间参数的 overload 仍是兼容接口，不是生产
Trusted UTC/Owner/Oxia 证据；真实 time authority、Broker visibility 与 production
Claim/Admission wiring 仍是 release blocker。
恢复首轮的 eligible 集合也按同一 trusted due-through 重新计算，而不是把所有
pending head 都算作当前机会；inner/outer 的最小 head byte budget 同样只看当前
due head。否则一个远期 future Lane/Shard 会让 `recovery_first_pass` 永不收敛，
阻塞其它已经 due 的 work。`LaneSchedulerTest.persistentRecoveryFirstPassIgnoresFutureLaneForDueFairness`
与 `WorkerSchedulerTest.futureShardDoesNotHoldRecoveryFirstPassOpenForDueWork` 覆盖
该本地公平性边界；生产 Trusted UTC、placement 和 Owner/Oxia authority 仍不由此
证明。
Legacy client receipt handling now closes the remaining local locator gap:
`CommandQueuedReceipt` binds `commandId`, `delayMessageId`, and Source Position
to the same Shard, and embedded `awaitApplied` validates its pinned Kafka source
and exact durable-or-pending physical locator before draining. A queued command
without a durable POSITION audit is admitted only when the pending record has the
same command/message/source tuple; after drain the audit is reread before the
result is returned. The exact pending `DelayShard.apply` result is retained for
that tuple, so a position-level `COMMAND_ID_CONFLICT` or fence rejection cannot
be collapsed into the first logical result (or become `null`) by a commandId-only
lookup. `EmbeddedDelayServiceTest.awaitAppliedRejectsForeignSourceBeforeDraining`
and `EmbeddedDelayServiceTest.awaitAppliedRejectsSameShardReceiptWithWrongPhysicalPositionBeforeDraining`
show that foreign or forged receipts are rejected without applying queued work,
while `EmbeddedDelayServiceTest.awaitAppliedReturnsTheExactPendingPhysicalConflictResult`
and `EmbeddedDelayServiceTest.queuedReceiptRejectsMessageIdFromAnotherShard` cover
the exact pending result and legacy constructor identity fences. This is a local
API/conformance guard; gateway authorization, production routing and durable
receipt-retention authority remain release blockers.
An explicit embedded `drain()` also retains physical apply outcomes within the
bounded `EmbeddedDelayServiceConfig.maxPendingCommandCount` window, so a later
legacy await can still return a position-level conflict or fence result. If
that local evidence is evicted and the durable logical result is anchored at a
different position, the seam fails closed instead of returning the wrong
logical result or `null`; this does not add outcome bytes to `dedupe/POSITION`.

§12.4 的本地时钟 guard 现在由 `TrustedUtcClock` 提供：它只从批准的
`TrustedUtcIntervalEvidence` 和注入的 monotonic reading 推导保守 interval，
对 uncertainty、sample age、wall/monotonic step 和 stabilization window
fail closed，并只允许 qualified interval 使用严格的 due/pre-expiry 谓词；
`TrustedUtcClockTest` 覆盖这些状态切换。同步源签名、Broker-time 认证以及
生产 Worker/Admission 接线仍是 release blocker。配置还必须让
`maxUncertainty` 覆盖 projection 两侧的 divergence 扩展，否则在 guard
激活前直接拒绝不可能 qualified 的 timing budget。
Placement 的 dominant-resource score 现在使用候选的 projected
`committedCapacity + required`，而不是只评分新 shard 的增量；因此 equal telemetry
下已接近 hard capacity 的 worker 不会因字典序 tie-break 抢到新 shard。
`WorkerPlacementPolicyTest.projectedCommittedCapacityBreaksEqualTelemetryTie`
覆盖该本地 placement fence。
`LaneSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` 与
`WorkerSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` 是本地回归证据，
`WorkerSchedulerTest.recoveryFirstPassServesEveryEligibleShardBeforeRepeatingOne` 与
`WorkerSchedulerTest.restoreStartsANewOuterFirstPass` 覆盖 outer recovery fairness；
不等于 production placement/authority 已完成。Lane runtime/control version 的
checked increment 也在 `Long.MAX_VALUE` fail closed，避免 READY key 或管理 CAS
版本回绕；`LaneRecordTest.versionCountersFailClosedBeforeLongOverflow` 覆盖该本地边界。
Message Control Version (`stateVersion`) 与本地 generation successor 在
Cancel/Reschedule 及其 replay projection 中也使用 checked increment；达到
Java 表示上限时先以 `INVALID_COMMAND` 持久拒绝，不会把负值写入 Message 或
timeline。`DelayShardTest.messageGenerationAndStateVersionOverflowFailClosedBeforeMutation`
覆盖这两个边界。
Scheduler round generations and persisted service-gap counters now saturate at
`Long.MAX_VALUE`, and inner scheduler byte accumulation is checked; the local
regressions are `LaneSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`
and `WorkerSchedulerTest.saturatesRoundGenerationBeforeServingAtLongMaximum`。
`PersistentLaneScheduler` also computes ring and READY-cursor wrap generations
locally and commits them to memory only after the complete five-value scheduler
`WriteBatch` succeeds. A failed projection write or malformed READY projection
cannot leave the process advertising a generation that was never durable; the
local regressions are
`LaneSchedulerTest.failedSchedulerProjectionWriteDoesNotAdvanceGenerationInMemory`
and `LaneSchedulerTest.failedReadyProjectionDecodeDoesNotAdvanceWrapGenerationInMemory`。
The inner and outer two-rotation visit limits also widen `ring.size() * 2`
before comparison, with `LaneSchedulerTest.ringVisitLimitUsesWideArithmetic`
and `WorkerSchedulerTest.outerVisitLimitUsesWideArithmetic` covering the
large-ring arithmetic boundary. This closes only the local integer-wrap path;
the production population, time-share and fairness bounds still require the
capacity/chaos evidence listed below.
Lane registration now also fences `laneIncarnation` as an immutable part of a
registered `destinationLaneId`: a different incarnation is rejected before
the in-memory queue or `PersistentLaneScheduler` registration map can be
replaced, while same-incarnation state updates remain supported. The local
regression is
`LaneSchedulerTest.rejectsLaneIncarnationChangeWithoutMutatingSchedulerState`;
this does not replace source-ordered Lane lifecycle or terminal-guard
authority.
Worker shard registration applies the same no-partial-update rule: weight
arithmetic is checked before the outer deficit cap can change, so a conflicting
duplicate Shard registration cannot influence subsequent service gaps. The
local regression is
`WorkerSchedulerTest.conflictingShardRegistrationDoesNotMutateOuterDeficitCap`.
Persisted inner fairness counters are also restored for registered Lanes that
are temporarily outside the active ring; a BLOCKED/paused Lane no longer loses
its `lastServedRound` or deficit across owner/store restart. The local
regression is
`LaneSchedulerTest.fairnessCountersSurviveRestartForLaneOutsideActiveRing`.
Scheduler recovery additionally rejects digest-valid cross-value generation
drift: the discovery cursor, active ring and round must describe the same
ring/round generations before the local scheduler is opened. The regression is
`LaneSchedulerTest.schedulerRestoreRejectsCrossProjectionGenerationDrift`.

当前代码已把 Lane 的 same-key ACTIVE/TERMINAL 分支和保守本地退休证明接入
`DelayShard`；并已补齐 Registry-shaped `ActiveLaneStateV1`、
`LaneQuotaUsageEntryV1/MapV1`、`ReadyCertificateV1`、`ActivationBarrierV1` 与
`EvidenceCursorV1` 的独立 canonical codec 与交叉校验，checkpoint manifest 也能
严格 round-trip 非空的 Kafka/Pulsar typed evidence-cursor 数组；
`RecoveryFloorRefV1` 已补齐 lineage/checkpoint/source/typed-cursor-array 的
canonical floor reference codec，`RecoveryCandidateRefV1` 与 `RecoveryPinV1`
也已补齐 candidate branch、lineage binding 和 session-identity digest 的
canonical value codecs；`EvidenceCursorV1` 现在还提供同 generation
identity/dominance 校验（Kafka offset/LSO、Pulsar inclusive member 和
Broker-time anchor），跨 generation 保持 incomparable；本地 `RecoveryCatalog` 现在对同一 shard 提供
typed Floor advancement/current-FloorRef 和同一 shard 的 Floor/catalog generation
绑定的单 active-pin create/idempotent reread/release 投影；typed Floor 还要求
传入 cursor 集合与候选 checkpoint manifest 的 evidence-cursor 数组 byte-equal，
再执行同 generation dominance，避免把 checkpoint 未覆盖的 evidence 推进到
Floor；legacy scalar Floor 创建 typed Recovery Pin 时也执行相同的 manifest
cursor 绑定；checkpoint parent lineage 还要求每个父 cursor 在子 manifest
中保持同一 identity 并单调 dominate，不能通过 source position 前进掩盖
evidence cursor 回退。但这仍不是 Oxia
Owner Lease/session CAS；`CheckpointResourceV1` 与
`CheckpointUploadIntentV1`
也已补齐 manifest-object identity 和 PENDING/PUBLISHED/REAPING 的 canonical
state branches；`CheckpointUploadIntentStore` 还提供了 exact-value create
idempotency 与本地 PENDING_UPLOAD -> PUBLISHED/REAPING revision CAS 投影。
它的 `Path` 构造还把完整 canonical intent 持久化到 checksum/atomic-rename/
directory-fsync 的 state file，并用 JVM/on-disk lock 保护跨实例 CAS；重启、
exact PUBLISHED reread 和损坏状态 fail-closed 已由
`CheckpointUploadIntentStoreTest` 覆盖。无参构造仍只是内存 projection。
`PersistentRecoveryCatalog(Path)` 现在把同一 shard 的已发布 manifest、immutable
manifest-object identity、scalar/typed Floor 和 active Recovery Pin 保存为排序的
canonical snapshot，并用 checksum、临时文件、atomic rename、directory fsync 与
JVM/on-disk lock 保护重启和跨实例 CAS；`PersistentRecoveryCatalogTest` 覆盖
manifest/Floor/Pin/ancestry 重开、generation CAS、scalar Floor 严格解码和 checksum
损坏 fail-closed。它只是本地 crash-durable projection，不能替代 Oxia
Owner Lease/session、catalog/Floor transaction 或 Object Store authority。
其中 REAPING 竞争在 response loss 后可用相同 pending identity 和
`reapingStartedAt` evidence 精确重读同一 successor；不同 evidence 仍 fail
closed，且 evidence 的 earliest trusted time 必须达到 upload deadline；
deadline 前的 reaper 证据不会推进状态。新增的
`CheckpointReapingGuard` 在进入 REAPING 前还检查 published catalog
protection 和同 lineage/checkpoint 的 active `RecoveryPinV1`；catalog/pin
读取失败也 fail closed。该边界不等于 owner abandonment/lease-loss
authority、quiescence、exact-version Object Store delete/final prefix sweep
或 Oxia transaction。
`ShardStore` 还提供 pin-aware restore overload：它在下载/暂存校验后、安装
新 Store Incarnation 前 reread exact active `RecoveryPinV1`，pin 缺失或值漂移
以及 Floor 已越过 candidate 都会 fail closed 并清理私有 `restore-tmp`。这把
本地安装边界接上了 pin/Floor 语义，但不冒充 production Oxia 的 Owner
Lease/session 同事务 CAS。
`OxiaRecoveryCatalog` 的 response boundary 现在会在 scalar/typed Floor CAS
后 reread exact published manifest，并拒绝 lineage、manifest hash、source
position、mutation sequence 或 typed evidence-cursor drift；typed 返回还必须
与请求的 cursor 集合 byte-equal，缺失 manifest 也 fail closed。这只是远端
响应验证，不等同于已经实现 Oxia transaction。`currentFloor`/
`currentFloorRef` 和 `proveFloorCoverage` 的只读响应也会绑定已发布
manifest、candidate/Floor identity、请求的 mutation/source boundary 与
ancestry 末端，拒绝漂移或缺失 Floor；equal order token 仍要求 canonical
Source Position 完全一致。
Publication 及 upload-intent publication 返回的可选 Floor 也会绑定到其已发布
manifest、同一 shard，并要求 Floor catalog generation 不晚于 publication
generation；因此 catalog publication response 不能夹带另一条 shard 或更高代的
伪造 Floor。这仍只是 adapter response fence。
这仍不是 Oxia 的 Owner Lease/session、lineage-head、catalog-generation
transaction，也不执行 Object Store upload/attestation/delete。现有 `DelayShard` 仍
通过兼容 `LaneRecord` 写入 ACTIVE 分支，因此这不被误报为已经完成 full
ActiveLaneState persistence、quota-map revision coupling、Oxia target
registration、Oxia Recovery Pin/Floor CAS、source/evidence replay 或
Recovery-Floor/retention gate。`LaneRecordEnvelopeV1` 现在还提供 Registry
field-10 直接承载 typed `ActiveLaneStateV1` 的构造、严格解码和
legacy-adapter 区分，并对 malformed typed bytes fail closed；不过当前
`ScheduleIntent` 只带 `destinationLaneId`，无法无损提供完整 active state 所需的
immutable Profile refs、canonical tuple、READY certificate 和 quota 输入，所以
`DelayShard` 仍明确停留在兼容 adapter 路径，不能把这一步误报成运行时 cutover。

协议边界也已开始按 Registry 收敛：`ScheduleIntentV1` 及其
`RetryPolicyRefV1`、`AdapterMetadataV1`、`KafkaMetadataV1`、
`CommittedPayloadDescriptorV1` 已提供严格 canonical value codec，覆盖
Schedule 的 Profile/Retry/时间/Delivery/Ordering、inline-versus-committed
payload、Kafka/Pulsar metadata、可选 business/event 字段和 quota version；
`forPrepare` 明确表示 PrepareLargeSchedule 的无 payload 形态。该增量只证明
wire/value 校验；`ScheduleCommandBodyV1` 与 `PrepareLargeScheduleBodyV1` 现在也
按 Registry 写入 Client common fields 1–3，`CommandBodies.*V1` 只作为显式迁移
seam；`PreparedCommand`/`CommandCodec.*V1` 还会把这些 fields 与 outer
message/type/retry identity 逐项比较。`DelayShard` 已把五类 body 分成明确
的运行时边界：Cancel/Reschedule V1 直接进入原子状态迁移；Schedule/Prepare
V1 必须经过显式 `V1ScheduleResolver`，校验 tuple 派生 Lane、payload 投影，
并把 canonical body/tuple 写入 `V1ScheduleBinding` sidecar。缺少 resolver 时
固定返回 `ROUTE_SNAPSHOT_UNAVAILABLE`，不会降级到旧 body；旧
`ScheduleIntent`/`LargeScheduleIntent` 只服务于非 V1 兼容命令。这个 resolver
仍是本地 authority seam，不等于 Profile/Policy/Oxia/真实 Adapter 已接入。
`ProfileBindingActivatePayloadV1`/`ProfileNewBindingClosePayloadV1` 和
`ProfileBindingControlState` 现在也提供了 source-ordered first-binding marker
投影；当 shard 已有 Profile marker 时，V1 Schedule/Prepare 会在 resolver 前
按 activation/close 边界返回对应稳定码，marker 与 System Mutation result
在同一 WriteBatch 持久化并可在 reopen 后恢复。`InMemoryProfileCatalog` 现在
提供 exact immutable semantic/binding/head/protection lookup；签名 control
target、source-ordered activation routing、历史 binding retention 与 provider
verification 仍是 release blocker。
CommitLargeSchedule V1 也有独立 canonical body 和嵌套
`PayloadCommitProofV1` codec，校验 reservation/message identity、typed Object
Store Profile、tenant scope、optional etag presence、proof ID/signature 后，
通过统一 proof view 复用现有 reservation commit 状态机。新增的
`InMemoryPayloadObjectStore` 是一个明确标注的 deterministic local seam：它
只接受 canonical `PayloadReservation`，为同一预约固定 service-owned
container/key/version，按配置的 max-handle-lifetime 与 reservation expiry
的较小值约束短期 handle，再按 Object Store Profile 的 max-bytes、expected
length/SHA-256 和 immutable-if-absent 规则处理上传；旧 handle 过期后，
同一 reservation/kind 才允许重签新的 handle，并把 response-loss 重试
固定为仍在有效期内的同一 opaque handle 与同一签名 `PayloadCommitProofV1`。
`reservationReceipt` 及 receipt-bound handle/upload/attestation 路径还会
校验完整的 service-owned object identity、Source Position、state version 和
trust-set reference，不能仅凭 bare reservation ID 冒充客户端授权；
`InMemoryPayloadObjectStoreTest` 还用 `PayloadProofTrustSet` 验证 proof。
这闭合的是本地预约绑定、handle/上传/attestation 的可测试协议形状，不是
真实 provider credentials、远端 if-absent/immutable 语义、Object Store
availability/ownership evidence、Oxia source-order authority 或完整
reservation migration；这些仍是 release blockers。
`RetryPolicySemanticV1`
现在也能按 Registry 公式重算 semantic hash、生成 typed ref，并拒绝 uncertain/
DLQ 分支和 backoff arithmetic 漂移；`RetryPolicyCatalog` 接入后，V1
Schedule/Prepare 会在 resolver 前校验 exact ref/hash 的 source-position 可见性，
并执行 ordering-mode guard，缺失语义返回
`RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION`。这仍只是 authority seam；policy
publication/source-position activation authority 之外，已接受 binding 的后续
Admission、UNCERTAIN retry 和 reopen 也会在 catalog 可用时重新使用其 immutable
budget，而不会回退到较宽的 shard default；历史 policy binding retention、
Profile/Adapter 运行时绑定和真实 ingress 迁移仍是 release blocker。
本地 source-history catalog 在同一 order token 时还要求完整 canonical
position bytes 一致，避免同一 offset/ledger-entry-batch 的 metadata 变体提前
获得 policy visibility。
业务 `PublishOutcomeBody` 与 evidence-resolution 现在还会在 catalog 可用时
保留 full-shape `RetryDecisionV1` 的 policy/cause/domain，并在 source-ordered
状态变更前重算 exact policy ref、checked retry deadline 和
`RETRY_JITTER_V1(MESSAGE_PUBLISH)`；错误 jitter 被拒绝，正确值才进入本地
timeline/state transition。旧的 opaque `UNKNOWN` placeholder 仍保留兼容路径。
Canonical V1 Admission bytes 已随 attempt ledger 保留，并为该重算提供
trusted `firstAttemptAt`。新 source-applied Admission 现在同时写入 V2
attempt ledger 的独立 typed `firstAttemptAt`/`retryDeadline`；catalog 可用时
该 deadline 再与 immutable Retry Policy 重算值交叉校验，catalog-less shard
至少以 message expiry 作为安全上界。旧 opaque V1 ledger 仍只走 structural
compatibility path，不能在没有 authoritative Admission replay 时补写窗口。
DLQ domain、外部 policy publication/activation 和历史 retention 仍是
release blocker。
Payload proof trust-set 也已补齐 canonical verifier-key list、semantic
hash/ref、Ed25519 raw-key projection 和本地 source-time validity-window
校验；`PayloadProofTrustSetControlState` 现在保留严格 source-ordered
activation/issuance-close markers，在 close 后阻止 first-seen issuance、同时
保留 historical verification 语义，`DelayShard` 以 catalog seam 校验 semantic
ref 后把 marker/result/cursor 原子写入 `meta_cf`。Oxia control authority、
trust-set catalog durability、签名 key/ACL 和历史 key retention 仍不能由该
local projection 自行推断。
对应的 `ControlReasonV1`、trust-set activate/issuance-close payload branches
也已按 Registry 严格解码；这些本地 marker apply 仍没有被误报成已经接入
Oxia control authority。
Lane PAUSE/RESUME/BREAK/CLOSE 的 ApplyShardControl projection 现在也复用
canonical `ControlReasonV1` 与 `AcknowledgementSetV1` decoder，不再用只检查
字段长度/哈希长度的浅层校验接受未知 reason kind 或 malformed optional
entries；`PayloadProofControlPayloadV1Test` 覆盖该 fail-closed 路径。

Managed Kafka/Pulsar ingress 在 Producer ownership 前拒绝无效 physical attempt；
transport exception、空结果、failed stage、CompletionStage callback
registration 失败或 malformed receipt projection 现在统一映射为 Registry 的
`ENQUEUE_RESULT_UNCERTAIN`；共享 transport 若误传
`NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED` 或 `NATIVE_ENQUEUE_RESULT_UNCERTAIN`，
也会在 managed projection 边界归一化为 managed stable-code family。只有
`PinnedPulsarNativeSubmissionAdapter` 使用 `NATIVE_ENQUEUE_RESULT_UNCERTAIN`。
`KafkaProduceResult`/`PulsarSendResult` 还在入口关闭
`PERSISTED`/non-persisted 的 stable-code、position 和 canonical identity 组合，
并要求 `DEFINITIVELY_NOT_PERSISTED` disposition 与已登记的
`BROKER_DEFINITIVE_NOT_PERSISTED`/`NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED`
stable code 配对；未知或错配 code 只产生 `ENQUEUE_UNCERTAIN`/
`NATIVE_ENQUEUE_UNCERTAIN`，绝不构造 proof，
避免把开放 result 变成 queued/proof。该分支映射由 `AdapterIngressTest` 覆盖，
包括 query-boundary projection failure、canonical identity/physical-attempt
rejection、managed
null-result 和 native-code 泄漏，避免把 managed Command 的 retry contract 误标成
native submission；malformed result 只作为 bounded `INTEGRITY_ERROR` diagnostic，
不是 non-persistence proof。错配 definitive-code 的 Kafka、Pulsar 和 native
回归向量以及 Kafka/Pulsar callback-registration failure 的 uncertain 回归也已覆盖。
managed callback 内部若在已返回的 `PERSISTED` result 上发现 malformed
position/identity，也会收敛为 `ENQUEUE_UNCERTAIN`，不会把异常泄漏成
exceptional Future；该保护与 wire projection 的 malformed-result 降级保持一致。
Native submission 的 `CompletionStage` callback registration 失败也已收敛为
`NATIVE_ENQUEUE_RESULT_UNCERTAIN`，保留原始 physical attempt；这类异常不能证明
Broker 在 Producer ownership 后没有持久化。

Kafka/Pulsar destination adapter 的 `CompletionStage.handle(...)` 注册失败也
已收敛为 `UNKNOWN / DESTINATION_OUTCOME_UNKNOWN`；若 `toCompletableFuture()`
fallback 也无法注册，则结果带有“physical completion 未观察”标记。它发生在目标
Producer 可能已经取得 ownership 之后，不能泄漏为 exceptional Future，更不能伪造
`DEFINITIVELY_NOT_PUBLISHED`；外层 `BoundedDestinationPublishAdapter` 会因此保留
对应的 zombie/in-flight charge，直到 `PublishCall` 获得物理释放证明。
`DestinationAdapterTest` 的 Kafka/Pulsar callback-registration 回归和
`BoundedDestinationPublishAdapterTest.pinnedAdapterRegistrationFailureRetainsPhysicalCharge`
覆盖这条组合边界。这仍只是 adapter transport-SPI 证据，真实 Broker side-effect
evidence 与 absence classifier 仍是发布门禁。

`PreparedSubmissionAdapter` 也在 managed wrapper 层保留该 fail-closed 语义：
managed adapter 返回 null、同步抛出，或其 `CompletionStage.thenApply(...)`
注册失败时，结果固定收敛为 managed `ENQUEUE_UNCERTAIN`，并保留原始
Prepared Command 与 physical attempt id。包装层不能把可能已经进入 Producer
ownership 的 managed 调用泄漏为 exceptional Future，也不能切换到 native
branch；`NativeSubmissionAdapterTest.preparedSubmissionWrapperRegistrationFailureRemainsManagedUncertain`
覆盖这条边界。若 physical attempt 本身无效，即使 wrapper 正在异常路径上，
也固定回到本地 `INVALID_PREPARED_COMMAND` definitive rejection，而不构造
缺少 attempt identity 的 uncertain branch；`NativeSubmissionAdapterTest.preparedSubmissionWrapperInvalidAttemptRemainsLocalDefinite`
覆盖该优先级。wrapper 自身现在也在 close 请求上 fence managed branch；close
之后不会再调用注入的 managed transport，而是返回本地 `CLIENT_CLOSED`，native
branch 仍交给其 pinned close gate，且失败 teardown 可由后续 close 重试。
`NativeSubmissionAdapterTest.preparedSubmissionAdapterFencesManagedSubmissionAfterClose`
覆盖该 branch/lifecycle 边界。这仍是本地 transport-SPI 证据，不等于真实
Broker response attestation。

`EmbeddedDelayService.enqueueOutcomeV1` 对 queued 与 uncertain 两个需要
physical attempt 的分支也先执行同一 nonzero 16-byte 校验；null、长度错误或全零
输入固定映射为本地 `DEFINITELY_NOT_QUEUED(INVALID_PREPARED_COMMAND)`，不会让
`CommandQueuedReceiptV1`/`EnqueueUncertainV1` 构造器异常穿透，也不会产生无 attempt
identity 的 uncertain union。`EmbeddedDelayServiceTest.embeddedIngressProjectsAllManagedOutcomeBranches`
覆盖 queued/uncertain 两个投影。这仍只属于 embedded conformance seam。

V1 managed submission 现在还在 Producer ownership 前强制执行
`CommandCodec.encodeFrameV1/decodeFrameV1`：`PinnedKafkaCommandIngress`、
`PinnedPulsarCommandIngress` 的 `enqueueOutcomeV1` 和
`PreparedSubmissionV1` 不再接受 legacy compatibility body；
`AdapterIngressTest.kafkaV1WireRejectsLegacyBodyBeforeTransportOwnership` 与
`PreparedCommandV1Test.managedPreparedSubmissionRejectsACompatibilityBody` 覆盖该
fail-closed 边界；同一边界由
`AdapterIngressTest.pulsarV1WireRejectsLegacyBodyBeforeTransportOwnership` 对
Pulsar 复核。`CommandQueuedReceiptV1.PreparedCommandRef` 也只从
`encodeFrameV1` 派生 frame digest；`ProtocolCodecTest.commandQueuedReceiptRejectsCompatibilityCommandBody`
证明 legacy body 不能被标成 V1 `ProtocolTuple`。旧 `enqueue()` 路径仍明确保留为
compatibility seam，不被计入 V1 submission/receipt 证据。

Target publish 的本地 transport 结果现在也在 adapter 边界执行 closed-product
校验：`PUBLISHED` 必须携带非空 delivery identity、非空 side-effect evidence、
`StableCode.OK` 和非负 Broker persistence time；若返回 pinned Broker resource，
必须同时携带完整 `uint32` physical partition；`UNKNOWN`/`DEFINITIVELY_NOT_PUBLISHED`
不得携带成功码、delivery identity、persistence time 或 Broker resource。证据是
`DestinationPublishResult` 与 `DestinationAdapterTest` 的非法组合回归。这只收紧
transport result 的本地输入边界；physical evidence journal、Lane/Worker/target
cluster admission 和真实 Broker outcome proof 仍是 release blocker。

Kafka/Pulsar target resource 现在也在 request 之前拒绝非 canonical cluster/topic
identity，避免把无法成为合法 `BrokerResourceIdentityV1` 的文本交给 Producer；
`DestinationAdapterTest.targetResourcesRejectNonCanonicalBrokerIdentityText` 覆盖
该构造期边界。

公开的 Kafka/Pulsar ingress、managed target 和 native target request value 现在在
自身构造期重复执行同一 canonical UTF-8/NFC identity fence，不能通过直接构造绕过
resource/profile 边界把 decomposed 或 malformed text 交给 transport。Pulsar native
request 还拒绝全零 `nativeDeliveryId`；`AdapterRequestIdentityTest` 覆盖五个 request
branch 的 direct-constructor rejection。

物理 admission registry 现在对同一 target cluster identity 使用相同的
canonical UTF-8/NFC fence；非 canonical 文本不能先进入 Worker/cluster 容量域，
避免 Unicode 等价字符串被拆成两个独立的 hard-cap accounting key。
`DestinationPhysicalAdmissionTest.targetClusterIdentityRejectsNonCanonicalText`
覆盖该构造期边界。

Kafka destination request 还在自身构造边界固定
`actionAt=deliverAt`。提前 action 只属于有固定 lead、Broker visibility guard
和能力位证明的 Pulsar handoff；`KafkaDestinationRequest` 遇到提前 action 会在
Producer ownership 前拒绝，`DestinationAdapterTest.kafkaDestinationDoesNotInvokeTransportForEarlyActionAt`
证明不会调用 transport。这与主设计的“`deliverAt` 是消费者最早可见时间”及
Kafka managed 时间关系保持一致；真实 Broker timing/target authority 仍需
release conformance evidence。

Pulsar managed target adapter 现在也默认只接受 ordinary `actionAt=deliverAt`。
只有显式传入 `PulsarDestinationTimingPolicy.certifiedHandoff(fixedLead)` 时，才会在
Producer ownership 前接受唯一的 `actionAt=deliverAt-fixedLead`；错误 lead、下溢和普通
policy 下的提前 action 都归一化为 `DEFINITIVELY_NOT_PUBLISHED/INVALID_METADATA`，且不调用
transport。这个 policy 只是低层 adapter 的 fail-closed guard，不能替代上游 immutable
Destination/Delivery Capability Profile、Broker visibility guard 或发布 authority；
`DestinationAdapterTest.pulsarDefaultTimingPolicyRejectsEarlyActionBeforeTransport` 和
`DestinationAdapterTest.pulsarCertifiedHandoffRequiresTheExactFixedLead` 覆盖了该局部边界。

本地 `DestinationPhysicalAdmission`/`BoundedDestinationPublishAdapter` 现在把
target 请求的 physical request/byte charge 作为显式 reservation：Worker 和 target
cluster hard cap、每 Lane cap 以及所有其它 READY Lane 的 committed minimum 都在
同一 gate 中检查；logical callback 超时只能把 reservation 标为 `ZOMBIE`，达到
Lane zombie cap 立即阻止该 Lane 的新 Admission，直到 physical release 后显式清除
block。delegate stage 完成（包括 `UNKNOWN`）才释放 request/byte charge；capacity
拒绝不会调用 delegate。`DestinationPhysicalAdmissionTest` 与
`BoundedDestinationPublishAdapterTest` 覆盖 READY minimum、跨层 cap、identity、
zombie 和 response completion；开启一个尚未 READY 的 Lane 时，其候选
READY minimum 现在只计入一次，`openingLaneCountsItsReadyMinimumExactlyOnce`
覆盖恰好填满 Worker/target-cluster 最小保护容量的边界。`BoundedDestinationPublishAdapter`
不再在 adapter monitor 内同步调用 delegate，而是把调用提交到注入的
Lane/Adapter executor（默认构造器使用 Java 21 virtual-thread executor）；因此同一
adapter 上一个永久阻塞的同步 metadata/send 调用不会阻塞另一个健康 Lane，且
`blockingDelegateCallDoesNotBlockHealthyLane` 覆盖了该隔离边界；executor 拒绝会在
delegate 尚未取得 ownership 前归一化为 `UNKNOWN` 并释放 reservation。Pinned
destination adapter 对同步 transport exception 或空 stage 也不假定 ownership
已结束，而是返回带“physical completion 未观察”标记的逻辑 `UNKNOWN`。若底层
`CompletionStage` 的 callback registration（包括其 `toCompletableFuture()` fallback）
都失败，则只归一化逻辑结果为 `UNKNOWN`，同时把 reservation 保留为
`ZOMBIE`（zombie cap 已耗尽时保留为 in-flight），直到 `PublishCall` 的物理释放
证明或 fenced teardown；不能把 registration failure 当成 physical completion。该组件只是进程内可重建的资源闸门，尚未接入持久
`ActiveLaneState`/`ReadyCertificate`、Owner/Lease/Oxia authority、真实 channel
teardown 或 Broker evidence journal，因此不能宣称 production admission 已闭合。

Adapter close 也已按同一 fail-closed 原则收敛：第一次 close 请求立即阻断
新的 ingress/submission/publish，但底层 channel/Producer close 抛错时仍保留
“未完成”状态，后续生命周期调用可重试 native teardown；只有底层成功后 close
才成为永久幂等。`CloseGuardTest` 与
`DestinationAdapterTest.destinationCloseFailureCanBeRetriedWhileAdapterRemainsFenced`
覆盖该本地 fence/retry 证据。它不等于 Broker-side cancellation、physical charge
release 或生产 channel quiescence；这些仍由外部 teardown/evidence gate 证明。

该 close gate 现在还把每次同步 transport invocation 的接受与 close 请求放在同一
线性化边界：不再允许独立读取 `isClosed()` 后在 gate 外调用 transport。close 前已
接受的 invocation 可以在 close 请求后完成，并继续按 UNKNOWN/physical-charge 规则
处理；close 线性化后则不能再开始新的 transport call。gate 不持有 adapter monitor
执行同步 transport，因此永久阻塞的调用不会阻止 close fence 或 teardown retry；
Bounded wrapper 仍把阻塞 transport 放在 Lane/Adapter executor。

本地 physical-admission registry 另有明确的 Lane teardown 边界：只有 READY
已关闭、所有 physical/zombie charge 已清零且 exact `laneIncarnation` 通过 fencing
时才可 unregister；旧 channel 的迟到 callback 不能删除新 registration。
`DestinationPhysicalAdmissionTest` 覆盖 READY/残留 charge/stale incarnation
拒绝和 quiescent replacement。该证据只覆盖进程内可重建资源回收，不替代
source-ordered retirement、Oxia grant、terminal guard、Recovery Floor 或真实
channel teardown authority。

Scheduler registry 也已补齐同一生命周期证据：terminal gate、empty queue 和
exact Lane incarnation 通过后才允许移除；`PersistentLaneScheduler` 在同一个
projection WriteBatch 清理 ring/fairness/discovery 状态，写失败会恢复内存
registration。`LaneSchedulerTest` 覆盖 stale identity、pending work 和 restart
后的空 projection。该证据只证明 bounded local index 回收，不替代 terminal
guard、Oxia 或 source-ordered retirement authority。

`PublishOutcomeBody.encodeInitial` 和
`PublishOutcomeBody.encodeEvidenceResolution` 现在复用 Registry 的 common fields
1–3，并在返回前执行本地 decode round-trip；初始 Outcome 的
`PUBLISHED`/`NOT_PUBLISHED`/`UNKNOWN` side-effect/disposition/stable-code/retry
组合因此不会由调用方随意拼出，definitive transfer 必须是 canonical
`ChargeVectorV1`，Evidence Resolution 的 cursor 必须是 typed canonical
`EvidenceCursorV1`。Apply 现在还要求 definitive Outcome/Resolution 的 transfer 与
对应 Admission ledger 保留的 charge 做 canonical byte-equality；不一致写入
`REJECTED(STALE_SYSTEM_MUTATION)`，不改变 attempt、message、timeline 或 quota，
而 `UNKNOWN` transfer 仍不参与 release。`PublishOutcomeBodyTest` 与 `DelayShardTest`
覆盖编码器、非规范 ChargeVector、typed cursor、transfer mismatch 以及
source-ordered close/requeue；这仍只是 canonical
body codec 和本地 transition seam；当前 local transition 还验证了已 admitted
generation 在 Close marker 后收到 definitive `NOT_PUBLISHED` 时固定写入
`LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED` 并停止 retry。它不等于签名服务、真实
Broker evidence、strong-capability retirement 或 production outcome authority 已完成。
Activation 还从 durable `PUBLISHING`/`UNCERTAIN` ledgers 重建 Registry
`meta/QUOTA` class-2 的 canonical aggregate 与 exact 66 维 outcome vector；
per-Lane class-3 map 提供 dimensions 1--17，open-attempt ledger 提供 outcome
dimensions 9--15，外部/物理 dimensions 18--66 在本地兼容投影中保持 zero。
兼容 shard 即使没有 immutable capacity envelope 也不会在重启后丢失这个本地
vector，绑定 envelope 时再校验其持久 grant-bound projection。旧 class-1
`ShardQuota` 与旧 class-2 `OutcomeReserveUsage` 只读用于迁移/校验，新 mutation
写 canonical class 2 并清除 stale class 1。这仍不等于外部 reserve authority。
Apply 同时验证 operation-specific logical identity：initial Publish Outcome 必须使用
body 中的 `PublishAttemptId`，Evidence Resolution 必须使用
`SHA-256("nereus-delay-evidence-resolution-logical-id-v1\0" || PublishAttemptId || evidenceId)`；
身份不匹配写入 `REJECTED(UNAUTHORIZED_SYSTEM_MUTATION)`，并在 source-ordered
路径中保持 attempt、message、timeline 与 quota 不变。`DelayShardTest` 覆盖 Publish
Outcome 与 Evidence Resolution 的错误 identity fence 以及后续正确 identity 的
source-ordered apply。
Initial Publish Outcome 现在还把 admitted ledger 中的完整 canonical
`OwnerIdentityV1` 与外层 Owner 做 byte-equality fence；同 epoch 但 deployment、worker
或 lease-fencing digest 不同的作者不会仅凭 epoch 命中旧 attempt。若新 Owner epoch
无法直接命中旧的 inflight key，bounded lookup 只为 Registry 规定的
`UNKNOWN + OWNER_FENCED + RECOVERY_FIRST_SEND_UNCERTAIN + UNCERTAIN_HOLD` recovery
tuple 重新绑定原 admitted ledger，并保留原 epoch key。当前 Owner 的 Oxia/guarded
recovery 资格仍在本地 shard 之外验证；任何跨 Owner definitive 或 scheduled Outcome
都拒绝。
同一关闭边界下的 `UNKNOWN` 结果保留 `UNCERTAIN` obligation 且不创建新的
`UNCERTAIN_RETRY` timeline；后续 Resolve retry 仍由 closed-Lane gate 拒绝。

Durable attempt-ledger charge projection now has a canonical-shape fence: when
retained `admissionBytes` starts with the Registry common-body field-1 tag
(`0x0a`) but `PublishAdmissionBody.decode` fails, `DelayShard` fails closed;
only arbitrary pre-V1 synthetic adapter bytes use the bounded zero-charge
compatibility projection. `DelayShardTest.malformedCanonicalAdmissionLedgerDoesNotDowngradeToZeroCharge`
proves that the malformed canonical-looking ledger does not create `PUBLISHING`
state or an open attempt. This is local integrity evidence, not authenticated
source-ordered Admission authority or external grant evidence.

同一 logical-identity fence 现在也覆盖三个此前容易被签名 envelope 掩盖的入口：
`PUBLISH_ADMISSION_V1` 必须使用 body 的 `PublishAttemptId`，
`CLAIM_RESULT_V1` 必须使用 body 的 `ClaimId`，而 `EXPIRE_GENERATION_V1`
必须使用 Registry §4.8 规定的
`SHA-256("nereus-delay-expiry-logical-id-v1" || DelayMessageId || u32be(generation) || i64be(expireAt))`。
任一身份不匹配都会在 handler 状态变化前持久化
`REJECTED(UNAUTHORIZED_SYSTEM_MUTATION)` 和该 Source Position；Admission、
Claim、expiry 的 message/timeline/quota 不被改变。`DelayShardTest` 的
`sourceOrderedPublishAdmissionPersistsAttemptAndMutationResultTogether`、
`sourceOrderedExpireMutationAtomicallyClosesScheduledGenerationAndDedupes` 和
`sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline` 覆盖错误身份
拒绝以及随后正确 mutation 的 source-ordered apply。

DLQ Export Result 的 canonical body 已校验 `ChargeVectorV1`，而 configured
`DlqExportRecord` 现在持久化 policy-derived retained charge projection；
`NOT_CONFIGURED` terminal record 固定保留 all-zero projection，legacy v1 record
也按 zero-charge 兼容解码。Source-ordered apply 要求每个 callback 的 transfer
与该 outbox projection canonical byte-equal；不一致写入
`REJECTED(STALE_SYSTEM_MUTATION)`，不改变 outbox state 或 source position
边界，且 `UNKNOWN` 仍不会释放 charge。`DlqExportApplyTest` 覆盖 non-zero
retained charge 的成功/不匹配结果、legacy decode 与 outbox 原子推进。真实
policy publication、external charge authority、adapter evidence 和 Object Store
ownership 仍是 release blocker。成功应用还保留 body 的 `stable_code` 到
`SystemMutationResult`；`DLQ_EXPORT_OUTCOME_UNKNOWN` 的 source-ordered 回归
覆盖该结果可见性。

DLQ `terminalRevision` now follows the Registry's complete nonzero `uint64`
identity boundary through `DlqExportRecord` IDs/bytes and
`DlqExportResultBody` parsing; zero remains invalid while high-bit patterns are
preserved. `DlqExportRecordTest` and `DlqExportResultBodyTest` cover the
high-bit vectors. Local terminal-state arithmetic remains bounded separately;
external DLQ policy and provider authority remain release evidence.

The same `DLQ_EXPORT_RESULT_V1` parser now closes the nested retry boundary:
`RetryDecisionV1` must use the exact canonical field sequence and its nested
`RetryPolicyRefV1` is decoded (not merely accepted as nonempty bytes), including
the complete raw nonzero policy version and 32-byte semantic hash. The parser
also enforces the local `first_attempt_at <= next_retry_at <= retry_deadline`
interval (and rejects a deadline before the first attempt), so a structurally
valid retry cannot schedule outside its own decision window. Negative vectors
are in `DlqExportResultBodyTest`; policy publication and source-ordered
authority remain external evidence.

The business `PublishOutcomeBody` retry decoder applies the same exact
`RetryDecisionV1` field sequence and first-attempt/deadline interval fence as
the DLQ parser. Unknown nested fields and an out-of-window `next_retry_at`
therefore fail closed on both outcome paths; `PublishOutcomeBodyTest` covers
the two rejection vectors.

`PublishEvidenceV1`/`ExternalDeliveryIdentityV1` 进一步把 Registry 的
`PublishEvidenceV1` 公共字段、kind 对应 oneof 分支、verification-status 语义、
owner identity 和 domain-separated `evidence_id` 固定在一个共享 codec 中；
`PUBLISH_OUTCOME_V1` 与 `DLQ_EXPORT_RESULT_V1` 不再只接受任意非空 nested bytes。
`ChannelResourceIdentityV1`/`CredentialUseLeaseV1` 现在把 channel-bearing
absence/non-submission 分支的 Adapter/target branch、strong-capability evidence
resource presence、producer digest、credential binding and destination-channel
holder-scope checks 收敛到同一 canonical implementation；Publish Admission、
Ready Certificate 和 Evidence codec 复用该边界，且 Admission/Ready Certificate
拒绝 credential binding drift 或 certificate 超过 protected Channel lease 的
有效期。当前分支检查覆盖 canonical shape、typed cursor、Broker/Channel/Profile
nested identity 和 owner 匹配，但
真实 adapter 的 authenticated response、lease protection CAS/TTL authority、
retention barrier、external proof ownership 仍是 release blocker。
其中 `OperatorAttestationEvidenceV1` 的 verifier 槽位也已强制为
`ProfileKindV1.EVIDENCE_VERIFIER`，并由 `PublishEvidenceV1Test` 覆盖错误
Profile kind 的负向回归；Profile 注册、key activation 与签名 authority
仍属于外部 release gate。
`LaneTerminalGuardV1` 的两个 Profile 槽位也已在构造和 decode 路径强制为
`DESTINATION`、`DELIVERY_CAPABILITY`，由 `LaneTerminalGuardV1Test` 覆盖
双向错误 kind；兼容层仍把已由 resolver 解析的 canonical Lane tuple 作为
opaque bytes 保存，完整的 tuple/profile byte projection 仍依赖 resolver 与
外部 authority，而不是在本地猜测 tuple 结构。

`ChannelResourceIdentityV1` now preserves the complete raw unsigned patterns for
its physical `channel_generation`, optional `evidence_generation`, and
`credential_binding_generation` fields, matching the raw `EvidenceCursorV1`
generation boundary. The same full-width rule now runs through credential
attestation/binding, Head/protection, use lease, Ready Certificate, native
capability snapshot, rotation request/result, and Profile control projections:
zero is invalid, but a host signed-integer high bit is valid. Head/protection
revisions are nonzero complete raw `uint64` identity values; other local
control versions remain bounded positive counters. The independent Broker
resource-guard configuration generation also preserves raw nonzero `uint64`
bits. High-bit coverage is provided by
`CredentialBindingV1Test`, `ProfileControlRequestV1Test`,
`ControlResultCodecTest`, `ChannelResourceIdentityV1Test`,
`ProtocolCodecTest`, and the Publish Admission/Ready Certificate fixtures.

The nested Ready Certificate Broker attestation and config generations now
also preserve complete raw `uint64` patterns through the shared
Admission/certificate decoder; `ReadyCertificateV1Test` covers both high-bit
values. The external Broker rollout remains the authority for their meaning.
The shared decoder also parses the nested `ActivationBarrierV1` and every
repeated `EvidenceCursorV1`, rather than treating those fields as opaque
non-empty bytes. It requires at least one cursor and rejects non-canonical
nested branches plus unsorted/duplicate cursor identities before either the
Admission body or the public `ReadyCertificateV1` wrapper can expose the
certificate; direct-parser rejection vectors are in `ReadyCertificateV1Test`.

`PublishAdmissionBody` 还把 `PreparedPublishDescriptorV1` 的 adapter kind、固定
adapter encoding version（并要求 immutable Destination Profile 也 pin 版本 `1`）、target resource、physical partition 与嵌套
`ChannelResourceIdentityV1` 做 exact equality，要求 `business_metadata` branch
与 adapter 一致，并要求 descriptor Destination
Profile 与 channel credential lease 的 ProfileRef 一致；descriptor 的两个 Profile
kind 也按 Registry 字段位置固定。`PublishAdmissionBodyTest` 的
`rejectsDescriptorAdapterIdentityDrift` 覆盖哈希有效但 adapter identity 漂移的
fail-closed 路径。这是本地跨对象解析证据，不替代 Profile semantic publication、
authenticated channel registration 或真实 Producer admission authority。

Descriptor timing 的结构解析现在允许 `actionAt <= deliverAt`，但不把提前值当作
许可：没有 Profile catalog 的兼容 shard 只接受 ordinary managed，配置 exact
catalog 的 shard 才会按 Destination/Delivery Capability semantic bytes 验证固定
Pulsar handoff lead、capability bit、target resource、target partition policy 和
checked subtraction。
`PublishAdmissionBodyTest.retainsCertifiedHandoffTimingForProfileSemanticValidation`
与 `acceptsOnlyThePinnedPulsarHandoffLead` 覆盖这一分层边界；真实 Profile 发布、
Broker guard attestation 和 Producer admission authority 仍是 release blocker。

Admission 的 Broker-time 边界也已有本地闭合证据：`DelayShardConfig` 暴露
`maxIngressBrokerTimestampDivergenceMs` 与 `maximumAdmissionMutationEnqueueAgeMs`，
`DelayShard` 在 source-ordered apply/replay 调用 `PublishAdmissionBody.requireBrokerTiming`，
检查 `bp + divergence < min(expireAt, readyCertificate.validUntil)` 及 decision interval
距离上界，并对加减溢出 fail closed；`PublishAdmissionBodyTest` 覆盖距离、expiry、
divergence 和溢出拒绝，`DelayShardConfigTest` 覆盖正式字段与兼容构造器。该证据只证明
本地状态机和算术 fence，不能替代真实 Broker timestamp certification、capacity artifact
发布或外部 Admission authority。

同一 apply/replay 路径还会在 timing/profile fence 失败时撤销 exact matching live Claim，
并持久化 `STALE_SYSTEM_MUTATION`；`DelayShardTest.publishAdmissionTimingFailureRevokesMatchingClaimBeforePersistingStaleMutation`
覆盖该 no-attempt/no-Producer 前置状态。该测试仍属于本地状态机证据，不代表外部 authority 已部署。

`ResolveUncertainBody` 的 `ATTACH_PUBLISHED_EVIDENCE`/
`ATTACH_NOT_PUBLISHED_EVIDENCE` 分支也不再接受任意 opaque nested bytes，
而是要求 typed evidence 的 Publish Attempt owner 和 verification status
匹配。`ATTACH_PUBLISHED_EVIDENCE` 现在在 source order 中校验 exact current
`UNCERTAIN` obligation；若当前代仍有 timeline/Claim work，会先删除该可逆
work 后 terminalize；若另有 current PUBLISHING attempt，则只移除目标
obligation 并保留该发送；也支持旧代 `terminal_cf` summary 中仍开放的 exact
obligation。当前代与 published terminal、ledger、pending quota、outcome
reserve、mutation result 和 source position 一起原子提交，旧代只更新
terminal summary、ledger、duplicate-risk 和 outcome reserve，不能修改新代；
重复 mutation 返回已持久化结果。`ATTACH_NOT_PUBLISHED_EVIDENCE` 现在也
校验 exact UNCERTAIN obligation 并按 remaining-obligation/all-absent 规则
保留未决工作、保留另一个 current PUBLISHING；若当前是 stale `CLAIMED`，
则与目标 ledger 一起原子撤销为 `UNCERTAIN/NONE` 并推进 Message state version，
或在无剩余 obligation 时
原子化为 definitive retry
及其 closed-lane/budget/expiry terminal 分支；当前 `CLAIMED` work 的撤销与
definitive retry 也由
`DelayShardTest.sourceOrderedNotPublishedEvidenceRevokesClaimAndNormalizesDefinitiveRetry`
以及
`DelayShardTest.sourceOrderedNotPublishedEvidenceRevokesClaimWhenAnotherUncertainObligationRemains`
覆盖；外部 authenticated control/evidence authority 和完整 retry/charge
policy 仍未完成。

此外，Claim 从 `UNCERTAIN_RETRY` timeline 创建时现在冻结实际的
`sourceWorkKind=UNCERTAIN_RETRY`，不再根据 retry timestamp 误判为
`DEFINITIVE_RETRY`；完整 Claim materialization/recovery 仍是独立的待完成边界。

`ClaimResultBody` 现在还要求 field 20 `ChargeVectorV1 transfer` 与
`ClaimPreconditionV1.claimed_charge` 做 canonical byte-equality。这样签名的
permanent pre-send callback 不能在 source-ordered apply 时偷偷替换 Claim 的
reversible charge projection；`DelayShardTest.sourceOrderedClaimResultTerminalizesMatchingReplayStableTimeline`
覆盖不一致 vector 在 parser 和 source-ordered apply 层的 fail-closed 拒绝，且确认
Shard 状态、quota 和 timeline 不被改变。该项只是本地 replay-stable charge fence，
不能冒充完整的 grant policy、external charge authority 或 materialization/recovery
accounting。

System Mutation 统一入口现在也把 handler 的 checked-arithmetic overflow
转换为 `STALE_SYSTEM_MUTATION`，持久化该 mutation 的拒绝结果和 Source
Position，而不是让异常逸出并留下可重复应用的 source gap；
`DelayShardTest.systemMutationStateVersionOverflowPersistsStaleResult` 用
`Message.stateVersion=Long.MAX_VALUE` 覆盖这一边界。该路径仍只保护本地
source-ordered state machine，不能替代外部 writer/lease authority。

Evidence branch validation also checks the adapter-specific target resource
and cursor/channel branch, so a Kafka evidence envelope cannot carry a Pulsar
resource (or vice versa); authenticated Broker response and external proof
ownership remain separate release gates.

`OwnedDelayShard` 现在还提供了带 assignment/barrier/source-connection 校验的
统一 `replay` seam，以及兼容性的 `replayCatchup`/`replaySystemMutations`：
Command 和 signed System Mutation 通过 `SourceReplayEntry` 在同一个
source-order stream 中选择分支，每条记录先走同一 shard WriteBatch，成功后才
推进 catch-up cursor，并返回带分支类型的 `SourceReplayOutcome`。对于同一
Command/Mutation 在后续 Source Position 的合法 logical duplicate，outcome 只把
首次 durable result 投影到当前 physical position；RocksDB 中的首次结果锚点不被
改写，canonical position metadata 不一致仍 fail closed。它仍不等同于
真实 Kafka/Pulsar consumer、Oxia session/ephemeral authority、broker assignment/
guard 或 production activation transaction。

为了闭合主设计 §8.4 的 source-turn 约束，当前 seam 还提供
`SourceReplayCursor`、`ReplayTurnBudget` 和 `SourceReplayTurn`：
`replayCatchupTurn`、`replaySystemMutationsTurn` 与混合 `replayTurn` 在每次
写入前同时检查 record count、canonical bytes 和 monotonic elapsed-time cap。
canonical-byte 计费使用 exact source-position bytes 加 canonical NDL1/System
Mutation frame；单项超出 byte cap 会在消费/WriteBatch 前 fail closed。cursor
只保留一条 look-ahead，因此 yield 不会丢失下一条 source record；旧的完整
`Iterable` overload 明确使用 unbounded compatibility budget，不能作为生产
source consumer 的 bounded turn。实现先 `peek()` 并完成 shard WriteBatch，
成功返回后才 `next()`；校验、fencing 或存储失败会让 exact look-ahead record
留在 cursor 上，下一轮可以原样重试。`OwnerLeaseTest` 已覆盖 record-cap 后的
cursor continuation、single-record byte overflow，以及 source-gap 后的 cursor
保留。

V1 的 assignment 接管路径现在还会显式 pin `SourceReplaySuccessor`：同一
canonical Source Position 的 broker redelivery 可以由 durable apply 幂等处理，
但任何后继位置都必须由 adapter proof 判定为 immediate successor；内置的
`strictKafka()` 在 offset gap 处 fail closed，`strictPulsarBatchMember()` 只覆盖
同一 batch entry，跨 entry 仍要求真实 Pulsar adapter 提供 successor 证明。
`SourceReplaySuccessorTest` 和
`OwnerLeaseTest.v1CatchupPinsTheAdapterSuccessorAndRejectsAKafkaGapBeforeApplyingIt`
证明跳过的 Kafka record 不会被静默重放。旧的 assignment-only overload 保留为
兼容性 monotonic seam，不能作为 V1 source-gap evidence。
接管 replay 还提供 live-clock overload，并在每条记录前重新检查 lease；长时间
catch-up 中途过期会在下一条记录前 fence，cursor 保留在最后已提交的位置，
不会继续用旧 owner 写入。固定 `nowEpochMs` overload 仅保留给确定性兼容调用，
`OwnerLeaseTest.liveCatchupClockFencesBeforeApplyingAfterLeaseExpiry` 覆盖该边界。
正常 source apply 还可使用 `OwnedDelayShard.applyAuthoritatively`，在每次
delegate WriteBatch 前 reread Oxia lease；同 identity 的续租可以更新本地 expiry，
而 owner/epoch/token/session、状态或 expiry 回退都会在写入前 fence。旧的本地
apply overload 仍明确只是 embedded seam。原始 `DelayShard` delegate accessor
现为 ownership 包内可见，仅供 drain/inspection 实现使用，包外调用方不能绕过
owner lifecycle 和 lease gate。

Worker 资源侧现在还提供了本地 `WorkerLoadVector` 与
`WorkerPlacementPolicy`：它们先按完整 committed capacity、固定/transition
demand 以及 owned/open DB slots 做 hard filter，再以 dominant-resource/load
分数处理 stale telemetry、minimum residence、hysteresis 和 checkpoint/replay
movement cost；这只是可复现的评分 seam，不是 Kafka cooperative assignor、Oxia
desired-placement plan 或 Owner Lease authority。

`ShardStoreConfig.maxWriteBufferBytesPerDb` 现在是显式的 Worker 配置，
`ShardStore` 会把它绑定到每个 Column Family 的 RocksDB
`ColumnFamilyOptions.setWriteBufferSize`，同时继续由进程级
`WriteBufferManager` 约束共享 memtable 总预算。共享 `Env` 明确承载进程级
background pool；每个 DB 另外绑定 `maxBackgroundJobsPerDb` 以及非零
`reservedFlushJobs`/`maxCompactionJobs` split，并在配置不满足 split 时 fail closed。
这证明了本地 option binding 和非法配置证据（`ShardStoreTest.perDbWriteBufferCeilingMustBePositive`、
`ShardStoreTest.backgroundJobSplitMustFitPerDbCeiling`），但不把每个 CF 的上限或
每个 DB 的 job split 误报成聚合 WAL/SST/temp 运行时记账；work-class reserve、
真实 JVM/cgroup/rlimit 和 Oxia placement authority 仍是外部资源证据。

资源审计现在还有一个可复用的本地物理观测边界：`RocksDbUsageSnapshot`
从打开的单 shard DB 读取 live SST、WAL、MANIFEST、L0、compaction-pending
以及不跟随符号链接的实际 DB 文件总量；`RocksDbUsageLimits` 按 shard identity
拒绝重复观测，逐 DB 校验上限，再用 checked addition 校验 Worker 聚合上限和
`FileStore` 对应卷的最小可用空间。`ShardStore.physicalUsage()`/
`requirePhysicalUsageWithin` 与 `RocksDbUsageLimitsTest`、
`ShardStoreTest.physicalUsageProbeAndGuardObserveOneShardDb` 证明了这个本地
guard。`SharedRocksDbResources.startRocksDbUsageMonitor` 现在注册每个打开
Store 的 identity-bound physical source，按 fixed delay 聚合所有 shard DB，并在
Store native teardown 前移除 source；缺失、身份不一致、逐 DB 或 Worker 聚合
上限超限以及 filesystem floor 证据都会复用同一个 sticky runtime gate。
`NativeResourceUsage`/`WorkerNativeResourceLedger` 又提供了互斥的
RocksDB-native bucket attribution（shared block cache、memtable、table-reader
metadata、pinned blocks/iterators、flush/compaction scratch）和独立的
other-native bucket；`SharedRocksDbResources` 在 JNI 创建前预留 shared cache/WBM，
并在对应 native close 后释放。`WorkClassScheduler` 对七个冻结 work class
提供 bounded queue/turn record-byte-time caps、`LEASE_FENCE` 抢占和 stale-class
选择；`WorkClassResourcePool` 还按 class 保护 non-borrowable record/byte
minimum、把 acquisition checked-sum overflow 转成 closed rejection，并限制
borrowed hold time；这些仍是本地 scheduler/resource seams。
`WorkerRuntimeSafetyGate` 还把新鲜的 JVM/cgroup/FD/filesystem
observation 接入一个 sticky `ACTIVE -> DRAIN_OR_MIGRATE` 门；共享资源的
ownership/restore slots 和 embedded Claim 在门未恢复前 fail closed，只有
显式 empty-drain activation 才能重新开放。`WorkerRuntimeResourceMonitor`
现在提供可关闭的 fixed-delay envelope probe scheduler，并把 probe 异常和
envelope mismatch 路由回同一个 sticky gate；`WorkerRocksDbUsageMonitor`
覆盖了本地 per-DB physical usage observation，但不提供 WriteBatch 的
work-class reserve admission，也不替代真实 checkpoint/compaction 调度和 Oxia
placement authority；这些继续保持 release blocker。两个 monitor 都由
`SharedRocksDbResources` 持有并在 native teardown 前关闭，补上了本地 monitor
生命周期边界。

Control Reserve 的本地投影也已覆盖 Registry 的 class 6：
`meta_cf/CONTROL_RESERVE` 以 `CapacityVectorV1` 持久化 Broker system-writer
reservation，绑定 `NON_OUTCOME_CONTROL` grant identity；class 6 只接受维度
51–53，class 3 排除这些维度，二者合计必须被同一 immutable grant 覆盖。
`DelayShard` 的同步 charge/release 和重开校验已有
`DelayShardTest.systemWriterReserveProjectionIsPartitionedAndPersistsAcrossReopen`
及错误维度 fail-closed 测试。该证据只闭合 shard-local projection，不证明
Route Broker/source-writer 的远端 quota authority、跨 shard placement 或
实际 operation charge。

当前 `DelayShard` 还把 Registry class-3 `meta_cf/QUOTA` 接入为一个本地兼容投影：
`LaneQuotaUsageProjection` 按 Lane incarnation/usage revision 记录可精确重建的
message、reservation、Lane-slot 以及每个 Claim/attempt obligation 的
`inflight_messages`/`inflight_bytes` 维度；命令及 source-ordered system mutation
与 Registry class-2 canonical aggregate、class-3 map 在同一 WriteBatch 更新，打开/恢复时从
`id_cf`/`meta_cf` 和 `inflight_cf` 的 durable Claim/attempt ledger 重建并逐字节校验，
同时复算 aggregate 的 pending/reservation/Lane-cardinality counts；已存在但数值漂移的
aggregate 会 fail closed，缺失的 aggregate 只在内存回填并等待下一次 source-ordered mutation
持久化。旧 class-1 `ShardQuota` 与旧 class-2 `OutcomeReserveUsage` 只读用于迁移/校验，
新 mutation 会清除 stale class 1。零 field-7
charge 的 legacy/synthetic ledger 按一个 durable record 计数，canonical admission
中的 field-8 attempt bytes 则保留；若运行时发现旧 map 缺失对应 ledger，释放路径
会先从 durable ledger 重建再在同一批次修复。`LaneQuotaUsageProjectionTest` 与
`DelayShardTest` 覆盖 checked arithmetic、Claim/admission、close/terminal/replay/
retirement 路径和 reopen fence。execution beyond local attempt bytes、retained、
evidence 与外部 adapter 维度仍未接入，因此这是 map 的 local compatibility subset，
不是完整 ActiveLaneState、grant revision coupling、Route Broker authority 或多
shard placement proof。退休前会扫描完整 17 维 usage vector；即使未来投影开始
记录 retained/control/evidence 维度而当前 adapter 尚未理解，仍会 fail closed，
不会把带残留 usage 的 Lane 变成 terminal guard，同时原子释放该 Lane 的普通
和 strong-capability cardinality slots。

Owner Lease 的本地 CAS 投影现在还按 V1 lifecycle graph 拒绝回退状态和
`FENCED -> ACTIVE_FOR_COMMANDS` 复活；允许的前向 acquisition/activation
跳转、fence 和 fenced recycle 都保留。续租响应若改变期望的 lifecycle
state 也会 fail closed，即使 fencing/assignment/session identity 相同，避免
把状态漂移误当作成功续租。真实 Oxia ephemeral session/CAS 仍未完成。
内存 Owner Lease 测试 authority 的 epoch successor 也按完整 raw
`uint64` 域递增：`0x7fff... -> 0x8000...` 合法，只有全 1 值耗尽时
fail closed；这与 `OwnerIdentityV1`、Store runtime metadata 和 inflight key
的 unsigned fencing 语义一致，不把 Java signed overflow 当作协议边界。
证据为 `OwnerLeaseTest.ownerEpochSuccessorUsesTheCompleteUnsignedDomain`；
lease 的 expiry checked-add 也在 epoch 写入之前执行，时间溢出不会消耗下一次
接管的 epoch，证据为 `OwnerLeaseTest.overflowingAcquireExpiryDoesNotConsumeOwnerEpoch`；
候选 lease 的 owner/context 校验同样先于 epoch/lease map 更新，非法身份输入不会
消耗 fencing epoch，证据为 `OwnerLeaseTest.invalidAcquireValueDoesNotConsumeOwnerEpoch`；
真实 Oxia sequence allocation 仍由外部 authority 负责。
Activation 的本地 Oxia adapter 还会在 CAS response loss 后仅接受同一
fencing/assignment/session identity 的 exact `ACTIVE_FOR_COMMANDS` 重读；
`transitionOrRead` 在 lifecycle graph 禁止的请求上不会执行重读，因此
非法 transition 不会被 coincidental current state 掩盖。
Response-loss reread 还拒绝同 identity 但 expiry 变短的 successor；本地证据为
`OxiaOwnerLeaseStoreTest.transitionOrReadRejectsAResponseLossSuccessorWithShorterExpiry`。
Assignment/session-bound acquisition 的 interface default 现在也 fail closed；它
不再把未实现 context CAS 的 backend 降级成 shard-only live lease，避免在发现
上下文丢失前先占用一个无法用于 V1 activation 的租约。本地证据为
`OwnerLeaseTest.shardOnlyOwnerLeaseStoreCannotFallbackForContextBoundAssignment`
和 `OxiaOwnerLeaseStoreTest.backendWithoutContextBoundAcquireCannotAllocateAShardOnlyLease`。
Oxia adapter 还要求 acquire 成功值保持 `ACQUIRING`；后端若直接返回
`RESTORING`/`ACTIVE_FOR_COMMANDS` 会被拒绝，避免跳过 Worker 可验证的 lifecycle
CAS，证据为 `OxiaOwnerLeaseStoreTest.rejectsBackendAcquireResultThatSkipsAcquiringState`。
V1 assignment acceptance also rejects a non-null compatibility lease context
with assignment epoch `0`; only a positive exact assignment epoch may bind the
catch-up window。证据为 `OwnerLeaseTest.legacyZeroEpochLeaseContextCannotAuthorizeV1Assignment`。
Activation of `OwnedDelayShard` now leaves the local lifecycle in
`CATCHING_UP` while the authority performs the `ACTIVE_FOR_COMMANDS` CAS; the
local gate opens only after the exact successor is validated. The regression is
`OwnerLeaseTest.authorityGatedActivationKeepsLocalGateClosedDuringLeaseCas`。
`OwnedDelayShard.beginDrain(OxiaOwnerLeaseStore, nowEpochMs)` 现在对
`ACTIVE_FOR_COMMANDS -> DRAINING` 使用同一 exact-successor CAS 规则；response
loss 只有在 owner/epoch/token/assignment/session 完全一致且在观测时刻仍有效的
successor 被重读时才算成功，否则本地视图转为 `FENCED`。
`OwnerLeaseTest.authorityGatedDrainRequiresTheExactLeaseSuccessor` 与
`OwnerLeaseTest.authorityGatedDrainFailsClosedWhenLeaseIsExpired` 覆盖该本地边界。
`DelayShard.revokeClaimsForOwner` 现在还提供 bounded 的 local `CLAIMED`
rollback：在单写锁内按 exact Owner Epoch 扫描并逐 Claim 原子恢复
timeline/Message/READY；Claim v2 还保留 `TimelineWorkRef`，因此
`UNCERTAIN_RETRY` 的 ControlRef/Source Position authority 不会在 revoke 时丢失。
超 bound fail closed，重复执行返回零；证据为
`DelayShardTest.localClaimIsDurableAndRevokeRestoresTimelineAtomically` 及
`DelayShardTest.sourceOrderedNotPublishedEvidenceRevokesClaimWhenAnotherUncertainObligationRemains`。
它只
关闭新的 command admission 并撤销可逆 Claim；in-flight publish quiescence、final
checkpoint 和 lease release 仍是生产 drain gate。
Close marker 现在额外把未 admitted 的 message/reservation quota 在同一个
WriteBatch 一次性转移，并写入已注册的 `timeline/SYSTEM` kind-2
`LaneCloseMaterializationCursor`。`materializeClosedLane` 按 canonical `id_cf`
key 顺序分 message/reservation bounded batch，重启从 cursor 继续；只有空
admitted-obligation set 的 generation 才会物化为
`LANE_CLOSED_BEFORE_ADMISSION`，`PUBLISHING`/`UNCERTAIN` 保留。证据为
`LaneCloseMaterializationCursorTest` 与
`DelayShardTest.closeTransfersUnadmittedQuotaAndResumesBoundedMaterializationCursor`。
`DelayShard.discoverLaneCloseMaterialization` 还严格校验
`timeline/SYSTEM(kind=2)` 的 key/value/Lane identity，`LaneCloseMaterializer`
提供不作新语义选择的 bounded local turn；这让本地调度器可以从持久 cursor
继续执行而不会把它混入 due-publish scan。它仍不证明 close-owned Claim 标记、
admitted outcome retirement、对象句柄 quiescence/GC、Recovery-Floor retention
或 owner/Oxia 负责的生产 materializer 编排已经闭合。
单个 close-materialization result 的 message/reservation 计数和一个 bounded
turn 的跨 Lane 聚合现在都使用 checked addition；`LaneCloseMaterializerTest`
覆盖 `int` wrap 会被拒绝，而不是伪造未超界的物化结果。
直接的 Lane 读取也会校验 `meta_cf/LANE` 值内 Lane id，以及 close cursor 的
Lane id/incarnation/control version/source shard；错挂的管理投影在暴露给调度或
物化器前 fail closed，回归证据为 `DelayShardTest` 的 key/value identity tests。
`id_cf/MESSAGE` 的直接读取及 activation/Close/retirement bounded scans 还会
校验 self-routing key 的 Shard 与值内 `scheduleSourcePosition` 的 Shard；跨
Shard 错挂的当前 Message 不会被当作本地工作，证据为
`DelayShardTest.messageLookupRejectsForeignSourcePosition`。
Command result、terminal history、reservation、open attempt、DLQ export 与
GC projection 的 direct reads 也做相同的 Source Position shard 检查；带有
message/command locator 的值同时检查其 self-routing Shard。跨 Shard 的历史
不能成为本地 query、drain 或 compaction 输入。
Lane 退休的最后一轮 inflight 扫描也复用 Claim/attempt 的 key/value 与 Source
Position 校验，错挂的 ledger 会直接 fence 退休，而不会只被当作普通 pending
work；`DelayShardTest.laneRetirementRejectsInflightKeyValueMismatchBeforeRetiring`
覆盖这一边界。
`PersistentLaneScheduler` 的 READY 恢复还会在重算 timeline key 前检查 READY
消息的 self-routing Shard 与 `scheduleSourcePosition` Shard，避免 scheduler-only
recovery 把跨 Shard 的 READY head 放入本地公平 ring；证据为
`LaneSchedulerTest.fencedRecoveryRejectsReadyMessageFromAnotherShard`。
稳态 `discoverReady` 还会把物理的、带 laneVersion 的 READY key 与 work item
一起记入进程内 discovered-head fence；因此相同 message/generation 只有在
work identity 和 READY key 都未改变时才会被抑制，合法的 Claim/READY
transition 即使保留相同 work item 也会重新进入发现队列，证据为
`LaneSchedulerTest.readyTransitionWithSameWorkUsesNewReadyKey`。
发现预算也没有“第一条 oversized READY”例外：serialized key/value
projection 超过本次 visit 的 byte cap 会直接 fail closed，证据为
`LaneSchedulerTest.readyDiscoveryRejectsFirstEntryThatExceedsByteBudget`；
这与激活时必须证明最大 admitted record 能同时适配所有 scheduler cap 的
V1/ADR 约束一致。
同一 discovery turn 的 elapsed cap 也在首条 projection decode 前检查；已耗尽
的 turn 不会因为尚无已收集 projection 而放行一条记录，证据为
`LaneSchedulerTest.readyDiscoveryStopsBeforeFirstEntryWhenTimeBudgetIsElapsed`。
内部 `dedupe_cf/COMMAND` replay lookup 也检查 command key 的 Shard 与结果的
Source Position；Claim lookup/scan 则检查其 `DelayMessageId` 的 self-routing
Shard，避免跨 Shard 的旧去重结果或 Claim 进入 source replay、owner drain 或
admission；证据为 `DelayShardTest.commandDedupeLookupRejectsForeignSourcePosition`
与 `DelayShardTest.claimLookupRejectsForeignMessageShard`。
SLO outbox 的 direct `get(sampleId)` 也与 bounded scan 使用同一
`meta_cf/SLO_OUTBOX` key/value sample-id fence，错挂的 Start 不会进入 Final
merge；`SloObservationOutboxStoreTest.scanRejectsKeyValueSampleIdentityMismatch`
覆盖 direct 与 scan 两条读取路径。

SLO Final 还按 Registry 的 success-event 分支绑定 endpoint kind：
`COMMAND_QUEUED_LATENCY` 与 `NATIVE_HANDOFF_ACK_LAG` 的 `SUCCESS` 必须来自
`BROKER_PERSISTENCE`，其它内部 WAL/barrier/probe 成功事件必须来自
`TRUSTED_OBSERVATION`；`SEMANTIC_FIXED_EPOCH` 只能作为 Start 端点，不能被
重复当成完成时间。`SloObservationOutboxV1Test` 覆盖 semantic-start-as-success
拒绝。该校验只关闭本地 endpoint-kind 混淆，真实 Broker receipt、Admission
WAL/evidence authority 与生产 Final 重建仍是 release blocker。

Close-materialization discovery 也会在返回 scheduler work 前重验 cursor 的
embedded close Source Position Shard，与 direct cursor query 保持同一边界；
`DelayShardTest.laneCloseMaterializationDiscoveryRejectsForeignSourcePosition`
覆盖该 scheduler-only 路径。
`discoverDue` 与 `discoverExpiry` 现在还会把每条 timeline projection 与当前
`id_cf/MESSAGE` 逐字段互证：status、generation、Lane、expiry 以及 exact
derived key 必须一致。orphan、terminal、旧 generation 或错挂的 DUE/EXPIRY
不会变成 publish/expiry work，而是直接 fail closed；Close 物化前仍合法的
`SCHEDULED`/`CLAIMED` generation 继续可发现。证据为
`DelayShardTest.timelineDiscoveryRejectsOrphanDueAndExpiryEntries`。
`rebuildReadyIndexes` 在恢复每个 Lane 的 READY head 时也要求 candidate 的
timeline key 是当前 `MESSAGE` 的 exact derived key；仅有当前 message 而时间、
source token 或 generation key 被篡改，不能使 READY 恢复成功。证据为
`DelayShardTest.readyRebuildRejectsTimelineKeyThatDiffersFromCurrentMessage`。
Direct `discoverReady` 也会在返回 scheduler work 前确认对应 timeline entry
仍存在且 key/value identity 正确；READY projection 不能在 timeline 被删除后
继续作为独立指针。证据为 `DelayShardTest.readyDiscoveryRejectsMissingTimelineEntry`。
`id_cf/V1_SCHEDULE_BINDING` 的 direct lookup 也会在读取 sidecar 前校验
`DelayMessageId` 的 self-routing Shard；即使当前 DB 只有错挂的 sidecar，也不会
把它暴露给 Registry 路径。证据为
`DelayShardTest.scheduleBindingLookupRejectsForeignMessageShard`。
Message、Command-result、Terminal、DLQ-export 和 message-based Claim 的
route-keyed lookup 现在也在 RocksDB 读取前执行同一 self-routing fence；foreign
ID 即使在当前 DB 没有对应记录，也不会被降级为 not-found。证据为
`DelayShardTest.routeKeyLookupsRejectForeignMessageShardBeforeMissingRead`。
READY rebuild 的 candidate scan 现在按 pending bound 多读一个条目并在溢出时
fail closed，不会静默丢掉同一 Lane 的后续 timeline head；证据为
`DelayShardTest.readyRebuildRejectsTimelineCandidateScanOverflow`。
Retired Lane guard 的直接读取也校验其 terminal Source Position 属于当前
Shard；错挂的退休证明不会通过 `getLaneTerminalGuard` 暴露。
`ShardStore.flushAndSync` 还提供 drain 的物理 flush/WAL-sync 原语，重开回归为
`ShardStoreTest.flushAndSyncMakesTheShardBoundaryExplicit`；它不替代远端 callback
quiescence 或 final checkpoint publication。
Store close 会先持久化 clean-close marker，再 fence 所有 public Store 操作；随后
关闭默认 Column Family、命名 handles、DB/options，只有完整 native teardown 后才
释放 Worker slots。每一项 teardown 都独立记账，native close 失败不会把未完成项丢掉，
后续 close 只重试未完成项；因此 read、scan、write、flush 和 sequence-number API
全部 fail closed，同时不会把 `maxOpenShardDbs`/`maxOwnedShards` slot 永久吞掉，
也不会让 shared native resources 在仍有活跃 handle 时提前关闭。
`ShardStoreTest.closedShardStoreFailsClosedForAllRocksDbOperations` 覆盖该本地生命周期
边界；共享 rate limiter、WriteBufferManager 和 block cache 采用同一 retryable
teardown 规则。`EmbeddedDelayService.close()` 在 final drain 后保持 fenced-but-retryable，
不会因 Store/Worker 首次 teardown 失败而永久吞掉后续 close。
`DelayShard.listOpenPublishAttempts` 还提供 bounded 的
`PUBLISHING`/`UNCERTAIN` ledger view，供 drain 等待 admitted callback 的本地轮询
使用；重复 attempt identity 或超 bound 都 fail closed，不能把未知 obligation
当作已清空。
`OwnerDrainCoordinator` 将 source/scheduler stop、authority-gated `DRAINING`、
Claim revoke、bounded callback poll、lease/deadline reread、flush/sync、可选
final checkpoint、Store close 和 exact release 串成一个可重试的本地顺序；
它还在 `OwnedDelayShard` 上持有 shard-local drain-attempt gate，避免仅靠
Worker-wide drain semaphore 时两个 coordinator 并发关闭或释放同一 shard
DB/lease；失败后 gate 可释放并保留 `DRAINING` 供重试。回归证据为
`OwnerDrainCoordinatorTest.duplicateCoordinatorCannotDrainTheSameShardConcurrently`。
如果 caller 提供 final checkpoint 的 exact 16-byte identity，coordinator 会把
它传入 `ShardStore.createCheckpoint`，让完整镜像携带对应 `lastCheckpointId`；
`flushAndSync` 后的可选 `commitSourceHint` 只收到最后已持久化的
`SourcePosition`，callback 返回后还会重新检查 draining lease；该 hint 仍不是
recovery authority。物理 final checkpoint 安装完成后也会再次检查 lease，只有
这条检查通过才会关闭 Store 和执行 exact release，从而把 checkpoint 期间的
lease 丢失转换为本地 fence，而不会让旧 owner 继续操作新 owner 的状态；
`OwnerDrainCoordinatorTest` 覆盖该边界。
如果 Store close 本身失败，local shard 保持 `DRAINING` 并保留
authoritative lease；后续 drain 只重试 Store native/slot teardown，不重复
Claim revoke、callback poll、flush 或 checkpoint，也不会在 DB 未确认关闭时
把 local state 置为 `FENCED` 而丢失重试入口。Store 已关闭但 exact lease release
响应未确认时也必须保持同一可重试的 `DRAINING` 状态，不能由 close-success
的 finally 路径提前改成 `FENCED`；后续 drain 只重试 lease release。只有 Store
完整关闭并确认 exact lease release 后才进入 `FENCED`。`OwnerDrainCoordinatorTest`
`storeCloseFailureLeavesDrainingStateForRetryableTeardown` 与
`unconfirmedLeaseReleaseKeepsClosedDrainRetryable` 覆盖这两个边界。
callback/source quiescence 仍由调用方和真实 transport 提供，超时保持
`DRAINING` 而不伪造成功。`OwnerDrainCoordinatorTest` 覆盖成功与 deadline
失败边界。
本地 Worker envelope 还会在创建 JNI 资源前检查显式 shared block cache 与
WriteBufferManager 预算之和不超过认证的 RocksDB native 桶；
`NativeResourceUsage`/`WorkerNativeResourceLedger` 将 block cache、memtable、
table-reader metadata、pinned blocks/iterators、flush/compaction scratch
保持互斥，并以 exact allocation identity 做 checked reserve/release；聚焦
回归覆盖拒绝、重复 identity 和释放路径。`WorkerRuntimeResourceProbe` 现在从 JVM、procfs、cgroup v1/v2
和 rootPath 对应的精确 FileStore 读取有限 runtime observation；`max`、缺失
或 malformed limit 均 fail closed，`WorkerResourceEnvelope.validate` 再逐项
检查 heap/direct/RSS/cgroup/FD/filesystem 交叉边界。`WorkerRuntimeSafetyGate`
提供 fresh observation 的 sticky drain/migrate 状态和 explicit empty-drain
activation；`WorkerRuntimeResourceMonitor` 将固定间隔 probe 接入同一 gate，
probe 异常或 envelope mismatch 都会进入 drain/migrate，并可显式关闭调度器；
`WorkerRuntimeResourceProbeTest`、`WorkerRuntimeSafetyGateTest` 与
`WorkerRuntimeResourceMonitorTest` 覆盖解析、envelope rejection、周期探针
生命周期和共享资源 ownership fencing；`WorkClassSchedulerTest` 覆盖七个
work-class 的 bounded queue/turn caps、lease/fence 抢占和 stale-class 选择。
`WorkClassResourcePoolTest` 还覆盖 non-borrowable minimum、borrowed hold
bound 和 acquisition overflow rejection。每 DB 的动态 RocksDB attribution、chunk-level token reacquisition 和
write-time reserve admission 仍是 release gate。
Lease validity additionally rejects negative observation times even when a
caller reaches `OwnerLease.validAt` directly rather than through an authority
request; `OwnerLeaseTest.negativeClockCannotMakeOwnerLeaseValid` covers the
fail-closed local gate.
Kafka source records now reject an unexpected Pulsar connection proof instead
of silently ignoring it.

Kafka Source Position ordering now uses the physical partition offset only;
leader epoch and append time remain authenticated metadata rather than a
second order dimension.  A replay that reuses the same offset/ledger-entry-
batch token with different canonical metadata is rejected, so one physical
record cannot be interpreted as a later Shard Log position or silently reuse a
Command result.
The shared `SourcePositionCodec` also requires decoded bytes to round-trip
exactly; malformed UTF-8 or a replacement-character variant cannot enter a
persisted position field. Kafka/Pulsar Source Position constructors apply the
same nonblank, NFC and UTF-8 check before producing identity bytes, so callers
cannot bypass the canonical boundary by constructing a position in memory;
`ProtocolCodecTest.sourcePositionsRejectNonCanonicalTextAtConstruction` covers
that direct-construction path. Decoder length prefixes and fixed-width fields
also fail with the closed validation error instead of leaking a buffer-underflow
or arithmetic exception; `ProtocolCodecTest.sourcePositionDecoderRejectsTruncatedLengthAndFixedFields`
covers the malformed-byte path.
The same strict UTF-8 round-trip fence now applies to direct construction of
Kafka/Pulsar broker resource identities, Pulsar `EvidenceCursorV1` physical
topics, and both managed queued-receipt `SafeBrokerAck` branches. These values
also become canonical identity bytes, so accepting an unpaired surrogate and
letting the JDK silently encode it as U+FFFD would otherwise make an in-memory
identity differ from its wire identity; `ProtocolCodecTest`
`brokerEvidenceAndQueuedAckIdentitiesRejectNonCanonicalUtf8AtConstruction`
covers all of those constructor paths.
The same exact-position check is applied to the owner catch-up cursor before
activation, not only to the subsequent Command/System Mutation WriteBatch.
An empty Pulsar activation barrier still validates a non-null persisted cursor's
resource incarnation and physical topic before declaring the barrier reached;
an old DB from another Pulsar resource therefore cannot bypass source identity
validation merely because no replay record is required.
The canonical empty Pulsar barrier also requires the guarded source connection
generation and resource-guard attestation digest as a pair; an unguarded empty
Pulsar barrier cannot enter a Ready Certificate.
The embedded queued-receipt query and applied-receipt projection apply the same
canonical-position fence at a reached barrier: a same Kafka offset or Pulsar
ledger/entry/batch token with different metadata is an integrity failure rather
than a successful query or applied frame.
`CommandQueuedReceiptV1` additionally binds the `PreparedCommandRef` shard to
the Source Position shard in the shared constructor used by both create and
decode, so a command-from-A/source-from-B receipt is rejected before barrier
evaluation; `ProtocolCodecTest.commandQueuedReceiptRejectsACommandAndSourceFromDifferentShards`
covers this self-routing identity fence.

本地 `RecoveryCatalog.publishUploadedCheckpoint` 现在要求 PUBLISHED intent
与完整 manifest 的 shard、lineage、checkpoint、manifest hash/length、owner
和 store incarnation 完全一致后才接受 catalog projection；同一 checkpoint
的 exact manifest 在 response-loss 重试中会作为幂等 reread 返回，即使 catalog
generation 已被其它操作推进；同 ID 不同 manifest hash 或 Object Store
container/key/version/profile 仍 fail closed。这仍不等于
Object Store 真实性或 Oxia transaction；Oxia validation adapter 同样允许
generation 相等的 exact reread，但拒绝 generation 回退。它现在在调用外部
upload-intent CAS 前复用同一组 state、base generation、对象身份、owner、store
incarnation 和 parent identity 校验；非法请求不会先触达 Oxia。Oxia 返回的
candidate、Floor 和 ancestry manifest 还必须与已发布值的完整 canonical JSON
字节投影一致，不能只依赖部分字段比较；checkpoint ID、evidence digest、
typed cursor 和 coverage positions 在调用 backend 前也会复制，校验快照与
backend 请求缓冲区彼此隔离，避免可变调用方或 adapter 在 CAS 期间改变请求内容。
恢复候选入口也会先 reread 同 checkpoint 的已发布 manifest 并做完整 canonical
投影校验，再调用 backend 的 floor/recovery-set 验证；同 ID 漂移候选不会把远端
验证当作第一次发现错误的边界。

Legacy/typed local Recovery Floor CAS 也支持 exact successor reread（含
checkpoint、manifest、source/mutation 和 evidence/cursor identity），response
loss 不会重复推进 Floor；不同 Floor 或 identity drift 仍 fail closed。
Floor coverage 与本地 GC guard 在 order token 相等时还要求 Source Position
canonical bytes 完全一致；同一 Kafka offset 或 Pulsar ledger/entry/batch 的
metadata 变体不能被当作已覆盖的 retention boundary。Oxia coverage response
还必须逐项对应已发布 manifest、无重复 checkpoint，并沿着 parent id/hash、
lineage generation、source position、mutation sequence 和 evidence cursor
逐边验证到 candidate；跳过中间 parent 或伪造 ancestry 会 fail closed。

Checkpoint GC 的 catalog-backed guard 现在会在 source/sequence/ancestry
证明之后 reread 当前 `RecoveryPinV1`。活动 pin 若保护待删 checkpoint 的
candidate 或 observed Floor，返回 `RECOVERY_PIN_PROTECTS_RESOURCE`；pin
读取失败返回 `RECOVERY_PIN_STATE_UNAVAILABLE`，两者都禁止本地 tombstone
compact。这只闭合了本地 pin-aware necessary condition，仍不等于 Oxia
session CAS、provider delete attestation 或完整的 external GC orchestration。
当前 Floor 读取或 ancestry coverage proof 任一 authority 异常也会返回
`FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING`，不会把异常当作无保护而删除 tombstone。

`DelayShard` 的本地 `gc_cf/TASK` lookup 还会把 requested resource kind、identity
hash 和 expected version 与嵌入的 retire intent 逐项比对；delete confirmation 的
nested intent 也必须匹配同一 key。错挂的 GC value 会在 query/compaction 前 fail closed，
回归证据为 `DelayShardTest.gcRetireIntentLookupRejectsKeyValueIdentityMismatch`。
`ResourceGcGuard` 对 nested intent 进一步执行 canonical record-byte equality，
因此 protection set、applied mutation sequence 或 applied Source Position 的漂移
也会返回 `INTENT_REFERENCE_MISMATCH`，不能仅凭 mutation/resource/version 子集
获得 tombstone compaction 资格。

普通 local catalog publish 对已存在的 exact manifest 也先做 identity reread，
因此 catalog generation 推进不会把一次已成功的 checkpoint insert 误报为冲突。

`ShardStore.createCheckpoint` 现在先把完整 RocksDB 镜像写入同文件系统的
`checkpoint-tmp` 命名空间，完成后才通过 atomic rename 安装到目标路径；已有
目标会被拒绝；如果目标已经移动但后续目录 durability 校验失败，失败路径
也会删除该自有目标并回滚本地 projection，失败 staging 会清理。这闭合的是本地物理 checkpoint 边界，
不代表 Object Store 上传、manifest publication 或 Oxia CAS 已完成。
`checkpoint-tmp` parent/staging directory 现在必须是 `NOFOLLOW_LINKS` 下的真实目录，
而 `ACTIVE.tmp` 在写入前拒绝符号链接和非 regular file；因此临时路径不能把
checkpoint 或 ACTIVE pointer 写到外部目标。`ShardStoreTest.checkpointAndActivePointerTemporaryPathsRejectSymbolicLinks`
验证了 staging symlink fail-closed 以及 pointer target bytes 保持不变。
带 manifest 的 restore 在 staged DB 打开后还会逐项比较镜像中的
`lastCheckpointId`、`appliedShardLogPosition`、`shardMutationSequence` 和
typed evidence-cursor projection；文件 checksum 正确但运行时状态与 manifest
不一致时仍会在 install 前 fail closed。这样 manifest 的物理文件边界与其
恢复状态描述保持一致，而不把 source replay 或外部 catalog authority 假定为
本地 RocksDB 校验已经完成。
`ShardStore.openAtPathWithSlot` 也把 RocksDB 成功打开后的 metadata decode、
format/identity validation 和 install-mode write 放在同一个失败清理边界内；
任一失败都会先关闭 DB、Column Family handles 和 options，再释放 Worker slot。
`ShardStoreTest.malformedExistingMetadataDoesNotLeaveRocksDbOpen` 随后用 raw
RocksDB reopen 证明不会遗留 native 文件锁。失败 cleanup 现在会逐项尝试
所有 Column Family handle、DB/options，并把 close runtime failure 作为
suppressed diagnostic 保留，而不是在第一项失败时短路。
固定的本地 `shards/<routeIncarnation>/<partition>` 祖先目录也不再通过
`Files.createDirectories` 隐式跟随符号链接：open/restore 会逐级创建并用
`NOFOLLOW_LINKS` 验证 `shards`、route 和 partition 目录，任一 ownership-boundary
组件为符号链接都会在 RocksDB 创建或 restore staging 前 fail closed。这样
一 shard 一 DB 的物理目录不会被重定向到配置 root 之外；回归证据为
`ShardStoreTest.openRejectsSymbolicShardPathAncestors`。
Restore 在把 staged DB 原子移动到新 incarnation 后，会先以正式 active path
打开并验证 DB，再写入 `ACTIVE`；任一 pointer 安装失败都会关闭已打开句柄，
在关闭成功时删除未被 `ACTIVE` 指向的自有 incarnation 目录；关闭失败会有界
重试并保留无法证明已关闭的 orphan 目录供离线修复，同时保留原始 pointer I/O
错误。`ShardStoreTest.failedActivePointerInstallRemovesUnpublishedDb` 覆盖
`ACTIVE.tmp` 故障路径。
在这次 pointer 切换之前，restore 还会对新 incarnation 的父目录执行
directory fsync；因此 staged DB 的 atomic rename 已先取得目录级持久性，crash
不会出现 `ACTIVE` 已发布但对应 incarnation 目录项仍只存在于 page cache 的窗口。
restore 的 checkpoint copy 还会先 fsync 每个拷贝文件，再按子目录到根目录
fsync staged tree；因此该窗口同时覆盖 DB 文件内容和目录项，而不只覆盖 rename。
Normal `ShardStore.open` 也在发布 `ACTIVE` 前 fsync DB 目录及同一 Store
Incarnation 父目录，包括接管没有 `ACTIVE` 指针的 orphan incarnation；fresh-
open 与 restore 因此共享同一 pointer-before-directory-durability 顺序，并覆盖
open 阶段新建的 WAL/MANIFEST 目录项。
Restore admission 只把 checksum-validated `ACTIVE` pointer 指向的
incarnation 视为 live DB；pointer 尚未切换时留下的 orphan incarnation 不会
阻塞新的 atomic restore，且不会被悄悄当作 active 覆盖。
如果 `ACTIVE` 本身存在但指向缺失或非目录 DB，restore 现在会把它视为
store-integrity failure 并 fail closed，不会把损坏指针伪装成“无 active DB”后
覆盖；只有不存在 `ACTIVE` 指针时才允许安装新的 incarnation。回归证据为
`ShardStoreTest.restoreRejectsAnActivePointerWhoseDbIsMissing`。
Normal `ShardStore.open` 也对 `ACTIVE`、incarnation、DB 目录和 `CURRENT`
使用 `NOFOLLOW_LINKS` 并拒绝符号链接，因此 open 与 restore 对
live-incarnation pointer 使用同一 fail-closed 边界；restore 也不会把
有效 `ACTIVE` 指向的符号链接 incarnation/DB 当作 live DB。
已有 RocksDB 如果缺少 `meta_cf` 的 shard-identity marker 也不再被当成 fresh
DB 初始化；只有没有 `CURRENT` 的真正新目录才允许写入初始 metadata，已有目录
缺 marker 会在 activation 前 fail closed，`ShardStoreTest` 覆盖该重开路径。
正常 incarnation 目录还会把路径 UUID 与 metadata 的 `storeIncarnation` 做交叉
校验，并拒绝全零 Store/DB identity；restore-tmp 在 install-mode 完成新 identity
写入前不套用这条路径检查，安装到 `incarnations/` 后再由正常 open 验证。
staged open/metadata validation 的 runtime failure 也会清理 private
`restore-tmp`，而 download-slot 尚未取得时仍保留原始 bounded-concurrency
错误。Restore 的 staged validation、install-mode probe 和正式 installed open
现在由显式 Store 生命周期管理；失败清理会有界重试可重试的 native/slot
teardown，只有在 staged/prepared/installed Store 都确认完成关闭后才删除
`restore-tmp` 或未发布 incarnation。若仍有 Store 无法证明已关闭，相关目录
会保留供离线修复，避免把 native handle 仍在使用的目录删除。

`meta/FIXED` 的 immutable key 1/2 也已与 Registry §7 的物理约束对齐：
store format 的 payload 是 canonical u32 `1`，shard/Store identity 的
payload 是 canonical `StoreMetadata`，两者都通过 fixed-key ValueEnvelope
写入并在 open、restore staging 和 install-mode 重读时执行 type/version/
length/CRC 校验；不再存在裸 format 或裸 identity value。回归证据为
`ShardStoreTest.fixedFormatAndIdentityValuesUseRegisteredValueEnvelope`。
`ValueEnvelope` 的 numeric discriminator 也已在本地 codec 中限制为 Registry
注册的 1--11；§7 明确记录了 context-specific mapping，而不是把 GC 的
retire/delete union 或 fixed control states 错误地压成同一个 payload schema。
`ValueEnvelopeTest` 覆盖上界、未知 type、长度和 CRC 的 fail-closed 行为。
`ShardStore.open` 现在也在 activation 边界校验固定 key 3 的 Source Position
属于当前 Shard、key 5/11 的 non-negative fixed-width sequence，以及 key
12/13 的 registered non-empty control-state envelope；错误不会等到
`DelayShard` 构造后才暴露。`ShardStoreTest.fixedControlMetadataIsValidatedBeforeShardActivation`
覆盖 type mismatch 和失败后 native DB 可重开的证据。
`DelayShard` 构造时还会逐 marker 校验 key 12/13 内 Profile/Trust-Set
source-ordered history 的 Shard，并再次确认 key 3 的 applied Position；错挂的
历史不会进入 compare/replay 运行时，证据为
`DelayShardTest.activationRejectsForeignAppliedSourcePosition` 与
`DelayShardTest.activationRejectsForeignSourcePositionInProfileControlState`。

主设计 §10.1 要求的可变 Store 元数据现在也有独立的本地投影：
`StoreRuntimeMetadata` 在注册的 `meta/FIXED` key 4/6/7/8/9 中 canonical 持久
`lastIngressFenceProofId`、typed `evidenceCursors`、`lastCheckpointId`、单调的
`lastOpenedOwnerEpoch` 和 `cleanCloseMarker`；key 4 的单一
`IngressFenceState` 同时承载 close deadline 与 proof identity，避免 DelayShard
与 Store projection 争用同一 fixed key；不再把这些字段打包写入已经保留给
compatible control snapshot 的 key 10。打开时逐项严格解码并清除 clean marker，
正常 close 通过同步 WriteBatch 写回 marker；fence/checkpoint/owner/evidence 更新也
沿用同一 WAL-sync 边界。`StoreRuntimeMetadataTest` 和
`ShardStoreTest.malformedRuntimeMetadataDoesNotLeaveRocksDbOpen` 覆盖注册 key、
codec、生命周期与失败清理。该投影只证明本地 Store 事实，不能替代 Oxia
lease/catalog 或真实 Broker fence authority。`TIME_FENCE` 的 verified proof ID
现在与 mutation result/source position 在同一 batch 原子落盘，重开回归也验证该
proof identity。
`meta/RECOVERY` 的四个注册 key 现在也有独立的本地投影：key 1 持久
`RecoveryCandidateRefV1` lineage/base，key 2 持久完整 typed `RecoveryFloorRefV1`，
key 3 持久与 Floor generation 绑定的 raw `uint64` catalog generation，key 4 持久
带 digest 的 `RecoveryInstallStateV1` install/open phase。Shard Store 在 open、
install-mode、normal close 与恢复候选安装时用 WAL-synchronised batch 更新这些值，
并拒绝 foreign-shard Floor、LOCAL_STORE candidate/DB identity 漂移和 install-state
Store Incarnation 漂移；restore 会为新 Store Incarnation 记录 LOCAL_STORE candidate
及 pin 观察到的 Floor。`StoreRecoveryMetadataTest`、
`RecoveryInstallStateV1Test` 和 `ShardStoreTest.catalogBoundRestoreRequiresPublishedFloorEligibleManifest`
覆盖 canonical round-trip、key/value projection、reopen 与 restore。该投影只闭合
本地 recovery-reuse facts；`hasReusableRecoveryProof()` 不证明 ancestry/Floor
coverage，也不替代 Oxia Owner Lease/session/catalog authority。
本地 `RecoveryCatalog.validateLocalStoreRecovery` 现在把这些 facts 与 exact typed
current Floor、published base manifest、parent-hash ancestry 以及 Store
Incarnation/install-state identity 绑定，拒绝 stale Floor、跨 shard 或跨 lineage
重用；它仍是只读 local proof seam，不是 Oxia transaction 或 Owner Lease/session
authority。
manifest restore 在 staged DB install 前还会把 `meta/RECOVERY` 的 lineage/base、
observed-Floor lineage 和 install-state checkpoint identity 与 checkpoint manifest
做 exact 比较，拒绝把一份合法 RocksDB 文件镜像与另一条 recovery projection 拼接；
`ShardStoreTest.restoreWithManifestRejectsRecoveryProjectionLineageDrift` 覆盖该
fail-closed 边界。它仍不等于真实 Object Store/Oxia catalog、Owner Lease/session
或 source replay activation 证据。
已有 DB 在任何 open-phase 重写前也会验证 install-state checkpoint 与
lineage/base 一致；没有 lineage/base 的 install state 不能带 checkpoint，避免
原始投影漂移被新的 `OPEN` marker 掩盖。`ShardStoreTest.recoveryInstallStateDriftDoesNotLeaveRocksDbOpen`
覆盖该失败清理边界。
带 identity 的 `ShardStore.createCheckpoint(path, checkpointId)` 会先把 exact
16-byte checkpoint identity 写入 live DB，再拍摄完整镜像；物理失败会同步恢复
旧 projection，恢复后的 DB 因而保留它所代表的 checkpoint identity。无 identity
的兼容性调用仍只证明本地物理镜像，不冒充 manifest/catalog publication。

`CheckpointUploadCoordinator` 现在在本地上传边界内先校验完整 checkpoint
inventory、intent deadline 和 shard/lineage/owner/store/parent identity，取得
Worker upload slot 后才调用 typed adapter；adapter 返回的 manifest object
length/SHA-256/profile/lineage/checkpoint identity 不匹配时保持 PENDING_UPLOAD，
只有校验通过才执行本地 PENDING_UPLOAD -> PUBLISHED CAS。这仍不是 provider
attestation、Object Store immutability 或 Oxia intent/catalog transaction；
同一 pending intent 在 response-loss 重试中会先精确重读已提交的
PUBLISHED successor，且不再次调用 adapter。
`CheckpointFileInventory` 与 restore 的 `copyTree` 现在都会拒绝符号链接及目录
之外的非 regular 文件，避免把 checkpoint 中未知的物理文件静默丢弃后仍继续
恢复；显式 `CheckpointManifestLimits` 还把文件数、单文件/总 bytes、路径和
manifest bytes、evidence cursor 数以及 file/manifest object identity 字段长度绑定到
inventory、canonical decode、upload 和 restore 边界，原始 manifest byte cap 在
JSON parse 前检查，file/evidence array bound 在 parser materialize 时检查，其余
超限在本地 hash/provider I/O 前 fail closed；inventory 与 manifest 的文件名排序
使用 normalized UTF-8 unsigned byte order，而不是 Java UTF-16 order；restore 的
`copyTree` 通过 streaming iterator 复制，不再把整棵路径树 materialize，inventory
也在任何 file hash 前拒绝非 canonical path；源目录通过 admission inventory 后，
restore-tmp 副本还会按同一 manifest 再做一次完整 inventory/checksum 校验，避免
复制截断或 copy-time source mutation 仅因 RocksDB 仍可打开而进入 install；
restore-tmp cleanup 使用
post-order `walkFileTree`，不再建立 sorted whole-tree list；inventory 和 manifest
limits 的 total-byte checked addition 溢出也会统一 fail closed，而不是泄漏
`ArithmeticException`。无 limits
overload 保留为 embedded compatibility seam，不能作为 production activated
limits；这仍只是本地文件完整性边界，不替代 Object Store 内容证明。

`SharedRocksDbResources` 现在也把 checkpoint create/upload slot 纳入进程级
关闭保护；后台 checkpoint 或上传操作持有 slot 时，资源 close 会 fail closed。
一旦关闭获准，rate limiter、shared WriteBufferManager 和 block cache 会逐项尝试
释放；某个 native close 抛出 runtime failure 时，后续资源仍会被尝试，异常以
suppressed 形式保留，避免进程级资源被首个失败短路。
长期 owned-shard slot 现在还绑定 exact `ShardId` identity，而不只是一个计数器。
同一 Worker 对同一 Shard 的第二次 `ShardStore.open` 会在 native DB open/create
之前 fail closed；这也封住了两个 no-`ACTIVE` 并发 open 各自选择新 incarnation、
最后覆盖 `ACTIVE` 而留下一个仍可写的非 active DB 的窗口。identity 只在对应 Store
close 时释放，`ShardStoreTest.duplicateOwnedShardOpenIsRejectedBeforeCreatingAnotherDb`
证明重复打开不产生第二个 `CURRENT`，释放后仍可正常 reopen。
Shard open/restore 的短生命周期 acquisition 阶段也有独立的
`maxConcurrentAcquiresPerWorker` slot；它在 native DB 打开或失败清理完成后立即
释放，不会把 acquisition 并发额度错误地当成长期 owned/DB capacity。已有
acquisition slot 被占用时，`ShardStore.open` 在创建 native handle 前 fail closed，
回归证据为 `ShardStoreTest.workerAcquireSlotIsReleasedAfterOpenAndFailsBeforeOpeningWhenHeld`。
同一进程的 restore/download staging 也有独立的 Worker 级 slot，并在
manifest/file 校验、临时目录复制、验证打开和 ACTIVE 安装完成后释放；真实
restore 回归会在返回的 DB 仍保持打开时重新取得该 slot，证明不会把恢复并发
额度错误地绑定到 DB 生命周期。`CheckpointScheduler` 则以确定性 shard
jitter、due claim 上限和 in-flight fence 提供错峰调度；completion 必须带回
`claimDue` 返回的 exact process-local 句柄，并以对象身份而非
`shardId + dueAt` 值相等进行校验，重建的 value-equal handle、shard-only
或迟到旧 claim 都 fail closed。它是 process-local 调度器，不冒充
checkpoint manifest、Upload Intent 或 Oxia catalog authority。
同一资源封套现在还提供 `maxConcurrentDrainsPerWorker` 的独立 drain slot，
争用和资源 close 保护由 `ShardStoreTest.drainSlotIsWorkerBoundedAndCloseProtected`
覆盖；这只证明进程级 drain 并发限额，不能替代 claim quiescence、final checkpoint
和 lease release 的生产编排。

`CheckpointScheduler` 的 jitter 百分比计算现在先除后乘，避免近
`Long.MAX_VALUE` 的合法 interval 在中间乘法处误报溢出；`CheckpointSchedulerTest.largeIntervalJitterUsesCheckedWideArithmetic`
覆盖可接受的最大 jitter span 和应拒绝的更大 span。该证据仍只属于本地
错峰调度器，不替代真实 Worker checkpoint/capacity/chaos 证据。

查询层也已补齐 `CheckpointSummaryV1`/`CheckpointCatalogResultV1` 的
canonical checkpoint-catalog projection，包含 shard identity、Floor identity
和严格排序的 summary array；它仍只是 public query value codec，不代表
durable control-operation query routing 或 Oxia catalog authority 已完成。
`CheckpointControlResultV1` 也已补齐 checkpoint-control typed result 的
shard/checkpoint/manifest/generation projection；其余 control result branches
现已补齐 Lane/Shard/Profile/Quota/Message/Route/Secret 的纯值 codecs 和
枚举/presence 校验，`ControlTypedResultV1` 也会按 branch 调用对应 codec，拒绝
tag/payload 漂移。本地 `ControlOperationAuthority` 现在把完整 receipt 作为
唯一 locator，覆盖幂等 register、严格 revision CAS 和固定 `queryUntil` 边界；
`PersistentControlOperationAuthority` 还把 exact receipt/current pair 以 checksum、
atomic rename 和 directory fsync 持久化，重启、同 bytes response-loss 重试以及
损坏/截断状态均 fail closed；`OxiaControlOperationAuthority` 对 backend 的 CURRENT
响应执行 operation/request/scope identity 与 revision 不回退校验。这闭合了本地
crash-durable CAS seam，durable control-operation query state 的生产 routing、
authorization、session ownership 和真实 Oxia authority 仍未完成。
`EmbeddedDelayService` 已将该 seam 暴露为本地 register/advance/query 入口，便于
conformance tests 验证 register 和 exact advance response-loss 后的精确 receipt
重读；`OxiaControlOperationAuthority.advance` 不接受更高或不同状态的后续
CURRENT 来冒充目标 revision 的成功。它不改变上述生产边界。
该 adapter 还在远端调用前校验 receipt identity、register revision 和
`expectedRevision + 1` 的连续性，避免把非法请求交给 authority；本地 authority
还以显式 checked-successor predicate 拒绝 `Long.MAX_VALUE` 的 expected revision，
不会依赖 Java `long` 回绕来产生拒绝（`ControlOperationAuthorityTest`
`revisionSuccessorFailsClosedBeforeLongWraparound`）。

嵌入式 Command query 现在还会在 barrier 之后把 receipt 的 `commandHash`
与 shard `dedupe_cf` 保留的命令身份核对；同一 `commandId` 但不同 hash 的
receipt 返回 `RECEIPT_MISMATCH`，applied-receipt 路径也拒绝该 locator。
`EmbeddedDelayServiceTest.embeddedQueryBindsReceiptCommandHashToDurableDedupeIdentity`
提供本地证据。该检查闭合了“不能只按 ID 暴露结果”的 shard 边界，但不等于
生产 Gateway 的租户授权、Oxia 路由或真实 Broker barrier。

同一物理 locator 还必须通过 `DelayShard.matchesCommandPosition` 读取精确
`dedupe_cf/POSITION` 审计并确认其命名 receipt 的 `commandId`；因此同 shard
的较早或伪造 Source Position 即使已经跨过 barrier，也不会借用另一条命令的
逻辑结果。`EmbeddedDelayServiceTest.embeddedQueryBindsReceiptToExactPhysicalPositionAudit`
覆盖 query 与 applied-receipt 两条路径；缺失或跨类型 POSITION 审计均 fail closed。

The local `DeliveryCapabilitySemanticV1` value codec now closes the Registry
baseline/strong outcome branches and Kafka/Pulsar evidence-resource and timing
compatibility checks. This is only semantic-value evidence; immutable Profile
publication/catalog resolution and authenticated Broker prerequisite authority
remain release blockers.
The four Profile semantic body codecs and `ProfileSemanticEnvelopeV1` now add
closed branch/kind checks, destination partition-policy validation, mandatory
Object Store safety flags, verifier validity bounds and the Registry
domain-separated semantic hash. They remain pure values; publication,
credential-binding protection and catalog/authority transactions are not
claimed complete.

Kafka offset and Pulsar ledger/entry Source Position fields now preserve the
complete unsigned-64 raw bit pattern through `SourcePositionCodec`, direct
constructors, activation barriers, evidence cursors, queued receipts, adapter
result values, canonical protobuf helpers and checkpoint-manifest JSON. The
position partition, leader epoch and Pulsar batch fields now preserve their
complete unsigned-32 raw bit patterns through the same local protocol paths;
checkpoint manifest JSON decoding also uses the unsigned parser for Pulsar
evidence-cursor `batchIndex`/`batchSize`, so `0x80000000` and `0xffffffff`
round-trip instead of being rejected by a signed parser. The boundary vector is
covered by `CheckpointManifestTest.manifestRoundTripsUnsignedSourceAndEvidencePositions`.
comparators and the strict Kafka successor use unsigned order. Boundary vectors
cover high-bit position fields, receipt/evidence round-trips and manifest
round-trips. The three `TrustedUtcIntervalEvidenceV1` uint64 counters and its
`sourceKeyVersion:uint32` also preserve their raw bit patterns through the
canonical codec and checkpoint `createdAt` JSON; high-bit signed-time evidence
is covered by `ProtocolCodecTest.trustedUtcEvidencePreservesUnsignedSourceKeyVersionBits`
and `CheckpointManifestTest.manifestRoundTripsUnsignedSourceAndEvidencePositions`.
Registry Pulsar `physicalTopicCreationTimestamp:u64be`
also preserves its raw bits through broker identity, queued ACK, evidence
cursor, managed/native adapter and checkpoint-manifest projections, with
high-bit vectors in the protocol, adapter and manifest suites. Other
auxiliary uint64/time fields, real Broker assignment/barrier proof and
production adapter authority remain release blockers, so the source-order
row below claims the implemented local u64/u32 position paths rather than full
production Source Position authority. Ownership/fencing `ownerEpoch` and
writer generations now preserve raw uint64 bit patterns through
`OwnerIdentityV1`, the OWNER/FENCE/SERVICE `AuthorIdentity` branches,
`ShardControlResultV1`, StoreRuntimeMetadata/inflight keys, Claim and
PublishAttempt records, and checkpoint `createdBy` JSON. High-bit vectors cover
the typed identities, runtime metadata, local inflight projections and
manifest JSON; this closes the local width/encoding path, not Oxia lease or
production ownership authority. The guarded Pulsar source-connection
generation is likewise preserved as a raw uint64 through the canonical
activation barrier, runtime barrier, replay-entry validation and
`OwnedDelayShard`; high-bit fence vectors cover both the wire and catch-up
paths.
`NativeCapabilitySnapshotV1` likewise preserves the complete nonzero raw
`resource_guard_config_generation` in its signed digest and canonical bytes;
the guarded Broker rollout attestation remains external evidence.

## Source locks

| 依赖 | 审计锁 |
|---|---|
| Kafka contract/patch source | `76f62f3b83e882105219b6c7687dbde594a8b8a2` |
| Pulsar contract/guard source | `50fc70fe4620febcf0fd31d97ff7d2be447af3d4` |

主设计 R12–R37 的 Kafka/Pulsar correctness-critical 链接全部使用上述 immutable commit。发布包还必须记录实际 patch/binary digest、Broker rollout attestation 和 delete/recreate cuts；仅有文档 source lock 不等于实现已通过。

## 机械审计结果

| 检查 | 结果 |
|---|---:|
| Markdown UTF-8、fence 配对、relative links | PASS（47 个 Markdown 输入） |
| 主配置 YAML parse + duplicate-key walk | PASS（1 个完整 config block） |
| Stable code numeric/symbol uniqueness | PASS（103） |
| non-default retryability sets | PASS（5 个集合，互斥且只引用 registered code） |
| `CapacityDimensionV1` | PASS（1–66 连续、完整） |
| ADR file/index sequence | PASS（0001–0042） |
| `*V1` cross-document references | PASS（336 个 unique refs 均进入 Registry） |
| RocksDB CF namespace | PASS（只出现七个 CF 的 registered tags） |
| `RETRY_JITTER_V1` independent recomputation | PASS（`dd78e75…d339`，first64 `15958759676622330853`） |
| stale placeholder/unfinished-decision markers | PASS（0） |
| Kafka/Pulsar correctness links use source lock | PASS |

## Release artifact matrix

这些是 implementation/release 的必交付物，不是允许延后决定的语义：

| Artifact | 固定输入 | 通过条件 |
|---|---|---|
| Generated IDL/descriptors | Registry field/enum/tag/version | 全语言 descriptor digest 一致，unknown/negative vectors fail closed |
| Golden vector bundle | ID、frame、canonical body/hash、key、cursor、manifest、signature、retry jitter | 每个 registered branch 有 positive/negative vector |
| Semantic catalogs | Stable errors、Profile/capability、Retry Policy、SLO、quota/capacity | catalog digest 绑定本 revision；无自由字符串扩展 |
| Benchmark config | §21 所有 `required` 数值 | §23.4 矩阵产出可复现 capacity envelope，而非单一 TPS |
| Capacity proof | dimensions 1–66、Control Reserve、RSS/cgroup、FD/disk/temp、Adapter zombie、DRR bound | checked sums 全部成立；无 double-count/oversell |
| Real-service conformance | Kafka/Pulsar/Oxia/Object Store 的 exact locked contract | §23.2 全通过，不能 stock/name fallback |
| Chaos evidence | §22 每一 failure cut + §23.3 target-isolation gates | deterministic cut、durable dump、external evidence、fresh-process recovery、invariant audit 齐全 |
| Binary/source attestation | locked commits、patches、guard/client binary digest、Broker coverage | 全 Broker rollout attested，delete/recreate 和 downgrade cuts 通过 |
| SLO evidence package | objective/load-envelope/outbox/collector schemas | timeout 不丢样本，HEALTHY 配同事件 ALL_ACCEPTED，merge 只保守变差 |
| Runbooks | restore、fence、checkpoint、DLQ replay、uncertain override、disaster boundary | 在 release candidate 上完成演练并留 evidence |
| Soak/upgrade report | 最长 retry/checkpoint/floor/GC 周期和 protocol rollout | 无 source gap、counter drift、unbounded resource 或 reader-before-writer |

`ShardQuota` 的单条 message/reservation add、remove 和 commit 入口现在先拒绝
负 bytes，再执行 checked arithmetic；`ShardQuotaTest.singleChargeOperationsRejectNegativeBytes`
覆盖了原本可能把扣费参数解释成加费的输入边界。这是 shard-local quota
完整性证据，仍不替代 Route Broker charge authority 或多 shard placement proof。

本地 `KafkaActivationBarrier` 现在在构造时复用 Source Position 的 canonical
UTF-8/NFC identity fence；无效 cluster identity 不会进入 assignment。`PulsarActivationBarrier` 现在把 non-empty inclusive cursor 的
`batchSize` 与最后 member index 一起固定；同一 ledger/entry 的 batch-shape 漂移在
`validatePosition/reachedBy` 即 fail closed，而不是把另一种 batch 形状判为已越过
barrier。兼容构造器仍明确不计入 V1 source-assignment 证据。

本地 `SloObservationOutboxStore` 已把 `meta_cf/SLO_OUTBOX` 的扫描边界收紧为
key/value `sampleId` 必须 byte-identical，并支持 record/ValueEnvelope byte
budget；首条超预算记录直接 fail closed，后续记录留给下一次 export turn。
错挂的 key 不会被导出为另一个样本，
而 collector acknowledgement 仍必须匹配当前完整 record digest 才能删除。回归
证据为 `SloObservationOutboxStoreTest`。`SloObservationOutboxV1` 现在还按 closed
objective branch 校验 Final 的 unit 与 merge direction，错误语义不会进入 durable
outbox；`SloObservationOutboxV1Test.rejectsFinalUnitAndMergeDirectionThatDisagreeWithObjective`
覆盖该边界；同一 severity 的重复 Final 还按较新 `observationRevision` 选择
evidence，避免 revision 与 evidence 混配。`SloObservationOutboxV1Test.mergeUsesNewestEvidenceWhenOutcomeSeverityIsEqual`
覆盖该边界；两个重复 Final 若携带不同的 non-null due exclusion reason
则 fail closed，而不是静默保留旧 reason，`SloObservationOutboxV1Test.mergeRejectsConflictingDueExclusionReasons`
覆盖 mutually-exclusive reason 的完整性边界。这只补足 shard-local 持久化完整性，不能
替代 SLO Start 重建、collector merge/export 或生产观测 authority。
持久化 store 的方向-only merge 入口现在还会拒绝带 exclusion 的新旧投影；
只有显式传入配对 HEALTHY objective 的 overload 才能写入 excluded ALL_ACCEPTED
Final，`SloObservationOutboxStoreTest.excludedFinalRequiresPairedHealthyObjectiveAtDurableBoundary`
覆盖该 catalog-pair 边界。

SLO outbox 还增加了显式的 `SloObservationOutboxLimits` 本地容量 envelope。带
limits 的 store 在写入新 Start 或替换 Final 前，按严格 key/value 解码统计当前
record 数和 `ValueEnvelope` 编码字节，使用 checked arithmetic 检查替换后的总占用，
并以 `Usage` 暴露同一 bounded projection；超出 record/byte 上限的写入和过大的
export scan 都 fail closed。回归证据为
`SloObservationOutboxStoreTest.configuredCapacityBoundsRecordsAndEncodedBytesBeforeWrite`。
这仍只是 shard-local capacity guard；超过 certified envelope 后如何 durable 记录
`BAD_EVIDENCE_GAP`、从 Message/Admission authority 重建缺失 Start、collector merge/
export 和生产观测 authority 仍是 release blocker。旧的无 limits 构造器只保留给嵌入式
兼容调用，生产 wiring 必须提供 §21 的 required envelope。
`SloObservationOutboxExportRate` 现在再为每个 export turn 施加 process-local
monotonic one-second record/encoded-byte budget；拒绝只停止本次 scan，不删除或
修改 durable Start/Final。回归证据为
`SloObservationOutboxStoreTest.exportRateBoundsEachScanWindowAndResetsAfterOneSecond`
和 `SloObservationOutboxExportRateTest`。该 rate state 在进程重启后不伪装成持久
collector authority，生产配额、merge/export 和 evidence-gap projection 仍须外部
证据闭合。
本地 `SloObservationCollector` 还把相同 `(sampleId,startDigest)` 的 at-least-once
记录合并为 deterministic projection：Start bytes 漂移 fail closed，Final 继续
使用 Registry 的 direction-aware conservative merge；配置
`SloObservationCollectorLimits` 后，sample 数和当前 canonical projection bytes
也有 checked 上限，超限不会修改已有 projection。`SloObservationCollectorTest`
覆盖 bad-evidence 单调性、Start drift、snapshot 排序以及 sample/byte capacity
rejection。它不替代生产 collector 的持久化、完整 merge/export history、授权和
metric publication。

`PersistentSloObservationCollector(Path)` 现在把该 projection 持久化为按 sample ID
排序的 canonical snapshot，并用 checksum、atomic rename、directory fsync 与
JVM/on-disk lock 保护重启、跨实例重读和 response-loss merge；
`PersistentSloObservationCollectorTest` 覆盖 conservative Final 重开、identity
failure rollback、capacity fence 与 checksum corruption。它不替代生产 collector
的 rolling-window/late-finalization retention、授权、完整 merge/export history 或
metric publication。

`SloObservationOutboxStore.reconcileDurableStarts(...)` 现在补足了本地恢复物化的
边界：它只接受调用方已经从 Message/Admission/Lane/Recovery authority 得到的 exact
`SloSampleStartV1`，按 sample ID 确定性排序并折叠 byte-identical 重复，冲突 Start
在写入前 fail closed，容量预检通过后以一个同步 batch 写入全部缺失 Start，并保留
已有 Final。`SloObservationOutboxStoreTest` 覆盖排序、重试保留 Final、冲突回滚和
容量回滚。该 seam 仍不等于 source-ordered Start reconstruction、接管时的 authority
认证或 evidence-gap `BAD_EVIDENCE_GAP` 记录；这些仍是 production release gate。

同一 store 现在还提供 `reconcileDurableStartsInBatch(...)`：source-apply caller 可以把
完整 authority-derived Start 集追加到自己的 `ShardStore.Batch`，在同一同步 WriteBatch
提交业务 projection 与 SLO denominator。该入口在追加前完成冲突/容量预检，并明确要求
一个 apply batch 只调用一次，且 batch identity 必须属于同一个 ShardStore；
`SloObservationOutboxStoreTest.reconcileInCallerBatchSharesBusinessCommitAndRollsBackTogether`
与 `reconcileInCallerBatchRejectsABatchFromAnotherShardStore` 证明 caller batch 的 joint
commit/joint abort 与 cross-store rejection。它关闭的是本地 WriteBatch 窗口，不是
生产 source-order authority、Message/Admission reconstruction 或 evidence-gap 计数。

`SloAuthoritativeStartFactory` 进一步把两条 Shard-derived reconstruction 输入固定为
typed fields：`commandApplied(...)` 使用 Registry `SourcePositionV1` canonical bytes、
Broker persistence time 和对应 SHA-256；`dueAdmission(...)` 强制完整 unsigned-32
generation、managed path、ordinary `deliverAt`/handoff `actionAt` 及调用方提供的
semantic evidence digest；store convenience entry 还验证这些 identity 属于当前 Shard。
两条路径都重新计算 sample ID、Start digest 和 checked timeout；
`SloAuthoritativeStartFactoryTest` 覆盖 Source Position identity、high-bit generation、
managed-path/time mismatch 与非法输入，store convenience test 覆盖幂等物化和 foreign
Shard 拒绝。此证据关闭的是本地 typed projection，不是生产 Message/Admission authority、
source-ordered recovery 编排或 evidence-gap 计数。

`DelayShard` 现在把 `COMMAND_APPLIED_LATENCY` 的本地 Start 物化接到真实 client
Command apply：注入外部 immutable objective 后，正常结果、stable rejection、命令冲突、
position-only dedupe 和 retry-window fence 都在各自的业务 `ShardStore.Batch` 中调用
`SloAuthoritativeStartFactory.commandApplied(...)` +
`reconcileDurableStartsInBatch(...)`，与 Message/result/Source Position 一起同步提交；
同一已提交 position 的 replay 只做 byte-identical repair。容量预检失败发生在
`db.write` 之前，`DelayShardTest.commandAppliedStartsShareClientCommandBatchesAndReplayIsIdempotent`
与 `DelayShardTest.commandAppliedOutboxCapacityAbortsTheBusinessBatch` 证明 joint commit、
replay 和 joint abort。该实现仍只证明 shard-local atomicity；objective/catalog authentication、
Message/Admission authority 驱动的 due Start、source-order recovery 编排和 production
collector/export 仍是 release gate。

The source-ordered `PUBLISH_ADMISSION_V1` seam now accepts an immutable
`DUE_ADMISSION_LAG` objective only for `ALL_ACCEPTED`. Typed Admission descriptor fields plus
the local Profile/timing/shard-state gate determine the message ID, unsigned generation,
ordinary managed or managed Pulsar handoff path, and `deliverAt/actionAt`; the implementation
uses the descriptor canonical bytes as a local semantic-evidence digest and materializes the
Start in the same batch as both successful Admission and `ADMISSION_CAPACITY_GATED`.
`SloStartMaterializationException` deliberately bypasses the stale-result compatibility catch:
capacity/integrity failure leaves the source position, message and System Mutation result
unadvanced rather than producing `STALE_SYSTEM_MUTATION`. The evidence is covered by
`DelayShardTest.sourceOrderedPublishAdmissionPersistsAttemptAndMutationResultTogether` and
`DelayShardTest.dueAdmissionSloCapacityFailureDoesNotBecomeStaleMutation`. This closes only
the local typed atomicity seam; the descriptor digest is not external eligibility authority,
and immediate accepted-due Starts, HEALTHY/full-interval proof, production Profile/Oxia/Broker
authority and collector/export remain release gates.

SLO 文档与 Registry 的 native 时间字段也已对齐：`native_handoff_ack_lag` 的起点是
`NativePreparedDelivery` field 10 的未平移 business `deliverAt`，field 11 的 shifted
Broker `deliverAt` 只用于 Broker visibility 语义；V1 native wire 没有独立的
`actionAt` 字段，不能在实现或审计中凭空引入该 API 名称。

Large-payload reservation 的本地读取也采用同一条组合身份边界：`id_cf/RESERVATION`
key 中的 reservationId 必须与 `PayloadReservation` 值一致，值中的 ShardId 必须与
当前 Delay Shard 一致；按 messageId 的 bounded lookup 在超界或发现多个 reservation
时直接 fail closed；`RESERVATION_EXPIRY` timeline entry 还必须与当前 `id_cf` 记录
byte-identical。`DelayShardTest` 覆盖错挂 key、重复 reservation 和 stale expiry projection，
避免 Cancel/Commit/expiry materializer 从不完整投影中猜测唯一预约。Object Store、Oxia
和 source-ordered reservation authority 仍不由此本地检查替代。

## Final gate

设计审计通过不代表实现发布通过。实现只有在上述 artifact matrix 和主设计 §23.5 十项 release gate 全部完成后才可宣称 V1 release-ready；缺少数值、binary、benchmark 或 chaos evidence 的状态是“实现证据未完成”，不是“设计可自行解释”。
