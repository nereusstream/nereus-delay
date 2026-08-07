# Nereus Delay V1 Design Audit

状态：PASS / design semantics closed  
Spec revision：`V1-FROZEN-2026-08-01`  
审计日期：2026-08-01  
性质：验收证据索引；不覆盖主设计、Protocol Registry 或 Accepted ADR

## 结论

V1 的业务语义、线性化点、fencing 范围、物理持久边界、故障隔离、恢复/GC 保护关系、公开错误模型和发布停止条件已经闭合。审计未留下需要实现自行选择的语义分支。

**Open semantic questions: none.**

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
and Head prerequisite gate; it does not turn a catalog lookup into a source
position activation receipt.
The Control Operation request union now has local canonical codecs for all
fifteen Registry branches, including the evidence/acknowledgement matrix. It
remains a request-value boundary and does not authenticate an actor/resource,
construct source mutations or produce an Oxia registration receipt.
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
adapter, not a real Oxia client or transport classifier.
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
| Lane retirement | source-ordered gate 先到 `CLOSED`，清理与 Floor 条件满足后，same-key replacement 为 `finalGate=RETIRED` guard；retirement progress 与 terminal guard 在 equal order token 时要求 canonical Source Position 完全一致 |
| Scheduler | Lane-first timeline + one READY head + persisted inner DRR；Worker outer DRR 从有限 shard DB 集合重建，不跨 DB 假装原子 |
| Quota | active/pending 每非终态 generation 一次；inflight 每 Claim/attempt obligation 一次；payload ownership 不随 attempt 倍增 |
| Recovery | candidate 必须是 exact Recovery Floor descendant；session-bound Recovery Pin 覆盖选择到 activation；ACTIVE CAS 删除 pin |
| Checkpoint upload | PENDING/PUBLISHED/REAPING Upload Intent CAS；catalog 只接受 complete immutable manifest |
| Time | Admission 使用 frozen decision interval + Broker persistence inequality；replay 不采样新墙钟 |

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
Terminal generation history uses the same guarded reads for source and
obligation framing in both legacy and v2 branches; the local prefix evidence
is `TerminalGenerationRecordTest`. Direct `DelayShard` history reads also
compare the embedded `messageId/generation` with the requested terminal key;
`DelayShardTest.terminalGenerationLookupRejectsKeyValueIdentityMismatch`
covers the misplaced-value fence before a query or runtime summary can use it.
System Mutation result reads apply the same mutationId/key check, so a
misplaced `dedupe_cf/SYSTEM_MUTATION` result cannot be returned as another
mutation's outcome.
The embedded Kafka source counter also fails closed at `Long.MAX_VALUE` before
mutating its next-offset state; `EmbeddedDelayServiceTest` proves that an
exhausted source cannot wrap into a negative offset after a failed enqueue.
Persisted Delay Shard mutation and Claim sequence metadata applies the same
non-negative u64 boundary on activation; a high-bit-set value is treated as
corrupt and cannot become a wrapped local sequence. The local evidence is
`DelayShardTest.rejectsNegativePersistedShardSequences`.
Every source-position WriteBatch computes its successor through one checked
mutation-sequence helper, including the applied sequence captured by resource
retire/delete records. At `Long.MAX_VALUE` the WriteBatch fails before any
authoritative command or position state is committed; the local evidence is
`DelayShardTest.mutationSequenceExhaustionFailsClosedBeforeCommandMutation`.
Kafka's exclusive activation LSO uses the same fail-closed boundary handling:
an applied offset at `Long.MAX_VALUE` proves a `Long.MAX_VALUE` exclusive
barrier without wrapping the successor calculation. The local evidence is
`SourceActivationBarrierTest.KafkaBarrierSaturatesExclusiveNextOffsetAtLongMaximum`.
The embedded source counter also saturates while reconstructing its next offset
from persisted state, so restart after the maximum offset remains a controlled
exhaustion rather than an arithmetic-open failure; the evidence is
`EmbeddedDelayServiceTest.reopenedEmbeddedServiceSaturatesPersistedSourceOffsetExhaustion`.
Canonical protocol writing now rejects values outside the unsigned 32-bit
range, and the Registry's resource-state versions are encoded as `uint64` in
both retire-intent fixtures and the `ResourceDeleteConfirmedBody` reference.
The local evidence is `CanonicalProtobufTest` plus
`ResourceDeleteConfirmedBodyTest.intentPreservesFullUnsignedResourceStateVersion`;
the latter exercises a version above 2^32 so a narrow wire-width regression
cannot hide behind ordinary small test values.
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
同样重启该 pass。该实现证明
的是单 DB 内的本地物理边界和确定性恢复顺序，不等于 Oxia owner/session fence、
typed `ActiveLaneStateV1` 运行时切换或真实 Lane certificate/adapter activation。
Worker 外层 DRR 也只把至少含有一个 schedulable pending head 的 shard 纳入 visit，
空 shard 不会消耗外层 deficit；其 cursor/round 仍是跨独立 shard DB 的 bounded
process state，不伪造跨 DB 的持久原子 ring。两级 scheduler 现在还对
`weight * quantum` 与 deficit cap 做 checked arithmetic，并对运行时 deficit 累加做
saturating arithmetic；配置、注册或恢复导致的整数溢出不会 wrap 成可调度的错误预算。
`LaneSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` 与
`WorkerSchedulerTest.rejectsQuantumAndWeightArithmeticOverflow` 是本地回归证据，
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
通过统一 proof view 复用现有 reservation commit 状态机；source-position
trust-set authority、Object Store attestation/ownership 和完整 reservation
binding 仍是 release blocker。
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

Managed Kafka/Pulsar ingress 的 transport exception、空结果或 failed stage
现在统一映射为 Registry 的 `ENQUEUE_RESULT_UNCERTAIN`；共享 transport 若误传
`NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED` 或 `NATIVE_ENQUEUE_RESULT_UNCERTAIN`，
也会在 managed projection 边界归一化为 managed stable-code family。只有
`PinnedPulsarNativeSubmissionAdapter` 使用 `NATIVE_ENQUEUE_RESULT_UNCERTAIN`。
该分支映射由 `AdapterIngressTest` 覆盖，避免把 managed Command 的 retry contract
误标成 native submission；managed null-result 和 native-code 泄漏也有回归向量。

Target publish 的本地 transport 结果现在也在 adapter 边界执行 closed-product
校验：`PUBLISHED` 必须携带非空 delivery identity、非空 side-effect evidence、
`StableCode.OK` 和非负 Broker persistence time；若返回 pinned Broker resource，
必须同时携带非负 physical partition；`UNKNOWN`/`DEFINITIVELY_NOT_PUBLISHED`
不得携带成功码、delivery identity、persistence time 或 Broker resource。证据是
`DestinationPublishResult` 与 `DestinationAdapterTest` 的非法组合回归。这只收紧
transport result 的本地输入边界；physical evidence journal、Lane/Worker/target
cluster admission 和真实 Broker outcome proof 仍是 release blocker。

本地 `DestinationPhysicalAdmission`/`BoundedDestinationPublishAdapter` 现在把
target 请求的 physical request/byte charge 作为显式 reservation：Worker 和 target
cluster hard cap、每 Lane cap 以及所有其它 READY Lane 的 committed minimum 都在
同一 gate 中检查；logical callback 超时只能把 reservation 标为 `ZOMBIE`，达到
Lane zombie cap 立即阻止该 Lane 的新 Admission，直到 physical release 后显式清除
block。delegate stage 完成（包括 `UNKNOWN`）才释放 request/byte charge；capacity
拒绝不会调用 delegate。`DestinationPhysicalAdmissionTest` 与
`BoundedDestinationPublishAdapterTest` 覆盖 READY minimum、跨层 cap、identity、
zombie 和 response completion。该组件只是进程内可重建的资源闸门，尚未接入持久
`ActiveLaneState`/`ReadyCertificate`、Owner/Lease/Oxia authority、真实 channel
teardown 或 Broker evidence journal，因此不能宣称 production admission 已闭合。

`PublishOutcomeBody.encodeInitial` 和
`PublishOutcomeBody.encodeEvidenceResolution` 现在复用 Registry 的 common fields
1–3，并在返回前执行本地 decode round-trip；初始 Outcome 的
`PUBLISHED`/`NOT_PUBLISHED`/`UNKNOWN` side-effect/disposition/stable-code/retry
组合因此不会由调用方随意拼出，definitive transfer 必须是 canonical
`ChargeVectorV1`，Evidence Resolution 的 cursor 必须是 typed canonical
`EvidenceCursorV1`。`PublishOutcomeBodyTest` 与 `DelayShardTest` 覆盖编码器、非规范
ChargeVector、typed cursor 以及 source-ordered close/requeue；这仍只是 canonical
body codec 和本地 transition seam；当前 local transition 还验证了已 admitted
generation 在 Close marker 后收到 definitive `NOT_PUBLISHED` 时固定写入
`LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED` 并停止 retry。它不等于签名服务、真实
Broker evidence、strong-capability retirement 或 production outcome authority 已完成。
同一关闭边界下的 `UNKNOWN` 结果保留 `UNCERTAIN` obligation 且不创建新的
`UNCERTAIN_RETRY` timeline；后续 Resolve retry 仍由 closed-Lane gate 拒绝。

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

Evidence branch validation also checks the adapter-specific target resource
and cursor/channel branch, so a Kafka evidence envelope cannot carry a Pulsar
resource (or vice versa); authenticated Broker response and external proof
ownership remain separate release gates.

`OwnedDelayShard` 现在还提供了带 assignment/barrier/source-connection 校验的
统一 `replay` seam，以及兼容性的 `replayCatchup`/`replaySystemMutations`：
Command 和 signed System Mutation 通过 `SourceReplayEntry` 在同一个
source-order stream 中选择分支，每条记录先走同一 shard WriteBatch，成功后才
推进 catch-up cursor，并返回带分支类型的 `SourceReplayOutcome`。它仍不等同于
真实 Kafka/Pulsar consumer、Oxia session/ephemeral authority、broker assignment/
guard 或 production activation transaction。

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
apply overload 仍明确只是 embedded seam。

Worker 资源侧现在还提供了本地 `WorkerLoadVector` 与
`WorkerPlacementPolicy`：它们先按完整 committed capacity、固定/transition
demand 以及 owned/open DB slots 做 hard filter，再以 dominant-resource/load
分数处理 stale telemetry、minimum residence、hysteresis 和 checkpoint/replay
movement cost；这只是可复现的评分 seam，不是 Kafka cooperative assignor、Oxia
desired-placement plan 或 Owner Lease authority。

Control Reserve 的本地投影也已覆盖 Registry 的 class 6：
`meta_cf/CONTROL_RESERVE` 以 `CapacityVectorV1` 持久化 Broker system-writer
reservation，绑定 `NON_OUTCOME_CONTROL` grant identity；class 6 只接受维度
51–53，class 3 排除这些维度，二者合计必须被同一 immutable grant 覆盖。
`DelayShard` 的同步 charge/release 和重开校验已有
`DelayShardTest.systemWriterReserveProjectionIsPartitionedAndPersistsAcrossReopen`
及错误维度 fail-closed 测试。该证据只闭合 shard-local projection，不证明
Route Broker/source-writer 的远端 quota authority、跨 shard placement 或
实际 operation charge。

Owner Lease 的本地 CAS 投影现在还按 V1 lifecycle graph 拒绝回退状态和
`FENCED -> ACTIVE_FOR_COMMANDS` 复活；允许的前向 acquisition/activation
跳转、fence 和 fenced recycle 都保留。续租响应若改变期望的 lifecycle
state 也会 fail closed，即使 fencing/assignment/session identity 相同，避免
把状态漂移误当作成功续租。真实 Oxia ephemeral session/CAS 仍未完成。
Activation 的本地 Oxia adapter 还会在 CAS response loss 后仅接受同一
fencing/assignment/session identity 的 exact `ACTIVE_FOR_COMMANDS` 重读；
`transitionOrRead` 在 lifecycle graph 禁止的请求上不会执行重读，因此
非法 transition 不会被 coincidental current state 掩盖。
Response-loss reread 还拒绝同 identity 但 expiry 变短的 successor；本地证据为
`OxiaOwnerLeaseStoreTest.transitionOrReadRejectsAResponseLossSuccessorWithShorterExpiry`。
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
内部 `dedupe_cf/COMMAND` replay lookup 也检查 command key 的 Shard 与结果的
Source Position；Claim lookup/scan 则检查其 `DelayMessageId` 的 self-routing
Shard，避免跨 Shard 的旧去重结果或 Claim 进入 source replay、owner drain 或
admission；证据为 `DelayShardTest.commandDedupeLookupRejectsForeignSourcePosition`
与 `DelayShardTest.claimLookupRejectsForeignMessageShard`。
SLO outbox 的 direct `get(sampleId)` 也与 bounded scan 使用同一
`meta_cf/SLO_OUTBOX` key/value sample-id fence，错挂的 Start 不会进入 Final
merge；`SloObservationOutboxStoreTest.scanRejectsKeyValueSampleIdentityMismatch`
覆盖 direct 与 scan 两条读取路径。
Close-materialization discovery 也会在返回 scheduler work 前重验 cursor 的
embedded close Source Position Shard，与 direct cursor query 保持同一边界；
`DelayShardTest.laneCloseMaterializationDiscoveryRejectsForeignSourcePosition`
覆盖该 scheduler-only 路径。
Retired Lane guard 的直接读取也校验其 terminal Source Position 属于当前
Shard；错挂的退休证明不会通过 `getLaneTerminalGuard` 暴露。
`ShardStore.flushAndSync` 还提供 drain 的物理 flush/WAL-sync 原语，重开回归为
`ShardStoreTest.flushAndSyncMakesTheShardBoundaryExplicit`；它不替代远端 callback
quiescence 或 final checkpoint publication。
`DelayShard.listOpenPublishAttempts` 还提供 bounded 的
`PUBLISHING`/`UNCERTAIN` ledger view，供 drain 等待 admitted callback 的本地轮询
使用；重复 attempt identity 或超 bound 都 fail closed，不能把未知 obligation
当作已清空。
`OwnerDrainCoordinator` 将 source/scheduler stop、authority-gated `DRAINING`、
Claim revoke、bounded callback poll、lease/deadline reread、flush/sync、可选
final checkpoint、Store close 和 exact release 串成一个可重试的本地顺序；
如果 caller 提供 final checkpoint 的 exact 16-byte identity，coordinator 会把
它传入 `ShardStore.createCheckpoint`，让完整镜像携带对应 `lastCheckpointId`；
`flushAndSync` 后的可选 `commitSourceHint` 只收到最后已持久化的
`SourcePosition`，callback 返回后还会重新检查 draining lease；该 hint 仍不是
recovery authority。物理 final checkpoint 安装完成后也会再次检查 lease，只有
这条检查通过才会关闭 Store 和执行 exact release，从而把 checkpoint 期间的
lease 丢失转换为本地 fence，而不会让旧 owner 继续操作新 owner 的状态；
`OwnerDrainCoordinatorTest` 覆盖该边界。
如果 Store close 本身失败，也只本地 fence 并保留 authoritative `DRAINING`
lease，等待可见重试，不会在 DB 未确认关闭时释放 lease。
callback/source quiescence 仍由调用方和真实 transport 提供，超时保持
`DRAINING` 而不伪造成功。`OwnerDrainCoordinatorTest` 覆盖成功与 deadline
失败边界。
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

本地 `RecoveryCatalog.publishUploadedCheckpoint` 现在要求 PUBLISHED intent
与完整 manifest 的 shard、lineage、checkpoint、manifest hash/length、owner
和 store incarnation 完全一致后才接受 catalog projection；同一 checkpoint
的 exact manifest 在 response-loss 重试中会作为幂等 reread 返回，即使 catalog
generation 已被其它操作推进；同 ID 不同 manifest hash 或 Object Store
container/key/version/profile 仍 fail closed。这仍不等于
Object Store 真实性或 Oxia transaction；Oxia validation adapter 同样允许
generation 相等的 exact reread，但拒绝 generation 回退。

Legacy/typed local Recovery Floor CAS 也支持 exact successor reread（含
checkpoint、manifest、source/mutation 和 evidence/cursor identity），response
loss 不会重复推进 Floor；不同 Floor 或 identity drift 仍 fail closed。
Floor coverage 与本地 GC guard 在 order token 相等时还要求 Source Position
canonical bytes 完全一致；同一 Kafka offset 或 Pulsar ledger/entry/batch 的
metadata 变体不能被当作已覆盖的 retention boundary。

Checkpoint GC 的 catalog-backed guard 现在会在 source/sequence/ancestry
证明之后 reread 当前 `RecoveryPinV1`。活动 pin 若保护待删 checkpoint 的
candidate 或 observed Floor，返回 `RECOVERY_PIN_PROTECTS_RESOURCE`；pin
读取失败返回 `RECOVERY_PIN_STATE_UNAVAILABLE`，两者都禁止本地 tombstone
compact。这只闭合了本地 pin-aware necessary condition，仍不等于 Oxia
session CAS、provider delete attestation 或完整的 external GC orchestration。

`DelayShard` 的本地 `gc_cf/TASK` lookup 还会把 requested resource kind、identity
hash 和 expected version 与嵌入的 retire intent 逐项比对；delete confirmation 的
nested intent 也必须匹配同一 key。错挂的 GC value 会在 query/compaction 前 fail closed，
回归证据为 `DelayShardTest.gcRetireIntentLookupRejectsKeyValueIdentityMismatch`。

普通 local catalog publish 对已存在的 exact manifest 也先做 identity reread，
因此 catalog generation 推进不会把一次已成功的 checkpoint insert 误报为冲突。

`ShardStore.createCheckpoint` 现在先把完整 RocksDB 镜像写入同文件系统的
`checkpoint-tmp` 命名空间，完成后才通过 atomic rename 安装到目标路径；已有
目标会被拒绝，失败 staging 会清理。这闭合的是本地物理 checkpoint 边界，
不代表 Object Store 上传、manifest publication 或 Oxia CAS 已完成。
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
RocksDB reopen 证明不会遗留 native 文件锁。
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
错误。

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
恢复；这仍只是本地文件完整性边界，不替代 Object Store 内容证明。

`SharedRocksDbResources` 现在也把 checkpoint create/upload slot 纳入进程级
关闭保护；后台 checkpoint 或上传操作持有 slot 时，资源 close 会 fail closed。
同一进程的 restore/download staging 也有独立的 Worker 级 slot，并在
manifest/file 校验、临时目录复制、验证打开和 ACTIVE 安装完成后释放；真实
restore 回归会在返回的 DB 仍保持打开时重新取得该 slot，证明不会把恢复并发
额度错误地绑定到 DB 生命周期。`CheckpointScheduler` 则以确定性 shard
jitter、due claim 上限和 in-flight fence 提供错峰调度；completion 必须带回
`claimDue` 返回的 exact process-local 句柄，只有当前 claim 才能推进 next due，
shard-only 或迟到旧 claim 都 fail closed。它是 process-local 调度器，不冒充
checkpoint manifest、Upload Intent 或 Oxia catalog authority。
同一资源封套现在还提供 `maxConcurrentDrainsPerWorker` 的独立 drain slot，
争用和资源 close 保护由 `ShardStoreTest.drainSlotIsWorkerBoundedAndCloseProtected`
覆盖；这只证明进程级 drain 并发限额，不能替代 claim quiescence、final checkpoint
和 lease release 的生产编排。

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
`OxiaControlOperationAuthority` 对 backend 的 CURRENT 响应执行 operation/request/
scope identity 与 revision 不回退校验。这只是本地 CAS/验证 seam，durable
control-operation query state、routing、authorization 和真实 Oxia ownership 仍未完成。
`EmbeddedDelayService` 已将该 seam 暴露为本地 register/advance/query 入口，便于
conformance tests 验证 register 和 exact advance response-loss 后的精确 receipt
重读；`OxiaControlOperationAuthority.advance` 不接受更高或不同状态的后续
CURRENT 来冒充目标 revision 的成功。它不改变上述生产边界。
该 adapter 还在远端调用前校验 receipt identity、register revision 和
`expectedRevision + 1` 的连续性，避免把非法请求交给 authority。

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

本地 `SloObservationOutboxStore` 已把 `meta_cf/SLO_OUTBOX` 的扫描边界收紧为
key/value `sampleId` 必须 byte-identical；错挂的 key 不会被导出为另一个样本，
而 collector acknowledgement 仍必须匹配当前完整 record digest 才能删除。回归
证据为 `SloObservationOutboxStoreTest`。这只补足 shard-local 持久化完整性，不能
替代 SLO Start 重建、collector merge/export 或生产观测 authority。

Large-payload reservation 的本地读取也采用同一条组合身份边界：`id_cf/RESERVATION`
key 中的 reservationId 必须与 `PayloadReservation` 值一致，值中的 ShardId 必须与
当前 Delay Shard 一致；按 messageId 的 bounded lookup 在超界或发现多个 reservation
时直接 fail closed；`RESERVATION_EXPIRY` timeline entry 还必须与当前 `id_cf` 记录
byte-identical。`DelayShardTest` 覆盖错挂 key、重复 reservation 和 stale expiry projection，
避免 Cancel/Commit/expiry materializer 从不完整投影中猜测唯一预约。Object Store、Oxia
和 source-ordered reservation authority 仍不由此本地检查替代。

## Final gate

设计审计通过不代表实现发布通过。实现只有在上述 artifact matrix 和主设计 §23.5 十项 release gate 全部完成后才可宣称 V1 release-ready；缺少数值、binary、benchmark 或 chaos evidence 的状态是“实现证据未完成”，不是“设计可自行解释”。
