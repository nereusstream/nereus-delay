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
| Lane retirement | source-ordered gate 先到 `CLOSED`，清理与 Floor 条件满足后，same-key replacement 为 `finalGate=RETIRED` guard |
| Scheduler | Lane-first timeline + one READY head + persisted inner DRR；Worker outer DRR 从有限 shard DB 集合重建，不跨 DB 假装原子 |
| Quota | active/pending 每非终态 generation 一次；inflight 每 Claim/attempt obligation 一次；payload ownership 不随 attempt 倍增 |
| Recovery | candidate 必须是 exact Recovery Floor descendant；session-bound Recovery Pin 覆盖选择到 activation；ACTIVE CAS 删除 pin |
| Checkpoint upload | PENDING/PUBLISHED/REAPING Upload Intent CAS；catalog 只接受 complete immutable manifest |
| Time | Admission 使用 frozen decision interval + Broker persistence inequality；replay 不采样新墙钟 |

当前 `PersistentLaneScheduler.rebuildFromAuthoritativeReady` 已提供 fenced 的本地
恢复桥：它从 bounded `timeline_cf/READY` 扫描开始，严格校验对应的
`meta_cf/LANE` incarnation/version/gate/readiness、`id_cf/MESSAGE` 的当前
generation/status，并从 Message 的 source position 重算 exact timeline key、
digest 与 timeline value，再一次性替换内存 pending heads 与 active DRR ring；旧、
孤儿、重复或非 schedulable projection 会 fail closed。`ReadyDiscoveryCursor` 的
`lastScannedReadyKey`、`wrapGeneration` 和 active-ring generation 也会在同一组
SCHEDULER projection 中持久化，恢复不会把 stale Lane 重新加入 ring。该实现证明
的是单 DB 内的本地物理边界和确定性恢复顺序，不等于 Oxia owner/session fence、
typed `ActiveLaneStateV1` 运行时切换或真实 Lane certificate/adapter activation。
Worker 外层 DRR 也只把至少含有一个 schedulable pending head 的 shard 纳入 visit，
空 shard 不会消耗外层 deficit；其 cursor/round 仍是跨独立 shard DB 的 bounded
process state，不伪造跨 DB 的持久原子 ring。

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
Floor。但这仍不是 Oxia
Owner Lease/session CAS；`CheckpointResourceV1` 与
`CheckpointUploadIntentV1`
也已补齐 manifest-object identity 和 PENDING/PUBLISHED/REAPING 的 canonical
state branches；`CheckpointUploadIntentStore` 还提供了 exact-value create
idempotency 与本地 PENDING_UPLOAD -> PUBLISHED/REAPING revision CAS 投影。
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
在同一 WriteBatch 持久化并可在 reopen 后恢复。immutable Profile catalog、
签名 control target 与历史 binding lookup 仍是 release blocker。
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

`OwnedDelayShard` 现在还提供了带 assignment/barrier/source-connection 校验的
统一 `replay` seam，以及兼容性的 `replayCatchup`/`replaySystemMutations`：
Command 和 signed System Mutation 通过 `SourceReplayEntry` 在同一个
source-order stream 中选择分支，每条记录先走同一 shard WriteBatch，成功后才
推进 catch-up cursor，并返回带分支类型的 `SourceReplayOutcome`。它仍不等同于
真实 Kafka/Pulsar consumer、Oxia session/ephemeral authority、broker assignment/
guard 或 production activation transaction。

Worker 资源侧现在还提供了本地 `WorkerLoadVector` 与
`WorkerPlacementPolicy`：它们先按完整 committed capacity、固定/transition
demand 以及 owned/open DB slots 做 hard filter，再以 dominant-resource/load
分数处理 stale telemetry、minimum residence、hysteresis 和 checkpoint/replay
movement cost；这只是可复现的评分 seam，不是 Kafka cooperative assignor、Oxia
desired-placement plan 或 Owner Lease authority。

Owner Lease 的本地 CAS 投影现在还按 V1 lifecycle graph 拒绝回退状态和
`FENCED -> ACTIVE_FOR_COMMANDS` 复活；允许的前向 acquisition/activation
跳转、fence 和 fenced recycle 都保留，真实 Oxia ephemeral session/CAS
仍未完成。Activation 的本地 Oxia adapter 还会在 CAS response loss 后
仅接受同一 fencing/assignment/session identity 的 exact `ACTIVE_FOR_COMMANDS`
重读。
Kafka source records now reject an unexpected Pulsar connection proof instead
of silently ignoring it.

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

普通 local catalog publish 对已存在的 exact manifest 也先做 identity reread，
因此 catalog generation 推进不会把一次已成功的 checkpoint insert 误报为冲突。

`ShardStore.createCheckpoint` 现在先把完整 RocksDB 镜像写入同文件系统的
`checkpoint-tmp` 命名空间，完成后才通过 atomic rename 安装到目标路径；已有
目标会被拒绝，失败 staging 会清理。这闭合的是本地物理 checkpoint 边界，
不代表 Object Store 上传、manifest publication 或 Oxia CAS 已完成。

`CheckpointUploadCoordinator` 现在在本地上传边界内先校验完整 checkpoint
inventory、intent deadline 和 shard/lineage/owner/store/parent identity，取得
Worker upload slot 后才调用 typed adapter；adapter 返回的 manifest object
length/SHA-256/profile/lineage/checkpoint identity 不匹配时保持 PENDING_UPLOAD，
只有校验通过才执行本地 PENDING_UPLOAD -> PUBLISHED CAS。这仍不是 provider
attestation、Object Store immutability 或 Oxia intent/catalog transaction；
同一 pending intent 在 response-loss 重试中会先精确重读已提交的
PUBLISHED successor，且不再次调用 adapter。

`SharedRocksDbResources` 现在也把 checkpoint create/upload slot 纳入进程级
关闭保护；后台 checkpoint 或上传操作持有 slot 时，资源 close 会 fail closed。
同一进程的 restore/download staging 也有独立的 Worker 级 slot，并在
manifest/file 校验、临时目录复制、验证打开和 ACTIVE 安装完成后释放；真实
restore 回归会在返回的 DB 仍保持打开时重新取得该 slot，证明不会把恢复并发
额度错误地绑定到 DB 生命周期。`CheckpointScheduler` 则以确定性 shard
jitter、due claim 上限和 in-flight fence 提供错峰调度；它是 process-local
调度器，不冒充 checkpoint manifest、Upload Intent 或 Oxia catalog authority。

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
重读；它不改变上述生产边界。

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

## Final gate

设计审计通过不代表实现发布通过。实现只有在上述 artifact matrix 和主设计 §23.5 十项 release gate 全部完成后才可宣称 V1 release-ready；缺少数值、binary、benchmark 或 chaos evidence 的状态是“实现证据未完成”，不是“设计可自行解释”。
