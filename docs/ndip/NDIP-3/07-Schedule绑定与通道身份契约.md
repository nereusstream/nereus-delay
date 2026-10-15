# NDIP-3 B2：Schedule 绑定与通道身份契约

Status: Draft / 契约冻结；切片验收见 progress.json。

本节扩展 [执行/控制兼容契约](06-执行与控制兼容契约.md)。已实现精确绑定、不可变通道
身份及静态投影/续期检查；完整 membership grant 对象见 [成员授权契约](08-成员授权与source关联契约.md)，认证控制与完整策略见 [§09](09-成员策略与认证控制契约.md)。C1/C2 承担
source/Owner 原子提交、Admission、实际通道池和 teardown，本文不授予运行时激活权限。

## 1. TargetScheduleBinding

新绑定保留已接受的 Schedule 或 PrepareLargeSchedule 完整 canonical body，不重新
生成 prepared bytes、command identity 或 hash。Prepare 的 bindingSource 是其授权
位置，Commit 创建 Message 时使用后续 source；同资源 source 顺序必须不早于绑定。
Reschedule 的新 generation 继续引用适用的精确绑定；变更绑定必须按显式 source 操作
形成新对象，不能就地改写被旧 generation/attempt/reservation 引用的 body。

closed canonical protobuf schema=1：

| field | 内容 |
|---|---|
| 1 | uint32 schema=1 |
| 2 | DelayMessageId[41] |
| 3 | uint32 command type，1=SCHEDULE、2=PREPARE_LARGE_SCHEDULE |
| 4 | 完整 Registry canonical body；由现有对应 decoder 验证，不接受 compact embedded body |
| 5 | bindingSource 的完整 TargetSourcePosition bytes |
| 6 | TargetPartitionId[32] |
| 7 | domainSlot uint32，0..63 |
| 8 | 非零 raw uint64 domainGeneration |
| 9 | 非零 accountingIncarnation[16] |
| 10 | 非零 requiredDispatchRef[32] |
| 11 | 非零 offeredDispatchRef[32] |
| 12 | 非零 controlScopeRef[32] |
| 13 | 非零 membershipGrantRef[32]；完整对象见 §08，认证控制/策略见 §09 |
| 14 | optional 非零 nativePolicyScopeRef[32] |
| 15 | optional 非零 orderingDomain[32]，仅 DELIVERY_TIME_FIFO 必填 |
| 16 | SHA-256(domain + canonical fields 1..15) |

hash domain 是 UTF-8 `nereus-delay-target-schedule-binding` 后接一个零 byte。必填字段
1..13/16 不可缺失；14/15 按 presence 规则处理。FORBID 不能携带 Native scope；ALLOW
表示许可，未取得共同 Native scope 时可以只走 ordinary（原设计 §7.10），不能因此
宣称已进入 Native 调度。strict intent 自身禁止 Native。旧 intent 缺 field 14 的精确
bytes 可以保留为 legacyPolicyDefault/FORBID，不能以兼容解码补出 Native 权限；新
source writer 的显式 policy 要求由 C1 激活门落实。

绑定不嵌入 TargetMessageLocator，以避免 binding digest 自引用。locator 的
message/target/domain/accounting/orderingMode/orderingDomain/bindingDigest 必须全部
匹配。queue 校验绑定非 VACANT 的精确 generation、offered/control ref 和必要的
Native ref；DRAINING/PAUSED 的历史投影可读，但不会由此获得新绑定或发送权限。

`requireReferences` 核对 exact destination/capability Profile refs、物理资源/partition
许可、完整 required projection、offered 覆盖关系和完整 control scope。仍执行消息
payload/metadata/ordering 限制。它不认证 Profile/retry 在 source 上已激活，不证明
membership/tenant 授权，不替代最终物理 record 大小、Native policy 或 live Owner gate。

### 1.1 先限定分配量，再调用原 decoder

`TargetScheduleBody` 在原 decoder 建立列表前检查每一层的长度/field 数，保留原编码：

- body≤19927040 bytes，root≤9 fields；intent≤14 fields。
- inline payload≤16MiB；ordering key、business key、完整 AdapterMetadata 各≤1MiB。
- Kafka header 或 Pulsar property 各最多 1024 项；metadata union 只有一支，每项≤2 fields。
- Profile ID、retry policy ID 各≤256 bytes；ProfileRef≤306、RetryPolicyRef≤304 bytes。
- committed descriptor≤4195328 bytes、≤9 fields；四个对象身份字段各≤1MiB。
- Prepare 的 trust-set ref≤45 bytes、≤2 fields，ObjectStore ProfileRef 同样受限。

body 的保守上限是 `16MiB + 3*1MiB + 4096`：4096 覆盖固定标量、引用和 framing；
object branch 的上限低于 inline branch。超限旧数据是 B6/F1 转换冲突，不截断或改写
原命令使其通过。完整 binding≤20976072 bytes；包含 16MiB payload、最大 metadata、
两类 key、Profile/retry raw versions 和完整最大 Pulsar source 的独立向量实际为
20972671 bytes，其中 body=19923675。该 envelope 需由 C1/A2 的实际预算装配兑现，
不能单凭 codec 上限宣布整个最大 mutation 已通过资源认证。

## 2. TargetChannelIdentity

通道固定一个 source Shard、Target、accounting/domain generation 和有界 channelSlot。
默认每个 source Shard 的强能力恢复域独立；同 Shard/Target 的兼容 Profile 可以共享
通道。不同 source Shard 不共用本次定义的 physical Producer。跨 Shard 的 baseline
Producer 合并属于另行证明的优化，不能从客户端线程安全推导权限或隔离性。

closed canonical protobuf schema=1：

| field | 内容 |
|---|---|
| 1 | uint32 schema=1 |
| 2 | sourceShard[20] = RouteIncarnation[16] + partition u32be |
| 3 | TargetPartitionId[32] |
| 4/5 | domainSlot uint32 (0..63) / 非零 raw uint64 domainGeneration |
| 6 | 非零 accountingIncarnation[16] |
| 7/8 | 非零 dispatchCompatibilityRef/controlScopeRef[32] |
| 9 | ChannelKind：BASELINE_PRODUCER=1、KAFKA_TRANSACTIONAL_RECEIPT=2、PULSAR_DEDUP_PRODUCER=3 |
| 10 | channelSlot uint32，保留完整 unsigned 位型；实际槽数由有限配置和资源预算约束 |
| 11 | 非零 raw uint64 channelGeneration |
| 12 | 精确 ASCII producer/transactional identity，公式见下文 |
| 13 | SHA-256(field 12) |
| 14 | optional 非零 raw uint64 evidenceGeneration，仅 kind 2/3 必填 |
| 15 | 非零 resourceGuardAttestationDigest[32] |
| 16 | 完整 CredentialUseLease，kind=DESTINATION_CHANNEL，Profile kind=DESTINATION |
| 17 | SHA-256(identity domain + canonical fields 1..16) |

生产者身份是 `nd-target-` 加 SHA-256 的 64 个小写 hex 字符（总计 74 ASCII bytes）。
其 hash domain 是 UTF-8 `nereus-delay-target-producer` 加一个零 byte，后接以下
canonical protobuf：1 sourceShard[20]；2 targetId[32]；3 domainSlot；4 domainGeneration；
5 accountingIncarnation[16]；6 ChannelKind；7 channelSlot；8 dispatchRef[32]；9 controlRef[32]。
它不含 Profile ID/version、channelGeneration、credential generation 或 mutable gate。

credential holder scope 是 SHA-256(UTF-8 `nereus-delay-credential-holder-target-channel`
加零 byte + canonical fields 1..15)。它与原 Lane holder domain 分离。整个 identity 的
hash domain 是 UTF-8 `nereus-delay-target-channel-identity` 加零 byte。

新通道不能任意传入 producer 名称；解码重新计算并验证字段 12/13。旧 Lane 的 producer/
transactional identity 不重新派生、不换标签；其原 ChannelResourceIdentity、Admission、
Journal/receipt 和 sequence 映射沿旧格式恢复，直到相应保护解除。新 namespace 不能
被用来绕过旧 UNKNOWN。

### 2.1 续期、资源依据与读取边界

续期保持 source/target/accounting/domain、完整 dispatch/control refs、kind、channelSlot
和 evidenceGeneration；channelGeneration 必须恰好加一。raw uint64 跨 signed 高位合法，
UINT64_MAX 耗尽沿用 TargetQueueState 的 IllegalStateException，禁止回绕。
同一 producer/transactional identity 及 durable sequence/journal 域继续保留，不从零
重置。更换 evidence namespace 或执行域需显式排空/转换，不能伪装成普通续期。

每次续期产生新的完整 CredentialUseLease/identity。即使 credential Profile 或 secret
版本未变，也不能原地改 expiry/protection。当前 credential provider Profile 必须与
lease.ref 完全相同，并指向相同物理资源、相同服务端授权 scope；不同 Profile 的候选
能否使用该 provider，仍需完整 membership grant。lease 的 binding/protection/live TTL
和 loaded fingerprint 使用现有凭据权威验证，resource guard 必须证明 offered guarantees。

Profile ID、trusted time source ID 各≤256 bytes。canonical 上限：ProfileRef=306、time
Evidence=422、lease=907、context fields 1..15=333、完整 identity=1279 bytes。最大全宽
独立向量实际 identity=1274、lease=904、time=420 bytes，保留 signed time service 的
64-byte signature 和所有 raw generations。root/lease/profile/time field 数分别≤17/11/4/10。

`decodeForStore` 同时核对 exact key 和 Store source Shard。`requireQueueProjection`
核对 accounting、generation、非 VACANT 与全部 refs；`requireNewWorkProjection` 还
要求 OPEN/ACTIVE、正数 activatedChannelSlots 和 slot 小于该上限。静态 snapshot
检查不认证 live Owner，不替代整个操作的预算、View/Source/Owner 提交屏障。

## 3. 冻结 attempt 与 teardown 契约

`requireExactFrozenIdentity` 比较整个不可变 identity，不能用一个 ref 相同的片段或
重新签发的 lease 替代旧 Admission 对象。首次 Producer call 必须执行原冻结 lease/
证据有效性及 gate-to-library ownership 间隔检查。租约失效且尚未交给库的请求，按
可证明的未提交分支结束；不得把同一个 Admission 换到健康 slot 重试。

对已提交/UNKNOWN 的恢复，可以建立经新 Owner/fencing 认证的恢复通道，但旧 attempt
仍引用原 frozen identity。新恢复授权与旧 attempt/Journal/producer/sequence 的精确
关联单独核验；恢复通道不反向覆盖旧证据，也不把其新 lease 当作旧首次发送许可。

| 事件 | 新工作/进程资源动作 | 必须保留的持久义务 |
|---|---|---|
| 一个 Profile 不再新增绑定 | 停止该 Profile 新绑定，其他兼容成员仍可使用通道 | 已接受 binding、reservation、attempt 及其引用 |
| Target/domain Pause 或 DRAINING | 不再取得新发送许可；可逆 queued/Claim 按 source 协议释放 | 已 Admission work 与完整 Outcome/UNKNOWN 恢复依据 |
| source Shard Owner loss | 立即关闭该 Shard 新请求 gate；停止未交给库的请求，调用该 Shard client 的 close/abort | close 请求不等于 physical completion；未决/zombie 继续计数和恢复 |
| Producer 故障 | 影响该 Producer 的全部 Profile 使用者；未 Admission 候选可选兼容健康 slot | 已 Admission 不换 slot/producer/sequence；确定 disposition 后才产生新 attempt |
| 普通 credential renewal | 建立新 channel generation、重新验证保护/live lease，保持 stable producer domain | 旧 identity 和 lease 仍被旧 attempt 原样引用 |
| 最后一个逻辑使用者退出 | 标记关闭并禁止新 acquire；等待所属 queued/physical request 的有界收敛 | 不用逻辑 refcount=0 推断 outstanding/zombie/Journal 义务为零 |
| physical client 已关闭 | 可以释放已明确结束的本地连接资源 | unresolved attempt/receipt/Journal/recovery pins 仍保护 immutable identity |
| 退休 immutable channel/binding | 仅在所有 Message/reservation/attempt/recovery/checkpoint 保护证明可释放后执行 | 没有证明就保留；terminal、TTL 或 lease 到期均不是单独的回收依据 |

C2 必须记录每个 queued/physical/zombie 请求的 source Shard、channel generation 和
attempt 归属，维护已配置的有限并发/bytes/lease/超时上限。实际 teardown 的状态机与
故障测试属于 C2/D2，不能以本表或 codec 测试代替。禁止引入跨 Shard 的全局 sequence
allocator，也不顺便放开原 P1 单未决、batching 或 response correlation 限制。

## 4. Store 预留、能力支持表与剩余项

2026-10-07：普通 managed Target Admission v4 已冻结完整不可变 channel/lease 与 physical Target，
并提交 Claim/CLAIMED Message digest、完整 Claim proof、payload length/hash、原 adapter/reserved
metadata、destination/capability Profiles、时间和 artifact-generation-set digest。其 prepared hash
覆盖全部字段及 optional eventTime presence，具体封闭字段与独立 tuple 见 Registry §5.3。
source 首应用要求 retained NV20 identity 全字节一致、queue projection 和实际 Claim/Message/绑定
一致；decision/Broker 时间还受 Claim deadline 与 frozen credential expiry 约束。
MaterializationAuthority 必须是 Store 外已解析的历史保护/能力/凭据 snapshot，缺失时保留 entry。
测试中的该 authority 和 credential lease 是显式 fixture。

这只闭合普通发送身份冻结和本地首次 Admission。Native 提前发送需独立完整模板/timing commitment，
不可套用普通 Claim；实际 Producer/request 构造、live gate、authenticated receipt/Journal 与 frozen
commitment 精确匹配、proof retention 和 Claim 缺失恢复仍未闭合。没有新 NV type 或物理发送许可。

2026-10-07 后续：普通 current PUBLISHED 首应用已消费该冻结身份和 retained Admission 首结果证明。
直接 ACK 的 Target/partition/prepared hash（Pulsar generation 2 另含 artifact digest）由静态 helper
比较；EvidenceContext 另要求 Store 外已解析的 provider authentication snapshot，核对完整响应、
sender/sequence 与请求关联。事务 receipt/Journal 等不能降为普通 ACK。成功保留其它旧义务和原
channel，payload 无剩余 ref 才 retain，预算只释放 execution charge 并等待 Floor。fixture 验证了
本地 source batch 和 ACK retry，生产 provider/Producer/Broker、完整 retention/recovery 仍未认证。

2026-10-07 后续：`DestinationPhysicalAdmission` 增加完整 immutable Target channel 的注册、READY、
acquire、source-Shard close 和 teardown 入口，复用原 Worker/cluster/active/zombie 总额及状态机。
Target generation 使用 NV20 内容键区分，续期不继承或清空旧请求；Source close 只停止所属 Shard
的新/queued call，其它 Source Shard 仍可工作，旧 physical/zombie charge 留到实际 completion。
Profile、Source Shard 和 slot 不新增 Lane READY minimum；Target 公平性/ordinary 保护仍属于原
Target ring/WorkClass 图。注册数量、activated slots、连接/Producer/FD envelope 仍由生产资源图证明。

`PulsarPreparedRecordFactory.targetManaged` 现从 frozen publication 和精确新 PUBLISHING Message，
将原 inline/object payload projection、Pulsar key/ordering key/properties/event time、九个 reserved
properties、artifact digest 和固定 Journal sequence 合为既有 final record；producer-name hash
必须等于 channel 的 stable producer identity。该构造不会分配序号，也不证明 Journal durability。

`BoundedDestinationPublishAdapter.submitTargetPreparedRecord` 使用上述同一 pool，不伪装 Lane。
强制传入的 final gate 经 Pinned adapter 到 source-locked transport，在 SDK metadata 构造后、
`sendAsync()` 前执行；没有该 hook 的旧 adapter/transport fail closed，不退回旧 record API。
Source close 在队列后及该最终点都复查；目标 Producer name 在 SDK 构造前检查。SEND 已调用后
同步异常/无法观察 stage 时传播不确定性，由原 bounded wrapper 保留 zombie，而非用 UNKNOWN
completed stage 假装物理完成。SDK H0 probe 使用 CountingProducer，认证/序号为 fixture，零 Broker；
实际 Target Journal namespace/recovery、exclusive send token、Owner/Store/credential/activation
providers、应用宿主接线、Native/baseline record 分支和真实 Broker 仍未闭合。

2026-10-07 后续：Target Journal v1 专用 codec/namespace 已复用旧四态实现，Lane generation 3
bytes/constructor/accessors 保留。完整 Target channel 冻结进 mapping；stable producer scope
跨正常 renewal 延续序号，同一 attempt 不能切换完整 channel、source、prepared/template/artifact
或另建 mapping。Target attempt 点查使用独立 bounded map，不扫描 retained history。
新的 `targetJournalIdentity`/mapping overload 把精确 Admission source、Message/payload 和 final
record 与完整 mapping 相联；mapping DTO 仍不等于 durable/Owner 权威。

`PulsarClientArtifactAttemptJournal.openTarget` 使用独立 codec、有限 retained-replay record/byte
预算与 mandatory Writer/namespace guard，复用 guarded ACK 和 contiguous startup/readback。
Target writer 使用 Exclusive producer，不以重新创建来 reverse-fence 已占有 topic 的 replacement。
guard 在创建前、恢复后、append 前和 ACK/readback 后检查；失败的 append 封住该 Journal 实例。
恢复出的 OWNERSHIP_STARTED 没有本地 first-send token；新 marker 的 token 只消费一次，异常不退还。
恢复预算不足/顺序损坏后同样封闭实例，不能将 partial replay 当作无历史。

该实现不覆盖 Target cursor/checkpoint/Floor seed、Journal GC、Native 完整模板、生产 Writer guard
装配或应用宿主。真实 P1 append/reopen 的本次范围及测试 fixture 边界见
`evidence/c2-target-journal-namespace-focused.log`，不能提升完整 C2/D2/生产 authority。

2026-10-07 后续：发送快照现在必须经 bounded Store read 绑定已应用 v4 首结果、精确 Budget/
Message/Store，再由当前 Worker Owner 包装；SDK final gate 再核对该快照。Target Publisher 已连接
mapping/ownership、final record、物理 stage、完整 ACK 验证、Journal PUBLISHED、signer 和 bounded
source Outcome handoff。handoff/completion 重试保留原 bytes，不建立新 SEND；source 实际首结果
APPLIED 后才释放该 in-process pending。live credential/activation、完整 authenticated response/
retention、production providers 仍不由 DTO 或这一连接隐式授予。

实际 P1 Target business SEND 使用 channel 的 stable producer name 和固定 sequence，核验
record/template/prepared hashes、producer/sequence authority、P1 source lock 和 artifact digest。
Source Message/Admission/live guard 是显式 fixture；它不代表 production Host→Store→Broker→Source
完整验证。RocksDB/Worker 编排证据与 Broker slice 的具体边界见
`evidence/c2-target-publish-source-bridge-focused.log`；Native/baseline、NOT_PUBLISHED、UNKNOWN/recovery
完整编排、Host drain/retention/Floor/GC 和生产 authority 仍开放。

2026-10-07 后续：可达的 MAPPED/ownership append 不确定和 synchronous first-send error 现保留
UNKNOWN/pending，而不是留下没有 completion 结果的 slot。recovery ownership 不重发。local
physical admission 的文字拒绝缺少 exact Target evidence，不能当成 definitive NOT_PUBLISHED；
转为 UNKNOWN 保留原预算，完整 typed NOT_PUBLISHED authority/path 仍待完成。completion/queue
拒绝可以继续处理原结果/bytes，不重复 SEND；Source actual first result 是最终 settlement 依据。
同一 handoff 实例只保留一条 exact action，queue acceptance response 丢失不会反复注册相同任务。


2026-10-07 后续：current NOT_PUBLISHED 已复用 definitive source batch，接受既有精确 Broker
rejection evidence 与 mandatory v4/history/authenticated classifier snapshot。消息/Lane-wait retry
核对原 policy/window/jitter/admission budget，生成非 Native DEFINITIVE_RETRY；旧 UNCERTAIN refs
存在时保持 hold，不创建普通重试。永久拒绝或 source CLOSED 写 DEAD_LETTER，Close 的首结果返回
LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED。strict barrier 仅在 refs 结清后解除，retry 保持原
watermark/ordered key 并重建 ORDER_HEAD。execution charge 结束，reserve/allocated 保留至 Floor；
终态仅无 refs 才 retain payload。细节与 fixture 边界见
`evidence/e5-current-not-published-source-focused.log`；其它 absence/recovery、production classifier、
DLQ export、terminal/historical、Broker/Host 和完整集中验证仍开放。


2026-10-07 后续：`readRecoveryAdmission` 现从当前 Worker 的 Owner/read guard 读取 exact retained
Admission first result、ADMITTED Budget/current PUBLISHING Message 和 source。它不要求旧 Owner/
Store 身份等于新的活动实例，返回独立 Recovery 类型，不能作为普通 Applied 发送快照。
普通首发仍保留原 Store/Owner 限制；两个 reader 均核对完整 budget execution/commitment/allocated
和当前 admissionsUsed。恢复 signer 只生成 OWNER_FENCED/RECOVERY_FIRST_SEND_UNCERTAIN、typed
UNCERTAIN_HOLD、无 next retry 且 zero transfer 的初始 UNKNOWN；新的 Owner 签名仍必须经实际
handoff 当前 lease 校验和 source historical/active-owner authority，不能借该 reader 获得 SEND 权。
证据见 `evidence/c2-recovery-admission-proof-focused.log`；真实新 Owner/new Store/restart、生产
history resolver、恢复枚举及 Host 编排仍开放。


2026-10-07 后续：`TargetPublishRecoveryExecutor` 现把 guarded retained Admission、只签一次的
恢复 UNKNOWN 和原 OUTCOME_AND_CONTROL handoff 接成单条有界 pending。handoff 必须属于该
exact Worker 实例，重复 Admission/Owner 复用同一 mutation；retry 不读回旧快照或重签，
actual Source APPLIED recovery-code/hash 首结果才清除 pending。它没有 Journal/Producer 依赖。

新增实际 Worker/RocksDB 场景在 v4 Admission 后释放旧 lease，取得更高 epoch，重新审计 Target
root 并通过 OwnerActivation 持久记录/激活新 Owner；旧 Admission 普通 send read 被拒绝，
新 Owner 只应用 UNKNOWN、保留旧 obligation epoch、execution/reserve/allocated 和 strict barrier。
ACK_UNKNOWN 后只 ACK，零重复 Store write/authority resolution。same Worker registry/resource
graph 保持不变。真实双 Oxia session 定向测试已接入现有 runner并通过；
source/Broker、clock、historical key/config 仍是 fixture。证据与未完成项见
`evidence/c2-target-publish-recovery-focused.log` 和 `evidence/c2-target-publish-recovery-oxia-real.log`。
恢复枚举/history resolver、Host 生命周期、actual Broker/process/new Store install/Floor 仍开放。


2026-10-08 后续：`TargetPublishRecoveryDiscovery` 通过活动 Worker 的同一 bounded read guard
逐页枚举 ADMITTED budget。SYSTEM/POSITION 首结果、完整 source stamp、原 PUBLISHING ref 与
Owner epoch 必须匹配；缺失/冲突是 integrity failure，不当作没有恢复工作。Page limit 按扫描
rows 计（含已结算/UNKNOWN 项），READ_BUDGET 不推进未完整验证的 row。opaque continuation
绑定 exact discovery 实例和 Store incarnation/native revision，提交/接管后旧 continuation 拒绝。

Reference 固定实际 Admission source、envelope digest、Budget 和原 accepted first result，
`requireImage` 校验完整 fetched signed image/source/Owner/locator/费用。它不证明 Broker history
retention/authentication，也不授予 SEND。RANGE_END 只证明此 cut 下 ADMITTED 初始 Outcome 的
扫描结束，UNKNOWN 的后续 evidence obligations 不因此释放。历史/terminal PUBLISHING ref 不
被静默过滤；相应 UNKNOWN settlement 仍须后续 source path。真实 Broker history fetch、pins/
Floor、Host 自动分页/恢复循环和 production resolver 继续开放。命令与范围见
`evidence/c2-recovery-discovery-focused.log`、`evidence/c2-recovery-discovery-oxia-real.log`。


2026-10-08 后续：K1/P1 native history reader 已按 recovery Reference 取回 exact Admission。
K1 创建独立 manual-assignment guarded consumer，移除 group/static membership、禁 auto commit/
auto offset reset，单条 poll、frame/Fetch/timeout 有界；authenticated Fetch、SourcePosition 和完整
signed envelope 必须匹配。P1 创建独立 NonDurable/inclusive-seek guarded cursor，post-seek 和
receive 的 connection/attestation 保持一致；只核对 exact source/frame，绝不 ACK 活动订阅。
异步创建超时后 late cursor 仍关闭，成功读取也须确认 owned cursor close。

两个 reader 都要求显式 history guard（retention、credential/Owner lifetime），不从 hash/codec
推导授权。K1真实三 Broker 场景连接了 actual Store discovery→guarded history，Worker group
frontier/Store sequence 不变。P1真实 Broker 场景确认 active subscription 保持原消息及 query
subscription 清理；retained Reference 由 declared fixture 反射构造，不能当作实际 Source Store
receipt。生产构造仍 private，没有加 DTO bypass。P1 actual Store→history 组合、protected pins/
Floor、生产 credential/Owner/history provider、Host 自动恢复及 process/new Store 验证仍开放。
详细锁定源码/artifact、命令和边界见 `evidence/c2-native-admission-history-focused.log`。


2026-10-08 后续：Target publish recovery maintenance 已连接有界发现、调用方拥有的异步 history/
policy preparation、一次签名的原 UNKNOWN、exact Worker handoff 与 actual Source first-result
settlement。每 turn 扫一 row 或推进一状态，至多一个 reference/history future/pending mutation；
不新建线程/队列，沿用 Host maintenance tick 与独立 Shard cursor。provider 必须 promptly 返回
CompletableFuture，并在调用方已有有界 I/O executor 上执行历史/权限 I/O，不能阻塞维护线程。

挂起 future 不重新发起 I/O；终结失败保留 reference 供重试。Source 已处理该初始 Budget 时，
必须等旧 future 终结并核对 immutable Budget，再 ALREADY_HANDLED，不伪造 UNKNOWN/释放 evidence
义务。Source settlement 后及 typed stale-cursor 都从头重查，不能推测 rebase 或忽略早插入；
恢复全量读成本尚未优化/计量。Owner/Store guard 每 turn/提交继续检查，late future 在 Owner loss
后零 append。Worker不在 recovery turn外持有自己的 monitor，避免 direct/timer反向锁序。

Host exact-instance configure 和原 timer/fleet 已装配该 loop；历史、time/key/credential/pins/
Floor、实际 native provider 的 deployment inputs 仍必须显式提供。当前真实 Oxia+RocksDB 场景的
history future/source append 是 fixture；尚无完整 native Broker→history→new-Owner UNKNOWN/Host
startup/process/new Store install 的组合证据。命令/状态边界见
`evidence/c2-recovery-maintenance-focused.log`、`evidence/c2-recovery-maintenance-oxia-real.log`。

| 对象 | key | reserved NV type |
|---|---|---|
| TargetChannelIdentity | meta `0e 01 + digest[32]` | 20 |
| TargetScheduleBinding | id `06 01 + digest[32]` | 21 |

两者均为 34-byte 不可变内容寻址 key，不重复创建 Column Family。binding 包含 Message
身份但按 locator digest 读取，旧 generation/attempt 可以保护不同的完整绑定对象。
格式 2 的完整激活和转换仍待 C1/B6/F1；活动 Lane reader/writer 继续使用 format 1 /
NV 1..11，并拒绝有效 CRC 的 NV 20/21。

| 拟支持 managed Target 类别 | channel kind / 恢复域 | Native 和验证责任 |
|---|---|---|
| Kafka at-least-once | BASELINE；source Shard/Target/domain/slot，bounded requests | 无 Pulsar timing；C2/D1 验真实普通发送与资源隔离 |
| Kafka transactional receipt | KAFKA_TRANSACTIONAL_RECEIPT；stable transactional ID、同 cluster receipt、单事务状态 | 不合并 source Shard 的事务/恢复域；D1/D2 验 fence、receipt loss 与恢复 |
| Pulsar at-least-once | BASELINE；bounded source-owned producer | Native 仅在 B3 共同 scope 与相应 guard 完整时可用；D1 认证 |
| Pulsar Managed Journal/dedup | PULSAR_DEDUP_PRODUCER；固定 producer/sequence、exact Attempt Journal | Native managed handoff 继续使用该强能力类别和 B3 policy；不降级为 kind 4 |
| AUTO_FAST 直送、DLQ export | 不属于上述 managed Target 通道对象；kind 4/5 在此拒绝 | 原路径/旧 attempt 按 C5 清单保留或迁移，不能混入 Target Claim/Admission |

以上是契约范围，尚无新 Target runtime 或 Broker 支持认证。基础执行域数仍=1，K>1
的实际候选结构由 E2 激活；公平份额不随 Profile、slot 或 channel 增加。

独立 Python `generate-ndip3-target-binding-channel-vectors.py` 复用既有独立 Python wire
函数，重新生成所有输入，不读取 Java 输出。41 条向量包含四类通道、Schedule/Native/
strict/Prepare/committed object、最大完整记录、key 和有效 CRC envelope；新只读任务
`checkTargetBindingChannelVectors` 纳入常规 check。

完整 membership grant、静态 group/credential/guarantee 声明与 source 注册关联见
[成员授权契约](08-成员授权与source关联契约.md)。认证发放/关闭 wire、完整策略及原 §17.3
验收对应见 [§09](09-成员策略与认证控制契约.md)。本文已
固定绑定/通道字段、冻结规则、teardown 与能力表；C1/C2 的实际 mutation/池/回收及
D/E/F 证据仍按原切片执行，不反向成为 B2 设计验收的运行时前置。

### 晚到 initial Outcome 的绑定读取（2026-10-08）

历史 generation 的初始结果以原 locator、Budget 和 APPLIED Admission 首结果核对绑定，
窗口来自 v4 Admission 的冻结 publication；不借新 generation Message 的窗口或义务。
当前终态首结果要求 terminal 与当前 Message 的 state/runtime 完全相同，保留原 terminal
Source mutation/code。未保存窗口的 v3 historical typed 结果保守保留，需另行重建权威历史。
没有新增 wire/body/NV 版本，也不把 retained proof 变成首发权；范围见
[执行记录](04-执行记录.md) 的 terminal/historical initial Outcome 节。

strict 的 barrier locator 可指向尚有 refs 的旧 terminal generation；投影必须读取精确
terminal runtime/revision/digest。不能用新 Message 覆盖这个 barrier；旧 UNKNOWN 保留，
旧初始明确结果关闭最后 ref 后才重建新 generation head，原水位保持。

恢复 Admission proof 现按原 locator 读取 terminal/historical runtime，而保留的 message()
始终是当前 Message 主记录。两者允许 generation 不同；完整首结果/Budget/source、旧
terminal/原 payload owner 和 strict barrier 必须相接。此为当前 Owner 下的 retained 事实，
不能作为 Applied 首发快照。Checkpoint order 审计也按 barrier locator 核对旧 terminal，
保留水位/引用要求；未决 refs 不因新 Message 或新 Owner 出现而消失。

Target Evidence Resolution 复用原 Admission publication/binding/transfer，service 作者与
Owner 初始 Outcome 分离。cursor 仅 canonical 不等于认证，必须由预解析 domain/history
snapshot 核对冻结请求和各 Source；UNKNOWN 原首结果/预算/ref 是本地结算依据。没有
SEND 权或新 wire，专用 absence/Journal/operator verifier 和 production provider 仍需闭合。

### 2026-10-08：独立 Target Journal cursor 与 PUBLISHED 证据

`EvidenceCursor` 增加 Target-only closed field set `{1,4,5,6,7,8,11}`，field8 的 bounded
TargetEvidenceScope 固定 Shard、Target、domain、accounting incarnation 与 stable Producer
hash；不含 Owner/credential lease，正常 renewal 延续同一 scope。Lane2/3 不出现，所有旧
Lane bytes/排序保持；Target/Lane accessor、sameIdentity/dominance 隔离。字段、128-byte
scope bound、排序与旧 decoder 拒绝规则见 PROTOCOL-REGISTRY；无新 NV/CF/mutation kind。

Target Journal cursor 不再调用拒绝 Target 的 Lane accessor。PUBLISHED evidence 使用既有
kind14/fields1..9（optional10），其 static binding 核对 Target scope/gen/Source partition/
prepared hash/Producer，完整 Journal verifier 另核对 frozen channel/Message/generation/
attempt/Source/artifact 和实际 PUBLISHED member/hash/sequence/cut，嵌入 cursor 与外层
resolution cursor 相同。这里只验证完整 retained Journal 的确切事实，Broker namespace/
retention/accepted service authority 仍必须由预解析 ResolutionAuthority 独立提供。

实际 Worker/RocksDB Source 回归从原 v4 Admission 的真实 Message 构建 Target Journal
mapping、OWNERSHIP_STARTED/PUBLISHED/cursor，再换 Owner UNKNOWN→service Journal Evidence
Resolution，并核对初始结果/预算/barrier。Journal appender/Source/time/key/cursor authentication
为声明 fixture。旧 Lane Journal/cursor 与 shared outcome codec 一并验证；局部80项、主/
测试 Checkstyle 通过。P1 actual Target Journal runner 验证真实 guarded business SEND、SDK ACK、
PUBLISHED marker 和新 cursor/mapping verifier，原 records5/reopen5/renewal/partial replay
拒绝及独立 Admission history 也通过。真实Broker与Source Store验证仍是分开的 slices，
不声称完整 production Host/source/provider、live Oxia/JVM/new Store/ACK-loss组合。

Source/artifact/命令/hash 与精确范围见 `evidence/e5-target-journal-evidence-focused.log` 和
`evidence/e5-target-journal-evidence-p1-real.log`。Target absence、专用 transaction/operator
verifier、完整 cursor checkpoint/Floor seeds/GC、保护性 retention、v3 history、生产装配、
F 迁移清退及集中验证仍必做，C2/B6/D2/E5 与 completion/production flags 不提升。

### 2026-10-08 Target pre-ownership absence 通道绑定

Target Journal absence 的 field2 使用 TargetChannelIdentity，必须保持原 Producer scope/
evidence generation 并递增 channel generation；完整 mapping/Admission Source/retirement/cursor
另行核对。这些字节不构成真实 Broker fencing 或 retention 认证，ResolutionAuthority 必须
独立认证。OWNERSHIP_STARTED/PUBLISHED 不可通过本路径退休；后续缺席协议仍开放。字段与
barrier digest recipe 见 PROTOCOL-REGISTRY，测试范围见[执行记录](04-执行记录.md)。

## 2026-10-12：Source channel 生命周期与当前代

用户已选择同 Shard 的 signed Source 事件，决定来源见[决策记录](decision-record.md)。
`ControlOperationKind` 25/26/27 分别为 CREATE_TARGET_CHANNEL、RENEW_TARGET_CHANNEL、
STOP_TARGET_CHANNEL_NEW_WORK；请求 oneof branch 同号，最低角色沿现有平台资源操作
使用 PLATFORM_OPERATOR，完整 actor/resource scope 仍须认证。APPLY_SHARD_CONTROL
内部 kind 为 24/25/26，保持 CONTROL 作者。Prepared operation 固定一个 SHARD target
index=0，并通过原 ControlTargetMutationBinding 登记精确 body/hash/logical identity。

request schema=1：field1 version；2 完整 NV20 identity；3 prior identity digest[32]
（CREATE 省略）；4 当前请求 OwnerIdentity（受已有 4096-byte 上界约束）；5 STOP reason
（其他操作省略）。CREATE 要求 generation=1 和 slot 不存在；RENEW 要求开放 slot 的
exact prior digest、原 producer/evidence domain 和 generation+1；STOP 保留 exact identity，
不开放新 Admission，也不清理旧代/attempt/Journal/physical/zombie。停止的 slot 不通过
普通 RENEW 重新开放；本实现没有 slot-reset 或删除绕过。旧数据缺 anchor/creation history
仍需受保护的 history/backfill/迁移，不视为可默认初始化的新 slot。

body 保留原 ApplyShardControl fields1/2/3/10–15：完整 Shard、type、retryUntil、
ControlRef、kind、schema1、semanticHash、expected-record-state（CREATE=1、其余=0）与
唯一对应 kind 的 ControlPayload。semantic domain 为
`nereus-delay-target-channel-control` 加 NUL；logical ID 复用 ControlRef 的 kind 派生。

NV43 是不可变 first-applied lifecycle record，META `24 01 + logicalID[32]`：schema1、
完整 body、Source-only TargetQuotaMutation、原 ControlAuthor、lineage[16]、digest[32]，
fields1–6；digest domain 为 `nereus-delay-target-channel-control-record` 加 NUL。
NV44 为 slot 当前代，META `25 01 + SHA256(slot-key-domain + stableProducerIdentity)`；
slot-key-domain 是 `nereus-delay-target-channel-slot` 加 NUL。fields1–7 为 schema1、
完整当前 identity、phase OPEN=1/STOPPED=2、最新 logicalID[32]、Source-only mutation、
lineage[16]、digest[32]；digest domain 为 `nereus-delay-target-channel-slot-record` 加 NUL。

首应用保持实际 Source 严格推进，双签名、注册操作、RBAC/scope、当前 Owner/fencing 和
held materialization/schema/reader/credential/resource authority 缺一不可；上述外部保护必须
持续到 native commit。NV20（CREATE/RENEW）、NV43、NV44、实际 metadata 差额账本、SYSTEM/
POSITION 首结果和 Source 同批提交。重复记录复用原首结果；STOP 不写/删 NV20。

新 Admission 的 append preflight 与 Source 首应用共用 actual Claim/channel 检查，要求
Source-applied slot OPEN 且 exact generation。已 APPLIED 的旧 Admission 继续用其原冻结
身份，续代/STOP 不反向改写它。checkpoint/reopen 独立折叠全部 lifecycle history，核对
每个 NV20 的唯一创建、续代 predecessor 和完整当前 slot，拒绝 bare NV20。NV43 首结果
精确核对原 body/hash/source/author，并进入原 Control history 与 semantic 摘要。

当前只有源码编译开发检查，测试统一后置；真实配置入口/签名注册 provider/lifecycle
提交、SDK/普通及 Native SEND、重开接管的包 1 整链尚未完成。不得据上述 codec/writer
存在授予生产权限；activated canonical schema bundle/eligible readers 必须覆盖新分支及
NV43/44，旧 Lane reader 格式保持拒绝。

包 1 生命周期提交已接配置的真实签名输入与 registration/Source frame 上界；
`TargetChannelControlMutationFactory` 在任何 I/O 前固定完整双签名 Prepared/frame，
restore 只复用受保护历史原 bytes。每 Worker 一个 lifecycle handoff slot；其 WorkClass
action 使用实际 session-bound Oxia immutable registration 和 exact reread，再经原
Shard Log appender 提交，actual first APPLIED/REJECTED 才结清。Broker append success
不是 channel 使用权限；未知注册/append 不换原 frame。当前 slot/SDK权限仍需独立
Store/Owner/credential/资源 gate。Host 正常 drain 在 final checkpoint/Owner release 前
用有限 dispatch/source turn 结清原 lifecycle，pending 明确返回 PENDING_CHANNEL_CONTROL；
新 poll/maintenance 的 pause 拒绝未结清 lifecycle。SDK client/pin/append 物理责任仍由
原 holder 持有，Source first result 不代表 SDK 原生 buffer 已释放。

独立 Native v5/HANDED_OFF 的主会话决策见现有 decision-record；不修改普通 v4。
上述提交接线仍是包 1 的开发部分，完整运行入口与普通/Native发送、Outcome及接管
继续实施。测试/Broker/验收全部后置。


### 2026-10-12 有界普通/Native 发布驱动接线

`TargetWorkerShardRuntime.configurePulsarPublishing` 将实际同日志 appender、OWNER signers、
Target namespace Journal、bounded SDK adapter、Artifact、真实准备/对象解析和最后 gate
组成唯一 `TargetPulsarPublishRuntime`。每个 exact Worker 至多持有一个已 committed Claim；
`TargetWorkerHostRuntime.startPulsarPublishing` 对普通/Native Requests 做同一容量反压，
ClaimConsumer 实际交给相应 Worker，复用现有 ordinary-first loop 和 maintenance fleet
轮转，不创建新的线程/队列。外部 I/O 使用调用方受限 executor/future；Scope/Profile/
credential/lease/Artifact 实际权威仍须在提供者与 Source verifier/最终 SDK gate 中成立，
构造 Inputs 或预选某个 channel 不提供该权威。

实际步骤是从 Store 准备 exact body、签名一次、原 Admission 同 Source append、等待
真实 APPLIED 首结果、重新读取私有构造 Applied proof、按原 payload commitment 解析、
真实 Journal/SDK 普通或 Managed Native 发送、保留原 signed Outcome 和 Source 首结果，
实际结算后才清理该进程 flight。ReadIncomplete 让出 turn；不因 append receipt 或
超时进入 SEND。明确 Admission 首拒绝先按真实 Claim/charge/Message 查询原子撤销；
签名前资格变化的可逆 Claim 同样撤销，无 Source ACK/Admission 成功造假。

正常 recovery maintenance 排除本 Worker 当前 flight 所持的 exact attempt，避免与活跃
SDK 发送竞争制造恢复 UNKNOWN；新 Worker 无旧进程 flight，继续从真实 retained
Admission/Source history 接管。完整新 Owner 未 Admission Claim takeover 入口尚未完成。

Host drain 增加 PENDING_PUBLICATION，有限推进已有 flight/Source action，未结算时不
执行最终 checkpoint、Store pause/close 或 Owner release。未配置、未持有实际权威、
未完成 I/O/fencing 或 Source first result 的路径保持非完成状态。Native Journal
published/absence verifier 已接受独立 Native binding 与实际 contract，复用同一精确
mapping/Source/Producer/cursor 审计；旧 pre-ownership retirement 语义保持。

本批 main compileJava 结束 exit 0、BUILD SUCCESSFUL in 6s；测试/Broker/格式和文档验收
按用户安排后置。包 1 未关闭：完整新 Owner takeover、Journal ownership 后真实
NOT_PUBLISHED 的受保护收尾仍待接通；不能用驱动类/编译结果宣称生产链已验收。


### 2026-10-12 实际 Claim 接管与 Worker 启动连接

`TargetOwnerRecoveryCoordinator` 现增加同 DB 重开的受保护 Source replay 入口；checkpoint
STAGED/INSTALLED 仍必须走原 published Catalog/exact RecoveryPin 校验，不借 local 入口
绕过 restore。新 `configureClaimTakeover` 接收实际当前 authenticated Owner identity 与
受保护完整重放的 Target Journal；Source replay 达到 accepted activation barrier 后，
root/reopen 审计与 durable Owner activation 成立，才逐 Claim 执行 takeover revoke。
缺 identity/Journal 权威不默认空或返回 false，保持恢复未完成。

每 turn 一个 exact Claim、有限 records/bytes/time，撤销同批保留 original generation/
其他 obligation/admissionsUsed/uncertain retry 权限并推进 revision。新恢复 digest 与
当前 Store/cut、原 charge/完整转换及恢复 barrier/Owner/session/lineage 相连。读取或
提交失权、冲突、Journal 发现 ownership、native write UNKNOWN 等均保持失败关闭，
重开后从实际记录继续。普通 revoke 同 Owner/Store 检查保持；旧 Native v1 不补 ref。

只有全范围确定结束及最终当前审计后，`completedRoot` 和 `activeRuntime` 返回，再由
`TargetWorkerShardFactory.createRecovered` 转入 actual accepted Source consumer/Worker。
active Source 构造器拒绝实际未完成 Claim 范围，ACTIVE lease 不是恢复完成证明。首尾
审计覆盖整个合法恢复边界；每个局部撤销只核对本批事实/保护，不重复全库 audit。
组件编译不代表恢复/Broker/权限场景已验证。包 1 仍缺 post-ownership definitive
NOT_PUBLISHED Journal 收尾及其真实跨重启连接。


### 2026-10-13 Target Journal 负终态与跨重启收尾

独立 Target Journal record v2 仅用于 kind 5 RESOLVED_NOT_PUBLISHED；v1 的 1–4 kinds、
原 Mapping canonical/ID/sequence、Lane generation 3 bytes 保持。两个旧 codec 明确拒绝
新增 kind；v2 保留 mapping fields 3–14，field 15 是完整有界
TargetJournalNegativeSettlement。其 schema v1 fields 1–10 为 version、mappingId、
原 signed Admission frame/Source/first result、原 signed Outcome 或 Resolution
frame/Source/first result、完整 PulsarRecordTemplate、独立 digest。完整 Source frames 与
first result/schema/payload 均按声明上限封闭，不把 parsed DTO 当权威。新 record canonical
使用独立 v2 域并包含完整 settlement payload，映射的冻结 Artifact 不改写。

TargetPublishOutcomeStore.readNegativeOutcome 只从实际受保护 Store 取得私有
NegativeOutcome：核对两份实际 APPLIED 首结果、POSITION companion、原 admitted body、
RESOLVED_AWAITING_FLOOR Budget/admission digest/commitment/execution/resolvedAt、root/
lineage 和当前 aggregate；Floor 已释放或首结果缺失不合成证明。完整 mapping/channel/
Target/template/contract/payload hash/Artifact/Source/attempt 身份另外精确相关。

PulsarAttemptJournal.resolveNotPublished 必须取得上述私有 proof 与独立实际 history/
evidence/writer/physical/retention/pin authentication，源已决后才 append；状态只允许
OWNERSHIP_STARTED → kind 5，与 PUBLISHED 互斥。durable position 和 held rights 复核
后才撤销所有 SEND/retransmit 资格并清 unresolved slot，后续 sequence 仍严格加一。
原 pre-ownership retirement 不接受新负终态，也不反用该记录认证产生它自身的 Source
NOT_PUBLISHED。append/ACK UNKNOWN 或失权沿 writeUncertain/fence/reopen，不先放开。

实时 TargetPulsarPublishExecutor 在 Source negative APPLIED 后仍保留 flight，等待受限
I/O executor 上的 durable settlement future 完成。后台
TargetJournalNegativeSettlementMaintenance 从实际 retained Budget 的 resolvedAt 找原
Admission/Outcome/template，不只扫描 ADMITTED，覆盖 OWNER live rejection 和 SERVICE
UNKNOWN resolution、Source 已决而 Journal 未收尾的崩溃窗口。每 tick 一行/一项 future，
复用原维护 fleet，活跃 flight 交自己的结算路径；扫描 cut 改变重启，完整 range end 与
源序列同一切点核对后才宣告 sweep 完成。无新线程/全局队列、无第二次 Source 扣费。

Host configured startup 要求 live settlement 与 retained cleanup 且共用同一 Journal；
normal drain 使用 PENDING_PUBLICATION 等待 cleanup，Source/GC 后复查再做最终切点。
metadata-only checkpoint 写不冒充新 Source negative；源序列推进使旧 sweep 失效。
物理/zombie/receipt/Journal/evidence 保护和实际 Floor 后删除仍分别处理，不因终态释放。

真实 P1 PulsarClientArtifactAttemptJournal 增加带独立 NegativeReplayAuthority 的
openTarget 入口。新 writer/reader 必须在 TargetWriterGuard.requireRecordVersion 取得
实际 covering schema/Artifact/eligible-reader activation；旧 functional guard 默认拒绝
v2。replay(kind5) 必须独立 authenticate 原 Source/history 并保持 rights；重复（同物理
position 或 ACK 丢失后的不同 position）先匹配完整 settlement bytes，内容冲突 fence。
旧 replay 不能解析新 kind 后直接开 next mapping。

此批只有编译开发证据，没有测试/Broker/生产激活证明。真实提供者权利、资源额度、
checkpoint/恢复/删除统一装配仍按四包继续；包 1/全部切片不因这个分支闭合而完成。


## 2026-10-13：Target BASELINE 实际 SDK 与 Source 单 flight

`TargetOrdinaryPublishRequest` 仅从私有的实际 APPLIED Admission proof 产生，核对当前
PUBLISHING Message、exact attempt/revision、payload length/hash 和 Artifact。独立 Target
入口不构造 Lane 身份；`PinnedKafkaDestinationAdapter`/`PinnedPulsarDestinationAdapter`
只将 BASELINE_PRODUCER 交给该入口，旧 transport 默认失败关闭，强能力不降级。
`BoundedDestinationPublishAdapter.submitTargetOrdinary` 使用原 Target channel 物理预算，
并把最终 ownership gate 传到真实 SDK 调用之前。

真实 K1 `KafkaClientArtifactTargetDestinationTransport` 用 request-level cluster/Topic UUID/
partition guard；`KafkaClientArtifactTargetRecordEncoder` 保留 key、重复/有序 caller headers、
可选业务 event time 与 Protocol Registry 定义的九个 opaque reserved headers。P1 ordinary
encoder 保留 partition key 编码、ordering key、caller/reserved properties、event time 和原
payload，不设置 Native deliverAt 或 Journal sequence。P1 transport 核对真实 producer name
与 resource attestation；K1/P1 都在构造最终 SDK record/builder 后执行同一个 final gate。
实际 guarded 回调产生既有 typed ACK，核对精确 Broker resource/position/attempt/prepared
hash；未确定 SEND/回调错误继续 UNKNOWN。历史 Lane request/record bytes 保持。

`TargetOrdinaryPublishExecutor` 只提交一次 SDK，保留已签原 Outcome bytes 并重试 Source
handoff；只有实际 exact Source APPLIED 首结果才能释放本进程 pending flight。UNKNOWN
进入原 Source hold，physical/zombie 与旧 Attempt budget 不由该释放动作清除。
原 `TargetPulsarPublishRuntime` 复用既有 Claim→Admission→payload→Outcome driver，新增
明确 BASELINE endpoint 分支；`configureBaselinePublishing`/`configureBaselineEndpoint` 与
`Host.startPublishing` 连接同一个 DRR/maintenance/drain，不增加线程或队列。纯 BASELINE
不选择 Native；未配置 channel kind 在签 Admission 前撤销实际 Claim。

本批仅执行必要编译：compileJava、compileRealKafka、compileRealPulsar 通过；K1/P1
使用当前锁定客户端 artifacts。没有运行单测、Broker、格式/文档或集中验收。实际
Profile/key/time/credential/provider/resource 启动装配、K2、真实重启恢复与最终故障验收
仍开放；typed ACK DTO 和可注入 ResultAuthority 不等于独立生产证据认证。


## 2026-10-13：K2 Source Admission v6 与 receipt 恢复域

主会话批准独立 Target K2 namespace，并裁定唯一 durable mapping 边界为实际 Source
Admission 首应用批次。Kafka receipt topic 不写非事务 MAPPED，不增加 Journal 后端。
Target v3/v4/v5、旧 Lane mapping/receipt/cursor bytes 保持；v6 只用于普通 Kafka
KAFKA_TRANSACTIONAL_RECEIPT。新增独立 ProtocolTuple body generation=6，沿 v4 fields
1–23，required field24 是 TargetKafkaReceiptBinding v1。旧 v4 K2 缺上下文的历史 Admission
可按原权限读回，不能补字段改签、重造 first-send permission。

K2 binding 的 closed fields1–9 为：schema1、target physical topic、原 Registry
KafkaReceiptSlotResource、receipt physical topic、slotsPerShard、receipt partition、真实
protected scanStartOffset、同次观察 digest[32]、retention protection digest[32]。两个 Kafka
物理名各≤249 ASCII bytes，资源 cluster≤256 UTF-8 bytes，binding≤2048 bytes；slot 物理
几何及同 cluster/source Shard/transactional channel 在解码/first application 核对。
签名的数字/digest 不是观察或保留权威；实际 MaterializationAuthority 必须独立认证。

NV45 TargetKafkaReceiptMapping v1 位于 META `26 01 + mappingId[32]`，closed fields1–7
为 schema1、原完整 v6 body、原 mutation hash、actual Source TargetQuotaMutation、非零 raw
uint64 logicalSequence、lineage[16]、mappingId[32]。mappingId 是 SHA-256(UTF-8
`nereus-delay-target-kafka-receipt-mapping` + NUL + canonical fields1–6)。不反向依赖 APPLIED
结果 hash，不改原 signed body/envelope/prepared hash。NV46 TargetKafkaReceiptSlot v1 位于
META `27 01 + slotId[32]`；slotId=SHA-256(UTF-8 `nereus-delay-target-kafka-receipt-slot` + NUL
+ stable producer identity)。closed fields1–6 为 schema1、最近分配的完整 channel、lastAllocated
raw sequence、optional pending mappingId、actual Source stamp、lineage。FREE 省略 field4，保留
counter；channel renewal/换 Owner 不换 slot。logical sequence 独立于 Kafka PID/内部 sequence。

首次 Admission 同批消耗 exact Claim/原 charge，保存 Message/Attempt budget、immutable
mapping、pending slot、SYSTEM/POSITION 首结果、Source frontier、实际 record delta 账本。
busy/exhausted slot 沿稳定 STALE 拒绝路径，仅推进结果/Source；不部分消耗 Claim、不阻塞
Source 等待 slot。只有实际 APPLIED/OK first result、映射及 pending slot 完整关联才产生
私有 Applied proof。新/历史 record 的冻结 Target accounting 以既有 STATE class 计实际
key/value 字节，不增加维度、不重复收费业务 payload、不重定价原 Attempt obligation。

Source definitive Outcome/Resolution 独立认证 evidence 后，在同批更新 exact pending slot、
Attempt/Message/结果及费用；UNKNOWN 保持 pending。正向只接受 Target K2 同事务
read_committed receipt，拒绝普通 Produce ACK；负向必须为 protected transaction/receipt
absence，不能以单个 target Broker rejection 清 slot。旧/重复结果仍走 immutable first result
和 retained attempt 分支，不能清掉后续 mapping 的 slot。逻辑 FREE 不解除 physical/zombie、
receipt/pin/query/Floor 保护。checkpoint full audit 核对 mapping→原 Admission/first/budget、
channel history、唯一 slot sequence、pending 或 actual settling Source result；semantic 原
Admission inputs 再比对 mapping 的 original signed bytes/Source，缺失/多余/替换失败关闭。

Target Kafka cursor 新 branch 保留 fields1/4/5/6/7/8，field8 为 TargetScope，field10 为既有
Kafka cursor member；不带 Lane fields2/3。旧 Target Pulsar field11 branch 及 Lane branch 不变。
Target receipt key=SHA-256(UTF-8 `nereus-delay-target-kafka-receipt-key` + NUL + mappingId)，
每个 mapping 独立 key；value 是 UTF-8 `nereus-delay-target-kafka-receipt-value` + NUL + closed
schema1 fields1–8：version、mappingId、logicalSequence、attempt、prepared hash、actual Source、
stable transactional ID hash、K2 binding hash。wire record hash 使用独立
`nereus-delay-target-kafka-receipt-wire-record` + NUL + lp32(key) + lp32(value)。成功证据复用
既有八字段 KAFKA_TRANSACTIONAL_RECEIPT shape，静态比对完整 mapping 及 receipt/cursor/LSO。
静态 decoder 不授予独立 Broker/retention/fencing authority。

已开发 actual K1 `KafkaClientArtifactTargetReceiptReader`：从 frozen scanStartOffset 读取，
一 poll/turn，固定 UUID/partition/read_committed、有限 records/bytes/time、explicit continuation、
真实 guarded record/LogAppendTime/LSO 和 exact key/value 校验。可找到 receipt 并返回私有
实际 Observation，恢复不依赖旧 callback/已知 receiptOffset。empty poll 不造否定证据。

本批 compileJava/compileRealKafka 通过，未执行测试。真实 K2 transaction first-send/资源
factory/driver、受保护初始空 range anchor、完整 fencing/absence 及 provider/startup/reopen
仍待连接，包1未完成。已确认原锁 K1 缺 logStartOffset 投影并丢失实际空 Fetch 证据；
主会话批准在既有 K1 worktree 最小 client 扩展、普通 fork 分支发布和开发锁更新，原
05849884 历史证据保持其原 scope，不修改 Broker wire或提前宣告新组合已验证。


## 2026-10-13：K1 同响应 log start 与 guarded 空 Fetch 开发锁

包1必要依赖已在既有 K1 worktree/既有 fork 分支完成并普通发布：
`nereus/delay-guarded-producer-v1@5128f46a358b5c3e9991cd5a51f2d1d0ef15339a`。
仅三处 client 路径：GuardedFetchEvidence 新 ctor/字段、AbstractFetch 同次响应投影、
ClassicKafkaConsumer 两处 guarded-only evidence 返回条件及当前 guard 复核。普通
Fetch.isEmpty/未 guarded poll、Broker RPC/wire/Producer API 保持。旧 ctor 明确
logStartOffset=-1 unavailable；新值参与 equals/hash。没有 lastEvidence cache，真实
timeout 的空结果无 response proof，旧 seek/assignment/error 检查仍由原 collector执行。

K1 `:clients:jar` 必要构建通过；该任务实际以 shadowJar 产物完成，jar task自身 SKIPPED，
不冒称额外 gate。显式 `-PcommitId=5128f46a358b5c3e9991cd5a51f2d1d0ef15339a` 使
kafka/kafka-version.properties 记录 `5128f46a358b5c3e`。当前 SDK JAR 为
`clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar`，SHA-256：
`7adbb8e6d3bfa76fc62064d598d25ff00ee70cd90b27ddedbde5ed8f878146fe`。
这是 client 身份；Broker/image 未重建，旧 05849884 receipts 保持其原证据 scope。

Delay K2 reader 使用新 API，区分 null 无 response、实际空 raw/filtered response 和
普通非空 receipt；同次 logStartOffset/LSO/HW 缺值、矛盾或 logStartOffset 超过原
scanStartOffset 时失败关闭。新 poll 前清空本 turn range observation，不把旧响应
挂到 timeout；空响应仍不自动推进 nextOffset/LSO，也不产生 NOT_PUBLISHED。
实际 SDK JAR 的有限读取核对 version 来源并计算 SHA-256；此 hash 还须由实际
Artifact/eligible-reader/Materialization protection 与激活值核对，不由源码声明授予权威。
新 K2 range observation closed canonical image version2 的 fields1–18 为 version、
完整 K1 commit ASCII、cluster、physical topic、UUID[16]、partition、requestVersion、
raw correlation ID、broker node、session、fetchOffset、first/last raw record offset、HW、
LSO、logStartOffset、authenticated response SHA-256、actual SDK JAR SHA-256。数字
unknown 使用明确 -1，不默认零；原 persisted v1 evidence encoder/含义保持。

未运行 K1 单测、checkstyle/spotless、Broker 或 Delay 测试。原必要检查和新增
empty/timeout/旧 ctor/-1/seek/assignment/guard/position、原始或过滤后空批次、LSO及
truncation/compaction 的实际边界均留待集中验证。K2 transaction/driver、初始 anchor、
完整受保护否定与新 Owner 恢复装配继续开放；client编译不证明这些路径或生产权威。


## K2 持久 CONTROL backing、Source adoption 与原对象恢复（2026-10-13 开发）

`KafkaClientArtifactTargetReceiptManagement.open` 用实际配置构造 Admin，并核对配置摘要与
source-bound deployment image；原 registered CONTROL、历史 issuer 与显式整 topic/receipt
scope 终止信任链，实际 current PLATFORM_OPERATOR 才可管理。闭合管理 image 保留 exact
cluster/topic UUID、capability、epoch、管理/Worker principals、部署根、配置 fingerprint 与
权限隔离依据，不增加新的公共 ControlOperationKind 或逐级自授权 grant。
管理组件与同 session-fenced catalog/trust 一次装配；Worker 仅收到 typed AnchorPublisher。

2026-10-16进一步明确：历史MANAGEMENT只按原注册Prepared/Source/key/signature及原作者/
角色/精确scope认证，不调用当前operator gate；当前管理动作另核对root真实认证器的
一致subject/role/scope/version视图及同整topic、对应动作权限。不同获准actor或无关
角色/scope投影变化不要求伪造为历史作者。current policy绑定原完整management image，
包含controlScope/deploymentRoot/UUID/物理topic/epoch/capability/credentials/principals；
PLATFORM角色、旧artifact bytes或一个scope hash均不足。副作用和catalog CAS前后重验，
当前证明失效拒绝动作但不因此写BROKEN；物理连续性、不复活和原签名/私钥持有契约不变。
此为授权核验边界，不改变上述持久image字段、Binding或Admission bytes。

同一 catalog 的有界物理容量 root 先持久登记 INSTALLING 和本次 installationID，再发实际
ACL/config Admin 操作，最后核对 exact UUID、整 topic 配置、完整有限 ACL 和原 capability
前提，CAS/reread ACTIVE。历史 INSTALLING 或 BROKEN 不重发未收敛请求、不倒补连续性。
管理固定 cleanup.policy=delete、retention.ms/bytes=-1、LogAppendTime、禁 unclean leader
 election；Worker 有 READ/WRITE/DESCRIBE，并明确 DENY DELETE/ALTER/ALTER_CONFIGS/CREATE。
后续实际观察失败或 policy 变化持久标记 BROKEN；签名 blob、当前配置快照或 Oxia CAS 本身
均不冒称远端 Admin fencing。部署前提与真实权限/Broker 验证仍须集中验收。

保护/anchor 保存原完整 CONTROL image 与签名，不跟随 Owner lease 过期。canonical schema、
签名域、字段与有限上限见 PROTOCOL-REGISTRY 对应开发条目。原 Binding v1/Admission v6
bytes 未改。保护首次实际 Source adoption 的 primary 是原 APPLIED Channel Control 冻结
TargetQuotaIncarnation；anchor primary 是首个实际 Source Admission 的 Attempt 冻结 owner。
相同摘要只能在原 Source/Target/recovery/accounting 域内引用，不隐式转移 owner 或重定价。

发布原对象前持久 capacity/publication intent，未知写必须 exact reread；物理储存额度有明确
配置，保持全 key/value 成本，adoption 后也不凭逻辑标志释放。Source enqueue 前同时持久
绑定原 signed Admission ID/hash/retry scope，防止可能已入 Source 的对象被当无引用孤儿。
实际 Source verifier 预加载原 catalog image 和 prepared 绑定，Store 内仅核对冻结输入；
Admission 同 native batch 写 NV47 unique owner、NV48 mapping refs 和 anchor-by-attempt
实际 alias，以及原 Claim/Attempt/Message/mapping/slot/结果/Source 与双账本。

`TargetPulsarPublishRuntime.Inputs` 的 K2 分支必须提供 ReceiptPublications；单 flight 等待
prepared bind 后才提交 Source，actual APPLIED 后再从 SYSTEM/POSITION、mapping、owner/ref
实际行构造私有 Adoption 并 CAS 对账 catalog，随后才进入 payload/SDK。失败保留同 signed
image，下一 caller turn 可对同 ID 重读，不换 attempt、不删原对象。Reader 与 cold factory
也通过实际 Worker/Owner/native proof 重读并对账 adoption；冷启动仍按原两 digest 和原
scanStart，不重新采样或补发旧 SEND。缺少新 owner/ref 闭合的历史 v6 不获得首次 SEND 权。

checkpoint 全库折叠增加 owner/ref/alias/mapping/首 adoption/原 Channel Control 关联，schema4
companion 保存每个原外部对象完整 bytes 一次。在 Store 锁外用实际 catalog 验证原签名、
exact key/hash/length/CONTROL/Source，恢复/Manifest 必须完整匹配，不能抄 aggregate/counter。
BROKEN 的原 bytes 仍可审计，但不授予当前 SEND 或 absence 权。Floor/retirement/共享 topic
引用解除、原 Prepared writer 收口及实际删除仍在包2/4接线；catalog 当前拒绝无证据解除。

本批源码编译通过，不代表全部包1/29切片完成。完整服务启动配置、物理资源/I/O关闭、
最大状态 envelope、Broker 与跨进程恢复、测试/style/docs 仍开放或集中后置；未改当前生产
配置，未执行迁移/删除/保护解除。全部 completion 与 production authority flags 不提升。


2026-10-13 SDK启动/关闭接线：KafkaClientArtifactTargetReceiptRuntime使用实际Source-applied
当前channel slot、immutable identity及Target；新初始化要求当前generation，cold模式允许
当前STOPPED scope恢复已存在mapping，但不授普通SEND。已拥有的receipt使用retained
identity/producer domain，不因后续STOP/renewal删除原数据。实际provider/CONTROL root仍是
签发和current resource/profile/credential/SDK检查来源，纯Settings不授权限。

同Worker SDK pool的finite vector覆盖connections/producers/threads/FD/buffer/direct/other
native；构造前预留，timeout/取消view/失败close不释放。创建后失权/未transfer关闭原SDK，
失败携私有handle与可重试cleanup；receipt与cold reader沿同池，original mapping/scanStart
不变。设置加载明确operation/poll/records/bytes/broker/buffer/physical bounds和完整SDK
成本，不取无限sentinel或未知项。broker拓扑/SDK内存真实envelope仍须集中实测/验证。

transaction关闭序列化queued no-start和实际running I/O；running reader未退出不冒称close
完成。producer和reader实际退出后才完成原悬挂physical UNKNOWN stage；SDK客户端费用
与单请求physical charge分别保持。Host先结清retained recovery/pub/Source，后等待原SDK
关闭、physical window为空才注销channel，再做final checkpoint/Store/Owner drain。原
CONTROL Admin/shared executor属于其原owner；关闭SDK不解除catalog保护、staging、
原Attempt/ref或删除任何资源。全服务/多通道生产配置和集中验收继续开放。


2026-10-13 Target多通道与P1关闭开发补充：TargetDestinationMux使用完整immutable channel
identity选择端点，ordinary/managed/native prepared提交保留实际publication和SDK前的
final gate，不能只凭producer name或续期后的当前channel处理旧generation。BASELINE
opening读取实际Source/Target、Profile/credential及loaded SDK hash；READY和失败/STOP
cleanup沿同Worker资源图，派发表移除要求实际native close和physical窗口已空。

P1 Producer重复close观察原操作；Closing、Closed、SDK view取消和close timeout均不是
新的成功结果。晚到的producer创建只关闭对应Producer，原close包含schema、任务、注册、
pending callbacks/buffers和该creation的关闭结果。Broker close失败后仍执行安全本地
清理并保留失败；不会据连接失活合成Broker确认。Delay保留原future和failed handle，
Journal创建后失败也携带cleanup义务。共享client的显式owner关闭只能在所有相关用户
quiesce后执行，实际owned executors未terminate时保留窗口/原资源；清理不撤销UNKNOWN、
Journal、Source、credential和retention义务。P1 managed factory及服务root装配继续开放。


2026-10-16 Target RECOVERY独立用途：冻结channel/Admission/mapping和原lease bytes
不变。当前恢复identity/材料/权限使用独立签名部署授权，不能用Source RECOVERY或
仅SEND scope证明替代；原历史合法性、当前恢复权、连续receipt保护是三个独立条件。
签名可以限定原channel/receipt/txn-id，actual root再以私有receipt绑定实际pending
mapping/Store cut/Owner/session/SDK opening，禁止caller自造或跨会话复用。
当前purpose-aware codec、issuer tuple trust、actual mTLS/probe loader及持有hold的
专用receipt已实现，实际typed无SEND SDK/resource/service及公共证据保留链尚未接通。
恢复native接口只可lookup、原ID initTransactions、原范围read_committed和关闭；
prepareBinding/anchor/newAdmission/slot/SEND/commit均不授予。每个native调用与证据
交付前持续核权限、原pending/Owner/连续保护；授权到期阻止新动作，原句柄/UNKNOWN/
费用/hold保留到真实退出。ordinary SEND路径继续核原lease，没有自动恢复fallback。


2026-10-16恢复runtime私有绑定已接actual Source：KafkaRecoveryPending在同一次
guarded native read核原首结果/slot/未释放义务/外部证据owner，捕获Store identity
与cut、native/source sequence。持续原Owner/session、exact原mapping/slot及单调
cut重核后才执行guarded lookup/同ID fencing/receipt poll；normal flight和仍未退出
的同stable producer SDK禁止并行。新授权deployment/tenant另与实际Owner/Source
tenant核对，不能仅因另一个Trust对象自身一致而跨realm。
独立typed资源没有业务SEND/anchor/raw producer返回面。内部Fence v2保留v1原字段，
新增当前恢复公共证明与actual Store/cut上下文；外层Source首结果及mapping编码不变。
public resolution authority仍需实际UNKNOWN及private scan observation，持久归档/
SERVICE Source结果与正常新工作接续尚未闭合；组件编译不证明端到端恢复或整包完成。


2026-10-16新工作接续使用独立normal入口：原恢复Source已结算、原SDK/reader/hold
实际退出后，在同Host私有construction许可下重核Owner、原OPEN channel和普通
SEND材料，再新建normal runtime并重走fresh anchor/fence/READY/dispatch。恢复
材料/typed SDK不改用途，也不替代旧普通lease。closed/replaced原channel只退场，
跨代当前OPEN选择仍是后续完整编排义务。

已批准的新durable恢复证明必须被精确签入Source结果ref，不能仅按evidenceId查
“当前版本”、修改旧anchor或靠旧进程闭包认证。RECOVERY_PROOF独立于CONTROL，
内容寻址/用途签名/原观察Owner/时间/SDK与完整公共验证材料持续可历史认证；新动作
仍须当前恢复授权。旧body/旧证据/evidenceId及原Admission/mapping语义保持，新
proof-bearing body只在显式支持它的protocol tuple/reader capability下接受。
归档/Source原子owner-ref/Checkpoint/Floor扩展尚待实现，权限/保留/费用不得简化。


2026-10-16新增proof-bearing Source body generation7格式：field9=7与必填field19
精确RECOVERY_PROOF ref共同出现，Ref固定原mapping/evidence/attempt、完整key和
proof/value digest/实际length。UNKNOWN不携ref，原无marker body/evidenceId/
Admission/mapping及签名身份不重释。原body字段继续参与原SystemMutation body
hash/签名，新的exact ref亦被签入；v7须独立tuple/reader支持。当前Source在真实
认证snapshot、原子owner/ref/冻结费用接通前拒绝采用v7，不能以普通内存观察或
仅解析成功替代durable证明；其余归档/Source/Checkpoint/Floor义务仍开放。
历史恢复Auth验签只核原观察时间与原配置/issuer/key/scope，不要求旧generation
今天仍active；新恢复I/O继续使用原active和实际current hold gate，二者不混用。


2026-10-16独立恢复归档签发只能接actual runtime私有ArchiveObservation，观察来自
当前Owner/pending/保护/材料仍有效的typed native扫描；SERVICE标签或任意DTO都
不能签发。新观察schema3/4携有界逐段实际Fetch/processed continuation/时间折叠；
旧普通入口schema1/2不改。proof完整保留原signed Admission/mapping/evidence、
observer/时间、SDK source/JAR、冻结Attempt/incarnation/capability与公共材料。
历史验证按原目的issuer/key/配置/时间和PKIX，旧Owner/凭据今天失效不抹除历史。

Catalog先预留同物理Root/blob预算再写不可变实物、完整认证精确回读，然后签入
Source ref并持久seal该frame的ID/hash/retry。mapping+evidence固定选择且冲突拒绝；
崩溃续接只接受实际存储的原选中blob。publication/prepared snapshot均不等于实际
Source adoption。部署Root及archive-aware Source链尚未接通，v7 Source仍拒绝，
typed恢复已关闭旧内存resolution旁路。原子Native owner/ref、完整冻结费用、
Checkpoint/Floor/保护退役仍必做；没有生产恢复可用或整包完成声明。
