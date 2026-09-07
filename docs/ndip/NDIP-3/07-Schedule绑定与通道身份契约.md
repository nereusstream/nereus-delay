# NDIP-3 B2：Schedule 绑定与通道身份契约

Status: Draft / IN_PROGRESS

本节扩展 [执行/控制兼容契约](06-执行与控制兼容契约.md)。已实现精确绑定、不可变通道
身份及静态投影/续期检查；完整 membership grant 对象和权威仍未闭合。C1/C2 承担
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
| 13 | 非零 membershipGrantRef[32]；完整 grant 对象/权威是 B2 剩余工作 |
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

B2 尚需定义并验证完整 membership grant、source-bound group completeness、共同
credential/guarantee 授权对象及 source 注册关联，然后按原 §17.3 验收整个 B2。本文已
固定绑定/通道字段、冻结规则、teardown 与能力表；C1/C2 的实际 mutation/池/回收及
D/E/F 证据仍按原切片执行，不反向成为 B2 设计验收的运行时前置。
