# NDIP-1：显式 Pulsar Native Delivery 与 Handoff

- Status: Accepted
- Authors: Nereus Delay maintainers
- Created: 2026-08-27
- Discussion: `docs/ndip/NDIP-1/`
- Baseline: `main@8915d21ed325a90ec305201ca85ab8daea3803dc`
- Supersedes: Gate B PASS 后有界修订当前全局 not-before、固定 Handoff lead 和 Pulsar guarded timing 约束；Gate C 仍保护 deployment
- Superseded by:
- Implementation status: H0 implemented (fail-closed)；Managed Handoff 与 AUTO_FAST 物理 record 链路仍未闭环

> NDIP 表示 Nereus Delay Improvement Proposal。`NDP-0002` 与本提案已于 2026-08-27 经
> 维护者接受，final receipt 已绑定 post-transition exact normative package。Gate B PASS 授权
> H1-H6 按 predecessor 顺序实施，并允许 exact disposable local tests；Gate C 仍保护 persistent
> deployment、SHADOW 和 ENABLED。

> 当前 NDIP gate 由 Accepted `NDP-0002` 注册。本文的 Accepted authority 由最终
> `acceptance-receipt.json`、README 定义的 exact normative package digest 和维护者决定共同
> 证明；不能通过直接修改 `Status` 或只运行 digest verifier 自我授权。

## 摘要

本提案把 Pulsar Handoff 从“ordinary Managed 的透明优化”改为显式选择的原生投递契约。
系统继续提供由 Nereus 承担的 not-before；只有消息明确允许 Pulsar native delivery 时，才可
在业务 `deliverAt` 前向 Pulsar 持久化，并在目标 Message 上设置 Pulsar `deliverAt`。

同时，本提案完成 Managed Handoff 的 physical record、Attempt Journal、P1 Resource Guard、
真实 SEND/ACK evidence 和 recovery 闭环，并让 AUTO_FAST 与 Managed Handoff 受同一显式
消息策略约束，但保持两者不同的 ownership/unknown 状态机。

Gate B 前只实现了 H0：所有 Managed early request 和有效 AUTO_FAST native request 都在
Producer ownership 前 fail-closed。当前 Gate B 已 PASS，H1 READY；H2-H6 依次等待前置 slice。
当前没有真实 persistent deployment，G0 为 `NOT_APPLICABLE_FOR_IMPLEMENTATION /
PENDING_DEPLOYMENT`，不阻塞实现，但 SHADOW/ENABLED 仍等待 Gate C。

## 动机

当前设计已经允许 `actionAt = deliverAt - handoffLeadMs`，Publish Admission 和 Worker 也能
携带两个时间，但真实 Managed Pulsar transport 只调用
`newMessage().value(payload).sendAsync()`。因此 timing state machine 与实际 target record
不一致。

单独补 `.deliverAt(...)` 仍然不足，因为当前链路同时缺少：

- 无损 Prepared Descriptor 到 physical request 的投影；
- actual delivery contract 的 Admission 绑定；
- key、ordering key、properties 和 event time 的 closed encoding；
- production Attempt Journal mapping-before-send 和固定 sequence；
- P1 实际 SEND command hash 到 Delay evidence 的投影；
- policy lease、replay、Disable 和 unknown outcome 的完整边界；
- AUTO_FAST 与 Managed Handoff 的统一 opt-in 权限。

## 范围

- 定义 Managed not-before 与 Pulsar native delivery 两种产品契约；
- 引入 required、default-forbid 的 `NativeDeliveryPolicy`；
- 将 Destination Profile 的固定 lead 改为最大允许 lead；
- 引入签名、有界、自包含的 runtime policy snapshot；
- 在 Publish Admission 冻结实际 delivery contract；
- 扩充 closed descriptor 和 Pulsar prepared record；
- 接通 Managed Pulsar Attempt Journal 和固定 sequence；
- 使用 source-locked P1 Resource Guard 发送真实 Pulsar record；
- 替换 Pulsar ACK evidence closed branch；
- 保持 AUTO_FAST 独立的 ownership/uncertain 状态机；
- 以直接替换或一次性迁移完成数据切换。

## 非目标

- 不新增 Pulsar Broker Visibility Guard；
- 不修改 Dispatcher；
- 不创建 Protected Topic；
- 不发现、认证或限制 Subscription；
- 不把 delayed-delivery enabled、strictness、tick、Broker clock、TTL 或 retention 作为
  Nereus correctness gate；
- 不承诺 `PULSAR_NATIVE_DELIVERY` 跨 Subscription not-before；
- 不宣称原生投递支持严格消费顺序；
- 不新增 Handoff Scanner；
- 不建立永久双轨、并行产品线或无限期兼容 reader；
- 不根据 `actionAt < deliverAt` 猜测业务 outcome。

## 当前约束

1. 当前 runtime 权威设计仍将 `deliverAt` 定义为 earliest consumer visibility；只有完成 H1
   contract slice 并同步对应权威文档后才能按本 Accepted NDIP 有界修订。persistent deployment
   仍必须等待 Gate C。
2. 当前 `PulsarDestinationTimingPolicy.certifiedHandoff` 只验证固定时间差，不构成完整
   capability authorization。
3. 当前 Managed real transport 不设置 Pulsar `deliverAt`，也不应用完整 metadata/sequence。
4. 当前 `DestinationPublishRequest` 不是 Prepared Descriptor 的无损 physical projection。
5. `PulsarAttemptJournal` 只有 local protocol seam，production transport 未接入。
6. 当前 ACK branch 缺少实际 SEND command hash 和 prepared record hash。
7. AUTO_FAST 已经进入独立 Producer ownership/uncertain 状态机，不能合并为 Managed retry。
8. P1 client/Broker artifact 是 source-locked 依赖，不是 stock Pulsar client。
9. AUTO_FAST real transport 当前把整个 prepared envelope 当作业务 payload，且同样没有设置
   Pulsar `deliverAt`。

## 提议设计

### 1. 产品契约

#### `NEREUS_MANAGED_NOT_BEFORE`

```text
Nereus holds until deliverAt
    -> ordinary target publish at/after deliverAt
    -> no Pulsar deliverAt field
```

`deliverAt` 继续表示消费者 not-before。

#### `PULSAR_NATIVE_DELIVERY`

```text
Nereus may persist before deliverAt
    -> Pulsar Message.deliverAt(deliverAt)
    -> Pulsar owns subsequent delivery behavior
```

此契约明确接受 Pulsar 原生 Subscription、tick、strictness、Broker clock、TTL 和 retention
行为，包括 Exclusive/Failover 可能在持久化后立即消费。

### 2. 消息级 opt-in

```java
public enum NativeDeliveryPolicy {
    FORBID,
    ALLOW_MANAGED_HANDOFF,
    ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF
}
```

它是 required wire field；缺失、unknown value 和 malformed encoding 一律 fail-closed。

| Policy | AUTO_FAST | Managed Handoff |
|---|---:|---:|
| `FORBID` | 禁止 | 禁止 |
| `ALLOW_MANAGED_HANDOFF` | 禁止 | 允许 |
| `ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF` | 允许 | 允许 |

默认行为等价于 `FORBID`，但 canonical wire 仍必须显式写出该值。

### 3. actual contract 冻结

Schedule 的 `NativeDeliveryPolicy` 表示允许范围；Publish Admission 必须冻结本次实际
`DeliveryContract`：

```text
ordinary due / native fallback before ownership
    -> NEREUS_MANAGED_NOT_BEFORE

Managed Handoff
    -> PULSAR_NATIVE_DELIVERY
```

AUTO_FAST 在自己的 `NativePreparedDelivery` 中绑定 native contract。`HANDED_OFF` 只根据
Admission/Prepared 中的实际 contract 与已验证 PUBLISHED evidence 投影，禁止根据两个时间
的大小关系推断。路径由外层 Managed Admission 或 AUTO_FAST prepared union 表达，不增加
独立 `DispatchMode`。

### 4. Ordering

第一阶段只支持：

```text
BEST_EFFORT + PULSAR_NATIVE_DELIVERY
```

以下组合在 ownership 前失败：

```text
DELIVERY_TIME_FIFO + NativeDeliveryPolicy != FORBID
    -> ORDERING_CAPABILITY_UNAVAILABLE
```

### 5. Profile 与 runtime policy

Destination Profile 保存 `maxHandoffLeadMs`，不再把一个固定 lead 当作所有消息的最终
发送时间。Kafka 和 ordinary-only Profile 的最大值为零。

runtime authority 签发 closed `HandoffPolicySnapshot`：

```text
policyScopeDigest
generation
mode = DISABLED | SHADOW | ENABLED
effectiveLeadMs
validFromEpochMs
validUntilEpochMs
issuerKeyGeneration
issuedTrustedTimeEvidence
snapshotDigest
signature
```

约束：

- `effectiveLeadMs > maxHandoffLeadMs` 返回 `POLICY_OUT_OF_BOUNDS`，不得静默截断；
- `SHADOW` 只计算 eligibility/metrics，不创建 early Admission、不触碰 Producer；
- Candidate 和 Claim 使用 current head；
- Admission 再次验证 current head 并冻结完整 snapshot；
- replay 只依赖 Admission 中的 snapshot、历史信任根和 generation activation，不读取历史 Oxia
  value；
- snapshot 越界、签名未知、issuer generation 未激活、scope 不匹配一律 fail-closed；
- Disable 停止签发 ENABLED lease，所有旧 lease 到期且无新 ownership marker 后才声明
  `EFFECTIVE_DISABLED`；
- Managed 在 durable Journal `OWNERSHIP_STARTED` marker 前再次验证 frozen lease；AUTO_FAST
  在调用 `transport.send` 前再次验证。进入 ownership 后不再用 lease 到期制造 definitive
  rejection。

### 6. Scheduler 与 trusted time

```text
candidateAt = deliverAt - effectiveLeadMs
```

Scheduler 不新增独立 Handoff Scanner。为避免 disabled/小 lead 的 native candidate 挡住同一
Lane 中更早的 ordinary due，同一个 Lane/DRR authority 维护两个内部 head 投影：

- ordinary head 按 `max(deliverAt, retryEligibilityAt)`；
- native head 只包含初次、显式 opt-in、Pulsar BEST_EFFORT 消息，静态最早边界为
  `deliverAt - maxHandoffLeadMs`。

两种索引在同一个 Store WriteBatch 中更新，由现有 Timeline、READY、Lane DRR、Claim 和
Admission work class 一起恢复/调度，不拆 Lane、不创建第二份 fairness authority。runtime
snapshot 只改变 process-local effective eligibility；ordinary head 始终独立可选。

当可信时间区间跨越 candidate 或 `deliverAt`：

```text
TIME_SAMPLE_REQUIRED
    -> wait for a new trusted-time sample
       or bounded monotonic backoff
```

禁止返回已在过去的 `WaitUntil(candidateAt)`。Handoff eligibility 失败不破坏 ordinary due；
系统在 `deliverAt` 继续尝试 `NEREUS_MANAGED_NOT_BEFORE`。

Managed Handoff 只允许初次 attempt；一旦 native Admission 得到 definitive-not-published，
后续 retry 只走 ordinary head。

### 7. Publish Admission

Admission 至少绑定：

- exact `NativeDeliveryPolicy`；
- actual `DeliveryContract`；
- full `HandoffPolicySnapshot`（native Managed Handoff 时）；
- Profile refs 与 semantic hashes；
- target resource/partition/channel；
- decision trusted-time evidence；
- effective lead 与 `actionAt/deliverAt` equality；
- `PulsarRecordTemplate`、template hash、descriptor 和 attempt identity。

ordinary：

```text
contract = NEREUS_MANAGED_NOT_BEFORE
actionAt = deliverAt
no HandoffPolicySnapshot
```

Managed Handoff：

```text
policy permits Managed Handoff
contract = PULSAR_NATIVE_DELIVERY
snapshot.mode = ENABLED
snapshot is in-scope, signed and valid
0 < effectiveLeadMs <= maxHandoffLeadMs
actionAt = deliverAt - effectiveLeadMs
ordering = BEST_EFFORT
```

### 8. closed descriptor 与 Pulsar record

`PreparedPublishDescriptor` 直接替换为新的 closed generation，并完整携带/绑定：

- actual delivery contract；
- policy snapshot 或其 closed optional branch；
- event time；
- Pulsar key closed oneof：`NONE / UTF8 / BINARY`；
- ordering key；
- caller properties：按 key unsigned-byte 排序、唯一、immutable list；
- eight reserved identity/time values plus the rule that final record appends
  `nereus.delay.prepared_hash` as the ninth property；
- target resource、partition、Profile、Lane/channel 和 timing；
- `PulsarRecordTemplate` 和 `recordTemplateHash`；
- exact `ArtifactGenerationSet` digest。

reserved properties 仍精确为：

```text
nereus.delay.route
nereus.delay.partition
nereus.delay.message_id
nereus.delay.generation
nereus.delay.attempt_id
nereus.delay.destination_profile_hash
nereus.delay.capability_profile_hash
nereus.delay.deliver_at
nereus.delay.prepared_hash
```

不得增加第十个 `dispatch_mode` property。

Admission 阶段的 template 不包含 Journal mapping、sequence、最终
`nereus.delay.prepared_hash` property value 或 SEND hash。descriptor 冻结后按以下顺序生成：

```text
recordTemplateHash
    -> preparedPublishHash
    -> Journal mapping + fixed sequenceId
    -> PulsarPreparedRecord
    -> preparedRecordHash
```

`PulsarPreparedRecord` 是 target record 的逻辑 canonical authority，绑定 resolved payload、key、
ordering key、final exact nine properties、event time、closed sequence authority、deliverAt branch、
target identity、template hash 和 prepared identity。Managed 使用 Journal mapping/fixed sequence；
AUTO_FAST 使用 `PRODUCER_ASSIGNED` branch，并在 ACK evidence 中记录 actual sequence。

### 9. Managed Attempt Journal 与 sequence

Managed Pulsar production path 强制：

```text
appendOrReuse exact mapping
    -> durable mapping position
    -> fixed sequenceId
    -> construct exact PulsarPreparedRecord
    -> revalidate frozen lease
    -> durable OWNERSHIP_STARTED marker
    -> builder.sequenceId(sequenceId)
    -> target SEND
```

Journal 不可用、mapping 冲突、retention/cursor 不能证明连续性时不得发送。mapping 已存在的
exact replay 必须复用相同 sequence；不得回退 Producer 自动分配。`OWNERSHIP_STARTED` 前的
exact failure 可以 durable retirement；marker 后没有 definitive proof 时必须保持 UNCERTAIN。

AUTO_FAST 不使用 Managed Attempt Journal，也不自动 resend。

### 10. P1 Resource Guard 与真实 transport

使用 source-locked P1 client/Broker artifact。P1 继续负责：

- resource incarnation；
- connection/producer generation；
- guarded message identity；
- actual SEND command evidence；
- authenticated ACK response evidence；
- definitive guard rejection。

P1 不负责 consumer visibility。

ordinary record：不调用 Pulsar `deliverAt`。

native record：调用 Pulsar `deliverAt(deliverAtEpochMs)`。

两者都必须应用 fixed sequence、key、ordering key、properties、event time 和 payload。所有
Admission/descriptor/record/guard equality 在 `producer.newMessage()` 前再次 fail-closed。

### 11. outcome 与 evidence

`PulsarSendAckEvidence` 直接替换为新的 closed branch，至少绑定：

```text
evidenceSchemaGeneration
targetResource
partition
ledgerId / entryId / normalizedBatchIndex / batchSize
brokerPersistenceTimeEpochMs
producerNameHash
P1 protocolVersion / connectionGeneration / producerId
sequenceId
ExternalDeliveryIdentity
prepared identity hash
recordTemplateHash
preparedRecordHash
sequence authority / journal mapping identity
sendCommandSha256
authenticatedResponseCommandSha256
P1 sourceLockDigest
artifactGenerationSetDigest
```

`preparedRecordHash` 证明 Nereus 逻辑 target record；`sendCommandSha256` 证明 source-locked P1
实际序列化的 SEND command；response hash 证明 Broker ACK。三者的可验证关联必须由
source-lock、deterministic encoder contract 和 golden vectors 闭合，不能只把三个无关 hash
并排存储。

### 12. unknown outcome

Managed path 在 durable `OWNERSHIP_STARTED` marker 后进入 Producer ownership，任何无法证明
definitive non-publication 的失败都保持 `UNCERTAIN`，由 Journal、target ACK evidence、guard
evidence 和 reread/resolution 处理；不得发送 ordinary duplicate。只有 `MAPPED` 而没有
ownership marker 时，才可在 exact local proof 下 durable retirement。

AUTO_FAST：

- ownership 前失败可 fallback Managed；
- ownership 后 unknown 进入 `NATIVE_ENQUEUE_UNCERTAIN`；
- 只允许 query/evidence resolution；
- 禁止 automatic resend。

## 被替代的约束

只有本提案 Accepted 后，才替代：

- “所有 `deliverAt` 都是跨 Subscription earliest consumer visibility”；
- Profile 中一个固定 `handoffLeadMs` 决定所有消息 action time；
- `PULSAR_GUARDED_HANDOFF` 隐含跨 Subscription no-early；
- 根据 `actionAt < deliverAt` 推断 `HANDED_OFF`；
- AUTO_FAST 的 clock-shift/visibility correctness 叙述；
- 当前 payload-only physical request 与 ACK branch generation。

ordinary Managed 的 not-before 权威语义不被删除。

## 数据、协议与兼容策略

选择：**直接替换，若环境证明失败则一次性迁移**。

不维护永久双读。新的 closed bytes 使用独立 generation，unknown/old bytes fail-closed。切换
前必须产生签名 `DataResetManifest`，覆盖所有内部持久资源、obligation、resource
incarnation、Worker 和 generation。

直接替换必须证明：

1. 没有需要保留的外部数据；
2. `PUBLISHING / UNCERTAIN` 为零；
3. 旧资源已清理或换新 incarnation；
4. eligible Worker 全部声明目标 generation set；
5. 启动和 assignment 前验证 exact manifest digest。

若任一条件不能证明，暂停切换并为该环境实现一次性迁移；迁移必须有退出条件和删除时间，
不得演化为永久双轨。

Gate B PASS 后直接按 H1-H6 依赖顺序实施，并允许带 exact attestation 的
`DISPOSABLE_LOCAL` 环境生成 synthetic data、执行集成/恢复/故障测试后销毁。G0 在准备接触
existing/staging/production persistent environment 时执行；它对明确 scope 内的 runtime
resources 只能读取，唯一允许的写入是本地 assessment receipt。closed outcome 为
`PASS_DIRECT_REPLACE / PASS_RETAIN / MIGRATION_REQUIRED / INCOMPLETE`，分别形成 RESET、RETAIN、
MIGRATE candidate 或保持不完整；Assessment 本身不授予 Gate C。

G0 不清理资源，不要求尚未构建的 Worker 已支持未来 generation，也不证明 actual cutover
readiness。Gate C 必须绑定 exact environment/classification/scope 与 Assessment/gate receipt；
Gate C PASS 后只允许 SHADOW。H6 提供的环境级 signed `DataResetManifest` 必须在切换窗口重新
证明 zero external retention、zero unresolved obligation、fresh resource incarnation 和全 Worker
exact generation；Gate C、SHADOW requirements 和 Manifest 全部 PASS 才允许 ENABLED/cutover。

## 安全、故障与运维影响

### 安全边界

- 默认 `FORBID`；
- unknown policy、snapshot、generation、signature、scope 和 schema 一律拒绝 native；
- snapshot 必须绑定 issuer generation 和 trusted-time evidence；
- P1 guard 的 resource/producer identity 必须与 Admission/record 完全相等；
- no-producer-touch gate 必须在 adapter 与 real transport 两层存在。

### 故障边界

- policy unavailable/disabled/out-of-bounds 只关闭 native，ordinary due 保持可用；
- snapshot 过期后不得静默延长；
- Producer ownership 后 unknown 不得 fallback/resend；
- Journal 映射不能证明 durable 时不得 target send；
- evidence 只在 exact request/record/command/ACK identity 闭合后提升 PUBLISHED。

### Pulsar 原生业务风险

选择 `PULSAR_NATIVE_DELIVERY` 的调用方承担：

- Subscription type 差异；
- delayed delivery 是否启用；
- non-strict tick 误差；
- Broker clock；
- TTL 和 retention；
- Broker 配置漂移。

Nereus 可以观测和告警，但不把诊断结果描述为 capability certification。

## 实施切片

### H0：Gate B 前 fail-closed（已完成）

- Managed 在 Worker physical admission、Pinned adapter 和 real transport 三层返回
  `DEFINITELY_NOT_PUBLISHED / CAPABILITY_UNAVAILABLE`；
- AUTO_FAST 在 Pinned native adapter 和 real send transport 两层返回 local definitive
  `CAPABILITY_UNAVAILABLE`；
- 两个 real transport 都在 `producer.newMessage()` 前拒绝；
- no-producer-touch、no-physical-admission、no-reservation-leak 测试；
- Implementation Status 只按真实完成情况更新。

当前真实状态：H0 已在 `main@7cb377ca9dd3135792237af0f027076630d5e4f3` 合入。聚焦测试、完整
`./gradlew test`、`./gradlew check`，以及 source-locked P1
`pulsar-worktrees/nereus-delay-p1@0a2536484cd3932801a98dc88ff112b2df88a1c7` 的 `compileRealPulsar` 与 no-Broker
`runRealPulsarH0Smoke` 均已通过；完整 physical record、`.deliverAt(...)`、Journal、evidence
和 H1-H6 仍未实现。

### G0：deployment 前的只读 DataResetAssessment

- core 已在 `main@68fe2c292e34f0162aac9377b9f935fe598831e4` 实现；
- 只包含 inventory reader、closed outcome evaluator 和 assessment receipt；
- 禁止任何 Producer/Writer、cleanup、drain、migration、resource mutation 或 Worker rollout；
- receipt 绑定 exact NDIP package digest、source baseline、assessment scope、observed resource
  identities、trusted observation time 和 evidence digests；
- closed outcome 映射为 RESET / RETAIN / MIGRATE candidate，不能直接授权 SHADOW/ENABLED；
- 没有真实 deployment 时为 `NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT`，不生成
  虚假 scope、placeholder PASS 或 receipt。

### H1：治理 receipt 证明 Accepted 后的 closed contracts 与 generation

- 定义 policy、contract、policy snapshot、ArtifactGenerationSet 和 cross-object equality；
- 替换 Schedule/Command/Message/Descriptor/Admission closed schema；
- 加入 event time、key oneof、sorted properties 和 generation activation；
- golden vectors、malformed/unknown generation tests。

### H2：runtime policy、Scheduler 与 Admission

- Oxia head、签发、签名、lease、Disable bounded semantics；
- dual ordinary/native head、dynamic candidate、trusted-time retry、ordinary fallback；
- Admission freezes full snapshot and actual contract；
- replay 不读取历史 Oxia value。

### H3：Managed Pulsar record 与 Attempt Journal

- 无损 physical request；
- deterministic `PulsarPreparedRecord`；
- production Journal appender/replay/cursor；
- fixed sequence mapping-before-send 与 durable ownership marker。

### H4：P1 transport、evidence 与 recovery

- builder 完整字段；
- native-only `.deliverAt(...)`；
- P1 guard equality；
- new ACK evidence branch；
- unknown resolution 与 target reread/evidence gate。

### H5：AUTO_FAST 统一权限与契约

- required `NativeDeliveryPolicy`；
- 删除 clock-shift 字段，target timestamp 精确等于业务 `deliverAt`；
- ownership 前 fallback、ownership 后 uncertain；
- actual native contract 和新 evidence binding。

### H6：数据切换与权威文档同步

- canonical signed DataResetManifest 工具；
- fresh resource incarnation 和 Worker barrier；
- activation smoke、restart/recovery、mixed generation rejection；
- 同步主设计、Protocol Registry、ADR、Implementation Status、Design Audit、Runbook 和 gate；
- 全部完成后将 NDIP 标记 Implemented。

## 验证与发布 gate

### H0 gate

- Managed early Worker call：physical admission、delegate、Producer interaction 全为零；
- Managed adapter/real transport direct call：transport 或 `newMessage/sendAsync` 全为零；
- AUTO_FAST adapter/real transport direct call：transport 或 `newMessage/sendAsync` 全为零；
- 两条路径对外都是 local definitive non-publication/not-queued；
- reservation、queue permits、buffer bytes 无泄漏。

### G0 assessment gate

- Gate B 已由 final accepted receipt 证明；candidate receipt 不满足前置条件；
- 所有 adapter 都是 read-only，Producer/Writer/cleanup/drain/resource mutation 调用为零；
- scope、resource identity、current obligation、external retention 和 Worker inventory 完整；
- closed outcome 只能是
  `PASS_DIRECT_REPLACE / PASS_RETAIN / MIGRATION_REQUIRED / INCOMPLETE`；
- Assessment receipt 与 exact NDIP package digest、source baseline 和 observation evidence 绑定；
- 任一 Assessment outcome 都不得被解释为 Gate C、Manifest 或 activation authority；
- `DISPOSABLE_LOCAL` 与 `UNKNOWN` 不能创建真实 Assessment scope。

### Contract/codec gate

- required default-forbid wire；
- enum/generation/oneof/property canonical golden vectors；
- exact nine reserved properties；
- event time round-trip；
- template -> prepared publish -> Journal -> prepared record 的时序/equality/mismatch tests；
- mixed bytes fail-closed。

### Runtime/Admission gate

- policy scope/signature/generation/lease boundary；
- `POLICY_OUT_OF_BOUNDS` 不影响 ordinary due；
- dual-head 下 native disabled/小 lead 不阻塞 ordinary due；
- `TIME_SAMPLE_REQUIRED` 无热循环；
- Disable 在 lease 上界后 `EFFECTIVE_DISABLED`；
- replay 不读历史 Oxia value；
- ordering incompatible matrix。

### Journal/recovery gate

- durable mapping-before-send；
- exact replay reuses sequence；
- unresolved lower sequence blocks later sequence；
- Journal loss/retention gap fails closed；
- crash before/after mapping、before/after durable ownership marker；
- no automatic resend after uncertain。

### P1 physical/evidence gate

- ordinary target record 无 Pulsar deliverAt；
- native target record 有 exact business deliverAt；
- key、ordering key、properties、event time、sequence、payload 全字段验证；
- record hash 与 command hash 不同 domain 的 deterministic projection contract；
- authenticated ACK response hash；
- definitive guard rejection 与 uncertain classification。

### Pulsar native behavior gate

按“原生行为观测”而不是 Nereus not-before certification 执行：

- Shared；
- Key_Shared；
- Exclusive；
- Failover；
- delayed delivery disabled；
- strict/non-strict 与 tick；
- TTL/retention 边界；
- Broker restart/failover。

报告必须明确哪些是 Pulsar 行为观测，不能把结果提升为跨 Subscription Nereus 保证。

### Reset/activation gate

- signed manifest canonical bytes/digest/signature；
- zero external retention obligation；
- zero unresolved publishing/uncertain；
- all resources fresh/clean；
- all eligible Workers support generation set；
- startup、assignment、replay 验证 reset generation 与 digest；
- stale Worker/old bytes/old resource incarnation fail-closed。

## 回滚

### H0

H0 是当前 authority-preserving 的 fail-closed 修复。回滚只允许回到同样禁止 Managed early
send 与 AUTO_FAST native send 的状态，不得恢复任一未闭环 physical reachability。

### Accepted 但尚未激活

保持 runtime policy `DISABLED`，不签发 ENABLED lease，不激活新 generation；可以回滚新
binary 和未启用的 control-plane 配置。

### 已激活 native contract

1. 停止签发 ENABLED snapshot；
2. 等待所有旧 lease 到期并确认 `EFFECTIVE_DISABLED`；
3. 停止新的 native Admission；
4. 解析所有 `PUBLISHING / UNCERTAIN`；
5. 仅在目标 binary 支持当前 bytes/reset generation 时回滚。

直接替换后不得把旧 binary 指向新 generation 资源。若无法证明兼容，只能前滚修复或执行
NDIP 中预先定义的一次性迁移/恢复流程。

## 已关闭的设计问题

### R1. actual authority

Schedule/Message 保存 requested policy；Admission/Prepared 保存 actual contract；外层 union 表达
路径。不存在 `DispatchMode`。

### R2. lease 线性化

Admission 冻结 snapshot；Managed 在 durable Journal `OWNERSHIP_STARTED` marker 前再次验证，
AUTO_FAST 在 transport call 前再次验证。进入 ownership 后按 exact published/definite/uncertain
状态机收敛，不以 ACK 时间判断 lease。

### R3. target timestamp

删除 `brokerDeliverAtEpochMs` 与 clock shift；所有 native target timestamp 等于业务
`deliverAtEpochMs`。

### R4. reset gate

Gate B PASS 允许 H1-H6 按 predecessor 顺序实施和 exact disposable local tests。H1-H6/local
tests 完成后，真实 persistent environment 才运行 G0；Gate C 必须绑定 RESET、RETAIN 或已完成
迁移的 MIGRATED resolution。Gate C PASS 允许 SHADOW；Gate C + SHADOW requirements + environment
signed `DataResetManifest` PASS 才允许 ENABLED/cutover。G0 不证明 future Worker capability、fresh
resource 或 actual zero obligation；这些只能由 H6 后的 Manifest 在切换窗口证明。

### R5. generation activation

一个 closed `ArtifactGenerationSet` 绑定所有低层 generation、reset 和 P1 source lock；复用
Worker capability、eligible reader set 和 source-ordered activation，一个 marker 原子激活
tuple + set。

### R6. logical record / command evidence

Admission 绑定 template；Journal 后产生 final record。`preparedRecordHash` 与 actual SEND hash
属于不同 domain，通过唯一 source-locked deterministic encoder、P1 两个 actual command hash
和逐字段 golden vectors关联，不要求 hash 数值相等。

字段表、dual-head scheduler、Journal ownership marker、exact evidence branch 和类级落点见
[`04-代码级目标设计.md`](04-代码级目标设计.md)。本 Accepted NDIP 不再把这些决定留给编码者任选。

## 权威文档同步清单

- [ ] 主设计
- [ ] Protocol Registry
- [ ] ADR / ADR index
- [ ] Implementation Status
- [ ] Design Audit
- [ ] Operations Runbook
- [ ] 自动化 gate 与测试

NDIP-1 已 Accepted 且 Gate B PASS，H1 可开始；H2-H6 与 authority docs 按 slice 同步。Gate C 尚未
通过，因此 deployment/SHADOW/ENABLED 相关复选框保持未完成，不能把 implementation authority
误报成环境 activation authority。
