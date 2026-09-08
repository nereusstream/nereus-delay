# NDIP-3 B4：局部 Quota 与增量计费契约

状态：**IN_PROGRESS / counter、计量、attempt reserve、跨 incarnation 总额、grant 激活、bookkeeping 预留、唯一 payload owner 、Message/共享 metadata 记录计费及 source-derived incarnation 契约已实现，B4 尚未冻结验收**。
本页与原设计 §11.1、§16.6、§17.3 的 B4 合读。它定义已落到代码的 counter 字段与
局部计算边界；逐项业务 owner、完整计量/grant 关联和恢复来源闭合后才办理
B4 VERIFIED。当前 Lane writer、ValueEnvelope 的既有 reader 与持久 quota map 未改动。
新对象的构造、编码及纯规划器均不授予 source apply、Store 激活或生产发送权限。

## 1. 费用归属与独立身份

Counter 按完整 identity 查询；一条记录包含一个 identity 的 usage 和 local revision。
Source Shard 是 Route Incarnation[16] 与原始 unsigned partition[4]；所有 digest 都是
SHA-256，域字符串以单个 NUL 结束。Profile、bucket、channel、domainSlot 不进入 counter
identity，新增这些对象不能复制 Target 总额度。

`TargetQuotaIdentity` schema 1 的字段：

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | kind：TARGET=1、TENANT_TARGET=2、SHARD=3、TENANT_SHARD=4 |
| 3 | sourceShard[20] |
| 4 | 非零 accountingIncarnation[16] |
| 5 | TargetPartitionId[32]，仅 kind 1/2 存在 |
| 6 | 已认证 tenant routing scope[32]，非零，仅 kind 2/4 存在 |
| 7 | `nereus-delay-target-quota-identity\0` + fields 1–6 的 digest |

`meta_cf` 新 key：`13 01 | kind:u8 | sourceShard[20] | accountingIncarnation[16] |
[targetId[32]] | [tenantScope[32]]`。各分支最大 103 bytes；value 中保留完整 identity，
Store 解码必须同时校验 key 和 Source Shard。SHARD 用于无 Target 归属的控制、结果和
保留记录；不得用它隐藏仍属于某个 Target 的 Message、Reservation 或 attempt。

Kind 1/3 是 aggregate 的唯一主来源。Kind 2/4 是 tenant 投影，不能再相加。
Outcome reserve 与保留账本先依据其冻结的 owner identity 投影到对应主 counter；
aggregate 不再另外加一遍这些账本。旧 incarnation 的费用仍在旧完整 identity 下，
新 queue/accounting incarnation 不覆盖旧费用。legacy reader 的账本来源切换必须
具有唯一所有者，迁移未决 attempt 时不能新旧路径各计一份。

## 2. 资源与 cardinality 不混用

`TargetQuotaUsage` schema 1：

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 `CapacityVector`，既有 accounting_version=1，66 维零显式 |
| 3 | Target 数量 |
| 4 | execution domain 数量 |
| 5 | strict ordering domain 数量 |
| 6 | 主计费 incarnation 数量 |
| 7 | `nereus-delay-target-quota-usage\0` + fields 1–6 的 digest |

Field 2 只允许既有维度 1–15、51–55 非零；第 16/17 维旧 LANE_COUNT /
STRONG_LANE_COUNT 必须为零，不能通过改名重解释它们。物理 request/zombie、RocksDB、
cache、IO、producer/thread、query/fetch 等 live Worker 维度同样为零；相应池与监控
继续单独提供有自身生命周期的资源证明，持久 aggregate 的零值不表示这些资源免费。
51–55 为共享 shard control/system-writer reserve，只能进入 SHARD/TENANT_SHARD。

Field 3–6 是独立 Target 格式的计数，不能投影回旧 Lane grant 的 16/17 维。§10 的新
grant artifact 完整携带这些计数与计量规则，认证/source 激活契约见 §11，禁止以既有
grant 自动授权这些新计数。Counter 约束：

- 每个 Target 主/镜像 identity 的 Target 数最多 1，execution domain 数最多 64。
- SHARD/TENANT_SHARD 不分配 Target 或 domain slot，也不能承担资源维度 1/2/5–8。
- 非零主 counter 必须保留恰好一个 accounting incarnation；tenant 镜像的该项始终为零。
- 全零 counter 是退休标记，不可在相同 identity 下恢复为非零。退休所需的真实保护证明
  由 source committer 校验，构造全零对象本身不构成证明。
- 各数值范围为 0..Long.MAX_VALUE，checked add/subtract；下溢、上溢不饱和、不回绕。

## 3. Mutation stamp、counter 与 aggregate

`TargetQuotaMutation` exact fields：1 非零 raw uint64 **source mutation sequence**；
2 完整有界 SourcePosition；3 已接受 source 操作或 §18 完整本地 Claim 操作的非零
canonical digest[32]；4 可选非零 raw uint64 `localClaimOrdinal`。Source 操作省略
field 4（内存值为 0），其序号与 source 顺序同时严格推进；本地 Claim/revoke 保持
source sequence 与完整 SourcePosition 不变，只推进 local ordinal。显式编码零
field 4 拒绝。SourcePosition 延续 B1 上限、物理资源身份和同 offset 元数据一致性。
恢复 replay 不重新采样时间或 accepted bytes，也不把本地 Claim 次数混入 source
sequence；source 派生 incarnation ID 因此不依赖机器曾执行的本地 Claim 历史。

`TargetQuotaCounter` schema 1，预留 NV type **26**：

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 TargetQuotaIdentity |
| 3 | 完整 TargetQuotaUsage |
| 4 | 非零 raw uint64 local revision |
| 5 | 完整 TargetQuotaMutation，记录该 counter 最后一次变化 |
| 6 | `nereus-delay-target-quota-counter\0` + fields 1–5 的 digest |

首次分配 local revision=1；实际 usage 变化才精确 +1。未变化的 counter 不重编码、不
刷新 revision、不写 bytes。Local revision 不大于 aggregate revision；它可因本地
Claim 超过 source mutation sequence。不要求不同 counter 的 revision 相等。Raw uint64 全一位值可读，
不可递增；跨 signed Long.MAX_VALUE 使用原始位模式，不误判为负数。

`TargetQuotaAggregate` schema 1，预留 NV type **27**；key：
`14 01 | sourceShard[20]`。Fields：1 schema=1；2 sourceShard[20]；3 非零 shard accounting
incarnation[16]；4 TargetQuotaUsage；5 raw uint64 aggregate revision；6 可选完整
TargetQuotaMutation；7 `nereus-delay-target-quota-aggregate\0` + fields 1–6 的 digest。

唯一 genesis 是 revision=0、usage 全零且无 field 6。首次发生 counter 变化时
aggregate revision=1，之后每个涉及 counter 的 mutation 精确 +1；tenant 镜像单独
变化或主费用之间净零转移也更新 aggregate 的 accounting stamp。业务 mutation 无
quota 变化时，aggregate 不写；Store mutation sequence/Source Position 可以继续推进。
Aggregate 的 shard accounting incarnation 标识当前计费基线，恢复不得因缺记录自行
重建 genesis；它不替代每个 Target 或历史 shard counter 的 owner incarnation。

Counter/aggregate/total 的非 genesis revision 非零，各自按实际 quota 写入递增，
不再以 source sequence 作上界。Counter 最后 source/sequence 不得领先 aggregate，
source 相同当且仅当 source sequence 相同，且完整 source metadata 相同；同一 source
下 ordinal 不得领先父记录，ordinal 也相同时才要求完整 stamp/digest 相同。
Aggregate 不得领先 Store source/sequence；Store frontier 不含 ordinal，故 commit
还必须检查 aggregate 完整旧 bytes，不能只比 SourcePosition。Source-only 的
allocation/grant/bookkeeping/payload owner/attempt budget 仍拒绝 local ordinal。
发现身份、source metadata、revision、ordinal 或摘要不一致时 fail closed，不能通过
正常 Schedule 重置。

所有外层与嵌套 schema 均拒绝未知/缺失字段、错误 oneof、非 canonical bytes、未知
version、digest mismatch 和超界输入。上限常量在对应 Java codec 中；独立 Python
向量校验普通 bytes，以及最大 usage/counter/aggregate 的长度和 SHA-256。当前
ValueEnvelope reader 仍只接受 1–11，合法 CRC 的 26/27 也被拒绝。

## 4. 局部 QuotaDelta 与提交顺序

`TargetQuotaDelta.prepare` 接收上一个 aggregate、Store source/sequence、本次已接受
mutation 的 source/digest、有限 `Update(identity,nextUsage)` 列表，以及只支持完整
identity 单点读取的函数。必须提供正数 `maximumTouchedCounters`，没有无限默认值；
该上限最终绑定 A2/C4 的最大合法 mutation 配置与读预算。

每个 identity 在同一计划中最多一次。Planner 不持有全部 counter 集合；它只读取
受影响的记录，输出 immutable prior/next counter 与 prior/next aggregate。来源端
先将同一 mutation 内同一 counter 的全部合法 ledger delta 合并，再进入 planner。

Aggregate 计算为：从旧 aggregate checked-subtract 所有被修改主 counter 的旧贡献，
再 checked-add 它们的新贡献。Tenant counter 不参与。此顺序与逐维净 delta 等价，
允许容量接近 Long.MAX_VALUE 时的净零转移，不因先加后减出现虚假的中间溢出。
成本取决于本次受影响 counter 数 K 和固定维度数，与全集 L 无关；不复制、排序或
hash 全部 counter，也不生成旧 LaneQuotaUsageMap bytes。

提交顺序固定为：

1. 先做 source replay / Command / System Mutation 去重；命中已完成结果不重计费。
2. 从同一 Store view 读取真实 ledger，校验完整 owner、incarnation、保护、grant 与业务前置条件。
3. 生成有限 delta 计划。任何 arithmetic/budget/source 错误均不改变内存或持久状态。
4. 在 Store mutation guard 内运行 `requireCurrent`，校验 prior aggregate、Store
   source/sequence 及每个变化 counter 的完整旧 bytes；与业务记录、结果、source 一批提交。
5. 仅确认 WriteBatch 成功后发布内存结果；失败不发布，写结果不确定时走 Store 恢复。

Planner 本身没有写 Store、提前更新共享 Map 或模拟成功提交的入口。过期 plan 被拒绝
而不是被解释为 dedupe success；真正重复 source 的结果由步骤 1 的真实账本返回。
`TargetQuotaUsage.permitsGrowth` 支持 grant 下调：已有超额不被删除，单维不增长或
释放仍可通过；增加到 limit 以上被拒绝。完整 grant 认证和保护状态仍由调用端负责。

## 5. 恢复与审计边界

`TargetQuotaDelta.audit` 仅用于恢复/显式审计，输入完整 durable counter 枚举和**独立
从 Message/reservation/attempt/outcome/retained/control/identity 账本重建**的 usage。
缺失或多出的 identity、重复记录、usage 不符、stamp 不符和 aggregate 不守恒均拒绝。
它按主 counter 求和，tenant 镜像只核对：每个镜像必须有对应主 identity；同一主
identity 的所有 tenant 资源维度之和不能超过主费用。共享 metadata 可由主 counter
独有，tenant Target/domain 计数不按主资源维度求和。

审计函数的重算参数不是权威账本 reader；从 counter 自己复制一个 Map 只能验证算术，
不能证明 recovery 正确。C4 将实际恢复遍历接入，D/E 再验证 checkpoint/source replay/
故障接管。正常 mutation 严禁调用全量 audit 作为修复手段。

## 6. B4 尚需闭合的原设计验收项

本批不将源码存在或局部测试冒充 B4 完整冻结。继续完成：

- §7 已实现固定计量 artifact，§8 已实现 attempt reserve 转实占；仍须闭合每类真实
  业务 record 的唯一 owner；§12 已固定 counter/budget 自身 bookkeeping 与相关共享投影，§14 已绑定 Message 家族的实际记录费用来源，§17 已绑定十类 Target metadata 的冻结归属与点读依赖。
- §9 已列原 §17.3 的完整业务 delta 表；继续绑定每行的完整 before/after ledger、
  §13 的 payload owner 与真实业务记录之间的 before/after 关联、identity 保护和结果/控制记录来源，避免只由方法参数声明费用。
- §10–§11 已定义 cardinality/grant artifact、跨 incarnation 总额和认证 source/control
  激活契约；§15 已固定 source-derived incarnation、局部 Queue/OrderState 计数和受保护退休检查。
  仍须闭合完整 tenant cut 关联、Queue 轮换/legacy handover 的 source 配方及独立账本所有权交接，保证不会因多个 Profile/domain 复制 grant。
- 本批向量/测试已覆盖重复 mutation、溢出、Outcome/UNKNOWN/旧 obligation 的必要
  算术与释放顺序；B4 最终仍须把完整 grant/owner/保护规则逐项绑定到原始验收证据。

C4 的真实原子提交、内存发布、账本恢复、固定 K 随 L 增长的 Store 读写计量继续保留；
它们不是本批纯 planner 测试的已完成结论。A2/A3、B5–B7、C–F 的责任没有缩减。

## 7. 固定计量 artifact

`TargetQuotaAccounting` schema 1 的 exact fields：1 schema=1；2 非零完整 Target schema
bundle hash[32]；3 输入 CanonicalScheduleIntent.QUOTA_ACCOUNTING_VERSION=1；4 每个
record 的固定 overhead bytes；5 Kafka adapter envelope overhead bytes；6 Pulsar
adapter envelope overhead bytes；7 最小 DRR record cost bytes；8 digest[32]，域为
`nereus-delay-target-quota-accounting\0`，覆盖 fields 1–7。

Field 4–6 非负，field 7 正数，均不超过 Long.MAX_VALUE；没有隐式默认 artifact。
常量由同一 source-activated Route/grant 选择，旧 charge 保留创建时完整 artifact。
Artifact 的值相等按完整 canonical bytes 判定，hashCode 与其域 digest 一致；独立
解码后的相同 artifact 必须可用于 owner/ref 核对，不能依赖 Java 对象同一性。
输入版本 1 延续既有公共 Schedule 编码，不意味着旧 Lane grant 自动适用新 Target
费用。Schema bundle / artifact / grant 的绑定须通过 §11 的完整授权检查及 C4 的实际后端。

已实现的计量公式：

```text
accountedPublishBytes = payloadLength + canonicalAdapterMetadataLength + adapterEnvelopeOverhead
schedulingCost = max(accountedPublishBytes, minimumRecordCost)
storedRecordBytes = canonicalKeyLength + canonicalTypedPayloadLength + 12 + recordOverhead
outcomeWalBytes = canonicalFramedWalRecordLength + recordOverhead
```

12 是 NV 的 type/version/length header 与 CRC 总字节；typed payload 不再包含该
header。WAL 输入已经是完整 canonical frame，不能再加 NV header。方法只接受 checked
非负长度，record key/WAL frame 非空；任何加法溢出在提交前拒绝，不读 SST、文件系统、
压缩率或对象实际计费大小。除 §12 专门列出的固定投影存储预留外，调用端必须从经过 canonical 校验的冻结 bytes 取得长度，
不能将方法参数当成可由客户端声明的费用。

一个持久 NV record 只属于一个 record-byte 类：STATE→维度 3；RESULT→9/10；
SYSTEM_MUTATION outbox→11/12；EVIDENCE→14/15；独立 WAL frame→13。完整 source/Broker
writer 的共享 quota 仍受 51–55 与其权威 grant 约束，不因本地字节公式获得远端额度。
应用 payload ownership（2/6/4）、执行 envelope（8）和已编码的存储副本是不同资源事实，
不能把同一个 record 再同时塞进 STATE、RESULT、SYSTEM 或 EVIDENCE 两个类；也不能把
outcome 的已分配 record 与覆盖它的 reserve 各加一次。

本批提供 `activePayload/reservedPayload/retainedPayload/executionCharge/recordCharge/
outcomeWalCharge` 的 checked 向量。它们不自动选择某条业务 mutation 的 record 集合。
Counter/aggregate/bookkeeping 及 attempt budget 记录自身的固定预留见 §12。完整业务
reserve sizing 仍须在 B4 最终绑定中闭合，不能通过递归计算自身编码长度或默认为免费跳过。

## 8. Attempt budget 与 reserve 转实占

`TargetQuotaAttemptBudget` schema 1，预留 NV **28**，meta key
`15 01 | PublishAttemptId[32]`。它保存独立计费生命周期，不重写旧 Admission 或 Journal。

| field | exact 内容 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 TargetMessageLocator，保留 message/generation/Target/domain/accounting/binding |
| 3 | 已认证非零 tenant scope[32] |
| 4 | 非零 PublishAttemptId[32] |
| 5 | 精确已接受 Admission canonical bytes 的非零 digest[32] |
| 6 | 完整冻结 TargetQuotaAccounting |
| 7 | 冻结 accounted execution bytes，0..Long.MAX_VALUE |
| 8 | 完整已承诺 reserve CapacityVector |
| 9 | 上述 reserve 内已分配持久 records 的 CapacityVector |
| 10 | phase：ADMITTED=1、UNKNOWN=2、RESOLVED_AWAITING_FLOOR=3、RETAINED=4、RELEASED=5 |
| 11 | local raw uint64 revision，初值 1，精确 +1，不回绕 |
| 12 | 完整最后 source mutation stamp |
| 13 | 确定 resolution 的完整 mutation stamp，仅 phase 3–5 存在 |
| 14 | 释放 reserve/retained 时检查的非零 Floor digest[32]，仅 phase 4/5 存在 |
| 15 | 冻结非零 Recovery Lineage[16] |
| 16 | `nereus-delay-target-quota-attempt-budget\0` + fields 1–15 的 digest[32] |

Field 8/9 只允许维度 3/9–15，commitment 非零且逐维覆盖 allocated；active/retained payload、
reservation、Claim/attempt execution、Target cardinality 和共享控制 pool 不混进该 reserve。
Retained payload 的维度 4 始终属于 Message Identity 的唯一 payload owner，不能在多个
attempt reserve 内各预留一份。Field 12 的 Source Shard 必须与 locator 的 message routing ID 相同。Field 13 在确定 resolution 时与 field 12 精确相等；phase 3 的最后 reserved writer
可以继续在原 commitment 内 source-order 更新 allocated，field 12 随之推进，field 13
保留首次确定结果。后续 source/sequence 严格推进；phase 4/5 的 field 12 必须严格晚于 field 13。
ADMITTED revision 必须为 1；其它 phase 分别至少为 2、2、3、4。完整 envelope/key/source
和 canonical/digest/bounds 校验不代表 Admission、tenant 或 Floor 已获权威认证。

Counter 的主 owner 从完整 locator 的 Source Shard/Target/accountingIncarnation 派生，
tenant owner 添加 field 3。始终使用这组冻结身份；不会因当前 Message 已变为新
Generation、当前 queue 已换 incarnation、Profile 已升级或 channel 已关闭而改变。
首次建立预算时 C4 必须解析完整 retained Admission/Binding/tenant grant，核对该预算
与 exact attempt；field 5 的 hash 不能替代实际引用对象和 source authority。

预算投影规则：

| phase | 加到主 counter 的资源向量 |
|---|---|
| ADMITTED / UNKNOWN | commitment + 一个逻辑 attempt 的维度 7/8 |
| RESOLVED_AWAITING_FLOOR | commitment；确定 Outcome 已 source-applied 后释放维度 7/8 |
| RETAINED | allocated，未使用的 commitment 才可减去 |
| RELEASED | 全零；历史 bytes 仍按实际保护/清退流程处理 |

allocated 是 commitment 内部的实占投影，不是额外费用。写 UNKNOWN/outcome/evidence
时更新 allocated，仍须 `allocated <= commitment`，不得借 tenant pending 额度补差。
UNKNOWN/timeout 即使改变或减少 allocated，也保持完整 commitment 和 execution charge。
`resolve` 只接受 VERIFIED_PUBLISHED / VERIFIED_NOT_PUBLISHED；外部调用端仍须验证
exact attempt 的完整 evidence、Outcome 和当前 source authority，不能由 enum 自证。

Unused reserve 的释放至少要求：已 RESOLVED；同 lineage 的 Floor 同时覆盖预算最后
source 与 mutation sequence；同 offset 必须 metadata 全相同；Floor 严格早于本次
释放 mutation 的 source/sequence。然后必须调用 `ReleaseAuthority`，验证当前 catalog
ancestry、Recovery Pins、完整账本 owner、所有 reserved writer 已结束，且该权威快照
在同一 batch 提交前仍有效。Floor DTO 本身不完成这些检查。

Retained 减额只能逐维下降；还需同样的 Floor 条件和 `ReleaseAuthority` 对 actual
provider/Store 删除确认、全部 retention/读/attempt/重放保护的验证。部分删除只减相应
allocated，不能清空其它 record classes；全部清零进入 RELEASED 后不可恢复使用。
异常或 fatal Error 不产生新预算值。上述权威 seam 尚无 C4 生产实现，本批只验证纯
转换不会绕过调用或必要条件；不把允许所有请求的测试函数当作生产释放证明。

逻辑 attempt 的维度 7/8 与 Worker physical/potential-zombie pool 分开。确定 Outcome
关闭前者不会调用物理池释放；后者继续等待实际 request 完成或 certified fenced teardown。

## 9. 原 §17.3 业务 delta 表

记 `P(len)` 为 active Message/payload 的 1/2；`R(len)` 为未提交 reservation 的 5/6；
`H(len)` 为唯一 retained payload ownership 的 4；`X` 为冻结 Claim 的 7/8；`B` 为
上一节 exact attempt budget 的 effectiveCharge；`S` 为本次实际改变的分类 record charges。
以下主 counter delta 由真实 before/after ledger 产生；tenant 投影相同可归属费用，
aggregate 只加主来源。共享 records/identity 不因 Profile、bucket 或 slot 新增重复计费。

| 业务路径 | 唯一来源及 delta | 释放时点与保持义务 |
|---|---|---|
| Schedule | 首次接受的 Message payload owner：`+P(len)`；`S(after)-S(before)` | target/domain/incarnation 数只取真实 registry 分配 delta；不按每条 Schedule 分配 slot |
| Prepare reservation | 新 reservation：`+R(expectedLen)`；分类 record delta | 对象上传成功本身不改变 quota；未 source-accepted Commit 仍是 reservation |
| reservation commit | 同一 reservation `-R(len)+P(len)`；proof/record delta | exact 已提交重试为零；不同 Proof/object 不得再加一份 Message/payload |
| reservation expire/abandon | `-R(len)`；必要保留记录/对象进入对应实占或 `H` | 先由已闭合 source time/control 决定；cursor 物化不能再次减额；对象删除未确认不免除 retained |
| Cancel | 合法可逆状态 `-P(len)`，有 Claim 时 `-X`；需要保留时 `+H(len)`，分类 record delta | TOO_LATE/NOT_FOUND 不改变业务 owner 费用；旧 admitted/UNKNOWN attempt 的 `B` 不变 |
| Reschedule | 同一 payload owner 的 `P` 不变；若原 Claim 被合法撤销则 `-X`；仅真实 record/索引差额 | 不因 generation、timeline 或 sibling 改变再计 payload；不越过已经 Admission 的前置条件 |
| Claim | §18 本地 durable reversible Claim `+X`，Claim record 加入分类 `S` | timeline/READY 本身不增加 7/8；失败且没有持久 Claim 不收费 |
| revoke/Claim 失效 | §18 本地 exact durable Claim `-X`，Claim record 删除/retained 转移 | 仅扣该 Claim 首次冻结 charge；不扣任何已 Admission attempt |
| Admission | 消费 Claim 时 `-X`，建立 exact attempt `+B(ADMITTED)`；其它 record 按所属 reserve/分类更新 | 不重复加 active/payload；已有 old UNKNOWN attempt 继续独立计费；容量拒绝不创建 attempt |
| definitive failure | exact verified NOT_PUBLISHED：`B(open)→B(RESOLVED_AWAITING_FLOOR)` | 只释放该 attempt 的逻辑 7/8；retry work 仍保有 `P`，future Claim 再产生自己的 `X` |
| UNKNOWN | exact budget `B(open)→B(UNKNOWN)`，effective delta 为零；allocated 在 commitment 内更新 | 不释放 execution、reserve 或 physical/zombie；新 retry/Admission 不覆盖旧预算 |
| Outcome/evidence resolution | 已验证 PUBLISHED/NOT_PUBLISHED 关闭 exact attempt，按 budget phase delta | stale/冲突/重复结果不收费或释放；UNKNOWN 不能借 transfer 字段授权确定释放 |
| terminal/HANDED_OFF | 当前 generation `-P`，需要保留的唯一 payload owner 转 `H`；其它实际 record delta | 未决旧 attempt 的 `B` 与实际 request/zombie 保留；不因 Message aggregate 终态清空它们 |
| checkpoint-safe reserve transfer | exact budget `commitment→allocated`，减未使用部分 | 仅当 §8 Floor 与完整 ReleaseAuthority 条件都成立；不把全部 commitment 当作 free |
| retained release | exact guarded deletion：`-H` 或删除的 record charge；budget 按 allocated 逐维下降 | 实际删除、pins、Floor、重放/查询/导出窗口全部满足；logical terminal/TTL 单独不够 |
| 旧 attempt 排空 | 对冻结的旧 generation/Target/accounting/Admission/artifact 执行同样 budget delta | 新 incarnation 不接受旧 release；旧 Lane reader/mapping 与新 owner 只能有一个 aggregate 来源 |

同一实体的 payload 在 reservation、active 与 retained 三个 ownership bucket 中只占
一个；DLQ Replay/new generation 复用同一不可变 payload 时执行 `-H+P`，不保留第二份
ownership 费用。独立持久副本仍由它自己唯一的 record-byte 类计费。Old attempt 持有
payload 引用是保护条件，不新造一份应用 payload ownership。

表中的业务有效性、完整 `S` 集合、retire owner 以及 grant 准入仍须在 B4 最终绑定；
本批用独立向量和转换测试覆盖 payload/Claim/attempt 的算术守恒与阶段释放，没有
声称已把这些规则接入活动 DelayShard。逻辑 grant 下调时，已有工作 Claim/Admission/
Outcome 的继续服务与独立 outcome/physical reserve gate 必须分开；不能把
`permitsGrowth` 无区别应用于所有路径而堵住 drain。

## 10. 跨 incarnation 的总额与完整 grant artifact

### 10.1 Scope 与租户边界

主设计 §5.4 已规定：每个 Ingress Route 只属于一个 tenant Security Domain，Route
Incarnation 内的 `tenantRoutingScope[32]` 不变。新的 `TargetQuotaScope` 使用这个受认证
的 routing scope；不能拿 payload tenant、Profile、调用者任意值或另一个 tenant hash
代替它。Constructor/decoder 不提供 Route registry 认证，C4 从可信 Route binding 传入。

Schema 1 的 exact fields：1 schema=1；2 sourceShard[20]；3 非零 tenantRoutingScope[32]；
4 可选 TargetPartitionId[32]；5 `nereus-delay-target-quota-scope\0` + fields 1–4 的 digest。
无 field 4 为整个 Source Shard，有 field 4 为该 shard 内一个 Target；bound 126 bytes。
Scope **不含 accounting incarnation、Profile、bucket、channel 或 domain**。完整 scope
是 grant 的额度边界，leaf counter identity 是历史费用的所有权边界，二者不互换。

Shard grant 使用现有 primary aggregate，覆盖该 Route 上全部 Target 与无 Target 的
共享 metadata/system 费用。共享 source-local 费用保守地占用该 shard 的 tenant cut，
不从 tenant cap 中扣除后再向别处借容量。TENANT_SHARD 仍只镜像无 Target 的 SHARD
counter，绝不是所有 tenant-target 的额外 rollup。Tenant mirror 用于归属核对/投影；
不因镜像比 primary 小而放宽 primary Target/shard 上限。

不同 Source Shard 的静态 grant 总和继续受 tenant hard policy 约束。本文没有增加跨
Worker 瞬时共享额度；同一个物理 Target 跨 shard 的发送仍共用既定物理 request/byte
pool。Policy authority 必须维持主设计 §18.2 的 shrink-before-increase、donor excess
占用与 recipient placement reserve，不把本地对象合法解码当作全租户容量证明。

### 10.2 TargetQuotaTotal

为避免新 incarnation 再获得一份 full cap，每个 Target 在 Source Shard 内另存一个
跨 incarnation 的 primary total，预留 **NV type 29 / meta tag 16（十六进制）**：

```text
key = 16 01 | 02 | sourceShard[20] | tenantRoutingScope[32] | targetId[32]
```

Key 固定 87 bytes；`02` 是 Target scope 分支。Shard scope 的 suffix 分支为 `01`，但
不会为它分配 total record，Shard 总额沿用 NV 27 aggregate。完整 key、source 与 tenant
必须同时校验。Total 的 exact fields：

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 TargetQuotaScope，必须含 Target |
| 3 | 完整 TargetQuotaUsage，各 accounting incarnation 的 TARGET primary usage 之和 |
| 4 | 非零 raw uint64 local total revision |
| 5 | 完整 TargetQuotaMutation |
| 6 | `nereus-delay-target-quota-total\0` + fields 1–5 的 digest |

总额是已有 primary counter 的派生视图，**不再加到 shard aggregate**；tenant mirror
不进入它的求和。旧 attempt、retained payload/result、保护中的费用保留旧 leaf identity，
同时持续占用这个稳定 scope 的上限。不会因新 generation 或新 accounting incarnation
从 grant 视图中消失。

整个 Target 的 `targets <= 1`，`executionDomains <= 64`，51–55 仍只能归无 Target 的
Shard 费用；strict-domain 与 accounting-incarnation 使用 checked 总计。Queue rotation
必须在同一个 delta 中把旧 incarnation 的 queue ownership 计数降为 0，才给新 incarnation
计 1；旧 protected 费用及 incarnation 自身仍保留。Domain 也须有独立身份保护和唯一
计数来源，不能给每个 incarnation 再分配 64 个 slot。该计数约束不替代 §6 尚待冻结的
真实 allocation/retirement 证明。

首次 total revision=1，之后每个改变该 Target primary leaf 的 mutation 精确 +1，即使
incarnation 间转移的净 usage 为零，也记录其新的 last-touch stamp。没有 primary leaf
变化时不读写 total；mirror-only 和 SHARD-only mutation 不更新它。Local total revision
不大于 aggregate revision；每个 primary leaf revision 不大于其 total revision。所有
last-touch source、raw source sequence、local ordinal 及完整 metadata/digest 须按 §3 相容。Total 还必须逐维覆盖
每个 primary leaf，且被 shard aggregate 逐维覆盖；stamp 相容不能掩盖父视图少计费用。
全零 total 可以记录
已排空状态；稳定 scope 后续再使用必须沿用已有 revision，不能将它误作可复活的旧 leaf。
Total 与最后的零 leaf tombstone 在本格式内保留，不能仅凭 zero usage 删除并重置 revision；
其最终受保护清退与 Route/F1 恢复边界一并办理。

### 10.3 同 batch 的有限 total delta 与恢复检查

`TargetQuotaTotalsDelta.prepare` 从已有 immutable `TargetQuotaDelta.changes` 派生需要
更新的 Target 集合，而不接受调用者另报一套 delta。显式 `maximumTouchedTargets > 0`；
先拒绝过量/跨 tenant，之后每个受影响 Target 恰好一次 point lookup。所有旧主贡献先减，
再加所有新主贡献，避免 Long.MAX_VALUE 附近的净零转移中间溢出。发现已有 leaf 缺少 total、
lookup scope 错误、source/revision 不符或 arithmetic 错误时拒绝，不走热路径扫描重建。

普通单 Target Schedule 的 quota 部分现在是：变化的 primary leaf、必要 tenant mirror、
一个 Target total 和 shard aggregate，均与业务/结果/source 同 batch。相比仅 leaf 的基础
规划增加一个固定 total write；不声称仍只有三条 quota 记录。旧新 incarnation 同时变化
时，可能多写 leaf，但仍只写该 Target 的一个 total；复杂度取决于 K，不取决于全集 L。
Bookkeeping 本身的费用与 mutation bytes 预算仍须按 §6 完成有限容量证明。

在相同 Store mutation guard 内运行 composite `requireCurrent`：先校验 leaf/aggregate/
Store sequence/source，再校验 total 的完整 prior bytes 或精确不存在。实际 WriteBatch
成功之后才能发布这两个 immutable plan；失败/结果不确定时遵循 §4，不能先改 Map。

`TargetQuotaTotalsDelta.audit` 仅在恢复/显式审计遍历全量 leaf 与 total，核对完整 scope 集合、
所有 incarnation primary sum、tenant/source/revision。缺失、重复、多余、漏掉旧 retained
费用均拒绝。必须先独立从真实业务账本重建并通过 `TargetQuotaDelta.audit`；从 leaf 求和
只能证明投影一致，不能证明叶子账本正确。零 total 与相应零 leaf 也必须纳入审计。

### 10.4 完整 grant artifact 与逻辑入口检查

`TargetQuotaGrant` schema 1 的 exact fields：

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 TargetQuotaScope |
| 3 | 非零 grantId[32]，同 scope 的连续授权保持不变 |
| 4 | 非零 raw uint64 grant version |
| 5 | 完整 TargetQuotaAccounting |
| 6 | 完整 TargetQuotaUsage limit，包含四个新的 cardinality 上限 |
| 7 | 非零 raw uint64 tenant policy version |
| 8 | 非零 tenant policy canonical hash[32] |
| 9 | `nereus-delay-target-quota-grant\0` + fields 1–8 的 digest |

Target branch 的 limit 同样约束 targets<=1、domains<=64、51–55 为零；Shard branch
允许其完整范围。零 limit 合法，表示停止新增而保留排空。首次 version=1，后续 exact
scope/grantId 不变且 checked +1；policy version 不回退，同 version 的 policy hash
必须一致。原始 uint64 可跨 signed 边界，全一位值不能再递增。新 grant 的计量 artifact
只约束新工作，旧 ledger 按自己冻结的 artifact 保留费用，不能以新常数重算历史费用。
§16 进一步约束已有 allocation 的普通 Target grant 更新：必须保留原 accounting artifact；
更换 artifact 需要独立受控轮换配方，不能靠一次普通 grant 更新完成。

该 artifact 自身没有单独 NV/meta key，也没有接入旧 PUBLISH_QUOTA_GRANT branch；
旧 17 维 `QuotaGrantRef` 不变。§11 的新 branch 与 NV 30 source 激活投影完整携带该
artifact、prior grant、ControlRef 和 source stamp。C4 实现真实 authority 后端、可信
Route/policy 解析和 guarded apply。仅 `requireSuccessor`、digest 或 decoder 均不能发布
额度，不能证明静态 cuts 总和或授权新 cardinality。

`TargetQuotaGrantGate.evaluate` 固定以下纯逻辑策略：

- FIRST_SCHEDULE、PREPARE、DLQ_REPLAY 在去重与完整业务检查之后，按 shard primary
  aggregate 和跨 incarnation Target total 做逐维 growth 检查。两个当前 grant 的计量
  artifact 必须与新工作完整 artifact 字节一致。返回 SHARD_LIMIT/TARGET_LIMIT 后仍按
  Source Position 记录确定性拒绝；拒绝记录使用已预留的控制/结果容量。
- Reservation commit/expire、Cancel、Reschedule、Claim/revoke、Admission、definitive
  failure、UNKNOWN、Outcome、terminal、retained release、old-attempt drain 均属于已有
  工作；经过完整 scope/历史 total 检查后返回 EXISTING_WORK_DRAIN，不用下调后的逻辑
  grant 阻断它们。逻辑 execution/retained 费用的合法阶段转换可以增加某一维。
- EXISTING_WORK_DRAIN 只说明本次不被 logical grant 下调阻断。真实既有 ledger、frozen
  attempt reserve、Outcome/system-writer reserve、target execution permit、物理/zombie
  pool 与合法状态转换必须各自通过；它不是发送或删除 permit。调用端不得将 first-seen
  ingress 重标成既有工作，也不能用此纯策略代替签名、激活和 ledger ownership。

以上 codec/纯规划/逻辑分支的专项验证不宣称实际 Store 原子性、Broker quota、生产
activation 或恢复正确性；这些仍由 C4/D/E 与对应证据完成。

## 11. 认证 grant control 与 source 激活契约

### 11.1 完整前后 grant 的新 Control 分支

`PUBLISH_TARGET_QUOTA_GRANT = 18` 是独立 ControlOperationKind；旧 branch 14 的
17 维 QuotaGrantRef 和旧 reader 保持原义。RBAC 延续 quota publication 的
PLATFORM_OPERATOR 要求，仍须匹配受认证 actor、role-set、resource-scope hash 和
完整 resource scope proof；Platform 角色本身不授予任意租户资源权限。

`TargetQuotaGrantControlRequest` schema 1 的 exact fields：

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 next TargetQuotaGrant |
| 3 | 可选完整 prior TargetQuotaGrant；仅首次 version=1 缺省 |
| 4 | 可选完整既有 QuotaTransferPlanRef |

Next 对 prior 执行 §10 的 exact scope/grantId/version/policy successor 规则。
不能以 prior version/hash 标量代替完整旧 artifact，也不能省略更新前值。TransferRef
的四个字段为独立 parent plan 的 operationId、requestHash、tenantPolicyVersion、planHash；
两个 hash 在此 branch 都必须非零，policy version 必须等于 next grant 的版本引用。
Parent plan operationId 不能等于本次 publication operationId，避免自引用请求。无 transfer
ref 只表示不属于已有转移计划，不表示省略 tenant cut 或 physical reservation 检查。

PreparedControlOperation 必须只有一个 SHARD target，index=0，完整 ShardSubject 与
next.scope.sourceShard 相同，expectedMutationId/hash 成对存在。Target quota scope 留在
完整 request 内，不复用旧 QUOTA_GRANT target branch，不跨多个 Source Shard 建立一个
本地原子更新的假象。多 Shard 的收缩/转移仍由受信 parent plan 分步推进。

Request 外层 oneof field=18；requestHash 延用 PreparedControlOperation 的既有域、
operation kind 与 lp32(完整 outer request) 公式。先生成无自引用的 body，再计算 mutation
ID/hash，最后将其放入 signed prepared target 并注册；canonical body 不含自身预期 ID/hash。

### 11.2 新 ApplyShardControl kind 17

`TargetQuotaGrantControlBody` exact fields：1 完整 ShardSubject；2 type=APPLY_SHARD_CONTROL=1；
3 非负 retryUntil；10 完整 ControlRef；11 controlKind=17；12 next grant 的 raw uint64
version；13 semanticHash[32]；15 ControlPayload，其唯一 oneof field=17，内容是完整
TargetQuotaGrantControlRequest。**Field 14 始终缺省**：完整 prior 及其 raw uint64 version
已在 field 15 的 signed request 内唯一编码，不改变旧外层 expected-prior scalar 的语义。

Semantic hash 为 `SHA-256("nereus-delay-target-quota-control\0" || u16be(17) ||
request.canonicalBytes)`。Logical identity 延用 `ControlRef.logicalOperationIdentity(17)`；
System Mutation hash、ID、signature domains 和 source framing 不变。ControlRef 必须
绑定此 request 的 hash、非零 operationId 和唯一 target index 0，body 的 Shard/retry
必须与外层 signed mutation 一致，semanticVersion 必须等于完整 next grant version。

ControlSystemMutationFactory 和 ControlTargetMutationBinding 已支持此独立分支并检查
request/body/ref/target/expected mutation 的逐字关联。旧 ApplyShardControlBody、默认
source semantic reader 和活动 Lane Store 仍拒绝 kind 17；C4 的 Target source reader
须从新格式 body 自行导出 logical identity 后完整验证 envelope，不能信任调用方报出的 ID。
旧 reader 的拒绝不是新 Target writer 已激活的证明。

### 11.3 有 source 的完整激活记录

`TargetQuotaGrantActivation` schema 1，预留 **NV 30 / meta tag 17（十六进制）**：

```text
17 01 | scopeKind:u8 | sourceShard[20] | tenantRoutingScope[32] | [targetId[32]]
```

ScopeKind=1 为 shard（key 55 bytes），2 为 Target（key 87 bytes）。Key/value/source/tenant
同时校验。Value exact fields：

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 TargetQuotaGrantControlRequest，含 next、可选 prior 与 transfer ref |
| 3 | 完整 ControlRef |
| 4 | 完整 TargetQuotaMutation |
| 5 | accepted systemMutationId[32] |
| 6 | accepted systemMutationHash[32] |
| 7 | 可选完整 OPEN TargetQuotaIncarnation 首次分配快照；§16 规定 presence 与来源 |
| 8 | `nereus-delay-target-quota-grant-activation\0` + fields 1–7（仅存在字段）的 digest |

Field 4 的 sequence 是本次 Store mutation sequence，source 是首次实际接受的完整
SourcePosition，mutationDigest 为 `SHA-256(SystemMutation.canonicalEnvelope())`，
覆盖完整签名、author 和 signing-key version；不含外层 NDL1 frame header/CRC。
这个完整字节摘要不能被 field 6 的 semantic mutationHash 替代。同一语义 body 换 signing
key 重签可以保持 SystemMutation ID/hash，却不能重写第一次接受的 envelope/source stamp。

Field 5 还须按完整 Shard、ControlRef.logicalIdentity(17) 和 field 6 重新导出核对。Grant
version 不大于 Store sequence；全 raw uint64 范围保留。Value 只包含 prior **grant artifact**，
不递归嵌入 prior activation，因此历史长度不随更新次数增长。编码 bound 由至多两个完整
grant、有限 TransferRef/ControlRef、至多一个完整 OPEN allocation snapshot、两份完整有界
SourcePosition 与固定 hash 字段相加。Shard branch 也按相同保守 activation 槽位预留。
独立向量覆盖 Target/Shard 两个最大分支、raw version/policy 全一位值和 1 MiB Pulsar source。

### 11.4 首应用校验、权威快照与同 batch 发布

`TargetQuotaGrantControlVerifier` 的顺序固定如下：

1. 调用端先处理 source replay、SystemMutation/Control 去重，保留首次结果与 source。
   本 verifier 没有 dedupe shortcut；重复/更旧 source 或不匹配 prior 均不能生成第二次激活。
2. 检查新 operation/type、完整 source/body/request/ControlRef、signed retry window；
   source timestamp 取 Broker 已认证时间，不使用重放时 wall clock。已登记 operation 的
   registration retry deadline 不替代它自己的 source mutation retryUntil。
3. 从 source-protected key authority 解析 prepared 和 mutation 两层签名所需的 key。
   两层签名均验证；signed author 必须逐字匹配 registered author。恢复可用受保护的
   historical key，不能任意换成 current key。Unproven absence/transient error 应抛出并
   停止该位置；null 只能表示权威确认缺失的 key，不能表示网络超时。
4. 验证受认证 actor/roles/resource scope 和完整 scope proof，读取精确注册的 prepared
   bytes，核对登记的唯一 target 及 expected mutation ID/hash。仅 caller 自签对象不够。
5. Route authority 核对 immutable tenantRoutingScope 与完整物理 source resource。
   本地 View 包含 current activation、aggregate、必要 Target total、Store sequence/source
   和冻结 recovery lineage；prior origin 的 lineage 必须一致。
   完整 prior grant 必须与 current activation 逐字相等；首次申请必须确实不存在旧激活。
6. View 中各 grant/accounting stamp 不得超出 Store source/sequence；任意两条 stamp 的
   source 顺序与 source sequence 顺序必须一致；同位置完整 source metadata 一致，
   local ordinal 决定其先后，ordinal 相同时完整 mutation digest 也须一致。
   Total 必须满足 §10 的层级覆盖关系。计算 checked next Store sequence，拒绝耗尽。
7. 按 §16 检查首次 Target allocation 的 absence/费用或 existing origin 保留与 artifact，
   调用强制 CapacityAuthority，传入**完整 Control body（含 ControlRef）**、精确 View、
   实际 source 与可空首次 allocation。成功才产生 immutable Change(before, after)。

CapacityAuthority 是 C4 需要实现的受信后端契约，不是已存在的生产证明。它必须解析
完整、source-protected tenant policy（版本与 canonical hash 必须匹配 grant）以及当前
static grant/physical placement 集合，并证明：

- Shard cuts 的 `sum(max(effectiveGrant, grandfatheredUsage))` 逐维不超过 tenant hard
  policy；Target grant 始终受同一 Source Shard cut 限制，不能把 Target 与 shard grant
  重复计入静态 cut 总和。
- 初始、增加、减少、零 grant 均对应精确注册的 ControlRef/request 与 frozen scope；
  新 allocation 的 non-borrowable physical reservation 已绑定，不能在线借用其他 shard
  当时看起来空闲的资源。旧 source/key/policy 证据按恢复保护窗口保留。
- TransferRef 必须解析到完整 immutable parent plan，匹配 operation/request/plan hash、
  tenant policy、完整 old/new grant sets 及本 publication 的归属。只携一个 ref 不够。
- Donor shrink/hold markers 先 source 生效；超出新 grant 的 usage 继续占 donor 和 tenant
  envelope。源端 GRANT_SHRINK_DRAINED 须重查完整 counter digest/source/usage；所有 donor
  已排空且 recipient physical placement 已预留后才增加，维持每 tenant policy 单 plan，
  increase 后只按该 plan roll forward。未经这些证明不能释放 donor 的容量。

上列规则沿用主设计 §18.2；本批没有创建代替实际 policy/placement/transfer 后端的
无条件默认实现。测试中的允许/拒绝回调用于校验调用边界，不能作为生产 CapacityAuthority。
所有权限、Route、key、registration 和 capacity snapshot 必须在 Owner/Store/source
guarded commit 内仍有效；C4 后端需以版本/CAS/受保护 reservation 保持这个条件。

`Change.requireCurrent` 在实际 Store guard 内复核完整 before grant/aggregate/total bytes、
精确不存在、Store source/sequence。该 activation 必须与本次 quota bookkeeping、Control/
SystemMutation result 和 source 在同一个 WriteBatch 提交；成功后才发布内存当前 grant。
外部 authority 拒绝、transient exception 或 fatal Error 都不会返回可提交 Change；不能
据此推进 source 或把异常吞成成功。写结果不确定时恢复 Store，禁止先改 current grant。

下调 grant 不重写 counters、旧冻结 artifact 或历史 charges，不直接释放已有 physical/
retained obligation；新入口/既有工作仍遵循 §10.4。实际 release、完整账本 owner 与
bookkeeping 源继续由本 B4 剩余契约和 C4 的原子装配完成，不能因本地激活对象存在而提前回收。

## 12. Accounting projection 自身的固定存储预留

### 12.1 唯一 root owner 与不可递归的费用

Counter/aggregate/total/grant activation 是计费投影，其编码含 usage、版本或历史 grant；
不通过“编码后再把长度加回自身 usage”求固定点。每个 Source Shard 的新格式 Store
只有一个 `TargetQuotaBookkeeping` 锚点，归属固定的 SHARD primary identity；其
accountingIncarnation 必须与 aggregate 的 shard accounting incarnation 一致。对应
TENANT_SHARD mirror 使用 Route 的 immutable tenantRoutingScope。

Root identity、完整 accounting artifact 和物理 source 身份在该 Store 格式生命周期
内冻结。后续 grant 更新不重定价这些已承诺的投影槽位；新增槽位也使用该 root 已冻结
的同一计量契约。新业务和 attempt 使用各自已接受的 accounting artifact。若改变
schema bundle、root incarnation 或 root 的计量规则，必须走 B6/F1 的受控格式转换，
普通 grant、重启或缺记录不构成重置锚点的许可。

锚点只承担下表列出的 accounting projection 元数据，不承担 Target 的 Message、
Reservation、Claim、attempt execution/payload 或普通结果/evidence 费用。它不会将
这些业务费用从 Target total 隐藏到 SHARD；业务记录的 owner 仍按 §9 和后续完整账本
规则确定。Root primary 与 tenant mirror 本身是两条不同的物理记录，分别占一个槽位；
**两条记录的总预留只通过 root primary 加入 aggregate 一次**。

| 投影记录 | 固定 key 预留 bytes | canonical typed payload 上限 | 唯一费用 owner |
|---|---:|---|---|
| Bookkeeping anchor（NV 31） | 22 | 本类 schema bound，含一个完整 mutation source | root SHARD；同额 tenant mirror |
| Shard aggregate（NV 27） | 22 | aggregate schema bound，含一个 source | root SHARD |
| 每条 primary 或 mirror counter（NV 26） | 103 | counter schema bound，含一个 source | root SHARD；包括全零退休 counter |
| 每条 Target total（NV 29） | 87 | total schema bound，含一个 source | root SHARD；包括 usage=0 的 total |
| 每条 Shard/Target grant activation（NV 30） | 87 | activation schema bound，含完整 OPEN origin 和两个 source | root SHARD；同 key 更新不分配第二槽位 |
| 每条 attempt budget（NV 28） | 34 | budget schema bound，至多两个 mutation source | budget.primaryIdentity / tenantIdentity，使用 budget 自己的冻结 artifact |

表内使用每类合法 key 的固定最大值，短 key 的余量不因 branch 切换而返还。预留已包含
该记录全部编码 bytes，不再叠加它的 actual record charge；普通 STATE/RESULT/SYSTEM/
EVIDENCE/WAL 的实际字节公式仍按 §7。Attempt budget 的记录预留在其 commitment/allocated
之外单独计一次，不能把预算记录本身再次放入它所覆盖的 allocated record 集合。

### 12.2 Source 身份决定的有限上限

所有表内 schema 上限沿用各 Java codec 的固定 `MAX_CANONICAL_BYTES`，其中全局
SourcePosition 上限替换为本 Route 的固定 `S`：

```text
Kafka S = 当前完整 canonical SourcePosition 长度 + (没有 leaderEpoch 时补 4 bytes)
Pulsar S = 当前完整 canonical SourcePosition 长度
payloadBound(type) = type.MAX_CANONICAL_BYTES - sourceCopies * globalSourceMaximum + sourceCopies * S
slotBytes(type) = keyBound(type) + payloadBound(type) + 12 + frozenRecordOverhead
```

Kafka cluster UTF-8、native topic UUID、Route Incarnation 和 partition 固定；offset、
time 和 epoch 都是固定宽度，始终为可选 epoch 预留空间。Pulsar resource incarnation、
完整 physical topic、partition 固定，ledger/entry/batch/time 同样固定宽度。`S` 不取
当前数值的 varint 长度，也不取字符数代替 UTF-8 字节数；后续记录必须属于相同完整
物理 source。改 topic、UUID/resource incarnation 或 tenant 不能沿用旧预留。

Protobuf 外层及嵌套长度前缀仍使用原 schema 的保守上限；不因 source 变短而低估前缀。
因此不需要给短 Kafka source 使用全局 1 MiB Pulsar topic 上限，也不会在 offset、epoch
或 revision 增长后要求额外向自己收费。上限依赖精确 schema bundle，未知 schema 必须
拒绝，不能套用旧值；C4 激活时证明 artifact 与运行的 codec bundle 一致。

锚点持有三个非负 long 数：`counterRecords >= 2`（含固定 root primary/mirror）、
`targetTotalRecords >= 0`、`grantActivationRecords >= 0`。Anchor 与 aggregate 各一条，
故 root 预留为：

```text
rootProjectionBytes = slotBytes(anchor) + slotBytes(aggregate)
                    + counterRecords * slotBytes(counter)
                    + targetTotalRecords * slotBytes(total)
                    + grantActivationRecords * slotBytes(activation)
```

所有乘加与 inventory 增减均 checked。先减真实删除、后加真实创建，允许接近 long
上限时合法净零变化；下溢、总和/乘积溢出、移除 root 对都失败，不饱和或隐式置零。
这些只产生 LOGICAL_STATE_BYTES（维度 3），不增加任何 payload/execution/cardinality，
也不冒充 RocksDB/RSS/WAL/temp 物理池证明。后者和控制/结果预留仍由 C4/E6 同时约束。

独立 Kafka `kfk` 样本 `S=65`、recordOverhead=32 时，anchor/aggregate/counter/total/
activation/budget 槽位分别为 565/1328/1518/1482/3872/2798 bytes。初始 root 的两个
counter、anchor 和 aggregate 共 4929 bytes。它们是有限逻辑预留，不是磁盘测量值。
Counter/aggregate/total 的上限各含 11 bytes 可选 local ordinal；source-only 投影
使用 `MAX_SOURCE_CANONICAL_BYTES`，不额外保留不允许出现的 ordinal。

### 12.3 Anchor wire 与记录生命周期

`TargetQuotaBookkeeping` schema 1，预留 **NV 31 / meta tag 18（十六进制）**；key：
`18 01 | sourceShard[20]`，共 22 bytes。Value exact fields：

| field | 内容 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 SHARD TargetQuotaIdentity，冻结 root owner/incarnation |
| 3 | 非零 tenantRoutingScope[32] |
| 4 | 完整冻结 TargetQuotaAccounting |
| 5 | counterRecords，非负范围且至少 2 |
| 6 | targetTotalRecords，非负 long |
| 7 | grantActivationRecords，非负 long |
| 8 | 非零 raw uint64 local revision |
| 9 | 完整 TargetQuotaMutation |
| 10 | `nereus-delay-target-quota-bookkeeping\0` + fields 1–9 的 SHA-256 |

First allocation revision=1；只有 inventory 数量真实变化才推进本记录 revision 和 stamp。
未变更 inventory 不改写锚点。原地改 counter usage、升级同 key grant、预算 phase 变化
都不增加 projection 槽位。不同类别数量变化但总费用碰巧相等时，锚点仍更新，root
counter usage 未变则不改写 root counter；因此不要求 anchor 与 counter revision 相等。

首次格式激活必须有 source-bound authority 与足够 shard logical/physical 预留，在
同 batch 创建 anchor、root primary/mirror、aggregate 及该批其它投影。初始 inventory
按实际记录计算，不能先推进 source 后补写根。Root 对不能通过普通退休/GC 删除；
只在完整 Store 被受控停用、保护全部解除后由 B6/F1 处理。构造器不提供此权限。

退休 Target counter、usage=0 total 或 zero grant 只改变业务意义，其投影预留继续保留。
只有完整 source/Floor/catalog/pin/replay/query/retention 保护允许且实际记录删除成功，
才递减对应 inventory。更换 accounting incarnation 不覆盖旧记录或免除旧槽位。

Attempt budget 在 ADMITTED、UNKNOWN、RESOLVED_AWAITING_FLOOR、RETAINED、RELEASED
五阶段均持有同一固定记录预留。`effectiveCharge()==0` 只表示其内部 execution/reserve
已释放，不表示 NV 28 记录或所属 incarnation 可以删除。删除预算记录时必须覆盖它的
**最新** mutation（包括最后 RELEASED 更新），保证无 writer/attempt/重放/查询保护后，
与所属 Target/mirror counter 减额同批；最终删除前不向新 incarnation 转移该费用。

### 12.4 C4 原子接入与独立 inventory 审计

正常 mutation 只从实际 before/after WriteBatch 记录集计算 CREATE/DELETE。Value 更新
计数 delta=0，source replay/Command/SystemMutation 去重先返回首次结果，不能再分配。
将同类创建/删除合并后更新 anchor；root 投影预留差额进入 root primary/mirror，和其它
Target counter/total/aggregate 计划共享一次 source-ordered 原子 batch。Anchor 的完整
prior bytes、原记录存在/不存在和其它读集合须在同一 Owner/Store guard 内再验证，
外部 grant/释放权威须保持有效；成功提交后才发布内存。失败/未知提交走既有 Store 恢复。

已有 Target/incarnation 的普通 Schedule 不创建上述元数据，仍维持原四条 quota 写。
首次创建 Target counter 对与 total 时额外改变 anchor 和 root counter 对，共增加
三条 quota 元数据写；aggregate 仍共享一次，不新写第二份。这个数量由本次受影响
记录决定，与全部 Target/counter 数 L 无关。实际业务总写量和故障恢复由 C4/D/E 测量。

`requireRoot` 要求实际非 genesis aggregate 的 root incarnation、费用覆盖与 source/sequence
一致，同 source 还须完整 mutation digest 相同；允许 anchor/aggregate 各自独立更新。
`auditInventory` 仅在恢复/显式审计使用，输入该实际 aggregate 和解码的 counter、total、
activation 记录，包括退休与零 usage 记录；检查完整 source/tenant、重复 key、root 对存在、root
与 mirror 覆盖固定预留，并与锚点三个计数核对。它不能从锚点数字生成假枚举。还必须
调用独立业务 ledger 重建和 aggregate/source/sequence 审计；inventory 数相符不证明
业务 usage、source 顺序或完整 Store 恢复正确。本批没有生产 inventory reader、原子
writer 或受保护删除实现。其余业务记录 owner、payload/identity 保护与 incarnation
分配/退休仍按 B4 原验收闭合，不以本锚点代替。


## 13. 唯一 Message payload owner

`TargetQuotaPayloadOwner` schema 1，预留 **NV 32 / meta tag 19（十六进制）**，key：
`19 01 | DelayMessageId[41]`，共 43 bytes。每个 Message identity 只有一个 owner，不包含
current generation、PublishAttemptId 或 Profile。现有 DLQ replay 沿用 Message ID 并推进
generation；payload 费用不能随 replay、Claim、Admission 或每个旧 attempt 复制。

### 13.1 冻结身份与完整字段

| field | exact 内容 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 DelayMessageId[41] |
| 3 | 完整原始 TARGET primary identity，包含原 accounting incarnation/Shard/Target |
| 4 | 非零已认证 tenantRoutingScope[32] |
| 5 | 完整冻结 TargetQuotaAccounting artifact |
| 6 | 原始完整 TargetScheduleBinding 的非零 digest[32] |
| 7 | kind：INLINE=1、OBJECT=2 |
| 8 | 非负 payload length，OBJECT 上限 Long.MAX_VALUE；INLINE 上限 16 MiB |
| 9 | payload SHA-256[32]；这是内容摘要，不把全零值当成缺失标记 |
| 10 | OBJECT 必填非零 reservationId[32]；INLINE absent |
| 11 | OBJECT 必填非零 objectStoreProfileHash[32]；INLINE absent |
| 12 | 可选完整 committed PayloadReference（wire version=2），四个身份 component 各至多 1 MiB |
| 13 | phase：RESERVED=1、ACTIVE=2、RETAINED=3、RELEASED=4 |
| 14 | 非零 raw uint64 local revision；不得超过完整 mutation sequence，耗尽拒绝 |
| 15 | 完整 TargetQuotaMutation，保留 source 与精确已接受 body 的 digest |
| 16 | 非零冻结 recoveryLineage[16] |
| 17 | RELEASED 必填非零 releaseFloorDigest[32]，其它 phase absent |
| 18 | `nereus-delay-target-quota-payload-owner\0` + fields 1–17 的 SHA-256 |

字段 presence、wire type、版本/枚举、排序、digest 与重编码必须全部匹配。Owner 只能是
TARGET；Message、owner、mutation 的 Shard 相同。Store decoder 另验证完整 key 与
已认证 tenant；codec 不由 tenant hash 或 source DTO 推导授权。Kind/phase 使用显式
wire number，不依赖 Java enum ordinal。最大 schema envelope 同时包含完整有界 Source
与完整 object reference；独立 Python 的最大样本只证明结构/编码上界，不证明实际已
认证过的 binding、物理 grant 或可执行 mutation trace。

第一次 Schedule 从完整 binding 的 inline bytes 计算长度/SHA，或保留完整 committed
object identity。第一次 Prepare 从完整 Prepare body 取得预期长度/SHA/Profile；
reservationId 必须由真实已认证 Prepare/receipt authority 提供。两者必须与 allocation
stamp 的**完整 binding source bytes**相同，不能只比 offset。Factory 之前 C4 仍须执行
source/Command 去重、tenant/grant 检查、ReservationReceipt/commit proof 及 Store
存在性检查。Decoder 或 factory 返回对象不是一次已经完成的资源分配。

`requireInitialBinding` 重验原始 Message/Target/incarnation、完整 binding digest、
Prepare 意图或实际 Schedule payload；binding source 不晚于 owner 最新 source，相同
位置必须连 epoch/timestamp 等完整元数据也相同。Prepare 的 reservationId 另由实际
receipt 验证。`requireMessagePayload` 要求 Message ID/Target、长度、inline SHA 或完整
object reference 一致；RESERVED、RELEASED 和未提交 object 不可伪装成可读取 Message。
它单独验证 payload 身份，当前 generation/state/执行与 grant 权限由实际 committer 检查。

### 13.2 费用、转换与存量保护

| phase / 动作 | payload 费用 | 必须保持的条件 |
|---|---|---|
| INLINE/已提交 OBJECT Schedule → ACTIVE | 1/2：一条 active message + 冻结 bytes | 原始完整 binding/commit proof 已认证 |
| Prepare → RESERVED | 5/6：一条 reservation + 冻结 bytes | 无 committed reference，真实 reservation 归属 |
| RESERVED commit → ACTIVE | 释放 5/6，增加 1/2 | 完整 reference 的 length/SHA/reservation/Profile 精确匹配 |
| ACTIVE 或 RESERVED → RETAINED | 释放原 bucket，增加 4 | terminal/expire 的真实 source 与业务 ledger 支持；未提交对象仍按预留量保守持有 |
| RETAINED DLQ replay → ACTIVE | 释放 4，增加 1/2 | 必须存在原 inline 或完整 committed object；未提交过期 reservation 不可 replay |
| RETAINED → RELEASED | 释放 4 | 最新引用/写入者、Floor、retention 和实际删除权威全部允许 |

RELEASED 不可重入其它阶段。所有转换要求同一物理 Source 的 source position 与 raw
sequence 严格前进，并独立递增 owner revision。转换后的完整 owner 交给强制
`TransitionAuthority` 验证；异常包括 fatal Error 原样传播，不返回成功 next。生产
没有默认 allow/no-op authority。此接口要求完整已接受 source body 与真实 before/after
ledger、grant、保护状态，且检查在同一 Store guard 到 commit 期间仍有效；测试回调
不是生产 authority backend。一次恢复解码不允许跳过完整账本验证后直接使用费用。

Payload owner **不含 inline 副本**，仅持有其长度/摘要；object 持有完整不可变身份。
其 NV record 单独按 STATE 维度 3 计一次实际
`key length + canonical owner length + 12 + frozen recordOverhead`。它不嵌入 usage，
没有递归计量。Owner 每次只在 payload 生命周期转换时写入，Claim/Admission/Reschedule
不改它；attempt 的执行 7/8 和 reserve 继续由独立 budget 持有。零 bytes 的 ACTIVE
仍占一条 message；零 bytes RETAINED 仍有 owner record 费用。

原 Target/accounting incarnation、tenant 和 artifact 跨 generation 冻结，不能因
DLQ replay、Profile/grant 更新或当前 Message locator 使用新 incarnation 而重定价、
重新获得 payload cap。Tenant mirror 与原 primary 同额，aggregate/跨 incarnation
Target total 只计 primary。当前 generation 的 attempt 使用其自己的冻结预算。该 helper
不授权 retarget/migration 或免除新 DLQ ingress grant；C4/B6 必须证明合法代际关联。

### 13.3 Release、记录删除与容量边界

Payload release 的 Floor lineage 必须匹配；完整 applied Source 与 sequence 覆盖
owner **最新** mutation，两者相对顺序一致，同位置还须完整 bytes 相同。Floor 又必须
严格早于本次 release source/sequence。除此以外，真实 authority 须在提交期间证明
latest payload references、所有 generation 的旧 attempt/writer、catalog ancestry、
query/replay/retention、pins 及实际 provider/Store 删除完成或有等效权威证明。只覆盖
owner mutation 的 Floor 不能替代仍在引用 payload 的其它账本的保护证明。

RELEASED 后 owner NV record 继续计 STATE，直到其最新 RELEASED mutation 及所有身份/
重放保护也允许真实删除，再与原 owner/mirror/total/aggregate 减额同批。不能用
`payloadCharge()==0` 提前退休 accounting incarnation 或丢失 dedupe/旧 source 义务。
初始 binding 验证是现存账本核对入口，不要求已受保护删除的初始 binding 永久重建；
其删除仍须满足自身的身份/重放保护，保留 owner 的 accounting 与 release proof。

现有 Message 与原始 Schedule binding 可能保存 literal inline bytes，source/结果/
evidence 也可能保留 payload 内容。这些已编码副本继续按各自唯一 record/WAL 类计费，
不能成为 RELEASED 后未重新授权的 payload fetch/replay 来源。Release 关闭应用 payload
读取/重放权并完成实际 primary Message/provider 清理，不表示所有历史证据 bytes 已
物理抹除。不得把 Message payload 替换成伪造空 bytes 绕过身份或存储计量。

Commit 增加完整 object reference、后续 source/revision/Floor 变长，会增加 owner record
费用。首次 Schedule/Prepare 必须为已承诺后续动作保留足够 metadata/physical/outcome
容量，grant 下调不能卡死存量 commit/terminal。完整有限 reserve sizing、其它业务记录
owner/实际 before-after ledger、tenant/domain unique cardinality、incarnation 分配/退休与
legacy handover 仍在 B4 §6 中闭合。本批没有把这些设计缺口交给 C4 临时猜测；C4 负责
冻结契约后的真实 authority backend、原子写、独立恢复与受保护删除。


## 14. Message 家族的实际记录计费来源

`TargetQuotaMessageRecords` 将 §13 的冻结 payload owner 与已有完整 record decoder
连接。它不创建额外 key/NV schema，不替换活动 Lane reader；Store 适配层必须先按
预期 value type 验证完整 NV header/length/CRC，再把实际 key 和 canonical typed payload
交给对应入口。不得传入整帧后再加一次 12-byte NV overhead。

### 14.1 唯一 owner 与闭合角色

下表每条**实际持久记录**只占 STATE 维度 3，owner 为原 Message 的 TARGET primary、
原 tenant mirror 和完整冻结 accounting artifact；不由当前 locator incarnation/Profile
重新选择 artifact。共享 ORDER_STATE/queue、Claim、attempt、结果和控制记录不在本表内。

| 角色 | CF / NV | 实际 key 来源 | 完整校验来源 |
|---|---|---|---|
| PAYLOAD_OWNER | meta / 32 | `19 01 + MessageId`，43 bytes | key/Shard/tenant 与完整 owner canonical bytes 精确等于当前计费视图 |
| INITIAL_BINDING | id / 21 | `06 01 + initialBindingDigest`，34 bytes | 完整 retained Schedule/Prepare binding 及 §13 initial-binding 校验 |
| MESSAGE | id / 15 | `05 01 + MessageId`，43 bytes | 完整 key/Shard、Message/runtime、不可变 payload 与原 Target |
| DUE | timeline / 14 | 完整 ordinary work key，106/118 bytes | Message 当前 runtime 的全部 work bytes、eligibility、Source order token、locator |
| NATIVE | timeline / 14 | 完整 native work key，106/118 bytes | 与普通 work 同一完整 value，但必须确实具有 Native candidate |
| ORDERED | timeline / 14 | 完整 FIFO key，128/140 bytes | Message 的完整 FIFO work、ordering domain、原业务时间/source order |
| ORDER_HEAD | timeline / 14 | 完整 serviceable-head key，84 bytes | 实际 ORDER_STATE 的 OPEN/serviceable/no-barrier、Message/work 与未决 attempt 约束 |
| EXPIRY | timeline / 16 | 完整 expireAt/Target/Message/generation，87 bytes | 完整 locator/expireAt 与当前非终态 Message 相符 |

DUE/ORDERED 的两种长度来自 Kafka/Pulsar 的完整 source-order token，不能截断后按较小
长度计费。Native 与 ordinary 即使 value bytes 相同，两个实际 key 各占一次 record 费用；
这是两个存储副本，不再增加 payload 1/2、5/6、4 或 execution 7/8。Generation runtime
嵌在 Message value 中，不另计一条不存在的独立 runtime record；head/barrier 嵌在共享
state 中也不由本 helper 再造一条费用。

Message 与 owner 必须来自同一 Source kind/Shard/完整物理资源，相同 source position
还须包含 timestamp/epoch 在内的 canonical bytes 相同。后续合法 Reschedule 可使用
更晚的 source，不能要求每次 Reschedule 都重写 payload owner；实际 accepted source/
Command/current binding/compatibility/grant 仍由 C4 完整权威校验。本表不以 payload
身份相同证明该 Message 的所有业务状态已被授权。

### 14.2 有界记录检查与 exact before/after

各入口返回不可由外部构造的 immutable Record，持有 role、CF、valueType、完整 key/
typed bytes、完整 owner 视图与（适用时）完整 Message 的 digest。费用只从已验证实际
bytes 计算 `key.length + typedPayload.length + 12 + frozenRecordOverhead`，拒绝溢出。
`requireStored` 可再次比较实际 CF/type/key/typed bytes；其检查与 owner、Message、共享
ORDER_STATE、Source/Owner/Store read set 必须在真实原子提交的 guard 内保持有效。

`total` 只对同一个 exact owner/Message 视图下的**已提供子集**求和。固定硬上限为
6 条：owner、initial binding、Message、Expiry 各至多一条，再加 DUE+NATIVE 或
ORDERED+ORDER_HEAD 两条。调用者可以声明更小上限，不能放大为任意 list；先检查条数，
再拒绝重复 `(CF,key)`、混合 owner phase/revision/artifact 或混合前后 Message 版本。
同一个 key 只计一次；不同 key 的真实副本分别计费。空子集返回零，但**不证明 Store
不存在其它记录**。这一上限仅覆盖本表同一视图，不是全部 source mutation 的写量上限。

C4 必须从同一 Store view 的实际 before 记录和实际拟提交 after WriteBatch 取得集合，
分别验完整 bytes 后，先减全部 before 贡献，再加全部 after 贡献，与已冻结 counter/
total/aggregate delta 同批。不能把旧、新同 key 的 Record 混放在一个 total 中，也不能
因 helper 没收到某条记录而推断它已删除。Owner 相位变化但 record 未删除时，要按新的
完整 owner bytes 保留它的 STATE 费用。实际 payload ownership 由 §13 另算一次。

| 原 §9 动作 | 本表实际 before/after 来源 | 仍须另外覆盖的账本 |
|---|---|---|
| 首次 Schedule/Prepare | 新 owner/initial binding；Schedule 的 Message 与实际存在的 timeline/Expiry；Prepare 尚无 Message | source/Command 去重、Receipt、grant/registry 分配、结果及未来容量预留 |
| reservation commit/expire | owner phase/full bytes 与实际新增 Message/索引；过期未提交 reservation 不伪造 Message | commit proof/provider、reservation/GC 记录和 retained 保护 |
| Reschedule | 同一 Message key 的完整 value 差额，实际旧/新 work key 与需要变化的 Expiry | source 业务有效性、Claim 撤销、结果与控制记录 |
| Claim/revoke/Admission | Message 当前 work 分支及实际删除/重建的 work 索引；稳定 Expiry 不凭 runtime revision 重写 | exact Claim 7/8、Claim record、attempt budget/reserve/Admission/Journal |
| failure/UNKNOWN/Outcome | Message/runtime、实际 retry work 与仍存在的索引；未变化的 owner 不额外分配 | 所有旧 attempt 的预算、完整结果/evidence/WAL/physical 义务 |
| Cancel/terminal | 实际移除 timeline/Expiry、保留 terminal Message 及转换后的 owner record | Claim/旧 attempt、terminal/GC/query/source 保护与结果 |
| DLQ replay | 同一 Message key 的新 generation 与实际新 work/Expiry；原 payload owner 不重定价 | 当前新 ingress grant、完整绑定/代际来源与旧 attempt |
| retained release/record GC | RELEASED owner 仍收费；真实删除 Message/binding/owner 时才减各条 STATE | §13 完整 Floor、引用/写入者/查询保护、实际 provider/Store 删除、身份退休 |

未应用 source 的重复处理先由真实 Result/SourceAdvance 账本 dedupe；不能再次构造一套
CREATE 费用。一般原地 rewrite 按真实 canonical before/after 长度差额计量，费用没有
“每次摸到 record 再加一次”的规则。恢复时 C4 必须枚举真实记录、验证本表关联并独立
重建所有其它业务账本；本 helper 不能给 partial list 签发完整恢复或完整 quota 证明。

本批独立向量重用完整既有 record corpus，并由 Python 计算 key/typed-payload digest/
STATE 长度；这些是结构与计量证据，不声称该测试拼接已有真实 source/Route/grant 授权。
剩余共享 identity/queue/domain/control 与 Claim/result 等 owner、完整 reserve sizing、
unique cardinality、incarnation lifecycle 和 legacy handover 继续按 §6 闭合。C4 真实
读集合、原子写、完整恢复和受保护删除仍是后续必要实现，B4 保持 IN_PROGRESS。


## 15. Accounting incarnation 的来源、唯一计数与退休

`TargetQuotaIncarnation` 固定一个 source-derived primary owner 的完整 origin，以及
一次不可逆 ingress drain。预留 **NV 33 / meta tag 1a（十六进制）**，没有 tenant
副本 descriptor；tenant counter 只镜像费用和局部 queue/domain 归属，incarnation 数
始终为零。该类不自动执行分配、轮换、删除或 Store 激活；完整真实 authority 仍必需。

### 15.1 确定性 ID 与闭合记录

每次**实际首次分配**使用以下 canonical protobuf 输入：1 schema=1；2 完整不含
incarnation 的 TargetQuotaScope；3 完整 TargetQuotaAccounting；4 非零 lineage[16]；
5 完整 allocation TargetQuotaMutation（raw sequence、完整 physical SourcePosition、
精确已接受 Command/SystemMutation bytes 的 digest）。ID 是
`SHA256("nereus-delay-target-quota-incarnation-id\0" + input)` 的前 16 bytes。
全零结果失败关闭，不随机重试或增加隐式 nonce。Descriptor 的 full identity 必须能由
保留的完整输入重新计算；仅给出相同短 ID 不证明同一个 origin。

Key 沿用 primary counter identity 的完整 suffix，换为独立 tag：
`1a 01 | primaryKind[1] | sourceShard[20] | accountingIncarnation[16] | [TargetId[32]]`。
SHARD kind=3/key=39 bytes，TARGET kind=1/key=71 bytes；拒绝 mirror kind。

| field | exact 内容 |
|---:|---|
| 1 | schema=1 |
| 2 | 完整 source-derived primary TargetQuotaIdentity |
| 3 | 已认证非零 tenantRoutingScope[32] |
| 4 | 完整冻结 accounting artifact |
| 5 | 完整首次 allocation mutation |
| 6 | 非零冻结 recoveryLineage[16] |
| 7 | 可选完整 drain mutation；absent=OPEN，present=DRAINING |
| 8 | `nereus-delay-target-quota-incarnation\0` + fields 1–7 的 SHA-256 |

Drain 必须在同一物理 Source 上严格晚于 allocation（position 与 raw sequence 都前进）。
没有冗余 phase/revision 字段：该 descriptor 只分配一次、drain 一次；重复 source 先走
实际 dedupe，不能再构造 OPEN 或重复 drain。未知/缺失/重复/错序字段、key/Shard/tenant、
派生 identity、版本、digest 或非 canonical 编码均拒绝。解码同时检查可计费的固定
record envelope，溢出发生在 allocation authority 被调用之前。

正常 Schedule/Prepare/DLQ ingress 从**当前已认证 Queue/Store registry 指向的完整
existing descriptor**取得 owner；不能每来一个 Command 就用它的新 source 再派生
一个 ID。第一次分配的 StateAuthority 必须检查完整已接受 source body、实际 prior
不存在、counter tombstone 不存在、当前 grant/accounting、registry 和容量 reservation。
同 ID/full origin 已存在时按实际 Source/Result 去重；短 ID 冲突、已退休 counter 或
不同 origin 占据该 ID 时失败关闭，不能覆盖或复活。Lineage、物理 Source/tenant 与
root artifact 的变更不能由这个 factory 授权，仍走 B6/F1 的受控格式边界。

Allocation 还必须具有非循环的 source 顺序：需要新 incarnation 的 membership/channel
注册或任何已签名绑定只能引用**已在前序 source action 分配并返回的 ID**。不能用
已经包含该新 ID 的注册 body（或尚未确定的 Broker offset）反过来求自己的 ID。独立
首次 Target allocation 的前序 grant request、持久 origin 和 read-set 检查现由 §16
规定；root bootstrap、轮换/handover 与实际 Result/apply 仍由后续 B4/B6/C4 source
配方和后端完成。StateAuthority 必须检查这条真实前置关系，DTO/factory 不代替后端。

当前 `TargetQueueState.requireSuccessorOf` 不允许修改 accounting incarnation；普通
原地演进继续复用它。本批没有绕过该规则提供热轮换。原设计的 Queue/accounting
轮换 source 协议、保护中的 legacy owner handover 与新 Store bootstrap/activation
仍须在 B4/B6 剩余设计中闭合，不能把“可派生新 ID”当作允许它进入当前队列的证明。

### 15.2 记录费用与唯一 cardinality 来源

Descriptor 为自身持有固定 STATE 预留和一个 accounting incarnation，不把自身 usage
嵌入编码。令 S 为 §12 的 immutable physical Source bound（Kafka 包括可选 epoch）：

```text
descriptorPayloadBound = MAX_CANONICAL_BYTES - 2 * globalSourceMaximum + 2 * S
descriptorStateBytes = actualKeyLength + descriptorPayloadBound + 12 + frozenRecordOverhead
ownContribution = (STATE=descriptorStateBytes, targets=0, domains=0, strictDomains=0, incarnations=1)
```

两个 source 槽位覆盖 allocation 与未来 drain，OPEN/DRAINING 的承诺相同；artifact、
完整 physical Source 与 lineage 冻结。已关闭 ingress 不等于费用减少；不因改 Profile/
grant/cap 自动重定价或分配新 origin。Descriptor 的 tenant contribution 只有同额 STATE，
incarnation=0；没有新的 root inventory 类，因为此条记录由自身 primary 支付，已有
counter/total 槽位仍按 §12 由 root 付费。

| 实际记录 | 唯一 cardinality 贡献 | STATE 费用与 owner |
|---|---|---|
| Incarnation descriptor | accountingIncarnations=1；mirror=0 | 上述固定 envelope，归自身原 primary |
| 当前持久 QueueState | targets=1；executionDomains=ACTIVE+DRAINING 槽位数 | 完整实际 key/typed bytes，归 queue 指向的完整 descriptor |
| 每条持久 OrderState | strictOrderDomains=1 | 完整实际 key/typed bytes，归 state 保留的完整 descriptor |

QueueState 为 PAUSED/CLOSED 仍有 Target 身份；DRAINING 槽位仍计数，只有实际进入
VACANT 才释放该执行域计数。VACANT 保留 slot generation 历史，由 QueueState record
费用覆盖。OrderState 即使 CLOSED/empty 仍占 ordering-domain identity，不能因没有
serviceable head 或无当前 Message 而漏计；真实受保护删除后才释放。共享 state 内嵌
head/barrier 不额外造一条 domain 费用。每条 record 的 incarnation contribution 为零，
只有 descriptor 贡献那个唯一的 1，再与其它业务费用相加生成 primary counter。

Queue 入口验证完整 key/physical Target、已激活 domain 上限与 descriptor Target/inc；
严格域入口验证完整 key/Target/inc/Source Shard。CLOSED retained OrderState 可继续
独立计费，但不因此获得 live queue 服务权限。未通过完整 source/control/Store guard
不得把原 state 的 accounting 字段改成新 ID 转移费用；合法受保护替换按真实 before
全减、after 全加处理，并保留原 Target total 和 Source aggregate 的守恒。

这些是 **Source Shard 内**实际唯一 key 的贡献来源，不是按每个 Message、Profile、
channel 或 incarnation 再遍历一遍 domain。相同物理 Target 跨 Source Shard 的全租户
切分/保守计数、租户 hard policy 与任何受控 reallocation 继续由完整 grant/control
契约校验；本 helper 不伪造一个跨 Worker 实时去重总数。正常 mutation 仅处理实际变化
的 Queue/OrderState 点记录；完整恢复才枚举全部实际 key 并独立求和。

### 15.3 Ingress drain 与保留归属

`requireNewIngress` 要求 descriptor OPEN 且完整 accepted accounting artifact 一致；
DRAINING 不接受新的 Schedule/Prepare/DLQ ingress。它不替代实际 queue/control/grant
准入，也不阻断已承诺工作的 Claim/Admission/Outcome、保留写入或旧 attempt 排空。
`requirePayloadOwner`/`requireAttemptBudget` 在 OPEN/DRAINING 均验证完整 primary、
tenant、accounting、lineage 与 source 不早于 allocation；同位置须完整 mutation 一致。
另一个 origin 即使 Target、tenant、grant scope 相同，也不能承接旧费用或释放旧预算。

Drain 的强制 StateAuthority 收到完整 prior/next，须验证真实 source/control 与 Store
读集合，并持续有效到原子提交；异常和 fatal Error 原样传播。没有生产 allow/no-op
实现。Grant 下调与新 ingress 停止都不能丢弃存量 payload/attempt/record 义务。

### 15.4 最终退休与 Counter tombstone

`requireRetirable` 只允许 DRAINING，并要求实际 primary counter usage **恰好**等于
其 descriptor ownContribution、actual tenant mirror 恰好等于 tenantContribution。
因此 Queue/strict-domain、其它记录、retained payload、旧 attempt、结果/evidence 或
任何其它正费用都必须先排空。零 counter 也不能代替这个 before 状态，提前清零后再
删除 descriptor 属于错误。Counter 的完整 source/sequence 不得早于或矛盾于 allocation。

完整 bookkeeping root/aggregate 必须相容，覆盖 root 与 retiring descriptor 的最低
费用及两个 primary incarnation；counter revision/source 须属于实际 aggregate。当前
Store root 自身禁止在活动 Store 内退休，其最后清退仍是 B6/F1/F3 的整个 Store 保护
边界。不能用另造 root DTO 绕过实际 Store read-set 和 authority。

Floor lineage 必须相同，完整 applied Source/sequence 覆盖 descriptor 最新 drain 以及
primary/mirror 各自**最新** mutation，二者相对顺序一致；同位置还须完整 source metadata
一致。删除 source 必须严格晚于该 Floor 和当前 aggregate source。最后强制
RetirementAuthority 验证**独立真实 ledger**、所有零费用但仍有效的引用、旧 source
replay/dedupe、全部 writer、catalog/checkpoint ancestry、pins、查询/保留与实际删除
条件；counter 数字或 Floor DTO 单独不授权退休。检查在完整 Store guard 到 commit
期间必须持续有效，helper 不执行删除或提前发布内存。

批准后实际同批删除 descriptor、扣除其固定 STATE 与唯一 incarnation，primary/mirror
变为零并保留 §10 的 counter tombstone；Target total 和 aggregate 同批更新。Counter/
total 记录本身仍占 §12 root 槽位，不递减其 inventory、不重置 revision、也不复活已退休
identity。删除 descriptor 的完整原 bytes、实际 counters/root/aggregate、SourceAdvance
和必要 Result 必须参与该原子批次。未来 source 只能通过新的真实分配产生另一个 origin。

本批冻结上述结构、计量与受保护退休检查，并不宣称实际 Source 控制后端、完整 ledger
重建或 Store 删除已实现。B4 还需完整业务 reserve、其它共享 identity/control/Claim/
result 等 owner、轮换/legacy handover source 配方及原 §17.3 最终绑定；C4/D/E/F 的真实
原子恢复、Broker、受控迁移与旧路径清退继续保留。


## 16. 首次 Target grant 的前序分配契约

### 16.1 无循环的来源与完整持久结果

复用 §11 的已注册、已签名 `PUBLISH_TARGET_QUOTA_GRANT=18` / Apply kind 17。
Request 的完整 scope/grant/accounting 不含待分配 incarnation ID，故可在 membership/
channel 注册之前先应用。首次实际接受的 signed envelope、完整 Broker SourcePosition、
本 Store 的下一 mutation sequence 和受保护 recovery lineage 一起组成 §15 分配输入。
不从后续含该 ID 的注册请求反推 ID，也不使用随机本地值、current config 或重试时的新签名。

`TargetQuotaGrantActivation` field 7 保留完整 **OPEN 历史 allocation snapshot**，field 8
改为 activation digest。该 schema 仍处于尚未接受、未启用 Target writer 的 Draft 1；
旧七字段布局不再由此 draft decoder 接受，未增加兼容读回退，也未修改活动 Lane NV reader。
OPEN snapshot 的 bound 为 descriptor MAX 减去一个 optional drain field 的完整上限。

Snapshot 不是当前可调度状态。后续 grant 更新保留它的完整 bytes，不随 descriptor drain/
retirement 改写；实际 NV 33 descriptor、queue/control/grant/registry 共同决定新 ingress。
已退休 origin 不能因 snapshot 仍 OPEN 或 grant 恢复为正值而重建、复活。历史指针与 grant
的结果/保留保护由 C4 与后续受控轮换继续实现，不能作为本批已实现 Broker/Result 后端。

### 16.2 首次分配、零额度与更新

- SHARD grant 不携带 allocation，不在本路径创建 Store root；root bootstrap 仍归 B6/C4。
- Target 从未分配且 next.limit 全零时，记录 deny-only activation，allocation 缺省。
  不因此创建 Target total/descriptor/counter；已有未知 total 不授权后续补造 origin。
- Target 没有 prior allocation 且 next.limit 非零时，必须证明当前 Target total 缺省。
  创建一次 source-derived descriptor candidate；其固定 STATE 和 incarnation=1 必须先能
  被 next grant 逐维容纳，再调用强制 capacity authority。正 payload limit 但 STATE=0，
  或足够 STATE 但 incarnation limit=0，均不能分配。异常/算术溢出不产生成功计划。
- 后续更新逐字保留 prior allocation，包含 down-to-zero 和再次 increase；不创建新候选，
  不修改 descriptor、不转移旧费用。Next 或已知 prior 非零时 snapshot 不得缺省。
  Frozen accounting artifact 不同则在 capacity 前拒绝。
- 完整 snapshot 必须是同一 Target scope/accounting 的 OPEN origin；allocation source/
  sequence 不晚于激活，顺序一致，同位置完整 mutation 相同。首次 grant 的 origin 必须
  就是本次 mutation；prior 为非零 grant 时 origin 必须严格早于本次更新。

### 16.3 Read set、容量和实际原子义务

Verifier View 显式持有 assigned nonzero recoveryLineage[16]，防御复制并纳入提交前 exact
read-set 对比；prior allocation lineage 必须与之逐字一致。该 lineage 来自实际 Store/
Manifest 的受保护元数据，不能由调用者猜测或由 decoder 当作 authority。

原签名、角色、资源、注册、Route、exact prior 和严格 source 前进检查全部先于 capacity。
`CapacityAuthority` 收到完整 body/view/source 以及可空 proposed allocation；非空仅代表
本次首次 Target 分配，空代表本次没有 descriptor 创建。后端必须在实际 Store guard 中
证明 descriptor/primary/mirror absent、root/grant/tenant policy 和静态 cuts、物理 placement、
完整控制/结果预留，并持续有效到原子提交。已有回调失败仍原样传播，没有生产 no-op 后端。

真实首次分配同批写 activation、NV 33 descriptor、primary/mirror、Target total、root
inventory/charges、aggregate、必要 Result 与 SourceAdvance；不得先发布候选 ID 给后续
membership/channel 注册。只有完整 durable source 结果被认证读取后才能引用 ID，重试
必须返回首个结果。`Change.after().allocation()` 是待提交计划，不能当作已提交结果。
实际 Result 查询/发布、dedupe、权限保留、完整账本重建和断电恢复仍由 C4/D/E 实现。

Activation 槽位现在固定预留当前 mutation 加原 allocation mutation 两份 Route-bounded
Source；保留整个 OPEN origin 的完整 accounting/identity/lineage/hash，长度不随 grant
更新次数增长。Root 只计一条 activation 槽位；NV 33 descriptor 自己仍按 §15 两 Source
槽位付费，不将嵌入的历史快照当作第二个 descriptor 或第二次 incarnation 计数。

本批完成首次 Target grant 分配计划与持久编码的契约，不完成 root bootstrap、accounting
轮换、Queue legacy handover、其它 record owners、完整 reserve sizing 或原 §17.3 的全部
验收绑定。B4 保持 IN_PROGRESS，C4 实际 atomic apply/authority、D/E Broker 恢复和 F
受保护迁移/旧路径清退继续保留。


## 17. Target metadata 的固定归属与实际记录计费

`TargetQuotaMetadataRecords` 为下面十类现有 META 记录生成完整贡献与有限 read set。
它不新增 NV/tag/CF、重复持久 payload，也不修改活动 Lane reader/writer。费用来自通过
现有完整 decoder、物理 identity、Source Shard、tenant 与 incarnation 校验的 key/value。

### 17.1 两类不可互换的归属

没有 incarnation 字段的共享 Target metadata，固定使用 §16 **首次 Target allocation**
的 owner。入口同时要求实际当前 grant activation、完整 allocation descriptor 与 physical
identity；origin identity、allocation mutation、lineage、tenant、冻结 accounting 必须逐字
相等。允许 descriptor 已 DRAINING，禁止使用同一 Target 的另一个 incarnation。
初始 deny-only grant 没有 origin，不能为共享 metadata 指定一个未发布的 owner。

该 first allocation 是共享记录的终身来源，不是“当前 queue 的 owner”别名。普通 grant
更新、down-to-zero/up、domain/channel/profile 变化均不能搬走旧费用。未来受控 rotation
仍须保留这条历史 allocation 关联；若要退休首次 descriptor，所有归属于它的共享记录及
零费用但有效的引用也必须满足 §15 的完整 ledger/reference/writer/retention 条件。不能
改写 activation origin 或重建 identity 来跳过保护；这不提前定义 B6 的轮换控制协议。

已经携带 incarnation 的 queue/order/channel/Native scope，使用其记录中的完整 Target/
Source Shard/incarnation 对应 descriptor。Native snapshot 使用完整所引用 scope 的
incarnation，不能仅凭 snapshot digest 或 current queue 推断 owner。所有记录按各自
owner 的冻结 accounting artifact 计费，保留态与 DRAINING 不自动释放费用。

| 记录 | NV / META key | 唯一 owner | 贡献与校验 |
|---|---|---|---|
| physical identity | 13 / `0b 01 + TargetId` | first allocation | 完整 canonical physical tuple 解码并重新导出 TargetId |
| dispatch compatibility | 18 / `0c 01 + digest` | first allocation | 完整 dispatch 与 physical identity 投影一致 |
| control scope | 19 / `0d 01 + digest` | first allocation | 完整 scope key/Target/Source Shard 一致 |
| membership grant | 22 / `0f 01 + digest` | first allocation | 完整 tenant、required/offered physical 投影；实际 activation source 严格晚于 allocation、同物理 source |
| membership policy | 23 / `10 01 + digest` | first allocation | 完整 tenant、offered physical 投影和 controls Source Shard |
| queue | 12 / `09 01 + TargetId` | queue incarnation | 完整 queue/physical/Shard/activated slot bound；一个 Target、ACTIVE+DRAINING 执行域数 |
| strict order state | 17 / `0a 01 + TargetId + orderingDomain` | state incarnation | 完整 key/Target/Shard；一个 strict-domain，CLOSED 仍保留 |
| channel identity | 20 / `0e 01 + digest` | channel context incarnation | 完整含 credential lease 的 channel key/Shard/Target/incarnation |
| Native scope | 24 / `11 01 + digest` | scope incarnation | 完整 scope key/Shard/Target/incarnation |
| Native snapshot | 25 / `12 01 + snapshotDigest` | referenced scope incarnation | 完整 snapshot/scope/artifacts/cap/path/key/Shard 关系 |

每条实际 META record 只计一次 `STATE = key.length + payload.length + 12 + frozenOverhead`，
checked 算术拒绝溢出；包括嵌在自己 value 内的 lease/physical/policy 字段。其它资源维度
全零，dependency 不另加费用。独立存储的其它物理副本仍应各计一次；不是由“内容相同”
免除真实记录费用。除 queue 与 strict order state 的上述 cardinality，其余记录没有
Target/domain/strict-domain/incarnation 增量；descriptor 自身唯一 incarnation 仍只计一次。

### 17.2 有限的 exact read set

Immutable Record 保存完整原 key/type/payload、owner 和贡献。所有读都是 META 单点：
本记录、owner descriptor，以及适用的 current first-allocation grant、physical identity
或 referenced Native scope。相同 key 的相同完整依赖合并，不同 type/bytes 冲突拒绝。
每个 Record 固定最多 **4** 个点读：identity 3、其它共享记录 4、queue 3、order/channel/
Native scope 2、Native snapshot 3。它不持有、扫描或复制全 Target/counter 集合。

`requireCurrent(ReadView)` 对实际已有 before 视图重新读取**所有**这些 key，比对完整
NV type 和 canonical payload；不存在、type/bytes 改变或读异常均拒绝，fatal Error 继续
传播。Key、payload、Stored 暴露防御副本，Reader 不能修改待核对的 read set。Store adapter
必须先验证 NV envelope/CRC，再在同一 Owner/Store/Source guard 下提供真实 META 读取。

对于 insert 或修改后的 proposed after，factory 只派生贡献；不能对不存在的 after 调用
`requireCurrent` 并宣称已获提交许可。C4 仍须核对真实 before/absence、实际完整触及 key
集合、owner/权限/source read set，并把 metadata、counter/mirror/total/root/aggregate、
Result 和 SourceAdvance 同批提交。相同物理 key 在一次 delta 中只计一次；记录依赖
不意味着重复创建或重复收费。独立恢复须枚举实际记录，不能从贡献对象或 counter 自抄。

四点读是单记录 attribution 的结构上限，不是 A2/E6 的完整字节/时间/RSS envelope。
长 physical topic、多个 touched records、真实 authority 查询和 Outcome/system-writer
reserve 仍必须落在激活配置的完整预算内。该 helper 不验证当前发送权限、snapshot key
trust、credential live protection 或记录删除条件；完整业务/生命周期 authority 继续保留。

本批闭合上述十类 metadata 的唯一 owner、费用/cardinality 与完整点读依赖。Claim、
Result/SystemMutation/evidence、其它共享 Shard 记录和完整 reserve sizing、真实 source
bootstrap/rotation/handover、原 B4 最终验收绑定继续实施；B4 状态保持 IN_PROGRESS。


## 18. 本地 Claim 的独立 ordinal 与有限 quota plan

现有 DelayShard 的 reversible Claim/revoke 通过本地 WriteBatch 更新 Claim、Message、
INFLIGHT/READY 与 quota，不 append source，也不推进 source mutation sequence。
Target 不能要求每个此类操作都生成新 Broker SourcePosition；同样不能为了本地记账
而推进参与 incarnation ID 派生和 Recovery Floor 的 source sequence。

`TargetQuotaDelta.prepare` 继续是严格 source 入口。`prepareLocalClaim` 必须具有
现存非零 source sequence、完整 source frontier、既有 aggregate/counters、精确
operation digest、CLAIM/REVOKE kind 和显式 `LocalClaimAuthority`。在同一 source
frontier，ordinal 从上次 aggregate ordinal 精确 +1；若 source 已前进而 aggregate
尚未触及，新 frontier 的首次 local ordinal=1。后续 source 操作 ordinal=0，并严格
推进 source sequence/position。三个量均 raw uint64，耗尽拒绝，不回绕。

Counter、Target total、aggregate 的 revision 仍按各自真正的 quota 写入递增。
本地 plan 的 read guard 绑定完整 prior aggregate、变化 counter、Store source/sequence；
随后同 source 的另一 local commit 也使旧计划失效。Total plan 继续校验完整旧 total
并纳入同一 commit。Source 派生 allocation 只能接收 ordinal=0，不能受 local revision
或本地 Claim 次数影响。Claim 冷恢复与计费重建仍须 C4 独立账本证明。

局部入口最多四个变化 counter，只能是一个完整 Target/tenant 下的 primary/mirror
配对；每个 primary 与镜像具有相同 incarnation，逐维 delta 必须相同。既有 counter
不得缺失，不创建/退休 incarnation，不修改 Target/domain/strict cardinality。
只允许 STATE（维度 3）和 Claim execution（7/8）变化：恰好一个 primary execution
owner 在 CLAIM 时增加 1 个 Claim 和其正字节 charge，REVOKE 时扣除对应 1 个及负
字节 charge。第二对可只承担冻结 Message owner 的 STATE 差额，使跨 incarnation
的 Message 与 Claim 归属不必被强行合并。维度 1/2/4–6/9–15、reserve、payload 和
Outcome 不允许变化。每个失败/读异常/fatal Error 均传播；不返回已授权 plan。

这些代数条件不能证明 Claim 合法。必须调用的 `LocalClaimAuthority` 负责在实际
Owner epoch / Store incarnation / source guard 下验证 exact durable Claim 建立或
撤销、首次冻结 charge、完整 Message/index before/after、route tenant 和完整操作
canonical digest；读集与 authority 在 atomic local commit 前持续有效。Digest 的
实际构造、Target Claim record 格式、权限后端与原子写入仍由 C4 接入，不能将任意
32-byte 摘要或测试中的空回调当作运行权威。Admission、attempt、Outcome 或 source
Control 不能借此入口绕过 source 接受/去重；内存发布仍在确定提交后，不确定写入先恢复。

RecoveryFloorRef 没有 local ordinal。仅凭同 source sequence/position 无法证明它
包含该位置之后的本地 Claim，因此 `requireCoveredByFloor` 对 local stamp 要求
Floor 的 source sequence 与位置都严格更晚；同位置 source-only stamp 仍按完整
metadata 接受。Lineage/catalog/pins、当前引用/写入者、replay/query/retention 和
实际删除权威继续由调用者证明；较晚 Floor 也不能单独授权释放。

独立 Python local 向量覆盖固定 source 的 Claim、revoke 与下一 source 操作的完整
stamp/counter/mirror/aggregate bytes；bookkeeping 向量同步增加三个槽位上限。
既有 source-only stamp 和 incarnation ID bytes 不变。该批只完善 B4 的 local
accounting 契约与有限 planner；不表示 Target Claim/Result 格式、完整 reserve、
actual Store/runtime/recovery、真实 Broker 或迁移清退已经完成。


## 19. Claim 的冻结计费投影

`TargetQuotaClaimCharge` schema 1 预留 NV **34**；META key 为
`1b 01 | sourceShard[20] | Owner epoch raw uint64be[8] | ClaimId[32]`，固定 62 bytes。
它是一个真实 reversible Claim 的计费投影，业务 Claim 继续承担完整 precondition、
materialization、current runtime branch 与 Admission/Result 的语义义务。不能将旧
Lane ClaimRecord NV 9 当作 Target 格式，也不因读到本投影取得 Claim/SEND 权限。

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 非零 ClaimId[32]，由实际 Claim 身份规则产生 |
| 3 | 完整 TargetTimelineWorkRef，含原 locator、普通/Native sibling 资格、语义与实例 digest |
| 4 | 完整 TARGET primary quota identity，与 work 的 Shard/Target/accounting incarnation 一致 |
| 5 | 非零 tenantRoutingScope[32] |
| 6 | 完整冻结 TargetQuotaAccounting artifact |
| 7 | 完整 OwnerIdentity，canonical envelope 最多 4096 bytes，沿用 durable owner envelope 上限 |
| 8 | 非零 Store incarnation[16] |
| 9 | 非零 raw uint64 Claim sequence，独立于 source sequence 和 quota ordinal |
| 10 | 非负 claim deadline epoch milliseconds，不凭到期自行释放 |
| 11 | 首次冻结 execution bytes，正数且不超过 Long.MAX_VALUE |
| 12 | 实际完整 canonical 业务 Claim typed payload 的 SHA-256[32] |
| 13 | 建立本投影的完整 local TargetQuotaMutation，必须具有非零 local ordinal |
| 14 | 非零 recovery lineage[16] |
| 15 | `nereus-delay-target-quota-claim-charge\0` + fields 1–14 的 digest |

本地操作 digest 的 canonical 输入是实际完整业务 Claim/Message/index before/after
和旧 Owner/Store/source guard；不包含由它派生的新 quota stamp/projection bytes。
业务 Claim 也不嵌入本投影 digest，防止 claim digest 与 quota digest 相互递归。
字段 12 的真实格式解码、key/type/业务身份检查由实际 Claim authority 完成，不能用
任意摘要代替。完整 payload、work、accounting 和 source 的 codec 上限仍逐层检查，
外层未知字段、非 canonical、digest mismatch、超界/零身份均拒绝。

创建入口从实际 source-derived incarnation descriptor 取得完整 identity、tenant、
accounting 和 lineage；创建时 descriptor 最新 allocation/drain stamp 不得晚于 local
creation。核对历史投影时仅要求 allocation 不晚于创建，允许 descriptor 后来 drain。Work
必须保留相同 accounting incarnation。Descriptor 已 DRAINING 不自动禁止已承诺
工作的 Claim，但也不因此允许新的 ingress；真实业务与容量许可由强制 `Authority`
检查。`create` 在返回前调用 authority，要求同一个 Store guard 下 exact Claim/
projection key 不存在、Claim 原始执行 charge、Message/index before/after、grant、
当前 Owner/Store/source 以及业务 Claim 成立。没有 production 默认回调。

费用分为两个互不递归的向量：executionCharge 只占 7=1 与 8=冻结 bytes；recordCharge
只占 3=`key.length + canonicalPayload.length + 12 + frozenRecordOverhead`。
贡献是两者相加；tenant mirror 逐维相同但不再加入 aggregate primary sum。投影中
嵌入的 work/accounting/source 是本条实际 bytes，不再作为独立实体收费；实际业务
Claim 自己的物理 record 另计一次 STATE，不能漏掉或按摘要长度代替。该投影不增加
payload、attempt/reserve、Target/domain/strict cardinality 或另一份 Message owner。

本投影建立后不更新、不 reprice。它持续关联首次 Claim，即使 policy、grant、Owner
或当前 Message generation 已变化也不按新值扣账。`requireStored` 要求 actual key、
NV type 与完整 old payload 相同；实际读取以及 Claim/Message/index/descriptor 等
完整读集由 C4 在同一 view 捕获并于 commit 前校验。

本地撤销必须仍是相同完整 OwnerIdentity 和 Store incarnation，并使用严格后续
local stamp；实际业务只能在 reversible Claim 尚存且未 Admission 的条件下撤销。
Owner takeover 不能冒用普通本地 revoke，另走明确的恢复业务证明。Source consumption
（REVOKE、ADMISSION、SOURCE_RESULT）必须是 source-only 严格后续 stamp；它可以处理
旧 Owner 的 Claim，但仍须通过已接受 source、去重与 actual business authority。
两类路径都强制回调检查首次 charge、实际 Claim 消费/删除与其它义务。校验函数本身
不改变投影、不删除记录，也不立即释放逻辑或物理容量。

在确定同一 batch 删除 projection 与实际 Claim、更新 Message/索引/attempt 及
counter/mirror/total/aggregate 后，才能扣除该原始贡献；若有保留义务则继续持有或
按完整保留配方转移，deadline、Owner loss、UNKNOWN 或较晚 Floor 单独都不授权释放。
Admission 的新 attempt reserve/原 Claim execution 交接须同一 source commit，不能
走 local shortcut。未确定写入结果时先恢复，不能提前发布可用容量。

独立 Python 向量覆盖 ordinary、Native、strict-order 三种完整 work 的投影 bytes、
key、execution 与 STATE 费用。测试同时验证 source-derived owner、draining 处理、
Owner/Store/source 消费门槛、完整旧 bytes、authority/fatal 传播、编码与边界，以及
本投影贡献接入 §18 Claim/revoke delta 的守恒。该批冻结计费投影；完整业务 Target
Claim/Result 格式、authority 后端、所有业务 record owner/reserve sizing、实际
atomic Store/recovery 与原 B4 验收仍继续实施。


## 20. 首次逻辑结果与物理 POSITION 审计

`TargetResultRecord` schema 1 预留 NV **35**，存于 DEDUPE；旧 Lane DEDUPE tags 01–05
与 NV 1–4 不变。新格式复用完整 canonical CommandResult/SystemMutationResult 和
CommandDedupeRecord payload version 2，但将其冻结 owner 与首次 source 明确纳入外层。

| Kind / wire | 新 DEDUPE key | 唯一 record-byte 类 |
|---|---|---|
| COMMAND / 1 | `06 01 + CommandId[41]` | EVIDENCE，14/15 |
| RESULT / 2 | `07 01 + CommandId[41]` | RESULT，9/10 |
| SYSTEM / 3 | `08 01 + SystemMutationId[32]` | RESULT，9/10 |
| POSITION_COMMAND / 4 | `09 01 + 完整 canonical SourcePosition` | EVIDENCE，14/15 |
| POSITION_SYSTEM / 5 | 同上 | EVIDENCE，14/15 |

表中 key 的加号表示字节拼接。两种 POSITION 使用同一物理 key，是闭合二选一；不能在
同一 source 位置同时接受 Command 和 System 分支。它不是第二份逻辑结果，也不
因名字包含 SystemMutation 就归 SYSTEM_MUTATION outbox（11/12）。COMMAND 虽保留
首次紧凑结果，整条实际记录只计 EVIDENCE 一次；单独 RESULT 记录再计自己的实际 bytes。

| field | 含义 |
|---:|---|
| 1 | schema=1 |
| 2 | 闭合 Kind 1–5 |
| 3 | 完整自路由 CommandId[41] 或非零 SystemMutationId[32] |
| 4 | 完整 primary quota identity，TARGET 或 SHARD；POSITION 必须 SHARD |
| 5 | 非零 tenantRoutingScope[32] |
| 6 | 完整冻结 TargetQuotaAccounting |
| 7 | 非零 recovery lineage[16] |
| 8 | 首次写入本条记录的完整 source-only TargetQuotaMutation，拒绝 local ordinal |
| 9 | 对应完整 canonical typed payload |
| 10 | RESULT/POSITION 必填原始逻辑记录的域 digest[32]，其它 Kind absent |
| 11 | 可选完整 OPEN allocation origin，仅成功分配控制的 SYSTEM 结果允许 |
| 12 | `nereus-delay-target-result-record\0` + fields 1–11（按实际存在）的 digest |

COMMAND payload 必须为 CommandDedupeRecord payload version 2，含完整 Client protocol tuple、非零
commandHash 与 CommandResult；Target reader 不接受其 legacy payload version 1。RESULT payload 为
CommandResult；SYSTEM payload 为 SystemMutationResult，ID 必须与外层相同，author
必须是其 mutation type 对应的闭合 AuthorIdentity。上述逻辑结果的原始 SourcePosition
必须与 field 8 完整相等，包括同 offset 的 metadata。POSITION payload 是 field 3
对应的原始 ID，不能另填另一个 ID、空串或自由形态数据。

外层最多 12 fields；protocol tuple 最多 30 canonical bytes。Command evidence payload
最多 `100 + TargetSourcePosition.MAX_CANONICAL_BYTES`；SYSTEM author 上限为 1 MiB，
先限制外层单一 author branch 和最多四个内层字段，再调用闭合 decoder。完整 payload
有独立有限上限，外层还保守容纳 source stamp、accounting/owner 和一份完整 allocation。
这些是 schema ceiling；激活后的 whole-operation bytes/time/RSS/physical 预算可以且
通常需要更小。不存在、未知字段、非 canonical、超界或 digest mismatch 都拒绝。

### 20.1 冻结归属与真实 source 权威

创建从实际 source-derived descriptor 提取完整 identity/tenant/accounting/lineage；
其最新 allocation/drain stamp 不得领先本次 creation。读取历史结果只要求 allocation
不晚于首次创建，后续 descriptor drain 不改变费用。RESULT 完全沿用 COMMAND owner、
artifact 与 creation；不能在查询或重试时重新选择 current Target/owner。

CreationAuthority 必须根据实际已接受 source 与完整业务账本选择归属，不能按调用者
提供的 counter 数字猜测：关联已冻结 Claim/attempt 的 System 结果沿用对应 execution
owner；关联已有 Message 的命令结果沿用 payload owner；成功初次 Schedule/Prepare
沿用本批新建的 payload owner。未建立可归属业务身份的拒绝、共享 source/control
结果与物理 POSITION 使用受控的 Shard owner。相同逻辑 ID 的重复记录直接返回首个
结果，不执行一次新的 owner 选择；RESULT 的 owner 不独立变化。上述选择仍需 C4
的实际 source/body/budget/权限后端证明，构造器本身不授予该权威。

强制 CreationAuthority 同时验证 source signature/tuple/hash/route、CommandId 冲突
或 SystemMutation 去重、首次结果、精确 key absence/before、完整 read set，以及
必要的 Result/evidence/writer reserve。COMMAND 与 RESULT 可以在同一原子 proposal
建立，其 reference 由本批 exact proposal 和实际 absence 共同证明；不能把内存中
构造成功的记录视为已提交查询结果。所有检查在 Owner/Store/source guard 下持续至 commit。

### 20.2 后续物理重复仍有独立审计

RESULT field 10 精确引用 COMMAND；两者 ID、tenant、lineage、owner/artifact、mutation
与首次 outcome 全部一致。POSITION_COMMAND 引用 COMMAND，POSITION_SYSTEM 引用
SYSTEM；拒绝引用可较早 GC 的 RESULT 副本。完整首记录 digest、ID、Shard、tenant、
lineage 都要一致，首记录 source sequence/position 不晚于本次物理位置；相同位置
要求完整 mutation/digest 一致。跨位置时 source sequence 与位置顺序一致。

后续物理重复只追加新的 POSITION key 及其 Shard evidence 费用，保留首次逻辑记录
的 bytes/source/owner/费用。重放同一已应用物理位置只核对 existing POSITION 和原始
逻辑记录，不重复追加或收费。`requireFirst` 和 `requireStored` 分别校验引用与实际
key/NV type/full payload；完整 source/Command 去重、counter delta 与 SourceAdvance
原子写入仍由 C4 完成。本批的 50-position 测试验证格式与引用不改写首记录，不声称
已实现 Broker 重复处理或业务副作用的 exactly-once。

### 20.3 首次分配的可持久返回值

成功 APPLY_SHARD_CONTROL 的 SYSTEM 结果可以携带 field 11。它必须 APPLIED/OK、
origin OPEN、scope Shard/tenant/lineage 匹配，allocation mutation 与本结果首次 mutation
完全相等；后续 drain、其它 System 类型、拒绝结果或另一个 source 的 origin 都拒绝。
实际 quota grant kind、授权 scope、first nonzero allocation、完整原始 origin 与 signed
request 的关联仍由 CreationAuthority 对 §16 的实际 source proposal 校验。

因此，实际存储的首次分配结果可返回完整 source-derived origin，不依赖查询时当前
可变 descriptor 或重算 ID。Attachment 的 bytes 在本条 RESULT 费用内计算一次；
它不是第二条 descriptor，不新增 incarnation cardinality，也不重复 descriptor 的
独立存储 charge。必须等原子提交确定并经受保护 Result 读取后，才能将 origin 作为
后续 membership/channel 注册输入。当前 codec/返回值测试不替代这一 runtime 门槛。

### 20.4 计费、保留与删除

每条记录费用为该唯一类别的一个 count 和
`key.length + canonical outer payload.length + 12 + frozenRecordOverhead` bytes；
不再将嵌套结果、source 或 allocation 当成额外独立记录收费，不增加 payload、execution
或 Target/domain/incarnation 数。Tenant mirror 逐维映射同一事实，不加入第二次 primary sum。

记录不可变，没有定时自动释放或 reprice。删除检查要求同 lineage 的 Floor 覆盖该
记录的首次 source/stamp，并且 deletion 是严格晚于该 Floor 的 source-only mutation；
随后强制 DeletionAuthority 验证 retry/query/replay 窗口、closed fences、catalog/pins、
所有 surviving RESULT/POSITION/allocation 引用及实际删除。任何剩余引用、写入者或
未知写入状态都不能通过仅比较 Floor DTO 释放费用。验证函数不删除、不提前公布容量；
只有同一 protected batch 实际删记录并更新计费账本后才能减原 charge。

六组独立向量是不同合法结构场景（同位置 Command/System POSITION 为备选场景，
不是同一 ledger 的同时记录）。测试覆盖所有 Kind、allocation 返回值、50 次物理
位置推进、首记录不变、归属/引用/source 错误、失败/fatal 传播、编码边界和删除保护。
B4 继续完成其它 shared Shard/outbox/evidence record owners、完整 reserve/交接配方与
原验收；C4 真实 source authority、写集、查询/去重、独立恢复，以及 D/E/F 仍须实施。


## 21. 结果账本的独立恢复核对

`TargetResultLedgerAudit` 是完整 Target DEDUPE 06–09 namespace 的有限恢复 fold，
不在普通 source mutation、Claim 或 scheduler poll 中扫描。它从实际 canonical
结果记录和 META 1a descriptor 读取费用，不接受持久 counter 的 usage 作为重建输入。
输出仅为结果账本的各 primary/tenant contribution 与 primary 小计；其它 Message、
Claim、payload、descriptor、outbox 等账本仍需各自重建后参与最终 counter/total/
aggregate 全量相等核对。结果小计与 counter 部分相符不等于整库恢复通过。

调用输入包含已验证的 Shard/tenant scope、recovery lineage、Store source sequence
与完整 frontier，以及三个显式正上限：结果记录数、唯一 owner 数、结果与 descriptor
的 encoded bytes 总额。预算应在调用前由实际已激活恢复/资源契约验证；不能在耗尽后
丢弃剩余行或自行提高上限。encoded bytes 每条按 key + typed payload + 12 计入，
每个唯一 descriptor 只点读/计入一次；它不包含 SST、Java 对象或 RSS 放大证明。

扫描每条结果时验证完整 key/NV type/payload、Shard/tenant/lineage 与 source frontier；
任何重复 physical key 都拒绝。Owner 通过完整 identity 推导 META descriptor key，
最多点读一次，检查实际 key/type/canonical descriptor、冻结 accounting/tenant/
lineage，以及 allocation 和 latest drain 对 source frontier 的一致性。

记录及 owner 的 allocation/latest stamps 进入同一个 raw unsigned source sequence
历史表：相同 sequence 必须完整 stamp/digest 相同；按 sequence 排序后 SourcePosition
也须严格同向推进，不能在同一 offset 使用不同 sequence 或 metadata。结果事件另以
sequence 关联闭合 Command/System + logical ID：一个物理 source 不能同时归两个
逻辑事件；COMMAND/RESULT/首 POSITION 可以共同描述同一个事件。

扫描完成后，RESULT 与两个 POSITION 分支按实际 key 查找本次完整集合中的 COMMAND/
SYSTEM 首记录，再调用 §20 的 exact first-reference 校验；不因扫描顺序先后而拒绝
合法记录，也不因为少提供首记录而接受孤立引用。没有查询副本或已受控清理的 POSITION
本身不被强行补写；实际 namespace 完整性和保留合法性仍须完整 Store 权威证明。

费用从每条完整 record 的 frozen accounting 和唯一 record class 计算，checked
累加到 primary 与 tenant identity；primary 小计只加一次。Descriptor 是 attribution
依赖，其自身费用由 descriptor 账本计入，不在本 fold 重复收费。所有计数/字节限额、
算术、解码、引用、读取失败和 fatal Error 均在返回 Summary 前失败，不返回部分 PASS。

最后必须调用显式 `CompletenessAuthority`，证明四个 Target namespace 已在同一
实际 Store snapshot 中遍历完毕，META 依赖来自同一视图，且 Route/tenant/lineage/
source、Owner 与 Store incarnation 的 guard 有效。调用者给出的集合、空结果或一个
no-op 回调均不是这个权威的生产实现。发布恢复小计或合并全账本时仍须保持实际 guard；
当前没有通过本 helper 自动创建 Store scanner、恢复 aggregate 或放行 production。

独立向量新增四项：Target contribution、Shard contribution、primary subtotal 与
实际 encoded bytes；原六组 record bytes/key/charge 不变。测试覆盖反序遍历、缺失/
变更首记录、重复 key、同 source 事件冲突、descriptor/source/tenant/lineage 错误、
精确预算边界、读/完整性/fatal 失败和不可变输出。独立解码同时揭示并修正了 accounting
artifact 的对象同一性误用：相同 canonical artifact 现在按值比较，Claim 和结果的
冷读 owner 核对不再因解码创建不同对象而失败。

本批完善 B4 的结果账本重建契约，并修复已存在的 decoded-owner 比较缺陷。C4 仍须
实现实际完整 snapshot traversal/guard、跨所有账本的精确相等、恢复发布和故障验证；
B4 其它 owners/reserves/交接配方与原验收、A2/E6 资源证明及 D/E/F 均继续保留。


## 22. 实际 Store 提交后端与独立遍历

TargetStoreBackend 已将 quota/total 的精确 prior 校验接到真实 ShardStore 读取。
prepare 在同一 ReadView 和 BoundedReadBudget 内完成所有依赖读取、完整 business
before 比较及写集计量；不存在运行时全 counter 扫描。源 Shard/tenant 与 aggregate
accounting incarnation 精确绑定，非空 source 缺少 aggregate 必须失败。

普通 Edit 只能修改已注册 Target namespace/type，不能重写固定 Store metadata 或
由后端统一生成的 leaf/total/aggregate。完整 canonical 业务语义及费用来源仍须由
实际 mutation planner 与 CommitAuthority 证明，NV/type 校验本身不承担该证明。
每个 batch 精确累加 key + 完整 NV after（delete 只计 key）；这是 encoded-write
预算，RocksDB WAL/SST 放大、RSS 和总操作成本仍属 A2/E6。

commit 一次性消费绑定该 backend 的 Prepared，取得跨 native commit 有效的实际
CommitGuard，在同一 Store view 下写业务 + touched counter/mirror + affected
Target totals + aggregate。source 变更追加 META fixed 3/5；local Claim/revoke 不
改 source/sequence。失败不发布 in-memory 状态；native uncertain write 沿用 Store
已有重开 fence。调用者不得在 exception 后猜测成功或重试旧 Prepared。

实际结果审计遍历完整 DEDUPE [06,0a) 并要求 RANGE_END，同视图读取精确 owner。
结果数、结果/descriptor bytes 与共享读取 record/bytes/time 均有限。返回的
AuditedResults 是 backend 私有绑定对象；后续发布必须再次校验 ReadView/authority。
该实现完成 §21 的实际结果 traversal 子项，不能替代其它账本独立重建、全部精确
counter equality、Owner/Store 激活及故障恢复证明。首次 root/bootstrap 与完整 Claim/
outbox/shared owners、其它 mutation、C5 Worker 装配和 F 工具仍须继续实现。


## 23. Message 投影生成先于业务计费规划

TargetMessageStore 先从同一 Store view 的完整 Message before/after 自动生成
Message、live Expiry 和当前 ordinary/native/ORDERED work 的差集，再根据严格
OrderState/barrier 计算 serviceable head，最后更新受影响 TargetQueue heads。
AccountingPlanner 接收的是这份完整最终 business Edit 集合，包含各条完整 NV
before/after 和自动生成 queue/order/head 的实际 bytes。不能先按调用者原始 subset
计算费用，再由后端追加未计入的索引或 queue bytes。

固定 read/write/domain/message 上限共同约束规划；同 key 替换、删除再插入、跨
前缀工作变化均按 full before 精确合并。Claim/source 计费 stamp 的选择仍由实际
业务 planner 决定，source-only/local ordinal 规则不变。所有结果仍通过同一
TargetStoreBackend 原子提交；没有单独的 head 写或公平性写。

本批实现的是实际投影生成和提交连接，不是完整费用来源、grant/resource reserve、
root、Claim/attempt/outbox/共享账本和生产 authority 的完成。上述完整 planner 与
Worker 装配继续推进；开发 smoke 的空 quota updates/no-op guard 只验证投影事务
机制，禁止用它作为生产计费或授权实现。旧证据不认证新增路径。


## 24. 实际 before/after 计费装配与固定 inventory

TargetRecordAccounting 将本契约各 record helper 连接到实际 Store 双视图，逐条
验证 key/type/value 与完整冻结 attribution 依赖。它覆盖当前可写的 Message、
binding、timeline/Expiry、queue/order、共享 metadata、channel/native、descriptor、
payload owner、Claim charge、attempt budget、首次结果/physical audit。Grant
activation 的实际 type/key/source/first origin 会验证，其存储费用由 root fixed
inventory 承担；bookkeeping 本身由装配器生成，调用者不能自己提交未计费 root。

每条 primary contribution 的 cardinality 只进入 primary；tenant mirror 仅保留
相同资源量。Result 的 class、payload phase 的活跃/保留/已释放费用、Claim execution
和 attempt reserve/allocated/retained 费用继续遵守原契约，不能按最新 grant 重定价。
source assembler 使用实际 counter prior 减原贡献再加新贡献，不接受 caller 提供
任意 nextUsage 替代记录推导。同 key 替换必须保持冻结 owner/artifact，首次结果和
POSITION 不可变 bytes 不允许更新。真正创建/转态/删除权威仍属于业务 source 处理。

新增 leaf counter、Target total、grant activation 的记录数从实际存在/不存在读取
得出，变化同批进入 root inventory。首次 bootstrap 要求空 source、实际 root
descriptor 和无既有 touched business/counter；这只是有限局部检查，首次安装还须
完整受控 bootstrap/Store 权威。保留零 counter/total，当前 assembler 不提供这些
账本的物理退休。已有 root 或 total 缺失、费用不足、inventory 下溢/溢出均失败。

本地 Claim assembler 使用实际差集、恰好一条 Claim charge 与原四-counter上限；
新 charge 的 creation 必须等于本次 local stamp，revoke 必须删除原 charge 且
operation 是其严格后继。它不修改 bookkeeping inventory，不推进 source。实际
Claim 业务 record 与 Owner/Store 证明仍须接入 LocalClaimAuthority/CommitGuard，
不能以 charge projection 的存在替代 Claim 或 SEND 授权。

本批 source 真实 Store smoke 核对 bootstrap 的 4 counters/1 total、首次
COMMAND/RESULT/POSITION 后的 primary/mirror/aggregate 资源与 cardinality、无
inventory 变化时 root bytes 不变。测试 guard 是开发替身，不是 grant/签名/retire
证明。完整全类型/local/缺失/错配/故障/恢复、业务和 Worker 装配仍待实现或集中验证。


## 25. 持久业务 Claim 与费用原子对应

TargetClaimStore 通过 TargetMessageStore accounted 入口实际创建/删除业务 Claim，
TargetRecordAccounting 新增 INFLIGHT/NV36 STATE record charge。费用从同视图 META
ClaimCharge 的冻结 Target descriptor 取得；Claim ID/work/Owner/Store/sequence/
deadline/execution bytes/local stamp/业务 Claim SHA-256 全部相等。ClaimCharge 反向
点查业务 Claim 并检查同视图 Message.currentWork，费用不能作为孤立 ownership。

TargetLocalClaimAccounting 现在要求恰好一个业务 Claim 和一个 charge，CLAIM 只创建，
REVOKE 只删除；仍限制原四 counters、既有 allocation、不变 source/cardinality，以及
原允许的 STATE/execution 维度。实际头/Message/索引投影的 bytes 差额继续计费，root
inventory 不变。TargetClaimStore 拒绝改工作/Owner/Store 的本地撤销；source 消费
业务决策、物化后的 AttemptBudget 转移、生产 grant/permit 权威及全账本恢复仍未完成。

NV36/INFLIGHT04 的独立 vectors 和完整负例留待集中验证；本轮只做最小记录转态开发
检查，不把源码结构或一项检查作为真实 Store Claim、生产授权或恢复证明。


## 26. 实际 activation/total 的有界逻辑检查

TargetQuotaStoreGate 接收真实 assembler 的完整 Mutation，并从同一 Reader 读取
实际 root、Shard/Target activations 与 first allocation descriptors。再以实际
prior/next primary aggregate 和跨 incarnation Target total 运行原逻辑 grant 策略。
不读调用者 Map 代替 activation，不以单 leaf counter 绕过历史占用；镜像不重复计费。
普通业务不能同批改变其 grant，grant 更新必须经过单独的完整认证控制路径。

增长型拒绝区分 SHARD_LIMIT/TARGET_LIMIT 并保留准确 scope；new ingress 必须显式
传递实际已接受的计量 artifact，现有工作不因额度下降或变更当前 artifact 而重价。
Claim/revoke 自动使用 gate；有界 targets/business 数和所有额外点读共享原预算，
任何 grant/Store 变化均使 Prepared 失效。此 gate 不替代 source 业务角色校验、
first-seen/dedupe、费用所有权/原 reserve、签名/完整 grant 分配保护或物理权限。

一项实际 RocksDB 开发检查使用真实计费 bootstrap 和 activation/descriptor records，
证明 Target limit 拒绝不写结果、existing-work 策略分支可准备、后续 grant 变更使旧
计划拒绝且未写结果。activation/operation label/CommitGuard 是明确测试输入，该
检查不证明真实签名激活、Schedule/Outcome 语义、Claim 运行或生产 authority。
完整拒绝结果与 source 的原子提交，以及全部下调/迁移/多 Target/异常场景保留在集中
清单；新 gate 没有授予认证或完整实施状态。


## 27. 首次 grant/control 结果与 activation 同批计费

TargetQuotaGrantStore 使用实际 source/Store View 调用 grant control verifier，成功
时同时写新 allocation（仅首次非零 Target 分配）、activation、Shard-owned SYSTEM
及 POSITION。新 origin 完整附于首次成功结果，费用由原 RESULT/EVIDENCE 分类和
真实 record/accounting assembler 推导；固定 inventory 新槽位仍由 root 计费。
明确 verifier 拒绝只写 Shard-owned 首结果/physical audit 和对应账本/source，不能
留下 allocation 或 activation。更新 grant 不经普通 ingress gate 自我授权，完整
父级/tenant/transfer/physical reserve 权威由控制 provider 与 commit guard 证明。

内部语义拒绝使用私有标记和 typed Decision 分离；外部 authority 即使抛出相同
CommandResolutionException 类型也不持久化拒绝。损坏、无 root/原 descriptor、
重复逻辑/物理键、source 回退、预算/I/O 或 native write 失败不被吞掉。只在实际
commit 成功后返回结果，失败/UNKNOWN 不提前发布 grant 或容量。

最小开发检查执行真实 Ed25519 Prepared/Mutation 签名与内存精确注册、实际 RocksDB
root/bootstrap 和后续 grant batch，核对 allocation、SYSTEM/POSITION 引用和 source
stamp；验证外部容量失败无写、已有逻辑拒绝重执行、过期首次请求仅写拒绝结果。
容量和 CommitGuard 仍为测试替身，未证明生产保留/迁移/授权。物理重复审计、完整
root bootstrap/Source dispatcher/Worker、容量 provider 和 crash/recovery 留待实现
或集中验证；原 29 切片及 B4/C1/C4 状态保持未完成。


## 28. 物理重复的实际 POSITION-only 费用

TargetSystemReplayStore 使用真实 SYSTEM/POSITION 与 frozen descriptors 验证原结果，
后续物理位置仅写新 POSITION_SYSTEM，其 EVIDENCE bytes/record charge 进入原 Shard
owner/mirror/aggregate，source 同批推进。首 SYSTEM 的 RESULT 费用、allocation
attachment、Target total 和 activation 不重复写或收费；root inventory 保持原计数。
同一物理 source 的只读完成不推进 source/aggregate/revision，不产生 native batch。

源序、完整 metadata、sequence、envelope digest 和 first digest 都按实际 Store
检查。过期重复只派生该物理位置的拒绝响应，原 logical result 仍不可变；下一次同
位置重放验证相同 audit。缺 first/position/owner、未来 position、旧 source、错
lineage、分支竞争或 guard/view 失效均不能变成重复成功。Prepare 不返回发送权限。

最小 RocksDB 开发检查核对 native sequence 零增长、后续一次 exact POSITION fee、
首次 result/activation bytes 不变、过期重复/再放、陈旧只读 plan 和重复完成拒绝、
同位置重签 envelope 拒绝。guard/capacity 仍为测试替身，完整错误/故障/Broker 与生产
read/write authority 的证明留待集中验证，B4/C1/C4 继续 IN_PROGRESS。


## 29. 首条 SHARD grant 为完整 root 批次付费

空态初始化使用首条已认证 SHARD grant 的 source-derived root incarnation，与该
activation、SYSTEM/POSITION、root inventory、两个 root counters、aggregate、source
一批写入。初始 inventory 为 counters=2、totals=0、activations=1；只含一个 SHARD
incarnation 的 primary cardinality，tenant mirror 不重复计入 aggregate。SHARD
activation 及 SYSTEM 无 Target allocation attachment，原 NV/key/摘要格式不改变。

完整 batch 的实际费用必须被初始 SHARD grant 覆盖，包括 descriptor 的固定预留、
RESULT/EVIDENCE 费用与 root projection/activation 槽位。外部 StartAuthority 还须
批准同一 Store/lineage/source 起点和完整 logical/physical 资源；原 grant parent/
tenant/transfer 权威继续执行，构造 root 或看到空 Store 均不提供部署/容量权限。

只有六项精确空态 META open markers 可存在，其他业务/未知 META/source/recovery
历史一律阻止初始化。全 CF/默认 CF 的完整空态读取共享预算并绑定 commit ReadView。
无合法首 grant 或费用不足时不创建用于记录拒绝的 root、不推进 source；必须修复
启动前提或走相应受控流程，不能跳过历史。非空迁移/恢复与生产 provider 不由此实现
自动完成，B4/C4/C5 保持 IN_PROGRESS 和 PENDING_CENTRAL_VALIDATION。
