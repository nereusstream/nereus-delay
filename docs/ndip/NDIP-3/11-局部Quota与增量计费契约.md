# NDIP-3 B4：局部 Quota 与增量计费契约

状态：**IN_PROGRESS / counter、计量、attempt reserve、跨 incarnation 总额与 grant artifact 已实现，B4 尚未冻结验收**。
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
grant artifact 完整携带这些计数与计量规则；其认证/source 激活仍待闭合，禁止以既有
grant 自动授权这些新计数。Counter 约束：

- 每个 Target 主/镜像 identity 的 Target 数最多 1，execution domain 数最多 64。
- SHARD/TENANT_SHARD 不分配 Target 或 domain slot，也不能承担资源维度 1/2/5–8。
- 非零主 counter 必须保留恰好一个 accounting incarnation；tenant 镜像的该项始终为零。
- 全零 counter 是退休标记，不可在相同 identity 下恢复为非零。退休所需的真实保护证明
  由 source committer 校验，构造全零对象本身不构成证明。
- 各数值范围为 0..Long.MAX_VALUE，checked add/subtract；下溢、上溢不饱和、不回绕。

## 3. Mutation stamp、counter 与 aggregate

`TargetQuotaMutation` exact fields：1 非零 raw uint64 mutation sequence；2 完整有界
SourcePosition；3 已接受 Command/System Mutation 精确 canonical bytes 的非零 digest[32]。
SourcePosition 延续 B1 上限、物理资源身份和同 offset 元数据一致性检查。序号与 source
顺序同时严格推进；恢复 replay 不能重新采样时间或制造新的 accepted mutation bytes。

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
刷新 revision、不写 bytes。Local revision 不大于最后 mutation sequence，且不大于
aggregate revision；不要求不同 counter 的 revision 相等。Raw uint64 全一位值可读，
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

所有非 genesis 值 revision 非零，且 `revision <= mutation.sequence`；counter 最后
source/sequence 不得领先 aggregate，source 相同当且仅当 sequence 相同，后者还必须
具有完全相同 stamp bytes。Aggregate 不得领先 Store 的 source/sequence。发现身份、
source metadata、revision 或摘要不一致时 fail closed，不能通过正常 Schedule 重置。

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
  record 的唯一 owner、counter/budget 自身 bookkeeping 与共享元数据的有界费用来源。
- §9 已列原 §17.3 的完整业务 delta 表；继续绑定每行的完整 before/after ledger、
  非 attempt 的 payload/identity 保护与结果/控制记录来源，避免只由方法参数声明费用。
- §10 已定义新 cardinality/grant artifact 和跨 incarnation 总额；完整 grant source
  激活、tenant Target/domain 计数来源、incarnation 分配/退休证明
  以及独立账本所有权交接的精确规则，保证不会因多个 Profile/domain 复制 grant。
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
输入版本 1 延续既有公共 Schedule 编码，不意味着旧 Lane grant 自动适用新 Target
费用。Schema bundle / artifact / grant 的绑定仍须通过下文未完成的完整授权检查。

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
压缩率或对象实际计费大小。调用端必须从经过 canonical 校验的冻结 bytes 取得长度，
不能将方法参数当成可由客户端声明的费用。

一个持久 NV record 只属于一个 record-byte 类：STATE→维度 3；RESULT→9/10；
SYSTEM_MUTATION outbox→11/12；EVIDENCE→14/15；独立 WAL frame→13。完整 source/Broker
writer 的共享 quota 仍受 51–55 与其权威 grant 约束，不因本地字节公式获得远端额度。
应用 payload ownership（2/6/4）、执行 envelope（8）和已编码的存储副本是不同资源事实，
不能把同一个 record 再同时塞进 STATE、RESULT、SYSTEM 或 EVIDENCE 两个类；也不能把
outcome 的已分配 record 与覆盖它的 reserve 各加一次。

本批提供 `activePayload/reservedPayload/retainedPayload/executionCharge/recordCharge/
outcomeWalCharge` 的 checked 向量。它们不自动选择某条业务 mutation 的 record 集合。
Counter/aggregate/charge bookkeeping 的自身存储归属和完整 reserve sizing 必须在
B4 最终计量/grant 绑定中闭合，不能通过递归计算自身编码长度或默认为免费跳过。

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
| Claim | 新 durable reversible Claim `+X`，Claim record 加入分类 `S` | timeline/READY 本身不增加 7/8；失败且没有持久 Claim 不收费 |
| revoke/Claim 失效 | exact durable Claim `-X`，Claim record 删除/retained 转移 | 仅扣该 Claim 首次冻结 charge；不扣任何已 Admission attempt |
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
last-touch source、raw sequence 及同位置 metadata/digest 必须相容。Total 还必须逐维覆盖
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

该 artifact **没有单独预留 NV/meta key，也没有接入旧 PUBLISH_QUOTA_GRANT branch**；
旧 17 维 `QuotaGrantRef` 不变。B4 仍须冻结完整 authority registration、source control
body、exact prior grant/source activation 和 transfer ref 关联；C4 实现真实后端、可信
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
