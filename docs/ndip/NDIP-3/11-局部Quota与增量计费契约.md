# NDIP-3 B4：局部 Quota 与增量计费契约

状态：**IN_PROGRESS / 字段与增量运算基础已实现，B4 尚未冻结验收**。
本页与原设计 §11.1、§16.6、§17.3 的 B4 合读。它定义已落到代码的 counter 字段与
局部计算边界；逐项业务 delta、完整计量 artifact、grant 关联和恢复来源闭合后才办理
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

Field 3–6 是独立 Target 格式的计数，不能投影回旧 Lane grant 的 16/17 维。完整新
grant/计量 artifact 的绑定仍在 B4 待闭合清单中；未绑定前禁止以既有 grant 自动授权
这些新计数。Counter 约束：

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

- 逐维唯一费用来源及完整 byte/record 计量 artifact，特别是结果 reserve 转实占、
  retained/object ownership、共享元数据和旧 attempt 的费用转移。
- 原 §17.3 要求的完整业务 delta 表：Schedule、Cancel、Reschedule、reservation
  commit/expire、Claim/revoke、Admission、definitive failure、UNKNOWN、Outcome、terminal、
  retained release、旧 attempt 排空；绑定确定的 before/after ledger 与释放时点。
- 新 cardinality/grant 关联、tenant Target/domain 计数来源、incarnation 分配/退休证明
  以及独立账本所有权交接的精确规则，保证不会因多个 Profile/domain 复制 grant。
- 对上述规则的重复 replay、溢出、Outcome/UNKNOWN/旧 obligation 守恒向量，并将
  B4 原始验收逐项绑定到规范、代码和证据。

C4 的真实原子提交、内存发布、账本恢复、固定 K 随 L 增长的 Store 读写计量继续保留；
它们不是本批纯 planner 测试的已完成结论。A2/A3、B5–B7、C–F 的责任没有缩减。
