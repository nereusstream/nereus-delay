# NDIP-1 工作包：Pulsar Native Delivery Handoff

## 目录身份

- 工作包目录：`docs/ndip/NDIP-1/`
- 当前改进提案：`NDIP-1`
- 提案状态：`Draft`
- 审查基线：`main@8915d21ed325a90ec305201ca85ab8daea3803dc`
- H0 实现提交：`main@7cb377ca9dd3135792237af0f027076630d5e4f3`
- 整理日期：`2026-08-27`

仓库现有的全局 NDP 治理及 Accepted `NDP-0001` 保持不变。本工作包及其内部改进提案
统一使用 `NDIP`（Nereus Delay Improvement Proposal）编号：当前为 `NDIP-1`，后续工作包
依次使用 `NDIP-2`、`NDIP-3`。在 Draft 阶段，本目录中的 NDIP 是评审稿，不修改当前
权威设计。

当前主干的治理索引和自动 gate 仍只识别 `docs/proposals/` 下的 NDP。因而 `NDIP-1` 不能
通过自行编辑本文件夹中的 `Status` 获得 Accepted 身份。H1-H6 开始前，现行治理目录中必须
存在一份 Accepted 记录，绑定 `NDIP-1` normative package digest、审查基线和接受结论；或者
先有 Accepted 治理变更正式注册 NDIP 命名与目录。两者都没有时，本文所有 `NDIP Accepted`
一律判定为 false。H0 不改变权威语义，不受这一命名桥接阻塞。

`normative package digest` 不包含本 README 和执行提示词，避免自引用与操作文本改变设计
身份。它固定包含 `01`、`02`、`03`、`04` 四个文件；文件必须是 UTF-8、LF、末尾单换行，按
relative path 的 unsigned UTF-8 byte order 排序。摘要输入固定为：

```text
SHA-256(
  "nereus-delay-ndip-package\0"
  || for each file: u32be(pathLength) || pathUtf8 || SHA-256(exactFileBytes)
)
```

Accepted 后任一 normative file 改动都必须产生新的治理审查/摘要绑定，不能沿用旧 receipt。

## 文档地图

1. [`01-调查与决策记录.md`](01-调查与决策记录.md)
   - 记录源码现状、问题根因、已经确认的产品决策和被淘汰的旧结论。
2. [`02-NDIP-1-Pulsar-Native-Delivery.md`](02-NDIP-1-Pulsar-Native-Delivery.md)
   - 沿用现行 NDP 必填结构、以 NDIP 命名的 Draft 改进提案，是本工作包的设计评审入口。
3. [`03-实施计划.md`](03-实施计划.md)
   - 按治理门、依赖关系、代码切片和验证 gate 组织的实施计划。
4. [`04-代码级目标设计.md`](04-代码级目标设计.md)
   - 固定 exact enum/wire、schema、hash、dual-head scheduler、Journal、P1 transport、evidence
     和 activation 契约，是实现会话的代码级输入。
5. [`05-目标模式执行提示词.md`](05-目标模式执行提示词.md)
   - 提供当前可直接使用的 H0 目标模式提示词，以及 Accepted 后才能使用的完整实施提示词。

旧的根目录稿件 `docs/修复 Pulsar Handoff 延迟投递.md` 已由本工作包替代，不再保留
两份并行计划。

## 当前可执行边界

```text
NDIP = Draft / Discussion
    -> 只允许 H0 fail-closed、测试和提案材料

current-governance Accepted receipt binds exact NDIP package digest
    -> 才允许修改当前权威产品契约

Accepted receipt + DataResetAssessment PASS
    -> 才允许 H1-H6 代码、wire 与持久化实现

直接替换切换
    -> 必须通过环境级数据重置/迁移 gate
```

H0 的目标只有一个：在完整契约尚未被接受、物理链路尚未闭环时，所有尚未正确编码业务
record 的 Pulsar native 物理入口都必须在 Producer ownership 前确定拒绝。范围同时包括：

- Managed `actionAt < deliverAt`；
- AUTO_FAST 有效 native request。

Managed 使用 Worker、adapter 和 real transport 三层门；AUTO_FAST 使用 adapter 和 real
transport 两层门。两条路径都必须提供 no-producer-touch 证明。

## H0 实施状态

H0 已在 `main@7cb377ca9dd3135792237af0f027076630d5e4f3` 实现并推送。当前结果是
fail-closed：Managed early request 和有效 AUTO_FAST native request 都在 Producer ownership
前返回 stable `CAPABILITY_UNAVAILABLE`；Worker 路径同时不取得 physical admission、不调用
adapter/delegate，并排队既有 source-log Outcome handoff。两个 real transport 的 direct bypass
也在 `producer.newMessage()` 前返回。

已通过的验证包括：

- `WorkerPhysicalPublishExecutorTest`、`DestinationAdapterTest`、`NativeSubmissionAdapterTest`；
- `./gradlew test`；
- `./gradlew check`（含 Spotless、Checkstyle、文档检查和 `checkProjectVersionMarkers`）；
- source-locked P1 `pulsar-worktrees/nereus-delay-p1@0a2536484cd3932801a98dc88ff112b2df88a1c7` 的
  `compileRealPulsar` 与 `runRealPulsarH0Smoke`。no-Broker smoke 报告两个 real transport 的
  `newMessage=0`、`sendAsync=0`。

这不是完整 Handoff 或 physical record chain 的实现：`.deliverAt(...)`、无损 record 投影、
Attempt Journal、P1 evidence/recovery、capability activation 以及 H1-H6 均未实施；NDIP-1
仍保持 `Draft`。

## 当前结论

Handoff 方向成立，但它不是 ordinary Managed 语义的透明优化。最终设计必须同时保留：

- `NEREUS_MANAGED_NOT_BEFORE`：Nereus 持有到 `deliverAt`，目标 Pulsar Message 不设置
  `deliverAt`；
- `PULSAR_NATIVE_DELIVERY`：显式选择 Pulsar 原生投递，允许提前持久化并设置 Pulsar
  `deliverAt`，消费行为、精度和运维风险继承 Pulsar 原生语义。

默认策略必须是 `NativeDeliveryPolicy.FORBID`，并在 wire 中显式编码。第一阶段原生投递
只允许 `BEST_EFFORT`，不宣称跨 Subscription 的严格 not-before 或消费顺序。

## 代码级就绪结论

此前的 R1-R6 已在本 Draft 中收敛为唯一答案，详见
[`04-代码级目标设计.md`](04-代码级目标设计.md)：

- Schedule policy 与 actual contract 分权，不增加 `DispatchMode`；
- Admission 冻结 snapshot，Producer ownership marker 前再次验证 lease；
- AUTO_FAST 删除 clock shift，目标 timestamp 精确等于业务 `deliverAt`；
- `DataResetAssessment` 阻塞完整实现，signed manifest 阻塞 activation；
- 一个 `ArtifactGenerationSet` 复用现有 Worker capability 与 source-ordered activation；
- Admission 只绑定 RecordTemplate，Journal 后才产生 sequence/final Record；record hash 与
  command hash 使用不同 domain，通过 source-locked encoder 和 golden vectors关联。

本轮还关闭了原计划未识别的 scheduler 问题：使用同一 Lane/DRR 下的 ordinary/native 双
head 投影，防止 policy disabled 或 lead 缩小时 native head 阻塞 ordinary due。

因此，设计内容已经达到代码级目标设计；它固定产品契约、持久化/协议目标、状态机、类级
落点和验收门，但不是承诺当前主干签名永不变化的逐行补丁说明。执行就绪度必须分开判断：

| 范围 | 设计详细度 | 现在能否执行 | 前置条件 |
|---|---|---:|---|
| H0 | patch-ready，五个生产入口与测试落点已固定 | 是 | 开始时重核 remote main 与调用图 |
| H1-H6 | code-level target，产品决策已关闭 | 否 | Accepted governance receipt 绑定 exact package digest + `DataResetAssessment` PASS |
| activation/cutover | environment-specific runbook input | 否 | signed `DataResetManifest`、source lock、Worker barrier 全部 PASS |

H1-H6 开始后仍需在每个切片前重核最新源码签名、P1 source lock 和生成号占用；这属于正常的
source-drift 审计，不得借此重新打开本文已经关闭的产品契约。当前应复制
[`05-目标模式执行提示词.md`](05-目标模式执行提示词.md) 的 H0 提示词开始工作。
