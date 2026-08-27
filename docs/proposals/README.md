# Nereus Delay Proposal（NDP，历史 bootstrap 治理）

Nereus Delay 只有一条持续演进的产品与设计主线。项目不用整体编号版本切分架构，也不因为一次重大修改而复制一套旧设计、旧实现或永久兼容层。重大变更通过提案提出、评审、接受并合入当前权威设计。

`NDP-0002` 已完成一次性治理桥接。`NDP-0001` 和 `NDP-0002` 作为 bootstrap 历史记录永久
保留；后续改进提案使用 [`docs/ndip/`](../ndip/README.md) 中定义的 `NDIP-N` 身份与工作包，
不再分配新的 NDP 编号。

## 何时必须提交 NDP

满足任一条件的变更必须先有 NDP：

- 改变公开语义、状态机、线性化点、故障模型或安全边界；
- 改变 wire、持久化布局、canonical bytes、稳定错误码或资源身份；
- 改变跨仓库 Kafka/Pulsar/Oxia/Object Store 契约；
- 改变发布 gate、运行边界、容量模型或恢复/迁移方式；
- 推翻或实质修订 Accepted ADR 或已接受 NDP。

局部实现、测试补强、等价重构、拼写和证据更新不需要 NDP，但不得借此改变语义。

## 状态

`Draft -> Discussion -> Accepted -> Implemented`

终止状态是 `Rejected` 或 `Withdrawn`。已接受提案可被后续提案 `Superseded`，但历史文件保留。只有 `Accepted` 的 NDP 才能修改权威设计；只有实现与所需 gate 完成后才可标记 `Implemented`。

## 历史编号与文件名

- 已登记编号为 `NDP-0001`、`NDP-0002`；后续编号由 NDIP 治理接替。
- 文件名为 `NNNN-short-kebab-title.md`。
- 编号只表达提案身份，不表达项目版本或发布代际。

## 必填内容

每份 NDP 必须写清：动机、范围与非目标、当前约束、提议设计、被替代的约束、数据/协议影响、安全与故障影响、实施切片、验证 gate、回滚方式和未决问题。

兼容不是默认目标。提案必须从以下三种策略中明确选择一种：

1. **直接替换**：尚未发布或没有需要保留的数据/用户，删除旧路径；
2. **一次性迁移**：保留有界迁移工具和退出条件，不形成永久双轨；
3. **并行格式**：只有存在已证明的外部兼容义务时才允许，并写明生命周期和删除条件。

如果没有明确且可验证的兼容义务，默认选择直接替换。

## 权威关系

NDP 记录“为什么以及如何改变”；被接受后，当前结论必须同步写入主设计、Protocol Registry、ADR、Implementation Status、Design Audit、运行手册和相关 gate。实现和权威文档始终面向同一个当前系统，不保留一整套按项目大版本分叉的文档树。

历史模板见 [`TEMPLATE.md`](TEMPLATE.md)，后续提案入口见 [`docs/ndip/`](../ndip/README.md)。

## 当前提案

- [`NDP-0001`](0001-adopt-continuous-design-evolution.md)：Accepted，采用单一持续演进设计主线。
- [`NDP-0002`](0002-register-ndip-governance.md)：Accepted，注册 NDIP 工作包、exact package digest
  与外置接受凭证；2026-08-27 amendment 明确 Gate B 授权 implementation，Gate C 只保护
  persistent deployment、SHADOW 与 ENABLED。
