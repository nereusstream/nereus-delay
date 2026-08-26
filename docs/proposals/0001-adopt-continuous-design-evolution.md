# NDP-0001：采用单一持续演进的设计主线

- Status: Accepted
- Authors: Nereus Delay maintainers
- Created: 2026-08-25
- Discussion: repository governance decision
- Supersedes: project-wide numbered release lines and default compatibility assumptions
- Superseded by:

## 摘要

Nereus Delay 不把整个项目固定为一个编号版本，也不为重大修改创建新的整体版本线。仓库维护一份当前权威设计和一套当前实现；重大变更通过 NDP 评审并直接演进这套设计。

## 动机

项目由团队自主创建，尚不存在必须长期支持的已发布大版本用户或数据契约。以整体版本切线会复制文档、gate 和实现边界，使尚未完成的设计被误当成需要永久兼容的历史产品。目标是持续迭代，直到设计和实现完整闭合。

## 范围

- 项目、设计、实现、测试、脚本、环境变量、证据 schema 和内部 API 的整体版本命名；
- 重大设计变更的提案、接受、落地和文档同步流程；
- 新旧内部格式或实现路径的兼容决策。

## 非目标

- 改写外部系统已经固定的 API、协议或能力名称；这些名称不构成本项目版本线；
- 改变 NDL1、NDR1 等已注册 magic 的字节值。本提案只禁止用它们推导项目大版本；若要改变 magic，必须另行提交 NDP 并更新 golden vectors；
- 把本次治理改名本身宣称为生产发布证据。

## 提议设计

1. 仓库只有“当前设计基线”，基线修订号用于校验文档同步，不是产品版本。
2. 重大改变必须提交 NDP；Accepted NDP 直接修订当前权威设计和实现。
3. ADR 保留具体架构决策理由；NDP 管理跨多个 ADR/协议/模块的改进。
4. 路线图按能力、风险和可验证切片组织，不按项目大版本组织。
5. 兼容策略由每个 NDP 基于真实外部义务决定；无义务时直接替换并删除旧路径。
6. 文档、类名、包名、脚本、环境变量、证据 schema 和分支不再使用项目整体编号版本标签。

## 被替代的约束

- “先冻结整个项目基线，再以另一条整体版本线承载重大设计变化”；
- “所有改变默认兼容旧项目大版本”；
- “按大版本复制设计、Registry、Audit、Runbook 与 release gate”；
- “因为旧格式曾存在，就永久保留 reader/writer 双轨”。

## 数据、协议与兼容策略

选择：**直接替换**。

当前没有已证明的外部发布兼容义务。本次迁移删除项目内部大版本命名和仅为该命名存在的兼容预设。外部固定 API 名称不属于本项目兼容层，保持不变。

## 安全、故障与运维影响

安全、fencing、恢复和 fail-closed 语义不因治理改名而放宽。任何需要改变这些语义的后续工作必须单独提交 NDP，并在实现前写明失败与回滚边界。

## 实施切片

1. 建立 NDP 目录、模板与校验；
2. 将权威文件迁移为无项目大版本的当前基线；
3. 清除内部代码、包、脚本、环境变量和证据 schema 的整体版本标签；
4. 删除只为旧项目版本线保留的兼容分支；
5. 运行残留扫描、文档 gate、编译和测试。

## 验证与发布 gate

- `checkDocumentation` 验证 NDP-0001、提案模板、当前基线修订和权威文件；
- 仓库残留扫描拒绝 Nereus Delay 自有的整体编号版本命名；
- Gradle `check`、相关真实 source-set 编译与 shell 语法检查通过；
- 外部固定 API 版本名称列入显式 allowlist。

## 回滚

本提案改变治理与内部命名，不创建另一条产品线。若迁移期间发现真实外部兼容义务，暂停相关切片并以新 NDP 选择一次性迁移或有界并行格式；不得恢复整体项目版本分叉。

## 未决问题

无。

## 权威文档同步清单

- [x] 主设计
- [x] Protocol Registry
- [x] ADR / ADR index
- [x] Implementation Status
- [x] Design Audit
- [x] Operations Runbook
- [x] 自动化 gate 与测试
