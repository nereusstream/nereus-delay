# NDIP-3 工作包：目标分区队列收敛与调度热路径改造

- 提案状态：`Draft`；完整实施已由用户于 2026-09-07 明确授权，契约冻结和接受凭证在 B7 闭合。
- 审查基线：`main@b521b614fbe22d30381593637e60ab615e5906cc`。
- 范围：A–F 全部 29 个切片，包含此前 P2 工作、最终真实 Broker/恢复验证、转换器和清退机制。
- proposal、环境迁移、性能测量与 production rollout 按 Accepted NDIP-2 分轨。
- `productionAuthority=false`。NDIP-1 已关闭；其历史凭证不认证本方案的新 runtime。

## Normative package

1. [完整详细设计](01-目标分区队列与调度热路径设计.md)：原始 1648 行全文，包含 §17 全部任务及 §17.8 覆盖表。
2. [实施计划](02-实施计划.md)：依赖、交付边界及验证要求。
3. [代码级设计](03-代码级设计.md)：实际调用链、接口与状态决定，随切片同步。

README、执行状态、用户决策记录、测量和 receipt 不进入 normative package。
当前完整设计保留导入时的 Draft/PLANNED 描述；它是启动基线，不代表新增权限障碍，
也不代表代码已经完成。后续 normative 变更与接受凭证必须绑定精确 bytes。

## 当前进展

A0/A1 已验证，A2 正在实施；其 READY discovery 共享预算和视图提交绑定已通过专项与完整检查。
head mutation/source 接入及资源上限证明仍未完成，当前 P0 进展不等于 Target 新架构或完整方案完成。

[29 项状态清单](progress.json) 是实施进度入口；[执行记录](04-执行记录.md) 保存实际命令、
源码身份、测试和环境结果。仅当全部必做实现及证据实际闭合才办理 Implemented。

原始输入 SHA-256：`3828585a94deea5bd37baa0f0258b7f83da9b2df6d8cc0a697fcfb07b7f331bf`。
导入保留全文与来源锁，Downloads 文件不承担后续仓库实施状态权威。

导入后的术语规范化：原示例 `Profile version 1/2` 使用显式 version 字样，以通过仓库
项目版本命名门；未改变 Profile 版本的业务含义。原始输入 digest 仅绑定导入来源。
