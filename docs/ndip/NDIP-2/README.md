# NDIP-2 工作包：生命周期与认证源码权威分层

- 工作包目录：`docs/ndip/NDIP-2/`
- 提案状态：`Accepted`
- 接受日期：`2026-09-02`
- 决策范围：NDIP lifecycle closure 与 scoped runtime-source authority

## Normative package

1. [`01-NDIP-生命周期与认证源码权威分层.md`](01-NDIP-生命周期与认证源码权威分层.md)
2. [`02-实施计划.md`](02-实施计划.md)
3. [`03-代码级目标设计.md`](03-代码级目标设计.md)

`README.md` 和 receipt 不进入 normative package。`acceptance-receipt.json` 绑定上述三个
normative files 的 exact bytes，并记录维护者在 2026-09-02 做出的显式接受决定。

## 当前结论

NDIP 的 `Implemented` 只关闭已接受的实现范围和提案生命周期。它不自动授予任何
target environment 的 deployment、性能/规模认证或 production rollout authority。

NDIP-1 的完整 fixed-staging certification 仍严格保持
`productionAuthority=false`。它可作为 NDIP-1 implementation closure evidence，但其他环境的
G0 / Manifest / Gate C / SHADOW / canary 必须在后续部署工作中独立执行。
