# Nereus Delay Improvement Proposal（NDIP）

NDIP 是 Nereus Delay 当前的重大改进提案治理入口。它延续单一持续设计主线：提案身份不表示
项目发布版本，不建立永久双轨，也不替代 Accepted 后必须同步更新的当前权威设计。

## 生命周期

```text
Draft -> Discussion -> Accepted -> Implemented
```

终止状态为 `Rejected`、`Withdrawn` 或 `Superseded`。只有 `Accepted` 的 NDIP 才能修改权威
设计；只有实现、环境 gate 和证据全部完成后才能标记 `Implemented`。

## 身份与工作包

- 身份使用单调递增的 `NDIP-N`；编号只表示提案身份。
- 每个提案位于 `docs/ndip/NDIP-N/`，至少包含 README、提案正文、实施计划和代码级设计。
- 工作包 README 必须列出 normative files；receipt、assessment 和运行证据默认不进入
  normative package。
- 仓库不保存会话执行提示词；需要时只在当前会话临时生成。

## 接受凭证

Accepted authority 由维护者决定、post-transition exact normative package digest 和 closed
`acceptance-receipt.json` 共同证明。verifier 只能验证 bytes 与 receipt 自洽，不能创建或推断
维护者权限。final receipt 必须明确后续独立 gate；不得把 proposal acceptance 等同于实现、
环境切换或生产就绪。

digest 和 receipt 的 canonical 规则由已接受的
[`NDP-0002`](../proposals/0002-register-ndip-governance.md) bootstrap 记录定义。NDP 历史继续在
[`docs/proposals/`](../proposals/README.md) 永久保留，但不再分配新的 NDP 编号。

## 当前工作包

- [`NDIP-1`](NDIP-1/README.md)：Accepted；Gate B PASS，H1 READY，H2-H6 按 slice 依赖等待。
  exact `DISPOSABLE_LOCAL` attestation 下允许本地集成、恢复与故障测试。当前没有真实 persistent
  deployment，因此 G0 为 `NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT`，没有
  Assessment receipt；Gate C 不阻塞 implementation，但继续严格阻塞 SHADOW 与 ENABLED。
