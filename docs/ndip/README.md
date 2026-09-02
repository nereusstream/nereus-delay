# Nereus Delay Improvement Proposal（NDIP）

NDIP 是 Nereus Delay 当前的重大改进提案治理入口。它延续单一持续设计主线：提案身份不表示
项目发布版本，不建立永久双轨，也不替代 Accepted 后必须同步更新的当前权威设计。

## 生命周期

```text
Draft -> Discussion -> Accepted -> Implemented
```

终止状态为 `Rejected`、`Withdrawn` 或 `Superseded`。只有 `Accepted` 的 NDIP 才能修改权威
设计。`Implemented` 表示 Accepted implementation scope 和 proposal lifecycle 已由 closed receipt
关闭；它不自动表示环境已部署、性能/规模已认证或 production rollout 已授权。

三条独立轨道为：

```text
proposal:    Draft -> Discussion -> Accepted -> Implemented
environment: G0 -> Gate C -> SHADOW -> bounded ENABLED -> safe final state
release:     deployment -> performance/scale -> production rollout
```

精确规则由 Accepted [`NDIP-2`](NDIP-2/README.md) 定义。

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

## 实现关闭凭证

Implemented authority 由维护者决定、post-transition normative package digest 和 closed
`implementation-receipt.json` 共同证明。receipt 必须保留历史 Accepted authority，绑定
certified runtime-source digest 和 exact evidence digests，并显式记录
`productionAuthority=false`。

runtime-source authority 按 verifier 注册的 closed path scope 计算，不再使用仓库 HEAD 相等
代替源码相等。README、执行记录、receipt 和其他非 runtime 文件不进入该 digest；
registered runnable source、构建依赖或 runtime path set 变化仍 fail-closed；`src/test/**` 和
纯格式/静态检查配置不冒充 runtime identity。

digest 和 receipt 的 canonical 规则由已接受的
[`NDP-0002`](../proposals/0002-register-ndip-governance.md) bootstrap 记录定义。NDP 历史继续在
[`docs/proposals/`](../proposals/README.md) 永久保留，但不再分配新的 NDP 编号。

## 当前工作包

- [`NDIP-1`](NDIP-1/README.md)：Implemented；implementation/lifecycle CLOSED。H0-H6、exact
  disposable 24/24 + 3/3 和完整 fixed-staging certification 已闭环；历史 Gate B PASS authority
  保留在 Accepted receipt。该 staging 证据仍为
  `productionAuthority=false`；其他 target deployment、性能/规模和 production rollout 后续独立进行。
- [`NDIP-2`](NDIP-2/README.md)：Accepted；定义 lifecycle closure、environment/release 分轨和
  scoped certified runtime-source authority，不授予 deployment authority。
