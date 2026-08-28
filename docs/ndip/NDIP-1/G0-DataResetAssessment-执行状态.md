# G0 DataResetAssessment 执行状态

## 当前结论

- Gate B：`PASS`
- implementation：`H1-H6 code slices implemented; disposable-local certification PASS; deployment pending`
- local disposable integration/recovery/fault testing：`ALLOWED_WITH_EXACT_ATTESTATION`
- G0 tooling core：`implemented`
- G0 lifecycle：`NOT_APPLICABLE_FOR_IMPLEMENTATION`
- persistent environment assessment：`NOT_RUN`
- authoritative assessment receipt：不存在
- Gate C：`PENDING_DEPLOYMENT`，当前尚未要求提供真实 deployment scope
- SHADOW：`BLOCKED_BY_GATE_C`
- ENABLED：`BLOCKED_BY_GATE_C_AND_SHADOW_REQUIREMENTS`

当前仓库尚未部署真实 nereus-delay 集群，也没有需要保留的旧 RocksDB、Oxia、Object Store、
Worker 或 unresolved obligation 环境。因此不能构造权威 Assessment scope，也不得为了过门生成
虚假 environment、placeholder PASS 或模拟 production receipt。

2026-08-28 的 `DISPOSABLE_LOCAL` certification runner 已在独占、可销毁环境中完成真实 native、
recovery、fault、Oxia/MinIO 和 cleanup 矩阵；local receipt 为 `PASS`，24 个 cell 全部
`EXECUTED_PASS`，且 `EXECUTED_FAIL=0`、`NOT_COVERED=0`、`skipped=0`。该结果只证明
本地认证边界，不创建 Assessment scope，不改变下面的 persistent deployment 状态。

`NOT_APPLICABLE_FOR_IMPLEMENTATION` 只表示 G0 不是 H1-H6 的前置条件；它不是 Gate C PASS。
第一个 existing/staging/production deployment 出现后，状态转为 `PENDING` 并使用真实 closed
scope 执行 G0。在此之前 H1-H6 的代码/本地 disposable 验证可以推进，但任何 persistent
deployment、SHADOW 或 ENABLED 仍被阻断。

## H1-H6 implementation boundary

H1-H6 的代码切片已经落入当前主干：current generation contract/store、signed
handoff policy and dynamic scheduler/admission、Attempt Journal、source-locked P1
record/evidence path、exact-timestamp AUTO_FAST，以及 signed manifest/activation
barriers。`DataResetManifest` 仅接受 fresh resource、zero-obligation proof 和所有 Worker
对同一 `ArtifactGenerationSet` 的 current declaration；`DataResetActivationGate` 在
startup、assignment、source apply 和 physical send 边界拒绝 stale/mixed generation。

这不改变当前 G0 结论。没有真实 persistent deployment，因此没有 Assessment receipt、Gate C
receipt、production manifest、SHADOW observation 或 ENABLED lease；这些缺失是
`PENDING_DEPLOYMENT`，不是可以用 synthetic placeholder 填补的证据。

H1-H6 代码切片历史锚点为 `main@c7c99d377dc9e8bb786032173d62d1981011a4e2`；当前
disposable certification 绑定的 Delay source 为
`main@da15290e47b9255403c92e4ebba3c7d5189edb75`。P1 source lock
为 `nereus/delay-resource-guard@0a2536484cd3932801a98dc88ff112b2df88a1c7`。下面引用的
`68fe2c29` G0 implementation gate 文字是该提交时的历史状态，不是当前 H1-H6 完成状态。

## 已实现的只读核心

代码位于 `com.nereusstream.delay.assessment`，实现提交为
`main@68fe2c292e34f0162aac9377b9f935fe598831e4`：

- `EnvironmentClassification`：closed
  `DISPOSABLE_LOCAL / EXISTING / STAGING / PRODUCTION / UNKNOWN`；只有后三个 persistent class
  可创建真实 Assessment scope，`UNKNOWN` fail-closed；
- `DataResetAssessmentScope`：固定 classification、environment、deployment、tenant、route、
  shard、resource 和 eligible Worker exact set，并计算独立 scope digest；
- `DataResetInventoryCollector`：要求每个 closed resource kind 都有显式 read-only adapter，逐个
  resource 读取且拒绝 identity substitution；
- `DataResetInventory`：绑定 scope 枚举完整性证据、可信观察时间、资源、obligation 和 Worker
  枚举证据；
- `DataResetAssessmentEvaluator`：closed outcome 为
  `PASS_DIRECT_REPLACE / PASS_RETAIN / MIGRATION_REQUIRED / INCOMPLETE`；
- `DataResetAssessmentReceipt`：schema generation 2、canonical JSON、NDIP package digest、source
  baseline、scope/inventory/findings 和 domain-separated assessment digest；
- `DataResetAssessmentReceiptWriter`：唯一写操作是调用者显式指定的本地文件，使用
  `CREATE_NEW + NOFOLLOW_LINKS`，拒绝覆盖和 symlink parent；
- `DataResetAssessmentRunner`：只读一次 inventory；不隐式写 receipt。

closed resource kind 覆盖 Command/system topic、RocksDB Store、Checkpoint Catalog、Profile/Oxia、
runtime policy、payload reservation/Object、Pulsar Attempt Journal、evidence topic/cursor、
query/dedupe、`PUBLISHING / UNCERTAIN` obligation index、resource incarnation 和 Worker registry。

Outcome 到部署决策的含义是：

```text
PASS_DIRECT_REPLACE -> RESET candidate
PASS_RETAIN         -> RETAIN candidate
MIGRATION_REQUIRED  -> MIGRATE required; migration completion must be proven separately
INCOMPLETE          -> no Gate C authority
```

Assessment receipt 只是 Gate C 输入，不直接授权 mutation、SHADOW 或 ENABLED。当前 obligation
非零也不会自动解释为迁移要求：G0 判断它是否必须跨 generation 保留；actual zero obligation、
fresh resource 和 all-Worker exact generation 仍由 H6 后的环境 Manifest 在 cutover window 证明。

## Lifecycle safety guard

`DeploymentSafetyGate` 把 implementation、local disposable operation 和 deployment 分成三个
closed API：

1. `implementation(...)` 只读取 Gate B 和调用者提供的 H1-H6 completed prefix；历史的
   `68fe2c29` G0 快照中 completed set 为空，因此当时 H1 AUTHORIZED、H2-H6 返回
   `PREDECESSOR_REQUIRED`。当前代码已完成这些实现切片，但该通用 API 仍不会凭空生成
   deployment 或 certification authority；它不读取 Gate C。
2. `localDisposable(...)` 只在 classification 为 `DISPOSABLE_LOCAL`，且 attestation 与 exact
   environmentId 相等、isolated scope、synthetic-only、exclusive ownership、cleanup authorization
   和 evidence digest 全部存在时，允许 create/reset/destroy/rebuild/integration test。
3. `deployment(...)` 对 `UNKNOWN` 无条件拒绝；对 existing/staging/production 必须验证绑定 exact
   environment/classification/scope/assessment/gate receipt digest 的 `GateCAuthorization`。Gate C
   PASS 只开放 SHADOW；ENABLED 还必须有 `SHADOW requirements PASS`。

`GateCAuthorization` 的 `RESET / RETAIN / MIGRATED` 表示已经完成并独立验证的 Gate C resolution；
`MIGRATION_REQUIRED` Assessment 本身不能直接构造 MIGRATED authority。

## Fail-closed 条件

以下任一条件都不能产生可供 Gate C 使用的 Assessment PASS：

- environment 为 `DISPOSABLE_LOCAL` 或 `UNKNOWN`；
- scope 枚举不完整或 observed scope digest 不匹配；
- 可信观察时间不 qualified；
- expected/observed resource、Worker set 不完全相等；
- resource access、identity、external retention 或 replacement disposition 不完整；
- obligation 枚举不完整或 disposition unknown；
- 任一证据 digest 缺失、长度错误或全零；
- external retention 要求与 `RETAIN_COMPATIBLE` 不一致；
- 已知 resource/obligation migration requirement 尚未闭环。

已知 migration requirement 即使与 incomplete evidence 同时存在也返回 `MIGRATION_REQUIRED`，不会
误降为 RESET/RETAIN candidate。`UNKNOWN` environment 即使携带另一个环境的 Gate C authority 也
拒绝；Gate C scope/classification mismatch 同样拒绝。

## 已验证

- Gate B PASS 时 H1-H6 可按 predecessor 实现；当前代码切片和 disposable-local
  认证已闭合，但 persistent deployment 证据未生成；Gate B PENDING 阻断 H1；
- exact disposable attestation 允许本地 reset/integration test，无 attestation 或 environment
  mismatch 拒绝；
- existing/staging/production 无 Gate C 拒绝，unknown 无条件拒绝；
- exact Gate C PASS 允许 SHADOW，ENABLED 仍要求 SHADOW requirements PASS；
- direct replacement、compatible retention、migration-wins-over-incomplete 与 incomplete paths；
- 每个 resource kind exactly-once read、reader identity substitution 拒绝；
- receipt canonical determinism、schema generation 2、local no-overwrite/symlink protection；
- assessment focused tests、Spotless、main/test Checkstyle。

## 未来 persistent deployment 输入

只有准备接触真实 existing/staging/production environment 时才需要：

1. environment/deployment identity 与 classification；
2. tenant、route、shard、eligible Worker closed exact set 及枚举证据；
3. 每类 runtime resource 的 exact identity、只读连接方式和访问边界；
4. 外部用户数据保留义务的责任人结论及证据；
5. qualified trusted UTC interval evidence；
6. Assessment receipt 的明确本地输出路径；
7. Gate C reviewer/authority、resolution 和 exact receipt binding。

这些输入出现前，不接入可写 Oxia authority、不打开会改变 metadata 的 Store path、不创建
Pulsar Producer、不访问或修改运行资源，也不生成占位 Assessment receipt。它们的缺失只表示
`PENDING_DEPLOYMENT`，不再阻塞 H1-H6 或 exact disposable local testing。
