# G0 DataResetAssessment 执行状态

## 当前结论

- Gate B：`PASS`
- implementation：`H1-H6 code slices implemented; disposable-local certification PASS`
- local disposable integration/recovery/fault testing：`ALLOWED_WITH_EXACT_ATTESTATION`
- G0 tooling core：`implemented`
- G0 lifecycle：`PASS` for exact staging run `20260830014412-97404`
- persistent environment assessment：`PASS_DIRECT_REPLACE`
- authoritative assessment receipt：已生成、签名并验证；仅绑定 `local-docker-staging-ndip1`
- Gate C：`PASS` for exact staging candidate/environment/resource state
- SHADOW：`PASS`（随后已撤销 SHADOW candidate policy）
- ENABLED：`PASS` for one-record staging canary；完成后已回到 `DISABLED`

本状态页现在记录的是一次真实、持久化、可重启的本机 staging 执行，而不是 production readiness。
环境根目录为
`/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1`；
Docker Compose 资源、Oxia/MinIO/Pulsar bind mounts 和所有证据均保留。该 run 的 candidate commit
为 `e7d67fe705d2cc0d87108ef2e07dd1340318fe69`，Accepted package digest 和 P1 source lock
仍分别为 `13caab8ecdc201901f06e905f1c0bf9792780e50c6f5948f93abf2bdb8f4d21b` 与
`0a2536484cd3932801a98dc88ff112b2df88a1c7`。

这不是可跨环境复用的通用 PASS：其他环境仍必须执行自己的只读 G0、signed Manifest 和 Gate C。
不得把 staging receipt 复制到 production 或把本记录写成 NDIP production authority。

执行前没有真实 persistent deployment 的历史状态不再是当前状态；其 `NOT_APPLICABLE_FOR_IMPLEMENTATION /
PENDING_DEPLOYMENT` 文字保留在下方历史说明中，用于解释为什么 disposable receipt 不能替代本次
staging evidence。当前 run 没有 unresolved `PUBLISHING / UNCERTAIN`，没有旧 generation 混用。

本次权威证据入口：

- G0 snapshot：`.../evidence/20260830014412-97404/g0/g0-snapshot.json`，snapshot digest
  `5278f3ee4a4e278aecb597c7aa214e5d744a5d73e8431f99ae64c80c2b2d9cc5`；
- signed DataResetAssessment：`.../authority/data-reset-assessment.signed.json`；assessment
  outcome 为 `PASS_DIRECT_REPLACE`，执行 resolution 为 `RESET`；
- signed DataResetManifest：`.../authority/data-reset-manifest.bin`，manifest digest
  `4aeeba43be47b7bf528ddcb24b4a2b73cc9da47b5b6a58f008b59118d013a890`；
- signed Gate C receipt：`.../authority/gate-c-receipt.signed.json`，SHA-256
  `ed859a2a82a8fafa1a91c8f00d21374a71debeef3b04b80fb2bed185dfa2d1f3`；
- signed SHADOW receipt：`.../authority/shadow-receipt.signed.json`，SHA-256
  `806f8afad3f3d18049c880594f06b7fabefaf592b8cb5e253d2f36df7530ba6d`；
- signed ENABLED canary receipt：`.../authority/enabled-canary-receipt.signed.json`，SHA-256
  `61d06d050949d115cc144a8b80892eed3910789f4d87aceda9188b4dccb37932`；
- signed final rollback policy：`.../authority/disabled-policy.signed.json`，最终状态为
  `DISABLED`，active lease/send 均为 `0`。

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
