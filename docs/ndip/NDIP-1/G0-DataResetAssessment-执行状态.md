# G0 DataResetAssessment 执行状态

## 当前结论

- Gate B：`PASS`
- implementation：`H1-H6 code slices implemented; disposable-local certification PASS`
- local disposable integration/recovery/fault testing：`ALLOWED_WITH_EXACT_ATTESTATION`
- G0 tooling core：`implemented`
- G0 lifecycle：`PASS` for exact staging run `20260830035421-73816`
- persistent environment assessment：`PASS_DIRECT_REPLACE`
- authoritative assessment receipt：已生成、签名并验证；仅绑定 `local-docker-staging-ndip1`
- Gate C：`PASS` for exact staging candidate/environment/resource state
- SHADOW：`PASS`（随后已撤销 SHADOW candidate policy）
- ENABLED：`PASS` for one-record staging canary；完成后已回到 `DISABLED`

本状态页现在记录的是一次真实、持久化、可重启的本机 staging 执行，而不是 production readiness。
环境根目录为
`/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1`；
Docker Compose 资源、Oxia/MinIO/Pulsar bind mounts 和所有证据均保留。该 run 的 candidate commit
为 `ed3cc4987dab6ebf179f6bfafcfd159c2e54188e`，Accepted package digest 和 P1 source lock
仍分别为 `13caab8ecdc201901f06e905f1c0bf9792780e50c6f5948f93abf2bdb8f4d21b` 与
`0a2536484cd3932801a98dc88ff112b2df88a1c7`。

这不是可跨环境复用的通用 PASS：其他环境仍必须执行自己的只读 G0、signed Manifest 和 Gate C。
不得把 staging receipt 复制到 production 或把本记录写成 NDIP production authority。

执行前没有真实 persistent deployment 的状态已经由本次 staging deployment 取代；其
`NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT` 文字保留在下方历史说明中，用于解释为什么
disposable receipt 不能替代本次 staging evidence。当前 run 没有 unresolved `PUBLISHING / UNCERTAIN`，
没有旧 generation 混用。

本次权威证据入口：

- G0 snapshot：`.../evidence/20260830035421-73816/g0/g0-snapshot.json`，snapshot 文件 SHA-256
  `3cffa885a3bee9d7578145c91dc868f1e9c4024be6260fe49bff61550f05730a`，canonical snapshot digest
  `2024d837493cafe5dcd55017ca4c08668ca04d20582397f4fc4d9e7c3c8b8ae8`；
- signed DataResetAssessment：`.../authority/data-reset-assessment.signed.json`，SHA-256
  `979cb8dcb8999d63eabdb823739baa8a390d98df9ef8bd9659b1556d0b10399f`；assessment outcome 为
  `PASS_DIRECT_REPLACE`，执行 resolution 为 `RESET`；
- signed DataResetManifest：`.../authority/data-reset-manifest.bin`，文件 SHA-256
  `8c9e8847c96e032b075857e28d45f40c38bf7c8988ee7ae2a713f31d535b7bfb`，manifest digest
  `ffd9d9aa85b157a6d055576b0693c2ed2adcb49388357bdb684bab6a4d1c76a0`；
- signed Gate C receipt：`.../authority/gate-c-receipt.signed.json`，SHA-256
  `e104e80691559dd0878af3bd0b8fd39f643de1802e023732d01da383fc2fdfd4`；
- signed SHADOW receipt：`.../authority/shadow-receipt.signed.json`，SHA-256
  `c395a98a8181c9ec448eb0e443d22cc8e51f1f3e4435e157bf39a1327e7639e2`；
- signed ENABLED canary receipt：`.../authority/enabled-canary-receipt.signed.json`，SHA-256
  `7bef3f194f747cc0b867ffb975840c1e3f020938281ad2d6f2a087ae7acead`；
- signed final rollback policy：`.../authority/disabled-policy.signed.json`，最终状态为
  `DISABLED`，active lease/send 均为 `0`。

2026-08-28 的 `DISPOSABLE_LOCAL` certification runner 已在独占、可销毁环境中完成真实 native、
recovery、fault、Oxia/MinIO 和 cleanup 矩阵；local receipt 为 `PASS`，24 个 cell 全部
`EXECUTED_PASS`，且 `EXECUTED_FAIL=0`、`NOT_COVERED=0`、`skipped=0`。该结果只证明
本地认证边界，不创建 Assessment scope，不改变下面的 persistent deployment 状态。

`NOT_APPLICABLE_FOR_IMPLEMENTATION` 只表示 G0 不是 H1-H6 的前置条件；它不是 Gate C PASS。
上面的历史边界描述的是本次 staging deployment 之前的状态；当前已使用真实 closed scope 完成
G0、Assessment、Manifest readback 和后续 Gate C/SHADOW/canary。其他 existing/staging/production
environment 仍必须各自重新执行 G0，不能复用本次 receipt。

## H1-H6 implementation boundary

H1-H6 的代码切片已经落入当前主干：current generation contract/store、signed
handoff policy and dynamic scheduler/admission、Attempt Journal、source-locked P1
record/evidence path、exact-timestamp AUTO_FAST，以及 signed manifest/activation
barriers。`DataResetManifest` 仅接受 fresh resource、zero-obligation proof 和所有 Worker
对同一 `ArtifactGenerationSet` 的 current declaration；`DataResetActivationGate` 在
startup、assignment、source apply 和 physical send 边界拒绝 stale/mixed generation。

上述 implementation gate 文字保留的是 `68fe2c29` 时尚未部署的历史状态；本次 exact staging run
已经生成并验证 Assessment、Manifest、Gate C、SHADOW 和 canary receipt。当前可用证据仍只绑定
本页开头列出的 environment/resource state，不是 production authority。

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
  认证已闭合；本次 persistent staging evidence 已生成，但 Accepted package receipt 自身仍
  保持 `gate_c=PENDING_DEPLOYMENT`，不能被 staging receipt 改写；
- exact disposable attestation 允许本地 reset/integration test，无 attestation 或 environment
  mismatch 拒绝；
- existing/staging/production 无 Gate C 拒绝，unknown 无条件拒绝；
- exact Gate C PASS 允许 SHADOW，ENABLED 仍要求 SHADOW requirements PASS；
- direct replacement、compatible retention、migration-wins-over-incomplete 与 incomplete paths；
- 每个 resource kind exactly-once read、reader identity substitution 拒绝；
- receipt canonical determinism、schema generation 2、local no-overwrite/symlink protection；
- assessment focused tests、Spotless、main/test Checkstyle。

## Persistent deployment 输入（本次已绑定；其他环境仍需重新提供）

只有准备接触真实 existing/staging/production environment 时才需要：

1. environment/deployment identity 与 classification；
2. tenant、route、shard、eligible Worker closed exact set 及枚举证据；
3. 每类 runtime resource 的 exact identity、只读连接方式和访问边界；
4. 外部用户数据保留义务的责任人结论及证据；
5. qualified trusted UTC interval evidence；
6. Assessment receipt 的明确本地输出路径；
7. Gate C reviewer/authority、resolution 和 exact receipt binding。

本次 staging run 已为上述输入建立 exact、签名、不可覆盖的 evidence；下一次 deployment/upgrade
仍必须重新绑定并复验。缺少这些输入时，runner 继续保持 fail-closed，不接入可写 Oxia authority、
不打开会改变 metadata 的 Store path、不创建 Pulsar Producer、不访问或修改运行资源，也不生成
占位 Assessment receipt；该 deployment 保持 `PENDING_DEPLOYMENT`，不影响 H1-H6 或 exact
disposable local testing 的既有授权边界。
