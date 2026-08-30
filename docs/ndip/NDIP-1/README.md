# NDIP-1 工作包：Pulsar Native Delivery Handoff

## 目录身份

- 工作包目录：`docs/ndip/NDIP-1/`
- 当前改进提案：`NDIP-1`
- 提案状态：`Accepted`
- 审查基线：`main@8915d21ed325a90ec305201ca85ab8daea3803dc`
- H0 实现提交：`main@7cb377ca9dd3135792237af0f027076630d5e4f3`
- H1-H6 实现提交：`main@c7c99d377dc9e8bb786032173d62d1981011a4e2`
- Managed Attempt Journal / recovery 闭环提交：
  `main@62cb5e322edbc98e9a97c0d15dc017b06cdf5fd7`
- Disposable-local 24-cell 认证代码与 source anchor：
  `main@da15290e47b9255403c92e4ebba3c7d5189edb75`
- 整理日期：`2026-08-28`

仓库的 Accepted `NDP-0001` 持续演进规则保持不变。Accepted
[`NDP-0002`](../../proposals/0002-register-ndip-governance.md) 已完成一次性治理桥接；本工作包
及后续改进提案统一使用 `NDIP`（Nereus Delay Improvement Proposal）编号。`NDIP-1` 已由
维护者明确接受，final `acceptance-receipt.json` 绑定 post-transition exact normative package
digest、审查基线和接受结论，因此 Gate B 为 PASS。

Gate B PASS 是 implementation authorization：H1-H6 可按切片依赖顺序实施，带 exact
`DISPOSABLE_LOCAL` attestation 的本地集成/恢复/故障测试也被允许。Gate C 是
deployment/upgrade authorization；在具体环境完成 G0、签名 Manifest 和 Gate C 前仍严格阻止
该环境的 persistent mutation、SHADOW 和 ENABLED。H0 不改变权威语义，不受这一命名桥接阻塞。

本轮实现开始时的前置状态为 `H1 READY`；H1→H6 已按依赖顺序完成代码切片，disposable-local
认证矩阵也已闭合。随后已在固定的 `local-docker-staging-ndip1` 上执行 G0/Gate C/SHADOW 和
最小 canary；该环境最终保持 `DISABLED`，其他环境仍须独立执行同一组门禁。

`normative package digest` 不包含本 README 和 candidate/final receipt，避免自引用与操作文本
改变设计身份。仓库不保存会话执行提示词；需要时由用户请求并在会话内临时生成。摘要固定
包含 `01`、`02`、`03`、`04` 四个文件；每个 path 必须是
**相对于仓库根目录的 UTF-8 POSIX path**，不允许绝对路径、`.`、`..`、反斜杠或 Unicode
normalization 替换。文件必须是无 BOM 的 strict UTF-8、只使用 LF，并以且仅以一个 LF 结尾。
条目按完整 repository-root-relative path 的 unsigned UTF-8 bytes 升序排列。摘要输入固定为：

```text
SHA-256(
  "nereus-delay-ndip-package\0"
  || for each file: u32be(pathLength) || pathUtf8 || SHA-256(exactFileBytes)
)
```

Accepted 后任一 normative file 改动都必须产生新的治理审查/摘要绑定，不能沿用旧 receipt。

摘要校验命令为：

```bash
python3 scripts/verify-ndip-package.py \
  --package-dir docs/ndip/NDIP-1 \
  --receipt docs/ndip/NDIP-1/acceptance-receipt.json \
  --require-accepted
```

校验成功必须输出 `receipt_status=ACCEPTED`、`authority=true`、`gate_b=PASS`、
`implementation=AUTHORIZED` 和 `local_disposable_testing=AUTHORIZED_WITH_EXACT_ATTESTATION`。
receipt 同时固定 `gateCRequiredBeforeShadow=true` 与 `gateCRequiredBeforeEnabled=true`。

## 2026-08-30 persistent staging certification 状态

本工作包已在固定的本机持久化环境
`environmentId=local-docker-staging-ndip1`（`classification=STAGING`）完成一次新的、候选版本绑定的
G0 → Gate C → SHADOW → 最小 ENABLED canary。当前可审计结论是：Gate C `PASS`、SHADOW
`PASS`、ENABLED canary `PASS`，随后已签发并验证 `DISABLED` rollback；这只授予该 exact staging
环境的有限证据，不是 production authority，也不把 NDIP 状态改写为 production readiness。

完整命令、环境拓扑、41 个条件 skip 的分类、签名 receipt、故障恢复和保留/回滚边界见
[`06-Persistent-Staging-Gate-C-SHADOW-执行记录.md`](06-Persistent-Staging-Gate-C-SHADOW-执行记录.md)。
该记录是非规范执行/运维材料；Accepted package 仍只由 `01`–`04` 和现有
`acceptance-receipt.json` 定义。

## 文档地图

1. [`01-调查与决策记录.md`](01-调查与决策记录.md)
   - 记录源码现状、问题根因、已经确认的产品决策和被淘汰的旧结论。
2. [`02-NDIP-1-Pulsar-Native-Delivery.md`](02-NDIP-1-Pulsar-Native-Delivery.md)
   - 沿用现行提案必填结构、已 Accepted 的 NDIP 改进提案，是本工作包的设计权威入口。
3. [`03-实施计划.md`](03-实施计划.md)
   - 按治理门、依赖关系、代码切片和验证 gate 组织的实施计划。
4. [`04-代码级目标设计.md`](04-代码级目标设计.md)
   - 固定 exact enum/wire、schema、hash、dual-head scheduler、Journal、P1 transport、evidence
     和 activation 契约，是实现会话的代码级输入。
5. [`acceptance-receipt.json`](acceptance-receipt.json)
   - 绑定 Accepted post-transition exact bytes 和维护者决定；它证明 Gate B 与 implementation
     authority，不提供 Gate C 或 activation authority。
6. [`G0-DataResetAssessment-执行状态.md`](G0-DataResetAssessment-执行状态.md)
   - 记录只读 G0 tooling、lifecycle safety guard、验证证据和 pending deployment；它不是
     Assessment receipt 或 Gate C
     authority。
7. [`05-Disposable-Local-Certification-执行记录.md`](05-Disposable-Local-Certification-执行记录.md)
   - 非规范地记录一次 source-bound `DISPOSABLE_LOCAL` 本地认证、矩阵结果和 exact cleanup；
     它不创建 Assessment scope，也不提供 deployment authority。
8. [`06-Persistent-Staging-Gate-C-SHADOW-执行记录.md`](06-Persistent-Staging-Gate-C-SHADOW-执行记录.md)
   - 非规范地记录固定本机 `STAGING` 环境的 G0、签名 Manifest、Gate C、SHADOW、最小 ENABLED
     canary 和最终 DISABLED rollback；它不提供 production authority。

旧的根目录稿件 `docs/修复 Pulsar Handoff 延迟投递.md` 已由本工作包替代，不再保留
两份并行计划。

## 当前可执行边界

```text
NDP-0002 Accepted
+ final NDIP-1 acceptance receipt binds exact package digest
    -> Gate B PASS（当前）
    -> H1-H6 按前置顺序实现；当前代码切片与 disposable-local 认证已完成
    -> exact disposable local integration/recovery/fault testing ALLOWED

H1 -> H2 -> H3 -> H4(.deliverAt) -> H5 -> H6
    -> local disposable integration/recovery/fault tests

first persistent deployment / upgrade
    -> G0 DataResetAssessment
    -> Gate C PASS
    -> SHADOW
    -> SHADOW requirements PASS + Gate C PASS
    -> ENABLED
```

执行前没有真实 persistent deployment 时，不生成虚假 Assessment receipt；那一阶段的 G0 状态是
`NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT`。本目录现已另有一份绑定
`local-docker-staging-ndip1` 的真实执行记录：该 staging 的 G0、Gate C、SHADOW 和最小 canary
已完成，最终策略保持 `DISABLED`；其他 existing、staging、production 或 unknown 环境仍不得
借用这份 authority，继续 fail-closed。

## 2026-08-28 实现切片状态

H1-H6 的当前代码切片已经按 `H1 -> H2 -> H3 -> H4(.deliverAt) -> H5 -> H6`
顺序落入主干并配有 focused tests：H1 的 generation-2 contract、generation-5
store 与双 READY head，H2 的签名 policy/head、动态 eligibility 与 snapshot-frozen
admission，H3 的无损 prepared record、固定 sequence 和 Attempt Journal，H4 的
source-locked P1 encoder/transport/evidence，H5 的无 clock shift AUTO_FAST，以及
H6 的 signed `DataResetManifest`、`ArtifactGenerationSet` activation 和四个运行时
generation gates 均已实现。

`main@62cb5e322edbc98e9a97c0d15dc017b06cdf5fd7` 进一步关闭了此前只在独立组件中存在、尚未接入
真实 Worker 的 Managed Pulsar Attempt Journal 链路：生产 Worker 现在必须先持久化固定
`sequenceId` mapping，再取得物理 reservation 并在 SEND 前持久化 ownership marker；P1
Producer 名称、current prepared descriptor、Journal replay 和 RocksDB projection 使用同一身份。
新 owner 只能检查并退休旧 owner 的 pre-ownership mapping，随后产生
`RECOVERY_FIRST_SEND_UNCERTAIN`，不得继承 SEND 权限或把旧尝试推断为 `PUBLISHED`。相应
fresh-process smoke 明确验证 target SEND 为 0。后续又增加了 definitive Outcome 后的真实
SIGKILL/replacement-Worker cut、未解决 `UNCERTAIN` drain 阻断及 TTL/retention 原生风险
cell；完整 24-cell disposable certification 曾在 `main@da15290e` 通过。

随后对 H1-H6 做的独立生产路径审查又关闭了四组不能由旧 fixture 代替的缺口：

- `main@52285864` 将 Attempt Journal 的 committed-response loss 解析接入真实 P1 Journal，
  并保持 mapping-before-send、固定 sequence、ownership marker 与 replacement-Owner
  `RECOVERY_FIRST_SEND_UNCERTAIN` 边界；
- `main@7d4ed573` 强制九项 reserved properties 按 unsigned UTF-8 bytes 排序，并把带认证
  P1 拒绝证据的 definite rejection 保持为 definitive non-publication；
- `main@8b25fbda` 把 current `PreparedSubmission` 的 hash-bound native record 从默认
  SDK/Gateway coordinator 接到 prepared-record sender，禁止退回 envelope-only H0 sender；
- `main@913a2a2e` 引入闭合 `PhysicalSendActivationGate`：disposable 测试只能使用完整
  attestation，persistent send 必须同时满足 Gate C、SHADOW requirements、签名
  `DataResetManifest` 与 exact running source baseline。Managed Handoff 缺少该 authority 时在
  Attempt Journal 写入和 Producer ownership 前拒绝，AUTO_FAST 在 transport ownership 前拒绝。

这里的“已实现”仅表示代码/本地 disposable 验证边界；它不表示 NDIP 已进入
`Implemented`，也不生成 production Manifest、Gate C、SHADOW、ENABLED 或 cutover
evidence。H0 仍然 fail-closed，缺少 exact current generation、signed manifest 或能力
gate 时不得触碰 Producer/物理适配器。2026-08-30 的 exact staging evidence 见上方执行记录；
Accepted package receipt 自身仍保持 `gate_c=PENDING_DEPLOYMENT`，不被 staging 结果改写。

## 2026-08-28 disposable local certification

runner `e2e/run-disposable-local-certification.sh` 已在 source-locked P1、真实双 Broker、
BookKeeper/metadata、Oxia、MinIO、RocksDB 和 ownership/failover 边界上完成严格
fail-on-missing 运行。receipt 为 `PASS`：24 个单元格全部 `EXECUTED_PASS`，
`EXECUTED_FAIL=0`、`NOT_COVERED=0`、`skipped=0`。完整命令、source/config/attestation
binding、证据路径与 cleanup 审计见
[`05-Disposable-Local-Certification-执行记录.md`](05-Disposable-Local-Certification-执行记录.md)。

`main@da15290e47b9255403c92e4ebba3c7d5189edb75` 的 generation-1 receipt 是独立审查前的
历史 PASS，不得替代后续 HEAD。当前 runner 生成 generation-2 receipt，并把
`p1.nativeCoordinator` 加入 closed supporting checks；该检查必须真实经过
`PreparedSubmission -> resolver -> coordinator -> guarded transport -> source-locked P1 encoder`
后才允许 PASS。当前 source-bound 结果以持久化 artifact 目录中的 receipt 及 verifier 结果为准，
不得从本 README 的历史 SHA 推断。

这只是 local disposable certification receipt/report；它不创建真实 DataResetAssessment
scope，不是 Gate C authority，也不允许 SHADOW 或 ENABLED。该 disposable receipt 仍不能替代
上方 exact staging run 的 G0/Gate C authority；当前 staging 结论见本 README 的 2026-08-30
状态段和 `06-Persistent-Staging-Gate-C-SHADOW-执行记录.md`，Accepted package receipt 自身仍为
`gate_c=PENDING_DEPLOYMENT`。

本轮 H1-H6 代码切片的历史锚点为
`main@c7c99d377dc9e8bb786032173d62d1981011a4e2`；独立审查前的 disposable certification 绑定
`main@da15290e47b9255403c92e4ebba3c7d5189edb75`。P1 correctness-critical source
lock 固定为 `nereus/delay-resource-guard@0a2536484cd3932801a98dc88ff112b2df88a1c7`。主线
`test`、`check`、`compileRealPulsar`、binding smoke 和 H0 smoke 均通过；独占 disposable
运行还实际覆盖真实双 Broker、Oxia/MinIO、ownership transfer、restart/reopen、
response-loss 与 TTL/retention 路径。
`Exclusive` 的立即可见行为按 P1 原生契约保留为显式风险，不被包装成 not-before 保证。
普通 `./gradlew test` 中没有真实服务的测试仍可按条件 skip；严格认证入口不接受 skip。

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

这不是部署、SHADOW、ENABLED 或 release certification：disposable-local real Broker behavior
matrix 已闭合，但生产 DataResetAssessment、Gate C、persistent reset receipt 和 cutover 仍未执行。
当前代码切片已经实现
`.deliverAt(...)` 的 generation-bound projection、无损 record/Attempt Journal、P1 evidence、
AUTO_FAST timestamp alignment 和 activation gates，但 H0 仍作为缺少 exact generation/manifest/
capability 的 fail-closed fallback。NDIP-1 虽为 `Accepted`，在 Gate C 与后续生产证据完成前不得
标记 `Implemented`。

## 当前结论

Handoff 方向成立，但它不是 ordinary Managed 语义的透明优化。最终设计必须同时保留：

- `NEREUS_MANAGED_NOT_BEFORE`：Nereus 持有到 `deliverAt`，目标 Pulsar Message 不设置
  `deliverAt`；
- `PULSAR_NATIVE_DELIVERY`：显式选择 Pulsar 原生投递，允许提前持久化并设置 Pulsar
  `deliverAt`，消费行为、精度和运维风险继承 Pulsar 原生语义。

默认策略必须是 `NativeDeliveryPolicy.FORBID`，并在 wire 中显式编码。第一阶段原生投递
只允许 `BEST_EFFORT`，不宣称跨 Subscription 的严格 not-before 或消费顺序。

## 代码级就绪结论

此前的 R1-R6 已在本 Accepted NDIP 中收敛为唯一答案，详见
[`04-代码级目标设计.md`](04-代码级目标设计.md)：

- Schedule policy 与 actual contract 分权，不增加 `DispatchMode`；
- Admission 冻结 snapshot，Producer ownership marker 前再次验证 lease；
- AUTO_FAST 删除 clock shift，目标 timestamp 精确等于业务 `deliverAt`；
- G0 `DataResetAssessment` 只判断已有 persistent environment 的兼容/重置策略，不阻塞实现；
- H6 signed `DataResetManifest` 重新证明 actual zero obligation、fresh resource 和 Worker exact
  generation，阻塞 activation；
- 一个 `ArtifactGenerationSet` 复用现有 Worker capability 与 source-ordered activation；
- Admission 只绑定 RecordTemplate，Journal 后才产生 sequence/final Record；record hash 与
  command hash 使用不同 domain，通过 source-locked encoder 和 golden vectors关联。

本轮还关闭了原计划未识别的 scheduler 问题：使用同一 Lane/DRR 下的 ordinary/native 双
head 投影，防止 policy disabled 或 lead 缩小时 native head 阻塞 ordinary due。

因此，设计内容已经达到代码级目标设计；它固定产品契约、持久化/协议目标、状态机、类级
落点和验收门，但不是承诺当前主干签名永不变化的逐行补丁说明。执行就绪度必须分开判断：

| 范围 | 设计详细度 | 当前状态 | 前置条件 |
|---|---|---:|---|
| H0 | patch-ready，五个生产入口与测试落点已固定 | complete；不得重复实施 | 已有 code/docs receipt |
| G0 | read-only compatibility/reset assessment tooling 与 receipt | core implemented；exact local staging `PASS_DIRECT_REPLACE` | 其他真实 persistent environment 仍需独立运行；不得修改运行资源 |
| H1 | code-level contract/store slice | **implemented; disposable certification passed** | focused/full gates passed |
| H2 | signed policy/scheduler/admission slice | **implemented; disposable certification passed** | H1 + focused/full gates passed |
| H3 | Attempt Journal/physical handoff slice | **implemented; disposable certification passed** | H2 + focused/full gates passed |
| H4 | P1 encoder/transport/evidence slice | **implemented; 24-cell source-locked certification passed** | H3 + source-locked P1 gates passed |
| H5 | AUTO_FAST contract/timestamp/recovery slice | **implemented; disposable certification passed** | H4 + focused/full gates passed |
| H6 | activation/reset/generation-barrier slice | **implemented; disposable tests and exact staging evidence passed** | H5 + each environment's exact gates |
| local disposable testing | synthetic integration/recovery/fault environment | **ALLOWED; current 24-cell run PASS** | Gate B PASS + exact complete disposable attestation |
| SHADOW | environment-specific deployment | **exact local staging PASS; other environments blocked** | exact environment Gate C PASS |
| ENABLED | controlled native delivery activation | **exact local staging canary PASS, then DISABLED** | Gate C PASS + SHADOW requirements PASS + signed Manifest/source lock/Worker barrier |

H1-H6 代码切片完成后仍需在每个发布/部署边界重核最新源码签名、P1 source lock 和生成号
占用；这属于正常的 source-drift 审计，不得借此重新打开本文已经关闭的产品契约。G0 pure
evaluator、closed inventory、本地 receipt writer、`DeploymentSafetyGate` 与 H6 manifest gate
均已实现；没有该环境自己的新鲜 scope 时不提供虚假 scope 或 PASS。需要执行提示词时由用户在
会话中另行请求，不写入仓库。
