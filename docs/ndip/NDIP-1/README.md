# NDIP-1 工作包：Pulsar Native Delivery Handoff

## 目录身份

- 工作包目录：`docs/ndip/NDIP-1/`
- 当前改进提案：`NDIP-1`
- 提案状态：`Accepted`
- 审查基线：`main@8915d21ed325a90ec305201ca85ab8daea3803dc`
- H0 实现提交：`main@7cb377ca9dd3135792237af0f027076630d5e4f3`
- 整理日期：`2026-08-27`

仓库的 Accepted `NDP-0001` 持续演进规则保持不变。Accepted
[`NDP-0002`](../../proposals/0002-register-ndip-governance.md) 已完成一次性治理桥接；本工作包
及后续改进提案统一使用 `NDIP`（Nereus Delay Improvement Proposal）编号。`NDIP-1` 已由
维护者明确接受，final `acceptance-receipt.json` 绑定 post-transition exact normative package
digest、审查基线和接受结论，因此 Gate B 为 PASS。

Gate B PASS 是 implementation authorization：H1-H6 可按切片依赖顺序实施，带 exact
`DISPOSABLE_LOCAL` attestation 的本地集成/恢复/故障测试也被允许。Gate C 是
deployment/upgrade authorization；它仍严格阻止 persistent environment mutation、SHADOW 和
ENABLED。H0 不改变权威语义，不受这一命名桥接阻塞。

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

旧的根目录稿件 `docs/修复 Pulsar Handoff 延迟投递.md` 已由本工作包替代，不再保留
两份并行计划。

## 当前可执行边界

```text
NDP-0002 Accepted
+ final NDIP-1 acceptance receipt binds exact package digest
    -> Gate B PASS（当前）
    -> H1 READY；H2-H6 按前置 slice 顺序推进
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

当前没有真实 persistent deployment，不生成虚假 Assessment receipt。G0 状态是
`NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT`：不阻塞 H1-H6，但 existing、staging、
production、unknown 环境在 Gate C PASS 前继续 fail-closed。

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

这不是完整 Handoff 或 physical record chain 的实现：`.deliverAt(...)`、无损 record 投影、
Attempt Journal、P1 evidence/recovery、capability activation 以及 H1-H6 均未实施。NDIP-1
虽为 `Accepted`，仍必须保持 H0 fail-closed，直到后续 gates 明确开放相应范围。

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
| G0 | read-only compatibility/reset assessment tooling 与 receipt | core implemented；`NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT` | 仅在真实 persistent environment 明确后运行；不得修改运行资源 |
| H1 | code-level target，产品决策已关闭 | **H1 READY** | Gate B PASS（已满足） |
| H2-H6 | code-level target，产品决策已关闭 | blocked by predecessor | 依次完成 H1、H2、H3、H4、H5 |
| local disposable testing | synthetic integration/recovery/fault environment | **ALLOWED** | Gate B PASS + exact complete disposable attestation |
| SHADOW | environment-specific deployment | blocked | exact environment Gate C PASS |
| ENABLED | controlled native delivery activation | blocked | Gate C PASS + SHADOW requirements PASS + signed Manifest/source lock/Worker barrier |

H1-H6 开始后仍需在每个切片前重核最新源码签名、P1 source lock 和生成号占用；这属于正常的
source-drift 审计，不得借此重新打开本文已经关闭的产品契约。H0 不应重复执行；当前可正式
开始 H1。G0 pure evaluator、closed inventory、本地 receipt writer 与 `DeploymentSafetyGate` 已
实现；真实 environment 出现前不提供虚假 scope 或 PASS。需要执行提示词时由用户在会话中
另行请求，不写入仓库。
