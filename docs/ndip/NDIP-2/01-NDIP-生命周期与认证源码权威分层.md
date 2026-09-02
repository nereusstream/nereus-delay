# NDIP-2：NDIP 生命周期与认证源码权威分层

- Status: Accepted
- Authors: Nereus Delay maintainers
- Created: 2026-09-02
- Discussion: `docs/ndip/NDIP-2/`
- Supersedes: `NDP-0002` 中将 Implemented 与 deployment/production closure 绑定的部分解释
- Superseded by:

> Nereus Delay 维护者于 2026-09-02 明确决定：NDIP-1 的实现和生命周期现在关闭；
> 环境部署、性能/规模场景与 production rollout 后续分开进行，不作为 NDIP-1
> `Implemented` 的前置条件。

## 摘要

NDIP 从此同时维护三条互不代替的轨道：

```text
proposal lifecycle: Draft -> Discussion -> Accepted -> Implemented
environment lifecycle: UNASSESSED -> Gate C -> SHADOW -> bounded ENABLED -> safe final state
release lifecycle: deployment -> performance/scale -> production rollout
```

`Implemented` 表示 accepted implementation scope 已关闭，并有 closed implementation receipt
绑定规范文本、认证源码和所需证据。它不表示任意环境已部署，不表示压测或
容量/SLO 已认证，也不表示获得 production authority。

## 动机

NDP-0002 正确将 Gate B implementation authorization 与 Gate C environment authorization
分开，但早期 NDIP-1 完成定义又将 `Implemented` 绑定到 production receipt、实际
cutover 和后续环境活动。这会让一个已完成的功能长期停留在 `Accepted`，也会把部署计划
误当成实现缺口。

原持久化 staging verifier 还要求 final-summary `candidateCommit` 等于待解释 checkout
HEAD。这使 README、执行记录或治理 receipt 的变更也会使已认证的 runtime source
看起来失效，并诱导重跑 24/24、Gate C、SHADOW 和 canary。

## 规范性决定

### 1. `Implemented` 的精确含义

NDIP 可以从 `Accepted` 进入 `Implemented`，仅当：

1. Accepted package 声明的 implementation slices 全部关闭；
2. 提案声明的 correctness/recovery/certification evidence 完成；
3. 没有在 implementation scope 中未解决的正确性状态；
4. authority documents 已同步；
5. 维护者明确决定关闭 lifecycle；
6. closed `implementation-receipt.json` 绑定历史 Accepted authority、Implemented normative
   package、certified runtime source authority 和 exact evidence digests。

`Implemented` 不要求：

- 任意尚未启动的 target environment 完成部署；
- 性能、规模、长时 soak、SLO、跨 region/tenant 或 release readiness 证据；
- production rollout 或 production data-disposition authority。

某个 NDIP 可以把一次环境认证声明为 implementation closure evidence。该证据的
environment classification 和 `productionAuthority` 不因此改变。

### 2. NDIP-1 的关闭规则

NDIP-1 使用 fixed `local-docker-staging-ndip1` 的完整 certification 作为 implementation
closure evidence。该 certification 必须同时包含：

- exact disposable-local 24/24 与 3/3 supporting checks；
- G0 和 13/13 Manifest readback；
- Gate C 41/41；
- SHADOW `0/0/0`；
- AUTO_FAST `1/1/0`；
- Managed Handoff `1/1/1`；
- response-loss / Attempt Journal recovery；
- final `DISABLED` rollback 与 zero active lease/send；
- independent validator PASS；
- `productionAuthority=false`。

这关闭 NDIP-1 implementation 和 proposal lifecycle，不关闭其他环境的 deployment lifecycle。

### 3. certified runtime source authority

认证证据仍绑定原始 `candidateCommit`；历史 run 不得重签、改写或换 commit。
当前 checkout 是否与该 run 具有同一 runtime source identity，改为通过 closed
`NDIP_RUNTIME_SOURCE` scope 计算 digest：

```text
SHA-256(
  "nereus-delay-runtime-source-authority\0"
  || for each registered runtime/build input path in unsigned UTF-8 order:
       u32be(pathLength) || pathUtf8 || SHA-256(projectedExactBytes)
)
```

scope 由 verifier 注册，receipt 不能缩小它。它包含：

- registered runnable source sets：`src/main/**`、`src/real-cross/**`、
  `src/real-kafka/**`、`src/real-pulsar/**`；已知操作系统 metadata（如 `.DS_Store`）不参与，
  其他 untracked runtime-source path 仍导致 path-set mismatch；
- Gradle wrapper 和项目构建输入；
- `build.gradle` 中除 NDIP receipt/documentation/source-identity governance block 外的 exact bytes。

它不包含 README、NDIP 执行记录、status/audit 记录、receipt、`src/test/**`、Checkstyle/
formatting 配置或其他非 runtime 文件。normative package 由独立 digest 管理，不与 runtime
digest 混合。若增加或改变 runnable source set，`build.gradle` 的非治理 bytes 会先发生变化并
使 digest fail-closed。

当前 runtime digest 与 implementation receipt 中认证 digest 相同时，非规范文档改动
不需重跑认证。任何 registered runtime path、文件集、构建依赖或非 governance
`build.gradle` bytes 变化都使 digest 不同，必须 fail-closed 并使用新的认证证据。

### 4. certification tooling history

runner、validator 和 e2e composition 以独立 historical tooling digest 绑定到原始
certified commit。当前 tooling 变更不能改写历史 run，也不会仅因为 verifier 文件
后来变动就改变当时证据的身份。

### 5. environment 和 release 边界

每个 target environment 继续独立要求 G0、signed Manifest、Gate C、SHADOW、bounded
ENABLED/canary 和 safe final state。认证源码相同不等于环境相同，不允许借用其他
environment receipt。

性能/规模测试及 production rollout 在后续独立工作中定义场景、阈值、环境和
authority。NDIP-1 Handoff 场景应在彼时纳入，但不回溯性重开 NDIP-1 lifecycle。

## 安全与失败语义

- receipt、digest、path set、historical commit 或 evidence digest 不一致时 fail-closed；
- current runtime digest 不同时，不得把旧 run 解释为当前 runtime authority；
- lifecycle authority 与 production authority 必须使用不同字段；
- `Implemented` 收据必须显式记录 `productionAuthority=false`；
- 不允许复制历史 receipt、修改证据或降低 validator 来强制 PASS。

## 决策结论

采用本提案。NDP-0002 的 Accepted authority、Gate B / Gate C 分权、外置 receipt 与
fail-closed 规则保持有效；仅其 `Implemented` 与 deployment/production completion 的耦合解释
被本 NDIP 替代。
