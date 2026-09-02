# NDP-0002：注册 NDIP 治理与外置接受凭证

- Status: Accepted
- Authors: Nereus Delay maintainers
- Created: 2026-08-27
- Amended: 2026-08-27（Gate B / Gate C 生命周期分权）
- Discussion: `docs/ndip/NDIP-1/`
- Supersedes: Accepted 后替代后续改进提案继续使用 NDP 身份与 `docs/proposals/` 入口的规则
- Superseded by: `NDIP-2` 替代 Implemented 与 deployment/production completion 耦合的部分解释；其余规则保持有效

> 本提案于 2026-08-27 经 Nereus Delay 维护者明确接受，并在同日明确修订生命周期：
> `Gate B = implementation authorization`，`Gate C = deployment / upgrade authorization`。
> `NDIP-1` final receipt 验证通过后允许按 H1-H6 顺序实施，并允许带 exact disposable
> attestation 的本地集成测试；它不授权 persistent environment mutation、SHADOW 或 ENABLED。

## 摘要

Nereus Delay 后续重大改进统一使用 `NDIP-N`（Nereus Delay Improvement Proposal），每个提案
使用 `docs/ndip/NDIP-N/` 工作包。Accepted 状态由工作包外置的 closed acceptance receipt 和
exact normative package digest 共同证明，避免状态文件对自身摘要产生循环依赖。

本提案是现行 NDP 治理下的一次性 bootstrap。Accepted 后，NDP-0001 和本提案作为历史治理
记录永久保留，但不再为后续产品改进分配新的 NDP 编号。

## 动机

`NDIP-1` 已把调查、提案、实施计划和代码级目标设计组织为一个工作包，但当前
Accepted NDP-0001 和 `checkDocumentation` 只识别 `docs/proposals/`。仅修改 NDIP 文件中的
`Status` 会绕过现行治理；仅保存一个摘要又不能证明维护者接受了什么。

需要建立一个无自引用、可机器复算、明确区分 candidate integrity 与 acceptance authority 的
桥接规则，同时不把治理编号误解为项目发布版本。

## 范围

- 注册 `NDIP-N` 身份、目录布局和状态流转；
- 定义 normative package 的 repository-root-relative digest；
- 定义 closed candidate/final acceptance receipt；
- 定义 verifier 与 `checkDocumentation` 的职责；
- 定义从当前 NDP 治理到 NDIP 治理的一次性接受过程；
- 定义 Gate B implementation authority 与 Gate C deployment authority 的分权；
- 将 `NDIP-1` 作为首个使用该机制的工作包。

## 非目标

- 本提案不把 proposal acceptance、implementation authority 或 deployment authority 合并为同一门；
- 不改变 Pulsar Handoff、not-before、wire、Store、evidence 或 runtime policy 语义；
- 本 bootstrap 提案自身不运行 environment `DataResetAssessment`，也不定义 H1-H6 产品实现；
  G0 core 与 H1-H6 的状态由 NDIP-1 工作包记录；
- 不创建、删除、清理、迁移或激活任何运行资源；
- 不删除 NDP-0001 或本提案的历史记录；
- 不允许 candidate、文件名、Git commit 或 verifier 自行产生维护者权限。

## 接受时约束

1. Accepted NDP-0001 规定只有 Accepted NDP 才能修改权威设计。
2. 当前 `checkDocumentation` 固定验证 NDP-0001、NDP 模板和 `docs/proposals/` 索引。
3. `NDIP-1` 的状态转换必须由 final receipt 绑定 post-transition exact normative bytes；H0 已
   fail-closed。Gate B PASS 后 H1 为 READY，后续 slice 仅受 H1-H6 自身依赖顺序约束；Gate C
   不得作为 implementation 前置。
4. `NDIP-1` 的 README 和 receipt 不能进入自身 normative digest，否则会产生自引用或把操作
   文本提升为产品契约；会话执行提示词不保存到仓库。
5. 接受决定必须由维护者显式给出，自动化只能验证 bytes 与 receipt 的一致性。

## 提议设计

### 1. NDIP identity 与目录

本提案 Accepted 后，新的重大改进使用单调递增的 `NDIP-N`，目录为：

```text
docs/ndip/NDIP-N/
```

每个工作包至少包含 README、提案正文、实施计划和代码级设计。README 明确列出 normative
files；candidate/final receipt、assessment 和运行证据默认不属于 normative package，除非后续
Accepted NDIP 明确改变。会话执行提示词由用户需要时临时生成，不作为仓库 artifact。

状态仍为：

```text
Draft -> Discussion -> Accepted -> Implemented
```

终止状态仍为 `Rejected / Withdrawn / Superseded`。编号只表示提案身份，不表示产品版本线。

### 2. normative package digest

每个 normative path 必须是相对仓库根目录的 UTF-8 POSIX path。文件必须是无 BOM 的 strict
UTF-8、只含 LF，并以且仅以一个 LF 结尾；条目按完整 path 的 unsigned UTF-8 bytes 排序。

```text
SHA-256(
  "nereus-delay-ndip-package\0"
  || for each file:
       u32be(pathUtf8ByteLength)
       || pathUtf8
       || SHA-256(exactFileBytes)
)
```

path 不允许绝对形式、`.`、`..`、反斜杠或 Unicode normalization 替换。verifier 中的已注册
path set 与 receipt path set 必须 exact equality；不能通过从 receipt 删除文件缩小审查范围。

### 3. candidate 与 final receipt

`acceptance-receipt.candidate.json` 是 closed review artifact，必须满足：

- `receiptStatus = CANDIDATE`；
- `authority = false`；
- governance observed status 为 `DRAFT`；
- decision fields 全部 pending/null；
- `gateB = PENDING`；
- `implementationAuthorized = false`；
- `localDisposableTestingAuthorized = false`；
- `gateCRequiredBeforeShadow = true`；
- `gateCRequiredBeforeEnabled = true`。

candidate verifier PASS 只证明当前 Draft bytes 与审查清单一致。candidate 不得被重命名为 final
receipt，也不得作为 G0 前置凭证。

维护者明确接受后，状态转换必须基于 exact reviewed diff。任何 normative bytes 改动——包括
status metadata——都要重新计算 post-transition digest。最终 `acceptance-receipt.json` 必须：

- `receiptStatus = ACCEPTED`、`authority = true`、`gateB = PASS`；
- 绑定 post-transition exact file hashes 和 package digest；
- 记录非空 `acceptedBy`、`acceptedAt` 和 `decisionReference`；
- 证明本 NDP-0002 已 Accepted；
- `implementationAuthorized = true`，因此 Gate B PASS 明确授权 H1-H6 实施；
- `localDisposableTestingAuthorized = true`，但每次 create/reset/destroy/rebuild/test 都必须再验证
  exact `DISPOSABLE_LOCAL` attestation；
- 明确 Gate C 在 SHADOW/ENABLED 前仍独立必需。

final receipt 自身不进入 normative digest。包含该 receipt 的 Git commit 提供 repository
provenance；receipt 不尝试嵌入自身 commit hash。

### 4. verifier 与 authority 分权

`scripts/verify-ndip-package.py`：

- 使用 closed schema，拒绝 duplicate JSON keys、unknown fields、unknown package/path set；
- 严格验证编码、行尾、路径排序、每文件 hash 和 package hash；
- candidate 校验成功时输出 `authority=false / gate_b=PENDING`；
- `--require-accepted` 必须拒绝 candidate；
- accepted 模式要求完整 decision fields、`gateB=PASS`、implementation/local-disposable authority，
  并强制保留 SHADOW/ENABLED 的 Gate C 前置。

自动 gate 只验证 committed authority artifact 是否自洽；它不能创建、推断或代替维护者决定。

### 5. NDIP-1 Gate B

NDIP-1 Gate B 只有同时满足以下条件才为 PASS：

1. 本 NDP-0002 已由维护者明确接受并按提案更新治理索引/gate；
2. NDIP-1 normative package 已完成被接受的 status/metadata transition；
3. final `acceptance-receipt.json --require-accepted` 通过；
4. receipt 绑定 exact post-transition package、review baseline、H0 commits 和 P1 source lock；
5. `./gradlew check` 通过。

Gate B PASS 是 implementation authorization：H1 可开始，H2-H6 按前置 slice 完成情况逐步开放；
带 exact attestation 的 disposable local integration/recovery/fault environment 也可创建、重置、
销毁和重建。G0 `DataResetAssessment` 不再是 H1 前置。

Gate C 是 environment-specific deployment / upgrade safety gate。任何 existing、staging、production
或 unknown environment 在没有 exact Gate C PASS 时都不能执行 destructive/migration 操作、进入
SHADOW 或 ENABLED；unknown 即使携带其他环境的 Gate C authority 也必须 fail-closed。

## 被替代的约束

本提案替代：

- 后续重大改进继续分配 `NDP-NNNN` 并放入 `docs/proposals/`；
- 仅靠提案 Markdown 内部 Status 行表达接受状态；
- 自动 gate 只检查 NDP-0001 而不绑定后续提案 exact bytes。

NDP-0001 的持续设计演进、兼容策略和无项目版本线规则不被替代。

## 数据、协议与兼容策略

选择：**直接替换**。

本提案只改变 repository governance metadata 和校验入口，不改变 runtime bytes、持久数据、外部
协议或运行资源。NDP-0001 与本提案保留为历史记录，不建立两套同时活跃的 proposal authority。

## 安全、故障与运维影响

- fail-closed：任何缺失、未知、重复、路径漂移、hash mismatch 或 pending decision 都不能通过
  accepted verification；
- candidate 永远不产生 authority；
- final receipt 授权 H1-H6 implementation，但不授权 persistent environment mutation、SHADOW 或
  ENABLED；
- Git 操作、文档状态或文件重命名不能替代维护者决定；
- 本治理变更不接触密钥、Broker、Oxia、Store、Object 或 Worker。

## 实施切片

1. Draft 阶段修正 NDIP-1 path 定义并准备 verifier/candidate；
2. 实现 read-only G0 Assessment tooling，并与 H6 Manifest 分权；
3. 维护者审查 NDP-0002、NDIP-1 exact diff 和 candidate digest；
4. 维护者明确接受后执行 status transition、治理索引/gate 更新和 final receipt；
5. Gate B PASS 后按 H1-H6 依赖顺序实施，并允许 exact disposable local environment tests；
6. H1-H6/local tests 完成后，对第一个真实 persistent deployment 执行 G0；
7. Gate C PASS 后才允许该 exact environment 进入 SHADOW，满足 SHADOW requirements 后才允许
   ENABLED。

## 验证与发布 gate

- candidate positive verification PASS，输出 `authority=false / gate_b=PENDING`；
- candidate 使用 `--require-accepted` 必须失败；
- 任一 file hash、package hash、path、顺序、status 或 decision 漂移必须失败；
- `checkDocumentation` 固定 NDP-0002 Accepted 边界并运行 final accepted verifier；
- `checkProjectVersionMarkers`、`./gradlew check` 通过；
- Accepted 状态不得继续保留可被误用为当前 authority 的 candidate receipt。

## 回滚

Accepted 后若 NDIP 治理需要修改，使用新的 NDIP supersede；不恢复并行 proposal authority，
不删除历史 NDP。H0 runtime 不依赖治理 receipt，治理回滚不得开放 native physical path。

2026-09-02，维护者按本条接受 `NDIP-2`。它不改变本提案的 Gate B / Gate C
分权、外置 acceptance receipt 或 fail-closed 边界；仅把 proposal `Implemented` 与后续
target deployment、performance/scale 及 production rollout 分开，并引入 scoped runtime-source
authority。

## 未决问题

- 当前尚无真实 persistent deployment，因此 G0 environment run 为
  `NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT`，不得生成虚假 scope、placeholder PASS
  或 assessment receipt。该状态不阻塞 H1-H6，但严格阻塞 SHADOW/ENABLED。

## 权威文档同步清单

- [x] proposal 历史索引与 NDIP 治理索引
- [x] 自动化 gate、closed verifier 与 final receipt 入口
- [x] 会话执行提示词不作为 repository artifact 的约束
- [x] runtime 主设计、Protocol Registry、ADR、Implementation Status、Design Audit 与
  Operations Runbook：当前不适用；本次只改变实现/部署门的阶段关系，不开放 runtime native
  capability
