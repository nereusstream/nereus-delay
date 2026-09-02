# G0 DataResetAssessment 执行状态

## 当前结论

- Gate B：`PASS`；H1–H6 implementation 与 exact disposable testing 已获授权。
- implementation-only 环境：G0 为
  `NOT_APPLICABLE_FOR_IMPLEMENTATION / PENDING_DEPLOYMENT`，不得伪造 Assessment receipt。
- G0 tooling：已实现 closed scope、逐资源只读 collector、evaluator、canonical receipt、可信时间、
  fail-closed validation 和本地 `CREATE_NEW` writer。
- persistent staging：状态不是本文中的静态常量，而由
  `nereus-delay-staging/local-docker-staging-ndip1/deployment/current.json` 指向的 immutable final
  summary 及独立 verifier 解析。
- production：没有 authority；任何 existing/staging/production/unknown 环境都不能借用另一个
  环境的 Assessment、Manifest 或 Gate C receipt。

Gate C 是 deployment/upgrade safety gate，不是 H1–H6 implementation gate。没有 persistent
environment 时可以开发并在 exact `DISPOSABLE_LOCAL` attestation 下运行 synthetic 集成、恢复与
故障测试；一旦要接触已有、非 disposable 状态，必须先完成本环境自己的 G0 与 Gate C。

## Assessment、数据处置与 Manifest 的分工

三类材料不能互相替代：

1. `DataResetAssessment` 只读观察已有环境，回答已有状态是可直接 reset、可兼容 retain、必须
   migrate，还是证据不完整。
2. `DataDispositionDeclaration` 是 operator 对该 exact previous/candidate deployment 的显式决定。
   runner 只接受：
   - existing deployment：`RESET_INTERNAL_ONLY`；
   - 首次创建：`CREATE_NEW_INTERNAL_ONLY`。
   两者都要求 operator identity 非空、`externalUserData=false`，并签名绑定 previous/candidate
   scope、资源 incarnation、trusted time 和 candidate source。runner 不会从 `STAGING` 分类或
   Assessment outcome 自动推断可以 reset。
3. `DataResetManifest` 是实际 mutation/activation 前的 cutover 证明。它必须绑定 fresh resource、
   actual zero `PUBLISHING / UNCERTAIN` obligation、exact Worker generation、source/package/P1 lock
   和签名数据处置。Manifest operation 后必须对全部 closed resources 做 readback，不能只证明
   “计划创建”。

因此：

```text
Assessment PASS_DIRECT_REPLACE
+ signed RESET_INTERNAL_ONLY declaration
+ no external user data
+ 13/13 Manifest operation readback
    -> RESET resolution candidate for Gate C

Assessment PASS_RETAIN
    -> RETAIN candidate；仍须证明 schema/state compatibility

Assessment MIGRATION_REQUIRED
    -> 不得构造 MIGRATED authority，直到一次性迁移另行完成并证明

Assessment INCOMPLETE
    -> no Gate C authority
```

## Closed scope 与 13 类资源

G0 必须枚举 tenant、route、shard、eligible Worker closed exact set，以及以下 13 类资源；每类
恰好由一个只读 adapter 读取并绑定 identity，缺少、重复或 substitution 均拒绝：

1. Command Topic；
2. System Topic；
3. RocksDB Store；
4. Checkpoint Catalog；
5. Profile/Oxia state；
6. runtime policy；
7. payload reservation/Object Store；
8. Pulsar Attempt Journal；
9. evidence topic/cursor；
10. query/dedupe state；
11. `PUBLISHING / UNCERTAIN` obligation index；
12. resource incarnation registry；
13. Worker registry。

当前 persistent runner 对真实 Pulsar、Oxia、MinIO 和 RocksDB 做只读 inventory：

- Pulsar topic 若仍有 active publisher/consumer，G0 拒绝；
- RocksDB inventory 固定文件集合与 digest，并拒绝 symlink；
- MinIO 只允许 exact bucket/prefix；
- Oxia 读取 exact key/value，并在 obligation index 中拒绝未解决状态；
- 每条 observation 都绑定 signed data-disposition envelope；
- 若已有 current deployment，runner 先用固定外部 staging trust key 验证前一代终态、policy、
  rollback 和 final summary，不能信任目录内自声明的公钥。

## Lifecycle safety guard

`DeploymentSafetyGate` 保持三个 closed API：

1. `implementation(...)` 只读取 Gate B 与 H1–H6 predecessor，不读取 Gate C。
2. `localDisposable(...)` 只接受 classification=`DISPOSABLE_LOCAL`，并要求 exact environmentId、
   isolated scope、synthetic-only、exclusive ownership、cleanup authorization 和 evidence digest。
3. `deployment(...)` 对 `UNKNOWN` 无条件拒绝；existing/staging/production 必须验证 exact
   environment/classification/scope/Assessment/Gate C binding。Gate C PASS 只开放 SHADOW；ENABLED
   还要求 SHADOW requirements PASS、签名 Manifest、current policy lease 和 source generation barrier。

缺少 Gate C 不阻塞编译、H1–H6 实现或 disposable 测试，但必须阻塞 persistent mutation、SHADOW、
ENABLED 和 destructive/migration 操作。

## Fail-closed 条件

以下任一条件都不能产生 Gate C authority：

- environment 为 `DISPOSABLE_LOCAL` 或 `UNKNOWN`；
- persistent environment 没有 closed exact scope；
- trusted-time evidence 不 qualified；
- resource/Worker expected set 与 observed set 不相等；
- 数据处置缺失、未签名、operator 为空、decision 不匹配或 `externalUserData=true`；
- external retention、replacement disposition 或 migration requirement 未闭环；
- 任一 evidence digest 缺失、长度错误、全零或签名 trust root 不匹配；
- 任一 `PUBLISHING / UNCERTAIN` obligation 未解决；
- Manifest operation/readback 不是 exact 13/13；
- previous final authority 无法验证，或新 resource identity 与旧 generation 重叠；
- current Oxia policy head generation 不单调、scope 不匹配、lease 过期或已 `DISABLED`。

runner 不执行隐式 destructive operation，也不通过手改 digest、placeholder PASS、fixture-only
结果或 skipped check 越过 Gate C。

## Persistent staging 输入与当前状态解析

入口必须显式提供与最终 HEAD 绑定的 disposable receipt 及 digest，以及 operator 数据处置：

```bash
NEREUS_DELAY_DISPOSABLE_RECEIPT=/absolute/path/to/disposable-local-certification-receipt.json \
NEREUS_DELAY_DISPOSABLE_RECEIPT_SHA256=<sha256> \
NEREUS_DELAY_STAGING_DATA_DECISION=RESET_INTERNAL_ONLY \
NEREUS_DELAY_STAGING_OPERATOR=<operator> \
NEREUS_DELAY_STAGING_EXTERNAL_USER_DATA=false \
NEREUS_DELAY_STAGING_ROOT=/absolute/path/to/local-docker-staging-ndip1 \
  bash e2e/run-ndip1-persistent-staging.sh
```

首次创建环境时，decision 改为 `CREATE_NEW_INTERNAL_ONLY`。字段缺失或与 runner 观察到的
`EXISTING / CREATE_NEW` 分类不一致时立即失败。

最新可用结果只从 `${NEREUS_DELAY_STAGING_ROOT}/deployment/current.json` 读取。该指针只有在整个
run 完成、独立 validator PASS、rollback 回到 `DISABLED` 后才写入；它绑定 immutable final summary
及 scope digests。若 pointer 缺失、签名/digest 无效、final state 不是 `DISABLED`，或
`NDIP_RUNTIME_SOURCE` 无法证明当前 checkout 与 summary candidate 具有相同 runtime
source digest，则该 runtime source 的 Gate C/canary 状态仍为 pending/blocked。README、执行记录和
receipt 变化不进入该 runtime digest。

历史 run `20260830035421-73816` 绑定旧候选且只有 AUTO_FAST 单路径 canary，仅保留为历史证据，
不得替代当前 Worker/Managed Handoff 链路认证。

## 验证边界

独立验证器 `e2e/validate-ndip1-persistent-certification.py` 不复用 runner 的签名判断；它重新计算
domain-separated Ed25519 envelope 和 binary Manifest 签名、固定 trust root、source/package/P1
binding、13-resource G0/Manifest、41 行 skip audit、raw canary evidence path/digest、Gate C 41/41、
SHADOW `0/0/0`、单调 policy head、AUTO_FAST `1/1/0`、Managed Handoff `1/1/1`、Attempt Journal
mapping/ownership/published chain 及同 subscription 重启完整回放、response-loss resolution 和 rollback
`DISABLED`。

通过本机 staging 认证仍只表示 `productionAuthority=false` 的环境级证据。维护者已在
2026-09-02 将它作为 NDIP-1 implementation closure evidence 的一部分，因此 NDIP-1 当前为
`Implemented`；这不改变该证据的环境级身份，也不提供 production deployment、release、容量、
长期 soak 或跨环境 authority。
