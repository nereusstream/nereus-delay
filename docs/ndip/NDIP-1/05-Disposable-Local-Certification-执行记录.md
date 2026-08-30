# NDIP-1 Disposable Local Certification 执行记录

> 本文件是非规范的实施/运维记录。它保存历史基线并定义当前 receipt 的读取方式，不能作为
> DataResetAssessment scope、Gate C authority、SHADOW 或 ENABLED 的输入凭证。

## 独立审查后的当前认证契约

独立生产路径审查在 `main@913a2a2e` 关闭了 Attempt Journal response-loss、canonical
record/evidence、默认 coordinator 到 P1 prepared-record sender，以及 physical activation
lifecycle 的缺口。`main@da15290e` 的 24/24 receipt 因而只保留为历史基线，不能认证这些后续
修改。

当前 runner 生成 `receiptSchemaGeneration=3`，closed supporting checks 保持三个：

- `p1.compileRealPulsar`；
- `p1.h0`；
- `p1.nativeCoordinator`。

最后一项必须在真实 source-locked P1 Broker 上经过
`PreparedSubmission -> RouteBoundSubmissionTransportPlanResolver -> DefaultSubmissionCoordinator ->
ProductionPulsarSendTransport -> PulsarClientArtifactRecordEncoder`，验证 `.deliverAt(...)`、exact
record evidence 和 Shared strict 模式的 early receive rejection。直接调用
`sendPreparedRecord` 的 fixture 或 matrix cell 不能替代它。

持久化认证使用非系统临时目录：

```bash
NEREUS_DELAY_DISPOSABLE_ARTIFACT_DIR=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-artifacts/ndip1-final \
NEREUS_DELAY_DISPOSABLE_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-disposable-local-certification.sh
```

runner 会在上述 base 下创建带 UTC 时间的不可混用子目录。哪个 receipt 属于当前 HEAD，必须由
`scripts/verify-disposable-local-certification.py` 对 exact source binding、三项 supporting
checks、24 个 matrix cell 和 cleanup 全部验证后确定，不能从文件名或本文中的历史 SHA 推断。
generation 3 为每个 supporting check 和 matrix cell 增加 `logSha256`；verifier 同时复算
log 与 evidence digest，因此 receipt 生成后修改任一原始日志都会 fail-closed。Persistent staging
入口只接受 exact candidate 的 generation-3、24/24、non-authoritative receipt。

`recovery.oxia_restart_reopen` 不再把 Oxia 进程 health 当作 data-plane 恢复。runner 分离保存
Gradle test output 与 stop/start control output，在 data-server-1 重启后先用 source-locked、clean
Oxia CLI 完成真实 namespace list，再等待 20 秒 Route session expiry grace（超过测试固定的
15 秒 session timeout），最后才释放 refresh/reopen gate。stop/start、data-plane read、grace 或
focused no-skip proof 任一缺失，cell 都保持 `EXECUTED_FAIL`。receipt 的 source branch 还直接
绑定该 CLI 与其 `go version -m` build-info digest，并验证 exact Oxia revision 与
`vcs.modified=false`。

## 历史 generation-1 PASS

2026-08-28 在 `main@da15290e` 的 source-bound 运行曾经通过：

- receipt status：`PASS`；
- matrix：24 个 closed cell，`EXECUTED_PASS=24`、`EXECUTED_FAIL=0`、
  `NOT_COVERED=0`、`skipped=0`；
- supporting checks：P1 `compileRealPulsar` 与 H0 no-producer-touch smoke 均为 `PASS`；
- cleanup：`PASS`，本次生成的 container、image、network、process、credential、topic
  和 volume 剩余集合全部为空；
- authority：`false`；Gate C：`false`；SHADOW：`false`；ENABLED：`false`。

这关闭了 NDIP-1 当前定义的 disposable-local native/recovery/fault 矩阵，但不是
production deployment 或 release certification。Gate C 仍为 `PENDING_DEPLOYMENT`。

## 边界

`DISPOSABLE_LOCAL` 只能创建本次独占、可销毁的 synthetic 测试资源。它不创建真实
`DataResetAssessment` scope，也不改变当前 G0 `NOT_APPLICABLE_FOR_IMPLEMENTATION /
PENDING_DEPLOYMENT` 结论。未来 Gate C 验证必须在明确分类为 `EXISTING`、
`STAGING` 或 `PRODUCTION` 的持久化环境中执行真实 G0；不得重命名本地环境复用本记录。

## 运行入口与 source binding

- 入口：`e2e/run-disposable-local-certification.sh`
- 运行命令：

  ```bash
  NEREUS_DELAY_DISPOSABLE_GRADLE_USER_HOME=/Users/liusinan/.gradle \
    bash e2e/run-disposable-local-certification.sh
  ```

- Delay source：`main@da15290e47b9255403c92e4ebba3c7d5189edb75`
- P1 source lock：`nereus/delay-resource-guard@0a2536484cd3932801a98dc88ff112b2df88a1c7`
- Oxia source：`main@37a17bef17202d5fd6e232da5fd26d94865484`
- Accepted NDIP-1 package digest：
  `13caab8ecdc201901f06e905f1c0bf9792780e50c6f5948f93abf2bdb8f4d21b`
- Compose/config digest：`188632d6a2709c000bb8fe60dfd905fd1ced547279c092cf11e728ff2c11c809`
- Disposable attestation digest：`158998156ab6dd46963c11ea464b9a581268f4dd87e9fec3180e386be4b5372d`
- P1 distribution digest：`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`
- Receipt digest：`e8dfd5feef88afdfdbebd89b2118ae214833e318c50d2377adbc84a1890b5e61`

运行时间为 `2026-08-28T06:30:17Z` 至 `2026-08-28T06:39:58Z`，耗时 9 分 41 秒。
独占 Compose project 为 `nereus-delay-ndip1-20260828063011-16651-13126`，resource
prefix 为 `nereus-delay-ndip1-20260828063011-16651-13126-resource`。

完整 artifact 位于：

```text
/var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/nereus-delay-ndip1-cert.XXXXXX.e3ypGoUrmz/
```

receipt：

```text
/var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/nereus-delay-ndip1-cert.XXXXXX.e3ypGoUrmz/disposable-local-certification-receipt.json
```

独立验证命令：

```bash
python3 -B scripts/verify-disposable-local-certification.py \
  --receipt /var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/nereus-delay-ndip1-cert.XXXXXX.e3ypGoUrmz/disposable-local-certification-receipt.json
```

历史 verifier 以退出码 0 验证了当时 generation-1 的 closed schema、exact Delay/P1/Oxia source、
Accepted package、Compose config、attestation、evidence digest 和 cleanup；历史 generation-1
未直接在 receipt 中绑定每个 log digest。由于 receipt 故意绑定
exact Delay commit，在后续仅文档提交上复核该历史证据时，应 checkout 上述 source commit；
不得手改 receipt 伪造当前 HEAD binding。

## 环境与资源边界

环境实际包含：

- 两个 source-locked P1 Pulsar Broker、ZooKeeper metadata 和可恢复 BookKeeper；
- 三个 Oxia coordinator、三个 data server，`default` namespace，replication factor 3；
- 真实 MinIO、versioning 和 immutable checkpoint 路径；
- 隔离的 Command、Evidence、Business topic 及每个矩阵单元的测试 topic；
- RocksDB reopen/retention focused path；
- `worker-a`、`worker-b` 的 ownership 边界，以两个真实 Oxia session 验证 owner
  epoch 转移；
- 真实 Broker-1 stop、Broker-2 resume 与 Broker-1 rejoin。

清理只作用于本次 exact Compose project、resource prefix、临时进程、临时凭证和生成
image，没有使用 global Docker prune，也没有修改其他容器、卷或工作树。

## 矩阵结果

### Pulsar native behavior（10/10）

| 单元格 | 结果 | 记录边界 |
| --- | --- | --- |
| `native.shared.strict` | `EXECUTED_PASS` | strict 在 `deliverAt` 前拒绝投递 |
| `native.shared.non_strict` | `EXECUTED_PASS` | non-strict 按 Pulsar tick 精度记录风险 |
| `native.shared.disabled` | `EXECUTED_PASS` | disabled 允许立即投递 |
| `native.key_shared.strict` | `EXECUTED_PASS` | strict 在 `deliverAt` 前拒绝投递 |
| `native.key_shared.non_strict` | `EXECUTED_PASS` | non-strict 按 Pulsar tick 精度记录风险 |
| `native.key_shared.disabled` | `EXECUTED_PASS` | disabled 允许立即投递 |
| `native.exclusive.immediate` | `EXECUTED_PASS` | 持久化后立即可消费是原生预期 |
| `native.failover.immediate` | `EXECUTED_PASS` | 持久化后立即可消费是原生预期 |
| `native.shared.ttl_expiry` | `EXECUTED_PASS` | TTL=1s 且显式 expiry 可在 `deliverAt` 前删除 delayed message |
| `native.shared.retention_zero` | `EXECUTED_PASS` | retention=0 在 ACK、rollover 和 trim 后移除目标 ledger |

native evidence 使用 closed `nereus-delay.disposable-local.native-cell-evidence-r2`，绑定
business `deliverAt`、payload、sequenceId、key、ordering key、九项 reserved properties、event
time、prepared hash、target/partition 及 SEND/ACK hashes。TTL/retention 的 PASS 表示风险已被
真实观测，不是 not-before 保证或 native capability 授权。

### Recovery、fault 与真实服务（14/14）

| 单元格 | 结果 |
| --- | --- |
| `recovery.candidate_claim` | `EXECUTED_PASS` |
| `recovery.admission` | `EXECUTED_PASS` |
| `recovery.journal_mapping` | `EXECUTED_PASS` |
| `recovery.response_loss_after_send_async_before_ack` | `EXECUTED_PASS` |
| `recovery.response_loss_after_ack_before_outcome` | `EXECUTED_PASS` |
| `recovery.response_loss_after_outcome_before_handoff` | `EXECUTED_PASS` |
| `recovery.response_loss_handed_off_before_checkpoint` | `EXECUTED_PASS` |
| `recovery.worker_ownership_transfer` | `EXECUTED_PASS` |
| `recovery.broker_restart_failover` | `EXECUTED_PASS` |
| `recovery.oxia_restart_reopen` | `EXECUTED_PASS` |
| `recovery.oxia_minio_checkpoint` | `EXECUTED_PASS` |
| `recovery.oxia_minio_reaping` | `EXECUTED_PASS` |
| `recovery.minio_idempotent_restore` | `EXECUTED_PASS` |
| `recovery.rocksdb_reopen_retention` | `EXECUTED_PASS` |

Admission fresh-process cell 明确证明：未解决的 `UNCERTAIN` 会阻止 clean drain，保留 lease/
Store/state 以便后续解析，不伪造 final checkpoint 或 lease release。Outcome-before-handoff
cell 使用两个真实 Worker JVM 和 SIGKILL，证明已持久化的 definitive
`PUBLISH_OUTCOME` 可由 replacement Worker 应用，且不产生第二次 SEND。

## 历史阻断运行

早期 `main@35986a08462bd5facbd9be6f3d28f06080115745` receipt 为 `BLOCKED`：22 个
cell 中 21 个 `EXECUTED_PASS`，`recovery.response_loss_after_outcome_before_handoff`
为 `NOT_COVERED`。后续代码增加了可控、真实的切点与 replacement-Worker 证据，再加上
TTL/retention 两个原生风险 cell，形成当前 24/24 PASS。历史 receipt 不得替代当前
source-bound receipt。

## 清理结果

清理结果为 `PASS`，且 `composeProjectAbsent=true`。以下审计集合全部为空：

```text
containersRemaining=[]
imagesRemaining=[]
networksRemaining=[]
processesRemaining=[]
temporaryCredentialsRemaining=[]
topicsRemaining=[]
volumesRemaining=[]
```

该结果只证明本次 disposable 资源已清理，不证明任何 persistent deployment readiness。

## 后续 persistent 输入

本次 PASS 不能替代 persistent G0。未来需要先明确 `EXISTING`、`STAGING` 或
`PRODUCTION` environment identity、scope、tenant/route/shard、Worker exact set、resource
inventory、外部保留义务和可信时间区间，再执行真实 G0 并生成独立 Gate C 审查输入。
Accepted package receipt 中的 Gate C 始终为 `PENDING_DEPLOYMENT`。固定本机 staging 是否已有
候选级 Gate C/SHADOW/canary 证据，应按
[`06-Persistent-Staging-Gate-C-SHADOW-执行记录.md`](06-Persistent-Staging-Gate-C-SHADOW-执行记录.md)
中的 `deployment/current.json` 与独立 verifier 读取；该证据也不能授予其他环境或 production。
