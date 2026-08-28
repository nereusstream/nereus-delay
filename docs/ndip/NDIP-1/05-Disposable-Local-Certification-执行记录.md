# NDIP-1 Disposable Local Certification 执行记录

> 本文件是非规范的实施/运维记录。它只记录一次 `DISPOSABLE_LOCAL` 本地认证运行，不能
> 作为 DataResetAssessment scope、Gate C authority、SHADOW 或 ENABLED 的输入凭证。

## 边界

`DISPOSABLE_LOCAL` 只能创建本次独占、可销毁的测试资源。它不创建真实
`DataResetAssessment` scope，也不改变当前 G0 `NOT_APPLICABLE_FOR_IMPLEMENTATION /
PENDING_DEPLOYMENT` 结论。任何未来的 Gate C 验证都必须在明确分类为 `EXISTING` 或 `STAGING`
的持久化环境中重新执行真实 G0；不能重命名本地环境复用本记录。

本记录中的产物只称为 local disposable certification receipt/report。运行时 authority、Gate C、
SHADOW 和 ENABLED 均保持关闭。

## 运行入口与 source binding

- 入口：`e2e/run-disposable-local-certification.sh`
- 运行命令：

  ```bash
  NEREUS_DELAY_DISPOSABLE_GRADLE_USER_HOME=/Users/liusinan/.gradle \
    bash e2e/run-disposable-local-certification.sh
  ```

- 认证绑定的 Delay source：`main@35986a08462bd5facbd9be6f3d28f06080115745`
- P1 source lock：`nereus/delay-resource-guard@0a2536484cd3932801a98dc88ff112b2df88a1c7`
- Oxia source：`main@37a17bef17202d5fd6e232da5fd26d94865484`
- Accepted NDIP-1 package digest：
  `13caab8ecdc201901f06e905f1c0bf9792780e50c6f5948f93abf2bdb8f4d21b`
- Compose/config digest：`c3da78dba615f8fa14a7b2d58f52761bdb0eb4e268cebc2676e4dc0247ffa8bd`
- Disposable attestation digest：`fd2a331299da9724b4202444f598921b0ff05c28c80a87a2bac58742dd7e143c`
- MinIO image：`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`
- MinIO repository digest：`14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`

运行时间为 `2026-08-28T02:40:52Z` 至 `2026-08-28T02:47:04Z`，耗时 6 分 12 秒。
本次本地 receipt 状态为 `BLOCKED`：这是唯一安全的 `NOT_COVERED` 单元格导致的汇总状态，
不是已执行单元格失败。

完整 artifact 位于：

```text
/var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/nereus-delay-ndip1-cert.XXXXXX.LcUObXDn7T/
```

receipt：

```text
/var/folders/vk/l_r0z80j1dj93fsrjx3zqv4r0000gn/T/nereus-delay-ndip1-cert.XXXXXX.LcUObXDn7T/disposable-local-certification-receipt.json
```

receipt verifier 以退出码 0 验证了 closed receipt、source/package/config/attestation 绑定、
每个命令/结果/证据路径和清理结果。

## 后续候选变更

`main@62cb5e322edbc98e9a97c0d15dc017b06cdf5fd7` 已把 source-locked P1 Attempt Journal、固定
Producer/sequence 身份、current prepared record、ownership marker 与 RocksDB projection 接入
真实 Worker 路径。fresh-process owner replacement 现在只能退休旧 owner 的 pre-ownership
mapping，并以 `UNCERTAIN` 进入 evidence resolution；测试明确断言 replacement owner 对目标
Topic 的 SEND 为 0。

本文件记录的 receipt 绑定 `main@35986a08`，因此不会被追认成上述候选变更的认证凭证。必须在
干净且与远端同步的最新 `main` 上重新执行完整 runner、生成新的 closed receipt 并重新验证，
才能更新当前 certification 结论。Gate C、SHADOW 与 ENABLED 不因候选代码或历史 receipt 改变。

## 环境与资源边界

本次独占 Compose project 为
`nereus-delay-ndip1-20260828024047-50462-27485`，resource prefix 为
`nereus-delay-ndip1-20260828024047-50462-27485-resource`。环境实际包含：

- 两个 source-locked P1 Pulsar Broker、ZooKeeper metadata 和可恢复 BookKeeper；
- 三个 Oxia coordinator、三个 data server，`default` namespace，replication factor 3；
- 真实 MinIO、versioning 和 immutable checkpoint 路径；
- 隔离的 Command、Evidence、Business topic，以及每个矩阵单元自己的测试 topic；
- RocksDB reopen/retention focused path；
- `worker-a`、`worker-b` 的 ownership 边界声明，并以两个真实 Oxia session 验证 owner epoch
  转移。Broker failover 使用真实 Worker smoke 在 broker-1 停止后通过 broker-2 恢复，并验证
  broker-1 rejoin。

环境使用唯一 project/resource 前缀。清理只对本次 Compose project、生成 topic、卷、网络、
临时进程、临时凭证和生成镜像执行，没有使用 global Docker prune，也没有修改其他容器、卷或
工作树。

## 矩阵结果

矩阵固定为 22 个单元格，`skipped=0`：21 个 `EXECUTED_PASS`，0 个 `EXECUTED_FAIL`，1 个
`NOT_COVERED`。

### Pulsar native behavior

| 单元格 | 结果 | 记录边界 |
| --- | --- | --- |
| `native.shared.strict` | `EXECUTED_PASS` | strict 在相应 `deliverAt` 前拒绝投递 |
| `native.shared.non_strict` | `EXECUTED_PASS` | non-strict 按 Pulsar tick 精度记录风险 |
| `native.shared.disabled` | `EXECUTED_PASS` | disabled 可立即投递 |
| `native.key_shared.strict` | `EXECUTED_PASS` | strict 在相应 `deliverAt` 前拒绝投递 |
| `native.key_shared.non_strict` | `EXECUTED_PASS` | non-strict 按 Pulsar tick 精度记录风险 |
| `native.key_shared.disabled` | `EXECUTED_PASS` | disabled 可立即投递 |
| `native.exclusive.immediate` | `EXECUTED_PASS` | 持久化后立即可消费是 Pulsar 原生预期 |
| `native.failover.immediate` | `EXECUTED_PASS` | 持久化后立即可消费是 Pulsar 原生预期 |

每个 native evidence 均绑定业务 `deliverAt`、payload、sequenceId、key、ordering key、九项
reserved properties、event time、prepared hash、target/partition 及 SEND/ACK evidence hashes。
本记录不从 native 结果推出跨 Subscription 的统一 not-before 保证。

### Recovery、fault 与真实服务

| 单元格 | 结果 |
| --- | --- |
| `recovery.candidate_claim` | `EXECUTED_PASS` |
| `recovery.admission` | `EXECUTED_PASS` |
| `recovery.journal_mapping` | `EXECUTED_PASS` |
| `recovery.response_loss_after_send_async_before_ack` | `EXECUTED_PASS` |
| `recovery.response_loss_after_ack_before_outcome` | `EXECUTED_PASS` |
| `recovery.response_loss_handed_off_before_checkpoint` | `EXECUTED_PASS` |
| `recovery.worker_ownership_transfer` | `EXECUTED_PASS` |
| `recovery.broker_restart_failover` | `EXECUTED_PASS` |
| `recovery.oxia_restart_reopen` | `EXECUTED_PASS` |
| `recovery.oxia_minio_checkpoint` | `EXECUTED_PASS` |
| `recovery.oxia_minio_reaping` | `EXECUTED_PASS` |
| `recovery.minio_idempotent_restore` | `EXECUTED_PASS` |
| `recovery.rocksdb_reopen_retention` | `EXECUTED_PASS` |
| `recovery.response_loss_after_outcome_before_handoff` | `NOT_COVERED` |

`recovery.response_loss_after_outcome_before_handoff` 没有当前源码中可安全、独立控制的真实
故障切点，因此没有用 mock、条件 skip 或改名的 disposable 运行伪造 PASS。它是本次 receipt
唯一的阻断覆盖项。

P1 supporting checks 同样通过：`p1.compileRealPulsar` 和 `p1.h0` 均为 `PASS`。对应日志、
每个 cell 的 evidence JSON 和 Broker failover 的
`recovery/broker-state/before-process-crash.json`、`after-fresh-process.json` 均保留在上述
artifact 目录。

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

方法是 exact Compose project `down --volumes --remove-orphans --rmi local` 加 exact
resource-prefix audit；没有 broad prune。该结果只证明本次 disposable 资源已清理，不证明任何
persistent deployment readiness。

## 后续 persistent 输入

本次结果不能替代 persistent G0。未来需要先明确 `EXISTING` 或 `STAGING` environment identity、
scope、tenant/route/shard、Worker exact set、资源 inventory、外部保留义务和可信时间区间，再
执行真实 G0，生成独立的部署决策输入并进入 Gate C 审查。当前 Gate C 仍为
`PENDING_DEPLOYMENT`，SHADOW/ENABLED 继续 fail-closed。
