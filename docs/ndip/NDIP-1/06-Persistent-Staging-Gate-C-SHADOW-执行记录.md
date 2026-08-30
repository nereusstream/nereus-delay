# NDIP-1 Persistent Staging Gate C / SHADOW 执行记录

> 本文件是非规范的 staging 执行与运维记录。它不修改 Accepted `01`–`04`，不改变
> `acceptance-receipt.json` 的 digest，也不提供 production authority。所有路径和 receipt
> 均属于本机固定 staging；不要复制到其他环境。

## Findings-first 结论

最终状态：`PASS` for the exact staging canary, then `DISABLED`。

- Gate C：`PASS`，`41/41` 个适用检查通过，`skipped=0`、`notExecuted=0`；
- SHADOW：`PASS`，205 秒观察，native Admission/send/HANDED_OFF 均为 `0`；
- ENABLED canary：`PASS`，单一 profile、单一 topic、单条记录，native Admission/send 为 `1/1`，
  `HANDED_OFF=0`；
- rollback：`PASS`，DISABLED activation 被拒绝，active lease/send 均为 `0`，native/Worker
  bounded process 均已退出；
- authority：仅为 `STAGING` evidence，`productionAuthority=false`；不等价于 production
  readiness、release certification 或跨环境升级授权。

历史 blocked run 没有被覆盖或删除，均保留在同一持久化 evidence root。当前正式闭环 run 为
`20260830035421-73816`；run status 为 `COMPLETED`、exit code 为 `0`。

此前因真实 fault 结果未被合法标注、外部 M3 资源竞争或观察边界被打断而 blocked 的 run
（`20260830021531-33433`、`20260830024005-53163`、`20260830030035-1173`、
`20260830031214-44311`、`20260830033042-55318`、`20260830033115-55772`）均未被当作
authority；它们的原始日志和状态仍保留。最终 run 对预期的 local-storage SIGKILL 使用机器绑定的
`EXPECTED_TERMINATION` marker 与 fresh-process recovery evidence，才将该条件计入 Gate C 的
`41/41 PASS`，没有把 fault/skip 伪装成正常测试 PASS。

## 固定边界与输入

| 项目 | 当前值 |
|---|---|
| environmentId | `local-docker-staging-ndip1` |
| classification | `STAGING` |
| persistent root | `/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1` |
| evidence run | `20260830035421-73816` |
| candidate commit | `ed3cc4987dab6ebf179f6bfafcfd159c2e54188e` |
| frozen entry baseline | `61d1dbf196834f9667f860b813e021cc1b998d96` |
| Accepted NDIP-1 package digest | `13caab8ecdc201901f06e905f1c0bf9792780e50c6f5948f93abf2bdb8f4d21b` |
| P1 source lock | `0a2536484cd3932801a98dc88ff112b2df88a1c7` (`nereus/delay-resource-guard`) |
| operator | `operator:local-ndip1` |
| signing key generation | `1` |
| final policy generation | `5` |

执行入口：

```bash
NEREUS_DELAY_STAGING_ROOT=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1 \
  bash e2e/run-ndip1-persistent-staging.sh
```

runner 固定使用 non-`/tmp` 的 root，拒绝复用已有 run directory，使用 `CREATE_NEW`/签名 envelope
链，并且不执行 `docker compose down` 或 global cleanup。凭证只由该本地 run 使用，没有写入 Git。

## 持久化拓扑与 exact identity

Docker Compose project 为 `ndip1-local-docker-staging`，资源使用上述 root 下的 bind mounts；
完成后 infrastructure 仍运行，证据与 source overlay 仍保留。拓扑为：

- Oxia：`default` namespace；3 个 coordinator（`16691`、`16692`、`16693`）和 3 个 data
  server（`16681`、`16682`、`16683`），真实 persistent Raft/WAL/DB；
- MinIO：真实 persistent object store，bucket `nereus-delay-ndip1-staging`，run prefix
  `ndip1-local-docker-staging/20260830035421-73816`；
- Pulsar P1：两个 source-locked Broker（web `21962`/`21964`，broker `21961`/`21963`）、
  ZooKeeper 和 BookKeeper，image tag 绑定 P1 lock；
- Gateway/Worker：通过真实 source-bound Gateway/Worker JVM smoke path 执行，覆盖 fresh process、
  owner transfer、response loss 和 broker recovery；测试进程结束后没有残留。

当前 run 固定的 topic/resource identity：

| 类型 | identity |
|---|---|
| Command | `persistent://public/default/ndip1-command-20260830035421-73816` |
| System | `persistent://public/default/ndip1-system-20260830035421-73816` |
| Mutation | `persistent://public/default/ndip1-mutation-20260830035421-73816` |
| Worker | `persistent://public/default/ndip1-worker-20260830035421-73816` |
| Evidence | `persistent://public/default/ndip1-evidence-20260830035421-73816`，cursor subscription `ndip1` |
| Attempt Journal | `persistent://public/default/ndip1-worker-destination-20260830035421-73816-attempt-journal` |
| Native canary | `persistent://public/default/ndip1-native-20260830035421-73816` |
| Oxia profile/policy | `oxia://default/ndip1-local-docker-staging/20260830035421-73816/{profile-state,runtime-policy}` |
| Oxia recovery/indexes | same prefix with `checkpoint-catalog`, `query-dedupe`, `obligation-index`, `resource-incarnation`, `worker-registry` |
| MinIO payload reservation | `s3://nereus-delay-ndip1-staging/ndip1-local-docker-staging/20260830035421-73816/payload-reservation` |
| RocksDB | `.../evidence/20260830035421-73816/worker-store/rocksdb` |
| Workers | `worker-ndip1-a`, `worker-ndip1-b` |
| route/shard/tenant | `route-ndip1` / `route-ndip1/0` / `tenant-ndip1` |

Oxia staging source 以 locked base checkout `37a17bef17202d5fd6e23282da5fd26d94865484` 为父提交，
只应用 `e2e/oxia-patches/raft-status-watch-replay-buffer.patch`；persistent patched source commit
为 `aa47797f70183f7551a1191388bc9b079915c79f`，patch SHA-256 为
`0a9f1e83d0bc61b53076f52c20c6289da9d0e2957fc36b37d8723fcab443e448`。该 source overlay 标记
`stagingOnly=true`、`productionAuthority=false`，不得当作 production Oxia 版本。

## G0、Assessment 与 Manifest

G0 是只读闭合 scope。snapshot 枚举了 Command/System/Worker/Native topics、Oxia profile/policy/
ownership/index/resource-incarnation、MinIO payload、RocksDB、checkpoint、Attempt Journal、
query/dedupe、`PUBLISHING/UNCERTAIN` 和 exact Worker set；最终
`unresolvedPublishingOrUncertain=false`。snapshot SHA-256 为
`3cffa885a3bee9d7578145c91dc868f1e9c4024be6260fe49bff61550f05730a`，snapshot 内的 canonical
`snapshotDigest` 为 `2024d837493cafe5dcd55017ca4c08668ca04d20582397f4fc4d9e7c3c8b8ae8`。

Assessment 的 outcome 是 `PASS_DIRECT_REPLACE`，本次执行 resolution 是 `RESET`；13 个 closed
resource 都是 `COMPLETE`、external retention 为 `NONE`、replacement disposition 为
`REINCARNATE`，2 个 Worker 都为 `UPGRADEABLE`。Assessment scope digest 为
`4796da46bef7e4f2b697bc71e4df8575824b6b7e1e38e7d5c84799fb88ed59d1`，assessment canonical digest 为
`e801e4729d609d6639bc317c2971a5fd7df2355c42b7dc0e732356af244f192e`，signed assessment envelope
SHA-256 为 `979cb8dcb8999d63eabdb823739baa8a390d98df9ef8bd9659b1556d0b10399f`。

signed DataResetManifest SHA-256 为
`8c9e8847c96e032b075857e28d45f40c38bf7c8988ee7ae2a713f31d535b7bfb`，manifest digest 为
`ffd9d9aa85b157a6d055576b0693c2ed2adcb49388357bdb684bab6a4d1c76a0`。Manifest readback 为
`exactScope=true`，topics、Oxia profile namespace、MinIO payload marker、RocksDB incarnation
均 read back；`destructiveOperations=[]`。Manifest readback 的 artifact set digest 为
`e530f6680728aaa9b64d27dc4a3531175e0f318e283198c089e4204d1d30c63c`。

证据路径：

```text
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/g0/g0-snapshot.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/data-reset-assessment.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/data-reset-manifest.bin
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/manifest-operation-readback.json
```

## Gate C 与 41 个条件 skip

Gate C receipt 的核心字段：

```text
gateCStatus=PASS
applicableChecks=41
passedChecks=41
startupAssignmentGate=true
noOldGeneration=true
noUnresolvedPublishing=true
noUnresolvedUncertain=true
freshness=true
realOxia=true
realMinio=true
realPulsarP1=true
realGateway=true
realWorker=true
oxiaAdminReady=true
oxiaCoordinatorRestart=true
brokerFailover=true
workerOwnershipTransfer=true
responseLossRecovery=true
```

41 个 baseline `CONDITIONAL_SKIP` 全部分类为 `REQUIRED_STAGING`，本次结果为 `PASS`；没有
`NOT_APPLICABLE`、mock、fixture-only 或未执行行。机器审计文件的 counts 为
`pass=41, failed=0, skipped=0, notExecuted=0`：

```text
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/staging-skip-audit.json
```

真实检查包含 Oxia initial/after-coordinator-restart admin readiness、Oxia coordinator restart、
MinIO fresh/fault、Gateway session churn/leader failover、route provider/notification restart、
credential binding、long GC、target isolation、本地 storage fsync/SST/disaster/ENOSPC、checkpoint
reaping、P1 compile/service/mutation/worker、response-loss、broker failover 和 fresh-process
recovery。缺少任何依赖时 runner 会 fail-closed，不会将 skip 晋升为 PASS。

Gate C signed receipt：

```text
path=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/gate-c-receipt.signed.json
sha256=e104e80691559dd0878af3bd0b8fd39f643de1802e023732d01da383fc2fdfd4
payloadSha256=720021c98bd31bbbc1cd1ce8df758fb3513cb4621d9a78edc0fffb2ff575a3e7
```

## SHADOW

Gate C PASS 后才签发 SHADOW policy。观察使用 finite workload `ndip1-shadow-profile`，ordinary
due publish 开启，持续 205 秒，覆盖 normal run、Worker restart、Worker ownership transfer、
Broker restart/failover、Oxia/MinIO 短暂不可用、policy update、candidate add/cancel、state
rebuild。机器 validator 输出：

```text
status=PASS
observationSeconds=205
nativeAdmission=0
nativeSend=0
handedOff=0
unresolvedPublishing=false
unresolvedUncertain=false
attemptJournalLeak=false
generationIncarnationMix=false
realOxiaRestart=true
realMinioOutage=true
realWorkerOwnershipTransfer=true
```

SHADOW signed receipt SHA-256 为
`c395a98a8181c9ec448eb0e443d22cc8e51f1f3e4435e157bf39a1327e7639e2`，validator 原始结果位于
`.../shadow/validation.log`。观察结束后 candidate policy 已取消并 read back，不保留 active
SHADOW lease。

## 最小 ENABLED canary

SHADOW 全部 PASS 后，仅执行一个 profile、一个 topic、一个固定 subscription、一个 record：

```text
profile=ndip1-enabled-canary-profile
topic=ndip1-native-20260830035421-73816
subscription=ndip1-enabled-canary-subscription
maxRecords=1
nativeAdmission=1
nativeSend=1
handedOff=0
typedP1SendAck=true
targetRecordReconciled=true
responseLossRecovery=true
brokerFailoverRecovery=true
workerOwnershipUnknownDoesNotFallback=true
ordinaryPathUnaffected=true
```

`deliverAtEpochMs=1788063315316`，native canary log、topic stats、internal stats 和 broker
failover before/after state 均已写入 receipt 的 evidence paths。ENABLED canary signed receipt
SHA-256 为 `7bef3f194f747cc0b867ffb975840c1e3f020938281ad2d6f2a087ae7acead`。该 PASS 只证明
这一个 staging record 的真实链路，不是吞吐、长时 soak 或 production capability certification。

## Rollback 与最终状态

canary 完成后写入并签名 `DISABLED` policy，随后用 authority verifier 尝试 activation；工具因
`policyStatus=DISABLED` 按预期拒绝 activation。rollback receipt 为 `PASS`，并核对：

```text
disabledActivationRejected=true
activeNativeProcessCount=0
activeWorkerProcessCount=0
activeLeaseCount=0
activeSendCount=0
environmentReturnedToDisabled=true
productionAuthority=false
```

最终入口：

```text
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/enabled-canary-receipt.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/rollback-receipt.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/disabled-policy.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830035421-73816/authority/final-state.json
```

`final-state.json` 为 `DISABLED`，disabled policy envelope SHA-256 为
`6f8e1ac4928eedff9e239b30596df7edcaf53a9dd8d7220ebd7d61f7d5731053`，rollback receipt envelope
SHA-256 为 `512a4d3aded423a37f3d20babb1c6c008ffd52b6ca402248fa0f0f5785aa8d0c`。Docker staging
资源、bind mounts、source overlay、日志和历史 blocked run 均保留；没有执行 disposable cleanup。

## 当前代码/证据边界

本次 candidate 从冻结 entry baseline 经逐个 reviewable main commit 演进；最终工作树和
`origin/main` 必须在后续文档/检查提交后重新核对。当前 staging receipt 不改变 Accepted
package digest，不修改 `01`–`04`，也不授予生产部署、跨环境 rollout、长期 soak、容量、外部
operator 或 release authority。任何后续重新启动/重新升级都必须创建新的 immutable run、重新执行
G0 和所有签名/freshness checks，不能覆盖本 run。
