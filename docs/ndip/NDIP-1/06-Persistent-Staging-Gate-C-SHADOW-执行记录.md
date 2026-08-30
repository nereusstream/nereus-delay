# NDIP-1 Persistent Staging Gate C / SHADOW 执行记录

> 本文件是非规范的 staging 执行与运维记录。它不修改 Accepted `01`–`04`，不改变
> `acceptance-receipt.json` 的 digest，也不提供 production authority。所有路径和 receipt
> 均属于本机固定 staging；不要复制到其他环境。

## Findings-first 结论

最终状态：`PASS` for the exact staging canary, then `DISABLED`。

- Gate C：`PASS`，`41/41` 个适用检查通过，`skipped=0`、`notExecuted=0`；
- SHADOW：`PASS`，218 秒观察，native Admission/send/HANDED_OFF 均为 `0`；
- ENABLED canary：`PASS`，单一 profile、单一 topic、单条记录，native Admission/send 为 `1/1`，
  `HANDED_OFF=0`；
- rollback：`PASS`，DISABLED activation 被拒绝，active lease/send 均为 `0`，native/Worker
  bounded process 均已退出；
- authority：仅为 `STAGING` evidence，`productionAuthority=false`；不等价于 production
  readiness、release certification 或跨环境升级授权。

历史 blocked run 没有被覆盖或删除，均保留在同一持久化 evidence root。当前正式闭环 run 为
`20260830014412-97404`；run status 为 `COMPLETED`、exit code 为 `0`。

## 固定边界与输入

| 项目 | 当前值 |
|---|---|
| environmentId | `local-docker-staging-ndip1` |
| classification | `STAGING` |
| persistent root | `/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1` |
| evidence run | `20260830014412-97404` |
| candidate commit | `e7d67fe705d2cc0d87108ef2e07dd1340318fe69` |
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
  `ndip1-local-docker-staging/20260830014412-97404`；
- Pulsar P1：两个 source-locked Broker（web `21962`/`21964`，broker `21961`/`21963`）、
  ZooKeeper 和 BookKeeper，image tag 绑定 P1 lock；
- Gateway/Worker：通过真实 source-bound Gateway/Worker JVM smoke path 执行，覆盖 fresh process、
  owner transfer、response loss 和 broker recovery；测试进程结束后没有残留。

当前 run 固定的 topic/resource identity：

| 类型 | identity |
|---|---|
| Command | `persistent://public/default/ndip1-command-20260830014412-97404` |
| System | `persistent://public/default/ndip1-system-20260830014412-97404` |
| Mutation | `persistent://public/default/ndip1-mutation-20260830014412-97404` |
| Worker | `persistent://public/default/ndip1-worker-20260830014412-97404` |
| Evidence | `persistent://public/default/ndip1-evidence-20260830014412-97404`，cursor subscription `ndip1` |
| Attempt Journal | `persistent://public/default/ndip1-worker-destination-20260830014412-97404-attempt-journal` |
| Native canary | `persistent://public/default/ndip1-native-20260830014412-97404` |
| Oxia profile/policy | `oxia://default/ndip1-local-docker-staging/20260830014412-97404/{profile-state,runtime-policy}` |
| Oxia recovery/indexes | same prefix with `checkpoint-catalog`, `query-dedupe`, `obligation-index`, `resource-incarnation`, `worker-registry` |
| MinIO payload reservation | `s3://nereus-delay-ndip1-staging/ndip1-local-docker-staging/20260830014412-97404/payload-reservation` |
| RocksDB | `.../evidence/20260830014412-97404/worker-store/rocksdb` |
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
`6f10ce5443a65e2843619b4c6c3e3bbb6536062b36f7d1568ff809db376ec49f`，snapshot 内的 canonical
`snapshotDigest` 为 `5278f3ee4a4e278aecb597c7aa214e5d744a5d73e8431f99ae64c80c2b2d9cc5`。

Assessment 的 outcome 是 `PASS_DIRECT_REPLACE`，本次执行 resolution 是 `RESET`；13 个 closed
resource 都是 `COMPLETE`、external retention 为 `NONE`、replacement disposition 为
`REINCARNATE`，2 个 Worker 都为 `UPGRADEABLE`。Assessment scope digest 为
`dfc888b5191b98209f89cbd8190ae4841e4ab7a34abc5d13b19870e9e3522d47`，signed assessment envelope
SHA-256 为 `f6f1b365133f74013c42fb2070431cb39bb306c0bcb48d3996750710c68bdfd1`。

signed DataResetManifest SHA-256 为
`9647e8b0665a5e6f5ba6d54ff7e7201d3bbcd829f6cbf57d8e3a2393eb001cf4`，manifest digest 为
`4aeeba43be47b7bf528ddcb24b4a2b73cc9da47b5b6a58f008b59118d013a890`。Manifest readback 为
`exactScope=true`，topics、Oxia profile namespace、MinIO payload marker、RocksDB incarnation
均 read back；`destructiveOperations=[]`。Manifest readback 的 artifact set digest 为
`e530f6680728aaa9b64d27dc4a3531175e0f318e283198c089e4204d1d30c63c`。

证据路径：

```text
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/g0/g0-snapshot.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/data-reset-assessment.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/data-reset-manifest.bin
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/manifest-operation-readback.json
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
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/staging-skip-audit.json
```

真实检查包含 Oxia initial/after-coordinator-restart admin readiness、Oxia coordinator restart、
MinIO fresh/fault、Gateway session churn/leader failover、route provider/notification restart、
credential binding、long GC、target isolation、本地 storage fsync/SST/disaster/ENOSPC、checkpoint
reaping、P1 compile/service/mutation/worker、response-loss、broker failover 和 fresh-process
recovery。缺少任何依赖时 runner 会 fail-closed，不会将 skip 晋升为 PASS。

Gate C signed receipt：

```text
path=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/gate-c-receipt.signed.json
sha256=ed859a2a82a8fafa1a91c8f00d21374a71debeef3b04b80fb2bed185dfa2d1f3
payloadSha256=7534be9af8979efd6b0f6d97cf84ca6e6644bbb9f8ca3bf461014ce10a725928
```

## SHADOW

Gate C PASS 后才签发 SHADOW policy。观察使用 finite workload `ndip1-shadow-profile`，ordinary
due publish 开启，持续 218 秒，覆盖 normal run、Worker restart、Worker ownership transfer、
Broker restart/failover、Oxia/MinIO 短暂不可用、policy update、candidate add/cancel、state
rebuild。机器 validator 输出：

```text
status=PASS
observationSeconds=218
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
`806f8afad3f3d18049c880594f06b7fabefaf592b8cb5e253d2f36df7530ba6d`，validator 原始结果位于
`.../shadow/validation.log`。观察结束后 candidate policy 已取消并 read back，不保留 active
SHADOW lease。

## 最小 ENABLED canary

SHADOW 全部 PASS 后，仅执行一个 profile、一个 topic、一个固定 subscription、一个 record：

```text
profile=ndip1-enabled-canary-profile
topic=ndip1-native-20260830014412-97404
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

`deliverAtEpochMs=1788055464508`，native canary log、topic stats、internal stats 和 broker
failover before/after state 均已写入 receipt 的 evidence paths。ENABLED canary signed receipt
SHA-256 为 `61d06d050949d115cc144a8b80892eed3910789f4d87aceda9188b4dccb37932`。该 PASS 只证明
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
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/enabled-canary-receipt.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/rollback-receipt.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/disabled-policy.signed.json
/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1/evidence/20260830014412-97404/authority/final-state.json
```

`final-state.json` 为 `DISABLED`，disabled policy envelope SHA-256 为
`ea95f3686072162e7fab6fbe7748b7f10f4e7fc523fe9871cbdafe3d28b1796a`，rollback receipt envelope
SHA-256 为 `3c8125bc66379c3403aae9e3d3970ce20539da0eeb39e05f50ff57ea03979f75`。Docker staging
资源、bind mounts、source overlay、日志和历史 blocked run 均保留；没有执行 disposable cleanup。

## 当前代码/证据边界

本次 candidate 从冻结 entry baseline 经逐个 reviewable main commit 演进；最终工作树和
`origin/main` 必须在后续文档/检查提交后重新核对。当前 staging receipt 不改变 Accepted
package digest，不修改 `01`–`04`，也不授予生产部署、跨环境 rollout、长期 soak、容量、外部
operator 或 release authority。任何后续重新启动/重新升级都必须创建新的 immutable run、重新执行
G0 和所有签名/freshness checks，不能覆盖本 run。
