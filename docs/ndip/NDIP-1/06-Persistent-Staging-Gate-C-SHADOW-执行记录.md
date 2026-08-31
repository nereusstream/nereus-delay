# NDIP-1 Persistent Staging Gate C / SHADOW 执行记录

> 本文件是非规范的 staging 认证与运维说明。它不修改 Accepted `01`–`04`，不改变
> `acceptance-receipt.json` 的 digest，也不提供 production authority。

## 当前状态的唯一读取方式

固定环境：

```text
environmentId=local-docker-staging-ndip1
classification=STAGING
root=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1
```

本文不复制“最新 run id / candidate SHA / receipt SHA”。这些动态值只从以下持久化指针读取：

```text
<root>/deployment/current.json
```

pointer 指向一个 immutable final summary。某个 checkout 只有同时满足以下条件，才可声称拥有本机
staging 候选认证：

1. pointer、final summary 与所有引用 artifact 存在且 digest 匹配；
2. final summary 的 `candidateCommit` 等于该 checkout 的 exact HEAD；
3. Accepted NDIP package digest 和 P1 source lock 等于仓库当前固定值；
4. 所有 authority envelope 由固定外部 staging trust root 验证；
5. `e2e/validate-ndip1-persistent-certification.py` 返回 PASS；
6. final state 为 `DISABLED`，active lease/send/Worker/native process 均为 0；
7. `productionAuthority=false`。

pointer 只在整个 pipeline 和独立验证全部完成后原子更新。中断、BLOCKED 或失败 run 不会替换
current pointer，也不得被手工提升为 authority。

## 当前认证入口

先在同一最终 HEAD 上生成不可被系统临时目录清理的 24/24 disposable receipt：

```bash
NEREUS_DELAY_DISPOSABLE_ARTIFACT_DIR=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-artifacts/ndip1-final \
NEREUS_DELAY_DISPOSABLE_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-disposable-local-certification.sh
```

然后把 verifier 已通过的 exact receipt 和 digest 传给 persistent runner。已有 staging 使用显式
`RESET_INTERNAL_ONLY`；首次创建使用 `CREATE_NEW_INTERNAL_ONLY`：

```bash
NEREUS_DELAY_DISPOSABLE_RECEIPT=/absolute/path/to/disposable-local-certification-receipt.json \
NEREUS_DELAY_DISPOSABLE_RECEIPT_SHA256=<sha256> \
NEREUS_DELAY_STAGING_DATA_DECISION=RESET_INTERNAL_ONLY \
NEREUS_DELAY_STAGING_OPERATOR=<operator> \
NEREUS_DELAY_STAGING_EXTERNAL_USER_DATA=false \
NEREUS_DELAY_STAGING_ROOT=/Users/liusinan/apps/ideaproject/nereusstream/nereus-delay-staging/local-docker-staging-ndip1 \
  bash e2e/run-ndip1-persistent-staging.sh
```

runner 拒绝 `/tmp` root、复用 run directory、receipt/source mismatch、缺失数据处置、环境分类与
decision 不一致、外部用户数据、旧终态签名无效、resource identity 重叠和非单调 policy
generation。它不执行 `docker compose down`、global prune、旧资源删除或隐式 migration。

## Certification pipeline

```text
exact 24/24 disposable receipt
  -> fixed external trust root / previous final authority verification
  -> signed DataDispositionDeclaration
  -> read-only G0 closed inventory
  -> signed DataResetAssessment
  -> fresh candidate resources + signed DataResetManifest
  -> 13/13 per-resource operation readback
  -> Gate C 41/41
  -> signed SHADOW lease + 0/0/0 observation
  -> signed ENABLED lease
  -> AUTO_FAST canary 1/1/0
  -> Managed Handoff canary 1/1/1
  -> response-loss resolution / Attempt Journal restart replay evidence
  -> signed DISABLED policy + lease expiry / rollback audit
  -> independent validator
  -> immutable final summary
  -> deployment/current.json
```

### Source 与 authority binding

每个 run 固定：Delay candidate HEAD、frozen entry baseline、Accepted NDIP package digest、P1
source lock、Oxia source/patch digest、Compose config digest、disposable receipt digest、environment/
deployment/tenant/route/shard/Worker exact set。签名使用固定 external staging trust key；artifact
内携带的 key 只作为被验证数据，不能自封为 trust root。

Oxia policy 是 current-head authority，而不是建议值。Candidate/Claim 读取当前 head，Admission
再次验证并冻结 self-contained lease snapshot，物理 send 在 ownership 前验证同一 generation、scope、
validity interval 和 digest。CAS revision 前进但 policy generation 不前进仍然拒绝。rollback 先停止
签发 ENABLED，再等待已签租约到期，最终证明 `EFFECTIVE_DISABLED`。

### G0、数据处置与 Manifest

G0 对全部 13 类 closed resource 做只读收集。若有 previous current deployment，先验证其 final
summary、Gate C/SHADOW/canary/rollback/policy 签名和终态，再签发本次数据处置声明。已有 staging
只能显式选择 `RESET_INTERNAL_ONLY`，且必须证明 `externalUserData=false`；runner 不因分类为
STAGING 就自动认定可 reset。

candidate 使用新的 run-scoped Pulsar topic、Oxia prefix、MinIO prefix、RocksDB path、resource
incarnation 和 Worker generation，不与 previous deployment 重叠。Manifest operation 后对以下
资源逐项 read back：topics、RocksDB、checkpoint、profile、runtime policy、payload reservation、
Attempt Journal、evidence cursor、query/dedupe、obligation index、resource incarnation、Worker
registry 和 candidate deployment markers。`destructiveOperations` 必须为 exact empty list。

### Gate C

Gate C 的 41 个适用检查必须全部 `PASS`：

```text
applicableChecks=41
passedChecks=41
failed=0
skipped=0
notExecuted=0
```

检查覆盖真实 Oxia coordinator readiness/restart、MinIO persistent/fault、Gateway session/leader、
Worker ownership transfer、local-storage fsync/SST/disaster/ENOSPC、checkpoint reaping、P1 compile/
service/mutation/worker、four-cut response loss、双 Broker failover、fresh-process recovery、exact
generation、zero unresolved `PUBLISHING / UNCERTAIN` 和 startup assignment gate。fixture-only、mock、
`CONDITIONAL_SKIP` 或未执行行不能晋升为 PASS。

### SHADOW

Gate C PASS 后才可签发有界 SHADOW policy。有限工作负载必须覆盖 Worker restart/ownership transfer、
Broker restart/failover、Oxia/MinIO 短暂不可用、policy update、candidate add/cancel 和 state rebuild，
并得到：

```text
nativeAdmission=0
nativeSend=0
handedOff=0
unresolvedPublishing=false
unresolvedUncertain=false
attemptJournalLeak=false
generationIncarnationMix=false
```

观察结束后取消 SHADOW candidate policy，并从 Oxia current head read back。

### ENABLED 双路径 canary

ENABLED 不是开放流量，而是一个 profile、一个 run-scoped topic、固定 subscription、最多两条记录：

```text
maxRecords=2

AUTO_FAST:
  nativeAdmission=1
  nativeSend=1
  handedOff=0

Managed Handoff:
  nativeAdmission=1
  nativeSend=1
  handedOff=1

combined:
  nativeAdmission=2
  nativeSend=2
  handedOff=1
```

AUTO_FAST 证明 current prepared-record coordinator、P1 encoder/transport、exact business
`deliverAt`、typed SEND/ACK evidence、broker failover/response-loss 和 ownership-after-unknown no
fallback。

Managed Handoff 必须通过真实 production composition：Worker schedule → eligibility → Claim →
Admission frozen snapshot → physical lease gate → Attempt Journal mapping-before-send → fixed
`sequenceId` → ownership marker → source-locked P1 `.deliverAt(...)` → SEND/ACK evidence → definitive
Outcome source log。证据必须绑定 `preparedPublishHash`、`preparedRecordHash`、
`sendCommandSha256`、`authenticatedResponseCommandSha256`、target message identity 与 Attempt
Journal 的 `MAPPED / OWNERSHIP_STARTED / PUBLISHED` 记录。response-loss 只能通过 evidence/query
resolution，不允许盲目 resend。canary 还必须关闭并重开同一个 durable Journal subscription，从
`MessageId.earliest` 重建完整三记录状态；只依赖已有 subscription 的 `InitialPosition.Earliest` 不算
重启恢复证明。

### Rollback 与独立验证

canary 后签发 `DISABLED`，验证 activation 被拒绝，并等待旧 ENABLED lease 的 `validUntil`。最终
要求：

```text
disabledActivationRejected=true
activeNativeProcessCount=0
activeWorkerProcessCount=0
activeLeaseCount=0
activeSendCount=0
environmentReturnedToDisabled=true
productionAuthority=false
```

独立 validator 使用标准 Python 与 OpenSSL 重新计算 domain-separated Ed25519 签名，不调用 runner
的验证函数。它同时重验 data disposition、G0/Manifest/readback、Gate C、SHADOW、完整 policy
generation chain、双路径 canary、Managed physical record/Journal chain 和 rollback。binary
`DataResetManifest` 的 logical digest/Ed25519 签名，以及 canary 引用的八份 raw evidence 文件摘要也
由独立 validator 重算；其 machine receipt 进入 final summary binding。

## 历史执行记录

run `20260830035421-73816` 曾在旧候选
`ed3cc4987dab6ebf179f6bfafcfd159c2e54188e` 上完成 Gate C、SHADOW、单条 AUTO_FAST canary 和
DISABLED rollback。它以及更早的 blocked runs 均保留在 persistent evidence root，不覆盖、不删除。

该历史 run 有以下明确边界：

- 不包含当前 Managed Handoff Worker/Admission/Journal/Outcome canary；
- canary 是 `maxRecords=1`、`1/1/0`，不能替代当前 `2/2/1` contract；
- 没有当前的显式 signed data-disposition、13/13 post-operation readback 与独立 full-chain verifier；
- 绑定旧 source commit，不能认证后续代码或文档 HEAD。

历史证据仍可用于回归比较和验证 previous terminal authority，但不能被 current pointer 指向当前
候选，除非它满足本文件开头的全部新条件（该 run 不满足）。

run `20260830081450-52897` 在候选
`6b2a0e19f0e31984ce00884531cd4d979ec46a39` 上执行到 Manifest 资源读回后 fail-closed，状态为
`BLOCKED`。它完成了此前的 Gate C 条件测试，包括预期 `SIGKILL` 后 fresh-process recovery、真实
ENOSPC、两类 P1 response-loss 与双 Broker failover，但没有签发 Gate C receipt，也没有进入
SHADOW/ENABLED。失败原因是 runner 曾把 `ROCKSDB_STORE` identity 声明为
`worker-store/rocksdb`，真实 Worker 却使用父级 `worker-store`，而 Worker smoke 的非 crash 清理又会
删除整个真实 root；因此声明资源与物理资源不相同，并在 13-resource readback 第三项被正确阻断。

当前 runner 已将 `ROCKSDB_STORE`、`NEREUS_DELAY_PULSAR_WORKER_ROOT` 和 incarnation marker 统一到
同一个 exact `worker-store` 根。real Pulsar Worker smoke 在 `STAGING` classification 下像保留
staging topic 一样保留该 Store，后续 readback 对实际 ShardStore 文件和 marker 一并取摘要。
`test_ndip1_persistent_staging_contract.py` 固定这两个约束。该修复只恢复真实资源绑定；失败 run
仍然不可晋升，任何新认证都必须绑定修复后的新 HEAD，并从 24/24 disposable receipt 重新开始。

后续 run `20260830084756-10599` 又在同一资源绑定上暴露了第二个 fail-closed 问题：不同 Shard 的
Worker drain 曾共用固定 `worker-final-checkpoint` 目录。保留真实 Store 后，第二个 Worker 场景会因
checkpoint target 已存在而拒绝。当前 P1 Worker production-composition smoke 将 final checkpoint
路径和 16-byte checkpoint identity 同时绑定到 Route Incarnation、unsigned partition 与 owner epoch；
同一次 drain retry 保持同一身份，不同 Shard 或 ownership generation 不再别名。该 blocked run 同样
没有 Gate C/SHADOW/ENABLED authority。

run `20260830091937-44496` 在候选
`127513618f78c3b21d88540406fc3ce40844cbe2` 上完成了 13/13 Manifest readback、Gate C 41/41 和
SHADOW 的真实 dependency/ownership 场景，但在 SHADOW observation validator 处 fail-closed。旧
validator 对 `shadow/` 递归读取所有 `*.log`，因而把 Worker `worker-root` 中 RocksDB 的二进制 WAL
`000023.log` 错当作 UTF-8 应用日志，触发 `UnicodeDecodeError`。该 run 没有签发 SHADOW receipt、
没有进入 ENABLED，也没有更新 current deployment pointer；其 Gate C receipt 仅绑定旧候选，不能
认证修复后的 HEAD。

当前 validator 将 `chaos/shadow-worker-ownership/worker-root` 定义为唯一的 typed persistent-state
树，不再把其中的 RocksDB WAL/checkpoint 文件解释为文本；该树外所有 `*.log` 仍必须是 regular、
non-symlink、strict UTF-8，并继续检查 native physical-send 禁止标记。回归测试同时覆盖二进制 WAL
被正确分类、状态树外非 UTF-8 证据 fail-closed，以及真实 native-send marker fail-closed。不能通过
`errors=ignore` 或捕获后静默跳过来放宽证据验证。

run `20260830100335-44793` 在候选
`57035ab04319fa1ff1538bc0a2fc41c15c480ca4` 上验证了修正后的 SHADOW parser：13/13 Manifest
readback、Gate C 41/41、250 秒 SHADOW observation 和真实 Oxia/MinIO/Worker ownership cut 均通过，
SHADOW 结果为 `0/0/0`。随后 AUTO_FAST 真实业务消息也成功在 `deliverAt` 前持久化并产生 typed
SEND/ACK evidence，但 runner 在进入 Managed Handoff canary 前 fail-closed。原因是物理证据中的
`p1SourceLock` 按规范保存 32-byte canonical source-lock digest，runner 却用 40-hex P1 commit 与它
比较。失败清理已把 Oxia current head 改回 `DISABLED`；其二级诊断又因错误文本的词序匹配过窄而
中止，因此该 run 没有 canary/rollback receipt，也没有更新 deployment current pointer。

当前 runner 和独立 validator 明确区分两种类型：Gate C/G0/最终 certification receipt 绑定 P1
commit；AUTO_FAST、Managed Handoff 和 `ArtifactGenerationSet` 的物理证据绑定
`SHA-256("nereus/delay-resource-guard@" + commit)`。独立 validator 只接受 canonical 40-hex commit，
再自行派生 64-hex digest验证 raw evidence；runner 不接受调用方直接注入 digest。stale ENABLED
校验则接受 `current Oxia handoff policy` 和 `current handoff policy ...` 两类 closed rejection 文本，
仍要求验证命令非零退出。专项测试固定 commit-to-digest 向量、双 canary evidence 检查和 stale-head
诊断；该修复不会把上述 blocked run 晋升为 authority。

run `20260830104711-17324` 在候选
`e46dba0371095913da298a623b4ff883c569e643` 上完成了 13/13 Manifest readback、Gate C 41/41、
SHADOW `0/0/0` 和 AUTO_FAST `1/1/0`，随后在 Managed Handoff 的 provider-driven Claim 前
fail-closed。后续源码复核纠正了该 run 的初步归因：Profile 的 `maxHandoffLeadMs=15000` 本来就应
生成持久 `earliestNativeCandidateAt`，真实 ENABLED snapshot 的 `effectiveLeadMs=7000` 只在当前
policy eligibility 与 Claim materialization 中生成实际 `actionAt`；把二者合并反而违反 accepted
设计。真正缺口是恢复后的 READY 游标永久排除了游标 key：恢复阶段没有 trusted time，只装入
ordinary 投影；唯一 READY head 随后无法被 current-policy 扫描重新访问，因此 32 个有界 due turn
都不能产生 Claim。该 run 没有 Managed evidence、canary receipt、rollback receipt 或
current-pointer authority。

这次失败同时证明旧 rollback 依赖顺序不闭合：失败发生在常规
`canary/native-topic-stats.json` 生成前，清理却读取该文件，导致二级 rollback 审计中止。当前
修复后环形 READY 扫描会在遍历 tail 与 wrapped prefix 后重新检查游标 key，现有
`discoveredHeads` 继续承担重复 Claim fence；Worker canary 同时断言持久候选等于 Profile 最大 lead，
并从已加载 policy publication 读取 actual `effectiveLeadMs` 用于等待、eligibility 与诊断，拒绝
小于等于零或大于 profile 上限的 lead，并给 prepare/resume 留出固定 30 秒 canary horizon。
rollback 不再依赖
canary 成功路径的文件，而是在旧 ENABLED lease 到期后通过 Pulsar Admin 重新读取 exact native
topic；HTTP 200 必须得到 closed publishers 集合且数量为零，HTTP 404 只表示 topic 不存在，其他
状态全部 fail-closed。该 live stats 的 path、digest 和 HTTP status 进入 generation-2 签名 rollback receipt，独立
validator 重新计算摘要并验证零 publisher。上述修复只使后续新 run 可重新认证，不会补写或晋升
失败 run。

run `20260830122759-34348` 在候选
`d228ca100df5a83a0ddb70004c9ef6c1098cd296` 上进一步完成了 13/13 Manifest readback、Gate C
41/41、280 秒 SHADOW `0/0/0`、AUTO_FAST `1/1/0`，并证明 singleton recovery cursor 修复后
Managed Handoff 已能产生 provider-driven Claim。该 run 随后在 Admission append 已返回 ENQUEUED、
但同一进程的 16 个非阻塞 source turns 尚未收到新记录时返回 `SOURCE_TURN_LIMIT`。runtime 对这个
状态保持无 attempt、无 destination touch、无 owner fence；real-client harness 却先解包 attempt，
把可重试的 source propagation 边界误报为 durable attempt 未保留。失败清理已发布签名
`DISABLED` head，未生成 Managed evidence、canary receipt、rollback receipt 或 current pointer。

该轮 real-client harness 修复只对 ENQUEUED 的同一 `publishAttemptId` 和同一 Admission Source Position
续跑 bounded source turns：每轮仍使用 runtime 的 16-turn 上限，轮间 25ms，wall-clock 总上限 30 秒；
它不重新 append Admission，也不改变 payload/attempt identity。只有
`SOURCE_TURN_LIMIT` 可续跑；其他状态立即按详细 status、source-turn 与 last-source-turn 证据
fail-closed；后文记录的 physical safe-time 修复再把 `PHYSICAL_DEFERRED` 纳入同一有界重试。
`PHYSICAL_SUBMITTED` 验证先于 attempt 解包，因此后续失败不再被错误诊断覆盖。该修复
仍须由新 HEAD 的完整 disposable 与 persistent certification 重新证明，不能晋升上述 blocked run。

run `20260830131642-82551` 在候选
`efd07d90c605338bf4e4a15ed5a646afc23ff2d3` 上完成了 13/13 Manifest readback、Gate C 41/41、
244 秒 SHADOW `0/0/0` 和 AUTO_FAST `1/1/0`。Managed Handoff 的 Admission 随后已经真实写入
source log，传播重试也读取并 ACK 了该 exact position，但 durable apply result 为
`REJECTED / STALE_SYSTEM_MUTATION`，因此没有生成 `PUBLISHING` ledger，runtime 正确围栏并停止
destination SEND。失败清理签发并验证了 `DISABLED` rollback；终态为零 active lease/send/Worker/native
process。该 run 没有 Managed evidence、canary receipt、final summary 或 current-pointer authority。

对失败 RocksDB 和 source frame 的只读解码证明 Admission 自身是合法的：冻结 contract 为
`PULSAR_NATIVE_DELIVERY`，`actionAt = deliverAt - 7000`，Profile 上限为 15000，decision interval、
broker time、policy generation/scope/signature 均通过逐项校验。真正缺口位于 real Worker 组合：
`DelayShard` 曾以 `profileCatalog=null` 创建，导致 Admission apply 落入 ordinary Managed 分支并错误
要求 `actionAt == deliverAt`；与此同时 staging catalog 的 credential Head 也是空值，不能只补一个
constructor 参数绕过 source-ordered Profile activation。

`main@80e5b580bae92f875437d9332f4c14357ac4d689` 关闭了这一组合缺口。Managed Worker 现在把同一个
closed Profile catalog 注入 Schedule resolver 与 Admission validator；catalog 提供与当前 Manifest/
policy 绑定的 immutable credential binding、Head 和 protection，Channel lease 必须逐项匹配该 binding
和 protection。Destination Profile activation 先作为签名 `APPLY_SHARD_CONTROL` mutation 经 guarded
P1 source append、source apply 和 ACK，再允许业务 Schedule；Managed 路径不再发送一个 catalog
不可解析的普通 active 占位命令。若 Admission 已 source-applied 却没有 ledger，诊断同时保留实际
durable `applyStatus/stableCode`，但继续 fail-closed 围栏。source contract、focused runtime tests、
完整 `./gradlew check` 和 source-locked P1 `compileRealPulsar` 已通过；它仍须由该提交之后的新 exact
24/24 receipt 与完整 persistent run 重新认证，不能反向晋升 `20260830131642-82551`。

run `20260831111813-39459` 在候选
`e68ef9ea68e0fb44ffda4fdffe5ec68fd98a728a` 上完成了 exact disposable receipt 校验、持久资源只读
inventory、G0 Assessment、签名 Manifest 及 readback，并继续执行 Gate C 的真实恢复检查。它在
`OxiaRealRouteAuthoritySmokeTest.signedRouteProviderRecoversAfterRealOxiaRestart` 失败：runner 已在
节点 restart 后用 fresh CLI client 证明 `127.0.0.1:16681` 可读，并等待超过 15 秒 session timeout
的 20 秒 grace；测试内的旧 authority `SyncOxiaClient` 仍在 60 秒后抛出 `TimeoutException`。run
因此写入 `BLOCKED`，未签发 Gate C receipt，未进入 SHADOW/ENABLED，也没有更新 current pointer。

源码调查确认 `OxiaRouteAuthoritySession.reconnectSession()` 只旋转 marker，却从未替换 connect
工厂创建的 authority client；因此该失败不是简单的等待预算不足。提交
`main@8aa6237526b2d047091d107c336934eed5aa8eb8` 为 factory-managed session 增加 fresh authority
client replacement：replacement 创建成功后接管 delegate，旧 client 必须关闭，然后才创建新的
ephemeral marker/session identity 并执行 authority read；任何创建、关闭或 start 失败都继续
fail-closed。notification client 不被无 offset 的新 watch 替换，继续依赖 Oxia 原生 offset-tracked
重试，避免跳过下一条 committed Route event。确定性测试验证旧/new client 的关闭边界和 session
identity 旋转；同一 persistent staging 集群上的精确 `data-server-1` stop/start、client-readiness、
20 秒 expiry-grace 定点测试在 47 秒内通过。该 focused PASS 只证明修复方向，仍不能晋升失败 run；
必须由该提交之后的新 exact 24/24 receipt 和完整 persistent certification 重新签发 authority。

run `20260831115134-69791` 在候选
`9518ec0bae054dcf9a51115fa966569bffdf26e1` 上验证了 Route reconnect 修复，并完成 13/13 Manifest
readback、Gate C 41/41、226 秒 SHADOW `0/0/0` 与 AUTO_FAST `1/1/0`。Gate C 签名 envelope SHA-256
为 `d51bc8bf99c93706b7a832c7ee593035e376f512d139303dd209dcefaa29066c`，SHADOW envelope SHA-256
为 `fe1cb3c648a780ab5db68ea93ece09b3f71d0bb419832ba557641c7ade1c6197`。Managed Handoff 随后在
业务 target SEND 前以 `FAILED / NullPointerException: PublishEvidence` 停止，因此没有 Managed
evidence、完整 canary receipt、final summary 或 current-pointer authority。失败清理签发并验证了
generation-2 rollback receipt，终态为 `DISABLED`、active native process/Worker/lease/send 全部为零，
`productionAuthority=false`。

源码调查确认根因由两个边界叠加形成。Admission 冻结的 decision time 是一个有界 interval，紧接着
取得的 physical trusted-time sample 可能暂时仍与它重叠；旧 executor 把这个可重试 safe-time
边界误作 definitive non-publication。该结果又只携带 `evidence=null`，而 source-log Outcome factory
要求所有 definitive 结果都能解码为 typed `PublishEvidence`，于是正确地拒绝了无证明状态，但只留下
了 NPE 诊断。业务 target 的 Producer 可以在 composition 初始化时存在，但此次失败没有调用其
message builder/SEND；Attempt Journal 也不应因暂时 overlap 创建 mapping。

`main@e97facf1cc31c101d46b54af598b1f9d92f7de13` 修复这两个生产边界：首次 frozen-lease 验证移到
Attempt Journal mapping 前，interval overlap 返回 `PHYSICAL_DEFERRED`，real-client 只保留同一
`publishAttemptId`/Admission 并在 30 秒有界窗口内重采 trusted time；其他已证明的 pre-library
拒绝创建 closed `AdapterNonSubmissionEvidence`。该证据的 channel、Publish Attempt owner、prepared
hash、完整 request hash、Adapter conformance generation 与 stable code 在 Outcome 交界处逐项绑定，
任何 null、opaque 或错配 proof 都降为 `UNKNOWN`。此外，foreign-owner recovery 不再受当前
physical-send activation 影响：它只读取/退休既有 Journal mapping，返回
`RECOVERY_FIRST_SEND_UNCERTAIN`，不触碰 target。focused tests、完整 `./gradlew check` 与 source-locked
P1 `compileRealPulsar` 均通过，但修复提交仍不是 staging PASS；后续必须绑定更新后的最终 HEAD
重新生成 exact 24/24 disposable receipt，并从头运行完整 persistent certification。

run `20260831131814-77467` 在候选
`827d9c47af98f6aadb7ec5029298e76ac72202aa` 上完成 Gate C 41/41、218 秒 SHADOW `0/0/0`
与 AUTO_FAST `1/1/0`，Managed Handoff 则在 Attempt Journal 已持久化 `MAPPED` 和
`OWNERSHIP_STARTED` 后返回 `UNKNOWN / PULSAR_EVIDENCE_DIVERGENCE`。失败路径签发并验证了
rollback，环境回到 `DISABLED`，active native process/Worker/lease/send 全部为零；该 run 没有
Managed evidence、完整 canary receipt、final summary 或 current-pointer authority。

物理证据与源码交叉检查排除了“PUBLISHED Journal response-loss 恢复失败”：Managed 业务 target
没有被触碰，target Topic 中唯一业务消息来自此前的 AUTO_FAST canary。真实缺口是 Worker
composition 在 persistent activation 已生成 `ManagedHandoffConfiguration` 后，仍调用 adapter 和
transport 的默认构造器；两个 `nativePreparedDeliveryEnabled=false` H0 门在 ownership marker
之后拒绝了 native prepared record。此时不能安全退休 mapping，也不能把拒绝提升为 definitive
non-publication，因此 executor 正确保留 UNKNOWN。

`main@4eeff43144aced28931e5b054299dc07be00daae` 只在 exact Managed persistent activation 存在时，
用同一个局部 activation 位同时打开 adapter 与 source-locked P1 transport 的 native prepared-record
门。默认构造器和其他调用路径不变；Admission frozen snapshot、physical activation gate、frozen
lease recheck、Attempt Journal ownership marker 仍在真正 SEND 前逐层 fail-closed。新增测试先证明
默认 adapter 不调用 transport，再证明显式 activation 后同一 native record 恰好一次到达 prepared
transport。该提交及测试仍不是环境认证；新 run 必须绑定其后的最终 HEAD，重新生成 24/24 receipt
并从 Gate C 起完整执行。

## Authority 边界

即使 current pointer 对某个 HEAD 验证 PASS，也只说明固定本机 staging 的 bounded 候选认证：

- `productionAuthority=false`；
- 不提供 production 数据处置、部署或升级 authority；
- 不证明长时 soak、容量、SLO、跨 region/tenant、operator rotation 或 release readiness；
- 不把 NDIP-1 状态改为 `Implemented`；
- 其他环境仍须独立 G0、Manifest、Gate C、SHADOW、canary 和 rollback。

Docker staging infrastructure、bind mounts、source overlay、历史/当前 immutable artifacts 均保留，
但任何后续重跑都必须创建新的 run id 和 resource incarnation，不能覆盖已有证据。
