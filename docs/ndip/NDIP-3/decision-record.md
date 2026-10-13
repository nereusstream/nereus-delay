# NDIP-3 用户授权与决策来源

日期：2026-09-07。来源：本任务 `01a07c2a-12f0-7eb1-9c5a-16c8f4810a16` 的用户交接，
父任务 `01a07bff-0ce3-7c61-b18f-9c826fe0c000`。

实际授权涵盖完整设计的 A–F、29 个切片，必要设计决定、实现、测试、docs 同步、
单 agent 直接 main 分阶段 commit/push；要求使用目标模式且没有 token budget。
这是一条授权事实记录，不是维护者签名，不声明执行者为维护者，也不是环境激活 receipt。

常规设计可自主完成，Draft/PLANNED 是起点。B7 仍须形成精确、闭合的规范包及如实反映
上述决定来源的接受记录；不能伪造维护者身份、签名或使用 NDIP-1 历史证据认证新源码。
所有持久环境操作必须绑定实际环境身份和既有规则；未识别数据不得清空。

## 2026-10-12：Target channel 使用 signed Source 生命周期事件

决策来源：用户在主会话“调整 A–F 分区队列推进方式”
（`01a13046-4fcd-7e90-8a56-dc2c3828fe23`）提交回答：
“采用 Source 事件（推荐）：每次生命周期变更多一条日志，复用现有恢复路径。”
实施会话已读取该用户原文；它不是预选项、维护者签名或生产 activation receipt。

CREATE、RENEW、STOP_NEW_WORK 进入同 Shard 的有序 Source，复用 existing
PreparedControlOperation/注册 CONTROL actor 的签名、actor/scope/key 授权、exact 首结果/
重放与原子账本计费。另核对对应 current Owner/fencing。APPLY_SHARD_CONTROL 保持
CONTROL 作者分支，不扩大 OWNER 签名权限，不新增独立 OWNER mutation type。封闭
body/branch/注册号和有限 slot 当前代 anchor 的必要实现细节可沿本方案自主落实。

必须观察 lifecycle 实际 APPLIED 后，该代才可用于新 Admission；append/响应 UNKNOWN
保留原签名字节，不换代或重复计费。正常 renewal generation 恰好加一，stable producer、
Journal、sequence 与 evidence domain 保持；旧 frozen identity/lease、旧 attempt/UNKNOWN
继续沿原义务恢复。STOP_NEW_WORK 只停止新增，不释放未决、physical/zombie 或历史保护。
无记录的旧 format2 不静默初始化/补写，旧 Lane/format1 bytes 与保护边界保持。

本决定只解决包 1 的持久 channel 语义，不表示 channel writer 或完整发送/接管链已完成；
所有测试、Broker、性能和验收仍在全部开发完成后，生产/完成 flags 不提升。

## 2026-10-12：架构裁决责任与 Target Native v5

用户在决策主会话明确要求“主会话做一些架构上的回答，不用让我来人工确认”；
已授权范围内的实质架构/协议分歧由主会话直接裁决，普通可逆细节由实施方选择。
该授权不改变四包、P2、正确性和真实环境执行边界，不另设逐项人工审批。

主会话已裁定新增独立 Target materialized Native Admission v5，保留 v3/v4、ordinary
binding 与旧 Lane bytes。完整 Claim/channel、Target Native scope/snapshot/head revision、
历史授权、physical/payload/metadata/window/hash/Artifact/实际 Native contract 同时冻结。
Source 首应用先核对 current head/Claim，再验证完整历史 trust、ENABLED/managed path/
fixed cap/UTC，再读 head 确认不变；首次 ownership 使用原冻结 snapshot 及 Admission
Source 下的 trust/lease，不能读新 head 改签原 attempt。

复用现有 signed Outcome/Resolution 和三种 sideEffect，不新增第四种。认证 Native
success 按实际 contract/evidence 投影 HANDED_OFF，ordinary v4 仍 PUBLISHED；不能仅
凭 deliverAt/actionAt 或发送时已 due 分类。Managed Handoff 保持 MANAGED_JOURNAL
sequence 与 publishAttempt identity，不误走 AUTO_FAST/producerAssigned factory。
优先复用 generation2 ACK/Journal evidence 和 P2 wire，只增必要 Native binding；确实
无法表达时再采用明确新增版本/reader gate，不能重解释旧 bytes 或把 Target 改成 Lane。

成功首应用同批投影 terminal/index/head/result/quota：active P 退出、唯一仍受保护
payload owner 转 H；旧 attempt B、physical/zombie、Journal/evidence/pins/retained reserve
不因聚合终态清空。UNKNOWN 保留，definitive failure 后新 attempt 只走 ordinary retry；
迟到 resolution 只结清 exact obligation，不覆盖新 generation 或重新开放 handoff work。
v5 同步进入 history/checkpoint/reopen/ledger audit/envelope 和 covering schema/Artifact/
eligible readers。签名分工仍 CONTROL lifecycle、OWNER Admission/initial Outcome、
SERVICE resolution。本条是源码架构决定，未计作实现完成或 SDK/Broker 验证。


## 2026-10-12：Native Claim 独立 v2 冻结 policy head

决策主会话核对实际 `nativeCommitAuthority → Host.claim → Shard.claim → TargetClaimStore`
后批准必要的 NV36 schema v2。原闭包只验证 expectedHead，v1 持久 Claim 没有保存它；
不能以 controlVersion 或事后 current-head 查询替代 scope/generation/snapshot/revision。
普通 Claim 与全部 v1 canonical bytes/ID/digest 保留；新 Native Claim 必须 v2，field 15
保存完整 bounded HandoffPolicyHeadRef，纳入独立 v2 identity preimage、record digest、
实际 quota charge 和最大 envelope。同一 expectedHead 贯穿调度、guard 和持久写入；
Native 本地 operation digest 也绑定该 ref。未知记录版本拒绝。

v5 首应用读取实际完整 v2 Claim，精确匹配冻结 ref，再执行既定 snapshot/trust/current
head 两次读取；新 writer 依赖覆盖该 v2/v5 的 schema/Artifact/eligible readers。旧 Native
v1 保留解码、重开、审计与历史恢复；尚未 Admission 的可逆 Claim 只能按原 Owner/Store/
原子计费撤销后重新选择。已 Admission、UNKNOWN 或 Journal ownership 保留原义务，
不得补 ref、原地升级或套用可逆 Claim 清理。本决定没有真实迁移、批量删除或生产激活授权。


## 2026-10-12：独立受保护 Claim takeover revoke

决策主会话批准补齐已有 quota §19 的恢复业务证明；普通 local revoke 保留同 Owner/
Store 限制，不新增 Source mutation、通用恢复框架或持久任务队列。新 Owner 必须先
受保护回放达到当前 accepted assignment 的完整 activation barrier，完成实际 root/
lineage/账本审计与旧 writer fencing，并持有完整当前 lease/token/session、已持久
openedOwnerEpoch；epoch 按 unsigned 严格高于旧 Claim Owner。仅 checkpoint CLAIMED、
epoch 增大或 DB 查无单条 Admission 均不足以取得权限。

在新 Owner 激活后、将 Shard 暴露给普通 poll/Claim/Admission/SEND/维护之前，有限 turn
扫描实际遗留 Claim；同 DB 重开及新 Store checkpoint restore 均适用。候选必须同 view
核对 full Claim/key/首次 charge/key/Message CLAIMED、scope/accounting/lineage/恢复来源，
exact 派生 attempt 的 Admission/Budget/obligation 与已完整 Source barrier。无任何已
Admission/UNKNOWN/Journal ownership 或未决 append 矛盾方可撤销；未知/缺保护失败关闭，
不能新造 false-returning absence provider。非候选交原 settlement，不改回 TIMELINE。

复用 claim.revoked 的完整语义，保留 generation、其他义务、Admission 数与 uncertain
retry 权限，推进 revision；原子恢复 indexes/head/order 并删除 exact Claim/首次 charge，
仅扣原冻结费用和实际 delta。新 local ordinal 的 operation digest 绑定旧完整 bytes、
转换与新恢复 Owner/Store/lineage/barrier；prepared/native commit guard 保护当前切点及
全部 before bytes。失权/新写/commit UNKNOWN 沿原 fence/reopen 精确重读，容量只在
确定提交后释放；崩溃后重走原审计/barrier，不后补 Native ref或重发旧 Attempt。

初始/完成审计沿现有恢复边界；每个合法局部批次只核对本次读写集/保护，不每 Claim
重扫全库或复用已经过期 audited.view。本条是必要实现裁决，入口尚未完成；不提升
包 1/全部切片/生产或验证状态。源码回归与真实恢复仍统一后置。


## 2026-10-12：Target Journal post-ownership 负结论收尾

主会话核对 retireNotPublished/replay 后批准 Target namespace 独立
RESOLVED_NOT_PUBLISHED durable settlement，使用 Target Journal record codec v2 新终态
分支。旧 Target v1 四种 bytes、Mapping ID/bytes、Lane generation 3 与 pre-ownership
RETIRED_NOT_PUBLISHED 的含义不变；两个旧 codec 显式封闭四种 kind，不能因 enum
增加而接受新分支。新增完整 settlement payload 有界并纳入 canonical/hash/cursor/
checkpoint/replay；重复必须核对完整 payload，不能仅以 mapping/kind/position 接受。

顺序为独立认证的 definitive 否定证据 → 原 OWNER Outcome/SERVICE Resolution 的真实
exact Source APPLIED 首结果 → durable Journal settlement → 解除 unresolved sequence
slot/in-process pending。原 Admission/mapping/prepared/template/contract/channel/Producer/
sequence/Artifact、完整 Source 与首结果、实际 Journal writer/Owner/physical/retention/pin
权利均须保持；不能以 DTO/boolean/timeout/Disable/expiry 或本地文本造负证明。新负
终态不得反过来单独认证产生它自身的 Source NOT_PUBLISHED，避免循环自证。

仅 OWNERSHIP_STARTED →新负终态；与 PUBLISHED 互斥，矛盾证据 integrity/fence，禁止
旧 mapping 再发/retransmit。旧 MAPPED 的 pre-ownership retirement 保持。append/ACK
UNKNOWN 不先清 slot；严格保持原 stable Producer/Journal/evidence generation 与已分配
sequence，新 mapping 只用 lastSequenceId+1，不借 renewal/reset 绕过阻塞。不新增
Outcome 第四态，不第二次扣 Source reserve，physical/zombie/receipt/Journal/evidence
释放仍独立受保护。

须接通 live definitive rejection 和 UNKNOWN→SERVICE NOT_PUBLISHED，跨崩溃从真实
Source 已决、Journal 未决的实际材料继续；已有仅 ADMITTED discovery 不足以承接此
路径，复用有界 retained input/recovery maintenance而不加线程/全局队列。在 Journal
settlement 未确定前保留对应 Source 首结果/Admission/evidence/pins。新 reader/writer/
Artifact/eligible readers 与有限空间覆盖此记录后才能激活。当前仅完成架构裁决，
该 Journal 分支及完整收尾仍待实施；测试和 Broker 验收继续后置。


## 2026-10-13：K2 Source Admission v6 与 receipt 恢复域

主会话批准独立 Target K2 namespace，并裁定唯一 durable mapping 边界为实际 Source
Admission 首应用批次。Kafka receipt topic 不写非事务 MAPPED，不增加 Journal 后端。
Target v3/v4/v5、旧 Lane mapping/receipt/cursor bytes 保持；v6 只用于普通 Kafka
KAFKA_TRANSACTIONAL_RECEIPT。新增独立 ProtocolTuple body generation=6，沿 v4 fields
1–23，required field24 是 TargetKafkaReceiptBinding v1。旧 v4 K2 缺上下文的历史 Admission
可按原权限读回，不能补字段改签、重造 first-send permission。

K2 binding 的 closed fields1–9 为：schema1、target physical topic、原 Registry
KafkaReceiptSlotResource、receipt physical topic、slotsPerShard、receipt partition、真实
protected scanStartOffset、同次观察 digest[32]、retention protection digest[32]。两个 Kafka
物理名各≤249 ASCII bytes，资源 cluster≤256 UTF-8 bytes，binding≤2048 bytes；slot 物理
几何及同 cluster/source Shard/transactional channel 在解码/first application 核对。
签名的数字/digest 不是观察或保留权威；实际 MaterializationAuthority 必须独立认证。

NV45 TargetKafkaReceiptMapping v1 位于 META `26 01 + mappingId[32]`，closed fields1–7
为 schema1、原完整 v6 body、原 mutation hash、actual Source TargetQuotaMutation、非零 raw
uint64 logicalSequence、lineage[16]、mappingId[32]。mappingId 是 SHA-256(UTF-8
`nereus-delay-target-kafka-receipt-mapping` + NUL + canonical fields1–6)。不反向依赖 APPLIED
结果 hash，不改原 signed body/envelope/prepared hash。NV46 TargetKafkaReceiptSlot v1 位于
META `27 01 + slotId[32]`；slotId=SHA-256(UTF-8 `nereus-delay-target-kafka-receipt-slot` + NUL
+ stable producer identity)。closed fields1–6 为 schema1、最近分配的完整 channel、lastAllocated
raw sequence、optional pending mappingId、actual Source stamp、lineage。FREE 省略 field4，保留
counter；channel renewal/换 Owner 不换 slot。logical sequence 独立于 Kafka PID/内部 sequence。

首次 Admission 同批消耗 exact Claim/原 charge，保存 Message/Attempt budget、immutable
mapping、pending slot、SYSTEM/POSITION 首结果、Source frontier、实际 record delta 账本。
busy/exhausted slot 沿稳定 STALE 拒绝路径，仅推进结果/Source；不部分消耗 Claim、不阻塞
Source 等待 slot。只有实际 APPLIED/OK first result、映射及 pending slot 完整关联才产生
私有 Applied proof。新/历史 record 的冻结 Target accounting 以既有 STATE class 计实际
key/value 字节，不增加维度、不重复收费业务 payload、不重定价原 Attempt obligation。

Source definitive Outcome/Resolution 独立认证 evidence 后，在同批更新 exact pending slot、
Attempt/Message/结果及费用；UNKNOWN 保持 pending。正向只接受 Target K2 同事务
read_committed receipt，拒绝普通 Produce ACK；负向必须为 protected transaction/receipt
absence，不能以单个 target Broker rejection 清 slot。旧/重复结果仍走 immutable first result
和 retained attempt 分支，不能清掉后续 mapping 的 slot。逻辑 FREE 不解除 physical/zombie、
receipt/pin/query/Floor 保护。checkpoint full audit 核对 mapping→原 Admission/first/budget、
channel history、唯一 slot sequence、pending 或 actual settling Source result；semantic 原
Admission inputs 再比对 mapping 的 original signed bytes/Source，缺失/多余/替换失败关闭。

Target Kafka cursor 新 branch 保留 fields1/4/5/6/7/8，field8 为 TargetScope，field10 为既有
Kafka cursor member；不带 Lane fields2/3。旧 Target Pulsar field11 branch 及 Lane branch 不变。
Target receipt key=SHA-256(UTF-8 `nereus-delay-target-kafka-receipt-key` + NUL + mappingId)，
每个 mapping 独立 key；value 是 UTF-8 `nereus-delay-target-kafka-receipt-value` + NUL + closed
schema1 fields1–8：version、mappingId、logicalSequence、attempt、prepared hash、actual Source、
stable transactional ID hash、K2 binding hash。wire record hash 使用独立
`nereus-delay-target-kafka-receipt-wire-record` + NUL + lp32(key) + lp32(value)。成功证据复用
既有八字段 KAFKA_TRANSACTIONAL_RECEIPT shape，静态比对完整 mapping 及 receipt/cursor/LSO。
静态 decoder 不授予独立 Broker/retention/fencing authority。

已开发 actual K1 `KafkaClientArtifactTargetReceiptReader`：从 frozen scanStartOffset 读取，
一 poll/turn，固定 UUID/partition/read_committed、有限 records/bytes/time、explicit continuation、
真实 guarded record/LogAppendTime/LSO 和 exact key/value 校验。可找到 receipt 并返回私有
实际 Observation，恢复不依赖旧 callback/已知 receiptOffset。empty poll 不造否定证据。

本批 compileJava/compileRealKafka 通过，未执行测试。真实 K2 transaction first-send/资源
factory/driver、受保护初始空 range anchor、完整 fencing/absence 及 provider/startup/reopen
仍待连接，包1未完成。已确认原锁 K1 缺 logStartOffset 投影并丢失实际空 Fetch 证据；
主会话批准在既有 K1 worktree 最小 client 扩展、普通 fork 分支发布和开发锁更新，原
05849884 历史证据保持其原 scope，不修改 Broker wire或提前宣告新组合已验证。


## 2026-10-13：K1 同响应 log start 与 guarded 空 Fetch 开发锁

包1必要依赖已在既有 K1 worktree/既有 fork 分支完成并普通发布：
`nereus/delay-guarded-producer-v1@5128f46a358b5c3e9991cd5a51f2d1d0ef15339a`。
仅三处 client 路径：GuardedFetchEvidence 新 ctor/字段、AbstractFetch 同次响应投影、
ClassicKafkaConsumer 两处 guarded-only evidence 返回条件及当前 guard 复核。普通
Fetch.isEmpty/未 guarded poll、Broker RPC/wire/Producer API 保持。旧 ctor 明确
logStartOffset=-1 unavailable；新值参与 equals/hash。没有 lastEvidence cache，真实
timeout 的空结果无 response proof，旧 seek/assignment/error 检查仍由原 collector执行。

K1 `:clients:jar` 必要构建通过；该任务实际以 shadowJar 产物完成，jar task自身 SKIPPED，
不冒称额外 gate。显式 `-PcommitId=5128f46a358b5c3e9991cd5a51f2d1d0ef15339a` 使
kafka/kafka-version.properties 记录 `5128f46a358b5c3e`。当前 SDK JAR 为
`clients/build/libs/kafka-clients-4.4.0-SNAPSHOT.jar`，SHA-256：
`7adbb8e6d3bfa76fc62064d598d25ff00ee70cd90b27ddedbde5ed8f878146fe`。
这是 client 身份；Broker/image 未重建，旧 05849884 receipts 保持其原证据 scope。

Delay K2 reader 使用新 API，区分 null 无 response、实际空 raw/filtered response 和
普通非空 receipt；同次 logStartOffset/LSO/HW 缺值、矛盾或 logStartOffset 超过原
scanStartOffset 时失败关闭。新 poll 前清空本 turn range observation，不把旧响应
挂到 timeout；空响应仍不自动推进 nextOffset/LSO，也不产生 NOT_PUBLISHED。
实际 SDK JAR 的有限读取核对 version 来源并计算 SHA-256；此 hash 还须由实际
Artifact/eligible-reader/Materialization protection 与激活值核对，不由源码声明授予权威。
新 K2 range observation closed canonical image version2 的 fields1–18 为 version、
完整 K1 commit ASCII、cluster、physical topic、UUID[16]、partition、requestVersion、
raw correlation ID、broker node、session、fetchOffset、first/last raw record offset、HW、
LSO、logStartOffset、authenticated response SHA-256、actual SDK JAR SHA-256。数字
unknown 使用明确 -1，不默认零；原 persisted v1 evidence encoder/含义保持。

未运行 K1 单测、checkstyle/spotless、Broker 或 Delay 测试。原必要检查和新增
empty/timeout/旧 ctor/-1/seek/assignment/guard/position、原始或过滤后空批次、LSO及
truncation/compaction 的实际边界均留待集中验证。K2 transaction/driver、初始 anchor、
完整受保护否定与新 Owner 恢复装配继续开放；client编译不证明这些路径或生产权威。


## 2026-10-13：K2 受保护 fencing、anchor 与有限恢复扫描

决策主会话批准：retained mapping 的同 stable transactional ID 新实际 producer，标准
`initTransactions` 成功可以成为私有本地 Fence observation。当前 Owner/session/
credential、同实际客户端 cluster/target 与 receipt UUID 必须操作前后及产生结果时有效；
旧 Owner 不得重新初始化该 ID 的隔离前提保留。timeout/interrupt/guard 失效不产生 Fence；
不新增 InitProducerId wire，不编造 Broker 签名/epoch，不由 DTO/hash 授予权限。

先取得持久 retention/compaction/delete/pin 保护，再采样 actual guarded read_committed
末端空响应，fetchOffset=HW=LSO=scanStartOffset，raw/returned range 一致，才签入 v6。
保护必须覆盖采样到恢复义务解除的全过程，冷启动按持久 digest 独立重认证；空 callback
或单次 describe-config 不证明连续保护。KafkaClientArtifactTargetReceiptAnchor 已提供私有
实际采样与固定 canonical image，provider 的持久保存/重读/认证仍待组装。

扫描采用同 poll `records.nextOffsets()`，只在所有返回记录完整校验后接续；不使用事后
consumer.position、raw lastOffset+1 或 HW/LSO 推测进度。已确认 Fetch.add 会覆盖同
partition 前一响应证据；在授权 K1 fork 分支将 guarded collector 限制为一个 CompletedFetch
处理片段，每片可为部分批次或实际 filtered/aborted/control 处理。普通未 guarded 路径不变，
不新增公共 Producer/wire/Fetch evidence 字段。K1 `a707e84e023d0eb174fdf52c879dc1f9f1d6d2ce`
已普通 push；client compileJava、按完整 commitId 的 shadowJar 必要构建通过（jar task
SKIPPED）。SDK SHA-256=`da8b1d38ab40c34ba1d00ecfd18d37504941895d4cf9bce3c6728b36ec95040c`。
前序源码锁和 receipts 保持原 scope；Broker/image 未重建，不提升验证状态。

否定必须先完成实际 fencing，再从其后实际响应固定 U，连续处理原 scanStartOffset→U，
每段有 paired actual continuation，最终 LSO>=U；原区间始终有 deletion/compaction/retention
保护，同次 logStart 不得越原 start，当前权限有效。exact receipt 优先返回正向。空 poll、
无 proof、事务未稳定、保护缺失或无法跨重启认证均保留 UNKNOWN/pending slot。

Target 正向独立 transactional transport 只接受 actual Applied v6 mapping，不新写 MAPPED，
以完整 ordinary metadata 的业务记录及 exact mapping receipt 同事务发送；callback/EndTxn
不授予 PUBLISHED，必须实际 read_committed 匹配。响应损失/部分 SEND/失败 abort 进入
UNKNOWN，原 signed Outcome bytes 在既有单 flight driver 保留至实际 Source APPLIED；
slot/账本由 Source 原子结算。实际 factory 使用同稳定 ID、有限 SDK 超时及 caller-owned
I/O executor；不新增线程/队列。失败时只有实际 SDK teardown 完成才结束 physical stage，
teardown 失败保留未完成 stage/资源义务。完整 fencing/absence/实际 provider/reopen 仍待
连接，包1、集中验证及 productionAuthority 均未完成。Delay compileJava/compileRealKafka
通过，未执行测试、Broker 或格式/文档验收。


## 2026-10-13：K2 实际 producer 资源检查与 Source 恢复接线

主会话批准 K1 extension-only `GuardedProducer.requireResourceGuard`，默认实现明确
unsupported；普通 Producer、send/initTransactions 和 Broker wire 不改。不能以
waitOnMetadata 的缓存命中或全局 updateVersion 增长当作当前检查。实际实现只有一个
当前 pending guard，注册时提升原 requestVersion，派生时保留这次 exact request version；
走原 Sender/Metadata 的单 topic 请求且 allowAutoTopicCreation=false，匹配本次版本的
raw response 必须覆盖精确 topic/partition，错误/缺字段失败。返回该响应的 Cluster
而非共享缓存，使用原 validateGuardedCluster 比对 cluster/UUID；有限 max.block.ms
包括整个调用等待，关闭/中断/取消/迟到旧响应不授予权限。Metadata 的 protected request
wrapper helper 仅供这一内部继承接线，不新增公共 proof DTO、线程、AdminClient 或 waiter
缓存。未调用扩展时原 metadata builder/update 路径保留。

必要依赖 `nereus/delay-guarded-producer-v1@8087db6498c4f4d03eabfbfcb83bc6410872531a`
已普通 push；client compileJava 与按完整 commitId 的 shadowJar 通过（jar task SKIPPED）。
SDK SHA-256=`e443b062e6d717164c28b6acf2ba0c163878d7df83af7b09fdeb8262a297e9b9`。
前序 a707/5128/05849884 证据保持各自原 scope，Broker/image 未重建，未运行测试。

Delay 实际 factory 在同一新 producer 上检查 target/receipt，标准 initTransactions 后
再检查两者及当前 Owner/session/credential 权限，才为原 retained mapping 创建私有 Fence。
冷启动重新执行，不恢复旧成功标志。KafkaClientArtifactTargetReceiptRecovery 逐 turn
一个实际 poll；首个有证据响应固定 U=HW，全段用 paired actual nextOffsets，最终覆盖 U
且 LSO>=U、logStart 未越原 start、持续保护与权限有效才产生私有 absence observation。
实际 receipt 优先正向；时间 frontier 只折叠 U 前实际 LogAppendTime，空区间为保守空 fold，
不把 U 后新写入时间并入旧 cursor。失败关闭 consumer；失败 teardown 保留可重试关闭。

TargetPublishOutcomeStore 新增私有构造 KafkaUnknown，只读核对原 Admission SYSTEM/
POSITION、actual UNKNOWN first result、unreleased budget/obligation、immutable mapping 和
pending slot。原 admitted readRecovery 不放宽。原 recovery maintenance 在初始 ADMITTED
扫描之后，以独立 META26 continuation 有界发现 K2 UNKNOWN；actual provider 认证结果后，
SERVICE signer 生成原 EVIDENCE_RESOLUTION，外部认证后再核对同一 UNKNOWN snapshot。
既有 WorkClass/appender 保留 exact signed bytes，直到 actual Source APPLIED 后才清 pending；
原 Source v6 批次负责 slot/Attempt/Message/结果和双账本结算。没有新的 SEND/retry权。
当前 compileJava/compileRealKafka 通过，全部测试、Broker 和格式/文档验收留至集中阶段。

## 2026-10-13：K2 持久保护与 anchor backing 的归属

主会话批准必要的专用实现，归原 PLATFORM_OPERATOR/CONTROL 历史 key、角色、精确资源
scope 与 Registry 信任域；业务 Target/CREATE_CHANNEL 权限不自动覆盖任意 receipt。
Worker/新 Owner 仅为 reader/user，不签发或解除保护，不升格 KafkaReceiptSlotResource DTO、
ephemeral RecoveryPin 或其他 credential/native-policy 签名域。

在同一 Registry Oxia prefix 建专用持久 catalog，保护键为
`/target-kafka-receipt-protection/<exact-resource-hash>/<evidenceGeneration>`；resource hash
复用 ResourceRetireIntent 的 exact canonical resource 与 `nereus-delay-resource-identity`
域。anchor 键为 `/target-kafka-receipt-anchor/<scanObservationDigest>`，保存完整原 Observation
image 与已认证关联依据。均有有限版本化 bytes、原 issuer/signature/scope；put-if-absent、
冲突全字节比对、未知响应后 exact reread，成功发布也须 reread。无 TTL/ephemeral；handle
session fence 保护 I/O，保护本身不随 Owner/session 死亡结束。新 image/签名域/上限须在
实际实现时同步 PROTOCOL-REGISTRY 与 07；现有 Binding v1/Admission v6 bytes 保持。

真实管理入口须在排他管理权下先建立 Nereus 独占 receipt topic 的持续约束：禁止 compaction，
关闭时间/容量自动删日志，隔离 Worker 的 DeleteRecords/DeleteTopics/配置修改权限，认证
exact resource 和原 capability 耐久前提，再持久发布保护及采样/持久重读 anchor，才可签 v6。
冷启动按原两 digest 重读并核验同资源/epoch/scanStart/current protection/真实环境激活。
两次 describeConfigs、普通 signed blob 或空 requireCurrent 不能证明连续性；发现中断/
不可解释管理变化，旧区间不可倒补证明，UNKNOWN/pending 保留。topic 约束覆盖所有仍引用
它的 Shard/slot，不能单个 Outcome/扫描完成就解除。

解除沿既有 RESOURCE_RETIRE_INTENT→Floor/ProtectionSet→外部动作→RESOURCE_DELETE_CONFIRMED，
覆盖所有 attempt/read/control/query/checkpoint/shared-topic 引用并保留不可复活依据；具体
工具按包2/4实现。包1 catalog/provider 必须默认持久保留并拒绝无证据解除。当前这一真实
activation→保护→anchor→Admission→冷恢复重读链仍待实现，包1和整体 flags 不变；本条不
授权当前开发阶段改生产配置、执行实际迁移/删除或解除保护。


## 2026-10-13：K2 backing 最终边界及实际 Source adoption 开发

主会话最终决策终止逐级 grant 扩展：整 topic 管理 record 与原 protection/anchor 留在同一
持久 catalog，闭合 source-bound deployment authorization image 与原 CONTROL/root/scope。
当前 operator 与配置管理凭据才可发 Admin 操作；原 Prepared 仅为历史授权。持久
INSTALLING/installationID 和物理 capacity reservation 必须先于远端操作；exact实际
配置/ACL/UUID 成功后才 ACTIVE，历史 INSTALLING 请求不明或 BROKEN 不可复活。
Oxia CAS 不冒称远端 fencing，不把 Owner/session 过期当保护解除。

保护 primary 从原实际 APPLIED Channel Control 冻结 TargetQuotaIncarnation 取；anchor
primary 从首实际 Source Admission 的 Attempt owner 取。management 整 topic 属部署
物理预算；每份 slot实际artifact的内嵌字节只收费一次。新的独立 owner/ref 行允许，旧
Binding v1/Admission v6 bytes 保持。外部对象必须在 Store 外认证全 key/raw value/digest/
实际length，native same batch 写 owner/ref 与原业务/first/Source/双账本；replay 不重复。
checkpoint 由实际物理行与原外部对象独立折叠，禁止抄 counters。

发布在任何可能 Source enqueue 前持久 reservation 和原 CONTROL publicationID/scope/
有限 bytes/records/budget/phase；原 prepared Admission ID/hash/retry 在 enqueue 前绑定。
只有实际 Source APPLIED 私有 proof 可作 adoption。adoption 不删除物理对象，两系统不是
分布式原子事务。孤儿清退仍须封闭原 writer/retry、Source barrier、从未采用证明及原
Control/query/Floor/retention；包1先完成 publication→Source adoption→冷重读/保守持有，
包2/4才实现有原权威依据的解除/GC。

本批已实现上述持久 catalog、实际 Admin、原CONTROL签名/范围验证、原anchor typed
publication、受实际 Worker/Owner/profile/SDK 约束的 provider、runtime prepared bind→
APPLIED adoption 等待点、native NV47/NV48和alias计费，及checkpoint schema4原实物闭合。
冷factory/reader须对账实际mapping和SYSTEM/POSITION/owner/ref，再取得恢复资源权限。
未知写失败保留原 publication/同signed frame，旧v6缺owner/ref不授首次SEND，release API
当前明确拒绝无证据解除。

必要编译修正了现有 View/Reader 与 Budget primaryIdentity 类型适配；最新实际编译结果
见本批执行输出。没有测试/style/docs/Broker/性能/迁退 gate 通过声明，也没有当前生产
Admin/config/delete 动作。完整服务启动/物理资源与I/O关闭、集中验证、包2/3/4和全部必做
义务继续；all completion/production flags 保持false。
