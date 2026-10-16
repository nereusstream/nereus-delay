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


## 2026-10-13：P1必要SDK关闭语义修复范围

决策来源仍为用户授权的主决策会话
“调整 A–F 分区队列推进方式”（`01a13046-4fcd-7e90-8a56-dc2c3828fe23`）。
该会话明确允许在既有P1 worktree/fork分支修复Producer首次close completion、
PulsarClientImpl shutdown失败传播及client-owned ExecutorProvider实际termination，
scoped commit并普通push；Delay保存首future/failed handle，明确共享backend owner
quiesce全部用户后才可关闭原client。不得据Closing/isShutdown或再次close制造成功，
不自动关闭借用client，不为每个Target新建client。

实际P1 fork提交为`69b2a64e9359681919d4e7239cc79c1d37c0122a`，只有三处client类；
必要client构建和Delay main/real SDK编译已有结果，测试/真实Broker/style/docs门禁
仍统一后置。source/JAR身份与有限回归意图见12，原P1/Broker历史scope保持。实际
共享owner/多通道服务root仍须完成，包1、全部29切片及production均未据此完成。


## 2026-10-13：Host quarantine 的原 owner、permit 与故障退场

主会话“调整 A–F 分区队列推进方式”批准独立 whole-Host 故障入口及 Consumer 原创建
完成语义修复。范围为此 Host 实际自有 shared P1 client 及全部原 native 使用者；不
采用 borrowed/其它 Host 的 client，不新增每 Target client、grant 或故障管理循环。

先封住 Host admission/新副作用并建立 Store/runtime 本地写屏障，再 mint 私有 permit，
绑定具体 Host、backend、关闭 generation 和冻结注册集。不能等待全部 native future、
failed-open/read/ACK/SEND 已成功后才关 client；它们可由原 client shutdown 推动终结。
重复调用保留首 close/failed handle，原 cleanup 仍用原有限 I/O 池；只有 SDK 及本地
资源实际结束才释放相应费用。裸 closeClientBackend 入口统一交还实际 Host owner。

故障 BACKEND_TERMINATED 不代表正常 drain、Source/UNKNOWN/Journal 结算或 final
checkpoint 成功。保留原 entry/首结果、prepared mutation、checkpoint intent 与全部
credential/retention 材料；故障本地 Store 关闭不写 clean-close/假 ACK，lease 释放仍须
原 Owner 身份及实际 CAS。冷启动用 retained Store 或原受保护 Catalog/Floor+Source
和 UNKNOWN/Journal 材料恢复；恢复材料缺失则维持 fault/protection。

本轮 P1 SDK lock 为 b8fc1f39eb517c14cc1cdf35e60a0b1dcad2ec40；先在原 Closing/Closed
边界终结 pending subscribe/producer future，原错误与晚到句柄保持，不扩大 native
ACK/cursor/guard/wire/schema 或共享 cnx 策略。必要编译结果与有限验证目的见04/12。
当前 mixed Source 故障退出、完整 startup/config/provider 装配及其它三个功能包仍需
实施；全部完成 flags 和集中验证后置安排保留。


## 2026-10-13：K1 Source owner 与实际组件清理裁决

主会话批准同现有 TargetOwnedBackendGroup/Host private permit 接实际自有 K1 Source，
不增加通用group层：冻结新poll/ACK/cut、在wrapper锁之外wakeup、真实调用退出后关闭。
保留首close失败与原handle，business gate和native termination分开。必要SDK范围包括
GuardedConsumer/KafkaConsumer/Classic的最小API及已证实妨碍完成的Fetch、network/
Selector/SSL/SASL/helper；不变更Fetch/commit/group/guard/wire或重开consumer。

SDK清理按原组件记录，已成功的协调/leave/session动作不重发；原partial-construction
handle只能清理。无证明仍pending/计费，超时不取消在途任务，不并发替换cleanup。
P1关闭不代替Kafka，未ACK数据及原首结果/UNKNOWN/checkpoint保护继续供重放。

当前K1 fork source lock为0a8fa588fe4d365771f463335759dfc5dcc053fe；owned Source 已
接实际Worker/Host native gate，必要构建/编译和SDK普通push结果见04。完整service
provider/startup/reopen和资源envelope继续实施，有限验证目的见12，测试/Broker/style
仍后置，全四包、29切片和全部完成flags不变。

## 2026-10-13：实际 Broker mTLS material receipt

主会话裁定新 Target 生产装配采用本地 immutable-version mTLS resolver，不接受任意
Authentication/安全 Map 加自报 fingerprint。独立 verifier tuple 和叶证书 DER 指纹、
私有 qualified reference/版本目录、原受信 Broker probe、RSA/EC PKCS#8 配对与证书
策略按 Registry 新条目执行；旧 tuple、opaque binding wire 和普通/旧 Lane API 不改。
resolver 不授予 ACL，也不在 Worker 自签或伪造 probe。真实部署材料/信任/保护缺失
时 activation unavailable，测试/Broker/轮换与文件故障仍集中后置，不以 fixture 补生产链。

解析 receipt 由私有构造产生，SDK 只消费同一不可变快照。K1 inline PEM SSL 与 P1
AuthenticationTls 捕获流使用固定 server-trust/endpoint/hostname 策略；SASL/token/
OAuth/dynamic Authentication/PLAINTEXT 在此新路径明确不支持。Source/SDK opening
pre/post 与 SEND 保留原 Source/resource/Owner gates，新增凭据检查在 SEND 本地执行。
旧 UNKNOWN/Journal/cleanup 继续持有原材料，重新解析/续租/rotation 沿原 control、
Head/protection 和 SDK generation 边界，不引入新持久 grant、插件或远端 secret 服务。
本轮不读取/生成真实凭据，不执行环境迁退，四包与29切片及完成 flags 保持。

## 2026-10-13：Source deployment credential输入

主会话确认Source权限不能由Destination receipt、Route的IngressCredentialBindingRef
或SourceAssignment推出。采用独立私有SourceTransportCredentialAuthorization：原配置
明确授予既有精确verifier tuple/key Source用途，独立签名域、closed部署image与原真实
Source probe，绑定实际signed Route/resource/topic/Shard、材料版本、配置generation、
roles/names/window及TLS策略。字段号一次固定于Registry，不新增Profile/UseKind/Source
命令或assignment字段，也不新建Oxia授权服务/动态rotation框架。

顺序为验证Route/image→同版本真实mTLS材料→私有Source receipt→实际SDK guard/接受
assignment→原Owner/恢复流程→actual consumer/Worker/assignment/Owner/native generation
本地use。active poll/ACK/cut检查不读文件/Oxia，依然保留原Author/Owner/资源和System
签名权限；到期/失效沿原quarantine，cleanup及旧未ACK/UNKNOWN义务不随其解除。实际
system append/history/observer/recovery roles与完整service/reopen继续接线，测试/Broker
和环境激活集中后置，无真实凭据读写或新fake production默认。全部完成flags保持。

## 2026-10-13：K1 Producer native事实与Source writer

主会话批准复用既有Consumer组件cleanup事实，补guarded Producer有限清理、真实
termination和private partial-construction handle；范围含同SDK的Source/BASELINE/K2。
保留普通Produce/事务/guard/wire；先封业务、单一deadline观察graceful/force和self-close，
Sender未退出时不由owner并发关network/accumulator/metrics。Sender退出与资源完成
分开；原失败/组件清理/constructor中的NetworkClient归属和晚到handle保留，只有真实
native completion释放SDK费用，Source/UNKNOWN/Journal/持久mutation义务不因此删除。
不用新清理pool、替代producer、重复SEND/commit/abort或第二次noop当证明。

开发K1 fork推进为5dbfa3863749669462f7a06993b1708e0c56300c，JAR hash与必要构建/编译
见04和progress开发锁。Source writer用原OwnedBackendGroup/Host permit和SystemWriter
预算，configured core验证实际Source appender身份，正常Host只在各Shard final drain
完成后关这些backends。history/observer角色使用真实Source receipt，不据该局部接线或
编译关闭包1/29切片。recovery/provider/service完整装配继续，全部测试及环境动作后置。

## 2026-10-14：完整启动的原内容/历史与capacity输入

主会话批准deployment-owned原内容和历史权威作为必填启动依赖：可信进程root提供原
生产provider实例，或受控有限immutable原key/issuer/role/tenant/Route/scope/window输入
由具体验证器加载。禁止用户配置反射class/script；不另建auth服务，不使用noop/fixture/
Source transport receipt补内容权限。当前执行Owner不能改写历史作者/key/Source窗口。
root实际创建/接续Source、Oxia session/原Route/catalog/Owner/assignment、Store、SDK组
与Host，派生局部Store/Owner guard；不以caller预造的完整runtime代替启动实现。
外部I/O在Store锁外bounded collector中形成immutable已认证snapshot，commit仅检查
持有的版本、来源、reservation/pins与保护；缺具体运行输入则明确拒绝相应能力。

CapacityAuthority当前没有完整现成后端。选定“真实完整且受保护的capacity snapshot
来源＋仓库具体验证器”，不新建allocator、不从local hardCaps/评分/单assignment推导。
11 §11.2的完整tenant cut、grandfathered usage、原Control/prior/next/allocation、真正
non-borrowable physical reservation、parent完整old/new/step及donor Source shrink/drained
先行都必须落实。provider负责原记录、完整域/absence、原policy/parent hash与实际保留
状态的认证；业务验证器不能只接一个ref/hash/Boolean授权。当前新增snapshot验证器
与paired grant/commit入口，真实collector、原内容provider和整条root/reopen继续开发。
只做必要编译，全部集中测试/Broker/凭据/环境迁退仍后置，完成flags不变。

## 2026-10-14：原Owner/session fencing查询保留

主会话收窄“所有外部I/O锁外”的表述：新增内容/历史/key/role/Profile/Artifact/capacity
材料化继续在有限collector、Store锁外执行，提交消费已取得的snapshot保护。原
TargetSourceApplyRuntime.guard/gcGuard→requireOwner→Oxia current，以及session marker/
原assignment fencing检查，允许保留现有guard位置的必要远端读取。不得删成cached
Owner/截止时间、watch connected或deployment Owner=true；不声称native commit零远端IO。
调用失败、主动fence/失权、超时和写不确定性继续原fence/恢复，未ACK/UNKNOWN与已
提交义务不释放。根入口必须配置真实client有限request timeout；Owner renewal竞争
重读在原操作deadline内最多四次，不开启无界锁内retry。此为保留原正确性边界，不
增加wire/grant/租约服务或新“本地Owner权威”，性能/原子性保证不因此提升。

## 2026-10-14：Oxia 0.9.0原资源生命周期补齐

主会话核对原SDK后批准在既有nereusstream/oxia-client-java fork的独立工作树/分支，
从v0.9.0@ce8143e06bcb089a2916c8ce4bf64b40c1d4d5bc实现最小本地生命周期补丁。
Delay仍单agent、直接main。不另建进程监督系统，不把未知进程终止provider或永久pending
作为完整root方案；不改变Oxia服务端协议、session/Owner/notification/CAS契约。

Sync/Async、opening/partial/late allocation保留同一真实cleanup owner。一个组件失败
仍尝试其它安全项，首失败不因closed=true/noop遗忘；原executor、transport、batcher/
stream/callback用实际退出/termination判定，不以shutdownNow/isShutdown或线程名证明。
统一有限观察预算，callback不等自身，未退出的首任务不并行重开cleanup，pending费用
保留；全局共享运行时不归本client关闭。退出不证明远端写未发生、marker已删除或Owner
已释放。应用Owner/Route构造和replacement同样保留原client及cleanup，原权限依赖退场
后才能关闭authority。具体root依赖图、费用envelope和集中验证仍是原必做项。

允许该专用分支scoped commit/普通push origin，禁止upstream/release/缓存JAR覆盖。
交付必须有精确source与client/API SHA锁，compile/runtime采用相同受控组合；历史O1/
Broker receipts不因新SDK编译重标。必要构建/编译已执行，其它测试及环境操作仍后置。

## 2026-10-15：P1 定位、独立 reader 与显式可重启续代

主会话批准补齐 P1 seek 换连接的真实启动路径。C 必须来自真实 publication/Route/
session 获取的 Owner 下、经物理保护和 root 审核的实际 Store；checkpoint 先有真实
Catalog/RecoveryPin 与认证安装，新建先有初始化成功的原子根。禁止无租约读 cut，
禁止用 C 替换作为恢复目标的 Route barrier B，禁止删除 generation/sameIdentity 检查。

Target 独立 NonDurable/no-ACK/RECOVERY reader 可绑定 exact assignment、Owner/session、
原 managed group/consumer、C 和 post-seek proof。逐 entry 持续检查，连接改变即废止，
不产生 assignment、ACK、普通 ACTIVE apply 或服务权限；旧 strict/兼容路径不放宽。

恢复后先冻结能改变 C 的 Source/recovery/Claim，等待原 I/O/回调退出，保留 Pin、
UNKNOWN、Journal、费用与 cleanup。已暴露 Source 先 fence exact Owner并证明旧
apply/ACK 退场；ACQUIRING/CATCHING_UP 只接受私有实际静默证明，不假激活、不接纳
未完成 RESTORING 或 caller boolean。FENCED 不能获得普通 Store 写权限。

同资源 owner 创建并保留最终 consumer，按 C 定位后构造显式 reactivation：稳定
B/resource/Shard/topic/batch/attestation、Worker/capacity/Route binding 不变，换
assignmentId、提高 assignmentEpoch/placementEpoch。实际 authority exact revision CAS
发布；不确定只重读 exact successor。旧 Owner 静默后释放，新 Owner context-bound
获取；SDK proof 不授发布权限。

批准同一 CAS record 的明确 v2 envelope 绑定 signed Route 根、前后 exact assignment、
前一 publication revision/digest 与 successor；固定根及最近一步，不无限追历史或
只依赖内存 FencedPlan。初始/旧格式仍 strict；各 Route-bound 接受入口复用核验和
reader eligibility。Source/Worker canonical bytes 不改，新格式同步协议登记。

新 Owner 下重开/重核 Store仍为 C，重建正确 Owner 的保护、元数据和必要恢复收尾后
才激活。旧 Pin 在替代保护实际成立前保持，不改 owner字段。最终 factory 复用 exact
定位 consumer及原 opening/cleanup，不再 subscribe/seek；失败/重连走显式有界续代。
完成条件是启动、恢复、连续续代及重启链源码贯通；组件编译不关闭包1。集中验证后置、
四包/29切片和所有完成 flags 保持，不使用 Fast。


## 2026-10-15：STAGED repair 的持久来源与发布互斥

主会话裁定：必须在首次复制/改写候选DB前耐久化有界独立版本intent，绑定原published
Manifest/lineage/Source/dbIdentity、目标UUID及原Owner/session/Pin。旧无intent残留保留
并拒绝接续，不补造来源。intent只是出处/阶段证据；repair仍需当前真实Owner/session、
Catalog和新Owner自己的精确有效Pin/Floor/Source保护，origin不改写也不要求旧权威复活。

所有Target ACTIVE发布入口共用真实跨进程root安装互斥；普通open只按有效ACTIVE，
fresh明确进入，不扫描选唯一/最新目录。repair只看intent UUID的restore-tmp或incarnations
路径，双份/冲突/无法证明未暴露则拒绝。原image必须重新下载完整认证，逐CF/key/value
有界核对，仅允许按阶段计算出的精确metadata改写，不能整体跳过META/runtime/recovery。

顺序为intent耐久→认证复制/精确metadata与WAL sync→move/父目录fsync→当前保护重验→
ACTIVE与父目录fsync→PUBLISHED耐久→交出INSTALLED Store；PUBLISHED不是Owner ACTIVE。
同UUID ACTIVE且receipt未完成走独立完整重核/收尾，外来ACTIVE或已PUBLISHED却缺ACTIVE
拒绝，未知发布保留并重读。坏候选保留失败，不修到相等、不删除未知/竞争目录；故障
保留原句柄、计费、Pin和termination责任，native关闭不等于残留磁盘已清退。本次只
开发与必要编译，测试/Broker及真实repair/删除后置；四包/29及总完成flags不提升。


## 2026-10-16：K2历史授权与当前整topic管理权限分别核验

主会话裁定不将原Prepared作者的actor ID、整个role-set hash和tenant-resource-scope hash
永久绑定为当前operator。历史MANAGEMENT严格核对原注册Prepared全bytes、原authorizedAt
Source及其key/signature、HistoricalActor/原三哈希/原最低角色与历史ReceiptScope；
历史验证不依赖当前身份、角色或当前scope是否仍可用，不授Admin/SEND/absence权限。
ControlOperationAuthorization原语义和独立新CONTROL请求的真实当前作者/注册协议不变。

当前动作另经root实际live authenticator的一致identity/policy snapshot；保有PLATFORM
角色只是必要条件，还须自己的subject/role/scope/version证明覆盖同一完整management
image和请求动作。受控immutable current policy可将当前scope/subject绑定到精确整topic
及动作，不以一个hash、任意true回调、历史Prepared或自填context替代认证。image完整
绑定tenant/controlScope、deploymentRoot、cluster/topic UUID/physical name、epoch、
capability、credential fingerprint及principal隔离。stale/revoked/unknown拒绝；政策更新
由可信配置/认证器版本接续，不静默重读任意mutable路径，不另建IAM服务或grant链。

获准新operator可使用同一exact ACTIVE epoch；不改历史作者/Source/signature，不更换
management image/credentials，不重跑Admin掩盖历史间隙。每个真实Admin副作用前后及
后续ACTIVE publication/CAS前重验当前授权和原epoch。当前授权单纯失效仅拒绝当前动作，
不冒称物理保护已BROKEN；真正物理策略/UUID/ACL连续性仍按原契约核验，INSTALLING不明
请求与BROKEN不可复活。原key/signature、私钥持有preflight及Worker无Admin/key边界保留。

实现以具体current scope loader/verifier接ManagementFactory、backing和catalog的begin/
activate/break/protection/anchor写入；历史链不再调用current gate。此决定不单独关闭
包1/29切片，仍只作必要编译，当前授权变化、错资源/动作、失效与历史独立性和不复活
等有限交错留集中验证；不授权当前执行实际Admin、部署或迁退。


## 2026-10-16：K2精确源码锁接续当前受控K1

K2新reader、anchor与同ID fence观察使用既有受控K1分支的精确提交
`02c9f768d6f77cb4860bada42cb22917914d2981`；实际JAR metadata的commitId必须为
`02c9f768d6f77cb4`，不接受旧提交、unknown或任意新提交。原provider继续精确比对
实际JAR SHA256与已激活Artifact，本次使用的实际JAR为
`4ec46de695c6ce30778d5a6e5b0bb8f13405fa271c54b067dbe54137fc4cb233`。
新观察按原字段记录新source及实际JAR；旧观察、原Admission/mapping/channel、
capability/sourceLockDigest及历史Broker证据不重写、不重标。源码锁更新不是部署
激活，真实环境仍须按原协议激活对应SDK/capability/Artifact组合；没有兼容allowlist。
独立Target RECOVERY授权与旧执行凭据过期后的实际恢复仍是未闭合代码义务，不能用
本次源码接续绕开旧gate。只作必要编译，集中验证和全程序完成flags不变。


## 2026-10-16：独立Target RECOVERY部署授权

主会话裁定沿原可信deployment/credential-verifier root加载独立签名的Target RECOVERY
授权，不新增CONTROL operation、grant链或CredentialUseKind。原Channel/Admission/
mapping/receipt及旧lease在原使用点的历史合法性保留；恢复当前有效性只核独立恢复
材料，不能要求过期旧SEND secret复活，也不改写原身份或旧义务。Source RECOVERY与
普通Destination SEND attestation不能替代恢复目的证明。

签名域、显式TARGET_RECOVERY issuer/key tuple allowlist、部署/config generation、
tenant/controlScope、原channel/Target/receipt native identity与topic/partition、原
transactional.id、当前恢复credential version/ref/binding/fingerprint/principal、
TLS policy、恢复专用probe/attestation、有限时间和当前失效/撤销均须分别验证。签名可
限一个channel/receipt/transactional.id，无需逐Owner/逐mapping签CONTROL；若配置含
mapping digest则不得扩大。实际root读取认证原pending mapping后，私有runtime receipt
还须绑定exact mapping/scanStart/anchor/protection/Store cut/当前Owner/session/SDK
group与本次opening，排除合法在途writer。该实际runtime/typed SDK接线继续实施。

恢复只允许guarded lookup、同原ID initTransactions fencing、原receipt read_committed
及关闭；prepareBinding、新anchor/Admission/slot和所有SEND/transaction commit均不可达。
typed恢复工厂不得返回正常SEND transport/raw producer。每个副作用/poll/Source证据
消费前核实际Owner/pending slot、当前恢复权限和原连续保护，失权/超时不生成否定结论。
原same-ID fence→post-fence固定U→实际连续处理/LSO/保留范围→Source Outcome链不变。

原历史证据继续由原Source/Store/Catalog/ProtectionSet/Floor保留；当前恢复授权/实际
probe/immutable材料由真实deployment material authority持有当前权限与retirement
hold。hold直至实际SDK/晚到I/O/cleanup退出后显式关闭，授权过期不自动释放；不能借
旧managedChannelProtectionUntil、ObjectStore lease或内容hash代替。崩溃后新Owner
重新取得当前恢复材料/保护。公共恢复证明保留安全投影和原受保护证据ref，私钥、原始
私密路径/请求不写Source/日志；内部Fence/observation需要时用显式新版本。

当前只完成独立签名codec/trust与专用mTLS加载/receipt及security配置入口，尚未完成
上述私有pending-runtime receipt、typed无SEND SDK/resource/service接线与公共证据保留
链，不能将loader编译视为旧lease过期已可端到端恢复。具体实际authority实例仍须由
原可信root提供，没有默认noop/true；材料/hold缺失拒绝。只必要编译，集中验证后置。


2026-10-16独立恢复裁决的runtime落实：新增typed无SEND原SDK资源和实际Source
pending snapshot，原Worker登记先于material/native分配。同stable producer identity
在原SDK实际退出前不能登记竞争恢复/正常SDK，实际本地publish flight也不可被反向
fence。材料加载前另核签名deployment/tenant与actual Owner/Source tenant；可信配置
自身一致不替代实际realm绑定。原Profile/binding只作历史合法性，新材料独立当前
授权/hold；每个guarded metadata/initTransactions和poll持续核Owner/pending/材料/
原catalog/anchor连续保护。私有Fence显式v2嵌入安全恢复公共证据和实际Store/cut/
预算快照，旧v1原语义不变；scanner仍按原固定U及完整Source Outcome契约。

已接K1 accepted Source Opening同process management的openReceiptRecovery和通用
Root factory；Host通过原PublicationResources保留实际opening/SDK/reader/cleanup，
当前恢复receipt不能由普通SEND材料代替。actual private observation有原resolution
authority/evidenceImage入口，但不因此授SERVICE签名、Source写入、归档或新SEND。
公共证据durable provider/ref、完整SERVICE/Source结果组装和typed恢复后的正常工作
接续仍继续开发；真实部署authority/材料仍是必填前提，全部集中验证及29义务不减。


## 2026-10-16：独立durable恢复证明与Source原子归属

主会话已批准在同receipt Catalog/Root增加独立RECOVERY_PROOF intent/object，
明确区别CONTROL PROTECTION/ANCHOR，不新增CONTROL operation/kind。原Root/Intent
保留旧canonical语义，新分支/版本显式辨识；对象内容寻址/不可变，完整绑定原
mapping/Admission/attempt/channel/producer/receipt/protection/anchor、原Evidence
及evidenceId、Fence2/实际范围观察、公共恢复授权/材料验证输入、原观察Owner/
session/可信时间及SDK/能力/算法。原SERVICE/明确recovery-verifier用途issuer/key
版本以独立proof签名域发行，只能实际typed native receipt及完整关联产生；历史
认证按原时点，不要求旧Owner/凭据今天有效。不得归档私钥或原始secret。

reserve→put/exact authenticated reread→签proof-ref Outcome→seal prepared use→
enqueue，不反写后来Source position。mapping+evidenceId固定发现索引且绑定完整
proof digest，同evidenceId不同proof不能覆盖；完整认证已选对象后复用或冲突。
新版proof-bearing PUBLISH_OUTCOME/EVIDENCE_RESOLUTION body带exact有限ref并须
显式tuple/reader支持；旧body、PublishEvidence/evidenceId和Admission/mapping不
重释，UNKNOWN不伪造definitive ref。Source锁外取完整认证snapshot，锁内核原
pending/first/ref，在首次有效APPLIED原子写结算/slot/attempt/first/POSITION/
Source、proof owner/ref和账本；Source真实APPLIED私有adoption才推Catalog。

费用归原Attempt冻结incarnation/tenant/lineage/policy，实物唯一EVIDENCE=1/
完整UTF8 key+raw signed bytes+overhead，实际local owner/ref/index按STATE；剩余
reserve/独立ledger/Checkpoint/restore/read bounds同补。新Admission预留有限
上界，旧冻结预算不足先拒绝并保留UNKNOWN，不自动补差。NV48子标记3是原anchor
alias，不可冒充新kind3；新增记录不伪造CONTROL provenance。物理staging/adopted
额度保留，全部引用经原ResourceRetireIntent/Source/Checkpoint/Floor/ProtectionSet
和实际删除确认后才清ref/owner/费用，未采用也须证明所有prepared use确定终止。
本项尚待实施；只必要编译/既有文档同步，全部集中验证后置，完成flags不提升。

2026-10-16格式实现选择：proof-bearing Target K2 Outcome/Resolution使用独立
body generation7（field9=7、field19必填exact ref），区别shared tuple2和原
Admission6；旧无marker body不变。Ref v1/type1独立命名空间，key上限2048 UTF8
bytes、proof上限4MiB，须另做真实容量/新Admission预留，不能推导旧预算已足够。
当前只完成codec/required tuple与Source拒绝采用边界，完整历史proof snapshot、
Catalog/Native首次原子owner-ref及账本未接通前不开放v7，也不提升任何完成flag。


2026-10-16实现落点：独立proof v1/algorithm1闭合19字段，保存原mapping/signed
Admission/evidence、实际observer/time/SDK、扫描及公有材料、tenant、目的issuer/
key、capability/Attempt/冻结incarnation、digest/signature。typed新观察schema3/4
保留有界逐段Fetch/continuation/time，旧普通schema1/2保持。签发只接actual runtime
私有观察；历史核原目的key/time/material配置及PKIX，不核今天旧Owner/credential。

同Catalog Root2/repeated10独立RecoveryIntent共享原物理预算，先预留全部未来
引用窗口，再不可变写/精确认证回读。固定mapping+evidence发现ID，冲突拒绝；崩溃
只对实际原选中blob完整认证后续接。Source frame ID/hash/retry seal先于enqueue，
私有publication/prepared token不等于Source APPLIED，不提供DTO式ADOPTED/GC入口。
归档前原Attempt必须至少支付anchor+proof实物，完整Native STATE收费仍待实现。
部署Root/signing配置、Source完整原子owner/ref/账本、Admission预算及Checkpoint/
Floor退役继续；v7 Source采用与typed旧内存resolution均拒绝。此为实现进展而非
包或全目标完成，集中验证后置，所有完成/生产flags保持false。


2026-10-16 Native恢复组件实现选择：独立NV49/meta42 owner和NV50/meta43 subtag1
引用/subtag2 Attempt alias，原NV48 subtag3不重用。记录原Admission/冻结Target/
观察者摘要与实际Outcome/Source provenance，不修改外部proof、不伪造CONTROL。
三条edit加入原definitive结算原子批次；Catalog ADOPTED只接Native guarded实际
APPLIED first/POSITION/owner/ref/slot/Attempt读取产生的私有token。Native独立
STATE/唯一blob费用与原Attempt reserve按actual adopted share转移，RELEASED后
proof/ref仍收费，部分分配不得发生减除下溢。新K2 preparation/fresh Admission
须fund实际anchor+有限proof/STATE上界；旧冻结预算不足先拒绝且不改预算。

补齐既有NV45–48的reader ceiling及META40/41映射，并注册49/50，无自动部署/
reader/tuple激活。独立fold核完整Native引用/原first与mapping/anchor/Attempt，
semantic digest追加恢复owner；没有恢复owner时旧字节不变。checkpoint完整proof
伴随/历史及部署Root Source装配继续实施，旧semantic companion遇新owner拒绝
发布，v7采用门禁保持；这是原包1组件实现，未关闭任何全程序完成/验证/生产flag。


2026-10-16 checkpoint实现选择：semantic schema5新增重复field15(完整exact key+
raw purpose-signed proof)、digest16，旧schema1–4不重编码。完整性关联实际Native
owner与所有Outcome first/原signed inputs，APPLIED v7必须有exact proof，REJECTED
不造adoption；schema>=3统一关联Outcome，补齐schema4漏检。历史认证通过原Catalog
实物+原prepared seal的私有snapshot，不依赖旧Owner/SDK/credential今天有效或
Catalog已ADOPTED。原完整Source/图像/pin/Floor/history保护仍为实际部署义务。

configured collector接新证明，candidate/Manifest/restore根检查要求完整schema5，
完整外部对象审计兼核CONTROL/recovery，旧CONTROL-only API有新owner时拒绝。预算
沿用现有有限semantic/read/time边界且proof I/O前核exact长度，不隐式扩大。核心
Source v7临时全面拒绝替换为显式tuple7+原签名+完整私有Catalog snapshot门禁，再
走已有Native原子归属/费用；无内存旁路，无自动reader/Artifact/tuple激活。Root
目的key/config与实际归档/Source pipeline、Floor/受控退役和集中验证仍开放，
所有完成flags保持false。


2026-10-16 Root实际归档准备：私有TargetArchivedKafkaResolution只在真实Native
UNKNOWN、实际Catalog publication、SERVICE v7签名与durable seal完成后生成，
maintenance拿原签名字节重读UNKNOWN再handoff，不做Catalog IO/不保留SERVICE
signer。K1 Opening绑定原startup/Worker/catalog/protection，现有recoveryHistory
只装饰匹配UNKNOWN，初始hold/其它row仍走原history。沿用原有限IO池推进SDK开/
finite scan/原proof认证复用或签发发布/签Source/seal，原Root provider/scan
bound固定，复用同样核原archive内存窗口。future追踪各原Native handles与同池
continuation，Stop/取消/拒绝/cleanup不伪造退出，不借新retry句柄；close等实际
退出再退SDK预算。修正新resolution签名logical ID按既有body注册规则，旧数据
不改。部署key/config、实际APPLIED自动adoption/重启reconcile、完整正常接续/
失权及Floor退役继续；只必要编译，全部完成/生产flags保持false。


2026-10-16 Source completion落点：Root私有回调在真实first后原IO池核Native
Adoption并CAS Catalog，Worker无Catalog I/O，不需旧SDK/material仍active。
APPLIED必须exact owner/ref/alias/Attempt/first/POSITION/slot；REJECTED不采用，
只凭真实不可变first重备signed image并按actual terminal first释放原queue image。
状态区分RECONCILING/SOURCE_REJECTED，known first不重复handoff。normal reopen
先等原completion，Stop也等adoption实际退出，不因view/cache变化提早退SDK。

live admitted checkpoint provider可用当前Native实际记录补记Source/Catalog崩溃
间隙，原Observer/credential无需存活，bootstrap仍历史验签。该只读跨原checkpoint
不授新Source/SEND，不是孤儿扫描/never-adopted/退役权限。物理与逻辑费用保留；
部署key/config、全Stop/失权/接续及保护Floor到实际删除仍开放，只必要编译与既有
文档同步，全部完成/生产flags不提升。

2026-10-16部署签发输入沿原receipt management owner、预算和I/O池落地，不另建
进程资源框架。RECOVERY_PROOF目的issuer/SDK清单和Source SERVICE key/window显式
配置，原材料历史仍由实际Root provider提供；文件身份/公钥匹配不授Source或
材料权限。预算先于新key文件读取，签名器前后检查停止/key窗口，关闭清新私钥
引用但保留历史验证语义。Opening仅包装原history/bootstrap/live collector，
不向Worker暴露私钥/factory，不自动激活tuple/Artifact/reader。必要编译之外
继续后置集中验证，完整部署、接续/Stop/失权及保护Floor/退役义务不收窄。

2026-10-16有限池cleanup暂满以原caller turn继续：保留实际旧handle/cleanup义务/
费用，成功接受新task才替换跟踪，不另建重试队列/线程或在Host tick内inline close。
适用于共享OwnedCheckpointIo retry和实际BASELINE/P1 adapter、K2 receipt/typed
recovery close提交；shutdown及真实close失败不按暂满处理。正常successor仍等实际
退出，不放宽原UNKNOWN/Owner/Source/保护条件。必要编译之外验证继续后置，完整
服务装配和所有包/29切片及退役义务保持。

2026-10-16 shared SDK退场由原调用轮次保证推进，回调hint只是唤醒机会：Host直接
replay-exit在实际barrier后复用原permit重试close，沿normal drain/quarantine同一
monitor外方法；未成立barrier不投close，状态查询仍只读。P1 hint/teardown提交的
活池暂满保留原owner/句柄/first close/费用，shutdown和原生执行错误继续保留。
不新增后台重试/队列/线程或关闭权限，不弱化真实SDK/users/participants退出和
Owner/Oxia/Pin退场顺序。集中验证继续后置，完整四包/29切片目标保持。

## 2026-10-16：Target外部证据对象独立退役裁定与身份开发

授权主会话已裁定新增ResourceKind8 TARGET_EXTERNAL_EVIDENCE_OBJECT，内部封闭
CONTROL_PROTECTION/CONTROL_ANCHOR/RECOVERY_PROOF，不借其它七类或CONTROL/SERVICE
权限。身份绑定真实Catalog/deployment namespace、Source Shard/tenant/原lineage、
exact key/type digest/raw SHA/length/后端独立version、原冻结计费owner/accounting及
原创建/首次Source adoption。当前executor/Owner/session和mutable uses/ref状态
不进身份。未采用staging明确独立分支，绑定原durable publication intent与不可变
origin、deployment physical owner/原授权/创建时刻，不虚构NV47/NV49或Target费用。

必须先由原Root CAS closure封住该exact对象的新publication/seal/adoption引用，
保留完整真实uses及revision/digest。每个prepared use逐个证明永久不能future-first：
exact已APPLIED/REJECTED且保留首事实，或原协议认可的Source有序期限/关闭和迟到
禁止事实；墙钟过期、Owner失效、取消、未找到、无ref/删uses台账均不构成证明。
UNKNOWN enqueue/晚回调继续保护。Target intent锁外认证完整object/closure/protection，
锁内核当前native state/expected version/所有owner/ref/alias与uses结论；intent/
closure/first/POSITION/Source/STATE费同批，此时不移owner/ref、不减费、不破坏合法
历史读取。

物理删除前：真实Source APPLIED intent与closure仍匹配且封住新增uses；业务/重放/
pending UNKNOWN/Journal/Attempt均闭合；完整跨mapping/Target共享引用核完，活channel/
slot还能产生新protection使用时不可退它。原Catalog parent-hash Floor覆盖intent/
引用关闭cut与sequence，所有合法可选checkpoint、candidate/observed Floor Pin及
query/audit/history/replay/retry/export/control保护按ProtectionSet逐项释放。当前
Root/Owner/session/目录权限、占用和owned I/O有效，实际读取/回调退出；未知或预算/
列表不完整拒绝。单个结束Attempt的anchor/proof可独立回收，不需整个slot退役。

Floor base必须能在原blob删除后独立认证恢复“待删除结算”的retire/closure/仍计费
owner/ref；优先schema5自包含材料，必要时最小显式扩展。不得恢复时仍从待删key
取blob，也不得提前移owner/ref/退费解除循环。ResourceGcGuard只用于确认后墓碑
压缩的必要条件，不能冒作删除前授权。

外部adapter只条件删除这个exact Catalog object；删除前重读raw/hash/length/version，
不做prefix/batch或Kafka DeleteRecords/DeleteTopics/slot rotation/父保护解除。
不确定保留原request责任/no-recreate barrier。Source confirmation绑定exact intent
ID/hash/resource/state version/实际request/response/观察时刻，严格在intent之后；
ALREADY_ABSENT必须原已认证closed intent下的真实authoritative absence。只有首次
有效APPLIED confirmation同批移对应闭合ref/alias/owner并按原冻结owner精确退
EVIDENCE/实际STATE，保留墓碑/first/Source/账本。重复不双退，冲突/UNKNOWN/REJECTED
不退；物理已删而确认丢失仍保守收费并对账原请求。

Catalog closure/uses永久结论/delete墓碑自身保留计费，待原Floor覆盖intent+
confirmation、全部保护退出且no-recreate/no-future-first保持后才压缩。Target新
handler必须接accounted native path、独立ledger fold、checkpoint/restore/reopen/
history与有限GC WorkClass，不借旧DelayShard投影。本轮仅落精确身份codec/旧body
拒绝门禁；完整closure/writer/checkpoint环/实际delete仍待实施，不关闭包1/2或全目标。

2026-10-16引用封闭的首个实际入口：原Catalog Root schema3持久化exact raw/version/
immutable origin、完整prepared ID/hash/retry集合摘要、closure revision以及同一CAS
cut的全部候选publication清单和Root摘要。新publication/seal/依赖引用在同一Root
CAS检查封闭；已封闭集合不能追加或变更。独立当前action10显式授权，历史签名或
旧管理动作不授予。当前原management Opening只接真实Native APPLIED recovery
Adoption，沿原预算/有限I/O执行、Stop等待实际退出；不开放任意key封闭入口，避免
冻结仍需重建Source image的UNKNOWN proof。CONTROL/shared/staging实际终止预检仍待接。

候选清单是cut时完整候选上集，不等于真实共享依赖已释放。原sealed Native APPLIED
补记可继续完成原义务，不是新adoption/seal许可；closure不阻止已发远端put迟到，
不替代no-recreate/per-use永久结论/Floor/Pin/原子退役链。原blob、owner/ref、uses与
新增closure物理费用全部保留，元数据在blob缺失后仍可认证，但缺失不证明删除成功。
本批完成proof引用封闭组件；完整外部证据生命周期及四包/29切片仍未完成。

2026-10-16已约定Source有序期限的实现依赖：Target三类publication帧此前缺少
first时未检查原Native ingress fence，且外部authority准备可能先要求待删blob。
现复用System replay前置Native路径，fence覆盖deadline才原子写过期REJECTED
first/POSITION/Source/根费用；direct first store也先核同一水位。保留first仍走
原重复路径，开放窗口完整验权。此拒绝不授业务或删除权限；没有把水位查询、
首结果或缺blob单独升级为全部逐use终止/GC/退费证明，其余退役门禁继续实施。

2026-10-16完整uses的Native准备组件：只从真实Catalog ClosedObject与原Native
Adoption进入，原guarded read复读owner/ref/alias、resolved Attempt、mapping/Admission
及冻结计费，每个sealed ID/hash/retry逐项核actual retained first/POSITION或已提交
fence；实际采用该proof的APPLIED first仍须在场。输出私有构造的准备快照，绑定
Store/source cut/fence与完整结论集合，没有caller subset或无first即成功分支。
Source intent尚须再核并持久保留义务，本快照不独立授永久结论/退款/删除权。

原management Root有界owned I/O前后回读同一Catalog closure，串行于封闭操作，
Stop等待实际退出。配置/SDK构造仍用原assembly gate；新增registered read gate供
Host转移后的此类Native/history读取及原live checkpoint adoption补记使用，核实际
Root注册/ready/Stop/失败/转移状态，不恢复构造/SEND/Source签发权限。完整Source
退役/确认、独立ledger、删除后checkpoint、保护/no-recreate与集中验收继续开放。

2026-10-16实际Native接线发现kind8 codec把原recovery lineage误设32 bytes；原
NV47/NV49冻结owner固定16 bytes，adopted分支不可能通过相等检查。构造/解码及
Registry已统一为原16-byte身份，无实际激活或迁移；仍待专用Source/reader接线。

2026-10-16主会话补充持久格式裁定已下发：field4明确为原历史已认证management
deploymentRootDigest[32]，不是固定长度的原Owner deploymentId，不自行hash/截断。
批准专用Target retire/confirm body generation8，只认kind8；原10..13 nested语义/
System envelope/旧1..7保持，Source tuple/Artifact/reader须单独准入。新增签名字段
绑定closure/reference、完整排序uses结论、原Native准备cut/fence、离线公共副本
manifest digest/length及冻结费用owner。Source重放核逻辑有序前缀/实际ref/monotone
fence，不能要求旧物理Store/native sequence相同，也不跳过本次commit read view。

批准NV51/meta44 retiring与NV52/meta45 confirmed：两者均无且原来源全部核实才
state0，intent expected0首次APPLIED写state1；同ID/hash幂等，冲突不换closure/收费。
confirmation旧RetireIntentRef.expectedVersion仍是0，另带expectedRetireStateVersion1；
首次APPLIED才同批state2墓碑、移NV51/已闭合owner/ref并按原费用结算。NV52存在
不能因NV51缺失回到0。重复/恢复先查实际first/墓碑，不要求再次读已删blob。

NV51保存完整原signed raw及独立历史认证所需public keys/issuer/authorization/
probe/Source公共材料，无secret/私钥。锁外有界认证，Source签完整manifest digest/
length与对象/closure/uses关联，apply按精确引用物化；无需把整份raw无界塞Source。
首次intent同Native批持久完整副本/first/POSITION/Source/STATE，成功前不可delete。
新增显式checkpoint schema，只有exact state1+closure/ref时collector/audit/restore
可用NV51离线材料，正常live对象仍真实Catalog；Floor base在blob已删且confirmation
未apply时必须独立认证恢复，不能提前删owner/ref/退费打破环。

adopted NV51/索引按原冻结Target实际STATE收费，原NV47/48或NV49/50/EVIDENCE继续
保留；staging物理费归原deployment，本地退役/墓碑STATE明确签入该Source真实冻结
SHARD owner，不伪造Target owner。完整frame/uses/public材料/单行整批/I/O/空间
峰值有限，容量不足不出intent或delete，不truncate/改旧预算。确认后可用紧凑NV52
替NV51，退实际STATE差额与原闭合owner/ref/EVIDENCE，墓碑仍同原归属收费，保留
identity/intent/closure/delete摘要/Source/version/不可复活事实；合法旧checkpoint
自含NV51并能replay确认，仍受保护的旧材料不可提前删。最终压缩继续等全Floor/
Pin/保护门禁。仍无真实delete/Admin/迁退授权、单agent不Fast、测试/Broker后置。

当前代码落实仅Root精确身份构造、generation8与RetirementContext严格codec/旧
parser拒绝门禁，字段/硬界限见Registry。Native NV51/52、writer/fold/checkpoint/
actual GC尚未落地，未准入新reader、未关闭包或提升完成flags。

2026-10-16 public copy准备/NV51表示实施落点：已由实际Root owned I/O完整认证
并取回recovery raw/version/closure及原proof/material公共历史；CONTROL历史导出
已具备但其实际Root准备尚待接通。Native cut与副本
manifest绑定，私有准备结果不授Source签发/delete。NV51/meta44严格表示与STATE
计费/独立fold已接，保留所有原owner/ref/alias/EVIDENCE；fold核实际APPLIED intent
first/POSITION。默认System envelope logical-ID推导已识别body8，不靠caller补ID。
新Native51可严格解码但尚无原子Source writer产行，NV52/新checkpoint还未实现；
旧schema1..5明确拒绝含退役行，不重释旧离线能力。完整恢复认证仍须验证原批准
的copy输入，公开keys不会自成trust root。仍只必要编译，不关闭包或抬完成flags。


2026-10-16 Native intent组件实施落点：TargetEvidenceRetirementStore在原Native
view核逻辑cut/单调fence/原owner-ref-Attempt-mapping及逐sealed first/POSITION，
把完整NV51与actual APPLIED first/POSITION/Source和冻结STATE同batch组装。原Source
purpose/Root/保护/capacity批准在锁外，held本地版本随actual commit guard核每个
write边界；新copy keys不能自授权。明确拒绝只记拒绝first，unknown不写成功。
独立计费也核完整retained-use，late exact expired first不推翻prepared fence。
目前只是实际Native store组件，原Source runtime分派/配置化provider/签发/准备
输入保留、新companion/NV52与GC仍未接通，不开放新reader或实际退役/删除。


2026-10-16 Source接线实施更新：原正常/冷恢复Source runtime现在分派body8/kind8
intent到Native原子组件，配置化Source preparation新增RETIREMENT typed hold，
冻结原scope/actor/full frame、Source key和实际Root Catalog receipt并显式计费。
沿原look-ahead/有限I/O/owned release，不追加线程/队列。已有first或本地已证明
过期先于外部copy加载；过期只记Source-accounted REJECTED first/POSITION。
具体Root退役purpose配置/实际签发/准备输入移交仍未接，旧recovery signer不自动
获退役权；Artifact/Source tuple准入配置未改，NV52/new companion/完整GC继续开放。
