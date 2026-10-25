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
origin、deployment physical owner/原授权，不虚构NV47/NV49或Target费用。创建UTC原设计已由下文后续裁定修订，新分支不再要求。

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


2026-10-16 Root本地签发/持有实施更新：独立可选retirement signing manifest不
借用recovery purpose，原Root预留后owned加载/核key，原Source time与Worker/
Owner前后检查后签body8/expected0。完整prepared结果与一个签名候选在原slot保留，
不会因I/O退出被覆盖；同进程Source原provider可借exact frame/private Catalog
receipt，原purpose/保护/capacity批准仍独立。original.close失败保留borrow；
已签UNKNOWN release只认实际Native first，不认caller结果。Root Stop/close等所有
原I/O/borrow再清key/退预算。尚未实际Source append/reconcile或实现完整历史
provider/冷输入重建，NV52/new companion/全GC仍开放，所有完成flags不变。


2026-10-16 Source实际handoff接线：Root候选现经同Worker实际registered owned
writer与原OUTCOME_AND_CONTROL队列append，Native first及action/registry退出才
结算，Broker PERSISTED不是APPLIED，UNKNOWN/排队失败不换image。Root预先保留
handle、提交不跨持Root锁，Stop区分未启动与运行I/O，Host drain纳入PENDING_GC并
先驱动Source，pause/final checkpoint不能越过pending。writer兼容snapshot含8为
必要门禁，Source Approval显式原activatedTuple，旧3参不给激活；不改实际准入。
另补state1迟到expected0候选本地STALE拒绝，核实际NV51/first/POSITION后只费新增
拒绝Source行，不取旧blob/换winning closure/退费。完整历史provider/冷输入、
NV52、新companion与protected GC仍开放，未运行Source服务或提升完成flags。

2026-10-16 历史Source输入接线实施更新：Root只读读取原Recovery公共副本已不依赖
Worker入场，不需要用新Root Native cut改写原signed context；实际Catalog闭包/
raw/version/公共历史必须重建出原manifest，Native joins留给Source原read view。
单Root loan可复用同进程原候选，随实际Source hold释放，失败保留所有权；busy沿
原Source owned-I/O且不重置有限deadline。此为既定原帧/公共历史契约的局部实现，
不是新的权限裁定：完整历史purpose/保护/capacity provider及cold composition、
CONTROL/shared/staging、NV52、新companion/离线认证与protected GC仍必做开放。
仅必要main/K1/P1编译通过，全部完成flags不变；当前会话后续不使用Fast模式。

2026-10-16 NV52既定裁定实施更新：META45→strict NV52已注册，墓碑保留原signed
confirmation（原intent ID/hash/0、resource、完整context/closure/manifest/uses/
冻结fee owner及delete证据）、两个实际Source stamp及独立checksum，不含raw副本。
独立STATE/ledger和正常/冷state2预检核真实两阶段first/POSITION、原Recovery owner/
ref/alias及NV51全无，完整uses/fence仍保留；迟到候选STALE拒绝不取blob。依赖copy/
digest纳入NV52，旧companion继续拒绝两种退役状态；确认handler/原子退款/实际
provider证明、新companion及全GC仍开放。具体历史SERVICE用途数据源尚无实现，
已向主会话请求一个授权语义裁定，未自行把本地签名配置或copy当Source权限。

2026-10-16 主会话裁定历史Source退役授权数据源（本轮收到，具体provider待实施）：
允许独立、原可信Root有界加载的公开history manifest，不新建IAM/grant链或另一
运行服务。它须由原deployment/control root明确启用本用途的authority以独立签名域/
issuer-key版本发布，Root独立pin来源；内嵌key/copy signer/current角色不能自证。
每entry绑定完整SERVICE(serviceId/run/configGeneration)、System keyVersion/公钥、
Root32/namespace/Shard/tenant/lineage、kind8和分别明确的RETIRE/CONFIRM action，
tuple8批准仍须关联原Source之前已生效的真实protocol/Artifact激活证据。

区间采用真实canonical SourcePosition的(afterExclusive,throughInclusive]，先核同
Shard及物理Source incarnation/资源域；不靠墙钟、offset算术或无限上界拼接。
snapshot须有authority generation及原签名/完整性依据，完整覆盖该purpose的grant/
key/tuple变更与撤销；allow清单不是完整性证明。已封区间不可冲突/回退/重写，后继
只能一致扩展。未关闭grant不证明无限未来；决定Source点S前必须有真实authority
覆盖到S。缺材料/水位落后/认证父版本失败是unknown，不能判deny。

future sign/enqueue另持原Root真实当前purpose/key/scope许可与实际已激活tuple，
每边界核当前保护，不预知offset。实际落点后按原历史snapshot判定；可受控补齐
immutable generation，Root只在现有owned-I/O捕获认证，HeldInputs持原immutable
版本、Store锁内不刷新。覆盖不足保留原pending帧/费用/cleanup，不ACK/换帧重发/
借当前角色apply；无当前许可或可推进历史来源时writer不可无限生产pending。

撤销/rotation/tuple关闭由权威认证Source边界R effective-after生效：R及之前原有效
授权保留历史效力，R之后deny；迟到旧key帧按实际落点判定，不靠自报enqueue时间
逃过撤销。当前过期/Owner更替不追溯撤销合法旧first，也不把历史grant当当前许可；
根或签名事实冲突停止为未知，不改已有first或账。

完整可信区间内精确批准为allow，明确无权限/签名不匹配已知授权key为确定deny
（可记REJECTED）；缺历史/覆盖/读取或认证完整性失败为unavailable/incomplete，
保留entry且不生成永久拒绝。Retirement.approval=null仅确定deny。commit callback
只核原snapshot本地hold/version/pin及frame/scope，无远程I/O/当前角色重采样。

精确public snapshot及key/tuple/撤销/coverage证明沿原材料owner/Source/Checkpoint/
Floor保护留存并计入实际字节，不能只缓存key/approved标志或因current配置变化删旧
版本。先捕获小manifest并预留全部有限输入，再owned加载有限文件/entry/key/range/
总字节，管理immutable版本/路径；close成功前保留原loan/预算。provider须同时覆盖
intent与confirmation，分别结合实际copy/Native/capacity/delete证据门禁；其它family
保持原provider。实现只处理加载/范围/认证/完整覆盖/版本及局部snapshot，不杜撰
未来授权，也不生成生产材料或执行实际服务/配置/Admin/delete/迁退/保护解除。

2026-10-16 历史授权reader/Root实施更新：已按上条裁定实现独立purpose签名及原
Root pin、真实Source完整区间权限表/明确deny段、gen+1父链和旧sealed区间不可改写、
有限head更新选择、coverage未知及原帧known-key验签、公有完整chain证明导出；Root
显式可选配置/有界owned加载与单Source loan/真实close/Stop已接。尚未构成完整
Provider：Native/Checkpoint持久proof/重启tip、当前append许可和原Source实际激活
核验继续开放。文件存在/数据grant/签名本身仍不授Source APPLIED或外部delete。

同轮确认公共激活缺口：现有ArtifactGenerationSet schema1硬要求body2，带Artifact
的ProtocolVersionActivatePayload及7字段Activation又强制tuple等于set.systemTuple，
无法表示g8。TargetSourceApplyRuntime当前无ProtocolActivationState/activate实际
Native入口，InitialControlSnapshot兼容集合只是必要门禁。已向主会话询问实际
Target g8激活投影/入口的格式裁定；当前history保留可构造的既有5字段g8引用数据，
不重释旧Artifact或制造原Source批准。其余已裁定代码继续推进，不提升完成flags。

2026-10-16 主会话进一步裁定Target g8激活路径（已收到，实际实现待继续）：
复用既有APPLY_SHARD_CONTROL、controlKind1/PROTOCOL_VERSION_ACTIVATE，不新增
SystemMutationType，不重释ArtifactGenerationSet schema1/body2、旧payload3/5字段
或ProtocolActivationState。独立版本化Target activation descriptor/manifest引用完整
原base Artifact set/digest和真实环境/reset manifest，另绑定exact tuple8、Target
schema hash、所需Native/Store/checkpoint/history/copy版本及真实source/SDK锁；
只列当前闭合组合，不把Target hash塞base hash，也不另造reset系统。

marker为kind1下明确新Target payload分支/版本，完整或有界引用上述descriptor、
tuple/schema/reader证据及原CONTROL引用，由原Control semanticHash和Source签名
覆盖。旧handler拒绝未知分支，不fallback legacy；真实CONTROL根批准scope/key/
身份，retirement SERVICE/history/copy signer均无激活权。

复用ActivationAuthorityCoordinator核完整eligible membership、声明revision/session、
tuple/base Artifact，并新增对exact Target descriptor digest的明确reader声明；若
旧CapabilityDeclaration不能表达，采用最小显式新版本/分支，旧bytes不变、不推定
支持。证据绑定完整集合与exact Target组合，缺成员/过期/错session/版本均拒绝；
声明不等于真实binary/SDK部署核验，原环境根/运行锁仍独立核验。

marker走已授权的控制引导入口，只允许该闭合Target payload，无需先激活g8，但
仍须CONTROL/Source/Owner/物理保护、兼容读能力及精确reader证据。初始snapshot
仅可声明读取兼容；真实首次Source APPLIED marker之后才生效，不能由配置常量
或manifest替代，也不新造需要预先激活的通用SYSTEM body。

新增未占Native type/key的Target activation Store/record，绑定Shard/tenant/lineage、
base/manifest/Target组合、reader、原CONTROL/key/引用、完整marker ID/hash和实际
Source/stamp。Source/AuthorityLoader锁外收集原权限/Artifact/reader/环境，锁内核
Source前缀/初始兼容/状态，与first/POSITION/Source/SHARD STATE同batch。重复幂等，
同tuple不同schema/manifest/证据冲突不覆盖。g8业务严格在匹配marker之后且在原
用途权限区间，当前writer待真实APPLIED，冷重放不采今天reader/角色补历史批准。

history引用必须关联实际Native activation、marker原签名/ID/hash/Source/APPLIED、
descriptor/reader；5字段仅定位，已在当前未激活history格式强制补reference version1
和descriptor/projection/marker hash，仍非实际激活。缺证明unavailable，已证明
不存在/不匹配才按三态deny；不能反向从caller引用造Native事实。新projection及
完整公共marker/Target依赖/历史材料接独立ledger/companion/root/restore/reopen，
不要求旧Owner/session现存，不由当前reader追认；旧初始snapshot无真实激活保持
未激活。purpose grant与实际marker是相交门禁，互不替代。

只补本次g8闭环，不迁所有旧tuple。仍单agent/不Fast/必要编译/文档同步，集中
验证后置；不运行真实激活、grant/key、service/config/Admin/delete/迁退，所有
完成flags保持未完成。此裁定已收到，不存在等待主会话的阻塞。


2026-10-16 上述裁定的实现进展：独立descriptor/payload/body、CONTROL operation28、
Capability v3 explicit Target declaration/exact reader set、NV53/meta46及原Source
ACTIVATION family/first/POSITION/SHARD STATE、实际历史grant绑定和writer/fold
门禁已实施；旧base/body2/activation不变。Descriptor固定完整闭合版本组合并要求
companion6；当前最高5，硬门禁留有效marker pending，不声称实际激活完成。数据
内公钥/reader声明不能自授历史角色、Root或部署批准。具体原Root complete
membership/session/revision/环境/SDK provider、声明发布/marker签名交接/当前许可、
companion6/完整公共证明离线认证恢复及剩余退役生命周期仍待继续，不存在裁定等待。
仅必要编译，单agent/不Fast，集中验证后置；不执行真实服务/配置/key/grant/Admin/
delete/迁退/保护解除，所有四包/29切片和三个完成flags保持未完成。


2026-10-16 按既有“新companion保留完整公共依赖、独立原Root认证、恢复不采当前
Owner/session”裁定推进：companion6完整格式/收集/Native库存与原first关联及
独立Pin/PublicProof整链/用途/签名/actual activation认证入口、Manifest/候选/
whole-image/原restore authority门禁已接。NV52额外保留原intent帧与原Source授权，
不从confirmation DTO追认。旧ImageAuthority默认拒新依赖，旧companion bytes不改。
本地最高现为6，但具体Root完整provider、Source决定时proof持久/重启tip及运行
交接/许可、实际恢复与确认删除退款仍开放；不声称实际部署/激活完成。当前无裁定
等待，单agent/不Fast/必要编译/集中验证后置，四包/29切片和三个完成flags不变。


2026-10-16 按“精确公共历史随实际Source决定保留并计真实字节”裁定实施NV54/meta47：
完整signed原帧/实际决定stamp/公有proof与first/POSITION/Source及STATE同batch，
批准或确定拒绝均保留所用证明。独立Pin从原Root配置/已验证chain导出，Loader冻结
与同原loan计费保留，missing history仍unknown。此数据不是新信任根/删除授权。
为不重释旧已登记格式，Target descriptor显式schema2声明Native54与historyRecord1，
1原bytes/digest可读；新g8写入要求真实2，不自动覆盖同tuple旧投影或执行迁退。
Companion6复用Native54原材料，Root accepted-tip重启对照/完整具体provider、实际
confirmation/current-purpose许可/原子退款/GC仍待继续。仅必要编译，单agent/不Fast，
未执行服务/配置/key/grant/Admin/delete/保护解除，四包/29切片/三个完成flags不变。


2026-10-16 按“重启仍核对已接受tip、历史材料不可回退/分叉”的原裁定继续实现：
Native55/meta48持久化每个历史scope的accepted proof及真实Source决定ID/stamp，与
54/first/POSITION/Source及STATE同batch，Source决定前点读并最终view复核，漏旧
signed tip为Unavailable；换baseline不在键上，不能绕过。原Root仅内存accepted不足，
Native门禁为实际决定提供重启保护，仍不声称启动pin装配和真实恢复已完成。
Descriptor显式3声明55/tip1，旧1/2原codec不改，不给旧tuple投影自动升级；新写要求
actual3。源码/必要编译和文档同步，完整Root/provider/旧pin信任归档与实际确认GC
继续实施；无裁定等待、无环境操作，单agent/不Fast，四包/29切片/flags不变。


2026-10-16 按既有“独立历史用途权限、真实Root副本/保护/容量、锁外owned收集及
actual Native另核”的裁定接Recovery具体Source provider。Content保留原guards与
其它family/provenance，完整history先核、正向才读copy，known deny无Approval/blob
仍留proof；原frame/Source/预算和真实close沿同loan持有。没有把signer/copy/tuple
locator当source激活许可。另补不同未借出prepared候选下的当前Source帧重建及全部
重叠预算，避免仅等待更晚候选；不释放原候选或增默认资源。仍未闭合整机启动/
provider/activation/CONTROL/shared/staging/confirmation/refund/GC或运行验证，
无新裁定等待/环境操作，单agent/不Fast/必要编译，四包/29切片/flags不提升。

2026-10-16 上述裁定的CONTROL输入实现进展：原management Opening现支持真实
Native APPLIED Adoption的PROTECTION/ANCHOR closure，复用原owned-I/O/预算及
独立当前action10，保留完整uses和原责任，不推导共享依赖已释放。原CONTROL
历史导出依赖随真实Root注入，Catalog按原publication/frozen scope与exact对象/
version/原protection/history/closure/context重建copy并重读，不用当前角色追认。
新Source组合接adopted CONTROL与Recovery，旧Recovery-only API保持语义；CONTROL
正向不需Recovery archive，deny无copy仍持完整history。无新wire/默认预算/权限；
Native CONTROL/shared投影仍明确未接线拒绝，CONTROL cut准备/签名、staging、
confirmation/退款/GC及真实运行尚未完成，不提升四包/29切片和三个完成flags。

2026-10-16 上述原UNKNOWN保护义务继续落实：前轮CONTROL单Adoption关闭入口
不足以证明所有共享Attempt均已结束。现旧API拒绝，新原Source入口在原Root cut
上取得实际Native完整sealed first/POSITION/永久Source fence和终态共享引用预检，
私有receipt绑定捕获Root；同CAS所见Root变化则拒绝重试，不能封闭UNKNOWN恢复
proof父依赖。当前action10/原Root/Source admission和完整有限工作窗口仍必需，
只补记所选对象adoption，不授权物理删除或原保护解除。
CONTROL kind1/2 Native51投影现按Admission用途核原CONTROL event/owner及完整
共享mapping/Attempt/ref/alias/creator，增量和checkpoint同View范围核双向完整性，
原owners/refs/EVIDENCE保留，copy STATE计冻结Target。原adoption读取共用于实际
Native与immutable image，补exact body hash/lineage/Owner author/first/POSITION。
无wire/默认预算/新线程；预算不足保未知。CONTROL cut/copy准备和签名、staging、
实际activation/原pin/环境与confirmation/退款/GC仍待继续，不关闭四包/29切片。

2026-10-16 上述独立退役裁定的CONTROL准备/签名实现进展：真实已封闭Catalog经
原Source guarded Native读返回私有ClosedUses，复用关闭前同一完整uses/共享refs/
终态Attempt核验，实际cut不得领先Source。anchor creator身份从原Attempt alias/
Native mapping重建，不以当前引用者或调用方数据替换。Root prepareControlRetirement
持原cut/context/认证public copy，并与Recovery共用原有限准备slot/owned-I/O/
Source borrow/UNKNOWN持有及actual-first release；CONTROL不需Recovery archive。
签名器只接两类实际ClosedUses及原Catalog receipt，经private共同函数签原g8
SERVICE intent，保原Root/key/time/retry/context检查；实际Source writer/WorkClass
交接不变，不以key/历史CONTROL权限批准Source或delete。无新wire/默认额度/线程，
完整启动/provider/activation/pin/当前purpose、staging、确认退款/GC与真实恢复尚
待实施/验证，不关闭四包/29切片或提升三个完成flags。

2026-10-16 主决策会话后续修订staging前置设计：创建UTC不参与本项已裁定的安全
证明，取消新staging必须具备publication createdAt的门槛；不增加Root时间字段/
provider，不以process TrustedClock.long、CONTROL Source时间或Recovery observation
冒充创建时刻。旧branch4六字段及严格decoder保持，新branch5 ReservedStaging字段
1版本2，保publication ID/原origin、原capacity-owner ref及authorization kind/digest。
物理owner为原持久capacity root派生身份，ref精确绑定原Catalog namespace/root key/
storage namespace/limits.capacitySourceDigest；裸source digest不是owner。私有
实际Root读取须原Root/reservation/limits/成员及management授权，不能只拼配置。
旧publication若原reservation/归属/授权/closure/uses均可证，可直接用新形态；未知
搬迁/预算替换/晚到I/O/曾否adopted保持Unavailable，不自动迁移或释放旧责任。
本轮实现data/ref codec、实际SDK namespace绑定及私有Root capacity receipt，原
Root1..3与originDigest不变；Descriptor4明确ref1/staging2，旧1..3 bytes保持，新
Stage2 Native前置/本地预检与state1/2原intent前4门禁已接。staging Native核全sealed
uses永久非APPLIED、精确对象无adoption/ref/alias及实际冻结Source SHARD STATE。
仍未实施staging closure/copy/provider/准备writer、完整环境/current-purpose、确认
退款/GC或真实运行。单agent/不Fast，只必要编译，四包/29切片/三个flags不提升。

2026-10-16 上述reservation裁定的copy/Source推进：已经闭合的ReservedStaging可经
原Root核exact capacity-owner/pubID/origin、原授权history、raw/version/closure与
captured/current phase后重建公共copy，并重读排除copy期间已adopted。Stage2原授权
摘要明确CONTROL PreparedControlOperation SHA256或Recovery原CredentialAuthorization
的digest，原Source同域且不晚于Native prepared cut，同点metadata一致。旧staging
不拿当前时间/材料补齐。Subtype决定Recovery archive依赖；新Content沿原loan接Stage2，
deny仅history，其它family/legacy保持原provider。无wire/默认额度/线程变更。
检查原SDK发现Sync put等future结果及Root PUBLISHED/reread不等于完整未知put/晚
回调退出证明；故本轮未提供以这些材料单独批准staging关闭的入口。copy receipt不
授closure/delete/退款，实际原writer/I/O责任、staging关闭/准备writer、完整environment/
current-purpose、confirmation/GC与真实恢复继续开放。单agent/不Fast，集中验证后置，
四包/29切片/三个完成flags保持未完成。

2026-10-16 在不预先裁定原publication关闭/no-recreate的前提下，接已closed Stage2的
实际Native准备及Root writer链路。私有ClosedUses从实际Store/cut/fence/完整拒绝或
永久fenced uses与SHARD fee读出，复用Source exact无adoption/ref/alias门禁；Catalog
从原reservation/capacity-root/原授权派生Resource并认证copy，无Target/createdAt猜测。
Root准备/sign/Source借用/UNKNOWN/release沿原有限slot，Recovery按subtype需原archive。
签名和新writer入队/append按Resource要求实际descriptor4及原SHARD/tenant/lineage，原
first重放不变。此推进不授closure/delete/退款；原I/O/no-recreate裁定、完整环境/current-
purpose、confirmation/GC与真实Broker/恢复继续开放。单agent/不Fast，仅必要编译，
四包/29切片与三个完成flags不提升。

2026-10-16 主决策会话批准原key条件CAS no-recreate marker，修订原物理delete-key前置。
同一key保留非ephemeral/无TTL marker，已核原raw/hash/length/version后IfVersionIdEquals
替换；它阻断旧IfAbsent/旧version CAS生效，不证明旧请求已完成。exact marker幂等，
外来marker/不同intent或原值/version冲突拒绝，无无条件覆盖/另key tombstone方案。
必须先有原Root closure及完整uses/candidates永久future-first否定、真实Source APPLIED
intent/NV51完整copy及合法Floor/checkpoint/Pin/共享引用条件。原本地owned I/O/晚回调/
buffer/SDK close责任不因marker释放；closure持续拒绝新seal/publish/adopt。满足全部门禁
后原key缺失可IfAbsent占位，旧put先赢只核确为原payload后CAS；无材料/外来值/不确定仍
Unavailable。跨重启缺每put参与者清单可用受信协议/部署历史证明所有旧写入仅IfAbsent/
旧exact-version CAS；当前源码不足为未知历史writer或无条件write/deleteRange背书。
Root必须预留marker与元数据实际有界峰值，旧publication origin/hash/bytes不改写。
新增kind8 outcome3明确payload退役、marker保留，不能编码DELETED/ALREADY_ABSENT；仅
actual条件操作及exact读回/原Root私有确认可签。首次Source APPLIED确认才沿原规则
结算逻辑EVIDENCE/STATE；UNKNOWN/REJECTED/重复不重复减费，确认未apply时NV51继续支撑
离线恢复且保守计费。物理费只回收真实payload差额，marker/key/Root防复活记录及容量
继续归原capacity root，不释放record/publication数、不压力逐出。没有额外真实server
fencing时即使Floor/Pin满足也不可最终删marker；当前SDK关闭/等待均不构成证明。
本轮先实施marker/evidence schema1、g8 outcome3独立证据、NV52 schema2、Descriptor5/
companion7及实际Native/历史capability检查；旧outcome/1..7/Descriptor1..4/NV52 schema1/
companion1..6保原分支。codec不授actual I/O或身份历史权限，原publication关闭/Root
marker投影和计费/当前purpose/条件provider/Source确认apply/refund/GC仍待接线。
仅必要编译、集中验证后置；未运行真实marker/Admin/delete/迁退/保护解除。

2026-10-16 Native确认转换及CONTROL独立退役推进：实际Source确认仍缺原Root条件操作/
exact读回私有凭据，未降级为签名/DTO/true。先提供同Source批次business confirmationEdits，
核实际NV51/原APPLIED intent history/first/POSITION、完整uses/capability并绑定新确认，
返回精确NV51→NV52及闭合owner/ref/alias删除；不批准provider或Source purpose，也不
独立commit。NV52独立账本补齐CONTROL/shared/staging，保完整引用/creator/历史/激活。
修正CONTROL原primary kind扫描遗漏孤立alias的风险，完整有限ref48扫描且按自身真实
mapping/Admission/Attempt/first验证，不要求另一CONTROL owner继续存活；原普通adoption
双owner保证不变。增加单owner选择器接原Root关闭/准备/实际Native验证，不能由selector
自证。live adopted proof依赖必须先退役，有限读耗尽不当无依赖；不新增协议/预算/线程。
真实Root operation/私有receipt/物理计费、Source确认分发/当前purpose/退款及全恢复仍待
实施验证；单agent/不Fast、集中验证后置，四包/29切片和完成flags保持未完成。

2026-10-16 marker实际provider前的Root持久预留推进，保留先前全部批准门禁。现有
checkpoint reaping predicate只覆盖checkpoint，不能批准kind8；未拿hash/布尔/签名
替代真实Floor/Pin/query/历史writer资格和原Root条件I/O。先增加Actual Native AppliedIntent
receipt及共享state1核验、Root4 pending MarkerReservation1与Catalog metadata CAS。
旧Root1..3和原publication origin/raw账不改；新record核actual native first/history/
owner/uses/5及原Root closure/capacity/SDK binding，预留future证据与Source引用窗口和值
正增量，保原records/publication槽。不提供provider success/Source确认或物理退款，
当前CLOSE_REFERENCES仅用于metadata准备。原Root owned-I/O/UNKNOWN/local budget、旧
namespace缺Root的bootstrap保护、实际conditional-writer来源与Floor/Pin/共享保护、
原payload CAS/观察私有receipt、Source确认/物理结算仍未闭合。marker/key最终GC仍无
server级保证，不进入真实操作。单agent/不Fast，集中验证后置，不提升四包/29切片flags。

2026-10-16 主决策会话裁定缺Root/bootstrap边界：批准独立INITIALIZE_FRESH_CATALOG_ROOT
与ADOPT_EXISTING_CATALOG_ROOT_GUARD用途，旧BEGIN/CLOSE/当前PLATFORM_OPERATOR/配置/双
缺失不继承。fresh签名current-policy须绑定实际SDK namespace/prefix/root+guard key/
deployment/capacity limits/allocation+nonce，并另有真实外部分配权威保留exact allocation
从未普通使用及一次性初始化责任；事实不得随本root/guard丢失，static allow不等于它。
只消费外部实际authority/provider，不新造allocator/IAM/Source CONTROL链；缺材料为环境
前提，不能换nonce/actor重开用过的key。普通read/find/update/begin/publication/Admin
只读受保护READY Root，缺/坏/不匹配均失败。私有typed bootstrap先耐久PENDING guard，
再不可普通使用的initial Root，有限CAS完成绑定/READY；中间态不能普通操作。仅FRESH
PENDING same allocation/nonce且外部仍确认never-used可续，不接未知非空Root；READY不能
回PENDING，Root显式live/初始化证据/actual version共同拒late初始化IfAbsent变可用空Root。
LEGACY adoption仅接actual现存完整Root，独立metadata权限下先CAS reserve关联事实/费用再
装guard，同nonce有限步骤，丢Root/降级/原绑定不符均Unavailable，旧bytes只保解码供受控
adoption。不扫描猜Root/不自动迁移。guard/Root关联真实bytes/records计原capacity，超额
不put，unknown不退，不能删guard/压缩曾READY事实回收fresh名额；无server保证不授GC。
这些新purpose/guard协议和Factory默认fail-closed切换仍未实现。
本轮实现此前批准的metadata I/O保守边界：Root-owned有限slot与actual Native intent/
Catalog CAS，进入Catalog后异常原slot/费用保留，Root原故障路径fence Sources；本项CAS
不把matching reread合成为I/O已退出。Admin与Oxia退出不同，实际method/Admin退出后把原
Client费用经原backend交实际Oxia owner，同图同pool/既有maxClients界限，actual SDK
termination+cleanup task exit后才退；释放失败保留。普通Source guard SDK责任另保，
未进Catalog的真实局部no-start可discard。只完成本地责任交接，未实际marker/Floor/
Pin/query保护/Source确认/物理退款；全client退出不证明remote不可重放，仍不得删Root/
guard/marker。无新service/thread/retry/default额度，单agent/不Fast，集中验证后置。
这次实际SDK费用交接最终覆盖所有managed Catalog backing的关闭，不仅marker异常，因Admin退出均不能证明Registry输入退出；borrowed backing保旧close路径但不能准入新marker操作。原pool/resources/maximumClients与实际未release charge必须保持。

2026-10-22 按既有裁定实施Root guard，无新增语义裁定：独立签名Scope/Policy与purpose/
issuer/key/deployment pins、actual current snapshot，显式fresh外部never-used/一次性birth
责任消费，legacy原Root/管理历史先reserve再装guard，Root5与永久Guard1有限CAS接原owned
opening；普通Root读取与更新已切为protected READY/LIVE，默认缺材料/未接管旧Root失败。
补原PENDING actual version绑定，避免READY guard丢失后late旧IfAbsent形成可恢复PENDING；
旧initial Root也不因晚写而LIVE。旧1..4 bytes保decode，仅明确接管入口可升级，不扫描重建。
原Root关联/guard bytes+record永久计原capacity，unknown/late/取消不退；实际Registry owner
首RPC前捕获，早期Catalog构造失败也交原SDK保费用。外部authority是必须真实提供的环境
接口，不新增allocator/IAM/本地file事实/Source CONTROL链；未装入或运行任何实际材料/
初始化/接管/guard/Admin/迁移/保护解除。真实server契约、外部provider、完整运行及恢复
验证仍开放；guard与marker GC/物理退款仍无新权限，四包/29切片/三个flags不提升。

2026-10-22 主会话明确exact marker幂等：必须先全部current physical-purpose、原READY
Root/reservation、Source APPLIED NV51、closure、Floor/checkpoint/Pin/query/shared及历史
conditional-only/实际部署资格门禁。再本次真实原payload-version CAS或IfAbsent，只有与
该请求对应的SDK明确条件拒绝后，actual exact persistent marker GET可形成新Root私有
观察，不能换marker version做CAS、不先裸GET跳请求，不声称本次写成功或旧UNKNOWN退出。
schema1/RequestKind1..2不变；request hash绑定实际dispatch/请求，不伪造旧或SDK无的
server ID；response hash带确定domain/结果类别及实际条件结果/读回key/raw/len/version。
Unknown一律保原owner/slot/费用与fault，不在其catch匹配读回转成功；恢复时新合法Root
可新观察，但不结清旧SDK输入/failed backing。无需新通用provider日志/重试层。原Root
物理差额结算仍需恢复幂等，记录/publication/marker/guard不退，无最终marker/key GC。
批准独立current action RETIRE_EVIDENCE_OBJECT_PAYLOAD，metadata CLOSE_REFERENCES不继承，
原枚举ordinal/policy bytes不变，约束原Root/namespace/deployment/管理资源及exact对象链。
本批接Source确认prepare/commit、完整history/grant/actual5、opaque receipt约束及同批
NV51→NV52/owner/ref/alias/history/tip/first/POSITION/accounting，接原writer/WorkClass/
正常冷恢复分派。Opaque观察类型无public构造；其actual provider/mint、Root确认签名/
content/loan/冷重建及完整physical gates、实际物理对账仍未实施，不拿pending/DTO补齐。
仅代码/必要编译，未运行tests/真实provider或环境操作，四包/29切片/三个flags不提升。

2026-10-22 主会话明确完整门禁采用“原受信部署历史资格 + 当前实际保护状态”。独立
QUALIFY_EVIDENCE_OBJECT_LIFECYCLE purpose的有界签名材料绑定actual SDK namespace、
exact prefix/Root/guard、allocation/incarnation、deployment、起止epoch/版本和可信完整
起点/前驱，覆盖全部曾准入writer/reader协议、条件写/禁止重用回退及引用/Pin/关闭转copy/
永久拒绝迟到引用规则与证据来源。未知路径、断链、reset/rewind、scope错误不可补造fresh
历史。静态历史之外，必须由原authenticator/held contract提供操作期间实际admission
约束，阻止未覆盖协议混入；缺失则该部署物理不可用。两次散读/Java对象不代替远端约束。
kind1沿原Catalog/Floor/Pin及完整实际image；kind3/4/6沿原Root closure/full inventory、
Native owner/ref/alias/Attempt/Journal/first/POSITION/history/permanent fence与实际SDK/read
生命周期，不凭空表/换Owner/超时推断退出。kind2/5只有完整qualified旧历史及当前准入
明确从不依赖原blob（或依赖已保护Native/copy），才判“不依赖本blob”，不写“义务释放”；
若真实旧audit/export依赖raw，须原系统实际retention/held authority证明终止或等价受保护
迁移，无接入保持不可退役。本轮不造通用inventory/lease/服务/后台poller。
ProtectionSet仅签名声明，完整义务由实际协议与exact对象/closure/Native/Catalog推导；
所有已声明Ref逐项exact id/generation解析，未知、缺失、截断、预算不足、变化均不可用，
不改已APPLIED intent。批准原Root私有组合实际held句柄，requireCurrent/close仅转发真实
责任；不收caller boolean自证完整性。历史/无raw依赖不替独立当前RETIRE权限、实际请求/
GET观察、Source确认或幂等物理账，不缩短Native/Source/history/copy保留，无最终GC。
本批只实现actual Floor/Pin/完整NV51图像事实及输入预算/重验，组合和实际provider未完成。

2026-10-22 主会话核Registry、首次定义8915d21e及gcProtection调用后裁定：现有
ProtectionRef ID/generation没有既定生产语义或原权威映射，kind1也不能猜为Floor digest/
Catalog ID/hash/generation。任何无法按既定真实权威exact解析的声明，均须在payload
CAS/IfAbsent及新正向confirmation授权前Unavailable；不得忽略ID/gen，仅匹配其它
字段，或以签名资格/caller resolver/空库存补造。保原decode/恢复/retained-first语义及
已APPLIED intent/NV51/copy/费用；空列表的支持路径仍做全动态门禁，不隐去已知引用。
本轮不造新通用库存/ID算法，后续支持这些声明须显式版本化producer/authority/generation
契约。此为物理资格限制，不是数据格式错误，不据空声明或编译提升整包完成状态。
现已接原Source/Catalog实际绑定与全动态composite、真实条件操作/GET/private observation
producer，整链的Root signing/Source loan/cold与幂等差额未闭合，继续连续推进。

2026-10-22 主会话批准独立HistoricalConfirmation私有历史认证，不另造通用provider
日志：完整可信Source事件本身是原writer受独立DELETE_CONFIRMATION grant授权、按
受限实际观察协议签发的耐久见证；一般Source权限、裸DTO或任意旧签名不够。资格历史
须覆盖原confirmation writer受限签发语义，历史Source区间/accepted tip/父链/exact
SERVICE/key/先行Target descriptor5必须认证，今天的角色/激活不补历史。只允许当前
合法Source/recovery装配原实际entry进入，绑定完整canonical envelope、ID/hash、物理
Source incarnation/position与原intent stamp/resource/context/closure。原READY Root/
guard/allocation、persistent marker reservation、实际Native NV51/完整uses/first/POSITION、
qualified history/current hold及实际Floor/copy/Pin/引用保护都重验；现有未解析Ref边界
不变。actual GET须原key的persistent exact marker，旧body的raw/完整bytes/长度/version
全匹配，同bytes新version亦拒绝，不能put修复。该私有类型与热ObservedRetiredMarker
不可互换，不表示本次条件成功/拒绝，不供sign/enqueue/physical退款或GC。原requestKind/
version/hash/response/time/签名原样保留，今天读事实只作当前一致性检查，不生成新UUID
假称旧请求。历史重放不要求旧SERVICE重获今天RETIRE/签发权限；commit只核本地held。
retained-first仍在任何取数前；没有first的历史确认同原Native批次首次结算。只有marker
而原签名确认未进实际Source时，不可合成历史帧，必须沿原UNKNOWN/pending约束恢复后
新实际条件尝试/观察及当前签发。历史认证不清旧SDK责任，也不代替幂等物理账。
此裁定已记录，独立历史类型/实际cold Native读桥接仍待实现，不声称本批已支持cold。


2026-10-22 历史确认裁定落地：采用独立HistoricalConfirmation，原完整signed kind8/
outcome3 Source帧在独立历史CONFIRM授权和全部writer受限协议资格下作为原真实条件
操作+exact观察的耐久见证；一般grant/DTO/今天GET不足。资格History2新增Rule5和原
Source→namespace/exact publication配置绑定，同DB重启从原Content配置装配，恢复沿
原实际Catalog，拒绝同SHARD fork。重验actual Native51/actual5/full Root/Floor/Pin/image/
当前admission和OBSERVE及原marker version；旧SERVICE无需今天重新签名/RETIRE。
历史分支不转换Observed、不新写/签/入队/退款，commit仅本地held检查；Native gate已能
在provider前处理确定过期/未激活，保retained-first优先。无未知ID/gen Ref降级与材料
补造，完整实现和部署/集中验证责任不缩减。


## 2026-10-23：原生资源回收必须使用独立 signed Source

主会话确认admission/HeldAdmission/FreshAllocation为可信composition-root的外部部署
权威边界，具体provider/获取释放失效协议及材料归部署集成；仓库保精确身份scope、
有界输入、held责任与默认拒绝，不造服务、IAM/allocator/通用库存，不将具体部署缺失
写成已生产可用。未定义ProtectionRef ID/generation限制继续保留。

原生Unused reserve、retained allocation、payload release、terminal/result reclaim批准
独立SERVICE type14 TARGET_RESOURCE_RECLAIM/body9/独立用途与tuple，沿原Source/
有界WorkClass/单pending/UNKNOWN/drain，不新增local ordinal。封闭有界组冻结实际
身份/原owner/lineage/预期完整状态与Floor；只有首次实际Source apply同批改phase/删
记录/减费并保history/first/POSITION，未知保护不假拒绝或视作空。Unused reserve仅
RESOLVED且全部reserved writer结束；retained实占按真实删记录精确减少；inline/
Native payload同批真实primary Message清理并保RELEASED owner STATE。外部payload先
Source持久retire-intent/禁止新用，再锁外真实有界条件provider，最后Source确认释放；
不借legacy kind1或kind8 receipt/GET授权。terminal/result只能删受保护闭合叶子/组，
保surviving first/Attempt/history/query/replay/export/control和永久fence，不自删本次
GC幂等依据。实际Catalog/Floor/ancestry/Pin/image、原Native/Journal/SDK持有责任、历史
SERVICE/key/用途/撤销区间/actual activation必须完整，commit只核held本地事实。
当前仅协议/历史用途基础，actual Source写者及全闭环继续实施，未授权环境删除或迁退。


2026-10-23 主会话明确裁定BASELINE unused-reserve逻辑闭合（来源：主会话
`01a13046-4fcd-7e90-8a56-dc2c3828fe23` 对本实施会话的裁定）：限定为已认证
materialized ordinary BASELINE Admission、同历史完整Owner首次APPLIED definitive
初始Outcome，无P1 Journal/K2/native/后置proof；完整expected Budget和真实first/
POSITION/无open或不明别名，实际已提交ingress fence覆盖两原帧retry deadline。
逻辑写者为Admission首次建预算、原Outcome首次转换及同完整帧重试；后续POSITION
归SHARD独立收费，新同预算写者必须显式扩展规则。允许新Owner按上述可重建Source
历史冷恢复，不新增随机closure token/旧进程退出witness。原Catalog/Floor/Pin/full
image/future-reader held保护与Source原子计费仍全部必需，仅退unused commitment；
allocated/原记录/SDK/physical/zombie责任保留。此子集不是全部原生回收或四包完成。


## 2026-10-23：UNKNOWN 后允许原 GC 准备借用独立退出

来源：主会话 `01a13046-4fcd-7e90-8a56-dc2c3828fe23` 对本实施会话的明确裁定。
GC Prepared 只服务提交前；actual Source apply 通过私有 ReclaimRead 独立取得完整
权限和 held Protection。允许原 Host/Worker 私有 quarantine 路径，在实际旧 Worker/
Store fence、Source authority stop/真实 I/O/cleanup、exact GC opening/签名/action/
registry 全退出以及原 writer/backend 物理 termination 后，关闭 GC 自己的临时
loan/admission acquisition，真实 cleanup 成功后清本地副本并退本地 input window。
不要求新增 durable handoff/transfer token/日志，不是把保护移交给新 Owner。
UNKNOWN/no-first 仍未知，正常 first 与 pure no-start 路径不放宽。不能据本地
termination 判服务端不会迟到或原帧不会再出现，不能造 first/ACK/消费水位或换帧。
独立 pins/retention、signed history、Catalog/镜像、唯一恢复材料及其它用户责任
必须留原 owner；混合 Held 只能关闭 GC 自己引用，不能移除持久保护或跨池退款。
新 Owner 沿实际 Source frame 重新验全部 Action3/activation/Native/Catalog/Pin/image/
held，缺材料保持 pending；旧 Prepared 不授 apply 权限。调整 cleanup 顺序解除
Source authority 被 GC 等 first 阻住的等待环。只有代码实施与必要编译，未授权真实
环境操作、删除或保护解除，四包/29 切片及全部恢复验证义务不缩减。


## 2026-10-23：unused commitment 允许完整认证的较早 Pin 前缀

来源：主会话 `01a13046-4fcd-7e90-8a56-dc2c3828fe23` 的明确裁定。每张旧镜像必须
已含 phase3/本次 RETAINED 不是必要条件；当前实际 Floor 的 exact expected、原
Admission/definitive Outcome/first/POSITION/fence 加 actual Native/held 才是当前
closure。合法更早 Pin 只按真实 Source cut 验完整不存在或 exact ADMITTED/revision1；
直接 definitive 子集不默认接受 UNKNOWN，越过 Outcome仍须 resolved/精确回收后态。
完整实际 Pin/session/version/namespace/Source/tenant/lineage/parent hash、ledger/
companion/ImageAuthority 历史均保留，位置同 incarnation且 metadata/sequence一致。
只消费既有恢复约束，不新增资格authority或 durable witness；保持当前 Floor restore
candidate gate 和实际 Pin/Owner/Store/CATCHING_UP、strict successor、accepted barrier/
activation/接管。不能仅凭 barrier 代码存在认为安全，未证明旧实例被隔离或必须重放
原安全 Source 前缀者仍 unavailable，不授未知/旧 reader 写资格。仅退 unused commitment，
不删记录、降 allocated 或解除 Pin/历史/恢复/SDK责任，后续删除另验依赖。
新签发仍 exact current Floor；actual ReclaimRead 在 S 时，若新当前 Floor 的真实 Source/
sequence 在 S/真实分配 stamp 前，不要求预含未来决定；到/越过 S 则须完整认证同
canonical envelope/identity/S/已分配 mutation stamp 的 APPLIED first/POS/history和
精确后态，不接 caller 预测位置，同排序点 metadata不一致拒绝。仅代码与必要编译，
无实际环境、Pin解除或删除授权，集中/真实恢复和全部四包义务继续。


## 2026-10-23：inline release 先补原 frozen terminal policy 内容

来源：主会话01a13046-4fcd-7e90-8a56-dc2c3828fe23明确裁定。新增内容寻址
TerminalPolicySemantic1，显式非负有限 minimumPayloadRetentionMs；首个时间基准为
RETAINED_OWNER_SOURCE_PERSISTENCE、终态 primary read=METADATA_ONLY、replay=DISABLED，
原 RetryPolicy.dlqExportMode 必须 NOT_CONFIGURED。摘要须精确匹配原冻结
terminalPolicyDigest，并沿相关全部代 RetryPolicyRef/原 Source/Profile/CONTROL
历史认证；不引入新策略服务或 signer，不由今天配置补造旧历史。deadline 为原
RETAINED transition Source persistence timestamp checked加 retention，不是本地 apply
后墙钟时长；实际到期/fence/迟到窗口、引用/读者/export/replay/Pin无primary依赖与
恢复安全 cut仍必须独立证明，无法证明就保留。unused reserve旧 Pin宽松规则不用于
删除；type14 kind2须新body10/tuple10及显式descriptor能力，旧body9/tuple9/descriptor6
原义不变。首个实际 Source须原子删Message、owner转RELEASED、精确结算并保owner
STATE/terminal/binding/history；OBJECT不借此分支。
本步只实现闭合策略值、原不可变publication2与按Message原Source uses的有界内容读取，
并修既有companion8摘要分派漏项。body10实际生命周期/companion/读者闭合继续实施，
未因本依赖完成而宣布payload回收或四包完成；单agent非Fast、仅必要编译、集中验证
后置，没有真实服务/材料/激活/删除/迁退或保护解除授权。


## 2026-10-23：落实inline新能力及独立恢复认证边界

承接主会话已批准的kind2/body10/tuple10新能力裁定，本次实现descriptor7、Native54v3、
lifecycle3和companion9（确切字段见Registry）。旧body9/tuple9/descriptor6、旧grant
身份及旧companion字节保持；Action3必须选择精确tuple，新tuple不能继承旧grant。
保留terminal locator/version/hash作为删除后metadata关联，不仅冻结payload owner。
只读账务/full ledger和原策略companion已接，原Image/lifecycle权限继续独立认证；旧
Source-history verifier默认拒绝APPLIED inline决定，不能借一般Action3批准实际删除。
当前正向Source仍因完整动态闭合未接而pending，不套unused旧Pin规则。此是协议与
恢复读路径进度，后续真实kind2准备/签发、Reader/Pin/retention fence、安全cut、
metadata查询、Native全部引用及原子删除/精确计费继续，未缩减完整包或授权真实环境。

## 2026-10-23：B路径，完整SourceFacts必须先随回收请求持久

主会话裁定选择B，拒依赖尚未接通的独立Source-retention provider无限保护旧原帧。
完整有界SourceFacts在GC准备中核原APPLIED first/POSITION/Source/policy/TimeFence后，
先随本次原signed Source请求持久，再由同批Native54及后继checkpoint延续。只在apply
后写companion不能覆盖append已持久/UNKNOWN而first尚不存在的窗口。
采用明确body11/tuple11、descriptor8、Native54v4/lifecycle4/companion10，旧body10及
其边界不补字段、不追认事实、不让旧grant授权新语义。完整原frames、Source/stamps和
签名绑定均持久保存；原Native first/POSITION、Root/pin/语义历史仍是独立信任链，
capsule不生成独立权限、不带秘密、不引入新服务/日志/token。每帧/组/总bytes都使用
原输入与STATE/checkpoint额度。完整字段和旧bytes边界见Registry。

同批原子Message删除/owner RELEASED/精确账务和metadata/Cancel/Reschedule已接线；
Source/cold取body11自含原帧，local reopen/ledger/companion要求持久依赖继续保留。
只要相关owner/first/history/terminal/查询/Pin/恢复仍依赖该证据，后继Floor就不能丢弃，
证据和历史物理副本继续计费。本次不批准真实Source截断、历史删除或保护解除。
capsule解决历史事实存续，不能证明当前无读者或未来不会复活；实际动态reader/replay/
export/Pin准入与read退出仍须真实原held协议。当前生产provider没有该具体实现，新契约
默认拒绝；已向主会话提出应复用哪条原admission权限链的实际分歧，不记inline功能闭合。
单agent非Fast、必要编译、集中验证后置和全部四包/29切片目标保持。


## 2026-10-23：inline动态保护的原部署集成边界

主会话确认：复用同一个真实deployment authenticator/admission的身份、epoch/失效和
acquire/close机制，目的独立为PROTECT_NATIVE_INLINE_PAYLOAD_RECLAIM；不继承Catalog
object生命周期权限、普通Snapshot/reader membership或Source Action3。仓内没有另一条
可自发发行全域无primary依赖许可的Owner/ReadAuthority/barrier替代路径。三个已有接点
（GC InputProvider/Held、SourceAuthorityLoader.RECLAIM、ImageAuthority）参数足够时直接
严格绑定，不再造第四套抽象/服务/日志。仓内负责实际load/使用/失效/关闭与UNKNOWN/失败
责任链；Source/cold重新取得自己的实际hold，GC-local退出只释放准备loan。外部provider
及资格材料、真实动态保护验证仍明确OPEN，缺失默认拒绝；该边界不再阻挡继续其它仓内
缺口，也不能把接口接通升格为生产验收。关闭cut、原全部引用、Action3与冻结账务独立保持。


## 2026-10-23：kind3有界Native记录与allocation结算

主会话批准type14 kind3/body12/tuple12/descriptor9，Native54/lifecycle/companion各
升级5/5/11，旧bytes与权限保持。首批明确正向叶子为RESULT查询副本和独立expired
POSITION，保原first且仍核原冻结窗口、完整引用与真实读者/Pin保护；不将TTL作为
payload/identity/history统一退休规则。0余额RETAINED可结清且NV28槽位仍收费。

记录费用与Budget.allocated分开核算。只能减少归属明确、尚未结算的实际allocation项，
Source推导remaining而非接受caller delta；当前实现支持原APPLIED Recovery确认对应
的adopted share，原Native历史给一次性结算依据。其它缺basis余额保留；不由“缺行”
猜零，不补compact tombstone或替代历史证书。仍被RELEASED owner/body11/Native54/55/
Budget/恢复/查询/Counter归属引用的first/POS/terminal继续付费保存。

实际GC发现/准备/签发、完整Floor/live及Pin image、Source同批删除/结算、正常/冷/账务
与独立ledger链已按首批接线。source facts存原完整Route/fence而非当前配置；默认预算
不提高，body12独立32MiB界限容纳既有16MiB Route。原外部可信准入/历史provider仍承担
真实loan/epoch/旧image/read退出，缺能力默认拒绝。集中验证后置，全部flags不提升。

OBJECT按主会话另定：后续type14专用“持久exact intent→原provider条件操作→Source确认”，
复用PAYLOAD_OBJECT身份并绑定Native owner/reservation/commit/各代/冻结费用；现有后端
没有生产条件删除API，不能借kind8/legacy intent/裸GET absence/SDK close伪称完成。
当前descriptor9不宣布该能力；此后续与其它未闭合记录/身份流程保留在完整目标中。


### 2026-10-23：OBJECT Source核心的实施边界

落实既定type14专用两阶段裁定：kind4/body13保完整Native owner/原commit proof及raw原
retained/fence；Native56记录pending，保收费；独立Source确认消费actual hot或原provider
合法历史见证，同批清理primary/pending与owner RELEASED、原精确账务，保恢复/查询依赖。
沿原GC/Source/image持有点增加OBJECT独立目的，缺任何真实能力默认拒绝。

本次只到Source/读取核心，不把尚未连接的OBJECT discovery/signing/provider-maintenance
视为实现完成。descriptor1..9字节及最大构造版本仍9，descriptor10尚未登记，tuple13不能
取得实际activation。实现完整写入编排后再一次性开放对应reader能力；不提早广告或扩大
默认输入预算。未提交reservation、lost-response provider历史及真实集成验证仍OPEN。


### 2026-10-23：采用D复用原认证Native接受事实，完成committed OBJECT维护编排接线

主会话明确选择D：原完整proof/Profile/tenant/trust/Source权限经合格writer验证后，同批
COMMITTED NV38/owner/Message/首结果/POS是既有持久接受事实；无需GC重取或重签上传proof。
body13旧operation schema1/field9字节不动，新schema2/field10完整区分PREPARE_COMMIT与
DIRECT_SCHEDULE，实际同视图重关联全部保留依赖，不能以proofId/公开DTO代替权威。
DIRECT_SCHEDULE另要求原Schedule接受语义和原删除所有权；缺失保持保留，不伪造Prepare。
Root在原完整image/Profile/历史语义认证后私有构造opaque receipt；Source/cold各自实际重核
与权利认证。旧key过期/轮换不抹销合法已接受事实，不重建旧签名/key版本。

实际发现→签发→持久intent→当前Floor含pending/Pin/live→原provider条件I/O→hot保管→
Source确认生产者已接原maintenance/I/O/Source链，descriptor10登记精确operation2/origin1
向量；旧1..9默认及字节保持，无Native57/NV38扩展，现有Native56/54v6/lifecycle6/comp12
完整envelope不再升版。provider前真实durable operation-history资格仍默认拒绝；UNKNOWN/
late I/O/local stop不释放全局义务。此项取代原raw-proof持久载体等待，不替代真实外部集成，
未提交reservation、其它退休种类及全部集中/Broker/恢复/迁退验收继续OPEN，无完成提升。


### 2026-10-23：未提交 reservation 沿原上传组件持久关闭与完整终态事实

主会话裁定采用type14/kind4独立UNCOMMITTED_RESERVATION分支及原upload/Object Store的
持久admission/关闭/完整terminal inventory，不新增Native57、Source Upload事件或通用可变
ledger。业务身份复用实际Prepare/NV38/first/POS/binding；PUT/handle责任从真实接管前即纳入
同一原provider history，与CLOSED持久互斥。APPLIED intent在前，全部旧远端写者真实终结/
server fencing及完整清单先耐久，exact版本条件删除在后；确认保存完整关闭/清单/实际删除
材料。CLOSED_NO_OBJECT不等于NEVER_CREATED，旧UNKNOWN/已删历史不能由缺行补成功。

local expiry/closure stamp与原真实Source因果分开保存/认证，旧Frame1/body13/D不重释。
新body14/tuple14/descriptor11、Native56v2/54v7/lifecycle7/comp13一次登记；实际Source原批次
first/POS/history/owner/账务、正常及冷读取和原GC/I/O/Source独立准入全链承接，RELEASED
之后仍保闭合identity与必要原依赖。真实deployment provider/历史/fencing资格缺失默认拒绝，
单列OPEN；不能用方法形状/no-op签名/当前flag或长期收费替代完整仓内分支。

本次按该裁定接线，仍仅必要编译。没有实际上传关闭/DELETE/退款/激活/恢复/迁退操作或
验收完成声明；包2余项及完整四包/29切片/集中与真实验证终点保持。


## 2026-10-23：包2活动域留存与最终整域清退边界

主会话裁定活动Source/Store内有实际用途的原完整历史及计费身份是合法留存状态。
被实际Native/Source/recovery/query/accounting引用的first/POS/terminal/owner/binding/
history/descriptor保原owner与冻结费用；不表示泄漏、长期空间恒定或永久不可回收。
能够证明整个候选组无外部存续引用且原replay/query/retention/全部writer/Pin/image承诺
结束的实际闭合组仍属包2回收范围，不删引用指针制造资格，不改旧帧/tuple/历史语义，
不引入compact tombstone/替代证书。原零counter tombstone继续收费。

真实drain/非根identity retire Source写者仍是B4必做缺口。须沿原Source/control/Registry
停止新ingress的实际原因和StateAuthority有序DRAINING，保持已承诺工作排空；独立真实
ledger含零费用引用证明仅剩descriptor contribution且完整Floor/引用/权威都满足后，
按原§15.4同批删除descriptor、结清STATE/cardinality、更新四层计费并保原零counter。
本次回收first/history必须归仍有效的原Source/bookkeeping owner；不可给已删身份加费
或转移旧费用，无真实eligible候选时保留，不用伪造empty DTO构造正向。

当前bookkeeping root禁止在活动Store内退休。整Source lineage/Store终结与仍互相承担
历史义务的根组，归原包4import/cutover/retirement具体生命周期：新入口/旧writer/晚到
I/O、幂等/query/replay/Journal/恢复、全部Catalog/Pin/ancestry承诺均终结，仍需保管的
原完整历史须有既定可独立认证且收费的原保管位置。Target Close/DRAINING/最新Floor/
空counter均不代替整域关闭；包2不新增通用history-close服务或新账本，整域义务不删除。

allocation只用原Admission/外部owner/Writer/Journal的完整实际归属及不可替代终态。
缺basis旧余额保留并明确原责任组件/缺失事实，不能称永久费用已验收。原合法终态若
完整覆盖剩余整份allocation可沿原契约结清，不为形式新造分项账本。已批准K2 Anchor
独占原blobCharge沿kind3一次性结算；NV47删除/ref/alias消失使保守份额回Budget与后续
Source退款是两次不同变化，不重复物理退款，不扣独立Native费或maximumRecoveryCharge。
本实现自主落定body15/tuple15/descriptor12、Native54v8/lifecycle8/companion14。

本阶段按原四包/29切片，将现有清单一次分为：必须接通的独立实际写者/组装、存续引用
下合法留存、缺真实provider/history资格的不可执行路径、包4整Store清退事项。包3如实
暴露留存占用/有限额度/拒绝，不承诺历史费用可无限自动循环回收；真实drain/identity与
其它必需writer/provider缺口不可改名留存后宣称包2完成。集中测试/环境执行仍后置。


## 2026-10-23：drain必须消费明确原生命周期决定

主会话补裁：当前grant零limit仅是准入/容量前置条件，不是授权永久停止incarnation的
生命周期意图，不能自动转DRAINING。必须使用原已认证、Source有序的明确Control/Registry
生命周期决定（既有Close/轮换等实际原因）及持续到原子提交的真实StateAuthority；以后
grant增大不能复活DRAINING/退休identity，旧承诺工作与责任仍保留。

仓内本批选择已有Target Close确切原因：冻结完整实际Close/OPEN descriptor及原first/POS，
沿type14 kind3 action4/5自主落定body16/tuple16/descriptor13、Native54v9/lifecycle9/
companion15。退休另需真实descriptor-only独立fold、所有零引用与原held权威，exact零grant
同批移除，保零counter/Target total及仍活动Source归属的新增历史。真实闭合metadata组
退休与部署qualification仍须继续；没有授权环境删除、保护解除或绕开当前root禁退。


## 2026-10-23：保留实际first防重义务，不新增closed-ID协议

主会话核对后不采纳REJECTED COMMAND first整组迁入永久Native54 closed-ID收据的方案。
当前first按CommandId直接点读，仍承担换hash/deadline重新入场的防重职责；first缺失时
replay用incoming retryUntil判过期，后续invalid才按当前Policy校验，仍可能重新写first。
无存量业务引用不表示未来ID义务结束。SYSTEM first也有late-expired重写路径，本次不
新增SYSTEM physical-only分支。两类first继续原样收费，等待原Source/Store真正生命周期
关闭；不能再列为“可回收但缺writer”，也不把它们的整域清退从包4移除。

不为此增加wire/result kind/永久ID事实、全Native54历史查找或大组分阶段协议；尤其不
接受Native54扫描预算耗尽后无限等待Source承担新命令正常准入。RESULT查询副本和真正
独立的POSITION叶子继续按原契约回收，首次来源POSITION、业务/旧body11/12/13/14/
Native54/55仍需的记录保持原bytes/费用/校验，不用替代证书或缺行推断退休。

本批依据该裁定只接后续重复COMMAND POSITION叶子：自主落定body17/kind3 action6、
tuple17/descriptor14、Native54v10/lifecycle10/companion16，保实际first并强制候选
Source与sequence均严格晚于first且早于当前frontier；原Route/fence/完整Floor/Pin/
reader与外部引用保护继续。没有实现不采纳的closed-ID分支或改变既有查询扫描优先级。
多页仅推进独立叶子，不截断成完整组。不以每种record都具备删除case作为包2终点。
对其它metadata组先证明现有终态确已结束其义务；若只能发明新的永久ID/审计责任替代
原记录，则归存续历史，不能自动扩成新回收项目。


## 2026-10-23：E4认证启停、样本归属与完整覆盖边界

主会话批准E4既定的同Shard Source有序certification启用/停止，复用原Control Operation→
APPLY_SHARD_CONTROL→first/POS/Native原子链，独立tuple/descriptor和实际原Control认证。
配置只表达请求/能力，ACTIVE必须来自实际APPLIED；scope固定tenant/物理Source/Shard/
lineage和明确objective/population/path，完整immutable objective及预算/保留语义同代冻结。
重新开启或改变声明必须新持久observation generation，非Message generation或进程编号。

原v1 Start/Final/Outbox bytes、sampleId/EventIdentity和时间/timeout/merge语义全部保留。
实际Target业务batch同时冻结独立新归属：完整scope、generation、原activation/Control与
生效cut、objective、原权威业务Source/事件身份和v1 sampleId/startDigest/受保护内容。
COMMAND_APPLIED按exact Source；DUE_ADMISSION按原authoritative Admission/稳定capacity
决定和managed path。旧普通/legacy事件不得追认，重试/replay/晚Final不换原代；未支持的
objective/population/path拒绝声明，不静默少采或改v1范围。

启用前独立认证有限outbox/bytes/retention/collector custody、旧代并存和最大业务mutation
样本数/字节/Final增长空间；新增Control/绑定/历史/样本均真实计费。认证缺证据不能提交
业务Source或ACK、不能普通降级或越过Source序列提前应用STOP。STOP只停后续新观测，
旧Start/Final/timeout/outbox/collector职责继续并受有限并存资源约束，重启恢复同持久状态。

Start/绑定与原Command/Admission/quota同批；Target不能借legacy store seam直接后台改CF。
Final/ack优先复用既有权威业务Source，没有时沿原同Shard System writer/apply提交独立
有界观测事件；无第二日志/无序本地写者。原I/O/WorkClass在业务线程外导出，collector
先durable merge完整样本及归属/coverage材料，后exact Source ack；退款/删除仍需原保留
和Target原子保护。checkpoint/restore/独立ledger包含新投影及旧代义务。

普通metrics另用实际process/Worker incarnation与exporter epoch、实际启动Source的有界
cut/采集区间，进程前未知覆盖=UNKNOWN，gap=0非完整SLO证据，无逐消息outbox/高基数标签。
新consumer独立认证Control/归属后才用v1计算；每Source区间正向枚举应有样本/Final并完整
保管、无未知职责才COMPLETE，watermark/count/gap=0/空outbox/STOP/普通Delivery各自不足。
复用实际Source/业务/collector材料，不另造通用coverage oracle。真实部署权限/预算缺失
拒绝并列OPEN；仓内写者、启动/失效/退出/恢复和consumer接线仍须完整实施，测试后置。


## 2026-10-25：E4分母来自durable eligibility，保持Target reserved-work drain

主会话已现场核验TargetQuotaGrantGate与TargetPublishAdmissionStore并裁定：不新增Target
Admission逻辑容量拒绝，保持ADMISSION(false)→EXISTING_WORK_DRAIN和原Claim/reserve/
Attempt/Outcome契约。此前“稳定capacity决定”引用format-1既有hook，不要求复制不存在的
APPLIED/ADMISSION_CAPACITY_GATED或HARD_QUOTA业务结果，降额不得重新阻断已承诺工作。
DUE_ADMISSION_LAG/ALL_ACCEPTED必须源于durable Message/managed eligibility，不能缩成成功
Admission集合；原Source已有完整权威字段时同批物化Start，后续才取得独立真实材料时沿
已批准的有限SLO Source事件保管原事实和原观察generation，不新增业务拒绝或改Claim撤销。
尚未Admission的已接受eligibility也进入应有样本/timeout/coverage，旧普通事件不补当代，
缺原归属为legacy/UNKNOWN及不完整。物理/执行门禁事实只能来自真实原holder/epoch/事件/
时间材料，本地事实不能冒充已有业务Source，metric/异常/空队列/未Admission不能推导准确
capacity exclusion；无资格原因用BAD_EVIDENCE_GAP/UNKNOWN，timeout保留v1 BAD规则。
不要求每poll/tryAcquire失败一条Source，按原sample/event和revision有限合并，保留真实最大
事件量/bytes/time/retention及Final空间，缺观测预算阻断缺证据提交/ACK或拒绝开启，不改成
业务HARD_QUOTA。Final/consumer必须有完整ALL_ACCEPTED集合和保留/保管证明，Admission
images正向枚举不能代替从未Admission的eligibility覆盖。原v1与format-1行为保持，单agent、
非Fast、必要编译，集中测试/环境后置，descriptor15仍待完整能力和原资格后开放。


## 2026-10-25：用户允许多 agent，完整目标与验证边界保持

用户将目标中的单agent限定移除，并明确“你可以多 agent 了”。后续允许按文件归属并行
调查/实现，由主agent合并并统一必要编译、scoped commit及push origin main。此前非Fast、
实现优先/测试与真实Broker恢复等集中验收后置的要求保持；A–F/29切片及四包范围不缩减，
实现/集中验证/生产完成标志不能因多agent、编译或局部Source接线而提升。历史单agent记录
保留，当前executionPolicy.singleAgent改为false。
