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
