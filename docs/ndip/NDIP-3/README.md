# NDIP-3 工作包：目标分区队列收敛与调度热路径改造

> **当前执行阶段：先完成全部实际运行代码，最后集中测试（用户于 2026-09-08 调整）。**
> 保持当前模型、单 agent、直接 main、分阶段 scoped commit/push。先贯通原 29 切片的
> 代码、运行路径装配、配置及迁移/清退工具；完整回归、真实 Broker、性能、故障恢复
> 和依赖验证的实际迁移/清退，留待用户切换模型后集中执行。当前只保留编译、格式/
> 文档一致性和影响开发正确性的最小局部检查；不需要现在确认或切换模型。
> 原目标总范围不变，实现状态与验证状态分别记录；未验证实现不得标记 VERIFIED、
> 提案治理 Implemented 或方案完成。

[原 29 切片集中验证交接清单](12-集中验证交接清单.md)

Target Close 首次原子批次现写入绑定首个 NV39 marker 与 recovery lineage 的 NV40
持久游标；按 Target 的活跃 reservation 索引仍由 Prepare 建立、各终态删除。受保护的
Close 发现入口验证 marker、CLOSED queue、游标与双 ID，只读当前第一个剩余候选。
首候选由 Close 或到期物化时，游标与终态、原 payload owner、两 counter/total/aggregate
同批前移；非首候选终态不跳过索引。空扫描可用独立 field7 本地 mutation 记录完成，
不推进 source。按 Target 的 GC WorkClass action 现从持久游标重新发现一个候选，
共享读预算并按有效终态分流 Close/expiry；Owner 失权零写，重复任务读出已完成状态。
四个实际 Worker 场景及协议检查通过；首次 Close 13 条 native 写，带游标的 Close 物化
10 条。另用第二个 Target 的实际 grant/队列/签名 Close 验证空索引完成：写权限失败
零写，重提 field7 与两 counter/total/aggregate 共 5 条原子写、source 不变，重复零写。
GC 现还可每次有界读取一条按 Target 排序的 NV40：跳过已完成 Target、处理未完成
Target，扫到末尾回卷；轮转位置仅在进程内，重建时从持久 NV40 起点重新扫描。
两 Target 的四组实际 Worker 场景和有限预算/外来游标检查通过；关闭原 Store 后，
新 RocksDB/backend/GC executor 从 NV40 起点重新扫描两个 COMPLETE Target、零写回卷。
现另建第三 Target 的实际 grant、source-accounted 队列与签名 Close，使 OPEN NV40 跨
Store reopen 留存；新 GC 跳过两个 COMPLETE，再用 field7 将 OPEN 改为 COMPLETE，
恰写 5 条 native 记录且 source frontier 不变。这是空 Target 的本地未完成游标续跑；
Target reopen 现从持久 NV31 固定 root anchor 读取 NV33 root，核对 aggregate/source
frontier 后重建 backend；Target source runtime 可接纳此重建结果，无需测试保存原 root 身份。
Close GC 现可由 Target source runtime 在同一 WorkClass 图上构造，并在任务接纳和 native 写前检查
Owner lease/Store；本地 lease CAS 场景中旧 Owner 排队后失权零写，新 Owner epoch
重开后完成 OPEN 游标。Target reservation GC 现有调用方驱动的有界 turn：Close 与
普通 expiry 交替，每轮至多排一项，小预算下保留同一待处理任务。
`TargetWorkerShardRuntime` 现把重开后的 Target source loop 与 GC turn 装配到同一
Store/共享资源/WorkClass 图，两个入口均受 Worker 资源 admission gate 限制；四组
实际 Store 参数场景经此入口跑完原有 GC 序列。它仍需要上层持续驱动、Owner drain
和原生 source teardown；单 Shard 入口本身不创建 timer。
`TargetWorkerShardFleetRuntime` 另对同一 Worker 图的 Target Shard 分别轮转 source 和
GC turn；两 Shard 调用次序及失败后前进已做局部检查，真实 Store 的重开 GC 改经该
fleet 的单 Shard 路径。fleet 自身不触发周期或 Owner drain。
`TargetWorkerMaintenanceLoop.start` 现可用正数固定间隔与有限 SchedulerBudget 启动
单线程 GC tick；普通 turn/故障回调失败不会停止后续 tick，关闭先取消新 tick 并等待
正在执行的 GC turn 退出。尚无 Worker 启动宿主调用它，也未装配 Owner drain。
Target Worker 现可暂停新的 source/GC turn，并只对已经排队的 GC action 做有界续跑：
小预算仍保留同一任务，完成后再次调用不创建新任务。四组实际 Store 重开场景验证
暂停后不能再走 fleet 新 turn、pending 任务可完成且无额外 native 写。这是 drain 前的
队列收敛入口，Owner CAS/Store 关闭与真实宿主接线仍待完成。真实 Oxia 接管、带活跃
reservation 的恢复、生产宿主持续触发、
跨 Target 服务机会界限、expiry-first 混合顺序、关闭汇总转移、生产历史权限/配置/
factory、format2 回填、完整迁移/恢复和集中验证仍未完成。原 29 切片不缩减。

以下为前序认证入口实施记录；持久 writer 的当前进展以上述说明为准。

Target TIME_FENCE 的有界 body/ProofId 解码及首次认证校验器已实现：强制历史 Fence
writer/config/key、完整 source 与时间证据 provider，校验外层签名、retry 边界、安全余量和
interval width；外部认证失败原样传播。八项必要开发检查通过。本批不写 Store：Target
source 分流、持久 fence/metadata/结果计费同批、reservation effective expiry 与生产历史
权限/时间证据 providers 仍待实施，集中验证和原 29 切片状态不变。

Target reservation durable 查询已接入 QUERY WorkClass：同一 Worker registry 的任务
身份绑定 Shard/request/reservation 和完整读取预算，排队费用包含最大读取字节。队列拒绝
不获取外部权限、不读 Store；执行时在第一次读取前持有当前 ReadAuthority，结束再核对
Store view/权限。四项必要开发检查通过。公网认证路由、effective control overlay、生产
Owner/read providers、Object Store/upload authority 及完整 Worker factory 仍待装配。

TargetReservationQueryStore 已实现有界 durable 点查及读取屏障：实际 root/source、NV38
双 ID、expiry、binding 和 payload owner 核对通过后，才在当前 ReadAuthority 下返回快照。
可用服务端固定对象位置重建原 Prepare receipt，Cancel/Commit 不改 receipt anchor；
receipt 自身沿用摘要格式，签名仍属于 Commit proof。四项必要开发检查通过。公网 Query、
QUERY WorkClass/控制 overlay、Object Store adapter/upload authority 及完整生产装配仍待接入。

首次 Commit 已接入实际 Worker Source Apply：有界解码、原 Prepare/Object Store/trust-set
校验、Ed25519 首次及历史验签、COMMITTED 双 ID、Message/索引/head、payload RESERVED→ACTIVE
和结果/source 计费同批。成功 Commit 使用已有 reservation 的 drain gate，不重新选择
binding 或计入第二份 payload。九项必要开发检查通过；生产 proof/control providers、
签名 receipt/query/upload、正式 expiry/Floor/GC、完整恢复/配置/迁移和集中验证仍未完成。

reservation 的 Reschedule 已按主设计 §11.3 接入实际 Worker：先核对 reservation
身份/owner 与 generation=0/stateVersion 前置条件，RESERVED 返回 RESERVATION_NOT_COMMITTED，
ABANDONED 返回 ALREADY_ABANDONED；不创建 Message，不改变 reservation、expiry 或 payload
配额。结果/source 仍原子计费，同位置重放零写。Commit 与其余后续实现、完整集中验证仍待完成。

首次 Prepare 与 reservation Cancel 已接入实际 Worker Source Apply。NV38 保存原 Prepare
anchor、冻结排序契约、按 MessageId/reservationId 查询及到期索引；Prepare 不创建 Message
或投递工作，Cancel 原子转 ABANDONED、移除到期索引并将 payload 配额转入 retained。
业务、首结果、真实增量费用与 source 同 batch；PREPARE 配额拒绝沿用同一 ReadView 的
仅结果转换。九项必要开发检查通过。Commit、签名 receipt/query/upload、正式到期处理、
Floor/GC、生产认证 providers、完整恢复/配置/迁移和集中验证仍未完成。

下列分批记录保留各次实施时的范围；当前范围以上述说明和集中交接清单为准。

首次 Schedule 已接入 Worker：绑定准备、Message/payload owner、ordinary/Native/Expiry
与 strict head、首结果和真实 source 计费同批提交。逻辑配额拒绝在同一 ReadView 内
丢弃业务投影，转为仅结果/source 的持久拒绝；不可换视图沿用过时容量结论。九项必要
开发检查通过。Route/retry/对象证明等生产 provider、Prepare/Commit 与其余业务、完整
恢复/配置/迁移仍未完成，集中验证状态不变。

首次 Schedule/Prepare 的 Store 绑定准备入口 TargetScheduleRegistration 已实现：
同一有界 ReadView 核对实际 membership grant、Target quota allocation/owner、完整 domain
契约与 Native scope，重新计算注册计划并返回增量记录。它尚未接入首次业务提交；
Message/Reservation、结果/计费、生产认证快照与集中验证仍待后续完成。

首次 Reschedule 已接入实际 Worker Source Apply：可逆 Timeline/Claim 上 generation
递增、旧代 SUPERSEDED 历史、索引/head 更新、Claim 与费用清除、首结果和 source
同批提交；原 Schedule binding、payload owner 与 payload 费用保留。Policy 新增强制
DeliveryWindow；新 FIFO contract 拒绝不高于 Admission watermark 的排序键。七项必要
开发检查通过；生产 provider、其余业务、完整恢复/配置/迁移仍在实施，集中验证未完成。

无保留首结果的过期 Command 现经 Worker 重放分流写入独立物理 POSITION：不创建
COMMAND/RESULT，不调用首次业务 resolver；同位置核对完整 frame 后零写重放，后续
重复只增加物理 EVIDENCE 费用与 source。NV35 增加闭合 Kind 6，结果计费与完整结果
命名空间审计已接入。五项必要开发检查通过；reservation/其它首次业务、生产 provider、
完整恢复/配置/迁移等继续实施，集中验证状态不变。

TargetCommandStore 已接入 Worker 首次 Command 分流，当前实际执行已有 Message 的
Cancel：核对前置版本、原 binding/queue 与 source-ordered closure，保留不可逆 attempt
边界；成功时同批更新 Message、候选/head、Claim/charge、retained payload、terminal
历史及 COMMAND/RESULT/POSITION/source/计费。新增 terminal NV37，不复制 payload。
reservation Cancel、无首记录的过期 POSITION、其余首次 Command/System、完整生产
Route/closure/资源 provider 与恢复/配置/迁移工具继续实施；未提升集中验证状态。

TargetCommandReplayStore 已接入同一 Worker source 循环：保留的 COMMAND 首结果用于
同物理位置只读重放、后续重复 POSITION 计费、CommandId 冲突与过期返回；完整 Command
frame digest、实际 owner/lineage、source 与提交 guard 一起核对。Command/System 重放
及首次 grant 现在读取实际 ingress deadline fence。首次 Command 业务（含 Cancel）、
reservation/terminal、其余 System、完整 Worker/recovery/providers 仍待实现或接线。
仅两项必要开发检查通过，实现继续 IN_PROGRESS，完整验证仍待集中执行。

TargetSourceApplyRuntime 已接入 WorkerSourceApplyLoop 的单记录 poll/WorkClass/apply/ACK
链路，现覆盖 grant 首次应用及保留的 System/Command 重放；提交与再次 ACK 前核对实际 Oxia lease、
assignment/session、Owner marker、Store 和 source 连续性。未知 ACK 保留原记录，
重复路径不重查首次 grant authority。其余 Command/System 业务、恢复激活、完整 Worker
factory/config 与生产资源 providers 仍待接入，不把这条控制路径视为全 Source 完成。

TargetStoreBootstrap 已接入首条签名 SHARD grant 的空 Store 初始化：全 CF 空态核对、
source-derived root、activation、SYSTEM/POSITION 与全部费用/source 原子写入，commit
成功后才返回 backend/root/result。StartAuthority 必须证明完整 source 起点和全批资源，
不能以空目录跳过历史义务；生产 provider、Worker/factory/recovery 接入仍待完成。
实现及验证状态保持分离，本批仅扩展一项签名/RocksDB 开发检查。

TargetSystemReplayStore 已接入 grant source 分流：同一物理位置核对 first/POSITION/
envelope 后经只读 guard 完成，后续重复只追加 POSITION 和实际费用，不重执行业务。
过期重复保留首次结果；真正首次应用独立复核不存在条件。最小 RocksDB 开发检查
通过，完整 Source/Worker、bootstrap 起点/容量生产 provider、读写 guard 与恢复验证仍待完成。


TargetQuotaGrantStore 已把首次 grant control 的签名/注册 verifier 接入真实 Store：
读取实际旧 activation/total，原子写入初次 allocation、activation、SYSTEM/POSITION、
全部费用和 source；明确业务拒绝只写结果。外部授权失败不转成持久拒绝。最小签名与
RocksDB 开发检查通过；后续批次已补 bootstrap writer 和重复物理记录，完整 source/Worker
及实际起点/容量/commit guard 仍待完成，实现与集中验证状态继续分开记录。


TargetQuotaStoreGate 已从同一有界 Store view 读取实际 Shard/Target activation、
root/首次 allocation descriptor 与跨 incarnation totals，再执行逻辑额度策略。
MessageStore 增加 granted 入口，Claim/revoke 自动接入；额度拒绝不会写 Store。
完整 source 业务分类、拒绝结果持久化、签名激活/物理资源 guard 和 Worker 仍待接线。
实现保持 IN_PROGRESS，完整验证保持 PENDING_CENTRAL_VALIDATION。


新增 TargetClaimRecord/TargetClaimStore，将实际可逆 Claim/revoke 接入 Message、
ordinary/native/strict head、INFLIGHT 与冻结费用的同一 Store batch；current Claim
可按 Message.claimId 直接点查。编译和一项记录转态开发检查通过；实际混合 Store
提交、Owner/Native/permit 生产 guard、物化/Admission/Outcome、恢复与 Worker 装配
仍需完成或集中验证。implementationStatus 保持 IN_PROGRESS，验证保持待集中执行。


最新计费实现：TargetRecordAccounting 从实际 before/after 与冻结 owner 推导贡献；
TargetSourceAccounting 自动生成 primary/mirror 差额和 bookkeeping inventory 更新，
TargetLocalClaimAccounting 装配不推进 source 的本地 Claim/revoke 计费。它们通过
TargetMessageStore.prepareAccounted/applyAccounted 接到实际 Store 提交。完整业务
source/签名/grant/物理资源权威、Claim record 和 Worker 装配仍未完成，不提升验证状态。


最新实现批次：TargetMessageStore 已连接实际 Message/index/order/head 投影与
TargetStoreBackend 的原子提交。ordinary/native sibling、稳定 Expiry 和严格
serviceable head 从完整 before/after 自动生成；完整计费/认证业务 planner 与
Worker source/Claim/Admission/Producer 装配继续实现。仅执行一项实际 Store 开发
smoke，未执行集中回归，不提升 C1/C4/E5 的验证状态。


- 提案状态：`Draft`；完整实施已由用户于 2026-09-07 明确授权，契约冻结和接受凭证在 B7 闭合。
- 审查基线：`main@b521b614fbe22d30381593637e60ab615e5906cc`。
- 范围：A–F 全部 29 个切片，包含此前 P2 工作、最终真实 Broker/恢复验证、转换器和清退机制。
- proposal、环境迁移、性能测量与 production rollout 按 Accepted NDIP-2 分轨。
- `productionAuthority=false`。NDIP-1 已关闭；其历史凭证不认证本方案的新 runtime。

## Normative package

1. [完整详细设计](01-目标分区队列与调度热路径设计.md)：原始 1648 行全文，包含 §17 全部任务及 §17.8 覆盖表。
2. [实施计划](02-实施计划.md)：依赖、交付边界及验证要求。
3. [代码级设计](03-代码级设计.md)：实际调用链、接口与状态决定，随切片同步。
4. [Target 身份与索引契约](05-Target身份与索引契约.md)：B1 已固定的物理身份、key、queue/domain/head/work/Message/runtime/Expiry/ORDER_STATE 编码及拒绝边界。
5. [执行与控制兼容契约](06-执行与控制兼容契约.md)：B2 已实现的规范化执行/控制引用、有界注册规划和稳定拒绝；切片验收按原设计 §17.3。
6. [Schedule 绑定与通道身份契约](07-Schedule绑定与通道身份契约.md)：精确 source/body、固定通道/lease scope、旧 attempt 冻结和 teardown 边界。
7. [成员授权与 source 关联契约](08-成员授权与source关联契约.md)：完整 grant、pre-append 注册内容、source 快照/关闭和精确绑定关联；生产后端装配由 C1 完成。
8. [成员策略与认证控制契约](09-成员策略与认证控制契约.md)：完整 policy、双签名/角色/资源授权、发放/关闭 wire、首次应用和原 B2 验收对应。

9. [Native 共同策略与签名契约](10-Native共同策略与签名契约.md)：完整 scope/artifact、独立签名、发布/成员权限、Mode/ordinary 回退和 trust/Admission 顺序。

10. [局部 Quota 与增量计费契约](11-局部Quota与增量计费契约.md)：B4 IN_PROGRESS，独立 counter/value、局部 revision、纯增量规划和原验收剩余义务。

README、执行状态、用户决策记录、测量和 receipt 不进入 normative package。
当前完整设计保留导入时的 Draft/PLANNED 描述；它是启动基线，不代表新增权限障碍，
也不代表代码已经完成。后续 normative 变更与接受凭证必须绑定精确 bytes。

## 当前进展

A0/A1 已验证，A2 正在实施；READY discovery、head mutation/control/Claim 的共享预算、
提交前视图校验及 source/recovery 本地重试已通过专项与完整检查。最大合法 mutation 的
资源上限证明和正式装配的强制有限配置仍未完成，兼容构造器不作为资源认证。
B1 身份与存储契约已验证：物理身份、候选/严格顺序 key、TargetQueueState 与域/head
摘要、generation 历史、完整可逆 work、消息定位、SourcePosition、Message/runtime/Expiry
及 ORDER_STATE/barrier 编码均由独立向量验证。完整检查 1780 项 Java 测试，失败/错误 0，
41 项外部环境 skip；Target 六类专项共 70 项零 skip，常规 check 已包含独立向量校验。
B1 的验收范围按原设计 §17.3，后续责任见字段契约 §19：B2–B6 引用与行为、C1/E5 原子
mutation、格式激活和 B6/F1 转换继续完成。B1 VERIFIED 不代表提案接受、Target 新 runtime
或完整方案完成；A2 仍为 IN_PROGRESS。

B2 按原 §17.3 设计/契约验收为 VERIFIED：执行/控制 scope 的完整对象与独立向量、有界注册规划、五个稳定拒绝码
已实现。已有 offered contract 可以覆盖较弱要求，独立 Pause/credential/permit 集合
保持不同域；注册规划绑定 queue revision/digest，禁止 DRAINING 绕过与 generation 回绕。
完整 Schedule/Prepare binding 与不可变 channel/lease identity 已实现，续期保持 producer/
sequence 域且禁止覆盖旧冻结身份。完整 membership grant 编码及 source 快照校验已实现；认证发放/关闭 wire、完整策略与授权校验输入已冻结；活动 Store/source 路径尚未接入这些对象，实际通道池/teardown 由 C2 实施。
兼容规划基础批次完整 check 通过：1800 项 Java 测试、失败/错误 0、外部 skip 41；B2 专项 20 项
零 skip，加上 B1 70 项共 90 项 Target 契约回归。证据见 `evidence/b2-compatibility-results.json`。
绑定/通道批次增加 18 项专项；完整 check 为 1818 项 Java 测试、失败/错误 0、外部 skip
41，Target 八类共 108 项零 skip。精确证据见 `evidence/b2-binding-channel-results.json`。

membership 批次增加 13 项专项；完整 check 通过 1831 项 Java 测试、失败/错误 0、
外部 skip 41，Target 契约九类共 121 项零 skip。源码、独立向量和检查结果见
`evidence/b2-membership-results.json`。

认证策略/Control 批次增加 15 项专项；最终完整 check 为 1846 项 Java 测试、失败/错误
0、外部 skip 41，十类 Target 契约共 136 项零 skip。原 §17.3 七项验收逐项绑定契约与
测试，B2 VERIFIED 仅代表该设计/字段范围；精确证据见 `evidence/b2-results.json`。
C1 权威后端/原子提交、C2 通道运行、D–F 真实验证与迁移清退继续实施，B3 进展见下文。

B3 的共同 scope、独立 schema/signature 域、完整 artifact 绑定、发布角色和 source trust、
固定 cap/ordinary 回退、Mode 状态表及 Admission 冻结顺序已形成契约与可执行校验。
38 条独立向量覆盖 RFC 8032 签名和最大输入；B3 按原 §17.3 设计/契约验收为 VERIFIED。
C1/C2 的真实权威后端、状态提交及通道/ownership 仍按原切片完成。

B3 最终完整 check 通过：1866 项 Java 测试、失败/错误 0、外部 skip 41；Target 十一类
契约共 156 项零 skip。精确源码、规范、38 条独立向量和原 B3 五项验收对应见
`evidence/b3-results.json`；本次完整 JUnit XML 保存在 `evidence/b3-junit.zip`。
B4 quota 已开始：独立 identity/counter/aggregate、有限增量规划与恢复算术核对已实现，
21 项专项通过。完整业务 delta、grant/计量 artifact 与保护规则仍待冻结，B4 保持
IN_PROGRESS。A2/A3、B4–B7、C–F 的未完成义务均保留，全部 29 项目标继续进行。

B4 counter 基础批次完整 check 通过：1887 项 Java 测试、失败/错误 0、外部 skip 41；
新增 21 项专项零 skip。24 条独立编码项与最大样本摘要、源码和全部 297 份 JUnit XML
归档见 `evidence/b4-counter-foundation-results.json`。B4 未冻结验收，下一步仍是其原始
完整计费/grant/保护规则。

B4 继续增加固定计量 artifact 与 attempt budget：UNKNOWN 保留 execution/reserve，
确定结果与 checkpoint-safe reserve 转实占分开，retained 减额要求独立释放权限；
retained payload 由 Message Identity 唯一持有，不按 attempt 重复预留。完整 delta 表
见 §11 契约的 §9。Owner/bookkeeping 与 incarnation 保护仍未全部冻结，B4 保持
IN_PROGRESS；本批新增 schema 仍未进入活动 Store。

B4 计量批次完整 check 通过：1906 项 Java 测试、失败/错误 0、外部 skip 41；
新增 19 项专项、连同 counter 基础共 40 项 quota 测试零 skip。30 条独立向量、
全部 298 份 JUnit XML 和精确来源见 `evidence/b4-accounting-results.json`。

[29 项状态清单](progress.json) 是实施进度入口；[执行记录](04-执行记录.md) 保存实际命令、
源码身份、测试和环境结果。仅当全部必做实现及证据实际闭合才办理 Implemented。

原始输入 SHA-256：`3828585a94deea5bd37baa0f0258b7f83da9b2df6d8cc0a697fcfb07b7f331bf`。
导入保留全文与来源锁，Downloads 文件不承担后续仓库实施状态权威。

导入后的术语规范化：原示例 `Profile version 1/2` 使用显式 version 字样，以通过仓库
项目版本命名门；未改变 Profile 版本的业务含义。原始输入 digest 仅绑定导入来源。

B4 继续实现跨 incarnation Target total（NV 29/meta 16）、完整 scope/grant artifact 与
逻辑入口/排空策略。旧 incarnation 的 retained 费用持续占用同一 Target 总额；一次局部
变更只 point lookup 受影响 Target。Grant source 激活契约已实现，完整 owner/bookkeeping 和
incarnation 分配/回收保护尚未全部冻结，B4 仍 IN_PROGRESS；新对象尚未成为活动 writer。

本批完整 check 通过：1929 项 Java 测试，失败/错误 0，外部 skip 41；新增 23 项、
共 63 项 quota 专项无 skip。18 个独立向量条目、全部 299 份 JUnit XML 与精确来源
保存在 `evidence/b4-scope-results.json` / `b4-scope-junit.zip`。B4 未办理 VERIFIED。

B4 grant control 批次已实现完整 prior/transfer 关联、operation 18 / ControlKind 17、
NV 30/meta 17 激活记录、认证注册与首次 source 应用校验，以及提交前精确视图 guard。
强制 capacity authority 的契约包含静态切分、物理预留与父 transfer plan；生产后端和
新 Target Store 原子应用仍由 C4 实现。旧 Lane Store 拒绝新分支且重开后结果保持。

本批完整 check：1952 项 Java 测试，失败/错误 0，外部 skip 41；新增 23 项、共 86 项
quota 契约无 skip，包含 Control 支持回归的专项运行共 113 项。74 条独立向量及全部
300 份 JUnit XML、检查前后不变的 1158 个输入见 `evidence/b4-grant-results.json` 与
`b4-grant-junit.zip`。B4 继续闭合唯一 record owner/bookkeeping、有限容量与 incarnation
分配/回收等原验收义务；没有活动 Target runtime、Broker 或新 Store 恢复认证。

B4 bookkeeping 批次已实现 NV 31/meta 18 的 Source Shard root anchor，固定 counter/
aggregate/total/activation 及自身的存储预留；真实退休/零 usage 记录仍占槽位，tenant
mirror 不重复加总。Attempt budget 的记录费用由其冻结 Target owner 独立承担，五阶段
包括 RELEASED 都保留到受保护的实际删除。上限绑定完整物理 source、schema 与 artifact，
不通过 usage 编码长度递归计费。

本批完整 check：1971 项 Java 测试，失败/错误 0，外部 skip 41；新增 19 项、共 105 项
quota 契约无 skip。45 条独立向量、全部 301 份 JUnit XML 和检查前后不变的 1162 个
输入见 `evidence/b4-bookkeeping-results.json` 与 `b4-bookkeeping-junit.zip`。B4 仍为
IN_PROGRESS：其它业务记录 owner、payload/identity 保护、完整 reserve sizing 和
incarnation 生命周期尚未闭合；C4 实际 inventory/Store/recovery 与删除后端未实现。

B4 payload 批次已固定唯一 Message owner（NV 32/meta 19），原 TARGET/incarnation/tenant/
accounting 跨 generation 保留。Reservation、active、retained、released 只移动一次
payload 费用；完整绑定与强制 Source/Floor/ledger authority 保护转换，RELEASED 后
owner record 仍计 STATE。详见 §11 契约 §13。完整 check：1987 项 Java tests，失败/错误
0、外部 skip 41；新增 16 项、全部 121 项 quota 契约无 skip。38 条独立向量、302 份
JUnit XML、1166 个不变输入见 `evidence/b4-payload-results.json` 与 `b4-payload-junit.zip`。
B4 仍 IN_PROGRESS；完整业务 reserve、其它 owner/ledger、unique cardinality 和
incarnation 生命周期继续闭合，尚无实际 Target writer/恢复、Broker 或迁移认证。

B4 Message record 批次已将既有 owner/binding/Message/work/Expiry 完整 key/value
连接到冻结 owner 的 STATE 费用，ORDER_HEAD 另验实际 serviceable state。子集硬上限
6 条，拒绝重复 key、混合 owner/Message 视图，C4 须在真实 Store guard 中重验完整
CF/type/key/value。没有新 NV/tag 或活动 writer。详见 §11 契约 §14。
完整 check：2001 项 Java tests，失败/错误 0、外部 skip 41；新增 14 项、全部 135 项
quota 契约无 skip。25 条独立向量、303 份 JUnit XML 和 1170 个不变输入见
`evidence/b4-message-records-results.json` 与 `b4-message-records-junit.zip`。B4 仍为
IN_PROGRESS；其它业务 owner、完整容量、unique cardinality 和 incarnation 生命周期
继续闭合，partial subset 不是完整账本、Source 授权或恢复认证。

B4 incarnation 批次增加 NV 33/meta 1a 的完整 source-derived origin、一次 ingress drain、
固定 descriptor 记录预留，以及实际 Queue/OrderState 的局部唯一计数来源。退休核对
counter 恰剩自身费用、最新 Floor/root/aggregate 和强制完整 ledger authority，不执行
实际删除；当前 Store root 不可独立退休。详见 §11 契约 §15。
完整 check：2020 项 Java tests，失败/错误 0、外部 skip 41；新增 19 项、全部 154 项
quota 契约无 skip。23 条独立向量、304 份 JUnit XML、1174 个不变输入见
`evidence/b4-incarnation-results.json` 与 `b4-incarnation-junit.zip`。B4 保持 IN_PROGRESS；
必须先定义独立且无循环的 allocation source action，再让 membership/channel 注册引用
其结果 ID。该控制协议、Queue 轮换/legacy handover、其它 owner 与完整 reserve 继续闭合。

B4 首次 grant allocation 批次复用已签名 Control 18/Apply kind 17，NV 30 field 7 保留
完整 OPEN 历史 origin、field 8 digest；更新不重分配，零初始额度不创建 descriptor。
Frozen lineage、total absence、自身 STATE/incarnation 费用和强制 capacity authority
纳入计划。Activation root 槽位预留两个 source，完整规则见 §11 契约 §16。
完整 check：2029 项 Java tests，失败/错误 0、外部 skip 41；全部 163 项 quota 测试
无 skip（新增 9 项）。304 份 XML、1174 个不变输入及两组更新向量见
`evidence/b4-grant-allocation-results.json`。B4 保持 IN_PROGRESS；实际 Result/Store
后端、root bootstrap、轮换/handover、剩余 owners/reserves 和原验收绑定继续完成。

B4 metadata 批次已绑定十类现有 META 记录的唯一 owner、实际字节 STATE/cardinality
和最多四个完整点读依赖；不新增格式或 active writer。共享记录保留 first allocation，
incarnation-bound records 保持自身身份；依赖不重复收费。完整规则见 quota 契约 §17。
完整 check：2041 项 Java tests，失败/错误 0、外部 skip 41；全部 175 项 quota tests
无 skip（新增 12 项）。305 份 XML、1176 个不变输入与复用向量见
`evidence/b4-metadata-results.json`。B4 继续闭合其它 owners/reserves、source 配方与
原验收；C4 真实 before/absence/atomic/recovery、D/E/F 义务不由本批代替。

B4 local Claim 批次将 source sequence 与 local ordinal 分开，Claim/revoke 不改变
source 派生 incarnation，最多四个 counter 并强制实际 Claim authority。完整规范见
quota 契约 §18。完整 check：2056 项 Java tests，失败/错误 0、外部 skip 41；全部
190 项 quota tests 无 skip，新增 15 项。306 份 XML、1179 个不变输入见
`evidence/b4-local-claim-results.json`。B4 仍 IN_PROGRESS；实际 Claim/Result 格式、
完整 reserve、authority/atomic Store/recovery、D/E Broker 和 F 迁移清退继续实施。

B4 Claim 计费投影批次已冻结原始 Target work、Claim ID、Owner/Store 与 owner/artifact/
execution charge，独立计量 STATE 并校验本地撤销/source 消费，见 quota 契约 §19。
完整 check：2067 项 Java tests，失败/错误 0、外部 skip 41；201 项 quota tests 全部
无 skip，新增 11 项。307 份 XML、1183 个不变输入见 `evidence/b4-claim-charge-results.json`。
B4 仍 IN_PROGRESS；实际业务 Claim 与其它 owner/reserve、原子接入/恢复及 D/E/F 继续。

B4 首次结果与 POSITION 审计批次已冻结五种 Kind 的 owner/source/reference 和费用，
成功分配结果可返回完整 OPEN origin，见 quota 契约 §20。完整 check：2082 项 Java
tests，失败/错误 0、外部 skip 41；216 项 quota/result 专项全部无 skip（新增 15 项）。
308 份 XML、1187 个不变输入见 `evidence/b4-result-record-results.json`。B4 仍
IN_PROGRESS；其它 owners/reserves/交接配方、实际 C4 source/query/dedupe/atomicity/
recovery 与 D/E/F 工作继续。

B4 结果账本独立核对批次新增有限恢复 fold，验证 owner/source/首记录引用并从完整记录
重建 contribution；同时修复 accounting artifact 独立解码后的值比较，见 quota
契约 §21。完整 check：2095 项 Java tests，失败/错误 0、外部 skip 41；229 项专项
全部无 skip（新增 13 项）。309 份 XML、1189 个不变输入见
`evidence/b4-result-audit-results.json`。B4 仍 IN_PROGRESS；实际完整 Store 权威、
跨账本恢复/atomicity、其它 owners/reserves/交接配方与 D/E/F 工作继续。


### 实现优先执行安排与 Store 后端

用户已调整为先贯通全部实现，最后切换模型集中验证；保留单 agent、直接 main 和
分阶段提交推送。[集中验证交接清单](12-集中验证交接清单.md) 覆盖原 29 切片。
progress.json 独立记录 implementationStatus 与 validationStatus，未验证新实现
不升级 VERIFIED 或提案治理 Implemented；历史证据仅适用于原绑定源码。

C4/C5 开始真实 Store 后端：显式 ShardStore.openTarget 创建/打开 format 2，拒绝
原地接管 format 1；TargetStoreBackend 将业务 exact read-set 与 quota/total/
aggregate/source 写入同一 RocksDB batch，并提供完整结果 namespace 遍历。当前只做
编译及两项最小 Store smoke；Worker/Claim/Admission/Producer 装配、完整业务 authority、
跨账本恢复、checkpoint/restore/migration 与集中验证尚未贯通，不是生产入口完成。

2026-09-23 的 Target Worker 增量已在实际 Store 场景中接入 source/Close/expiry GC、
固定间隔维护组件与单 Shard 本地 Owner drain。drain 拒绝 pending source entry，
小预算保留旧 GC task；正常路径经精确 Owner DRAINING CAS、Store flush/close 和
lease release。替代 Owner 不被释放，写结果不确定时先关闭旧 Store。四组本地参数
场景通过；pending ACK 的真实 Broker 消解、checkpoint、真实 Oxia/Broker、
恢复和生产关闭接线仍开放，C2/C5 与整份 NDIP-3 状态不提升。

drain 前可用 `settlePendingSourceTurn` 只重试保留的 source entry，空时不 poll；
已完成 Store apply 的 ACK 重试不要求新的业务 admission。真实 Broker ACK UNKNOWN、
失权与重投的关闭配方仍待完成。

本地 Target drain 另通过 lease transition/release「CAS 成功而响应丢失」的返回空与
抛错重试用例。format 2 Manifest 已可规范编码/解码，但 Target 控制快照与发布/
恢复通路仍缺；现有 format 1 通路不能证明 Target 快照可恢复，真实 Oxia/Broker
故障证据仍待完成。

全 fleet 本地 host 现启动定时 GC、提供 source 单轮入口，并在关闭时等待在途 GC
后逐 Shard 运行 source settlement 与 Owner drain。单 Shard 撤出也会等待原在途
turn，之后仅 drain 该 Shard；其它 Shard 在其 drain 期间继续接收 source/GC。
两 Shard 可控调度测试通过。宿主还可在同一 Worker 图中动态加入 Shard，或在旧实例
完成 drain 后以同一 ShardId 替换；迟到的旧实例撤出请求被拒绝，已完成的 Store
不会在整宿主重试时再次 drain。上述仅有本地生命周期测试；生产 assignment 驱动、
进程接线、真实 Broker/Oxia 接管和 format 2 checkpoint/restore 仍开放。

现有 scheduled/drain checkpoint 执行器及旧 `ShardStore.createCheckpoint` 入口仍提前拒绝 Target format 2
Store，避免旧链先写入不可发布的 checkpoint ID；format 2 的可恢复发布链仍待实现。

format 2 Manifest 现可规范编码/解码；上传、Catalog/Oxia 和下载/安装继续提前
拒绝其发布或接管，直到 Target 控制快照、物理校验与恢复权威闭合。

format 2 的只读物理根核对现可在有限文件预算下检查真实 RocksDB 镜像中的
bookkeeping、root、aggregate 与 source frontier；本地真实 Worker Store 镜像通过，
持久 mutation sequence 被改写后拒绝。第二只读入口把 format 2 Manifest 的文件清单、
DB/Store 身份、lineage、checkpoint ID、Owner epoch、source/sequence 和 evidence cursors
与镜像绑定，局部错配均拒绝。它尚未审核所有 Target 投影、认证控制/语义摘要或接入
Catalog 发布与 restore/install，不能作为 checkpoint 成功证据。

同一只读镜像还可在有限记录数和 key/value 字节预算下核对配额投影：NV31 inventory、
primary counter 到 NV27 aggregate/各 Target total 的精确求和、tenant mirror 和 mutation
先后关系。真实 Worker Store 通过，篡改 aggregate、删除 mirror 或耗尽预算会拒绝。
另一个有限只读扫描现可用与写入路径相同的冻结计费规则，从实际业务记录重算各
primary/tenant counter；真实 Store 镜像通过，删除 POSITION 结果后虽配额投影仍自洽，
账本核对却拒绝。Manifest 身份、配额投影和账本可在同一次只读打开中核对。
四组实际 Schedule/Claim/Reschedule/Close 的关闭后 Store 镜像也通过本地账本核对。
同一完整 DEDUPE 扫描现送入结果账本校验：检查规范键、同源事件与 mutation、首结果
引用、Owner 描述符，并将结果贡献小计与实际逐记录计费对齐。真实 Worker 和命令路径
镜像通过，结果冲突的局部负例仍拒绝；此步骤不认证外部 Owner/控制权威。
受保护零用量退休身份、固定控制/语义投影和其它 Target 关系仍缺认证与完整审计，
C5、Catalog 发布及 restore/install 门禁不提升。

format 2 Target Store 现能用显式 checkpoint ID 生成**本地物理候选镜像**：创建过程中
沿用 Store 的 checkpoint 容量槽与失败回滚，镜像在原子落位前必须通过有限物理文件、
配额投影和实际业务/结果账本审计。真实 Worker Store 的候选镜像通过；非法无界预算
零写，审计预算失败撤销 checkpoint ID 且不留下候选目录。此入口没有 Owner/CHECKPOINT
调度授权、认证控制快照、Manifest 发布、Catalog 或恢复安装权限，旧 format 2 门禁保持关闭。

本地候选现还绑定调用方 lineage 与实际 Store/source 与 native write 切点、META7 checkpoint ID、
META8 opened Owner epoch、META6 evidence cursors；没有已打开 Owner 或已应用 source
时在写前拒绝。同一 checkpoint ID 响应丢失后，可在完整只读审计成功且上述身份未变时
零写复用现有目录；错 ID/lineage 或任何后续 native 写均拒绝。该复用核对本地持久事实，不认证当前外部 Owner
lease，也不解除 Manifest/上传/安装门禁。

本地 Target 候选现在可经同一 Worker 的 `CHECKPOINT` 队列执行：提交和执行时核对当前
session-bound ACTIVE Owner lease、精确 pending upload intent、Store/ID/lineage 和有效期，
物理操作后再次核对。实际 Worker Store 的排队创建、零写复用与排队后 Owner/intent
失效拒绝已通过局部测试。生产 checkpoint 调度与 source cut、完整 Owner 身份组装、
认证控制/语义快照、Manifest/Catalog 发布和 restore/install 尚未贯通；C5 和 format 2
发布/恢复门禁保持原状态。

format 2 候选的完整账本扫描现逐项核对固定 META 1–9 的类型、身份与规范字节，
并拒绝旧 compatible control/reader 键 10–14；旧控制快照的正式写入口也对
format 2 写前拒绝。真实 Store 注入旧快照时审计失败，删除后恢复通过。
Target 专用控制/语义快照及认证摘要仍未实现，发布与恢复门禁不变。

活动 Target Source runtime 现以实际 Owner Lease、Store 和已绑定的 WorkClass 图提交本地候选，
候选动作前后复核该 runtime 的本地 fencing/时间守卫。真实 Source ACK 后的创建、
零写复用及排队后 DRAINING 零写拒绝通过。生产宿主调度、pending ACK 处置、
Broker cut、完整控制/语义认证及发布/安装仍待完成。

本地 format 2 候选的完整账本扫描现要求当前 grant activation 对应实际保留的首条成功 System 结果，并对齐 source mutation、ID/hash 与首次 allocation。真实 Store 中规范编码但错配的 activation 仅在完整审计被拒绝。控制请求认证、结果 Floor 清理后的证明和发布/恢复链仍待完成；C5 门禁不变。
