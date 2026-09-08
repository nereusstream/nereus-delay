# NDIP-3 工作包：目标分区队列收敛与调度热路径改造

> **当前执行阶段：先完成全部实际运行代码，最后集中测试（用户于 2026-09-08 调整）。**
> 保持当前模型、单 agent、直接 main、分阶段 scoped commit/push。先贯通原 29 切片的
> 代码、运行路径装配、配置及迁移/清退工具；完整回归、真实 Broker、性能、故障恢复
> 和依赖验证的实际迁移/清退，留待用户切换模型后集中执行。当前只保留编译、格式/
> 文档一致性和影响开发正确性的最小局部检查；不需要现在确认或切换模型。
> 原目标总范围不变，实现状态与验证状态分别记录；未验证实现不得标记 VERIFIED、
> 提案治理 Implemented 或方案完成。

[原 29 切片集中验证交接清单](12-集中验证交接清单.md)

TargetQuotaGrantStore 已把首次 grant control 的签名/注册 verifier 接入真实 Store：
读取实际旧 activation/total，原子写入初次 allocation、activation、SYSTEM/POSITION、
全部费用和 source；明确业务拒绝只写结果。外部授权失败不转成持久拒绝。最小签名与
RocksDB 开发检查通过；root bootstrap、重复物理记录、完整 source/Worker 和实际
容量/commit guard 仍待完成，实现与集中验证状态继续分开记录。


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
