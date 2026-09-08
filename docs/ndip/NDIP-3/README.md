# NDIP-3 工作包：目标分区队列收敛与调度热路径改造

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
