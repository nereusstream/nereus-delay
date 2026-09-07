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
下一项为 B4 quota。A2/A3、B4–B7、C–F 的未完成义务均保留，全部 29 项目标继续进行。

[29 项状态清单](progress.json) 是实施进度入口；[执行记录](04-执行记录.md) 保存实际命令、
源码身份、测试和环境结果。仅当全部必做实现及证据实际闭合才办理 Implemented。

原始输入 SHA-256：`3828585a94deea5bd37baa0f0258b7f83da9b2df6d8cc0a697fcfb07b7f331bf`。
导入保留全文与来源锁，Downloads 文件不承担后续仓库实施状态权威。

导入后的术语规范化：原示例 `Profile version 1/2` 使用显式 version 字样，以通过仓库
项目版本命名门；未改变 Profile 版本的业务含义。原始输入 digest 仅绑定导入来源。
