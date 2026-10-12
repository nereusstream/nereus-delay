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
