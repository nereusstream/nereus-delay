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
