# NDIP-3 B3：Native 共同策略与签名契约

Status: Draft / 契约冻结；切片验收见 progress.json。

本文冻结原设计 §7.5–7.6、§7.10、§17.3 B3 的共同范围、签名、发布权限、固定 cap、
ordinary 回退和 trust/Admission 顺序。对应代码为 TargetNativeArtifactSet、
TargetNativePolicyScope/Snapshot/Head、TargetNativePolicyTrust/Authority 及
TargetNativePolicyChecks。C1/C2 接入 source、Claim、Admission、实际持久权威和通道；
这些纯契约实现不激活 Target writer，也不认证 Broker、旧环境转换或生产权限。

## 1. scope 的完整 canonical 输入

Native policy 的共同范围是 source Shard 下真实执行/控制域。它包含 dispatch 和 control
完整引用：授权、outcome、Journal/receipt、资源保证或独立门不同，不能仅因物理 Target
相同而使用一个 Native policy。schema=1 的 Native ordering 固定为 BEST_EFFORT。scope 不包含 Profile ID/version、消息、bucket、独立
Profile policy generation、channel generation 或凭据 lease。A/B Profile 的额外共同
Native 授权由 §3 的 MemberApproval 证明，不能从 scope hash 相同反推该授权。

TargetNativePolicyScope schema=1；reserved NV 24，meta key `11 01 + digest[32]`。
字段按编号递增编码、presence 必须精确，无 unknown/repeated/default omission：

| field | 内容 |
| --- | --- |
| 1 | uint32 scope schema=1 |
| 2 | 非零 policy authority namespace[32]，由受认证控制权威分配 |
| 3 | 非零 Control resource scope[32]，保持原 tenant/资源 RBAC 边界 |
| 4 | source Shard 原始 20 bytes：route incarnation[16] + u32be partition |
| 5 | TargetPartitionId[32]，按 §05 的完整物理 tuple 派生 |
| 6 | 非零 Target accounting incarnation[16] |
| 7 / 8 | uint32 domain slot 0..63 / 非零 raw uint64 generation |
| 9 / 10 | 非零 dispatchCompatibilityRef[32] / controlScopeRef[32] |
| 11 | 正的固定 nativeIndexLeadCapMs；不超过 Long.MAX_VALUE |
| 12 | allowed paths=MANAGED_HANDOFF=0x01；AUTO_FAST/其他组合在此拒绝 |
| 13 | 完整 TargetNativeArtifactSet，定义见下表 |
| 14 | SHA-256(`nereus-delay-target-native-policy-scope\0` + fields 1..13) |

scope 引用必须解析完整对象，核对物理 Pulsar Target、managed timing capability、完整
execution/control refs、source Shard 和 queue/domain/accounting/cap。queue 的 domain
nativePolicyScopeRef 必须为该 digest；Slot 复用、accounting 变化、cap 或 artifact 变化
均形成不同 scope。DRAINING 的历史工作仍保留原 scope，VACANT 不承载新 Native 工作。

ArtifactSet 是新 Target Native 协议的完整 schema/source-lock/environment 绑定，不
复用旧 ArtifactGenerationSet 中声明为 1 的 handoff snapshot generation：

| field | 内容 |
| --- | --- |
| 1 | artifact schema=1 |
| 2 | 非零 canonical Target schema bundle SHA-256[32] |
| 3 | 精确 Pulsar P1 source lock digest[32]，沿用锁定的 resource guard 契约 |
| 4 | 非零 raw uint64 environment reset generation |
| 5 / 6 / 7 | scope schema=1 / snapshot schema=2 / reserved Target Store format=2 |
| 8 | SHA-256(`nereus-delay-target-native-artifacts\0` + fields 1..7) |

schema bundle 由 B7 的完整新规范包确定；不能填任意非零 hash 或借旧 NDIP-1 receipt
获得授权。C1/C5 的 artifact authority 必须另外核验精确运行源码/环境资格。上述编码
绑定不替代 D/E 的 runtime/Broker 认证；旧 ArtifactGenerationSet/decoder bytes 不变。
AUTO_FAST 是 C5 明确管理的独立入口；消息即使允许两条路径，也只授予此 managed scope
中的 0x01，不派生另一条 Profile Native 前缀或获得 direct transport 权限。

## 2. 新签名 snapshot、head 与上限

TargetNativePolicySnapshot 使用 schema generation=2、reserved NV 25，meta key
`12 01 + snapshotDigest[32]`。旧 HandoffPolicySnapshot 保持 generation=1，拒绝新对象。

| field | 内容 |
| --- | --- |
| 1 / 2 / 3 | schema=2 / 非零 scopeDigest[32] / 非零 raw uint64 publication generation |
| 4 / 5 | mode：DISABLED=1、SHADOW=2、ENABLED=3 / 非负 effectiveLeadMs |
| 6 / 7 | 非负 validFrom / 严格更大的 validUntil，毫秒，均 ≤Long.MAX_VALUE |
| 8 | paths；DISABLED 为 0，SHADOW/ENABLED 为 0x01 |
| 9 | 完整 TrustedUtcIntervalEvidence，sourceId ≤256 bytes；issued.latest ≤validFrom |
| 10 | 非零 raw uint32 issuerKeyGeneration，保留完整 bit identity |
| 11 | 非零 TargetNativeArtifactSet.digest[32] |
| 12 | SHA-256(`nereus-delay-target-native-policy-snapshot\0` + fields 1..11) |
| 13 | Ed25519 signature[64] |

签名输入是 `nereus-delay-target-native-policy-snapshot-signature\0` + snapshotDigest[32]
+ u32be issuerKeyGeneration。新 schema、digest 域和签名域均独立；给旧签名换 field 1
或 scope ID 不会获得新权限。effective lead 必须落在 `[0, fixed cap]`；DISABLED 必须
lead=0，SHADOW/ENABLED 必须 lead>0。解析 snapshot 不构成 trusted；必须按 §3 核对
完整 scope、artifact、历史 issuer 与 exact activation。

TargetNativePolicyHead 是独立 current-head 值：field 1 schema=2，field 2 完整 snapshot，
field 3 authorizedLeaseUntilEpochMs，field 4 为 SHA-256(`nereus-delay-target-native-policy-head\0`
+ fields 1..3)。它保留该 scope 曾批准 ENABLED snapshot 的 validUntil 最大值。
`next` 要求首次 generation=1、后继恰好 raw +1、UINT64MAX 不回绕，并从真实前态重算
高水位。CAS backend 必须在同一个 revision 事务中重验此规则；先读再无条件覆盖不合法。

Publication 使用正的后端 revision，head ref 保留既有四字段结构
(scopeDigest、generation、snapshotDigest、revision)，禁止 revision=0。这是精确观察
引用；它不把旧 scope 或旧 snapshot 解释成新格式。CAS retry 冲突重新读当前完整头；
不得重置 generation、复制另一个 scope 的 head 或降低已授权 lease 高水位。

| 对象 | 保守 canonical 上限 bytes | 独立最大合法向量 bytes |
| --- | --- | --- |
| ArtifactSet | 121 | 121 |
| scope | 397 | 394 |
| snapshot | 646 | 644 |
| head | 695 | 693 |

最大向量覆盖 uint64/uint32 全宽值、slot 63、Long.MAX_VALUE 时间/cap、256-byte time
sourceId 和完整 signed time evidence。嵌套字段/字节有界；这些上限不代替 A2/C1 的
整个 source mutation/Admission envelope 预算。旧 Store format 1 和 NV 1..11 继续拒绝
有正确 CRC 的新 NV 24/25；本批不扩展 ValueEnvelope 或活动 DelayShard decoder。

## 3. 谁能发布、哪些成员能进入共同 Native 前缀

TargetNativePolicyTrust 的输入必须来自受保护的、source-bound 认证控制权威，不能从
请求 DTO 临时拼出后当作审批。权威 backend 的以下完整投影及读取语义固定：

| 投影 | 完整字段及不可变含义 |
| --- | --- |
| PublisherPermission | full scope、ControlAuthor(actor/role-set/resource scope)、非零 uint32 key generation、Ed25519 public key、正 maximumLeaseMs、完整 activeFrom SourcePosition |
| Activation | full scope、完整已签名 snapshot、完整 activeFrom SourcePosition；绑定整个 snapshot，不能只保存 generation |
| MemberApproval | 完整 B2 grant、完整 scope、完整 activeFrom、可选首次 closedAt；activeFrom 严格晚于独立 grant 激活，closedAt 严格晚于 activeFrom |

publisher 查找使用 exact scope digest、key generation、as-of source；activation 查找
使用 exact scope digest/generation，返回内容必须与签名 snapshot 逐项相同；member 查找
使用 exact grant digest/scope digest/as-of source。后端必须保存不可变授权与关闭历史，
按物理 source 身份读取，不能用当前 key、当前 Profile policy 或最后一个审批值替代。
同 offset 不同 canonical SourcePosition 为冲突，其他 cluster/resource/Shard 不可比较。

发布要求 TENANT_POLICY_ADMINISTRATOR + PLATFORM_OPERATOR，认证 actor、role-set、
resource scope 与已安装 PublisherPermission 完全一致，还须服务端证明涵盖整个
namespace/Target/source/控制范围。namespace 不因移除 tenant hash 自动成为跨租户授权。
每个 tenant/Profile 的共同 Native 许可必须通过其完整 MemberApproval 单独证明；
同一 scope 可以服务多个已明确批准的成员。B2 的共同 credential provider mode 本身
不授予共同 Native policy 控制权限。

`publish` 先从 trusted backend 解析精确已安装 permission，再核对角色/范围、source、
签名、key generation 和 `validUntil-validFrom <= maximumLeaseMs`，最后执行 current-head
CAS。低层 CAS 仅供该认证服务使用，不能作为绕过发布权限的外部 API。C1/C2 backend
接入时必须把上述读取/CAS 和权限/租约校验放在受 fence 保护的权威路径，保留其审批
来源与 source 激活证据。Java record 存在或测试 lambda 返回它均不是生产审批。

新 snapshot 只有在 Activation 可按 source 查到后才能成为 Native Claim/Admission
依据。发布已成功而激活未完成时只走 ordinary；恢复用签名中的完整 snapshot和保留的
activation，禁止依赖已被覆盖的历史 current head。issuer/permission/activation/member
记录必须保留到引用它们的 binding、reservation、attempt、checkpoint/recovery 保护解除。
关闭 publisher 的新发布权限不能删除仍需历史验证的公钥或许可。

C1 首次绑定先执行 B2 完整 grant/Profile/tenant/queue-plan 授权，再执行 firstBinding。
它核对原始 intent、完整 Profile/capability/physical Target 和所有 refs；这些损坏直接
报完整性错误，不能当成普通 policy 不可用。读取 MemberApproval 的后端失败必须重试
整个 source 应用，不能把未知状态持久化成 ordinary 决定。

| 条件 | 首次 Native 索引决定 |
| --- | --- |
| FORBID 或历史 omitted policy | ordinary；FORBIDDEN |
| 非 Pulsar 或不具备 managed timing 能力 | ordinary；CAPABILITY_UNAVAILABLE；原协议明确禁止的组合仍直接拒绝 |
| 固定 cap=0 | ordinary；FIXED_CAP_DISABLED |
| pinned Profile 最大 lead <cap | ordinary；PINNED_CAP_TOO_SMALL；不得缩小单个消息 cap 后混入前缀 |
| deliverAt <cap | ordinary；TIMESTAMP_BELOW_CAP，避免产生负的 action time |
| 没有共同 scope 引用 | ordinary；NO_COMMON_SCOPE |
| 未获该共同 scope 的完整成员许可，或首次绑定已关闭 | ordinary；NO_COMMON_MEMBERSHIP |
| 完整静态授权满足 | 建立按 deliverAt 排序的 initial Native sibling，绑定唯一共同 scope |
| Native permission 与 strict ordering 同时存在 | 沿用 ORDERING_CAPABILITY_UNAVAILABLE/原 intent decoder 拒绝；不放宽成新能力 |

上述 prospective binding 若判为 ordinary，最终持久 binding 不得留下 Native ref，
Native 索引也不得写入；诊断原因应保存/投影到请求结果或配置诊断。已经存在且引用了
scope 的 durable binding 缺失完整 scope 是损坏，不是静默 ordinary 回退。

MemberApproval.closedAt 仅阻止新的首次绑定，不成为已接受 Native head 的独立开关。
Prepare 继续使用最初 binding source；Commit 不重新按当前 approval/旧 Profile policy
取值。没有新共同授权的旧未 Admission 工作由 B6/F1 明确转换为 ordinary；旧 Admission、
snapshot、signature、Journal/producer/sequence 和 UNKNOWN obligation 原样保留。

## 4. Mode、trust、Claim 与 Admission 顺序

C1/C2 先完成 work/Message/locator、initial attempt/admissionsUsed、pinned binding 和完整
scope 的一致性校验，再调用本批纯 policy checks。非 initial/已消耗 Admission 的工作
不能重建 Native sibling。ordinary head 始终按自己的 due/retry 时间参与调度；不能把
无效 Native head 的等待当成整个域无 ordinary 工作。

| 当前状态 | 决定及唤醒 | 新 Native Claim/Admission |
| --- | --- | --- |
| ordinary 已 due | ordinary 优先，不读取 Native trust 阻止它 | 不转为提前发送 |
| 共同 publication 缺失或不可用、签名/trust/scope 不可信 | ordinary 等待/due，记录 POLICY_UNAVAILABLE 或 POLICY_UNTRUSTED | 禁止 |
| DISABLED | 共同 Native 分支停发；ordinary 独立 | 禁止 |
| SHADOW | 仅算 shadowWouldBeEligible；完整时间/lease 边界满足才为 true | 禁止，不接触 Producer |
| ENABLED、尚未 validFrom | 在 validFrom 或更早 ordinary due 唤醒 | 暂不允许 |
| ENABLED、尚未 deliverAt-lead | 在该 boundary 或更早 lease expiry 唤醒 | 暂不允许 |
| ENABLED，可信区间完全在 lease 内、earliest ≥actionAt、latest <deliverAt | 产生带 exact positive-revision head ref 的 Native 候选 | 可继续其他门 |
| 时间区间跨越资格/lease/due 边界 | 要求新可信采样；给出严格晚于旧 latest 的下一次采样时间 | 暂不允许 |
| lease 过期 | ordinary；POLICY_EXPIRED，policy 刷新事件或安全重查唤醒 | 禁止 |

Time evidence 必须来自已认证的时间服务/本机时钟配置，不能采用 client 自报的
TrustedUtcIntervalEvidence。时间 DTO 的合法编码不等于时钟认证。新的 policy generation
可改变 lead 和 mode；60→30 秒只改变共同 head 的资格及唤醒，不改所有 Message/index key。

校验时序固定为：

1. Candidate/Claim 使用当前完整 publication，并验证 exact scope/artifact、historical
   permission、完整 Activation 和签名；新的 Claim 再检 head/ref 与 source/Owner/Store。
2. Admission 先读 current head，必须等于 Claim 的 scope/generation/snapshot/revision；
   验证 source 位置下的历史 trust、完整有效 UTC 区间、ENABLED、managed path、fixed cap
   和 `actionAt=deliverAt-effectiveLead`，再读 current head 确认未变，冻结完整 snapshot。
3. C1 将该 snapshot、Claim/Message/binding/hash/identity 与 Admission 原子记录。此处
   的纯 freezeCurrent 只返回 policy；它不完成这些 source/Owner/Admission 业务提交。
4. physical ownership 前验证冻结 snapshot 和 Admission source 下的历史 trust，并要求
   当前完整 UTC 区间仍在其 lease 内。此阶段不读最新 current head、不改写 snapshot；
   可在原 deliverAt 后完成已批准 attempt，但不得越过有效 lease 去开始新的 ownership。
5. 进入 ownership/Journal OWNERSHIP_STARTED 后，lease 到期或 Disable 不制造 NOT_SENT；
   继续原 UNKNOWN、Journal、恢复与清退义务。保留所有已有强能力边界和单未决限制。

DISABLED head 中的 authorizedLeaseUntil 是既有 ENABLED lease 的高水位。只有已过该
边界并独立证明无新 ownership marker，才可宣告 EFFECTIVE_DISABLED；head 标量单独
不证明后者。scope、cap、namespace、dispatch/control 或 artifact 变化不原地重写旧
binding：由 B6/F1 显式映射/重建或新域/新 incarnation 承接，旧记录保持恢复语义。

## 5. B3 原始验收及证据

| 原 §17.3 要求 | 固定产物与测试 |
| --- | --- |
| scope 精确输入及旧 Profile 收敛 | §1/§3 完整范围和成员许可；两种 Pulsar execution scope vectors、不同 Profile/cap 同 scope、缺共同审批回退 |
| 新签名版本及发布权限 | §2/§3；独立 RFC 8032 签名、双角色/完整 source permission、旧 decoder/签名域拒绝 |
| 固定 cap、scope 变化与 ordinary 回退 | §1/§3/§4；小 cap、FORBID、strict、scope/cap/generation/identity/时间边界和无 policy ordinary |
| scope/snapshot 向量及 Mode 状态表 | 38 条标准库 Python 向量，最大 bytes/hash 和 Ed25519；Mode/SHADOW/expiry/共同 lead 测试 |
| trust 与 Admission 校验时序 | §4；完整 activation、历史 key/source、CAS/lease 高水位、两次 current head、冻结 snapshot 与租约终点 |

生成器 `scripts/generate-ndip3-target-native-policy-vectors.py` 不读取 Java 输出，先通过
RFC 8032 test 1 再生成项目向量；`checkTargetNativePolicyVectors` 纳入普通 check。
本批不实现生产 backend/运行装配；其完整权威输入、门顺序与拒绝语义已明确给 C1/C2。
B3 验收只覆盖原设计冻结范围，B7 接受、A2/A3、C–F 必做实现和实证均继续保留。
