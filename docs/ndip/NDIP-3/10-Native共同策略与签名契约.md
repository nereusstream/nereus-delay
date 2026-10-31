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

C2 当前头的 Oxia 实现将每个 scope 持久化到
`{canonicalKeyPrefix}/target-native-policy/head/{lowercase-hex(scopeDigest)}`，值为完整
canonical head；Oxia `versionId + 1` 映射为正 publication revision。首次写使用
`IfRecordDoesNotExist`，替换使用 `IfVersionIdEquals`，并在返回前按精确 key、version 和
head 重读。Worker 得到的 authority 只读；发布服务持有 source-trust 与 scope-proof provider，
不会接受请求级 provider。Target Source Apply 现已把 publisher/activation/member 记录和首次
scope 注册原子持久化到 Source Shard Store，并提供受 Owner、ReadAuthority 与本地 applied-frontier
保护的 bounded trust 读取；首次 Schedule binding 使用同一 Store view 读取成员批准。Oxia
current-head 服务尚未与生产 Source Control/provider 装配，完整 Native Claim/Admission/Producer
链路仍未完成。

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

Source Control 操作固定为 `INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION=20`、
`CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION=21`、`ACTIVATE_TARGET_NATIVE_POLICY=22`、
`APPROVE_TARGET_NATIVE_MEMBER=23`、`CLOSE_TARGET_NATIVE_MEMBER=24`，对应
ApplyShardControl kind 19..23。全部绑定完整 scope 和单一 source Shard，使用
TENANT_POLICY_ADMINISTRATOR + PLATFORM_OPERATOR 与认证资源范围证明。Publisher 安装记录
捕获完整 ControlAuthor 和 source；关闭只追加 first closedAt 记录，不删除公钥或历史许可。
Activation 保存完整已签名 snapshot。Member approval 从同一受保护 Store view 解析完整 B2
grant，再保存完整 grant/scope/source；独立关闭保留首次 closedAt。编码字段、哈希和上限见
仓库的 [Protocol Registry](../../PROTOCOL-REGISTRY.md#source-ordered-target-native-policy-authority-controls)。
实际 Source Apply 将记录保存为 META tag 31..35 / NV42，并在一个 Target Store batch 中提交
authority、结果记录、Source advance 和精确 quota accounting。重复应用依赖既有 source replay
记录保持幂等；Native Trust 由 Worker Shard 从本地 applied history 构造，读请求不得超过 Store
frontier，且读取前后校验活动 Owner、外部 ReadAuthority 和 source frontier。这个接线只证明本地
Target Source Apply 与 first-binding 判定，不等于 Oxia Producer、Admission 或 Broker 生产装配。

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

### Target Claim v2 与 materialized Native Admission v5

普通 NV36 Claim 保持 schema v1。新 Native Claim 使用 schema v2：fields 1–12 的既有
身份事实保持，field 13/14 仍为 Claim ID/record digest，field 15 为完整
HandoffPolicyHeadRef。身份 preimage 是 fields 1–12 加 field 15；序列化按
1…15 排序。version=2 纳入原 ID/digest 域，不能把 v1 解码后重编码为 v2。ref 上限
90 bytes，正 Target publication revision；Claim envelope 增量最多 92 bytes。它连同
ref 进入实际 NV36 charge、嵌入 Admission 的 Claim proof、checkpoint/reopen 账本。
`nativeClaimSelector` 的 expectedHead 原样贯穿 Host/Shard/Store，并由最终 commit guard
重验；本地 operation digest 精确绑定该 ref。v1 Native 缺 ref 不后补当前策略。

Target 独立 type 13 的 operation body v5 保留 v3/v4 编码。其 field 22 是完整
TargetNativePublicationBinding，field 23 是完整 v2 Claim proof；激活 tuple 为
`(1,1,SYSTEM_MUTATION,1,5)`。Native binding schema v1 fields 1–18、可选 20、21 是
locator/physical/channel/attempt/Claim/Message/Profile/payload/metadata/window/Artifact；
23/24/25 分别完整 Target scope/snapshot/head，26 为 actionAt，27 固定
PULSAR_NATIVE_DELIVERY，28 为独立 Native prepared hash。不存在将 Target snapshot 转为
Lane 或把 ordinary binding 当 Native 的路径。binding 拒绝非初次、非 BEST_EFFORT、
非 Pulsar、非 Managed Journal 通道及 scope/domain/hash 不一致。

`TargetPublishAdmissionStore.prepareBody` 从实际 committed Claim、Message、binding、
physical、NV20/NV44 与历史 Native authority 准备 unsigned body；配置的 OWNER signer
检查实际 Source frame 上限后签名，同日志 handoff 仍等待真实 Source 首结果。
v5 首应用读取实际 v2 Claim 并匹配其 ref；Native actionAt 与 ordinary due 分别检查，
同批消费 Claim、写 Budget/Message/index/head/首结果/Source 和实际 quota。首次发送
使用原 APPLIED proof；首次 Journal marker 前重读 actual Store 并验证冻结 policy/credential
区间及 Admission Source 下的 trust，最后 SDK gate 保留。已有 ownership marker 不重送。

Native ACK 复用 generation-2 PULSAR_SEND_ACK，仍为 publishAttempt identity 与
MANAGED_JOURNAL sequence。必须匹配独立 prepared hash、Target/physical、Producer、
sequence/Artifact/P1 source lock，并由真实 provider 鉴别响应；Journal evidence/absence
保留原 Target cursor/fenced channel 精确绑定。成功 Outcome 三态保持，Source 按冻结
Native contract 投影 HANDED_OFF；ordinary v4 投影 PUBLISHED。resolution 使用独立
Native historical authority，已 terminal/new generation 的决策保持；definitive retry 只
产生 ordinary work，UNKNOWN 不释放。完整配置入口/实际 provider/SDK 接管装配仍属
包 1 未完成代码，以上不构成生产 activation 或 Broker 验证。

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


2026-10-13 开发补充：独立 Target BASELINE K1/P1 SDK、完整业务/reserved metadata、
最终 ownership gate 和实际 Source Admission/Outcome 单 flight 已连接原 Host driver/drain；
详见 07 的“Target BASELINE 实际 SDK 与 Source 单 flight”。本批三个相关 compile 通过，
全部测试继续后置；K2、完整启动 provider/resource 及跨进程恢复仍未完成，不提升验收权威。

## 6. Parent Control credit 的独立协议接点

Parent 读者扩展保留原 Source Control/历史权威边界；完整字段登记见
[Protocol Registry 的 Parent 扩展](../../PROTOCOL-REGISTRY.md#parent-control-credit-reader-extension)。
它不改变本文 Native policy 的 NV24/25、签名、scope 或 Claim/Admission 契约。

| 对象 | 本次明确登记及旧代边界 |
| --- | --- |
| descriptor22 | 仅既有 tuple23、Native63 schema1；独立 matrix 的 field58=2 声明 Root issuer policy2 codec |
| checkpoint | Parent-only 使用 lifecycle1/companion6；与高阶 lifecycle 共存仍按真实输入选 companion6–16 |
| Declaration schema6 | `parentControlCreditTarget` 最多12个显式 descriptor：原1–14最多4个、扩展15–22最多8个；32 tuple及1MiB上限不变 |
| 旧 Declaration | schema3仍4个原descriptor；schema4仍10=4+6、最高20；schema5仍11=4+7、最高21；全部拒绝22，旧bytes/digest/接受集合不变 |
| RootIssuerAuthorization2 | field1/13严格为2/2；显式非空子集13、14、23、30–34、36、37，最多10项/26 fields；专用 factory 必须显式含23 |

`TargetMigrationRootIssuerAuthorization` 的 policy2 使用 Registry 登记的独立v2
digest/signature域，完整 fields1–15 进入 digest，
原独立 management issuer 按本版本签名域签名；错 schema/purpose、跨域签名或未来版本
均拒绝，不回退旧域。policy1 继续使用1/1、原v1域、旧9项集合/cap9/25 fields；旧构造及
`signed` 保持policy1并拒绝23。完整原 plan/Source/Store/lineage/issuer/history 绑定及1MiB
history/原总bytes上限不变，已落 A1/旧policy不自动升级，也不补授原先没有的23。

新 A2 若嵌 policy2，原 owned-I/O/authenticate 必须取得 `plan.readerEvidence` 对应的完整
pre-A ReaderSet，核对 object ref/version/length/hash、exact plan/Source/Store/lineage、全部
eligible revisions/sessions 和真实 binary/SDK 锁，并用同一 held Runnable 重核至 commit。
所有读者同时具备 Declaration6+descriptor22/policy2 codec，以及同 base/source commits/
SDK JAR 锁下独立 descriptor15–21 的迁移数据 matrix；tuple35列表或Parent Native63
不能替代 Native32v2/迁移读者证据。缺材料保持 unavailable，旧已落 A 不重写。

Parent 仍须走原 Root 签名授权及真实 Native53 activation2+APPLIED first/POS：normal Root
保留实际 META10/initial grant/原 issuer，migration Root 保留实际 A/plan/root/原policy2 grant。
fund 严格晚于该23激活 Source；不借37同批例外。descriptor1–21、默认14/Native56广告、
Parent请求和Native63原bytes保持；22不授 child24–26、SLO、回收或迁移44–50 matrix。
以上登记不构成真实 Owner/完整 Source 资格、迁退或集中验证完成结论。
