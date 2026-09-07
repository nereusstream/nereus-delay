# NDIP-3 B2：成员授权与 source 关联契约

Status: Draft / IN_PROGRESS

本节补全 `TargetScheduleBinding.membershipGrantRef` 引用对象及静态授权校验。
完整对象已实现；发放/关闭操作的 Control/SystemMutation wire 分配、认证权威记录与
source apply 装配尚未闭合，B2 仍为 IN_PROGRESS。本文不是运行时发送或生产权限凭证。

## 1. 权威必须证明什么

grant 由服务端权威对一个 authenticated tenant scope、精确 Destination Profile 和
source Shard 发放，包含 required 与选定 offered 的完整契约，以及所有额外独立控制门
和发送 permit 组。它不是由 gateway 自报集合、ProfileCatalog 查到相同 hash、Profile
名字相似或当前凭据恰好可用推导出的成员资格。

发放权威必须依据精确、不可变的 `authorityPolicyRef`，核验：

1. authenticated tenant 对该 Profile、物理 Target 和 source 路由的真实授权；
2. Profile/capability 的精确引用，required projection 与 offered 覆盖关系；
3. `controls` 列举所有会单独挡住该成员的额外发送控制和 permit 组；空集合需要证明
   没有此类独立门。不能用 mutable Pause 当前为 false 省略稳定的控制范围；
4. 该成员允许通过 offered authorization scope 下任一合法共同 credential provider
   执行，且 offered 所声明的结果、证据、timing 和资源最低保证受到所选策略约束。

第 4 项是共同通道使用授权，不是对真实 Broker 现状的永久认证。C2 首次发送和恢复仍
执行 live credential/protection/resource-guard/fencing 检查。若某成员还单独依赖其
原 Profile credential 的可用性，须保留独立 AUTHORIZATION_CONTROL 或拒绝共队；
不得发放声称其可使用任意共同 provider 的 grant 后在 hot path 添加隐藏私有门。

grant 可以随 tenant/Profile 版本不同而不同；这些字段不加入 Target ID、execution
domain compatibility 或 producer identity。运行时按绑定定位 grant，不遍历全部成员。
共同 provider Profile/secret generation 不固定在 grant 中；合法轮换由新 channel/lease
记录表示。旧 binding/grant/attempt 的不可变引用不因轮换改写。

## 2. 先准备注册内容，再记录实际 source 位置

`prepareRegistration` 在 append 前生成完整 canonical fields 1..8，不需要虚构 offset。
认证与授权通过的 source mutation 应携带这段精确注册内容；应用后 `fromRegistration`
附加实际 mutation digest 和完整 SourcePosition。这样 mutation 的 digest 不包含自身，
也不依赖 append 前尚未知的 source 位置。`requireSourceRegistration` 比较完整注册
bytes、operation ID、mutation digest 和 source canonical bytes；相同 offset 但不同
leader epoch/append time 不算同一证据。

闭合 canonical protobuf schema=1：

| field | 内容 |
|---|---|
| 1 | uint32 schema=1 |
| 2 | 非零 authenticated tenant scope[32] |
| 3 | 完整 Destination ProfileRef；ID≤256 bytes，semantic hash 非零 |
| 4 | 完整 required TargetDispatchCompatibility |
| 5 | 完整 offered TargetDispatchCompatibility，必须覆盖 field 4 |
| 6 | 完整 TargetControlScope；Target 与 field 5 相同 |
| 7 | 非零 authorityPolicyRef[32]；必须解析到权威实际使用的完整不可变策略 |
| 8 | 非零 authorityOperationId[32] |
| 9 | 非零实际 applied sourceMutationDigest[32] |
| 10 | 完整、受 B1 bounds 约束的 activation SourcePosition；Shard 与 field 6 相同 |
| 11 | SHA-256(domain + canonical fields 1..10) |

hash domain 为 UTF-8 `nereus-delay-target-membership-grant` 加一个零 byte。fields 1..8
全部必填，构成独立可准备的注册 payload；完整 grant 的 11 个字段全部必填。未知字段、
重复字段、非 canonical bytes、零 authority/ref、错误嵌套语义及超限均拒绝。

注册上限 2101194 bytes；完整 grant 上限 3149936 bytes。两个完整 dispatch、最大
control/permit 集合、最大 Profile 和最大 Pulsar source 的独立向量实际分别为
2101182 / 3149924 bytes。root 限 8/11 fields，ProfileRef 限 306 bytes/4 fields，
其他嵌套对象复用原有 bounded decoder。上限不是 A2 完整 mutation envelope 的认证。

Store key 为 meta `0f 01 + digest[32]`（34 bytes），NV type 22 仅预留。
`decodeForStore` 必须核对 key 和 Store source Shard；`decodeReferenced` 核对完整 digest。
活动 format 1 / NV 1..11 及七个 CF 不变，当前 reader 拒绝有效 CRC 的 NV 22。

## 3. Source 快照、关闭和业务拒绝

`TargetMembershipAuthority` 是 source-applied authority seam，不能由普通 ProfileCatalog
或未经授权的 map 提供生产实现。`resolve(exactGrantRef)` 返回完整授权 grant 和可选
`closedAt`；该关闭位置必须属于同物理 source，且严格晚于 activation。权威不可用或
存储损坏必须抛出并停止当前 apply，不能伪装成“没有授权”提交业务拒绝。

`TargetMembershipAuthorization.firstBinding` 在同一个待提交 source/Store/Owner 快照中：

- 缺少授权记录、未到激活后位置、已到关闭位置或 tenant 不匹配：`UNAUTHORIZED`；
- 返回了不同完整 grant、body/profile/target/required/offered/control 引用损坏：完整性错误；
- Destination 或 capability 在 binding source 尚未激活：
  `PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION`；
- 已 source-close 的 Profile：`PROFILE_DEPRECATED_FOR_NEW_USE`；
- 所有上述检查及原 payload/metadata/ordering 限制通过：返回静态 `OK`。

三个拒绝码均复用 Registry 已注册语义与 NEW_PREPARATION_REQUIRED；`OK` 不代表 retry
policy、Native policy、对象 proof、quota、Owner、预算和 source 原子提交已完成。
authenticated tenant 参数必须来自原 source/gateway 授权链，不能从调用者填写的 grant
反向获取。activation 与 binding 不允许同一 source 位置；raw uint64 source 顺序保留。

closure 只影响之后的首次绑定。旧 binding 按其原 bindingSource 重放；Prepare 已接受后
的 Commit 即使晚于 grant/profile closure，仍依据原 reservation/binding 和原有 Commit
规则校验，不能把 Commit 当作一次新的 Profile/grant 绑定。安全吊销已接受发送应通过
显式 source control gate 表达，不以关闭首次绑定记录偷偷改变历史消息语义。

## 4. 与 domain plan、通道和保护的关联

`requireBindingProjection` 核对 tenant、member Profile、grant ref、Target、完整执行/
控制 refs 及 grant activation 先于 bindingSource。`requireChannelProjection` 再核对
channel 的 source Shard、Target、accounting incarnation、domain generation 和全部
offered/control refs。共同 credential provider 另外通过 channel 的 `requireProjection`
及现有 credential 权威校验；投影 helper 不证明 live lease 或 resource guard。

注册规划首先按 §06 的有界规则选择域。source apply 必须从完整 snapshot 重算 plan；
`requirePlan` 关联其 queue revision/digest、chosen domain、accounting incarnation 和
精确 binding，并拒绝 CLOSED、复用 DRAINING、覆盖活动槽或跳过 slot/generation 历史。
复用域必须已保存同一 offered/control contract；新域以授权的 offered
contract 建立，不能将 Profile required 当作已认证的更强 offered。若既有最小可服务
域与所提供 grant 的精确 offered 不符，应重新准备该域的授权；不能静默扩大 grant。

grant 的关闭、Profile 首次绑定关闭、credential lease 过期、Message terminal、logical
channel refcount 为零均不能单独删除 grant。所有 binding/reservation/attempt/恢复与
checkpoint 保护解除后才允许按 C1/C2/B6/F1 引用退休协议回收。

## 5. 本批证据和未闭合项

独立 Python 生成器 `generate-ndip3-target-membership-vectors.py` 重算四类 capability 的
注册内容、grant、key、NV envelope 及最大完整记录；不读取 Java 输出。17 条向量由
常规 `checkTargetMembershipVectors` 只读验证。Java 回归覆盖 source 精确身份、完整
授权对象、控制组遗漏、tenant/Profile、关闭后历史重放、Prepare、unsigned source、
共享 provider、plan snapshot、bounds 与旧 reader 拒绝。

下一批 B2 必须闭合发放/关闭 Control/SystemMutation 的明确 wire 和认证注册记录、
authorityPolicyRef 的真实对象解析及完整授权校验输入，随后按原 §17.3 做整个 B2 审核。
当前 authority seam 的契约测试不冒充这些生产权威实现。C1/C2 的实际原子写入、有限
装配、通道池与 teardown 和 D/E/F 的 Broker/恢复/迁移证据继续保留原切片责任。
