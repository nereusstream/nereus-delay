# NDIP-3：Target 身份与索引契约

Status: Draft / B1 IN_PROGRESS

本节固定 B1 的物理身份、基础/严格顺序 key、TargetQueueState 和域/head 摘要编码。
候选 work value、ORDER_STATE/消息定位、B2–B4 引用定义、协议激活及迁移转换
仍未闭合，B1 不作 VERIFIED。
本批 codec 尚未接入业务 writer。A2 的最大合法 mutation envelope 仍须包括 Message、
Lane、binding、source 与旧 inflight 依赖，不能由本文 Target 字节上限替代。

## 1. 物理身份

`CanonicalTargetPartition` 的精确 bytes：

```text
version:u8 = 1
lp32(BrokerResourceIdentity.canonicalBytes)
physicalPartition:u32be

TargetPartitionId = SHA-256(
  UTF8("nereus-delay-target-partition") || 0x00 || canonicalTargetPartition
)
```

BrokerResourceIdentity 沿用 Registry 的闭合 canonical protobuf oneof：
Kafka 是 field 1，内含 authenticated cluster UTF-8 与 native topic UUID[16]；
Pulsar 是 field 2，内含 authenticated cluster UTF-8、resource token[32]、
physical topic UTF-8 和 creation timestamp。分支本身决定 adapter，不重复存另一份 adapter
或 Kafka UUID 以制造两个可以相互矛盾的投影。不可添加未知字段或 trailing bytes。

cluster 最多 256 UTF-8 bytes；Pulsar physical topic 最多 1,048,576 UTF-8 bytes。
两者要求非空白、无 NUL、严格 UTF-8、NFC；不隐式规范化输入。Kafka 全零 UUID 和
Pulsar 全零 token 表示未分配资源，禁止构造 Target；creation timestamp 非负。
partition 使用完整 uint32 范围；Broker 支持的分区集合仍由独立认证 binding 检查。

最大 canonical tuple 的保守编码上限为 **1,048,897 bytes**：version 1 + lp32 4 +
oneof tag/length 4 + cluster field 259 + token field 34 + topic field 1,048,580 +
creation field 11 + partition 4。长度检查在分配嵌套资源数组之前执行；这里只是编码上限，
并不声称某 Worker 已认证容纳同样大小的消息或整个 head plan。

Target ID 不含 tenant、Profile ref/version、ordering key、unordered bucket、domain slot、
channel slot 或连接 generation。Profile 选物理分区的旧 `TARGET_PARTITION_HASH` 算法
保持原含义；它的 preimage 没有上述 0x00/version，不能拿其结果充当物理 Target ID。

旧 Lane 转换先严格解析**整个** canonicalLaneTuple，再提取资源/分区；不能截取一段
不验证的字符串。旧 Kafka 两处 UUID 不一致必须拒绝。转换器在合并之前还必须把提取
结果与被保护、独立认证的资源绑定逐字段比较；Pulsar token、creation、topic 或 partition
任何不一致均为冲突，不能仅因 topic 文本相同而合并。codec 的相等检查不提供认证权。
值或注册表保留完整 canonical tuple，与 ID 同时验证，hash 碰撞不得合并资源。

## 2. 基础 key 预留

沿用七个 application CF；数字均为十六进制 tag。新 key 不复用 Lane tag。

| CF / tag | 布局 | 长度 |
|---|---|---|
| timeline / 08 TARGET_DUE | 08 01 + target[32] + slot:u16be + domainGeneration:u64be + eligibleAt:u64be + sourceOrderToken + messageId[41] + generation:u32be | Kafka 106 / Pulsar 118 |
| timeline / 09 TARGET_NATIVE | 09 01 + target[32] + slot:u16be + domainGeneration:u64be + deliverAt:u64be + sourceOrderToken + messageId[41] + generation:u32be | Kafka 106 / Pulsar 118 |
| timeline / 0a TARGET_EXPIRY | 0a 01 + expireAt:u64be + target[32] + messageId[41] + generation:u32be | 87 |
| meta / 09 TARGET_STATE | 09 01 + target[32] | 34 |

DUE 的 eligibleAt 是 max(deliverAt, retryEligibilityAt)，NATIVE 永久按 deliverAt 排序。
本表的 DUE 是普通候选；严格 FIFO 使用下文独立的 ORDERED/ORDER_HEAD key，
barrier/value 和迟到规则仍需 B5 冻结，不能把 business order 改为 retry eligibility 排序。SourceOrderToken 沿用 Kafka `01+offset:u64be`（9 bytes）及
Pulsar `02+ledger:u64be+entry:u64be+batchIndex:u32be`（21 bytes），拒绝其他 kind/长度。
时间为非负有符号 long 范围；Message generation 和 domainGeneration 保留无符号位模式。

Domain slot 固定 u16，0..65535；**generation 必须非零**。基础单域从 slot=0、
generation=1 开始。slot 重用必须先解除旧义务，再 checked 增加 generation，禁止回绕。
B2/E2 的最大活动域数由已激活资源配置进一步限制；编码能表达 65536 个 slot 不代表
默认可启用该数量。slot 和 generation 均位于时间之前，旧 generation 不可混进新前缀。
前缀固定 44 bytes；同前缀内比较 time、完整 typed source token、Message ID、generation
的 unsigned bytes，与 RocksDB 顺序一致。所有时间索引都不复制 payload。

## 3. 旧 reader 的拒绝边界

为 Target Store 预留 **storeFormatVersion=2**。转换后 `meta/FIXED` 的 format marker
和 StoreMetadata.storeFormatVersion 都必须为 2，且与 checkpoint manifest 一致；
实际激活/转换/rollback 由 C1、B6、F1 接入。现有运行时仍只打开 format 1。

不能声称“新增 tag 即保证旧 reader 拒绝”：某些旧路径只扫描已知前缀。完整格式标记
是旧 reader 在开放 application 之前失败的必要边界。测试在独立 RocksDB fixture 写入
新 key 与 format marker 2（分别覆盖 identity 未转换和已转换），然后用本批未改动的
Lane ShardStore/StoreMetadata reader 重开，均必须拒绝。该测试证明当前基线 reader 的
行为；后续跨已发布二进制矩阵仍须锁定对应 artifact，不能由此代替。

## 4. 固定向量

向量在 `src/test/resources/ndip3/target-identity-vectors.properties`，独立生成器为
`scripts/generate-ndip3-target-identity-vectors.py`，使用 Python 标准库和显式 protobuf/CRC32C
wire 运算，不调用 Java 实现。Java 测试从该文件验证 canonical bytes、ID 和完整 key。

- Kafka cluster-a / UUID 00112233-4455-6677-8899-aabbccddeeff / partition 3：
  `dd0827a5a54f4ae24ef39ecebacac32b736f9015446df4a45c7d837a5e6cc43b`。
- Pulsar cluster-p / token 00..1f / persistent://tenant/ns/topic-partition-5 /
  creation 1700000000000 / partition 5：
  `9509556bf75007710a8f6987cfdfecd91b0fda7d39882b0fff6b46b270ae1c7d`。

额外测试覆盖不同 Profile/tenant/bucket/ordering key 合并、cluster/resource/partition
区分、Kafka UUID 投影冲突、Pulsar pin 冲突、未知版本/分支、非 canonical 文本、
编码长度上限、slot/generation 隔离及完整 uint64 高位排序。


## 5. 不可变身份记录与严格顺序 key

完整物理 tuple 放在独立 `meta/TARGET_IDENTITY`；注册、加载与迁移时按 key 重新推导 ID，
合并前还要逐字段比较已注册与拟注册的 canonical tuple，不能只比较 hash。后续 head
更新不重写完整 Broker 资源文本。凭证、Profile、tenant 授权与旧 attempt 继续保留原义务。

| CF / tag | 精确 key | 长度 |
|---|---|---|
| meta / 0b TARGET_IDENTITY | 0b 01 + target[32] | 34 |
| timeline / 0b TARGET_ORDERED | 0b 01 + target[32] + orderingDomain[32] + deliverAt:u64be + sourceOrderToken + messageId[41] + generation:u32be | Kafka 128 / Pulsar 140 |
| timeline / 0c TARGET_ORDER_HEAD | 0c 01 + target[32] + slot:u16be + domainGeneration:u64be + headEligibility:u64be + orderingDomain[32] | 84 |
| meta / 0a TARGET_ORDER_STATE | 0a 01 + target[32] + orderingDomain[32] | 66 |

orderingDomain 为非零 32-byte 身份，仍绑定 tenant、原 routing/binding 边界和 ordering key；
其完整 preimage/contract revision 在 B5 闭合。ORDERED 的 business order 不随 retry
改变；ORDER_HEAD 只暴露每个 orderingDomain 中允许服务的一个 head，按 eligibility
参与 execution domain 的普通选择。Target 的 ordinaryHead 可引用 DUE 或 ORDER_HEAD，
Native 仍只引用 NATIVE；严格顺序功能不会仅因有这些 codec 而生效。

为 Target Store format 2 预留 ValueEnvelope **type 12=TargetQueueState**、
**type 13=CanonicalTargetPartition**，NV envelope 版本仍为 1。type 13 payload 就是 §1
完整 canonical tuple，key 必须等于其派生 Target ID 的 TARGET_IDENTITY key。当前 Lane
ValueEnvelope 仍只注册 type 1..11，两个新增 type 只有 C1 接入 format 2 时才可写。
有效 CRC 的新 type 向量也被当前 reader 拒绝，不用损坏 CRC 冒充版本拒绝。

## 6. TargetHeadRef

所有字段必需，闭合 canonical protobuf，禁止未知、重复、乱序字段。

| field | 类型 / 含义 |
|---|---|
| 1 | bytes selection_key：完整 DUE、NATIVE 或 ORDER_HEAD key |
| 2 | DelayMessageId[41] |
| 3 | generation:uint32，保留全部位模式 |
| 4 | time_epoch_ms:uint64，非负 long 范围 |
| 5 | head_digest[32] |

field 5 = SHA-256(`UTF8("nereus-delay-target-head") || 0x00 || canonicalProtobuf(fields 1..4)`)。
DUE/NATIVE 的 key 必须与字段 2/3/4 完全一致；ORDER_HEAD 的 key 校验字段 4、
Target/domain，字段 2/3 必须在 C1 选中后与 ORDER_HEAD value、ORDER_STATE 和 Message
重新核对。解析 head summary 不产生发送权或证明 Message 尚未变化。

字段 4 对 DUE/ORDER_HEAD 表示普通 eligibility，对 NATIVE 表示 deliverAt。
DUE/NATIVE key 自带 typed source-order token；同一 Message 的 ordinary/native heads
必须 generation 相等、native 时间不大于 ordinary eligibility，并在 ordinary 为 DUE 时
保留相同 source-order token。Native head 的完整 Store 校验还要求物理资源为 Pulsar。

## 7. TargetDomainState

| field | 类型 / presence |
|---|---|
| 1 | slot:uint32，取值 0..65535；state 中受更小 schema/activation 上限限制 |
| 2 | domain_generation:uint64，非零、禁止回绕 |
| 3 | lifecycle：1 ACTIVE、2 DRAINING、3 VACANT |
| 4 | optional dispatch_compatibility_ref[32] |
| 5 | optional control_scope_ref[32] |
| 6 | optional native_policy_scope_ref[32] |
| 7 | optional ordinary_head:TargetHeadRef |
| 8 | optional native_head:TargetHeadRef |
| 9 | domain_digest[32]，必需 |

field 9 = SHA-256(`UTF8("nereus-delay-target-execution-domain") || 0x00 || canonicalProtobuf(fields 1..8)`)。
ACTIVE/DRAINING 必须有非零字段 4、5；字段 6 可缺省，存在时为非零 hash。字段 8
存在则必须同时存在字段 6、7；两个 heads 的 Target/slot/generation 必须一致，且角色
匹配。VACANT 禁止字段 4..8，包括显式空 bytes；保留 slot 和上次 generation。
B2/B3 的被引用对象需通过独立内容、授权和签名检查，32-byte ref 本身不提供 authority。

一个 Target 的 slot 分配从 0 连续增长，所有分配过的 slot 都保留轻量 history；
VACANT 也占 schema/activated slot limit。基础运行至多分配一个 slot，E2 完成前不能
以 decoder 支持多 slot 为由开大。schema 硬上限为 **64 slots**，资源配置可以更小。
因此不需要保存一个会无限增长的已释放 slot map，也不能把同一 slot 的两个 generation
同时作为现行域。旧 attempt 保护仍由原不可变 attempt 身份、Source/Owner/Store 和
独立 obligation 记录完成，摘要不能覆盖或重新命名旧 attempt。

结构转换固定为 ACTIVE→DRAINING→VACANT→ACTIVE；最后一步 generation 精确 +1。
ACTIVE/DRAINING 内的 generation 和三个 scope refs 不变；scope 改变需排空并重用
新 generation，snapshot/lease 的同 scope 轮换不改变 ref。新 slot 初始 ACTIVE/generation=1。
VACANT 保持空闲时不得改变 generation；禁止 ACTIVE 直接变 VACANT、DRAINING 恢复 ACTIVE、
忘记末尾空闲 slot 或 uint64 回绕。`requireSuccessorOf` 验证这些结构规则，但不证明
Claim、attempt、物理引用及恢复保护已排空；B2/C1/E2 仍必须提供真实退出 gate。

## 8. TargetQueueState

| field | 类型 / 含义 |
|---|---|
| 1 | schema_version:uint32=1 |
| 2 | target_id[32] |
| 3 | head_revision:uint64，非零 |
| 4 | source_ordered_control_version:uint64，非零 |
| 5 | admission_state：1 OPEN、2 PAUSED、3 CLOSED |
| 6 | accounting_incarnation[16]，非零；计费生命周期规则由 B4 闭合 |
| 7 | native_index_lead_cap_ms:uint64，非负 long 范围 |
| 8 | repeated TargetDomainState，按 slot 0..n-1 严格排列，n<=64 |
| 9 | state_digest[32] |

除 repeated field 8 可以没有元素外，全部字段必须出现。field 9 = SHA-256(
`UTF8("nereus-delay-target-queue-state") || 0x00 || canonicalProtobuf(fields 1..8)`)。
CLOSED 不能有任一路 head，仍可保留无可逆工作的 DRAINING slot 与独立旧义务；
PAUSED 保留候选但不得据此发送。Target/domain/head 不能互相引用其他 Target 或 generation。
状态不保存 deficit、ring、进程 round、健康、backoff、当前 wake 或 Producer secrets。

`decodeForStore` 必须给出 exact key、不可变物理 identity、source Shard 和已激活 slot
上限；它在 canonical/digest 校验后核对 key/Target ID、每个 Message 的 source Shard、
slot 数及 Native 的 Pulsar 资源条件。它仍不替代实时 Owner、binding、policy、Message
与 obligation 读取。B2 注册时须完整比较 identity tuple，不能只用本方法的 hash 比较
处理碰撞或冲突；C1 将不可变 identity 与首次 Target state 同 batch 安装。

在同一 accounting incarnation 内，任何已提交的 queue/control/domain/head 投影变更
都使 headRevision 精确 +1；纯 quota counter 变化不改本记录，空 poll 不构造后继写。
controlVersion 只能不变或精确 +1；admission state 改变必须 +1，CLOSED 不重新开放。
Target identity、accounting incarnation、native cap 在此后继关系中不可变；需要新的
计费生命周期时另走保护解除/重建协议，不通过构造一个不同 incarnation 绕过检查。
上述 revision 都使用完整 uint64 位模式：跨 Java signed 高位合法，最大值后禁止回绕。

## 9. 编码资源边界与未完成项

| 局部记录 | 可检查的保守上限 |
|---|---|
| TargetHeadRef canonical | 214 bytes |
| TargetDomainState canonical | 587 bytes |
| TargetQueueState canonical | 37,883 bytes |
| Target state key + NV envelope + canonical payload | 37,929 bytes |
| Immutable Target identity key + NV envelope + canonical payload | 1,048,943 bytes |

上限按每个 protobuf field 的 tag/最大 varint/length-prefix 与固定 key 宽度逐项求和，
代码常量用同一显式公式；root decoder 还在构造字段列表时限制总 field 数为 72，不能
利用 repeated 字段创建无界临时列表。最大合法宽度的 64-slot 独立向量实际 canonical
为 **37,626 bytes**，Java 编码和 Python 标准库参考 SHA-256 一致；保守上限包括
不能在本 state 同时达到的 slot/varint 额外字节，没有用任意 MiB 测试额度替代公式。

这些边界只覆盖本节编码，不是完整 A2 读取预算、实际 JVM/RocksDB 内存或认证容量。
A2 仍需 Message/runtime/source/binding/legacy 数据边界和强制有限装配；B1 仍需
Target timeline work、ORDER_STATE/消息定位、B2–B4 ref 对象和协议激活等完整契约。
原 Lane/Store format 1 业务路径尚未切换，真实 Broker、迁移、权限和最终完整目标均未完成。
