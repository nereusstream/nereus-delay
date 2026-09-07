# NDIP-3：Target 身份与索引契约

Status: Draft / B1 IN_PROGRESS

本节固定 B1 的物理身份与基础 key 编码。完整 TargetQueueState value/presence/digest、
严格顺序索引、B2–B4 引用定义、协议激活及迁移转换仍未闭合，B1 不作 VERIFIED。
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
本表的 DUE 是普通候选，严格 FIFO 的 barrier/索引仍需 B5 冻结，不能将其简单改为
retry eligibility 排序。SourceOrderToken 沿用 Kafka `01+offset:u64be`（9 bytes）及
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
