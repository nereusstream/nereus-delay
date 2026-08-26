# Nereus Delay 文档地图

这些文档不是互相替代的版本，而是分工不同的设计、协议、决策和证据层。

## 权威顺序

1. [`Nereus Delay 设计.md`](Nereus%20Delay%20设计.md) 是当前语义、生命周期、恢复、资源和验收基线。实现必须满足它；它持续演进，不按整体版本复制。
2. [`PROTOCOL-REGISTRY.md`](PROTOCOL-REGISTRY.md) 是线上的精确契约：字段号、枚举值、canonical bytes、key tag、稳定错误码和 presence/union 规则。实现与 prose 有冲突时，以这里的可编码细节为准，并回到设计基线记录冲突。
3. [`adr/`](adr/README.md) 保存已经冻结的架构决策及其理由，例如 `deliverAt`、一 Shard 一 RocksDB、命令排队与应用分离、Source Position 顺序和 Lane 隔离。ADR 解释“为什么这样定”，不替代规范字段表。
4. [`DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md`](DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md) 把 ADR 0043/0044 映射到 Gradle 模块、Java API、signed RouteSnapshot、Gateway 幂等记录、Kafka/Pulsar patch 点、失败分类和逐切片测试门。它是代码级实现蓝图，不覆盖主设计或 Registry。
5. [`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) 只记录当前代码、测试和剩余 blocker 的证据。它不能把“未实现”变成实现许可，也不能放宽设计或 Registry 的要求。
6. [`DESIGN-AUDIT.md`](DESIGN-AUDIT.md) 是跨文档审计和发布检查视图，用来发现规范、ADR、代码和证据之间的漂移；它不是新的协议规范。
7. [`proposals/`](proposals/README.md) 保存 NDP。重大跨模块变更先形成提案；Accepted 后直接同步当前主设计、Registry、ADR、实现和 gate，不形成另一条版本线。
8. [`CONTEXT.md`](../CONTEXT.md) 是术语和语义速查表，帮助统一 `deliverAt`、`actionAt`、Receipt、Source Position 等名称；它不新增规范，也不覆盖主设计、Registry 或 ADR。

## 按问题查文档

| 你想确认什么 | 应先看 | 它不负责什么 |
| --- | --- | --- |
| 当前系统必须保证的业务语义、生命周期、恢复、资源边界和验收条件 | [`Nereus Delay 设计.md`](Nereus%20Delay%20设计.md) | 不记录每个类当前实现到哪一步 |
| 线上字段号、枚举、canonical bytes、key tag、错误码和 union presence | [`PROTOCOL-REGISTRY.md`](PROTOCOL-REGISTRY.md) | 不解释完整业务背景和取舍过程 |
| 为什么冻结 `deliverAt`、一 Shard 一 RocksDB、Source Position、Lane 隔离等决策 | [`adr/`](adr/README.md) 中对应 ADR | 不替代 Registry 的精确字段契约 |
| Direct SDK/Gateway 如何复用语义、Kafka/Pulsar client 改哪些类和怎么验收 | [`DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md`](DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md) | 不证明这些生产模块已经实现或通过 release gate |
| 当前代码、测试证据和剩余 release blocker | [`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) | 不把未实现项变成实现许可，也不放宽主设计 |
| 多份文档、代码和证据是否发生漂移，能否发布 | [`DESIGN-AUDIT.md`](DESIGN-AUDIT.md) | 不新增协议语义 |
| 重大设计为何以及如何改变 | [`proposals/`](proposals/README.md) | 不替代被同步更新后的权威设计 |
| 某个术语在当前系统中的固定含义和禁用混淆 | [`CONTEXT.md`](../CONTEXT.md) | 不决定字段号、实现状态或架构取舍 |

因此，`Nereus Delay 设计.md` 不是废弃文档；它是“系统要成为什么样”的基线。其余文档分别回答“怎么编码”“为什么这样定”“现在做到哪”“有没有偏离”，是对它的分层补充。

## 如何阅读

- 想知道系统必须保证什么：先读设计基线，再查 Registry 的精确编码。
- 先遇到术语不清时：查 [`CONTEXT.md`](../CONTEXT.md)，再回到设计基线确认完整语义。
- 想知道某个决策为何存在：查对应 ADR。
- 想按类、线程和仓库切片实施双入口/guarded transport：查代码级详细设计，再回到 Registry 核对 exact wire。
- 想知道现在代码做到哪一步：查 Implementation Status，并以其中列出的测试/源码证据为准。
- 想做发布或评审：查 Design Audit，确认剩余 blocker 没有被误标为完成。

当前仓库仍是单 Gradle Java 21 工程；详细设计中的多模块拆分、Delay Gateway、Route authority 和真实 Kafka/Pulsar guarded transport 都是待实施切片，会随着代码与测试逐项变成状态证据，但不会改变上述权威分工。

Gradle 的 `checkDocumentation` verification task 会在 `check` 中执行，验证上述
权威文件存在、文档地图没有丢失主设计入口，并且主设计、Protocol Registry、ADR、
Status 与 Audit 使用同一个当前设计基线修订号，并验证 Accepted NDP 与版本命名残留 gate。
