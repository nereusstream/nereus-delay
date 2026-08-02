# Nereus Delay 文档地图

这些文档不是互相替代的版本，而是分工不同的设计、协议、决策和证据层。

## 权威顺序

1. [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md) 是 V1 的语义、生命周期、恢复、资源和验收基线。实现必须满足它；它仍然有效，没有被状态文档取代。
2. [`V1-PROTOCOL-REGISTRY.md`](V1-PROTOCOL-REGISTRY.md) 是线上的精确契约：字段号、枚举值、canonical bytes、key tag、稳定错误码和 presence/union 规则。实现与 prose 有冲突时，以这里的可编码细节为准，并回到设计基线记录冲突。
3. [`adr/`](adr/README.md) 保存已经冻结的架构决策及其理由，例如 `deliverAt`、一 Shard 一 RocksDB、命令排队与应用分离、Source Position 顺序和 Lane 隔离。ADR 解释“为什么这样定”，不替代规范字段表。
4. [`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) 只记录当前代码、测试和剩余 blocker 的证据。它不能把“未实现”变成实现许可，也不能放宽设计或 Registry 的要求。
5. [`V1-DESIGN-AUDIT.md`](V1-DESIGN-AUDIT.md) 是跨文档审计和发布检查视图，用来发现规范、ADR、代码和证据之间的漂移；它不是新的协议规范。
6. [`CONTEXT.md`](../CONTEXT.md) 是术语和语义速查表，帮助统一 `deliverAt`、`actionAt`、Receipt、Source Position 等名称；它不新增规范，也不覆盖主设计、Registry 或 ADR。

## 按问题查文档

| 你想确认什么 | 应先看 | 它不负责什么 |
| --- | --- | --- |
| V1 必须保证的业务语义、生命周期、恢复、资源边界和验收条件 | [`Nereus Delay V1 设计.md`](Nereus%20Delay%20V1%20设计.md) | 不记录每个类当前实现到哪一步 |
| 线上字段号、枚举、canonical bytes、key tag、错误码和 union presence | [`V1-PROTOCOL-REGISTRY.md`](V1-PROTOCOL-REGISTRY.md) | 不解释完整业务背景和取舍过程 |
| 为什么冻结 `deliverAt`、一 Shard 一 RocksDB、Source Position、Lane 隔离等决策 | [`adr/`](adr/README.md) 中对应 ADR | 不替代 Registry 的精确字段契约 |
| 当前代码、测试证据和剩余 release blocker | [`IMPLEMENTATION-STATUS.md`](IMPLEMENTATION-STATUS.md) | 不把未实现项变成实现许可，也不放宽主设计 |
| 多份文档、代码和证据是否发生漂移，能否发布 | [`V1-DESIGN-AUDIT.md`](V1-DESIGN-AUDIT.md) | 不新增协议语义 |
| 某个术语在 V1 中的固定含义和禁用混淆 | [`CONTEXT.md`](../CONTEXT.md) | 不决定字段号、实现状态或架构取舍 |

因此，`Nereus Delay V1 设计.md` 不是废弃文档；它是“系统要成为什么样”的基线。其余文档分别回答“怎么编码”“为什么这样定”“现在做到哪”“有没有偏离”，是对它的分层补充。

## 如何阅读

- 想知道系统必须保证什么：先读设计基线，再查 Registry 的精确编码。
- 先遇到术语不清时：查 [`CONTEXT.md`](../CONTEXT.md)，再回到设计基线确认完整语义。
- 想知道某个决策为何存在：查对应 ADR。
- 想知道现在代码做到哪一步：查 Implementation Status，并以其中列出的测试/源码证据为准。
- 想做发布或评审：查 Design Audit，确认剩余 blocker 没有被误标为完成。

当前仓库仍是单 Gradle Java 21 工程；文档中的多模块目标和真实 Broker/Oxia/Object Store 集成会随着实现逐项变成状态证据，但不会改变上述权威分工。
