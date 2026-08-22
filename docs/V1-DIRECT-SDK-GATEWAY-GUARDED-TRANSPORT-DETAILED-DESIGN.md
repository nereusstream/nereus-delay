# Nereus Delay V1 双入口与 Guarded Transport 代码级详细设计

状态：Accepted implementation blueprint
Spec revision：`V1-FROZEN-2026-08-13`
日期：2026-08-13
适用范围：Direct Java SDK、可选 Delay Gateway、Kafka/Pulsar Command Transport、现有 V1 Worker

本文把“共享语义内核 + Direct SDK/Gateway 双入口 + Kafka/Pulsar 通用受保护发送”落实到类、模块、线程、协议、失败分类和测试切片。它是主设计的代码级实现蓝图，不改变 [`V1-PROTOCOL-REGISTRY.md`](V1-PROTOCOL-REGISTRY.md) 已冻结的 NDL1/NDR1 字段、Command Hash、Source Position、状态机和 RocksDB key；如果实现需要新增公共 wire 字段，必须先在 Registry 中登记，不能以本文中的 Java 草图绕过 Registry。

相关权威决策：

- [ADR 0043](adr/0043-share-one-semantic-core-across-direct-sdk-and-delay-gateway.md)：Direct SDK 与 Gateway 复用一个语义内核；
- [ADR 0044](adr/0044-use-first-class-guarded-broker-transports.md)：Kafka/Pulsar Client 提供通用的资源身份保护能力；
- [ADR 0024](adr/0024-use-versioned-self-routing-identities-and-a-fixed-hash.md)：继续使用现有 V1 self-routing ID；
- [ADR 0038](adr/0038-pin-broker-resource-incarnations.md)：资源 incarnation 必须在真实 Broker 请求边界校验。

## 1. 最终边界

目标结构固定为：

```text
Direct Java SDK --------------------+
                                     |
Light SDK -> Nereus Delay Gateway ---+--> Entry Composition
                                              |
                                              +--> Semantic Core
                                              |      +--> RouteSnapshotProvider
                                              |      +--> PreparedCommandFactoryV1
                                              |      +--> SubmissionPlannerV1
                                              |               |
                                              |        PreparedSubmissionV1
                                              |               |
                                              +--> Submission Coordinator
                                                     +--> CommandTransportRegistry
                                                            +--> Guarded Kafka/Pulsar
                                                                      |
                                                               Command Topic
                                                                      |
                                                              Existing V1 Worker
```

职责不交叉：

| 层 | 负责 | 明确不负责 |
| --- | --- | --- |
| Semantic Core | Route 选择、ID、Shard、canonical Command、AUTO_FAST 分支冻结 | 网络监听、认证、Broker 连接池、physical attempt、Worker 状态 |
| Submission Coordinator | physical attempt ownership、exact transport lookup、generic result 到 NDR1 三态投影 | Route 重选、Command 重编码、Gateway 请求幂等 |
| Direct SDK | 本地快照缓存、准备、transport 组合、backpressure、可选 durable outbox | 集中租户认证、全局 Gateway quota |
| Delay Gateway | 认证、tenant 推导、请求幂等、集中凭证、配额、审计、调用同一 Semantic Core | 重写 Command 语义、维护另一套路由算法、把 HTTP 200 当作 queued |
| Kafka/Pulsar Client | 网络、batch/retry、连接、指定物理资源身份的 guarded operation | Nereus Route、Schedule/Cancel/Reschedule、Nereus receipt 语义 |
| Broker | append 前验证实际资源 incarnation，返回可关联的结果 | Nereus Command application |
| Worker | Shard Log apply、RocksDB、调度、Lane、publish outcome、checkpoint/recovery | 入口身份认证和客户端幂等键 |

以下方案不采用：

- 不把完整 Nereus Delay SDK 塞入 `KafkaProducer` 或 `PulsarClient`；
- 不强制所有 Java 流量经过 Gateway；
- 不允许 stock/name-only Producer 作为 guarded path 的降级；
- 不在 SDK、Gateway 和 Worker 各实现一份路由/Hash/Command 编码；
- 不把 Gateway idempotency success 冒充 Command queued/applied；
- 不把循环任务字段加入 V1。循环任务需要新的 Registry/状态机版本，属于后续 capability。

## 2. 源码基线与现有资产

设计核对的只读基线：

| 仓库 | 基线 | 用途 |
| --- | --- | --- |
| `nereus-delay` | `origin/main@2dfc3289ffdbe9cf9d7f4d0de1d701493d1b49a6` | 当前 V1 protocol/client/adapter/worker 代码；本地 D1/D5 implementation branch 在该基线之上 |
| Kafka | `trunk@c300006a7705c240642db6950b5a95fec982bfc5` | Produce v13、ProducerBatch、RecordAccumulator、Sender |
| Pulsar | `5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551` | Producer API、PulsarApi、ServerCnx、Producer、ManagedLedger properties |
| DDMQ Chronos | 主设计引用的 `2f30b61a5741d55a5b515f3d8d19a8a35be8c9e2` | PProxy/inner topic/RocksDB timeline 对照，不作为代码依赖 |

现有代码不是原型空壳，下面的类型直接复用：

| 现有类型 | 目标位置/用途 | 处理 |
| --- | --- | --- |
| `PreparedCommand`、`CommandCodec`、`CommandBodies` | Semantic Core 的 canonical preparation | 保留 wire；补一个从预生成 UUIDv7 构造 self-routing ID 的入口 |
| `PreparedSubmissionV1` | MANAGED/AUTO_FAST 冻结分支 | 原样复用，I/O 后禁止重选 |
| `AutoFastSchedule.NativeCandidate` | 当前 embedded/conformance AUTO_FAST wrapper | 移到 testkit/conformance；production API 不接受 caller-supplied `PublicKey`/candidate，改由本地已验签 provider 构造 |
| `PinnedKafkaCommandIngress` | Kafka result -> `EnqueueOutcomeMessageV1` | 保留投影；替换 nested test SPI 为生产 transport 实现 |
| `PinnedPulsarCommandIngress` | Pulsar result -> `EnqueueOutcomeMessageV1` | 保留投影；接入 guarded receipt/error |
| `PreparedSubmissionAdapter` | exact branch dispatch | 移入 client-core composition；不得吸收 Route 选择 |
| `KafkaIngressResource` / `PulsarIngressResource` | exact ingress identity | 作为 RouteSnapshot ingress union 的 Java 投影 |
| `KafkaProduceRequest` / `PulsarSendRequest` | transport request | 保留 exact frame/identity；从 adapter nested interface 提升为 top-level SPI |
| `KafkaProduceResult` / `PulsarSendResult` | transport result | 保留 closed disposition；把单一 arbitrary `evidence` 拆成 exact request/response evidence，缺任一必需证据即降为 UNKNOWN |
| `DelayClient` | 当前公共 facade | 收敛为严格 V1 API；legacy/embedded overload 留在 conformance artifact |
| `EmbeddedDelayService` | in-process conformance | 不再作为生产 Direct SDK；迁入 `delay-testkit`/`delay-conformance` |
| `DelayShard`、`OwnedDelayShard`、Lane/Store/Scheduler | Worker | 本方案不复制、不重写 |

当前三个未提交的 Lane/Profile 校验文件不属于本设计改动，实施时必须保留其工作树内容并在模块迁移中以当前文件为准。

## 3. Gradle 模块与依赖方向

### 3.1 目标模块

```text
:delay-api
  io.nereusstream.delay.protocol.*
  io.nereusstream.delay.client.api.*
  （包括 SubmissionModeV1、authenticated intent/error value API）

:delay-semantic-core
  io.nereusstream.delay.semantic.*
  只依赖 :delay-api、:delay-route-spi

:delay-route-spi
  RouteSnapshotV1 / RouteSnapshotProvider / RouteSnapshotCacheView

:delay-route-oxia
  Oxia watch、snapshot verification、cache publication

:delay-transport-spi
  CommandTransport / CommandTransportRegistry / transport request-result union

:delay-client-core
  DefaultDelayClient / PreparedSubmissionAdapter / local backpressure / outbox SPI

:delay-client-kafka
  ProductionKafkaProduceTransport / Kafka client factory

:delay-client-pulsar
  ProductionPulsarSendTransport / Pulsar client factory / native transport

:delay-gateway-api
  gRPC proto、HTTP transcoding DTO、multi-language generated interfaces

:delay-gateway
  auth、idempotency、quota、audit、service composition

:delay-worker-core
  current runtime + ownership + scheduler

:delay-ingress-kafka / :delay-ingress-pulsar
  source consumer、source guard、ACK-after-sync

:delay-adapter-kafka / :delay-adapter-pulsar
  destination publish/evidence adapters

:delay-store-rocksdb / :delay-metadata-oxia / :delay-object-store

:delay-testkit / :delay-conformance / :delay-distribution
```

### 3.2 强制依赖规则

```text
delay-api                   -> JDK only
delay-route-spi             -> delay-api
delay-transport-spi         -> delay-api
delay-semantic-core         -> delay-api + route-spi
delay-client-core           -> semantic-core + route-spi + transport-spi
delay-client-kafka          -> delay-api + transport-spi + Kafka Client
delay-client-pulsar         -> delay-api + transport-spi + Pulsar Client
delay-gateway               -> client-core + route-oxia + selected transports + gRPC
delay-worker-core           -> delay-api + store/ownership SPIs
```

禁止的边：

```text
Kafka/Pulsar Client -> any Nereus Delay artifact
delay-api           -> RocksDB/Oxia/Kafka/Pulsar/gRPC
semantic-core       -> RocksDB/Worker/Gateway
semantic-core       -> Kafka/Pulsar/gRPC/transport-spi
Gateway             -> private Worker DB classes
Worker              -> Gateway idempotency/auth DTO
```

### 3.3 非 Big-Bang 迁移

物理拆模块分四次完成，避免一次移动数百个类：

1. 先在单 module 中建立 `semantic`、`route`、`transport` package 和依赖测试；
2. 把 `protocol` + 精简 client API 抽到 `delay-api`，原坐标暂时发布 forwarding artifact；
3. 抽 client/transport/gateway；
4. 最后移动 Worker/store/adapter，并删除 forwarding artifact。

每一步运行 ArchUnit 或 Gradle package-dependency gate；禁止为了过编译把 RocksDB/Oxia 重新暴露为 `api`。

## 4. RouteSnapshot：唯一入口路由事实

### 4.1 Java 模型

```java
public record RouteSnapshotV1(
        int version,
        RouteIncarnation routeIncarnation,
        Digest32 authenticatedTenantScopeHash,
        Bytes32 tenantRoutingScope,
        RouteLifecycleV1 lifecycle,
        long newScheduleAcceptUntilEpochMs,
        IngressRouteResourceV1 ingress,
        RoutingHashVersionV1 routingHashVersion,
        ProtocolTupleV1 protocolTuple,
        long controlVersion,
        List<RoutePartitionPolicyV1> partitions,
        long queuedReceiptQueryWindowMs,
        long fullCommandResultRetentionMs,
        long maxInlinePayloadBytes,
        long maxCommandBytes,
        long maxBatchCommands,
        long maxBatchBytes,
        long maximumPreparationAgeMs,
        long validFromEpochMs,
        long validUntilEpochMs,
        IngressCredentialBindingRefV1 credentialBinding,
        Digest32 routePrerequisiteDigest,
        TrustedUtcIntervalEvidenceV1 issuedAt,
        int signingKeyVersion,
        Digest32 snapshotDigest,
        RouteSnapshotSignature signature) {
}

public sealed interface IngressRouteResourceV1
        permits KafkaIngressRouteResourceV1, PulsarIngressRouteResourceV1 {
    String authenticatedClusterId();
    int partitionCount();
}

public record KafkaIngressRouteResourceV1(
        String authenticatedClusterId,
        String canonicalPhysicalTopic,
        UUID nativeTopicUuid,
        int partitionCount) implements IngressRouteResourceV1 {
}

public record PulsarIngressRouteResourceV1(
        String authenticatedClusterId,
        String physicalTopicBase,
        List<PulsarPhysicalPartitionIdentityV1> partitions)
        implements IngressRouteResourceV1 {
    @Override public int partitionCount() { return partitions.size(); }
}

public record RoutePartitionPolicyV1(
        int partition,
        ActivationBarrierV1 activationBarrier,
        QuotaGrantRefV1 quotaGrant,
        long brokerGuardAttestationGeneration,
        Digest32 brokerGuardAttestationDigest) {
}
```

`PulsarPhysicalPartitionIdentityV1` 包含 physical topic、`resourceIncarnation[32]`、service-owned creation timestamp 和 partition。`RouteSnapshotV1` 不包含 plaintext credential、mutable secret reference 或 caller-supplied tenant ID。
以上字段与 signature/digest 的 exact canonical schema 已登记在 Registry §6.6；Java codec 只能
投影该 schema，不能用 Java serialization/JSON/map iteration 生成签名 bytes。snapshot 直接保存
Registry fields 12–13 的两个 duration scalar；现有 adapter 层
`QueuedReceiptQueryPolicy`/`CommandResultRetentionPolicy` 在边界处从
`(snapshot.controlVersion(), duration)` 构造，`delay-route-spi` 不反向依赖 adapter package。
record 字段按 Registry fields 1–26 的顺序排列；constructor 固定校验 `version == 1`，而不是
依赖类型名隐式补出 field 1。所有 byte array/value wrapper 在构造时 defensive copy，并按内容
实现 equality/hash，禁止 record 默认把 mutable array identity 当成签名字段语义。

### 4.2 本地快照接口

`prepare*` 的“零 I/O”契约保持不变。Oxia watch 在后台刷新，prepare 只读本地 immutable view：

```java
public interface RouteSnapshotProvider {
    RouteSnapshotV1 activeForNewSchedule(AuthenticatedTenantContext context,
                                         RouteSelectionHint hint);

    RouteSnapshotV1 exact(RouteIncarnation incarnation,
                          AuthenticatedTenantContext context);

    long publishedRevision();
}

public interface RouteSnapshotRefresher extends AutoCloseable {
    CompletionStage<Void> start();
    RouteCacheHealth health();
}

public interface NativePreparationSnapshotProvider {
    Optional<NativePreparationSnapshotV1> eligibleFor(
            AuthenticatedTenantContext context,
            RouteSnapshotV1 managedRoute,
            ScheduleIntentV1 intent,
            TrustedTimeSnapshot trustedTime);
}
```

`NativePreparationSnapshotV1` 是 immutable、已验签的本地投影，包含 exact Destination/
Capability Profile envelope、`NativeCapabilitySnapshotV1`、Pulsar target/partition 和 issuer
trust-key version；不含 plaintext secret。`eligibleFor` 不做网络/Oxia/Admin I/O，不能把 caller
提交的 target/token 重新包装成“可信 snapshot”。

约束：

- cache miss、过期签名、watch gap、lifecycle 不匹配均在 I/O 前返回 `ROUTE_SNAPSHOT_UNAVAILABLE`；
- `activeForNewSchedule` 只返回 `ACTIVE_FOR_NEW`；
- Cancel/Reschedule/Query 先从 `delayMessageId` 解出 Route Incarnation/partition，再调用 `exact`；
- `PreparedCommand` 创建后不得因 watch 更新而更换 semantic Route/resource/partition/policy 或
  已冻结 branch；提交时可使用同一 Route 的更新 lifecycle/control snapshot、leader、更新的
  Quota Grant、完整且语义等价的 guard-rollout attestation，以及经证明等价的新 Credential
  Binding/channel，但 Activation Barrier、Broker resource incarnation、Route prerequisite digest
  和其它 immutable semantic field 必须逐字相同；
- 一个 Route Incarnation 的 `partitionCount`、hash version、tenant routing scope、receipt/
  retention policy、size/preparation limits、Route prerequisite digest、Activation Barrier 和 ingress
  incarnation 永不原地变化；扩容或 semantic prerequisite 变化创建新 Route Incarnation。

当前代码实现了上述 native 本地判定的可复用缓存：
`VerifiedNativePreparationSnapshotCache` 在安装时 canonical-decode
Destination/Capability/Profile 与 `NativeCapabilitySnapshotV1`，验证 issuer
Ed25519 signature，然后只暴露 immutable local views。共享
`NativePreparationEligibilityV1` 在 Semantic Core 中再次绑定 authenticated
principal scope、Pulsar AUTO_FAST capability、Destination target/partition
policy、`DELAY_MESSAGE_ID` hash input、broker clock bound、validity window 和
target-record size；任何失败都返回已经冻结的 managed frame。Direct SDK 的
`prepareScheduleSubmissionV1(..., SubmissionModeV1)` 与 Gateway 的
`SubmissionModeV1` 请求共用这个入口。这里的 issuer/catalog/Oxia refresh 仍是
外部 authority，cache 不是 native capability 的签发者，也不执行 Broker probe。

Commit `3bae4a6b` adds the issuance-side `NativeCapabilitySnapshotIssuer`.
It verifies the signed Route and exact catalog Profile/Binding/Head, checks
the credential-equivalence attestation, obtains principal-scoped Broker guard
evidence, bounds expiry by the trusted interval and every prerequisite, and
requires the external authority to persist native credential protection before
returning the signed snapshot. The injected authority is still the boundary
for production Oxia protection, live guard reads, credential resolution and
issuer key rotation; the local issuer test is not a live authority receipt.

### 4.3 路由与 ID 构造顺序

无序 Schedule 需要先生成逻辑 UUIDv7，再计算 partition，不能先随机选择 Shard：

```text
1. read exact immutable RouteSnapshot
2. create DelayLogicalIdSeed(UUIDv7)
3. routingKey = orderingKey or seed UUID bytes
4. partition = ROUTING_HASH_V1(route, tenantRoutingScope, routingKey)
5. shardId = routeIncarnation + partition
6. delayMessageId = SelfRoutingId(shardId, same UUIDv7 seed)
7. commandId = new SelfRoutingId(same shardId, independent UUIDv7)
8. canonical body -> commandHash -> exact NDL1 frame
```

因此 `SelfRoutingId` 增加受校验的 factory，而不改变 41-byte wire：

```java
public static SelfRoutingId fromLogicalUuid(ShardId shardId, UUID logicalUuidV7);

public interface LogicalUuidV7Generator {
    UUID next(TrustedClock clock);
}
```

factory 必须复用 decode 的 version/variant 校验，并用 route policy 允许的 Trusted Clock 生成
48-bit timestamp；random bits 来自进程级 `SecureRandom`/DRBG。生产 Semantic Core 注入
`LogicalUuidV7Generator`，测试注入固定 sequence，不能继续让不可替换的 `Instant.now()` 隐藏在
路由算法里。现有 `PreparedCommand.scheduleV1(ShardId, ...)` 保留为低层/测试入口；生产
Semantic Core 不允许调用方自己传 Shard。

## 5. Semantic Core

### 5.1 API

```java
public interface DelaySemanticCore {
    PreparedSubmissionV1 prepareSchedule(AuthenticatedTenantContext tenant,
                                         RouteSelectionHint route,
                                         ScheduleIntentV1 intent,
                                         long retryUntilEpochMs,
                                         SubmissionModeV1 submissionMode);

    PreparedCommand prepareLargeSchedule(AuthenticatedTenantContext tenant,
                                         RouteSelectionHint route,
                                         LargeSchedulePreparationV1 request);

    PreparedCommand preparePayloadCommit(AuthenticatedTenantContext tenant,
                                         PayloadReservationReceiptV1 reservation,
                                         PayloadCommitProofV1 proof,
                                         long retryUntilEpochMs);

    PreparedCommand prepareCancel(AuthenticatedTenantContext tenant,
                                  DelayMessageId id,
                                  MessagePreconditionV1 precondition,
                                  long retryUntilEpochMs);

    PreparedCommand prepareReschedule(AuthenticatedTenantContext tenant,
                                      DelayMessageId id,
                                      MessagePreconditionV1 precondition,
                                      long deliverAtEpochMs,
                                      long expireAtEpochMs,
                                      long retryUntilEpochMs);

    PreparedSubmissionV1 prepareManaged(AuthenticatedTenantContext tenant,
                                        PreparedCommand command);
}
```

`AuthenticatedTenantContext` 是调用环境提供的 trusted object：Direct SDK 从独占 Route/credential binding 构造；Gateway 从 mTLS/JWT/service account 构造。它绝不进入 NDL1 Command body。Worker 仍从 Route + Broker ACL 推导 tenant。

`prepareSchedule` 保留 ADR 0031 的两阶段契约：总是先生成 managed Command，再在任何 I/O 前
按 `SubmissionModeV1` 冻结 branch。Direct SDK 与 Gateway 都只能传 `MANAGED/AUTO_FAST` enum；
`AUTO_FAST` candidate 只能来自 Semantic Core 构造时注入的、只读本地已验签
`NativePreparationSnapshotProvider`。production API 不接受 caller-supplied target/token、
`PublicKey` 或 `AutoFastSchedule.NativeCandidate`，避免调用方自行选择“信任根”。Gateway 与
Direct 因而调用逐字相同的方法；不满足 native 条件时返回同一个 managed frame。

`NativePreparationSnapshotProvider` 保留四参数 functional seam，并提供带已冻结
`PreparedCommand` 的 default overload；后者只用于需要 Delay Message ID 作为目标分区
hash input 的 provider，不改变 prepare 的零 I/O 边界。

### 5.2 单一 preparation pipeline

```java
RouteSnapshotV1 snapshot = routeProvider.activeForNewSchedule(tenant, hint);
snapshotVerifier.requireUsable(snapshot, trustedTime);
PreparedIdentity identity = identityPlanner.newSchedule(snapshot, intent);
PreparedCommand command = commandFactory.scheduleV1(identity, intent, retryUntil);
preparedValidator.requireExactV1(command, snapshot);
Optional<NativePreparationSnapshotV1> candidate = submissionMode == SubmissionModeV1.AUTO_FAST
        ? nativeSnapshotProvider.eligibleFor(tenant, snapshot, intent, trustedTime)
        : Optional.empty();
return submissionPlanner.freezeSchedule(
        tenant, snapshot, command, intent, submissionMode, candidate, trustedTime);
```

Direct SDK 和 Gateway 只能调用这一 pipeline。Gateway 不得重写以下逻辑：

- `ROUTING_HASH_V1`；
- UUIDv7/self-routing 编码；
- `CommandBodies.*V1`；
- `CommandHash`；
- payload inline/object union；
- `AUTO_FAST` eligibility 和 branch freeze；
- stable error/retryability；
- receipt/outcome union。

### 5.3 Cancel/Reschedule

```text
decode delayMessageId
  -> validate UUIDv7 + CRC
  -> obtain original routeIncarnation + partition
  -> authorize exact historical Route for tenant
  -> create new commandId in the same Shard
  -> encode CancelV1/RescheduleV1
```

禁止基于当前 active Route 重新 hash。Route 已进入 `CONTROL_ONLY`/`DRAINING` 时仍允许已有 ID 的控制命令；`RETIRED` 只有在 retry/result/identity obligations 已全部结束后才可从 cache 移除。

## 6. Direct Java SDK

### 6.1 生产 facade

```java
public final class DefaultDelayClient implements DelayClient {
    private final DelaySemanticCore semanticCore;
    private final SubmissionCoordinator submissions;
    private final QueryClient queryClient;
    private final ClientAdmission admission;
    private final ClientOutbox outbox;
}

public interface SubmissionCoordinator {
    CompletionStage<SubmissionOutcomeMessageV1> submit(
            AuthenticatedTenantContext tenant,
            PreparedSubmissionV1 submission,
            TransportOwnershipPermit ownershipPermit);
}
```

`DefaultDelayClient` 与 Gateway 共用 `SubmissionCoordinator` 的实现类和状态机，不要求共用一个
multi-tenant object instance；前者传 builder 已绑定的 trusted tenant context，后者传当前请求由
authentication interceptor 构造的 context。该 coordinator 在
`delay-client-core` 中只负责 preparation 之后的 transport registry、attempt ownership 和
outcome projection。Direct client/Gateway 各自的 entry composition 先调用同一个 Semantic Core，
再把其 exact `PreparedSubmissionV1` 交给 coordinator。Semantic Core 本身不引用
`SubmissionCoordinator` 或 `CommandTransportRegistry`。

`TransportOwnershipPermit` 是 process-local、不可序列化的一次性 capability，内部绑定
`PhysicalEnqueueAttemptId`。Direct SDK 为每次本地物理调用创建 `LocalTransportOwnershipPermit`；
Gateway 只能使用 idempotency-record attempt CAS 的确定成功响应派生
`GatewayAttemptOwnershipPermit`。Coordinator 必须把 permit 原对象传到 transport，不能只取出
attempt ID 后丢掉 capability。

生产 constructor 使用 builder，所有安全相关项显式提供：

```java
NereusDelayClient.builder()
    .tenantContext(authenticatedTenantContext)
    .routeSnapshotProvider(routeCache)
    .transportRegistry(registry)
    .queryClient(queryClient)
    .admission(clientAdmission)
    .outbox(optionalDurableOutbox)
    .trustedClock(trustedClock)
    .build();
```

不提供会静默创建 stock Kafka/Pulsar Producer 的默认 constructor。

### 6.2 submit 流程

```text
PreparedSubmissionV1
  -> strict decode/branch check
  -> acquire client count+byte permit
  -> allocate nonzero PhysicalEnqueueAttemptId + LocalTransportOwnershipPermit
  -> optional durable outbox Start
  -> registry.lookup(exact frozen route resource)
  -> guarded transport ownership
  -> map to SubmissionOutcomeMessageV1
  -> durable outbox Final
  -> release permit only at certified completion boundary
```

The local `bcf2f0a8` composition treats a failure while writing the optional
outbox Final as an unobservable completion boundary: it retains the exact
prepared branch and physical attempt and projects `ENQUEUE_UNCERTAIN`. An
outbox exception must not become a caller-visible exceptional Future that
could be mistaken for a definitive non-persistence result. This remains local
evidence; the outbox implementation and its restart durability are external
deployment responsibilities.

Future cancel、SDK timeout 或 callback registration failure不证明未入 Broker，统一保留原 prepared identity 并返回/记录 uncertain。相同逻辑重试复用 Prepared bytes，新的物理调用使用新的 `PhysicalEnqueueAttemptId`；一次已发起调用的 callback 重挂不创建第二个 attempt。

### 6.3 API 清理

`DelayClient` 的生产 V1 artifact 只暴露 Registry-shaped 方法。以下内容移到 conformance/legacy artifact：

- legacy `ScheduleIntent`；
- `runtime.CommandResult` 返回值；
- caller-supplied absolute receipt/query boundary；
- caller-supplied `AutoFastSchedule.NativeCandidate`/issuer `PublicKey`；
- `EmbeddedDelayService` drain/本地 offset 行为。

这不是 wire 迁移：现有 NDL1/NDR1 bytes 不变；只是阻止 production artifact 暴露可绕过 Route policy 的兼容入口。

## 7. Delay Gateway

### 7.1 服务分层

```text
GatewayServer
  -> AuthenticationInterceptor
  -> RequestLimitInterceptor
  -> GatewaySubmissionCoordinator
       -> GatewayIdempotencyService
       -> DelaySemanticCore
       -> SubmissionCoordinator
            -> CommandTransportRegistry
       -> GatewayOutcomeMapper
```

建议代码位置：

```text
delay-gateway-api/src/main/proto/nereus/delay/gateway/v1/delay_gateway.proto
delay-gateway/src/main/java/io/nereusstream/delay/gateway/DelayGatewayService.java
delay-gateway/src/main/java/io/nereusstream/delay/gateway/GatewayIdempotencyService.java
delay-gateway/src/main/java/io/nereusstream/delay/gateway/GatewayAppliedObservationService.java
delay-gateway/src/main/java/io/nereusstream/delay/gateway/GatewayAuthenticationInterceptor.java
delay-gateway/src/main/java/io/nereusstream/delay/gateway/GatewayQuotaAdmission.java
delay-gateway/src/main/java/io/nereusstream/delay/gateway/GatewayAuditSink.java
```

### 7.2 RPC surface

```proto
service DelayGatewayV1 {
  rpc Schedule(GatewayScheduleRequestV1) returns (GatewaySubmissionOutcomeV1);
  rpc PrepareLargeSchedule(GatewayPrepareLargeScheduleRequestV1)
      returns (GatewaySubmissionOutcomeV1);
  rpc IssuePayloadUploadHandle(GatewayIssuePayloadUploadHandleRequestV1)
      returns (GatewayPayloadUploadHandleResponseV1);
  rpc AttestPayloadUpload(GatewayAttestPayloadUploadRequestV1)
      returns (GatewayPayloadAttestationResponseV1);
  rpc CommitLargeSchedule(GatewayCommitLargeScheduleRequestV1)
      returns (GatewaySubmissionOutcomeV1);
  rpc Cancel(GatewayCancelRequestV1) returns (GatewaySubmissionOutcomeV1);
  rpc Reschedule(GatewayRescheduleRequestV1) returns (GatewaySubmissionOutcomeV1);
  rpc RetryUncertain(GatewayRetryUncertainRequestV1) returns (GatewaySubmissionOutcomeV1);
  rpc GetCommandResult(GatewayGetCommandResultRequestV1)
      returns (GatewayCommandQueryResponseV1);
  rpc AwaitApplied(GatewayAwaitAppliedRequestV1)
      returns (stream GatewayCommandQueryResponseV1);
  rpc GetMessage(GatewayGetMessageRequestV1) returns (GatewayMessageQueryResponseV1);
}

message GatewayRouteSelectorV1 {
  uint32 ingress_adapter_kind = 1;       // AdapterKindV1
  bytes route_alias_utf8_nfc = 2;        // 1..128 bytes, tenant-scoped alias
}

message GatewayScheduleRequestV1 {
  bytes idempotency_key = 1;             // 16..64 bytes
  GatewayRouteSelectorV1 route = 2;
  bytes schedule_intent_v1 = 3;          // exact canonical ScheduleIntentV1
  int64 retry_until_epoch_ms = 4;
  uint32 submission_mode_v1 = 5;         // exact SubmissionModeV1
}

message GatewayPrepareLargeScheduleRequestV1 {
  bytes idempotency_key = 1;
  GatewayRouteSelectorV1 route = 2;
  bytes schedule_intent_v1 = 3;          // forPrepare form; no payload branch
  uint64 expected_payload_length = 4;
  bytes payload_sha256 = 5;              // exact 32 bytes
  uint64 reservation_ttl_ms = 6;
  bytes payload_proof_trust_set_ref_v1 = 7;
  bytes object_store_profile_ref_v1 = 8;
  int64 retry_until_epoch_ms = 9;
}

message GatewayCommitLargeScheduleRequestV1 {
  bytes idempotency_key = 1;
  bytes payload_reservation_receipt_v1 = 2;
  bytes payload_commit_proof_v1 = 3;
  int64 retry_until_epoch_ms = 4;
}

message GatewayCancelRequestV1 {
  bytes idempotency_key = 1;
  bytes delay_message_id = 2;             // exact 41 bytes
  bytes message_precondition_v1 = 3;
  int64 retry_until_epoch_ms = 4;
}

message GatewayRescheduleRequestV1 {
  bytes idempotency_key = 1;
  bytes delay_message_id = 2;
  bytes message_precondition_v1 = 3;
  int64 deliver_at_epoch_ms = 4;
  int64 expire_at_epoch_ms = 5;
  int64 retry_until_epoch_ms = 6;
}

message GatewayRetryUncertainRequestV1 {
  bytes original_idempotency_key = 1;
  bytes expected_prior_physical_attempt_id = 2; // exact 16 bytes
  bytes retry_request_id = 3;                    // exact 16 bytes
}

message GatewayIssuePayloadUploadHandleRequestV1 {
  bytes payload_reservation_receipt_v1 = 1;
  uint32 upload_handle_kind = 2;          // UploadHandleKindV1
}

message GatewayAttestPayloadUploadRequestV1 {
  bytes payload_reservation_receipt_v1 = 1;
  bytes opaque_payload_upload_handle_v1 = 2;
}

message GatewayGetCommandResultRequestV1 {
  oneof locator {
    bytes command_queued_receipt_v1 = 1;
    bytes command_id = 2;                 // exact self-routing CommandId[41]
  }
}

message GatewayAwaitAppliedRequestV1 {
  bytes command_queued_receipt_v1 = 1;
}

message GatewayGetMessageRequestV1 {
  oneof locator {
    bytes delay_message_id = 1;           // exact 41 bytes
    bytes command_queued_receipt_v1 = 2;  // receipt with MessageSubject
  }
}

message GatewaySubmissionOutcomeV1 {
  oneof result {
    bytes submission_outcome_ndr1 = 1;
    bytes preparation_error_v1 = 2;      // exact StableErrorV1, no prepared ref
  }
}

message GatewayPayloadUploadHandleResponseV1 {
  bytes payload_upload_handle_response_v1 = 1;
}

message GatewayPayloadAttestationResponseV1 {
  bytes payload_attestation_response_v1 = 1;
}

message GatewayCommandQueryResponseV1 {
  bytes command_query_response_v1 = 1;
}

message GatewayMessageQueryResponseV1 {
  bytes message_query_response_v1 = 1;
}
```

Payload handle/attestation 与 query RPC 也只封装对应 Registry canonical bytes：request
携带 exact receipt/ID，response field 1 携带 exact canonical response。`AwaitApplied` 的等待
上限取 gRPC deadline 与服务端 policy 的较小值，不接受 caller-supplied absolute Broker/source
time。Gateway proto 是 transport DTO，不重新定义 NDL1/NDR1；服务端不得把 domain result
压成 boolean。上述字段号、长度、presence 和 request-hash 规则登记在 Protocol Registry
§6.5，并生成跨语言 golden vectors。
Gateway 的 `AUTO_FAST` 只接受 preference；native candidate、签名快照和 credential authority
由服务端已认证 catalog 构造，请求不能注入 target/token/capability snapshot。

Canonical/authorization/Route/quota，以及当前 canonical request 的 idempotency-key conflict，
在该 request 取得任何 prepared submission/attempt 前返回 response field 2：exact
`StableErrorV1(stage=PREPARATION)`，且不得伪造或泄露已有 key 所绑定的 prepared/native ref。
该 request 一旦取得 prepared identity，所有 enqueue 结论只能走 field 1 NDR1。gRPC
`INVALID_ARGUMENT/UNAUTHENTICATED/PERMISSION_DENIED/UNAVAILABLE/DEADLINE_EXCEEDED` 仅用于无法
可靠返回 canonical domain bytes 的 transport/auth framing 状态；一旦 `STARTED` durable，caller
deadline 不取消 Producer attempt，也不能把 gRPC deadline 宣称为 definite。客户端以同一
idempotency key 重读 eventual record。

2026-08-14 implementation evidence: commits
`9695eba7ca384d99cd28ece238f6cbfe1bcd08be`,
`724fdad95971dd096e116056f8e5da1a7ba76d14` and
`44bffea6063ef68ce36f8fb49527ee00a9bfa36b` add generated gRPC handling for
`PrepareLargeSchedule`, `CommitLargeSchedule`, `Cancel` and `Reschedule`. The handlers decode the exact self-routing
`DelayMessageId` and canonical `MessagePreconditionV1`, while the shared
Gateway ingress performs tenant authentication, control admission and
digest-only audit. The domain path prepares the control Command before the
same idempotency CAS, one-shot physical-attempt ownership and NDR1 outcome
projection used by Schedule. This is local conformance evidence; the other
RPC handlers and receipt-bound upload handlers require an explicitly injected
`GatewayPayloadAuthority`; deployable mTLS/JWT authority, distributed quota/audit and real
transport/Worker integration remain open.

2026-08-15 implementation evidence: commit
`59d492041ac42b79a632ebddfb56a7608b2d7283` adds the generated
`GetCommandResult`, bounded `AwaitApplied` and `GetMessage` handlers. Their
transport-neutral request records select only the frozen receipt/ID locators;
`GatewayQueryIngressService` authenticates the tenant, applies control
admission, records digest-only audit events and delegates receipt binding,
source/store reads and deadline policy to an explicitly injected
`GatewayQueryAuthority`. `AwaitApplied` responses are bounded before canonical
stream encoding. This is local query composition evidence; without that
authority composition the generated handlers remain `UNIMPLEMENTED`, and
production query routing, retention/source authority, deployable auth and
Worker integration remain open.

Commit `39744ac70cae21a3ad4a5401da33805d9221dec7` adds the Oxia composition
for the digest-only audit sink in
`OxiaGatewayAuditSink`. It stores each canonical `GatewayAuditEventV1` under
an event-content-derived immutable key, accepts exact duplicate records, and
requires an exact key/version/value reread after a lost write response. The
sink is bounded and never persists request or prepared-submission bytes. This
is durable audit storage evidence only; the deployable mTLS/JWT authority,
distributed quota/control reserve, HA idempotency and late-evidence paths
remain separate gates.

Commit `8e0ed49b706dda2a6cb0d7d011c72d2a9270157b` adds
`OxiaRealGatewayAuditSinkSmokeTest`. With Oxia source
`37a17bef17202d5fd6e23282da5fd26d94865484` running in standalone mode on
`127.0.0.1:16648`, the real Owner/Control/Recovery smoke classes and this
Gateway audit smoke passed on 2026-08-15. The live Gateway check wrote an
identical digest-only event twice and read back one exact canonical record.
This is a single-record Oxia service check; Route activation/session fencing,
cross-record transactions, deployed authentication, real Broker transports
and Worker vertical integration remain open.

Commit `de1da743` adds the durable admission implementation
`OxiaGatewayAdmissionController`. It keeps one canonical, digest-checked
lease record per authenticated tenant scope, uses independent schedule,
retry-uncertain and control pools, charges schedule bytes atomically, reclaims
expired leases and releases by exact lease identity. Bounded CAS and exact
response-loss rereads are covered by `OxiaGatewayAdmissionControllerTest`.
The implementation closes the distributed single-record admission contract.
Commit `b6154072` adds `OxiaRealGatewayAdmissionSmokeTest` to the isolated
Oxia Docker harness; its 2026-08-15 run passed the selected real-service
tests and reread the empty canonical admission record after expiry/release.
This is live single-node evidence, not HA/session-churn observability,
rate-window/load evidence, cross-record Gateway transactions or production
wiring.

### 7.3 认证与 tenant

- mTLS principal、JWT subject 或 service account 映射为 `AuthenticatedTenantContext`；
- 请求体不接受 `tenantId`；即使兼容 HTTP header 带 tenant hint，也只能作为和认证上下文逐字匹配的提示；
- Route 授权在解析 self-routing ID 并暴露 route/partition existence 之前完成；
- unknown route 与 cross-tenant 使用 non-enumerating `NOT_FOUND_OR_NOT_AUTHORIZED`。

### 7.4 幂等记录

每个会准备 Command 的首次写 RPC（Schedule/Prepare Large/Commit/Cancel/Reschedule）必须携带
16–64 bytes `idempotencyKey`；`RetryUncertain` 引用原 key 并携独立 retry request ID。Gateway
先计算：

```text
keyHash = SHA-256(
  "nereus-delay-gateway-idempotency-key-v1\0" ||
  authenticatedTenantScopeHash[32] || lp32(idempotencyKey)
)

bodyHash = SHA-256(
  "nereus-delay-gateway-request-v1\0" ||
  u16be(GatewayOperationKindV1) ||
  lp32(canonicalProtobuf(request fields 2..N with original field numbers))
)

retryRequestHash = SHA-256(
  "nereus-delay-gateway-retry-request-v1\0" ||
  keyHash[32] || expectedPriorPhysicalAttemptId[16] || retryRequestId[16]
)
```

Oxia record：

```text
/nereus-delay/v1/gateway-idempotency/
  <first-two-gatewayKeyHash-bytes-as-lower-hex>/
  <gatewayKeyHash-as-unpadded-base64url>
```

路径不含 tenant 名、raw idempotency key 或 RPC body。value field 2 必须等于路径解出的完整
hash；不相等是 integrity failure。

```java
public record GatewayIdempotencyRecordV1(
        int version,
        Digest32 gatewayKeyHash,
        GatewayOperationKindV1 operation,
        Digest32 requestBodyHash,
        byte[] preparedSubmissionBytes,
        Digest32 preparedSubmissionHash,
        GatewayIdempotencyPhaseV1 phase,
        List<GatewayPhysicalAttemptV1> attempts,
        byte[] aggregateOutcomeBytes,
        long createdAtEpochMs,
        long retainUntilEpochMs,
        long revision,
        Digest32 recordDigest) {
}

public record GatewayPhysicalAttemptV1(
        int attemptNo,
        PhysicalEnqueueAttemptId physicalAttemptId,
        GatewayPhysicalAttemptStateV1 state,
        byte[] outcomeBytes,
        long startedAtEpochMs,
        long uncertaintyAtEpochMs,
        Bytes16 retryRequestId,
        Digest32 retryRequestHash,
        long revision,
        long ownershipNotAfterEpochMs) {
}
```

`outcomeBytes`、`retryRequestId/hash`、`aggregateOutcomeBytes` 按 Registry presence rule 可 absent；
Java 实现用显式 nullable/private constructor 或 sealed presence type，不能用空 bytes 混淆 absent。
所有 `byte[]` constructor/accessor defensive-copy，record digest 由 codec 重算而非信任 caller。

Oxia create 使用 `expectedVersion=NOT_EXISTS` 和 record `revision=1`；每次更新同时 compare
上次读取的 Oxia version、record revision/digest，并 checked-increment record revision。CAS
response ambiguous 时只 reread exact key并按 attempt ID/retryRequestId/outcome digest判断该转换
是否已安装，禁止用新 version 盲重放 transition。相同 attempt 的不同 terminal evidence 是
integrity conflict，保留原 value并告警，不能 last-write-wins。

phase 与 outcome 分离：

```text
PREPARED -> ACTIVE -> QUIESCENT
               ^         |
               +---------+  explicit RetryUncertain only
```

`PREPARED` 只表示 exact prepared bytes 已 durable、尚无 attempt；首次提交同样需要 CAS 到
`ACTIVE` 并取得 permit。`PREPARED` 不能因普通重复 RPC 被长期遗留：原 proposer、同-key caller
或 sweeper 都可竞争这次“首个 attempt”CAS；只有一个 CAS winner 发送。若 `PREPARED` 已超过
prepared retry/expiry fence，则不创建 attempt，保持 record phase=`PREPARED`、attempt/outcome
仍 absent，返回从已存 prepared bytes 与可信时间确定性派生的 Registry-shaped typed failure，并按
retention 保留或在无并发 reader/lease 后 GC。这个失败不是 stored aggregate outcome；恢复规则不改变
prepared identity，也不重跑 Semantic Core。响应走 field 1 NDR1：managed 使用同一 prepared ref、
`PREPARED_COMMAND_EXPIRED` 和 local-before-ownership proof；native 使用同一 native prepared ref、
`NATIVE_PREPARED_SUBMISSION_EXPIRED`。它不能降格成 field 2 preparation error。

`GatewayPhysicalAttemptV1` 至少保存递增 `attemptNo`、16-byte
`physicalEnqueueAttemptId`、`STARTED/QUEUED/DEFINITELY_NOT_QUEUED/UNCERTAIN`
状态、exact NDR1 outcome bytes（尚无结果时 absent）、`startedAtEpochMs`、
`ownershipNotAfterEpochMs`、`uncertaintyAtEpochMs`、可选 retryRequestId/hash 和 revision。
attempt 数由租户策略硬限制，
但不能因覆盖旧 attempt 而把历史 uncertainty 当作 definitive。

算法：

1. Semantic Core prepare 完成但未进行 Broker I/O；
2. `putIfAbsent(keyHash)` 持久化 exact prepared bytes/bodyHash；lost response 时 exact reread；
3. 同 key + 同 bodyHash 返回相同 prepared identity；
4. 同 key + 不同 bodyHash 表示当前 canonical request 从未取得 prepared identity，返回 response
   field 2 中的现有稳定码 `PREPARED_SUBMISSION_MISMATCH`，不暴露已有 prepared bytes/ref，也不触发
   Broker；
5. 单 record CAS 先追加一个带新 `PhysicalEnqueueAttemptId` 的 `STARTED` attempt，
   phase 改为 `ACTIVE`；只有收到该 CAS 的确定成功响应的 proposer 才取得一个不可序列化、
   one-shot 的 `GatewayAttemptOwnershipPermit(recordRevision, attemptId, notAfter)`（实现
   `TransportOwnershipPermit`）并允许调用 transport；
   CAS response 丢失后即使 reread 看见同一个 attempt，也不能重建 permit 或调用 transport；
6. transport callback 以 `(keyHash, attemptNo, physicalAttemptId)` CAS 写入 exact NDR1 outcome；
   Gateway 进程在 `STARTED` 后退出时，同-key caller 等待到持久的
   `uncertaintyAtEpochMs` 后以 CAS 把该 attempt 收敛到 `UNCERTAIN`，不能猜作未提交；迟到的
   authenticated callback 仍可 CAS merge 为 `QUEUED`；
7. Gateway crash 后只重用已记录 prepared bytes；绝不重新 prepare 到新 Route。

`GatewayAttemptOwnershipPermit.tryTransferToLibraryOwnership()` 在 production transport 紧邻
底层 client send 的同一调用栈内做 `AVAILABLE -> LIBRARY_OWNED` CAS，并同时检查 trusted
deadline 与本地 monotonic gate age；失败时不调用 client library或进行 Broker I/O。
`uncertaintyAtEpochMs` 不早于 permit 的
`notAfter`。已进入 library ownership 的 callback 可以在 deadline 后作为 late evidence merge；
尚未进入 ownership 的过期 permit 永远不能复活。读取 `ACTIVE/STARTED` 的其它 Gateway
实例只能 await/收敛 uncertainty，绝不能“接管并发送”。

两个并发首次请求可以各自在内存 prepare 一个随机 identity，但只有 `putIfAbsent` winner 的
bytes 成为该 key 的 prepared identity；loser 丢弃自己的零-I/O对象并读取 winner record。
未取得 Oxia record 的本地 identity 从未进入 Broker，也不形成 Command obligation。Oxia
unavailable、CAS unknown 或 durable reread 失败时禁止调用 transport。

普通的同-key RPC 只读取现有状态，不会因为客户端重试而再发一次 Broker 请求。若 active
attempt 尚未到 uncertainty deadline，handler 可以在 caller deadline 内 await record revision；
caller deadline 先到只结束 RPC，不改 attempt 语义。
`RetryUncertain` 是显式的物理重试：它要求原 key、expected prior
`physicalEnqueueAttemptId` 和一个新的 16-byte `retryRequestId`，CAS 校验当前 aggregate 仍为
uncertain、expected prior 等于当前 aggregate 中携带的最高 unresolved attempt ID、phase 为
QUIESCENT 且 attempt cap 未耗尽后，从记录中读取同一
`preparedSubmissionBytes` 并追加新 attempt；调用方不能提交
替代 bytes。`retryRequestId` 和 hash 存在同一 Oxia record 中且跨 attempts 唯一，因此重复
RPC 返回同一新 attempt；同 ID 配不同 prior request 返回 field 2
`PREPARED_SUBMISSION_MISMATCH` 且不暴露 prepared ref，
不需要不受支持的跨 record transaction。若 expected prior 已 stale，返回当前 aggregate 且不创建
attempt；handler 不能在等待另一个 retry 完成后用旧 precondition 自动补发。并发 callback 使用
CAS merge：任一 attempt `QUEUED` 则 aggregate 为
`QUEUED`；只有所有已启动 attempt 都有 definitive non-persistence proof 时才可 aggregate
为 `DEFINITELY_NOT_QUEUED`；其余组合均为 `ENQUEUE_UNCERTAIN`。

aggregate bytes 的选择也是 closed：第一个成功 CAS 进入 record 的 `QUEUED` receipt 一旦成为
aggregate 就不再被另一 queued receipt 替换；全 definite 且 QUIESCENT 时返回最高 attemptNo 的
definitive proof；uncertain 返回最高仍 unresolved 的 attempt，作为下一次显式 retry 的 CAS
precondition。late evidence 只能把 uncertain
收敛为 queued/全-definite，不能把 queued 降级或仅因新 guard mismatch 抹掉旧 uncertainty。
这里的 Gateway attempt state 名称对两种 prepared branch 共用：managed 的
`QUEUED`/`DEFINITELY_NOT_QUEUED`/`UNCERTAIN` 分别绑定 managed NDR1 三态；native
的同名内部 state 分别绑定
`NativeDeliveryReceiptV1`/native-definite/native-uncertain。record、attempt、aggregate 始终保持
field 5 冻结的 managed/native branch，不能用一次重试跨分支或把 native receipt 编成 managed
queued receipt。

后台 `GatewayAppliedObservationService` 可对 managed uncertainty 使用 stored prepared frame
解出的 exact historical Route、CommandId 和 CommandHash 做内部 authenticated bare-command
lookup，并把 exact applied/rejected query result 作为独立 observation 返回/审计。但 Worker 的
Source Position/dedupe result 不含原 Producer response hash，不能构造 Registry §6.3 要求的
`SafeBrokerAckV1`；因此 observation 绝不能伪造 NDR1 queued receipt，也不 CAS 修改 idempotency
aggregate。调用方通过 `GetCommandResult` 的 bare CommandId locator 读取该 observation；Schedule/
`RetryUncertain` 仍只返回当前 submission aggregate。只有原 guarded Producer callback 或
durable transport evidence 中逐字完整的 NDR1
outcome 可以作为 attempt late evidence。`NOT_FOUND`、query timeout、Route/Owner transition 或
freshness fence 尚未闭合也不能推导 definitive non-persistence。Native uncertainty 没有 Delay
query authority，只能等待原 Broker evidence 或走显式 retry/operator 流程。Observation service
不创建 physical attempt、不重发 bytes；未来若要把 source observation 升格为 queued，必须先用
新的 Registry revision 定义 source-derived receipt/evidence branch。

Oxia idempotency 是 Gateway 网络请求去重，不是 Command application 权威。`PREPARED` 不等于
queued。`aggregateOutcomeBytes` 从完整 attempt 集合单调计算：任一 `QUEUED` 即 queued；
全部已开始 attempt 都 definite 才是 definitely-not-queued；存在 active/unknown 则 absent 或
uncertain。phase 只表示是否仍有 live attempt，不能被当作 submission outcome。

该方案故意只更新一个 Oxia value，不依赖当前 client 不提供的 cross-record transaction。
record 的 canonical byte size、attempt 数和单 prepared payload 都有硬上限；
`gatewayIdempotencyMaxRecordBytes` 必须小于认证过的 Oxia value/request limit。超限发生在
Broker I/O 前并返回 `PAYLOAD_TOO_LARGE`；大 payload 改走 reserve/upload/attest/commit。
ACTIVE/含 uncertain attempt 的 record 不可因 retention 或空间压力删除。QUIESCENT record
只有在 trusted time 越过 `retainUntil`、没有 callback/attempt lease 且审计导出完成后才可 GC。

### 7.5 配额与背压

Gateway 至少按 authenticated tenant 分离：

- Schedule QPS/bytes；
- Cancel/Reschedule control QPS；
- concurrent transport attempts；
- uncertain attempt count；
- idempotency records/bytes；
- query/await streams；
- large-payload reservations。

Schedule pool 耗尽不能占用 control reserve。Cancel/Reschedule、查询 uncertain 和关闭请求使用独立上限。Gateway transport executor、gRPC event loop、Oxia callback executor 分离，任何 `.join()`/阻塞 Broker future 都不得运行在 Netty event loop。

2026-08-15 implementation evidence: `OxiaGatewayAdmissionController` stores
one strict canonical lease record per authenticated tenant-scope digest. Its
schedule, retry-uncertain and control pools are independent; schedule bytes
are charged in the same version-CAS successor. Expiry reclaim, bounded CAS,
exact response-loss reread and idempotent release are covered by
`OxiaGatewayAdmissionControllerTest`. `InMemoryGatewayAdmissionController`
remains a local conformance implementation. The follow-up
`OxiaRealGatewayAdmissionSmokeTest` passes against the isolated Oxia Docker
service for expiry/release and exact empty-record reread. This closes the
durable admission composition plus a single-node service cut, not HA/session
observability, rate-window/load proof, cross-record transactionality or
production Gateway wiring.

## 8. Command Transport SPI

### 8.1 SPI

```java
public interface CommandTransport extends AutoCloseable {
    AdapterKindV1 adapterKind();
    CommandTransportKey key();
    CompletionStage<TransportResult> send(TransportRequest request,
                                          TransportOwnershipPermit ownershipPermit);
}

public interface TransportOwnershipPermit extends AutoCloseable {
    PhysicalEnqueueAttemptId physicalAttemptId();

    // AVAILABLE -> LIBRARY_OWNED exactly once; false performs no Broker/client I/O.
    boolean tryTransferToLibraryOwnership();

    TransportOwnershipState state();

    // AVAILABLE -> INVALID; LIBRARY_OWNED is monotonic and is not undone by close().
    @Override
    void close();
}

public enum TransportOwnershipState {
    AVAILABLE,
    LIBRARY_OWNED,
    INVALID
}

public sealed interface CommandTransportKey
        permits KafkaCommandTransportKey, PulsarCommandTransportKey {
    AdapterKindV1 kind();
    CredentialBindingKey credentialBinding();
}

public record KafkaCommandTransportKey(
        String authenticatedClusterId,
        String canonicalTopic,
        UUID nativeTopicUuid,
        int partition,
        CredentialBindingKey credentialBinding) implements CommandTransportKey {
    @Override public AdapterKindV1 kind() { return AdapterKindV1.KAFKA; }
}

public record PulsarCommandTransportKey(
        String authenticatedClusterId,
        String canonicalPhysicalTopic,
        Bytes32 resourceIncarnation,
        long topicCreationTimestamp,
        int partition,
        CredentialBindingKey credentialBinding) implements CommandTransportKey {
    @Override public AdapterKindV1 kind() { return AdapterKindV1.PULSAR; }
}

public record CredentialBindingKey(
        long generation,
        Digest32 bindingDigest,
        Digest32 resolvedCredentialFingerprint) {
}

public sealed interface TransportRequest
        permits KafkaProduceRequest, PulsarSendRequest {
}

public sealed interface TransportResult
        permits KafkaProduceResult, PulsarSendResult {
}

public interface CommandTransportRegistry extends AutoCloseable {
    CommandTransport exact(CommandTransportKey key);
}

public interface SubmissionTransportPlanResolver {
    SubmissionTransportPlan resolve(AuthenticatedTenantContext tenant,
                                    PreparedSubmissionV1 submission);
}

public record SubmissionTransportPlan(
        PreparedSubmissionV1 submission,
        SubmissionRouteAuthority routeAuthority,
        CommandTransportKey transportKey,
        TransportRequest request,
        SubmissionProjectionKey projectionKey) {
}

public sealed interface SubmissionRouteAuthority
        permits ManagedRouteAuthority, NativeTargetAuthority {
}

public record ManagedRouteAuthority(RouteSnapshotV1 historicalRoute)
        implements SubmissionRouteAuthority {
}

public record NativeTargetAuthority(NativePreparedDeliveryV1 prepared)
        implements SubmissionRouteAuthority {
}

public enum PreparedSubmissionBranch {
    MANAGED,
    NATIVE
}

public record SubmissionProjectionKey(
        PreparedSubmissionBranch branch,
        AdapterKindV1 adapterKind) {
}

public interface SubmissionOutcomeProjector {
    SubmissionProjectionKey key();

    SubmissionOutcomeMessageV1 project(
            SubmissionTransportPlan plan,
            PhysicalEnqueueAttemptId physicalAttemptId,
            TransportResult result);

    SubmissionOutcomeMessageV1 localFailure(
            SubmissionTransportPlan plan,
            PhysicalEnqueueAttemptId physicalAttemptId,
            StableCode code);

    SubmissionOutcomeMessageV1 uncertain(
            SubmissionTransportPlan plan,
            PhysicalEnqueueAttemptId physicalAttemptId,
            StableCode code);
}

public interface SubmissionOutcomeProjectorRegistry {
    SubmissionOutcomeProjector exact(SubmissionProjectionKey key);
}
```

上述代码块有明确的 module 分界：`CommandTransport`、key、request/result 与
`CommandTransportRegistry` 在 `delay-transport-spi`；从
`SubmissionTransportPlanResolver` 开始的 plan/authority/projection/coordinator 类在
`delay-client-core`，因为它们同时依赖 route-spi 与 transport-spi。不得为了共用
把 `RouteSnapshotV1` 引入 `delay-transport-spi`，否则会违反 §3.2 的依赖方向。

`SubmissionTransportPlanResolver` 不是第二个 Semantic Core：它严格 decode 已冻结 branch，
managed 分支从 41-byte self-routing ID 解出 Route Incarnation/partition，然后使用
`RouteSnapshotProvider.exact(incarnation, tenant)` 取授权的历史 snapshot；native 分支只校验
prepared ref/snapshot 中已冻结的 target。它禁止选新 active Route、重新 hash、重编 Command
或改换 managed/native branch。`SubmissionRouteAuthority` 用 closed branch 表达这两种
authority，不使用 nullable `RouteSnapshotV1`。
在 native 分支，resolver 还必须在 permit transfer 前调用本地
`NativeSubmissionAuthorityVerifier`：重算 prepared bytes/submission hash，验证完整 signed
capability snapshot、expiry、target/partition/guard attestation、principal scope，并从已验证的
credential cache 解决 exact generation/digest/fingerprint。cache miss/漂移返回 typed local
definitive outcome，不可在 submit 时改用新 target/snapshot。Route/capability/credential cache 都由
后台 control-plane 刷新；resolver 本身不做 Oxia/Admin/vault 网络调用。Producer
connection establishment 可在 transport 内异步进行，但只有真正进入底层 guarded
send 才 transfer permit ownership。

`DefaultSubmissionCoordinator` 的固定调用顺序是：

```text
resolver.resolve(tenant, exact prepared bytes)
  -> registry.exact(plan.transportKey)
  -> transport.send(plan.request, original one-shot permit)
  -> projectorRegistry.exact(plan.projectionKey)
  -> projector.project(plan, permit.physicalAttemptId, typed result)
```

resolver/registry/transport 在 permit 仍为 `AVAILABLE` 时抛出或返回本地失败，由 projector
构造 local-before-ownership definitive branch；`send` 返回 null stage、抛出或 callback 注册
失败时，coordinator 先读 permit state：`AVAILABLE/INVALID` 仍可 local definite，
`LIBRARY_OWNED` 只能 uncertain。任何 malformed typed result 也只能 integrity/uncertain。
coordinator 在 `send` 同步返回/抛出后立即 `close()` permit，但已 transfer 状态不回退。
`LIBRARY_OWNED -> uncertain` 这条只适用于 coordinator 面对的未分类抛出/null/
callback 故障；底层 guarded client 仍可以返回一个完整 typed
`DEFINITIVELY_NOT_PERSISTED` result，例如 Kafka client 证明请求未进 accumulator，或
Broker 返回已注册的 pre-persistence rejection。这种结论由 projector 验证 exact
proof，不由 permit state 单独推导。transport 必须 catch 并转成 typed result；若只向上
抛出 exception，coordinator 仍按 uncertainty 处理。

现有 `PinnedKafkaCommandIngress.projectWire` / `PinnedPulsarCommandIngress.projectWire` 在 D1/D2/D3
抽为 top-level `KafkaManagedSubmissionOutcomeProjector` /
`PulsarManagedSubmissionOutcomeProjector`；native 分支使用
`PulsarNativeSubmissionOutcomeProjector`。现有两个 `Pinned*CommandIngress` 留在 conformance
artifact 做 facade，删除其 nested transport SPI 的 production 地位，不再同时承担
transport ownership、调用和 NDR1 投影。

`CommandTransport.send` 必须在返回 `CompletionStage` 之前、紧邻底层 `sendGuarded`/`sendAsync`
调用的同一线程栈执行 `tryTransferToLibraryOwnership()`；中间禁止 executor handoff、queue 或阻塞
I/O。返回 false 时不得调用 client library，并返回 local definitive non-persistence。Coordinator
在 `send` 调用返回/抛出后立即 `close()` permit：未 transfer 的 permit 因而永久失效；已 transfer
的 permit 保持 `LIBRARY_OWNED`，后续 timeout/cancel/callback failure 按 UNKNOWN。transport result
中的 physical attempt ID 必须逐字等于 permit ID。

`Bytes32`/`Digest32` 是 defensive-copy、content-equality 的 immutable value type。任何用作
record/map key 的 binary field 都必须使用这种 wrapper；禁止直接把 `byte[]` 放进 Java
record 后依赖默认 reference `equals/hashCode`。本文件其它 Java 草图中的 `byte[]` 也必须在
真实 constructor/accessor 做 defensive copy。
Nereus 的 route/transport SPI 使用 JDK `java.util.UUID` 表达 Kafka native topic UUID，
不依赖 Kafka artifact；只在 `delay-client-kafka` 边界按两个 raw 64-bit half 无损
转成 `org.apache.kafka.common.Uuid`。禁止经 UUID text 重新 parse 作为 evidence
preimage。

### 8.2 Registry 生命周期

- key 包含 canonical physical topic/partition、exact Broker resource incarnation、Credential
  Binding generation/digest 和 resolved credential fingerprint；
- 同名 Topic 的旧/新 incarnation 不能共享 Producer；
- Route watch 只可创建新 entry 或停止新借用，不能把旧 entry 的 key 原地改成新 incarnation；
- close 分 `OPEN -> DRAINING -> CLOSED`；DRAINING 不接新 attempt，但保留 callback/uncertain evidence；
- credential 等价轮换创建新 key，旧 transport 保留到 lease/attempt/quiescence 全部结束；
- lookup 失败发生在 Producer ownership 前，可返回 local definitive `ROUTE_SNAPSHOT_UNAVAILABLE`；
- 一旦 transport 已取得 library/network ownership，异常、null stage、callback registration failure 都映射 UNKNOWN。

### 8.3 结果闭包

生产 transport 只能返回：

```text
PERSISTED
  exact pinned identity + source position + broker timestamp + response evidence

DEFINITIVELY_NOT_PERSISTED
  authenticated local/pre-persistence guard proof + stable code + evidence

UNKNOWN
  no success position; may carry bounded diagnostic, never fabricate proof
```

`PinnedKafkaCommandIngress` 和 `PinnedPulsarCommandIngress` 继续做 NDR1 投影；transport 不自行构造 Nereus receipt。

## 9. Kafka Client 代码设计

实施基线：`/Users/liusinan/apps/ideaproject/nereusstream/kafka` 的 `trunk@c300006a77`。实施分支建议：`nereus/delay-guarded-producer-v1`。

### 9.1 公共、通用 API

新增：

```java
package org.apache.kafka.clients.producer;

public final class ProducerResourceGuard {
    private final String authenticatedClusterId;
    private final String canonicalTopic;
    private final Uuid expectedTopicId;
    private final int partition;

    public ProducerResourceGuard(String authenticatedClusterId, String canonicalTopic,
                                 Uuid expectedTopicId, int partition) {
        this.authenticatedClusterId = Objects.requireNonNull(authenticatedClusterId);
        this.canonicalTopic = Objects.requireNonNull(canonicalTopic);
        this.expectedTopicId = Objects.requireNonNull(expectedTopicId);
        if (authenticatedClusterId.isEmpty() || canonicalTopic.isEmpty()
                || Uuid.ZERO_UUID.equals(expectedTopicId) || partition < 0) {
            throw new IllegalArgumentException("invalid Producer resource guard");
        }
        this.partition = partition;
    }
    public String authenticatedClusterId() { return authenticatedClusterId; }
    public String canonicalTopic() { return canonicalTopic; }
    public Uuid expectedTopicId() { return expectedTopicId; }
    public int partition() { return partition; }
    // content-based equals/hashCode/toString; toString never exposes credentials
}

public final class GuardedResponseEvidence {
    private final String authenticatedClusterId;
    private final String canonicalTopic;
    private final Uuid expectedTopicId;
    private final int partition;
    private final short requestVersion;
    private final int correlationId;
    private final int brokerNodeId;
    private final short errorCode;
    private final long baseOffset;
    private final long logAppendTimeMs;
    private final OptionalInt responseLeaderEpoch;
    private final byte[] produceRequestBodySha256;
    private final byte[] produceResponseBodySha256;
    private final byte[] selectedBatchRecordsSha256;
    private final byte[] selectedRecordValueSha256;
    private final int selectedBatchRecordIndex;
    private final int selectedBatchRecordCount;

    GuardedResponseEvidence(String clusterId, String topic, Uuid topicId,
            int partition, short requestVersion, int correlationId, int brokerNodeId,
            short errorCode, long baseOffset, long logAppendTimeMs,
            OptionalInt responseLeaderEpoch, byte[] produceRequestBodySha256,
            byte[] produceResponseBodySha256, byte[] selectedBatchRecordsSha256,
            byte[] selectedRecordValueSha256,
            int selectedBatchRecordIndex, int selectedBatchRecordCount) {
        // Assign only after exact guard/response validation; reject invalid sentinels.
        this.authenticatedClusterId = Objects.requireNonNull(clusterId);
        this.canonicalTopic = Objects.requireNonNull(topic);
        this.expectedTopicId = Objects.requireNonNull(topicId);
        this.partition = partition;
        this.requestVersion = requestVersion;
        this.correlationId = correlationId;
        this.brokerNodeId = brokerNodeId;
        this.errorCode = errorCode;
        this.baseOffset = baseOffset;
        this.logAppendTimeMs = logAppendTimeMs;
        this.responseLeaderEpoch = Objects.requireNonNull(responseLeaderEpoch);
        this.produceRequestBodySha256 = requireSha256(produceRequestBodySha256);
        this.produceResponseBodySha256 = requireSha256(produceResponseBodySha256);
        this.selectedBatchRecordsSha256 = requireSha256(selectedBatchRecordsSha256);
        this.selectedRecordValueSha256 = requireSha256(selectedRecordValueSha256);
        if (selectedBatchRecordIndex < 0 || selectedBatchRecordCount <= 0
                || selectedBatchRecordIndex >= selectedBatchRecordCount) {
            throw new IllegalArgumentException("invalid batch index/count");
        }
        this.selectedBatchRecordIndex = selectedBatchRecordIndex;
        this.selectedBatchRecordCount = selectedBatchRecordCount;
    }
    public String authenticatedClusterId() { return authenticatedClusterId; }
    public String canonicalTopic() { return canonicalTopic; }
    public Uuid expectedTopicId() { return expectedTopicId; }
    public int partition() { return partition; }
    public short requestVersion() { return requestVersion; }
    public int correlationId() { return correlationId; }
    public int brokerNodeId() { return brokerNodeId; }
    public short errorCode() { return errorCode; }
    public long baseOffset() { return baseOffset; }
    public long logAppendTimeMs() { return logAppendTimeMs; }
    public OptionalInt responseLeaderEpoch() { return responseLeaderEpoch; }
    public byte[] produceRequestBodySha256() { return produceRequestBodySha256.clone(); }
    public byte[] produceResponseBodySha256() { return produceResponseBodySha256.clone(); }
    public byte[] selectedBatchRecordsSha256() { return selectedBatchRecordsSha256.clone(); }
    public byte[] selectedRecordValueSha256() { return selectedRecordValueSha256.clone(); }
    public int selectedBatchRecordIndex() { return selectedBatchRecordIndex; }
    public int selectedBatchRecordCount() { return selectedBatchRecordCount; }
    private static byte[] requireSha256(byte[] value) {
        Objects.requireNonNull(value);
        if (value.length != 32) throw new IllegalArgumentException("SHA-256 length");
        return value.clone();
    }
    // content-based equals/hashCode/toString
}

public final class GuardedRecordMetadata {
    private final RecordMetadata recordMetadata;
    private final ProducerResourceGuard resourceGuard;
    private final GuardedResponseEvidence responseEvidence;

    GuardedRecordMetadata(RecordMetadata metadata, ProducerResourceGuard guard,
                          GuardedResponseEvidence evidence) {
        this.recordMetadata = Objects.requireNonNull(metadata);
        this.resourceGuard = Objects.requireNonNull(guard);
        this.responseEvidence = Objects.requireNonNull(evidence);
    }
    public RecordMetadata recordMetadata() { return recordMetadata; }
    public ProducerResourceGuard resourceGuard() { return resourceGuard; }
    public GuardedResponseEvidence responseEvidence() { return responseEvidence; }
}

@FunctionalInterface
public interface GuardedCallback {
    void onCompletion(GuardedRecordMetadata metadata, Exception exception);
}

public interface GuardedProducer<K, V> extends Producer<K, V> {
    Future<GuardedRecordMetadata> sendGuarded(ProducerRecord<K, V> record,
                                              ProducerResourceGuard guard);

    Future<GuardedRecordMetadata> sendGuarded(ProducerRecord<K, V> record,
                                              ProducerResourceGuard guard,
                                              GuardedCallback callback);
}
```

Kafka `clients` 模块的 inspected trunk 仍以 Java 11 为最低 client bytecode（根
`build.gradle` 的 `minClientJavaVersion = 11`），因此以上三个 public value 必须实现为
Java 11-compatible `final class`，不能使用 `record`、sealed type 或其它 Java 17 语法。真实代码
补齐 public constructor、accessor、content-based `equals/hashCode` 和安全 `toString`；所有字段
在 constructor 校验，instance 完全 immutable。

`GuardedCallback` 与现有 `Callback` 保持同一 completion 约定：success 时 metadata 非空且
exception 为 null；failure 时 metadata 为 null 且 exception 非空；一次 send 恰好完成一次。

`KafkaProducer` 实现 `GuardedProducer`。不修改 `ProducerRecord`，避免 guard 被 interceptor 当作业务 header/value 改写；guard 是独立调用参数。`sendGuarded` 要求 record 显式 partition 且等于 guard.partition。

K1 的 API/证据范围是 non-transactional single-record Produce：它可用于 Command Topic，不能
证明主设计中的 Kafka target + receipt atomic transaction。后者仍需独立的 source-locked
transactional v13 guarded request 设计/patch/test；完成前对应 Destination Profile 不得激活，
也不得回落到 Produce v11/name-only transaction。

在 `org.apache.kafka.clients.producer` 新增 public error；它是 client operation failure，
不冒充新的 Kafka wire error：

```java
public final class ResourceGuardException extends KafkaException {
    private final ResourceGuardFailureReason reason;
    private final ProducerResourceGuard guard;
    private final Optional<GuardedResponseEvidence> responseEvidence;
    private final boolean definitelyNotPersisted;

    ResourceGuardException(String message, Throwable cause,
            ResourceGuardFailureReason reason, ProducerResourceGuard guard,
            Optional<GuardedResponseEvidence> responseEvidence,
            boolean definitelyNotPersisted) {
        super(message, cause);
        this.reason = Objects.requireNonNull(reason);
        this.guard = Objects.requireNonNull(guard);
        this.responseEvidence = Objects.requireNonNull(responseEvidence);
        this.definitelyNotPersisted = definitelyNotPersisted;
    }
    public ResourceGuardFailureReason reason() { return reason; }
    public ProducerResourceGuard guard() { return guard; }
    public Optional<GuardedResponseEvidence> responseEvidence() { return responseEvidence; }
    public boolean definitelyNotPersisted() { return definitelyNotPersisted; }
}

public enum ResourceGuardFailureReason {
    INVALID_GUARD,
    UNSUPPORTED_CONFIGURATION,
    CLUSTER_MISMATCH,
    TOPIC_ID_MISMATCH,
    UNSUPPORTED_REQUEST_VERSION,
    AUTHENTICATED_BROKER_REJECTION,
    RESPONSE_EVIDENCE_INTEGRITY,
    AMBIGUOUS_PRIOR_ATTEMPT
}
```

Evidence/metadata/exception constructor 均为 package-private，只能由 Kafka client completion path
产生；应用只能构造输入 guard 并读取输出。`definitelyNotPersisted` 由 Sender 的 ownership/response
state machine 计算，不能由调用方传入或从 exception message 推断。

Nereus 只把 `definitelyNotPersisted=true` 映射为 definitive；所有普通 timeout/disconnect/unknown callback 均为 UNKNOWN。

### 9.2 `KafkaProducer`

`KafkaProducer.sendGuarded` 在 interceptor 之后、serialization/accumulator 之前执行：

1. guard 非空；clusterId/canonical topic 非空且 canonical；topicId 非 zero；record topic/显式
   partition 与 guard 逐字匹配；
2. 禁止配置 `transactional.id` 和 `acks=0`；允许 non-transactional idempotence；guarded API
   必须取得 Broker response，配置不兼容在 accumulator ownership 前返回
   `UNSUPPORTED_CONFIGURATION`；
3. `waitOnMetadata` 必须同时得到 cluster ID、topic ID 和 partition leader；
4. cluster 不同或当前非 zero topic ID 不同，在 accumulator ownership 前返回 definitive guard failure；
5. metadata 暂缺/zero 只触发 bounded refresh；在 accumulator/Produce ownership 前超时是
   local definitive non-persistence，不能改走 name-only；
6. serialization 后计算 exact serialized value SHA-256，调用新的 accumulator overload，把
   immutable guard 传入 batch、把 value digest 传入该 record 的 thunk/future context。

普通 `send()` 完全保持现状。

新增 internal `FutureGuardedRecordMetadata`，它包装现有 `FutureRecordMetadata` 并在 `get()`
成功后沿 split chain 从最终 `ProduceRequestResult` 读取 immutable batch evidence，再用最终
child-relative record index 与该 record 的 serialized-value digest 构造 public per-record
evidence。不能把 callback 时刻的 metadata 拼成 evidence。`FutureRecordMetadata` 增加
package-private `guardedResponseEvidence()` 和 immutable value digest；普通 Future 的 public
行为不变。

### 9.3 `RecordAccumulator` / `ProducerBatch`

改动点：

```java
RecordAccumulator.append(..., ProducerResourceGuard resourceGuard,
                         byte[] serializedValueSha256, ...)

ProducerBatch(
    TopicPartition topicPartition,
    ProducerResourceGuard resourceGuard,
    MemoryRecordsBuilder builder,
    long createdMs,
    boolean splitBatch)
```

规则：

- `ProducerBatch` 保存 nullable immutable guard；
- 每个 guarded thunk/future 保存 exact post-serializer value digest；batch-level request/response/
  `MemoryRecords` hashes 不复制到每个 thunk；
- `tryAppend` 只有 guard 全字段相等才可进入已有 batch；guarded 与 unguarded、旧/新 TopicId 不能混 batch；
- `ProducerBatch.split()` 把同一 guard 复制到每个子 batch，并把每个 thunk/value digest 移到实际
  child、重建 child-relative index；
- retry/reenqueue 不重新读 name -> topicId；
- batch 记录 `ambiguousAttemptObserved`。任何已写入 NetworkClient 后的 disconnect/timeout/response-loss 将其置 true，后续 guard rejection 也不能抹掉历史 uncertainty。

### 9.4 `Sender`

发送端不再对 guarded batch 执行：

```java
metadata.topicIds().get(batch.topicPartition.topic())
```

而是：

```java
Uuid topicId = batch.resourceGuard() == null
        ? metadata.topicIds().getOrDefault(topic, Uuid.ZERO_UUID)
        : batch.resourceGuard().expectedTopicId();
```

具体修改：

- `sendProduceRequests` 把 guarded 与 ordinary batch 分开，避免一条 guarded batch 把普通老 Broker 请求强制升级后连带失败；
- guarded request 使用 `new ProduceRequest.Builder((short) 13, ApiKeys.PRODUCE.latestVersion(), data)`；不允许 API negotiation 回落到 12；
- request map 改为 `(topicId, partition) -> ProducerBatch`，v13 response 不再经当前 `metadata.topicNames()` 反查后定位；
- 一个 guarded request 中的所有 batch 必须匹配当前 authenticated cluster ID；
- leader 可以从新 metadata 更新，但 wire 始终携 expected TopicId；即使 metadata 已指向同名 replacement，也只会向 Broker发送旧 ID，不能写入 replacement；
- `NotLeaderOrFollower`/leader epoch 变化重试同一 ID；
- `UNKNOWN_TOPIC_ID` 触发一次 exact-ID metadata refresh。若确认 name 映射到不同非 zero ID，则停止；
- 若此前没有任何 ambiguous attempt，confirmed guard rejection 可完成 definitive failure；否则完成 UNKNOWN guard failure；
- transaction V1 的 Produce v11 上限与 guarded v13 冲突，在 API 层直接拒绝，不做事务降级。

在当前 inspected trunk 的 `KafkaApis.handleProduceRequest` 中，v13 请求的 TopicId
无法解析成 topic name 时在进入 authorized append map 前返回
`UNKNOWN_TOPIC_ID(100)`。K1 只把这一个 partition error 注册为 Broker resource-guard
definitive allowlist，且仍需 request/response TopicId+partition 匹配、authenticated response
与无 prior ambiguity。`INCONSISTENT_TOPIC_ID` 在该基线不是 Produce v13 的这个
Broker 路径；其它 Produce error 继续走原有 retry/failure 逻辑，不能被通用
`error != NONE` 分支全部升格为 definitive。新增 allowlist 必须另有 Broker
source-lock 和持久化前 cut test。

为了把 response evidence 与 Future/callback 绑定到同一 batch，internal completion 改为：

```text
ProduceRequestResult
  + nullable final GuardedBatchCompletionEvidence（set-once before done）
    request/response body hashes + child MemoryRecords hash/count + guard/request context

ProducerBatch.completeGuarded(baseOffset, logAppendTime, batchEvidence)
  -> produceFuture.set(..., batchEvidence)
  -> for each thunk: combine batchEvidence + child index + its value digest
  -> guarded callback(metadata, perRecordEvidence, null)
  -> produceFuture.done()

FutureGuardedRecordMetadata.get()
  -> ordinary FutureRecordMetadata.get()
  -> require final chained ProduceRequestResult.batchEvidence
  -> combine final relativeOffset + stored value digest
  -> GuardedRecordMetadata(perRecordEvidence)
```

`ProducerBatch.Thunk`/internal append callback 增加 guarded completion 分支；ordinary callback
仍走现有二参 API。split 后原 thunk/future chain 指向子 batch，guard、batch evidence、index 和
value digest 必须来自真正完成该 record 的子 batch。batch evidence 缺失、重复 set、index/digest
不一致或与 batch guard 不一致均以 integrity
exception 完成 guarded record，不能先调用 success callback。

### 9.5 Broker response evidence

`GuardedResponseEvidence` 是 guarded API 的公开 immutable result，不进入业务 record，
也不改变普通 `RecordMetadata`。Sender 从完成该 request 的真实 response/correlation/broker
上下文构造它，`GuardedRecordMetadata` 同时携带普通 metadata、exact input guard 和证据。
失败时 exception 持有同形 rejection evidence；不得用 callback 时刻的当前 metadata 回填。
success evidence 固定 `errorCode=0`、nonnegative base offset、`logAppendTimeMs >= -1`（Kafka 的
`CreateTime` topic 合法返回 `-1` sentinel）、actual request
version >=13、request destination Broker node 和 correlation ID。只有 ProduceResponse 的
`currentLeader.leaderEpoch` 非负时才放入 optional `responseLeaderEpoch`；发送前 metadata 中的
leader epoch 没有出现在 Produce request/response 中，不能伪装成 authenticated response
evidence。`RecordMetadata.offset()` 必须等于 checked `baseOffset + batchIndex`。`acks=0`、
`DUPLICATE_SEQUENCE_NUMBER` 的 unknown offset/time 或 response-less completion 不能产生 guarded
success。
`produceRequestBodySha256` 是 actual negotiated version 下、header 之外的 exact
`ProduceRequestData` serialized body SHA-256；`produceResponseBodySha256` 同理覆盖 exact
`ProduceResponseData` body（包括 retained unknown tagged fields）；`selectedBatchRecordsSha256`
覆盖真正发送的 child `MemoryRecords` bytes，`selectedRecordValueSha256` 覆盖该 future 对应的
post-interceptor/post-serializer value，并与 record index/count 一起证明它属于该 child batch。
Sender 在 request builder/version/correlation 与
response 同时可见时计算并 set-once 到 batch result，不能在 callback 中从当前 metadata 重建。
无法保留 exact versioned bytes/unknown tags、hash 重算不一致或 split 后仍引用 parent batch hash
时，guarded completion 是 integrity/UNKNOWN，不是 success/definitive rejection。

实现不从 `toString()`、request header 或 callback DTO 拼 bytes。`Sender` 为 guarded
request 保留一个 `GuardedProduceRequestContext`，在 `ClientResponse.requestHeader().apiVersion()`
可见且 batch 未 deallocate 时，使用 Kafka generated-message codec
`MessageUtil.toByteBufferAccessor(data, version)` 对真正交给 `ProduceRequest.Builder` 的
`ProduceRequestData` 和收到的 `ProduceResponseData` 生成 header-free canonical body，立即
SHA-256 并丢弃临时 buffer。Response 的 unknown tagged fields 必须在 parse/re-encode 后
byte-round-trip；否则 integrity failure。K1 首版允许这个 guarded-only bounded copy，上限是
已验证的 Produce request size；若 benchmark 要求 streaming digest，优化必须和上述
canonical bytes 做 golden byte-equivalence，不得改 hash preimage。

`delay-client-kafka` 对这些字段生成 Registry §6.3
`KafkaGuardedRequestEvidenceV1` / `KafkaGuardedResponseEvidenceV1` canonical bytes，并把 request
与 response evidence 分别交给 `KafkaProduceResult`；不能继续让
`WireIngressOutcomeSupport.brokerDefinite` 把 NDL1 frame 当作 Broker request bytes。Nereus 还要求
`selectedRecordValueSha256 == SHA-256(exact NDL1 frame)`；否则 Broker 可能持久化了 interceptor/
serializer 改写后的 value，结果必须是 integrity/UNKNOWN。如果成功路径缺少 exact TopicId、partition、request version、Broker
response 或 offset，或 `logAppendTimeMs < -1`，Nereus transport 不得把普通
`RecordMetadata.timestamp()` 猜成 persisted evidence；
该实现不能通过 Phase K1 gate。

### 9.6 Kafka 测试

至少修改/新增：

```text
KafkaProducerTest
  sendGuardedRejectsMissingPartitionBeforeAccumulator
  sendGuardedRejectsRecordTopicDifferentFromGuard
  sendGuardedRejectsClusterMismatchBeforeNetwork
  sendGuardedRejectsCurrentDifferentTopicIdBeforeNetwork
  guardedSendRejectsAcksZero
  guardedSendRejectsTransactionalProducer

RecordAccumulatorTest
  differentTopicIdsNeverShareBatch
  guardedAndUnguardedNeverShareBatch
  splitBatchRetainsExactGuard

ProducerBatchTest
  retryAndSplitRetainGuardAndAmbiguity
  splitFutureAndCallbackUseFinalChildEvidence
  guardedEvidenceIsSetOnceBeforeCallback

SenderTest
  guardedProduceUsesV13AndExpectedTopicId
  guardedProduceDoesNotUseReplacementMetadataTopicId
  leaderChangeRetriesSameTopicId
  unsupportedV13FailsClosed
  priorDisconnectThenMismatchIsUnknown
  firstConfirmedMismatchIsDefinitelyNotPersisted
  unknownTopicIdIsTheOnlyK1BrokerDefinitiveResourceError
  nonAllowlistedProduceErrorNeverSetsDefinitelyNotPersisted
  responseLessAndDuplicateSequenceCompletionAreNotGuardedSuccess
  requestResponseAndChildBatchDigestsMatchActualV13Bytes
  canonicalResponseBodyHashRetainsUnknownTaggedFields
  selectedRecordValueDigestFollowsPostInterceptorSerializedChildRecord
  digestOrRetainedTaggedFieldMismatchFailsGuardedCompletion
```

集成 cut：创建 Topic AAA，prepare/send 前删除并同名创建 BBB；旧 guard 的 payload 在 BBB 中为零，结果不得是 success。保留 AAA 只做 leader failover 时同一 guarded record 必须仍可成功。

## 10. Pulsar 5.0.0-M1 代码设计

实施基线：`/Users/liusinan/apps/ideaproject/nereusstream/pulsar` 的 `5.0.0-M1@8dae0236c0`。从该基线 `checkout -b nereus/delay-resource-guard-v1`，不基于当前无关功能分支。

### 10.1 为什么采用 first-class guard

当前 `CommandProducer.metadata` + `BrokerInterceptor.onPulsarCommand` 能做 plugin-only guard，但存在三个发布风险：

- `NotAllowedError + message string` 不是稳定 typed resource mismatch；
- producer success/send receipt 不回显 actual validated identity；
- plugin rollout/配置缺口容易退化为普通 Producer。

因此 V1 production path 使用 Pulsar 通用 first-class `TopicResourceGuard`。旧 plugin-only 方式可作为审计/兼容实验，不可满足 release gate。

### 10.2 Client API

在 `pulsar-client-api` 新增：

```java
public final class TopicResourceGuard implements Serializable {
    public static final int VERSION = 1;
    String authenticatedClusterId();
    byte[] resourceIncarnation();      // exact 32 bytes
    long topicCreationTimestamp();     // service-owned u64 bit pattern
}

public interface GuardedMessageId extends MessageId {
    TopicResourceGuard resourceGuard();
    String physicalTopic();
    int partition();
    long brokerEntryTimestamp();
    GuardedSendSuccessEvidence responseEvidence();
}

public interface GuardedConsumer<T> extends Consumer<T> {
    TopicResourceGuard resourceGuard();
    Optional<TopicResourceGuardAttestation> resourceGuardAttestation();
    long connectionGeneration();
}

public record TopicResourceGuardAttestation(
        int guardVersion,
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        long topicCreationTimestamp,
        String physicalTopic,
        int partition) {
}

public sealed interface TopicResourceGuardResponseEvidence
        permits GuardedSendSuccessEvidence, GuardedSendErrorEvidence {
    int protocolVersion();
    long connectionGeneration();
    long producerId();
    long sequenceId();
    byte[] sendCommandSha256();
    byte[] authenticatedResponseCommandSha256();
}

public record GuardedSendSuccessEvidence(
        int protocolVersion,
        long connectionGeneration,
        long producerId,
        long sequenceId,
        TopicResourceGuardAttestation attestation,
        long ledgerId,
        long entryId,
        long brokerEntryTimestamp,
        byte[] sendCommandSha256,
        byte[] authenticatedResponseCommandSha256)
        implements TopicResourceGuardResponseEvidence {
}

public record GuardedSendErrorEvidence(
        int protocolVersion,
        long connectionGeneration,
        long producerId,
        long sequenceId,
        int serverErrorCode,
        byte[] sendCommandSha256,
        byte[] authenticatedResponseCommandSha256)
        implements TopicResourceGuardResponseEvidence {
}

public final class TopicResourceGuardException extends PulsarClientException {
    TopicResourceGuard expectedGuard();
    Optional<GuardedSendErrorEvidence> responseEvidence();
    boolean definitelyNotPersisted();
}
```

这些 public value 全部 defensive-copy、content-equality；sealed success/error evidence 避免用
sentinel 混淆 presence，typed guard rejection 固定 `serverErrorCode=26`。
`delay-client-pulsar` 只从该 typed evidence 生成 canonical proof bytes，不解析 exception message。
evidence/exception constructor 由 client implementation 控制，调用方只能读取；两个 SHA-256
数组在 compact constructor/accessor clone，并在 `equals/hashCode` 按内容比较。
`topicCreationTimestamp` 使用 Java `long` 保存完整 uint64 bit pattern，property 读写使用
`Long.parseUnsignedLong`/`Long.toUnsignedString`，不能拒绝 high-bit 值或把 `-1` 当作 persisted
identity 的通用 absent sentinel。

`GuardedMessageId` 只证明当前 send completion；其标准 `toByteArray()` 仍只编码普通
Pulsar MessageId。`MessageId.fromByteArray*` 不得重建 `GuardedMessageId` 或 guard evidence，
因此 Nereus transport 必须在 callback 内把 typed evidence 持久/投影，不能稍后从序列化
MessageId 猜回 receipt。

P1 broker/client implementation additionally snapshots the wire guard into a copied
`TopicResourceGuard` before `ServerCnx` enters asynchronous authorization/topic
creation. The async path must use that snapshot rather than rereading the mutable
decoded `CommandProducer`; otherwise decoder reuse can erase the guard and create
an ordinary producer. A create-time `ResourceIncarnationMismatch` is converted to
`TopicResourceGuardException`, marked `definitelyNotPersisted`, and treated as
non-retriable. The real in-process broker cut at
`7eebd41d5b0917a0dfe5ea26ef3062a39f70a6d9` verifies this boundary together with
attested SEND evidence and same-name delete/recreate.

`ProducerBuilder<T>` 增加：

```java
ProducerBuilder<T> resourceGuard(TopicResourceGuard guard);
```

`ConsumerBuilder<T>` 同样增加 guarded source 入口：

```java
ConsumerBuilder<T> resourceGuard(TopicResourceGuard guard);
```

它返回的 native consumer 必须实现 `GuardedConsumer<T>`。P1 source factory
固定 exact physical topic、`ReceiverQueueSize=1`、Exclusive/Earliest 和
`autoUpdatePartitions(false)`；guarded source 禁止 pattern/multiple-topic
订阅、partitioned base 自动展开和 topic auto-create。每次成功
`SUBSCRIBE` 都必须返回与请求完全匹配的 attestation 以及非零
`connectionGeneration`；重连只能重新发送同一个 guarded `SUBSCRIBE`，不能
退回普通 Consumer。source adapter 在 Broker timestamp、position、guard 或
generation 证据缺失/漂移时 fail closed，并且只在同步 ACK 成功后释放下一条
record。

旧自定义实现通过 default method fail closed，不允许默认忽略。`ProducerBuilderImpl` 写入 `ProducerConfigurationData.topicResourceGuard`，clone 时 deep copy。

guarded producer 限制：

- persistent physical topic only；
- 禁止 auto-topic-create；
- V1 factory 固定 `batchingEnabled(false)`；
- V1 factory 固定 chunking disabled，并证明 `maxCommandBytes` 小于 exact Broker max-message bound；
- V1 factory 固定 `maxPendingMessages(1)`/nonblocking local admission；SDK 外层按 channel slots
  提供有界并行，不能让 Pulsar 内部队列隐藏第二个 unresolved SEND；
- 禁止 transaction send；
- partitioned base 由 Nereus factory 展开成 exact physical topic producer，不能让 client 自动换 partition。

### 10.3 PulsarApi wire

`PulsarApi.proto` 新增 protocol `v22` 和兼容 optional fields：

```proto
message TopicResourceGuard {
  required uint32 guard_version = 1;
  required string authenticated_cluster_id = 2;
  required bytes resource_incarnation = 3;
  required uint64 topic_creation_timestamp = 4;
}

message TopicResourceGuardAttestation {
  required uint32 guard_version = 1;
  required string authenticated_cluster_id = 2;
  required bytes resource_incarnation = 3;
  required uint64 topic_creation_timestamp = 4;
  required string physical_topic = 5;
  required uint32 partition = 6;
}

message TopicResourceGuardReceipt {
  required TopicResourceGuardAttestation attestation = 1;
  required uint64 broker_entry_timestamp = 2;
}

message CommandProducer {
  // existing fields 1..13
  optional TopicResourceGuard resource_guard = 14;
}

message CommandProducerSuccess {
  // existing fields 1..6
  optional TopicResourceGuardAttestation resource_guard_attestation = 7;
}

message CommandSubscribe {
  // existing fields 1..19
  optional TopicResourceGuard resource_guard = 20;
}

message CommandSuccess {
  // existing fields 1..2
  optional TopicResourceGuardAttestation resource_guard_attestation = 3;
  optional uint64 connection_generation = 4;
}

message CommandSendReceipt {
  // existing fields 1..4
  optional TopicResourceGuardReceipt resource_guard_receipt = 5;
}
```

`ProtocolVersion` 追加 `v22 = 22`；`ServerError` 在现有
`ProducerFenced = 25` 后追加唯一新值 `ResourceIncarnationMismatch = 26`。Producer create
不匹配通过同 request id 的 `CommandError` 返回该值；per-SEND 不匹配通过
`CommandSendError` 返回。新 client 连接旧 Broker 时在
`ProducerImpl.connectionOpened` 看到 remote protocol `< v22`，在发送 Producer command/SEND
前返回 `UnsupportedVersionException`；不得建立无 guard 的 Producer。

wire plumbing 同一提交必须修改：

```text
pulsar-common/.../PulsarApi.proto
pulsar-common/.../protocol/Commands.java
  peerSupportsTopicResourceGuard(peerVersion >= v22)
  newProducer / newProducerSuccess / newSendReceipt overloads
pulsar-client-api/.../TopicResourceGuard.java
pulsar-client-api/.../TopicResourceGuardAttestation.java
pulsar-client-api/.../TopicResourceGuardResponseEvidence.java
pulsar-client-api/.../GuardedMessageId.java
pulsar-client-api/.../ProducerBuilder.java
pulsar-client-api/.../PulsarClientException.java
pulsar-client/.../impl/conf/ProducerConfigurationData.java
pulsar-client/.../impl/ProducerBuilderImpl.java
pulsar-broker/.../service/PulsarCommandSender.java
pulsar-broker/.../service/PulsarCommandSenderImpl.java
pulsar-broker/.../service/ServerCnx.java
pulsar-broker/.../service/Producer.java
pulsar-broker/.../service/persistent/PersistentTopic.java
pulsar-broker/.../admin/impl/PersistentTopicsBase.java
pulsar-client/.../impl/ClientCnx.java
pulsar-client/.../impl/ProducerImpl.java
pulsar-proxy/.../server/DirectProxyHandler.java（协议协商/透传验证）
```

P1 以稳定的 `org.apache.pulsar.client.api` 为实现面；实验中的
`pulsar-client-api-v5`/`pulsar-client-v5` 不在本切片伪造同名 capability，也不能被 Nereus
factory 选用。若未来要从 v5 facade 暴露 guard，必须增加独立 delegate/parity tests，不能以
配置 map 偷渡后由实现静默忽略。

旧 overload 继续生成 absent guard fields，保持普通 client 行为。guarded overload 必须要求
v22 和 non-null exact values，不能把 absent 当作“让 Broker 自己查当前 topic”。若部署经过
Pulsar Proxy，Proxy 的前后端协议协商与转发也必须保留全部 v22 fields；未认证的旧 Proxy
等同旧 Broker，Route 不得激活。
`ProducerImpl.connectionOpened` 中 guarded branch 在收到 `CommandProducerSuccess` 后先要求
field 7 存在并与 expected guard/actual physical topic/partition 逐字段匹配，然后才可
`changeToReadyState`/`resendMessages`。缺失或 mismatch 关闭该 connection generation，不能把
已 pending op 送出。Producer-create 的 `CommandError(ResourceIncarnationMismatch)` 由
`ClientCnx.getPulsarClientException` 映射为 typed `TopicResourceGuardException`，且因尚未有 SEND
只能是 local/pre-SEND definitive；不从 error message 分类。

### 10.4 资源属性

每个 actual physical persistent topic 的 ManagedLedger properties 必须包含：

```text
nereus.resource.guard.version = "1"
nereus.resource.incarnation  = unpadded-base64url(32 random bytes)
nereus.resource.created-at   = unsigned decimal uint64
```

5.0.0-M1 没有可直接用于此契约的 durable native topic creation timestamp，因此 `created-at` 与 random token 一起由 Nereus Resource Controller 在创建/注册 physical topic 时写入并保护。删除重建必须生成新 token 和新 created-at；不得从旧 topic 复制。base partition metadata 的属性不能替代每个 physical topic 的 ManagedLedger properties。
attestation 的 partition 使用一个固定 normalization：actual partitioned physical topic
取 `TopicName.getPartitionIndex()`，non-partitioned persistent topic 取 `0`。guarded Nereus
factory 不允许对 partitioned base name 自动选 partition；它必须先展开到 exact physical
topic。因此 attestation 中不出现 Java/Pulsar 的 `-1` partition sentinel。

`ManagedLedger.getProperties()` 在该基线不是一个可供 SEND 热路径无锁组合读取的
多字段快照；因此 Broker 不在每条 SEND 里重复查三个 String map entry。
`PersistentTopic` 新增 `AtomicReference<CurrentTopicResourceGuardView>`，view 是由已加载
ManagedLedger properties strict parse 出的 immutable tuple，含一个显式 `INVALID/VALID`
state。`Producer.checkAndStartPublish` 每次只做一次 atomic read 和 allocation-free content
compare；该 view 是“current-property comparison”的唯一热路径投影，不是创建
Producer 时永不刷新的 expected cache。

guard 属性只能经 Resource Controller 的受保护操作更新：开始更新时先在
topic ordered executor 把 view 设为 `INVALID(UPDATE_IN_PROGRESS)`；用一次
`asyncSetProperties` 持久化完整 tuple；success callback 从 callback 的完整 map 重新
strict parse 并一次 atomic publish；failure/malformed 保持 INVALID、关闭现有 guarded
Producer 并报警，不回退到旧 view。`PersistentTopicsBase` 的通用 property API 对
`nereus.resource.*` key 只允许该 principal/path，不允许普通 admin 绕过 publish hook。
incarnation 不原地“轮换”；继续业务的 replacement 必须 delete/recreate 并使用
新 Route/Profile。这一设计同时避免了 map 并发可见性和三字段半更新窗口。

### 10.5 Broker validation

新增建议类：

```text
pulsar-broker/src/main/java/org/apache/pulsar/broker/service/TopicResourceGuardValidator.java
pulsar-broker/src/main/java/org/apache/pulsar/broker/service/ValidatedTopicResourceGuard.java
```

Producer 创建：

```text
ServerCnx.handleProducer
  -> load actual Topic
  -> validate protocol/version/cluster/principal/topic kind
  -> atomically read one actual PersistentTopic.currentTopicResourceGuardView
  -> compare token + created-at
  -> construct Producer with ValidatedTopicResourceGuard
  -> addProducer
  -> echo exact attestation in CommandProducerSuccess
```

每次 SEND：

```java
if (!producer.validateResourceGuardNow()) {
    cnx.execute(() -> {
        cnx.getCommandSender().sendSendError(producerId, sequenceId,
                ServerError.ResourceIncarnationMismatch, boundedReason);
        cnx.completedSendOperation(isNonPersistentTopic, headersAndPayload.readableBytes());
    });
    return false; // before startPublishOperation and before topic.publishMessage
}
```

校验放在 `Producer.checkAndStartPublish` 的最前部，发生在
`startPublishOperation`/`topic.publishMessage` 之前并覆盖普通和 batch send。此时
`ServerCnx` 已增加的 connection pending-send counter 必须通过
`completedSendOperation(...)` 成对回滚，不得泄漏 admission。transaction path 则在
`ServerCnx.handleSend` 的 `increasePendingSendRequestsAndPublishBytes` 和
`publishTxnMessage` 之前显式拒绝 guarded producer，不能绕过。actual comparison 只读一个
已发布的 `CurrentTopicResourceGuardView`；Producer 创建与每条 SEND 都不得重新组合
ManagedLedger properties，也不做 metadata-store/Oxia I/O。

token 是身份而不是 secret，hot path 使用 canonical String/byte equality即可；不允许每次 Base64 decode/分配。Producer 创建时保存 expected canonical form，PersistentTopic 暴露上述 allocation-free current atomic view。性能基准必须测 atomic read/compare，但不能用缓存到 Producer 后永不复查，否则属性撤销/重建不能 fence。

### 10.6 Receipt 与 client correlation

`Producer.MessagePublishContext` 已持有 ledgerId/entryId，并声明了 `entryTimestamp` 字段，
但 `5.0.0-M1` 的该 context 没有覆写 `setMetadataFromEntryData`，所以不能把当前默认值当作
Broker time。patch 必须新增：

```java
@Override
public void setMetadataFromEntryData(ByteBuf entryData) {
    entryTimestamp = Commands.peekBrokerEntryMetadataToLong(entryData, metadata ->
            metadata != null && metadata.hasBrokerTimestamp()
                    ? metadata.getBrokerTimestamp() : -1L);
}
```

两个 `MessagePublishContext.get(...)` factory 和 `recycle()` 都把 `entryTimestamp` 重置为
`-1L`；否则 Recycler 可能把上一条消息的时间误回显给下一条消息。

Route activation 要求 `AppendBrokerTimestampMetadataInterceptor` 在完整 Broker set 启用。
若 guarded success callback 仍得到 `< 0` timestamp，Broker 不得构造成功 guard receipt；它
关闭该连接/记录 invariant metric，使 client 保留 UNKNOWN，因为 record 此时可能已经持久化。
正常成功 callback 构造 `TopicResourceGuardReceipt` 并通过
`PulsarCommandSender.sendSendReceiptResponse` 返回；
ledger/entry 继续使用现有 `CommandSendReceipt.message_id`，guard receipt 不复制 MessageId。
Client：

```text
ClientCnx.handleSendReceipt
  -> require guarded receipt when producer has guard
  -> exact compare expected guard + actual physical topic/partition
  -> reject Pulsar duplicate sentinel ledgerId=-1/entryId=-1
  -> require nonnegative brokerEntryTimestamp; preserve other u64 raw ID bit patterns
  -> ProducerImpl.ackReceived(..., guardReceipt, connectionGeneration)
  -> complete send with GuardedMessageId
```

Guarded validation发生在 `ProducerImpl.ackReceived` 的 synchronized pending-head section 内，且在
`pendingMessages.remove()`/semaphore release/callback 之前；sequence、highest sequence、producer、
connection generation、attestation、MessageId 和 timestamp 任一不匹配，都保留 pending op、标记
该 connection generation ambiguous 并关闭连接。guarded producer batching/chunking 已关闭，
`OpSendMsg.setMessageId` 为该单条消息安装 `GuardedMessageIdImpl`；ordinary producer 仍安装现有
`MessageIdImpl/BatchMessageIdImpl`。

缺少/畸形/mismatch receipt 不是 success：关闭当前 connection，给 exact pending op 单调标记
ambiguity/integrity，但不调用 success callback；后续只有 validated receipt 可收敛 success，send
timeout/close 则向 transport 暴露 UNKNOWN。重连后旧 generation 的回调不能完成新 generation
的 pending op。Pulsar dedup 的 duplicate acknowledgement 在该基线可能返回
`ledgerId=-1/entryId=-1`；它不足以构造 Nereus `SafeBrokerAckV1`，因此 guarded transport 保持
UNKNOWN，不能伪造 Source Position。要把该分支提升为 queued，必须另行 source-lock Broker
返回 original position 的协议与持久映射证据。

`ClientCnx.handleSendError(ResourceIncarnationMismatch)` 调用：

```java
ProducerImpl.recoverResourceIncarnationMismatch(sequenceId, errorMessage)
```

它移除并完成 exact pending op，fence producer，禁止自动连接到同名 replacement。只有该 op 从未经历 response loss/disconnect 时，异常才带 `definitelyNotPersisted=true`；任何此前已写 socket但 response 丢失的 op保持 UNKNOWN。

`OpSendMsg` 增加 guarded-only `lastWrittenConnectionGeneration` 和
`ambiguousPriorAttemptObserved`，两个字段都在 Recycler `initialize/recycle` 清零。
generation 直接使用当前 `ConnectionHandler.switchClientCnx`/
`ConnectionHandler.getEpoch()` 的 raw `long` epoch，不另用 wall clock 伪造。immediate
path 的 `processOpSendMsg` 把该 epoch 传入 `WriteInEventLoopCallback.create`；reconnect
path 的 `recoverProcessOpSendMsgFrom(..., expectedEpoch)` 使用已有 `expectedEpoch`。
两个写路径都在当次 checksum/schema repopulation 完成之后、`write`/
`writeAndFlush` 之前调用同一个 `prepareGuardedWrite(op, cnx, epoch)`，记录
generation 并对真正将写出的 `op.cmd` 重算 request digest。connection
close/reconnect 在重发前，对该 generation 已写且仍 pending 的 op 单调设置
`ambiguousPriorAttemptObserved=true`。当前同一 generation 的 correlated typed rejection本身
不是“prior ambiguity”；但旧 generation 先断连、随后重发收到 mismatch 时必须返回 UNKNOWN。
只看 `retryCount/firstSentAt` 不够，因为得到正常 typed rejection之前本来就必须写一次请求。

`sendCommandSha256` 在 `WriteInEventLoopCallback` 所属写路径、修改 reader index 之前覆盖该次
actual outbound SEND command + metadata/payload composite buffer 的全部 readable bytes；使用
ByteBuf NIO segments 增量 hash，不复制 payload。`authenticatedResponseCommandSha256` 在
`ClientCnx` 对 v22 `CommandSendReceipt`/`CommandSendError` 完成 strict field/presence 校验后，覆盖
该 negotiated v22 LightProto command 的 canonical `toByteArray()`；v22 client 不接受更高版本
未知 guarded response field。两者与 connection generation/producer/sequence 一起 set-once 到
exact `OpSendMsg`，reconnect 后先标记 ambiguity，再用新 generation 的 request digest
替换 current-attempt evidence，不能沿用上一 generation digest。
`ClientCnx.handleSendReceipt` 和新增的 `ResourceIncarnationMismatch` error case 把
canonical response digest 与 `this` connection 一起传给 `ProducerImpl`；`ProducerImpl`
在 synchronized pending-head section 中要求 `cnx == current cnx`、
`connectionHandler.getEpoch() == op.lastWrittenConnectionGeneration`，然后才移除 op、
释放 semaphore 和完成 callback。

`delay-client-pulsar` 把这些字段编码成 Registry §6.3
`PulsarGuardedRequestEvidenceV1` / `PulsarGuardedResponseEvidenceV1`，并分别交给
`PulsarSendResult`。现有单一 arbitrary `evidence` bytes 不再同时冒充 Broker request 与 response；
缺 digest、digest 与 exact request/response 不匹配或 Recycler 残留一律是 integrity/UNKNOWN。

### 10.7 Pulsar 测试

```text
pulsar-common CommandsTest
  v22 guard/producer-success/send-receipt round trip
  old field compatibility

ProducerBuilderImplTest
  guard deep copy/validation
  guarded producer forces supported settings
  highBitCreationIdentityRoundTripsAsUnsignedProperty

ProducerImplTest
  old broker fails before SEND
  missing/mismatched receipt is not success
  duplicateReceiptWithoutOriginalPositionRemainsUnknown
  reconnect keeps exact guard
  prior ambiguous attempt remains unknown
  sameGenerationTypedMismatchIsDefinitive
  recyclerClearsConnectionGenerationAndAmbiguity
  requestAndResponseDigestsBindTheExactConnectionGeneration
  recyclerCannotReusePriorRequestOrReceiptDigest

ServerCnxTest / persistent PersistentTopicTest
  create producer rejects cluster/token/created-at mismatch
  every SEND rechecks actual ManagedLedger property
  guardedPropertyUpdatePublishesOneAtomicView
  updateInProgressAndFailedUpdateRemainFailClosed
  halfUpdatedGuardTupleIsNeverVisibleToSend
  transaction cannot bypass guard
  guard rejection occurs before startPublishOperation/publishMessage
  guard rejection balances connection and producer pending counters
  success echoes ledger/entry/broker timestamp/resource identity
  missing broker timestamp after persistence never fabricates success evidence
  recycled publish context cannot reuse a prior entry timestamp

AdminApi2Test / AuthorizationProducerConsumerTest
  only resource controller principal may mutate guard properties

ProxyConnectionTest / proxy integration
  v22 fields survive direct proxy in both directions
  proxy/backend protocol downgrade fails guarded create before SEND
```

集成 cut：

- unload、Broker failover、bundle transfer 不改变 properties，old guarded Producer reconnect 后成功；
- delete + recreate 同名 topic 生成新 identity，old Producer 的 SEND 被 typed pre-persistence error 拒绝；
- 移除某 Broker 的 v22/guard capability 时 Route activation 失败；
- 任何旧 client/无 guard Producer 不具备 Nereus production channel capability。

## 11. Nereus 生产 Transport

### 11.1 Kafka

建议文件：

```text
delay-client-kafka/.../ProductionKafkaProduceTransport.java
delay-client-kafka/.../KafkaCommandTransportFactory.java
delay-client-kafka/.../KafkaGuardedOutcomeClassifier.java
```

构造配置：

```properties
acks=all
enable.idempotence=true
allow.auto.create.topics=false
security.protocol=SASL_SSL
ssl.endpoint.identification.algorithm=https
key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
```

Command Topic/Route activation 另要求 topic `message.timestamp.type=LogAppendTime` 并逐 Broker
probe；`CreateTime` 或 response `logAppendTime=-1` 不具备 queued receipt authority。并强制无
`transactional.id`。`PLAINTEXT`/`SASL_PLAINTEXT` 不能建立主设计要求的 TLS endpoint identity，
不得把 metadata `clusterId` 宣称为 authenticated。使用双向 TLS 且不需要 SASL 的部署可显式
选择 `SSL`；两种模式都必须验证 endpoint/principal。Factory 从 `RouteSnapshotV1` 创建
`ProducerResourceGuard`，调用
`sendGuarded`。映射：

| Kafka result | `KafkaProduceResult` |
| --- | --- |
| exact success + evidence | `PERSISTED/OK` |
| local guard/capability failure before accumulator ownership | local `DEFINITIVELY_NOT_PERSISTED/BROKER_RESOURCE_UNCERTIFIED`; no Broker proof |
| registered authenticated exact-ID Broker rejection, no prior ambiguity | `DEFINITIVELY_NOT_PERSISTED/BROKER_DEFINITIVE_NOT_PERSISTED` + exact request/response proof |
| timeout/disconnect/callback loss/prior ambiguous | `UNKNOWN/ENQUEUE_RESULT_UNCERTAIN` |
| malformed success/evidence missing | `UNKNOWN/INTEGRITY_ERROR` |

`ResourceGuardException.definitelyNotPersisted()` 只是一阶 side-effect 结论；classifier 还必须按
`reason/responseEvidence` 区分 local proof 与 authenticated Broker proof。缺少后者时不能构造
`KAFKA_DEFINITIVE_REJECTION`。

2026-08-15 implementation evidence: the Delay worktree's opt-in
`realKafka` source set contains `KafkaClientArtifactProduceTransport`. It
constructs the exact K1 `ProducerResourceGuard` from the request identity,
calls only `GuardedProducer.sendGuarded`, validates response cluster/topic/
TopicId/partition/error/offset/evidence fields, and treats a missing
`logAppendTimeMs` (`-1`) as `UNKNOWN` because it cannot certify a queued
receipt. `KafkaClientArtifactSmoke` runs this binding against the source-built
K1 broker image; its Docker harness covers three-replica produce,
delete/recreate old-TopicId rejection and broker-1 failover. The binding is
not a K2 target-plus-receipt transaction and is not enabled in the normal
Gradle source set. The separate K2 binding is described in Phase K2 above and
is also opt-in; neither binding silently falls back to a stock/name-only
producer.

现有 result 类的 exact 修改形状为（其余 identity/position/stable-code 字段保持）：

```java
public record KafkaProduceResult(
        Disposition disposition,
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        int partition,
        long offset,
        Integer leaderEpoch,
        long brokerLogAppendTimeEpochMs,
        int stableCode,
        byte[] requestEvidenceBytes,
        byte[] responseEvidenceBytes) {

    // Both accessors defensive-copy. Factories enforce the presence matrix below.
}
```

`PERSISTED` 要求 identity/position/time 和两个 evidence 都存在；Broker-certified
`DEFINITIVELY_NOT_PERSISTED` 要求两个 evidence；local-before-ownership definitive 要求
两者都 absent；`UNKNOWN` 可保留已认证的单边 evidence 供审计，但不因单边
存在升格。constructor/factory 不接受第三种 arbitrary evidence 字段。

本切片同时把原 `KafkaProduceResult.evidence` 改为两个 defensive-copy 字段
`requestEvidenceBytes` / `responseEvidenceBytes`，内容分别是 Registry §6.3 的 canonical helper
message。`PinnedKafkaCommandIngress` 校验两个 domain hash、prepared frame value hash、resource/
partition/offset/time 后再构造 ACK/proof；`WireIngressOutcomeSupport.brokerDefinite` 的新签名接收
两个 evidence，不能再以 `request.frame()` 代替 Broker request。Producer interceptor 必须为空，
并使用 exact byte-array serializers；否则 post-interceptor value hash 无法等于 NDL1 frame hash，
Route factory 启动失败。

### 11.2 Pulsar

建议文件：

```text
delay-client-pulsar/.../ProductionPulsarSendTransport.java
delay-client-pulsar/.../PulsarCommandTransportFactory.java
delay-client-pulsar/.../PulsarGuardedOutcomeClassifier.java
```

Factory 使用 exact physical topic，设置 `TopicResourceGuard`、BYTES schema、batching false、auto update partitions false。成功必须拿到 `GuardedMessageId` 并完整匹配 RouteSnapshot。映射：

Factory 同时要求 TLS hostname verification 和已认证 client principal；明文或仅凭 topic
property 的连接不能把 cluster ID/typed receipt 升格为 authenticated Broker evidence。
Factory 不安装 Producer interceptor，也不启用会改变 Command value 的 client-side transform；
exact NDL1 frame 以 `Schema.BYTES` 进入 SEND composite buffer。

| Pulsar result | `PulsarSendResult` |
| --- | --- |
| exact `GuardedMessageId` | `PERSISTED/OK` |
| unsupported guard before ownership | local definitive `BROKER_RESOURCE_UNCERTIFIED` |
| typed mismatch、exact response且无 prior ambiguity | `DEFINITIVELY_NOT_PERSISTED/BROKER_DEFINITIVE_NOT_PERSISTED` + exact request/response proof |
| connection loss/timeout/malformed receipt | `UNKNOWN/ENQUEUE_RESULT_UNCERTAIN` |

2026-08-15 implementation evidence: the Delay worktree's opt-in
`realPulsar` source set contains `PulsarClientArtifactProducerFactory` and
`PulsarClientArtifactSendTransport`. The factory uses `Schema.BYTES`, exact
physical topic, `TopicResourceGuard`, disabled batching/chunking and disabled
automatic partition updates. The transport validates the request identity,
requires `GuardedMessageId` plus matching guard/topic/partition and
`MessageIdAdv`, and encodes the typed success/error evidence without payload
or credential bytes. `PulsarClientArtifactBindingSmoke` covers persisted
success, incarnation mismatch and typed code 26 rejection. This is a P1
artifact/API binding smoke; it does not claim D3 source/ACK integration or
multi-broker unload/reconnect evidence.

Commit `62ea85e8` adds `PulsarClientArtifactRealServiceSmoke` and
`e2e/run-pulsar-real-client-e2e.sh`. Against a server image built from the
locked P1 distribution, with broker timestamp/index metadata enabled and
exposed, the real client first returns evidenced `PERSISTED`, then after a
closed-producer delete/recreate rejects old-guard producer creation with
typed `TopicResourceGuardException(definitelyNotPersisted=true)`, and finally
returns evidenced `PERSISTED` for the replacement guard. The run used P1
`7eebd41d5b0917a0dfe5ea26ef3062a39f70a6d9`, distribution SHA-256
`d4b9e8aa6b44582c383262007217980793ec41bdf7fa3a1a4285e220407fef32`, image
ID `sha256:f377aeddd73913830a1004287e14eae910e739f39793a96fe41d38f2e5aca264`
and Compose project `nereus-delay-pulsar-e2e-1786737555-46201`. This is a
single-node P1 client/broker lifecycle cut only; a connected stale producer
SEND after forced deletion can lose response evidence and remains `UNKNOWN`,
so it is not promoted to definite failure. D3 source/ACK and unload,
multi-broker and proxy-reconnect evidence remain open.

现有 result 类对应改为：

```java
public record PulsarSendResult(
        Disposition disposition,
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        long physicalTopicCreationTimestamp,
        int partition,
        long ledgerId,
        long entryId,
        int batchIndex,
        int batchSize,
        boolean batched,
        long brokerEntryTimestampEpochMs,
        int stableCode,
        byte[] requestEvidenceBytes,
        byte[] responseEvidenceBytes) {

    // All binary fields/accessors defensive-copy; use content equality/hash.
}
```

presence matrix 与 Kafka 相同；Pulsar V1 production success 另外固定
`batched=false, batchIndex=0, batchSize=1`，不得因现有 result 支持 batch 就放宽
guarded channel。原 `PulsarSendResult.evidence` 同样拆为
`requestEvidenceBytes` / `responseEvidenceBytes`；
`PinnedPulsarCommandIngress` 与 native submission adapter 只接受 Registry §6.3 helper codecs 的
canonical bytes/domain hashes。Broker definitive proof 使用二者，queued/native ACK 使用已绑定
request hash的 response evidence；任意 diagnostic string/单 byte array 都不能填两个 proof field。

### 11.3 Source Adapter

本设计首先交付 writer。Worker source 仍必须满足主设计的 request-level source identity：

- Kafka Fetch v13 exact TopicId；
- Pulsar guarded SUBSCRIBE/connection generation；
- RocksDB sync WriteBatch 后才 ACK/commit；
- source identity mismatch 一条 record 都不能 apply/ACK。

当前 Delay worktree 的 `1bee5b45` 已提供 transport-neutral
`SourceRecordConsumer`/`WorkerSourceApplyLoop` composition：一个 bounded turn
最多 poll 一个 physical record，idle poll 返回可重试状态；exact record 在
queue rejection、apply failure、ACK unknown 期间保持不变，只有 broker ACK
明确返回 `ACKED` 才允许下一次 poll。该 callback 以原始 polled object 做
identity binding，并在 Store apply 已可见后才被调用。它是 Worker 接线和
本地 crash-boundary 的证据，不是 Kafka/Pulsar client artifact、Fetch/ACK
或 commit/rewind 的实测证据。

2026-08-15 implementation evidence: Delay commit
`412441c47cce4e61d3cc015b95c7d3cffcab2f7f` adds the opt-in
`KafkaClientArtifactSourceRecordConsumer`. It binds one Kafka partition to the
Worker-facing SPI, decodes NDL1/V1 bytes, and carries the exact authenticated
cluster/Topic UUID/offset/leader-epoch/LogAppendTime into `KafkaSourcePosition`.
Its acknowledgement callback calls Kafka `commitSync` and returns `ACKED` only
after that call returns; any runtime failure returns `UNKNOWN`. The accompanying
source smoke proves same-group replay of an unacknowledged record and no replay
after two committed offsets. The source reader intentionally uses Kafka's stock
Consumer API: no guarded Fetch or source-session authority is claimed here.

The final three-broker harness run also passed K1/K2 and survivor-broker K1
checks with Kafka source `8bd66fbb26eae1b0e4c5867e61f41900c3f5e318`, client
SHA-256 `4b6362d10146568c7ef78629ad678e50f164a750fdbb362ba0899dc49b815656`,
broker image `sha256:3116a80efc9d4a9399ca225c1de4288abde253659fd6fad2292af7727a2e9505`,
and clean project `nereus-delay-kafka-e2e-1786741055-82662`.
RocksDB/Worker apply, Oxia placement/session, dynamic IO admission, due/
publish/checkpoint/recovery and full D6 crash cuts remain open.

2026-08-15 implementation evidence: Delay commit
`a85a91d8dfd44e8d871673f9244356ba8356c062` adds
`PulsarClientArtifactSourceRecordConsumer` and the opt-in
`runRealPulsarSourceSmoke` task. It consumes only through P1
`GuardedConsumer<byte[]>`, checks exact guard/physical topic/attestation and
non-zero connection generation, decodes the exact `MessageIdAdv` position and
Broker entry timestamp, retains at most one record, and calls synchronous ACK
before returning `ACKED` or polling again. Commit `b5ce0fb8` records both
connection generations in the smoke output.

The source-locked P1 branch is
`nereus/delay-resource-guard-v1@f813c96687cc19e6fca1c82d3d161cf3e045c86b`,
based on `5.0.0-M1@8dae0236c0a0d405ed7f8303081080520fe91551`. Its guarded
`SUBSCRIBE` carries `TopicResourceGuard resource_guard = 20`; successful
`SUBSCRIBE` returns `TopicResourceGuardAttestation` and
`connection_generation`; `ConsumerImpl` validates the exact values and
`ServerCnx` validates the current broker guard before allocating the
generation. The three client artifact SHA-256 values are
`a636470f7d3f04af18980b84703a2b90f240a4bb58f77f8c19c1fd05b5bb40b2`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`.

The latest isolated Docker run used distribution SHA-256
`bfe0c479c60db1a7a56f4548bd821d218c4c284dceb7c112d92f425606adec37`, image
`sha256:735e2a6b952e2f7d4c8fc4c7a7b0d4ec2a852a9f4a9b21e82b076477cf19669f`,
project `nereus-delay-pulsar-e2e-1786743812-11877`, and ports `19827,19828`.
The writer returned `initial=PERSISTED,
stale=DEFINITIVELY_NOT_PERSISTED, replacement=PERSISTED`. The source replayed
unacknowledged position `11/0` across generations `1` and `2`, delivered
`11/1`, and observed an empty poll after synchronous ACKs. This is a
single-node guarded source SUBSCRIBE/replay/ACK cut; unload, multi-broker
failover, proxy compatibility, source session/rewind, full D3 integration and
Worker production E2E remain open.

Writer E2E 不能被误报为完整 Worker production E2E。

2026-08-15 implementation evidence: commit
`decb965e3991264ac243eb68c62ba0827759e616` adds `WorkerShardRuntime`, which
composes the source loop, owner drain coordinator and shared runtime
admission gate. It rejects drain while an exact source record has an
unconfirmed ACK, pauses source turns at the drain callback boundary, retains
checkpoint retries, and closes the source only after Store close and exact
lease release. The full local `check` passed at the commit. This is a local
Worker lifecycle seam; real Kafka/Pulsar consumers, Oxia placement/session
authority, due/publish/checkpoint/recovery wiring and real Broker ACK/rewind
remain open.

Commit `5c53f866` adds source-set Worker composition for the real native
consumers. `KafkaClientArtifactWorkerSourceFactory` verifies the exact accepted
assignment and active Owner state, then seeks the assigned Kafka partition to
the signed Route barrier's exclusive offset before constructing the common
runtime. `PulsarClientArtifactWorkerSourceFactory` verifies the guarded
consumer's current TopicResourceGuard, physical topic, partition, attestation
digest and connection generation against the activation barrier before the
runtime is exposed. Both close the native source on failed composition. The
locked K1 and P1 `compileReal*` gates pass. This does not itself publish a
source assignment, perform Owner Lease CAS, run catch-up, or establish the
full source/apply/ACK/recovery production vertical.

Commit `bbbc3160a6674b04a90b48a1f00865c079313bc7` adds native replay inputs
for the recovery side of this boundary. `KafkaClientArtifactRecoverySourceCursor`
requires the exact Kafka activation barrier and caller-supplied durable start
offset, assigns/seeks one partition, and never commits a group offset.
`PulsarClientArtifactRecoverySourceCursor` requires the exact guarded
SUBSCRIBE proof and activation barrier, validates the proof before and after
receive, and leaves positioning at the durable recovery cursor to the
Owner/Store composition. Both retain one decoded entry until `next()` is
called after the Store apply decision; neither ACKs a recovery message. The
locked Kafka and Pulsar source smokes exercise exact first/second replay and
look-ahead retention, and the latest Docker reruns pass the source/K1/K2 and
guarded writer/source cuts. This is native replay-input evidence only; it does
not prove assignment publication, Owner Lease CAS, Store-positioned Pulsar
recovery, RocksDB apply, or the full D6 Worker vertical.

2026-08-15 implementation evidence: Delay commit `72d4accf` adds
`PulsarClientArtifactRecoverySourcePositioner`. It validates the current P1
guard proof, maps the durable `PulsarSourcePosition` to a native MessageId,
performs the seek, waits for two stable post-seek proofs after the client
rotates its guarded SUBSCRIBE generation, and requires the caller to build the
activation barrier from that proof before constructing the no-ACK recovery
cursor. Separate physical route topics deliberately use native partition
index `-1`; the logical `ShardId.partition` remains independently validated.

The real E2E exposed and closed a P1 client edge: `resourceGuard` keeps the
source receiver queue at one, and a non-batch seek target filtered before
application must replenish its permit. P1 commit `358ce4a103` implements that
return path. The focused P1 client test, distribution assembly, Delay
`compileRealPulsar`, and full Delay `check` passed. The latest isolated run
used P1 distribution `7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`,
client `57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
image `sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
project `nereus-delay-pulsar-e2e-1786753971-20261`, and ports `19811,19812`.
It reported `skipped=11/0`, returned the second command, and completed the
guarded source ACK smoke with source generations `5` and `6`. This closes the
native positioning sub-boundary, not durable Store cursor selection, Owner
assignment/Lease CAS, RocksDB apply, activation or the complete D6 vertical.

2026-08-15 implementation evidence: commit
`3d0bf7ea081ae7b652e3a0ca4b66003bc4b23618` adds an isolated Docker Compose
Oxia smoke harness. `./e2e/run-oxia-real-service.sh` built source
`37a17bef17202d5fd6e23282da5fd26d94865484`, started the uniquely named
project `nereus-delay-v1-oxia-e2e-1786729940-65321` on
`127.0.0.1:16649`, waited for in-container health, and passed the Owner,
Control, Recovery and Gateway audit real-service classes with
`BUILD SUCCESSFUL`; cleanup removed the matching container and network. This
is Dockerized Oxia authority/audit evidence only and remains separate from
Route activation/session reconnect, real Broker transport, Worker vertical,
HA and release gates.

2026-08-15 implementation evidence: commits
`becfb1a35fc05cbf7ae7c77816f91bd72e546566` and
`3d45dcd7bc457d0ab308b51b9dee4abf5de6adf4` add the real Oxia Route
publication/refresh smoke, dispatch notification refreshes to an owned
executor rather than the Oxia callback thread, and isolate the notification
stream on a separate client from the session-fenced Route client. The Docker
harness built source `37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-v1-oxia-e2e-1786732310-90387` on `127.0.0.1:16649`; eight
selected real-service methods passed with `BUILD SUCCESSFUL`. The Route tests
cover separate publisher/provider sessions, explicit refresh after signed
event/head revisions and notification-driven refresh. Session
timeout/reconnect, cache-staleness recovery, activation barrier, native
eligibility, real Broker transport and Worker vertical evidence remain open.

2026-08-15 implementation evidence: commit
`4a4cb9424ec731a59bb117028ae631557c907e2f` adds
`e2e/validate-cross-repo-contracts.sh`. The read-only audit passed after
checking clean isolated worktrees, locked Kafka/Pulsar ancestry and exact
heads, Oxia source `37a17bef17202d5fd6e23282da5fd26d94865484`, and the
cross-repository resource-identity, TopicId, v22 guard and result-evidence
symbols. The shared roots remain intentionally out of scope because they have
pre-existing user edits. This is source-lock/contract evidence only and does
not close the real Broker, Route activation/session, Worker vertical, HA or
release gates.

## 12. Worker 对接边界

Gateway/Direct SDK 产生的都是同一 NDL1 bytes，因此 Worker 不增加“gateway command”分支。apply 仍是：

```text
source record
  -> strict NDL1 decode
  -> physical Shard locator check
  -> commandId + commandHash dedupe
  -> deterministic Profile/Route resolution
  -> one synchronous RocksDB WriteBatch
       message / reservation
       dedupe
       V1ScheduleBinding
       timeline/lane
       result
       source position
  -> durable
  -> ACK source
```

本地 `SourceApplyCoordinator`/`WorkerSourceApplyLoop` 只闭合上述流程的
transport-neutral handoff；真实 source adapter 仍必须把 native cursor 的
ACK/commit/rewind、Kafka TopicId 或 Pulsar connection-generation proof 绑定
到同一 exact record，并在其结果不确定时保留 Broker retry authority。

Recovery uses a separate no-ACK cursor composition. The Kafka recovery cursor
starts at a caller-validated durable offset and releases only its local
look-ahead after the coordinator proves Store application. The Pulsar recovery
cursor validates the guarded subscription proof on every receive and
`PulsarClientArtifactRecoverySourcePositioner` performs the seek from a
caller-validated durable position, waits for the new guarded generation, and
returns the proof used to build the activation barrier. Neither cursor ACKs a
recovery message; the Owner/Store recovery path still chooses the durable
position and the eventual ACK/rewind boundary. These native cursors therefore
cannot be read as proof of Owner assignment/session authority, recovery
catalog/Floor selection, RocksDB replay application, or activation CAS.

已有 Destination Lane、Publish Admission、UNCERTAIN、checkpoint/replay 逻辑保持权威。Chronos 的单 seek cursor、目标无限重试、墓碑-only cancel 和公开 RocksDB key 都不进入代码。

## 13. 配置、指标与审计

### 13.1 配置

所有数值无安全默认，发布时显式提供：

```text
routeSnapshotMaxStaleness
routeWatchGapTimeout
clientMaxPendingCommands / clientMaxPendingBytes
transportMaxChannelsPerRoute
transportMaxPendingPerChannel
gatewayScheduleQps / gatewayControlQps / gatewayQueryQps
gatewayIdempotencyMaxRecords / maxBytes / maxRecordBytes
gatewayIdempotencyMaxAttemptsPerOperation / retention
gatewayTransportOutcomeWait
gatewayAttemptOwnershipMaxAge
gatewayMaxConcurrentAwaitStreams
guardedProducerCallMaxAge
transportCloseQuiescence
```

Pulsar V1 固定 batching false/one unresolved SEND，不作为可调开关。Kafka v13 minimum、auto-create false、transactional.id absent 也不是可降级配置。

所有 count/bytes/timeout/retention 必须为正且 checked arithmetic。
`uncertaintyAt = checkedAdd(startedAt, gatewayTransportOutcomeWait)`；`retainUntil` 必须覆盖
operation retryUntil、所有 attempt uncertainty deadline、允许的 explicit retry window 和
audit export grace。`ownershipNotAfter = checkedAdd(startedAt, gatewayAttemptOwnershipMaxAge)`
且必须 `ownershipNotAfter <= uncertaintyAt`。任何不满足 Oxia max-value/request 或该时间
包含关系的配置启动失败。

### 13.2 指标

至少按低基数 route/profile/adapter/stable-code 输出：

```text
delay_prepare_total
delay_prepare_rejected_total
delay_enqueue_outcome_total{queued,definite,uncertain}
route_snapshot_age_ms
route_snapshot_watch_gap_total
transport_guard_mismatch_total
transport_guard_receipt_integrity_total
transport_pending_commands / transport_pending_bytes
gateway_idempotency_hit_total
gateway_idempotency_conflict_total
gateway_uncertain_observation_total{applied,rejected,unresolved,error}
gateway_control_reserve_rejection_total
```

禁止把 raw tenant、topic、message ID、idempotency key、credential reference 放进 metric label。

### 13.3 审计

Gateway audit 记录 safe digest：authenticated principal hash、tenant scope hash、RPC kind、idempotency key hash、prepared ref/hash、physical attempt ID、outcome kind、stable code、route incarnation。不得记录 payload、secret reference、JWT、完整 arbitrary headers。

## 14. 验证矩阵

### 14.1 Semantic Core 同源

同一输入 snapshot/request 固定 clock/random seed：

```text
Direct SDK prepared bytes == Gateway prepared bytes
commandId/delayMessageId/shard/commandHash 全部 byte-equal
same invalid input -> same StableErrorV1
same AUTO_FAST snapshot -> same frozen branch
same Registry §6.6 bytes -> same RouteSnapshot fields/digest/signature decision
unknown field/noncanonical order/high-bit u64/signature/tenant mismatch -> same fail-closed result
```

### 14.2 双入口互操作

- Direct Schedule -> Gateway Cancel/Query；
- Gateway Schedule -> Direct Reschedule/Query；
- 同一 idempotency key + 同 body 返回同一 prepared ref；
- 同 key + 不同 body 在任何 Broker I/O 前冲突；
- Gateway crash 在 idempotency `PREPARED`、CAS 到 `ACTIVE`、transport ownership、
  callback 与 outcome CAS 前后逐 cut 恢复；`STARTED` 到 deadline 只能变 uncertain；
- crash 留下 `PREPARED` 后只有一个 CAS winner 创建首 attempt；过期 prepared 不发送且不重新
  prepare；
- attempt CAS response loss 不生成 ownership permit，reread/另一实例都不能发送；permit
  到期与 local gate-age 超限不能进入 accumulator/Producer ownership；
- Direct/Gateway 都把同一 authenticated tenant context 传到 plan resolver；cross-tenant
  historical Route 在 permit transfer 前 fail closed；
- resolver/registry 抛出、transport 同步抛出/null-stage 在 permit 为 `AVAILABLE` 时产生
  local definite，为 `LIBRARY_OWNED` 时只产生 uncertain；projector 不能跨
  managed/native branch；
- managed uncertain 后 internal query 即使得到 exact CommandHash + applied Source Position，也只
  返回独立 applied/rejected observation；缺少原 `SafeBrokerAckV1` 时 aggregate 仍 uncertain，
  not-found/timeout/transition 也不得收敛 definite；
- 重复普通 RPC 不产生新 attempt；显式 `RetryUncertain` 的相同 retryRequestId 只产生一个新
  attempt，并逐字复用原 prepared bytes；
- 两个不同 retryRequestId 并发引用同一 expected prior 时至多一个赢得下一 attempt；loser 的旧
  precondition 不能在 winner 收敛后自动补发，必须返回最新 aggregate；
- Gateway unavailable 不影响已有 Direct SDK path。

### 14.3 Resource recreation

四个硬门：

1. Kafka AAA 删除重建 BBB，旧 PreparedCommand 不进入 BBB；
2. Kafka AAA leader failover，同 TopicId 允许 exact retry；
3. Pulsar unload/failover 不改变 guard，旧 producer 可重连；
4. Pulsar delete/recreate 改变 guard，旧 producer 每次 SEND 在 persistence 前拒绝。

### 14.4 Uncertainty

- socket write 后断连 -> UNKNOWN；
- callback registration failure -> UNKNOWN；
- guard mismatch before library ownership -> DEFINITELY_NOT_QUEUED；
- registered Kafka rejection/Pulsar typed guard rejection 且无 earlier ambiguous attempt -> DEFINITELY_NOT_QUEUED；
- earlier disconnect 后再收到 guard mismatch -> UNKNOWN；
- retry始终使用相同 PreparedCommand/route/shard/frame/hash。

### 14.5 性能

分别测：

- Direct SDK prepare 和 enqueue；
- Gateway auth + idempotency + enqueue；
- Kafka ordinary vs guarded Producer；
- Pulsar ordinary vs guarded SEND；
- route watch 扇出/内存；
- topic recreation guard hot path；
- bad destination Lane 与 Command ingestion 隔离。

性能结果不能放宽 correctness gate。Gateway 比 Direct 多一跳是显式权衡；Pulsar
每 SEND 的 atomic guard-view read/compare 是防误投成本；Kafka batch 只因 guard
identity 不同才额外分裂。

## 15. 实施切片与提交边界

### Phase D0：文档与 package seam

改动 `nereus-delay`：

- 本文、ADR 0043/0044、文档地图/审计/status；
- 登记目标 package/module dependency matrix；真实 package-level dependency test 在 D1
  第一个新 package 与它同提交，不对尚未存在的 module 伪造 PASS；
- 不改 NDL1/NDR1。

完成门：`checkDocumentation`、`git diff --check`、现有 full check；状态仍是 design accepted，production transport 未实现。

### Phase D1：Semantic Core / Route SPI

2026-08-14 progress evidence: Delay commits `532f8ad5`,
`402b27fa0dced95c2312bfedc0678af03463f2d5` and
`67ef3de3ab6f69ae992c3ccb70c7cb65cad47613` and
`c42405ce6c69aef8ae0f8a9a63158c917410309f`, `62a9438967112f96e65b8daa7b2b86d52a103b10`,
`e276bec3ffff7f5015367bed55f5b8d63c080e21` and
`69d89839e4e80326e5317a4f5066667e270a7136`,
`a06ab232a5608ec0e7c9152ef80fc72c06966e66`,
`1dc28eaf391429f2dc9221f416af968d36575dff`,
`5cc955e1306e1f54db06a06a2bb2b84f232c2a7b` and
`9695eba7ca384d99cd28ece238f6cbfe1bcd08be`,
`724fdad95971dd096e116056f8e5da1a7ba76d14` and
`44bffea6063ef68ce36f8fb49527ee00a9bfa36b` supply the local canonical
Route/resource value types, UUIDv7 identity seam, `ROUTING_HASH_V1`
calculator, zero-I/O `DefaultDelaySemanticCore`, fail-closed signed-cache
watch, exact historical-route plan
resolver, shared `DefaultSubmissionCoordinator`, explicit `DefaultDelayClient`,
guarded transport bridges, the in-memory Gateway Schedule/idempotency
composition, the Oxia event/head-CAS Route publisher/provider, generated
Java/gRPC Gateway API descriptors, Schedule/RetryUncertain/PrepareLargeSchedule/CommitLargeSchedule/Cancel/Reschedule handlers and receipt-bound upload handlers behind
the shared authenticated ingress, and transport result/attempt binding.
Focused deterministic tests and a full local `check` pass at
`5cc955e1306e1f54db06a06a2bb2b84f232c2a7b`; Route
authority focused checks pass at `62a94389`, and Gateway CAS focused checks at
`e276bec3`. This is not completion of D1/D4/D5: activation-barrier/session-fenced
real Oxia authority, native eligibility authority, the remaining Gateway RPC
handlers and bound authentication,
durable/HA idempotency, package/module split, final production Kafka/Pulsar
module integration beyond the opt-in source-bound bindings, Worker wiring and
real-service cuts remain open.

```text
SelfRoutingId.fromLogicalUuid
PreparedIdentityPlannerV1
RouteSnapshotV1 + verifier + local provider
AutoFastSubmissionPlannerV1 + trusted NativePreparationSnapshotProvider
DefaultDelaySemanticCore
DefaultSubmissionCoordinator + plan resolver + projector registry
DefaultDelayClient composition
package/module dependency test
Direct/Gateway byte-equivalence tests
```

完成门：全 deterministic vectors；prepare zero-I/O；Cancel/Reschedule original route cut；
coordinator/projector ownership tests；Direct/Gateway byte-equivalence vectors。

### Phase K1：Kafka generic guarded producer

在 Kafka `trunk` 基线独立分支实现 §9，不引入 Nereus package/name。

2026-08-15 progress evidence: Kafka worktree commit
`95d48e89e7e8a4e6d8718e44d424ffef8f17829f` on
`nereus/delay-guarded-producer-v1` implements the generic public guarded
producer surface and the first Sender/RecordAccumulator/ProducerBatch evidence
path. Focused tests pass for public preflight/future completion, v13 and exact
TopicId binding, guard-separated batches, leader retry with the same identity,
disconnect ambiguity, definitive `UNKNOWN_TOPIC_ID` and non-allowlisted
rejection. The same commit adds real KRaft integration coverage for delete/recreate
TopicId rejection and leader failover, and fixes the legal Kafka `-1`
`logAppendTimeMs` sentinel on successful `CreateTime` topics. Those real cuts
pass with the independent Gradle integration test task. The Delay opt-in
binding/E2E now records the K1 source SHA, client SHA-256, base image digest
and local broker image ID; complete release attestation, the Nereus D2
production module and K2 remain open. D2 must not use stock Producer as a
substitute.

完成门：focused client tests + delete/recreate/leader failover integration + source lock SHA/digest。未完成 Kafka patch 前 `ProductionKafkaProduceTransport` 不得使用 stock Producer 冒充。

### Phase D2：Kafka Nereus transport

`ProductionKafkaProduceTransport` 的严格配置/guard bridge 与现有 pinned outcome
mapping 已在 Delay 中建立 source-level composition seam。Commit
`3f76e836964d818360d5affc122515ccbac04717` 进一步在显式 `realKafka` source
set 接入锁定 K1 client artifact、TopicId/v13 response evidence 和
`KafkaClientArtifactSmoke`; 独立三 Broker Docker E2E 已覆盖
delete/recreate 与 leader failover。仍需将此 opt-in binding 接入最终生产
module、完成 K2 之外的 source/ACK vertical 和 Worker ACK-after-sync。

完成门：QUEUED/definite/uncertain 三态和 Worker ACK-after-sync crash cut。

### Phase D2-source：Kafka guarded Fetch/source handoff

The source side now has its own K1 contract; the guarded Producer contract is
not treated as evidence for reads. Kafka commit
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
descended from `trunk@c300006a7705c240642db6950b5a95fec982bfc5`, adds the
public `GuardedConsumer`, `ConsumerResourceGuard`, `GuardedConsumerRecords` and
`GuardedFetchEvidence` APIs. A bound consumer is restricted to one exact
cluster/topic/TopicId/partition identity. Its Fetch request must use v13 or
newer and the completed response proof records the request version,
correlation/broker/session identity, fetch and returned offset range, high
watermark, last stable offset and canonical response-body digest. The proof
is carried through both Classic and Async consumer Fetch paths; TopicId or
partition drift is a typed failure and never falls back to name-only metadata.

Delay commit `17b4d7e6` uses a source-set
`KafkaClientArtifactSourceConsumerFactory` that binds the immutable guard and
rejects a runtime without `GuardedConsumer`. The active source and recovery
cursor call `pollGuarded`, validate every returned record against the proof,
and use the same Java-UUID/Kafka-`Uuid` conversion as the existing Kafka
producer binding. The active source retains one in-flight record until
`commitSync` succeeds; recovery has no ACK/commit path. The Worker composition
factory checks the same guard before seeking its accepted activation barrier.

The focused Kafka client tests, Delay `check`, source-set compile/checkstyle,
and the three-broker K1/K2 Docker harness passed. The latest harness used
Kafka `05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
broker image
`sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`.
This closes opt-in source Fetch proof and ACK/replay handoff only. Fetch
response-loss, LSO/retention-floor recovery, assignment/session authority,
ACK-failure injection, Store apply and the full Worker vertical remain open.

### Phase K2：Kafka transactional guarded destination

在 Kafka 独立切片中为 target + receipt transaction 提供 exact TopicId、Produce v13+、transaction
coordinator/reenqueue identity 保持和双 topic response evidence。K1 的 `sendGuarded` 不自动满足
这个门。2026-08-15 progress evidence: Kafka
`nereus/delay-guarded-producer-v1@8bd66fbb26eae1b0e4c5867e61f41900c3f5e318`
adds the public `GuardedTransactionalProducer` API and requires an active
transaction, transaction-v2 capability and exact partition registration
before a guarded in-transaction send. Delay commit
`ca9134ec8a1922e68f76f69ae0aa9bdd6e7180d5` adds
`KafkaTransactionalDestinationAdapter` and the source-locked real client
transport. It journals the canonical receipt mapping before sending target
and receipt records through one guarded transaction; a generic publish call
without an exact prepared hash remains `UNKNOWN` rather than fabricating
evidence.

`LC_ALL=C LANG=C ./e2e/run-kafka-real-client-e2e.sh` passed against a clean
three-broker KRaft Compose project
`nereus-delay-kafka-e2e-1786739311-64581` on ports `19173,19174,19175`.
The source-built client jar SHA-256 was
`4b6362d10146568c7ef78629ad678e50f164a750fdbb362ba0899dc49b815656`, and
the temporary broker image was
`sha256:3116a80efc9d4a9399ca225c1de4288abde253659fd6fad2292af7727a2e9505`.
The smoke proves target-plus-receipt atomic commit, abort, exact target
payload and canonical receipt key/value reads, same-name target
delete/recreate rejection and replacement commit with `read_committed` checks.
It does not yet prove EndTxn response-loss classification, Fetch v13/LSO/
contiguous replay, retention-floor recovery or
independent target/receipt failover; the atomic-target-receipt profile remains
inactive.

当前完成门仍要求同事务两个资源的 delete/recreate/failover cuts、任一
identity 漂移不向 replacement 写入、commit/abort/response-loss 三态、
精确 `read_committed` Fetch/LSO/连续 cursor/retention evidence 与现有
Attempt 契约一致。当前切片只关闭了上述 opt-in commit/abort/target-fence
子集；K2 完成前 Kafka atomic-target-receipt Profile 必须保持不可激活。

### Phase P1：Pulsar first-class resource guard

从 `5.0.0-M1@8dae0236` 创建独立分支，实现 §10。

2026-08-15 progress evidence: Pulsar worktree branch
`nereus/delay-resource-guard-v1` is implemented in the earlier writer commits
`19c97bf836d521f0e6103c542819723e70ccdbab`,
`be226fe6c88634e9a94ba5c6a0f5859bc510cb66` and
`7eebd41d5b0917a0dfe5ea26ef3062a39f70a6d9`, followed by the current source
commit `f813c96687cc19e6fca1c82d3d161cf3e045c86b`. The v22 wire/API,
broker/client enforcement and guarded source `SUBSCRIBE` slices pass their
focused compilation/tests and affected-module checks with independent Gradle
user homes. The producer path snapshots and copies the guard before
asynchronous broker topic work; the source path carries the guard in
`CommandSubscribe`, validates the returned attestation/generation in
`ConsumerImpl`, and validates the current broker view in `ServerCnx` before
allocating a connection generation. A real in-process broker/client test
passes guarded SEND evidence and same-name delete/recreate; the Delay-side
single-node Docker cut additionally passes guarded source replay and
synchronous ACK handoff.

This remains an isolated upstream slice plus an opt-in Delay transport, not a
production release. The current writer/source cuts record the client artifact
SHA-256 values, real distribution/image digests and clean Compose teardown.
Unload, multi-broker failover, old-peer proxy compatibility, source session
ownership/rewind, complete artifact attestation, Direct SDK integration and
Worker production wiring remain required before the completion gate can pass;
D3 must not use an ordinary Pulsar producer as a substitute.

完成门：v22 wire compatibility、create+per-SEND guard、receipt echo、delete/recreate/unload/failover cut、focused modules格式检查。

### Phase D3：Pulsar Nereus transport

`ProductionPulsarSendTransport` 的严格配置/managed-native guard bridge 与现有
pinned outcome mapping 已在 Delay 中建立 source-level composition seam；commit
`3f76e836964d818360d5affc122515ccbac04717` 已在显式 `realPulsar` source set
接入真实 locked Pulsar v22 client artifact、`GuardedMessageId` 和 typed
response evidence，并通过 API/evidence smoke。Commit `62ea85e8` further
passes a real single-node Broker Docker delete/recreate cut with initial and
replacement evidenced sends plus typed stale-producer rejection. Commit
`a85a91d8dfd44e8d871673f9244356ba8356c062` adds the matching guarded source
binding; its single-node Docker smoke proves guarded SUBSCRIBE, replay across
connection generations, strict Broker timestamp/position evidence and
synchronous ACK handoff. 仍需完成 D3 的 unload/multi-broker/proxy reconnect、
Direct SDK E2E、source session/rewind authority 和 Worker production wiring。

完成门：exact GuardedMessageId、typed pre-persistence rejection、uncertainty persistence、source lock。

### Phase D4：Oxia Route authority

`InMemorySignedRouteSnapshotProvider` and the 2026-08-14 Delay commit
`62a94389` now supply local signed-cache/Oxia composition evidence. The
2026-08-15 commit `4f606fec86aaeb74472f6575e5ee7ddcb8dc8f82` adds the optional
`OxiaRouteAuthoritySession` path: an ephemeral marker is created with
`AsEphemeralRecord`, its Oxia session/client metadata is derived into a stable
session identity, and every delegated Route operation rereads the exact
marker/value/version before proceeding. Provider/publisher response loss is
accepted only after an exact marker reread.
`OxiaSignedRouteSnapshotPublisher` writes immutable canonical Route events and
advances an Oxia head with version CAS; `OxiaSignedRouteSnapshotProvider`
rebuilds only through the head, verifies snapshot/event canonical bytes and
signatures, replays contiguous revisions, refreshes from Oxia notifications,
and quarantines same-incarnation immutable drift. Exact lookup remains
tenant-scoped and preparation still reads only the local cache.

Explicit-refresh publication and notification-driven tenant-scoped cache
replay now have a live Oxia smoke. The notification stream uses a separate
client from the session-fenced Route read/write client, and refresh work is
dispatched off the Oxia callback thread. Commit `a71a0667` adds explicit
`reconnectSession()` recovery: a fenced publisher operation remains failed
closed, while provider `refresh()` can rotate the ephemeral marker and then
rebuild the cache. Local tests cover marker expiry and identity rotation;
live session-timeout/connection-loss and cache-staleness recovery cuts,
activation barrier publication and native eligibility authority remain
required。Follow-up commits `8e404a30` and `57d6dfd7` now close two local
sub-boundaries: the issuer-verified native eligibility cache/Core checks, and
the exact projection from a signed Route partition `ActivationBarrierV1` to a
Worker `SourceAssignment`. These do not create an Oxia/catalog issuer,
perform a live Broker capability probe, publish activation evidence, or prove
real source ownership/reconnect. Commit `f8ffaff9` additionally routes both
current-alias and exact-historical Worker assignment construction through the
tenant-authorized `RouteSnapshotProvider`; missing historical authorization
fails closed. This remains assignment projection, not assignment publication,
Owner Lease CAS or Broker source ownership. Commit `3bae4a6b` also supplies
the local issuance ordering boundary: signed Route/Profile/Binding/attestation
and guard evidence are checked before external native protection is required
and the snapshot is signed. Production Oxia protection, live Broker guard
reads, credential resolution and issuer key rotation remain external. 完成门：snapshot
signature/digest、lifecycle、route
expansion、credential binding、cache staleness cuts。
同一 Route Incarnation 的 resource/partition/hash/query-retention/size drift 必须 quarantine；仅
lifecycle/control-version/validity 与有等价证明的 credential generation 可发布新 snapshot。

### Phase D5：Gateway

本地 conformance slice 已提供 `GatewayScheduleRequestV1`、canonical body/key
hash、prepared-before-ownership、in-memory single-record CAS、one-shot Gateway
permit、outcome replay、body conflict、`RetryUncertain` expected-prior/retry-ID
CAS、`GatewayIdempotencyStore`、strict record decoders、Oxia single-record
version-CAS、source `delay_gateway.proto` 以及由 Gradle 生成的 Java/gRPC
stubs/service descriptor。`GatewayGrpcApiTest` 固定 eleven-RPC descriptor
surface。`GatewayIngressService` 要求 tenant authority、独立 schedule/retry/control
admission pool 和 digest-only audit；`GatewayGrpcService` implements all eleven
generated RPCs. Schedule/RetryUncertain/PrepareLargeSchedule/CommitLargeSchedule/
Cancel/Reschedule use the shared ingress directly; receipt-bound upload methods
are active only when an explicit `GatewayPayloadIngressService` is configured,
and query/await/message RPCs are active only when an explicit
`GatewayQueryIngressService` is configured; otherwise those optional paths
remain generated-base `UNIMPLEMENTED`.
Commit
`9695eba7ca384d99cd28ece238f6cbfe1bcd08be` also covers canonical control
body decoding and the shared idempotency/attempt path for Cancel and
Reschedule。Commit `724fdad95971dd096e116056f8e5da1a7ba76d14` adds the
same path for canonical large-payload reservation/proof preparation.
Commit `44bffea6063ef68ce36f8fb49527ee00a9bfa36b` adds canonical receipt/
opaque-handle decode and digest-audited payload ingress; real Object Store
credential/registration authority and remote durability remain open.
Commit `59d492041ac42b79a632ebddfb56a7608b2d7283` adds strict query locator
decode, tenant/admission/audit composition and bounded canonical query response
streaming through `GatewayQueryAuthority`; receipt-to-source/store binding and
deadline/retention authority remain the responsibility of that explicit
composition and are not established by the generated adapter.
Guarded Kafka/Pulsar result 还必须携带与 one-shot permit 相同的
`PhysicalEnqueueAttemptId`；缺失或错配在 coordinator/projector 边界
fail-closed 为 `INTEGRITY_ERROR`。
Response loss is fail-closed:
the durable store may replay an exact aggregate but does not reconstruct a
physical ownership permit。Commit `9a805f2ef879ce7e9c78168d4fff31a973f7c186`
adds `GatewayGrpcContext`, a mandatory-client-certificate Netty server factory,
and `MutualTlsJwtGatewayTenantAuthority`, which requires a Bearer token and
delegates signature/claim verification to an explicit `GatewayJwtVerifier`.
Delay commit `19099e2e` adds `RsaSha256GatewayJwtVerifier`, which supplies the
concrete RS256 policy: exact
issuer/audience/key-id, strict NumericDate bounds, signed non-zero tenant and
routing scope digests, duplicate-free canonical JSON, and mTLS leaf binding
through `cnf.x5t#S256`. `RsaSha256GatewayJwtVerifierTest` covers valid,
signature, policy, time, duplicate-member, canonical-base64 and certificate
negative vectors. Certificate issuance/rotation deployment evidence remains
external to this reusable boundary. Durable single-record quota/control reserve is now
implemented by `OxiaGatewayAdmissionController`, with a single-node Oxia
Docker smoke in `b6154072`, but HA/session-churn observability、Gateway HA/transactional durability、RetryUncertain late-evidence/aggregate、crash cuts 和
多语言最小 SDK。

完成门：双入口 byte equivalence、HA crash cuts、non-enumerating auth、control reserve、load test。

### Phase D6：完整 Worker production vertical

真实 Kafka/Pulsar source、ownership、RocksDB apply/ACK、due/Lane/publish/checkpoint/recovery。

`1bee5b45` 和 `decb965e` 完成 transport-neutral source-consumer/Worker
生命周期组合；`412441c4` 再完成了锁定 Kafka client 上的 source frame/
position/`commitSync`/restart-replay handoff 子集。`bbbc3160` 增加了不确认
Broker 记录的 Kafka/Pulsar native recovery cursor；后续 `72d4accf` 增加了
guarded Pulsar durable-position seek、post-seek generation proof 和新
barrier 投影，并由 P1 `358ce4a103` 修复 queue-size-one seek permit 返还。

Delay commit `c72cac90` now adds an opt-in real-Kafka Worker vertical:
`KafkaClientArtifactWorkerSmoke` uses the locked guarded Kafka Producer and
Consumer, recovers offset 0 through `OwnerRecoveryCoordinator`, activates the
owned shard at the exclusive barrier, applies offset 1 through
`WorkerShardRuntime` and RocksDB `WriteBatch`, and calls native `commitSync`
only after the apply. The smoke checks exact Fetch v13 evidence, source
position, group offset and owner-lease release on drain. The 2026-08-15
three-broker Docker run passed with Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786757667-58603`, and ports
`19195,19196,19197`. Its evidence lines were:

```text
Kafka source ACK smoke passed: topicId=ay9r1XMxQUycBBCYwxqqvg, firstOffset=0, secondOffset=1, committedAfterRestart=empty
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch and commitSync ACK
```

This closes the real Kafka guarded Fetch → recovery → local RocksDB apply →
ACK cut for one partition. Commit `a7fd5fa7` adds an opt-in variant that
connects the same Worker smoke to a real Oxia service, creates an ephemeral
session marker, derives the context identity from Oxia session metadata and
revalidates that marker before lease operations. The integrated run used
Kafka source `05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Kafka Compose
`nereus-delay-kafka-e2e-1786759086-73769` on `19300,19301,19302`, and Oxia
Compose `nereus-delay-kafka-oxia-e2e-1786759086-73769` on `16656`; it printed
`Kafka Worker authority smoke passed: real Oxia session-bound lease`.

This adds network session-bound owner authority evidence for the one
assignment constructed by the smoke. It does not establish Route assignment
publication, placement, Broker source ownership, real Pulsar Worker
apply/ACK, Broker ACK/rewind failure cuts, due/Lane/publish/checkpoint/
recovery production wiring or Docker crash cuts. The §23.5 completion gate
still controls release status; this slice cannot imply V1 release-ready.

Commit `202368d4` adds the matching real P1 Pulsar Worker smoke. It reuses the
guarded consumer and its post-positioning connection proof across the no-ACK
recovery cursor and the active source loop, then proves the next physical
record through RocksDB `WriteBatch`, synchronous ACK and exact owner drain.
The single-node run used P1
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`, image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
and Compose `nereus-delay-pulsar-e2e-1786760203-85592` on `19930,19931`; it
printed `Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=15/0, active apply ledger/entry=15/1, guarded SUBSCRIBE, RocksDB WriteBatch and ACK`.

This closes the single-node guarded Pulsar recovery → active apply → ACK cut
for one smoke-created source. Its authority is deterministic in-memory, so it
does not establish network Oxia session/placement, Route publication,
multi-broker failover, production multi-shard Worker wiring, due/Lane/
publish/checkpoint/recovery production paths or Docker crash cuts. The §23.5
completion gate still controls release status.

Commit `10e21cbf0e6f741f10b353c56a316a0b57b71b9d` adds an opt-in real Oxia
authority path to the same Pulsar Worker smoke. With
`NEREUS_DELAY_OXIA_ENDPOINT` set, it creates and revalidates an ephemeral
Oxia session marker before using the owner lease, while the default path stays
in-memory. The run used P1 image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, Oxia image
`sha256:4fdba6125c3f3ceca0d5ebe0224464ec83eb815e91999e1910660c60416231ca`,
Pulsar/Oxia Compose projects
`nereus-delay-pulsar-e2e-1786761304-98904` /
`nereus-delay-pulsar-oxia-e2e-1786761304-98904`, and ports
`19940,19941` / `16657`; it printed
`Pulsar Worker authority smoke passed: real Oxia session-bound lease` after
the Worker recovery/apply/ACK line. Commit
`0da18a7b4d6040eeb6700195a1132ee224087ffa` also makes the optional Gradle
arguments safe under the harness's `set -u`. This proves only session-bound
owner authority for the smoke-created assignment; placement, RouteSnapshot
publication, production multi-shard Worker wiring, failover and
crash/failure-injection gates remain open.

Commit `759c4a49b54395211c8ee02c2705006525288fe3` adds the next accepted
assignment boundary. `WorkerPlacementPolicy` remains the local scoring seam;
`WorkerAssignmentCoordinator` turns its selected candidate into canonical
`WorkerAssignment` bytes, publishes them through either the deterministic
revision-CAS authority or the session-fenced Oxia record backend, and requires
an exact authoritative reread before the Kafka/Pulsar native source factory is
allowed to construct source state. The record carries the exact
`SourceAssignment`, placement epoch and capacity-envelope digest, and both
authority implementations reject stale epochs, mismatched revisions and
withdrawals of a different canonical assignment.

Focused codec/authority/Oxia/coordinator tests and isolated real-client
compile/checkstyle gates passed. Fresh Kafka and Pulsar default runs printed
`Worker assignment publication/acceptance passed` with `revision=1` and
`authority=in-memory`; the corresponding optional Oxia runs printed the same
line with `authority=real Oxia session-bound`. The optional Kafka run used
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, Compose projects
`nereus-delay-kafka-e2e-1786763887-31303` /
`nereus-delay-kafka-oxia-e2e-1786763887-31303`, and port `16658`. The optional
Pulsar run used the same Oxia source, projects
`nereus-delay-pulsar-e2e-1786764116-34287` /
`nereus-delay-pulsar-oxia-e2e-1786764116-34287`, and port `16659`; it also
passed the real Oxia owner-authority, Worker recovery/apply/ACK and final P1
E2E lines. This is authoritative per-shard assignment acceptance for the
smoke-created assignments, not yet catalog-driven multi-shard placement,
capacity-envelope registry authority, signed RouteSnapshot publication,
source ownership transfer/reconnect, due/Lane/publish/checkpoint wiring or
crash/failover evidence.

Commit `e173cf0e02e701229f07c37ccac926416ea5c3cb` closes the Route-to-
assignment publication seam. `WorkerAssignment` now carries the signed
`RouteSnapshotV1.snapshotDigest`; `RouteWorkerAssignmentCoordinator` obtains
the Route through the tenant-authorized provider, projects the selected
partition barrier, publishes through revision-CAS authority and revalidates
the exact historical Route projection plus digest before Worker acceptance.
The selected Dockerized Oxia real-service suite included
`OxiaRealRouteWorkerAssignmentSmokeTest` and passed with `BUILD SUCCESSFUL`
against Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786765353-47776` on port `16660`, and image
`sha256:7001f39d94a8d21d74928aad06e7666fcf4bcf3879ef6d27940c9a7ef8db702f`.
It proves the real signed Route event/head, tenant-authorized cache and
session-bound assignment CAS cut for one assignment; catalog-driven
multi-shard placement, capacity-envelope authority, Broker source ownership,
due/Lane/publish/checkpoint wiring and crash/failover evidence remain open.

Commit `2dd2cfff83f4d029972cf7fbeb569fbf4538c026` extends the two real Worker
smokes through the final local drain checkpoint. The Worker supplies an exact
checkpoint identity to `OwnerDrainCoordinator`, executes the bounded
`CHECKPOINT` work class, requires non-empty `CheckpointFileInventory` output,
and verifies exact lease release only after the checkpoint and Store close.
Fresh default Kafka and Pulsar Docker runs both passed their source,
recovery/apply/ACK and final-checkpoint lines, with Compose projects
`nereus-delay-kafka-e2e-1786765675-51303` and
`nereus-delay-pulsar-e2e-1786765675-51304`; both harnesses removed their own
containers, networks and volumes. This is bounded local RocksDB checkpoint
evidence only. It does not provide object-store manifest/publication, due/Lane/
publish orchestration, crash recovery, multi-broker failover or production
multi-shard Worker wiring.

The same code was also exercised with the opt-in Oxia endpoint. The Kafka and
Pulsar runs used ports `19450,19451,19452` / `16661` and `20000,20001` /
`16662`, respectively, and both printed the final-checkpoint Worker line plus
`real Oxia session-bound lease`. Their matching containers, networks and
volumes were removed. The optional Pulsar run emitted multiple-provider SLF4J
warnings from the combined runtime classpath; the run still exited
successfully. This adds network owner-authority evidence only and does not
promote object-store checkpoint publication, due/Lane/publish orchestration,
crash recovery, failover or production multi-shard Worker wiring.

### 2026-08-15 Worker scheduling composition implementation note

Delay commit `c124b216` adds `WorkerSchedulingRuntime` and the
`PersistentLaneScheduler.forActiveOwner` factory. The accepted active Lane
projection is registered before persisted fairness restore; a strict active
Owner/Store check then gates authoritative READY rebuild and each READY poll.
Due discovery is submitted to the shared `DUE_SCHEDULER` work class and the
result is returned as typed `ScheduleWorkItem` candidates. The composition
does not infer Claim materialization, Publish Admission descriptors, external
adapter readiness or checkpoint publication state. Those remain explicit
authority inputs to the later work-class executors and are still required for
the D6 production vertical.

### 2026-08-15 Worker lifecycle fence implementation note

Delay commit `579ad3ba` makes the scheduling graph an optional dependency of
`WorkerShardRuntime`. Source apply and due/READY scheduling now cross the same
runtime admission and drain fence; once the OwnerDrain callback stops source
and scheduling, all three Worker scheduling entrypoints fail closed. This
orders local lifecycle state without inventing remote Claim, publish or
checkpoint authority.

### 2026-08-15 Worker checkpoint queue and atomic Oxia publication implementation note

`WorkerCheckpointRuntime` now owns the local `claimDue` → `CHECKPOINT` queue
handoff while the caller supplies the exact pending intent, manifest factory,
upload adapter and Owner/session prerequisite reread. The gate runs again
after queue wait and, if it fails before physical I/O, completes the exact
process-local claim without creating a checkpoint directory. This preserves a
retryable schedule without turning local Store bytes into Owner or catalog
authority.

Because the pinned Oxia Java client exposes single-record version CAS rather
than a multi-record transaction, `OxiaSyncCheckpointPublicationBackend` uses
one canonical record containing the bounded catalog snapshot and shard upload
intent projections. After an immutable provider upload, one Oxia CAS binds the
exact PUBLISHED intent and catalog manifest. The split per-record Oxia
backends continue to fail closed for this cross-record operation, and an
atomic backend cannot be paired with a different catalog authority. The real
smoke uses a filesystem adapter and one shard; remote Object Store
attestation/quiescence, Owner-session fencing, multi-shard scheduling and
crash/failover evidence remain separate gates.

### 2026-08-15 Worker Claim/Publish command composition implementation note

`WorkerCommandRuntime` is the handoff point from the active Worker graph to
Claim and Publish Admission. It carries either caller-prepared exact Claim
bytes or derives the V1 Claim materialization from the accepted durable
Schedule binding, current Message and canonical Lane tuple behind the same
resource/Owner/READY fence. The underlying executors retain their post-queue
Owner, Claim, permit, prerequisite, signature and Shard Log uncertainty checks;
the wrapper does not infer live Profile, payload serialization, credential,
charge, Publish descriptor, certificate or Source Position authority. This
closes local Claim graph composition while the live Broker append/ACK,
automatic Publish preparation and external prerequisite evidence remain
required.

### 2026-08-15 checkpoint Owner/session and Kafka survivor implementation note

The real Oxia checkpoint publication smoke now acquires an
assignment/session-bound Owner Lease from the same connected Oxia client and
rereads it at the `CHECKPOINT` execution gate before local checkpoint I/O.
After the combined single-record publication CAS, it releases and rereads the
exact lease. This is one-shard owner/session evidence around the local
filesystem adapter; remote Object Store provider ownership and crash recovery
remain separate.

The Kafka real-client harness also reruns the Worker recovery/apply/ACK path
after stopping broker 1, using only brokers 2 and 3. The survivor cut proves
fresh assignment recovery, guarded Fetch v13, WriteBatch-before-`commitSync`
and final checkpoint ordering. It does not prove live source ownership
transfer, an in-flight ACK cut or full D6 failover/crash completion.

### 2026-08-15 Kafka Worker same-topic failover/resume implementation note

The K1 Worker harness now has an explicit `prepare`/`resume` cut. It persists
one guarded record on an exact topic while the three-broker cluster is healthy,
stops broker 1, and launches a new Worker JVM against the same topic through
brokers 2 and 3. The resume path deterministically reconstructs the shard
incarnation from the topic, recovers offset 0 without ACK, applies and ACKs
offset 1 through the guarded Fetch v13 path, writes the bounded final local
checkpoint and releases the exact Owner lease. The fresh run was
`3ca85c74` with Kafka source `05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786771524-17482`, and ports
`19270,19271,19272`. This is a fresh-process, one-broker-stop recovery cut;
same-process ownership transfer, response-loss recovery, multi-shard
placement, crash-at-every-boundary coverage and release gates remain open.

### 2026-08-15 Pulsar Worker broker-restart implementation note

The P1 Worker harness now has an explicit prepare/resume cut. It persists one
guarded source record on the standalone broker's named volume, restarts the
broker container and starts a new Worker JVM with the same topic. The resumed
Worker recovers the prepared ledger/entry, applies and ACKs a new active
record, writes the bounded final checkpoint and releases the exact lease.
This is one single-node process-restart proof with deterministic in-memory
Owner authority; Pulsar multi-broker failover, in-flight response-loss and
full D6 crash evidence remain separate.

### 2026-08-15 Kafka Shard Log mutation implementation note

The Kafka source set now treats the V1 Shard Log as one ordered union of
Client Command and System Mutation frames. `SystemMutationIdentityV1` parses
the canonical typed body and derives the registry identity before a replay
object is constructed; no adapter may inject an operation identity from
outside the frame. `KafkaClientArtifactShardLogMutationAppender` uses the
locked K1 guarded Producer and returns a source position only from checked
response evidence. A missing Produce-response leader epoch is preserved as
optional rather than invented; a later Fetch replay may enrich it.

The recovery and active source paths use the same frame decoder, retain the
exact mutation bytes and source identity, and keep the existing ACK rule:
`commitSync` is the durable cursor boundary and any ambiguous commit remains
unacknowledged. The real Docker smoke covers one `TIME_FENCE` append, ordered
recovery replay, active exposure and ACK on one partition. It does not make
the source adapter a Worker apply authority, and it leaves trust-set
authorization, automatic Claim/Publish, Pulsar mutation apply,
response-loss/crash cuts, multi-shard wiring and release gates open.

### 2026-08-15 Pulsar Shard Log mutation implementation note

`PulsarClientArtifactShardLogMutationAppender` uses the locked P1
resource-guarded Producer to publish the exact signed System Mutation frame.
It validates `GuardedMessageId`, P1 SEND evidence, physical topic/partition,
ledger/entry, batch identity and Broker entry timestamp before creating a
`PulsarSourcePosition`. The current guarded SUBSCRIBE connection generation and
attestation digest are captured before and after the send; a changed proof
returns `UNKNOWN` so the exact mutation must be reconciled through recovery.
`PulsarClientArtifactSourceRecordDecoder` lets both recovery and active source
paths decode the same command/mutation union, and active ACK remains behind the
existing synchronous receipt boundary.

The fresh P1 Docker harness passed the real append → replay → active-source ACK
cut for one `TIME_FENCE` mutation, as well as the existing Worker and
broker-restart/resume cuts. This is bounded single-node transport evidence;
mutation apply, trust-set authorization, response-loss/crash recovery,
multi-broker/multi-shard orchestration and release gates remain open.

### 2026-08-15 automatic V1 Claim materialization implementation note

`CanonicalLaneTupleV1.project` is the public immutable tuple projection used by
the local Claim materializer. `DelayShard.resolveClaimMaterializationV1`
requires a current scheduled Message and its accepted V1 Schedule or
PrepareLargeSchedule sidecar, then derives the exact Destination/Capability
Profile refs, Broker target identity, physical partition, adapter metadata,
timing/actionAt and payload branch. The PrepareLargeSchedule branch rebuilds a
`CommittedPayloadDescriptorV1` from the durable `PayloadReference` and the
binding's Object Store Profile, preserving reservation/proof identity.

`OwnedDelayShard`, `ClaimHandoffWorkClassExecutor`, `WorkerCommandRuntime` and
`WorkerShardRuntime` expose this as a derived Claim submission overload. The
derivation is local durable projection only; trusted time, Claim charge,
external Profile/Object Store/credential/channel readiness, serialization,
Producer ownership and all Publish descriptor/Ready Certificate inputs stay
explicit. Focused inline/object tests cover equality with the strict Claim
validator and fail-closed missing-proof behavior.

### 2026-08-15 Claim-derived Publish descriptor implementation note

`PublishAdmissionWorkClassExecutor` now accepts the exact Claim plus an
externally-authorized `ChannelResourceIdentityV1` and derives the canonical
`PreparedPublishDescriptorV1` projection. The derivation carries the Claim
materialization, lane incarnation, adapter/resource/profile identity,
Claim-derived attempt number and logical identity, and Reserved Publish
metadata into the existing signed Admission body. `WorkerCommandRuntime` and
`WorkerShardRuntime` expose the same overload.

The channel remains a first-class external input because its producer or
transactional identity, resource-guard attestation, credential binding and
credential-use lease cannot be inferred from Claim bytes. Ready Certificate,
trusted decision time, retry deadline, signing key and live adapter
prerequisite remain explicit. This is local Claim → descriptor composition;
automatic channel/Ready-Certificate preparation, physical append/ACK and
response-loss/crash evidence remain separate gates.

### 2026-08-15 bounded READY-to-derived-Claim Worker wiring note

`WorkerShardRuntime.pollAndSubmitClaim` narrows one scheduling visit to a
single READY head and sends that exact item through the derived Claim overload.
This preserves the persistent scheduler's per-head `requeueFailedClaim` and
`completeClaim` semantics when the Claim queue or pre-commit validation fails;
the Worker does not need to speculate about rollback for a multi-head poll.

Trusted UTC evidence, Claim deadline/charge and the external prerequisite gate
remain inputs. The method is a local one-shard DUE/READY → Claim composition;
channel/Ready Certificate/Publish preparation, physical append/ACK,
multi-shard assignment and crash/failover evidence remain separate.

### 2026-08-15 Claim-result-bound Publish composition

Delay commit `e5828f40` adds a Publish handoff that takes the exact successful
Claim result and a typed external preparation bundle. The runtime carries the
Claim's active reservation unchanged into the canonical Publish builder and
rejects all non-CLAIMED results before queue admission. Channel identity,
Ready Certificate, trusted decision time, retry deadline, signing key and
Owner clock remain external inputs, so this boundary cannot manufacture live
Broker or credential authority.

This closes local Claim-result → Publish queue composition only; it does not
prepare channels, append/ACK the Shard Log, classify response loss, or prove
multi-shard, failover, crash or release evidence.

### 2026-08-15 bounded due-to-Claim Worker composition

Delay commit `ce4bc2d5` adds a bounded Worker entrypoint that submits due
discovery, polls at most one strict READY head and queues its durable-derived
Claim. The Claim work-class action is intentionally returned before execution;
the shared command turn remains the only executor. This preserves the
one-head scheduler requeue identity and the existing Owner/Claim/permit/
prerequisite fences.

This closes only local one-shard DUE/READY → Claim orchestration. It does not
infer external channel/credential/Ready-Certificate state, execute Publish
Admission, append or ACK the Shard Log, or provide multi-shard/failover/crash
release evidence.

### 2026-08-15 bounded due-to-Claim-to-Publish Worker composition

Delay commit `5305748b02965f171ac751615bb00b4dda8a9eb0` extends the bounded
entrypoint so the exact Claim work-class task is observed through bounded fair
shared-command turns. After a successful Claim, an injected typed
`PublishPreparationProvider` may supply the external preparation bundle; the
exact Claim result and active reservation are then used to submit and observe
the exact Publish task through the same bounded command-turn mechanism. An
empty preparation result returns the Claim result with its reservation still
active for retry or explicit revoke. Provider failure fences the Owner, and a
Publish `UNKNOWN` result retains the reservation until source-ordered
resolution or explicit release.

The Claim regression covers this composed path and the Claim-plus-Publish
focused pair, followed by a passing full `check` (21 actionable tasks). This
is local one-shard orchestration with caller-supplied preparation. It does not
create live Profile/credential/Broker authority, automatically prepare a
channel, append or ACK a physical Broker mutation, resolve response loss or
crash cuts, place multiple shards, publish to remote Object Store, or satisfy
the release gates.

### 2026-08-15 checkpoint preflight and bounded multi-shard Worker composition

The checkpoint work-class boundary must not strand a process-local schedule
claim. Delay commit `ad5020f0` therefore releases the exact
`ScheduledCheckpoint` when Owner/intent preflight fails before queue
registration; queue saturation remains a no-I/O retry path with the claim
still current. The corresponding focused test proves the next schedule can be
claimed after a rejected preflight.

Delay commit `d0fe7158` adds a bounded `WorkerShardFleetRuntime` around already
activated `WorkerShardRuntime` instances. It enforces one shared WorkClass
execution graph and one shared RocksDB resource envelope, rejects duplicate
Shard identities, and round-robins one source/scheduling/command turn at a
time. It does not own Route assignment, Owner Lease CAS, native client
construction or checkpoint publication; a shard must still drain itself before
the fleet can close it. This is local multi-shard event-loop wiring only, not
catalog placement, automatic Ready/Publish orchestration, Broker failover,
crash evidence or §23.5 release completion.

### 2026-08-15 recurring checkpoint Worker wiring

Delay commit `46ca2b1e` extends that composition with an optional recurring
checkpoint graph. A shard accepts `WorkerCheckpointRuntime` only when its
WorkClass registry, Shard Store and shared RocksDB resource envelope are the
same object identities as the owning Worker composition. Its register,
due-claim, submit and run methods therefore remain behind the source-running
and runtime-business-admission fences. The fleet supplies a separate bounded
checkpoint cursor and returns no checkpoint action for a shard without the
graph.

This wires local schedule/queue execution only. It does not claim remote Object
Store/provider/catalog authority, automatic Ready/Publish preparation,
checkpoint-on-drain completeness, assignment/failover ownership, crash-boundary
evidence or §23.5 release completion.

### 2026-08-15 checkpoint claim-to-Store shard fence

Delay commit `c8d85e66` makes the Worker checkpoint wrapper identity-aware:
registration rejects a shard different from the attached Store, and due
selection uses a shard-filtered scheduler claim when the process schedule is
shared. This prevents a valid process-local schedule handle from being routed
to the wrong Store/manifest/catalog execution boundary.

The fence is local identity protection only. It does not turn the schedule into
durable authority or provide remote publication, assignment/failover,
crash-boundary or §23.5 release evidence.

### 2026-08-15 recurring checkpoint drain fence

Delay commit `6f1f6d25` makes recurring checkpoint lifecycle explicit at the
Worker drain boundary. A shard with an in-flight exact checkpoint claim fails
closed before Owner/Store/lease drain begins; an idle schedule is unregistered
before drain, preventing a replacement owner from colliding with stale
process-local registration. This is only local schedule lifecycle protection;
provider quiescence, remote publication durability, assignment/failover,
crash-boundary and §23.5 release evidence remain separate.

### 2026-08-15 recurring checkpoint claim-to-submit wiring

Delay commit `5dacd6f3` adds the bounded `claimDueAndSubmit` bridge to the
Worker checkpoint graph. It claims one exact schedule capability, lets the
injected request factory assemble the immutable Store/intent/manifest/upload
inputs, verifies the same capability is retained, and releases that claim if
request construction fails before work-class admission. The public Worker
wrapper keeps this behind the existing source lifecycle and shared-resource
fences; no Store or provider I/O is inferred by the bridge.

This closes local recurring-checkpoint claim-to-request composition. It does
not provide durable schedule authority, remote Object Store/catalog
publication, automatic Ready/Publish orchestration, source ownership transfer,
broker failover, crash evidence or §23.5 release completion.

### 2026-08-15 Kafka System Mutation Worker apply

The Kafka real-client binding now has a mixed-mutation Worker receipt. Two
signed `TIME_FENCE` frames are appended through the guarded K1 Producer. The
recovery cursor is bounded to the first frame, and the strict Owner recovery
action applies it through the shared `SOURCE_APPLY` WorkClass before the
exclusive Kafka activation barrier opens. The active Worker seeks to the
second offset, validates Fetch v13 evidence, applies `DelayShard`'s durable
System Mutation WriteBatch, verifies the exact `SystemMutationResult` and
Store Source Position, and then acknowledges with `commitSync`; drain also
requires the final local checkpoint.

The receipt used Delay `eee022bd`, Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786779783-8472`, and ports
`19700,19701,19702`. The K1 append response may omit leader epoch; replay
evidence may enrich it, but cluster, TopicId, partition, offset and append
time remain exact identity fields.

This is an integration cut for the local/in-memory owner authority. It does
not itself establish real Oxia assignment/session CAS, mutation response-loss
or crash recovery, Pulsar mutation apply, automatic Claim/Publish authority,
multi-shard placement or §23.5 release completion.

### 2026-08-15 Pulsar System Mutation Worker apply

The P1 real-client binding now has a mixed-mutation Worker receipt. Two signed
`TIME_FENCE` frames are appended through the guarded P1 Producer on one topic.
The recovery cursor is bounded to the first frame, and strict Owner recovery
applies ledger/entry `18/0` through the shared `SOURCE_APPLY` WorkClass before
the exclusive activation barrier opens. The active Worker consumes ledger/
entry `18/1` through guarded SUBSCRIBE, validates the exact P1 source proof,
applies `DelayShard`'s durable System Mutation WriteBatch, verifies the exact
`SystemMutationResult` and Store Source Position, and acknowledges the source.
Drain then requires the final local checkpoint and releases the Owner lease.

The receipt used Delay `016288b1`, P1 source
`358ce4a1033bd566faebcd3465c3ba4606f3c83f`, client SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
distribution SHA-256
`7ba7bd3d02e104fc935c2accd49b3e7645a4f4c21a4c5978e99dac5c5a1d137d`, P1 image
`sha256:eb33130364ffaf319bb20052698745f5d84de20fe78cd5fa7d7c6a9f19c402c0`,
and the Docker receipt `nereus-delay-pulsar-e2e-1786780346-14394` on ports
`19930,19931`. A second full run on `19940,19941` reproduced the same
ledger/entry pair and the final aggregate pass.

This is an integration cut for local/in-memory owner authority. It does not
establish real Oxia assignment/session CAS, Pulsar multi-broker failover,
native source ownership transfer, response-loss or crash evidence,
automatic Claim/Publish authority, catalog-driven placement or §23.5 release
completion.

### 2026-08-15 real Oxia mutation Worker authority

The bounded Kafka and Pulsar mutation Worker cuts were rerun with network Oxia
enabled. Kafka used source `05849884ca81fad767fda058444d1e17c7f9cbf9` and
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, with Compose projects
`nereus-delay-kafka-e2e-1786781272-24697` /
`nereus-delay-kafka-oxia-e2e-1786781272-24697`, broker ports
`19710,19711,19712` and Oxia port `16671`. The mutation Worker applied
offset `0` during recovery and offset `1` through guarded Fetch v13 before
`commitSync`, then reported a real Oxia session-bound lease. Pulsar used P1
source `358ce4a1033bd566faebcd3465c3ba4606f3c83f`, the same Oxia source,
Compose projects `nereus-delay-pulsar-e2e-1786781139-23243` /
`nereus-delay-pulsar-oxia-e2e-1786781139-23243`, broker/web ports
`19950,19951` and Oxia port `16670`. Its mutation Worker applied ledger/entry
`18/0` during recovery and `18/1` through guarded SUBSCRIBE before ACK, then
reported the same real Oxia lease proof.

These are bounded one-shard assignment/session receipts. They do not establish
catalog-driven multi-shard placement, signed Route activation/source ownership
transfer, Pulsar multi-broker failover, response-loss or crash evidence,
automatic Claim/Publish authority or §23.5 release completion.

### 2026-08-15 Kafka guarded Fetch to signed Route Worker assignment

Delay commit `7e0abb87fff8db1c1d2d2f73ffdd44a0c6097112` adds `KafkaClientArtifactRouteWorkerSmoke`. It first
requires a real K1 guarded Fetch v13 response for one persisted record and
retains the exact cluster, native TopicId, partition, record range,
`lastStableOffset` and response digest. The smoke projects that evidence into
`ActivationBarrierV1.kafka`, signs a one-partition `RouteSnapshotV1`, publishes
the immutable Route event/head through a session-fenced real Oxia client, and
refreshes the provider from the Oxia head. `RouteWorkerAssignmentCoordinator`
then resolves the active Route, publishes a revision-CAS assignment carrying
the Route digest and barrier, rereads it before acceptance, recovers the
pre-Route record into the real Worker Store and ACKs it after the RocksDB
apply, appends a second record, seeks the guarded Worker source to the signed
exclusive offset and applies/ACKs it with `commitSync`. The Worker then drains
through its final local checkpoint and proves the Oxia owner lease and
assignment release.

The source-locked receipt used Delay
`7e0abb87fff8db1c1d2d2f73ffdd44a0c6097112`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, Kafka client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
Compose projects `nereus-delay-kafka-e2e-1786785694-74566` /
`nereus-delay-kafka-oxia-e2e-1786785694-74566` on broker ports
`19730,19731,19732` and Oxia port `16673`. The exact receipt was:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=1, commitSync ACK, final checkpoint
```

This closes the bounded real-Kafka barrier-to-assignment/Worker-apply/source-
ACK/checkpoint cut. It does not claim that a production Worker can refresh a Route through session
churn, survive Broker failover with an accepted assignment, establish native
eligibility, perform catalog-driven multi-shard placement or complete the
Object Store/checkpoint/Claim/Publish/release gates.

### 2026-08-15 Kafka accepted Route broker failover implementation note

Delay commit `7e94d0f8a3e374832a111dbd2f741be5f20795d5` adds an explicit
failover gate to the real Kafka Route Worker smoke. After the signed Route and
route-bound Oxia assignment are reread and accepted, the Worker waits without
changing the assignment. The opt-in harness stops `kafka-1`, waits for a
surviving broker, releases the gate, and requires the next guarded source
record to pass the same Route-bound Store apply, exact Kafka position check and
`commitSync` ACK before final local checkpoint and assignment/owner release.

The source-locked standalone command set
`NEREUS_DELAY_KAFKA_WITH_OXIA=1`,
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1`, and
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY=1` against Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, Kafka/Oxia Compose projects
`nereus-delay-kafka-e2e-1786787846-2966` /
`nereus-delay-kafka-oxia-e2e-1786787846-2966`, broker ports
`19750,19751,19752`, and Oxia port `16677`. The bounded receipt was:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=2, commitSync ACK, accepted-route broker failover, final checkpoint
```

This implementation note establishes only the one-topic/one-partition
accepted-Route broker-1 failover cut. It does not establish Route session
churn/reconnect, catalog-driven placement, native eligibility, production
source ownership transfer, remote Object Store authority, response-loss or
crash-at-boundary evidence, Pulsar multi-broker failover or the §23.5 release
gates.

### 2026-08-15 Pulsar guarded SUBSCRIBE to signed Route Worker assignment

Delay commit `bf858b089b927fcf65129214d8ed5a7fc5300deb` adds
`PulsarClientArtifactRouteWorkerSmoke` for the locked P1 source. P1 commit
`0a2536484cd3932801a98dc88ff112b2df88a1c7` adds a dedicated,
admin/ownership-checked Resource Controller endpoint for the exact guard
tuple. The generic topic-properties mutation remains fail-closed, so the
smoke creates a native one-partition topic and stamps the physical
`-partition-0` guard through the dedicated endpoint before opening the guarded
source.

The first guarded record supplies the exact Pulsar ledger/entry/batch source
position and stable attestation. Those values, together with the guarded
connection generation and attestation digest, form
`ActivationBarrierV1.pulsar`. The smoke recovers the pre-Route record into the
real Worker Store before ACK, signs and publishes the immutable Route
event/head through a session-fenced real Oxia client, rereads it through the
Route provider, and projects the exact Route digest/barrier into a revision-CAS
Worker assignment. It hands the guarded connection to `WorkerShardRuntime`,
which applies and ACKs the second record, publishes a bounded local RocksDB
checkpoint, and releases the owner lease and assignment.

The assignment-only source receipt at Delay
`a73faf3e836ada67931f709d46214dde7caf3ad0` is retained as historical
provenance. The current source-locked receipt uses Delay `bf858b089b927fcf65129214d8ed5a7fc5300deb`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` from
`8dae0236c0a0d405ed7f8303081080520fe91551`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`. P1 artifact digests were client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
and distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`. The
base image was
`eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`.
The exact receipt was:

```text
Pulsar signed Route -> guarded SUBSCRIBE barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: generation=15, barrier=20/0, routeRevision=1, assignmentRevision=1, source=20/1, ACK, final checkpoint
```

This is bounded one-partition Route barrier/assignment/Worker-apply/source-ACK
and local-checkpoint evidence. It is not production activation evidence and
does not close session reconnect/churn, multi-broker failover,
catalog-driven multi-shard placement, native eligibility, source ownership
transfer, Object Store/checkpoint publication, automatic Claim/Publish
authority or the release cross-entry gate.

### 2026-08-15 Dockerized Oxia authority and checkpoint publication revalidation

The isolated real-service harness passed from Delay
`ac72e43803806b9c309b62150c0aa54b43f8a3ea` against Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, using Compose project
`nereus-delay-v1-oxia-e2e-1786787138-90186` and host port `16675`. The selected
Owner Lease, Control, Recovery Catalog, atomic checkpoint intent/catalog,
Route, Gateway audit and Gateway admission tests completed with
`BUILD SUCCESSFUL in 11s`.

The provider-side checkpoint upload in this evidence is the crash-durable
filesystem object seam; remote Object Store credentials/quiescence,
session-bound RecoveryPin transaction, multi-shard placement and release
cross-entry gates remain unproven.

### 2026-08-15 Oxia Route provider restart/revalidation implementation note

Delay commit `164597c39f1da6fc403c5283494b1f0c6b132802` adds a dedicated,
file-gated real-service smoke for the Route cache lifecycle. The provider first
publishes and caches a signed Route, then the harness restarts the same Oxia
container and waits for service health before releasing the test. Explicit
`refresh()` must revalidate the Oxia session and rebuild the head/event cache;
the test checks revision, cache health and exact signed snapshot bytes.

The source-locked receipt used Oxia
`37a17bef17202d5fd6e232da5fd26d94865484`, Compose project
`nereus-delay-v1-oxia-e2e-1786789198-22565`, port `16684`, and image
`sha256:1ea8324636e65d92bf6f0767062e58078fd617767c9c74540443c5b6a2c1293d`:

```text
Oxia Route provider restart recovery passed: revision=1, session revalidated, cache healthy
Dockerized Oxia Route restart smoke passed: provider session recovery and signed Route cache rebuild
```

This is bounded one-stream restart/revalidation evidence. It does not prove
ephemeral marker rotation after expiry, notification-stream reconnect/churn,
multi-node Oxia failover, catalog-driven placement, remote Object Store
authority or the §23.5 release gates.

### 2026-08-15 current-source Kafka and Pulsar real-client revalidation

The current Delay tree at `efa422a9ec16cb370376e0c5a72b18bbbdb3a906` was
revalidated against the locked Kafka and P1 worktrees. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from base `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786795881-97477`, ports
`19835,19836,19837`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484` in project
`nereus-delay-kafka-oxia-e2e-1786795881-97477` on `16686`. The complete
current-source receipt was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786796050-99359`, broker/web ports
`20135,20136`, and Oxia in project
`nereus-delay-pulsar-oxia-e2e-1786796050-99359` on `16687`. The complete
receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

These receipts are current-source transport/Worker evidence only. The Kafka
cut includes three-broker broker-1 failover; the Pulsar harness is the checked-
in single-standalone-broker restart path and does not prove multi-broker
failover. Neither receipt promotes typed Lane activation to production: live
Profile/credential/Broker prerequisite authority, automatic due-to-Claim-to-
Publish execution, multi-shard placement, remote Object Store checkpoint
authority, crash/response-loss resolution and the §23.5 release gates remain
open. The temporary Compose resources were removed on exit.

### 2026-08-15 current-source Kafka and Pulsar revalidation after bounded Worker composition

The implementation source lock for this rerun is Delay
`5305748b02965f171ac751615bb00b4dda8a9eb0`. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from base `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786797371-14292`, ports
`19845,19846,19847`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484` in project
`nereus-delay-kafka-oxia-e2e-1786797371-14292` on port `16696`. The exact
receipt was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from base
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786797371-14293`, broker/web ports
`20145,20146`, and Oxia
`37a17bef17202d5fd6e232da5fd26d94865484` in project
`nereus-delay-pulsar-oxia-e2e-1786797371-14293` on port `16697`, with image
`sha256:b8e9f6e6497308be5e1c1cb937a6af96be10d8b258cb660696f605cdf0b495e3`.
The exact receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

These reruns revalidate the locked transport/Worker paths after
`5305748b`; the harnesses do not invoke `runDueClaimPublishTurn`, so they do
not prove its provider-driven live preparation or physical Publish append/ACK.
Kafka covers the checked-in three-broker K1/K2 cuts. Pulsar covers one
standalone-broker restart and not multi-broker failover. Live
Profile/credential/Broker authority, multi-shard placement, remote Object
Store checkpoint authority, crash/response-loss resolution and the §23.5
release gates remain open. Temporary Compose resources were removed, and the
Kafka Oxia image was removed by cleanup.

## 16. 当前结论与仍需实测的数值

已经冻结：

- Direct SDK 为 Java 默认高性能入口，Gateway 为可选多语言/集中治理入口；
- 两者复用同一 Semantic Core 和 exact Command/receipt types；
- Gateway 请求幂等先持久化 exact prepared bytes，再取得 Broker ownership；
- Kafka/Pulsar 提供通用 guarded resource API，绝不依赖 Nereus SDK；
- Kafka K1 non-transactional wire 固定 Produce v13+ exact TopicId；K2
  provides an opt-in transaction-v2 target-plus-receipt binding。受控
  committed-EndTxn 与 source/Worker response-loss receipt 子集已有 evidence，
  但 raw network/process response-loss、Fetch/LSO/retention 与 production
  source/placement gates 仍独立开放；
- Pulsar 使用 first-class v22 create/per-SEND guard 和 guarded receipt；受控
  SEND、source-ACK 与 Worker response-loss receipt 子集已有真实 Broker
  evidence，但 raw network/proxy/session、rewind 与完整 D3/production gates
  仍保持独立开放；
- native AUTO_FAST 只允许 issuer-verified local snapshots，且 managed fallback
  保持 exact bytes；issuer now has a local protection-before-signing boundary,
  but production native capability authority and live Broker eligibility remain open；
- Route activation barriers now have an exact signed-snapshot-to-source-assignment
  projection, and Worker assignment lookup is tenant-authorized for current or
  historical Routes；bounded real Kafka/Pulsar activation、assignment and
  Broker-survivor Worker failover evidence exists，while catalog placement、
  native eligibility、production Owner Lease CAS and source-session authority
  remain open；
- 当前 V1 self-routing ID、tenant authority、Worker 状态机不改；
- stock/name-only fallback 不可进入生产包。

仍由 benchmark/部署证据给出，而非在代码中猜默认值：

- 每 Route/Worker/Gateway channel 数量；
- client/gateway pending count/bytes；
- Gateway idempotency Oxia 吞吐与 retention；
- Kafka guarded batch 影响；
- Pulsar per-SEND compare 成本；
- transport close quiescence 和 credential lease 时间界限。

这些是发布数值 evidence，不是语义 OPEN。实现缺少它们时状态为“未达到 production/release gate”，不能静默采用无界默认值。

### K2 broker failover evidence boundary

Delay commit `6912b940` adds a test-only file gate immediately before the
guarded transaction-v2 `commitTransaction`/`EndTxn` boundary. The Docker
harness uses `NEREUS_DELAY_KAFKA_K2_FAILOVER_ONLY=1` to stop broker 1 while the
target and keyed receipt are already guarded and enqueued, releases the gate,
and then proves the result with `read_committed` counts plus exact target and
receipt reads. The successful source lock was Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from `c300006a7705c240642db6950b5a95fec982bfc5`; the client SHA-256 was
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3` and the
broker image was
`sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`.

The receipt observed `PUBLISHED` after the broker cut, and the client
rediscovered the transaction coordinator on broker 3 after broker 1 stopped.
This is a bounded source-locked failover proof, not a lost-response proof:
generic `UNKNOWN` resolution, Fetch/LSO/retention ambiguity and crash recovery
remain separately gated. The normal K2 smoke continues to own abort and target
delete/recreate fencing.

### 2026-08-15 strict typed Lane activation and complete Worker graph implementation note

Delay commit `defce755` makes a V1 Schedule/Prepare Lane typed when the first
projection contains the exact canonical V1 Lane tuple. The persisted
`ActiveLaneStateV1` carries the immutable Destination/Capability Profile refs,
tuple identity and typed readiness; a malformed legacy resolver tuple remains
a compatibility projection and is not upgraded by guessing Profile data.
`ReadyCertificateV1` now exposes its exact activation barrier and evidence
cursors so activation can bind the external proof rather than treating a
readiness enum as authority.

`LaneActivationCoordinator` captures the exact Lane incarnation, Owner and
Store Incarnation after the owned shard enters `CATCHING_UP`, then delegates
Profile/credential/channel/evidence preparation to an injected authority.
`OwnedDelayShard` rereads the context-bound Oxia catch-up lease before
`DelayShard.activateLaneReadiness` atomically replaces the Lane projection and
READY index with the externally supplied Channel Resource, Ready Certificate
and evidence cursors. Exact-certificate retries are idempotent; a different
certificate, Lane, tuple, Store or Owner is rejected. The raw readiness test
seam remains compatibility-only and cannot make a typed Lane READY without a
certificate.

Kafka and Pulsar `WorkerSourceFactory.create` now also accept the complete
`WorkerSchedulingRuntime`, `WorkerCommandRuntime` and
`WorkerCheckpointRuntime` graph. The existing resource/assignment/Owner
identity fences remain in `WorkerShardRuntime`; the overload does not create a
Profile, Broker, credential, Oxia or checkpoint authority.

`check` passed with the focused Lane activation, Claim handoff and Claim
materialization tests; the real Kafka and Pulsar factory compile gates passed
against their locked client artifacts. This closes local typed Lane activation
and Worker graph composition only. It does not provide a live prerequisite
authority, automatic due-to-Claim-to-Publish execution, multi-shard or
transport E2E, crash/response-loss evidence, or V1 release PASS.

### 2026-08-15 typed Lane scheduling bootstrap implementation note

Delay commit `7a48f85b` adds `WorkerSchedulingRuntime.openForActiveOwnerFromTypedLanes`.
The production-shaped bootstrap accepts only the exact Lane identities that
were persisted by typed activation, rereads each `ActiveLaneStateV1`, and
rejects missing/legacy state, non-OPEN or non-READY state, absent Ready
Certificate, duplicate Lane identity, or LaneRecord incarnation/version drift
before constructing the scheduler. It then uses the existing strict
Owner/Store/Shard fence and authoritative READY-index rebuild.

The caller-supplied `List<LaneRecord>` factory remains an embedded compatibility
seam for historical composition tests. The new path makes the intended
activation-to-scheduling boundary explicit: a restart can restore a durable
READY head before a due turn, so that turn reports no newly promoted head while
the strict READY poll still returns the restored candidate. This is local typed
bootstrap and fairness-state recovery only; it does not add live Profile,
Broker, credential, Oxia placement, Claim/Publish or checkpoint authority.

### 2026-08-15 typed Worker publish-preparation coordinator implementation note

Delay commit `2a8c198328e5a8879db9c23faf6e805b6d7ea819` adds
`WorkerPublishPreparationCoordinator` between the successful Claim result and
the external Publish preparation authority. It rereads the context-bound
Owner/Claim admission and the persisted typed Lane before invoking that
authority. The Lane must remain `OPEN` and `READY`, and its
`ReadyCertificateV1`, Channel Resource, Owner/Store/Lane identities, activation
barrier, materialization target/partition and typed Destination/Capability
Profile refs must match the Claim exactly.

The callback receives the immutable Claim/Lane/channel/certificate bundle and
still owns live credential/channel resolution, signing key selection and
trusted publish timing. An empty result leaves the Claim reservation active;
any returned preparation must preserve the exact persisted channel and
certificate. The focused Claim regression rejects a foreign Store Certificate.
The full `check` passed (`BUILD SUCCESSFUL in 1m 13s`, 21 actionable tasks),
with real Oxia smokes still controlled by their opt-in gates.

This closes the local typed Claim-to-preparation identity boundary. It does
not manufacture live prerequisite authority, invoke physical append/ACK,
resolve response loss or crash cuts, provide automatic due-to-Claim-to-Publish
execution, place multiple shards, or satisfy the release gates.

### 2026-08-15 current-source Kafka and Pulsar revalidation after typed preparation coordinator

The current Delay source lock is
`2a8c198328e5a8879db9c23faf6e805b6d7ea819`. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from base `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786798539-27043`, ports
`19855,19856,19857`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-kafka-oxia-e2e-1786798539-27043` on `16698`. The receipt was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from base
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786798539-27042`, broker/web ports
`20155,20156`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-pulsar-oxia-e2e-1786798539-27042` on `16699`, with image
`sha256:58c9302be462dc5f16ba593c289b17373a14d85cead7b0526b0bc02cfa2ee575`.
The receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

These receipts revalidate only the locked transport/Worker source paths after
`2a8c1983`; the harnesses do not construct the new coordinator or invoke
provider-driven `runDueClaimPublishTurn`. They therefore do not prove live
preparation or physical Publish append/ACK. Kafka covers the checked-in
three-broker K1/K2 cuts; Pulsar covers one standalone-broker restart, not
multi-broker failover. Live prerequisite authority, multi-shard placement,
remote Object Store checkpointing, crash/response-loss resolution and §23.5
release gates remain open. Temporary Compose resources were removed, and the
Kafka Oxia image was removed by cleanup.

### 2026-08-15 typed Worker preparation provider binding implementation note

Delay commit `d02d81201d0cff3f9fa5fb3c8bba912721de5575` carries the
`PublishPreparationProvider` through complete Worker graph construction. The
no-provider `WorkerShardRuntime.runDueClaimPublishTurn(...)` overload resolves
that bound provider and fails closed when no provider was bound; the explicit
provider overload remains a narrow composition seam. Kafka and Pulsar complete
`WorkerSourceFactory.create` overloads pass the provider into the runtime, while
their older complete-graph overloads intentionally leave it unbound. The
focused Claim handoff test binds `WorkerPublishPreparationCoordinator` through
the graph and verifies the unbound runtime failure.

The provider remains caller-supplied external authority. This implementation
does not create live Profile/credential/Broker/channel/signing-key authority,
physical Publish append/ACK or checkpoint authority. Focused tests, exact real
Kafka/Pulsar compile gates and full `check` passed; real Oxia smokes remain
controlled by their opt-in gates.

### 2026-08-15 current-source Kafka and Pulsar revalidation after typed Worker provider binding

The current Delay source lock is
`d02d81201d0cff3f9fa5fb3c8bba912721de5575`. Kafka used
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`
from base `c300006a7705c240642db6950b5a95fec982bfc5`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786799494-38038`, ports
`19865,19866,19867`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-kafka-oxia-e2e-1786799494-38038` on `16700`. The receipt was:

```text
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

Pulsar used P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` from base
`8dae0236c0a0d405ed7f8303081080520fe91551`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
client-api `f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`,
common `94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`,
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
runtime library count `341`, Compose project
`nereus-delay-pulsar-e2e-1786799494-38039`, broker/web ports `20165,20166`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` in project
`nereus-delay-pulsar-oxia-e2e-1786799494-38039` on `16701`. The receipt was:

```text
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

These receipts revalidate only the locked transport/Worker paths after
`d02d8120`; neither harness constructs the bound provider or invokes
provider-driven `runDueClaimPublishTurn`, so they do not prove live preparation
or physical Publish append/ACK. Kafka covers three-broker K1/K2 failover;
Pulsar covers a standalone-broker restart, not multi-broker failover. Live
prerequisite authority, multi-shard placement, remote Object Store
checkpointing, crash/response-loss resolution and §23.5 release gates remain
open. Temporary Compose resources were removed; the Kafka Oxia image was
removed by cleanup and the Pulsar Oxia image remained locally with digest
`sha256:71d69981a5b9dd458158a8c440fb6d90642450d96b0e92e59f7c49745bbc498c`.

### 2026-08-15 fleet-level due-to-Claim-to-Publish dispatch implementation note

Delay commit `d5672a46c9558aa4417f744f30cccd79518adde0` adds
`WorkerShardFleetRuntime.runNextDueClaimPublishTurn(...)`. The fair fleet
cursor selects only a shard that has both `WorkerSchedulingRuntime` and
`WorkerCommandRuntime`; the selected shard executes its existing bounded
`WorkerShardRuntime.runDueClaimPublishTurn(...)` through the provider bound at
graph construction. No caller-supplied replacement provider is accepted, and
an unbound selected runtime fails closed.

The focused Claim handoff test now enters through the fleet boundary, and the
full `check` passed. This closes a common multi-shard dispatch seam only. The
real-client Kafka/Pulsar receipts were not rerun for this API-only change and
remain source-qualified to `d02d8120`; they do not prove this fleet method.
Live prerequisite authority, catalog placement, physical Publish append/ACK,
checkpoint publication, crash/response-loss recovery and release gates remain
open.

### 2026-08-15 PUBLISHING physical adapter and Outcome bridge implementation note

Delay commit `9a9c14827d01f94b36820e2e4381373725cec7fa` adds the common
`WorkerPhysicalPublishExecutor` bridge. It accepts only a retained
`PUBLISHING` attempt whose canonical `PUBLISH_ADMISSION` descriptor matches the
Claim, READY Lane, message, generation, attempt and prepared payload identity.
Inline payloads and Object Store payload bytes are checked against the frozen
length/SHA-256 projection before the exact destination request is built.

The bridge checks the injected `PhysicalPublishGate` before local physical
admission and again via `BoundedDestinationPublishAdapter.PublishPreflight`
immediately before the delegate. This preserves the required happens-before
boundary while retaining bounded-adapter zombie/in-flight semantics. Deferred
and definitive gate results do not call the target. An observed destination
result, including `UNKNOWN`, is passed to the injected signed
`PublishOutcomeMutationFactory` and then the bounded `OutcomeWorkClassExecutor`;
the bridge does not apply RocksDB state or invent a Source Position.

This is a common composition seam, not physical Broker proof. Live gate,
payload/Object Store, channel/certificate/credential, signed outcome/evidence/
retry/charge factories, Kafka/Pulsar append/ACK, adapter journals and
non-persistence classifiers remain external. The real-client harnesses do not
construct this executor and were not rerun for this change, so their latest
receipts remain source-qualified to `d02d81201d0cff3f9fa5fb3c8bba912721de5575`.

### 2026-08-15 typed signed PUBLISH_OUTCOME factory implementation note

`WorkerPublishOutcomeMutationFactory` implements the bridge's Outcome factory
with a strict local boundary: it accepts only a PUBLISHING ledger and matching
destination request, preserves the adapter's PUBLISHED/definitive/UNKNOWN
branch, requires attempt-owned typed Publish Evidence for observed definitive
results, validates the canonical `PublishOutcomeBody`, and signs the exact
`PUBLISH_OUTCOME` System Mutation with the supplied owner author/key.

The factory's `OutcomeContextProvider` supplies retry/disposition, charge
transfer and trusted-time projections from external authority. The factory
does not classify Broker response loss, mint physical evidence, apply RocksDB
state or append to the Shard Log. Those boundaries, plus live signing-key
protection and source-factory production wiring, remain open.

### 2026-08-15 source-bound physical adapter invocation implementation note

Delay commit `5222c9bbb9f64e6a0fe58009ccf143ca8ec59636` adds a default
source-aware adapter method carrying canonical Source Position and prepared
Publish hash. The bounded wrapper forwards that context through its existing
physical admission and final preflight boundary. The Worker decodes the
Source Position retained in the PUBLISHING ledger immediately before the
delegate call, and Kafka's transactional target-plus-receipt adapter now
implements the overload explicitly; ordinary adapters keep the compatibility
one-argument path.

This is not source replay or Broker evidence. The runtime still needs the
source-applied PUBLISHING ledger, live assignment/ACK authority, K2 response
loss/LSO resolution and a real Worker E2E before this composition can be
promoted.

### 2026-08-15 Pulsar source-bound physical adapter implementation note

Delay commit `1d969cb8fa15430faaf8b38ae1e34390ce5e7769` extends the source-aware
adapter path to `PinnedPulsarDestinationAdapter`. It rejects a source-bound
request whose Source Position is not a Pulsar position for the same Shard,
then passes the exact Source Position and prepared Publish hash to the guarded
Pulsar transport. Request-only transport implementations remain on the
compatibility default.

This does not prove Pulsar Broker persistence, source ACK, reconnect/rewind or
response-loss resolution. Those require the source-applied Worker ledger,
live assignment/channel authority and a real-client Worker E2E.

### 2026-08-15 persisted PUBLISHING attempt lookup implementation note

Delay commit `e9cfde1415e2c389c8587b1d72ed7f42afa47b79` adds a Worker physical
entrypoint keyed by the durable Publish Attempt ID. It performs the bounded
owned-shard inflight scan and accepts only a current `PUBLISHING` ledger before
calling the existing Admission payload validator and source-aware adapter
path. Missing or already-UNCERTAIN attempts stop before adapter, Outcome or
fence side effects.

This removes stale caller-held ledger state from the local composition but does
not perform source replay, live Owner/Assignment rereads, Object Store fetch,
Broker append/ACK or crash/response-loss resolution.

### 2026-08-15 source-applied PUBLISHING ledger to physical Worker dispatch

Delay commit `ada1d2aa80bbdaf73293e46203fcb7dfd4f0a93d` adds
`WorkerShardRuntime.runSourceBoundPhysicalPublish(...)`. The entrypoint accepts
the exact Admission logical identity and its persisted Shard Log Source
Position, then runs only bounded source turns until that Position is applied
and ACKed. It reloads the current inflight ledger from the owned `DelayShard`,
requires the ledger Source Position to equal the Admission append, and accepts
only `PUBLISHING` before invoking the existing source-aware physical bridge.

The payload callback is intentionally an external `Optional<byte[]>` authority:
an empty result leaves the attempt open for Object Store retry, while a present
value still passes the frozen inline/length/SHA-256 validation in
`WorkerPhysicalPublishExecutor`. The combined one-shard and fair fleet
entrypoints also compose bound due/Claim/Publish, source application and
physical dispatch; Kafka and Pulsar source factories can now bind the optional
physical executor into the complete Worker graph.

`WorkerShardRuntimePhysicalLookupTest` covers the bounded no-source path, and
the focused test, full `check`, `compileRealKafka` and `compileRealPulsar`
checks passed. This closes source-position-to-local-ledger composition only.
The physical result's signed `PUBLISH_OUTCOME` remains an append/source-apply
handoff, and the real-client harnesses were not changed or rerun. Live payload,
Owner/Assignment and channel authority, destination Broker append/ACK,
Outcome source application, response-loss/crash resolution, multi-broker
failover and release gates remain open.

### 2026-08-15 typed Kafka K2 read-committed receipt evidence implementation note

Delay commit `3c7128eb6caecc50f3d6f4865ed2cdfa2838ad8a` adds the source-locked
Kafka K2 receipt-evidence bridge. `KafkaTransactionalPublishEvidence` builds
the closed `KAFKA_TRANSACTIONAL_RECEIPT` / `VERIFIED_PUBLISHED` branch only
after binding the exact receipt cursor to the mapped lane, lane incarnation,
receipt TopicId, partition, non-zero evidence generation, receipt offset and
LSO. The branch also carries the Publish Attempt identity, prepared hash,
target Broker resource/partition, transactional identity digest and the exact
keyed receipt-record digest. The record digest is
`SHA-256("nereus-delay-kafka-receipt-wire-record-v1\\0" || LP32(key) ||
LP32(value))`.

`KafkaClientArtifactTransactionalReceiptEvidenceProvider` creates a fresh
source-locked consumer with `isolation.level=read_committed`, seeks to the
guarded producer receipt offset, validates Fetch v13 evidence and the exact
TopicId/topic/partition, requires an LSO strictly covering the receipt, and
returns the typed branch. The transactional transport decodes that result and
requires the Kafka transactional-receipt kind, `VERIFIED_PUBLISHED` status and
Publish Attempt ownership. A missing or invalid reread stays `UNKNOWN`; it is
never converted to `DEFINITIVELY_NOT_PUBLISHED`. The same provider is retried
after a commit/EndTxn runtime failure, so a lost commit response can resolve
only through the fresh guarded `read_committed` receipt.

The real K2 smoke binds this provider for the initial, stale-incarnation and
replacement paths and decodes every returned `PUBLISHED` evidence value. The
focused typed-evidence test, full `check` and exact `compileRealKafka` gate
passed. The dedicated three-broker failover receipt used Kafka source
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786806083-13395`, and ports
`19985,19986,19987`:

```text
K2 broker failover commit returned PUBLISHED: typed KAFKA_TRANSACTIONAL_RECEIPT evidence and read_committed target+receipt pair
K2 broker failover smoke passed: target-plus-receipt transaction crossed broker-1 failover and exact read_committed records were verified
Kafka K2 broker failover E2E passed: target-plus-receipt transaction crossed broker-1 failover with read_committed resolution.
```

This closes positive typed K2 receipt resolution for the direct real-client
adapter smoke, including the exercised EndTxn response-loss/failover cut. It
does not bind the checked-in real Worker harness to the physical
due/Claim/Publish executor, prove Fetch response-loss or retention-floor
recovery, provide live Profile/credential/channel/Object Store authority,
apply a signed `PUBLISH_OUTCOME` through source ordering, prove Pulsar
multi-broker equivalence, or satisfy the V1 release gates.

### 2026-08-15 typed Pulsar `PULSAR_SEND_ACK` direct destination implementation note

Delay commit `4f2297e1dc593f8b5e16f7733e6ed1109544cb4a` adds the first real P1
destination binding that returns the Registry's typed Pulsar publication
branch. `PulsarSendAckEvidence` emits
`PULSAR_SEND_ACK`/`VERIFIED_PUBLISHED` with the exact target resource and
partition, ledger/entry, normalized batch index, broker persistence time,
caller-owned producer-name SHA-256, guarded sequence ID, Publish Attempt
identity, prepared-publish hash and the P1 authenticated response SHA-256.
The response digest is taken directly from
`GuardedSendSuccessEvidence.authenticatedResponseCommandSha256()`; it is not
manufactured from a local callback or an opaque result wrapper.

`PulsarClientArtifactDestinationTransport` requires the source-bound adapter
overload and the exact prepared hash. The request-only overload remains
`CAPABILITY_UNAVAILABLE`, because it cannot bind a retained Prepared Publish.
Before constructing typed evidence it checks the pinned request identity,
`GuardedMessageId`, expected `TopicResourceGuard`, topic/partition/attestation,
`MessageIdAdv` ledger/entry/batch identity and the corresponding guarded
success fields. Non-batch P1 messages are normalized to index `0`, size `1`.
Any missing or divergent proof remains `UNKNOWN`; this slice does not turn a
guard error into a definitive not-published mutation without the separate
typed guard-rejection branch.

The focused `PulsarSendAckEvidenceTest`, full `check`, and exact
`compileRealPulsar` gate passed. The source-qualified real P1 E2E used Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values `57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`
(`pulsar-client-original`),
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`
(`pulsar-client-api`) and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`
(`pulsar-common`), image
`sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d5`,
Compose project `nereus-delay-pulsar-e2e-1786807647-30858`, and ports
`20305,20306`. The typed destination smoke receipt was:

```text
Pulsar destination typed-evidence smoke passed: topic=persistent://public/default/p1-destination-30858, ledger=11, entry=0, batchIndex=0, sequence=0, brokerPersistenceTime=1786807670952
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, source-bound typed destination SEND ACK/payload readback, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, ACK handoff, and broker-restart resume.
```

This closes positive typed Pulsar direct-destination evidence and exact
payload readback on the checked-in standalone P1 service. It does not yet
bind the real Worker harness's due/Claim/Publish turn to this destination
transport, prove Pulsar multi-broker failover, provide typed guard-rejection
or response-loss/crash resolution, or satisfy live Profile/credential,
Owner/Assignment, Object Store, checkpoint/quiescence and V1 release gates.

### 2026-08-16 source-applied Pulsar Worker physical Publish and typed Outcome implementation note

Delay commit `cb309d82` binds the real P1 Worker smoke to one bounded
source-applied physical Publish vertical. With
`-PpulsarWorkerDestinationTopic`, the smoke first sends the physical Schedule
to the guarded Shard Log and retains its exact `PulsarSourcePosition`. The
Worker source loop then applies that Schedule, derives the typed Lane
incarnation from that source position, and activates the Lane using the
bounded smoke authority's exact channel/certificate/cursor projections.

The same source appender records a signed `PUBLISH_ADMISSION` after the local
Claim and prepared descriptor are built. `WorkerShardRuntime
runSourceBoundPhysicalPublish(...)` source-applies that Admission, reloads the
persisted `PUBLISHING` ledger, and invokes `WorkerPhysicalPublishExecutor`
with the source-aware `PulsarClientArtifactDestinationTransport`. The P1
transport returns the typed `PULSAR_SEND_ACK` branch; the signed Outcome
factory queues a source-log `PUBLISH_OUTCOME`, which the same Worker source
loop applies before the smoke verifies `PUBLISHED` and reads the exact payload
back through a guarded destination consumer.

The current-source receipt used P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, image
`sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
Compose project `nereus-delay-pulsar-e2e-1786809927-56224`, and ports
`20515,20516`:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=22/3, typed PULSAR_SEND_ACK target ledger/entry=23/0, Outcome source ledger=22/4, exact payload readback
Pulsar Worker source-applied physical publish passed: Admission source ledger=33/2, typed PULSAR_SEND_ACK target ledger/entry=34/0, Outcome source ledger=33/3, exact payload readback
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, source-bound typed destination SEND ACK/payload readback, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, source-applied physical publish with typed Outcome and payload readback, ACK handoff, and broker-restart resume.
```

This closes the positive source-applied PUBLISHING-to-typed-Outcome path for
the direct P1 Worker smoke. The Claim, readiness certificate, credential
projection and payload are still supplied by the bounded smoke authority; the
run does not invoke the live due/Claim/Object Store provider graph or
`runDueClaimPublishPhysicalTurn`. It is a single standalone Broker restart
receipt, not Pulsar multi-Broker failover, crash/response-loss resolution,
runtime/milestone PASS, or V1 release readiness. Oxia Route Worker authority
was disabled for this receipt (`NEREUS_DELAY_PULSAR_WITH_OXIA=0`).

### 2026-08-16 source-applied Kafka Worker physical Publish and typed Outcome implementation note

Delay commit `112522e6` binds the real K1 Worker smoke to the corresponding
source-applied physical Publish vertical. With
`-PkafkaWorkerDestinationTopic`, the smoke produces a physical Schedule to the
guarded Kafka Shard Log and immediately reads the exact record back through a
guarded Fetch v13. That readback completes the optional Produce leader epoch
when the broker response omits it; the Worker then uses the authenticated
cluster/topic/shard/offset/append-time identity, while rejecting conflicting
known leader epochs, for the Lane incarnation and source-bound Admission
match.

After the source loop applies the Schedule, the smoke builds the bounded typed
Lane/channel/certificate projection and appends a signed `PUBLISH_ADMISSION`
for the local Claim and prepared descriptor. `WorkerShardRuntime
runSourceBoundPhysicalPublish(...)` source-applies that Admission, reloads the
persisted `PUBLISHING` ledger, and invokes
`KafkaClientArtifactTransactionalDestinationTransport` only with the exact
source-bound position and prepared hash. The K2 adapter commits the target and
receipt atomically; a fresh `read_committed` receipt yields typed
`KAFKA_TRANSACTIONAL_RECEIPT` evidence, a signed source `PUBLISH_OUTCOME`
closes the attempt as `PUBLISHED`, and the smoke reads the exact destination
payload back.

The current-source three-broker receipt used K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose project `nereus-delay-kafka-e2e-1786812109-79794`, and ports
`21092,21093,21094`:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=2, Outcome source offset=4, exact payload readback
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

`./gradlew check`, exact `compileRealKafka` against the locked client JAR and
the full real-client script passed. This is positive three-broker source/
Worker/K2 evidence, not a V1 release or milestone PASS: Claim, readiness,
credential and payload inputs remain bounded smoke authority, the run does not
invoke the live due/Claim/Object Store provider graph or
`runDueClaimPublishPhysicalTurn`, Oxia Route authority was disabled, and the
full crash/response-loss matrix, multi-shard placement, checkpoint/quiescence
and §23.5 release gates remain open.

### 2026-08-16 provider-driven Kafka and Pulsar Worker physical Publish implementation note

The previous notes above are retained as historical receipts. Delay commit
`e5cae7b8e7d9988cc6dca516212d011d49fea5fa` binds the real K1 Worker smoke to
the active-owner typed scheduling, Claim Handoff, Publish Admission and
`WorkerPublishPreparationCoordinator` graph. The smoke now calls
`runDueClaimPublishPhysicalTurn(...)`, source-applies the provider-driven
Claim and Admission, and then reaches the already source-bound physical
Publish/Outcome path. The companion Pulsar implementation is commit
`3c6e605a33cea2de85fce473af740b5e05fcf74e`.

For Kafka, the locked K1 source was
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256 was
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image was
`sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and the three-broker receipt used Compose project
`nereus-delay-kafka-e2e-1786814042-841`, ports `21492,21493,21494`:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, and final checkpoint
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

For Pulsar, the locked P1 source was
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256 was
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values were
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, image
was `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`,
and the receipt used Compose project
`nereus-delay-pulsar-e2e-1786814719-7983`, ports `21515,21516`:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=22/3, typed PULSAR_SEND_ACK target ledger/entry=23/0, Outcome source ledger=22/4, exact payload readback
Pulsar Worker source-applied physical publish passed: Admission source ledger=33/2, typed PULSAR_SEND_ACK target ledger/entry=34/0, Outcome source ledger=33/3, exact payload readback
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, source-bound typed destination SEND ACK/payload readback, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, source-applied physical publish with typed Outcome and payload readback, ACK handoff, and broker-restart resume.
```

Both `./gradlew check` and the exact locked-client compile gates passed. This
is now positive provider-driven due-to-Claim-to-physical-Publish evidence for
the two real Worker smokes. The graph still uses bounded in-memory authority
and deterministic preparation inputs; it does not prove live Profile/
credential/Object Store/catalog authority, crash or response-loss resolution,
Pulsar multi-Broker failover, multi-shard placement, checkpoint/quiescence,
or the §23.5 V1 release gates. Pulsar ran with
`NEREUS_DELAY_PULSAR_WITH_OXIA=0`; neither receipt is a runtime, milestone or
release PASS.

### 2026-08-16 real Oxia authority provider-driven Pulsar receipt

The same provider-driven P1 E2E was rerun with
`NEREUS_DELAY_PULSAR_WITH_OXIA=1`, so Worker assignment publication,
session-bound ownership and the physical Publish graph used the real Oxia
backend rather than the in-memory authority. The locked Oxia checkout was
`37a17bef17202d5fd6e23282da5fd26d94865484`; P1 remained
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
image `sha256:892add226a105fb04b6df05df2c58f43e49f76647d39ed73944fcfc9ea1cb3d`.
The receipt used Compose project `nereus-delay-pulsar-e2e-1786815185-13398`,
Pulsar ports `21615,21616`, and Oxia port `16658`:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker source-applied physical publish passed: Admission source ledger=24/3, typed PULSAR_SEND_ACK target ledger/entry=25/0, Outcome source ledger=24/4, exact payload readback
Pulsar Worker source-applied physical publish passed: Admission source ledger=35/2, typed PULSAR_SEND_ACK target ledger/entry=36/0, Outcome source ledger=35/3, exact payload readback
Pulsar Worker authority smoke passed: real Oxia session-bound lease
Pulsar signed Route -> guarded SUBSCRIBE barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: generation=16, barrier=22/0, routeRevision=1, assignmentRevision=1, source=22/1, ACK, final checkpoint
Pulsar P1 real-client E2E passed: guarded send, stale resource rejection, source-bound typed destination SEND ACK/payload readback, guarded source replay, signed mutation append/replay/ACK, signed Route barrier/assignment/source ACK, Broker timestamp, Worker recovery/apply, source-applied physical publish with typed Outcome and payload readback, ACK handoff, and broker-restart resume.
```

This closes positive real-Oxia authority evidence for the provider-driven P1
Worker path across a standalone Broker restart. It remains a single Oxia
service and single Pulsar Broker receipt: multi-Broker failover, Oxia
failover/partition behavior, crash/response-loss resolution, live
Profile/credential/Object Store/catalog authority, placement,
checkpoint/quiescence and §23.5 release gates remain open.

### 2026-08-16 real Oxia authority provider-driven Kafka receipt

The provider-driven Kafka Worker E2E was rerun with
`NEREUS_DELAY_KAFKA_WITH_OXIA=1`. Worker assignment/ownership and the
due-to-Claim-to-physical-Publish graph used the real Oxia backend. The locked
K1 source was
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. The receipt used Compose
project `nereus-delay-kafka-e2e-1786815566-17636`, Kafka ports
`21792,21793,21794`, and Oxia port `16659`:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=2, Outcome source offset=4, exact payload readback
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka source/Worker/K1/K2 real-client E2E passed: guarded source ACK/restart, assignment recovery to RocksDB Worker apply before and after broker-1 failover, source-applied physical publish with typed KAFKA_TRANSACTIONAL_RECEIPT Outcome and payload readback, same-topic Worker resume after failover, K1 identity/failover, and K2 atomic target+receipt commit, abort, and delete/recreate fence.
```

This closes positive real-Oxia authority evidence for the provider-driven
Kafka Worker path across the three-broker K1/K2 run and broker-1 survivor
cut. It remains one standalone Oxia service; Oxia failover/partition
behavior, crash/response-loss resolution, live Profile/credential/Object
Store/catalog authority, multi-shard placement, checkpoint/quiescence and
§23.5 release gates remain open.

### 2026-08-16 real Oxia accepted-Route Kafka failover receipt

The accepted-Route Kafka Worker failover-only E2E used
`NEREUS_DELAY_KAFKA_WITH_OXIA=1`,
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER=1` and
`NEREUS_DELAY_KAFKA_ROUTE_FAILOVER_ONLY=1`. Locks were K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. The receipt used Compose
project `nereus-delay-kafka-e2e-1786815918-21809`, Kafka ports
`21892,21893,21894`, and Oxia port `16660`:

```text
Kafka signed Route -> guarded Fetch barrier -> Oxia Worker assignment -> RocksDB apply/checkpoint smoke passed: fetch=v18, lso=1, routeRevision=1, assignmentRevision=1, barrierOffset=1, sourceOffset=2, commitSync ACK, accepted-route broker failover, final checkpoint
Kafka accepted-route broker failover E2E passed: Route-bound Worker applied and ACKed after broker-1 failover, then released its final checkpoint and Oxia assignment.
```

This closes positive real-Oxia accepted-Route Worker evidence across a
three-broker Kafka broker-1 failover cut. It remains a failover-only
single-shard receipt: Oxia failover/partition behavior, crash/response-loss
resolution, live Profile/credential/Object Store/catalog authority,
multi-shard placement, physical delayed Publish and §23.5 release gates
remain open.

### 2026-08-16 locked P1 two-Broker Worker failover receipt

The checked-in `e2e/run-pulsar-multi-broker-failover-e2e.sh` harness now runs a
same-topic Worker through a real Broker-1 stop and Broker-2 resume. The
Compose topology is deliberately bounded to one ZooKeeper, one BookKeeper and
two P1 Brokers. Internal broker forwarding uses the bridge-network listener;
the host Worker uses the dedicated external listener and the P1 client
`listenerName=external` lookup path. The harness follows Admin owner redirects
and removes its containers, volumes and image on exit.

The accepted run used `NEREUS_DELAY_PULSAR_WITH_OXIA=1` with Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, image
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
Compose project `nereus-delay-pulsar-multi-e2e-1786819171-58253`, Pulsar
ports `21985,21986,21987,21988`, and Oxia port `16666`:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/3, typed PULSAR_SEND_ACK target ledger/entry=5/0, Outcome source ledger=3/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=3/0, active apply ledger/entry=3/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker authority smoke passed: real Oxia session-bound lease
Pulsar multi-Broker failover E2E passed: same-topic guarded Worker resumed through broker-2 after broker-1 stop, applied the source record, completed provider-driven physical Publish, ACKed the source and released its final checkpoint and owner assignment.
```

This is positive real-Oxia same-topic Worker failover evidence: Broker-1 was
stopped after the prepared source record, Broker-2 resumed the guarded source
and completed the source-applied physical Publish/readback path, and Broker-1
was then restarted. It is not production HA evidence: the run has one
BookKeeper and one ZooKeeper, does not fail over Oxia or metadata/storage
services, uses one single-shard topic and deterministic smoke profiles, and
does not prove crash/response-loss resolution, multi-shard placement, live
Profile/credential/Object Store/catalog authority, checkpoint/quiescence or
the §23.5 V1 release gates.

### 2026-08-16 real Gateway mTLS/RS256 network and Oxia durability receipt

The deployable Gateway composition is now exercised through a real network
boundary by `e2e/run-gateway-real-e2e.sh` and
`OxiaRealGatewayGrpcSmokeTest` (Delay commit `9a170837`). The harness creates
short-lived test certificates, starts `GatewayGrpcServer.mutualTls`, and uses
a Netty client certificate plus an RS256 JWT whose `cnf.x5t#S256` is bound to
that certificate. The generated Schedule request crosses the gRPC interceptor
and `GatewayGrpcService`; authentication, admission, durable prepared-byte
idempotency and digest-only audit are still separate layers.

The accepted run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786820415-72294`, Oxia port `16668`, and Gateway
port `22350`. Two identical authenticated requests produced the same public
outcome while the real Oxia scans proved one released admission record, one
quiescent idempotency record with one attempt, and two deduplicated audit
records. A mutated JWT signature was rejected before Semantic-Core
preparation. The run ended with `BUILD SUCCESSFUL` and the exact script lines:

```text
Gateway mTLS/RS256 network E2E passed: authenticated Schedule and invalid JWT rejection
Gateway Oxia durable E2E passed: admission released, one idempotency attempt, and two digest-only audit events
Dockerized Gateway real-service smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

This is a bounded Gateway/authentication and durable-record receipt. The
smoke uses deterministic Semantic-Core/submission doubles and a local definite
non-submission outcome; certificate deployment/rotation, live transport
publish, admission HA/session churn, load, crash cuts, multi-language vectors
and the release gates remain open.

### 2026-08-16 Gateway server-restart idempotency revalidation

Commit `232ce29d` extends the real Gateway receipt across a server restart.
After the first authenticated Schedule response, the harness closes the
first `GatewayGrpcServer`, starts a second instance on the same port with the
same Oxia-backed records, and sends the exact request again through a new
mTLS channel. The second response is byte-identical; the Semantic-Core
preparation and submission counters remain one, so a durable reread does not
recreate ownership or a physical attempt.

The accepted revalidation used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786820937-77983`, Oxia port `16669`, and Gateway
port `22351`. It ended with `BUILD SUCCESSFUL` and the exact output:

```text
Gateway mTLS/RS256 network E2E passed: authenticated Schedule and invalid JWT rejection
Gateway restart/idempotency E2E passed: server restarted and returned the exact durable outcome without a second attempt
Gateway Oxia durable E2E passed: admission released, one idempotency attempt, and two digest-only audit events
Dockerized Gateway real-service smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

This closes only single-process restart/reconnect durability for the Gateway
composition. Multi-process HA/session churn, certificate operations, load,
crash/response-loss cuts and live Kafka/Pulsar publication remain open.

### 2026-08-16 Gateway two-server Oxia CAS race receipt

Commit `1213650b` adds a bounded concurrent race between two independent
Gateway service compositions. The servers use separate Oxia sessions and
separate admission/idempotency/audit wrappers, while sharing the same
tenant-scoped durable key prefix. Concurrent identical requests are sent over
two mTLS channels; one coordinator obtains the physical attempt and the
other observes the protocol's exact in-flight/uncertain boundary. A settled
request after the race rereads the durable aggregate, and scans require one
attempt, one aggregate outcome and released admission.

The accepted run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-gateway-e2e-1786821521-84089`, Oxia port `16670`, Gateway ports
`22353,22354`, and printed:

```text
Gateway two-server CAS race E2E passed: independent Gateway servers converged on one durable physical attempt
```

This is bounded independent-client concurrency in one test JVM. It does not
establish production multi-process HA, Oxia failover, notification/session
churn, certificate deployment/rotation, load, crash/response-loss handling or
live Kafka/Pulsar publication.

### 2026-08-16 Route notification stream recovery after Oxia session rotation

Commit `6a64ca894928a9a6f210129e2567b02f7df1329f` completes the bounded
notification recovery seam for `OxiaSignedRouteSnapshotProvider`. The provider
still treats `refresh()` as the explicit authority-recovery boundary: it first
revalidates or rotates the session-fenced marker, then the session composition
closes its old watch client, creates a new client with the same namespace and
watch identity, and registers the provider callback on the new notification
manager. The cache is rebuilt from the signed head/event stream after that
subscription is installed. A raw `SyncOxiaClient` keeps the Oxia client's own
retry behavior through the no-op compatibility hook and is never registered a
second time.

The real-service gate uses two-second session timeouts and
`NEREUS_DELAY_OXIA_ROUTE_RESTART_PAUSE_SECONDS=5`, then publishes a second
revision after one explicit provider refresh. Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:05d66cf3117d24b358baee21fb87caa001c99bec2f734ea9ce2549f7675d085a`,
Compose `nereus-delay-v1-oxia-e2e-1786822655-96457`, and host port `16675`
were used. The receipt was:

```text
Oxia Route notification restart recovery passed: revision=2, session rotated, notification stream resumed without a second provider refresh
Dockerized Oxia Route notification restart smoke passed: session rotation and notification stream recovery
```

This is a positive single-node restart/session-rotation and resumed-notification
receipt. It is not evidence of multi-node Oxia failover or partition healing,
multi-shard Route activation, native Broker eligibility, live Profile/credential
authority, production transport publication, crash/response-loss resolution or
the §23.5 release gates.

### 2026-08-16 Gateway certificate replacement and channel revalidation

Commit `cbe895e1` adds a bounded deployment-replacement cut to the Gateway
network harness. It generates independent old and rotated CA/server/client
sets. The old server accepts the first request and persists the durable
idempotency result. After same-port replacement, the new server trusts only
the rotated CA and presents the rotated server certificate. An old client is
configured to trust the rotated server but presents the old client certificate,
so the handshake is rejected by the new trust bundle. A new client certificate
and a new RS256 JWT with matching `cnf.x5t#S256` authenticate and reread the
exact old outcome; the core and coordinator counters remain one.

The source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:054bb7d13cd9c3d7a6c4dd0b70d5820b6ece2115e840cf47ff8ea0e679a9248c`,
Compose `nereus-delay-gateway-e2e-1786823102-1813`, Oxia port `16677` and
Gateway port `22356`:

```text
Gateway certificate rotation E2E passed: old mTLS client rejected and new certificate reread the exact durable outcome
```

This is positive same-port replacement and authenticated-channel
revalidation, not hot certificate reload, staged rollback, revocation/CRL or
OCSP proof, multi-process Gateway HA, load, crash/response-loss resolution,
live Kafka/Pulsar publication or the §23.5 release gates.

### 2026-08-16 Gateway durable admission/idempotency recovery after Oxia session churn

Commit `f9fa48b7` makes the deployable Gateway durable-record composition
session-fenced. `SessionBoundOxiaGatewayRecordClient` wraps the three Oxia
record surfaces with the exact ephemeral marker from
`OxiaSyncOwnerLeaseBackend.ClientHandle`; every `get` and `put` is checked both
before and after the operation. Marker loss or metadata/version drift is a
transport-unavailable boundary, not a retry permission. The old composition is
closed to durable I/O until an operator creates a new set of client handles;
the new composition may reread durable idempotency state but cannot recreate a
one-shot attempt permit.

Test commit `241068fd` adds the gated network test
`gatewayDurableRecordsRecoverAfterOxiaSessionChurn`. It keeps the old Gateway
server listening while a two-second Oxia session expires during a five-second
service stop. The stale admission and idempotency wrappers fail with the
session-fenced exception and the old mTLS RPC maps that boundary to
`UNAVAILABLE`. A new three-handle composition then serves the same durable
prefix and returns the exact prior outcome with one preparation, one physical
attempt, zero live admission leases and two digest-only audit records.

The source-locked receipt used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:e36ced8f25cff4ea67e61a1dd392668d53b5ac79ffe992587d49548cf038a059`,
Compose `nereus-delay-gateway-e2e-1786824181-13578`, host ports `16678` and
`22357`, and ended with `BUILD SUCCESSFUL` / `11 actionable tasks: 11 executed`:

```text
Gateway Oxia session churn E2E passed: stale durable sessions failed closed and recovery reread the exact durable outcome
Dockerized Gateway Oxia session churn smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

This is a bounded single-node session-rotation and controlled-recomposition
proof. It is not transparent reconnect, multi-node Oxia failover, production
Gateway HA, crash/response-loss, load, live Broker publication or release
evidence.

### 2026-08-16 Gateway recovery across a real multi-node Oxia DataServer leader stop

Commit `43493a709e4041e94c7f4f270a25b2725534ab59` adds an isolated
three-Coordinator/three-DataServer Oxia deployment in
`e2e/docker-compose.oxia-cluster.yml` and the gated
`e2e/run-oxia-multi-node-gateway-e2e.sh` run. The harness registers a
three-replica `default` namespace through the real admin API, discovers the
current shard leader, and bootstraps the Gateway against a surviving
DataServer. The Gateway server, three Oxia client handles and all durable
admission/idempotency/audit wrappers remain alive across the cut.

The source-locked run used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Compose project
`nereus-delay-oxia-cluster-gateway-e2e-1786825431-27266`, host ports
`16691,16692,16693` for Coordinators, `16681,16682,16683` for DataServers and
`22358` for Gateway. The run stopped `ds-3`, observed `ds-1` as the successor,
revalidated all three session markers, reread the exact byte-identical public
outcome and required one preparation, one physical attempt, zero admission
leases and two audit records:

```text
Oxia shard successor leader: ds-1
Gateway multi-node Oxia failover E2E passed: session-bound clients preserved the exact durable outcome after the shard leader stopped
Oxia multi-node Gateway failover E2E passed: session-bound Gateway reread the exact durable outcome after leader stop
Dockerized Oxia multi-node Gateway failover smoke passed for Oxia 37a17bef17202d5fd6e23282da5fd26d94865484
```

This opens only the bounded multi-node DataServer leader-stop/session
preservation evidence. It does not change the explicit fail-closed policy for
a total outage, prove transparent reconnect after all durable replicas are
unavailable, establish Gateway HA/load or crash/response-loss resolution, or
promote live Kafka/Pulsar publication and release readiness.

### 2026-08-16 Gateway STARTED CAS response-loss recovery

Commits `a120b6bd` and `7adb95f0` close the durable Gateway state-machine gap
after a CAS response is lost immediately after a `STARTED` attempt write. Both the
in-memory conformance store and `OxiaGatewayIdempotencyStore` still refuse to
reconstruct the one-shot `GatewayAttemptOwnershipPermit`; a caller before the
attempt's `uncertaintyAtEpochMs` only observes the active attempt. Once that
trusted deadline has passed, a same-key caller decodes the already persisted
prepared bytes, CASes the exact attempt to `UNCERTAIN` with the canonical NDR1
aggregate, and returns no permit. A concurrent CAS race is reread rather than
treated as permission to send. The same deadline recovery is applied to an
explicit retry attempt, and outcome completion preserves its
`retryRequestId/retryRequestHash` so a repeated retry request remains
`EXISTING_RETRY` rather than becoming a stale precondition.

The deterministic regression is
`OxiaGatewayIdempotencyStoreTest.attemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit`.
It injects a committed-then-lost response, checks the pre-deadline `ACTIVE`
record remains without an aggregate, advances trusted time to the exact
deadline, and verifies `QUIESCENT/UNCERTAIN`, one persisted attempt and a
canonical managed outcome without a physical resend. The companion
`retryAttemptCasResponseLossConvergesToUncertainAfterDeadlineWithoutPermit`
test covers the explicit retry path and preserved retry identity.

Focused verification:

```text
./gradlew test --tests io.nereusstream.delay.gateway.OxiaGatewayIdempotencyStoreTest --no-daemon --console=plain
BUILD SUCCESSFUL in 59s
11 actionable tasks: 3 executed, 8 up-to-date

./gradlew check --no-daemon --console=plain
BUILD SUCCESSFUL in 1m 22s
21 actionable tasks: 3 executed, 18 up-to-date
```

This closes the local durable STARTED-CAS response-loss convergence cut for
both first attempts and explicit retries only.
It does not by itself provide a real Oxia fault-injection receipt, transparent
Gateway reconnect, physical transport response-loss/crash resolution,
multi-node placement or Gateway HA, live Kafka/Pulsar publication, or V1
release readiness.

### 2026-08-16 real Oxia Gateway STARTED CAS response-loss receipt

Test commit `1ce8b7e604ca969adabd7372e80ce04f96e5b45a` adds the source-bound
network test `gatewayRecoversAfterCommittedOxiaAttemptResponseLoss`. It uses
the deployable Gateway mTLS/RS256 path and three real
`SessionBoundOxiaGatewayRecordClient` handles. Only the idempotency handle is
wrapped by `ResponseLossOnceRecordClient`: the prepare write succeeds, the
real Oxia `STARTED` CAS succeeds durably, and the wrapper then raises a
client-side exception before returning its result. The first request therefore
returns managed `ENQUEUE_UNCERTAIN` with no physical submission. After the
test clock advances beyond `uncertaintyAtEpochMs`, the same request performs
the exact-record recovery CAS, returns byte-identical output, and still has no
physical submission.

The source-locked receipt used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:db1b0409c36cbf16bc21d63f74932a0f9f188f5d0101b9b398e6b90de3e01cc`,
Compose `nereus-delay-gateway-e2e-1786827281-47103`, host ports `16695` and
`22360`, and ended with `BUILD SUCCESSFUL in 1m 14s` / `11 actionable tasks:
11 executed` for the six-test class (two opt-in tests skipped):

```text
Gateway Oxia STARTED response-loss E2E passed: committed attempt was reread after deadline as exact UNCERTAIN without a second physical submission
```

The real Oxia scans prove one quiescent idempotency record, one uncertain
attempt, a non-null aggregate, zero admission leases and four audit records.
This is durable post-commit response-loss recovery with a controlled client
response cut, not raw socket fault injection. It does not promote physical
Kafka/Pulsar response-loss/crash resolution, transparent Gateway reconnect/HA,
load, multi-shard placement or the V1 release gates.

### 2026-08-16 real Oxia Gateway RETRY_UNCERTAIN response-loss receipt

Commit `bcac733ae7e48776ce7d427d66643d21a6dd2a7d` closes the explicit retry
state transition exposed by the real network test. When
`OxiaGatewayIdempotencyStore.startRetry` creates a new `ACTIVE` attempt,
`GatewayIdempotencyRecordV1.withAttempt` now drops the prior uncertain
aggregate. This prevents the first retry response from replaying the prior
attempt's physical identity; the retry aggregate is written only when the
new retry attempt is resolved or recovered.

`gatewayRecoversAfterCommittedOxiaRetryAttemptResponseLoss` uses the real
mTLS/RS256 Gateway and three session-bound Oxia record clients. The controlled
wrapper loses the response after the real Oxia `STARTED` CAS for the first
attempt and again after the explicit retry `STARTED` CAS. The test recovers the
first attempt, sends `RetryUncertain` with its exact prior-attempt precondition,
and verifies the retry response is byte-identical before and after the retry
deadline, with one preparation and zero physical submissions.

The source-locked receipt used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, image
`sha256:ba41122a1fa21cdcfb1c2680e81a3f14519d6f1f9213f82dfa284fb3e792428d`,
Compose `nereus-delay-gateway-e2e-1786828250-57299`, host ports `16697` and
`22362`, and ended with `BUILD SUCCESSFUL in 16s` / `11 actionable tasks: 11
executed` for the seven-test class (two opt-in tests skipped):

```text
Gateway Oxia RETRY_UNCERTAIN response-loss E2E passed: committed retry attempt was reread after deadline as exact UNCERTAIN without a second physical submission
```

The final real-Oxia scans prove two uncertain attempts in one quiescent record,
a non-null aggregate, zero admission leases and eight audit records. This is
durable explicit-retry response-loss recovery with a controlled client-side
response cut, not raw socket fault injection. It does not promote physical
Kafka/Pulsar response-loss/crash resolution, transparent Gateway reconnect/HA,
load, multi-shard placement or the V1 release gates.

### 2026-08-16 Kafka K2 committed EndTxn response-loss receipt

The K2 transport now has a source-bound real-client receipt for the case where
the broker commits the target-plus-keyed-receipt transaction but the producer
does not receive the local `EndTxn` result. Commit `376252bae0faf6f2d5120e223886b3af8a54e636`
adds a response-loss-only harness mode whose wrapper delegates the real
`commitTransaction()` and throws after it returns. It does not alter the
production transaction ordering or make a second transaction.

The existing uncertainty branch creates a fresh `read_committed` evidence
consumer, checks the exact receipt and target, and accepts only typed
`KAFKA_TRANSACTIONAL_RECEIPT` evidence with the required Fetch/LSO proof. The
locked run used Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, image
`sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786828912-64477`, ports
`19569,19570,19571`, and printed:

```text
K2 committed response-loss smoke passed: real EndTxn committed the exact target-plus-receipt pair, the local response was discarded, and typed read_committed evidence resolved PUBLISHED
Kafka K2 committed response-loss E2E passed: real EndTxn commit was followed by local response loss and exact read_committed typed receipt resolution.
```

This closes only the controlled client-side post-commit K2 response-loss
receipt. It is not raw packet-loss, Broker crash/failover, generic Fetch
response-loss, LSO/retention-floor, or V1 release evidence.

### 2026-08-16 Pulsar destination committed SEND response-loss receipt

The real Pulsar destination transport now accepts an optional
`PublishEvidenceProvider` for an uncertain guarded `sendAsync()` completion.
The provider is a source-bound seam: it must prove the same request and return
typed `PULSAR_SEND_ACK` evidence; otherwise the transport stays `UNKNOWN`.
The transport still uses the existing exact guard/attestation checks on normal
success, and the recovery result must carry the same `publishAttemptId` as a
business mutation.

The real-client smoke implements a bounded receipt with a test-only proxy. The
underlying P1 client persists the exact guarded payload and yields a real
`GuardedMessageId`; the proxy captures that ID and replaces only the local
completion with a failure. The provider validates the resource guard, topic,
partition, ledger/entry, batch index/size and guarded SEND attestation, then
builds `PULSAR_SEND_ACK`. A guarded consumer reads the exact payload after the
transport returns `PUBLISHED`. `PulsarAttemptJournal` remains a local protocol
seam and is intentionally not presented as real transport durability by this
receipt.

The source-locked run used Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose project `nereus-delay-pulsar-e2e-1786829967-75545`, and ports
`21885,21886`. It printed:

```text
Pulsar committed response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved PUBLISHED
Pulsar destination committed response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and exact guarded payload readback.
```

This receipt is narrower than D6: it does not inject raw network packet loss,
exercise Attempt Journal recovery, stop a producer/client/Broker during an
in-flight SEND, prove multi-Broker failover or activate the Pulsar destination
release profile. The shared Dockerfile build-context copy fix is harness-only.

### 2026-08-16 Pulsar Worker source ACK response-loss receipt

The source Worker ACK boundary now has an explicit bounded retry receipt. The
test-only `GuardedConsumer` proxy delegates the receipt-enabled synchronous
`acknowledge` call, then throws after the real Broker receipt returns. The
adapter keeps the exact `inFlight` message and reports `ACK_UNKNOWN`; the
Worker turn runner accepts that retryable status and calls the same
acknowledgement again. The `SourceApplyCoordinator` retains its applied
outcome, so the retry does not re-run Store apply or advance the native cursor
early.

The source-locked run used Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose project `nereus-delay-pulsar-e2e-1786830626-82754`, and ports
`21887,21888`. It printed:

```text
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker source ACK response-loss smoke passed: real ACK was accepted before the local response was discarded, and the same source record was ACKed on the next bounded Worker turn
Pulsar Worker source ACK response-loss E2E passed: real ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

This is narrower than D6: it is controlled client-side loss after a real ACK
receipt, not raw network packet loss, process/consumer/Broker crash recovery,
multi-Broker failover or full source-ACK release evidence.

### 2026-08-16 Pulsar Worker source-applied destination response-loss receipt

The Worker physical bridge now uses the same source-bound destination recovery
provider as the standalone P1 destination smoke. The guarded SEND's real
`GuardedMessageId` is captured by a test-only wrapper before its local
completion is discarded. Exact guard/attestation, ledger/entry and batch
coordinates are checked, typed `PULSAR_SEND_ACK` is returned, and the
`WorkerPhysicalPublishExecutor` hands the verified result to the source log's
typed `PUBLISH_OUTCOME`. The receipt also verifies the closed publish attempt
and exact destination payload readback.

The source-locked run used Pulsar
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
SHA-256 values
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, base
image `eclipse-temurin:21-jre@sha256:371da296b8cb74c7e53fbe7083d5374befc0011b493231d97d45fa789915e434`,
P1 image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Compose project `nereus-delay-pulsar-e2e-1786830983-86815`, and ports
`21889,21890`. It printed:

```text
Pulsar Worker destination response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved the source-applied PUBLISHED Outcome
Pulsar Worker source-applied physical publish passed: Admission source ledger=9/3, typed PULSAR_SEND_ACK target ledger/entry=10/0, Outcome source ledger=9/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker destination response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and the source-applied Outcome completed.
```

This is narrower than D6: controlled client-side response loss after real
physical persistence is covered, but raw network loss, process/Broker crash
between persistence and Outcome, multi-Broker failover, Attempt Journal
recovery and release activation remain open.

### 2026-08-16 Pulsar Worker UNKNOWN Publish Admission response-loss receipt

Delay commit `88d58c02` adds a bounded test-only
`AdmissionResponseLossMutationAppender` around the real
`PulsarClientArtifactShardLogMutationAppender`. On its first real
`PERSISTED` result it returns `UNKNOWN` without writing a second mutation;
subsequent appends, including `PUBLISH_OUTCOME`, pass through unchanged. The
Worker's existing `recoverUnknownPublishAdmission` path must locate the exact
canonical mutation from the guarded source and use that source position for
the physical PUBLISHING handoff.

The focused source-bound command was:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_WORKER_ADMISSION_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-worker-admission-response-loss-oxia-20260816 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The receipt locked P1 to
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
the distribution and three client artifact digests recorded in the runner
output, P1 image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Pulsar ran in project
`nereus-delay-pulsar-e2e-1786886923-27929` on `19679/19680`; Oxia ran in
`nereus-delay-pulsar-oxia-e2e-1786886923-27929` on `16657`.

The live receipt was:

```text
Pulsar Worker recovered UNKNOWN Publish Admission from exact source mutation: PulsarSourcePosition[shardId=ShardId[routeIncarnation=85d32917c2004c0ca801400cc3da8572, partition=0], brokerResourceIncarnation=[B@69a2b3b6, physicalTopic=persistent://public/default/p1-real-client-27929-worker-4e3cfaad-300e-437f-befa-1e3205c2d2a2, ledgerId=9, entryId=3, normalizedBatchIndex=0, batchSize=1, entryKind=NON_BATCH, brokerEntryTimestampEpochMs=1786886946158]
Pulsar Worker Publish Admission response-loss smoke passed: the real Shard Log mutation was persisted, its local append response was discarded, and exact source replay recovered the PUBLISHING admission
Pulsar Worker source-applied physical publish passed: Admission source ledger=9/3, typed PULSAR_SEND_ACK target ledger/entry=10/0, Outcome source ledger=9/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar Worker authority smoke passed: real Oxia session-bound lease
BUILD SUCCESSFUL in 15s
```

This is a controlled client-side committed-response loss after a real Broker
append. It closes the bounded real-Oxia Worker `UNKNOWN` Admission recovery
composition; it does not establish raw socket/process/Broker chaos,
multi-Broker source reactivation, combined Gateway failover, multi-shard
placement, checkpoint REAPING or V1 release activation.

### 2026-08-16 Route-driven multi-shard Worker placement receipt

Delay commit `e629a404` extends the real Oxia Route-to-Worker assignment
composition to two Route partitions. The first placement selects `worker-a`
with one available DB slot; the second placement consumes the reflected
committed capacity and selects `worker-b`. The assignment authority writes one
exact revision-CAS record per `ShardId`; the smoke rereads both through the
signed Route projection and withdraws both exact publications.

The source-bound run used:

```bash
NEREUS_DELAY_OXIA_E2E_PORT=16659 \
NEREUS_DELAY_E2E_GRADLE_USER_HOME=/tmp/nereus-delay-oxia-multishard-20260816 \
  bash e2e/run-oxia-real-service.sh
```

It locked Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`, project
`nereus-delay-v1-oxia-e2e-1786887413-34183`, endpoint `127.0.0.1:16659`, and
temporary image
`sha256:e05630a933783a3925150ad1a1ca38869249d06a9983a6a7c4ed1e0bef98c460`.
The result XML recorded two executed tests with no skips/failures/errors:

```text
Oxia signed Route -> multi-shard Worker placement smoke passed: routeRevision=1, shards=2, workers=2, session-bound CAS
BUILD SUCCESSFUL in 1m 20s
```

This closes only the Route/Assignment authority slice. It does not widen the
accepted design into a caller-created multi-shard runtime: native source
ownership, per-shard Owner Lease and catch-up, scheduler/Store/ACK wiring,
multi-Broker reactivation, raw chaos and release gates remain separate.

## Kafka Worker destination response-loss receipt

Run the focused Kafka Worker physical-publish cut with:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-gradle \
NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS=1 \
NEREUS_DELAY_KAFKA_WORKER_DESTINATION_RESPONSE_LOSS_ONLY=1 \
KAFKA_BROKER_1_PORT=19669 KAFKA_BROKER_2_PORT=19670 KAFKA_BROKER_3_PORT=19671 \
./e2e/run-kafka-real-client-e2e.sh
```

The receipt is locked to Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786831579-93599`, ports `19669,19670,19671`,
and Delay commit `e95d1c0cbaf4b94c8523d6fd9994b6487102f400`.

It printed:

```text
Kafka Worker destination response-loss smoke passed: real EndTxn committed the exact target-plus-receipt pair, the local response was discarded, and typed read_committed KAFKA_TRANSACTIONAL_RECEIPT evidence resolved the source-applied PUBLISHED Outcome
Kafka Worker source-applied physical publish passed: Admission source offset=3, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=4, exact payload readback
Kafka Worker destination response-loss E2E passed: real EndTxn response loss resolved through typed read_committed KAFKA_TRANSACTIONAL_RECEIPT evidence and the source-applied Outcome completed.
```

The proxy cuts only the client-side response after real Kafka transaction
commit. The source-bound provider and Worker Outcome remain the authority for
publication; this receipt does not establish raw network loss, process/Broker
crash recovery, multi-Broker failover, Attempt Journal recovery or V1 release
readiness.

## Kafka Worker source ACK response-loss receipt

Run the focused Kafka Worker source-ACK cut with:

```bash
NEREUS_DELAY_KAFKA_CHECKOUT=/Users/liusinan/apps/ideaproject/nereusstream/kafka-worktrees/nereus-delay-k1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-gradle \
NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS=1 \
NEREUS_DELAY_KAFKA_SOURCE_ACK_RESPONSE_LOSS_ONLY=1 \
KAFKA_BROKER_1_PORT=19679 KAFKA_BROKER_2_PORT=19680 KAFKA_BROKER_3_PORT=19681 \
./e2e/run-kafka-real-client-e2e.sh
```

The receipt is locked to Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:4ad4078ccea32586873ae089a66c2d7425a0c96051d2a2de47dbd284f016724f`,
Compose `nereus-delay-kafka-e2e-1786832218-928`, ports `19679,19680,19681`,
and Delay commit `d165e73e457834be55af58d238980be65c2054c7`.

It printed:

```text
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker source ACK response-loss smoke passed: real commitSync ACK was accepted before the local response was discarded, and the same source record was ACKed on the next bounded Worker turn
Kafka Worker source ACK response-loss E2E passed: real commitSync ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

The proxy cuts only the client-side response after `commitSync` returns; the
source adapter's in-flight record and the Worker applied outcome remain the
retry authority. This receipt does not establish raw network loss,
process/consumer/Broker crash recovery, multi-Broker failover, coordinator
recovery or V1 release readiness.

### 2026-08-16 S3-compatible checkpoint Object Store adapter implementation note

Delay commit `e01d3ee8708a53487747b0ef721d1f0d107ff677` adds the
`S3CompatibleCheckpointObjectStoreAdapter` for the existing checkpoint
`CheckpointUploadRequest`/`CheckpointDownloadRequest` contract. It accepts only
the S3/S3-compatible Profile branches and checks the Profile-bound endpoint and
credential authorization-scope digests before any provider call. The JDK HTTP
client emits SigV4 requests with canonical path-style bucket/object keys;
checkpoint file objects reuse the filesystem adapter's deterministic object
identity, use `If-None-Match: *`, and the manifest is uploaded last. A 2xx file
write is reread and hashed, while 409/412 and transport failure resolve only by
an exact remote length/SHA-256 reread. The manifest resource records the
provider version when present and otherwise the deterministic content identity;
resource length/version bounds are checked against the activated limits.

Download requires the full exact `ProfileRefV1`, bucket and canonical manifest
key, verifies manifest bytes/hash/version, streams each file with bounded
length/checksum validation into a private staging directory, re-inventories the
complete tree and atomically renames it into place. The focused local raw HTTP
fixture exercises SigV4/conditional headers, endpoint/credential drift,
manifest response loss, exact restore and immutable conflict:

```bash
./gradlew test \
  --tests io.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapterTest \
  --no-daemon --console=plain
```

This is a provider-shaped adapter and local evidence seam. It does not claim
real S3/MinIO conformance, provider credential-use lease/rotation,
quiescence/consistency attestation, version-aware delete/final sweep,
multi-shard RecoveryPin/catalog transaction, process/network chaos,
credential failover, or V1 release readiness.

### 2026-08-16 Object Store credential-use lease gate implementation note

`ObjectStoreCredentialUseLeaseGate` is the local call gate for an activated
Object Store Adapter. The lease-gated
`S3CompatibleCheckpointObjectStoreAdapter` constructor requires the same
`ProfileRefV1` as the gate; immediately before `upload` or `download` starts
provider I/O, the adapter rechecks the exact `CredentialBindingV1`,
`CredentialBindingProtectionV1` and `CredentialUseLeaseV1` projection, the
configured lease TTL and attestation age, the local trusted-time window and
the loaded credential fingerprint. A gate failure therefore occurs before
SigV4 HTTP ownership. `ObjectStoreCredentialUseLeaseGateTest` covers current
and expired leases, loaded-fingerprint drift and a protection horizon shorter
than the lease.

This implementation supplies only the local recheck. The issuer's Oxia
Head/protection CAS, attestation trust-set verification, secret resolution,
credential rotation, provider quiescence/consistency and external Object Store
authority remain outside this slice. The pre-existing ungated constructors are
retained for the bounded provider-shaped adapter fixture and are not a
production credential-authority claim.

### 2026-08-16 Oxia credential Profile Head/Protection CAS implementation note

`OxiaSyncProfileCatalogBackend` supplies the narrow external authority needed
before the local Object Store lease gate. It stores one bindable Profile
version per canonical Oxia record: the exact semantic envelope, unsigned-sorted
immutable credential generations, the current `CredentialBindingHeadV1` and
matching `CredentialBindingProtectionV1` values are wrapped in a versioned
record with a final digest. Generation-1 publication and
`RotateEquivalentSecretRequestV1` compare the current record version and Head
identity, retry conflicts and accept a response-loss retry only after an exact
canonical reread.

`issueCredentialUseLease` compares the requested Head generation/revision and
binding digest, validates the resolved credential fingerprint and the
attestation's Profile scope/age, bounds the lease TTL, and advances the
managed-channel or Object Store protection horizon by checked monotonic max in
the same record CAS. The returned `CredentialUseLeaseV1` carries the resulting
protection revision, so the local adapter gate can prove the lease was
protected before provider ownership.

The deterministic test is
`OxiaSyncProfileCatalogBackendTest`. The opt-in real test
`OxiaRealProfileCatalogSmokeTest.profileHeadProtectionLeaseAndRotationReopenAgainstRealService`
passed in the Dockerized Oxia harness at source
`37a17bef17202d5fd6e23282da5fd26d94865484` (Compose
`nereus-delay-v1-oxia-e2e-1786835835-39861`, port `16693`). This remains a
single-record/single-node authority receipt; secret resolution, trust-set and
actor authorization, source ordering, retained-generation quota/GC,
cross-record session transactions, multi-node failover for this authority,
provider rotation/quiescence and release evidence remain outside the slice.

### 2026-08-16 Object Store authority-to-adapter activation implementation note

`CredentialProfileAuthority` is the activation-facing extension of the exact
Profile catalog. `OxiaObjectStoreCredentialLeaseActivator` resolves the exact
Object Store Profile, current Head and immutable Binding, invokes an injected
private material resolver, compares its immutable fingerprint with the
Binding's equivalence attestation before any lease CAS, then asks the authority
to issue the bounded `OBJECT_STORE_ADAPTER` lease. It rereads the resulting
Protection projection, validates the lease against both Binding and Protection,
and passes the resulting `ObjectStoreCredentialUseLeaseGate` into
`S3CompatibleCheckpointObjectStoreAdapter`.

The adapter therefore has one explicit activation-time Oxia boundary and no
per-provider-call Oxia read. `OxiaObjectStoreCredentialLeaseActivatorTest`
covers exact composition, resolver fingerprint drift before issuance and a
lease/protection revision mismatch. The injected resolver is a private
material seam only; secret-manager resolution, trust-set/actor authorization,
automatic renewal, rotation coordination, multi-node activation failover,
provider quiescence and external Object Store evidence remain open.

### 2026-08-16 Credential attestation trust-set implementation note

`CredentialAttestationTrustSet` is the verifier authority supplied to
`OxiaSyncProfileCatalogBackend`. It is immutable, sorted by the complete
verifier-version/verifier-id/signing-key tuple and retains only canonical
Ed25519 public-key bytes plus an explicit verification window. Its semantic
digest is stable and excludes private key material. Profile binding
publication, equivalent-secret rotation, canonical record decode/reopen and
credential-use lease issuance all require the exact attestation tuple to be
present, its `verifiedAt`/`notAfter` interval to fit the retained key window,
and its Ed25519 signature to verify.

This closes the local trust-set/signature boundary before Object Store
activation and prevents a persisted untrusted binding from becoming usable
after reopen. The authority still does not publish or source-order trust-set
records, authorize actors, resolve private secrets, coordinate cross-record
Owner/Route/session state, renew leases automatically, fail over as a
multi-node Profile authority or prove provider rotation/quiescence and real
Object Store behavior.

### 2026-08-16 Same-generation Object Store lease renewal implementation note

`RenewableS3CompatibleCheckpointObjectStoreAdapter` wraps the existing
lease-gated adapter with an explicit renewal window. Before an upload or
download it checks the local lease expiry; outside the window it performs no
authority I/O. Inside the window it resolves the exact Profile, requires the
Head generation and immutable Binding digest to remain unchanged, resolves the
same material fingerprint, obtains fresh trusted-time evidence, asks the
`CredentialProfileAuthority` for a bounded `OBJECT_STORE_ADAPTER` lease,
rereads and validates its `CredentialBindingProtectionV1`, then atomically
replaces the gate projection. Provider credentials are not silently changed;
Head rotation fails closed and requires adapter quiescence/re-activation.

This is an opportunistic, single-process control-plane renewal composition.
It does not establish a scheduled multi-process renewal owner, source-ordered
rotation/quiescence, secret-manager resolution, cross-record
Owner/Route/session transactions, multi-node authority failover, real
S3/MinIO consistency, deletion or release evidence.

### 2026-08-16 Verified credential material cache implementation note

`VerifiedCredentialMaterialCache` is the local verified-cache implementation
for the activation resolver boundary. An install validates the exact Object
Store Profile, binding generation/reference hash, authorization scope,
configured `CredentialAttestationTrustSet` and resolved credential fingerprint
before publishing a new immutable-view snapshot. The lookup key contains the
Profile ref, generation, binding digest and secret-reference hash; cache miss
returns null and never resolves another generation or performs Oxia, Vault or
Provider I/O. Batch replacement validates all entries before publishing, so a
drifted entry cannot partially replace the existing cache.

This supplies the cache-side contract required by native/managed activation;
an external secret-manager reader and source-ordered refresh/publication remain
control-plane responsibilities, as do actor authorization, rotation
quiescence and production availability evidence.

### 2026-08-16 MinIO S3-compatible checkpoint provider implementation note

`e2e/run-minio-real-e2e.sh` is the opt-in real-provider harness for the
existing `S3CompatibleCheckpointObjectStoreAdapter`. It locks the local
MinIO image tag and repository digest, starts a unique container with a
dynamic host port, creates only a generated bucket with curl AWS SigV4, and
passes the exact endpoint/region/bucket/access-key scope into
`S3CompatibleMinioRealSmokeTest`. The Gradle invocation uses `--rerun-tasks`
so an earlier skipped real-service result cannot be mistaken for a live
provider receipt; cleanup removes only the container created by this run.

The locked MinIO run at image digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
completed the real adapter upload, same-key immutable retry and atomic
download restore, with JUnit `tests=1 skipped=0 failures=0 errors=0` and
`BUILD SUCCESSFUL`. This is bounded evidence for one MinIO S3-compatible
endpoint. It does not create a generic provider conformance claim or close
credential-authority renewal/rotation, version-aware deletion, consistency
attestation, cross-provider behavior, chaos, failover or release gates.

### 2026-08-16 Catalog-bound manifest version readback implementation note

`S3CompatibleCheckpointObjectStoreAdapter.download` now decodes the
catalog-bound `CheckpointResourceV1.immutableVersion()` as canonical UTF-8,
adds it as the exact S3 `versionId` query and signs that query in the SigV4
canonical request. The returned manifest version must still match the catalog
resource byte-for-byte. The focused fake-provider path rejects a mismatched
version query, and the locked MinIO run passed with provider version
`ac201fe8-ba70-4bcb-a49c-a75a6657be55` and JUnit
`tests=1 skipped=0 failures=0 errors=0`.

This closes only the manifest's catalog-bound version read. It does not yet
retain provider versions for each file object, authorize or execute complete
checkpoint deletion, resolve retire/Floor/Pin state, or close consistency,
rotation, chaos, failover or release gates.

### 2026-08-16 Exact Object Store provider-version implementation note

The mandatory V1 Object Store Profile bit
`requireExactVersionDelete()` now controls a fail-closed response check in
`S3CompatibleCheckpointObjectStoreAdapter`. PUT success, immutable conflict
reread and every bounded GET require `x-amz-version-id`; the old
`sha256-*` content identity remains only as an unreachable compatibility
fallback for a profile that could permit it, while V1 Profile construction
does not permit that unsafe branch. The local fake-provider regression proves
that omitted version headers are rejected before the adapter can claim an
exact provider identity.

The MinIO harness enables bucket versioning before running the same adapter.
The source-locked run returned provider manifest version
`780f1e1f-c7da-4dc1-ae4e-a7b9be4f801c` and passed JUnit
`tests=1 skipped=0 failures=0 errors=0`. This establishes the provider-version
response prerequisite for a later exact delete implementation; it does not
itself authorize deletion, remove the complete checkpoint object set, prove
Recovery Floor/Pin release, or close provider consistency, rotation, chaos,
failover or release gates.

### 2026-08-16 Exact checkpoint object-set deletion implementation note

Delay commit `3bfe030a` adds a narrow delete adapter for a catalog-bound
checkpoint resource. `S3CompatibleCheckpointObjectStoreAdapter.delete`
validates the supplied manifest/resource pair, preflights the exact manifest
version and every deterministic file object by bounded length/SHA-256, then
deletes each captured provider version with a signed S3 `versionId` query;
the manifest is the final delete operation. Every successful DELETE must
return the requested `x-amz-version-id` and a nonblank
`x-amz-request-id`. The adapter returns only a `DELETED`
`CheckpointDeleteResult` after all operations succeed, with domain-separated
aggregate request-ID and response hashes for the external evidence bridge.
The local fake provider covers exact paths, manifest-last order and a missing
delete version response; the real MinIO path exercises the same versioned
bucket and SigV4 request shape.

This is direct provider deletion evidence only. `ALREADY_ABSENT` reconciliation
after partial deletion, final prefix sweeps, retire-intent/Floor/Pin
authorization, provider consistency/quiescence, credential rotation,
multi-provider behavior, chaos, failover and release gates remain external
boundaries.

### 2026-08-16 Checkpoint delete retry-convergence implementation note

Delay commit `220fc98a` extends the exact delete boundary with bounded
presence/absence probes. A missing exact manifest or file is represented by a
provider response-evidence projection rather than an untyped exception. When
all manifest/file objects are absent, the adapter returns
`ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT` and hashes every
probe request ID and response. When only file objects are absent, the adapter
retains the manifest identity, deletes the remaining verified file versions,
and then deletes the manifest exact version. A manifest-absent/file-present
combination remains fail closed because it cannot prove the pinned complete
resource identity through this bounded adapter.

The focused regression simulates a DELETE response loss after the provider
has removed the first file, retries to complete the remaining set, and then
retries once more to obtain `ALREADY_ABSENT`. The locked MinIO test continued
to pass against the versioned bucket. Final prefix sweeping, retire/Floor/Pin
authorization, provider consistency/quiescence, credential rotation,
multi-provider behavior, chaos, failover and release gates remain external
boundaries.

### 2026-08-16 Bounded checkpoint prefix sweep implementation note

Delay commit `c32a98f328400c71346b98188930a6efa80da7c9` adds an explicit
`CheckpointPrefixSweepAdapter` seam for the reaper's provider step. The
caller supplies the exact Object Store Profile, recovery lineage, checkpoint
identity and a bounded one-page version limit. The S3-compatible adapter
derives only `checkpoints/<lineage>/<checkpoint>/`, signs a bucket-level
`ListObjectVersions` request, parses bounded XML with DTD/external entities
disabled, rejects `IsTruncated`, incomplete/duplicate entries and prefix
escape, deletes every returned version with the exact version/request-ID
proof, and performs a final list that must be empty before returning
`CheckpointPrefixSweepResult`.

The local fake-provider test and the locked MinIO harness both passed. The
MinIO run used container `nereus-delay-minio-e2e-1786842572-18888`, endpoint
`http://127.0.0.1:56466`, bucket
`nereus-delay-checkpoints-1786842572-18888`, image repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
and provider manifest version `f905db1e-1a7e-455c-bb32-5fa90bb7ed1f`; JUnit
recorded `tests=1 skipped=0 failures=0 errors=0` and the harness ended with
`BUILD SUCCESSFUL`.

This seam is intentionally not a lifecycle authority: external REAPING
state, source ordering, retire-intent validation, Recovery Floor/Pin/Owner
authorization, provider consistency/quiescence and multi-page reaper policy
remain outside it.

### 2026-08-16 REAPING-to-prefix sweep coordination implementation note

Delay commit `b9fcd2aa846329ed13986b122d287375a441b2fd` composes the existing
intent CAS and provider seam without widening either authority. The
`CheckpointReapingSweepCoordinator` requires `PENDING_UPLOAD`, calls the
catalog/pin-guarded `beginReaping` transition, rereads the exact REAPING value
through `CheckpointUploadIntentAuthority.current`, and only then invokes the
prefix sweep. Its result retains the exact REAPING intent alongside the
provider receipt. A response-loss retry therefore cannot fall back to a
deadline-only or name-only delete: it must reread the same REAPING identity
and the provider must prove the prefix empty again.

The focused coordinator tests and the locked MinIO run passed. The MinIO run
used container `nereus-delay-minio-e2e-1786843326-27711`, endpoint
`http://127.0.0.1:58388`, bucket
`nereus-delay-checkpoints-1786843326-27711`, image repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
and provider manifest version `f5404da4-4944-4581-a75d-80dccdad92c3`; JUnit
recorded `tests=1 skipped=0 failures=0 errors=0` and the harness ended with
`BUILD SUCCESSFUL`.

The composition still requires external proof of old-Owner abandonment or
session loss, provider-owned request quiescence, source-ordered retire and
delete confirmation, and Recovery Floor/Pin/Owner transactions. It does not
claim those authority or release gates.

### 2026-08-16 REAPING quiescence proof implementation note

Delay commit `7b8b73885c5ec26dfc96c1b5b8a1a6ab8ec0d1d9` makes the provider
handoff consume an explicit `CheckpointReapingQuiescenceProof`. The proof
binds the pending intent digest and the exact `reapingStartedAt` evidence,
requires the configured request-quiescence horizon to cover maximum provider
ownership lifetime plus trusted-clock interval width, and carries separate
opaque evidence digests for the certified old-owner local guard and provider
request horizon. `CheckpointReapingQuiescenceGuard` rejects an unelapsed
request boundary or either unclosed external horizon before the coordinator's
current-intent reread and prefix listing.

The focused proof-gate tests and locked MinIO run passed. The MinIO run used
container `nereus-delay-minio-e2e-1786843920-34723`, endpoint
`http://127.0.0.1:59954`, bucket
`nereus-delay-checkpoints-1786843920-34723`, image repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
and provider manifest version `9c4dcab9-c03c-4860-81de-07e62302d30e`; JUnit
recorded `tests=1 skipped=0 failures=0 errors=0` and the harness ended with
`BUILD SUCCESSFUL`.

The proof is a local contract for external evidence, not an evidence issuer:
Owner/session loss detection, provider quiescence attestation, source-ordered
delete confirmation, Floor/Pin/Owner transactions, provider breadth, chaos,
failover and release gates remain outside this implementation.

### 2026-08-16 REAPING Owner proof implementation note

Delay commit `44cd3230709f5e87742cd94cd9a8b7bce314a184` adds a typed
`CheckpointReapingOwnerProof` boundary. Its canonical evidence binds the
pending intent digest, exact `OwnerIdentityV1`, source Store Incarnation and
the complete session-bound recorded `OwnerLease`, including its assignment,
session, token, epoch and lifecycle state. The closed proof kind separates
`EXACT_OWNER_EXPLICIT_ABANDON` from `RECORDED_OWNER_NOT_CURRENT`; the issuer
uses `OxiaOwnerLeaseStore.release` plus an absence reread for the first and a
current-lease reread for the second. Both paths require trusted UTC at or
after the pending upload deadline.

`CheckpointReapingSweepCoordinator` consumes the proof before its
PENDING_UPLOAD to REAPING CAS. The existing quiescence proof must carry the
typed proof digest and an old-owner closure interval no earlier than the
Owner observation. This prevents an opaque caller digest from being silently
treated as an Owner/session proof while preserving the external authority
boundary: the issuer does not implement the production atomic intent,
Owner/session and catalog transaction, and it does not attest provider
ownership or source-ordered deletion.

The focused issuer/coordinator tests and the locked MinIO real smoke passed;
the MinIO run used container `nereus-delay-minio-e2e-1786845031-48170`,
endpoint `http://127.0.0.1:62715`, bucket
`nereus-delay-checkpoints-1786845031-48170`, image repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
and provider manifest version `ea89d80e-e63e-4980-b225-94b070d3c36b`;
JUnit recorded `tests=1 skipped=0 failures=0 errors=0` and the harness ended
with `BUILD SUCCESSFUL`.

### 2026-08-16 Object Store provider-owned request horizon implementation note

Delay commit `cc97c7654cb19f88c69045cd3c33a4d970a9fed3` implements the local
ownership ledger required around the Object Store adapter boundary. One
tracker operation spans the complete adapter method, including nested
checkpoint requests and streamed response-body consumption. Normal completion
closes it; any runtime/error path records an uncertainty horizon bounded by
`maximumProviderOwnershipLifetimeMs`. A one-way `beginProviderQuiescence`
fence prevents new operations, and a typed observation is accepted locally
only after active operations reach zero and the uncertainty horizon elapses.
The renewable wrapper checks this fence before renewal. The adapter also
rechecks `ObjectStoreCredentialUseLeaseGate` immediately before every
`HttpClient.send`, preserving the no-per-call-Oxia-read boundary while
preventing a stale lease from spanning a multi-object operation.

The focused tracker/adapter tests passed with 3 and 9 tests; the full check
returned 0. The locked MinIO run used container
`nereus-delay-minio-e2e-1786846128-60582`, endpoint
`http://127.0.0.1:49215`, bucket
`nereus-delay-checkpoints-1786846128-60582`, image repository digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
and provider manifest version `1b904a10-2104-46eb-a6fd-0bd2afe24524`;
JUnit recorded `tests=1 skipped=0 failures=0 errors=0` and the harness ended
with `BUILD SUCCESSFUL`.

The tracker is local evidence for an external issuer, not a provider-side
quiescence or consistency attestation. Remote request execution,
cross-record lifecycle authority, source-ordered delete confirmation,
Floor/Pin/Owner transactions, provider breadth, chaos, failover and release
gates remain open.

### 2026-08-16 Checkpoint delete-confirmation mutation composition implementation note

Delay commit `70e5f0da` adds `CheckpointDeleteConfirmationComposer` as the next
local boundary after the provider adapter receipt. It accepts the exact
durable `ResourceRetireIntentRecord` and `CheckpointDeleteResult`, compares
the full checkpoint `ExactResourceIdentity` canonical bytes and hash, adapts
the provider request/response result into `ExternalDeleteEvidence`, and
requires the trusted confirmation interval to begin no earlier than the
trusted provider-observation interval ends. It derives the mutation shard
from the retire intent's applied Source Position, emits the canonical common
body plus `RetireIntentRef`, outcome, evidence and `confirmedAt`, verifies the
nested body before signing, and uses the retire mutation ID as the registered
logical identity.

This is an evidence-to-mutation composer, not a lifecycle transaction. The
service author and Ed25519 signature authenticate the composed mutation, but
the composer does not perform provider deletion, establish remote provider
completion, authorize the retire intent, evaluate Recovery Floor/Pin/Owner
coverage, append to or apply the Shard Log, or release any protection. The
existing `GcWorkClassExecutor` and `DelayShard` boundaries therefore remain
the callers' explicit source-order and ownership gates.

The focused `CheckpointDeleteConfirmationComposerTest` covers DELETED,
ALREADY_ABSENT, exact-identity mismatch and an observation/confirmation
interval overlap; all four tests passed, as did the full Gradle check.

### 2026-08-16 Delete-confirmation temporal evidence fence implementation note

Delay commit `a26c6816` makes the time-causality rule a canonical protocol and
durable-record invariant rather than a property of one factory. The
`RESOURCE_DELETE_CONFIRMED_V1` parser rejects a confirmation interval whose
earliest trusted time is before the provider evidence's latest trusted
observation time. `ResourceDeleteConfirmedRecord` repeats that validation
before retaining the two intervals, covering direct construction, replay
decode and any future signed-body composer. Existing fixtures now carry a
strictly later confirmation interval.

This fence protects evidence ordering only. It does not turn trusted time
into provider-side completion, perform owner/Floor/Pin authorization, append
or apply a source mutation, or close the external GC lifecycle transaction.

### 2026-08-16 Source-ordered GC confirmation handoff implementation note

Delay commit `b225cef9` adds a typed `GcWorkClassExecutor.submitDeleteConfirmation`
entrypoint for the already-composed confirmation mutation. Before queueing, it
matches the nested retire reference against the complete durable
`ResourceRetireIntentRecord` identity fields and decodes the exact predecessor
Source Position. After the external appender returns `PERSISTED`, the executor
requires the returned position to have the same authenticated physical source
identity and a strictly later source order; otherwise it fences the local
Owner and returns an `UNKNOWN` handoff result.

This protects the meaning of the local handoff result without moving source
position allocation into Delay. The appender, source assignment, provider
delete, tombstone WriteBatch, source apply and lifecycle authorization remain
separate authorities.

### 2026-08-16 Oxia Recovery Pin session-bound CAS implementation note

Delay commit `dedd03a94fb2ab1e8d12f19ba993408646426578` implements the next
bounded Recovery Catalog seam without folding a second record into the
catalog snapshot. `OxiaSyncRecoveryCatalogBackend` keeps manifest/resource
and scalar/typed Floor state in its canonical version-CAS record, while
`RecoveryPinV1` is encoded at a sibling recovery-pin key with Oxia's
`IfRecordDoesNotExist` plus `AsEphemeralRecord` options. The caller supplies
the session identity derived from the connected client session; the backend
rejects catalog-only create/release calls and checks the identity encoded in
the returned Oxia `Version`.

The create path validates the pin through the local catalog projection,
checks the requested catalog generation before the ephemeral CAS, validates
the exact key/version/session response, rereads canonical pin bytes, and
checks the catalog generation again. The release path compares the complete
pin value, deletes with the exact returned version, and accepts response loss
only after an exact absence reread. A separate `activeRecoveryPin` read
validates key, value size, canonical `RecoveryPinV1` bytes, non-null Oxia
version and the version-derived session digest before exposing the projection.

The before/after generation checks close the obvious stale-generation race,
but they are intentionally not an Oxia multi-record transaction. A catalog
write can still race between those checks, and upload-intent/catalog/pin
activation remains a production authority boundary. The implementation does
not issue Owner Lease/session-loss attestations, provider proofs, Source Log
positions or GC authorization.

### 2026-08-16 Atomic publication Recovery Pin CAS implementation note

Delay commit `04976375` reuses `OxiaSessionBoundRecoveryPinStore` from the
atomic publication authority. `OxiaSyncCheckpointPublicationBackend` keeps
its combined Upload Intent plus Recovery Catalog state in the existing
canonical `/publication` record and places `RecoveryPinV1` at a sibling
`/recovery-pin` key. The identity-bearing constructor passes the connected
Oxia session digest into the pin store; the legacy catalog/publication
constructor remains deliberately pin-ineligible.

The publication callback validates the pin against a fresh
`RecoveryCatalog.fromSnapshot` projection before each CAS attempt, while the
pin store validates the exact ephemeral key, canonical bytes, Oxia version and
version-derived session identity. The final catalog-generation reread is
required after the pin CAS, and exact-version release accepts a lost response
only after proving the sibling record absent. This makes the same local pin
semantics available to `CheckpointReapingGuard` and restore callers when the
atomic publication authority is their catalog backend.

The sibling record does not become part of `PublicationState`, and the
before/after generation checks are not a multi-record Oxia transaction. The
implementation therefore still requires a higher-level Owner/session,
Intent/Catalog/Pin activation authority and does not provide provider,
source-order or GC lifecycle authorization.

### 2026-08-16 Oxia Control Operation session-bound CAS implementation note

Delay commit `cc8001b528bb9943a2f683c6ad14728c426cb8f2` adds a
session-bound constructor to `OxiaSyncControlOperationBackend`. The constructor
accepts `OxiaSyncOwnerLeaseBackend.ClientHandle` and composes its connected
session marker into a private `SessionBoundRecordClient`. Every control
operation record read and version-CAS write checks the marker before and after
the Oxia call. Expected CAS races are rethrown only after the post-call marker
check succeeds; if the marker changes after a committed write, the exact
successor reread cannot run under the fenced session and the operation remains
unknown/fenced.

The deterministic regression includes a fake record service that commits the
value and then fences the session before the put response. The backend throws,
while a separately reopened unbound test seam can observe the exact durable
value; this distinguishes durability from permission to report success. The
focused backend suite passed 5 tests, the two real-service methods were
skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full
Gradle check returned 0.

The unbound `SyncOxiaClient` constructor remains an explicit external/test
surface. This note does not claim actor/scope authorization, source-ordered
control mutation routing, cross-record target/state transactionality,
automatic session reconnect, production query routing, provider evidence,
chaos or release readiness.

### 2026-08-16 Oxia Control Target Registration session-bound CAS implementation note

Delay commit `50435a1364d2e8f7d823cc05faa18e4766f5cbd6` extends the same
session-bound record-I/O rule to `OxiaSyncControlTargetRegistrationBackend`.
The `ClientHandle` constructor supplies the connected Oxia marker to a private
`SessionBoundRecordClient`, which checks before and after every `get` and
`put`. Registration response loss after a committed
`IfRecordDoesNotExist` write is therefore not classified as `RECORDED` or
`ALREADY_RECORDED` when the session marker has changed; `find` and mutation
validation are fenced by the same wrapper.

The deterministic regression commits the target record, fences the session
before the put response returns, asserts the backend fails, and then reopens
the bytes through the explicit unbound test seam. Four target-registration
tests passed; the two real-service methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full Gradle check
returned 0.

This is a single-record target-registry fence. It does not make Control
Operation and target registration one Oxia transaction, and it does not add
actor/scope authorization, source-ordered routing, automatic session
reconnection, production query routing, chaos or release evidence.

### 2026-08-16 Oxia credential Profile catalog session-bound CAS implementation note

Delay commit `89020c97c29f99d98f7f3259ab7b27131644adcd` adds a
`ClientHandle` constructor to `OxiaSyncProfileCatalogBackend`. The constructor
binds the canonical Profile catalog record to the connected Oxia session marker
through a private `SessionBoundRecordClient`. Every `get` and version-CAS
`put` checks the marker before and after the client call, so publication,
equivalent-secret rotation, protection-before-lease issuance, resolution and
response-loss rereads cannot report success after session loss.

The deterministic regression commits a generation-one Profile record, fences
the session before the put response returns, asserts the backend fails, and
then reopens the exact bytes through the explicit unbound test seam. Four
Profile catalog tests passed; the real-service method was skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full Gradle check
returned 0.

The unbound `SyncOxiaClient` constructor remains an explicit external/test
surface. This note does not claim secret-manager resolution, source-ordered
Profile publication, actor/target authorization, retained-generation GC,
cross-record Owner/Route/session transactionality, automatic reconnect,
provider rotation/quiescence, chaos or release readiness.

### 2026-08-16 Oxia Recovery Catalog session-bound CAS implementation note

Delay commit `f04f58d15588662b71be68809e1a11a627baf540` adds a
`ClientHandle` constructor to `OxiaSyncRecoveryCatalogBackend`. The canonical
catalog record uses a session-bound record wrapper for every catalog
`get`/version-CAS `put`. The original pin-store construction still received
the raw record client, so pin session-identity bytes and ephemeral CAS were
covered, but current-marker fencing of pin I/O was not proven by that commit.

The deterministic regression commits a manifest, fences the session before the
catalog put response returns, asserts failure, and reopens the exact manifest
through the explicit unbound test seam. Eighteen Recovery Catalog tests
passed; the three real-service methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full Gradle check
returned 0.

The unbound constructor remains an explicit external/test seam. This note does
not claim Catalog/Pin/Upload-Intent transactionality, source-ordered
activation, Owner/session recovery, immutable Object Store publication,
provider deletion, source/evidence replay, chaos or release readiness.

### 2026-08-16 Recovery Pin session-fenced client wiring correction implementation note

Delay commit `f0e45cbdf6eb30d730c6678e71c4c19d34e06072` passes the
already session-wrapped catalog/publication record client into
`OxiaSessionBoundRecoveryPinStore` in both Oxia authorities. Pin `get`,
`AsEphemeralRecord` `put` and exact-version `delete` now check the connected
marker before and after the client operation, including response-loss paths.

`OxiaSyncRecoveryCatalogBackendTest` and
`OxiaSyncCheckpointPublicationBackendTest` add create/release regressions for
both authorities. The focused Recovery Catalog and Publication suites passed;
the tests prove that a committed pin operation is fenced after marker loss
while an unbound reread observes the exact durable result. This corrects only
the per-record pin I/O session fence and does not claim a Catalog/Pin/
Upload-Intent transaction, Owner/session recovery, provider evidence, source
ordering, chaos or release readiness.

### 2026-08-16 Oxia Checkpoint Publication session-bound CAS implementation note

Delay commit `ffe0e5e15894ba377248068258444a1484bfb7f2` adds a
`ClientHandle` constructor to `OxiaSyncCheckpointPublicationBackend`. The
canonical `/publication` record continues to hold the PUBLISHED Upload Intent
and Recovery Catalog manifest projection in one Oxia version-CAS value, while
the handle-bound path now composes a private `SessionBoundRecordClient` around
that record. The wrapper checks the connected session marker before and after
each publication-record `get` and `put` (and preserves the narrow delete/
identity delegation surface used by the record client), so a committed CAS
whose response arrives after session loss cannot be reported as a successful
publication.

The deterministic regression commits the combined publication value, fences
the session before the put response returns, asserts the session failure and
then reopens the exact manifest through the explicit unbound seam. Five
Publication tests passed; the two opt-in real-service methods were skipped
because `NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full Gradle
check returned 0.

This is still one canonical publication record plus a separate ephemeral
Recovery Pin record, not an Oxia multi-record Intent/Catalog/Pin transaction.
Provider immutability/completion, source/evidence ordering, Owner/session
recovery, raw chaos, failover and release gates remain outside this receipt.

### 2026-08-16 Oxia Checkpoint Upload Intent session-bound CAS implementation note

Delay commit `0a1e6020` adds a `ClientHandle` constructor to
`OxiaSyncCheckpointUploadIntentBackend`. The separate `/intent` record keeps
its existing canonical intent bytes and exact version CAS, while the
handle-bound path composes a private `SessionBoundRecordClient` around every
record read and write. The marker is checked before and after the Oxia call;
therefore a committed PENDING_UPLOAD/PUBLISHED/REAPING successor whose
response arrives after session loss cannot be reported as a successful intent
transition.

The deterministic regression commits the fake intent, fences the session
before the put response returns, asserts the failure and then reopens the exact
intent through the explicit unbound seam. Four Upload Intent tests passed; the
three opt-in real recovery-authority methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full Gradle check
returned 0.

This remains an independent single-record compatibility authority. It does
not provide the missing cross-record Intent/Catalog/Pin transaction, Owner or
session recovery, provider completion/attestation, source/evidence ordering,
raw chaos, failover or release evidence.

### 2026-08-16 Oxia Worker assignment session-bound CAS implementation note

Delay commit `cca59a92df395c11cfdda23d24bb27a8b5269cca` strengthens the
existing handle-bound `OxiaSyncWorkerAssignmentBackend` constructor. The
desired assignment remains a durable canonical record with revision CAS, but
the record client now checks the exact connected Oxia marker before and after
every `get`, `put` and exact-version `delete`. A successful durable put whose
response arrives after marker loss cannot be returned as a valid Worker
assignment publication; the unbound constructor remains the explicit
deterministic/external surface.

The deterministic regression commits the fake assignment, fences the session
before the put response returns, asserts failure and then reopens the exact
assignment through the unbound seam. Five Worker assignment tests passed; the
opt-in real route-worker smoke method was skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full Gradle check
returned 0.

This is a single desired-assignment session fence, not an Assignment/Owner/
Route transaction or placement authority. Session recovery, source ordering,
raw chaos, failover and release evidence remain outside this receipt.

### 2026-08-16 Oxia Owner Lease session-bound CAS implementation note

Delay commit `7a76a3af61ea16bceb81cc566462c078ca8de2a5` strengthens the
connected `OxiaSyncOwnerLeaseBackend` path. The backend retains the raw client
for session-marker inspection and wraps the owner epoch and ephemeral lease
record client in a private `SessionBoundRecordClient`. Every `get`, version-CAS
`put` and exact-version `delete` checks the exact marker before and after the
record call. A committed lease whose response arrives after marker loss
therefore fails closed rather than being exposed as a successful acquire,
renewal, lifecycle transition or release; the unbound constructor remains the
explicit deterministic/external surface.

The deterministic regression commits the fake lease, fences the session before
the ephemeral put response returns, asserts failure and then reopens the exact
lease through the unbound seam. Fourteen Owner Lease tests passed; the opt-in
real Oxia owner smoke was skipped because `NEREUS_DELAY_OXIA_ENDPOINT` was not
configured, and the full Gradle check returned 0.

This is a per-record epoch/lease session fence, not an Assignment/Owner/Route
transaction or placement authority. Session recovery, source ordering, raw
chaos, failover and release evidence remain outside this receipt.

### 2026-08-16 Oxia Route authority session-bound I/O fence implementation note

Delay commit `57e466786aea596cfdbd75020e48310415da0335` strengthens the
`OxiaRouteAuthoritySession` record/watch surface. Synchronous Route `get` and
`put`, notification registration and range-scan creation now check the exact
ephemeral marker before and after the delegated call. Range scans are wrapped
by a private `SessionBoundIterable` whose lazy iterator checks before and after
each `hasNext`, `next` and `remove`, so a provider cannot continue consuming a
stale authority stream after marker loss. A committed Route head whose marker
disappears before the response is therefore fenced rather than returned as a
successful publication.

The deterministic regression commits the fake Route head, expires the marker
before the head response returns, asserts failure and rereads the exact head
through the raw fake seam. Six Route provider/session tests passed; four real
Route authority methods and one real Route-worker method were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured, and the full Gradle check
returned 0.

This is per-operation Route session fencing and lazy range protection, not an
event/head transaction or automatic reconnect authority. Multi-node failover,
placement/source ownership, raw chaos and release evidence remain outside
this receipt.

### 2026-08-16 Atomic checkpoint publication authority pairing fence implementation note

Delay commit `920197ad41aaa6f0b88871f5ddf631f6899a53d3` closes the reverse
split-authority path in `CheckpointPublicationCoordinator`. The constructor
now checks both supplied sides: when either the Upload Intent authority or the
Recovery Catalog authority implements `CheckpointAtomicPublicationAuthority`,
the same object must be supplied as both authorities. A separately supplied
atomic catalog therefore fails before a provider upload can create an object
whose intent projection belongs to another record.

`CheckpointUploadCoordinatorTest.rejectsMismatchedAtomicAuthorityRegardlessOfWhichSideDeclaresIt`
covers both directions. The focused `CheckpointUploadCoordinatorTest` suite
passed with the full Gradle test task. This is a constructor-time wiring fence
only; it does not claim a three-record Intent/Catalog/Pin transaction, Owner or
session recovery, provider evidence, source ordering, chaos or release
readiness.

### 2026-08-16 Oxia Route notification reconnect session fence implementation note

Delay commit `de203e4dc14de32746ce73da75381843152af922` closes the missing
session fence around the replacement notification path in
`OxiaRouteAuthoritySession.reconnectNotifications`. The current Route marker
is required before replacement-client creation and immediately before
registration; it is checked again after registration. If marker loss follows a
committed registration, the replacement client is closed, the previous client
reference is restored and the call fails closed.

`OxiaSignedRouteSnapshotProviderTest.notificationReconnectRequiresTheCurrentSessionBeforeRegistration`
and
`OxiaSignedRouteSnapshotProviderTest.notificationReconnectRejectsACommittedRegistrationAfterTheMarkerChanges`
cover the pre-registration and committed-registration/marker-loss boundaries.
The deterministic Route provider/session suite passed 8 tests; the opt-in real
Route authority and Route-worker methods were skipped because
`NEREUS_DELAY_OXIA_ENDPOINT` was not configured. This is replacement
notification registration fencing, not automatic reconnect, event/head
transactionality, multi-node failover, placement/source ownership, raw chaos or
release evidence.

### 2026-08-16 Oxia Route provider start retry after notification fence implementation note

Delay commit `d241246eefc284fea9719c8e162afa8e2a8e4828` fixes the provider
state left after a replacement notification registration commits and the Route
session fence rejects its post-call response. `start()` now completes without
work only for a healthy started cache; a non-healthy started cache enters the
explicit `refresh()` path, which reconnects the Route session, replaces the
notification client and rebuilds the signed Route cache before returning
success.

`OxiaSignedRouteSnapshotProviderTest.startRetriesNotificationRegistrationAfterACommittedRegistrationIsFenced`
covers the failed first start, `WATCH_GAP` state and successful second start
with a replacement notification registration. Nine deterministic Route
provider/session tests passed. This is a retry state transition after a fenced
registration, not transparent automatic reconnect, event/head transactionality,
multi-node failover, placement/source ownership, raw chaos or release evidence.

### 2026-08-16 Oxia Route initial-refresh notification restoration implementation note

Delay commit `22780082d24e2011d44ead6ca62c38251a03633b` closes the initial
subscription gap in `OxiaSignedRouteSnapshotProvider.refresh()`. When the
provider has not completed `start()` because the first Route replay failed,
`refresh()` now rebuilds the exact authority cache and registers the initial
notification callback before returning. The provider records the started
subscription before registration so a response-loss marker fence is handled
by the replacement path on the next explicit retry.

`OxiaSignedRouteSnapshotProviderTest.refreshAfterAnInitialRouteGapRestoresTheNotificationStream`
forces a missing event, repairs the event/head pair and requires revision 2
plus one initial registration. Ten deterministic Route provider/session tests
passed. This is initial-refresh subscription restoration, not transparent
automatic reconnect, event/head transactionality, multi-node failover,
placement/source ownership, raw chaos or release evidence.

### 2026-08-16 fleet and Route resource close aggregation implementation note

Delay commit `eb47cb807ceb45d68a9f8db5f53ef3a7cc6ead4e` completes the local
close-attempt boundary for two independently composed surfaces. The fleet
continues calling every admitted `WorkerShardRuntime.close()` after a failure,
retains the first failure and suppresses later failures, and sets its closed
state only after all shard drains/closures succeed. The session applies the
same pattern to its authority and separate notification client, preventing an
authority teardown exception from skipping watch cleanup.

The focused command passed 2 fleet tests and 11 Route provider/session tests.
This is retryable local teardown reporting only: owner drain ordering remains
required, and no Route transactionality, automatic session recovery,
placement/source authority, chaos, failover or release evidence is inferred.

### 2026-08-16 Worker source close retry boundary implementation note

Delay commit `874fccb4fc521ad51b7954236ec5e37c1591e011` makes the source-loop
close state retryable. `WorkerSourceApplyLoop.close()` invokes the native
`SourceRecordConsumer.close()` before setting its closed flag; a thrown native
close therefore leaves the loop available for the same owner-drain retry and
does not turn partial resource release into a terminal source state.

`SourceApplyCoordinatorTest.workerSourceLoopRetriesNativeCloseAfterAReleaseFailure`
passed in the 8-test deterministic source/apply suite. The note closes only
local source teardown retryability; pending-ACK/owner-drain ordering, Broker
reconnect/ACK evidence, crash/chaos, failover and release gates remain open.

### 2026-08-16 Route client teardown retry boundary implementation note

Delay commit `9f24b2f38ba4f21962bebdaa2455d7f86ba0cd1b` keeps Route teardown
retryable while fencing new I/O. `OxiaRouteAuthoritySession.close()` records
completion only after both independently owned clients close successfully; the
provider uses the same state split for its owned notification executor and
Route client, attempts both resources and preserves the first failure with
later failures suppressed.

The deterministic Route provider/session suite passed 12 tests, including
failed-session-close and failed-provider-client-close retry regressions. This
is local teardown state only; automatic session recovery, Route transactionality,
placement/source authority, chaos, failover and release evidence remain open.

### 2026-08-16 Direct SDK client teardown retry boundary implementation note

Delay commit `677026b3` keeps `DefaultDelayClient` fenced but retryable during
child teardown. `close()` sets the existing client-operation fence before
calling the outbox, query client and optional `CommandTransportRegistry`; it
attempts each child independently, preserves the first `RuntimeException` or
`Error` with later failures suppressed, and sets `closeCompleted` only after
the complete child set closes successfully. A failed first close therefore
does not permit new SDK I/O and does not discard the explicit retry path.

`DefaultDelayClientTest.closeRetriesEveryChildAfterTheFirstCloseFailure`
passed in the 11-test deterministic Direct SDK client suite. The note closes
only local SDK teardown retryability; provider/session recovery, transport
delivery, durable outbox authority, crash/chaos, failover and release gates
remain open.

### 2026-08-16 Route connect prefix validation boundary implementation note

Delay commit `4da7bcf46b0ab9350adebf1f614590851a1fadd8` makes the Route connect
factory validate `keyPrefix` before external client creation. The canonical
prefix is computed after the scalar connect arguments pass their basic checks,
then reused for the session constructor; malformed trailing-slash, blank,
non-NFC or NUL-bearing prefixes cannot reach either authority or notification
`OxiaClientBuilder` call.

`OxiaRouteAuthoritySessionTest.connectRejectsAnInvalidKeyPrefixBeforeCreatingOxiaClients`
passed in the 1-test deterministic Route session construction suite. This
note closes only local input/resource-ordering validation; session recovery,
Route transactionality, placement/source authority, chaos, failover and
release gates remain open.

### 2026-08-16 Worker monitor teardown retry boundary implementation note

Delay commit `2f7d9d667547380355a27517ea2c1e4941962693` aligns both Worker
monitor close paths with the shared-resource retry contract. Each monitor sets
its `closed` fence before invoking `ScheduledFuture.cancel` and
`ScheduledExecutorService.shutdownNow`, aggregates failures across those
independent actions and sets `closeCompleted` only after all actions succeed.
If the injected or platform executor rejects the first shutdown, a later owner
close can retry it while polling remains fenced.

The two monitor regressions
`WorkerRuntimeResourceMonitorTest.closeRetriesExecutorShutdownAfterTheFirstFailure`
and
`WorkerRocksDbUsageMonitorTest.closeRetriesExecutorShutdownAfterTheFirstFailure`
passed in the 12-test deterministic monitor suites. This note closes only
local monitor teardown retryability; native process recovery, production
resource authority, Owner/Oxia, chaos, failover and release gates remain open.

### 2026-08-16 In-memory command transport registry teardown retry implementation note

Delay commit `0378e9a7585397e6f5e71a301f58c6d00835f2a0` keeps the deterministic
command transport registry fenced but retryable. Its close path snapshots
entries, attempts every registered transport, removes successful entries only,
and sets `closeCompleted` after the map is drained; a failed transport remains
owned for the next explicit close while successful siblings are not repeated.

`InMemoryCommandTransportRegistryTest.closeRetriesOnlyTheTransportThatFailedTheFirstTeardown`
passed in the 1-test deterministic registry suite. This note closes only local
registry teardown retryability; production Kafka/Pulsar client lifecycle,
transport delivery, Broker failover, chaos and release gates remain open.

### 2026-08-16 Guarded Pulsar transport teardown aggregation implementation note

Delay commit `9d164037f9ba3832cd1f83846813b44de18967ab` makes the guarded
Pulsar bridge attempt `managedSender.close()` and `nativeSender.close()` as
independent teardown actions. The first failure is retained and later
failures are suppressed; the enclosing pinned ingress/native adapter's
retryable `CloseGuard` can therefore retry the failed sender after both child
paths have been attempted.

`GuardedTransportOwnershipTest.pulsarCloseAttemptsNativeSenderAfterManagedSenderFailure`
passed in the 4-test deterministic guarded transport suite. This note closes
only local teardown aggregation; native/managed Broker delivery, client
authority, failover, chaos and release gates remain open.

### 2026-08-16 Owner connect prefix validation boundary implementation note

Delay commit `499e8439f2fe0f1b1c1114dbfd1bb7e55a06c43c` makes the Owner lease
connect factory validate its canonical key prefix before any external Oxia
client is built. Namespace, client identifier and prefix are canonicalized
once, and the validated prefix is passed into the session-establishing backend
constructor.

`OxiaSyncOwnerLeaseBackendTest.connectRejectsAnInvalidKeyPrefixBeforeCreatingAnOxiaClient`
passed in the 15-test deterministic Owner backend suite. This note closes
only local connect-input/resource-ordering validation; Owner/Oxia recovery,
lease authority, placement, chaos, failover and release gates remain open.

### 2026-08-16 Gateway admission lease release retry boundary implementation note

Delay commit `d5384b954e4d99ad291b2aea004910e1b1666ec8` keeps the durable
admission lease handle open until its exact CAS release succeeds. The local
`closed` marker is written after `owner.release(tenantScopeHash, lease)`
returns, so a failed bounded CAS sequence does not prevent a later caller
retrying the same lease identity.

`OxiaGatewayAdmissionControllerTest.leaseCloseRemainsRetryableAfterReleaseCasDoesNotConverge`
passed in the 6-test deterministic Gateway admission suite. This note closes
only local lease-release retryability; distributed Gateway authority, session
recovery, transport delivery, failover, chaos and release gates remain open.

### 2026-08-16 Gateway idempotency evidence monotonicity implementation note

Delay commit `b19f998ffe811d0a6dee1051491eae6c61131712` implements the
single-record evidence rules in the Gateway idempotency contract. Outcome CAS
installation now checks the durable prepared branch, exact prepared command or
native reference, and physical attempt identity. An identical terminal replay
is an exact no-op; a different terminal value for the same attempt is an
integrity conflict, so a late callback cannot last-write-wins over stored
evidence.

Aggregate recomputation preserves the first queued receipt, keeps any queued
aggregate from being downgraded, and chooses the highest unresolved attempt
when uncertainty remains. Retry admission uses that highest unresolved attempt
as its precondition even when a newer retry has a definitive non-queued result.
The Oxia store avoids a new put for an exact replay, while the in-memory store
uses the same record transition logic.

The focused Gateway suites passed 13 tests with zero failures/skips/errors;
the full `./gradlew check` passed 1532 tests with 24 skips and zero
failures/errors. This note closes only local durable evidence ordering and
retry-precondition behavior; transport delivery, distributed authority,
failover, chaos and release gates remain open.

### 2026-08-16 Gateway prepared-expiry fence and aggregate replay implementation note

Delay commit `66508783f5e8230ace8bae37ff04c28dfb353653` makes the prepared
retention fence durable at `startAttempt()` in both store implementations. A
record still in `PREPARED` with no attempt cannot cross its retention deadline
to create a `STARTED` attempt or ownership permit. The schedule handler first
resolves the durable state, allowing an already installed aggregate to replay
exactly after the caller's retry deadline; only an empty expired preparation
maps to `PREPARED_COMMAND_EXPIRED`.

The focused Gateway suites passed 16 tests with zero failures/skips/errors;
the full `./gradlew check` passed 1535 tests with 24 skips and zero
failures/errors. This note closes only prepared-expiry and aggregate-replay
ordering; transport delivery, distributed authority, failover, chaos and
release gates remain open.

### 2026-08-16 Gateway attempt projection integrity fence implementation note

Delay commit `52c6ed1c604a98b56668e510a3cf84ad364ec9cc` makes the persisted
Gateway attempt projection fail closed before state-machine code consumes it.
`GatewayPhysicalAttemptV1` couples evidence presence to the lifecycle state;
`GatewayIdempotencyRecordV1` additionally checks source order, unique physical
and retry identities, phase consistency and aggregate presence for prepared
and quiescent records.

The deterministic idempotency suite passed 10 tests with zero
failures/skips/errors; the full `./gradlew check` passed 1536 tests with 24
skips and zero failures/errors. This note closes only local projection
integrity; transport delivery, distributed authority, failover, chaos and
release gates remain open.

### 2026-08-16 Gateway stored evidence binding implementation note

Delay commit `380e279725e9ac5d31f98ad49ee711cd15c5b25c` makes the durable
Gateway record validate semantic evidence before it can be consumed. Record
construction and strict decode validate the prepared submission, decode every
terminal attempt outcome, bind its branch/reference/physical attempt identity
through the existing outcome validator, and require the outcome kind to agree
with the persisted attempt state. The record then recomputes the aggregate
from the complete attempt history and rejects any stored aggregate whose bytes
do not equal that deterministic projection.

`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsOutcomeStateAndAggregateMismatches`
passed in the 11-test deterministic idempotency suite. The full
`./gradlew check` passed 1537 tests with 24 skips and zero failures/errors.
This closes only local semantic projection integrity; transport delivery,
distributed authority, failover, chaos and release gates remain open.

### 2026-08-16 Gateway retry evidence hash binding implementation note

Delay commit `5e1bd9f6b3e2bcf24972e7b9ecdd78db49520734` extends stored Gateway
projection validation to the retry identity formula:
`SHA-256("nereus-delay-gateway-retry-request-v1\0" || gatewayKeyHash ||
priorPhysicalAttemptId || retryRequestId)`. The record rejects a retry hash
that does not bind to any earlier physical attempt in the same history. It
does not infer the earlier attempt's state from the final projection, because
late evidence may legally promote that prior attempt after the retry CAS.

`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsOutcomeStateAndAggregateMismatches`
passed in the 11-test deterministic idempotency suite. The full
`./gradlew check` passed 1537 tests with 24 skips and zero failures/errors.
This closes only local retry-evidence hash binding; transport delivery,
distributed authority, failover, chaos and release gates remain open.

### 2026-08-16 Gateway operation/prepared binding implementation note

Delay commit `f27800424a7cde3b8496b4fbbb4d4586cbeb07ca` makes the stored
Gateway operation tag a checked projection of prepared bytes. Managed command
types map one-to-one to the five Gateway operations; native prepared delivery
is restricted to Schedule. A canonical record with valid branch and digest
bytes but a mismatched operation now fails during construction/decode.

`OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsImpossibleAttemptAndRecordShapes`
and the Gateway gRPC/schedule suites passed 10 focused tests. The full
`./gradlew check` passed 1537 tests with 24 skips and zero failures/errors.
This closes only local operation/prepared binding; transport delivery,
distributed authority, failover, chaos and release gates remain open.

### 2026-08-16 Gateway audit phase evidence implementation note

Delay commit `745da182c72af27dff09a8fb55db6cc15a4f20e3` makes the
`GatewayAuditEventV1` phase/digest union fail closed. `COMPLETED` events must
carry the outcome digest, while `RECEIVED` and `FAILED` events must leave the
digest absent; impossible combinations now fail during record construction.

`OxiaGatewayAuditSinkTest.auditOutcomeDigestIsPresentOnlyForCompletedEvents`
passed in the focused 4-test audit suite. The full `./gradlew check` passed
1538 tests with 24 skips and zero failures/errors. This closes only local
audit phase/digest shape validation; distributed authority, transport
delivery, failover, chaos and release gates remain open.

### 2026-08-16 Gateway active attempt tail fence implementation note

Delay commit `a1a85f99471743c48126943fad92fbb80ce6be34` makes the
`GatewayIdempotencyRecordV1` ACTIVE projection match the Registry lifecycle
shape: there is at most one `STARTED` attempt, and it must be the final
source-ordered attempt. This still permits late evidence on an earlier
terminal/uncertain attempt while a newer final attempt is active, but rejects
two simultaneous starts and a terminal attempt after an unresolved STARTED
entry.

The deterministic idempotency suite passed 11 tests with zero
failures/skips/errors, and the full `./gradlew check` passed 1538 tests with
24 skips and zero failures/errors. This closes only local active-attempt
projection integrity; distributed authority, transport delivery, failover,
chaos and release gates remain open.

### 2026-08-16 Gateway attempt timing/retry shape implementation note

Delay commit `e0d5bc9761fea57103518819165d54eb60662b99` aligns
`GatewayPhysicalAttemptV1` construction and strict decode with the Registry:
the uncertainty and ownership boundaries must advance beyond the start, the
ownership boundary cannot exceed uncertainty, attempt 1 cannot carry retry
identity, and every later attempt must carry the retry ID/hash pair.

The deterministic idempotency suite passed 11 tests with zero
failures/skips/errors; the full `./gradlew check` passed 1538 tests with 24
skips and zero failures/errors. This closes only local physical-attempt
temporal/retry-shape validation; distributed authority, transport delivery,
failover, chaos and release gates remain open.

### 2026-08-16 Gateway queued aggregate tail fence implementation note

Delay commit `5b4d99e3` makes the stored Gateway attempt history reject any
attempt after a persisted `QUEUED` attempt. The V1 queued aggregate is sticky:
once a physical attempt has authenticated persistence, a later retry cannot
become an active or terminal sibling. The validator now rejects a queued
attempt followed by any later entry, including a final `STARTED` attempt,
before it can be consumed by either idempotency store.

The fence does not reject the existing valid path where an earlier unresolved
attempt remains uncertain while a newer retry is definitely not queued; that
history can still accept a later retry under the highest unresolved attempt
precondition. `OxiaGatewayIdempotencyStoreTest.gatewayProjectionRejectsImpossibleAttemptAndRecordShapes`
covers the queued-tail regression. The focused deterministic idempotency suite
and the full `./gradlew check` passed with 1538 tests, 24 skips, zero failures
and zero errors. This closes only local sticky-queued projection integrity;
distributed authority, transport delivery, failover, chaos and release gates
remain open.

### 2026-08-16 Pulsar managed SEND evidence identity fence implementation note

Delay commit `4ed28c89f6cf9e20c12f1ee226752327f05f7953` closes the opt-in
Pulsar managed-SEND evidence binding gap. Before constructing a `PERSISTED`
`PulsarSendResult`, `PulsarClientArtifactSendTransport` now compares the
`GuardedSendSuccessEvidence` attestation with the exact producer guard,
physical topic and partition, and compares evidence ledger/entry/timestamp
with the returned `MessageIdAdv` and `GuardedMessageId` values. A mismatch is
`UNKNOWN/INTEGRITY_ERROR`, never a queued receipt.

The source-locked `compileRealPulsar` task passed against the P1 worktree
classes/JARs, and `runRealPulsarSmoke` passed with
`persisted=PERSISTED, mismatch=UNKNOWN, rejection=DEFINITIVELY_NOT_PERSISTED`.
The smoke's mismatched-ledger regression would have been accepted as
`PERSISTED` before this fence. This closes only the local opt-in SEND evidence
projection; Broker rollout, multi-broker failover, source/ACK integration,
Worker production wiring and release gates remain open.

### 2026-08-16 Kafka client metadata identity fence implementation note

Delay commit `1ab1d53fa4e14235fbb510035f2afaeea1ff3605` closes the symmetric
opt-in K1 binding gap for native response metadata. The ordinary
`KafkaClientArtifactProduceTransport` now requires the returned
`RecordMetadata.topic()` and `partition()` to equal the pinned request before
it can return `PERSISTED`. The K2 transactional destination binding applies
the same check independently to both the business target and keyed receipt
metadata before `PUBLISHED` can be reached.

The source-locked `compileRealKafka` task passed against the K1 client artifact
`kafka-clients-4.4.0-SNAPSHOT.jar`. No Kafka broker was listening in the local
environment, so the broker-dependent K1/K2 smoke was not run. This closes only
the local opt-in metadata-to-guard identity projection; Kafka Broker rollout,
multi-broker failover, read-committed receipt authority, source/ACK
integration, Worker production wiring and release gates remain open.

### 2026-08-16 recovered publish evidence identity fence implementation note

Delay commit `c6b6a5f9b52e8f5c358e047218ee606ea58aed3f` closes the provider
recovery promotion gap for the opt-in K2 and Pulsar destination bindings.
`KafkaTransactionalPublishEvidence.requireExactBinding` now checks the
read-committed cursor channel, receipt offset, prepared hash, target resource
and partition, transactional identity and exact receipt-record hash before K2
can return `PUBLISHED`. `PulsarSendAckEvidence.requireExactBinding` applies
the corresponding exact target, partition, prepared hash, producer hash and
broker-persistence-time checks before an uncertain SEND can be promoted.

The provider remains responsible for the live Fetch/reread proof; the
transport now owns the final request/evidence identity fence. The focused
evidence tests and source-locked `compileRealKafka`/`compileRealPulsar` tasks
passed. Follow-up commit `df2d021fc7e8c5586b062870325efa71835b6d3b` retains
the explicit K2 owner check required by the cross-repository contract audit.
This closes only local recovered-evidence binding; read-committed or
Pulsar reread authority, Broker rollout/failover, source/ACK integration,
Worker production wiring and release gates remain open.

### 2026-08-16 Pulsar Large-payload Gateway-to-destination authority receipt

Delay implementation commit
`accdc7074bfd38aed2cfd7c696a8c3ff62a972ba` adds the source-bound
`PulsarClientArtifactLargePayloadGatewaySmoke` path and the isolated runner
`e2e/run-pulsar-large-payload-gateway-e2e.sh`. The receipt command was:

```bash
bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The run locked P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
its distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and the locked MinIO image
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The isolated Compose project was
`nereus-delay-pulsar-large-e2e-1786879186-27914` with service/admin
`29114/29115`, broker-2 `29116/29117`, Oxia `29124`, MinIO `29125`, Gateway
`29126`, and destination `pulsar-large-payload-destination-27914`.

The source-bound output was:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=2/4, typed PULSAR_SEND_ACK target ledger/entry=3/0, Outcome source ledger=2/5, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
BUILD SUCCESSFUL
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed
```

This receipt binds the production path from real Gateway mTLS/JWT and Oxia
admission/idempotency/audit through source-ordered Worker apply, versioned
MinIO upload/attestation/readback, due/Claim/Publish Admission/
`PUBLISHING`, typed Pulsar destination evidence, source outcome application and
exact 1 MiB + 4 KiB destination bytes. The Worker released its final local
checkpoint and Oxia Owner; the duplicate Prepare returned identical bytes
without a sixth-plus command append beyond the expected six source records.

This is not a release claim. The harness has one physical source partition,
one ZooKeeper and one BookKeeper and does not combine the Gateway path with
the separate multi-Broker failover cut. Multi-shard placement, raw
crash/network/proxy/process chaos, Kafka LSO/retention recovery, Object Store
checkpoint publication and V1 release gates remain open.

### 2026-08-16 Pulsar Large-payload clean production-authority revalidation

The normal Large Payload Gateway-to-destination path was rerun from clean Delay
commit `667458b98bd5adcec04eae53e2d2fe7da157be8c` after the guarded source
reconnect replay, recovered `UNKNOWN` Publish Admission handling and exact
Compose cleanup changes. The source-bound command was:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-large-revalidation-20260816 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

The source locks were P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1 distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image `sha256:4faa8217a39de36a030e449473fc07f4cd04553477f4f2e84c5d799720989cf0`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The isolated project was
`nereus-delay-pulsar-large-e2e-1786884946-97580`, using Pulsar
`29180/29181`, broker-2 `29182/29183`, Oxia/MinIO/Gateway
`29190/29191/29192`, and destination
`pulsar-large-payload-destination-97580`.

The clean revalidation reported:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/4, typed PULSAR_SEND_ACK target ledger/entry=4/0, Outcome source ledger=3/5, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=3/2, commit=3/3, exactGatewayIdempotency=true, sourceRecords=6
BUILD SUCCESSFUL in 57s
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed
```

This is current positive evidence for the normal real Gateway + Oxia + Pulsar
Worker + versioned MinIO production-authority chain and exact
`1,052,672`-byte destination readback. The exact Compose project and temporary
P1/Oxia images were absent after cleanup. It does not promote the experimental
combined multi-Broker failover mode or the untriggered recovered `UNKNOWN`
response-loss branch, and it does not close multi-shard, chaos, checkpoint
publication or V1 release gates.

### 2026-08-16 Pulsar Worker destination response-loss with real Oxia

The focused Worker destination response-loss mode was corrected at Delay
`b647176ed92491fd96514eed2b87098454078a79` so its real-Oxia option is passed
into the focused Worker process. The source-bound command was:

```bash
NEREUS_DELAY_PULSAR_WITH_OXIA=1 \
NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS=1 \
NEREUS_DELAY_PULSAR_WORKER_DESTINATION_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_PULSAR_GRADLE_USER_HOME=/tmp/nereus-delay-pulsar-worker-destination-response-loss-oxia-20260816 \
  bash e2e/run-pulsar-real-client-e2e.sh
```

The run locked P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, client
artifacts `57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394`, and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, P1
image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. Pulsar used Compose
project `nereus-delay-pulsar-e2e-1786885612-9737` on `19787/19788`; Oxia used
`nereus-delay-pulsar-oxia-e2e-1786885612-9737` on `16657`.

The source-bound output was:

```text
Pulsar Worker assignment publication/acceptance passed: revision=1, worker=pulsar-worker, authority=real Oxia session-bound
Pulsar Worker destination response-loss smoke passed: real SEND persisted the exact payload, the local response was discarded, and typed PULSAR_SEND_ACK evidence resolved the source-applied PUBLISHED Outcome
Pulsar Worker source-applied physical publish passed: Admission source ledger=9/3, typed PULSAR_SEND_ACK target ledger/entry=10/0, Outcome source ledger=9/4, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=9/0, active apply ledger/entry=9/1, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
BUILD SUCCESSFUL in 1m 1s
Pulsar Worker destination response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and the source-applied Outcome completed.
```

This is positive evidence for the bounded real-Oxia Worker destination SEND
response-loss path and its exact typed evidence/readback. The exact temporary
containers, images, volumes and networks were absent after cleanup. The
admission in this run was the normal `ENQUEUED` path, so this does not claim
execution of the recovered `UNKNOWN` Publish Admission branch, raw socket or
process loss, combined Gateway multi-Broker failover, checkpoint publication,
multi-shard placement or V1 release readiness.

### 2026-08-16 Worker checkpoint publication with real Oxia and MinIO

Delay commit `7a2b7b7461dd56ff5c3ebbc0e5471756d148ad18` adds the isolated
`e2e/run-oxia-minio-checkpoint-e2e.sh` runner and the focused
`workerCheckpointRuntimePublishesToRealMinioAndOxia` smoke. It binds the
scheduled Worker checkpoint execution to a real Oxia session-bound Owner Lease,
the canonical Oxia PUBLISHED Intent/Catalog record, and the S3-compatible
adapter against a real versioned MinIO bucket. The exact provider resource is
downloaded back and its checkpoint file inventory is compared with the
published manifest.

The source-bound command was:

```bash
./e2e/run-oxia-minio-checkpoint-e2e.sh
```

The receipt used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, locked MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`,
Compose project `nereus-delay-oxia-minio-checkpoint-e2e-1786886192-18395`,
and ports `16719/16729`.

The live output was:

```text
Oxia + MinIO Worker checkpoint publication passed: atomic Intent/Catalog=true, immutable object upload/download=true, checkpoint=00000000000000000000000000000003
BUILD SUCCESSFUL
Oxia + MinIO Worker checkpoint publication E2E passed: real Oxia Intent/Catalog authority and real MinIO immutable objects
```

This is a positive Worker/object-authority receipt, not a release claim. It
does not prove real-Oxia REAPING/RecoveryPin competition, provider-side
quiescence/consistency attestation, late-PUT or delete response-loss handling,
restore activation, multi-shard placement, raw chaos or V1 release gates.
The runner removed its exact Compose resources and temporary Oxia image while
retaining the locked MinIO base image.

### 2026-08-16 Kafka source Fetch response-loss receipt

Delay commit `8f1116abad2bd77e2f384c04411dabaeb70b4f72` adds
`KafkaClientArtifactFetchResponseLossSmoke`, which composes the locked K1
guarded source consumer with a real three-Broker KRaft test mode:

```bash
NEREUS_DELAY_KAFKA_FETCH_RESPONSE_LOSS_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-fetch-response-loss-gradle \
  bash e2e/run-kafka-real-client-e2e.sh
```

The receipt used K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:b0fcef7eb6f8350af6c22d333de889155acf4b1ec157887266568fc78beada0e`,
Delay `8f1116abad2bd77e2f384c04411dabaeb70b4f72`, and Compose project
`nereus-delay-kafka-e2e-1786879840-36136` on `19228,19229,19230`.

The output was:

```text
Kafka source Fetch response-loss smoke passed: responseDiscardedAfterFetch=true, replayOffset=0, secondOffset=1, fetchLso=2, committedAfterReplay=2
BUILD SUCCESSFUL
Kafka source Fetch response-loss E2E passed: real read_committed Fetch v13 response was discarded before ACK, exact source replay and LSO coverage were recovered.
```

This establishes the bounded Fetch uncertainty rule with real Broker data:
the response was discarded after the exact offset-0/1 batch and its LSO=2 had
been received, a fresh same-group source consumer replayed offset 0, then
source ACK committed offset 2. This is controlled client-side response loss;
raw socket loss, coordinator/process/Broker crash cuts, multi-shard placement,
checkpoint publication and release gates remain outside this receipt.

### 2026-08-16 Kafka source retention-floor receipt

Delay commit `d8dc5f45` adds
`KafkaClientArtifactRetentionFloorSmoke`, the
`runRealKafkaRetentionFloorSmoke` task and the opt-in mode:

```bash
NEREUS_DELAY_KAFKA_RETENTION_FLOOR_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-retention-floor-gradle-2 \
  bash e2e/run-kafka-real-client-e2e.sh
```

The source-bound receipt locks K1 to
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Delay `d8dc5f45`, and Compose project
`nereus-delay-kafka-e2e-1786880647-45643` on `19235,19236,19237`.

It prints:

```text
Kafka source retention-floor smoke passed: oldOffset=0, retentionFloor=20, endOffset=21, staleOffsetRejected=true, floorFetchOffset=20, fetchLso=21
BUILD SUCCESSFUL
Kafka source retention-floor E2E passed: real Broker retention advanced the earliest offset, stale source offset was rejected, and the current floor remained readable through guarded Fetch v13 with LSO.
```

The smoke produces twenty guarded records, waits for real Broker retention to
advance the earliest offset from `0` to `20`, appends a current tail at offset
`20`, and rejects a stale guarded Fetch as a typed
`ConsumerResourceGuardException` carrying `OFFSET_OUT_OF_RANGE`. It then
reads the floor record through guarded `read_committed` Fetch v13 with
`lastStableOffset=21`. This is the source retention-floor contract; raw socket
loss, disk-full behavior, consumer-coordinator/process/Broker crash recovery,
multi-shard placement, checkpoint publication, chaos and release gates are
separate obligations.

### 2026-08-16 Kafka source process-crash recovery receipt

The focused process-crash mode composes the locked K1 client with a real
three-Broker KRaft cluster:

```bash
NEREUS_DELAY_KAFKA_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/tmp/nereus-delay-kafka-process-crash-e2e-receipt \
  bash e2e/run-kafka-real-client-e2e.sh
```

The post-commit receipt passed at Delay
`2bcaff5e0c0b15b819cbc614c166c47e19571be3`, Kafka
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
and Compose project `nereus-delay-kafka-e2e-1786881618-58469` on ports
`19561,19562,19563`.

It printed:

```text
Kafka source process-crash cut reached: fetchedOffsets=0,1, fetchLso=2, responseAcked=false, consumerClosed=false
Kafka source process-crash recovery smoke passed: crashExit=86, replayOffset=0, secondOffset=1, committedAfterRecovery=2
BUILD SUCCESSFUL in 5s
Kafka source process-crash recovery E2E passed: the crashed JVM fetched exact guarded records without ACK, and a fresh same-group process replayed offsets 0 and 1 before committing offset 2.
```

The crash process deliberately halts after the real Fetch response and before
source ACK or consumer close. The resume process uses the same deterministic
source command identities and group, verifies exact replay at offsets `0` and
`1`, then commits offset `2`. This closes only the isolated JVM process-crash
source-cursor slice; raw network/proxy/socket loss, coordinator/Broker crash or
leader failover, Worker apply/publish crash, multi-shard placement, checkpoint
publication, the broader chaos matrix and V1 release gates remain open.

### 2026-08-16 Checkpoint REAPING real Oxia/MinIO receipt

Delay commit `d58ca4d7038c994c4415898b91362760a01896d0` adds
`OxiaRealCheckpointReapingSmokeTest` to the locked
`e2e/run-oxia-minio-checkpoint-e2e.sh` runner. The live invocation used Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, versioned MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
MinIO image ID
`sha256:8f08aee614800a237906bd48114d733e5ac5bfac4ccdf731f141b0e880d7a253`,
and Compose project `nereus-delay-oxia-minio-checkpoint-e2e-1786888377-45712`.

The real-service result was:

```text
Oxia + MinIO checkpoint REAPING authority passed: real Owner abandonment=true, real Intent PENDING_UPLOAD->REAPING=true, exact-version prefix sweep=2, finalEmptyPrefix=true, localProviderOwnershipClosed=true
```

The test runs a real session-bound Owner Lease release and absence reread,
creates the exact pending upload Intent through real Oxia, fences the uploader
generation, then lets the coordinator win the `PENDING_UPLOAD -> REAPING` CAS
and invoke a separate real MinIO adapter for the exact one-page version sweep.
The final MinIO listing is empty. `localProviderOwnershipClosed` is deliberately
only a local ownership-horizon observation; it is not a provider-side
quiescence or consistency certificate. RecoveryPin/cross-record transaction,
source-ordered delete confirmation, response-loss retry, multi-shard runtime,
raw chaos and release gates remain explicit boundaries.

### 2026-08-16 Kafka Broker SIGKILL Worker recovery receipt

Delay commit `2a560a9d3f288b08bd02e139c52f4cfe6fda8ff3` adds a focused real
Broker process-crash mode to `e2e/run-kafka-real-client-e2e.sh`:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16679 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-kafka-real-client-e2e.sh
```

The receipt binds K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and isolated projects
`nereus-delay-kafka-e2e-1786888793-51634` /
`nereus-delay-kafka-oxia-e2e-1786888793-51634`.

The runner performs Worker preparation, sends `SIGKILL` to `kafka-1`, resumes
the same topic through the survivor bootstrap, and starts `kafka-1` again. It
reported:

```text
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Broker process-crash recovery E2E passed: kafka-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia authority, and kafka-1 rejoined afterward.
```

The boundary is deliberately narrower than a production chaos PASS: no raw
network/proxy/socket fault, coordinator/controller leader proof, Worker
apply/publish crash, production multi-shard runtime or V1 release gate is
claimed. Exact Compose resource and temporary image cleanup passed.

### 2026-08-16 Kafka Broker network-partition Worker recovery receipt

Delay commit `5460746c74b2a4cc05f9ecfb71c5d2a285828380` adds a focused network
partition mode to `e2e/run-kafka-real-client-e2e.sh` and the Java Admin
survivor-leader gate:

```bash
NEREUS_DELAY_KAFKA_WITH_OXIA=1 \
NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY=1 \
NEREUS_DELAY_KAFKA_OXIA_PORT=16689 \
NEREUS_DELAY_KAFKA_GRADLE_USER_HOME=/Users/liusinan/.gradle \
  bash e2e/run-kafka-real-client-e2e.sh
```

The receipt binds K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and isolated projects
`nereus-delay-kafka-e2e-1786889717-63599` /
`nereus-delay-kafka-oxia-e2e-1786889717-63599`.

The runner disconnects the live Broker process from the Compose network,
waits for all three Worker-related topic leaders to move to the survivors,
performs source-only Worker recovery/apply/ACK/checkpoint through
`kafka-2,kafka-3`, and reconnects `kafka-1`. It reported:

```text
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Broker network-partition recovery E2E passed: kafka-1 stayed alive but was disconnected from the Compose network after guarded Worker preparation, the same topic resumed through kafka-2/kafka-3 with real Oxia Worker authority and source apply/ACK/checkpoint, and kafka-1 reconnected afterward.
```

This is a bounded Docker bridge partition receipt. It does not claim physical
destination egress during the partition, raw packet/proxy/socket injection,
controller/coordinator leader proof beyond the topic-leader gate,
multi-shard production or V1 release readiness. Exact Compose resource and
temporary image cleanup passed.

## Kafka Worker JVM SIGKILL recovery receipt

The source-bound implementation is Delay `d35dce96`. Its focused
`NEREUS_DELAY_KAFKA_WORKER_PROCESS_CRASH_ONLY=1` branch starts a real K1
three-Broker cluster and real Oxia, opens the guarded Worker source/runtime,
and waits at a gate while the next source record is unACKed. The harness kills
the recorded Worker JVM PID with `SIGKILL`; a fresh JVM reopens the same local
Store root, reacquires the Oxia lease, replays/ACKs the record and publishes the
final checkpoint.

The live receipt used K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, projects
`nereus-delay-kafka-e2e-1786890291-72188` /
`nereus-delay-kafka-oxia-e2e-1786890291-72188`, ports
`19280,19281,19282/16709`, and temporary Oxia image
`sha256:803fdb3a48af0411170bc96e81bcb39bd5674c8766a105973dfed8cc46bcc449`.

This is a bounded Worker JVM/source replay receipt. It does not promote a
crash during destination physical publish, raw packet/proxy/socket chaos,
controller/coordinator leader failover, production multi-shard runtime or V1
release readiness. The exact Compose resources and temporary images were
removed after the run; no global Docker prune was used.

## 2026-08-16 Kafka Worker durable-apply-before-ACK SIGKILL receipt

The source-bound implementation is Delay `2cfc207f`. Its focused
`NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY=1` branch starts a real K1
three-Broker cluster and real Oxia, then opens the guarded Worker source/runtime
against a fresh local Store root. A test-only `GuardedConsumer` proxy reaches a
gate after the Store WriteBatch is durable and before the source
`commitSync` ACK starts. The harness kills the exact recorded Worker JVM PID
with `SIGKILL`; a fresh JVM reopens the same Store root, reacquires the real
Oxia lease, replays and deduplicates the source record, ACKs it, and publishes
the final checkpoint.

The live receipt used K1
`nereus/delay-guarded-producer-v1@05849884ca81fad767fda058444d1e17c7f9cbf9`,
client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, broker
image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, projects
`nereus-delay-kafka-e2e-1786890684-77735` /
`nereus-delay-kafka-oxia-e2e-1786890684-77735`, ports
`19327,19328,19329/16719`, topic
`nereus-delay-worker-ack-crash-live-20260816`, and temporary Oxia image
`sha256:6b8082d3b205230306c243b332a02c1c9d3ecd9c4286ae22b90743a0fc80d26c`.

The source-bound gate and final output were:

```text
Kafka Worker ACK process-crash cut reached: pid=78620, storeWriteBatchDurable=true, kafkaCommitSyncStarted=false
Kafka Worker vertical smoke passed: assignment recovery offset=0, active apply offset=1, guarded Fetch v13, RocksDB WriteBatch, commitSync ACK, and final checkpoint
Kafka Worker authority smoke passed: real Oxia session-bound lease
Kafka Worker ACK process-crash recovery E2E passed: the Worker Store WriteBatch was durable before SIGKILL and before Kafka commitSync ACK, and a fresh JVM replayed the exact source record through real Oxia authority, dedupe, ACK and final checkpoint.
```

This is positive evidence for the narrow Store-durable-before-source-ACK
process cut and replay path. It does not prove a crash during destination
physical publish, raw packet/proxy/socket chaos, controller/coordinator leader
failover, production multi-shard runtime or V1 release readiness. Exact cleanup
found no containers, networks, volumes or temporary Kafka/Oxia images for the
two named projects; base images were retained and no global Docker prune was
used.

## 2026-08-16 Raw Kafka endpoint-cut boundary

The Kafka fault matrix now has a source-Worker receipt for a live Broker-1
public-endpoint cut. The harness is intentionally explicit about the authority
sequence:

1. A real Kafka Admin client places the source partition on Broker-2 and puts
   the `__consumer_offsets` partition selected by Kafka's actual
   `Utils.abs(groupId.hashCode()) % partitionCount` rule on Broker-2.
2. Broker-1 remains running. A local raw TCP proxy closes the pre-cut sockets,
   rejects one new connection to the Broker-1 public endpoint, and forwards
   later connections on that endpoint to Broker-2.
3. A fresh Worker uses the complete bootstrap list, real Oxia assignment and
   Owner Lease authority, then proves source replay/apply, guarded ACK and final
   checkpoint.

The exact receipt used Delay `79d4617c`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, source
leader and coordinator `leader=2, replicas=[2, 3, 1]`, and real Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The runner required raw
pre-cut, cut-ack, post-cut rejection and post-cut handoff marker files.

This boundary must not be generalized to automatic Kafka controller or group
coordinator failover: the placement and endpoint handoff are test authority
actions. Broker crash, Docker network partition, production proxy/load-balancer
behavior, multi-shard scheduling, the full chaos matrix and the V1 release gate
remain separate obligations.

## 2026-08-16 Pulsar source reactivation successor implementation note

The Pulsar large-payload Gateway harness now has an explicit successor
protocol for the bounded two-Broker source-reactivation case. The canonical
`PulsarSourceReactivationV1` transition binds the route snapshot digest, the
previous and successor `SourceAssignment`, and the immutable Pulsar resource,
topic, cursor, batch shape and attestation. It requires a new assignment ID,
new assignment epoch and a different guarded source connection generation;
changing source identity or reusing the old generation is rejected.

`PulsarSourceReactivationCoordinator` owns the authority sequence: exact
Route/Assignment/Owner validation, Oxia `ACTIVE -> FENCED` CAS, caller-proved
source quiescence, exact old Owner release, successor Assignment revision CAS,
and successor Owner acquisition only after the successor is durable. The old
`WorkerShardRuntime` source loop is closed only through
`closeForOwnerReactivation`, preserving Store and lease authority until the
quiescence proof is recorded. This is a reactivation boundary, not a weakening
of `RouteSnapshotCompatibilityV1` or `SourceAssignment.sameIdentity`, and it
does not claim automatic Pulsar controller/coordinator failover or production
multi-shard placement.

The source-bound implementation is Delay
`49665a75041ea05cd7b47e887c9e28fa08647b9`; its deterministic
`PulsarSourceReactivationTest` covers canonical round-trip/rejection, the
fence-quiesce-publish-acquire sequence and the failed-quiescence fence. The
real E2E source lock additionally covers Gateway mTLS/JWT, real Oxia, two real
Pulsar Brokers, Worker source apply and real MinIO exact payload readback.

## 2026-08-16 Kafka current large-payload destination implementation note

The current Kafka source-bound receipt reruns the existing
`KafkaClientArtifactLargePayloadGatewaySmoke` composition at Delay
`eb8e4a9df859316253202ba3abfb48236bf64196` with the destination branch
enabled. It binds the signed Route, session-bound Oxia Assignment/Owner, Gateway
mTLS/JWT admission and idempotency, Worker Prepare/Commit, versioned MinIO
payload proof, typed `KAFKA_TRANSACTIONAL_RECEIPT`, source-applied Outcome and
exact destination payload readback.

This remains the one-partition destination-authority slice. The current live
receipt is evidence for the Kafka path only; it does not collapse separately
required response-loss/LSO/retention, raw crash/chaos, catalog-driven
multi-shard or release-gate obligations into this smoke.

## 2026-08-16 Current multi-shard placement implementation note

The real Oxia placement harness remains intentionally below native Worker
fleet authority. At Delay `b059d99aef1793f56c4b33d4293ec141e20c4d96`,
`OxiaRealRouteWorkerAssignmentSmokeTest` placed two Route partitions on two
workers after reflecting the first committed capacity, reread both signed
assignments and withdrew each exact identity through session-bound CAS.

The next production boundary is to bind each accepted assignment to a native
Kafka/Pulsar source consumer, Owner Lease, catch-up, scheduler and source
ACK/checkpoint lifecycle. Until that exists, the current evidence must remain
“multi-shard placement authority”, not “multi-shard Worker production”.

## 2026-08-16 Kafka native multi-shard Worker fleet implementation note

The Kafka Route smoke now has an explicit `NEREUS_DELAY_KAFKA_ROUTE_WORKER_SHARDS=2`
path, exposed by `NEREUS_DELAY_KAFKA_MULTI_SHARD_ONLY=1`. It publishes one
signed `RouteSnapshotV1` containing two partition-specific guarded Fetch/LSO
barriers. Each partition is independently projected through the real Oxia
Assignment CAS and Owner Lease, recovered from offset zero, and admitted to a
native guarded Kafka source. The two `WorkerShardRuntime` instances share one
`SharedRocksDbResources`, one `WorkClassExecutionRegistry` and one
`WorkerShardFleetRuntime`; fleet source turns are round-robin and no assignment
or lease authority is moved into the fleet.

The source-bound run at Delay `c6b2d0ea` passed both partition apply/ACK paths,
committed source offsets, per-shard final checkpoint, exact Owner release and
exact Assignment withdrawal against K1
`05849884ca81fad767fda058444d1e17c7f9cbf9` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. This closes the Kafka native
two-shard Worker source composition. It deliberately does not imply native
Pulsar multi-shard wiring, multiple Worker processes, scheduler/placement
churn, raw chaos completeness or V1 release approval.

## 2026-08-17 Pulsar native multi-shard Worker fleet implementation note

The Pulsar Route smoke now has an explicit
`NEREUS_DELAY_PULSAR_ROUTE_WORKER_SHARDS=2` path, exposed by
`NEREUS_DELAY_PULSAR_MULTI_SHARD_ONLY=1`. It creates one real partitioned
Pulsar topic and stamps each physical partition with the guarded resource
identity. Each partition sends and replays its own pre-Route command, captures
its guarded SUBSCRIBE connection-generation/attestation proof, and contributes
one exact `PulsarActivationBarrier` to the single signed `RouteSnapshotV1`.

Each Route partition is independently projected through real Oxia Assignment
CAS and Owner Lease acquisition. The Worker recovers the exact pre-Route
record, ACKs it on the original guarded source, sends the post-barrier record
with its explicit physical partition, and admits the source runtime only when
the connection-generation proof still matches. Two `WorkerShardRuntime`
instances share one `SharedRocksDbResources`, one
`WorkClassExecutionRegistry` and one `WorkerShardFleetRuntime`; source turns
are round-robin while Assignment and Owner authority remain per shard. Each
runtime drains its final checkpoint before exact Assignment withdrawal.

The source-bound run at Delay `c2003627` passed against Pulsar
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. This closes the Pulsar native
two-shard Worker source composition for one P1 Broker. It deliberately does
not imply Pulsar multi-Broker failover, multiple Worker processes,
scheduler/placement churn, raw chaos completeness or V1 release approval.

## 2026-08-17 Current Kafka Broker network-partition implementation note

The existing Kafka real-client runner exposes
`NEREUS_DELAY_KAFKA_BROKER_NETWORK_PARTITION_ONLY=1` as a focused source
recovery cut. It first opens the guarded Worker source and persists one
record, then removes only the live `kafka-1` container's membership from the
three-Broker Compose network. `KafkaClientArtifactSurvivorLeaderRecoverySmoke`
uses the survivor bootstrap list and the real Admin API to verify source,
destination and receipt topic leaders before the Worker is resumed.

The resumed Worker uses the same exact source topic and real Oxia
Assignment/Owner authority, recovers the unacknowledged record, applies it,
commits the guarded Kafka ACK and final checkpoint, and only then does the
runner reconnect and readiness-check `kafka-1`. The current source-bound run
locks Delay to `35745db08672f1bf2e3178419422a46741da20d1`, K1 to
`05849884ca81fad767fda058444d1e17c7f9cbf9` and Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`.

This is deliberately a bounded Docker-network membership cut. It does not
turn the topic-leader check into controller/coordinator failover evidence and
does not cover physical destination publish during the partition, raw
packet/proxy/socket injection, production multi-shard fault coverage or the
V1 release gate.

## 2026-08-17 Current Kafka raw TCP Broker endpoint-cut implementation note

The focused `NEREUS_DELAY_KAFKA_BROKER_TCP_CUT_ONLY=1` path starts a real
three-Broker K1 cluster plus real Oxia and inserts
`KafkaClientArtifactTcpFaultProxy` in front of Broker-1's public endpoint.
Before cutting, `KafkaClientArtifactLeaderPlacementSmoke` uses the real Admin
API to place both the one-partition source leader and the selected fixed-group
`__consumer_offsets` partition on Broker-2, with Broker-1 still alive.

The proxy then closes existing sockets, rejects exactly one new Broker-1
endpoint connection, and hands a later connection on that same endpoint to
Broker-2. The resumed Worker keeps the original guarded source identity and
full bootstrap list, reopens the real Oxia Assignment/Owner authority, applies
the unacknowledged source record, commits the source ACK and final checkpoint,
and the runner requires pre-cut forward, cut-ack, post-cut rejection and
post-cut handoff markers before passing.

The current source-bound run locks Delay to
`47fa6620e7816dbd13ea393b42891a53286009ec`, K1 to
`05849884ca81fad767fda058444d1e17c7f9cbf9` and Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`. This remains an explicit endpoint
fault/handoff boundary, not automatic controller/coordinator failover,
Broker crash recovery, Docker network partition, destination egress under
the cut, multi-shard chaos or the V1 release gate.

## 2026-08-17 Current Kafka Worker durable-apply-before-ACK implementation note

The focused `NEREUS_DELAY_KAFKA_WORKER_ACK_PROCESS_CRASH_ONLY=1` path runs the
real Worker against a three-Broker KRaft cluster and real Oxia. The first JVM
opens the guarded source, applies the record through the local RocksDB
WriteBatch, reaches a cut gate only after the Store reports durability and
before the guarded Kafka `commitSync` begins, and publishes its PID for the
harness.

The runner then sends SIGKILL and starts a fresh JVM against the same isolated
Store root and source topic. The successor must reacquire the session-bound
Oxia Assignment/Owner authority, replay the still-uncommitted source record,
deduplicate the durable local apply, commit the source ACK and release the
final checkpoint. This keeps the Store durability-before-ACK invariant
explicit instead of treating a process restart as generic success.

The current source-bound run locks Delay to
`ade0c813bb8919793eecdd2e07cf76073432237f`, K1 to
`05849884ca81fad767fda058444d1e17c7f9cbf9` and Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`. It remains a bounded Worker
process-cut contract and does not imply destination-publish crash recovery,
raw network chaos, controller/coordinator failover, production multi-shard
fault coverage or V1 release readiness.

## 2026-08-17 Current Kafka Broker process-crash implementation note

The focused `NEREUS_DELAY_KAFKA_BROKER_PROCESS_CRASH_ONLY=1` path prepares one
guarded Worker record against a real three-Broker KRaft cluster and real Oxia,
then sends SIGKILL to `kafka-1`. The survivor bootstrap list is used to reopen
the same source, recover the Oxia Assignment/Owner authority and run the full
Worker path: guarded source replay, RocksDB apply, physical Kafka destination
publish, typed transactional receipt/readback, source ACK and final
checkpoint.

Only after the survivor Worker completes does the runner start `kafka-1` and
wait for readiness. The current source-bound run locks Delay to
`13857e57cee134c2bc0fcf20a4d8b988fbe0f02a`, K1 to
`05849884ca81fad767fda058444d1e17c7f9cbf9` and Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`. This remains a bounded Broker
process-cut contract and does not imply raw endpoint/network fault injection,
controller/coordinator failover, production multi-shard chaos or V1 release
readiness.

## 2026-08-17 Current Kafka Fetch response-loss implementation note

The current source-bound Fetch rerun locks Delay to
`a3bb8462edc3d4e32006f5d98af958d1c8d7ef18`, K1 to
`05849884ca81fad767fda058444d1e17c7f9cbf9`, the K1 client SHA-256 to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
the broker image ID to
`sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e6`.
The runner's three-Broker KRaft project was
`nereus-delay-kafka-e2e-1786898037-72717`.

`KafkaClientArtifactFetchResponseLossSmoke` uses the K1 patched
`GuardedConsumer.pollGuarded` path and `GuardedFetchEvidence` before source
ACK. The focused run deliberately discards the first real Fetch response,
reopens the same group, requires the exact replay offsets and an LSO covering
the batch, then advances the group offset only after guarded evidence is
validated. The receipt was:

```text
Kafka source Fetch response-loss smoke passed: responseDiscardedAfterFetch=true, replayOffset=0, secondOffset=1, fetchLso=2, committedAfterReplay=2
```

This is a current-source implementation/evidence refresh for the controlled
Fetch response-loss and LSO replay contract. Raw socket loss, retention-floor
recovery, Broker/coordinator crash, multi-shard placement, checkpoint
publication, chaos and V1 release gates remain separate.

## 2026-08-17 Current Kafka retention-floor implementation note

The current source-bound retention rerun uses the same K1 guard/evidence
path and a three-Broker KRaft project
`nereus-delay-kafka-e2e-1786898140-73898`. The smoke produces twenty guarded
records under an accelerated retention interval, observes the real earliest
offset move from `0` to `20`, appends a fresh tail, and tests both sides of
the floor: a stale guarded Fetch must fail closed with typed
`OFFSET_OUT_OF_RANGE`, while a Fetch at the new floor must carry valid
`GuardedFetchEvidence` and an LSO of `21`.

The current-source receipt was:

```text
Kafka source retention-floor smoke passed: oldOffset=0, retentionFloor=20, endOffset=21, staleOffsetRejected=true, floorFetchOffset=20, fetchLso=21
```

This closes the bounded retention-floor/LSO readability implementation
slice. The acceleration is test-only; disk ENOSPC, raw socket/process
chaos, automatic controller/coordinator failover, multi-shard placement,
checkpoint publication and V1 release readiness remain unclaimed.

## 2026-08-17 Current Pulsar multi-Broker Worker failover implementation note

The current source-bound failover rerun locks Delay to
`19577006e4c104b2934617719b711aa5d549ed27`, P1 to
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
the P1 image to
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`.
The real cluster uses two P1 Broker services backed by one ZooKeeper and one
BookKeeper service; the run also uses real Oxia at `127.0.0.1:16765`.

The harness first runs the guarded Worker preparation path against Broker-1,
then stops Broker-1 and starts a fresh Worker with the multi-endpoint service
URL. The Worker must observe the failed first endpoint, connect through
Broker-2, reacquire the Oxia Assignment/Owner session, validate the guarded
SUBSCRIBE evidence, apply to RocksDB, publish the physical destination and
typed `PULSAR_SEND_ACK` evidence, ACK the source and release the final
checkpoint. Broker-1 is restarted only after that receipt.

The current-source receipt was:

```text
Pulsar Worker source-applied physical publish passed: Admission source ledger=3/2, typed PULSAR_SEND_ACK target ledger/entry=4/0, Outcome source ledger=3/3, exact payload readback
Pulsar Worker vertical smoke passed: assignment recovery ledger/entry=2/0, active apply ledger/entry=3/0, guarded SUBSCRIBE, RocksDB WriteBatch, ACK, and final checkpoint
Pulsar multi-Broker failover E2E passed: same-topic guarded Worker resumed through broker-2 after broker-1 stop, applied the source record, completed provider-driven physical Publish, ACKed the source and released its final checkpoint and owner assignment.
```

This refreshes the explicit two-Broker Worker failover contract. It does not
claim controller/coordinator failover, raw socket/network cuts, multiple
independent Broker processes beyond this cluster, Gateway ingress,
multi-shard production placement, the full chaos matrix or V1 release gates.

## 2026-08-17 Current Kafka Large-payload production-authority implementation note

The current source-bound composition locks Delay to
`f3adc8cba4c78479f2daa883f0605136dc085f50`, K1 to
`05849884ca81fad767fda058444d1e17c7f9cbf9`, K1 client artifacts to
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, and
Oxia to `37a17bef17202d5fd6e23282da5fd26d94865484`. It uses the locked MinIO
digest `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`
and the real three-Broker KRaft project
`nereus-delay-large-payload-e2e-1786898894-84130`.

The implementation path is now source-bound end to end: Gateway mTLS/RS256-
JWT admission creates the real Oxia-backed Route/Assignment/Owner authority;
the Worker consumes guarded Kafka Fetch evidence, uploads and attests the
large payload through versioned MinIO, publishes the physical Kafka
destination with typed `KAFKA_TRANSACTIONAL_RECEIPT` evidence, then appends
the source-applied Outcome and final checkpoint. The current receipt was:

```text
Kafka Worker source-applied physical publish passed: Admission source offset=4, typed KAFKA_TRANSACTIONAL_RECEIPT receipt offset=0, Outcome source offset=5, exact payload readback
Kafka + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: activationOffset=0, barrierOffset=2, prepareOffset=KafkaSourcePosition[shardId=ShardId[routeIncarnation=0a18766bd5b24b43ae29a62e8b7e8df1, partition=0], authenticatedClusterId=MkU3OEVBNTcwNTJENDM2Qk, nativeTopicUuid=81c2e553-92d4-4ba7-954a-83fb227d3cce, offset=2, leaderEpoch=null, brokerLogAppendTimeEpochMs=1786898913930], commitOffset=KafkaSourcePosition[shardId=ShardId[routeIncarnation=0a18766bd5b24b43ae29a62e8b7e8df1, partition=0], authenticatedClusterId=MkU3OEVBNTcwNTJENDM2Qk, nativeTopicUuid=81c2e553-92d4-4ba7-954a-83fb227d3cce, offset=3, leaderEpoch=null, brokerLogAppendTimeEpochMs=1786898914695], providerVersion=295e66ce-feec-467c-a7cf-6db22e473dbf, exactGatewayIdempotency=true
Kafka + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload + Kafka destination authority E2E passed
```

The contract remains bounded to one physical source partition and the local
semantic trust resolver seam; it does not promote checkpoint REAPING/GC,
external credential-provider authority, full response-loss/chaos coverage or
V1 release readiness.

## 2026-08-21 Candidate source lock versus evidence overlay

The r19/r20 receipts remain historical at Delay
cec7641b96a57d3108723c8cb27eb51594846543; r19 certifies only its declared
14-cell chaos profile and r20 correctly remains NOT_READY because the
capacity/soak/activation/operations inputs are source-locked to older Delay
code.

Complete V1 evidence must freeze the candidate implementation and normative
documents before execution. A separate, post-run documentation overlay may
record receipt results only in these six evidence-ledger files:

~~~text
docs/IMPLEMENTATION-STATUS.md
docs/Nereus Delay V1 设计.md
docs/V1-DESIGN-AUDIT.md
docs/V1-DIRECT-SDK-GATEWAY-GUARDED-TRANSPORT-DETAILED-DESIGN.md
docs/V1-OPERATIONS-RUNBOOK.md
e2e/README.md
~~~

The external evidence manifest stores both locks: candidate four-repository
HEADs and overlay Delay commit. It also stores exact document/artifact hashes
and all ten required PASS_CERTIFIED inputs. The verifier rejects any diff
outside the six ledgers or any later byte/source/status drift, so recording
evidence cannot silently broaden a bounded transport receipt into a complete
V1 claim.

The stable release command `e2e/run-v1-release-gate.sh` now uses the adjacent
full-V1 validator. It requires a separately frozen four-repository candidate
lock and ten exact-source `PASS_CERTIFIED` inputs with the common
`nereus-delay-v1-full-gate-input-v1` contract. `scope=full-v1`,
`complete_v1=true`, empty exclusions/boundaries, and complete per-gate coverage
are mandatory; the former bounded RC1 receipts cannot satisfy this gate.

## 2026-08-17 Current Pulsar Gateway large-payload multi-Broker failover implementation note

The current failover composition locks Delay to
`f3adc8cba4c78479f2daa883f0605136dc085f50`, P1 to
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, the P1 distribution to
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and the
P1 image to
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`.
The real two-Broker project is
`nereus-delay-pulsar-large-e2e-1786898952-84840`; real Oxia is at `29210`,
MinIO at `29211`, and Gateway at `29212`.

The failover contract is source-applied and authority-bound. After the first
Gateway Commit/readback, the harness stops broker-1. The old Owner is fenced,
quiesced and released; only then does the successor coordinator publish the
digest-bound Assignment and acquire the successor Owner Lease. The fresh
Worker reconnects through broker-2, validates guarded SUBSCRIBE evidence,
applies the exact large payload, publishes the physical destination with
typed `PULSAR_SEND_ACK`, appends the source Outcome and releases the final
checkpoint. The source-bound receipt was:

```text
Pulsar source reactivation successor accepted: oldGeneration=2, newGeneration=3, assignmentRevision=2, ownerEpoch=2
Pulsar Worker source-applied physical publish passed: Admission source ledger=5/0, typed PULSAR_SEND_ACK target ledger/entry=7/0, Outcome source ledger=5/1, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload multi-Broker failover E2E passed: broker-1 stopped after Gateway Commit/readback and the same source-applied physical Publish completed through broker-2
```

This proves the bounded external-stop reactivation path, not automatic
Pulsar controller/coordinator leader failover, more than one physical source
partition, Profile/Oxia external credential authority, checkpoint REAPING/GC,
the complete chaos matrix or V1 release gates.

## 2026-08-17 Current Checkpoint REAPING real-service implementation note

The current real-service composition locks Delay to
`f3adc8cba4c78479f2daa883f0605136dc085f50` and Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`. It runs
`OxiaRealCheckpointPublicationSmokeTest` and
`OxiaRealCheckpointReapingSmokeTest` against real Oxia and a locked,
version-enabled MinIO endpoint in project
`nereus-delay-oxia-minio-checkpoint-e2e-1786899309-90091`.

The reaping path is now source/authority-bound rather than a local deadline:
the real Owner Lease is abandoned and reread absent, the exact pending Intent
is CASed to `REAPING`, provider ownership/quiescence evidence is checked, and
the reaper lists and deletes only the canonical checkpoint prefix with exact
object versions before requiring an empty-prefix reread. A provider response
loss therefore retains the same REAPING identity for retry instead of
inventing a new sweep.

The source-bound receipt was:

```text
Oxia + MinIO Worker checkpoint publication and REAPING E2E passed: real Oxia Intent/Catalog/Owner authority and real MinIO immutable objects
```

The implementation remains bounded to one real Oxia authority session and one
real MinIO provider. Multi-worker disaster takeover, external credential
rotation/quiescence, full chaos, soak and V1 release gates remain separate.

## 2026-08-17 Current Oxia Profile/Route authority and session-recovery implementation note

The current source is Delay
`d521aeb41c13d396716f8ac726a63bf4f96db4db` against Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The real Oxia main smoke passed
Profile catalog Head/protection/rotation, credential trust verification,
Owner/control/recovery authority, Route/Assignment and Gateway
audit/admission. Its three explicit skips were retained as skips rather than
being treated as evidence.

The fault cut exposed a session-recovery edge: `reconnectSession()` only
revalidated the old marker. After a service restart, Oxia could still return
that marker while the underlying session was already invalid, so the next
publish hit `SessionFenceException`. Explicit recovery now clears the local
session projection, rotates the marker identity and performs a fresh
session-bound CAS even when the old marker reread succeeds. The deterministic
regression is
`explicitSessionReconnectRotatesMarkerWhenTheOldMarkerIsStillReadable`, and
the real current-source receipt is:

```text
Dockerized Oxia Route notification restart smoke passed: session rotation and notification stream recovery
```

The fix is scoped to explicit recovery; ordinary start/notification operation
still preserves session fencing. External secret-manager resolution,
source-ordered credential rotation, multi-node authority failover, provider
quiescence, chaos and V1 release gates remain separate.

## 2026-08-17 V1 release-gate implementation boundary

At Delay `9e29af8e70fa4d84725d624959f377c271d9f319`, with K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, the implementation has crossed
the real Gateway/Oxia/Broker/Worker/MinIO composition boundary. It has not
crossed the release boundary. Protocol/fresh-process/real-service/no-early
and patch-distribution gates are partial; benchmark, capacity/SLO, soak,
upgrade/downgrade and runbook gates are open. The complete crash/chaos matrix,
external credential authority/rotation, multi-node provider failover and
source-locked release artifacts must be completed before any V1-ready claim.

### 2026-08-17 Current-source Gateway/Oxia session-churn implementation note

The current-source real-service cut uses Delay
`262254fcefea86f34cc153282706cfb2b16ad222`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, project
`nereus-delay-gateway-e2e-1786900154-5135`, Oxia `26500` and Gateway `28500`.
The harness keeps the old Gateway process alive while stopping Oxia long
enough for the two-second Oxia sessions to expire, then starts Oxia and opens
three fresh session-bound record clients.

The gated real test passed with one test, zero failures and zero errors:

```text
Gateway Oxia session churn E2E passed: stale admission/idempotency sessions failed closed and new sessions reread the exact durable outcome
```

The old admission, idempotency and audit wrappers reject stale-session I/O;
the new composition rereads the exact durable record and preserves one
preparation, one physical attempt, zero live admission leases and two
digest-only audit records. The run-created image
`sha256:15ca9bafe5206cc9709255955a99a6b7761c85916163831ea248c350dea3335`
was removed after exact compose cleanup. This is bounded single-node
session-churn/recomposition evidence, not transparent reconnect, Gateway HA,
load, full crash/response-loss resolution or V1 release evidence. The locked
MinIO image was retained; no global Docker prune was used.

### 2026-08-17 Current-source Object Store credential renewal implementation note

The current-source Oxia+MinIO runner uses Delay
`e6d28a5b0fecc6c20daded998b1d324990fe95c2`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`,
project `nereus-delay-oxia-minio-checkpoint-e2e-1786900763-13173`, Oxia
`26320` and MinIO `27320`.

The real renewal test builds the adapter from the session-bound Oxia Profile
Head, immutable binding, protection and use lease, renews only inside the
configured window, verifies the extended protection horizon, and rejects
renewal when the durable Head rotates to generation two. The same run passed
the real MinIO checkpoint publication/download and REAPING tests:

```text
Oxia + MinIO Worker checkpoint publication passed: atomic Intent/Catalog=true, immutable object upload/download=true, checkpoint=00000000000000000000000000000003
Oxia + MinIO checkpoint REAPING authority passed: real Owner abandonment=true, real Intent PENDING_UPLOAD->REAPING=true, exact-version prefix sweep=2, finalEmptyPrefix=true, localProviderOwnershipClosed=true
Oxia + MinIO Object Store credential renewal E2E passed: real Profile Head/protection CAS renewed the exact lease and fenced the live adapter at secret rotation
```

This closes the bounded real Profile/protection/lease renewal and rotation
fence composition. It does not claim external secret-manager resolution,
source-ordered trust-set publication, multi-process renewal ownership or a
provider operation after renewal. The run-created Oxia image was removed;
the locked MinIO image was retained and no global Docker prune was used.

### 2026-08-17 Current-source Pulsar Worker Publish Admission response-loss implementation note

The current-source real-service cut locks Delay to
`ef8ad3fcdb0765565b93036f901a45781f163bb0`, P1 to
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, and Oxia to
`37a17bef17202d5fd6e23282da5fd26d94865484`. It uses the P1 distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3` and the
real-service project pair
`nereus-delay-pulsar-e2e-1786901196-18866` /
`nereus-delay-pulsar-oxia-e2e-1786901196-18866`.

The focused harness wraps only the real guarded Shard Log mutation append:
the broker persists the admission mutation, the client discards the first
local `PERSISTED` response, and the Worker must retain the exact identity as
`UNKNOWN` until source replay finds the durable mutation. A successful replay
opens the existing physical publish path; it cannot append a retry mutation
under a new identity. The current receipt is:

```text
Pulsar Worker Publish Admission response-loss E2E passed: the real Shard Log mutation was persisted, its append response was discarded, and exact source replay recovered the PUBLISHING admission.
```

This is the production-shaped response-loss rule for the bounded slice. The
test intentionally does not claim raw TCP packet loss, process/Broker crash,
multi-Broker failover, multi-shard placement, REAPING or release-gate
coverage. Exact project cleanup removed the temporary P1 image
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`
and Oxia image
`sha256:fb4ef2e60386870ff19076357e35d59c7006476bf63eb9c1fecc3f5b2a89f074`;
the locked MinIO base was retained and no global Docker prune was used.

### 2026-08-17 Current-source Pulsar Broker process-crash implementation note

The multi-Broker runner at Delay
`123ffe6e6f70c7779a5712012f1836f8d792b43b` adds the explicit
`NEREUS_DELAY_PULSAR_MULTI_BROKER_PROCESS_CRASH=1` cut. It uses
`docker compose kill --signal KILL pulsar-broker-1`, waits for Broker-2's
fully initialized `/admin/v2/brokers/ready` endpoint, resumes the same topic
through the two-entry Pulsar service URL, starts Broker-1 again and waits for
its readiness. The real Worker topic-create path retries only transient
`IOException` failures for a bounded 60 seconds, preserving the existing
guarded resource metadata and exact topic identity.

The current source-bound run used P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, P1 image
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
temporary Oxia image
`sha256:d4808a1f1860d744ec8d12539d1a85daf583114589b36a70c62aaffcae7819e6`,
and projects
`nereus-delay-pulsar-multi-e2e-1786902614-37701` /
`nereus-delay-pulsar-multi-oxia-e2e-1786902614-37701`. Its receipt was:

```text
Pulsar Broker process-crash failover E2E passed: broker-1 was SIGKILLed after guarded Worker preparation, the same topic resumed through broker-2 with real Oxia authority, and broker-1 rejoined afterward.
```

This is bounded process-level Broker failover evidence with one
ZooKeeper/BookKeeper pair. It does not claim raw network cuts, metadata
controller or storage failover, Gateway-plus-Broker failover, multi-shard
placement, full chaos or V1 release readiness. Exact project cleanup removed
the P1/Oxia temporary images and all named resources; the locked Oxia base
was retained and no global Docker prune was used.

### 2026-08-17 Current-source Pulsar Worker source ACK response-loss implementation note

The real-P1/real-Oxia focused cut uses Delay
`75f451758c30c6eafc50b252bffdcef22f0137b4`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1 distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`. The projects were
`nereus-delay-pulsar-e2e-1786901489-23214` /
`nereus-delay-pulsar-oxia-e2e-1786901489-23214`.

The source ACK wrapper makes the uncertainty boundary explicit: real Pulsar
accepts the source ACK, the local response is discarded, and the Worker
reuses the same Source Position on the next bounded turn. The current receipt
is:

```text
Pulsar Worker source ACK response-loss E2E passed: real ACK response loss was retried on the same source record and the bounded Worker vertical completed.
```

The slice does not claim raw socket, process/Broker crash, multi-Broker or
multi-shard fault coverage. Exact cleanup removed the temporary P1 image
`sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`
and Oxia image
`sha256:e4dd8a04d8a9018a9f2a1f21aef4a66c6fe886fe1da9d`.

### 2026-08-17 Current-source Pulsar guarded destination SEND response-loss implementation note

The P1-only focused cut uses Delay
`75f451758c30c6eafc50b252bffdcef22f0137b4`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1 distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
project `nereus-delay-pulsar-e2e-1786901571-24129`.

Real guarded SEND persisted the exact payload, the first local response was
discarded, and the typed `PULSAR_SEND_ACK` evidence resolved the same
physical publish:

```text
Pulsar destination committed response-loss E2E passed: real SEND response loss resolved through typed PULSAR_SEND_ACK evidence and exact guarded payload readback.
```

This is bounded transport evidence, not Worker/Oxia authority, raw socket,
process/Broker crash, multi-Broker, multi-shard, REAPING or V1 release
evidence. The exact temporary P1 image was removed; the locked MinIO base was
retained and no global Docker prune was used.

### 2026-08-17 Current-source Pulsar Worker JVM process-crash implementation note

The focused runner at Delay
`fdee96ca5e402bd725ff1454c1086b249e0ce8da` adds `crash-wait` and persistent
`NEREUS_DELAY_PULSAR_WORKER_ROOT` support to the real Worker smoke. It creates
the cut only after guarded source/runtime construction and before the next
source record is consumed, writes the exact JVM PID, and waits on a file gate.
The shell harness SIGKILLs that JVM, removes the gate, and runs `resume` with
the same Store root. The real current-source receipt is:

```text
Pulsar Worker process-crash recovery E2E passed: a real Worker JVM was SIGKILLed after opening the guarded source/runtime with the next record unACKed, and a fresh JVM reopened the exact local Store, reacquired the real Oxia lease, replayed and ACKed the source record, and published the final checkpoint.
```

This proves one exact local-store crash/reopen sequence with real P1 Broker
and Oxia Owner authority. It intentionally does not claim a crash during
physical destination publish, raw network cuts, Broker/controller failover,
multi-Worker placement, REAPING or full release-gate coverage. The runner
removed its temporary P1/Oxia images and state; the locked MinIO base was
retained and no global Docker prune was used.

### 2026-08-17 Current-source Pulsar Gateway + Broker process-crash large-payload implementation note

The combined source-bound runner now exercises the full bounded production-
authority path at Delay `888c0513c433234282a12eff6e401aa4a8a40116`: Gateway
mTLS/JWT prepare/upload-attest/commit through real Oxia, real two-Broker P1
source reactivation, Worker Store apply and ACK, real MinIO large-payload
readback, typed destination publish evidence, source Outcome/apply and final
checkpoint. The run locks P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`,
P1 distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, the
three client artifacts
`57de344822b16ff664a8e0d071b2392de1c82b5faabc6a93714b4eabba039a5c`,
`f832e20478b7baa808e22f577028d26f7ae2fab8ddc0870d869a06e40dbd8394` and
`94a865b5d858ea62ec980bdad70316c3cba576a7ce37009a20f4acae89f2d8e8`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, and locked MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

After Gateway Commit/readback the shell harness executes
`docker compose kill --signal KILL pulsar-broker-1`, releases the Java cut
only after Broker-2 is ready, and starts Broker-1 after the same source-
applied physical publish completes. The current receipt is:

```text
Pulsar source reactivation successor accepted: oldGeneration=2, newGeneration=3, assignmentRevision=2, ownerEpoch=2
Pulsar Worker source-applied physical publish passed: Admission source ledger=5/0, typed PULSAR_SEND_ACK target ledger/entry=3/0, Outcome source ledger=5/1, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker process-crash failover E2E passed: broker-1 was SIGKILLed after Gateway Commit/readback, the same source-applied physical Publish completed through broker-2, and broker-1 rejoined afterward
```

The source reactivation proof remains immutable: the successor retains the
same Broker resource incarnation, physical topic, cursor position, batch
shape and attestation digest while requiring a new assignment identity,
assignment epoch and distinct guarded connection generation. A source-bound
trial before `888c0513` correctly failed when the P1 Broker-local allocator
returned the same raw generation after handoff. The new
`createSuccessorSource` helper does not weaken
`PulsarSourceReactivationV1`; it closes the equal-generation candidate and
retries a fresh guarded SUBSCRIBE with a bounded budget, failing closed if no
distinct proof appears. This handles the current P1 per-Broker allocator
boundary without claiming cluster-global generation monotonicity.

The verified project was
`nereus-delay-pulsar-large-e2e-1786903675-50550` on Pulsar
`29520/29521,29522/29523`, Oxia `29530`, MinIO `29531` and Gateway `29532`.
Exact post-run checks found no project containers, networks, volumes, P1
image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`
or run-created Oxia image
`sha256:f0248322573f38df19556f4eda1146f4ffc89fe362566cf50e26f64ed22292f4`.
The locked MinIO base was retained and no global Docker prune was used. This
is bounded two-Broker/one-physical-source-partition evidence; automatic
controller/coordinator failover, raw socket/network chaos, ZooKeeper/
BookKeeper/storage failover, multi-shard production placement, full chaos and
V1 release gates remain separate obligations.

### 2026-08-17 Current-source Pulsar Gateway + Broker network-partition large-payload implementation note

The combined source-bound runner now covers the bounded network-partition
variant at Delay `f95c8a5468d6a1ee6df0bc1bd99000dc769d8797`: real Gateway
mTLS/JWT prepare/upload-attest/commit through real Oxia, real two-Broker P1
source reactivation, Worker Store apply and ACK, real MinIO large-payload
readback, typed destination publish evidence, source Outcome/apply and final
checkpoint. The run locks P1
`nereus/delay-resource-guard-v1@0a2536484cd3932801a98dc88ff112b2df88a1c7`, P1
distribution
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, P1
image `sha256:819a2a34b91d34468ac6caa048ec5cbf959fb9ecb40dbfd649a9fabf067318de`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, and locked MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The exact source-bound command was:

```bash
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_FAILOVER=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION=1 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_NETWORK_PARTITION_HANDOFF_WAIT_SECONDS=75 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_GRADLE_USER_HOME=/tmp/nereus-delay-full-check-20260817 \
PULSAR_LARGE_BROKER_1_PORT=30440 \
PULSAR_LARGE_WEB_1_PORT=30441 \
PULSAR_LARGE_BROKER_2_PORT=30442 \
PULSAR_LARGE_WEB_2_PORT=30443 \
NEREUS_DELAY_PULSAR_LARGE_OXIA_PORT=30450 \
NEREUS_DELAY_PULSAR_LARGE_MINIO_PORT=30451 \
NEREUS_DELAY_PULSAR_LARGE_GATEWAY_PORT=30452 \
PULSAR_LARGE_PAYLOAD_TOPIC=nereus-delay-pulsar-large-payload-networkcut-20260817-r14 \
NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_DESTINATION_TOPIC=nereus-delay-pulsar-large-destination-networkcut-20260817-r14 \
  bash e2e/run-pulsar-large-payload-gateway-e2e.sh
```

After Gateway Commit/readback the shell harness disconnects Broker-1 from the
exact Compose `pulsar-cluster` network, verifies that the live container is no
longer a member, waits the bounded 75-second handoff window, releases the Java
cut, and reconnects the same container after the Worker publishes through
Broker-2. The current receipt is:

```text
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906846912
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906861998
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906877097
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906892146
Pulsar Owner Lease renewed during failover cut: ownerEpoch=1, expiresAt=1786906907254
Pulsar source reactivation successor accepted: oldGeneration=2, newGeneration=3, assignmentRevision=2, ownerEpoch=2
Pulsar Worker source-applied physical publish passed: Admission source ledger=5/0, typed PULSAR_SEND_ACK target ledger/entry=7/0, Outcome source ledger=5/1, exact payload readback
Pulsar + Oxia Route/Assignment/Owner + Gateway mTLS/JWT + Worker + MinIO large-payload authority E2E passed: prepare=2/2, commit=2/3, exactGatewayIdempotency=true, sourceRecords=6
Pulsar + Oxia + Gateway mTLS/JWT + Worker + MinIO large-payload Broker network-partition failover E2E passed: broker-1 stayed alive but lost its exact Compose network endpoint after Gateway Commit/readback, the same source-applied physical Publish completed through broker-2, and broker-1 rejoined afterward
BUILD SUCCESSFUL in 2m 5s
```

The verified project was
`nereus-delay-pulsar-large-e2e-1786906721-85706` on Pulsar
`30440/30441,30442/30443`, Oxia `30450`, MinIO `30451` and Gateway `30452`.
The large-payload fixture keeps `PulsarSourceReactivationV1` strict: equal
Broker-local generation candidates are closed and retried, and a bounded
three-round quiet proof window is required after `seekAfter` before the Route
barrier is bound. The latter closes a real P1 post-seek consumer-replacement
race without weakening generation or attestation checks.

This is bounded two-Broker/one-physical-source-partition evidence, not a claim
for arbitrary network failure shapes, automatic controller/coordinator or
ZooKeeper/BookKeeper/storage failover, multi-shard production placement, the
full crash/chaos matrix or V1 release readiness. Exact post-run checks found no
project containers, networks, volumes, P1 image or run-created Oxia image; the
locked MinIO base remained and no global Docker prune was used.

### 2026-08-17 Current-source Checkpoint Intent/Catalog/REAPING real MinIO fault implementation note

Delay `c930413d146879b68b06f9f313eef3f290c63e1e` extends the real Oxia + MinIO
checkpoint runner with the same deterministic fault proxy used by the adapter
fault tests. The proxy forwards the first immutable `manifest.json` PUT to
real version-enabled MinIO, then injects `PUT_503_AFTER_COMMIT` or holds the
response for three seconds for `PUT_TIMEOUT_AFTER_COMMIT`. The real checkpoint
adapter accepts the ambiguous result only after exact immutable read-back.

The runner resets the fault between separate publication and REAPING JVMs.
Therefore each cell covers real Oxia Owner/Intent/Catalog authority, the
PUBLISHED publication path, and the `PENDING_UPLOAD -> REAPING` CAS followed by
exact-version prefix deletion and final-empty proof under the selected
provider ambiguity. Timeout runs bind
`NEREUS_DELAY_MINIO_REQUEST_TIMEOUT_MS=1000`.

The source-bound receipts were:

```text
503-after-commit: project nereus-delay-oxia-minio-checkpoint-e2e-1786926546-44708,
publication BUILD SUCCESSFUL in 1m 17s, REAPING BUILD SUCCESSFUL in 13s,
exact-version sweep=2, finalEmptyPrefix=true
timeout-after-commit: project nereus-delay-oxia-minio-checkpoint-e2e-1786926652-46178,
publication BUILD SUCCESSFUL in 1m 17s, REAPING BUILD SUCCESSFUL in 14s,
exact-version sweep=2, finalEmptyPrefix=true
```

The receipt locks Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and MinIO
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Exact postchecks found no run-created container, network, volume, listener or
Oxia image; locked Oxia/MinIO bases were retained. This is a bounded
post-commit provider-ambiguity receipt, not provider-side quiescence or
consistency certification, multi-worker takeover, target isolation, the
remaining §23.3 matrix, multi-shard production, chaos or V1 release proof.

### Current implementation binding: full large-payload provider failure before Commit

The real Kafka and P1 Pulsar large-payload Gateway bindings now carry the
MinIO fault mode into the real-client smoke. For
`PUT_503_BEFORE_COMMIT`, the proxy rejects the first payload PUT with HTTP 503;
for `PUT_TIMEOUT_BEFORE_COMMIT`, it does not forward the request and holds the
response past the adapter's `1000ms` timeout. The Worker catches only these
explicitly expected pre-commit faults, verifies the exact source and payload
authority state, drains the checkpoint and releases the Oxia Owner before
withdrawing the placement. A provider error is never promoted into Commit or
PUBLISHED.

The current source is Delay
`2a0db42290da0fa47a28356a1d4bcb6bcf2123b8`, with K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Kafka client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, P1
distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, and
locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The four source-bound receipts are:

```text
Kafka 503: project nereus-delay-large-payload-e2e-1786928021-63153,
ports 31840/31841/31842, 31850/31851/31852, proxy 31853,
BUILD SUCCESSFUL in 10s
Kafka timeout: project nereus-delay-large-payload-e2e-1786928059-63650,
ports 31860/31861/31862, 31870/31871/31872, proxy 31873,
adapter timeout 1000ms, BUILD SUCCESSFUL in 10s
Pulsar 503: project nereus-delay-pulsar-large-e2e-1786928197-65083,
broker/web 31880/31881 and 31882/31883, 31890/31891/31892, proxy 31893,
BUILD SUCCESSFUL in 40s
Pulsar timeout: project nereus-delay-pulsar-large-e2e-1786928269-65870,
broker/web 31900/31901 and 31902/31903, 31910/31911/31912, proxy 31913,
adapter timeout 1000ms, BUILD SUCCESSFUL in 40s
```

Each receipt proves `Prepare retained RESERVED`, no source Commit, absent
payload object by retryable attestation, Worker checkpoint drain and Owner
release. This is bounded two-Broker/one-source-partition evidence for the
large-payload pre-commit fault boundary; Kafka response-loss/LSO/retention,
Pulsar multi-Broker failover, multi-shard placement, crash/chaos and V1
release proof remain separate.

### Current implementation binding: checkpoint provider failure before commit

The Worker checkpoint composition now has a real-provider fail-closed test for
both an immediate 503 and a response timeout before MinIO receives the
`manifest.json` PUT. The required state transition is:

`PENDING_UPLOAD` Intent -> failed Worker attempt -> no PUBLISHED Catalog or
manifest -> exact partial-prefix sweep -> Owner Lease release.

The test also proves that the scheduler has no in-flight claim after failure.
The sweep is scoped by the authoritative object-store profile, recovery
lineage and checkpoint ID; it does not use a broad bucket delete. A timeout
is not treated as evidence that the provider committed: the proxy deliberately
does not forward the request, and the adapter's `1000ms` timeout remains an
error boundary.

The current source is Delay
`33714d9a5470edf50aed57bc8a2aefe5cfb52b5c`, with Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The 503 receipt is project
`nereus-delay-oxia-minio-checkpoint-e2e-1786927303-54007` on
`31950/31951/31952`; the timeout receipt is project
`nereus-delay-oxia-minio-checkpoint-e2e-1786927391-54902` on
`31960/31961/31962`. Both passed their source-locked Gradle invocation and
left no run-created Docker resources. This is a checkpoint authority receipt;
full Gateway/large-payload pre-commit, multi-shard, chaos and release gates
remain separate.

### 2026-08-17 Current implementation binding: Kafka source recovery and Pulsar Broker failover

The current source binds the guarded Kafka Fetch path to a real three-Broker
K1 cluster and the Pulsar Worker path to a real two-Broker P1 cluster with
real Oxia authority. The focused receipts are locked to Delay
`883352e2bdc4f376cbf892020b0e8f02e8319797`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

```text
Kafka Fetch response-loss: project nereus-delay-kafka-e2e-1786928641-71203,
ports 31940/31941/31942, BUILD SUCCESSFUL in 49s,
responseDiscardedAfterFetch=true, replayOffset=0, secondOffset=1,
fetchLso=2, committedAfterReplay=2
Kafka retention floor: project nereus-delay-kafka-e2e-1786928713-71988,
ports 31950/31951/31952, BUILD SUCCESSFUL in 30s,
oldOffset=0, retentionFloor=20, endOffset=21,
staleOffsetRejected=true, floorFetchOffset=20, fetchLso=21
Pulsar Worker process crash: project nereus-delay-pulsar-multi-e2e-1786928804-72884,
broker/web 31970/31971 and 31972/31973, Oxia 31980,
preparation BUILD SUCCESSFUL in 52s, recovery BUILD SUCCESSFUL in 37s
```

The Pulsar receipt proves that broker-1 can be killed after a guarded source
record is durable and that broker-2 can recover the Oxia session-bound
assignment/Owner, execute guarded SUBSCRIBE, apply the source mutation,
publish to the destination and ACK; broker-1 then rejoins. The Kafka receipts
prove exact Fetch replay/LSO and real retention-floor rejection/readability.
These are bounded broker-client bindings, not coordinator/controller/storage
failover, multi-shard placement, the complete chaos matrix or V1 release proof.

### 2026-08-17 Current implementation binding: bounded capacity/SLO evidence

The clean Delay source
`c12c23c248adfb9f19238c4315a58b2eb6613d22` also runs the local
`BoundedCapacitySloProbeTest` through
`e2e/run-bounded-capacity-slo-probe.sh`. The source-locked invocation passed in
`1m 15s`, verified persistent RocksDB payload readback at 256/4096/65536 bytes,
and exported/reopened 24 durable SLO Start/Final samples. The artifact remains
`PARTIAL` because the platform probe cannot certify its envelope when the JVM
does not explicitly set `MaxDirectMemorySize`.

This binding is intentionally below benchmark/capacity certification: it does
not establish Broker throughput, production multi-Worker placement, restore
throughput, fairness/SLO denominators, cgroup/procfs authority or a long-cycle
soak. The release gate must retain those boundaries.

### 2026-08-17 Source-ordered Initial Route control application

Commit `f6b7c4ee` wires the Registry kind-14
`InitialRouteControlActivatePayloadV1` into `DelayShard`. The apply path uses
the existing `CompatibleControlSnapshotV1` projection and `meta/FIXED` key 10;
it verifies the immutable snapshot digest and shard subject, then commits the
snapshot, `SystemMutationResult` and Source Position in one synchronous
WriteBatch. Exact mutation replay remains idempotent, a same-snapshot new
operation is stale without changing the snapshot, and a divergent or
tampered snapshot is rejected.

The focused apply/reopen/conflict tests and current full `check` pass. This
does not yet supply kind-1 Protocol Version activation, eligible-reader
assignment, writer-before-reader cutover, downgrade/release packaging or
external Oxia/Worker rollout authority; those remain explicit Gate 8 and V1
release boundaries. No Docker run is required for this local Store slice.

### 2026-08-17 Bounded Linux platform resource receipt

The capacity evidence producer now emits JSON-valid `AVAILABLE` platform
objects when all authority fields are present, and its reopen markers are JSON
booleans. The repeatable runner is
`e2e/run-bounded-capacity-slo-container-probe.sh`. At Delay
`84003e7aa55b7a5278cab45b606b941cdef3bcec`, it ran the bounded probe in
`eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769`
with 2 GiB cgroup memory, 2 CPUs, 65536 FDs and an executable 4 GiB temporary
filesystem. `WorkerRuntimeResourceProbe` observed the JVM, RSS, cgroup, rlimit
and filesystem authorities; the RocksDB/reopen and 24-sample durable SLO
checks passed.

This is a platform-observation receipt, not a capacity or V1 release PASS. It
does not establish the required broker throughput, size/burst/Lane/shard/
restore benchmark matrix, reserve/fairness or adapter/zombie capacity,
multi-Worker placement, or long-cycle soak. The temporary JDK image was
removed after the exact run; locked Oxia/MinIO bases remain the only related
reusable bases.

### 2026-08-17 Current-source two-shard placement receipt

The current Delay source `54541a00b65bf911febb543ac1a956b1e281c602` was rerun
through the existing Kafka and Pulsar two-shard Route Worker runners with real
K1/P1 Broker and Oxia authority. Kafka passed in 68 seconds and Pulsar in 64
seconds; both observed two guarded source barriers, two real Assignment/Owner
CAS paths, one shared Worker fleet, per-shard RocksDB apply/ACK and final
checkpoint/assignment release. The K1/P1 and Oxia source locks remain the
current cross-repository locks recorded by the validator.

This advances the bounded placement evidence only. Catalog-driven production
placement, multi-shard large-payload egress, churn/isolation, controller/
coordinator/storage failover, the complete chaos matrix, benchmark/soak and V1
release gates remain separate obligations. Exact project cleanup removed all
run-created resources and images; locked Oxia/MinIO bases remain.

### 2026-08-17 Current-source Gateway/Oxia session-expiry cell

`e2e/run-bounded-chaos-matrix.sh` now runs the existing real Gateway/Oxia
session-churn smoke as a thirteenth focused cell. At Delay `f6acacdca87b6e91a953030f5a523e39df5ed314`, with
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484`, the old Gateway process
survived a two-second real Oxia stop; stale durable sessions failed closed and
fresh sessions reread one exact durable outcome. The source-locked smoke passed
in 21 seconds.

The cell is bounded single-node session-expiry evidence. It does not establish
transparent reconnect, Gateway HA, controller/coordinator/provider failover,
target isolation, full chaos or V1 release readiness. The Gateway runner now
removes its exact generated Oxia image after Compose teardown; locked Oxia and
MinIO bases are retained.

### 2026-08-17 Current-source 13-cell bounded chaos receipt

The current source locks Delay `80fdb63d3512be8fcb3af51c7f9e0aa5bba9382f`, K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, P1 `0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` completed the 13-cell bounded matrix with `matrix_status=0`. The receipt directory is `/tmp/nereus-delay-chaos-current-20260817-r2`.

This source-locked PASS advances the crash/response-loss/recovery evidence boundary across Kafka, Pulsar, Oxia/MinIO Checkpoint REAPING and Gateway session expiry. It does not certify catalog-driven placement, target isolation, controller/coordinator/storage/provider failover, all large-payload fault cuts, benchmark/soak, activation/cutover or V1 release readiness. Matrix cleanup removed its exact project containers, networks, volumes, listeners and generated images; locked Oxia/MinIO images remain.

### 2026-08-17 Current-source Linux bounded capacity/benchmark receipt

The current Delay source `4713a54c983a025bbd1bda64dd25831416642fe1` completed the reproducible Linux matrix runner with three payload-record/SLO configurations and a valid JSON index at `/tmp/nereus-delay-capacity-matrix-current-20260817-r4/capacity-benchmark-matrix.json`. The pinned JDK image was `eclipse-temurin@sha256:57865c22b954cf920cb05a610af81d577e89783282514ba071e99c7357f6c769`; its exact runtime image ID was removed after the run.

This receipt proves bounded local Store writes, payload readback, durable SLO merge, platform resource observation and persistent reopen across smoke/burst/sustained cases. It does not establish the required Broker/Lane/shard/compaction/restore/inline-object benchmark campaign, multi-Worker fairness, long-cycle soak or V1 release readiness; those gates remain separate.

### 2026-08-17 Current-source Kafka two-shard Large Payload Object Store binding

Delay `048b4d8f220557d510ced088999f94077bc253d4` binds the opt-in
`NEREUS_DELAY_KAFKA_LARGE_PAYLOAD_MULTI_SHARD=1` receipt. The implementation
keeps one Gateway coordinator and uses a route-bound
`KafkaManagedSubmissionOutcomeProjector` that validates the exact request
resource for each partition, rather than weakening projection to a topic-only
identity.

The source-locked run used K1 `05849884ca81fad767fda058444d1e17c7f9`, Kafka
client source `1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` and MinIO
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The real Compose project was `nereus-delay-large-payload-e2e-1786934597-44345`;
Kafka used `32545/32546/32547`, Oxia `32645`, MinIO `32745` and Gateway
`32845`.

The receipt proves source-ordered activation/pre-route barriers on both Kafka
partitions, one signed Route revision, two Oxia Assignment/Owner leases, one
Worker fleet, Gateway mTLS/JWT Prepare/Commit, real MinIO upload/attest/readback,
exact Prepare idempotency and final checkpoint/Owner release. Both partitions
reported Prepare offset `2`, Commit offset `3`, `sourceBarriers=[2,2]` and
`exactGatewayIdempotency=true`.

This binding is deliberately limited to Kafka two-shard Object Store authority:
the runner rejects MinIO fault modes, Broker failover modes and destination
egress in this mode. Pulsar multi-shard Large Payload, multi-shard destination
egress, placement churn/failover, benchmark/soak and V1 release certification
remain separate gates. Run-created Kafka resources and the generated image were
removed exactly; locked Oxia/MinIO bases were retained and no global Docker
prune was used.

### 2026-08-17 Source-ordered Protocol Version activation binding

Delay `1c924f479c284161771c24b013622f645c4fab06` adds the first runtime
binding for Registry kind-1 Protocol Version activation. The shard persists a
canonical `ProtocolActivationStateV1` at `meta/FIXED` key 14 / ValueEnvelope
type 11. Its marker entries retain the exact tuple, canonical schema hash,
compatible-reader-set evidence hash, source position and System Mutation ID;
the state digest and shard subject are checked on reopen.

Kind-14 Initial Route control writes an empty activation state beside the
compatible control snapshot, result and source cursor in one synchronous
WriteBatch. Kind-1 requires the tuple to be listed in the current compatible
reader snapshot and atomically records its source-ordered marker evidence.
Managed Command V1 remains the compatibility baseline; a different tuple is
position-level `UNACTIVATED_PROTOCOL_VERSION` until its marker is applied.
The command gate also retains the explicit
`UNSUPPORTED_ACTIVATED_PROTOCOL` mapping for an activated tuple no longer
present in the reader snapshot.

The focused state/codec, Initial Route regression, marker gate and restart
tests passed, along with full Gradle `check` and the cross-repository contract
validator. This is still a local source-ordered projection: authenticated
multi-Worker eligible-reader rollout, writer-before-reader orchestration,
downgrade/release packaging and a certified activation artifact remain open.

### 2026-08-17 Current-source release-gate rerun after activation binding

The current fail-closed gate receipt is
`/tmp/nereus-delay-v1-release-gate-20260817-r3/v1-release-candidate-gate.json`
at Delay `7835a4c4bb5ac8e083c73885047c4165918cbdab`, with K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source checks, cross-repository
validation and full Gradle `check` passed. The release status correctly stays
`NOT_READY` because bounded/partial evidence is not `PASS_CERTIFIED` and the
soak, activation/cutover and operations receipts are missing. A fresh-cache
attempt hit an external Maven Central TLS handshake failure during Checkstyle
resolution; rerunning with the known-good cache passed without a source or
gate bypass.

### 2026-08-17 Current-source canonical chaos and release-gate binding

Delay `fe62065750f86b607d4c395afd52197e3cb31008`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` are the source locks for the
canonical 13-cell bounded chaos rerun. Its JSON artifact is
`/tmp/nereus-delay-chaos-release-20260817-r1/bounded-chaos-matrix.json` with
`matrix_status=PASS_BOUNDED`; all cells completed with status `0`.

The new fail-closed release runner produced
`/tmp/nereus-delay-v1-release-gate-20260817-r1/v1-release-candidate-gate.json`.
Source checks, cross-repository contract validation and full Gradle `check`
passed, while the final release status is `NOT_READY` because only bounded or
partial capacity/chaos evidence exists and certified soak, activation/cutover
and operations-drill receipts are absent. Only `PASS_CERTIFIED` satisfies the
release gate; allowing the script to emit `NOT_READY` does not promote V1.

This binds the current evidence boundary: the bounded failure matrix and gate
machinery are reproducible, but certified production readiness remains open.
The exact rerun left no project resources or generated Kafka/Pulsar/Oxia/
Gateway images. Locked Oxia/MinIO bases remain available, and no global Docker
prune was used.

### 2026-08-17 Source-locked activation/cutover smoke receipt

Delay `b0d2f757716d24cbf148a6990daeaf555cfa1369` adds
`e2e/run-protocol-activation-cutover-smoke.sh`, which runs the canonical
`ProtocolActivationStateV1Test`, `InitialRouteControlApplyTest` and
`ProtocolVersionActivationApplyTest` from a clean source lock. The current
artifact is
`/tmp/nereus-delay-protocol-activation-cutover-20260817-r1/protocol-activation-cutover.json`
with `status=PASS_BOUNDED`; Gradle reported `BUILD SUCCESSFUL in 21s`.

The receipt establishes local source-ordered activation projection and
restart/cutover behavior. It deliberately does not establish authenticated
external Oxia Worker eligibility, writer-before-reader rollout, downgrade or
release packaging, real Kafka/Pulsar cutover, or disaster continuity. Since the
runner does not start Docker or external services, no related image cleanup was
required; `PASS_BOUNDED` cannot satisfy the V1 `PASS_CERTIFIED` gate.

### 2026-08-17 Current-source gate rerun with activation receipt

The clean-source release artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r4/v1-release-candidate-gate.json`
at Delay `3e21eb072f41014ed893ef5799817f2f8cb305cb`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source, cross-repository and full
Gradle checks pass; the fail-closed result remains `NOT_READY` because the
activation artifact is only `PASS_BOUNDED`, capacity is `PARTIAL`, chaos is
bounded and certified soak/operations artifacts are absent.

### 2026-08-17 Current-source Pulsar Large Payload authority rerun

The clean Delay source `7ca4cd89d6f2f7fc5a4309dc3a383e5f34f736a6` reran the
real P1/Pulsar/Oxia/Gateway/Worker/MinIO single-shard path. Locks were P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, distribution SHA-256
`373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
The isolated project `nereus-delay-pulsar-large-e2e-1786937594-84236` passed
in `1m 15s`.

The receipt proves guarded recovery, Gateway mTLS/JWT Prepare, real MinIO
upload/attest/Commit/readback, Worker apply/ACK, typed `PULSAR_SEND_ACK`,
source-applied physical Publish, exact destination payload readback, final
checkpoint and Owner release. Prepare/Commit positions were `3/2` and `3/3`,
the typed target was `4/0`, and exact Prepare replay left six source records.
This remains single-shard evidence: Pulsar multi-shard Large Payload,
controller/storage/provider failover, benchmark/soak and V1 certification are
not closed. Exact Compose resources and generated P1/Oxia images were removed;
the locked MinIO base remains and no global prune was used.

### 2026-08-17 Release artifact source-lock enforcement

Commit `41b66de37980ecca624c0f2d69cbd52307d8d452` makes the release gate
source-qualified: every `PASS_CERTIFIED` input must expose exact current
`source_locks` for Delay, Kafka, Pulsar and Oxia. Missing/stale locks are
fail-closed `BLOCKED`; bounded/partial evidence never promotes. The current
rerun is `/tmp/nereus-delay-v1-release-gate-20260817-r5/v1-release-candidate-gate.json`
at Delay `41b66de37980ecca624c0f2d69cbd52307d8d452`, with source,
cross-repository and full-check PASS but overall `NOT_READY`. This slice uses
no Docker resources.

### 2026-08-17 Source-locked bounded operations drills

The clean current-source run of `e2e/run-bounded-operations-drills.sh` produced
`/tmp/nereus-delay-operations-20260817-r2/operations-drills.json` with
`status=PASS_BOUNDED` at Delay `441a148ba4570ba0af3b6c2cfb7af3d324690954`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The local operation cut covers checkpoint restore/catalog validation, Owner
recovery and drain fences, Dead Letter replay and source-ordered resolution of
published, not-published, retry-with-possible-duplicate and possible-delivery
terminalization. A real Oxia + MinIO run then passed immutable checkpoint
publication and exact REAPING. The receipt remains bounded: it does not claim
external operator authorization, fresh-process disaster continuity, a
cross-record Oxia transaction or a certified multi-Worker soak.

The run used checkpoint ports `31510/31511` and project
`nereus-delay-oxia-minio-checkpoint-e2e-1786938487-94600`. Exact postchecks
found no related containers, networks, volumes, listeners or generated Oxia
image. The locked MinIO digest was retained, as was the existing locked Oxia
base; no global Docker prune was used.

### 2026-08-17 Current-source gate rerun with bounded operations

The source-locked release artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r6/v1-release-candidate-gate.json`
at Delay `d405d2fa00bcaf99a0d34c892291ea0a425d4c47`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source/contract/full-check all
pass. The fail-closed result remains `NOT_READY`; capacity is `PARTIAL`,
activation/operations/chaos are bounded and certified soak is missing. The
operations artifact is intentionally non-promoting because its status is
`PASS_BOUNDED`, not `PASS_CERTIFIED`.

### 2026-08-17 Pulsar Large Payload Broker failover receipt

At clean Delay `11728ea29b6b27d8a314b0afc1c7805cd0af4e1f`, the current P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` Large Payload Gateway harness ran
against two real Brokers, Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` and
the locked MinIO digest. After Gateway Commit/readback, broker-1 was stopped;
the same source-applied physical Publish completed through broker-2 and the
exact 1 MiB payload was read back. The receipt reported Admission `2/4`, typed
`PULSAR_SEND_ACK` `7/0`, Outcome `2/5`, `prepare=2/2`, `commit=2/3` and
`sourceRecords=6`; Gradle completed in 56 seconds.

This closes the named single-shard Large Payload Broker-failover cut only. It
does not prove multi-shard Large Payload, controller/storage/provider failover,
long-cycle soak or V1 release certification. The runner removed its exact
Compose resources and generated P1/Oxia images, retained locked bases and used
no global Docker prune.

### 2026-08-17 Current-source gate rerun after Pulsar failover

The clean-source release artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r7/v1-release-candidate-gate.json`
at Delay `9ec909d95b890dd227b572396091e500a9c72299`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source, cross-repository and full
Gradle checks pass, while bounded/partial/missing certification inputs keep the
fail-closed result at `NOT_READY`.

### 2026-08-17 Pulsar Large Payload Broker network-partition receipt

At Delay `fc004146b807087fcd72ee7188419eaa8f6eac06`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` and the locked MinIO digest, the
real Gateway mTLS/JWT -> Oxia Route/Assignment/Owner -> Worker -> Pulsar ->
MinIO path passed a two-Broker network-partition cut. Project
`nereus-delay-pulsar-large-e2e-1786939347-6325` used Pulsar ports
`33100/33101/33102/33103`, Oxia `33110`, MinIO `33111`, Gateway `33112`, and a
75-second ownership handoff wait.

Broker-1 stayed alive but was removed from the exact Compose network after
Gateway Commit/readback. Broker-2 then completed the same source-applied
physical Publish with exact 1 MiB readback, and broker-1 rejoined. The receipt
was Admission `5/0`, typed `PULSAR_SEND_ACK` target `3/0`, Outcome `5/1`,
`prepare=2/2`, `commit=2/3`, `sourceRecords=6` and
`exactGatewayIdempotency=true`; Gradle passed in 2m 7s. This is bounded
single-shard Broker failover evidence, not multi-shard production, provider or
controller failover, soak or release certification. Run-scoped Compose
resources and generated P1/Oxia images were removed; locked bases were
retained and no global Docker prune was used.

### 2026-08-17 Current-source gate rerun after network-partition receipt

The current source-locked artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r8/v1-release-candidate-gate.json`
at Delay `54759958b0c7af41ffa2374d835831ec7df72d13`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Source, cross-repository and full
Gradle checks pass, but capacity remains `PARTIAL`, soak is missing and the
bounded activation/operations/chaos receipts are blocked by the
`PASS_CERTIFIED` release rule; overall status is `NOT_READY`.

## Current-source Pulsar multi-shard Large Payload transport receipt

The opt-in runner flag
`NEREUS_DELAY_PULSAR_LARGE_PAYLOAD_MULTI_SHARD=1` exercises the following
transport composition over two real Pulsar physical partitions:

```text
signed Route snapshot
  -> guarded SUBSCRIBE barrier[0] + barrier[1]
  -> Oxia Assignment CAS[0] + CAS[1]
  -> Owner Lease[0] + Lease[1]
  -> one fair WorkerShardFleetRuntime
  -> one Gateway mTLS/JWT coordinator
  -> request-exact Pulsar managed receipt projection
  -> MinIO reservation / upload / attestation / Commit / readback per shard
  -> checkpoint publication and Owner release per shard
```

The keyed `PulsarManagedSubmissionOutcomeProjector(PulsarCommandTransportKey)`
contract remains unchanged for a single physical source. The multi-partition
coordinator uses its no-argument exact-resource mode; `PulsarSendRequest` is
the authority for cluster, resource incarnation, physical topic, creation
timestamp and partition fencing, and a persisted result still requires
response evidence. A result from a different request resource is uncertain,
not accepted.

This receipt proves Object Store authority only. The opt-in mode intentionally
does not create a destination topic or run destination egress, and it is
mutually exclusive with Broker failover/network-partition and MinIO fault
modes. The normal single-shard path and the separate failover receipts retain
those boundaries; no multi-shard destination or release claim should be
inferred from this result.

## Current-source 13-cell bounded fault matrix and release boundary

The current-source matrix artifact is
`/tmp/nereus-delay-chaos-release-20260817-r2/bounded-chaos-matrix.json`. All 13
Kafka, Pulsar, checkpoint and Gateway process/network/response-loss cells
passed with exit code zero under Delay
`3370bfbeb03a26186156528507e379dcb1dd3021`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`; its status is intentionally
`PASS_BOUNDED`.

The matching release gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r11/v1-release-candidate-gate.json`.
Source, cross-repository and full Gradle checks pass, while capacity remains
`PARTIAL`, certified soak is missing and activation/operations/chaos remain
bounded; therefore the release decision is `NOT_READY`. The matrix runners
removed their exact Compose resources and generated images, retaining only the
locked Oxia/MinIO bases.

### 2026-08-17 Current Pulsar multi-shard Large Payload destination egress implementation note

The current implementation at Delay
`ee292f4090e23a3f26f949aa54ac075b8ed94a78` composes the production-authority
path as:

```text
two guarded SUBSCRIBE source partitions
  -> signed Route / Oxia Assignment CAS / Owner Lease per shard
  -> one WorkerShardFleetRuntime
  -> Gateway mTLS/JWT Prepare / real MinIO upload-attest-Commit-readback
  -> source-ordered PUBLISH_OUTCOME per shard
  -> guarded destination partition 0 or 1
  -> checkpoint and Owner release per shard
```

The destination physical partition is explicit at every proof boundary. The
Worker creates distinct canonical Lane tuples for
`...-destination-partition-0` and `...-destination-partition-1`, registers
both lanes in one fleet-scoped `ClaimExecutionAdmission` and
`DestinationPhysicalAdmission`, and passes the same partition into the
ChannelResourceIdentity, ReadyCertificate, ActivationBarrier, EvidenceCursor,
`PulsarTargetResource` and guarded destination transport. The destination
consumer verifies exact bytes on the matching physical topic. This preserves
resource and partition fencing; it is not a relaxation of the existing
single-physical-topic projector.

The source-locked real receipt was r12 project
`nereus-delay-pulsar-large-e2e-1786945120-74832`, with P1 distribution
SHA-256 `373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`,
P1 image `sha256:a2c76925f2504337a55c1b88d0a83cc80147d563189041514b63bc1e347cf9d3`,
Oxia `37a17bef17202d5fd6e23282da5fd26d94865484` and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Both partitions produced exact destination payload readback, six source
records and `exactGatewayIdempotency=true`; Gradle reported
`BUILD SUCCESSFUL in 1m 34s`.

This note records a bounded current-source transport PASS. It does not claim
Kafka multi-shard egress, placement churn, provider/controller/storage
failover, certified soak or V1 release readiness.

### 2026-08-17 Current Kafka multi-shard Large Payload destination egress implementation note

The current implementation at Delay
`b641fc714db779787054811f7229709b1a3fa0ba` composes the Kafka path as:

```text
two guarded Fetch source partitions
  -> signed Route / Oxia Assignment CAS / Owner Lease per shard
  -> one WorkerShardFleetRuntime
  -> Gateway mTLS/JWT Prepare / real MinIO upload-attest-Commit-readback
  -> source-ordered PUBLISH_OUTCOME per shard
  -> one guarded Kafka target transaction per shard
       [business target partition n + keyed receipt partition n]
  -> read-committed typed receipt evidence
  -> checkpoint and Owner release per shard
```

The target partition is explicit in the Kafka target resource, Channel and
Lane tuple; the receipt partition is explicit in the receipt resource, receipt
transaction record and EvidenceCursor. The Worker registers both physical
lanes in one fleet-scoped `ClaimExecutionAdmission` and
`DestinationPhysicalAdmission`. The readback consumer uses the same guarded
partition and verifies exact payload bytes, so partition 1 cannot be silently
projected as partition 0.

The source-locked r3 receipt used project
`nereus-delay-large-payload-e2e-1786946121-90342`, Kafka ports
`34700/34701/34702`, Oxia `34710`, MinIO `34711`, Gateway `34712`, K1 client
SHA-256 `1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`,
K1 image `sha256:eb968fa8ea2fcc6c89dca3a9fbfcb4945af3909b574c3896947ffec85a2862e`
and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
Both partitions passed exact target/receipt transactions, typed evidence,
payload readback and sourceRecords `6`; Gradle reported
`BUILD SUCCESSFUL in 44s`.

This closes the bounded Kafka multi-shard destination egress path. It does not
claim placement churn, provider/controller/storage failover, certified soak or
V1 release readiness.

### 2026-08-17 Gateway/Oxia session-churn fail-closed implementation note

Delay `56f39ff80ee32ff46ce7086895a3b875d7284134` makes the current-source
Gateway session-churn receipt authoritative for the bounded fault cut. The
harness has two explicit barriers: Oxia is stopped before stale-handle
operations, and Oxia is started only after those operations have failed closed.
Fresh session-bound clients then reread the exact durable idempotency outcome;
the core prepare and coordinator submit counters remain one.

The source-locked matrix
`/tmp/nereus-delay-chaos-release-20260817-r4/bounded-chaos-matrix.json` reports
`PASS_BOUNDED` for all 13 cells. `SessionBoundOxiaGatewayRecordClient` wraps
both runtime and checked Oxia request failures as
`OxiaGatewaySessionUnavailableException`; `OxiaSyncOwnerLeaseBackend.connect`
also exposes an explicit request-timeout bound for outage-sensitive clients.
This is bounded current-source evidence, not a `PASS_CERTIFIED` chaos or V1
release result. The matching r17 gate is
`/tmp/nereus-delay-v1-release-gate-20260817-r17/v1-release-candidate-gate.json`
and remains `NOT_READY`.

### 2026-08-17 Source-locked real Oxia multi-node Gateway failover receipt

`e2e/run-oxia-multi-node-gateway-e2e.sh` now persists the complete real-service
receipt at
`/tmp/nereus-delay-oxia-multi-node-gateway-current-20260817-r2/oxia-multi-node-gateway-e2e.json`.
The receipt is `status=PASS` and locks Delay
`53c9fc0c7b1609ba37109536326dad330d994ebb`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The Compose cluster contains three coordinators and three DataServers. With
one namespace shard, the run observed `ds-2` as the initial leader, stopped
`data-server-2` after the Gateway client reached its cut gate, observed `ds-1`
as successor and allowed the Gateway test to reread the exact durable outcome.
The test exited 0 with `BUILD SUCCESSFUL in 16s`. The artifact records six
generated image IDs before cleanup and an empty exact project
container/network/volume/image postcheck; locked reusable bases remain.

This is bounded real Oxia leader-stop and session-bound Gateway recovery
evidence. It does not promote Gateway HA, coordinator/storage failover,
placement churn, disaster continuity, certified soak or the V1 release gate.

### 2026-08-17 Current-source r25 gate refresh

The current-source gate artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r25/v1-release-candidate-gate.json`.
It passes clean four-repository source checks, the cross-repository contract
validator and full Delay `check` at Delay
`6a5cd494d7122a01d666cd681a3dac7fe6e11769`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

It remains `release_status=NOT_READY`: capacity is `PARTIAL`, certified soak
is absent, and activation/operations/chaos are bounded rather than
`PASS_CERTIFIED`. The new Oxia/Gateway receipt strengthens the real-service
boundary but does not alter the release gate. This documentation append does
not refresh the artifact's source lock.

### 2026-08-17 Bounded production-authority soak implementation receipt

`e2e/run-bounded-production-chain-soak.sh` now provides the bounded
current-source production-chain check for the accepted Large Payload design.
The canonical artifact is
`/tmp/nereus-delay-production-chain-soak-current-20260817-r2/production-chain-soak.json`;
its status is `PASS_BOUNDED` with one strictly sequential cycle and four
zero-exit cases. Runtime locks are Delay
`57a02095e51bf6c143aef57c330b415f95b61e96`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The first two cases exercise multi-shard destination authority: signed Route,
per-shard Oxia Assignment/Owner, guarded source ingress, one Worker fleet,
Gateway mTLS/JWT, real MinIO upload/attest/Commit/readback, typed broker
evidence and source-ordered `PUBLISH_OUTCOME -> PUBLISHED` application. The
last two cases exercise the immutable-object uncertainty rule with real MinIO
post-Commit timeout/503 responses; exact readback resolves the committed
object before source application. Every case reports exact Compose cleanup
PASS, with generated provider images removed and the locked MinIO base
retained.

This receipt validates the named implementation path under a bounded run. It
does not alter the normative release requirements for fresh-process failure
cuts, capacity/placement, certified long-cycle soak, rollout compatibility,
operator authorization or disaster continuity, and it is not
`PASS_CERTIFIED`. The append is documentation only and does not change the
receipt's source lock.

### 2026-08-17 Current-source r28 release-gate result

`/tmp/nereus-delay-v1-release-gate-20260817-r28/v1-release-candidate-gate.json`
records clean source locks, a passing cross-repository validator and a passing
full Gradle check for Delay `cd79d92056c31f2e66ef8936d94359cddc141883`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The release decision is still `NOT_READY`. The bounded soak is not a
`PASS_CERTIFIED` capacity/soak/activation/operations/chaos substitute, so the
accepted design's non-promotion boundary remains unchanged. This append is
documentation only and does not change the r28 receipt source lock.

### 2026-08-17 Current-source r29 release-gate result

The final gate artifact is
`/tmp/nereus-delay-v1-release-gate-20260817-r29/v1-release-candidate-gate.json`.
It records clean Delay `830fce40c77c52a3a8b25d657355db9abee851c4`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, with passing source,
cross-repository and Gradle checks. The design remains non-promoted at
`release_status=NOT_READY`; bounded runtime evidence is not
`PASS_CERTIFIED`. This append does not refresh the r29 source lock.

### 2026-08-17 Certified production-chain authority harness

The certified wrapper
`e2e/run-certified-production-chain-soak.sh` was exercised against the
current four-repository candidate. Receipt:
`/tmp/nereus-delay-certified-soak-harness-20260817-r5/certified-production-chain-soak.json`;
profile: `harness-integration-production-chain-r1`; status:
`PASS_CERTIFIED` for that explicit harness profile. The source locks are Delay
`8f6fddd4c3e626a90bbe73be1360398c78114065`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The wrapper composes the four real authority cases in strict sequence and
records source locks, child status, mode/count invariants, process-tree
RSS/FD samples, measured coverage, duration and exact run-scoped Docker
cleanup. The successful receipt has 4/4 cases, 269 seconds child runtime,
36 samples, maximum sample gap 8 seconds, peak RSS `1003392 KiB`, peak FDs
`1151`, and empty matching container/network/volume/generated-image arrays.

This contract deliberately separates “certified under a named harness
policy” from V1 release promotion. The release gate requires this schema,
the measured coverage fields and a separately supplied approved profile id;
the integration profile above is not approved release evidence. It therefore
does not close the §23.5 longest-cycle soak or capacity, complete chaos,
activation/cutover, operations, upgrade/downgrade or disaster boundaries.
Generated run images were removed exactly; the locked MinIO and canonical
Oxia bases were retained, with no global Docker prune.

### 2026-08-17 Current-source r30 release-gate result

`/tmp/nereus-delay-v1-release-gate-20260817-r30/v1-release-candidate-gate.json`
records clean Delay `b9a7fa9994542b9bc9630d7b12c63ade2fc1c57b`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, with passing source,
cross-repository and full Gradle checks. The release decision remains
`release_status=NOT_READY`; no approved capacity, soak, activation,
operations or chaos certification was supplied. This append is documentation
only and does not change the r30 receipt source lock.

### 2026-08-17 Current-source r32 release-gate result

`/tmp/nereus-delay-v1-release-gate-20260817-r32/v1-release-candidate-gate.json`
records clean Delay `5d282244524de0d002cc7122ebf389150a4fd9f2`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, with passing source,
cross-repository and full Gradle checks. The release decision remains
`release_status=NOT_READY`; bounded chaos is not `PASS_CERTIFIED` release
evidence.

### 2026-08-17 Current-source bounded chaos receipt

`/tmp/nereus-delay-chaos-current-20260817-r3/bounded-chaos-matrix.json`
records a current-source `PASS_BOUNDED` matrix with all 13 focused cells
passing and locks Delay `8cfa6acc97a7a966e76b0ce086572c53cd731f7d`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Exact run resources and
generated images were removed; locked Oxia and MinIO bases were retained.

The result is bounded fault evidence only. It does not close the §23.3
long-GC, half-open, ENOSPC, fsync/SST, target-isolation,
controller/coordinator/storage/provider or durable state-dump/invariant
requirements, and it is not `PASS_CERTIFIED` release evidence.

### 2026-08-17 Current-source bounded chaos audit r5

The audited bounded matrix is
`/tmp/nereus-delay-chaos-current-20260817-r5/bounded-chaos-matrix.json`:
`PASS_BOUNDED`, 13/13 cells at status `0`, with Delay
`75b347da58a4086d19df912ca82f974401432f44`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

This strengthens the evidence contract around the real Kafka/Pulsar/Oxia/
MinIO cuts: each required source, target and authority marker is checked, and
the injection point plus duplicate boundary is recorded per cell. It remains a
bounded marker audit, not a canonical durable-state or independent invariant
audit. The six crash/network cells have fresh-process recovery evidence; the
seven response-loss/checkpoint/session cells intentionally remain
`NOT_COVERED`. Full §23.3 and V1 `PASS_CERTIFIED` release evidence remain open.

## 2026-08-17 Certified bounded-capacity evidence boundary

The capacity evidence path now has an explicit wrapper,
`e2e/run-certified-capacity-benchmark.sh`, rather than treating the local
probe's `PARTIAL` artifact as a generic release certificate. The integration
receipt is
`/tmp/nereus-delay-certified-capacity-harness-20260817-r3/certified-capacity-benchmark.json`
for profile `harness-integration-bounded-capacity-r1`.

Its contract is strict and source-bound: three serial cases with fixed
payload/SLO counts; real WorkerRuntimeResourceProbe cgroup, direct-memory,
RSS, FD and filesystem observations; RocksDB local/WAL/SST limits; durable SLO
outbox/collector limits; payload readback; Store and collector reopen; and an
empty exact Docker postcheck. The wrapper emits `PASS_CERTIFIED` only after
those checks and records the four-repository source locks in the receipt.

The semantic boundary remains explicit. This is not Broker/Lane/placement
capacity, Control Reserve or Adapter/zombie capacity, nor checkpoint restore,
inline/object, upgrade/downgrade or longest-cycle soak evidence. The V1 gate
therefore requires the strict schema plus an explicitly supplied approved
capacity profile id and continues to keep all other release gates independent.

## 2026-08-17 Fresh-process Publish Admission recovery contract

The current-source bounded-chaos r6 run adds one concrete recovery proof for
the Pulsar Worker Admission response-loss boundary. The external response is
discarded after the Shard Log durably records the Publish Admission, then the
Worker process is terminated. The pre-crash dump is authoritative for the
durable attempt: it contains one `PUBLISHING` attempt and
`outcome_applied=false`.

On restart, source replay may contain no new record because the same Store has
already applied the Admission. Recovery therefore seeks from the persisted
ShardStore applied Pulsar position, reopens the durable `PUBLISHING` message
and resumes its physical publish. The fresh process must reuse the exact
persisted READY Lane proof (certificate, channel and evidence cursors) for the
current durable state; it must not create a second READY certificate for the
same lane. The physical result is then applied as one `PUBLISH_OUTCOME`, and
the source position advances to `PUBLISHED`.

The recovery authority is the durable Store identity, shard, Store
incarnation, DB identity, message identity and Publish Attempt identity. The
r6 receipt independently compares those fields across the forced pre-crash
and post-recovery dumps and requires exactly one durable attempt/message. This
establishes the no-second-Admission/no-second-physical-publish boundary for
this cell. It remains bounded chaos evidence, not a claim that the complete
§23.3 matrix or V1 release gate is closed.

## 2026-08-17 Gateway-to-real-Broker large-payload authority proof

The current multi-shard Kafka and Pulsar runs exercise the intended authority
sequence end to end. A signed Route is accepted before the two guarded source
barriers; Oxia publishes and fences the two shard assignments and owners; the
Worker fleet consumes the source records; Gateway mTLS/JWT admits the exact
prepared large-payload request; MinIO stores and attests the immutable object;
the Worker applies the physical destination result; and the source advances to
the final `PUBLISHED` state before checkpoint publication.

The Kafka log proves two guarded Fetch partitions and the Pulsar log proves two
guarded SUBSCRIBE partitions. Both logs independently show two destination
`PUBLISHED` outcomes, per-partition source positions, object versions, exact
Gateway idempotency and the final checkpoint. The logs are retained under
`/tmp/nereus-delay-large-payload-gateway-current-20260817-r1/` and are bound
to the current four-repository locks recorded in the implementation status.

This is the functional production-authority proof the abstraction design
requires. It does not weaken the release boundary: the run is not an approved
capacity or soak profile, and it does not cover the remaining full chaos,
activation, operations, upgrade/downgrade or disaster-continuity gates.

### 2026-08-21 Current-source 14-cell bounded chaos r15

The current-source bounded wrapper was regenerated after Delay commit
`d14d9a6a7e55d77bd1a3a42ea3f2e30291896b61`, which fixes the Kafka
process-crash durable-dump marker expected by the wrapper. The canonical
receipt is:

```text
/private/tmp/nereus-delay-chaos-current-20260821-r15/bounded-chaos-matrix.json
```

It reports `PASS_BOUNDED` with all fourteen child statuses at `0`. Every
cell independently passes the marker, durable before/after,
fresh-process-recovery and independent-field invariant checks:
`audit_status=PASS`, `CAPTURED_AND_VERIFIED`, `PASS` and
`INDEPENDENT_FIELDS_PASS`. The source locks are Delay
`d14d9a6a7e55d77bd1a3a42ea3f2e30291896b61`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

This is the complete bounded fault-coverage receipt for the focused V1
production authority paths. It does not promote the certified chaos wrapper,
capacity/soak, activation/cutover, operations or V1 release gate; those
remain separate source-locked and fail-closed inputs.

### 2026-08-21 Pulsar failover and admission response-loss follow-up

Delay commits `16c3792e`, `2f57b5f8` and `b7b156e6` close the focused recovery
boundaries found after r15. The Broker state reader retries while a survivor
reconstructs its Admin endpoint; the multi-Broker runner requires three
consecutive readiness probes; and the durable managed-ledger invariant treats
the post-failover ledger set as a retained extension of the pre-failure set.

The real multi-Broker receipt is:

```text
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r7-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-multi-broker-process-crash-20260821-r7-state/after-fresh-process.json
```

It preserves the same topic, cluster and confirmed `3:0` position while the
ledger IDs extend from `[-1,3]` to `[-1,3,4]`. The fresh Worker completed the
source-applied physical publish and real Oxia authority path, and Broker-1
rejoined.

The real Worker admission response-loss receipt is:

```text
/private/tmp/nereus-delay-pulsar-worker-admission-response-loss-20260821-r1-state/before-process-crash.json
/private/tmp/nereus-delay-pulsar-worker-admission-response-loss-20260821-r1-state/after-fresh-process.json
```

The durable state converges from `PUBLISHING` to `PUBLISHED` after a fresh
Worker process. The same publish attempt, message ID, Store incarnation and
DB identity are preserved; the before/after forced durable reads and typed
destination evidence pass an independent field comparison.

These are focused current-source receipts at Delay `b7b156e6`. They do not
promote r15 to current source, and they do not claim certified chaos, generic
transport response-loss, multi-shard placement or V1 release readiness.

## 2026-08-21 Current-source bounded chaos r17 and Kafka TCP-cut evidence

The complete strict-sequential bounded fault matrix was regenerated at Delay
`257161a203090fdf5657acdea896d6b8b5777040`:

```text
/private/tmp/nereus-delay-chaos-current-20260821-r17/bounded-chaos-matrix.json
```

It reports `PASS_BOUNDED`; all fourteen declared cells returned zero and each
has durable before/after state, fresh-process recovery and
`INDEPENDENT_FIELDS_PASS`. K1, P1 and Oxia are locked to
`05849884ca81fad767fda058444d1e17c7f9cbf9`,
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The r16 Kafka TCP-cut timeout remains diagnostic only. Focused rerun r2 at
`/private/tmp/nereus-delay-kafka-broker-tcp-cut-20260821-r2-state/` passed the
same-topic/cluster/topic-ID, changed-process, monotonic-offset, Broker-1
recovery and independent-field checks. These receipts strengthen the bounded
authority/failure evidence; they do not claim certified chaos, multi-shard
placement or V1 release readiness.

## 2026-08-21 current-source protocol-golden receipt

The protocol gate is now independently `PASS_CERTIFIED` at Delay
`dc37d2c2093eb46d3bf85f2bd964d5055a086194`, Kafka
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`:

```text
/private/tmp/nereus-delay-v1-protocol-golden-run-20260821-f.1N9Xji/protocol-golden.json
sha256=e144407304580231c879ff3ed9f4c84951f85f537bcda2f06a9f101b1f375365
```

The receipt covers 392 Delay protocol/store/clock tests, 17 Kafka guarded
tests and 8 Pulsar guarded tests, with zero failures, errors or skips. It closes
only the protocol-golden gate; the complete §23.1–§23.5 evidence set, including
the remaining nine release gates, is still open.

## 2026-08-21 current-source no-early receipt

The no-early gate is `PASS_CERTIFIED` at Delay
`f82e914d22c5b7d84f618e0ca31fa378a27bf3a2`. Its exact-source receipt is:

```text
/private/tmp/nereus-delay-v1-no-early-20260821-a.bOg67w/no-early.json
sha256=91692a7301b5e4fc99605ef6698c0c9208a12ea1379f7123d9db928ae7138d37
```

The 34-test run proves the trusted-UTC earliest-edge due rule, signed Pulsar
target clock bound, inclusive/batch/empty source boundary and reactivation
strictness. It records `max_early_ms=0`, with 20 ms worker and target bounds.
This is a gate-specific receipt, not a complete V1 release result.

## 2026-08-21 current-source same-adapter Large Payload authority

The real production chain now reaches destination egress on both locked
adapters. Kafka K1 uses
`/private/tmp/nereus-delay-v1-real-service-kafka-20260821-c.4owPvQ/run.log`
(`sha256=358271def7aeb50bc503c8a09f4eda430fbd7e4db8850f6775ba6d22de60f4d8`),
and Pulsar P1 uses
`/private/tmp/nereus-delay-v1-real-service-pulsar-20260821-a.WCUeKp/run.log`
(`sha256=84bf7f5171c0124463dd5efe40ca061ef7cea7bbc240bce14a569d77877c8d11`).
At Delay `2f38677f491bd0b9071269dc27937ec691827c49`, each two-shard run
proved guarded source barriers, real Oxia assignment/owner authority, Gateway
mTLS/JWT, real MinIO upload/attest/Commit/readback, Worker apply/ACK,
destination `PUBLISHED`, checkpoint and exact Gateway idempotency. The run
receipts are evidence for the two same-adapter cells only.

The full real-service contract still requires the cross-adapter Kafka-to-Pulsar
and Pulsar-to-Kafka paths plus activation cutover and the other §23 evidence.
Same-adapter success must not be promoted to a complete V1 release PASS.

## 2026-08-21 current-source cross-adapter Large Payload authority

The cross-adapter production-authority harness passed at Delay
`6b5c357c207169f98ec78be7f7007e2ebf3c1209`, with Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7`, Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`, Kafka client SHA-256
`1609dbd2794c5034d165769608767d5f8a01ea63293019cc0341e00d88ee1ed3`, Pulsar
distribution SHA-256 `373d8ac01bb82e6625a18690ed62a95719719acebf05145f8c2eefcfc23cd3f3`
and locked MinIO digest
`sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.

The canonical logs are:

```text
/private/tmp/nereus-delay-v1-cross-20260821-r29/K_TO_P.log
sha256=02db290caafda6d4cc814f2e2397726c50dcd91a2a3f1e0d9f2b27cfcdd76f40
/private/tmp/nereus-delay-v1-cross-20260821-r29/P_TO_K.log
sha256=44ffccb5e043f59ed15e60de6696e324359bd7d738a2276bbb816a259dee3608
```

K→P establishes the signed Kafka ingress route, real Gateway/Oxia/MinIO
authority, Worker due→Claim→Admission flow, Pulsar `PULSAR_SEND_ACK`, source
Outcome and exact payload/idempotency checks. P→K establishes the corresponding
Pulsar ingress route and Kafka `KAFKA_TRANSACTIONAL_RECEIPT`. Both directions
returned `BUILD SUCCESSFUL` and the harness returned
`CROSS_ADAPTER_LARGE_PAYLOAD_GATEWAY_E2E=PASS_CERTIFIED` after exact scoped
resource cleanup.

This is a PASS for the two named cross-adapter Large Payload cells, not for the
complete V1 release. Activation/cutover, full 19-cell chaos, capacity, soak,
upgrade/downgrade, operations/disaster-continuity and patch-distribution gates
remain required.

## 2026-08-21 current-source V1 closure audit

The current implementation candidate is Delay
`e44a23ccd76e9976c49427ebf46240fda8410abd`, locked with K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The exact-source real-service receipt is
`/private/tmp/nereus-delay-v1-real-service-candidate-20260821/real-service-r6/real-service.json`
with SHA-256
`db0297371961dbc8d3791a80f24940eaa07ca27da5938e6aa4fb547097e779c0` and
`PASS_CERTIFIED` status. It now proves the complete current Gateway/Oxia/
Broker/Worker/MinIO Large Payload production-authority chain for both adapters,
both cross-adapter directions and activation cutover. In particular, Worker
egress is complete and source-certified; it must not be tracked as an
unimplemented module.

The companion current receipts are protocol-golden and no-early:

```text
/private/tmp/nereus-delay-v1-protocol-golden-current-20260821-r2/protocol-golden.json
sha256=362f54f6cec0d6041be3be07f1b8ba6188322980f00fa853a4eae2fb4791d90c
/private/tmp/nereus-delay-v1-no-early-current-20260821-r2/no-early.json
sha256=d424f5017a110ff884355b4d7f28c5367a2855d2562eac97606efce6054d1a3a
```

The bounded capacity, soak and operations receipts are current-source
`PASS_CERTIFIED` profiles but preserve their `PASS_BOUNDED` boundaries. The
full chaos receipt has 11/19 independently passing cells and remains blocked
on eight named deterministic fault children. The strict release artifact
`/private/tmp/nereus-delay-v1-release-gate-current-20260821-r6/v1-release-candidate-gate.json`
(SHA-256 `bd64e1897210f834b6160223221c3b65360b74c7861fa6b37c874b0f202fd597`)
is `NOT_READY`; this is the correct V1 boundary until the remaining full-gate
inputs are independently produced.

## 2026-08-21 source-locked full-v1 contract runner

The Delay worktree now contains \`e2e/run-v1-full-contract-gate.sh\`, which
turns the existing protocol/restore/DLQ/resource tests into independently
auditable full-v1 input receipts. The runner binds Delay, K1, P1 and Oxia
HEADs to an external candidate lock and records required versus observed cells
without self-hashing the documentation overlay. Its first verified slice is
the upgrade/downgrade matrix at Delay
\`f3a0fd8f93a66e491825ee921179f8ede17dd4e6\`.

This does not promote a local contract receipt to a production release. The
physical capacity envelope, full long-cycle soak, operations authority and
patch-distribution gates remain separate and must be supplied by their own
real-observation runners.

The guarded-client distribution seam is
\`e2e/run-v1-full-patch-distribution-gate.sh\`. It keeps K1 and P1 source
authority separate, tests their typed resource-guard rejection and
delete/recreate behavior, records binary digests, and requires an actual
multi-Broker partial rollout. A compiled client or static source match alone
cannot satisfy this gate.

## 2026-08-21 physical capacity-envelope producer

The full-v1 capacity seam is explicit in
`e2e/run-v1-full-capacity-envelope-gate.sh`. It source-locks Delay, K1, P1 and
Oxia, runs local resource/capacity contracts, and optionally runs both real
multi-shard Large Payload production chains. It additionally requires a
source-matching `nereus-delay-v1-capacity-observation-v1` file whose declared
configurations are physically measured; missing or stale input is rejected.

The current base-source probe is retained at
`/private/tmp/nereus-delay-v1-full-capacity-real-current-20260821-r3/` for
Delay `9ab82d11c0b1b8bd60547d94ea695403d2c73b1c`. Both real child paths passed,
but the full capacity artifact is `FAIL` with
`measurement_status=MISSING`. This preserves the boundary between functional
Gateway/Oxia/Worker/MinIO E2E and a Broker/Lane/resource capacity envelope.

## 2026-08-22 current-source guarded patch-distribution certification

The gate fix is Delay `1631f8c1821116e8c7b3ef3f7166bab06c4b8a76`: K1, P1 and
Delay tests now use `--rerun-tasks`, so an up-to-date Gradle result cannot be
mistaken for fresh execution. The canonical artifact is
`/private/tmp/nereus-delay-v1-patch-distribution-current-20260822-r3/full-v1-gate-input.json`
with SHA-256
`c92104c707d208035aff782a3def37d84c409830bd2214bc543381e5eeab2ebb`.

It locks Kafka K1 `05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The artifact is
`PASS_CERTIFIED`, exclusion-free and source-lock exact: Kafka guarded producer
cases passed, Pulsar guarded common/broker tests and Delay guarded transport
tests were freshly executed, and the real two-Broker Pulsar partial-rollout
child passed broker stop/recovery, physical publish, ACK and checkpoint
release. Binary digests are in
`/private/tmp/nereus-delay-v1-patch-distribution-current-20260822-r3/binary-digests.tsv`.

This certifies only the patch-distribution input, not the V1 release. Capacity
measurement, complete chaos, soak, upgrade/downgrade, operations/disaster and
the remaining release inputs stay fail-closed. Exact scoped Docker postchecks
were empty; base images were retained.

## 2026-08-22 current-source release audit boundary

The final audit for candidate lock Delay
`1631f8c1821116e8c7b3ef3f7166bab06c4b8a76`, Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484` is retained at
`/private/tmp/nereus-delay-v1-release-gate-current-20260822-r1/v1-release-candidate-gate.json`
with SHA-256
`6436c4279cf3be7e579cbd0bae5c48fa6a1684e857bc711691dca015cba0b3d0`.
The documentation-only overlay is Delay `03e285c7d2d99c1389cf6d8d73338a9e8f8205c0`.

Source checks, cross-repository contracts and the full Gradle `check` passed;
the patch-distribution input is also exact-source `PASS`. The nine other full
V1 inputs were absent and therefore `BLOCKED` by the validator, leaving the
strict release result `NOT_READY`. The Gradle run still skips opt-in external
Oxia/MinIO/chaos methods when their endpoints are unset; this audit does not
promote those skips or any bounded receipt into release PASS.

## 2026-08-22 current-source Large Payload and operations evidence refresh

The current documentation-overlay source is Delay `336f6586a7013938356eea6bd3093225a646d7b1`, with Kafka K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, Pulsar P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`.

The source-locked physical-capacity runner produced
`/private/tmp/nereus-delay-v1-full-capacity-current-20260822-r1/full-v1-gate-input.json`
(SHA-256 `1e69acee2181ba87ec0d03bea9cc8689ed40951eda1db1ff5bb8ddd4361cba0d`).
Its Delay contract tests and both real children passed. Kafka's two-shard
Gateway mTLS/JWT -> Oxia Assignment/Owner -> Worker -> real MinIO -> destination
`PUBLISHED` receipt is recorded in
`kafka-large-payload-multi-shard.log` (SHA-256
`7e1a8ac79733a8b86e28ea1683787a01863ae3d5dfc757b0a449f9ade47311ad`); Pulsar's
corresponding two-guarded-partition/two-Worker chain is recorded in
`pulsar-large-payload-multi-shard.log` (SHA-256
`54617e4489106e56e183a771244af5bb8401a4df914dca66c8ced8a79c9ffdc8`).
The full capacity artifact remains `FAIL` with `measurement_status=MISSING`:
functional Large Payload E2E is now current-source evidence, but it is not a
Broker/Lane/resource capacity envelope.

The current-source operations retry is retained at
`/private/tmp/nereus-delay-v1-operations-current-20260822-r2/full-v1-gate-input.json`
(SHA-256 `69cc717120703bba10fdf0650f2187298a68b87fb52d9e6c0e32d99d4247af2a`).
Its bounded child is `PASS_BOUNDED` (SHA-256
`bca3dcdfb55fcb871396ca0af484a30a32ca389795086027398ea277d0acac59`): local
state-machine, real Oxia/MinIO checkpoint recovery, separate fresh-process
recovery and exact Docker cleanup all passed. The certified operations wrapper
remains `BLOCKED` only because the independent multi-Worker soak artifact is
missing; no operations or disaster-continuity release PASS is claimed.

For the candidate source lock itself, the upgrade/downgrade full-v1 artifact was
rerun in an isolated candidate clone at
`/private/tmp/nereus-delay-v1-upgrade-downgrade-candidate-20260822-r1/full-v1-gate-input.json`
(SHA-256 `023460f978fcc6a74c752419521e86e0869eb087fbb08aa4419e8af2547778a1`).
It is `PASS_CERTIFIED`, exclusion-free and covers all six required cells. The
capacity, soak, operations, chaos and remaining release obligations stay
fail-closed. Exact related Docker postchecks were empty; retained base images
were not globally pruned.

## 2026-08-22 release audit after candidate upgrade refresh

The strict audit artifact is
`/private/tmp/nereus-delay-v1-release-gate-current-20260822-r2/v1-release-candidate-gate.json`
with SHA-256 `6a3f7ff024933555613fd93c682d41d9b56b00c711e8d531947e086aac13c375`.
It used candidate Delay `1631f8c1821116e8c7b3ef3f7166bab06c4b8a76`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`; the audit-time documentation
overlay was Delay `ea3a76e24b7c7aa5e4bb20a3be50e0b101d13172`.

Source checks, cross-repository contracts and full Gradle `check` passed. The
candidate-source upgrade/downgrade and patch-distribution artifacts passed
exactly. Capacity and operations were present but rejected because they were
not complete `PASS_CERTIFIED` full-v1 inputs (`measurement_status=MISSING` and
missing independent soak, respectively); protocol-golden, chaos, real-service,
no-early, benchmark and soak had no complete full-v1 artifact. The resulting
release status is therefore `NOT_READY`; no complete ten-gate manifest exists.

## 2026-08-22 current Gateway-to-production evidence

Candidate locks are Delay `a40588bec6d363a4cfd2a4b7d3df5695649a0d79`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. Large Payload receipt r3 is
`PASS_CERTIFIED` in both directions, including exact payload readback and
idempotency through real Broker/Oxia/MinIO services.

Protocol-golden r3, no-early r4, real-service r2, chaos r6 (19/19),
upgrade-downgrade r4, patch-distribution r5, operations r16 and the certified
Soak child in r15 pass. The release wrapper keeps Soak blocked because it omits
`policy.longest_configured_period_seconds`. Capacity r10 and benchmark r11 also
remain blocked by missing physical Broker/Lane measurements. The strict audit
`/private/tmp/nereus-delay-v1-full-gates-20260822-r20/release/v1-release-candidate-gate.json`
is therefore `NOT_READY`.

## 2026-08-22 current Gateway-to-production release certification

The production-authority candidate is Delay
`c448e52607c8ff8bf3206c443fed35137a0c4cdc`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The strict release receipt
`/private/tmp/nereus-delay-v1-release-gate-20260822-r1/v1-release-candidate-gate.json`
is `PASS` with SHA-256
`e25fcec81e766afb6d9ba8c2e68149439bd25ced902ab3b260d346be11e563e9`.

The real-service receipt proves Kafka-to-Kafka, Pulsar-to-Pulsar and both
cross-adapter paths through Gateway mTLS/JWT, real Oxia authority, real MinIO,
Worker assignment/ownership and destination `PUBLISHED` outcomes. The
independent physical measurement receipts close the Broker/Lane/resource
envelope; the 19-cell chaos receipt, no-early receipt and 3-cycle soak close
the remaining authority/fault boundaries. Operations is release-certified only
with its exact independent soak input, wired by `c448e526`.

The architecture and evidence chain are complete for this source candidate.
Target-branch integration and release publication remain delivery boundaries,
not unverified claims of this audit.

## 2026-08-22 latest production-authority boundary — f4b7e005

The source-locked production chain is now evidenced end to end for the
functional path: Gateway mTLS/JWT → real Oxia assignment/owner authority →
Worker ingress and egress → real MinIO Large Payload/checkpoint handling →
Kafka/Pulsar destination `PUBLISHED` and source apply. Kafka and Pulsar
multi-shard cases both pass, including cross-adapter authority cases. The
19-cell full chaos receipt and the no-early clock-bound receipt are also
current-source `PASS_CERTIFIED` evidence.

The exact candidate is Delay
`f4b7e005c217d938c26bdba1eaa107cadb355da`, K1
`05849884ca81fad767fda058444d1e17c7f9cbf9`, P1
`0a2536484cd3932801a98dc88ff112b2df88a1c7` and Oxia
`37a17bef17202d5fd6e23282da5fd26d94865484`. The release receipt
`/private/tmp/nereus-delay-v1-release-gate-20260822-f4b7e005-rerun/v1-release-candidate-gate.json`
is `NOT_READY` solely because the independent physical Benchmark and Capacity
matrices required by §23.4 are absent. Functional E2E is therefore a
production-authority proof, not a capacity-envelope proof; no bounded result
is promoted across that boundary.

## 2026-08-22 physical-capacity execution boundary — a11d281c

The implementation source is `a11d281cbc39416359c9a03085146c40d2142053`.
The guarded K1/P1 capacity producers and
`e2e/run-v1-physical-capacity-matrix.sh` now provide the concrete §23.4
execution path for the Broker/Lane envelope, including real Broker admission,
object-reference mode, shard placement, target-health rejection, throughput,
broker evidence and host/container resource observations. The runner is
serial and source-lock checked; its fast mode is explicitly non-certifying.

The implementation passed K1/P1 compilation, full Gradle `test
checkDocumentation`, shell syntax and diff checks. It has not yet produced a
new physical matrix receipt. Therefore f4 evidence cannot be reused after the
source move to `a11d281c`, and the production-authority design is still
`NOT_READY` for release purposes until the new independent measurements and
full gate rerun pass.
