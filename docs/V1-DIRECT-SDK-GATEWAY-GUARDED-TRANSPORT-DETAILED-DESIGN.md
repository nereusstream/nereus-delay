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
success evidence 固定 `errorCode=0`、nonnegative base offset/log append time、actual request
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
response、offset 或 nonnegative Broker `logAppendTimeMs`，Nereus transport 不得把普通
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

`ProducerBuilder<T>` 增加：

```java
ProducerBuilder<T> resourceGuard(TopicResourceGuard guard);
```

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

Writer E2E 不能被误报为完整 Worker production E2E。

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
`69d89839e4e80326e5317a4f5066667e270a7136` supply the local canonical
Route/resource value types, UUIDv7 identity seam, `ROUTING_HASH_V1`
calculator, zero-I/O `DefaultDelaySemanticCore`, fail-closed signed-cache
watch, exact historical-route plan
resolver, shared `DefaultSubmissionCoordinator`, explicit `DefaultDelayClient`,
guarded transport bridges, the in-memory Gateway Schedule/idempotency
composition and the Oxia event/head-CAS Route publisher/provider. Focused
deterministic tests and a full local `check` pass at
`ec12efbf2bf82fc15c5038af5db84e3e634674bd`; Route
authority focused checks pass at `62a94389`, and Gateway CAS focused checks at
`e276bec3`. This is not completion of D1/D4/D5: activation-barrier/session-fenced
real Oxia authority, native eligibility authority, generated Gateway service,
durable/HA idempotency, package/module split, production Kafka/Pulsar client
artifacts, Worker wiring and real-service cuts remain open.

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

2026-08-14 progress evidence: Kafka worktree commit
`d1810fa3466e1378a33c5c6327c7f401cec03d07` on
`nereus/delay-guarded-producer-v1` implements the generic public guarded
producer surface and the first Sender/RecordAccumulator/ProducerBatch evidence
path. Focused tests pass for public preflight/future completion, v13 and exact
TopicId binding, guard-separated batches, leader retry with the same identity,
disconnect ambiguity, definitive `UNKNOWN_TOPIC_ID` and non-allowlisted
rejection. It remains a client/mock slice: real Kafka delete/recreate and
leader-failover integration, artifact/source digest capture and the completion
gate are still open. D2 must not use stock Producer as a substitute.

完成门：focused client tests + delete/recreate/leader failover integration + source lock SHA/digest。未完成 Kafka patch 前 `ProductionKafkaProduceTransport` 不得使用 stock Producer 冒充。

### Phase D2：Kafka Nereus transport

`ProductionKafkaProduceTransport` 的严格配置/guard bridge 与现有 pinned outcome
mapping 已在 Delay 中建立 source-level composition seam；仍需在 D2 接入
真实 locked Kafka client artifact、TopicId/v13 response evidence、Direct SDK
E2E 和 Worker ACK-after-sync。

完成门：QUEUED/definite/uncertain 三态和 Worker ACK-after-sync crash cut。

### Phase K2：Kafka transactional guarded destination

在 Kafka 独立切片中为 target + receipt transaction 提供 exact TopicId、Produce v13+、transaction
coordinator/reenqueue identity 保持和双 topic response evidence。K1 的 `sendGuarded` 不自动满足
这个门。

完成门：同事务两个资源均做 delete/recreate/failover cuts；任一 identity 漂移不向 replacement
写入；commit/abort/response-loss 三态与现有 Attempt evidence 契约一致。K2 完成前 Kafka
atomic-target-receipt Profile 必须保持不可激活。

### Phase P1：Pulsar first-class resource guard

从 `5.0.0-M1@8dae0236` 创建独立分支，实现 §10。

2026-08-14 progress evidence: Pulsar worktree branch
`nereus/delay-resource-guard-v1` is implemented in commits
`19c97bf836d521f0e6103c542819723e70ccdbab` and
`be226fe6c88634e9a94ba5c6a0f5859bc510cb66`. The v22 wire/API slice and the
broker/client enforcement slice pass the focused common/broker tests and
affected-module checkstyle with an independent Gradle user home. The patch
keeps generic Pulsar APIs and uses strict three-property resource tuples,
INVALID-before-update publication, exact create/SEND guard comparison,
broker-entry timestamp receipt echo and typed evidence correlation.

This remains an isolated upstream slice, not a Delay repository production
transport. Real delete/recreate, unload/failover, old-peer proxy compatibility,
artifact/source digest and Docker lifecycle cuts remain required before the
completion gate can pass; D3 must not use an ordinary Pulsar producer as a
substitute.

完成门：v22 wire compatibility、create+per-SEND guard、receipt echo、delete/recreate/unload/failover cut、focused modules格式检查。

### Phase D3：Pulsar Nereus transport

`ProductionPulsarSendTransport` 的严格配置/managed-native guard bridge 与现有
pinned outcome mapping 已在 Delay 中建立 source-level composition seam；仍需
在 D3 接入真实 locked Pulsar v22 client artifact、GuardedMessageId、typed
response evidence、Direct SDK E2E 和 Worker ACK-after-sync。

完成门：exact GuardedMessageId、typed pre-persistence rejection、uncertainty persistence、source lock。

### Phase D4：Oxia Route authority

`InMemorySignedRouteSnapshotProvider` and the 2026-08-14 Delay commit
`62a94389` now supply local signed-cache/Oxia composition evidence.
`OxiaSignedRouteSnapshotPublisher` writes immutable canonical Route events and
advances an Oxia head with version CAS; `OxiaSignedRouteSnapshotProvider`
rebuilds only through the head, verifies snapshot/event canonical bytes and
signatures, replays contiguous revisions, refreshes from Oxia notifications,
and quarantines same-incarnation immutable drift. Exact lookup remains
tenant-scoped and preparation still reads only the local cache.

仍需 activation barrier publication, Oxia session fencing, cross-process
response-loss/reconnect evidence, real-service cache staleness cuts and native
eligibility authority。完成门：snapshot signature/digest、lifecycle、route
expansion、credential binding、cache staleness cuts。
同一 Route Incarnation 的 resource/partition/hash/query-retention/size drift 必须 quarantine；仅
lifecycle/control-version/validity 与有等价证明的 credential generation 可发布新 snapshot。

### Phase D5：Gateway

本地 conformance slice 已提供 `GatewayScheduleRequestV1`、canonical body/key
hash、prepared-before-ownership、in-memory single-record CAS、one-shot Gateway
permit、outcome replay、body conflict、`RetryUncertain` expected-prior/retry-ID
CAS、`GatewayIdempotencyStore`、strict record decoders、Oxia single-record
version-CAS 和 source `delay_gateway.proto`。Response loss is fail-closed:
the durable store may replay an exact aggregate but does not reconstruct a
physical ownership permit。仍需实现 generated gRPC modules、mTLS/JWT tenant
authority、quota/control reserve、safe audit、Gateway HA/transactional
durability、RetryUncertain late-evidence/aggregate、crash cuts 和
多语言最小 SDK。

完成门：双入口 byte equivalence、HA crash cuts、non-enumerating auth、control reserve、load test。

### Phase D6：完整 Worker production vertical

真实 Kafka/Pulsar source、ownership、RocksDB apply/ACK、due/Lane/publish/checkpoint/recovery。

完成门仍以主设计 §23.5 为准；不能从 D1–D5 推导 V1 release-ready。

## 16. 当前结论与仍需实测的数值

已经冻结：

- Direct SDK 为 Java 默认高性能入口，Gateway 为可选多语言/集中治理入口；
- 两者复用同一 Semantic Core 和 exact Command/receipt types；
- Gateway 请求幂等先持久化 exact prepared bytes，再取得 Broker ownership；
- Kafka/Pulsar 提供通用 guarded resource API，绝不依赖 Nereus SDK；
- Kafka K1 non-transactional wire 固定 Produce v13+ exact TopicId；target + receipt transaction
  仍由 K2 独立 gate；
- Pulsar 使用 first-class v22 create/per-SEND guard 和 guarded receipt；
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
