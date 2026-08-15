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

## 16. 当前结论与仍需实测的数值

已经冻结：

- Direct SDK 为 Java 默认高性能入口，Gateway 为可选多语言/集中治理入口；
- 两者复用同一 Semantic Core 和 exact Command/receipt types；
- Gateway 请求幂等先持久化 exact prepared bytes，再取得 Broker ownership；
- Kafka/Pulsar 提供通用 guarded resource API，绝不依赖 Nereus SDK；
- Kafka K1 non-transactional wire 固定 Produce v13+ exact TopicId；K2
  provides an opt-in transaction-v2 target-plus-receipt binding, while its
  response-loss/Fetch/LSO/retention/source gates remain independently open；
- Pulsar 使用 first-class v22 create/per-SEND guard 和 guarded receipt；
- native AUTO_FAST 只允许 issuer-verified local snapshots，且 managed fallback
  保持 exact bytes；issuer now has a local protection-before-signing boundary,
  but production native capability authority and live Broker eligibility remain open；
- Route activation barriers now have an exact signed-snapshot-to-source-assignment
  projection, and Worker assignment lookup is tenant-authorized for current or
  historical Routes; live activation publication, Owner Lease CAS and
  source-session authority remain open；
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
