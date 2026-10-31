package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation;
import com.nereusstream.delay.ownership.SourceTransportCredentialUse;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetOwnedBackendGroup;
import com.nereusstream.delay.ownership.TargetSdkArtifactIdentity;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** One self-created, manual-assignment K1 Source consumer; never adopts a caller's borrowed consumer. */
public final class KafkaClientArtifactOwnedSourceBackend implements TargetOwnedBackendGroup {
    public record Settings(
            String allocationId,
            String authenticatedClusterId,
            UUID topicId,
            ShardId shard,
            String topic,
            Duration pollTimeout,
            Duration operationTimeout,
            int maximumConnections,
            int fetchBytes) {
        public Settings {
            Objects.requireNonNull(topicId);
            Objects.requireNonNull(shard);
            if (allocationId.isBlank()
                    || authenticatedClusterId.isBlank()
                    || topic.isBlank()
                    || pollTimeout.toMillis() <= 0
                    || pollTimeout.toMillis() > 60_000
                    || operationTimeout.toMillis() <= 0
                    || operationTimeout.toMillis() > 60_000
                    || maximumConnections <= 0
                    || maximumConnections == Integer.MAX_VALUE
                    || fetchBytes <= 0
                    || fetchBytes == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("owned K1 Source requires finite actual settings");
            }
        }
    }

    private final Settings settings;
    private final Map<String, Object> configuration;
    private final byte[] sdkJar;
    private final WorkClassExecutionRegistry classes;
    private final SharedRocksDbResources resources;
    private final TargetDestinationSdkBudget.Client allocation;
    private final ThreadPoolExecutor io;
    private final Object gate = new Object();
    private final Object construction = new Object();
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private OwnedCheckpointIo<KafkaClientArtifactSourceRecordConsumer> opening;
    private OwnedCheckpointIo<Boolean> cleanup;
    private GuardedConsumer<byte[], byte[]> nativeConsumer;
    private KafkaClientArtifactSourceRecordConsumer source;
    private TargetWorkerHostRuntime host;
    private TargetWorkerShardRuntime worker;
    private TargetWorkerHostRuntime.BackendClosePermit permit;
    private List<Object> frozenRegistrations;
    private CompletableFuture<Void> firstClose;
    private Throwable firstFailure;
    private int nativeCalls;
    private boolean ready;
    private boolean frozen;
    private boolean cleanupStarting;
    private boolean startupConstructed;
    private boolean sdkTerminated;
    private com.nereusstream.delay.ownership.SourceAssignment positionedAssignment;
    private long positionedOffset;
    private volatile com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt;
    private LocalBrokerMtlsResolver.SourceReceipt sourceCredentialReceipt;
    private SourceTransportCredentialUse sourceCredentialUse;

    public static KafkaClientArtifactOwnedSourceBackend openVerifiedSourceMtls(
            Settings settings,
            Map<String, Object> operational,
            byte[] sdkJar,
            LocalBrokerMtlsResolver.SourceReceipt receipt,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            Runnable actualActivation,
            TargetDestinationSdkBudget budget,
            CapacityVector cost,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ThreadPoolExecutor io) {
        final var authorization = receipt.authorization();
        if (!authorization.shard().equals(settings.shard())
                || authorization.resource().kind() != com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.KAFKA
                || !authorization.resource().kafka().authenticatedClusterId().equals(settings.authenticatedClusterId())
                || !authorization.resource().kafka().nativeTopicUuid().equals(settings.topicId())
                || !authorization.physicalTopic().equals(settings.topic())) {
            throw new IllegalStateException("K1 Source settings differ from the signed deployment resource");
        }
        authorization.requireName(Operation.ACK, (String) operational.get(ConsumerConfig.GROUP_ID_CONFIG));
        final var owner = open(
                settings,
                KafkaClientArtifactMtlsConfiguration.applySource(operational, receipt),
                sdkJar,
                () -> {
                    actualActivation.run();
                    receipt.requireOperation(Operation.LOOKUP, time.get());
                    receipt.requireOperation(Operation.CONSUME, time.get());
                    receipt.requireOperation(Operation.ACK, time.get());
                },
                budget,
                cost,
                classes,
                resources,
                io);
        owner.sourceCredentialReceipt = receipt;
        return owner;
    }

    @Override
    public LocalBrokerMtlsResolver.SourceReceipt sourceCredentialReceipt() {
        return sourceCredentialReceipt;
    }

    @Override
    public boolean sourceCredentialsReady(TargetWorkerShardRuntime expected) {
        final SourceTransportCredentialUse use;
        final Object consumer;
        synchronized (gate) {
            if (!workerReady(expected) || sourceCredentialReceipt == null || sourceCredentialUse == null) {
                return false;
            }
            use = sourceCredentialUse;
            consumer = nativeConsumer;
        }
        use.require(Operation.CONSUME, consumer);
        use.require(Operation.ACK, consumer);
        return true;
    }

    void bindSourceCredential(
            TargetWorkerShardRuntime actual,
            com.nereusstream.delay.ownership.SourceAssignment assignment,
            com.nereusstream.delay.protocol.OwnerIdentity owner,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        final GuardedConsumer<byte[], byte[]> consumer;
        synchronized (gate) {
            if (!ready || frozen || host != null || sourceCredentialUse != null || sourceCredentialReceipt == null) {
                throw new IllegalStateException("K1 Source credential binding is unavailable");
            }
            consumer = nativeConsumer;
        }
        final var use =
                actual.bindSourceCredential(sourceCredentialReceipt, source, assignment, owner, consumer, time, clock);
        synchronized (gate) {
            if (frozen || nativeConsumer != consumer) {
                throw new IllegalStateException("K1 Source changed during credential binding");
            }
            sourceCredentialUse = use;
        }
    }

    public static KafkaClientArtifactOwnedSourceBackend openVerifiedMtls(
            Settings settings,
            Map<String, Object> operational,
            byte[] sdkJar,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt receipt,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            Runnable actualActivation,
            TargetDestinationSdkBudget budget,
            CapacityVector cost,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ThreadPoolExecutor io) {
        final var loaded = Objects.requireNonNull(receipt, "resolved Source mTLS material");
        final var owner = open(
                settings,
                KafkaClientArtifactMtlsConfiguration.apply(operational, loaded),
                sdkJar,
                () -> {
                    actualActivation.run();
                    loaded.requireActive(time.get());
                },
                budget,
                cost,
                classes,
                resources,
                io);
        owner.credentialReceipt = loaded;
        return owner;
    }

    @Override
    public com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt() {
        return credentialReceipt;
    }

    private KafkaClientArtifactOwnedSourceBackend(
            Settings settings,
            Map<String, Object> input,
            byte[] sdkJar,
            TargetDestinationSdkBudget budget,
            CapacityVector cost,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ThreadPoolExecutor io) {
        this.settings = Objects.requireNonNull(settings);
        this.classes = Objects.requireNonNull(classes);
        this.resources = Objects.requireNonNull(resources);
        this.io = Objects.requireNonNull(io);
        Bytes.requireLength(sdkJar, 32, "activated K1 JAR");
        this.sdkJar = Bytes.copy(sdkJar);
        budget.requireWorker(classes, resources);
        configuration = sourceConfiguration(input, settings);
        // Configuration validation precedes any native allocation, including partial SDK construction.
        new ConsumerConfig(configuration);
        TargetSdkArtifactIdentity.requireExact(KafkaConsumer.class, this.sdkJar);
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = settings.maximumConnections();
        minimum[CapacityDimension.OPEN_FILES.wireValue() - 1] = settings.maximumConnections();
        minimum[CapacityDimension.ADAPTER_THREADS.wireValue() - 1] = 1; // built-in SASL refresh, if configured
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = settings.fetchBytes();
        if (!cost.covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException("K1 Source allocation does not cover configured SDK resources");
        }
        allocation = budget.reserve(settings.allocationId(), cost);
    }

    public static KafkaClientArtifactOwnedSourceBackend open(
            Settings settings,
            Map<String, Object> configuration,
            byte[] sdkJar,
            Runnable actualActivation,
            TargetDestinationSdkBudget budget,
            CapacityVector cost,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources,
            ThreadPoolExecutor io) {
        final var owner = new KafkaClientArtifactOwnedSourceBackend(
                settings, configuration, sdkJar, budget, cost, classes, resources, io);
        try {
            owner.opening = OwnedCheckpointIo.submit(
                    guard -> {
                        guard.run();
                        actualActivation.run();
                        owner.allocation.beginConstruction();
                        synchronized (owner.gate) {
                            owner.startupConstructed = true;
                        }
                        final var consumer = new KafkaConsumer<byte[], byte[]>(owner.configuration);
                        synchronized (owner.gate) {
                            owner.nativeConsumer = consumer;
                        }
                        TargetSdkArtifactIdentity.requireExact(consumer.getClass(), owner.sdkJar);
                        consumer.bindResourceGuard(new ConsumerResourceGuard(
                                settings.authenticatedClusterId(),
                                settings.topic(),
                                new org.apache.kafka.common.Uuid(
                                        settings.topicId().getMostSignificantBits(),
                                        settings.topicId().getLeastSignificantBits()),
                                settings.shard().partition()));
                        final var source = KafkaClientArtifactSourceRecordConsumer.forOwnedTarget(
                                consumer,
                                settings.authenticatedClusterId(),
                                settings.topicId(),
                                settings.shard(),
                                settings.topic(),
                                settings.pollTimeout(),
                                owner);
                        synchronized (owner.gate) {
                            owner.source = source;
                        }
                        actualActivation.run();
                        guard.run();
                        return source;
                    },
                    () -> {
                        synchronized (owner.gate) {
                            if (owner.frozen) {
                                throw new IllegalStateException("K1 Source construction is frozen");
                            }
                        }
                    },
                    io);
            owner.opening.receipt().whenComplete((value, failure) -> {
                if (failure != null) {
                    owner.recordFailure(failure);
                }
                owner.cleanupProgress();
            });
            return owner;
        } catch (RuntimeException | Error failed) {
            owner.allocation.releaseUnstarted();
            throw failed;
        }
    }

    private static Map<String, Object> sourceConfiguration(Map<String, Object> input, Settings settings) {
        final var config = new HashMap<>(Objects.requireNonNull(input, "actual Source SDK configuration"));
        final var providers = config.get("config.providers");
        if (providers != null && !List.of("", "[]").contains(providers.toString())) {
            throw new IllegalArgumentException("resolve Source configuration before native resource admission");
        }
        final var protocol = config.get(ConsumerConfig.GROUP_PROTOCOL_CONFIG);
        if (protocol != null && !"classic".equalsIgnoreCase(protocol.toString())) {
            throw new IllegalArgumentException("owned K1 Source requires the classic manual-assignment consumer");
        }
        if (config.get(ConsumerConfig.GROUP_ID_CONFIG) == null
                || config.get(ConsumerConfig.GROUP_ID_CONFIG).toString().isBlank()) {
            throw new IllegalArgumentException("K1 Source ACK requires an explicit stable group ID");
        }
        requireBytesDeserializer(config, ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        requireBytesDeserializer(config, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        if (config.containsKey(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
                && !List.of("", "[]")
                        .contains(config.get(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)
                                .toString())) {
            throw new IllegalArgumentException("owned Source does not adopt interceptor-owned native resources");
        }
        config.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, settings.fetchBytes());
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, settings.fetchBytes());
        config.put(
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG,
                Math.toIntExact(settings.operationTimeout().toMillis()));
        config.put(
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                Math.toIntExact(settings.operationTimeout().toMillis()));
        config.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        return Map.copyOf(config);
    }

    private static void requireBytesDeserializer(Map<String, Object> config, String key) {
        final var configured = config.get(key);
        if (configured != null
                && configured != ByteArrayDeserializer.class
                && !ByteArrayDeserializer.class.getName().equals(configured.toString())) {
            throw new IllegalArgumentException("owned Source requires the original byte-array decoder");
        }
        config.put(key, ByteArrayDeserializer.class);
    }

    public boolean finishStartup() {
        synchronized (gate) {
            if (frozen) {
                throw new IllegalStateException("K1 Source is frozen");
            }
            if (!opening.operationExited()) {
                return false;
            }
            final var result = opening.receipt();
            if (!result.isDone()) {
                return false;
            }
            result.join(); // Propagate the original failure even while its native cleanup remains retained.
            if (!opening.ioExited()) {
                return false;
            }
            source = opening.completedValue()
                    .orElseThrow(() -> new IllegalStateException("K1 Source opening failed", openingFailure()));
            ready = true;
            return true;
        }
    }

    public KafkaClientArtifactSourceRecordConsumer source() {
        synchronized (gate) {
            if (!ready || frozen) {
                throw new IllegalStateException("K1 Source has not handed off its actual native consumer");
            }
            return source;
        }
    }

    /** Position the same owned consumer before Host binding; no application poll or ACK is enabled here. */
    void positionAcceptedAssignment(
            com.nereusstream.delay.ownership.SourceAssignment assignment,
            com.nereusstream.delay.protocol.SourcePosition resumeAfter) {
        final GuardedConsumer<byte[], byte[]> consumer;
        if (!(assignment.activationBarrier() instanceof com.nereusstream.delay.protocol.KafkaActivationBarrier barrier)
                || !settings.shard().equals(assignment.shardId())
                || !settings.authenticatedClusterId().equals(barrier.authenticatedClusterId())
                || !settings.topicId().equals(barrier.nativeTopicUuid())
                || barrier.exclusiveOffset() < 0) {
            throw new IllegalArgumentException("owned K1 Source differs from the accepted activation barrier");
        }
        barrier.validatePosition(Objects.requireNonNull(resumeAfter, "actual activated Store cut"));
        final var cut = (com.nereusstream.delay.protocol.KafkaSourcePosition) resumeAfter;
        if (cut.offset() < 0 || !barrier.reachedBy(cut)) {
            throw new IllegalArgumentException(
                    "owned K1 Source resume cut has not reached its original activation barrier");
        }
        final long nextOffset = Math.addExact(cut.offset(), 1L);
        synchronized (gate) {
            if (!ready || frozen || host != null || nativeCalls != 0) {
                throw new IllegalStateException("K1 Source positioning is not available before Host binding");
            }
            if (positionedAssignment != null) {
                if (!positionedAssignment.sameIdentity(assignment) || positionedOffset != nextOffset) {
                    throw new IllegalStateException("K1 Source cannot replace its original positioned assignment/cut");
                }
                return;
            }
            nativeCalls++;
            consumer = nativeConsumer;
        }
        try {
            final var partition = new org.apache.kafka.common.TopicPartition(
                    settings.topic(), settings.shard().partition());
            consumer.seek(partition, nextOffset);
            if (consumer.position(partition, settings.operationTimeout()) != nextOffset) {
                throw new IllegalStateException("owned K1 Source did not establish its actual Store resume position");
            }
            synchronized (gate) {
                if (frozen) {
                    throw new IllegalStateException("K1 Source was frozen during positioning");
                }
                positionedAssignment = assignment;
                positionedOffset = nextOffset;
            }
        } finally {
            synchronized (gate) {
                nativeCalls--;
            }
            cleanupProgress();
        }
    }

    @Override
    public void requireWorkerGraph(WorkClassExecutionRegistry expected, SharedRocksDbResources shared) {
        if (classes != expected || resources != shared) {
            throw new IllegalArgumentException("K1 Source belongs to another Worker graph");
        }
    }

    @Override
    public void requireHostComposition(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        Objects.requireNonNull(expected, "host");
        for (var member : fleet) {
            member.requireFleetComposition(classes, resources);
        }
        synchronized (gate) {
            requireHostCompositionLocked(expected, fleet);
        }
    }

    private void requireHostCompositionLocked(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        if (!ready || frozen || positionedAssignment == null || host != null && host != expected) {
            throw new IllegalStateException("K1 Source is unready, frozen or owned by another Host");
        }
        if (fleet.stream().filter(member -> member.ownsSourceConsumer(source)).count() != 1) {
            throw new IllegalStateException("K1 Source requires its one actual Worker owner");
        }
    }

    @Override
    public void bindHost(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        Objects.requireNonNull(expected, "host");
        for (var member : fleet) {
            member.requireFleetComposition(classes, resources);
        }
        synchronized (gate) {
            requireHostCompositionLocked(expected, fleet);
            worker = fleet.stream()
                    .filter(member -> member.ownsSourceConsumer(source))
                    .findFirst()
                    .orElseThrow();
            host = expected;
        }
    }

    @Override
    public void admitWorker(TargetWorkerHostRuntime expected, TargetWorkerShardRuntime candidate) {
        candidate.requireFleetComposition(classes, resources);
        synchronized (gate) {
            if (host != expected || frozen || worker != candidate || !candidate.ownsSourceConsumer(source)) {
                throw new IllegalStateException("a replacement K1 Source requires a new actual native owner");
            }
        }
    }

    @Override
    public boolean applicationReady() {
        synchronized (gate) {
            return ready && host != null && !frozen;
        }
    }

    @Override
    public boolean workerReady(TargetWorkerShardRuntime expected) {
        synchronized (gate) {
            return ready && host != null && !frozen && worker == expected;
        }
    }

    <T> T nativeOperation(Supplier<T> operation) {
        return nativeOperation(Operation.CONSUME, operation);
    }

    <T> T nativeOperation(Operation role, Supplier<T> operation) {
        return nativeOperation(role, role != Operation.ACK, operation);
    }

    <T> T nativeReadOperation(Operation role, Supplier<T> operation) {
        return nativeOperation(role, true, operation);
    }

    private <T> T nativeOperation(Operation role, boolean checkAfter, Supplier<T> operation) {
        final SourceTransportCredentialUse credential;
        final GuardedConsumer<byte[], byte[]> actual;
        synchronized (gate) {
            if (!ready || host == null || frozen) {
                throw new IllegalStateException("K1 Source native admission is frozen or unbound");
            }
            credential = sourceCredentialUse;
            actual = nativeConsumer;
            nativeCalls = Math.incrementExact(nativeCalls);
        }
        try {
            requireSourceMaterial(credential, role, actual);
            final var result = operation.get();
            if (checkAfter) {
                requireSourceMaterial(credential, role, actual);
            }
            return result;
        } finally {
            synchronized (gate) {
                nativeCalls--;
            }
            cleanupProgress();
        }
    }

    private void requireSourceMaterial(SourceTransportCredentialUse credential, Operation role, Object actual) {
        if (sourceCredentialReceipt == null) {
            return;
        }
        try {
            Objects.requireNonNull(credential, "actual bound Source material use")
                    .require(role, actual);
        } catch (RuntimeException | Error failure) {
            recordFailure(failure);
            try {
                host.quarantineOwnedBackends();
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    @Override
    public List<Object> freezeRegistrations() {
        final GuardedConsumer<byte[], byte[]> consumer;
        final List<Object> snapshot;
        synchronized (gate) {
            frozen = true;
            if (frozenRegistrations == null) {
                final var registered = new java.util.ArrayList<Object>();
                registered.add(construction);
                if (nativeConsumer != null) {
                    registered.add(nativeConsumer);
                }
                if (source != null) {
                    registered.add(source);
                }
                frozenRegistrations = List.copyOf(registered);
            }
            consumer = nativeConsumer;
            snapshot = frozenRegistrations;
        }
        // wakeup is the only cross-thread consumer call; never acquire the Source wrapper/poll monitor first.
        if (consumer != null) {
            try {
                consumer.wakeup();
            } catch (RuntimeException | Error failed) {
                recordFailure(failed);
            }
        }
        return snapshot;
    }

    CompletableFuture<Void> originalClose() {
        synchronized (gate) {
            return firstClose == null ? null : firstClose.copy();
        }
    }

    CompletableFuture<Void> closeSourceNormally() {
        synchronized (gate) {
            if (firstClose == null) {
                firstClose = new CompletableFuture<>();
            }
        }
        freezeRegistrations();
        advanceCleanup();
        synchronized (gate) {
            return firstClose.copy();
        }
    }

    public CompletableFuture<Void> closeBeforeHostBinding() {
        synchronized (gate) {
            if (host != null) {
                return CompletableFuture.failedFuture(new IllegalStateException("K1 Source needs its Host permit"));
            }
            frozen = true;
        }
        freezeRegistrations();
        advanceCleanup();
        return terminated.copy();
    }

    @Override
    public CompletableFuture<Void> closeOwnedBackend(TargetWorkerHostRuntime.BackendClosePermit actual) {
        actual.requireOriginalGroup(this, frozenRegistrations);
        synchronized (gate) {
            if (permit != null && permit != actual) {
                throw new IllegalStateException("K1 Source close generation changed");
            }
            permit = actual;
        }
        advanceCleanup();
        return terminated.copy();
    }

    private void advanceCleanup() {
        synchronized (gate) {
            if (!frozen
                    || terminated.isDone()
                    || cleanupStarting
                    || cleanup != null && !cleanup.ioExited()
                    || host != null && permit == null && firstClose == null
                    || nativeCalls != 0
                    || opening == null
                    || !opening.ioExited()) {
                return;
            }
            cleanupStarting = true;
            try {
                cleanup = OwnedCheckpointIo.submit(
                        ignored -> {
                            final GuardedConsumer<byte[], byte[]> actual;
                            final CompletableFuture<Void> original;
                            synchronized (gate) {
                                if (nativeCalls != 0) {
                                    return false;
                                }
                                actual = nativeConsumer;
                                if (firstClose == null) {
                                    firstClose = new CompletableFuture<>();
                                }
                                original = firstClose;
                            }
                            try {
                                if (actual != null) {
                                    actual.closeGuardedResources(settings.operationTimeout());
                                    if (!actual.guardedResourcesTerminated()) {
                                        throw new IllegalStateException("K1 native component cleanup is not confirmed");
                                    }
                                } else {
                                    final var failed = openingFailure();
                                    if (failed instanceof ClassicKafkaConsumer.ConstructionCleanupFailure partial) {
                                        partial.reconcile(settings.operationTimeout());
                                        if (!partial.resourcesTerminated()) {
                                            throw new IllegalStateException("K1 partial construction remains live");
                                        }
                                    } else if (startupConstructed
                                            && !(failed instanceof org.apache.kafka.common.KafkaException)) {
                                        throw new IllegalStateException(
                                                "K1 construction has no original cleanup evidence", failed);
                                    }
                                }
                                synchronized (gate) {
                                    sdkTerminated = true;
                                }
                                original.complete(
                                        null); // repeated cleanup cannot rewrite an already failed first result
                                return true;
                            } catch (RuntimeException | Error failed) {
                                original.completeExceptionally(failed);
                                recordFailure(failed);
                                throw failed;
                            }
                        },
                        () -> {},
                        io);
                cleanup.receipt().whenComplete((done, failed) -> {
                    if (failed != null) {
                        recordFailure(failed);
                    }
                    if (Boolean.TRUE.equals(done)) {
                        finishCleanup();
                    }
                });
            } catch (RuntimeException | Error failed) {
                recordFailure(failed);
            } finally {
                cleanupStarting = false;
            }
        }
    }

    private void finishCleanup() {
        final TargetWorkerHostRuntime.BackendClosePermit actual;
        synchronized (gate) {
            if (!sdkTerminated
                    || nativeCalls != 0
                    || cleanup == null
                    || !cleanup.ioExited()
                    || host != null && permit == null) {
                return;
            }
            actual = permit;
        }
        if (actual != null) {
            actual.requireOriginalGroup(this, frozenRegistrations);
        }
        allocation.closed();
        if (source != null) {
            source.backendTerminated(this);
        }
        terminated.complete(null);
    }

    void cleanupProgress() {
        synchronized (gate) {
            if (!frozen) {
                return;
            }
        }
        try {
            io.execute(this::advanceCleanup);
        } catch (RuntimeException | Error failed) {
            recordFailure(failed);
        }
    }

    private Throwable openingFailure() {
        return CheckpointInputFuture.ownedFailure(opening.receipt());
    }

    private void recordFailure(Throwable failed) {
        synchronized (gate) {
            if (firstFailure == null) {
                firstFailure = failed;
            }
        }
    }

    @Override
    public boolean resourcesTerminated() {
        return terminated.isDone() && !terminated.isCompletedExceptionally();
    }

    @Override
    public boolean nativeResourcesTerminated() {
        synchronized (gate) {
            return sdkTerminated;
        }
    }

    @Override
    public Throwable cleanupFailure() {
        synchronized (gate) {
            return firstFailure;
        }
    }
}
