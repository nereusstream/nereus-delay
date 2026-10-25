package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.*;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/** One pre-Claim K1 producer; only the original recovery root can admit its exact type-4 Fence. */
public final class KafkaClientArtifactRecoveryFenceWriter implements TargetRecoveryFenceWriter, TargetOwnedBackendGroup {
    private final Object gate = new Object();
    private final Object construction = new Object();
    private final TargetWorkerSourceStartup.RecoveryInputsOpening opening;
    private final KafkaClientArtifactOwnedSystemWriter.Settings settings;
    private final SourceAssignment assignment;
    private final LocalBrokerMtlsResolver.SourceReceipt credential;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final TargetDestinationSdkBudget.Client allocation;
    private final SystemWriterAppendBudget.Writer writer;
    private final WorkClassExecutionRegistry classes;
    private final SharedRocksDbResources resources;
    private final ThreadPoolExecutor io;
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private GuardedProducer<byte[], byte[]> producer;
    private KafkaProducer.ConstructionCleanupFailure partial;
    private KafkaClientArtifactShardLogMutationAppender appender;
    private OwnedCheckpointIo<Boolean> cleanup;
    private TargetWorkerSourceStartup.RecoveryFenceAppend activePermit;
    private TargetTimeFenceMutationFactory.Prepared activeFence;
    private TargetWorkerSourceStartup.RecoveryFenceHandoff handoff;
    private TargetWorkerHostRuntime host;
    private List<Object> registrations;
    private Throwable failure;
    private boolean openingExited, constructionStarted, ready, frozen, cleanupStarting;
    private boolean sdkTerminated, normallyClosed, failureCleanup;
    private int activeAppends;
    private long closeStarted, normalCloseAt;

    private KafkaClientArtifactRecoveryFenceWriter(TargetWorkerSourceStartup.RecoveryInputsOpening opening,
            KafkaClientArtifactOwnedSystemWriter.Settings settings, LocalBrokerMtlsResolver.SourceReceipt credential,
            Supplier<TrustedUtcIntervalEvidence> time, TargetDestinationSdkBudget.Client allocation,
            SystemWriterAppendBudget.Writer writer, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io) {
        this.opening = opening; this.settings = settings; this.credential = credential; this.time = time;
        this.allocation = allocation; this.writer = writer; this.classes = classes; this.resources = resources; this.io = io;
        assignment = opening.assignment();
    }

    /** Invoked synchronously by the original prepare hook already running on graph.io; never submits and waits on that pool. */
    public static KafkaClientArtifactRecoveryFenceWriter open(KafkaClientArtifactOwnedSystemWriter.Settings settings,
            Map<String, Object> operational, byte[] sdkJar, TargetWorkerRuntimeConfig configuration,
            TargetWorkerSourceStartup.RecoveryInputsOpening opening, Supplier<TrustedUtcIntervalEvidence> time,
            TargetDestinationSdkBudget pool, CapacityVector cost, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources) {
        Objects.requireNonNull(settings); Objects.requireNonNull(opening); Objects.requireNonNull(time);
        opening.requireFenceFactory();
        final var graph = opening.startup().graph();
        if (graph.config() != configuration || graph.classes() != classes || graph.resources() != resources) {
            throw new IllegalArgumentException("recovery Fence producer requires its original process graph");
        }
        configuration.requireRuntime(classes, resources); pool.requireWorker(classes, resources);
        final var assignment = opening.assignment();
        final var credential = opening.credential();
        final var auth = credential.authorization();
        auth.requireAssignment(assignment);
        if (!(assignment.activationBarrier() instanceof KafkaActivationBarrier)
                || auth.resource().kind() != BrokerResourceIdentity.Kind.KAFKA
                || opening.maximumFrameBytes() > configuration.maximumSourceFrameBytes()
                || settings.bufferBytes() < configuration.maximumSourceFrameBytes()) {
            throw new IllegalArgumentException("recovery Fence producer changed its original K1 frame/resource envelope");
        }
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
        final var config = new HashMap<>(KafkaClientArtifactMtlsConfiguration.applySource(operational, credential));
        if (config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG)) {
            throw new IllegalArgumentException("K1 recovery Fence uses the original nontransactional Source writer");
        }
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
        minimum[CapacityDimension.ADAPTER_THREADS.wireValue() - 1] = 1;
        minimum[CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = 2L * settings.maximumBrokers();
        minimum[CapacityDimension.OPEN_FILES.wireValue() - 1] = 2L * settings.maximumBrokers();
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = settings.bufferBytes();
        if (!cost.covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException("recovery Fence producer cost does not cover its native settings");
        }
        final int millis = Math.toIntExact(settings.timeout().toMillis());
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, settings.bufferBytes());
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Math.toIntExact(configuration.maximumSourceFrameBytes()));
        config.put(ProducerConfig.SEND_BUFFER_CONFIG, settings.socketBytes());
        config.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, settings.socketBytes());
        config.put(ProducerConfig.ACKS_CONFIG, "all"); config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); config.put(ProducerConfig.LINGER_MS_CONFIG, 0L);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, (long) millis); config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, millis);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(10_000, millis));
        config.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        final var sealed = Map.copyOf(config);
        final var checked = new HashMap<>(sealed);
        checked.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        checked.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        new ProducerConfig(checked);
        TargetSdkArtifactIdentity.requireExact(KafkaProducer.class, sdkJar);
        final var allocation = pool.reserve(settings.allocationId(), cost);
        final SystemWriterAppendBudget.Writer writer;
        try { writer = opening.startup().systemWriterBudget().registerWriter(assignment.shardId()); }
        catch (RuntimeException | Error failed) { allocation.releaseUnstarted(); throw failed; }
        final var backend = new KafkaClientArtifactRecoveryFenceWriter(opening, settings, credential, time,
                allocation, writer, classes, resources, graph.io());
        try {
            opening.startup().retainRecoveryInputBackend(backend);
            opening.retainFenceWriter(backend);
            backend.requireOpening();
            allocation.beginConstruction();
            synchronized (backend.gate) { backend.constructionStarted = true; }
            final var nativeProducer = new KafkaProducer<byte[], byte[]>(sealed, new ByteArraySerializer(), new ByteArraySerializer());
            synchronized (backend.gate) { backend.producer = nativeProducer; }
            writer.bindProducer(nativeProducer);
            backend.requireOpening();
            final var resource = auth.resource().kafka();
            final var uuid = resource.nativeTopicUuid();
            nativeProducer.requireResourceGuard(new ProducerResourceGuard(resource.authenticatedClusterId(), auth.physicalTopic(),
                    new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()), assignment.shardId().partition()));
            backend.requireOpening();
            final var appender = new KafkaClientArtifactShardLogMutationAppender(nativeProducer, assignment.shardId(),
                    resource.authenticatedClusterId(), auth.physicalTopic(), uuid, settings.timeout(), writer, backend::requireAppend);
            synchronized (backend.gate) { backend.appender = appender; backend.ready = true; }
            opening.requireFenceFactory();
            return backend;
        } catch (RuntimeException | Error failed) {
            synchronized (backend.gate) {
                if (failed instanceof KafkaProducer.ConstructionCleanupFailure nativeFailure) backend.partial = nativeFailure;
                backend.frozen = true;
            }
            backend.remember(failed); writer.beginClose();
            if (!backend.constructionStarted) {
                writer.abandonUnstarted(); allocation.releaseUnstarted();
                synchronized (backend.gate) { backend.sdkTerminated = true; }
            }
            throw failed;
        } finally {
            synchronized (backend.gate) { backend.openingExited = true; }
            backend.refreshTermination();
        }
    }

    private void requireOpening() {
        opening.requireFenceOpening(this);
        credential.authorization().requireAssignment(assignment);
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
        opening.requireFenceFactory();
    }

    @Override public void requireBinding(TargetWorkerSourceStartup.RecoveryInputsOpening actual) {
        if (opening != actual) throw new IllegalArgumentException("recovery Fence writer belongs to another original input opening");
    }
    @Override public TargetOwnedBackendGroup backend() { return this; }

    private void requireAppend() {
        final TargetWorkerSourceStartup.RecoveryFenceAppend permit;
        final TargetTimeFenceMutationFactory.Prepared exact;
        synchronized (gate) {
            if (!ready || frozen || activeAppends != 1 || activePermit == null) {
                throw new IllegalStateException("original recovery Fence producer is not admitted for this call");
            }
            permit = activePermit; exact = activeFence;
        }
        permit.beforeSdk(this, exact);
        credential.authorization().requireAssignment(assignment);
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
        permit.beforeSdk(this, exact);
    }

    @Override public ShardLogMutationAppender.AppendOutcome append(TargetTimeFenceMutationFactory.Prepared exact,
            TargetWorkerSourceStartup.RecoveryFenceAppend permit) {
        Objects.requireNonNull(exact); Objects.requireNonNull(permit);
        if (exact.mutation().type() != SystemMutationType.TIME_FENCE || !assignment.shardId().equals(exact.mutation().shardId())) {
            throw new IllegalArgumentException("recovery writer accepts only its original bounded type-4 Fence");
        }
        permit.beforeSdk(this, exact);
        final KafkaClientArtifactShardLogMutationAppender current;
        synchronized (gate) {
            if (!openingExited || !ready || frozen || activeAppends != 0) {
                throw new IllegalStateException("original recovery Fence writer is unready, closing or already in use");
            }
            activeAppends = 1; activePermit = permit; activeFence = exact; current = appender;
        }
        try {
            requireAppend();
            final var outcome = current.append(exact.mutation());
            requireAppend();
            return outcome;
        } finally {
            synchronized (gate) { activeAppends = 0; activePermit = null; activeFence = null; }
        }
    }

    @Override public boolean operationsExited() {
        synchronized (gate) {
            return openingExited && activeAppends == 0 && !cleanupStarting
                    && (appender == null || appender.nativeOperationsExited())
                    && (cleanup == null || cleanup.ioExited());
        }
    }

    @Override public boolean closeForHandoff(TargetWorkerSourceStartup.RecoveryFenceHandoff permit) {
        Objects.requireNonNull(permit).requireWriter(this);
        synchronized (gate) {
            if (handoff == null) { handoff = permit; closeStarted = System.nanoTime(); }
            if (handoff != permit) throw new IllegalStateException("original recovery Fence handoff cannot be replaced");
            frozen = true;
        }
        writer.beginClose(); advanceCleanup(); refreshTermination();
        permit.requireWriter(this);
        synchronized (gate) {
            if (normallyClosed && sdkTerminated && operationsExited()
                    && normalCloseAt - closeStarted < settings.timeout().toNanos()) return true;
            if (System.nanoTime() - closeStarted >= settings.timeout().toNanos()) {
                throw new IllegalStateException("original recovery Fence producer exceeded its finite normal handoff deadline", failure);
            }
            return false;
        }
    }

    private Duration cleanupTimeout() {
        synchronized (gate) {
            if (handoff == null || failureCleanup) return settings.timeout();
            final long remaining = settings.timeout().toNanos() - (System.nanoTime() - closeStarted);
            if (remaining <= 0) throw new IllegalStateException("original recovery Fence handoff deadline elapsed");
            return Duration.ofNanos(remaining);
        }
    }

    private void advanceCleanup() {
        synchronized (gate) {
            if (!frozen || sdkTerminated || !openingExited || activeAppends != 0 || cleanupStarting
                    || cleanup != null && !cleanup.ioExited()) return;
            cleanupStarting = true;
            try {
                cleanup = OwnedCheckpointIo.submit(ignored -> {
                    try {
                        if (producer != null) {
                            producer.closeGuardedResources(cleanupTimeout());
                            if (!producer.guardedResourcesTerminated()) throw new IllegalStateException("original Fence producer native cleanup remains live");
                            if (producer.guardedFirstCloseFailure() != null) remember(producer.guardedFirstCloseFailure());
                            if (appender != null) appender.producerNativeClosed();
                            synchronized (gate) { normallyClosed = true; normalCloseAt = System.nanoTime(); }
                            writer.producerClosed(); allocation.closed();
                        } else if (partial != null) {
                            partial.reconcile(cleanupTimeout());
                            if (!partial.resourcesTerminated()) throw new IllegalStateException("original Fence producer partial construction remains live");
                            if (partial.firstCleanupFailure() != null) remember(partial.firstCleanupFailure());
                            writer.producerClosed(); allocation.closed();
                        } else if (!constructionStarted) {
                            writer.abandonUnstarted(); allocation.releaseUnstarted();
                        } else {
                            throw new IllegalStateException("started Fence producer lacks a retained native cleanup handle");
                        }
                        synchronized (gate) { sdkTerminated = true; }
                        return Boolean.TRUE;
                    } catch (RuntimeException | Error failed) { remember(failed); throw failed; }
                }, () -> {}, io);
                cleanup.receipt().whenComplete((done, failed) -> { if (failed != null) remember(failed); refreshTermination(); });
            } catch (RuntimeException | Error failed) { remember(failed); }
            finally { cleanupStarting = false; }
        }
    }

    private void refreshTermination() {
        synchronized (gate) { if (sdkTerminated && operationsExited()) terminated.complete(null); }
    }

    @Override public CompletableFuture<Void> closeBeforeHostBinding() {
        synchronized (gate) {
            if (host != null) return CompletableFuture.failedFuture(new IllegalStateException("recovery Fence backend requires its original Host permit"));
            frozen = true; failureCleanup = true;
        }
        writer.beginClose(); advanceCleanup(); refreshTermination(); return terminated.copy();
    }
    @Override public void requireWorkerGraph(WorkClassExecutionRegistry expected, SharedRocksDbResources shared) {
        if (classes != expected || resources != shared) throw new IllegalArgumentException("recovery Fence producer belongs to another process graph");
    }
    @Override public void requireHostComposition(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        Objects.requireNonNull(expected);
        for (var member : fleet) member.requireFleetComposition(classes, resources);
        synchronized (gate) {
            if (!normallyClosed || !resourcesTerminated() || host != null && host != expected
                    || fleet.stream().filter(member -> member.shardId().equals(assignment.shardId())).count() != 1) {
                throw new IllegalStateException("recovery Fence producer must normally exit before its original Shard transfers to Host");
            }
        }
    }
    @Override public void bindHost(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        requireHostComposition(expected, fleet); synchronized (gate) { host = expected; }
    }
    @Override public void admitWorker(TargetWorkerHostRuntime expected, TargetWorkerShardRuntime worker) {
        worker.requireFleetComposition(classes, resources);
        synchronized (gate) {
            if (host != expected || !worker.shardId().equals(assignment.shardId()) || !normallyClosed || !resourcesTerminated()) {
                throw new IllegalStateException("closed recovery Fence producer cannot transfer to another Shard/Host");
            }
        }
    }
    @Override public boolean applicationReady() { synchronized (gate) { return normallyClosed && resourcesTerminated(); } }
    @Override public boolean workerReady(TargetWorkerShardRuntime worker) {
        return worker.shardId().equals(assignment.shardId()) && applicationReady();
    }
    @Override public List<Object> freezeRegistrations() {
        synchronized (gate) {
            frozen = true;
            if (registrations == null) registrations = producer != null ? List.of(construction, producer)
                    : partial != null ? List.of(construction, partial) : List.of(construction);
        }
        writer.beginClose(); return registrations;
    }
    @Override public CompletableFuture<Void> closeOwnedBackend(TargetWorkerHostRuntime.BackendClosePermit permit) {
        permit.requireOriginalGroup(this, registrations);
        synchronized (gate) { failureCleanup = true; }
        advanceCleanup(); refreshTermination(); return terminated.copy();
    }
    @Override public boolean resourcesTerminated() { refreshTermination(); return terminated.isDone(); }
    @Override public boolean nativeResourcesTerminated() { synchronized (gate) { return sdkTerminated; } }
    @Override public Throwable cleanupFailure() { synchronized (gate) { return failure; } }
    @Override public LocalBrokerMtlsResolver.SourceReceipt sourceCredentialReceipt() { return credential; }

    private void remember(Throwable failed) {
        synchronized (gate) {
            if (failure == null) failure = failed;
            else if (failed != failure) {
                for (var prior : failure.getSuppressed()) if (prior == failed) return;
                failure.addSuppressed(failed);
            }
        }
    }
}
