package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetOwnedBackendGroup;
import com.nereusstream.delay.ownership.TargetSourceBacklog;
import com.nereusstream.delay.ownership.TargetSourceBacklogRead;
import com.nereusstream.delay.ownership.TargetWorkerCheckpointComposition;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfig;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** One Source's four finite read slots and the exact SDK/cleanup owners behind their receipts. */
public final class TargetWorkerSourceReadResources
        implements AutoCloseable, TargetCheckpointCandidateWorkClassExecutor.PreparationIo {
    private enum Read {
        CONTROL,
        OUTCOME,
        ADMISSION,
        OBSERVATION
    }

    private static final Set<CapacityDimension> SDK_DIMENSIONS = Set.of(
            CapacityDimension.OPEN_FILES,
            CapacityDimension.ADAPTER_CONNECTIONS,
            CapacityDimension.ADAPTER_PRODUCERS,
            CapacityDimension.ADAPTER_THREADS,
            CapacityDimension.BUFFERED_MESSAGES,
            CapacityDimension.BUFFERED_BYTES,
            CapacityDimension.DIRECT_BUFFER_BYTES,
            CapacityDimension.OTHER_NATIVE_BYTES);

    /** Literal resource declarations only; neither these costs nor a reader credential grants retention. */
    public static final class Configuration {
        private final String allocationPrefix;
        private final Map<Read, CapacityVector> costs;
        private final PulsarClientArtifactOwnedBackendGroup.Settings historyClient;
        private final CapacityVector historyClientCost;
        private final int maximumFrameBytes;
        private final TargetSourceBacklog.Limits backlog;
        private final CapacityVector demand;
        private final Set<String> identities;
        private final long retainedHeapBytes;

        private Configuration(
                String prefix,
                Map<Read, CapacityVector> costs,
                PulsarClientArtifactOwnedBackendGroup.Settings historyClient,
                CapacityVector historyClientCost,
                TargetWorkerRuntimeConfig worker) {
            if (prefix == null || !prefix.matches("[A-Za-z0-9._/-]{1,448}") || prefix.endsWith("/")) {
                throw new IllegalArgumentException("Source read allocation prefix must be a bounded literal identity");
            }
            this.allocationPrefix = prefix;
            this.costs = Map.copyOf(costs);
            this.historyClient = historyClient;
            this.historyClientCost = historyClientCost;
            this.maximumFrameBytes = Math.toIntExact(worker.maximumSourceFrameBytes());
            this.backlog = worker.backlogRead();
            CapacityVector sum = CapacityVector.empty();
            long heap = 0;
            final var ids = new HashSet<String>();
            for (var role : Read.values()) {
                final var cost = Objects.requireNonNull(costs.get(role));
                requireSdkCost(cost);
                sum = sum.add(cost);
                heap = Math.addExact(heap, cost.amount(CapacityDimension.BUFFERED_BYTES));
                ids.add(id(role));
            }
            if (historyClient != null) {
                requireSdkCost(historyClientCost);
                sum = sum.add(historyClientCost);
                heap = Math.addExact(heap, historyClientCost.amount(CapacityDimension.BUFFERED_BYTES));
                ids.add(historyClient.allocationId());
            }
            demand = sum;
            identities = Set.copyOf(ids);
            retainedHeapBytes = heap;
        }

        public static Configuration load(
                Path file,
                TargetWorkerServiceConfiguration.SourceConfiguration source,
                TargetWorkerRuntimeConfig worker)
                throws IOException {
            final var fields = TargetWorkerRuntimeConfigLoader.loadFields(file);
            final String prefix = fields.text("reads.allocationPrefix");
            final var costs = new EnumMap<Read, CapacityVector>(Read.class);
            for (var role : Read.values()) {
                final var cost =
                        fields.capacity("reads.operationCost." + role.name().toLowerCase(java.util.Locale.ROOT) + '.');
                final long connections = source instanceof TargetWorkerServiceConfiguration.KafkaSource kafka
                        ? kafka.deployment().source().settings().maximumConnections()
                        : 1;
                requireMinimum(
                        cost,
                        connections,
                        source instanceof TargetWorkerServiceConfiguration.KafkaSource ? 1 : 0,
                        1,
                        worker.maximumSourceFrameBytes());
                costs.put(role, cost);
            }
            PulsarClientArtifactOwnedBackendGroup.Settings history = null;
            CapacityVector historyCost = CapacityVector.empty();
            if (source instanceof TargetWorkerServiceConfiguration.PulsarSource pulsar) {
                final var original = pulsar.deployment().source().settings();
                if (original.maximumUsers() < 3) {
                    throw new IllegalArgumentException(
                            "P1 Source settings must cover the three independent history readers");
                }
                history = new PulsarClientArtifactOwnedBackendGroup.Settings(
                        prefix + "/history-client",
                        original.serviceUrl(),
                        original.authenticatedClusterId(),
                        original.ioThreads(),
                        original.listenerThreads(),
                        original.connectionsPerBroker(),
                        original.maximumConnections(),
                        original.maximumUsers(),
                        original.memoryBytes(),
                        original.operationTimeoutMs(),
                        original.maximumLookupRedirects());
                historyCost = fields.capacity("reads.historyClientCost.");
                requireMinimum(
                        historyCost,
                        history.maximumConnections(),
                        Math.addExact(Math.multiplyExact(3L, history.ioThreads()), history.listenerThreads() + 4L),
                        0,
                        history.memoryBytes());
            }
            fields.requireComplete();
            return new Configuration(prefix, costs, history, historyCost, worker);
        }

        public CapacityVector sdkDemand() {
            return demand;
        }

        public int sdkClients() {
            return identities.size();
        }

        public Set<String> allocationIds() {
            return identities;
        }

        public long retainedHeapBytes() {
            return retainedHeapBytes;
        }

        private String id(Read role) {
            return allocationPrefix + '/' + role.name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    private final TargetWorkerServiceConfiguration.Source specification;
    private final Configuration configuration;
    private final TargetDestinationSdkBudget budget;
    private final LocalBrokerMtlsResolver resolver;
    private final SourceTransportCredentialAuthorization.Trust trust;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final Object gate = new Object();
    private final Map<Read, ReadOwner> slots = new EnumMap<>(Read.class);
    private TargetWorkerSourceStartup startup;
    private TargetOwnedBackendGroup sourceBackend;
    private Consumer<SourcePosition> historyProtection;
    private volatile boolean stopping;
    private OwnedCheckpointIo<PulsarClientArtifactOwnedBackendGroup> historyPreparation;
    private CompletableFuture<Void> historyReady;
    private volatile PulsarClientArtifactOwnedBackendGroup historyBackend;
    private volatile LocalBrokerMtlsResolver.SourceReceipt historyCredential;

    private TargetWorkerSourceReadResources(
            TargetWorkerServiceConfiguration.Source specification,
            Configuration configuration,
            TargetDestinationSdkBudget budget,
            LocalBrokerMtlsResolver resolver,
            SourceTransportCredentialAuthorization.Trust trust,
            Supplier<TrustedUtcIntervalEvidence> time) {
        this.specification = Objects.requireNonNull(specification);
        this.configuration = Objects.requireNonNull(configuration);
        this.budget = Objects.requireNonNull(budget);
        this.resolver = Objects.requireNonNull(resolver);
        this.trust = Objects.requireNonNull(trust);
        this.time = Objects.requireNonNull(time);
        if (specification.typedReadResources() != configuration
                || (specification.typedSource() instanceof TargetWorkerServiceConfiguration.PulsarSource)
                        != (configuration.historyClient != null)) {
            throw new IllegalArgumentException("read resources must be the original typed Source declaration");
        }
    }

    /** Pure local construction: the checkpoint composition retains this owner before any I/O/native registration. */
    public static TargetWorkerSourceReadResources create(
            TargetWorkerServiceConfiguration.Source specification,
            Configuration configuration,
            TargetDestinationSdkBudget budget,
            LocalBrokerMtlsResolver resolver,
            SourceTransportCredentialAuthorization.Trust trust,
            Supplier<TrustedUtcIntervalEvidence> time) {
        return new TargetWorkerSourceReadResources(specification, configuration, budget, resolver, trust, time);
    }

    public TargetWorkerCheckpointComposition.Histories histories() {
        return new TargetWorkerCheckpointComposition.Histories(this::control, this::outcome, this::admission, this);
    }

    /** Binds once to the actual Source graph and its original, independently held history-retention guard. */
    public void bindStartup(
            TargetWorkerSourceStartup actual,
            TargetOwnedBackendGroup backend,
            Consumer<SourcePosition> originalHistoryProtection) {
        Objects.requireNonNull(actual);
        Objects.requireNonNull(backend);
        Objects.requireNonNull(originalHistoryProtection);
        budget.requireWorker(actual.graph().classes(), actual.graph().resources());
        backend.requireWorkerGraph(actual.graph().classes(), actual.graph().resources());
        if (configuration.maximumFrameBytes != actual.graph().config().maximumSourceFrameBytes()
                || !configuration.backlog.equals(actual.graph().config().backlogRead())) {
            throw new IllegalArgumentException("read resources changed the original Worker frame/backlog limits");
        }
        if ((specification.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource)
                != (backend instanceof KafkaClientArtifactOwnedSourceBackend)) {
            throw new IllegalArgumentException("read resources changed their actual Source transport");
        }
        final var receipt = Objects.requireNonNull(backend.sourceCredentialReceipt(), "actual Source credential");
        if (!receipt.authorization().shard().equals(specification.scope().shard())) {
            throw new IllegalArgumentException("read resources changed the original Source scope");
        }
        synchronized (gate) {
            if (startup != null || stopping) {
                throw new IllegalStateException("Source read owner is already bound or stopped");
            }
            startup = actual;
            sourceBackend = backend;
            historyProtection = originalHistoryProtection;
        }
    }

    private CompletableFuture<SystemMutation> control(TargetCheckpointDependencies.ControlReference reference) {
        return history(Read.CONTROL, reference.source(), guard -> {
            if (specification.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource kafka) {
                final var source = kafka.deployment().source();
                return KafkaClientArtifactTargetAdmissionHistory.readControlVerified(
                        kafkaConfiguration(source),
                        source.settings().topic(),
                        reference,
                        source.settings().operationTimeout(),
                        guard,
                        historyCredential,
                        time,
                        source.sdkJar(),
                        configuration.maximumFrameBytes);
            }
            final var source = pulsarSource();
            return PulsarClientArtifactTargetAdmissionHistory.readControlVerified(
                    historyBackend,
                    source.guard(),
                    reference,
                    source.receiveTimeout(),
                    guard,
                    historyCredential,
                    time,
                    configuration.maximumFrameBytes);
        });
    }

    private CompletableFuture<SystemMutation> outcome(TargetCheckpointDependencies.OutcomeReference reference) {
        return history(Read.OUTCOME, reference.source(), guard -> {
            if (specification.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource kafka) {
                final var source = kafka.deployment().source();
                return KafkaClientArtifactTargetAdmissionHistory.readOutcomeVerified(
                        kafkaConfiguration(source),
                        source.settings().topic(),
                        reference,
                        source.settings().operationTimeout(),
                        guard,
                        historyCredential,
                        time,
                        source.sdkJar(),
                        configuration.maximumFrameBytes);
            }
            final var source = pulsarSource();
            return PulsarClientArtifactTargetAdmissionHistory.readOutcomeVerified(
                    historyBackend,
                    source.guard(),
                    reference,
                    source.receiveTimeout(),
                    guard,
                    historyCredential,
                    time,
                    configuration.maximumFrameBytes);
        });
    }

    private CompletableFuture<SystemMutation> admission(TargetPublishRecoveryDiscovery.Reference reference) {
        return history(Read.ADMISSION, reference.source(), guard -> {
            if (specification.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource kafka) {
                final var source = kafka.deployment().source();
                return KafkaClientArtifactTargetAdmissionHistory.readVerified(
                        kafkaConfiguration(source),
                        source.settings().topic(),
                        reference,
                        source.settings().operationTimeout(),
                        guard,
                        historyCredential,
                        time,
                        source.sdkJar(),
                        configuration.maximumFrameBytes);
            }
            final var source = pulsarSource();
            return PulsarClientArtifactTargetAdmissionHistory.readVerified(
                    historyBackend,
                    source.guard(),
                    reference,
                    source.receiveTimeout(),
                    guard,
                    historyCredential,
                    time,
                    configuration.maximumFrameBytes);
        });
    }

    private CompletableFuture<SystemMutation> history(
            Read role, SourcePosition position, Function<Runnable, SystemMutation> operation) {
        final HistoryRead read;
        synchronized (gate) {
            requireBound(position);
            reap();
            if (slots.containsKey(role)) {
                throw new IllegalStateException("Source history read slot is still owned");
            }
            read = new HistoryRead(
                    position, operation, budget.reserve(configuration.id(role), configuration.costs.get(role)));
            slots.put(role, read);
            try {
                prepareHistory().whenComplete((ignored, failure) -> {
                    synchronized (gate) {
                        if (failure != null) {
                            read.fail(failure);
                        } else {
                            read.submit();
                        }
                    }
                });
            } catch (RuntimeException | Error failure) {
                read.fail(failure);
            }
        }
        return read.receipt.copy();
    }

    /** Resolves the separate signed HISTORY material only on the existing finite I/O executor. */
    private CompletableFuture<Void> prepareHistory() {
        if (historyReady != null) {
            return historyReady;
        }
        historyReady = new CompletableFuture<>();
        try {
            historyPreparation = OwnedCheckpointIo.submit(
                    current -> {
                        current.run();
                        startup.requireCurrent();
                        final byte[] route = specification.typedSource()
                                        instanceof TargetWorkerServiceConfiguration.KafkaSource kafka
                                ? kafka.deployment().source().signedRoute()
                                : pulsarSource().signedRoute();
                        final var credential = resolver.resolveSource(
                                specification.typedCheckpoint().historyAuthorizationFile(), route, trust, time);
                        credential.requireOperation(
                                SourceTransportCredentialAuthorization.Operation.HISTORY, time.get());
                        credential.requireOperation(
                                SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
                        if (!credential
                                .authorization()
                                .shard()
                                .equals(specification.scope().shard())) {
                            throw new IllegalStateException("HISTORY credential changed the original Source scope");
                        }
                        current.run();
                        startup.requireCurrent();
                        historyCredential = credential;
                        if (configuration.historyClient == null) {
                            return null;
                        }
                        return PulsarClientArtifactOwnedBackendGroup.openVerifiedSourceMtls(
                                configuration.historyClient,
                                pulsarSource().sdkJar(),
                                credential,
                                time,
                                this::requireRemoteCurrent,
                                budget,
                                configuration.historyClientCost,
                                startup.graph().classes(),
                                startup.graph().resources(),
                                startup.graph().io(),
                                actual -> {
                                    synchronized (gate) {
                                        if (historyBackend != null) {
                                            throw new IllegalStateException("history backend already retained");
                                        }
                                        historyBackend = actual;
                                    }
                                });
                    },
                    this::requireRemoteCurrent,
                    startup.graph().io());
        } catch (RuntimeException | Error rejected) {
            historyReady.completeExceptionally(rejected);
            throw rejected;
        }
        historyPreparation.receipt().whenComplete((group, failure) -> {
            if (failure != null) {
                historyReady.completeExceptionally(failure);
                return;
            }
            if (group == null) {
                historyReady.complete(null);
                return;
            }
            group.sourceReadOpeningReceipt().whenComplete((ignored, rejected) -> {
                if (rejected != null) {
                    historyReady.completeExceptionally(rejected);
                    return;
                }
                try {
                    if (!group.finishStartup()) {
                        throw new IllegalStateException("history SDK opening has not actually exited");
                    }
                    historyReady.complete(null);
                } catch (RuntimeException | Error failed) {
                    historyReady.completeExceptionally(failed);
                }
            });
        });
        return historyReady;
    }

    public TargetSourceBacklogRead observe(TargetWorkerShardRuntime worker, TargetSourceBacklog.Cut cut) {
        synchronized (gate) {
            requireBound(cut.applied());
            startup.requireAdmittedWorker(worker);
            reap();
            if (!sourceBackend.workerReady(worker) || slots.containsKey(Read.OBSERVATION)) {
                throw new IllegalStateException("original Source observer slot is unavailable");
            }
            final var operation = new Observation(
                    cut, budget.reserve(configuration.id(Read.OBSERVATION), configuration.costs.get(Read.OBSERVATION)));
            slots.put(Read.OBSERVATION, operation);
            return operation;
        }
    }

    private void requireBound(SourcePosition position) {
        if (stopping
                || startup == null
                || !position.shardId().equals(specification.scope().shard())) {
            throw new IllegalStateException("read owner is stopped, unbound or belongs to another Source");
        }
    }

    private void requireRemoteCurrent() {
        if (stopping) {
            throw new IllegalStateException("Source read owner is stopped");
        }
        startup.requireCurrent();
        if (stopping) {
            throw new IllegalStateException("Source read owner is stopped");
        }
    }

    private PulsarClientArtifactTargetWorkerSourceFactory.SourceConfiguration pulsarSource() {
        return ((TargetWorkerServiceConfiguration.PulsarSource) specification.typedSource())
                .deployment()
                .source();
    }

    private static Map<String, Object> kafkaConfiguration(
            KafkaClientArtifactTargetWorkerSourceFactory.SourceConfiguration source) {
        final var result = new HashMap<String, Object>(source.operational());
        result.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        result.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        result.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
        result.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        return result;
    }

    private interface ReadOwner {
        boolean ioExited();

        Throwable pendingFailure();

        void requestStop();

        void retryCleanup();

        void release();
    }

    private final class HistoryRead implements ReadOwner {
        final SourcePosition position;
        final Function<Runnable, SystemMutation> operation;
        final TargetDestinationSdkBudget.Client allocation;
        final CompletableFuture<SystemMutation> completion = new CompletableFuture<>();
        final CheckpointInputFuture<SystemMutation> receipt =
                CheckpointInputFuture.track(completion, this::requestStop, this::retryCleanup, this::ioExited);
        OwnedCheckpointIo<SystemMutation> attempt;
        boolean stopped, constructionBegun;

        HistoryRead(
                SourcePosition position,
                Function<Runnable, SystemMutation> operation,
                TargetDestinationSdkBudget.Client allocation) {
            this.position = position;
            this.operation = operation;
            this.allocation = allocation;
        }

        void submit() {
            if (stopped || stopping) {
                fail(new IllegalStateException("history input stopped before SDK read"));
                return;
            }
            try {
                attempt = OwnedCheckpointIo.submit(
                        current -> {
                            current.run();
                            allocation.beginConstruction();
                            constructionBegun = true;
                            return operation.apply(current);
                        },
                        () -> {
                            requireRemoteCurrent();
                            historyProtection.accept(position);
                        },
                        startup.graph().io());
                attempt.receipt().whenComplete((value, failure) -> {
                    if (failure == null) {
                        completion.complete(value);
                    } else {
                        completion.completeExceptionally(failure);
                    }
                });
            } catch (RuntimeException | Error failure) {
                fail(failure);
            }
        }

        void fail(Throwable failure) {
            completion.completeExceptionally(failure);
        }

        @Override
        public boolean ioExited() {
            synchronized (gate) {
                return completion.isDone() && (attempt == null || attempt.ioExited());
            }
        }

        @Override
        public Throwable pendingFailure() {
            synchronized (gate) {
                return attempt == null ? null : CheckpointInputFuture.ownedFailure(attempt.receipt());
            }
        }

        @Override
        public void requestStop() {
            synchronized (gate) {
                stopped = true;
                if (attempt != null) {
                    attempt.requestStop();
                } else {
                    fail(new IllegalStateException("history input stopped before SDK read"));
                }
            }
        }

        @Override
        public void retryCleanup() {
            synchronized (gate) {
                if (attempt != null) {
                    attempt.retryCleanup();
                }
            }
        }

        @Override
        public void release() {
            if (constructionBegun) {
                allocation.closed();
            } else {
                allocation.releaseUnstarted();
            }
        }
    }

    private final class Observation implements TargetSourceBacklogRead, ReadOwner {
        final TargetSourceBacklog.Cut cut;
        final TargetDestinationSdkBudget.Client allocation;
        TargetSourceBacklogRead query;
        OwnedCheckpointIo<CompletableFuture<Void>> closing;
        CompletableFuture<Void> closeReceipt;
        Throwable failure;
        boolean stopped, started, readExited, constructionBegun;

        Observation(TargetSourceBacklog.Cut cut, TargetDestinationSdkBudget.Client allocation) {
            this.cut = cut;
            this.allocation = allocation;
        }

        @Override
        public TargetSourceBacklog.Snapshot read() {
            synchronized (gate) {
                if (started || stopped || stopping) {
                    throw new IllegalStateException("observation is already used or stopped");
                }
                started = true;
            }
            try {
                requireRemoteCurrent();
                cut.requireCurrent();
                final Runnable protection = () -> {
                    requireRemoteCurrent();
                    cut.requireCurrent();
                };
                final TargetSourceBacklogRead opened;
                if (specification.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource kafka) {
                    final var source = kafka.deployment().source();
                    opened = KafkaClientArtifactTargetSourceBacklog.openVerified(
                            kafkaConfiguration(source),
                            source.settings().topic(),
                            cut,
                            configuration.backlog,
                            protection,
                            sourceBackend.sourceCredentialReceipt(),
                            time,
                            source.sdkJar());
                } else {
                    opened = PulsarClientArtifactTargetSourceBacklog.openVerified(
                            (PulsarClientArtifactOwnedBackendGroup) sourceBackend,
                            pulsarSource().guard(),
                            cut,
                            configuration.backlog,
                            protection,
                            sourceBackend.sourceCredentialReceipt(),
                            time);
                }
                synchronized (gate) {
                    query = opened;
                    if (stopped || stopping) {
                        throw new IllegalStateException("observation stopped before native construction");
                    }
                    allocation.beginConstruction();
                    constructionBegun = true;
                }
                return opened.read();
            } catch (RuntimeException | Error rejected) {
                synchronized (gate) {
                    failure = rejected;
                }
                throw new TargetSourceBacklogRead.Failure(this, rejected);
            } finally {
                synchronized (gate) {
                    readExited = true;
                }
                requestClose();
            }
        }

        private CompletableFuture<Void> close(boolean retry) {
            synchronized (gate) {
                stopped = true;
                if (!started) {
                    readExited = true;
                }
                if (query == null) {
                    return CompletableFuture.completedFuture(null);
                }
                if (query.cleanupComplete() && (closing == null || closing.ioExited())) {
                    return CompletableFuture.completedFuture(null);
                }
                if (closing != null
                        && (!closing.ioExited()
                                || !retry
                                || closeReceipt != null && !closeReceipt.isDone() && query.cleanupFailure() == null)) {
                    return closeReceipt == null
                            ? closing.receipt().thenCompose(Function.identity())
                            : closeReceipt.copy();
                }
                try {
                    closing = OwnedCheckpointIo.submit(
                            ignored -> retry ? query.retryClose() : query.requestClose(),
                            () -> {},
                            startup.graph().io());
                    closeReceipt = closing.receipt().thenCompose(Function.identity());
                    return closeReceipt.copy();
                } catch (RuntimeException | Error rejected) {
                    failure = rejected;
                    return CompletableFuture.failedFuture(rejected);
                }
            }
        }

        @Override
        public CompletableFuture<Void> requestClose() {
            return close(false);
        }

        @Override
        public CompletableFuture<Void> retryClose() {
            return close(true);
        }

        @Override
        public boolean cleanupComplete() {
            return ioExited();
        }

        @Override
        public Throwable cleanupFailure() {
            return pendingFailure();
        }

        @Override
        public boolean ioExited() {
            synchronized (gate) {
                return readExited
                        && (query == null || query.cleanupComplete())
                        && (closing == null || closing.ioExited())
                        && (closeReceipt == null || closeReceipt.isDone());
            }
        }

        @Override
        public Throwable pendingFailure() {
            synchronized (gate) {
                return query != null && query.cleanupFailure() != null ? query.cleanupFailure() : failure;
            }
        }

        @Override
        public void requestStop() {
            requestClose();
        }

        @Override
        public void retryCleanup() {
            retryClose();
        }

        @Override
        public void release() {
            if (constructionBegun) {
                allocation.closed();
            } else {
                allocation.releaseUnstarted();
            }
        }
    }

    /** Completed slot handles do not accumulate across successive checkpoints/observations. */
    private void reap() {
        slots.entrySet().removeIf(entry -> {
            if (!entry.getValue().ioExited()) {
                return false;
            }
            entry.getValue().release();
            return true;
        });
    }

    @Override
    public void requestStop() {
        synchronized (gate) {
            stopping = true;
            slots.values().forEach(ReadOwner::requestStop);
            if (historyPreparation != null) {
                historyPreparation.requestStop();
            }
            closeIdleHistoryBackend();
            reap();
        }
    }

    @Override
    public void retryCleanup() {
        synchronized (gate) {
            slots.values().forEach(ReadOwner::retryCleanup);
            if (historyPreparation != null) {
                historyPreparation.retryCleanup();
            }
            closeIdleHistoryBackend();
            reap();
        }
    }

    @Override
    public boolean ioExited() {
        synchronized (gate) {
            reap();
            return slots.isEmpty()
                    && (historyPreparation == null || historyPreparation.ioExited())
                    && (historyBackend == null
                            || historyPreparation != null
                                    && historyPreparation.operationExited()
                                    && CheckpointInputFuture.ioExited(historyBackend.sourceReadOpeningReceipt()))
                    && (!stopping || historyBackend == null || historyBackend.resourcesTerminated());
        }
    }

    @Override
    public Throwable pendingIoFailure() {
        synchronized (gate) {
            if (historyPreparation != null) {
                final var failure = CheckpointInputFuture.ownedFailure(historyPreparation.receipt());
                if (failure != null) {
                    return failure;
                }
            }
            if (historyBackend != null && historyBackend.cleanupFailure() != null) {
                return historyBackend.cleanupFailure();
            }
            for (var operation : slots.values()) {
                if (operation.pendingFailure() != null) {
                    return operation.pendingFailure();
                }
            }
            return null;
        }
    }

    @Override
    public void close() {
        requestStop();
        retryCleanup();
    }

    /** Stop requests fence each user first; only its real method exit permits shared SDK teardown. */
    private void closeIdleHistoryBackend() {
        if (!stopping
                || historyBackend == null
                || historyPreparation == null
                || !historyPreparation.operationExited()) {
            return;
        }
        for (var read : slots.values()) {
            if (read instanceof HistoryRead history && history.attempt != null && !history.attempt.operationExited()) {
                return;
            }
        }
        historyBackend.closeBeforeHostBinding();
    }

    private static void requireSdkCost(CapacityVector cost) {
        for (var dimension : CapacityDimension.values()) {
            if (cost.amount(dimension) == Long.MAX_VALUE
                    || !SDK_DIMENSIONS.contains(dimension) && cost.amount(dimension) != 0) {
                throw new IllegalArgumentException(
                        "read SDK resource declaration is unsupported or unbounded: " + dimension);
            }
        }
    }

    private static void requireMinimum(CapacityVector cost, long connections, long threads, long messages, long bytes) {
        requireSdkCost(cost);
        final var minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.OPEN_FILES.wireValue() - 1] = connections;
        minimum[CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = connections;
        minimum[CapacityDimension.ADAPTER_THREADS.wireValue() - 1] = threads;
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = messages;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = bytes;
        if (!cost.covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException(
                    "read SDK cost does not cover the actual client, cursor and Source frame");
        }
    }
}
