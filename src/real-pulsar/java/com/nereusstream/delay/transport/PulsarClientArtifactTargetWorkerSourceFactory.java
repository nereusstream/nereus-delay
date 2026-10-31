package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetSourceRecoveryInputs;
import com.nereusstream.delay.ownership.TargetWorkerShardFactory;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

/** Opens the configured guarded P1 Source and binds its actual activated Target Worker. */
public final class PulsarClientArtifactTargetWorkerSourceFactory {
    private PulsarClientArtifactTargetWorkerSourceFactory() {}

    /** Immutable deployment settings; transport credentials are resolved on the original startup call. */
    public record SourceConfiguration(PulsarClientArtifactOwnedBackendGroup.Settings settings, byte[] sdkJar,
            Path authorizationFile, byte[] signedRoute, TopicResourceGuard guard, String topic, String subscription,
            Duration receiveTimeout, byte[] deploymentId, byte[] workerRunId, long maximumRecoveryInputBytes) {
        public SourceConfiguration {
            Objects.requireNonNull(settings); Objects.requireNonNull(guard);
            Bytes.requireLength(sdkJar, 32, "P1 SDK artifact hash");
            if (authorizationFile == null || !authorizationFile.isAbsolute()
                    || signedRoute == null || signedRoute.length == 0 || signedRoute.length > 256 * 1024
                    || topic == null || topic.isBlank() || topic.length() > 1024
                    || subscription == null || subscription.isBlank() || subscription.length() > 512
                    || receiveTimeout == null || receiveTimeout.toMillis() < 1 || receiveTimeout.toMillis() > 60_000
                    || deploymentId == null || deploymentId.length == 0 || deploymentId.length > 512
                    || workerRunId == null || workerRunId.length == 0 || workerRunId.length > 512
                    || maximumRecoveryInputBytes < 1 || maximumRecoveryInputBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("configured P1 Source requires bounded original deployment settings");
            }
            sdkJar = Bytes.copy(sdkJar); signedRoute = Bytes.copy(signedRoute);
            deploymentId = Bytes.copy(deploymentId); workerRunId = Bytes.copy(workerRunId);
            authorizationFile = authorizationFile.normalize();
        }
        @Override public byte[] sdkJar() { return Bytes.copy(sdkJar); }
        @Override public byte[] signedRoute() { return Bytes.copy(signedRoute); }
        @Override public byte[] deploymentId() { return Bytes.copy(deploymentId); }
        @Override public byte[] workerRunId() { return Bytes.copy(workerRunId); }
    }

    public record SystemWriterConfiguration(String allocationId, Duration timeout, Path authorizationFile, byte[] signedRoute,
            com.nereusstream.delay.protocol.CapacityVector cost) {
        public SystemWriterConfiguration {
            Objects.requireNonNull(cost);
            if (allocationId == null || allocationId.isBlank() || allocationId.length() > 512
                    || timeout == null || timeout.toMillis() < 1 || timeout.toMillis() > 60_000
                    || authorizationFile == null || !authorizationFile.isAbsolute() || signedRoute == null
                    || signedRoute.length == 0 || signedRoute.length > 256 * 1024) {
                throw new IllegalArgumentException("P1 Source writer requires finite original allocation/time/authorization settings");
            }
            authorizationFile = authorizationFile.normalize(); signedRoute = Bytes.copy(signedRoute);
        }
        @Override public byte[] signedRoute() { return Bytes.copy(signedRoute); }
    }

    public record SourceDeployment(SourceConfiguration source, com.nereusstream.delay.protocol.CapacityVector cost) {
        public SourceDeployment { Objects.requireNonNull(source); Objects.requireNonNull(cost); }
    }

    public static SourceDeployment loadSourceDeployment(Path path) throws java.io.IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final String cluster = f.text("client.clusterId");
        final var settings = new PulsarClientArtifactOwnedBackendGroup.Settings(f.text("client.allocationId"),
                f.text("client.serviceUrl"), cluster, f.integer("client.ioThreads"), f.integer("client.listenerThreads"),
                f.integer("client.connectionsPerBroker"), f.integer("client.maximumConnections"),
                f.integer("client.maximumUsers"), f.number("client.memoryBytes"), f.integer("client.operationTimeoutMs"),
                f.integer("client.maximumLookupRedirects"));
        final var guard = new TopicResourceGuard(cluster, f.hex("source.resourceIncarnation", 32, 32),
                f.unsigned64("source.physicalTopicCreationTimestamp"));
        final var source = new SourceConfiguration(settings, f.hex("sdk.sha256", 32, 32),
                f.absolutePath("source.authorizationFile"), f.image("source.signedRouteFile", 256 * 1024), guard,
                f.text("source.topic"), f.text("source.subscription"), Duration.ofMillis(f.number("source.receiveTimeoutMs")),
                f.hex("source.deploymentId", 1, 512), f.hex("source.workerRunId", 1, 512),
                f.number("recovery.maximumInputBytes"));
        final var result = new SourceDeployment(source, f.capacity("source.cost."));
        f.requireComplete(); return result;
    }

    public static SystemWriterConfiguration loadSystemWriterConfiguration(Path path) throws java.io.IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var result = new SystemWriterConfiguration(f.text("writer.allocationId"),
                Duration.ofMillis(f.number("writer.timeoutMs")), f.absolutePath("writer.authorizationFile"),
                f.image("writer.signedRouteFile", 256 * 1024), f.capacity("writer.cost."));
        f.requireComplete(); return result;
    }

    public static Opening openConfiguredSource(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            Path path, LocalBrokerMtlsResolver resolver, SourceTransportCredentialAuthorization.Trust trust,
            TargetSourceRecoveryInputs.Provider recoveryProvider,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget,
            TargetWorkerShardRuntime.Maintenance maintenance, java.util.function.LongSupplier clock) throws java.io.IOException {
        final var deployment = loadSourceDeployment(path);
        return openConfiguredSource(startup, deployment.source(), resolver, trust, recoveryProvider, time,
                budget, deployment.cost(), maintenance, clock);
    }

    /** Resolves actual mTLS material and derives the Registry Owner from this root's acquired lease. */
    public static Opening openConfiguredSource(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            SourceConfiguration configuration, LocalBrokerMtlsResolver resolver,
            SourceTransportCredentialAuthorization.Trust trust, TargetSourceRecoveryInputs.Provider recoveryProvider,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, com.nereusstream.delay.protocol.CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, java.util.function.LongSupplier clock) {
        Objects.requireNonNull(configuration); Objects.requireNonNull(resolver); Objects.requireNonNull(trust);
        Objects.requireNonNull(recoveryProvider); Objects.requireNonNull(time);
        try (var original = startup.beginPulsarClientOpening()) {
            original.requireCurrent();
            if (configuration.maximumRecoveryInputBytes() > startup.graph().config().resources().certifiedJvmHeapBytes()) {
                throw new IllegalArgumentException("configured recovery input bytes exceed the Worker heap bound");
            }
            // Bounded file reads, PKIX and signature verification run without the Store/startup monitor.
            final var receipt = resolver.resolveSource(configuration.authorizationFile(), configuration.signedRoute(), trust, time);
            original.requireCurrent();
            final var lease = startup.ownerLease();
            final var identity = new com.nereusstream.delay.protocol.OwnerIdentity(configuration.deploymentId(),
                    configuration.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
            final var opening = openVerifiedSource(original, startup, configuration.settings(), configuration.sdkJar(), receipt,
                    configuration.guard(), configuration.topic(), configuration.subscription(), configuration.receiveTimeout(),
                    time, budget, cost, maintenance, identity, clock);
            opening.configureRecoveryProvider(recoveryProvider, configuration.maximumRecoveryInputBytes());
            original.requireCurrent();
            return opening;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    /** Completes this process root's actual Source startup and retains its original SDK owner on failure. */
    public static TargetWorkerShardRuntime createVerifiedSource(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard guard, String topic, Duration receiveTimeout,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        startup.registerBackend(group);
        try (var original = startup.beginWorkerConstruction()) {
            original.requireCurrent();
            final var graph = startup.graph();
            final var worker = createVerifiedSource(group, consumer, guard, topic, receiveTimeout,
                    startup.activation(), graph.classes(), startup.store(), graph.resources(), maintenance, identity, time, clock);
            startup.admitWorker(worker);
            return worker;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    /** Allocates the actual verified client and later receipt-enabled consumer on the original process graph. */
    public static Opening openVerifiedSource(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            PulsarClientArtifactOwnedBackendGroup.Settings settings, byte[] sdkJar,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
            TopicResourceGuard guard, String topic, String subscription, Duration receiveTimeout,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, com.nereusstream.delay.protocol.CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.LongSupplier clock) {
        try (var original = startup.beginPulsarClientOpening()) {
            return openVerifiedSource(original, startup, settings, sdkJar, receipt, guard, topic, subscription,
                    receiveTimeout, time, budget, cost, maintenance, identity, clock);
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    private static Opening openVerifiedSource(com.nereusstream.delay.ownership.TargetWorkerSourceStartup.Construction original,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            PulsarClientArtifactOwnedBackendGroup.Settings settings, byte[] sdkJar, LocalBrokerMtlsResolver.SourceReceipt receipt,
            TopicResourceGuard guard, String topic, String subscription, Duration receiveTimeout,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, com.nereusstream.delay.protocol.CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.LongSupplier clock) {
        original.requireCurrent();
        startup.requireCurrent();
        Objects.requireNonNull(maintenance); Objects.requireNonNull(identity); Objects.requireNonNull(time); Objects.requireNonNull(clock);
        final var assignment = startup.acceptedAssignment();
        final var lease = startup.ownerLease();
        receipt.authorization().requireAssignment(assignment);
        if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)
                || !Bytes.constantTimeEquals(identity.deploymentId(), receipt.authorization().deploymentId())
                || identity.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(identity.leaseFencingDigest(), lease.leaseToken())
                || !assignment.shardId().equals(receipt.authorization().shard()) || assignment.shardId().partition() < 0
                || !topic.equals(barrier.physicalTopic())
                || !Arrays.equals(guard.resourceIncarnation(), barrier.brokerResourceIncarnation())
                || receiveTimeout == null || receiveTimeout.toMillis() < 1 || receiveTimeout.toMillis() > 60_000) {
            throw new IllegalArgumentException("P1 SDK opening changes its original accepted Source resource/time bound");
        }
        final var authorization = receipt.authorization();
        final var resource = authorization.resource().pulsar();
        if (!guard.authenticatedClusterId().equals(resource.authenticatedClusterId())
                || guard.topicCreationTimestamp() != resource.physicalTopicCreationTimestamp()) {
            throw new IllegalArgumentException("P1 Source guard differs from the signed deployment resource");
        }
        authorization.requireName(SourceTransportCredentialAuthorization.Operation.CONSUME, subscription);
        final var now = time.get();
        for (var role : java.util.List.of(SourceTransportCredentialAuthorization.Operation.CONSUME,
                SourceTransportCredentialAuthorization.Operation.ACK, SourceTransportCredentialAuthorization.Operation.LOOKUP)) {
            receipt.requireOperation(role, now);
        }
        startup.bindSourceIdentity(receipt, identity, now);
        final var graph = startup.graph();
        final var group = PulsarClientArtifactOwnedBackendGroup.openVerifiedSourceMtls(settings, sdkJar, receipt,
                time, startup::requirePulsarOpeningAuthority, budget, cost, graph.classes(), graph.resources(), graph.io());
        startup.registerBackend(group);
        startup.requireCurrent();
        return new Opening(startup, group, receipt, guard, topic, subscription, receiveTimeout, maintenance, identity, time, clock, budget);
    }

    public static final class Opening {
        private final com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup;
        private final PulsarClientArtifactOwnedBackendGroup group;
        private final com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt;
        private final TopicResourceGuard guard;
        private final String topic;
        private final String subscription;
        private final Duration timeout;
        private final TargetWorkerShardRuntime.Maintenance maintenance;
        private final com.nereusstream.delay.protocol.OwnerIdentity identity;
        private final java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time;
        private final java.util.function.LongSupplier clock;
        private final com.nereusstream.delay.ownership.TargetDestinationSdkBudget sdkBudget;
        private PulsarClientArtifactOwnedSystemWriter systemWriter;
        private TargetWorkerShardRuntime worker;
        private com.nereusstream.delay.ownership.TargetWorkerSourceStartup.PulsarPreparation preparation;
        private GuardedConsumer<byte[]> consumer;
        private PulsarClientArtifactSourceRecordConsumer preparedSource;
        private com.nereusstream.delay.ownership.TargetSourceRecoveryInputs.Provider recoveryProvider;
        private long maximumRecoveryInputBytes;
        private com.nereusstream.delay.ownership.TargetWorkerSourceStartup.RecoveryInputsOpening recoveryInputs;
        private PulsarClientArtifactRecoverySourceCursor migrationHistoryReader;
        private com.nereusstream.delay.ownership.TargetWorkerRecoverySource payloadSummaryReader;
        private boolean failed;
        private Opening(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
                PulsarClientArtifactOwnedBackendGroup group, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
                TopicResourceGuard guard, String topic, String subscription, Duration timeout,
                TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
                java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
                java.util.function.LongSupplier clock, com.nereusstream.delay.ownership.TargetDestinationSdkBudget sdkBudget) {
            this.startup = startup; this.group = group; this.receipt = receipt; this.guard = guard;
            this.sdkBudget = sdkBudget;
            this.topic = topic; this.subscription = subscription; this.timeout = timeout;
            this.maintenance = maintenance; this.identity = identity; this.time = time; this.clock = clock;
        }

        /** Opens the actual Target producer on this original Source client's graph/material. */
        public com.nereusstream.delay.ownership.TargetDestinationChannelRuntime openPublicationChannel(Path configuration,
                byte[] channelDigest, com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch,
                LocalBrokerMtlsResolver resolver, com.nereusstream.delay.runtime.ProfileCatalog profiles,
                com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
                java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) throws java.io.IOException {
            return PulsarClientArtifactTargetProducerFactory.openConfigured(startup, group, dispatch, channelDigest,
                    configuration, resolver, profiles, trust, time, reads, sdkBudget);
        }

        /** Reuses the final actually positioned consumer and its original shared client/material after Source recovery. */
        public void openSystemWriter(Path path, LocalBrokerMtlsResolver resolver,
                SourceTransportCredentialAuthorization.Trust trust) throws java.io.IOException {
            openSystemWriter(loadSystemWriterConfiguration(path), resolver, trust);
        }

        public synchronized void openSystemWriter(SystemWriterConfiguration configuration, LocalBrokerMtlsResolver resolver,
                SourceTransportCredentialAuthorization.Trust trust) {
            if (failed || worker == null || systemWriter != null || consumer == null || preparedSource == null) {
                throw new IllegalStateException("original P1 Source writer requires its admitted Worker/final consumer and one configuration");
            }
            try (var construction = startup.beginWorkerResourceConstruction(worker)) {
                construction.requireCurrent();
                final var graph = startup.graph();
                final var writerReceipt = Objects.requireNonNull(resolver).resolveSource(configuration.authorizationFile(),
                        configuration.signedRoute(), Objects.requireNonNull(trust), time);
                construction.requireCurrent();
                final var actualOwner = startup.currentPulsarOwnerIdentity();
                writerReceipt.authorization().requireAssignment(startup.acceptedAssignment());
                if (!Bytes.constantTimeEquals(actualOwner.deploymentId(), writerReceipt.authorization().deploymentId())) {
                    throw new IllegalArgumentException("P1 system writer authorization changes the original deployment");
                }
                writerReceipt.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
                writerReceipt.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
                systemWriter = PulsarClientArtifactOwnedSystemWriter.open(configuration.allocationId(), group, consumer,
                        graph.config(), worker, startup.acceptedAssignment(), actualOwner, writerReceipt,
                        configuration.timeout(), time, clock, sdkBudget, configuration.cost(), startup.systemWriterBudget(),
                        graph.classes(), graph.resources(), graph.io(), startup.scopeExposureIssuer(worker));
                construction.requireCurrent();
            } catch (RuntimeException | Error failure) { failed = true; startup.failResourceStartup(failure); throw failure; }
        }

        public synchronized java.util.Optional<PulsarClientArtifactOwnedSystemWriter> advanceSystemWriter() {
            if (failed || systemWriter == null) throw new IllegalStateException("original P1 Source writer was not opened");
            try {
                if (!systemWriter.finishStartup()) return java.util.Optional.empty();
                startup.requireAdmittedWorker(worker);
                return java.util.Optional.of(systemWriter);
            } catch (RuntimeException | Error failure) { failed = true; startup.failResourceStartup(failure); throw failure; }
        }

        /** Installs one original Native GC owner on the existing Host maintenance tick; no new executor. */
        public com.nereusstream.delay.ownership.TargetResourceReclaimMaintenance configureResourceReclaimMaintenance(
                com.nereusstream.delay.ownership.TargetResourceReclaimMaintenance.Configuration configuration,
                com.nereusstream.delay.ownership.TargetResourceReclaimMaintenance.InputProvider provider) {
            final TargetWorkerShardRuntime actual; final PulsarClientArtifactOwnedSystemWriter writer;
            synchronized (this) {
                if (failed || worker == null || systemWriter == null) throw new IllegalStateException("original P1 Source writer is unavailable");
                actual = worker; writer = systemWriter;
            }
            return startup.configureResourceReclaimMaintenance(actual, writer, configuration, provider, time, clock);
        }
        /** Bounded actual Native candidates on this admitted P1 Source; no prediction of future Source positions. */
        public com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Page discoverResourceReclaim(
                com.nereusstream.delay.store.BoundedReadBudget reads,
                com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Cursor cursor,int maximumRows){
            startup.requireCurrent();final TargetWorkerShardRuntime actual;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted P1 Source Worker is unavailable");actual=worker;}
            final var page=actual.discoverResourceReclaim(reads,cursor,maximumRows,clock);startup.requireCurrent();return page;
        }
        /** Caller owns this operation and its complete reservation on the original process bounded I/O pool. */
        public com.nereusstream.delay.ownership.TargetResourceReclaimSigner.Prepared prepareConfiguredResourceReclaim(
                java.util.List<com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate> candidates,Path configuration,byte[] actualRoot,
                java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
                com.nereusstream.delay.store.BoundedReadBudget keyReads,com.nereusstream.delay.store.BoundedReadBudget recoveryReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads,long retryUntil,long maximumInputBytes,Runnable originalHeld)throws java.io.IOException{
            startup.requireCurrent();
            if(maximumInputBytes>startup.graph().config().resources().certifiedJvmHeapBytes())
                throw new IllegalArgumentException("Native signing input reservation exceeds original process heap envelope");
            final TargetWorkerShardRuntime actual;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted P1 Source Worker is unavailable");actual=worker;}
            final Runnable held=()->{startup.requireCurrent();originalHeld.run();};
            final var prepared=actual.prepareConfiguredResourceReclaim(candidates,configuration,actualRoot,images,keyReads,recoveryReads,nativeReads,
                    time,retryUntil,maximumInputBytes,held,clock);
            held.run();return prepared;
        }
        public com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission submitResourceReclaim(
                com.nereusstream.delay.ownership.TargetResourceReclaimSigner.Prepared prepared,
                java.util.function.Consumer<com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission> retained){
            startup.requireCurrent();final TargetWorkerShardRuntime actual;final PulsarClientArtifactOwnedSystemWriter writer;
            synchronized(this){
                if(failed||worker==null||systemWriter==null)throw new IllegalStateException("original P1 Source writer is unavailable");actual=worker;writer=systemWriter;
            }
            return actual.submitResourceReclaim(prepared,writer,clock,retained);
        }
        public java.util.Optional<com.nereusstream.delay.runtime.SystemMutationResult> settleResourceReclaim(
                com.nereusstream.delay.ownership.TargetResourceReclaimSigner.Prepared prepared){
            startup.requireCurrent();final TargetWorkerShardRuntime actual;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted P1 Source Worker is unavailable");actual=worker;}
            return actual.settleResourceReclaim(prepared.mutation(),clock);
        }
        /** Trusted composition-root provider; no reflected classes/scripts or preconstructed Source runtime. */
        public synchronized void configureRecoveryProvider(
                com.nereusstream.delay.ownership.TargetSourceRecoveryInputs.Provider provider, long maximumRetainedBytes) {
            if (failed || worker != null || recoveryProvider != null || recoveryInputs != null
                    || maximumRetainedBytes < 1 || maximumRetainedBytes == Long.MAX_VALUE
                    || maximumRetainedBytes > startup.graph().config().resources().certifiedJvmHeapBytes()) {
                throw new IllegalStateException("recovery provider requires one original finite startup configuration");
            }
            recoveryProvider = Objects.requireNonNull(provider); maximumRecoveryInputBytes = maximumRetainedBytes;
        }
        synchronized PulsarClientArtifactOwnedBackendGroup actualBackend() {
            if (worker == null || failed) throw new IllegalStateException("original P1 Source Worker is unavailable");
            return group;
        }

        /** Explicit driver turn; subscribe/native proof waits retain their original finite SDK bounds. */
        public synchronized java.util.Optional<TargetWorkerShardRuntime> advance() {
            if (failed) throw new IllegalStateException("original P1 Source opening failed");
            if (worker != null) return java.util.Optional.of(worker);
            try {
                startup.requirePulsarOpeningAuthority();
                if (!group.finishStartup()) return java.util.Optional.empty();
                if (recoveryProvider != null && startup.needsPulsarRecovery()) {
                    final var owner = preparation == null ? identity : startup.currentPulsarOwnerIdentity();
                    if (recoveryInputs == null || !Arrays.equals(recoveryInputs.owner().canonicalBytes(), owner.canonicalBytes())) {
                        recoveryInputs = startup.openRecoveryInputs(recoveryProvider, owner, maximumRecoveryInputBytes);
                    }
                    final var ready = recoveryInputs.ready();
                    if (ready.isEmpty()) return java.util.Optional.empty();
                    final var inputs = ready.orElseThrow();
                    if (!startRecovery(inputs.credential(), inputs.maximumFrameBytes(), inputs::requireSourceCurrent,
                            inputs.journal(), inputs.journal() == null ? null : inputs::requireJournalCurrent)) {
                        return java.util.Optional.empty();
                    }
                }
                if (startup.pulsarRecoveryPending()) startup.runRecoveryTurn();
                if (preparation == null) {
                    final var current = startup.preparePulsarSource();
                    if (current.isEmpty()) return java.util.Optional.empty();
                    preparation = current.orElseThrow();
                }
                if (preparedSource == null) {
                    try (var original = startup.beginPulsarConsumerOpening(preparation)) {
                        original.requireCurrent();
                        consumer = PulsarClientArtifactSourceConsumerFactory.createVerifiedSource(
                                group, receipt, guard, topic, subscription, time);
                        preparedSource = PulsarClientArtifactSourceRecordConsumer.prepareTarget(consumer, guard,
                                preparation.assignment().shardId(), topic, timeout, preparation);
                        original.requireCurrent(); receipt.requireActive(time.get());
                    }
                }
                if (!preparation.advanceOwner(preparedSource) || !startup.activationReady()) return java.util.Optional.empty();
                if (!startup.prepareServiceJournalHandoff()) return java.util.Optional.empty();
                try (var original = startup.beginWorkerConstruction()) {
                    original.requireCurrent();
                    final var graph = startup.graph();
                    final var activation = startup.activation();
                    requireCurrentGuardProof(consumer, guard, (PulsarActivationBarrier) activation.assignment().activationBarrier(),
                            topic, activation.assignment().shardId().partition());
                    worker = TargetWorkerShardFactory.create(preparedSource, activation.assignment(), graph.classes(),
                            startup.store(), graph.resources(), activation.runtime(), maintenance);
                    group.bindSourceCredential(consumer, worker, activation.assignment(), startup.currentPulsarOwnerIdentity(), time, clock);
                    worker.bindActualSourceSuccessor(activation);
                    startup.admitWorker(worker);
                }
                return java.util.Optional.of(worker);
            } catch (org.apache.pulsar.client.api.PulsarClientException | RuntimeException | Error failure) {
                failed = true; startup.retain(failure);
                if (failure instanceof RuntimeException runtime) throw runtime;
                if (failure instanceof Error fatal) throw fatal;
                throw new IllegalStateException("original P1 Source subscribe failed", failure);
            }
        }

        /** Explicit provider phase before initial preparation and again under the newly acquired successor Owner. */
        public synchronized boolean startRecovery(
                com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt recoveryCredential,
                int maximumFrameBytes, Runnable sourceProtection,
                com.nereusstream.delay.adapter.PulsarAttemptJournal journal, Runnable journalProtection) {
            if (failed || worker != null) throw new IllegalStateException("original P1 opening no longer accepts recovery");
            try {
                if (!group.finishStartup() || !startup.needsPulsarRecovery()) return false;
                if (migrationHistoryReader != null) {
                    if (startup.advanceMigrationHistory(migrationHistoryReader)) migrationHistoryReader = null;
                    return false;
                }
                if (startup.migrationHistoryRequired()) {
                    migrationHistoryReader = PulsarClientArtifactRecoverySourceCursor.openForStartup(startup, group, guard,
                            recoveryCredential, timeout, maximumFrameBytes, time, sourceProtection);
                    return false;
                }
                if (payloadSummaryReader == null) payloadSummaryReader = PulsarClientArtifactRecoverySourceCursor.openForStartup(startup, group, guard,
                        recoveryCredential, timeout, maximumFrameBytes, time, sourceProtection);
                final var preparedReader = startup.payloadSummaryRecoverySource(payloadSummaryReader);
                if (preparedReader.isEmpty()) {
                    if (startup.payloadSummaryCaptureReaderClosed(payloadSummaryReader)) payloadSummaryReader = null;
                    return false;
                }
                final var owner = preparation == null ? identity : startup.currentPulsarOwnerIdentity();
                startup.startRecovery(preparedReader.orElseThrow(), owner, journal, sourceProtection, journalProtection);
                payloadSummaryReader = null;
                return true;
            } catch (RuntimeException | Error failure) { failed = true; startup.retain(failure); throw failure; }
        }
    }

    /** Actual service path consumes only the repository's privately constructed Source activation. */
    public static TargetWorkerShardRuntime createVerifiedSource(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard guard, String topic, Duration receiveTimeout,
            com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Activated activation,
            WorkClassExecutionRegistry classes, ShardStore store, SharedRocksDbResources resources,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        activation.requireGraph(classes, resources, store);
        try { return createVerifiedSource(group, consumer, guard, topic, receiveTimeout, activation.assignment(),
                classes, store, resources, activation.runtime(), maintenance, identity, time, clock);
        } catch (RuntimeException | Error failure) {
            throw com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.sourceConstructionFailed(activation, failure);
        }
    }

    public static TargetWorkerShardRuntime createVerifiedSource(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard expectedGuard, String physicalTopic, Duration receiveTimeout,
            SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses, ShardStore store,
            SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime, TargetWorkerShardRuntime.Maintenance maintenance,
            com.nereusstream.delay.protocol.OwnerIdentity owner,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        final var worker = create(group, consumer, expectedGuard, physicalTopic, receiveTimeout, acceptedAssignment,
                workClasses, store, resources, sourceRuntime, maintenance);
        try { group.bindSourceCredential(consumer, worker, acceptedAssignment, owner, time, clock); return worker; }
        catch (RuntimeException | Error failure) {
            try { PulsarClientArtifactOwnedBackendGroup.closeConsumer(consumer).get(group.operationTimeoutMs(), java.util.concurrent.TimeUnit.MILLISECONDS); }
            catch (Exception cleanup) { if (cleanup instanceof InterruptedException) Thread.currentThread().interrupt(); failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Reject an unrelated/shared consumer before handing an actual owned group into the Worker graph. */
    public static TargetWorkerShardRuntime create(PulsarClientArtifactOwnedBackendGroup group,
            GuardedConsumer<byte[]> consumer, TopicResourceGuard expectedGuard, String physicalTopic,
            Duration receiveTimeout, SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses,
            ShardStore store, SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime,
            TargetWorkerShardRuntime.Maintenance maintenance) {
        Objects.requireNonNull(group, "actual P1 group").requireWorkerGraph(workClasses, resources);
        if (PulsarClientArtifactOwnedBackendGroup.findNative(consumer) != group)
            throw new IllegalArgumentException("P1 Source consumer is not owned by this actual backend group");
        return create(consumer, expectedGuard, physicalTopic, receiveTimeout, acceptedAssignment, workClasses,
                store, resources, sourceRuntime, maintenance);
    }

    /**
     * Creates one Target Worker source for the exact accepted Pulsar assignment. The caller must complete Store
     * bootstrap/reopen and Owner activation first; this factory never creates or authorizes a Target root.
     */
    public static TargetWorkerShardRuntime create(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final Duration receiveTimeout,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime sourceRuntime,
            final TargetWorkerShardRuntime.Maintenance maintenance) {
        final var exactConsumer = Objects.requireNonNull(consumer, "consumer");
        final var guard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        final var topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        final var assignment = Objects.requireNonNull(acceptedAssignment, "acceptedAssignment");
        final var runtime = Objects.requireNonNull(sourceRuntime, "sourceRuntime");
        runtime.requireAcceptedAssignment(assignment);
        if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)) {
            throw new IllegalArgumentException("Pulsar Target Worker requires a Pulsar activation barrier");
        }
        if (assignment.shardId().partition() < 0) {
            throw new IllegalArgumentException("Pulsar Target Worker partition must be non-negative");
        }
        if (!topic.equals(barrier.physicalTopic())
                || !Arrays.equals(barrier.brokerResourceIncarnation(), guard.resourceIncarnation())) {
            throw new IllegalArgumentException("Pulsar Target Worker differs from the activation resource identity");
        }
        requireCurrentGuardProof(exactConsumer, guard, barrier, topic, assignment.shardId().partition());

        final PulsarClientArtifactSourceRecordConsumer source;
        try {
            source = PulsarClientArtifactSourceRecordConsumer.forTarget(
                    exactConsumer, guard, assignment.shardId(), topic, Objects.requireNonNull(receiveTimeout));
        } catch (RuntimeException | Error failure) {
            closeConsumerAfterFailure(exactConsumer, failure);
            throw failure;
        }
        return TargetWorkerShardFactory.create(
                source, assignment, workClasses, store, resources, runtime, maintenance);
    }

    private static void requireCurrentGuardProof(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final PulsarActivationBarrier barrier,
            final String physicalTopic,
            final int partition) {
        if (!expectedGuard.equals(consumer.resourceGuard()) || !physicalTopic.equals(consumer.getTopic())) {
            throw new IllegalArgumentException("Pulsar Target Worker consumer is bound to a different topic");
        }
        if (consumer.connectionGeneration() != barrier.guardedSourceConnectionGeneration()) {
            throw new IllegalArgumentException("Pulsar Target Worker connection differs from activation barrier");
        }
        final TopicResourceGuardAttestation attestation = consumer.resourceGuardAttestation()
                .orElseThrow(() -> new IllegalStateException("Pulsar Target Worker has no current guard proof"));
        if (!expectedGuard.authenticatedClusterId().equals(attestation.authenticatedClusterId())
                || !Arrays.equals(expectedGuard.resourceIncarnation(), attestation.resourceIncarnation())
                || !physicalTopic.equals(attestation.physicalTopic())
                || attestation.partition() != partition
                || !Bytes.constantTimeEquals(
                        barrier.resourceGuardAttestationDigest(),
                        PulsarClientArtifactSourceRecordConsumer.attestationDigest(attestation))) {
            throw new IllegalArgumentException("Pulsar Target Worker proof differs from activation barrier");
        }
    }

    private static void closeConsumerAfterFailure(final GuardedConsumer<byte[]> consumer, final Throwable failure) {
        try {
            consumer.close();
        } catch (Exception | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
