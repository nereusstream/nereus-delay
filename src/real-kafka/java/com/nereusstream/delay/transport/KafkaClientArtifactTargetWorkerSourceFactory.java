package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.ownership.TargetSourceRecoveryInputs;
import com.nereusstream.delay.ownership.TargetWorkerShardFactory;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerResourceGuard;
import org.apache.kafka.clients.consumer.GuardedConsumer;
import org.apache.kafka.common.TopicPartition;

/** Opens the configured guarded K1 Source and binds its actual recovered/activated Target Worker. */
public final class KafkaClientArtifactTargetWorkerSourceFactory {
    private KafkaClientArtifactTargetWorkerSourceFactory() {}

    public record RecoveryReader(String allocationId, Duration pollTimeout, int maximumConnections, CapacityVector cost) {
        public RecoveryReader {
            Objects.requireNonNull(cost);
            for (var dimension : com.nereusstream.delay.protocol.CapacityDimension.values()) {
                if (cost.amount(dimension) == Long.MAX_VALUE) throw new IllegalArgumentException("K1 recovery cost must be finite");
            }
            if (allocationId == null || allocationId.isBlank() || allocationId.length() > 512
                    || pollTimeout == null || pollTimeout.toMillis() < 1 || pollTimeout.toMillis() > 60_000
                    || maximumConnections < 1 || maximumConnections == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("K1 recovery reader requires finite original SDK settings");
            }
        }
    }

    public record SystemWriterConfiguration(KafkaClientArtifactOwnedSystemWriter.Settings settings,
            Map<String,String> operational, Path authorizationFile, byte[] signedRoute, CapacityVector cost) {
        public SystemWriterConfiguration {
            Objects.requireNonNull(settings); Objects.requireNonNull(cost);
            if (authorizationFile == null || !authorizationFile.isAbsolute() || signedRoute == null
                    || signedRoute.length == 0 || signedRoute.length > 256 * 1024) {
                throw new IllegalArgumentException("K1 Source writer requires bounded original authorization/Route input");
            }
            authorizationFile = authorizationFile.normalize(); signedRoute = Bytes.copy(signedRoute);
            operational = literalOperational(operational);
        }
        @Override public byte[] signedRoute() { return Bytes.copy(signedRoute); }
    }

    private static Map<String,String> literalOperational(Map<String,String> operational) {
        Objects.requireNonNull(operational);
        if (operational.size() > 64) throw new IllegalArgumentException("K1 operational field bound exceeded");
        operational.forEach((key, value) -> {
            if (key == null || key.isBlank() || key.length() > 128 || value == null || value.length() > 4096) {
                throw new IllegalArgumentException("K1 operational settings exceed their finite literal bound");
            }
        });
        return Map.copyOf(operational);
    }

    /** Immutable deployment values; no caller credential/Owner runtime is accepted by the configured entry. */
    public record SourceConfiguration(KafkaClientArtifactOwnedSourceBackend.Settings settings,
            Map<String, String> operational, byte[] sdkJar, Path authorizationFile, byte[] signedRoute,
            byte[] deploymentId, byte[] workerRunId, long maximumRecoveryInputBytes, RecoveryReader recoveryReader) {
        public SourceConfiguration {
            Objects.requireNonNull(settings); Objects.requireNonNull(recoveryReader); Objects.requireNonNull(operational);
            Bytes.requireLength(sdkJar, 32, "K1 SDK artifact hash");
            Bytes.requireLength(deploymentId, 32, "signed Source deployment identity");
            if (operational.size() > 64 || authorizationFile == null || !authorizationFile.isAbsolute()
                    || signedRoute == null || signedRoute.length == 0 || signedRoute.length > 256 * 1024
                    || workerRunId == null || workerRunId.length == 0 || workerRunId.length > 512
                    || maximumRecoveryInputBytes < 1 || maximumRecoveryInputBytes == Long.MAX_VALUE
                    || recoveryReader.allocationId().equals(settings.allocationId())) {
                throw new IllegalArgumentException("configured K1 Source requires bounded original deployment values");
            }
            operational = literalOperational(operational); sdkJar = Bytes.copy(sdkJar); signedRoute = Bytes.copy(signedRoute);
            deploymentId = Bytes.copy(deploymentId); workerRunId = Bytes.copy(workerRunId);
            authorizationFile = authorizationFile.normalize();
        }
        @Override public byte[] sdkJar() { return Bytes.copy(sdkJar); }
        @Override public byte[] signedRoute() { return Bytes.copy(signedRoute); }
        @Override public byte[] deploymentId() { return Bytes.copy(deploymentId); }
        @Override public byte[] workerRunId() { return Bytes.copy(workerRunId); }
    }

    public record SourceDeployment(SourceConfiguration source, CapacityVector cost) {
        public SourceDeployment { Objects.requireNonNull(source); Objects.requireNonNull(cost); }
    }

    /** Literal configuration only; the original Root supplies the accepted Shard and all live authority. */
    public static SourceDeployment loadSourceDeployment(Path path, com.nereusstream.delay.protocol.ShardId shard)
            throws java.io.IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var settings = new KafkaClientArtifactOwnedSourceBackend.Settings(f.text("source.allocationId"),
                f.text("source.clusterId"), java.util.UUID.fromString(f.text("source.topicId")), Objects.requireNonNull(shard),
                f.text("source.topic"), Duration.ofMillis(f.number("source.pollTimeoutMs")),
                Duration.ofMillis(f.number("source.operationTimeoutMs")), f.integer("source.maximumConnections"),
                f.integer("source.fetchBytes"));
        final var recovery = new RecoveryReader(f.text("recovery.allocationId"),
                Duration.ofMillis(f.number("recovery.pollTimeoutMs")), f.integer("recovery.maximumConnections"),
                f.capacity("recovery.cost."));
        final var source = new SourceConfiguration(settings, f.prefixed("kafka.", 64), f.hex("sdk.sha256", 32, 32),
                f.absolutePath("source.authorizationFile"), f.image("source.signedRouteFile", 256 * 1024),
                f.hex("source.deploymentId", 32, 32), f.hex("source.workerRunId", 1, 512),
                f.number("recovery.maximumInputBytes"), recovery);
        final var result = new SourceDeployment(source, f.capacity("source.cost."));
        f.requireComplete(); return result;
    }

    public static SystemWriterConfiguration loadSystemWriterConfiguration(Path path) throws java.io.IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var settings = new KafkaClientArtifactOwnedSystemWriter.Settings(f.text("writer.allocationId"),
                f.integer("writer.maximumBrokers"), f.number("writer.bufferBytes"), f.integer("writer.socketBytes"),
                Duration.ofMillis(f.number("writer.timeoutMs")));
        final var result = new SystemWriterConfiguration(settings, f.prefixed("kafka.", 64),
                f.absolutePath("writer.authorizationFile"), f.image("writer.signedRouteFile", 256 * 1024),
                f.capacity("writer.cost."));
        f.requireComplete(); return result;
    }

    /** File-backed entry reaches the same actual Source factory with no caller-supplied runtime or Owner. */
    public static Opening openConfiguredSource(TargetWorkerSourceStartup startup, Path path,
            LocalBrokerMtlsResolver resolver, SourceTransportCredentialAuthorization.Trust trust,
            TargetSourceRecoveryInputs.Provider recoveryProvider,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget,
            TargetWorkerShardRuntime.Maintenance maintenance, java.util.function.LongSupplier clock) throws java.io.IOException {
        final var deployment = loadSourceDeployment(path, startup.acceptedAssignment().shardId());
        return openConfiguredSource(startup, deployment.source(), resolver, trust, recoveryProvider, time,
                budget, deployment.cost(), maintenance, clock);
    }

    /** Resolves actual signed Source-use/mTLS material under the original Root lease and construction call. */
    public static Opening openConfiguredSource(TargetWorkerSourceStartup startup, SourceConfiguration configuration,
            LocalBrokerMtlsResolver resolver, SourceTransportCredentialAuthorization.Trust trust,
            TargetSourceRecoveryInputs.Provider recoveryProvider,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, java.util.function.LongSupplier clock) {
        Objects.requireNonNull(configuration); Objects.requireNonNull(resolver); Objects.requireNonNull(trust);
        Objects.requireNonNull(recoveryProvider); Objects.requireNonNull(time);
        try (var original = startup.beginKafkaClientOpening()) {
            original.requireCurrent();
            if (configuration.maximumRecoveryInputBytes() > startup.graph().config().resources().certifiedJvmHeapBytes()) {
                throw new IllegalArgumentException("configured recovery input bytes exceed the Worker heap bound");
            }
            final var receipt = resolver.resolveSource(configuration.authorizationFile(), configuration.signedRoute(), trust, time);
            original.requireCurrent();
            final var lease = startup.ownerLease();
            final var identity = new com.nereusstream.delay.protocol.OwnerIdentity(configuration.deploymentId(),
                    configuration.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
            final Map<String,Object> operational = new java.util.HashMap<>(configuration.operational());
            final var opening = openVerifiedSource(original, startup, configuration.settings(), operational,
                    configuration.sdkJar(), receipt, time, budget, cost, maintenance, identity, clock);
            opening.configureRecoveryProvider(recoveryProvider, configuration.maximumRecoveryInputBytes(),
                    configuration.recoveryReader(), operational, configuration.sdkJar(), budget);
            original.requireCurrent();
            return opening;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    /** Completes this process root's actual Source startup and retains its original SDK owner on failure. */
    public static TargetWorkerShardRuntime createVerifiedSource(KafkaClientArtifactOwnedSourceBackend owner,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        startup.registerBackend(owner);
        try (var original = startup.beginWorkerConstruction()) {
            original.requireCurrent();
            final var graph = startup.graph();
            final var worker = createVerifiedSource(owner, startup.activation(), graph.classes(), startup.store(),
                    graph.resources(), maintenance, identity, time, clock);
            startup.admitWorker(worker);
            return worker;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    /** Allocates the actual verified SDK Source on this startup's original finite process pool. */
    public static Opening openVerifiedSource(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
            KafkaClientArtifactOwnedSourceBackend.Settings settings, java.util.Map<String, Object> operational,
            byte[] sdkJar, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, com.nereusstream.delay.protocol.CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.LongSupplier clock) {
        try (var original = startup.beginKafkaClientOpening()) {
            return openVerifiedSource(original, startup, settings, operational, sdkJar, receipt, time,
                    budget, cost, maintenance, identity, clock);
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    private static Opening openVerifiedSource(TargetWorkerSourceStartup.Construction original,
            TargetWorkerSourceStartup startup, KafkaClientArtifactOwnedSourceBackend.Settings settings,
            Map<String,Object> operational, byte[] sdkJar, LocalBrokerMtlsResolver.SourceReceipt receipt,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget, CapacityVector cost,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.LongSupplier clock) {
        original.requireCurrent(); startup.requireCurrent();
        Objects.requireNonNull(maintenance); Objects.requireNonNull(identity); Objects.requireNonNull(time); Objects.requireNonNull(clock);
        final var assignment = startup.acceptedAssignment();
        final var lease = startup.ownerLease();
        receipt.authorization().requireAssignment(assignment);
        if (!(assignment.activationBarrier() instanceof KafkaActivationBarrier barrier)
                || !Bytes.constantTimeEquals(identity.deploymentId(), receipt.authorization().deploymentId())
                || identity.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(identity.leaseFencingDigest(), lease.leaseToken())
                || !assignment.shardId().equals(settings.shard()) || assignment.shardId().partition() < 0
                || !barrier.authenticatedClusterId().equals(settings.authenticatedClusterId())
                || !barrier.nativeTopicUuid().equals(settings.topicId())) {
            throw new IllegalArgumentException("K1 SDK opening changes its original accepted Source resource/Owner");
        }
        final var now = time.get();
        for (var role : java.util.List.of(SourceTransportCredentialAuthorization.Operation.CONSUME,
                SourceTransportCredentialAuthorization.Operation.ACK, SourceTransportCredentialAuthorization.Operation.LOOKUP)) {
            receipt.requireOperation(role, now);
        }
        startup.bindSourceIdentity(receipt, identity, now);
        final var graph = startup.graph();
        final var owner = KafkaClientArtifactOwnedSourceBackend.openVerifiedSourceMtls(settings, operational, sdkJar,
                receipt, time, startup::requireCurrent, budget, cost, graph.classes(), graph.resources(), graph.io());
        startup.registerBackend(owner);
        original.requireCurrent();
        return new Opening(startup, owner, maintenance, identity, time, clock, sdkJar, budget);
    }

    public static final class Opening {
        private final com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup;
        private final KafkaClientArtifactOwnedSourceBackend owner;
        private final TargetWorkerShardRuntime.Maintenance maintenance;
        private final com.nereusstream.delay.protocol.OwnerIdentity identity;
        private final java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time;
        private final java.util.function.LongSupplier clock;
        private final byte[] sdkJar;
        private final com.nereusstream.delay.ownership.TargetDestinationSdkBudget sdkBudget;
        private KafkaClientArtifactOwnedSystemWriter systemWriter;
        private TargetWorkerShardRuntime worker;
        private boolean failed;
        private TargetSourceRecoveryInputs.Provider recoveryProvider;
        private long maximumRecoveryInputBytes;
        private RecoveryReader recoveryReader;
        private Map<String,Object> recoveryOperational;
        private byte[] recoverySdkJar;
        private com.nereusstream.delay.ownership.TargetDestinationSdkBudget recoveryBudget;
        private TargetWorkerSourceStartup.RecoveryInputsOpening recoveryInputs;
        private Opening(com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup,
                KafkaClientArtifactOwnedSourceBackend owner, TargetWorkerShardRuntime.Maintenance maintenance,
                com.nereusstream.delay.protocol.OwnerIdentity identity,
                java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
                java.util.function.LongSupplier clock, byte[] sdkJar,
                com.nereusstream.delay.ownership.TargetDestinationSdkBudget sdkBudget) {
            this.startup = startup; this.owner = owner; this.maintenance = maintenance;
            this.sdkJar = Bytes.copy(sdkJar); this.sdkBudget = sdkBudget;
            this.identity = identity; this.time = time; this.clock = clock;
        }
        private void configureRecoveryProvider(TargetSourceRecoveryInputs.Provider provider, long maximumRetainedBytes,
                RecoveryReader reader, Map<String,Object> operational, byte[] sdkJar,
                com.nereusstream.delay.ownership.TargetDestinationSdkBudget budget) {
            if (recoveryProvider != null || recoveryInputs != null || worker != null || failed) {
                throw new IllegalStateException("K1 recovery preparation cannot replace its original configuration");
            }
            recoveryProvider = Objects.requireNonNull(provider); maximumRecoveryInputBytes = maximumRetainedBytes;
            recoveryReader = Objects.requireNonNull(reader); recoveryOperational = Map.copyOf(operational);
            recoverySdkJar = Bytes.copy(sdkJar); recoveryBudget = Objects.requireNonNull(budget);
        }

        public com.nereusstream.delay.ownership.TargetDestinationChannelRuntime openBaselineChannel(Path configuration,
                byte[] channelDigest, com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch,
                LocalBrokerMtlsResolver resolver, com.nereusstream.delay.runtime.ProfileCatalog profiles,
                com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
                java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) throws java.io.IOException {
            return KafkaClientArtifactTargetBaselineFactory.openConfigured(startup, dispatch, channelDigest, configuration,
                    resolver, profiles, trust, time, reads, sdkBudget);
        }

        public KafkaClientArtifactTargetReceiptFactory.Opening openReceiptChannel(Path configuration, byte[] channelDigest,
                com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch,
                KafkaClientArtifactTargetReceiptFactory.RecoveryAdmission originalAdmission, LocalBrokerMtlsResolver resolver,
                com.nereusstream.delay.runtime.ProfileCatalog profiles,
                com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
                KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher,
                java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) throws java.io.IOException {
            final TargetWorkerShardRuntime original;
            synchronized (this) {
                if (failed || worker == null) throw new IllegalStateException("original K1 Source Worker is unavailable");
                original = worker;
            }
            return KafkaClientArtifactTargetReceiptFactory.open(startup, original, dispatch, channelDigest, configuration,
                    originalAdmission, resolver, profiles, trust, catalog, publisher, sdkBudget, time, reads, clock);
        }

        /** Uses the ready privileged backing retained by the same process; no Admin or CONTROL key reaches this Worker. */
        public KafkaClientArtifactTargetReceiptFactory.Opening openReceiptChannel(Path configuration, byte[] channelDigest,
                com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch,
                KafkaClientArtifactTargetReceiptFactory.RecoveryAdmission originalAdmission, LocalBrokerMtlsResolver resolver,
                com.nereusstream.delay.runtime.ProfileCatalog profiles,
                com.nereusstream.delay.runtime.CredentialAttestationTrustSet trust,
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) throws java.io.IOException {
            startup.requireReceiptManagement(management);
            final var config = KafkaClientArtifactTargetReceiptFactory.load(configuration);
            if (!java.util.Arrays.equals(config.protectionDigest(), management.protection(channelDigest).digest()))
                throw new IllegalArgumentException("configured K2 protection differs from the same process's original backing");
            final TargetWorkerShardRuntime original;
            synchronized (this) {
                if (failed || worker == null) throw new IllegalStateException("original K1 Source Worker is unavailable");
                original = worker;
            }
            return KafkaClientArtifactTargetReceiptFactory.openLoaded(startup, original, dispatch, channelDigest, config,
                    originalAdmission, resolver, profiles, trust, management.catalog(), management.publisher(channelDigest),
                    sdkBudget, time, reads, clock);
        }

        /** Uses independent signed Target recovery purpose and the same process's ready CONTROL backing. */
        public KafkaClientArtifactTargetRecoveryRuntime openReceiptRecovery(Path configuration, byte[] channelDigest,
                KafkaClientArtifactTargetReceiptFactory.RecoveryAdmission originalAdmission, Path authorization,
                LocalBrokerMtlsResolver resolver, com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization.Trust recoveryTrust,
                com.nereusstream.delay.runtime.ProfileCatalog profiles,
                com.nereusstream.delay.runtime.CredentialAttestationTrustSet historicalTrust,
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) throws java.io.IOException {
            startup.requireReceiptManagement(management);
            final var config=KafkaClientArtifactTargetReceiptFactory.load(configuration);
            if(!java.util.Arrays.equals(config.protectionDigest(),management.protection(channelDigest).digest()))
                throw new IllegalArgumentException("K2 recovery protection differs from the original process backing");
            final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original K1 Source Worker is unavailable");original=worker;}
            return KafkaClientArtifactTargetRecoveryRuntime.create(startup,original,channelDigest,config,originalAdmission,
                    authorization,resolver,recoveryTrust,profiles,historicalTrust,management.catalog(),sdkBudget,time,reads,clock);
        }

        /** Actual Root archive preparation; no purpose/service key is returned in the Worker result. */
        public java.util.concurrent.CompletableFuture<com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.Prepared> prepareReceiptRecoveryResolution(
                KafkaClientArtifactTargetRecoveryRuntime recovery,KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                KafkaClientArtifactTargetRecoveryProofIssuer issuer,com.nereusstream.delay.ownership.TargetRecoveryProofTrust proofTrust,
                com.nereusstream.delay.ownership.TargetPublishOutcomeMutationFactory sourceSigner,
                KafkaClientArtifactTargetRecoveryRuntime.ResolutionContext contexts){
            startup.requireReceiptManagement(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original K1 Source Worker is unavailable");original=worker;}
            final var inputs=recovery.reopenInputs(startup,original);
            if(inputs.catalog()!=management.catalog()||!java.util.Arrays.equals(inputs.config().protectionDigest(),management.protection(recovery.channelIdentity().digest()).digest()))
                throw new IllegalArgumentException("K2 archive changed the actual same-process CONTROL backing");
            return recovery.prepareArchivedResolution(issuer,proofTrust,sourceSigner,contexts);
        }

        /** Installs into existing publication recovery configuration; other rows and initial UNKNOWN holds use original history. */
        public com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.History receiptRecoveryHistory(
                KafkaClientArtifactTargetRecoveryRuntime recovery,KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                int maximumTurns,KafkaClientArtifactTargetRecoveryProofIssuer issuer,
                com.nereusstream.delay.ownership.TargetRecoveryProofTrust proofTrust,
                com.nereusstream.delay.ownership.TargetPublishOutcomeMutationFactory sourceSigner,
                KafkaClientArtifactTargetRecoveryRuntime.ResolutionContext contexts,
                com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance.History originalHistory){
            startup.requireReceiptManagement(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original K1 Source Worker is unavailable");original=worker;}
            final var inputs=recovery.reopenInputs(startup,original);
            if(inputs.catalog()!=management.catalog()||maximumTurns<1||!java.util.Arrays.equals(inputs.config().protectionDigest(),management.protection(recovery.channelIdentity().digest()).digest()))
                throw new IllegalArgumentException("K2 recovery history changed actual Root/protection/bound");
            java.util.Objects.requireNonNull(originalHistory);java.util.Objects.requireNonNull(issuer);java.util.Objects.requireNonNull(proofTrust);
            java.util.Objects.requireNonNull(sourceSigner);java.util.Objects.requireNonNull(contexts);
            return reference->{
                if(reference.attempt().phase()!=com.nereusstream.delay.protocol.TargetQuotaAttemptBudget.Phase.UNKNOWN
                        ||!java.util.Arrays.equals(reference.attempt().publishAttemptId(),inputs.mapping().admission().publishAttemptId()))
                    return originalHistory.load(reference);
                startup.requireReceiptManagement(management);reference.requireImage(inputs.admission(),reference.source());
                return recovery.recoverArchivedResolution(maximumTurns,issuer,proofTrust,sourceSigner,contexts);
            };
        }

        /** Live admitted checkpoint collection also reconciles post-crash APPLIED proofs; bootstrap image audit uses history-only authority. */
        public com.nereusstream.delay.store.TargetCheckpointDependencies.RecoveryEvidenceAuthority reconcilingReceiptRecoveryEvidenceAuthority(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.ownership.TargetRecoveryProofTrust proofTrust,
                java.util.function.Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads){
            startup.requireReceiptManagement(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            java.util.Objects.requireNonNull(proofTrust);java.util.Objects.requireNonNull(reads);
            return (owner,input)->{
                startup.requireReceiptManagementRead(management);
                final var budget=java.util.Objects.requireNonNull(reads.get(),"bounded original reconciliation reads");
                if(!budget.beforeRead()||!budget.tryCharge(com.nereusstream.delay.protocol.Bytes.utf8(owner.reference().exactKey()).length,
                        Math.toIntExact(owner.reference().canonicalValueBytes())))throw budget.incomplete();
                final var actual=management.catalog().retainedRecovery(owner,input.image(),proofTrust);
                final var applied=original.readRecoveryEvidenceAdoption(budget,actual,input.image(),clock);
                management.catalog().adoptRecovery(applied);startup.requireReceiptManagementRead(management);
                return actual;
            };
        }

        /** Actual same-process Catalog and admitted Native cut, kept inside the management owner's I/O operation. */
        com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses readClosedRecoveryUses(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var actualClosure=management.catalog().readReferenceClosure(closure,catalogReads,current);
            final var actual=original.readClosedRecoveryUses(nativeReads,actualClosure,applied,clock);
            management.catalog().readReferenceClosure(actualClosure,catalogReads,current);
            startup.requireReceiptManagementRead(management);current.run();return actual;
        }
        com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ControlClosure readControlReferenceClosure(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ControlReferenceSnapshot snapshot,
                com.nereusstream.delay.store.BoundedReadBudget reads,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var actual=original.readControlReferenceClosure(reads,snapshot,clock);
            startup.requireReceiptManagementRead(management);current.run();return actual;
        }
        com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses readClosedControlUses(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption applied,int kind,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads,Runnable current){
            final var owner=kind==com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.PROTECTION?applied.protection()
                    :kind==com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact.ANCHOR?applied.anchor():null;
            if(owner==null)throw new IllegalArgumentException("CONTROL selection requires protection or anchor");
            return readClosedControlUses(management,closure,owner,catalogReads,nativeReads,current);
        }
        com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses readClosedControlUses(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.protocol.TargetExternalEvidenceOwner owner,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var actualClosure=management.catalog().readReferenceClosure(closure,catalogReads,current);
            final var actual=original.readClosedControlUses(nativeReads,actualClosure,owner,clock);
            management.catalog().readReferenceClosure(actualClosure,catalogReads,current);
            startup.requireReceiptManagementRead(management);current.run();return actual;
        }
        com.nereusstream.delay.runtime.TargetStagingEvidenceStore.ClosedUses readClosedStagingUses(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var actualClosure=management.catalog().readReferenceClosure(closure,catalogReads,current);
            final var actual=original.readClosedStagingUses(nativeReads,actualClosure,clock);
            management.catalog().readReferenceClosure(actualClosure,catalogReads,current);
            startup.requireReceiptManagementRead(management);current.run();return actual;
        }
        com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent readAppliedRetirementIntent(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,com.nereusstream.delay.protocol.SystemMutation selected,
                com.nereusstream.delay.store.BoundedReadBudget reads,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var actual=original.readAppliedRetirementIntent(reads,selected,clock);
            startup.requireReceiptManagementRead(management);current.run();return actual;
        }
        void settleRetiredMarkerCapacity(KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.protocol.SystemMutation selected,
                com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,com.nereusstream.delay.store.BoundedReadBudget nativeReads,Runnable current){
            requireRetirementReadCurrent(management,current);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            management.catalog().settleRetiredMarkerCapacity(startup,original,management,selected,admission,catalogReads,nativeReads,current);
        }
        /** Bounded actual Native candidates on this admitted K1 Source; no prediction of future Source positions. */
        public com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Page discoverResourceReclaim(
                com.nereusstream.delay.store.BoundedReadBudget reads,
                com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Cursor cursor,int maximumRows){
            startup.requireCurrent();final TargetWorkerShardRuntime actual;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted K1 Source Worker is unavailable");actual=worker;}
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
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted K1 Source Worker is unavailable");actual=worker;}
            final Runnable held=()->{startup.requireCurrent();originalHeld.run();};
            final var prepared=actual.prepareConfiguredResourceReclaim(candidates,configuration,actualRoot,images,keyReads,recoveryReads,nativeReads,
                    time,retryUntil,maximumInputBytes,held,clock);
            held.run();return prepared;
        }
        public com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission submitResourceReclaim(
                com.nereusstream.delay.ownership.TargetResourceReclaimSigner.Prepared prepared,
                java.util.function.Consumer<com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission> retained){
            startup.requireCurrent();final TargetWorkerShardRuntime actual;final KafkaClientArtifactOwnedSystemWriter writer;
            synchronized(this){
                if(failed||worker==null||systemWriter==null)throw new IllegalStateException("original K1 Source writer is unavailable");actual=worker;writer=systemWriter;
            }
            return actual.submitResourceReclaim(prepared,writer,clock,retained);
        }
        public java.util.Optional<com.nereusstream.delay.runtime.SystemMutationResult> settleResourceReclaim(
                com.nereusstream.delay.ownership.TargetResourceReclaimSigner.Prepared prepared){
            startup.requireCurrent();final TargetWorkerShardRuntime actual;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted K1 Source Worker is unavailable");actual=worker;}
            return actual.settleResourceReclaim(prepared.mutation(),clock);
        }
        void requireRetirementReadCurrent(KafkaClientArtifactTargetReceiptManagementFactory.Opening management,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            original.requireRetirementReadCurrent(clock);startup.requireReceiptManagementRead(management);current.run();
        }
        com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.PayloadRetirementProtection preparePayloadRetirementProtection(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.protocol.SystemMutation selected,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ReservedMarker reserved,
                com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission,
                java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,com.nereusstream.delay.store.BoundedReadBudget nativeReads,
                com.nereusstream.delay.store.BoundedReadBudget recoveryReads,Runnable current){
            requireRetirementReadCurrent(management,current);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            return management.catalog().preparePayloadRetirementProtection(startup,original,management,selected,reserved,admission,
                    images,catalogReads,nativeReads,recoveryReads,current);
        }
        java.util.Optional<com.nereusstream.delay.runtime.SystemMutationResult> retirementMutationResult(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.protocol.SystemMutation mutation,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var result=original.retirementMutationResult(mutation,clock);
            startup.requireReceiptManagementRead(management);current.run();return result;
        }
        com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence retirementSigningTime(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource,Runnable current){
            requireRetirementReadCurrent(management,current);
            final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            original.requireRetirementProtocolActive(resource,clock);
            final var observed=Objects.requireNonNull(time.get(),"original Source time provider");
            original.requireRetirementProtocolActive(resource,clock);requireRetirementReadCurrent(management,current);return observed;
        }
        com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission submitRetirementIntent(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.protocol.SystemMutation image,
                com.nereusstream.delay.ownership.OwnedSourceSystemWriter writer,Runnable protection,
                java.util.function.Consumer<com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission> retained){
            startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            return original.submitRetirementIntent(image,writer,protection,clock,retained);
        }
        java.util.Optional<com.nereusstream.delay.runtime.SystemMutationResult> settleRetirementIntent(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,
                com.nereusstream.delay.protocol.SystemMutation image,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var first=original.settleRetirementIntent(image,clock);
            startup.requireReceiptManagementRead(management);current.run();return first;
        }
        com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission submitRetirementConfirmation(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,com.nereusstream.delay.protocol.SystemMutation image,
                com.nereusstream.delay.ownership.OwnedSourceSystemWriter writer,Runnable protection,
                java.util.function.Consumer<com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission> retained){
            startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            return original.submitRetirementConfirmation(image,writer,protection,clock,retained);
        }
        java.util.Optional<com.nereusstream.delay.runtime.SystemMutationResult> settleRetirementConfirmation(
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management,com.nereusstream.delay.protocol.SystemMutation image,Runnable current){
            current.run();startup.requireReceiptManagementRead(management);final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original admitted Source Worker is unavailable");original=worker;}
            final var first=original.settleRetirementConfirmation(image,clock);startup.requireReceiptManagementRead(management);current.run();return first;
        }

        /** Distinct normal-work continuation; the independent recovery entry never returns a SEND resource. */
        public KafkaClientArtifactTargetReceiptFactory.RecoveryReopen configureReceiptRecoveryReopen(
                KafkaClientArtifactTargetRecoveryRuntime recovery,
                com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch dispatch,
                KafkaClientArtifactTargetReceiptManagementFactory.Opening management) {
            startup.requireReceiptManagement(management);
            final TargetWorkerShardRuntime original;
            synchronized(this){if(failed||worker==null)throw new IllegalStateException("original K1 Source Worker is unavailable");original=worker;}
            final var inputs=recovery.reopenInputs(startup,original);
            if(inputs.catalog()!=management.catalog()||!java.util.Arrays.equals(inputs.config().protectionDigest(),
                    management.protection(recovery.channelIdentity().digest()).digest()))
                throw new IllegalArgumentException("K2 resume changed its original process CONTROL backing");
            return KafkaClientArtifactTargetReceiptFactory.configureRecoveryReopen(startup,original,recovery,dispatch,
                    management.publisher(recovery.channelIdentity().digest()));
        }

        /** Opens the Source producer against this exact Root/Worker/material, before original Host composition. */
        public void openSystemWriter(Path path, LocalBrokerMtlsResolver resolver,
                SourceTransportCredentialAuthorization.Trust trust) throws java.io.IOException {
            openSystemWriter(loadSystemWriterConfiguration(path), resolver, trust);
        }

        public synchronized void openSystemWriter(SystemWriterConfiguration configuration, LocalBrokerMtlsResolver resolver,
                SourceTransportCredentialAuthorization.Trust trust) {
            if (failed || worker == null || systemWriter != null) {
                throw new IllegalStateException("original K1 Source writer requires its admitted Worker and one configuration");
            }
            try (var construction = startup.beginWorkerResourceConstruction(worker)) {
                construction.requireCurrent();
                final var graph = startup.graph();
                final var lease = startup.ownerLease();
                final var actualOwner = new com.nereusstream.delay.protocol.OwnerIdentity(identity.deploymentId(),
                        identity.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
                final var receipt = Objects.requireNonNull(resolver).resolveSource(configuration.authorizationFile(),
                        configuration.signedRoute(), Objects.requireNonNull(trust), time);
                construction.requireCurrent();
                receipt.authorization().requireAssignment(startup.acceptedAssignment());
                if (!Bytes.constantTimeEquals(actualOwner.deploymentId(), receipt.authorization().deploymentId())) {
                    throw new IllegalArgumentException("K1 system writer authorization changes the original deployment");
                }
                receipt.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
                receipt.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
                final Map<String,Object> operational = new java.util.HashMap<>(configuration.operational());
                systemWriter = KafkaClientArtifactOwnedSystemWriter.open(configuration.settings(), operational, sdkJar,
                        graph.config(), worker, startup.acceptedAssignment(), actualOwner, receipt, time, clock,
                        sdkBudget, configuration.cost(), startup.systemWriterBudget(), graph.classes(), graph.resources(), graph.io());
                construction.requireCurrent();
            } catch (RuntimeException | Error failure) { failed = true; startup.failResourceStartup(failure); throw failure; }
        }

        /** Observes the original producer opening, retaining its failure even if partial native cleanup is pending. */
        public synchronized java.util.Optional<KafkaClientArtifactOwnedSystemWriter> advanceSystemWriter() {
            if (failed || systemWriter == null) throw new IllegalStateException("original K1 Source writer was not opened");
            try {
                if (!systemWriter.finishStartup()) return java.util.Optional.empty();
                startup.requireAdmittedWorker(worker);
                return java.util.Optional.of(systemWriter);
            } catch (RuntimeException | Error failure) { failed = true; startup.failResourceStartup(failure); throw failure; }
        }

        /** Explicit driver turn; positioning/credential binding use the existing finite SDK timeout. */
        public synchronized java.util.Optional<TargetWorkerShardRuntime> advance() {
            if (failed) throw new IllegalStateException("original K1 Source opening failed");
            if (worker != null) return java.util.Optional.of(worker);
            try {
                startup.requireCurrent();
                if (!owner.finishStartup()) return java.util.Optional.empty();
                if (recoveryProvider != null && startup.needsKafkaRecovery()) {
                    if (recoveryInputs == null) {
                        recoveryInputs = startup.openRecoveryInputs(recoveryProvider, identity, maximumRecoveryInputBytes);
                    }
                    final var ready = recoveryInputs.ready();
                    if (ready.isEmpty()) return java.util.Optional.empty();
                    final var inputs = ready.orElseThrow();
                    final var reader = KafkaClientArtifactRecoverySourceCursor.openForStartup(startup, recoveryOperational,
                            recoverySdkJar, inputs.credential(), recoveryReader.pollTimeout(), inputs.maximumFrameBytes(),
                            time, inputs::requireSourceCurrent, recoveryBudget, recoveryReader.allocationId(),
                            recoveryReader.maximumConnections(), recoveryReader.cost());
                    startup.startRecovery(reader, identity, inputs.journal(), inputs::requireSourceCurrent,
                            inputs.journal() == null ? null : inputs::requireJournalCurrent);
                }
                if (startup.kafkaRecoveryPending()) startup.runRecoveryTurn();
                if (!startup.activationReady()) return java.util.Optional.empty();
                if (!startup.prepareServiceJournalHandoff()) return java.util.Optional.empty();
                worker = createVerifiedSource(owner, startup, maintenance, identity, time, clock);
                return java.util.Optional.of(worker);
            } catch (RuntimeException | Error failure) { failed = true; startup.retain(failure); throw failure; }
        }
    }

    /** Actual service path consumes only the repository's privately constructed Source activation. */
    public static TargetWorkerShardRuntime createVerifiedSource(KafkaClientArtifactOwnedSourceBackend owner,
            com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Activated activation,
            WorkClassExecutionRegistry classes, ShardStore store, SharedRocksDbResources resources,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        activation.requireGraph(classes, resources, store);
        try {
            final var worker = createVerifiedSource(owner, activation.assignment(), classes, store, resources, activation.runtime(),
                    maintenance, identity, time, clock);
            worker.bindActualSourceSuccessor(activation);
            return worker;
        } catch (RuntimeException | Error failure) {
            throw com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.sourceConstructionFailed(activation, failure);
        }
    }

    public static TargetWorkerShardRuntime createVerifiedSource(KafkaClientArtifactOwnedSourceBackend owner,
            SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses, ShardStore store,
            SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime,
            TargetWorkerShardRuntime.Maintenance maintenance, com.nereusstream.delay.protocol.OwnerIdentity identity,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        final var worker = create(owner, acceptedAssignment, workClasses, store, resources, sourceRuntime, maintenance);
        try { owner.bindSourceCredential(worker, acceptedAssignment, identity, time, clock); return worker; }
        catch (RuntimeException | Error failure) {
            try { owner.source().close(); }
            catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    /** Actual owned Source startup uses the same native instance for positioning, Worker composition and teardown. */
    public static TargetWorkerShardRuntime create(KafkaClientArtifactOwnedSourceBackend owner,
            SourceAssignment acceptedAssignment, WorkClassExecutionRegistry workClasses, ShardStore store,
            SharedRocksDbResources resources, TargetSourceApplyRuntime sourceRuntime,
            TargetWorkerShardRuntime.Maintenance maintenance) {
        Objects.requireNonNull(sourceRuntime, "sourceRuntime").requireAcceptedAssignment(acceptedAssignment);
        Objects.requireNonNull(owner, "actual K1 Source owner").requireWorkerGraph(workClasses, resources);
        owner.positionAcceptedAssignment(acceptedAssignment, store.appliedShardLogPosition());
        return TargetWorkerShardFactory.create(owner.source(), acceptedAssignment, workClasses, store,
                resources, sourceRuntime, maintenance);
    }

    /**
     * Creates one Target Worker source for the exact accepted Kafka assignment. The caller must complete Store
     * bootstrap/reopen and Owner activation first; this factory never creates or authorizes a Target root.
     */
    public static TargetWorkerShardRuntime create(
            final GuardedConsumer<byte[], byte[]> consumer,
            final String physicalTopic,
            final Duration pollTimeout,
            final SourceAssignment acceptedAssignment,
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime sourceRuntime,
            final TargetWorkerShardRuntime.Maintenance maintenance) {
        final var exactConsumer = Objects.requireNonNull(consumer, "consumer");
        final var assignment = Objects.requireNonNull(acceptedAssignment, "acceptedAssignment");
        final var runtime = Objects.requireNonNull(sourceRuntime, "sourceRuntime");
        runtime.requireAcceptedAssignment(assignment);
        final String topic = requirePhysicalTopic(physicalTopic);
        if (!(assignment.activationBarrier() instanceof KafkaActivationBarrier barrier)) {
            throw new IllegalArgumentException("Kafka Target Worker requires a Kafka activation barrier");
        }
        if (barrier.exclusiveOffset() < 0 || assignment.shardId().partition() < 0) {
            throw new IllegalArgumentException("Kafka Target Worker activation position is invalid");
        }
        final var expectedGuard = new ConsumerResourceGuard(
                barrier.authenticatedClusterId(),
                topic,
                new org.apache.kafka.common.Uuid(
                        barrier.nativeTopicUuid().getMostSignificantBits(),
                        barrier.nativeTopicUuid().getLeastSignificantBits()),
                assignment.shardId().partition());
        if (!expectedGuard.equals(exactConsumer.resourceGuard())) {
            throw new IllegalArgumentException("Kafka Target Worker consumer has a different resource guard");
        }

        final KafkaClientArtifactSourceRecordConsumer source;
        try {
            source = KafkaClientArtifactSourceRecordConsumer.forTarget(
                    exactConsumer,
                    barrier.authenticatedClusterId(),
                    barrier.nativeTopicUuid(),
                    assignment.shardId(),
                    topic,
                    Objects.requireNonNull(pollTimeout, "pollTimeout"));
            final TopicPartition topicPartition = new TopicPartition(topic, assignment.shardId().partition());
            exactConsumer.seek(topicPartition, barrier.exclusiveOffset());
        } catch (RuntimeException | Error failure) {
            closeConsumerAfterFailure(exactConsumer, failure);
            throw failure;
        }
        return TargetWorkerShardFactory.create(
                source, assignment, workClasses, store, resources, runtime, maintenance);
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private static void closeConsumerAfterFailure(
            final GuardedConsumer<byte[], byte[]> consumer, final Throwable failure) {
        try {
            consumer.close();
        } catch (RuntimeException | Error closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
