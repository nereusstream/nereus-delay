package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
import com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** File-backed actual K2 startup; original Source facts, CONTROL protection and native owners remain mandatory. */
public final class KafkaClientArtifactTargetReceiptFactory {
    private KafkaClientArtifactTargetReceiptFactory() {}

    /** A selector, never a proof: each requested phase is independently reread from the original Source Store. */
    public record RecoveryAdmission(SystemMutation image, TargetQuotaAttemptBudget.Phase phase) {
        public RecoveryAdmission {
            Objects.requireNonNull(image);
            if (phase != TargetQuotaAttemptBudget.Phase.ADMITTED && phase != TargetQuotaAttemptBudget.Phase.UNKNOWN) {
                throw new IllegalArgumentException("K2 cold opening requires an original ADMITTED or UNKNOWN selector");
            }
        }
    }

    public record Configuration(
            KafkaClientArtifactTargetReceiptRuntime.Settings settings,
            Map<String, String> producer,
            Map<String, String> reader,
            byte[] sdkJar,
            UUID receiptTopicId,
            int receiptLaneSlot,
            long slotGeneration,
            byte[] protectionDigest) {
        public Configuration {
            Objects.requireNonNull(settings);
            Objects.requireNonNull(receiptTopicId);
            producer = literal(producer);
            reader = literal(reader);
            Bytes.requireLength(sdkJar, 32, "K2 SDK artifact hash");
            sdkJar = Bytes.copy(sdkJar);
            Bytes.requireLength(protectionDigest, 32, "original K2 protection digest");
            protectionDigest = Bytes.copy(protectionDigest);
            if (receiptLaneSlot < 0 || receiptLaneSlot == Integer.MAX_VALUE || slotGeneration == 0) {
                throw new IllegalArgumentException("K2 receipt slot/generation is outside its finite native geometry");
            }
        }

        @Override
        public byte[] sdkJar() {
            return Bytes.copy(sdkJar);
        }

        @Override
        public byte[] protectionDigest() {
            return Bytes.copy(protectionDigest);
        }

        @Override
        public String toString() {
            return "ConfiguredK2Deployment";
        }

        private static Map<String, String> literal(Map<String, String> values) {
            final var result = Map.copyOf(values);
            if (result.size() > 64) {
                throw new IllegalArgumentException("K2 operational field bound exceeded");
            }
            result.forEach((key, value) -> {
                if (key.isBlank() || key.length() > 128 || value.isBlank() || value.length() > 4096) {
                    throw new IllegalArgumentException("K2 operational settings require finite literals");
                }
            });
            return result;
        }
    }

    public static Configuration load(Path path) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var result = new Configuration(
                KafkaClientArtifactTargetReceiptRuntime.Settings.load(f.prefixed("runtime.", 128)),
                f.prefixed("producer.", 64),
                f.prefixed("reader.", 64),
                f.hex("sdk.sha256", 32, 32),
                UUID.fromString(f.text("receipt.topicId")),
                f.integer("receipt.laneSlot"),
                f.unsigned64("receipt.slotGeneration"),
                f.hex("receipt.protectionDigest", 32, 32));
        f.requireComplete();
        return result;
    }

    /** No supplied Runtime/Owner/mapping is accepted: a cold mapping is read from its actual original Admission. */
    public static Opening open(
            TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime worker,
            TargetWorkerPublicationDispatch dispatch,
            byte[] channelDigest,
            Path configuration,
            RecoveryAdmission originalAdmission,
            LocalBrokerMtlsResolver resolver,
            ProfileCatalog profiles,
            CredentialAttestationTrustSet trust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            KafkaClientArtifactTargetReceiptProvider.AnchorPublisher controlPublisher,
            TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time,
            Supplier<BoundedReadBudget> reads,
            LongSupplier ownerClock)
            throws IOException {
        return openLoaded(
                startup,
                worker,
                dispatch,
                channelDigest,
                load(configuration),
                originalAdmission,
                resolver,
                profiles,
                trust,
                catalog,
                controlPublisher,
                budget,
                time,
                reads,
                ownerClock);
    }

    /** Independent current recovery material; result cannot publish, sample a new anchor or expose a raw producer. */
    public static KafkaClientArtifactTargetRecoveryRuntime openRecovery(
            TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime worker,
            byte[] channelDigest,
            Path configuration,
            RecoveryAdmission originalAdmission,
            Path authorization,
            LocalBrokerMtlsResolver resolver,
            com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization.Trust recoveryTrust,
            ProfileCatalog profiles,
            CredentialAttestationTrustSet historicalTrust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time,
            Supplier<BoundedReadBudget> reads,
            LongSupplier clock)
            throws IOException {
        return KafkaClientArtifactTargetRecoveryRuntime.create(
                startup,
                worker,
                channelDigest,
                load(configuration),
                originalAdmission,
                authorization,
                resolver,
                recoveryTrust,
                profiles,
                historicalTrust,
                catalog,
                budget,
                time,
                reads,
                clock);
    }

    static Opening openLoaded(
            TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime worker,
            TargetWorkerPublicationDispatch dispatch,
            byte[] channelDigest,
            Configuration config,
            RecoveryAdmission originalAdmission,
            LocalBrokerMtlsResolver resolver,
            ProfileCatalog profiles,
            CredentialAttestationTrustSet trust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            KafkaClientArtifactTargetReceiptProvider.AnchorPublisher controlPublisher,
            TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time,
            Supplier<BoundedReadBudget> reads,
            LongSupplier ownerClock) {
        final byte[] digest = Bytes.copy(channelDigest);
        try (var construction = startup.beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            startup.retainPublicationDispatch(worker, dispatch);
            final var entry = startup.publicationChannel(worker, digest, originalAdmission == null);
            final var channel = entry.identity();
            if (channel.context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT) {
                throw new IllegalArgumentException("K2 startup requires its exact Source receipt channel");
            }
            final var owner = startup.publicationOwnerIdentity(worker);
            final var original = originalAdmission == null
                    ? null
                    : Objects.requireNonNull(
                            switch (originalAdmission.phase()) {
                                case ADMITTED ->
                                    worker.readRecoveryAdmission(reads.get(), originalAdmission.image(), ownerClock)
                                            .kafkaMapping();
                                case UNKNOWN ->
                                    worker.readKafkaUnknown(reads.get(), originalAdmission.image(), ownerClock)
                                            .mapping();
                                default -> throw new IllegalArgumentException("unregistered cold K2 phase");
                            },
                            "original Source K2 mapping");
            final var resource = new KafkaReceiptSlotResource(
                    entry.physical().resource().kafka().authenticatedClusterId(),
                    config.receiptTopicId(),
                    channel.context().sourceShard().routeIncarnation(),
                    channel.context().sourceShard().partition(),
                    config.receiptLaneSlot(),
                    config.slotGeneration());
            if (original != null) {
                channel.requireExactFrozenIdentity(original.admission().channel());
                final var binding = original.admission().kafkaReceipt();
                if (!resource.equals(binding.receiptResource())
                        || !Arrays.equals(config.protectionDigest(), binding.retentionProtectionDigest())
                        || !config.settings().targetPhysicalTopic().equals(binding.targetPhysicalTopic())) {
                    throw new IllegalArgumentException("cold K2 configuration changes its original Source binding");
                }
            }
            // Material/Registry I/O does not hold a Startup or Store monitor.
            final var material = resolver.resolve(channel, entry.physical(), profiles, trust, time);
            construction.requireCurrent();
            startup.publicationChannel(worker, digest, original == null);
            final var provider = new KafkaClientArtifactTargetReceiptProvider(
                    worker,
                    owner,
                    channel,
                    catalog,
                    config.protectionDigest(),
                    resource,
                    profiles,
                    trust,
                    material,
                    time,
                    ownerClock,
                    config.sdkJar(),
                    reads,
                    original == null);
            if (original != null) {
                provider.requireBinding(original.admission().kafkaReceipt());
            }
            final KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher = original == null
                    ? Objects.requireNonNull(controlPublisher, "actual restricted CONTROL anchor publisher")
                    : (observation, target) -> {
                        throw new IllegalStateException("cold K2 mapping cannot publish a replacement anchor");
                    };
            final var graph = startup.graph();
            final var runtime = new KafkaClientArtifactTargetReceiptRuntime(
                    worker,
                    channel,
                    entry.physical(),
                    provider,
                    publisher,
                    budget,
                    dispatch.physical(),
                    graph.classes(),
                    graph.resources(),
                    config.settings(),
                    graph.io(),
                    new HashMap<>(config.producer()),
                    new HashMap<>(config.reader()),
                    reads,
                    ownerClock,
                    original);
            construction.requireCurrent();
            final var reopen = original == null
                    ? null
                    : new ReopenInputs(
                            config,
                            channel,
                            entry.physical(),
                            owner,
                            originalAdmission.image(),
                            original,
                            resolver,
                            profiles,
                            trust,
                            catalog,
                            budget,
                            time,
                            reads,
                            ownerClock);
            final var result = new Opening(startup, worker, dispatch, runtime, original == null, reopen);
            if (original != null && controlPublisher != null) {
                result.installAutomaticReopen(controlPublisher);
            }
            return result;
        } catch (RuntimeException | Error failure) {
            startup.failResourceStartup(failure);
            throw failure;
        }
    }

    record ReopenInputs(
            Configuration config,
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            com.nereusstream.delay.protocol.CanonicalTargetPartition physical,
            com.nereusstream.delay.protocol.OwnerIdentity owner,
            SystemMutation admission,
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
            LocalBrokerMtlsResolver resolver,
            ProfileCatalog profiles,
            CredentialAttestationTrustSet trust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time,
            Supplier<BoundedReadBudget> reads,
            LongSupplier clock) {}

    /** Separate normal-work entry. Recovery authority is not a SEND credential or permission. */
    public static RecoveryReopen configureRecoveryReopen(
            TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime worker,
            KafkaClientArtifactTargetRecoveryRuntime recovery,
            TargetWorkerPublicationDispatch dispatch,
            KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher) {
        try (var construction = startup.beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent();
            startup.retainPublicationDispatch(worker, dispatch);
            final var inputs = recovery.reopenInputs(startup, worker);
            final var result =
                    new RecoveryReopen(startup, worker, recovery, inputs, dispatch, Objects.requireNonNull(publisher));
            worker.registerPublicationMaintenance(recovery, result, inputs.budget(), inputs.clock());
            construction.requireCurrent();
            return result;
        } catch (RuntimeException | Error failure) {
            startup.failResourceStartup(failure);
            throw failure;
        }
    }

    private static Opening createNewWorkSuccessor(
            TargetWorkerSourceStartup startup,
            TargetWorkerShardRuntime worker,
            TargetWorkerPublicationDispatch dispatch,
            KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher,
            ReopenInputs inputs,
            TargetWorkerHostRuntime.PublicationConstruction construction) {
        construction.requireCurrent();
        worker.requirePublicationOwner(inputs.owner(), inputs.clock());
        final var actual = worker.requirePublicationChannel(
                inputs.channel(), true, inputs.reads().get(), inputs.clock());
        if (!Arrays.equals(actual.canonicalBytes(), inputs.physical().canonicalBytes())) {
            throw new IllegalStateException("K2 successor changes its original physical Target");
        }
        // Fresh ordinary SEND material, never the independent RECOVERY receipt or a renewed/reinterpreted old lease.
        final var material =
                inputs.resolver().resolve(inputs.channel(), actual, inputs.profiles(), inputs.trust(), inputs.time());
        construction.requireCurrent();
        if (!worker.kafkaMappingSettled(inputs.reads().get(), inputs.admission(), inputs.mapping(), inputs.clock())) {
            throw new IllegalStateException("K2 successor lost its original Source settlement");
        }
        final var provider = new KafkaClientArtifactTargetReceiptProvider(
                worker,
                inputs.owner(),
                inputs.channel(),
                inputs.catalog(),
                inputs.config().protectionDigest(),
                inputs.mapping().admission().kafkaReceipt().receiptResource(),
                inputs.profiles(),
                inputs.trust(),
                material,
                inputs.time(),
                inputs.clock(),
                inputs.config().sdkJar(),
                inputs.reads(),
                true);
        final var graph = startup.graph();
        final var active = new KafkaClientArtifactTargetReceiptRuntime(
                worker,
                inputs.channel(),
                actual,
                provider,
                publisher,
                inputs.budget(),
                dispatch.physical(),
                graph.classes(),
                graph.resources(),
                inputs.config().settings(),
                graph.io(),
                new HashMap<>(inputs.config().producer()),
                new HashMap<>(inputs.config().reader()),
                inputs.reads(),
                inputs.clock(),
                null);
        return new Opening(startup, worker, dispatch, active, true, null);
    }

    /** Root-owned normal-work continuation, separate from the recovery-only SDK and its public entry surface. */
    public static final class RecoveryReopen implements TargetWorkerShardRuntime.PublicationMaintenance {
        private final TargetWorkerSourceStartup startup;
        private final TargetWorkerShardRuntime worker;
        private final KafkaClientArtifactTargetRecoveryRuntime recovery;
        private final ReopenInputs inputs;
        private final TargetWorkerPublicationDispatch dispatch;
        private final KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher;
        private final java.util.concurrent.ThreadPoolExecutor io;
        private TargetWorkerHostRuntime host;
        private OwnedCheckpointIo<Boolean> pending;
        private volatile Opening successor;
        private volatile boolean stopping, failed;
        private boolean retired;

        private RecoveryReopen(
                TargetWorkerSourceStartup startup,
                TargetWorkerShardRuntime worker,
                KafkaClientArtifactTargetRecoveryRuntime recovery,
                ReopenInputs inputs,
                TargetWorkerPublicationDispatch dispatch,
                KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher) {
            this.startup = startup;
            this.worker = worker;
            this.recovery = recovery;
            this.inputs = inputs;
            this.dispatch = dispatch;
            this.publisher = publisher;
            io = startup.graph().io();
        }

        @Override
        public TargetWorkerShardRuntime.PublicationResources originalResources() {
            return recovery;
        }

        @Override
        public synchronized boolean runTurn(TargetWorkerHostRuntime actualHost) {
            if (stopping) {
                return ioExited();
            }
            if (host != null && host != actualHost) {
                throw new IllegalArgumentException("K2 continuation changed its original Host");
            }
            host = actualHost;
            if (pending != null) {
                final var result = pending.receipt();
                if (!pending.operationExited() || !result.isDone()) {
                    return false;
                }
                final boolean complete = result.join();
                if (!pending.ioExited()) {
                    return false;
                }
                pending = null;
                if (complete) {
                    return true;
                }
            }
            try {
                pending = OwnedCheckpointIo.submit(
                        guard -> {
                            guard.run();
                            final boolean complete = advance(actualHost);
                            guard.run();
                            return complete;
                        },
                        () -> {},
                        io);
            } catch (java.util.concurrent.RejectedExecutionException full) {
                if (io.isShutdown()) {
                    throw full;
                }
            }
            return false;
        }

        private boolean advance(TargetWorkerHostRuntime actualHost) {
            final var admitted = actualHost.tryBeginPublicationConstruction(worker);
            if (admitted.isEmpty()) {
                return false;
            }
            try (var construction = admitted.orElseThrow()) {
                construction.requireCurrent();
                if (worker.pendingCheckpointTask().isPresent()) {
                    return false;
                }
                if (retired) {
                    return true;
                }
                if (successor == null) {
                    worker.requirePublicationOwner(inputs.owner(), inputs.clock());
                    if (!worker.kafkaMappingSettled(
                            inputs.reads().get(), inputs.admission(), inputs.mapping(), inputs.clock())) {
                        return false;
                    }
                    if (worker.publishRecoveryRetainsAttempt(
                            inputs.mapping().admission().publishAttemptId())) {
                        return false;
                    }
                    if (!recovery.settleArchivedSource()) {
                        return false;
                    }
                    if (!recovery.settleClose()) {
                        return false;
                    }
                    if (!worker.publicationChannelOpen(
                            inputs.channel(), inputs.reads().get(), inputs.clock())) {
                        retired = true;
                        return true;
                    }
                    successor = createNewWorkSuccessor(startup, worker, dispatch, publisher, inputs, construction);
                }
                construction.requireCurrent();
                successor.advance();
                construction.requireCurrent();
                return successor.newWorkRuntime().isPresent();
            } catch (TargetWorkerHostRuntime.PublicationConstruction.Unavailable unavailable) {
                return false;
            } catch (com.nereusstream.delay.store.ReadIncompleteException yield) {
                return false;
            } catch (RuntimeException | Error failure) {
                failed = true;
                startup.failResourceStartup(failure);
                throw failure;
            }
        }
        /** A fresh independently authorized normal runtime becomes visible only after its actual READY/dispatch handoff. */
        public Optional<KafkaClientArtifactTargetReceiptRuntime> newWorkRuntime() {
            final var active = successor;
            return failed || stopping || active == null ? Optional.empty() : active.newWorkRuntime();
        }

        @Override
        public synchronized void requestStop() {
            stopping = true;
            if (pending != null) {
                pending.requestStop();
                pending.retryCleanup();
            }
        }

        @Override
        public synchronized boolean ioExited() {
            return pending == null || pending.ioExited();
        }
    }

    /** Observes only the original registered native opening, including its late failed-cleanup obligations. */
    public static final class Opening {
        private final TargetWorkerSourceStartup startup;
        private final TargetWorkerShardRuntime worker;
        private final TargetWorkerPublicationDispatch dispatch;
        private final KafkaClientArtifactTargetReceiptRuntime runtime;
        private final boolean newWork;
        private final ReopenInputs reopen;
        private TargetWorkerHostRuntime reopenHost;
        private volatile Opening successor;
        private volatile boolean failed;
        private volatile boolean retiredWithoutSuccessor;
        private AutomaticReopen automatic;

        private Opening(
                TargetWorkerSourceStartup startup,
                TargetWorkerShardRuntime worker,
                TargetWorkerPublicationDispatch dispatch,
                KafkaClientArtifactTargetReceiptRuntime runtime,
                boolean newWork,
                ReopenInputs reopen) {
            this.startup = startup;
            this.worker = worker;
            this.dispatch = dispatch;
            this.runtime = runtime;
            this.newWork = newWork;
            this.reopen = reopen;
        }

        /** Completes the original startup configuration, before Host transfer; never installs a replacement controller. */
        public void configureAutomaticReopen(KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher) {
            try (var construction = startup.beginWorkerResourceConstruction(worker)) {
                construction.requireCurrent();
                installAutomaticReopen(publisher);
                construction.requireCurrent();
            } catch (RuntimeException | Error failure) {
                startup.failResourceStartup(failure);
                throw failure;
            }
        }

        private synchronized void installAutomaticReopen(
                KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher) {
            if (newWork || reopen == null || failed) {
                throw new IllegalStateException("automatic K2 handoff requires its original cold handle");
            }
            Objects.requireNonNull(publisher, "actual restricted CONTROL publisher");
            if (automatic != null) {
                if (automatic.publisher != publisher) {
                    throw new IllegalStateException("original CONTROL publisher cannot be replaced");
                }
                return;
            }
            final var original =
                    new AutomaticReopen(this, publisher, startup.graph().io());
            worker.registerPublicationMaintenance(runtime, original, reopen.budget(), reopen.clock());
            automatic = original;
        }

        public synchronized KafkaClientArtifactTargetReceiptRuntime.Turn advance() {
            if (failed) {
                throw new IllegalStateException("original configured K2 startup failed");
            }
            try {
                startup.requireCurrent();
                startup.requireAdmittedWorker(worker);
                final var turn = runtime.runStartupTurn();
                if (turn.failure() != null || turn.status() == KafkaClientArtifactTargetReceiptRuntime.Status.FAILED) {
                    throw new IllegalStateException("actual configured K2 opening failed", turn.failure());
                }
                startup.requireCurrent();
                startup.requireAdmittedWorker(worker);
                if (turn.status() == KafkaClientArtifactTargetReceiptRuntime.Status.READY && newWork) {
                    runtime.bindDispatch(dispatch);
                }
                return turn;
            } catch (RuntimeException | Error failure) {
                failed = true;
                startup.failResourceStartup(failure);
                throw failure;
            }
        }
        /** One caller-driven transition on the same Host; no new SDK is allocated until original Source and exit proofs hold. */
        public synchronized Optional<Opening> tryReopenForNewWork(
                TargetWorkerHostRuntime host, KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher) {
            if (failed || newWork || reopen == null) {
                throw new IllegalStateException("only the original cold K2 opening can hand off");
            }
            Objects.requireNonNull(publisher, "actual restricted CONTROL anchor publisher");
            if (reopenHost != null && reopenHost != host) {
                throw new IllegalArgumentException("K2 successor changed its original Host");
            }
            final var admitted = host.tryBeginPublicationConstruction(worker);
            if (admitted.isEmpty()) {
                return Optional.empty();
            }
            try (var construction = admitted.orElseThrow()) {
                construction.requireCurrent();
                reopenHost = host;
                if (worker.pendingCheckpointTask().isPresent() || retiredWithoutSuccessor) {
                    return Optional.empty();
                }
                if (successor != null) {
                    return Optional.of(successor);
                }
                worker.requirePublicationOwner(reopen.owner(), reopen.clock());
                if (!worker.kafkaMappingSettled(
                        reopen.reads().get(), reopen.admission(), reopen.mapping(), reopen.clock())) {
                    return Optional.empty();
                }
                if (!runtime.settleClose()) {
                    return Optional.empty();
                }
                if (!worker.publicationChannelOpen(
                        reopen.channel(), reopen.reads().get(), reopen.clock())) {
                    retiredWithoutSuccessor = true;
                    return Optional.empty();
                }
                // Registration already transferred ownership to the original Worker before this final Host recheck.
                successor = createNewWorkSuccessor(startup, worker, dispatch, publisher, reopen, construction);
                construction.requireCurrent();
                return Optional.of(successor);
            } catch (TargetWorkerHostRuntime.PublicationConstruction.Unavailable stopping) {
                return Optional.empty();
            } catch (com.nereusstream.delay.store.ReadIncompleteException yield) {
                return Optional.empty();
            } catch (RuntimeException | Error failure) {
                failed = true;
                startup.failResourceStartup(failure);
                throw failure;
            }
        }

        /** Actual SEND preparation can follow the one privately retained successor; cold recovery retains runtime(). */
        public Optional<KafkaClientArtifactTargetReceiptRuntime> newWorkRuntime() {
            if (failed) {
                return Optional.empty();
            }
            final var next = successor;
            if (!newWork) {
                return next == null ? Optional.empty() : next.newWorkRuntime();
            }
            return runtime.readyForNewWork() ? Optional.of(runtime) : Optional.empty();
        }

        public KafkaClientArtifactTargetReceiptRuntime runtime() {
            return runtime;
        }
    }

    /** One owned queued method on the original finite I/O pool; the existing fleet tick only submits/observes it. */
    private static final class AutomaticReopen implements TargetWorkerShardRuntime.PublicationMaintenance {
        private final Opening original;
        private final KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher;
        private final java.util.concurrent.ThreadPoolExecutor io;
        private OwnedCheckpointIo<Boolean> pending;
        private boolean stopping;

        private AutomaticReopen(
                Opening original,
                KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher,
                java.util.concurrent.ThreadPoolExecutor io) {
            this.original = original;
            this.publisher = publisher;
            this.io = io;
        }

        @Override
        public TargetWorkerShardRuntime.PublicationResources originalResources() {
            return original.runtime;
        }

        @Override
        public synchronized boolean runTurn(TargetWorkerHostRuntime host) {
            if (stopping) {
                return ioExited();
            }
            if (pending != null) {
                final var result = pending.receipt();
                if (!pending.operationExited() || !result.isDone()) {
                    return false;
                }
                final boolean complete = result.join();
                if (!pending.ioExited()) {
                    return false;
                }
                pending = null;
                if (complete) {
                    return true;
                }
            }
            try {
                pending = OwnedCheckpointIo.submit(
                        guard -> {
                            guard.run();
                            final var successor = original.tryReopenForNewWork(host, publisher);
                            if (original.retiredWithoutSuccessor) {
                                guard.run();
                                return true;
                            }
                            if (successor.isEmpty()) {
                                return false;
                            }
                            successor.orElseThrow().advance();
                            guard.run();
                            return successor.orElseThrow().newWorkRuntime().isPresent();
                        },
                        () -> {},
                        io);
            } catch (java.util.concurrent.RejectedExecutionException full) {
                if (io.isShutdown()) {
                    throw full;
                }
                return false;
            }
            return false;
        }

        @Override
        public synchronized void requestStop() {
            stopping = true;
            if (pending != null) {
                pending.requestStop();
                pending.retryCleanup();
            }
        }

        @Override
        public synchronized boolean ioExited() {
            return pending == null || pending.ioExited();
        }
    }
}
