package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.OwnedSourceSystemWriter;
import com.nereusstream.delay.ownership.OwnerLease;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization;
import com.nereusstream.delay.ownership.TargetDestinationActivation;
import com.nereusstream.delay.ownership.TargetDestinationChannelRuntime;
import com.nereusstream.delay.ownership.TargetDestinationCredentialChecks;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetOwnerDrainCoordinator;
import com.nereusstream.delay.ownership.TargetScopeResourceManager;
import com.nereusstream.delay.ownership.TargetSourceBacklog;
import com.nereusstream.delay.ownership.TargetSourceBacklogRead;
import com.nereusstream.delay.ownership.TargetSourceRecoveryInputs;
import com.nereusstream.delay.ownership.TargetWorkerChannelInventory;
import com.nereusstream.delay.ownership.TargetWorkerCheckpointComposition;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
import com.nereusstream.delay.ownership.TargetWorkerOrdinaryDrr;
import com.nereusstream.delay.ownership.TargetWorkerOxiaRuntime;
import com.nereusstream.delay.ownership.TargetWorkerProcessRuntime;
import com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch;
import com.nereusstream.delay.ownership.TargetWorkerPublishingConfiguration;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter;
import com.nereusstream.delay.semantic.TrustedClock;
import com.nereusstream.delay.store.BoundedReadBudget;
import io.oxia.client.api.Authentication;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Embedded production service over the original configured Process, Source and SDK owners. */
public final class TargetWorkerConfiguredService {
    public enum State { STARTING, RUNNING, DRAINING, FAILED, TERMINATED }
    public enum RecoveryAction { DOWNLOAD, INSTALLED, REPAIR }

    /** Selection is deployment input; the live pin is always acquired by the original Source startup. */
    public record Recovery(RecoveryCandidateRef candidate, byte[] pinId, RecoveryAction action, UUID repairTarget) {
        public Recovery {
            Objects.requireNonNull(candidate); Objects.requireNonNull(action);
            Bytes.requireLength(pinId, 16, "configured recovery pin"); pinId = Bytes.copy(pinId);
            int pinBits = 0; for (byte part : pinId) pinBits |= part;
            if (pinBits == 0) throw new IllegalArgumentException("configured recovery pin must be assigned");
            if ((action == RecoveryAction.REPAIR) != (repairTarget != null)) {
                throw new IllegalArgumentException("STAGED repair requires its exact original target");
            }
        }
        @Override public byte[] pinId() { return Bytes.copy(pinId); }
    }

    /** Genuine protected inputs remain separate from the literal service manifest. */
    public record CheckpointInputs(TargetWorkerCheckpointComposition.TrustedDeployment deployment,
            Clock clock, Recovery recovery) {
        public CheckpointInputs {
            Objects.requireNonNull(deployment); Objects.requireNonNull(clock);
        }
    }

    public record ReceiptInputs(KafkaClientArtifactTargetReceiptManagementFactory.TrustInputs management,
            KafkaClientArtifactTargetReceiptFactory.RecoveryAdmission recoveryAdmission) {
        public ReceiptInputs { Objects.requireNonNull(management); }
    }

    /** Independent original Journal material and literal client settings; no destination channel supplies its authority. */
    public record JournalInputs(TargetWorkerServiceConfiguration.PulsarClientConfiguration client,
            CredentialBinding binding, LocalBrokerMtlsResolver.Receipt credential,
            PulsarClientArtifactOwnedTargetJournal.Configuration configuration,
            CapacityVector cost, PulsarClientArtifactAttemptJournal.TargetWriterGuard writerGuard,
            PulsarAttemptJournal.NegativeReplayAuthority negativeReader) {
        public JournalInputs {
            Objects.requireNonNull(client); Objects.requireNonNull(binding); Objects.requireNonNull(credential);
            Objects.requireNonNull(configuration); Objects.requireNonNull(cost);
            Objects.requireNonNull(writerGuard); Objects.requireNonNull(negativeReader);
            credential.requireBinding(binding);
        }
    }

    public record SourceInputs(TargetWorkerSourceStartup.Owner owner, TargetWorkerSourceStartup.Initial initial,
            TargetWorkerSourceRuntimeFactory.Content content, SourceReplaySuccessor successor,
            ShardCapacityEnvelope capacity, TargetSourceRecoveryInputs.Provider recovery,
            TargetWorkerShardRuntime.Maintenance maintenance, TargetWorkerPublishingConfiguration publishing,
            Supplier<TargetWorkerShardRuntime.MessageExpiryRequest> expiry, CheckpointInputs checkpoint,
            Map<String, ReceiptInputs> receipts, JournalInputs journal) {
        public SourceInputs {
            Objects.requireNonNull(owner); Objects.requireNonNull(content); Objects.requireNonNull(successor);
            Objects.requireNonNull(capacity); Objects.requireNonNull(recovery); Objects.requireNonNull(maintenance);
            Objects.requireNonNull(publishing); Objects.requireNonNull(expiry); Objects.requireNonNull(checkpoint);
            receipts = Map.copyOf(receipts);
        }
    }

    /** This root supplies authority and operational requests, never prebuilt Workers, Hosts or Source runtimes. */
    public record Dependencies(TargetWorkerOxiaRuntime.Registries registries, Authentication authentication,
            TrustedClock trustedClock, LongSupplier nanos, BoundedAsyncMetricExporter.Exporter exporter,
            Runnable startupGuard, LocalBrokerMtlsResolver mtls, SourceTransportCredentialAuthorization.Trust sourceTrust,
            Supplier<TrustedUtcIntervalEvidence> time, ProfileCatalog profiles, CredentialAttestationTrustSet targetTrust,
            Supplier<BoundedReadBudget> reads, LongSupplier ownerClock, LongSupplier schedulerClock,
            TargetWorkerOrdinaryDrr.Requests requests, TargetOwnerDrainCoordinator.Request exitRequest,
            Consumer<Throwable> failures, Map<ShardId, SourceInputs> sources) {
        public Dependencies {
            Objects.requireNonNull(registries); Objects.requireNonNull(trustedClock); Objects.requireNonNull(nanos);
            Objects.requireNonNull(exporter); Objects.requireNonNull(startupGuard); Objects.requireNonNull(mtls);
            Objects.requireNonNull(sourceTrust); Objects.requireNonNull(time); Objects.requireNonNull(profiles);
            Objects.requireNonNull(targetTrust); Objects.requireNonNull(reads); Objects.requireNonNull(ownerClock);
            Objects.requireNonNull(schedulerClock); Objects.requireNonNull(requests); Objects.requireNonNull(exitRequest);
            Objects.requireNonNull(failures); sources = Map.copyOf(sources);
        }
    }

    private enum Stage { CHECKPOINT, SOURCE, RESTORE_PREPARATION, RESTORE, OPEN_SOURCE, SOURCE_READY,
        WRITER, WRITER_READY, INVENTORY, MANAGEMENT, CHANNELS, JOURNAL, PUBLISHING, READY }

    private static final class Source {
        final TargetWorkerServiceConfiguration.Source configuration;
        final SourceInputs inputs;
        Stage stage = Stage.CHECKPOINT;
        TargetWorkerCheckpointComposition checkpoint;
        TargetWorkerSourceReadResources reads;
        TargetWorkerSourceStartup startup;
        KafkaClientArtifactTargetWorkerSourceFactory.Opening kafka;
        PulsarClientArtifactTargetWorkerSourceFactory.Opening pulsar;
        TargetWorkerShardRuntime worker;
        OwnedSourceSystemWriter writer;
        TargetWorkerChannelInventory inventory;
        TargetWorkerPublicationDispatch dispatch;
        final Map<String, KafkaClientArtifactTargetReceiptManagementFactory.Opening> management = new LinkedHashMap<>();
        TargetDestinationChannelRuntime channel;
        KafkaClientArtifactTargetReceiptFactory.Opening receipt;
        TargetDestinationActivation channelActivation;
        PulsarClientArtifactOwnedBackendGroup channelBackend;
        PulsarClientArtifactOwnedTargetJournal journal;
        PulsarClientArtifactOwnedBackendGroup journalBackend;
        int managementIndex;
        int channelIndex;
        Source(TargetWorkerServiceConfiguration.Source configuration, SourceInputs inputs) {
            this.configuration = configuration; this.inputs = inputs;
        }
    }

    private final TargetWorkerServiceConfiguration configuration;
    private final Dependencies dependencies;
    private final List<Source> sources;
    private final Map<TargetWorkerShardRuntime, Source> workers = new java.util.IdentityHashMap<>();
    private final Map<Path, KafkaClientArtifactTargetReceiptManagementFactory.Opening> managementOwners = new LinkedHashMap<>();
    private TargetWorkerProcessRuntime process;
    private TargetDestinationSdkBudget sdk;
    private final Object failureGate = new Object();
    private volatile State state = State.STARTING;
    private volatile Throwable firstFailure;
    private int sourceIndex;
    private boolean processReady;
    private TargetOwnerDrainCoordinator.Request stopRequest;

    private TargetWorkerConfiguredService(TargetWorkerServiceConfiguration configuration, Dependencies dependencies) {
        this.configuration = Objects.requireNonNull(configuration);
        this.dependencies = Objects.requireNonNull(dependencies);
        preflight(configuration, dependencies);
        sources = configuration.sources().stream().map(source -> new Source(source,
                dependencies.sources().get(source.scope().shard()))).toList();
    }

    /** All manifest/subconfiguration reads and closed admission checks precede the first native allocation. */
    public static TargetWorkerConfiguredService open(Path manifest, Dependencies dependencies) throws IOException {
        return open(TargetWorkerServiceConfiguration.load(manifest), dependencies);
    }

    public static TargetWorkerConfiguredService open(TargetWorkerServiceConfiguration configuration,
            Dependencies dependencies) {
        final var service = new TargetWorkerConfiguredService(configuration, dependencies);
        try {
            service.process = TargetWorkerProcessRuntime.open(configuration.processConfiguration(), dependencies.registries(),
                    dependencies.authentication(), dependencies.trustedClock(), dependencies.nanos(), dependencies.exporter(),
                    dependencies.startupGuard());
            return service;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof TargetWorkerProcessRuntime.StartupFailure partial) service.process = partial.root();
            service.fail(failure);
            throw new StartupFailure(failure, service);
        }
    }

    private static void preflight(TargetWorkerServiceConfiguration configuration, Dependencies dependencies) {
        if (configuration.sources().size() != dependencies.sources().size()) {
            throw new IllegalArgumentException("service requires exact genuine input inventory for every configured Source");
        }
        final var envelopes = new ArrayList<ShardCapacityEnvelope>();
        final var management = new HashMap<Path, KafkaClientArtifactTargetReceiptManagementFactory.TrustInputs>();
        final var allocationIds = new java.util.HashSet<>(configuration.allocationIds());
        final var journalNamespaces = new java.util.HashSet<String>();
        CapacityVector sdkDemand = configuration.configuredSdkDemand();
        int sdkClients = configuration.configuredSdkClients();
        long retainedHeap = configuration.configurationHeapBytes();
        for (var source : configuration.sources()) {
            final var input = Objects.requireNonNull(dependencies.sources().get(source.scope().shard()), "Source authority inputs");
            final var assignment = input.owner().publication().assignment();
            if (!assignment.sourceAssignment().shardId().equals(source.scope().shard())
                    || !Arrays.equals(source.scope().tenantScope(), input.owner().tenant().tenantRoutingScope())
                    || !Arrays.equals(assignment.capacityEnvelopeDigest(), input.capacity().envelopeDigest())
                    || !assignment.workerId().equals(input.owner().workerId())) {
                throw new IllegalArgumentException("configured Source changes its actual placement/capacity/Owner");
            }
            final var barrier = assignment.sourceAssignment().activationBarrier();
            if (source.scope().shard().partition() < 0) throw new IllegalArgumentException("Source partition exceeds the native SDK domain");
            if (source.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource kafka) {
                if (!(barrier instanceof KafkaActivationBarrier actual)
                        || !actual.authenticatedClusterId().equals(kafka.deployment().source().settings().authenticatedClusterId())
                        || !actual.nativeTopicUuid().equals(kafka.deployment().source().settings().topicId())) {
                    throw new IllegalArgumentException("configured Kafka Source differs from its original activation barrier");
                }
            } else {
                final var pulsar = ((TargetWorkerServiceConfiguration.PulsarSource) source.typedSource()).deployment().source();
                if (!(barrier instanceof PulsarActivationBarrier actual) || !actual.physicalTopic().equals(pulsar.topic())
                        || !Arrays.equals(actual.brokerResourceIncarnation(), pulsar.guard().resourceIncarnation())
                        || !pulsar.settings().authenticatedClusterId().equals(pulsar.guard().authenticatedClusterId())) {
                    throw new IllegalArgumentException("configured Pulsar Source differs from its original activation barrier");
                }
            }
            if ((source.mode() == TargetWorkerServiceConfiguration.Mode.FRESH) != (input.initial() != null)
                    || (source.mode() == TargetWorkerServiceConfiguration.Mode.PUBLISHED_RECOVERY)
                        != (input.checkpoint().recovery() != null)) {
                throw new IllegalArgumentException("Source mode lacks its exact Initial or published recovery selection");
            }
            if (input.initial() != null && (!source.scope().equals(input.initial().scope())
                    || !Arrays.equals(source.lineage(), input.initial().lineage()))) {
                throw new IllegalArgumentException("fresh Source authority changes its configured root");
            }
            if (input.checkpoint().recovery() != null && !Arrays.equals(source.lineage(),
                    input.checkpoint().recovery().candidate().recoveryLineageId())) {
                throw new IllegalArgumentException("published recovery selection changes its configured Source lineage");
            }
            if (input.content().evidencePublication() != null) {
                throw new IllegalArgumentException("service constructs its actual original checkpoint publication coordinator");
            }
            if (!(input.content().reads() instanceof com.nereusstream.delay.store.TargetStoreBackend.CheckpointReadAuthority)) {
                throw new IllegalArgumentException("configured service requires the original checkpoint read authority with independent external and local checks");
            }
            retainedHeap = Math.addExact(retainedHeap, Math.addExact(input.content().maximumBytes(),
                    Math.addExact(source.typedSource().maximumRecoveryInputBytes(),
                            Math.addExact(source.typedCheckpoint().retainedHeapBytes(), source.inventoryLimits().maximumRetainedBytes()))));
            if (input.content().scopeResources() != null) retainedHeap = Math.addExact(retainedHeap,
                    TargetScopeResourceManager.maximumResidentBytes(input.content().scopeResources(),
                            input.content().maximumBytes(), input.content().maximumBytes()));
            final var receiptKeys = new java.util.HashSet<String>();
            final boolean managed = input.publishing().endpoints().stream()
                    .anyMatch(TargetWorkerPublishingConfiguration.Managed.class::isInstance);
            if (managed != (input.journal() != null)) {
                throw new IllegalArgumentException("managed publishing requires its genuine configured active Journal inputs");
            }
            if (input.journal() != null) {
                final var journal = input.journal();
                if (journal.configuration().resource().partition() != source.scope().shard().partition()
                        || !journal.configuration().resource().authenticatedClusterId()
                            .equals(journal.client().settings().authenticatedClusterId())) {
                    throw new IllegalArgumentException("Journal configuration changes its Source partition or actual client cluster");
                }
                journal.credential().requireBinding(journal.binding());
                journal.credential().requirePulsarEndpoint(journal.client().settings().serviceUrl());
                sdkDemand = sdkDemand.add(journal.client().cost()).add(journal.cost());
                sdkClients = Math.addExact(sdkClients, 2);
                requireAdditionalAllocation(configuration, allocationIds, journal.client().settings().allocationId());
                requireAdditionalAllocation(configuration, allocationIds, journal.configuration().allocationId());
                final var resource = journal.configuration().resource();
                final String namespace = resource.authenticatedClusterId() + '\0' + Bytes.hex(resource.resourceIncarnation())
                        + '\0' + resource.physicalTopic() + '\0' + Long.toUnsignedString(resource.physicalTopicCreationTimestamp())
                        + '\0' + Integer.toUnsignedString(resource.partition()) + '\0' + journal.configuration().producerName();
                if (!journalNamespaces.add(namespace)) throw new IllegalArgumentException("Sources cannot open the same active Journal producer namespace");
            }
            for (var channel : source.channels()) {
                if (channel.typedConfiguration() instanceof TargetWorkerServiceConfiguration.KafkaReceipt) {
                    final var key = Bytes.hex(channel.digest()); receiptKeys.add(key);
                    final var receipt = Objects.requireNonNull(input.receipts().get(key), "actual K2 management authority");
                    Objects.requireNonNull(receipt.management().current(), "actual current receipt management");
                    final var prior = management.putIfAbsent(channel.managementFile(), receipt.management());
                    if (prior != null && prior != receipt.management()) {
                        throw new IllegalArgumentException("shared management manifest must keep the same original trusted input owner");
                    }
                }
            }
            if (!receiptKeys.equals(input.receipts().keySet())) {
                throw new IllegalArgumentException("receipt authority inventory differs from configured K2 channels");
            }
            envelopes.add(input.capacity());
        }
        if (retainedHeap > configuration.processConfiguration().worker().resources().certifiedJvmHeapBytes()) {
            throw new IllegalArgumentException("trusted Source and Scope inputs exceed the configured process heap envelope");
        }
        if (sdkClients > configuration.maximumSdkClients() || !configuration.sdkLimit().covers(sdkDemand)) {
            throw new IllegalArgumentException("configured SDK envelope omits the independent active Journal resources");
        }
        configuration.processConfiguration().worker().requireCapacity(envelopes,
                configuration.processFixedCost().add(configuration.otherFixedCost()).add(configuration.sdkLimit()),
                configuration.transitionDemand());
    }

    private static void requireAdditionalAllocation(TargetWorkerServiceConfiguration configuration,
            java.util.Set<String> allocated, String identity) {
        if (identity == null || identity.isBlank() || identity.length() > 512 || !allocated.add(identity)) {
            throw new IllegalArgumentException("SDK allocation identity is invalid or already used by this service");
        }
        for (var source : configuration.sources()) for (var channel : source.channels()) {
            if (identity.startsWith(Bytes.hex(channel.digest()) + "/reader/")) {
                throw new IllegalArgumentException("SDK allocation identity collides with an original K2 reader generation");
            }
        }
    }

    /** One finite driver step. Pending transitions retain their exact original Opening and owned I/O handles. */
    public synchronized State advance() {
        if (state == State.FAILED || state == State.DRAINING) { advanceStop(); return state; }
        if (state != State.STARTING) return state;
        try {
            if (!processReady) {
                if (!process.advanceStartup()) return state;
                sdk = process.createDestinationSdkBudget(sources.stream().map(source -> source.inputs.capacity()).toList(),
                        configuration.processFixedCost().add(configuration.otherFixedCost()), configuration.transitionDemand(),
                        configuration.sdkLimit(), configuration.maximumSdkClients());
                processReady = true;
                return state;
            }
            process.runStartupWorkTurn();
            if (sourceIndex < sources.size()) {
                final var source = sources.get(sourceIndex);
                advanceSource(source);
                if (source.stage == Stage.READY) { workers.put(source.worker, source); sourceIndex++; }
                return state;
            }
            process.prepareHostWithDeferredCheckpoints((worker, claim, cut) -> original(worker).checkpoint.deferredRequestFactory().create(worker, claim, cut),
                    dependencies.ownerClock(), this::backgroundFailure);
            process.activate(this::observe, dependencies.requests(), dependencies.ownerClock(), dependencies.schedulerClock(),
                    this::backgroundFailure);
            if (firstFailure != null) throw new IllegalStateException("original service failed during activation", firstFailure);
            state = State.RUNNING;
            return state;
        } catch (RuntimeException | Error failure) {
            fail(failure);
            throw new StartupFailure(failure, this);
        }
    }

    private void advanceSource(Source source) {
        final var config = source.configuration;
        final var input = source.inputs;
        switch (source.stage) {
            case CHECKPOINT -> {
                if (source.reads == null) source.reads = TargetWorkerSourceReadResources.create(config, config.typedReadResources(),
                        sdk, dependencies.mtls(), dependencies.sourceTrust(), dependencies.time());
                if (source.checkpoint == null) source.checkpoint = process.openCheckpointComposition(config.typedCheckpoint(),
                        input.checkpoint().deployment(), source.reads.histories(), input.checkpoint().clock());
                if (source.checkpoint.advancePreparation()) source.stage = Stage.SOURCE;
            }
            case SOURCE -> {
                final var original = input.content();
                final var content = new TargetWorkerSourceRuntimeFactory.Content(original.provider(), original.physical(),
                        original.reads(), original.lookupTimeout(), original.maximumBytes(), source.checkpoint.evidencePublication(),
                        original.scopeResources());
                source.startup = switch (config.mode()) {
                    case FRESH -> process.initializeSource(input.owner(), input.initial(), content, input.successor());
                    case REOPEN -> process.reopenSource(input.owner(), config.scope(), content, input.successor());
                    case PUBLISHED_RECOVERY -> process.checkpointSource(input.owner(), config.scope(), content, input.successor());
                };
                source.checkpoint.bindStartup(source.startup, dependencies.ownerClock());
                if (config.mode() == TargetWorkerServiceConfiguration.Mode.PUBLISHED_RECOVERY) {
                    final var recovery = input.checkpoint().recovery();
                    final var lease = source.startup.ownerLease();
                    final var pin = source.startup.createCheckpointPin(config.typedCheckpoint().shardPrefix(), recovery.pinId(),
                            ownerIdentity(config, lease), recovery.candidate());
                    if (recovery.action() == RecoveryAction.INSTALLED) {
                        source.startup.resumeInstalledCheckpoint(); source.stage = Stage.OPEN_SOURCE;
                    } else {
                        source.checkpoint.prepareRestore(recovery.candidate(), pin); source.stage = Stage.RESTORE_PREPARATION;
                    }
                } else source.stage = Stage.OPEN_SOURCE;
            }
            case RESTORE_PREPARATION -> {
                final var prepared = source.checkpoint.restoreMaterial();
                if (prepared.isEmpty()) return;
                final var material = prepared.orElseThrow();
                final var recovery = input.checkpoint().recovery();
                if (recovery.action() == RecoveryAction.REPAIR) source.startup.repairCheckpoint(recovery.repairTarget(),
                        material.request(), material.downloader(), material.images(), material.protection());
                else source.startup.restoreCheckpoint(material.request(), material.downloader(), material.images(), material.protection());
                source.stage = Stage.RESTORE;
            }
            case RESTORE -> { if (source.startup.finishCheckpointRestore()) source.stage = Stage.OPEN_SOURCE; }
            case OPEN_SOURCE -> {
                if (config.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource kafka) {
                    source.kafka = KafkaClientArtifactTargetWorkerSourceFactory.openConfiguredSource(source.startup,
                            kafka.deployment().source(), dependencies.mtls(), dependencies.sourceTrust(), input.recovery(),
                            dependencies.time(), sdk, kafka.deployment().cost(), input.maintenance(), dependencies.ownerClock());
                } else if (config.typedSource() instanceof TargetWorkerServiceConfiguration.PulsarSource pulsar) {
                    source.pulsar = PulsarClientArtifactTargetWorkerSourceFactory.openConfiguredSource(source.startup,
                            pulsar.deployment().source(), dependencies.mtls(), dependencies.sourceTrust(), input.recovery(),
                            dependencies.time(), sdk, pulsar.deployment().cost(), input.maintenance(), dependencies.ownerClock());
                } else throw new IllegalArgumentException("unsupported configured Source");
                source.stage = Stage.SOURCE_READY;
            }
            case SOURCE_READY -> {
                final var ready = source.kafka != null ? source.kafka.advance() : source.pulsar.advance();
                if (ready.isPresent()) {
                    source.worker = ready.orElseThrow();
                    if (!source.checkpoint.rebindStartupOwner(source.startup, dependencies.ownerClock())) return;
                    source.reads.bindStartup(source.startup, source.kafka != null ? source.kafka.actualBackend() : source.pulsar.actualBackend(),
                            source.checkpoint::requireHistoryReadable);
                    source.stage = Stage.WRITER;
                }
            }
            case WRITER -> {
                if (config.typedWriter() instanceof TargetWorkerServiceConfiguration.KafkaWriter writer)
                    source.kafka.openSystemWriter(writer.configuration(), dependencies.mtls(), dependencies.sourceTrust());
                else if (config.typedWriter() instanceof TargetWorkerServiceConfiguration.PulsarWriter writer)
                    source.pulsar.openSystemWriter(writer.configuration(), dependencies.mtls(), dependencies.sourceTrust());
                else throw new IllegalArgumentException("unsupported configured Source writer");
                source.stage = Stage.WRITER_READY;
            }
            case WRITER_READY -> {
                final Optional<? extends OwnedSourceSystemWriter> ready = source.kafka != null
                        ? source.kafka.advanceSystemWriter() : source.pulsar.advanceSystemWriter();
                if (ready.isEmpty()) return;
                source.writer = ready.orElseThrow();
                source.dispatch = source.startup.publicationDispatch(configuration.publicationDispatch());
                source.inventory = source.startup.beginChannelInventory(config.inventoryLimits());
                source.stage = Stage.INVENTORY;
            }
            case INVENTORY -> {
                if (source.startup.advanceChannelInventory().status() != TargetWorkerChannelInventory.Status.COMPLETE) return;
                requireCompleteChannels(source);
                source.stage = Stage.MANAGEMENT;
            }
            case MANAGEMENT -> advanceManagement(source);
            case CHANNELS -> advanceChannel(source);
            case JOURNAL -> {
                if (input.journal() == null) { source.stage = Stage.PUBLISHING; return; }
                final var journal = input.journal();
                if (source.journalBackend == null) {
                    source.startup.requireActiveJournalOpening(journal.configuration().resource(), journal.configuration().producerName());
                    source.journalBackend = PulsarClientArtifactOwnedBackendGroup.openVerifiedMtlsForStartup(journal.client().settings(),
                            journal.binding(), journal.client().sdkJar(), journal.credential(), dependencies.time(), () -> {
                                source.worker.requirePublicationOwner(ownerIdentity(config, source.startup.ownerLease()), dependencies.ownerClock());
                                journal.writerGuard().requireActive(config.scope().shard(), journal.configuration().resource());
                            }, sdk, journal.client().cost(), source.startup, source.worker);
                    return;
                }
                if (!source.journalBackend.finishStartup()) return;
                if (source.journal == null) {
                    source.journal = PulsarClientArtifactOwnedTargetJournal.open(source.startup, source.worker, source.journalBackend,
                            journal.configuration(), sdk, journal.cost(), ownerIdentity(config, source.startup.ownerLease()),
                            journal.writerGuard(), journal.negativeReader(), dependencies.time(), dependencies.ownerClock());
                    return;
                }
                if (source.journal.finishStartup()) source.stage = Stage.PUBLISHING;
            }
            case PUBLISHING -> {
                source.startup.configurePublishing(source.writer, input.publishing());
                source.worker.configureMessageExpiryMaintenance(source.writer, input.expiry());
                source.stage = Stage.READY;
            }
            case READY -> { }
        }
    }

    private static OwnerIdentity ownerIdentity(TargetWorkerServiceConfiguration.Source config, OwnerLease lease) {
        if (config.typedSource() instanceof TargetWorkerServiceConfiguration.KafkaSource kafka) {
            return new OwnerIdentity(kafka.deployment().source().deploymentId(), kafka.deployment().source().workerRunId(),
                    lease.ownerEpoch(), lease.leaseToken());
        }
        final var pulsar = ((TargetWorkerServiceConfiguration.PulsarSource) config.typedSource()).deployment().source();
        return new OwnerIdentity(pulsar.deploymentId(), pulsar.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
    }

    private static void requireCompleteChannels(Source source) {
        final var configured = new HashMap<String, TargetWorkerServiceConfiguration.Channel>();
        for (var channel : source.configuration.channels()) configured.put(Bytes.hex(channel.digest()), channel);
        for (var channel : source.inventory.entries()) {
            final var key = Bytes.hex(channel.identity().digest());
            final var selected = configured.remove(key);
            if (channel.currentOpen() && selected == null) {
                throw new IllegalArgumentException("current OPEN Source channel is absent from the service manifest");
            }
            if (selected != null && !channel.currentOpen()
                    && (!(selected.typedConfiguration() instanceof TargetWorkerServiceConfiguration.KafkaReceipt)
                        || source.inputs.receipts().get(key).recoveryAdmission() == null)) {
                throw new IllegalArgumentException("historical channel requires its exact K2 recovery Admission");
            }
        }
        if (!configured.isEmpty()) throw new IllegalArgumentException("configured channel is absent from the complete Source inventory");
    }

    private void advanceManagement(Source source) {
        if (source.managementIndex == source.configuration.channels().size()) { source.stage = Stage.CHANNELS; return; }
        final var channel = source.configuration.channels().get(source.managementIndex);
        if (!(channel.typedConfiguration() instanceof TargetWorkerServiceConfiguration.KafkaReceipt)) {
            source.managementIndex++; return;
        }
        final var key = Bytes.hex(channel.digest());
        var opening = managementOwners.get(channel.managementFile());
        if (opening == null) {
            final var authority = source.inputs.receipts().get(key).management();
            opening = KafkaClientArtifactTargetReceiptManagementFactory.openConfigured(process, channel.typedManagement(), sdk,
                    authority);
            managementOwners.put(channel.managementFile(), opening);
        }
        source.management.put(key, opening);
        if (opening.failure().isPresent()) throw new IllegalStateException("original K2 management opening failed", opening.failure().orElseThrow());
        if (opening.ready()) source.managementIndex++;
    }

    private void advanceChannel(Source source) {
        if (source.channelIndex == source.configuration.channels().size()) { source.stage = Stage.JOURNAL; return; }
        final var channel = source.configuration.channels().get(source.channelIndex);
        if (source.channel == null && source.receipt == null) {
            if (channel.typedConfiguration() instanceof TargetWorkerServiceConfiguration.KafkaBaseline baseline) {
                source.channel = KafkaClientArtifactTargetBaselineFactory.openConfigured(source.startup, source.dispatch, channel.digest(),
                        baseline.configuration(), dependencies.mtls(), dependencies.profiles(), dependencies.targetTrust(), dependencies.time(),
                        dependencies.reads(), sdk);
            } else if (channel.typedConfiguration() instanceof TargetWorkerServiceConfiguration.KafkaReceipt receipt) {
                final var key = Bytes.hex(channel.digest());
                final var management = source.management.get(key);
                source.startup.requireReceiptManagement(management);
                if (!Arrays.equals(receipt.configuration().protectionDigest(), management.protection(channel.digest()).digest())) {
                    throw new IllegalArgumentException("configured K2 protection differs from the original process backing");
                }
                source.receipt = KafkaClientArtifactTargetReceiptFactory.openLoaded(source.startup, source.worker, source.dispatch,
                        channel.digest(), receipt.configuration(), source.inputs.receipts().get(key).recoveryAdmission(), dependencies.mtls(),
                        dependencies.profiles(), dependencies.targetTrust(), management.catalog(), management.publisher(channel.digest()),
                        sdk, dependencies.time(), dependencies.reads(), dependencies.ownerClock());
            } else {
                final var config = channel.typedConfiguration() instanceof TargetWorkerServiceConfiguration.PulsarBaseline baseline
                        ? baseline.configuration() : ((TargetWorkerServiceConfiguration.PulsarManaged) channel.typedConfiguration()).configuration();
                if (source.channelBackend == null) {
                    source.channelActivation = source.startup.preparePublicationChannel(channel.digest(), dependencies.mtls(),
                            dependencies.profiles(), dependencies.targetTrust(), dependencies.reads(), dependencies.time());
                    final var activation = source.channelActivation;
                    final var client = channel.typedPulsarClient();
                    final var binding = TargetDestinationCredentialChecks.requireBinding(activation.channel(), activation.physical(),
                            dependencies.profiles(), dependencies.targetTrust());
                    source.channelBackend = PulsarClientArtifactOwnedBackendGroup.openVerifiedMtlsForStartup(client.settings(), binding,
                            client.sdkJar(), activation.verifiedMaterial(), dependencies.time(), activation, sdk, client.cost(),
                            source.startup, source.worker);
                    return;
                }
                if (!source.channelBackend.finishStartup()) return;
                final var activation = source.channelActivation;
                final var opening = PulsarClientArtifactTargetProducerFactory.opening(source.channelBackend, activation.channel(),
                        activation.physical(), config.timeout(), config.maximumPublicationBytes(), activation, config.nativeEnabled());
                source.channel = source.startup.openPublicationChannel(source.dispatch, activation.channel(), activation.physical(), sdk,
                        config.cost(), config.maximumPublicationBytes(), opening, dependencies.reads());
            }
            return;
        }
        final boolean ready = source.channel != null ? source.startup.finishPublicationChannel(source.channel)
                : source.receipt.advance().status() == KafkaClientArtifactTargetReceiptRuntime.Status.READY;
        if (ready) {
            source.channel = null; source.receipt = null; source.channelActivation = null; source.channelBackend = null;
            source.channelIndex++;
        }
    }

    private Source original(TargetWorkerShardRuntime worker) {
        final var source = workers.get(worker);
        if (source == null || source.worker != worker) throw new IllegalArgumentException("service request changed its original Source Worker");
        return source;
    }

    private TargetSourceBacklogRead observe(TargetWorkerShardRuntime worker, TargetSourceBacklog.Cut cut) {
        final var source = original(worker);
        return source.reads.observe(worker, cut);
    }

    private void backgroundFailure(Throwable failure) {
        retainFailure(failure);
        state = State.FAILED;
        // Quarantine fences the existing loops without awaiting SDK futures on their callback thread.
        final var original = process;
        if (original != null) {
            try { original.host().ifPresent(TargetWorkerHostRuntime::quarantineOwnedBackends); }
            catch (RuntimeException | Error cleanup) { retainFailure(cleanup); }
        }
        // Caller-driven advance/stop settles replay exit and reports completion only after actual ownership exits.
        dependencies.failures().accept(failure);
    }

    private void fail(Throwable failure) {
        while ((failure instanceof TargetWorkerProcessRuntime.StartupFailure
                || failure instanceof TargetWorkerSourceStartup.StartupFailure)
                && failure.getCause() != null) failure = failure.getCause();
        retainFailure(failure);
        state = State.FAILED;
        if (process == null) return;
        if (process.cleanupComplete()) return;
        // The original process fences all retained Sources, including one whose constructor did not return.
        try {
            if (process.host().isPresent()) process.exitForReplay(exitRequest(), dependencies.ownerClock());
            else process.failControlledStartup(firstFailure, exitRequest());
        } catch (RuntimeException | Error cleanup) {
            if (cleanup != firstFailure) firstFailure.addSuppressed(cleanup);
        }
    }

    private void retainFailure(Throwable failure) {
        synchronized (failureGate) {
            if (firstFailure == null) firstFailure = Objects.requireNonNull(failure);
            else if (firstFailure != failure) firstFailure.addSuppressed(failure);
        }
    }

    private TargetOwnerDrainCoordinator.Request exitRequest() {
        return stopRequest == null ? dependencies.exitRequest() : stopRequest;
    }

    /** One original Host drain turn; repeated calls settle the same in-flight requests and ownership. */
    public synchronized boolean stop(TargetOwnerDrainCoordinator.Request request) {
        Objects.requireNonNull(request);
        stopRequest = request;
        if (state == State.TERMINATED) return true;
        if (state == State.STARTING) fail(new java.util.concurrent.CancellationException("configured service stopped during startup"));
        else if (state == State.RUNNING) state = State.DRAINING;
        advanceStop();
        return state == State.TERMINATED;
    }

    /** Fault stop always reuses the retained original Process/Host quarantine and replay exit. */
    public synchronized boolean stopForReplay(Throwable cause, TargetOwnerDrainCoordinator.Request request) {
        Objects.requireNonNull(request);
        stopRequest = request;
        fail(cause);
        advanceStop();
        return cleanupComplete();
    }

    private void advanceStop() {
        if (process == null) return;
        if (process.cleanupComplete()) {
            if (state == State.DRAINING) state = State.TERMINATED;
            return;
        }
        try {
            if (state == State.DRAINING) {
                if (process.drain(exitRequest(), dependencies.ownerClock()).complete()) state = State.TERMINATED;
            } else if (state == State.FAILED) {
                if (process.host().isPresent()) process.exitForReplay(exitRequest(), dependencies.ownerClock());
                else process.failControlledStartup(firstFailure, exitRequest());
            }
        } catch (RuntimeException | Error failure) {
            retainFailure(failure);
            state = State.FAILED;
        }
    }

    public synchronized State state() { return state; }
    public synchronized Optional<Throwable> firstFailure() { return Optional.ofNullable(firstFailure); }
    public synchronized boolean cleanupComplete() { return process != null && process.cleanupComplete(); }
    public synchronized Optional<TargetWorkerProcessRuntime> process() { return Optional.ofNullable(process); }

    public static final class StartupFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;
        private final transient TargetWorkerConfiguredService root;
        private StartupFailure(Throwable cause, TargetWorkerConfiguredService root) {
            super("configured service failed; original startup and native cleanup remain retained", cause); this.root = root;
        }
        public TargetWorkerConfiguredService root() { return root; }
    }
}
