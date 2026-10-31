package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetExpiryDiscoveryStore;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetNativePolicyTrustStore;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetReservationControls;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentAuthority;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Active-owner Target source and maintenance on one Worker resource graph.
 *
 * <p>The host drives bounded turns and owns Owner drain and native source teardown. The shared
 * resource envelope gates both turns before source poll or GC task submission.
 */
public final class TargetWorkerShardRuntime
        implements TargetWorkerShardFleetRuntime.ShardTurns, TargetWorkerHostRuntime.Shard {
    /** Authority inputs for this Owner's Close and ordinary expiry maintenance. */
    public record Maintenance(
            TargetReservationControls.Authority controls,
            TargetReservationClosureWorkClassExecutor.Limits closeLimits,
            TargetReservationExpiryWorkClassExecutor.Limits expiryLimits,
            TargetStoreBackend.CommitAuthority physicalWrites,
            TargetQuotaDelta.ReservationClosureAuthority closureQuota,
            TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            TargetQuotaDelta.CloseCursorAuthority cursorQuota,
            LongSupplier ownerClock) {
        public Maintenance {
            Objects.requireNonNull(controls, "controls");
            Objects.requireNonNull(closeLimits, "closeLimits");
            Objects.requireNonNull(expiryLimits, "expiryLimits");
            Objects.requireNonNull(physicalWrites, "physicalWrites");
            Objects.requireNonNull(closureQuota, "closureQuota");
            Objects.requireNonNull(expiryQuota, "expiryQuota");
            Objects.requireNonNull(cursorQuota, "cursorQuota");
            Objects.requireNonNull(ownerClock, "ownerClock");
        }
    }

    /** Per-turn proof and signing inputs for scheduled message-expiry discovery. */
    public record MessageExpiryRequest(
            BoundedReadBudget discoveryBudget,
            TrustedUtcIntervalEvidence evidence,
            long retryUntilEpochMs,
            OwnerIdentity owner,
            int signingKeyVersion,
            PrivateKey signingKey) {
        public MessageExpiryRequest {
            Objects.requireNonNull(discoveryBudget, "discoveryBudget");
            Objects.requireNonNull(evidence, "evidence");
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(signingKey, "signingKey");
        }
    }

    private final ShardId shardId;
    private final WorkClassExecutionRegistry workClasses;
    private final SharedRocksDbResources resources;
    private final ShardStore store;
    private final WorkerSourceApplyLoop sourceLoop;
    private final TargetSourceApplyRuntime target;
    private final TargetReservationGcRuntime maintenance;
    private final TargetOwnerDrainCoordinator drainCoordinator;
    private final LongSupplier ownerClock;
    private volatile boolean sourceAndMaintenancePaused;
    private volatile boolean backendQuarantined;
    private final java.util.concurrent.atomic.AtomicLong nativeExposureAdmissionEpoch =
            new java.util.concurrent.atomic.AtomicLong();
    private volatile TargetWorkerHostRuntime publicationHost;
    private final TargetOwnedBackendGroup sourceBackendGroup;
    private final SourceRecordConsumer actualSourceConsumer;
    private TargetOwnedBackendGroup recoveryBackend;
    private TargetOwnedBackendGroup priorRecoveryBackend;
    private final java.util.Set<TargetOwnedBackendGroup> recoveryInputBackends =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private volatile TargetMessageExpiryWorkClassExecutor messageExpiryHandoff;
    private volatile TargetMessageExpiryMaintenance messageExpiryMaintenance;
    private volatile TargetCheckpointCandidateWorkClassExecutor.Submission pendingCheckpoint;
    private boolean checkpointTurnInProgress;
    private com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend evidenceRecoveryCatalog;
    private TargetPublishRecoveryMaintenance publishRecoveryMaintenance;
    private volatile TargetJournalNegativeSettlementMaintenance journalSettlement;
    private volatile TargetPulsarPublishRuntime publishRuntime;
    private volatile TargetChannelControlWorkClassExecutor channelControlHandoff;
    private volatile TargetEvidenceRetirementWorkClassExecutor retirementHandoff;
    private volatile TargetResourceReclaimMaintenance resourceReclaim;

    synchronized void registerResourceReclaimMaintenance(
            TargetResourceReclaimMaintenance original, OwnedSourceSystemWriter writer) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (resourceReclaim != null
                || writer.sourceWriterWorker() != this
                || writer.resourcesTerminated()
                || !writer.readyForAppending()
                || systemWriterBackends.values().stream().noneMatch(retained -> retained == writer)) {
            throw new IllegalStateException(
                    "Native GC requires the original admitted Source writer and one maintenance owner");
        }
        target.registerResourceReclaimMaintenance(Objects.requireNonNull(original));
        resourceReclaim = original;
    }

    @Override
    public Optional<TargetResourceReclaimMaintenance.Turn> runResourceReclaimMaintenanceTurn() {
        final var original = resourceReclaim;
        return original == null ? Optional.empty() : Optional.of(original.runTurn());
    }

    @Override
    public void requestResourceReclaimStop() {
        final var original = resourceReclaim;
        if (original != null) {
            original.requestStop();
        }
    }

    @Override
    public Optional<WorkClassTask> settlePendingResourceReclaimForDrain() {
        final var original = resourceReclaim;
        if (original == null) {
            return Optional.empty();
        }
        original.requestStop();
        original.runTurn();
        return original.ioExited()
                ? Optional.empty()
                : Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-native-reclaim-exit/" + shardId(),
                        1));
    }

    private SourceRecordConsumer.CheckpointCut preparedCheckpointCut;

    TargetWorkerShardRuntime(
            final SourceRecordConsumer consumer,
            final WorkClassExecutionRegistry workClasses,
            final ShardStore store,
            final SharedRocksDbResources resources,
            final TargetSourceApplyRuntime target,
            final Maintenance maintenanceInputs) {
        final var exactClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.workClasses = exactClasses;
        final var exactStore = Objects.requireNonNull(store, "store");
        this.store = exactStore;
        this.resources = Objects.requireNonNull(resources, "resources");
        this.resources.requireWorkerActivationReady();
        final var exactTarget = Objects.requireNonNull(target, "target");
        this.target = exactTarget;
        final var inputs = Objects.requireNonNull(maintenanceInputs, "maintenanceInputs");
        ownerClock = inputs.ownerClock();
        exactTarget.requireWorkerStore(exactStore, this.resources);
        this.resources.bindWorkClassExecutionRegistry(exactClasses);
        shardId = exactStore.shardId();
        sourceBackendGroup = consumer.ownedBackendGroup();
        actualSourceConsumer = consumer;
        exactTarget.bindSourceFrameDecode(consumer);
        sourceLoop = new WorkerSourceApplyLoop(Objects.requireNonNull(consumer, "consumer"), exactClasses, exactTarget);
        maintenance = exactTarget.newReservationGcRuntime(
                exactClasses,
                inputs.controls(),
                inputs.closeLimits(),
                inputs.expiryLimits(),
                inputs.physicalWrites(),
                inputs.closureQuota(),
                inputs.expiryQuota(),
                inputs.cursorQuota(),
                inputs.ownerClock());
        drainCoordinator =
                new TargetOwnerDrainCoordinator(exactStore, this.resources, exactTarget, sourceLoop, maintenance);
    }

    void bindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        target.bindTargetQueueChangeSignal(signal);
    }

    void unbindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        target.unbindTargetQueueChangeSignal(signal);
    }

    void bindPublicationHost(final TargetWorkerHostRuntime host) {
        if (publicationHost != null && publicationHost != host) {
            throw new IllegalStateException("Target Worker publication resources already belong to another Host");
        }
        publicationHost = Objects.requireNonNull(host, "host");
    }

    void unbindPublicationHost(final TargetWorkerHostRuntime host) {
        if (publicationHost == host) {
            publicationHost = null;
        }
    }

    synchronized void configureTargetQueueHeadCache(final int maximumEntries) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.configureTargetQueueHeadCache(maximumEntries);
    }

    public ShardId shardId() {
        return shardId;
    }

    public synchronized com.nereusstream.delay.protocol.CanonicalTargetPartition requirePublicationChannel(
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            boolean newWork,
            BoundedReadBudget reads,
            LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.requirePublicationChannel(channel, newWork, reads, clock);
    }

    public synchronized com.nereusstream.delay.protocol.CanonicalTargetPartition requireRetainedPublicationChannel(
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            BoundedReadBudget reads,
            LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.requireRetainedPublicationChannel(channel, reads, clock);
    }

    public synchronized boolean publicationChannelOpen(
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            BoundedReadBudget reads,
            LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.publicationChannelOpen(channel, reads, clock);
    }

    public synchronized com.nereusstream.delay.protocol.TargetQuotaAccounting readPublicationAccounting(
            BoundedReadBudget reads,
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readPublicationAccounting(reads, channel, clock);
    }

    public interface PublicationResources {
        boolean settleClose();

        boolean closed();

        boolean readyForPublishing();

        default boolean recoveryOnly() {
            return false;
        }

        default boolean readyForRecovery() {
            return false;
        }

        default TargetOwnedBackendGroup ownedBackendGroup() {
            return null;
        }

        default com.nereusstream.delay.protocol.TargetChannelIdentity channelIdentity() {
            return null;
        }

        default LocalBrokerMtlsResolver.Receipt credentialReceipt() {
            return null;
        }

        default LocalBrokerMtlsResolver.TargetRecoveryReceipt recoveryCredentialReceipt() {
            return null;
        }
    }
    /** One original resource-preparation operation, separately retained after its old SDK has closed. */
    public interface PublicationMaintenance {
        PublicationResources originalResources();

        boolean runTurn(TargetWorkerHostRuntime host);

        void requestStop();

        boolean ioExited();
    }

    private final java.util.Map<String, PublicationResources> publicationResources = new java.util.HashMap<>();
    private final java.util.Map<String, PublicationMaintenance> publicationMaintainers =
            new java.util.LinkedHashMap<>();
    private int publicationMaintenanceCursor;
    private boolean publicationMaintenanceStopping;
    private final java.util.Map<String, OwnedSourceSystemWriter> systemWriterBackends = new java.util.HashMap<>();
    private final java.util.List<TargetOwnedBackendGroup> preparedPublicationBackends = new java.util.ArrayList<>();
    private OwnedTargetJournal journalBackend;
    private TargetWorkerChannelInventory channelInventory;

    synchronized TargetWorkerChannelInventory newChannelInventory(
            TargetWorkerChannelInventory.Limits limits, LongSupplier clock, LongSupplier nanos) {
        requirePublishRecoveryOwner(clock);
        return target.newChannelInventory(limits, clock, nanos);
    }

    synchronized void retainChannelInventory(TargetWorkerChannelInventory original) {
        original.requireOwner(target);
        if (channelInventory != null && channelInventory != original) {
            throw new IllegalStateException("original channel inventory cannot be replaced");
        }
        channelInventory = original;
    }

    synchronized TargetWorkerChannelInventory.Entry requireOpenPublicationChannel(byte[] digest) {
        return requirePublicationInventoryChannel(digest, true);
    }

    synchronized TargetWorkerChannelInventory.Entry requirePublicationInventoryChannel(byte[] digest, boolean newWork) {
        final var original = Objects.requireNonNull(channelInventory, "complete original Source channel inventory");
        original.requireOwner(target);
        return newWork ? original.requireOpen(digest) : original.requireRetained(digest);
    }

    synchronized void requirePublicationResource(PublicationResources original) {
        final var channel = Objects.requireNonNull(original.channelIdentity(), "original channel identity");
        if (publicationResources.get(com.nereusstream.delay.protocol.Bytes.hex(channel.digest())) != original) {
            throw new IllegalArgumentException(
                    "publication resource is not this Worker's original registered SDK owner");
        }
    }

    /** Only an already registered original SDK may retain these read checks across a periodic checkpoint. */
    public synchronized PublicationReads publicationReads(PublicationResources original) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        requirePublicationResource(original);
        if (original.closed()) {
            throw new IllegalStateException("original publication SDK has exited");
        }
        return new PublicationReads(this, original, original.channelIdentity());
    }

    /** Resource-bound read-only view; no Source mutation, Claim, registration or SEND admission entry is exposed. */
    public static final class PublicationReads {
        private final TargetWorkerShardRuntime worker;
        private final PublicationResources original;
        private final com.nereusstream.delay.protocol.TargetChannelIdentity channel;

        private PublicationReads(
                TargetWorkerShardRuntime worker,
                PublicationResources original,
                com.nereusstream.delay.protocol.TargetChannelIdentity channel) {
            this.worker = worker;
            this.original = original;
            this.channel = channel;
        }

        private void requireOriginal() {
            worker.requireRuntimeAdmitted();
            worker.resources.requireRuntimeBusinessAdmission();
            worker.requirePublicationResource(original);
            if (original.closed()) {
                throw new IllegalStateException("original publication SDK has exited");
            }
        }

        public void requireBinding(
                TargetWorkerShardRuntime expected, com.nereusstream.delay.protocol.TargetChannelIdentity identity) {
            synchronized (worker) {
                if (worker != expected) {
                    throw new IllegalArgumentException("publication reads belong to another original Worker");
                }
                channel.requireExactFrozenIdentity(identity);
                requireOriginal();
            }
        }

        public OwnerIdentity requireOwner(OwnerIdentity expected, LongSupplier clock) {
            synchronized (worker) {
                requireOriginal();
                final var actual = worker.target.requirePublicationOwner(expected, clock);
                requireOriginal();
                return actual;
            }
        }

        public com.nereusstream.delay.protocol.CanonicalTargetPartition requireOpenChannel(
                com.nereusstream.delay.protocol.TargetChannelIdentity identity,
                BoundedReadBudget budget,
                LongSupplier clock) {
            synchronized (worker) {
                channel.requireExactFrozenIdentity(identity);
                requireOriginal();
                final var actual = worker.target.requirePublicationChannel(identity, true, budget, clock);
                requireOriginal();
                return actual;
            }
        }

        public com.nereusstream.delay.protocol.CanonicalTargetPartition requireRetainedChannel(
                com.nereusstream.delay.protocol.TargetChannelIdentity identity,
                BoundedReadBudget budget,
                LongSupplier clock) {
            synchronized (worker) {
                channel.requireExactFrozenIdentity(identity);
                requireOriginal();
                final var actual = worker.target.requireRetainedPublicationChannel(identity, budget, clock);
                requireOriginal();
                return actual;
            }
        }

        public com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption readAdoption(
                BoundedReadBudget budget,
                com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
                LongSupplier clock) {
            synchronized (worker) {
                channel.requireExactFrozenIdentity(mapping.admission().channel());
                requireOriginal();
                final var actual = worker.target.readExternalEvidenceAdoption(budget, mapping, clock);
                requireOriginal();
                return actual;
            }
        }

        public com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaRecoveryPending readRecoveryPending(
                BoundedReadBudget budget,
                SystemMutation admission,
                com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
                LongSupplier clock) {
            synchronized (worker) {
                channel.requireExactFrozenIdentity(mapping.admission().channel());
                requireOriginal();
                if (!original.recoveryOnly()
                        || worker.publishRuntime != null
                                && worker.publishRuntime.ownsAttempt(
                                        mapping.admission().publishAttemptId())) {
                    throw new IllegalStateException("K2 recovery cannot fence the current normal publication flight");
                }
                final var actual = worker.target.readKafkaRecoveryPending(budget, admission, mapping, clock);
                requireOriginal();
                return actual;
            }
        }

        public com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaUnknown readKafkaUnknown(
                BoundedReadBudget budget, SystemMutation admission, LongSupplier clock) {
            synchronized (worker) {
                final var body =
                        com.nereusstream.delay.protocol.TargetPublishAdmissionBody.decode(admission.canonicalBody());
                channel.requireExactFrozenIdentity(body.channel());
                requireOriginal();
                if (!original.recoveryOnly()
                        || worker.publishRuntime != null
                                && worker.publishRuntime.ownsAttempt(body.publishAttemptId())) {
                    throw new IllegalStateException("archival recovery cannot use a live normal publication flight");
                }
                final var actual = worker.target.readKafkaUnknown(budget, admission, clock);
                requireOriginal();
                return actual;
            }
        }

        public Optional<com.nereusstream.delay.runtime.SystemMutationResult> recoveryOutcomeResult(
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery snapshot,
                SystemMutation image,
                LongSupplier clock) {
            synchronized (worker) {
                channel.requireExactFrozenIdentity(
                        snapshot.verified().proof().mapping().admission().channel());
                snapshot.requireExact(image);
                requireOriginal();
                final var actual = worker.target.outcomeMutationResult(image, clock);
                requireOriginal();
                return actual;
            }
        }

        public com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption readRecoveryAdoption(
                BoundedReadBudget reads,
                com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery snapshot,
                SystemMutation image,
                LongSupplier clock) {
            synchronized (worker) {
                channel.requireExactFrozenIdentity(
                        snapshot.verified().proof().mapping().admission().channel());
                snapshot.requireExact(image);
                requireOriginal();
                final var actual = worker.target.readRecoveryEvidenceAdoption(reads, snapshot, image, clock);
                requireOriginal();
                return actual;
            }
        }
    }

    public synchronized void registerPublicationMaintenance(
            PublicationResources original,
            PublicationMaintenance maintenance,
            TargetDestinationSdkBudget budget,
            LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        budget.requireWorker(workClasses, resources);
        requirePublicationResource(original);
        final String id = com.nereusstream.delay.protocol.Bytes.hex(
                original.channelIdentity().digest());
        if (publicationMaintenanceStopping || maintenance.originalResources() != original) {
            throw new IllegalStateException("publication maintenance changed its original owned SDK or is stopping");
        }
        if (publicationMaintainers.get(id) == maintenance) {
            return;
        }
        if (publicationMaintainers.size() >= budget.maximumClients()
                || publicationMaintainers.putIfAbsent(id, maintenance) != null) {
            throw new IllegalStateException("original publication maintenance window is full or already registered");
        }
    }

    @Override
    public Optional<WorkClassTask> runPublicationResourceMaintenanceTurn() {
        final java.util.Map.Entry<String, PublicationMaintenance> selected;
        final TargetWorkerHostRuntime host;
        synchronized (this) {
            requireNewTurnsAdmitted();
            if (publicationMaintenanceStopping || publicationMaintainers.isEmpty() || publicationHost == null) {
                return Optional.empty();
            }
            final var entries = new java.util.ArrayList<>(publicationMaintainers.entrySet());
            if (publicationMaintenanceCursor >= entries.size()) {
                publicationMaintenanceCursor = 0;
            }
            selected = entries.get(publicationMaintenanceCursor++);
            host = publicationHost;
        }
        // Never hold Worker while observing a controller that can call back into the same Worker.
        if (selected.getValue().runTurn(host)) {
            if (!selected.getValue().ioExited()) {
                throw new IllegalStateException("publication maintenance returned before its original I/O exit");
            }
            synchronized (this) {
                publicationMaintainers.remove(selected.getKey(), selected.getValue());
            }
        }
        return Optional.of(new WorkClassTask(
                com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                "target-publication-maintenance/" + selected.getKey(),
                1));
    }

    synchronized com.nereusstream.delay.adapter.PulsarAttemptJournal requireOwnedPublishing(
            OwnedSourceSystemWriter writer,
            TargetWorkerPublicationDispatch dispatch,
            TargetWorkerPublishingConfiguration configuration) {
        requirePublishRecoveryOwner(configuration.ownerClock());
        dispatch.requireWorker(this);
        if (publishRuntime != null
                || journalSettlement != null
                || publishRecoveryMaintenance != null
                || writer.sourceWriterWorker() != this
                || writer.resourcesTerminated()
                || systemWriterBackends.values().stream().noneMatch(original -> original == writer)) {
            throw new IllegalStateException(
                    "publishing requires fresh dependencies and this Worker's original Source appender");
        }
        requireChannelInventoryCoverage();
        requirePublicationResourcesReady();
        for (var owner : publicationResources.values()) {
            if (owner.closed()) {
                continue;
            }
            final var channel = Objects.requireNonNull(owner.channelIdentity(), "actual publication channel");
            if (!configuration.supports(channel.context().kind())) {
                throw new IllegalStateException("configured publisher does not cover its actual channel SDK owner");
            }
            if (owner.recoveryOnly()) {
                continue;
            }
            if (!(owner instanceof com.nereusstream.delay.adapter.DestinationPublishAdapter actual)) {
                throw new IllegalStateException("new-work SDK owner has no actual destination adapter");
            }
            dispatch.mux().requireEndpoint(channel, actual);
        }
        if (!configuration.supports(com.nereusstream.delay.protocol.ChannelKind.PULSAR_DEDUP_PRODUCER)) {
            return null;
        }
        if (journalBackend == null || journalBackend.journalWorker() != this) {
            throw new IllegalStateException("managed publishing requires its original registered owned Journal");
        }
        return journalBackend.journal();
    }

    private void requireChannelInventoryCoverage() {
        final var inventory = Objects.requireNonNull(channelInventory, "complete original Source channel inventory");
        inventory.requireOwner(target);
        final var open = new java.util.HashSet<String>();
        for (var entry : inventory.entries()) {
            if (!entry.currentOpen()) {
                continue;
            }
            final String id =
                    com.nereusstream.delay.protocol.Bytes.hex(entry.identity().digest());
            open.add(id);
            final var sdk = publicationResources.get(id);
            if (sdk == null
                    || sdk.closed()
                    || !(sdk.recoveryOnly() ? sdk.readyForRecovery() : sdk.readyForPublishing())) {
                throw new IllegalStateException(
                        "current OPEN Source channel lacks its actual SEND or original recovery SDK owner");
            }
            entry.identity().requireExactFrozenIdentity(Objects.requireNonNull(sdk.channelIdentity()));
        }
        for (var sdk : publicationResources.values()) {
            if (!sdk.closed()
                    && !sdk.recoveryOnly()
                    && !open.contains(com.nereusstream.delay.protocol.Bytes.hex(
                            Objects.requireNonNull(sdk.channelIdentity()).digest()))) {
                throw new IllegalStateException(
                        "actual SEND SDK owner is absent from the current OPEN Source inventory");
            }
        }
    }

    private void requirePublicationResourcesReady() {
        if (publicationResources.values().stream().noneMatch(owner -> !owner.closed())) {
            if (channelInventory == null || channelInventory.hasOpenChannels()) {
                throw new IllegalStateException(
                        "idle publication startup lacks its complete unchanged Source channel inventory");
            }
            return;
        }
        if (publicationResources.values().stream()
                .anyMatch(owner -> !owner.closed()
                        && !(owner.recoveryOnly() ? owner.readyForRecovery() : owner.readyForPublishing()))) {
            throw new IllegalStateException(
                    "configured service has absent or unfinished actual publication/recovery resources");
        }
    }

    public void registerJournalBackend(
            OwnedTargetJournal original, TargetDestinationSdkBudget budget, LongSupplier clock) {
        final TargetWorkerHostRuntime host;
        synchronized (this) {
            requirePublishRecoveryOwner(clock);
            budget.requireWorker(workClasses, resources);
            if (original.journalWorker() != this || journalBackend != null) {
                throw new IllegalStateException("the original managed Journal cannot be replaced");
            }
            original.ownedBackendGroup().requireWorkerGraph(workClasses, resources);
            journalBackend = original;
            host = publicationHost;
        }
        if (host != null) {
            host.registerPublicationBackend(this, original.ownedBackendGroup());
        }
    }

    public void registerSystemWriterBackend(
            String identity, OwnedSourceSystemWriter backend, TargetDestinationSdkBudget budget, LongSupplier clock) {
        final TargetWorkerHostRuntime host;
        synchronized (this) {
            requirePublishRecoveryOwner(clock);
            budget.requireWorker(workClasses, resources);
            backend.ownedBackendGroup().requireWorkerGraph(workClasses, resources);
            systemWriterBackends.entrySet().removeIf(entry -> entry.getValue().resourcesTerminated());
            if (backend.sourceWriterWorker() != this
                    || systemWriterBackends.size() >= budget.maximumClients()
                    || systemWriterBackends.putIfAbsent(identity, backend) != null) {
                throw new IllegalStateException("Source writer generation is already owned or full");
            }
            host = publicationHost;
        }
        if (host != null) {
            host.registerPublicationBackend(this, backend.ownedBackendGroup());
        }
    }

    public void registerPublicationResources(
            String identity, PublicationResources owner, TargetDestinationSdkBudget budget, LongSupplier clock) {
        final var actual = Objects.requireNonNull(owner, "publication owner");
        final TargetWorkerHostRuntime host;
        synchronized (this) {
            requirePublishRecoveryOwner(clock);
            budget.requireWorker(workClasses, resources);
            publicationResources.entrySet().removeIf(entry -> entry.getValue().closed());
            for (var retained : publicationResources.values()) {
                if ((actual.recoveryOnly() || retained.recoveryOnly())
                        && actual.channelIdentity() != null
                        && retained.channelIdentity() != null
                        && java.util.Arrays.equals(
                                actual.channelIdentity().context().producerIdentity(),
                                retained.channelIdentity().context().producerIdentity())) {
                    throw new IllegalStateException(
                            "K2 recovery and another SDK cannot own the same stable producer identity");
                }
            }
            if (publicationResources.size() >= budget.maximumClients()
                    || publicationResources.putIfAbsent(identity, actual) != null) {
                throw new IllegalStateException(
                        "publication resource generation is already owned or its window is exhausted");
            }
            host = publicationHost;
        }
        // Do not acquire the Host monitor while holding a Worker/native-read monitor.
        final var backend = actual.ownedBackendGroup();
        if (host != null && backend != null) {
            host.registerPublicationBackend(this, backend);
        }
    }

    /** Retains a concrete pre-Host SDK owner before its first native opening can be queued. */
    public synchronized void retainPublicationBackend(
            TargetOwnedBackendGroup original, TargetDestinationSdkBudget budget) {
        Objects.requireNonNull(original).requireWorkerGraph(workClasses, resources);
        Objects.requireNonNull(budget).requireWorker(workClasses, resources);
        resources.requireRuntimeBusinessAdmission();
        if (publicationHost != null || backendQuarantined || publicationMaintenanceStopping) {
            throw new IllegalStateException("original publication construction is no longer admitted");
        }
        preparedPublicationBackends.removeIf(TargetOwnedBackendGroup::resourcesTerminated);
        if (preparedPublicationBackends.stream().anyMatch(value -> value == original)) {
            return;
        }
        if (preparedPublicationBackends.size() >= budget.maximumClients()) {
            throw new IllegalStateException("original prepared publication owner window is full");
        }
        preparedPublicationBackends.add(original);
    }

    synchronized java.util.List<TargetOwnedBackendGroup> publicationBackendGroups() {
        final var groups = new java.util.ArrayList<TargetOwnedBackendGroup>();
        groups.addAll(preparedPublicationBackends);
        if (journalBackend != null) {
            groups.add(journalBackend.ownedBackendGroup());
        }
        if (recoveryBackend != null && !groups.contains(recoveryBackend)) {
            groups.add(recoveryBackend);
        }
        if (priorRecoveryBackend != null && !groups.contains(priorRecoveryBackend)) {
            groups.add(priorRecoveryBackend);
        }
        for (var backend : recoveryInputBackends) {
            if (!groups.contains(backend)) {
                groups.add(backend);
            }
        }
        for (var owner : publicationResources.values()) {
            final var group = owner.ownedBackendGroup();
            if (group != null && groups.stream().noneMatch(existing -> existing == group)) {
                groups.add(group);
            }
        }
        for (var backend : systemWriterBackends.values()) {
            final var group = backend.ownedBackendGroup();
            if (groups.stream().noneMatch(existing -> existing == group)) {
                groups.add(group);
            }
        }
        return java.util.List.copyOf(groups);
    }

    private Optional<WorkClassTask> stopPublicationMaintenanceForExit() {
        final java.util.List<java.util.Map.Entry<String, PublicationMaintenance>> maintainers;
        synchronized (this) {
            publicationMaintenanceStopping = true;
            maintainers = new java.util.ArrayList<>(publicationMaintainers.entrySet());
        }
        for (var entry : maintainers) {
            entry.getValue().requestStop();
        }
        for (var entry : maintainers) {
            if (!entry.getValue().ioExited()) {
                return Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-publication-maintenance-exit/" + entry.getKey(),
                        1));
            }
            synchronized (this) {
                publicationMaintainers.remove(entry.getKey(), entry.getValue());
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<WorkClassTask> settlePendingPublicationResourcesForDrain() {
        final var preparing = stopPublicationMaintenanceForExit();
        if (preparing.isPresent()) {
            return preparing;
        }
        final java.util.List<java.util.Map.Entry<String, PublicationResources>> original;
        synchronized (this) {
            original = new java.util.ArrayList<>(publicationResources.entrySet());
        }
        for (var entry : original) {
            if (!entry.getValue().settleClose()) {
                return Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-publication-resources/" + entry.getKey(),
                        1));
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<WorkClassTask> settlePendingJournalResourcesForDrain() {
        final OwnedTargetJournal original;
        synchronized (this) {
            original = journalBackend;
            if (original == null) {
                return Optional.empty();
            }
            if (publishRuntime != null && !publishRuntime.available()
                    || journalSettlement != null
                            && !journalSettlement.completedForSource(store.shardMutationSequence())) {
                return Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-journal-final-settlement/" + shardId,
                        1));
            }
        }
        return original.settleClose()
                ? Optional.empty()
                : Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-journal-close/" + shardId,
                        1));
    }

    /**
     * Actual native Store/Source/Oxia assignment-session guard for external publication resources; never SEND
     * permission.
     */
    public synchronized com.nereusstream.delay.protocol.OwnerIdentity requirePublicationOwner(
            com.nereusstream.delay.protocol.OwnerIdentity expected, LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.requirePublicationOwner(expected, clock);
    }

    /** Only the actual owned Source can attach a parsed deployment receipt to this accepted Worker graph. */
    public synchronized SourceTransportCredentialUse bindSourceCredential(
            LocalBrokerMtlsResolver.SourceReceipt receipt,
            SourceRecordConsumer consumer,
            SourceAssignment assignment,
            com.nereusstream.delay.protocol.OwnerIdentity owner,
            Object nativeGeneration,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            LongSupplier clock) {
        if (consumer != actualSourceConsumer
                || sourceBackendGroup == null
                || !sourceBackendGroup.holdsSourceCredential(receipt)) {
            throw new IllegalStateException("Source credential binding lacks this actual owned consumer");
        }
        target.requireAcceptedAssignment(assignment);
        requirePublicationOwner(owner, clock);
        receipt.authorization().requireAssignment(assignment);
        receipt.requireActive(time.get());
        return new SourceTransportCredentialUse(receipt, target, assignment, owner, nativeGeneration, time, clock);
    }

    public synchronized SourceTransportCredentialUse bindSystemWriterCredential(
            LocalBrokerMtlsResolver.SourceReceipt receipt,
            SourceAssignment assignment,
            com.nereusstream.delay.protocol.OwnerIdentity owner,
            Object nativeGeneration,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            LongSupplier clock) {
        target.requireAcceptedAssignment(assignment);
        requirePublicationOwner(owner, clock);
        receipt.authorization().requireAssignment(assignment);
        receipt.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        return new SourceTransportCredentialUse(receipt, target, assignment, owner, nativeGeneration, time, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption
            readExternalEvidenceAdoption(
                    BoundedReadBudget reads,
                    com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
                    LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readExternalEvidenceAdoption(reads, mapping, clock);
    }
    /** Current admitted Root read for live checkpoint reconciliation; no old observer SDK/material is required. */
    public synchronized com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption
            readRecoveryEvidenceAdoption(
                    BoundedReadBudget reads,
                    com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery
                            snapshot,
                    SystemMutation image,
                    LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readRecoveryEvidenceAdoption(reads, snapshot, image, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses readClosedRecoveryUses(
            BoundedReadBudget reads,
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readClosedRecoveryUses(reads, closure, applied, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ControlClosure
            readControlReferenceClosure(
                    BoundedReadBudget reads,
                    com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ControlReferenceSnapshot
                            snapshot,
                    LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readControlReferenceClosure(reads, snapshot, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses readClosedControlUses(
            BoundedReadBudget reads,
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption applied,
            int kind,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readClosedControlUses(reads, closure, applied, kind, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetStagingEvidenceStore.ClosedUses readClosedStagingUses(
            BoundedReadBudget reads,
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readClosedStagingUses(reads, closure, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses readClosedControlUses(
            BoundedReadBudget reads,
            com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            com.nereusstream.delay.protocol.TargetExternalEvidenceOwner selected,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readClosedControlUses(reads, closure, selected, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent
            readAppliedRetirementIntent(BoundedReadBudget reads, SystemMutation selected, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readAppliedRetirementIntent(reads, selected, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedConfirmation
            readAppliedRetirementConfirmation(BoundedReadBudget reads, SystemMutation selected, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readAppliedRetirementConfirmation(reads, selected, clock);
    }

    public synchronized void requireRetirementReadCurrent(LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requirePublishRecoveryOwner(clock);
    }

    synchronized com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend originalEvidenceRecoveryCatalog() {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return evidenceRecoveryCatalog;
    }

    private void bindEvidenceRecoveryCatalog(TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication) {
        if (publication == null) {
            return;
        }
        publication.coordinator().managedEvidenceRecoveryCatalog().ifPresent(actual -> {
            target.bindOriginalEvidenceCatalog(actual);
            if (evidenceRecoveryCatalog == null) {
                evidenceRecoveryCatalog = actual;
            } else if (evidenceRecoveryCatalog != actual) {
                evidenceRecoveryCatalog.requireSameOriginalEvidenceAuthority(actual);
            }
        });
    }

    public synchronized void requireRetirementProtocolActive(LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireRetirementProtocolActive(clock);
    }

    public synchronized void requireRetirementProtocolActive(
            com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireRetirementProtocolActive(resource, clock);
    }

    public synchronized java.util.Optional<com.nereusstream.delay.runtime.SystemMutationResult>
            retirementMutationResult(SystemMutation mutation, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.retirementMutationResult(mutation, clock);
    }
    /** Only this Worker's original registered, Host-admitted Source writer may carry the retirement frame. */
    public TargetEvidenceRetirementWorkClassExecutor.Submission submitRetirementIntent(
            SystemMutation image,
            OwnedSourceSystemWriter writer,
            Runnable protection,
            LongSupplier clock,
            java.util.function.Consumer<TargetEvidenceRetirementWorkClassExecutor.Submission> retained) {
        if (image.type() != SystemMutationType.RESOURCE_RETIRE_INTENT) {
            throw new IllegalArgumentException("retirement intent type required");
        }
        return submitRetirement(image, writer, protection, clock, retained);
    }

    public TargetEvidenceRetirementWorkClassExecutor.Submission submitRetirementConfirmation(
            SystemMutation image,
            OwnedSourceSystemWriter writer,
            Runnable protection,
            LongSupplier clock,
            java.util.function.Consumer<TargetEvidenceRetirementWorkClassExecutor.Submission> retained) {
        if (image.type() != SystemMutationType.RESOURCE_DELETE_CONFIRMED) {
            throw new IllegalArgumentException("retirement confirmation type required");
        }
        return submitRetirement(image, writer, protection, clock, retained);
    }

    public synchronized com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Page discoverResourceReclaim(
            BoundedReadBudget reads,
            com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Cursor cursor,
            int maximumRows,
            LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverResourceReclaim(reads, cursor, maximumRows, clock);
    }
    /** Caller owns this synchronous operation on its original bounded I/O pool, not a maintenance or Store thread. */
    public TargetResourceReclaimSigner.Prepared prepareResourceReclaim(
            java.util.List<com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate> candidates,
            TargetResourceReclaimSigner signer,
            java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget recoveryReads,
            BoundedReadBudget nativeReads,
            java.util.function.Supplier<TrustedUtcIntervalEvidence> time,
            long retryUntil,
            long maximumInputBytes,
            Runnable originalHeld,
            LongSupplier clock) {
        if (Thread.holdsLock(this) || Thread.holdsLock(store) || Thread.holdsLock(target)) {
            throw new IllegalStateException(
                    "Native signing/authentication must run outside original Worker/Source/Store monitors");
        }
        final TargetSourceApplyRuntime.ReclaimPreparationRead source;
        synchronized (this) {
            requireNewTurnsAdmitted();
            resources.requireRuntimeBusinessAdmission();
            source = target.reclaimPreparationRead(candidates, clock);
        }
        return Objects.requireNonNull(signer)
                .prepare(source, images, recoveryReads, nativeReads, time, retryUntil, maximumInputBytes, originalHeld);
    }
    /** The former single-action entry cannot provide the original whole-flight preparation loan. */
    public com.nereusstream.delay.adapter.PayloadObjectDelete.Observation deletePendingObjectPayload(
            com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate candidate,
            com.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore provider,
            java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget recoveryReads,
            BoundedReadBudget nativeReads,
            long maximumInputBytes,
            Runnable originalHeld,
            LongSupplier clock) {
        throw new TargetSourceAuthorityPreparation.InputUnavailable(
                "pending OBJECT provider operation requires its original protected multi-turn preparation");
    }
    /** The actual maintenance caller uses beginPendingObjectPayload and retains its owned-I/O flight. */
    public com.nereusstream.delay.adapter.PayloadUncommittedDelete.Observation retirePendingUncommittedPayload(
            com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate candidate,
            com.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore provider,
            java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget recoveryReads,
            BoundedReadBudget nativeReads,
            long maximumInputBytes,
            Runnable originalHeld,
            LongSupplier clock) {
        throw new TargetSourceAuthorityPreparation.InputUnavailable(
                "pending uncommitted provider operation requires its original protected multi-turn preparation");
    }

    /** Captures the actual pending intent and Catalog receipt on the original owned I/O action. */
    PendingPayloadPreparation beginPendingObjectPayload(
            com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate candidate,
            com.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore provider,
            java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget recoveryReads,
            long maximumInputBytes,
            Runnable originalHeld,
            LongSupplier clock) {
        if (Thread.holdsLock(this) || Thread.holdsLock(store) || Thread.holdsLock(target)) {
            throw new IllegalStateException(
                    "pending provider preparation must run outside Worker/Source/Store monitors");
        }
        Objects.requireNonNull(candidate);
        Objects.requireNonNull(provider);
        Objects.requireNonNull(recoveryReads);
        Objects.requireNonNull(originalHeld);
        final TargetSourceApplyRuntime.ReclaimPreparationRead source;
        synchronized (this) {
            requireNewTurnsAdmitted();
            resources.requireRuntimeBusinessAdmission();
        }
        source = target.reclaimPreparationRead(java.util.List.of(candidate), clock);
        synchronized (this) {
            requireNewTurnsAdmitted();
            resources.requireRuntimeBusinessAdmission();
        }
        source.requireCurrent();
        final var pending = candidate.objectIntent();
        final long window = Math.addExact(
                com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.recoveryProtectionInputWindowBytes(
                        images),
                4L * (com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES + 4096L)
                        + TargetResourceReclaimSigner.CONFIGURATION_WINDOW_BYTES);
        if (maximumInputBytes < 1 || maximumInputBytes == Long.MAX_VALUE || window > maximumInputBytes) {
            throw new IllegalStateException(
                    "pending provider/signing stages exceed their original shared input reservation");
        }
        final Runnable current = () -> {
            source.requireCurrent();
            originalHeld.run();
        };
        current.run();
        final long beforeRows = recoveryReads.actualRecords(), beforeBytes = recoveryReads.actualBytes();
        final var recovery = source.catalog()
                .capturePendingObjectProviderProtection(pending, images, recoveryReads, maximumInputBytes, current);
        return new PendingPayloadPreparation(
                this,
                source,
                candidate,
                pending,
                recovery,
                provider,
                originalHeld,
                recoveryReads.actualRecords() - beforeRows,
                recoveryReads.actualBytes() - beforeBytes,
                maximumInputBytes);
    }

    /** Actual pending provider flight. Its original Held owns provider/Root resources until I/O exit. */
    public static final class PendingPayloadPreparation implements AutoCloseable {
        public record Result(
                com.nereusstream.delay.adapter.PayloadObjectDelete.Observation object,
                com.nereusstream.delay.adapter.PayloadUncommittedDelete.Observation uncommitted) {
            public Result {
                if ((object == null) == (uncommitted == null)) {
                    throw new IllegalArgumentException("one actual provider observation required");
                }
            }
        }

        private final TargetWorkerShardRuntime worker;
        private final TargetSourceApplyRuntime.ReclaimPreparationRead source;
        private final com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate candidate;
        private final com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord pending;
        private final com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection
                recovery;
        private final com.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore provider;
        private final Runnable originalHeld, current;
        private final long captureRows, captureBytes, recoveryRetained, maximumInputBytes;
        private com.nereusstream.delay.runtime.TargetRetirementPreparation preparation;
        private com.nereusstream.delay.runtime.TargetRetirementPreparation.Protection protection;
        private volatile Result observation;
        private volatile long preparedRetained;
        private boolean advancing, closed;

        private PendingPayloadPreparation(
                TargetWorkerShardRuntime worker,
                TargetSourceApplyRuntime.ReclaimPreparationRead source,
                com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate candidate,
                com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord pending,
                com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery,
                com.nereusstream.delay.adapter.S3CompatiblePayloadObjectStore provider,
                Runnable originalHeld,
                long captureRows,
                long captureBytes,
                long maximumInputBytes) {
            this.worker = worker;
            this.source = source;
            this.candidate = candidate;
            this.pending = pending;
            this.recovery = recovery;
            this.provider = provider;
            this.originalHeld = originalHeld;
            this.captureRows = captureRows;
            this.captureBytes = captureBytes;
            this.maximumInputBytes = maximumInputBytes;
            recoveryRetained = recovery.retainedBytes();
            current = () -> {
                source.requireCurrent();
                this.originalHeld.run();
                final var selected = protection;
                if (selected != null) {
                    selected.requireCurrent();
                }
            };
            if (captureRows < 0 || captureBytes < 0) {
                throw new IllegalStateException("pending provider capture budget regressed");
            }
            requireRetainedWindow();
        }

        public TargetSourceApplyRuntime.ReclaimPreparationRead source() {
            return source;
        }

        public SystemMutation mutation() {
            return pending.intent();
        }

        public long retainedBytes() {
            return Math.addExact(recoveryRetained, preparedRetained);
        }
        /** A real response remains observable even if the enclosing OwnedIO's final guard fails. */
        public Optional<Result> observation() {
            return Optional.ofNullable(observation);
        }

        public void protect(com.nereusstream.delay.runtime.TargetRetirementPreparation.Protection original) {
            requireOutsideMonitors();
            synchronized (this) {
                if (closed || advancing || protection != null) {
                    throw new IllegalStateException("pending provider protection is already selected or closed");
                }
                protection = Objects.requireNonNull(original);
                advancing = true;
            }
            try {
                current.run();
                protection.requireCurrent();
                // The Source mints this preparation only for its actual selected pending Native56.
                final var selected = source.beginPendingProtection(pending, protection, recovery);
                synchronized (this) {
                    preparation = selected;
                }
                selected.debitReads(captureRows, captureBytes);
                updateRetained();
            } finally {
                synchronized (this) {
                    advancing = false;
                }
            }
        }

        /** Adds actual writer dependency I/O to this same original reclaim flight, without granting authority. */
        public void debitDependencyReads(long records, long bytes) {
            requireOutsideMonitors();
            final com.nereusstream.delay.runtime.TargetRetirementPreparation selected;
            synchronized (this) {
                if (closed || advancing || preparation == null) {
                    throw new IllegalStateException("pending provider preparation cannot accept dependency reads");
                }
                selected = preparation;
                advancing = true;
            }
            try {
                selected.debitReads(records, bytes);
                updateRetained();
            } finally {
                synchronized (this) {
                    advancing = false;
                }
            }
        }

        /** One original OwnedIO turn; final Catalog rereads use this turn's real budget. */
        public Optional<Result> advance(BoundedReadBudget nativeReads, int maxRows, BoundedReadBudget recoveryReads) {
            requireOutsideMonitors();
            Objects.requireNonNull(nativeReads);
            Objects.requireNonNull(recoveryReads);
            if (maxRows < 1 || maxRows == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("finite pending provider page required");
            }
            synchronized (this) {
                if (closed || advancing || preparation == null) {
                    throw new IllegalStateException("pending provider preparation is unprotected, advancing or closed");
                }
                if (observation != null) {
                    return Optional.of(observation);
                }
                advancing = true;
            }
            try {
                current.run();
                protection.requireCurrent();
                if (!preparation.advance(nativeReads, maxRows)) {
                    updateRetained();
                    return Optional.empty();
                }
                // The actual Source point reader debits its own Native reads through this exact proof.
                source.consumePendingProvider(pending, recovery, nativeReads, current, preparation.proof());
                final long recoveryRows = recoveryReads.actualRecords(), recoveryBytes = recoveryReads.actualBytes();
                try {
                    recovery.requireCurrent(recoveryReads, current);
                    current.run();
                    protection.requireCurrent();
                } finally {
                    preparation.debitReads(
                            recoveryReads.actualRecords() - recoveryRows, recoveryReads.actualBytes() - recoveryBytes);
                }
                updateRetained();
                if (candidate.uncommittedObject()) {
                    observation = new Result(null, provider.retireUncommitted(pending, recovery, current));
                } else {
                    final var operation = pending.operation();
                    final var request = operation.nativeOrigin() == null
                            ? new com.nereusstream.delay.adapter.PayloadObjectDelete.Request(
                                    operation.expected(), operation.proof(), pending.appliedAt())
                            : new com.nereusstream.delay.adapter.PayloadObjectDelete.Request(
                                    operation.expected(), recovery.objectAcceptance(), pending.appliedAt());
                    observation = new Result(provider.deleteExact(request, current), null);
                }
                // Never place a post-provider guard between the actual receipt and its retained owner.
                return Optional.of(observation);
            } catch (com.nereusstream.delay.runtime.TargetRetirementPreparation.ViewChanged changed) {
                preparation.invalidate();
                return Optional.empty();
            } catch (com.nereusstream.delay.store.ReadIncompleteException incomplete) {
                current.run();
                protection.requireCurrent();
                return Optional.empty();
            } finally {
                synchronized (this) {
                    advancing = false;
                }
            }
        }

        private void requireOutsideMonitors() {
            if (Thread.holdsLock(worker) || Thread.holdsLock(worker.store) || Thread.holdsLock(worker.target)) {
                throw new IllegalStateException("pending provider I/O must run outside Worker/Source/Store monitors");
            }
        }

        private void updateRetained() {
            preparedRetained = preparation.retainedBytes();
            requireRetainedWindow();
        }

        private void requireRetainedWindow() {
            if (retainedBytes() > maximumInputBytes) {
                throw new TargetSourceAuthorityPreparation.InputUnavailable(
                        "pending provider preparation exceeds its original shared input reservation");
            }
        }

        @Override
        public void close() {
            synchronized (this) {
                if (advancing) {
                    throw new IllegalStateException("pending provider I/O has not exited");
                }
                if (closed) {
                    return;
                }
                advancing = true;
            }
            try {
                if (preparation != null) {
                    preparation.close();
                }
                synchronized (this) {
                    closed = true;
                }
            } finally {
                synchronized (this) {
                    advancing = false;
                }
            }
        }
    }
    /** The original caller reserves CONFIGURATION_WINDOW_BYTES plus the complete signing/authentication window. */
    public TargetResourceReclaimSigner.Prepared prepareConfiguredResourceReclaim(
            java.util.List<com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate> candidates,
            java.nio.file.Path configuration,
            byte[] actualRoot,
            java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget keyReads,
            BoundedReadBudget recoveryReads,
            BoundedReadBudget nativeReads,
            java.util.function.Supplier<TrustedUtcIntervalEvidence> time,
            long retryUntil,
            long maximumInputBytes,
            Runnable originalHeld,
            LongSupplier clock)
            throws java.io.IOException {
        if (maximumInputBytes <= TargetResourceReclaimSigner.CONFIGURATION_WINDOW_BYTES
                || maximumInputBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Native signing configuration requires its original finite reserved input window");
        }
        if (Thread.holdsLock(this) || Thread.holdsLock(store) || Thread.holdsLock(target)) {
            throw new IllegalStateException(
                    "Native signing/authentication must run outside original Worker/Source/Store monitors");
        }
        final TargetSourceApplyRuntime.ReclaimPreparationRead source;
        synchronized (this) {
            requireNewTurnsAdmitted();
            resources.requireRuntimeBusinessAdmission();
            source = target.reclaimPreparationRead(candidates, clock);
        }
        final Runnable held = () -> {
            source.requireCurrent();
            originalHeld.run();
        };
        try (var signer = TargetResourceReclaimSigner.loadConfigured(configuration, actualRoot, keyReads, held)) {
            return signer.prepare(
                    source,
                    images,
                    recoveryReads,
                    nativeReads,
                    time,
                    retryUntil,
                    maximumInputBytes - TargetResourceReclaimSigner.CONFIGURATION_WINDOW_BYTES,
                    held);
        }
    }

    TargetResourceReclaimSigner.Draft beginConfiguredResourceReclaim(
            java.util.List<com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate> candidates,
            java.nio.file.Path configuration,
            byte[] actualRoot,
            java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
            BoundedReadBudget keyReads,
            BoundedReadBudget recoveryReads,
            BoundedReadBudget nativeReads,
            java.util.function.Supplier<TrustedUtcIntervalEvidence> time,
            long retryUntil,
            long maximumInputBytes,
            Runnable originalHeld,
            LongSupplier clock)
            throws java.io.IOException {
        if (maximumInputBytes <= TargetResourceReclaimSigner.CONFIGURATION_WINDOW_BYTES
                || maximumInputBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Native signing configuration requires its original finite reserved input window");
        }
        if (Thread.holdsLock(this) || Thread.holdsLock(store) || Thread.holdsLock(target)) {
            throw new IllegalStateException(
                    "Native signing/authentication must run outside original Worker/Source/Store monitors");
        }
        final TargetSourceApplyRuntime.ReclaimPreparationRead source;
        synchronized (this) {
            requireNewTurnsAdmitted();
            resources.requireRuntimeBusinessAdmission();
        }
        source = target.reclaimPreparationRead(candidates, clock);
        synchronized (this) {
            requireNewTurnsAdmitted();
            resources.requireRuntimeBusinessAdmission();
        }
        source.requireCurrent();
        final Runnable held = () -> {
            source.requireCurrent();
            originalHeld.run();
        };
        final long keyRows = keyReads.actualRecords(), keyBytes = keyReads.actualBytes();
        try (var signer = TargetResourceReclaimSigner.loadConfigured(configuration, actualRoot, keyReads, held)) {
            final var draft = signer.begin(
                    source,
                    images,
                    recoveryReads,
                    nativeReads,
                    time,
                    retryUntil,
                    maximumInputBytes - TargetResourceReclaimSigner.CONFIGURATION_WINDOW_BYTES,
                    held);
            draft.acquisition(keyReads.actualRecords() - keyRows, keyReads.actualBytes() - keyBytes);
            return draft;
        }
    }

    public TargetEvidenceRetirementWorkClassExecutor.Submission submitResourceReclaim(
            TargetResourceReclaimSigner.Prepared prepared,
            OwnedSourceSystemWriter writer,
            LongSupplier clock,
            java.util.function.Consumer<TargetEvidenceRetirementWorkClassExecutor.Submission> retained) {
        Objects.requireNonNull(prepared).requireRuntime(target);
        return submitResourceReclaim(prepared.mutation(), writer, prepared::requireCurrent, clock, retained);
    }

    public TargetEvidenceRetirementWorkClassExecutor.Submission submitResourceReclaim(
            SystemMutation image,
            OwnedSourceSystemWriter writer,
            Runnable protection,
            LongSupplier clock,
            java.util.function.Consumer<TargetEvidenceRetirementWorkClassExecutor.Submission> retained) {
        if (image.type() != SystemMutationType.TARGET_RESOURCE_RECLAIM) {
            throw new IllegalArgumentException("Native reclaim type required");
        }
        return submitRetirement(image, writer, protection, clock, retained);
    }

    private TargetEvidenceRetirementWorkClassExecutor.Submission submitRetirement(
            SystemMutation image,
            OwnedSourceSystemWriter writer,
            Runnable protection,
            LongSupplier clock,
            java.util.function.Consumer<TargetEvidenceRetirementWorkClassExecutor.Submission> retained) {
        final TargetEvidenceRetirementWorkClassExecutor handoff;
        synchronized (this) {
            requireNewTurnsAdmitted();
            resources.requireRuntimeBusinessAdmission();
            if (writer.sourceWriterWorker() != this
                    || writer.resourcesTerminated()
                    || !writer.readyForAppending()
                    || systemWriterBackends.values().stream().noneMatch(original -> original == writer)) {
                throw new IllegalArgumentException("retirement requires this Worker's actual admitted Source writer");
            }
            if (retirementHandoff == null) {
                retirementHandoff = new TargetEvidenceRetirementWorkClassExecutor(this, writer);
            }
            retirementHandoff.requireAppender(writer);
            handoff = retirementHandoff;
        }
        return handoff.submit(image, protection, clock, retained);
    }

    boolean retirementActionExited(WorkClassTask task) {
        return workClasses.state(task).isEmpty();
    }

    boolean withdrawRetirementAction(WorkClassTask task) {
        return workClasses.withdrawQueued(task);
    }

    void discardFailedRetirementAction(WorkClassTask task) {
        workClasses.discardFailed(task);
    }

    synchronized boolean resourceReclaimPreparationCanExitForReplay(
            TargetResourceReclaimMaintenance original,
            TargetEvidenceRetirementWorkClassExecutor.Submission submission,
            OwnedSourceSystemWriter writer) {
        if (resourceReclaim != original) {
            throw new IllegalArgumentException("GC preparation is not this Worker's original owner");
        }
        original.requireReplayPreparation(this, writer, submission);
        return backendQuarantined
                && sourceAndMaintenancePaused
                && target.fenced()
                && store.isCloseStarted()
                && target.authorityIoExited()
                && sourceBackendGroup != null
                && sourceBackendGroup.resourcesTerminated()
                && sourceBackendGroup.nativeResourcesTerminated()
                && writer.sourceWriterWorker() == this
                && writer.resourcesTerminated()
                && writer.ownedBackendGroup().resourcesTerminated()
                && writer.ownedBackendGroup().nativeResourcesTerminated();
    }

    synchronized void submitRetirementAction(
            WorkClassTask task, SystemMutation image, LongSupplier clock, Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (target.retirementMutationResult(image, clock).isEmpty()) {
            target.requireRetirementWriter(image, clock);
        }
        workClasses.requireTaskServiceable(task);
        workClasses.submit(task, action);
    }

    synchronized SourceMutationAppend appendRetirement(
            SystemMutation image,
            OwnedSourceSystemWriter writer,
            Runnable protection,
            LongSupplier clock,
            Runnable beforeAppend) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        final var first = target.retirementMutationResult(image, clock).orElse(null);
        if (first != null) {
            return new SourceMutationAppend(first, null);
        }
        target.requireRetirementWriter(image, clock);
        protection.run();
        beforeAppend.run();
        try {
            final var result = Objects.requireNonNull(writer.append(image), "actual retirement Source append");
            if (result.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                target.requireCurrentExpiryLogPosition(
                        result.sourcePosition(),
                        result.sourceConnectionGeneration(),
                        result.guardAttestationDigest(),
                        clock);
            }
            return new SourceMutationAppend(null, result);
        } catch (RuntimeException | Error unknown) {
            target.fence();
            throw unknown;
        }
    }

    public Optional<SystemMutationResult> settleRetirementIntent(SystemMutation image, LongSupplier clock) {
        if (image.type() != SystemMutationType.RESOURCE_RETIRE_INTENT) {
            throw new IllegalArgumentException("retirement intent type required");
        }
        return settleRetirement(image, clock);
    }

    public Optional<SystemMutationResult> settleRetirementConfirmation(SystemMutation image, LongSupplier clock) {
        if (image.type() != SystemMutationType.RESOURCE_DELETE_CONFIRMED) {
            throw new IllegalArgumentException("retirement confirmation type required");
        }
        return settleRetirement(image, clock);
    }

    public Optional<SystemMutationResult> settleResourceReclaim(SystemMutation image, LongSupplier clock) {
        if (image.type() != SystemMutationType.TARGET_RESOURCE_RECLAIM) {
            throw new IllegalArgumentException("Native reclaim type required");
        }
        return settleRetirement(image, clock);
    }

    private Optional<SystemMutationResult> settleRetirement(SystemMutation image, LongSupplier clock) {
        final var handoff = retirementHandoff;
        return handoff == null ? retirementMutationResult(image, clock) : handoff.settle(image, clock);
    }

    @Override
    public void requireFleetComposition(
            final WorkClassExecutionRegistry expectedClasses, final SharedRocksDbResources expectedResources) {
        if (resources != Objects.requireNonNull(expectedResources, "expectedResources")
                || workClasses != Objects.requireNonNull(expectedClasses, "expectedClasses")) {
            throw new IllegalArgumentException("Target Worker shard uses another resource or WorkClass graph");
        }
    }

    void requireRuntimeConfiguration(final TargetWorkerRuntimeConfig config) {
        final var expected = Objects.requireNonNull(config, "config");
        expected.requireRuntime(workClasses, resources);
        target.requireRuntimeLimits(expected.source());
    }

    public synchronized TargetSourceBacklog.Cut captureSourceBacklogCut(final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        final var captured = target.captureSourceBacklogCut(Objects.requireNonNull(ownerClock, "ownerClock"));
        return new TargetSourceBacklog.Cut(captured.applied(), () -> {
            synchronized (TargetWorkerShardRuntime.this) {
                requireNewTurnsAdmitted();
            }
            resources.requireRuntimeBusinessAdmission();
            captured.requireCurrent();
        });
    }

    public synchronized SourceApplyCoordinator.TurnResult runSourceTurn(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        preparedCheckpointCut = null;
        resources.requireRuntimeBusinessAdmission();
        return sourceLoop.runTurn(
                Objects.requireNonNull(budget, "budget"), Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    public synchronized TargetReservationGcRuntime.Turn runMaintenanceTurn(final SchedulerBudget budget) {
        requireNewTurnsAdmitted();
        preparedCheckpointCut = null;
        resources.requireRuntimeBusinessAdmission();
        return maintenance.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    /** One caller-driven producer turn; the Host maintenance loop selects Shards in rotation. */
    @Override
    public Optional<WorkClassTask> runMessageExpiryMaintenanceTurn() {
        final var configured = messageExpiryMaintenance;
        return configured == null ? Optional.empty() : configured.runTurn();
    }

    /** Claims one previously selected head only while this exact Worker admits new business turns. */
    public synchronized TargetClaimRecord claim(
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final OwnerIdentity owner,
            final long nowEpochMs,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        return claim(
                budget,
                selected,
                owner,
                nowEpochMs,
                deadlineEpochMs,
                executionBytes,
                operationDigest,
                quota,
                physicalWrites,
                ownerClock,
                null);
    }

    public synchronized TargetClaimRecord claim(
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final OwnerIdentity owner,
            final long nowEpochMs,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock,
            final com.nereusstream.delay.protocol.HandoffPolicyHeadRef policyHead) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.claim(
                budget,
                selected,
                owner,
                nowEpochMs,
                deadlineEpochMs,
                executionBytes,
                operationDigest,
                quota,
                physicalWrites,
                ownerClock,
                policyHead);
    }

    /** Revokes an exact unadmitted Claim while this Worker still admits business turns. */
    synchronized boolean claimStillCurrent(
            final BoundedReadBudget budget,
            final TargetClaimRecord claim,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.claimStillCurrent(budget, claim, quota, clock);
    }

    public synchronized void revokeClaim(
            final BoundedReadBudget budget,
            final TargetClaimRecord expected,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.revokeClaim(budget, expected, operationDigest, quota, physicalWrites, ownerClock);
    }

    /** Reads one bounded page only while this exact Shard admits new Worker turns. */
    public synchronized TargetQueueSnapshotReader.Page scanTargetQueues(
            final BoundedReadBudget budget,
            final TargetPartitionId after,
            final int maximumTargets,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.scanTargetQueues(budget, after, maximumTargets, ownerClock);
    }

    /** Refreshes the exact physical Target's heads under current Worker admission. */
    public synchronized Optional<TargetQueueSnapshotReader.Entry> readTargetQueue(
            final BoundedReadBudget budget, final TargetPartitionId targetId, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readTargetQueue(budget, targetId, ownerClock);
    }

    public synchronized SystemMutation preparePublishAdmission(
            final BoundedReadBudget budget,
            final TargetClaimRecord claim,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability,
            final byte[] artifacts,
            final long retryUntilEpochMs,
            final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            final TargetPublishAdmissionMutationFactory signer,
            final LongSupplier clock) {
        return preparePublishAdmission(
                budget,
                claim,
                channel,
                capability,
                artifacts,
                retryUntilEpochMs,
                commitment,
                allocated,
                time,
                policies,
                null,
                signer,
                clock);
    }

    public synchronized SystemMutation preparePublishAdmission(
            final BoundedReadBudget budget,
            final TargetClaimRecord claim,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability,
            final byte[] artifacts,
            final long retryUntilEpochMs,
            final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            final com.nereusstream.delay.protocol.TargetKafkaReceiptBinding kafkaReceipt,
            final TargetPublishAdmissionMutationFactory signer,
            final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return Objects.requireNonNull(signer, "admissionSigner")
                .sign(target.preparePublishAdmission(
                        budget,
                        claim,
                        channel,
                        capability,
                        artifacts,
                        retryUntilEpochMs,
                        commitment,
                        allocated,
                        time,
                        policies,
                        kafkaReceipt,
                        clock));
    }

    public synchronized void requireNativeOwnership(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Applied applied,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireNativeOwnership(budget, applied, time, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Applied readAppliedAdmission(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.protocol.SystemMutation image,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readAppliedAdmission(budget, image, ownerClock);
    }

    /** Binds one lifecycle submission slot to this exact Worker and caller-owned registry/client protection. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishOutcomeStore.NegativeOutcome readNegativeOutcome(
            final BoundedReadBudget budget,
            final byte[] mappingId,
            final SystemMutation admission,
            final SystemMutation outcome,
            final com.nereusstream.delay.protocol.PulsarRecordTemplate template,
            final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readNegativeOutcome(budget, mappingId, admission, outcome, template, clock);
    }

    public synchronized TargetChannelControlWorkClassExecutor configureChannelControlHandoff(
            final ShardLogMutationAppender appender,
            final ControlTargetRegistrationAuthority registrations,
            final Runnable protection) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (channelControlHandoff != null) {
            throw new IllegalStateException("channel lifecycle handoff is already configured for this Worker");
        }
        channelControlHandoff = new TargetChannelControlWorkClassExecutor(this, appender, registrations, protection);
        return channelControlHandoff;
    }

    /** Reuses the actual connected Oxia handle/session; it allocates no client, executor or alternative registry. */
    public synchronized TargetChannelControlWorkClassExecutor configureOxiaChannelControlHandoff(
            final ShardLogMutationAppender appender,
            final OxiaSyncOwnerLeaseBackend.ClientHandle handle,
            final String registryPrefix,
            final Runnable protection) {
        return configureChannelControlHandoff(
                appender,
                new OxiaControlTargetRegistrationAuthority(
                        new OxiaSyncControlTargetRegistrationBackend(handle, registryPrefix)),
                protection);
    }

    public TargetChannelControlWorkClassExecutor.Submission submitChannelLifecycle(
            final TargetChannelControlMutationFactory.Prepared prepared, final LongSupplier clock) {
        final var handoff = Objects.requireNonNull(channelControlHandoff, "configured channel lifecycle handoff");
        return handoff.submit(prepared, clock);
    }

    /** Original Control registration/appender slot, including its existing retry, pause and drain ownership. */
    public TargetChannelControlWorkClassExecutor.Submission submitSloCertification(
            final TargetSloObservationControlMutationFactory.Prepared prepared, final LongSupplier clock) {
        final var handoff = Objects.requireNonNull(channelControlHandoff, "configured original Target Control handoff");
        return handoff.submitObservation(prepared, clock);
    }

    public TargetChannelControlWorkClassExecutor.Submission submitSloFinalEvent(
            final TargetSloObservationEventMutationFactory.Prepared prepared, final LongSupplier clock) {
        return Objects.requireNonNull(channelControlHandoff, "configured original Target System handoff")
                .submitFinalEvent(prepared, clock);
    }

    public TargetChannelControlWorkClassExecutor.Submission submitParentControlCredit(
            TargetParentControlCreditMutationFactory.Prepared prepared, LongSupplier clock) {
        return Objects.requireNonNull(channelControlHandoff, "configured original Source Control handoff")
                .submitParentCredit(prepared, clock);
    }

    public TargetChannelControlWorkClassExecutor.Submission submitScopeControlObligation(
            TargetScopeControlObligationMutationFactory.Prepared prepared, LongSupplier clock) {
        return Objects.requireNonNull(channelControlHandoff, "configured original Source Control handoff")
                .submitScopeObligation(prepared, clock);
    }

    /** Retained Admission proof for conservative recovery only; current Owner/read guards still apply. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Recovery readRecoveryAdmission(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.protocol.SystemMutation image,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readRecoveryAdmission(budget, image, ownerClock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaUnknown readKafkaUnknown(
            BoundedReadBudget budget, SystemMutation admission, LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readKafkaUnknown(budget, admission, clock);
    }

    public synchronized boolean kafkaMappingSettled(
            BoundedReadBudget budget,
            SystemMutation admission,
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
            LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.kafkaMappingSettled(budget, admission, mapping, clock);
    }

    /** Enumerates retained Admission references under this active Worker, with opaque same-view continuation. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Page discoverPublishRecovery(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor continuation,
            final int maximumRows,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverPublishRecovery(budget, continuation, maximumRows, ownerClock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Page discoverKafkaUnknown(
            BoundedReadBudget reads,
            com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor continuation,
            int maximumRows,
            LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverKafkaUnknown(reads, continuation, maximumRows, clock);
    }

    synchronized boolean publishRecoveryStillPending(
            BoundedReadBudget reads,
            com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Reference reference,
            LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null
                && publishRuntime.ownsAttempt(reference.attempt().publishAttemptId())) {
            return false;
        }
        return target.publishRecoveryStillPending(reads, reference, clock);
    }

    /** Enumerates all retained Admission inputs for checkpoint/history protection under this active Worker. */
    public com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Page discoverCheckpointInputs(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor continuation,
            final int maximumRows,
            final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        return target.discoverCheckpointInputs(budget, continuation, maximumRows, ownerClock);
    }

    /** Revalidates the retained-input cut under the current Worker and Owner guards. */
    void requireCheckpointInputsCurrent(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor cursor,
            final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        target.requireCheckpointInputsCurrent(budget, cursor, ownerClock);
    }

    void requireCheckpointAdmissionInputsSameCut(
            BoundedReadBudget budget,
            com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor cursor,
            TargetCheckpointControlInputs.Cut expected,
            LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        target.requireCheckpointInputsCurrent(budget, cursor, ownerClock);
        target.requireCheckpointControlInputsCurrent(budget, expected, ownerClock);
        cursor.requireSameRetainedCut(expected.view);
    }

    TargetCheckpointControlInputs.Cut captureCheckpointControlInputs(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.store.TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        return target.captureCheckpointControlInputs(budget, ledgerLimits, ownerClock);
    }

    void requireCheckpointControlInputsCurrent(
            final BoundedReadBudget budget,
            final TargetCheckpointControlInputs.Cut cut,
            final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        target.requireCheckpointControlInputsCurrent(budget, cut, ownerClock);
    }

    /** Install before Host ticks begin. History must return promptly and perform its bounded I/O asynchronously. */
    public synchronized TargetPulsarPublishRuntime configurePulsarPublishing(
            final ShardLogMutationAppender appender,
            final TargetPublishAdmissionMutationFactory signer,
            final TargetPublishOutcomeMutationFactory outcomeSigner,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            final com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            final TargetPulsarPublishRuntime.Preparation preparation,
            final TargetPulsarPublishRuntime.Payloads payloads,
            final Supplier<BoundedReadBudget> reads,
            final Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            final LongSupplier clock,
            final java.util.concurrent.Executor io,
            final java.util.concurrent.Executor completion,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight
                    ordinaryGate,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight
                    nativeGate,
            final TargetPulsarPublishExecutor.ResultAuthority results) {
        return configurePulsarPublishing(
                appender,
                signer,
                outcomeSigner,
                journal,
                adapter,
                artifacts,
                preparation,
                payloads,
                reads,
                time,
                clock,
                io,
                completion,
                ordinaryGate,
                nativeGate,
                results,
                null);
    }

    public synchronized TargetPulsarPublishRuntime configurePulsarPublishing(
            final ShardLogMutationAppender appender,
            final TargetPublishAdmissionMutationFactory signer,
            final TargetPublishOutcomeMutationFactory outcomeSigner,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            final com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            final TargetPulsarPublishRuntime.Preparation preparation,
            final TargetPulsarPublishRuntime.Payloads payloads,
            final Supplier<BoundedReadBudget> reads,
            final Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            final LongSupplier clock,
            final java.util.concurrent.Executor io,
            final java.util.concurrent.Executor completion,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight
                    ordinaryGate,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight
                    nativeGate,
            final TargetPulsarPublishExecutor.ResultAuthority results,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal.NegativeSettlementAuthority negativeAuthority) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null) {
            throw new IllegalStateException("Target publication runtime is already configured");
        }
        if (journalSettlement != null) {
            journalSettlement.requireJournal(journal);
        }
        publishRuntime = new TargetPulsarPublishRuntime(
                this,
                appender,
                signer,
                outcomeSigner,
                journal,
                adapter,
                artifacts,
                preparation,
                payloads,
                reads,
                time,
                clock,
                io,
                completion,
                ordinaryGate,
                nativeGate,
                results,
                negativeAuthority);
        return publishRuntime;
    }

    public synchronized TargetJournalNegativeSettlementMaintenance configureJournalNegativeSettlement(
            final com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal.NegativeSettlementAuthority authority,
            final Runnable protection,
            final Supplier<BoundedReadBudget> reads,
            final TargetJournalNegativeSettlementMaintenance.History history,
            final java.util.concurrent.Executor io,
            final LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (journalSettlement != null) {
            throw new IllegalStateException("Journal settlement maintenance is already configured");
        }
        if (publishRuntime != null) {
            publishRuntime.requireJournal(journal);
        }
        journalSettlement = new TargetJournalNegativeSettlementMaintenance(
                this, journal, authority, protection, reads, history, io, clock);
        return journalSettlement;
    }

    synchronized long confirmJournalSweep(
            com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor completeScan) {
        requireNewTurnsAdmitted();
        synchronized (store) {
            completeScan.requireSameRetainedCut(new com.nereusstream.delay.runtime.TargetQueueSnapshotReader.Cut(
                    store.metadata().storeIncarnation(), store.latestSequenceNumber()));
            return store.shardMutationSequence();
        }
    }

    synchronized long publicationSourceSequence() {
        return store.shardMutationSequence();
    }

    boolean hasLivePublicationAttempt(byte[] attempt) {
        final var publisher = publishRuntime;
        return publisher != null && publisher.ownsAttempt(attempt);
    }

    @Override
    public Optional<TargetJournalNegativeSettlementMaintenance.Turn> runJournalSettlementMaintenanceTurn() {
        final var configured = journalSettlement;
        return configured == null ? Optional.empty() : Optional.of(configured.runTurn());
    }

    /** Configures actual baseline transport; shares the existing finite Source/Claim driver when Journal is present. */
    public synchronized TargetPulsarPublishRuntime configureBaselinePublishing(
            ShardLogMutationAppender appender,
            TargetPublishAdmissionMutationFactory signer,
            TargetPublishOutcomeMutationFactory outcomeSigner,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            TargetPulsarPublishRuntime.Preparation preparation,
            TargetPulsarPublishRuntime.Payloads payloads,
            Supplier<BoundedReadBudget> reads,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            LongSupplier clock,
            java.util.concurrent.Executor io,
            java.util.concurrent.Executor completion,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null) {
            throw new IllegalStateException("Target publication driver is already configured");
        }
        publishRuntime = new TargetPulsarPublishRuntime(
                this,
                appender,
                signer,
                outcomeSigner,
                adapter,
                artifacts,
                preparation,
                payloads,
                reads,
                time,
                clock,
                io,
                completion,
                gate,
                results);
        return publishRuntime;
    }

    /** Adds a baseline endpoint to the already configured Source driver; it uses that driver's original inputs. */
    public synchronized void configureBaselineEndpoint(
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            TargetPublishOutcomeMutationFactory outcomeSigner,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results,
            java.util.concurrent.Executor completion,
            LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        final var driver = Objects.requireNonNull(publishRuntime, "configured Source publication driver");
        driver.requireArtifacts(artifacts);
        driver.configureBaseline(adapter, outcomeSigner, gate, results, completion);
    }

    /** K2-only Source driver, with an actual guarded transaction endpoint and independently authenticated anchor. */
    public synchronized TargetPulsarPublishRuntime configureKafkaReceiptPublishing(
            ShardLogMutationAppender appender,
            TargetPublishAdmissionMutationFactory signer,
            TargetPublishOutcomeMutationFactory outcomeSigner,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            TargetPulsarPublishRuntime.Preparation preparation,
            TargetPulsarPublishRuntime.Payloads payloads,
            Supplier<BoundedReadBudget> reads,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            LongSupplier clock,
            java.util.concurrent.Executor io,
            java.util.concurrent.Executor completion,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null) {
            throw new IllegalStateException("Target publication driver is already configured");
        }
        publishRuntime = new TargetPulsarPublishRuntime(
                this,
                appender,
                signer,
                outcomeSigner,
                adapter,
                artifacts,
                preparation,
                payloads,
                reads,
                time,
                clock,
                io,
                completion,
                gate,
                results,
                com.nereusstream.delay.protocol.ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT);
        return publishRuntime;
    }

    /** Adds K2 to the same single-flight Claim/Source driver before Host scheduling starts. */
    public synchronized void configureKafkaReceiptEndpoint(
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            TargetPublishOutcomeMutationFactory outcomeSigner,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results,
            java.util.concurrent.Executor completion,
            LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        final var driver = Objects.requireNonNull(publishRuntime, "configured Source publication driver");
        driver.requireArtifacts(artifacts);
        driver.configureKafkaReceipt(adapter, outcomeSigner, gate, results, completion);
    }

    void requirePublishingStartupClosure() {
        final var runtime = Objects.requireNonNull(publishRuntime, "configured Target publisher");
        runtime.requireConfiguredEndpoints();
        if (runtime.hasKafkaReceiptEndpoint()) {
            requirePublicationResourcesReady();
        }
        if (runtime.requiresJournal()) {
            Objects.requireNonNull(journalSettlement, "configured retained Source/Journal cleanup");
        }
    }

    /**
     * Complete local dependencies for configured service activation; native/Broker authority stays in each provider.
     */
    synchronized void requireConfiguredServiceReady(TargetWorkerHostRuntime expected, LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (publicationHost != expected
                || sourceBackendGroup == null
                || !sourceBackendGroup.workerReady(this)
                || !sourceBackendGroup.sourceCredentialsReady(this)) {
            throw new IllegalStateException("configured service requires its actual owned Source and Host");
        }
        requireChannelInventoryCoverage();
        requirePublishingStartupClosure();
        if (!target.hasAuthorityPreparation()) {
            throw new IllegalStateException(
                    "configured service requires complete bounded Source content/history preparation");
        }
        Objects.requireNonNull(publishRecoveryMaintenance, "configured retained Admission/UNKNOWN recovery");
        if (publishRuntime.requiresJournal()) {
            requirePulsarStartupClosure();
            if (journalBackend == null || !journalBackend.readyForJournal()) {
                throw new IllegalStateException("configured service requires its actual owned managed Journal");
            }
            publishRuntime.requireJournal(journalBackend.journal());
        }
        requirePublicationResourcesReady();
        if (publicationResources.values().stream()
                .anyMatch(owner -> !owner.closed()
                        && (owner.recoveryOnly()
                                ? owner.recoveryCredentialReceipt() == null
                                : owner.credentialReceipt() == null))) {
            throw new IllegalStateException("new Target service requires actual resolved mTLS publication material");
        }
        if (!publishRuntime.available()) {
            throw new IllegalStateException("configured service activation has an existing publication flight");
        }
        if (systemWriterBackends.values().stream()
                .noneMatch(writer -> writer.readyForAppending() && publishRuntime.usesSourceAppender(writer))) {
            throw new IllegalStateException("configured service requires its actual owned Source system appender");
        }
    }

    boolean publisherSupportsNative() {
        return publishRuntime != null && publishRuntime.supportsNative();
    }

    void requirePulsarStartupClosure() {
        Objects.requireNonNull(publishRuntime, "configured Target publisher").requireNegativeSettlementConfigured();
        Objects.requireNonNull(journalSettlement, "configured retained Source/Journal cleanup");
    }

    boolean publisherAvailable() {
        return publishRuntime != null && publishRuntime.available();
    }

    void acceptPublicationClaim(final TargetClaimRecord claim) {
        Objects.requireNonNull(publishRuntime, "configured Target publisher").accept(claim);
    }

    @Override
    public Optional<TargetPulsarPublishRuntime.Turn> runPublishMaintenanceTurn() {
        final var runtime = publishRuntime;
        return runtime == null ? Optional.empty() : Optional.of(runtime.runTurn());
    }

    public synchronized TargetPublishRecoveryMaintenance configurePublishRecoveryMaintenance(
            final TargetPublishOutcomeMutationFactory outcomes,
            final ShardLogMutationAppender appender,
            final Supplier<BoundedReadBudget> reads,
            final TargetPublishRecoveryMaintenance.History history,
            final LongSupplier recoveryClock) {
        requirePublishRecoveryOwner(recoveryClock);
        if (publishRecoveryMaintenance != null) {
            throw new IllegalStateException("Target publish recovery maintenance is already configured");
        }
        final var handoff = new TargetOutcomeWorkClassExecutor(this, appender);
        final var executor = new TargetPublishRecoveryExecutor(this, handoff, outcomes);
        publishRecoveryMaintenance =
                new TargetPublishRecoveryMaintenance(this, executor, reads, history, recoveryClock);
        return publishRecoveryMaintenance;
    }

    synchronized void requirePublishRecoveryOwner(final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requirePublishRecoveryOwner(clock);
    }

    void requireCheckpointHistoryOwner(LongSupplier clock) {
        final TargetCheckpointCandidateWorkClassExecutor.Submission checkpoint;
        synchronized (this) {
            checkpoint = pendingCheckpoint;
            if (checkpoint == null) {
                requireNewTurnsAdmitted();
            }
        }
        if (checkpoint != null) {
            checkpoint.requirePreparationCurrent();
        }
        resources.requireRuntimeBusinessAdmission();
        target.requirePublishRecoveryOwner(clock);
    }

    synchronized boolean publishRecoveryStillAdmitted(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Reference reference,
            final LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null
                && publishRuntime.ownsAttempt(reference.attempt().publishAttemptId())) {
            return false;
        }
        return target.publishRecoveryStillAdmitted(budget, reference, clock);
    }

    @Override
    public Optional<TargetPublishRecoveryMaintenance.Turn> runPublishRecoveryMaintenanceTurn() {
        final TargetPublishRecoveryMaintenance recovery;
        synchronized (this) {
            requireNewTurnsAdmitted();
            recovery = publishRecoveryMaintenance;
        }
        // Recovery rechecks this Worker for each action; avoid Worker -> recovery -> Worker lock inversion.
        return recovery == null ? Optional.empty() : Optional.of(recovery.runTurn());
    }
    /** Scheduling-only retention check; actual Native Source settlement remains a separate requirement. */
    public boolean publishRecoveryRetainsAttempt(byte[] attempt) {
        final TargetPublishRecoveryMaintenance recovery;
        synchronized (this) {
            requireRuntimeAdmitted();
            recovery = publishRecoveryMaintenance;
        }
        // Do not take the maintenance monitor under the Worker monitor: its turns call back into this Worker.
        return recovery != null && recovery.retainsAttempt(attempt);
    }

    synchronized void submitOutcomeAction(
            final WorkClassTask task,
            final com.nereusstream.delay.protocol.SystemMutation mutation,
            final LongSupplier ownerClock,
            final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireOutcomeWriter(mutation, ownerClock);
        workClasses.submit(task, action);
    }

    synchronized void submitAdmissionAction(
            final WorkClassTask task,
            final SystemMutation mutation,
            final LongSupplier ownerClock,
            final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (target.admissionMutationResult(mutation, ownerClock).isEmpty()) {
            target.requireAdmissionWriter(mutation, ownerClock);
        }
        workClasses.submit(task, action);
    }

    record SourceMutationAppend(SystemMutationResult firstResult, ShardLogMutationAppender.AppendOutcome append) {
        SourceMutationAppend {
            if ((firstResult == null) == (append == null)) {
                throw new IllegalArgumentException(
                        "Source mutation turn must observe a first result or one append outcome");
            }
        }
    }

    synchronized SourceMutationAppend appendAdmission(
            final SystemMutation mutation, final ShardLogMutationAppender appender, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        final var first = target.admissionMutationResult(mutation, ownerClock).orElse(null);
        if (first != null) {
            return new SourceMutationAppend(first, null);
        }
        target.requireAdmissionWriter(mutation, ownerClock);
        try {
            final var appended = Objects.requireNonNull(appender.append(mutation), "Target Admission append");
            if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                target.requireCurrentExpiryLogPosition(
                        appended.sourcePosition(),
                        appended.sourceConnectionGeneration(),
                        appended.guardAttestationDigest(),
                        ownerClock);
            }
            return new SourceMutationAppend(null, appended);
        } catch (RuntimeException | Error uncertainAppend) {
            target.fence();
            throw uncertainAppend;
        }
    }

    synchronized Optional<SystemMutationResult> admissionMutationResult(
            final SystemMutation mutation, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.admissionMutationResult(mutation, ownerClock);
    }

    private volatile TargetSloCustodyExportMaintenance sloCustodyExport;
    private volatile TargetSloFinalMaintenance sloFinalMaintenance;

    public synchronized void configureSloFinalMaintenance(
            OwnedSourceSystemWriter writer,
            java.util.concurrent.ThreadPoolExecutor executor,
            java.util.function.Supplier<BoundedReadBudget> reads,
            LongSupplier clock,
            TargetSloFinalMaintenance.Limits pageLimits,
            TargetSloFinalMaintenance.Provider provider,
            long maximumBytes) {
        requireNewTurnsAdmitted();
        if (sloFinalMaintenance != null
                || writer.sourceWriterWorker() != this
                || writer.resourcesTerminated()
                || !writer.readyForAppending()
                || systemWriterBackends.values().stream().noneMatch(original -> original == writer)) {
            throw new IllegalArgumentException("SLO Final maintenance requires one original admitted Source writer");
        }
        workClasses.requireTaskServiceable(new WorkClassTask(
                com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                "target-slo-timeout-envelope",
                maximumBytes));
        final var maintenance =
                new TargetSloFinalMaintenance(this, writer, executor, reads, clock, pageLimits, provider, maximumBytes);
        target.registerSloFinalMaintenance(maintenance);
        sloFinalMaintenance = maintenance;
    }

    @Override
    public Optional<WorkClassTask> runSloFinalMaintenanceTurn() {
        final var maintenance = sloFinalMaintenance;
        return maintenance == null || !target.sloScopeQualified() ? Optional.empty() : maintenance.runTurn();
    }

    synchronized TargetSloFinalMaintenance.Page captureSloFinalPage(
            BoundedReadBudget budget,
            TargetSloFinalMaintenance.Cursor after,
            TargetSloFinalMaintenance.Limits limits,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.captureSloFinalPage(budget, after, limits, clock);
    }

    synchronized void submitSloFinalMaintenanceAction(WorkClassTask task, Runnable action) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireSloScopeQualified();
        workClasses.requireTaskServiceable(task);
        workClasses.submit(task, action);
    }

    synchronized boolean withdrawSloFinalMaintenanceAction(WorkClassTask task) {
        return workClasses.withdrawQueued(task);
    }

    synchronized boolean sloFinalMaintenanceActionExited(WorkClassTask task) {
        return workClasses.state(task).isEmpty();
    }

    synchronized void requireSloFinalMaintenanceContext(
            com.nereusstream.delay.protocol.TargetQuotaMutation inspected, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireSloExportContext(inspected, clock);
    }

    synchronized boolean sloFinalCandidateCurrent(
            TargetSloFinalMaintenance.Candidate candidate,
            TargetSloObservationEventMutationFactory.Prepared exact,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.sloFinalCandidateCurrent(candidate, exact, clock);
    }

    @Override
    public Optional<WorkClassTask> settlePendingSloFinalForDrain(
            SchedulerBudget workBudget, SchedulerBudget sourceBudget, LongSupplier clock) {
        final var maintenance = sloFinalMaintenance;
        if (maintenance == null) {
            return Optional.empty();
        }
        maintenance.stopNew();
        maintenance.retryCleanup();
        final var task = maintenance.runTurn();
        if (!maintenance.hasPending() && target.sloFinalMaintenanceSourceSettled(clock)) {
            return Optional.empty();
        }
        workClasses.runTurn(workBudget);
        synchronized (this) {
            if (sourceLoop.pendingEntry().isPresent()) {
                sourceLoop.settlePendingEntry(sourceBudget, clock);
            } else {
                sourceLoop.runTurn(sourceBudget, clock);
            }
        }
        maintenance.runTurn();
        return maintenance.hasPending() || !target.sloFinalMaintenanceSourceSettled(clock)
                ? task.or(() -> Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-slo-timeout-drain/" + shardId,
                        1)))
                : Optional.empty();
    }

    public synchronized void configureSloCustodyExport(
            OwnedSourceSystemWriter writer,
            java.util.concurrent.ThreadPoolExecutor executor,
            java.util.function.Supplier<BoundedReadBudget> reads,
            LongSupplier clock,
            com.nereusstream.delay.runtime.TargetSloExportPageReader.Limits pageLimits,
            TargetSloCustodyExportMaintenance.Provider provider,
            long maximumBytes) {
        requireNewTurnsAdmitted();
        if (sloCustodyExport != null
                || writer.sourceWriterWorker() != this
                || writer.resourcesTerminated()
                || !writer.readyForAppending()
                || systemWriterBackends.values().stream().noneMatch(original -> original == writer)) {
            throw new IllegalArgumentException("SLO custody requires one original admitted Source writer");
        }
        workClasses.requireTaskServiceable(new WorkClassTask(
                com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                "target-slo-custody-envelope",
                maximumBytes));
        final var exporter = new TargetSloCustodyExportMaintenance(
                this, writer, executor, reads, clock, pageLimits, provider, maximumBytes);
        target.registerSloCustodyExport(exporter);
        sloCustodyExport = exporter;
    }

    @Override
    public Optional<WorkClassTask> runSloCustodyExportMaintenanceTurn() {
        final var exporter = sloCustodyExport;
        return exporter == null || !target.sloScopeQualified() ? Optional.empty() : exporter.runTurn();
    }

    synchronized com.nereusstream.delay.runtime.TargetSloExportPageReader.Page captureSloExportPage(
            BoundedReadBudget budget,
            com.nereusstream.delay.runtime.TargetSloExportPageReader.Cursor cursor,
            com.nereusstream.delay.runtime.TargetSloExportPageReader.Limits limits,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.captureSloExportPage(budget, cursor, limits, clock);
    }

    synchronized void submitSloExportAction(WorkClassTask task, Runnable action) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireSloScopeQualified();
        workClasses.requireTaskServiceable(task);
        workClasses.submit(task, action);
    }

    synchronized boolean withdrawSloExportAction(WorkClassTask task) {
        return workClasses.withdrawQueued(task);
    }

    synchronized boolean sloExportActionExited(WorkClassTask task) {
        return workClasses.state(task).isEmpty();
    }

    synchronized void requireSloExportContext(
            com.nereusstream.delay.protocol.TargetQuotaMutation cut, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireSloExportContext(cut, clock);
    }

    synchronized void requireSloCustodyWriter(SystemMutation image, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireTargetControlWriter(image, clock);
    }

    synchronized Optional<SystemMutationResult> sloCustodyMutationResult(SystemMutation image, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.targetControlMutationResult(image, clock);
    }

    synchronized boolean hasSloCustody(
            com.nereusstream.delay.protocol.TargetSloObservationExport export, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.hasSloCustody(export, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetSloWriterEpochStore.Applied observeSloWriterEpoch(
            com.nereusstream.delay.protocol.TargetSloWriterContract.Reference reference, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.observeSloWriterEpoch(reference, clock);
    }
    /** Captures actual Native payment/writer facts before the original Source SDK handoff. */
    public TargetSourceApplyRuntime.NativeExposureHold captureNativeExposure(
            SystemMutation exact, long maximumRetainedBytes, LongSupplier clock) {
        final var selected = nativeCaptureContext();
        return selected.runtime().captureNativeExposure(exact, maximumRetainedBytes, clock, selected.admission());
    }
    /** Resolves full Native64 payment on the original Source without granting an SDK frame. */
    public TargetSourceApplyRuntime.NativeExposureHold capturePaidScope(
            com.nereusstream.delay.protocol.TargetSloControlScopeHeader.Reference reference,
            long maximumRetainedBytes,
            LongSupplier clock) {
        final var selected = nativeCaptureContext();
        return selected.runtime().capturePaidScope(reference, maximumRetainedBytes, clock, selected.admission());
    }
    /** Reads the exact original Source fee payer without repricing legacy payment as SDK exposure capacity. */
    public TargetSourceApplyRuntime.NativeExposureHold captureScopeFeeOrigin(
            SystemMutation exact, long maximumRetainedBytes, LongSupplier clock) {
        final var selected = nativeCaptureContext();
        return selected.runtime().captureScopeFeeOrigin(exact, maximumRetainedBytes, clock, selected.admission());
    }

    private record NativeCaptureContext(TargetSourceApplyRuntime runtime, Runnable admission) {}

    private NativeCaptureContext nativeCaptureContext() {
        final NativeCaptureContext selected;
        synchronized (this) {
            requireRuntimeAdmitted();
            resources.requireRuntimeBusinessAdmission();
            selected = new NativeCaptureContext(target, nativeExposureLocalAdmission());
        }
        // Runtime/Store validation and all subsequent authority acquisition run without the Worker monitor.
        selected.runtime().requireWorkerStore(store, resources);
        selected.admission().run();
        return selected;
    }

    private Runnable nativeExposureLocalAdmission() {
        final long epoch = nativeExposureAdmissionEpoch.get();
        final var originalTarget = target;
        final var originalStore = store;
        final var originalResources = resources;
        return () -> {
            if (target != originalTarget
                    || store != originalStore
                    || resources != originalResources
                    || store.sharedResources() != originalResources
                    || backendQuarantined
                    || sourceAndMaintenancePaused
                    || nativeExposureAdmissionEpoch.get() != epoch) {
                throw new IllegalStateException("Native exposure lost its original Worker admission");
            }
            resources.requireRuntimeBusinessAdmission();
            if (backendQuarantined || sourceAndMaintenancePaused || nativeExposureAdmissionEpoch.get() != epoch) {
                throw new IllegalStateException("Native exposure lost its original Worker admission");
            }
        };
    }

    synchronized void requireSloCustodyAppend(
            SystemMutation image,
            ShardLogMutationAppender.AppendOutcome appended,
            com.nereusstream.delay.protocol.TargetQuotaMutation cut,
            LongSupplier clock) {
        requireSloFinalAppend(image, appended, cut, clock);
    }

    @Override
    public Optional<WorkClassTask> settlePendingSloCustodyForDrain(
            SchedulerBudget workBudget, SchedulerBudget sourceBudget, LongSupplier clock) {
        final var exporter = sloCustodyExport;
        if (exporter == null) {
            return Optional.empty();
        }
        exporter.stopNew();
        exporter.retryCleanup();
        final var task = exporter.runTurn();
        if (!exporter.hasPending()) {
            return Optional.empty();
        }
        workClasses.runTurn(workBudget);
        synchronized (this) {
            if (sourceLoop.pendingEntry().isPresent()) {
                sourceLoop.settlePendingEntry(sourceBudget, clock);
            } else {
                sourceLoop.runTurn(sourceBudget, clock);
            }
        }
        exporter.runTurn();
        return exporter.hasPending()
                ? task.or(() -> Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-slo-custody-drain/" + shardId,
                        1)))
                : Optional.empty();
    }

    public synchronized void configureSloFinalContinuations(
            OwnedSourceSystemWriter appender,
            java.util.concurrent.ThreadPoolExecutor originalExecutor,
            TargetSloFinalContinuation.Provider provider,
            long maximumBytes) {
        requireNewTurnsAdmitted();
        Objects.requireNonNull(appender);
        Objects.requireNonNull(originalExecutor);
        Objects.requireNonNull(provider);
        if (appender.sourceWriterWorker() != this
                || appender.resourcesTerminated()
                || !appender.readyForAppending()
                || systemWriterBackends.values().stream().noneMatch(original -> original == appender)) {
            throw new IllegalArgumentException("SLO Final continuation requires the original admitted Source writer");
        }
        if (maximumBytes <= 0 || maximumBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("SLO Final Source envelope must be finite");
        }
        workClasses.requireTaskServiceable(new WorkClassTask(
                com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                "target-slo-final-envelope",
                maximumBytes));
        target.configureSloFinalContinuations((candidates, inspected, entry, outcome) -> new TargetSloFinalContinuation(
                this, appender, originalExecutor, candidates, inspected, entry, outcome, provider, maximumBytes));
    }

    synchronized void submitSloFinalContinuationAction(WorkClassTask task, Runnable action) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireSloScopeQualified();
        workClasses.requireTaskServiceable(task);
        workClasses.submit(task, action);
    }

    synchronized boolean withdrawSloFinalContinuationAction(WorkClassTask task) {
        return workClasses.withdrawQueued(task);
    }

    synchronized boolean sloFinalContinuationActionExited(WorkClassTask task) {
        return workClasses.state(task).isEmpty();
    }

    synchronized void requireSloFinalContinuation(
            SourceReplayEntry entry,
            SourceReplayOutcome outcome,
            com.nereusstream.delay.protocol.TargetQuotaMutation inspected,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireSloScopeQualified();
        target.requireSloFinalContinuation(entry, outcome, inspected, clock);
    }

    synchronized void requireSloFinalWriter(SystemMutation image, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireTargetControlWriter(image, clock);
    }

    synchronized Optional<SystemMutationResult> sloFinalMutationResult(SystemMutation image, LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.targetControlMutationResult(image, clock);
    }

    synchronized void requireSloFinalAppend(
            SystemMutation image,
            ShardLogMutationAppender.AppendOutcome appended,
            com.nereusstream.delay.protocol.TargetQuotaMutation inspected,
            LongSupplier clock) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireTargetControlWriter(image, clock);
        if (appended.disposition() != ShardLogMutationAppender.AppendDisposition.PERSISTED
                || appended.sourcePosition().compareTo(inspected.source()) <= 0
                || appended.sourcePosition().brokerPersistenceTimeEpochMs() > image.retryUntilEpochMs()) {
            throw new IllegalStateException("Final observation lacks later guarded in-window persisted Source");
        }
        target.requireCurrentExpiryLogPosition(
                appended.sourcePosition(),
                appended.sourceConnectionGeneration(),
                appended.guardAttestationDigest(),
                clock);
    }

    synchronized void submitTargetControlAction(
            final WorkClassTask task, final SystemMutation mutation, final LongSupplier clock, final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireTargetControlWriter(mutation, clock);
        workClasses.submit(task, action);
    }

    synchronized boolean targetControlActionExited(WorkClassTask task) {
        return workClasses.state(task).isEmpty();
    }

    synchronized void submitTargetControlCleanupAction(WorkClassTask task, Runnable action) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        workClasses.requireTaskServiceable(task);
        workClasses.submit(task, action);
    }

    synchronized SourceMutationAppend appendTargetControl(
            final SystemMutation mutation, final ShardLogMutationAppender appender, final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        final var first = target.targetControlMutationResult(mutation, clock).orElse(null);
        if (first != null) {
            return new SourceMutationAppend(first, null);
        }
        target.requireTargetControlWriter(mutation, clock);
        try {
            final var appended = Objects.requireNonNull(appender.append(mutation), "Target Control append");
            if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                target.requireCurrentExpiryLogPosition(
                        appended.sourcePosition(),
                        appended.sourceConnectionGeneration(),
                        appended.guardAttestationDigest(),
                        clock);
            }
            return new SourceMutationAppend(null, appended);
        } catch (RuntimeException | Error uncertainAppend) {
            target.fence();
            throw uncertainAppend;
        }
    }

    synchronized Optional<SystemMutationResult> targetControlMutationResult(
            final SystemMutation mutation, final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.targetControlMutationResult(mutation, clock);
    }

    synchronized ShardLogMutationAppender.AppendOutcome appendOutcome(
            final com.nereusstream.delay.protocol.SystemMutation mutation,
            final ShardLogMutationAppender appender,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireOutcomeWriter(mutation, ownerClock);
        final var appended = Objects.requireNonNull(appender.append(mutation), "Target Outcome append");
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            target.requireCurrentExpiryLogPosition(
                    appended.sourcePosition(),
                    appended.sourceConnectionGeneration(),
                    appended.guardAttestationDigest(),
                    ownerClock);
        }
        return appended;
    }

    synchronized Optional<SystemMutationResult> outcomeMutationResult(
            final com.nereusstream.delay.protocol.SystemMutation mutation, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.outcomeMutationResult(mutation, ownerClock);
    }

    /** Reads one bounded persisted active-message total for optional queue-depth telemetry. */
    public synchronized OptionalLong readTargetActiveMessages(
            final BoundedReadBudget budget, final TargetPartitionId targetId, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readTargetActiveMessages(budget, targetId, ownerClock);
    }
    /** Metadata only, including a Source-released INLINE identity; no historical payload read is granted. */
    public synchronized java.util.Optional<com.nereusstream.delay.runtime.TargetMessageMetadataStore.Snapshot>
            readMessageMetadata(
                    BoundedReadBudget budget,
                    com.nereusstream.delay.protocol.DelayMessageId id,
                    LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readMessageMetadata(budget, id, ownerClock);
    }

    /** Rechecks this source Shard's Store revision after paged head enumeration. */
    public synchronized TargetQueueSnapshotReader.Cut readTargetQueueCut(
            final BoundedReadBudget budget, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readTargetQueueCut(budget, ownerClock);
    }

    /** Reads one guarded message-expiry candidate without applying or appending it. */
    public synchronized TargetExpiryDiscoveryStore.Discovery discoverMessageExpiry(
            final BoundedReadBudget budget,
            final TargetExpiryDiscoveryStore.Cursor cursor,
            final TrustedUtcIntervalEvidence evidence,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverMessageExpiry(budget, cursor, evidence, ownerClock);
    }

    /** Creates this Shard's single source-preserving expiry handoff on the same Worker graph. */
    public synchronized TargetMessageExpiryWorkClassExecutor newMessageExpiryWorkClassExecutor(
            final ShardLogMutationAppender appender) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (messageExpiryHandoff != null) {
            throw new IllegalStateException("Target Worker message expiry executor is already created");
        }
        messageExpiryHandoff = new TargetMessageExpiryWorkClassExecutor(this, appender);
        return messageExpiryHandoff;
    }

    /**
     * Configures periodic discovery and append using per-turn trusted proof and signing inputs.
     * Call before Host maintenance starts; the provider must return a fresh bounded read budget.
     */
    public synchronized TargetMessageExpiryWorkClassExecutor configureMessageExpiryMaintenance(
            final ShardLogMutationAppender appender, final Supplier<MessageExpiryRequest> requestProvider) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (messageExpiryHandoff != null || messageExpiryMaintenance != null) {
            throw new IllegalStateException("Target Worker message expiry maintenance is already configured");
        }
        final var exactAppender = Objects.requireNonNull(appender, "appender");
        final var exactRequestProvider = Objects.requireNonNull(requestProvider, "requestProvider");
        final var handoff = new TargetMessageExpiryWorkClassExecutor(this, exactAppender);
        final var configured = new TargetMessageExpiryMaintenance(this, handoff, exactRequestProvider, ownerClock);
        messageExpiryHandoff = handoff;
        messageExpiryMaintenance = configured;
        return handoff;
    }

    synchronized void requireMessageExpiryMaintenanceConfigured() {
        if (messageExpiryMaintenance == null) {
            throw new IllegalStateException(
                    "Target Worker message expiry maintenance must be configured before Host start");
        }
    }

    synchronized void submitMessageExpiryAction(
            final WorkClassTask task,
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final LongSupplier ownerClock,
            final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireExpirySubmission(candidate, evidence, owner, ownerClock);
        workClasses.submit(Objects.requireNonNull(task, "task"), Objects.requireNonNull(action, "action"));
    }

    synchronized ShardLogMutationAppender.AppendOutcome appendMessageExpiry(
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final com.nereusstream.delay.protocol.SystemMutation mutation,
            final ShardLogMutationAppender appender,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireExpiryAuthoritativelyStrict(candidate, evidence, owner, ownerClock);
        final var appended = Objects.requireNonNull(
                appender.append(Objects.requireNonNull(mutation, "mutation")), "Shard Log append outcome");
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            target.requireCurrentExpiryLogPosition(
                    appended.sourcePosition(),
                    appended.sourceConnectionGeneration(),
                    appended.guardAttestationDigest(),
                    ownerClock);
        }
        return appended;
    }

    synchronized boolean messageExpiryAppendApplied(
            final com.nereusstream.delay.protocol.SourcePosition position, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.expiryAppendApplied(position, ownerClock);
    }

    synchronized Optional<SystemMutationResult> messageExpiryMutationResult(
            final SystemMutation mutation, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.expiryMutationResult(mutation, ownerClock);
    }

    synchronized void fenceMessageExpiry() {
        target.fence();
    }

    /** Reads the frozen cost of one current head; Claim still rechecks Store and live authority. */
    public synchronized TargetHeadCostProbe.Cost probeSelectedHead(
            final BoundedReadBudget budget, final TargetHeadRef selected, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.probeSelectedHead(budget, selected, ownerClock);
    }

    /** Supplies Claim-time Native trust from this Worker's source-applied Shard Store. */
    public synchronized TargetNativePolicyTrustStore nativePolicyTrustStore(final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.nativePolicyTrustStore(ownerClock);
    }

    /** Admits only an ACK-settled active Shard to the bound, unpublished CHECKPOINT work class. */
    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitLocalCheckpointCandidate(
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending source acknowledgement");
        }
        if (maintenance.hasPendingTurn()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending GC action");
        }
        requireMessageExpirySettledForCut();
        final var submitted = target.submitLocalCheckpointCandidate(
                workClasses, intents, ownerClock, checkpointPath, pending, physicalLimits, quotaLimits, ledgerLimits);
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    /** Scheduled candidates additionally require an exact broker-confirmed source cut. */
    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedCheckpointCandidate(
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return submitProtectedCheckpointCandidate(
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                protectCheckpointCut());
    }

    /** Captures the cut before a scheduler creates its pending intent, under the exact host Shard lock. */
    synchronized SourceRecordConsumer.CheckpointCut protectCheckpointCut() {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (maintenance.hasPendingTurn()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending GC action");
        }
        requireMessageExpirySettledForCut();
        final var cut = sourceLoop.checkpointCut(store.appliedShardLogPosition());
        preparedCheckpointCut = cut;
        return cut;
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedCheckpointCandidate(
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final SourceRecordConsumer.CheckpointCut cut) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut")
                || maintenance.hasPendingTurn()
                || hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target checkpoint cut is no longer prepared for this Shard");
        }
        cut.requireCurrent();
        final var submitted = target.submitLocalCheckpointCandidate(
                workClasses,
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                cut::requireCurrent);
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    /** Persists exact semantic bytes under this Worker's ACK/GC/Owner cut; the result remains unpublished. */
    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission
            submitProtectedSemanticCheckpointCandidate(
                    final CheckpointUploadIntentAuthority intents,
                    final LongSupplier ownerClock,
                    final Path checkpointPath,
                    final CheckpointUploadIntent pending,
                    final CheckpointManifestLimits physicalLimits,
                    final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
                    final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
                    final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
                    final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
                    final Runnable protectionGuard) {
        return submitProtectedSemanticCheckpointCandidate(
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                snapshot,
                semanticLimits,
                protectionGuard,
                protectCheckpointCut());
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedSemanticCheckpointCandidate(
            CheckpointUploadIntentAuthority intents,
            LongSupplier ownerClock,
            Path checkpointPath,
            CheckpointUploadIntent pending,
            CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protectionGuard,
            SourceRecordConsumer.CheckpointCut cut) {
        return submitProtectedSemanticCheckpointCandidate(
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                snapshot,
                semanticLimits,
                protectionGuard,
                cut,
                null);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedSemanticCheckpointCandidate(
            CheckpointUploadIntentAuthority intents,
            LongSupplier ownerClock,
            Path checkpointPath,
            CheckpointUploadIntent pending,
            CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protectionGuard,
            SourceRecordConsumer.CheckpointCut cut,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication) {
        return submitProtectedSemanticCheckpointCandidate(
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                snapshot,
                semanticLimits,
                protectionGuard,
                cut,
                publication,
                null);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedSemanticCheckpointCandidate(
            CheckpointUploadIntentAuthority intents,
            LongSupplier ownerClock,
            Path checkpointPath,
            CheckpointUploadIntent pending,
            CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protectionGuard,
            SourceRecordConsumer.CheckpointCut cut,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication,
            SchedulerBudget settlementBudget) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut")
                || maintenance.hasPendingTurn()
                || hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target semantic checkpoint cut is no longer prepared for this Shard");
        }
        cut.requireCurrent();
        bindEvidenceRecoveryCatalog(publication);
        final var submitted = target.submitSemanticCheckpointCandidate(
                workClasses,
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                snapshot,
                semanticLimits,
                cut::requireCurrent,
                protectionGuard,
                publication,
                settlementBudget);
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission
            submitCollectedSemanticCheckpointCandidate(
                    TargetCheckpointCandidateSchedule.CollectedSemanticRequest request) {
        return submitCollectedSemanticCheckpointCandidate(
                Objects.requireNonNull(request, "request"), protectCheckpointCut());
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission
            submitDeferredCollectedSemanticCheckpointCandidate(
                    TargetCheckpointCandidateSchedule.DeferredPreparation preparation,
                    SourceRecordConsumer.CheckpointCut cut,
                    SchedulerBudget settlementBudget) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut")
                || maintenance.hasPendingTurn()
                || hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target deferred checkpoint cut is no longer prepared for this Shard");
        }
        Objects.requireNonNull(preparation, "preparation");
        cut.requireCurrent();
        final Runnable localGuard = () -> {
            requireRuntimeAdmitted();
            resources.requireRuntimeBusinessAdmission();
            if (target.fenced() || store.isCloseStarted() || store.isWriteOutcomeUncertain()) {
                throw new IllegalStateException("Target deferred checkpoint Owner/Store is fenced");
            }
            cut.requireCurrent();
        };
        final var expectedLease = target.snapshotDrainLease();
        final var submitted = TargetCheckpointCandidateWorkClassExecutor.submitDeferredCollectedSemantic(
                workClasses,
                preparation.localTaskIdentity(),
                preparation.maximumTaskBytes(),
                preparation,
                localGuard,
                original -> {
                    target.requireCheckpointReadCapability();
                    final var ready = Objects.requireNonNull(preparation.preparedRequest(), "prepared request");
                    localGuard.run();
                    if (ready.isEmpty()) {
                        return;
                    }
                    final var request = ready.orElseThrow();
                    final var candidate = request.candidate();
                    bindEvidenceRecoveryCatalog(request.publication());
                    final var executor = new TargetCheckpointCandidateWorkClassExecutor(
                            workClasses,
                            store,
                            target.drainAuthority(),
                            candidate.intents(),
                            candidate.ownerClock(),
                            localGuard,
                            settlementBudget);
                    executor.continueCollectedSemantic(
                            original,
                            new TargetCheckpointCandidateWorkClassExecutor.Request(
                                    candidate.checkpointPath(),
                                    candidate.pending(),
                                    expectedLease,
                                    candidate.physicalLimits(),
                                    candidate.quotaLimits(),
                                    candidate.ledgerLimits()),
                            request.inputs(),
                            request.semanticLimits(),
                            request.protection(),
                            request.publication(),
                            request.registration());
                },
                preparation::accepted,
                settlementBudget,
                preparation.checkpointExecutor());
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitCollectedSemanticCheckpointCandidate(
            TargetCheckpointCandidateSchedule.CollectedSemanticRequest request,
            SourceRecordConsumer.CheckpointCut cut) {
        return submitCollectedSemanticCheckpointCandidate(request, cut, null);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitCollectedSemanticCheckpointCandidate(
            TargetCheckpointCandidateSchedule.CollectedSemanticRequest request,
            SourceRecordConsumer.CheckpointCut cut,
            SchedulerBudget settlementBudget) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut")
                || maintenance.hasPendingTurn()
                || hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target semantic collection cut is no longer prepared for this Shard");
        }
        cut.requireCurrent();
        final var candidate = request.candidate();
        bindEvidenceRecoveryCatalog(request.publication());
        final var submitted = target.submitCollectedSemanticCheckpointCandidate(
                workClasses,
                candidate.intents(),
                candidate.ownerClock(),
                candidate.checkpointPath(),
                candidate.pending(),
                candidate.physicalLimits(),
                candidate.quotaLimits(),
                candidate.ledgerLimits(),
                request.inputs(),
                request.semanticLimits(),
                cut::requireCurrent,
                request.protection(),
                request.publication(),
                settlementBudget,
                request.registration());
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    /** Requests local registration withdrawal before Host drain; an active RPC remains owned. */
    @Override
    public synchronized void requestCheckpointRegistrationStop() {
        if (pendingCheckpoint != null) {
            pendingCheckpoint.requestRegistrationStop();
        }
    }

    @Override
    public synchronized void requestCheckpointPreparationStop() {
        if (pendingCheckpoint != null) {
            pendingCheckpoint.requestPreparationStop();
        }
    }

    /** Runs one shared bounded turn and releases the local source/maintenance cut only after settlement. */
    public Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> runCheckpointTurn(
            final SchedulerBudget budget) {
        final TargetCheckpointCandidateWorkClassExecutor.Submission checkpoint;
        synchronized (this) {
            checkpoint = pendingCheckpoint;
            if (checkpoint == null || checkpointTurnInProgress) {
                return Optional.empty();
            }
            checkpointTurnInProgress = true;
        }
        try {
            if (!checkpoint.settleFailedRegistrationIo() && checkpoint.outcome().isEmpty()) {
                if (!checkpoint.usesOwnedPreparation()) {
                    resources.requireRuntimeBusinessAdmission();
                }
                checkpoint.enqueueNextPreparation();
                workClasses.runTurn(Objects.requireNonNull(budget, "budget"));
            }
            checkpoint.settleFailedRegistrationIo();
            final var outcome = checkpoint.outcome();
            synchronized (this) {
                if (outcome.isPresent() && pendingCheckpoint == checkpoint) {
                    pendingCheckpoint = null;
                }
            }
            return outcome;
        } finally {
            synchronized (this) {
                checkpointTurnInProgress = false;
            }
        }
    }

    /** Exact queued checkpoint task, if it has not yet reached a terminal outcome. */
    public synchronized Optional<WorkClassTask> pendingCheckpointTask() {
        return pendingCheckpoint == null || pendingCheckpoint.outcome().isPresent()
                ? Optional.empty()
                : Optional.of(pendingCheckpoint.task());
    }

    /** Local original-Owner identity checks for consuming a separately held publication validation. */
    synchronized void requirePublishedCheckpointOwnerCurrent(
            TargetCheckpointCandidateWorkClassExecutor.Submission original) {
        requireRuntimeAdmitted();
        resources.requireRuntimeBusinessAdmission();
        final var publication = original.publication()
                .orElseThrow(
                        () -> new IllegalStateException("Target checkpoint has no original successful publication"));
        final var pending = publication.uploadIntent();
        final var lease = target.snapshotDrainLease();
        if (target.fenced()
                || store.isCloseStarted()
                || store.isWriteOutcomeUncertain()
                || !lease.validAt(ownerClock.getAsLong())
                || lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || lease.ownerEpoch() != pending.owner().ownerEpoch()
                || !com.nereusstream.delay.protocol.Bytes.constantTimeEquals(
                        lease.leaseToken(), pending.owner().leaseFencingDigest())
                || !com.nereusstream.delay.protocol.Bytes.constantTimeEquals(
                        store.metadata().storeIncarnation(), pending.sourceStoreIncarnation())) {
            throw new IllegalStateException("Target published checkpoint lost its original local Owner/Store");
        }
    }

    /** Exact nonterminal provider failure, including its original native cursor cleanup handle. */
    public synchronized Optional<Throwable> pendingCheckpointIoFailure() {
        return pendingCheckpoint == null ? Optional.empty() : pendingCheckpoint.pendingIoFailure();
    }

    /** Enqueues one original-provider cleanup step; failure remains nonterminal until actual cleanup/step exit. */
    public synchronized void retryPendingCheckpointIoCleanup() {
        if (pendingCheckpoint != null) {
            pendingCheckpoint.retryPendingIoCleanup();
        }
    }

    public Throwable pendingSourceAuthorityFailure() {
        return target.authorityIoFailure();
    }

    public void retryPendingSourceAuthorityCleanup() {
        target.retryAuthorityCleanup();
    }
    /** Final shared-I/O release gate; live SDK/manager loans do not change Source producer-close ordering. */
    public boolean nativeReadLoansExited() {
        return target.nativeReadLoansExited();
    }

    /** Stops new source polls and maintenance submissions after retained maintenance work settles. */
    public synchronized void pauseNewTurns() {
        requireCheckpointSettled();
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target Worker cannot pause a pending source acknowledgement");
        }
        final var finalMaintenance = sloFinalMaintenance;
        if (finalMaintenance != null) {
            finalMaintenance.stopNew();
            if (!finalMaintenance.ioExited() || finalMaintenance.hasPending()) {
                throw new IllegalStateException("Target Worker cannot pause retained SLO Final maintenance");
            }
        }
        final var exporter = sloCustodyExport;
        if (exporter != null) {
            exporter.stopNew();
            if (!exporter.ioExited() || exporter.hasPending()) {
                throw new IllegalStateException("Target Worker cannot pause retained SLO custody export");
            }
        }
        if (hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target Worker cannot pause a pending message expiry handoff");
        }
        if (channelControlHandoff != null && channelControlHandoff.pending().isPresent()) {
            throw new IllegalStateException(
                    "Target Worker cannot pause a pending channel/certification Control submission");
        }
        if (retirementHandoff != null && retirementHandoff.pending().isPresent()) {
            throw new IllegalStateException("Target Worker cannot pause a retained retirement Source submission");
        }
        if (resourceReclaim != null && !resourceReclaim.ioExited()) {
            throw new IllegalStateException("Target Worker cannot pause retained Native GC inputs or owned I/O");
        }
        if (publishRuntime != null && !publishRuntime.available()) {
            throw new IllegalStateException("Target Worker cannot pause a retained publication flight");
        }
        if (journalSettlement != null && !journalSettlement.completedForSource(store.shardMutationSequence())) {
            throw new IllegalStateException("Target Worker cannot pause an unfinished Journal settlement sweep");
        }
        if (publicationResources.values().stream().anyMatch(owner -> !owner.closed())) {
            throw new IllegalStateException("Target Worker cannot pause live publication resources");
        }
        if (!publicationMaintainers.isEmpty()) {
            throw new IllegalStateException("Target Worker cannot pause retained publication preparation");
        }
        if (journalBackend != null && !journalBackend.closed()) {
            throw new IllegalStateException("Target Worker cannot pause a live managed Journal");
        }
        sourceAndMaintenancePaused = true;
        nativeExposureAdmissionEpoch.incrementAndGet();
        preparedCheckpointCut = null;
    }

    /** Runs only the exact GC action already queued when admission was paused. */
    public synchronized Optional<TargetReservationGcRuntime.Turn> settlePendingMaintenance(
            final SchedulerBudget budget) {
        resources.requireRuntimeBusinessAdmission();
        return maintenance.settlePendingTurn(Objects.requireNonNull(budget, "budget"));
    }

    @Override
    public synchronized Optional<WorkClassTask> settlePendingGcForDrain(SchedulerBudget budget) {
        return settlePendingMaintenance(budget)
                .filter(TargetReservationGcRuntime.Turn::pending)
                .map(TargetReservationGcRuntime.Turn::task);
    }

    @Override
    public synchronized boolean canPublishFinalCheckpoint() {
        return !sourceAndMaintenancePaused
                && !target.fenced()
                && !store.isCloseStarted()
                && !store.isWriteOutcomeUncertain();
    }

    @Override
    public synchronized long checkpointStoreSequence() {
        return store.latestSequenceNumber();
    }

    @Override
    public Optional<WorkClassTask> settlePendingPublicationForDrain(
            final SchedulerBudget workBudget, final SchedulerBudget sourceBudget, final LongSupplier clock) {
        final var publisher = publishRuntime;
        if (publisher == null || publisher.available()) {
            final var recovery = publishRecoveryMaintenance;
            if (recovery != null && recovery.pendingForDrain()) {
                recovery.retryLoadingCleanup();
                final var loaded = recovery.runTurn();
                if (loaded.failure() != null) {
                    throw new IllegalStateException("retained recovery could not settle", loaded.failure());
                }
                workClasses.runTurn(workBudget);
                synchronized (this) {
                    requireNewTurnsAdmitted();
                    if (sourceLoop.pendingEntry().isPresent()) {
                        sourceLoop.settlePendingEntry(sourceBudget, clock);
                    } else {
                        sourceLoop.runTurn(sourceBudget, clock);
                    }
                }
                if (recovery.pendingForDrain()) {
                    return Optional.of(new WorkClassTask(
                            com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                            "target-recovery-drain/" + shardId,
                            1));
                }
            }
            return settleJournalForDrain(clock);
        }
        final var turn = publisher.runTurn();
        if (turn.failure() != null) {
            throw new IllegalStateException("retained publication could not settle", turn.failure());
        }
        if (publisher.available()) {
            return settleJournalForDrain(clock);
        }
        workClasses.runTurn(workBudget);
        synchronized (this) {
            requireNewTurnsAdmitted();
            if (sourceLoop.pendingEntry().isPresent()) {
                sourceLoop.settlePendingEntry(sourceBudget, clock);
            } else {
                sourceLoop.runTurn(sourceBudget, clock);
            }
        }
        final var settled = publisher.runTurn();
        if (settled.failure() != null) {
            throw new IllegalStateException("retained publication Source turn could not settle", settled.failure());
        }
        final var task = publisher.pendingTask();
        return task.isPresent() ? task : settleJournalForDrain(clock);
    }

    private Optional<WorkClassTask> settleJournalForDrain(final LongSupplier clock) {
        final var configured = journalSettlement;
        if (configured == null) {
            return Optional.empty();
        }
        final var turn = configured.runTurn();
        if (turn.failure() != null) {
            throw new IllegalStateException("Source-decided Journal cleanup is unsettled", turn.failure());
        }
        return configured.pending()
                ? Optional.of(new WorkClassTask(
                        com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-journal-settlement/" + shardId,
                        1))
                : Optional.empty();
    }

    @Override
    public Optional<WorkClassTask> settlePendingChannelControlForDrain(
            final SchedulerBudget workBudget, final SchedulerBudget sourceBudget, final LongSupplier clock) {
        final var handoff = channelControlHandoff;
        if (handoff == null || handoff.pending().isEmpty()) {
            return Optional.empty();
        }
        if (handoff.settle(clock).isPresent()) {
            return Optional.empty();
        }
        handoff.retryPending(clock);
        workClasses.runTurn(Objects.requireNonNull(workBudget, "workBudget"));
        if (handoff.settle(clock).isPresent()) {
            return Optional.empty();
        }
        synchronized (this) {
            requireNewTurnsAdmitted();
            if (sourceLoop.pendingEntry().isPresent()) {
                sourceLoop.settlePendingEntry(sourceBudget, clock);
            } else {
                sourceLoop.runTurn(sourceBudget, clock);
            }
        }
        if (handoff.settle(clock).isPresent()) {
            return Optional.empty();
        }
        return handoff.pending().map(TargetChannelControlWorkClassExecutor.Submission::task);
    }

    @Override
    public Optional<WorkClassTask> settlePendingRetirementForDrain(
            SchedulerBudget workBudget, SchedulerBudget sourceBudget, LongSupplier clock) {
        final var handoff = retirementHandoff;
        if (handoff == null || handoff.pending().isEmpty()) {
            return Optional.empty();
        }
        if (handoff.pending().orElseThrow().releaseIfNeverAppended()) {
            return Optional.empty();
        }
        if (handoff.settle(clock).isPresent()) {
            return Optional.empty();
        }
        handoff.retryPending(clock);
        workClasses.runTurn(workBudget);
        if (handoff.pending()
                .filter(TargetEvidenceRetirementWorkClassExecutor.Submission::releaseIfNeverAppended)
                .isPresent()) {
            return Optional.empty();
        }
        if (handoff.settle(clock).isPresent()) {
            return Optional.empty();
        }
        synchronized (this) {
            requireNewTurnsAdmitted();
            if (sourceLoop.pendingEntry().isPresent()) {
                sourceLoop.settlePendingEntry(sourceBudget, clock);
            } else {
                sourceLoop.runTurn(sourceBudget, clock);
            }
        }
        if (handoff.settle(clock).isPresent()) {
            return Optional.empty();
        }
        return handoff.pending().map(TargetEvidenceRetirementWorkClassExecutor.Submission::task);
    }

    /** Advances only the retained expiry append and its source result by one bounded drain step. */
    @Override
    public Optional<WorkClassTask> settlePendingMessageExpiryForDrain(
            final SchedulerBudget workBudget, final SchedulerBudget sourceBudget, final LongSupplier ownerClock) {
        final var handoff = messageExpiryHandoff;
        if (handoff == null) {
            return Optional.empty();
        }
        final var submission = handoff.pendingSubmission();
        if (submission == null) {
            return Optional.empty();
        }
        final var exactWorkBudget = Objects.requireNonNull(workBudget, "workBudget");
        final var exactSourceBudget = Objects.requireNonNull(sourceBudget, "sourceBudget");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        if (submission.result().isEmpty()) {
            workClasses.runTurn(exactWorkBudget);
        }
        handoff.settlePending(clock);
        if (handoff.pendingSubmission() != null) {
            final var result = submission.result().orElse(null);
            if (result != null
                    && (result.kind() == TargetMessageExpiryWorkClassExecutor.ResultKind.APPENDED
                            || result.kind() == TargetMessageExpiryWorkClassExecutor.ResultKind.UNKNOWN)) {
                if (sourceLoop.pendingEntry().isPresent()) {
                    sourceLoop.settlePendingEntry(exactSourceBudget, clock);
                } else {
                    sourceLoop.runTurn(exactSourceBudget, clock);
                }
                handoff.settlePending(clock);
            }
        }
        final var pending = handoff.pendingSubmission();
        return pending == null ? Optional.empty() : Optional.of(pending.task());
    }

    public synchronized Optional<SourceReplayEntry> pendingSourceEntry() {
        return sourceLoop.pendingEntry();
    }

    /** Retries one retained source apply/ACK without polling a new record before drain. */
    public synchronized Optional<SourceApplyCoordinator.TurnResult> settlePendingSourceTurn(
            final SchedulerBudget budget, final LongSupplier ownerClock) {
        if (sourceAndMaintenancePaused) {
            throw new IllegalStateException("Target Worker source and maintenance admission is paused");
        }
        if (sourceLoop.pendingEntry().isEmpty()) {
            return Optional.empty();
        }
        if (sourceLoop.pendingRequiresApply()) {
            resources.requireRuntimeBusinessAdmission();
        }
        return sourceLoop.settlePendingEntry(
                Objects.requireNonNull(budget, "budget"), Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    /** Runs or retries strict Target drain after the host has closed its maintenance loop. */
    public synchronized TargetOwnerDrainCoordinator.Result drain(
            final TargetOwnerDrainCoordinator.Request request, final LongSupplier clock) {
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target Worker cannot drain a pending source acknowledgement");
        }
        pauseNewTurns();
        return drainCoordinator.drain(request, clock);
    }

    /** Closes the native source when no ACK is pending; Owner drain remains the host's responsibility. */
    public synchronized void closeSource() {
        requireCheckpointSettled();
        sourceLoop.close();
    }

    private void requireRuntimeAdmitted() {
        if (backendQuarantined || sourceBackendGroup != null && !sourceBackendGroup.workerReady(this)) {
            throw new IllegalStateException("Target Worker backend is unbound or quarantined");
        }
        if (sourceAndMaintenancePaused) {
            throw new IllegalStateException("Target Worker source and maintenance admission is paused");
        }
    }

    private void requireNewTurnsAdmitted() {
        requireRuntimeAdmitted();
        requireCheckpointSettled();
    }

    void requireOxiaAuthority(
            OxiaOwnerLeaseStore expected, WorkClassExecutionRegistry classes, SharedRocksDbResources resources) {
        if (target.drainAuthority() != expected || workClasses != classes || this.resources != resources) {
            throw new IllegalArgumentException("Worker does not belong to the original Oxia/resource graph");
        }
    }

    void requireSourceStartup(TargetSourceApplyRuntime expected, ShardStore actualStore) {
        if (target != expected || store != actualStore) {
            throw new IllegalArgumentException("Worker belongs to another original Source startup/Store");
        }
    }

    /** Only a privately activated original runtime can acquire this exact owned active adapter's delivery proof. */
    public synchronized void bindActualSourceSuccessor(TargetWorkerSourceRuntimeFactory.Activated activation) {
        activation.requireGraph(workClasses, resources, store);
        requireSourceStartup(activation.runtime(), store);
        if (publicationHost != null || sourceBackendGroup == null) {
            throw new IllegalStateException("active Source proof requires its original pre-Host owned consumer");
        }
        target.bindActiveSourceSuccessor(() -> actualSourceConsumer.successorAfter(store.appliedShardLogPosition()));
    }

    /** The completed reader's original shared SDK owner participates in the same Host composition/close preflight. */
    synchronized void retainRecoveryBackend(TargetOwnedBackendGroup actual) {
        if (actual == null) {
            return;
        }
        actual.requireWorkerGraph(workClasses, resources);
        if (publicationHost != null || recoveryBackend != null && recoveryBackend != actual) {
            throw new IllegalStateException("recovery SDK owner cannot transfer to another Worker/Host");
        }
        recoveryBackend = actual;
    }

    synchronized void retainRecoveryInputBackend(TargetWorkerSourceStartup original, TargetOwnedBackendGroup actual) {
        if (actual == null
                || actual == sourceBackendGroup
                || actual == recoveryBackend
                || actual == priorRecoveryBackend) {
            return;
        }
        original.requireRecoveryInputBackend(actual);
        actual.requireWorkerGraph(workClasses, resources);
        if (publicationHost != null || !recoveryInputBackends.contains(actual) && recoveryInputBackends.size() == 4) {
            throw new IllegalStateException(
                    "recovery input SDK owners cannot be replaced or added after Host transfer");
        }
        recoveryInputBackends.add(actual);
    }

    synchronized void retainPriorRecoveryBackend(
            TargetWorkerSourceStartup.PulsarPreparation original, TargetOwnedBackendGroup actual) {
        if (actual == null || actual == sourceBackendGroup || actual == recoveryBackend) {
            return;
        }
        original.requirePriorBackend(actual);
        actual.requireWorkerGraph(workClasses, resources);
        if (publicationHost != null || priorRecoveryBackend != null && priorRecoveryBackend != actual) {
            throw new IllegalStateException("previous Source preparation SDK owner cannot be replaced");
        }
        priorRecoveryBackend = actual;
    }

    public TargetOwnedBackendGroup sourceBackendGroup() {
        return sourceBackendGroup;
    }

    public boolean ownsSourceConsumer(SourceRecordConsumer expected) {
        return actualSourceConsumer == expected;
    }

    /** Does not wait for Source ACK or SDK reads holding the Worker monitor; it fences their later Store commits. */
    void quarantineBackendWrites() {
        requestResourceReclaimStop();
        backendQuarantined = true;
        sourceAndMaintenancePaused = true;
        nativeExposureAdmissionEpoch.incrementAndGet();
        drainCoordinator.fenceNormalDrain();
        target.fence();
        store.fenceForUncleanClose();
        final var checkpoint = pendingCheckpoint;
        if (checkpoint != null) {
            checkpoint.requestPreparationStop();
        }
    }

    synchronized Optional<TargetOwnerDrainCoordinator.ReplayExit> exitQuarantinedForReplay(
            TargetOwnerDrainCoordinator.Request request, LongSupplier clock) {
        if (!backendQuarantined) {
            throw new IllegalStateException("Worker is not quarantined");
        }
        final var publications = stopPublicationMaintenanceForExit();
        if (sourceBackendGroup == null || !sourceBackendGroup.resourcesTerminated()) {
            return Optional.empty();
        }
        target.stopAuthorityPreparation();
        target.retryAuthorityCleanup();
        final var reclaim = resourceReclaim;
        if (reclaim != null) {
            reclaim.runQuarantinedTurn();
        }
        if (!target.authorityIoExited() || reclaim != null && !reclaim.ioExited()) {
            return Optional.empty();
        }
        if (publications.isPresent()) {
            return Optional.empty();
        }
        if (pendingCheckpoint != null) {
            pendingCheckpoint.retryPendingIoCleanup();
            if (!pendingCheckpoint.ownedIoExited()) {
                return Optional.empty();
            }
        }
        for (var owner : publicationResources.values()) {
            final var backend = owner.ownedBackendGroup();
            if (backend != null
                    && backend.resourcesTerminated()
                    && owner.channelIdentity() != null
                    && publishRuntime != null
                    && !publishRuntime.releasePhysicalAfterBackendTermination(owner.channelIdentity())) {
                return Optional.empty();
            }
            if (!owner.settleClose()) {
                return Optional.empty();
            }
        }
        if (journalBackend != null && !journalBackend.settleClose()) {
            return Optional.empty();
        }
        return Optional.of(drainCoordinator.exitForReplay(request, clock, sourceBackendGroup));
    }

    private void requireCheckpointSettled() {
        if (pendingCheckpoint != null && pendingCheckpoint.outcome().isPresent()) {
            pendingCheckpoint = null;
        }
        if (pendingCheckpoint != null) {
            throw new IllegalStateException("Target Worker checkpoint cut is still pending");
        }
    }

    private void requireMessageExpirySettledForCut() {
        if (hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target checkpoint cannot cut a pending message expiry handoff");
        }
    }

    private boolean hasUnsettledMessageExpiry() {
        return messageExpiryHandoff != null && messageExpiryHandoff.hasUnsettledSubmission();
    }
}
