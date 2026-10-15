package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
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
    private volatile TargetWorkerHostRuntime publicationHost;
    private final TargetOwnedBackendGroup sourceBackendGroup;
    private final SourceRecordConsumer actualSourceConsumer;
    private TargetOwnedBackendGroup recoveryBackend;
    private volatile TargetMessageExpiryWorkClassExecutor messageExpiryHandoff;
    private volatile TargetMessageExpiryMaintenance messageExpiryMaintenance;
    private volatile TargetCheckpointCandidateWorkClassExecutor.Submission pendingCheckpoint;
    private TargetPublishRecoveryMaintenance publishRecoveryMaintenance;
    private volatile TargetJournalNegativeSettlementMaintenance journalSettlement;
    private volatile TargetPulsarPublishRuntime publishRuntime;
    private volatile TargetChannelControlWorkClassExecutor channelControlHandoff;
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
        if (publicationHost == host) { publicationHost = null; }
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
            com.nereusstream.delay.protocol.TargetChannelIdentity channel, boolean newWork, BoundedReadBudget reads, LongSupplier clock) {
        requireNewTurnsAdmitted();resources.requireRuntimeBusinessAdmission();
        return target.requirePublicationChannel(channel,newWork,reads,clock);
    }

    public synchronized com.nereusstream.delay.protocol.CanonicalTargetPartition requireRetainedPublicationChannel(
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,BoundedReadBudget reads,LongSupplier clock) {
        requireNewTurnsAdmitted();resources.requireRuntimeBusinessAdmission();
        return target.requireRetainedPublicationChannel(channel,reads,clock);
    }

    public synchronized com.nereusstream.delay.protocol.TargetQuotaAccounting readPublicationAccounting(
            BoundedReadBudget reads,com.nereusstream.delay.protocol.TargetChannelIdentity channel,LongSupplier clock) {
        requireNewTurnsAdmitted();resources.requireRuntimeBusinessAdmission();return target.readPublicationAccounting(reads,channel,clock);
    }

    public interface PublicationResources {
        boolean settleClose();
        boolean closed();
        boolean readyForPublishing();
        default TargetOwnedBackendGroup ownedBackendGroup() { return null; }
        default com.nereusstream.delay.protocol.TargetChannelIdentity channelIdentity() { return null; }
        default LocalBrokerMtlsResolver.Receipt credentialReceipt() { return null; }
    }
    private final java.util.Map<String,PublicationResources> publicationResources=new java.util.HashMap<>();
    private final java.util.Map<String,OwnedSourceSystemWriter> systemWriterBackends = new java.util.HashMap<>();
    public void registerSystemWriterBackend(String identity, OwnedSourceSystemWriter backend,
            TargetDestinationSdkBudget budget, LongSupplier clock) {
        final TargetWorkerHostRuntime host;
        synchronized (this) {
            requirePublishRecoveryOwner(clock); budget.requireWorker(workClasses, resources);
            backend.ownedBackendGroup().requireWorkerGraph(workClasses, resources);
            systemWriterBackends.entrySet().removeIf(entry -> entry.getValue().resourcesTerminated());
            if (backend.sourceWriterWorker() != this || systemWriterBackends.size() >= budget.maximumClients()
                    || systemWriterBackends.putIfAbsent(identity, backend) != null) throw new IllegalStateException("Source writer generation is already owned or full");
            host = publicationHost;
        }
        if (host != null) host.registerPublicationBackend(this, backend.ownedBackendGroup());
    }
    public void registerPublicationResources(String identity,PublicationResources owner,
            TargetDestinationSdkBudget budget,LongSupplier clock) {
        final var actual = Objects.requireNonNull(owner, "publication owner");
        final TargetWorkerHostRuntime host;
        synchronized (this) {
            requirePublishRecoveryOwner(clock);
            budget.requireWorker(workClasses,resources);
            publicationResources.entrySet().removeIf(entry->entry.getValue().closed());
            if(publicationResources.size()>=budget.maximumClients()||publicationResources.putIfAbsent(identity,actual)!=null)
                throw new IllegalStateException("publication resource generation is already owned or its window is exhausted");
            host = publicationHost;
        }
        // Do not acquire the Host monitor while holding a Worker/native-read monitor.
        final var backend = actual.ownedBackendGroup();
        if (host != null && backend != null) { host.registerPublicationBackend(this, backend); }
    }

    synchronized java.util.List<TargetOwnedBackendGroup> publicationBackendGroups() {
        final var groups = new java.util.ArrayList<TargetOwnedBackendGroup>();
        if (recoveryBackend != null) groups.add(recoveryBackend);
        for (var owner : publicationResources.values()) {
            final var group = owner.ownedBackendGroup();
            if (group != null && groups.stream().noneMatch(existing -> existing == group)) { groups.add(group); }
        }
        for (var backend : systemWriterBackends.values()) {
            final var group = backend.ownedBackendGroup();
            if (groups.stream().noneMatch(existing -> existing == group)) groups.add(group);
        }
        return java.util.List.copyOf(groups);
    }
    @Override public synchronized Optional<WorkClassTask> settlePendingPublicationResourcesForDrain(){
        for(var entry:publicationResources.entrySet()){
            if(!entry.getValue().settleClose())return Optional.of(new WorkClassTask(com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                    "target-publication-resources/"+entry.getKey(),1));
        }
        return Optional.empty();
    }

    /** Actual native Store/Source/Oxia assignment-session guard for external publication resources; never SEND permission. */
    public synchronized com.nereusstream.delay.protocol.OwnerIdentity requirePublicationOwner(
            com.nereusstream.delay.protocol.OwnerIdentity expected, LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.requirePublicationOwner(expected, clock);
    }

    /** Only the actual owned Source can attach a parsed deployment receipt to this accepted Worker graph. */
    public synchronized SourceTransportCredentialUse bindSourceCredential(
            LocalBrokerMtlsResolver.SourceReceipt receipt, SourceRecordConsumer consumer, SourceAssignment assignment,
            com.nereusstream.delay.protocol.OwnerIdentity owner, Object nativeGeneration,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time, LongSupplier clock) {
        if (consumer != actualSourceConsumer || sourceBackendGroup == null
                || !sourceBackendGroup.holdsSourceCredential(receipt)) {
            throw new IllegalStateException("Source credential binding lacks this actual owned consumer");
        }
        target.requireAcceptedAssignment(assignment);
        requirePublicationOwner(owner, clock);
        receipt.authorization().requireAssignment(assignment);
        receipt.requireActive(time.get());
        return new SourceTransportCredentialUse(receipt, target, assignment, owner, nativeGeneration, time, clock);
    }
    public synchronized SourceTransportCredentialUse bindSystemWriterCredential(LocalBrokerMtlsResolver.SourceReceipt receipt,
            SourceAssignment assignment, com.nereusstream.delay.protocol.OwnerIdentity owner, Object nativeGeneration,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time, LongSupplier clock) {
        target.requireAcceptedAssignment(assignment); requirePublicationOwner(owner, clock);
        receipt.authorization().requireAssignment(assignment);
        receipt.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        return new SourceTransportCredentialUse(receipt, target, assignment, owner, nativeGeneration, time, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption readExternalEvidenceAdoption(
            BoundedReadBudget reads, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping, LongSupplier clock) {
        requireNewTurnsAdmitted(); resources.requireRuntimeBusinessAdmission();
        return target.readExternalEvidenceAdoption(reads, mapping, clock);
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
            synchronized (TargetWorkerShardRuntime.this) { requireNewTurnsAdmitted(); }
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
        return claim(budget, selected, owner, nowEpochMs, deadlineEpochMs, executionBytes, operationDigest,
                quota, physicalWrites, ownerClock, null);
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
                ownerClock, policyHead);
    }

    /** Revokes an exact unadmitted Claim while this Worker still admits business turns. */
    synchronized boolean claimStillCurrent(final BoundedReadBudget budget, final TargetClaimRecord claim,
            final TargetQuotaDelta.LocalClaimAuthority quota, final LongSupplier clock) {
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
            final BoundedReadBudget budget, final TargetClaimRecord claim,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability, final byte[] artifacts,
            final long retryUntilEpochMs, final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            final TargetPublishAdmissionMutationFactory signer, final LongSupplier clock) {
        return preparePublishAdmission(budget, claim, channel, capability, artifacts, retryUntilEpochMs,
                commitment, allocated, time, policies, null, signer, clock);
    }

    public synchronized SystemMutation preparePublishAdmission(
            final BoundedReadBudget budget, final TargetClaimRecord claim,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability, final byte[] artifacts,
            final long retryUntilEpochMs, final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            final com.nereusstream.delay.protocol.TargetKafkaReceiptBinding kafkaReceipt,
            final TargetPublishAdmissionMutationFactory signer, final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return Objects.requireNonNull(signer, "admissionSigner").sign(target.preparePublishAdmission(
                budget, claim, channel, capability, artifacts, retryUntilEpochMs, commitment, allocated,
                time, policies, kafkaReceipt, clock));
    }

    public synchronized void requireNativeOwnership(final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Applied applied,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time, final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireNativeOwnership(budget, applied, time, clock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Applied readAppliedAdmission(
            final BoundedReadBudget budget, final com.nereusstream.delay.protocol.SystemMutation image,
            final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readAppliedAdmission(budget, image, ownerClock);
    }

    /** Binds one lifecycle submission slot to this exact Worker and caller-owned registry/client protection. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishOutcomeStore.NegativeOutcome readNegativeOutcome(
            final BoundedReadBudget budget, final byte[] mappingId, final SystemMutation admission,
            final SystemMutation outcome, final com.nereusstream.delay.protocol.PulsarRecordTemplate template,
            final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.readNegativeOutcome(budget, mappingId, admission, outcome, template, clock);
    }

    public synchronized TargetChannelControlWorkClassExecutor configureChannelControlHandoff(
            final ShardLogMutationAppender appender, final ControlTargetRegistrationAuthority registrations,
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
            final ShardLogMutationAppender appender, final OxiaSyncOwnerLeaseBackend.ClientHandle handle,
            final String registryPrefix, final Runnable protection) {
        return configureChannelControlHandoff(appender, new OxiaControlTargetRegistrationAuthority(
                new OxiaSyncControlTargetRegistrationBackend(handle, registryPrefix)), protection);
    }

    public TargetChannelControlWorkClassExecutor.Submission submitChannelLifecycle(
            final TargetChannelControlMutationFactory.Prepared prepared, final LongSupplier clock) {
        final var handoff = Objects.requireNonNull(channelControlHandoff, "configured channel lifecycle handoff");
        return handoff.submit(prepared, clock);
    }

    /** Retained Admission proof for conservative recovery only; current Owner/read guards still apply. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishAdmissionStore.Recovery readRecoveryAdmission(
            final BoundedReadBudget budget, final com.nereusstream.delay.protocol.SystemMutation image,
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

    /** Enumerates retained Admission references under this active Worker, with opaque same-view continuation. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Page discoverPublishRecovery(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor continuation,
            final int maximumRows, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverPublishRecovery(budget, continuation, maximumRows, ownerClock);
    }

    public synchronized com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Page discoverKafkaUnknown(
            BoundedReadBudget reads, com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor continuation,
            int maximumRows, LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.discoverKafkaUnknown(reads, continuation, maximumRows, clock);
    }

    synchronized boolean publishRecoveryStillPending(BoundedReadBudget reads,
            com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Reference reference, LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null && publishRuntime.ownsAttempt(reference.attempt().publishAttemptId())) {
            return false;
        }
        return target.publishRecoveryStillPending(reads, reference, clock);
    }

    /** Enumerates all retained Admission inputs for checkpoint/history protection under this active Worker. */
    public synchronized com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Page discoverCheckpointInputs(
            final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor continuation,
            final int maximumRows, final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        return target.discoverCheckpointInputs(budget, continuation, maximumRows, ownerClock);
    }

    /** Revalidates the retained-input cut under the current Worker and Owner guards. */
    synchronized void requireCheckpointInputsCurrent(final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor cursor,
            final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        target.requireCheckpointInputsCurrent(budget, cursor, ownerClock);
    }

    synchronized void requireCheckpointAdmissionInputsSameCut(BoundedReadBudget budget,
            com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Cursor cursor,
            TargetCheckpointControlInputs.Cut expected, LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        target.requireCheckpointInputsCurrent(budget, cursor, ownerClock);
        target.requireCheckpointControlInputsCurrent(budget, expected, ownerClock);
        cursor.requireSameRetainedCut(expected.view);
    }

    synchronized TargetCheckpointControlInputs.Cut captureCheckpointControlInputs(final BoundedReadBudget budget,
            final com.nereusstream.delay.store.TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        return target.captureCheckpointControlInputs(budget, ledgerLimits, ownerClock);
    }

    synchronized void requireCheckpointControlInputsCurrent(final BoundedReadBudget budget,
            final TargetCheckpointControlInputs.Cut cut, final LongSupplier ownerClock) {
        requireCheckpointHistoryOwner(ownerClock);
        resources.requireRuntimeBusinessAdmission();
        target.requireCheckpointControlInputsCurrent(budget, cut, ownerClock);
    }

    /** Install before Host ticks begin. History must return promptly and perform its bounded I/O asynchronously. */
    public synchronized TargetPulsarPublishRuntime configurePulsarPublishing(
            final ShardLogMutationAppender appender, final TargetPublishAdmissionMutationFactory signer,
            final TargetPublishOutcomeMutationFactory outcomeSigner,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            final com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            final TargetPulsarPublishRuntime.Preparation preparation, final TargetPulsarPublishRuntime.Payloads payloads,
            final Supplier<BoundedReadBudget> reads,
            final Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            final LongSupplier clock, final java.util.concurrent.Executor io, final java.util.concurrent.Executor completion,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight ordinaryGate,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight nativeGate,
            final TargetPulsarPublishExecutor.ResultAuthority results) {
        return configurePulsarPublishing(appender, signer, outcomeSigner, journal, adapter, artifacts, preparation,
                payloads, reads, time, clock, io, completion, ordinaryGate, nativeGate, results, null);
    }

    public synchronized TargetPulsarPublishRuntime configurePulsarPublishing(
            final ShardLogMutationAppender appender, final TargetPublishAdmissionMutationFactory signer,
            final TargetPublishOutcomeMutationFactory outcomeSigner,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            final com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            final TargetPulsarPublishRuntime.Preparation preparation, final TargetPulsarPublishRuntime.Payloads payloads,
            final Supplier<BoundedReadBudget> reads,
            final Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            final LongSupplier clock, final java.util.concurrent.Executor io, final java.util.concurrent.Executor completion,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight ordinaryGate,
            final com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight nativeGate,
            final TargetPulsarPublishExecutor.ResultAuthority results,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal.NegativeSettlementAuthority negativeAuthority) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null) { throw new IllegalStateException("Target publication runtime is already configured"); }
        if (journalSettlement != null) { journalSettlement.requireJournal(journal); }
        publishRuntime = new TargetPulsarPublishRuntime(this, appender, signer, outcomeSigner, journal, adapter,
                artifacts, preparation, payloads, reads, time, clock, io, completion, ordinaryGate, nativeGate, results, negativeAuthority);
        return publishRuntime;
    }

    public synchronized TargetJournalNegativeSettlementMaintenance configureJournalNegativeSettlement(
            final com.nereusstream.delay.adapter.PulsarAttemptJournal journal,
            final com.nereusstream.delay.adapter.PulsarAttemptJournal.NegativeSettlementAuthority authority,
            final Runnable protection, final Supplier<BoundedReadBudget> reads,
            final TargetJournalNegativeSettlementMaintenance.History history,
            final java.util.concurrent.Executor io, final LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (journalSettlement != null) { throw new IllegalStateException("Journal settlement maintenance is already configured"); }
        if (publishRuntime != null) { publishRuntime.requireJournal(journal); }
        journalSettlement = new TargetJournalNegativeSettlementMaintenance(this, journal, authority, protection,
                reads, history, io, clock);
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

    synchronized long publicationSourceSequence() { return store.shardMutationSequence(); }

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
            ShardLogMutationAppender appender, TargetPublishAdmissionMutationFactory signer,
            TargetPublishOutcomeMutationFactory outcomeSigner,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            TargetPulsarPublishRuntime.Preparation preparation, TargetPulsarPublishRuntime.Payloads payloads,
            Supplier<BoundedReadBudget> reads,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            LongSupplier clock, java.util.concurrent.Executor io, java.util.concurrent.Executor completion,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null) { throw new IllegalStateException("Target publication driver is already configured"); }
        publishRuntime = new TargetPulsarPublishRuntime(this, appender, signer, outcomeSigner, adapter,
                artifacts, preparation, payloads, reads, time, clock, io, completion, gate, results);
        return publishRuntime;
    }

    /** Adds a baseline endpoint to the already configured Source driver; it uses that driver's original inputs. */
    public synchronized void configureBaselineEndpoint(
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            TargetPublishOutcomeMutationFactory outcomeSigner,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results,
            java.util.concurrent.Executor completion, LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        final var driver = Objects.requireNonNull(publishRuntime, "configured Source publication driver");
        driver.requireArtifacts(artifacts);
        driver.configureBaseline(adapter, outcomeSigner, gate, results, completion);
    }

    /** K2-only Source driver, with an actual guarded transaction endpoint and independently authenticated anchor. */
    public synchronized TargetPulsarPublishRuntime configureKafkaReceiptPublishing(
            ShardLogMutationAppender appender, TargetPublishAdmissionMutationFactory signer,
            TargetPublishOutcomeMutationFactory outcomeSigner,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter adapter,
            com.nereusstream.delay.protocol.ArtifactGenerationSet artifacts,
            TargetPulsarPublishRuntime.Preparation preparation, TargetPulsarPublishRuntime.Payloads payloads,
            Supplier<BoundedReadBudget> reads,
            Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            LongSupplier clock, java.util.concurrent.Executor io, java.util.concurrent.Executor completion,
            com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate,
            TargetOrdinaryPublishExecutor.ResultAuthority results) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null) { throw new IllegalStateException("Target publication driver is already configured"); }
        publishRuntime = new TargetPulsarPublishRuntime(this, appender, signer, outcomeSigner, adapter,
                artifacts, preparation, payloads, reads, time, clock, io, completion, gate, results,
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
            java.util.concurrent.Executor completion, LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        final var driver = Objects.requireNonNull(publishRuntime, "configured Source publication driver");
        driver.requireArtifacts(artifacts);
        driver.configureKafkaReceipt(adapter, outcomeSigner, gate, results, completion);
    }

    void requirePublishingStartupClosure() {
        final var runtime = Objects.requireNonNull(publishRuntime, "configured Target publisher");
        runtime.requireConfiguredEndpoints();
        if(runtime.hasKafkaReceiptEndpoint() && (publicationResources.values().stream().noneMatch(owner->!owner.closed()&&owner.readyForPublishing())
                ||publicationResources.values().stream().anyMatch(owner->!owner.closed()&&!owner.readyForPublishing()))) {
            throw new IllegalStateException("K2 Claim startup requires actual SDK resource ownership");
        }
        if (runtime.requiresJournal()) {
            Objects.requireNonNull(journalSettlement, "configured retained Source/Journal cleanup");
        }
    }

    /** Complete local dependencies for configured service activation; native/Broker authority stays in each provider. */
    synchronized void requireConfiguredServiceReady(TargetWorkerHostRuntime expected, LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (publicationHost != expected || sourceBackendGroup == null || !sourceBackendGroup.workerReady(this)
                || !sourceBackendGroup.sourceCredentialsReady(this)) {
            throw new IllegalStateException("configured service requires its actual owned Source and Host");
        }
        requirePublishingStartupClosure();
        if (!target.hasAuthorityPreparation()) {
            throw new IllegalStateException("configured service requires complete bounded Source content/history preparation");
        }
        Objects.requireNonNull(publishRecoveryMaintenance, "configured retained Admission/UNKNOWN recovery");
        if (publishRuntime.requiresJournal()) { requirePulsarStartupClosure(); }
        if (publicationResources.values().stream().noneMatch(owner -> !owner.closed() && owner.readyForPublishing())
                || publicationResources.values().stream().anyMatch(owner -> !owner.closed() && !owner.readyForPublishing())) {
            throw new IllegalStateException("configured service has absent or unfinished actual publication resources");
        }
        if (publicationResources.values().stream().anyMatch(owner -> !owner.closed() && owner.credentialReceipt() == null)) {
            throw new IllegalStateException("new Target service requires actual resolved mTLS publication material");
        }
        if (!publishRuntime.available()) {
            throw new IllegalStateException("configured service activation has an existing publication flight");
        }
        if (systemWriterBackends.values().stream().noneMatch(writer -> writer.readyForAppending()
                && publishRuntime.usesSourceAppender(writer))) {
            throw new IllegalStateException("configured service requires its actual owned Source system appender");
        }
    }

    boolean publisherSupportsNative() { return publishRuntime != null && publishRuntime.supportsNative(); }

    void requirePulsarStartupClosure() {
        Objects.requireNonNull(publishRuntime, "configured Target publisher").requireNegativeSettlementConfigured();
        Objects.requireNonNull(journalSettlement, "configured retained Source/Journal cleanup");
    }

    boolean publisherAvailable() { return publishRuntime != null && publishRuntime.available(); }

    void acceptPublicationClaim(final TargetClaimRecord claim) {
        Objects.requireNonNull(publishRuntime, "configured Target publisher").accept(claim);
    }

    @Override
    public Optional<TargetPulsarPublishRuntime.Turn> runPublishMaintenanceTurn() {
        final var runtime = publishRuntime;
        return runtime == null ? Optional.empty() : Optional.of(runtime.runTurn());
    }

    public synchronized TargetPublishRecoveryMaintenance configurePublishRecoveryMaintenance(
            final TargetPublishOutcomeMutationFactory outcomes, final ShardLogMutationAppender appender,
            final Supplier<BoundedReadBudget> reads, final TargetPublishRecoveryMaintenance.History history,
            final LongSupplier recoveryClock) {
        requirePublishRecoveryOwner(recoveryClock);
        if (publishRecoveryMaintenance != null) {
            throw new IllegalStateException("Target publish recovery maintenance is already configured");
        }
        final var handoff = new TargetOutcomeWorkClassExecutor(this, appender);
        final var executor = new TargetPublishRecoveryExecutor(this, handoff, outcomes);
        publishRecoveryMaintenance = new TargetPublishRecoveryMaintenance(
                this, executor, reads, history, recoveryClock);
        return publishRecoveryMaintenance;
    }

    synchronized void requirePublishRecoveryOwner(final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requirePublishRecoveryOwner(clock);
    }

    synchronized void requireCheckpointHistoryOwner(LongSupplier clock) {
        if (pendingCheckpoint == null) {
            requireNewTurnsAdmitted();
        } else {
            pendingCheckpoint.requirePreparationCurrent();
        }
        resources.requireRuntimeBusinessAdmission();
        target.requirePublishRecoveryOwner(clock);
    }

    synchronized boolean publishRecoveryStillAdmitted(final BoundedReadBudget budget,
            final com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery.Reference reference,
            final LongSupplier clock) {
        requirePublishRecoveryOwner(clock);
        if (publishRuntime != null && publishRuntime.ownsAttempt(reference.attempt().publishAttemptId())) {
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

    synchronized void submitOutcomeAction(
            final WorkClassTask task, final com.nereusstream.delay.protocol.SystemMutation mutation,
            final LongSupplier ownerClock, final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireOutcomeWriter(mutation, ownerClock);
        workClasses.submit(task, action);
    }

    synchronized void submitAdmissionAction(
            final WorkClassTask task, final SystemMutation mutation,
            final LongSupplier ownerClock, final Runnable action) {
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
                throw new IllegalArgumentException("Source mutation turn must observe a first result or one append outcome");
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
                target.requireCurrentExpiryLogPosition(appended.sourcePosition(), appended.sourceConnectionGeneration(),
                        appended.guardAttestationDigest(), ownerClock);
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

    synchronized void submitChannelControlAction(final WorkClassTask task, final SystemMutation mutation,
            final LongSupplier clock, final Runnable action) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireChannelControlWriter(mutation, clock);
        workClasses.submit(task, action);
    }

    synchronized SourceMutationAppend appendChannelControl(final SystemMutation mutation,
            final ShardLogMutationAppender appender, final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        final var first = target.channelControlMutationResult(mutation, clock).orElse(null);
        if (first != null) { return new SourceMutationAppend(first, null); }
        target.requireChannelControlWriter(mutation, clock);
        try {
            final var appended = Objects.requireNonNull(appender.append(mutation), "channel lifecycle append");
            if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
                target.requireCurrentExpiryLogPosition(appended.sourcePosition(), appended.sourceConnectionGeneration(),
                        appended.guardAttestationDigest(), clock);
            }
            return new SourceMutationAppend(null, appended);
        } catch (RuntimeException | Error uncertainAppend) {
            target.fence();
            throw uncertainAppend;
        }
    }

    synchronized Optional<SystemMutationResult> channelControlMutationResult(
            final SystemMutation mutation, final LongSupplier clock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        return target.channelControlMutationResult(mutation, clock);
    }

    synchronized ShardLogMutationAppender.AppendOutcome appendOutcome(
            final com.nereusstream.delay.protocol.SystemMutation mutation,
            final ShardLogMutationAppender appender, final LongSupplier ownerClock) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        target.requireOutcomeWriter(mutation, ownerClock);
        final var appended = Objects.requireNonNull(appender.append(mutation), "Target Outcome append");
        if (appended.disposition() == ShardLogMutationAppender.AppendDisposition.PERSISTED) {
            target.requireCurrentExpiryLogPosition(appended.sourcePosition(), appended.sourceConnectionGeneration(),
                    appended.guardAttestationDigest(), ownerClock);
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
            final ShardLogMutationAppender appender,
            final Supplier<MessageExpiryRequest> requestProvider) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (messageExpiryHandoff != null || messageExpiryMaintenance != null) {
            throw new IllegalStateException("Target Worker message expiry maintenance is already configured");
        }
        final var exactAppender = Objects.requireNonNull(appender, "appender");
        final var exactRequestProvider = Objects.requireNonNull(requestProvider, "requestProvider");
        final var handoff = new TargetMessageExpiryWorkClassExecutor(
                this, exactAppender);
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
            final CheckpointUploadIntentAuthority intents, final LongSupplier ownerClock,
            final Path checkpointPath, final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final Runnable protectionGuard) {
        return submitProtectedSemanticCheckpointCandidate(intents, ownerClock, checkpointPath, pending,
                physicalLimits, quotaLimits, ledgerLimits, snapshot, semanticLimits, protectionGuard,
                protectCheckpointCut());
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedSemanticCheckpointCandidate(
            CheckpointUploadIntentAuthority intents, LongSupplier ownerClock, Path checkpointPath,
            CheckpointUploadIntent pending, CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protectionGuard, SourceRecordConsumer.CheckpointCut cut) {
        return submitProtectedSemanticCheckpointCandidate(intents, ownerClock, checkpointPath, pending,
                physicalLimits, quotaLimits, ledgerLimits, snapshot, semanticLimits, protectionGuard, cut, null);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedSemanticCheckpointCandidate(
            CheckpointUploadIntentAuthority intents, LongSupplier ownerClock, Path checkpointPath,
            CheckpointUploadIntent pending, CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protectionGuard, SourceRecordConsumer.CheckpointCut cut,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication) {
        return submitProtectedSemanticCheckpointCandidate(intents, ownerClock, checkpointPath, pending,
                physicalLimits, quotaLimits, ledgerLimits, snapshot, semanticLimits, protectionGuard, cut,
                publication, null);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitProtectedSemanticCheckpointCandidate(
            CheckpointUploadIntentAuthority intents, LongSupplier ownerClock, Path checkpointPath,
            CheckpointUploadIntent pending, CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protectionGuard, SourceRecordConsumer.CheckpointCut cut,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication, SchedulerBudget settlementBudget) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut") || maintenance.hasPendingTurn()
                || hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target semantic checkpoint cut is no longer prepared for this Shard");
        }
        cut.requireCurrent();
        final var submitted = target.submitSemanticCheckpointCandidate(workClasses, intents, ownerClock,
                checkpointPath, pending, physicalLimits, quotaLimits, ledgerLimits, snapshot, semanticLimits,
                cut::requireCurrent, protectionGuard, publication, settlementBudget);
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission
            submitCollectedSemanticCheckpointCandidate(
            TargetCheckpointCandidateSchedule.CollectedSemanticRequest request) {
        return submitCollectedSemanticCheckpointCandidate(Objects.requireNonNull(request, "request"),
                protectCheckpointCut());
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitCollectedSemanticCheckpointCandidate(
            TargetCheckpointCandidateSchedule.CollectedSemanticRequest request,
            SourceRecordConsumer.CheckpointCut cut) {
        return submitCollectedSemanticCheckpointCandidate(request, cut, null);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitCollectedSemanticCheckpointCandidate(
            TargetCheckpointCandidateSchedule.CollectedSemanticRequest request,
            SourceRecordConsumer.CheckpointCut cut, SchedulerBudget settlementBudget) {
        requireNewTurnsAdmitted();
        resources.requireRuntimeBusinessAdmission();
        if (preparedCheckpointCut != Objects.requireNonNull(cut, "cut") || maintenance.hasPendingTurn()
                || hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target semantic collection cut is no longer prepared for this Shard");
        }
        cut.requireCurrent();
        final var candidate = request.candidate();
        final var submitted = target.submitCollectedSemanticCheckpointCandidate(workClasses, candidate.intents(),
                candidate.ownerClock(), candidate.checkpointPath(), candidate.pending(), candidate.physicalLimits(),
                candidate.quotaLimits(), candidate.ledgerLimits(), request.inputs(), request.semanticLimits(),
                cut::requireCurrent, request.protection(), request.publication(), settlementBudget,
                request.registration());
        pendingCheckpoint = submitted;
        preparedCheckpointCut = null;
        return submitted;
    }

    /** Requests local registration withdrawal before Host drain; an active RPC remains owned. */
    @Override
    public synchronized void requestCheckpointRegistrationStop() {
        if (pendingCheckpoint != null) { pendingCheckpoint.requestRegistrationStop(); }
    }

    @Override
    public synchronized void requestCheckpointPreparationStop() {
        if (pendingCheckpoint != null) { pendingCheckpoint.requestPreparationStop(); }
    }

    /** Runs one shared bounded turn and releases the local source/maintenance cut only after settlement. */
    public synchronized Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> runCheckpointTurn(
            final SchedulerBudget budget) {
        if (pendingCheckpoint == null) {
            return Optional.empty();
        }
        if (pendingCheckpoint.settleFailedRegistrationIo()) {
            final var failed = pendingCheckpoint.outcome();
            if (failed.isPresent()) { pendingCheckpoint = null; }
            return failed;
        }
        resources.requireRuntimeBusinessAdmission();
        if (pendingCheckpoint.outcome().isEmpty()) {
            pendingCheckpoint.enqueueNextPreparation();
            workClasses.runTurn(Objects.requireNonNull(budget, "budget"));
        }
        final var outcome = pendingCheckpoint.outcome();
        if (outcome.isPresent()) {
            pendingCheckpoint = null;
        }
        return outcome;
    }

    /** Exact queued checkpoint task, if it has not yet reached a terminal outcome. */
    public synchronized Optional<WorkClassTask> pendingCheckpointTask() {
        return pendingCheckpoint == null || pendingCheckpoint.outcome().isPresent()
                ? Optional.empty()
                : Optional.of(pendingCheckpoint.task());
    }

    /** Exact nonterminal provider failure, including its original native cursor cleanup handle. */
    public synchronized Optional<Throwable> pendingCheckpointIoFailure() {
        return pendingCheckpoint == null ? Optional.empty() : pendingCheckpoint.pendingIoFailure();
    }

    /** Enqueues one original-provider cleanup step; failure remains nonterminal until actual cleanup/step exit. */
    public synchronized void retryPendingCheckpointIoCleanup() {
        if (pendingCheckpoint != null) { pendingCheckpoint.retryPendingIoCleanup(); }
    }

    public Throwable pendingSourceAuthorityFailure() { return target.authorityIoFailure(); }
    public void retryPendingSourceAuthorityCleanup() { target.retryAuthorityCleanup(); }

    /** Stops new source polls and maintenance submissions after retained maintenance work settles. */
    public synchronized void pauseNewTurns() {
        requireCheckpointSettled();
        if (sourceLoop.pendingEntry().isPresent()) {
            throw new IllegalStateException("Target Worker cannot pause a pending source acknowledgement");
        }
        if (hasUnsettledMessageExpiry()) {
            throw new IllegalStateException("Target Worker cannot pause a pending message expiry handoff");
        }
        if (channelControlHandoff != null && channelControlHandoff.pending().isPresent()) {
            throw new IllegalStateException("Target Worker cannot pause a pending channel lifecycle submission");
        }
        if (publishRuntime != null && !publishRuntime.available()) {
            throw new IllegalStateException("Target Worker cannot pause a retained publication flight");
        }
        if (journalSettlement != null && !journalSettlement.completedForSource(store.shardMutationSequence())) {
            throw new IllegalStateException("Target Worker cannot pause an unfinished Journal settlement sweep");
        }
        if(publicationResources.values().stream().anyMatch(owner->!owner.closed()))
            throw new IllegalStateException("Target Worker cannot pause live publication resources");
        sourceAndMaintenancePaused = true;
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
        return settlePendingMaintenance(budget).filter(TargetReservationGcRuntime.Turn::pending)
                .map(TargetReservationGcRuntime.Turn::task);
    }

    @Override
    public synchronized boolean canPublishFinalCheckpoint() {
        return !target.fenced() && !store.isCloseStarted()
                && !store.isWriteOutcomeUncertain();
    }

    @Override
    public synchronized long checkpointStoreSequence() { return store.latestSequenceNumber(); }

    @Override
    public Optional<WorkClassTask> settlePendingPublicationForDrain(final SchedulerBudget workBudget,
            final SchedulerBudget sourceBudget, final LongSupplier clock) {
        final var publisher = publishRuntime;
        if (publisher == null || publisher.available()) {
            final var recovery=publishRecoveryMaintenance;
            if(recovery!=null&&recovery.pendingForDrain()){
                recovery.retryLoadingCleanup();final var loaded=recovery.runTurn();
                if(loaded.failure()!=null)throw new IllegalStateException("retained recovery could not settle",loaded.failure());
                workClasses.runTurn(workBudget);
                synchronized(this){requireNewTurnsAdmitted();if(sourceLoop.pendingEntry().isPresent())sourceLoop.settlePendingEntry(sourceBudget,clock);
                    else sourceLoop.runTurn(sourceBudget,clock);}
                if(recovery.pendingForDrain())return Optional.of(new WorkClassTask(com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                        "target-recovery-drain/"+shardId,1));
            }
            return settleJournalForDrain(clock);
        }
        final var turn = publisher.runTurn();
        if (turn.failure() != null) { throw new IllegalStateException("retained publication could not settle", turn.failure()); }
        if (publisher.available()) { return settleJournalForDrain(clock); }
        workClasses.runTurn(workBudget);
        synchronized (this) {
            requireNewTurnsAdmitted();
            if (sourceLoop.pendingEntry().isPresent()) { sourceLoop.settlePendingEntry(sourceBudget, clock); }
            else { sourceLoop.runTurn(sourceBudget, clock); }
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
        if (configured == null) { return Optional.empty(); }
        final var turn = configured.runTurn();
        if (turn.failure() != null) { throw new IllegalStateException("Source-decided Journal cleanup is unsettled", turn.failure()); }
        return configured.pending() ? Optional.of(new WorkClassTask(
                com.nereusstream.delay.scheduler.WorkClass.OUTCOME_AND_CONTROL,
                "target-journal-settlement/" + shardId, 1)) : Optional.empty();
    }

    @Override
    public Optional<WorkClassTask> settlePendingChannelControlForDrain(final SchedulerBudget workBudget,
            final SchedulerBudget sourceBudget, final LongSupplier clock) {
        final var handoff = channelControlHandoff;
        if (handoff == null || handoff.pending().isEmpty()) { return Optional.empty(); }
        if (handoff.settle(clock).isPresent()) { return Optional.empty(); }
        handoff.retryPending(clock);
        workClasses.runTurn(Objects.requireNonNull(workBudget, "workBudget"));
        if (handoff.settle(clock).isPresent()) { return Optional.empty(); }
        synchronized (this) {
            requireNewTurnsAdmitted();
            if (sourceLoop.pendingEntry().isPresent()) {
                sourceLoop.settlePendingEntry(sourceBudget, clock);
            } else {
                sourceLoop.runTurn(sourceBudget, clock);
            }
        }
        if (handoff.settle(clock).isPresent()) { return Optional.empty(); }
        return handoff.pending().map(TargetChannelControlWorkClassExecutor.Submission::task);
    }

    /** Advances only the retained expiry append and its source result by one bounded drain step. */
    @Override
    public Optional<WorkClassTask> settlePendingMessageExpiryForDrain(
            final SchedulerBudget workBudget,
            final SchedulerBudget sourceBudget,
            final LongSupplier ownerClock) {
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

    private void requireNewTurnsAdmitted() {
        if (backendQuarantined || sourceBackendGroup != null && !sourceBackendGroup.workerReady(this)) {
            throw new IllegalStateException("Target Worker backend is unbound or quarantined");
        }
        if (sourceAndMaintenancePaused) {
            throw new IllegalStateException("Target Worker source and maintenance admission is paused");
        }
        requireCheckpointSettled();
    }

    void requireOxiaAuthority(OxiaOwnerLeaseStore expected,
            WorkClassExecutionRegistry classes, SharedRocksDbResources resources) {
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
        if (actual == null) return;
        actual.requireWorkerGraph(workClasses, resources);
        if (publicationHost != null || recoveryBackend != null && recoveryBackend != actual) {
            throw new IllegalStateException("recovery SDK owner cannot transfer to another Worker/Host");
        }
        recoveryBackend = actual;
    }

    public TargetOwnedBackendGroup sourceBackendGroup() { return sourceBackendGroup; }
    public boolean ownsSourceConsumer(SourceRecordConsumer expected) { return actualSourceConsumer == expected; }

    /** Does not wait for Source ACK or SDK reads holding the Worker monitor; it fences their later Store commits. */
    void quarantineBackendWrites() {
        backendQuarantined = true;
        sourceAndMaintenancePaused = true;
        drainCoordinator.fenceNormalDrain();
        target.fence();
        store.fenceForUncleanClose();
        final var checkpoint = pendingCheckpoint;
        if (checkpoint != null) { checkpoint.requestPreparationStop(); }
    }

    synchronized Optional<TargetOwnerDrainCoordinator.ReplayExit> exitQuarantinedForReplay(
            TargetOwnerDrainCoordinator.Request request, LongSupplier clock) {
        if (!backendQuarantined) { throw new IllegalStateException("Worker is not quarantined"); }
        if (sourceBackendGroup == null || !sourceBackendGroup.resourcesTerminated()) { return Optional.empty(); }
        target.stopAuthorityPreparation(); target.retryAuthorityCleanup();
        if (!target.authorityIoExited()) return Optional.empty();
        if (pendingCheckpoint != null) {
            pendingCheckpoint.retryPendingIoCleanup();
            if (!pendingCheckpoint.ownedIoExited()) { return Optional.empty(); }
        }
        for (var owner : publicationResources.values()) {
            final var backend = owner.ownedBackendGroup();
            if (backend != null && backend.resourcesTerminated() && owner.channelIdentity() != null
                    && publishRuntime != null && !publishRuntime.releasePhysicalAfterBackendTermination(owner.channelIdentity())) {
                return Optional.empty();
            }
            if (!owner.settleClose()) { return Optional.empty(); }
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
