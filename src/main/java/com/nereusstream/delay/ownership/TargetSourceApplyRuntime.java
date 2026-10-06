package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetCloseBody;
import com.nereusstream.delay.protocol.TargetCloseRequest;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetMembershipControlBody;
import com.nereusstream.delay.protocol.TargetNativePolicyControlBody;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetClaimStore;
import com.nereusstream.delay.runtime.TargetCloseStore;
import com.nereusstream.delay.runtime.TargetCloseVerifier;
import com.nereusstream.delay.runtime.TargetCommandReplayStore;
import com.nereusstream.delay.runtime.TargetCommandStore;
import com.nereusstream.delay.runtime.TargetExpireGenerationStore;
import com.nereusstream.delay.runtime.TargetExpireGenerationVerifier;
import com.nereusstream.delay.runtime.TargetExpiryDiscoveryStore;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetMembershipControlStore;
import com.nereusstream.delay.runtime.TargetMembershipControlVerifier;
import com.nereusstream.delay.runtime.TargetNativePolicyControlStore;
import com.nereusstream.delay.runtime.TargetNativePolicyControlVerifier;
import com.nereusstream.delay.runtime.TargetNativePolicyTrustStore;
import com.nereusstream.delay.runtime.TargetQueueHeadCache;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetQuotaGrantStore;
import com.nereusstream.delay.runtime.TargetReservationControls;
import com.nereusstream.delay.runtime.TargetReservationExpiryStore;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.runtime.TargetSystemReplayStore;
import com.nereusstream.delay.runtime.TargetTimeFenceStore;
import com.nereusstream.delay.runtime.TargetTimeFenceVerifier;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentAuthority;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

/** Active Target source execution over the existing bounded poll/apply/ACK loop. */
public final class TargetSourceApplyRuntime extends SourceApplyTarget {
    public record Limits(int records, long bytes, long elapsedNanos, int counters, int domains) {
        public Limits {
            if (records <= 0
                    || bytes <= 0
                    || elapsedNanos <= 0
                    || counters < 2
                    || domains < 1
                    || domains > TargetQueueState.MAX_DOMAIN_SLOTS) {
                throw new IllegalArgumentException("Target source runtime requires finite positive limits");
            }
        }
    }

    /** First-application authority only. Immutable duplicates do not resolve current registrations or capacity. */
    public record GrantControl(
            PreparedControlOperation prepared,
            TargetQuotaGrantControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public GrantControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface GrantControls {
        GrantControl resolve(SourceReplayMutation entry);
    }

    public record FenceControl(TargetTimeFenceVerifier.Authority authority, TargetStoreBackend.CommitAuthority commit) {
        public FenceControl {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface Fences {
        FenceControl resolve(SourceReplayMutation entry);
    }

    public record ExpiryControl(
            TargetExpireGenerationVerifier.Authority authority, TargetStoreBackend.CommitAuthority commit) {
        public ExpiryControl {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface ExpiryControls {
        ExpiryControl resolve(SourceReplayMutation entry);
    }

    public record CloseControl(
            PreparedControlOperation prepared,
            TargetCloseVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public CloseControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface Closes {
        CloseControl resolve(SourceReplayMutation entry);
    }

    public record MembershipControl(
            PreparedControlOperation prepared,
            CanonicalTargetPartition physical,
            TargetMembershipControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public MembershipControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(physical, "physical");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface MembershipControls {
        MembershipControl resolve(SourceReplayMutation entry);
    }

    public record NativePolicyControl(
            PreparedControlOperation prepared,
            TargetNativePolicyControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public NativePolicyControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface NativePolicyControls {
        NativePolicyControl resolve(SourceReplayMutation entry);
    }

    public record CommandControl(
            TargetCommandStore.Policy policy,
            TargetCommandStore.CancellationControls cancellations,
            com.nereusstream.delay.runtime.TargetReservationControls.Authority reservationControls,
            TargetCommandStore.Schedules schedules,
            TargetCommandStore.PayloadProofControls payloadProofs,
            TargetStoreBackend.CommitAuthority commit) {
        public CommandControl {
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(cancellations, "cancellations");
            Objects.requireNonNull(reservationControls, "reservationControls");
            Objects.requireNonNull(schedules, "schedules");
            Objects.requireNonNull(payloadProofs, "payloadProofs");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface Commands {
        CommandControl resolve(SourceReplayRecord entry);
    }

    /** External resource/Control snapshots remain mandatory; local ownership checks cannot substitute for them. */
    public record Authorities(
            OxiaOwnerLeaseStore leases,
            SourceReplaySuccessor successor,
            GrantControls grants,
            Fences fences,
            ExpiryControls expiries,
            Closes closes,
            MembershipControls membershipControls,
            TargetStoreBackend.CommitAuthority duplicateWrites,
            TargetStoreBackend.ReadAuthority reads,
            Commands commands,
            NativePolicyControls nativePolicyControls) {
        public Authorities(
                final OxiaOwnerLeaseStore leases,
                final SourceReplaySuccessor successor,
                final GrantControls grants,
                final Fences fences,
                final ExpiryControls expiries,
                final Closes closes,
                final MembershipControls membershipControls,
                final TargetStoreBackend.CommitAuthority duplicateWrites,
                final TargetStoreBackend.ReadAuthority reads,
                final Commands commands) {
            this(
                    leases,
                    successor,
                    grants,
                    fences,
                    expiries,
                    closes,
                    membershipControls,
                    duplicateWrites,
                    reads,
                    commands,
                    entry -> {
                        throw new IllegalStateException("Target Native policy Control authorities are not configured");
                    });
        }

        public Authorities {
            Objects.requireNonNull(leases, "leases");
            Objects.requireNonNull(successor, "successor");
            Objects.requireNonNull(grants, "grants");
            Objects.requireNonNull(fences, "fences");
            Objects.requireNonNull(expiries, "expiries");
            Objects.requireNonNull(closes, "closes");
            Objects.requireNonNull(membershipControls, "membershipControls");
            Objects.requireNonNull(duplicateWrites, "duplicateWrites");
            Objects.requireNonNull(reads, "reads");
            Objects.requireNonNull(commands, "commands");
            Objects.requireNonNull(nativePolicyControls, "nativePolicyControls");
        }
    }

    private final ShardStore store;
    private final TargetStoreBackend backend;
    private final StoreMetadata metadata;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final TargetQuotaGrantStore grants;
    private final TargetTimeFenceStore fences;
    private final TargetExpiryDiscoveryStore expiryDiscovery;
    private final TargetExpireGenerationStore expiries;
    private final TargetCloseStore closes;
    private final TargetMembershipControlStore membershipControls;
    private final TargetNativePolicyControlStore nativePolicyControls;
    private final TargetSystemReplayStore replay;
    private final TargetCommandReplayStore commandReplay;
    private final TargetCommandStore commands;
    private final TargetQueueHeadCache targetQueueHeadCache = new TargetQueueHeadCache();
    private final SourceAssignment assignment;
    private final Authorities authorities;
    private final Limits limits;
    private final LongSupplier monotonicClock;
    // Store-read guards revalidate ownership while holding the Store monitor; keep this lock free of Store access.
    private final Object ownerStateLock = new Object();
    private WorkClassExecutionRegistry workClasses;
    private TargetReservationGcRuntime maintenanceRuntime;
    private volatile OwnerLease lease;
    private volatile boolean fenced;
    private long lastOwnerTime = -1;

    /** Requires prior explicit activation, including persisted Owner epoch and the reached source barrier. */
    public TargetSourceApplyRuntime(
            final TargetStoreBootstrap.Initialized initialized,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final Authorities authorities,
            final Limits limits,
            final LongSupplier monotonicClock) {
        this(
                Objects.requireNonNull(initialized, "initialized").backend(),
                initialized.root(),
                store,
                assignment,
                lease,
                authorities,
                limits,
                monotonicClock);
    }

    /** Recovery composition uses only the root reconstructed from the persisted fixed Shard anchor. */
    public TargetSourceApplyRuntime(
            final TargetStoreBootstrap.Reopened reopened,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final Authorities authorities,
            final Limits limits,
            final LongSupplier monotonicClock) {
        this(
                Objects.requireNonNull(reopened, "reopened").backend(),
                reopened.root(),
                store,
                assignment,
                lease,
                authorities,
                limits,
                monotonicClock);
    }

    private TargetSourceApplyRuntime(
            final TargetStoreBackend openedBackend,
            final TargetQuotaIncarnation root,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final Authorities authorities,
            final Limits limits,
            final LongSupplier monotonicClock) {
        this.store = Objects.requireNonNull(store, "store");
        backend = Objects.requireNonNull(openedBackend, "backend");
        backend.requireStore(store);
        metadata = store.metadata();
        final var exactRoot = Objects.requireNonNull(root, "root");
        scope = exactRoot.scope();
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.authorities = Objects.requireNonNull(authorities, "authorities");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        if (!scope.shard().equals(lease.shardId())
                || !scope.shard().equals(assignment.shardId())
                || lease.context() == null
                || !Arrays.equals(lease.sourceAssignmentId(), assignment.assignmentId())
                || lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()
                || lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()
                || !assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())) {
            throw new IllegalArgumentException("Target source runtime lacks exact active Owner/assignment/Store state");
        }
        lineage = exactRoot.recoveryLineage();
        grants = new TargetQuotaGrantStore(backend, scope, lineage, limits.counters(), limits.domains());
        fences = new TargetTimeFenceStore(backend, scope, lineage, limits.counters(), limits.domains());
        expiryDiscovery = new TargetExpiryDiscoveryStore(backend, scope);
        expiries = new TargetExpireGenerationStore(backend, scope, lineage, limits.counters(), limits.domains());
        closes = new TargetCloseStore(backend, scope, lineage, limits.counters(), limits.domains());
        membershipControls =
                new TargetMembershipControlStore(backend, scope, lineage, limits.counters(), limits.domains());
        nativePolicyControls =
                new TargetNativePolicyControlStore(backend, scope, lineage, limits.counters(), limits.domains());
        replay = new TargetSystemReplayStore(backend, scope, lineage, limits.counters(), limits.domains());
        commandReplay = new TargetCommandReplayStore(backend, scope, lineage, limits.counters(), limits.domains());
        commands = new TargetCommandStore(backend, scope, lineage, limits.counters(), limits.domains());
        backend.bindTargetQueueHeadCache(targetQueueHeadCache);
    }

    /** Requires a broker factory's accepted assignment to match this activated Target runtime exactly. */
    public void requireAcceptedAssignment(final SourceAssignment acceptedAssignment) {
        if (!assignment.sameIdentity(Objects.requireNonNull(acceptedAssignment, "acceptedAssignment"))) {
            throw new IllegalArgumentException("Target source runtime uses a different accepted assignment");
        }
    }

    /** Exact assignment whose activation barrier this runtime has validated against its Store frontier. */
    public SourceAssignment acceptedAssignment() {
        return assignment;
    }

    synchronized void bindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        backend.bindTargetQueueChangeSignal(signal);
    }

    synchronized void unbindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        backend.unbindTargetQueueChangeSignal(signal);
    }

    synchronized void configureTargetQueueHeadCache(final int maximumEntries) {
        targetQueueHeadCache.configure(maximumEntries);
    }

    @Override
    synchronized void bind(final WorkClassExecutionRegistry registry) {
        if (workClasses != null && workClasses != registry) {
            throw new IllegalStateException("Target source runtime is bound to another work-class registry");
        }
        store.sharedResources().bindWorkClassExecutionRegistry(registry);
        workClasses = registry;
    }

    /** Builds Close GC on this exact active Owner, Store and shared WorkClass graph. */
    public synchronized TargetReservationClosureWorkClassExecutor newCloseGcExecutor(
            final WorkClassExecutionRegistry registry,
            final TargetReservationControls.Authority controls,
            final TargetReservationClosureWorkClassExecutor.Limits gcLimits,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final TargetQuotaDelta.ReservationClosureAuthority closureQuota,
            final TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            final TargetQuotaDelta.CloseCursorAuthority cursorQuota,
            final LongSupplier ownerClock) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("Close GC requires the bound Target source WorkClass graph");
        }
        Objects.requireNonNull(physicalWrites, "physicalWrites");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetReservationClosureWorkClassExecutor(
                registry,
                backend,
                scope,
                lineage,
                limits.domains(),
                controls,
                gcLimits,
                () -> requireGcOwner(clock),
                (actual, actualScope) -> {
                    requireGcOwner(clock);
                    return gcGuard(authorities.reads().acquire(actual, actualScope), actual, actualScope, clock);
                },
                (actual, actualScope, mutation) -> {
                    final var stamp = mutation.quota().counters().mutation();
                    if (!stamp.reservationClosure() && !stamp.reservationExpiry() && !stamp.reservationCloseCursor()) {
                        throw new IllegalStateException("Close GC cannot commit another mutation kind");
                    }
                    requireGcOwner(clock);
                    return gcGuard(physicalWrites.acquire(actual, actualScope, mutation), actual, actualScope, clock);
                },
                closureQuota,
                expiryQuota,
                cursorQuota,
                monotonicClock);
    }

    /** Builds ordinary reservation expiry on the same exact Owner, Store and WorkClass graph. */
    public synchronized TargetReservationExpiryWorkClassExecutor newExpiryGcExecutor(
            final WorkClassExecutionRegistry registry,
            final TargetReservationControls.Authority controls,
            final TargetReservationExpiryWorkClassExecutor.Limits gcLimits,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            final LongSupplier ownerClock) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("expiry GC requires the bound Target source WorkClass graph");
        }
        Objects.requireNonNull(physicalWrites, "physicalWrites");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetReservationExpiryWorkClassExecutor(
                registry,
                new TargetReservationExpiryStore(backend, scope, lineage, limits.domains(), controls),
                gcLimits,
                () -> requireGcOwner(clock),
                (actual, actualScope) -> {
                    requireGcOwner(clock);
                    return gcGuard(authorities.reads().acquire(actual, actualScope), actual, actualScope, clock);
                },
                (actual, actualScope, mutation) -> {
                    if (!mutation.quota().counters().mutation().reservationExpiry()) {
                        throw new IllegalStateException("expiry GC cannot commit another mutation kind");
                    }
                    requireGcOwner(clock);
                    return gcGuard(physicalWrites.acquire(actual, actualScope, mutation), actual, actualScope, clock);
                },
                expiryQuota,
                monotonicClock);
    }

    /** Provides one bounded alternating Close/expiry action per caller-driven maintenance turn. */
    public synchronized TargetReservationGcRuntime newReservationGcRuntime(
            final WorkClassExecutionRegistry registry,
            final TargetReservationControls.Authority controls,
            final TargetReservationClosureWorkClassExecutor.Limits closeLimits,
            final TargetReservationExpiryWorkClassExecutor.Limits expiryLimits,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final TargetQuotaDelta.ReservationClosureAuthority closureQuota,
            final TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            final TargetQuotaDelta.CloseCursorAuthority cursorQuota,
            final LongSupplier ownerClock) {
        if (maintenanceRuntime != null) {
            throw new IllegalStateException("Target reservation GC runtime is already bound to this Owner");
        }
        final var closeGc = newCloseGcExecutor(
                registry, controls, closeLimits, physicalWrites, closureQuota, expiryQuota, cursorQuota, ownerClock);
        final var expiryGc =
                newExpiryGcExecutor(registry, controls, expiryLimits, physicalWrites, expiryQuota, ownerClock);
        maintenanceRuntime =
                new TargetReservationGcRuntime(registry, scope.shard(), lease.ownerEpoch(), closeGc, expiryGc);
        return maintenanceRuntime;
    }

    synchronized void requireWorkerStore(final ShardStore expectedStore, final SharedRocksDbResources resources) {
        if (store != Objects.requireNonNull(expectedStore, "store")
                || store.sharedResources() != Objects.requireNonNull(resources, "resources")) {
            throw new IllegalArgumentException("Target Worker requires the exact source Store resource graph");
        }
    }

    /**
     * Commits one selected Target head under the active Owner and Store graph. The supplied write
     * authority must also protect current time, policy, permits and the complete Owner identity.
     */
    synchronized TargetClaimRecord claim(
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
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var exactOwner = Objects.requireNonNull(owner, "owner");
        if (exactOwner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(exactOwner.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Target Claim Owner identity differs from the active lease");
        }
        final var claims =
                new TargetClaimStore(backend, scope, lineage, limits.domains(), Objects.requireNonNull(quota, "quota"));
        final var prepared = claims.prepareClaim(
                budget, selected, exactOwner, nowEpochMs, deadlineEpochMs, executionBytes, operationDigest);
        claims.commit(prepared, (actual, actualScope, mutation) -> {
            if (!mutation.quota().counters().mutation().isLocalClaim()) {
                throw new IllegalStateException("Target Claim cannot commit another mutation kind");
            }
            requireGcOwner(clock);
            return gcGuard(
                    Objects.requireNonNull(physicalWrites, "physicalWrites").acquire(actual, actualScope, mutation),
                    actual,
                    actualScope,
                    clock);
        });
        return prepared.claim();
    }

    /** Restores only an unadmitted, exact Claim under the same active Owner and Store guards. */
    synchronized void revokeClaim(
            final BoundedReadBudget budget,
            final TargetClaimRecord expected,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var exact = Objects.requireNonNull(expected, "expected");
        if (exact.owner().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(exact.owner().leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Target revoke Owner identity differs from the active lease");
        }
        final var claims =
                new TargetClaimStore(backend, scope, lineage, limits.domains(), Objects.requireNonNull(quota, "quota"));
        final var prepared = claims.prepareRevoke(budget, exact, exact.owner(), operationDigest);
        claims.commit(prepared, (actual, actualScope, mutation) -> {
            if (!mutation.quota().counters().mutation().isLocalClaim()) {
                throw new IllegalStateException("Target revoke cannot commit another mutation kind");
            }
            requireGcOwner(clock);
            return gcGuard(
                    Objects.requireNonNull(physicalWrites, "physicalWrites").acquire(actual, actualScope, mutation),
                    actual,
                    actualScope,
                    clock);
        });
    }

    /** Rebuilds bounded Target head summaries without granting Claim or Producer authority. */
    synchronized TargetQueueSnapshotReader.Page scanTargetQueues(
            final BoundedReadBudget budget,
            final TargetPartitionId after,
            final int maximumTargets,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains(), targetQueueHeadCache)
                .scan(budget, after, maximumTargets, workerReads(clock));
    }

    synchronized Optional<TargetQueueSnapshotReader.Entry> readTargetQueue(
            final BoundedReadBudget budget, final TargetPartitionId target, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains(), targetQueueHeadCache)
                .readTarget(budget, target, workerReads(clock));
    }

    synchronized OptionalLong readTargetActiveMessages(
            final BoundedReadBudget budget, final TargetPartitionId target, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains())
                .readActiveMessages(budget, target, workerReads(clock));
    }

    synchronized TargetQueueSnapshotReader.Cut readTargetQueueCut(
            final BoundedReadBudget budget, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains()).readCut(budget, workerReads(clock));
    }

    synchronized TargetExpiryDiscoveryStore.Discovery discoverMessageExpiry(
            final BoundedReadBudget budget,
            final TargetExpiryDiscoveryStore.Cursor cursor,
            final TrustedUtcIntervalEvidence evidence,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return expiryDiscovery.discover(budget, cursor, evidence, workerReads(clock));
    }

    synchronized void requireExpirySubmission(
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var work = Objects.requireNonNull(candidate, "candidate");
        final var proof = Objects.requireNonNull(evidence, "evidence");
        final var exactOwner = Objects.requireNonNull(owner, "owner");
        if (!scope.shard().equals(work.locator().messageId().routingId().shardId())
                || exactOwner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(
                        exactOwner.leaseFencingDigest(), Bytes.sha256(lease.leaseToken()))) {
            throw new IllegalArgumentException("Target expiry candidate or Owner differs from the active Shard");
        }
        proof.requireEarliestAtLeast(work.expireAtEpochMs());
    }

    synchronized void requireExpiryAuthoritativelyStrict(
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final LongSupplier ownerClock) {
        requireExpirySubmission(candidate, evidence, owner, ownerClock);
    }

    synchronized void requireCurrentExpiryLogPosition(
            final SourcePosition position,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        try {
            requireGcOwner(clock);
            final var persisted = Objects.requireNonNull(position, "persisted Source Position");
            if (!scope.shard().equals(persisted.shardId())) {
                throw new IllegalStateException("Target expiry appender returned a foreign Shard position");
            }
            assignment.activationBarrier().validatePosition(persisted);
            if (persisted instanceof PulsarSourcePosition) {
                if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)
                        || sourceConnectionGeneration == null
                        || guardAttestationDigest == null) {
                    throw new IllegalStateException("Target expiry append lacks a Pulsar source connection proof");
                }
                barrier.validateSourceConnection(sourceConnectionGeneration, guardAttestationDigest);
            } else if (sourceConnectionGeneration != null || guardAttestationDigest != null) {
                throw new IllegalStateException("Kafka Target expiry append cannot carry a Pulsar connection proof");
            }
            final var applied =
                    Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target source position");
            if (!applied.sameSourceIdentity(persisted)) {
                throw new IllegalStateException("Target expiry position belongs to another assigned source");
            }
        } catch (RuntimeException | Error failure) {
            fenced = true;
            throw failure;
        }
    }

    synchronized boolean expiryAppendApplied(final SourcePosition position, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var current = Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target source");
        return current.compareTo(Objects.requireNonNull(position, "expiry source position")) >= 0;
    }

    synchronized Optional<SystemMutationResult> expiryMutationResult(
            final com.nereusstream.delay.protocol.SystemMutation mutation, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var exact = Objects.requireNonNull(mutation, "mutation");
        if (exact.type() != SystemMutationType.EXPIRE_GENERATION) {
            throw new IllegalArgumentException("Target expiry reconciliation requires EXPIRE_GENERATION");
        }
        return replay.appliedResult(
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock),
                exact,
                workerReads(clock));
    }

    /** Validates one current head and its frozen byte cost only when the Worker selects it. */
    synchronized TargetHeadCostProbe.Cost probeSelectedHead(
            final BoundedReadBudget budget, final TargetHeadRef selected, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetHeadCostProbe(backend, scope, limits.domains()).probe(budget, selected, workerReads(clock));
    }

    /** Builds historical Native trust from this Owner's exact source-applied Store view. */
    public synchronized TargetNativePolicyTrustStore nativePolicyTrustStore(final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetNativePolicyTrustStore(
                backend,
                scope,
                lineage,
                limits.records(),
                limits.bytes(),
                limits.elapsedNanos(),
                monotonicClock,
                nativePolicyReads(clock));
    }

    private TargetStoreBackend.ReadAuthority workerReads(final LongSupplier clock) {
        return (actual, actualScope) -> {
            requireGcOwner(clock);
            return gcGuard(authorities.reads().acquire(actual, actualScope), actual, actualScope, clock);
        };
    }

    private TargetStoreBackend.ReadAuthority nativePolicyReads(final LongSupplier clock) {
        return (actual, actualScope) -> {
            requireGcOwner(clock);
            if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                throw new IllegalStateException("Target Native trust belongs to another Store/scope");
            }
            final byte[] sourceFrontier = Objects.requireNonNull(
                            store.appliedShardLogPosition(), "initialized Target source")
                    .canonicalBytes();
            final var external = Objects.requireNonNull(
                    authorities.reads().acquire(actual, actualScope), "Target Native read guard");
            return new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {
                    if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                        throw new IllegalStateException("Target Native trust guard belongs to another Store/scope");
                    }
                    requireGcOwner(clock);
                    external.requireCurrent();
                    requireGcOwner(clock);
                    final var current =
                            Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target source");
                    if (!Arrays.equals(sourceFrontier, current.canonicalBytes())) {
                        throw new IllegalStateException("Target Native trust source frontier changed during read");
                    }
                }

                @Override
                public void close() {
                    external.close();
                }
            };
        };
    }

    /** Queues an unpublished candidate on this Owner's exact source/Store WorkClass graph. */
    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitLocalCheckpointCandidate(
            final WorkClassExecutionRegistry registry,
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return submitLocalCheckpointCandidate(
                registry,
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                () -> {});
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitLocalCheckpointCandidate(
            final WorkClassExecutionRegistry registry,
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final Runnable sourceCutGuard) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("Target checkpoint requires the bound source WorkClass graph");
        }
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        final Runnable cut = Objects.requireNonNull(sourceCutGuard, "sourceCutGuard");
        requireGcOwner(clock);
        cut.run();
        final var executor = new TargetCheckpointCandidateWorkClassExecutor(
                registry, store, authorities.leases(), intents, clock, () -> {
                    requireGcOwner(clock);
                    cut.run();
                });
        return executor.submit(new TargetCheckpointCandidateWorkClassExecutor.Request(
                checkpointPath, pending, lease, physicalLimits, quotaLimits, ledgerLimits));
    }

    synchronized OxiaOwnerLeaseStore drainAuthority() {
        return authorities.leases();
    }

    synchronized OwnerLease requireActiveDrainLease(final LongSupplier clock) {
        requireGcOwner(Objects.requireNonNull(clock, "clock"));
        return lease;
    }

    synchronized OwnerLease snapshotDrainLease() {
        return lease;
    }

    private TargetStoreBackend.CommitGuard gcGuard(
            final TargetStoreBackend.CommitGuard external,
            final StoreMetadata actual,
            final TargetQuotaScope actualScope,
            final LongSupplier clock) {
        final var delegate = Objects.requireNonNull(external, "Close GC authority guard");
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                    throw new IllegalStateException("Close GC guard belongs to another Store/scope");
                }
                requireGcOwner(clock);
                delegate.requireCurrent();
                requireGcOwner(clock);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    private void requireGcOwner(final LongSupplier clock) {
        try {
            requireOwner(clock);
        } catch (RuntimeException | Error failure) {
            fenced = true;
            throw failure;
        }
    }

    @Override
    synchronized void requireSubmission(final SourceReplayEntry entry, final boolean recovery) {
        if (fenced || recovery) {
            throw new IllegalStateException("Target active source is fenced or was used as a recovery executor");
        }
        validateEntry(entry);
    }

    @Override
    synchronized SourceReplayOutcome apply(
            final SourceReplayEntry entry, final LongSupplier clock, final boolean recovery) {
        try {
            requireSubmission(entry, recovery);
            requireOwner(clock);
            if (entry instanceof SourceReplayRecord command) {
                return applyCommand(command, clock);
            }
            final var mutation = (SourceReplayMutation) entry;
            final var budget =
                    new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock);
            final TargetSystemReplayStore.Prepared duplicate;
            // Only backend preparation yields are retryable. External resolution and commit failures must fence.
            try {
                duplicate = replay.prepareIfPresent(budget, mutation.mutation(), entry.position())
                        .orElse(null);
            } catch (ReadIncompleteException incomplete) {
                throw new ReadYield(incomplete);
            }
            final SystemMutationResult result;
            if (duplicate != null) {
                result = replay.commit(
                        duplicate, writes(authorities.duplicateWrites(), entry, clock), reads(entry, clock));
            } else if (mutation.mutation().type() == SystemMutationType.TIME_FENCE) {
                final var control = Objects.requireNonNull(authorities.fences().resolve(mutation), "fence control");
                final TargetTimeFenceStore.Prepared first;
                try {
                    first = fences.prepareFirst(budget, mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = fences.commit(first, writes(control.commit(), entry, clock));
            } else if (mutation.mutation().type() == SystemMutationType.EXPIRE_GENERATION) {
                final var control =
                        Objects.requireNonNull(authorities.expiries().resolve(mutation), "Target expiry control");
                final TargetExpireGenerationStore.Prepared first;
                try {
                    first = expiries.prepareFirst(budget, mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = expiries.commit(first, writes(control.commit(), entry, clock));
            } else if (isTargetClose(mutation.mutation())) {
                final var control =
                        Objects.requireNonNull(authorities.closes().resolve(mutation), "Target Close control");
                final TargetCloseStore.Prepared first;
                try {
                    first = closes.prepareFirst(
                            budget, control.prepared(), mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = closes.commit(first, writes(control.commit(), entry, clock));
            } else if (isMembershipControl(mutation.mutation())) {
                final var control = Objects.requireNonNull(
                        authorities.membershipControls().resolve(mutation), "membership control");
                final TargetMembershipControlStore.Prepared first;
                try {
                    first = membershipControls.prepareFirst(
                            budget,
                            control.prepared(),
                            mutation.mutation(),
                            entry.position(),
                            control.physical(),
                            control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = membershipControls.commit(first, writes(control.commit(), entry, clock));
            } else if (isNativePolicyControl(mutation.mutation())) {
                final var control = Objects.requireNonNull(
                        authorities.nativePolicyControls().resolve(mutation), "Target Native policy control");
                final TargetNativePolicyControlStore.Prepared first;
                try {
                    first = nativePolicyControls.prepareFirst(
                            budget, control.prepared(), mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = nativePolicyControls.commit(first, writes(control.commit(), entry, clock));
            } else {
                final var control = Objects.requireNonNull(authorities.grants().resolve(mutation), "grant control");
                final TargetQuotaGrantStore.Prepared first;
                try {
                    first = grants.prepareFirst(
                            budget, control.prepared(), mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = grants.commit(first, writes(control.commit(), entry, clock));
            }
            // Broker ACK carries this physical record's anchor, while the Store retains the first logical result.
            return SourceReplayOutcome.systemMutation(
                    entry.position(),
                    new SystemMutationResult(
                            result.mutationId(),
                            result.mutationHash(),
                            result.mutationType(),
                            result.retryUntilEpochMs(),
                            result.authorIdentity(),
                            result.applyStatus(),
                            result.stableCode(),
                            entry.position().canonicalBytes()));
        } catch (ReadYield incomplete) {
            throw incomplete;
        } catch (RuntimeException | Error failure) {
            fenced = true;
            throw failure;
        }
    }

    private SourceReplayOutcome applyCommand(final SourceReplayRecord entry, final LongSupplier clock) {
        final var budget =
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock);
        final TargetCommandReplayStore.Prepared prepared;
        try {
            prepared = commandReplay
                    .prepareReplayOrExpired(budget, entry.command(), entry.position())
                    .orElse(null);
        } catch (ReadIncompleteException incomplete) {
            throw new ReadYield(incomplete);
        }
        final CommandResult result;
        if (prepared != null) {
            result = commandReplay.commit(
                    prepared, writes(authorities.duplicateWrites(), entry, clock), reads(entry, clock));
        } else {
            // Resolve external snapshots outside the zero-write backend retry classifier.
            final var control = Objects.requireNonNull(authorities.commands().resolve(entry), "command control");
            final TargetCommandStore.Prepared first;
            try {
                first = commands.prepareFirst(
                        budget,
                        entry.command(),
                        entry.position(),
                        control.policy(),
                        (reader, binding, source) -> {
                            try {
                                return control.cancellations().closed(reader, binding, source);
                            } catch (ReadIncompleteException external) {
                                throw new IllegalStateException(
                                        "external cancellation authority did not complete", external);
                            }
                        },
                        control.reservationControls(),
                        control.schedules(),
                        control.payloadProofs());
            } catch (ReadIncompleteException incomplete) {
                throw new ReadYield(incomplete);
            }
            result = commands.commit(first, writes(control.commit(), entry, clock));
        }
        return SourceReplayOutcome.command(
                entry.position(),
                new CommandResult(
                        result.applyStatus(),
                        result.stableCode(),
                        result.generation(),
                        result.stateVersion(),
                        result.messageStatus(),
                        entry.position().canonicalBytes()));
    }

    private static byte[] sourceDigest(final SourceReplayEntry entry) {
        if (entry instanceof SourceReplayRecord command) {
            return command.command().canonicalFrameDigest();
        }
        return Bytes.sha256(((SourceReplayMutation) entry).mutation().canonicalEnvelope());
    }

    private TargetStoreBackend.CommitAuthority writes(
            final TargetStoreBackend.CommitAuthority external,
            final SourceReplayEntry entry,
            final LongSupplier clock) {
        return (actual, actualScope, mutation) -> {
            final var stamp = mutation.quota().counters().mutation();
            if (stamp.isLocalMutation()
                    || !Arrays.equals(
                            stamp.source().canonicalBytes(), entry.position().canonicalBytes())
                    || !Arrays.equals(stamp.mutationDigest(), sourceDigest(entry))) {
                throw new IllegalStateException("Target source commit changed its exact source/envelope stamp");
            }
            requireOwner(clock);
            return guard(external.acquire(actual, actualScope, mutation), actual, actualScope, entry, clock);
        };
    }

    private TargetStoreBackend.ReadAuthority reads(final SourceReplayEntry entry, final LongSupplier clock) {
        return (actual, actualScope) -> {
            requireOwner(clock);
            return guard(authorities.reads().acquire(actual, actualScope), actual, actualScope, entry, clock);
        };
    }

    private TargetStoreBackend.CommitGuard guard(
            final TargetStoreBackend.CommitGuard external,
            final StoreMetadata actual,
            final TargetQuotaScope actualScope,
            final SourceReplayEntry entry,
            final LongSupplier clock) {
        Objects.requireNonNull(external, "external guard");
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                    throw new IllegalStateException("Target source guard belongs to another Store/scope");
                }
                validateEntry(entry);
                final SourcePosition prior =
                        Objects.requireNonNull(store.appliedShardLogPosition(), "initialized source");
                authorities.successor().validate(prior, entry.position());
                external.requireCurrent();
                requireOwner(clock);
            }

            @Override
            public void close() {
                external.close();
            }
        };
    }

    @Override
    synchronized void beforeAcknowledgement(
            final SourceReplayEntry entry, final SourceReplayOutcome outcome, final LongSupplier clock) {
        try (var guard = reads(entry, clock).acquire(store.metadata(), scope)) {
            guard.requireCurrent();
            if (!Arrays.equals(
                    store.appliedShardLogPosition().canonicalBytes(),
                    entry.position().canonicalBytes())) {
                throw new IllegalStateException("Target ACK no longer names the current durable source");
            }
        } catch (RuntimeException | Error failure) {
            fenced = true;
            throw failure;
        }
    }

    private void requireOwner(final LongSupplier clock) {
        final OwnerLease initialLease = lease;
        if (fenced || store.runtimeMetadata().lastOpenedOwnerEpoch() != initialLease.ownerEpoch()) {
            throw new IllegalStateException("Target source Owner/Store is fenced");
        }
        final LongSupplier ownerClock = Objects.requireNonNull(clock, "ownerClock");
        while (true) {
            final OwnerLease observedLease = lease;
            final OwnerLease current = authorities.leases().current(scope.shard()).orElse(null);
            synchronized (ownerStateLock) {
                final OwnerLease activeLease = lease;
                if (fenced || !activeLease.sameIdentity(initialLease)) {
                    throw new IllegalStateException("Target source Owner/Store is fenced");
                }
                if (current == null
                        || !activeLease.sameIdentity(current)
                        || current.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                    throw new IllegalStateException("Target source authoritative lease/time changed");
                }
                if (current.expiresAtEpochMs() < activeLease.expiresAtEpochMs()) {
                    if (activeLease != observedLease) {
                        continue;
                    }
                    throw new IllegalStateException("Target source authoritative lease/time changed");
                }
                final long now = ownerClock.getAsLong();
                if (now < 0 || now < lastOwnerTime || !current.validAt(now)) {
                    throw new IllegalStateException("Target source authoritative lease/time changed");
                }
                lastOwnerTime = now;
                if (current.expiresAtEpochMs() > activeLease.expiresAtEpochMs()) {
                    lease = current;
                }
                return;
            }
        }
    }

    private void validateEntry(final SourceReplayEntry entry) {
        Objects.requireNonNull(entry, "entry");
        assignment.activationBarrier().validatePosition(entry.position());
        if (entry.position() instanceof PulsarSourcePosition) {
            if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)
                    || entry.sourceConnectionGeneration() == null
                    || entry.guardAttestationDigest() == null) {
                throw new IllegalArgumentException("Target source lacks a Pulsar connection proof");
            }
            barrier.validateSourceConnection(entry.sourceConnectionGeneration(), entry.guardAttestationDigest());
        } else if (entry.sourceConnectionGeneration() != null || entry.guardAttestationDigest() != null) {
            throw new IllegalArgumentException("Kafka source cannot carry Pulsar connection proof");
        }
        if (entry instanceof SourceReplayRecord) {
            return;
        }
        if (entry instanceof SourceReplayMutation fence && fence.mutation().type() == SystemMutationType.TIME_FENCE) {
            return;
        }
        if (entry instanceof SourceReplayMutation expiry
                && expiry.mutation().type() == SystemMutationType.EXPIRE_GENERATION) {
            TargetExpireGenerationBody.decode(expiry.mutation().canonicalBody());
            return;
        }
        if (!(entry instanceof SourceReplayMutation mutation)
                || mutation.mutation().type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException(
                    "Target source business branch is not wired yet; source must remain pending");
        }
        if (isTargetClose(mutation.mutation())) {
            TargetCloseBody.decode(mutation.mutation().canonicalBody());
        } else if (isMembershipControl(mutation.mutation())) {
            TargetMembershipControlBody.decode(mutation.mutation().canonicalBody());
        } else if (isNativePolicyControl(mutation.mutation())) {
            TargetNativePolicyControlBody.decode(mutation.mutation().canonicalBody());
        } else {
            TargetQuotaGrantControlBody.decode(mutation.mutation().canonicalBody());
        }
    }

    /** Bounded structural discriminator only; the selected full decoder must still validate every field. */
    private static boolean isTargetClose(com.nereusstream.delay.protocol.SystemMutation mutation) {
        return targetControlKind(mutation) == TargetCloseRequest.CONTROL_KIND;
    }

    private static boolean isMembershipControl(com.nereusstream.delay.protocol.SystemMutation mutation) {
        final int kind = targetControlKind(mutation);
        return kind == 15 || kind == 16;
    }

    private static boolean isNativePolicyControl(com.nereusstream.delay.protocol.SystemMutation mutation) {
        final int kind = targetControlKind(mutation);
        return kind >= 19 && kind <= 23;
    }

    private static int targetControlKind(com.nereusstream.delay.protocol.SystemMutation mutation) {
        final byte[] body = mutation.canonicalBody();
        if (body.length
                > Math.max(
                        TargetNativePolicyControlBody.MAX_CANONICAL_BYTES,
                        Math.max(
                                TargetMembershipControlBody.MAX_CANONICAL_BYTES,
                                Math.max(
                                        TargetCloseBody.MAX_CANONICAL_BYTES,
                                        TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES)))) {
            throw new IllegalArgumentException("Target control body exceeds activated codec bounds");
        }
        final var reader = new CanonicalProtobuf.Reader(body);
        for (int field = 0; field < 5 && reader.hasRemaining(); field++) {
            final var value = reader.next();
            if (value.number() == 11) {
                if (value.wireType() != 0) {
                    throw new IllegalArgumentException("Target control kind has a non-varint wire type");
                }
                return Math.toIntExact(value.unsignedValue());
            }
        }
        return -1;
    }

    @Override
    public synchronized void fence() {
        fenced = true;
    }

    public synchronized boolean fenced() {
        return fenced;
    }

    @Override
    boolean isReadIncomplete(final Throwable failure) {
        return failure instanceof ReadYield;
    }

    private static final class ReadYield extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ReadYield(final ReadIncompleteException cause) {
            super("Target source preparation yielded before commit", cause);
        }
    }
}
