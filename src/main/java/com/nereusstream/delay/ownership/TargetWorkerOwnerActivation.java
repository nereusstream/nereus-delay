package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Activates a Target Store owner for a lease-bound assignment after its source barrier is durable. */
public final class TargetWorkerOwnerActivation {
    private TargetWorkerOwnerActivation() {}

    /**
     * Persists the Owner epoch before transitioning the exact assignment-bound lease to ACTIVE. If the CAS result is
     * uncertain, an exact current ACTIVE lease is accepted on readback or a subsequent retry. The caller retains
     * Store/lease cleanup if this method fails; no Worker or source consumer has been transferred yet. The caller
     * must establish external assignment acceptance; this method verifies only the supplied assignment's local
     * lease identity and Store barrier. The supplied nondecreasing epoch-millisecond clock must also be used by the
     * activated Target source runtime.
     */
    public static OwnerLease activate(
            final TargetStoreBootstrap.Initialized initialized,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final OxiaOwnerLeaseStore leases,
            final LongSupplier ownerClock) {
        final var opened = Objects.requireNonNull(initialized, "initialized");
        return activate(opened.backend(), opened.root().scope(), store, assignment, lease, leases, ownerClock);
    }

    /** Recovery counterpart for a Target root reconstructed from the reopened Store. */
    public static OwnerLease activate(
            final TargetStoreBootstrap.Reopened reopened,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final OxiaOwnerLeaseStore leases,
            final LongSupplier ownerClock) {
        final var opened = Objects.requireNonNull(reopened, "reopened");
        return activate(opened.backend(), opened.root().scope(), store, assignment, lease, leases, ownerClock);
    }

    private static OwnerLease activate(
            final TargetStoreBackend backend,
            final TargetQuotaScope scope,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final OxiaOwnerLeaseStore leases,
            final LongSupplier ownerClock) {
        final var exactBackend = Objects.requireNonNull(backend, "backend");
        final var exactScope = Objects.requireNonNull(scope, "scope");
        final var exactStore = Objects.requireNonNull(store, "store");
        final var exactAssignment = Objects.requireNonNull(assignment, "assignment");
        final var expectedLease = Objects.requireNonNull(lease, "lease");
        final var authority = Objects.requireNonNull(leases, "leases");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        exactBackend.requireStore(exactStore);
        if (exactStore.metadata().storeFormatVersion() != 2
                || !exactScope.shard().equals(exactStore.shardId())
                || !exactScope.shard().equals(exactAssignment.shardId())
                || expectedLease.context() == null
                || !expectedLease.shardId().equals(exactAssignment.shardId())
                || !Arrays.equals(expectedLease.sourceAssignmentId(), exactAssignment.assignmentId())
                || expectedLease.sourceAssignmentEpoch() != exactAssignment.assignmentEpoch()
                || !exactAssignment.activationBarrier().reachedBy(exactStore.appliedShardLogPosition())) {
            throw new IllegalArgumentException("Target Owner activation lacks exact Store/assignment/barrier state");
        }
        if (expectedLease.state() != ShardLifecycleState.ACQUIRING
                && expectedLease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            throw new IllegalArgumentException("Target Owner activation requires an ACQUIRING or exact ACTIVE lease");
        }

        final long beforeWriteNow = requireNonNegativeTime(clock);
        final OwnerLease current = authority.current(exactAssignment.shardId())
                .orElseThrow(() -> new IllegalStateException("Target Owner lease is no longer current"));
        requireSameLease(expectedLease, current);
        if (!current.validAt(beforeWriteNow)) {
            throw new IllegalStateException("Target Owner lease expired before Store activation");
        }

        final long openedOwnerEpoch = exactStore.runtimeMetadata().lastOpenedOwnerEpoch();
        if (current.state() == ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
            if (expectedLease.state() != ShardLifecycleState.ACQUIRING
                    && expectedLease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                throw new IllegalStateException("Target Owner activation observed an unexpected lease state");
            }
            if (Long.compareUnsigned(openedOwnerEpoch, current.ownerEpoch()) != 0) {
                throw new IllegalStateException("active Target Owner does not match the persisted Store epoch");
            }
            return current;
        }
        if (current.state() != ShardLifecycleState.ACQUIRING
                || expectedLease.state() != ShardLifecycleState.ACQUIRING) {
            throw new IllegalStateException("Target Owner activation lease is not ACQUIRING");
        }
        if (Long.compareUnsigned(openedOwnerEpoch, current.ownerEpoch()) > 0) {
            throw new IllegalStateException("Target Store already records a newer Owner epoch");
        }

        if (Long.compareUnsigned(openedOwnerEpoch, current.ownerEpoch()) < 0) {
            exactStore.recordOpenedOwnerEpoch(current.ownerEpoch());
        }
        final long beforeCasNow = requireNonDecreasingTime(clock, beforeWriteNow);
        final OwnerLease stillAcquiring = authority.current(exactAssignment.shardId())
                .orElseThrow(() -> new IllegalStateException("Target Owner lease disappeared before activation"));
        requireSameLease(current, stillAcquiring);
        if (stillAcquiring.state() != ShardLifecycleState.ACQUIRING || !stillAcquiring.validAt(beforeCasNow)) {
            throw new IllegalStateException("Target Owner lease changed before activation CAS");
        }

        final OwnerLease active = authority.transitionOrRead(current, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                .orElseThrow(() -> new IllegalStateException("Target Owner activation CAS was not observed"));
        final long afterCasNow = requireNonDecreasingTime(clock, beforeCasNow);
        final OwnerLease observed = authority.current(exactAssignment.shardId())
                .orElseThrow(() -> new IllegalStateException("Target Owner lease disappeared after activation CAS"));
        requireSameLease(current, active);
        requireSameLease(current, observed);
        if (active.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || observed.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || !active.validAt(afterCasNow)
                || !observed.validAt(afterCasNow)
                || Long.compareUnsigned(exactStore.runtimeMetadata().lastOpenedOwnerEpoch(), active.ownerEpoch())
                        != 0) {
            throw new IllegalStateException("Target Owner activation did not retain the exact active Store epoch");
        }
        return observed;
    }

    private static long requireNonNegativeTime(final LongSupplier clock) {
        final long now = clock.getAsLong();
        if (now < 0) {
            throw new IllegalArgumentException("Target Owner clock must be non-negative");
        }
        return now;
    }

    private static long requireNonDecreasingTime(final LongSupplier clock, final long previous) {
        final long now = requireNonNegativeTime(clock);
        if (now < previous) {
            throw new IllegalStateException("Target Owner clock moved backwards during activation");
        }
        return now;
    }

    private static void requireSameLease(final OwnerLease expected, final OwnerLease observed) {
        if (!expected.sameIdentity(observed)) {
            throw new IllegalStateException("Target Owner lease identity changed during activation");
        }
    }
}
