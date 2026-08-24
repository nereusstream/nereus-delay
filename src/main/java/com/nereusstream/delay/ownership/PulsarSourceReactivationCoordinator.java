package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ActivationBarrierV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Oxia authority sequence for reactivating a Pulsar source after reconnect.
 *
 * <p>The old Owner is fenced before the old source is closed.  A caller must
 * then prove that the old runtime is quiescent, after which the old lease is
 * released and the successor assignment is published by revision CAS.  Only
 * that published successor can acquire the next context-bound Owner Lease.</p>
 */
public final class PulsarSourceReactivationCoordinator {
    private final RouteSnapshotProvider routeProvider;
    private final WorkerAssignmentAuthority assignmentAuthority;
    private final OxiaOwnerLeaseStore ownerAuthority;

    public PulsarSourceReactivationCoordinator(
            final RouteSnapshotProvider routeProvider,
            final WorkerAssignmentAuthority assignmentAuthority,
            final OxiaOwnerLeaseStore ownerAuthority) {
        this.routeProvider = Objects.requireNonNull(routeProvider, "routeProvider");
        this.assignmentAuthority = Objects.requireNonNull(assignmentAuthority, "assignmentAuthority");
        this.ownerAuthority = Objects.requireNonNull(ownerAuthority, "ownerAuthority");
    }

    /** Fences the exact active Owner after all route/assignment checks pass. */
    public FencedPlan fenceForReactivation(
            final AuthenticatedTenantContext context,
            final WorkerAssignmentAuthority.Publication expectedPublication,
            final OwnerLease expectedLease,
            final PulsarSourceReactivationV1 reactivation,
            final long nowEpochMs) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(expectedPublication, "expectedPublication");
        Objects.requireNonNull(expectedLease, "expectedLease");
        Objects.requireNonNull(reactivation, "reactivation");
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("reactivation clock must be non-negative");
        }
        final WorkerAssignment previous = expectedPublication.assignment();
        requirePreviousAssignment(previous, reactivation);
        if (!previous.workerId().equals(expectedLease.ownerId())) {
            throw new IllegalArgumentException("Owner Lease is held by a different Worker");
        }
        final RouteSnapshotV1 route =
                routeProvider.exact(reactivation.previousAssignment().shardId().routeIncarnation(), context);
        requireRoute(route, context, reactivation);
        final WorkerAssignmentAuthority.Publication observedPublication = assignmentAuthority
                .current(reactivation.previousAssignment().shardId())
                .orElseThrow(
                        () -> new IllegalStateException("Pulsar source assignment is unavailable during reactivation"));
        if (!samePublication(expectedPublication, observedPublication)) {
            throw new IllegalStateException("Pulsar source assignment changed before reactivation fence");
        }
        final OwnerLease observedLease = ownerAuthority
                .current(expectedLease.shardId())
                .orElseThrow(
                        () -> new IllegalStateException("Pulsar Owner Lease disappeared before reactivation fence"));
        if (!expectedLease.sameIdentity(observedLease)
                || observedLease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                || !observedLease.validAt(nowEpochMs)
                || !contextMatches(observedLease, reactivation.previousAssignment())) {
            throw new IllegalStateException("Pulsar Owner Lease is not the exact active source owner");
        }
        final OwnerLease fenced = ownerAuthority
                .transitionOrRead(expectedLease, ShardLifecycleState.FENCED)
                .orElseThrow(() -> new IllegalStateException("Pulsar Owner Lease fence CAS was lost"));
        if (!expectedLease.sameIdentity(fenced)
                || fenced.state() != ShardLifecycleState.FENCED
                || !fenced.validAt(nowEpochMs)) {
            throw new IllegalStateException("Pulsar Owner Lease fence changed identity or expired");
        }
        return new FencedPlan(expectedPublication, expectedLease, fenced, reactivation);
    }

    /**
     * Publishes the successor only after the caller proves that the old source
     * runtime and its callbacks are closed/quiescent. This method is
     * idempotent after a successful successor publication.
     */
    public WorkerAssignmentAuthority.Publication publishSuccessor(
            final FencedPlan plan, final WorkerAssignment successor, final SourceQuiescenceProof quiescence) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(successor, "successor");
        Objects.requireNonNull(quiescence, "quiescence");
        requireSuccessorAssignment(plan, successor);
        final ShardId shard = plan.reactivation().successorAssignment().shardId();
        final long successorRevision = nextRevision(plan.expectedPublication().revision());
        final Optional<OwnerLease> currentOwner = ownerAuthority.current(shard);
        if (currentOwner.isPresent()
                && (!plan.fencedLease().sameIdentity(currentOwner.get())
                        || currentOwner.get().state() != ShardLifecycleState.FENCED)) {
            throw new IllegalStateException("Pulsar successor publication found a foreign Owner Lease");
        }
        final Optional<WorkerAssignmentAuthority.Publication> currentAssignment = assignmentAuthority.current(shard);
        if (currentAssignment.isPresent()
                && currentAssignment.get().revision() == successorRevision
                && currentAssignment.get().assignment().sameIdentity(successor)) {
            if (currentOwner.isPresent()) {
                throw new IllegalStateException("Pulsar successor assignment is published while old Owner remains");
            }
            return currentAssignment.get();
        }
        if (currentAssignment.isEmpty() || !samePublication(plan.expectedPublication(), currentAssignment.get())) {
            throw new IllegalStateException("Pulsar source assignment changed before successor publication");
        }

        quiescence.requireQuiesced();
        releaseFencedOwner(plan.fencedLease(), shard);
        final Optional<WorkerAssignmentAuthority.Publication> afterRelease = assignmentAuthority.current(shard);
        if (afterRelease.isPresent()
                && afterRelease.get().revision() == successorRevision
                && afterRelease.get().assignment().sameIdentity(successor)) {
            return afterRelease.get();
        }
        if (afterRelease.isEmpty() || !samePublication(plan.expectedPublication(), afterRelease.get())) {
            throw new IllegalStateException("Pulsar source assignment changed while old Owner was released");
        }
        try {
            final WorkerAssignmentAuthority.Publication published = assignmentAuthority.publish(
                    successor, plan.expectedPublication().revision());
            requireSuccessorPublication(published, successor, successorRevision);
            return published;
        } catch (RuntimeException failure) {
            final Optional<WorkerAssignmentAuthority.Publication> reread = assignmentAuthority.current(shard);
            if (reread.isPresent()
                    && reread.get().revision() == successorRevision
                    && reread.get().assignment().sameIdentity(successor)) {
                return reread.get();
            }
            throw failure;
        }
    }

    /** Acquires the new context-bound Owner only after the successor is durable. */
    public OwnerLease acquireSuccessor(
            final FencedPlan plan,
            final WorkerAssignmentAuthority.Publication successorPublication,
            final String workerId,
            final byte[] sessionIdentity,
            final long nowEpochMs,
            final long leaseDurationMs) {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(successorPublication, "successorPublication");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(sessionIdentity, "sessionIdentity");
        requireSuccessorAssignment(plan, successorPublication.assignment());
        final WorkerAssignmentAuthority.Publication observed = assignmentAuthority
                .current(successorPublication.assignment().sourceAssignment().shardId())
                .orElseThrow(() ->
                        new IllegalStateException("Pulsar successor assignment is unavailable before Owner acquire"));
        if (!samePublication(successorPublication, observed)) {
            throw new IllegalStateException("Pulsar successor assignment changed before Owner acquire");
        }
        if (ownerAuthority
                .current(successorPublication.assignment().sourceAssignment().shardId())
                .isPresent()) {
            throw new IllegalStateException("Pulsar successor Owner Lease already exists");
        }
        final OwnerLease acquired = ownerAuthority
                .acquire(
                        successorPublication.assignment().sourceAssignment(),
                        workerId,
                        sessionIdentity,
                        nowEpochMs,
                        leaseDurationMs)
                .orElseThrow(() -> new IllegalStateException("Pulsar successor Owner Lease acquire was rejected"));
        if (!workerId.equals(acquired.ownerId())
                || acquired.state() != ShardLifecycleState.ACQUIRING
                || !acquired.validAt(nowEpochMs)
                || !contextMatches(acquired, plan.reactivation().successorAssignment())) {
            throw new IllegalStateException("Pulsar successor Owner Lease is not context-bound");
        }
        return acquired;
    }

    private void releaseFencedOwner(final OwnerLease fencedLease, final ShardId shard) {
        if (ownerAuthority.current(shard).isEmpty()) {
            return;
        }
        if (!ownerAuthority.release(fencedLease)) {
            if (ownerAuthority.current(shard).isPresent()) {
                throw new IllegalStateException("Pulsar fenced Owner Lease release was lost");
            }
            return;
        }
        if (ownerAuthority.current(shard).isPresent()) {
            throw new IllegalStateException("Pulsar fenced Owner Lease remained after release");
        }
    }

    private static void requirePreviousAssignment(
            final WorkerAssignment previous, final PulsarSourceReactivationV1 reactivation) {
        if (!previous.routeBound()
                || !Bytes.constantTimeEquals(previous.routeSnapshotDigest(), reactivation.routeSnapshotDigest())
                || !previous.sourceAssignment().sameIdentity(reactivation.previousAssignment())) {
            throw new IllegalArgumentException("Pulsar reactivation does not name the accepted Route assignment");
        }
    }

    private static void requireRoute(
            final RouteSnapshotV1 route,
            final AuthenticatedTenantContext context,
            final PulsarSourceReactivationV1 reactivation) {
        if (route == null) {
            throw new IllegalArgumentException("Pulsar reactivation Route snapshot is unavailable");
        }
        route.requireTenantScope(context.authenticatedTenantScopeHash(), context.tenantRoutingScope());
        if (!Bytes.constantTimeEquals(route.snapshotDigest(), reactivation.routeSnapshotDigest())) {
            throw new IllegalArgumentException("Pulsar reactivation Route snapshot digest changed");
        }
        final int partition = reactivation.previousAssignment().shardId().partition();
        final ActivationBarrierV1 routeBarrier;
        try {
            routeBarrier = route.partitionPolicy(partition).activationBarrier();
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(
                    "Pulsar reactivation Route does not contain the source partition", failure);
        }
        if (!routeBarrier
                .toSourceBarrier(reactivation.previousAssignment().shardId().routeIncarnation())
                .equals(reactivation.previousAssignment().activationBarrier())) {
            throw new IllegalArgumentException("Pulsar reactivation Route barrier differs from old assignment");
        }
    }

    private static void requireSuccessorAssignment(final FencedPlan plan, final WorkerAssignment successor) {
        final WorkerAssignment previous = plan.expectedPublication().assignment();
        if (!successor.routeBound()
                || !Bytes.constantTimeEquals(previous.routeSnapshotDigest(), successor.routeSnapshotDigest())
                || !successor
                        .sourceAssignment()
                        .sameIdentity(plan.reactivation().successorAssignment())
                || !successor.workerId().equals(previous.workerId())
                || !Arrays.equals(successor.capacityEnvelopeDigest(), previous.capacityEnvelopeDigest())
                || Long.compareUnsigned(successor.placementEpoch(), previous.placementEpoch()) <= 0) {
            throw new IllegalArgumentException("Pulsar successor Worker assignment is not an exact reactivation");
        }
    }

    private static void requireSuccessorPublication(
            final WorkerAssignmentAuthority.Publication publication,
            final WorkerAssignment successor,
            final long expectedRevision) {
        if (publication == null
                || publication.revision() != expectedRevision
                || !publication.assignment().sameIdentity(successor)) {
            throw new IllegalStateException("Pulsar successor assignment authority returned an unexpected record");
        }
    }

    private static boolean samePublication(
            final WorkerAssignmentAuthority.Publication expected, final WorkerAssignmentAuthority.Publication actual) {
        return actual != null
                && expected.revision() == actual.revision()
                && expected.assignment().sameIdentity(actual.assignment());
    }

    private static boolean contextMatches(final OwnerLease lease, final SourceAssignment assignment) {
        return lease.context() != null
                && Bytes.constantTimeEquals(lease.sourceAssignmentId(), assignment.assignmentId())
                && lease.sourceAssignmentEpoch() == assignment.assignmentEpoch();
    }

    private static long nextRevision(final long revision) {
        try {
            return Math.addExact(revision, 1);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("Pulsar assignment revision exhausted", overflow);
        }
    }

    /** State captured after the old exact Owner was fenced. */
    public record FencedPlan(
            WorkerAssignmentAuthority.Publication expectedPublication,
            OwnerLease expectedLease,
            OwnerLease fencedLease,
            PulsarSourceReactivationV1 reactivation) {
        public FencedPlan {
            Objects.requireNonNull(expectedPublication, "expectedPublication");
            Objects.requireNonNull(expectedLease, "expectedLease");
            Objects.requireNonNull(fencedLease, "fencedLease");
            Objects.requireNonNull(reactivation, "reactivation");
            if (!expectedLease.sameIdentity(fencedLease) || fencedLease.state() != ShardLifecycleState.FENCED) {
                throw new IllegalArgumentException("fenced plan does not retain the exact Owner identity");
            }
        }
    }

    /** Caller-owned proof that no old source callback can apply or ACK a record. */
    @FunctionalInterface
    public interface SourceQuiescenceProof {
        void requireQuiesced();
    }
}
