package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.PublishAttemptLedger;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.SharedRocksDbResources;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Local owner-drain sequencing for one independently stored Delay Shard.
 *
 * <p>The coordinator only composes local safety boundaries. Source consumer
 * quiescence and publish callback progress are supplied by the caller; an
 * exhausted poll budget fails closed and leaves the DB/lease available for a
 * later retry. Oxia remains the authority for lifecycle and lease CAS.</p>
 */
public final class OwnerDrainCoordinator {
    private final OwnedDelayShard ownedShard;
    private final ShardStore store;
    private final SharedRocksDbResources resources;
    private final OxiaOwnerLeaseStore authority;
    private int pendingRevokedClaims;
    private int pendingCallbackPolls;
    private Path pendingFinalCheckpoint;

    public OwnerDrainCoordinator(final OwnedDelayShard ownedShard, final ShardStore store,
                                 final SharedRocksDbResources resources, final OxiaOwnerLeaseStore authority) {
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.store = Objects.requireNonNull(store, "store");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.authority = Objects.requireNonNull(authority, "authority");
        if (!ownedShard.shard().shardId().equals(store.shardId())) {
            throw new IllegalArgumentException("owned shard and Store belong to different shards");
        }
        if (store.sharedResources() != resources) {
            throw new IllegalArgumentException("drain resources are not the Store resource envelope");
        }
    }

    /**
     * Executes the bounded local drain sequence. A retry is allowed after a
     * callback timeout while the owner remains in {@code DRAINING}.
     */
    public DrainResult drain(final DrainRequest request, final LongSupplier clock,
                             final DrainCallbacks callbacks) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(clock, "clock");
        Objects.requireNonNull(callbacks, "callbacks");
        resources.acquireDrainSlot();
        boolean shardDrainAcquired = false;
        boolean storeClosed = false;
        boolean leaseReleased = false;
        try {
            if (!ownedShard.tryAcquireDrainAttempt()) {
                throw new IllegalStateException("owner drain is already in progress for this shard");
            }
            shardDrainAcquired = true;
            final long startNow = readNow(clock);
            final OwnerLease expectedLease = ownedShard.lease();
            if (store.isWriteOutcomeUncertain()) {
                // A Store whose synchronous write result cannot be proved is
                // not eligible for the normal ACTIVE -> DRAINING CAS: the
                // local owner may already have fenced itself while replaying
                // the source record. Close that exact native Store first,
                // then release only an authority lease with the same fencing
                // identity. The branch remains retryable across native close
                // failure and response-loss lease release.
                if (!store.isCloseStarted() && ownedShard.state() != ShardLifecycleState.DRAINING) {
                    callbacks.stopSourceAndScheduling();
                }
                ownedShard.fence();
                if (!store.isClosed()) {
                    store.close();
                    storeClosed = true;
                } else {
                    storeClosed = true;
                }
                final Optional<OwnerLease> current = authority.current(expectedLease.shardId());
                if (current.isEmpty()) {
                    // The lease may already have been released after the
                    // storage failure; there is nothing safe left to release.
                    leaseReleased = true;
                    return new DrainResult(pendingRevokedClaims, pendingCallbackPolls,
                            pendingFinalCheckpoint);
                }
                final OwnerLease observed = current.orElseThrow();
                if (!expectedLease.sameIdentity(observed)) {
                    // The DB is closed, but never release a replacement
                    // owner's lease after an identity change.
                    throw new IllegalStateException("owner lease changed while closing uncertain Store");
                }
                releaseExactLease(observed);
                leaseReleased = true;
                return new DrainResult(pendingRevokedClaims, pendingCallbackPolls, pendingFinalCheckpoint);
            }
            if (store.isCloseStarted()) {
                if (ownedShard.state() != ShardLifecycleState.DRAINING) {
                    throw new IllegalStateException("Store close already started outside draining state");
                }
                // A previous attempt fenced Store operations but failed during
                // retryable native/slot teardown.  Do not rerun Claim revoke,
                // callback polling, flush or checkpoint logic: those paths
                // are correctly fenced after closeStarted, and replaying them
                // would turn a close retry into a new drain decision.
                RuntimeException closeFailure = null;
                try {
                    store.close();
                    storeClosed = true;
                } catch (RuntimeException failure) {
                    closeFailure = failure;
                }
                if (closeFailure != null) {
                    // Keep local state DRAINING so this exact coordinator can
                    // retry without releasing the authoritative lease while
                    // native teardown remains unconfirmed.
                    throw closeFailure;
                }
                ensureLeaseStillDraining(expectedLease, request, clock);
                releaseExactLease(ownedShard.lease());
                leaseReleased = true;
                return new DrainResult(pendingRevokedClaims, pendingCallbackPolls, pendingFinalCheckpoint);
            }
            if (ownedShard.state() == ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                callbacks.stopSourceAndScheduling();
                ownedShard.beginDrain(authority, startNow);
            } else if (ownedShard.state() != ShardLifecycleState.DRAINING) {
                throw new IllegalStateException("owner drain requires an active or already draining shard");
            }
            ensureLeaseStillDraining(expectedLease, request, clock);
            final DelayShard shard = ownedShard.shard();
            store.recordOpenedOwnerEpoch(expectedLease.ownerEpoch());
            final int revokedClaims = shard.revokeClaimsForOwner(expectedLease.ownerEpoch());
            List<PublishAttemptLedger> openAttempts = shard.listOpenPublishAttempts();
            int callbackPolls = 0;
            while (!openAttempts.isEmpty()) {
                ensureLeaseStillDraining(expectedLease, request, clock);
                if (callbackPolls >= request.maxCallbackPolls()) {
                    throw new IllegalStateException("owner drain callback quiescence budget exhausted");
                }
                callbacks.pollOpenPublishAttempts(openAttempts, callbackPolls);
                callbackPolls++;
                openAttempts = shard.listOpenPublishAttempts();
            }
            ensureLeaseStillDraining(expectedLease, request, clock);
            store.flushAndSync();
            final SourcePosition persistedPosition = shard.lastAppliedSourcePosition();
            if (persistedPosition != null) {
                callbacks.commitSourceHint(persistedPosition);
                ensureLeaseStillDraining(expectedLease, request, clock);
            }
            Path finalCheckpoint = null;
            if (request.finalCheckpointPath() != null) {
                finalCheckpoint = request.finalCheckpointId() == null
                        ? store.createCheckpoint(request.finalCheckpointPath())
                        : store.createCheckpoint(request.finalCheckpointPath(), request.finalCheckpointId());
                // Checkpoint creation can include a long RocksDB file walk and
                // hard-link phase.  Revalidate the lease after that boundary
                // before closing the DB or attempting release; otherwise a
                // lease loss during checkpointing could make this owner act
                // on a newer owner’s state.
                ensureLeaseStillDraining(expectedLease, request, clock);
            }
            pendingRevokedClaims = revokedClaims;
            pendingCallbackPolls = callbackPolls;
            pendingFinalCheckpoint = finalCheckpoint;
            RuntimeException closeFailure = null;
            try {
                store.close();
                storeClosed = true;
            } catch (RuntimeException failure) {
                closeFailure = failure;
            }
            if (closeFailure != null) {
                // A failed close does not prove that the old DB stopped
                // owning its files.  Keep the authoritative lease in
                // DRAINING for a visible retry; releasing it here could let a
                // new owner open the same shard while this DB is still live.
                throw closeFailure;
            }
            // A drain callback may renew the same lease while it waits for
            // an in-flight attempt.  Release the exact current lease
            // value, not the acquisition-time snapshot, so a backend that
            // includes expiry in its CAS cannot reject a valid renewal.
            releaseExactLease(ownedShard.lease());
            leaseReleased = true;
            return new DrainResult(revokedClaims, callbackPolls, finalCheckpoint);
        } finally {
            // A successfully closed Store is not enough to make the local
            // owner terminal: if lease release was not confirmed, the exact
            // DRAINING state must remain retryable.  Fencing here would make
            // the retry branch reject the still-held lease and leak it in
            // Oxia.  A confirmed release (or an earlier lease-loss check)
            // is what permits the terminal local fence.
            if (storeClosed && leaseReleased) {
                ownedShard.fence();
            }
            if (shardDrainAcquired) {
                ownedShard.releaseDrainAttempt();
            }
            resources.releaseDrainSlot();
        }
    }

    private void ensureLeaseStillDraining(final OwnerLease expected, final DrainRequest request,
                                          final LongSupplier clock) {
        final long now = readNow(clock);
        if (now >= request.deadlineEpochMs()) {
            throw new IllegalStateException("owner drain deadline expired");
        }
        final Optional<OwnerLease> current = authority.current(expected.shardId());
        if (current.isEmpty() || !expected.sameIdentity(current.orElseThrow())
                || current.orElseThrow().state() != ShardLifecycleState.DRAINING) {
            ownedShard.fence();
            throw new IllegalStateException("owner lease changed during drain");
        }
        final OwnerLease observed = current.orElseThrow();
        if (observed.expiresAtEpochMs() < ownedShard.lease().expiresAtEpochMs()
                || !observed.validAt(now)) {
            ownedShard.fence();
            throw new IllegalStateException("owner lease expired during drain");
        }
        if (observed.expiresAtEpochMs() > ownedShard.lease().expiresAtEpochMs()) {
            ownedShard.updateLease(observed);
        }
    }

    private void releaseExactLease(final OwnerLease expected) {
        if (authority.release(expected)) {
            return;
        }
        final Optional<OwnerLease> current = authority.current(expected.shardId());
        if (current.isEmpty()) {
            // The release may have committed before its response was lost.
            return;
        }
        if (expected.sameIdentity(current.orElseThrow())) {
            throw new IllegalStateException("owner lease release was not confirmed");
        }
        throw new IllegalStateException("owner lease changed before release confirmation");
    }

    private static long readNow(final LongSupplier clock) {
        final long now = clock.getAsLong();
        if (now < 0) {
            throw new IllegalArgumentException("drain clock returned a negative time");
        }
        return now;
    }

    public record DrainRequest(long deadlineEpochMs, int maxCallbackPolls, Path finalCheckpointPath,
                               byte[] finalCheckpointId) {
        public DrainRequest(final long deadlineEpochMs, final int maxCallbackPolls,
                            final Path finalCheckpointPath) {
            this(deadlineEpochMs, maxCallbackPolls, finalCheckpointPath, null);
        }

        public DrainRequest {
            if (deadlineEpochMs < 0 || maxCallbackPolls < 0) {
                throw new IllegalArgumentException("invalid owner drain bounds");
            }
            if (finalCheckpointId != null) {
                Bytes.requireLength(finalCheckpointId, 16, "finalCheckpointId");
                boolean nonZero = false;
                for (byte value : finalCheckpointId) {
                    nonZero |= value != 0;
                }
                if (!nonZero) {
                    throw new IllegalArgumentException("finalCheckpointId must be non-zero");
                }
                if (finalCheckpointPath == null) {
                    throw new IllegalArgumentException("finalCheckpointId requires a checkpoint path");
                }
                finalCheckpointId = Bytes.copy(finalCheckpointId);
            }
        }

        @Override
        public byte[] finalCheckpointId() {
            return finalCheckpointId == null ? null : Bytes.copy(finalCheckpointId);
        }
    }

    @FunctionalInterface
    public interface DrainCallbacks {
        /** Stops source fetch, due Claim and new admission scheduling. */
        void stopSourceAndScheduling();

        /**
         * Advances external callback/evidence processing once. The supplied
         * list is a bounded immutable view and must not be treated as proof of
         * remote outcome success.
         */
        default void pollOpenPublishAttempts(final List<PublishAttemptLedger> openAttempts,
                                              final int pollNumber) {
            // No-op is useful for a caller that only expects the bounded
            // budget to fail closed while an external callback is pending.
        }

        /**
         * Commits an optional broker/source hint after the DB flush boundary.
         * The callback must never acknowledge a position greater than the
         * supplied durable shard position; it is a transport hint, not a
         * replacement for the RocksDB recovery cursor.
         */
        default void commitSourceHint(final SourcePosition persistedPosition) {
            // Source hint commits are optional and remain transport-owned.
        }
    }

    public record DrainResult(int revokedClaims, int callbackPolls, Path finalCheckpointPath) {
        public DrainResult {
            if (revokedClaims < 0 || callbackPolls < 0) {
                throw new IllegalArgumentException("drain result counters must be non-negative");
            }
        }
    }
}
