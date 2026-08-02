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
        boolean storeClosed = false;
        try {
            final long startNow = readNow(clock);
            final OwnerLease expectedLease = ownedShard.lease();
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
            RuntimeException closeFailure = null;
            try {
                store.close();
                storeClosed = true;
            } catch (RuntimeException failure) {
                storeClosed = true;
                closeFailure = failure;
            }
            try {
                // A drain callback may renew the same lease while it waits for
                // an in-flight attempt.  Release the exact current lease
                // value, not the acquisition-time snapshot, so a backend that
                // includes expiry in its CAS cannot reject a valid renewal.
                releaseExactLease(ownedShard.lease());
            } finally {
                ownedShard.fence();
            }
            if (closeFailure != null) {
                throw closeFailure;
            }
            return new DrainResult(revokedClaims, callbackPolls, finalCheckpoint);
        } finally {
            if (storeClosed) {
                ownedShard.fence();
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
