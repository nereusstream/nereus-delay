package io.nereusstream.delay.transport;

import io.nereusstream.delay.semantic.TrustedClock;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Ownership permit derived from a successful durable Gateway attempt CAS. */
public final class GatewayAttemptOwnershipPermit implements TransportOwnershipPermit {
    private final PhysicalEnqueueAttemptId physicalAttemptId;
    private final long recordRevision;
    private final long ownershipNotAfterEpochMs;
    private final TrustedClock trustedClock;
    private final AtomicReference<TransportOwnershipState> state =
            new AtomicReference<>(TransportOwnershipState.AVAILABLE);

    public GatewayAttemptOwnershipPermit(final PhysicalEnqueueAttemptId physicalAttemptId,
                                         final long recordRevision, final long ownershipNotAfterEpochMs,
                                         final TrustedClock trustedClock) {
        this.physicalAttemptId = Objects.requireNonNull(physicalAttemptId, "physicalAttemptId");
        if (recordRevision <= 0 || ownershipNotAfterEpochMs < 0) {
            throw new IllegalArgumentException("invalid Gateway ownership permit bounds");
        }
        this.recordRevision = recordRevision;
        this.ownershipNotAfterEpochMs = ownershipNotAfterEpochMs;
        this.trustedClock = Objects.requireNonNull(trustedClock, "trustedClock");
    }

    public long recordRevision() {
        return recordRevision;
    }

    public long ownershipNotAfterEpochMs() {
        return ownershipNotAfterEpochMs;
    }

    @Override
    public PhysicalEnqueueAttemptId physicalAttemptId() {
        return physicalAttemptId;
    }

    @Override
    public boolean tryTransferToLibraryOwnership() {
        final long now;
        try {
            now = trustedClock.nowEpochMs();
        } catch (RuntimeException failure) {
            return false;
        }
        return now <= ownershipNotAfterEpochMs
                && state.compareAndSet(TransportOwnershipState.AVAILABLE, TransportOwnershipState.LIBRARY_OWNED);
    }

    @Override
    public TransportOwnershipState state() {
        return state.get();
    }

    @Override
    public void close() {
        state.compareAndSet(TransportOwnershipState.AVAILABLE, TransportOwnershipState.INVALID);
    }
}
