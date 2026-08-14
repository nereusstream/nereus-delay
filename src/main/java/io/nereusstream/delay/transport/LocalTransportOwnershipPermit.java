package io.nereusstream.delay.transport;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** Direct SDK ownership permit with no serializable or remotely transferable state. */
public final class LocalTransportOwnershipPermit implements TransportOwnershipPermit {
    private final PhysicalEnqueueAttemptId physicalAttemptId;
    private final AtomicReference<TransportOwnershipState> state =
            new AtomicReference<>(TransportOwnershipState.AVAILABLE);

    public LocalTransportOwnershipPermit(final PhysicalEnqueueAttemptId physicalAttemptId) {
        this.physicalAttemptId = Objects.requireNonNull(physicalAttemptId, "physicalAttemptId");
    }

    @Override
    public PhysicalEnqueueAttemptId physicalAttemptId() {
        return physicalAttemptId;
    }

    @Override
    public boolean tryTransferToLibraryOwnership() {
        return state.compareAndSet(TransportOwnershipState.AVAILABLE, TransportOwnershipState.LIBRARY_OWNED);
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
