package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;

import java.time.Clock;
import java.util.Objects;

/**
 * Tracks the local ownership horizon of one Object Store adapter instance.
 *
 * <p>The tracker is deliberately narrower than a provider quiescence
 * attestation.  It fences new local operations, keeps an operation active
 * until the complete adapter method (including streamed response bodies) has
 * returned, and retains a bounded uncertainty horizon after an ambiguous
 * failure.  A caller may bind the resulting observation into an external
 * provider attestation, but this class never claims that a remote provider has
 * stopped executing a request.</p>
 */
public final class ObjectStoreProviderOwnershipTracker {
    public static final long DEFAULT_MAXIMUM_PROVIDER_OWNERSHIP_LIFETIME_MS = 60_000;
    private static final int HASH_LENGTH = 32;
    private static final long NO_TIME = -1;

    private final Clock clock;
    private final long maximumProviderOwnershipLifetimeMs;
    private boolean acceptingNewOperations = true;
    private long activeOperationCount;
    private long uncertainUntilEpochMs;
    private long lastObservedEpochMs = NO_TIME;
    private long lastOperationClosedAtEpochMs = NO_TIME;
    private long operationSequence;

    public ObjectStoreProviderOwnershipTracker(final Clock clock,
                                               final long maximumProviderOwnershipLifetimeMs) {
        this.clock = Objects.requireNonNull(clock, "clock");
        if (maximumProviderOwnershipLifetimeMs < 0) {
            throw new IllegalArgumentException("maximum provider ownership lifetime must not be negative");
        }
        this.maximumProviderOwnershipLifetimeMs = maximumProviderOwnershipLifetimeMs;
    }

    /** Opens one complete local adapter operation before its first provider call. */
    public synchronized Operation begin() {
        if (!acceptingNewOperations) {
            throw new IllegalStateException("Object Store provider ownership is fenced");
        }
        final long startedAtEpochMs = now();
        activeOperationCount = Math.addExact(activeOperationCount, 1);
        operationSequence = Math.addExact(operationSequence, 1);
        return new Operation(this, startedAtEpochMs);
    }

    /** Permanently prevents this adapter generation from starting new operations. */
    public synchronized void beginQuiescence() {
        now();
        acceptingNewOperations = false;
    }

    /** Returns a point-in-time local observation for an external evidence issuer. */
    public synchronized Observation observe() {
        final long observedAtEpochMs = now();
        final byte[] digest = observationDigest(acceptingNewOperations, activeOperationCount,
                observedAtEpochMs, uncertainUntilEpochMs, lastOperationClosedAtEpochMs,
                maximumProviderOwnershipLifetimeMs, operationSequence);
        return new Observation(acceptingNewOperations, activeOperationCount,
                observedAtEpochMs, uncertainUntilEpochMs, lastOperationClosedAtEpochMs,
                maximumProviderOwnershipLifetimeMs, operationSequence, digest);
    }

    /**
     * Requires the local fence, operation drain and ambiguity horizon to be
     * closed.  The returned observation is still only local evidence.
     */
    public synchronized Observation requireLocallyQuiescent() {
        final Observation observation = observe();
        if (!observation.locallyQuiescent()) {
            throw new IllegalStateException("Object Store provider ownership is not locally quiescent: "
                    + observation);
        }
        return observation;
    }

    public long maximumProviderOwnershipLifetimeMs() {
        return maximumProviderOwnershipLifetimeMs;
    }

    private synchronized void complete(final Operation operation, final boolean uncertain) {
        if (operation.closed) {
            throw new IllegalStateException("Object Store provider ownership operation already closed");
        }
        if (activeOperationCount <= 0) {
            throw new IllegalStateException("Object Store provider ownership operation is not current");
        }
        final long closedAtEpochMs = now();
        activeOperationCount--;
        lastOperationClosedAtEpochMs = closedAtEpochMs;
        if (uncertain) {
            final long horizonBase = Math.max(operation.startedAtEpochMs, closedAtEpochMs);
            final long candidate;
            try {
                candidate = Math.addExact(horizonBase, maximumProviderOwnershipLifetimeMs);
            } catch (ArithmeticException overflow) {
                uncertainUntilEpochMs = Long.MAX_VALUE;
                operation.closed = true;
                return;
            }
            uncertainUntilEpochMs = Math.max(uncertainUntilEpochMs, candidate);
        }
        operation.closed = true;
    }

    private long now() {
        final long current = clock.millis();
        if (current < 0) {
            throw new IllegalStateException("Object Store provider ownership clock is negative");
        }
        if (lastObservedEpochMs != NO_TIME && current < lastObservedEpochMs) {
            throw new IllegalStateException("Object Store provider ownership clock moved backwards");
        }
        lastObservedEpochMs = current;
        return current;
    }

    private static byte[] observationDigest(final boolean acceptingNewOperations,
                                            final long activeOperationCount,
                                            final long observedAtEpochMs,
                                            final long uncertainUntilEpochMs,
                                            final long lastOperationClosedAtEpochMs,
                                            final long maximumProviderOwnershipLifetimeMs,
                                            final long operationSequence) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-object-store-provider-ownership-observation-v1\0"),
                Bytes.u8(acceptingNewOperations ? 1 : 0), Bytes.u64be(activeOperationCount),
                Bytes.u64be(observedAtEpochMs), Bytes.u64be(uncertainUntilEpochMs),
                Bytes.i64be(lastOperationClosedAtEpochMs), Bytes.u64be(maximumProviderOwnershipLifetimeMs),
                Bytes.u64be(operationSequence));
    }

    /** One adapter operation whose close state is explicit and one-shot. */
    public static final class Operation {
        private final ObjectStoreProviderOwnershipTracker tracker;
        private final long startedAtEpochMs;
        private boolean closed;

        private Operation(final ObjectStoreProviderOwnershipTracker tracker, final long startedAtEpochMs) {
            this.tracker = tracker;
            this.startedAtEpochMs = startedAtEpochMs;
        }

        /** Closes after a complete response and body-consumption path. */
        public void complete() {
            tracker.complete(this, false);
        }

        /** Closes locally but retains the configured provider uncertainty horizon. */
        public void uncertain() {
            tracker.complete(this, true);
        }
    }

    /** Immutable local observation; it is not a provider-side certificate. */
    public record Observation(boolean acceptingNewOperations,
                              long activeOperationCount,
                              long observedAtEpochMs,
                              long uncertainUntilEpochMs,
                              long lastOperationClosedAtEpochMs,
                              long maximumProviderOwnershipLifetimeMs,
                              long operationSequence,
                              byte[] observationDigest) {
        public Observation {
            if (activeOperationCount < 0 || observedAtEpochMs < 0 || uncertainUntilEpochMs < 0
                    || maximumProviderOwnershipLifetimeMs < 0 || operationSequence < 0
                    || lastOperationClosedAtEpochMs < NO_TIME) {
                throw new IllegalArgumentException("invalid Object Store provider ownership observation");
            }
            Bytes.requireLength(observationDigest, HASH_LENGTH, "observationDigest");
            observationDigest = Bytes.copy(observationDigest);
        }

        public boolean locallyQuiescent() {
            return !acceptingNewOperations && activeOperationCount == 0
                    && observedAtEpochMs >= uncertainUntilEpochMs;
        }

        @Override
        public byte[] observationDigest() {
            return Bytes.copy(observationDigest);
        }
    }
}
