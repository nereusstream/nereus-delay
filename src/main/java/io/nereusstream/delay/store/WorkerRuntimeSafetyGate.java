package io.nereusstream.delay.store;

import java.util.Objects;

/**
 * Explicit runtime safety state for a Worker shared-resource failure domain.
 *
 * <p>The gate is intentionally driven by an authoritative runtime probe.  A
 * failed observation is sticky: once the certified envelope no longer fits,
 * the Worker must drain or migrate instead of automatically reopening work
 * when one later observation happens to look healthy.  This keeps a hot
 * cgroup/FD/disk shrink from becoming a replay-dependent business result.</p>
 *
 * <p>This class does not perform the probe, drain shards, or acquire Oxia
 * placement authority.  It is the local state/fencing seam shared resources
 * and admission callers can use after those external layers publish an
 * observation.</p>
 */
public final class WorkerRuntimeSafetyGate {
    public enum State {
        ACTIVE,
        STAGED,
        DRAIN_OR_MIGRATE
    }

    private final ShardStoreConfig config;
    private WorkerResourceEnvelope certifiedEnvelope;
    private WorkerResourceEnvelope stagedEnvelope;
    private WorkerRuntimeResourceObservation lastObservation;
    private State state = State.ACTIVE;
    private String failureReason;

    public WorkerRuntimeSafetyGate(final ShardStoreConfig config,
                                   final WorkerResourceEnvelope certifiedEnvelope) {
        this(config, certifiedEnvelope, null);
    }

    public WorkerRuntimeSafetyGate(final ShardStoreConfig config,
                                   final WorkerResourceEnvelope certifiedEnvelope,
                                   final WorkerRuntimeResourceObservation initialObservation) {
        this.config = Objects.requireNonNull(config, "config");
        this.certifiedEnvelope = Objects.requireNonNull(certifiedEnvelope, "certifiedEnvelope");
        if (initialObservation == null) {
            certifiedEnvelope.validate(config);
        } else {
            certifiedEnvelope.validate(config, initialObservation);
            lastObservation = initialObservation;
        }
    }

    /**
     * Revalidates the certified envelope against a fresh runtime observation.
     * A mismatch transitions the gate to the sticky drain state and rejects
     * the observation.
     */
    public synchronized void observe(final WorkerRuntimeResourceObservation observation) {
        Objects.requireNonNull(observation, "observation");
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Worker runtime safety gate is already " + state);
        }
        try {
            certifiedEnvelope.validate(config, observation);
        } catch (IllegalArgumentException failure) {
            lastObservation = observation;
            failureReason = failure.getMessage();
            state = State.DRAIN_OR_MIGRATE;
            throw new IllegalStateException("Worker runtime resource envelope no longer fits", failure);
        }
        lastObservation = observation;
    }

    /**
     * Records a probe failure as a shared safety breach.  A missing,
     * malformed, or temporarily unreadable platform limit is not treated as
     * an unlimited resource and cannot leave the Worker accepting work.
     */
    public synchronized void rejectProbeFailure(final Throwable failure) {
        Objects.requireNonNull(failure, "failure");
        if (state != State.DRAIN_OR_MIGRATE) {
            failureReason = boundedFailureMessage(failure);
            state = State.DRAIN_OR_MIGRATE;
        }
    }

    /**
     * Stages a new certified envelope.  New ownership is fenced immediately;
     * the staged envelope becomes active only after all old DB/ownership and
     * transition resources have drained.
     */
    public synchronized void stage(final WorkerResourceEnvelope nextEnvelope,
                                   final WorkerRuntimeResourceObservation observation) {
        Objects.requireNonNull(nextEnvelope, "nextEnvelope");
        Objects.requireNonNull(observation, "observation");
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Worker runtime safety gate is already " + state);
        }
        nextEnvelope.validate(config, observation);
        stagedEnvelope = nextEnvelope;
        lastObservation = observation;
        state = State.STAGED;
        failureReason = null;
    }

    /** Moves a staged or currently unsafe Worker into the drain/migrate state. */
    public synchronized void beginDrainOrMigrate() {
        if (state == State.ACTIVE || state == State.STAGED) {
            state = State.DRAIN_OR_MIGRATE;
        }
    }

    /**
     * Activates the staged envelope only after the old physical boundary is
     * empty.  A runtime observation is required again at the activation edge.
     */
    public synchronized void activateAfterDrain(final long ownedShardDbs,
                                                 final long openShardDbs,
                                                 final boolean transitionInFlight,
                                                 final WorkerRuntimeResourceObservation observation) {
        Objects.requireNonNull(observation, "observation");
        if (ownedShardDbs < 0 || openShardDbs < 0) {
            throw new IllegalArgumentException("drain occupancy cannot be negative");
        }
        if (state == State.ACTIVE) {
            throw new IllegalStateException("Worker runtime safety gate is already ACTIVE");
        }
        if (state != State.DRAIN_OR_MIGRATE) {
            throw new IllegalStateException("Worker runtime safety gate must enter DRAIN_OR_MIGRATE before activation");
        }
        if (ownedShardDbs != 0 || openShardDbs != 0 || transitionInFlight) {
            throw new IllegalStateException("cannot activate a Worker envelope before drain completes");
        }
        final WorkerResourceEnvelope candidate = stagedEnvelope == null ? certifiedEnvelope : stagedEnvelope;
        candidate.validate(config, observation);
        certifiedEnvelope = candidate;
        stagedEnvelope = null;
        lastObservation = observation;
        failureReason = null;
        state = State.ACTIVE;
    }

    /** Fences ownership, restore and Claim/Admission callers while not active. */
    public synchronized void requireActive(final String operation) {
        Objects.requireNonNull(operation, "operation");
        if (state != State.ACTIVE) {
            throw new IllegalStateException("Worker runtime safety gate " + state + " blocks " + operation
                    + (failureReason == null ? "" : ": " + failureReason));
        }
    }

    public synchronized State state() {
        return state;
    }

    public synchronized WorkerRuntimeResourceObservation lastObservation() {
        return lastObservation;
    }

    public synchronized byte[] certifiedEnvelopeDigest() {
        return certifiedEnvelope.digest();
    }

    public synchronized byte[] stagedEnvelopeDigest() {
        return stagedEnvelope == null ? null : stagedEnvelope.digest();
    }

    public synchronized String failureReason() {
        return failureReason;
    }

    private static String boundedFailureMessage(final Throwable failure) {
        final String message = failure.getMessage();
        final String value = failure.getClass().getSimpleName()
                + (message == null || message.isBlank() ? "" : ": " + message);
        return value.length() <= 256 ? value : value.substring(0, 256);
    }
}
