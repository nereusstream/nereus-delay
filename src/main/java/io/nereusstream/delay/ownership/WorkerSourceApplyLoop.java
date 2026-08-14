package io.nereusstream.delay.ownership;

import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;

import java.security.PublicKey;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Worker-facing source vertical for one owned shard.
 *
 * <p>The loop adapts a native source consumer to the bounded source apply
 * coordinator.  There is exactly one native look-ahead record.  A record is
 * polled once, retained across queue rejection, Store failure and ACK
 * uncertainty, and is eligible for the next native poll only after a
 * successful ACK and cursor advance.  The class intentionally does not
 * create a Kafka/Pulsar client; those adapters supply {@link
 * SourceRecordConsumer} and therefore remain independently testable.</p>
 */
public final class WorkerSourceApplyLoop implements AutoCloseable {
    private final SourceRecordConsumer consumer;
    private final SourceApplyCoordinator coordinator;
    private boolean closed;

    public WorkerSourceApplyLoop(final SourceRecordConsumer consumer,
                                 final WorkClassExecutionRegistry workClasses,
                                 final OwnedDelayShard ownedShard,
                                 final OxiaOwnerLeaseStore authority,
                                 final PublicKey verificationKey) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.coordinator = new SourceApplyCoordinator(this.consumer, workClasses, ownedShard, authority,
                verificationKey);
    }

    /** Runs one bounded source apply/ACK turn. */
    public synchronized SourceApplyCoordinator.TurnResult runTurn(final SchedulerBudget workBudget,
                                                                    final LongSupplier ownerClock) {
        if (closed) {
            throw new IllegalStateException("Worker source apply loop is closed");
        }
        return coordinator.runTurn(workBudget, ownerClock);
    }

    /** Returns the exact source record retained across an uncertain boundary. */
    public synchronized Optional<SourceReplayEntry> pendingEntry() {
        return coordinator.pendingEntry();
    }

    /**
     * Closes the native source only after the coordinator has no pending
     * record.  Closing with an unproven ACK would discard the broker retry
     * authority and is therefore rejected.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        if (coordinator.pendingEntry().isPresent()) {
            throw new IllegalStateException("cannot close source loop with a pending source record");
        }
        closed = true;
        consumer.close();
    }
}
