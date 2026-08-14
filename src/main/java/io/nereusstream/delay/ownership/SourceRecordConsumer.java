package io.nereusstream.delay.ownership;

import java.util.Objects;
import java.util.Optional;

/**
 * Broker-neutral source poll and acknowledgement boundary for one Worker.
 *
 * <p>The implementation owns the native cursor, but it must not advance that
 * cursor from {@link #poll()} or from a local Store callback.  The
 * acknowledgement callback is the only operation that may make a source
 * record eligible for cursor advancement, and it must report {@link
 * SourceAcknowledgement.Disposition#ACKED} only after the broker has durably
 * accepted the acknowledgement.  Kafka and Pulsar adapters can implement
 * this SPI without leaking their client types into the semantic core.</p>
 */
public interface SourceRecordConsumer extends AutoCloseable {
    /**
     * Polls at most one record.  An empty result means that this turn has no
     * record available; it is not permission to release an unacknowledged
     * record retained by the Worker loop.
     */
    Optional<PolledSourceRecord> poll();

    @Override
    default void close() {
        // A source with no native resource has nothing to close.
    }

    /** One exact source entry together with its broker-owned ACK operation. */
    record PolledSourceRecord(SourceReplayEntry entry, Acknowledger acknowledger) {
        public PolledSourceRecord {
            Objects.requireNonNull(entry, "entry");
            Objects.requireNonNull(acknowledger, "acknowledger");
        }

        /**
         * Rejects an entry substitution before invoking the native ACK.  The
         * Worker retains object identity for the polled record; a different
         * object is never allowed to reuse this ACK authority.
         */
        SourceAcknowledgement acknowledgement() {
            return (candidate, outcome) -> {
                if (candidate != entry) {
                    return SourceAcknowledgement.AcknowledgementResult.unknown(
                            new IllegalStateException("source acknowledgement entry identity changed"));
                }
                return Objects.requireNonNull(acknowledger.acknowledge(candidate, outcome),
                        "source acknowledgement result");
            };
        }
    }

    @FunctionalInterface
    interface Acknowledger {
        SourceAcknowledgement.AcknowledgementResult acknowledge(SourceReplayEntry entry,
                                                                 SourceReplayOutcome appliedOutcome);
    }
}
