package com.nereusstream.delay.ownership;

import java.util.Objects;

/**
 * Broker-owned acknowledgement boundary for one already applied Shard Log
 * record.
 *
 * <p>An implementation must not report {@link Disposition#ACKED} until the
 * broker cursor/ack operation is durably accepted. A thrown exception or a
 * response whose outcome cannot be proved is represented as
 * {@link Disposition#UNKNOWN}; the source record remains the retry authority
 * in either non-ACKED branch.</p>
 */
@FunctionalInterface
public interface SourceAcknowledgement {
    AcknowledgementResult acknowledge(SourceReplayEntry entry, SourceReplayOutcome appliedOutcome);

    enum Disposition {
        ACKED,
        DEFINITIVELY_NOT_ACKED,
        UNKNOWN
    }

    record AcknowledgementResult(Disposition disposition, Throwable failure) {
        public AcknowledgementResult {
            Objects.requireNonNull(disposition, "disposition");
            if (disposition == Disposition.ACKED && failure != null) {
                throw new IllegalArgumentException("ACKED cannot carry failure evidence");
            }
        }

        public static AcknowledgementResult acked() {
            return new AcknowledgementResult(Disposition.ACKED, null);
        }

        public static AcknowledgementResult definitelyNotAcked(final Throwable failure) {
            return new AcknowledgementResult(
                    Disposition.DEFINITIVELY_NOT_ACKED, Objects.requireNonNull(failure, "failure"));
        }

        public static AcknowledgementResult unknown(final Throwable failure) {
            return new AcknowledgementResult(Disposition.UNKNOWN, failure);
        }
    }
}
