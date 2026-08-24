package com.nereusstream.delay.client;

import com.nereusstream.delay.protocol.PreparedSubmissionV1;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessageV1;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

/** Optional durable outbox hook; implementations must not reinterpret outcomes. */
public interface ClientOutbox extends AutoCloseable {
    default void start(final PreparedSubmissionV1 submission, final PhysicalEnqueueAttemptId attempt) {
        // Optional durable Start record.
    }

    default void finish(
            final PreparedSubmissionV1 submission,
            final PhysicalEnqueueAttemptId attempt,
            final SubmissionOutcomeMessageV1 outcome) {
        // Optional durable Final record.
    }

    @Override
    default void close() {
        // Optional outbox resources.
    }
}
