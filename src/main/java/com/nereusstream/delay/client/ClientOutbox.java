package com.nereusstream.delay.client;

import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;

/** Optional durable outbox hook; implementations must not reinterpret outcomes. */
public interface ClientOutbox extends AutoCloseable {
    default void start(final PreparedSubmission submission, final PhysicalEnqueueAttemptId attempt) {
        // Optional durable Start record.
    }

    default void finish(
            final PreparedSubmission submission,
            final PhysicalEnqueueAttemptId attempt,
            final SubmissionOutcomeMessage outcome) {
        // Optional durable Final record.
    }

    @Override
    default void close() {
        // Optional outbox resources.
    }
}
