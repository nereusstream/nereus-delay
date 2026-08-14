package io.nereusstream.delay.submission;

import java.util.Objects;

/** Lookup for the one closed outcome projector matching a prepared branch. */
public interface SubmissionOutcomeProjectorRegistry {
    SubmissionOutcomeProjector exact(SubmissionProjectionKey key);

    static SubmissionOutcomeProjectorRegistry of(final SubmissionOutcomeProjector... projectors) {
        final java.util.Map<SubmissionProjectionKey, SubmissionOutcomeProjector> values = new java.util.HashMap<>();
        for (SubmissionOutcomeProjector projector : projectors) {
            Objects.requireNonNull(projector, "projector");
            if (values.putIfAbsent(projector.key(), projector) != null) {
                throw new IllegalArgumentException("duplicate submission projector key");
            }
        }
        return key -> values.get(key);
    }
}
