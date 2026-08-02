package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.CheckpointUploadIntentV1;
import io.nereusstream.delay.protocol.CheckpointUploadStateV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;

import java.util.Objects;
import java.util.Optional;

/**
 * Deterministic local projection of the checkpoint upload-intent CAS.
 *
 * <p>The production protocol stores this value in Oxia and compares the
 * active Owner Lease/session, lineage head and catalog generation in the same
 * transaction.  This class only supplies the exact value/state transition
 * semantics for local tests and embedded orchestration; it does not upload,
 * publish, delete or attest an Object Store object.</p>
 */
public final class CheckpointUploadIntentStore {
    private CheckpointUploadIntentV1 current;

    /**
     * Creates a PENDING_UPLOAD intent.  A retry with byte-identical intent is
     * idempotent; a different value cannot replace the active intent.
     */
    public synchronized CheckpointUploadIntentV1 create(final CheckpointUploadIntentV1 pending) {
        Objects.requireNonNull(pending, "pending");
        requireState(pending, CheckpointUploadStateV1.PENDING_UPLOAD);
        if (current == null) {
            current = pending;
            return current;
        }
        if (current.equals(pending)) {
            return current;
        }
        throw new IllegalStateException("checkpoint upload intent CAS conflict");
    }

    /**
     * Atomically projects PENDING_UPLOAD to PUBLISHED after an exact expected
     * value match.  The resource identity is additionally checked by the
     * immutable intent codec.
     */
    public synchronized CheckpointUploadIntentV1 publish(final CheckpointUploadIntentV1 expectedPending,
                                                          final CheckpointResourceV1 resource) {
        requireExpectedPending(expectedPending);
        Objects.requireNonNull(resource, "resource");
        current = next(expectedPending, CheckpointUploadStateV1.PUBLISHED, resource, null);
        return current;
    }

    /**
     * Rereads the exact PUBLISHED successor after a publication response loss.
     * A different pending value, revision or resource identity is not treated
     * as the caller's response.
     */
    public synchronized Optional<CheckpointUploadIntentV1> currentPublishedFor(
            final CheckpointUploadIntentV1 expectedPending) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        if (current == null || current.state() != CheckpointUploadStateV1.PUBLISHED) {
            return Optional.empty();
        }
        final CheckpointUploadIntentV1 expectedPublished = next(expectedPending,
                CheckpointUploadStateV1.PUBLISHED, current.publishedManifest(), null);
        return current.equals(expectedPublished) ? Optional.of(current) : Optional.empty();
    }

    /**
     * Atomically competes for the PENDING_UPLOAD to REAPING transition.  The
     * evidence is retained in the value so a later reaper cannot treat a
     * deadline alone as delete authority.  The local projection enforces the
     * trusted-time lower bound; the caller must still prove owner abandonment
     * or lease loss through the external authority before invoking it.
     */
    public synchronized CheckpointUploadIntentV1 beginReaping(
            final CheckpointUploadIntentV1 expectedPending,
            final TrustedUtcIntervalEvidence evidence) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        Objects.requireNonNull(evidence, "evidence");
        evidence.requireEarliestAtLeast(expectedPending.uploadDeadlineEpochMs());
        if (current != null && current.state() == CheckpointUploadStateV1.REAPING) {
            final CheckpointUploadIntentV1 expectedReaping = next(expectedPending,
                    CheckpointUploadStateV1.REAPING, null, evidence);
            if (current.equals(expectedReaping)) {
                return current;
            }
            throw new IllegalStateException("checkpoint reaping successor does not match current state");
        }
        requireExpectedPending(expectedPending);
        current = next(expectedPending, CheckpointUploadStateV1.REAPING, null, evidence);
        return current;
    }

    /** Returns the current local projection, if an intent has been created. */
    public synchronized Optional<CheckpointUploadIntentV1> current() {
        return Optional.ofNullable(current);
    }

    private void requireExpectedPending(final CheckpointUploadIntentV1 expectedPending) {
        Objects.requireNonNull(expectedPending, "expectedPending");
        requireState(expectedPending, CheckpointUploadStateV1.PENDING_UPLOAD);
        if (current == null || !current.equals(expectedPending)) {
            throw new IllegalStateException("checkpoint upload intent expected value does not match current state");
        }
    }

    private static void requireState(final CheckpointUploadIntentV1 intent,
                                     final CheckpointUploadStateV1 state) {
        if (intent.state() != state) {
            throw new IllegalArgumentException("checkpoint upload intent must be " + state);
        }
    }

    private static CheckpointUploadIntentV1 next(final CheckpointUploadIntentV1 expected,
                                                  final CheckpointUploadStateV1 state,
                                                  final CheckpointResourceV1 resource,
                                                  final TrustedUtcIntervalEvidence evidence) {
        return new CheckpointUploadIntentV1(
                expected.shard(), expected.recoveryLineageId(), expected.checkpointId(), expected.owner(),
                expected.sourceStoreIncarnation(), expected.uploadToken(), expected.baseCatalogGeneration(),
                expected.parentCheckpointId(), expected.parentManifestSha256(), expected.objectStoreProfile(),
                expected.checkpointCreatedAt(), expected.uploadDeadlineEpochMs(), state,
                Math.addExact(expected.stateRevision(), 1), resource, evidence);
    }
}
