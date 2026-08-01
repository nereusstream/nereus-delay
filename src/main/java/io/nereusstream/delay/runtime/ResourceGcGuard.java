package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.store.RecoveryCatalogAuthority;
import io.nereusstream.delay.store.RecoveryFloor;

import java.util.Objects;

/**
 * Read-only necessary-condition check for compacting a retained GC tombstone.
 * The overload accepting a {@link RecoveryCatalogAuthority} also checks local
 * parent-hash ancestry; Oxia CAS and provider ownership remain outside this
 * predicate.
 */
public final class ResourceGcGuard {
    private ResourceGcGuard() {
    }

    public enum Decision {
        NO_RETIRE_INTENT,
        DELETE_NOT_CONFIRMED,
        INTENT_REFERENCE_MISMATCH,
        LEGACY_MUTATION_SEQUENCE_UNAVAILABLE,
        FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING,
        SOURCE_AND_SEQUENCE_COVERED
    }

    public static Decision evaluate(final ResourceRetireIntentRecord intent,
                                    final ResourceDeleteConfirmedRecord confirmation,
                                    final RecoveryFloor floor) {
        if (intent == null) {
            return Decision.NO_RETIRE_INTENT;
        }
        if (confirmation == null) {
            return Decision.DELETE_NOT_CONFIRMED;
        }
        if (!sameIntent(intent, confirmation.retireIntent())) {
            return Decision.INTENT_REFERENCE_MISMATCH;
        }
        if (intent.appliedMutationSequence() <= 0 || confirmation.appliedMutationSequence() <= 0) {
            return Decision.LEGACY_MUTATION_SEQUENCE_UNAVAILABLE;
        }
        if (floor == null) {
            return Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING;
        }
        final SourcePosition intentPosition;
        final SourcePosition confirmationPosition;
        try {
            intentPosition = SourcePositionCodec.decode(intent.appliedSourcePosition());
            confirmationPosition = SourcePositionCodec.decode(confirmation.appliedSourcePosition());
            if (floor.appliedSourcePosition().compareTo(intentPosition) < 0
                    || floor.appliedSourcePosition().compareTo(confirmationPosition) < 0) {
                return Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING;
            }
        } catch (IllegalArgumentException exception) {
            return Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING;
        }
        final long requiredSequence = Math.max(intent.appliedMutationSequence(),
                confirmation.appliedMutationSequence());
        return floor.includedMutationSequence() >= requiredSequence
                ? Decision.SOURCE_AND_SEQUENCE_COVERED
                : Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING;
    }

    /**
     * Evaluates the same necessary predicate against a catalog-backed
     * candidate.  In addition to the Floor scalar checks this verifies the
     * exact parent-hash ancestry through {@link RecoveryCatalogAuthority}; it remains a
     * local proof and does not replace Oxia/provider authorization.
     */
    public static Decision evaluate(final ResourceRetireIntentRecord intent,
                                    final ResourceDeleteConfirmedRecord confirmation,
                                    final RecoveryCatalogAuthority catalog,
                                    final byte[] candidateCheckpointId) {
        Objects.requireNonNull(catalog, "catalog");
        final Decision scalar = evaluate(intent, confirmation, catalog.currentFloor().orElse(null));
        if (scalar != Decision.SOURCE_AND_SEQUENCE_COVERED) {
            return scalar;
        }
        final SourcePosition intentPosition;
        final SourcePosition confirmationPosition;
        try {
            intentPosition = SourcePositionCodec.decode(intent.appliedSourcePosition());
            confirmationPosition = SourcePositionCodec.decode(confirmation.appliedSourcePosition());
        } catch (IllegalArgumentException exception) {
            return Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING;
        }
        final long requiredSequence = Math.max(intent.appliedMutationSequence(),
                confirmation.appliedMutationSequence());
        return catalog.proveFloorCoverage(candidateCheckpointId, requiredSequence, intentPosition,
                confirmationPosition).isPresent()
                ? Decision.SOURCE_AND_SEQUENCE_COVERED
                : Decision.FLOOR_SOURCE_OR_SEQUENCE_NOT_COVERING;
    }

    private static boolean sameIntent(final ResourceRetireIntentRecord left,
                                      final ResourceRetireIntentRecord right) {
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
        return Bytes.constantTimeEquals(left.mutationId(), right.mutationId())
                && Bytes.constantTimeEquals(left.mutationHash(), right.mutationHash())
                && left.resourceKind() == right.resourceKind()
                && Bytes.constantTimeEquals(left.resourceIdentityHash(), right.resourceIdentityHash())
                && left.expectedResourceStateVersion() == right.expectedResourceStateVersion();
    }
}
