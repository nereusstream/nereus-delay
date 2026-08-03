package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.RecoveryPinV1;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.store.RecoveryCatalogAuthority;
import io.nereusstream.delay.store.RecoveryFloor;

import java.util.Objects;
import java.util.Optional;

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
        RECOVERY_PIN_STATE_UNAVAILABLE,
        RECOVERY_PIN_PROTECTS_RESOURCE,
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
            if (!coversPosition(floor.appliedSourcePosition(), intentPosition)
                    || !coversPosition(floor.appliedSourcePosition(), confirmationPosition)) {
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
        final Decision pinDecision = recoveryPinDecision(intent, catalog);
        if (pinDecision != null) {
            return pinDecision;
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

    /**
     * A checkpoint object is not deletable while the exact checkpoint is held
     * by an active recovery pin.  A pin protects both its selected candidate
     * and the observed Floor checkpoint.  If the authority cannot read the
     * pin, this predicate fails closed rather than treating the absence of a
     * response as proof that no pin exists.
     */
    private static Decision recoveryPinDecision(final ResourceRetireIntentRecord intent,
                                                final RecoveryCatalogAuthority catalog) {
        if (intent.resourceKind() != ResourceKind.CHECKPOINT) {
            return null;
        }
        final Optional<RecoveryPinV1> active;
        try {
            active = Objects.requireNonNull(catalog.activeRecoveryPin(), "activeRecoveryPin");
        } catch (RuntimeException exception) {
            return Decision.RECOVERY_PIN_STATE_UNAVAILABLE;
        }
        if (active.isPresent() && checkpointIdentityMatchesPin(intent.resourceIdentity(), active.orElseThrow())) {
            return Decision.RECOVERY_PIN_PROTECTS_RESOURCE;
        }
        return null;
    }

    private static boolean checkpointIdentityMatchesPin(final byte[] encodedIdentity,
                                                        final RecoveryPinV1 pin) {
        try {
            final CanonicalProtobuf.Reader outerReader = new CanonicalProtobuf.Reader(encodedIdentity);
            final CanonicalProtobuf.Reader.Field outer = outerReader.next();
            if (outerReader.hasRemaining() || outer.number() != ResourceKind.CHECKPOINT.wireValue()
                    || outer.wireType() != 2) {
                return false;
            }
            final CanonicalProtobuf.Reader branchReader = new CanonicalProtobuf.Reader(outer.rawValue());
            byte[] checkpointId = null;
            byte[] manifestHash = null;
            while (branchReader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = branchReader.next();
                if (field.number() == 2 && field.wireType() == 2) {
                    checkpointId = field.rawValue();
                } else if (field.number() == 8 && field.wireType() == 2) {
                    manifestHash = field.rawValue();
                }
            }
            if (checkpointId == null || manifestHash == null) {
                return false;
            }
            return matches(checkpointId, manifestHash, pin.candidate().checkpointId(),
                    pin.candidate().manifestSha256())
                    || matches(checkpointId, manifestHash, pin.observedFloor().checkpointId(),
                    pin.observedFloor().manifestSha256());
        } catch (IllegalArgumentException exception) {
            // ResourceRetireIntentRecord already validates the identity.  If
            // a future identity version reaches this guard, fail closed.
            return false;
        }
    }

    private static boolean matches(final byte[] checkpointId, final byte[] manifestHash,
                                   final byte[] pinnedCheckpointId, final byte[] pinnedManifestHash) {
        return Bytes.constantTimeEquals(checkpointId, pinnedCheckpointId)
                && Bytes.constantTimeEquals(manifestHash, pinnedManifestHash);
    }

    private static boolean coversPosition(final SourcePosition covered, final SourcePosition required) {
        try {
            final int order = covered.compareTo(required);
            return order > 0 || (order == 0
                    && Bytes.constantTimeEquals(covered.canonicalBytes(), required.canonicalBytes()));
        } catch (IllegalArgumentException exception) {
            return false;
        }
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
