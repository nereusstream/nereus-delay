package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Exact predecessor requests for Source-ordered certification generations; these are not ACTIVE decisions. */
public final class TargetSloObservationControlRequest implements ControlOperationRequestBranch {
    public static final int ENABLE_CONTROL_KIND = 28;
    public static final int STOP_CONTROL_KIND = 29;
    public static final int MAX_V1_CANONICAL_BYTES =
            TargetSloObservationPolicy.MAX_V1_CANONICAL_BYTES + TargetQuotaScope.MAX_CANONICAL_BYTES + 256;
    public static final int MAX_CANONICAL_BYTES =
            TargetSloObservationPolicy.MAX_CANONICAL_BYTES + TargetQuotaScope.MAX_CANONICAL_BYTES + 256;

    private final ControlOperationKind operation;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final long generation;
    private final byte[] expectedPriorDigest;
    private final TargetSloObservationPolicy policy;
    private final ControlReason reason;

    private TargetSloObservationControlRequest(
            ControlOperationKind operation,
            TargetQuotaScope scope,
            byte[] lineage,
            long generation,
            byte[] expectedPriorDigest,
            TargetSloObservationPolicy policy,
            ControlReason reason) {
        this.operation = Objects.requireNonNull(operation);
        this.scope = Objects.requireNonNull(scope);
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "Target SLO Control lineage");
        this.generation = generation;
        this.expectedPriorDigest = expectedPriorDigest == null
                ? null
                : TargetCompatibilityCodec.assigned(expectedPriorDigest, 32, "Target SLO exact predecessor");
        this.policy = policy;
        this.reason = reason;
        if (scope.target() != null || generation == 0) {
            throw new IllegalArgumentException(
                    "Target SLO Control requires original Shard scope and nonzero observation generation");
        }
        switch (operation) {
            case ENABLE_TARGET_SLO_CERTIFICATION -> {
                if (policy == null
                        || reason != null
                        || !scope.equals(policy.scope())
                        || !Arrays.equals(this.lineage, policy.lineage())
                        || (generation == 1) != (expectedPriorDigest == null)) {
                    throw new IllegalArgumentException(
                            "Target SLO enable must freeze policy and exact preceding stopped generation");
                }
            }
            case STOP_TARGET_SLO_CERTIFICATION -> {
                if (policy != null || reason == null || expectedPriorDigest == null) {
                    throw new IllegalArgumentException("Target SLO stop must reference its exact current generation");
                }
            }
            default -> throw new IllegalArgumentException("not a Target certification lifecycle operation");
        }
    }

    public static TargetSloObservationControlRequest enable(
            TargetSloObservationPolicy policy, long generation, byte[] expectedPriorStoppedDigest) {
        Objects.requireNonNull(policy);
        return new TargetSloObservationControlRequest(
                ControlOperationKind.ENABLE_TARGET_SLO_CERTIFICATION,
                policy.scope(),
                policy.lineage(),
                generation,
                expectedPriorStoppedDigest,
                policy,
                null);
    }

    public static TargetSloObservationControlRequest stop(
            TargetQuotaScope scope,
            byte[] lineage,
            long generation,
            byte[] expectedCurrentDigest,
            ControlReason reason) {
        return new TargetSloObservationControlRequest(
                ControlOperationKind.STOP_TARGET_SLO_CERTIFICATION,
                scope,
                lineage,
                generation,
                Objects.requireNonNull(expectedCurrentDigest),
                null,
                Objects.requireNonNull(reason));
    }

    public ControlOperationKind operationKind() {
        return operation;
    }

    public int controlKind() {
        return operation == ControlOperationKind.ENABLE_TARGET_SLO_CERTIFICATION
                ? ENABLE_CONTROL_KIND
                : STOP_CONTROL_KIND;
    }

    public TargetQuotaScope scope() {
        return scope;
    }

    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    public long generation() {
        return generation;
    }

    public byte[] expectedPriorDigest() {
        return expectedPriorDigest == null ? null : Bytes.copy(expectedPriorDigest);
    }

    public TargetSloObservationPolicy policy() {
        return policy;
    }

    public ControlReason reason() {
        return reason;
    }

    public ControlOperationRequest operationRequest() {
        return new ControlOperationRequest(operation, this);
    }

    @Override
    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, lineage);
            CanonicalProtobuf.uint64Bits(out, 4, generation);
            if (expectedPriorDigest != null) {
                CanonicalProtobuf.bytes(out, 5, expectedPriorDigest);
            }
            if (policy != null) {
                CanonicalProtobuf.bytes(out, 6, policy.canonicalBytes());
            }
            if (reason != null) {
                CanonicalProtobuf.bytes(out, 7, reason.canonicalBytes());
            }
        });
        if (encoded.length
                > (policy != null && policy.requiresWriterEpoch() ? MAX_CANONICAL_BYTES : MAX_V1_CANONICAL_BYTES)) {
            throw new IllegalArgumentException("Target SLO Control request exceeds bound");
        }
        return encoded;
    }

    public static TargetSloObservationControlRequest decode(ControlOperationKind operation, byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Target SLO Control request");
        if (f.size() < 5 || QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown Target SLO Control schema");
        }
        final boolean enable = operation == ControlOperationKind.ENABLE_TARGET_SLO_CERTIFICATION;
        if (!enable && operation != ControlOperationKind.STOP_TARGET_SLO_CERTIFICATION) {
            throw new IllegalArgumentException("not a Target SLO Control kind");
        }
        final boolean prior = f.get(4).number() == 5;
        QueryCodecSupport.requireNumbers(
                f,
                enable
                        ? prior ? new int[] {1, 2, 3, 4, 5, 6} : new int[] {1, 2, 3, 4, 6}
                        : new int[] {1, 2, 3, 4, 5, 7},
                "Target SLO Control request");
        final var reason = enable ? null : QueryCodecSupport.bytes(f.getLast(), 7);
        if (reason != null && reason.length > 80) {
            throw new IllegalArgumentException("Target SLO Control reason exceeds bound");
        }
        final var result = new TargetSloObservationControlRequest(
                operation,
                TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 16),
                QueryCodecSupport.uint64Bits(f.get(3), 4),
                prior ? QueryCodecSupport.fixed(f.get(4), 5, 32) : null,
                enable ? TargetSloObservationPolicy.decode(QueryCodecSupport.bytes(f.getLast(), 6)) : null,
                enable ? null : ControlReason.decode(reason));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO Control request");
        return result;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetSloObservationControlRequest that
                && operation == that.operation
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
