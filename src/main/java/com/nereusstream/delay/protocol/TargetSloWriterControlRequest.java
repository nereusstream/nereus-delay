package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Exact original Control requests for writer epochs, independent of observation generations. */
public final class TargetSloWriterControlRequest implements ControlOperationRequestBranch {
    public static final int REGISTER_CONTROL_KIND = 30;
    public static final int CLOSE_CONTROL_KIND = 31;
    private static final int CONTROL_ENVELOPE_OVERHEAD = TargetSloObservationGeneration.Control.MAX_INPUT_BYTES
            - TargetSloObservationControlBody.MAX_CANONICAL_BYTES;
    public static final int MAX_FENCE_INPUT_BYTES = TargetTimeFenceBody.MAX_CANONICAL_BYTES
            + TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES
            + CONTROL_ENVELOPE_OVERHEAD;
    public static final int MAX_CANONICAL_BYTES = 2
            + (6 + TargetQuotaScope.MAX_CANONICAL_BYTES)
            + 18
            + Math.max(
                    6 + TargetSloWriterContract.MAX_CANONICAL_BYTES + 34,
                    6
                            + TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES
                            + 6
                            + MAX_FENCE_INPUT_BYTES
                            + 6
                            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES);
    private final ControlOperationKind operation;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final TargetSloWriterContract contract;
    private final TargetSloWriterContract.Reference reference;
    private final byte[] expectedPriorDigest;
    private final SystemMutation fence;
    private final TargetQuotaMutation fenceAt;

    private TargetSloWriterControlRequest(
            ControlOperationKind operation,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloWriterContract contract,
            TargetSloWriterContract.Reference reference,
            byte[] expectedPriorDigest,
            SystemMutation fence,
            TargetQuotaMutation fenceAt) {
        this.operation = Objects.requireNonNull(operation);
        this.scope = Objects.requireNonNull(scope);
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "SLO writer Control lineage");
        this.contract = contract;
        this.reference = reference;
        this.expectedPriorDigest = expectedPriorDigest == null
                ? null
                : TargetCompatibilityCodec.assigned(expectedPriorDigest, 32, "original preceding writer contract");
        this.fence = fence;
        this.fenceAt = fenceAt;
        if (scope.target() != null) {
            throw new IllegalArgumentException("SLO writer Control requires its original Shard scope");
        }
        switch (operation) {
            case REGISTER_TARGET_SLO_WRITER_EPOCH -> {
                if (contract == null
                        || reference != null
                        || fence != null
                        || fenceAt != null
                        || !scope.equals(contract.scope())
                        || !Arrays.equals(this.lineage, contract.lineage())
                        || (contract.epoch() == 1) != (expectedPriorDigest == null)) {
                    throw new IllegalArgumentException(
                            "writer REGISTER requires its exact declaration and preceding epoch digest");
                }
            }
            case CLOSE_TARGET_SLO_WRITER_EPOCH -> {
                if (contract != null
                        || reference == null
                        || expectedPriorDigest != null
                        || fence == null
                        || fenceAt == null) {
                    throw new IllegalArgumentException(
                            "writer CLOSE requires the original applied reference and actual TIME_FENCE");
                }
                fenceAt.requireSourceApplied();
                if (fence.type() != SystemMutationType.TIME_FENCE
                        || !scope.shard().equals(fence.shardId())
                        || !scope.shard().equals(fenceAt.source().shardId())
                        || fence.canonicalEnvelope().length > MAX_FENCE_INPUT_BYTES
                        || !Arrays.equals(Bytes.sha256(fence.canonicalEnvelope()), fenceAt.mutationDigest())) {
                    throw new IllegalArgumentException("writer CLOSE changed exact original TIME_FENCE input/stamp");
                }
                final var body = TargetTimeFenceBody.decode(fence.canonicalBody());
                final var author = AuthorIdentity.decode(fence.authorIdentity());
                if (!body.shard().equals(scope.shard())
                        || body.retryUntil() != fence.retryUntilEpochMs()
                        || !Arrays.equals(body.proofId(), fence.logicalOperationIdentity())
                        || author.kind() != AuthorIdentity.Kind.FENCE
                        || author.first().length > TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES) {
                    throw new IllegalArgumentException("writer CLOSE changed original fence protocol/identity bounds");
                }
            }
            default -> throw new IllegalArgumentException("not a Target SLO writer Control operation");
        }
    }

    public static TargetSloWriterControlRequest register(
            TargetSloWriterContract contract, byte[] expectedPriorContractDigest) {
        final var exact = Objects.requireNonNull(contract);
        return new TargetSloWriterControlRequest(
                ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH,
                exact.scope(),
                exact.lineage(),
                exact,
                null,
                expectedPriorContractDigest,
                null,
                null);
    }

    public static TargetSloWriterControlRequest close(
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloWriterContract.Reference reference,
            SystemMutation fence,
            TargetQuotaMutation fenceAt) {
        return new TargetSloWriterControlRequest(
                ControlOperationKind.CLOSE_TARGET_SLO_WRITER_EPOCH,
                scope,
                lineage,
                null,
                reference,
                null,
                fence,
                fenceAt);
    }

    public ControlOperationKind operationKind() {
        return operation;
    }

    public int controlKind() {
        return operation == ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH
                ? REGISTER_CONTROL_KIND
                : CLOSE_CONTROL_KIND;
    }

    public TargetQuotaScope scope() {
        return scope;
    }

    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    public TargetSloWriterContract contract() {
        return contract;
    }

    public TargetSloWriterContract.Reference reference() {
        return reference;
    }

    public byte[] expectedPriorDigest() {
        return expectedPriorDigest == null ? null : Bytes.copy(expectedPriorDigest);
    }

    public SystemMutation fence() {
        return fence;
    }

    public TargetQuotaMutation fenceAt() {
        return fenceAt;
    }

    public long epoch() {
        return contract == null ? reference.epoch() : contract.epoch();
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
            if (contract != null) {
                CanonicalProtobuf.bytes(out, 4, contract.canonicalBytes());
                if (expectedPriorDigest != null) {
                    CanonicalProtobuf.bytes(out, 5, expectedPriorDigest);
                }
            } else {
                CanonicalProtobuf.bytes(out, 6, reference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 7, fence.canonicalEnvelope());
                CanonicalProtobuf.bytes(out, 8, fenceAt.canonicalBytes());
            }
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("SLO writer Control request exceeds its closed codec bound");
        }
        return encoded;
    }

    public static TargetSloWriterControlRequest decode(ControlOperationKind operation, byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 6, false, "Target SLO writer Control request");
        final boolean register = operation == ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH;
        if (!register && operation != ControlOperationKind.CLOSE_TARGET_SLO_WRITER_EPOCH) {
            throw new IllegalArgumentException("not a SLO writer Control operation");
        }
        QueryCodecSupport.requireNumbers(
                f,
                register
                        ? (f.size() == 5 ? new int[] {1, 2, 3, 4, 5} : new int[] {1, 2, 3, 4})
                        : new int[] {1, 2, 3, 6, 7, 8},
                "Target SLO writer Control request");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown SLO writer Control request schema");
        }
        final var scope = TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2));
        final var lineage = QueryCodecSupport.fixed(f.get(2), 3, 16);
        final TargetSloWriterControlRequest value;
        if (register) {
            final var contract = TargetSloWriterContract.decode(QueryCodecSupport.bytes(f.get(3), 4));
            if (!scope.equals(contract.scope()) || !Arrays.equals(lineage, contract.lineage())) {
                throw new IllegalArgumentException("writer REGISTER wrapper changed its declaration scope");
            }
            value = register(contract, f.size() == 5 ? QueryCodecSupport.fixed(f.get(4), 5, 32) : null);
        } else {
            final var envelope = QueryCodecSupport.bytes(f.get(4), 7);
            if (envelope.length > MAX_FENCE_INPUT_BYTES) {
                throw new IllegalArgumentException("writer CLOSE fence exceeds its original codec bound");
            }
            value = close(
                    scope,
                    lineage,
                    TargetSloWriterContract.Reference.decode(QueryCodecSupport.bytes(f.get(3), 6)),
                    SystemMutation.decodeTargetEnvelope(envelope, SystemMutationType.TIME_FENCE),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 8)));
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target SLO writer Control request");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetSloWriterControlRequest request
                && operation == request.operation
                && Arrays.equals(canonicalBytes(), request.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
