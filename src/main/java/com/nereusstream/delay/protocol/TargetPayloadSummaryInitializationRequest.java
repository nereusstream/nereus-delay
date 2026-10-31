package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** One exact old Native32 upgrade at a protected complete cut. Count data is not initialization authority. */
public record TargetPayloadSummaryInitializationRequest(
        TargetQuotaScope scope,
        byte[] lineage,
        TargetQuotaPayloadOwner expectedOwner,
        TargetQuotaMutation auditedCut,
        CheckpointResource protectedImage,
        byte[] foldDigest,
        long unresolvedCount)
        implements ControlOperationRequestBranch {
    public static final int CONTROL_KIND = 38;
    public static final int MAX_CANONICAL_BYTES = TargetResourceReclaimBody.MAX_NATIVE_CANONICAL_BYTES;

    public TargetPayloadSummaryInitializationRequest {
        Objects.requireNonNull(scope);
        Objects.requireNonNull(expectedOwner);
        Objects.requireNonNull(auditedCut);
        Objects.requireNonNull(protectedImage);
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "payload summary original lineage");
        foldDigest = TargetCompatibilityCodec.assigned(foldDigest, 32, "complete payload summary fold");
        if (scope.target() != null
                || expectedOwner.summaryQualified()
                || unresolvedCount < 0
                || !scope.shard().equals(expectedOwner.messageId().routingId().shardId())
                || !Arrays.equals(scope.tenantScope(), expectedOwner.tenantScope())
                || !Arrays.equals(lineage, expectedOwner.recoveryLineage())
                || !Arrays.equals(lineage, protectedImage.recoveryLineageId())
                || protectedImage.manifestLength() <= 0
                || protectedImage.manifestLength() == Long.MAX_VALUE) {
            throw new IllegalArgumentException("summary upgrade changed the exact old owner/image/scope");
        }
        expectedOwner.mutation().requireAtOrBefore(auditedCut);
    }

    @Override
    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    @Override
    public byte[] foldDigest() {
        return Bytes.copy(foldDigest);
    }

    public ControlOperationKind operationKind() {
        return ControlOperationKind.INITIALIZE_TARGET_PAYLOAD_SUMMARY;
    }

    public ControlOperationRequest operationRequest() {
        return new ControlOperationRequest(operationKind(), this);
    }

    public ProtocolTuple tuple() {
        return ProtocolTuple.targetPayloadObligationSummary();
    }

    public void requireControlRef(ControlRef ref) {
        if (ref.targetIndex() != 0
                || !Arrays.equals(
                        ref.requestHash(), PreparedControlOperation.requestHash(operationKind(), operationRequest()))) {
            throw new IllegalArgumentException("summary initialization changed its exact Prepared request");
        }
    }

    @Override
    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, lineage);
            CanonicalProtobuf.bytes(out, 4, expectedOwner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, auditedCut.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, protectedImage.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, foldDigest);
            CanonicalProtobuf.uint64(out, 8, unresolvedCount);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("summary initialization exceeds complete Source codec bound");
        }
        return encoded;
    }

    public static TargetPayloadSummaryInitializationRequest decode(byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 8, false, "payload summary initialization request");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "payload summary initialization request");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown summary initialization schema");
        }
        final byte[] owner = QueryCodecSupport.bytes(fields.get(3), 4);
        if (owner.length > TargetQuotaPayloadOwner.V1_MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("old owner exceeds its original v1 ceiling");
        }
        final var request = new TargetPayloadSummaryInitializationRequest(
                TargetQuotaScope.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 16),
                TargetQuotaPayloadOwner.decode(owner),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                CheckpointResource.decode(QueryCodecSupport.bytes(fields.get(5), 6)),
                QueryCodecSupport.fixed(fields.get(6), 7, 32),
                QueryCodecSupport.uint64Bits(fields.get(7), 8));
        QueryCodecSupport.requireCanonical(encoded, request.canonicalBytes(), "payload summary initialization request");
        return request;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetPayloadSummaryInitializationRequest that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
