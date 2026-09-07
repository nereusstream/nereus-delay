package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Reserved ApplyShardControl kinds 15/16. The active Lane semantic decoder still rejects them. */
public final class TargetMembershipControlBody {
    public static final int MAX_CANONICAL_BYTES = TargetMembershipControlRequest.MAX_CANONICAL_BYTES + 160;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-membership-control\0");
    private final ShardId shard;
    private final long retryUntil;
    private final ControlRef controlRef;
    private final TargetMembershipControlRequest request;

    public TargetMembershipControlBody(
            final ShardId shard,
            final long retryUntil,
            final ControlRef controlRef,
            final TargetMembershipControlRequest request) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.request = Objects.requireNonNull(request, "request");
        TargetCompatibilityCodec.assigned(controlRef.operationId(), 32, "membership operation ID");
        if (retryUntil < 0
                || controlRef.targetIndex() != 0
                || !shard.equals(request.policy().controls().sourceShard())
                || !Arrays.equals(
                        controlRef.requestHash(),
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()))
                || (request.isIssue()
                        && !Arrays.equals(
                                controlRef.operationId(), request.policy().requireRegistration(request.value())))) {
            throw new IllegalArgumentException("membership Control body source/request/operation mismatch");
        }
        this.retryUntil = retryUntil;
    }

    public ShardId shard() {
        return shard;
    }

    public long retryUntil() {
        return retryUntil;
    }

    public ControlRef controlRef() {
        return controlRef;
    }

    public TargetMembershipControlRequest request() {
        return request;
    }

    public byte[] logicalIdentity() {
        return controlRef.logicalOperationIdentity(request.controlKind());
    }

    public byte[] semanticHash() {
        return Bytes.sha256(DIGEST_DOMAIN, Bytes.u16be(request.controlKind()), request.canonicalBytes());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64Bits(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, request.controlKind());
            CanonicalProtobuf.uint64Bits(out, 12, 1);
            CanonicalProtobuf.bytes(out, 13, semanticHash());
            if (!request.isIssue()) {
                CanonicalProtobuf.uint64Bits(out, 14, 0);
            }
            CanonicalProtobuf.bytes(
                    out,
                    15,
                    CanonicalProtobuf.message(payload ->
                            CanonicalProtobuf.bytes(payload, request.controlKind(), request.canonicalBytes())));
        });
    }

    public static TargetMembershipControlBody decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "membership Control body");
        if (fields.size() != 8 && fields.size() != 9) {
            throw new IllegalArgumentException("membership Control body field count");
        }
        final int kind = QueryCodecSupport.uint32(fields.get(4), 11);
        final boolean issue = kind == 15;
        if (!issue && kind != 16) {
            throw new IllegalArgumentException("unknown membership Control kind");
        }
        QueryCodecSupport.requireNumbers(
                fields,
                issue ? new int[] {1, 2, 3, 10, 11, 12, 13, 15} : new int[] {1, 2, 3, 10, 11, 12, 13, 14, 15},
                "membership Control body");
        if (QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint(fields.get(5), 12) != 1
                || (!issue && QueryCodecSupport.uint(fields.get(7), 14) != 0)) {
            throw new IllegalArgumentException("membership Control body type/version/precondition mismatch");
        }
        final byte[] shardBytes = QueryCodecSupport.bytes(fields.getFirst(), 1);
        TargetCompatibilityCodec.read(shardBytes, 24, 2, false, "membership subject");
        final byte[] ref = QueryCodecSupport.bytes(fields.get(3), 10);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "membership ControlRef");
        final var payload = TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(fields.getLast(), 15),
                TargetMembershipControlRequest.MAX_CANONICAL_BYTES + 5,
                1,
                false,
                "membership ControlPayload");
        QueryCodecSupport.requireNumbers(payload, new int[] {kind}, "membership ControlPayload");
        final var request = TargetMembershipControlRequest.decode(
                issue ? ControlOperationKind.GRANT_TARGET_MEMBERSHIP : ControlOperationKind.CLOSE_TARGET_MEMBERSHIP,
                QueryCodecSupport.bytes(payload.getFirst(), kind));
        final var result = new TargetMembershipControlBody(
                ShardSubject.decode(shardBytes).shardId(),
                QueryCodecSupport.uint(fields.get(2), 3),
                ControlRef.decode(ref),
                request);
        if (!Bytes.constantTimeEquals(result.semanticHash(), QueryCodecSupport.fixed(fields.get(6), 13, 32))) {
            throw new IllegalArgumentException("membership Control semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "membership Control body");
        return result;
    }
}
