package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Reserved ApplyShardControl kinds 19..23 for source-ordered Target Native authority history. */
public final class TargetNativePolicyControlBody {
    public static final int MAX_CANONICAL_BYTES = TargetNativePolicyControlRequest.MAX_CANONICAL_BYTES + 192;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-native-policy-control\0");

    private final ShardId shard;
    private final long retryUntil;
    private final ControlRef controlRef;
    private final TargetNativePolicyControlRequest request;
    private final int expectedRecordState;

    public TargetNativePolicyControlBody(
            final ShardId shard,
            final long retryUntil,
            final ControlRef controlRef,
            final TargetNativePolicyControlRequest request,
            final int expectedRecordState) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.request = Objects.requireNonNull(request, "request");
        TargetCompatibilityCodec.assigned(controlRef.operationId(), 32, "Target Native operation ID");
        if (retryUntil < 0
                || controlRef.targetIndex() != 0
                || !shard.equals(request.scope().sourceShard())
                || !Arrays.equals(
                        controlRef.requestHash(),
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()))
                || expectedRecordState != (request.createsImmutableRecord() ? 1 : 0)) {
            throw new IllegalArgumentException("Target Native Control body source/request/precondition mismatch");
        }
        this.retryUntil = retryUntil;
        this.expectedRecordState = expectedRecordState;
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

    public TargetNativePolicyControlRequest request() {
        return request;
    }

    public int expectedRecordState() {
        return expectedRecordState;
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
            CanonicalProtobuf.uint64Bits(out, 14, expectedRecordState);
            CanonicalProtobuf.bytes(
                    out,
                    15,
                    CanonicalProtobuf.message(payload ->
                            CanonicalProtobuf.bytes(payload, request.controlKind(), request.canonicalBytes())));
        });
    }

    public static TargetNativePolicyControlBody decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 9, false, "Target Native policy Control body");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 10, 11, 12, 13, 14, 15}, "Target Native policy Control body");
        final int kind = QueryCodecSupport.uint32(fields.get(4), 11);
        if (kind < TargetNativePolicyControlRequest.INSTALL_PUBLISHER_CONTROL_KIND
                || kind > TargetNativePolicyControlRequest.CLOSE_MEMBER_CONTROL_KIND
                || QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint(fields.get(5), 12) != 1) {
            throw new IllegalArgumentException("Target Native policy Control type/version mismatch");
        }
        final byte[] shardBytes = QueryCodecSupport.bytes(fields.getFirst(), 1);
        TargetCompatibilityCodec.read(shardBytes, 24, 2, false, "Target Native Control Shard");
        final byte[] ref = QueryCodecSupport.bytes(fields.get(3), 10);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "Target Native ControlRef");
        final byte[] payloadBytes = QueryCodecSupport.bytes(fields.getLast(), 15);
        final var payload = TargetCompatibilityCodec.read(
                payloadBytes,
                TargetNativePolicyControlRequest.MAX_CANONICAL_BYTES + 5,
                1,
                false,
                "Target Native ControlPayload");
        QueryCodecSupport.requireNumbers(payload, new int[] {kind}, "Target Native ControlPayload");
        final var request = TargetNativePolicyControlRequest.decode(
                operationKind(kind), QueryCodecSupport.bytes(payload.getFirst(), kind));
        final var result = new TargetNativePolicyControlBody(
                ShardSubject.decode(shardBytes).shardId(),
                QueryCodecSupport.uint(fields.get(2), 3),
                ControlRef.decode(ref),
                request,
                Math.toIntExact(QueryCodecSupport.uint(fields.get(7), 14)));
        if (!Bytes.constantTimeEquals(result.semanticHash(), QueryCodecSupport.fixed(fields.get(6), 13, 32))) {
            throw new IllegalArgumentException("Target Native policy Control semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target Native policy Control body");
        return result;
    }

    private static ControlOperationKind operationKind(final int kind) {
        return switch (kind) {
            case TargetNativePolicyControlRequest.INSTALL_PUBLISHER_CONTROL_KIND ->
                ControlOperationKind.INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION;
            case TargetNativePolicyControlRequest.CLOSE_PUBLISHER_CONTROL_KIND ->
                ControlOperationKind.CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION;
            case TargetNativePolicyControlRequest.ACTIVATE_POLICY_CONTROL_KIND ->
                ControlOperationKind.ACTIVATE_TARGET_NATIVE_POLICY;
            case TargetNativePolicyControlRequest.APPROVE_MEMBER_CONTROL_KIND ->
                ControlOperationKind.APPROVE_TARGET_NATIVE_MEMBER;
            case TargetNativePolicyControlRequest.CLOSE_MEMBER_CONTROL_KIND ->
                ControlOperationKind.CLOSE_TARGET_NATIVE_MEMBER;
            default -> throw new IllegalArgumentException("unknown Target Native policy Control kind");
        };
    }
}
