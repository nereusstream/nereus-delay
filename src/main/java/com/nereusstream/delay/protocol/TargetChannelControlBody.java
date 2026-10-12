package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** ApplyShardControl kinds 24..26 for signed Source channel lifecycle. */
public final class TargetChannelControlBody {
    public static final int MAX_CANONICAL_BYTES = TargetChannelControlRequest.MAX_CANONICAL_BYTES + 192;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-channel-control\0");

    private final ShardId shard;
    private final long retryUntil;
    private final ControlRef controlRef;
    private final TargetChannelControlRequest request;
    private final int expectedRecordState;

    public TargetChannelControlBody(
            final ShardId shard,
            final long retryUntil,
            final ControlRef controlRef,
            final TargetChannelControlRequest request,
            final int expectedRecordState) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.request = Objects.requireNonNull(request, "request");
        TargetCompatibilityCodec.assigned(controlRef.operationId(), 32, "Target channel operation ID");
        if (retryUntil < 0
                || controlRef.targetIndex() != 0
                || !shard.equals(request.channel().context().sourceShard())
                || !Arrays.equals(
                        controlRef.requestHash(),
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()))
                || expectedRecordState != (request.operationKind() == ControlOperationKind.CREATE_TARGET_CHANNEL
                        ? 1 : 0)) {
            throw new IllegalArgumentException("Target channel Control body source/request/precondition mismatch");
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

    public TargetChannelControlRequest request() {
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

    public static TargetChannelControlBody decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 9, false, "Target channel policy Control body");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 10, 11, 12, 13, 14, 15}, "Target channel policy Control body");
        final int kind = QueryCodecSupport.uint32(fields.get(4), 11);
        if (kind < TargetChannelControlRequest.CREATE_CONTROL_KIND
                || kind > TargetChannelControlRequest.STOP_CONTROL_KIND
                || QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint(fields.get(5), 12) != 1) {
            throw new IllegalArgumentException("Target channel policy Control type/version mismatch");
        }
        final byte[] shardBytes = QueryCodecSupport.bytes(fields.getFirst(), 1);
        TargetCompatibilityCodec.read(shardBytes, 24, 2, false, "Target channel Control Shard");
        final byte[] ref = QueryCodecSupport.bytes(fields.get(3), 10);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "Target channel ControlRef");
        final byte[] payloadBytes = QueryCodecSupport.bytes(fields.getLast(), 15);
        final var payload = TargetCompatibilityCodec.read(
                payloadBytes,
                TargetChannelControlRequest.MAX_CANONICAL_BYTES + 5,
                1,
                false,
                "Target channel ControlPayload");
        QueryCodecSupport.requireNumbers(payload, new int[] {kind}, "Target channel ControlPayload");
        final var request = TargetChannelControlRequest.decode(
                operationKind(kind), QueryCodecSupport.bytes(payload.getFirst(), kind));
        final var result = new TargetChannelControlBody(
                ShardSubject.decode(shardBytes).shardId(),
                QueryCodecSupport.uint(fields.get(2), 3),
                ControlRef.decode(ref),
                request,
                Math.toIntExact(QueryCodecSupport.uint(fields.get(7), 14)));
        if (!Bytes.constantTimeEquals(result.semanticHash(), QueryCodecSupport.fixed(fields.get(6), 13, 32))) {
            throw new IllegalArgumentException("Target channel policy Control semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target channel policy Control body");
        return result;
    }

    private static ControlOperationKind operationKind(final int kind) {
        return switch (kind) {
            case TargetChannelControlRequest.CREATE_CONTROL_KIND -> ControlOperationKind.CREATE_TARGET_CHANNEL;
            case TargetChannelControlRequest.RENEW_CONTROL_KIND -> ControlOperationKind.RENEW_TARGET_CHANNEL;
            case TargetChannelControlRequest.STOP_CONTROL_KIND -> ControlOperationKind.STOP_TARGET_CHANNEL_NEW_WORK;
            default -> throw new IllegalArgumentException("unknown Target channel Control kind");
        };
    }
}
