package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Bootstrap Control marker at APPLY_SHARD_CONTROL/kind1, without requiring the tuple it activates. */
public record TargetProtocolActivationBody(
        ShardId shard, long retryUntil, ControlRef controlRef, TargetProtocolActivatePayload payload) {
    public static final int V1_MAX_BYTES = TargetProtocolActivatePayload.V1_MAX_BYTES + 256;
    public static final int MAX_BYTES = TargetProtocolActivatePayload.MAX_BYTES + 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-protocol-activation-control\0");

    public TargetProtocolActivationBody {
        Objects.requireNonNull(shard);
        Objects.requireNonNull(controlRef);
        Objects.requireNonNull(payload);
        if (retryUntil < 0
                || controlRef.targetIndex() != 0
                || !shard.equals(payload.scope().shard())
                || !Arrays.equals(
                        controlRef.requestHash(),
                        PreparedControlOperation.requestHash(
                                ControlOperationKind.ACTIVATE_TARGET_PROTOCOL, payload.operationRequest()))) {
            throw new IllegalArgumentException("Target protocol activation changed exact Control request/Source scope");
        }
    }

    public byte[] logicalIdentity() {
        return controlRef.logicalOperationIdentity(TargetProtocolActivatePayload.CONTROL_KIND);
    }

    public byte[] semanticHash() {
        return Bytes.sha256(DOMAIN, Bytes.u16be(TargetProtocolActivatePayload.CONTROL_KIND), payload.canonicalBytes());
    }
    /** Only the explicit varint schema discriminator selects this branch; malformed new versions never fall back. */
    public static boolean hasTargetPayload(byte[] encoded) {
        final var fields = SystemMutationBodyCodec.fields(SystemMutationType.APPLY_SHARD_CONTROL, encoded);
        final var kind =
                fields.stream().filter(f -> f.number() == 11).findFirst().orElseThrow();
        if (QueryCodecSupport.uint32(kind, 11) != TargetProtocolActivatePayload.CONTROL_KIND) {
            return false;
        }
        final var payload =
                fields.stream().filter(f -> f.number() == 15).findFirst().orElseThrow();
        final var branch = new CanonicalProtobuf.Reader(QueryCodecSupport.bytes(payload, 15));
        if (!branch.hasRemaining()) {
            throw new IllegalArgumentException("empty activation ControlPayload");
        }
        final var value = branch.next();
        if (value.number() != 1 || value.wireType() != 2 || branch.hasRemaining()) {
            throw new IllegalArgumentException("invalid activation ControlPayload branch");
        }
        final var reader = new CanonicalProtobuf.Reader(QueryCodecSupport.bytes(value, 1));
        if (!reader.hasRemaining()) {
            throw new IllegalArgumentException("empty activation payload");
        }
        final var first = reader.next();
        return first.number() == 1 && first.wireType() == 0;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, 1);
            CanonicalProtobuf.uint32(out, 12, 1);
            CanonicalProtobuf.bytes(out, 13, semanticHash());
            CanonicalProtobuf.uint32(out, 14, 0);
            CanonicalProtobuf.bytes(
                    out,
                    15,
                    CanonicalProtobuf.message(branch -> CanonicalProtobuf.bytes(branch, 1, payload.canonicalBytes())));
        });
    }

    public static TargetProtocolActivationBody decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_BYTES, 9, false, "Target protocol activation body");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 10, 11, 12, 13, 14, 15}, "Target protocol activation body");
        if (QueryCodecSupport.uint(f.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint(f.get(4), 11) != 1
                || QueryCodecSupport.uint(f.get(5), 12) != 1
                || QueryCodecSupport.uint(f.get(7), 14) != 0) {
            throw new IllegalArgumentException("invalid Target protocol activation marker/precondition");
        }
        final var branch = TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(f.get(8), 15),
                TargetProtocolActivatePayload.MAX_BYTES + 8,
                1,
                false,
                "Target activation branch");
        QueryCodecSupport.requireNumbers(branch, new int[] {1}, "Target activation branch");
        final var body = new TargetProtocolActivationBody(
                ShardSubject.decode(QueryCodecSupport.bytes(f.get(0), 1)).shardId(),
                QueryCodecSupport.uint(f.get(2), 3),
                ControlRef.decode(QueryCodecSupport.bytes(f.get(3), 10)),
                TargetProtocolActivatePayload.decode(QueryCodecSupport.bytes(branch.getFirst(), 1)));
        if (!Arrays.equals(QueryCodecSupport.fixed(f.get(6), 13, 32), body.semanticHash())) {
            throw new IllegalArgumentException("Target activation semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, body.canonicalBytes(), "Target protocol activation body");
        return body;
    }
}
