package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Separate bootstrap ApplyShardControl branch. Only actual paid Source apply may create its credit. */
public record TargetParentControlCreditBody(
        ShardId shard,
        long retryUntil,
        ControlRef controlRef,
        TargetParentControlCreditRequest request,
        int expectedRecordState) {
    public static final int BODY_VERSION = 23;
    public static final int MAX_CANONICAL_BYTES = TargetParentControlCreditRequest.MAX_CANONICAL_BYTES + 192;
    public static final int MAX_INPUT_BYTES = MAX_CANONICAL_BYTES + 1024;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-parent-control-credit\0");

    public TargetParentControlCreditBody {
        Objects.requireNonNull(shard);
        Objects.requireNonNull(controlRef);
        Objects.requireNonNull(request);
        request.requireControlRef(controlRef);
        if (retryUntil < 0
                || !shard.equals(request.scope().shard())
                || expectedRecordState != 1
                || expectedRecordState != request.expectedRecordState()) {
            throw new IllegalArgumentException(
                    "parent Control credit changed its Source Shard or absence precondition");
        }
    }

    public byte[] logicalIdentity() {
        return controlRef.logicalOperationIdentity(TargetParentControlCreditRequest.CONTROL_KIND);
    }

    public int schemaVersion() {
        return 1;
    }

    public ProtocolTuple tuple() {
        return ProtocolTuple.targetParentControlCredit();
    }

    public byte[] semanticHash() {
        return Bytes.sha256(
                DOMAIN, Bytes.u16be(TargetParentControlCreditRequest.CONTROL_KIND), request.canonicalBytes());
    }

    public static boolean isControl(SystemMutation input) {
        if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            return false;
        }
        final var reader = new CanonicalProtobuf.Reader(input.canonicalBody());
        for (int count = 0; count < 5 && reader.hasRemaining(); count++) {
            final var field = reader.next();
            if (field.number() == 11) {
                if (field.wireType() != 0) {
                    throw new IllegalArgumentException("parent Control credit discriminator is not varint");
                }
                return field.unsignedValue() == TargetParentControlCreditRequest.CONTROL_KIND;
            }
        }
        return false;
    }

    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64Bits(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, TargetParentControlCreditRequest.CONTROL_KIND);
            CanonicalProtobuf.uint32(out, 12, schemaVersion());
            CanonicalProtobuf.bytes(out, 13, semanticHash());
            CanonicalProtobuf.uint32(out, 14, expectedRecordState);
            CanonicalProtobuf.bytes(
                    out,
                    15,
                    CanonicalProtobuf.message(payload -> CanonicalProtobuf.bytes(
                            payload, TargetParentControlCreditRequest.CONTROL_KIND, request.canonicalBytes())));
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("parent Control credit body exceeds its codec bound");
        }
        return encoded;
    }

    public static TargetParentControlCreditBody decode(byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, 9, false, "Target parent Control credit body");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 10, 11, 12, 13, 14, 15}, "Target parent Control credit body");
        if (QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint32(fields.get(4), 11) != TargetParentControlCreditRequest.CONTROL_KIND
                || QueryCodecSupport.uint32(fields.get(5), 12) != 1) {
            throw new IllegalArgumentException("unknown parent Control credit type/kind/schema");
        }
        final var shard = QueryCodecSupport.bytes(fields.getFirst(), 1);
        TargetCompatibilityCodec.read(shard, 24, 2, false, "parent Control credit Shard subject");
        final var ref = QueryCodecSupport.bytes(fields.get(3), 10);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "parent Control credit ControlRef");
        final var payload = TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(fields.get(8), 15),
                TargetParentControlCreditRequest.MAX_CANONICAL_BYTES + 8,
                1,
                false,
                "parent Control credit payload");
        QueryCodecSupport.requireNumbers(
                payload, new int[] {TargetParentControlCreditRequest.CONTROL_KIND}, "parent Control credit payload");
        final var result = new TargetParentControlCreditBody(
                ShardSubject.decode(shard).shardId(),
                QueryCodecSupport.uint64Bits(fields.get(2), 3),
                ControlRef.decode(ref),
                TargetParentControlCreditRequest.decode(
                        QueryCodecSupport.bytes(payload.getFirst(), TargetParentControlCreditRequest.CONTROL_KIND)),
                QueryCodecSupport.uint32(fields.get(7), 14));
        if (!Arrays.equals(result.semanticHash(), QueryCodecSupport.fixed(fields.get(6), 13, 32))) {
            throw new IllegalArgumentException("parent Control credit semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target parent Control credit body");
        return result;
    }
}
