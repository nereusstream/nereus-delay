package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Explicit registered management Source branch; ordinary Outcomes cannot initialize old owners. */
public record TargetPayloadSummaryInitializationBody(
        ShardId shard, long retryUntil, ControlRef controlRef, TargetPayloadSummaryInitializationRequest request) {
    public static final int MAX_CANONICAL_BYTES = TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES + 192;
    public static final int MAX_INPUT_BYTES = MAX_CANONICAL_BYTES + 512;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-payload-summary-initialize-v1\0");

    public TargetPayloadSummaryInitializationBody {
        Objects.requireNonNull(shard);
        Objects.requireNonNull(controlRef);
        Objects.requireNonNull(request);
        request.requireControlRef(controlRef);
        if (retryUntil < 0
                || retryUntil == Long.MAX_VALUE
                || !shard.equals(request.scope().shard())) {
            throw new IllegalArgumentException("summary initialization changed Source scope/window");
        }
    }

    public ProtocolTuple tuple() {
        return request.tuple();
    }

    public byte[] logicalIdentity() {
        return controlRef.logicalOperationIdentity(TargetPayloadSummaryInitializationRequest.CONTROL_KIND);
    }

    public byte[] semanticHash() {
        return Bytes.sha256(DOMAIN, request.canonicalBytes());
    }

    public static boolean isControl(SystemMutation input) {
        if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            return false;
        }
        final var reader = new CanonicalProtobuf.Reader(input.canonicalBody());
        for (int count = 0; count < 5 && reader.hasRemaining(); count++) {
            final var field = reader.next();
            if (field.number() == 11) {
                return field.wireType() == 0
                        && field.unsignedValue() == TargetPayloadSummaryInitializationRequest.CONTROL_KIND;
            }
        }
        return false;
    }

    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, TargetPayloadSummaryInitializationRequest.CONTROL_KIND);
            CanonicalProtobuf.uint32(out, 12, 1);
            CanonicalProtobuf.bytes(out, 13, semanticHash());
            CanonicalProtobuf.uint32(out, 14, 1);
            CanonicalProtobuf.bytes(
                    out,
                    15,
                    CanonicalProtobuf.message(payload -> CanonicalProtobuf.bytes(
                            payload,
                            TargetPayloadSummaryInitializationRequest.CONTROL_KIND,
                            request.canonicalBytes())));
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("summary Source body exceeds its complete bound");
        }
        return encoded;
    }

    public void requireInput(SystemMutation input) {
        if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL
                || !input.shardId().equals(shard)
                || input.retryUntilEpochMs() != retryUntil
                || AuthorIdentity.decode(input.authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(input.canonicalBody(), canonicalBytes())
                || !Arrays.equals(input.logicalOperationIdentity(), logicalIdentity())
                || input.canonicalEnvelope().length > MAX_INPUT_BYTES) {
            throw new IllegalArgumentException("summary initialization changed complete signed CONTROL input");
        }
    }

    public static TargetPayloadSummaryInitializationBody decode(byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "payload summary Source body");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 10, 11, 12, 13, 14, 15}, "payload summary Source body");
        if (QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint32(fields.get(4), 11) != TargetPayloadSummaryInitializationRequest.CONTROL_KIND
                || QueryCodecSupport.uint32(fields.get(5), 12) != 1
                || QueryCodecSupport.uint32(fields.get(7), 14) != 1) {
            throw new IllegalArgumentException("unknown summary initialization kind/schema/state");
        }
        final var payload = TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(fields.get(8), 15),
                TargetPayloadSummaryInitializationRequest.MAX_CANONICAL_BYTES + 8,
                1,
                false,
                "summary initialization payload");
        QueryCodecSupport.requireNumbers(
                payload,
                new int[] {TargetPayloadSummaryInitializationRequest.CONTROL_KIND},
                "summary initialization payload");
        final var body = new TargetPayloadSummaryInitializationBody(
                ShardSubject.decode(QueryCodecSupport.bytes(fields.getFirst(), 1))
                        .shardId(),
                QueryCodecSupport.uint64Bits(fields.get(2), 3),
                ControlRef.decode(QueryCodecSupport.bytes(fields.get(3), 10)),
                TargetPayloadSummaryInitializationRequest.decode(QueryCodecSupport.bytes(
                        payload.getFirst(), TargetPayloadSummaryInitializationRequest.CONTROL_KIND)));
        if (!Arrays.equals(QueryCodecSupport.fixed(fields.get(6), 13, 32), body.semanticHash())) {
            throw new IllegalArgumentException("summary initialization semantic hash changed");
        }
        QueryCodecSupport.requireCanonical(encoded, body.canonicalBytes(), "payload summary Source body");
        return body;
    }
}
