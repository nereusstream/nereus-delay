package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Reserved ApplyShardControl kind 17. Its canonical body is complete before mutation identity registration. */
public final class TargetQuotaGrantControlBody {
    public static final int MAX_CANONICAL_BYTES = TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES + 192;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-control\0");
    private final ShardId shard;
    private final long retryUntil;
    private final ControlRef controlRef;
    private final TargetQuotaGrantControlRequest request;

    public TargetQuotaGrantControlBody(
            final ShardId shard,
            final long retryUntil,
            final ControlRef controlRef,
            final TargetQuotaGrantControlRequest request) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.request = Objects.requireNonNull(request, "request");
        request.requireControlRef(controlRef);
        if (retryUntil < 0 || !shard.equals(request.next().scope().shard())) {
            throw new IllegalArgumentException("Target quota grant body source/retry mismatch");
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

    public TargetQuotaGrantControlRequest request() {
        return request;
    }

    public byte[] logicalIdentity() {
        return controlRef.logicalOperationIdentity(TargetQuotaGrantControlRequest.CONTROL_KIND);
    }

    public byte[] semanticHash() {
        return Bytes.sha256(
                DIGEST_DOMAIN, Bytes.u16be(TargetQuotaGrantControlRequest.CONTROL_KIND), request.canonicalBytes());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, TargetQuotaGrantControlRequest.CONTROL_KIND);
            CanonicalProtobuf.uint64Bits(out, 12, request.next().version());
            CanonicalProtobuf.bytes(out, 13, semanticHash());
            CanonicalProtobuf.bytes(
                    out,
                    15,
                    CanonicalProtobuf.message(payload -> CanonicalProtobuf.bytes(
                            payload, TargetQuotaGrantControlRequest.CONTROL_KIND, request.canonicalBytes())));
        });
    }

    public static TargetQuotaGrantControlBody decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Target quota grant body");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 10, 11, 12, 13, 15}, "Target quota grant body");
        if (QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint32(fields.get(4), 11) != TargetQuotaGrantControlRequest.CONTROL_KIND) {
            throw new IllegalArgumentException("not a Target quota grant control body");
        }
        final byte[] subject = QueryCodecSupport.bytes(fields.getFirst(), 1);
        TargetCompatibilityCodec.read(subject, 24, 2, false, "quota grant subject");
        final byte[] ref = QueryCodecSupport.bytes(fields.get(3), 10);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "quota grant ControlRef");
        final var payload = TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(fields.getLast(), 15),
                TargetQuotaGrantControlRequest.MAX_CANONICAL_BYTES + 4,
                1,
                false,
                "quota grant ControlPayload");
        QueryCodecSupport.requireNumbers(
                payload, new int[] {TargetQuotaGrantControlRequest.CONTROL_KIND}, "quota grant ControlPayload");
        final var request = TargetQuotaGrantControlRequest.decode(
                QueryCodecSupport.bytes(payload.getFirst(), TargetQuotaGrantControlRequest.CONTROL_KIND));
        if (QueryCodecSupport.uint64Bits(fields.get(5), 12) != request.next().version()) {
            throw new IllegalArgumentException("quota grant semantic/prior version mismatch");
        }
        final var result = new TargetQuotaGrantControlBody(
                ShardSubject.decode(subject).shardId(),
                QueryCodecSupport.uint(fields.get(2), 3),
                ControlRef.decode(ref),
                request);
        if (!Arrays.equals(result.semanticHash(), QueryCodecSupport.fixed(fields.get(6), 13, 32))) {
            throw new IllegalArgumentException("quota grant control semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target quota grant body");
        return result;
    }
}
