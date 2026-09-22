package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Target Close ApplyShardControl kind 18, bound to one member of the frozen complete Shard set. */
public final class TargetCloseBody {
    public static final int MAX_CANONICAL_BYTES = TargetCloseRequest.MAX_CANONICAL_BYTES + 192;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-close-control\0");
    private final ShardId shard;
    private final long retryUntil;
    private final ControlRef controlRef;
    private final TargetCloseRequest request;

    public TargetCloseBody(
            final ShardId shard, final long retryUntil, final ControlRef controlRef, final TargetCloseRequest request) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.controlRef = Objects.requireNonNull(controlRef, "controlRef");
        this.request = Objects.requireNonNull(request, "request");
        if (retryUntil < 0
                || !shard.equals(request.requireControlRef(controlRef).shard())) {
            throw new IllegalArgumentException("Target close body source/retry mismatch");
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

    public TargetCloseRequest request() {
        return request;
    }

    public byte[] logicalIdentity() {
        return controlRef.logicalOperationIdentity(TargetCloseRequest.CONTROL_KIND);
    }

    public byte[] semanticHash() {
        return Bytes.sha256(DIGEST_DOMAIN, Bytes.u16be(TargetCloseRequest.CONTROL_KIND), request.canonicalBytes());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.uint64(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, controlRef.canonicalBytes());
            CanonicalProtobuf.uint32(out, 11, TargetCloseRequest.CONTROL_KIND);
            CanonicalProtobuf.uint64Bits(
                    out,
                    12,
                    TargetQueueState.nextRevision(
                            request.requireControlRef(controlRef).expectedControlVersion()));
            CanonicalProtobuf.bytes(out, 13, semanticHash());
            CanonicalProtobuf.bytes(
                    out,
                    15,
                    CanonicalProtobuf.message(payload -> CanonicalProtobuf.bytes(
                            payload, TargetCloseRequest.CONTROL_KIND, request.canonicalBytes())));
        });
    }

    public static TargetCloseBody decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Target close body");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 10, 11, 12, 13, 15}, "Target close body");
        if (QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.APPLY_SHARD_CONTROL.wireValue()
                || QueryCodecSupport.uint32(fields.get(4), 11) != TargetCloseRequest.CONTROL_KIND) {
            throw new IllegalArgumentException("not a Target close control body");
        }
        final byte[] subject = QueryCodecSupport.bytes(fields.getFirst(), 1);
        TargetCompatibilityCodec.read(subject, 24, 2, false, "Target close subject");
        final byte[] ref = QueryCodecSupport.bytes(fields.get(3), 10);
        TargetCompatibilityCodec.read(ref, 74, 3, false, "Target close ControlRef");
        final var payload = TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(fields.getLast(), 15),
                TargetCloseRequest.MAX_CANONICAL_BYTES + 5,
                1,
                false,
                "Target close ControlPayload");
        QueryCodecSupport.requireNumbers(
                payload, new int[] {TargetCloseRequest.CONTROL_KIND}, "Target close ControlPayload");
        final var request =
                TargetCloseRequest.decode(QueryCodecSupport.bytes(payload.getFirst(), TargetCloseRequest.CONTROL_KIND));
        if (QueryCodecSupport.uint64Bits(fields.get(5), 12)
                != TargetQueueState.nextRevision(
                        request.requireControlRef(ControlRef.decode(ref)).expectedControlVersion())) {
            throw new IllegalArgumentException("Target close semantic/prior version mismatch");
        }
        final var result = new TargetCloseBody(
                ShardSubject.decode(subject).shardId(),
                QueryCodecSupport.uint(fields.get(2), 3),
                ControlRef.decode(ref),
                request);
        if (!Arrays.equals(result.semanticHash(), QueryCodecSupport.fixed(fields.get(6), 13, 32))) {
            throw new IllegalArgumentException("Target close control semantic hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target close body");
        return result;
    }
}
