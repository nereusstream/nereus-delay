package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Local attempt provenance only; current Owner, Catalog and Pin authority remain mandatory. */
public record TargetCheckpointInstallIntent(RecoveryPin origin, byte[] targetIncarnation,
        byte[] sourceIncarnation, byte[] dbIdentity, SourcePosition source, boolean published) {
    public static final int MAX_BYTES = 16 * 1024;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-checkpoint-install-intent-v1\0");

    public TargetCheckpointInstallIntent {
        Objects.requireNonNull(origin); Objects.requireNonNull(source);
        Bytes.requireLength(targetIncarnation, 16, "target incarnation");
        Bytes.requireLength(sourceIncarnation, 16, "source incarnation");
        Bytes.requireLength(dbIdentity, 32, "DB identity");
        if (origin.candidate().kind() != RecoveryCandidateKind.CATALOG_CHECKPOINT
                || !origin.shard().shardId().equals(source.shardId())
                || Arrays.equals(targetIncarnation, new byte[16])
                || Arrays.equals(sourceIncarnation, new byte[16])
                || Arrays.equals(targetIncarnation, sourceIncarnation)
                || Arrays.equals(dbIdentity, new byte[32]) || origin.canonicalBytes().length > 4096
                || source.canonicalBytes().length > 4096) {
            throw new IllegalArgumentException("invalid bounded Target install provenance");
        }
        targetIncarnation = Bytes.copy(targetIncarnation); sourceIncarnation = Bytes.copy(sourceIncarnation);
        dbIdentity = Bytes.copy(dbIdentity);
    }
    @Override public byte[] targetIncarnation() { return Bytes.copy(targetIncarnation); }
    @Override public byte[] sourceIncarnation() { return Bytes.copy(sourceIncarnation); }
    @Override public byte[] dbIdentity() { return Bytes.copy(dbIdentity); }
    public TargetCheckpointInstallIntent asPublished() {
        return published ? this : new TargetCheckpointInstallIntent(origin, targetIncarnation, sourceIncarnation, dbIdentity, source, true);
    }
    private byte[] core() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, origin.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, targetIncarnation);
            CanonicalProtobuf.bytes(out, 4, sourceIncarnation);
            CanonicalProtobuf.bytes(out, 5, dbIdentity);
            CanonicalProtobuf.bytes(out, 6, source.canonicalBytes());
            CanonicalProtobuf.uint32(out, 7, 2);
            CanonicalProtobuf.uint32(out, 8, published ? 2 : 1);
        });
    }
    public byte[] canonicalBytes() {
        final byte[] core = core();
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(core); CanonicalProtobuf.bytes(out, 9, Bytes.sha256(DOMAIN, core));
        });
    }
    public static TargetCheckpointInstallIntent decode(byte[] image) {
        if (image == null || image.length == 0 || image.length > MAX_BYTES) {
            throw new IllegalArgumentException("Target install intent exceeds its bound");
        }
        final var f = QueryCodecSupport.read(image, "TargetCheckpointInstallIntent");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9}, "TargetCheckpointInstallIntent");
        final int phase = QueryCodecSupport.uint32(f.get(7), 8);
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1 || QueryCodecSupport.uint32(f.get(6), 7) != 2
                || phase != 1 && phase != 2) throw new IllegalArgumentException("unsupported Target install intent version/phase");
        final var result = new TargetCheckpointInstallIntent(RecoveryPin.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 16), QueryCodecSupport.fixed(f.get(3), 4, 16),
                QueryCodecSupport.fixed(f.get(4), 5, 32), SourcePositionCodec.decode(QueryCodecSupport.bytes(f.get(5), 6)), phase == 2);
        if (!Bytes.constantTimeEquals(QueryCodecSupport.fixed(f.get(8), 9, 32), Bytes.sha256(DOMAIN, result.core()))) {
            throw new IllegalArgumentException("Target install intent digest mismatch");
        }
        QueryCodecSupport.requireCanonical(image, result.canonicalBytes(), "TargetCheckpointInstallIntent");
        return result;
    }
}
