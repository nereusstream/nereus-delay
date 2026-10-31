package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Latest old-shadow progress; exact content remains on the original protected physical Source, never another log. */
public record TargetMigrationShadowProgress(byte[] planDigest, byte[] migrationId, int recordKind,
        byte[] originalIdentity, byte[] originalSemanticHash, byte[] originalFrameDigest,
        TargetQuotaMutation appliedAt, SourcePosition prior, byte[] targetRootDigest,
        byte[] targetFirstDigest, byte[] targetPositionDigest, LogicalProofKind proofKind) {
    public static final int VALUE_TYPE = 70;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetSourcePosition.MAX_CANONICAL_BYTES + 384;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-shadow-progress-v1\0");
    public enum LogicalProofKind {
        FIRST_POSITION(1), EXPIRED_COMMAND_POSITION(2), MIGRATION_PREFIX(3);
        private final int wire;
        LogicalProofKind(int wire) { this.wire = wire; }
        public int wireValue() { return wire; }
        private static LogicalProofKind fromWire(int wire) {
            return switch (wire) { case 1 -> FIRST_POSITION; case 2 -> EXPIRED_COMMAND_POSITION; case 3 -> MIGRATION_PREFIX;
                default -> throw new IllegalArgumentException("unknown shadow logical proof kind"); };
        }
    }
    public TargetMigrationShadowProgress {
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        if (recordKind != ProtocolTuple.CLIENT_COMMAND && recordKind != ProtocolTuple.SYSTEM_MUTATION)
            throw new IllegalArgumentException("shadow progress must identify an original Source Command or System Mutation");
        originalIdentity = TargetCompatibilityCodec.assigned(originalIdentity, recordKind == ProtocolTuple.CLIENT_COMMAND ? CommandId.LENGTH : 32,
                "original Source record identity");
        originalSemanticHash = TargetCompatibilityCodec.assigned(originalSemanticHash, 32, "original Source semantic hash");
        originalFrameDigest = TargetCompatibilityCodec.assigned(originalFrameDigest, 32, "complete original Source frame digest");
        targetRootDigest = TargetCompatibilityCodec.assigned(targetRootDigest, 32, "actual Target migration root");
        Objects.requireNonNull(proofKind);
        if ((proofKind == LogicalProofKind.FIRST_POSITION) != (targetFirstDigest != null)
                || proofKind == LogicalProofKind.EXPIRED_COMMAND_POSITION && recordKind != ProtocolTuple.CLIENT_COMMAND) {
            throw new IllegalArgumentException("shadow must preserve an actual first or an explicit physical-only result kind");
        }
        targetFirstDigest = targetFirstDigest == null ? null : TargetCompatibilityCodec.assigned(targetFirstDigest, 32, "actual Target retained logical proof");
        targetPositionDigest = TargetCompatibilityCodec.assigned(targetPositionDigest, 32, "actual Target physical proof");
        Objects.requireNonNull(appliedAt).requireSourceApplied(); prior = TargetSourcePosition.requireBounded(prior);
        if (appliedAt.source().compareTo(prior) <= 0 || recordKind == ProtocolTuple.CLIENT_COMMAND
                && !new CommandId(originalIdentity).routingId().shardId().equals(appliedAt.source().shardId())) {
            throw new IllegalArgumentException("shadow progress changed actual original Source order/identity");
        }
    }
    public static TargetMigrationShadowProgress fromFrame(TargetMigrationPlan plan, byte[] originalFrame,
            TargetQuotaMutation actual, SourcePosition prior, byte[] root, byte[] first, byte[] position) {
        return fromFrame(plan, originalFrame, actual, prior, root, first, position, LogicalProofKind.FIRST_POSITION);
    }
    public static TargetMigrationShadowProgress fromFrame(TargetMigrationPlan plan, byte[] originalFrame,
            TargetQuotaMutation actual, SourcePosition prior, byte[] root, byte[] first, byte[] position, LogicalProofKind proofKind) {
        final var frame = ShardLogFrame.decode(originalFrame);
        final byte[] id; final byte[] hash;
        if (frame.recordKind() == ProtocolTuple.CLIENT_COMMAND) {
            final var command = CommandCodec.decodeFrame(originalFrame); id = command.commandId().bytes(); hash = command.commandHash();
        } else {
            final var input = SystemMutation.decodeTargetFrame(originalFrame); id = input.systemMutationId(); hash = input.mutationHash();
        }
        final var value = new TargetMigrationShadowProgress(plan.digest(), plan.migrationId(), frame.recordKind(), id, hash,
                Bytes.sha256(originalFrame), actual, prior, root, first, position, proofKind);
        value.requirePlan(plan); value.requireFrame(originalFrame); return value;
    }
    /** Source position/resource is the exact content reference; original Pin/custody must retain its full frame. */
    public void requireFrame(byte[] frame) {
        final var decoded = ShardLogFrame.decode(frame);
        if (decoded.recordKind() != recordKind || !Arrays.equals(Bytes.sha256(frame), originalFrameDigest)) {
            throw new IllegalArgumentException("shadow progress lost its complete original physical Source frame");
        }
        if (recordKind == ProtocolTuple.CLIENT_COMMAND) {
            final var command = CommandCodec.decodeFrame(frame);
            if (!command.shardId().equals(appliedAt.source().shardId()) || !Arrays.equals(command.commandId().bytes(), originalIdentity)
                    || !Arrays.equals(command.commandHash(), originalSemanticHash)
                    || !Arrays.equals(command.canonicalFrameDigest(), appliedAt.mutationDigest())) throw new IllegalArgumentException("shadow original Command changed");
        } else {
            final var input = SystemMutation.decodeTargetFrame(frame);
            if (!input.shardId().equals(appliedAt.source().shardId()) || !Arrays.equals(input.systemMutationId(), originalIdentity)
                    || !Arrays.equals(input.mutationHash(), originalSemanticHash)
                    || !Arrays.equals(Bytes.sha256(input.canonicalEnvelope()), appliedAt.mutationDigest())) throw new IllegalArgumentException("shadow original System Mutation changed");
        }
    }
    @Override public byte[] planDigest() { return Bytes.copy(planDigest); }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] originalIdentity() { return Bytes.copy(originalIdentity); }
    @Override public byte[] originalSemanticHash() { return Bytes.copy(originalSemanticHash); }
    @Override public byte[] originalFrameDigest() { return Bytes.copy(originalFrameDigest); }
    @Override public byte[] targetRootDigest() { return Bytes.copy(targetRootDigest); }
    @Override public byte[] targetFirstDigest() { return targetFirstDigest == null ? null : Bytes.copy(targetFirstDigest); }
    @Override public byte[] targetPositionDigest() { return Bytes.copy(targetPositionDigest); }
    public void requirePlan(TargetMigrationPlan plan) {
        if (!Arrays.equals(plan.digest(), planDigest) || !Arrays.equals(plan.migrationId(), migrationId)
                || !plan.baseline().source().sameSourceIdentity(appliedAt.source())
                || prior.compareTo(plan.baseline().source()) < 0
                || prior.compareTo(plan.baseline().source()) == 0
                        && !Arrays.equals(prior.canonicalBytes(), plan.baseline().source().canonicalBytes())) {
            throw new IllegalArgumentException("shadow progress changed original migration/physical Source");
        }
    }
    public static byte[] keyFor(ShardId shard) {
        return Bytes.concat(new byte[] {TargetKeyCodec.MIGRATION_SHADOW_PROGRESS_TAG, TargetKeyCodec.KEY_FORMAT}, TargetQuotaIdentity.shardBytes(shard));
    }
    public byte[] key() { return keyFor(appliedAt.source().shardId()); }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, planDigest); CanonicalProtobuf.bytes(out, 3, migrationId);
            CanonicalProtobuf.uint32(out, 4, recordKind); CanonicalProtobuf.bytes(out, 5, originalIdentity);
            CanonicalProtobuf.bytes(out, 6, originalSemanticHash); CanonicalProtobuf.bytes(out, 7, originalFrameDigest);
            CanonicalProtobuf.bytes(out, 8, appliedAt.canonicalBytes()); CanonicalProtobuf.bytes(out, 9, prior.canonicalBytes());
            CanonicalProtobuf.bytes(out, 10, targetRootDigest);
            if (targetFirstDigest != null) CanonicalProtobuf.bytes(out, 11, targetFirstDigest);
            CanonicalProtobuf.bytes(out, 12, targetPositionDigest);
            CanonicalProtobuf.uint32(out, 13, proofKind.wireValue());
        });
    }
    public byte[] digest() { return Bytes.sha256(DOMAIN, fields()); }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 14, digest()); }); }
    public static TargetMigrationShadowProgress decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 14, false, "migration shadow progress");
        final boolean first = f.size() == 14;
        QueryCodecSupport.requireNumbers(f, first ? new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14}
                : new int[] {1,2,3,4,5,6,7,8,9,10,12,13,14}, "migration shadow progress");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown migration shadow progress schema");
        final var value = new TargetMigrationShadowProgress(QueryCodecSupport.fixed(f.get(1), 2, 32), QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.uint32(f.get(3), 4), QueryCodecSupport.bytes(f.get(4), 5), QueryCodecSupport.fixed(f.get(5), 6, 32),
                QueryCodecSupport.fixed(f.get(6), 7, 32), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(8), 9)), QueryCodecSupport.fixed(f.get(9), 10, 32),
                first ? QueryCodecSupport.fixed(f.get(10), 11, 32) : null, QueryCodecSupport.fixed(f.get(first ? 11 : 10), 12, 32),
                LogicalProofKind.fromWire(QueryCodecSupport.uint32(f.get(first ? 12 : 11), 13)));
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.getLast(), 14, 32))) throw new IllegalArgumentException("migration shadow progress digest changed");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration shadow progress"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationShadowProgress value && Arrays.equals(canonicalBytes(), value.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
