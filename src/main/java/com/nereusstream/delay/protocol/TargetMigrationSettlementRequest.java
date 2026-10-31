package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** One exact legacy responsibility settlement. Full terminal/raw and no-future custody remain independently required. */
public record TargetMigrationSettlementRequest(TargetQuotaScope scope, byte[] migrationId, byte[] planDigest,
        DelayMessageId messageId, int originalGeneration, byte[] expectedGuardDigest,
        TargetMigrationRecordReference originalTerminal, SourcePosition originalTerminalSource,
        byte[] originalTerminalDigest, TargetMigrationObjectReference noFutureProof, TargetMigrationCustodyGuard.Kind guardKind)
        implements TargetMigrationRequest {
    public static final int CONTROL_KIND = 35;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaScope.MAX_CANONICAL_BYTES
            + TargetMigrationRecordReference.MAX_CANONICAL_BYTES + TargetSourcePosition.MAX_CANONICAL_BYTES
            + TargetMigrationObjectReference.MAX_CANONICAL_BYTES + 256;
    public TargetMigrationSettlementRequest {
        Objects.requireNonNull(scope); Objects.requireNonNull(messageId); Objects.requireNonNull(originalTerminal); Objects.requireNonNull(noFutureProof); Objects.requireNonNull(guardKind);
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        expectedGuardDigest = TargetCompatibilityCodec.assigned(expectedGuardDigest, 32, "original held custody guard");
        originalTerminalDigest = TargetCompatibilityCodec.assigned(originalTerminalDigest, 32, "complete original terminal record digest");
        originalTerminalSource = TargetSourcePosition.requireBounded(originalTerminalSource);
        if (scope.target() != null || guardKind == TargetMigrationCustodyGuard.Kind.IDENTITY && originalGeneration != 0
                || !scope.shard().equals(messageId.routingId().shardId())
                || !scope.shard().equals(originalTerminalSource.shardId())) {
            throw new IllegalArgumentException("migration settlement changed original Source/ID/generation");
        }
    }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] planDigest() { return Bytes.copy(planDigest); }
    @Override public byte[] expectedGuardDigest() { return Bytes.copy(expectedGuardDigest); }
    @Override public byte[] originalTerminalDigest() { return Bytes.copy(originalTerminalDigest); }
    @Override public ControlOperationKind operationKind() { return ControlOperationKind.SETTLE_TARGET_MIGRATION_CUSTODY; }
    @Override public int controlKind() { return CONTROL_KIND; }
    @Override public ProtocolTuple tuple() { return ProtocolTuple.targetMigrationSettlement(); }
    @Override public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, migrationId); CanonicalProtobuf.bytes(out, 4, planDigest);
            CanonicalProtobuf.bytes(out, 5, messageId.bytes()); CanonicalProtobuf.uint32Bits(out, 6, originalGeneration);
            CanonicalProtobuf.bytes(out, 7, expectedGuardDigest); CanonicalProtobuf.bytes(out, 8, originalTerminal.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, originalTerminalSource.canonicalBytes()); CanonicalProtobuf.bytes(out, 10, originalTerminalDigest);
            CanonicalProtobuf.bytes(out, 11, noFutureProof.canonicalBytes());
            CanonicalProtobuf.uint32(out, 12, guardKind.wireValue());
        });
    }
    public static TargetMigrationSettlementRequest decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 12, false, "migration settlement request");
        QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9,10,11,12}, "migration settlement request");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown migration settlement schema");
        final var value = new TargetMigrationSettlementRequest(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32), QueryCodecSupport.fixed(f.get(3), 4, 32),
                new DelayMessageId(QueryCodecSupport.fixed(f.get(4), 5, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32(f.get(5), 6), QueryCodecSupport.fixed(f.get(6), 7, 32),
                TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(8), 9)), QueryCodecSupport.fixed(f.get(9), 10, 32),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(10), 11)),
                TargetMigrationCustodyGuard.Kind.fromWire(QueryCodecSupport.uint32(f.get(11), 12)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration settlement request"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationSettlementRequest value && Arrays.equals(canonicalBytes(), value.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
}
