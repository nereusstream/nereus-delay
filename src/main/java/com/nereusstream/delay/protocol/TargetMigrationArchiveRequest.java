package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One original retirement management purpose, with two closed bounded Source actions. */
public record TargetMigrationArchiveRequest(TargetQuotaScope scope, byte[] lineage, byte[] migrationId, byte[] planDigest,
        TargetQuotaMutation activation, byte[] intentDigest, TargetQuotaMutation intentAt,
        byte[] expectedStateDigest, byte[] expectedArchiveRoot, Action action) implements TargetMigrationRequest {
    public static final int CONTROL_KIND = 39;
    public static final int MAX_CANONICAL_BYTES = 32 * 1024 * 1024;
    public static final int MAX_ROWS = 32;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-migration-logical-first-archive-v1\0");
    public sealed interface Action permits CopyBatch, Takeover {
        long generation(); TargetQuotaMutation shadowCut(); TargetQuotaMutation coveredThrough();
        TargetMigrationArchiveCoverage coverage(); int wireValue(); byte[] canonicalBytes();
    }
    public record CopyBatch(long generation, TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough,
            List<TargetMigrationArchiveEntry> rows, TargetMigrationArchiveCoverage coverage,
            TargetMigrationObjectReference receivedInventory) implements Action {
        public CopyBatch(long generation, TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough,
                List<TargetMigrationArchiveEntry> rows, TargetMigrationArchiveCoverage coverage) {
            this(generation, shadowCut, coveredThrough, rows, coverage, null);
        }
        public CopyBatch {
            bound(generation, shadowCut, coveredThrough); Objects.requireNonNull(coverage); rows = List.copyOf(rows);
            if (rows.size() > MAX_ROWS) throw new IllegalArgumentException("archive COPY_BATCH exceeds bounded Source rows");
            long bytes = coverage.canonicalBytes().length + 2L * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 256;
            for (int i = 0; i < rows.size(); i++) {
                if (i > 0 && Arrays.compareUnsigned(rows.get(i - 1).originalKey(), rows.get(i).originalKey()) >= 0)
                    throw new IllegalArgumentException("archive batch original namespaces/keys must be unique and sorted");
                rows.get(i).provenanceCut().requireAtOrBefore(shadowCut);
                bytes = Math.addExact(bytes, rows.get(i).canonicalBytes().length + 6L);
            }
            if (bytes > MAX_CANONICAL_BYTES - 4096) throw new IllegalArgumentException("archive batch exceeds complete Source codec capacity");
        }
        @Override public int wireValue() { return 1; }
        @Override public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.uint64Bits(out, 2, generation);
            CanonicalProtobuf.bytes(out, 3, shadowCut.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, coveredThrough.canonicalBytes());
            for (var row : rows) CanonicalProtobuf.bytes(out, 5, row.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, coverage.canonicalBytes());
            if (receivedInventory != null) CanonicalProtobuf.bytes(out, 7, receivedInventory.canonicalBytes());
        }); }
        static CopyBatch decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, MAX_ROWS + 6, true, "archive COPY_BATCH");
            if (f.size() < 5) throw new IllegalArgumentException("incomplete archive COPY_BATCH");
            for (int i = 0; i < 4; i++) if (f.get(i).number() != i + 1) throw new IllegalArgumentException("unknown archive copy fields");
            TargetMigrationRetirementInventory.version(f.getFirst()); final var rows = new ArrayList<TargetMigrationArchiveEntry>();
            final boolean inventory = f.getLast().number() == 7;
            for (int i = 4; i < f.size() - (inventory ? 2 : 1); i++) rows.add(TargetMigrationArchiveEntry.decode(QueryCodecSupport.bytes(f.get(i), 5)));
            final var value = new CopyBatch(QueryCodecSupport.uint64Bits(f.get(1), 2), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)), rows, TargetMigrationArchiveCoverage.decode(QueryCodecSupport.bytes(f.get(f.size() - (inventory ? 2 : 1)), 6)),
                    inventory ? TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.getLast(), 7)) : null);
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "archive COPY_BATCH"); return value;
        }
    }
    public record Takeover(long generation, TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough,
            TargetMigrationArchiveCoverage coverage, TargetMigrationArchiveReceipt receipt) implements Action {
        public Takeover {
            bound(generation, shadowCut, coveredThrough); Objects.requireNonNull(coverage); Objects.requireNonNull(receipt);
            if (!coverage.complete() || !coverage.equals(receipt.coverage()) || !shadowCut.equals(receipt.shadowCut())
                    || !coveredThrough.equals(receipt.coveredThrough())) throw new IllegalArgumentException("archive takeover requires exact complete received coverage");
        }
        @Override public int wireValue() { return 2; }
        @Override public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.uint64Bits(out, 2, generation);
            CanonicalProtobuf.bytes(out, 3, shadowCut.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, coveredThrough.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, coverage.canonicalBytes()); CanonicalProtobuf.bytes(out, 6, receipt.canonicalBytes());
        }); }
        static Takeover decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "archive TAKEOVER");
            QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6}, "archive TAKEOVER"); TargetMigrationRetirementInventory.version(f.getFirst());
            final var value = new Takeover(QueryCodecSupport.uint64Bits(f.get(1), 2), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(3), 4)), TargetMigrationArchiveCoverage.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                    TargetMigrationArchiveReceipt.decode(QueryCodecSupport.bytes(f.get(5), 6)));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "archive TAKEOVER"); return value;
        }
    }
    private static void bound(long generation, TargetQuotaMutation shadowCut, TargetQuotaMutation coveredThrough) {
        if (generation == 0) throw new IllegalArgumentException("archive generation must be assigned");
        Objects.requireNonNull(shadowCut).requireSourceApplied(); Objects.requireNonNull(coveredThrough).requireSourceApplied();
        shadowCut.requireAtOrBefore(coveredThrough);
    }
    public TargetMigrationArchiveRequest {
        Objects.requireNonNull(scope); Objects.requireNonNull(activation).requireSourceApplied(); Objects.requireNonNull(intentAt).requireSourceApplied(); Objects.requireNonNull(action);
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "new archive lineage"); migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan"); intentDigest = TargetCompatibilityCodec.assigned(intentDigest, 32, "original Source retirement intent");
        expectedStateDigest = expectedStateDigest == null ? null : TargetCompatibilityCodec.assigned(expectedStateDigest, 32, "exact prior archive state");
        expectedArchiveRoot = TargetCompatibilityCodec.assigned(expectedArchiveRoot, 32, "exact prior archive content root");
        if (scope.target() != null || !scope.shard().equals(activation.source().shardId()) || !scope.shard().equals(intentAt.source().shardId())
                || (action.generation() == 1) != (expectedStateDigest == null)) throw new IllegalArgumentException("archive changed original Shard/Source/predecessor");
        intentAt.requireAfter(activation); intentAt.requireAtOrBefore(action.coveredThrough());
        if (action instanceof Takeover takeover && (!Arrays.equals(intentDigest, takeover.receipt().intentDigest())
                || !intentAt.equals(takeover.receipt().intentAt()) || !Arrays.equals(expectedArchiveRoot, takeover.receipt().archiveRoot())))
            throw new IllegalArgumentException("takeover changed original intent or new owner's exact received root");
        if (action.canonicalBytes().length > MAX_CANONICAL_BYTES - 4096) throw new IllegalArgumentException("archive Source action exceeds finite request bound");
    }
    @Override public byte[] lineage() { return Bytes.copy(lineage); }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] planDigest() { return Bytes.copy(planDigest); }
    @Override public byte[] intentDigest() { return Bytes.copy(intentDigest); }
    @Override public byte[] expectedStateDigest() { return expectedStateDigest == null ? null : Bytes.copy(expectedStateDigest); }
    @Override public byte[] expectedArchiveRoot() { return Bytes.copy(expectedArchiveRoot); }
    @Override public ControlOperationKind operationKind() { return ControlOperationKind.MIGRATE_TARGET_LOGICAL_FIRST_ARCHIVE; }
    @Override public int controlKind() { return CONTROL_KIND; }
    @Override public ProtocolTuple tuple() { return ProtocolTuple.targetMigrationLogicalFirstArchive(); }
    public boolean takeover() { return action instanceof Takeover; }
    public CopyBatch copyBatch() { return action instanceof CopyBatch copy ? copy : null; }
    public TargetMigrationArchiveCoverage coverage() { return action.coverage(); }
    public TargetQuotaMutation coveredThrough() { return action.coveredThrough(); }
    public TargetQuotaMutation shadowCut() { return action.shadowCut(); }
    public long generation() { return action.generation(); }
    public byte[] resultingRoot() {
        byte[] digest = expectedArchiveRoot;
        if (action instanceof CopyBatch copy) for (var row : copy.rows()) digest = appendRoot(digest, row);
        return Bytes.copy(digest);
    }
    public static byte[] appendRoot(byte[] previous, TargetMigrationArchiveEntry entry) {
        return Bytes.sha256(DOMAIN, TargetCompatibilityCodec.assigned(previous, 32, "exact preceding archive root"), entry.digest());
    }
    public static byte[] genesisRoot(byte[] intentDigest) { return Bytes.sha256(DOMAIN, TargetCompatibilityCodec.assigned(intentDigest, 32, "original retirement intent")); }
    public void requireRetirement(TargetMigrationRoot root, TargetMigrationRetirementRecord retirement) {
        retirement.requireRoot(root);
        if (!scope.equals(root.plan().scope()) || !Arrays.equals(lineage, root.plan().newLineage())
                || !Arrays.equals(migrationId, root.plan().migrationId()) || !Arrays.equals(planDigest, root.plan().digest())
                || !activation.equals(root.activatedAt()) || !Arrays.equals(intentDigest, retirement.intention().digest()) || !intentAt.equals(retirement.intentAt()))
            throw new IllegalStateException("archive operation changed exact actual A/original RETIRING intent");
    }
    @Override public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> {
        CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes()); CanonicalProtobuf.bytes(out, 3, lineage);
        CanonicalProtobuf.bytes(out, 4, migrationId); CanonicalProtobuf.bytes(out, 5, planDigest); CanonicalProtobuf.bytes(out, 6, activation.canonicalBytes());
        CanonicalProtobuf.bytes(out, 7, intentDigest); CanonicalProtobuf.bytes(out, 8, intentAt.canonicalBytes());
        if (expectedStateDigest != null) CanonicalProtobuf.bytes(out, 9, expectedStateDigest);
        CanonicalProtobuf.bytes(out, 10, expectedArchiveRoot); CanonicalProtobuf.uint32(out, 11, action.wireValue()); CanonicalProtobuf.bytes(out, 12, action.canonicalBytes());
    }); }
    public static TargetMigrationArchiveRequest decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 12, false, "migration archive request");
        final boolean prior = f.size() == 12; QueryCodecSupport.requireNumbers(f, prior ? new int[] {1,2,3,4,5,6,7,8,9,10,11,12}
                : new int[] {1,2,3,4,5,6,7,8,10,11,12}, "migration archive request"); TargetMigrationRetirementInventory.version(f.getFirst()); int i = 8;
        final byte[] expected = prior ? QueryCodecSupport.fixed(f.get(i++), 9, 32) : null;
        final byte[] archive = QueryCodecSupport.fixed(f.get(i++), 10, 32); final int kind = QueryCodecSupport.uint32(f.get(i++), 11);
        final byte[] action = QueryCodecSupport.bytes(f.get(i), 12);
        final Action selected = switch (kind) { case 1 -> CopyBatch.decode(action); case 2 -> Takeover.decode(action); default -> throw new IllegalArgumentException("unknown archive management action"); };
        final var value = new TargetMigrationArchiveRequest(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)), QueryCodecSupport.fixed(f.get(2), 3, 16),
                QueryCodecSupport.fixed(f.get(3), 4, 32), QueryCodecSupport.fixed(f.get(4), 5, 32), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                QueryCodecSupport.fixed(f.get(6), 7, 32), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(7), 8)), expected, archive, selected);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration archive request"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationArchiveRequest v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
}
