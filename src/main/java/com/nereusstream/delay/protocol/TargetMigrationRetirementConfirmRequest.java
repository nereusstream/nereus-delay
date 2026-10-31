package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Coverage of every frozen member and all original closure obligations, after the actual Catalog barrier. */
public record TargetMigrationRetirementConfirmRequest(TargetQuotaScope scope, byte[] migrationId, byte[] planDigest,
        byte[] intentDigest, TargetQuotaMutation intentAt, TargetMigrationCatalogBarrierReceipt catalogBarrier,
        List<TargetMigrationRetirementResourceReceipt> receipts, Closure closure, TargetQuotaMutation takeoverAt) implements TargetMigrationRequest {
    public static final int CONTROL_KIND = 37;
    public static final int MAX_CANONICAL_BYTES = 224 * 1024 * 1024;
    public record Closure(byte[] noFuture, byte[] pin, byte[] query, byte[] reader, byte[] retention, byte[] sourceHistory, byte[] fees) {
        public static final int MAX_MATERIAL_BYTES = 1024 * 1024;
        public static final int MAX_CANONICAL_BYTES = 7 * (MAX_MATERIAL_BYTES + 6) + 8;
        public Closure {
            noFuture = copy(noFuture, "no-future"); pin = copy(pin, "Pin"); query = copy(query, "query");
            reader = copy(reader, "reader"); retention = copy(retention, "retention");
            sourceHistory = copy(sourceHistory, "Source history"); fees = copy(fees, "original owner fee closure");
        }
        private static byte[] copy(byte[] value, String label) { return TargetMigrationRetirementInventory.material(value, MAX_MATERIAL_BYTES, label); }
        @Override public byte[] noFuture() { return Bytes.copy(noFuture); }
        @Override public byte[] pin() { return Bytes.copy(pin); }
        @Override public byte[] query() { return Bytes.copy(query); }
        @Override public byte[] reader() { return Bytes.copy(reader); }
        @Override public byte[] retention() { return Bytes.copy(retention); }
        @Override public byte[] sourceHistory() { return Bytes.copy(sourceHistory); }
        @Override public byte[] fees() { return Bytes.copy(fees); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, noFuture); CanonicalProtobuf.bytes(out, 3, pin);
                CanonicalProtobuf.bytes(out, 4, query); CanonicalProtobuf.bytes(out, 5, reader); CanonicalProtobuf.bytes(out, 6, retention);
                CanonicalProtobuf.bytes(out, 7, sourceHistory); CanonicalProtobuf.bytes(out, 8, fees);
            });
        }
        public static Closure decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "migration retirement closure");
            QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8}, "migration retirement closure"); TargetMigrationRetirementInventory.version(f.getFirst());
            final var value = new Closure(QueryCodecSupport.bytes(f.get(1), 2), QueryCodecSupport.bytes(f.get(2), 3), QueryCodecSupport.bytes(f.get(3), 4),
                    QueryCodecSupport.bytes(f.get(4), 5), QueryCodecSupport.bytes(f.get(5), 6), QueryCodecSupport.bytes(f.get(6), 7), QueryCodecSupport.bytes(f.get(7), 8));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration retirement closure"); return value;
        }
        @Override public boolean equals(Object other) { return other instanceof Closure v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
        @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
    }
    public TargetMigrationRetirementConfirmRequest(TargetQuotaScope scope, byte[] migrationId, byte[] planDigest,
            byte[] intentDigest, TargetQuotaMutation intentAt, TargetMigrationCatalogBarrierReceipt catalogBarrier,
            List<TargetMigrationRetirementResourceReceipt> receipts, Closure closure) {
        this(scope, migrationId, planDigest, intentDigest, intentAt, catalogBarrier, receipts, closure, null);
    }
    public TargetMigrationRetirementConfirmRequest {
        Objects.requireNonNull(scope); Objects.requireNonNull(intentAt).requireSourceApplied(); Objects.requireNonNull(catalogBarrier); Objects.requireNonNull(closure);
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        intentDigest = TargetCompatibilityCodec.assigned(intentDigest, 32, "original Source retirement intent");
        receipts = List.copyOf(receipts);
        if (takeoverAt != null) { takeoverAt.requireSourceApplied(); takeoverAt.requireAfter(intentAt); }
        if (scope.target() != null || !scope.shard().equals(intentAt.source().shardId()) || receipts.isEmpty()
                || receipts.size() > TargetMigrationRetirementInventory.MAX_RESOURCES
                || !Arrays.equals(intentDigest, catalogBarrier.intentDigest()) || !intentAt.equals(catalogBarrier.intentAt()))
            throw new IllegalArgumentException("retirement confirmation changed original Source intent/barrier/finite coverage");
        long bytes = catalogBarrier.canonicalBytes().length + (long) closure.canonicalBytes().length + 1024;
        for (int i = 0; i < receipts.size(); i++) {
            if (i > 0 && Arrays.compareUnsigned(receipts.get(i - 1).resourceId(), receipts.get(i).resourceId()) >= 0)
                throw new IllegalArgumentException("resource receipt coverage must be uniquely sorted");
            bytes = Math.addExact(bytes, receipts.get(i).canonicalBytes().length + 6L);
        }
        if (bytes > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("retirement confirmation exceeds operation capacity");
    }
    @Override public byte[] migrationId() { return Bytes.copy(migrationId); }
    @Override public byte[] planDigest() { return Bytes.copy(planDigest); }
    @Override public byte[] intentDigest() { return Bytes.copy(intentDigest); }
    @Override public ControlOperationKind operationKind() { return ControlOperationKind.CONFIRM_TARGET_MIGRATION_RETIRED; }
    @Override public int controlKind() { return CONTROL_KIND; }
    @Override public ProtocolTuple tuple() { return ProtocolTuple.targetMigrationRetireConfirm(); }
    public void requireIntent(TargetMigrationRetirementIntentRequest intent, TargetQuotaMutation at) {
        catalogBarrier.requireIntent(intent, at);
        if (!scope.equals(intent.scope()) || !Arrays.equals(migrationId, intent.migrationId()) || !Arrays.equals(planDigest, intent.planDigest())
                || !Arrays.equals(intentDigest, intent.digest()) || !intentAt.equals(at) || receipts.size() != intent.inventory().resources().size())
            throw new IllegalArgumentException("retirement confirmation changed full frozen intent/coverage");
        for (int i = 0; i < receipts.size(); i++) receipts.get(i).requireResource(intent.inventory().resources().get(i));
    }
    @Override public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, takeoverAt == null ? 1 : 2); CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, migrationId); CanonicalProtobuf.bytes(out, 4, planDigest); CanonicalProtobuf.bytes(out, 5, intentDigest);
            CanonicalProtobuf.bytes(out, 6, intentAt.canonicalBytes()); CanonicalProtobuf.bytes(out, 7, catalogBarrier.canonicalBytes());
            for (var receipt : receipts) CanonicalProtobuf.bytes(out, 8, receipt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, closure.canonicalBytes());
            if (takeoverAt != null) CanonicalProtobuf.bytes(out, 10, takeoverAt.canonicalBytes());
        });
    }
    public static TargetMigrationRetirementConfirmRequest decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, TargetMigrationRetirementInventory.MAX_RESOURCES + 9, true, "migration retirement confirmation");
        if (f.size() < 9) throw new IllegalArgumentException("incomplete migration retirement confirmation");
        for (int i = 0; i < 7; i++) if (f.get(i).number() != i + 1) throw new IllegalArgumentException("unknown migration retirement confirmation fields");
        final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (version != 1 && version != 2) throw new IllegalArgumentException("unknown retirement confirmation schema");
        final var receipts = new ArrayList<TargetMigrationRetirementResourceReceipt>();
        for (int i = 7; i < f.size() - (version == 2 ? 2 : 1); i++) receipts.add(TargetMigrationRetirementResourceReceipt.decode(QueryCodecSupport.bytes(f.get(i), 8)));
        final var value = new TargetMigrationRetirementConfirmRequest(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32), QueryCodecSupport.fixed(f.get(3), 4, 32), QueryCodecSupport.fixed(f.get(4), 5, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)), TargetMigrationCatalogBarrierReceipt.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                receipts, Closure.decode(QueryCodecSupport.bytes(f.get(f.size() - (version == 2 ? 2 : 1)), 9)),
                version == 2 ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.getLast(), 10)) : null);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration retirement confirmation"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationRetirementConfirmRequest v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
}
