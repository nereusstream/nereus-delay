package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact immutable old-domain cut. References name original material; none conveys deletion authority. */
public record TargetMigrationRetirementInventory(CatalogDomain catalog, List<Resource> resources) {
    public static final int MAX_RESOURCES = 128;
    public static final int MAX_CANONICAL_BYTES = 96 * 1024 * 1024;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-retirement-inventory-v1\0");
    public enum Kind {
        SHARED_SOURCE, SHARED_REGISTRY, SHARED_CLIENT, SHARED_POOL, WRITER, READER, CHANNEL,
        LOCAL_COPY, CHECKPOINT, PAYLOAD, COUNTER, CUSTODY, HISTORY, TOMBSTONE;
        public int wireValue() { return ordinal() + 1; }
        static Kind fromWire(int value) {
            if (value < 1 || value > values().length) throw new IllegalArgumentException("unknown migration retirement resource kind");
            return values()[value - 1];
        }
    }
    public enum Disposition {
        SHARED_RETAINED, TRANSFER, RETIRE, PERMANENT;
        public int wireValue() { return ordinal() + 1; }
        static Disposition fromWire(int value) {
            if (value < 1 || value > values().length) throw new IllegalArgumentException("unknown migration retirement disposition");
            return values()[value - 1];
        }
    }
    public record CatalogDomain(String namespace, String rootKey, long originalVersion,
            byte[] publicationDigest, byte[] memberCut) {
        public static final int MAX_MEMBER_CUT_BYTES = 64 * 1024 * 1024;
        public static final int MAX_CANONICAL_BYTES = MAX_MEMBER_CUT_BYTES + 8300;
        public CatalogDomain {
            text(namespace, "original Catalog namespace"); text(rootKey, "original Catalog root key");
            if (originalVersion < 0) throw new IllegalArgumentException("Catalog original version must be actual");
            publicationDigest = TargetCompatibilityCodec.assigned(publicationDigest, 32, "original Catalog publication");
            memberCut = material(memberCut, MAX_MEMBER_CUT_BYTES, "complete frozen Catalog membership cut");
        }
        @Override public byte[] publicationDigest() { return Bytes.copy(publicationDigest); }
        @Override public byte[] memberCut() { return Bytes.copy(memberCut); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, Bytes.utf8(namespace));
                CanonicalProtobuf.bytes(out, 3, Bytes.utf8(rootKey)); CanonicalProtobuf.uint64(out, 4, originalVersion);
                CanonicalProtobuf.bytes(out, 5, publicationDigest); CanonicalProtobuf.bytes(out, 6, memberCut);
            });
        }
        public static CatalogDomain decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "migration Catalog domain");
            QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6}, "migration Catalog domain");
            version(f.getFirst());
            final var value = new CatalogDomain(new String(QueryCodecSupport.bytes(f.get(1), 2), StandardCharsets.UTF_8),
                    new String(QueryCodecSupport.bytes(f.get(2), 3), StandardCharsets.UTF_8), QueryCodecSupport.uint(f.get(3), 4),
                    QueryCodecSupport.fixed(f.get(4), 5, 32), QueryCodecSupport.bytes(f.get(5), 6));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration Catalog domain"); return value;
        }
        @Override public boolean equals(Object other) { return other instanceof CatalogDomain v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
        @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
    }
    /** heldUsage belongs to the original owner. Encoding it does not create a replacement counter or refund. */
    public record Resource(byte[] id, Kind kind, Disposition disposition, TargetQuotaIdentity originalOwner,
            byte[] exactIdentity, byte[] originalObligation, byte[] retention, CapacityVector heldUsage) {
        public static final int MAX_MATERIAL_BYTES = 64 * 1024;
        public static final int MAX_CANONICAL_BYTES = 3 * MAX_MATERIAL_BYTES + TargetQuotaIdentity.MAX_CANONICAL_BYTES
                + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES + 160;
        public Resource {
            id = TargetCompatibilityCodec.assigned(id, 32, "exact resource identity");
            Objects.requireNonNull(kind); Objects.requireNonNull(disposition); Objects.requireNonNull(originalOwner); Objects.requireNonNull(heldUsage);
            exactIdentity = material(exactIdentity, MAX_MATERIAL_BYTES, "exact original resource identity");
            originalObligation = material(originalObligation, MAX_MATERIAL_BYTES, "complete original resource obligations");
            retention = material(retention, MAX_MATERIAL_BYTES, "complete original resource retention");
            if (kind.ordinal() <= Kind.SHARED_POOL.ordinal() && disposition != Disposition.SHARED_RETAINED
                    || kind == Kind.TOMBSTONE && disposition != Disposition.PERMANENT)
                throw new IllegalArgumentException("shared Source/Registry/client/pool and permanent tombstones retain their original owner");
        }
        @Override public byte[] id() { return Bytes.copy(id); }
        @Override public byte[] exactIdentity() { return Bytes.copy(exactIdentity); }
        @Override public byte[] originalObligation() { return Bytes.copy(originalObligation); }
        @Override public byte[] retention() { return Bytes.copy(retention); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, id);
                CanonicalProtobuf.uint32(out, 3, kind.wireValue()); CanonicalProtobuf.uint32(out, 4, disposition.wireValue());
                CanonicalProtobuf.bytes(out, 5, originalOwner.canonicalBytes()); CanonicalProtobuf.bytes(out, 6, exactIdentity);
                CanonicalProtobuf.bytes(out, 7, originalObligation); CanonicalProtobuf.bytes(out, 8, retention);
                CanonicalProtobuf.bytes(out, 9, heldUsage.canonicalBytes());
            });
        }
        public byte[] digest() { return Bytes.sha256(DOMAIN, canonicalBytes()); }
        public static Resource decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "migration retirement resource");
            QueryCodecSupport.requireNumbers(f, new int[] {1,2,3,4,5,6,7,8,9}, "migration retirement resource"); version(f.getFirst());
            final var value = new Resource(QueryCodecSupport.fixed(f.get(1), 2, 32), Kind.fromWire(QueryCodecSupport.uint32(f.get(2), 3)),
                    Disposition.fromWire(QueryCodecSupport.uint32(f.get(3), 4)), TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                    QueryCodecSupport.bytes(f.get(5), 6), QueryCodecSupport.bytes(f.get(6), 7), QueryCodecSupport.bytes(f.get(7), 8),
                    CapacityVector.decode(QueryCodecSupport.bytes(f.get(8), 9)));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration retirement resource"); return value;
        }
        @Override public boolean equals(Object other) { return other instanceof Resource v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
        @Override public int hashCode() { return Arrays.hashCode(digest()); }
    }
    public TargetMigrationRetirementInventory {
        Objects.requireNonNull(catalog); resources = List.copyOf(resources);
        if (resources.isEmpty() || resources.size() > MAX_RESOURCES) throw new IllegalArgumentException("finite complete retirement resource inventory required");
        byte[] previous = null; long bytes = catalog.canonicalBytes().length + 32L;
        for (var resource : resources) {
            if (previous != null && Arrays.compareUnsigned(previous, resource.id()) >= 0)
                throw new IllegalArgumentException("retirement resources must be uniquely sorted by exact identity");
            previous = resource.id(); bytes = Math.addExact(bytes, resource.canonicalBytes().length + 6L);
        }
        if (bytes > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("retirement inventory exceeds the operation capacity");
    }
    public Resource resource(byte[] id) {
        return resources.stream().filter(r -> Arrays.equals(r.id(), id)).findFirst().orElseThrow(
                () -> new IllegalArgumentException("resource is outside frozen retirement cut"));
    }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, catalog.canonicalBytes());
            for (var resource : resources) CanonicalProtobuf.bytes(out, 3, resource.canonicalBytes());
        });
    }
    public byte[] digest() { return Bytes.sha256(DOMAIN, canonicalBytes()); }
    public static TargetMigrationRetirementInventory decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, MAX_RESOURCES + 2, true, "migration retirement inventory");
        if (f.size() < 3 || f.getFirst().number() != 1 || f.get(1).number() != 2) throw new IllegalArgumentException("incomplete retirement inventory");
        version(f.getFirst()); final var resources = new ArrayList<Resource>();
        for (int i = 2; i < f.size(); i++) resources.add(Resource.decode(QueryCodecSupport.bytes(f.get(i), 3)));
        final var value = new TargetMigrationRetirementInventory(CatalogDomain.decode(QueryCodecSupport.bytes(f.get(1), 2)), resources);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration retirement inventory"); return value;
    }
    static byte[] material(byte[] value, int maximum, String label) {
        Objects.requireNonNull(value, label);
        if (value.length == 0 || value.length > maximum) throw new IllegalArgumentException(label + " must be complete, nonempty and bounded");
        return Bytes.copy(value);
    }
    static void version(CanonicalProtobuf.Reader.Field field) {
        if (QueryCodecSupport.uint32(field, 1) != 1) throw new IllegalArgumentException("unknown migration retirement schema");
    }
    private static void text(String value, String label) {
        Objects.requireNonNull(value, label);
        if (value.isBlank() || Bytes.utf8(value).length > 4096 || value.indexOf('\0') >= 0) throw new IllegalArgumentException(label + " is not exact bounded text");
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationRetirementInventory v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
