package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Original owner terminal/transfer observation; only that actual owner's verifier can authorize its use. */
public record TargetMigrationRetirementResourceReceipt(byte[] resourceId,
        TargetMigrationRetirementInventory.Disposition disposition, byte[] exactResourceDigest,
        byte[] durableOperationId, TargetQuotaIdentity originalOwner, CapacityVector releasedUsage,
        byte[] actualEvidence, TargetQuotaIdentity receiverOwner, List<TargetMigrationObjectReference> receiverMaterials) {
    public static final int MAX_EVIDENCE_BYTES = 1024 * 1024;
    public static final int MAX_MATERIALS = 32;
    public static final int MAX_CANONICAL_BYTES = MAX_EVIDENCE_BYTES
            + MAX_MATERIALS * (TargetMigrationObjectReference.MAX_CANONICAL_BYTES + 6)
            + 2 * TargetQuotaIdentity.MAX_CANONICAL_BYTES + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES + 192;
    public TargetMigrationRetirementResourceReceipt {
        resourceId = TargetCompatibilityCodec.assigned(resourceId, 32, "frozen retirement resource");
        exactResourceDigest = TargetCompatibilityCodec.assigned(exactResourceDigest, 32, "complete original retirement resource");
        durableOperationId = TargetCompatibilityCodec.assigned(durableOperationId, 32, "original durable resource operation");
        Objects.requireNonNull(disposition); Objects.requireNonNull(originalOwner); Objects.requireNonNull(releasedUsage);
        actualEvidence = TargetMigrationRetirementInventory.material(actualEvidence, MAX_EVIDENCE_BYTES, "actual original owner terminal/transfer receipt");
        receiverMaterials = List.copyOf(receiverMaterials);
        if (receiverMaterials.size() > MAX_MATERIALS) throw new IllegalArgumentException("excess receiver custody material");
        if (disposition == TargetMigrationRetirementInventory.Disposition.TRANSFER) {
            if (receiverOwner == null || receiverOwner.equals(originalOwner) || receiverMaterials.isEmpty()) throw new IllegalArgumentException("transfer requires actual protected receiver and complete material references");
        } else if (disposition == TargetMigrationRetirementInventory.Disposition.RETIRE) {
            if (receiverOwner != null || !receiverMaterials.isEmpty()) throw new IllegalArgumentException("retired resource cannot invent a receiver");
        } else if (!releasedUsage.isZero() || !originalOwner.equals(receiverOwner) || !receiverMaterials.isEmpty()) {
            throw new IllegalArgumentException("shared/permanent responsibility keeps its original owner and charge");
        }
        for (int i = 1; i < receiverMaterials.size(); i++)
            if (Arrays.compareUnsigned(receiverMaterials.get(i - 1).canonicalBytes(), receiverMaterials.get(i).canonicalBytes()) >= 0)
                throw new IllegalArgumentException("receiver material references must be unique and sorted");
    }
    @Override public byte[] resourceId() { return Bytes.copy(resourceId); }
    @Override public byte[] exactResourceDigest() { return Bytes.copy(exactResourceDigest); }
    @Override public byte[] durableOperationId() { return Bytes.copy(durableOperationId); }
    @Override public byte[] actualEvidence() { return Bytes.copy(actualEvidence); }
    public void requireResource(TargetMigrationRetirementInventory.Resource resource) {
        final boolean released = disposition == TargetMigrationRetirementInventory.Disposition.RETIRE
                || disposition == TargetMigrationRetirementInventory.Disposition.TRANSFER;
        if (!Arrays.equals(resourceId, resource.id()) || disposition != resource.disposition()
                || !Arrays.equals(exactResourceDigest, resource.digest()) || !originalOwner.equals(resource.originalOwner())
                || !(released ? resource.heldUsage() : CapacityVector.empty()).equals(releasedUsage))
            throw new IllegalArgumentException("resource receipt changed exact cut/disposition/original owner or its actual fee share");
    }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1); CanonicalProtobuf.bytes(out, 2, resourceId);
            CanonicalProtobuf.uint32(out, 3, disposition.wireValue()); CanonicalProtobuf.bytes(out, 4, exactResourceDigest);
            CanonicalProtobuf.bytes(out, 5, durableOperationId); CanonicalProtobuf.bytes(out, 6, originalOwner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, releasedUsage.canonicalBytes()); CanonicalProtobuf.bytes(out, 8, actualEvidence);
            if (receiverOwner != null) CanonicalProtobuf.bytes(out, 9, receiverOwner.canonicalBytes());
            for (var material : receiverMaterials) CanonicalProtobuf.bytes(out, 10, material.canonicalBytes());
        });
    }
    public static TargetMigrationRetirementResourceReceipt decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9 + MAX_MATERIALS, true, "migration resource receipt");
        if (f.size() < 8) throw new IllegalArgumentException("incomplete migration resource receipt");
        for (int i = 0; i < 8; i++) if (f.get(i).number() != i + 1) throw new IllegalArgumentException("unknown migration resource receipt fields");
        TargetMigrationRetirementInventory.version(f.getFirst()); int i = 8;
        final var receiver = i < f.size() && f.get(i).number() == 9 ? TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(i++), 9)) : null;
        final var materials = new ArrayList<TargetMigrationObjectReference>();
        for (; i < f.size(); i++) materials.add(TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(i), 10)));
        final var value = new TargetMigrationRetirementResourceReceipt(QueryCodecSupport.fixed(f.get(1), 2, 32),
                TargetMigrationRetirementInventory.Disposition.fromWire(QueryCodecSupport.uint32(f.get(2), 3)), QueryCodecSupport.fixed(f.get(3), 4, 32),
                QueryCodecSupport.fixed(f.get(4), 5, 32), TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                CapacityVector.decode(QueryCodecSupport.bytes(f.get(6), 7)), QueryCodecSupport.bytes(f.get(7), 8), receiver, materials);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration resource receipt"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationRetirementResourceReceipt v && Arrays.equals(canonicalBytes(), v.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(canonicalBytes()); }
}
