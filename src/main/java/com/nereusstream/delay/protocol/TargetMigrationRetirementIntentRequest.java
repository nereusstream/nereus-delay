package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Irreversible freeze of precisely the old domain of the actual original Source A. */
public record TargetMigrationRetirementIntentRequest(
        TargetQuotaScope scope,
        byte[] migrationId,
        byte[] planDigest,
        TargetQuotaMutation activation,
        TargetMigrationRetirementInventory inventory,
        TargetMigrationObjectReference eligibleReaders)
        implements TargetMigrationRequest {
    public static final int CONTROL_KIND = 36;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaScope.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetMigrationRetirementInventory.MAX_CANONICAL_BYTES
            + TargetMigrationObjectReference.MAX_CANONICAL_BYTES
            + 134;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-retire-intent-v1\0");

    public TargetMigrationRetirementIntentRequest {
        Objects.requireNonNull(scope);
        Objects.requireNonNull(activation).requireSourceApplied();
        Objects.requireNonNull(inventory);
        Objects.requireNonNull(eligibleReaders);
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        if (scope.target() != null || !scope.shard().equals(activation.source().shardId())) {
            throw new IllegalArgumentException("retirement changed actual Source A/scope");
        }
        for (var resource : inventory.resources()) {
            if (!resource.originalOwner().shard().equals(scope.shard())) {
                throw new IllegalArgumentException("old-domain resource owner belongs to another Source");
            }
        }
    }

    @Override
    public byte[] migrationId() {
        return Bytes.copy(migrationId);
    }

    @Override
    public byte[] planDigest() {
        return Bytes.copy(planDigest);
    }

    @Override
    public ControlOperationKind operationKind() {
        return ControlOperationKind.RETIRE_TARGET_MIGRATION;
    }

    @Override
    public int controlKind() {
        return CONTROL_KIND;
    }

    @Override
    public ProtocolTuple tuple() {
        return ProtocolTuple.targetMigrationRetireIntent();
    }

    public void requireRoot(TargetMigrationRoot root) {
        final var plan = root.plan();
        if (!root.activated()
                || !scope.equals(plan.scope())
                || !Arrays.equals(migrationId, plan.migrationId())
                || !Arrays.equals(planDigest, plan.digest())
                || !activation.equals(root.activatedAt())) {
            throw new IllegalStateException("retirement changed original migration plan/Store/lineage/actual A");
        }
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, migrationId);
            CanonicalProtobuf.bytes(out, 4, planDigest);
            CanonicalProtobuf.bytes(out, 5, activation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, inventory.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, eligibleReaders.canonicalBytes());
        });
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, canonicalBytes());
    }

    public static TargetMigrationRetirementIntentRequest decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "migration retirement intent");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6, 7}, "migration retirement intent");
        TargetMigrationRetirementInventory.version(f.getFirst());
        final var value = new TargetMigrationRetirementIntentRequest(
                TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.fixed(f.get(3), 4, 32),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                TargetMigrationRetirementInventory.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(6), 7)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration retirement intent");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationRetirementIntentRequest v
                && Arrays.equals(canonicalBytes(), v.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest());
    }
}
