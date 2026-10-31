package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Exact sealed MIGRATION_PREPARED candidate; does not bind the post-A Manifest containing this activation. */
public record TargetMigrationActivateRequest(
        TargetMigrationPlan plan,
        TargetMigrationObjectReference candidate,
        TargetMigrationRootIssuerAuthorization rootIssuerAuthorization)
        implements TargetMigrationRequest {
    public TargetMigrationActivateRequest(TargetMigrationPlan plan, TargetMigrationObjectReference candidate) {
        this(plan, candidate, null);
    }

    public static final int CONTROL_KIND = 34;
    public static final int V1_MAX_CANONICAL_BYTES =
            TargetMigrationPlan.MAX_CANONICAL_BYTES + TargetMigrationObjectReference.MAX_CANONICAL_BYTES + 14;
    public static final int MAX_CANONICAL_BYTES =
            V1_MAX_CANONICAL_BYTES + TargetMigrationRootIssuerAuthorization.MAX_BYTES + 6;

    public TargetMigrationActivateRequest {
        Objects.requireNonNull(plan);
        Objects.requireNonNull(candidate);
        if (rootIssuerAuthorization != null) {
            rootIssuerAuthorization.requirePlan(plan);
        }
        if (!Arrays.equals(plan.newLineage(), candidate.resource().recoveryLineageId())
                || plan.oldManifest().equals(candidate)) {
            throw new IllegalArgumentException(
                    "migration ACTIVATE candidate changed its independent new lineage/image");
        }
    }

    @Override
    public TargetQuotaScope scope() {
        return plan.scope();
    }

    @Override
    public ControlOperationKind operationKind() {
        return ControlOperationKind.ACTIVATE_TARGET_MIGRATION;
    }

    @Override
    public int controlKind() {
        return CONTROL_KIND;
    }

    @Override
    public ProtocolTuple tuple() {
        return rootIssuerAuthorization == null
                ? ProtocolTuple.targetMigrationActivate()
                : ProtocolTuple.targetMigrationActivateRootIssuer();
    }

    public int requiredPayloadOwnerSchema() {
        return rootIssuerAuthorization == null ? 1 : 2;
    }

    public int schemaVersion() {
        return rootIssuerAuthorization == null ? 1 : 2;
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion());
            CanonicalProtobuf.bytes(out, 2, plan.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, candidate.canonicalBytes());
            if (rootIssuerAuthorization != null) {
                CanonicalProtobuf.bytes(out, 4, rootIssuerAuthorization.canonicalBytes());
            }
        });
    }

    public static TargetMigrationActivateRequest decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4, false, "migration ACTIVATE request");
        QueryCodecSupport.requireNumbers(
                f, f.size() == 3 ? new int[] {1, 2, 3} : new int[] {1, 2, 3, 4}, "migration ACTIVATE request");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != (f.size() == 3 ? 1 : 2)) {
            throw new IllegalArgumentException("unknown migration ACTIVATE request schema");
        }
        final var value = new TargetMigrationActivateRequest(
                TargetMigrationPlan.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                f.size() == 4
                        ? TargetMigrationRootIssuerAuthorization.decode(QueryCodecSupport.bytes(f.get(3), 4))
                        : null);
        if (value.schemaVersion() == 1 && encoded.length > V1_MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("A1 request exceeds original fixed bound");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration ACTIVATE request");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationActivateRequest value
                && Arrays.equals(canonicalBytes(), value.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
