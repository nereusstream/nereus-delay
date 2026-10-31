package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Durable before-SDK exact attempt. Its permanent phase conservatively includes possible append exposure. */
public record TargetMigrationActivationAttempt(
        byte[] planDigest,
        byte[] migrationId,
        TargetMigrationObjectReference candidate,
        PreparedControlOperation control,
        SystemMutation input,
        TargetQuotaMutation preparedAt) {
    public static final int VALUE_TYPE = 69;
    public static final int V1_MAX_PREPARED_BYTES = TargetMigrationActivateRequest.V1_MAX_CANONICAL_BYTES + 1024;
    public static final int MAX_PREPARED_BYTES = TargetMigrationActivateRequest.MAX_CANONICAL_BYTES + 1024;
    public static final int V1_MAX_CANONICAL_BYTES = V1_MAX_PREPARED_BYTES
            + TargetMigrationControlBody.LEGACY_MAX_INPUT_BYTES
            + TargetMigrationObjectReference.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + 160;
    public static final int MAX_CANONICAL_BYTES = V1_MAX_CANONICAL_BYTES
            + (MAX_PREPARED_BYTES - V1_MAX_PREPARED_BYTES)
            + (TargetMigrationControlBody.ACTIVATE_V2_MAX_INPUT_BYTES
                    - TargetMigrationControlBody.LEGACY_MAX_INPUT_BYTES)
            + (TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES - TargetQuotaMutation.MAX_CANONICAL_BYTES);
    private static final byte[] V2_DOMAIN = Bytes.utf8("nereus-delay-target-migration-activation-attempt-v2\0");
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-activation-attempt-v1\0");

    public enum Phase {
        APPEND_MAY_BE_EXPOSED
    }

    public TargetMigrationActivationAttempt {
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        Objects.requireNonNull(candidate);
        Objects.requireNonNull(control);
        Objects.requireNonNull(input);
        Objects.requireNonNull(preparedAt);
        final var body = TargetMigrationControlBody.decode(input.canonicalBody());
        body.requireInput(input);
        if (!(body.request() instanceof TargetMigrationActivateRequest request)
                || !Arrays.equals(planDigest, request.plan().digest())
                || !Arrays.equals(migrationId, request.plan().migrationId())
                || !candidate.equals(request.candidate())
                || control.targets().size() != 1
                || control.kind() != ControlOperationKind.ACTIVATE_TARGET_MIGRATION
                || control.canonicalBytes().length > MAX_PREPARED_BYTES
                || !preparedAt.migrationPreparation()
                || !Arrays.equals(preparedAt.mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))
                || !preparedAt
                        .source()
                        .sameSourceIdentity(request.plan().baseline().source())) {
            throw new IllegalArgumentException(
                    "durable migration attempt changed its exact registered frame/candidate/local purpose");
        }
        ControlTargetMutationBinding.validate(control, control.targets().getFirst(), input);
    }

    @Override
    public byte[] planDigest() {
        return Bytes.copy(planDigest);
    }

    @Override
    public byte[] migrationId() {
        return Bytes.copy(migrationId);
    }

    public int schemaVersion() {
        return ((TargetMigrationActivateRequest)
                        TargetMigrationControlBody.decode(input.canonicalBody()).request())
                .schemaVersion();
    }

    public Phase phase() {
        return Phase.APPEND_MAY_BE_EXPOSED;
    }

    public void requirePlan(TargetMigrationPlan plan) {
        if (!Arrays.equals(planDigest, plan.digest()) || !Arrays.equals(migrationId, plan.migrationId())) {
            throw new IllegalArgumentException("staged migration attempt changed original sealed plan");
        }
        final int order = preparedAt.source().compareTo(plan.baseline().source());
        if (order < 0
                || order == 0
                        && !Arrays.equals(
                                preparedAt.source().canonicalBytes(),
                                plan.baseline().source().canonicalBytes())) {
            throw new IllegalArgumentException("staged migration attempt precedes exact C");
        }
    }

    public static byte[] keyFor(byte[] migrationId) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.MIGRATION_ACTIVATION_ATTEMPT_TAG, TargetKeyCodec.KEY_FORMAT},
                TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID"));
    }

    public byte[] key() {
        return keyFor(migrationId);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion());
            CanonicalProtobuf.bytes(out, 2, planDigest);
            CanonicalProtobuf.bytes(out, 3, migrationId);
            CanonicalProtobuf.bytes(out, 4, candidate.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, control.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, input.canonicalEnvelope());
            CanonicalProtobuf.bytes(out, 7, preparedAt.canonicalBytes());
            CanonicalProtobuf.uint32(out, 8, 1);
        });
    }

    public byte[] digest() {
        return Bytes.sha256(schemaVersion() == 1 ? DOMAIN : V2_DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest());
        });
    }

    public static TargetMigrationActivationAttempt decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "migration activation attempt");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "migration activation attempt");
        if ((QueryCodecSupport.uint32(f.getFirst(), 1) < 1 || QueryCodecSupport.uint32(f.getFirst(), 1) > 2)
                || QueryCodecSupport.uint32(f.get(7), 8) != 1) {
            throw new IllegalArgumentException("unknown migration activation attempt schema/exposure phase");
        }
        final byte[] prepared = QueryCodecSupport.bytes(f.get(4), 5);
        if (prepared.length > MAX_PREPARED_BYTES) {
            throw new IllegalArgumentException("migration exact Prepared exceeds its fixed branch bound");
        }
        final var value = new TargetMigrationActivationAttempt(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                TargetMigrationObjectReference.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                PreparedControlOperation.decode(prepared),
                SystemMutation.decodeTargetEnvelope(
                        QueryCodecSupport.bytes(f.get(5), 6), SystemMutationType.APPLY_SHARD_CONTROL),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(6), 7)));
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != value.schemaVersion()
                || value.schemaVersion() == 1
                        && (encoded.length > V1_MAX_CANONICAL_BYTES || prepared.length > V1_MAX_PREPARED_BYTES)) {
            throw new IllegalArgumentException("migration attempt schema/A1 bound mismatch");
        }
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(8), 9, 32))) {
            throw new IllegalArgumentException("migration activation attempt digest changed");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration activation attempt");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationActivationAttempt value
                && Arrays.equals(canonicalBytes(), value.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest());
    }
}
