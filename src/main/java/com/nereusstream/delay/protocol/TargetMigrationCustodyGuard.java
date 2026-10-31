package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Durable per-ID no-send responsibility. Settlement retains its original terminal/dedupe guard and old payer. */
public record TargetMigrationCustodyGuard(
        byte[] migrationId,
        byte[] planDigest,
        DelayMessageId messageId,
        int originalGeneration,
        Kind kind,
        TargetMigrationRecordReference legacyRecord,
        byte[] originalResultDigest,
        TargetQuotaIncarnation owner,
        CapacityVector retainedLegacyCharge,
        SystemMutation settlement,
        TargetQuotaMutation settledAt) {
    public static final int VALUE_TYPE = 67;
    public static final int MAX_CANONICAL_BYTES = TargetMigrationRecordReference.MAX_CANONICAL_BYTES
            + TargetQuotaIncarnation.MAX_CANONICAL_BYTES
            + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES
            + TargetMigrationControlBody.LEGACY_MAX_INPUT_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-custody-guard-v1\0");

    public enum Kind {
        GENERATION(1),
        IDENTITY(2);
        private final int wire;

        Kind(int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        public static Kind fromWire(int wire) {
            return switch (wire) {
                case 1 -> GENERATION;
                case 2 -> IDENTITY;
                default -> throw new IllegalArgumentException("unknown migration custody guard kind");
            };
        }
    }

    /** Must authenticate original terminal/raw bytes, no-future/producer obligations and original durable accounting. */
    @FunctionalInterface
    public interface SettlementAuthority {
        void requireAuthorized(
                TargetMigrationCustodyGuard prior,
                TargetMigrationSettlementRequest request,
                TargetQuotaMutation actualSource);
    }

    public TargetMigrationCustodyGuard {
        migrationId = TargetCompatibilityCodec.assigned(migrationId, 32, "migration ID");
        planDigest = TargetCompatibilityCodec.assigned(planDigest, 32, "original migration plan");
        originalResultDigest =
                TargetCompatibilityCodec.assigned(originalResultDigest, 32, "original retained result/dedupe closure");
        Objects.requireNonNull(messageId);
        Objects.requireNonNull(kind);
        Objects.requireNonNull(legacyRecord);
        Objects.requireNonNull(owner);
        Objects.requireNonNull(retainedLegacyCharge);
        if (!owner.identity().shard().equals(messageId.routingId().shardId())
                || kind == Kind.IDENTITY && originalGeneration != 0
                || (settlement == null) != (settledAt == null)) {
            throw new IllegalArgumentException("custody guard changed original ID/generation/owner");
        }
        if (settlement != null) {
            final var body = TargetMigrationControlBody.decode(settlement.canonicalBody());
            body.requireInput(settlement);
            if (!(body.request() instanceof TargetMigrationSettlementRequest request)
                    || !messageId.equals(request.messageId())
                    || originalGeneration != request.originalGeneration()
                    || kind != request.guardKind()
                    || !Arrays.equals(migrationId, request.migrationId())
                    || !Arrays.equals(planDigest, request.planDigest())
                    || !Arrays.equals(
                            heldDigest(
                                    migrationId,
                                    planDigest,
                                    messageId,
                                    originalGeneration,
                                    kind,
                                    legacyRecord,
                                    originalResultDigest,
                                    owner,
                                    retainedLegacyCharge),
                            request.expectedGuardDigest())
                    || !request.scope().equals(owner.scope().shardScope())) {
                throw new IllegalArgumentException(
                        "settled custody changed its complete original held guard/registered purpose");
            }
            settledAt.requireSourceApplied();
            if (!Arrays.equals(settledAt.mutationDigest(), Bytes.sha256(settlement.canonicalEnvelope()))
                    || !request.originalTerminalSource().sameSourceIdentity(settledAt.source())
                    || request.originalTerminalSource().compareTo(settledAt.source()) >= 0) {
                throw new IllegalArgumentException(
                        "custody settlement changed actual Source or precedes original terminal evidence");
            }
            owner.latestMutation().requireAtOrBefore(settledAt);
        }
    }

    public static TargetMigrationCustodyGuard held(
            byte[] migrationId,
            byte[] planDigest,
            DelayMessageId messageId,
            int originalGeneration,
            TargetMigrationRecordReference legacyRecord,
            byte[] originalResultDigest,
            TargetQuotaIncarnation owner,
            CapacityVector retainedLegacyCharge) {
        return new TargetMigrationCustodyGuard(
                migrationId,
                planDigest,
                messageId,
                originalGeneration,
                Kind.GENERATION,
                legacyRecord,
                originalResultDigest,
                owner,
                retainedLegacyCharge,
                null,
                null);
    }

    public static TargetMigrationCustodyGuard heldIdentity(
            byte[] migrationId,
            byte[] planDigest,
            DelayMessageId messageId,
            TargetMigrationRecordReference legacyRecord,
            byte[] originalResultDigest,
            TargetQuotaIncarnation owner,
            CapacityVector retainedLegacyCharge) {
        return new TargetMigrationCustodyGuard(
                migrationId,
                planDigest,
                messageId,
                0,
                Kind.IDENTITY,
                legacyRecord,
                originalResultDigest,
                owner,
                retainedLegacyCharge,
                null,
                null);
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
    public byte[] originalResultDigest() {
        return Bytes.copy(originalResultDigest);
    }

    public boolean settled() {
        return settlement != null;
    }

    public TargetMigrationCustodyGuard settle(
            SystemMutation input, TargetQuotaMutation at, SettlementAuthority authority) {
        if (settled()) {
            if (!Arrays.equals(settlement.canonicalEnvelope(), input.canonicalEnvelope()) || !settledAt.equals(at)) {
                throw new IllegalStateException("legacy custody already settled by another exact Source input");
            }
            return this;
        }
        final var body = TargetMigrationControlBody.decode(input.canonicalBody());
        body.requireInput(input);
        if (!(body.request() instanceof TargetMigrationSettlementRequest request)) {
            throw new IllegalArgumentException("custody requires explicit Source settlement purpose");
        }
        final var next = new TargetMigrationCustodyGuard(
                migrationId,
                planDigest,
                messageId,
                originalGeneration,
                kind,
                legacyRecord,
                originalResultDigest,
                owner,
                retainedLegacyCharge,
                input,
                at);
        Objects.requireNonNull(authority, "independent exact original terminal/no-future authority")
                .requireAuthorized(this, request, at);
        return next;
    }
    /** This gate only removes the old migration prohibition; ordinary terminal/command rules still apply. */
    public void requireSendAllowed(DelayMessageId id, int generation) {
        if (!messageId.equals(Objects.requireNonNull(id))) {
            throw new IllegalArgumentException("custody gate ID mismatch");
        }
        if (kind == Kind.IDENTITY || !settled() || Integer.compareUnsigned(generation, originalGeneration) <= 0) {
            throw new IllegalStateException("original legacy custody/terminal generation prohibits Target send");
        }
    }

    public static byte[] keyFor(DelayMessageId messageId) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.MIGRATION_CUSTODY_GUARD_TAG, TargetKeyCodec.KEY_FORMAT},
                Objects.requireNonNull(messageId).bytes());
    }

    public byte[] key() {
        return keyFor(messageId);
    }

    private static byte[] heldDigest(
            byte[] migrationId,
            byte[] planDigest,
            DelayMessageId id,
            int generation,
            Kind kind,
            TargetMigrationRecordReference raw,
            byte[] originalResult,
            TargetQuotaIncarnation owner,
            CapacityVector oldCharge) {
        return Bytes.sha256(
                DOMAIN,
                fields(
                        migrationId,
                        planDigest,
                        id,
                        generation,
                        kind,
                        raw,
                        originalResult,
                        owner,
                        oldCharge,
                        null,
                        null));
    }

    private static byte[] fields(
            byte[] migrationId,
            byte[] planDigest,
            DelayMessageId id,
            int generation,
            Kind kind,
            TargetMigrationRecordReference raw,
            byte[] originalResult,
            TargetQuotaIncarnation owner,
            CapacityVector oldCharge,
            SystemMutation settlement,
            TargetQuotaMutation at) {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, migrationId);
            CanonicalProtobuf.bytes(out, 3, planDigest);
            CanonicalProtobuf.bytes(out, 4, id.bytes());
            CanonicalProtobuf.uint32Bits(out, 5, generation);
            CanonicalProtobuf.bytes(out, 6, raw.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, originalResult);
            CanonicalProtobuf.bytes(out, 8, owner.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, oldCharge.canonicalBytes());
            if (settlement != null) {
                CanonicalProtobuf.bytes(out, 10, settlement.canonicalEnvelope());
                CanonicalProtobuf.bytes(out, 11, at.canonicalBytes());
            }
            CanonicalProtobuf.uint32(out, 13, kind.wireValue());
        });
    }

    private byte[] fields() {
        return fields(
                migrationId,
                planDigest,
                messageId,
                originalGeneration,
                kind,
                legacyRecord,
                originalResultDigest,
                owner,
                retainedLegacyCharge,
                settlement,
                settledAt);
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 14, digest());
        });
    }

    public static TargetMigrationCustodyGuard decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 13, false, "migration custody guard");
        final boolean settled = f.size() == 13;
        QueryCodecSupport.requireNumbers(
                f,
                settled
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 13, 14},
                "migration custody guard");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown migration custody guard schema");
        }
        final var value = new TargetMigrationCustodyGuard(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                new DelayMessageId(QueryCodecSupport.fixed(f.get(3), 4, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32(f.get(4), 5),
                Kind.fromWire(QueryCodecSupport.uint32(f.get(f.size() - 2), 13)),
                TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                QueryCodecSupport.fixed(f.get(6), 7, 32),
                TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                CapacityVector.decode(QueryCodecSupport.bytes(f.get(8), 9)),
                settled
                        ? SystemMutation.decodeTargetEnvelope(
                                QueryCodecSupport.bytes(f.get(9), 10), SystemMutationType.APPLY_SHARD_CONTROL)
                        : null,
                settled ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(10), 11)) : null);
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.getLast(), 14, 32))) {
            throw new IllegalArgumentException("custody guard digest changed");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration custody guard");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetMigrationCustodyGuard value
                && Arrays.equals(canonicalBytes(), value.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest());
    }
}
