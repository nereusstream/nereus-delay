package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMigrationOrigin;
import com.nereusstream.delay.protocol.TargetMigrationPayloadHistory;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;

/**
 * Actual A/first/POSITION and immutable Native66 joins. Original objects/history are read separately outside Store
 * locks.
 */
public final class TargetMigrationPayloadJoins {
    private TargetMigrationPayloadJoins() {}

    @FunctionalInterface
    public interface Read {
        byte[] get(ColumnFamily family, byte[] key, int type);
    }

    public static TargetMigrationRoot require(
            Read read,
            TargetQuotaPayloadOwner owner,
            TargetScheduleBinding binding,
            TargetReservationRecord reservation,
            TargetMigrationPayloadHistory history,
            TargetQuotaMutation frontier) {
        final var root = TargetMigrationRoot.decode(Objects.requireNonNull(
                read.get(
                        ColumnFamily.META,
                        TargetMigrationRoot.keyFor(owner.primaryIdentity().shard()),
                        TargetMigrationRoot.VALUE_TYPE),
                "actual migration root"));
        if (!root.activated()
                || !Arrays.equals(root.plan().newLineage(), owner.recoveryLineage())
                || !Arrays.equals(root.plan().scope().tenantScope(), owner.tenantScope())) {
            throw new IllegalStateException("migrated payload changed actual A/tenant/lineage");
        }
        root.activatedAt().requireAtOrBefore(frontier);
        if (!binding.imported()
                || !Arrays.equals(binding.migrationPlanDigest(), root.plan().digest())) {
            throw new IllegalStateException("migrated payload changed original plan binding");
        }
        if (reservation != null) {
            Objects.requireNonNull(reservation.migrationOrigin()).requirePlan(root.plan(), binding);
        } else if (binding.commandType() != CommandType.SCHEDULE
                || binding.intent().hasInlinePayload()) {
            throw new IllegalStateException("only actual imported object Schedule may omit reservation");
        }
        requireActivation(read, root, frontier);
        final byte[] index = TargetMigrationOrigin.keyFor(ColumnFamily.ID, Bytes.sha256(binding.encodedKey()));
        final var origin = TargetMigrationOrigin.decode(Objects.requireNonNull(
                read.get(ColumnFamily.META, index, TargetMigrationOrigin.VALUE_TYPE),
                "actual immutable binding origin"));
        origin.requirePlan(root.plan());
        origin.requireRow(
                ColumnFamily.ID,
                binding.encodedKey(),
                TargetValueEnvelope.encode(TargetScheduleBinding.VALUE_TYPE, binding.canonicalBytes()));
        if (!Arrays.equals(index, origin.key())
                || !origin.oldRecord().equals(binding.migrationRecord())
                || !origin.owner().identity().equals(owner.primaryIdentity())
                || !origin.owner().accounting().equals(owner.accounting())) {
            throw new IllegalStateException("migrated payload changed original mapped accounting owner");
        }
        if (binding.schemaVersion() == 3) {
            final var at = binding.migrationSource();
            final byte[] key = TargetMigrationPrefixRecord.keyFor(root.plan().migrationId(), at.sequence());
            final var prefix = TargetMigrationPrefixRecord.decode(Objects.requireNonNull(
                    read.get(ColumnFamily.META, key, TargetMigrationPrefixRecord.VALUE_TYPE),
                    "actual imported prefix"));
            prefix.requirePlan(root.plan());
            if (!prefix.at().equals(at)) {
                throw new IllegalStateException("migration reservation changed original actual prefix");
            }
            at.requireAtOrBefore(frontier);
        }
        if (history != null) {
            history.require(root, binding, reservation);
            if (!Arrays.equals(origin.canonicalBytes(), history.bindingOrigin().canonicalBytes())) {
                throw new IllegalStateException("frozen migration payload changed actual Native66");
            }
        }
        return root;
    }

    public static void requireActivation(Read read, TargetMigrationRoot root, TargetQuotaMutation frontier) {
        if (!root.activated()) {
            throw new IllegalStateException("MIGRATION_PREPARED is not actual A");
        }
        root.activatedAt().requireAtOrBefore(frontier);
        final byte[] firstKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                root.activation().systemMutationId());
        final var first = TargetResultRecord.decode(Objects.requireNonNull(
                read.get(ColumnFamily.DEDUPE, firstKey, TargetResultRecord.VALUE_TYPE), "actual migration A first"));
        first.requireStored(firstKey, TargetResultRecord.VALUE_TYPE, first.canonicalBytes());
        final byte[] posKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                root.activatedAt().source().canonicalBytes());
        final var pos = TargetResultRecord.decode(Objects.requireNonNull(
                read.get(ColumnFamily.DEDUPE, posKey, TargetResultRecord.VALUE_TYPE), "actual migration A POSITION"));
        pos.requireStored(posKey, TargetResultRecord.VALUE_TYPE, pos.canonicalBytes());
        if (first.kind() != TargetResultRecord.Kind.SYSTEM
                || !first.appliedFirst()
                || !first.mutation().equals(root.activatedAt())
                || pos.kind() != TargetResultRecord.Kind.POSITION_SYSTEM
                || !pos.mutation().equals(root.activatedAt())
                || !Arrays.equals(
                        first.typedPayload(),
                        SystemMutationResult.from(
                                        root.activation(),
                                        ApplyStatus.APPLIED,
                                        StableCode.OK,
                                        root.activatedAt().source().canonicalBytes())
                                .encode())) {
            throw new IllegalStateException("migrated payload lost exact APPLIED actual A");
        }
        first.requireOwner(root.owner());
        pos.requireOwner(root.owner());
        pos.requireFirst(first);
    }

    public static Read checkpoint(TargetCheckpointDependencies d) {
        return (family, key, type) -> {
            if (type == TargetMigrationRoot.VALUE_TYPE) {
                final var value = d.migrationRoot();
                return value == null ? null : value.canonicalBytes();
            }
            if (type == TargetMigrationOrigin.VALUE_TYPE) {
                final var value = d.migrationOrigin(key);
                return value == null ? null : value.canonicalBytes();
            }
            if (type == TargetMigrationPrefixRecord.VALUE_TYPE) {
                final var value = d.migrationPrefix(key);
                return value == null ? null : value.canonicalBytes();
            }
            if (type == TargetResultRecord.VALUE_TYPE) {
                final var value = d.resultRecord(key);
                return value == null ? null : value.canonicalBytes();
            }
            throw new IllegalArgumentException("unsupported migration payload join");
        };
    }
}
