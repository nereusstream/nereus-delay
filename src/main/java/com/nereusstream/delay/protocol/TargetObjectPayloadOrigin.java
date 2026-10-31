package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.runtime.TargetReservationRecord;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.util.Arrays;
import java.util.Objects;

/** Full projections of retained Native acceptance facts. These bytes are neither an authority nor a new ledger. */
public record TargetObjectPayloadOrigin(
        Kind kind,
        TargetScheduleBinding binding,
        TargetResultRecord acceptedFirst,
        TargetResultRecord acceptedPosition,
        TargetReservationRecord reservation,
        TargetResultRecord prepareFirst,
        TargetResultRecord preparePosition,
        TargetMigrationPayloadHistory migrationHistory) {
    public TargetObjectPayloadOrigin(
            Kind kind,
            TargetScheduleBinding binding,
            TargetResultRecord first,
            TargetResultRecord position,
            TargetReservationRecord reservation,
            TargetResultRecord prepareFirst,
            TargetResultRecord preparePosition) {
        this(kind, binding, first, position, reservation, prepareFirst, preparePosition, null);
    }

    public static final int SCHEMA_VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES;

    public enum Kind {
        PREPARE_COMMIT,
        DIRECT_SCHEDULE,
        MIGRATED_PREPARE_COMMIT,
        MIGRATED_DIRECT_SCHEDULE
    }

    public TargetObjectPayloadOrigin {
        Objects.requireNonNull(kind);
        Objects.requireNonNull(binding);
        if (kind == Kind.MIGRATED_DIRECT_SCHEDULE) {
            if (!binding.imported()
                    || binding.commandType() != CommandType.SCHEDULE
                    || binding.intent().hasInlinePayload()
                    || reservation != null
                    || acceptedFirst != null
                    || acceptedPosition != null
                    || prepareFirst != null
                    || preparePosition != null) {
                throw new IllegalArgumentException(
                        "migrated direct OBJECT retains original Schedule history without fabricated Native records");
            }
        } else if (kind == Kind.MIGRATED_PREPARE_COMMIT) {
            Objects.requireNonNull(reservation);
            Objects.requireNonNull(reservation.migrationOrigin());
            if (!binding.imported()
                    || binding.commandType() != CommandType.PREPARE_LARGE_SCHEDULE
                    || reservation.status() != PayloadReservationStatus.COMMITTED
                    || prepareFirst != null
                    || preparePosition != null) {
                throw new IllegalArgumentException("explicit migrated committed reservation required");
            }
            reservation.requireBinding(binding);
            if (reservation.mutation().equals(reservation.migrationOrigin().importedAt())) {
                if (reservation.migrationOrigin().originalStatus() != PayloadReservationStatus.COMMITTED
                        || acceptedFirst != null
                        || acceptedPosition != null) {
                    throw new IllegalArgumentException(
                            "baseline Commit retains original legacy history without fabricated Native first");
                }
            } else {
                requirePair(acceptedFirst, acceptedPosition);
                if (!acceptedFirst.mutation().equals(reservation.mutation())) {
                    throw new IllegalArgumentException("migrated payload lost real post-import Commit");
                }
            }
        } else {
            if (migrationHistory != null || binding.imported()) {
                throw new IllegalArgumentException("ordinary OBJECT origin cannot carry imported history");
            }
            requirePair(acceptedFirst, acceptedPosition);
            if (kind == Kind.PREPARE_COMMIT) {
                Objects.requireNonNull(reservation);
                requirePair(prepareFirst, preparePosition);
                if (binding.commandType() != CommandType.PREPARE_LARGE_SCHEDULE
                        || reservation.status() != PayloadReservationStatus.COMMITTED
                        || !reservation.mutation().equals(acceptedFirst.mutation())
                        || !reservation.prepareAnchor().equals(prepareFirst.mutation())) {
                    throw new IllegalArgumentException(
                            "Native OBJECT acceptance changed original Prepare/first Commit identity");
                }
                reservation.requireBinding(binding);
                reservation.mutation().requireSourceApplied();
                acceptedFirst.mutation().requireAfter(prepareFirst.mutation());
            } else if (reservation != null
                    || prepareFirst != null
                    || preparePosition != null
                    || binding.commandType() != CommandType.SCHEDULE
                    || binding.intent().hasInlinePayload()
                    || !Arrays.equals(
                            binding.bindingSource().canonicalBytes(),
                            acceptedFirst.mutation().source().canonicalBytes())) {
                throw new IllegalArgumentException(
                        "DIRECT_SCHEDULE must retain its own original object Schedule and no fabricated reservation");
            }
        }
    }

    public boolean migrated() {
        return kind == Kind.MIGRATED_PREPARE_COMMIT || kind == Kind.MIGRATED_DIRECT_SCHEDULE;
    }

    public TargetQuotaMutation acceptedAt() {
        return kind == Kind.MIGRATED_DIRECT_SCHEDULE
                ? Objects.requireNonNull(migrationHistory, "frozen direct Schedule migration history")
                        .bindingOrigin()
                        .baseline()
                : migrated() ? reservation.mutation() : acceptedFirst.mutation();
    }

    public TargetObjectPayloadOrigin withMigrationHistory(TargetMigrationPayloadHistory history) {
        if (!migrated() || migrationHistory != null) {
            throw new IllegalArgumentException("wrong migration origin stage");
        }
        return new TargetObjectPayloadOrigin(
                kind,
                binding,
                acceptedFirst,
                acceptedPosition,
                reservation,
                null,
                null,
                Objects.requireNonNull(history));
    }

    private static void requirePair(TargetResultRecord first, TargetResultRecord position) {
        Objects.requireNonNull(first);
        Objects.requireNonNull(position);
        if (first.kind() != TargetResultRecord.Kind.COMMAND
                || !first.appliedFirst()
                || position.kind() != TargetResultRecord.Kind.POSITION_COMMAND
                || !first.mutation().equals(position.mutation())) {
            throw new IllegalArgumentException("actual original Command first/POSITION required");
        }
        position.requireFirst(first);
    }

    public ProfileRef profile() {
        return kind != Kind.DIRECT_SCHEDULE && kind != Kind.MIGRATED_DIRECT_SCHEDULE
                ? PrepareLargeScheduleBody.decode(binding.canonicalBody()).objectStoreProfile()
                : binding.intent().committedPayload().objectStoreProfile();
    }

    public void requireOwner(TargetQuotaPayloadOwner expected) {
        expected.requireInitialBinding(binding);
        if (expected.kind() != TargetQuotaPayloadOwner.Kind.OBJECT
                || expected.committedPayload() == null
                || !Arrays.equals(profile().semanticHash(), expected.objectStoreProfileHash())) {
            throw new IllegalArgumentException("Native acceptance changed exact committed OBJECT/Profile");
        }
        if (reservation != null) {
            reservation.requireOwner(expected);
        } else if (!PayloadReference.fromDescriptor(binding.intent().committedPayload())
                .equals(expected.committedPayload())) {
            throw new IllegalArgumentException("direct Schedule acceptance changed its immutable payload descriptor");
        }
        if (acceptedFirst != null) {
            requireOwner(acceptedFirst, expected);
        }
        // POSITION is charged to the original Source SHARD, joined to this Target first by requirePair.
        if (prepareFirst != null) {
            requireOwner(prepareFirst, expected);
        }
        if (kind != Kind.MIGRATED_DIRECT_SCHEDULE || migrationHistory != null) {
            acceptedAt().requireAtOrBefore(expected.mutation());
        }
    }

    private static void requireOwner(TargetResultRecord record, TargetQuotaPayloadOwner owner) {
        if (!record.primaryIdentity().equals(owner.primaryIdentity())
                || !record.accounting().equals(owner.accounting())
                || !Arrays.equals(record.tenantScope(), owner.tenantScope())
                || !Arrays.equals(record.recoveryLineage(), owner.recoveryLineage())) {
            throw new IllegalArgumentException(
                    "original OBJECT acceptance changed frozen owner/accounting/tenant/lineage");
        }
    }

    public byte[] canonicalBytes() {
        if (migrated()) {
            final byte[] bytes = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 2);
                CanonicalProtobuf.uint32(out, 2, kind == Kind.MIGRATED_DIRECT_SCHEDULE ? 4 : 3);
                CanonicalProtobuf.bytes(out, 3, binding.canonicalBytes());
                if (acceptedFirst != null) {
                    CanonicalProtobuf.bytes(out, 4, acceptedFirst.canonicalBytes());
                    CanonicalProtobuf.bytes(out, 5, acceptedPosition.canonicalBytes());
                }
                if (reservation != null) {
                    CanonicalProtobuf.bytes(out, 6, reservation.canonicalBytes());
                }
                if (migrationHistory != null) {
                    CanonicalProtobuf.bytes(out, 9, migrationHistory.canonicalBytes());
                }
            });
            if (bytes.length > MAX_CANONICAL_BYTES) {
                throw new IllegalArgumentException("migrated OBJECT origin exceeds original intent bound");
            }
            return bytes;
        }
        final byte[] value = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, SCHEMA_VERSION);
            CanonicalProtobuf.uint32(out, 2, kind == Kind.PREPARE_COMMIT ? 1 : 2);
            CanonicalProtobuf.bytes(out, 3, binding.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, acceptedFirst.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, acceptedPosition.canonicalBytes());
            if (reservation != null) {
                CanonicalProtobuf.bytes(out, 6, reservation.canonicalBytes());
                CanonicalProtobuf.bytes(out, 7, prepareFirst.canonicalBytes());
                CanonicalProtobuf.bytes(out, 8, preparePosition.canonicalBytes());
            }
        });
        if (value.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Native OBJECT origin exceeds original body input bound");
        }
        return value;
    }

    public static TargetObjectPayloadOrigin decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "Native OBJECT origin");
        if (QueryCodecSupport.uint32(f.get(0), 1) == 2) {
            if (QueryCodecSupport.uint32(f.get(1), 2) == 4) {
                QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 9}, "migrated direct OBJECT origin");
                final var value = new TargetObjectPayloadOrigin(
                        Kind.MIGRATED_DIRECT_SCHEDULE,
                        TargetScheduleBinding.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                        null,
                        null,
                        null,
                        null,
                        null,
                        TargetMigrationPayloadHistory.decode(QueryCodecSupport.bytes(f.get(3), 9)));
                if (!value.migrationHistory().direct()) {
                    throw new IllegalArgumentException("direct origin has reservation custody");
                }
                QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migrated direct OBJECT origin");
                return value;
            }
            final boolean nativeCommit = f.size() == 7;
            QueryCodecSupport.requireNumbers(
                    f,
                    nativeCommit ? new int[] {1, 2, 3, 4, 5, 6, 9} : new int[] {1, 2, 3, 6, 9},
                    "migrated OBJECT origin");
            if (QueryCodecSupport.uint32(f.get(1), 2) != 3) {
                throw new IllegalArgumentException("unknown migrated OBJECT origin kind");
            }
            final var value = new TargetObjectPayloadOrigin(
                    Kind.MIGRATED_PREPARE_COMMIT,
                    TargetScheduleBinding.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                    nativeCommit ? TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(3), 4)) : null,
                    nativeCommit ? TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(4), 5)) : null,
                    TargetReservationRecord.decode(QueryCodecSupport.bytes(f.get(nativeCommit ? 5 : 3), 6)),
                    null,
                    null,
                    TargetMigrationPayloadHistory.decode(QueryCodecSupport.bytes(f.getLast(), 9)));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migrated OBJECT origin");
            return value;
        }
        QueryCodecSupport.requireNumbers(
                f,
                f.size() == 8 ? new int[] {1, 2, 3, 4, 5, 6, 7, 8} : new int[] {1, 2, 3, 4, 5},
                "Native OBJECT origin");
        final int kind = QueryCodecSupport.uint32(f.get(1), 2);
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1 || kind < 1 || kind > 2 || (kind == 1) != (f.size() == 8)) {
            throw new IllegalArgumentException("unknown Native OBJECT origin schema/provenance");
        }
        final var value = new TargetObjectPayloadOrigin(
                kind == 1 ? Kind.PREPARE_COMMIT : Kind.DIRECT_SCHEDULE,
                TargetScheduleBinding.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                kind == 1 ? TargetReservationRecord.decode(QueryCodecSupport.bytes(f.get(5), 6)) : null,
                kind == 1 ? TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(6), 7)) : null,
                kind == 1 ? TargetResultRecord.decode(QueryCodecSupport.bytes(f.get(7), 8)) : null);
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Native OBJECT origin");
        return value;
    }
}
