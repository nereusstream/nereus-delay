package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandHash;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ScheduleCommandBody;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMigrationOrigin;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetObjectPayloadOrigin;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Exact retained Native joins, not a proof-key verifier or a deletion authority. Root history authenticates acceptance. */
public final class TargetObjectPayloadAcceptance {
    private TargetObjectPayloadAcceptance() {}

    @FunctionalInterface
    private interface Read {
        byte[] get(ColumnFamily family, byte[] key, int type);
    }

    public static TargetObjectPayloadOrigin collect(
            TargetStoreBackend.Reader reader, TargetQuotaPayloadOwner expected) {
        expected.mutation().requireAtOrBefore(reader.aggregate().mutation());
        return collect(
                (family, key, type) -> TargetValueEnvelope.decode(
                                Objects.requireNonNull(reader.get(family, key), "original OBJECT acceptance row"), type)
                        .payload(),
                expected);
    }

    public static void requireActual(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaPayloadOwner expected,
            TargetObjectPayloadOrigin frozen,
            TargetQuotaMutation frontier) {
        final var actual = collect(
                (family, key, type) -> TargetValueEnvelope.decode(
                                Objects.requireNonNull(
                                        view.projected(family, key, overlay), "retained OBJECT acceptance dependency"),
                                type)
                        .payload(),
                expected);
        (actual.migrated() ? frozen : actual).acceptedAt().requireAtOrBefore(frontier);
        if (actual.migrated()) {
            TargetMigrationPayloadJoins.require(
                    (family, key, type) -> TargetValueEnvelope.decode(
                                    Objects.requireNonNull(view.projected(family, key, overlay)), type)
                            .payload(),
                    expected,
                    actual.binding(),
                    actual.reservation(),
                    Objects.requireNonNull(frozen.migrationHistory()),
                    frontier);
        }
        requireSame(frozen, actual);
    }

    public static void requireActual(
            TargetCheckpointDependencies d, TargetQuotaPayloadOwner expected, TargetObjectPayloadOrigin frozen) {
        final var actual = collect(
                (family, key, type) -> {
                    if (type == TargetMigrationRoot.VALUE_TYPE
                            || type == TargetMigrationOrigin.VALUE_TYPE
                            || type == TargetMigrationPrefixRecord.VALUE_TYPE) {
                        return TargetMigrationPayloadJoins.checkpoint(d).get(family, key, type);
                    }
                    if (family == ColumnFamily.META && type == TargetQuotaIncarnation.VALUE_TYPE) {
                        final var root = d.root().root();
                        final byte[] rootKey = root.identity().key();
                        rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
                        if (!Arrays.equals(key, rootKey)) {
                            throw new IllegalStateException("OBJECT original POSITION changed actual Source root");
                        }
                        return root.canonicalBytes();
                    }
                    if (family == ColumnFamily.DEDUPE && type == TargetResultRecord.VALUE_TYPE) {
                        return Objects.requireNonNull(d.resultRecord(key), "actual original OBJECT first/POSITION")
                                .canonicalBytes();
                    }
                    if (family == ColumnFamily.ID && type == TargetReservationRecord.VALUE_TYPE) {
                        return Objects.requireNonNull(
                                        d.reservationProjection(key), "actual complete Native reservation projection")
                                .canonicalBytes();
                    }
                    if (family == ColumnFamily.ID && type == TargetScheduleBinding.VALUE_TYPE) {
                        final var binding = Objects.requireNonNull(
                                d.binding(expected.initialBindingDigest()), "actual original OBJECT binding");
                        if (!Arrays.equals(key, TargetKeyCodec.scheduleBinding(binding.digest()))) {
                            throw new IllegalStateException("OBJECT original binding key mismatch");
                        }
                        return binding.canonicalBytes();
                    }
                    throw new IllegalArgumentException("unsupported OBJECT acceptance dependency");
                },
                expected);
        (actual.migrated() ? frozen : actual)
                .acceptedAt()
                .requireAtOrBefore(d.root().aggregate().mutation());
        if (actual.migrated()) {
            TargetMigrationPayloadJoins.require(
                    TargetMigrationPayloadJoins.checkpoint(d),
                    expected,
                    actual.binding(),
                    actual.reservation(),
                    Objects.requireNonNull(frozen.migrationHistory()),
                    d.root().aggregate().mutation());
        }
        requireSame(frozen, actual);
    }

    private static TargetObjectPayloadOrigin collect(Read read, TargetQuotaPayloadOwner expected) {
        final byte[] bindingKey = TargetKeyCodec.scheduleBinding(expected.initialBindingDigest());
        final var binding = TargetScheduleBinding.decodeForStore(
                bindingKey,
                read.get(ColumnFamily.ID, bindingKey, TargetScheduleBinding.VALUE_TYPE),
                expected.primaryIdentity().shard());
        expected.requireInitialBinding(binding);
        final TargetObjectPayloadOrigin origin;
        if (binding.commandType() == CommandType.PREPARE_LARGE_SCHEDULE) {
            final byte[] key = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESERVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                    expected.messageId().bytes());
            final byte[] encoded = read.get(ColumnFamily.ID, key, TargetReservationRecord.VALUE_TYPE);
            final var reservation = TargetReservationRecord.decode(encoded);
            if (!Arrays.equals(key, reservation.key())
                    || !Arrays.equals(
                            encoded,
                            read.get(ColumnFamily.ID, reservation.lookupKey(), TargetReservationRecord.VALUE_TYPE))) {
                throw new IllegalStateException("original COMMITTED primary/lookup Native projections differ");
            }
            if (reservation.migrationOrigin() != null) {
                final boolean baseline = reservation
                        .mutation()
                        .equals(reservation.migrationOrigin().importedAt());
                final var acceptedPosition = baseline ? null : position(read, reservation.mutation());
                final var acceptedFirst = baseline ? null : first(read, acceptedPosition.logicalId());
                final var migrated = new TargetObjectPayloadOrigin(
                        TargetObjectPayloadOrigin.Kind.MIGRATED_PREPARE_COMMIT,
                        binding,
                        acceptedFirst,
                        acceptedPosition,
                        reservation,
                        null,
                        null);
                migrated.requireOwner(expected);
                TargetMigrationPayloadJoins.require(
                        read::get, expected, binding, reservation, null, expected.mutation());
                if (acceptedPosition != null) {
                    requirePositionOwner(read, acceptedPosition, expected);
                    requireCreation(acceptedFirst);
                }
                return migrated;
            }
            final var acceptedPosition = position(read, reservation.mutation());
            final var acceptedFirst = first(read, acceptedPosition.logicalId());
            final var prepareFirst = first(read, reservation.prepareCommandId().bytes());
            final var preparePosition = position(read, reservation.prepareAnchor());
            origin = new TargetObjectPayloadOrigin(
                    TargetObjectPayloadOrigin.Kind.PREPARE_COMMIT,
                    binding,
                    acceptedFirst,
                    acceptedPosition,
                    reservation,
                    prepareFirst,
                    preparePosition);
            final var prepare = CommandDedupeRecord.decode(prepareFirst.typedPayload());
            if (!prepare.protocolTuple().equals(reservation.prepareTuple())
                    || !Arrays.equals(prepare.commandHash(), reservation.prepareCommandHash())
                    || prepare.result().applyStatus() != ApplyStatus.APPLIED
                    || prepare.result().stableCode() != StableCode.OK
                    || prepare.result().generation() != -1
                    || prepare.result().stateVersion() != 0
                    || prepare.result().messageStatus() != null) {
                throw new IllegalStateException("Native acceptance changed original successful Prepare first");
            }
        } else if (binding.commandType() == CommandType.SCHEDULE
                && !binding.intent().hasInlinePayload()) {
            if (binding.imported()) {
                final var migrated = new TargetObjectPayloadOrigin(
                        TargetObjectPayloadOrigin.Kind.MIGRATED_DIRECT_SCHEDULE, binding, null, null, null, null, null);
                migrated.requireOwner(expected);
                TargetMigrationPayloadJoins.require(read::get, expected, binding, null, null, expected.mutation());
                return migrated;
            }
            final byte[] posKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    binding.bindingSource().canonicalBytes());
            final var acceptedPosition =
                    TargetResultRecord.decode(read.get(ColumnFamily.DEDUPE, posKey, TargetResultRecord.VALUE_TYPE));
            acceptedPosition.requireStored(posKey, TargetResultRecord.VALUE_TYPE, acceptedPosition.canonicalBytes());
            final var acceptedFirst = first(read, acceptedPosition.logicalId());
            origin = new TargetObjectPayloadOrigin(
                    TargetObjectPayloadOrigin.Kind.DIRECT_SCHEDULE,
                    binding,
                    acceptedFirst,
                    acceptedPosition,
                    null,
                    null,
                    null);
            final var retained = CommandDedupeRecord.decode(acceptedFirst.typedPayload());
            final var body = ScheduleCommandBody.decode(binding.canonicalBody());
            final var command = new PreparedCommand(
                    expected.primaryIdentity().shard(),
                    new CommandId(acceptedFirst.logicalId()),
                    expected.messageId(),
                    CommandType.SCHEDULE,
                    retained.protocolTuple(),
                    body.retryUntilEpochMs(),
                    binding.canonicalBody(),
                    retained.commandHash());
            if (!Arrays.equals(
                            retained.commandHash(),
                            CommandHash.compute(
                                    retained.protocolTuple(),
                                    CommandType.SCHEDULE,
                                    command.commandId(),
                                    expected.messageId(),
                                    body.retryUntilEpochMs(),
                                    binding.canonicalBody()))
                    || !Arrays.equals(
                            command.canonicalFrameDigest(),
                            acceptedFirst.mutation().mutationDigest())) {
                throw new IllegalStateException("direct OBJECT Schedule differs from original accepted first frame");
            }
        } else {
            throw new IllegalStateException("OBJECT owner has no supported original accepted creation provenance");
        }
        origin.requireOwner(expected);
        requirePositionOwner(read, origin.acceptedPosition(), expected);
        if (origin.preparePosition() != null) {
            requirePositionOwner(read, origin.preparePosition(), expected);
        }
        final var accepted = CommandDedupeRecord.decode(origin.acceptedFirst().typedPayload());
        final var result = accepted.result();
        if (result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.SCHEDULED
                || result.generation() != 0
                || result.stateVersion() != 1
                || result.messageStatus() != MessageStatus.SCHEDULED) {
            throw new IllegalStateException(
                    "OBJECT origin must use its first successful creation, not ALREADY_COMMITTED or a later retry");
        }
        requireResultSource(origin.acceptedFirst(), result);
        if (origin.prepareFirst() != null) {
            requireResultSource(
                    origin.prepareFirst(),
                    CommandDedupeRecord.decode(origin.prepareFirst().typedPayload())
                            .result());
        }
        return origin;
    }

    private static void requirePositionOwner(Read read, TargetResultRecord position, TargetQuotaPayloadOwner expected) {
        final byte[] key = position.primaryIdentity().key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var sourceOwner = TargetQuotaIncarnation.decodeForStore(
                key,
                read.get(ColumnFamily.META, key, TargetQuotaIncarnation.VALUE_TYPE),
                expected.primaryIdentity().shard(),
                expected.tenantScope());
        position.requireOwner(sourceOwner);
    }

    private static void requireResultSource(TargetResultRecord first, CommandResult result) {
        if (!Arrays.equals(
                result.appliedSourcePosition(), first.mutation().source().canonicalBytes())) {
            throw new IllegalStateException("OBJECT accepted result changed its actual Source metadata");
        }
        if (!new CommandId(first.logicalId())
                .routingId()
                .shardId()
                .equals(first.mutation().source().shardId())) {
            throw new IllegalStateException("OBJECT original Command crossed Source Shard");
        }
    }

    private static TargetResultRecord first(Read read, byte[] id) {
        final byte[] key = Bytes.concat(new byte[] {TargetKeyCodec.RESULT_COMMAND_TAG, TargetKeyCodec.KEY_FORMAT}, id);
        final var first = TargetResultRecord.decode(read.get(ColumnFamily.DEDUPE, key, TargetResultRecord.VALUE_TYPE));
        first.requireStored(key, TargetResultRecord.VALUE_TYPE, first.canonicalBytes());
        return first;
    }

    private static TargetResultRecord position(Read read, TargetQuotaMutation stamp) {
        final byte[] key = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                stamp.source().canonicalBytes());
        final var position =
                TargetResultRecord.decode(read.get(ColumnFamily.DEDUPE, key, TargetResultRecord.VALUE_TYPE));
        position.requireStored(key, TargetResultRecord.VALUE_TYPE, position.canonicalBytes());
        if (!position.mutation().equals(stamp)) {
            throw new IllegalStateException("OBJECT original POSITION changed accepted Source stamp");
        }
        return position;
    }

    private static void requireCreation(TargetResultRecord first) {
        final var value = CommandDedupeRecord.decode(first.typedPayload());
        final var result = value.result();
        if (result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.SCHEDULED
                || result.generation() != 0
                || result.stateVersion() != 1
                || result.messageStatus() != MessageStatus.SCHEDULED) {
            throw new IllegalStateException("migrated OBJECT requires the actual first successful native Commit");
        }
        requireResultSource(first, result);
    }

    private static void requireSame(TargetObjectPayloadOrigin expected, TargetObjectPayloadOrigin actual) {
        if (actual.migrated()) {
            actual = actual.withMigrationHistory(Objects.requireNonNull(expected.migrationHistory()));
        }
        if (!Arrays.equals(Objects.requireNonNull(expected).canonicalBytes(), actual.canonicalBytes())) {
            throw new IllegalStateException("OBJECT frozen native origin differs from actual retained records");
        }
    }
}
