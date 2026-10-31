package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetInlinePayloadSourceFacts;
import com.nereusstream.delay.protocol.TargetMigrationOrigin;
import com.nereusstream.delay.protocol.TargetMigrationPrefixRecord;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetResourceReclaimBody;
import com.nereusstream.delay.protocol.TargetRetirementSourceHistoryRecord;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetUncommittedPayloadFacts;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetRetirementLedgerCursor;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Actual Native reservation/causal joins. Upload closure and provider/public-history authentication remain independent.
 */
public final class TargetUncommittedPayloadReclaim {
    private TargetUncommittedPayloadReclaim() {}

    @FunctionalInterface
    private interface Read {
        byte[] get(ColumnFamily family, byte[] key, int type);
    }

    private static byte[] commandKey(byte[] id) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_COMMAND_TAG, TargetKeyCodec.KEY_FORMAT}, id);
    }

    private static byte[] systemKey(byte[] id) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT}, id);
    }

    private static byte[] positionKey(SourcePosition source) {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT}, source.canonicalBytes());
    }

    private static Read read(TargetRecordAccounting.View view, List<TargetStoreBackend.Edit> overlay) {
        return (family, key, type) -> {
            final byte[] raw = view.projected(family, key, overlay);
            return raw == null ? null : TargetValueEnvelope.decode(raw, type).payload();
        };
    }

    private static Read read(TargetCheckpointDependencies d) {
        return (family, key, type) -> {
            if (type == TargetMigrationRoot.VALUE_TYPE
                    || type == TargetMigrationOrigin.VALUE_TYPE
                    || type == TargetMigrationPrefixRecord.VALUE_TYPE) {
                return TargetMigrationPayloadJoins.checkpoint(d).get(family, key, type);
            }
            if (family == ColumnFamily.DEDUPE && type == TargetResultRecord.VALUE_TYPE) {
                final var value = d.resultRecord(key);
                return value == null ? null : value.canonicalBytes();
            }
            if (family == ColumnFamily.ID && type == TargetReservationRecord.VALUE_TYPE) {
                final var value = d.reservationProjection(key);
                return value == null ? null : value.canonicalBytes();
            }
            if (family == ColumnFamily.ID && type == TargetScheduleBinding.VALUE_TYPE) {
                final var value = d.binding(Arrays.copyOfRange(key, 2, key.length));
                return value == null ? null : value.canonicalBytes();
            }
            if (family == ColumnFamily.ID && type == TargetMessageRecord.VALUE_TYPE) {
                return d.messageState(new DelayMessageId(Arrays.copyOfRange(key, 2, key.length))) != null
                        ? new byte[] {1}
                        : null;
            }
            if (family == ColumnFamily.META && type == TargetQuotaIncarnation.VALUE_TYPE) {
                final var root = d.root().root();
                final byte[] rootKey = root.identity().key();
                rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
                return Arrays.equals(key, rootKey) ? root.canonicalBytes() : null;
            }
            throw new IllegalArgumentException("unsupported uncommitted Native fact read");
        };
    }

    private static TargetResultRecord result(Read read, byte[] key) {
        final var value = TargetResultRecord.decode(Objects.requireNonNull(
                read.get(ColumnFamily.DEDUPE, key, TargetResultRecord.VALUE_TYPE),
                "original uncommitted first/POSITION"));
        value.requireStored(key, TargetResultRecord.VALUE_TYPE, value.canonicalBytes());
        return value;
    }

    private static void pair(
            Read read, TargetResultRecord first, TargetQuotaMutation at, TargetQuotaPayloadOwner expected) {
        if (!first.appliedFirst() || !first.mutation().equals(at)) {
            throw new IllegalStateException("uncommitted causality lost its actual first successful Source stamp");
        }
        final var position = result(read, positionKey(at.source()));
        position.requireFirst(first);
        if (!position.mutation().equals(at)) {
            throw new IllegalStateException("uncommitted POSITION is a later retry");
        }
        final byte[] ownerKey = position.primaryIdentity().key();
        ownerKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        position.requireOwner(TargetQuotaIncarnation.decodeForStore(
                ownerKey,
                Objects.requireNonNull(
                        read.get(ColumnFamily.META, ownerKey, TargetQuotaIncarnation.VALUE_TYPE),
                        "actual Source SHARD descriptor"),
                expected.primaryIdentity().shard(),
                expected.tenantScope()));
    }

    private static TargetResultRecord frame(
            Read read, TargetInlinePayloadSourceFacts.Frame frame, TargetQuotaPayloadOwner expected) {
        final TargetResultRecord first;
        if (frame.kind() == 1) {
            final var command = CommandCodec.decodeFrame(frame.frame());
            first = result(read, commandKey(command.commandId().bytes()));
            final var retained = CommandDedupeRecord.decode(first.typedPayload());
            if (!retained.protocolTuple().equals(command.protocolTuple())
                    || !Arrays.equals(retained.commandHash(), command.commandHash())
                    || !Arrays.equals(
                            retained.result().appliedSourcePosition(),
                            frame.appliedAt().source().canonicalBytes())) {
                throw new IllegalStateException("uncommitted cause changed original accepted Command");
            }
            if (command.type() != CommandType.CANCEL
                    || retained.result().stableCode() != StableCode.PAYLOAD_RESERVATION_ABANDONED) {
                throw new IllegalStateException("uncommitted Cancel cause is not the original abandonment");
            }
        } else {
            final var mutation = SystemMutation.decodeEnvelope(frame.frame());
            first = result(read, systemKey(mutation.systemMutationId()));
            final var retained = SystemMutationResult.decode(first.typedPayload());
            if (!Arrays.equals(
                            first.typedPayload(),
                            SystemMutationResult.from(
                                            mutation,
                                            ApplyStatus.APPLIED,
                                            retained.stableCode(),
                                            frame.appliedAt().source().canonicalBytes())
                                    .encode())
                    || mutation.type() == SystemMutationType.TIME_FENCE && retained.stableCode() != StableCode.OK) {
                throw new IllegalStateException("uncommitted cause changed original accepted System input");
            }
        }
        pair(read, first, frame.appliedAt(), expected);
        return first;
    }

    public static void requireFacts(
            TargetCheckpointDependencies d, TargetQuotaPayloadOwner expected, TargetUncommittedPayloadFacts facts) {
        requireFacts(read(d), expected, facts, d.root().aggregate().mutation());
        if (d.messageState(expected.messageId()) != null
                || d.admissions().stream()
                        .anyMatch(value -> value.attempt().locator().messageId().equals(expected.messageId()))) {
            throw new IllegalStateException("uncommitted reservation unexpectedly owns a Message/Attempt");
        }
    }

    private static void requireFacts(
            Read read,
            TargetQuotaPayloadOwner expected,
            TargetUncommittedPayloadFacts facts,
            TargetQuotaMutation frontier) {
        Objects.requireNonNull(facts).requireOwner(expected);
        expected.mutation().requireAtOrBefore(frontier);
        final var reservation = facts.reservation();
        for (var key : List.of(reservation.key(), reservation.lookupKey())) {
            if (!Arrays.equals(
                    reservation.canonicalBytes(), read.get(ColumnFamily.ID, key, TargetReservationRecord.VALUE_TYPE))) {
                throw new IllegalStateException(
                        "uncommitted Native primary/lookup changed from frozen terminal reservation");
            }
        }
        if (!Arrays.equals(
                facts.binding().canonicalBytes(),
                read.get(
                        ColumnFamily.ID,
                        TargetKeyCodec.scheduleBinding(facts.binding().digest()),
                        TargetScheduleBinding.VALUE_TYPE))) {
            throw new IllegalStateException("uncommitted original binding changed");
        }
        if (facts.migrated()) {
            TargetMigrationPayloadJoins.require(
                    read::get,
                    expected,
                    facts.binding(),
                    reservation,
                    Objects.requireNonNull(facts.migrationHistory()),
                    frontier);
        } else {
            final var prepare =
                    result(read, commandKey(reservation.prepareCommandId().bytes()));
            if (!Arrays.equals(prepare.canonicalBytes(), facts.prepareFirst().canonicalBytes())
                    || !Arrays.equals(
                            result(read, positionKey(reservation.prepareAnchor().source()))
                                    .canonicalBytes(),
                            facts.preparePosition().canonicalBytes())) {
                throw new IllegalStateException("uncommitted Prepare first/POSITION changed");
            }
            final var original = CommandDedupeRecord.decode(prepare.typedPayload());
            if (!original.protocolTuple().equals(reservation.prepareTuple())
                    || !Arrays.equals(original.commandHash(), reservation.prepareCommandHash())
                    || original.result().applyStatus() != ApplyStatus.APPLIED
                    || original.result().stableCode() != StableCode.OK
                    || original.result().generation() != -1
                    || original.result().stateVersion() != 0
                    || original.result().messageStatus() != null
                    || !Arrays.equals(
                            original.result().appliedSourcePosition(),
                            reservation.prepareAnchor().source().canonicalBytes())) {
                throw new IllegalStateException("uncommitted origin lacks first successful Prepare");
            }
            pair(read, prepare, reservation.prepareAnchor(), expected);
        }
        frame(read, facts.cause(), expected);
        frame(read, facts.fence(), expected);
        if (read.get(ColumnFamily.ID, TargetKeyCodec.message(expected.messageId()), TargetMessageRecord.VALUE_TYPE)
                != null) {
            throw new IllegalStateException("uncommitted reservation has a Message");
        }
    }

    public static TargetQuotaMutation requireClosed(
            TargetCheckpointDependencies d, SystemMutation input, boolean allowApplied, Runnable current) {
        final var body = TargetResourceReclaimBody.decode(input.canonicalBody());
        final var selected = body.uncommittedOperation();
        requireFacts(d, selected.expected(), selected.facts());
        selected.facts().requireFloor(body.floor());
        final var first = d.resultRecord(systemKey(input.systemMutationId()));
        final boolean applied = first != null && first.appliedFirst();
        if (applied && !allowApplied) {
            throw new IllegalStateException("uncommitted Source stage already decided");
        }
        if (applied) {
            requireDecision(read(d), input, first.mutation(), selected.expected());
        }
        final var owner =
                Objects.requireNonNull(d.payloadOwner(selected.expected().messageId()), "uncommitted actual owner");
        final var pending = d.payloadObjectIntent(owner.messageId());
        if (owner.phase() == TargetQuotaPayloadOwner.Phase.RELEASED) {
            if (!applied || pending != null) {
                throw new IllegalStateException("uncommitted released without exact Source decision");
            }
            final var history = d.sourceHistories().stream()
                    .filter(value -> value.decidedAt().equals(owner.mutation()))
                    .findFirst()
                    .orElseThrow();
            requireRelease(read(d), body, input, first.mutation(), owner, history);
        } else {
            if (!Arrays.equals(owner.canonicalBytes(), selected.expected().canonicalBytes())) {
                throw new IllegalStateException("uncommitted owner changed after selection");
            }
            if (body.operations().getFirst()
                    instanceof TargetResourceReclaimBody.UncommittedObjectConfirm confirmation) {
                same(confirmation.intent(), Objects.requireNonNull(pending, "uncommitted confirmation lost intent"));
                requireDecision(read(d), pending.intent(), pending.appliedAt(), selected.expected());
            } else if (applied) {
                same(
                        new TargetPayloadObjectRetiringRecord(input, first.mutation()),
                        Objects.requireNonNull(pending, "uncommitted APPLIED intent missing"));
            } else if (pending != null) {
                throw new IllegalStateException("uncommitted reservation already has another intent");
            }
        }
        current.run();
        return applied ? first.mutation() : null;
    }
    /** Uses the complete physical ledger's indexes; the independent checkpoint predicate remains above. */
    public static TargetQuotaMutation requirePreparedClosed(
            TargetRetirementLedgerCursor ledger, SystemMutation input, boolean allowApplied, Runnable current) {
        final var d = Objects.requireNonNull(ledger).dependenciesForClosure();
        final var body = TargetResourceReclaimBody.decode(input.canonicalBody());
        final var selected = body.uncommittedOperation();
        requireFacts(
                read(d),
                selected.expected(),
                selected.facts(),
                d.root().aggregate().mutation());
        if (d.messageState(selected.expected().messageId()) != null
                || ledger.hasMessageAdmission(selected.expected().messageId())) {
            throw new IllegalStateException("uncommitted reservation unexpectedly owns a Message/Attempt");
        }
        selected.facts().requireFloor(body.floor());
        final var first = d.resultRecord(systemKey(input.systemMutationId()));
        final boolean applied = first != null && first.appliedFirst();
        if (applied && !allowApplied) {
            throw new IllegalStateException("uncommitted Source stage already decided");
        }
        if (applied) {
            requireDecision(read(d), input, first.mutation(), selected.expected());
        }
        final var owner =
                Objects.requireNonNull(d.payloadOwner(selected.expected().messageId()), "uncommitted actual owner");
        final var pending = d.payloadObjectIntent(owner.messageId());
        if (owner.phase() == TargetQuotaPayloadOwner.Phase.RELEASED) {
            if (!applied || pending != null) {
                throw new IllegalStateException("uncommitted released without exact Source decision");
            }
            final var release =
                    Objects.requireNonNull(ledger.resultAt(owner.mutation()), "uncommitted release Source first");
            if (release.kind() != TargetResultRecord.Kind.SYSTEM
                    || !release.appliedFirst()
                    || !release.mutation().equals(owner.mutation())) {
                throw new IllegalStateException("uncommitted release changed actual Source first");
            }
            final var history =
                    Objects.requireNonNull(d.sourceHistory(release.logicalId()), "uncommitted release Source history");
            requireRelease(read(d), body, input, first.mutation(), owner, history);
        } else {
            if (!Arrays.equals(owner.canonicalBytes(), selected.expected().canonicalBytes())) {
                throw new IllegalStateException("uncommitted owner changed after selection");
            }
            if (body.operations().getFirst()
                    instanceof TargetResourceReclaimBody.UncommittedObjectConfirm confirmation) {
                same(confirmation.intent(), Objects.requireNonNull(pending, "uncommitted confirmation lost intent"));
                requireDecision(read(d), pending.intent(), pending.appliedAt(), selected.expected());
            } else if (applied) {
                same(
                        new TargetPayloadObjectRetiringRecord(input, first.mutation()),
                        Objects.requireNonNull(pending, "uncommitted APPLIED intent missing"));
            } else if (pending != null) {
                throw new IllegalStateException("uncommitted reservation already has another intent");
            }
        }
        current.run();
        return applied ? first.mutation() : null;
    }

    public static void requireCurrent(TargetStoreBackend.Reader reader, SystemMutation input, Runnable current) {
        final var body = TargetResourceReclaimBody.decode(input.canonicalBody());
        final var selected = body.uncommittedOperation();
        final var read = read(reader, List.of());
        requireFacts(
                read, selected.expected(), selected.facts(), reader.aggregate().mutation());
        selected.facts().requireFloor(body.floor());
        final var owner = TargetQuotaPayloadOwner.decode(Objects.requireNonNull(
                read.get(ColumnFamily.META, selected.expected().key(), TargetQuotaPayloadOwner.VALUE_TYPE)));
        if (!Arrays.equals(owner.canonicalBytes(), selected.expected().canonicalBytes())) {
            throw new IllegalStateException("uncommitted actual retained owner changed");
        }
        final byte[] raw = reader.get(ColumnFamily.META, TargetPayloadObjectRetiringRecord.key(owner.messageId()));
        if (body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm confirmation) {
            final var pending = TargetPayloadObjectRetiringRecord.decode(TargetValueEnvelope.decode(
                            Objects.requireNonNull(raw), TargetPayloadObjectRetiringRecord.VALUE_TYPE)
                    .payload());
            same(confirmation.intent(), pending);
            requirePending(reader, List.of(), pending, reader.aggregate().mutation());
        } else if (raw != null) {
            throw new IllegalStateException("uncommitted intent already exists");
        }
        current.run();
    }

    public static void requirePending(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetPayloadObjectRetiringRecord pending,
            TargetQuotaMutation frontier) {
        final var read = read(view, overlay);
        final var selected = pending.uncommittedOperation();
        pending.appliedAt().requireAtOrBefore(frontier);
        selected.facts().requireFloor(pending.body().floor());
        requireFacts(read, selected.expected(), selected.facts(), frontier);
        requireDecision(read, pending.intent(), pending.appliedAt(), selected.expected());
        if (!Arrays.equals(
                selected.expected().canonicalBytes(),
                read.get(ColumnFamily.META, selected.expected().key(), TargetQuotaPayloadOwner.VALUE_TYPE))) {
            throw new IllegalStateException("pending uncommitted intent changed actual owner");
        }
    }

    public static void requireAppliedDecision(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            SystemMutation input,
            TargetQuotaMutation decided,
            TargetQuotaMutation frontier) {
        final var body = TargetResourceReclaimBody.decode(input.canonicalBody());
        final var selected = body.uncommittedOperation();
        final var read = read(view, overlay);
        requireFacts(read, selected.expected(), selected.facts(), frontier);
        decided.requireAtOrBefore(frontier);
        selected.facts().requireFloor(body.floor());
        final var original =
                body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm confirmation
                        ? confirmation.intent()
                        : new TargetPayloadObjectRetiringRecord(input, decided);
        requireDecision(read, original.intent(), original.appliedAt(), selected.expected());
        final var owner = TargetQuotaPayloadOwner.decode(Objects.requireNonNull(
                read.get(ColumnFamily.META, selected.expected().key(), TargetQuotaPayloadOwner.VALUE_TYPE)));
        final byte[] pending =
                read.get(ColumnFamily.META, original.key(), TargetPayloadObjectRetiringRecord.VALUE_TYPE);
        if (owner.phase() == TargetQuotaPayloadOwner.Phase.RETAINED) {
            if (body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm
                    || !Arrays.equals(
                            owner.canonicalBytes(), selected.expected().canonicalBytes())) {
                throw new IllegalStateException("APPLIED uncommitted confirmation retained owner");
            }
            same(original, TargetPayloadObjectRetiringRecord.decode(Objects.requireNonNull(pending)));
            return;
        }
        if (owner.phase() != TargetQuotaPayloadOwner.Phase.RELEASED || pending != null) {
            throw new IllegalStateException("invalid uncommitted release successor");
        }
        final var position = result(read, positionKey(owner.mutation().source()));
        final var history = TargetRetirementSourceHistoryRecord.decode(Objects.requireNonNull(read.get(
                ColumnFamily.META,
                TargetRetirementSourceHistoryRecord.key(position.logicalId()),
                TargetRetirementSourceHistoryRecord.VALUE_TYPE)));
        requireRelease(read, body, input, decided, owner, history);
    }

    public static TargetRetirementSourceHistoryRecord releaseHistory(
            TargetRecordAccounting.View view,
            List<TargetStoreBackend.Edit> overlay,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetQuotaPayloadOwner owner,
            TargetQuotaMutation frontier) {
        if (owner.kind() != TargetQuotaPayloadOwner.Kind.OBJECT
                || owner.phase() != TargetQuotaPayloadOwner.Phase.RELEASED
                || owner.committedPayload() != null) {
            throw new IllegalArgumentException("released uncommitted reservation owner required");
        }
        final var read = read(view, overlay);
        final var position = result(read, positionKey(owner.mutation().source()));
        final byte[] key = TargetRetirementSourceHistoryRecord.key(position.logicalId());
        final var history = TargetRetirementSourceHistoryRecord.decodeForStore(
                key,
                Objects.requireNonNull(
                        read.get(ColumnFamily.META, key, TargetRetirementSourceHistoryRecord.VALUE_TYPE)),
                scope,
                lineage);
        if (!history.authorizationTuple().equals(ProtocolTuple.targetUncommittedObjectReclaim())
                || !history.decidedAt().equals(owner.mutation())) {
            throw new IllegalStateException("released uncommitted owner changed actual Source history");
        }
        requireAppliedDecision(view, overlay, history.input(), history.decidedAt(), frontier);
        return history;
    }

    private static void requireRelease(
            Read read,
            TargetResourceReclaimBody body,
            SystemMutation input,
            TargetQuotaMutation decided,
            TargetQuotaPayloadOwner owner,
            TargetRetirementSourceHistoryRecord history) {
        final var released = TargetResourceReclaimBody.decode(history.input().canonicalBody());
        if (!(released.operations().getFirst()
                        instanceof TargetResourceReclaimBody.UncommittedObjectConfirm confirmation)
                || !history.decidedAt().equals(owner.mutation())) {
            throw new IllegalStateException("uncommitted release lost complete original confirmation");
        }
        final var expected = body.uncommittedOperation().expected();
        owner.requireReleasedFrom(expected, released.floor());
        final var original =
                body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm selected
                        ? selected.intent()
                        : new TargetPayloadObjectRetiringRecord(input, decided);
        same(original, confirmation.intent());
        requireDecision(read, history.input(), history.decidedAt(), expected);
        if (body.operations().getFirst() instanceof TargetResourceReclaimBody.UncommittedObjectConfirm
                && (!Arrays.equals(input.canonicalEnvelope(), history.input().canonicalEnvelope())
                        || !decided.equals(owner.mutation()))) {
            throw new IllegalStateException("uncommitted confirmation changed actual released Source first");
        }
    }

    private static void requireDecision(
            Read read, SystemMutation input, TargetQuotaMutation at, TargetQuotaPayloadOwner expected) {
        final var first = result(read, systemKey(input.systemMutationId()));
        pair(read, first, at, expected);
        if (!Arrays.equals(
                first.typedPayload(),
                SystemMutationResult.from(
                                input,
                                ApplyStatus.APPLIED,
                                StableCode.OK,
                                at.source().canonicalBytes())
                        .encode())) {
            throw new IllegalStateException("uncommitted stage lost original APPLIED/OK first");
        }
    }

    private static void same(TargetPayloadObjectRetiringRecord expected, TargetPayloadObjectRetiringRecord actual) {
        if (!Arrays.equals(expected.canonicalBytes(), actual.canonicalBytes())) {
            throw new IllegalStateException("uncommitted stage changed exact original intent");
        }
    }
}
