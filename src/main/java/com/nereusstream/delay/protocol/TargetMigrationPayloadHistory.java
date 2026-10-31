package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.runtime.TargetReservationRecord;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.LegacyCheckpointStateInventory;
import com.nereusstream.delay.store.LegacyMigrationBundle;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetValueEnvelope;
import com.nereusstream.delay.store.ValueEnvelope;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Finite original custody records, authenticated by reading the complete sealed migration material. No new ledger. */
public record TargetMigrationPayloadHistory(
        byte[] rootDigest, TargetMigrationOrigin bindingOrigin, List<Evidence> evidence) {
    public static final int MAX_BYTES = TargetResourceReclaimBody.MAX_OBJECT_INTENT_BYTES;

    public enum Kind {
        RESERVATION,
        BINDING,
        RECEIPT,
        PREPARE_SOURCE,
        PREPARE_FIRST,
        PREPARE_RESULT,
        COMMIT_SOURCE,
        COMMIT_FIRST,
        COMMIT_RESULT,
        SCHEDULE_SOURCE,
        SCHEDULE_FIRST,
        SCHEDULE_RESULT
    }

    public record Evidence(Kind kind, TargetMigrationRecordReference reference, byte[] recordDigest, byte[] record) {
        public Evidence(Kind kind, TargetMigrationRecordReference reference, byte[] exactRecord) {
            this(kind, reference, Bytes.sha256(Objects.requireNonNull(exactRecord)), exactRecord);
        }

        public Evidence {
            Objects.requireNonNull(kind);
            Objects.requireNonNull(reference);
            recordDigest =
                    TargetCompatibilityCodec.assigned(recordDigest, 32, "complete original custody record digest");
            if (record != null) {
                record = Bytes.copy(record);
                if (record.length == 0 || !Arrays.equals(recordDigest, Bytes.sha256(record))) {
                    throw new IllegalArgumentException("original migration record digest mismatch");
                }
            }
        }

        @Override
        public byte[] recordDigest() {
            return Bytes.copy(recordDigest);
        }

        @Override
        public byte[] record() {
            return Bytes.copy(Objects.requireNonNull(
                    record, "read the complete exact migration object before interpreting original record bytes"));
        }

        public boolean loaded() {
            return record != null;
        }

        byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, kind.ordinal() + 1);
                CanonicalProtobuf.bytes(out, 2, reference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, recordDigest);
            });
        }

        static Evidence decode(byte[] bytes) {
            final var f = TargetCompatibilityCodec.read(
                    bytes,
                    TargetMigrationRecordReference.MAX_CANONICAL_BYTES + 48,
                    3,
                    false,
                    "migration payload evidence");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "migration payload evidence");
            final int kind = QueryCodecSupport.uint32(f.get(0), 1);
            if (kind < 1 || kind > Kind.values().length) {
                throw new IllegalArgumentException("unknown migration payload evidence kind");
            }
            return new Evidence(
                    Kind.values()[kind - 1],
                    TargetMigrationRecordReference.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                    QueryCodecSupport.fixed(f.get(2), 3, 32),
                    null);
        }
    }

    public TargetMigrationPayloadHistory {
        rootDigest = TargetCompatibilityCodec.assigned(rootDigest, 32, "actual activated migration root");
        Objects.requireNonNull(bindingOrigin);
        evidence = List.copyOf(evidence);
        if (evidence.size() != 4 && evidence.size() != 6 && evidence.size() != 9) {
            throw new IllegalArgumentException("complete original Prepare/optional Commit custody required");
        }
        long bytes = 0;
        for (int i = 0; i < evidence.size(); i++) {
            if (evidence.get(i).kind().ordinal() != (evidence.size() == 4 ? (i == 0 ? 1 : i + 8) : i)) {
                throw new IllegalArgumentException("migration payload records must be complete and ordered");
            }
            bytes = Math.addExact(bytes, evidence.get(i).canonicalBytes().length);
        }
        if (bytes > MAX_BYTES) {
            throw new IllegalArgumentException("migration payload custody exceeds existing intent bound");
        }
    }

    @Override
    public byte[] rootDigest() {
        return Bytes.copy(rootDigest);
    }

    public boolean direct() {
        return evidence.size() == 4;
    }

    public Evidence get(Kind kind) {
        return evidence.stream()
                .filter(value -> value.kind() == kind)
                .findFirst()
                .orElseThrow();
    }

    public void require(TargetMigrationRoot root, TargetScheduleBinding binding, TargetReservationRecord reservation) {
        if (!root.activated() || !Arrays.equals(root.digest(), rootDigest)) {
            throw new IllegalArgumentException("migration payload lost actual activated root");
        }
        bindingOrigin.requirePlan(root.plan());
        bindingOrigin.requireRow(
                ColumnFamily.ID,
                TargetKeyCodec.scheduleBinding(binding.digest()),
                TargetValueEnvelope.encode(TargetScheduleBinding.VALUE_TYPE, binding.canonicalBytes()));
        if (!binding.imported()
                || !Arrays.equals(binding.migrationPlanDigest(), root.plan().digest())
                || !bindingOrigin.oldRecord().equals(binding.migrationRecord())
                || !get(Kind.BINDING).reference().equals(binding.migrationRecord())) {
            throw new IllegalArgumentException("migration payload changed original full binding reference");
        }
        if (direct()) {
            if (reservation != null
                    || binding.commandType() != CommandType.SCHEDULE
                    || binding.intent().hasInlinePayload()) {
                throw new IllegalArgumentException("migration direct OBJECT has no fabricated reservation");
            }
        } else {
            final var original =
                    Objects.requireNonNull(reservation.migrationOrigin(), "explicit reservation migration origin");
            original.requirePlan(root.plan(), binding);
            if (!get(Kind.RESERVATION).reference().equals(original.originalReservation())
                    || !get(Kind.RECEIPT).reference().equals(original.receiptRecord())) {
                throw new IllegalArgumentException("migration payload changed original reservation/receipt reference");
            }
        }
        // Serialized refs are indexes. Only the concrete full immutable read below can interpret/authenticate
        // originals.
        if (evidence.stream().noneMatch(Evidence::loaded)) {
            return;
        }
        if (evidence.stream().anyMatch(value -> !value.loaded())) {
            throw new IllegalArgumentException("original migration payload material is incomplete");
        }
        final var rawBinding = raw(get(Kind.BINDING));
        final var oldBinding = ScheduleBinding.decode(
                ValueEnvelope.decode(rawBinding.value(), 4).payload());
        if (rawBinding.family() != ColumnFamily.ID
                || !Arrays.equals(rawBinding.key(), KeyCodec.idScheduleBinding(binding.messageId()))
                || !Arrays.equals(Bytes.sha256(rawBinding.key()), bindingOrigin.oldKeyDigest())
                || !Arrays.equals(Bytes.sha256(rawBinding.value()), bindingOrigin.oldValueDigest())
                || !Arrays.equals(oldBinding.canonicalBody(), binding.canonicalBody())
                || oldBinding.commandType() != binding.commandType()
                || !oldBinding.delayMessageId().equals(binding.messageId())) {
            throw new IllegalArgumentException("migration payload changed original full binding");
        }
        if (direct()) {
            if (reservation != null
                    || binding.commandType() != CommandType.SCHEDULE
                    || binding.intent().hasInlinePayload()) {
                throw new IllegalArgumentException("migration direct OBJECT has no fabricated reservation");
            }
            final var command = command(get(Kind.SCHEDULE_SOURCE), binding.bindingSource());
            if (command.type() != CommandType.SCHEDULE
                    || !command.delayMessageId().equals(binding.messageId())
                    || !Arrays.equals(command.canonicalBody(), binding.canonicalBody())) {
                throw new IllegalArgumentException("migrated OBJECT changed its actual original Schedule");
            }
            requireCreated(
                    first(command, get(Kind.SCHEDULE_FIRST), get(Kind.SCHEDULE_RESULT), binding.bindingSource()));
            return;
        }
        final var origin =
                Objects.requireNonNull(reservation.migrationOrigin(), "explicit reservation migration origin");
        origin.requirePlan(root.plan(), binding);
        if (!get(Kind.RESERVATION).reference().equals(origin.originalReservation())
                || !get(Kind.RECEIPT).reference().equals(origin.receiptRecord())) {
            throw new IllegalArgumentException("migration payload changed original reservation/receipt reference");
        }
        final var raw = raw(get(Kind.RESERVATION));
        final var old =
                PayloadReservation.decode(ValueEnvelope.decode(raw.value(), 2).payload());
        if (raw.family() != ColumnFamily.ID
                || !Arrays.equals(raw.key(), KeyCodec.idReservation(reservation.reservationId()))
                || !old.commandId().equals(reservation.prepareCommandId())
                || !Arrays.equals(old.commandHash(), reservation.prepareCommandHash())
                || !old.delayMessageId().equals(reservation.locator().messageId())
                || old.status() != origin.originalStatus()
                || old.stateVersion() != origin.originalStateVersion()
                || !Arrays.equals(
                        old.sourcePosition(), origin.originalMutationSource().canonicalBytes())
                || !Arrays.equals(
                        old.receiptAnchorSourcePosition(),
                        origin.receiptAnchorSource().canonicalBytes())
                || old.receiptAnchorStateVersion() != origin.receiptAnchorStateVersion()) {
            throw new IllegalArgumentException("migration payload changed original reservation/owner/source");
        }
        final var receipt = external(get(Kind.RECEIPT));
        if (!receipt.kind().equals("RECEIPT")
                || !Arrays.equals(
                        receipt.originalBytes(), origin.originalReceipt().frame())) {
            throw new IllegalArgumentException("migration payload changed full original receipt");
        }
        final var prepare = command(get(Kind.PREPARE_SOURCE), origin.originalPrepareSource());
        if (prepare.type() != CommandType.PREPARE_LARGE_SCHEDULE
                || !prepare.commandId().equals(reservation.prepareCommandId())
                || !Arrays.equals(prepare.commandHash(), reservation.prepareCommandHash())
                || !prepare.protocolTuple().equals(reservation.prepareTuple())
                || !Arrays.equals(prepare.canonicalBody(), binding.canonicalBody())) {
            throw new IllegalArgumentException("migration payload changed actual original Prepare frame");
        }
        final var prepared =
                first(prepare, get(Kind.PREPARE_FIRST), get(Kind.PREPARE_RESULT), origin.originalPrepareSource());
        if (prepared.applyStatus() != ApplyStatus.APPLIED
                || prepared.stableCode() != StableCode.OK
                || prepared.generation() != -1
                || prepared.stateVersion() != 0
                || prepared.messageStatus() != null) {
            throw new IllegalArgumentException("migration payload lacks actual successful original Prepare");
        }
        final var receiptBody = origin.originalReceipt();
        final var body = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        if (!Arrays.equals(receiptBody.reservationId(), old.reservationId())
                || !receiptBody.delayMessageId().equals(old.delayMessageId())
                || receiptBody.expectedLength() != old.intent().expectedPayloadLength()
                || !Arrays.equals(receiptBody.payloadSha256(), old.intent().payloadSha256())
                || receiptBody.reservationExpiryEpochMs() != old.reservationExpiryEpochMs()
                || !receiptBody.objectStoreProfile().equals(body.objectStoreProfile())
                || !receiptBody.trustSet().equals(body.trustSet())) {
            throw new IllegalArgumentException(
                    "migration payload changed receipt immutable object/Profile/trust semantics");
        }
        final var reconstructed = TargetReservationRecord.imported(
                binding,
                root.plan(),
                origin.originalReservation(),
                old,
                prepare,
                origin.receiptRecord(),
                receiptBody,
                reservation.orderingContract());
        if (!Arrays.equals(reconstructed.migrationOrigin().canonicalBytes(), origin.canonicalBytes())
                || !Arrays.equals(
                        origin.originalPrepareSource().canonicalBytes(),
                        origin.receiptAnchorSource().canonicalBytes())) {
            throw new IllegalArgumentException(
                    "migration payload changed complete original Prepare intent/receipt anchor");
        }
        if (old.status() == PayloadReservationStatus.COMMITTED) {
            if (evidence.size() != 9 || !Objects.equals(old.committedPayload(), reservation.committedPayload())) {
                throw new IllegalArgumentException("baseline Commit changed original immutable payload");
            }
            final var commit = command(get(Kind.COMMIT_SOURCE), origin.originalMutationSource());
            final var committed =
                    first(commit, get(Kind.COMMIT_FIRST), get(Kind.COMMIT_RESULT), origin.originalMutationSource());
            if (commit.type() != CommandType.COMMIT_LARGE_SCHEDULE
                    || !commit.delayMessageId().equals(old.delayMessageId())
                    || committed.applyStatus() != ApplyStatus.APPLIED
                    || committed.stableCode() != StableCode.SCHEDULED
                    || committed.generation() != 0
                    || committed.stateVersion() != 1
                    || committed.messageStatus() != MessageStatus.SCHEDULED) {
                throw new IllegalArgumentException("migration payload lacks actual first successful original Commit");
            }
            final var proof =
                    CommitLargeScheduleBody.decode(commit.canonicalBody()).proof();
            if (!proof.objectStoreProfile().equals(body.objectStoreProfile())
                    || !Arrays.equals(
                            proof.tenantRoutingScope(), root.plan().scope().tenantScope())
                    || proof.trustSetVersion() != body.trustSet().version()
                    || !Arrays.equals(proof.reservationId(), old.reservationId())
                    || !Arrays.equals(proof.container(), receiptBody.container())
                    || !Arrays.equals(proof.objectKey(), receiptBody.objectKey())
                    || !new PayloadReference(
                                    proof.objectStoreProfileHash(),
                                    proof.container(),
                                    proof.objectKey(),
                                    proof.immutableObjectVersion(),
                                    proof.etag(),
                                    proof.length(),
                                    proof.payloadSha256(),
                                    proof.reservationId(),
                                    proof.proofId())
                            .equals(old.committedPayload())) {
                throw new IllegalArgumentException("migration Commit changed original receipt/object proof");
            }
        } else if (old.status() != PayloadReservationStatus.RESERVED || evidence.size() != 6) {
            throw new IllegalArgumentException("migration payload origin was not a safe imported reservation");
        }
    }

    public static LegacyCheckpointStateInventory.RawEntry raw(Evidence e) {
        final var in = ByteBuffer.wrap(e.record());
        if (in.remaining() < 14) {
            throw new IllegalArgumentException("truncated migration raw custody");
        }
        final int kind = Byte.toUnsignedInt(in.get());
        if (kind != 1 && kind != 2) {
            throw new IllegalArgumentException("not original raw custody");
        }
        final long ordinal = in.getLong();
        final int family = Byte.toUnsignedInt(in.get());
        if (family >= ColumnFamily.values().length) {
            throw new IllegalArgumentException("unknown original raw family");
        }
        final var value =
                new LegacyCheckpointStateInventory.RawEntry(ordinal, ColumnFamily.values()[family], lp(in), lp(in));
        if (in.hasRemaining()) {
            throw new IllegalArgumentException("trailing original raw custody");
        }
        return value;
    }

    public static LegacyMigrationBundle.ExternalRecord external(Evidence e) {
        final var in = ByteBuffer.wrap(e.record());
        if (!in.hasRemaining()) {
            throw new IllegalArgumentException("not original external custody");
        }
        final byte kind = in.get();
        if (kind != 3 && kind != 4) {
            throw new IllegalArgumentException("not original external custody");
        }
        final var value = new LegacyMigrationBundle.ExternalRecord(
                new String(lp(in), java.nio.charset.StandardCharsets.UTF_8),
                lp(in),
                TargetSourcePosition.decode(lp(in)),
                lp(in));
        if (in.hasRemaining()) {
            throw new IllegalArgumentException("trailing original external custody");
        }
        return value;
    }

    private static PreparedCommand command(Evidence e, SourcePosition expected) {
        final var record = external(e);
        if (!record.kind().equals("SOURCE")
                || !Arrays.equals(record.source().canonicalBytes(), expected.canonicalBytes())) {
            throw new IllegalArgumentException("migration command changed actual original Source");
        }
        return CommandCodec.decodeFrame(record.originalBytes());
    }

    private static CommandResult first(
            PreparedCommand command, Evidence first, Evidence result, SourcePosition source) {
        final var raw = raw(first);
        final var side = raw(result);
        if (raw.family() != ColumnFamily.DEDUPE
                || side.family() != ColumnFamily.DEDUPE
                || !Arrays.equals(raw.key(), KeyCodec.dedupeCommand(command.commandId()))
                || !Arrays.equals(side.key(), KeyCodec.dedupeResult(command.commandId()))) {
            throw new IllegalArgumentException("migration original first/result keys changed");
        }
        final var in = ByteBuffer.wrap(ValueEnvelope.decode(raw.value(), 1).payload());
        final int version = in.getInt();
        final ProtocolTuple tuple =
                version == 1 ? ProtocolTuple.managedCommand() : version == 2 ? ProtocolTuple.decode(lp(in)) : null;
        if (tuple == null || in.remaining() < 36) {
            throw new IllegalArgumentException("unknown original first schema");
        }
        final byte[] hash = new byte[32];
        in.get(hash);
        final byte[] encoded = lp(in);
        if (in.hasRemaining()
                || !tuple.equals(command.protocolTuple())
                || !Arrays.equals(hash, command.commandHash())
                || !Arrays.equals(encoded, ValueEnvelope.decode(side.value(), 2).payload())) {
            throw new IllegalArgumentException("migration original first/hash/result changed");
        }
        final var value = CommandResult.decode(encoded);
        if (!Arrays.equals(value.appliedSourcePosition(), source.canonicalBytes())) {
            throw new IllegalArgumentException("migration original first Source changed");
        }
        return value;
    }

    private static void requireCreated(CommandResult result) {
        if (result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.SCHEDULED
                || result.generation() != 0
                || result.stateVersion() != 1
                || result.messageStatus() != MessageStatus.SCHEDULED) {
            throw new IllegalArgumentException("migration OBJECT requires actual first successful creation");
        }
    }

    private static byte[] lp(ByteBuffer in) {
        if (in.remaining() < 4) {
            throw new IllegalArgumentException("truncated custody length");
        }
        final int n = in.getInt();
        if (n < 0 || n > in.remaining()) {
            throw new IllegalArgumentException("invalid custody length");
        }
        final byte[] value = new byte[n];
        in.get(value);
        return value;
    }

    public byte[] canonicalBytes() {
        final byte[] bytes = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, rootDigest);
            CanonicalProtobuf.bytes(out, 3, bindingOrigin.canonicalBytes());
            for (var e : evidence) {
                CanonicalProtobuf.bytes(out, 4, e.canonicalBytes());
            }
        });
        if (bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("migration history exceeds original bounded intent");
        }
        return bytes;
    }

    public static TargetMigrationPayloadHistory decode(byte[] bytes) {
        final var f = TargetCompatibilityCodec.read(bytes, MAX_BYTES, 12, true, "migration payload history");
        if ((f.size() != 7 && f.size() != 9 && f.size() != 12) || QueryCodecSupport.uint32(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown migration payload history schema");
        }
        final List<Evidence> evidence = new ArrayList<>();
        for (int i = 3; i < f.size(); i++) {
            evidence.add(Evidence.decode(QueryCodecSupport.bytes(f.get(i), 4)));
        }
        final var value = new TargetMigrationPayloadHistory(
                QueryCodecSupport.fixed(f.get(1), 2, 32),
                TargetMigrationOrigin.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                evidence);
        QueryCodecSupport.requireCanonical(bytes, value.canonicalBytes(), "migration payload history");
        return value;
    }
}
