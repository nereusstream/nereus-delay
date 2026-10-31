package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandHash;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPayloadReference;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaPayloadOwner;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TargetMigrationPlan;
import com.nereusstream.delay.protocol.TargetMigrationRecordReference;
import com.nereusstream.delay.protocol.TargetMigrationReservationOrigin;
import com.nereusstream.delay.protocol.TargetMigrationRoot;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Source-ordered Target reservation, retaining its immutable Prepare receipt anchor without a Lane identity. */
public record TargetReservationRecord(
        TargetMessageLocator locator,
        byte[] reservationId,
        CommandId prepareCommandId,
        ProtocolTuple prepareTuple,
        byte[] prepareCommandHash,
        long expiryEpochMs,
        PayloadReservationStatus status,
        long stateVersion,
        TargetQuotaMutation prepareAnchor,
        TargetQuotaMutation mutation,
        PayloadReference committedPayload,
        byte[] recoveryLineage,
        TargetOrderState.OrderingContract orderingContract,
        TargetReservationControls.Closure closure,
        TargetMigrationReservationOrigin migrationOrigin) {
    public static final int VALUE_TYPE = 38;
    public static final int MAX_CANONICAL_BYTES = TargetMessageLocator.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + TargetPayloadReference.MAX_CANONICAL_BYTES
            + 512
            + TargetReservationControls.Closure.MAX_CANONICAL_BYTES
            + TargetMigrationReservationOrigin.MAX_CANONICAL_BYTES + 17;

    public TargetReservationRecord(TargetMessageLocator locator, byte[] reservationId, CommandId prepareCommandId,
            ProtocolTuple prepareTuple, byte[] prepareCommandHash, long expiryEpochMs, PayloadReservationStatus status,
            long stateVersion, TargetQuotaMutation prepareAnchor, TargetQuotaMutation mutation, PayloadReference committedPayload,
            byte[] recoveryLineage, TargetOrderState.OrderingContract orderingContract, TargetReservationControls.Closure closure) {
        this(locator, reservationId, prepareCommandId, prepareTuple, prepareCommandHash, expiryEpochMs, status, stateVersion,
                prepareAnchor, mutation, committedPayload, recoveryLineage, orderingContract, closure, null);
    }

    public TargetReservationRecord(
            TargetMessageLocator locator,
            byte[] reservationId,
            CommandId prepareCommandId,
            ProtocolTuple prepareTuple,
            byte[] prepareCommandHash,
            long expiryEpochMs,
            PayloadReservationStatus status,
            long stateVersion,
            TargetQuotaMutation prepareAnchor,
            TargetQuotaMutation mutation,
            PayloadReference committedPayload,
            byte[] recoveryLineage,
            TargetOrderState.OrderingContract orderingContract) {
        this(
                locator,
                reservationId,
                prepareCommandId,
                prepareTuple,
                prepareCommandHash,
                expiryEpochMs,
                status,
                stateVersion,
                prepareAnchor,
                mutation,
                committedPayload,
                recoveryLineage,
                orderingContract,
                null);
    }

    public TargetReservationRecord {
        Objects.requireNonNull(locator, "locator");
        Objects.requireNonNull(prepareCommandId, "prepareCommandId");
        Objects.requireNonNull(prepareTuple, "prepareTuple");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(orderingContract, "orderingContract");
        Objects.requireNonNull(prepareAnchor, "prepareAnchor").requireSourceApplied();
        Objects.requireNonNull(mutation, "mutation");
        if (mutation.isLocalMutation()
                && !((mutation.reservationExpiry() && status == PayloadReservationStatus.EXPIRED)
                        || (mutation.reservationClosure() && status == PayloadReservationStatus.ABANDONED))) {
            throw new IllegalArgumentException("reservation local stamp must match its terminal cause");
        }
        Bytes.requireLength(reservationId, 32, "reservationId");
        Bytes.requireLength(prepareCommandHash, 32, "prepareCommandHash");
        Bytes.requireLength(recoveryLineage, 16, "lineage");
        if (locator.generation() != 0
                || stateVersion == 0
                || expiryEpochMs < 0
                || prepareTuple.recordKind() != ProtocolTuple.CLIENT_COMMAND
                || !locator.messageId()
                        .routingId()
                        .shardId()
                        .equals(prepareCommandId.routingId().shardId())
                || !locator.messageId()
                        .routingId()
                        .shardId()
                        .equals(prepareAnchor.source().shardId())
                || !locator.messageId()
                        .routingId()
                        .shardId()
                        .equals(mutation.source().shardId())
                || Arrays.equals(recoveryLineage, new byte[16])
                || !Arrays.equals(reservationId, id(prepareCommandId, locator, prepareCommandHash))
                || expiryEpochMs <= (migrationOrigin == null ? prepareAnchor.source() : migrationOrigin.originalPrepareSource()).brokerPersistenceTimeEpochMs()
                || (status == PayloadReservationStatus.COMMITTED) != (committedPayload != null)) {
            throw new IllegalArgumentException("invalid Target reservation identity/lifecycle/anchor");
        }
        if ((closure != null) != mutation.reservationClosure()) {
            throw new IllegalArgumentException("local Close requires its durable closure evidence");
        }
        if (closure != null
                && (!closure.target().equals(locator.target())
                        || !Arrays.equals(closure.bindingDigest(), locator.scheduleBindingDigest())
                        || !Arrays.equals(closure.recoveryLineage(), recoveryLineage)
                        || !closure.source().shardId().equals(mutation.source().shardId())
                        || closure.source().compareTo(migrationOrigin == null ? prepareAnchor.source() : migrationOrigin.originalPrepareSource()) <= 0
                        || closure.source().compareTo(mutation.source()) > 0
                        || (closure.source().compareTo(mutation.source()) == 0
                                && !Arrays.equals(
                                        closure.source().canonicalBytes(),
                                        mutation.source().canonicalBytes()))
                        || closure.fenceAtClose() >= expiryEpochMs)) {
            throw new IllegalArgumentException("closure does not prove Close-before-expiry for this reservation");
        }
        if (status == PayloadReservationStatus.RESERVED) {
            if ((migrationOrigin == null ? stateVersion != 1 : stateVersion < migrationOrigin.receiptAnchorStateVersion())
                    || !prepareAnchor.equals(mutation)) {
                throw new IllegalArgumentException("reserved projection must be the immutable Prepare anchor");
            }
        } else if (migrationOrigin != null && mutation.equals(migrationOrigin.importedAt())) {
            if (status != migrationOrigin.originalStatus() || stateVersion != migrationOrigin.originalStateVersion()
                    || status != PayloadReservationStatus.COMMITTED) {
                throw new IllegalArgumentException("initial imported terminal reservation lost its complete original state");
            }
        } else {
            if (Long.compareUnsigned(stateVersion, migrationOrigin == null ? 1 : migrationOrigin.receiptAnchorStateVersion()) <= 0) {
                throw new IllegalArgumentException("terminal reservation requires a lifecycle successor");
            }
            mutation.requireStoreSuccessorOf(prepareAnchor);
        }
        if (migrationOrigin != null) {
            final var receipt = migrationOrigin.originalReceipt();
            if (!prepareAnchor.equals(migrationOrigin.importedAt()) || !locator.messageId().equals(receipt.delayMessageId())
                    || !Arrays.equals(reservationId, receipt.reservationId()) || expiryEpochMs != receipt.reservationExpiryEpochMs()) {
                throw new IllegalArgumentException("migration reservation changed original immutable receipt/explicit import anchor");
            }
        }
        if (committedPayload != null) {
            TargetPayloadReference.requireBounded(committedPayload);
            if (!Arrays.equals(committedPayload.reservationId(), reservationId)) {
                throw new IllegalArgumentException("committed object belongs to another reservation");
            }
        }
        reservationId = Bytes.copy(reservationId);
        prepareCommandHash = Bytes.copy(prepareCommandHash);
        recoveryLineage = Bytes.copy(recoveryLineage);
    }

    @Override
    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    @Override
    public byte[] prepareCommandHash() {
        return Bytes.copy(prepareCommandHash);
    }

    @Override
    public byte[] recoveryLineage() {
        return Bytes.copy(recoveryLineage);
    }

    public static TargetReservationRecord prepare(
            TargetMessageLocator locator,
            PreparedCommand command,
            TargetQuotaMutation stamp,
            long expiry,
            byte[] lineage,
            TargetOrderState.OrderingContract orderingContract) {
        if (command.type() != CommandType.PREPARE_LARGE_SCHEDULE
                || !command.delayMessageId().equals(locator.messageId())
                || !Arrays.equals(stamp.mutationDigest(), command.canonicalFrameDigest())) {
            throw new IllegalArgumentException("reservation is not anchored to its exact Prepare frame");
        }
        return new TargetReservationRecord(
                locator,
                id(command.commandId(), locator, command.commandHash()),
                command.commandId(),
                command.protocolTuple(),
                command.commandHash(),
                expiry,
                PayloadReservationStatus.RESERVED,
                1,
                stamp,
                stamp,
                null,
                lineage,
                orderingContract);
    }

    public static TargetReservationRecord imported(TargetScheduleBinding binding, TargetMigrationPlan plan,
            TargetMigrationRecordReference originalRow, PayloadReservation original, PreparedCommand originalPrepare,
            TargetMigrationRecordReference receiptRecord, PayloadReservationReceipt originalReceipt,
            TargetOrderState.OrderingContract orderingContract) {
        Objects.requireNonNull(original); Objects.requireNonNull(originalPrepare);
        if (!binding.imported() || original.status() != PayloadReservationStatus.RESERVED && original.status() != PayloadReservationStatus.COMMITTED
                || originalPrepare.type() != CommandType.PREPARE_LARGE_SCHEDULE
                || !original.commandId().equals(originalPrepare.commandId()) || !original.delayMessageId().equals(binding.messageId())
                || !Arrays.equals(original.commandHash(), originalPrepare.commandHash())
                || !Arrays.equals(binding.canonicalBody(), originalPrepare.canonicalBody())) {
            throw new IllegalArgumentException("imported reservation changed complete original accepted Prepare identity");
        }
        final var body = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        final var oldIntent = original.intent();
        if (oldIntent.deliverAtEpochMs() != body.intentWithoutPayload().deliverAtEpochMs()
                || oldIntent.expireAtEpochMs() != body.intentWithoutPayload().expireAtEpochMs()
                || oldIntent.orderingMode() != body.intentWithoutPayload().orderingMode()
                || oldIntent.expectedPayloadLength() != body.expectedPayloadLength()
                || !Arrays.equals(oldIntent.payloadSha256(), body.payloadSha256())
                || oldIntent.reservationTtlMs() != body.reservationTtlMs() || oldIntent.payloadProofTrustSetVersion() != body.trustSet().version()) {
            throw new IllegalArgumentException("imported reservation changed original complete large-payload intent");
        }
        final var at = binding.schemaVersion() == 2 ? TargetMigrationRoot.baselineFor(plan) : binding.migrationSource();
        final var origin = new TargetMigrationReservationOrigin(plan.digest(), plan.migrationId(), originalRow,
                binding.bindingSource(), original.receiptAnchorStateVersion(), SourcePositionCodec.decode(original.receiptAnchorSourcePosition()),
                at, receiptRecord, originalReceipt, original.stateVersion(), original.status(), SourcePositionCodec.decode(original.sourcePosition()));
        origin.requirePlan(plan, binding);
        final var locator = new TargetMessageLocator(binding.messageId(), 0, binding.target(), binding.domain(),
                binding.accountingIncarnation(), binding.intent().orderingMode(), binding.orderingDomain(), binding.digest());
        final var record = new TargetReservationRecord(locator, original.reservationId(), original.commandId(), originalPrepare.protocolTuple(),
                original.commandHash(), original.reservationExpiryEpochMs(), original.status(), original.stateVersion(),
                at, at, original.committedPayload(), plan.newLineage(), orderingContract, null, origin);
        record.requireBinding(binding); return record;
    }

    public int schemaVersion() { return migrationOrigin == null ? 1 : 2; }
    public SourcePosition originalPrepareSource() { return migrationOrigin == null ? prepareAnchor.source() : migrationOrigin.originalPrepareSource(); }
    public long receiptAnchorStateVersion() { return migrationOrigin == null ? 1 : migrationOrigin.receiptAnchorStateVersion(); }
    public SourcePosition receiptAnchorSource() { return migrationOrigin == null ? prepareAnchor.source() : migrationOrigin.receiptAnchorSource(); }

    private static byte[] id(CommandId command, TargetMessageLocator locator, byte[] hash) {
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-reservation-id\0"),
                command.bytes(),
                locator.messageId().bytes(),
                hash);
    }

    /** Logical time-fence overlay; does not materialize a lifecycle version, source stamp or quota transfer. */
    public PayloadReservationStatus effectiveStatus(final long closedIngressDeadlineThrough) {
        if (closedIngressDeadlineThrough < -1) {
            throw new IllegalArgumentException("invalid committed ingress fence watermark");
        }
        return status == PayloadReservationStatus.RESERVED && expiryEpochMs <= closedIngressDeadlineThrough
                ? PayloadReservationStatus.EXPIRED
                : status;
    }

    public TargetReservationRecord finish(
            PayloadReservationStatus next, TargetQuotaMutation stamp, PayloadReference payload) {
        if (status != PayloadReservationStatus.RESERVED || next == PayloadReservationStatus.RESERVED) {
            throw new IllegalStateException("reservation lifecycle is already terminal");
        }
        stamp.requireStoreSuccessorOf(mutation);
        return new TargetReservationRecord(
                locator,
                reservationId,
                prepareCommandId,
                prepareTuple,
                prepareCommandHash,
                expiryEpochMs,
                next,
                TargetQueueState.nextRevision(stateVersion),
                prepareAnchor,
                stamp,
                payload,
                recoveryLineage,
                orderingContract, null, migrationOrigin);
    }

    public TargetReservationRecord finishClosed(TargetQuotaMutation stamp, TargetReservationControls.Closure evidence) {
        if (status != PayloadReservationStatus.RESERVED || !stamp.reservationClosure()) {
            throw new IllegalStateException(
                    "Close materialization requires an uncommitted reservation and typed stamp");
        }
        stamp.requireStoreSuccessorOf(mutation);
        return new TargetReservationRecord(
                locator,
                reservationId,
                prepareCommandId,
                prepareTuple,
                prepareCommandHash,
                expiryEpochMs,
                PayloadReservationStatus.ABANDONED,
                TargetQueueState.nextRevision(stateVersion),
                prepareAnchor,
                stamp,
                null,
                recoveryLineage,
                orderingContract,
                Objects.requireNonNull(evidence, "closure"), migrationOrigin);
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESERVATION_TAG, 1},
                locator.messageId().bytes());
    }

    public byte[] lookupKey() {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_LOOKUP_TAG, 1}, reservationId);
    }

    /** Present only while physically RESERVED; supports Target-local closing independently of expiry order. */
    public byte[] targetIndexKey() {
        return TargetKeyCodec.targetReservation(locator.target(), locator.messageId());
    }

    public byte[] expiryKey() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESERVATION_EXPIRY_TAG, 1},
                Bytes.u64be(expiryEpochMs),
                locator.messageId().bytes());
    }

    public void requireBinding(TargetScheduleBinding binding) {
        binding.requireLocator(locator);
        final var body = PrepareLargeScheduleBody.decode(binding.canonicalBody());
        final var command = new PreparedCommand(
                locator.messageId().routingId().shardId(),
                prepareCommandId,
                locator.messageId(),
                CommandType.PREPARE_LARGE_SCHEDULE,
                prepareTuple,
                body.retryUntilEpochMs(),
                binding.canonicalBody(),
                prepareCommandHash);
        if (binding.commandType() != CommandType.PREPARE_LARGE_SCHEDULE
                || !Arrays.equals(
                        binding.bindingSource().canonicalBytes(),
                        originalPrepareSource().canonicalBytes())
                || !Arrays.equals(
                        prepareCommandHash,
                        CommandHash.compute(
                                prepareTuple,
                                CommandType.PREPARE_LARGE_SCHEDULE,
                                prepareCommandId,
                                locator.messageId(),
                                body.retryUntilEpochMs(),
                                binding.canonicalBody()))
                || (migrationOrigin == null && !Arrays.equals(prepareAnchor.mutationDigest(), command.canonicalFrameDigest()))
                || expiryEpochMs
                        != Math.addExact(
                                binding.bindingSource().brokerPersistenceTimeEpochMs(), body.reservationTtlMs())) {
            throw new IllegalStateException("reservation differs from its immutable full Prepare binding");
        }
        if (migrationOrigin != null) {
            final var receipt = migrationOrigin.originalReceipt();
            if (!binding.imported() || !Arrays.equals(binding.migrationPlanDigest(), migrationOrigin.planDigest())
                    || !receipt.objectStoreProfile().equals(body.objectStoreProfile()) || receipt.expectedLength() != body.expectedPayloadLength()
                    || !Arrays.equals(receipt.payloadSha256(), body.payloadSha256()) || !receipt.trustSet().equals(body.trustSet())) {
                throw new IllegalStateException("imported reservation changed original full receipt/Profile/trust-set");
            }
        } else if (binding.imported()) throw new IllegalStateException("imported reservation lacks explicit immutable receipt origin");
    }

    public void requireOwner(TargetQuotaPayloadOwner owner) {
        mutation.requireAtOrBefore(owner.mutation());
        if (status == PayloadReservationStatus.RESERVED && !prepareAnchor.equals(owner.mutation())) {
            throw new IllegalStateException("reserved payload owner changed the Prepare anchor");
        }
        if (!owner.messageId().equals(locator.messageId())
                || !owner.primaryIdentity().target().equals(locator.target())
                || !Arrays.equals(owner.primaryIdentity().accountingIncarnation(), locator.accountingIncarnation())
                || !Arrays.equals(owner.initialBindingDigest(), locator.scheduleBindingDigest())
                || !Arrays.equals(owner.reservationId(), reservationId)
                || !Arrays.equals(owner.recoveryLineage(), recoveryLineage)
                || !Objects.equals(owner.committedPayload(), committedPayload)
                || (status == PayloadReservationStatus.RESERVED
                        && owner.phase() != TargetQuotaPayloadOwner.Phase.RESERVED)
                || (status != PayloadReservationStatus.RESERVED
                        && owner.phase() == TargetQuotaPayloadOwner.Phase.RESERVED)
                || ((status == PayloadReservationStatus.ABANDONED || status == PayloadReservationStatus.EXPIRED)
                        && owner.phase() == TargetQuotaPayloadOwner.Phase.ACTIVE)) {
            throw new IllegalStateException("reservation differs from its actual frozen payload owner");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion());
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, reservationId);
            CanonicalProtobuf.bytes(out, 4, prepareCommandId.bytes());
            CanonicalProtobuf.bytes(out, 5, prepareTuple.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, prepareCommandHash);
            CanonicalProtobuf.uint64(out, 7, expiryEpochMs);
            CanonicalProtobuf.uint32(out, 8, status.wireValue());
            CanonicalProtobuf.uint64Bits(out, 9, stateVersion);
            CanonicalProtobuf.bytes(out, 10, prepareAnchor.canonicalBytes());
            CanonicalProtobuf.bytes(out, 11, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 12, committedPayload == null ? new byte[0] : committedPayload.encode());
            CanonicalProtobuf.bytes(out, 13, recoveryLineage);
            CanonicalProtobuf.uint32(out, 14, orderingContract.wireValue());
        });
    }

    public byte[] canonicalBytes() {
        final byte[] fields = fields();
        if (migrationOrigin != null) {
            final byte[] core = CanonicalProtobuf.message(out -> {
                out.writeBytes(fields);
                if (closure != null) CanonicalProtobuf.bytes(out, 16, closure.canonicalBytes());
                CanonicalProtobuf.bytes(out, 17, migrationOrigin.canonicalBytes());
            });
            return CanonicalProtobuf.message(out -> {
                out.writeBytes(core); CanonicalProtobuf.bytes(out, 18,
                        Bytes.sha256(Bytes.utf8("nereus-delay-target-migration-reservation-v2\0"), core));
            });
        }
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            final byte[] evidence = closure == null
                    ? new byte[0]
                    : CanonicalProtobuf.message(value -> CanonicalProtobuf.bytes(value, 16, closure.canonicalBytes()));
            CanonicalProtobuf.bytes(
                    out, 15, Bytes.sha256(Bytes.utf8("nereus-delay-target-reservation\0"), fields, evidence));
            out.writeBytes(evidence);
        });
    }

    public static TargetReservationRecord decode(byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target reservation exceeds bounded schema");
        }
        final var f = QueryCodecSupport.read(encoded, "TargetReservationRecord");
        final int schema = QueryCodecSupport.uint32(f.getFirst(), 1);
        if (schema == 2) return decodeImported(encoded, f);
        QueryCodecSupport.requireNumbers(
                f,
                f.size() == 16
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                "TargetReservationRecord");
        if (QueryCodecSupport.uint(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown Target reservation version");
        }
        final byte[] object = QueryCodecSupport.bytes(f.get(11), 12);
        final var result = new TargetReservationRecord(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                new CommandId(QueryCodecSupport.fixed(f.get(3), 4, CommandId.LENGTH)),
                ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                QueryCodecSupport.fixed(f.get(5), 6, 32),
                QueryCodecSupport.uint(f.get(6), 7),
                PayloadReservationStatus.fromWire(QueryCodecSupport.uint32(f.get(7), 8)),
                QueryCodecSupport.uint64Bits(f.get(8), 9),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(9), 10)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(10), 11)),
                object.length == 0 ? null : TargetPayloadReference.decode(object),
                QueryCodecSupport.fixed(f.get(12), 13, 16),
                switch (QueryCodecSupport.uint32(f.get(13), 14)) {
                    case 1 -> TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO;
                    case 2 -> TargetOrderState.OrderingContract.ADMISSION_WATERMARK;
                    default -> throw new IllegalArgumentException("unknown reservation ordering contract");
                },
                f.size() == 16
                        ? TargetReservationControls.Closure.decode(QueryCodecSupport.bytes(f.get(15), 16))
                        : null);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetReservationRecord");
        return result;
    }

    private static TargetReservationRecord decodeImported(byte[] encoded, java.util.List<CanonicalProtobuf.Reader.Field> f) {
        final boolean closed = f.size() == 17;
        QueryCodecSupport.requireNumbers(f, closed ? new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,16,17,18}
                : new int[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,17,18}, "imported Target reservation");
        final byte[] object = QueryCodecSupport.bytes(f.get(11), 12);
        final var result = new TargetReservationRecord(TargetMessageLocator.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 32), new CommandId(QueryCodecSupport.fixed(f.get(3), 4, CommandId.LENGTH)),
                ProtocolTuple.decode(QueryCodecSupport.bytes(f.get(4), 5)), QueryCodecSupport.fixed(f.get(5), 6, 32),
                QueryCodecSupport.uint(f.get(6), 7), PayloadReservationStatus.fromWire(QueryCodecSupport.uint32(f.get(7), 8)),
                QueryCodecSupport.uint64Bits(f.get(8), 9), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(9), 10)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(10), 11)), object.length == 0 ? null : TargetPayloadReference.decode(object),
                QueryCodecSupport.fixed(f.get(12), 13, 16), switch (QueryCodecSupport.uint32(f.get(13), 14)) {
                    case 1 -> TargetOrderState.OrderingContract.LEGACY_DELIVERY_TIME_FIFO;
                    case 2 -> TargetOrderState.OrderingContract.ADMISSION_WATERMARK;
                    default -> throw new IllegalArgumentException("unknown imported reservation ordering contract"); },
                closed ? TargetReservationControls.Closure.decode(QueryCodecSupport.bytes(f.get(14), 16)) : null,
                TargetMigrationReservationOrigin.decode(QueryCodecSupport.bytes(f.get(closed ? 15 : 14), 17)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "imported Target reservation"); return result;
    }
}
