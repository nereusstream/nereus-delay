package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandCodec;
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
        TargetOrderState.OrderingContract orderingContract) {
    public static final int VALUE_TYPE = 38;
    public static final int MAX_CANONICAL_BYTES = TargetMessageLocator.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_CANONICAL_BYTES
            + TargetPayloadReference.MAX_CANONICAL_BYTES
            + 512;

    public TargetReservationRecord {
        Objects.requireNonNull(locator, "locator");
        Objects.requireNonNull(prepareCommandId, "prepareCommandId");
        Objects.requireNonNull(prepareTuple, "prepareTuple");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(orderingContract, "orderingContract");
        Objects.requireNonNull(prepareAnchor, "prepareAnchor").requireSourceApplied();
        Objects.requireNonNull(mutation, "mutation");
        if (mutation.isLocalMutation()
                && (!mutation.reservationExpiry() || status != PayloadReservationStatus.EXPIRED)) {
            throw new IllegalArgumentException("only expired reservations may carry a local expiry stamp");
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
                || expiryEpochMs <= prepareAnchor.source().brokerPersistenceTimeEpochMs()
                || (status == PayloadReservationStatus.COMMITTED) != (committedPayload != null)) {
            throw new IllegalArgumentException("invalid Target reservation identity/lifecycle/anchor");
        }
        if (status == PayloadReservationStatus.RESERVED) {
            if (stateVersion != 1 || !prepareAnchor.equals(mutation)) {
                throw new IllegalArgumentException("reserved projection must be the immutable Prepare anchor");
            }
        } else {
            if (Long.compareUnsigned(stateVersion, 1) <= 0) {
                throw new IllegalArgumentException("terminal reservation requires a lifecycle successor");
            }
            mutation.requireStoreSuccessorOf(prepareAnchor);
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
                || !Arrays.equals(stamp.mutationDigest(), Bytes.sha256(CommandCodec.encodeFrame(command)))) {
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
                orderingContract);
    }

    public byte[] key() {
        return Bytes.concat(
                new byte[] {TargetKeyCodec.RESERVATION_TAG, 1},
                locator.messageId().bytes());
    }

    public byte[] lookupKey() {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESERVATION_LOOKUP_TAG, 1}, reservationId);
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
                        prepareAnchor.source().canonicalBytes())
                || !Arrays.equals(
                        prepareCommandHash,
                        CommandHash.compute(
                                prepareTuple,
                                CommandType.PREPARE_LARGE_SCHEDULE,
                                prepareCommandId,
                                locator.messageId(),
                                body.retryUntilEpochMs(),
                                binding.canonicalBody()))
                || !Arrays.equals(prepareAnchor.mutationDigest(), Bytes.sha256(CommandCodec.encodeFrame(command)))
                || expiryEpochMs
                        != Math.addExact(
                                binding.bindingSource().brokerPersistenceTimeEpochMs(), body.reservationTtlMs())) {
            throw new IllegalStateException("reservation differs from its immutable full Prepare binding");
        }
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
            CanonicalProtobuf.uint32(out, 1, 1);
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
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields);
            CanonicalProtobuf.bytes(out, 15, Bytes.sha256(Bytes.utf8("nereus-delay-target-reservation\0"), fields));
        });
    }

    public static TargetReservationRecord decode(byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target reservation exceeds bounded schema");
        }
        final var f = QueryCodecSupport.read(encoded, "TargetReservationRecord");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}, "TargetReservationRecord");
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
                });
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetReservationRecord");
        return result;
    }
}
