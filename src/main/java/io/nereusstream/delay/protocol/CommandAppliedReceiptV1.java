package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical receipt proving that a command was durably applied or rejected by
 * one Shard.  It never replaces the queued ingress receipt.
 */
public final class CommandAppliedReceiptV1 {
    public static final int RECEIPT_VERSION = 1;
    private static final int HASH_LENGTH = 32;

    private final byte[] queuedReceiptPayloadDigest;
    private final CommandApplyStatusV1 applyStatus;
    private final StableCode stableCode;
    private final SourcePosition appliedSourcePosition;
    private final Integer generation;
    private final Long stateVersion;
    private final PublicDestinationBindingViewV1 binding;
    private final long fullResultRetainUntilEpochMs;
    private final byte[] receiptPayloadDigest;

    private CommandAppliedReceiptV1(final byte[] queuedReceiptPayloadDigest,
                                    final CommandApplyStatusV1 applyStatus,
                                    final StableCode stableCode,
                                    final SourcePosition appliedSourcePosition,
                                    final Integer generation,
                                    final Long stateVersion,
                                    final PublicDestinationBindingViewV1 binding,
                                    final long fullResultRetainUntilEpochMs,
                                    final byte[] receiptPayloadDigest) {
        Bytes.requireLength(queuedReceiptPayloadDigest, HASH_LENGTH, "queuedReceiptPayloadDigest");
        this.queuedReceiptPayloadDigest = Bytes.copy(queuedReceiptPayloadDigest);
        this.applyStatus = Objects.requireNonNull(applyStatus, "applyStatus");
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.appliedSourcePosition = Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (stateVersion != null && stateVersion <= 0) {
            throw new IllegalArgumentException("stateVersion must be positive when present");
        }
        if (generation == null && (stateVersion != null || binding != null)) {
            throw new IllegalArgumentException("stateVersion/binding require a Message generation");
        }
        if (applyStatus == CommandApplyStatusV1.REJECTED
                && (generation != null || stateVersion != null || binding != null)) {
            throw new IllegalArgumentException("rejected result cannot fabricate Message fields");
        }
        if (fullResultRetainUntilEpochMs < appliedSourcePosition.brokerPersistenceTimeEpochMs()) {
            throw new IllegalArgumentException("full result retention deadline precedes Broker persistence time");
        }
        Bytes.requireLength(receiptPayloadDigest, HASH_LENGTH, "receiptPayloadDigest");
        this.generation = generation;
        this.stateVersion = stateVersion;
        this.binding = binding;
        this.fullResultRetainUntilEpochMs = fullResultRetainUntilEpochMs;
        this.receiptPayloadDigest = Bytes.copy(receiptPayloadDigest);
    }

    public static CommandAppliedReceiptV1 create(final CommandQueuedReceiptV1 queuedReceipt,
                                                 final CommandApplyStatusV1 applyStatus,
                                                 final StableCode stableCode,
                                                 final SourcePosition appliedSourcePosition,
                                                 final Integer generation,
                                                 final Long stateVersion,
                                                 final PublicDestinationBindingViewV1 binding,
                                                 final long fullResultRetainUntilEpochMs) {
        Objects.requireNonNull(queuedReceipt, "queuedReceipt");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        validateAppliedPosition(queuedReceipt.sourcePosition(), appliedSourcePosition);
        final byte[] fields = canonicalFields(queuedReceipt.receiptPayloadDigest(), applyStatus, stableCode,
                appliedSourcePosition, generation, stateVersion, binding, fullResultRetainUntilEpochMs);
        return new CommandAppliedReceiptV1(queuedReceipt.receiptPayloadDigest(), applyStatus, stableCode,
                appliedSourcePosition, generation, stateVersion, binding, fullResultRetainUntilEpochMs,
                Bytes.sha256(Bytes.utf8("nereus-delay-command-applied-receipt-v1\0"), fields));
    }

    public static CommandAppliedReceiptV1 decodeFrame(final byte[] frame) {
        final ReceiptFrame.Decoded decoded = ReceiptFrame.decode(frame);
        if (decoded.kind() != ReceiptKind.COMMAND_APPLIED) {
            throw new IllegalArgumentException("receipt frame is not COMMAND_APPLIED");
        }
        return decodePayload(decoded.payload());
    }

    public static CommandAppliedReceiptV1 decodePayload(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "CommandAppliedReceiptV1");
        if (fields.size() < 7 || fields.size() > 10) {
            throw new IllegalArgumentException("invalid CommandAppliedReceiptV1 field count");
        }
        if (fields.get(0).number() != 1 || fields.get(1).number() != 2 || fields.get(2).number() != 3
                || fields.get(3).number() != 4 || fields.get(4).number() != 5) {
            throw new IllegalArgumentException("invalid CommandAppliedReceiptV1 required fields");
        }
        int index = 5;
        Integer generation = null;
        Long stateVersion = null;
        PublicDestinationBindingViewV1 binding = null;
        while (index < fields.size() - 2) {
            switch (fields.get(index).number()) {
                case 6 -> generation = QueryCodecSupport.uint32Bits(fields.get(index++), 6);
                case 7 -> stateVersion = QueryCodecSupport.uint(fields.get(index++), 7);
                case 8 -> binding = PublicDestinationBindingViewV1.decode(
                        QueryCodecSupport.nested(fields.get(index++), 8));
                default -> throw new IllegalArgumentException("invalid CommandAppliedReceiptV1 optional field");
            }
        }
        if (index != fields.size() - 2 || fields.get(index).number() != 9) {
            throw new IllegalArgumentException("invalid CommandAppliedReceiptV1 optional field order");
        }
        final byte[] digest = QueryCodecSupport.fixed(fields.get(index + 1), 10, HASH_LENGTH);
        final byte[] queuedDigest = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        final CommandAppliedReceiptV1 result = new CommandAppliedReceiptV1(queuedDigest,
                CommandApplyStatusV1.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(3), 4)),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(4), 5)), generation,
                stateVersion, binding, QueryCodecSupport.uint(fields.get(index), 9), digest);
        final byte[] expected = Bytes.sha256(Bytes.utf8("nereus-delay-command-applied-receipt-v1\0"),
                canonicalFields(queuedDigest, result.applyStatus, result.stableCode, result.appliedSourcePosition,
                        generation, stateVersion, binding, result.fullResultRetainUntilEpochMs));
        if (!Bytes.constantTimeEquals(digest, expected)) {
            throw new IllegalArgumentException("CommandAppliedReceipt payload digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.payload(), "CommandAppliedReceiptV1");
        return result;
    }

    public byte[] queuedReceiptPayloadDigest() {
        return Bytes.copy(queuedReceiptPayloadDigest);
    }

    public CommandApplyStatusV1 applyStatus() {
        return applyStatus;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public SourcePosition appliedSourcePosition() {
        return appliedSourcePosition;
    }

    public Integer generation() {
        return generation;
    }

    public Long stateVersion() {
        return stateVersion;
    }

    public PublicDestinationBindingViewV1 binding() {
        return binding;
    }

    public long fullResultRetainUntilEpochMs() {
        return fullResultRetainUntilEpochMs;
    }

    public byte[] receiptPayloadDigest() {
        return Bytes.copy(receiptPayloadDigest);
    }

    public byte[] payload() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, queuedReceiptPayloadDigest);
            CanonicalProtobuf.uint32(output, 3, applyStatus.wireValue());
            CanonicalProtobuf.uint32(output, 4, stableCode.wireValue());
            CanonicalProtobuf.bytes(output, 5, QueryCodecSupport.encodeSourcePosition(appliedSourcePosition));
            if (generation != null) {
                CanonicalProtobuf.uint32Bits(output, 6, generation);
            }
            if (stateVersion != null) {
                CanonicalProtobuf.uint64(output, 7, stateVersion);
            }
            if (binding != null) {
                CanonicalProtobuf.bytes(output, 8, binding.canonicalBytes());
            }
            CanonicalProtobuf.int64(output, 9, fullResultRetainUntilEpochMs);
            CanonicalProtobuf.bytes(output, 10, receiptPayloadDigest);
        });
    }

    public byte[] frame() {
        return ReceiptFrame.encode(ReceiptKind.COMMAND_APPLIED, payload());
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof CommandAppliedReceiptV1 that)) {
            return false;
        }
        return fullResultRetainUntilEpochMs == that.fullResultRetainUntilEpochMs
                && applyStatus == that.applyStatus && stableCode == that.stableCode
                && Arrays.equals(queuedReceiptPayloadDigest, that.queuedReceiptPayloadDigest)
                && appliedSourcePosition.equals(that.appliedSourcePosition)
                && Objects.equals(generation, that.generation) && Objects.equals(stateVersion, that.stateVersion)
                && Objects.equals(binding, that.binding)
                && Arrays.equals(receiptPayloadDigest, that.receiptPayloadDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(queuedReceiptPayloadDigest), applyStatus, stableCode,
                appliedSourcePosition, generation, stateVersion, binding, fullResultRetainUntilEpochMs,
                Arrays.hashCode(receiptPayloadDigest));
    }

    private static byte[] canonicalFields(final byte[] queuedDigest, final CommandApplyStatusV1 applyStatus,
                                          final StableCode stableCode, final SourcePosition appliedPosition,
                                          final Integer generation, final Long stateVersion,
                                          final PublicDestinationBindingViewV1 binding,
                                          final long fullResultRetainUntilEpochMs) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, queuedDigest);
            CanonicalProtobuf.uint32(output, 3, applyStatus.wireValue());
            CanonicalProtobuf.uint32(output, 4, stableCode.wireValue());
            CanonicalProtobuf.bytes(output, 5, QueryCodecSupport.encodeSourcePosition(appliedPosition));
            if (generation != null) {
                CanonicalProtobuf.uint32Bits(output, 6, generation);
            }
            if (stateVersion != null) {
                CanonicalProtobuf.uint64(output, 7, stateVersion);
            }
            if (binding != null) {
                CanonicalProtobuf.bytes(output, 8, binding.canonicalBytes());
            }
            CanonicalProtobuf.int64(output, 9, fullResultRetainUntilEpochMs);
        });
    }

    private static void validateAppliedPosition(final SourcePosition queuedPosition,
                                                final SourcePosition appliedPosition) {
        if (!queuedPosition.sameSourceIdentity(appliedPosition) || !queuedPosition.shardId().equals(
                appliedPosition.shardId())) {
            throw new IllegalArgumentException("applied Source Position is not after queued position");
        }
        final int order = appliedPosition.compareTo(queuedPosition);
        if (order < 0 || (order == 0 && !Bytes.constantTimeEquals(appliedPosition.canonicalBytes(),
                queuedPosition.canonicalBytes()))) {
            throw new IllegalArgumentException("applied Source Position is not the exact queued position");
        }
    }
}
