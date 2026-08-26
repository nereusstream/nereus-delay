package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Pulsar-only NDR1 native delivery receipt with no managed query authority. */
public final class NativeDeliveryReceipt {
    public static final int RECEIPT_VERSION = 1;
    public static final int CAPABILITY_BITS = 0;
    private static final int ATTEMPT_ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;

    private final NativePreparedRef prepared;
    private final CanonicalCommandQueuedReceipt.PulsarQueuedAck brokerAck;
    private final byte[] physicalEnqueueAttemptId;
    private final byte[] payloadDigest;

    private NativeDeliveryReceipt(
            final NativePreparedRef prepared,
            final CanonicalCommandQueuedReceipt.PulsarQueuedAck brokerAck,
            final byte[] physicalEnqueueAttemptId,
            final byte[] payloadDigest) {
        this.prepared = Objects.requireNonNull(prepared, "prepared");
        this.brokerAck = Objects.requireNonNull(brokerAck, "brokerAck");
        if (brokerAck.partition() != prepared.physicalPartition()
                || !brokerAck.authenticatedClusterId().equals(prepared.target().authenticatedClusterId())
                || !Arrays.equals(
                        brokerAck.brokerResourceIncarnation(), prepared.target().resourceIncarnation())
                || !brokerAck.physicalTopic().equals(prepared.target().physicalTopic())
                || brokerAck.physicalTopicCreationTimestamp()
                        != prepared.target().physicalTopicCreationTimestamp()) {
            throw new IllegalArgumentException("native receipt Broker ACK does not match prepared target");
        }
        requireNonZero(physicalEnqueueAttemptId, ATTEMPT_ID_LENGTH, "physicalEnqueueAttemptId");
        Bytes.requireLength(payloadDigest, HASH_LENGTH, "payloadDigest");
        this.physicalEnqueueAttemptId = Bytes.copy(physicalEnqueueAttemptId);
        this.payloadDigest = Bytes.copy(payloadDigest);
    }

    public static NativeDeliveryReceipt create(
            final NativePreparedRef prepared,
            final CanonicalCommandQueuedReceipt.PulsarQueuedAck brokerAck,
            final byte[] physicalEnqueueAttemptId) {
        final byte[] fields = canonicalFields(prepared, brokerAck, physicalEnqueueAttemptId);
        return new NativeDeliveryReceipt(prepared, brokerAck, physicalEnqueueAttemptId, Bytes.sha256(fields));
    }

    public static NativeDeliveryReceipt decodeFrame(final byte[] frame) {
        final ReceiptFrame.Decoded decoded = ReceiptFrame.decode(frame);
        if (decoded.kind() != ReceiptKind.NATIVE_DELIVERY) {
            throw new IllegalArgumentException("receipt frame is not NATIVE_DELIVERY");
        }
        return decodePayload(decoded.payload());
    }

    public static NativeDeliveryReceipt decodePayload(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "NativeDeliveryReceipt");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "NativeDeliveryReceipt");
        if (QueryCodecSupport.uint(fields.get(0), 1) != RECEIPT_VERSION
                || QueryCodecSupport.uint(fields.get(3), 4) != CAPABILITY_BITS) {
            throw new IllegalArgumentException("unsupported NativeDeliveryReceipt version or capability bits");
        }
        final NativePreparedRef prepared = NativePreparedRef.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final CanonicalCommandQueuedReceipt.SafeBrokerAck safeAck =
                CanonicalCommandQueuedReceipt.SafeBrokerAck.decode(QueryCodecSupport.nested(fields.get(2), 3));
        if (!(safeAck instanceof CanonicalCommandQueuedReceipt.PulsarQueuedAck brokerAck)) {
            throw new IllegalArgumentException("native receipt requires Pulsar Broker ACK");
        }
        final byte[] attempt = QueryCodecSupport.fixed(fields.get(4), 5, ATTEMPT_ID_LENGTH);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(5), 6, HASH_LENGTH);
        final byte[] expected = Bytes.sha256(canonicalFields(prepared, brokerAck, attempt));
        if (!Bytes.constantTimeEquals(digest, expected)) {
            throw new IllegalArgumentException("NativeDeliveryReceipt payload digest mismatch");
        }
        final NativeDeliveryReceipt result = new NativeDeliveryReceipt(prepared, brokerAck, attempt, digest);
        QueryCodecSupport.requireCanonical(encoded, result.payload(), "NativeDeliveryReceipt");
        return result;
    }

    public NativePreparedRef prepared() {
        return prepared;
    }

    public CanonicalCommandQueuedReceipt.PulsarQueuedAck brokerAck() {
        return brokerAck;
    }

    public byte[] physicalEnqueueAttemptId() {
        return Bytes.copy(physicalEnqueueAttemptId);
    }

    public byte[] payloadDigest() {
        return Bytes.copy(payloadDigest);
    }

    public byte[] payload() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, prepared.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, brokerAck.canonicalBytes());
            CanonicalProtobuf.uint32(output, 4, CAPABILITY_BITS);
            CanonicalProtobuf.bytes(output, 5, physicalEnqueueAttemptId);
            CanonicalProtobuf.bytes(output, 6, payloadDigest);
        });
    }

    public byte[] frame() {
        return ReceiptFrame.encode(ReceiptKind.NATIVE_DELIVERY, payload());
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof NativeDeliveryReceipt that)) {
            return false;
        }
        return prepared.equals(that.prepared)
                && brokerAck.equals(that.brokerAck)
                && Arrays.equals(physicalEnqueueAttemptId, that.physicalEnqueueAttemptId)
                && Arrays.equals(payloadDigest, that.payloadDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                prepared, brokerAck, Arrays.hashCode(physicalEnqueueAttemptId), Arrays.hashCode(payloadDigest));
    }

    private static byte[] canonicalFields(
            final NativePreparedRef prepared,
            final CanonicalCommandQueuedReceipt.PulsarQueuedAck brokerAck,
            final byte[] attempt) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, prepared.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, brokerAck.canonicalBytes());
            CanonicalProtobuf.uint32(output, 4, CAPABILITY_BITS);
            CanonicalProtobuf.bytes(output, 5, attempt);
        });
    }

    private static void requireNonZero(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        for (byte item : value) {
            if (item != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }
}
