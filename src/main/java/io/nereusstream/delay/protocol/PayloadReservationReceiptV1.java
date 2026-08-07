package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Receipt emitted only after a large-payload reservation is durably prepared. */
public final class PayloadReservationReceiptV1 {
    public static final int RECEIPT_VERSION = 1;

    private final byte[] reservationId;
    private final DelayMessageId delayMessageId;
    private final ShardId shardId;
    private final SourcePosition appliedSourcePosition;
    private final long stateVersion;
    private final ProfileRefV1 objectStoreProfile;
    private final byte[] container;
    private final byte[] objectKey;
    private final long expectedLength;
    private final byte[] payloadSha256;
    private final long reservationExpiryEpochMs;
    private final PayloadProofTrustSetRefV1 trustSet;
    private final byte[] receiptPayloadDigest;

    private PayloadReservationReceiptV1(final byte[] reservationId, final DelayMessageId delayMessageId,
                                        final ShardId shardId, final SourcePosition appliedSourcePosition,
                                        final long stateVersion, final ProfileRefV1 objectStoreProfile,
                                        final byte[] container, final byte[] objectKey, final long expectedLength,
                                        final byte[] payloadSha256, final long reservationExpiryEpochMs,
                                        final PayloadProofTrustSetRefV1 trustSet, final byte[] receiptPayloadDigest) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        this.reservationId = Bytes.copy(reservationId);
        this.delayMessageId = Objects.requireNonNull(delayMessageId, "delayMessageId");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.appliedSourcePosition = Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (!shardId.equals(delayMessageId.routingId().shardId())
                || !shardId.equals(appliedSourcePosition.shardId())) {
            throw new IllegalArgumentException("reservation receipt identity does not belong to shard");
        }
        if (stateVersion <= 0 || expectedLength < 0 || reservationExpiryEpochMs < 0) {
            throw new IllegalArgumentException("invalid payload reservation receipt numbers");
        }
        this.stateVersion = stateVersion;
        this.objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKindV1.OBJECT_STORE) {
            throw new IllegalArgumentException("reservation receipt requires an OBJECT_STORE profile");
        }
        this.container = requireOpaqueName(container, "container", 1024);
        this.objectKey = requireOpaqueName(objectKey, "objectKey", 4096);
        this.expectedLength = expectedLength;
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        this.payloadSha256 = Bytes.copy(payloadSha256);
        this.reservationExpiryEpochMs = reservationExpiryEpochMs;
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
        Bytes.requireLength(receiptPayloadDigest, 32, "receiptPayloadDigest");
        this.receiptPayloadDigest = Bytes.copy(receiptPayloadDigest);
    }

    public static PayloadReservationReceiptV1 create(final byte[] reservationId,
                                                     final DelayMessageId delayMessageId,
                                                     final ShardId shardId,
                                                     final SourcePosition appliedSourcePosition,
                                                     final long stateVersion,
                                                     final ProfileRefV1 objectStoreProfile,
                                                     final byte[] container,
                                                     final byte[] objectKey,
                                                     final long expectedLength,
                                                     final byte[] payloadSha256,
                                                     final long reservationExpiryEpochMs,
                                                     final PayloadProofTrustSetRefV1 trustSet) {
        final byte[] fields = canonicalFields(reservationId, delayMessageId, shardId, appliedSourcePosition,
                stateVersion, objectStoreProfile, container, objectKey, expectedLength, payloadSha256,
                reservationExpiryEpochMs, trustSet);
        return new PayloadReservationReceiptV1(reservationId, delayMessageId, shardId, appliedSourcePosition,
                stateVersion, objectStoreProfile, container, objectKey, expectedLength, payloadSha256,
                reservationExpiryEpochMs, trustSet, Bytes.sha256(fields));
    }

    public static PayloadReservationReceiptV1 decodeFrame(final byte[] frame) {
        final ReceiptFrame.Decoded decoded = ReceiptFrame.decode(frame);
        if (decoded.kind() != ReceiptKind.PAYLOAD_RESERVATION) {
            throw new IllegalArgumentException("receipt frame is not PAYLOAD_RESERVATION");
        }
        return decodePayload(decoded.payload());
    }

    public static PayloadReservationReceiptV1 decodePayload(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PayloadReservationReceiptV1");
        QueryCodecSupport.requireNumbers(fields,
                new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
                "PayloadReservationReceiptV1");
        if (QueryCodecSupport.uint(fields.get(0), 1) != RECEIPT_VERSION) {
            throw new IllegalArgumentException("unsupported PayloadReservationReceiptV1 version");
        }
        final byte[] reservationId = QueryCodecSupport.fixed(fields.get(1), 2, 32);
        final DelayMessageId delayMessageId = new DelayMessageId(QueryCodecSupport.fixed(fields.get(2), 3,
                DelayMessageId.LENGTH));
        final RouteIncarnation route = new RouteIncarnation(QueryCodecSupport.fixed(fields.get(3), 4,
                RouteIncarnation.LENGTH));
        final int partition = QueryCodecSupport.uint32Bits(fields.get(4), 5);
        final SourcePosition sourcePosition = QueryCodecSupport.decodeSourcePosition(
                QueryCodecSupport.nested(fields.get(5), 6));
        final ShardId shardId = new ShardId(route, partition);
        final ProfileRefV1 objectStoreProfile = ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(7), 8));
        final PayloadReservationReceiptV1 result = new PayloadReservationReceiptV1(reservationId, delayMessageId,
                shardId, sourcePosition, QueryCodecSupport.uint(fields.get(6), 7), objectStoreProfile,
                QueryCodecSupport.bytes(fields.get(8), 9), QueryCodecSupport.bytes(fields.get(9), 10),
                QueryCodecSupport.uint(fields.get(10), 11), QueryCodecSupport.fixed(fields.get(11), 12, 32),
                QueryCodecSupport.uint(fields.get(12), 13),
                PayloadProofTrustSetRefV1.decode(QueryCodecSupport.nested(fields.get(13), 14)),
                QueryCodecSupport.fixed(fields.get(14), 15, 32));
        final byte[] expected = Bytes.sha256(canonicalFields(reservationId, delayMessageId, shardId, sourcePosition,
                result.stateVersion, objectStoreProfile, result.container, result.objectKey, result.expectedLength,
                result.payloadSha256, result.reservationExpiryEpochMs, result.trustSet));
        if (!Bytes.constantTimeEquals(result.receiptPayloadDigest, expected)) {
            throw new IllegalArgumentException("PayloadReservationReceipt digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.payload(), "PayloadReservationReceiptV1");
        return result;
    }

    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    public DelayMessageId delayMessageId() {
        return delayMessageId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public SourcePosition appliedSourcePosition() {
        return appliedSourcePosition;
    }

    public long stateVersion() {
        return stateVersion;
    }

    public ProfileRefV1 objectStoreProfile() {
        return objectStoreProfile;
    }

    public byte[] container() {
        return Bytes.copy(container);
    }

    public byte[] objectKey() {
        return Bytes.copy(objectKey);
    }

    public long expectedLength() {
        return expectedLength;
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public long reservationExpiryEpochMs() {
        return reservationExpiryEpochMs;
    }

    public PayloadProofTrustSetRefV1 trustSet() {
        return trustSet;
    }

    public byte[] receiptPayloadDigest() {
        return Bytes.copy(receiptPayloadDigest);
    }

    public byte[] payload() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, reservationId);
            CanonicalProtobuf.bytes(output, 3, delayMessageId.bytes());
            CanonicalProtobuf.bytes(output, 4, shardId.routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 5, shardId.partition());
            CanonicalProtobuf.bytes(output, 6, QueryCodecSupport.encodeSourcePosition(appliedSourcePosition));
            CanonicalProtobuf.uint64(output, 7, stateVersion);
            CanonicalProtobuf.bytes(output, 8, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 9, container);
            CanonicalProtobuf.bytes(output, 10, objectKey);
            CanonicalProtobuf.uint64(output, 11, expectedLength);
            CanonicalProtobuf.bytes(output, 12, payloadSha256);
            CanonicalProtobuf.int64(output, 13, reservationExpiryEpochMs);
            CanonicalProtobuf.bytes(output, 14, trustSet.canonicalBytes());
            CanonicalProtobuf.bytes(output, 15, receiptPayloadDigest);
        });
    }

    public byte[] frame() {
        return ReceiptFrame.encode(ReceiptKind.PAYLOAD_RESERVATION, payload());
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PayloadReservationReceiptV1 that)) {
            return false;
        }
        return stateVersion == that.stateVersion && expectedLength == that.expectedLength
                && reservationExpiryEpochMs == that.reservationExpiryEpochMs
                && delayMessageId.equals(that.delayMessageId) && shardId.equals(that.shardId)
                && appliedSourcePosition.equals(that.appliedSourcePosition)
                && objectStoreProfile.equals(that.objectStoreProfile) && Arrays.equals(reservationId, that.reservationId)
                && Arrays.equals(container, that.container) && Arrays.equals(objectKey, that.objectKey)
                && Arrays.equals(payloadSha256, that.payloadSha256) && trustSet.equals(that.trustSet)
                && Arrays.equals(receiptPayloadDigest, that.receiptPayloadDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(reservationId), delayMessageId, shardId, appliedSourcePosition,
                stateVersion, objectStoreProfile, Arrays.hashCode(container), Arrays.hashCode(objectKey), expectedLength,
                Arrays.hashCode(payloadSha256), reservationExpiryEpochMs, trustSet,
                Arrays.hashCode(receiptPayloadDigest));
    }

    private static byte[] canonicalFields(final byte[] reservationId, final DelayMessageId delayMessageId,
                                          final ShardId shardId, final SourcePosition sourcePosition,
                                          final long stateVersion, final ProfileRefV1 objectStoreProfile,
                                          final byte[] container, final byte[] objectKey, final long expectedLength,
                                          final byte[] payloadSha256, final long reservationExpiryEpochMs,
                                          final PayloadProofTrustSetRefV1 trustSet) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, RECEIPT_VERSION);
            CanonicalProtobuf.bytes(output, 2, reservationId);
            CanonicalProtobuf.bytes(output, 3, delayMessageId.bytes());
            CanonicalProtobuf.bytes(output, 4, shardId.routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 5, shardId.partition());
            CanonicalProtobuf.bytes(output, 6, QueryCodecSupport.encodeSourcePosition(sourcePosition));
            CanonicalProtobuf.uint64(output, 7, stateVersion);
            CanonicalProtobuf.bytes(output, 8, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 9, container);
            CanonicalProtobuf.bytes(output, 10, objectKey);
            CanonicalProtobuf.uint64(output, 11, expectedLength);
            CanonicalProtobuf.bytes(output, 12, payloadSha256);
            CanonicalProtobuf.int64(output, 13, reservationExpiryEpochMs);
            CanonicalProtobuf.bytes(output, 14, trustSet.canonicalBytes());
        });
    }

    private static byte[] requireOpaqueName(final byte[] value, final String name, final int maxLength) {
        Objects.requireNonNull(value, name);
        if (value.length == 0 || value.length > maxLength) {
            throw new IllegalArgumentException(name + " is outside the V1 bound");
        }
        return Bytes.copy(value);
    }
}
