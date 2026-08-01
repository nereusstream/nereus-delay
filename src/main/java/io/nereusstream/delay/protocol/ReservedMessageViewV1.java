package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Public view of a large-payload reservation before commit. */
public final class ReservedMessageViewV1 implements QueryResponseBranchV1 {
    private final byte[] reservationId;
    private final long stateVersion;
    private final PayloadReservationStateV1 reservationState;
    private final long reservationExpiryEpochMs;
    private final PublicDestinationBindingViewV1 binding;

    public ReservedMessageViewV1(final byte[] reservationId, final long stateVersion,
                                 final PayloadReservationStateV1 reservationState,
                                 final long reservationExpiryEpochMs,
                                 final PublicDestinationBindingViewV1 binding) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        if (stateVersion <= 0 || reservationExpiryEpochMs < 0) {
            throw new IllegalArgumentException("invalid reserved message view numbers");
        }
        if (reservationState != PayloadReservationStateV1.PAYLOAD_RESERVED) {
            throw new IllegalArgumentException("ReservedMessageViewV1 must be PAYLOAD_RESERVED");
        }
        this.reservationId = Bytes.copy(reservationId);
        this.stateVersion = stateVersion;
        this.reservationState = Objects.requireNonNull(reservationState, "reservationState");
        this.reservationExpiryEpochMs = reservationExpiryEpochMs;
        this.binding = Objects.requireNonNull(binding, "binding");
    }

    public byte[] reservationId() {
        return Bytes.copy(reservationId);
    }

    public long stateVersion() {
        return stateVersion;
    }

    public PayloadReservationStateV1 reservationState() {
        return reservationState;
    }

    public long reservationExpiryEpochMs() {
        return reservationExpiryEpochMs;
    }

    public PublicDestinationBindingViewV1 binding() {
        return binding;
    }

    public PayloadAvailabilityV1 payloadAvailability() {
        return PayloadAvailabilityV1.UPLOAD_PENDING;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reservationId);
            CanonicalProtobuf.uint64(output, 2, stateVersion);
            CanonicalProtobuf.uint32(output, 3, reservationState.wireValue());
            CanonicalProtobuf.int64(output, 4, reservationExpiryEpochMs);
            CanonicalProtobuf.bytes(output, 5, binding.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, payloadAvailability().wireValue());
        });
    }

    public static ReservedMessageViewV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ReservedMessageViewV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6}, "ReservedMessageViewV1");
        if (PayloadAvailabilityV1.fromWire(QueryCodecSupport.uint(fields.get(5), 6))
                != PayloadAvailabilityV1.UPLOAD_PENDING) {
            throw new IllegalArgumentException("reserved view payload must be UPLOAD_PENDING");
        }
        final ReservedMessageViewV1 result = new ReservedMessageViewV1(
                QueryCodecSupport.fixed(fields.get(0), 1, 32),
                QueryCodecSupport.uint(fields.get(1), 2),
                PayloadReservationStateV1.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4),
                PublicDestinationBindingViewV1.decode(QueryCodecSupport.nested(fields.get(4), 5)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ReservedMessageViewV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ReservedMessageViewV1 that)) {
            return false;
        }
        return stateVersion == that.stateVersion && reservationExpiryEpochMs == that.reservationExpiryEpochMs
                && reservationState == that.reservationState && Arrays.equals(reservationId, that.reservationId)
                && binding.equals(that.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(reservationId), stateVersion, reservationState,
                reservationExpiryEpochMs, binding);
    }
}
