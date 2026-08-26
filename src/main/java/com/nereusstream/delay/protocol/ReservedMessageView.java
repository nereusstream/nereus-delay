package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Public view of a large-payload reservation before commit. */
public final class ReservedMessageView implements QueryResponseBranch {
    private final byte[] reservationId;
    private final long stateVersion;
    private final PayloadReservationState reservationState;
    private final long reservationExpiryEpochMs;
    private final PublicDestinationBindingView binding;

    public ReservedMessageView(
            final byte[] reservationId,
            final long stateVersion,
            final PayloadReservationState reservationState,
            final long reservationExpiryEpochMs,
            final PublicDestinationBindingView binding) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        if (stateVersion <= 0 || reservationExpiryEpochMs < 0) {
            throw new IllegalArgumentException("invalid reserved message view numbers");
        }
        if (reservationState != PayloadReservationState.PAYLOAD_RESERVED) {
            throw new IllegalArgumentException("ReservedMessageView must be PAYLOAD_RESERVED");
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

    public PayloadReservationState reservationState() {
        return reservationState;
    }

    public long reservationExpiryEpochMs() {
        return reservationExpiryEpochMs;
    }

    public PublicDestinationBindingView binding() {
        return binding;
    }

    public PayloadAvailability payloadAvailability() {
        return PayloadAvailability.UPLOAD_PENDING;
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

    public static ReservedMessageView decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ReservedMessageView");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "ReservedMessageView");
        if (PayloadAvailability.fromWire(QueryCodecSupport.uint(fields.get(5), 6))
                != PayloadAvailability.UPLOAD_PENDING) {
            throw new IllegalArgumentException("reserved view payload must be UPLOAD_PENDING");
        }
        final ReservedMessageView result = new ReservedMessageView(
                QueryCodecSupport.fixed(fields.get(0), 1, 32),
                QueryCodecSupport.uint(fields.get(1), 2),
                PayloadReservationState.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4),
                PublicDestinationBindingView.decode(QueryCodecSupport.nested(fields.get(4), 5)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ReservedMessageView");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ReservedMessageView that)) {
            return false;
        }
        return stateVersion == that.stateVersion
                && reservationExpiryEpochMs == that.reservationExpiryEpochMs
                && reservationState == that.reservationState
                && Arrays.equals(reservationId, that.reservationId)
                && binding.equals(that.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(reservationId), stateVersion, reservationState, reservationExpiryEpochMs, binding);
    }
}
