package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Public view of a non-terminal Message generation. */
public final class ActiveMessageViewV1 implements QueryResponseBranchV1 {
    private final int generation;
    private final long stateVersion;
    private final MessageGenerationStateV1 state;
    private final long deliverAtEpochMs;
    private final long expireAtEpochMs;
    private final PublicDestinationBindingViewV1 binding;
    private final PayloadAvailabilityV1 payloadAvailability;
    private final boolean possibleDestinationDuplicate;

    public ActiveMessageViewV1(
            final int generation,
            final long stateVersion,
            final MessageGenerationStateV1 state,
            final long deliverAtEpochMs,
            final long expireAtEpochMs,
            final PublicDestinationBindingViewV1 binding,
            final PayloadAvailabilityV1 payloadAvailability,
            final boolean possibleDestinationDuplicate) {
        if (stateVersion <= 0 || deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs) {
            throw new IllegalArgumentException("invalid active message view numbers");
        }
        if (state == null || !state.active()) {
            throw new IllegalArgumentException("active view requires a non-terminal generation state");
        }
        this.generation = generation;
        this.stateVersion = stateVersion;
        this.state = state;
        this.deliverAtEpochMs = deliverAtEpochMs;
        this.expireAtEpochMs = expireAtEpochMs;
        this.binding = Objects.requireNonNull(binding, "binding");
        this.payloadAvailability = Objects.requireNonNull(payloadAvailability, "payloadAvailability");
        this.possibleDestinationDuplicate = possibleDestinationDuplicate;
    }

    public int generation() {
        return generation;
    }

    public long stateVersion() {
        return stateVersion;
    }

    public MessageGenerationStateV1 state() {
        return state;
    }

    public long deliverAtEpochMs() {
        return deliverAtEpochMs;
    }

    public long expireAtEpochMs() {
        return expireAtEpochMs;
    }

    public PublicDestinationBindingViewV1 binding() {
        return binding;
    }

    public PayloadAvailabilityV1 payloadAvailability() {
        return payloadAvailability;
    }

    public boolean possibleDestinationDuplicate() {
        return possibleDestinationDuplicate;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32Bits(output, 1, generation);
            CanonicalProtobuf.uint64(output, 2, stateVersion);
            CanonicalProtobuf.uint32(output, 3, state.wireValue());
            CanonicalProtobuf.int64(output, 4, deliverAtEpochMs);
            CanonicalProtobuf.int64(output, 5, expireAtEpochMs);
            CanonicalProtobuf.bytes(output, 6, binding.canonicalBytes());
            CanonicalProtobuf.uint32(output, 7, payloadAvailability.wireValue());
            CanonicalProtobuf.uint32(output, 8, possibleDestinationDuplicate ? 1 : 0);
        });
    }

    public static ActiveMessageViewV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ActiveMessageViewV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "ActiveMessageViewV1");
        final ActiveMessageViewV1 result = new ActiveMessageViewV1(
                QueryCodecSupport.uint32Bits(fields.get(0), 1),
                QueryCodecSupport.uint(fields.get(1), 2),
                MessageGenerationStateV1.fromWire(QueryCodecSupport.uint(fields.get(2), 3)),
                QueryCodecSupport.uint(fields.get(3), 4),
                QueryCodecSupport.uint(fields.get(4), 5),
                PublicDestinationBindingViewV1.decode(QueryCodecSupport.nested(fields.get(5), 6)),
                PayloadAvailabilityV1.fromWire(QueryCodecSupport.uint(fields.get(6), 7)),
                QueryCodecSupport.bool(fields.get(7), 8));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ActiveMessageViewV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ActiveMessageViewV1 that)) {
            return false;
        }
        return generation == that.generation
                && stateVersion == that.stateVersion
                && deliverAtEpochMs == that.deliverAtEpochMs
                && expireAtEpochMs == that.expireAtEpochMs
                && possibleDestinationDuplicate == that.possibleDestinationDuplicate
                && state == that.state
                && binding.equals(that.binding)
                && payloadAvailability == that.payloadAvailability;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                generation,
                stateVersion,
                state,
                deliverAtEpochMs,
                expireAtEpochMs,
                binding,
                payloadAvailability,
                possibleDestinationDuplicate);
    }
}
