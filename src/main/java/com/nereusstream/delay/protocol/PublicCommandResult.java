package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Full retained public command result projection. */
public final class PublicCommandResult implements QueryResponseBranch {
    private final CommandApplyStatus status;
    private final StableCode stableCode;
    private final SourcePosition appliedSourcePosition;
    private final Integer generation;
    private final Long stateVersion;
    private final PublicDestinationBindingView binding;
    private final long fullResultRetainUntilEpochMs;

    public PublicCommandResult(
            final CommandApplyStatus status,
            final StableCode stableCode,
            final SourcePosition appliedSourcePosition,
            final Integer generation,
            final Long stateVersion,
            final PublicDestinationBindingView binding,
            final long fullResultRetainUntilEpochMs) {
        this.status = Objects.requireNonNull(status, "status");
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.appliedSourcePosition = Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if (stateVersion != null && stateVersion <= 0) {
            throw new IllegalArgumentException("stateVersion must be positive when present");
        }
        if (status == CommandApplyStatus.REJECTED && (generation != null || stateVersion != null || binding != null)) {
            throw new IllegalArgumentException("rejected result cannot carry Message fields");
        }
        if (generation == null && (stateVersion != null || binding != null)) {
            throw new IllegalArgumentException("stateVersion/binding require a Message generation");
        }
        if (fullResultRetainUntilEpochMs < appliedSourcePosition.brokerPersistenceTimeEpochMs()) {
            throw new IllegalArgumentException("full result retention deadline precedes Broker persistence time");
        }
        this.generation = generation;
        this.stateVersion = stateVersion;
        this.binding = binding;
        this.fullResultRetainUntilEpochMs = fullResultRetainUntilEpochMs;
    }

    public CommandApplyStatus status() {
        return status;
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

    public PublicDestinationBindingView binding() {
        return binding;
    }

    public long fullResultRetainUntilEpochMs() {
        return fullResultRetainUntilEpochMs;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, status.wireValue());
            CanonicalProtobuf.uint32(output, 2, stableCode.wireValue());
            CanonicalProtobuf.bytes(output, 3, QueryCodecSupport.encodeSourcePosition(appliedSourcePosition));
            if (generation != null) {
                CanonicalProtobuf.uint32Bits(output, 4, generation);
            }
            if (stateVersion != null) {
                CanonicalProtobuf.uint64(output, 5, stateVersion);
            }
            if (binding != null) {
                CanonicalProtobuf.bytes(output, 6, binding.canonicalBytes());
            }
            CanonicalProtobuf.int64(output, 7, fullResultRetainUntilEpochMs);
        });
    }

    public static PublicCommandResult decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PublicCommandResult");
        if (fields.size() < 4 || fields.size() > 7) {
            throw new IllegalArgumentException("invalid PublicCommandResult field count");
        }
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("invalid PublicCommandResult required fields");
        }
        int index = 3;
        Integer generation = null;
        Long stateVersion = null;
        PublicDestinationBindingView binding = null;
        if (index < fields.size() - 1 && fields.get(index).number() == 4) {
            generation = QueryCodecSupport.uint32Bits(fields.get(index++), 4);
        }
        if (index < fields.size() - 1 && fields.get(index).number() == 5) {
            stateVersion = QueryCodecSupport.uint(fields.get(index++), 5);
        }
        if (index < fields.size() - 1 && fields.get(index).number() == 6) {
            binding = PublicDestinationBindingView.decode(QueryCodecSupport.nested(fields.get(index++), 6));
        }
        if (index != fields.size() - 1 || fields.get(index).number() != 7) {
            throw new IllegalArgumentException("invalid PublicCommandResult optional field order");
        }
        final PublicCommandResult result = new PublicCommandResult(
                CommandApplyStatus.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                StableCode.fromWire(QueryCodecSupport.uint32(fields.get(1), 2)),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(2), 3)),
                generation,
                stateVersion,
                binding,
                QueryCodecSupport.uint(fields.get(index), 7));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublicCommandResult");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PublicCommandResult that)) {
            return false;
        }
        return fullResultRetainUntilEpochMs == that.fullResultRetainUntilEpochMs
                && status == that.status
                && stableCode == that.stableCode
                && appliedSourcePosition.equals(that.appliedSourcePosition)
                && Objects.equals(generation, that.generation)
                && Objects.equals(stateVersion, that.stateVersion)
                && Objects.equals(binding, that.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                status,
                stableCode,
                appliedSourcePosition,
                generation,
                stateVersion,
                binding,
                fullResultRetainUntilEpochMs);
    }
}
