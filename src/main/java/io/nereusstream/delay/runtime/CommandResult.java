package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/** Durable, queryable result of one applied or rejected Command. */
public record CommandResult(
        ApplyStatus applyStatus,
        StableCode stableCode,
        int generation,
        long stateVersion,
        MessageStatus messageStatus,
        byte[] appliedSourcePosition) {
    public CommandResult {
        Objects.requireNonNull(applyStatus, "applyStatus");
        Objects.requireNonNull(stableCode, "stableCode");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        if ((generation < -1 && stateVersion == 0 && messageStatus == null) || stateVersion < 0) {
            throw new IllegalArgumentException("invalid command result");
        }
        // A durable result is only meaningful when its source-order anchor is
        // a complete canonical Source Position.  Without this constructor
        // fence, an empty or non-canonical byte string could survive in a
        // result value until a later shard-specific lookup happened to read
        // it, allowing other local projections to observe an invalid anchor.
        appliedSourcePosition = SourcePositionCodec.decode(appliedSourcePosition).canonicalBytes();
    }

    @Override
    public byte[] appliedSourcePosition() {
        return Bytes.copy(appliedSourcePosition);
    }

    /** Whether this applied result carries a real Message generation projection. */
    public boolean hasGeneration() {
        return applyStatus == ApplyStatus.APPLIED && (stateVersion > 0 || messageStatus != null);
    }

    public byte[] encode() {
        final ByteBuffer result = ByteBuffer.allocate(4 + 1 + 4 + 4 + 8 + 1 + 4 + appliedSourcePosition.length);
        result.putInt(1).put((byte) applyStatus.wireValue()).putInt(stableCode.wireValue()).putInt(generation)
                .putLong(stateVersion).put((byte) (messageStatus == null ? 0 : messageStatus.wireValue()))
                .putInt(appliedSourcePosition.length).put(appliedSourcePosition);
        return result.array();
    }

    public static CommandResult decode(final byte[] encoded) {
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.remaining() < 4 + 1 + 4 + 4 + 8 + 1 + 4) {
            throw new IllegalArgumentException("command result is truncated");
        }
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported command result version");
        }
        final ApplyStatus status = switch (input.get() & 0xff) {
            case 1 -> ApplyStatus.APPLIED;
            case 2 -> ApplyStatus.REJECTED;
            default -> throw new IllegalArgumentException("unknown apply status");
        };
        final int stableCode = input.getInt();
        final StableCode code = java.util.Arrays.stream(StableCode.values())
                .filter(candidate -> candidate.wireValue() == stableCode)
                .findFirst().orElseThrow(() -> new IllegalArgumentException("unknown stable code: " + stableCode));
        final int generation = input.getInt();
        final long stateVersion = input.getLong();
        final int messageStatus = input.get() & 0xff;
        final MessageStatus decodedStatus = messageStatus == 0 ? null : MessageStatus.fromWire(messageStatus);
        final int sourceLength = input.getInt();
        if (sourceLength < 0 || sourceLength != input.remaining()) {
            throw new IllegalArgumentException("invalid source position length");
        }
        final byte[] source = new byte[sourceLength];
        input.get(source);
        return new CommandResult(status, code, generation, stateVersion, decodedStatus, source);
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof CommandResult that)) {
            return false;
        }
        return applyStatus == that.applyStatus && stableCode == that.stableCode && generation == that.generation
                && stateVersion == that.stateVersion && messageStatus == that.messageStatus
                && Arrays.equals(appliedSourcePosition, that.appliedSourcePosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applyStatus, stableCode, generation, stateVersion, messageStatus,
                Arrays.hashCode(appliedSourcePosition));
    }
}
