package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Small canonical value stored beside one timeline/READY key.
 *
 * <p>The complete value is a registry protobuf. This embedded core keeps
 * the same physical identity checks (lane, lane version and exact timeline
 * work) in a fixed binary value until the registry binding is wired in.</p>
 */
public record ReadyIndexValue(
        DestinationLaneId laneId,
        long nextEligibleAtEpochMs,
        long laneVersion,
        DelayMessageId messageId,
        int generation,
        byte[] timelineKeySha256) {
    private static final int VERSION = 1;

    public ReadyIndexValue {
        if (nextEligibleAtEpochMs < 0 || laneVersion < 0) {
            throw new IllegalArgumentException("invalid READY value");
        }
        Bytes.requireLength(timelineKeySha256, 32, "timelineKeySha256");
        timelineKeySha256 = Bytes.copy(timelineKeySha256);
    }

    @Override
    public byte[] timelineKeySha256() {
        return Bytes.copy(timelineKeySha256);
    }

    public byte[] encode() {
        return ByteBuffer.allocate(4 + 32 + 8 + 8 + DelayMessageId.LENGTH + 4 + 32)
                .putInt(VERSION)
                .put(laneId.bytes())
                .putLong(nextEligibleAtEpochMs)
                .putLong(laneVersion)
                .put(messageId.bytes())
                .putInt(generation)
                .put(timelineKeySha256)
                .array();
    }

    public static ReadyIndexValue decode(final byte[] encoded) {
        final int expectedLength = 4 + 32 + 8 + 8 + DelayMessageId.LENGTH + 4 + 32;
        if (encoded.length != expectedLength) {
            throw new IllegalArgumentException("invalid READY value length");
        }
        final ByteBuffer input = ByteBuffer.wrap(encoded);
        if (input.getInt() != VERSION) {
            throw new IllegalArgumentException("unsupported READY value version");
        }
        final byte[] lane = new byte[32];
        input.get(lane);
        final long nextEligibleAt = input.getLong();
        final long laneVersion = input.getLong();
        final byte[] message = new byte[DelayMessageId.LENGTH];
        input.get(message);
        final int generation = input.getInt();
        final byte[] timelineHash = new byte[32];
        input.get(timelineHash);
        final ReadyIndexValue result = new ReadyIndexValue(
                new DestinationLaneId(lane),
                nextEligibleAt,
                laneVersion,
                new DelayMessageId(message),
                generation,
                timelineHash);
        if (!Arrays.equals(encoded, result.encode())) {
            throw new IllegalArgumentException("non-canonical READY value");
        }
        return result;
    }
}
