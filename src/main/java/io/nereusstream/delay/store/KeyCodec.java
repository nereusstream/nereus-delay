package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;

import java.util.Objects;

/** Registered RocksDB key prefixes and fixed layouts used by the core state machine. */
public final class KeyCodec {
    private KeyCodec() {
    }

    public static byte[] idMessage(final DelayMessageId messageId) {
        return Bytes.concat(new byte[]{1, 1}, messageId.bytes());
    }

    public static byte[] dedupeCommand(final CommandId commandId) {
        return Bytes.concat(new byte[]{1, 1}, commandId.bytes());
    }

    public static byte[] dedupeResult(final CommandId commandId) {
        return Bytes.concat(new byte[]{2, 1}, commandId.bytes());
    }

    public static byte[] dedupePosition(final byte[] canonicalSourcePosition) {
        return Bytes.concat(new byte[]{3, 1}, canonicalSourcePosition);
    }

    public static byte[] timelineDue(final DestinationLaneId laneId, final long eligibleAtEpochMs,
                                     final byte[] sourceOrderToken, final DelayMessageId messageId,
                                     final int generation) {
        Objects.requireNonNull(sourceOrderToken, "sourceOrderToken");
        if (eligibleAtEpochMs < 0 || generation < 0) {
            throw new IllegalArgumentException("invalid timeline key values");
        }
        return Bytes.concat(new byte[]{1, 1}, laneId.bytes(), Bytes.u64be(eligibleAtEpochMs),
                Bytes.lp32(sourceOrderToken), messageId.bytes(), Bytes.u32be(generation));
    }

    public static byte[] timelineOrdered(final DestinationLaneId laneId, final long deliverAtEpochMs,
                                         final byte[] sourceOrderToken, final DelayMessageId messageId,
                                         final int generation) {
        if (deliverAtEpochMs < 0 || generation < 0) {
            throw new IllegalArgumentException("invalid timeline key values");
        }
        return Bytes.concat(new byte[]{2, 1}, laneId.bytes(), Bytes.u64be(deliverAtEpochMs),
                Bytes.lp32(sourceOrderToken), messageId.bytes(), Bytes.u32be(generation));
    }

    public static byte[] timelineReady(final long nextEligibleAtEpochMs, final DestinationLaneId laneId,
                                       final long laneVersion) {
        if (nextEligibleAtEpochMs < 0 || laneVersion < 0) {
            throw new IllegalArgumentException("invalid READY key values");
        }
        return Bytes.concat(new byte[]{3, 1}, Bytes.u64be(nextEligibleAtEpochMs), laneId.bytes(),
                Bytes.u64be(laneVersion));
    }

    public static byte[] metaFixed(final int fixedKeyKind) {
        if (fixedKeyKind <= 0 || fixedKeyKind > 10) {
            throw new IllegalArgumentException("unknown FIXED meta key kind");
        }
        return new byte[]{1, 1, (byte) fixedKeyKind};
    }

    public static byte[] metaLane(final DestinationLaneId laneId) {
        return Bytes.concat(new byte[]{2, 1}, laneId.bytes());
    }
}

