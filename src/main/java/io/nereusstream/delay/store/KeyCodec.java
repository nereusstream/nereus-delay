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

    public static byte[] timelineExpiry(final long expireAtEpochMs, final DestinationLaneId laneId,
                                        final DelayMessageId messageId, final int generation) {
        if (expireAtEpochMs < 0 || generation < 0) {
            throw new IllegalArgumentException("invalid expiry key values");
        }
        return Bytes.concat(new byte[]{4, 1}, Bytes.u64be(expireAtEpochMs), laneId.bytes(), messageId.bytes(),
                Bytes.u32be(generation));
    }

    public static byte[] reservationExpiry(final long expireAtEpochMs, final byte[] reservationId) {
        Objects.requireNonNull(reservationId, "reservationId");
        if (expireAtEpochMs < 0 || reservationId.length != 32) {
            throw new IllegalArgumentException("invalid reservation expiry key values");
        }
        return Bytes.concat(new byte[]{5, 1}, Bytes.u64be(expireAtEpochMs), reservationId);
    }

    public static byte[] timelineSystem(final byte systemWorkKind, final long nextEligibleAtEpochMs,
                                        final byte[] workId, final long workVersion) {
        Objects.requireNonNull(workId, "workId");
        if (systemWorkKind <= 0 || nextEligibleAtEpochMs < 0 || workId.length == 0 || workVersion < 0) {
            throw new IllegalArgumentException("invalid system timeline key values");
        }
        return Bytes.concat(new byte[]{6, 1, systemWorkKind}, Bytes.u64be(nextEligibleAtEpochMs),
                Bytes.lp32(workId), Bytes.u64be(workVersion));
    }

    public static byte[] idReservation(final byte[] reservationId) {
        return typedIdentity((byte) 2, reservationId, "reservationId");
    }

    public static byte[] idPayloadRef(final byte[] payloadId) {
        return typedIdentity((byte) 3, payloadId, "payloadId");
    }

    public static byte[] inflight(final byte recordKind, final long ownerEpoch, final byte[] attemptId) {
        Objects.requireNonNull(attemptId, "attemptId");
        if (recordKind < 1 || recordKind > 3 || ownerEpoch < 0 || attemptId.length == 0) {
            throw new IllegalArgumentException("invalid inflight key values");
        }
        return Bytes.concat(new byte[]{recordKind, 1}, Bytes.u64be(ownerEpoch), Bytes.lp32(attemptId));
    }

    public static byte[] terminalGeneration(final DelayMessageId messageId, final int generation) {
        Objects.requireNonNull(messageId, "messageId");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        return Bytes.concat(new byte[]{1, 1}, messageId.bytes(), Bytes.u32be(generation));
    }

    public static byte[] gcTask(final long notBeforeEpochMs, final byte kind, final byte[] resourceId,
                                final long expectedVersion) {
        Objects.requireNonNull(resourceId, "resourceId");
        if (notBeforeEpochMs < 0 || kind <= 0 || expectedVersion < 0 || resourceId.length == 0) {
            throw new IllegalArgumentException("invalid GC key values");
        }
        return Bytes.concat(new byte[]{1, 1}, Bytes.u64be(notBeforeEpochMs), new byte[]{kind},
                Bytes.lp32(resourceId), Bytes.u64be(expectedVersion));
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

    public static byte[] metaScheduler(final int schedulerKeyKind) {
        if (schedulerKeyKind <= 0 || schedulerKeyKind > 5) {
            throw new IllegalArgumentException("unknown SCHEDULER meta key kind");
        }
        return Bytes.concat(new byte[]{5, 1, (byte) schedulerKeyKind});
    }

    private static byte[] typedIdentity(final byte tag, final byte[] identity, final String name) {
        Objects.requireNonNull(identity, name);
        if (identity.length != 32) {
            throw new IllegalArgumentException(name + " must be 32 bytes");
        }
        return Bytes.concat(new byte[]{tag, 1}, identity);
    }
}
