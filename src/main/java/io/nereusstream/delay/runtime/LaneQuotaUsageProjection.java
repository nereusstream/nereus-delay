package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.LaneQuotaUsageEntryV1;
import io.nereusstream.delay.protocol.LaneQuotaUsageMapV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Immutable local projection for the Registry {@code meta/QUOTA} per-Lane map.
 *
 * <p>The projection deliberately uses the same checked arithmetic boundary as
 * the shard aggregate.  It currently owns the message, reservation, Lane and
 * durable Claim/attempt inflight dimensions that the compatibility runtime can
 * account exactly; retained and external-adapter dimensions remain zero until
 * their respective durable ledgers are wired into the runtime.</p>
 */
public final class LaneQuotaUsageProjection {
    private static final int INCARNATION_LENGTH = 16;

    private final LaneQuotaUsageMapV1 map;

    private LaneQuotaUsageProjection(final LaneQuotaUsageMapV1 map) {
        this.map = Objects.requireNonNull(map, "map");
    }

    public static LaneQuotaUsageProjection empty() {
        return new LaneQuotaUsageProjection(new LaneQuotaUsageMapV1(List.of()));
    }

    public static LaneQuotaUsageProjection decode(final byte[] encoded) {
        return new LaneQuotaUsageProjection(LaneQuotaUsageMapV1.decode(encoded));
    }

    public LaneQuotaUsageMapV1 map() {
        return map;
    }

    public byte[] canonicalBytes() {
        return map.canonicalBytes();
    }

    /** Ensures that one still-live Lane slot is represented in the map. */
    public LaneQuotaUsageProjection ensureLane(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                               final long usageRevision) {
        requireRevision(usageRevision);
        Objects.requireNonNull(laneId, "laneId");
        final byte[] incarnation = fixedIncarnation(laneIncarnation);
        final LaneQuotaUsageEntryV1 current = find(laneId, incarnation);
        if (current != null && current.usage().laneCount() > 0) {
            return this;
        }
        return update(laneId, incarnation, usageRevision, current == null,
                new long[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0}, false);
    }

    /** Adds one non-terminal inline/object-backed Message generation. */
    public LaneQuotaUsageProjection addSchedule(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                                final long payloadBytes, final boolean newLane,
                                                final long usageRevision) {
        requireRevision(usageRevision);
        requireBytes(payloadBytes);
        return update(laneId, laneIncarnation, usageRevision, newLane,
                new long[]{1, payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, newLane ? 1 : 0, 0},
                false);
    }

    /** Releases one non-terminal Message generation. */
    public LaneQuotaUsageProjection removeSchedule(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                                   final long payloadBytes, final long usageRevision) {
        requireRevision(usageRevision);
        requireBytes(payloadBytes);
        return update(laneId, laneIncarnation, usageRevision, false,
                new long[]{-1, -payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                false);
    }

    /** Adds one uncommitted Payload Reservation. */
    public LaneQuotaUsageProjection addReservation(final DestinationLaneId laneId,
                                                   final byte[] laneIncarnation, final long payloadBytes,
                                                   final boolean newLane, final long usageRevision) {
        requireRevision(usageRevision);
        requireBytes(payloadBytes);
        return update(laneId, laneIncarnation, usageRevision, newLane,
                new long[]{0, 0, 0, 0, 1, payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0, newLane ? 1 : 0, 0},
                false);
    }

    /** Releases one uncommitted Payload Reservation. */
    public LaneQuotaUsageProjection removeReservation(final DestinationLaneId laneId,
                                                      final byte[] laneIncarnation, final long payloadBytes,
                                                      final long usageRevision) {
        requireRevision(usageRevision);
        requireBytes(payloadBytes);
        return update(laneId, laneIncarnation, usageRevision, false,
                new long[]{0, 0, 0, 0, -1, -payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                false);
    }

    /** Converts one Reservation into one non-terminal Message generation. */
    public LaneQuotaUsageProjection commitReservation(final DestinationLaneId laneId,
                                                      final byte[] laneIncarnation, final long payloadBytes,
                                                      final long usageRevision) {
        requireRevision(usageRevision);
        requireBytes(payloadBytes);
        return update(laneId, laneIncarnation, usageRevision, false,
                new long[]{1, payloadBytes, 0, 0, -1, -payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
                false);
    }

    /** Adds the persisted execution charge for one Claim or attempt obligation. */
    public LaneQuotaUsageProjection addInflight(final DestinationLaneId laneId,
                                                final byte[] laneIncarnation, final long messages,
                                                final long bytes, final long usageRevision) {
        requireRevision(usageRevision);
        requireCount(messages, "inflightMessages");
        requireBytes(bytes);
        return update(laneId, laneIncarnation, usageRevision, false,
                new long[]{0, 0, 0, 0, 0, 0, messages, bytes, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false);
    }

    /** Releases the persisted execution charge for one Claim or attempt obligation. */
    public LaneQuotaUsageProjection removeInflight(final DestinationLaneId laneId,
                                                   final byte[] laneIncarnation, final long messages,
                                                   final long bytes, final long usageRevision) {
        requireRevision(usageRevision);
        requireCount(messages, "inflightMessages");
        requireBytes(bytes);
        return update(laneId, laneIncarnation, usageRevision, false,
                new long[]{0, 0, 0, 0, 0, 0, -messages, -bytes, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false);
    }

    /**
     * Transfers unadmitted Lane work to the close materializer.  The Lane
     * remains active until its terminal guard is installed, so its Lane slot
     * is intentionally retained here.
     */
    public LaneQuotaUsageProjection removeClosedWork(final DestinationLaneId laneId,
                                                     final byte[] laneIncarnation, final long messages,
                                                     final long messageBytes, final long reservations,
                                                     final long reservationBytes, final long usageRevision) {
        requireRevision(usageRevision);
        requireCount(messages, "messages");
        requireCount(reservations, "reservations");
        requireBytes(messageBytes);
        requireBytes(reservationBytes);
        return update(laneId, laneIncarnation, usageRevision, false,
                new long[]{-messages, -messageBytes, 0, 0, -reservations, -reservationBytes,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, false);
    }

    /** Removes the active Lane slot when the same key becomes a terminal guard. */
    public LaneQuotaUsageProjection removeLane(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                               final long usageRevision) {
        requireRevision(usageRevision);
        final LaneQuotaUsageEntryV1 current = requireEntry(laneId, laneIncarnation);
        final PublishAdmissionBody.ChargeVector usage = current.usage();
        if (usage.activeMessages() != 0 || usage.pendingPayloadBytes() != 0
                || usage.reservationMessages() != 0 || usage.reservationPayloadBytes() != 0
                || usage.inflightMessages() != 0 || usage.inflightBytes() != 0) {
            throw new IllegalStateException("Lane quota still has live usage");
        }
        final PublishAdmissionBody.ChargeVector next = change(usage,
                new long[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, 0});
        return replace(laneId, laneIncarnation, new LaneQuotaUsageEntryV1(laneId, laneIncarnation, next,
                usageRevision));
    }

    private LaneQuotaUsageProjection update(final DestinationLaneId laneId, final byte[] laneIncarnation,
                                            final long usageRevision, final boolean newLane,
                                            final long[] deltas, final boolean removeIfEmpty) {
        Objects.requireNonNull(laneId, "laneId");
        final byte[] incarnation = fixedIncarnation(laneIncarnation);
        final LaneQuotaUsageEntryV1 current = find(laneId, incarnation);
        if (current == null && !newLane) {
            throw new IllegalStateException("missing per-Lane quota entry");
        }
        if (current != null && !Arrays.equals(current.laneIncarnation(), incarnation)) {
            throw new IllegalStateException("per-Lane quota incarnation mismatch");
        }
        final PublishAdmissionBody.ChargeVector prior = current == null
                ? zeroUsage() : current.usage();
        final PublishAdmissionBody.ChargeVector next = change(prior, deltas);
        final LaneQuotaUsageEntryV1 replacement = new LaneQuotaUsageEntryV1(laneId, incarnation, next,
                usageRevision);
        return replace(laneId, incarnation, replacement, removeIfEmpty);
    }

    private LaneQuotaUsageProjection replace(final DestinationLaneId laneId, final byte[] incarnation,
                                             final LaneQuotaUsageEntryV1 replacement) {
        return replace(laneId, incarnation, replacement, true);
    }

    private LaneQuotaUsageProjection replace(final DestinationLaneId laneId, final byte[] incarnation,
                                             final LaneQuotaUsageEntryV1 replacement, final boolean removeIfEmpty) {
        final List<LaneQuotaUsageEntryV1> entries = new ArrayList<>(map.entries());
        entries.removeIf(entry -> entry.laneId().equals(laneId)
                && Arrays.equals(entry.laneIncarnation(), incarnation));
        if (!removeIfEmpty || hasUsage(replacement.usage())) {
            entries.add(replacement);
        }
        final List<LaneQuotaUsageEntryV1> normalized = entries.stream().map(entry ->
                entry.usageRevision() == replacement.usageRevision() ? entry
                        : new LaneQuotaUsageEntryV1(entry.laneId(), entry.laneIncarnation(), entry.usage(),
                        replacement.usageRevision())).toList();
        return new LaneQuotaUsageProjection(new LaneQuotaUsageMapV1(normalized.stream().sorted(
                (left, right) -> {
                    int result = Arrays.compareUnsigned(left.laneId().bytes(), right.laneId().bytes());
                    return result != 0 ? result
                            : Arrays.compareUnsigned(left.laneIncarnation(), right.laneIncarnation());
                }).toList()));
    }

    private LaneQuotaUsageEntryV1 requireEntry(final DestinationLaneId laneId, final byte[] incarnation) {
        final LaneQuotaUsageEntryV1 result = find(laneId, fixedIncarnation(incarnation));
        if (result == null) {
            throw new IllegalStateException("missing per-Lane quota entry");
        }
        return result;
    }

    private LaneQuotaUsageEntryV1 find(final DestinationLaneId laneId, final byte[] incarnation) {
        for (LaneQuotaUsageEntryV1 entry : map.entries()) {
            if (entry.laneId().equals(laneId)) {
                if (!Arrays.equals(entry.laneIncarnation(), incarnation)) {
                    throw new IllegalStateException("per-Lane quota incarnation mismatch");
                }
                return entry;
            }
        }
        return null;
    }

    private static PublishAdmissionBody.ChargeVector change(
            final PublishAdmissionBody.ChargeVector usage, final long[] deltas) {
        if (deltas.length != 17) {
            throw new IllegalArgumentException("ChargeVector delta must contain 17 dimensions");
        }
        final long[] values = {usage.activeMessages(), usage.pendingPayloadBytes(), usage.logicalStateBytes(),
                usage.retainedBytes(), usage.reservationMessages(), usage.reservationPayloadBytes(),
                usage.inflightMessages(), usage.inflightBytes(), usage.resultRecords(), usage.resultBytes(),
                usage.systemMutationRecords(), usage.systemMutationBytes(), usage.outcomeWalBytes(),
                usage.evidenceRecords(), usage.evidenceBytes(), usage.laneCount(), usage.strongLaneCount()};
        for (int index = 0; index < values.length; index++) {
            try {
                values[index] = Math.addExact(values[index], deltas[index]);
            } catch (ArithmeticException exception) {
                throw new IllegalStateException("per-Lane quota arithmetic overflow", exception);
            }
            if (values[index] < 0) {
                throw new IllegalStateException("per-Lane quota usage underflow");
            }
        }
        return new PublishAdmissionBody.ChargeVector(values[0], values[1], values[2], values[3], values[4],
                values[5], values[6], values[7], values[8], values[9], values[10], values[11], values[12],
                values[13], values[14], values[15], values[16]);
    }

    private static PublishAdmissionBody.ChargeVector zeroUsage() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static boolean hasUsage(final PublishAdmissionBody.ChargeVector usage) {
        return usage.activeMessages() != 0 || usage.pendingPayloadBytes() != 0 || usage.logicalStateBytes() != 0
                || usage.retainedBytes() != 0 || usage.reservationMessages() != 0
                || usage.reservationPayloadBytes() != 0 || usage.inflightMessages() != 0
                || usage.inflightBytes() != 0 || usage.resultRecords() != 0 || usage.resultBytes() != 0
                || usage.systemMutationRecords() != 0 || usage.systemMutationBytes() != 0
                || usage.outcomeWalBytes() != 0 || usage.evidenceRecords() != 0 || usage.evidenceBytes() != 0
                || usage.laneCount() != 0 || usage.strongLaneCount() != 0;
    }

    private static byte[] fixedIncarnation(final byte[] value) {
        Bytes.requireLength(value, INCARNATION_LENGTH, "laneIncarnation");
        return Bytes.copy(value);
    }

    private static void requireRevision(final long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("usageRevision must be positive");
        }
    }

    private static void requireCount(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static void requireBytes(final long value) {
        if (value < 0) {
            throw new IllegalArgumentException("quota bytes must be non-negative");
        }
    }
}
