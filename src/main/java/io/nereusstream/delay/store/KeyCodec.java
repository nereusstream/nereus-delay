package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.SourcePositionCodec;

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
        Objects.requireNonNull(canonicalSourcePosition, "canonicalSourcePosition");
        if (canonicalSourcePosition.length == 0) {
            throw new IllegalArgumentException("canonicalSourcePosition must not be empty");
        }
        // This key is the physical-position audit locator.  Do not allow a
        // caller to manufacture a look-alike key with malformed or
        // non-canonical bytes; every durable POSITION lookup must use the
        // exact registered Source Position encoding.
        final byte[] canonical = SourcePositionCodec.decode(canonicalSourcePosition).canonicalBytes();
        return Bytes.concat(new byte[]{3, 1}, canonical);
    }

    /** Stable dedupe/FENCE locator for one deterministic TIME_FENCE proof. */
    public static byte[] dedupeFence(final byte[] proofId) {
        return typedIdentity((byte) 4, proofId, "proofId");
    }

    public static byte[] dedupeSystemMutation(final byte[] mutationId) {
        Objects.requireNonNull(mutationId, "mutationId");
        if (mutationId.length != 32) {
            throw new IllegalArgumentException("mutationId must be 32 bytes");
        }
        return Bytes.concat(new byte[]{5, 1}, mutationId);
    }

    public static byte[] timelineDue(final DestinationLaneId laneId, final long eligibleAtEpochMs,
                                     final byte[] sourceOrderToken, final DelayMessageId messageId,
                                     final int generation) {
        Objects.requireNonNull(sourceOrderToken, "sourceOrderToken");
        if (eligibleAtEpochMs < 0) {
            throw new IllegalArgumentException("invalid timeline key values");
        }
        validateSourceOrderToken(sourceOrderToken);
        return Bytes.concat(new byte[]{1, 1}, laneId.bytes(), Bytes.u64be(eligibleAtEpochMs), sourceOrderToken,
                messageId.bytes(), Bytes.u32beBits(generation));
    }

    public static byte[] timelineOrdered(final DestinationLaneId laneId, final long deliverAtEpochMs,
                                         final byte[] sourceOrderToken, final DelayMessageId messageId,
                                         final int generation) {
        if (deliverAtEpochMs < 0) {
            throw new IllegalArgumentException("invalid timeline key values");
        }
        validateSourceOrderToken(sourceOrderToken);
        return Bytes.concat(new byte[]{2, 1}, laneId.bytes(), Bytes.u64be(deliverAtEpochMs), sourceOrderToken,
                messageId.bytes(), Bytes.u32beBits(generation));
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
        if (expireAtEpochMs < 0) {
            throw new IllegalArgumentException("invalid expiry key values");
        }
        return Bytes.concat(new byte[]{4, 1}, Bytes.u64be(expireAtEpochMs), laneId.bytes(), messageId.bytes(),
                Bytes.u32beBits(generation));
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
        if (systemWorkKind <= 0 || systemWorkKind > 4 || nextEligibleAtEpochMs < 0
                || workId.length == 0 || workVersion < 0) {
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

    /** Stable id_cf locator for a durable Registry Schedule binding sidecar. */
    public static byte[] idV1ScheduleBinding(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        return Bytes.concat(new byte[]{4, 1}, messageId.bytes());
    }

    public static byte[] inflight(final byte recordKind, final long ownerEpoch, final byte[] attemptId) {
        Objects.requireNonNull(attemptId, "attemptId");
        if (recordKind < 1 || recordKind > 3 || attemptId.length == 0) {
            throw new IllegalArgumentException("invalid inflight key values");
        }
        return Bytes.concat(new byte[]{recordKind, 1}, Bytes.u64beBits(ownerEpoch), Bytes.lp32(attemptId));
    }

    public static byte[] terminalGeneration(final DelayMessageId messageId, final int generation) {
        Objects.requireNonNull(messageId, "messageId");
        return Bytes.concat(new byte[]{1, 1}, messageId.bytes(), Bytes.u32beBits(generation));
    }

    /** Stable terminal_cf/DLQ_EXPORT locator: {@code 02 01 | dlqExportId[32]}. */
    public static byte[] terminalDlqExport(final byte[] dlqExportId) {
        Objects.requireNonNull(dlqExportId, "dlqExportId");
        if (dlqExportId.length != 32 || isZero(dlqExportId)) {
            throw new IllegalArgumentException("dlqExportId must be a non-zero 32-byte identity");
        }
        return Bytes.concat(new byte[]{2, 1}, dlqExportId);
    }

    public static byte[] gcTask(final long notBeforeEpochMs, final byte kind, final byte[] resourceId,
                                final long expectedVersion) {
        Objects.requireNonNull(resourceId, "resourceId");
        if (notBeforeEpochMs < 0 || kind <= 0 || kind > 10 || resourceId.length == 0) {
            throw new IllegalArgumentException("invalid GC key values");
        }
        return Bytes.concat(new byte[]{1, 1}, Bytes.u64be(notBeforeEpochMs), new byte[]{kind},
                Bytes.lp32(resourceId), Bytes.u64beBits(expectedVersion));
    }

    /** Stable gc_cf locator for a resource identity/version retire intent. */
    public static byte[] gcRetireIntent(final ResourceKind resourceKind, final byte[] resourceIdentityHash,
                                        final long expectedVersion) {
        Objects.requireNonNull(resourceKind, "resourceKind");
        Objects.requireNonNull(resourceIdentityHash, "resourceIdentityHash");
        if (resourceIdentityHash.length != 32) {
            throw new IllegalArgumentException("invalid resource retire intent key values");
        }
        return gcTask(0, (byte) resourceKind.wireValue(), resourceIdentityHash, expectedVersion);
    }

    /** Stable gc_cf/PROTECTION locator for one guarded resource generation. */
    public static byte[] gcProtection(final int protectionKind, final byte[] resourceId,
                                      final long protectionGeneration) {
        Objects.requireNonNull(resourceId, "resourceId");
        if (protectionKind <= 0 || protectionKind > 6 || resourceId.length == 0) {
            throw new IllegalArgumentException("invalid GC protection key values");
        }
        return Bytes.concat(new byte[]{2, 1, (byte) protectionKind}, Bytes.lp32(resourceId),
                Bytes.u64beBits(protectionGeneration));
    }

    public static byte[] metaFixed(final int fixedKeyKind) {
        if (fixedKeyKind <= 0 || fixedKeyKind > 14) {
            throw new IllegalArgumentException("unknown FIXED meta key kind");
        }
        return new byte[]{1, 1, (byte) fixedKeyKind};
    }

    public static byte[] metaLane(final DestinationLaneId laneId) {
        return Bytes.concat(new byte[]{2, 1}, laneId.bytes());
    }

    public static byte[] metaQuota(final int quotaClass) {
        if (quotaClass <= 0 || quotaClass > 5) {
            throw new IllegalArgumentException("unknown QUOTA meta key kind");
        }
        return new byte[]{3, 1, (byte) quotaClass};
    }

    public static byte[] metaScheduler(final int schedulerKeyKind) {
        if (schedulerKeyKind <= 0 || schedulerKeyKind > 5) {
            throw new IllegalArgumentException("unknown SCHEDULER meta key kind");
        }
        return Bytes.concat(new byte[]{5, 1, (byte) schedulerKeyKind});
    }

    /** Stable meta/PRODUCER locator for one Lane physical channel slot. */
    public static byte[] metaProducer(final DestinationLaneId laneId, final long physicalPartition,
                                      final long channelSlot) {
        Objects.requireNonNull(laneId, "laneId");
        if (physicalPartition < 0 || physicalPartition > 0xffff_ffffL
                || channelSlot < 0 || channelSlot > 0xffff_ffffL) {
            throw new IllegalArgumentException("invalid producer key values");
        }
        return Bytes.concat(new byte[]{4, 1}, laneId.bytes(), Bytes.u32be(physicalPartition),
                Bytes.u32be(channelSlot));
    }

    /** Stable meta/RECOVERY locator for one registered recovery projection. */
    public static byte[] metaRecovery(final int recoveryKeyKind) {
        if (recoveryKeyKind <= 0 || recoveryKeyKind > 4) {
            throw new IllegalArgumentException("unknown RECOVERY meta key kind");
        }
        return new byte[]{7, 1, (byte) recoveryKeyKind};
    }

    /** Stable meta/SLO_OUTBOX locator: {@code 08 01 | sampleId[32]}. */
    public static byte[] metaSloOutbox(final byte[] sampleId) {
        Objects.requireNonNull(sampleId, "sampleId");
        if (sampleId.length != 32 || isZero(sampleId)) {
            throw new IllegalArgumentException("sampleId must be a non-zero 32-byte identity");
        }
        return Bytes.concat(new byte[]{8, 1}, sampleId);
    }

    /**
     * Exact meta/CONTROL_RESERVE locator: {@code 06 01 | reserveClass |
     * lp32(grantId)}. The grant identity is part of the key so a rotated
     * immutable grant cannot reuse a prior usage projection.
     */
    public static byte[] metaControlReserve(final int reserveClass, final byte[] grantId) {
        Objects.requireNonNull(grantId, "grantId");
        if (reserveClass <= 0 || reserveClass > 6 || grantId.length != 32 || isZero(grantId)) {
            throw new IllegalArgumentException("invalid CONTROL_RESERVE meta key");
        }
        return Bytes.concat(new byte[]{6, 1, (byte) reserveClass}, Bytes.lp32(grantId));
    }

    private static boolean isZero(final byte[] value) {
        for (byte current : value) {
            if (current != 0) {
                return false;
            }
        }
        return true;
    }

    private static byte[] typedIdentity(final byte tag, final byte[] identity, final String name) {
        Objects.requireNonNull(identity, name);
        if (identity.length != 32) {
            throw new IllegalArgumentException(name + " must be 32 bytes");
        }
        return Bytes.concat(new byte[]{tag, 1}, identity);
    }

    private static void validateSourceOrderToken(final byte[] token) {
        Objects.requireNonNull(token, "sourceOrderToken");
        final int expectedLength = token.length == 9 && token[0] == 1 ? 9
                : token.length == 21 && token[0] == 2 ? 21 : -1;
        if (expectedLength < 0) {
            throw new IllegalArgumentException("sourceOrderToken is not a registered Kafka/Pulsar variant");
        }
    }
}
