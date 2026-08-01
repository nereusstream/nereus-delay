package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;

/** Closed target side-effect result; UNKNOWN is never treated as not-published. */
public record DestinationPublishResult(
        Disposition disposition,
        StableCode stableCode,
        byte[] externalDeliveryIdentity,
        long brokerPersistenceTimeEpochMs,
        byte[] evidence) {
    public DestinationPublishResult {
        Objects.requireNonNull(disposition, "disposition");
        Objects.requireNonNull(stableCode, "stableCode");
        if (disposition == Disposition.PUBLISHED && brokerPersistenceTimeEpochMs < 0) {
            throw new IllegalArgumentException("published result requires a broker persistence time");
        }
        if (disposition != Disposition.PUBLISHED && brokerPersistenceTimeEpochMs >= 0) {
            throw new IllegalArgumentException("non-published result cannot carry a persistence time");
        }
        if (externalDeliveryIdentity != null && externalDeliveryIdentity.length == 0) {
            throw new IllegalArgumentException("external delivery identity must be non-empty");
        }
        externalDeliveryIdentity = externalDeliveryIdentity == null ? null : Bytes.copy(externalDeliveryIdentity);
        evidence = evidence == null ? null : Bytes.copy(evidence);
    }

    public static DestinationPublishResult published(final byte[] externalDeliveryIdentity,
                                                     final long brokerPersistenceTimeEpochMs,
                                                     final byte[] evidence) {
        return new DestinationPublishResult(Disposition.PUBLISHED, StableCode.OK, externalDeliveryIdentity,
                brokerPersistenceTimeEpochMs, evidence);
    }

    public static DestinationPublishResult definitelyNotPublished(final StableCode code, final byte[] evidence) {
        return new DestinationPublishResult(Disposition.DEFINITIVELY_NOT_PUBLISHED, code, null, -1, evidence);
    }

    public static DestinationPublishResult unknown(final StableCode code, final byte[] evidence) {
        return new DestinationPublishResult(Disposition.UNKNOWN, code, null, -1, evidence);
    }

    @Override
    public byte[] externalDeliveryIdentity() {
        return externalDeliveryIdentity == null ? null : Bytes.copy(externalDeliveryIdentity);
    }

    @Override
    public byte[] evidence() {
        return evidence == null ? null : Bytes.copy(evidence);
    }

    public enum Disposition {
        PUBLISHED,
        DEFINITIVELY_NOT_PUBLISHED,
        UNKNOWN
    }
}
