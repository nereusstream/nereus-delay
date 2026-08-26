package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.StableCode;
import java.util.Objects;

/** Closed target side-effect result; UNKNOWN is never treated as not-published. */
public record DestinationPublishResult(
        Disposition disposition,
        StableCode stableCode,
        byte[] externalDeliveryIdentity,
        long brokerPersistenceTimeEpochMs,
        byte[] evidence,
        BrokerResourceIdentity brokerResource,
        int brokerPartition) {
    public DestinationPublishResult {
        Objects.requireNonNull(disposition, "disposition");
        Objects.requireNonNull(stableCode, "stableCode");
        if (disposition == Disposition.PUBLISHED) {
            if (stableCode != StableCode.OK) {
                throw new IllegalArgumentException("published result must use stable code OK");
            }
            if (brokerPersistenceTimeEpochMs < 0) {
                throw new IllegalArgumentException("published result requires a broker persistence time");
            }
            if (externalDeliveryIdentity == null || externalDeliveryIdentity.length == 0) {
                throw new IllegalArgumentException("published result requires a delivery identity");
            }
            if (evidence == null || evidence.length == 0) {
                throw new IllegalArgumentException("published result requires side-effect evidence");
            }
            if (brokerResource == null && brokerPartition != -1) {
                throw new IllegalArgumentException("published Broker resource and partition must be paired");
            }
        } else {
            if (stableCode == StableCode.OK) {
                throw new IllegalArgumentException("non-published result cannot use stable code OK");
            }
            if (brokerPersistenceTimeEpochMs >= 0) {
                throw new IllegalArgumentException("non-published result cannot carry a persistence time");
            }
            if (externalDeliveryIdentity != null) {
                throw new IllegalArgumentException("non-published result cannot carry a delivery identity");
            }
            if (brokerResource != null || brokerPartition != -1) {
                throw new IllegalArgumentException("non-published result cannot carry Broker resource identity");
            }
        }
        externalDeliveryIdentity = externalDeliveryIdentity == null ? null : Bytes.copy(externalDeliveryIdentity);
        evidence = evidence == null ? null : Bytes.copy(evidence);
    }

    public static DestinationPublishResult published(
            final byte[] externalDeliveryIdentity, final long brokerPersistenceTimeEpochMs, final byte[] evidence) {
        return new DestinationPublishResult(
                Disposition.PUBLISHED,
                StableCode.OK,
                externalDeliveryIdentity,
                brokerPersistenceTimeEpochMs,
                evidence,
                null,
                -1);
    }

    public static DestinationPublishResult published(
            final BrokerResourceIdentity brokerResource,
            final int brokerPartition,
            final byte[] externalDeliveryIdentity,
            final long brokerPersistenceTimeEpochMs,
            final byte[] evidence) {
        return new DestinationPublishResult(
                Disposition.PUBLISHED,
                StableCode.OK,
                externalDeliveryIdentity,
                brokerPersistenceTimeEpochMs,
                evidence,
                Objects.requireNonNull(brokerResource, "brokerResource"),
                brokerPartition);
    }

    public static DestinationPublishResult definitelyNotPublished(final StableCode code, final byte[] evidence) {
        return new DestinationPublishResult(Disposition.DEFINITIVELY_NOT_PUBLISHED, code, null, -1, evidence, null, -1);
    }

    public static DestinationPublishResult unknown(final StableCode code, final byte[] evidence) {
        return new DestinationPublishResult(Disposition.UNKNOWN, code, null, -1, evidence, null, -1);
    }

    public BrokerResourceIdentity brokerResource() {
        return brokerResource;
    }

    public int brokerPartition() {
        return brokerPartition;
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
