package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;

import java.util.Objects;
import java.util.UUID;

/** Typed result returned by a pinned Kafka transport. */
public record KafkaProduceResult(
        Disposition disposition,
        String authenticatedClusterId,
        UUID nativeTopicUuid,
        int partition,
        long offset,
        Integer leaderEpoch,
        long brokerLogAppendTimeEpochMs,
        int stableCode,
        byte[] evidence) {
    public KafkaProduceResult {
        Objects.requireNonNull(disposition, "disposition");
        if (disposition == Disposition.PERSISTED) {
            Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
            Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
            if (authenticatedClusterId.isBlank() || partition < 0 || offset < 0
                    || (leaderEpoch != null && leaderEpoch < 0) || brokerLogAppendTimeEpochMs < 0) {
                throw new IllegalArgumentException("invalid persisted Kafka result");
            }
        }
        if (disposition != Disposition.PERSISTED && stableCode <= 0) {
            throw new IllegalArgumentException("non-persisted Kafka result requires a stable code");
        }
        evidence = evidence == null ? null : Bytes.copy(evidence);
    }

    public static KafkaProduceResult persisted(final String clusterId, final UUID topicUuid, final int partition,
                                               final long offset, final Integer leaderEpoch,
                                               final long brokerLogAppendTimeEpochMs, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.PERSISTED, clusterId, topicUuid, partition, offset, leaderEpoch,
                brokerLogAppendTimeEpochMs, 0, evidence);
    }

    public static KafkaProduceResult definitelyNotPersisted(final int stableCode, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.DEFINITIVELY_NOT_PERSISTED, null, null, -1, -1, null, -1,
                stableCode, evidence);
    }

    public static KafkaProduceResult unknown(final int stableCode, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.UNKNOWN, null, null, -1, -1, null, -1, stableCode, evidence);
    }

    @Override
    public byte[] evidence() {
        return evidence == null ? null : Bytes.copy(evidence);
    }

    public enum Disposition {
        PERSISTED,
        DEFINITIVELY_NOT_PERSISTED,
        UNKNOWN
    }
}
