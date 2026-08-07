package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;

import java.util.Objects;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

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
            authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
            Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
            if (stableCode != 0 || partition < 0 || offset < 0
                    || (leaderEpoch != null && leaderEpoch < 0) || brokerLogAppendTimeEpochMs < 0) {
                throw new IllegalArgumentException("invalid persisted Kafka result");
            }
        } else {
            if (stableCode <= 0 || authenticatedClusterId != null || nativeTopicUuid != null || partition != -1
                    || offset != -1 || leaderEpoch != null || brokerLogAppendTimeEpochMs != -1) {
                throw new IllegalArgumentException("invalid non-persisted Kafka result");
            }
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

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        final String decoded = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
        if (!decoded.equals(value) || value.isBlank() || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }
}
