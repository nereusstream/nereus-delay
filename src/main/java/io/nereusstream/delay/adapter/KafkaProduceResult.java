package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import io.nereusstream.delay.transport.TransportResult;

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
        byte[] requestEvidenceBytes,
        byte[] responseEvidenceBytes,
        PhysicalEnqueueAttemptId physicalAttemptId) implements TransportResult {
    public KafkaProduceResult {
        Objects.requireNonNull(disposition, "disposition");
        if (disposition == Disposition.PERSISTED) {
            authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
            Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
            if (stableCode != 0 || brokerLogAppendTimeEpochMs < 0) {
                throw new IllegalArgumentException("invalid persisted Kafka result");
            }
        } else {
            if (stableCode <= 0 || authenticatedClusterId != null || nativeTopicUuid != null || partition != -1
                    || offset != -1 || leaderEpoch != null || brokerLogAppendTimeEpochMs != -1) {
                throw new IllegalArgumentException("invalid non-persisted Kafka result");
            }
        }
        requestEvidenceBytes = requestEvidenceBytes == null ? null : Bytes.copy(requestEvidenceBytes);
        responseEvidenceBytes = responseEvidenceBytes == null ? null : Bytes.copy(responseEvidenceBytes);
    }

    public KafkaProduceResult(final Disposition disposition, final String authenticatedClusterId,
                              final UUID nativeTopicUuid, final int partition, final long offset,
                              final Integer leaderEpoch, final long brokerLogAppendTimeEpochMs,
                              final int stableCode, final byte[] requestEvidenceBytes,
                              final byte[] responseEvidenceBytes) {
        this(disposition, authenticatedClusterId, nativeTopicUuid, partition, offset, leaderEpoch,
                brokerLogAppendTimeEpochMs, stableCode, requestEvidenceBytes, responseEvidenceBytes, null);
    }

    public KafkaProduceResult(final Disposition disposition, final String authenticatedClusterId,
                              final UUID nativeTopicUuid, final int partition, final long offset,
                              final Integer leaderEpoch, final long brokerLogAppendTimeEpochMs,
                              final int stableCode, final byte[] evidence) {
        this(disposition, authenticatedClusterId, nativeTopicUuid, partition, offset, leaderEpoch,
                brokerLogAppendTimeEpochMs, stableCode, evidence, evidence, null);
    }

    public static KafkaProduceResult persisted(final String clusterId, final UUID topicUuid, final int partition,
                                               final long offset, final Integer leaderEpoch,
                                               final long brokerLogAppendTimeEpochMs, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.PERSISTED, clusterId, topicUuid, partition, offset, leaderEpoch,
                brokerLogAppendTimeEpochMs, 0, evidence, evidence, null);
    }

    public static KafkaProduceResult persisted(final PhysicalEnqueueAttemptId physicalAttemptId,
                                               final String clusterId, final UUID topicUuid, final int partition,
                                               final long offset, final Integer leaderEpoch,
                                               final long brokerLogAppendTimeEpochMs, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.PERSISTED, clusterId, topicUuid, partition, offset, leaderEpoch,
                brokerLogAppendTimeEpochMs, 0, evidence, evidence,
                Objects.requireNonNull(physicalAttemptId, "physicalAttemptId"));
    }

    public static KafkaProduceResult definitelyNotPersisted(final int stableCode, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.DEFINITIVELY_NOT_PERSISTED, null, null, -1, -1, null, -1,
                stableCode, brokerEvidence(stableCode, evidence), brokerEvidence(stableCode, evidence), null);
    }

    public static KafkaProduceResult definitelyNotPersisted(final PhysicalEnqueueAttemptId physicalAttemptId,
                                                            final int stableCode, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.DEFINITIVELY_NOT_PERSISTED, null, null, -1, -1, null, -1,
                stableCode, brokerEvidence(stableCode, evidence), brokerEvidence(stableCode, evidence),
                Objects.requireNonNull(physicalAttemptId, "physicalAttemptId"));
    }

    public static KafkaProduceResult unknown(final int stableCode, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.UNKNOWN, null, null, -1, -1, null, -1, stableCode, null,
                evidence, null);
    }

    public static KafkaProduceResult unknown(final PhysicalEnqueueAttemptId physicalAttemptId,
                                             final int stableCode, final byte[] evidence) {
        return new KafkaProduceResult(Disposition.UNKNOWN, null, null, -1, -1, null, -1, stableCode, null,
                evidence, Objects.requireNonNull(physicalAttemptId, "physicalAttemptId"));
    }

    public KafkaProduceResult bindPhysicalAttemptId(final PhysicalEnqueueAttemptId attemptId) {
        Objects.requireNonNull(attemptId, "attemptId");
        if (physicalAttemptId != null && !physicalAttemptId.equals(attemptId)) {
            throw new IllegalArgumentException("Kafka result physical attempt mismatch");
        }
        return physicalAttemptId == null ? new KafkaProduceResult(disposition, authenticatedClusterId,
                nativeTopicUuid, partition, offset, leaderEpoch, brokerLogAppendTimeEpochMs, stableCode,
                requestEvidenceBytes, responseEvidenceBytes, attemptId) : this;
    }

    @Override
    public byte[] requestEvidenceBytes() {
        return requestEvidenceBytes == null ? null : Bytes.copy(requestEvidenceBytes);
    }

    @Override
    public byte[] responseEvidenceBytes() {
        return responseEvidenceBytes == null ? null : Bytes.copy(responseEvidenceBytes);
    }

    /** Compatibility accessor for older adapter facades; response evidence is authoritative. */
    public byte[] evidence() {
        return responseEvidenceBytes == null ? requestEvidenceBytes() : responseEvidenceBytes();
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

    private static byte[] brokerEvidence(final int stableCode, final byte[] evidence) {
        return stableCode == io.nereusstream.delay.protocol.StableCode.BROKER_DEFINITIVE_NOT_PERSISTED.wireValue()
                || stableCode == io.nereusstream.delay.protocol.StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED.wireValue()
                ? evidence : null;
    }
}
