package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.transport.TransportResult;

import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;

/** Typed result returned by a guarded Pulsar transport. */
public record PulsarSendResult(
        Disposition disposition,
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        long physicalTopicCreationTimestamp,
        int partition,
        long ledgerId,
        long entryId,
        int batchIndex,
        int batchSize,
        boolean batched,
        long brokerEntryTimestampEpochMs,
        int stableCode,
        byte[] requestEvidenceBytes,
        byte[] responseEvidenceBytes) implements TransportResult {
    public PulsarSendResult {
        Objects.requireNonNull(disposition, "disposition");
        if (disposition == Disposition.PERSISTED) {
            authenticatedClusterId = canonicalText(authenticatedClusterId, "authenticatedClusterId");
            Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
            physicalTopic = canonicalText(physicalTopic, "physicalTopic");
            Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
            if (stableCode != 0 || batchSize == 0
                    || Integer.compareUnsigned(batchIndex, batchSize) >= 0
                    || (!batched && (batchIndex != 0 || batchSize != 1)) || brokerEntryTimestampEpochMs < 0) {
                throw new IllegalArgumentException("invalid persisted Pulsar result");
            }
        } else {
            if (stableCode <= 0 || authenticatedClusterId != null || resourceIncarnation != null
                    || physicalTopic != null || physicalTopicCreationTimestamp != -1 || partition != -1
                    || ledgerId != -1 || entryId != -1 || batchIndex != -1 || batchSize != -1 || batched
                    || brokerEntryTimestampEpochMs != -1) {
                throw new IllegalArgumentException("invalid non-persisted Pulsar result");
            }
        }
        resourceIncarnation = resourceIncarnation == null ? null : Bytes.copy(resourceIncarnation);
        requestEvidenceBytes = requestEvidenceBytes == null ? null : Bytes.copy(requestEvidenceBytes);
        responseEvidenceBytes = responseEvidenceBytes == null ? null : Bytes.copy(responseEvidenceBytes);
    }

    public PulsarSendResult(final Disposition disposition, final String authenticatedClusterId,
                            final byte[] resourceIncarnation, final String physicalTopic,
                            final long physicalTopicCreationTimestamp, final int partition, final long ledgerId,
                            final long entryId, final int batchIndex, final int batchSize, final boolean batched,
                            final long brokerEntryTimestampEpochMs, final int stableCode, final byte[] evidence) {
        this(disposition, authenticatedClusterId, resourceIncarnation, physicalTopic,
                physicalTopicCreationTimestamp, partition, ledgerId, entryId, batchIndex, batchSize, batched,
                brokerEntryTimestampEpochMs, stableCode, evidence, evidence);
    }

    public static PulsarSendResult persisted(final String clusterId, final byte[] resourceIncarnation,
                                             final String physicalTopic, final long physicalTopicCreationTimestamp,
                                             final int partition, final long ledgerId,
                                             final long entryId, final int batchIndex, final int batchSize,
                                             final boolean batched, final long brokerEntryTimestampEpochMs,
                                             final byte[] evidence) {
        return new PulsarSendResult(Disposition.PERSISTED, clusterId, resourceIncarnation, physicalTopic,
                physicalTopicCreationTimestamp, partition, ledgerId, entryId, batchIndex, batchSize, batched,
                brokerEntryTimestampEpochMs, 0, evidence, evidence);
    }

    public static PulsarSendResult definitelyNotPersisted(final int stableCode, final byte[] evidence) {
        return new PulsarSendResult(Disposition.DEFINITIVELY_NOT_PERSISTED, null, null, null, -1, -1, -1, -1,
                -1, -1, false, -1, stableCode, brokerEvidence(stableCode, evidence),
                brokerEvidence(stableCode, evidence));
    }

    public static PulsarSendResult unknown(final int stableCode, final byte[] evidence) {
        return new PulsarSendResult(Disposition.UNKNOWN, null, null, null, -1, -1, -1, -1, -1, -1, false,
                -1, stableCode, null, evidence);
    }

    @Override
    public byte[] resourceIncarnation() {
        return resourceIncarnation == null ? null : Bytes.copy(resourceIncarnation);
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
