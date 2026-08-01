package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;

import java.util.Objects;

/** Typed result returned by a guarded Pulsar transport. */
public record PulsarSendResult(
        Disposition disposition,
        String authenticatedClusterId,
        byte[] resourceIncarnation,
        String physicalTopic,
        int partition,
        long ledgerId,
        long entryId,
        int batchIndex,
        int batchSize,
        boolean batched,
        long brokerEntryTimestampEpochMs,
        int stableCode,
        byte[] evidence) {
    public PulsarSendResult {
        Objects.requireNonNull(disposition, "disposition");
        if (disposition == Disposition.PERSISTED) {
            Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId");
            Objects.requireNonNull(resourceIncarnation, "resourceIncarnation");
            Objects.requireNonNull(physicalTopic, "physicalTopic");
            Bytes.requireLength(resourceIncarnation, 32, "resourceIncarnation");
            if (authenticatedClusterId.isBlank() || physicalTopic.isBlank() || partition < 0 || ledgerId < 0
                    || entryId < 0 || batchIndex < 0 || batchSize <= 0 || batchIndex >= batchSize
                    || (!batched && (batchIndex != 0 || batchSize != 1)) || brokerEntryTimestampEpochMs < 0) {
                throw new IllegalArgumentException("invalid persisted Pulsar result");
            }
        }
        if (disposition != Disposition.PERSISTED && stableCode <= 0) {
            throw new IllegalArgumentException("non-persisted Pulsar result requires a stable code");
        }
        resourceIncarnation = resourceIncarnation == null ? null : Bytes.copy(resourceIncarnation);
        evidence = evidence == null ? null : Bytes.copy(evidence);
    }

    public static PulsarSendResult persisted(final String clusterId, final byte[] resourceIncarnation,
                                             final String physicalTopic, final int partition, final long ledgerId,
                                             final long entryId, final int batchIndex, final int batchSize,
                                             final boolean batched, final long brokerEntryTimestampEpochMs,
                                             final byte[] evidence) {
        return new PulsarSendResult(Disposition.PERSISTED, clusterId, resourceIncarnation, physicalTopic, partition,
                ledgerId, entryId, batchIndex, batchSize, batched, brokerEntryTimestampEpochMs, 0, evidence);
    }

    public static PulsarSendResult definitelyNotPersisted(final int stableCode, final byte[] evidence) {
        return new PulsarSendResult(Disposition.DEFINITIVELY_NOT_PERSISTED, null, null, null, -1, -1, -1, -1,
                -1, false, -1, stableCode, evidence);
    }

    public static PulsarSendResult unknown(final int stableCode, final byte[] evidence) {
        return new PulsarSendResult(Disposition.UNKNOWN, null, null, null, -1, -1, -1, -1, -1, false, -1,
                stableCode, evidence);
    }

    @Override
    public byte[] resourceIncarnation() {
        return resourceIncarnation == null ? null : Bytes.copy(resourceIncarnation);
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
