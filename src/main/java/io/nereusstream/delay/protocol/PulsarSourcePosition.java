package io.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Pulsar source position including resource incarnation and batch identity. */
public record PulsarSourcePosition(
        ShardId shardId,
        byte[] brokerResourceIncarnation,
        String physicalTopic,
        long ledgerId,
        long entryId,
        int normalizedBatchIndex,
        int batchSize,
        EntryKind entryKind,
        long brokerEntryTimestampEpochMs) implements SourcePosition {
    public PulsarSourcePosition {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        Objects.requireNonNull(entryKind, "entryKind");
        Bytes.requireLength(brokerResourceIncarnation, 32, "brokerResourceIncarnation");
        if (physicalTopic.isBlank() || ledgerId < 0 || entryId < 0 || normalizedBatchIndex < 0
                || batchSize <= 0 || normalizedBatchIndex >= batchSize || brokerEntryTimestampEpochMs < 0) {
            throw new IllegalArgumentException("invalid Pulsar source position");
        }
        if (entryKind == EntryKind.NON_BATCH && (normalizedBatchIndex != 0 || batchSize != 1)) {
            throw new IllegalArgumentException("non-batch position must have index 0 and size 1");
        }
        brokerResourceIncarnation = Bytes.copy(brokerResourceIncarnation);
    }

    @Override
    public SourcePositionKind kind() {
        return SourcePositionKind.PULSAR;
    }

    @Override
    public long brokerPersistenceTimeEpochMs() {
        return brokerEntryTimestampEpochMs;
    }

    @Override
    public byte[] brokerResourceIncarnation() {
        return Bytes.copy(brokerResourceIncarnation);
    }

    @Override
    public byte[] canonicalBytes() {
        final byte[] topic = physicalTopic.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer result = ByteBuffer.allocate(1 + 16 + 4 + brokerResourceIncarnation.length
                + 4 + topic.length + 4 + 8 + 8 + 4 + 4 + 1 + 8);
        result.put((byte) kind().wireValue()).put(shardId.routeIncarnation().bytes());
        result.putInt(brokerResourceIncarnation.length).put(brokerResourceIncarnation);
        result.putInt(topic.length).put(topic).putInt(shardId.partition());
        result.putLong(ledgerId).putLong(entryId).putInt(normalizedBatchIndex).putInt(batchSize);
        result.put((byte) entryKind.wireValue()).putLong(brokerEntryTimestampEpochMs);
        return result.array();
    }

    @Override
    public byte[] sourceOrderToken() {
        return ByteBuffer.allocate(1 + 8 + 8 + 4).put((byte) 2).putLong(ledgerId).putLong(entryId)
                .putInt(normalizedBatchIndex).array();
    }

    @Override
    public boolean sameSourceIdentity(final SourcePosition other) {
        if (!(other instanceof PulsarSourcePosition that)) {
            return false;
        }
        return Arrays.equals(brokerResourceIncarnation, that.brokerResourceIncarnation)
                && physicalTopic.equals(that.physicalTopic);
    }

    @Override
    public int compareWithinShard(final SourcePosition other) {
        final PulsarSourcePosition that = (PulsarSourcePosition) other;
        int result = Long.compare(ledgerId, that.ledgerId);
        if (result == 0) {
            result = Long.compare(entryId, that.entryId);
        }
        if (result == 0) {
            result = Integer.compare(normalizedBatchIndex, that.normalizedBatchIndex);
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PulsarSourcePosition that)) {
            return false;
        }
        return shardId.equals(that.shardId) && Arrays.equals(brokerResourceIncarnation, that.brokerResourceIncarnation)
                && physicalTopic.equals(that.physicalTopic) && ledgerId == that.ledgerId && entryId == that.entryId
                && normalizedBatchIndex == that.normalizedBatchIndex && batchSize == that.batchSize
                && entryKind == that.entryKind && brokerEntryTimestampEpochMs == that.brokerEntryTimestampEpochMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, Arrays.hashCode(brokerResourceIncarnation), physicalTopic, ledgerId, entryId,
                normalizedBatchIndex, batchSize, entryKind, brokerEntryTimestampEpochMs);
    }

    public enum EntryKind {
        NON_BATCH(1),
        BATCH(2);

        private final int wireValue;

        EntryKind(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }
    }
}
