package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Pulsar inclusive last-message barrier, including the final batch member. */
public record PulsarActivationBarrier(
        ShardId shardId,
        byte[] brokerResourceIncarnation,
        String physicalTopic,
        long ledgerId,
        long entryId,
        int normalizedLastBatchIndex,
        boolean empty) implements SourceActivationBarrier {
    public PulsarActivationBarrier {
        Objects.requireNonNull(shardId, "shardId");
        Bytes.requireLength(brokerResourceIncarnation, 32, "brokerResourceIncarnation");
        Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (physicalTopic.isBlank() || ledgerId < 0 || entryId < 0 || normalizedLastBatchIndex < 0) {
            throw new IllegalArgumentException("invalid Pulsar activation barrier");
        }
        if (empty && (ledgerId != 0 || entryId != 0 || normalizedLastBatchIndex != 0)) {
            throw new IllegalArgumentException("empty Pulsar barrier must use the sentinel cursor");
        }
        brokerResourceIncarnation = Bytes.copy(brokerResourceIncarnation);
    }

    public static PulsarActivationBarrier empty(final ShardId shardId, final byte[] resourceIncarnation,
                                                final String physicalTopic) {
        return new PulsarActivationBarrier(shardId, resourceIncarnation, physicalTopic, 0, 0, 0, true);
    }

    @Override
    public SourcePositionKind kind() {
        return SourcePositionKind.PULSAR;
    }

    @Override
    public byte[] brokerResourceIncarnation() {
        return Bytes.copy(brokerResourceIncarnation);
    }

    @Override
    public boolean reachedBy(final SourcePosition lastAppliedPosition) {
        if (empty) {
            return true;
        }
        if (lastAppliedPosition == null) {
            return false;
        }
        if (!(lastAppliedPosition instanceof PulsarSourcePosition pulsar)
                || !shardId.equals(pulsar.shardId())
                || !Arrays.equals(brokerResourceIncarnation, pulsar.brokerResourceIncarnation())
                || !physicalTopic.equals(pulsar.physicalTopic())) {
            throw new IllegalArgumentException("Pulsar activation barrier source identity mismatch");
        }
        if (pulsar.ledgerId() != ledgerId) {
            return pulsar.ledgerId() > ledgerId;
        }
        if (pulsar.entryId() != entryId) {
            return pulsar.entryId() > entryId;
        }
        return pulsar.normalizedBatchIndex() >= normalizedLastBatchIndex;
    }
}
