package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PulsarSourcePosition;
import io.nereusstream.delay.protocol.SourcePosition;

import java.util.Objects;

/**
 * Adapter-owned proof that a replay record immediately follows the previous
 * record in the physical Shard Log.
 *
 * <p>Exact redelivery of the same canonical position is always accepted.  It
 * is not a successor, but broker replay can deliver it again and the durable
 * shard apply path already fences it by the command/mutation identity.  Any
 * later position must be accepted by this proof; a merely monotonic cursor is
 * only a compatibility seam and does not prove that no source records were
 * skipped.</p>
 */
@FunctionalInterface
public interface SourceReplaySuccessor {
    /** Returns whether {@code current} is the adapter-proven immediate successor of {@code previous}. */
    boolean isSuccessor(SourcePosition previous, SourcePosition current);

    /** Applies the complete same-source, exact-redelivery and successor fence. */
    default void validate(final SourcePosition previous, final SourcePosition current) {
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(current, "current");
        if (!previous.shardId().equals(current.shardId())
                || !previous.sameSourceIdentity(current)
                || previous.kind() != current.kind()) {
            throw new IllegalArgumentException("source replay positions do not share one physical source");
        }
        final int order = current.compareTo(previous);
        if (order < 0) {
            throw new IllegalStateException("source replay position regressed");
        }
        if (order == 0) {
            if (!Bytes.constantTimeEquals(previous.canonicalBytes(), current.canonicalBytes())) {
                throw new IllegalStateException("source replay position has conflicting canonical identity");
            }
            return;
        }
        if (!isSuccessor(previous, current)) {
            throw new IllegalStateException("source replay has a gap before the current position");
        }
    }

    /** Compatibility-only monotonic check; it does not prove source continuity. */
    static SourceReplaySuccessor monotonic() {
        return (previous, current) -> true;
    }

    /** Strict Kafka successor for a non-compacted Command Topic partition. */
    static SourceReplaySuccessor strictKafka() {
        return (previous, current) -> previous instanceof KafkaSourcePosition previousKafka
                && current instanceof KafkaSourcePosition currentKafka
                && previousKafka.offset() != Long.MAX_VALUE
                && currentKafka.offset() == previousKafka.offset() + 1;
    }

    /**
     * Strict Pulsar successor for members within one batch entry.  Transition
     * to a later entry is adapter-specific (entry IDs can have broker-level
     * gaps), so a production adapter must provide its own successor for that
     * boundary rather than guessing entry contiguity.
     */
    static SourceReplaySuccessor strictPulsarBatchMember() {
        return (previous, current) -> previous instanceof PulsarSourcePosition previousPulsar
                && current instanceof PulsarSourcePosition currentPulsar
                && previousPulsar.ledgerId() == currentPulsar.ledgerId()
                && previousPulsar.entryId() == currentPulsar.entryId()
                && previousPulsar.batchSize() == currentPulsar.batchSize()
                && previousPulsar.normalizedBatchIndex() != previousPulsar.batchSize() - 1
                && currentPulsar.normalizedBatchIndex() == previousPulsar.normalizedBatchIndex() + 1;
    }
}
