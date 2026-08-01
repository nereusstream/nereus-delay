package io.nereusstream.delay.protocol;

/**
 * Broker-specific catch-up boundary. It is deliberately separate from a
 * regular Source Position: Kafka uses an exclusive LSO while Pulsar uses an
 * inclusive last batch member.
 */
public sealed interface SourceActivationBarrier
        permits KafkaActivationBarrier, PulsarActivationBarrier {
    ShardId shardId();

    SourcePositionKind kind();

    /**
     * Returns whether the supplied last durably applied position has reached
     * this barrier. A null position is valid only for an explicitly empty
     * barrier.
     */
    boolean reachedBy(SourcePosition lastAppliedPosition);
}
