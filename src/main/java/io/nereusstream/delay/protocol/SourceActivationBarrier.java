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
     * Returns the guard attestation digest when the barrier is bound to a
     * physical source connection. Kafka barriers have no such field.
     */
    default byte[] resourceGuardAttestationDigest() {
        return null;
    }

    /**
     * Validates the physical source identity before a catch-up record is
     * admitted. This is separate from {@link #reachedBy(SourcePosition)} so an
     * empty barrier cannot accidentally accept a record from a replacement
     * source.
     */
    default void validatePosition(final SourcePosition position) {
        if (position == null || !shardId().equals(position.shardId()) || kind() != position.kind()) {
            throw new IllegalArgumentException("activation barrier source identity mismatch");
        }
    }

    /**
     * Returns whether the supplied last durably applied position has reached
     * this barrier. A null position is valid only for an explicitly empty
     * barrier.
     */
    boolean reachedBy(SourcePosition lastAppliedPosition);
}
