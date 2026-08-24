package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;

/**
 * Positions a guarded Pulsar recovery subscription before its activation
 * barrier is captured.
 *
 * <p>Pulsar seek can close and recreate the guarded SUBSCRIBE connection. The
 * returned proof must therefore be used to build a fresh activation barrier;
 * a barrier captured before this operation is intentionally rejected by the
 * recovery cursor.</p>
 */
public final class PulsarClientArtifactRecoverySourcePositioner {
    private PulsarClientArtifactRecoverySourcePositioner() {}

    /** Seeks after the durable last-applied position, or to the earliest record when absent. */
    public static PositionedGuardProof seekAfter(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final ShardId expectedShard,
            final Optional<PulsarSourcePosition> lastApplied,
            final Duration proofTimeout) {
        final GuardedConsumer<byte[]> acceptedConsumer = Objects.requireNonNull(consumer, "consumer");
        final TopicResourceGuard guard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        final String topic = requirePhysicalTopic(physicalTopic);
        final ShardId shard = requireShard(expectedShard);
        final Optional<PulsarSourcePosition> cursor = Objects.requireNonNull(lastApplied, "lastApplied");
        requireCurrentProof(acceptedConsumer, guard, topic, shard.partition());
        final MessageId target = cursor.map(position -> toMessageId(position, guard, topic, shard))
                .orElse(MessageId.earliest);
        try {
            acceptedConsumer.seek(target);
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Pulsar guarded recovery seek failed", failure);
        }
        return awaitStableProof(acceptedConsumer, guard, topic, shard.partition(), proofTimeout);
    }

    /** Waits for two consecutive reads of the same post-seek guard proof. */
    public static PositionedGuardProof awaitStableProof(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final int partition,
            final Duration proofTimeout) {
        final GuardedConsumer<byte[]> acceptedConsumer = Objects.requireNonNull(consumer, "consumer");
        final TopicResourceGuard guard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        final String topic = requirePhysicalTopic(physicalTopic);
        if (partition < 0) {
            throw new IllegalArgumentException("Pulsar recovery partition must be non-negative");
        }
        final Duration timeout = Objects.requireNonNull(proofTimeout, "proofTimeout");
        final long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("proofTimeout is too large", overflow);
        }
        if (timeoutNanos <= 0) {
            throw new IllegalArgumentException("proofTimeout must be positive");
        }
        final long started = System.nanoTime();
        final long deadline = started > Long.MAX_VALUE - timeoutNanos ? Long.MAX_VALUE : started + timeoutNanos;
        PositionedGuardProof previous = null;
        RuntimeException lastFailure = null;
        while (true) {
            try {
                final PositionedGuardProof current = requireCurrentProof(acceptedConsumer, guard, topic, partition);
                if (previous != null && previous.equals(current)) {
                    return current;
                }
                previous = current;
                lastFailure = null;
            } catch (RuntimeException failure) {
                previous = null;
                lastFailure = failure;
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("Pulsar guarded recovery proof did not stabilize", lastFailure);
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while awaiting Pulsar recovery proof", interrupted);
            }
        }
    }

    private static PositionedGuardProof requireCurrentProof(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final int partition) {
        if (!consumer.isConnected()) {
            throw new IllegalStateException("Pulsar recovery consumer is not connected");
        }
        if (!expectedGuard.equals(consumer.resourceGuard()) || !physicalTopic.equals(consumer.getTopic())) {
            throw new IllegalStateException("Pulsar recovery consumer identity changed");
        }
        final long generation = consumer.connectionGeneration();
        if (generation == 0) {
            throw new IllegalStateException("Pulsar recovery consumer has no connection generation");
        }
        final TopicResourceGuardAttestation attestation = consumer.resourceGuardAttestation()
                .orElseThrow(() -> new IllegalStateException("Pulsar recovery consumer has no guard proof"));
        if (!expectedGuard.authenticatedClusterId().equals(attestation.authenticatedClusterId())
                || !Arrays.equals(expectedGuard.resourceIncarnation(), attestation.resourceIncarnation())
                || !physicalTopic.equals(attestation.physicalTopic())
                || attestation.partition() != partition) {
            throw new IllegalStateException("Pulsar recovery consumer returned a foreign guard proof");
        }
        return new PositionedGuardProof(
                generation, attestation, PulsarClientArtifactSourceRecordConsumer.attestationDigest(attestation));
    }

    private static MessageId toMessageId(
            final PulsarSourcePosition position,
            final TopicResourceGuard expectedGuard,
            final String physicalTopic,
            final ShardId expectedShard) {
        final PulsarSourcePosition accepted = Objects.requireNonNull(position, "lastApplied position");
        if (!expectedShard.equals(accepted.shardId())
                || !physicalTopic.equals(accepted.physicalTopic())
                || !Arrays.equals(expectedGuard.resourceIncarnation(), accepted.brokerResourceIncarnation())
                || accepted.ledgerId() < 0
                || accepted.entryId() < 0) {
            throw new IllegalArgumentException("Pulsar recovery position does not match the guarded assignment");
        }
        if (accepted.entryKind() == PulsarSourcePosition.EntryKind.BATCH) {
            // Route partitions are separate physical topics.  Pulsar's native
            // MessageId therefore uses -1 for the partition index even though
            // the Nereus ShardId carries the logical partition number.
            return new BatchMessageIdImpl(
                    accepted.ledgerId(),
                    accepted.entryId(),
                    -1,
                    accepted.normalizedBatchIndex(),
                    accepted.batchSize(),
                    null);
        }
        return new MessageIdImpl(accepted.ledgerId(), accepted.entryId(), -1);
    }

    private static ShardId requireShard(final ShardId shard) {
        final ShardId accepted = Objects.requireNonNull(shard, "expectedShard");
        if (accepted.partition() < 0) {
            throw new IllegalArgumentException("Pulsar recovery partition must be non-negative");
        }
        return accepted;
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    /** Guard proof captured after seek has settled. */
    public record PositionedGuardProof(
            long connectionGeneration, TopicResourceGuardAttestation attestation, byte[] attestationDigest) {
        public PositionedGuardProof {
            if (connectionGeneration == 0) {
                throw new IllegalArgumentException("connectionGeneration must be nonzero");
            }
            Objects.requireNonNull(attestation, "attestation");
            Bytes.requireLength(attestationDigest, 32, "attestationDigest");
            attestationDigest = Bytes.copy(attestationDigest);
        }

        @Override
        public byte[] attestationDigest() {
            return Bytes.copy(attestationDigest);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof PositionedGuardProof that
                    && connectionGeneration == that.connectionGeneration
                    && attestation.equals(that.attestation)
                    && Arrays.equals(attestationDigest, that.attestationDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectionGeneration, attestation, Arrays.hashCode(attestationDigest));
        }
    }
}
