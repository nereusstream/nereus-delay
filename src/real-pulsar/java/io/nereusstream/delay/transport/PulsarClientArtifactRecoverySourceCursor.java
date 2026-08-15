package io.nereusstream.delay.transport;

import io.nereusstream.delay.ownership.SourceAssignment;
import io.nereusstream.delay.ownership.SourceReplayEntry;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Native guarded Pulsar replay input for {@code OwnerRecoveryCoordinator}.
 *
 * <p>The caller positions the guarded subscription at the durable recovery
 * cursor and builds the activation barrier from the post-positioning proof
 * before constructing this class. No ACK is issued here: releasing a decoded
 * message with {@link #next()} only advances this local iterator after the
 * coordinator has proven the Store apply. A changed guarded SUBSCRIBE
 * generation or attestation fails the cursor and therefore requires a fresh
 * assignment/session.</p>
 */
public final class PulsarClientArtifactRecoverySourceCursor
        implements Iterator<SourceReplayEntry>, AutoCloseable {
    private final GuardedConsumer<byte[]> consumer;
    private final TopicResourceGuard expectedGuard;
    private final SourceAssignment assignment;
    private final PulsarActivationBarrier barrier;
    private final String physicalTopic;
    private final int receiveTimeoutMs;
    private Message<byte[]> buffered;
    private SourceReplayEntry current;
    private boolean closed;

    public PulsarClientArtifactRecoverySourceCursor(final GuardedConsumer<byte[]> consumer,
                                                    final TopicResourceGuard expectedGuard,
                                                    final SourceAssignment assignment,
                                                    final String physicalTopic,
                                                    final Duration receiveTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.expectedGuard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier pulsarBarrier)) {
            throw new IllegalArgumentException("Pulsar recovery cursor requires a Pulsar activation barrier");
        }
        this.barrier = pulsarBarrier;
        this.physicalTopic = requirePhysicalTopic(physicalTopic);
        if (assignment.shardId().partition() < 0 || !physicalTopic.equals(barrier.physicalTopic())
                || !Arrays.equals(barrier.brokerResourceIncarnation(), expectedGuard.resourceIncarnation())) {
            throw new IllegalArgumentException("Pulsar recovery cursor resource identity mismatch");
        }
        Objects.requireNonNull(receiveTimeout, "receiveTimeout");
        final long timeoutMs = receiveTimeout.toMillis();
        if (timeoutMs <= 0 || timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("receiveTimeout must fit a positive millisecond int");
        }
        this.receiveTimeoutMs = (int) timeoutMs;
        requireProof();
    }

    @Override
    public synchronized boolean hasNext() {
        ensureOpen();
        if (current != null) {
            return true;
        }
        final SourceProof before = requireProof();
        final Message<byte[]> message;
        if (buffered != null) {
            message = buffered;
        } else {
            try {
                message = consumer.receive(receiveTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (PulsarClientException failure) {
                throw new IllegalStateException("Pulsar guarded recovery receive failed", failure);
            }
        }
        if (message == null) {
            return false;
        }
        try {
            final SourceProof after = requireProof();
            if (!before.equals(after)) {
                throw new IllegalStateException("Pulsar recovery proof changed during receive");
            }
            current = PulsarClientArtifactSourceRecordConsumer.decodeReplayRecord(message, assignment.shardId(),
                    physicalTopic, after.attestation(), after.generation(), after.digest());
            buffered = null;
            return true;
        } catch (RuntimeException | Error failure) {
            buffered = message;
            throw failure;
        }
    }

    @Override
    public synchronized SourceReplayEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Pulsar recovery source is exhausted");
        }
        final SourceReplayEntry result = current;
        current = null;
        return result;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                consumer.close();
            } catch (PulsarClientException failure) {
                throw new IllegalStateException("Pulsar guarded recovery close failed", failure);
            }
        }
    }

    private SourceProof requireProof() {
        if (!expectedGuard.equals(consumer.resourceGuard()) || !physicalTopic.equals(consumer.getTopic())) {
            throw new IllegalStateException("Pulsar guarded recovery consumer identity changed");
        }
        final long generation = consumer.connectionGeneration();
        if (generation == 0 || generation != barrier.guardedSourceConnectionGeneration()) {
            throw new IllegalStateException("Pulsar guarded recovery generation changed");
        }
        final TopicResourceGuardAttestation attestation = consumer.resourceGuardAttestation()
                .orElseThrow(() -> new IllegalStateException("Pulsar guarded recovery has no connection proof"));
        if (!expectedGuard.authenticatedClusterId().equals(attestation.authenticatedClusterId())
                || !Arrays.equals(expectedGuard.resourceIncarnation(), attestation.resourceIncarnation())
                || !physicalTopic.equals(attestation.physicalTopic())
                || attestation.partition() != assignment.shardId().partition()) {
            throw new IllegalStateException("Pulsar guarded recovery returned foreign proof");
        }
        final byte[] digest = PulsarClientArtifactSourceRecordConsumer.attestationDigest(attestation);
        if (!Bytes.constantTimeEquals(barrier.resourceGuardAttestationDigest(), digest)) {
            throw new IllegalStateException("Pulsar guarded recovery proof differs from activation barrier");
        }
        return new SourceProof(generation, attestation, digest);
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Pulsar guarded recovery source is closed");
        }
    }

    private record SourceProof(long generation, TopicResourceGuardAttestation attestation, byte[] digest) {
        private SourceProof {
            digest = Bytes.copy(digest);
        }

        @Override
        public byte[] digest() {
            return Bytes.copy(digest);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof SourceProof that && generation == that.generation
                    && attestation.equals(that.attestation) && Arrays.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generation, attestation, Arrays.hashCode(digest));
        }
    }
}
