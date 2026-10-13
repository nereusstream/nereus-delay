package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.OwnedSourceCursor;
import com.nereusstream.delay.ownership.SourceAssignment;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.ownership.SourceReplaySuccessor;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

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
public final class PulsarClientArtifactRecoverySourceCursor implements Iterator<SourceReplayEntry>,
        com.nereusstream.delay.ownership.TargetWorkerRecoverySource {
    private final GuardedConsumer<byte[]> consumer;
    private final TopicResourceGuard expectedGuard;
    private final SourceAssignment assignment;
    private final PulsarActivationBarrier barrier;
    private final String physicalTopic;
    private final int receiveTimeoutMs;
    private final boolean target;
    private final int maximumFrameBytes;
    private final Runnable protection;
    private final OwnedSourceCursor<Consumer<byte[]>> cleanup;
    private final PulsarClientArtifactOwnedBackendGroup backend;
    private LocalBrokerMtlsResolver.SourceReceipt verifiedCredential;
    private Message<byte[]> buffered;
    private SourceReplayEntry current;
    private PulsarSourcePosition successorAnchor;
    private PulsarSourcePosition successorPrevious;
    private PulsarSourcePosition successorCurrent;
    private boolean hasDelivered;
    private PulsarSourcePosition lastDelivered;
    private boolean closed;
    private boolean failed;

    public PulsarClientArtifactRecoverySourceCursor(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final SourceAssignment assignment,
            final String physicalTopic,
            final Duration receiveTimeout) {
        this(consumer, expectedGuard, assignment, physicalTopic, receiveTimeout, false, Integer.MAX_VALUE, () -> {});
    }

    /** Explicit Target decoder and finite frame boundary; caller owns the guarded no-ACK consumer and all pins. */
    public static PulsarClientArtifactRecoverySourceCursor forTarget(GuardedConsumer<byte[]> consumer,
            TopicResourceGuard expectedGuard, SourceAssignment assignment, String physicalTopic, Duration receiveTimeout,
            int maximumFrameBytes, Runnable protection) {
        if (maximumFrameBytes <= 0) { throw new IllegalArgumentException("Target recovery frame bound must be positive"); }
        return new PulsarClientArtifactRecoverySourceCursor(consumer, expectedGuard, assignment, physicalTopic,
                receiveTimeout, true, maximumFrameBytes, Objects.requireNonNull(protection, "protection"));
    }

    /**
     * Opens a private no-ACK consumer from the group's actual verified material. The callback must accept the
     * post-seek assignment through the existing assignment authority; the protection must recheck that accepted
     * assignment, Owner and retained Source pins. Neither the credential nor the native proof grants assignment authority.
     */
    public static PulsarClientArtifactRecoverySourceCursor openVerified(PulsarClientArtifactOwnedBackendGroup group,
            TopicResourceGuard resource, LocalBrokerMtlsResolver.SourceReceipt credential,
            Optional<PulsarSourcePosition> lastApplied, Duration receiveTimeout, int maximumFrameBytes,
            Supplier<TrustedUtcIntervalEvidence> time, Runnable originalProtection,
            Function<PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof, SourceAssignment> acceptPostSeekAssignment) {
        final long timeoutMs = Objects.requireNonNull(receiveTimeout, "receiveTimeout").toMillis();
        if (timeoutMs < 1 || timeoutMs > 60_000 || maximumFrameBytes < 1 || maximumFrameBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Source recovery settings must be finite");
        }
        Objects.requireNonNull(acceptPostSeekAssignment, "original post-seek assignment acceptance");
        Objects.requireNonNull(lastApplied, "lastApplied");
        group.requireSourceReadResource(credential, resource);
        final var authorization = credential.authorization();
        final String topic = authorization.physicalTopic();
        lastApplied.ifPresent(position -> authorization.requirePosition(position, topic));
        final String name = authorization.newReaderName(SourceTransportCredentialAuthorization.Operation.RECOVERY);
        final var protection = credential.guardRecovery(authorization.shard(), topic, name, time, originalProtection);
        protection.run();
        final var client = group.nativeClientForFactory();
        final var builder = client.newConsumer(org.apache.pulsar.client.api.Schema.BYTES).topic(topic)
                .subscriptionName(name).subscriptionType(org.apache.pulsar.client.api.SubscriptionType.Exclusive)
                .subscriptionMode(org.apache.pulsar.client.api.SubscriptionMode.NonDurable)
                .subscriptionInitialPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .receiverQueueSize(1).autoUpdatePartitions(false).resourceGuard(resource);
        final var opening = new CompletableFuture<Consumer<byte[]>>();
        final var owned = new OwnedSourceCursor<>(opening, PulsarClientArtifactOwnedBackendGroup::closeConsumer, group);
        boolean started = false;
        try {
            group.registerCleanup(owned, owned::operationExited, owned::cleanupComplete, proof -> {
                proof.requireClient(client); owned.requestClose(); owned.backendShutdownCompleted(group);
            });
            protection.run();
            started = true;
            try {
                PulsarClientArtifactOwnedBackendGroup.subscribe(client, builder).whenComplete((value, failure) -> {
                    if (failure == null) { opening.complete(value); }
                    else { opening.completeExceptionally(failure); }
                });
            } catch (RuntimeException | Error failure) { opening.completeExceptionally(failure); throw failure; }
            return owned.read(timeoutMs, nativeConsumer -> {
                if (!(nativeConsumer instanceof GuardedConsumer<?>)) {
                    throw new IllegalStateException("P1 recovery requires the guarded Source proof API");
                }
                @SuppressWarnings("unchecked") final var consumer = (GuardedConsumer<byte[]>) nativeConsumer;
                protection.run();
                final var positioned = PulsarClientArtifactRecoverySourcePositioner.seekAfter(consumer, resource,
                        topic, authorization.shard(), lastApplied, receiveTimeout);
                protection.run();
                final var assignment = Objects.requireNonNull(acceptPostSeekAssignment.apply(positioned), "accepted assignment");
                authorization.requireAssignment(assignment);
                protection.run();
                final var cursor = new PulsarClientArtifactRecoverySourceCursor(consumer, resource, assignment, topic,
                        receiveTimeout, true, maximumFrameBytes, protection, owned, group);
                cursor.verifiedCredential = credential;
                return cursor;
            });
        } catch (Exception | Error failure) {
            if (failure instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            if (!started) { owned.abandonBeforeOpen(); }
            owned.requestClose();
            group.cleanupProgress();
            if (!owned.cleanupComplete()) { throw new CleanupFailure(owned, group, failure); }
            if (failure instanceof Error fatal) { throw fatal; }
            throw new IllegalStateException("P1 verified Source recovery opening failed", failure);
        }
    }

    private PulsarClientArtifactRecoverySourceCursor(GuardedConsumer<byte[]> consumer, TopicResourceGuard expectedGuard,
            SourceAssignment assignment, String physicalTopic, Duration receiveTimeout, boolean target,
            int maximumFrameBytes, Runnable protection) {
        this(consumer, expectedGuard, assignment, physicalTopic, receiveTimeout, target, maximumFrameBytes,
                protection, null, null);
    }

    private PulsarClientArtifactRecoverySourceCursor(GuardedConsumer<byte[]> consumer, TopicResourceGuard expectedGuard,
            SourceAssignment assignment, String physicalTopic, Duration receiveTimeout, boolean target,
            int maximumFrameBytes, Runnable protection, OwnedSourceCursor<Consumer<byte[]>> originalCleanup,
            PulsarClientArtifactOwnedBackendGroup originalBackend) {
        this.target = target;
        this.maximumFrameBytes = maximumFrameBytes;
        this.protection = protection;
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        backend = originalCleanup == null ? PulsarClientArtifactOwnedBackendGroup.findNative(consumer) : originalBackend;
        cleanup = originalCleanup == null ? new OwnedSourceCursor<>(CompletableFuture.completedFuture(consumer),
                PulsarClientArtifactOwnedBackendGroup::closeConsumer, backend) : originalCleanup;
        if (originalCleanup == null && backend != null) { backend.registerCleanup(cleanup, cleanup::operationExited, cleanup::cleanupComplete, proof -> {
            proof.requireClient(backend.clientForProof()); cleanup.requestClose(); cleanup.backendShutdownCompleted(backend);
        }); }
        this.expectedGuard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier pulsarBarrier)) {
            throw new IllegalArgumentException("Pulsar recovery cursor requires a Pulsar activation barrier");
        }
        this.barrier = pulsarBarrier;
        this.physicalTopic = requirePhysicalTopic(physicalTopic);
        if (assignment.shardId().partition() < 0
                || !physicalTopic.equals(barrier.physicalTopic())
                || !Arrays.equals(barrier.brokerResourceIncarnation(), expectedGuard.resourceIncarnation())) {
            throw new IllegalArgumentException("Pulsar recovery cursor resource identity mismatch");
        }
        Objects.requireNonNull(receiveTimeout, "receiveTimeout");
        final long timeoutMs = receiveTimeout.toMillis();
        if (timeoutMs <= 0 || timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("receiveTimeout must fit a positive millisecond int");
        }
        this.receiveTimeoutMs = (int) timeoutMs;
        protection.run();
        requireProof();
        protection.run();
    }

    /** Keeps the same open/close operation and backend obligation after a timeout or partial SDK failure. */
    public static final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient OwnedSourceCursor<?> cleanup;
        private final transient PulsarClientArtifactOwnedBackendGroup backend;
        private CleanupFailure(OwnedSourceCursor<?> cleanup, PulsarClientArtifactOwnedBackendGroup backend, Throwable failure) {
            super("P1 recovery Source cleanup is not confirmed", failure);
            this.cleanup = cleanup; this.backend = backend;
        }
        @Override public boolean cleanupComplete() { return cleanup.cleanupComplete(); }
        @Override public CompletableFuture<Void> retryCleanup() { return cleanup.retryClose(); }
        public Throwable cleanupFailure() { return cleanup.cleanupFailure(); }
        public CompletableFuture<Void> requestHostQuarantine() { return backend.requestHostQuarantine(); }
    }

    /**
     * Binds an exact successor proof to this cursor's native delivery sequence after a seek.
     *
     * <p>The caller must have positioned this consumer with
     * {@link PulsarClientArtifactRecoverySourcePositioner#seekAfter} at {@code resumeAfter}.
     * The proof accepts only the pair most recently delivered by this same guarded,
     * single-topic consumer. Ledger IDs are ordered positions, not arithmetic sequence
     * numbers, so ledger rollover is proven by the consumer's ordered delivery rather than
     * by guessing that the next ledger ID differs by one. The proof does not establish
     * retention of source records deleted before this recovery subscription was opened.</p>
     */
    public synchronized SourceReplaySuccessor successorAfter(final PulsarSourcePosition resumeAfter) {
        ensureOpen();
        final PulsarSourcePosition anchor = Objects.requireNonNull(resumeAfter, "resumeAfter");
        if (successorAnchor != null
                || successorCurrent != null
                || hasDelivered
                || buffered != null
                || current != null) {
            throw new IllegalStateException("Pulsar successor proof must be bound before cursor delivery");
        }
        if (!assignment.shardId().equals(anchor.shardId())
                || !physicalTopic.equals(anchor.physicalTopic())
                || !Arrays.equals(expectedGuard.resourceIncarnation(), anchor.brokerResourceIncarnation())
                || anchor.ledgerId() < 0
                || anchor.entryId() < 0) {
            throw new IllegalArgumentException("Pulsar resume position does not match this recovery cursor");
        }
        barrier.validatePosition(anchor);
        requireProof();
        successorAnchor = anchor;
        successorPrevious = anchor;
        return this::isLastDeliveredSuccessor;
    }

    @Override public synchronized SourceReplaySuccessor successorAfter(SourcePosition resumeAfter) {
        if (!(resumeAfter instanceof PulsarSourcePosition position)) throw new IllegalArgumentException("P1 replay anchor changes Source kind");
        return successorAfter(position);
    }
    @Override public synchronized void requireStartup(SourceAssignment expected, long frameBytes,
            com.nereusstream.delay.scheduler.WorkClassExecutionRegistry classes,
            com.nereusstream.delay.store.SharedRocksDbResources resources) {
        if (!target || verifiedCredential == null || backend == null || !assignment.sameIdentity(expected)
                || maximumFrameBytes > frameBytes) throw new IllegalStateException("recovery cursor lacks its original verified Source startup");
        backend.requireWorkerGraph(classes, resources);
        verifiedCredential.authorization().requireAssignment(expected);
    }
    @Override public com.nereusstream.delay.ownership.TargetOwnedBackendGroup backend() { return backend; }
    @Override public synchronized boolean cleanupComplete() { return closed && cleanup.cleanupComplete(); }

    /** Keeps the exact post-seek native guard live across Target recovery look-ahead and queued writes. */
    public com.nereusstream.delay.ownership.SourceReplayCursor<SourceReplayEntry> replayCursor() {
        return com.nereusstream.delay.ownership.SourceReplayCursor.guarded(this, this::requireCurrent);
    }

    @Override
    public synchronized boolean hasNext() {
        ensureOpen();
        try {
            return cleanup.read(receiveTimeoutMs, ignored -> load());
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            if (target) { failed = true; }
            throw new IllegalStateException("Pulsar guarded recovery receive interrupted", failure);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
            if (target) { failed = true; }
            throw new IllegalStateException("Pulsar guarded recovery opening is unconfirmed", failure);
        } catch (RuntimeException | Error failure) {
            if (target) { failed = true; }
            throw failure;
        } finally {
            if (backend != null) { backend.cleanupProgress(); }
        }
    }

    private boolean load() {
        protection.run();
        if (current != null) {
            requireProof();
            return true;
        }
        final SourceProof before = requireProof();
        if (target && (barrier.empty() || lastDelivered != null && barrier.reachedBy(lastDelivered))) { return false; }
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
            requireProof();
            protection.run();
            if (target && !barrier.empty()) {
                throw new IllegalStateException("Target recovery Source was unavailable before its receive deadline");
            }
            return false;
        }
        try {
            final SourceProof after = requireProof();
            if (!before.equals(after)) {
                throw new IllegalStateException("Pulsar recovery proof changed during receive");
            }
            if (target && (message.getData() == null || message.getData().length > maximumFrameBytes)) {
                throw new IllegalStateException("Target recovery Source frame exceeds its declared byte bound");
            }
            current = target
                    ? PulsarClientArtifactSourceRecordConsumer.decodeTargetReplayRecord(message, assignment.shardId(),
                            physicalTopic, after.attestation(), after.generation(), after.digest())
                    : PulsarClientArtifactSourceRecordConsumer.decodeReplayRecord(message, assignment.shardId(),
                            physicalTopic, after.attestation(), after.generation(), after.digest());
            buffered = null;
            protection.run();
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
        lastDelivered = (PulsarSourcePosition) result.position();
        if (successorAnchor != null) {
            if (!(result.position() instanceof PulsarSourcePosition position)) {
                throw new IllegalStateException("Pulsar recovery cursor produced a foreign position kind");
            }
            successorPrevious = successorCurrent == null ? successorAnchor : successorCurrent;
            successorCurrent = position;
        }
        hasDelivered = true;
        current = null;
        return result;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            failed = true;
            try {
                if (target) {
                    cleanup.retryClose().get(receiveTimeoutMs, TimeUnit.MILLISECONDS);
                } else {
                    consumer.close();
                }
                closed = true;
            } catch (PulsarClientException failure) {
                throw new IllegalStateException("Pulsar guarded recovery close failed", failure);
            } catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Pulsar guarded recovery close interrupted", failure);
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
                throw new IllegalStateException("Pulsar guarded recovery close is unconfirmed; retain exact cursor", failure);
            }
        }
    }

    /** Rechecks the exact connection/attestation and caller-held Source protection without receive or ACK. */
    public synchronized void requireCurrent() {
        ensureOpen();
        try {
            protection.run();
            requireProof();
            protection.run();
        } catch (RuntimeException | Error failure) {
            if (target) { failed = true; }
            throw failure;
        }
    }

    private SourceProof requireProof() {
        if (!consumer.isConnected()) {
            throw new IllegalStateException("Pulsar guarded recovery consumer is not connected");
        }
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

    private synchronized boolean isLastDeliveredSuccessor(
            final SourcePosition previous, final SourcePosition next) {
        if (successorAnchor == null || successorCurrent == null) {
            return false;
        }
        requireProof();
        return sameCanonicalPosition(successorPrevious, previous)
                && sameCanonicalPosition(successorCurrent, next);
    }

    private static boolean sameCanonicalPosition(final SourcePosition left, final SourcePosition right) {
        return left != null
                && right != null
                && Bytes.constantTimeEquals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static String requirePhysicalTopic(final String physicalTopic) {
        final String topic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        if (topic.isBlank()) {
            throw new IllegalArgumentException("physicalTopic must be non-blank");
        }
        return topic;
    }

    private void ensureOpen() {
        if (closed || failed) {
            throw new IllegalStateException("Pulsar recovery source is closed or failed; retain it only for cleanup");
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
            return other instanceof SourceProof that
                    && generation == that.generation
                    && attestation.equals(that.attestation)
                    && Arrays.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generation, attestation, Arrays.hashCode(digest));
        }
    }
}
