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
import java.util.List;
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
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TopicMessageId;

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
    private final SourceProof independentReaderProof;
    private final PulsarSourcePosition positionedReaderCut;
    private final com.nereusstream.delay.ownership.TargetWorkerSourceStartup sourceStartup;
    private Message<byte[]> buffered;
    private SourceReplayEntry current;
    private PulsarSourcePosition successorAnchor;
    private PulsarSourcePosition successorPrevious;
    private PulsarSourcePosition successorCurrent;
    private boolean hasDelivered;
    private PulsarSourcePosition lastDelivered;
    private SourceReplayEntry lastDeliveredEntry;
    private ReplayTail readThrough;
    private CompletableFuture<?> replayTailIo;
    private boolean replayTailCaptureStarted;
    private boolean replayTailAtPositionedCut;
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
    public static PulsarClientArtifactRecoverySourceCursor openForStartup(
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup, PulsarClientArtifactOwnedBackendGroup group,
            TopicResourceGuard resource, LocalBrokerMtlsResolver.SourceReceipt credential, Duration receiveTimeout,
            int maximumFrameBytes, Supplier<TrustedUtcIntervalEvidence> time, Runnable originalProtection) {
        try (var original = startup.beginRecoveryReaderConstruction()) {
            original.requireCurrent();
            final var graph = startup.graph(); group.requireWorkerGraph(graph.classes(), graph.resources());
            if (maximumFrameBytes > graph.config().maximumSourceFrameBytes()) {
                throw new IllegalArgumentException("recovery reader exceeds its actual Source frame envelope");
            }
            if (!(startup.auditedRecoveryCut() instanceof PulsarSourcePosition position)) {
                throw new IllegalArgumentException("audited recovery Store lacks its P1 Source cut");
            }
            final var cursor = openVerified0(group, resource, credential, Optional.of(position), receiveTimeout,
                    maximumFrameBytes, time, () -> { startup.requireRecoveryReaderCurrent(); originalProtection.run(); },
                    null, startup, position);
            startup.retainRecoverySource(cursor); original.requireCurrent();
            return cursor;
        } catch (CleanupFailure partial) {
            startup.retainRecoveryOpeningBackend(group); startup.retainRecoveryOpeningCleanup(partial);
            startup.retain(partial); throw partial;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    /** Legacy strict assignment-callback path; it cannot provide the new independent Target reader binding. */
    public static PulsarClientArtifactRecoverySourceCursor openForStartup(
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup, PulsarClientArtifactOwnedBackendGroup group,
            TopicResourceGuard resource, LocalBrokerMtlsResolver.SourceReceipt credential, Duration receiveTimeout,
            int maximumFrameBytes, Supplier<TrustedUtcIntervalEvidence> time, Runnable originalProtection,
            Function<PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof, SourceAssignment> acceptPostSeekAssignment) {
        try (var original = startup.beginRecoveryReaderConstruction()) {
            original.requireCurrent();
            final var graph = startup.graph(); group.requireWorkerGraph(graph.classes(), graph.resources());
            if (maximumFrameBytes > graph.config().maximumSourceFrameBytes()) {
                throw new IllegalArgumentException("recovery reader exceeds the original Source frame envelope");
            }
            final var applied = startup.store().appliedShardLogPosition();
            if (!(applied instanceof PulsarSourcePosition position)) {
                throw new IllegalArgumentException("actual recovery Store lacks its P1 Source cut");
            }
            final var cursor = openVerified(group, resource, credential, Optional.of(position), receiveTimeout,
                    maximumFrameBytes, time, () -> { startup.requireCurrent(); originalProtection.run(); }, proof -> {
                        final var accepted = Objects.requireNonNull(acceptPostSeekAssignment.apply(proof));
                        if (!accepted.sameIdentity(startup.acceptedAssignment())) {
                            throw new IllegalStateException("post-seek assignment changes the acquired original Source identity");
                        }
                        return accepted;
                    });
            startup.retainRecoverySource(cursor); original.requireCurrent();
            return cursor;
        } catch (CleanupFailure partial) {
            startup.retainRecoveryOpeningBackend(group); startup.retainRecoveryOpeningCleanup(partial);
            startup.retain(partial); throw partial;
        } catch (RuntimeException | Error failure) { startup.retain(failure); throw failure; }
    }

    public static PulsarClientArtifactRecoverySourceCursor openVerified(PulsarClientArtifactOwnedBackendGroup group,
            TopicResourceGuard resource, LocalBrokerMtlsResolver.SourceReceipt credential,
            Optional<PulsarSourcePosition> lastApplied, Duration receiveTimeout, int maximumFrameBytes,
            Supplier<TrustedUtcIntervalEvidence> time, Runnable originalProtection,
            Function<PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof, SourceAssignment> acceptPostSeekAssignment) {
        return openVerified0(group, resource, credential, lastApplied, receiveTimeout, maximumFrameBytes, time,
                originalProtection, Objects.requireNonNull(acceptPostSeekAssignment), null, null);
    }

    private static PulsarClientArtifactRecoverySourceCursor openVerified0(PulsarClientArtifactOwnedBackendGroup group,
            TopicResourceGuard resource, LocalBrokerMtlsResolver.SourceReceipt credential,
            Optional<PulsarSourcePosition> lastApplied, Duration receiveTimeout, int maximumFrameBytes,
            Supplier<TrustedUtcIntervalEvidence> time, Runnable originalProtection,
            Function<PulsarClientArtifactRecoverySourcePositioner.PositionedGuardProof, SourceAssignment> acceptPostSeekAssignment,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup, PulsarSourcePosition actualCut) {
        final long timeoutMs = Objects.requireNonNull(receiveTimeout, "receiveTimeout").toMillis();
        if (timeoutMs < 1 || timeoutMs > 60_000 || maximumFrameBytes < 1 || maximumFrameBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Source recovery settings must be finite");
        }
        if (startup == null) Objects.requireNonNull(acceptPostSeekAssignment, "original post-seek assignment acceptance");
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
                final var assignment = startup == null
                        ? Objects.requireNonNull(acceptPostSeekAssignment.apply(positioned), "accepted assignment")
                        : startup.acceptedAssignment();
                authorization.requireAssignment(assignment);
                protection.run();
                final var cursor = new PulsarClientArtifactRecoverySourceCursor(consumer, resource, assignment, topic,
                        receiveTimeout, true, maximumFrameBytes, protection, owned, group,
                        startup == null ? null : new SourceProof(positioned.connectionGeneration(),
                                positioned.attestation(), positioned.attestationDigest()), actualCut, startup);
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
        this(consumer, expectedGuard, assignment, physicalTopic, receiveTimeout, target, maximumFrameBytes,
                protection, originalCleanup, originalBackend, null, null, null);
    }

    private PulsarClientArtifactRecoverySourceCursor(GuardedConsumer<byte[]> consumer, TopicResourceGuard expectedGuard,
            SourceAssignment assignment, String physicalTopic, Duration receiveTimeout, boolean target,
            int maximumFrameBytes, Runnable protection, OwnedSourceCursor<Consumer<byte[]>> originalCleanup,
            PulsarClientArtifactOwnedBackendGroup originalBackend, SourceProof readerProof, PulsarSourcePosition actualCut,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup startup) {
        independentReaderProof = readerProof;
        positionedReaderCut = actualCut;
        sourceStartup = startup;
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
    @Override public synchronized void requireReaderBinding(SourceAssignment expected, SourcePosition cut) {
        if (independentReaderProof == null || sourceStartup == null || positionedReaderCut == null
                || !assignment.sameIdentity(expected) || !sameCanonicalPosition(positionedReaderCut, cut)
                || verifiedCredential == null || backend == null) {
            throw new IllegalStateException("reader lacks its original independent Target startup binding");
        }
        sourceStartup.requireRecoveryReaderCurrent();
        verifiedCredential.authorization().requireAssignment(expected);
        protection.run(); requireProof();
    }
    @Override public synchronized void requireReaderEntry(SourceReplayEntry entry, SourceAssignment expected, SourcePosition cut) {
        requireReaderBinding(expected, cut);
        if (entry != lastDeliveredEntry || entry.sourceConnectionGeneration() == null
                || entry.sourceConnectionGeneration() != independentReaderProof.generation()
                || !Bytes.constantTimeEquals(entry.guardAttestationDigest(), independentReaderProof.digest())) {
            throw new IllegalStateException("Target recovery entry is not the original reader's native delivery");
        }
        barrier.validatePosition(entry.position());
    }
    @Override public synchronized boolean cleanupComplete() {
        return closed && cleanup.cleanupComplete() && (replayTailIo == null || replayTailIo.isDone());
    }

    /** Observes one finite tail on the original independent reader; it does not change the signed barrier. */
    @Override public synchronized void captureReplayTail() {
        ensureOpen();
        requireIndependentReader();
        if (replayTailCaptureStarted) {
            if (readThrough != null) { requireCurrent(); return; }
            throw new IllegalStateException("P1 replay tail capture already started; retain its original I/O");
        }
        if (current != null || buffered != null) {
            throw new IllegalStateException("P1 replay tail capture requires no undelivered Source input");
        }
        if (!barrier.reachedBy(lastDelivered == null ? positionedReaderCut : lastDelivered)) {
            throw new IllegalStateException("P1 replay tail capture must follow the original activation barrier");
        }
        replayTailCaptureStarted = true;
        final long started = System.nanoTime();
        final long budgetNanos = TimeUnit.MILLISECONDS.toNanos(receiveTimeoutMs);
        try {
            cleanup.read(receiveTimeoutMs, ignored -> {
                protection.run();
                final SourceProof before = requireProof();
                final var reading = PulsarClientArtifactOwnedBackendGroup.beginNativeRead(consumer);
                if (reading == null) {
                    throw new IllegalStateException("P1 replay tail lacks its original native read ownership");
                }
                final CompletableFuture<List<TopicMessageId>> pending;
                try {
                    pending = Objects.requireNonNull(consumer.getLastMessageIdsAsync(), "P1 replay tail future")
                            .whenComplete((value, failure) -> reading.close());
                    replayTailIo = pending;
                } catch (RuntimeException | Error failure) {
                    reading.close();
                    throw failure;
                }
                final List<TopicMessageId> ids;
                try {
                    final long remaining = budgetNanos - (System.nanoTime() - started);
                    if (remaining <= 0) { throw new java.util.concurrent.TimeoutException("P1 replay tail deadline"); }
                    ids = pending.get(remaining, TimeUnit.NANOSECONDS);
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("P1 replay tail capture interrupted; retain its original I/O", failure);
                } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
                    throw new IllegalStateException("P1 replay tail capture is unconfirmed; retain its original I/O", failure);
                }
                final SourceProof after = requireProof();
                if (!before.equals(after)) {
                    throw new IllegalStateException("P1 replay tail changed its guarded SUBSCRIBE proof");
                }
                if (ids == null || ids.size() != 1 || ids.getFirst() == null
                        || !physicalTopic.equals(ids.getFirst().getOwnerTopic())
                        || !(ids.getFirst() instanceof MessageIdAdv tail)) {
                    throw new IllegalStateException("P1 replay tail lacks its single physical Source response");
                }
                final ReplayTail captured = ReplayTail.from(tail, assignment.shardId().partition());
                captured.requireNotBefore(positionedReaderCut);
                if (!barrier.empty()) {
                    final int relative = captured.compare(barrier.ledgerId(), barrier.entryId(),
                            barrier.normalizedLastBatchIndex());
                    if (relative < 0 || relative == 0 && barrier.batchSize() != 0
                            && captured.batchSize() != barrier.batchSize()) {
                        throw new IllegalStateException("P1 replay tail precedes or changes the original activation barrier");
                    }
                }
                if (lastDelivered != null) { captured.requireAtOrBefore(lastDelivered); }
                if (current != null) { captured.requireAtOrBefore((PulsarSourcePosition) current.position()); }
                final boolean atPositionedCut = captured.matches(positionedReaderCut);
                protection.run();
                readThrough = captured;
                replayTailAtPositionedCut = atPositionedCut;
                return null;
            });
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            failed = true;
            throw new IllegalStateException("P1 replay tail reader wait interrupted", failure);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
            failed = true;
            throw new IllegalStateException("P1 replay tail reader wait is unconfirmed", failure);
        } catch (RuntimeException | Error failure) {
            failed = true;
            throw failure;
        } finally {
            if (backend != null) { backend.cleanupProgress(); }
        }
    }

    /** Requires the actual decoded delivery (or exact original Store cut) and the actual applied Source position. */
    @Override public synchronized boolean replayTailReached(SourcePosition actualApplied) {
        ensureOpen();
        requireIndependentReader();
        if (readThrough == null) {
            throw new IllegalStateException("P1 replay tail was not captured on its original reader");
        }
        protection.run();
        requireProof();
        final PulsarSourcePosition observed;
        if (lastDelivered != null && readThrough.matches(lastDelivered)) {
            observed = lastDelivered;
        } else if (replayTailAtPositionedCut && lastDelivered == null && current == null && buffered == null) {
            // This is the original complete Store position, never a SourcePosition made from a last-message id.
            observed = positionedReaderCut;
        } else {
            return false;
        }
        if (!(actualApplied instanceof PulsarSourcePosition applied)) {
            throw new IllegalStateException("P1 replay completion lacks its actual applied Source position");
        }
        final int relative = applied.compareTo(observed);
        if (relative == 0 && !sameCanonicalPosition(applied, observed)) {
            throw new IllegalStateException("P1 replay applied position changed the complete observed Source position");
        }
        if (applied.ledgerId() == observed.ledgerId() && applied.entryId() == observed.entryId()) {
            readThrough.requireAtOrBefore(applied);
        }
        protection.run();
        return relative >= 0;
    }

    private void requireIndependentReader() {
        if (!target || independentReaderProof == null || sourceStartup == null || positionedReaderCut == null
                || verifiedCredential == null || backend == null) {
            throw new IllegalStateException("P1 replay tail requires its original independent Target startup reader");
        }
        requireReaderBinding(assignment, positionedReaderCut);
        barrier.validatePosition(positionedReaderCut);
    }

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
        final boolean independentReplay = target && sourceStartup != null;
        if (current != null) {
            requireProof();
            if (independentReplay) { requireReplayBound((PulsarSourcePosition) current.position()); }
            return true;
        }
        final SourceProof before = requireProof();
        if (independentReplay) {
            if (readThrough == null) {
                if (barrier.reachedBy(lastDelivered == null ? positionedReaderCut : lastDelivered)) {
                    throw new IllegalStateException("P1 recovery must capture its new tail before probing past the original barrier");
                }
            } else if ((lastDelivered != null && readThrough.matches(lastDelivered))
                    || (replayTailAtPositionedCut && lastDelivered == null && buffered == null)) { return false; }
        } else if (target && (barrier.empty() || lastDelivered != null && barrier.reachedBy(lastDelivered))) {
            return false;
        }
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
            if (target && (independentReplay || !barrier.empty())) {
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
            final SourceReplayEntry decoded = target
                    ? PulsarClientArtifactSourceRecordConsumer.decodeTargetReplayRecord(message, assignment.shardId(),
                            physicalTopic, after.attestation(), after.generation(), after.digest())
                    : PulsarClientArtifactSourceRecordConsumer.decodeReplayRecord(message, assignment.shardId(),
                            physicalTopic, after.attestation(), after.generation(), after.digest());
            if (independentReplay) {
                requireReplayBound((PulsarSourcePosition) decoded.position());
            }
            protection.run();
            current = decoded;
            buffered = null;
            return true;
        } catch (RuntimeException | Error failure) {
            buffered = message;
            throw failure;
        }
    }

    private void requireReplayBound(PulsarSourcePosition position) {
        barrier.validatePosition(position);
        if (readThrough != null) {
            readThrough.requireAtOrBefore(position);
            return;
        }
        int relative = Long.compareUnsigned(position.ledgerId(), barrier.ledgerId());
        if (relative == 0) { relative = Long.compareUnsigned(position.entryId(), barrier.entryId()); }
        if (relative == 0) {
            relative = Integer.compareUnsigned(position.normalizedBatchIndex(), barrier.normalizedLastBatchIndex());
        }
        if (barrier.empty() || relative > 0) {
            throw new IllegalStateException("P1 recovery delivered past the original barrier before its new tail capture");
        }
    }

    @Override
    public synchronized SourceReplayEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Pulsar recovery source is exhausted");
        }
        final SourceReplayEntry result = current;
        lastDeliveredEntry = result;
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
        if (generation == 0 || generation != (independentReaderProof == null
                ? barrier.guardedSourceConnectionGeneration() : independentReaderProof.generation())) {
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
        final var proof = new SourceProof(generation, attestation, digest);
        if (independentReaderProof != null && !independentReaderProof.equals(proof)) {
            throw new IllegalStateException("original Target recovery reader native proof changed");
        }
        return proof;
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

    /**
     * Native last-message coordinates only: timestamps and complete Source positions come from actual delivery.
     * An entry-only query leaves kind unknown because the SDK also uses it for a singleton batch's last member.
     */
    private record ReplayTail(long ledgerId, long entryId, int lastBatchIndex, int batchSize, boolean batchKindKnown) {
        private static ReplayTail from(MessageIdAdv tail, int partition) {
            final int nativeIndex = tail.getBatchIndex();
            final int nativeSize = tail.getBatchSize();
            if (tail.getLedgerId() < 0 || tail.getEntryId() < 0 || tail.getFirstChunkMessageId() != null
                    || tail.getPartitionIndex() < -1
                    || tail.getPartitionIndex() >= 0 && tail.getPartitionIndex() != partition
                    || nativeIndex < -1 || nativeIndex == Integer.MAX_VALUE || nativeSize < 0) {
                throw new IllegalStateException("P1 replay tail lacks a finite complete native message identity");
            }
            final int last = Math.max(0, nativeIndex);
            // Broker GetLastMessageId reports the largest batch index. Its client wrapper can omit batchSize.
            final int size = last + 1;
            if (nativeSize != 0 && nativeSize != size) {
                throw new IllegalStateException("P1 replay tail changes the native final batch shape");
            }
            return new ReplayTail(tail.getLedgerId(), tail.getEntryId(), last, size, nativeIndex >= 0);
        }

        /** Compares captured tail coordinates with another actual native coordinate. */
        private int compare(long ledger, long entry, int index) {
            int result = Long.compareUnsigned(ledgerId, ledger);
            if (result == 0) { result = Long.compareUnsigned(entryId, entry); }
            return result == 0 ? Integer.compareUnsigned(lastBatchIndex, index) : result;
        }

        private boolean sameEntry(PulsarSourcePosition position) {
            return ledgerId == position.ledgerId() && entryId == position.entryId();
        }

        private void requireShape(PulsarSourcePosition position) {
            if (sameEntry(position) && (position.batchSize() != batchSize
                    || batchKindKnown && position.entryKind() != PulsarSourcePosition.EntryKind.BATCH)) {
                throw new IllegalStateException("P1 replay position does not match the captured tail's complete batch shape");
            }
        }

        private void requireNotBefore(PulsarSourcePosition position) {
            requireShape(position);
            if (compare(position.ledgerId(), position.entryId(), position.normalizedBatchIndex()) < 0) {
                throw new IllegalStateException("P1 replay tail precedes the original audited Store cut");
            }
        }

        private void requireAtOrBefore(PulsarSourcePosition position) {
            requireShape(position);
            if (compare(position.ledgerId(), position.entryId(), position.normalizedBatchIndex()) < 0) {
                throw new IllegalStateException("P1 replay delivered beyond or skipped its captured finite tail");
            }
        }

        private boolean matches(PulsarSourcePosition position) {
            requireShape(position);
            return compare(position.ledgerId(), position.entryId(), position.normalizedBatchIndex()) == 0;
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
