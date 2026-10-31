package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SystemWriterAppendBudget;
import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.TargetScopeExposureSource;
import com.nereusstream.delay.ownership.TargetSloControlScopeAuthority;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.GuardedSourceCustody;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TopicResourceGuardException;
import org.apache.pulsar.client.api.GuardedSourceCustodyException;

/**
 * Guarded P1 append authority for signed System Mutations on one Shard Log
 * topic.
 *
 * <p>The producer is resource guarded, while the source consumer supplies the
 * current guarded SUBSCRIBE connection proof required by the active Pulsar
 * barrier. A proof change during the send makes the append UNKNOWN even when
 * the broker accepted the bytes; recovery must then reconcile the exact
 * mutation from the Shard Log.</p>
 */
public final class PulsarClientArtifactShardLogMutationAppender implements ShardLogMutationAppender, AutoCloseable {
    private final Producer<byte[]> producer;
    private final GuardedConsumer<?> sourceConsumer;
    private final ShardId shard;
    private final TopicResourceGuard expectedGuard;
    private final String physicalTopic;
    private final int partition;
    private final Duration responseTimeout;
    private final SystemWriterAppendBudget.Writer writer;
    private final Runnable sourceGate;
    private final TargetScopeExposureSource.CopyContract copyContract;
    private final TargetScopeExposureSource.Issuer exposureIssuer;
    private final Set<NativeAppend> retainedAppends = new HashSet<>();
    private boolean nativeTerminated;

    public PulsarClientArtifactShardLogMutationAppender(
            final Producer<byte[]> producer,
            final GuardedConsumer<?> sourceConsumer,
            final ShardId shard,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final Duration responseTimeout) {
        this(producer, sourceConsumer, shard, authenticatedClusterId, resourceIncarnation, physicalTopic,
                physicalTopicCreationTimestamp, responseTimeout, null);
    }

    public PulsarClientArtifactShardLogMutationAppender(
            final Producer<byte[]> producer,
            final GuardedConsumer<?> sourceConsumer,
            final ShardId shard,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final Duration responseTimeout,
            final SystemWriterAppendBudget.Writer writer) {
        this(producer, sourceConsumer, shard, authenticatedClusterId, resourceIncarnation, physicalTopic,
                physicalTopicCreationTimestamp, responseTimeout, writer, null);
    }
    PulsarClientArtifactShardLogMutationAppender(Producer<byte[]> producer, GuardedConsumer<?> sourceConsumer,
            ShardId shard, String authenticatedClusterId, byte[] resourceIncarnation, String physicalTopic,
            long physicalTopicCreationTimestamp, Duration responseTimeout, SystemWriterAppendBudget.Writer writer, Runnable sourceGate) {
        this(producer, sourceConsumer, shard, authenticatedClusterId, resourceIncarnation, physicalTopic,
                physicalTopicCreationTimestamp, responseTimeout, writer, sourceGate, null, null);
    }
    PulsarClientArtifactShardLogMutationAppender(Producer<byte[]> producer, GuardedConsumer<?> sourceConsumer,
            ShardId shard, String authenticatedClusterId, byte[] resourceIncarnation, String physicalTopic,
            long physicalTopicCreationTimestamp, Duration responseTimeout, SystemWriterAppendBudget.Writer writer,
            Runnable sourceGate, TargetScopeExposureSource.CopyContract copyContract, TargetScopeExposureSource.Issuer exposureIssuer) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.sourceConsumer = Objects.requireNonNull(sourceConsumer, "sourceConsumer");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.expectedGuard = new TopicResourceGuard(
                Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId"),
                resourceIncarnation,
                physicalTopicCreationTimestamp);
        this.physicalTopic = requireText(physicalTopic, "physicalTopic");
        this.partition = shard.partition();
        this.responseTimeout = Objects.requireNonNull(responseTimeout, "responseTimeout");
        if (responseTimeout.isZero() || responseTimeout.isNegative() || responseTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("responseTimeout must be positive");
        }
        if (!physicalTopic.equals(producer.getTopic())
                || !physicalTopic.equals(sourceConsumer.getTopic())
                || !expectedGuard.equals(sourceConsumer.resourceGuard())) {
            throw new IllegalArgumentException("Pulsar mutation binding is not pinned to one guarded topic");
        }
        this.writer = writer;
        this.sourceGate = sourceGate;
        this.copyContract = copyContract;
        this.exposureIssuer = exposureIssuer;
        if (writer != null) { writer.bindProducer(producer); }
    }

    @Override
    public AppendOutcome append(final SystemMutation mutation) {
        return append(mutation, null, null, null, null, null, null);
    }

    /** Only the original Root permit admits this type4 send before the active Host exists. */
    AppendOutcome appendRecoveryFence(com.nereusstream.delay.ownership.TargetTimeFenceMutationFactory.Prepared exact,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup.RecoveryFenceAppend permit,
            PulsarClientArtifactRecoveryFenceWriter recoveryWriter) {
        return append(Objects.requireNonNull(exact).mutation(), Objects.requireNonNull(recoveryWriter), exact,
                Objects.requireNonNull(permit), null, null, null);
    }

    /** Independent migration CONTROL entry; the ordinary Host nativeSend gate remains unchanged. */
    AppendOutcome appendMigrationControl(com.nereusstream.delay.ownership.TargetMigrationControlMutationFactory.Prepared exact,
            com.nereusstream.delay.ownership.TargetMigrationAppendPermit permit,
            PulsarClientArtifactTargetMigrationControlWriter writer) {
        return append(Objects.requireNonNull(exact).mutation(), null, null, null, Objects.requireNonNull(writer),
                exact, Objects.requireNonNull(permit));
    }

    private AppendOutcome append(final SystemMutation mutation, PulsarClientArtifactRecoveryFenceWriter recoveryWriter,
            com.nereusstream.delay.ownership.TargetTimeFenceMutationFactory.Prepared fence,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup.RecoveryFenceAppend permit,
            PulsarClientArtifactTargetMigrationControlWriter migrationWriter,
            com.nereusstream.delay.ownership.TargetMigrationControlMutationFactory.Prepared migration,
            com.nereusstream.delay.ownership.TargetMigrationAppendPermit migrationPermit) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        if (!shard.equals(exact.shardId())) {
            throw new IllegalArgumentException("Pulsar Shard Log mutation belongs to another Shard");
        }
        final var reservation = writer == null ? null : writer.tryReserve(shard).orElse(null);
        if (writer != null && reservation == null) {
            return AppendOutcome.definitelyNotPersisted();
        }
        try {
            return appendReserved(exact, reservation, recoveryWriter, fence, permit, migrationWriter, migration, migrationPermit);
        } finally {
            if (reservation != null) { reservation.callerReturned(); }
        }
    }

    private AppendOutcome appendReserved(final SystemMutation exact,
            final SystemWriterAppendBudget.Reservation reservation, PulsarClientArtifactRecoveryFenceWriter recoveryWriter,
            com.nereusstream.delay.ownership.TargetTimeFenceMutationFactory.Prepared fence,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup.RecoveryFenceAppend permit,
            PulsarClientArtifactTargetMigrationControlWriter migrationWriter,
            com.nereusstream.delay.ownership.TargetMigrationControlMutationFactory.Prepared migration,
            com.nereusstream.delay.ownership.TargetMigrationAppendPermit migrationPermit) {
        final SourceProof before;
        try {
            before = sourceProof();
        } catch (RuntimeException failure) {
            if (reservation != null) { reservation.sdkCompleted(); }
            return AppendOutcome.unknown();
        }
        final byte[] frame;
        try {
            frame = exact.encodeFrame();
        } catch (RuntimeException failure) {
            if (reservation == null) { throw failure; }
            reservation.sdkCompleted();
            return AppendOutcome.definitelyNotPersisted();
        }
        if (reservation != null && !reservation.frameEncoded(frame.length)) {
            reservation.sdkCompleted();
            return AppendOutcome.definitelyNotPersisted();
        }
        final NativeAppend nativeAppend;
        try {
            nativeAppend = new NativeAppend(TargetScopeExposureSource.acquire(exact, copyContract, exposureIssuer), exact);
            synchronized (this) { retainedAppends.add(nativeAppend); }
        } catch (RuntimeException | Error failed) {
            if (reservation != null) reservation.sdkCompleted();
            throw failed;
        }
        final CompletableFuture<MessageId> completion;
        try {
            final java.util.function.Supplier<CompletableFuture<MessageId>> send = () -> {
                if (sourceGate != null) {
                    try { sourceGate.run(); }
                    catch (RuntimeException failed) { throw new SourceWriteNotStarted(failed); }
                }
                final java.util.function.Supplier<CompletableFuture<MessageId>> nativeSend = () -> {
                    try {
                        if (nativeAppend.exposure != null) {
                            nativeAppend.exposure.claim(frame, copyContract);
                        }
                    } catch (RuntimeException failed) { throw new SourceWriteNotStarted(failed); }
                    synchronized (this) {
                        if (nativeTerminated) throw new SourceWriteNotStarted(new IllegalStateException("original Pulsar producer is closed"));
                        nativeAppend.started = true;
                    }
                    return nativeAppend.exposure == null
                            ? producer.newMessage().value(frame).sendAsync()
                            : producer.sendGuardedSource(frame, new GuardedSourceCustody() {
                                @Override public void beforeCustody(byte[] exactSerializedFrame) {
                                    nativeAppend.exposure.beforeSdkCustody(exactSerializedFrame);
                                }
                                @Override public void afterCustody() {
                                    nativeAppend.exposure.afterSdkCustody();
                                }
                            });
                };
                return migrationWriter != null
                        ? migrationWriter.nativeSend(this, producer, migration, migrationPermit, nativeSend)
                        : recoveryWriter == null
                            ? PulsarClientArtifactOwnedBackendGroup.nativeSend(producer, nativeSend)
                            : recoveryWriter.nativeSend(this, producer, fence, permit, nativeSend);
            };
            if (writer == null) {
                completion = send.get();
            } else {
                final var handed = writer.invokeSdk(reservation, send);
                if (!handed.started()) { notInvoked(nativeAppend); return AppendOutcome.definitelyNotPersisted(); }
                completion = handed.result();
            }
        } catch (RuntimeException failure) {
            if (failure instanceof SourceWriteNotStarted) {
                if (reservation != null) reservation.sdkCompleted();
                notInvoked(nativeAppend);
                if (failure.getCause() instanceof CheckpointInputFuture.Cleanup
                        && failure.getCause() instanceof RuntimeException cleanup) throw cleanup;
                return AppendOutcome.definitelyNotPersisted();
            }
            if (!nativeAppend.started) {
                if (reservation != null) reservation.sdkCompleted();
                notInvoked(nativeAppend);
            } else if (failure(failure).disposition() == AppendDisposition.DEFINITIVELY_NOT_PERSISTED) {
                if (reservation != null) reservation.sdkCompleted();
                nativeExited(nativeAppend);
            }
            if (failure instanceof CheckpointInputFuture.Cleanup) throw failure;
            return failure(failure);
        }
        if (completion == null) { return AppendOutcome.unknown(); }
        final var accountingExit = completion.whenComplete((result, failure) -> {
            if (!completion.isCancelled()) {
                if (reservation != null) reservation.sdkCompleted();
                nativeExited(nativeAppend);
            }
        });
        if (recoveryWriter != null) recoveryWriter.trackSdkCompletion(completion, accountingExit);
        if (migrationWriter != null) migrationWriter.trackSdkCompletion(completion, accountingExit);
        final MessageId messageId;
        try {
            messageId = accountingExit.get(responseTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return AppendOutcome.unknown();
        } catch (ExecutionException execution) {
            if (execution.getCause() instanceof CheckpointInputFuture.Cleanup
                    && execution.getCause() instanceof RuntimeException cleanup) throw cleanup;
            return failure(execution.getCause());
        } catch (TimeoutException timeout) {
            return AppendOutcome.unknown();
        }
        final SourceProof after;
        try {
            after = sourceProof();
        } catch (RuntimeException failure) {
            return AppendOutcome.unknown();
        }
        if (!before.equals(after)) {
            return AppendOutcome.unknown();
        }
        return persisted(messageId, after);
    }

    private static final class SourceWriteNotStarted extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private SourceWriteNotStarted(Throwable cause) { super("Source credential denied before native append", cause); }
    }

    private void notInvoked(NativeAppend append) {
        synchronized (append) {
            synchronized (this) {
                if (append.exited) return;
                append.exited = true;
            }
            exposureExited(append, false);
        }
    }

    private void nativeExited(NativeAppend append) {
        synchronized (append) {
            synchronized (this) {
                if (append.exited) return;
                append.exited = true;
            }
            exposureExited(append, true);
        }
    }

    private void exposureExited(NativeAppend append, boolean invoked) {
        if (append.exposure == null) {
            synchronized (this) { retainedAppends.remove(append); }
            return;
        }
        synchronized (append) {
            try {
                if (invoked) append.exposure.sdkCompleted(); else append.exposure.notInvoked();
            } catch (RuntimeException | Error failed) {
                append.cleanupFailure = failed;
                if (failed instanceof CheckpointInputFuture.Cleanup cleanup) append.cleanup = cleanup;
            }
        }
        advanceExposureCleanup(append, false);
    }

    private CompletableFuture<Void> advanceExposureCleanup(NativeAppend append, boolean retryFailed) {
        synchronized (append) {
            if (append.cleanupRetry == null && append.exposure.cleanupComplete()) {
                synchronized (this) { retainedAppends.remove(append); }
                return CompletableFuture.completedFuture(null);
            }
            if (append.cleanupRetry != null && (!append.cleanupRetry.isDone() || !retryFailed)) {
                return append.cleanupRetry;
            }
            final CompletableFuture<Void> attempt;
            try { attempt = Objects.requireNonNull(append.cleanup.retryCleanup(), "original exposure cleanup future"); }
            catch (RuntimeException | Error failed) {
                if (append.cleanupFailure == null) append.cleanupFailure = failed;
                return append.cleanupRetry = CompletableFuture.failedFuture(failed);
            }
            append.cleanupRetry = attempt.whenComplete((ignored, failed) -> {
                synchronized (append) {
                    if (failed != null && append.cleanupFailure == null) append.cleanupFailure = failed;
                }
                if (failed == null) {
                    if (!append.exposure.cleanupComplete()) throw new IllegalStateException("original exposure cleanup has not exited");
                    synchronized (this) { retainedAppends.remove(append); }
                }
            });
            return append.cleanupRetry;
        }
    }

    /** Requests only the original read-loan cleanup; SDK callbacks never wait for its I/O. */
    CompletableFuture<Void> retryExposureCleanup() {
        final Set<NativeAppend> pending;
        synchronized (this) { pending = Set.copyOf(retainedAppends); }
        final var attempts = new java.util.ArrayList<CompletableFuture<Void>>();
        for (NativeAppend append : pending) {
            if (append.exposure != null && append.exited) attempts.add(advanceExposureCleanup(append, true));
        }
        return CompletableFuture.allOf(attempts.toArray(CompletableFuture<?>[]::new));
    }

    synchronized boolean exposureLoansExited() {
        return retainedAppends.stream().noneMatch(append -> append.exposure != null);
    }
    synchronized boolean nativeOperationsExited(SystemMutation exact) {
        return retainedAppends.stream().noneMatch(append -> !append.exited && java.util.Arrays.equals(append.mutationId, exact.systemMutationId()));
    }

    /** Called only after this producer's original native close or its original client backend actually exits. */
    void producerNativeClosed() {
        final Set<NativeAppend> pending;
        synchronized (this) { nativeTerminated = true; pending = Set.copyOf(retainedAppends); }
        for (NativeAppend append : pending) nativeExited(append);
    }

    private static final class NativeAppend {
        private final TargetSloControlScopeAuthority.ExposurePermit exposure;
        private final byte[] mutationId;
        private boolean started;
        private volatile boolean exited;
        private CheckpointInputFuture.Cleanup cleanup;
        private CompletableFuture<Void> cleanupRetry;
        private Throwable cleanupFailure;
        private NativeAppend(TargetSloControlScopeAuthority.ExposurePermit exposure, SystemMutation exact) {
            this.exposure = exposure; this.cleanup = exposure; mutationId = exact.systemMutationId();
        }
    }

    @Override
    public void close() {
        if (writer != null) { writer.beginClose(); }
        try {
            producer.close();
            producerNativeClosed();
            retryExposureCleanup().thenRun(() -> {
                if (writer != null && exposureLoansExited()) writer.producerClosed();
            });
        } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
            throw new IllegalStateException("Pulsar guarded mutation producer close failed", failure);
        }
    }

    private AppendOutcome persisted(final MessageId messageId, final SourceProof sourceProof) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !(messageId instanceof MessageIdAdv advanced)
                || !expectedGuard.equals(guarded.resourceGuard())
                || !physicalTopic.equals(guarded.physicalTopic())
                || guarded.partition() != partition
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null
                || advanced.getFirstChunkMessageId() != null) {
            return AppendOutcome.unknown();
        }
        final long ledgerId = advanced.getLedgerId();
        final long entryId = advanced.getEntryId();
        if (ledgerId < 0 || entryId < 0) {
            return AppendOutcome.unknown();
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final boolean batched = rawBatchIndex >= 0;
        final int batchIndex = batched ? rawBatchIndex : 0;
        final int batchSize = batched ? rawBatchSize : 1;
        if (batched && rawBatchSize <= 0 || batchSize <= 0 || Integer.compareUnsigned(batchIndex, batchSize) >= 0) {
            return AppendOutcome.unknown();
        }
        final GuardedSendSuccessEvidence evidence = guarded.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition);
        if (!expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != ledgerId
                || evidence.entryId() != entryId
                || evidence.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            return AppendOutcome.unknown();
        }
        final PulsarSourcePosition position = new PulsarSourcePosition(
                shard,
                expectedGuard.resourceIncarnation(),
                physicalTopic,
                ledgerId,
                entryId,
                batchIndex,
                batchSize,
                batched ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                guarded.brokerEntryTimestamp());
        return AppendOutcome.persisted(position, sourceProof.connectionGeneration(), sourceProof.digest());
    }

    private AppendOutcome failure(final Throwable failure) {
        final TopicResourceGuardException guardFailure = unwrap(failure);
        return guardFailure != null
                        && expectedGuard.equals(guardFailure.expectedGuard())
                        && guardFailure.definitelyNotPersisted()
                        && (guardFailure.responseEvidence().isPresent()
                                || guardFailure instanceof GuardedSourceCustodyException)
                ? AppendOutcome.definitelyNotPersisted()
                : AppendOutcome.unknown();
    }

    private SourceProof sourceProof() {
        if (!expectedGuard.equals(sourceConsumer.resourceGuard()) || !physicalTopic.equals(sourceConsumer.getTopic())) {
            throw new IllegalStateException("Pulsar source consumer guard binding changed");
        }
        final Optional<TopicResourceGuardAttestation> attestation = sourceConsumer.resourceGuardAttestation();
        final long generation = sourceConsumer.connectionGeneration();
        if (attestation.isEmpty() || generation <= 0) {
            throw new IllegalStateException("Pulsar source consumer has no guarded connection proof");
        }
        final TopicResourceGuardAttestation exact = attestation.orElseThrow();
        if (!new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition).equals(exact)) {
            throw new IllegalStateException("Pulsar source consumer returned a foreign connection proof");
        }
        return new SourceProof(generation, PulsarClientArtifactSourceRecordConsumer.attestationDigest(exact));
    }

    private static TopicResourceGuardException unwrap(final Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof TopicResourceGuardException guardFailure) {
                return guardFailure;
            }
            current = current.getCause();
        }
        return null;
    }

    private static String requireText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }

    private record SourceProof(long connectionGeneration, byte[] digest) {
        private SourceProof {
            digest = digest.clone();
        }

        @Override
        public byte[] digest() {
            return digest.clone();
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof SourceProof that
                    && connectionGeneration == that.connectionGeneration
                    && Arrays.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectionGeneration, Arrays.hashCode(digest));
        }
    }
}
