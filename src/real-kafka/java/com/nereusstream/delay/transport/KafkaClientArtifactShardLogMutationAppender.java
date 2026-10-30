package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SystemWriterAppendBudget;
import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.TargetScopeExposureSource;
import com.nereusstream.delay.ownership.TargetSloControlScopeAuthority;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
import java.time.Duration;
import java.util.Objects;
import java.util.HashSet;
import java.util.Set;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.GuardedCallback;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.clients.producer.GuardedResponseEvidence;
import org.apache.kafka.clients.producer.GuardedSourceCustody;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.clients.producer.ResourceGuardException;
import org.apache.kafka.common.Uuid;

/**
 * Guarded K1 append authority for signed System Mutations on one Shard Log
 * partition.
 *
 * <p>The synchronous SPI is backed by the native guarded producer callback,
 * so a successful result is returned only after the broker response carries
 * the exact cluster/topic UUID/partition and log-append timestamp. A timeout,
 * malformed response or ordinary producer failure remains UNKNOWN; only the
 * K1 resource-guard exception that proves non-persistence can return the
 * definitive negative branch.</p>
 */
public final class KafkaClientArtifactShardLogMutationAppender implements ShardLogMutationAppender, AutoCloseable {
    private final GuardedProducer<byte[], byte[]> producer;
    private final ShardId shard;
    private final String authenticatedClusterId;
    private final String physicalTopic;
    private final UUID nativeTopicUuid;
    private final int partition;
    private final ProducerResourceGuard guard;
    private final Duration responseTimeout;
    private final SystemWriterAppendBudget.Writer writer;
    private final Runnable sourceGate;
    private final TargetScopeExposureSource.CopyContract copyContract;
    private final TargetScopeExposureSource.Issuer exposureIssuer;
    private final Set<NativeAppend> retainedAppends = new HashSet<>();
    private int nativeAppends;
    private boolean nativeTerminated;

    public KafkaClientArtifactShardLogMutationAppender(
            final GuardedProducer<byte[], byte[]> producer,
            final ShardId shard,
            final String authenticatedClusterId,
            final String physicalTopic,
            final UUID nativeTopicUuid,
            final Duration responseTimeout) {
        this(producer, shard, authenticatedClusterId, physicalTopic, nativeTopicUuid, responseTimeout, null);
    }

    public KafkaClientArtifactShardLogMutationAppender(
            final GuardedProducer<byte[], byte[]> producer,
            final ShardId shard,
            final String authenticatedClusterId,
            final String physicalTopic,
            final UUID nativeTopicUuid,
            final Duration responseTimeout,
            final SystemWriterAppendBudget.Writer writer) {
        this(producer, shard, authenticatedClusterId, physicalTopic, nativeTopicUuid, responseTimeout, writer, null);
    }
    KafkaClientArtifactShardLogMutationAppender(GuardedProducer<byte[],byte[]> producer, ShardId shard,
            String authenticatedClusterId, String physicalTopic, UUID nativeTopicUuid, Duration responseTimeout,
            SystemWriterAppendBudget.Writer writer, Runnable sourceGate) {
        this(producer, shard, authenticatedClusterId, physicalTopic, nativeTopicUuid, responseTimeout,
                writer, sourceGate, null, null);
    }
    KafkaClientArtifactShardLogMutationAppender(GuardedProducer<byte[],byte[]> producer, ShardId shard,
            String authenticatedClusterId, String physicalTopic, UUID nativeTopicUuid, Duration responseTimeout,
            SystemWriterAppendBudget.Writer writer, Runnable sourceGate, TargetScopeExposureSource.CopyContract copyContract,
            TargetScopeExposureSource.Issuer exposureIssuer) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.authenticatedClusterId = requireText(authenticatedClusterId, "authenticatedClusterId");
        this.physicalTopic = requireText(physicalTopic, "physicalTopic");
        this.nativeTopicUuid = Objects.requireNonNull(nativeTopicUuid, "nativeTopicUuid");
        this.partition = shard.partition();
        this.guard = new ProducerResourceGuard(
                this.authenticatedClusterId,
                this.physicalTopic,
                new Uuid(nativeTopicUuid.getMostSignificantBits(), nativeTopicUuid.getLeastSignificantBits()),
                this.partition);
        this.responseTimeout = Objects.requireNonNull(responseTimeout, "responseTimeout");
        if (responseTimeout.isZero() || responseTimeout.isNegative() || responseTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("responseTimeout must be positive");
        }
        this.writer = writer;
        this.sourceGate = sourceGate;
        this.copyContract = copyContract;
        this.exposureIssuer = exposureIssuer;
        if (writer != null) {
            writer.bindProducer(producer);
        }
    }

    @Override
    public AppendOutcome append(final SystemMutation mutation) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        if (!shard.equals(exact.shardId())) {
            throw new IllegalArgumentException("Kafka Shard Log mutation belongs to another Shard");
        }
        final var reservation = writer == null ? null : writer.tryReserve(shard).orElse(null);
        if (writer != null && reservation == null) {
            return AppendOutcome.definitelyNotPersisted();
        }
        try {
            return appendReserved(exact, reservation);
        } finally {
            if (reservation != null) {
                reservation.callerReturned();
            }
        }
    }

    private AppendOutcome appendReserved(final SystemMutation exact,
            final SystemWriterAppendBudget.Reservation reservation) {
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
        final CompletableFuture<Observation> completion = new CompletableFuture<>();
        final NativeAppend nativeAppend;
        try {
            nativeAppend = new NativeAppend(TargetScopeExposureSource.acquire(exact, copyContract, exposureIssuer));
            synchronized (this) { retainedAppends.add(nativeAppend); }
        } catch (RuntimeException | Error failed) {
            if (reservation != null) reservation.sdkCompleted();
            throw failed;
        }
        try {
            final java.util.function.Supplier<java.util.concurrent.Future<GuardedRecordMetadata>> send = () -> {
                if (sourceGate != null) {
                    try { sourceGate.run(); }
                    catch (RuntimeException failed) { throw new SourceWriteNotStarted(failed); }
                }
                try {
                    if (nativeAppend.exposure != null) {
                        nativeAppend.exposure.claim(frame, copyContract);
                    }
                } catch (RuntimeException failed) { throw new SourceWriteNotStarted(failed); }
                nativeStarted(nativeAppend);
                final var record = new ProducerRecord<byte[], byte[]>(physicalTopic, partition, null, frame);
                final GuardedCallback callback = (metadata, failure) -> {
                    try {
                        if (reservation != null) reservation.sdkCompleted();
                        nativeExited(nativeAppend);
                        completion.complete(new Observation(metadata, failure));
                    } catch (RuntimeException | Error cleanup) {
                        completion.completeExceptionally(cleanup);
                        throw cleanup;
                    }
                };
                return nativeAppend.exposure == null
                        ? producer.sendGuarded(record, guard, callback)
                        : producer.sendGuarded(record, guard, new GuardedSourceCustody() {
                            @Override public void beforeCustody(byte[] exactSerializedFrame) {
                                nativeAppend.exposure.beforeSdkCustody(exactSerializedFrame);
                            }
                            @Override public void afterCustody() {
                                nativeAppend.exposure.afterSdkCustody();
                            }
                        }, callback);
            };
            if (writer == null) {
                send.get();
            } else if (!writer.invokeSdk(reservation, send).started()) {
                notInvoked(nativeAppend);
                return AppendOutcome.definitelyNotPersisted();
            }
        } catch (RuntimeException failure) {
            if (failure instanceof SourceWriteNotStarted) {
                if (reservation != null) reservation.sdkCompleted();
                notInvoked(nativeAppend);
                if (failure.getCause() instanceof CheckpointInputFuture.Cleanup
                        && failure.getCause() instanceof RuntimeException cleanup) throw cleanup;
                return AppendOutcome.definitelyNotPersisted();
            }
            final ResourceGuardException proof = unwrap(failure);
            if (!nativeAppend.started) {
                if (reservation != null) reservation.sdkCompleted();
                notInvoked(nativeAppend);
            }
            if (reservation != null && proof != null && proof.definitelyNotPersisted()) {
                reservation.sdkCompleted();
            }
            if (proof != null && proof.definitelyNotPersisted()) nativeExited(nativeAppend);
            if (failure instanceof CheckpointInputFuture.Cleanup) throw failure;
            return failure(failure);
        }
        final Observation observation;
        try {
            observation = completion.get(responseTimeout.toMillis(), TimeUnit.MILLISECONDS);
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
        if (observation.failure() != null) {
            return failure(observation.failure());
        }
        return persisted(observation.metadata());
    }

    private synchronized void nativeStarted(NativeAppend append) {
        if (nativeTerminated) throw new SourceWriteNotStarted(new IllegalStateException("original Kafka producer is closed"));
        nativeAppends = Math.incrementExact(nativeAppends); append.started = true;
    }

    private void nativeExited(NativeAppend append) {
        synchronized (append) {
            synchronized (this) {
                if (append.exited) return;
                append.exited = true;
                if (append.started && !nativeTerminated) nativeAppends--;
            }
            exposureExited(append, true);
        }
    }

    private void notInvoked(NativeAppend append) {
        synchronized (append) {
            synchronized (this) {
                if (append.exited) return;
                append.exited = true;
                if (append.started && !nativeTerminated) nativeAppends--;
            }
            exposureExited(append, false);
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

    /** An UNKNOWN result keeps its original native callback obligation until completion or actual producer exit. */
    synchronized boolean nativeOperationsExited() { return nativeAppends == 0; }

    /** Only the same producer's actual guarded native termination settles callbacks with no observed response. */
    void producerNativeClosed() {
        if (!producer.guardedResourcesTerminated()) throw new IllegalStateException("original Kafka producer resources remain live");
        final Set<NativeAppend> pending;
        synchronized (this) {
            nativeTerminated = true; nativeAppends = 0;
            pending = Set.copyOf(retainedAppends);
        }
        for (NativeAppend append : pending) nativeExited(append);
    }

    @Override
    public void close() {
        if (writer != null) { writer.beginClose(); }
        producer.close();
        if (producer.guardedResourcesTerminated()) producerNativeClosed();
        retryExposureCleanup().thenRun(() -> {
            if (writer != null && exposureLoansExited()) writer.producerClosed();
        });
    }

    private AppendOutcome persisted(final GuardedRecordMetadata metadata) {
        if (metadata == null || metadata.recordMetadata() == null || metadata.responseEvidence() == null) {
            return AppendOutcome.unknown();
        }
        final GuardedResponseEvidence evidence = metadata.responseEvidence();
        if (!authenticatedClusterId.equals(evidence.authenticatedClusterId())
                || !physicalTopic.equals(evidence.canonicalTopic())
                || !guard.expectedTopicId().equals(evidence.expectedTopicId())
                || evidence.partition() != partition
                || evidence.errorCode() != 0
                || !physicalTopic.equals(metadata.recordMetadata().topic())
                || metadata.recordMetadata().partition() != partition
                || metadata.recordMetadata().offset() < 0
                || evidence.logAppendTimeMs() < 0) {
            return AppendOutcome.unknown();
        }
        final OptionalInt leaderEpoch = evidence.responseLeaderEpoch();
        final KafkaSourcePosition position = new KafkaSourcePosition(
                shard,
                authenticatedClusterId,
                nativeTopicUuid,
                metadata.recordMetadata().offset(),
                leaderEpoch.isPresent() ? leaderEpoch.getAsInt() : null,
                evidence.logAppendTimeMs());
        return AppendOutcome.persisted(position);
    }

    private static AppendOutcome failure(final Throwable failure) {
        final ResourceGuardException guardFailure = unwrap(failure);
        return guardFailure != null && guardFailure.definitelyNotPersisted()
                ? AppendOutcome.definitelyNotPersisted()
                : AppendOutcome.unknown();
    }

    private static ResourceGuardException unwrap(final Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof ResourceGuardException resourceGuardException) {
                return resourceGuardException;
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

    private record Observation(GuardedRecordMetadata metadata, Throwable failure) {}
    private static final class NativeAppend {
        private final TargetSloControlScopeAuthority.ExposurePermit exposure;
        private boolean started;
        private volatile boolean exited;
        private CheckpointInputFuture.Cleanup cleanup;
        private CompletableFuture<Void> cleanupRetry;
        private Throwable cleanupFailure;
        private NativeAppend(TargetSloControlScopeAuthority.ExposurePermit exposure) {
            this.exposure = exposure; this.cleanup = exposure;
        }
    }
    private static final class SourceWriteNotStarted extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private SourceWriteNotStarted(Throwable cause) { super("Source credential denied before native append", cause); }
    }
}
