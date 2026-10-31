package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.*;
import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Actual K1 SDK openings under the original maintenance owner, before any Target activation exists. */
public final class KafkaClientArtifactTargetMigrationFactory {
    private KafkaClientArtifactTargetMigrationFactory() {}

    /** Literal original Source deployment; the service Source client is never constructed here. */
    public static Opening openConfigured(TargetMigrationSourceOpening original, Path sourceConfiguration,
            Path writerConfiguration, LocalBrokerMtlsResolver resolver, SourceTransportCredentialAuthorization.Trust trust,
            Supplier<TrustedUtcIntervalEvidence> time) throws IOException {
        final var source = KafkaClientArtifactTargetWorkerSourceFactory.loadSourceDeployment(
                sourceConfiguration, original.assignment().shardId()).source();
        final var writer = KafkaClientArtifactTargetWorkerSourceFactory.loadSystemWriterConfiguration(writerConfiguration);
        return openConfigured(original, source, writer, resolver, trust, time);
    }

    public static final int MAXIMUM_SDK_ALLOCATIONS = 2;

    /** A replacement reader reuses its allocation only after the prior exact native consumer actually exits. */
    public static CapacityVector migrationSdkCost(
            KafkaClientArtifactTargetWorkerSourceFactory.SourceConfiguration source,
            KafkaClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration writer) {
        if (source.recoveryReader().allocationId().equals(writer.settings().allocationId())) {
            throw new IllegalArgumentException("migration reader and original CONTROL producer require distinct allocations");
        }
        return source.recoveryReader().cost().add(writer.cost());
    }

    /** Called in the root's existing owned preparation I/O, with no nested submit-and-wait. */
    public static Opening openConfigured(TargetMigrationSourceOpening original,
            KafkaClientArtifactTargetWorkerSourceFactory.SourceConfiguration source,
            KafkaClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration writer,
            LocalBrokerMtlsResolver resolver, SourceTransportCredentialAuthorization.Trust trust,
            Supplier<TrustedUtcIntervalEvidence> time) {
        Objects.requireNonNull(original).requireCurrent(); Objects.requireNonNull(source); Objects.requireNonNull(writer);
        migrationSdkCost(source, writer);
        final var readerReceipt = Objects.requireNonNull(resolver).resolveSource(source.authorizationFile(),
                source.signedRoute(), Objects.requireNonNull(trust), Objects.requireNonNull(time));
        original.requireCurrent();
        final var writerReceipt = resolver.resolveSource(writer.authorizationFile(), writer.signedRoute(), trust, time);
        original.requireCurrent();
        readerReceipt.authorization().requireAssignment(original.assignment());
        writerReceipt.authorization().requireAssignment(original.assignment());
        final var resource = readerReceipt.authorization().resource().kafka();
        if (!source.settings().shard().equals(original.assignment().shardId())
                || !source.settings().authenticatedClusterId().equals(resource.authenticatedClusterId())
                || !source.settings().topicId().equals(resource.nativeTopicUuid())
                || !source.settings().topic().equals(readerReceipt.authorization().physicalTopic())) {
            throw new IllegalArgumentException("configured migration reader changes its original K1 Source resource");
        }
        final var lease = original.ownerLease();
        final var identity = new OwnerIdentity(source.deploymentId(), source.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
        original.bindSourceIdentity(readerReceipt, identity, time.get());
        if (!Bytes.constantTimeEquals(writerReceipt.authorization().deploymentId(), identity.deploymentId())) {
            throw new IllegalArgumentException("migration CONTROL transport changes the original maintenance deployment");
        }
        final var actual = new Opening(original, source, readerReceipt, time);
        final var graph = original.graph();
        actual.writer = KafkaClientArtifactTargetMigrationControlWriter.open(writer.settings(),
                new java.util.HashMap<>(writer.operational()), source.sdkJar(), graph.config(), original,
                writerReceipt, time, original.sdkBudget(), writer.cost(), graph.classes(), graph.resources());
        original.requireCurrent(); return actual;
    }

    public static final class Opening implements TargetMigrationTransport {
        private final TargetMigrationSourceOpening original;
        private final KafkaClientArtifactTargetWorkerSourceFactory.SourceConfiguration source;
        private final LocalBrokerMtlsResolver.SourceReceipt credential;
        private final Supplier<TrustedUtcIntervalEvidence> time;
        private KafkaClientArtifactTargetMigrationControlWriter writer;
        private TargetWorkerRecoverySource reader;
        private boolean stopping;
        private Throwable failure;
        private final java.util.List<ReaderOwner> readers = new java.util.ArrayList<>();
        private Opening(TargetMigrationSourceOpening original,
                KafkaClientArtifactTargetWorkerSourceFactory.SourceConfiguration source,
                LocalBrokerMtlsResolver.SourceReceipt credential, Supplier<TrustedUtcIntervalEvidence> time) {
            this.original = original; this.source = source; this.credential = credential; this.time = time;
        }
        @Override public void requireBinding(TargetMigrationSourceOpening expected) {
            if (original != expected) throw new IllegalArgumentException("migration SDK transport changes its original owner");
        }
        @Override public boolean advance() {
            original.requireCurrent();
            if (stopping) throw new IllegalStateException("migration transport is stopping");
            if (reader == null) openReaderAfter(original.baseline());
            return writer != null && reader != null;
        }
        @Override public TargetMigrationControlWriter writer() { original.requireCurrent(); return writer; }
        @Override public TargetWorkerRecoverySource reader() { original.requireCurrent(); return reader; }
        @Override public void requestStop() {
            stopping = true;
            if (writer != null) writer.requestStop();
            for (var owned : readers) owned.closeBeforeHostBinding();
        }
        @Override public boolean ioExited() {
            return (writer == null || writer.operationsExited())
                    && readers.stream().allMatch(owned -> owned.openingExited && (owned.cleanup == null || owned.cleanup.ioExited()));
        }
        @Override public boolean cleanupComplete() {
            return stopping && (writer == null || writer.cleanupComplete()) && readers.stream().allMatch(ReaderOwner::resourcesTerminated);
        }
        @Override public Throwable pendingIoFailure() {
            if (failure != null) return failure;
            if (writer != null && writer.cleanupFailure() != null) return writer.cleanupFailure();
            return readers.stream().map(ReaderOwner::cleanupFailure).filter(Objects::nonNull).findFirst().orElse(null);
        }

        /** Each call captures a new independent finite prefix; old readers remain charged until actual exit. */
        @Override public void openReaderAfter(SourcePosition actualAppliedCut) {
            original.requireCurrent();
            if (!(actualAppliedCut instanceof KafkaSourcePosition cut)) {
                throw new IllegalArgumentException("K1 migration requires its complete original Source cut");
            }
            if (stopping) throw new IllegalStateException("migration transport is stopping");
            if (reader != null && !reader.cleanupComplete()) reader.close();
            if (reader != null && !reader.cleanupComplete()) {
                throw new IllegalStateException("previous finite migration reader must actually exit before replacement");
            }
            readers.removeIf(ReaderOwner::resourcesTerminated);
            if (readers.size() >= 2) throw new IllegalStateException("original migration reader cleanup window is full");
            final var owner = new ReaderOwner(original); readers.add(owner);
            original.retainBackend(owner); // Construction and partial constructor cleanup are owned before SDK entry.
            try {
                final var config = source.recoveryReader();
                owner.reader = KafkaClientArtifactRecoverySourceCursor.openForMigration(original, owner,
                        new java.util.HashMap<>(source.operational()), source.sdkJar(), credential, cut,
                        config.pollTimeout(), Math.toIntExact(original.graph().config().maximumSourceFrameBytes()), time,
                        config.allocationId(), config.maximumConnections(), config.cost());
                original.retainRecoverySource(owner.reader); original.requireCurrent();
                owner.reader.requireCurrent(); owner.reader.requireLocalCurrent();
                reader = owner.reader;
            } catch (RuntimeException | Error failure) {
                this.failure = failure; owner.failure = failure;
                if (failure instanceof CheckpointInputFuture.Cleanup partial) owner.partial = partial;
                throw failure;
            } finally { owner.openingExited = true; }
        }
    }

    /** Pre-registered ownership of the no-ACK consumer, including failed construction and same-handle cleanup. */
    private static final class ReaderOwner implements TargetOwnedBackendGroup {
        private final TargetMigrationSourceOpening original;
        private final TargetWorkerProcessRuntime.Assembly graph;
        private final Object construction = new Object();
        private final CompletableFuture<Void> terminated = new CompletableFuture<>();
        private volatile KafkaClientArtifactRecoverySourceCursor reader;
        private volatile CheckpointInputFuture.Cleanup partial;
        private volatile Throwable failure;
        private volatile boolean openingExited, stopping;
        private OwnedCheckpointIo<Boolean> cleanup;
        private List<Object> registrations;
        private ReaderOwner(TargetMigrationSourceOpening original) { this.original = original; this.graph = original.graph(); }
        @Override public void requireWorkerGraph(WorkClassExecutionRegistry classes, SharedRocksDbResources resources) {
            if (graph.classes() != classes || graph.resources() != resources) {
                throw new IllegalArgumentException("migration reader changes its original process graph");
            }
        }
        @Override public synchronized List<Object> freezeRegistrations() {
            stopping = true;
            if (registrations == null) registrations = reader == null ? List.of(construction) : List.of(construction, reader);
            return registrations;
        }
        @Override public synchronized CompletableFuture<Void> closeBeforeHostBinding() {
            freezeRegistrations();
            if (!openingExited || cleanup != null && !cleanup.ioExited()) return terminated.copy();
            if (cleanup != null && cleanup.completedValue().orElse(false)) { terminated.complete(null); return terminated.copy(); }
            try {
                cleanup = OwnedCheckpointIo.submit(ignored -> {
                    if (reader != null) {
                        reader.close();
                        if (!reader.cleanupComplete()) throw new IllegalStateException("migration reader native cleanup remains live");
                    }
                    if (partial != null) {
                        final var attempt = partial.retryCleanup();
                        // K1 partial cleanup is synchronous; a foreign asynchronous completion is never guessed closed.
                        if (!attempt.isDone() || attempt.isCompletedExceptionally() || !partial.cleanupComplete()) {
                            throw new IllegalStateException("migration reader partial cleanup remains owned");
                        }
                    }
                    return true;
                }, () -> {}, graph.io());
                cleanup.receipt().whenComplete((done, rejected) -> {
                    if (rejected != null) failure = rejected;
                    else if (Boolean.TRUE.equals(done)) terminated.complete(null);
                });
            } catch (RuntimeException | Error rejected) { failure = rejected; }
            return terminated.copy();
        }
        @Override public CompletableFuture<Void> closeOwnedBackend(TargetWorkerHostRuntime.BackendClosePermit permit) {
            permit.requireOriginalGroup(this, registrations); return closeBeforeHostBinding();
        }
        @Override public synchronized boolean resourcesTerminated() {
            if (openingExited && (reader != null && reader.cleanupComplete() || stopping && cleanup != null
                    && cleanup.ioExited() && cleanup.completedValue().orElse(false))) {
                terminated.complete(null);
            }
            return terminated.isDone();
        }
        @Override public boolean nativeResourcesTerminated() {
            return openingExited && (reader == null || reader.cleanupComplete()) && (partial == null || partial.cleanupComplete()) && stopping;
        }
        @Override public Throwable cleanupFailure() { return failure; }
        @Override public boolean applicationReady() { return false; }
        @Override public boolean workerReady(TargetWorkerShardRuntime worker) { return false; }
        @Override public void requireHostComposition(TargetWorkerHostRuntime host, List<TargetWorkerShardRuntime> fleet) {
            throw new IllegalStateException("migration reader must exit under its original maintenance owner");
        }
        @Override public void bindHost(TargetWorkerHostRuntime host, List<TargetWorkerShardRuntime> fleet) { requireHostComposition(host, fleet); }
        @Override public void admitWorker(TargetWorkerHostRuntime host, TargetWorkerShardRuntime worker) {
            throw new IllegalStateException("migration reader cannot become an ordinary Source worker");
        }
    }
}
