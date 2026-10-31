package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.*;
import com.nereusstream.delay.protocol.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/** Actual guarded P1 maintenance SDK, independent of ordinary Source activation and Target Root creation. */
public final class PulsarClientArtifactTargetMigrationFactory {
    private PulsarClientArtifactTargetMigrationFactory() {}

    public static Opening openConfigured(TargetMigrationProcessDriver.Opening original, Path sourceConfiguration,
            Path writerConfiguration, LocalBrokerMtlsResolver resolver, SourceTransportCredentialAuthorization.Trust trust,
            Supplier<TrustedUtcIntervalEvidence> time) throws IOException {
        return openConfigured(original, PulsarClientArtifactTargetWorkerSourceFactory.loadSourceDeployment(sourceConfiguration),
                PulsarClientArtifactTargetWorkerSourceFactory.loadSystemWriterConfiguration(writerConfiguration), resolver, trust, time);
    }

    /** Shared client plus its one producer; both independent readers remain on that paid client envelope. */
    public static final int MAXIMUM_SDK_ALLOCATIONS = 2;

    /** Same explicit cost check is usable by the configured service preflight before any native open. */
    public static CapacityVector migrationSdkCost(
            PulsarClientArtifactTargetWorkerSourceFactory.SourceDeployment source,
            PulsarClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration writer, long maximumFrameBytes) {
        if (maximumFrameBytes < 1 || maximumFrameBytes >= Integer.MAX_VALUE || source.source().settings().maximumUsers() < 3
                || source.source().settings().allocationId().equals(writer.allocationId())) {
            throw new IllegalArgumentException("P1 migration requires two bounded original readers and its one producer");
        }
        final var settings = source.source().settings();
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_THREADS.wireValue() - 1] = Math.addExact(
                Math.multiplyExact(3L, settings.ioThreads()), settings.listenerThreads() + 4L);
        minimum[CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = settings.maximumConnections();
        minimum[CapacityDimension.OPEN_FILES.wireValue() - 1] = settings.maximumConnections();
        // Each reader has receiverQueueSize=1 and at most one delivered/look-ahead Source frame.
        // The original migration retained-byte reservation separately pays decoded material and the import batch.
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 4;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = Math.addExact(settings.memoryBytes(),
                Math.multiplyExact(4L, maximumFrameBytes));
        minimum[CapacityDimension.OTHER_NATIVE_BYTES.wireValue() - 1] = 1;
        if (!source.cost().covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException("P1 migration Source cost omits its two original finite reader buffers/client envelope");
        }
        return source.cost().add(writer.cost());
    }

    /** File reads and real SDK construction run only in the original root's owned preparation I/O. */
    public static Opening openConfigured(TargetMigrationProcessDriver.Opening original,
            PulsarClientArtifactTargetWorkerSourceFactory.SourceDeployment source,
            PulsarClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration writer,
            LocalBrokerMtlsResolver resolver, SourceTransportCredentialAuthorization.Trust trust,
            Supplier<TrustedUtcIntervalEvidence> time) {
        Objects.requireNonNull(original).requireCurrent(); Objects.requireNonNull(source); Objects.requireNonNull(writer);
        migrationSdkCost(source, writer, original.graph().config().maximumSourceFrameBytes());
        final var config = source.source();
        final var readerReceipt = Objects.requireNonNull(resolver).resolveSource(config.authorizationFile(), config.signedRoute(),
                Objects.requireNonNull(trust), Objects.requireNonNull(time));
        original.requireCurrent();
        final var writerReceipt = resolver.resolveSource(writer.authorizationFile(), writer.signedRoute(), trust, time);
        original.requireCurrent();
        readerReceipt.authorization().requireAssignment(original.assignment());
        writerReceipt.authorization().requireAssignment(original.assignment());
        if (!config.topic().equals(readerReceipt.authorization().physicalTopic())) {
            throw new IllegalArgumentException("configured migration reader changes its original P1 Source topic");
        }
        if (!readerReceipt.sameMaterial(writerReceipt)) {
            throw new IllegalArgumentException("P1 migration CONTROL and reader require the same original client mTLS material");
        }
        final var lease = original.ownerLease();
        final var identity = new OwnerIdentity(config.deploymentId(), config.workerRunId(), lease.ownerEpoch(), lease.leaseToken());
        original.bindSourceIdentity(readerReceipt, identity, time.get());
        if (!Bytes.constantTimeEquals(writerReceipt.authorization().deploymentId(), identity.deploymentId())) {
            throw new IllegalArgumentException("migration CONTROL transport changes the original maintenance deployment");
        }
        final var graph = original.graph();
        final var group = PulsarClientArtifactOwnedBackendGroup.openVerifiedSourceMtls(config.settings(), config.sdkJar(),
                readerReceipt, time, original::requireCurrent, original.sdkBudget(), source.cost(), graph.classes(), graph.resources(),
                graph.io(), original::retainBackend);
        return new Opening(original, config, writer, group, readerReceipt, writerReceipt, time);
    }

    public static final class Opening implements TargetMigrationTransport {
        private final TargetMigrationProcessDriver.Opening original;
        private final PulsarClientArtifactTargetWorkerSourceFactory.SourceConfiguration source;
        private final PulsarClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration writerConfiguration;
        private final PulsarClientArtifactOwnedBackendGroup group;
        private final LocalBrokerMtlsResolver.SourceReceipt credential, writerCredential;
        private final Supplier<TrustedUtcIntervalEvidence> time;
        private final List<PulsarClientArtifactRecoverySourceCursor> readers = new ArrayList<>();
        private PulsarClientArtifactRecoverySourceCursor reader, writerReader;
        private PulsarClientArtifactTargetMigrationControlWriter writer;
        private SourcePosition pendingCut;
        private boolean stopping, operationActive;
        private Throwable failure;
        private Opening(TargetMigrationProcessDriver.Opening original,
                PulsarClientArtifactTargetWorkerSourceFactory.SourceConfiguration source,
                PulsarClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration writerConfiguration,
                PulsarClientArtifactOwnedBackendGroup group, LocalBrokerMtlsResolver.SourceReceipt credential,
                LocalBrokerMtlsResolver.SourceReceipt writerCredential, Supplier<TrustedUtcIntervalEvidence> time) {
            this.original = original; this.source = source; this.writerConfiguration = writerConfiguration; this.group = group;
            this.credential = credential; this.writerCredential = writerCredential; this.time = time; pendingCut = original.baseline();
        }
        @Override public void requireBinding(TargetMigrationProcessDriver.Opening expected) {
            if (original != expected) throw new IllegalArgumentException("migration P1 transport changes its original owner");
        }
        @Override public boolean advance() {
            original.requireCurrent();
            if (stopping) throw new IllegalStateException("migration transport is stopping");
            operationActive = true;
            try {
                if (!group.finishStartup()) return false;
                if (reader == null || pendingCut != null) openPendingReader();
                if (writer == null) {
                    writerReader = reader;
                    writer = PulsarClientArtifactTargetMigrationControlWriter.open(writerConfiguration.allocationId(), original,
                            group, writerConfiguration.timeout(), time, original.sdkBudget(), writerConfiguration.cost(),
                            writerCredential, writerReader);
                }
                original.requireCurrent(); return true;
            } catch (RuntimeException | Error rejected) { failure = rejected; throw rejected; }
            finally { operationActive = false; }
        }
        private void openPendingReader() {
            if (!(pendingCut instanceof PulsarSourcePosition cut)) {
                throw new IllegalArgumentException("P1 migration requires its complete original Source cut");
            }
            readers.removeIf(candidate -> candidate != writerReader && candidate.cleanupComplete());
            if (readers.size() >= 2) throw new IllegalStateException("original migration reader cleanup window is full");
            final var actual = PulsarClientArtifactRecoverySourceCursor.openForMigration(original, group, source.guard(), credential,
                    cut, source.receiveTimeout(), Math.toIntExact(original.graph().config().maximumSourceFrameBytes()), time);
            readers.add(actual); original.retainRecoverySource(actual); reader = actual; pendingCut = null;
        }
        @Override public TargetWorkerRecoverySource reader() { original.requireCurrent(); return reader; }
        @Override public TargetMigrationControlWriter writer() { original.requireCurrent(); return writer; }
        @Override public void openReaderAfter(SourcePosition actualApplied) {
            original.requireCurrent(); Objects.requireNonNull(actualApplied);
            if (reader != null && reader != writerReader && !reader.cleanupComplete()) reader.close();
            if (stopping || pendingCut != null || reader != null && reader != writerReader && !reader.cleanupComplete()) {
                throw new IllegalStateException("prior finite migration reader remains owned or opening");
            }
            // The original writer reader remains open solely for its exact SUBSCRIBE proof; it is never ACKed.
            pendingCut = actualApplied;
        }
        @Override public void requestStop() {
            stopping = true;
            if (writer != null) writer.requestStop();
            Throwable rejected = null;
            for (var candidate : readers) {
                try { candidate.close(); }
                catch (RuntimeException | Error closeFailure) {
                    if (rejected == null) rejected = closeFailure;
                    else if (rejected != closeFailure) rejected.addSuppressed(closeFailure);
                }
            }
            group.closeBeforeHostBinding();
            if (rejected != null) {
                failure = rejected;
                if (rejected instanceof Error fatal) throw fatal;
                throw (RuntimeException) rejected;
            }
        }
        @Override public boolean ioExited() { return !operationActive && (writer == null || writer.operationsExited()); }
        @Override public boolean cleanupComplete() {
            return stopping && ioExited() && (writer == null || writer.cleanupComplete())
                    && readers.stream().allMatch(PulsarClientArtifactRecoverySourceCursor::cleanupComplete) && group.resourcesTerminated();
        }
        @Override public Throwable pendingIoFailure() { return failure == null ? group.cleanupFailure() : failure; }
    }
}
