package com.nereusstream.delay.ownership;

import com.nereusstream.delay.runtime.TargetMigrationArchivePreparation;
import com.nereusstream.delay.runtime.TargetQuotaCapacitySnapshot;
import com.nereusstream.delay.runtime.TargetRetirementPreparation;
import com.nereusstream.delay.runtime.TargetScopeFeeAuthority;
import com.nereusstream.delay.runtime.TargetSloWriterOrigin;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;

/** One protected content snapshot for the existing Source look-ahead, on the root's original bounded I/O pool. */
public final class TargetSourceAuthorityPreparation {
    /** No retained partial input; retry on the same Source turn after the failed owned I/O has actually exited. */
    public static final class InputUnavailable extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public InputUnavailable(String message) {
            super(message);
        }

        public InputUnavailable(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public enum Family {
        COMMAND,
        GRANT,
        FENCE,
        EXPIRY,
        CLOSE,
        MEMBERSHIP,
        NATIVE_POLICY,
        ADMISSION,
        OUTCOME,
        CHANNEL,
        RETIREMENT,
        ACTIVATION,
        RECLAIM,
        SLO_MODE,
        SLO_EVENT,
        SLO_WRITER,
        PARENT_CONTROL_CREDIT,
        SCOPE_OBLIGATION,
        SCOPE_FEES,
        MIGRATION,
        PAYLOAD_SUMMARY
    }

    /** Trusted deployment content/history providers; metadata declares parser coverage, never authorization. */
    public interface Loader {
        Set<Family> families();

        Held load(SourceReplayEntry entry, Family family, Runnable guard);

        default Held load(
                SourceReplayEntry entry,
                Family family,
                Runnable guard,
                TargetSourceApplyRuntime.ConfirmationRead source) {
            return load(entry, family, guard);
        }

        default Held load(
                SourceReplayEntry entry,
                Family family,
                Runnable guard,
                TargetSourceApplyRuntime.ConfirmationRead source,
                TargetSourceApplyRuntime.ReclaimRead reclaim) {
            return load(entry, family, guard, source);
        }

        default Held loadWriterPrelude(
                SourceReplayEntry entry,
                TargetSloWriterOrigin.PreparationRequired request,
                Runnable guard,
                TargetSourceApplyRuntime.ConfirmationRead source,
                TargetSourceApplyRuntime.ReclaimRead reclaim) {
            throw new InputUnavailable("original fee-only writer preparation capability unavailable");
        }
    }

    /** Immutable original author/key/role/scope/history data, already acquired outside Store locks. */
    public interface Held extends AutoCloseable {
        SourceReplayEntry entry();

        Object control();

        long retainedBytes();

        default boolean preparationReady() {
            return true;
        }
        /** One actual bounded cursor turn on the original owned I/O; no control is consumable before it returns true. */
        default boolean advancePreparation() {
            return true;
        }

        default void invalidateRetirement(TargetRetirementPreparation.ViewChanged changed) {
            throw changed;
        }

        default TargetSloWriterOrigin.Protection detachWriterOrigin(TargetSloWriterOrigin.PreparationRequired request) {
            throw new InputUnavailable("original paid writer history/retention subloan unavailable");
        }

        default TargetSourceAuthorityLoader.ArchiveTailInputs archiveTailInputs(
                TargetMigrationArchivePreparation.TailPreparationRequired request) {
            throw new InputUnavailable("original archive receiver preparation inputs unavailable");
        }

        default TargetScopeFeeAuthority scopeFees() {
            return TargetScopeFeeAuthority.UNAVAILABLE;
        }
        /** Keeps this fee-only read hold alive through the original physical Source commit. */
        default TargetStoreBackend.CommitAuthority protectScopeFees(TargetStoreBackend.CommitAuthority physical) {
            throw new InputUnavailable("original Scope fee hold has no Source commit protection");
        }
        /** Rechecks only held local versions/pins; verifiers may not perform remote I/O through this snapshot. */
        void requireCurrent();
        /** Releases this operation's read protection, not the authoritative active grant/physical reservation. */
        @Override
        void close();
    }

    private final Loader loader;
    private final ThreadPoolExecutor executor;
    private final LongSupplier nanos;
    private final long timeoutNanos, maximumBytes;
    private SourceReplayEntry entry;
    private Family family;
    private OwnedCheckpointIo<Held> opening;
    private volatile OwnedCheckpointIo<Boolean> advancing;
    private OwnedCheckpointIo<Boolean> envelopeOpening;
    private OwnedCheckpointIo<Boolean> serviceEnvelopeOpening;
    private TargetStoreBackend.SourceOperationEnvelopeRequired envelopeRequest;
    private SourceReplayEntry envelopeEntry;
    private WorkClassTask queuedPreparation;
    private WorkClassExecutionRegistry preparationClasses;
    private long preparationTurns;
    private OwnedCheckpointIo<Boolean> closing;
    private Held held;
    private boolean prepared;
    private boolean writerPreludeMode, restartingAfterPrelude;
    private volatile TargetSloWriterOrigin.PreparationRequired writerRequest;
    private volatile TargetSloWriterOrigin.Preparation writerCursor;
    private volatile TargetSloWriterOrigin.Protection writerProtection;
    private volatile boolean writerComplete;
    private volatile TargetSloWriterOrigin.PreparationRequired preparedWriter;
    private volatile TargetMigrationArchivePreparation.TailPreparationRequired tailRequest, preparedTailRequest;
    private volatile TargetMigrationArchivePreparation.PreparedTail archiveTail;
    private volatile OwnedCheckpointIo<com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover>
            archiveTakeoverRead;
    private Runnable localOwnerAndSource;
    private TargetSourceApplyRuntime originalSource;
    private TargetWorkerProcessRuntime.SourcePlanInputs sourcePlanInputs;
    private LongSupplier originalSourceClock;
    private long started;
    private boolean stopping;
    private TargetSourceApplyRuntime.Authorities authorities;
    private final Set<TargetSourceApplyRuntime.NativeExposureHold> nativeReadLoans =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

    public TargetSourceAuthorityPreparation(
            Loader loader, ThreadPoolExecutor executor, LongSupplier nanos, Duration timeout, long maximumBytes) {
        this.loader = Objects.requireNonNull(loader, "deployment content/history providers");
        if (!Objects.requireNonNull(loader.families()).equals(EnumSet.allOf(Family.class))) {
            throw new IllegalArgumentException("Target service requires every Source content family");
        }
        this.executor = Objects.requireNonNull(executor);
        this.nanos = Objects.requireNonNull(nanos);
        timeoutNanos = Objects.requireNonNull(timeout).toNanos();
        this.maximumBytes = maximumBytes;
        if (timeoutNanos <= 0
                || timeoutNanos == Long.MAX_VALUE
                || maximumBytes <= 0
                || maximumBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Source authority preparation needs finite time/bytes");
        }
        if (executor.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) executor.getQueue().size() + executor.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || executor.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException(
                    "Source authority executor requires finite threads/queue and AbortPolicy");
        }
    }

    /** The root supplies the actual lease/successor and Store guards; content I/O is resolved only by prepare. */
    public synchronized TargetSourceApplyRuntime.Authorities authorities(
            OxiaOwnerLeaseStore leases,
            SourceReplaySuccessor successor,
            TargetStoreBackend.CommitAuthority duplicateWrites,
            TargetStoreBackend.ReadAuthority reads) {
        if (authorities != null) {
            throw new IllegalStateException("Source authority graph is already constructed");
        }
        authorities = new TargetSourceApplyRuntime.Authorities(
                leases,
                successor,
                value -> require(value, TargetSourceApplyRuntime.GrantControl.class),
                value -> require(value, TargetSourceApplyRuntime.FenceControl.class),
                value -> require(value, TargetSourceApplyRuntime.ExpiryControl.class),
                value -> require(value, TargetSourceApplyRuntime.CloseControl.class),
                value -> require(value, TargetSourceApplyRuntime.MembershipControl.class),
                duplicateWrites,
                reads,
                value -> require(value, TargetSourceApplyRuntime.CommandControl.class),
                value -> require(value, TargetSourceApplyRuntime.NativePolicyControl.class),
                value -> require(value, TargetSourceApplyRuntime.AdmissionControl.class),
                value -> require(value, TargetSourceApplyRuntime.OutcomeControl.class),
                value -> require(value, TargetSourceApplyRuntime.ChannelControl.class),
                value -> require(value, TargetSourceApplyRuntime.RetirementControl.class),
                value -> require(value, TargetSourceApplyRuntime.ProtocolActivationControl.class),
                value -> require(value, TargetSourceApplyRuntime.ReclaimControl.class),
                value -> require(value, TargetSourceApplyRuntime.SloObservationControl.class),
                value -> require(value, TargetSourceApplyRuntime.SloEventControl.class),
                value -> require(value, TargetSourceApplyRuntime.SloWriterControl.class),
                value -> require(value, TargetSourceApplyRuntime.ParentCreditControl.class),
                value -> require(value, TargetSourceApplyRuntime.ScopeObligationControl.class),
                this::requireScopeFees,
                value -> require(value, TargetSourceApplyRuntime.MigrationControl.class));
        return authorities;
    }

    synchronized void bind(TargetSourceApplyRuntime.Authorities actual, Runnable originalLocalOwnerAndSource) {
        bind(actual, originalLocalOwnerAndSource, null, null);
    }

    synchronized void bind(
            TargetSourceApplyRuntime.Authorities actual,
            Runnable originalLocalOwnerAndSource,
            TargetSourceApplyRuntime source,
            LongSupplier clock) {
        if (authorities != actual || localOwnerAndSource != null) {
            throw new IllegalStateException("Source preparation is not this runtime's original authority graph");
        }
        localOwnerAndSource = Objects.requireNonNull(originalLocalOwnerAndSource);
        originalSource = source;
        originalSourceClock = clock;
    }

    synchronized void bindSourcePlanInputs(TargetWorkerProcessRuntime.SourcePlanInputs actual) {
        if (sourcePlanInputs != null && sourcePlanInputs != actual) {
            throw new IllegalStateException("Source preparation changed its original plan reservation");
        }
        sourcePlanInputs = Objects.requireNonNull(actual);
    }

    synchronized TargetWorkerProcessRuntime.SourcePlanInputs sourcePlanInputs() {
        return sourcePlanInputs;
    }

    synchronized void requireOriginalEntry(SourceReplayEntry expected) {
        if (stopping || entry == null || !SourceApplyCoordinator.sameEntry(entry, expected)) {
            throw new IllegalStateException("original Source input is no longer selected");
        }
    }

    synchronized boolean prepareOperationEnvelope(
            SourceReplayEntry expected, TargetStoreBackend.SourceOperationEnvelopeRequired requested) {
        if (stopping) {
            throw new IllegalStateException("original Source preparation is stopped");
        }
        if (entry != null && !SourceApplyCoordinator.sameEntry(entry, expected)
                || envelopeEntry != null && !SourceApplyCoordinator.sameEntry(envelopeEntry, expected)
                || envelopeRequest != null && envelopeRequest != requested) {
            throw new IllegalStateException("Source service continuation changed its selected event/request");
        }
        if (envelopeEntry == null) {
            envelopeEntry = expected;
            envelopeRequest = requested;
        }
        if (envelopeOpening == null) {
            try {
                envelopeOpening = OwnedCheckpointIo.submit(
                        guard -> {
                            guard.run();
                            requested.acquire();
                            guard.run();
                            return true;
                        },
                        localOwnerAndSource,
                        executor);
            } catch (RejectedExecutionException full) {
                if (executor.isShutdown()) {
                    throw full;
                }
                return false;
            }
        }
        if (!envelopeOpening.receipt().isDone() || !envelopeOpening.ioExited()) {
            return false;
        }
        try {
            envelopeOpening.receipt().join();
        } catch (RuntimeException failure) {
            throw new InputUnavailable(
                    "actual protocol family exceeds the original Source service/reservation contract", failure);
        }
        envelopeOpening = null;
        envelopeRequest = null;
        envelopeEntry = null;
        return true;
    }

    synchronized boolean prepareServiceEnvelope() {
        if (stopping) {
            throw new IllegalStateException("original Source service qualification is stopped");
        }
        if (serviceEnvelopeOpening == null) {
            try {
                serviceEnvelopeOpening = OwnedCheckpointIo.submit(
                        guard -> {
                            guard.run();
                            Objects.requireNonNull(originalSource).verifyServiceEnvelope(originalSourceClock);
                            guard.run();
                            return true;
                        },
                        localOwnerAndSource,
                        executor);
            } catch (RejectedExecutionException full) {
                if (executor.isShutdown()) {
                    throw full;
                }
                return false;
            }
        }
        if (!serviceEnvelopeOpening.receipt().isDone() || !serviceEnvelopeOpening.ioExited()) {
            return false;
        }
        serviceEnvelopeOpening.receipt().join();
        return true;
    }

    /** External consumers own these loans; registering one neither changes their lifetime nor grants Source authority. */
    synchronized void retainNativeReadLoan(
            TargetSourceApplyRuntime source, TargetSourceApplyRuntime.NativeExposureHold loan) {
        if (stopping || originalSource != source) {
            throw new InputUnavailable("Native read loan lacks its original active Source I/O owner");
        }
        nativeReadLoans.removeIf(TargetSourceApplyRuntime.NativeExposureHold::cleanupComplete);
        nativeReadLoans.add(Objects.requireNonNull(loan));
    }

    /** Cleanup remains admissible after business/Owner admission stops, on the original bounded executor. */
    OwnedCheckpointIo<Boolean> submitNativeReadCleanup(Runnable release) {
        return OwnedCheckpointIo.submit(
                ignored -> {
                    release.run();
                    return true;
                },
                () -> {},
                executor);
    }

    /** Final executor-release debt, separate from Source content I/O and its producer-close ordering. */
    synchronized boolean nativeReadLoansExited() {
        nativeReadLoans.removeIf(TargetSourceApplyRuntime.NativeExposureHold::cleanupComplete);
        return nativeReadLoans.isEmpty();
    }

    private void retryRequestedNativeReadCleanup() {
        for (var loan : nativeReadLoans) {
            loan.retryRequestedNativeReadCleanup();
        }
        nativeReadLoans.removeIf(TargetSourceApplyRuntime.NativeExposureHold::cleanupComplete);
    }

    synchronized boolean prepare(SourceReplayEntry expected, Family expectedFamily) {
        if (stopping) {
            throw new IllegalStateException("Source authority preparation stopped");
        }
        if (entry != null && entry != expected) {
            throw new IllegalStateException("Source authority look-ahead changed");
        }
        if (entry == null) {
            entry = expected;
            family = expectedFamily;
            started = nanos.getAsLong();
        }
        if (restartingAfterPrelude) {
            if (!closeHeld()) {
                return false;
            }
            opening = null;
            closing = null;
            advancing = null;
            held = null;
            prepared = false;
            writerPreludeMode = false;
            restartingAfterPrelude = false;
        }
        if (family != expectedFamily || closing != null) {
            throw new IllegalStateException("Source authority family is changed or closing");
        }
        final long admittedElapsed = nanos.getAsLong() - started;
        if (admittedElapsed < 0 || admittedElapsed >= timeoutNanos) {
            requestStop();
            throw new IllegalStateException("Source preparation exceeded its original finite deadline");
        }
        if (opening == null) {
            final long elapsed = nanos.getAsLong() - started;
            if (elapsed < 0 || elapsed >= timeoutNanos) {
                requestStop();
                throw new IllegalStateException("Source input admission exceeded its original finite deadline");
            }
            opening = OwnedCheckpointIo.submitResource(
                    guard -> {
                        final var source = originalSource == null
                                ? null
                                : originalSource.confirmationRead(expected, originalSourceClock);
                        final var reclaim = originalSource == null
                                ? null
                                : originalSource.reclaimRead(expected, originalSourceClock);
                        if (writerPreludeMode) {
                            Objects.requireNonNull(originalSource)
                                    .requireWriterPrelude(
                                            expected, Objects.requireNonNull(writerRequest), originalSourceClock);
                        }
                        final var loaded = Objects.requireNonNull(
                                writerPreludeMode
                                        ? loader.loadWriterPrelude(
                                                expected, Objects.requireNonNull(writerRequest), guard, source, reclaim)
                                        : loader.load(expected, expectedFamily, guard, source, reclaim),
                                "original content snapshot");
                        try {
                            guard.run();
                            loaded.requireCurrent();
                            if (!SourceApplyCoordinator.sameEntry(expected, loaded.entry())
                                    || loaded.retainedBytes() < 0
                                    || loaded.retainedBytes() > maximumBytes) {
                                throw new IllegalStateException(
                                        "Source content snapshot changes its frame/family or exceeds its byte bound");
                            }
                            loaded.requireCurrent();
                            guard.run();
                            return loaded;
                        } catch (RuntimeException | Error failure) {
                            release(loaded, failure);
                            throw failure;
                        }
                    },
                    localOwnerAndSource,
                    value -> release(value, null),
                    executor);
        }
        if (!opening.receipt().isDone()) {
            final long elapsed = nanos.getAsLong() - started;
            if (elapsed < 0 || elapsed >= timeoutNanos) {
                requestStop();
                throw new IllegalStateException("Source content acquisition exceeded its finite deadline");
            }
            return false;
        }
        if (opening.receipt().isCompletedExceptionally()) {
            final var requested = writerRequired(CheckpointInputFuture.ownedFailure(opening.receipt()));
            if (requested != null) {
                opening.retryCleanup();
                if (!opening.ioExited()) {
                    return false;
                }
                if (writerPreludeMode) {
                    throw new InputUnavailable(
                            "fee-only writer prelude cannot acquire independent original history/payment material");
                }
                writerRequest = requested;
                writerPreludeMode = true;
                opening = null;
                return false;
            }
            if (CheckpointInputFuture.ownedFailure(opening.receipt()) instanceof InputUnavailable
                    && opening.ioExited()) {
                opening = null;
                return false;
            }
            opening.receipt().join();
        }
        if (!opening.ioExited()) {
            return false;
        }
        if (held == null) {
            held = opening.completedValue().orElseGet(() -> opening.receipt().join());
        }
        held.requireCurrent();
        localOwnerAndSource.run();
        if (!prepared && advancing == null && writerRequest == null && tailRequest == null && held.preparationReady()) {
            prepared = true;
        }
        if (!prepared) {
            if (queuedPreparation != null) {
                final var state = preparationClasses.state(queuedPreparation);
                if (state.isPresent()) {
                    if (state.orElseThrow() == WorkClassExecutionRegistry.ExecutionState.FAILED) {
                        throw new IllegalStateException("original Source preparation WorkClass action failed");
                    }
                    return false;
                }
                queuedPreparation = null;
            }
            if (advancing == null) {
                try {
                    final var original = held;
                    final long retained = Math.addExact(
                            original.retainedBytes(), tailRequest == null ? 0 : tailRequest.retainedBytes());
                    if (retained > maximumBytes) {
                        throw new InputUnavailable(
                                "prospective archive Source batch exceeds its actual retained input reservation");
                    }
                    preparationClasses = Objects.requireNonNull(originalSource, "original Source preparation scheduler")
                            .preparationClasses();
                    final var task = new WorkClassTask(
                            WorkClass.SOURCE_APPLY,
                            "source-input/"
                                    + com.nereusstream.delay.protocol.Bytes.hex(
                                            com.nereusstream.delay.protocol.Bytes.sha256(
                                                    expected.position().canonicalBytes()))
                                    + '/'
                                    + Math.incrementExact(preparationTurns),
                            originalSource.preparationTaskBytes(expected, retained));
                    preparationTurns = Math.incrementExact(preparationTurns);
                    preparationClasses.submit(task, () -> {
                        try {
                            advancing = OwnedCheckpointIo.submit(
                                    guard -> {
                                        guard.run();
                                        original.requireCurrent();
                                        final var requestedTail = tailRequest;
                                        if (requestedTail != null) {
                                            final var actual =
                                                    Objects.requireNonNull(original.archiveTailInputs(requestedTail));
                                            final var material = requestedTail.material(actual.opening(), expected);
                                            guard.run();
                                            original.requireCurrent();
                                            final var received =
                                                    actual.receiver().receive(material);
                                            archiveTail = requestedTail.received(material, received);
                                            archiveTail.requireCurrent();
                                            preparedTailRequest = requestedTail;
                                            tailRequest = null;
                                        }
                                        final var requestedWriter = writerRequest;
                                        if (requestedWriter != null) {
                                            if (writerCursor == null) {
                                                writerProtection = Objects.requireNonNull(
                                                        original.detachWriterOrigin(requestedWriter),
                                                        "independent original writer subloan");
                                                writerCursor = Objects.requireNonNull(
                                                                originalSource,
                                                                "original Source for writer preparation")
                                                        .beginWriterPreparation(
                                                                expected,
                                                                requestedWriter,
                                                                writerProtection,
                                                                originalSourceClock);
                                                writerComplete = false;
                                            }
                                            if (!writerComplete) {
                                                writerComplete = writerCursor.advance();
                                                return false;
                                            }
                                            writerCursor.install();
                                            // The actual kernel owns the independent loan after install, beyond this
                                            // entry's lifetime.
                                            writerCursor = null;
                                            writerProtection = null;
                                            writerComplete = false;
                                            preparedWriter = requestedWriter;
                                            writerRequest = null;
                                        }
                                        final boolean ready = original.advancePreparation();
                                        if (original.retainedBytes() < 0 || original.retainedBytes() > maximumBytes) {
                                            throw new IllegalStateException(
                                                    "Source preparation exceeded its original retained input window");
                                        }
                                        original.requireCurrent();
                                        guard.run();
                                        return ready;
                                    },
                                    localOwnerAndSource,
                                    executor);
                        } catch (RejectedExecutionException full) {
                            if (executor.isShutdown()) {
                                throw full;
                            }
                        }
                    });
                    queuedPreparation = task;
                    return false;
                } catch (RejectedExecutionException full) {
                    if (executor.isShutdown()) {
                        throw full;
                    }
                    return false;
                }
            }
            if (!advancing.receipt().isDone() || !advancing.ioExited()) {
                final long elapsed = nanos.getAsLong() - started;
                if (elapsed < 0 || elapsed >= timeoutNanos) {
                    requestStop();
                    throw new IllegalStateException("Source preparation exceeded its original finite deadline");
                }
                return false;
            }
            if (CheckpointInputFuture.ownedFailure(advancing.receipt())
                    instanceof TargetRetirementPreparation.ViewChanged changed) {
                held.invalidateRetirement(changed);
                advancing = null;
                return false;
            }
            if (CheckpointInputFuture.ownedFailure(advancing.receipt())
                    instanceof TargetSloWriterOrigin.PreparationRequired requested) {
                writerRequest = requested;
                advancing = null;
                return false;
            }
            prepared = advancing.receipt().join();
            advancing = null;
            if (!prepared) {
                return false;
            }
        }
        if (writerPreludeMode) {
            restartingAfterPrelude = true;
            return false;
        }
        if (familyOf(held.control()) != expectedFamily) {
            throw new IllegalStateException("prepared Source control changed its original family");
        }
        if (held.control() instanceof TargetSourceApplyRuntime.GrantControl grant
                && !(grant.authority().capacity() instanceof TargetQuotaCapacitySnapshot.Verified)) {
            throw new IllegalStateException(
                    "configured Source grant requires the concrete protected capacity verifier");
        }
        return true;
    }

    private static TargetSloWriterOrigin.PreparationRequired writerRequired(Throwable failure) {
        for (var current = failure; current != null; current = current.getCause()) {
            if (current instanceof TargetSloWriterOrigin.PreparationRequired requested) {
                return requested;
            }
        }
        return null;
    }

    synchronized boolean prepareWriter(
            SourceReplayEntry expected, Family fallback, TargetSloWriterOrigin.PreparationRequired request) {
        if (entry != null && entry != expected) {
            throw new IllegalStateException("writer preparation changed its original Source entry");
        }
        if (preparedWriter != null
                && preparedWriter.origin() == request.origin()
                && java.util.Arrays.equals(
                        preparedWriter.selected().canonicalBytes(),
                        request.selected().canonicalBytes())) {
            return prepare(expected, family == null ? fallback : family);
        }
        if (writerRequest != null
                && writerRequest != request
                && !java.util.Arrays.equals(
                        writerRequest.selected().canonicalBytes(),
                        request.selected().canonicalBytes())) {
            throw new IllegalStateException("Source already retains another writer prefix request");
        }
        writerRequest = Objects.requireNonNull(request);
        prepared = false;
        return prepare(expected, family == null ? fallback : family);
    }

    synchronized void invalidateRetirement(
            SourceReplayEntry expected, TargetRetirementPreparation.ViewChanged changed) {
        if (entry != expected || held == null) {
            throw new IllegalStateException("retirement invalidation changed its original Source hold", changed);
        }
        held.invalidateRetirement(changed);
        prepared = false;
    }

    synchronized boolean prepareArchiveTail(
            SourceReplayEntry expected,
            Family fallback,
            TargetMigrationArchivePreparation.TailPreparationRequired request) {
        if (entry != null && entry != expected) {
            throw new IllegalStateException("archive tail changed its actual Source entry");
        }
        if (preparedTailRequest == request) {
            return prepare(expected, family == null ? fallback : family);
        }
        if (tailRequest != null && tailRequest != request) {
            throw new IllegalStateException("another actual prospective Source batch is retained");
        }
        tailRequest = Objects.requireNonNull(request);
        prepared = false;
        return prepare(expected, family == null ? fallback : family);
    }

    synchronized TargetMigrationArchivePreparation.PreparedTail archiveTail(SourceReplayEntry expected) {
        if (archiveTail == null) {
            return null;
        }
        if (entry != expected) {
            throw new IllegalStateException("archive tail changed its original Source entry");
        }
        if (!prepared) {
            return null;
        }
        archiveTail.requireCurrent();
        return archiveTail;
    }

    synchronized com.nereusstream.delay.runtime.TargetMigrationArchiveStore.AppliedTakeover committedArchive(
            SourceReplayEntry expected,
            com.nereusstream.delay.protocol.TargetMigrationArchiveRequest request,
            LongSupplier clock) {
        if (entry != expected || held == null || !prepared || stopping) {
            throw new IllegalStateException("archive activation lost its original committed Source hold");
        }
        held.requireCurrent();
        localOwnerAndSource.run();
        if (queuedPreparation != null) {
            final var state = preparationClasses.state(queuedPreparation);
            if (state.isPresent()) {
                if (state.orElseThrow() == WorkClassExecutionRegistry.ExecutionState.FAILED) {
                    throw new IllegalStateException("original archive activation WorkClass action failed");
                }
                return null;
            }
            queuedPreparation = null;
        }
        if (archiveTakeoverRead == null) {
            preparationClasses = Objects.requireNonNull(originalSource).preparationClasses();
            final var original = held;
            final var task = new WorkClassTask(
                    WorkClass.SOURCE_APPLY,
                    "source-archive-takeover/" + com.nereusstream.delay.protocol.Bytes.hex(request.migrationId()) + '/'
                            + Math.incrementExact(preparationTurns),
                    originalSource.preparationTaskBytes(expected, original.retainedBytes()));
            preparationTurns = Math.incrementExact(preparationTurns);
            preparationClasses.submit(task, () -> {
                try {
                    archiveTakeoverRead = OwnedCheckpointIo.submit(
                            guard -> {
                                original.requireCurrent();
                                guard.run();
                                final var actual =
                                        originalSource.readCommittedArchiveTakeover(expected, request, clock);
                                original.requireCurrent();
                                guard.run();
                                return actual;
                            },
                            localOwnerAndSource,
                            executor);
                } catch (RejectedExecutionException full) {
                    if (executor.isShutdown()) {
                        throw full;
                    }
                }
            });
            queuedPreparation = task;
            return null;
        }
        if (!archiveTakeoverRead.receipt().isDone() || !archiveTakeoverRead.ioExited()) {
            return null;
        }
        return archiveTakeoverRead.receipt().join();
    }

    /** No I/O: used by the existing verifier while preparing and committing the exact original Source frame. */
    private synchronized <T> T require(SourceReplayEntry expected, Class<T> type) {
        if (entry == null || held == null || !prepared) {
            throw new Required();
        }
        if (stopping || entry != expected || closing != null || !type.isInstance(held.control())) {
            throw new IllegalStateException("Source content does not belong to this original queued action");
        }
        held.requireCurrent();
        localOwnerAndSource.run();
        return type.cast(held.control());
    }

    synchronized TargetSourceApplyRuntime.PayloadSummaryControl payloadSummary(SourceReplayMutation expected) {
        return require(expected, TargetSourceApplyRuntime.PayloadSummaryControl.class);
    }

    /** A normal typed hold already owns its fees; an unprepared replay acquires payment material only. */
    private synchronized TargetScopeFeeAuthority requireScopeFees(SourceReplayMutation expected) {
        if (entry == null || held == null || !prepared) {
            throw new Required(Family.SCOPE_FEES);
        }
        if (stopping || entry != expected || closing != null) {
            throw new IllegalStateException("Scope fee hold does not belong to this original Source action");
        }
        held.requireCurrent();
        localOwnerAndSource.run();
        return Objects.requireNonNull(held.scopeFees());
    }

    synchronized TargetStoreBackend.CommitAuthority protectScopeFees(
            SourceReplayMutation expected, TargetStoreBackend.CommitAuthority physical) {
        requireScopeFees(expected);
        return Objects.requireNonNull(held.protectScopeFees(physical));
    }

    synchronized boolean complete(SourceReplayEntry expected) {
        if (entry == null) {
            return true;
        }
        if (entry != expected) {
            throw new IllegalStateException("Source authority completion changes its entry");
        }
        if (!closeHeld()) {
            return false;
        }
        entry = null;
        family = null;
        opening = null;
        advancing = null;
        closing = null;
        held = null;
        prepared = false;
        writerRequest = null;
        preparedWriter = null;
        writerPreludeMode = false;
        restartingAfterPrelude = false;
        tailRequest = null;
        preparedTailRequest = null;
        archiveTail = null;
        archiveTakeoverRead = null;
        return true;
    }

    /** Retry this occurrence's Scope material without renewing its original admission deadline. */
    synchronized boolean refresh(SourceReplayEntry expected, Family expectedFamily) {
        if (entry == null) {
            return true;
        }
        if (entry != expected) {
            throw new IllegalStateException("Source authority refresh changes its original entry");
        }
        if (!closeHeld()) {
            return false;
        }
        family = Objects.requireNonNull(expectedFamily);
        opening = null;
        advancing = null;
        closing = null;
        held = null;
        prepared = false;
        return true;
    }

    public synchronized void requestStop() {
        stopping = true;
        if (serviceEnvelopeOpening != null) {
            serviceEnvelopeOpening.requestStop();
        }
        if (envelopeOpening != null) {
            envelopeOpening.requestStop();
        }
        if (opening != null) {
            opening.requestStop();
        }
        if (advancing != null) {
            advancing.requestStop();
        }
        if (archiveTakeoverRead != null) {
            archiveTakeoverRead.requestStop();
        }
        closeHeld();
        retryRequestedNativeReadCleanup();
    }

    public synchronized void retryCleanup() {
        if (serviceEnvelopeOpening != null) {
            serviceEnvelopeOpening.retryCleanup();
        }
        if (envelopeOpening != null) {
            envelopeOpening.retryCleanup();
        }
        if (opening != null) {
            opening.retryCleanup();
        }
        if (advancing != null) {
            advancing.retryCleanup();
        }
        if (archiveTakeoverRead != null) {
            archiveTakeoverRead.retryCleanup();
        }
        if (closing != null) {
            closing.retryCleanup();
        }
        if (stopping) {
            closeHeld();
        }
        retryRequestedNativeReadCleanup();
    }

    public synchronized boolean ioExited() {
        if (serviceEnvelopeOpening != null && !serviceEnvelopeOpening.ioExited()) {
            return false;
        }
        if (envelopeOpening != null && !envelopeOpening.ioExited()) {
            return false;
        }
        for (var loan : nativeReadLoans) {
            if (!loan.nativeReadIoExited()) {
                return false;
            }
        }
        if (opening != null && !opening.ioExited()) {
            return false;
        }
        if (advancing != null && !advancing.ioExited()) {
            return false;
        }
        if (archiveTakeoverRead != null && !archiveTakeoverRead.ioExited()) {
            return false;
        }
        return opening == null || opening.completedValue().isEmpty() || closeConfirmed();
    }

    public synchronized Throwable pendingIoFailure() {
        final var serviceFailure = serviceEnvelopeOpening == null
                ? null
                : CheckpointInputFuture.ownedFailure(serviceEnvelopeOpening.receipt());
        if (serviceFailure != null) {
            return serviceFailure;
        }
        final var envelopeFailure =
                envelopeOpening == null ? null : CheckpointInputFuture.ownedFailure(envelopeOpening.receipt());
        if (envelopeFailure != null) {
            return envelopeFailure;
        }
        final var failure = closing == null ? null : CheckpointInputFuture.ownedFailure(closing.receipt());
        if (failure != null) {
            return failure;
        }
        final var preparationFailure =
                advancing == null ? null : CheckpointInputFuture.ownedFailure(advancing.receipt());
        if (preparationFailure != null) {
            return preparationFailure;
        }
        final var archiveFailure =
                archiveTakeoverRead == null ? null : CheckpointInputFuture.ownedFailure(archiveTakeoverRead.receipt());
        if (archiveFailure != null) {
            return archiveFailure;
        }
        final var openingFailure = opening == null ? null : CheckpointInputFuture.ownedFailure(opening.receipt());
        if (openingFailure != null) {
            return openingFailure;
        }
        for (var loan : nativeReadLoans) {
            final var nativeFailure = loan.nativeReadCleanupFailure();
            if (nativeFailure != null) {
                return nativeFailure;
            }
        }
        return null;
    }

    synchronized TargetSourceAuthorityPreparation fork() {
        final var fork = new TargetSourceAuthorityPreparation(
                loader, executor, nanos, Duration.ofNanos(timeoutNanos), maximumBytes);
        if (sourcePlanInputs != null) {
            fork.bindSourcePlanInputs(sourcePlanInputs);
        }
        return fork;
    }

    private boolean closeHeld() {
        if (queuedPreparation != null) {
            if (!preparationClasses.withdrawQueued(queuedPreparation)
                    && !preparationClasses.discardFailed(queuedPreparation)
                    && preparationClasses.state(queuedPreparation).isPresent()) {
                return false;
            }
            queuedPreparation = null;
        }
        if (advancing != null && !advancing.ioExited()) {
            return false;
        }
        if (archiveTakeoverRead != null && !archiveTakeoverRead.ioExited()) {
            return false;
        }
        final var cursor = writerCursor;
        if (cursor != null) {
            cursor.requestStop();
            if (!cursor.exited()) {
                return false;
            }
            writerCursor = null;
        }
        final var protection = writerProtection;
        if (protection != null) {
            protection.requestRelease();
            if (!protection.released()) {
                return false;
            }
            writerProtection = null;
        }
        if (opening == null || !opening.ioExited()) {
            return opening == null;
        }
        final var original = opening.completedValue().orElse(null);
        if (original == null) {
            return true;
        }
        if (closing == null) {
            try {
                closing = OwnedCheckpointIo.submit(
                        ignored -> {
                            release(original, null);
                            return true;
                        },
                        () -> {},
                        executor);
            } catch (RejectedExecutionException full) {
                return false;
            }
        }
        return closeConfirmed();
    }

    private boolean closeConfirmed() {
        if (closing == null || !closing.ioExited()) {
            return false;
        }
        if (closing.completedValue().orElse(false)) {
            return true;
        }
        final var failure = CheckpointInputFuture.ownedFailure(closing.receipt());
        return failure instanceof CleanupFailure cleanup && cleanup.cleanupComplete();
    }

    private static void release(Held original, Throwable cause) {
        try {
            original.close();
        } catch (RuntimeException | Error failure) {
            final var retained = new CleanupFailure(original, cause == null ? failure : cause);
            if (cause != null && failure != cause) {
                retained.addSuppressed(failure);
            }
            throw retained;
        }
    }

    /** Selects the original bounded content family; replay payment never reloads current business authorization. */
    static final class Required extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final Family family;

        Required() {
            this(null);
        }

        Required(Family family) {
            this(family, null);
        }

        Required(Family family, Throwable cause) {
            super(cause);
            this.family = family;
        }

        Family family() {
            return family;
        }
    }

    private static Family familyOf(Object control) {
        if (control instanceof TargetSourceApplyRuntime.CommandControl) {
            return Family.COMMAND;
        }
        if (control instanceof TargetSourceApplyRuntime.GrantControl) {
            return Family.GRANT;
        }
        if (control instanceof TargetSourceApplyRuntime.FenceControl) {
            return Family.FENCE;
        }
        if (control instanceof TargetSourceApplyRuntime.ExpiryControl) {
            return Family.EXPIRY;
        }
        if (control instanceof TargetSourceApplyRuntime.CloseControl) {
            return Family.CLOSE;
        }
        if (control instanceof TargetSourceApplyRuntime.MembershipControl) {
            return Family.MEMBERSHIP;
        }
        if (control instanceof TargetSourceApplyRuntime.NativePolicyControl) {
            return Family.NATIVE_POLICY;
        }
        if (control instanceof TargetSourceApplyRuntime.AdmissionControl) {
            return Family.ADMISSION;
        }
        if (control instanceof TargetSourceApplyRuntime.OutcomeControl) {
            return Family.OUTCOME;
        }
        if (control instanceof TargetSourceApplyRuntime.ChannelControl) {
            return Family.CHANNEL;
        }
        if (control instanceof TargetSourceApplyRuntime.RetirementControl) {
            return Family.RETIREMENT;
        }
        if (control instanceof TargetSourceApplyRuntime.ReclaimControl) {
            return Family.RECLAIM;
        }
        if (control instanceof TargetSourceApplyRuntime.ProtocolActivationControl) {
            return Family.ACTIVATION;
        }
        if (control instanceof TargetSourceApplyRuntime.SloObservationControl) {
            return Family.SLO_MODE;
        }
        if (control instanceof TargetSourceApplyRuntime.SloEventControl) {
            return Family.SLO_EVENT;
        }
        if (control instanceof TargetSourceApplyRuntime.SloWriterControl) {
            return Family.SLO_WRITER;
        }
        if (control instanceof TargetSourceApplyRuntime.ParentCreditControl) {
            return Family.PARENT_CONTROL_CREDIT;
        }
        if (control instanceof TargetSourceApplyRuntime.ScopeObligationControl) {
            return Family.SCOPE_OBLIGATION;
        }
        if (control instanceof TargetSourceApplyRuntime.ScopeFeeControl) {
            return Family.SCOPE_FEES;
        }
        if (control instanceof TargetSourceApplyRuntime.MigrationControl) {
            return Family.MIGRATION;
        }
        if (control instanceof TargetSourceApplyRuntime.PayloadSummaryControl) {
            return Family.PAYLOAD_SUMMARY;
        }
        throw new IllegalArgumentException("unsupported Source content snapshot");
    }

    public static final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held original;
        private boolean complete;

        CleanupFailure(Held original, Throwable failure) {
            super("Source content cleanup is not confirmed", failure);
            this.original = original;
        }

        @Override
        public synchronized boolean cleanupComplete() {
            return complete;
        }

        @Override
        public synchronized CompletableFuture<Void> retryCleanup() {
            if (complete) {
                return CompletableFuture.completedFuture(null);
            }
            try {
                original.close();
                complete = true;
                return CompletableFuture.completedFuture(null);
            } catch (RuntimeException | Error failure) {
                return CompletableFuture.failedFuture(failure);
            }
        }
    }
}
