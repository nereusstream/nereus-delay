package com.nereusstream.delay.ownership;

import com.nereusstream.delay.runtime.TargetQuotaCapacitySnapshot;
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
        private static final long serialVersionUID=1L;
        public InputUnavailable(String message){super(message);}
    }
    public enum Family { COMMAND, GRANT, FENCE, EXPIRY, CLOSE, MEMBERSHIP, NATIVE_POLICY, ADMISSION, OUTCOME, CHANNEL, RETIREMENT, ACTIVATION }

    /** Trusted deployment content/history providers; metadata declares parser coverage, never authorization. */
    public interface Loader {
        Set<Family> families();
        Held load(SourceReplayEntry entry, Family family, Runnable guard);
    }

    /** Immutable original author/key/role/scope/history data, already acquired outside Store locks. */
    public interface Held extends AutoCloseable {
        SourceReplayEntry entry();
        Object control();
        long retainedBytes();
        /** Rechecks only held local versions/pins; verifiers may not perform remote I/O through this snapshot. */
        void requireCurrent();
        /** Releases this operation's read protection, not the authoritative active grant/physical reservation. */
        @Override void close();
    }

    private final Loader loader;
    private final ThreadPoolExecutor executor;
    private final LongSupplier nanos;
    private final long timeoutNanos, maximumBytes;
    private SourceReplayEntry entry;
    private Family family;
    private OwnedCheckpointIo<Held> opening;
    private OwnedCheckpointIo<Boolean> closing;
    private Held held;
    private Runnable localOwnerAndSource;
    private long started;
    private boolean stopping;
    private TargetSourceApplyRuntime.Authorities authorities;

    public TargetSourceAuthorityPreparation(Loader loader, ThreadPoolExecutor executor, LongSupplier nanos,
            Duration timeout, long maximumBytes) {
        this.loader = Objects.requireNonNull(loader, "deployment content/history providers");
        if (!Objects.requireNonNull(loader.families()).equals(EnumSet.allOf(Family.class))) {
            throw new IllegalArgumentException("Target service requires every Source content family");
        }
        this.executor = Objects.requireNonNull(executor); this.nanos = Objects.requireNonNull(nanos);
        timeoutNanos = Objects.requireNonNull(timeout).toNanos(); this.maximumBytes = maximumBytes;
        if (timeoutNanos <= 0 || timeoutNanos == Long.MAX_VALUE || maximumBytes <= 0 || maximumBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Source authority preparation needs finite time/bytes");
        }
        if (executor.getMaximumPoolSize() == Integer.MAX_VALUE
                || (long) executor.getQueue().size() + executor.getQueue().remainingCapacity() >= Integer.MAX_VALUE
                || executor.getRejectedExecutionHandler().getClass() != ThreadPoolExecutor.AbortPolicy.class) {
            throw new IllegalArgumentException("Source authority executor requires finite threads/queue and AbortPolicy");
        }
    }

    /** The root supplies the actual lease/successor and Store guards; content I/O is resolved only by prepare. */
    public synchronized TargetSourceApplyRuntime.Authorities authorities(OxiaOwnerLeaseStore leases,
            SourceReplaySuccessor successor, TargetStoreBackend.CommitAuthority duplicateWrites,
            TargetStoreBackend.ReadAuthority reads) {
        if (authorities != null) throw new IllegalStateException("Source authority graph is already constructed");
        authorities = new TargetSourceApplyRuntime.Authorities(leases, successor,
                value -> require(value, TargetSourceApplyRuntime.GrantControl.class),
                value -> require(value, TargetSourceApplyRuntime.FenceControl.class),
                value -> require(value, TargetSourceApplyRuntime.ExpiryControl.class),
                value -> require(value, TargetSourceApplyRuntime.CloseControl.class),
                value -> require(value, TargetSourceApplyRuntime.MembershipControl.class), duplicateWrites, reads,
                value -> require(value, TargetSourceApplyRuntime.CommandControl.class),
                value -> require(value, TargetSourceApplyRuntime.NativePolicyControl.class),
                value -> require(value, TargetSourceApplyRuntime.AdmissionControl.class),
                value -> require(value, TargetSourceApplyRuntime.OutcomeControl.class),
                value -> require(value, TargetSourceApplyRuntime.ChannelControl.class),
                value -> require(value, TargetSourceApplyRuntime.RetirementControl.class),
                value -> require(value, TargetSourceApplyRuntime.ProtocolActivationControl.class));
        return authorities;
    }

    synchronized void bind(TargetSourceApplyRuntime.Authorities actual, Runnable originalLocalOwnerAndSource) {
        if (authorities != actual || localOwnerAndSource != null) {
            throw new IllegalStateException("Source preparation is not this runtime's original authority graph");
        }
        localOwnerAndSource = Objects.requireNonNull(originalLocalOwnerAndSource);
    }

    synchronized boolean prepare(SourceReplayEntry expected, Family expectedFamily) {
        if (stopping) throw new IllegalStateException("Source authority preparation stopped");
        if (entry != null && entry != expected) throw new IllegalStateException("Source authority look-ahead changed");
        if (entry == null) { entry = expected; family = expectedFamily; started = nanos.getAsLong(); }
        if (family != expectedFamily || closing != null) throw new IllegalStateException("Source authority family is changed or closing");
        if (opening == null) {
            final long elapsed=nanos.getAsLong()-started;
            if(elapsed<0||elapsed>=timeoutNanos){requestStop();throw new IllegalStateException("Source input admission exceeded its original finite deadline");}
            opening = OwnedCheckpointIo.submitResource(guard -> {
                final var loaded = Objects.requireNonNull(loader.load(expected, expectedFamily, guard), "original content snapshot");
                try {
                    guard.run(); loaded.requireCurrent();
                    if (!SourceApplyCoordinator.sameEntry(expected, loaded.entry()) || loaded.retainedBytes() < 0
                            || loaded.retainedBytes() > maximumBytes || familyOf(loaded.control()) != expectedFamily) {
                        throw new IllegalStateException("Source content snapshot changes its frame/family or exceeds its byte bound");
                    }
                    if (loaded.control() instanceof TargetSourceApplyRuntime.GrantControl grant
                            && !(grant.authority().capacity() instanceof TargetQuotaCapacitySnapshot.Verified)) {
                        throw new IllegalStateException("configured Source grant requires the concrete protected capacity verifier");
                    }
                    loaded.requireCurrent(); guard.run(); return loaded;
                } catch (RuntimeException | Error failure) { release(loaded, failure); throw failure; }
            }, localOwnerAndSource, value -> release(value, null), executor);
        }
        if (!opening.receipt().isDone()) {
            final long elapsed = nanos.getAsLong() - started;
            if (elapsed < 0 || elapsed >= timeoutNanos) {
                requestStop(); throw new IllegalStateException("Source content acquisition exceeded its finite deadline");
            }
            return false;
        }
        if (opening.receipt().isCompletedExceptionally()) {
            if(CheckpointInputFuture.ownedFailure(opening.receipt()) instanceof InputUnavailable&&opening.ioExited()){
                opening=null;return false;
            }
            opening.receipt().join();
        }
        if (!opening.ioExited()) return false;
        held = opening.completedValue().orElseGet(() -> opening.receipt().join());
        held.requireCurrent(); localOwnerAndSource.run();
        return true;
    }

    /** No I/O: used by the existing verifier while preparing and committing the exact original Source frame. */
    private synchronized <T> T require(SourceReplayEntry expected, Class<T> type) {
        if (entry == null || held == null) throw new Required();
        if (stopping || entry != expected || closing != null || !type.isInstance(held.control())) {
            throw new IllegalStateException("Source content does not belong to this original queued action");
        }
        held.requireCurrent(); localOwnerAndSource.run();
        return type.cast(held.control());
    }

    synchronized boolean complete(SourceReplayEntry expected) {
        if (entry == null) return true;
        if (entry != expected) throw new IllegalStateException("Source authority completion changes its entry");
        if (!closeHeld()) return false;
        entry = null; family = null; opening = null; closing = null; held = null;
        return true;
    }

    public synchronized void requestStop() {
        stopping = true;
        if (opening != null) opening.requestStop();
        closeHeld();
    }

    public synchronized void retryCleanup() {
        if (opening != null) opening.retryCleanup();
        if (closing != null) closing.retryCleanup();
        if (stopping) closeHeld();
    }

    public synchronized boolean ioExited() {
        if (opening != null && !opening.ioExited()) return false;
        return opening == null || opening.completedValue().isEmpty() || closeConfirmed();
    }

    public synchronized Throwable pendingIoFailure() {
        final var failure = closing == null ? null : CheckpointInputFuture.ownedFailure(closing.receipt());
        return failure != null ? failure : opening == null ? null : CheckpointInputFuture.ownedFailure(opening.receipt());
    }

    synchronized TargetSourceAuthorityPreparation fork() {
        return new TargetSourceAuthorityPreparation(loader, executor, nanos, Duration.ofNanos(timeoutNanos), maximumBytes);
    }

    private boolean closeHeld() {
        if (opening == null || !opening.ioExited()) return opening == null;
        final var original = opening.completedValue().orElse(null);
        if (original == null) return true;
        if (closing == null) {
            try { closing = OwnedCheckpointIo.submit(ignored -> { release(original, null); return true; }, () -> {}, executor); }
            catch (RejectedExecutionException full) { return false; }
        }
        return closeConfirmed();
    }

    private boolean closeConfirmed() {
        if (closing == null || !closing.ioExited()) return false;
        if (closing.completedValue().orElse(false)) return true;
        final var failure = CheckpointInputFuture.ownedFailure(closing.receipt());
        return failure instanceof CleanupFailure cleanup && cleanup.cleanupComplete();
    }

    private static void release(Held original, Throwable cause) {
        try { original.close(); }
        catch (RuntimeException | Error failure) {
            final var retained = new CleanupFailure(original, cause == null ? failure : cause);
            if (cause != null && failure != cause) retained.addSuppressed(failure);
            throw retained;
        }
    }

    /** Only a missing first-application snapshot after a local replay precheck may request fresh preparation. */
    static final class Required extends RuntimeException { private static final long serialVersionUID = 1L; }

    private static Family familyOf(Object control) {
        if (control instanceof TargetSourceApplyRuntime.CommandControl) return Family.COMMAND;
        if (control instanceof TargetSourceApplyRuntime.GrantControl) return Family.GRANT;
        if (control instanceof TargetSourceApplyRuntime.FenceControl) return Family.FENCE;
        if (control instanceof TargetSourceApplyRuntime.ExpiryControl) return Family.EXPIRY;
        if (control instanceof TargetSourceApplyRuntime.CloseControl) return Family.CLOSE;
        if (control instanceof TargetSourceApplyRuntime.MembershipControl) return Family.MEMBERSHIP;
        if (control instanceof TargetSourceApplyRuntime.NativePolicyControl) return Family.NATIVE_POLICY;
        if (control instanceof TargetSourceApplyRuntime.AdmissionControl) return Family.ADMISSION;
        if (control instanceof TargetSourceApplyRuntime.OutcomeControl) return Family.OUTCOME;
        if (control instanceof TargetSourceApplyRuntime.ChannelControl) return Family.CHANNEL;
        if (control instanceof TargetSourceApplyRuntime.RetirementControl) return Family.RETIREMENT;
        if (control instanceof TargetSourceApplyRuntime.ProtocolActivationControl) return Family.ACTIVATION;
        throw new IllegalArgumentException("unsupported Source content snapshot");
    }

    public static final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held original;
        private boolean complete;
        CleanupFailure(Held original, Throwable failure) { super("Source content cleanup is not confirmed", failure); this.original = original; }
        @Override public synchronized boolean cleanupComplete() { return complete; }
        @Override public synchronized CompletableFuture<Void> retryCleanup() {
            if (complete) return CompletableFuture.completedFuture(null);
            try { original.close(); complete = true; return CompletableFuture.completedFuture(null); }
            catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
        }
    }
}
