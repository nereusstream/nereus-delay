package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetCheckpointDependencies;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Bounded original-Control collection on one actual Worker cut; providers own I/O, cleanup and protection. */
public final class TargetCheckpointControlInputs {
    public enum Status { CAPTURED, LOADING, ACCEPTED, READ_YIELD, COMPLETE, FAILED }

    @FunctionalInterface
    public interface History {
        /** Must return promptly; the provider owns finite I/O deadlines, history/credential/pin guards and cleanup. */
        CompletableFuture<SystemMutation> load(TargetCheckpointDependencies.ControlReference reference);
    }

    public record Limits(int maximumInputs, long maximumFrameBytes) {
        public Limits {
            if (maximumInputs <= 0 || maximumInputs == Integer.MAX_VALUE
                    || maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Control history collection requires finite positive limits");
            }
        }
    }

    public record Turn(Status status, Throwable failure) {}

    /** Constructed only by the owning Source runtime after its guarded complete ledger fold. */
    public static final class Cut {
        final TargetSourceApplyRuntime owner;
        final TargetQueueSnapshotReader.Cut view;
        private final TargetCheckpointDependencies dependencies;
        private final List<TargetCheckpointDependencies.ControlReference> references;

        Cut(TargetSourceApplyRuntime owner, TargetQueueSnapshotReader.Cut view,
                TargetCheckpointDependencies dependencies) {
            this.owner = owner;
            this.view = view;
            this.dependencies = dependencies;
            references = dependencies.controlReferences();
        }

        public TargetCheckpointDependencies dependencies() { return dependencies; }
    }

    public record Result(Cut cut, List<TargetCheckpointDependencies.ControlImage> inputs) {
        public Result { inputs = List.copyOf(inputs); }
    }

    private final TargetWorkerShardRuntime worker;
    private final Supplier<BoundedReadBudget> reads;
    private final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits;
    private final History history;
    private final Limits limits;
    private final LongSupplier ownerClock;
    private final List<TargetCheckpointDependencies.ControlImage> inputs = new ArrayList<>();
    private Cut cut;
    private CompletableFuture<SystemMutation> loading;
    private long bytes;
    private Throwable failure;
    private boolean complete;

    public TargetCheckpointControlInputs(TargetWorkerShardRuntime worker, Supplier<BoundedReadBudget> reads,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits, History history, Limits limits,
            LongSupplier ownerClock) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.ledgerLimits = Objects.requireNonNull(ledgerLimits, "ledgerLimits");
        this.history = Objects.requireNonNull(history, "history");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    }

    public synchronized Turn runTurn() {
        if (failure != null) { return new Turn(Status.FAILED, failure); }
        try {
            requireCurrent();
            if (complete) { return new Turn(Status.COMPLETE, null); }
            if (cut == null) {
                final var captured = worker.captureCheckpointControlInputs(readBudget(), ledgerLimits, ownerClock);
                if (captured.references.size() > limits.maximumInputs()) {
                    throw new IllegalArgumentException("Control history input count exceeds its bound");
                }
                cut = captured;
                return new Turn(Status.CAPTURED, null);
            }
            if (inputs.size() == cut.references.size()) {
                complete = true;
                return new Turn(Status.COMPLETE, null);
            }
            final var selected = cut.references.get(inputs.size());
            if (loading == null) {
                loading = Objects.requireNonNull(history.load(selected), "history future");
                requireCurrent();
                return new Turn(Status.LOADING, null);
            }
            if (!loading.isDone()) { return new Turn(Status.LOADING, null); }
            final var image = selected.requireImage(Objects.requireNonNull(loading.join(), "Control image"),
                    selected.source());
            final long nextBytes = Math.addExact(bytes, image.encodeFrame().length);
            if (nextBytes > limits.maximumFrameBytes()) {
                throw new IllegalArgumentException("Control history frames exceed their byte bound");
            }
            requireCurrent();
            inputs.add(new TargetCheckpointDependencies.ControlImage(selected.source(), image));
            bytes = nextBytes;
            loading = null;
            return new Turn(Status.ACCEPTED, null);
        } catch (ReadIncompleteException incomplete) {
            return new Turn(Status.READ_YIELD, null);
        } catch (RuntimeException invalid) {
            return fail(invalid.getCause() == null ? invalid : invalid.getCause());
        }
    }

    /** Results remain bound to this active exact Worker/Store cut, including after history completion. */
    public synchronized Optional<Result> result() {
        if (failure != null || !complete) { return Optional.empty(); }
        try {
            requireCurrent();
            return Optional.of(new Result(cut, inputs));
        } catch (ReadIncompleteException incomplete) {
            throw incomplete;
        } catch (RuntimeException invalid) {
            fail(invalid);
            return Optional.empty();
        }
    }

    private void requireCurrent() {
        worker.requirePublishRecoveryOwner(ownerClock);
        if (cut != null) { worker.requireCheckpointControlInputsCurrent(readBudget(), cut, ownerClock); }
    }

    private BoundedReadBudget readBudget() { return Objects.requireNonNull(reads.get(), "read budget"); }

    private Turn fail(Throwable invalid) {
        failure = invalid;
        complete = false;
        inputs.clear();
        // Do not cancel caller-owned history I/O; its provider must finish cleanup even after invalidation.
        loading = null;
        return new Turn(Status.FAILED, invalid);
    }
}
