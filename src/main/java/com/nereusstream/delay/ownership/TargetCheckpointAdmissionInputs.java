package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Bounded original-Admission input collection on one Store cut; no thread, queue, SEND or publication authority. */
public final class TargetCheckpointAdmissionInputs {
    public enum Status { DISCOVERED, LOADING, ACCEPTED, READ_YIELD, COMPLETE, FAILED }

    /** Must return promptly; the provider owns bounded history I/O, retention/credential authentication and cleanup. */
    @FunctionalInterface
    public interface History {
        CompletableFuture<SystemMutation> load(TargetPublishRecoveryDiscovery.Reference reference);
    }

    public record Limits(int maximumInputs, long maximumFrameBytes) {
        public Limits {
            if (maximumInputs <= 0 || maximumInputs == Integer.MAX_VALUE
                    || maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("checkpoint Admission inputs require finite positive bounds");
            }
        }
    }

    public record Input(TargetPublishRecoveryDiscovery.Reference reference, SystemMutation image) {
        public Input {
            Objects.requireNonNull(reference, "reference").requireImage(image, reference.source());
        }
    }

    public record Turn(Status status, Throwable failure) {
        public Turn { Objects.requireNonNull(status, "status"); }
    }

    private final TargetWorkerShardRuntime worker;
    private final Supplier<BoundedReadBudget> reads;
    private final History history;
    private final Limits limits;
    private final LongSupplier ownerClock;
    private final List<Input> inputs = new ArrayList<>();
    private TargetPublishRecoveryDiscovery.Cursor cursor;
    private TargetPublishRecoveryDiscovery.Reference selected;
    private CompletableFuture<SystemMutation> loading;
    private long bytes;
    private boolean complete;
    private Throwable failure;

    public TargetCheckpointAdmissionInputs(TargetWorkerShardRuntime worker, Supplier<BoundedReadBudget> reads,
            History history, Limits limits, LongSupplier ownerClock) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.history = Objects.requireNonNull(history, "history");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    }

    public synchronized Turn runTurn() {
        if (failure != null) {
            return new Turn(Status.FAILED, failure);
        }
        try {
            requireCurrent();
            if (complete) {
                return new Turn(Status.COMPLETE, null);
            }
            if (selected == null) {
                final var page = worker.discoverCheckpointInputs(readBudget(), cursor, 1, ownerClock);
                cursor = page.continuation();
                if (!page.entries().isEmpty()) {
                    if (inputs.size() == limits.maximumInputs()) {
                        throw new IllegalStateException("checkpoint Admission input count exceeds its bound");
                    }
                    selected = page.entries().getFirst();
                    return new Turn(Status.DISCOVERED, null);
                }
                if (page.complete()) {
                    requireCurrent();
                    complete = true;
                    return new Turn(Status.COMPLETE, null);
                }
                return new Turn(Status.READ_YIELD, null);
            }
            if (loading == null) {
                loading = Objects.requireNonNull(history.load(selected), "history future");
                requireCurrent();
                return new Turn(Status.LOADING, null);
            }
            if (!loading.isDone()) {
                return new Turn(Status.LOADING, null);
            }
            final var image = Objects.requireNonNull(loading.join(), "Admission image");
            final var input = new Input(selected, image);
            final long nextBytes = Math.addExact(bytes, image.encodeFrame().length);
            if (nextBytes > limits.maximumFrameBytes()) {
                throw new IllegalStateException("checkpoint Admission input bytes exceed their bound");
            }
            requireCurrent();
            inputs.add(input);
            bytes = nextBytes;
            selected = null;
            loading = null;
            return new Turn(Status.ACCEPTED, null);
        } catch (ReadIncompleteException incomplete) {
            return new Turn(Status.READ_YIELD, null);
        } catch (RuntimeException invalid) {
            return fail(invalid.getCause() == null ? invalid : invalid.getCause());
        }
    }

    /** A complete local input set still requires authenticated semantic closure and protected publication cut. */
    public synchronized Optional<List<Input>> result() {
        if (failure != null || !complete) {
            return Optional.empty();
        }
        try {
            requireCurrent();
            return Optional.of(List.copyOf(inputs));
        } catch (ReadIncompleteException incomplete) {
            throw incomplete;
        } catch (RuntimeException invalid) {
            fail(invalid);
            return Optional.empty();
        }
    }

    private void requireCurrent() {
        worker.requirePublishRecoveryOwner(ownerClock);
        if (cursor != null) {
            worker.requireCheckpointInputsCurrent(readBudget(), cursor, ownerClock);
        }
    }

    private BoundedReadBudget readBudget() { return Objects.requireNonNull(reads.get(), "read budget"); }

    private Turn fail(Throwable invalid) {
        failure = invalid;
        complete = false;
        inputs.clear();
        selected = null;
        loading = null;
        return new Turn(Status.FAILED, invalid);
    }
}
