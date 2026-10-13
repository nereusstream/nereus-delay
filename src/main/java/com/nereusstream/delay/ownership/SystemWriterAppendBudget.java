package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * One Worker-wide window for system appends on the existing Source log. An UNKNOWN return retains the window
 * until the SDK completes or its owning producer successfully closes. This is neither durable quota release
 * nor proof that the SDK has freed all native allocations; the producer generation's physical envelope is separate.
 */
public final class SystemWriterAppendBudget {
    public record Limits(int records, long bytes, long maximumFrameBytes, int writers) {
        public Limits {
            if (records <= 0 || records == Integer.MAX_VALUE || bytes <= 0 || bytes == Long.MAX_VALUE
                    || maximumFrameBytes <= 0 || maximumFrameBytes > bytes || maximumFrameBytes == Long.MAX_VALUE
                    || writers <= 0 || writers == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("system writer append limits must be finite and serve one frame");
            }
        }
    }

    public record Usage(int writers, int outstandingRecords, long outstandingBytes) {}

    public record Handoff<T>(boolean started, T result) {}

    private final Limits limits;
    private final Set<Writer> writers = new HashSet<>();
    private int records;
    private long bytes;

    public SystemWriterAppendBudget(final Limits limits, final WorkClassExecutionRegistry classes,
            final SharedRocksDbResources resources) {
        this.limits = Objects.requireNonNull(limits, "limits");
        final var exactClasses = Objects.requireNonNull(classes, "classes");
        final var exactResources = Objects.requireNonNull(resources, "resources");
        exactResources.requireWorkerActivationReady();
        exactResources.bindWorkClassExecutionRegistry(exactClasses);
        exactClasses.bindWorkerSingleton(WorkClassExecutionRegistry.WorkerSingleton.SYSTEM_WRITER_APPEND_BUDGET, this);
    }

    /** One handle per owned producer generation; replacements cannot discard old UNKNOWN charges. */
    public synchronized Writer registerWriter(final ShardId shard) {
        if (writers.size() >= limits.writers()) {
            throw new IllegalStateException("system writer generation window is full");
        }
        final var writer = new Writer(Objects.requireNonNull(shard, "shard"));
        writers.add(writer);
        return writer;
    }

    public synchronized Usage usage() {
        return new Usage(writers.size(), records, bytes);
    }

    public final class Writer {
        private final ShardId shard;
        private final Object handoffLock = new Object();
        private final Set<Reservation> pending = new HashSet<>();
        private boolean closing;
        private boolean producerClosed;
        private Object producerIdentity;

        private Writer(final ShardId shard) {
            this.shard = shard;
        }

        public void bindProducer(final Object producer) {
            synchronized (SystemWriterAppendBudget.this) {
                final var selected = Objects.requireNonNull(producer, "producer");
                if (closing || (producerIdentity != null && producerIdentity != selected)
                        || writers.stream().anyMatch(other -> other != this && other.producerIdentity == selected)) {
                    throw new IllegalArgumentException("system writer must bind one exact owned producer generation");
                }
                producerIdentity = selected;
            }
        }

        /** Opening rejected before any SDK producer exists; no pending/native obligation may be discarded. */
        public void abandonUnstarted() {
            synchronized (SystemWriterAppendBudget.this) {
                if (producerIdentity != null || !pending.isEmpty()) throw new IllegalStateException("system writer has an original producer obligation");
                closing = true; producerClosed = true; writers.remove(this);
            }
        }

        /** Reserves one maximum frame before encoding or entering the SDK; no local wait/queue is created. */
        public Optional<Reservation> tryReserve(final ShardId requestedShard) {
            synchronized (SystemWriterAppendBudget.this) {
                if (!shard.equals(Objects.requireNonNull(requestedShard, "requestedShard"))) {
                    throw new IllegalArgumentException("system writer is bound to another Source Shard");
                }
                if (closing || records >= limits.records() || bytes > limits.bytes() - limits.maximumFrameBytes()) {
                    return Optional.empty();
                }
                final var reservation = new Reservation(this, limits.maximumFrameBytes());
                records++;
                bytes += reservation.charge;
                pending.add(reservation);
                return Optional.of(reservation);
            }
        }

        /** Fences new appends before the caller attempts producer close; failed close retains every charge. */
        public void beginClose() {
            synchronized (handoffLock) {
                synchronized (SystemWriterAppendBudget.this) {
                    closing = true;
                }
            }
        }

        /** Serializes only SDK submission against this producer's close, never the response wait. */
        public <T> Handoff<T> invokeSdk(final Reservation reservation, final Supplier<T> action) {
            Objects.requireNonNull(action, "action");
            synchronized (handoffLock) {
                synchronized (SystemWriterAppendBudget.this) {
                    if (Objects.requireNonNull(reservation, "reservation").writer != this
                            || reservation.released || reservation.sdkCompleted || reservation.callerReturned
                            || reservation.sdkStarted || producerIdentity == null) {
                        throw new IllegalStateException("system append SDK reservation is not available");
                    }
                    reservation.sdkStarted = true;
                    if (closing) {
                        reservation.sdkCompleted = true;
                        reservation.releaseIfComplete();
                        return new Handoff<>(false, null);
                    }
                }
                return new Handoff<>(true, action.get());
            }
        }

        /** Call only after this exact producer's native close returned successfully, never after an append timeout. */
        public void producerClosed() {
            synchronized (SystemWriterAppendBudget.this) {
                if (!closing) {
                    throw new IllegalStateException("system writer producer close was not started");
                }
                producerClosed = true;
                for (Reservation reservation : Set.copyOf(pending)) {
                    reservation.sdkCompleted = true;
                    reservation.releaseIfComplete();
                }
                unregisterIfComplete();
            }
        }

        private void unregisterIfComplete() {
            if (producerClosed && pending.isEmpty()) {
                writers.remove(this);
            }
        }
    }

    public final class Reservation {
        private final Writer writer;
        private long charge;
        private boolean sdkCompleted;
        private boolean sdkStarted;
        private boolean callerReturned;
        private boolean released;

        private Reservation(final Writer writer, final long charge) {
            this.writer = writer;
            this.charge = charge;
        }

        /** Shrinks the maximum-frame reservation to the actual encoded frame before SDK handoff. */
        public boolean frameEncoded(final int frameBytes) {
            synchronized (SystemWriterAppendBudget.this) {
                if (released || sdkStarted || sdkCompleted || callerReturned || frameBytes <= 0) {
                    throw new IllegalStateException("system append frame reservation is not available");
                }
                if (frameBytes > charge) {
                    return false;
                }
                bytes -= charge - frameBytes;
                charge = frameBytes;
                return true;
            }
        }

        /** Actual SDK completion callback, or exact proof that SDK handoff never started. */
        public void sdkCompleted() {
            synchronized (SystemWriterAppendBudget.this) {
                sdkCompleted = true;
                releaseIfComplete();
            }
        }

        /** Called from append's finally block; UNKNOWN alone is not SDK completion. */
        public void callerReturned() {
            synchronized (SystemWriterAppendBudget.this) {
                callerReturned = true;
                if (!sdkStarted) { sdkCompleted = true; }
                releaseIfComplete();
            }
        }

        private void releaseIfComplete() {
            if (released || !sdkCompleted || !callerReturned) {
                return;
            }
            released = true;
            if (!writer.pending.remove(this)) {
                throw new IllegalStateException("system append reservation lost its producer generation");
            }
            records--;
            bytes -= charge;
            writer.unregisterIfComplete();
        }
    }
}
