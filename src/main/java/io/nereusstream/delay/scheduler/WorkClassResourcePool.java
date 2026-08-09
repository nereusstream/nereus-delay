package io.nereusstream.delay.scheduler;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Checked local record/byte token pool for Worker work classes.
 *
 * <p>A request may borrow currently idle capacity, but every other class's
 * configured non-borrowable minimum is protected before the request is
 * admitted.  Borrowed leases have a bounded hold window; callers must finish
 * or release them before the next bounded work chunk.</p>
 */
public final class WorkClassResourcePool {
    private final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
    private final EnumMap<WorkClass, HeldUsage> held = new EnumMap<>(WorkClass.class);
    private final Map<Long, ResourceLease> leases = new HashMap<>();
    private final long totalRecords;
    private final long totalBytes;
    private final long maxBorrowedHoldNanos;
    private final LongSupplier clockNanos;
    private long usedRecords;
    private long usedBytes;
    private long nextLeaseId = 1;
    private long lastClockNanos;

    public WorkClassResourcePool(final Map<WorkClass, WorkClassPolicy> policies,
                                 final long totalRecords,
                                 final long totalBytes,
                                 final long maxBorrowedHoldNanos,
                                 final LongSupplier clockNanos) {
        Objects.requireNonNull(policies, "policies");
        if (!java.util.EnumSet.allOf(WorkClass.class).equals(policies.keySet())) {
            throw new IllegalArgumentException("policies must cover every V1 work class exactly");
        }
        if (totalRecords <= 0 || totalBytes <= 0 || maxBorrowedHoldNanos <= 0) {
            throw new IllegalArgumentException("work-class resource limits must be positive");
        }
        this.totalRecords = totalRecords;
        this.totalBytes = totalBytes;
        this.maxBorrowedHoldNanos = maxBorrowedHoldNanos;
        this.clockNanos = Objects.requireNonNull(clockNanos, "clockNanos");
        lastClockNanos = readClock();
        long minimumRecords = 0;
        long minimumBytes = 0;
        try {
            for (WorkClass workClass : WorkClass.values()) {
                final WorkClassPolicy policy = Objects.requireNonNull(policies.get(workClass), "work-class policy");
                this.policies.put(workClass, policy);
                held.put(workClass, new HeldUsage());
                minimumRecords = Math.addExact(minimumRecords, policy.nonBorrowableMinimumRecords());
                minimumBytes = Math.addExact(minimumBytes, policy.nonBorrowableMinimumBytes());
            }
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("work-class non-borrowable minima overflow", overflow);
        }
        if (minimumRecords > totalRecords || minimumBytes > totalBytes) {
            throw new IllegalArgumentException("work-class non-borrowable minima exceed the shared pool");
        }
    }

    /** Acquires exact record/byte tokens, preserving all other class minima. */
    public synchronized ResourceLease acquire(final WorkClass workClass,
                                              final long records,
                                              final long bytes) {
        final WorkClassPolicy policy = policy(workClass);
        if (records <= 0 || bytes < 0) {
            throw new IllegalArgumentException("resource acquisition must contain positive records and bytes");
        }
        final HeldUsage classUsage = held.get(workClass);
        final long nextClassRecords;
        final long nextClassBytes;
        final long protectedOtherRecords;
        final long protectedOtherBytes;
        try {
            nextClassRecords = Math.addExact(classUsage.records, records);
            nextClassBytes = Math.addExact(classUsage.bytes, bytes);
            protectedOtherRecords = protectedMinimumRecords(workClass);
            protectedOtherBytes = protectedMinimumBytes(workClass);
            if (Math.addExact(Math.addExact(usedRecords, records), protectedOtherRecords) > totalRecords
                    || Math.addExact(Math.addExact(usedBytes, bytes), protectedOtherBytes) > totalBytes) {
                throw new IllegalStateException("work-class resource minimums leave no admissible capacity");
            }
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("work-class resource accounting overflow", overflow);
        }
        final long previousBorrowedRecords = Math.max(0,
                classUsage.records - policy.nonBorrowableMinimumRecords());
        final long previousBorrowedBytes = Math.max(0,
                classUsage.bytes - policy.nonBorrowableMinimumBytes());
        final long borrowedRecords = Math.max(0,
                nextClassRecords - policy.nonBorrowableMinimumRecords()) - previousBorrowedRecords;
        final long borrowedBytes = Math.max(0,
                nextClassBytes - policy.nonBorrowableMinimumBytes()) - previousBorrowedBytes;
        final long leaseId;
        try {
            leaseId = Math.addExact(nextLeaseId, 0);
            nextLeaseId = Math.addExact(nextLeaseId, 1);
        } catch (ArithmeticException overflow) {
            throw new IllegalStateException("work-class lease identity exhausted", overflow);
        }
        final ResourceLease lease = new ResourceLease(leaseId, workClass, records, bytes,
                borrowedRecords > 0 || borrowedBytes > 0, readClock());
        leases.put(leaseId, lease);
        classUsage.records = nextClassRecords;
        classUsage.bytes = nextClassBytes;
        usedRecords = Math.addExact(usedRecords, records);
        usedBytes = Math.addExact(usedBytes, bytes);
        return lease;
    }

    /** Fails closed when a borrowed lease was held across its bounded chunk window. */
    public synchronized void requireWithinBorrowedHold(final ResourceLease lease) {
        Objects.requireNonNull(lease, "lease");
        if (leases.get(lease.leaseId) != lease) {
            throw new IllegalStateException("work-class lease is not active");
        }
        if (lease.borrowed && elapsedSince(lease.acquiredAtNanos, readClock()) > maxBorrowedHoldNanos) {
            throw new IllegalStateException("borrowed work-class resource hold exceeded its bound");
        }
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(usedRecords, usedBytes, totalRecords, totalBytes, leases.size());
    }

    private long protectedMinimumRecords(final WorkClass excluded) {
        long result = 0;
        for (WorkClass workClass : WorkClass.values()) {
            if (workClass == excluded) {
                continue;
            }
            final WorkClassPolicy policy = policies.get(workClass);
            result = Math.addExact(result, Math.max(0,
                    policy.nonBorrowableMinimumRecords() - held.get(workClass).records));
        }
        return result;
    }

    private long protectedMinimumBytes(final WorkClass excluded) {
        long result = 0;
        for (WorkClass workClass : WorkClass.values()) {
            if (workClass == excluded) {
                continue;
            }
            final WorkClassPolicy policy = policies.get(workClass);
            result = Math.addExact(result, Math.max(0,
                    policy.nonBorrowableMinimumBytes() - held.get(workClass).bytes));
        }
        return result;
    }

    private WorkClassPolicy policy(final WorkClass workClass) {
        return policies.get(Objects.requireNonNull(workClass, "workClass"));
    }

    private void release(final ResourceLease lease) {
        synchronized (this) {
            if (leases.get(lease.leaseId) != lease) {
                return;
            }
            final HeldUsage classUsage = held.get(lease.workClass);
            if (classUsage.records < lease.records || classUsage.bytes < lease.bytes
                    || usedRecords < lease.records || usedBytes < lease.bytes) {
                throw new IllegalStateException("work-class resource accounting underflow");
            }
            leases.remove(lease.leaseId);
            classUsage.records -= lease.records;
            classUsage.bytes -= lease.bytes;
            usedRecords -= lease.records;
            usedBytes -= lease.bytes;
        }
    }

    private long readClock() {
        final long now = clockNanos.getAsLong();
        if (now < 0 || now < lastClockNanos) {
            throw new IllegalStateException("work-class clock must be monotonic and non-negative");
        }
        lastClockNanos = now;
        return now;
    }

    private static long elapsedSince(final long start, final long end) {
        try {
            return Math.subtractExact(end, start);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    public record Snapshot(long usedRecords, long usedBytes,
                           long totalRecords, long totalBytes, int activeLeases) {
        public Snapshot {
            if (usedRecords < 0 || usedBytes < 0 || totalRecords <= 0 || totalBytes <= 0
                    || activeLeases < 0 || usedRecords > totalRecords || usedBytes > totalBytes) {
                throw new IllegalArgumentException("invalid work-class resource snapshot");
            }
        }
    }

    /** Exact, idempotently releasable reservation handle. */
    public final class ResourceLease implements AutoCloseable {
        private final long leaseId;
        private final WorkClass workClass;
        private final long records;
        private final long bytes;
        private final boolean borrowed;
        private final long acquiredAtNanos;

        private ResourceLease(final long leaseId, final WorkClass workClass,
                              final long records, final long bytes,
                              final boolean borrowed, final long acquiredAtNanos) {
            this.leaseId = leaseId;
            this.workClass = workClass;
            this.records = records;
            this.bytes = bytes;
            this.borrowed = borrowed;
            this.acquiredAtNanos = acquiredAtNanos;
        }

        public WorkClass workClass() {
            return workClass;
        }

        public long records() {
            return records;
        }

        public long bytes() {
            return bytes;
        }

        public boolean borrowed() {
            return borrowed;
        }

        @Override
        public void close() {
            WorkClassResourcePool.this.release(this);
        }
    }

    private static final class HeldUsage {
        private long records;
        private long bytes;
    }
}
