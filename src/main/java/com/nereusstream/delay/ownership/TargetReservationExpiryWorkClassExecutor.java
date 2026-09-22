package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetReservationExpiryStore;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One bounded Target expiry candidate per GC action, sharing its read budget from discovery through accounting. */
public final class TargetReservationExpiryWorkClassExecutor {
    private final WorkClassExecutionRegistry workClasses;
    private final TargetReservationExpiryStore expiries;
    private final Limits limits;
    private final Runnable localEligibility;
    private final TargetStoreBackend.ReadAuthority reads;
    private final TargetStoreBackend.CommitAuthority writes;
    private final TargetQuotaDelta.ReservationExpiryAuthority quota;
    private final LongSupplier monotonicClock;
    private TargetReservationExpiryStore.Cursor cursor;
    private Submission pending;

    /**
     * Providers hold actual Owner/Store/fence/quota authority; eligibility only inspects local lifecycle state.
     */
    public TargetReservationExpiryWorkClassExecutor(
            WorkClassExecutionRegistry workClasses,
            TargetReservationExpiryStore expiries,
            Limits limits,
            Runnable localEligibility,
            TargetStoreBackend.ReadAuthority reads,
            TargetStoreBackend.CommitAuthority writes,
            TargetQuotaDelta.ReservationExpiryAuthority quota,
            LongSupplier monotonicClock) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.expiries = Objects.requireNonNull(expiries, "expiries");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.localEligibility = Objects.requireNonNull(localEligibility, "localEligibility");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.writes = Objects.requireNonNull(writes, "writes");
        this.quota = Objects.requireNonNull(quota, "quota");
        final var clock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.monotonicClock = () -> external(clock::getAsLong);
    }

    public record Limits(int maximumRecords, long maximumBytes, long maximumElapsedNanos) {
        public Limits {
            if (maximumRecords <= 0 || maximumBytes <= 0 || maximumElapsedNanos <= 0) {
                throw new IllegalArgumentException("finite positive reservation expiry limits required");
            }
        }
    }

    public record Request(ShardId shard, byte[] requestId) {
        public Request {
            Objects.requireNonNull(shard, "shard");
            Bytes.requireLength(requestId, 16, "requestId");
            if (Arrays.equals(requestId, new byte[16])) {
                throw new IllegalArgumentException("reservation expiry request identity must be assigned");
            }
            requestId = Bytes.copy(requestId);
        }

        @Override
        public byte[] requestId() {
            return Bytes.copy(requestId);
        }
    }

    /** Single outstanding action per executor; queue rejection performs no reads and preserves the sweep. */
    public synchronized Submission submit(Request request) {
        Objects.requireNonNull(request, "request");
        expiries.requireShard(request.shard());
        requireEligible();
        if (pending != null) {
            throw new IllegalStateException("reservation expiry already has an outstanding GC action");
        }
        final byte[] identity = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, request.shard().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(out, 2, request.shard().partition());
            CanonicalProtobuf.bytes(out, 3, request.requestId());
            CanonicalProtobuf.uint64(out, 4, limits.maximumRecords());
            CanonicalProtobuf.uint64(out, 5, limits.maximumBytes());
            CanonicalProtobuf.uint64(out, 6, limits.maximumElapsedNanos());
        });
        final var task = new WorkClassTask(
                WorkClass.GC,
                "target-reservation-expiry/"
                        + Bytes.hex(
                                Bytes.sha256(Bytes.utf8("nereus-delay-target-reservation-expiry-work\0"), identity)),
                Math.addExact(identity.length, limits.maximumBytes()));
        final var submission = new Submission(task);
        pending = submission;
        try {
            workClasses.submit(task, () -> execute(submission));
        } catch (RuntimeException | Error failure) {
            pending = null;
            throw failure;
        }
        return submission;
    }

    private synchronized void execute(Submission submission) {
        if (pending != submission || submission.result != null) {
            throw new IllegalStateException("completed or foreign reservation expiry action cannot run again");
        }
        try {
            requireEligible();
            final var budget = new BoundedReadBudget(
                    limits.maximumRecords(), limits.maximumBytes(), limits.maximumElapsedNanos(), monotonicClock);
            final TargetStoreBackend.ReadAuthority readAuthority =
                    (metadata, scope) -> guarded(() -> reads.acquire(metadata, scope));
            final var discovery = expiries.discover(budget, cursor, readAuthority);
            if (discovery.candidate().isEmpty()) {
                cursor = null;
                submission.complete(new Result(Kind.SWEEP_COMPLETE, null));
                return;
            }
            final Kind outcome;
            try {
                outcome = expiries.materialize(
                                budget,
                                discovery.candidate().orElseThrow().reservationId(),
                                readAuthority,
                                (metadata, scope, mutation) -> guarded(() -> writes.acquire(metadata, scope, mutation)),
                                delta -> external(() -> {
                                    quota.requireAuthorized(delta);
                                    return null;
                                }))
                        ? Kind.MATERIALIZED
                        : Kind.NO_LONGER_ELIGIBLE;
            } catch (TargetReservationExpiryStore.ClosedTargetException closed) {
                cursor = discovery.nextCursor();
                submission.complete(new Result(Kind.DEFERRED_CLOSED_TARGET, null));
                return;
            }
            cursor = discovery.nextCursor();
            submission.complete(new Result(outcome, null));
        } catch (ReadIncompleteException incomplete) {
            submission.complete(new Result(Kind.READ_INCOMPLETE, incomplete));
        } catch (RuntimeException failure) {
            submission.complete(new Result(Kind.FAILED, failure));
        } catch (Error failure) {
            submission.complete(new Result(Kind.FAILED, failure));
            throw failure;
        } finally {
            pending = null;
        }
    }

    private void requireEligible() {
        external(() -> {
            localEligibility.run();
            return null;
        });
    }

    private static TargetStoreBackend.CommitGuard guarded(Supplier<TargetStoreBackend.CommitGuard> acquire) {
        final var guard = Objects.requireNonNull(external(acquire), "expiry authority guard");
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                external(() -> {
                    guard.requireCurrent();
                    return null;
                });
            }

            @Override
            public void close() {
                external(() -> {
                    guard.close();
                    return null;
                });
            }
        };
    }

    private static <T> T external(Supplier<T> action) {
        try {
            return action.get();
        } catch (ReadIncompleteException failure) {
            throw new IllegalStateException("expiry authority/clock failed outside the local read budget", failure);
        }
    }

    public enum Kind {
        MATERIALIZED,
        NO_LONGER_ELIGIBLE,
        DEFERRED_CLOSED_TARGET,
        SWEEP_COMPLETE,
        READ_INCOMPLETE,
        FAILED
    }

    public record Result(Kind kind, Throwable failure) {
        public Result {
            Objects.requireNonNull(kind, "kind");
            if ((kind == Kind.FAILED || kind == Kind.READ_INCOMPLETE) != (failure != null)) {
                throw new IllegalArgumentException("reservation expiry result fields disagree");
            }
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private volatile Result result;

        private Submission(WorkClassTask task) {
            this.task = task;
        }

        public WorkClassTask task() {
            return task;
        }

        public Optional<Result> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(Result value) {
            if (result != null) {
                throw new IllegalStateException("reservation expiry already completed");
            }
            result = Objects.requireNonNull(value, "result");
        }
    }
}
