package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.runtime.TargetReservationQueryStore;
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

/** QUERY handoff for an already authenticated/routed durable reservation read, without legacy Lane projection. */
public final class TargetReservationQueryWorkClassExecutor {
    private final WorkClassExecutionRegistry workClasses;
    private final TargetReservationQueryStore queries;
    private final Limits limits;
    private final Runnable localEligibility;
    private final TargetStoreBackend.ReadAuthority authority;
    private final LongSupplier monotonicClock;

    /**
     * localEligibility inspects only process-local lifecycle state; read authority holds the exact current Owner.
     */
    public TargetReservationQueryWorkClassExecutor(
            WorkClassExecutionRegistry workClasses,
            TargetReservationQueryStore queries,
            Limits limits,
            Runnable localEligibility,
            TargetStoreBackend.ReadAuthority authority,
            LongSupplier monotonicClock) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.queries = Objects.requireNonNull(queries, "queries");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.localEligibility = Objects.requireNonNull(localEligibility, "localEligibility");
        this.authority = Objects.requireNonNull(authority, "authority");
        final var clock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.monotonicClock = () -> {
            try {
                return clock.getAsLong();
            } catch (ReadIncompleteException failure) {
                throw authorityFailure(failure);
            }
        };
    }

    public record Limits(int maximumRecords, long maximumBytes, long maximumElapsedNanos) {
        public Limits {
            if (maximumRecords <= 0 || maximumBytes <= 0 || maximumElapsedNanos <= 0) {
                throw new IllegalArgumentException("finite positive reservation query limits required");
            }
        }
    }

    public record Request(ShardId shard, byte[] requestId, byte[] reservationId) {
        public Request {
            Objects.requireNonNull(shard, "shard");
            Bytes.requireLength(requestId, 16, "requestId");
            Bytes.requireLength(reservationId, 32, "reservationId");
            if (Arrays.equals(requestId, new byte[16])) {
                throw new IllegalArgumentException("reservation query request identity must be assigned");
            }
            requestId = Bytes.copy(requestId);
            reservationId = Bytes.copy(reservationId);
        }

        @Override
        public byte[] requestId() {
            return Bytes.copy(requestId);
        }

        @Override
        public byte[] reservationId() {
            return Bytes.copy(reservationId);
        }
    }

    public Submission submit(Request request) {
        Objects.requireNonNull(request, "request");
        queries.requireShard(request.shard());
        requireEligible();
        final byte[] identity = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, request.shard().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(out, 2, request.shard().partition());
            CanonicalProtobuf.bytes(out, 3, request.requestId());
            CanonicalProtobuf.bytes(out, 4, request.reservationId());
            CanonicalProtobuf.uint64(out, 5, limits.maximumRecords());
            CanonicalProtobuf.uint64(out, 6, limits.maximumBytes());
            CanonicalProtobuf.uint64(out, 7, limits.maximumElapsedNanos());
        });
        final var task = new WorkClassTask(
                WorkClass.QUERY,
                "target-reservation-query/"
                        + Bytes.hex(Bytes.sha256(Bytes.utf8("nereus-delay-target-reservation-query\0"), identity)),
                Math.addExact(identity.length, limits.maximumBytes()));
        final var submission = new Submission(task);
        workClasses.submit(task, () -> execute(request, submission));
        return submission;
    }

    private void execute(Request request, Submission submission) {
        if (submission.result != null) {
            throw new IllegalStateException("completed reservation query action cannot run again");
        }
        try {
            requireEligible();
            final var budget = new BoundedReadBudget(
                    limits.maximumRecords(), limits.maximumBytes(), limits.maximumElapsedNanos(), monotonicClock);
            final var snapshot = queries.read(budget, request.reservationId(), this::acquireReadGuard);
            submission.complete(new Result(Kind.COMPLETED, snapshot, null));
        } catch (ReadIncompleteException incomplete) {
            submission.complete(new Result(Kind.READ_INCOMPLETE, Optional.empty(), incomplete));
        } catch (RuntimeException failure) {
            submission.complete(new Result(Kind.FAILED, Optional.empty(), failure));
        } catch (Error failure) {
            submission.complete(new Result(Kind.FAILED, Optional.empty(), failure));
            throw failure;
        }
    }

    private void requireEligible() {
        authorityAction(localEligibility);
    }

    private TargetStoreBackend.CommitGuard acquireReadGuard(
            com.nereusstream.delay.store.StoreMetadata metadata,
            com.nereusstream.delay.protocol.TargetQuotaScope scope) {
        final TargetStoreBackend.CommitGuard guard;
        try {
            guard = Objects.requireNonNull(authority.acquire(metadata, scope), "query read guard");
        } catch (ReadIncompleteException failure) {
            throw authorityFailure(failure);
        }
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                authorityAction(guard::requireCurrent);
            }

            @Override
            public void close() {
                authorityAction(guard::close);
            }
        };
    }

    private static void authorityAction(Runnable action) {
        try {
            action.run();
        } catch (ReadIncompleteException failure) {
            throw authorityFailure(failure);
        }
    }

    private static IllegalStateException authorityFailure(ReadIncompleteException failure) {
        return new IllegalStateException("query authority/clock failed outside the local read budget", failure);
    }

    public enum Kind {
        COMPLETED,
        READ_INCOMPLETE,
        FAILED
    }

    public record Result(Kind kind, Optional<TargetReservationQueryStore.Snapshot> snapshot, Throwable failure) {
        public Result {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(snapshot, "snapshot");
            if ((kind == Kind.COMPLETED) != (failure == null) || (kind != Kind.COMPLETED && snapshot.isPresent())) {
                throw new IllegalArgumentException("reservation query result fields disagree");
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
                throw new IllegalStateException("reservation query already completed");
            }
            result = Objects.requireNonNull(value, "result");
        }
    }
}
