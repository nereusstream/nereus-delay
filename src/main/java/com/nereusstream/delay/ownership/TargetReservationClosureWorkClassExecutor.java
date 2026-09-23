package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetReservationClosureStore;
import com.nereusstream.delay.runtime.TargetReservationControls;
import com.nereusstream.delay.runtime.TargetReservationExpiryStore;
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
import java.util.function.Supplier;

/** One durable-cursor Target reservation Close step per GC action; no process-local scan position. */
public final class TargetReservationClosureWorkClassExecutor {
    private final WorkClassExecutionRegistry workClasses;
    private final TargetReservationClosureStore closures;
    private final TargetReservationExpiryStore expiries;
    private final TargetReservationQueryStore queries;
    private final Limits limits;
    private final Runnable localEligibility;
    private final TargetStoreBackend.ReadAuthority reads;
    private final TargetStoreBackend.CommitAuthority writes;
    private final TargetQuotaDelta.ReservationClosureAuthority closureQuota;
    private final TargetQuotaDelta.ReservationExpiryAuthority expiryQuota;
    private final TargetQuotaDelta.CloseCursorAuthority cursorQuota;
    private final LongSupplier monotonicClock;
    private Submission pending;

    /** Providers must retain current Owner/Store/physical/quota authority through the guarded batch. */
    public TargetReservationClosureWorkClassExecutor(
            WorkClassExecutionRegistry workClasses,
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumDomains,
            TargetReservationControls.Authority controls,
            Limits limits,
            Runnable localEligibility,
            TargetStoreBackend.ReadAuthority reads,
            TargetStoreBackend.CommitAuthority writes,
            TargetQuotaDelta.ReservationClosureAuthority closureQuota,
            TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            TargetQuotaDelta.CloseCursorAuthority cursorQuota,
            LongSupplier monotonicClock) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.closures = new TargetReservationClosureStore(backend, scope, lineage, maximumDomains, controls);
        this.expiries = new TargetReservationExpiryStore(backend, scope, lineage, maximumDomains, controls);
        this.queries = new TargetReservationQueryStore(backend, scope, lineage, controls);
        this.limits = Objects.requireNonNull(limits, "limits");
        this.localEligibility = Objects.requireNonNull(localEligibility, "localEligibility");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.writes = Objects.requireNonNull(writes, "writes");
        this.closureQuota = Objects.requireNonNull(closureQuota, "closureQuota");
        this.expiryQuota = Objects.requireNonNull(expiryQuota, "expiryQuota");
        this.cursorQuota = Objects.requireNonNull(cursorQuota, "cursorQuota");
        final var clock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.monotonicClock = () -> external(clock::getAsLong);
    }

    public record Limits(int maximumRecords, long maximumBytes, long maximumElapsedNanos) {
        public Limits {
            if (maximumRecords <= 0 || maximumBytes <= 0 || maximumElapsedNanos <= 0) {
                throw new IllegalArgumentException("finite positive reservation Close limits required");
            }
        }
    }

    public record Request(ShardId shard, TargetPartitionId target, byte[] requestId) {
        public Request {
            Objects.requireNonNull(shard, "shard");
            Objects.requireNonNull(target, "target");
            Bytes.requireLength(requestId, 16, "requestId");
            if (Arrays.equals(requestId, new byte[16])) {
                throw new IllegalArgumentException("reservation Close request identity must be assigned");
            }
            requestId = Bytes.copy(requestId);
        }

        @Override
        public byte[] requestId() {
            return Bytes.copy(requestId);
        }
    }

    /** Queue rejection reads no Store state; the next submission rediscovers from durable NV40. */
    public synchronized Submission submit(Request request) {
        Objects.requireNonNull(request, "request");
        closures.requireShard(request.shard());
        expiries.requireShard(request.shard());
        queries.requireShard(request.shard());
        requireEligible();
        if (pending != null) {
            throw new IllegalStateException("reservation Close already has an outstanding GC action");
        }
        final byte[] identity = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, request.shard().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(out, 2, request.shard().partition());
            CanonicalProtobuf.bytes(out, 3, request.target().bytes());
            CanonicalProtobuf.bytes(out, 4, request.requestId());
            CanonicalProtobuf.uint64(out, 5, limits.maximumRecords());
            CanonicalProtobuf.uint64(out, 6, limits.maximumBytes());
            CanonicalProtobuf.uint64(out, 7, limits.maximumElapsedNanos());
        });
        final var task = new WorkClassTask(
                WorkClass.GC,
                "target-reservation-close/"
                        + Bytes.hex(Bytes.sha256(Bytes.utf8("nereus-delay-target-reservation-close-work\0"), identity)),
                Math.addExact(identity.length, limits.maximumBytes()));
        final var submission = new Submission(task);
        pending = submission;
        try {
            workClasses.submit(task, () -> execute(submission, request));
        } catch (RuntimeException | Error failure) {
            pending = null;
            throw failure;
        }
        return submission;
    }

    private synchronized void execute(Submission submission, Request request) {
        if (pending != submission || submission.result != null) {
            throw new IllegalStateException("completed or foreign reservation Close action cannot run again");
        }
        try {
            requireEligible();
            final var budget = new BoundedReadBudget(
                    limits.maximumRecords(), limits.maximumBytes(), limits.maximumElapsedNanos(), monotonicClock);
            final TargetStoreBackend.ReadAuthority readAuthority =
                    (metadata, scope) -> guarded(() -> reads.acquire(metadata, scope));
            final TargetStoreBackend.CommitAuthority writeAuthority =
                    (metadata, scope, mutation) -> guarded(() -> writes.acquire(metadata, scope, mutation));
            final var discovered = closures.discover(budget, request.target(), readAuthority);
            if (discovered.isEmpty()) {
                if (closures.completeEmpty(
                        budget,
                        request.target(),
                        readAuthority,
                        writeAuthority,
                        delta -> external(() -> {
                            cursorQuota.requireAuthorized(delta);
                            return null;
                        }))) {
                    submission.complete(new Result(Kind.RESERVATIONS_COMPLETE, null));
                    return;
                }
                final var progress = closures.progress(budget, request.target(), readAuthority);
                submission.complete(new Result(
                        switch (progress) {
                            case NOT_CLOSED -> Kind.NO_CLOSE_MARKER;
                            case OPEN -> Kind.RETRY_DISCOVERY;
                            case COMPLETE -> Kind.ALREADY_COMPLETE;
                        },
                        null));
                return;
            }
            final var candidate = discovered.orElseThrow();
            final var snapshot = queries.read(budget, candidate.reservationId(), readAuthority);
            if (snapshot.isEmpty()
                    || snapshot.orElseThrow().reservation().status() != PayloadReservationStatus.RESERVED
                    || !Arrays.equals(
                            snapshot.orElseThrow().reservation().canonicalBytes(), candidate.canonicalBytes())) {
                submission.complete(new Result(Kind.NO_LONGER_ELIGIBLE, null));
                return;
            }
            final var decision = snapshot.orElseThrow().effectiveStatus();
            final boolean changed;
            final Kind kind;
            if (decision == PayloadReservationStatus.ABANDONED) {
                changed = closures.materialize(
                        budget,
                        candidate.reservationId(),
                        readAuthority,
                        writeAuthority,
                        delta -> external(() -> {
                            closureQuota.requireAuthorized(delta);
                            return null;
                        }));
                kind = Kind.CLOSE_MATERIALIZED;
            } else if (decision == PayloadReservationStatus.EXPIRED) {
                try {
                    changed = expiries.materialize(
                            budget,
                            candidate.reservationId(),
                            readAuthority,
                            writeAuthority,
                            delta -> external(() -> {
                                expiryQuota.requireAuthorized(delta);
                                return null;
                            }));
                } catch (TargetReservationExpiryStore.ClosedTargetException changedDecision) {
                    submission.complete(new Result(Kind.RETRY_DISCOVERY, null));
                    return;
                }
                kind = Kind.EXPIRY_MATERIALIZED;
            } else {
                throw new IllegalStateException("closed Target reservation has no terminal effective decision");
            }
            submission.complete(new Result(changed ? kind : Kind.NO_LONGER_ELIGIBLE, null));
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
        final var guard = Objects.requireNonNull(external(acquire), "Close authority guard");
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
            throw new IllegalStateException("Close authority/clock failed outside the local read budget", failure);
        }
    }

    public enum Kind {
        CLOSE_MATERIALIZED,
        EXPIRY_MATERIALIZED,
        NO_LONGER_ELIGIBLE,
        RESERVATIONS_COMPLETE,
        ALREADY_COMPLETE,
        NO_CLOSE_MARKER,
        RETRY_DISCOVERY,
        READ_INCOMPLETE,
        FAILED
    }

    public record Result(Kind kind, Throwable failure) {
        public Result {
            Objects.requireNonNull(kind, "kind");
            if ((kind == Kind.FAILED || kind == Kind.READ_INCOMPLETE) != (failure != null)) {
                throw new IllegalArgumentException("reservation Close result fields disagree");
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
                throw new IllegalStateException("reservation Close already completed");
            }
            result = Objects.requireNonNull(value, "result");
        }
    }
}
