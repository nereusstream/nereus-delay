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

/** One bounded Close step per GC action; scan rotation is local, while each Target's progress is durable. */
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
    private TargetReservationClosureStore.ScanCursor scanCursor;
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

    public record SweepRequest(ShardId shard, byte[] requestId) {
        public SweepRequest {
            Objects.requireNonNull(shard, "shard");
            Bytes.requireLength(requestId, 16, "requestId");
            if (Arrays.equals(requestId, new byte[16])) {
                throw new IllegalArgumentException("reservation Close sweep request identity must be assigned");
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
        return submit(request.shard(), request.target(), request.requestId());
    }

    /** One NV40 range seek; completed Targets are skipped and range end wraps the local rotation. */
    public synchronized Submission submit(SweepRequest request) {
        Objects.requireNonNull(request, "request");
        return submit(request.shard(), null, request.requestId());
    }

    private Submission submit(ShardId shard, TargetPartitionId target, byte[] requestId) {
        closures.requireShard(shard);
        expiries.requireShard(shard);
        queries.requireShard(shard);
        requireEligible();
        if (pending != null) {
            throw new IllegalStateException("reservation Close already has an outstanding GC action");
        }
        final byte[] identity = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, shard.routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(out, 2, shard.partition());
            CanonicalProtobuf.uint32(out, 3, target == null ? 2 : 1);
            if (target != null) {
                CanonicalProtobuf.bytes(out, 4, target.bytes());
            }
            CanonicalProtobuf.bytes(out, 5, requestId);
            CanonicalProtobuf.uint64(out, 6, limits.maximumRecords());
            CanonicalProtobuf.uint64(out, 7, limits.maximumBytes());
            CanonicalProtobuf.uint64(out, 8, limits.maximumElapsedNanos());
        });
        final var task = new WorkClassTask(
                WorkClass.GC,
                "target-reservation-close/"
                        + Bytes.hex(Bytes.sha256(Bytes.utf8("nereus-delay-target-reservation-close-work\0"), identity)),
                Math.addExact(identity.length, limits.maximumBytes()));
        final var submission = new Submission(task);
        pending = submission;
        try {
            workClasses.submit(task, () -> execute(submission, target));
        } catch (RuntimeException | Error failure) {
            pending = null;
            throw failure;
        }
        return submission;
    }

    private synchronized void execute(Submission submission, TargetPartitionId requestedTarget) {
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
            TargetPartitionId target = requestedTarget;
            TargetReservationClosureStore.ScanCursor nextCursor = null;
            if (target == null) {
                final var scan = closures.discoverNextTarget(budget, scanCursor, readAuthority);
                if (scan.target().isEmpty()) {
                    scanCursor = null;
                    submission.complete(new Result(Kind.SWEEP_COMPLETE, null));
                    return;
                }
                target = scan.target().orElseThrow();
                nextCursor = scan.nextCursor();
                if (scan.progress() == TargetReservationClosureStore.Progress.COMPLETE) {
                    scanCursor = nextCursor;
                    submission.complete(new Result(Kind.SKIPPED_COMPLETE, null));
                    return;
                }
            }
            final Kind outcome = executeTarget(budget, target, readAuthority, writeAuthority);
            if (requestedTarget == null && outcome != Kind.RETRY_DISCOVERY) {
                scanCursor = nextCursor;
            }
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

    private Kind executeTarget(
            BoundedReadBudget budget,
            TargetPartitionId target,
            TargetStoreBackend.ReadAuthority readAuthority,
            TargetStoreBackend.CommitAuthority writeAuthority) {
        final var discovered = closures.discover(budget, target, readAuthority);
        if (discovered.isEmpty()) {
            if (closures.completeEmpty(
                    budget,
                    target,
                    readAuthority,
                    writeAuthority,
                    delta -> external(() -> {
                        cursorQuota.requireAuthorized(delta);
                        return null;
                    }))) {
                return Kind.RESERVATIONS_COMPLETE;
            }
            final var progress = closures.progress(budget, target, readAuthority);
            return switch (progress) {
                case NOT_CLOSED -> Kind.NO_CLOSE_MARKER;
                case OPEN -> Kind.RETRY_DISCOVERY;
                case COMPLETE -> Kind.ALREADY_COMPLETE;
            };
        }
        final var candidate = discovered.orElseThrow();
        final var snapshot = queries.read(budget, candidate.reservationId(), readAuthority);
        if (snapshot.isEmpty()
                || snapshot.orElseThrow().reservation().status() != PayloadReservationStatus.RESERVED
                || !Arrays.equals(snapshot.orElseThrow().reservation().canonicalBytes(), candidate.canonicalBytes())) {
            return Kind.NO_LONGER_ELIGIBLE;
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
                return Kind.RETRY_DISCOVERY;
            }
            kind = Kind.EXPIRY_MATERIALIZED;
        } else {
            throw new IllegalStateException("closed Target reservation has no terminal effective decision");
        }
        return changed ? kind : Kind.NO_LONGER_ELIGIBLE;
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
        SKIPPED_COMPLETE,
        SWEEP_COMPLETE,
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
