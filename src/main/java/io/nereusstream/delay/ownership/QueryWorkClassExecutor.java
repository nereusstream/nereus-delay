package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded read-only handoff for an already routed and authorized Query.
 *
 * <p>The executor owns only the {@code QUERY} queue, the exact request-byte
 * identity and the local Owner Lease read fence.  The supplied operation must
 * read the shard-local projection and must not mutate RocksDB, allocate a
 * Source Position, perform external I/O or make an authorization/routing
 * decision.  Those authorities stay outside this local composition seam.</p>
 */
public final class QueryWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-query-read-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;

    public QueryWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                  final OwnedDelayShard ownedShard,
                                  final OxiaOwnerLeaseStore authority) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /**
     * Queues one exact canonical request.  Preflight is local and read-free;
     * queue rejection therefore cannot touch the Store or authority.
     */
    public <T> Submission<T> submit(final QueryRequest request,
                                     final LongSupplier ownerClock,
                                     final QueryOperation<T> operation) {
        final QueryRequest exact = Objects.requireNonNull(request, "request");
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        final QueryOperation<T> read = Objects.requireNonNull(operation, "operation");
        ownedShard.requireQuerySubmission(authority, exact.shardId());
        final byte[] taskBytes = taskBytes(exact);
        final WorkClassTask task = new WorkClassTask(WorkClass.QUERY,
                "query-read/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, taskBytes)), taskBytes.length);
        final Submission<T> submission = new Submission<>(task, exact);
        workClasses.submit(task, () -> execute(exact, clock, read, submission));
        return submission;
    }

    private <T> void execute(final QueryRequest request, final LongSupplier clock,
                              final QueryOperation<T> operation, final Submission<T> submission) {
        try {
            final long nowEpochMs = ownedShard.requireQueryAuthoritativelyStrict(
                    authority, request.shardId(), clock);
            final T value = operation.read(nowEpochMs);
            // A query is linearized only when the same Owner is still
            // authoritative after the local read.  If this check fences or
            // fails, discard the value instead of returning a stale snapshot.
            ownedShard.requireQueryAuthoritativelyStrict(authority, request.shardId(), clock);
            submission.complete(QueryResult.completed(value));
        } catch (RuntimeException failure) {
            if (ownedShard.state() == ShardLifecycleState.FENCED
                    || ownedShard.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS) {
                submission.complete(QueryResult.transitioning(failure));
            } else {
                submission.complete(QueryResult.failed(failure));
            }
        } catch (Error failure) {
            submission.complete(QueryResult.failed(failure));
            throw failure;
        }
    }

    private static byte[] taskBytes(final QueryRequest request) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, request.shardId().routeIncarnation().bytes());
            CanonicalProtobuf.uint32Bits(output, 2, request.shardId().partition());
            CanonicalProtobuf.bytes(output, 3, request.canonicalBytes());
        });
    }

    public record QueryRequest(ShardId shardId, byte[] canonicalBytes) {
        public QueryRequest {
            Objects.requireNonNull(shardId, "shardId");
            Objects.requireNonNull(canonicalBytes, "canonicalBytes");
            if (canonicalBytes.length == 0) {
                throw new IllegalArgumentException("query canonical bytes must not be empty");
            }
            canonicalBytes = Bytes.copy(canonicalBytes);
        }

        @Override
        public byte[] canonicalBytes() {
            return Bytes.copy(canonicalBytes);
        }
    }

    @FunctionalInterface
    public interface QueryOperation<T> {
        /** Performs one bounded shard-local read; it must not mutate state. */
        T read(long ownerNowEpochMs);
    }

    public enum ResultKind {
        COMPLETED,
        SHARD_TRANSITIONING,
        FAILED
    }

    public record QueryResult<T>(ResultKind kind, T value, Throwable failure) {
        public QueryResult {
            Objects.requireNonNull(kind, "kind");
            if (kind == ResultKind.COMPLETED && failure != null) {
                throw new IllegalArgumentException("completed query cannot carry failure evidence");
            }
            if (kind != ResultKind.COMPLETED && failure == null) {
                throw new IllegalArgumentException("non-completed query requires failure evidence");
            }
        }

        private static <T> QueryResult<T> completed(final T value) {
            return new QueryResult<>(ResultKind.COMPLETED, value, null);
        }

        private static <T> QueryResult<T> transitioning(final Throwable failure) {
            return new QueryResult<>(ResultKind.SHARD_TRANSITIONING, null,
                    Objects.requireNonNull(failure, "failure"));
        }

        private static <T> QueryResult<T> failed(final Throwable failure) {
            return new QueryResult<>(ResultKind.FAILED, null, Objects.requireNonNull(failure, "failure"));
        }
    }

    public static final class Submission<T> {
        private final WorkClassTask task;
        private final QueryRequest request;
        private volatile QueryResult<T> result;

        private Submission(final WorkClassTask task, final QueryRequest request) {
            this.task = Objects.requireNonNull(task, "task");
            this.request = Objects.requireNonNull(request, "request");
        }

        public WorkClassTask task() {
            return task;
        }

        public QueryRequest request() {
            return request;
        }

        public Optional<QueryResult<T>> result() {
            return Optional.ofNullable(result);
        }

        private synchronized void complete(final QueryResult<T> completed) {
            if (result != null) {
                throw new IllegalStateException("query handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }
}
