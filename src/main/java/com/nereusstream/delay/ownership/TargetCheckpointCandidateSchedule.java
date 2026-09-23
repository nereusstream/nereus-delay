package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointScheduler;
import com.nereusstream.delay.store.CheckpointScheduler.ScheduledCheckpoint;
import com.nereusstream.delay.store.CheckpointUploadIntentAuthority;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Process-local due claims for unpublished Target checkpoint candidates on one Worker host. */
public final class TargetCheckpointCandidateSchedule {
    /** Supplies an already-authorized pending intent and finite audit limits at the source cut. */
    @FunctionalInterface
    public interface RequestFactory {
        Request create(
                TargetWorkerShardRuntime shard,
                ScheduledCheckpoint claim,
                SourceRecordConsumer.CheckpointCut cut);
    }

    public record Request(
            CheckpointUploadIntentAuthority intents,
            LongSupplier ownerClock,
            Path checkpointPath,
            CheckpointUploadIntent pending,
            CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        public Request {
            Objects.requireNonNull(intents, "intents");
            Objects.requireNonNull(ownerClock, "ownerClock");
            Objects.requireNonNull(checkpointPath, "checkpointPath");
            Objects.requireNonNull(pending, "pending");
            Objects.requireNonNull(physicalLimits, "physicalLimits");
            Objects.requireNonNull(quotaLimits, "quotaLimits");
            Objects.requireNonNull(ledgerLimits, "ledgerLimits");
        }
    }

    interface Dispatcher {
        Candidate submit(TargetWorkerHostRuntime.Shard shard, ScheduledCheckpoint claim);
    }

    record Candidate(
            WorkClassTask task, Supplier<Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome>> outcome) {
        Candidate {
            Objects.requireNonNull(task, "task");
            Objects.requireNonNull(outcome, "outcome");
        }
    }

    private record Flight(TargetWorkerHostRuntime.Shard shard, ScheduledCheckpoint claim, Candidate candidate) {}

    private final TargetWorkerHostRuntime host;
    private final CheckpointScheduler scheduler;
    private final Dispatcher dispatcher;
    private final Map<ShardId, TargetWorkerHostRuntime.Shard> registered = new HashMap<>();
    private final Map<ShardId, Flight> flights = new HashMap<>();

    public TargetCheckpointCandidateSchedule(
            final TargetWorkerHostRuntime host, final CheckpointScheduler scheduler, final RequestFactory factory) {
        this(host, scheduler, (Dispatcher) (shard, claim) -> {
            final TargetWorkerShardRuntime worker = (TargetWorkerShardRuntime) shard;
            final var cut = worker.protectCheckpointCut();
            final Request request = Objects.requireNonNull(factory, "factory").create(worker, claim, cut);
            final var submission = worker.submitProtectedCheckpointCandidate(
                    request.intents(),
                    request.ownerClock(),
                    request.checkpointPath(),
                    request.pending(),
                    request.physicalLimits(),
                    request.quotaLimits(),
                    request.ledgerLimits(),
                    cut);
            return new Candidate(submission.task(), submission::outcome);
        });
    }

    /** Test seam: the host still reserves the exact Shard before calling this dispatcher. */
    TargetCheckpointCandidateSchedule(
            final TargetWorkerHostRuntime host, final CheckpointScheduler scheduler, final Dispatcher dispatcher) {
        this.host = Objects.requireNonNull(host, "host");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
    }

    public long register(final TargetWorkerShardRuntime shard, final long nowEpochMs) {
        return registerShard(shard, nowEpochMs);
    }

    long registerShard(final TargetWorkerHostRuntime.Shard shard, final long nowEpochMs) {
        final ShardId shardId = Objects.requireNonNull(shard, "shard").shardId();
        return host.withCheckpointAdmission(shard, () -> {
            synchronized (this) {
                if (registered.containsKey(shardId)) {
                    throw new IllegalStateException("Target checkpoint Shard is already scheduled");
                }
                final long due = scheduler.register(shardId, nowEpochMs);
                registered.put(shardId, shard);
                return due;
            }
        });
    }

    /** An in-flight claim cannot be removed until its exact candidate reaches a terminal outcome. */
    public synchronized void unregister(final ShardId shardId) {
        scheduler.unregister(shardId);
        registered.remove(shardId);
    }

    /**
     * Claims due Shards and submits through host admission. A failed request or admission
     * releases only that exact claim for a later retry. A successfully queued candidate
     * retains its claim until {@link #settle(ShardId, SchedulerBudget, long)} observes its outcome.
     */
    public List<WorkClassTask> claimDueAndSubmit(final long nowEpochMs, final int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("checkpoint claim limit must be positive");
        }
        final List<WorkClassTask> submitted = new ArrayList<>();
        for (int index = 0; index < limit; index++) {
            final ScheduledCheckpoint claim;
            final TargetWorkerHostRuntime.Shard shard;
            synchronized (this) {
                final List<ScheduledCheckpoint> due = scheduler.claimDue(nowEpochMs, 1);
                if (due.isEmpty()) {
                    break;
                }
                claim = due.getFirst();
                shard = registered.get(claim.shardId());
                if (shard == null) {
                    throw new IllegalStateException("claimed Target checkpoint Shard is not registered");
                }
            }
            try {
                final Candidate candidate = host.withCheckpointAdmission(
                        shard, () -> dispatcher.submit(shard, claim));
                synchronized (this) {
                    scheduler.requireCurrentClaim(claim);
                    flights.put(claim.shardId(), new Flight(shard, claim, candidate));
                }
                submitted.add(candidate.task());
            } catch (RuntimeException | Error failure) {
                synchronized (this) {
                    if (!flights.containsKey(claim.shardId())) {
                        scheduler.complete(claim, nowEpochMs);
                    }
                }
                throw failure;
            }
        }
        return List.copyOf(submitted);
    }

    /** Runs a bounded host turn and completes the exact due claim only at candidate terminal state. */
    public Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> settle(
            final ShardId shardId, final SchedulerBudget budget, final long completedAtEpochMs) {
        Objects.requireNonNull(budget, "budget");
        final Flight flight;
        synchronized (this) {
            flight = flights.get(Objects.requireNonNull(shardId, "shardId"));
            if (flight == null) {
                return Optional.empty();
            }
            scheduler.requireCurrentClaim(flight.claim());
        }
        // An independent host settlement may already have made the candidate terminal.
        Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> outcome = flight.candidate().outcome().get();
        if (outcome.isEmpty()) {
            // The host fences an exact Shard and permits settlement after whole-host stop.
            host.settlePendingCheckpointTurn(flight.shard(), flight.candidate().task(), budget);
            outcome = flight.candidate().outcome().get();
        }
        if (outcome.isPresent()) {
            synchronized (this) {
                if (flights.get(shardId) == flight) {
                    scheduler.complete(flight.claim(), completedAtEpochMs);
                    flights.remove(shardId);
                }
            }
        }
        return outcome;
    }

    public synchronized Optional<WorkClassTask> pendingTask(final ShardId shardId) {
        final Flight flight = flights.get(Objects.requireNonNull(shardId, "shardId"));
        return flight == null ? Optional.empty() : Optional.of(flight.candidate().task());
    }
}
