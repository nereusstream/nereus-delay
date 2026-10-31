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
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Process-local due claims for unpublished Target checkpoint candidates on one Worker host. */
public final class TargetCheckpointCandidateSchedule {
    /** Supplies an already-authorized pending intent and finite audit limits at the source cut. */
    @FunctionalInterface
    public interface RequestFactory {
        Request create(
                TargetWorkerShardRuntime shard, ScheduledCheckpoint claim, SourceRecordConsumer.CheckpointCut cut);
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

    @FunctionalInterface
    public interface SemanticRequestFactory {
        SemanticRequest create(
                TargetWorkerShardRuntime shard, ScheduledCheckpoint claim, SourceRecordConsumer.CheckpointCut cut);
    }

    public record SemanticRequest(
            Request candidate,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protection,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication) {
        public SemanticRequest(
                Request candidate,
                com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
                com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
                Runnable protection) {
            this(candidate, snapshot, semanticLimits, protection, null);
        }

        public SemanticRequest {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(snapshot, "snapshot");
            Objects.requireNonNull(semanticLimits, "semanticLimits");
            Objects.requireNonNull(protection, "protection");
        }
    }

    @FunctionalInterface
    public interface CollectedSemanticRequestFactory {
        CollectedSemanticRequest create(
                TargetWorkerShardRuntime shard, ScheduledCheckpoint claim, SourceRecordConsumer.CheckpointCut cut);
    }

    @FunctionalInterface
    public interface DeferredCollectedSemanticRequestFactory {
        /** Captures only local identity and the original claim/cut; external reads start on the retained task. */
        DeferredPreparation create(
                TargetWorkerShardRuntime shard, ScheduledCheckpoint claim, SourceRecordConsumer.CheckpointCut cut);
    }

    public interface DeferredPreparation extends TargetCheckpointCandidateWorkClassExecutor.PreparationIo {
        String localTaskIdentity();

        long maximumTaskBytes();

        java.util.concurrent.ThreadPoolExecutor checkpointExecutor();
        /** Empty is normal preparation progress. The first complete request is frozen by the original task. */
        Optional<CollectedSemanticRequest> preparedRequest();
        /** Local ownership handoff, before queue admission or the first external read. */
        default void accepted(TargetCheckpointCandidateWorkClassExecutor.Submission original) {}

        default PublishedValidationHold acquirePublishedValidation(
                PublishedValidationRequest request, Runnable current) {
            throw new UnsupportedOperationException("original checkpoint publication validation hold is unavailable");
        }
    }

    public record PublishedValidationRequest(
            TargetCheckpointCandidateWorkClassExecutor.Submission original, long validationGeneration) {
        public PublishedValidationRequest {
            Objects.requireNonNull(original, "original");
        }
    }

    public interface PublishedValidationHold
            extends TargetCheckpointCandidateWorkClassExecutor.PublicationValidationLoan {
        PublishedValidationRequest request();
        /** Checks the exact still-held original authority allocation; no external reads are permitted. */
        void requireLocalCurrent();
    }

    public enum PublishedCutState {
        VALID,
        PENDING,
        NO_MATCH
    }

    public record PublishedCutValidation(
            PublishedCutState state, WorkClassTask task, long generation, Object originalCut) {}

    public record CollectedSemanticRequest(
            Request candidate,
            Supplier<Optional<com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot>> inputs,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable protection,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication,
            TargetCheckpointCandidateWorkClassExecutor.IntentRegistration registration) {
        public CollectedSemanticRequest(
                Request candidate,
                Supplier<Optional<com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot>> inputs,
                com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
                Runnable protection,
                TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication) {
            this(candidate, inputs, semanticLimits, protection, publication, null);
        }

        public CollectedSemanticRequest {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(inputs, "inputs");
            Objects.requireNonNull(semanticLimits, "semanticLimits");
            Objects.requireNonNull(protection, "protection");
        }
    }

    /** Queues bounded history preparation under the same pending task and due claim as image/publication. */
    public static TargetCheckpointCandidateSchedule collectedSemantic(
            TargetWorkerHostRuntime host, CheckpointScheduler scheduler, CollectedSemanticRequestFactory factory) {
        return collectedSemantic(host, scheduler, factory, null);
    }

    public static TargetCheckpointCandidateSchedule collectedSemantic(
            TargetWorkerHostRuntime host,
            CheckpointScheduler scheduler,
            CollectedSemanticRequestFactory factory,
            SchedulerBudget settlementBudget) {
        Objects.requireNonNull(factory, "factory");
        return new TargetCheckpointCandidateSchedule(host, scheduler, (Dispatcher) (shard, claim) -> {
            final var worker = (TargetWorkerShardRuntime) shard;
            final var cut = worker.protectCheckpointCut();
            final var request = Objects.requireNonNull(factory.create(worker, claim, cut), "collected request");
            final var submission = worker.submitCollectedSemanticCheckpointCandidate(request, cut, settlementBudget);
            return new Candidate(
                    submission.task(),
                    submission::outcome,
                    () -> submission.publication().isPresent(),
                    submission::publicationStoreSequence,
                    submission::requirePublicationCurrent);
        });
    }

    public static TargetCheckpointCandidateSchedule deferredCollectedSemantic(
            TargetWorkerHostRuntime host,
            CheckpointScheduler scheduler,
            DeferredCollectedSemanticRequestFactory factory) {
        return deferredCollectedSemantic(host, scheduler, factory, null);
    }

    public static TargetCheckpointCandidateSchedule deferredCollectedSemantic(
            TargetWorkerHostRuntime host,
            CheckpointScheduler scheduler,
            DeferredCollectedSemanticRequestFactory factory,
            SchedulerBudget settlementBudget) {
        Objects.requireNonNull(factory, "factory");
        return new TargetCheckpointCandidateSchedule(host, scheduler, (Dispatcher) (shard, claim) -> {
            final var worker = (TargetWorkerShardRuntime) shard;
            final var cut = worker.protectCheckpointCut();
            final var preparation =
                    Objects.requireNonNull(factory.create(worker, claim, cut), "deferred collected preparation");
            final var submission =
                    worker.submitDeferredCollectedSemanticCheckpointCandidate(preparation, cut, settlementBudget);
            return new Candidate(
                    submission.task(),
                    submission::outcome,
                    () -> submission.publication().isPresent(),
                    submission::publicationStoreSequence,
                    submission::requirePublicationCurrent,
                    preparation,
                    submission,
                    cut::requireCurrent,
                    () -> worker.requirePublishedCheckpointOwnerCurrent(submission));
        });
    }

    /** Schedules complete semantic candidates under the same native cut captured before request construction. */
    public static TargetCheckpointCandidateSchedule semantic(
            TargetWorkerHostRuntime host, CheckpointScheduler scheduler, SemanticRequestFactory factory) {
        return semantic(host, scheduler, factory, null);
    }

    public static TargetCheckpointCandidateSchedule semantic(
            TargetWorkerHostRuntime host,
            CheckpointScheduler scheduler,
            SemanticRequestFactory factory,
            SchedulerBudget settlementBudget) {
        Objects.requireNonNull(factory, "factory");
        return new TargetCheckpointCandidateSchedule(host, scheduler, (Dispatcher) (shard, claim) -> {
            final var worker = (TargetWorkerShardRuntime) shard;
            final var cut = worker.protectCheckpointCut();
            final var semantic = Objects.requireNonNull(factory.create(worker, claim, cut), "semantic request");
            final var request = semantic.candidate();
            final var submission = worker.submitProtectedSemanticCheckpointCandidate(
                    request.intents(),
                    request.ownerClock(),
                    request.checkpointPath(),
                    request.pending(),
                    request.physicalLimits(),
                    request.quotaLimits(),
                    request.ledgerLimits(),
                    semantic.snapshot(),
                    semantic.semanticLimits(),
                    semantic.protection(),
                    cut,
                    semantic.publication(),
                    settlementBudget);
            return new Candidate(
                    submission.task(),
                    submission::outcome,
                    () -> submission.publication().isPresent(),
                    submission::publicationStoreSequence,
                    submission::requirePublicationCurrent);
        });
    }

    interface Dispatcher {
        Candidate submit(TargetWorkerHostRuntime.Shard shard, ScheduledCheckpoint claim);
    }

    record Candidate(
            WorkClassTask task,
            Supplier<Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome>> outcome,
            BooleanSupplier published,
            LongSupplier publicationStoreSequence,
            Runnable requirePublicationCurrent,
            DeferredPreparation deferred,
            TargetCheckpointCandidateWorkClassExecutor.Submission submission,
            Runnable requireLocalCutCurrent,
            Runnable requireLocalOwnerCurrent) {
        Candidate(
                WorkClassTask task,
                Supplier<Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome>> outcome,
                BooleanSupplier published,
                LongSupplier publicationStoreSequence,
                Runnable requirePublicationCurrent) {
            this(
                    task,
                    outcome,
                    published,
                    publicationStoreSequence,
                    requirePublicationCurrent,
                    null,
                    null,
                    () -> {},
                    () -> {});
        }

        Candidate(WorkClassTask task, Supplier<Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome>> outcome) {
            this(task, outcome, () -> false, () -> 0, () -> {});
        }

        Candidate(
                WorkClassTask task,
                Supplier<Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome>> outcome,
                BooleanSupplier published) {
            this(task, outcome, published, () -> 0, () -> {});
        }

        Candidate(
                WorkClassTask task,
                Supplier<Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome>> outcome,
                BooleanSupplier published,
                LongSupplier publicationStoreSequence) {
            this(task, outcome, published, publicationStoreSequence, () -> {});
        }

        Candidate {
            Objects.requireNonNull(task, "task");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(published, "published");
            Objects.requireNonNull(publicationStoreSequence, "publicationStoreSequence");
            Objects.requireNonNull(requirePublicationCurrent, "requirePublicationCurrent");
        }
    }

    private record Flight(
            TargetWorkerHostRuntime.Shard shard, ScheduledCheckpoint claim, Candidate candidate, boolean finalDrain) {}

    private static final class PublishedCut {
        private final TargetWorkerHostRuntime.Shard shard;
        private final long nativeSequence;
        private final Candidate candidate;
        private long generation;
        private PublishedValidationRequest request;
        private TargetCheckpointCandidateWorkClassExecutor.PublicationValidationAttempt<PublishedValidationHold>
                validation;
        private boolean stopping;

        PublishedCut(TargetWorkerHostRuntime.Shard shard, long nativeSequence, Candidate candidate) {
            this.shard = shard;
            this.nativeSequence = nativeSequence;
            this.candidate = candidate;
        }

        void stop() {
            stopping = true;
            if (validation != null) {
                validation.requestStop();
            }
        }

        void retryCleanup() {
            if (validation != null) {
                validation.retryCleanup();
            }
        }

        boolean exited() {
            return validation == null || validation.ioExited();
        }
    }

    private final TargetWorkerHostRuntime host;
    private final CheckpointScheduler scheduler;
    private final Dispatcher dispatcher;
    private final Map<ShardId, TargetWorkerHostRuntime.Shard> registered = new HashMap<>();
    private final Map<ShardId, Flight> flights = new HashMap<>();
    private final Map<ShardId, PublishedCut> publishedCuts = new HashMap<>();
    private final List<PublishedCut> retiredPublishedCuts = new ArrayList<>();
    private boolean publicationValidationStopping;

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
        final var removed = publishedCuts.remove(shardId);
        if (removed != null) {
            retire(removed);
        }
    }

    /**
     * Claims due Shards and submits through host admission. A failed request or admission
     * releases only that exact claim for a later retry. A successfully queued candidate
     * retains its claim until {@link #settle(ShardId, SchedulerBudget, long)} observes its outcome.
     */
    public List<WorkClassTask> claimDueAndSubmit(final long nowEpochMs, final int limit) {
        return claimDueAndSubmit(null, nowEpochMs, limit);
    }

    /** Host round-robin selects a single eligible Shard without claiming a withdrawn sibling. */
    List<WorkClassTask> claimDueForShard(ShardId shardId, long nowEpochMs, int limit) {
        return claimDueAndSubmit(Objects.requireNonNull(shardId, "shardId"), nowEpochMs, limit);
    }

    /** Refreshes membership only for an actually admitted replacement; an old in-flight task is never discarded. */
    void ensureRegistered(TargetWorkerHostRuntime.Shard shard, long now, SchedulerBudget budget) {
        final Flight oldFlight;
        synchronized (this) {
            if (registered.get(shard.shardId()) == shard) {
                return;
            }
            oldFlight = flights.get(shard.shardId());
        }
        if (oldFlight != null) {
            if (oldFlight.candidate().outcome().get().isEmpty()) {
                throw new IllegalStateException("replacement still has an in-flight checkpoint claim");
            }
            settle(shard.shardId(), Objects.requireNonNull(budget, "budget"), now);
        }
        synchronized (this) {
            if (registered.containsKey(shard.shardId())) {
                unregister(shard.shardId());
            }
        }
        registerShard(shard, now);
    }

    void requireHost(TargetWorkerHostRuntime expected) {
        if (host != expected) {
            throw new IllegalArgumentException("checkpoint schedule belongs to another Host");
        }
    }

    private List<WorkClassTask> claimDueAndSubmit(ShardId selectedShard, long nowEpochMs, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("checkpoint claim limit must be positive");
        }
        final List<WorkClassTask> submitted = new ArrayList<>();
        for (int index = 0; index < limit; index++) {
            final ScheduledCheckpoint claim;
            final TargetWorkerHostRuntime.Shard shard;
            synchronized (this) {
                final List<ScheduledCheckpoint> due = selectedShard == null
                        ? scheduler.claimDue(nowEpochMs, 1)
                        : scheduler.claimDueForShard(selectedShard, nowEpochMs, 1);
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
                final Candidate candidate = host.withCheckpointAdmission(shard, () -> dispatcher.submit(shard, claim));
                synchronized (this) {
                    scheduler.requireCurrentClaim(claim);
                    flights.put(claim.shardId(), new Flight(shard, claim, candidate, false));
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
            if (flight.finalDrain()) {
                throw new IllegalStateException("final checkpoint must be settled by Host drain");
            }
            scheduler.requireCurrentClaim(flight.claim());
        }
        // An independent host settlement may already have made the candidate terminal.
        Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> outcome =
                flight.candidate().outcome().get();
        if (outcome.isEmpty()) {
            // The host fences an exact Shard and permits settlement after whole-host stop.
            host.settlePendingCheckpointTurn(flight.shard(), flight.candidate().task(), budget);
            outcome = flight.candidate().outcome().get();
        }
        if (outcome.isPresent()) {
            rememberPublished(flight, outcome.orElseThrow());
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
        return flight == null
                ? Optional.empty()
                : Optional.of(flight.candidate().task());
    }

    synchronized boolean hasFinalDrainFlight(TargetWorkerHostRuntime.Shard shard) {
        final var flight = flights.get(shard.shardId());
        return flight != null && flight.shard() == shard && flight.finalDrain();
    }

    boolean hasCurrentPublishedCut(TargetWorkerHostRuntime.Shard shard) {
        final PublishedCut cut;
        synchronized (this) {
            cut = publishedCuts.get(shard.shardId());
        }
        if (cut == null || cut.shard != shard || cut.nativeSequence != shard.checkpointStoreSequence()) {
            return false;
        }
        cut.candidate.requirePublicationCurrent().run();
        return cut.nativeSequence == shard.checkpointStoreSequence();
    }

    PublishedCutValidation validateCurrentPublishedCutForDrain(TargetWorkerHostRuntime.Shard shard) {
        final PublishedCut cut;
        synchronized (this) {
            if (publicationValidationStopping) {
                throw new IllegalStateException("checkpoint validation is stopping");
            }
            cut = publishedCuts.get(shard.shardId());
        }
        cleanupRetiredPublications();
        if (cut == null) {
            return new PublishedCutValidation(PublishedCutState.NO_MATCH, null, 0, null);
        }
        if (cut.shard != shard) {
            synchronized (cut) {
                cut.stop();
                cut.retryCleanup();
                requireNoValidationFailure(cut);
                return validationResult(cut, cut.exited() ? PublishedCutState.NO_MATCH : PublishedCutState.PENDING);
            }
        }
        cut.candidate.requireLocalOwnerCurrent().run();
        final boolean initialMatch = matchesLocalCut(cut, shard);
        synchronized (cut) {
            if (!initialMatch) {
                cut.stop();
                cut.retryCleanup();
                requireNoValidationFailure(cut);
                return validationResult(cut, cut.exited() ? PublishedCutState.NO_MATCH : PublishedCutState.PENDING);
            }
            if (cut.stopping) {
                cut.retryCleanup();
                requireNoValidationFailure(cut);
                return validationResult(cut, cut.exited() ? PublishedCutState.NO_MATCH : PublishedCutState.PENDING);
            }
            if (cut.candidate.deferred() == null) {
                // Legacy synchronous providers retain their original authority semantics outside Host/Shard locks.
                cut.candidate.requirePublicationCurrent().run();
                return validationResult(cut, PublishedCutState.VALID);
            }
            if (cut.validation == null) {
                cut.request = new PublishedValidationRequest(cut.candidate.submission(), ++cut.generation);
                cut.validation = cut.candidate.submission().beginPublishedValidation((current, retain) -> {
                    final var hold = Objects.requireNonNull(
                            cut.candidate.deferred().acquirePublishedValidation(cut.request, current),
                            "original publication validation loan");
                    retain.accept(hold);
                    if (hold.request() != cut.request) {
                        throw new IllegalStateException("publication validation changed its original request");
                    }
                    current.run();
                    cut.candidate.requirePublicationCurrent().run();
                    hold.requireLocalCurrent();
                    current.run();
                    return hold;
                });
            }
        }
        cut.candidate.submission().advancePublishedValidation();
        final boolean currentCut;
        synchronized (this) {
            currentCut = publishedCuts.get(shard.shardId()) == cut;
        }
        cut.candidate.requireLocalOwnerCurrent().run();
        final boolean finalMatch = matchesLocalCut(cut, shard);
        synchronized (cut) {
            requireNoValidationFailure(cut);
            if (!currentCut) {
                cut.stop();
                return validationResult(cut, PublishedCutState.PENDING);
            }
            if (!finalMatch) {
                cut.stop();
                cut.retryCleanup();
                requireNoValidationFailure(cut);
                return validationResult(cut, cut.exited() ? PublishedCutState.NO_MATCH : PublishedCutState.PENDING);
            }
            final Optional<PublishedValidationHold> ready;
            try {
                ready = cut.validation.ready();
            } catch (RuntimeException | Error failure) {
                cut.stop();
                throw failure;
            }
            if (ready.isEmpty()) {
                return validationResult(cut, PublishedCutState.PENDING);
            }
            final var hold = ready.orElseThrow();
            if (hold.request() != cut.request) {
                throw new IllegalStateException("publication validation loan changed its original generation");
            }
            hold.requireLocalCurrent();
            return validationResult(cut, PublishedCutState.VALID);
        }
    }

    boolean consumeCurrentPublishedCutForDrain(TargetWorkerHostRuntime.Shard shard, PublishedCutValidation result) {
        if (result.state() != PublishedCutState.VALID) {
            return false;
        }
        final PublishedCut cut;
        synchronized (this) {
            cut = publishedCuts.get(shard.shardId());
        }
        if (cut == null || result.originalCut() != cut) {
            return false;
        }
        cut.candidate.requireLocalOwnerCurrent().run();
        final boolean sameLocalCut = matchesLocalCut(cut, shard);
        synchronized (cut) {
            requireNoValidationFailure(cut);
            if (cut.stopping || result.generation() != cut.generation || !sameLocalCut) {
                cut.stop();
                return false;
            }
            if (cut.candidate.deferred() != null && cut.validation == null) {
                return false;
            }
            if (cut.validation != null) {
                final var hold = cut.validation
                        .ready()
                        .orElseThrow(
                                () -> new IllegalStateException("validated publication lost its held authority loan"));
                if (hold.request() != cut.request) {
                    throw new IllegalStateException("consumed publication validation changed generation");
                }
                hold.requireLocalCurrent();
            }
            return true;
        }
    }

    private boolean matchesLocalCut(PublishedCut cut, TargetWorkerHostRuntime.Shard shard) {
        if (cut.shard != shard || cut.nativeSequence != shard.checkpointStoreSequence()) {
            return false;
        }
        try {
            cut.candidate.requireLocalCutCurrent().run();
            return true;
        } catch (IllegalStateException changed) {
            return false;
        }
    }

    private PublishedCutValidation validationResult(PublishedCut cut, PublishedCutState state) {
        return new PublishedCutValidation(state, cut.candidate.task(), cut.generation, cut);
    }

    private void requireNoValidationFailure(PublishedCut cut) {
        if (cut.validation == null) {
            return;
        }
        final var failure = cut.validation.pendingIoFailure();
        if (failure == null || cut.stopping && failure instanceof OwnedCheckpointIo.StoppedException) {
            return;
        }
        cut.stop();
        throw new IllegalStateException("original published checkpoint validation or cleanup failed", failure);
    }

    private synchronized void retire(PublishedCut cut) {
        synchronized (cut) {
            cut.stop();
        }
        if (!retiredPublishedCuts.contains(cut)) {
            retiredPublishedCuts.add(cut);
        }
    }

    private void cleanupRetiredPublications() {
        final List<PublishedCut> retired;
        synchronized (this) {
            retired = List.copyOf(retiredPublishedCuts);
        }
        for (var cut : retired) {
            cut.retryCleanup();
        }
        synchronized (this) {
            retiredPublishedCuts.removeIf(PublishedCut::exited);
        }
    }

    public void requestPublishedValidationStop() {
        final List<PublishedCut> cuts;
        synchronized (this) {
            publicationValidationStopping = true;
            cuts = new ArrayList<>(publishedCuts.values());
            cuts.addAll(retiredPublishedCuts);
        }
        for (var cut : cuts) {
            synchronized (cut) {
                cut.stop();
            }
        }
    }

    public boolean settlePublishedValidationCleanup() {
        final List<PublishedCut> cuts;
        synchronized (this) {
            cuts = new ArrayList<>(publishedCuts.values());
            cuts.addAll(retiredPublishedCuts);
        }
        for (var cut : cuts) {
            cut.retryCleanup();
        }
        cleanupRetiredPublications();
        return cuts.stream().allMatch(PublishedCut::exited);
    }

    private void rememberPublished(Flight flight, TargetCheckpointCandidateWorkClassExecutor.Outcome outcome) {
        if (outcome.failure() == null && flight.candidate().published().getAsBoolean()) {
            final long sequence = flight.candidate().publicationStoreSequence().getAsLong();
            synchronized (this) {
                final var previous = publishedCuts.put(
                        flight.shard().shardId(), new PublishedCut(flight.shard(), sequence, flight.candidate()));
                if (previous != null) {
                    retire(previous);
                }
            }
        }
    }

    /** Called only inside the exact Host drain lifecycle slot and Shard lock, after prior work settles. */
    WorkClassTask submitFinalForDrain(TargetWorkerHostRuntime.Shard shard, long now) {
        final ScheduledCheckpoint claim;
        synchronized (this) {
            if (flights.containsKey(shard.shardId())) {
                throw new IllegalStateException("final checkpoint cannot discard an earlier claim");
            }
            if (registered.get(shard.shardId()) != shard) {
                if (registered.containsKey(shard.shardId())) {
                    unregister(shard.shardId());
                }
                scheduler.register(shard.shardId(), now);
                registered.put(shard.shardId(), shard);
            }
            claim = scheduler.claimForDrain(shard.shardId(), now);
        }
        try {
            final var candidate = dispatcher.submit(shard, claim);
            synchronized (this) {
                scheduler.requireCurrentClaim(claim);
                flights.put(shard.shardId(), new Flight(shard, claim, candidate, true));
            }
            return candidate.task();
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                scheduler.complete(claim, now);
            }
            throw failure;
        }
    }

    /** Settles under the caller's existing drain slot, without reacquiring Host admission. */
    Optional<TargetCheckpointCandidateWorkClassExecutor.Outcome> settleForDrain(
            TargetWorkerHostRuntime.Shard shard, SchedulerBudget budget, long now) {
        final Flight flight;
        synchronized (this) {
            flight = flights.get(shard.shardId());
            if (flight == null) {
                return Optional.empty();
            }
            if (flight.shard() != shard) {
                throw new IllegalStateException("drain checkpoint Shard was replaced");
            }
            scheduler.requireCurrentClaim(flight.claim());
        }
        var outcome = flight.candidate().outcome().get();
        if (outcome.isEmpty()) {
            final var pending = shard.pendingCheckpointTask();
            if (pending.isEmpty() || pending.orElseThrow() != flight.candidate().task()) {
                throw new IllegalStateException("drain checkpoint task has changed");
            }
            shard.runCheckpointTurn(budget);
            outcome = flight.candidate().outcome().get();
        }
        if (outcome.isPresent()) {
            final boolean published = flight.candidate().published().getAsBoolean();
            rememberPublished(flight, outcome.orElseThrow());
            synchronized (this) {
                scheduler.complete(flight.claim(), now);
                flights.remove(shard.shardId());
            }
            if (flight.finalDrain() && outcome.orElseThrow().failure() == null && !published) {
                throw new IllegalStateException("final checkpoint completed without Catalog publication");
            }
        }
        return outcome;
    }
}
