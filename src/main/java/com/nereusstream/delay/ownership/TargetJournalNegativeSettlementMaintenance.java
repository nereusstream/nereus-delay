package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.PulsarRecordTemplate;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Restarts Source-decided/Journal-unsettled negative cleanup from retained actual Source facts, one row per turn. */
public final class TargetJournalNegativeSettlementMaintenance {
    public enum Status {
        IDLE,
        SCANNED,
        LOADING,
        SETTLING,
        SETTLED,
        YIELD,
        RESTART,
        FAILED
    }

    public record Turn(Status status, Throwable failure) {}

    public record Original(SystemMutation admission, SystemMutation outcome, PulsarRecordTemplate template) {
        public Original {
            Objects.requireNonNull(admission, "admission");
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(template, "original template");
        }
    }

    /** Real retained Source/history/template authentication and pins; no synthesized Source first result. */
    @FunctionalInterface
    public interface History {
        CompletableFuture<Original> load(TargetPublishRecoveryDiscovery.Reference reference);
    }

    private final TargetWorkerShardRuntime worker;
    private final PulsarAttemptJournal journal;
    private final PulsarAttemptJournal.NegativeSettlementAuthority authority;
    private final Runnable protection;
    private final Supplier<BoundedReadBudget> reads;
    private final History history;
    private final Executor io;
    private final LongSupplier clock;
    private TargetPublishRecoveryDiscovery.Cursor cursor;
    private volatile TargetPublishRecoveryDiscovery.Reference selected;
    private volatile com.nereusstream.delay.runtime.TargetQueueSnapshotReader.Cut completedCut;
    private volatile long completedSourceSequence = -1;
    private final java.util.concurrent.atomic.AtomicBoolean active = new java.util.concurrent.atomic.AtomicBoolean();
    private CompletableFuture<Original> loading;
    private volatile CompletableFuture<Void> settling;

    public TargetJournalNegativeSettlementMaintenance(
            TargetWorkerShardRuntime worker,
            PulsarAttemptJournal journal,
            PulsarAttemptJournal.NegativeSettlementAuthority authority,
            Runnable protection,
            Supplier<BoundedReadBudget> reads,
            History history,
            Executor io,
            LongSupplier clock) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.journal = Objects.requireNonNull(journal, "real Target Journal");
        this.authority = Objects.requireNonNull(authority, "actual negative authentication");
        this.protection = Objects.requireNonNull(protection, "Journal/history/writer/pin protection");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.history = Objects.requireNonNull(history, "history");
        this.io = Objects.requireNonNull(io, "bounded I/O executor");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** Serialized caller-driven turn; external futures never block a fleet tick. */
    public Turn runTurn() {
        if (!active.compareAndSet(false, true)) {
            return new Turn(Status.YIELD, null);
        }
        try {
            worker.requirePublishRecoveryOwner(clock);
            protection.run();
            if (completedCut != null) {
                if (completedSourceSequence == worker.publicationSourceSequence()) {
                    return new Turn(Status.IDLE, null);
                }
                completedCut = null;
            }
            if (settling != null) {
                if (!settling.isDone()) {
                    return new Turn(Status.SETTLING, null);
                }
                settling.join();
                clear();
                cursor = null;
                return new Turn(Status.SETTLED, null);
            }
            if (selected == null) {
                final var page = worker.discoverCheckpointInputs(readBudget(), cursor, 1, clock);
                cursor = page.continuation();
                if (page.entries().isEmpty()) {
                    if (page.complete()) {
                        final var cut = worker.readTargetQueueCut(readBudget(), clock);
                        completedSourceSequence = worker.confirmJournalSweep(page.continuation());
                        completedCut = cut;
                        cursor = null;
                        return new Turn(Status.IDLE, null);
                    }
                    return new Turn(Status.YIELD, null);
                }
                final var ref = page.entries().getFirst();
                if (ref.attempt().phase() != TargetQuotaAttemptBudget.Phase.RESOLVED_AWAITING_FLOOR
                        || worker.hasLivePublicationAttempt(ref.attempt().publishAttemptId())) {
                    return new Turn(Status.SCANNED, null);
                }
                selected = ref;
                return new Turn(Status.SCANNED, null);
            }
            if (loading == null) {
                loading = Objects.requireNonNull(history.load(selected), "history future");
            }
            if (!loading.isDone()) {
                return new Turn(Status.LOADING, null);
            }
            final var original = Objects.requireNonNull(loading.join(), "original Source images");
            selected.requireImage(original.admission(), selected.source());
            final var admission =
                    TargetPublishAdmissionBody.decode(original.admission().canonicalBody());
            final var body = original.outcome().type() == SystemMutationType.EVIDENCE_RESOLUTION
                    ? PublishOutcomeBody.decodeEvidenceResolution(
                            original.outcome().canonicalBody())
                    : PublishOutcomeBody.decode(original.outcome().canonicalBody());
            if (!admission.materialized()
                    || admission.channel().context().kind() != ChannelKind.PULSAR_DEDUP_PRODUCER
                    || original.template().targetResource().kind() != BrokerResourceIdentity.Kind.PULSAR
                    || body.sideEffect() != 2) {
                clear();
                return new Turn(Status.SCANNED, null);
            }
            final var mapping = journal.targetMapping(admission.publishAttemptId())
                    .orElseThrow(() -> new IllegalStateException(
                            "Source-decided managed Attempt lost its retained Journal mapping"));
            protection.run();
            final var state = journal.state(mapping.mappingId());
            if (state == PulsarAttemptJournal.AttemptState.RETIRED_NOT_PUBLISHED) {
                clear();
                return new Turn(Status.SCANNED, null);
            }
            final var future = new CompletableFuture<Void>();
            settling = future;
            try {
                io.execute(() -> {
                    try {
                        protection.run();
                        final var proof = worker.readNegativeOutcome(
                                readBudget(),
                                mapping.mappingId(),
                                original.admission(),
                                original.outcome(),
                                original.template(),
                                clock);
                        journal.resolveNotPublished(mapping, proof, authority);
                        protection.run();
                        future.complete(null);
                    } catch (RuntimeException | Error failed) {
                        future.completeExceptionally(failed);
                    }
                });
            } catch (RuntimeException rejected) {
                settling = null;
                throw rejected;
            }
            return new Turn(Status.SETTLING, null);
        } catch (TargetPublishRecoveryDiscovery.StaleCursor changed) {
            cursor = null;
            return new Turn(Status.RESTART, null);
        } catch (ReadIncompleteException incomplete) {
            return new Turn(Status.YIELD, null);
        } catch (RuntimeException failed) {
            final var cause = failed.getCause() == null ? failed : failed.getCause();
            if (cause instanceof ReadIncompleteException
                    || cause instanceof java.util.concurrent.RejectedExecutionException) {
                settling = null;
            }
            return new Turn(Status.FAILED, cause);
        } finally {
            active.set(false);
        }
    }

    void requireJournal(PulsarAttemptJournal expected) {
        if (journal != expected) {
            throw new IllegalArgumentException("cleanup belongs to another protected Journal");
        }
    }

    boolean completedForSource(long sequence) {
        return completedCut != null && completedSourceSequence == sequence && !pending();
    }

    public boolean pending() {
        return completedCut == null || selected != null || settling != null || active.get();
    }

    public com.nereusstream.delay.runtime.TargetQueueSnapshotReader.Cut completedCut() {
        return completedCut;
    }

    private void clear() {
        selected = null;
        loading = null;
        settling = null;
    }

    private BoundedReadBudget readBudget() {
        return Objects.requireNonNull(reads.get(), "read budget");
    }
}
