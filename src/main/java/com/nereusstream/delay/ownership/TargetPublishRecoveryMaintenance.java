package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One-row recovery turns and one caller-owned asynchronous history load; no extra thread, queue or SEND path. */
public final class TargetPublishRecoveryMaintenance {
    public enum Status {
        DISCOVERED, SCAN_YIELD, RESTART_SCAN, LOADING, SUBMITTED, WAITING_SOURCE, SETTLED, ALREADY_HANDLED, IDLE, FAILED
    }

    /** Authenticated history and policy/key/time preparation must run outside the Store and maintenance thread. */
    @FunctionalInterface
    public interface History {
        CompletableFuture<Prepared> load(TargetPublishRecoveryDiscovery.Reference reference);
    }

    /** Actual provider authentication of the private guarded scan plus original Source/resource/Artifact protection. */
    @FunctionalInterface
    public interface ResolutionAuthority {
        void requireAuthenticated(com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaUnknown unknown,
                com.nereusstream.delay.protocol.PublishEvidence evidence);
    }

    public record Resolution(com.nereusstream.delay.protocol.PublishEvidence evidence,
            com.nereusstream.delay.protocol.AuthorIdentity service, com.nereusstream.delay.protocol.StableCode stableCode,
            TargetPublishOutcomeMutationFactory signer, ResolutionAuthority authority,TargetArchivedKafkaResolution archived) {
        public Resolution(com.nereusstream.delay.protocol.PublishEvidence evidence,com.nereusstream.delay.protocol.AuthorIdentity service,
                com.nereusstream.delay.protocol.StableCode code,TargetPublishOutcomeMutationFactory signer,ResolutionAuthority authority){
            this(evidence,service,code,signer,authority,null);
        }
        public static Resolution archived(TargetArchivedKafkaResolution actual){
            final var mutation=Objects.requireNonNull(actual).mutation();final var body=com.nereusstream.delay.protocol.PublishOutcomeBody.decodeEvidenceResolution(mutation.canonicalBody());
            return new Resolution(com.nereusstream.delay.protocol.PublishEvidence.decode(body.evidence()),
                    com.nereusstream.delay.protocol.AuthorIdentity.decode(mutation.authorIdentity()),body.stableCode(),null,null,actual);
        }
        public Resolution {
            Objects.requireNonNull(evidence, "actual recovery evidence");
            Objects.requireNonNull(service, "actual SERVICE author");
            Objects.requireNonNull(stableCode, "result code");
            if(archived==null){Objects.requireNonNull(signer,"SERVICE signer");Objects.requireNonNull(authority,"actual recovery authority");}
            else{
                final var mutation=archived.mutation();final var body=com.nereusstream.delay.protocol.PublishOutcomeBody.decodeEvidenceResolution(mutation.canonicalBody());
                archived.prepared().requireExact(mutation);
                if(signer!=null||authority!=null||!java.util.Arrays.equals(evidence.canonicalBytes(),body.evidence())
                        ||!java.util.Arrays.equals(service.canonicalBytes(),mutation.authorIdentity())||stableCode!=body.stableCode())
                    throw new IllegalArgumentException("archived resolution must retain its exact sealed Source image without Worker signing keys");
            }
            if (service.kind() != com.nereusstream.delay.protocol.AuthorIdentity.Kind.SERVICE) {
                throw new IllegalArgumentException("recovery resolution requires its SERVICE identity");
            }
        }
    }

    public record Prepared(SystemMutation admission, OwnerIdentity owner,
            WorkerPublishOutcomeMutationFactory.OutcomeContext context, Resolution resolution) {
        public Prepared(SystemMutation admission, OwnerIdentity owner,
                WorkerPublishOutcomeMutationFactory.OutcomeContext context) { this(admission, owner, context, null); }
        public Prepared {
            Objects.requireNonNull(admission, "admission");
            Objects.requireNonNull(owner, "owner");
            Objects.requireNonNull(context, "context");
        }
    }

    public record Turn(Status status, SystemMutation mutation, Throwable failure) {
        public Turn { Objects.requireNonNull(status, "status"); }
    }

    private final TargetWorkerShardRuntime worker;
    private final TargetPublishRecoveryExecutor executor;
    private final Supplier<BoundedReadBudget> reads;
    private final History history;
    private final LongSupplier ownerClock;
    private TargetPublishRecoveryDiscovery.Cursor cursor;
    private boolean scanningKafkaUnknown;
    private TargetPublishRecoveryDiscovery.Reference selected;
    private CompletableFuture<Prepared> loading;
    private TargetPublishRecoveryExecutor.Submission pending;

    public TargetPublishRecoveryMaintenance(TargetWorkerShardRuntime worker, TargetPublishRecoveryExecutor executor,
            Supplier<BoundedReadBudget> reads, History history, LongSupplier ownerClock) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.executor.requireWorker(this.worker);
        this.reads = Objects.requireNonNull(reads, "reads");
        this.history = Objects.requireNonNull(history, "history");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
    }

    public synchronized Turn runTurn() {
        try {
            worker.requirePublishRecoveryOwner(ownerClock);
            if (pending != null) {
                if (executor.settleApplied(ownerClock)) {
                    final var mutation = pending.mutation();
                    pending = null;
                    selected = null;
                    loading = null;
                    // Any Source/local delta can insert an earlier attempt; do not rebase an old cursor by guesswork.
                    cursor = null;
                    return new Turn(Status.SETTLED, mutation, null);
                }
                executor.retryHandoff(ownerClock);
                return new Turn(Status.WAITING_SOURCE, pending.mutation(), pending.failure().orElse(null));
            }
            if (selected == null) {
                final var page = scanningKafkaUnknown
                        ? worker.discoverKafkaUnknown(readBudget(), cursor, 1, ownerClock)
                        : worker.discoverPublishRecovery(readBudget(), cursor, 1, ownerClock);
                cursor = page.continuation();
                if (!page.entries().isEmpty()) {
                    selected = page.entries().getFirst();
                    return new Turn(Status.DISCOVERED, null, null);
                }
                if (page.complete()) {
                    cursor = null;
                    scanningKafkaUnknown = !scanningKafkaUnknown;
                    return new Turn(scanningKafkaUnknown ? Status.SCAN_YIELD : Status.IDLE, null, null);
                }
                return new Turn(Status.SCAN_YIELD, null, null);
            }
            if (loading == null) {
                if (!worker.publishRecoveryStillPending(readBudget(), selected, ownerClock)) {
                    return clearHandled();
                }
                loading = Objects.requireNonNull(history.load(selected), "history future");
                return new Turn(Status.LOADING, null, null);
            }
            if (!CheckpointInputFuture.ioExited(loading)) {
                return new Turn(Status.LOADING, null, null);
            }
            if (!worker.publishRecoveryStillPending(readBudget(), selected, ownerClock)) {
                return clearHandled();
            }
            final Prepared prepared;
            try {
                prepared = Objects.requireNonNull(loading.join(), "prepared history");
            } catch (RuntimeException failedHistory) {
                loading = null;
                return new Turn(Status.FAILED, null,
                        failedHistory.getCause() == null ? failedHistory : failedHistory.getCause());
            }
            selected.requireImage(prepared.admission(), selected.source());
            if (selected.attempt().phase() == com.nereusstream.delay.protocol.TargetQuotaAttemptBudget.Phase.UNKNOWN) {
                pending = executor.submitResolution(readBudget(), prepared.admission(), prepared.owner(), prepared.context(),
                        Objects.requireNonNull(prepared.resolution(), "actual K2 UNKNOWN recovery result"), ownerClock);
            } else {
                if (prepared.resolution() != null) {
                    throw new IllegalArgumentException("initial ADMITTED recovery cannot skip its Source UNKNOWN hold");
                }
                pending = executor.submit(readBudget(), prepared.admission(), prepared.owner(), prepared.context(), ownerClock);
            }
            return new Turn(Status.SUBMITTED, pending.mutation(), pending.failure().orElse(null));
        } catch (TargetPublishRecoveryDiscovery.StaleCursor changed) {
            cursor = null;
            return new Turn(Status.RESTART_SCAN, null, null);
        } catch (ReadIncompleteException incomplete) {
            return new Turn(Status.SCAN_YIELD, pending == null ? null : pending.mutation(), null);
        } catch (RuntimeException failure) {
            return new Turn(Status.FAILED, pending == null ? null : pending.mutation(), failure);
        }
    }

    /** Drain owns only the selected row/loading/Source handoff; it does not start another discovery sweep. */
    public synchronized boolean pendingForDrain(){return selected!=null||pending!=null||loading!=null&&!CheckpointInputFuture.ioExited(loading);}
    public synchronized void retryLoadingCleanup(){if(loading!=null)CheckpointInputFuture.retryCleanup(loading);}

    public synchronized Optional<SystemMutation> pendingMutation() {
        return pending == null ? Optional.empty() : Optional.of(pending.mutation());
    }

    private BoundedReadBudget readBudget() { return Objects.requireNonNull(reads.get(), "read budget"); }

    private Turn clearHandled() {
        selected = null;
        loading = null;
        cursor = null;
        return new Turn(Status.ALREADY_HANDLED, null, null);
    }

}
