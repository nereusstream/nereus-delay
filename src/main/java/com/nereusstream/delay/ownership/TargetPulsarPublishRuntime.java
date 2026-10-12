package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ResolvedPayload;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetPublishAdmissionStore;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.semantic.TargetNativePolicyAuthority;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** One bounded Claim→Source Admission→Managed Journal/SDK→Source Outcome flight on an exact Worker. */
public final class TargetPulsarPublishRuntime {
    public enum Status { IDLE, BUSY, PREPARING, WAITING_ADMISSION, RESOLVING_PAYLOAD, SUBMITTING,
        WAITING_OUTCOME, SETTLED, REJECTED, REVOKED, READ_YIELD, FAILED }

    public record Turn(Status status, SystemMutation image, Throwable failure) {}

    /** Real Profile/credential/quota preparation on the caller's bounded I/O executor; never a SEND permission. */
    @FunctionalInterface
    public interface Preparation {
        CompletableFuture<Inputs> load(TargetClaimRecord claim);
    }

    /** Frozen external references are checked against actual Claim/Source facts before signing. */
    public record Inputs(TargetChannelIdentity channel, ProfileRef capability, long retryUntilEpochMs,
            CapacityVector commitment, CapacityVector allocated, TargetNativePolicyAuthority policies,
            TargetQuotaDelta.LocalClaimAuthority revokeQuota, TargetStoreBackend.CommitAuthority revokeWrites) {
        public Inputs {
            Objects.requireNonNull(channel, "channel");
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(commitment, "commitment");
            Objects.requireNonNull(allocated, "allocated");
            Objects.requireNonNull(revokeQuota, "revokeQuota");
            Objects.requireNonNull(revokeWrites, "revokeWrites");
            if (retryUntilEpochMs < 0) { throw new IllegalArgumentException("negative Admission retry deadline"); }
        }
    }

    public record Payload(PayloadForPublish descriptor, ResolvedPayload resolved) {
        public Payload {
            Objects.requireNonNull(descriptor, "descriptor");
            Objects.requireNonNull(resolved, "resolved");
        }
    }

    @FunctionalInterface
    public interface Payloads {
        CompletableFuture<Payload> resolve(TargetPublishAdmissionStore.Applied applied);
    }

    private static final class Flight {
        final TargetClaimRecord claim;
        CompletableFuture<Inputs> preparing;
        Inputs inputs;
        volatile SystemMutation admission;
        TargetPublishAdmissionStore.Applied applied;
        CompletableFuture<Payload> payload;
        CompletableFuture<TargetPulsarPublishExecutor.Submission> sending;
        TargetPulsarPublishExecutor.Submission submission;
        boolean admissionSettled;
        boolean rejected;
        Flight(TargetClaimRecord claim) { this.claim = claim; }
    }

    private final TargetWorkerShardRuntime worker;
    private final Preparation preparation;
    private final Payloads payloads;
    private final Supplier<BoundedReadBudget> reads;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final LongSupplier ownerClock;
    private final Executor io;
    private final ArtifactGenerationSet artifacts;
    private final TargetPublishAdmissionMutationFactory signer;
    private final TargetPublishAdmissionWorkClassExecutor admissions;
    private final TargetOutcomeWorkClassExecutor outcomes;
    private final TargetPulsarPublishExecutor publisher;
    private final AtomicReference<Flight> pending = new AtomicReference<>();
    private final AtomicBoolean active = new AtomicBoolean();

    public TargetPulsarPublishRuntime(TargetWorkerShardRuntime worker, ShardLogMutationAppender appender,
            TargetPublishAdmissionMutationFactory signer, TargetPublishOutcomeMutationFactory outcomeSigner,
            PulsarAttemptJournal journal, BoundedDestinationPublishAdapter adapter, ArtifactGenerationSet artifacts,
            Preparation preparation, Payloads payloads, Supplier<BoundedReadBudget> reads,
            Supplier<TrustedUtcIntervalEvidence> time, LongSupplier ownerClock, Executor io, Executor completion,
            BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight ordinaryGate,
            BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight nativeGate,
            TargetPulsarPublishExecutor.ResultAuthority results) {
        this.worker = Objects.requireNonNull(worker, "worker");
        this.signer = Objects.requireNonNull(signer, "signer");
        this.preparation = Objects.requireNonNull(preparation, "preparation");
        this.payloads = Objects.requireNonNull(payloads, "payloads");
        this.reads = Objects.requireNonNull(reads, "reads");
        this.time = Objects.requireNonNull(time, "authenticatedTrustedTime");
        this.ownerClock = Objects.requireNonNull(ownerClock, "ownerClock");
        this.io = Objects.requireNonNull(io, "boundedIoExecutor");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        admissions = new TargetPublishAdmissionWorkClassExecutor(worker, appender);
        outcomes = new TargetOutcomeWorkClassExecutor(worker, appender);
        publisher = new TargetPulsarPublishExecutor(journal, adapter, artifacts, () -> {
            final var flight = Objects.requireNonNull(pending.get(), "pending publication");
            return worker.readAppliedAdmission(readBudget(), flight.admission, ownerClock);
        }, ordinaryGate, Objects.requireNonNull(nativeGate, "nativeGate"),
                applied -> worker.requireNativeOwnership(readBudget(), applied,
                        Objects.requireNonNull(time.get(), "trusted interval"), ownerClock),
                results, outcomeSigner, image -> outcomes.submit(image, ownerClock), completion);
    }

    void requireWorker(TargetWorkerShardRuntime expected) {
        if (worker != expected) { throw new IllegalArgumentException("publisher belongs to another Worker"); }
    }

    /** The scheduler checks this before Claim; acceptance retains only one already durable Claim. */
    public boolean available() { return pending.get() == null; }

    public void accept(TargetClaimRecord claim) {
        Objects.requireNonNull(claim, "claim");
        if (!worker.shardId().equals(claim.work().locator().messageId().routingId().shardId())
                || !pending.compareAndSet(null, new Flight(claim))) {
            throw new IllegalStateException("publisher cannot accept another Shard or a second durable Claim");
        }
    }

    boolean ownsAttempt(byte[] attempt) {
        final var flight = pending.get();
        return flight != null && flight.admission != null
                && Bytes.constantTimeEquals(flight.admission.logicalOperationIdentity(), attempt);
    }

    Optional<SystemMutation> pendingAdmission() {
        final var flight = pending.get();
        return flight == null ? Optional.empty() : Optional.ofNullable(flight.admission);
    }

    Optional<com.nereusstream.delay.scheduler.WorkClassTask> pendingTask() {
        final var flight = pending.get();
        return flight == null ? Optional.empty() : Optional.of(new com.nereusstream.delay.scheduler.WorkClassTask(
                com.nereusstream.delay.scheduler.WorkClass.DUE_SCHEDULER,
                "target-publication/" + Bytes.hex(flight.claim.claimId()), flight.claim.executionBytes()));
    }

    /** One caller-driven turn; no waiting for external futures and no additional thread or queue. */
    public Turn runTurn() {
        final var flight = pending.get();
        if (flight == null) { return new Turn(Status.IDLE, null, null); }
        if (!active.compareAndSet(false, true)) { return new Turn(Status.BUSY, flight.admission, null); }
        try {
            worker.requirePublishRecoveryOwner(ownerClock);
            if (flight.inputs == null) {
                if (flight.preparing == null) {
                    flight.preparing = Objects.requireNonNull(preparation.load(flight.claim), "preparation future");
                }
                if (!flight.preparing.isDone()) { return turn(Status.PREPARING, flight); }
                flight.inputs = Objects.requireNonNull(flight.preparing.join(), "publication inputs");
            }
            if (flight.rejected) {
                if (worker.claimStillCurrent(readBudget(), flight.claim, flight.inputs.revokeQuota(), ownerClock)) {
                    worker.revokeClaim(readBudget(), flight.claim,
                            Bytes.sha256(Bytes.utf8("nereus-delay-target-rejected-admission-revoke\0"),
                                    flight.claim.digest(), flight.admission == null
                                            ? new byte[32] : flight.admission.mutationHash()),
                            flight.inputs.revokeQuota(), flight.inputs.revokeWrites(), ownerClock);
                }
                pending.compareAndSet(flight, null);
                return turn(flight.admission == null ? Status.REVOKED : Status.REJECTED, flight);
            }
            if (flight.admission == null) {
                final var input = flight.inputs;
                flight.admission = worker.preparePublishAdmission(readBudget(), flight.claim, input.channel(),
                        input.capability(), artifacts.setDigest(), input.retryUntilEpochMs(), input.commitment(),
                        input.allocated(), Objects.requireNonNull(time.get(), "trusted interval"), input.policies(),
                        signer, ownerClock);
            }
            if (!flight.admissionSettled) {
                admissions.submit(flight.admission, ownerClock);
                final var first = admissions.settle(ownerClock);
                if (first.isEmpty()) { return turn(Status.WAITING_ADMISSION, flight); }
                if (first.orElseThrow().applyStatus() != ApplyStatus.APPLIED) {
                    flight.rejected = true;
                    return turn(Status.WAITING_ADMISSION, flight);
                }
                flight.applied = worker.readAppliedAdmission(readBudget(), flight.admission, ownerClock);
                flight.admissionSettled = true;
            }
            if (flight.payload == null) {
                flight.payload = Objects.requireNonNull(payloads.resolve(flight.applied), "payload future");
            }
            if (!flight.payload.isDone()) { return turn(Status.RESOLVING_PAYLOAD, flight); }
            if (flight.sending == null) {
                final var payload = Objects.requireNonNull(flight.payload.join(), "resolved payload");
                final var sending = new CompletableFuture<TargetPulsarPublishExecutor.Submission>();
                flight.sending = sending;
                try {
                    io.execute(() -> {
                        try { sending.complete(publisher.submit(payload.descriptor(), payload.resolved())); }
                        catch (RuntimeException | Error failure) { sending.completeExceptionally(failure); }
                    });
                } catch (RuntimeException rejectedExecution) {
                    flight.sending = null;
                    throw rejectedExecution;
                }
            }
            if (!flight.sending.isDone()) { return turn(Status.SUBMITTING, flight); }
            if (flight.submission == null) { flight.submission = flight.sending.join(); }
            if (flight.submission.mutation().isPresent() && publisher.settleApplied(outcomes, ownerClock)) {
                pending.compareAndSet(flight, null);
                return turn(Status.SETTLED, flight);
            }
            if (flight.submission.result().isPresent()) { publisher.retryCompletion(); }
            return new Turn(Status.WAITING_OUTCOME, flight.admission, flight.submission.failure().orElse(null));
        } catch (TargetPublishAdmissionStore.StaleAdmission stale) {
            if (flight.admission == null && flight.inputs != null) {
                flight.rejected = true;
                return turn(Status.PREPARING, flight);
            }
            return new Turn(Status.FAILED, flight.admission, stale);
        } catch (ReadIncompleteException incomplete) {
            return turn(Status.READ_YIELD, flight);
        } catch (RuntimeException failure) {
            return new Turn(Status.FAILED, flight.admission,
                    failure.getCause() == null ? failure : failure.getCause());
        } finally { active.set(false); }
    }

    private BoundedReadBudget readBudget() { return Objects.requireNonNull(reads.get(), "read budget"); }
    private static Turn turn(Status status, Flight flight) { return new Turn(status, flight.admission, null); }
}
