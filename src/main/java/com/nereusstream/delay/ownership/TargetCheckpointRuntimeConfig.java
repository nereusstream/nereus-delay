package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointScheduler;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Required local checkpoint bounds; trusted publication/provider/retention authority is supplied independently. */
public record TargetCheckpointRuntimeConfig(
        long intervalMillis,
        int jitterPercent,
        SchedulerBudget settlementTurn,
        int readRecords,
        long readBytes,
        long readNanos,
        Duration registryLookupTimeout,
        TargetCheckpointControlInputs.Limits controls,
        TargetCheckpointOutcomeInputs.Limits outcomes,
        TargetCheckpointAdmissionInputs.Limits admissions,
        TargetCheckpointRegistryInputs.Limits registry,
        TargetCheckpointSemanticSnapshot.Limits semantics,
        CheckpointManifestLimits physical,
        TargetCheckpointRootVerifier.QuotaAuditLimits quota,
        TargetCheckpointRootVerifier.LedgerAuditLimits ledger) {
    public TargetCheckpointRuntimeConfig {
        Objects.requireNonNull(settlementTurn, "settlementTurn");
        Objects.requireNonNull(registryLookupTimeout, "registryLookupTimeout");
        Objects.requireNonNull(controls, "controls");
        Objects.requireNonNull(outcomes, "outcomes");
        Objects.requireNonNull(admissions, "admissions");
        Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(semantics, "semantics");
        Objects.requireNonNull(quota, "quota");
        Objects.requireNonNull(ledger, "ledger");
        Objects.requireNonNull(physical, "physical");
        if (physical.maxFiles() == Integer.MAX_VALUE
                || physical.maxTotalFileBytes() == Long.MAX_VALUE
                || physical.maxIndividualFileBytes() == Long.MAX_VALUE
                || physical.maxPathBytes() == Integer.MAX_VALUE
                || physical.maxManifestBytes() == Integer.MAX_VALUE
                || physical.maxEvidenceCursors() == Integer.MAX_VALUE
                || physical.maxObjectIdentityBytes() == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("checkpoint configuration requires finite physical bounds");
        }
        if (intervalMillis <= 0
                || intervalMillis == Long.MAX_VALUE
                || jitterPercent < 0
                || jitterPercent >= 100
                || readRecords <= 0
                || readRecords == Integer.MAX_VALUE
                || readBytes <= 0
                || readBytes == Long.MAX_VALUE
                || readNanos <= 0
                || readNanos == Long.MAX_VALUE
                || registryLookupTimeout.isZero()
                || registryLookupTimeout.isNegative()) {
            throw new IllegalArgumentException("checkpoint configuration requires finite positive bounds");
        }
        CheckpointScheduler.requireLimits(intervalMillis, jitterPercent, 1);
        try {
            if (registryLookupTimeout.toNanos() == Long.MAX_VALUE) {
                throw new IllegalArgumentException("checkpoint Registry timeout is unbounded");
            }
            final long histories = Math.addExact((long) controls.maximumInputs(), outcomes.maximumInputs());
            final long records =
                    Math.addExact(Math.addExact(histories, admissions.maximumInputs()), registry.maximumUses());
            if (records > semantics.maximumRecords()
                    || admissions.maximumFrameBytes() > semantics.maximumAdmissionFrameBytes()
                    || semantics.maximumBytes() >= settlementTurn.maxBytes()
                    || readNanos > settlementTurn.maxElapsedNanos()) {
                throw new IllegalArgumentException("checkpoint limits cannot serve the configured collection envelope");
            }
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("checkpoint configuration overflows", overflow);
        }
    }

    /** Validates the full declared task byte ceiling without constructing a dispatcher or allocating resources. */
    public void requireWorkClasses(WorkClassRuntimeConfig classes) {
        final var config = Objects.requireNonNull(classes, "classes");
        config.requireTaskServiceable(
                new WorkClassTask(WorkClass.CHECKPOINT, "target-configured-checkpoint", settlementTurn.maxBytes()));
        if (readNanos > config.policies().get(WorkClass.CHECKPOINT).maxTimePerTurnNanos()) {
            throw new IllegalArgumentException("CHECKPOINT policy cannot serve the configured read time bound");
        }
    }

    public CheckpointScheduler newScheduler(int maximumShards) {
        return new CheckpointScheduler(intervalMillis, jitterPercent, maximumShards);
    }

    public void requireSchedule(CheckpointScheduler scheduler, int maximumShards, SchedulerBudget turn) {
        Objects.requireNonNull(scheduler, "scheduler")
                .requireConfiguration(intervalMillis, jitterPercent, maximumShards);
        if (!settlementTurn.equals(Objects.requireNonNull(turn, "turn"))) {
            throw new IllegalArgumentException("checkpoint settlement budget differs from the configured Worker");
        }
    }

    public BoundedReadBudget newReadBudget(LongSupplier nanos) {
        return new BoundedReadBudget(readRecords, readBytes, readNanos, Objects.requireNonNull(nanos, "nanos"));
    }

    /** Constructs the actual three collectors and authenticated Registry preparation under these exact bounds. */
    public TargetCheckpointSemanticInputs newInputs(
            TargetWorkerShardRuntime worker,
            TargetCheckpointControlInputs.History controlHistory,
            TargetCheckpointOutcomeInputs.History outcomeHistory,
            TargetCheckpointAdmissionInputs.History admissionHistory,
            TargetCheckpointRegistryInputs.Providers providers,
            TargetCheckpointSemanticInputs.OriginalAuthorityFactory authorities,
            LongSupplier ownerClock,
            LongSupplier nanos,
            Runnable protection) {
        final java.util.function.Supplier<BoundedReadBudget> reads = () -> newReadBudget(nanos);
        return TargetCheckpointSemanticInputs.authenticated(
                new TargetCheckpointControlInputs(worker, reads, ledger, controlHistory, controls, ownerClock),
                new TargetCheckpointOutcomeInputs(worker, reads, ledger, outcomeHistory, outcomes, ownerClock),
                new TargetCheckpointAdmissionInputs(worker, reads, admissionHistory, admissions, ownerClock),
                new TargetCheckpointSemanticInputs.Preparation(
                        worker, reads, ownerClock, protection, providers, registry, semantics, authorities));
    }

    /** Only public exact semantics; caller still configures the underlying SDK request timeout and owns I/O cleanup. */
    public TargetCheckpointRegistryInputs.Providers newRegistryProviders(
            com.nereusstream.delay.runtime.OxiaSyncSourceProfileCatalog profiles,
            com.nereusstream.delay.runtime.OxiaSyncRetryPolicyCatalog retries,
            com.nereusstream.delay.runtime.OxiaSyncPayloadProofTrustSetCatalog trusts,
            Runnable protection,
            java.util.concurrent.ThreadPoolExecutor executor) {
        return TargetCheckpointRegistryInputs.Providers.fromOxiaCatalogs(
                profiles, retries, trusts, registryLookupTimeout, protection, executor);
    }

    public void requireRequest(TargetCheckpointCandidateSchedule.CollectedSemanticRequest request) {
        requireCandidate(Objects.requireNonNull(request, "request").candidate(), request.semanticLimits());
        Objects.requireNonNull(request.publication(), "configured checkpoint requires formal publication");
    }

    public void requireRequest(TargetCheckpointCandidateSchedule.SemanticRequest request) {
        requireCandidate(Objects.requireNonNull(request, "request").candidate(), request.semanticLimits());
        Objects.requireNonNull(request.publication(), "configured checkpoint requires formal publication");
    }

    private void requireCandidate(
            TargetCheckpointCandidateSchedule.Request request, TargetCheckpointSemanticSnapshot.Limits limits) {
        if (!physical.equals(request.physicalLimits())
                || !quota.equals(request.quotaLimits())
                || !ledger.equals(request.ledgerLimits())
                || !semantics.equals(limits)) {
            throw new IllegalArgumentException("checkpoint request bounds differ from the configured Worker");
        }
    }
}
