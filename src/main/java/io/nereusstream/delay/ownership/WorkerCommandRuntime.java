package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ClaimMaterializationV1;
import io.nereusstream.delay.protocol.PreparedPublishDescriptorV1;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.ScheduleWorkItem;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.runtime.ClaimRecord;
import io.nereusstream.delay.store.SharedRocksDbResources;

import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Worker composition for the exact Claim and Publish Admission handoffs.
 *
 * <p>The caller remains responsible for materializing and authorizing every
 * external input: Profile/Object Store readiness, payload serialization,
 * credential/channel leases, a Claim charge, a signed publish descriptor and
 * its Ready Certificate. This runtime only applies the common Worker resource
 * admission and lifecycle boundary before delegating to the already-fenced
 * bounded executors.</p>
 */
public final class WorkerCommandRuntime {
    private final WorkClassExecutionRegistry workClasses;
    private final SharedRocksDbResources resources;
    private final ClaimHandoffWorkClassExecutor claimExecutor;
    private final PublishAdmissionWorkClassExecutor publishExecutor;

    public WorkerCommandRuntime(final WorkClassExecutionRegistry workClasses,
                                final SharedRocksDbResources resources,
                                final ClaimHandoffWorkClassExecutor claimExecutor,
                                final PublishAdmissionWorkClassExecutor publishExecutor) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.resources = Objects.requireNonNull(resources, "resources");
        this.claimExecutor = Objects.requireNonNull(claimExecutor, "claimExecutor");
        this.publishExecutor = Objects.requireNonNull(publishExecutor, "publishExecutor");
        this.claimExecutor.requireWorkClassExecutionRegistry(this.workClasses);
        this.publishExecutor.requireWorkClassExecutionRegistry(this.workClasses);
        this.resources.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Requires Worker wrappers to use the registry owned by both command executors. */
    void requireWorkClassExecutionRegistry(final WorkClassExecutionRegistry registry) {
        if (workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalArgumentException("command runtime uses another work-class registry");
        }
    }

    /** Queues one exact READY candidate for Claim handoff after Worker admission. */
    public ClaimHandoffWorkClassExecutor.Submission submitClaim(final ClaimRequest request) {
        final ClaimRequest exact = Objects.requireNonNull(request, "request");
        resources.requireRuntimeBusinessAdmission();
        return claimExecutor.submit(exact.item(), exact.evidence(), exact.claimDeadlineEpochMs(),
                exact.materialization(), exact.claimedCharge(), exact.ownerClock());
    }

    /** Queues one exact Claim-to-Publish Admission handoff after Worker admission. */
    public PublishAdmissionWorkClassExecutor.Submission submitPublish(final PublishRequest request) {
        final PublishRequest exact = Objects.requireNonNull(request, "request");
        resources.requireRuntimeBusinessAdmission();
        return publishExecutor.submit(exact.claim(), exact.reservation(), exact.descriptor(),
                exact.readyCertificate(), exact.decisionTime(), exact.retryUntilEpochMs(),
                exact.signingKeyVersion(), exact.signingKey(), exact.ownerClock());
    }

    /** Runs one bounded turn for Claim and Publish Admission actions. */
    public List<WorkClassTask> runTurn(final SchedulerBudget budget) {
        resources.requireRuntimeBusinessAdmission();
        return workClasses.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    public record ClaimRequest(ScheduleWorkItem item,
                               TrustedUtcIntervalEvidence evidence,
                               long claimDeadlineEpochMs,
                               ClaimMaterializationV1 materialization,
                               byte[] claimedCharge,
                               LongSupplier ownerClock) {
        public ClaimRequest {
            Objects.requireNonNull(item, "item");
            Objects.requireNonNull(evidence, "evidence");
            if (claimDeadlineEpochMs < 0) {
                throw new IllegalArgumentException("claimDeadlineEpochMs must be non-negative");
            }
            Objects.requireNonNull(materialization, "materialization");
            claimedCharge = Bytes.copy(Objects.requireNonNull(claimedCharge, "claimedCharge"));
            Objects.requireNonNull(ownerClock, "ownerClock");
        }

        @Override
        public byte[] claimedCharge() {
            return Bytes.copy(claimedCharge);
        }
    }

    public record PublishRequest(ClaimRecord claim,
                                 ClaimExecutionAdmission.Reservation reservation,
                                 PreparedPublishDescriptorV1 descriptor,
                                 ReadyCertificateV1 readyCertificate,
                                 TrustedUtcIntervalEvidence decisionTime,
                                 long retryUntilEpochMs,
                                 int signingKeyVersion,
                                 PrivateKey signingKey,
                                 LongSupplier ownerClock) {
        public PublishRequest {
            Objects.requireNonNull(claim, "claim");
            Objects.requireNonNull(reservation, "reservation");
            Objects.requireNonNull(descriptor, "descriptor");
            Objects.requireNonNull(readyCertificate, "readyCertificate");
            Objects.requireNonNull(decisionTime, "decisionTime");
            if (retryUntilEpochMs < 0) {
                throw new IllegalArgumentException("retryUntilEpochMs must be non-negative");
            }
            Objects.requireNonNull(signingKey, "signingKey");
            Objects.requireNonNull(ownerClock, "ownerClock");
        }
    }
}
