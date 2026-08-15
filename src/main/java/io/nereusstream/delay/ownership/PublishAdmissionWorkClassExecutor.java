package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.ClaimMaterializationV1;
import io.nereusstream.delay.protocol.ClaimResultBody;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.PreparedPublishDescriptorV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.ReservedPublishMetadataV1;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.ClaimRecord;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassTask;

import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Bounded Claim-to-PUBLISH_ADMISSION handoff.
 *
 * <p>The exact mutation is prepared and signed before queue admission.  The
 * bounded action rechecks the Owner/Claim boundary, asks the injected live
 * prerequisite gate, and then calls the external Shard Log appender.  It never
 * calls {@code DelayShard.applySystemMutation}; only the source-ordered
 * {@link SourceApplyWorkClassExecutor} may apply a mutation and advance the
 * local Source Position.</p>
 */
public final class PublishAdmissionWorkClassExecutor {
    private static final byte[] TASK_ID_DOMAIN =
            Bytes.utf8("nereus-delay-publish-admission-handoff-task-v1\0");

    private final WorkClassExecutionRegistry workClasses;
    private final OwnedDelayShard ownedShard;
    private final OxiaOwnerLeaseStore authority;
    private final ClaimExecutionAdmission permits;
    private final ShardLogMutationAppender appender;
    private final AdmissionPrerequisiteGate prerequisiteGate;

    public PublishAdmissionWorkClassExecutor(final WorkClassExecutionRegistry workClasses,
                                             final OwnedDelayShard ownedShard,
                                             final OxiaOwnerLeaseStore authority,
                                             final ClaimExecutionAdmission permits,
                                             final ShardLogMutationAppender appender,
                                             final AdmissionPrerequisiteGate prerequisiteGate) {
        this.workClasses = Objects.requireNonNull(workClasses, "workClasses");
        this.ownedShard = Objects.requireNonNull(ownedShard, "ownedShard");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.permits = Objects.requireNonNull(permits, "permits");
        this.appender = Objects.requireNonNull(appender, "appender");
        this.prerequisiteGate = Objects.requireNonNull(prerequisiteGate, "prerequisiteGate");
        this.workClasses.bindClaimExecutionAdmission(this.permits);
        this.ownedShard.bindWorkClassExecutionRegistry(this.workClasses);
    }

    /** Requires callers to use the registry that owns this executor's queue. */
    void requireWorkClassExecutionRegistry(final WorkClassExecutionRegistry registry) {
        if (workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalArgumentException("Publish Admission executor uses another work-class registry");
        }
    }

    /**
     * Derives the replay-stable descriptor from the exact Claim and the
     * externally-authorized channel identity. Ready Certificate, timing,
     * signing and prerequisite inputs remain explicit.
     */
    public Submission submit(final ClaimRecord claim,
                             final ClaimExecutionAdmission.Reservation reservation,
                             final ChannelResourceIdentityV1 channel,
                             final ReadyCertificateV1 readyCertificate,
                             final TrustedUtcIntervalEvidence decisionTime,
                             final long retryUntilEpochMs,
                             final int signingKeyVersion,
                             final PrivateKey signingKey,
                             final LongSupplier ownerClock) {
        return submit(claim, reservation, deriveDescriptor(Objects.requireNonNull(claim, "Claim"),
                Objects.requireNonNull(channel, "channel")), readyCertificate, decisionTime, retryUntilEpochMs,
                signingKeyVersion, signingKey, ownerClock);
    }

    /**
     * Prepares the exact canonical body/envelope and registers one bounded
     * append action.  The Claim reservation remains active until the source
     * ordered Admission apply or an explicit Claim revoke releases it.
     */
    public Submission submit(final ClaimRecord claim,
                             final ClaimExecutionAdmission.Reservation reservation,
                             final PreparedPublishDescriptorV1 descriptor,
                             final ReadyCertificateV1 readyCertificate,
                             final TrustedUtcIntervalEvidence decisionTime,
                             final long retryUntilEpochMs,
                             final int signingKeyVersion,
                             final PrivateKey signingKey,
                             final LongSupplier ownerClock) {
        permits.requireOwnedReservation(reservation);
        final Request request = Request.prepare(claim, reservation, descriptor, readyCertificate, decisionTime,
                retryUntilEpochMs, signingKeyVersion, signingKey, ownerClock);
        ownedShard.requirePublishAdmissionSubmission(authority, request.claim);
        final byte[] taskBytes = request.taskBytes();
        final WorkClassTask task = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                "publish-admission/" + Bytes.hex(Bytes.sha256(TASK_ID_DOMAIN, taskBytes)), taskBytes.length);
        final Submission submission = new Submission(task, request.mutation, request.reservation);
        workClasses.submit(task, () -> execute(request, submission));
        return submission;
    }

    private static PreparedPublishDescriptorV1 deriveDescriptor(final ClaimRecord claim,
                                                                 final ChannelResourceIdentityV1 channel) {
        final ClaimMaterializationV1 materialization = claim.materialization();
        final ClaimResultBody.ClaimPrecondition precondition = ClaimResultBody.decodePrecondition(
                claim.preconditionBytes());
        final long attemptNo = Math.addExact(Integer.toUnsignedLong(precondition.expectedAdmissionsUsed()), 1);
        final byte[] publishAttemptId = SystemMutation.computePublishAttemptLogicalIdentity(claim.claimId(),
                claim.delayMessageId(), Integer.toUnsignedLong(claim.generation()), attemptNo);
        final AdapterKindV1 adapterKind = materialization.targetResource().kind() ==
                io.nereusstream.delay.protocol.BrokerResourceIdentityV1.Kind.KAFKA
                ? AdapterKindV1.KAFKA : AdapterKindV1.PULSAR;
        final ReservedPublishMetadataV1 reserved = new ReservedPublishMetadataV1(
                claim.delayMessageId().routingId().shardId().routeIncarnation(),
                claim.delayMessageId().routingId().shardId().unsignedPartition(), claim.delayMessageId(),
                Integer.toUnsignedLong(claim.generation()), publishAttemptId,
                materialization.destinationProfile().semanticHash(), materialization.capabilityProfile().semanticHash(),
                materialization.deliverAtEpochMs(), DeliveryMode.MANAGED);
        return new PreparedPublishDescriptorV1(adapterKind, claim.laneId(), claim.laneIncarnation(),
                materialization.destinationProfile(), materialization.capabilityProfile(),
                materialization.targetResource(), materialization.physicalPartition(), channel,
                materialization.messageId(), materialization.generation(), publishAttemptId, attemptNo,
                materialization.payload(), materialization.businessMetadata(), reserved,
                materialization.deliverAtEpochMs(), materialization.expireAtEpochMs(),
                materialization.actionAtEpochMs());
    }

    private void execute(final Request request, final Submission submission) {
        try {
            ownedShard.requirePublishAdmissionAuthoritativelyStrict(authority, request.claim, request.ownerClock);
            final PrerequisiteDecision prerequisite = Objects.requireNonNull(
                    prerequisiteGate.validate(new AdmissionPrerequisite(request.claim, request.mutation,
                            request.body, request.descriptor, request.readyCertificate, request.decisionTime)),
                    "Admission prerequisite decision");
            if (!prerequisite.ready()) {
                submission.complete(AdmissionHandoffResult.deferred(request.mutation, request.reservation,
                        prerequisite.reason()));
                return;
            }
            final ShardLogMutationAppender.AppendOutcome appended = Objects.requireNonNull(
                    appender.append(request.mutation), "Shard Log append outcome");
            switch (appended.disposition()) {
                case PERSISTED -> {
                    ownedShard.requireCurrentShardLogPosition(appended.sourcePosition(), request.mutation.shardId(),
                            appended.sourceConnectionGeneration(), appended.guardAttestationDigest());
                    submission.complete(AdmissionHandoffResult.enqueued(request.mutation, request.reservation,
                            appended.sourcePosition()));
                }
                case DEFINITIVELY_NOT_PERSISTED -> submission.complete(AdmissionHandoffResult.notEnqueued(
                        request.mutation, request.reservation));
                case UNKNOWN -> submission.complete(AdmissionHandoffResult.unknown(request.mutation,
                        request.reservation, null));
            }
        } catch (RuntimeException failure) {
            // A writer exception has no non-persistence proof.  Fence the
            // owner and retain the exact mutation/Claim for evidence-driven
            // recovery; do not create a local retry with new bytes.
            ownedShard.fence();
            submission.complete(AdmissionHandoffResult.unknown(request.mutation, request.reservation, failure));
        } catch (Error failure) {
            ownedShard.fence();
            submission.complete(AdmissionHandoffResult.unknown(request.mutation, request.reservation, failure));
            throw failure;
        }
    }

    @FunctionalInterface
    public interface AdmissionPrerequisiteGate {
        PrerequisiteDecision validate(AdmissionPrerequisite prerequisite);
    }

    public record AdmissionPrerequisite(ClaimRecord claim, SystemMutation mutation,
                                        PublishAdmissionBody body,
                                        PreparedPublishDescriptorV1 descriptor,
                                        ReadyCertificateV1 readyCertificate,
                                        TrustedUtcIntervalEvidence decisionTime) {
        public AdmissionPrerequisite {
            Objects.requireNonNull(claim, "claim");
            Objects.requireNonNull(mutation, "mutation");
            Objects.requireNonNull(body, "body");
            Objects.requireNonNull(descriptor, "descriptor");
            Objects.requireNonNull(readyCertificate, "readyCertificate");
            Objects.requireNonNull(decisionTime, "decisionTime");
        }
    }

    public record PrerequisiteDecision(boolean ready, PrerequisiteRejection reason) {
        public PrerequisiteDecision {
            if (ready == (reason != null)) {
                throw new IllegalArgumentException("Admission prerequisite must be ready or rejected");
            }
        }

        public static PrerequisiteDecision available() {
            return new PrerequisiteDecision(true, null);
        }

        public static PrerequisiteDecision unavailable(final PrerequisiteRejection reason) {
            return new PrerequisiteDecision(false, Objects.requireNonNull(reason, "reason"));
        }
    }

    public enum PrerequisiteRejection {
        PROFILE_OR_CATALOG_UNAVAILABLE,
        PAYLOAD_OR_SERIALIZATION_UNAVAILABLE,
        CHANNEL_OR_CREDENTIAL_UNAVAILABLE
    }

    public enum ResultKind {
        ENQUEUED,
        DEFINITIVELY_NOT_ENQUEUED,
        UNKNOWN,
        PREREQUISITE_UNAVAILABLE
    }

    public record AdmissionHandoffResult(ResultKind kind, SystemMutation mutation,
                                         ClaimExecutionAdmission.Reservation reservation,
                                         io.nereusstream.delay.protocol.SourcePosition sourcePosition,
                                         PrerequisiteRejection prerequisiteRejection,
                                         Throwable failure) {
        public AdmissionHandoffResult {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(mutation, "mutation");
            Objects.requireNonNull(reservation, "reservation");
            if ((kind == ResultKind.ENQUEUED) != (sourcePosition != null)
                    || (kind != ResultKind.ENQUEUED && sourcePosition != null)
                    || (kind == ResultKind.PREREQUISITE_UNAVAILABLE)
                    != (prerequisiteRejection != null)
                    || (kind != ResultKind.PREREQUISITE_UNAVAILABLE && prerequisiteRejection != null)) {
                throw new IllegalArgumentException("invalid Publish Admission handoff result");
            }
            if (kind != ResultKind.UNKNOWN && failure != null) {
                throw new IllegalArgumentException("only UNKNOWN may carry failure evidence");
            }
        }

        private static AdmissionHandoffResult enqueued(final SystemMutation mutation,
                                                        final ClaimExecutionAdmission.Reservation reservation,
                                                        final io.nereusstream.delay.protocol.SourcePosition position) {
            return new AdmissionHandoffResult(ResultKind.ENQUEUED, mutation, reservation, position, null, null);
        }

        private static AdmissionHandoffResult notEnqueued(final SystemMutation mutation,
                                                          final ClaimExecutionAdmission.Reservation reservation) {
            return new AdmissionHandoffResult(ResultKind.DEFINITIVELY_NOT_ENQUEUED, mutation, reservation,
                    null, null, null);
        }

        private static AdmissionHandoffResult unknown(final SystemMutation mutation,
                                                      final ClaimExecutionAdmission.Reservation reservation,
                                                      final Throwable failure) {
            return new AdmissionHandoffResult(ResultKind.UNKNOWN, mutation, reservation, null, null, failure);
        }

        private static AdmissionHandoffResult deferred(final SystemMutation mutation,
                                                       final ClaimExecutionAdmission.Reservation reservation,
                                                       final PrerequisiteRejection rejection) {
            return new AdmissionHandoffResult(ResultKind.PREREQUISITE_UNAVAILABLE, mutation, reservation, null,
                    Objects.requireNonNull(rejection, "rejection"), null);
        }
    }

    public static final class Submission {
        private final WorkClassTask task;
        private final SystemMutation mutation;
        private final ClaimExecutionAdmission.Reservation reservation;
        private volatile AdmissionHandoffResult result;

        private Submission(final WorkClassTask task, final SystemMutation mutation,
                           final ClaimExecutionAdmission.Reservation reservation) {
            this.task = Objects.requireNonNull(task, "task");
            this.mutation = Objects.requireNonNull(mutation, "mutation");
            this.reservation = Objects.requireNonNull(reservation, "reservation");
        }

        public WorkClassTask task() { return task; }
        public SystemMutation mutation() { return mutation; }
        public ClaimExecutionAdmission.Reservation reservation() { return reservation; }
        public Optional<AdmissionHandoffResult> result() { return Optional.ofNullable(result); }

        private synchronized void complete(final AdmissionHandoffResult completed) {
            if (result != null) {
                throw new IllegalStateException("Publish Admission handoff already completed");
            }
            result = Objects.requireNonNull(completed, "completed");
        }
    }

    private static final class Request {
        private final ClaimRecord claim;
        private final ClaimExecutionAdmission.Reservation reservation;
        private final PreparedPublishDescriptorV1 descriptor;
        private final ReadyCertificateV1 readyCertificate;
        private final TrustedUtcIntervalEvidence decisionTime;
        private final SystemMutation mutation;
        private final PublishAdmissionBody body;
        private final LongSupplier ownerClock;

        private Request(final ClaimRecord claim, final ClaimExecutionAdmission.Reservation reservation,
                        final PreparedPublishDescriptorV1 descriptor, final ReadyCertificateV1 readyCertificate,
                        final TrustedUtcIntervalEvidence decisionTime, final SystemMutation mutation,
                        final PublishAdmissionBody body, final LongSupplier ownerClock) {
            this.claim = claim;
            this.reservation = reservation;
            this.descriptor = descriptor;
            this.readyCertificate = readyCertificate;
            this.decisionTime = decisionTime;
            this.mutation = mutation;
            this.body = body;
            this.ownerClock = ownerClock;
        }

        private static Request prepare(final ClaimRecord claim,
                                       final ClaimExecutionAdmission.Reservation reservation,
                                       final PreparedPublishDescriptorV1 descriptor,
                                       final ReadyCertificateV1 readyCertificate,
                                       final TrustedUtcIntervalEvidence decisionTime,
                                       final long retryUntilEpochMs,
                                       final int signingKeyVersion,
                                       final PrivateKey signingKey,
                                       final LongSupplier ownerClock) {
            final ClaimRecord typedClaim = Objects.requireNonNull(claim, "claim");
            final ClaimExecutionAdmission.Reservation typedReservation =
                    Objects.requireNonNull(reservation, "reservation");
            final PreparedPublishDescriptorV1 typedDescriptor = Objects.requireNonNull(descriptor, "descriptor");
            final ReadyCertificateV1 typedCertificate = Objects.requireNonNull(readyCertificate,
                    "readyCertificate");
            final TrustedUtcIntervalEvidence typedDecision = Objects.requireNonNull(decisionTime, "decisionTime");
            Objects.requireNonNull(signingKey, "signingKey");
            Objects.requireNonNull(ownerClock, "ownerClock");
            if (typedReservation.state() != ClaimExecutionAdmission.ReservationState.ACTIVE
                    || !typedReservation.messageId().equals(typedClaim.delayMessageId())
                    || typedReservation.generation() != Integer.toUnsignedLong(typedClaim.generation())
                    || !typedReservation.shardId().equals(typedClaim.delayMessageId().routingId().shardId())
                    || !typedReservation.laneId().equals(typedClaim.laneId())
                    || !Arrays.equals(typedReservation.laneIncarnation(), typedClaim.laneIncarnation())) {
                throw new IllegalArgumentException("Claim reservation does not match Claim identity");
            }
            final ClaimResultBody.ClaimPrecondition precondition = ClaimResultBody.decodePrecondition(
                    typedClaim.preconditionBytes());
            if (!precondition.hasMaterialization()) {
                throw new IllegalArgumentException("Publish Admission requires Claim materialization");
            }
            if (!typedDescriptor.materialization().equals(precondition.materializationValue())
                    || !typedDescriptor.messageId().equals(typedClaim.delayMessageId())
                    || typedDescriptor.generation() != Integer.toUnsignedLong(typedClaim.generation())
                    || !typedDescriptor.destinationLaneId().equals(typedClaim.laneId())
                    || !Arrays.equals(typedDescriptor.laneIncarnation(), typedClaim.laneIncarnation())) {
                throw new IllegalArgumentException("Prepared Publish descriptor does not match Claim");
            }
            final byte[] expectedAttempt = SystemMutation.computePublishAttemptLogicalIdentity(
                    typedClaim.claimId(), typedClaim.delayMessageId(), Integer.toUnsignedLong(typedClaim.generation()),
                    typedDescriptor.attemptNo());
            if (!Arrays.equals(expectedAttempt, typedDescriptor.publishAttemptId())) {
                throw new IllegalArgumentException("Prepared Publish attempt identity is not Claim-derived");
            }
            final OwnerIdentityV1 owner = OwnerIdentityV1.decode(typedClaim.ownerIdentity());
            if (!Arrays.equals(owner.canonicalBytes(), typedCertificate.ownerIdentity())
                    || !Arrays.equals(typedClaim.storeIncarnation(), typedCertificate.storeIncarnation())
                    || !Arrays.equals(typedClaim.laneId().bytes(), typedCertificate.destinationLaneId())
                    || !Arrays.equals(typedClaim.laneIncarnation(), typedCertificate.laneIncarnation())
                    || !Arrays.equals(typedDescriptor.channel().canonicalBytes(), typedCertificate.channel())) {
                throw new IllegalArgumentException("Ready Certificate does not match Claim/descriptor");
            }
            if (typedDecision.earliestEpochMs() < typedDescriptor.actionAtEpochMs()
                    || typedDecision.earliestEpochMs() < typedCertificate.issuedAt().latestEpochMs()
                    || typedDecision.latestEpochMs() >= typedCertificate.validUntilEpochMs()
                    || typedDecision.latestEpochMs() >= typedDescriptor.expireAtEpochMs()
                    || typedDecision.latestEpochMs() >= precondition.claimDeadline()
                    || retryUntilEpochMs < typedDecision.latestEpochMs()
                    || retryUntilEpochMs > Math.min(typedDescriptor.expireAtEpochMs(),
                    Math.min(typedCertificate.validUntilEpochMs(), precondition.claimDeadline()))) {
                throw new IllegalArgumentException("Publish Admission timing is outside the Claim window");
            }
            final PublishAdmissionBody.ChargeVector charge = PublishAdmissionBody.ChargeVector.decodeCanonical(
                    precondition.claimedCharge());
            final byte[] bodyBytes = PublishAdmissionBody.canonicalBytes(
                    typedClaim.delayMessageId().routingId().shardId(), retryUntilEpochMs, owner,
                    typedClaim.storeIncarnation(), typedClaim.claimId(), typedClaim.laneId(),
                    typedClaim.laneIncarnation(), typedClaim.delayMessageId(),
                    Integer.toUnsignedLong(typedClaim.generation()), typedDescriptor.publishAttemptId(),
                    typedDescriptor, charge, typedCertificate, typedDecision, typedClaim.preconditionBytes());
            final byte[] logicalIdentity = SystemMutation.computePublishAttemptLogicalIdentity(typedClaim.claimId(),
                    typedClaim.delayMessageId(), Integer.toUnsignedLong(typedClaim.generation()),
                    typedDescriptor.attemptNo());
            final AuthorIdentity author = AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(),
                    owner.ownerEpoch(), owner.leaseFencingDigest());
            final SystemMutation mutation = SystemMutation.signed(
                    typedClaim.delayMessageId().routingId().shardId(), SystemMutationType.PUBLISH_ADMISSION,
                    retryUntilEpochMs, logicalIdentity, bodyBytes, author.canonicalBytes(), signingKeyVersion,
                    signingKey);
            return new Request(typedClaim, typedReservation, typedDescriptor, typedCertificate, typedDecision,
                    mutation, PublishAdmissionBody.decode(bodyBytes), ownerClock);
        }

        private byte[] taskBytes() {
            return Bytes.concat(mutation.shardId().routeIncarnation().bytes(),
                    Bytes.u32beBits(mutation.shardId().partition()),
                    claim.claimId(), mutation.logicalOperationIdentity(), mutation.encodeFrame(),
                    Bytes.lp32(decisionTime.canonicalBytes()));
        }
    }
}
