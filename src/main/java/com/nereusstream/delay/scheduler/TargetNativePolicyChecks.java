package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyHeadRef;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetNativePolicySnapshot;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CommandResolutionException;
import com.nereusstream.delay.semantic.TargetNativePolicyAuthority;
import com.nereusstream.delay.semantic.TargetNativePolicyTrust;
import java.util.Objects;

/** B3 policy contracts only. Source/Owner/Store/Message fences and actual Claim/Admission commits belong to C1/C2. */
public final class TargetNativePolicyChecks {
    private TargetNativePolicyChecks() {}

    /** Stable diagnostic numbers; these are optimization outcomes, not source command rejection codes. */
    public enum Reason {
        ELIGIBLE(1),
        FORBIDDEN(2),
        CAPABILITY_UNAVAILABLE(3),
        FIXED_CAP_DISABLED(4),
        PINNED_CAP_TOO_SMALL(5),
        NO_COMMON_SCOPE(6),
        NO_COMMON_MEMBERSHIP(7),
        NOT_INITIAL_ATTEMPT(8),
        POLICY_UNAVAILABLE(9),
        POLICY_UNTRUSTED(10),
        POLICY_DISABLED(11),
        POLICY_SHADOW(12),
        POLICY_NOT_YET_VALID(13),
        POLICY_EXPIRED(14),
        TIME_REQUIRED(15),
        BEFORE_NATIVE_BOUNDARY(16),
        ORDINARY_DUE(17),
        TIMESTAMP_BELOW_CAP(18);
        private final int wire;

        Reason(final int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }
    }

    public enum Action {
        ORDINARY_DUE,
        WAIT_UNTIL,
        TIME_SAMPLE_REQUIRED,
        NATIVE_CANDIDATE
    }

    public record Decision(
            Action action,
            Reason reason,
            long wakeAtEpochMs,
            Long nativeActionAtEpochMs,
            HandoffPolicyHeadRef policyHeadRef,
            boolean shadowWouldBeEligible) {
        private Decision(
                final Action action,
                final Reason reason,
                final long wakeAtEpochMs,
                final Long nativeActionAtEpochMs,
                final HandoffPolicyHeadRef policyHeadRef) {
            this(action, reason, wakeAtEpochMs, nativeActionAtEpochMs, policyHeadRef, false);
        }

        public Decision {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(reason, "reason");
            if (shadowWouldBeEligible && reason != Reason.POLICY_SHADOW
                    || wakeAtEpochMs < 0
                    || nativeActionAtEpochMs != null && nativeActionAtEpochMs < 0
                    || action == Action.NATIVE_CANDIDATE
                            && (reason != Reason.ELIGIBLE || policyHeadRef == null || nativeActionAtEpochMs == null)) {
                throw new IllegalArgumentException("invalid Target Native decision");
            }
        }
    }

    /**
     * Classifies a prospective first binding. B2 first-binding authorization must already have passed in the same
     * protected source snapshot. An ineligible prospective native ref must not be written to the final binding.
     * No current policy or private Profile credential/policy switch is read here; member approval is historical.
     */
    public static Reason firstBinding(
            final TargetScheduleBinding binding,
            final byte[] authenticatedTenant,
            final TargetMembershipGrant grant,
            final CanonicalTargetPartition physical,
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capability,
            final TargetQueueState queue,
            final TargetNativePolicyScope scope,
            final TargetNativePolicyTrust trust) {
        grant.requireBindingProjection(binding, authenticatedTenant);
        binding.requireReferences(
                physical, destination, capability, grant.required(), grant.offered(), grant.controls());
        binding.requireQueueProjection(queue);
        final var intent = binding.intent();
        if (intent.nativeDeliveryPolicy() != NativeDeliveryPolicy.FORBID
                && intent.orderingMode() != OrderingMode.BEST_EFFORT) {
            throw new CommandResolutionException(
                    StableCode.ORDERING_CAPABILITY_UNAVAILABLE, "strict Native delivery remains unsupported");
        }
        // A referenced durable object must resolve exactly even when another static condition would disable Native.
        if (binding.nativePolicyScopeRef() != null) {
            if (scope == null) {
                throw new IllegalArgumentException("referenced Target Native scope is missing");
            }
            scope.requireBinding(binding);
            scope.requireQueue(queue);
            scope.requireReferences(physical, grant.offered(), grant.controls());
        }
        if (intent.legacyPolicyDefault() || !intent.nativeDeliveryPolicy().allowsManagedHandoff()) {
            return Reason.FORBIDDEN;
        }
        final var dest = (DestinationProfileSemantic) destination.body();
        if (physical.resource().kind() != BrokerResourceIdentity.Kind.PULSAR
                || !TimingCapability.includes(
                        grant.required().capability().timingCapabilityBits(),
                        TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF)) {
            return Reason.CAPABILITY_UNAVAILABLE;
        }
        if (queue.nativeIndexLeadCapMs() == 0) {
            return Reason.FIXED_CAP_DISABLED;
        }
        if (dest.handoffLeadMs() < queue.nativeIndexLeadCapMs()) {
            return Reason.PINNED_CAP_TOO_SMALL;
        }
        if (intent.deliverAtEpochMs() < queue.nativeIndexLeadCapMs()) {
            return Reason.TIMESTAMP_BELOW_CAP;
        }
        if (binding.nativePolicyScopeRef() == null) {
            return Reason.NO_COMMON_SCOPE;
        }
        final var approval = Objects.requireNonNull(trust, "trust")
                .member(grant.digest(), scope.digest(), binding.bindingSource())
                .orElse(null);
        if (approval == null) {
            return Reason.NO_COMMON_MEMBERSHIP;
        }
        if (!grant.equals(approval.grant()) || !scope.equals(approval.scope())) {
            throw new IllegalArgumentException("common Native member approval differs from full grant/scope");
        }
        return approval.allowsFirstBinding(binding.bindingSource()) ? Reason.ELIGIBLE : Reason.NO_COMMON_MEMBERSHIP;
    }

    /** Resolves one integrity-checked Native head; the ordinary head is scheduled independently. */
    public static Decision resolve(
            final TargetNativePolicyScope scope,
            final boolean initialAttempt,
            final long deliverAt,
            final long retryEligibilityAt,
            final TargetNativePolicyAuthority.Publication publication,
            final TargetNativePolicyTrust trust,
            final SourcePosition at,
            final TrustedUtcIntervalEvidence time) {
        if (deliverAt < 0 || retryEligibilityAt < 0) {
            throw new IllegalArgumentException("negative Target candidate time");
        }
        final long ordinaryAt = Math.max(deliverAt, retryEligibilityAt);
        if (time != null) {
            TargetNativePolicySnapshot.requireBoundedTime(time);
            if (time.earliestEpochMs() >= ordinaryAt) {
                return ordinary(time, ordinaryAt, Reason.ORDINARY_DUE);
            }
        }
        if (!initialAttempt) {
            return ordinary(time, ordinaryAt, Reason.NOT_INITIAL_ATTEMPT);
        }
        if (publication == null) {
            return ordinary(time, ordinaryAt, Reason.POLICY_UNAVAILABLE);
        }
        final var snapshot = publication.head().snapshot();
        try {
            publication.requireScope(scope);
            Objects.requireNonNull(trust, "trust").requireTrusted(scope, snapshot, at);
        } catch (RuntimeException unavailableOrUntrusted) {
            return ordinary(time, ordinaryAt, Reason.POLICY_UNTRUSTED);
        }
        if (snapshot.mode() == HandoffPolicyMode.DISABLED) {
            return ordinary(time, ordinaryAt, Reason.POLICY_DISABLED);
        }
        if (snapshot.mode() == HandoffPolicyMode.SHADOW) {
            final var ordinary = ordinary(time, ordinaryAt, Reason.POLICY_SHADOW);
            final Long candidate = deliverAt < scope.fixedLeadCapMs() ? null : deliverAt - snapshot.effectiveLeadMs();
            final boolean wouldBeEligible = candidate != null
                    && time != null
                    && time.earliestEpochMs() >= candidate
                    && time.latestEpochMs() < ordinaryAt
                    && time.earliestEpochMs() >= snapshot.validFromEpochMs()
                    && time.latestEpochMs() < snapshot.validUntilEpochMs();
            return new Decision(
                    ordinary.action(),
                    ordinary.reason(),
                    ordinary.wakeAtEpochMs(),
                    candidate,
                    publication.head().ref(publication.revision()),
                    wouldBeEligible);
        }
        if (deliverAt < scope.fixedLeadCapMs()) {
            return ordinary(time, ordinaryAt, Reason.TIMESTAMP_BELOW_CAP);
        }
        final long actionAt = deliverAt - snapshot.effectiveLeadMs();
        final var ref = publication.head().ref(publication.revision());
        if (time == null) {
            return new Decision(Action.TIME_SAMPLE_REQUIRED, Reason.TIME_REQUIRED, actionAt, actionAt, ref);
        }
        if (time.earliestEpochMs() >= snapshot.validUntilEpochMs()) {
            return ordinary(time, ordinaryAt, Reason.POLICY_EXPIRED);
        }
        if (time.latestEpochMs() < snapshot.validFromEpochMs()) {
            return new Decision(
                    Action.WAIT_UNTIL,
                    Reason.POLICY_NOT_YET_VALID,
                    Math.min(ordinaryAt, snapshot.validFromEpochMs()),
                    actionAt,
                    ref);
        }
        if (time.earliestEpochMs() < snapshot.validFromEpochMs()
                || time.latestEpochMs() >= snapshot.validUntilEpochMs()
                || time.earliestEpochMs() < actionAt && time.latestEpochMs() >= actionAt
                || time.earliestEpochMs() < ordinaryAt && time.latestEpochMs() >= ordinaryAt) {
            return new Decision(Action.TIME_SAMPLE_REQUIRED, Reason.TIME_REQUIRED, afterInterval(time), actionAt, ref);
        }
        if (time.latestEpochMs() < actionAt) {
            return new Decision(
                    Action.WAIT_UNTIL,
                    Reason.BEFORE_NATIVE_BOUNDARY,
                    Math.min(actionAt, snapshot.validUntilEpochMs()),
                    actionAt,
                    ref);
        }
        return new Decision(Action.NATIVE_CANDIDATE, Reason.ELIGIBLE, actionAt, actionAt, ref);
    }

    private static Decision ordinary(final TrustedUtcIntervalEvidence time, final long at, final Reason reason) {
        final Action action = time == null || time.earliestEpochMs() < at && time.latestEpochMs() >= at
                ? Action.TIME_SAMPLE_REQUIRED
                : time.earliestEpochMs() >= at ? Action.ORDINARY_DUE : Action.WAIT_UNTIL;
        return new Decision(
                action,
                reason,
                action == Action.TIME_SAMPLE_REQUIRED && time != null ? afterInterval(time) : at,
                null,
                null);
    }

    private static long afterInterval(final TrustedUtcIntervalEvidence time) {
        if (time.latestEpochMs() == Long.MAX_VALUE) {
            throw new IllegalArgumentException("trusted Target Native time cannot advance beyond Long.MAX_VALUE");
        }
        return time.latestEpochMs() + 1;
    }

    /** Exact current publication is reread across trust/time validation before an Admission may be constructed. */
    public static TargetNativePolicySnapshot freezeCurrent(
            final TargetNativePolicyScope scope,
            final HandoffPolicyHeadRef claimRef,
            final long deliverAt,
            final long actionAt,
            final TargetNativePolicyAuthority policies,
            final TargetNativePolicyTrust trust,
            final SourcePosition at,
            final TrustedUtcIntervalEvidence time) {
        final var first = policies.requireCurrent(scope.digest());
        first.requireScope(scope);
        if (!first.head().ref(first.revision()).equals(claimRef)) {
            throw new IllegalArgumentException("Target Native Claim publication is no longer current");
        }
        final var snapshot = first.head().snapshot();
        requireFrozen(scope, snapshot, deliverAt, actionAt, trust, at, time, true);
        if (!first.sameHead(policies.requireCurrent(scope.digest()))) {
            throw new IllegalStateException("Target Native publication changed during Admission validation");
        }
        return snapshot;
    }

    /**
     * Before physical ownership, use the full Admission-frozen snapshot and its historical source trust position.
     * After ownership starts, preserve UNKNOWN/Journal obligations; this lease gate cannot manufacture NOT_SENT.
     */
    public static void requireFrozen(
            final TargetNativePolicyScope scope,
            final TargetNativePolicySnapshot snapshot,
            final long deliverAt,
            final long actionAt,
            final TargetNativePolicyTrust trust,
            final SourcePosition admissionPosition,
            final TrustedUtcIntervalEvidence time,
            final boolean beforeAdmission) {
        TargetNativePolicySnapshot.requireBoundedTime(Objects.requireNonNull(time, "time"));
        trust.requireTrusted(scope, snapshot, admissionPosition);
        snapshot.requireActiveAt(time);
        if (snapshot.mode() != HandoffPolicyMode.ENABLED
                || !snapshot.allows(HandoffPath.MANAGED_HANDOFF)
                || deliverAt < scope.fixedLeadCapMs()
                || actionAt != deliverAt - snapshot.effectiveLeadMs()
                || actionAt < 0
                || time.earliestEpochMs() < actionAt
                || beforeAdmission && time.latestEpochMs() >= deliverAt) {
            throw new IllegalArgumentException("Target Native frozen lease/action interval is not valid");
        }
    }
}
