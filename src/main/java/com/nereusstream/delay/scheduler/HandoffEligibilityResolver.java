package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyHeadRef;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.semantic.HandoffPolicyAuthority;
import java.util.Objects;

/**
 * Resolves one already-discovered scheduler head against a trusted UTC
 * interval and the current bounded handoff lease.
 *
 * <p>This class is deliberately side-effect free. It does not read Oxia,
 * mutate READY, or contact a destination. A caller can therefore use the
 * same decision during recovery, steady-state refresh, Claim, and Admission.
 * Native eligibility is an optimization: every blocked native result falls
 * back to the ordinary due projection.</p>
 */
public final class HandoffEligibilityResolver {
    private HandoffEligibilityResolver() {}

    public static Decision resolve(final Input input) {
        Objects.requireNonNull(input, "input");
        final long ordinaryAt = Math.max(input.deliverAtEpochMs(), input.retryEligibilityAtEpochMs());
        final HandoffEligibilityReason staticReason = nativeBlockReason(input);
        if (staticReason != HandoffEligibilityReason.ELIGIBLE) {
            return ordinaryDecision(input.trustedTime(), ordinaryAt, staticReason, null, null);
        }

        final HandoffPolicySnapshot snapshot = input.policySnapshot();
        final long candidateAt;
        try {
            candidateAt = Math.subtractExact(input.deliverAtEpochMs(), snapshot.effectiveLeadMs());
        } catch (ArithmeticException overflow) {
            return ordinaryDecision(
                    input.trustedTime(), ordinaryAt, HandoffEligibilityReason.POLICY_OUT_OF_BOUNDS, null, snapshot);
        }
        if (candidateAt < 0) {
            return ordinaryDecision(
                    input.trustedTime(),
                    ordinaryAt,
                    HandoffEligibilityReason.POLICY_OUT_OF_BOUNDS,
                    candidateAt,
                    snapshot);
        }
        final TrustedUtcIntervalEvidence time = input.trustedTime();
        if (time == null) {
            return new Decision(
                    HandoffEligibilityAction.TIME_SAMPLE_REQUIRED,
                    HandoffEligibilityReason.ELIGIBLE,
                    candidateAt,
                    candidateAt,
                    headRef(snapshot),
                    snapshot);
        }
        final long earliest = time.earliestEpochMs();
        final long latest = time.latestEpochMs();
        if (latest < candidateAt) {
            return wait(candidateAt, HandoffEligibilityReason.ELIGIBLE, snapshot);
        }
        if (earliest < candidateAt) {
            return new Decision(
                    HandoffEligibilityAction.TIME_SAMPLE_REQUIRED,
                    HandoffEligibilityReason.ELIGIBLE,
                    candidateAt,
                    candidateAt,
                    headRef(snapshot),
                    snapshot);
        }
        if (latest < ordinaryAt) {
            return new Decision(
                    HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE,
                    HandoffEligibilityReason.ELIGIBLE,
                    candidateAt,
                    candidateAt,
                    headRef(snapshot),
                    snapshot);
        }
        if (earliest < ordinaryAt) {
            return new Decision(
                    HandoffEligibilityAction.TIME_SAMPLE_REQUIRED,
                    HandoffEligibilityReason.ELIGIBLE,
                    candidateAt,
                    ordinaryAt,
                    headRef(snapshot),
                    snapshot);
        }
        return new Decision(
                HandoffEligibilityAction.ORDINARY_DUE,
                HandoffEligibilityReason.ELIGIBLE,
                candidateAt,
                ordinaryAt,
                headRef(snapshot),
                snapshot);
    }

    /**
     * Resolves against an already-read Oxia publication. The publication is
     * supplied by the caller so this pure resolver never rereads or relies on
     * a mutable current value during Claim/replay.
     */
    public static Decision resolve(final Input input, final HandoffPolicyAuthority.Publication publication) {
        Objects.requireNonNull(input, "input");
        final HandoffPolicyAuthority.Publication observed = Objects.requireNonNull(publication, "publication");
        final HandoffPolicySnapshot snapshot = observed.head().snapshot();
        if (input.policySnapshot() != null && !input.policySnapshot().equals(snapshot)) {
            throw new IllegalArgumentException("handoff policy publication differs from the frozen snapshot");
        }
        final Input bound = new Input(
                input.adapterKind(),
                input.policy(),
                input.orderingMode(),
                input.initialAttempt(),
                input.capabilityAvailable(),
                input.destinationProfile(),
                input.capabilityProfile(),
                snapshot,
                input.deliverAtEpochMs(),
                input.retryEligibilityAtEpochMs(),
                input.trustedTime());
        return resolve(bound).withPolicyHeadRef(observed.head().ref(observed.oxiaVersion()));
    }

    /** Convenience entry point for callers that do not need an Input value. */
    public static Decision resolve(
            final AdapterKind adapterKind,
            final NativeDeliveryPolicy policy,
            final OrderingMode orderingMode,
            final boolean initialAttempt,
            final boolean capabilityAvailable,
            final DestinationProfileSemantic destinationProfile,
            final DeliveryCapabilitySemantic capabilityProfile,
            final HandoffPolicySnapshot policySnapshot,
            final long deliverAtEpochMs,
            final long retryEligibilityAtEpochMs,
            final TrustedUtcIntervalEvidence trustedTime) {
        return resolve(new Input(
                adapterKind,
                policy,
                orderingMode,
                initialAttempt,
                capabilityAvailable,
                destinationProfile,
                capabilityProfile,
                policySnapshot,
                deliverAtEpochMs,
                retryEligibilityAtEpochMs,
                trustedTime));
    }

    private static HandoffEligibilityReason nativeBlockReason(final Input input) {
        if (input.policy() == NativeDeliveryPolicy.FORBID) {
            return HandoffEligibilityReason.FORBIDDEN;
        }
        if (input.adapterKind() != AdapterKind.PULSAR
                || input.destinationProfile() == null
                || input.capabilityProfile() == null
                || input.destinationProfile().adapterKind() != AdapterKind.PULSAR
                || input.capabilityProfile().adapterKind() != AdapterKind.PULSAR) {
            return HandoffEligibilityReason.WRONG_ADAPTER;
        }
        if (input.orderingMode() != OrderingMode.BEST_EFFORT) {
            return HandoffEligibilityReason.ORDERING_UNAVAILABLE;
        }
        if (!input.initialAttempt()) {
            return HandoffEligibilityReason.NOT_INITIAL_ATTEMPT;
        }
        if (!input.capabilityAvailable()) {
            return HandoffEligibilityReason.CAPABILITY_UNAVAILABLE;
        }
        final HandoffPolicySnapshot snapshot = input.policySnapshot();
        if (snapshot == null) {
            return HandoffEligibilityReason.CAPABILITY_UNAVAILABLE;
        }
        if (snapshot.mode() == HandoffPolicyMode.DISABLED) {
            return HandoffEligibilityReason.POLICY_DISABLED;
        }
        if (snapshot.mode() == HandoffPolicyMode.SHADOW) {
            return HandoffEligibilityReason.POLICY_SHADOW;
        }
        if (snapshot.effectiveLeadMs() <= 0
                || snapshot.effectiveLeadMs() > input.destinationProfile().handoffLeadMs()
                || !snapshot.allows(HandoffPath.MANAGED_HANDOFF)
                || input.destinationProfile().handoffLeadMs() <= 0
                || input.deliverAtEpochMs() < snapshot.effectiveLeadMs()) {
            return HandoffEligibilityReason.POLICY_OUT_OF_BOUNDS;
        }
        if (input.trustedTime() != null) {
            if (input.trustedTime().latestEpochMs() < snapshot.validFromEpochMs()) {
                return HandoffEligibilityReason.POLICY_NOT_YET_VALID;
            }
            if (input.trustedTime().earliestEpochMs() >= snapshot.validUntilEpochMs()) {
                return HandoffEligibilityReason.POLICY_EXPIRED;
            }
        }
        return HandoffEligibilityReason.ELIGIBLE;
    }

    private static Decision ordinaryDecision(
            final TrustedUtcIntervalEvidence time,
            final long ordinaryAt,
            final HandoffEligibilityReason reason,
            final Long candidateAt,
            final HandoffPolicySnapshot snapshot) {
        if (time == null) {
            return new Decision(
                    HandoffEligibilityAction.TIME_SAMPLE_REQUIRED,
                    reason,
                    candidateAt,
                    ordinaryAt,
                    headRef(snapshot),
                    snapshot);
        }
        if (ordinaryAt <= time.earliestEpochMs()) {
            return new Decision(
                    HandoffEligibilityAction.ORDINARY_DUE,
                    reason,
                    candidateAt,
                    ordinaryAt,
                    headRef(snapshot),
                    snapshot);
        }
        if (ordinaryAt <= time.latestEpochMs()) {
            return new Decision(
                    HandoffEligibilityAction.TIME_SAMPLE_REQUIRED,
                    reason,
                    candidateAt,
                    ordinaryAt,
                    headRef(snapshot),
                    snapshot);
        }
        return wait(ordinaryAt, reason, snapshot, candidateAt);
    }

    private static Decision wait(
            final long boundary, final HandoffEligibilityReason reason, final HandoffPolicySnapshot snapshot) {
        return wait(boundary, reason, snapshot, null);
    }

    private static Decision wait(
            final long boundary,
            final HandoffEligibilityReason reason,
            final HandoffPolicySnapshot snapshot,
            final Long candidateAt) {
        final long persistentWake = snapshot == null ? boundary : boundary;
        return new Decision(
                HandoffEligibilityAction.WAIT_UNTIL,
                reason,
                candidateAt,
                boundary,
                headRef(snapshot),
                snapshot,
                persistentWake);
    }

    private static HandoffPolicyHeadRef headRef(final HandoffPolicySnapshot snapshot) {
        return snapshot == null ? null : snapshot.headRef();
    }

    public record Input(
            AdapterKind adapterKind,
            NativeDeliveryPolicy policy,
            OrderingMode orderingMode,
            boolean initialAttempt,
            boolean capabilityAvailable,
            DestinationProfileSemantic destinationProfile,
            DeliveryCapabilitySemantic capabilityProfile,
            HandoffPolicySnapshot policySnapshot,
            long deliverAtEpochMs,
            long retryEligibilityAtEpochMs,
            TrustedUtcIntervalEvidence trustedTime) {
        public Input {
            Objects.requireNonNull(adapterKind, "adapterKind");
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(orderingMode, "orderingMode");
            if (deliverAtEpochMs < 0 || retryEligibilityAtEpochMs < 0) {
                throw new IllegalArgumentException("scheduler timestamps must be non-negative");
            }
            if (trustedTime != null && trustedTime.latestEpochMs() < trustedTime.earliestEpochMs()) {
                throw new IllegalArgumentException("trusted time interval is inverted");
            }
        }
    }

    public record Decision(
            HandoffEligibilityAction action,
            HandoffEligibilityReason reason,
            Long candidateAtEpochMs,
            long effectiveEligibleAtEpochMs,
            HandoffPolicyHeadRef policyHeadRef,
            HandoffPolicySnapshot policySnapshot,
            long persistentWakeAtEpochMs) {
        private Decision(
                final HandoffEligibilityAction action,
                final HandoffEligibilityReason reason,
                final Long candidateAtEpochMs,
                final long effectiveEligibleAtEpochMs,
                final HandoffPolicyHeadRef policyHeadRef,
                final HandoffPolicySnapshot policySnapshot) {
            this(
                    action,
                    reason,
                    candidateAtEpochMs,
                    effectiveEligibleAtEpochMs,
                    policyHeadRef,
                    policySnapshot,
                    effectiveEligibleAtEpochMs);
        }

        public Decision {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(reason, "reason");
            if (effectiveEligibleAtEpochMs < 0 || persistentWakeAtEpochMs < 0) {
                throw new IllegalArgumentException("eligibility times must be non-negative");
            }
            if (candidateAtEpochMs != null && candidateAtEpochMs < 0) {
                throw new IllegalArgumentException("candidateAtEpochMs must be non-negative");
            }
            if (action == HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE
                    && (candidateAtEpochMs == null || policySnapshot == null)) {
                throw new IllegalArgumentException("native candidate must carry its frozen policy");
            }
            if (action == HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE
                    && reason != HandoffEligibilityReason.ELIGIBLE) {
                throw new IllegalArgumentException("blocked native policy cannot become a candidate");
            }
            if (action == HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE
                    && policySnapshot.mode() == HandoffPolicyMode.SHADOW) {
                throw new IllegalArgumentException("SHADOW cannot produce a native candidate");
            }
        }

        public boolean isNativeCandidate() {
            return action == HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE;
        }

        public long candidateAtOr(final long fallback) {
            return candidateAtEpochMs == null ? fallback : candidateAtEpochMs;
        }

        private Decision withPolicyHeadRef(final HandoffPolicyHeadRef ref) {
            return new Decision(
                    action,
                    reason,
                    candidateAtEpochMs,
                    effectiveEligibleAtEpochMs,
                    ref,
                    policySnapshot,
                    persistentWakeAtEpochMs);
        }
    }
}
