package com.nereusstream.delay.scheduler;

import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.HandoffPolicyHeadRef;
import java.util.Objects;

/** Bounded scheduler snapshot; a visit never waits for a Broker future. */
public record ScheduleWorkItem(
        DestinationLaneId laneId,
        DelayMessageId messageId,
        int generation,
        long persistentWakeAtEpochMs,
        long effectiveEligibleAtEpochMs,
        CandidateKind candidateKind,
        HandoffPolicyHeadRef policyHeadRef,
        long accountedBytes) {
    public enum CandidateKind {
        ORDINARY,
        MANAGED_NATIVE
    }

    /** Compatibility constructor for an ordinary scheduler head. */
    public ScheduleWorkItem(
            final DestinationLaneId laneId,
            final DelayMessageId messageId,
            final int generation,
            final long eligibleAtEpochMs,
            final long accountedBytes) {
        this(
                laneId,
                messageId,
                generation,
                eligibleAtEpochMs,
                eligibleAtEpochMs,
                CandidateKind.ORDINARY,
                null,
                accountedBytes);
    }

    /** Constructs an ordinary or managed-native scheduler head. */
    public ScheduleWorkItem(
            final DestinationLaneId laneId,
            final DelayMessageId messageId,
            final int generation,
            final long persistentWakeAtEpochMs,
            final long effectiveEligibleAtEpochMs,
            final CandidateKind candidateKind,
            final HandoffPolicyHeadRef policyHeadRef,
            final long accountedBytes) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.generation = generation;
        this.persistentWakeAtEpochMs = nonNegative(persistentWakeAtEpochMs, "persistentWakeAtEpochMs");
        this.effectiveEligibleAtEpochMs = nonNegative(effectiveEligibleAtEpochMs, "effectiveEligibleAtEpochMs");
        this.candidateKind = Objects.requireNonNull(candidateKind, "candidateKind");
        this.policyHeadRef = policyHeadRef;
        if (candidateKind == CandidateKind.ORDINARY && policyHeadRef != null) {
            throw new IllegalArgumentException("ordinary work cannot carry a handoff policy head");
        }
        if (candidateKind == CandidateKind.MANAGED_NATIVE && policyHeadRef == null) {
            throw new IllegalArgumentException("native work requires a handoff policy head");
        }
        if (accountedBytes <= 0) {
            throw new IllegalArgumentException("accountedBytes must be positive");
        }
        this.accountedBytes = accountedBytes;
    }

    /** Existing scheduler callers use effective process-local eligibility. */
    public long eligibleAtEpochMs() {
        return effectiveEligibleAtEpochMs;
    }

    public boolean isNativeCandidate() {
        return candidateKind == CandidateKind.MANAGED_NATIVE;
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }
}
