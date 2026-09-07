package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Pure bounded registration planning. No decision proves authority or writes a binding/source result. */
public final class TargetDomainRegistration {
    private TargetDomainRegistration() {}

    public record Requirements(
            TargetDispatchCompatibility dispatch, TargetControlScope controls, byte[] nativePolicyScope) {
        public Requirements {
            Objects.requireNonNull(dispatch, "dispatch");
            Objects.requireNonNull(controls, "controls");
            if (!dispatch.target().equals(controls.target())) {
                throw new IllegalArgumentException("Target execution/control requirements disagree on target");
            }
            if (nativePolicyScope != null) {
                Bytes.requireLength(nativePolicyScope, 32, "nativePolicyScope");
                if (Arrays.equals(nativePolicyScope, new byte[32])
                        || dispatch.capability().adapterKind() != AdapterKind.PULSAR
                        || !TimingCapability.includes(
                                dispatch.capability().timingCapabilityBits(),
                                TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF)) {
                    throw new IllegalArgumentException(
                            "Target Native scope requires the declared Pulsar handoff capability");
                }
                nativePolicyScope = Bytes.copy(nativePolicyScope);
            }
        }

        @Override
        public byte[] nativePolicyScope() {
            return nativePolicyScope == null ? null : Bytes.copy(nativePolicyScope);
        }

        public boolean sameRequirements(final Requirements other) {
            return dispatch.equals(other.dispatch)
                    && controls.equals(other.controls)
                    && Arrays.equals(nativePolicyScope, other.nativePolicyScope);
        }

        /** A binding that requests no Native work may join a domain offering an additional Native path. */
        public boolean canServe(final Requirements requested) {
            return dispatch.canServe(requested.dispatch)
                    && controls.equals(requested.controls)
                    && (requested.nativePolicyScope == null
                            || Arrays.equals(nativePolicyScope, requested.nativePolicyScope));
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Requirements that && sameRequirements(that);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dispatch, controls, Arrays.hashCode(nativePolicyScope));
        }
    }

    public record BoundDomain(TargetKeyCodec.Domain domain, Requirements requirements) {
        public BoundDomain {
            Objects.requireNonNull(domain, "domain");
            Objects.requireNonNull(requirements, "requirements");
        }
    }

    public enum Action {
        REUSE_DOMAIN,
        BIND_DOMAIN,
        REJECT
    }

    public record Plan(
            Action action,
            StableCode result,
            TargetKeyCodec.Domain domain,
            long expectedQueueRevision,
            byte[] expectedQueueDigest) {
        public Plan {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(result, "result");
            Bytes.requireLength(expectedQueueDigest, 32, "expectedQueueDigest");
            if (expectedQueueRevision == 0
                    || (action == Action.REJECT) != (domain == null)
                    || (action != Action.REJECT) != (result == StableCode.OK)) {
                throw new IllegalArgumentException("invalid Target registration plan");
            }
            expectedQueueDigest = Bytes.copy(expectedQueueDigest);
        }

        @Override
        public byte[] expectedQueueDigest() {
            return Bytes.copy(expectedQueueDigest);
        }

        public void requireQueueSnapshot(final TargetQueueState queue) {
            if (expectedQueueRevision != queue.headRevision()
                    || !Bytes.constantTimeEquals(expectedQueueDigest, queue.digest())) {
                throw new IllegalArgumentException("Target registration queue snapshot changed");
            }
        }
    }

    /**
     * Bound contracts must cover every non-VACANT slot in slot order, including DRAINING ones.
     * The base runtime activates one slot; a larger explicit bound is usable only after the E2 runtime gate.
     */
    public static Plan plan(
            final CanonicalTargetPartition identity,
            final TargetQueueState queue,
            final ShardId shard,
            final List<BoundDomain> bound,
            final Requirements candidate,
            final int activatedMaxSlots) {
        Objects.requireNonNull(queue, "queue");
        Objects.requireNonNull(shard, "shard");
        Objects.requireNonNull(candidate, "candidate");
        if (!queue.targetId().equals(identity.id())
                || activatedMaxSlots < 1
                || activatedMaxSlots > TargetQueueState.MAX_DOMAIN_SLOTS
                || queue.domains().size() > activatedMaxSlots
                || bound.size() > activatedMaxSlots) {
            throw new IllegalArgumentException("Target registration identity or activated slot bound mismatch");
        }
        requireCandidate(identity, shard, candidate);
        final List<BoundDomain> contracts = List.copyOf(bound);
        if (contracts.size() > activatedMaxSlots) {
            throw new IllegalArgumentException("Target registration contract count exceeds activated bound");
        }
        int contractIndex = 0;
        for (TargetDomainState summary : queue.domains()) {
            if (summary.lifecycle() == TargetDomainState.Lifecycle.VACANT) {
                continue;
            }
            if (contractIndex == contracts.size()) {
                throw new IllegalArgumentException("Target registration is missing a stored domain contract");
            }
            final BoundDomain stored = contracts.get(contractIndex++);
            requireCandidate(identity, shard, stored.requirements());
            if (!summary.domain().equals(stored.domain())
                    || !Bytes.constantTimeEquals(
                            summary.dispatchCompatibilityRef(), stored.requirements.dispatch.digest())
                    || !Bytes.constantTimeEquals(summary.controlScopeRef(), stored.requirements.controls.digest())
                    || !Arrays.equals(summary.nativePolicyScopeRef(), stored.requirements.nativePolicyScope)) {
                throw new IllegalArgumentException("Target registration stored ref/slot projection mismatch");
            }
            for (int n = 0; n < contractIndex - 1; n++) {
                if (stored.requirements.sameRequirements(contracts.get(n).requirements)) {
                    throw new IllegalArgumentException(
                            "Target registration repeats an interchangeable execution domain");
                }
            }
        }
        if (contractIndex != contracts.size()) {
            throw new IllegalArgumentException("Target registration has extra stored contracts");
        }
        if (queue.admissionState() == TargetQueueState.AdmissionState.CLOSED) {
            return reject(queue, StableCode.TARGET_CLOSED);
        }
        boolean drainingMatch = false;
        for (BoundDomain stored : contracts) {
            if (stored.requirements.canServe(candidate)) {
                if (queue.domains().get(stored.domain.slot()).lifecycle() == TargetDomainState.Lifecycle.ACTIVE) {
                    return result(queue, Action.REUSE_DOMAIN, StableCode.OK, stored.domain);
                }
                drainingMatch = true;
            }
        }
        if (drainingMatch) {
            return reject(queue, StableCode.TARGET_EXECUTION_DOMAIN_DRAINING);
        }
        boolean exhaustedVacancy = false;
        for (TargetDomainState summary : queue.domains()) {
            if (summary.lifecycle() == TargetDomainState.Lifecycle.VACANT) {
                if (summary.domain().generation() == -1L) {
                    exhaustedVacancy = true;
                } else {
                    return result(
                            queue,
                            Action.BIND_DOMAIN,
                            StableCode.OK,
                            new TargetKeyCodec.Domain(
                                    summary.domain().slot(), summary.domain().generation() + 1));
                }
            }
        }
        if (queue.domains().size() < activatedMaxSlots) {
            return result(
                    queue,
                    Action.BIND_DOMAIN,
                    StableCode.OK,
                    new TargetKeyCodec.Domain(queue.domains().size(), 1));
        }
        return reject(
                queue,
                exhaustedVacancy
                        ? StableCode.TARGET_EXECUTION_DOMAIN_GENERATION_EXHAUSTED
                        : activatedMaxSlots == 1
                                ? StableCode.TARGET_BINDING_INCOMPATIBLE
                                : StableCode.TARGET_EXECUTION_DOMAIN_LIMIT_EXCEEDED);
    }

    private static void requireCandidate(
            final CanonicalTargetPartition identity, final ShardId shard, final Requirements requirements) {
        requirements.dispatch.requireTargetProjection(identity);
        if (!shard.equals(requirements.controls.sourceShard())) {
            throw new IllegalArgumentException("Target registration control scope belongs to another source Shard");
        }
    }

    private static Plan reject(final TargetQueueState queue, final StableCode result) {
        return result(queue, Action.REJECT, result, null);
    }

    private static Plan result(
            final TargetQueueState queue,
            final Action action,
            final StableCode result,
            final TargetKeyCodec.Domain domain) {
        return new Plan(action, result, domain, queue.headRevision(), queue.digest());
    }
}
