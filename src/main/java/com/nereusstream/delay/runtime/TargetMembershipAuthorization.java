package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ProfileAcceptance;
import com.nereusstream.delay.protocol.ProfileBindingControlState;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import java.util.Objects;

/** Static, source-snapshot authorization checks. A returned OK is not an Owner or send permit. */
public final class TargetMembershipAuthorization {
    private TargetMembershipAuthorization() {}

    public static StableCode firstBinding(
            final TargetMembershipAuthority authority,
            final TargetMembershipGrant grant,
            final TargetScheduleBinding binding,
            final byte[] authenticatedTenantScope,
            final ProfileBindingControlState profileState,
            final CanonicalTargetPartition physical,
            final ProfileSemanticEnvelope destination,
            final ProfileSemanticEnvelope capability) {
        Objects.requireNonNull(authority, "authority");
        final var applied = authority.resolve(grant.digest());
        if (applied == null) {
            return StableCode.UNAUTHORIZED;
        }
        if (!grant.equals(applied.grant())) {
            throw new IllegalArgumentException("Target membership authority returned a different full grant");
        }
        if (!applied.allowsFirstBinding(binding.bindingSource())
                || !Bytes.constantTimeEquals(grant.tenantScope(), authenticatedTenantScope)) {
            return StableCode.UNAUTHORIZED;
        }
        grant.requireBindingProjection(binding, authenticatedTenantScope);
        binding.requireReferences(
                physical, destination, capability, grant.required(), grant.offered(), grant.controls());
        for (ProfileRef profile : java.util.List.of(binding.intent().profile(), capability.ref())) {
            final var acceptance = profileState.firstBindingAcceptance(profile, binding.bindingSource());
            if (acceptance == ProfileAcceptance.ABSENT) {
                return StableCode.PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION;
            }
            if (acceptance == ProfileAcceptance.CLOSED_FOR_FIRST_BINDING) {
                return StableCode.PROFILE_DEPRECATED_FOR_NEW_USE;
            }
        }
        return StableCode.OK;
    }

    /** A source apply must also recompute the bounded plan from its complete contract snapshot before commit. */
    public static void requirePlan(
            final TargetDomainRegistration.Plan plan,
            final TargetQueueState before,
            final TargetScheduleBinding binding,
            final TargetMembershipGrant grant) {
        plan.requireQueueSnapshot(before);
        grant.requireBindingProjection(binding, grant.tenantScope());
        if (plan.action() == TargetDomainRegistration.Action.REJECT
                || before.admissionState() == TargetQueueState.AdmissionState.CLOSED
                || !plan.domain().equals(binding.domain())
                || !before.targetId().equals(binding.target())
                || !Bytes.constantTimeEquals(before.accountingIncarnation(), binding.accountingIncarnation())) {
            throw new IllegalArgumentException("Target membership does not match the accepted registration plan");
        }
        if (plan.action() == TargetDomainRegistration.Action.REUSE_DOMAIN) {
            binding.requireQueueProjection(before);
            if (before.domains().get(binding.domain().slot()).lifecycle() != TargetDomainState.Lifecycle.ACTIVE) {
                throw new IllegalArgumentException("Target membership cannot reuse a draining domain");
            }
        } else {
            final int slot = binding.domain().slot();
            if (slot > before.domains().size()) {
                throw new IllegalArgumentException("Target membership cannot skip a domain slot");
            }
            if (slot == before.domains().size()) {
                if (binding.domain().generation() != 1) {
                    throw new IllegalArgumentException("new Target membership domain must start at generation 1");
                }
            } else {
                final var previous = before.domains().get(slot);
                if (previous.lifecycle() != TargetDomainState.Lifecycle.VACANT
                        || previous.domain().generation() == -1L
                        || binding.domain().generation() != previous.domain().generation() + 1) {
                    throw new IllegalArgumentException("Target membership cannot overwrite domain generation history");
                }
            }
        }
    }
}
