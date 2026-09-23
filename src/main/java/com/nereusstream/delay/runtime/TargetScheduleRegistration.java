package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileBindingControlState;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetScheduleBinding;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Prepares first-binding records from the same bounded Store view as the eventual Message/Reservation batch.
 * This is neither a standalone writer nor permission to ACK: the caller must add business/results/accounting,
 * validate retry/payload/Route policy, and hold all external snapshots through the actual commit.
 */
public final class TargetScheduleRegistration {
    private TargetScheduleRegistration() {}

    /**
     * Source-authenticated inputs. Profile activations must be fenced at commit;
     * a supplied binding is a proposal, and its slot/contracts are recomputed from actual records below.
     * initialNativeLeadCapMs applies only when the physical Target has no queue yet.
     */
    public record Authority(
            TargetScheduleBinding proposed,
            CanonicalTargetPartition physical,
            ProfileSemanticEnvelope destination,
            ProfileSemanticEnvelope capability,
            ProfileBindingControlState profiles,
            long initialNativeLeadCapMs) {
        public Authority {
            Objects.requireNonNull(proposed, "proposed");
            Objects.requireNonNull(physical, "physical");
            Objects.requireNonNull(destination, "destination");
            Objects.requireNonNull(capability, "capability");
            Objects.requireNonNull(profiles, "profiles");
            if (initialNativeLeadCapMs < 0) {
                throw new IllegalArgumentException("negative initial Native lead cap");
            }
        }
    }

    public record Plan(
            StableCode code,
            TargetScheduleBinding binding,
            TargetQueueState queue,
            TargetQuotaIncarnation owner,
            List<TargetStoreBackend.Edit> edits) {
        public Plan {
            Objects.requireNonNull(code, "code");
            edits = List.copyOf(edits);
            if (code == StableCode.OK) {
                Objects.requireNonNull(binding, "binding");
                Objects.requireNonNull(queue, "queue");
                Objects.requireNonNull(owner, "owner");
            } else if (binding != null || queue != null || owner != null || !edits.isEmpty()) {
                throw new IllegalArgumentException("rejected registration must carry no business edits");
            }
        }
    }

    public static Plan prepare(
            TargetStoreBackend.Reader reader,
            PreparedCommand command,
            SourcePosition source,
            TargetQuotaScope shardScope,
            byte[] lineage,
            Authority authority,
            int activatedMaxSlots) {
        Bytes.requireLength(lineage, 16, "lineage");
        if (Arrays.equals(lineage, new byte[16])
                || activatedMaxSlots < 1
                || activatedMaxSlots > TargetQueueState.MAX_DOMAIN_SLOTS) {
            throw new IllegalArgumentException("first binding requires assigned lineage and bounded domains");
        }
        final var binding = authority.proposed();
        if (shardScope.target() != null
                || !shardScope.shard().equals(reader.shardId())
                || !source.shardId().equals(shardScope.shard())
                || (command.type() != CommandType.SCHEDULE && command.type() != CommandType.PREPARE_LARGE_SCHEDULE)
                || command.type() != binding.commandType()
                || !command.delayMessageId().equals(binding.messageId())
                || !Arrays.equals(command.canonicalBody(), binding.canonicalBody())
                || !Arrays.equals(
                        source.canonicalBytes(), binding.bindingSource().canonicalBytes())
                || !authority.physical().id().equals(binding.target())) {
            throw new IllegalArgumentException("first binding does not match its exact Command/source/scope");
        }
        if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
            throw new IllegalStateException("first binding requires an advancing source");
        }
        // A provider cannot issue a grant merely by returning its digest or a proposed binding.
        final var applied = Objects.requireNonNull(
                TargetMembershipStoreAuthority.resolve(reader, shardScope, lineage, binding.membershipGrantRef()),
                "first binding lacks a durable membership grant");
        final var grant = applied.grant();
        final var authorization = TargetMembershipAuthorization.firstBinding(
                ref -> {
                    if (!Arrays.equals(ref, binding.membershipGrantRef())) {
                        throw new IllegalStateException("first binding resolved another membership grant");
                    }
                    return applied;
                },
                grant,
                binding,
                shardScope.tenantScope(),
                authority.profiles(),
                authority.physical(),
                authority.destination(),
                authority.capability());
        if (authorization != StableCode.OK) {
            return rejected(authorization);
        }
        final var targetScope = new TargetQuotaScope(shardScope.shard(), shardScope.tenantScope(), binding.target());
        final byte[] activationKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                targetScope.keySuffix());
        final var activation = TargetQuotaGrantActivation.decodeForStore(
                activationKey,
                required(reader, activationKey, TargetQuotaGrantActivation.VALUE_TYPE),
                shardScope.shard(),
                shardScope.tenantScope());
        activation.mutation().requireAtOrBefore(reader.aggregate().mutation());
        if (activation.allocation() == null) {
            throw new IllegalStateException("first binding has no applied Target quota allocation");
        }
        final byte[] ownerKey = activation.allocation().identity().key();
        ownerKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final var owner = TargetQuotaIncarnation.decodeForStore(
                ownerKey,
                required(reader, ownerKey, TargetQuotaIncarnation.VALUE_TYPE),
                shardScope.shard(),
                shardScope.tenantScope());
        if (!owner.identity().equals(activation.allocation().identity())
                || !Arrays.equals(owner.recoveryLineage(), lineage)
                || !Arrays.equals(owner.identity().accountingIncarnation(), binding.accountingIncarnation())) {
            throw new IllegalStateException("first binding differs from the actual allocated owner/lineage");
        }
        owner.requireNewIngress(activation.grant().accounting());
        owner.latestMutation().requireAtOrBefore(reader.aggregate().mutation());
        final byte[] identityKey = TargetKeyCodec.identity(binding.target());
        final byte[] identity = reader.get(ColumnFamily.META, identityKey);
        if (identity != null
                && !Arrays.equals(
                        TargetValueEnvelope.decode(identity, CanonicalTargetPartition.VALUE_TYPE)
                                .payload(),
                        authority.physical().canonicalBytes())) {
            throw new IllegalStateException("stored physical Target differs from authenticated Profile binding");
        }
        final byte[] queueKey = TargetKeyCodec.state(binding.target());
        final byte[] rawQueue = reader.get(ColumnFamily.META, queueKey);
        if (rawQueue != null && identity == null) {
            throw new IllegalStateException("existing Target queue lacks physical identity");
        }
        final var before = rawQueue == null
                ? new TargetQueueState(
                        binding.target(),
                        1,
                        1,
                        TargetQueueState.AdmissionState.OPEN,
                        binding.accountingIncarnation(),
                        authority.initialNativeLeadCapMs(),
                        List.of())
                : TargetQueueState.decodeForStore(
                        queueKey,
                        TargetValueEnvelope.decode(rawQueue, TargetQueueState.VALUE_TYPE)
                                .payload(),
                        authority.physical(),
                        shardScope.shard(),
                        activatedMaxSlots);
        if (!before.targetId().equals(binding.target())
                || !Arrays.equals(before.accountingIncarnation(), binding.accountingIncarnation())) {
            throw new IllegalStateException("actual Target queue has a different accounting incarnation");
        }
        if (before.domains().size() > activatedMaxSlots) {
            throw new IllegalStateException("stored Target domains exceed the activated bound");
        }
        final var bound = new ArrayList<TargetDomainRegistration.BoundDomain>();
        for (var domain : before.domains()) {
            if (domain.lifecycle() == TargetDomainState.Lifecycle.VACANT) {
                continue;
            }
            final byte[] dispatchKey = TargetKeyCodec.dispatchCompatibility(domain.dispatchCompatibilityRef());
            final var dispatch = TargetDispatchCompatibility.decode(
                    required(reader, dispatchKey, TargetDispatchCompatibility.VALUE_TYPE));
            final byte[] controlsKey = TargetKeyCodec.controlScope(domain.controlScopeRef());
            final var controls =
                    TargetControlScope.decode(required(reader, controlsKey, TargetControlScope.VALUE_TYPE));
            if (domain.nativePolicyScopeRef() != null) {
                final var nativeScope = nativeScope(reader, domain.nativePolicyScopeRef(), shardScope);
                nativeScope.requireReferences(authority.physical(), dispatch, controls);
                nativeScope.requireQueue(before);
            }
            bound.add(new TargetDomainRegistration.BoundDomain(
                    domain.domain(),
                    new TargetDomainRegistration.Requirements(dispatch, controls, domain.nativePolicyScopeRef())));
        }
        final var registration = TargetDomainRegistration.plan(
                authority.physical(),
                before,
                shardScope.shard(),
                bound,
                new TargetDomainRegistration.Requirements(
                        grant.offered(), grant.controls(), binding.nativePolicyScopeRef()),
                activatedMaxSlots);
        if (registration.action() == TargetDomainRegistration.Action.REJECT) {
            return rejected(registration.result());
        }
        TargetMembershipAuthorization.requirePlan(registration, before, binding, grant);
        final var domains = new ArrayList<>(before.domains());
        if (registration.action() == TargetDomainRegistration.Action.BIND_DOMAIN) {
            final var domain = new TargetDomainState(
                    registration.domain(),
                    TargetDomainState.Lifecycle.ACTIVE,
                    grant.offered().digest(),
                    grant.controls().digest(),
                    binding.nativePolicyScopeRef(),
                    null,
                    null);
            if (domain.domain().slot() == domains.size()) {
                domains.add(domain);
            } else {
                domains.set(domain.domain().slot(), domain);
            }
        }
        final var after = registration.action() == TargetDomainRegistration.Action.REUSE_DOMAIN
                ? before
                : new TargetQueueState(
                        before.targetId(),
                        rawQueue == null ? 1 : TargetQueueState.nextRevision(before.headRevision()),
                        before.controlVersion(),
                        before.admissionState(),
                        before.accountingIncarnation(),
                        before.nativeIndexLeadCapMs(),
                        domains);
        binding.requireQueueProjection(after);
        if (binding.nativePolicyScopeRef() != null) {
            final var nativeScope = nativeScope(reader, binding.nativePolicyScopeRef(), shardScope);
            nativeScope.requireReferences(authority.physical(), grant.offered(), grant.controls());
            nativeScope.requireQueue(after);
            nativeScope.requireBinding(binding);
        }
        if (reader.get(ColumnFamily.ID, binding.encodedKey()) != null) {
            throw new IllegalStateException("first binding cannot replace an existing accepted binding");
        }
        final var edits = new ArrayList<TargetStoreBackend.Edit>();
        addImmutable(
                reader,
                edits,
                identityKey,
                CanonicalTargetPartition.VALUE_TYPE,
                authority.physical().canonicalBytes());
        addImmutable(
                reader,
                edits,
                TargetKeyCodec.dispatchCompatibility(grant.required().digest()),
                TargetDispatchCompatibility.VALUE_TYPE,
                grant.required().canonicalBytes());
        addImmutable(
                reader,
                edits,
                TargetKeyCodec.dispatchCompatibility(grant.offered().digest()),
                TargetDispatchCompatibility.VALUE_TYPE,
                grant.offered().canonicalBytes());
        addImmutable(
                reader,
                edits,
                TargetKeyCodec.controlScope(grant.controls().digest()),
                TargetControlScope.VALUE_TYPE,
                grant.controls().canonicalBytes());
        if (rawQueue == null || after != before) {
            edits.add(reader.replace(ColumnFamily.META, queueKey, TargetQueueState.VALUE_TYPE, after.canonicalBytes()));
        }
        edits.add(reader.replace(
                ColumnFamily.ID, binding.encodedKey(), TargetScheduleBinding.VALUE_TYPE, binding.canonicalBytes()));
        return new Plan(StableCode.OK, binding, after, owner, edits);
    }

    private static TargetNativePolicyScope nativeScope(
            TargetStoreBackend.Reader reader, byte[] digest, TargetQuotaScope scope) {
        final byte[] key = TargetKeyCodec.nativePolicyScope(digest);
        return TargetNativePolicyScope.decodeForStore(
                key, required(reader, key, TargetNativePolicyScope.VALUE_TYPE), scope.shard());
    }

    private static void addImmutable(
            TargetStoreBackend.Reader reader,
            List<TargetStoreBackend.Edit> edits,
            byte[] key,
            int type,
            byte[] payload) {
        final byte[] prior = reader.get(ColumnFamily.META, key);
        if (prior != null) {
            if (!Arrays.equals(TargetValueEnvelope.decode(prior, type).payload(), payload)) {
                throw new IllegalStateException("shared binding record content differs at its immutable key");
            }
            return;
        }
        // Required and offered contracts may be identical; the batch must still have only one edit per key.
        if (edits.stream().noneMatch(edit -> edit.family() == ColumnFamily.META && Arrays.equals(edit.key(), key))) {
            edits.add(reader.replace(ColumnFamily.META, key, type, payload));
        }
    }

    private static byte[] required(TargetStoreBackend.Reader reader, byte[] key, int type) {
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("first binding is missing an actual authority/contract record");
        }
        return TargetValueEnvelope.decode(raw, type).payload();
    }

    private static Plan rejected(StableCode code) {
        return new Plan(code, null, null, null, List.of());
    }
}
