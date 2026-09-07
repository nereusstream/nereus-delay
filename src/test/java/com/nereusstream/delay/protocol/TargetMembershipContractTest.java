package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.runtime.TargetDomainRegistration;
import com.nereusstream.delay.runtime.TargetMembershipAuthority;
import com.nereusstream.delay.runtime.TargetMembershipAuthorization;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetMembershipContractTest {
    private final Properties vectors = load("target-membership-vectors.properties");
    private final Properties compat = load("target-compatibility-vectors.properties");
    private final Properties bindings = load("target-binding-channel-vectors.properties");
    private final CanonicalTargetPartition physical = CanonicalTargetPartition.decode(hex(compat, "pulsar.target"));
    private final TargetDispatchCompatibility dispatch =
            TargetDispatchCompatibility.decode(hex(compat, "pulsar.journal.dispatch"));
    private final TargetControlScope controls = TargetControlScope.decode(hex(compat, "scope.shared"));
    private final KafkaSourcePosition source =
            (KafkaSourcePosition) TargetSourcePosition.decode(hex(bindings, "source"));
    private final ProfileSemanticEnvelope capability =
            new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap"), 1, dispatch.capability());
    private final ProfileSemanticEnvelope destination = destination("member", 0xAA);

    @Test
    void fourCapabilitiesMatchIndependentRegistrationGrantKeyAndDecode() {
        for (String name : List.of("kafka.baseline", "kafka.receipt", "pulsar.baseline", "pulsar.journal")) {
            final var contract = TargetDispatchCompatibility.decode(hex(compat, name + ".dispatch"));
            final var scope = new TargetControlScope(
                    contract.target(), source.shardId(), controls.controls(), controls.permits());
            final var value = grant(
                    new ProfileRef(Bytes.utf8("dest"), 1, repeat(32, 0x61), ProfileKind.DESTINATION), contract, scope);
            assertArrayEquals(hex(vectors, name + ".registration"), value.registrationBytes());
            assertArrayEquals(hex(vectors, name + ".grant"), value.canonicalBytes());
            assertArrayEquals(hex(vectors, name + ".key"), value.encodedKey());
            assertEquals(value, TargetMembershipGrant.decodeReferenced(value.digest(), value.canonicalBytes()));
            assertEquals(
                    value,
                    TargetMembershipGrant.decodeForStore(value.encodedKey(), value.canonicalBytes(), source.shardId()));
        }
    }

    @Test
    void exactAppliedSourceIdentityIsRequiredWithoutRegistrationHashCycle() {
        final var grant = grant();
        final byte[] prepared = TargetMembershipGrant.prepareRegistration(
                grant.tenantScope(),
                grant.memberProfile(),
                grant.required(),
                grant.offered(),
                grant.controls(),
                grant.authorityPolicyRef(),
                grant.authorityOperationId());
        assertArrayEquals(prepared, grant.registrationBytes());
        assertEquals(grant, TargetMembershipGrant.fromRegistration(prepared, grant.sourceMutationDigest(), at(6)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.fromRegistration(
                        Bytes.concat(prepared, field(9, new byte[32])), grant.sourceMutationDigest(), at(6)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.fromRegistration(
                        new byte[TargetMembershipGrant.MAX_REGISTRATION_BYTES + 1],
                        grant.sourceMutationDigest(),
                        at(6)));
        assertDoesNotThrow(() -> grant.requireSourceRegistration(
                grant.registrationBytes(), grant.authorityOperationId(), grant.sourceMutationDigest(), at(6)));
        assertThrows(
                IllegalArgumentException.class,
                () -> grant.requireSourceRegistration(
                        grant.registrationBytes(), repeat(32, 0x75), grant.sourceMutationDigest(), at(6)));
        assertThrows(
                IllegalArgumentException.class,
                () -> grant.requireSourceRegistration(
                        grant.registrationBytes(), grant.authorityOperationId(), repeat(32, 0x75), at(6)));
        assertThrows(
                IllegalArgumentException.class,
                () -> grant.requireSourceRegistration(
                        grant.registrationBytes(), grant.authorityOperationId(), grant.sourceMutationDigest(), at(7)));
        final var conflict = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                6,
                source.leaderEpoch(),
                source.brokerLogAppendTimeEpochMs() + 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> grant.requireSourceRegistration(
                        grant.registrationBytes(),
                        grant.authorityOperationId(),
                        grant.sourceMutationDigest(),
                        conflict));
        final var later = new TargetMembershipGrant(
                grant.tenantScope(),
                grant.memberProfile(),
                dispatch,
                dispatch,
                controls,
                grant.authorityPolicyRef(),
                grant.authorityOperationId(),
                repeat(32, 0x75),
                at(8));
        assertArrayEquals(grant.registrationBytes(), later.registrationBytes());
        assertNotEquals(grant, later);
    }

    @Test
    void fullAuthorizedRecordTenantAndExactProfileActivationAreMandatory() {
        final var grant = grant();
        final var binding = binding(grant, "best", source);
        assertEquals(
                StableCode.OK,
                authorize(ref -> new TargetMembershipAuthority.AppliedGrant(grant, null), grant, binding, active()));
        assertEquals(StableCode.UNAUTHORIZED, authorize(ref -> null, grant, binding, active()));
        assertEquals(
                StableCode.UNAUTHORIZED,
                TargetMembershipAuthorization.firstBinding(
                        ref -> new TargetMembershipAuthority.AppliedGrant(grant, null),
                        grant,
                        binding,
                        repeat(32, 0x71),
                        active(),
                        physical,
                        destination,
                        capability));
        assertEquals(
                StableCode.PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION,
                authorize(
                        ref -> new TargetMembershipAuthority.AppliedGrant(grant, null),
                        grant,
                        binding,
                        ProfileBindingControlState.empty()));
        final var missingCapability = ProfileBindingControlState.empty().activate(destination.ref(), at(1));
        assertEquals(
                StableCode.PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION,
                authorize(
                        ref -> new TargetMembershipAuthority.AppliedGrant(grant, null),
                        grant,
                        binding,
                        missingCapability));
        final var closed = active().close(
                        new ProfileNewBindingClosePayload(
                                capability.ref(), new ControlReason(ControlReasonKind.POLICY_CHANGE, null, null)),
                        at(5));
        assertEquals(
                StableCode.PROFILE_DEPRECATED_FOR_NEW_USE,
                authorize(ref -> new TargetMembershipAuthority.AppliedGrant(grant, null), grant, binding, closed));
        assertThrows(
                IllegalStateException.class,
                () -> authorize(
                        ref -> {
                            throw new IllegalStateException("unavailable source view");
                        },
                        grant,
                        binding,
                        active()));
    }

    @Test
    void matchingHashesDoNotAuthorizeOmittedControlOrPermitGroups() {
        final var authorized = grant();
        for (var incomplete : List.of(
                new TargetControlScope(physical.id(), source.shardId(), List.of(), controls.permits()),
                new TargetControlScope(physical.id(), source.shardId(), controls.controls(), List.of()))) {
            final var forged = grant(destination.ref(), dispatch, incomplete);
            final var binding = binding(forged, "best", source);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> authorize(
                            ref -> new TargetMembershipAuthority.AppliedGrant(authorized, null),
                            forged,
                            binding,
                            active()));
            assertEquals(
                    StableCode.UNAUTHORIZED,
                    authorize(
                            ref -> Arrays.equals(ref, authorized.digest())
                                    ? new TargetMembershipAuthority.AppliedGrant(authorized, null)
                                    : null,
                            forged,
                            binding,
                            active()));
        }
    }

    @Test
    void closureStopsNewBindingsButHistoricalBindingAndPrepareSourceStayValid() {
        final var grant = grant();
        final var applied = new TargetMembershipAuthority.AppliedGrant(grant, at(10));
        for (long position : List.of(0L, 6L, 10L, 11L)) {
            assertFalse(applied.allowsFirstBinding(at(position)));
        }
        assertTrue(applied.allowsFirstBinding(at(7)));
        for (String kind : List.of("best", "prepare", "native", "strict", "object")) {
            final var binding = binding(grant, kind, at(7));
            assertEquals(StableCode.OK, authorize(ref -> applied, grant, binding, active()));
            assertDoesNotThrow(() -> binding.requireMessageSource(at(12)));
            assertDoesNotThrow(() -> grant.requireBindingProjection(binding, grant.tenantScope()));
            assertEquals(
                    StableCode.UNAUTHORIZED, authorize(ref -> applied, grant, binding(grant, kind, at(10)), active()));
        }
        assertThrows(IllegalArgumentException.class, () -> new TargetMembershipAuthority.AppliedGrant(grant, at(6)));
        final var changedTime = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                10,
                source.leaderEpoch(),
                source.brokerLogAppendTimeEpochMs() + 1);
        assertThrows(IllegalArgumentException.class, () -> applied.allowsFirstBinding(changedTime));
    }

    @Test
    void sourceResourcesAndUnsignedPositionsCannotBeSubstituted() {
        final var grant = grant();
        final var applied = new TargetMembershipAuthority.AppliedGrant(grant, null);
        final var otherResource = new KafkaSourcePosition(
                source.shardId(), source.authenticatedClusterId(), UUID.randomUUID(), 7, null, 100);
        assertThrows(IllegalArgumentException.class, () -> applied.allowsFirstBinding(otherResource));
        assertThrows(
                IllegalArgumentException.class,
                () -> grant.requireBindingProjection(binding(grant, "best", otherResource), grant.tenantScope()));
        final var high = new TargetMembershipGrant(
                grant.tenantScope(),
                grant.memberProfile(),
                dispatch,
                dispatch,
                controls,
                grant.authorityPolicyRef(),
                grant.authorityOperationId(),
                grant.sourceMutationDigest(),
                at(Long.MAX_VALUE));
        final var highApplied = new TargetMembershipAuthority.AppliedGrant(high, at(-1));
        assertTrue(highApplied.allowsFirstBinding(at(Long.MIN_VALUE)));
        assertFalse(highApplied.allowsFirstBinding(at(1)));
        assertFalse(highApplied.allowsFirstBinding(at(-1)));
    }

    @Test
    void grantCannotWidenProfileTargetOrCapabilityAndMutationsNeedExactPlan() {
        final var grant = grant();
        final var binding = binding(grant, "best", source);
        final var before = queue(List.of());
        final var requirements = new TargetDomainRegistration.Requirements(dispatch, controls, null);
        final var plan = TargetDomainRegistration.plan(physical, before, source.shardId(), List.of(), requirements, 1);
        assertDoesNotThrow(() -> TargetMembershipAuthorization.requirePlan(plan, before, binding, grant));
        final var changed = new TargetQueueState(
                physical.id(), 2, 1, TargetQueueState.AdmissionState.OPEN, repeat(16, 1), 60000, List.of());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipAuthorization.requirePlan(plan, changed, binding, grant));
        final var summary = new TargetDomainState(
                binding.domain(),
                TargetDomainState.Lifecycle.ACTIVE,
                dispatch.digest(),
                controls.digest(),
                null,
                null,
                null);
        final var existing = queue(List.of(summary));
        final var reuse = TargetDomainRegistration.plan(
                physical,
                existing,
                source.shardId(),
                List.of(new TargetDomainRegistration.BoundDomain(binding.domain(), requirements)),
                requirements,
                1);
        assertDoesNotThrow(() -> TargetMembershipAuthorization.requirePlan(reuse, existing, binding, grant));
        final var other = grant(destination("other", 0xAA).ref(), dispatch, controls);
        assertThrows(
                IllegalArgumentException.class, () -> other.requireBindingProjection(binding, other.tenantScope()));
        final var weak = TargetDispatchCompatibility.decode(hex(compat, "pulsar.baseline.dispatch"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetMembershipGrant(
                        grant.tenantScope(),
                        grant.memberProfile(),
                        dispatch,
                        weak,
                        controls,
                        grant.authorityPolicyRef(),
                        grant.authorityOperationId(),
                        grant.sourceMutationDigest(),
                        at(6)));
    }

    @Test
    void membershipPlanCannotForgeAnActiveSlotReplacementOrReuseDrainingState() {
        final var grant = grant();
        final var binding = binding(grant, "best", source);
        for (var lifecycle : List.of(TargetDomainState.Lifecycle.ACTIVE, TargetDomainState.Lifecycle.DRAINING)) {
            final var before = queue(List.of(new TargetDomainState(
                    binding.domain(), lifecycle, dispatch.digest(), controls.digest(), null, null, null)));
            final var overwrite = new TargetDomainRegistration.Plan(
                    TargetDomainRegistration.Action.BIND_DOMAIN,
                    StableCode.OK,
                    binding.domain(),
                    before.headRevision(),
                    before.digest());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetMembershipAuthorization.requirePlan(overwrite, before, binding, grant));
            if (lifecycle == TargetDomainState.Lifecycle.DRAINING) {
                final var reuse = new TargetDomainRegistration.Plan(
                        TargetDomainRegistration.Action.REUSE_DOMAIN,
                        StableCode.OK,
                        binding.domain(),
                        before.headRevision(),
                        before.digest());
                assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetMembershipAuthorization.requirePlan(reuse, before, binding, grant));
            }
        }
        final var closed = new TargetQueueState(
                physical.id(), 1, 1, TargetQueueState.AdmissionState.CLOSED, repeat(16, 1), 60000, List.of());
        final var forged = new TargetDomainRegistration.Plan(
                TargetDomainRegistration.Action.BIND_DOMAIN,
                StableCode.OK,
                binding.domain(),
                closed.headRevision(),
                closed.digest());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipAuthorization.requirePlan(forged, closed, binding, grant));
    }

    @Test
    void sharedProviderCanRotateWithoutRebindingAndOldChannelDomainCannotBeRelabeled() {
        final var grant = grant();
        final var binding = binding(grant, "best", source);
        final var original = TargetChannelIdentity.decode(hex(bindings, "channel.pulsar.journal"));
        final var oldLease = original.credentialLease();
        for (String id : List.of("provider-a", "provider-b")) {
            final var provider = destination(id, 0xAA);
            final var lease = new CredentialUseLease(
                    provider.ref(),
                    CredentialUseKind.DESTINATION_CHANNEL,
                    original.context().credentialHolderScope(),
                    1,
                    oldLease.credentialBindingDigest(),
                    oldLease.resolvedCredentialFingerprintDigest(),
                    oldLease.issuedAt(),
                    oldLease.validUntilEpochMs(),
                    oldLease.protectionRevision());
            final var channel = new TargetChannelIdentity(original.context(), lease);
            assertDoesNotThrow(() -> grant.requireChannelProjection(binding, channel));
            assertDoesNotThrow(() -> channel.requireProjection(physical, dispatch, controls, provider));
            assertThrows(IllegalArgumentException.class, () -> channel.requireExactFrozenIdentity(original));
        }
        final var context = original.context();
        final var wrong = new TargetChannelIdentity.Context(
                context.sourceShard(),
                context.target(),
                new TargetKeyCodec.Domain(0, 2),
                context.accountingIncarnation(),
                context.dispatchCompatibilityRef(),
                context.controlScopeRef(),
                context.kind(),
                context.channelSlot(),
                context.channelGeneration(),
                context.evidenceGeneration(),
                context.resourceGuardAttestationDigest());
        final var lease = new CredentialUseLease(
                oldLease.profile(),
                CredentialUseKind.DESTINATION_CHANNEL,
                wrong.credentialHolderScope(),
                1,
                oldLease.credentialBindingDigest(),
                oldLease.resolvedCredentialFingerprintDigest(),
                oldLease.issuedAt(),
                oldLease.validUntilEpochMs(),
                oldLease.protectionRevision());
        assertThrows(
                IllegalArgumentException.class,
                () -> grant.requireChannelProjection(binding, new TargetChannelIdentity(wrong, lease)));
    }

    @Test
    void closedCodecRejectsMissingUnknownTamperedAndOversizedFieldsBeforeNestedDecode() {
        final var grant = grant();
        for (int field = 1; field <= 11; field++) {
            final int omitted = field;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetMembershipGrant.decode(rewrite(grant.canonicalBytes(), omitted, null)));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decode(Bytes.concat(grant.canonicalBytes(), field(12, new byte[0]))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decode(rewrite(grant.canonicalBytes(), 1, uint(1, 2))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decode(rewrite(grant.canonicalBytes(), 2, field(2, new byte[32]))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decode(rewrite(
                        grant.canonicalBytes(),
                        3,
                        field(
                                3,
                                new ProfileRef(new byte[257], 1, repeat(32, 1), ProfileKind.DESTINATION)
                                        .canonicalBytes()))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decode(new byte[TargetMembershipGrant.MAX_CANONICAL_BYTES + 1]));
        final byte[] damaged = grant.canonicalBytes();
        damaged[damaged.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetMembershipGrant.decode(damaged));
    }

    @Test
    void maximumFullGrantAndRegistrationMatchIndependentLengthsAndHashes() {
        final byte[] token = new byte[32];
        for (int n = 0; n < 32; n++) {
            token[n] = (byte) n;
        }
        final var evidence = BrokerResourceIdentity.pulsar(
                new PulsarBrokerResourceIdentity("x".repeat(256), token, "x".repeat(1 << 20), Long.MAX_VALUE));
        final var cap = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.PULSAR_BROKER_DEDUP,
                7,
                evidence,
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                repeat(32, 0x22),
                repeat(32, 0x33),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE);
        final var dispatch = new TargetDispatchCompatibility(
                physical.id(),
                repeat(32, 0xAA),
                Integer.MAX_VALUE,
                cap,
                repeat(32, 0xBB),
                Long.MAX_VALUE,
                Long.MAX_VALUE);
        final var groups = new ArrayList<TargetControlScope.ControlGroup>();
        final var permits = new ArrayList<byte[]>();
        for (int n = 1; n <= 32; n++) {
            final byte[] group = new byte[32];
            group[31] = (byte) n;
            groups.add(new TargetControlScope.ControlGroup(TargetControlScope.GroupKind.BINDING_SEND_CONTROL, group));
            final byte[] permit = new byte[32];
            permit[31] = (byte) (n + 32);
            permits.add(permit);
        }
        final var controls = new TargetControlScope(physical.id(), source.shardId(), groups, permits);
        final var source = new PulsarSourcePosition(
                this.source.shardId(),
                token,
                "x".repeat(1 << 20),
                -1,
                -1,
                -2,
                -1,
                PulsarSourcePosition.EntryKind.BATCH,
                Long.MAX_VALUE);
        final var grant = new TargetMembershipGrant(
                repeat(32, 0x70),
                new ProfileRef(Bytes.utf8("x".repeat(256)), -1, repeat(32, 0x61), ProfileKind.DESTINATION),
                dispatch,
                dispatch,
                controls,
                repeat(32, 0x71),
                repeat(32, 0x72),
                repeat(32, 0x73),
                source);
        assertEquals(Integer.parseInt(vectors.getProperty("maximum.grant.length")), grant.canonicalBytes().length);
        assertArrayEquals(hex(vectors, "maximum.grant.sha256"), Bytes.sha256(grant.canonicalBytes()));
        assertEquals(
                Integer.parseInt(vectors.getProperty("maximum.registration.length")), grant.registrationBytes().length);
        assertArrayEquals(hex(vectors, "maximum.registration.sha256"), Bytes.sha256(grant.registrationBytes()));
        assertTrue(grant.canonicalBytes().length <= TargetMembershipGrant.MAX_CANONICAL_BYTES);
        assertTrue(grant.registrationBytes().length <= TargetMembershipGrant.MAX_REGISTRATION_BYTES);
        assertEquals(grant, TargetMembershipGrant.decode(grant.canonicalBytes()));
    }

    @Test
    void reservedEnvelopeHasValidCrcButLegacyReaderAndWrongStoreRefRejectIt() {
        final byte[] value = hex(vectors, "grant.value");
        assertEquals(TargetMembershipGrant.VALUE_TYPE, Byte.toUnsignedInt(value[2]));
        assertEquals(Bytes.crc32c(value, 0, value.length - 4), Bytes.readU32be(value, value.length - 4));
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
        final var grant = grant();
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decodeReferenced(repeat(32, 1), grant.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decodeForStore(
                        grant.encodedKey(),
                        grant.canonicalBytes(),
                        new ShardId(source.shardId().routeIncarnation(), 4)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetMembershipGrant.decodeForStore(
                        TargetKeyCodec.controlScope(grant.digest()), grant.canonicalBytes(), source.shardId()));
    }

    @Test
    void inputAndOutputArraysAreDefensive() {
        final byte[] tenant = repeat(32, 0x70);
        final var grant = new TargetMembershipGrant(
                tenant,
                destination.ref(),
                dispatch,
                dispatch,
                controls,
                repeat(32, 0x71),
                repeat(32, 0x72),
                repeat(32, 0x73),
                at(6));
        tenant[0] = 0;
        final byte[] expected = grant.canonicalBytes();
        for (byte[] returned : List.of(
                grant.tenantScope(),
                grant.authorityPolicyRef(),
                grant.authorityOperationId(),
                grant.sourceMutationDigest(),
                grant.digest(),
                grant.canonicalBytes(),
                grant.registrationBytes())) {
            returned[0] ^= 1;
        }
        assertArrayEquals(expected, grant.canonicalBytes());
        assertEquals(grant(), grant);
        assertEquals(grant().hashCode(), grant.hashCode());
    }

    private TargetMembershipGrant grant() {
        return grant(destination.ref(), dispatch, controls);
    }

    private TargetMembershipGrant grant(
            final ProfileRef profile, final TargetDispatchCompatibility dispatch, final TargetControlScope scope) {
        return new TargetMembershipGrant(
                repeat(32, 0x70),
                profile,
                dispatch,
                dispatch,
                scope,
                repeat(32, 0x71),
                repeat(32, 0x72),
                repeat(32, 0x73),
                at(6));
    }

    private KafkaSourcePosition at(final long offset) {
        return new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                offset,
                source.leaderEpoch(),
                source.brokerLogAppendTimeEpochMs());
    }

    private ProfileBindingControlState active() {
        return ProfileBindingControlState.empty()
                .activate(destination.ref(), at(1))
                .activate(capability.ref(), at(2));
    }

    private StableCode authorize(
            final TargetMembershipAuthority authority,
            final TargetMembershipGrant grant,
            final TargetScheduleBinding binding,
            final ProfileBindingControlState profiles) {
        return TargetMembershipAuthorization.firstBinding(
                authority, grant, binding, grant.tenantScope(), profiles, physical, destination, capability);
    }

    private TargetQueueState queue(final List<TargetDomainState> domains) {
        return new TargetQueueState(
                physical.id(), 1, 1, TargetQueueState.AdmissionState.OPEN, repeat(16, 1), 60000, domains);
    }

    private TargetScheduleBinding binding(
            final TargetMembershipGrant grant, final String kind, final SourcePosition at) {
        final var original = TargetScheduleBinding.decode(hex(bindings, "binding." + kind));
        final byte[] body = replaceProfile(original.canonicalBody(), grant.memberProfile());
        return new TargetScheduleBinding(
                original.messageId(),
                original.commandType(),
                body,
                at,
                physical.id(),
                original.domain(),
                original.accountingIncarnation(),
                grant.required().digest(),
                grant.offered().digest(),
                grant.controls().digest(),
                grant.digest(),
                original.nativePolicyScopeRef(),
                original.orderingDomain());
    }

    private static byte[] replaceProfile(final byte[] body, final ProfileRef profile) {
        return CanonicalProtobuf.message(out -> {
            for (var f : QueryCodecSupport.read(body, "body")) {
                if (f.number() == 10) {
                    final byte[] intent = CanonicalProtobuf.message(nested -> {
                        for (var p : QueryCodecSupport.read(f.rawValue(), "intent")) {
                            if (p.number() == 1) {
                                CanonicalProtobuf.bytes(nested, 1, profile.canonicalBytes());
                            } else if (p.wireType() == 2) {
                                CanonicalProtobuf.bytes(nested, p.number(), p.rawValue());
                            } else {
                                CanonicalProtobuf.uint64Bits(nested, p.number(), p.unsignedValue());
                            }
                        }
                    });
                    CanonicalProtobuf.bytes(out, 10, intent);
                } else if (f.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, f.number(), f.rawValue());
                } else {
                    CanonicalProtobuf.uint64Bits(out, f.number(), f.unsignedValue());
                }
            }
        });
    }

    private ProfileSemanticEnvelope destination(final String id, final int authorization) {
        return new ProfileSemanticEnvelope(
                ProfileKind.DESTINATION,
                Bytes.utf8(id),
                1,
                new DestinationProfileSemantic(
                        AdapterKind.PULSAR,
                        physical.resource(),
                        8,
                        TargetPartitionPolicy.EXPLICIT_ONLY,
                        TargetPartitionHashInput.DELAY_MESSAGE_ID,
                        List.of(5),
                        capability.ref(),
                        3,
                        60000,
                        repeat(32, authorization),
                        20000,
                        10000,
                        10000,
                        1,
                        Bytes.utf8(id),
                        86400000,
                        172800000,
                        2,
                        repeat(32, 0xBB)));
    }

    private static byte[] rewrite(final byte[] encoded, final int number, final byte[] replacement) {
        final byte[] body = CanonicalProtobuf.message(out -> {
            for (var f : QueryCodecSupport.read(encoded, "rewrite")) {
                if (f.number() == number) {
                    if (replacement != null) {
                        out.writeBytes(replacement);
                    }
                } else if (f.number() != 11) {
                    if (f.wireType() == 2) {
                        CanonicalProtobuf.bytes(out, f.number(), f.rawValue());
                    } else {
                        CanonicalProtobuf.uint64Bits(out, f.number(), f.unsignedValue());
                    }
                }
            }
        });
        return number == 11
                ? body
                : Bytes.concat(
                        body, field(11, Bytes.sha256(Bytes.utf8("nereus-delay-target-membership-grant\0"), body)));
    }

    private static byte[] field(final int number, final byte[] bytes) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, number, bytes));
    }

    private static byte[] uint(final int number, final long value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.uint64Bits(out, number, value));
    }

    private static byte[] repeat(final int length, final int value) {
        final byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] hex(final Properties properties, final String key) {
        return HexFormat.of().parseHex(properties.getProperty(key));
    }

    private static Properties load(final String name) {
        try (var input = TargetMembershipContractTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var properties = new Properties();
            properties.load(input);
            return properties;
        } catch (IOException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
