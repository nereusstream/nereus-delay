package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.Retryability;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetControlScope;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TargetQueueState;
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

class TargetCompatibilityContractTest {
    private final Properties vectors = vectors();
    private final CanonicalTargetPartition pulsar = CanonicalTargetPartition.decode(bytes("pulsar.target"));
    private final ShardId shard =
            new ShardId(RouteIncarnation.fromUuid(UUID.fromString("11111111-2222-4333-8444-555555555555")), 3);
    private final TargetKeyCodec.Domain domain = new TargetKeyCodec.Domain(0, 1);

    @Test
    void allFourExecutionCapabilitiesMatchIndependentBytesAndStoreReferences() {
        for (String name : List.of("kafka.baseline", "kafka.receipt", "pulsar.baseline", "pulsar.journal")) {
            final var target =
                    CanonicalTargetPartition.decode(bytes(name.startsWith("kafka") ? "kafka.target" : "pulsar.target"));
            final var cap = DeliveryCapabilitySemantic.decode(bytes(name + ".capability"));
            final var actual = new TargetDispatchCompatibility(
                    target.id(), repeated(32, 0xAA), 2, cap, repeated(32, 0xBB), 86400000, 172800000);
            assertArrayEquals(bytes(name + ".dispatch"), actual.canonicalBytes(), name);
            assertArrayEquals(bytes(name + ".key"), actual.encodedKey());
            assertEquals(
                    actual,
                    TargetDispatchCompatibility.decodeReferenced(actual.digest(), actual.canonicalBytes(), target));
            assertEquals(
                    actual,
                    TargetDispatchCompatibility.decodeForStore(actual.encodedKey(), actual.canonicalBytes(), target));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetDispatchCompatibility.decodeReferenced(
                            repeated(32, 9), actual.canonicalBytes(), target));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetDispatchCompatibility.decodeForStore(
                            TargetKeyCodec.controlScope(actual.digest()), actual.canonicalBytes(), target));
        }
    }

    @Test
    void profileIdentityVersionBucketsAndPerMessageLimitsDoNotCreateDispatchClasses() {
        final var body = capability();
        final var firstCap = new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap-a"), 1, body);
        final var secondCap =
                new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap-b"), 19, body);
        final var first = destination("profile-a", 1, firstCap, 1, 8192, 4096);
        final var second = destination("profile-b", 77, secondCap, 128, 65536, 32768);
        assertNotEquals(first.ref(), second.ref());
        assertNotEquals(first.canonicalBytes().length, second.canonicalBytes().length);
        assertEquals(dispatch(), TargetDispatchCompatibility.fromProfiles(pulsar, first, firstCap));
        assertEquals(dispatch(), TargetDispatchCompatibility.fromProfiles(pulsar, second, secondCap));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetDispatchCompatibility.fromProfiles(pulsar, first, secondCap));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetDispatchCompatibility.fromProfiles(
                        new CanonicalTargetPartition(pulsar.resource(), 4), first, firstCap));
    }

    @Test
    void authorizationEncodingOutcomeAndBrokerLifetimeRequirementsRemainDistinct() {
        final var original = dispatch();
        final var changed = List.of(
                new TargetDispatchCompatibility(
                        pulsar.id(), repeated(32, 0xAB), 2, capability(), repeated(32, 0xBB), 86400000, 172800000),
                new TargetDispatchCompatibility(
                        pulsar.id(), repeated(32, 0xAA), 3, capability(), repeated(32, 0xBB), 86400000, 172800000),
                new TargetDispatchCompatibility(
                        pulsar.id(), repeated(32, 0xAA), 2, capability(), repeated(32, 0xBC), 86400000, 172800000),
                new TargetDispatchCompatibility(
                        pulsar.id(), repeated(32, 0xAA), 2, capability(), repeated(32, 0xBB), 86400001, 172800000),
                new TargetDispatchCompatibility(
                        pulsar.id(), repeated(32, 0xAA), 2, capability(), repeated(32, 0xBB), 86400000, 172800001),
                TargetDispatchCompatibility.decode(bytes("pulsar.baseline.dispatch")));
        for (var candidate : changed) {
            assertNotEquals(original, candidate);
            assertTrue(!Arrays.equals(original.digest(), candidate.digest()));
            assertEquals(
                    StableCode.TARGET_BINDING_INCOMPATIBLE,
                    plan(
                                    queue(List.of(summary(
                                            domain, TargetDomainState.Lifecycle.ACTIVE, requirements("shared")))),
                                    List.of(new TargetDomainRegistration.BoundDomain(domain, requirements("shared"))),
                                    new TargetDomainRegistration.Requirements(candidate, scope("shared"), null),
                                    1)
                            .result());
        }
    }

    @Test
    void fixedOfferedGuaranteesCanCoverWeakerRequirementsWithoutChangingDomainIdentity() {
        final var offered = dispatch();
        final var requested = lifetimeRequirements(1, 3600000, 300000, 128, 60000, 86400000);
        assertTrue(offered.canServe(requested));
        assertFalse(requested.canServe(offered));
        assertNotEquals(offered, requested);
        final var existing = new TargetDomainRegistration.Requirements(offered, scope("shared"), null);
        final var candidate = new TargetDomainRegistration.Requirements(requested, scope("shared"), null);
        final var queue = queue(List.of(summary(domain, TargetDomainState.Lifecycle.ACTIVE, existing)));
        final var before = queue.canonicalBytes();
        assertEquals(
                TargetDomainRegistration.Action.REUSE_DOMAIN,
                plan(queue, List.of(new TargetDomainRegistration.BoundDomain(domain, existing)), candidate, 1)
                        .action());
        assertArrayEquals(offered.digest(), queue.domains().getFirst().dispatchCompatibilityRef());
        assertArrayEquals(before, queue.canonicalBytes());
    }

    @Test
    void coverageChecksEachMinimumGuaranteeAndMaximumProducerCeilingInTheSafeDirection() {
        final var offered = dispatch();
        for (var required : List.of(
                lifetimeRequirements(7, 86400000, 600000, 64, 86400000, 172800000),
                lifetimeRequirements(3, 86400001, 600000, 64, 86400000, 172800000),
                lifetimeRequirements(3, 86400000, 600001, 64, 86400000, 172800000),
                lifetimeRequirements(3, 86400000, 600000, 63, 86400000, 172800000),
                lifetimeRequirements(3, 86400000, 600000, 64, 86400001, 172800000),
                lifetimeRequirements(3, 86400000, 600000, 64, 86400000, 172800001))) {
            assertFalse(offered.canServe(required));
            assertTrue(required.canServe(offered));
        }
        assertTrue(offered.canServe(offered));
    }

    @Test
    void activeCoveringDomainWinsOverDrainingDomainAndOptionalNativeAddsNoBindingAuthority() {
        final var offered = new TargetDomainRegistration.Requirements(dispatch(), scope("shared"), repeated(32, 0xCC));
        final var requested = new TargetDomainRegistration.Requirements(
                lifetimeRequirements(1, 3600000, 300000, 128, 60000, 86400000), scope("shared"), null);
        final var second = new TargetKeyCodec.Domain(1, 2);
        final var queue = queue(List.of(
                summary(domain, TargetDomainState.Lifecycle.DRAINING, requested),
                summary(second, TargetDomainState.Lifecycle.ACTIVE, offered)));
        assertEquals(
                second,
                plan(
                                queue,
                                List.of(
                                        new TargetDomainRegistration.BoundDomain(domain, requested),
                                        new TargetDomainRegistration.BoundDomain(second, offered)),
                                requested,
                                2)
                        .domain());
        assertTrue(offered.canServe(requested));
        assertFalse(requested.canServe(offered));
        assertNull(requested.nativePolicyScope());
        assertEquals(1, requested.dispatch().capability().timingCapabilityBits());
        assertEquals(
                StableCode.TARGET_EXECUTION_DOMAIN_DRAINING,
                plan(
                                queue(List.of(summary(domain, TargetDomainState.Lifecycle.DRAINING, offered))),
                                List.of(new TargetDomainRegistration.BoundDomain(domain, offered)),
                                requested,
                                2)
                        .result());
    }

    @Test
    void differentReceiptClusterAndPhysicalAdapterCannotBeHiddenBehindATargetDigest() {
        final var kafka = CanonicalTargetPartition.decode(bytes("kafka.target"));
        final var cap = DeliveryCapabilitySemantic.decode(bytes("kafka.receipt.capability"));
        final var crossCluster = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                OutcomeCapability.KAFKA_TRANSACTIONAL_RECEIPT,
                1,
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                        "different-cluster", UUID.fromString("11223344-5566-7788-99aa-bbccddeeff00"))),
                cap.evidencePartitionCount(),
                cap.minimumEvidenceRetentionMs(),
                cap.minimumDedupHorizonMs(),
                cap.maximumCertifiedProducerKeys(),
                cap.brokerPrerequisiteDigest(),
                cap.sourceLockDigest(),
                cap.adapterConformanceVersion(),
                cap.rejectionClassifierVersion());
        final var candidate = new TargetDispatchCompatibility(
                kafka.id(), repeated(32, 0xAA), 2, crossCluster, repeated(32, 0xBB), 0, 0);
        assertThrows(IllegalArgumentException.class, () -> candidate.requireTargetProjection(kafka));
        final var wrongAdapter =
                new TargetDispatchCompatibility(pulsar.id(), repeated(32, 0xAA), 2, cap, repeated(32, 0xBB), 0, 0);
        assertThrows(IllegalArgumentException.class, () -> wrongAdapter.requireTargetProjection(pulsar));
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(
                        queue(List.of()),
                        List.of(),
                        new TargetDomainRegistration.Requirements(wrongAdapter, scope("empty"), null),
                        1));
    }

    @Test
    void completeControlAndPermitSetsMatchIndependentScopeVectors() {
        final var controls = List.of(
                group(TargetControlScope.GroupKind.BINDING_SEND_CONTROL, 0x11),
                group(TargetControlScope.GroupKind.AUTHORIZATION_CONTROL, 0x33));
        final var shared =
                new TargetControlScope(pulsar.id(), shard, controls, List.of(repeated(32, 0x44), repeated(32, 0x55)));
        assertArrayEquals(bytes("scope.shared"), shared.canonicalBytes());
        assertArrayEquals(bytes("scope.key"), shared.encodedKey());
        assertEquals(
                shared,
                TargetControlScope.decodeForStore(shared.encodedKey(), shared.canonicalBytes(), pulsar.id(), shard));
        for (String name : List.of("empty", "shared", "private", "tenant", "permit")) {
            final var scope = scope(name);
            assertEquals(
                    scope,
                    TargetControlScope.decodeReferenced(scope.digest(), scope.canonicalBytes(), pulsar.id(), shard));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetControlScope.decodeReferenced(
                        shared.digest(),
                        shared.canonicalBytes(),
                        pulsar.id(),
                        new ShardId(shard.routeIncarnation(), 4)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetControlScope.decodeReferenced(
                        repeated(32, 0x11), shared.canonicalBytes(), pulsar.id(), shard));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetControlScope.decodeForStore(
                        TargetKeyCodec.dispatchCompatibility(shared.digest()),
                        shared.canonicalBytes(),
                        pulsar.id(),
                        shard));
    }

    @Test
    void independentPauseCredentialAvailabilityAndPermitGroupsCannotHideTheHealthyBinding() {
        final var old = requirements("shared");
        final var queue = queue(List.of(summary(domain, TargetDomainState.Lifecycle.ACTIVE, old)));
        final byte[] before = queue.canonicalBytes();
        final var independentlyRotatedCredentials = new TargetControlScope(
                pulsar.id(),
                shard,
                List.of(
                        group(TargetControlScope.GroupKind.BINDING_SEND_CONTROL, 0x11),
                        group(TargetControlScope.GroupKind.AUTHORIZATION_CONTROL, 0x34)),
                scope("shared").permits());
        final List<TargetControlScope> variants =
                List.of(scope("private"), scope("tenant"), scope("permit"), independentlyRotatedCredentials);
        for (var independent : variants) {
            final var result = plan(
                    queue,
                    List.of(new TargetDomainRegistration.BoundDomain(domain, old)),
                    new TargetDomainRegistration.Requirements(dispatch(), independent, null),
                    1);
            assertEquals(TargetDomainRegistration.Action.REJECT, result.action());
            assertEquals(StableCode.TARGET_BINDING_INCOMPATIBLE, result.result());
            assertNull(result.domain());
            assertArrayEquals(before, queue.canonicalBytes());
        }
        // No message-tail scan is an input to this decision; C1 will persist the source-ordered rejection.
        assertEquals(
                TargetDomainRegistration.Action.REUSE_DOMAIN,
                plan(queue, List.of(new TargetDomainRegistration.BoundDomain(domain, old)), requirements("shared"), 1)
                        .action());
    }

    @Test
    void emptyQueueBindsOneDomainAndCompatibleProfilesReuseItWithoutMutatingTheSnapshot() {
        final var candidate = requirements("shared");
        final var empty = queue(List.of());
        final var created = plan(empty, List.of(), candidate, 1);
        assertEquals(TargetDomainRegistration.Action.BIND_DOMAIN, created.action());
        assertEquals(domain, created.domain());
        assertEquals(StableCode.OK, created.result());
        assertTrue(empty.domains().isEmpty());
        created.requireQueueSnapshot(empty);
        final var populated = queue(List.of(summary(domain, TargetDomainState.Lifecycle.ACTIVE, candidate)));
        assertThrows(IllegalArgumentException.class, () -> created.requireQueueSnapshot(populated));
        final var reused = plan(
                populated,
                List.of(new TargetDomainRegistration.BoundDomain(domain, candidate)),
                requirements("shared"),
                1);
        assertEquals(TargetDomainRegistration.Action.REUSE_DOMAIN, reused.action());
        assertEquals(domain, reused.domain());
        assertEquals(1, populated.domains().size());
    }

    @Test
    void explicitBoundLimitsIncompatibleDomainsAndKeepsExistingHealthyDomainSelectable() {
        final var a = requirements("shared");
        final var b = requirements("private");
        final var first = queue(List.of(summary(domain, TargetDomainState.Lifecycle.ACTIVE, a)));
        final var plan = plan(first, List.of(new TargetDomainRegistration.BoundDomain(domain, a)), b, 2);
        assertEquals(TargetDomainRegistration.Action.BIND_DOMAIN, plan.action());
        assertEquals(new TargetKeyCodec.Domain(1, 1), plan.domain());
        final var second = queue(List.of(
                summary(domain, TargetDomainState.Lifecycle.DRAINING, a),
                summary(plan.domain(), TargetDomainState.Lifecycle.ACTIVE, b)));
        final var bound = List.of(
                new TargetDomainRegistration.BoundDomain(domain, a),
                new TargetDomainRegistration.BoundDomain(plan.domain(), b));
        assertEquals(plan.domain(), plan(second, bound, b, 2).domain());
        assertEquals(
                StableCode.TARGET_EXECUTION_DOMAIN_LIMIT_EXCEEDED,
                plan(second, bound, requirements("tenant"), 2).result());
        assertThrows(IllegalArgumentException.class, () -> plan(second, bound, b, 1));
        for (int limit : List.of(0, 65)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> plan(first, List.of(new TargetDomainRegistration.BoundDomain(domain, a)), b, limit));
        }
    }

    @Test
    void drainingAndClosedScopesCannotBeBypassedByAllocatingAnotherSlot() {
        final var r = requirements("shared");
        final var summary = summary(domain, TargetDomainState.Lifecycle.DRAINING, r);
        final var stored = List.of(new TargetDomainRegistration.BoundDomain(domain, r));
        assertEquals(
                StableCode.TARGET_EXECUTION_DOMAIN_DRAINING,
                plan(queue(List.of(summary)), stored, r, 2).result());
        final var closed = new TargetQueueState(
                pulsar.id(), 1, 1, TargetQueueState.AdmissionState.CLOSED, repeated(16, 1), 60000, List.of(summary));
        assertEquals(StableCode.TARGET_CLOSED, plan(closed, stored, r, 2).result());
        final var paused = new TargetQueueState(
                pulsar.id(),
                1,
                1,
                TargetQueueState.AdmissionState.PAUSED,
                repeated(16, 1),
                60000,
                List.of(summary(domain, TargetDomainState.Lifecycle.ACTIVE, r)));
        assertEquals(
                TargetDomainRegistration.Action.REUSE_DOMAIN,
                plan(paused, stored, r, 1).action());
        // Pause blocks dispatch, while registration alone neither publishes nor opens the gate.
        assertEquals(TargetQueueState.AdmissionState.PAUSED, paused.admissionState());
    }

    @Test
    void vacantSlotReuseRetainsUnsignedGenerationAndCannotWrap() {
        for (long generation : List.of(1L, Long.MAX_VALUE)) {
            final var vacant = new TargetDomainState(
                    new TargetKeyCodec.Domain(0, generation),
                    TargetDomainState.Lifecycle.VACANT,
                    null,
                    null,
                    null,
                    null,
                    null);
            assertEquals(
                    new TargetKeyCodec.Domain(0, generation + 1),
                    plan(queue(List.of(vacant)), List.of(), requirements("shared"), 1)
                            .domain());
        }
        final var exhausted = new TargetDomainState(
                new TargetKeyCodec.Domain(0, -1), TargetDomainState.Lifecycle.VACANT, null, null, null, null, null);
        assertEquals(
                StableCode.TARGET_EXECUTION_DOMAIN_GENERATION_EXHAUSTED,
                plan(queue(List.of(exhausted)), List.of(), requirements("shared"), 1)
                        .result());
        assertEquals(
                new TargetKeyCodec.Domain(1, 1),
                plan(queue(List.of(exhausted)), List.of(), requirements("shared"), 2)
                        .domain());
        assertEquals(-1L, exhausted.domain().generation());
    }

    @Test
    void missingWrongOrDuplicateStoredReferencesAreIntegrityErrorsRatherThanBusinessRejections() {
        final var a = requirements("shared");
        final var queue = queue(List.of(summary(domain, TargetDomainState.Lifecycle.ACTIVE, a)));
        final var stored = new TargetDomainRegistration.BoundDomain(domain, a);
        assertThrows(IllegalArgumentException.class, () -> plan(queue, List.of(), a, 1));
        assertThrows(IllegalArgumentException.class, () -> plan(queue(List.of()), List.of(stored), a, 1));
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(
                        queue,
                        List.of(new TargetDomainRegistration.BoundDomain(domain, requirements("private"))),
                        a,
                        1));
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(
                        queue,
                        List.of(new TargetDomainRegistration.BoundDomain(new TargetKeyCodec.Domain(0, 2), a)),
                        a,
                        1));
        final var second = new TargetKeyCodec.Domain(1, 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(
                        queue(List.of(
                                summary(domain, TargetDomainState.Lifecycle.ACTIVE, a),
                                summary(second, TargetDomainState.Lifecycle.ACTIVE, a))),
                        List.of(stored, new TargetDomainRegistration.BoundDomain(second, a)),
                        a,
                        2));
        final var wrongShard =
                new TargetControlScope(pulsar.id(), new ShardId(shard.routeIncarnation(), 4), List.of(), List.of());
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(
                        queue(List.of()),
                        List.of(),
                        new TargetDomainRegistration.Requirements(dispatch(), wrongShard, null),
                        1));
    }

    @Test
    void nativeScopeIsAnExactCompatibilityDimensionAndNeverCreatesNativeAuthority() {
        final var first = new TargetDomainRegistration.Requirements(dispatch(), scope("shared"), repeated(32, 0xCC));
        final var candidate =
                new TargetDomainRegistration.Requirements(dispatch(), scope("shared"), repeated(32, 0xCD));
        final var queue = queue(List.of(summary(domain, TargetDomainState.Lifecycle.ACTIVE, first)));
        assertEquals(
                StableCode.TARGET_BINDING_INCOMPATIBLE,
                plan(queue, List.of(new TargetDomainRegistration.BoundDomain(domain, first)), candidate, 1)
                        .result());
        assertThrows(
                IllegalArgumentException.class,
                () -> plan(
                        queue,
                        List.of(new TargetDomainRegistration.BoundDomain(domain, requirements("shared"))),
                        first,
                        1));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetDomainRegistration.Requirements(
                        TargetDispatchCompatibility.decode(bytes("kafka.baseline.dispatch")),
                        scope("shared"),
                        repeated(32, 0xCC)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetDomainRegistration.Requirements(dispatch(), scope("shared"), new byte[32]));
    }

    @Test
    void controlSetsMustBeCompleteCanonicalUniqueAndWithinCountBounds() {
        final var low = group(TargetControlScope.GroupKind.BINDING_SEND_CONTROL, 0x11);
        final var high = group(TargetControlScope.GroupKind.TENANT_SEND_CONTROL, 0x22);
        for (List<TargetControlScope.ControlGroup> invalid : List.of(List.of(low, low), List.of(high, low))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new TargetControlScope(pulsar.id(), shard, invalid, List.of()));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetControlScope.ControlGroup(
                        TargetControlScope.GroupKind.BINDING_SEND_CONTROL, new byte[32]));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetControlScope(pulsar.id(), shard, List.of(), List.of(new byte[32])));
        for (List<byte[]> invalid :
                List.of(List.of(repeated(32, 1), repeated(32, 1)), List.of(repeated(32, 2), repeated(32, 1)))) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new TargetControlScope(pulsar.id(), shard, List.of(), invalid));
        }
        final List<TargetControlScope.ControlGroup> controls = new ArrayList<>();
        final List<byte[]> permits = new ArrayList<>();
        for (int n = 1; n <= 33; n++) {
            controls.add(group(TargetControlScope.GroupKind.BINDING_SEND_CONTROL, n));
            permits.add(repeated(32, n));
        }
        assertThrows(
                IllegalArgumentException.class, () -> new TargetControlScope(pulsar.id(), shard, controls, List.of()));
        assertThrows(
                IllegalArgumentException.class, () -> new TargetControlScope(pulsar.id(), shard, List.of(), permits));
    }

    @Test
    void maximumContractsMatchIndependentHashesAndDerivedBounds() {
        final List<TargetControlScope.ControlGroup> controls = new ArrayList<>();
        final List<byte[]> permits = new ArrayList<>();
        for (int n = 1; n <= 32; n++) {
            controls.add(
                    new TargetControlScope.ControlGroup(TargetControlScope.GroupKind.BINDING_SEND_CONTROL, number(n)));
            permits.add(number(n + 32));
        }
        final var maximum = new TargetControlScope(pulsar.id(), shard, controls, permits);
        assertEquals(TargetControlScope.MAX_CANONICAL_BYTES, maximum.canonicalBytes().length);
        assertEquals(Integer.parseInt(vectors.getProperty("scope.maximum.length")), maximum.canonicalBytes().length);
        assertArrayEquals(bytes("scope.maximum.sha256"), Bytes.sha256(maximum.canonicalBytes()));
        assertEquals(maximum, TargetControlScope.decode(maximum.canonicalBytes()));
        final byte[] token = new byte[32];
        for (int n = 0; n < token.length; n++) {
            token[n] = (byte) n;
        }
        final var evidence = BrokerResourceIdentity.pulsar(
                new PulsarBrokerResourceIdentity("x".repeat(256), token, "x".repeat(1 << 20), Long.MAX_VALUE));
        final var capability = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.PULSAR_BROKER_DEDUP,
                7,
                evidence,
                Integer.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                repeated(32, 0x22),
                repeated(32, 0x33),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE);
        final var dispatch = new TargetDispatchCompatibility(
                pulsar.id(),
                repeated(32, 0xAA),
                Integer.MAX_VALUE,
                capability,
                repeated(32, 0xBB),
                Long.MAX_VALUE,
                Long.MAX_VALUE);
        assertEquals(
                Integer.parseInt(vectors.getProperty("dispatch.maximum.length")), dispatch.canonicalBytes().length);
        assertArrayEquals(bytes("dispatch.maximum.sha256"), Bytes.sha256(dispatch.canonicalBytes()));
        assertTrue(dispatch.canonicalBytes().length <= TargetDispatchCompatibility.MAX_CANONICAL_BYTES);
        assertEquals(dispatch, TargetDispatchCompatibility.decode(dispatch.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetControlScope.decode(new byte[TargetControlScope.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetDispatchCompatibility.decode(
                        new byte[TargetDispatchCompatibility.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void malformedFieldsCannotBecomeValidContractsEvenWithRecomputedOuterDigest() {
        final byte[] dispatch = bytes("pulsar.journal.dispatch");
        for (int n = 1; n <= 9; n++) {
            final int number = n;
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetDispatchCompatibility.decode(
                            rewrite(dispatch, number, null, 9, "nereus-delay-target-dispatch-compatibility\0")),
                    "missing " + n);
        }
        for (int n : List.of(1, 4)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetDispatchCompatibility.decode(
                            rewrite(dispatch, n, uint(n, 0), 9, "nereus-delay-target-dispatch-compatibility\0")));
        }
        for (int n : List.of(3, 6)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetDispatchCompatibility.decode(rewrite(
                            dispatch, n, field(n, new byte[32]), 9, "nereus-delay-target-dispatch-compatibility\0")));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetDispatchCompatibility.decode(
                        rewrite(dispatch, 7, uint(7, -1), 9, "nereus-delay-target-dispatch-compatibility\0")));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetDispatchCompatibility.decode(Bytes.concat(dispatch, uint(10, 1))));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetDispatchCompatibility.decode(Bytes.concat(uint(1, 1), dispatch)));
        final byte[] scope = bytes("scope.shared");
        for (int n : List.of(1, 2, 3, 6)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetControlScope.decode(rewrite(scope, n, null, 6, "nereus-delay-target-control-scope\0")));
        }
        final byte[] invalidGroup = Bytes.concat(uint(1, 4), field(2, repeated(32, 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetControlScope.decode(
                        rewrite(scope, 4, field(4, invalidGroup), 6, "nereus-delay-target-control-scope\0")));
        assertThrows(IllegalArgumentException.class, () -> TargetControlScope.decode(Bytes.concat(scope, uint(7, 1))));
        assertThrows(IllegalArgumentException.class, () -> TargetControlScope.decode(Bytes.concat(uint(1, 1), scope)));
        final byte[] corrupt = scope.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetControlScope.decode(corrupt));
    }

    @Test
    void reservedEnvelopesAndNewStableCodesHaveExactIndependentRegistryValues() {
        for (String name : List.of("dispatch.value", "scope.value")) {
            final var value = bytes(name);
            assertEquals(
                    Bytes.readU32be(value, value.length - 4), Bytes.crc32c(Arrays.copyOf(value, value.length - 4)));
            assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(value));
        }
        assertEquals(18, TargetDispatchCompatibility.VALUE_TYPE);
        assertEquals(19, TargetControlScope.VALUE_TYPE);
        for (String key : vectors.stringPropertyNames()) {
            if (key.startsWith("code.")) {
                final var code = StableCode.valueOf(key.substring(5));
                assertEquals(Integer.parseInt(vectors.getProperty(key)), code.wireValue());
                assertEquals(code, StableCode.fromWire(code.wireValue()));
                assertEquals(Retryability.NEW_PREPARATION_REQUIRED, Retryability.forCode(code));
            }
        }
    }

    @Test
    void callersCannotChangeRetainedScopeNativeReferencesOrQueueSnapshot() {
        final var permits = new ArrayList<>(List.of(repeated(32, 0x44), repeated(32, 0x55)));
        final var scope =
                new TargetControlScope(pulsar.id(), shard, scope("shared").controls(), permits);
        final byte[] before = scope.canonicalBytes();
        Arrays.fill(permits.getFirst(), (byte) 0);
        permits.clear();
        Arrays.fill(scope.permits().getFirst(), (byte) 0);
        Arrays.fill(scope.controls().getFirst().id(), (byte) 0);
        assertArrayEquals(before, scope.canonicalBytes());
        assertThrows(UnsupportedOperationException.class, () -> scope.controls().clear());
        final byte[] nativeScope = repeated(32, 0xCC);
        final var candidate = new TargetDomainRegistration.Requirements(dispatch(), scope, nativeScope);
        Arrays.fill(nativeScope, (byte) 0);
        assertEquals(new TargetDomainRegistration.Requirements(dispatch(), scope, repeated(32, 0xCC)), candidate);
        Arrays.fill(candidate.nativePolicyScope(), (byte) 0);
        final var queue = queue(List.of());
        final var plan = plan(queue, List.of(), candidate, 1);
        Arrays.fill(plan.expectedQueueDigest(), (byte) 0);
        assertDoesNotThrow(() -> plan.requireQueueSnapshot(queue));
    }

    private ProfileSemanticEnvelope destination(
            final String id,
            final long version,
            final ProfileSemanticEnvelope cap,
            final int buckets,
            final int recordLimit,
            final int payloadLimit) {
        final var body = new DestinationProfileSemantic(
                AdapterKind.PULSAR,
                pulsar.resource(),
                8,
                TargetPartitionPolicy.EXPLICIT_ONLY,
                TargetPartitionHashInput.DELAY_MESSAGE_ID,
                List.of(5),
                cap.ref(),
                3,
                60000,
                repeated(32, 0xAA),
                recordLimit,
                1024,
                payloadLimit,
                buckets,
                Bytes.utf8(id),
                86400000,
                172800000,
                2,
                repeated(32, 0xBB));
        return new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8(id), version, body);
    }

    private TargetDispatchCompatibility lifetimeRequirements(
            final int timing,
            final long evidenceRetention,
            final long dedupHorizon,
            final long producerCeiling,
            final long ttl,
            final long retention) {
        final var cap = capability();
        return new TargetDispatchCompatibility(
                pulsar.id(),
                repeated(32, 0xAA),
                2,
                new DeliveryCapabilitySemantic(
                        cap.adapterKind(),
                        cap.outcomeCapability(),
                        timing,
                        cap.evidenceResource(),
                        cap.evidencePartitionCount(),
                        evidenceRetention,
                        dedupHorizon,
                        producerCeiling,
                        cap.brokerPrerequisiteDigest(),
                        cap.sourceLockDigest(),
                        cap.adapterConformanceVersion(),
                        cap.rejectionClassifierVersion()),
                repeated(32, 0xBB),
                ttl,
                retention);
    }

    private DeliveryCapabilitySemantic capability() {
        return DeliveryCapabilitySemantic.decode(bytes("pulsar.journal.capability"));
    }

    private TargetDispatchCompatibility dispatch() {
        return TargetDispatchCompatibility.decode(bytes("pulsar.journal.dispatch"));
    }

    private TargetControlScope scope(final String name) {
        return TargetControlScope.decode(bytes("scope." + name));
    }

    private TargetDomainRegistration.Requirements requirements(final String name) {
        return new TargetDomainRegistration.Requirements(dispatch(), scope(name), null);
    }

    private static TargetControlScope.ControlGroup group(final TargetControlScope.GroupKind kind, final int id) {
        return new TargetControlScope.ControlGroup(kind, repeated(32, id));
    }

    private TargetQueueState queue(final List<TargetDomainState> domains) {
        return new TargetQueueState(
                pulsar.id(), 1, 1, TargetQueueState.AdmissionState.OPEN, repeated(16, 1), 60000, domains);
    }

    private static TargetDomainState summary(
            final TargetKeyCodec.Domain domain,
            final TargetDomainState.Lifecycle life,
            final TargetDomainRegistration.Requirements requirements) {
        return new TargetDomainState(
                domain,
                life,
                requirements.dispatch().digest(),
                requirements.controls().digest(),
                requirements.nativePolicyScope(),
                null,
                null);
    }

    private TargetDomainRegistration.Plan plan(
            final TargetQueueState queue,
            final List<TargetDomainRegistration.BoundDomain> bound,
            final TargetDomainRegistration.Requirements candidate,
            final int maxSlots) {
        return TargetDomainRegistration.plan(pulsar, queue, shard, bound, candidate, maxSlots);
    }

    private byte[] bytes(final String name) {
        return HexFormat.of().parseHex(vectors.getProperty(name));
    }

    private static byte[] repeated(final int length, final int value) {
        final byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] number(final int value) {
        final byte[] bytes = new byte[32];
        bytes[31] = (byte) value;
        return bytes;
    }

    private static byte[] field(final int number, final byte[] value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, number, value));
    }

    private static byte[] uint(final int number, final long value) {
        return CanonicalProtobuf.message(out -> CanonicalProtobuf.uint64Bits(out, number, value));
    }

    private static byte[] rewrite(
            final byte[] encoded,
            final int number,
            final byte[] replacement,
            final int hashField,
            final String hashDomain) {
        final var fields = QueryCodecSupport.read(encoded, "fixture", true);
        final byte[] raw = CanonicalProtobuf.message(out -> {
            boolean replaced = false;
            for (var f : fields) {
                if (f.number() == hashField) {
                    continue;
                }
                if (f.number() == number) {
                    if (!replaced && replacement != null) {
                        out.writeBytes(replacement);
                    }
                    replaced = true;
                } else {
                    out.writeBytes(
                            f.wireType() == 2 ? field(f.number(), f.rawValue()) : uint(f.number(), f.unsignedValue()));
                }
            }
        });
        return number == hashField
                ? raw
                : Bytes.concat(raw, field(hashField, Bytes.sha256(Bytes.utf8(hashDomain), raw)));
    }

    private static Properties vectors() {
        try (var in = TargetCompatibilityContractTest.class.getResourceAsStream(
                "/ndip3/target-compatibility-vectors.properties")) {
            final var result = new Properties();
            result.load(in);
            return result;
        } catch (IOException error) {
            throw new AssertionError(error);
        }
    }
}
