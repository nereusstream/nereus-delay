package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.scheduler.TargetNativePolicyChecks;
import com.nereusstream.delay.scheduler.TargetNativePolicyChecks.Action;
import com.nereusstream.delay.scheduler.TargetNativePolicyChecks.Reason;
import com.nereusstream.delay.semantic.TargetNativePolicyAuthority;
import com.nereusstream.delay.semantic.TargetNativePolicyTrust;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.ValueEnvelope;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class TargetNativePolicyContractTest {
    private final Properties vectors = load("target-native-policy-vectors.properties");
    private final Properties compat = load("target-compatibility-vectors.properties");
    private final Properties bindings = load("target-binding-channel-vectors.properties");
    private final TargetNativeArtifactSet artifacts = TargetNativeArtifactSet.decode(hex(vectors, "artifacts"));
    private final TargetNativePolicyScope scope = TargetNativePolicyScope.decode(hex(vectors, "pulsar.journal.scope"));
    private final CanonicalTargetPartition physical = CanonicalTargetPartition.decode(hex(compat, "pulsar.target"));
    private final TargetDispatchCompatibility dispatch =
            TargetDispatchCompatibility.decode(hex(compat, "pulsar.journal.dispatch"));
    private final TargetControlScope controls = TargetControlScope.decode(hex(compat, "scope.shared"));
    private final KafkaSourcePosition source =
            (KafkaSourcePosition) TargetSourcePosition.decode(hex(bindings, "source"));
    private final KeyPair keys = keyPair();
    private final ProfileSemanticEnvelope capability =
            new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("cap"), 1, dispatch.capability());
    private final ControlRoleSet roles =
            ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR, ControlRole.PLATFORM_OPERATOR);
    private final ControlAuthorizationContext actor =
            new ControlAuthorizationContext(repeat(32, 0x91), roles, scope.controlResourceScope());

    @Test
    void artifactsAndBothPulsarScopesMatchIndependentBytesAndKeys() {
        assertArrayEquals(
                hex(vectors, "artifacts"),
                new TargetNativeArtifactSet(repeat(32, 0x81), PulsarSourceLock.digest(), 7).canonicalBytes());
        for (String name : List.of("pulsar.baseline", "pulsar.journal")) {
            final var d = TargetDispatchCompatibility.decode(hex(compat, name + ".dispatch"));
            final var value = new TargetNativePolicyScope(
                    repeat(32, 0x82),
                    repeat(32, 0x74),
                    source.shardId(),
                    physical.id(),
                    repeat(16, 1),
                    new TargetKeyCodec.Domain(0, 1),
                    d.digest(),
                    controls.digest(),
                    60000,
                    artifacts);
            assertArrayEquals(hex(vectors, name + ".scope"), value.canonicalBytes());
            assertArrayEquals(hex(vectors, name + ".key"), value.encodedKey());
            assertEquals(
                    value,
                    TargetNativePolicyScope.decodeForStore(
                            value.encodedKey(), value.canonicalBytes(), source.shardId()));
            value.requireReferences(physical, d, controls);
        }
    }

    @Test
    void allModesMatchIndependentEd25519SnapshotHeadAndReferenceVectors() {
        for (HandoffPolicyMode mode : HandoffPolicyMode.values()) {
            final String name = mode.name().toLowerCase(Locale.ROOT);
            final var snapshot = snapshot(1, mode, mode == HandoffPolicyMode.DISABLED ? 0 : 30000, 120000);
            assertArrayEquals(hex(vectors, name + ".snapshot"), snapshot.canonicalBytes());
            assertArrayEquals(hex(vectors, name + ".snapshotDigest"), snapshot.snapshotDigest());
            assertArrayEquals(hex(vectors, name + ".key"), snapshot.encodedKey());
            assertTrue(snapshot.verifySignature(keys.getPublic()));
            final var decoded = TargetNativePolicySnapshot.decodeForStore(
                    snapshot.encodedKey(), snapshot.canonicalBytes(), scope, source.shardId());
            assertEquals(snapshot, decoded);
            final var head = TargetNativePolicyHead.next(null, snapshot);
            assertArrayEquals(hex(vectors, name + ".head"), head.canonicalBytes());
            assertEquals(head, TargetNativePolicyHead.decode(head.canonicalBytes()));
        }
        assertArrayEquals(
                hex(vectors, "enabled.headRef"),
                snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000).headRef(5).canonicalBytes());
    }

    @Test
    void fullNamespaceSourceDomainControlCapAndArtifactsEnterScopeIdentity() {
        final byte[] raw = scope.canonicalBytes();
        for (int field : List.of(2, 3, 4, 5, 6, 9, 10)) {
            final byte[] changed = QueryCodecSupport.bytes(
                    QueryCodecSupport.field(QueryCodecSupport.read(raw, "scope"), field), field);
            changed[0] ^= 1;
            final var value = TargetNativePolicyScope.decode(rehash(
                    rewrite(raw, Map.of(field, changed), Map.of()), 14, "nereus-delay-target-native-policy-scope"));
            assertNotEquals(scope, value);
        }
        for (int field : List.of(7, 8, 11)) {
            final var value = TargetNativePolicyScope.decode(rehash(
                    rewrite(raw, Map.of(), Map.of(field, field == 7 ? 1L : 2L)),
                    14,
                    "nereus-delay-target-native-policy-scope"));
            assertNotEquals(scope, value);
        }
        assertNotEquals(scope.artifacts(), new TargetNativeArtifactSet(repeat(32, 0x81), PulsarSourceLock.digest(), 8));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetNativePolicyScope(
                        scope.authorityNamespace(),
                        scope.controlResourceScope(),
                        scope.sourceShard(),
                        scope.target(),
                        scope.accountingIncarnation(),
                        new TargetKeyCodec.Domain(64, 1),
                        scope.dispatchRef(),
                        scope.controlRef(),
                        60000,
                        artifacts));
    }

    @Test
    void canonicalLimitsAndAllMaximumVectorsIncludeUnsignedGenerationsAndSignedTime() {
        final var a = TargetNativeArtifactSet.decode(hex(vectors, "maximum.artifacts"));
        final var s = TargetNativePolicyScope.decode(hex(vectors, "maximum.scope"));
        final var snap = TargetNativePolicySnapshot.decode(hex(vectors, "maximum.snapshot"));
        final var head = TargetNativePolicyHead.decode(hex(vectors, "maximum.head"));
        assertEquals(-1L, a.environmentResetGeneration());
        assertEquals(-1L, s.domain().generation());
        assertEquals(-1, s.sourceShard().partition());
        assertEquals(-1, snap.issuerKeyGeneration());
        assertEquals(-1L, snap.generation());
        assertTrue(snap.verifySignature(keys.getPublic()));
        snap.requireScope(s);
        checkMaximum("artifacts", a.canonicalBytes(), TargetNativeArtifactSet.MAX_CANONICAL_BYTES);
        checkMaximum("scope", s.canonicalBytes(), TargetNativePolicyScope.MAX_CANONICAL_BYTES);
        checkMaximum("snapshot", snap.canonicalBytes(), TargetNativePolicySnapshot.MAX_CANONICAL_BYTES);
        checkMaximum("head", head.canonicalBytes(), TargetNativePolicyHead.MAX_CANONICAL_BYTES);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyScope.decode(new byte[TargetNativePolicyScope.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicySnapshot.decode(new byte[TargetNativePolicySnapshot.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativeArtifactSet.decode(new byte[TargetNativeArtifactSet.MAX_CANONICAL_BYTES + 1]));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyHead.decode(new byte[TargetNativePolicyHead.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void closedCodecsRejectUnknownMissingAndTamperedFieldsEvenWithFreshDigests() {
        assertThrows(
                IllegalArgumentException.class, () -> new TargetNativeArtifactSet(repeat(32, 0x81), repeat(32, 1), 7));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetNativeArtifactSet(repeat(32, 0x81), PulsarSourceLock.digest(), 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativeArtifactSet.decode(rehash(
                        rewrite(artifacts.canonicalBytes(), Map.of(), Map.of(6, 1L)),
                        8,
                        "nereus-delay-target-native-artifacts")));
        final byte[] raw = scope.canonicalBytes();
        for (int field : List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)) {
            assertThrows(IllegalArgumentException.class, () -> TargetNativePolicyScope.decode(omit(raw, field)));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyScope.decode(
                        Bytes.concat(raw, CanonicalProtobuf.message(o -> CanonicalProtobuf.uint32(o, 15, 1)))));
        for (long path : List.of(0L, 2L, 3L, 4L)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetNativePolicyScope.decode(rehash(
                            rewrite(raw, Map.of(), Map.of(12, path)), 14, "nereus-delay-target-native-policy-scope")));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyScope.decode(rewrite(raw, Map.of(), Map.of(11, 60001L))));
        final var snap = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        for (int field : List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> TargetNativePolicySnapshot.decode(omit(snap.canonicalBytes(), field)));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicySnapshot.create(
                        scope, 1, HandoffPolicyMode.ENABLED, 60001, 1000, 120000, 1, issued(), 9, keys.getPrivate()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicySnapshot.create(
                        scope, 1, HandoffPolicyMode.SHADOW, 0, 1000, 120000, 0, issued(), 9, keys.getPrivate()));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicySnapshot.create(
                        scope, 1, HandoffPolicyMode.DISABLED, 1, 1000, 120000, 0, issued(), 9, keys.getPrivate()));
        final var oversizedTime = new TrustedUtcIntervalEvidence(
                100,
                101,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                new byte[257],
                1,
                2,
                3,
                repeat(32, 0x44),
                0,
                null);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicySnapshot.create(
                        scope, 1, HandoffPolicyMode.ENABLED, 1, 1000, 120000, 1, oversizedTime, 9, keys.getPrivate()));
    }

    @Test
    void legacyReadersAndSignatureDomainsCannotReinterpretTargetNativeAuthority() {
        final var old = HandoffPolicySnapshot.create(
                scope.digest(),
                1,
                HandoffPolicyMode.ENABLED,
                30000,
                1000,
                120000,
                1,
                issued(),
                9,
                artifacts.digest(),
                keys.getPrivate());
        final var target = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        assertThrows(IllegalArgumentException.class, () -> TargetNativePolicySnapshot.decode(old.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> HandoffPolicySnapshot.decode(target.canonicalBytes()));
        assertThrows(
                IllegalArgumentException.class,
                () -> HandoffPolicyHead.decode(
                        TargetNativePolicyHead.next(null, target).canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> ArtifactGenerationSet.decode(artifacts.canonicalBytes()));
        final byte[] relabeled = rewrite(target.canonicalBytes(), Map.of(13, old.signature()), Map.of());
        assertFalse(TargetNativePolicySnapshot.decode(relabeled).verifySignature(keys.getPublic()));
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decode(hex(vectors, "scope.value"), 24));
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decode(hex(vectors, "snapshot.value"), 25));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyScope.decodeForStore(
                        target.encodedKey(), scope.canonicalBytes(), source.shardId()));
        assertThrows(IllegalArgumentException.class, () -> target.headRef(0));
    }

    @Test
    void exactSourceApprovedPublisherAndBothRolesAreMandatory() {
        final var trust = new Trust();
        final var store = new Heads();
        final var snap = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        final var permission = trust.permission;
        assertEquals(
                1,
                store.publish(permission, trust, actor, (p, a, s) -> true, at(10), 0, snap)
                        .revision());
        final var missing = new Trust();
        missing.permission = null;
        assertThrows(IllegalArgumentException.class, () -> new Heads()
                .publish(permission, missing, actor, (p, a, s) -> true, at(10), 0, snap));
        for (ControlRoleSet absent : List.of(
                ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR),
                ControlRoleSet.of(ControlRole.TENANT_POLICY_ADMINISTRATOR))) {
            final var other =
                    new ControlAuthorizationContext(actor.actorIdHash(), absent, actor.tenantResourceScopeHash());
            assertThrows(IllegalArgumentException.class, () -> new Heads()
                    .publish(permission, trust, other, (p, a, s) -> true, at(10), 0, snap));
        }
        assertThrows(IllegalArgumentException.class, () -> new Heads()
                .publish(permission, trust, actor, (p, a, s) -> false, at(10), 0, snap));
        final var forged = new TargetNativePolicyTrust.PublisherPermission(
                scope, permission.author(), 9, keys.getPublic(), 999999, at(1));
        assertThrows(IllegalArgumentException.class, () -> new Heads()
                .publish(forged, trust, actor, (p, a, s) -> true, at(10), 0, snap));
        assertThrows(IllegalArgumentException.class, () -> new Heads()
                .publish(permission, trust, actor, (p, a, s) -> true, at(0), 0, snap));
    }

    @Test
    void trustBindsExactSignedSnapshotFullSourceAndHistoricalPublisherPermission() {
        final var trust = new Trust();
        final var snapshot = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        trust.activate(snapshot, at(2));
        trust.requireTrusted(scope, snapshot, at(10));
        assertThrows(
                IllegalArgumentException.class,
                () -> trust.requireTrusted(scope, snapshot(1, HandoffPolicyMode.ENABLED, 15000, 120000), at(10)));
        assertThrows(IllegalArgumentException.class, () -> trust.requireTrusted(scope, snapshot, at(1)));
        final var differentSource =
                new KafkaSourcePosition(source.shardId(), "other-cluster", source.nativeTopicUuid(), 10, null, 90);
        assertThrows(IllegalArgumentException.class, () -> trust.requireTrusted(scope, snapshot, differentSource));
        final var sameOffsetDifferentTime = new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                2,
                source.leaderEpoch(),
                999);
        assertThrows(
                IllegalArgumentException.class, () -> trust.requireTrusted(scope, snapshot, sameOffsetDifferentTime));
        trust.permission = new TargetNativePolicyTrust.PublisherPermission(
                scope, trust.permission.author(), 9, newKeys().getPublic(), 119000, at(1));
        assertThrows(IllegalArgumentException.class, () -> trust.requireTrusted(scope, snapshot, at(10)));
    }

    @Test
    void publicationCasRejectsStaleRevisionsGenerationSkipsAndWrapAndRetainsDisableWatermark() {
        final var trust = new Trust();
        final var store = new Heads();
        final var first = store.publish(
                trust.permission,
                trust,
                actor,
                (p, a, s) -> true,
                at(10),
                0,
                snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000));
        assertThrows(
                IllegalStateException.class,
                () -> store.publish(
                        trust.permission,
                        trust,
                        actor,
                        (p, a, s) -> true,
                        at(11),
                        0,
                        snapshot(2, HandoffPolicyMode.DISABLED, 0, 100000)));
        assertThrows(
                IllegalArgumentException.class,
                () -> store.publish(
                        trust.permission,
                        trust,
                        actor,
                        (p, a, s) -> true,
                        at(11),
                        1,
                        snapshot(3, HandoffPolicyMode.DISABLED, 0, 100000)));
        final var next = store.publish(
                trust.permission,
                trust,
                actor,
                (p, a, s) -> true,
                at(11),
                1,
                snapshot(2, HandoffPolicyMode.DISABLED, 0, 120000));
        assertEquals(120000, next.head().authorizedLeaseUntilEpochMs());
        assertArrayEquals(
                hex(vectors, "disabled.after.enabled.head"), next.head().canonicalBytes());
        final var max = TargetNativePolicyHead.decode(hex(vectors, "maximum.head"));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyHead.next(max, snapshot(1, HandoffPolicyMode.DISABLED, 0, 120000)));
        assertFalse(first.sameHead(next));
    }

    @Test
    void commonScopeAllowsDifferentProfilesAndLargerPinnedCapsWithoutAddingPolicyPrefixes() {
        final var trust = new Trust();
        for (ProfileSemanticEnvelope dest : List.of(destination("A", 60000), destination("B", 120000))) {
            final var grant = grant(dest);
            final var binding =
                    binding(dest, grant, NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF, true, OrderingMode.BEST_EFFORT);
            trust.approval = new TargetNativePolicyTrust.MemberApproval(grant, scope, at(4), null);
            assertEquals(Reason.ELIGIBLE, classify(binding, grant, dest, trust));
            assertArrayEquals(scope.digest(), binding.nativePolicyScopeRef());
        }
    }

    @Test
    void smallCapMissingCommonApprovalAndForbidFallBackWithoutReadingCurrentProfilePolicy() {
        final var trust = new Trust();
        final var small = destination("small", 5000);
        final var smallGrant = grant(small);
        assertEquals(
                Reason.PINNED_CAP_TOO_SMALL,
                classify(
                        binding(
                                small,
                                smallGrant,
                                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                                true,
                                OrderingMode.BEST_EFFORT),
                        smallGrant,
                        small,
                        trust));
        final var dest = destination("member", 60000);
        final var grant = grant(dest);
        assertEquals(
                Reason.NO_COMMON_MEMBERSHIP,
                classify(
                        binding(
                                dest,
                                grant,
                                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                                true,
                                OrderingMode.BEST_EFFORT),
                        grant,
                        dest,
                        trust));
        assertEquals(
                Reason.NO_COMMON_SCOPE,
                classify(
                        binding(
                                dest,
                                grant,
                                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                                false,
                                OrderingMode.BEST_EFFORT),
                        grant,
                        dest,
                        trust));
        assertEquals(
                Reason.FORBIDDEN,
                classify(
                        binding(dest, grant, NativeDeliveryPolicy.FORBID, false, OrderingMode.BEST_EFFORT),
                        grant,
                        dest,
                        trust));
    }

    @Test
    void nativeMemberClosureStopsOnlyNewBindingsAndCannotBecomeAnIndependentRuntimeSwitch() {
        final var trust = new Trust();
        final var dest = destination("A", 60000);
        final var grant = grant(dest);
        final var binding =
                binding(dest, grant, NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF, true, OrderingMode.BEST_EFFORT);
        trust.approval = new TargetNativePolicyTrust.MemberApproval(grant, scope, at(4), at(12));
        assertEquals(Reason.ELIGIBLE, classify(binding, grant, dest, trust));
        assertFalse(trust.approval.allowsFirstBinding(at(12)));
        final var snapshot = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        trust.activate(snapshot, at(2));
        trust.failMember = true;
        assertEquals(
                Action.NATIVE_CANDIDATE, resolve(snapshot, trust, 70000, 70000).action());
    }

    @Test
    void pinnedIntegrityAndForbiddenOrderingAreErrorsRatherThanOptimizationFallbacks() {
        final var trust = new Trust();
        final var dest = destination("A", 60000);
        final var grant = grant(dest);
        final var binding =
                binding(dest, grant, NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF, true, OrderingMode.BEST_EFFORT);
        assertThrows(
                IllegalArgumentException.class, () -> classify(binding, grant, destination("other", 60000), trust));
        assertThrows(
                IllegalArgumentException.class,
                () -> binding(
                        dest,
                        grant,
                        NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                        false,
                        OrderingMode.DELIVERY_TIME_FIFO));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyChecks.firstBinding(
                        binding, repeat(32, 1), grant, physical, dest, capability, queue(), scope, trust));
        trust.failMember = true;
        assertThrows(IllegalStateException.class, () -> classify(binding, grant, dest, trust));
    }

    @Test
    void modesAndExpiryNeverBlockOrdinaryDueOrCreateNewNativeWork() {
        final var trust = new Trust();
        for (HandoffPolicyMode mode : HandoffPolicyMode.values()) {
            final var snapshot = snapshot(1, mode, mode == HandoffPolicyMode.DISABLED ? 0 : 30000, 120000);
            trust.activate(snapshot, at(2));
            final var decision = resolve(snapshot, trust, 70000, 70000);
            assertEquals(
                    mode == HandoffPolicyMode.ENABLED ? Action.NATIVE_CANDIDATE : Action.WAIT_UNTIL, decision.action());
            assertEquals(mode == HandoffPolicyMode.SHADOW, decision.shadowWouldBeEligible());
            assertEquals(
                    Action.ORDINARY_DUE,
                    resolve(snapshot, trust, 100000, 100000).action());
            assertEquals(
                    Action.ORDINARY_DUE,
                    resolve(snapshot, trust, 120000, 120000).action());
        }
        trust.failActivation = true;
        assertEquals(
                Action.ORDINARY_DUE,
                resolve(snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000), trust, 100000, 100000)
                        .action());
        assertEquals(
                Reason.POLICY_UNTRUSTED,
                resolve(snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000), trust, 70000, 70000)
                        .reason());
    }

    @Test
    void commonLeadChangesOnlyEligibilityAndPreservesTheScopeAndStaticIndexOrder() {
        final var trust = new Trust();
        final var first = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        trust.activate(first, at(2));
        assertEquals(
                Action.NATIVE_CANDIDATE, resolve(first, trust, 70000, 70000).action());
        final var next = snapshot(2, HandoffPolicyMode.ENABLED, 15000, 120000);
        trust.activate(next, at(3));
        final var decision = resolve(next, trust, 70000, 70000);
        assertEquals(Action.WAIT_UNTIL, decision.action());
        assertEquals(85000, decision.wakeAtEpochMs());
        assertArrayEquals(first.policyScopeDigest(), next.policyScopeDigest());
        assertNotEquals(first.snapshotDigest(), next.snapshotDigest());
    }

    @Test
    void trustedIntervalsMustFitBothNativeEligibilityAndLeaseBoundaries() {
        final var trust = new Trust();
        final var snapshot = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 90000);
        trust.activate(snapshot, at(2));
        assertEquals(Action.WAIT_UNTIL, resolve(snapshot, trust, 69998, 69999).action());
        assertEquals(
                Action.TIME_SAMPLE_REQUIRED,
                resolve(snapshot, trust, 69999, 70000).action());
        assertEquals(
                Action.NATIVE_CANDIDATE, resolve(snapshot, trust, 70000, 70000).action());
        assertEquals(
                Action.TIME_SAMPLE_REQUIRED,
                resolve(snapshot, trust, 89999, 90000).action());
        assertEquals(
                Reason.POLICY_EXPIRED, resolve(snapshot, trust, 90000, 90000).reason());
        final var pub = new TargetNativePolicyAuthority.Publication(1, TargetNativePolicyHead.next(null, snapshot));
        assertEquals(
                Reason.NOT_INITIAL_ATTEMPT,
                TargetNativePolicyChecks.resolve(scope, false, 100000, 100001, pub, trust, at(10), time(70000, 70000))
                        .reason());
    }

    @Test
    void admissionRereadsExactCurrentHeadAndFreezesTheCompleteSnapshot() {
        final var trust = new Trust();
        final var store = new Heads();
        final var snapshot = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        trust.activate(snapshot, at(2));
        final var pub = store.publish(trust.permission, trust, actor, (p, a, s) -> true, at(10), 0, snapshot);
        assertEquals(
                snapshot,
                TargetNativePolicyChecks.freezeCurrent(
                        scope,
                        pub.head().ref(pub.revision()),
                        100000,
                        70000,
                        store,
                        trust,
                        at(10),
                        time(70000, 70000)));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyChecks.freezeCurrent(
                        scope, snapshot.headRef(2), 100000, 70000, store, trust, at(10), time(70000, 70000)));
        store.changeAfterRead = true;
        store.reads = 0;
        assertThrows(
                IllegalStateException.class,
                () -> TargetNativePolicyChecks.freezeCurrent(
                        scope, pub.head().ref(1), 100000, 70000, store, trust, at(10), time(70000, 70000)));
    }

    @Test
    void frozenAdmissionUsesHistoricalTrustUntilLeaseEndWithoutRereadingDisabledHead() {
        final var trust = new Trust();
        final var snapshot = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        trust.activate(snapshot, at(2));
        trust.activate(snapshot(2, HandoffPolicyMode.DISABLED, 0, 120000), at(3));
        TargetNativePolicyChecks.requireFrozen(
                scope, snapshot, 100000, 70000, trust, at(10), time(119999, 119999), false);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyChecks.requireFrozen(
                        scope, snapshot, 100000, 70000, trust, at(10), time(120000, 120000), false));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyChecks.requireFrozen(
                        scope, snapshot, 100000, 70001, trust, at(10), time(70001, 70001), true));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetNativePolicyChecks.requireFrozen(
                        scope, snapshot, 100000, 70000, trust, at(10), time(99999, 100000), true));
    }

    @Test
    void noCurrentPublicationCannotAuthorizeNativeButOrdinaryStillUsesItsOwnDueTime() {
        assertEquals(
                Action.WAIT_UNTIL,
                TargetNativePolicyChecks.resolve(scope, true, 100000, 0, null, null, at(10), time(70000, 70000))
                        .action());
        assertEquals(
                Action.ORDINARY_DUE,
                TargetNativePolicyChecks.resolve(scope, true, 100000, 0, null, null, at(10), time(100000, 100000))
                        .action());
        assertEquals(
                Action.TIME_SAMPLE_REQUIRED,
                TargetNativePolicyChecks.resolve(scope, true, 100000, 0, null, null, at(10), time(99999, 100000))
                        .action());
    }

    @Test
    void mutableArraysCannotAlterScopeArtifactsSnapshotOrHeadIdentity() {
        final var snapshot = snapshot(1, HandoffPolicyMode.ENABLED, 30000, 120000);
        final byte[] original = snapshot.canonicalBytes();
        for (byte[] a : List.of(
                snapshot.signature(),
                snapshot.snapshotDigest(),
                snapshot.policyScopeDigest(),
                snapshot.artifactGenerationSetDigest(),
                scope.digest(),
                scope.authorityNamespace(),
                scope.controlResourceScope(),
                scope.accountingIncarnation(),
                scope.dispatchRef(),
                scope.controlRef(),
                artifacts.schemaBundleHash(),
                artifacts.sourceLock(),
                artifacts.digest())) {
            a[0] ^= 1;
        }
        assertArrayEquals(original, snapshot.canonicalBytes());
        assertTrue(snapshot.verifySignature(keys.getPublic()));
        assertEquals(scope, TargetNativePolicyScope.decode(hex(vectors, "pulsar.journal.scope")));
    }

    private TargetNativePolicyChecks.Decision resolve(
            TargetNativePolicySnapshot snapshot, Trust trust, long earliest, long latest) {
        return TargetNativePolicyChecks.resolve(
                scope,
                true,
                100000,
                0,
                new TargetNativePolicyAuthority.Publication(
                        1,
                        new TargetNativePolicyHead(
                                snapshot,
                                snapshot.mode() == HandoffPolicyMode.ENABLED ? snapshot.validUntilEpochMs() : 0)),
                trust,
                at(10),
                time(earliest, latest));
    }

    private Reason classify(
            TargetScheduleBinding binding, TargetMembershipGrant grant, ProfileSemanticEnvelope dest, Trust trust) {
        return TargetNativePolicyChecks.firstBinding(
                binding, grant.tenantScope(), grant, physical, dest, capability, queue(), scope, trust);
    }

    private ProfileSemanticEnvelope destination(String name, long cap) {
        return new ProfileSemanticEnvelope(
                ProfileKind.DESTINATION,
                Bytes.utf8(name),
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
                        cap,
                        repeat(32, 0xaa),
                        20000,
                        10000,
                        10000,
                        1,
                        Bytes.utf8("member"),
                        86400000,
                        172800000,
                        2,
                        repeat(32, 0xbb)));
    }

    private TargetMembershipGrant grant(ProfileSemanticEnvelope dest) {
        return new TargetMembershipGrant(
                repeat(32, 0x70),
                dest.ref(),
                TargetDispatchCompatibility.fromProfiles(physical, dest, capability),
                dispatch,
                controls,
                repeat(32, 0x71),
                repeat(32, 0x72),
                repeat(32, 0x73),
                at(3));
    }

    private TargetScheduleBinding binding(
            ProfileSemanticEnvelope dest,
            TargetMembershipGrant grant,
            NativeDeliveryPolicy policy,
            boolean nativeRef,
            OrderingMode ordering) {
        final var old = TargetScheduleBinding.decode(
                hex(bindings, ordering == OrderingMode.BEST_EFFORT ? "binding.native" : "binding.strict"));
        final byte[] intent = rewrite(
                old.intent().canonicalBytes(),
                Map.of(1, dest.ref().canonicalBytes()),
                Map.of(3, 100000L, 4, 200000L, 14, (long) policy.wireValue()));
        final byte[] body = rewrite(old.canonicalBody(), Map.of(10, intent), Map.of());
        return new TargetScheduleBinding(
                old.messageId(),
                old.commandType(),
                body,
                at(10),
                physical.id(),
                scope.domain(),
                scope.accountingIncarnation(),
                grant.required().digest(),
                dispatch.digest(),
                controls.digest(),
                grant.digest(),
                nativeRef ? scope.digest() : null,
                old.orderingDomain());
    }

    private TargetQueueState queue() {
        return new TargetQueueState(
                physical.id(),
                1,
                1,
                TargetQueueState.AdmissionState.OPEN,
                scope.accountingIncarnation(),
                60000,
                List.of(new TargetDomainState(
                        scope.domain(),
                        TargetDomainState.Lifecycle.ACTIVE,
                        dispatch.digest(),
                        controls.digest(),
                        scope.digest(),
                        null,
                        null)));
    }

    private TargetNativePolicySnapshot snapshot(long generation, HandoffPolicyMode mode, long lead, long until) {
        return TargetNativePolicySnapshot.create(
                scope,
                generation,
                mode,
                lead,
                1000,
                until,
                mode == HandoffPolicyMode.DISABLED ? 0 : 1,
                issued(),
                9,
                keys.getPrivate());
    }

    private TrustedUtcIntervalEvidence issued() {
        return time(100, 101);
    }

    private TrustedUtcIntervalEvidence time(long earliest, long latest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                2,
                3,
                repeat(32, 0x44),
                0,
                null);
    }

    private KafkaSourcePosition at(long offset) {
        return new KafkaSourcePosition(
                source.shardId(),
                source.authenticatedClusterId(),
                source.nativeTopicUuid(),
                offset,
                source.leaderEpoch(),
                source.brokerLogAppendTimeEpochMs());
    }

    private KeyPair keyPair() {
        try {
            final var factory = KeyFactory.getInstance("Ed25519");
            return new KeyPair(
                    factory.generatePublic(new X509EncodedKeySpec(Bytes.concat(
                            HexFormat.of().parseHex("302a300506032b6570032100"), hex(vectors, "issuer.public")))),
                    factory.generatePrivate(new PKCS8EncodedKeySpec(Bytes.concat(
                            HexFormat.of().parseHex("302e020100300506032b657004220420"),
                            hex(vectors, "issuer.seed")))));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static KeyPair newKeys() {
        try {
            return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void checkMaximum(String name, byte[] raw, int limit) {
        assertEquals(Integer.parseInt(vectors.getProperty("maximum." + name + ".length")), raw.length);
        assertTrue(raw.length <= limit);
        assertArrayEquals(hex(vectors, "maximum." + name + ".sha256"), Bytes.sha256(raw));
    }

    private static byte[] rewrite(byte[] raw, Map<Integer, byte[]> bytes, Map<Integer, Long> ints) {
        return CanonicalProtobuf.message(out -> {
            for (var field : QueryCodecSupport.read(raw, "test")) {
                if (bytes.containsKey(field.number())) {
                    CanonicalProtobuf.bytes(out, field.number(), bytes.get(field.number()));
                } else if (ints.containsKey(field.number())) {
                    CanonicalProtobuf.uint64Bits(out, field.number(), ints.get(field.number()));
                } else if (field.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, field.number(), field.rawValue());
                } else {
                    CanonicalProtobuf.uint64Bits(out, field.number(), field.unsignedValue());
                }
            }
        });
    }

    private static byte[] omit(byte[] raw, int number) {
        return CanonicalProtobuf.message(out -> {
            for (var f : QueryCodecSupport.read(raw, "test")) {
                if (f.number() == number) {
                    continue;
                }
                if (f.wireType() == 2) {
                    CanonicalProtobuf.bytes(out, f.number(), f.rawValue());
                } else {
                    CanonicalProtobuf.uint64Bits(out, f.number(), f.unsignedValue());
                }
            }
        });
    }

    private static byte[] rehash(byte[] raw, int digestField, String domain) {
        final var content = omit(raw, digestField);
        return rewrite(raw, Map.of(digestField, Bytes.sha256(Bytes.utf8(domain + "\0"), content)), Map.of());
    }

    private static byte[] repeat(int size, int b) {
        byte[] out = new byte[size];
        Arrays.fill(out, (byte) b);
        return out;
    }

    private static byte[] hex(Properties p, String key) {
        return HexFormat.of().parseHex(Objects.requireNonNull(p.getProperty(key), key));
    }

    private static Properties load(String name) {
        try (var in = TargetNativePolicyContractTest.class.getResourceAsStream("/ndip3/" + name)) {
            final var p = new Properties();
            p.load(in);
            return p;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private final class Trust implements TargetNativePolicyTrust {
        private PublisherPermission permission = new PublisherPermission(
                scope,
                new ControlAuthor(actor.actorIdHash(), roles.digest(), actor.tenantResourceScopeHash()),
                9,
                keys.getPublic(),
                119000,
                at(1));
        private final Map<Long, Activation> activations = new HashMap<>();
        private MemberApproval approval;
        private boolean failMember, failActivation;

        void activate(TargetNativePolicySnapshot snapshot, SourcePosition at) {
            activations.put(snapshot.generation(), new Activation(scope, snapshot, at));
        }

        @Override
        public Optional<PublisherPermission> publisher(byte[] digest, int generation, SourcePosition at) {
            return Optional.ofNullable(permission);
        }

        @Override
        public Optional<Activation> activation(byte[] digest, long generation) {
            if (failActivation) {
                throw new IllegalStateException("policy authority unavailable");
            }
            return Optional.ofNullable(activations.get(generation));
        }

        @Override
        public Optional<MemberApproval> member(byte[] grant, byte[] digest, SourcePosition at) {
            if (failMember) {
                throw new IllegalStateException("member approval unavailable");
            }
            return Optional.ofNullable(approval);
        }
    }

    private final class Heads implements TargetNativePolicyAuthority {
        private Publication current;
        private boolean changeAfterRead;
        private int reads;

        @Override
        public Optional<Publication> current(byte[] digest) {
            if (changeAfterRead && ++reads == 2) {
                return Optional.of(new Publication(current.revision() + 1, current.head()));
            }
            return Optional.ofNullable(current);
        }

        @Override
        public Publication compareAndSet(byte[] digest, long expected, TargetNativePolicyHead next) {
            if ((current == null ? 0 : current.revision()) != expected) {
                throw new IllegalStateException("CAS conflict");
            }
            assertEquals(next, TargetNativePolicyHead.next(current == null ? null : current.head(), next.snapshot()));
            current = new Publication(expected + 1, next);
            return current;
        }
    }
}
