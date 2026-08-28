package com.nereusstream.delay.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.semantic.HandoffPolicyAuthority;
import com.nereusstream.delay.semantic.InMemoryHandoffPolicyAuthority;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class HandoffEligibilityResolverTest {
    @Test
    void enabledPolicyProducesNativeCandidateWithTheFrozenOxiaHead() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final HandoffPolicySnapshot snapshot = snapshot(keys, HandoffPolicyMode.ENABLED);
        final HandoffPolicyHead head = new HandoffPolicyHead(
                snapshot.policyScopeDigest(), snapshot.generation(), snapshot.mode(), snapshot, 0);
        final InMemoryHandoffPolicyAuthority authority = new InMemoryHandoffPolicyAuthority();
        final HandoffPolicyAuthority.Publication publication =
                authority.compareAndSet(snapshot.policyScopeDigest(), 0, head);

        final HandoffEligibilityResolver.Decision decision = HandoffEligibilityResolver.resolve(
                input(
                        snapshot,
                        new TrustedUtcIntervalEvidence(
                                1_950,
                                1_950,
                                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                                Bytes.utf8("resolver-clock"),
                                1,
                                2,
                                3,
                                bytes(32, 51),
                                0,
                                null)),
                publication);

        assertEquals(HandoffEligibilityAction.MANAGED_NATIVE_CANDIDATE, decision.action());
        assertEquals(1_900, decision.candidateAtEpochMs());
        assertEquals(1, decision.policyHeadRef().oxiaVersion());
        assertEquals(snapshot, decision.policySnapshot());
    }

    @Test
    void shadowAndFifoNeverBecomeNativeCandidates() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final HandoffPolicySnapshot shadow = snapshot(keys, HandoffPolicyMode.SHADOW);
        final HandoffEligibilityResolver.Decision shadowDecision =
                HandoffEligibilityResolver.resolve(input(shadow, exactTime(2_000), OrderingMode.BEST_EFFORT));
        assertFalse(shadowDecision.isNativeCandidate());
        assertEquals(HandoffEligibilityReason.POLICY_SHADOW, shadowDecision.reason());
        assertEquals(HandoffEligibilityAction.ORDINARY_DUE, shadowDecision.action());

        final HandoffEligibilityResolver.Decision fifoDecision = HandoffEligibilityResolver.resolve(
                input(snapshot(keys, HandoffPolicyMode.ENABLED), exactTime(1_100), OrderingMode.DELIVERY_TIME_FIFO));
        assertFalse(fifoDecision.isNativeCandidate());
        assertEquals(HandoffEligibilityReason.ORDERING_UNAVAILABLE, fifoDecision.reason());
    }

    @Test
    void crossingCandidateBoundaryRequiresAFreshSampleAndDoesNotSchedulePastWork() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final HandoffEligibilityResolver.Decision decision = HandoffEligibilityResolver.resolve(input(
                snapshot(keys, HandoffPolicyMode.ENABLED),
                new TrustedUtcIntervalEvidence(
                        1_850,
                        1_950,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Bytes.utf8("resolver-crossing-clock"),
                        1,
                        3,
                        4,
                        bytes(32, 61),
                        0,
                        null)));

        assertEquals(HandoffEligibilityAction.TIME_SAMPLE_REQUIRED, decision.action());
        assertEquals(1_900, decision.effectiveEligibleAtEpochMs());
        assertEquals(1_951, decision.persistentWakeAtEpochMs());
    }

    @Test
    void crossingPolicyLeaseBoundaryRequiresAFreshSample() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final HandoffPolicySnapshot snapshot = snapshot(keys, HandoffPolicyMode.ENABLED);

        final HandoffEligibilityResolver.Decision validFrom = HandoffEligibilityResolver.resolve(input(
                snapshot,
                new TrustedUtcIntervalEvidence(
                        999,
                        1_001,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Bytes.utf8("resolver-valid-from-clock"),
                        1,
                        3,
                        4,
                        bytes(32, 62),
                        0,
                        null)));
        final HandoffEligibilityResolver.Decision validUntil = HandoffEligibilityResolver.resolve(input(
                snapshot,
                new TrustedUtcIntervalEvidence(
                        2_999,
                        3_001,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Bytes.utf8("resolver-valid-until-clock"),
                        1,
                        3,
                        4,
                        bytes(32, 63),
                        0,
                        null)));

        assertEquals(HandoffEligibilityAction.TIME_SAMPLE_REQUIRED, validFrom.action());
        assertEquals(1_002, validFrom.persistentWakeAtEpochMs());
        assertEquals(HandoffEligibilityAction.TIME_SAMPLE_REQUIRED, validUntil.action());
        assertEquals(3_002, validUntil.persistentWakeAtEpochMs());
    }

    @Test
    void nativePolicyIsRejectedForKafkaAndInvalidProfileCombinations() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final HandoffEligibilityResolver.Input base =
                input(snapshot(keys, HandoffPolicyMode.ENABLED), exactTime(1_100));
        assertEquals(
                HandoffEligibilityReason.WRONG_ADAPTER,
                HandoffEligibilityResolver.resolve(new HandoffEligibilityResolver.Input(
                                AdapterKind.KAFKA,
                                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                                OrderingMode.BEST_EFFORT,
                                true,
                                true,
                                base.destinationProfile(),
                                base.capabilityProfile(),
                                base.policySnapshot(),
                                base.deliverAtEpochMs(),
                                base.retryEligibilityAtEpochMs(),
                                base.trustedTime()))
                        .reason());
        assertThrows(
                IllegalArgumentException.class,
                () -> new HandoffEligibilityResolver.Input(
                        AdapterKind.PULSAR,
                        NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                        OrderingMode.BEST_EFFORT,
                        true,
                        true,
                        base.destinationProfile(),
                        base.capabilityProfile(),
                        base.policySnapshot(),
                        -1,
                        0,
                        base.trustedTime()));
    }

    private static HandoffEligibilityResolver.Input input(
            final HandoffPolicySnapshot snapshot, final TrustedUtcIntervalEvidence time) {
        return input(snapshot, time, OrderingMode.BEST_EFFORT);
    }

    private static HandoffEligibilityResolver.Input input(
            final HandoffPolicySnapshot snapshot, final TrustedUtcIntervalEvidence time, final OrderingMode ordering) {
        final DeliveryCapabilitySemantic capability = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 70),
                bytes(32, 71),
                0,
                0);
        final ProfileRef capabilityRef =
                new ProfileRef(Bytes.utf8("resolver-capability"), 1, bytes(32, 72), ProfileKind.DELIVERY_CAPABILITY);
        final DestinationProfileSemantic destination = new DestinationProfileSemantic(
                AdapterKind.PULSAR,
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        "resolver-cluster", bytes(32, 73), "persistent://public/default/resolver", 74)),
                1,
                TargetPartitionPolicy.EXPLICIT_ONLY,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(0),
                capabilityRef,
                0x01,
                500,
                bytes(32, 75),
                4_096,
                1_024,
                2_048,
                1,
                Bytes.utf8("resolver-destination"),
                0,
                0,
                1,
                bytes(32, 76));
        return new HandoffEligibilityResolver.Input(
                AdapterKind.PULSAR,
                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                ordering,
                true,
                true,
                destination,
                capability,
                snapshot,
                2_000,
                0,
                time);
    }

    private static HandoffPolicySnapshot snapshot(final KeyPair keys, final HandoffPolicyMode mode) {
        final boolean enabled = mode == HandoffPolicyMode.ENABLED;
        return HandoffPolicySnapshot.create(
                bytes(32, 80),
                1,
                mode,
                enabled ? 100 : 0,
                1_000,
                3_000,
                enabled ? HandoffPath.MANAGED_HANDOFF : HandoffPath.MANAGED_HANDOFF,
                new TrustedUtcIntervalEvidence(
                        900,
                        910,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Bytes.utf8("policy-issuer-clock"),
                        1,
                        1,
                        1,
                        bytes(32, 81),
                        0,
                        null),
                1,
                bytes(32, 82),
                keys.getPrivate());
    }

    private static TrustedUtcIntervalEvidence exactTime(final long epochMs) {
        return new TrustedUtcIntervalEvidence(
                epochMs,
                epochMs,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("resolver-exact-clock"),
                1,
                1,
                1,
                bytes(32, 90),
                0,
                null);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
