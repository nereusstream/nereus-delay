package com.nereusstream.delay.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.CredentialBindingProtection;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicyScope;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarSourceLock;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleBinding;
import com.nereusstream.delay.protocol.ScheduleCommandBody;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TimingCapability;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.semantic.HandoffPolicyAuthority;
import com.nereusstream.delay.semantic.InMemoryHandoffPolicyAuthority;
import com.nereusstream.delay.semantic.InMemoryHandoffPolicyTrustStore;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ProfileCatalogManagedNativeEligibilityAuthorityTest {
    @Test
    void admissionFreezesTheExactCasStableCurrentHead() throws Exception {
        final Fixture fixture = Fixture.create();

        assertEquals(
                fixture.snapshot,
                fixture.gate.freezeCurrent(fixture.materialization, fixture.binding, exactTime(1_950)));
        assertThrows(
                IllegalArgumentException.class,
                () -> fixture.gate.freezeCurrent(
                        fixture.materialization,
                        fixture.binding,
                        interval(1_950, fixture.snapshot.validUntilEpochMs())));
    }

    @Test
    void admissionRejectsAHeadThatChangesBetweenItsTwoReads() throws Exception {
        final Fixture fixture = Fixture.create();
        final HandoffPolicySnapshot replacement = HandoffPolicySnapshot.create(
                fixture.snapshot.policyScopeDigest(),
                2,
                HandoffPolicyMode.DISABLED,
                0,
                1_000,
                4_000,
                0,
                exactTime(900),
                1,
                fixture.artifacts.setDigest(),
                fixture.keys.getPrivate());
        final HandoffPolicyHead replacementHead = new HandoffPolicyHead(
                replacement.policyScopeDigest(), replacement.generation(), replacement.mode(), replacement, 4_000);
        final HandoffPolicyAuthority.Publication changed = new HandoffPolicyAuthority.Publication(2, replacementHead);
        final AtomicInteger reads = new AtomicInteger();
        final HandoffPolicyAuthority drifting = new HandoffPolicyAuthority() {
            @Override
            public java.util.Optional<Publication> current(final byte[] ignored) {
                return java.util.Optional.of(reads.getAndIncrement() == 0 ? fixture.publication : changed);
            }

            @Override
            public Publication compareAndSet(
                    final byte[] ignoredScope, final long ignoredVersion, final HandoffPolicyHead ignoredNext) {
                throw new UnsupportedOperationException("read-only test authority");
            }
        };
        final ProfileCatalogManagedNativeEligibilityAuthority gate =
                new ProfileCatalogManagedNativeEligibilityAuthority(
                        fixture.catalog, drifting, fixture.trust, fixture.artifacts, () -> fixture.trustPosition);

        assertThrows(
                IllegalStateException.class,
                () -> gate.freezeCurrent(fixture.materialization, fixture.binding, exactTime(1_950)));
    }

    private record Fixture(
            ProfileCatalog catalog,
            ArtifactGenerationSet artifacts,
            InMemoryHandoffPolicyTrustStore trust,
            SourcePosition trustPosition,
            KeyPair keys,
            HandoffPolicySnapshot snapshot,
            HandoffPolicyAuthority.Publication publication,
            ScheduleBinding binding,
            ClaimMaterialization materialization,
            ProfileCatalogManagedNativeEligibilityAuthority gate) {
        private static Fixture create() throws Exception {
            final ArtifactGenerationSet artifacts = ArtifactGenerationSet.current(
                    1, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("admission-schema")));
            final BrokerResourceIdentity target = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                    "admission-cluster",
                    Bytes.sha256(Bytes.utf8("admission-resource")),
                    "persistent://tenant/ns/admission-topic-partition-0",
                    17));
            final DeliveryCapabilitySemantic capabilityBody = new DeliveryCapabilitySemantic(
                    AdapterKind.PULSAR,
                    OutcomeCapability.AT_LEAST_ONCE,
                    TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_NATIVE_MANAGED_HANDOFF,
                    null,
                    0,
                    0,
                    0,
                    0,
                    Bytes.sha256(Bytes.utf8("admission-prerequisite")),
                    PulsarSourceLock.digest(),
                    0,
                    0);
            final ProfileSemanticEnvelope capability = new ProfileSemanticEnvelope(
                    ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("admission-capability"), 1, capabilityBody);
            final DestinationProfileSemantic destinationBody = new DestinationProfileSemantic(
                    AdapterKind.PULSAR,
                    target,
                    1,
                    TargetPartitionPolicy.EXPLICIT_ONLY,
                    TargetPartitionHashInput.ORDERING_KEY,
                    List.of(0),
                    capability.ref(),
                    0x02,
                    500,
                    Bytes.sha256(Bytes.utf8("admission-credential")),
                    4_096,
                    1_024,
                    2_048,
                    1,
                    Bytes.utf8("admission-destination"),
                    0,
                    0,
                    2,
                    Bytes.sha256(Bytes.utf8("admission-policy")));
            final ProfileSemanticEnvelope destination = new ProfileSemanticEnvelope(
                    ProfileKind.DESTINATION, Bytes.utf8("admission-destination"), 1, destinationBody);
            final ProfileCatalog catalog =
                    ProfileCatalogManagedNativeEligibilityAuthorityTest.catalog(destination, capability);
            final byte[] tenantScope = Bytes.sha256(Bytes.utf8("admission-tenant"));
            final byte[] tuple = pulsarTuple(tenantScope, target, destination.ref(), capability.ref());
            final DestinationLaneId lane = DestinationLaneId.derive(tuple);
            final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
            final DelayMessageId message = DelayMessageId.random(shard);
            final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                    destination.ref(),
                    new RetryPolicyRef(Bytes.utf8("admission-retry"), 1, Bytes.sha256(Bytes.utf8("retry"))),
                    2_000,
                    5_000,
                    DeliveryMode.MANAGED,
                    OrderingMode.BEST_EFFORT,
                    NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                    Bytes.utf8("ordering"),
                    Bytes.utf8("payload"),
                    null,
                    AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                    null,
                    777L);
            final ScheduleBinding binding = new ScheduleBinding(
                    CommandType.SCHEDULE,
                    message,
                    lane,
                    tuple,
                    new ScheduleCommandBody(message, 9_000, intent).canonicalBytes());
            final byte[] scope = HandoffPolicyScope.digest(
                    tenantScope,
                    destination.ref(),
                    capability.ref(),
                    target,
                    0,
                    OrderingMode.BEST_EFFORT,
                    HandoffPath.MANAGED_HANDOFF,
                    artifacts);
            final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final HandoffPolicySnapshot snapshot = HandoffPolicySnapshot.create(
                    scope,
                    1,
                    HandoffPolicyMode.ENABLED,
                    100,
                    1_000,
                    3_000,
                    HandoffPath.MANAGED_HANDOFF,
                    exactTime(900),
                    1,
                    artifacts.setDigest(),
                    keys.getPrivate());
            final InMemoryHandoffPolicyAuthority policies = new InMemoryHandoffPolicyAuthority();
            final HandoffPolicyAuthority.Publication publication = policies.compareAndSet(
                    scope, 0, new HandoffPolicyHead(scope, 1, HandoffPolicyMode.ENABLED, snapshot, 0));
            final ClaimMaterialization materialization = new ClaimMaterialization(
                    destination.ref(),
                    capability.ref(),
                    target,
                    0,
                    message,
                    0,
                    PayloadForPublish.inline(Bytes.utf8("payload")),
                    intent.adapterMetadata(),
                    2_000,
                    5_000,
                    1_900,
                    NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                    777L,
                    publication.head().ref(publication.oxiaVersion()));
            final KafkaSourcePosition active = position(shard, 1, 1_100);
            final KafkaSourcePosition trustPosition = position(shard, 2, 1_200);
            final InMemoryHandoffPolicyTrustStore trust = new InMemoryHandoffPolicyTrustStore();
            trust.installIssuerKey(1, keys.getPublic(), active);
            trust.activatePolicy(scope, 1, active);
            final ProfileCatalogManagedNativeEligibilityAuthority gate =
                    new ProfileCatalogManagedNativeEligibilityAuthority(
                            catalog, policies, trust, artifacts, () -> trustPosition);
            return new Fixture(
                    catalog,
                    artifacts,
                    trust,
                    trustPosition,
                    keys,
                    snapshot,
                    publication,
                    binding,
                    materialization,
                    gate);
        }
    }

    private static ProfileCatalog catalog(
            final ProfileSemanticEnvelope destination, final ProfileSemanticEnvelope capability) {
        return new ProfileCatalog() {
            @Override
            public ProfileSemanticEnvelope resolve(final ProfileRef reference) {
                if (destination.ref().equals(reference)) {
                    return destination;
                }
                return capability.ref().equals(reference) ? capability : null;
            }

            @Override
            public CredentialBinding resolveBinding(final ProfileRef profile, final long secretGeneration) {
                return null;
            }

            @Override
            public CredentialBindingHead resolveHead(final ProfileRef profile) {
                return null;
            }

            @Override
            public CredentialBindingProtection resolveProtection(
                    final ProfileRef profile, final long secretGeneration) {
                return null;
            }
        };
    }

    private static byte[] pulsarTuple(
            final byte[] tenantScope,
            final BrokerResourceIdentity target,
            final ProfileRef destination,
            final ProfileRef capability) {
        final PulsarBrokerResourceIdentity pulsar = target.pulsar();
        return Bytes.concat(
                tenantScope,
                Bytes.u8(AdapterKind.PULSAR.wireValue()),
                Bytes.lp32(Bytes.utf8(pulsar.authenticatedClusterId())),
                Bytes.u8(2),
                pulsar.resourceIncarnation(),
                Bytes.u64be(pulsar.physicalTopicCreationTimestamp()),
                Bytes.lp32(Bytes.utf8(pulsar.physicalTopic())),
                Bytes.u32be(0),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(2),
                Bytes.u32be(0));
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(
                shard,
                "admission-source",
                UUID.nameUUIDFromBytes(Bytes.utf8("admission-source-topic")),
                offset,
                1,
                timestamp);
    }

    private static TrustedUtcIntervalEvidence exactTime(final long epochMs) {
        return interval(epochMs, epochMs);
    }

    private static TrustedUtcIntervalEvidence interval(final long earliest, final long latest) {
        return new TrustedUtcIntervalEvidence(
                earliest,
                latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("admission-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("admission-clock-attestation")),
                0,
                null);
    }
}
