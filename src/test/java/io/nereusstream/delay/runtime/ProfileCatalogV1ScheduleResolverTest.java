package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CredentialBindingHeadV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarMetadataV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.TargetPartitionHashInputV1;
import io.nereusstream.delay.protocol.TargetPartitionPolicyV1;
import io.nereusstream.delay.protocol.TimingCapabilityV1;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileCatalogV1ScheduleResolverTest {
    @Test
    void delegatesOnlyAfterExactProfileAndHeadAreResolved() {
        final ProfileSemanticEnvelopeV1 semantic = semantic(1);
        final ProfileRefV1 profile = semantic.ref();
        final ProfileCatalog catalog = new StubProfileCatalog(semantic, true);
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogV1ScheduleResolver resolver = new ProfileCatalogV1ScheduleResolver(delegate, catalog);
        final ScheduleIntentV1 intent = intent(profile);
        final ShardId shard = new ShardId(io.nereusstream.delay.protocol.RouteIncarnation.random(), 1);
        final DelayMessageId message = DelayMessageId.random(shard);

        final V1ScheduleResolver.ResolvedSchedule result = resolver.resolveSchedule(shard, message, intent,
                null);

        assertEquals(delegate.schedule.laneId(), result.laneId());
        assertArrayEquals(delegate.schedule.canonicalLaneTuple(), result.canonicalLaneTuple());
        assertArrayEquals(delegate.schedule.inlinePayload(), result.inlinePayload());
        assertEquals(10L, result.actionAtEpochMs());
        assertTrue(delegate.scheduleCalled);
    }

    @Test
    void failsClosedBeforeDelegateWhenProfileOrHeadIsMissing() {
        final ProfileSemanticEnvelopeV1 semantic = semantic(2);
        final ProfileRefV1 profile = semantic.ref();
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogV1ScheduleResolver resolver = new ProfileCatalogV1ScheduleResolver(delegate,
                new StubProfileCatalog(semantic, false));
        final V1CommandResolutionException exception = assertThrows(V1CommandResolutionException.class,
                () -> resolver.resolveSchedule(null, null, intent(profile), null));

        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, exception.stableCode());
        assertFalse(delegate.scheduleCalled);
    }

    @Test
    void appliesTheSameGateToPrepareLargeSchedule() {
        final ProfileSemanticEnvelopeV1 semantic = semantic(3);
        final ProfileRefV1 profile = semantic.ref();
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogV1ScheduleResolver resolver = new ProfileCatalogV1ScheduleResolver(delegate,
                new StubProfileCatalog(semantic, false));
        final ShardId shard = new ShardId(io.nereusstream.delay.protocol.RouteIncarnation.random(), 3);
        final PrepareLargeScheduleBodyV1 body = new PrepareLargeScheduleBodyV1(DelayMessageId.random(shard),
                100, ScheduleIntentV1.forPrepare(profile, new RetryPolicyRefV1(Bytes.utf8("retry"), 1,
                bytes(32, 4)), 10, 100, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0],
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null), 10, bytes(32, 5),
                100, new PayloadProofTrustSetRefV1(1, bytes(32, 6)));

        final V1CommandResolutionException exception = assertThrows(V1CommandResolutionException.class,
                () -> resolver.resolvePrepare(null, null, body, null));
        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, exception.stableCode());
        assertFalse(delegate.prepareCalled);
    }

    @Test
    void failsClosedWhenReferencedDeliveryCapabilityIsMissing() {
        final ProfileSemanticEnvelopeV1 semantic = semantic(4);
        final ProfileCatalogV1ScheduleResolver resolver = new ProfileCatalogV1ScheduleResolver(
                new RecordingResolver(), new StubProfileCatalog(semantic, true, false));
        final V1CommandResolutionException exception = assertThrows(V1CommandResolutionException.class,
                () -> resolver.resolveSchedule(null, null, intent(semantic.ref()), null));

        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, exception.stableCode());
    }

    @Test
    void derivesCertifiedPulsarActionAtFromImmutableProfileAndCapability() {
        final int version = 5;
        final ProfileSemanticEnvelopeV1 capability = pulsarCapability(version);
        final ProfileSemanticEnvelopeV1 destination = pulsarSemantic(version, capability);
        final ProfileCatalogV1ScheduleResolver resolver = new ProfileCatalogV1ScheduleResolver(
                new RecordingResolver(), new StubProfileCatalog(destination, true, capability));
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination.ref(),
                new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 8)), 2_000, 5_000,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("payload"), null,
                AdapterMetadataV1.pulsar(new PulsarMetadataV1(null, null, null, List.of())), null, null);

        final V1ScheduleResolver.ResolvedSchedule result = resolver.resolveSchedule(null, null, intent, null);

        assertEquals(1_500L, result.actionAtEpochMs());
    }

    private static ScheduleIntentV1 intent(final ProfileRefV1 profile) {
        return ScheduleIntentV1.create(profile, new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 3)),
                10, 100, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8("payload"),
                null, AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
    }

    private static ProfileSemanticEnvelopeV1 semantic(final int version) {
        final ProfileSemanticEnvelopeV1 capability = capability(version);
        final DestinationProfileSemanticV1 body = new DestinationProfileSemanticV1(
                AdapterKindV1.KAFKA,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster",
                        UUID.nameUUIDFromBytes(Bytes.utf8("profile-" + version)))),
                2, TargetPartitionPolicyV1.EXPLICIT_OR_HASH, TargetPartitionHashInputV1.ORDERING_KEY,
                List.of(0), capability.ref(), 1, 0, 0, bytes(32, version), 1_000, 128, 512, 1,
                Bytes.utf8("destination"), 0, 0, 1, bytes(32, version + 1));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION, Bytes.utf8("destination"), version, body);
    }

    private static ProfileSemanticEnvelopeV1 capability(final int version) {
        final DeliveryCapabilitySemanticV1 body = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.KAFKA, io.nereusstream.delay.protocol.OutcomeCapabilityV1.AT_LEAST_ONCE,
                io.nereusstream.delay.protocol.TimingCapabilityV1.ORDINARY_MANAGED,
                null, 0, 0, 0, 0, bytes(32, version + 2), bytes(32, version + 3), 0, 0);
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("capability"), version,
                body);
    }

    private static ProfileSemanticEnvelopeV1 pulsarCapability(final int version) {
        final DeliveryCapabilitySemanticV1 body = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.PULSAR, io.nereusstream.delay.protocol.OutcomeCapabilityV1.AT_LEAST_ONCE,
                TimingCapabilityV1.ORDINARY_MANAGED | TimingCapabilityV1.PULSAR_GUARDED_HANDOFF,
                null, 0, 0, 0, 0, bytes(32, version + 2), bytes(32, version + 3), 0, 0);
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("capability"), version,
                body);
    }

    private static ProfileSemanticEnvelopeV1 pulsarSemantic(final int version,
                                                              final ProfileSemanticEnvelopeV1 capability) {
        final DestinationProfileSemanticV1 body = new DestinationProfileSemanticV1(
                AdapterKindV1.PULSAR,
                BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1("cluster", bytes(32, version),
                        "persistent://tenant/ns/topic", 1)),
                1, TargetPartitionPolicyV1.HASH_ONLY, TargetPartitionHashInputV1.ORDERING_KEY, List.of(),
                capability.ref(), 1, 500, 100, bytes(32, version + 4), 1_000, 128, 512, 1,
                Bytes.utf8("pulsar-destination"), 0, 0, 1, bytes(32, version + 5));
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION, Bytes.utf8("destination"), version, body);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class RecordingResolver implements V1ScheduleResolver {
        private final ResolvedSchedule schedule = new ResolvedSchedule(DestinationLaneId.derive(bytes(32, 50)),
                bytes(32, 50), Bytes.utf8("payload"), null);
        private boolean scheduleCalled;
        private boolean prepareCalled;

        @Override
        public ResolvedSchedule resolveSchedule(final ShardId shardId, final DelayMessageId messageId,
                                                final ScheduleIntentV1 intent, final SourcePosition sourcePosition) {
            scheduleCalled = true;
            return schedule;
        }

        @Override
        public ResolvedPrepare resolvePrepare(final ShardId shardId, final DelayMessageId messageId,
                                              final PrepareLargeScheduleBodyV1 body,
                                              final SourcePosition sourcePosition) {
            prepareCalled = true;
            return new ResolvedPrepare(DestinationLaneId.derive(bytes(32, 51)), bytes(32, 51));
        }
    }

    private static final class StubProfileCatalog implements ProfileCatalog {
        private final ProfileSemanticEnvelopeV1 semantic;
        private final boolean available;
        private final ProfileSemanticEnvelopeV1 capability;
        private final boolean capabilityAvailable;

        private StubProfileCatalog(final ProfileSemanticEnvelopeV1 semantic, final boolean available) {
            this(semantic, available, available);
        }

        private StubProfileCatalog(final ProfileSemanticEnvelopeV1 semantic, final boolean available,
                                    final boolean capabilityAvailable) {
            this(semantic, available, capabilityAvailable, capability(Math.toIntExact(semantic.version())));
        }

        private StubProfileCatalog(final ProfileSemanticEnvelopeV1 semantic, final boolean available,
                                    final ProfileSemanticEnvelopeV1 capability) {
            this(semantic, available, true, capability);
        }

        private StubProfileCatalog(final ProfileSemanticEnvelopeV1 semantic, final boolean available,
                                    final boolean capabilityAvailable,
                                    final ProfileSemanticEnvelopeV1 capability) {
            this.semantic = semantic;
            this.available = available;
            this.capability = capability;
            this.capabilityAvailable = capabilityAvailable;
        }

        @Override
        public ProfileSemanticEnvelopeV1 resolve(final ProfileRefV1 reference) {
            if (!available) {
                return null;
            }
            if (semantic.ref().equals(reference)) {
                return semantic;
            }
            return capabilityAvailable && capability.ref().equals(reference) ? capability : null;
        }

        @Override
        public io.nereusstream.delay.protocol.CredentialBindingV1 resolveBinding(final ProfileRefV1 profile,
                                                                                  final long secretGeneration) {
            return null;
        }

        @Override
        public CredentialBindingHeadV1 resolveHead(final ProfileRefV1 reference) {
            return available && semantic.ref().equals(reference)
                    ? CredentialBindingHeadV1.create(reference, 1, bytes(32, 60), 1) : null;
        }

        @Override
        public io.nereusstream.delay.protocol.CredentialBindingProtectionV1 resolveProtection(
                final ProfileRefV1 profile, final long secretGeneration) {
            return null;
        }
    }
}
