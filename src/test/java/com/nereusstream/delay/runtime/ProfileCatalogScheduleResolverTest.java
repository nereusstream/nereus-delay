package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CredentialBindingHead;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TimingCapability;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProfileCatalogScheduleResolverTest {
    @Test
    void decoratorCannotHideAnotherProfileCatalog() {
        final ProfileSemanticEnvelope semantic = semantic(1);
        final ProfileCatalog first = new StubProfileCatalog(semantic, true);
        final ProfileCatalogScheduleResolver decorated =
                new ProfileCatalogScheduleResolver(new RecordingResolver(), first);

        assertThrows(
                IllegalArgumentException.class,
                () -> new ProfileCatalogScheduleResolver(decorated, new StubProfileCatalog(semantic, true)));
    }

    @Test
    void delegatesOnlyAfterExactProfileAndHeadAreResolved() {
        final ProfileSemanticEnvelope semantic = semantic(1);
        final ProfileRef profile = semantic.ref();
        final ProfileCatalog catalog = new StubProfileCatalog(semantic, true);
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogScheduleResolver resolver = new ProfileCatalogScheduleResolver(delegate, catalog);
        final CanonicalScheduleIntent intent = intent(profile);
        final ShardId shard = new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 1);
        final DelayMessageId message = DelayMessageId.random(shard);

        final ScheduleResolver.ResolvedSchedule result = resolver.resolveSchedule(shard, message, intent, null);

        assertEquals(delegate.schedule.laneId(), result.laneId());
        assertArrayEquals(delegate.schedule.canonicalLaneTuple(), result.canonicalLaneTuple());
        assertArrayEquals(delegate.schedule.inlinePayload(), result.inlinePayload());
        assertEquals(10L, result.actionAtEpochMs());
        assertTrue(delegate.scheduleCalled);
    }

    @Test
    void failsClosedBeforeDelegateWhenProfileOrHeadIsMissing() {
        final ProfileSemanticEnvelope semantic = semantic(2);
        final ProfileRef profile = semantic.ref();
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogScheduleResolver resolver =
                new ProfileCatalogScheduleResolver(delegate, new StubProfileCatalog(semantic, false));
        final CommandResolutionException exception = assertThrows(
                CommandResolutionException.class, () -> resolver.resolveSchedule(null, null, intent(profile), null));

        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, exception.stableCode());
        assertFalse(delegate.scheduleCalled);
    }

    @Test
    void appliesTheSameGateToPrepareLargeSchedule() {
        final ProfileSemanticEnvelope semantic = semantic(3);
        final ProfileRef profile = semantic.ref();
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogScheduleResolver resolver =
                new ProfileCatalogScheduleResolver(delegate, new StubProfileCatalog(semantic, false));
        final ShardId shard = new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 3);
        final PrepareLargeScheduleBody body = new PrepareLargeScheduleBody(
                DelayMessageId.random(shard),
                100,
                CanonicalScheduleIntent.forPrepare(
                        profile,
                        new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 4)),
                        10,
                        100,
                        DeliveryMode.MANAGED,
                        OrderingMode.BEST_EFFORT,
                        new byte[0],
                        AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                        null,
                        null),
                10,
                bytes(32, 5),
                100,
                new PayloadProofTrustSetRef(1, bytes(32, 6)),
                new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 7), ProfileKind.OBJECT_STORE));

        final CommandResolutionException exception =
                assertThrows(CommandResolutionException.class, () -> resolver.resolvePrepare(null, null, body, null));
        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, exception.stableCode());
        assertFalse(delegate.prepareCalled);
    }

    @Test
    void failsClosedWhenReferencedDeliveryCapabilityIsMissing() {
        final ProfileSemanticEnvelope semantic = semantic(4);
        final ProfileCatalogScheduleResolver resolver = new ProfileCatalogScheduleResolver(
                new RecordingResolver(), new StubProfileCatalog(semantic, true, false));
        final CommandResolutionException exception = assertThrows(
                CommandResolutionException.class,
                () -> resolver.resolveSchedule(null, null, intent(semantic.ref()), null));

        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, exception.stableCode());
    }

    @Test
    void derivesCertifiedPulsarActionAtFromImmutableProfileAndCapability() {
        final int version = 5;
        final ProfileSemanticEnvelope capability = pulsarCapability(version);
        final ProfileSemanticEnvelope destination = pulsarSemantic(version, capability);
        final ProfileCatalogScheduleResolver resolver = new ProfileCatalogScheduleResolver(
                new RecordingResolver(), new StubProfileCatalog(destination, true, capability));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination.ref(),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 8)),
                2_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null);

        final ScheduleResolver.ResolvedSchedule result = resolver.resolveSchedule(null, null, intent, null);

        assertEquals(1_500L, result.actionAtEpochMs());
    }

    @Test
    void rejectsCertifiedPulsarPrepareUnderflowBeforeDelegateOrReservationProjection() {
        final int version = 6;
        final ProfileSemanticEnvelope capability = pulsarCapability(version);
        final ProfileSemanticEnvelope destination = pulsarSemantic(version, capability);
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogScheduleResolver resolver =
                new ProfileCatalogScheduleResolver(delegate, new StubProfileCatalog(destination, true, capability));
        final ShardId shard = new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 6);
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.forPrepare(
                destination.ref(),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 9)),
                400,
                1_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null);
        final PrepareLargeScheduleBody body = new PrepareLargeScheduleBody(
                DelayMessageId.random(shard),
                2_000,
                intent,
                10,
                bytes(32, 10),
                100,
                new PayloadProofTrustSetRef(1, bytes(32, 11)),
                new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 12), ProfileKind.OBJECT_STORE));

        final CommandResolutionException exception = assertThrows(
                CommandResolutionException.class,
                () -> resolver.resolvePrepare(shard, body.delayMessageId(), body, null));

        assertEquals(StableCode.INVALID_DELIVERY_WINDOW, exception.stableCode());
        assertFalse(delegate.prepareCalled);
    }

    @Test
    void rejectsCommandFieldsOutsideDestinationProfileBeforeDelegate() {
        final ProfileSemanticEnvelope destination = semantic(7);
        final RecordingResolver delegate = new RecordingResolver();
        final ProfileCatalogScheduleResolver resolver =
                new ProfileCatalogScheduleResolver(delegate, new StubProfileCatalog(destination, true));
        final RetryPolicyRef retry = new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 13));

        final CanonicalScheduleIntent wrongAdapter = CanonicalScheduleIntent.create(
                destination.ref(),
                retry,
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null);
        assertEquals(
                StableCode.INVALID_METADATA,
                assertThrows(
                                CommandResolutionException.class,
                                () -> resolver.resolveSchedule(null, null, wrongAdapter, null))
                        .stableCode());

        final CanonicalScheduleIntent disallowedOrdering = CanonicalScheduleIntent.create(
                destination.ref(),
                retry,
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.DELIVERY_TIME_FIFO,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        assertEquals(
                StableCode.ORDERING_CAPABILITY_UNAVAILABLE,
                assertThrows(
                                CommandResolutionException.class,
                                () -> resolver.resolveSchedule(null, null, disallowedOrdering, null))
                        .stableCode());

        final CanonicalScheduleIntent oversizedPayload = CanonicalScheduleIntent.create(
                destination.ref(),
                retry,
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                new byte[513],
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        assertEquals(
                StableCode.PAYLOAD_TOO_LARGE,
                assertThrows(
                                CommandResolutionException.class,
                                () -> resolver.resolveSchedule(null, null, oversizedPayload, null))
                        .stableCode());

        final CanonicalScheduleIntent oversizedMetadata = CanonicalScheduleIntent.create(
                destination.ref(),
                retry,
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(new byte[129], List.of())),
                null,
                null);
        assertEquals(
                StableCode.INVALID_METADATA,
                assertThrows(
                                CommandResolutionException.class,
                                () -> resolver.resolveSchedule(null, null, oversizedMetadata, null))
                        .stableCode());

        final CanonicalScheduleIntent oversizedPrepareIntent = CanonicalScheduleIntent.forPrepare(
                destination.ref(),
                retry,
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final PrepareLargeScheduleBody oversizedPrepare = new PrepareLargeScheduleBody(
                DelayMessageId.random(new ShardId(com.nereusstream.delay.protocol.RouteIncarnation.random(), 7)),
                200,
                oversizedPrepareIntent,
                513,
                bytes(32, 14),
                100,
                new PayloadProofTrustSetRef(1, bytes(32, 15)),
                new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 16), ProfileKind.OBJECT_STORE));
        assertEquals(
                StableCode.PAYLOAD_TOO_LARGE,
                assertThrows(
                                CommandResolutionException.class,
                                () -> resolver.resolvePrepare(
                                        null, oversizedPrepare.delayMessageId(), oversizedPrepare, null))
                        .stableCode());

        assertFalse(delegate.scheduleCalled);
        assertFalse(delegate.prepareCalled);
    }

    private static CanonicalScheduleIntent intent(final ProfileRef profile) {
        return CanonicalScheduleIntent.create(
                profile,
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 3)),
                10,
                100,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                new byte[0],
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static ProfileSemanticEnvelope semantic(final int version) {
        final ProfileSemanticEnvelope capability = capability(version);
        final DestinationProfileSemantic body = new DestinationProfileSemantic(
                AdapterKind.KAFKA,
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                        "cluster", UUID.nameUUIDFromBytes(Bytes.utf8("profile-" + version)))),
                2,
                TargetPartitionPolicy.EXPLICIT_OR_HASH,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(0),
                capability.ref(),
                1,
                0,
                0,
                bytes(32, version),
                1_000,
                128,
                512,
                1,
                Bytes.utf8("destination"),
                0,
                0,
                1,
                bytes(32, version + 1));
        return new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), version, body);
    }

    private static ProfileSemanticEnvelope capability(final int version) {
        final DeliveryCapabilitySemantic body = new DeliveryCapabilitySemantic(
                AdapterKind.KAFKA,
                com.nereusstream.delay.protocol.OutcomeCapability.AT_LEAST_ONCE,
                com.nereusstream.delay.protocol.TimingCapability.ORDINARY_MANAGED,
                null,
                0,
                0,
                0,
                0,
                bytes(32, version + 2),
                bytes(32, version + 3),
                0,
                0);
        return new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("capability"), version, body);
    }

    private static ProfileSemanticEnvelope pulsarCapability(final int version) {
        final DeliveryCapabilitySemantic body = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                com.nereusstream.delay.protocol.OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_GUARDED_HANDOFF,
                null,
                0,
                0,
                0,
                0,
                bytes(32, version + 2),
                bytes(32, version + 3),
                0,
                0);
        return new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("capability"), version, body);
    }

    private static ProfileSemanticEnvelope pulsarSemantic(final int version, final ProfileSemanticEnvelope capability) {
        final DestinationProfileSemantic body = new DestinationProfileSemantic(
                AdapterKind.PULSAR,
                BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                        "cluster", bytes(32, version), "persistent://tenant/ns/topic", 1)),
                1,
                TargetPartitionPolicy.HASH_ONLY,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(),
                capability.ref(),
                1,
                500,
                100,
                bytes(32, version + 4),
                1_000,
                128,
                512,
                1,
                Bytes.utf8("pulsar-destination"),
                0,
                0,
                1,
                bytes(32, version + 5));
        return new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), version, body);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class RecordingResolver implements ScheduleResolver {
        private final ResolvedSchedule schedule = new ResolvedSchedule(
                DestinationLaneId.derive(bytes(32, 50)), bytes(32, 50), Bytes.utf8("payload"), null);
        private boolean scheduleCalled;
        private boolean prepareCalled;

        @Override
        public ResolvedSchedule resolveSchedule(
                final ShardId shardId,
                final DelayMessageId messageId,
                final CanonicalScheduleIntent intent,
                final SourcePosition sourcePosition) {
            scheduleCalled = true;
            return schedule;
        }

        @Override
        public ResolvedPrepare resolvePrepare(
                final ShardId shardId,
                final DelayMessageId messageId,
                final PrepareLargeScheduleBody body,
                final SourcePosition sourcePosition) {
            prepareCalled = true;
            return new ResolvedPrepare(DestinationLaneId.derive(bytes(32, 51)), bytes(32, 51));
        }
    }

    private static final class StubProfileCatalog implements ProfileCatalog {
        private final ProfileSemanticEnvelope semantic;
        private final boolean available;
        private final ProfileSemanticEnvelope capability;
        private final boolean capabilityAvailable;

        private StubProfileCatalog(final ProfileSemanticEnvelope semantic, final boolean available) {
            this(semantic, available, available);
        }

        private StubProfileCatalog(
                final ProfileSemanticEnvelope semantic, final boolean available, final boolean capabilityAvailable) {
            this(semantic, available, capabilityAvailable, capability(Math.toIntExact(semantic.version())));
        }

        private StubProfileCatalog(
                final ProfileSemanticEnvelope semantic,
                final boolean available,
                final ProfileSemanticEnvelope capability) {
            this(semantic, available, true, capability);
        }

        private StubProfileCatalog(
                final ProfileSemanticEnvelope semantic,
                final boolean available,
                final boolean capabilityAvailable,
                final ProfileSemanticEnvelope capability) {
            this.semantic = semantic;
            this.available = available;
            this.capability = capability;
            this.capabilityAvailable = capabilityAvailable;
        }

        @Override
        public ProfileSemanticEnvelope resolve(final ProfileRef reference) {
            if (!available) {
                return null;
            }
            if (semantic.ref().equals(reference)) {
                return semantic;
            }
            return capabilityAvailable && capability.ref().equals(reference) ? capability : null;
        }

        @Override
        public com.nereusstream.delay.protocol.CredentialBinding resolveBinding(
                final ProfileRef profile, final long secretGeneration) {
            return null;
        }

        @Override
        public CredentialBindingHead resolveHead(final ProfileRef reference) {
            return available && semantic.ref().equals(reference)
                    ? CredentialBindingHead.create(reference, 1, bytes(32, 60), 1)
                    : null;
        }

        @Override
        public com.nereusstream.delay.protocol.CredentialBindingProtection resolveProtection(
                final ProfileRef profile, final long secretGeneration) {
            return null;
        }
    }
}
