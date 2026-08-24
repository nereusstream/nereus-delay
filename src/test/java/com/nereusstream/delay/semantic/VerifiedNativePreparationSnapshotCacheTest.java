package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.ActivationBarrierV1;
import com.nereusstream.delay.protocol.AdapterKindV1;
import com.nereusstream.delay.protocol.AdapterMetadataV1;
import com.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshotV1;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OutcomeCapabilityV1;
import com.nereusstream.delay.protocol.ProfileKindV1;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import com.nereusstream.delay.protocol.ProtocolTupleV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import com.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import com.nereusstream.delay.protocol.PulsarMetadataV1;
import com.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.RetryPolicyRefV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycleV1;
import com.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import com.nereusstream.delay.protocol.RouteSnapshotV1;
import com.nereusstream.delay.protocol.RoutingHashVersionV1;
import com.nereusstream.delay.protocol.ScheduleIntentV1;
import com.nereusstream.delay.protocol.SubmissionModeV1;
import com.nereusstream.delay.protocol.TargetPartitionHashInputV1;
import com.nereusstream.delay.protocol.TargetPartitionHashV1;
import com.nereusstream.delay.protocol.TargetPartitionPolicyV1;
import com.nereusstream.delay.protocol.TimingCapabilityV1;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class VerifiedNativePreparationSnapshotCacheTest {
    @Test
    void cacheVerifiesIssuerAndCoreFreezesTheNativeBranch() throws Exception {
        final Fixture fixture = fixture();
        final VerifiedNativePreparationSnapshotCache cache =
                new VerifiedNativePreparationSnapshotCache(fixture.keys().getPublic());
        cache.install(fixture.candidate());

        final Optional<NativePreparationSnapshotV1> selected = cache.eligibleFor(
                fixture.tenant(), fixture.route(), fixture.intent(), fixture.command(), new TrustedTimeSnapshot(300));
        assertTrue(selected.isPresent());
        assertArrayEquals(
                fixture.candidate().capabilitySnapshot().snapshotDigest(),
                selected.orElseThrow().capabilitySnapshot().snapshotDigest());

        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(
                new SingleRouteProvider(fixture.route()),
                new SequenceUuids(uuidV7(300, 1), uuidV7(300, 2)),
                () -> 300,
                cache,
                (command, intent) -> bytes(32, 90));
        final var prepared = core.prepareSchedule(
                fixture.tenant(), fixture.hint(), fixture.intent(), 700, SubmissionModeV1.AUTO_FAST);
        assertFalse(prepared.isManaged());
        assertEquals(fixture.candidate().target(), prepared.nativePrepared().target());
        assertEquals(
                fixture.candidate().physicalPartition(),
                prepared.nativePrepared().physicalPartition());
    }

    @Test
    void cacheRejectsWrongTargetPartitionAndFallsBackToTheExactManagedFrame() throws Exception {
        final Fixture fixture = fixture();
        final int wrongPartition = fixture.candidate().physicalPartition() == 0 ? 1 : 0;
        final NativeCapabilitySnapshotV1 wrongSnapshot = NativeCapabilitySnapshotV1.create(
                fixture.candidate().destination().ref(),
                fixture.candidate().capability().ref(),
                fixture.target(),
                wrongPartition,
                bytes(32, 70),
                1,
                1,
                bytes(32, 71),
                bytes(32, 72),
                fixture.tenant().principalScopeHash(),
                fixture.nativeIssuedAt(),
                900,
                1,
                fixture.keys().getPrivate());
        final NativePreparationSnapshotV1 wrongCandidate = new NativePreparationSnapshotV1(
                fixture.candidate().destination(),
                fixture.candidate().capability(),
                fixture.target(),
                wrongPartition,
                wrongSnapshot,
                420);
        final VerifiedNativePreparationSnapshotCache cache =
                new VerifiedNativePreparationSnapshotCache(fixture.keys().getPublic());
        cache.install(wrongCandidate);

        final DefaultDelaySemanticCore managedCore = new DefaultDelaySemanticCore(
                new SingleRouteProvider(fixture.route()),
                new SequenceUuids(uuidV7(300, 1), uuidV7(300, 2)),
                () -> 300,
                cache,
                (command, intent) -> bytes(32, 91));
        final var managed = managedCore.prepareSchedule(
                fixture.tenant(), fixture.hint(), fixture.intent(), 700, SubmissionModeV1.MANAGED);
        final DefaultDelaySemanticCore autoCore = new DefaultDelaySemanticCore(
                new SingleRouteProvider(fixture.route()),
                new SequenceUuids(uuidV7(300, 1), uuidV7(300, 2)),
                () -> 300,
                cache,
                (command, intent) -> bytes(32, 91));
        final var observed = autoCore.prepareSchedule(
                fixture.tenant(), fixture.hint(), fixture.intent(), 700, SubmissionModeV1.AUTO_FAST);
        assertTrue(observed.isManaged());
        assertArrayEquals(managed.managedFrame(), observed.managedFrame());
    }

    @Test
    void installRejectsSnapshotWithInvalidIssuerSignature() throws Exception {
        final Fixture fixture = fixture();
        final byte[] encoded = fixture.candidate().capabilitySnapshot().canonicalBytes();
        encoded[encoded.length - 1] ^= 0x01;
        final NativeCapabilitySnapshotV1 tampered = NativeCapabilitySnapshotV1.decode(encoded);
        final NativePreparationSnapshotV1 candidate = new NativePreparationSnapshotV1(
                fixture.candidate().destination(),
                fixture.candidate().capability(),
                fixture.target(),
                fixture.candidate().physicalPartition(),
                tampered,
                500);
        final VerifiedNativePreparationSnapshotCache cache =
                new VerifiedNativePreparationSnapshotCache(fixture.keys().getPublic());
        assertThrows(IllegalArgumentException.class, () -> cache.install(candidate));
    }

    private static Fixture fixture() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final AuthenticatedTenantContext tenant =
                new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
        final PulsarBrokerResourceIdentityV1 target = new PulsarBrokerResourceIdentityV1(
                "target-cluster", bytes(32, 10), "persistent://tenant/ns/destination", 20);
        final ProfileSemanticEnvelopeV1 capability = capability();
        final DestinationProfileSemanticV1 destinationBody = new DestinationProfileSemanticV1(
                AdapterKindV1.PULSAR,
                BrokerResourceIdentityV1.pulsar(target),
                2,
                TargetPartitionPolicyV1.HASH_ONLY,
                TargetPartitionHashInputV1.ORDERING_KEY,
                List.of(),
                capability.ref(),
                1,
                0,
                20,
                bytes(32, 40),
                1024,
                512,
                512,
                1,
                Bytes.utf8("destination"),
                0,
                0,
                1,
                bytes(32, 41));
        final ProfileSemanticEnvelopeV1 destination =
                new ProfileSemanticEnvelopeV1(ProfileKindV1.DESTINATION, Bytes.utf8("destination"), 1, destinationBody);
        final PulsarMetadataV1 metadata = new PulsarMetadataV1(null, null, Bytes.utf8("native-ordering"), List.of());
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(
                destination.ref(),
                new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 42)),
                400,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("delay-ordering"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadataV1.pulsar(metadata),
                null,
                null);
        final int physicalPartition =
                (int) TargetPartitionHashV1.partition(destination.ref(), 2, metadata.orderingKey());
        final TrustedUtcIntervalEvidence nativeIssuedAt = nativeIssuedAt();
        final NativeCapabilitySnapshotV1 capabilitySnapshot = NativeCapabilitySnapshotV1.create(
                destination.ref(),
                capability.ref(),
                target,
                physicalPartition,
                bytes(32, 50),
                1,
                1,
                bytes(32, 51),
                bytes(32, 52),
                tenant.principalScopeHash(),
                nativeIssuedAt,
                900,
                1,
                keys.getPrivate());
        final NativePreparationSnapshotV1 candidate = new NativePreparationSnapshotV1(
                destination, capability, target, physicalPartition, capabilitySnapshot, 420);
        final RouteSnapshotV1 route = route(keys);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.PULSAR, Bytes.utf8("primary"));
        final com.nereusstream.delay.protocol.ShardId shard =
                new com.nereusstream.delay.protocol.ShardId(route.routeIncarnation(), 0);
        final var command = com.nereusstream.delay.protocol.PreparedCommand.scheduleV1(shard, intent, 700);
        return new Fixture(keys, tenant, route, hint, target, intent, candidate, command, nativeIssuedAt);
    }

    private static ProfileSemanticEnvelopeV1 capability() {
        final DeliveryCapabilitySemanticV1 body = new DeliveryCapabilitySemanticV1(
                AdapterKindV1.PULSAR,
                OutcomeCapabilityV1.AT_LEAST_ONCE,
                TimingCapabilityV1.ORDINARY_MANAGED | TimingCapabilityV1.PULSAR_AUTO_FAST,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 60),
                bytes(32, 61),
                0,
                0);
        return new ProfileSemanticEnvelopeV1(ProfileKindV1.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 1, body);
    }

    private static RouteSnapshotV1 route(final KeyPair keys) {
        final String base = "persistent://tenant/ns/commands";
        final PulsarPhysicalPartitionIdentityV1 physical =
                new PulsarPhysicalPartitionIdentityV1(0, base + "-partition-0", bytes(32, 80), 81);
        final PulsarIngressRouteResourceV1 ingress =
                new PulsarIngressRouteResourceV1("source-cluster", base, List.of(physical));
        final BrokerResourceIdentityV1 source = BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                "source-cluster",
                physical.resourceIncarnation(),
                physical.physicalTopic(),
                physical.physicalTopicCreationTimestamp()));
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(
                bytes(32, 82),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final RoutePartitionPolicyV1 policy = new RoutePartitionPolicyV1(
                0, ActivationBarrierV1.empty(source, 0, 1L, bytes(32, 83)), quota, 1, bytes(32, 84));
        return RouteSnapshotV1.create(
                new RouteIncarnation(bytes(16, 85)),
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycleV1.ACTIVE_FOR_NEW,
                900,
                ingress,
                RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1),
                1,
                List.of(policy),
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                100,
                1000,
                new com.nereusstream.delay.protocol.IngressCredentialBindingRefV1(
                        bytes(32, 86), 1, bytes(32, 87), bytes(32, 88), bytes(32, 89)),
                bytes(32, 90),
                new TrustedUtcIntervalEvidence(
                        200,
                        201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        bytes(8, 91),
                        1,
                        2,
                        3,
                        bytes(32, 92),
                        0,
                        null),
                1,
                keys.getPrivate());
    }

    private static TrustedUtcIntervalEvidence nativeIssuedAt() {
        return new TrustedUtcIntervalEvidence(
                200,
                210,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                bytes(8, 93),
                1,
                2,
                3,
                bytes(32, 94),
                0,
                null);
    }

    private static UUID uuidV7(final long timestamp, final int entropy) {
        return new UUID(
                (timestamp << 16) | 0x7000L | (entropy & 0x0fffL), Long.MIN_VALUE | (entropy & 0x3fff_ffff_ffff_ffffL));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record Fixture(
            KeyPair keys,
            AuthenticatedTenantContext tenant,
            RouteSnapshotV1 route,
            RouteSelectionHint hint,
            PulsarBrokerResourceIdentityV1 target,
            ScheduleIntentV1 intent,
            NativePreparationSnapshotV1 candidate,
            com.nereusstream.delay.protocol.PreparedCommand command,
            TrustedUtcIntervalEvidence nativeIssuedAt) {}

    private static final class SingleRouteProvider implements com.nereusstream.delay.route.RouteSnapshotProvider {
        private final RouteSnapshotV1 route;

        private SingleRouteProvider(final RouteSnapshotV1 route) {
            this.route = route;
        }

        @Override
        public RouteSnapshotV1 activeForNewSchedule(
                final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
            return route;
        }

        @Override
        public RouteSnapshotV1 exact(final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
            return route;
        }

        @Override
        public long publishedRevision() {
            return 1;
        }
    }

    private static final class SequenceUuids implements LogicalUuidV7Generator {
        private final UUID[] values;
        private int index;

        private SequenceUuids(final UUID... values) {
            this.values = values;
        }

        @Override
        public UUID next(final TrustedTimeSnapshot trustedTime) {
            return values[index++];
        }
    }
}
