package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.HandoffPath;
import com.nereusstream.delay.protocol.HandoffPolicyMode;
import com.nereusstream.delay.protocol.HandoffPolicySnapshot;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarIngressRouteResource;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentity;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.TargetPartitionHash;
import com.nereusstream.delay.protocol.TargetPartitionHashInput;
import com.nereusstream.delay.protocol.TargetPartitionPolicy;
import com.nereusstream.delay.protocol.TimingCapability;
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

        final Optional<NativePreparationSnapshot> selected = cache.eligibleFor(
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
        final var prepared =
                core.prepareSchedule(fixture.tenant(), fixture.hint(), fixture.intent(), 700, SubmissionMode.AUTO_FAST);
        assertFalse(prepared.isManaged());
        assertTrue(prepared.isNativeRecordReady());
        assertArrayEquals(bytes(32, 90), prepared.nativeRecordContext().publishAttemptId());
        com.nereusstream.delay.protocol.NativePreparedRecordBinding.requireExact(
                prepared.nativeRecordContext(), prepared.nativePrepared());
        assertEquals(fixture.candidate().target(), prepared.nativePrepared().target());
        assertEquals(
                fixture.candidate().physicalPartition(),
                prepared.nativePrepared().physicalPartition());
    }

    @Test
    void cacheRejectsWrongTargetPartitionAndFallsBackToTheExactManagedFrame() throws Exception {
        final Fixture fixture = fixture();
        final int wrongPartition = fixture.candidate().physicalPartition() == 0 ? 1 : 0;
        final NativeCapabilitySnapshot wrongSnapshot = NativeCapabilitySnapshot.create(
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
        final NativePreparationSnapshot wrongCandidate = new NativePreparationSnapshot(
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
                fixture.tenant(), fixture.hint(), fixture.intent(), 700, SubmissionMode.MANAGED);
        final DefaultDelaySemanticCore autoCore = new DefaultDelaySemanticCore(
                new SingleRouteProvider(fixture.route()),
                new SequenceUuids(uuidV7(300, 1), uuidV7(300, 2)),
                () -> 300,
                cache,
                (command, intent) -> bytes(32, 91));
        final var observed = autoCore.prepareSchedule(
                fixture.tenant(), fixture.hint(), fixture.intent(), 700, SubmissionMode.AUTO_FAST);
        assertTrue(observed.isManaged());
        assertArrayEquals(managed.managedFrame(), observed.managedFrame());
    }

    @Test
    void installRejectsSnapshotWithInvalidIssuerSignature() throws Exception {
        final Fixture fixture = fixture();
        final byte[] encoded = fixture.candidate().capabilitySnapshot().canonicalBytes();
        encoded[encoded.length - 1] ^= 0x01;
        final NativeCapabilitySnapshot tampered = NativeCapabilitySnapshot.decode(encoded);
        final NativePreparationSnapshot candidate = new NativePreparationSnapshot(
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
        final PulsarBrokerResourceIdentity target = new PulsarBrokerResourceIdentity(
                "target-cluster", bytes(32, 10), "persistent://tenant/ns/destination", 20);
        final ProfileSemanticEnvelope capability = capability();
        final DestinationProfileSemantic destinationBody = new DestinationProfileSemantic(
                AdapterKind.PULSAR,
                BrokerResourceIdentity.pulsar(target),
                2,
                TargetPartitionPolicy.HASH_ONLY,
                TargetPartitionHashInput.ORDERING_KEY,
                List.of(),
                capability.ref(),
                1,
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
        final ProfileSemanticEnvelope destination =
                new ProfileSemanticEnvelope(ProfileKind.DESTINATION, Bytes.utf8("destination"), 1, destinationBody);
        final PulsarMetadata metadata = new PulsarMetadata(null, null, Bytes.utf8("native-ordering"), List.of());
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination.ref(),
                new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 42)),
                400,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("delay-ordering"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.pulsar(metadata),
                null,
                null,
                NativeDeliveryPolicy.ALLOW_AUTO_FAST_AND_MANAGED_HANDOFF);
        final int physicalPartition = (int) TargetPartitionHash.partition(destination.ref(), 2, metadata.orderingKey());
        final TrustedUtcIntervalEvidence nativeIssuedAt = nativeIssuedAt();
        final NativeCapabilitySnapshot capabilitySnapshot = NativeCapabilitySnapshot.create(
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
        final HandoffPolicySnapshot handoffPolicy = HandoffPolicySnapshot.create(
                bytes(32, 53),
                1,
                HandoffPolicyMode.ENABLED,
                20,
                250,
                900,
                HandoffPath.AUTO_FAST,
                nativeIssuedAt,
                1,
                bytes(32, 54),
                keys.getPrivate());
        final NativePreparationSnapshot candidate = new NativePreparationSnapshot(
                destination, capability, target, physicalPartition, capabilitySnapshot, handoffPolicy);
        final RouteSnapshot route = route(keys);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.PULSAR, Bytes.utf8("primary"));
        final com.nereusstream.delay.protocol.ShardId shard =
                new com.nereusstream.delay.protocol.ShardId(route.routeIncarnation(), 0);
        final var command = com.nereusstream.delay.protocol.PreparedCommand.schedule(shard, intent, 700);
        return new Fixture(keys, tenant, route, hint, target, intent, candidate, command, nativeIssuedAt);
    }

    private static ProfileSemanticEnvelope capability() {
        final DeliveryCapabilitySemantic body = new DeliveryCapabilitySemantic(
                AdapterKind.PULSAR,
                OutcomeCapability.AT_LEAST_ONCE,
                TimingCapability.ORDINARY_MANAGED | TimingCapability.PULSAR_AUTO_FAST,
                null,
                0,
                0,
                0,
                0,
                bytes(32, 60),
                bytes(32, 61),
                0,
                0);
        return new ProfileSemanticEnvelope(ProfileKind.DELIVERY_CAPABILITY, Bytes.utf8("capability"), 1, body);
    }

    private static RouteSnapshot route(final KeyPair keys) {
        final String base = "persistent://tenant/ns/commands";
        final PulsarPhysicalPartitionIdentity physical =
                new PulsarPhysicalPartitionIdentity(0, base + "-partition-0", bytes(32, 80), 81);
        final PulsarIngressRouteResource ingress =
                new PulsarIngressRouteResource("source-cluster", base, List.of(physical));
        final BrokerResourceIdentity source = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                "source-cluster",
                physical.resourceIncarnation(),
                physical.physicalTopic(),
                physical.physicalTopicCreationTimestamp()));
        final QuotaGrantRef quota = new QuotaGrantRef(
                bytes(32, 82),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final RoutePartitionPolicy policy = new RoutePartitionPolicy(
                0, ActivationBarrier.empty(source, 0, 1L, bytes(32, 83)), quota, 1, bytes(32, 84));
        return RouteSnapshot.create(
                new RouteIncarnation(bytes(16, 85)),
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycle.ACTIVE_FOR_NEW,
                900,
                ingress,
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
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
                new com.nereusstream.delay.protocol.IngressCredentialBindingRef(
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
            RouteSnapshot route,
            RouteSelectionHint hint,
            PulsarBrokerResourceIdentity target,
            CanonicalScheduleIntent intent,
            NativePreparationSnapshot candidate,
            com.nereusstream.delay.protocol.PreparedCommand command,
            TrustedUtcIntervalEvidence nativeIssuedAt) {}

    private static final class SingleRouteProvider implements com.nereusstream.delay.route.RouteSnapshotProvider {
        private final RouteSnapshot route;

        private SingleRouteProvider(final RouteSnapshot route) {
            this.route = route;
        }

        @Override
        public RouteSnapshot activeForNewSchedule(
                final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
            return route;
        }

        @Override
        public RouteSnapshot exact(final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
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
