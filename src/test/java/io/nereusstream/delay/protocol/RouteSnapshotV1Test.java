package io.nereusstream.delay.protocol;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RouteSnapshotV1Test {
    @Test
    void signedKafkaRouteRoundTripsAndRejectsTampering() throws Exception {
        final KeyPair keyPair = ed25519();
        final RouteSnapshotV1 snapshot = kafkaSnapshot(keyPair);

        final RouteSnapshotV1 decoded = RouteSnapshotV1.decode(snapshot.canonicalBytes(), keyPair.getPublic());
        assertEquals(snapshot, decoded);
        assertEquals(0x8000_0001L, decoded.signingKeyVersion());
        assertArrayEquals(snapshot.snapshotDigest(), decoded.snapshotDigest());
        assertDoesNotThrow(() -> snapshot.requireUsableForNewSchedule(bytes(32, 1), bytes(32, 2), 200));

        final byte[] tampered = snapshot.canonicalBytes();
        tampered[tampered.length - 1] ^= 0x01;
        assertThrows(IllegalArgumentException.class,
                () -> RouteSnapshotV1.decode(tampered, keyPair.getPublic()));
        assertThrows(IllegalArgumentException.class,
                () -> RouteSnapshotV1.decode(snapshot.canonicalBytes(), ed25519().getPublic()));
    }

    @Test
    void routeAdmissionKeepsTenantAndLifecycleBoundaries() throws Exception {
        final RouteSnapshotV1 snapshot = kafkaSnapshot(ed25519());

        assertThrows(IllegalArgumentException.class,
                () -> snapshot.requireUsableForNewSchedule(bytes(32, 9), bytes(32, 2), 200));
        assertThrows(IllegalArgumentException.class,
                () -> snapshot.requireUsableForNewSchedule(bytes(32, 1), bytes(32, 9), 200));
        assertThrows(IllegalArgumentException.class,
                () -> snapshot.requireUsableForNewSchedule(bytes(32, 1), bytes(32, 2), 901));

        final RouteSnapshotV1 controlOnly = RouteSnapshotV1.create(
                snapshot.routeIncarnation(), snapshot.authenticatedTenantScopeHash(), snapshot.tenantRoutingScope(),
                RouteLifecycleV1.CONTROL_ONLY, snapshot.newScheduleAcceptUntilEpochMs(), snapshot.ingress(),
                snapshot.routingHashVersion(), snapshot.protocolTuple(), snapshot.controlVersion(),
                snapshot.partitions(), snapshot.queuedReceiptQueryWindowMs(), snapshot.fullCommandResultRetentionMs(),
                snapshot.maxInlinePayloadBytes(), snapshot.maxCommandBytes(), snapshot.maxBatchCommands(),
                snapshot.maxBatchBytes(), snapshot.maximumPreparationAgeMs(), snapshot.validFromEpochMs(),
                snapshot.validUntilEpochMs(), snapshot.credentialBinding(), snapshot.routePrerequisiteDigest(),
                snapshot.issuedAt(), snapshot.signingKeyVersion(), ed25519().getPrivate());
        assertThrows(IllegalArgumentException.class,
                () -> controlOnly.requireUsableForNewSchedule(bytes(32, 1), bytes(32, 2), 200));
    }

    @Test
    void pulsarResourceUsesExactPhysicalPartitionIdentity() {
        final String base = "persistent://tenant/ns/delay";
        final PulsarIngressRouteResourceV1 resource = new PulsarIngressRouteResourceV1("cluster", base, List.of(
                new PulsarPhysicalPartitionIdentityV1(0, base + "-partition-0", bytes(32, 10), 100),
                new PulsarPhysicalPartitionIdentityV1(1, base + "-partition-1", bytes(32, 11), 101)));

        assertEquals(resource, IngressRouteResourceV1.decode(resource.canonicalBytes()));
        assertThrows(IllegalArgumentException.class, () -> new PulsarIngressRouteResourceV1("cluster", base,
                List.of(new PulsarPhysicalPartitionIdentityV1(0, base + "-partition-1", bytes(32, 10), 100))));
    }

    private static RouteSnapshotV1 kafkaSnapshot(final KeyPair keyPair) {
        final UUID topicUuid = UUID.fromString("12345678-1234-7abc-8def-1234567890ab");
        final KafkaIngressRouteResourceV1 ingress = new KafkaIngressRouteResourceV1(
                "cluster", "persistent://tenant/ns/delay", topicUuid, 2);
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", topicUuid));
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(bytes(32, 20), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0));
        final List<RoutePartitionPolicyV1> partitions = List.of(
                partition(0, broker, quota), partition(1, broker, quota));
        return RouteSnapshotV1.create(new RouteIncarnation(bytes(16, 30)), bytes(32, 1), bytes(32, 2),
                RouteLifecycleV1.ACTIVE_FOR_NEW, 900, ingress, RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1, partitions,
                100, 200, 1024, 4096, 10, 8192, 500, 100, 1000,
                new IngressCredentialBindingRefV1(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                bytes(32, 44), new TrustedUtcIntervalEvidence(200, 201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 45), 1, 2, 3,
                        bytes(32, 46), 0, null), 0x8000_0001L, keyPair.getPrivate());
    }

    private static RoutePartitionPolicyV1 partition(final int number, final BrokerResourceIdentityV1 broker,
                                                     final QuotaGrantRefV1 quota) {
        return new RoutePartitionPolicyV1(number, ActivationBarrierV1.kafka(broker, number, 0, 0), quota, 1,
                bytes(32, 50 + number));
    }

    private static KeyPair ed25519() throws Exception {
        return KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
