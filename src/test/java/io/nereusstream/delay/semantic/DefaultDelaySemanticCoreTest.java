package io.nereusstream.delay.semantic;

import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaIngressRouteResourceV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedSubmissionV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.SelfRoutingId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.route.RouteHashV1;
import io.nereusstream.delay.route.RouteSnapshotProvider;

import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultDelaySemanticCoreTest {
    @Test
    void managedScheduleUsesOneRouteHashAndIndependentPreIoIdentities() throws Exception {
        final RouteSnapshotV1 snapshot = kafkaSnapshot();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        final ScheduleIntentV1 intent = schedule();
        final UUID logicalMessage = uuidV7(210, 1);
        final UUID logicalCommand = uuidV7(211, 2);
        final CountingRoutes routes = new CountingRoutes(snapshot);
        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(routes,
                new SequenceUuids(logicalMessage, logicalCommand), () -> 200);

        final PreparedSubmissionV1 submission = core.prepareSchedule(tenant, hint, intent, 600,
                SubmissionModeV1.MANAGED);
        assertTrue(submission.isManaged());
        final PreparedCommand command = CommandCodec.decodeFrameV1(submission.managedFrame());
        final int expectedPartition = RouteHashV1.partition(snapshot, tenant.tenantRoutingScope(), Bytes.utf8("key"));
        assertEquals(expectedPartition, command.shardId().partition());
        assertEquals(logicalMessage, SelfRoutingId.decode(command.delayMessageId().bytes()).logicalId());
        assertEquals(logicalCommand, SelfRoutingId.decode(command.commandId().bytes()).logicalId());
        assertNotEquals(command.commandId(), command.delayMessageId());
        assertEquals(1, routes.activeReads);
        assertEquals(0, routes.exactReads);
    }

    @Test
    void directAndGatewayCompositionsCanProduceIdenticalBytes() throws Exception {
        final RouteSnapshotV1 snapshot = kafkaSnapshot();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));

        final PreparedSubmissionV1 direct = new DefaultDelaySemanticCore(new CountingRoutes(snapshot),
                new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200)
                .prepareSchedule(tenant, hint, schedule(), 600, SubmissionModeV1.MANAGED);
        final PreparedSubmissionV1 gateway = new DefaultDelaySemanticCore(new CountingRoutes(snapshot),
                new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200)
                .prepareSchedule(tenant, hint, schedule(), 600, SubmissionModeV1.MANAGED);

        assertArrayEquals(direct.canonicalBytes(), gateway.canonicalBytes());
    }

    @Test
    void largePreparationUsesTheIntentOrderingKeyForTheSameRoutingContract() throws Exception {
        final RouteSnapshotV1 snapshot = kafkaSnapshot();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        final ScheduleIntentV1 intent = ScheduleIntentV1.forPrepare(destination(), retryPolicy(), 300, 800,
                DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT, Bytes.utf8("large-key"),
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        final LargeSchedulePreparationV1 request = new LargeSchedulePreparationV1(intent, 10, bytes(32, 80), 100,
                new PayloadProofTrustSetRefV1(1, bytes(32, 81)),
                new ProfileRefV1(Bytes.utf8("object-store"), 1, bytes(32, 82), ProfileKindV1.OBJECT_STORE));
        final PreparedCommand command = new DefaultDelaySemanticCore(new CountingRoutes(snapshot),
                new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200)
                .prepareLargeSchedule(tenant, hint, request, 600);

        assertEquals(RouteHashV1.partition(snapshot, tenant.tenantRoutingScope(), Bytes.utf8("large-key")),
                command.shardId().partition());
        assertEquals(command, CommandCodec.decodeFrameV1(CommandCodec.encodeFrameV1(command)));
    }

    @Test
    void tenantMismatchFailsClosedBeforeCommandPreparation() throws Exception {
        final RouteSnapshotV1 snapshot = kafkaSnapshot();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKindV1.KAFKA, Bytes.utf8("primary"));
        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(new CountingRoutes(snapshot),
                new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200);

        final SemanticPreparationException failure = assertThrows(SemanticPreparationException.class,
                () -> core.prepareSchedule(new AuthenticatedTenantContext(bytes(32, 9), bytes(32, 2), bytes(32, 3)),
                        hint, schedule(), 600, SubmissionModeV1.MANAGED));
        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, failure.error().code());
    }

    private static ScheduleIntentV1 schedule() {
        return ScheduleIntentV1.create(destination(), retryPolicy(), 300, 800, DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT, Bytes.utf8("key"), Bytes.utf8("payload"), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
    }

    private static ProfileRefV1 destination() {
        return new ProfileRefV1(Bytes.utf8("destination"), 1, bytes(32, 60), ProfileKindV1.DESTINATION);
    }

    private static RetryPolicyRefV1 retryPolicy() {
        return new RetryPolicyRefV1(Bytes.utf8("retry"), 1, bytes(32, 61));
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static RouteSnapshotV1 kafkaSnapshot() throws Exception {
        final UUID topicUuid = UUID.fromString("12345678-1234-7abc-8def-1234567890ab");
        final KafkaIngressRouteResourceV1 ingress = new KafkaIngressRouteResourceV1(
                "cluster", "persistent://tenant/ns/delay", topicUuid, 2);
        final BrokerResourceIdentityV1 broker = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("cluster", topicUuid));
        final QuotaGrantRefV1 quota = new QuotaGrantRefV1(bytes(32, 20), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0));
        final List<RoutePartitionPolicyV1> partitions = List.of(
                policy(0, broker, quota), policy(1, broker, quota));
        return RouteSnapshotV1.create(new RouteIncarnation(bytes(16, 30)), bytes(32, 1), bytes(32, 2),
                RouteLifecycleV1.ACTIVE_FOR_NEW, 900, ingress, RoutingHashVersionV1.ROUTING_HASH_V1,
                new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), 1, partitions,
                100, 200, 1024, 4096, 10, 8192, 500, 100, 1000,
                new io.nereusstream.delay.protocol.IngressCredentialBindingRefV1(
                        bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                bytes(32, 44), new TrustedUtcIntervalEvidence(200, 201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 45), 1, 2, 3,
                        bytes(32, 46), 0, null), 1,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
    }

    private static RoutePartitionPolicyV1 policy(final int number, final BrokerResourceIdentityV1 broker,
                                                   final QuotaGrantRefV1 quota) {
        return new RoutePartitionPolicyV1(number, ActivationBarrierV1.kafka(broker, number, 0, 0), quota, 1,
                bytes(32, 50 + number));
    }

    private static UUID uuidV7(final long timestamp, final int entropy) {
        return new UUID((timestamp << 16) | 0x7000L | (entropy & 0x0fffL),
                Long.MIN_VALUE | (entropy & 0x3fff_ffff_ffff_ffffL));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class SequenceUuids implements LogicalUuidV7Generator {
        private final UUID[] values;
        private int index;

        private SequenceUuids(final UUID... values) {
            this.values = values;
        }

        @Override
        public UUID next(final TrustedTimeSnapshot trustedTime) {
            if (index >= values.length) {
                throw new IllegalStateException("UUID sequence exhausted");
            }
            return values[index++];
        }
    }

    private static final class CountingRoutes implements RouteSnapshotProvider {
        private final RouteSnapshotV1 snapshot;
        private int activeReads;
        private int exactReads;

        private CountingRoutes(final RouteSnapshotV1 snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public RouteSnapshotV1 activeForNewSchedule(final AuthenticatedTenantContext context,
                                                     final RouteSelectionHint hint) {
            activeReads++;
            return snapshot;
        }

        @Override
        public RouteSnapshotV1 exact(final RouteIncarnation incarnation,
                                     final AuthenticatedTenantContext context) {
            exactReads++;
            return snapshot;
        }

        @Override
        public long publishedRevision() {
            return 1;
        }
    }
}
