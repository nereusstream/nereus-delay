package com.nereusstream.delay.semantic;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.CommandBodies;
import com.nereusstream.delay.protocol.CommandCodec;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.route.RouteHash;
import com.nereusstream.delay.route.RouteSnapshotProvider;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DefaultDelaySemanticCoreTest {
    @Test
    void managedScheduleUsesOneRouteHashAndIndependentPreIoIdentities() throws Exception {
        final RouteSnapshot snapshot = kafkaSnapshot();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        final CanonicalScheduleIntent intent = schedule();
        final UUID logicalMessage = uuidV7(210, 1);
        final UUID logicalCommand = uuidV7(211, 2);
        final CountingRoutes routes = new CountingRoutes(snapshot);
        final DefaultDelaySemanticCore core =
                new DefaultDelaySemanticCore(routes, new SequenceUuids(logicalMessage, logicalCommand), () -> 200);

        final PreparedSubmission submission = core.prepareSchedule(tenant, hint, intent, 600, SubmissionMode.MANAGED);
        assertTrue(submission.isManaged());
        final PreparedCommand command = CommandCodec.decodeManagedFrame(submission.managedFrame());
        final int expectedPartition = RouteHash.partition(snapshot, tenant.tenantRoutingScope(), Bytes.utf8("key"));
        assertEquals(expectedPartition, command.shardId().partition());
        assertEquals(
                logicalMessage,
                SelfRoutingId.decode(command.delayMessageId().bytes()).logicalId());
        assertEquals(
                logicalCommand,
                SelfRoutingId.decode(command.commandId().bytes()).logicalId());
        assertNotEquals(command.commandId(), command.delayMessageId());
        assertEquals(1, routes.activeReads);
        assertEquals(0, routes.exactReads);
    }

    @Test
    void directAndGatewayCompositionsCanProduceIdenticalBytes() throws Exception {
        final RouteSnapshot snapshot = kafkaSnapshot();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));

        final PreparedSubmission direct = new DefaultDelaySemanticCore(
                        new CountingRoutes(snapshot), new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200)
                .prepareSchedule(tenant, hint, schedule(), 600, SubmissionMode.MANAGED);
        final PreparedSubmission gateway = new DefaultDelaySemanticCore(
                        new CountingRoutes(snapshot), new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200)
                .prepareSchedule(tenant, hint, schedule(), 600, SubmissionMode.MANAGED);

        assertArrayEquals(direct.canonicalBytes(), gateway.canonicalBytes());
    }

    @Test
    void largePreparationUsesTheIntentOrderingKeyForTheSameRoutingContract() throws Exception {
        final RouteSnapshot snapshot = kafkaSnapshot();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.forPrepare(
                destination(),
                retryPolicy(),
                300,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("large-key"),
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final LargeSchedulePreparation request = new LargeSchedulePreparation(
                intent,
                10,
                bytes(32, 80),
                100,
                new PayloadProofTrustSetRef(1, bytes(32, 81)),
                new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 82), ProfileKind.OBJECT_STORE));
        final PreparedCommand command = new DefaultDelaySemanticCore(
                        new CountingRoutes(snapshot), new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200)
                .prepareLargeSchedule(tenant, hint, request, 600);

        assertEquals(
                RouteHash.partition(snapshot, tenant.tenantRoutingScope(), Bytes.utf8("large-key")),
                command.shardId().partition());
        assertEquals(command, CommandCodec.decodeManagedFrame(CommandCodec.encodeManagedFrame(command)));
    }

    @Test
    void largePreparationSeparatesIngressRouteFromDestinationMetadata() throws Exception {
        final RouteSnapshot snapshot = kafkaSnapshot();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.forPrepare(
                destination(),
                retryPolicy(),
                300,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("cross-key"),
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                null);
        final LargeSchedulePreparation request = new LargeSchedulePreparation(
                intent,
                10,
                bytes(32, 80),
                100,
                new PayloadProofTrustSetRef(1, bytes(32, 81)),
                new ProfileRef(Bytes.utf8("object-store"), 1, bytes(32, 82), ProfileKind.OBJECT_STORE));

        final PreparedCommand command = new DefaultDelaySemanticCore(
                        new CountingRoutes(snapshot), new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200)
                .prepareLargeSchedule(tenant, hint, request, 600);
        final PrepareLargeScheduleBody body = CommandBodies.decodePrepareLarge(command.canonicalBody());

        assertEquals(
                AdapterMetadata.Kind.PULSAR,
                body.intentWithoutPayload().adapterMetadata().kind());
        assertEquals(command, CommandCodec.decodeManagedFrame(CommandCodec.encodeManagedFrame(command)));
    }

    @Test
    void tenantMismatchFailsClosedBeforeCommandPreparation() throws Exception {
        final RouteSnapshot snapshot = kafkaSnapshot();
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        final DefaultDelaySemanticCore core = new DefaultDelaySemanticCore(
                new CountingRoutes(snapshot), new SequenceUuids(uuidV7(210, 1), uuidV7(211, 2)), () -> 200);

        final SemanticPreparationException failure = assertThrows(
                SemanticPreparationException.class,
                () -> core.prepareSchedule(
                        new AuthenticatedTenantContext(bytes(32, 9), bytes(32, 2), bytes(32, 3)),
                        hint,
                        schedule(),
                        600,
                        SubmissionMode.MANAGED));
        assertEquals(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE, failure.error().code());
    }

    private static CanonicalScheduleIntent schedule() {
        return CanonicalScheduleIntent.create(
                destination(),
                retryPolicy(),
                300,
                800,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("key"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
    }

    private static ProfileRef destination() {
        return new ProfileRef(Bytes.utf8("destination"), 1, bytes(32, 60), ProfileKind.DESTINATION);
    }

    private static RetryPolicyRef retryPolicy() {
        return new RetryPolicyRef(Bytes.utf8("retry"), 1, bytes(32, 61));
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static RouteSnapshot kafkaSnapshot() throws Exception {
        final UUID topicUuid = UUID.fromString("12345678-1234-7abc-8def-1234567890ab");
        final KafkaIngressRouteResource ingress =
                new KafkaIngressRouteResource("cluster", "persistent://tenant/ns/delay", topicUuid, 2);
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", topicUuid));
        final QuotaGrantRef quota = new QuotaGrantRef(
                bytes(32, 20),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        final List<RoutePartitionPolicy> partitions = List.of(policy(0, broker, quota), policy(1, broker, quota));
        return RouteSnapshot.create(
                new RouteIncarnation(bytes(16, 30)),
                bytes(32, 1),
                bytes(32, 2),
                RouteLifecycle.ACTIVE_FOR_NEW,
                900,
                ingress,
                RoutingHashVersion.ROUTING_HASH,
                new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1),
                1,
                partitions,
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
                        bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
                bytes(32, 44),
                new TrustedUtcIntervalEvidence(
                        200,
                        201,
                        TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        bytes(8, 45),
                        1,
                        2,
                        3,
                        bytes(32, 46),
                        0,
                        null),
                1,
                KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
    }

    private static RoutePartitionPolicy policy(
            final int number, final BrokerResourceIdentity broker, final QuotaGrantRef quota) {
        return new RoutePartitionPolicy(
                number, ActivationBarrier.kafka(broker, number, 0, 0), quota, 1, bytes(32, 50 + number));
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
        private final RouteSnapshot snapshot;
        private int activeReads;
        private int exactReads;

        private CountingRoutes(final RouteSnapshot snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public RouteSnapshot activeForNewSchedule(
                final AuthenticatedTenantContext context, final RouteSelectionHint hint) {
            activeReads++;
            return snapshot;
        }

        @Override
        public RouteSnapshot exact(final RouteIncarnation incarnation, final AuthenticatedTenantContext context) {
            exactReads++;
            return snapshot;
        }

        @Override
        public long publishedRevision() {
            return 1;
        }
    }
}
