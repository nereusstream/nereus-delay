package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.IngressCredentialBindingRef;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.route.InMemorySignedRouteSnapshotProvider;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.store.WorkerLoadVector;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RouteWorkerAssignmentCoordinatorTest {
    @Test
    void signedRouteProjectionIsPublishedAndRereadBeforeWorkerAcceptance() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemorySignedRouteSnapshotProvider routes =
                new InMemorySignedRouteSnapshotProvider(keys.getPublic(), () -> 200);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        final RouteSnapshot snapshot = route(keys);
        routes.accept(1, 0, hint, snapshot);
        final InMemoryWorkerAssignmentAuthority authority = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentCoordinator worker = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(routes, worker);

        final RouteWorkerAssignmentCoordinator.RoutePlacementResult result =
                coordinator.placeActive(tenant(), hint, request());
        final WorkerAssignmentAuthority.Publication publication = result.publication();
        final WorkerAssignment assignment = publication.assignment();

        assertTrue(assignment.routeBound());
        assertArrayEquals(snapshot.snapshotDigest(), assignment.routeSnapshotDigest());
        assertEquals(
                snapshot.routeIncarnation(),
                assignment.sourceAssignment().shardId().routeIncarnation());
        assertEquals(
                17,
                ((com.nereusstream.delay.protocol.KafkaActivationBarrier)
                                assignment.sourceAssignment().activationBarrier())
                        .exclusiveOffset());
        assertEquals(assignment, coordinator.requireAccepted(tenant(), publication.revision(), assignment));
    }

    @Test
    void acceptanceRejectsAnUnboundOrChangedRouteProjection() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemorySignedRouteSnapshotProvider routes =
                new InMemorySignedRouteSnapshotProvider(keys.getPublic(), () -> 200);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        routes.accept(1, 0, hint, route(keys));
        final InMemoryWorkerAssignmentAuthority authority = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentCoordinator worker = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
        final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(routes, worker);

        final ShardId unboundShard = new ShardId(RouteIncarnation.random(), 0);
        final WorkerAssignmentCoordinator.PlacementResult local = worker.place(
                new SourceAssignment(
                        unboundShard,
                        Bytes.sha256(Bytes.utf8("unbound")),
                        1,
                        new com.nereusstream.delay.protocol.KafkaActivationBarrier(
                                unboundShard, "cluster", UUID.randomUUID(), 0)),
                Bytes.sha256(Bytes.utf8("capacity")),
                1,
                List.of(candidate()),
                capacity(1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                200,
                0,
                0);
        final WorkerAssignment unbound = local.publication().orElseThrow().assignment();
        assertThrows(IllegalArgumentException.class, () -> coordinator.requireAccepted(tenant(), 1, unbound));

        final RouteWorkerAssignmentCoordinator.RoutePlacementResult placed =
                coordinator.placeActive(tenant(), hint, request());
        final WorkerAssignment changedDigest = new WorkerAssignment(
                placed.publication().assignment().workerId(),
                placed.sourceAssignment(),
                placed.publication().assignment().placementEpoch(),
                placed.publication().assignment().capacityEnvelopeDigest(),
                Bytes.sha256(Bytes.utf8("changed")));
        assertThrows(
                IllegalStateException.class,
                () -> coordinator.requireAccepted(tenant(), placed.publication().revision(), changedDigest));
    }

    @Test
    void optionalCapabilityAuthorityGatesRoutePlacementAndAcceptance() throws Exception {
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemorySignedRouteSnapshotProvider routes =
                new InMemorySignedRouteSnapshotProvider(keys.getPublic(), () -> 200);
        final RouteSelectionHint hint = new RouteSelectionHint(AdapterKind.KAFKA, Bytes.utf8("primary"));
        routes.accept(1, 0, hint, route(keys));
        final InMemoryWorkerAssignmentAuthority assignments = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentCoordinator worker = new WorkerAssignmentCoordinator(
                new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), assignments);
        final InMemoryProtocolCapabilityAuthority capabilities = new InMemoryProtocolCapabilityAuthority();
        final ProtocolTuple tuple = route(keys).protocolTuple();
        capabilities.publish(
                new ProtocolCapabilityDeclaration("worker-a", bytes(32, 70), List.of(tuple), 1, bytes(32, 71)), 0);
        final RouteWorkerAssignmentCoordinator coordinator =
                new RouteWorkerAssignmentCoordinator(routes, worker, capabilities);

        final var placed = coordinator.placeActive(tenant(), hint, request());
        assertEquals(
                "worker-a",
                coordinator
                        .requireAccepted(
                                tenant(),
                                placed.publication().revision(),
                                placed.publication().assignment())
                        .workerId());
        capabilities.clear();
        assertThrows(IllegalStateException.class, () -> coordinator.placeActive(tenant(), hint, request()));
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest request() {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                0,
                Bytes.sha256(Bytes.utf8("route-assignment")),
                1,
                Bytes.sha256(Bytes.utf8("capacity-envelope")),
                1,
                List.of(candidate()),
                capacity(1),
                CapacityVector.empty(),
                CapacityVector.empty(),
                null,
                200,
                0,
                0);
    }

    private static WorkerPlacementPolicy.WorkerCandidate candidate() {
        return new WorkerPlacementPolicy.WorkerCandidate(
                "worker-a",
                capacity(2),
                CapacityVector.empty(),
                0,
                10,
                0,
                10,
                WorkerLoadVector.empty(),
                WorkerLoadVector.empty(),
                200,
                true,
                0);
    }

    private static CapacityVector capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimension.COUNT];
        values[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVector(values);
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static RouteSnapshot route(final KeyPair keys) {
        final UUID topic = UUID.fromString("12345678-1234-7abc-8def-1234567890ab");
        final KafkaIngressRouteResource ingress =
                new KafkaIngressRouteResource("cluster", "persistent://tenant/ns/delay", topic, 1);
        final BrokerResourceIdentity broker =
                BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", topic));
        final QuotaGrantRef quota = new QuotaGrantRef(
                Bytes.sha256(Bytes.utf8("quota")),
                1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
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
                List.of(new RoutePartitionPolicy(
                        0, ActivationBarrier.kafka(broker, 0, 17, 18), quota, 1, bytes(32, 3))),
                100,
                200,
                1024,
                4096,
                10,
                8192,
                500,
                100,
                1000,
                new IngressCredentialBindingRef(bytes(32, 40), 1, bytes(32, 41), bytes(32, 42), bytes(32, 43)),
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
                keys.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private static final class InMemoryProtocolCapabilityAuthority implements ProtocolCapabilityAuthority {
        private final Map<String, Publication> values = new java.util.HashMap<>();

        @Override
        public Publication publish(final ProtocolCapabilityDeclaration declaration, final long expectedRevision) {
            final Publication publication = new Publication(expectedRevision + 1, declaration);
            values.put(declaration.workerId(), publication);
            return publication;
        }

        @Override
        public Optional<Publication> current(final String workerId) {
            return Optional.ofNullable(values.get(workerId));
        }

        @Override
        public boolean withdraw(final Publication expected) {
            return values.remove(expected.declaration().workerId(), expected);
        }

        private void clear() {
            values.clear();
        }
    }
}
