package io.nereusstream.delay.route;

import io.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import io.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import io.nereusstream.delay.ownership.RouteWorkerAssignmentCoordinator;
import io.nereusstream.delay.ownership.WorkerAssignment;
import io.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import io.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.nereusstream.delay.semantic.RouteSelectionHint;
import io.nereusstream.delay.store.WorkerLoadVector;
import io.nereusstream.delay.store.WorkerPlacementPolicy;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in network proof from signed Oxia Route publication to Worker assignment CAS. */
@Tag("real-service")
class OxiaRealRouteWorkerAssignmentSmokeTest {
    @Test
    void signedRoutePublicationFeedsSessionBoundWorkerAssignmentAuthority() throws Exception {
        final String endpoint = endpoint();
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String routePrefix = "nereus-delay-real-route-worker/" + UUID.randomUUID();
        final String assignmentPrefix = "nereus-delay-real-route-worker-assignment/" + UUID.randomUUID();
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSelectionHint hint = OxiaSignedRouteSnapshotProviderTest.hint();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 30));

        try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                     endpoint, namespace, "nereus-delay-route-worker-publisher-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), routePrefix);
             OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                     endpoint, namespace, "nereus-delay-route-worker-provider-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), routePrefix);
             OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                     endpoint, namespace, "nereus-delay-route-worker-assignment-" + UUID.randomUUID(),
                     Duration.ofSeconds(15), assignmentPrefix)) {
            final OxiaSignedRouteSnapshotPublisher publisher = new OxiaSignedRouteSnapshotPublisher(
                    publisherSession, routePrefix, keys.getPublic());
            final OxiaSignedRouteSnapshotProvider provider = new OxiaSignedRouteSnapshotProvider(
                    providerSession, routePrefix, keys.getPublic(), () -> 200);
            final var snapshot = OxiaSignedRouteSnapshotProviderTest.snapshot(keys, incarnation,
                    io.nereusstream.delay.protocol.RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
            assertEquals(1, publisher.publish(hint, snapshot, 0).revision());
            provider.refresh().toCompletableFuture().join();

            final WorkerAssignmentAuthority authority = new OxiaSyncWorkerAssignmentBackend(assignmentHandle,
                    assignmentPrefix);
            final WorkerAssignmentCoordinator worker = new WorkerAssignmentCoordinator(
                    new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)),
                    authority);
            final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(provider,
                    worker);
            final RouteWorkerAssignmentCoordinator.RoutePlacementResult result = coordinator.placeExact(
                    tenant, incarnation, request());
            final WorkerAssignmentAuthority.Publication publication = result.publication();
            final WorkerAssignment accepted = coordinator.requireAccepted(tenant, publication.revision(),
                    publication.assignment());

            assertEquals(1, result.routeRevision());
            assertTrue(accepted.routeBound());
            assertArrayEquals(snapshot.snapshotDigest(), accepted.routeSnapshotDigest());
            assertEquals(accepted, publication.assignment());
            assertTrue(authority.withdraw(publication));
            assertTrue(authority.current(accepted.sourceAssignment().shardId()).isEmpty());
            provider.close();
            System.out.println("Oxia signed Route -> Worker assignment smoke passed: routeRevision=1, "
                    + "assignmentRevision=" + publication.revision() + ", session-bound CAS");
        }
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest request() {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(0,
                Bytes.sha256(Bytes.utf8("real-route-worker-assignment")), 1,
                Bytes.sha256(Bytes.utf8("real-route-worker-capacity")), 1, List.of(candidate()), capacity(1),
                CapacityVectorV1.empty(), CapacityVectorV1.empty(), null, 200, 0, 0);
    }

    private static WorkerPlacementPolicy.WorkerCandidate candidate() {
        return new WorkerPlacementPolicy.WorkerCandidate("route-worker", capacity(2), CapacityVectorV1.empty(),
                0, 16, 0, 16, WorkerLoadVector.empty(), WorkerLoadVector.empty(), 200, true, 0);
    }

    private static CapacityVectorV1 capacity(final long dbInstances) {
        final long[] values = new long[CapacityDimensionV1.COUNT];
        values[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        return new CapacityVectorV1(values);
    }

    private static AuthenticatedTenantContext tenant() {
        return new AuthenticatedTenantContext(bytes(32, 1), bytes(32, 2), bytes(32, 3));
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
