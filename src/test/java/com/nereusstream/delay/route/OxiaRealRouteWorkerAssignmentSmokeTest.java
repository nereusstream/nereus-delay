package com.nereusstream.delay.route;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import com.nereusstream.delay.ownership.OxiaSyncWorkerAssignmentBackend;
import com.nereusstream.delay.ownership.RouteWorkerAssignmentCoordinator;
import com.nereusstream.delay.ownership.WorkerAssignment;
import com.nereusstream.delay.ownership.WorkerAssignmentAuthority;
import com.nereusstream.delay.ownership.WorkerAssignmentCoordinator;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimensionV1;
import com.nereusstream.delay.protocol.CapacityVectorV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import com.nereusstream.delay.store.WorkerLoadVector;
import com.nereusstream.delay.store.WorkerPlacementPolicy;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
                        endpoint,
                        namespace,
                        "nereus-delay-route-worker-publisher-" + UUID.randomUUID(),
                        Duration.ofSeconds(15),
                        routePrefix);
                OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                        endpoint,
                        namespace,
                        "nereus-delay-route-worker-provider-" + UUID.randomUUID(),
                        Duration.ofSeconds(15),
                        routePrefix);
                OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                        endpoint,
                        namespace,
                        "nereus-delay-route-worker-assignment-" + UUID.randomUUID(),
                        Duration.ofSeconds(15),
                        assignmentPrefix)) {
            final OxiaSignedRouteSnapshotPublisher publisher =
                    new OxiaSignedRouteSnapshotPublisher(publisherSession, routePrefix, keys.getPublic());
            final OxiaSignedRouteSnapshotProvider provider =
                    new OxiaSignedRouteSnapshotProvider(providerSession, routePrefix, keys.getPublic(), () -> 200);
            final var snapshot = OxiaSignedRouteSnapshotProviderTest.snapshot(
                    keys, incarnation, com.nereusstream.delay.protocol.RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
            assertEquals(1, publisher.publish(hint, snapshot, 0).revision());
            provider.refresh().toCompletableFuture().join();

            final WorkerAssignmentAuthority authority =
                    new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
            final WorkerAssignmentCoordinator worker = new WorkerAssignmentCoordinator(
                    new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
            final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(provider, worker);
            final RouteWorkerAssignmentCoordinator.RoutePlacementResult result =
                    coordinator.placeExact(tenant, incarnation, request());
            final WorkerAssignmentAuthority.Publication publication = result.publication();
            final WorkerAssignment accepted =
                    coordinator.requireAccepted(tenant, publication.revision(), publication.assignment());

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

    @Test
    void signedRoutePublicationPlacesTwoShardsAcrossTwoWorkersWithSessionBoundCas() throws Exception {
        final String endpoint = endpoint();
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String routePrefix = "nereus-delay-real-route-worker-multi-shard/" + UUID.randomUUID();
        final String assignmentPrefix = "nereus-delay-real-route-worker-multi-shard-assignment/" + UUID.randomUUID();
        final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final RouteSelectionHint hint = OxiaSignedRouteSnapshotProviderTest.hint();
        final AuthenticatedTenantContext tenant = tenant();
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 31));

        try (OxiaRouteAuthoritySession publisherSession = OxiaRouteAuthoritySession.connect(
                        endpoint,
                        namespace,
                        "nereus-delay-route-worker-multi-publisher-" + UUID.randomUUID(),
                        Duration.ofSeconds(15),
                        routePrefix);
                OxiaRouteAuthoritySession providerSession = OxiaRouteAuthoritySession.connect(
                        endpoint,
                        namespace,
                        "nereus-delay-route-worker-multi-provider-" + UUID.randomUUID(),
                        Duration.ofSeconds(15),
                        routePrefix);
                OxiaSyncOwnerLeaseBackend.ClientHandle assignmentHandle = OxiaSyncOwnerLeaseBackend.connectUnchecked(
                        endpoint,
                        namespace,
                        "nereus-delay-route-worker-multi-assignment-" + UUID.randomUUID(),
                        Duration.ofSeconds(15),
                        assignmentPrefix)) {
            final OxiaSignedRouteSnapshotPublisher publisher =
                    new OxiaSignedRouteSnapshotPublisher(publisherSession, routePrefix, keys.getPublic());
            final OxiaSignedRouteSnapshotProvider provider =
                    new OxiaSignedRouteSnapshotProvider(providerSession, routePrefix, keys.getPublic(), () -> 200);
            final var snapshot = OxiaSignedRouteSnapshotProviderTest.snapshot(
                    keys, incarnation, com.nereusstream.delay.protocol.RouteLifecycleV1.ACTIVE_FOR_NEW, 1);
            assertEquals(1, publisher.publish(hint, snapshot, 0).revision());
            provider.refresh().toCompletableFuture().join();

            final WorkerAssignmentAuthority authority =
                    new OxiaSyncWorkerAssignmentBackend(assignmentHandle, assignmentPrefix);
            final WorkerAssignmentCoordinator worker = new WorkerAssignmentCoordinator(
                    new WorkerPlacementPolicy(new WorkerPlacementPolicy.Configuration(1_000, 0, 0, 0, 0)), authority);
            final RouteWorkerAssignmentCoordinator coordinator = new RouteWorkerAssignmentCoordinator(provider, worker);
            final RouteWorkerAssignmentCoordinator.RoutePlacementResult first = coordinator.placeExact(
                    tenant,
                    incarnation,
                    request(0, 60, List.of(candidate("worker-a", 0, 0), candidate("worker-b", 0, 0))));
            final WorkerAssignment firstAccepted = coordinator.requireAccepted(
                    tenant, first.publication().revision(), first.publication().assignment());
            final RouteWorkerAssignmentCoordinator.RoutePlacementResult second = coordinator.placeExact(
                    tenant,
                    incarnation,
                    request(1, 61, List.of(candidate("worker-a", 1, 1), candidate("worker-b", 0, 0))));
            final WorkerAssignment secondAccepted = coordinator.requireAccepted(
                    tenant,
                    second.publication().revision(),
                    second.publication().assignment());

            assertEquals(1, first.routeRevision());
            assertEquals(1, second.routeRevision());
            assertEquals(0, firstAccepted.sourceAssignment().shardId().partition());
            assertEquals(1, secondAccepted.sourceAssignment().shardId().partition());
            assertNotEquals(firstAccepted.workerId(), secondAccepted.workerId());
            assertEquals(
                    firstAccepted,
                    authority
                            .current(firstAccepted.sourceAssignment().shardId())
                            .orElseThrow()
                            .assignment());
            assertEquals(
                    secondAccepted,
                    authority
                            .current(secondAccepted.sourceAssignment().shardId())
                            .orElseThrow()
                            .assignment());
            assertTrue(authority.withdraw(first.publication()));
            assertTrue(authority.withdraw(second.publication()));
            assertTrue(authority
                    .current(firstAccepted.sourceAssignment().shardId())
                    .isEmpty());
            assertTrue(authority
                    .current(secondAccepted.sourceAssignment().shardId())
                    .isEmpty());
            provider.close();
            System.out.println("Oxia signed Route -> multi-shard Worker placement smoke passed: routeRevision=1, "
                    + "shards=2, workers=2, session-bound CAS");
        }
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest request() {
        return request(0, 50, List.of(candidate()));
    }

    private static RouteWorkerAssignmentCoordinator.PlacementRequest request(
            final int partition,
            final int assignmentSeed,
            final List<WorkerPlacementPolicy.WorkerCandidate> candidates) {
        return new RouteWorkerAssignmentCoordinator.PlacementRequest(
                partition,
                Bytes.sha256(Bytes.utf8("real-route-worker-assignment-" + assignmentSeed)),
                1,
                Bytes.sha256(Bytes.utf8("real-route-worker-capacity-" + assignmentSeed)),
                1,
                candidates,
                capacity(1),
                CapacityVectorV1.empty(),
                CapacityVectorV1.empty(),
                null,
                200,
                0,
                0);
    }

    private static WorkerPlacementPolicy.WorkerCandidate candidate() {
        return candidate("route-worker", 0, 0);
    }

    private static WorkerPlacementPolicy.WorkerCandidate candidate(
            final String workerId, final long committedDbInstances, final long ownedShardDbs) {
        return new WorkerPlacementPolicy.WorkerCandidate(
                workerId,
                capacity(1),
                capacity(committedDbInstances),
                ownedShardDbs,
                1,
                ownedShardDbs,
                1,
                WorkerLoadVector.empty(),
                WorkerLoadVector.empty(),
                200,
                true,
                0);
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
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
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
