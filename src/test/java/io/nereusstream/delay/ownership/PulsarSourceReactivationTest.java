package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.IngressCredentialBindingRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PulsarActivationBarrier;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PulsarIngressRouteResourceV1;
import io.nereusstream.delay.protocol.PulsarPhysicalPartitionIdentityV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RouteLifecycleV1;
import io.nereusstream.delay.protocol.RoutePartitionPolicyV1;
import io.nereusstream.delay.protocol.RouteSnapshotV1;
import io.nereusstream.delay.protocol.RoutingHashVersionV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.route.RouteSnapshotProvider;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PulsarSourceReactivationTest {
    private static final long NOW = 1_000;
    private static final byte[] RESOURCE = bytes(32, 10);
    private static final byte[] ATTESTATION = bytes(32, 20);
    private static final byte[] ROUTE_DIGEST_SEED = bytes(32, 30);
    private static final AuthenticatedTenantContext TENANT = new AuthenticatedTenantContext(
            bytes(32, 40), bytes(32, 50), bytes(32, 60));

    @Test
    void successorCanonicalBytesBindNewGenerationAndImmutableCursor() {
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 1));
        final ShardId shard = new ShardId(incarnation, 0);
        final SourceAssignment previous = assignment(shard, bytes(32, 70), 1, 11);
        final SourceAssignment successor = assignment(shard, bytes(32, 71), 2, 12);
        final PulsarSourceReactivationV1 transition = new PulsarSourceReactivationV1(
                ROUTE_DIGEST_SEED, previous, successor);

        assertEquals(transition, PulsarSourceReactivationV1.decode(transition.canonicalBytes()));
        assertArrayEquals(ROUTE_DIGEST_SEED, transition.routeSnapshotDigest());
        assertEquals(11, transition.previousBarrier().guardedSourceConnectionGeneration());
        assertEquals(12, transition.successorBarrier().guardedSourceConnectionGeneration());

        assertThrows(IllegalArgumentException.class,
                () -> new PulsarSourceReactivationV1(ROUTE_DIGEST_SEED, previous,
                        assignment(shard, bytes(32, 72), 2, 11)));
        assertThrows(IllegalArgumentException.class,
                () -> new PulsarSourceReactivationV1(ROUTE_DIGEST_SEED, previous,
                        assignment(shard, bytes(32, 73), 2, 12,
                                new PulsarActivationBarrier(shard, bytes(32, 99),
                                        "persistent://public/default/source-partition-0", 10, 20, 0, 1,
                                        12, ATTESTATION, false))));
        assertThrows(IllegalArgumentException.class,
                () -> new PulsarSourceReactivationV1(ROUTE_DIGEST_SEED, previous,
                        assignment(shard, bytes(32, 74), 1, 12)));
    }

    @Test
    void coordinatorFencesQuiescesPublishesAndAcquiresOnlyTheSuccessor() {
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 2));
        final ShardId shard = new ShardId(incarnation, 0);
        final SourceAssignment previous = assignment(shard, bytes(32, 80), 1, 21);
        final SourceAssignment successor = assignment(shard, bytes(32, 81), 2, 22);
        final RouteSnapshotV1 route = route(incarnation, previous, 1);
        final PulsarSourceReactivationV1 transition = new PulsarSourceReactivationV1(
                route.snapshotDigest(), previous, successor);
        final WorkerAssignment previousWorker = new WorkerAssignment("worker-a", previous, 1,
                bytes(32, 90), route.snapshotDigest());
        final WorkerAssignment successorWorker = new WorkerAssignment("worker-a", successor, 2,
                bytes(32, 90), route.snapshotDigest());
        final InMemoryWorkerAssignmentAuthority assignments = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentAuthority.Publication previousPublication = assignments.publish(previousWorker, 0);
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OxiaOwnerLeaseStore owners = new OxiaOwnerLeaseStore(backend);
        final byte[] session = bytes(32, 100);
        OwnerLease lease = owners.acquire(previous, "worker-a", session, NOW, 10_000).orElseThrow();
        lease = owners.transition(lease, ShardLifecycleState.CATCHING_UP).orElseThrow();
        lease = owners.transition(lease, ShardLifecycleState.ACTIVE_FOR_COMMANDS).orElseThrow();
        final RouteSnapshotProvider routeProvider = provider(route);
        final PulsarSourceReactivationCoordinator coordinator = new PulsarSourceReactivationCoordinator(
                routeProvider, assignments, owners);

        final PulsarSourceReactivationCoordinator.FencedPlan plan = coordinator.fenceForReactivation(
                TENANT, previousPublication, lease, transition, NOW);
        assertEquals(ShardLifecycleState.FENCED, owners.current(shard).orElseThrow().state());
        assertEquals(previousPublication, assignments.current(shard).orElseThrow());

        final AtomicBoolean quiesced = new AtomicBoolean();
        final WorkerAssignmentAuthority.Publication published = coordinator.publishSuccessor(plan, successorWorker,
                () -> {
                    assertEquals(ShardLifecycleState.FENCED, owners.current(shard).orElseThrow().state());
                    assertEquals(previousPublication, assignments.current(shard).orElseThrow());
                    quiesced.set(true);
                });
        assertTrue(quiesced.get());
        assertEquals(2, published.revision());
        assertTrue(published.assignment().sameIdentity(successorWorker));
        assertTrue(owners.current(shard).isEmpty());

        final OwnerLease successorLease = coordinator.acquireSuccessor(plan, published, "worker-a",
                bytes(32, 101), NOW, 10_000);
        assertEquals(ShardLifecycleState.ACQUIRING, successorLease.state());
        assertArrayEquals(successor.assignmentId(), successorLease.sourceAssignmentId());
        assertEquals(successor.assignmentEpoch(), successorLease.sourceAssignmentEpoch());
    }

    @Test
    void coordinatorLeavesFencedOldStateWhenQuiescenceProofFails() {
        final RouteIncarnation incarnation = new RouteIncarnation(bytes(16, 3));
        final ShardId shard = new ShardId(incarnation, 0);
        final SourceAssignment previous = assignment(shard, bytes(32, 110), 1, 31);
        final SourceAssignment successor = assignment(shard, bytes(32, 111), 2, 32);
        final RouteSnapshotV1 route = route(incarnation, previous, 1);
        final PulsarSourceReactivationV1 transition = new PulsarSourceReactivationV1(
                route.snapshotDigest(), previous, successor);
        final WorkerAssignment oldWorker = new WorkerAssignment("worker-b", previous, 1,
                bytes(32, 112), route.snapshotDigest());
        final WorkerAssignment newWorker = new WorkerAssignment("worker-b", successor, 2,
                bytes(32, 112), route.snapshotDigest());
        final InMemoryWorkerAssignmentAuthority assignments = new InMemoryWorkerAssignmentAuthority();
        final WorkerAssignmentAuthority.Publication publication = assignments.publish(oldWorker, 0);
        final OxiaOwnerLeaseStore owners = new OxiaOwnerLeaseStore(new InMemoryOwnerLeaseStore());
        OwnerLease lease = owners.acquire(previous, "worker-b", bytes(32, 113), NOW, 10_000).orElseThrow();
        lease = owners.transition(lease, ShardLifecycleState.CATCHING_UP).orElseThrow();
        lease = owners.transition(lease, ShardLifecycleState.ACTIVE_FOR_COMMANDS).orElseThrow();
        final PulsarSourceReactivationCoordinator coordinator = new PulsarSourceReactivationCoordinator(
                provider(route), assignments, owners);
        final PulsarSourceReactivationCoordinator.FencedPlan plan = coordinator.fenceForReactivation(
                TENANT, publication, lease, transition, NOW);

        assertThrows(IllegalStateException.class, () -> coordinator.publishSuccessor(plan, newWorker,
                () -> {
                    throw new IllegalStateException("old callback still in flight");
                }));
        assertEquals(ShardLifecycleState.FENCED, owners.current(shard).orElseThrow().state());
        assertEquals(publication, assignments.current(shard).orElseThrow());
        assertFalse(owners.current(shard).orElseThrow().state() == ShardLifecycleState.ACTIVE_FOR_COMMANDS);
    }

    private static SourceAssignment assignment(final ShardId shard, final byte[] assignmentId,
                                               final long epoch, final long generation) {
        return assignment(shard, assignmentId, epoch, generation,
                new PulsarActivationBarrier(shard, RESOURCE,
                        "persistent://public/default/source-partition-0", 10, 20, 0, 1,
                        generation, ATTESTATION, false));
    }

    private static SourceAssignment assignment(final ShardId shard, final byte[] assignmentId,
                                               final long epoch, final long generation,
                                               final PulsarActivationBarrier barrier) {
        return new SourceAssignment(shard, assignmentId, epoch, barrier);
    }

    private static RouteSnapshotProvider provider(final RouteSnapshotV1 route) {
        return new RouteSnapshotProvider() {
            @Override
            public RouteSnapshotV1 activeForNewSchedule(final AuthenticatedTenantContext context,
                                                         final io.nereusstream.delay.semantic.RouteSelectionHint hint) {
                return route;
            }

            @Override
            public RouteSnapshotV1 exact(final RouteIncarnation incarnation,
                                          final AuthenticatedTenantContext context) {
                return route.routeIncarnation().equals(incarnation) ? route : null;
            }

            @Override
            public long publishedRevision() {
                return 1;
            }
        };
    }

    private static RouteSnapshotV1 route(final RouteIncarnation incarnation,
                                         final SourceAssignment previous,
                                         final long controlVersion) {
        try {
            final KeyPair keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final PulsarActivationBarrier barrier = (PulsarActivationBarrier) previous.activationBarrier();
            final BrokerResourceIdentityV1 resource = BrokerResourceIdentityV1.pulsar(
                    new PulsarBrokerResourceIdentityV1("standalone", RESOURCE,
                            barrier.physicalTopic(), 7));
            final PulsarIngressRouteResourceV1 ingress = new PulsarIngressRouteResourceV1(
                    "standalone", "persistent://public/default/source", List.of(
                    new PulsarPhysicalPartitionIdentityV1(0, barrier.physicalTopic(), RESOURCE, 7)));
            final long now = 1_000;
            final ActivationBarrierV1 routeBarrier = ActivationBarrierV1.pulsar(resource, 0,
                    barrier.ledgerId(), barrier.entryId(), barrier.normalizedLastBatchIndex(), barrier.batchSize(),
                    barrier.guardedSourceConnectionGeneration(), barrier.resourceGuardAttestationDigest());
            return RouteSnapshotV1.create(incarnation, TENANT.authenticatedTenantScopeHash(),
                    TENANT.tenantRoutingScope(), RouteLifecycleV1.ACTIVE_FOR_NEW, now + 500, ingress,
                    RoutingHashVersionV1.ROUTING_HASH_V1,
                    new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1), controlVersion,
                    List.of(new RoutePartitionPolicyV1(0, routeBarrier, zeroQuota(),
                            barrier.guardedSourceConnectionGeneration(), barrier.resourceGuardAttestationDigest())),
                    100, 200, 1_024, 2_048, 10, 4_096, 100, 900, 3_000,
                    new IngressCredentialBindingRefV1(bytes(32, 120), 1, bytes(32, 121), bytes(32, 122),
                            bytes(32, 123)), bytes(32, 124),
                    new TrustedUtcIntervalEvidence(900, 901,
                            TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, bytes(8, 125),
                            1, 2, 3, bytes(32, 126), 0, null), 1, keys.getPrivate());
        } catch (Exception failure) {
            throw new IllegalStateException("cannot create Pulsar reactivation Route fixture", failure);
        }
    }

    private static QuotaGrantRefV1 zeroQuota() {
        return new QuotaGrantRefV1(bytes(32, 127), 1, new PublishAdmissionBody.ChargeVector(
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
