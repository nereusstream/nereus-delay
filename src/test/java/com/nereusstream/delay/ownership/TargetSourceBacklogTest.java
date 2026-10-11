package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ActivationBarrier;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.IngressCredentialBindingRef;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.MessagePrecondition;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteLifecycle;
import com.nereusstream.delay.protocol.RoutePartitionPolicy;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.RoutingHashVersion;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.submission.SubmissionPlanException;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TargetSourceBacklogTest {
    private final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
    private final UUID topic = UUID.randomUUID();

    @Test
    void countsActualFramesExcludesAnchorAndUsesOldestTimestampRatherThanOffsetDistance() {
        final var anchor = source(10, 100);
        final var cut = new TargetSourceBacklog.Cut(anchor, () -> {});
        final var collector = collector(cut, new AtomicLong(0));
        collector.add(anchor, 8);
        collector.add(source(50, 102), 16);
        collector.add(source(90, 101), 32);
        final var snapshot = collector.finish(source(90, 101));
        assertEquals(2, snapshot.records());
        assertEquals(48, snapshot.bytes());
        assertEquals(101L, snapshot.oldestBrokerPersistenceTimeEpochMs());
        assertThrows(IllegalStateException.class, () -> collector.add(source(91, 103), 1));
        final var empty = collector(cut, new AtomicLong(0));
        empty.add(anchor, 8);
        assertEquals(0, empty.finish(anchor).records());
    }

    @Test
    void incompleteBudgetExpiredChangedAndForeignPrefixesCannotProduceCompleteObservation() {
        final var anchor = source(10, 100);
        final var active = new AtomicBoolean(true);
        final var cut = new TargetSourceBacklog.Cut(anchor, () -> {
            if (!active.get()) { throw new IllegalStateException("fixture cut changed"); }
        });
        final var time = new AtomicLong(0);
        final var collector = collector(cut, time);
        assertThrows(IllegalStateException.class, () -> collector.add(source(11, 100), 8));
        collector.add(anchor, 8);
        assertThrows(IllegalStateException.class, () -> collector.finish(source(11, 101)));
        assertThrows(IllegalStateException.class, () -> collector.add(source(11, 101), 129));
        assertThrows(IllegalArgumentException.class, () -> collector.add(new KafkaSourcePosition(shard,
                "other-cluster", topic, 11, 1, 101), 8));
        active.set(false);
        assertThrows(IllegalStateException.class, collector::requireCurrent);
        active.set(true);
        time.set(1000);
        assertThrows(IllegalStateException.class, collector::remainingNanos);
        final var backward = new AtomicLong(10);
        final var another = collector(cut, backward);
        backward.set(9);
        assertThrows(IllegalStateException.class, another::remainingNanos);
        final var limited = new TargetSourceBacklog.Collector(cut,
                new TargetSourceBacklog.Limits(2, 128, 128, 1000), () -> 0, () -> {});
        limited.add(anchor, 8);
        limited.add(source(11, 101), 16);
        assertThrows(IllegalStateException.class, () -> limited.add(source(12, 102), 16));
    }

    @Test
    void growthRejectsFullStaleWrongResourceOldAgeAndProtectionFailureWhileCancelKeepsControlPath() throws Exception {
        final var cut = new TargetSourceBacklog.Cut(source(10, 100), () -> {});
        final var collector = collector(cut, new AtomicLong(0));
        collector.add(cut.applied(), 8);
        collector.add(source(11, 101), 32);
        final var snapshot = collector.finish(source(11, 101));
        final var command = PreparedCommand.reschedule(shard, DelayMessageId.random(shard),
                new MessagePrecondition(null, null), 200, 1000, 1000);
        final var route = route(topic);
        final var time = new AtomicLong(0);
        final var gate = new TargetSourceBacklogAdmission(new TargetSourceBacklogAdmission.Limits(2, 64, 100, 1000),
                ignored -> snapshot, () -> 150, time::get, (actual, bytes) -> {});
        gate.requireAllowed(command, route, 32);
        assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED,
                assertThrows(SubmissionPlanException.class, () -> gate.requireAllowed(command, route, 33)).code());
        assertThrows(SubmissionPlanException.class, () -> gate.requireAllowed(command, route(UUID.randomUUID()), 1));
        time.set(1000);
        assertThrows(SubmissionPlanException.class, () -> gate.requireAllowed(command, route, 1));
        final var oldest = new TargetSourceBacklogAdmission(new TargetSourceBacklogAdmission.Limits(2, 64, 49, 1000),
                ignored -> snapshot, () -> 150, () -> 0, (actual, bytes) -> {});
        assertThrows(SubmissionPlanException.class, () -> oldest.requireAllowed(command, route, 1));
        final var denied = new TargetSourceBacklogAdmission(new TargetSourceBacklogAdmission.Limits(2, 64, 100, 1000),
                ignored -> snapshot, () -> 150, () -> 0,
                (actual, bytes) -> { throw new IllegalStateException("retention fixture denied"); });
        assertThrows(SubmissionPlanException.class, () -> denied.requireAllowed(command, route, 1));
        final var cancel = PreparedCommand.cancel(shard, CommandId.random(shard), command.delayMessageId(),
                new MessagePrecondition(null, null), 1000);
        denied.requireAllowed(cancel, route, 1);
    }

    private TargetSourceBacklog.Collector collector(TargetSourceBacklog.Cut cut, AtomicLong time) {
        return new TargetSourceBacklog.Collector(cut, new TargetSourceBacklog.Limits(8, 1024, 128, 1000),
                time::get, () -> {});
    }

    @Test
    void actualRouteResolverRejectsGrowthBeforeTransportLookupOrLibraryOwnership() throws Exception {
        final var cut = new TargetSourceBacklog.Cut(source(10, 100), () -> {});
        final var collector = collector(cut, new AtomicLong(0));
        collector.add(cut.applied(), 8);
        collector.add(source(11, 101), 32);
        final var snapshot = collector.finish(source(11, 101));
        final var route = route(topic);
        final var gate = new TargetSourceBacklogAdmission(new TargetSourceBacklogAdmission.Limits(1, 64, 100, 1000),
                ignored -> snapshot, () -> 150, () -> 0, (actual, bytes) -> {});
        final var routes = new com.nereusstream.delay.route.RouteSnapshotProvider() {
            @Override
            public RouteSnapshot activeForNewSchedule(
                    com.nereusstream.delay.semantic.AuthenticatedTenantContext tenant,
                    com.nereusstream.delay.semantic.RouteSelectionHint hint) {
                throw new AssertionError("submit cannot select a new route");
            }

            @Override
            public RouteSnapshot exact(RouteIncarnation incarnation,
                    com.nereusstream.delay.semantic.AuthenticatedTenantContext tenant) {
                return route;
            }

            @Override
            public long publishedRevision() { return 1; }
        };
        final var resolver = new com.nereusstream.delay.submission.RouteBoundSubmissionTransportPlanResolver(
                routes, () -> 150, null, gate);
        final var coordinator = new com.nereusstream.delay.submission.DefaultSubmissionCoordinator(resolver,
                key -> { throw new AssertionError("backpressured growth reached transport lookup"); },
                key -> { throw new AssertionError("backpressured growth reached outcome projector lookup"); });
        final var command = PreparedCommand.reschedule(shard, DelayMessageId.random(shard),
                new MessagePrecondition(null, null), 200, 1000, 1000);
        final var prepared = com.nereusstream.delay.protocol.PreparedSubmission.managed(
                com.nereusstream.delay.protocol.CommandCodec.encodeManagedFrame(command));
        final var permit = new com.nereusstream.delay.transport.LocalTransportOwnershipPermit(
                com.nereusstream.delay.transport.PhysicalEnqueueAttemptId.random());
        final var result = coordinator.submit(new com.nereusstream.delay.semantic.AuthenticatedTenantContext(
                        bytes(2), bytes(3), bytes(12)), prepared, permit).toCompletableFuture().get();
        assertEquals(com.nereusstream.delay.protocol.EnqueueOutcomeKind.DEFINITELY_NOT_QUEUED, result.managed().kind());
        assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED,
                result.managed().definitelyNotQueued().error().code());
        assertEquals(com.nereusstream.delay.transport.TransportOwnershipState.INVALID, permit.state());
    }

    private KafkaSourcePosition source(long offset, long timestamp) {
        return new KafkaSourcePosition(shard, "cluster", topic, offset, 1, timestamp);
    }

    private RouteSnapshot route(UUID nativeTopic) throws Exception {
        final var broker = BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity("cluster", nativeTopic));
        final var quota = new QuotaGrantRef(bytes(1), 1,
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        return RouteSnapshot.create(shard.routeIncarnation(), bytes(2), bytes(3), RouteLifecycle.ACTIVE_FOR_NEW,
                900, new KafkaIngressRouteResource("cluster", "source", nativeTopic, 1),
                RoutingHashVersion.ROUTING_HASH, new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 1), 1,
                List.of(new RoutePartitionPolicy(0, ActivationBarrier.kafka(broker, 0, 0, 0), quota, 1, bytes(4))),
                100, 200, 1024, 4096, 10, 8192, 500, 100, 1000,
                new IngressCredentialBindingRef(bytes(5), 1, bytes(6), bytes(7), bytes(8)), bytes(9),
                new TrustedUtcIntervalEvidence(200, 201, TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                        Arrays.copyOf(bytes(10), 8), 1, 2, 3, bytes(11), 0, null),
                1, KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPrivate());
    }

    private static byte[] bytes(int seed) {
        final var bytes = new byte[32];
        Arrays.fill(bytes, (byte) seed);
        return bytes;
    }
}
