package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActiveLaneStateV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.LaneCircuitStateV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTestFixtures;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishAdmissionBodyTest;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.ReadyIndexValue;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.runtime.TimelineEntry;
import io.nereusstream.delay.scheduler.LaneScheduler;
import io.nereusstream.delay.scheduler.PersistentLaneScheduler;
import io.nereusstream.delay.scheduler.ScheduleWorkItem;
import io.nereusstream.delay.scheduler.SchedulerBudget;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import io.nereusstream.delay.scheduler.WorkClassTask;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DueSchedulerWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void readyDiscoveryUsesBoundedQueueAndExecutionTimeOwnerFence() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 27);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shard,
                Bytes.sha256(Bytes.utf8("due-work-assignment")), 8,
                new KafkaActivationBarrier(shard, "due-work-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "due-work-owner",
                Bytes.sha256(Bytes.utf8("due-work-session")), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "due-work-cluster", topic,
                0, null, 1_000);
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("due-work-destination"), 1,
                Bytes.sha256(Bytes.utf8("due-work-destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("due-work-capability"), 1,
                Bytes.sha256(Bytes.utf8("due-work-capability-semantic")),
                ProfileKindV1.DELIVERY_CAPABILITY);
        final byte[] laneTuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final DestinationLaneId laneId = DestinationLaneId.derive(laneTuple);
        final LaneRecord lane = new LaneRecord(laneId, incarnation(), 1, 1,
                io.nereusstream.delay.runtime.AdmissionGate.OPEN, RuntimeReadiness.READY, 1, 2_000);
        final io.nereusstream.delay.protocol.DelayMessageId messageId =
                io.nereusstream.delay.protocol.DelayMessageId.random(shard);
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 1, 1,
                2_000, 9_000, laneId, OrderingMode.BEST_EFFORT, Bytes.utf8("due-work-payload"),
                source.canonicalBytes());
        final byte[] timelineKey = KeyCodec.timelineDue(laneId, 2_000, source.sourceOrderToken(),
                messageId, message.generation());
        final byte[] readyKey = KeyCodec.timelineReady(2_000, laneId, lane.laneVersion());
        final ReadyIndexValue ready = new ReadyIndexValue(laneId, 2_000, lane.laneVersion(),
                messageId, message.generation(), Bytes.sha256(timelineKey));
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("due-work-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("due-work-time-proof")), 0, null);
        final SchedulerBudget budget = new SchedulerBudget(1, 4_096, 1_000_000_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("due-work-store"));
        final ShardStoreConfig foreignConfig = ShardStoreConfig.defaults(
                tempDir.resolve("due-work-foreign-store"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             SharedRocksDbResources foreignResources = new SharedRocksDbResources(foreignConfig);
             ShardStore store = ShardStore.open(config, shard, resources);
             ShardStore foreignStore = ShardStore.open(foreignConfig, shard, foreignResources)) {
            final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("due-work-deployment"),
                    Bytes.utf8("due-work-worker"), lease.ownerEpoch(),
                    Bytes.sha256(Bytes.utf8("due-work-owner-fence")));
            final OwnedDelayShard owned = new OwnedDelayShard(
                    new DelayShard(store, DelayShardConfig.defaults()), lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.recordCatchup(source);
            owned.activateForCommands(authority, 101);
            final PersistentLaneScheduler scheduler = new PersistentLaneScheduler(
                    store, LaneScheduler.defaults(), owner);
            final byte[] certificate = bindReadyCertificate(PublishAdmissionBody.decode(
                    PublishAdmissionBodyTest.Fixture.createForSourceWithLane(shard, messageId, incarnation(),
                            timelineKey, 1, 0, 0, Bytes.sha256(Bytes.utf8("due-work-obligations")),
                            Bytes.sha256(Bytes.utf8("due-work-semantic")), laneId.bytes()).body())
                    .readyCertificate().canonicalBytes(), scheduler.ownerIdentity(),
                    store.metadata().storeIncarnation());
            final ActiveLaneStateV1 activeLane = new ActiveLaneStateV1(laneId, incarnation(),
                    io.nereusstream.delay.runtime.AdmissionGate.OPEN, RuntimeReadiness.READY, null,
                    lane.laneControlVersion(), lane.laneVersion(), destination, capability, laneTuple, lane.weight(),
                    zeroCharge(), 2_000L, 2_000L, LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                    readyKey, certificate, null);
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(laneId),
                        LaneRecordEnvelopeV1.active(activeLane).canonicalBytes());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), message.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey,
                        new TimelineEntry(messageId, message.generation()).encode());
                batch.putValue(ColumnFamily.TIMELINE, 3, readyKey, ready.encode());
            });
            io.nereusstream.delay.scheduler.PersistentLaneSchedulerTestSupport.register(scheduler, lane);
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final DueSchedulerWorkClassExecutor executor = new DueSchedulerWorkClassExecutor(
                    workClasses, owned, authority, scheduler);
            final WorkerSchedulingRuntime schedulingRuntime = new WorkerSchedulingRuntime(
                    workClasses, owned, authority, scheduler);
            final WorkerShardRuntime workerRuntime = new WorkerShardRuntime(
                    () -> java.util.Optional.empty(), workClasses, owned, store, resources, authority,
                    KeyPairGenerator.getInstance("Ed25519").generateKeyPair().getPublic(), schedulingRuntime);

            workClasses.submit(new WorkClassTask(WorkClass.DUE_SCHEDULER, "occupied", 1), () -> {
            });
            assertThrows(IllegalStateException.class, () -> executor.submit(evidence, budget, () -> 101));
            assertEquals(0, scheduler.snapshot().lanes().get(0).pendingItems());
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            final TrustedUtcIntervalEvidence certificateBoundary = new TrustedUtcIntervalEvidence(
                    8_000, 8_000, TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                    Bytes.utf8("due-work-expired-clock"), 1, 2, 2,
                    Bytes.sha256(Bytes.utf8("due-work-expired-proof")), 0, null);
            assertThrows(IllegalStateException.class,
                    () -> scheduler.discoverReady(certificateBoundary, budget));
            assertEquals(0, scheduler.snapshot().lanes().get(0).pendingItems());

            final byte[] otherWorker = Bytes.utf8("due-work-other-owner");
            final OwnerIdentityV1 otherOwner = new OwnerIdentityV1(Bytes.utf8("embedded-scheduler"),
                    otherWorker, lease.ownerEpoch(), Bytes.sha256(Bytes.utf8("due-work-other-owner-fence")));
            final PersistentLaneScheduler otherOwnerScheduler = new PersistentLaneScheduler(
                    store, LaneScheduler.defaults(), otherOwner);
            io.nereusstream.delay.scheduler.PersistentLaneSchedulerTestSupport.register(otherOwnerScheduler, lane);
            assertThrows(IllegalStateException.class,
                    () -> otherOwnerScheduler.discoverReady(evidence, budget));
            assertEquals(0, otherOwnerScheduler.snapshot().lanes().get(0).pendingItems());
            final DueSchedulerWorkClassExecutor wrongOwnerExecutor = new DueSchedulerWorkClassExecutor(
                    workClasses, owned, authority, otherOwnerScheduler);
            assertThrows(IllegalArgumentException.class,
                    () -> wrongOwnerExecutor.submit(evidence, budget, () -> 101));

            final PersistentLaneScheduler foreignStoreScheduler = new PersistentLaneScheduler(
                    foreignStore, LaneScheduler.defaults(), owner);
            final DueSchedulerWorkClassExecutor foreignStoreExecutor = new DueSchedulerWorkClassExecutor(
                    workClasses, owned, authority, foreignStoreScheduler);
            final long foreignSequence = foreignStore.latestSequenceNumber();
            assertThrows(IllegalArgumentException.class,
                    () -> foreignStoreExecutor.submit(evidence, budget, () -> 101));
            assertEquals(foreignSequence, foreignStore.latestSequenceNumber());
            assertEquals(0, workClasses.registeredActions());

            final WorkerSchedulingRuntime.DueTurn dueTurn = workerRuntime.runDueTurn(evidence, budget, () -> 101);
            assertEquals(16 + 4 + 4 + 8 + 8 + 4 + evidence.canonicalBytes().length + budget.maxBytes(),
                    dueTurn.task().bytes());
            assertEquals(List.of(dueTurn.task()), dueTurn.completedTasks());
            assertEquals(List.of(messageId), dueTurn.discoveredItems().stream()
                    .map(ScheduleWorkItem::messageId).toList());
            assertEquals(0, workClasses.registeredActions());
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());

            final ScheduleWorkItem selected = workerRuntime.pollReady(evidence, budget, () -> 101).get(0);
            scheduler.requeueFailedClaim(selected);
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            final ScheduleWorkItem retried = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            assertEquals(selected, retried);
            assertThrows(IllegalStateException.class, () -> scheduler.completeClaim(retried));
            store.write(batch -> batch.delete(ColumnFamily.TIMELINE, readyKey));
            scheduler.completeClaim(retried);
            assertThrows(IllegalArgumentException.class, () -> scheduler.requeueFailedClaim(retried));

            final DueSchedulerWorkClassExecutor.Submission expired =
                    executor.submit(evidence, budget, () -> 200);
            final IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000)));
            assertEquals("shard owner lease is not active", failure.getMessage());
            assertEquals(ShardLifecycleState.FENCED, owned.state());
            assertTrue(expired.discovered().isEmpty());
            assertEquals(WorkClassExecutionRegistry.ExecutionState.FAILED,
                    workClasses.state(expired.task()).orElseThrow());
            assertEquals(1, workClasses.registeredActions());
        }
    }

    private static byte[] incarnation() {
        final byte[] bytes = new byte[16];
        bytes[15] = 1;
        return bytes;
    }

    private static PublishAdmissionBody.ChargeVector zeroCharge() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static byte[] bindReadyCertificate(final byte[] encoded, final OwnerIdentityV1 owner,
                                               final byte[] storeIncarnation) {
        final byte[] ownerAuthor = owner.canonicalBytes();
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 16) {
                    break;
                }
                if (field.number() == 2) {
                    CanonicalProtobuf.bytes(output, 2, ownerAuthor);
                } else if (field.number() == 3) {
                    CanonicalProtobuf.bytes(output, 3, storeIncarnation);
                } else {
                    writeField(output, field);
                }
            }
        });
        return CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(prefix);
            while (reader.hasRemaining()) {
                writeField(output, reader.next());
            }
            CanonicalProtobuf.bytes(output, 16,
                    Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"), prefix));
        });
    }

    private static void writeField(final ByteArrayOutputStream output,
                                   final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, maxQueueRecords, 1_000_000,
                    maxQueueRecords, 1_000_000, 1_000,
                    protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), new AtomicLong()::get);
    }
}
