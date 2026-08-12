package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActiveLaneStateV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ClaimMaterializationV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LaneCircuitStateV1;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.PayloadForPublishV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTestFixtures;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishAdmissionBodyTest;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.GenerationRuntimeIndex;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
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
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClaimHandoffWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void claimHandoffRequeuesRejectionAndDeferralThenRetainsSuccessfulPermit() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 28);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("claim-work-assignment")), 9,
                new KafkaActivationBarrier(shardId, "claim-work-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "claim-work-owner",
                Bytes.sha256(Bytes.utf8("claim-work-session")), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "claim-work-destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "claim-work-capability");
        final byte[] laneTuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final DestinationLaneId laneId = DestinationLaneId.derive(laneTuple);
        final byte[] payload = Bytes.utf8("claim-work-payload");
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new ScheduleIntent(laneId, 2_000, 9_000, OrderingMode.BEST_EFFORT, payload), 10_000);
        final KafkaSourcePosition source = new KafkaSourcePosition(shardId, "claim-work-cluster", topic,
                0, null, 1_000);
        final TrustedUtcIntervalEvidence evidence = evidence();
        final SchedulerBudget budget = new SchedulerBudget(1, 4_096, 1_000_000_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-work-store"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            shard.apply(schedule, source);
            io.nereusstream.delay.runtime.DelayShardTestSupport.updateLaneReadiness(
                    shard, laneId, RuntimeReadiness.READY);
            final OwnedDelayShard owned = new OwnedDelayShard(shard, lease);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.recordCatchup(source);
            owned.activateForCommands(authority, 101);
            final MessageRecord message = shard.getMessage(schedule.delayMessageId());
            final LaneRecord lane = shard.getLane(laneId);
            final byte[] readyKey = KeyCodec.timelineReady(lane.nextEligibleAtEpochMs(), laneId,
                    lane.laneVersion());
            final PersistentLaneScheduler scheduler = PersistentLaneScheduler.defaults(store);
            final byte[] certificate = bindReadyCertificate(PublishAdmissionBody.decode(
                    PublishAdmissionBodyTest.Fixture.createForSourceWithLane(shardId,
                            schedule.delayMessageId(), lane.laneIncarnation(),
                            message.runtimeIndex().timeline().encodedTimelineKey(),
                            message.runtimeIndex().timeline().workKind().wireValue(),
                            message.runtimeIndex().admissionsUsed(),
                            message.runtimeIndex().uncertainRetryAdmissionsUsed(),
                            GenerationRuntimeIndex.obligationSetDigest(
                                    message.runtimeIndex().attemptObligations()),
                            message.runtimeIndex().timeline().semanticWorkDigest(), laneId.bytes()).body())
                    .readyCertificate().canonicalBytes(), scheduler.ownerIdentity(),
                    store.metadata().storeIncarnation());
            final ActiveLaneStateV1 activeLane = new ActiveLaneStateV1(laneId, lane.laneIncarnation(),
                    io.nereusstream.delay.runtime.AdmissionGate.OPEN, RuntimeReadiness.READY, null,
                    lane.laneControlVersion(), lane.laneVersion(), destination, capability, laneTuple, lane.weight(),
                    zeroCharge(), message.runtimeIndex().timeline().actionAtEpochMs(),
                    lane.nextEligibleAtEpochMs(), LaneCircuitStateV1.CLOSED, 0, 0, 0, 0,
                    readyKey, certificate, null);
            store.write(batch -> batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(laneId),
                    LaneRecordEnvelopeV1.active(activeLane).canonicalBytes()));
            scheduler.register(lane);
            scheduler.discoverReady(evidence, budget);

            final ClaimExecutionAdmission permits = new ClaimExecutionAdmission(1, payload.length);
            permits.registerShard(new ClaimExecutionAdmission.ShardSpec(shardId, 1, payload.length));
            permits.registerLane(new ClaimExecutionAdmission.LaneSpec(shardId, laneId,
                    lane.laneIncarnation(), 0, 0, 1, payload.length));
            permits.openReady(shardId, laneId, lane.laneIncarnation());
            final AtomicReference<ClaimHandoffWorkClassExecutor.PrerequisiteDecision> prerequisite =
                    new AtomicReference<>(ClaimHandoffWorkClassExecutor.PrerequisiteDecision.unavailable(
                            ClaimHandoffWorkClassExecutor.PrerequisiteRejection.CHANNEL_OR_CREDENTIAL_UNAVAILABLE));
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final ClaimHandoffWorkClassExecutor executor = new ClaimHandoffWorkClassExecutor(workClasses,
                    owned, authority, scheduler, permits, ignored -> prerequisite.get());
            final ClaimMaterializationV1 materialization = materialization(destination, capability,
                    schedule.delayMessageId(), message, payload);
            final byte[] claimCharge = claimCharge(payload.length);

            ScheduleWorkItem selected = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            workClasses.submit(new WorkClassTask(WorkClass.DUE_SCHEDULER, "occupied", 1), () -> {
            });
            final ScheduleWorkItem queueRejected = selected;
            assertThrows(IllegalStateException.class, () -> executor.submit(queueRejected, evidence,
                    3_000, materialization, claimCharge, () -> 101));
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            selected = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            final ClaimHandoffWorkClassExecutor.Submission prerequisiteDeferred = executor.submit(selected,
                    evidence, 3_000, materialization, claimCharge, () -> 101);
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(ClaimHandoffWorkClassExecutor.ResultKind.PREREQUISITE_UNAVAILABLE,
                    prerequisiteDeferred.result().orElseThrow().kind());
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            assertEquals(MessageStatus.SCHEDULED, shard.getMessage(schedule.delayMessageId()).status());

            prerequisite.set(ClaimHandoffWorkClassExecutor.PrerequisiteDecision.available());
            final ClaimExecutionAdmission.Reservation occupied = permits.tryAcquire(shardId, laneId,
                    lane.laneIncarnation(), io.nereusstream.delay.protocol.DelayMessageId.random(shardId),
                    1, payload.length).reservation();
            selected = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            final ClaimHandoffWorkClassExecutor.Submission permitDeferred = executor.submit(selected,
                    evidence, 3_000, materialization, claimCharge, () -> 101);
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(ClaimHandoffWorkClassExecutor.ResultKind.PERMIT_UNAVAILABLE,
                    permitDeferred.result().orElseThrow().kind());
            assertEquals(ClaimExecutionAdmission.Rejection.LANE_CAPACITY,
                    permitDeferred.result().orElseThrow().permitRejection());
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            occupied.release();

            selected = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            final ScheduleWorkItem claimedItem = selected;
            final ClaimHandoffWorkClassExecutor.Submission claimed = executor.submit(claimedItem, evidence,
                    3_000, materialization, claimCharge, () -> 101);
            assertTrue(claimed.result().isEmpty());
            assertEquals(List.of(claimed.task()),
                    workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000)));
            final ClaimHandoffWorkClassExecutor.ClaimHandoffResult result = claimed.result().orElseThrow();
            assertEquals(ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED, result.kind());
            assertEquals(schedule.delayMessageId(), result.claim().delayMessageId());
            assertEquals(ClaimExecutionAdmission.ReservationState.ACTIVE, result.reservation().state());
            assertEquals(MessageStatus.CLAIMED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(0, scheduler.snapshot().lanes().get(0).pendingItems());
            assertThrows(IllegalArgumentException.class, () -> scheduler.requeueFailedClaim(claimedItem));
            assertEquals(0, workClasses.registeredActions());
            assertTrue(result.reservation().release());
            assertFalse(result.reservation().release());
        }
    }

    private static ClaimMaterializationV1 materialization(final ProfileRefV1 destination,
                                                           final ProfileRefV1 capability,
                                                           final io.nereusstream.delay.protocol.DelayMessageId id,
                                                           final MessageRecord message,
                                                           final byte[] payload) {
        return new ClaimMaterializationV1(destination, capability,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("target-cluster",
                        UUID.nameUUIDFromBytes(Bytes.utf8("claim-target-topic")))), 0, id,
                Integer.toUnsignedLong(message.generation()), PayloadForPublishV1.inline(payload),
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                message.deliverAtEpochMs(), message.expireAtEpochMs(),
                message.runtimeIndex().timeline().actionAtEpochMs());
    }

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("claim-work-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("claim-work-time-proof")), 0, null);
    }

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final String value) {
        return new ProfileRefV1(Bytes.utf8(value), 1, Bytes.sha256(Bytes.utf8(value + "-semantic")), kind);
    }

    private static PublishAdmissionBody.ChargeVector zeroCharge() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes, 0,
                0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes();
    }

    private static byte[] bindReadyCertificate(final byte[] encoded, final OwnerIdentityV1 owner,
                                               final byte[] storeIncarnation) {
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == 16) {
                    break;
                }
                if (field.number() == 2) {
                    CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
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
