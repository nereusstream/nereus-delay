package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.ActiveLaneState;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.ChannelResourceIdentity;
import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.LaneCircuitState;
import com.nereusstream.delay.protocol.LaneRecordEnvelope;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.PublishAdmissionBodyTest;
import com.nereusstream.delay.protocol.ReadyCertificate;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.DelayShard;
import com.nereusstream.delay.runtime.DelayShardConfig;
import com.nereusstream.delay.runtime.DelayShardTestSupport;
import com.nereusstream.delay.runtime.GenerationRuntimeIndex;
import com.nereusstream.delay.runtime.LaneRecord;
import com.nereusstream.delay.runtime.MessageRecord;
import com.nereusstream.delay.runtime.MessageStatus;
import com.nereusstream.delay.runtime.RuntimeReadiness;
import com.nereusstream.delay.runtime.ScheduleResolver;
import com.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import com.nereusstream.delay.scheduler.LaneScheduler;
import com.nereusstream.delay.scheduler.PersistentLaneScheduler;
import com.nereusstream.delay.scheduler.ScheduleWorkItem;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.scheduler.WorkClassPolicy;
import com.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import com.nereusstream.delay.scheduler.WorkClassTask;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClaimHandoffWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void claimHandoffRequeuesRejectionAndDeferralThenRetainsSuccessfulPermit() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 28);
        final UUID topic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(
                shardId,
                Bytes.sha256(Bytes.utf8("claim-work-assignment")),
                9,
                new KafkaActivationBarrier(shardId, "claim-work-cluster", topic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(
                        assignment, "claim-work-owner", Bytes.sha256(Bytes.utf8("claim-work-session")), 100, 100)
                .orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ProfileRef destination = profile(ProfileKind.DESTINATION, "destination");
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "capability");
        final byte[] laneTuple = canonicalClaimKafkaLaneTuple(destination, capability);
        final DestinationLaneId laneId = DestinationLaneId.derive(laneTuple);
        final byte[] payload = Bytes.utf8("claim-work-payload");
        final CanonicalScheduleIntent scheduleIntent = CanonicalScheduleIntent.create(
                destination,
                new com.nereusstream.delay.protocol.RetryPolicyRef(
                        Bytes.utf8("claim-work-retry"), 1, Bytes.sha256(Bytes.utf8("claim-work-retry-semantic"))),
                2_000,
                9_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("claim-work-ordering"),
                payload,
                null,
                AdapterMetadata.kafka(new com.nereusstream.delay.protocol.KafkaMetadata(null, List.of())),
                null,
                null);
        final PreparedCommand schedule = PreparedCommand.schedule(shardId, scheduleIntent, 10_000);
        final ScheduleResolver resolver = new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId messageId,
                    final CanonicalScheduleIntent intent,
                    final SourcePosition source) {
                return new ResolvedSchedule(laneId, laneTuple, payload, null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId shard,
                    final com.nereusstream.delay.protocol.DelayMessageId messageId,
                    final com.nereusstream.delay.protocol.PrepareLargeScheduleBody body,
                    final SourcePosition source) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };
        final KafkaSourcePosition source =
                new KafkaSourcePosition(shardId, "claim-work-cluster", topic, 0, null, 1_000);
        final TrustedUtcIntervalEvidence evidence = evidence();
        final SchedulerBudget budget = new SchedulerBudget(1, 4_096, 1_000_000_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-work-store"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnerIdentity owner = new OwnerIdentity(
                    Bytes.utf8("claim-work-deployment"),
                    Bytes.utf8("claim-work-worker"),
                    lease.ownerEpoch(),
                    Bytes.sha256(Bytes.utf8("claim-work-owner-fence")));
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            shard.apply(schedule, source);
            final OwnedDelayShard owned = new OwnedDelayShard(shard, lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.recordCatchup(source);
            owned.activateForCommands(authority, 101);
            DelayShardTestSupport.activateTypedLaneReadinessForTest(shard, laneId);
            final MessageRecord message = shard.getMessage(schedule.delayMessageId());
            final LaneRecord lane = shard.getLane(laneId);
            final byte[] readyKey = KeyCodec.timelineReady(lane.nextEligibleAtEpochMs(), laneId, lane.laneVersion());
            final PersistentLaneScheduler scheduler =
                    new PersistentLaneScheduler(store, LaneScheduler.defaults(), owner);
            final byte[] certificate = bindReadyCertificate(
                    PublishAdmissionBody.decode(PublishAdmissionBodyTest.Fixture.createForSourceWithLane(
                                            shardId,
                                            schedule.delayMessageId(),
                                            lane.laneIncarnation(),
                                            message.runtimeIndex().timeline().encodedTimelineKey(),
                                            message.runtimeIndex()
                                                    .timeline()
                                                    .workKind()
                                                    .wireValue(),
                                            message.runtimeIndex().admissionsUsed(),
                                            message.runtimeIndex().uncertainRetryAdmissionsUsed(),
                                            GenerationRuntimeIndex.obligationSetDigest(
                                                    message.runtimeIndex().attemptObligations()),
                                            message.runtimeIndex().timeline().semanticWorkDigest(),
                                            laneId.bytes())
                                    .body())
                            .readyCertificate()
                            .canonicalBytes(),
                    scheduler.ownerIdentity(),
                    store.metadata().storeIncarnation());
            final ActiveLaneState activeLane = new ActiveLaneState(
                    laneId,
                    lane.laneIncarnation(),
                    com.nereusstream.delay.runtime.AdmissionGate.OPEN,
                    RuntimeReadiness.READY,
                    null,
                    lane.laneControlVersion(),
                    lane.laneVersion(),
                    destination,
                    capability,
                    laneTuple,
                    lane.weight(),
                    zeroCharge(),
                    message.runtimeIndex().timeline().actionAtEpochMs(),
                    lane.nextEligibleAtEpochMs(),
                    LaneCircuitState.CLOSED,
                    0,
                    0,
                    0,
                    0,
                    readyKey,
                    certificate,
                    null);
            store.write(batch -> batch.putValue(
                    ColumnFamily.META,
                    2,
                    KeyCodec.metaLane(laneId),
                    LaneRecordEnvelope.active(activeLane).canonicalBytes()));
            com.nereusstream.delay.scheduler.PersistentLaneSchedulerTestSupport.register(scheduler, lane);
            scheduler.discoverReady(evidence, budget);

            final ClaimExecutionAdmission permits = new ClaimExecutionAdmission(1, payload.length);
            permits.registerShard(new ClaimExecutionAdmission.ShardSpec(shardId, 1, payload.length));
            permits.registerLane(new ClaimExecutionAdmission.LaneSpec(
                    shardId, laneId, lane.laneIncarnation(), 0, 0, 1, payload.length));
            permits.openReady(shardId, laneId, lane.laneIncarnation());
            final AtomicReference<ClaimHandoffWorkClassExecutor.PrerequisiteDecision> prerequisite =
                    new AtomicReference<>(ClaimHandoffWorkClassExecutor.PrerequisiteDecision.unavailable(
                            ClaimHandoffWorkClassExecutor.PrerequisiteRejection.CHANNEL_OR_CREDENTIAL_UNAVAILABLE));
            final WorkClassExecutionRegistry workClasses = workClasses(1);
            final ClaimHandoffWorkClassExecutor executor = new ClaimHandoffWorkClassExecutor(
                    workClasses, owned, authority, scheduler, permits, ignored -> prerequisite.get());
            final PublishAdmissionWorkClassExecutor publishExecutor = new PublishAdmissionWorkClassExecutor(
                    workClasses,
                    owned,
                    authority,
                    permits,
                    ignored -> ShardLogMutationAppender.AppendOutcome.unknown(),
                    ignored -> PublishAdmissionWorkClassExecutor.PrerequisiteDecision.available());
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new WorkerCommandRuntime(workClasses(1), resources, executor, publishExecutor));
            final WorkerCommandRuntime commandRuntime =
                    new WorkerCommandRuntime(workClasses, resources, executor, publishExecutor);
            final WorkerSchedulingRuntime schedulingRuntime =
                    new WorkerSchedulingRuntime(workClasses, owned, authority, scheduler);
            final KeyPair verificationKey =
                    KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final ReadyCertificate readyCertificate = ReadyCertificate.decode(certificate);
            final ChannelResourceIdentity channel = ChannelResourceIdentity.decode(readyCertificate.channel());
            final WorkerPublishPreparationCoordinator preparationCoordinator = new WorkerPublishPreparationCoordinator(
                    owned,
                    authority,
                    () -> 101,
                    preparation -> java.util.Optional.of(new WorkerCommandRuntime.PublishPreparation(
                            channel, readyCertificate, evidence, 3_000, 1, verificationKey.getPrivate(), () -> 101)));
            final WorkerShardRuntime unboundWorkerRuntime = new WorkerShardRuntime(
                    () -> java.util.Optional.empty(),
                    workClasses,
                    owned,
                    store,
                    resources,
                    authority,
                    verificationKey.getPublic(),
                    schedulingRuntime,
                    commandRuntime);
            assertThrows(
                    IllegalStateException.class,
                    () -> unboundWorkerRuntime.runDueClaimPublishTurn(
                            evidence,
                            budget,
                            3_000,
                            claimCharge(payload.length),
                            () -> 101,
                            new SchedulerBudget(1, 1_000_000, 1_000),
                            2));
            final WorkerShardRuntime workerRuntime = new WorkerShardRuntime(
                    () -> java.util.Optional.empty(),
                    workClasses,
                    owned,
                    store,
                    resources,
                    authority,
                    verificationKey.getPublic(),
                    schedulingRuntime,
                    commandRuntime,
                    null,
                    preparationCoordinator);
            final WorkerShardFleetRuntime fleet =
                    new WorkerShardFleetRuntime(workClasses, resources, List.of(workerRuntime));
            final ClaimMaterialization materialization = shard.resolveClaimMaterialization(schedule.delayMessageId());
            final byte[] claimCharge = claimCharge(payload.length);

            final WorkerSchedulingRuntime.DueTurn dueTurn = workerRuntime.runDueTurn(evidence, budget, () -> 101);
            assertEquals(List.of(dueTurn.task()), dueTurn.completedTasks());
            ScheduleWorkItem selected =
                    scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            workClasses.submit(new WorkClassTask(WorkClass.DUE_SCHEDULER, "occupied", 1), () -> {});
            final ScheduleWorkItem queueRejected = selected;
            assertThrows(
                    IllegalStateException.class,
                    () -> executor.submit(queueRejected, evidence, 3_000, materialization, claimCharge, () -> 101));
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));

            selected = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            final ClaimHandoffWorkClassExecutor.Submission prerequisiteDeferred =
                    executor.submit(selected, evidence, 3_000, materialization, claimCharge, () -> 101);
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(
                    ClaimHandoffWorkClassExecutor.ResultKind.PREREQUISITE_UNAVAILABLE,
                    prerequisiteDeferred.result().orElseThrow().kind());
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            assertEquals(
                    MessageStatus.SCHEDULED,
                    shard.getMessage(schedule.delayMessageId()).status());

            prerequisite.set(ClaimHandoffWorkClassExecutor.PrerequisiteDecision.available());
            final ClaimExecutionAdmission.Reservation occupied = permits.tryAcquire(
                            shardId,
                            laneId,
                            lane.laneIncarnation(),
                            com.nereusstream.delay.protocol.DelayMessageId.random(shardId),
                            1,
                            payload.length)
                    .reservation();
            selected = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            final ClaimHandoffWorkClassExecutor.Submission permitDeferred =
                    executor.submit(selected, evidence, 3_000, materialization, claimCharge, () -> 101);
            workClasses.runTurn(new SchedulerBudget(1, 1_000_000, 1_000));
            assertEquals(
                    ClaimHandoffWorkClassExecutor.ResultKind.PERMIT_UNAVAILABLE,
                    permitDeferred.result().orElseThrow().kind());
            assertEquals(
                    ClaimExecutionAdmission.Rejection.LANE_CAPACITY,
                    permitDeferred.result().orElseThrow().permitRejection());
            assertEquals(1, scheduler.snapshot().lanes().get(0).pendingItems());
            occupied.release();

            final ScheduleWorkItem claimedItem =
                    scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            scheduler.requeueFailedClaim(claimedItem);
            final WorkerShardFleetRuntime.DueClaimPublishTurn fleetTurn = fleet.runNextDueClaimPublishTurn(
                            evidence,
                            budget,
                            3_000,
                            claimCharge,
                            () -> 101,
                            new SchedulerBudget(1, 1_000_000, 1_000),
                            2)
                    .orElseThrow();
            assertEquals(shardId, fleetTurn.shardId());
            final WorkerShardRuntime.DueClaimPublishTurn dueClaim = fleetTurn.result();
            final ClaimHandoffWorkClassExecutor.ClaimHandoffResult result =
                    dueClaim.claimResult().orElseThrow();
            assertEquals(ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED, result.kind());
            assertEquals(schedule.delayMessageId(), result.claim().delayMessageId());
            assertEquals(
                    ClaimExecutionAdmission.ReservationState.ACTIVE,
                    result.reservation().state());
            assertEquals(
                    MessageStatus.CLAIMED,
                    shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(0, scheduler.snapshot().lanes().get(0).pendingItems());
            assertThrows(IllegalArgumentException.class, () -> scheduler.requeueFailedClaim(claimedItem));
            final PublishAdmissionWorkClassExecutor.Submission publish =
                    dueClaim.publishSubmission().orElseThrow();
            assertEquals(List.of(publish.task()), dueClaim.publishCompletedTasks());
            assertEquals(
                    PublishAdmissionWorkClassExecutor.ResultKind.UNKNOWN,
                    publish.result().orElseThrow().kind());
            assertEquals(
                    ClaimExecutionAdmission.ReservationState.ACTIVE,
                    result.reservation().state());
            final WorkerPublishPreparationCoordinator certificateDrift = new WorkerPublishPreparationCoordinator(
                    owned,
                    authority,
                    () -> 101,
                    preparation -> java.util.Optional.of(new WorkerCommandRuntime.PublishPreparation(
                            preparation.channel(),
                            ReadyCertificate.decode(bindReadyCertificate(
                                    certificate, owner, Bytes.sha256(Bytes.utf8("claim-work-foreign-store")))),
                            evidence,
                            3_000,
                            1,
                            verificationKey.getPrivate(),
                            () -> 101)));
            assertThrows(IllegalArgumentException.class, () -> certificateDrift.prepare(result));
            assertEquals(0, workClasses.registeredActions());
            assertTrue(result.reservation().release());
            assertFalse(result.reservation().release());
        }
    }

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(
                2_000,
                2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("claim-work-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("claim-work-time-proof")),
                0,
                null);
    }

    private static ProfileRef profile(final ProfileKind kind, final String value) {
        return new ProfileRef(Bytes.utf8(value), 1, Bytes.sha256(Bytes.utf8(value + "-hash")), kind);
    }

    private static byte[] canonicalClaimKafkaLaneTuple(final ProfileRef destination, final ProfileRef capability) {
        final byte[] topicUuid = new byte[16];
        return Bytes.concat(
                new byte[32],
                Bytes.u8(1),
                Bytes.lp32(Bytes.utf8("cluster")),
                Bytes.u8(1),
                topicUuid,
                Bytes.lp32(topicUuid),
                Bytes.u32be(0),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(1),
                Bytes.sha256(Bytes.utf8("claim-work-ordering-domain")));
    }

    private static PublishAdmissionBody.ChargeVector zeroCharge() {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                .canonicalBytes();
    }

    private static byte[] bindReadyCertificate(
            final byte[] encoded, final OwnerIdentity owner, final byte[] storeIncarnation) {
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
            CanonicalProtobuf.bytes(output, 16, Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate\0"), prefix));
        });
    }

    private static void writeField(final ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }

    private static WorkClassExecutionRegistry workClasses(final int maxQueueRecords) {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass =
                    switch (workClass) {
                        case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                        case QUERY, CHECKPOINT -> false;
                    };
            policies.put(
                    workClass,
                    new WorkClassPolicy(
                            1,
                            maxQueueRecords,
                            1_000_000,
                            maxQueueRecords,
                            1_000_000,
                            1_000,
                            protectedClass ? 1 : 0,
                            protectedClass ? 1 : 0,
                            workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 2_000_000), new AtomicLong()::get);
    }
}
