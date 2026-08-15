package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActiveLaneStateV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.ClaimMaterializationV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LaneCircuitStateV1;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishAdmissionBodyTest;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.DelayShardTestSupport;
import io.nereusstream.delay.runtime.GenerationRuntimeIndex;
import io.nereusstream.delay.runtime.LaneRecord;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.runtime.V1ScheduleResolver;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
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
import java.security.KeyPair;
import java.security.KeyPairGenerator;
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
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "capability");
        final byte[] laneTuple = canonicalClaimKafkaLaneTuple(destination, capability);
        final DestinationLaneId laneId = DestinationLaneId.derive(laneTuple);
        final byte[] payload = Bytes.utf8("claim-work-payload");
        final ScheduleIntentV1 scheduleIntent = ScheduleIntentV1.create(destination,
                new io.nereusstream.delay.protocol.RetryPolicyRefV1(Bytes.utf8("claim-work-retry"), 1,
                        Bytes.sha256(Bytes.utf8("claim-work-retry-semantic"))),
                2_000, 9_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT,
                Bytes.utf8("claim-work-ordering"), payload, null,
                AdapterMetadataV1.kafka(new io.nereusstream.delay.protocol.KafkaMetadataV1(null, List.of())),
                null, null);
        final PreparedCommand schedule = PreparedCommand.scheduleV1(shardId, scheduleIntent, 10_000);
        final V1ScheduleResolver resolver = new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard,
                                                     final io.nereusstream.delay.protocol.DelayMessageId messageId,
                                                     final ScheduleIntentV1 intent,
                                                     final SourcePosition source) {
                return new ResolvedSchedule(laneId, laneTuple, payload, null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard,
                                                  final io.nereusstream.delay.protocol.DelayMessageId messageId,
                                                  final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                  final SourcePosition source) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };
        final KafkaSourcePosition source = new KafkaSourcePosition(shardId, "claim-work-cluster", topic,
                0, null, 1_000);
        final TrustedUtcIntervalEvidence evidence = evidence();
        final SchedulerBudget budget = new SchedulerBudget(1, 4_096, 1_000_000_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("claim-work-store"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("claim-work-deployment"),
                    Bytes.utf8("claim-work-worker"), lease.ownerEpoch(),
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
            final byte[] readyKey = KeyCodec.timelineReady(lane.nextEligibleAtEpochMs(), laneId,
                    lane.laneVersion());
            final PersistentLaneScheduler scheduler = new PersistentLaneScheduler(
                    store, LaneScheduler.defaults(), owner);
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
            io.nereusstream.delay.scheduler.PersistentLaneSchedulerTestSupport.register(scheduler, lane);
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
            final PublishAdmissionWorkClassExecutor publishExecutor = new PublishAdmissionWorkClassExecutor(
                    workClasses, owned, authority, permits, ignored -> ShardLogMutationAppender.AppendOutcome.unknown(),
                    ignored -> PublishAdmissionWorkClassExecutor.PrerequisiteDecision.available());
            assertThrows(IllegalArgumentException.class, () -> new WorkerCommandRuntime(workClasses(1), resources,
                    executor, publishExecutor));
            final WorkerCommandRuntime commandRuntime = new WorkerCommandRuntime(workClasses, resources,
                    executor, publishExecutor);
            final WorkerSchedulingRuntime schedulingRuntime = new WorkerSchedulingRuntime(
                    workClasses, owned, authority, scheduler);
            final KeyPair verificationKey = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
            final WorkerShardRuntime workerRuntime = new WorkerShardRuntime(() -> java.util.Optional.empty(),
                    workClasses, owned, store, resources, authority, verificationKey.getPublic(),
                    schedulingRuntime, commandRuntime);
            final ClaimMaterializationV1 materialization = shard.resolveClaimMaterializationV1(
                    schedule.delayMessageId());
            final byte[] claimCharge = claimCharge(payload.length);
            final ReadyCertificateV1 readyCertificate = ReadyCertificateV1.decode(certificate);
            final ChannelResourceIdentityV1 channel = ChannelResourceIdentityV1.decode(
                    readyCertificate.channel());
            final WorkerPublishPreparationCoordinator preparationCoordinator =
                    new WorkerPublishPreparationCoordinator(owned, authority, () -> 101,
                            preparation -> java.util.Optional.of(new WorkerCommandRuntime.PublishPreparation(
                                    channel, readyCertificate, evidence, 3_000, 1,
                                    verificationKey.getPrivate(), () -> 101)));

            final WorkerSchedulingRuntime.DueTurn dueTurn = workerRuntime.runDueTurn(evidence, budget, () -> 101);
            assertEquals(List.of(dueTurn.task()), dueTurn.completedTasks());
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

            final ScheduleWorkItem claimedItem = scheduler.poll(evidence.earliestEpochMs(), budget).get(0);
            scheduler.requeueFailedClaim(claimedItem);
            final WorkerShardRuntime.DueClaimPublishTurn dueClaim = workerRuntime.runDueClaimPublishTurn(
                    evidence, budget, 3_000, claimCharge, () -> 101,
                    new SchedulerBudget(1, 1_000_000, 1_000), 2,
                    preparationCoordinator);
            final ClaimHandoffWorkClassExecutor.ClaimHandoffResult result = dueClaim.claimResult().orElseThrow();
            assertEquals(ClaimHandoffWorkClassExecutor.ResultKind.CLAIMED, result.kind());
            assertEquals(schedule.delayMessageId(), result.claim().delayMessageId());
            assertEquals(ClaimExecutionAdmission.ReservationState.ACTIVE, result.reservation().state());
            assertEquals(MessageStatus.CLAIMED, shard.getMessage(schedule.delayMessageId()).status());
            assertEquals(0, scheduler.snapshot().lanes().get(0).pendingItems());
            assertThrows(IllegalArgumentException.class, () -> scheduler.requeueFailedClaim(claimedItem));
            final PublishAdmissionWorkClassExecutor.Submission publish = dueClaim.publishSubmission().orElseThrow();
            assertEquals(List.of(publish.task()), dueClaim.publishCompletedTasks());
            assertEquals(PublishAdmissionWorkClassExecutor.ResultKind.UNKNOWN,
                    publish.result().orElseThrow().kind());
            assertEquals(ClaimExecutionAdmission.ReservationState.ACTIVE, result.reservation().state());
            final WorkerPublishPreparationCoordinator certificateDrift =
                    new WorkerPublishPreparationCoordinator(owned, authority, () -> 101,
                            preparation -> java.util.Optional.of(new WorkerCommandRuntime.PublishPreparation(
                                    preparation.channel(),
                                    ReadyCertificateV1.decode(bindReadyCertificate(certificate, owner,
                                            Bytes.sha256(Bytes.utf8("claim-work-foreign-store")))),
                                    evidence, 3_000, 1, verificationKey.getPrivate(), () -> 101)));
            assertThrows(IllegalArgumentException.class, () -> certificateDrift.prepare(result));
            assertEquals(0, workClasses.registeredActions());
            assertTrue(result.reservation().release());
            assertFalse(result.reservation().release());
        }
    }

    private static TrustedUtcIntervalEvidence evidence() {
        return new TrustedUtcIntervalEvidence(2_000, 2_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("claim-work-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("claim-work-time-proof")), 0, null);
    }

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final String value) {
        return new ProfileRefV1(Bytes.utf8(value), 1, Bytes.sha256(Bytes.utf8(value + "-hash")), kind);
    }

    private static byte[] canonicalClaimKafkaLaneTuple(final ProfileRefV1 destination,
                                                        final ProfileRefV1 capability) {
        final byte[] topicUuid = new byte[16];
        return Bytes.concat(new byte[32], Bytes.u8(1), Bytes.lp32(Bytes.utf8("cluster")), Bytes.u8(1),
                topicUuid, Bytes.lp32(topicUuid), Bytes.u32be(0), Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()), destination.semanticHash(), Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()), capability.semanticHash(), Bytes.u8(1),
                Bytes.sha256(Bytes.utf8("claim-work-ordering-domain")));
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
