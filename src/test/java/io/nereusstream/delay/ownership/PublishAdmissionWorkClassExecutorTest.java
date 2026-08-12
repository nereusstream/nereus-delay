package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ActivationBarrierV1;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ChannelKindV1;
import io.nereusstream.delay.protocol.ChannelResourceIdentityV1;
import io.nereusstream.delay.protocol.ClaimMaterializationV1;
import io.nereusstream.delay.protocol.CredentialUseKindV1;
import io.nereusstream.delay.protocol.CredentialUseLeaseV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EvidenceCursorV1;
import io.nereusstream.delay.protocol.KafkaActivationBarrier;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.OwnerIdentityV1;
import io.nereusstream.delay.protocol.PayloadForPublishV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedPublishDescriptorV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.ReadyCertificateV1;
import io.nereusstream.delay.protocol.ReservedPublishMetadataV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.MessageRecord;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.RuntimeReadiness;
import io.nereusstream.delay.runtime.ClaimRecord;
import io.nereusstream.delay.scheduler.ClaimExecutionAdmission;
import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PublishAdmissionWorkClassExecutorTest {
    @TempDir
    Path tempDir;

    @Test
    void deferralRetainsExactReservationThenPreparesSignsAndAppends() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 31);
        final UUID sourceTopic = UUID.randomUUID();
        final SourceAssignment assignment = new SourceAssignment(shardId,
                Bytes.sha256(Bytes.utf8("publish-admission-assignment")), 7,
                new KafkaActivationBarrier(shardId, "publish-admission-cluster", sourceTopic, 0));
        final InMemoryOwnerLeaseStore backend = new InMemoryOwnerLeaseStore();
        final OwnerLease lease = backend.acquire(assignment, "publish-admission-owner",
                Bytes.sha256(Bytes.utf8("publish-admission-session")), 100, 100).orElseThrow();
        final OxiaOwnerLeaseStore authority = new OxiaOwnerLeaseStore(backend);
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "publish-admission-destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "publish-admission-capability");
        final DestinationLaneId laneId = DestinationLaneId.derive(Bytes.concat(destination.canonicalBytes(),
                capability.canonicalBytes()));
        final byte[] payload = Bytes.utf8("publish-admission-payload");
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new io.nereusstream.delay.protocol.ScheduleIntent(laneId, 2_000, 9_000,
                        io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT, payload), 10_000);
        final KafkaSourcePosition schedulePosition = new KafkaSourcePosition(shardId,
                "publish-admission-cluster", sourceTopic, 0, null, 1_000);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("publish-admission-store"));
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final OwnerIdentityV1 owner = new OwnerIdentityV1(Bytes.utf8("publish-admission-deployment"),
                    Bytes.utf8("publish-admission-worker"), lease.ownerEpoch(),
                    Bytes.sha256(Bytes.utf8("publish-admission-fence")));
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            shard.apply(schedule, schedulePosition);
            io.nereusstream.delay.runtime.DelayShardTestSupport.updateLaneReadiness(
                    shard, laneId, RuntimeReadiness.READY);
            final OwnedDelayShard owned = new OwnedDelayShard(shard, lease, owner);
            owned.markCatchingUp(authority, assignment, SourceReplaySuccessor.strictKafka(), 101);
            owned.recordCatchup(schedulePosition);
            owned.activateForCommands(authority, 101);

            final MessageRecord message = shard.getMessage(schedule.delayMessageId());
            final ClaimMaterializationV1 materialization = materialization(destination, capability,
                    schedule.delayMessageId(), message, payload);
            final ClaimRecord claim = shard.claimForPublishV1(schedule.delayMessageId(),
                    AuthorIdentity.owner(owner.deploymentId(), owner.workerRunId(), owner.ownerEpoch(),
                            owner.leaseFencingDigest()), 3_000, materialization, claimCharge(payload.length));
            final PreparedPublishDescriptorV1 descriptor = descriptor(claim, materialization);
            final ReadyCertificateV1 certificate = certificate(owner, store.metadata().storeIncarnation(),
                    descriptor, sourceTopic);
            final TrustedUtcIntervalEvidence decision = evidence(2_000, 2_001);

            final ClaimExecutionAdmission permits = new ClaimExecutionAdmission(1, payload.length);
            permits.registerShard(new ClaimExecutionAdmission.ShardSpec(shardId, 1, payload.length));
            permits.registerLane(new ClaimExecutionAdmission.LaneSpec(shardId, laneId, claim.laneIncarnation(),
                    0, 0, 1, payload.length));
            permits.openReady(shardId, laneId, claim.laneIncarnation());
            final ClaimExecutionAdmission.Reservation reservation = permits.tryAcquire(shardId, laneId,
                    claim.laneIncarnation(), claim.delayMessageId(), Integer.toUnsignedLong(claim.generation()),
                    payload.length).reservation();

            final AtomicReference<PublishAdmissionWorkClassExecutor.PrerequisiteDecision> gate =
                    new AtomicReference<>(PublishAdmissionWorkClassExecutor.PrerequisiteDecision.unavailable(
                            PublishAdmissionWorkClassExecutor.PrerequisiteRejection.CHANNEL_OR_CREDENTIAL_UNAVAILABLE));
            final AtomicReference<SystemMutation> appended = new AtomicReference<>();
            final AtomicInteger appendCalls = new AtomicInteger();
            final KafkaSourcePosition admissionPosition = new KafkaSourcePosition(shardId,
                    "publish-admission-cluster", sourceTopic, 1, null, 2_100);
            final ShardLogMutationAppender appender = mutation -> {
                appended.set(mutation);
                appendCalls.incrementAndGet();
                return ShardLogMutationAppender.AppendOutcome.persisted(admissionPosition);
            };
            final WorkClassExecutionRegistry workClasses = workClasses();
            final PublishAdmissionWorkClassExecutor executor = new PublishAdmissionWorkClassExecutor(
                    workClasses, owned, authority, permits, appender, ignored -> gate.get());

            assertThrows(IllegalArgumentException.class, () -> executor.submit(claim, reservation,
                    descriptor, certificate, evidence(1_999, 2_000), 2_500, 1, keyPair.getPrivate(), () -> 101));
            final ReadyCertificateV1 certificateIssuedAfterDecision = certificate(owner,
                    store.metadata().storeIncarnation(), descriptor, sourceTopic, evidence(2_200, 2_201));
            assertThrows(IllegalArgumentException.class, () -> executor.submit(claim, reservation,
                    descriptor, certificateIssuedAfterDecision, evidence(2_100, 2_101), 2_500, 1,
                    keyPair.getPrivate(), () -> 101));
            assertEquals(0, workClasses.registeredActions());
            assertEquals(0, appendCalls.get());

            final ClaimExecutionAdmission foreignPermits = new ClaimExecutionAdmission(1, payload.length);
            foreignPermits.registerShard(new ClaimExecutionAdmission.ShardSpec(shardId, 1, payload.length));
            foreignPermits.registerLane(new ClaimExecutionAdmission.LaneSpec(shardId, laneId,
                    claim.laneIncarnation(), 0, 0, 1, payload.length));
            foreignPermits.openReady(shardId, laneId, claim.laneIncarnation());
            final ClaimExecutionAdmission.Reservation foreignReservation = foreignPermits.tryAcquire(
                    shardId, laneId, claim.laneIncarnation(), claim.delayMessageId(),
                    Integer.toUnsignedLong(claim.generation()), payload.length).reservation();
            assertThrows(IllegalArgumentException.class, () -> executor.submit(claim, foreignReservation,
                    descriptor, certificate, decision, 2_500, 1, keyPair.getPrivate(), () -> 101));
            assertEquals(0, workClasses.registeredActions());
            assertEquals(0, appendCalls.get());
            foreignReservation.release();

            final PublishAdmissionWorkClassExecutor.Submission deferred = executor.submit(claim, reservation,
                    descriptor, certificate, decision, 2_500, 1, keyPair.getPrivate(), () -> 101);
            workClasses.runTurn(new io.nereusstream.delay.scheduler.SchedulerBudget(1, 1_000_000, 1_000));

            final PublishAdmissionWorkClassExecutor.AdmissionHandoffResult deferredResult =
                    deferred.result().orElseThrow();
            assertEquals(PublishAdmissionWorkClassExecutor.ResultKind.PREREQUISITE_UNAVAILABLE,
                    deferredResult.kind());
            assertEquals(0, appendCalls.get());
            assertEquals(ClaimExecutionAdmission.ReservationState.ACTIVE, reservation.state());
            assertEquals(MessageStatus.CLAIMED, shard.getMessage(schedule.delayMessageId()).status());

            gate.set(PublishAdmissionWorkClassExecutor.PrerequisiteDecision.available());
            final PublishAdmissionWorkClassExecutor.Submission admitted = executor.submit(claim, reservation,
                    descriptor, certificate, decision, 2_500, 1, keyPair.getPrivate(), () -> 101);
            assertTrue(admitted.result().isEmpty());
            workClasses.runTurn(new io.nereusstream.delay.scheduler.SchedulerBudget(1, 1_000_000, 1_000));
            final PublishAdmissionWorkClassExecutor.AdmissionHandoffResult admittedResult =
                    admitted.result().orElseThrow();
            assertEquals(PublishAdmissionWorkClassExecutor.ResultKind.ENQUEUED, admittedResult.kind());
            assertEquals(admissionPosition, admittedResult.sourcePosition());
            assertEquals(1, appendCalls.get());
            assertEquals(ClaimExecutionAdmission.ReservationState.ACTIVE, reservation.state());
            assertArrayEquals(appended.get().canonicalBody(), admitted.mutation().canonicalBody());
            assertTrue(admitted.mutation().verifySignature(keyPair.getPublic()));
            assertEquals(claim.claimId()[0], PublishAdmissionBody.decode(admitted.mutation().canonicalBody())
                    .claimId()[0]);
        }
    }

    private static ClaimMaterializationV1 materialization(final ProfileRefV1 destination,
                                                           final ProfileRefV1 capability,
                                                           final DelayMessageId messageId,
                                                           final MessageRecord message,
                                                           final byte[] payload) {
        return new ClaimMaterializationV1(destination, capability,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("publish-admission-cluster",
                        UUID.nameUUIDFromBytes(Bytes.utf8("publish-admission-target")))), 0, messageId,
                Integer.toUnsignedLong(message.generation()), PayloadForPublishV1.inline(payload),
                io.nereusstream.delay.protocol.AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())),
                message.deliverAtEpochMs(), message.expireAtEpochMs(),
                message.runtimeIndex().timeline().actionAtEpochMs());
    }

    private static PreparedPublishDescriptorV1 descriptor(final ClaimRecord claim,
                                                          final ClaimMaterializationV1 materialization) {
        final byte[] attempt = SystemMutation.computePublishAttemptLogicalIdentity(claim.claimId(),
                claim.delayMessageId(), Integer.toUnsignedLong(claim.generation()), 1);
        final ChannelResourceIdentityV1 channel = channel(materialization, claim.laneId().bytes(),
                claim.laneIncarnation());
        final ReservedPublishMetadataV1 reserved = new ReservedPublishMetadataV1(
                claim.delayMessageId().routingId().shardId().routeIncarnation(),
                claim.delayMessageId().routingId().shardId().partition(),
                claim.delayMessageId(), Integer.toUnsignedLong(claim.generation()), attempt,
                materialization.destinationProfile().semanticHash(), materialization.capabilityProfile().semanticHash(),
                materialization.deliverAtEpochMs(), DeliveryMode.MANAGED);
        return new PreparedPublishDescriptorV1(AdapterKindV1.KAFKA, claim.laneId(), claim.laneIncarnation(),
                materialization.destinationProfile(), materialization.capabilityProfile(), materialization.targetResource(),
                materialization.physicalPartition(), channel, materialization.messageId(), materialization.generation(),
                attempt, 1, materialization.payload(), materialization.businessMetadata(), reserved,
                materialization.deliverAtEpochMs(), materialization.expireAtEpochMs(), materialization.actionAtEpochMs());
    }

    private static ChannelResourceIdentityV1 channel(final ClaimMaterializationV1 materialization,
                                                      final byte[] lane, final byte[] laneIncarnation) {
        final byte[] producer = Bytes.utf8("publish-admission-producer");
        final byte[] guard = Bytes.sha256(Bytes.utf8("publish-admission-guard"));
        final byte[] binding = Bytes.sha256(Bytes.utf8("publish-admission-binding"));
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("publish-admission-fingerprint"));
        final TrustedUtcIntervalEvidence issuedAt = evidence(1_000, 1_001);
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKindV1.KAFKA.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKindV1.BASELINE_PRODUCER.wireValue());
            CanonicalProtobuf.bytes(output, 3, lane);
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, materialization.targetResource().canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, materialization.physicalPartition());
            CanonicalProtobuf.uint64(output, 7, 1);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 13, guard);
        });
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(materialization.destinationProfile(),
                CredentialUseKindV1.DESTINATION_CHANNEL,
                CredentialUseLeaseV1.destinationChannelHolderScope(prefix), 1, binding, fingerprint, issuedAt,
                9_000, 1);
        return new ChannelResourceIdentityV1(AdapterKindV1.KAFKA, ChannelKindV1.BASELINE_PRODUCER, lane,
                laneIncarnation, materialization.targetResource(), materialization.physicalPartition(), 1, 0,
                producer, Bytes.sha256(producer), null, null, guard, 1, binding, fingerprint, lease);
    }

    private static ReadyCertificateV1 certificate(final OwnerIdentityV1 owner, final byte[] storeIncarnation,
                                                   final PreparedPublishDescriptorV1 descriptor, final UUID sourceTopic) {
        return certificate(owner, storeIncarnation, descriptor, sourceTopic, evidence(1_000, 1_001));
    }

    private static ReadyCertificateV1 certificate(final OwnerIdentityV1 owner, final byte[] storeIncarnation,
                                                   final PreparedPublishDescriptorV1 descriptor,
                                                   final UUID sourceTopic,
                                                   final TrustedUtcIntervalEvidence issuedAt) {
        final byte[] barrier = ActivationBarrierV1.kafka(descriptor.targetResource(),
                (int) descriptor.physicalPartition(), 0, 0).canonicalBytes();
        final byte[] topicUuid = uuidBytes(sourceTopic);
        final byte[] cursor = EvidenceCursorV1.kafka(descriptor.destinationLaneId().bytes(), descriptor.laneIncarnation(),
                topicUuid, 0, 1, 2_000, 1, 1).canonicalBytes();
        final byte[] binding = descriptor.channel().credentialBindingDigest();
        final byte[] fingerprint = descriptor.channel().resolvedCredentialVersionFingerprintDigest();
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.bytes(output, 2, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, storeIncarnation);
            CanonicalProtobuf.bytes(output, 4, descriptor.destinationLaneId().bytes());
            CanonicalProtobuf.bytes(output, 5, descriptor.laneIncarnation());
            CanonicalProtobuf.bytes(output, 6, descriptor.channel().canonicalBytes());
            CanonicalProtobuf.bytes(output, 7, barrier);
            CanonicalProtobuf.bytes(output, 8, cursor);
            CanonicalProtobuf.uint32(output, 9, 1);
            CanonicalProtobuf.uint32(output, 10, 1);
            CanonicalProtobuf.int64(output, 11, 8_000);
            CanonicalProtobuf.bytes(output, 12, issuedAt.canonicalBytes());
            CanonicalProtobuf.uint64(output, 13, descriptor.channel().credentialBindingGeneration());
            CanonicalProtobuf.bytes(output, 14, binding);
            CanonicalProtobuf.bytes(output, 15, fingerprint);
        });
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(prefix);
            while (reader.hasRemaining()) {
                writeField(output, reader.next());
            }
            CanonicalProtobuf.bytes(output, 16,
                    Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"), prefix));
        });
        return ReadyCertificateV1.decode(encoded);
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest) {
        return new TrustedUtcIntervalEvidence(earliest, latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("publish-admission-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("publish-admission-time")), 0, null);
    }

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final String value) {
        return new ProfileRefV1(Bytes.utf8(value), 1, Bytes.sha256(Bytes.utf8(value + "-semantic")), kind);
    }

    private static byte[] claimCharge(final long payloadBytes) {
        return new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 1, payloadBytes,
                0, 0, 0, 0, 0, 0, 0, 0, 0).canonicalBytes();
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }

    private static void writeField(final ByteArrayOutputStream output,
                                   final io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            io.nereusstream.delay.protocol.CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
        } else {
            io.nereusstream.delay.protocol.CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 4, 1_000_000,
                    4, 1_000_000, 1_000, protectedClass ? 1 : 0, protectedClass ? 1 : 0,
                    workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(new WorkClassRuntimeConfig(policies, 100, 100,
                16, 2_000_000), () -> 0);
    }
}
