package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ClaimMaterializationV1;
import io.nereusstream.delay.protocol.CommittedPayloadDescriptorV1;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PayloadForPublishV1;
import io.nereusstream.delay.protocol.PayloadReference;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTestFixtures;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClaimMaterializationRuntimeTest {
    @TempDir
    Path tempDir;

    @Test
    void strictClaimBindsTypedMaterializationToCurrentInlineMessage() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("inline-claim"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final byte[] payload = Bytes.utf8("strict-claim-payload");
        final io.nereusstream.delay.protocol.DestinationLaneId lane =
                io.nereusstream.delay.protocol.DestinationLaneId.derive(Bytes.utf8("strict-claim-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(shardId,
                new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, payload), 9_000);
        final AuthorIdentity owner = owner();

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(io.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);

            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final ClaimMaterializationV1 valid = materialization(schedule.delayMessageId(), current,
                    PayloadForPublishV1.inline(payload));
            final ClaimMaterializationV1 wrongMessage = materialization(DelayMessageId.random(shardId), current,
                    valid.payload());
            final ClaimMaterializationV1 wrongGeneration = newMaterialization(valid, valid.generation() + 1,
                    valid.deliverAtEpochMs(), valid.expireAtEpochMs(), valid.actionAtEpochMs(), valid.payload());
            final ClaimMaterializationV1 wrongWindow = newMaterialization(valid, valid.generation(),
                    valid.deliverAtEpochMs() + 1, valid.expireAtEpochMs(), valid.actionAtEpochMs(), valid.payload());
            final ClaimMaterializationV1 wrongPayload = newMaterialization(valid, valid.generation(),
                    valid.deliverAtEpochMs(), valid.expireAtEpochMs(), valid.actionAtEpochMs(),
                    PayloadForPublishV1.inline(Bytes.utf8("different-payload")));

            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner, 3_000, wrongMessage,
                            zeroCharge()));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner, 3_000, wrongGeneration,
                            zeroCharge()));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner, 3_000, wrongWindow,
                            zeroCharge()));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner, 3_000, wrongPayload,
                            zeroCharge()));

            final ClaimRecord claim = shard.claimForPublishV1(schedule.delayMessageId(), owner, 3_000, valid,
                    zeroCharge());
            assertEquals(valid, claim.materialization());
            assertEquals(MessageStatus.CLAIMED, shard.getMessage(schedule.delayMessageId()).status());
        }
    }

    @Test
    void derivesInlineClaimMaterializationFromAcceptedV1Binding() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("derived-inline-claim"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "derived-destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "derived-capability");
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final io.nereusstream.delay.protocol.DestinationLaneId lane =
                io.nereusstream.delay.protocol.DestinationLaneId.derive(tuple);
        final byte[] payload = Bytes.utf8("derived-inline-payload");
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination,
                new io.nereusstream.delay.protocol.RetryPolicyRefV1(Bytes.utf8("derived-retry"), 1,
                        Bytes.sha256(Bytes.utf8("derived-retry-semantic"))),
                2_000, 5_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT,
                Bytes.utf8("derived-ordering"), payload, null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand schedule = PreparedCommand.scheduleV1(shardId, intent, 9_000);
        final V1ScheduleResolver resolver = new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final DelayMessageId messageId,
                                                     final ScheduleIntentV1 resolvedIntent,
                                                     final SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, payload, null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard, final DelayMessageId messageId,
                                                  final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                  final SourcePosition source) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(io.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());

            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final ClaimMaterializationV1 derived = shard.resolveClaimMaterializationV1(schedule.delayMessageId());

            assertEquals(destination, derived.destinationProfile());
            assertEquals(capability, derived.capabilityProfile());
            assertEquals(lane, current.laneId());
            assertEquals(schedule.delayMessageId(), derived.messageId());
            assertEquals(Integer.toUnsignedLong(current.generation()), derived.generation());
            assertEquals(PayloadForPublishV1.inline(payload), derived.payload());
            assertEquals(intent.adapterMetadata(), derived.businessMetadata());
            assertEquals(current.runtimeIndex().timeline().actionAtEpochMs(), derived.actionAtEpochMs());
        }
    }

    @Test
    void strictClaimBindsCommittedObjectPayloadReference() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("object-claim"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final ProfileRefV1 objectStore = new ProfileRefV1(Bytes.utf8("object-store"), 1,
                Bytes.sha256(Bytes.utf8("object-store")), ProfileKindV1.OBJECT_STORE);
        final byte[] payload = Bytes.utf8("committed-payload");
        final CommittedPayloadDescriptorV1 descriptor = new CommittedPayloadDescriptorV1(objectStore,
                Bytes.utf8("bucket"), Bytes.utf8("object"), Bytes.utf8("version"), null, payload.length,
                Bytes.sha256(payload), Bytes.sha256(Bytes.utf8("reservation")),
                Bytes.sha256(Bytes.utf8("proof")));
        final ProfileRefV1 destination = profile(ProfileKindV1.DESTINATION, "destination");
        final ProfileRefV1 capability = profile(ProfileKindV1.DELIVERY_CAPABILITY, "capability");
        final ScheduleIntentV1 intent = ScheduleIntentV1.create(destination,
                new io.nereusstream.delay.protocol.RetryPolicyRefV1(Bytes.utf8("retry"), 1,
                        Bytes.sha256(Bytes.utf8("retry"))),
                2_000, 5_000, DeliveryMode.MANAGED, OrderingMode.BEST_EFFORT,
                Bytes.utf8("ordering"), null, descriptor,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
        final PreparedCommand schedule = PreparedCommand.scheduleV1(shardId, intent, 9_000);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final io.nereusstream.delay.protocol.DestinationLaneId lane =
                io.nereusstream.delay.protocol.DestinationLaneId.derive(tuple);
        final V1ScheduleResolver resolver = new V1ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(final ShardId shard, final DelayMessageId messageId,
                                                     final ScheduleIntentV1 resolvedIntent,
                                                     final SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, null,
                        PayloadReference.fromDescriptor(resolvedIntent.committedPayload()));
            }

            @Override
            public ResolvedPrepare resolvePrepare(final ShardId shard, final DelayMessageId messageId,
                                                  final io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1 body,
                                                  final SourcePosition source) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(io.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);
            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final ClaimMaterializationV1 valid = new ClaimMaterializationV1(destination, capability,
                    BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("fixture-target-cluster",
                            UUID.nameUUIDFromBytes(Bytes.utf8("fixture-lane-topic")))),
                    3, schedule.delayMessageId(), Integer.toUnsignedLong(current.generation()),
                    PayloadForPublishV1.object(descriptor), intent.adapterMetadata(),
                    current.deliverAtEpochMs(), current.expireAtEpochMs(),
                    current.runtimeIndex().timeline().actionAtEpochMs());
            assertEquals(valid, shard.resolveClaimMaterializationV1(schedule.delayMessageId()));
            final ProfileRefV1 foreignObjectStore = new ProfileRefV1(Bytes.utf8("foreign-object-store"), 7,
                    objectStore.semanticHash(), ProfileKindV1.OBJECT_STORE);
            final CommittedPayloadDescriptorV1 foreignDescriptor = new CommittedPayloadDescriptorV1(
                    foreignObjectStore, descriptor.container(), descriptor.objectKey(),
                    descriptor.immutableObjectVersion(), descriptor.etag(), descriptor.length(),
                    descriptor.payloadSha256(), descriptor.reservationId(), descriptor.proofId());
            final ClaimMaterializationV1 wrongObjectStoreIdentity = newMaterialization(valid, valid.generation(),
                    valid.deliverAtEpochMs(), valid.expireAtEpochMs(), valid.actionAtEpochMs(),
                    PayloadForPublishV1.object(foreignDescriptor));
            final ProfileRefV1 foreignDestination = new ProfileRefV1(Bytes.utf8("foreign-destination"), 8,
                    destination.semanticHash(), ProfileKindV1.DESTINATION);
            final ClaimMaterializationV1 wrongDestinationIdentity = new ClaimMaterializationV1(
                    foreignDestination, valid.capabilityProfile(), valid.targetResource(), valid.physicalPartition(),
                    valid.messageId(), valid.generation(), valid.payload(), valid.businessMetadata(),
                    valid.deliverAtEpochMs(), valid.expireAtEpochMs(), valid.actionAtEpochMs());
            final ClaimMaterializationV1 wrongCapability = new ClaimMaterializationV1(
                    valid.destinationProfile(), profile(ProfileKindV1.DELIVERY_CAPABILITY, "foreign-capability"),
                    valid.targetResource(), valid.physicalPartition(), valid.messageId(), valid.generation(),
                    valid.payload(), valid.businessMetadata(), valid.deliverAtEpochMs(), valid.expireAtEpochMs(),
                    valid.actionAtEpochMs());
            final ClaimMaterializationV1 wrongTarget = new ClaimMaterializationV1(
                    valid.destinationProfile(), valid.capabilityProfile(),
                    BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("fixture-target-cluster",
                            UUID.nameUUIDFromBytes(Bytes.utf8("foreign-target-topic")))),
                    valid.physicalPartition(), valid.messageId(), valid.generation(), valid.payload(),
                    valid.businessMetadata(), valid.deliverAtEpochMs(), valid.expireAtEpochMs(),
                    valid.actionAtEpochMs());
            final ClaimMaterializationV1 wrongPartition = new ClaimMaterializationV1(
                    valid.destinationProfile(), valid.capabilityProfile(), valid.targetResource(), 4,
                    valid.messageId(), valid.generation(), valid.payload(), valid.businessMetadata(),
                    valid.deliverAtEpochMs(), valid.expireAtEpochMs(), valid.actionAtEpochMs());

            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner(), 3_000,
                            wrongObjectStoreIdentity, zeroCharge()));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner(), 3_000,
                            wrongDestinationIdentity, zeroCharge()));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner(), 3_000,
                            wrongCapability, zeroCharge()));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner(), 3_000,
                            wrongTarget, zeroCharge()));
            assertThrows(IllegalArgumentException.class,
                    () -> shard.claimForPublishV1(schedule.delayMessageId(), owner(), 3_000,
                            wrongPartition, zeroCharge()));

            final ClaimRecord claim = shard.claimForPublishV1(schedule.delayMessageId(), owner(), 3_000, valid,
                    zeroCharge());
            assertEquals(valid, claim.materialization());
        }
    }

    private static ClaimMaterializationV1 materialization(final DelayMessageId messageId,
                                                           final MessageRecord current,
                                                           final PayloadForPublishV1 payload) {
        return newMaterialization(messageId, Integer.toUnsignedLong(current.generation()),
                current.deliverAtEpochMs(), current.expireAtEpochMs(), current.runtimeIndex().timeline()
                        .actionAtEpochMs(), payload);
    }

    private static ClaimMaterializationV1 newMaterialization(final ClaimMaterializationV1 source,
                                                              final long generation,
                                                              final long deliverAt,
                                                              final long expireAt,
                                                              final long actionAt,
                                                              final PayloadForPublishV1 payload) {
        return new ClaimMaterializationV1(source.destinationProfile(), source.capabilityProfile(),
                source.targetResource(), source.physicalPartition(), source.messageId(), generation, payload,
                source.businessMetadata(), deliverAt, expireAt, actionAt);
    }

    private static ClaimMaterializationV1 newMaterialization(final DelayMessageId messageId,
                                                              final long generation,
                                                              final long deliverAt,
                                                              final long expireAt,
                                                              final long actionAt,
                                                              final PayloadForPublishV1 payload) {
        return new ClaimMaterializationV1(profile(ProfileKindV1.DESTINATION, "destination"),
                profile(ProfileKindV1.DELIVERY_CAPABILITY, "capability"),
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster",
                        UUID.nameUUIDFromBytes(Bytes.utf8("topic")))), 0, messageId, generation, payload,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), deliverAt, expireAt, actionAt);
    }

    private static ProfileRefV1 profile(final ProfileKindV1 kind, final String id) {
        return new ProfileRefV1(Bytes.utf8(id), 1, Bytes.sha256(Bytes.utf8(id + "-hash")), kind);
    }

    private static AuthorIdentity owner() {
        return AuthorIdentity.owner(Bytes.utf8("claim-deployment"), Bytes.utf8("claim-worker"), 1,
                Bytes.sha256(Bytes.utf8("claim-lease")));
    }

    private static byte[] zeroCharge() {
        return io.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                io.nereusstream.delay.protocol.CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shard, "cluster", UUID.nameUUIDFromBytes(Bytes.utf8("command-topic")), offset,
                1, timestamp);
    }
}
