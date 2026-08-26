package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.CommittedPayloadDescriptor;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaMetadata;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PayloadForPublish;
import com.nereusstream.delay.protocol.PayloadReference;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTestFixtures;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ClaimMaterializationRuntimeTest {
    @TempDir
    Path tempDir;

    @Test
    void strictClaimBindsTypedMaterializationToCurrentInlineMessage() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("inline-claim"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final byte[] payload = Bytes.utf8("strict-claim-payload");
        final com.nereusstream.delay.protocol.DestinationLaneId lane =
                com.nereusstream.delay.protocol.DestinationLaneId.derive(Bytes.utf8("strict-claim-lane"));
        final PreparedCommand schedule = PreparedCommand.schedule(
                shardId, new ScheduleIntent(lane, 2_000, 5_000, OrderingMode.BEST_EFFORT, payload), 9_000);
        final AuthorIdentity owner = owner();

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            assertEquals(
                    com.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            shard.updateLaneReadiness(lane, RuntimeReadiness.READY);

            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final ClaimMaterialization valid =
                    materialization(schedule.delayMessageId(), current, PayloadForPublish.inline(payload));
            final ClaimMaterialization wrongMessage =
                    materialization(DelayMessageId.random(shardId), current, valid.payload());
            final ClaimMaterialization wrongGeneration = newMaterialization(
                    valid,
                    valid.generation() + 1,
                    valid.deliverAtEpochMs(),
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs(),
                    valid.payload());
            final ClaimMaterialization wrongWindow = newMaterialization(
                    valid,
                    valid.generation(),
                    valid.deliverAtEpochMs() + 1,
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs(),
                    valid.payload());
            final ClaimMaterialization wrongPayload = newMaterialization(
                    valid,
                    valid.generation(),
                    valid.deliverAtEpochMs(),
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs(),
                    PayloadForPublish.inline(Bytes.utf8("different-payload")));

            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(schedule.delayMessageId(), owner, 3_000, wrongMessage, zeroCharge()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(
                            schedule.delayMessageId(), owner, 3_000, wrongGeneration, zeroCharge()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(schedule.delayMessageId(), owner, 3_000, wrongWindow, zeroCharge()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(schedule.delayMessageId(), owner, 3_000, wrongPayload, zeroCharge()));

            final ClaimRecord claim =
                    shard.claimForPublish(schedule.delayMessageId(), owner, 3_000, valid, zeroCharge());
            assertEquals(valid, claim.materialization());
            assertEquals(
                    MessageStatus.CLAIMED,
                    shard.getMessage(schedule.delayMessageId()).status());
        }
    }

    @Test
    void derivesInlineClaimMaterializationFromAcceptedBinding() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("derived-inline-claim"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final ProfileRef destination = profile(ProfileKind.DESTINATION, "derived-destination");
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "derived-capability");
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final com.nereusstream.delay.protocol.DestinationLaneId lane =
                com.nereusstream.delay.protocol.DestinationLaneId.derive(tuple);
        final byte[] payload = Bytes.utf8("derived-inline-payload");
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                new com.nereusstream.delay.protocol.RetryPolicyRef(
                        Bytes.utf8("derived-retry"), 1, Bytes.sha256(Bytes.utf8("derived-retry-semantic"))),
                2_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("derived-ordering"),
                payload,
                null,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final PreparedCommand schedule = PreparedCommand.schedule(shardId, intent, 9_000);
        final ScheduleResolver resolver = new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final DelayMessageId messageId,
                    final CanonicalScheduleIntent resolvedIntent,
                    final SourcePosition source) {
                return new ResolvedSchedule(lane, tuple, payload, null);
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId shard,
                    final DelayMessageId messageId,
                    final com.nereusstream.delay.protocol.PrepareLargeScheduleBody body,
                    final SourcePosition source) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(
                    com.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());

            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final ClaimMaterialization derived = shard.resolveClaimMaterialization(schedule.delayMessageId());

            assertEquals(destination, derived.destinationProfile());
            assertEquals(capability, derived.capabilityProfile());
            assertEquals(lane, current.laneId());
            assertEquals(schedule.delayMessageId(), derived.messageId());
            assertEquals(Integer.toUnsignedLong(current.generation()), derived.generation());
            assertEquals(PayloadForPublish.inline(payload), derived.payload());
            assertEquals(intent.adapterMetadata(), derived.businessMetadata());
            assertEquals(current.runtimeIndex().timeline().actionAtEpochMs(), derived.actionAtEpochMs());
        }
    }

    @Test
    void strictClaimBindsCommittedObjectPayloadReference() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("object-claim"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 2);
        final ProfileRef objectStore = new ProfileRef(
                Bytes.utf8("object-store"), 1, Bytes.sha256(Bytes.utf8("object-store")), ProfileKind.OBJECT_STORE);
        final byte[] payload = Bytes.utf8("committed-payload");
        final CommittedPayloadDescriptor descriptor = new CommittedPayloadDescriptor(
                objectStore,
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                null,
                payload.length,
                Bytes.sha256(payload),
                Bytes.sha256(Bytes.utf8("reservation")),
                Bytes.sha256(Bytes.utf8("proof")));
        final ProfileRef destination = profile(ProfileKind.DESTINATION, "destination");
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "capability");
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                new com.nereusstream.delay.protocol.RetryPolicyRef(
                        Bytes.utf8("retry"), 1, Bytes.sha256(Bytes.utf8("retry"))),
                2_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                Bytes.utf8("ordering"),
                null,
                descriptor,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                null,
                null);
        final PreparedCommand schedule = PreparedCommand.schedule(shardId, intent, 9_000);
        final byte[] tuple = ProtocolTestFixtures.canonicalKafkaLaneTuple(destination, capability);
        final com.nereusstream.delay.protocol.DestinationLaneId lane =
                com.nereusstream.delay.protocol.DestinationLaneId.derive(tuple);
        final ScheduleResolver resolver = new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId shard,
                    final DelayMessageId messageId,
                    final CanonicalScheduleIntent resolvedIntent,
                    final SourcePosition source) {
                return new ResolvedSchedule(
                        lane, tuple, null, PayloadReference.fromDescriptor(resolvedIntent.committedPayload()));
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId shard,
                    final DelayMessageId messageId,
                    final com.nereusstream.delay.protocol.PrepareLargeScheduleBody body,
                    final SourcePosition source) {
                throw new UnsupportedOperationException("not used by this test");
            }
        };

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(
                    com.nereusstream.delay.protocol.StableCode.SCHEDULED,
                    shard.apply(schedule, position(shardId, 0, 1_000)).stableCode());
            DelayShardTestSupport.activateTypedLaneReadinessForTest(shard, lane);
            final MessageRecord current = shard.getMessage(schedule.delayMessageId());
            final ClaimMaterialization valid = new ClaimMaterialization(
                    destination,
                    capability,
                    BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                            "fixture-target-cluster", UUID.nameUUIDFromBytes(Bytes.utf8("fixture-lane-topic")))),
                    3,
                    schedule.delayMessageId(),
                    Integer.toUnsignedLong(current.generation()),
                    PayloadForPublish.object(descriptor),
                    intent.adapterMetadata(),
                    current.deliverAtEpochMs(),
                    current.expireAtEpochMs(),
                    current.runtimeIndex().timeline().actionAtEpochMs());
            assertEquals(valid, shard.resolveClaimMaterialization(schedule.delayMessageId()));
            final ProfileRef foreignObjectStore = new ProfileRef(
                    Bytes.utf8("foreign-object-store"), 7, objectStore.semanticHash(), ProfileKind.OBJECT_STORE);
            final CommittedPayloadDescriptor foreignDescriptor = new CommittedPayloadDescriptor(
                    foreignObjectStore,
                    descriptor.container(),
                    descriptor.objectKey(),
                    descriptor.immutableObjectVersion(),
                    descriptor.etag(),
                    descriptor.length(),
                    descriptor.payloadSha256(),
                    descriptor.reservationId(),
                    descriptor.proofId());
            final ClaimMaterialization wrongObjectStoreIdentity = newMaterialization(
                    valid,
                    valid.generation(),
                    valid.deliverAtEpochMs(),
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs(),
                    PayloadForPublish.object(foreignDescriptor));
            final ProfileRef foreignDestination = new ProfileRef(
                    Bytes.utf8("foreign-destination"), 8, destination.semanticHash(), ProfileKind.DESTINATION);
            final ClaimMaterialization wrongDestinationIdentity = new ClaimMaterialization(
                    foreignDestination,
                    valid.capabilityProfile(),
                    valid.targetResource(),
                    valid.physicalPartition(),
                    valid.messageId(),
                    valid.generation(),
                    valid.payload(),
                    valid.businessMetadata(),
                    valid.deliverAtEpochMs(),
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs());
            final ClaimMaterialization wrongCapability = new ClaimMaterialization(
                    valid.destinationProfile(),
                    profile(ProfileKind.DELIVERY_CAPABILITY, "foreign-capability"),
                    valid.targetResource(),
                    valid.physicalPartition(),
                    valid.messageId(),
                    valid.generation(),
                    valid.payload(),
                    valid.businessMetadata(),
                    valid.deliverAtEpochMs(),
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs());
            final ClaimMaterialization wrongTarget = new ClaimMaterialization(
                    valid.destinationProfile(),
                    valid.capabilityProfile(),
                    BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                            "fixture-target-cluster", UUID.nameUUIDFromBytes(Bytes.utf8("foreign-target-topic")))),
                    valid.physicalPartition(),
                    valid.messageId(),
                    valid.generation(),
                    valid.payload(),
                    valid.businessMetadata(),
                    valid.deliverAtEpochMs(),
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs());
            final ClaimMaterialization wrongPartition = new ClaimMaterialization(
                    valid.destinationProfile(),
                    valid.capabilityProfile(),
                    valid.targetResource(),
                    4,
                    valid.messageId(),
                    valid.generation(),
                    valid.payload(),
                    valid.businessMetadata(),
                    valid.deliverAtEpochMs(),
                    valid.expireAtEpochMs(),
                    valid.actionAtEpochMs());

            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(
                            schedule.delayMessageId(), owner(), 3_000, wrongObjectStoreIdentity, zeroCharge()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(
                            schedule.delayMessageId(), owner(), 3_000, wrongDestinationIdentity, zeroCharge()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(
                            schedule.delayMessageId(), owner(), 3_000, wrongCapability, zeroCharge()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(schedule.delayMessageId(), owner(), 3_000, wrongTarget, zeroCharge()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> shard.claimForPublish(
                            schedule.delayMessageId(), owner(), 3_000, wrongPartition, zeroCharge()));

            final ClaimRecord claim =
                    shard.claimForPublish(schedule.delayMessageId(), owner(), 3_000, valid, zeroCharge());
            assertEquals(valid, claim.materialization());
        }
    }

    private static ClaimMaterialization materialization(
            final DelayMessageId messageId, final MessageRecord current, final PayloadForPublish payload) {
        return newMaterialization(
                messageId,
                Integer.toUnsignedLong(current.generation()),
                current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.runtimeIndex().timeline().actionAtEpochMs(),
                payload);
    }

    private static ClaimMaterialization newMaterialization(
            final ClaimMaterialization source,
            final long generation,
            final long deliverAt,
            final long expireAt,
            final long actionAt,
            final PayloadForPublish payload) {
        return new ClaimMaterialization(
                source.destinationProfile(),
                source.capabilityProfile(),
                source.targetResource(),
                source.physicalPartition(),
                source.messageId(),
                generation,
                payload,
                source.businessMetadata(),
                deliverAt,
                expireAt,
                actionAt);
    }

    private static ClaimMaterialization newMaterialization(
            final DelayMessageId messageId,
            final long generation,
            final long deliverAt,
            final long expireAt,
            final long actionAt,
            final PayloadForPublish payload) {
        return new ClaimMaterialization(
                profile(ProfileKind.DESTINATION, "destination"),
                profile(ProfileKind.DELIVERY_CAPABILITY, "capability"),
                BrokerResourceIdentity.kafka(
                        new KafkaBrokerResourceIdentity("cluster", UUID.nameUUIDFromBytes(Bytes.utf8("topic")))),
                0,
                messageId,
                generation,
                payload,
                AdapterMetadata.kafka(new KafkaMetadata(null, List.of())),
                deliverAt,
                expireAt,
                actionAt);
    }

    private static ProfileRef profile(final ProfileKind kind, final String id) {
        return new ProfileRef(Bytes.utf8(id), 1, Bytes.sha256(Bytes.utf8(id + "-hash")), kind);
    }

    private static AuthorIdentity owner() {
        return AuthorIdentity.owner(
                Bytes.utf8("claim-deployment"), Bytes.utf8("claim-worker"), 1, Bytes.sha256(Bytes.utf8("claim-lease")));
    }

    private static byte[] zeroCharge() {
        return com.nereusstream.delay.protocol.CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(output, number, 0);
            }
        });
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(
                shard, "cluster", UUID.nameUUIDFromBytes(Bytes.utf8("command-topic")), offset, 1, timestamp);
    }
}
