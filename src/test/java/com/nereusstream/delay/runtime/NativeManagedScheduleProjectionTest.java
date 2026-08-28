package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import com.nereusstream.delay.protocol.AdapterKind;
import com.nereusstream.delay.protocol.AdapterMetadata;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.ClaimMaterialization;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.DeliveryMode;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.NativeDeliveryPolicy;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PrepareLargeScheduleBody;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.PulsarMetadata;
import com.nereusstream.delay.protocol.RetryPolicyRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeManagedScheduleProjectionTest {
    @TempDir
    Path tempDir;

    @Test
    void explicitNativeScheduleKeepsOrdinaryDueAndMaintainsSeparateStaticCandidate() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final ProfileRef destination = profile(ProfileKind.DESTINATION, "native-destination");
        final ProfileRef capability = profile(ProfileKind.DELIVERY_CAPABILITY, "native-capability");
        final byte[] tuple = pulsarTuple(destination, capability);
        final DestinationLaneId lane = DestinationLaneId.derive(tuple);
        final CanonicalScheduleIntent intent = CanonicalScheduleIntent.create(
                destination,
                new RetryPolicyRef(Bytes.utf8("native-retry"), 1, Bytes.sha256(Bytes.utf8("native-retry"))),
                2_000,
                5_000,
                DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT,
                NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF,
                Bytes.utf8("ordering"),
                Bytes.utf8("payload"),
                null,
                AdapterMetadata.pulsar(new PulsarMetadata(null, null, null, List.of())),
                null,
                777L);
        final PreparedCommand schedule = PreparedCommand.schedule(shardId, intent, 9_000);
        final ScheduleResolver resolver = new ScheduleResolver() {
            @Override
            public ResolvedSchedule resolveSchedule(
                    final ShardId ignoredShard,
                    final DelayMessageId ignoredMessage,
                    final CanonicalScheduleIntent resolved,
                    final SourcePosition ignoredPosition) {
                return new ResolvedSchedule(lane, tuple, resolved.inlinePayload(), null, 1_500L);
            }

            @Override
            public ResolvedPrepare resolvePrepare(
                    final ShardId ignoredShard,
                    final DelayMessageId ignoredMessage,
                    final PrepareLargeScheduleBody ignoredBody,
                    final SourcePosition ignoredPosition) {
                throw new UnsupportedOperationException("not used");
            }
        };
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("native-static-index"));
        final KafkaSourcePosition schedulePosition = position(shardId, 0, 1_000);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults(), null, null, resolver);
            assertEquals(
                    StableCode.SCHEDULED,
                    shard.apply(schedule, schedulePosition).stableCode());

            MessageRecord message = shard.getMessage(schedule.delayMessageId());
            assertEquals(2_000, message.runtimeIndex().timeline().actionAtEpochMs());
            assertEquals(1_500, message.earliestNativeCandidateAtEpochMs());
            assertEquals(NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF, message.nativeDeliveryPolicy());
            final byte[] firstNativeKey = KeyCodec.timelineNativeCandidate(
                    lane, 1_500, schedulePosition.sourceOrderToken(), schedule.delayMessageId(), 0);
            assertNotNull(store.getValue(ColumnFamily.TIMELINE, firstNativeKey, 1));

            final ClaimMaterialization ordinary = shard.resolveClaimMaterialization(schedule.delayMessageId());
            assertEquals(2_000, ordinary.actionAtEpochMs());
            assertEquals(NativeDeliveryPolicy.ALLOW_MANAGED_HANDOFF, ordinary.nativeDeliveryPolicy());
            assertEquals(777L, ordinary.eventTimeEpochMs());
            assertNull(ordinary.handoffPolicyHeadRef());

            final PreparedCommand reschedule =
                    PreparedCommand.reschedule(shardId, schedule.delayMessageId(), 0, 3_000, 6_000, 9_000);
            final KafkaSourcePosition reschedulePosition = position(shardId, 1, 1_100);
            assertEquals(
                    StableCode.SUPERSEDED,
                    shard.apply(reschedule, reschedulePosition).stableCode());
            message = shard.getMessage(schedule.delayMessageId());
            assertEquals(3_000, message.runtimeIndex().timeline().actionAtEpochMs());
            assertEquals(2_500, message.earliestNativeCandidateAtEpochMs());
            assertNull(store.get(ColumnFamily.TIMELINE, firstNativeKey));
            final byte[] secondNativeKey = KeyCodec.timelineNativeCandidate(
                    lane, 2_500, reschedulePosition.sourceOrderToken(), schedule.delayMessageId(), 1);
            assertNotNull(store.getValue(ColumnFamily.TIMELINE, secondNativeKey, 1));

            final PreparedCommand cancel = PreparedCommand.cancel(shardId, schedule.delayMessageId(), 1, 9_000);
            assertEquals(
                    StableCode.CANCELED,
                    shard.apply(cancel, position(shardId, 2, 1_200)).stableCode());
            assertNull(store.get(ColumnFamily.TIMELINE, secondNativeKey));
        }
    }

    private static ProfileRef profile(final ProfileKind kind, final String id) {
        return new ProfileRef(Bytes.utf8(id), 1, Bytes.sha256(Bytes.utf8(id + "-semantic")), kind);
    }

    private static byte[] pulsarTuple(final ProfileRef destination, final ProfileRef capability) {
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("native-tenant-scope")),
                Bytes.u8(AdapterKind.PULSAR.wireValue()),
                Bytes.lp32(Bytes.utf8("pulsar-cluster")),
                Bytes.u8(2),
                Bytes.sha256(Bytes.utf8("native-resource-incarnation")),
                Bytes.u64be(17),
                Bytes.lp32(Bytes.utf8("persistent://tenant/ns/native-topic-partition-0")),
                Bytes.u32be(0),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(2),
                Bytes.u32be(0));
    }

    private static KafkaSourcePosition position(final ShardId shard, final long offset, final long timestamp) {
        return new KafkaSourcePosition(
                shard, "cluster", UUID.nameUUIDFromBytes(Bytes.utf8("native-command-topic")), offset, 1, timestamp);
    }
}
