package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandBodies;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandType;
import com.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.DestinationLaneId;
import com.nereusstream.delay.protocol.InitialRouteControlActivatePayloadV1;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.ProtocolActivationStateV1;
import com.nereusstream.delay.protocol.ProtocolTupleV1;
import com.nereusstream.delay.protocol.ProtocolVersionActivatePayloadV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ScheduleIntent;
import com.nereusstream.delay.protocol.SelfRoutingId;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProtocolVersionActivationApplyTest {
    @Test
    void markerIsSourceOrderedDurableAndGatesNewTupleUntilCutover() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 12);
        final ProtocolTupleV1 managed = ProtocolTupleV1.managedCommandV1();
        final ProtocolTupleV1 nextTuple = new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 2);
        final CompatibleControlSnapshotV1 snapshot = new CompatibleControlSnapshotV1(
                new ShardSubjectV1(shardId),
                List.of(managed, nextTuple),
                List.of(),
                new QuotaGrantRefV1(
                        Bytes.sha256(Bytes.utf8("grant")),
                        1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final long now = System.currentTimeMillis();
        final UUID sourceTopic = UUID.randomUUID();
        final ShardStoreConfig config =
                ShardStoreConfig.defaults(java.nio.file.Files.createTempDirectory("protocol-activation-apply"));

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final InitialRouteControlActivatePayloadV1 initial = new InitialRouteControlActivatePayloadV1(
                    snapshot.protocolTuples(),
                    snapshot.profiles(),
                    snapshot.initialQuotaGrant(),
                    snapshot.snapshotDigest());
            final SystemMutation initialMutation =
                    controlMutation(shardId, 14, initial.canonicalBytes(), "initial", now, keyPair);
            final SystemMutationResult initialResult = shard.applySystemMutation(
                    initialMutation, position(shardId, sourceTopic, 0, now), keyPair.getPublic());
            assertEquals(StableCode.OK, initialResult.stableCode());

            final PreparedCommand before = command(shardId, nextTuple, "before", now);
            assertEquals(
                    StableCode.UNACTIVATED_PROTOCOL_VERSION,
                    shard.apply(before, position(shardId, sourceTopic, 1, now + 1))
                            .stableCode());

            final ProtocolVersionActivatePayloadV1 payload = new ProtocolVersionActivatePayloadV1(
                    nextTuple, Bytes.sha256(Bytes.utf8("schema-v2")), Bytes.sha256(Bytes.utf8("readers-v2")));
            final SystemMutation marker = controlMutation(shardId, 1, payload.canonicalBytes(), "marker", now, keyPair);
            assertEquals(
                    StableCode.OK,
                    shard.applySystemMutation(marker, position(shardId, sourceTopic, 2, now + 2), keyPair.getPublic())
                            .stableCode());
            assertNotNull(shard.protocolActivationState());
            assertEquals(
                    payload.tuple(),
                    shard.protocolActivationState().activation(nextTuple).tuple());

            final PreparedCommand after = command(shardId, nextTuple, "after", now);
            assertEquals(
                    StableCode.SCHEDULED,
                    shard.apply(after, position(shardId, sourceTopic, 3, now + 3))
                            .stableCode());
        }

        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore reopenedStore = ShardStore.open(config, shardId, resources)) {
            final DelayShard reopened = new DelayShard(reopenedStore, DelayShardConfig.defaults());
            final ProtocolActivationStateV1 state = reopened.protocolActivationState();
            assertNotNull(state);
            assertEquals(nextTuple, state.activation(nextTuple).tuple());
        }
    }

    private static PreparedCommand command(
            final ShardId shardId, final ProtocolTupleV1 tuple, final String identity, final long now) {
        final DestinationLaneId lane = DestinationLaneId.derive(Bytes.utf8("protocol-activation-lane"));
        final CommandId commandId = new CommandId(SelfRoutingId.random(shardId).bytes());
        final com.nereusstream.delay.protocol.DelayMessageId messageId =
                new com.nereusstream.delay.protocol.DelayMessageId(
                        SelfRoutingId.random(shardId).bytes());
        return PreparedCommand.create(
                shardId,
                commandId,
                messageId,
                CommandType.SCHEDULE,
                tuple,
                now + 30_000,
                CommandBodies.schedule(new ScheduleIntent(
                        lane, now + 1_000, now + 5_000, OrderingMode.BEST_EFFORT, Bytes.utf8(identity))));
    }

    private static SystemMutation controlMutation(
            final ShardId shardId,
            final int kind,
            final byte[] payload,
            final String identity,
            final long now,
            final KeyPair keyPair) {
        final ControlRef ref = new ControlRef(
                Bytes.sha256(Bytes.utf8(identity + "-operation")), Bytes.sha256(Bytes.utf8(identity + "-request")), 0);
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, now + 30_000);
            CanonicalProtobuf.bytes(output, 10, ref.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, kind);
            CanonicalProtobuf.uint64Bits(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8(identity + "-semantic")));
            CanonicalProtobuf.bytes(
                    output,
                    15,
                    CanonicalProtobuf.message(
                            controlPayload -> CanonicalProtobuf.bytes(controlPayload, kind, payload)));
        });
        final AuthorIdentity author = AuthorIdentity.control(
                Bytes.sha256(Bytes.utf8("activation-actor")),
                Bytes.sha256(Bytes.utf8("activation-roles")),
                Bytes.sha256(Bytes.utf8("activation-scope")));
        return SystemMutation.signed(
                shardId,
                SystemMutationType.APPLY_SHARD_CONTROL,
                now + 30_000,
                ref.logicalOperationIdentity(kind),
                body,
                author.canonicalBytes(),
                1,
                keyPair.getPrivate());
    }

    private static SourcePosition position(
            final ShardId shardId, final UUID sourceTopic, final long offset, final long timestamp) {
        return new KafkaSourcePosition(shardId, "activation-cluster", sourceTopic, offset, 1, timestamp);
    }
}
