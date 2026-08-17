package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.ControlRef;
import io.nereusstream.delay.protocol.InitialRouteControlActivatePayloadV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.store.SharedRocksDbResources;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InitialRouteControlApplyTest {
    @Test
    void appliesInitialSnapshotAtomicallyAndRetainsItAcrossRestart() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final CompatibleControlSnapshotV1 snapshot = snapshot(shardId);
        final InitialRouteControlActivatePayloadV1 payload = new InitialRouteControlActivatePayloadV1(
                snapshot.protocolTuples(), snapshot.profiles(), snapshot.initialQuotaGrant(),
                snapshot.snapshotDigest());
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlRef ref = controlRef("initial-apply");
        final SystemMutation mutation = mutation(shardId, ref, payload, keyPair);
        final UUID sourceTopic = UUID.randomUUID();
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                java.nio.file.Files.createTempDirectory("initial-route-apply"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final SourcePosition position = position(shardId, sourceTopic, 0);
            final SystemMutationResult result = shard.applySystemMutation(mutation, position,
                    keyPair.getPublic());

            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(snapshot, shard.controlSnapshot());
            assertEquals(position, shard.lastAppliedSourcePosition());
            assertEquals(result, shard.applySystemMutation(mutation, position, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertEquals(snapshot, reopened.controlSnapshot());
            assertArrayEquals(snapshot.canonicalBytes(), reopened.controlSnapshot().canonicalBytes());
        }
    }

    @Test
    void rejectsConflictingOrTamperedInitialActivationWithoutOverwritingSnapshot() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final CompatibleControlSnapshotV1 snapshot = snapshot(shardId);
        final InitialRouteControlActivatePayloadV1 payload = new InitialRouteControlActivatePayloadV1(
                snapshot.protocolTuples(), snapshot.profiles(), snapshot.initialQuotaGrant(),
                snapshot.snapshotDigest());
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final UUID sourceTopic = UUID.randomUUID();
        final ShardStoreConfig config = ShardStoreConfig.defaults(
                java.nio.file.Files.createTempDirectory("initial-route-conflict"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final SystemMutation first = mutation(shardId, controlRef("first"), payload, keyPair);
            assertEquals(StableCode.OK, shard.applySystemMutation(first, position(shardId, sourceTopic, 0),
                    keyPair.getPublic()).stableCode());

            final SystemMutation replayAsAnotherOperation = mutation(shardId, controlRef("another"), payload,
                    keyPair);
            final SystemMutationResult stale = shard.applySystemMutation(replayAsAnotherOperation,
                    position(shardId, sourceTopic, 1), keyPair.getPublic());
            assertEquals(ApplyStatus.APPLIED, stale.applyStatus());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION, stale.stableCode());

            final CompatibleControlSnapshotV1 different = new CompatibleControlSnapshotV1(
                    new ShardSubjectV1(shardId), snapshot.protocolTuples(),
                    List.of(new ProfileRefV1(Bytes.utf8("different-profile"), 1,
                            Bytes.sha256(Bytes.utf8("different-profile")), ProfileKindV1.DESTINATION)),
                    snapshot.initialQuotaGrant());
            final InitialRouteControlActivatePayloadV1 differentPayload = new InitialRouteControlActivatePayloadV1(
                    different.protocolTuples(), different.profiles(), different.initialQuotaGrant(),
                    different.snapshotDigest());
            final SystemMutationResult conflict = shard.applySystemMutation(
                    mutation(shardId, controlRef("conflict"), differentPayload, keyPair),
                    position(shardId, sourceTopic, 2), keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, conflict.applyStatus());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, conflict.stableCode());
            assertEquals(snapshot, shard.controlSnapshot());

            final InitialRouteControlActivatePayloadV1 tamperedPayload = new InitialRouteControlActivatePayloadV1(
                    snapshot.protocolTuples(), snapshot.profiles(), snapshot.initialQuotaGrant(),
                    Bytes.sha256(Bytes.utf8("wrong-snapshot")));
            final SystemMutationResult tampered = shard.applySystemMutation(
                    mutation(shardId, controlRef("tampered"), tamperedPayload, keyPair),
                    position(shardId, sourceTopic, 3), keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, tampered.applyStatus());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, tampered.stableCode());
            assertEquals(snapshot, shard.controlSnapshot());
        }
    }

    private static SystemMutation mutation(final ShardId shardId, final ControlRef ref,
                                           final InitialRouteControlActivatePayloadV1 payload,
                                           final KeyPair keyPair) {
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubjectV1(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, ref.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 14);
            CanonicalProtobuf.uint64Bits(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("initial-route-semantic")));
            CanonicalProtobuf.bytes(output, 15, CanonicalProtobuf.message(controlPayload ->
                    CanonicalProtobuf.bytes(controlPayload, 14, payload.canonicalBytes())));
        });
        final AuthorIdentity author = AuthorIdentity.control(Bytes.sha256(Bytes.utf8("initial-route-actor")),
                Bytes.sha256(Bytes.utf8("initial-route-roles")), Bytes.sha256(Bytes.utf8("initial-route-scope")));
        return SystemMutation.signed(shardId, SystemMutationType.APPLY_SHARD_CONTROL, 9_000,
                ref.logicalOperationIdentity(14), body, author.canonicalBytes(), 1, keyPair.getPrivate());
    }

    private static ControlRef controlRef(final String value) {
        return new ControlRef(Bytes.sha256(Bytes.utf8(value + "-operation")),
                Bytes.sha256(Bytes.utf8(value + "-request")), 0);
    }

    private static SourcePosition position(final ShardId shardId, final UUID sourceTopic, final long offset) {
        return new KafkaSourcePosition(shardId, "initial-route-cluster", sourceTopic, offset, null,
                1_000 + offset);
    }

    private static CompatibleControlSnapshotV1 snapshot(final ShardId shardId) {
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("initial-profile"), 1,
                Bytes.sha256(Bytes.utf8("initial-profile-semantic")), ProfileKindV1.DESTINATION);
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shardId),
                List.of(ProtocolTupleV1.managedCommandV1()), List.of(profile),
                new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8("initial-grant")), 1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0,
                                0, 0, 0, 0, 0, 0, 0, 0)));
    }
}
