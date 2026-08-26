package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CompatibleControlSnapshot;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.InitialRouteControlActivatePayload;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
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

class InitialRouteControlApplyTest {
    @Test
    void appliesInitialSnapshotAtomicallyAndRetainsItAcrossRestart() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final CompatibleControlSnapshot snapshot = snapshot(shardId);
        final InitialRouteControlActivatePayload payload = new InitialRouteControlActivatePayload(
                snapshot.protocolTuples(),
                snapshot.profiles(),
                snapshot.initialQuotaGrant(),
                snapshot.snapshotDigest());
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlRef ref = controlRef("initial-apply");
        final SystemMutation mutation = mutation(shardId, ref, payload, keyPair);
        final UUID sourceTopic = UUID.randomUUID();
        final ShardStoreConfig config =
                ShardStoreConfig.defaults(java.nio.file.Files.createTempDirectory("initial-route-apply"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final SourcePosition position = position(shardId, sourceTopic, 0);
            final SystemMutationResult result = shard.applySystemMutation(mutation, position, keyPair.getPublic());

            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(StableCode.OK, result.stableCode());
            assertEquals(snapshot, shard.controlSnapshot());
            assertEquals(position, shard.lastAppliedSourcePosition());
            assertEquals(result, shard.applySystemMutation(mutation, position, keyPair.getPublic()));
        }
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore reopened = ShardStore.open(config, shardId, resources)) {
            assertEquals(snapshot, reopened.controlSnapshot());
            assertArrayEquals(
                    snapshot.canonicalBytes(), reopened.controlSnapshot().canonicalBytes());
        }
    }

    @Test
    void rejectsConflictingOrTamperedInitialActivationWithoutOverwritingSnapshot() throws Exception {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final CompatibleControlSnapshot snapshot = snapshot(shardId);
        final InitialRouteControlActivatePayload payload = new InitialRouteControlActivatePayload(
                snapshot.protocolTuples(),
                snapshot.profiles(),
                snapshot.initialQuotaGrant(),
                snapshot.snapshotDigest());
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final UUID sourceTopic = UUID.randomUUID();
        final ShardStoreConfig config =
                ShardStoreConfig.defaults(java.nio.file.Files.createTempDirectory("initial-route-conflict"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
                ShardStore store = ShardStore.open(config, shardId, resources)) {
            final DelayShard shard = new DelayShard(store, DelayShardConfig.defaults());
            final SystemMutation first = mutation(shardId, controlRef("first"), payload, keyPair);
            assertEquals(
                    StableCode.OK,
                    shard.applySystemMutation(first, position(shardId, sourceTopic, 0), keyPair.getPublic())
                            .stableCode());

            final SystemMutation replayAsAnotherOperation = mutation(shardId, controlRef("another"), payload, keyPair);
            final SystemMutationResult stale = shard.applySystemMutation(
                    replayAsAnotherOperation, position(shardId, sourceTopic, 1), keyPair.getPublic());
            assertEquals(ApplyStatus.APPLIED, stale.applyStatus());
            assertEquals(StableCode.STALE_SYSTEM_MUTATION, stale.stableCode());

            final CompatibleControlSnapshot different = new CompatibleControlSnapshot(
                    new ShardSubject(shardId),
                    snapshot.protocolTuples(),
                    List.of(new ProfileRef(
                            Bytes.utf8("different-profile"),
                            1,
                            Bytes.sha256(Bytes.utf8("different-profile")),
                            ProfileKind.DESTINATION)),
                    snapshot.initialQuotaGrant());
            final InitialRouteControlActivatePayload differentPayload = new InitialRouteControlActivatePayload(
                    different.protocolTuples(),
                    different.profiles(),
                    different.initialQuotaGrant(),
                    different.snapshotDigest());
            final SystemMutationResult conflict = shard.applySystemMutation(
                    mutation(shardId, controlRef("conflict"), differentPayload, keyPair),
                    position(shardId, sourceTopic, 2),
                    keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, conflict.applyStatus());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, conflict.stableCode());
            assertEquals(snapshot, shard.controlSnapshot());

            final InitialRouteControlActivatePayload tamperedPayload = new InitialRouteControlActivatePayload(
                    snapshot.protocolTuples(),
                    snapshot.profiles(),
                    snapshot.initialQuotaGrant(),
                    Bytes.sha256(Bytes.utf8("wrong-snapshot")));
            final SystemMutationResult tampered = shard.applySystemMutation(
                    mutation(shardId, controlRef("tampered"), tamperedPayload, keyPair),
                    position(shardId, sourceTopic, 3),
                    keyPair.getPublic());
            assertEquals(ApplyStatus.REJECTED, tampered.applyStatus());
            assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, tampered.stableCode());
            assertEquals(snapshot, shard.controlSnapshot());
        }
    }

    private static SystemMutation mutation(
            final ShardId shardId,
            final ControlRef ref,
            final InitialRouteControlActivatePayload payload,
            final KeyPair keyPair) {
        final byte[] body = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, new ShardSubject(shardId).canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, SystemMutationType.APPLY_SHARD_CONTROL.wireValue());
            CanonicalProtobuf.int64(output, 3, 9_000);
            CanonicalProtobuf.bytes(output, 10, ref.canonicalBytes());
            CanonicalProtobuf.uint32(output, 11, 14);
            CanonicalProtobuf.uint64Bits(output, 12, 1);
            CanonicalProtobuf.bytes(output, 13, Bytes.sha256(Bytes.utf8("initial-route-semantic")));
            CanonicalProtobuf.bytes(
                    output,
                    15,
                    CanonicalProtobuf.message(
                            controlPayload -> CanonicalProtobuf.bytes(controlPayload, 14, payload.canonicalBytes())));
        });
        final AuthorIdentity author = AuthorIdentity.control(
                Bytes.sha256(Bytes.utf8("initial-route-actor")),
                Bytes.sha256(Bytes.utf8("initial-route-roles")),
                Bytes.sha256(Bytes.utf8("initial-route-scope")));
        return SystemMutation.signed(
                shardId,
                SystemMutationType.APPLY_SHARD_CONTROL,
                9_000,
                ref.logicalOperationIdentity(14),
                body,
                author.canonicalBytes(),
                1,
                keyPair.getPrivate());
    }

    private static ControlRef controlRef(final String value) {
        return new ControlRef(
                Bytes.sha256(Bytes.utf8(value + "-operation")), Bytes.sha256(Bytes.utf8(value + "-request")), 0);
    }

    private static SourcePosition position(final ShardId shardId, final UUID sourceTopic, final long offset) {
        return new KafkaSourcePosition(shardId, "initial-route-cluster", sourceTopic, offset, null, 1_000 + offset);
    }

    private static CompatibleControlSnapshot snapshot(final ShardId shardId) {
        final ProfileRef profile = new ProfileRef(
                Bytes.utf8("initial-profile"),
                1,
                Bytes.sha256(Bytes.utf8("initial-profile-semantic")),
                ProfileKind.DESTINATION);
        return new CompatibleControlSnapshot(
                new ShardSubject(shardId),
                List.of(ProtocolTuple.managedCommand()),
                List.of(profile),
                new QuotaGrantRef(
                        Bytes.sha256(Bytes.utf8("initial-grant")),
                        1,
                        new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }
}
