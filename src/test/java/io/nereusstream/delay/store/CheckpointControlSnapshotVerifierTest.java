package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CompatibleControlSnapshotV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.QuotaGrantRefV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CheckpointControlSnapshotVerifierTest {
    @TempDir
    Path tempDir;

    @Test
    void validatesPhysicalControlSnapshotAgainstManifestDigest() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 7);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("source"));
        final Path checkpoint = tempDir.resolve("checkpoint");
        final CompatibleControlSnapshotV1 snapshot = controlSnapshot(shardId);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.recordControlSnapshot(snapshot);
            store.createCheckpoint(checkpoint, bytes(16, 1));
        }

        assertDoesNotThrow(() -> CheckpointControlSnapshotVerifier.validate(
                checkpoint, shardId, snapshot.snapshotDigest()));
        assertThrows(IllegalArgumentException.class, () -> CheckpointControlSnapshotVerifier.validate(
                checkpoint, shardId, new byte[32]));
    }

    @Test
    void rejectsMissingControlSnapshotFromRecognizedRocksDbImage() {
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 8);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("missing-control-source"));
        final Path checkpoint = tempDir.resolve("missing-control-checkpoint");
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.createCheckpoint(checkpoint, bytes(16, 9));
        }

        final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> CheckpointControlSnapshotVerifier.validate(checkpoint, shardId, new byte[32]));
        org.junit.jupiter.api.Assertions.assertTrue(failure.getMessage().contains("missing control snapshot"));
    }

    private static CompatibleControlSnapshotV1 controlSnapshot(final ShardId shardId) {
        return new CompatibleControlSnapshotV1(new ShardSubjectV1(shardId),
                List.of(new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1)),
                List.of(new ProfileRefV1(bytes(32, 2), 1, bytes(32, 3), ProfileKindV1.DESTINATION)),
                new QuotaGrantRefV1(bytes(32, 4), 1, new PublishAdmissionBody.ChargeVector(
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
