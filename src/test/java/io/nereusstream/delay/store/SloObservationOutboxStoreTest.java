package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.SloObservationOutboxV1;
import io.nereusstream.delay.protocol.SloObjectiveNameV1;
import io.nereusstream.delay.protocol.SloPathV1;
import io.nereusstream.delay.protocol.SloPopulationV1;
import io.nereusstream.delay.protocol.SloSampleEventIdentityV1;
import io.nereusstream.delay.protocol.SloSampleStartV1;
import io.nereusstream.delay.protocol.SloTimeEndpointKindV1;
import io.nereusstream.delay.protocol.SloTimeEndpointV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SloObservationOutboxStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void scanRejectsKeyValueSampleIdentityMismatch() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-key-fence"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 0);
        final SloObservationOutboxV1 outbox = SloObservationOutboxV1.open(start());
        final byte[] anotherSampleId = bytes(32, 9);
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            store.write(batch -> batch.putValue(ColumnFamily.META, SloObservationOutboxStore.VALUE_TYPE,
                    KeyCodec.metaSloOutbox(anotherSampleId), outbox.canonicalBytes()));
            final SloObservationOutboxStore outboxStore = new SloObservationOutboxStore(store);

            assertThrows(IllegalStateException.class, () -> outboxStore.get(anotherSampleId));
            assertThrows(IllegalStateException.class, () -> outboxStore.scan(10));
        }
    }

    @Test
    void scanReturnsOnlyExactKeyValuePairing() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("slo-outbox-scan"));
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 1);
        final SloSampleStartV1 sample = start();
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config);
             ShardStore store = ShardStore.open(config, shardId, resources)) {
            final SloObservationOutboxStore outboxStore = new SloObservationOutboxStore(store);
            outboxStore.ensureStart(sample);

            assertEquals(1, outboxStore.scan(10).size());
            assertEquals(sample, outboxStore.scan(10).get(0).start());
            final long encodedBytes = ValueEnvelope.encode(SloObservationOutboxStore.VALUE_TYPE,
                    SloObservationOutboxV1.open(sample).canonicalBytes()).length;
            assertEquals(1, outboxStore.scan(10, encodedBytes).size());
            assertThrows(IllegalStateException.class, () -> outboxStore.scan(10, encodedBytes - 1));
        }
    }

    private static SloSampleStartV1 start() {
        final byte[] commandHash = bytes(32, 2);
        final byte[] physicalAttemptId = bytes(16, 3);
        final byte[] completeBranchPayload = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, bytes(41, 1));
            CanonicalProtobuf.bytes(output, 2, commandHash);
            CanonicalProtobuf.bytes(output, 3, physicalAttemptId);
        });
        final SloSampleEventIdentityV1 identity = new SloSampleEventIdentityV1(
                SloObjectiveNameV1.COMMAND_QUEUED_LATENCY, completeBranchPayload);
        return new SloSampleStartV1(bytes(32, 1), SloObjectiveNameV1.COMMAND_QUEUED_LATENCY,
                SloPopulationV1.ALL_ACCEPTED, SloPathV1.NOT_APPLICABLE, identity,
                endpoint(100), 200L);
    }

    private static SloTimeEndpointV1 endpoint(final long epochMs) {
        return new SloTimeEndpointV1(SloTimeEndpointKindV1.SEMANTIC_FIXED_EPOCH, epochMs, epochMs,
                bytes(32, (int) epochMs));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
