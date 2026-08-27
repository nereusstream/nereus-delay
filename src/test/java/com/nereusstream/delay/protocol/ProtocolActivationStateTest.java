package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ProtocolActivationStateTest {
    @Test
    void roundTripsSortedMarkerEvidenceAndDigest() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        final ProtocolTuple tuple = new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 2);
        final SourcePosition position =
                new KafkaSourcePosition(shard, "activation-cluster", UUID.randomUUID(), 7, 1, 10_000);
        final byte[] mutationId = Bytes.sha256(Bytes.utf8("activation-mutation"));
        final ProtocolActivationState state = new ProtocolActivationState(
                new ShardSubject(shard),
                List.of(new ProtocolActivationState.Activation(
                        tuple,
                        Bytes.sha256(Bytes.utf8("schema")),
                        Bytes.sha256(Bytes.utf8("readers")),
                        position.canonicalBytes(),
                        mutationId)));

        final ProtocolActivationState decoded = ProtocolActivationState.decode(state.canonicalBytes());
        assertEquals(state, decoded);
        assertEquals(tuple, decoded.activation(tuple).tuple());
        assertEquals(position, decoded.activation(tuple).sourcePosition());
        assertArrayEquals(state.stateDigest(), decoded.stateDigest());
        assertEquals(1, decoded.activations().size());
    }

    @Test
    void rejectsDuplicateTupleAndForeignSourcePosition() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        final ProtocolTuple tuple = new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, 2);
        final SourcePosition position =
                new KafkaSourcePosition(shard, "activation-cluster", UUID.randomUUID(), 1, 1, 10_000);
        final ProtocolActivationState state = new ProtocolActivationState(new ShardSubject(shard), List.of());
        final byte[] schema = Bytes.sha256(Bytes.utf8("schema"));
        final byte[] readers = Bytes.sha256(Bytes.utf8("readers"));
        final byte[] mutationId = Bytes.sha256(Bytes.utf8("mutation"));
        final ProtocolActivationState activated = state.activate(tuple, schema, readers, position, mutationId);

        assertThrows(
                IllegalArgumentException.class, () -> activated.activate(tuple, schema, readers, position, mutationId));
        final ShardId otherShard = new ShardId(RouteIncarnation.random(), 5);
        final SourcePosition foreign =
                new KafkaSourcePosition(otherShard, "activation-cluster", UUID.randomUUID(), 2, 1, 10_001);
        assertThrows(
                IllegalArgumentException.class,
                () -> state.activate(tuple, schema, readers, foreign, Bytes.sha256(Bytes.utf8("other-mutation"))));
    }

    @Test
    void currentMarkerRoundTripsTheArtifactSetAndManifestDigest() {
        final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 41)), 2);
        final ArtifactGenerationSet artifacts = ArtifactGenerationSet.current(
                11, PulsarSourceLock.digest(), Bytes.sha256(Bytes.utf8("activation-schema")));
        final SourcePosition position =
                new KafkaSourcePosition(shard, "activation-current-cluster", UUID.randomUUID(), 3, 1, 20_000);
        final byte[] manifest = Bytes.sha256(Bytes.utf8("activation-manifest"));
        final ProtocolActivationState state = new ProtocolActivationState(new ShardSubject(shard), List.of())
                .activate(
                        artifacts.clientCommandTuple(),
                        artifacts.canonicalSchemaBundleHash(),
                        Bytes.sha256(Bytes.utf8("activation-readers")),
                        artifacts,
                        manifest,
                        position,
                        Bytes.sha256(Bytes.utf8("activation-current-mutation")));

        final ProtocolActivationState decoded = ProtocolActivationState.decode(state.canonicalBytes());
        final ProtocolActivationState.Activation activation = decoded.activation(artifacts.clientCommandTuple());
        assertEquals(state, decoded);
        assertTrue(activation.isCurrentGeneration());
        assertEquals(artifacts, activation.artifactGenerationSet());
        assertArrayEquals(manifest, activation.manifestDigest());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }
}
