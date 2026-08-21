package io.nereusstream.delay.protocol;

import io.nereusstream.delay.ownership.ProtocolActivationAuthorityCoordinator;
import io.nereusstream.delay.ownership.ProtocolCapabilityAuthority;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Full-V1 activation/cutover contract tests, including rollback packaging. */
class ProtocolActivationCutoverContractTest {
    @Test
    void writerBeforeReaderRequiresEveryEligibleWorkerAtTheActivationEdge() {
        final ProtocolTupleV1 next = tuple(2);
        final InMemoryAuthority authority = new InMemoryAuthority();
        authority.put(declaration("worker-a", next, 1));
        authority.put(declaration("worker-b", tuple(1), 2));
        final ProtocolActivationAuthorityCoordinator coordinator =
                new ProtocolActivationAuthorityCoordinator(authority);

        assertThrows(IllegalStateException.class,
                () -> coordinator.requireEligibleReaders(next, List.of("worker-a", "worker-b")));

        authority.put(declaration("worker-b", next, 3));
        final ProtocolActivationAuthorityCoordinator.EligibleReaderSet readers =
                coordinator.requireEligibleReaders(next, List.of("worker-b", "worker-a"));
        assertEquals(List.of("worker-a", "worker-b"), readers.workerIds());
        assertEquals(2, readers.publications().size());
    }

    @Test
    void downgradePackageBindsTheActivatedMarkerAndNeverDeletesIt() {
        final ProtocolTupleV1 fallback = tuple(1);
        final ProtocolTupleV1 activated = tuple(2);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final SourcePosition fallbackPosition = position(shardId, 1);
        final SourcePosition activatedPosition = position(shardId, 2);
        final ProtocolActivationStateV1 state = new ProtocolActivationStateV1(
                new ShardSubjectV1(shardId), List.of(
                        new ProtocolActivationStateV1.Activation(fallback, hash(1), hash(2),
                                fallbackPosition.canonicalBytes(), hash(3)),
                        new ProtocolActivationStateV1.Activation(activated, hash(4), hash(5),
                                activatedPosition.canonicalBytes(), hash(6))));
        final ProtocolDowngradePackageV1 downgrade = new ProtocolDowngradePackageV1(
                fallback, activated, hash(4), state.stateDigest(), hash(7), hash(8));

        downgrade.validateAgainst(state);
        final ProtocolDowngradePackageV1 decoded = ProtocolDowngradePackageV1.decode(downgrade.canonicalBytes());
        decoded.validateAgainst(state);
        assertArrayEquals(downgrade.packageDigest(), decoded.packageDigest());
        assertEquals(activated, state.activation(activated).tuple());
        assertThrows(IllegalStateException.class, () -> downgrade.validateAgainst(
                new ProtocolActivationStateV1(new ShardSubjectV1(shardId), List.of(
                        new ProtocolActivationStateV1.Activation(fallback, hash(1), hash(2),
                                fallbackPosition.canonicalBytes(), hash(3))))));
    }

    @Test
    void sameCanonicalPayloadWithDifferentProtocolVersionsHasDifferentPackageIdentity() {
        final ProtocolTupleV1 fallback = tuple(1);
        final ProtocolTupleV1 v2 = tuple(2);
        final ProtocolTupleV1 v3 = tuple(3);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 4);
        final ProtocolActivationStateV1 v2State = state(shardId, fallback, v2);
        final ProtocolActivationStateV1 v3State = state(shardId, fallback, v3);
        final ProtocolDowngradePackageV1 v2Package = new ProtocolDowngradePackageV1(
                fallback, v2, hash(10), v2State.stateDigest(), hash(11), hash(12));
        final ProtocolDowngradePackageV1 v3Package = new ProtocolDowngradePackageV1(
                fallback, v3, hash(10), v3State.stateDigest(), hash(11), hash(12));

        assertThrows(IllegalStateException.class, () -> v2Package.validateAgainst(v3State));
        if (java.util.Arrays.equals(v2Package.packageDigest(), v3Package.packageDigest())) {
            throw new AssertionError("different protocol versions reused the same downgrade identity");
        }
    }

    private static ProtocolActivationStateV1 state(final ShardId shardId,
                                                   final ProtocolTupleV1 fallback,
                                                   final ProtocolTupleV1 activated) {
        return new ProtocolActivationStateV1(new ShardSubjectV1(shardId), List.of(
                new ProtocolActivationStateV1.Activation(fallback, hash(1), hash(2),
                        position(shardId, 1).canonicalBytes(), hash(3)),
                new ProtocolActivationStateV1.Activation(activated, hash(4), hash(5),
                        position(shardId, 2).canonicalBytes(), hash(6))));
    }

    private static ProtocolCapabilityDeclarationV1 declaration(final String workerId,
                                                               final ProtocolTupleV1 tuple,
                                                               final int seed) {
        return new ProtocolCapabilityDeclarationV1(workerId, hash(seed), List.of(tuple), seed, hash(seed + 10));
    }

    private static ProtocolTupleV1 tuple(final int bodyVersion) {
        return new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, bodyVersion);
    }

    private static KafkaSourcePosition position(final ShardId shardId, final long offset) {
        return new KafkaSourcePosition(shardId, "activation-cutover-cluster",
                UUID.nameUUIDFromBytes(Bytes.utf8("activation-cutover-topic")), offset, 1,
                10_000 + offset);
    }

    private static byte[] hash(final int seed) {
        return Bytes.sha256(Bytes.utf8("activation-cutover-" + seed));
    }

    private static final class InMemoryAuthority implements ProtocolCapabilityAuthority {
        private final Map<String, Publication> publications = new HashMap<>();

        private void put(final ProtocolCapabilityDeclarationV1 declaration) {
            publications.put(declaration.workerId(), new Publication(declaration.capabilityEpoch(), declaration));
        }

        @Override
        public Publication publish(final ProtocolCapabilityDeclarationV1 declaration, final long expectedRevision) {
            final Publication publication = new Publication(expectedRevision + 1, declaration);
            publications.put(declaration.workerId(), publication);
            return publication;
        }

        @Override
        public Optional<Publication> current(final String workerId) {
            return Optional.ofNullable(publications.get(workerId));
        }

        @Override
        public boolean withdraw(final Publication expected) {
            return publications.remove(expected.declaration().workerId(), expected);
        }
    }
}
