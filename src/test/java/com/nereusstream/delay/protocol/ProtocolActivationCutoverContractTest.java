package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.ownership.ProtocolActivationAuthorityCoordinator;
import com.nereusstream.delay.ownership.ProtocolCapabilityAuthority;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Full activation/cutover contract tests, including rollback packaging. */
class ProtocolActivationCutoverContractTest {
    @Test
    void writerBeforeReaderRequiresEveryEligibleWorkerAtTheActivationEdge() {
        final ProtocolTuple next = tuple(2);
        final InMemoryAuthority authority = new InMemoryAuthority();
        authority.put(declaration("worker-a", next, 1));
        authority.put(declaration("worker-b", tuple(1), 2));
        final ProtocolActivationAuthorityCoordinator coordinator =
                new ProtocolActivationAuthorityCoordinator(authority);

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.requireEligibleReaders(next, List.of("worker-a", "worker-b")));

        authority.put(declaration("worker-b", next, 3));
        final ProtocolActivationAuthorityCoordinator.EligibleReaderSet readers =
                coordinator.requireEligibleReaders(next, List.of("worker-b", "worker-a"));
        assertEquals(List.of("worker-a", "worker-b"), readers.workerIds());
        assertEquals(2, readers.publications().size());
    }

    @Test
    void downgradePackageBindsTheActivatedMarkerAndNeverDeletesIt() {
        final ProtocolTuple fallback = tuple(1);
        final ProtocolTuple activated = tuple(2);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 3);
        final SourcePosition fallbackPosition = position(shardId, 1);
        final SourcePosition activatedPosition = position(shardId, 2);
        final ProtocolActivationState state = new ProtocolActivationState(
                new ShardSubject(shardId),
                List.of(
                        new ProtocolActivationState.Activation(
                                fallback, hash(1), hash(2), fallbackPosition.canonicalBytes(), hash(3)),
                        new ProtocolActivationState.Activation(
                                activated, hash(4), hash(5), activatedPosition.canonicalBytes(), hash(6))));
        final ProtocolDowngradePackage downgrade =
                new ProtocolDowngradePackage(fallback, activated, hash(4), state.stateDigest(), hash(7), hash(8));

        downgrade.validateAgainst(state);
        final ProtocolDowngradePackage decoded = ProtocolDowngradePackage.decode(downgrade.canonicalBytes());
        decoded.validateAgainst(state);
        assertArrayEquals(downgrade.packageDigest(), decoded.packageDigest());
        assertEquals(activated, state.activation(activated).tuple());
        assertThrows(
                IllegalStateException.class,
                () -> downgrade.validateAgainst(new ProtocolActivationState(
                        new ShardSubject(shardId),
                        List.of(new ProtocolActivationState.Activation(
                                fallback, hash(1), hash(2), fallbackPosition.canonicalBytes(), hash(3))))));
    }

    @Test
    void sameCanonicalPayloadWithDifferentProtocolVersionsHasDifferentPackageIdentity() {
        final ProtocolTuple fallback = tuple(1);
        final ProtocolTuple current = tuple(2);
        final ProtocolTuple replacement = tuple(3);
        final ShardId shardId = new ShardId(RouteIncarnation.random(), 4);
        final ProtocolActivationState priorState = state(shardId, fallback, current);
        final ProtocolActivationState replacementState = state(shardId, fallback, replacement);
        final ProtocolDowngradePackage priorPackage =
                new ProtocolDowngradePackage(fallback, current, hash(10), priorState.stateDigest(), hash(11), hash(12));
        final ProtocolDowngradePackage replacementPackage = new ProtocolDowngradePackage(
                fallback, replacement, hash(10), replacementState.stateDigest(), hash(11), hash(12));

        assertThrows(IllegalStateException.class, () -> priorPackage.validateAgainst(replacementState));
        if (java.util.Arrays.equals(priorPackage.packageDigest(), replacementPackage.packageDigest())) {
            throw new AssertionError("different protocol versions reused the same downgrade identity");
        }
    }

    private static ProtocolActivationState state(
            final ShardId shardId, final ProtocolTuple fallback, final ProtocolTuple activated) {
        return new ProtocolActivationState(
                new ShardSubject(shardId),
                List.of(
                        new ProtocolActivationState.Activation(
                                fallback, hash(1), hash(2), position(shardId, 1).canonicalBytes(), hash(3)),
                        new ProtocolActivationState.Activation(
                                activated,
                                hash(4),
                                hash(5),
                                position(shardId, 2).canonicalBytes(),
                                hash(6))));
    }

    private static ProtocolCapabilityDeclaration declaration(
            final String workerId, final ProtocolTuple tuple, final int seed) {
        return new ProtocolCapabilityDeclaration(workerId, hash(seed), List.of(tuple), seed, hash(seed + 10));
    }

    private static ProtocolTuple tuple(final int bodyVersion) {
        return new ProtocolTuple(1, 1, ProtocolTuple.CLIENT_COMMAND, 1, bodyVersion);
    }

    private static KafkaSourcePosition position(final ShardId shardId, final long offset) {
        return new KafkaSourcePosition(
                shardId,
                "activation-cutover-cluster",
                UUID.nameUUIDFromBytes(Bytes.utf8("activation-cutover-topic")),
                offset,
                1,
                10_000 + offset);
    }

    private static byte[] hash(final int seed) {
        return Bytes.sha256(Bytes.utf8("activation-cutover-" + seed));
    }

    private static final class InMemoryAuthority implements ProtocolCapabilityAuthority {
        private final Map<String, Publication> publications = new HashMap<>();

        private void put(final ProtocolCapabilityDeclaration declaration) {
            publications.put(declaration.workerId(), new Publication(declaration.capabilityEpoch(), declaration));
        }

        @Override
        public Publication publish(final ProtocolCapabilityDeclaration declaration, final long expectedRevision) {
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
