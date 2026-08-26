package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ProtocolCapabilityDeclaration;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.ProtocolVersionActivatePayload;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ProtocolActivationAuthorityCoordinatorTest {
    @Test
    void activationUsesCurrentEligibleReaderSetEvidence() {
        final ProtocolTuple tuple = tuple(2);
        final InMemoryAuthority authority = new InMemoryAuthority();
        authority.put("worker-a", declaration("worker-a", tuple, 1));
        authority.put("worker-b", declaration("worker-b", tuple, 2));
        final ProtocolActivationAuthorityCoordinator coordinator =
                new ProtocolActivationAuthorityCoordinator(authority);

        final var evidence = coordinator.requireEligibleReaders(tuple, List.of("worker-b", "worker-a"));
        final ProtocolVersionActivatePayload payload =
                new ProtocolVersionActivatePayload(tuple, bytes(32, 30), evidence.evidenceHash());
        final var authorized = coordinator.authorize(payload, List.of("worker-a", "worker-b"));

        assertEquals(List.of("worker-a", "worker-b"), authorized.workerIds());
        assertArrayEquals(evidence.evidenceHash(), authorized.evidenceHash());
    }

    @Test
    void missingOrIncompatibleEligibleReaderFailsClosed() {
        final ProtocolTuple tuple = tuple(3);
        final InMemoryAuthority authority = new InMemoryAuthority();
        authority.put("worker-a", declaration("worker-a", tuple, 1));
        authority.put("worker-b", declaration("worker-b", tuple(4), 2));
        final ProtocolActivationAuthorityCoordinator coordinator =
                new ProtocolActivationAuthorityCoordinator(authority);

        assertThrows(
                IllegalStateException.class,
                () -> coordinator.requireEligibleReaders(tuple, List.of("worker-a", "worker-b")));
        assertThrows(
                IllegalStateException.class,
                () -> coordinator.requireEligibleReaders(tuple, List.of("worker-a", "worker-c")));
    }

    private static ProtocolCapabilityDeclaration declaration(
            final String workerId, final ProtocolTuple tuple, final int seed) {
        return new ProtocolCapabilityDeclaration(workerId, bytes(32, seed), List.of(tuple), seed, bytes(32, seed + 10));
    }

    private static ProtocolTuple tuple(final int minor) {
        return new ProtocolTuple(1, minor, ProtocolTuple.CLIENT_COMMAND, 1, 1);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }

    private static final class InMemoryAuthority implements ProtocolCapabilityAuthority {
        private final Map<String, Publication> values = new HashMap<>();

        private void put(final String workerId, final ProtocolCapabilityDeclaration declaration) {
            values.put(workerId, new Publication(declaration.capabilityEpoch(), declaration));
        }

        @Override
        public Publication publish(final ProtocolCapabilityDeclaration declaration, final long expectedRevision) {
            final Publication next = new Publication(expectedRevision + 1, declaration);
            values.put(declaration.workerId(), next);
            return next;
        }

        @Override
        public Optional<Publication> current(final String workerId) {
            return Optional.ofNullable(values.get(workerId));
        }

        @Override
        public boolean withdraw(final Publication expected) {
            return values.remove(expected.declaration().workerId(), expected);
        }
    }
}
