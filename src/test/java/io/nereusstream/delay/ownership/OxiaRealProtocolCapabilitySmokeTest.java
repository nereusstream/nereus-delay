package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ProtocolCapabilityDeclarationV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ProtocolActivationStateV1;
import io.nereusstream.delay.protocol.ProtocolTupleV1;
import io.nereusstream.delay.protocol.ProtocolVersionActivatePayloadV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in real Oxia proof for external Worker capability-gated activation. */
@Tag("real-service")
class OxiaRealProtocolCapabilitySmokeTest {
    @Test
    void eligibleReaderSetIsSessionBoundAndActivationEvidenceIsExact() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-protocol-capability/" + UUID.randomUUID();
        final ProtocolTupleV1 tuple = new ProtocolTupleV1(1, 1, ProtocolTupleV1.CLIENT_COMMAND, 1, 1);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle session = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, "default", "nereus-delay-protocol-capability-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/session")) {
            final OxiaSyncProtocolCapabilityBackend authority =
                    new OxiaSyncProtocolCapabilityBackend(session, prefix + "/capabilities");
            authority.publish(declaration("worker-a", tuple, session.backend().connectedSessionIdentity(), 1), 0);
            authority.publish(declaration("worker-b", tuple, session.backend().connectedSessionIdentity(), 2), 0);

            final ProtocolActivationAuthorityCoordinator coordinator =
                    new ProtocolActivationAuthorityCoordinator(authority);
            final var evidence = coordinator.requireEligibleReaders(tuple, List.of("worker-b", "worker-a"));
            final ProtocolVersionActivatePayloadV1 payload = new ProtocolVersionActivatePayloadV1(
                    tuple, bytes(32, 90), evidence.evidenceHash());
            final var authorized = coordinator.authorize(payload, List.of("worker-a", "worker-b"));
            assertArrayEquals(evidence.evidenceHash(), authorized.evidenceHash());
            final ShardId shard = new ShardId(new RouteIncarnation(bytes(16, 60)), 0);
            final KafkaSourcePosition markerPosition = new KafkaSourcePosition(shard, "activation-cluster",
                    UUID.fromString("12345678-1234-4abc-8def-1234567890ab"), 7, 0, 8);
            final ProtocolActivationStateV1 marker = new ProtocolActivationStateV1(
                    new ShardSubjectV1(shard), List.of()).activate(tuple, bytes(32, 90),
                    authorized.evidenceHash(), markerPosition, bytes(32, 91));
            assertArrayEquals(authorized.evidenceHash(), marker.activation(tuple)
                    .compatibleReaderSetEvidenceHash());
            System.out.println("Oxia protocol capability authority passed: eligibleReaders=2, "
                    + "capabilityBeforeActivationMarker=true, activationEvidence=exact, sessionBound=true");

            final var workerB = authority.current("worker-b").orElseThrow();
            assertTrue(authority.withdraw(workerB));
            assertThrows(IllegalStateException.class,
                    () -> coordinator.authorize(payload, List.of("worker-a", "worker-b")));
        }

        try (OxiaSyncOwnerLeaseBackend.ClientHandle replacement = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, "default", "nereus-delay-protocol-capability-replacement-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix + "/replacement")) {
            final OxiaSyncProtocolCapabilityBackend reopened =
                    new OxiaSyncProtocolCapabilityBackend(replacement, prefix + "/capabilities");
            assertTrue(reopened.current("worker-a").isEmpty());
        }
    }

    private static ProtocolCapabilityDeclarationV1 declaration(final String workerId,
                                                               final ProtocolTupleV1 tuple,
                                                               final byte[] sessionIdentity,
                                                               final int seed) {
        return new ProtocolCapabilityDeclarationV1(workerId, bytes(32, seed), List.of(tuple), seed,
                sessionIdentity);
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] result = new byte[length];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) (seed + index);
        }
        return result;
    }
}
