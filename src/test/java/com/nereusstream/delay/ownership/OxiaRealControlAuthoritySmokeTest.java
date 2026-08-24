package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorV1;
import com.nereusstream.delay.protocol.ControlOperationQueryResultV1;
import com.nereusstream.delay.protocol.ControlOperationReceiptV1;
import com.nereusstream.delay.protocol.ControlOperationRequestV1;
import com.nereusstream.delay.protocol.ControlOperationStateV1;
import com.nereusstream.delay.protocol.ControlReasonKindV1;
import com.nereusstream.delay.protocol.ControlReasonV1;
import com.nereusstream.delay.protocol.ControlTargetKindV1;
import com.nereusstream.delay.protocol.ControlTargetRefV1;
import com.nereusstream.delay.protocol.CurrentControlOperationV1;
import com.nereusstream.delay.protocol.ForceCheckpointRequestV1;
import com.nereusstream.delay.protocol.PreparedControlOperationV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import io.oxia.client.api.exceptions.OxiaException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Opt-in real Oxia coverage for the single-record control authorities. */
@Tag("real-service")
class OxiaRealControlAuthoritySmokeTest {
    @Test
    void controlOperationCasAndReopenWorkAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-control/" + UUID.randomUUID();
        final ControlOperationReceiptV1 receipt = receipt(1, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final CurrentControlOperationV1 next = current(receipt, 2, ControlOperationStateV1.DISPATCHING);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(endpoint, prefix + "/client")) {
            final OxiaSyncControlOperationBackend backend =
                    new OxiaSyncControlOperationBackend(client, prefix + "/operation");
            assertEquals(
                    ControlOperationQueryResultV1.CURRENT,
                    backend.register(receipt, initial).resultKind());
            assertEquals(
                    ControlOperationQueryResultV1.CURRENT,
                    backend.advance(receipt, 1, next).resultKind());
            final OxiaSyncControlOperationBackend reopened =
                    new OxiaSyncControlOperationBackend(client, prefix + "/operation");
            assertEquals(next, reopened.query(receipt, 2_000).current());
        }
    }

    @Test
    void controlTargetRegistrationCasAndReopenWorkAgainstRealService() throws Exception {
        final String endpoint = endpoint();
        final String prefix = "nereus-delay-real-control-target/" + UUID.randomUUID();
        final PreparedControlOperationV1 prepared = prepared(7);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(endpoint, prefix + "/client")) {
            final OxiaSyncControlTargetRegistrationBackend backend =
                    new OxiaSyncControlTargetRegistrationBackend(client, prefix + "/target");
            assertEquals(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, backend.register(prepared));
            final OxiaSyncControlTargetRegistrationBackend reopened =
                    new OxiaSyncControlTargetRegistrationBackend(client, prefix + "/target");
            assertEquals(prepared, reopened.find(prepared.operationId()).orElseThrow());
        }
    }

    @Test
    void freshProcessPhaseReopensDurableControlAuthority() throws Exception {
        final String phase = System.getenv("NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PHASE");
        final String prefix = System.getenv("NEREUS_DELAY_FRESH_PROCESS_AUTHORITY_PREFIX");
        Assumptions.assumeTrue(
                phase != null && (phase.equals("WRITE") || phase.equals("READ")),
                "fresh-process authority phase is not configured");
        Assumptions.assumeTrue(prefix != null && !prefix.isBlank(), "fresh-process authority prefix is not configured");
        final String endpoint = endpoint();
        final ControlOperationReceiptV1 receipt = receipt(77, 4_000);
        final CurrentControlOperationV1 initial = current(receipt, 1, ControlOperationStateV1.PENDING);
        final CurrentControlOperationV1 next = current(receipt, 2, ControlOperationStateV1.DISPATCHING);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(endpoint, prefix + "/control-client")) {
            final OxiaSyncControlOperationBackend backend =
                    new OxiaSyncControlOperationBackend(client, prefix + "/control");
            if (phase.equals("WRITE")) {
                assertEquals(
                        ControlOperationQueryResultV1.CURRENT,
                        backend.register(receipt, initial).resultKind());
                assertEquals(
                        ControlOperationQueryResultV1.CURRENT,
                        backend.advance(receipt, 1, next).resultKind());
                System.out.println("fresh-process control authority write phase passed");
            } else {
                assertEquals(next, backend.query(receipt, 4_000).current());
                System.out.println("fresh-process control authority read phase passed");
            }
        }
    }

    private static String endpoint() {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        return endpoint;
    }

    private static OxiaSyncOwnerLeaseBackend.ClientHandle connect(final String endpoint, final String identifier)
            throws OxiaException {
        return OxiaSyncOwnerLeaseBackend.connect(endpoint, "default", identifier, Duration.ofSeconds(15), "real-smoke");
    }

    private static ControlOperationReceiptV1 receipt(final int seed, final long queryUntil) {
        final var registered = new com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence(
                1_000,
                1_100,
                com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("control-clock" + seed),
                1,
                1,
                1,
                bytes(32, seed + 10),
                0,
                null);
        return ControlOperationReceiptV1.create(
                bytes(32, seed),
                bytes(32, seed + 1),
                bytes(32, seed + 2),
                bytes(32, seed + 3),
                1,
                registered,
                queryUntil);
    }

    private static CurrentControlOperationV1 current(
            final ControlOperationReceiptV1 receipt, final long revision, final ControlOperationStateV1 state) {
        return new CurrentControlOperationV1(
                receipt.operationId(),
                receipt.requestHash(),
                receipt.authenticatedScopeHash(),
                state,
                revision,
                List.of(),
                null);
    }

    private static PreparedControlOperationV1 prepared(final int seed) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, seed + 1)), seed);
        final ControlTargetRefV1 target =
                new ControlTargetRefV1(0, ControlTargetKindV1.SHARD, new ShardSubjectV1(shardId), null, null);
        return PreparedControlOperationV1.prepare(
                bytes(32, seed),
                request.kind(),
                new ControlAuthorV1(bytes(32, seed + 2), bytes(32, seed + 3), bytes(32, seed + 4)),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
