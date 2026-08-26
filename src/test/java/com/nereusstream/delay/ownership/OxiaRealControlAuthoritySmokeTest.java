package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlOperationQueryResult;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.ControlOperationRequest;
import com.nereusstream.delay.protocol.ControlOperationState;
import com.nereusstream.delay.protocol.ControlReason;
import com.nereusstream.delay.protocol.ControlReasonKind;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.CurrentControlOperation;
import com.nereusstream.delay.protocol.ForceCheckpointRequest;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
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
        final ControlOperationReceipt receipt = receipt(1, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final CurrentControlOperation next = current(receipt, 2, ControlOperationState.DISPATCHING);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(endpoint, prefix + "/client")) {
            final OxiaSyncControlOperationBackend backend =
                    new OxiaSyncControlOperationBackend(client, prefix + "/operation");
            assertEquals(
                    ControlOperationQueryResult.CURRENT,
                    backend.register(receipt, initial).resultKind());
            assertEquals(
                    ControlOperationQueryResult.CURRENT,
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
        final PreparedControlOperation prepared = prepared(7);

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
        final ControlOperationReceipt receipt = receipt(77, 4_000);
        final CurrentControlOperation initial = current(receipt, 1, ControlOperationState.PENDING);
        final CurrentControlOperation next = current(receipt, 2, ControlOperationState.DISPATCHING);

        try (OxiaSyncOwnerLeaseBackend.ClientHandle client = connect(endpoint, prefix + "/control-client")) {
            final OxiaSyncControlOperationBackend backend =
                    new OxiaSyncControlOperationBackend(client, prefix + "/control");
            if (phase.equals("WRITE")) {
                assertEquals(
                        ControlOperationQueryResult.CURRENT,
                        backend.register(receipt, initial).resultKind());
                assertEquals(
                        ControlOperationQueryResult.CURRENT,
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

    private static ControlOperationReceipt receipt(final int seed, final long queryUntil) {
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
        return ControlOperationReceipt.create(
                bytes(32, seed),
                bytes(32, seed + 1),
                bytes(32, seed + 2),
                bytes(32, seed + 3),
                1,
                registered,
                queryUntil);
    }

    private static CurrentControlOperation current(
            final ControlOperationReceipt receipt, final long revision, final ControlOperationState state) {
        return new CurrentControlOperation(
                receipt.operationId(),
                receipt.requestHash(),
                receipt.authenticatedScopeHash(),
                state,
                revision,
                List.of(),
                null);
    }

    private static PreparedControlOperation prepared(final int seed) throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final ControlOperationRequest request = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, seed + 1)), seed);
        final ControlTargetRef target =
                new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(shardId), null, null);
        return PreparedControlOperation.prepare(
                bytes(32, seed),
                request.kind(),
                new ControlAuthor(bytes(32, seed + 2), bytes(32, seed + 3), bytes(32, seed + 4)),
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
