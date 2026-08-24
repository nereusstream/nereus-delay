package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ControlAuthorV1;
import com.nereusstream.delay.protocol.ControlOperationRequestV1;
import com.nereusstream.delay.protocol.ControlReasonKindV1;
import com.nereusstream.delay.protocol.ControlReasonV1;
import com.nereusstream.delay.protocol.ControlTargetKindV1;
import com.nereusstream.delay.protocol.ControlTargetRefV1;
import com.nereusstream.delay.protocol.ForceCheckpointRequestV1;
import com.nereusstream.delay.protocol.PreparedControlOperationV1;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubjectV1;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlTargetRegistrationAuthorityTest {
    @Test
    void registersExactPreparedBytesIdempotentlyAndRejectsConflicts() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemoryControlTargetRegistrationAuthority authority = new InMemoryControlTargetRegistrationAuthority();
        final PreparedControlOperationV1 prepared = prepared(keyPair, 1);
        assertEquals(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, authority.register(prepared));
        assertEquals(
                ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED,
                authority.register(PreparedControlOperationV1.decode(prepared.canonicalBytes())));
        assertEquals(prepared, authority.find(prepared.operationId()).orElseThrow());

        final PreparedControlOperationV1 conflict = preparedWithOperationId(keyPair, bytes(32, 99), 2);
        final PreparedControlOperationV1 sameIdDifferentBytes = PreparedControlOperationV1.prepare(
                prepared.operationId(),
                conflict.kind(),
                conflict.author(),
                conflict.request(),
                conflict.targets(),
                conflict.controlQueryPolicyVersion(),
                conflict.registrationRetryUntil(),
                conflict.signingKeyVersion(),
                keyPair.getPrivate());
        assertThrows(IllegalArgumentException.class, () -> authority.register(sameIdDifferentBytes));
    }

    private static PreparedControlOperationV1 prepared(final KeyPair keyPair, final int seed) {
        return preparedWithOperationId(keyPair, bytes(32, seed), seed);
    }

    private static PreparedControlOperationV1 preparedWithOperationId(
            final KeyPair keyPair, final byte[] operationId, final int seed) {
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, seed + 1)), seed);
        final ControlTargetRefV1 target =
                new ControlTargetRefV1(0, ControlTargetKindV1.SHARD, new ShardSubjectV1(shardId), null, null);
        return PreparedControlOperationV1.prepare(
                operationId,
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
