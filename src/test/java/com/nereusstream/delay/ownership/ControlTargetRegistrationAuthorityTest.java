package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlOperationRequest;
import com.nereusstream.delay.protocol.ControlReason;
import com.nereusstream.delay.protocol.ControlReasonKind;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.ForceCheckpointRequest;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.ShardSubject;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlTargetRegistrationAuthorityTest {
    @Test
    void registersExactPreparedBytesIdempotentlyAndRejectsConflicts() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final InMemoryControlTargetRegistrationAuthority authority = new InMemoryControlTargetRegistrationAuthority();
        final PreparedControlOperation prepared = prepared(keyPair, 1);
        assertEquals(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, authority.register(prepared));
        assertEquals(
                ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED,
                authority.register(PreparedControlOperation.decode(prepared.canonicalBytes())));
        assertEquals(prepared, authority.find(prepared.operationId()).orElseThrow());

        final PreparedControlOperation conflict = preparedWithOperationId(keyPair, bytes(32, 99), 2);
        final PreparedControlOperation sameIdDifferentBytes = PreparedControlOperation.prepare(
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

    private static PreparedControlOperation prepared(final KeyPair keyPair, final int seed) {
        return preparedWithOperationId(keyPair, bytes(32, seed), seed);
    }

    private static PreparedControlOperation preparedWithOperationId(
            final KeyPair keyPair, final byte[] operationId, final int seed) {
        final ControlOperationRequest request = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, seed + 1)), seed);
        final ControlTargetRef target =
                new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(shardId), null, null);
        return PreparedControlOperation.prepare(
                operationId,
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
