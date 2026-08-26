package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import com.nereusstream.delay.protocol.SystemMutation;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class OxiaControlTargetRegistrationAuthorityTest {
    @Test
    void adapterRequiresExactPreparedRereadAfterCas() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperation prepared = prepared(keyPair, 1);
        final InMemoryControlTargetRegistrationAuthority local = new InMemoryControlTargetRegistrationAuthority();
        final OxiaControlTargetRegistrationAuthority authority =
                new OxiaControlTargetRegistrationAuthority(new OxiaControlTargetRegistrationAuthority.CasBackend() {
                    @Override
                    public ControlTargetRegistrationAuthority.RegistrationResult register(
                            final PreparedControlOperation value) {
                        return local.register(value);
                    }

                    @Override
                    public Optional<PreparedControlOperation> find(final byte[] operationId) {
                        return local.find(operationId);
                    }

                    @Override
                    public void validateMutation(
                            final PreparedControlOperation value,
                            final ControlTargetRef target,
                            final SystemMutation mutation) {
                        local.validateMutation(value, target, mutation);
                    }
                });
        assertEquals(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, authority.register(prepared));
        assertEquals(
                ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED,
                authority.register(PreparedControlOperation.decode(prepared.canonicalBytes())));
    }

    @Test
    void adapterRejectsMissingOrChangedRegistrationReread() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperation prepared = prepared(keyPair, 2);
        final PreparedControlOperation changed = prepared(keyPair, 3);
        final OxiaControlTargetRegistrationAuthority.CasBackend missing =
                new StubBackend(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, Optional.empty());
        assertThrows(IllegalStateException.class, () -> new OxiaControlTargetRegistrationAuthority(missing)
                .register(prepared));

        final OxiaControlTargetRegistrationAuthority.CasBackend drift =
                new StubBackend(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, Optional.of(changed));
        assertThrows(IllegalStateException.class, () -> new OxiaControlTargetRegistrationAuthority(drift)
                .register(prepared));
    }

    @Test
    void adapterRejectsLookupForAnotherOperation() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperation requested = prepared(keyPair, 4);
        final PreparedControlOperation other = prepared(keyPair, 5);
        final OxiaControlTargetRegistrationAuthority.CasBackend backend = new StubBackend(
                ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED, Optional.of(other));
        assertThrows(IllegalStateException.class, () -> new OxiaControlTargetRegistrationAuthority(backend)
                .find(requested.operationId()));
    }

    @Test
    void adapterKeepsLookupIdentitySnapshotWhenBackendMutatesBuffer() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperation prepared = prepared(keyPair, 6);
        final byte[] requestedOperationId = prepared.operationId();
        final OxiaControlTargetRegistrationAuthority.CasBackend backend =
                new OxiaControlTargetRegistrationAuthority.CasBackend() {
                    @Override
                    public ControlTargetRegistrationAuthority.RegistrationResult register(
                            final PreparedControlOperation ignored) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Optional<PreparedControlOperation> find(final byte[] operationId) {
                        operationId[0] ^= 0x44;
                        return Optional.of(prepared);
                    }

                    @Override
                    public void validateMutation(
                            final PreparedControlOperation ignored,
                            final ControlTargetRef target,
                            final SystemMutation mutation) {
                        throw new UnsupportedOperationException();
                    }
                };
        assertEquals(
                prepared,
                new OxiaControlTargetRegistrationAuthority(backend)
                        .find(requestedOperationId)
                        .orElseThrow());
        assertArrayEquals(prepared.operationId(), requestedOperationId);
    }

    private static PreparedControlOperation prepared(final KeyPair keyPair, final int seed) {
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

    private record StubBackend(
            ControlTargetRegistrationAuthority.RegistrationResult result, Optional<PreparedControlOperation> found)
            implements OxiaControlTargetRegistrationAuthority.CasBackend {
        @Override
        public ControlTargetRegistrationAuthority.RegistrationResult register(final PreparedControlOperation prepared) {
            return result;
        }

        @Override
        public Optional<PreparedControlOperation> find(final byte[] operationId) {
            return found;
        }

        @Override
        public void validateMutation(
                final PreparedControlOperation prepared, final ControlTargetRef target, final SystemMutation mutation) {
            // Registration/lookup tests do not need a completed mutation.
        }
    }
}
