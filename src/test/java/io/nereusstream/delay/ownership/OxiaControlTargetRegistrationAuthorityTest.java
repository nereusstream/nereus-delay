package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ControlAuthorV1;
import io.nereusstream.delay.protocol.ControlOperationRequestV1;
import io.nereusstream.delay.protocol.ControlReasonKindV1;
import io.nereusstream.delay.protocol.ControlReasonV1;
import io.nereusstream.delay.protocol.ControlTargetKindV1;
import io.nereusstream.delay.protocol.ControlTargetRefV1;
import io.nereusstream.delay.protocol.ForceCheckpointRequestV1;
import io.nereusstream.delay.protocol.PreparedControlOperationV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.SystemMutation;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaControlTargetRegistrationAuthorityTest {
    @Test
    void adapterRequiresExactPreparedRereadAfterCas() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperationV1 prepared = prepared(keyPair, 1);
        final InMemoryControlTargetRegistrationAuthority local = new InMemoryControlTargetRegistrationAuthority();
        final OxiaControlTargetRegistrationAuthority authority = new OxiaControlTargetRegistrationAuthority(
                new OxiaControlTargetRegistrationAuthority.CasBackend() {
                    @Override
                    public ControlTargetRegistrationAuthority.RegistrationResult register(
                            final PreparedControlOperationV1 value) {
                        return local.register(value);
                    }

                    @Override
                    public Optional<PreparedControlOperationV1> find(final byte[] operationId) {
                        return local.find(operationId);
                    }

                    @Override
                    public void validateMutation(final PreparedControlOperationV1 value,
                                                 final ControlTargetRefV1 target, final SystemMutation mutation) {
                        local.validateMutation(value, target, mutation);
                    }
                });
        assertEquals(ControlTargetRegistrationAuthority.RegistrationResult.RECORDED,
                authority.register(prepared));
        assertEquals(ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED,
                authority.register(PreparedControlOperationV1.decode(prepared.canonicalBytes())));
    }

    @Test
    void adapterRejectsMissingOrChangedRegistrationReread() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperationV1 prepared = prepared(keyPair, 2);
        final PreparedControlOperationV1 changed = prepared(keyPair, 3);
        final OxiaControlTargetRegistrationAuthority.CasBackend missing = new StubBackend(
                ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, Optional.empty());
        assertThrows(IllegalStateException.class,
                () -> new OxiaControlTargetRegistrationAuthority(missing).register(prepared));

        final OxiaControlTargetRegistrationAuthority.CasBackend drift = new StubBackend(
                ControlTargetRegistrationAuthority.RegistrationResult.RECORDED, Optional.of(changed));
        assertThrows(IllegalStateException.class,
                () -> new OxiaControlTargetRegistrationAuthority(drift).register(prepared));
    }

    @Test
    void adapterRejectsLookupForAnotherOperation() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperationV1 requested = prepared(keyPair, 4);
        final PreparedControlOperationV1 other = prepared(keyPair, 5);
        final OxiaControlTargetRegistrationAuthority.CasBackend backend = new StubBackend(
                ControlTargetRegistrationAuthority.RegistrationResult.ALREADY_RECORDED, Optional.of(other));
        assertThrows(IllegalStateException.class,
                () -> new OxiaControlTargetRegistrationAuthority(backend).find(requested.operationId()));
    }

    @Test
    void adapterKeepsLookupIdentitySnapshotWhenBackendMutatesBuffer() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperationV1 prepared = prepared(keyPair, 6);
        final byte[] requestedOperationId = prepared.operationId();
        final OxiaControlTargetRegistrationAuthority.CasBackend backend =
                new OxiaControlTargetRegistrationAuthority.CasBackend() {
                    @Override
                    public ControlTargetRegistrationAuthority.RegistrationResult register(
                            final PreparedControlOperationV1 ignored) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Optional<PreparedControlOperationV1> find(final byte[] operationId) {
                        operationId[0] ^= 0x44;
                        return Optional.of(prepared);
                    }

                    @Override
                    public void validateMutation(final PreparedControlOperationV1 ignored,
                                                 final ControlTargetRefV1 target, final SystemMutation mutation) {
                        throw new UnsupportedOperationException();
                    }
                };
        assertEquals(prepared,
                new OxiaControlTargetRegistrationAuthority(backend).find(requestedOperationId).orElseThrow());
        assertArrayEquals(prepared.operationId(), requestedOperationId);
    }

    private static PreparedControlOperationV1 prepared(final KeyPair keyPair, final int seed) {
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ShardId shardId = new ShardId(new RouteIncarnation(bytes(16, seed + 1)), seed);
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(shardId), null, null);
        return PreparedControlOperationV1.prepare(bytes(32, seed), request.kind(),
                new ControlAuthorV1(bytes(32, seed + 2), bytes(32, seed + 3), bytes(32, seed + 4)), request,
                List.of(target), 1, 2, 1, keyPair.getPrivate());
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }

    private record StubBackend(ControlTargetRegistrationAuthority.RegistrationResult result,
                               Optional<PreparedControlOperationV1> found)
            implements OxiaControlTargetRegistrationAuthority.CasBackend {
        @Override
        public ControlTargetRegistrationAuthority.RegistrationResult register(
                final PreparedControlOperationV1 prepared) {
            return result;
        }

        @Override
        public Optional<PreparedControlOperationV1> find(final byte[] operationId) {
            return found;
        }

        @Override
        public void validateMutation(final PreparedControlOperationV1 prepared,
                                     final ControlTargetRefV1 target, final SystemMutation mutation) {
            // Registration/lookup tests do not need a completed mutation.
        }
    }
}
