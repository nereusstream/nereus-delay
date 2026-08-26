package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlRegistrationBindingTest {
    @Test
    void bindsRecordedDefinitiveAndUncertainOutcomes() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] scope = bytes(32, 1);
        final ControlOperationRequest request = ControlOperationRequest.forceCheckpoint(
                new ForceCheckpointRequest(new ControlReason(ControlReasonKind.MAINTENANCE, null, null)));
        final ControlTargetRef target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(new ShardId(new RouteIncarnation(bytes(16, 2)), 0)),
                null,
                null);
        final PreparedControlOperation prepared = PreparedControlOperation.prepare(
                bytes(32, 3),
                request.kind(),
                new ControlAuthor(bytes(32, 4), bytes(32, 5), scope),
                request,
                List.of(target),
                1,
                2,
                1,
                keyPair.getPrivate());
        final TrustedUtcIntervalEvidence time = new TrustedUtcIntervalEvidence(
                100,
                110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                2,
                3,
                bytes(32, 6),
                0,
                new byte[0]);
        final ControlOperationReceipt receipt = ControlOperationReceipt.create(
                prepared.operationId(), prepared.requestHash(), scope, prepared.targetSnapshotHash(), 1, time, 200);
        assertDoesNotThrow(() ->
                ControlRegistrationBinding.validate(prepared, ControlRegistrationOutcomeMessage.recorded(receipt)));

        final StableError error = StableError.of(FailureStage.CONTROL, StableCode.UNAUTHORIZED, null, null, null, null);
        final ControlNonPersistenceProof proof = ControlNonPersistenceProof.create(
                ControlNonPersistenceProofKind.BEFORE_OXIA_OWNERSHIP,
                prepared.operationId(),
                prepared.preparedDigest(),
                null,
                null);
        assertDoesNotThrow(() -> ControlRegistrationBinding.validate(
                prepared,
                ControlRegistrationOutcomeMessage.definitelyNotRecorded(
                        new ControlDefinitelyNotRecorded(prepared.preparedDigest(), proof, error))));
        assertDoesNotThrow(() -> ControlRegistrationBinding.validate(
                prepared,
                ControlRegistrationOutcomeMessage.recordUncertain(
                        new ControlRecordUncertain(prepared.operationId(), prepared.preparedDigest(), error))));

        final ControlOperationReceipt wrongReceipt = ControlOperationReceipt.create(
                prepared.operationId(), bytes(32, 7), scope, prepared.targetSnapshotHash(), 1, time, 200);
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlRegistrationBinding.validate(
                        prepared, ControlRegistrationOutcomeMessage.recorded(wrongReceipt)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
