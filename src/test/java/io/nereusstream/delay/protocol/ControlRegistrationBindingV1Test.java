package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlRegistrationBindingV1Test {
    @Test
    void bindsRecordedDefinitiveAndUncertainOutcomes() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final byte[] scope = bytes(32, 1);
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(new ShardId(new RouteIncarnation(bytes(16, 2)), 0)), null, null);
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(bytes(32, 3), request.kind(),
                new ControlAuthorV1(bytes(32, 4), bytes(32, 5), scope), request, List.of(target), 1, 2, 1,
                keyPair.getPrivate());
        final TrustedUtcIntervalEvidence time = new TrustedUtcIntervalEvidence(100, 110,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("clock"), 1, 2, 3,
                bytes(32, 6), 0, new byte[0]);
        final ControlOperationReceiptV1 receipt = ControlOperationReceiptV1.create(prepared.operationId(),
                prepared.requestHash(), scope, prepared.targetSnapshotHash(), 1, time, 200);
        assertDoesNotThrow(() -> ControlRegistrationBindingV1.validate(prepared,
                ControlRegistrationOutcomeMessageV1.recorded(receipt)));

        final StableErrorV1 error = StableErrorV1.of(FailureStageV1.CONTROL, StableCode.UNAUTHORIZED,
                null, null, null, null);
        final ControlNonPersistenceProofV1 proof = ControlNonPersistenceProofV1.create(
                ControlNonPersistenceProofKindV1.BEFORE_OXIA_OWNERSHIP, prepared.operationId(),
                prepared.preparedDigest(), null, null);
        assertDoesNotThrow(() -> ControlRegistrationBindingV1.validate(prepared,
                ControlRegistrationOutcomeMessageV1.definitelyNotRecorded(
                        new ControlDefinitelyNotRecordedV1(prepared.preparedDigest(), proof, error))));
        assertDoesNotThrow(() -> ControlRegistrationBindingV1.validate(prepared,
                ControlRegistrationOutcomeMessageV1.recordUncertain(
                        new ControlRecordUncertainV1(prepared.operationId(), prepared.preparedDigest(), error))));

        final ControlOperationReceiptV1 wrongReceipt = ControlOperationReceiptV1.create(prepared.operationId(),
                bytes(32, 7), scope, prepared.targetSnapshotHash(), 1, time, 200);
        assertThrows(IllegalArgumentException.class, () -> ControlRegistrationBindingV1.validate(prepared,
                ControlRegistrationOutcomeMessageV1.recorded(wrongReceipt)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
