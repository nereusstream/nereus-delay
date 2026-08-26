package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ControlRegistrationOutcomeCodecTest {
    @Test
    void roundTripsAllRegistrationOutcomeBranches() {
        final byte[] operationId = bytes(32, 1);
        final byte[] preparedDigest = bytes(32, 2);
        final StableError error = StableError.of(FailureStage.CONTROL, StableCode.UNAUTHORIZED, null, null, null, 7);
        final ControlNonPersistenceProof before = ControlNonPersistenceProof.create(
                ControlNonPersistenceProofKind.BEFORE_OXIA_OWNERSHIP, operationId, preparedDigest, null, null);
        final ControlNonPersistenceProof conditional = ControlNonPersistenceProof.create(
                ControlNonPersistenceProofKind.OXIA_CONDITIONAL_REJECTION,
                operationId,
                preparedDigest,
                bytes(32, 3),
                bytes(32, 4));
        final ControlDefinitelyNotRecorded rejected = new ControlDefinitelyNotRecorded(preparedDigest, before, error);
        final ControlRecordUncertain uncertain = new ControlRecordUncertain(
                operationId,
                preparedDigest,
                StableError.of(FailureStage.CONTROL, StableCode.SHARD_UNAVAILABLE, null, null, null, null));
        final ControlOperationReceipt receipt = ControlOperationReceipt.create(
                operationId, bytes(32, 5), bytes(32, 6), bytes(32, 7), 1, trustedTime(), 2_000);

        assertEquals(before, ControlNonPersistenceProof.decode(before.canonicalBytes()));
        assertEquals(conditional, ControlNonPersistenceProof.decode(conditional.canonicalBytes()));
        assertEquals(rejected, ControlDefinitelyNotRecorded.decode(rejected.canonicalBytes()));
        assertEquals(uncertain, ControlRecordUncertain.decode(uncertain.canonicalBytes()));
        assertEquals(
                ControlRegistrationOutcomeMessage.recorded(receipt),
                ControlRegistrationOutcomeMessage.decode(
                        ControlRegistrationOutcomeMessage.recorded(receipt).canonicalBytes()));
        assertEquals(
                ControlRegistrationOutcomeMessage.definitelyNotRecorded(rejected),
                ControlRegistrationOutcomeMessage.decode(
                        ControlRegistrationOutcomeMessage.definitelyNotRecorded(rejected)
                                .canonicalBytes()));
        assertEquals(
                ControlRegistrationOutcomeMessage.recordUncertain(uncertain),
                ControlRegistrationOutcomeMessage.decode(ControlRegistrationOutcomeMessage.recordUncertain(uncertain)
                        .canonicalBytes()));
    }

    @Test
    void rejectsIllegalProofEvidenceAndBranchMismatch() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlNonPersistenceProof.create(
                        ControlNonPersistenceProofKind.BEFORE_OXIA_OWNERSHIP,
                        bytes(32, 1),
                        bytes(32, 2),
                        bytes(32, 3),
                        null));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlNonPersistenceProof.create(
                        ControlNonPersistenceProofKind.OXIA_CONDITIONAL_REJECTION,
                        bytes(32, 1),
                        bytes(32, 2),
                        null,
                        bytes(32, 4)));
        final ControlNonPersistenceProof proof = ControlNonPersistenceProof.create(
                ControlNonPersistenceProofKind.BEFORE_OXIA_OWNERSHIP, bytes(32, 5), bytes(32, 6), null, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlDefinitelyNotRecorded(
                        bytes(32, 7),
                        proof,
                        StableError.of(FailureStage.CONTROL, StableCode.UNAUTHORIZED, null, null, null, null)));
        final ControlRecordUncertain uncertain = new ControlRecordUncertain(
                bytes(32, 8),
                bytes(32, 9),
                StableError.of(FailureStage.CONTROL, StableCode.SHARD_UNAVAILABLE, null, null, null, null));
        final byte[] wrongBranch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ControlRegistrationOutcome.RECORDED.wireValue());
            CanonicalProtobuf.bytes(output, 12, uncertain.canonicalBytes());
        });
        assertThrows(IllegalArgumentException.class, () -> ControlRegistrationOutcomeMessage.decode(wrongBranch));
    }

    private static TrustedUtcIntervalEvidence trustedTime() {
        return new TrustedUtcIntervalEvidence(
                1_000,
                1_010,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("clock"),
                1,
                2,
                3,
                bytes(32, 20),
                0,
                new byte[0]);
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
