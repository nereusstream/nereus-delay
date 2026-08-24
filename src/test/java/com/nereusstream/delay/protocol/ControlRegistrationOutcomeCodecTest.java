package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ControlRegistrationOutcomeCodecTest {
    @Test
    void roundTripsAllRegistrationOutcomeBranches() {
        final byte[] operationId = bytes(32, 1);
        final byte[] preparedDigest = bytes(32, 2);
        final StableErrorV1 error =
                StableErrorV1.of(FailureStageV1.CONTROL, StableCode.UNAUTHORIZED, null, null, null, 7);
        final ControlNonPersistenceProofV1 before = ControlNonPersistenceProofV1.create(
                ControlNonPersistenceProofKindV1.BEFORE_OXIA_OWNERSHIP, operationId, preparedDigest, null, null);
        final ControlNonPersistenceProofV1 conditional = ControlNonPersistenceProofV1.create(
                ControlNonPersistenceProofKindV1.OXIA_CONDITIONAL_REJECTION,
                operationId,
                preparedDigest,
                bytes(32, 3),
                bytes(32, 4));
        final ControlDefinitelyNotRecordedV1 rejected =
                new ControlDefinitelyNotRecordedV1(preparedDigest, before, error);
        final ControlRecordUncertainV1 uncertain = new ControlRecordUncertainV1(
                operationId,
                preparedDigest,
                StableErrorV1.of(FailureStageV1.CONTROL, StableCode.SHARD_UNAVAILABLE, null, null, null, null));
        final ControlOperationReceiptV1 receipt = ControlOperationReceiptV1.create(
                operationId, bytes(32, 5), bytes(32, 6), bytes(32, 7), 1, trustedTime(), 2_000);

        assertEquals(before, ControlNonPersistenceProofV1.decode(before.canonicalBytes()));
        assertEquals(conditional, ControlNonPersistenceProofV1.decode(conditional.canonicalBytes()));
        assertEquals(rejected, ControlDefinitelyNotRecordedV1.decode(rejected.canonicalBytes()));
        assertEquals(uncertain, ControlRecordUncertainV1.decode(uncertain.canonicalBytes()));
        assertEquals(
                ControlRegistrationOutcomeMessageV1.recorded(receipt),
                ControlRegistrationOutcomeMessageV1.decode(
                        ControlRegistrationOutcomeMessageV1.recorded(receipt).canonicalBytes()));
        assertEquals(
                ControlRegistrationOutcomeMessageV1.definitelyNotRecorded(rejected),
                ControlRegistrationOutcomeMessageV1.decode(
                        ControlRegistrationOutcomeMessageV1.definitelyNotRecorded(rejected)
                                .canonicalBytes()));
        assertEquals(
                ControlRegistrationOutcomeMessageV1.recordUncertain(uncertain),
                ControlRegistrationOutcomeMessageV1.decode(
                        ControlRegistrationOutcomeMessageV1.recordUncertain(uncertain)
                                .canonicalBytes()));
    }

    @Test
    void rejectsIllegalProofEvidenceAndBranchMismatch() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlNonPersistenceProofV1.create(
                        ControlNonPersistenceProofKindV1.BEFORE_OXIA_OWNERSHIP,
                        bytes(32, 1),
                        bytes(32, 2),
                        bytes(32, 3),
                        null));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlNonPersistenceProofV1.create(
                        ControlNonPersistenceProofKindV1.OXIA_CONDITIONAL_REJECTION,
                        bytes(32, 1),
                        bytes(32, 2),
                        null,
                        bytes(32, 4)));
        final ControlNonPersistenceProofV1 proof = ControlNonPersistenceProofV1.create(
                ControlNonPersistenceProofKindV1.BEFORE_OXIA_OWNERSHIP, bytes(32, 5), bytes(32, 6), null, null);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ControlDefinitelyNotRecordedV1(
                        bytes(32, 7),
                        proof,
                        StableErrorV1.of(FailureStageV1.CONTROL, StableCode.UNAUTHORIZED, null, null, null, null)));
        final ControlRecordUncertainV1 uncertain = new ControlRecordUncertainV1(
                bytes(32, 8),
                bytes(32, 9),
                StableErrorV1.of(FailureStageV1.CONTROL, StableCode.SHARD_UNAVAILABLE, null, null, null, null));
        final byte[] wrongBranch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, ControlRegistrationOutcomeV1.RECORDED.wireValue());
            CanonicalProtobuf.bytes(output, 12, uncertain.canonicalBytes());
        });
        assertThrows(IllegalArgumentException.class, () -> ControlRegistrationOutcomeMessageV1.decode(wrongBranch));
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
