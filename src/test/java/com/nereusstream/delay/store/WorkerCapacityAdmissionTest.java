package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityGrant;
import com.nereusstream.delay.protocol.CapacityGrantKind;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRef;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerCapacityAdmissionTest {
    @Test
    void sumsCommittedEnvelopeOnceWithoutDoubleCountingComponentGrants() {
        final ShardCapacityEnvelope envelope = envelope("one", 7, 3);
        final CapacityVector fixed = vector(CapacityDimension.DB_INSTANCES, 2);
        final CapacityVector transition = vector(CapacityDimension.CHECKPOINT_CREATE_TEMP_BYTES, 4);
        final CapacityVector hardCaps = vector(CapacityDimension.CONTROL_RESERVE_BYTES, 7)
                .add(vector(CapacityDimension.DB_INSTANCES, 5))
                .add(vector(CapacityDimension.CHECKPOINT_CREATE_TEMP_BYTES, 4));

        assertEquals(
                7,
                WorkerCapacityAdmission.sumCommitted(List.of(envelope))
                        .amount(CapacityDimension.CONTROL_RESERVE_BYTES));
        WorkerCapacityAdmission.requireFits(hardCaps, List.of(envelope), fixed, transition);
    }

    @Test
    void rejectsDuplicateEnvelopeIdentityHardCapOverflowAndCheckedArithmeticOverflow() {
        final ShardCapacityEnvelope envelope = envelope("duplicate", 1, 0);
        assertThrows(
                IllegalArgumentException.class,
                () -> WorkerCapacityAdmission.sumCommitted(List.of(envelope, envelope)));
        assertThrows(
                IllegalArgumentException.class,
                () -> WorkerCapacityAdmission.requireFits(
                        CapacityVector.empty(), List.of(envelope), CapacityVector.empty(), CapacityVector.empty()));

        final ShardCapacityEnvelope max = envelope("max", Long.MAX_VALUE, 0);
        assertThrows(
                ArithmeticException.class,
                () -> WorkerCapacityAdmission.required(
                        List.of(max), vector(CapacityDimension.CONTROL_RESERVE_BYTES, 1), CapacityVector.empty()));
    }

    private static ShardCapacityEnvelope envelope(
            final String identity, final long controlBytes, final long dbInstances) {
        final PublishAdmissionBody.ChargeVector logicalLimit =
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        final QuotaGrantRef logical =
                new QuotaGrantRef(Bytes.sha256(Bytes.utf8(identity + "-logical")), 1, logicalLimit);
        final long[] committedAmounts = new long[CapacityDimension.COUNT];
        committedAmounts[CapacityDimension.CONTROL_RESERVE_BYTES.wireValue() - 1] = controlBytes;
        committedAmounts[CapacityDimension.DB_INSTANCES.wireValue() - 1] = dbInstances;
        final CapacityVector committed = new CapacityVector(committedAmounts);
        final long outcomeBytes = Math.min(3, controlBytes);
        final CapacityVector outcome = vector(CapacityDimension.CONTROL_RESERVE_BYTES, outcomeBytes);
        final CapacityVector nonOutcome = vector(CapacityDimension.CONTROL_RESERVE_BYTES, controlBytes - outcomeBytes);
        return new ShardCapacityEnvelope(
                Bytes.sha256(Bytes.utf8(identity + "-envelope")),
                1,
                logical,
                committed,
                grant(CapacityGrantKind.OUTCOME_RESERVE, identity + "-outcome", outcome),
                grant(CapacityGrantKind.NON_OUTCOME_CONTROL, identity + "-non-outcome", nonOutcome),
                grant(CapacityGrantKind.RECOVERY_WORKING, identity + "-recovery", CapacityVector.empty()),
                grant(CapacityGrantKind.EMERGENCY_HEADROOM, identity + "-emergency", CapacityVector.empty()),
                Bytes.sha256(Bytes.utf8(identity + "-artifact")));
    }

    private static CapacityGrant grant(
            final CapacityGrantKind kind, final String identity, final CapacityVector vector) {
        return new CapacityGrant(kind, Bytes.sha256(Bytes.utf8(identity)), 1, vector);
    }

    private static CapacityVector vector(final CapacityDimension dimension, final long amount) {
        final long[] amounts = new long[CapacityDimension.COUNT];
        amounts[dimension.wireValue() - 1] = amount;
        return new CapacityVector(amounts);
    }
}
