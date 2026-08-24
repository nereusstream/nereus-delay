package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimensionV1;
import com.nereusstream.delay.protocol.CapacityGrantKindV1;
import com.nereusstream.delay.protocol.CapacityGrantV1;
import com.nereusstream.delay.protocol.CapacityVectorV1;
import com.nereusstream.delay.protocol.PublishAdmissionBody;
import com.nereusstream.delay.protocol.QuotaGrantRefV1;
import com.nereusstream.delay.protocol.ShardCapacityEnvelopeV1;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerCapacityAdmissionTest {
    @Test
    void sumsCommittedEnvelopeOnceWithoutDoubleCountingComponentGrants() {
        final ShardCapacityEnvelopeV1 envelope = envelope("one", 7, 3);
        final CapacityVectorV1 fixed = vector(CapacityDimensionV1.DB_INSTANCES, 2);
        final CapacityVectorV1 transition = vector(CapacityDimensionV1.CHECKPOINT_CREATE_TEMP_BYTES, 4);
        final CapacityVectorV1 hardCaps = vector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, 7)
                .add(vector(CapacityDimensionV1.DB_INSTANCES, 5))
                .add(vector(CapacityDimensionV1.CHECKPOINT_CREATE_TEMP_BYTES, 4));

        assertEquals(
                7,
                WorkerCapacityAdmission.sumCommitted(List.of(envelope))
                        .amount(CapacityDimensionV1.CONTROL_RESERVE_BYTES));
        WorkerCapacityAdmission.requireFits(hardCaps, List.of(envelope), fixed, transition);
    }

    @Test
    void rejectsDuplicateEnvelopeIdentityHardCapOverflowAndCheckedArithmeticOverflow() {
        final ShardCapacityEnvelopeV1 envelope = envelope("duplicate", 1, 0);
        assertThrows(
                IllegalArgumentException.class,
                () -> WorkerCapacityAdmission.sumCommitted(List.of(envelope, envelope)));
        assertThrows(
                IllegalArgumentException.class,
                () -> WorkerCapacityAdmission.requireFits(
                        CapacityVectorV1.empty(),
                        List.of(envelope),
                        CapacityVectorV1.empty(),
                        CapacityVectorV1.empty()));

        final ShardCapacityEnvelopeV1 max = envelope("max", Long.MAX_VALUE, 0);
        assertThrows(
                ArithmeticException.class,
                () -> WorkerCapacityAdmission.required(
                        List.of(max), vector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, 1), CapacityVectorV1.empty()));
    }

    private static ShardCapacityEnvelopeV1 envelope(
            final String identity, final long controlBytes, final long dbInstances) {
        final PublishAdmissionBody.ChargeVector logicalLimit =
                new PublishAdmissionBody.ChargeVector(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        final QuotaGrantRefV1 logical =
                new QuotaGrantRefV1(Bytes.sha256(Bytes.utf8(identity + "-logical")), 1, logicalLimit);
        final long[] committedAmounts = new long[CapacityDimensionV1.COUNT];
        committedAmounts[CapacityDimensionV1.CONTROL_RESERVE_BYTES.wireValue() - 1] = controlBytes;
        committedAmounts[CapacityDimensionV1.DB_INSTANCES.wireValue() - 1] = dbInstances;
        final CapacityVectorV1 committed = new CapacityVectorV1(committedAmounts);
        final long outcomeBytes = Math.min(3, controlBytes);
        final CapacityVectorV1 outcome = vector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, outcomeBytes);
        final CapacityVectorV1 nonOutcome =
                vector(CapacityDimensionV1.CONTROL_RESERVE_BYTES, controlBytes - outcomeBytes);
        return new ShardCapacityEnvelopeV1(
                Bytes.sha256(Bytes.utf8(identity + "-envelope")),
                1,
                logical,
                committed,
                grant(CapacityGrantKindV1.OUTCOME_RESERVE, identity + "-outcome", outcome),
                grant(CapacityGrantKindV1.NON_OUTCOME_CONTROL, identity + "-non-outcome", nonOutcome),
                grant(CapacityGrantKindV1.RECOVERY_WORKING, identity + "-recovery", CapacityVectorV1.empty()),
                grant(CapacityGrantKindV1.EMERGENCY_HEADROOM, identity + "-emergency", CapacityVectorV1.empty()),
                Bytes.sha256(Bytes.utf8(identity + "-artifact")));
    }

    private static CapacityGrantV1 grant(
            final CapacityGrantKindV1 kind, final String identity, final CapacityVectorV1 vector) {
        return new CapacityGrantV1(kind, Bytes.sha256(Bytes.utf8(identity)), 1, vector);
    }

    private static CapacityVectorV1 vector(final CapacityDimensionV1 dimension, final long amount) {
        final long[] amounts = new long[CapacityDimensionV1.COUNT];
        amounts[dimension.wireValue() - 1] = amount;
        return new CapacityVectorV1(amounts);
    }
}
