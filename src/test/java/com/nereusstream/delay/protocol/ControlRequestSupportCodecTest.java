package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlRequestSupportCodecTest {
    @Test
    void acknowledgementSetRoundTripsEmptyAndSortedEntries() {
        final AcknowledgementSet empty = AcknowledgementSet.empty();
        assertEquals(empty, AcknowledgementSet.decode(empty.canonicalBytes()));
        final AcknowledgementSet set = new AcknowledgementSet(List.of(
                new Acknowledgement(AcknowledgementKind.POSSIBLE_DUPLICATE, bytes(32, 1), bytes(32, 2)),
                new Acknowledgement(AcknowledgementKind.ORDER_LOSS, bytes(32, 3), bytes(32, 4))));
        assertEquals(set, AcknowledgementSet.decode(set.canonicalBytes()));
        assertTrue(set.has(AcknowledgementKind.POSSIBLE_DUPLICATE));
        assertFalse(set.has(AcknowledgementKind.POSSIBLE_DELIVERY));
    }

    @Test
    void acknowledgementSetRejectsDuplicateOrOutOfOrderKindsAndMalformedFields() {
        final Acknowledgement first =
                new Acknowledgement(AcknowledgementKind.POSSIBLE_DUPLICATE, bytes(32, 1), bytes(32, 2));
        assertThrows(IllegalArgumentException.class, () -> new AcknowledgementSet(List.of(first, first)));
        assertThrows(
                IllegalArgumentException.class,
                () -> new AcknowledgementSet(List.of(
                        new Acknowledgement(AcknowledgementKind.ORDER_LOSS, bytes(32, 1), bytes(32, 2)), first)));
        final byte[] malformed = CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 2, 1));
        assertThrows(IllegalArgumentException.class, () -> AcknowledgementSet.decode(malformed));
    }

    @Test
    void quotaTransferPlanAndControlEnumsRoundTrip() {
        final QuotaTransferPlanRef plan = new QuotaTransferPlanRef(bytes(32, 5), bytes(32, 6), 7, bytes(32, 8));
        assertEquals(plan, QuotaTransferPlanRef.decode(plan.canonicalBytes()));
        assertEquals(ControlOperationKind.ROTATE_EQUIVALENT_SECRET_REFERENCE, ControlOperationKind.fromWire(15));
        assertEquals(ClosePolicy._FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED, ClosePolicy.fromWire(1));
        assertEquals(UncertainResolutionKind.TERMINALIZE_POSSIBLE_DELIVERY, UncertainResolutionKind.fromWire(4));
    }

    @Test
    void quotaTransferPlanPreservesCompleteUnsigned64BitPolicyVersion() {
        final QuotaTransferPlanRef plan =
                new QuotaTransferPlanRef(bytes(32, 31), bytes(32, 32), Long.MIN_VALUE, bytes(32, 33));

        final QuotaTransferPlanRef decoded = QuotaTransferPlanRef.decode(plan.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.tenantPolicyVersion());
        assertEquals(plan, decoded);
    }

    @Test
    void supportValuesRejectInvalidWireNumbersAndNonZeroIds() {
        assertThrows(IllegalArgumentException.class, () -> AcknowledgementKind.fromWire(4));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationKind.fromWire(16));
        assertThrows(
                IllegalArgumentException.class,
                () -> new QuotaTransferPlanRef(new byte[32], bytes(32, 1), 1, bytes(32, 2)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
