package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlRequestSupportCodecTest {
    @Test
    void acknowledgementSetRoundTripsEmptyAndSortedEntries() {
        final AcknowledgementSetV1 empty = AcknowledgementSetV1.empty();
        assertEquals(empty, AcknowledgementSetV1.decode(empty.canonicalBytes()));
        final AcknowledgementSetV1 set = new AcknowledgementSetV1(List.of(
                new AcknowledgementV1(AcknowledgementKindV1.POSSIBLE_DUPLICATE, bytes(32, 1), bytes(32, 2)),
                new AcknowledgementV1(AcknowledgementKindV1.ORDER_LOSS, bytes(32, 3), bytes(32, 4))));
        assertEquals(set, AcknowledgementSetV1.decode(set.canonicalBytes()));
        assertTrue(set.has(AcknowledgementKindV1.POSSIBLE_DUPLICATE));
        assertFalse(set.has(AcknowledgementKindV1.POSSIBLE_DELIVERY));
    }

    @Test
    void acknowledgementSetRejectsDuplicateOrOutOfOrderKindsAndMalformedFields() {
        final AcknowledgementV1 first = new AcknowledgementV1(AcknowledgementKindV1.POSSIBLE_DUPLICATE,
                bytes(32, 1), bytes(32, 2));
        assertThrows(IllegalArgumentException.class, () -> new AcknowledgementSetV1(List.of(first, first)));
        assertThrows(IllegalArgumentException.class, () -> new AcknowledgementSetV1(List.of(
                new AcknowledgementV1(AcknowledgementKindV1.ORDER_LOSS, bytes(32, 1), bytes(32, 2)), first)));
        final byte[] malformed = CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 2, 1));
        assertThrows(IllegalArgumentException.class, () -> AcknowledgementSetV1.decode(malformed));
    }

    @Test
    void quotaTransferPlanAndControlEnumsRoundTrip() {
        final QuotaTransferPlanRefV1 plan = new QuotaTransferPlanRefV1(bytes(32, 5), bytes(32, 6), 7,
                bytes(32, 8));
        assertEquals(plan, QuotaTransferPlanRefV1.decode(plan.canonicalBytes()));
        assertEquals(ControlOperationKindV1.ROTATE_EQUIVALENT_SECRET_REFERENCE,
                ControlOperationKindV1.fromWire(15));
        assertEquals(ClosePolicyV1.V1_FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED,
                ClosePolicyV1.fromWire(1));
        assertEquals(UncertainResolutionKindV1.TERMINALIZE_POSSIBLE_DELIVERY,
                UncertainResolutionKindV1.fromWire(4));
    }

    @Test
    void supportValuesRejectInvalidWireNumbersAndNonZeroIds() {
        assertThrows(IllegalArgumentException.class, () -> AcknowledgementKindV1.fromWire(4));
        assertThrows(IllegalArgumentException.class, () -> ControlOperationKindV1.fromWire(16));
        assertThrows(IllegalArgumentException.class, () -> new QuotaTransferPlanRefV1(
                new byte[32], bytes(32, 1), 1, bytes(32, 2)));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
