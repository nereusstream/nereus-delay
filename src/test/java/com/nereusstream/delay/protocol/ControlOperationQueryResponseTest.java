package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class ControlOperationQueryResponseTest {
    @Test
    void currentAndTypedResultRoundTrip() {
        final ControlTargetStateView target =
                new ControlTargetStateView(2, TargetMarkerState.EFFECTIVE, StableCode.OK, 3, null);
        final CurrentControlOperation current = new CurrentControlOperation(
                bytes(32, 1),
                bytes(32, 2),
                bytes(32, 3),
                ControlOperationState.SUCCEEDED,
                4,
                List.of(target),
                new ControlTypedResult(
                        ControlResultKind.LANE,
                        new LaneControlResult(
                                        new DestinationLaneId(bytes(32, 4)),
                                        bytes(16, 5),
                                        6,
                                        LaneAdmissionGate.OPEN,
                                        0,
                                        StableCode.OK)
                                .canonicalBytes()));
        final ControlOperationQueryResponse response = ControlOperationQueryResponse.current(current);
        assertArrayEquals(
                response.canonicalBytes(),
                ControlOperationQueryResponse.decode(response.canonicalBytes()).canonicalBytes());
        assertArrayEquals(
                target.canonicalBytes(),
                ControlTargetStateView.decode(target.canonicalBytes()).canonicalBytes());
    }

    @Test
    void errorBranchesAreClosedAndTypedResultIsRequiredForSuccess() {
        for (ControlOperationQueryResponse response : List.of(
                ControlOperationQueryResponse.invalidReceipt(),
                ControlOperationQueryResponse.notFoundOrNotAuthorized(),
                ControlOperationQueryResponse.integrityError())) {
            assertArrayEquals(
                    response.canonicalBytes(),
                    ControlOperationQueryResponse.decode(response.canonicalBytes())
                            .canonicalBytes());
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> new CurrentControlOperation(
                        bytes(32, 1), bytes(32, 2), bytes(32, 3), ControlOperationState.SUCCEEDED, 1, List.of(), null));
        final byte[] invalid = ControlOperationQueryResponse.invalidReceipt().canonicalBytes();
        invalid[invalid.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> ControlOperationQueryResponse.decode(invalid));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
