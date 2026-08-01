package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ControlOperationQueryResponseV1Test {
    @Test
    void currentAndTypedResultRoundTrip() {
        final ControlTargetStateViewV1 target = new ControlTargetStateViewV1(2,
                TargetMarkerStateV1.EFFECTIVE, StableCode.OK, 3, null);
        final CurrentControlOperationV1 current = new CurrentControlOperationV1(bytes(32, 1), bytes(32, 2),
                bytes(32, 3), ControlOperationStateV1.SUCCEEDED, 4, List.of(target),
                new ControlTypedResultV1(ControlResultKindV1.LANE,
                        CanonicalProtobuf.message(output -> CanonicalProtobuf.uint32(output, 1, 1))));
        final ControlOperationQueryResponseV1 response = ControlOperationQueryResponseV1.current(current);
        assertArrayEquals(response.canonicalBytes(),
                ControlOperationQueryResponseV1.decode(response.canonicalBytes()).canonicalBytes());
        assertArrayEquals(target.canonicalBytes(),
                ControlTargetStateViewV1.decode(target.canonicalBytes()).canonicalBytes());
    }

    @Test
    void errorBranchesAreClosedAndTypedResultIsRequiredForSuccess() {
        for (ControlOperationQueryResponseV1 response : List.of(
                ControlOperationQueryResponseV1.invalidReceipt(),
                ControlOperationQueryResponseV1.notFoundOrNotAuthorized(),
                ControlOperationQueryResponseV1.integrityError())) {
            assertArrayEquals(response.canonicalBytes(),
                    ControlOperationQueryResponseV1.decode(response.canonicalBytes()).canonicalBytes());
        }
        assertThrows(IllegalArgumentException.class, () -> new CurrentControlOperationV1(
                bytes(32, 1), bytes(32, 2), bytes(32, 3), ControlOperationStateV1.SUCCEEDED, 1,
                List.of(), null));
        final byte[] invalid = ControlOperationQueryResponseV1.invalidReceipt().canonicalBytes();
        invalid[invalid.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> ControlOperationQueryResponseV1.decode(invalid));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] result = new byte[length];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
