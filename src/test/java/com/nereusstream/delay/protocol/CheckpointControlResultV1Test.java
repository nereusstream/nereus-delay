package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class CheckpointControlResultV1Test {
    @Test
    void roundTripsCheckpointControlResult() {
        final CheckpointControlResultV1 result = new CheckpointControlResultV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 1)), 7), bytes(16, 2), bytes(32, 3), 4);
        assertEquals(result, CheckpointControlResultV1.decode(result.canonicalBytes()));
    }

    @Test
    void catalogGenerationPreservesCompleteUnsigned64BitPattern() {
        final CheckpointControlResultV1 result = new CheckpointControlResultV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 1)), 7), bytes(16, 2), bytes(32, 3), Long.MIN_VALUE);

        final CheckpointControlResultV1 decoded = CheckpointControlResultV1.decode(result.canonicalBytes());
        assertEquals(Long.MIN_VALUE, decoded.catalogGeneration());
        assertEquals(result, decoded);
    }

    @Test
    void rejectsInvalidIdentityAndFieldOrder() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointControlResultV1(
                        new ShardSubjectV1(new RouteIncarnation(bytes(16, 1)), 7), new byte[16], bytes(32, 3), 4));
        final CheckpointControlResultV1 result = new CheckpointControlResultV1(
                new ShardSubjectV1(new RouteIncarnation(bytes(16, 1)), 7), bytes(16, 2), bytes(32, 3), 4);
        final byte[] malformed = Bytes.concat(result.canonicalBytes(), new byte[] {0x08, 0x01});
        assertThrows(IllegalArgumentException.class, () -> CheckpointControlResultV1.decode(malformed));
    }

    private static byte[] bytes(final int length, final int seed) {
        final byte[] value = new byte[length];
        for (int index = 0; index < length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
