package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.PreparedCommand;

/**
 * Generates a pre-I/O nonzero AUTO_FAST attempt seed. The final Native
 * Delivery ID is derived from this seed and the exact record context.
 */
@FunctionalInterface
public interface NativeDeliveryIdGenerator {
    byte[] next(PreparedCommand managedCommand, CanonicalScheduleIntent intent);

    static NativeDeliveryIdGenerator random() {
        final java.security.SecureRandom random = new java.security.SecureRandom();
        return (command, schedule) -> {
            final byte[] id = new byte[32];
            random.nextBytes(id);
            return id;
        };
    }

    static byte[] require(final byte[] value) {
        Bytes.requireLength(value, 32, "nativeDeliveryAttemptSeed");
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException("nativeDeliveryAttemptSeed must be non-zero");
    }
}
