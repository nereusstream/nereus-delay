package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.PreparedCommand;

/** Generates the pre-I/O, nonzero Native Delivery ID required by Registry §6.3. */
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
        Bytes.requireLength(value, 32, "nativeDeliveryId");
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException("nativeDeliveryId must be non-zero");
    }
}
