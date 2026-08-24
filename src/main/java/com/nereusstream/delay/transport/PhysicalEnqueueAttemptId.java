package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.Bytes;
import java.security.SecureRandom;

/** Non-zero process-local identity for one physical enqueue invocation. */
public final class PhysicalEnqueueAttemptId extends Bytes16 {
    private static final SecureRandom RANDOM = new SecureRandom();

    public PhysicalEnqueueAttemptId(final byte[] value) {
        super(value);
        requireNonZero(value);
    }

    public static PhysicalEnqueueAttemptId random() {
        final byte[] value = new byte[LENGTH];
        do {
            RANDOM.nextBytes(value);
        } while (allZero(value));
        return new PhysicalEnqueueAttemptId(value);
    }

    public static PhysicalEnqueueAttemptId require(final byte[] value) {
        return new PhysicalEnqueueAttemptId(value);
    }

    private static void requireNonZero(final byte[] value) {
        if (allZero(value)) {
            throw new IllegalArgumentException("physicalEnqueueAttemptId must be non-zero");
        }
    }

    private static boolean allZero(final byte[] value) {
        final byte[] checked = Bytes.copy(value);
        for (byte item : checked) {
            if (item != 0) {
                return false;
            }
        }
        return true;
    }
}
