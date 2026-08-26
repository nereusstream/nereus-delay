package com.nereusstream.delay.protocol;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Deterministic full-jitter projection used by retry decisions.
 *
 * <p>The digest preimage is fixed by the Protocol Registry. This class only
 * derives the bounded jitter amount; policy publication and source-ordered
 * application remain runtime concerns.</p>
 */
public final class RetryJitter {
    public static final int MESSAGE_PUBLISH = 1;
    public static final int DLQ_EXPORT = 2;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-retry");

    private RetryJitter() {}

    /**
     * Returns the inclusive full-jitter sample in {@code [0, maxBackoffMs]}.
     * The mapping is the Protocol Registry's {@code floor(r * (cap + 1) /
     * 2^64)} projection. The unsigned high half of the product is computed
     * with checked Java 21 arithmetic; no floating point or wrapped 64-bit
     * multiplication is used.
     */
    public static long delayMs(
            final int retryDomain,
            final DelayMessageId messageId,
            final long generation,
            final long attemptNo,
            final long maxBackoffMs) {
        if (retryDomain != MESSAGE_PUBLISH && retryDomain != DLQ_EXPORT) {
            throw new IllegalArgumentException("unsupported retry domain: " + retryDomain);
        }
        Objects.requireNonNull(messageId, "messageId");
        if (generation < 0
                || generation > 0xffff_ffffL
                || attemptNo <= 0
                || attemptNo > 0xffff_ffffL
                || maxBackoffMs < 0) {
            throw new IllegalArgumentException("invalid retry jitter inputs");
        }
        final byte[] digest = Bytes.sha256(
                DOMAIN, Bytes.u8(retryDomain), messageId.bytes(), Bytes.u32be(generation), Bytes.u32be(attemptNo));
        final long sample = ByteBuffer.wrap(digest).getLong();
        if (maxBackoffMs == 0) {
            return 0;
        }
        // cap + 1 is 2^63 when cap is Long.MAX_VALUE. The registry scaling
        // therefore reduces to floor(r / 2), which is representable directly.
        if (maxBackoffMs == Long.MAX_VALUE) {
            return sample >>> 1;
        }
        final long span = Math.addExact(maxBackoffMs, 1);
        final long unsignedHigh = Math.multiplyHigh(sample, span) + (sample < 0 ? span : 0);
        return unsignedHigh;
    }
}
