package com.nereusstream.delay.protocol;

/**
 * Source lock for the P1 Pulsar Resource Guard contract used by this build.
 * The lock text, rather than a mutable classpath version, is what enters the
 * ArtifactGenerationSet and every final ACK evidence record.
 */
public final class PulsarSourceLock {
    public static final String BRANCH = "nereus/delay-resource-guard";
    public static final String COMMIT = "0a2536484cd3932801a98dc88ff112b2df88a1c7";
    public static final String SOURCE_LOCK = BRANCH + "@" + COMMIT;

    private PulsarSourceLock() {}

    public static byte[] digest() {
        return Bytes.sha256(Bytes.utf8(SOURCE_LOCK));
    }

    public static void requireExact(final byte[] candidate) {
        if (!Bytes.constantTimeEquals(digest(), candidate)) {
            throw new IllegalArgumentException("Pulsar P1 source lock is not the active exact lock");
        }
    }
}
