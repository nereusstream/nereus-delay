package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.SelfRoutingId;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.UUID;

/** Production UUIDv7 generator using the trusted time sample and process DRBG. */
public final class SecureLogicalUuidV7Generator implements LogicalUuidV7Generator {
    private final SecureRandom random;

    public SecureLogicalUuidV7Generator() {
        this(new SecureRandom());
    }

    public SecureLogicalUuidV7Generator(final SecureRandom random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    @Override
    public UUID next(final TrustedTimeSnapshot trustedTime) {
        return SelfRoutingId.uuidV7(
                Objects.requireNonNull(trustedTime, "trustedTime").epochMs(), random);
    }
}
