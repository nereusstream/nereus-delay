package com.nereusstream.delay.semantic;

import java.util.UUID;

/** Injectable UUIDv7 source; tests may use a fixed sequence without changing routing. */
@FunctionalInterface
public interface LogicalUuidV7Generator {
    UUID next(TrustedTimeSnapshot trustedTime);
}
