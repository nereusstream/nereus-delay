package io.nereusstream.delay.semantic;

/** Immutable time sample captured once at the Semantic Core boundary. */
public record TrustedTimeSnapshot(long epochMs) {
    public TrustedTimeSnapshot {
        if (epochMs < 0) {
            throw new IllegalArgumentException("trusted time must be non-negative");
        }
    }
}
