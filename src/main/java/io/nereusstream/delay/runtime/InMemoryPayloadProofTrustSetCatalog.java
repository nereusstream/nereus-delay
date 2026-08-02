package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Deterministic local catalog for exact immutable payload-proof trust sets. */
public final class InMemoryPayloadProofTrustSetCatalog implements PayloadProofTrustSetControlCatalog {
    private final Map<PayloadProofTrustSetRefV1, PayloadProofTrustSetSemanticV1> values = new HashMap<>();

    public synchronized void publish(final PayloadProofTrustSetSemanticV1 semantic) {
        Objects.requireNonNull(semantic, "semantic");
        final PayloadProofTrustSetRefV1 reference = semantic.ref();
        final PayloadProofTrustSetSemanticV1 previous = values.putIfAbsent(reference, semantic);
        if (previous != null && !previous.equals(semantic)) {
            throw new IllegalStateException("trust-set reference changed semantic bytes");
        }
    }

    @Override
    public synchronized PayloadProofTrustSetSemanticV1 resolve(final PayloadProofTrustSetRefV1 reference) {
        Objects.requireNonNull(reference, "reference");
        final PayloadProofTrustSetSemanticV1 semantic = values.get(reference);
        return semantic != null && reference.equals(semantic.ref()) ? semantic : null;
    }

    public synchronized int size() {
        return values.size();
    }
}
