package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Deterministic local catalog for exact immutable payload-proof trust sets. */
public final class InMemoryPayloadProofTrustSetCatalog implements PayloadProofTrustSetControlCatalog {
    private final Map<PayloadProofTrustSetRef, PayloadProofTrustSetSemantic> values = new HashMap<>();

    public synchronized void publish(final PayloadProofTrustSetSemantic semantic) {
        Objects.requireNonNull(semantic, "semantic");
        final PayloadProofTrustSetRef reference = semantic.ref();
        final PayloadProofTrustSetSemantic previous = values.putIfAbsent(reference, semantic);
        if (previous != null && !previous.equals(semantic)) {
            throw new IllegalStateException("trust-set reference changed semantic bytes");
        }
    }

    @Override
    public synchronized PayloadProofTrustSetSemantic resolve(final PayloadProofTrustSetRef reference) {
        Objects.requireNonNull(reference, "reference");
        final PayloadProofTrustSetSemantic semantic = values.get(reference);
        return semantic != null && reference.equals(semantic.ref()) ? semantic : null;
    }

    public synchronized int size() {
        return values.size();
    }
}
