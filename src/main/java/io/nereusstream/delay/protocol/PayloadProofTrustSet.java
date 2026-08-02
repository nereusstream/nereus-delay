package io.nereusstream.delay.protocol;

import java.security.PublicKey;
import java.util.Map;
import java.util.Objects;

/** Immutable verifier key set selected by the source-ordered Prepare state. */
public final class PayloadProofTrustSet {
    private final long version;
    private final Map<Integer, PublicKey> keys;

    public PayloadProofTrustSet(final long version, final Map<Integer, PublicKey> keys) {
        if (version <= 0) {
            throw new IllegalArgumentException("trust set version must be positive");
        }
        Objects.requireNonNull(keys, "keys");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("trust set must contain a verifier key");
        }
        for (Map.Entry<Integer, PublicKey> entry : keys.entrySet()) {
            if (entry.getKey() == null || entry.getKey() <= 0 || entry.getValue() == null) {
                throw new IllegalArgumentException("invalid payload proof verifier key");
            }
        }
        this.version = version;
        this.keys = Map.copyOf(keys);
    }

    public long version() {
        return version;
    }

    public boolean verifies(final PayloadCommitProofView proof) {
        if (proof.trustSetVersion() != version) {
            return false;
        }
        final PublicKey key = keys.get(proof.proofKeyVersion());
        return key != null && proof.verifySignature(key);
    }
}
