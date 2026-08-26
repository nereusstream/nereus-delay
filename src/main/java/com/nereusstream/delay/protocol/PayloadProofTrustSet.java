package com.nereusstream.delay.protocol;

import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable verifier key set selected by the source-ordered Prepare state. */
public final class PayloadProofTrustSet {
    private final long version;
    private final Map<Integer, PublicKey> keys;
    private final Map<Integer, KeyWindow> keyWindows;

    public PayloadProofTrustSet(final long version, final Map<Integer, PublicKey> keys) {
        if (version == 0) {
            throw new IllegalArgumentException("trust set version must be nonzero");
        }
        Objects.requireNonNull(keys, "keys");
        if (keys.isEmpty()) {
            throw new IllegalArgumentException("trust set must contain a verifier key");
        }
        for (Map.Entry<Integer, PublicKey> entry : keys.entrySet()) {
            if (entry.getKey() == null || entry.getKey() == 0 || entry.getValue() == null) {
                throw new IllegalArgumentException("invalid payload proof verifier key");
            }
        }
        this.version = version;
        this.keys = Map.copyOf(keys);
        final Map<Integer, KeyWindow> windows = new HashMap<>();
        for (Integer keyVersion : keys.keySet()) {
            windows.put(keyVersion, new KeyWindow(0, Long.MAX_VALUE));
        }
        this.keyWindows = Map.copyOf(windows);
    }

    /** Builds the local verifier adapter from the canonical Registry semantic value. */
    public static PayloadProofTrustSet fromSemantic(final PayloadProofTrustSetSemantic semantic) {
        Objects.requireNonNull(semantic, "semantic");
        final Map<Integer, PublicKey> keys = new HashMap<>();
        final Map<Integer, KeyWindow> windows = new HashMap<>();
        for (PayloadProofVerifierKey key : semantic.keys()) {
            keys.put(key.keyVersion(), key.toPublicKey());
            windows.put(key.keyVersion(), new KeyWindow(key.verifyNotBeforeEpochMs(), key.verifyNotAfterEpochMs()));
        }
        return new PayloadProofTrustSet(semantic.version(), keys, windows);
    }

    public long version() {
        return version;
    }

    public boolean verifies(final PayloadCommitProofView proof) {
        return verifies(proof, Long.MAX_VALUE);
    }

    /** Verifies a proof and applies the key's source-time validity window. */
    public boolean verifies(final PayloadCommitProofView proof, final long sourceEpochMs) {
        if (sourceEpochMs < 0) {
            throw new IllegalArgumentException("sourceEpochMs must be non-negative");
        }
        if (proof.trustSetVersion() != version) {
            return false;
        }
        final PublicKey key = keys.get(proof.proofKeyVersion());
        final KeyWindow window = keyWindows.get(proof.proofKeyVersion());
        return key != null
                && window != null
                && sourceEpochMs >= window.notBeforeEpochMs()
                && sourceEpochMs <= window.notAfterEpochMs()
                && proof.verifySignature(key);
    }

    /**
     * Verifies an already accepted semantic proof identity with a retained
     * historical key. Issuance and source-time windows gate first acceptance;
     * they must not make a previously committed ProofId unverifiable.
     */
    public boolean verifiesHistoricalSignature(final PayloadCommitProofView proof) {
        if (proof.trustSetVersion() != version) {
            return false;
        }
        final PublicKey key = keys.get(proof.proofKeyVersion());
        return key != null && proof.verifySignature(key);
    }

    private PayloadProofTrustSet(
            final long version, final Map<Integer, PublicKey> keys, final Map<Integer, KeyWindow> keyWindows) {
        this.version = version;
        this.keys = Map.copyOf(keys);
        this.keyWindows = Map.copyOf(keyWindows);
    }

    private record KeyWindow(long notBeforeEpochMs, long notAfterEpochMs) {}
}
