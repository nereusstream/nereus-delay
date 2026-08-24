package com.nereusstream.delay.transport;

import java.util.Objects;

/** Exact credential binding identity included in a transport registry key. */
public record CredentialBindingKey(long generation, Digest32 bindingDigest, Digest32 resolvedCredentialFingerprint) {
    public CredentialBindingKey {
        if (generation <= 0) {
            throw new IllegalArgumentException("credential binding generation must be positive");
        }
        bindingDigest = Objects.requireNonNull(bindingDigest, "bindingDigest");
        resolvedCredentialFingerprint =
                Objects.requireNonNull(resolvedCredentialFingerprint, "resolvedCredentialFingerprint");
    }
}
