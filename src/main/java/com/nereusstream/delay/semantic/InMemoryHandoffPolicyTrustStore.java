package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Deterministic historical trust store used by disposable recovery tests. */
public final class InMemoryHandoffPolicyTrustStore implements HandoffPolicyTrustStore {
    private final Map<Integer, KeyActivation> keys = new HashMap<>();
    private final Map<PolicyGeneration, SourcePosition> policies = new HashMap<>();

    public synchronized void installIssuerKey(
            final int issuerKeyGeneration, final PublicKey key, final SourcePosition activeFrom) {
        if (issuerKeyGeneration <= 0) {
            throw new IllegalArgumentException("issuerKeyGeneration must be positive");
        }
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(activeFrom, "activeFrom");
        final KeyActivation prior = keys.putIfAbsent(issuerKeyGeneration, new KeyActivation(key, activeFrom));
        if (prior != null && (!prior.key().equals(key) || !prior.activeFrom().equals(activeFrom))) {
            throw new IllegalArgumentException("issuer key generation is already bound to another key");
        }
    }

    public synchronized void activatePolicy(
            final byte[] policyScopeDigest, final long policyGeneration, final SourcePosition activeFrom) {
        Bytes.requireLength(policyScopeDigest, 32, "policyScopeDigest");
        if (policyGeneration == 0) {
            throw new IllegalArgumentException("policyGeneration must be non-zero");
        }
        Objects.requireNonNull(activeFrom, "activeFrom");
        final PolicyGeneration identity = new PolicyGeneration(policyScopeDigest, policyGeneration);
        final SourcePosition prior = policies.putIfAbsent(identity, activeFrom);
        if (prior != null && !prior.equals(activeFrom)) {
            throw new IllegalArgumentException("policy generation is already activated at another position");
        }
    }

    @Override
    public synchronized Optional<PublicKey> issuerKey(
            final int issuerKeyGeneration, final SourcePosition sourcePosition) {
        final KeyActivation candidate = keys.get(issuerKeyGeneration);
        if (candidate == null || !isAtOrBefore(candidate.activeFrom(), sourcePosition)) {
            return Optional.empty();
        }
        return Optional.of(candidate.key());
    }

    @Override
    public synchronized Optional<SourcePosition> activationPosition(
            final byte[] policyScopeDigest, final long policyGeneration) {
        return Optional.ofNullable(policies.get(new PolicyGeneration(policyScopeDigest, policyGeneration)));
    }

    private static boolean isAtOrBefore(final SourcePosition left, final SourcePosition right) {
        try {
            return left.sameSourceIdentity(right) && left.compareTo(right) <= 0;
        } catch (IllegalArgumentException incompatible) {
            return false;
        }
    }

    private record KeyActivation(PublicKey key, SourcePosition activeFrom) {}

    private static final class PolicyGeneration {
        private final byte[] scope;
        private final long generation;

        private PolicyGeneration(final byte[] scope, final long generation) {
            Bytes.requireLength(scope, 32, "policyScopeDigest");
            this.scope = Bytes.copy(scope);
            this.generation = generation;
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof PolicyGeneration that
                    && generation == that.generation
                    && java.util.Arrays.equals(scope, that.scope);
        }

        @Override
        public int hashCode() {
            return 31 * java.util.Arrays.hashCode(scope) + Long.hashCode(generation);
        }
    }
}
