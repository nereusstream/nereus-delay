package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.HandoffPolicyHead;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Deterministic CAS authority for disposable policy/recovery tests. */
public final class InMemoryHandoffPolicyAuthority implements HandoffPolicyAuthority {
    private final Map<Scope, Publication> heads = new HashMap<>();

    @Override
    public synchronized Optional<Publication> current(final byte[] policyScopeDigest) {
        return Optional.ofNullable(heads.get(new Scope(policyScopeDigest)));
    }

    @Override
    public synchronized Publication compareAndSet(
            final byte[] policyScopeDigest, final long expectedOxiaVersion, final HandoffPolicyHead next) {
        final Scope scope = new Scope(policyScopeDigest);
        Objects.requireNonNull(next, "next");
        if (!Arrays.equals(scope.bytes(), next.scopeDigest())) {
            throw new IllegalArgumentException("policy head scope mismatch");
        }
        final Publication prior = heads.get(scope);
        final long actual = prior == null ? 0 : prior.oxiaVersion();
        if (actual != expectedOxiaVersion) {
            throw new IllegalStateException("policy head compare-and-set revision conflict");
        }
        final Publication publication = new Publication(actual + 1, next);
        heads.put(scope, publication);
        return publication;
    }

    private static final class Scope {
        private final byte[] bytes;

        private Scope(final byte[] value) {
            Bytes.requireLength(value, 32, "policyScopeDigest");
            this.bytes = Bytes.copy(value);
        }

        private byte[] bytes() {
            return Bytes.copy(bytes);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Scope that && Arrays.equals(bytes, that.bytes);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
    }
}
