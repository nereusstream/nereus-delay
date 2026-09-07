package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Closed common-policy head. The high-water lease end preserves bounded disable semantics across renewals. */
public final class TargetNativePolicyHead {
    public static final int VERSION = 2;
    public static final int MAX_CANONICAL_BYTES = 2 + 3 + TargetNativePolicySnapshot.MAX_CANONICAL_BYTES + 10 + 34;
    private static final byte[] HASH_DOMAIN = Bytes.utf8("nereus-delay-target-native-policy-head\0");
    private final TargetNativePolicySnapshot snapshot;
    private final long authorizedLeaseUntilEpochMs;
    private final byte[] digest;

    public TargetNativePolicyHead(final TargetNativePolicySnapshot snapshot, final long authorizedLeaseUntilEpochMs) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        if (authorizedLeaseUntilEpochMs < 0
                || (snapshot.mode() == HandoffPolicyMode.ENABLED
                        && authorizedLeaseUntilEpochMs < snapshot.validUntilEpochMs())) {
            throw new IllegalArgumentException("Target Native head loses an authorized lease interval");
        }
        this.authorizedLeaseUntilEpochMs = authorizedLeaseUntilEpochMs;
        digest = Bytes.sha256(HASH_DOMAIN, fields());
    }

    public TargetNativePolicySnapshot snapshot() {
        return snapshot;
    }

    public long authorizedLeaseUntilEpochMs() {
        return authorizedLeaseUntilEpochMs;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public HandoffPolicyHeadRef ref(final long revision) {
        return snapshot.headRef(revision);
    }

    /** Publication CAS must enforce this transition against the same prior head/revision it replaces. */
    public static TargetNativePolicyHead next(
            final TargetNativePolicyHead prior, final TargetNativePolicySnapshot next) {
        Objects.requireNonNull(next, "next");
        final long expected;
        if (prior == null) {
            expected = 1;
        } else {
            if (!Arrays.equals(prior.snapshot.policyScopeDigest(), next.policyScopeDigest())
                    || prior.snapshot.generation() == -1L) {
                throw new IllegalArgumentException("Target Native head cannot change scope or wrap generation");
            }
            expected = prior.snapshot.generation() + 1;
        }
        if (next.generation() != expected) {
            throw new IllegalArgumentException("Target Native policy generation must advance exactly once");
        }
        final long until = next.mode() == HandoffPolicyMode.ENABLED ? next.validUntilEpochMs() : 0;
        return new TargetNativePolicyHead(next, Math.max(prior == null ? 0 : prior.authorizedLeaseUntilEpochMs, until));
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, snapshot.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 3, authorizedLeaseUntilEpochMs);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 4, digest);
        });
    }

    public static TargetNativePolicyHead decode(final byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4, false, "Target Native head");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4}, "Target Native head");
        if (QueryCodecSupport.uint32(f.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target Native head generation");
        }
        final var result = new TargetNativePolicyHead(
                TargetNativePolicySnapshot.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.uint(f.get(2), 3));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(f.get(3), 4, 32))) {
            throw new IllegalArgumentException("Target Native head digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target Native head");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetNativePolicyHead that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
