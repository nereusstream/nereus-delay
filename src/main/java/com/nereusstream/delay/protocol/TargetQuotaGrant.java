package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Complete quota grant artifact. Construction/decoding alone does not authorize activation or tenant capacity. */
public final class TargetQuotaGrant {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 2
            + 3
            + TargetQuotaScope.MAX_CANONICAL_BYTES
            + 34
            + 11
            + 3
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 3
            + TargetQuotaUsage.MAX_CANONICAL_BYTES
            + 11
            + 34
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-grant\0");

    private final TargetQuotaScope scope;
    private final byte[] grantId;
    private final long version;
    private final TargetQuotaAccounting accounting;
    private final TargetQuotaUsage limit;
    private final long tenantPolicyVersion;
    private final byte[] tenantPolicyHash;
    private final byte[] digest;

    public TargetQuotaGrant(
            final TargetQuotaScope scope,
            final byte[] grantId,
            final long version,
            final TargetQuotaAccounting accounting,
            final TargetQuotaUsage limit,
            final long tenantPolicyVersion,
            final byte[] tenantPolicyHash) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.grantId = TargetCompatibilityCodec.assigned(grantId, 32, "grantId");
        if (version == 0 || tenantPolicyVersion == 0) {
            throw new IllegalArgumentException("Target grant and tenant policy versions must be nonzero");
        }
        this.version = version;
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.limit = Objects.requireNonNull(limit, "limit");
        if (scope.target() != null) {
            TargetQuotaTotal.requireTargetUsage(limit);
        }
        this.tenantPolicyVersion = tenantPolicyVersion;
        this.tenantPolicyHash = TargetCompatibilityCodec.assigned(tenantPolicyHash, 32, "tenantPolicyHash");
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetQuotaScope scope() {
        return scope;
    }

    public byte[] grantId() {
        return Bytes.copy(grantId);
    }

    public long version() {
        return version;
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
    }

    public TargetQuotaUsage limit() {
        return limit;
    }

    public long tenantPolicyVersion() {
        return tenantPolicyVersion;
    }

    public byte[] tenantPolicyHash() {
        return Bytes.copy(tenantPolicyHash);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    /** Local monotonicity only; the authority also proves static tenant cuts and shrink-before-increase transfer. */
    public void requireSuccessor(final TargetQuotaGrant prior) {
        if (prior == null) {
            if (version != 1) {
                throw new IllegalStateException("initial Target grant version must be one");
            }
        } else if (!scope.equals(prior.scope)
                || !Arrays.equals(grantId, prior.grantId)
                || version != TargetQuotaMutation.increment(prior.version)
                || Long.compareUnsigned(tenantPolicyVersion, prior.tenantPolicyVersion) < 0
                || (tenantPolicyVersion == prior.tenantPolicyVersion
                        && !Arrays.equals(tenantPolicyHash, prior.tenantPolicyHash))) {
            throw new IllegalStateException("Target grant successor changes identity or regresses policy/version");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, grantId);
            CanonicalProtobuf.uint64Bits(out, 4, version);
            CanonicalProtobuf.bytes(out, 5, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 6, limit.canonicalBytes());
            CanonicalProtobuf.uint64Bits(out, 7, tenantPolicyVersion);
            CanonicalProtobuf.bytes(out, 8, tenantPolicyHash);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 9, digest);
        });
    }

    public static TargetQuotaGrant decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 9, false, "TargetQuotaGrant");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, "TargetQuotaGrant");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota grant version");
        }
        final var result = new TargetQuotaGrant(
                TargetQuotaScope.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                TargetQuotaAccounting.decode(QueryCodecSupport.bytes(fields.get(4), 5)),
                TargetQuotaUsage.decode(QueryCodecSupport.bytes(fields.get(5), 6)),
                QueryCodecSupport.uint64Bits(fields.get(6), 7),
                QueryCodecSupport.fixed(fields.get(7), 8, 32));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 9, 32))) {
            throw new IllegalArgumentException("Target quota grant digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaGrant");
        return result;
    }
}
