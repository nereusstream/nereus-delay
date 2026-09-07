package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Durable Target accounting, separate from legacy Lane cardinalities and live Worker resource gauges. */
public final class TargetQuotaUsage {
    public static final int VERSION = 1;
    public static final int MAX_CAPACITY_VECTOR_BYTES = 36 + CapacityDimension.COUNT * 14;
    public static final int MAX_CANONICAL_BYTES = 2 + 3 + MAX_CAPACITY_VECTOR_BYTES + 4 * 11 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-quota-usage\0");

    private final CapacityVector resources;
    private final long targets;
    private final long executionDomains;
    private final long strictOrderDomains;
    private final long accountingIncarnations;
    private final byte[] digest;

    public TargetQuotaUsage(
            final CapacityVector resources,
            final long targets,
            final long executionDomains,
            final long strictOrderDomains,
            final long accountingIncarnations) {
        this.resources = Objects.requireNonNull(resources, "resources");
        for (CapacityDimension dimension : CapacityDimension.values()) {
            final int wire = dimension.wireValue();
            if (!(wire <= 15 || (wire >= 51 && wire <= 55)) && resources.amount(dimension) != 0) {
                throw new IllegalArgumentException("not a durable Target quota dimension: " + dimension);
            }
        }
        if (targets < 0 || executionDomains < 0 || strictOrderDomains < 0 || accountingIncarnations < 0) {
            throw new IllegalArgumentException("Target cardinalities exceed the local capacity range");
        }
        this.targets = targets;
        this.executionDomains = executionDomains;
        this.strictOrderDomains = strictOrderDomains;
        this.accountingIncarnations = accountingIncarnations;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public static TargetQuotaUsage empty() {
        return new TargetQuotaUsage(CapacityVector.empty(), 0, 0, 0, 0);
    }

    public CapacityVector resources() {
        return resources;
    }

    public long targets() {
        return targets;
    }

    public long executionDomains() {
        return executionDomains;
    }

    public long strictOrderDomains() {
        return strictOrderDomains;
    }

    public long accountingIncarnations() {
        return accountingIncarnations;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public boolean isZero() {
        return resources.isZero()
                && targets == 0
                && executionDomains == 0
                && strictOrderDomains == 0
                && accountingIncarnations == 0;
    }

    public TargetQuotaUsage add(final TargetQuotaUsage other) {
        Objects.requireNonNull(other, "other");
        return new TargetQuotaUsage(
                resources.add(other.resources),
                Math.addExact(targets, other.targets),
                Math.addExact(executionDomains, other.executionDomains),
                Math.addExact(strictOrderDomains, other.strictOrderDomains),
                Math.addExact(accountingIncarnations, other.accountingIncarnations));
    }

    public TargetQuotaUsage subtract(final TargetQuotaUsage other) {
        Objects.requireNonNull(other, "other");
        return new TargetQuotaUsage(
                resources.subtract(other.resources),
                subtract(targets, other.targets),
                subtract(executionDomains, other.executionDomains),
                subtract(strictOrderDomains, other.strictOrderDomains),
                subtract(accountingIncarnations, other.accountingIncarnations));
    }

    private static long subtract(final long prior, final long released) {
        if (released > prior) {
            throw new IllegalStateException("Target quota cardinality underflow");
        }
        return prior - released;
    }

    /** A reduced grant preserves existing usage, but cannot authorize growth above its limit. */
    public boolean permitsGrowth(final TargetQuotaUsage prior, final TargetQuotaUsage next) {
        Objects.requireNonNull(prior, "prior");
        Objects.requireNonNull(next, "next");
        for (CapacityDimension dimension : CapacityDimension.values()) {
            if (!permits(
                    resources.amount(dimension), prior.resources.amount(dimension), next.resources.amount(dimension))) {
                return false;
            }
        }
        return permits(targets, prior.targets, next.targets)
                && permits(executionDomains, prior.executionDomains, next.executionDomains)
                && permits(strictOrderDomains, prior.strictOrderDomains, next.strictOrderDomains)
                && permits(accountingIncarnations, prior.accountingIncarnations, next.accountingIncarnations);
    }

    private static boolean permits(final long limit, final long prior, final long next) {
        return next <= prior || next <= limit;
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, resources.canonicalBytes());
            CanonicalProtobuf.uint64(out, 3, targets);
            CanonicalProtobuf.uint64(out, 4, executionDomains);
            CanonicalProtobuf.uint64(out, 5, strictOrderDomains);
            CanonicalProtobuf.uint64(out, 6, accountingIncarnations);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 7, digest);
        });
    }

    public static TargetQuotaUsage decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "TargetQuotaUsage");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6, 7}, "TargetQuotaUsage");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target quota usage version");
        }
        final var result = new TargetQuotaUsage(
                CapacityVector.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.uint(fields.get(2), 3),
                QueryCodecSupport.uint(fields.get(3), 4),
                QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.uint(fields.get(5), 6));
        if (!Arrays.equals(result.digest, QueryCodecSupport.fixed(fields.get(6), 7, 32))) {
            throw new IllegalArgumentException("Target quota usage digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaUsage");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQuotaUsage that
                && resources.equals(that.resources)
                && targets == that.targets
                && executionDomains == that.executionDomains
                && strictOrderDomains == that.strictOrderDomains
                && accountingIncarnations == that.accountingIncarnations;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
