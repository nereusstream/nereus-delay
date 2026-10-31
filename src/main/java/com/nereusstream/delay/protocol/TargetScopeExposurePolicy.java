package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Finite logical exposure bounds, to be bound by an outer paid plan. Scope identity, epoch and
 * Header/plan hashes deliberately stay outside these bytes, avoiding a digest cycle.
 * Construction proves neither physical Broker/SDK funding nor Source receipts, refunds or authority.
 */
public final class TargetScopeExposurePolicy {
    public static final int VERSION = 1;
    public static final int MAX_DEBITS = 4096;
    public static final int MAX_CANONICAL_BYTES =
            2 + 2 * 3 + 2 * (2 + Budget.MAX_CANONICAL_BYTES) + 10 + 2 + Window.MAX_CANONICAL_BYTES + 34;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-scope-exposure-policy\0");

    /** Classification alone grants no caller permission; a bootstrap FENCE is not SERVICE authority. */
    public enum Kind {
        NEW_PROMISE,
        CLOSURE
    }

    /** Independent finite upper-bound dimensions, including partially exhausted balances. */
    public record Budget(long records, long bytes) {
        public static final int MAX_CANONICAL_BYTES = 2 + 2 * 10;
        public static final Budget ZERO = new Budget(0, 0);

        public Budget {
            requireFiniteNonnegative(records, "exposure records");
            requireFiniteNonnegative(bytes, "exposure bytes");
        }

        public Budget plus(Budget other) {
            Objects.requireNonNull(other);
            return new Budget(Math.addExact(records, other.records), Math.addExact(bytes, other.bytes));
        }

        public Budget subtract(Budget other) {
            if (!covers(other)) {
                throw new IllegalArgumentException("exposure budget underflow");
            }
            return new Budget(Math.subtractExact(records, other.records), Math.subtractExact(bytes, other.bytes));
        }

        public boolean covers(Budget other) {
            Objects.requireNonNull(other);
            return records >= other.records && bytes >= other.bytes;
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint64(out, 2, records);
                CanonicalProtobuf.uint64(out, 3, bytes);
            });
        }

        public static Budget decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "exposure budget");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "exposure budget");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
                throw new IllegalArgumentException("unknown exposure budget schema");
            }
            final var value =
                    new Budget(QueryCodecSupport.uint64Bits(f.get(1), 2), QueryCodecSupport.uint64Bits(f.get(2), 3));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "exposure budget");
            return value;
        }
    }

    /** Fixed UTC buckets; the caller must independently qualify the supplied UTC time. */
    public record Window(long anchorEpochMs, long durationMillis, Budget maximum, Budget closureReserved) {
        public static final int MAX_CANONICAL_BYTES = 2 + 2 * 10 + 2 * (2 + Budget.MAX_CANONICAL_BYTES);

        public Window {
            requireFiniteNonnegative(anchorEpochMs, "exposure window anchor");
            requireFinitePositive(durationMillis, "exposure window duration");
            requirePositiveBudget(maximum, "exposure window maximum");
            Objects.requireNonNull(closureReserved);
            if (!maximum.covers(closureReserved)) {
                throw new IllegalArgumentException("closure window reserve exceeds total maximum");
            }
        }

        /** Pure floor((time - anchor) / duration); it neither reads a clock nor grants time qualification. */
        public long windowIndex(long qualifiedUtcEpochMs) {
            requireFiniteNonnegative(qualifiedUtcEpochMs, "qualified UTC epoch");
            if (qualifiedUtcEpochMs < anchorEpochMs) {
                throw new IllegalArgumentException("qualified UTC epoch precedes exposure window anchor");
            }
            return Math.floorDiv(Math.subtractExact(qualifiedUtcEpochMs, anchorEpochMs), durationMillis);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint64(out, 2, anchorEpochMs);
                CanonicalProtobuf.uint64(out, 3, durationMillis);
                CanonicalProtobuf.bytes(out, 4, maximum.canonicalBytes());
                CanonicalProtobuf.bytes(out, 5, closureReserved.canonicalBytes());
            });
        }

        public static Window decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "exposure window");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5}, "exposure window");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
                throw new IllegalArgumentException("unknown exposure window schema");
            }
            final var value = new Window(
                    QueryCodecSupport.uint64Bits(f.get(1), 2),
                    QueryCodecSupport.uint64Bits(f.get(2), 3),
                    Budget.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                    Budget.decode(QueryCodecSupport.bytes(f.get(4), 5)));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "exposure window");
            return value;
        }
    }

    private final int maximumDebits;
    private final int closureReservedDebits;
    private final Budget lifetime;
    private final Budget closureReserved;
    private final long maximumFrameBytes;
    private final Window window;
    private final byte[] digest;

    /**
     * Zero reserves are valid structural declarations, not closure qualification. The actual Provider
     * must prove the separately paid margin for its concrete outstanding obligations. Positive values
     * alone also do not prove that funding. No automatic reserve or physical-copy allowance is inferred.
     */
    public TargetScopeExposurePolicy(
            int maximumDebits,
            int closureReservedDebits,
            Budget lifetime,
            Budget closureReserved,
            long maximumFrameBytes,
            Window window) {
        if (maximumDebits < 1
                || maximumDebits > MAX_DEBITS
                || closureReservedDebits < 0
                || closureReservedDebits > maximumDebits) {
            throw new IllegalArgumentException("exposure debit bounds must be finite and reserve cannot exceed total");
        }
        requirePositiveBudget(lifetime, "exposure lifetime maximum");
        Objects.requireNonNull(closureReserved);
        Objects.requireNonNull(window);
        requireFinitePositive(maximumFrameBytes, "maximum exposure frame bytes");
        if (!lifetime.covers(closureReserved) || !lifetime.covers(window.maximum())) {
            throw new IllegalArgumentException("exposure reserve or window exceeds lifetime maximum");
        }
        if (window.maximum().bytes() < maximumFrameBytes) {
            throw new IllegalArgumentException("exposure window must declare room for one maximum frame");
        }
        // N * F is only a checked logical total; it does not count Broker units or SDK copies.
        requireFinitePositive(
                Math.multiplyExact(lifetime.records(), maximumFrameBytes), "logical exposure frame total");
        this.maximumDebits = maximumDebits;
        this.closureReservedDebits = closureReservedDebits;
        this.lifetime = lifetime;
        this.closureReserved = closureReserved;
        this.maximumFrameBytes = maximumFrameBytes;
        this.window = window;
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("exposure policy exceeds codec bound");
        }
    }

    public int schemaVersion() {
        return VERSION;
    }

    public int maximumDebits() {
        return maximumDebits;
    }

    public int closureReservedDebits() {
        return closureReservedDebits;
    }

    public Budget lifetime() {
        return lifetime;
    }

    public Budget closureReserved() {
        return closureReserved;
    }

    public long maximumFrameBytes() {
        return maximumFrameBytes;
    }

    public Window window() {
        return window;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public Budget ordinaryLifetimeLimit() {
        return lifetime.subtract(closureReserved);
    }

    public Budget ordinaryWindowLimit() {
        return window.maximum().subtract(window.closureReserved());
    }

    public int ordinaryDebitLimit() {
        return Math.subtractExact(maximumDebits, closureReservedDebits);
    }

    /** CLOSURE may use total remaining capacity; every debit must still settle against current usage. */
    public Budget lifetimeLimit(Kind kind) {
        return switch (Objects.requireNonNull(kind)) {
            case NEW_PROMISE -> ordinaryLifetimeLimit();
            case CLOSURE -> lifetime;
        };
    }

    public Budget windowLimit(Kind kind) {
        return switch (Objects.requireNonNull(kind)) {
            case NEW_PROMISE -> ordinaryWindowLimit();
            case CLOSURE -> window.maximum();
        };
    }

    public int debitLimit(Kind kind) {
        return switch (Objects.requireNonNull(kind)) {
            case NEW_PROMISE -> ordinaryDebitLimit();
            case CLOSURE -> maximumDebits;
        };
    }

    /** The durable Root must reject window regression and retain lifetime counters when a bucket advances. */
    public long windowIndex(long qualifiedUtcEpochMs) {
        return window.windowIndex(qualifiedUtcEpochMs);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.uint32(out, 2, maximumDebits);
            CanonicalProtobuf.uint32(out, 3, closureReservedDebits);
            CanonicalProtobuf.bytes(out, 4, lifetime.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, closureReserved.canonicalBytes());
            CanonicalProtobuf.uint64(out, 6, maximumFrameBytes);
            CanonicalProtobuf.bytes(out, 7, window.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 8, digest);
        });
    }

    public static TargetScopeExposurePolicy decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 8, false, "exposure policy");
        QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6, 7, 8}, "exposure policy");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unknown exposure policy schema");
        }
        final var value = new TargetScopeExposurePolicy(
                QueryCodecSupport.uint32(f.get(1), 2),
                QueryCodecSupport.uint32(f.get(2), 3),
                Budget.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                Budget.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                QueryCodecSupport.uint64Bits(f.get(5), 6),
                Window.decode(QueryCodecSupport.bytes(f.get(6), 7)));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(7), 8, 32))) {
            throw new IllegalArgumentException("exposure policy digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "exposure policy");
        return value;
    }

    private static void requireFiniteNonnegative(long value, String name) {
        if (value < 0 || value == Long.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be finite nonnegative");
        }
    }

    private static void requireFinitePositive(long value, String name) {
        if (value < 1 || value == Long.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be finite positive");
        }
    }

    private static void requirePositiveBudget(Budget value, String name) {
        Objects.requireNonNull(value);
        if (value.records() < 1 || value.bytes() < 1) {
            throw new IllegalArgumentException(name + " requires positive records and bytes");
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetScopeExposurePolicy that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
