package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Bounded immutable projection of one logical exposure debit. These bytes neither allocate a Root
 * revision nor grant or restore an SDK permit. The Root authority must privately assign the attempt
 * ID and issue a one-shot permit for that exact operation; a spent slot cannot mint another permit.
 */
public final class TargetScopeExposureDebit {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = 2 + 3 + 2 * 34 + 2 + Origin.MAX_CANONICAL_BYTES
            + 2 + 4 * 10 + 2 * 34;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-scope-exposure-debit\0");

    /** Exact original input identity, independent of current aliases or observation generations. */
    public sealed interface Origin permits ControlMember, ServiceWriter {
        int MAX_CANONICAL_BYTES = 2 + 2 + 2 + TargetSloWriterContract.Reference.MAX_CANONICAL_BYTES;

        enum Type { CONTROL_MEMBER, SERVICE_WRITER }

        Type type();
        byte[] canonicalBytes();

        static Origin controlMember(int memberSlot, byte[] registeredInputDigest) {
            return new ControlMember(memberSlot, registeredInputDigest);
        }

        static Origin serviceWriter(TargetSloWriterContract.Reference exactWriter) {
            return new ServiceWriter(exactWriter);
        }

        static Origin decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4, false, "exposure debit origin");
            if (f.size() < 2 || QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION)
                throw new IllegalArgumentException("unknown exposure debit origin schema");
            final Origin value;
            switch (QueryCodecSupport.uint32(f.get(1), 2)) {
                case 1 -> {
                    QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4}, "Control exposure debit origin");
                    value = new ControlMember(QueryCodecSupport.uint32(f.get(2), 3), QueryCodecSupport.fixed(f.get(3), 4, 32));
                }
                case 2 -> {
                    QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 5}, "SERVICE exposure debit origin");
                    value = new ServiceWriter(TargetSloWriterContract.Reference.decode(QueryCodecSupport.bytes(f.get(2), 5)));
                }
                default -> throw new IllegalArgumentException("unknown exposure debit origin type");
            }
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "exposure debit origin");
            return value;
        }
    }

    /** The Root authority must verify the original MEMBER range and exact registered input digest. */
    public record ControlMember(int memberSlot, byte[] registeredInputDigest) implements Origin {
        public ControlMember {
            if (memberSlot < 0) throw new IllegalArgumentException("negative exposure Control member slot");
            registeredInputDigest = copyDigest(registeredInputDigest, "registered exposure input digest");
        }

        @Override public byte[] registeredInputDigest() { return Bytes.copy(registeredInputDigest); }
        @Override public Type type() { return Type.CONTROL_MEMBER; }

        @Override public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint32(out, 2, 1);
                CanonicalProtobuf.uint32(out, 3, memberSlot);
                CanonicalProtobuf.bytes(out, 4, registeredInputDigest);
            });
        }

        @Override public boolean equals(Object other) {
            return other instanceof ControlMember that && memberSlot == that.memberSlot
                    && Arrays.equals(registeredInputDigest, that.registeredInputDigest);
        }

        @Override public int hashCode() { return Objects.hash(memberSlot, Arrays.hashCode(registeredInputDigest)); }
    }

    /**
     * Requires actual Native type-62 APPLIED original REGISTER/Scope proof outside this codec. A
     * reference DTO supplies no qualification and grants no SERVICE identity to a bootstrap FENCE.
     */
    public record ServiceWriter(TargetSloWriterContract.Reference exactWriter) implements Origin {
        public ServiceWriter { Objects.requireNonNull(exactWriter); }
        @Override public Type type() { return Type.SERVICE_WRITER; }

        @Override public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.uint32(out, 2, 2);
                CanonicalProtobuf.bytes(out, 5, exactWriter.canonicalBytes());
            });
        }
    }

    private final int index;
    private final byte[] attemptId;
    private final byte[] frameDigest;
    private final Origin origin;
    private final TargetScopeExposurePolicy.Kind kind;
    private final long maximumCopies;
    private final long frameBytes;
    private final long windowIndex;
    private final long qualifiedAtEpochMs;
    private final byte[] expansionContractDigest;
    private final TargetScopeExposurePolicy.Budget charge;
    private final byte[] digest;

    /** Kind and the declared SDK/provider copy contract require independent protection validation. */
    public TargetScopeExposureDebit(int index, byte[] attemptId, byte[] frameDigest, Origin origin,
            TargetScopeExposurePolicy.Kind kind, long maximumCopies, long frameBytes, long windowIndex,
            long qualifiedAtEpochMs, byte[] expansionContractDigest) {
        if (index < 0 || index >= TargetScopeExposurePolicy.MAX_DEBITS)
            throw new IllegalArgumentException("exposure debit index exceeds closed slot bound");
        this.index = index;
        this.attemptId = TargetCompatibilityCodec.assigned(attemptId, 32, "exposure attempt ID");
        this.frameDigest = copyDigest(frameDigest, "complete encoded exposure frame digest");
        this.origin = Objects.requireNonNull(origin);
        this.kind = Objects.requireNonNull(kind);
        requireFinitePositive(maximumCopies, "maximum exposure copies");
        requireFinitePositive(frameBytes, "complete encoded exposure frame bytes");
        requireFiniteNonnegative(windowIndex, "exposure window index");
        requireFiniteNonnegative(qualifiedAtEpochMs, "qualified exposure UTC epoch");
        this.maximumCopies = maximumCopies;
        this.frameBytes = frameBytes;
        this.windowIndex = windowIndex;
        this.qualifiedAtEpochMs = qualifiedAtEpochMs;
        this.expansionContractDigest = TargetCompatibilityCodec.assigned(expansionContractDigest, 32, "exposure copy expansion contract");
        charge = new TargetScopeExposurePolicy.Budget(maximumCopies, Math.multiplyExact(maximumCopies, frameBytes));
        digest = Bytes.sha256(DOMAIN, fields());
        if (canonicalBytes().length > MAX_CANONICAL_BYTES)
            throw new IllegalArgumentException("exposure debit exceeds codec bound");
    }

    public int schemaVersion() { return VERSION; }
    public int index() { return index; }
    public byte[] attemptId() { return Bytes.copy(attemptId); }
    public byte[] frameDigest() { return Bytes.copy(frameDigest); }
    public Origin origin() { return origin; }
    public TargetScopeExposurePolicy.Kind kind() { return kind; }
    public long maximumCopies() { return maximumCopies; }
    public long frameBytes() { return frameBytes; }
    public long windowIndex() { return windowIndex; }
    public long qualifiedAtEpochMs() { return qualifiedAtEpochMs; }
    public byte[] expansionContractDigest() { return Bytes.copy(expansionContractDigest); }
    public TargetScopeExposurePolicy.Budget charge() { return charge; }
    public byte[] digest() { return Bytes.copy(digest); }

    /** Checks the entire encodeFrame output, including its envelope and framing, without retaining it. */
    public void requireFrame(byte[] exactEncodedFrame) {
        Objects.requireNonNull(exactEncodedFrame);
        if (exactEncodedFrame.length != frameBytes || !Arrays.equals(frameDigest, Bytes.sha256(exactEncodedFrame)))
            throw new IllegalArgumentException("exposure debit changed its complete encoded frame");
    }

    /**
     * Static consistency only. Current usage, ordinary/closure slot balances, UTC qualification,
     * original Origin and Kind authority, paid copy expansion and one-shot issuance remain external.
     */
    public void requirePolicy(TargetScopeExposurePolicy policy) {
        final var exact = Objects.requireNonNull(policy);
        if (index >= exact.maximumDebits() || frameBytes > exact.maximumFrameBytes()
                || windowIndex != exact.windowIndex(qualifiedAtEpochMs)
                || !exact.lifetimeLimit(kind).covers(charge) || !exact.windowLimit(kind).covers(charge))
            throw new IllegalArgumentException("exposure debit exceeds or changes its declared policy");
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.uint32(out, 2, index);
            CanonicalProtobuf.bytes(out, 3, attemptId);
            CanonicalProtobuf.bytes(out, 4, frameDigest);
            CanonicalProtobuf.bytes(out, 5, origin.canonicalBytes());
            CanonicalProtobuf.uint32(out, 6, kind == TargetScopeExposurePolicy.Kind.NEW_PROMISE ? 1 : 2);
            CanonicalProtobuf.uint64(out, 7, maximumCopies);
            CanonicalProtobuf.uint64(out, 8, frameBytes);
            CanonicalProtobuf.uint64(out, 9, windowIndex);
            CanonicalProtobuf.uint64(out, 10, qualifiedAtEpochMs);
            CanonicalProtobuf.bytes(out, 11, expansionContractDigest);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 12, digest);
        });
    }

    public static TargetScopeExposureDebit decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 12, false, "exposure debit");
        QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, "exposure debit");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION)
            throw new IllegalArgumentException("unknown exposure debit schema");
        final var kind = switch (QueryCodecSupport.uint32(f.get(5), 6)) {
            case 1 -> TargetScopeExposurePolicy.Kind.NEW_PROMISE;
            case 2 -> TargetScopeExposurePolicy.Kind.CLOSURE;
            default -> throw new IllegalArgumentException("unknown exposure debit kind");
        };
        final var value = new TargetScopeExposureDebit(QueryCodecSupport.uint32(f.get(1), 2),
                QueryCodecSupport.fixed(f.get(2), 3, 32), QueryCodecSupport.fixed(f.get(3), 4, 32),
                Origin.decode(QueryCodecSupport.bytes(f.get(4), 5)), kind, QueryCodecSupport.uint64Bits(f.get(6), 7),
                QueryCodecSupport.uint64Bits(f.get(7), 8), QueryCodecSupport.uint64Bits(f.get(8), 9),
                QueryCodecSupport.uint64Bits(f.get(9), 10), QueryCodecSupport.fixed(f.get(10), 11, 32));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(11), 12, 32)))
            throw new IllegalArgumentException("exposure debit digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "exposure debit");
        return value;
    }

    private static byte[] copyDigest(byte[] value, String name) {
        Bytes.requireLength(value, 32, name);
        return Bytes.copy(value);
    }

    private static void requireFiniteNonnegative(long value, String name) {
        if (value < 0 || value == Long.MAX_VALUE) throw new IllegalArgumentException(name + " must be finite nonnegative");
    }

    private static void requireFinitePositive(long value, String name) {
        if (value < 1 || value == Long.MAX_VALUE) throw new IllegalArgumentException(name + " must be finite positive");
    }

    public boolean valueEquals(TargetScopeExposureDebit other) { return equals(other); }

    @Override public boolean equals(Object other) {
        return other instanceof TargetScopeExposureDebit that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override public int hashCode() { return Arrays.hashCode(digest); }
}
