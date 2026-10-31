package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Original finite writer declaration. Its construction is neither registration nor resource qualification. */
public record TargetSloWriterContract(
        TargetQuotaScope scope,
        byte[] lineage,
        SourcePosition sourceAnchor,
        AuthorIdentity service,
        int keyVersion,
        long epoch,
        long maximumRetryUntil,
        Limits limits,
        byte[] resourceContractDigest) {
    public static final int MAX_SERVICE_BYTES = TargetSloObservationPolicy.MAX_SERVICE_BYTES;
    public static final int MAX_CANONICAL_BYTES = 2
            + (6 + TargetQuotaScope.MAX_CANONICAL_BYTES)
            + 18
            + (6 + TargetSourcePosition.MAX_CANONICAL_BYTES)
            + (6 + MAX_SERVICE_BYTES)
            + 6
            + 11
            + 11
            + (6 + Limits.MAX_CANONICAL_BYTES)
            + 34
            + 34;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-writer-contract\0");

    public record Limits(
            int maximumEpochs,
            long maximumSourceOccurrences,
            long maximumBurstOccurrences,
            long maximumFrameBytes,
            long retainedUntilEpochMs) {
        public static final int MAX_CANONICAL_BYTES = 2 + 6 + 4 * 11;

        public Limits {
            if (maximumEpochs < 1
                    || maximumEpochs == Integer.MAX_VALUE
                    || maximumSourceOccurrences < 1
                    || maximumSourceOccurrences == Long.MAX_VALUE
                    || maximumBurstOccurrences < 1
                    || maximumBurstOccurrences == Long.MAX_VALUE
                    || maximumBurstOccurrences > maximumSourceOccurrences
                    || maximumFrameBytes < 1
                    || maximumFrameBytes == Long.MAX_VALUE
                    || retainedUntilEpochMs < 1
                    || retainedUntilEpochMs == Long.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "SLO writer needs explicit finite epoch/occurrence/burst/frame/retention bounds");
            }
            Math.multiplyExact(maximumSourceOccurrences, maximumFrameBytes);
            Math.multiplyExact(maximumBurstOccurrences, maximumFrameBytes);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.uint32(out, 2, maximumEpochs);
                CanonicalProtobuf.uint64Bits(out, 3, maximumSourceOccurrences);
                CanonicalProtobuf.uint64Bits(out, 4, maximumBurstOccurrences);
                CanonicalProtobuf.uint64Bits(out, 5, maximumFrameBytes);
                CanonicalProtobuf.uint64Bits(out, 6, retainedUntilEpochMs);
            });
        }

        public static Limits decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 6, false, "Target SLO writer finite limits");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5, 6}, "Target SLO writer finite limits");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
                throw new IllegalArgumentException("unknown SLO writer limit schema");
            }
            final var value = new Limits(
                    QueryCodecSupport.uint32(f.get(1), 2),
                    QueryCodecSupport.uint64Bits(f.get(2), 3),
                    QueryCodecSupport.uint64Bits(f.get(3), 4),
                    QueryCodecSupport.uint64Bits(f.get(4), 5),
                    QueryCodecSupport.uint64Bits(f.get(5), 6));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target SLO writer finite limits");
            return value;
        }
    }

    /** References the actual Source-applied REGISTER, not the unsigned pure declaration digest. */
    public record Reference(byte[] writerIdentityDigest, long epoch, byte[] contractDigest) {
        public static final int MAX_CANONICAL_BYTES = 2 + 34 + 11 + 34;

        public Reference {
            writerIdentityDigest = TargetCompatibilityCodec.assigned(writerIdentityDigest, 32, "SLO writer identity");
            contractDigest =
                    TargetCompatibilityCodec.assigned(contractDigest, 32, "original applied SLO writer contract");
            if (epoch == 0) {
                throw new IllegalArgumentException("SLO writer reference requires an assigned epoch");
            }
        }

        @Override
        public byte[] writerIdentityDigest() {
            return Bytes.copy(writerIdentityDigest);
        }

        @Override
        public byte[] contractDigest() {
            return Bytes.copy(contractDigest);
        }

        public byte[] key(TargetQuotaScope scope) {
            return epochKey(scope, writerIdentityDigest, epoch);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, writerIdentityDigest);
                CanonicalProtobuf.uint64Bits(out, 3, epoch);
                CanonicalProtobuf.bytes(out, 4, contractDigest);
            });
        }

        public static Reference decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 4, false, "Target SLO applied writer reference");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4}, "Target SLO applied writer reference");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
                throw new IllegalArgumentException("unknown SLO writer reference schema");
            }
            final var value = new Reference(
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.uint64Bits(f.get(2), 3),
                    QueryCodecSupport.fixed(f.get(3), 4, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target SLO applied writer reference");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Reference ref
                    && epoch == ref.epoch
                    && Arrays.equals(writerIdentityDigest, ref.writerIdentityDigest)
                    && Arrays.equals(contractDigest, ref.contractDigest);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    public TargetSloWriterContract {
        Objects.requireNonNull(scope);
        Objects.requireNonNull(service);
        Objects.requireNonNull(limits);
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "SLO writer recovery lineage");
        resourceContractDigest = TargetCompatibilityCodec.assigned(
                resourceContractDigest, 32, "original complete SLO Scope resource contract");
        sourceAnchor = TargetSourcePosition.requireBounded(sourceAnchor);
        if (scope.target() != null
                || !scope.shard().equals(sourceAnchor.shardId())
                || service.kind() != AuthorIdentity.Kind.SERVICE
                || service.canonicalBytes().length > MAX_SERVICE_BYTES
                || keyVersion == 0
                || epoch == 0
                || Long.compareUnsigned(epoch, limits.maximumEpochs()) > 0
                || maximumRetryUntil < 1
                || maximumRetryUntil == Long.MAX_VALUE
                || limits.retainedUntilEpochMs() < maximumRetryUntil) {
            throw new IllegalArgumentException(
                    "SLO writer declaration changed scope/identity or finite deadline/retention bounds");
        }
    }

    @Override
    public byte[] lineage() {
        return Bytes.copy(lineage);
    }

    @Override
    public byte[] resourceContractDigest() {
        return Bytes.copy(resourceContractDigest);
    }

    public byte[] writerIdentityDigest() {
        return Bytes.sha256(service.canonicalBytes());
    }

    public void requireSource(SourcePosition source) {
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId()) || !sourceAnchor.sameSourceIdentity(source)) {
            throw new IllegalArgumentException("SLO writer changed its frozen physical Source");
        }
    }

    public static byte[] epochKey(TargetQuotaScope scope, byte[] writerIdentityDigest, long epoch) {
        Objects.requireNonNull(scope);
        Bytes.requireLength(writerIdentityDigest, 32, "SLO writer identity");
        if (scope.target() != null || epoch == 0) {
            throw new IllegalArgumentException("SLO writer key requires a Shard scope and epoch");
        }
        return Bytes.concat(
                new byte[] {TargetKeyCodec.SLO_WRITER_EPOCH_TAG, TargetKeyCodec.KEY_FORMAT},
                scope.keySuffix(),
                writerIdentityDigest,
                Bytes.u64beBits(epoch));
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, lineage);
            CanonicalProtobuf.bytes(out, 4, sourceAnchor.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, service.canonicalBytes());
            CanonicalProtobuf.uint32Bits(out, 6, keyVersion);
            CanonicalProtobuf.uint64Bits(out, 7, epoch);
            CanonicalProtobuf.uint64Bits(out, 8, maximumRetryUntil);
            CanonicalProtobuf.bytes(out, 9, limits.canonicalBytes());
            CanonicalProtobuf.bytes(out, 10, resourceContractDigest);
        });
    }

    public byte[] digest() {
        return Bytes.sha256(DOMAIN, fields());
    }

    public byte[] canonicalBytes() {
        return Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 11, digest())));
    }

    public static TargetSloWriterContract decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 11, false, "Target SLO writer contract");
        QueryCodecSupport.requireNumbers(
                f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, "Target SLO writer contract");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown SLO writer contract schema");
        }
        final var value = new TargetSloWriterContract(
                TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 16),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                AuthorIdentity.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                QueryCodecSupport.uint32Bits(f.get(5), 6),
                QueryCodecSupport.uint64Bits(f.get(6), 7),
                QueryCodecSupport.uint64Bits(f.get(7), 8),
                Limits.decode(QueryCodecSupport.bytes(f.get(8), 9)),
                QueryCodecSupport.fixed(f.get(9), 10, 32));
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(10), 11, 32))) {
            throw new IllegalArgumentException("SLO writer declaration digest differs");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Target SLO writer contract");
        return value;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetSloWriterContract contract
                && Arrays.equals(canonicalBytes(), contract.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
