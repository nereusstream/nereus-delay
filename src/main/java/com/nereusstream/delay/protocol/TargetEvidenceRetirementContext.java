package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.IngressFenceState;
import com.nereusstream.delay.store.StoreMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Signed preparation facts, not authentication of Catalog closure, Native retention, or deletion eligibility. */
public record TargetEvidenceRetirementContext(
        long closureRevision,
        byte[] closureDigest,
        long closureBytes,
        StoreMetadata preparedStore,
        TargetQuotaMutation preparedCut,
        IngressFenceState fence,
        List<Use> uses,
        byte[] copyManifestDigest,
        long copyManifestBytes,
        TargetQuotaIncarnation feeOwner) {
    public static final int MAX_USES = 128;
    public static final int MAX_CLOSURE_BYTES = 16 * 1024 * 1024;
    public static final int MAX_COPY_BYTES = 64 * 1024 * 1024;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaMutation.MAX_CANONICAL_BYTES
            + TargetQuotaIncarnation.MAX_CANONICAL_BYTES
            + MAX_USES * 192
            + 2048;

    public enum Conclusion {
        RETAINED_APPLIED(1),
        RETAINED_REJECTED(2),
        SOURCE_FENCED(3);
        private final int wire;

        Conclusion(int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        private static Conclusion decode(int wire) {
            for (var value : values()) {
                if (value.wire == wire) {
                    return value;
                }
            }
            throw new IllegalArgumentException("unknown closed-use conclusion");
        }
    }

    public record Use(
            byte[] id, byte[] hash, long retryUntil, Conclusion conclusion, byte[] firstDigest, byte[] positionDigest) {
        public Use {
            id = assigned(id);
            hash = assigned(hash);
            Objects.requireNonNull(conclusion);
            if (retryUntil < 0) {
                throw new IllegalArgumentException("negative sealed retry deadline");
            }
            firstDigest = Bytes.copy(Objects.requireNonNull(firstDigest));
            positionDigest = Bytes.copy(Objects.requireNonNull(positionDigest));
            if (conclusion == Conclusion.SOURCE_FENCED) {
                if (firstDigest.length != 0 || positionDigest.length != 0) {
                    throw new IllegalArgumentException("absent fenced use cannot claim a retained first");
                }
            } else {
                firstDigest = assigned(firstDigest);
                positionDigest = assigned(positionDigest);
            }
        }

        @Override
        public byte[] id() {
            return Bytes.copy(id);
        }

        @Override
        public byte[] hash() {
            return Bytes.copy(hash);
        }

        @Override
        public byte[] firstDigest() {
            return Bytes.copy(firstDigest);
        }

        @Override
        public byte[] positionDigest() {
            return Bytes.copy(positionDigest);
        }

        private byte[] preparedBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, id);
                CanonicalProtobuf.bytes(out, 2, hash);
                CanonicalProtobuf.int64(out, 3, retryUntil);
            });
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, id);
                CanonicalProtobuf.bytes(out, 2, hash);
                CanonicalProtobuf.int64(out, 3, retryUntil);
                CanonicalProtobuf.uint32(out, 4, conclusion.wire);
                if (conclusion != Conclusion.SOURCE_FENCED) {
                    CanonicalProtobuf.bytes(out, 5, firstDigest);
                    CanonicalProtobuf.bytes(out, 6, positionDigest);
                }
            });
        }

        private static Use decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, 192, 6, false, "closed prepared use");
            if (f.size() < 4) {
                throw new IllegalArgumentException("incomplete closed prepared use");
            }
            final var conclusion = Conclusion.decode(QueryCodecSupport.uint32(f.get(3), 4));
            QueryCodecSupport.requireNumbers(
                    f,
                    conclusion == Conclusion.SOURCE_FENCED ? new int[] {1, 2, 3, 4} : new int[] {1, 2, 3, 4, 5, 6},
                    "closed prepared use");
            final var value = new Use(
                    QueryCodecSupport.fixed(f.get(0), 1, 32),
                    QueryCodecSupport.fixed(f.get(1), 2, 32),
                    QueryCodecSupport.uint(f.get(2), 3),
                    conclusion,
                    f.size() == 4 ? new byte[0] : QueryCodecSupport.fixed(f.get(4), 5, 32),
                    f.size() == 4 ? new byte[0] : QueryCodecSupport.fixed(f.get(5), 6, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "closed prepared use");
            return value;
        }
    }

    public TargetEvidenceRetirementContext {
        closureDigest = assigned(closureDigest);
        copyManifestDigest = assigned(copyManifestDigest);
        Objects.requireNonNull(preparedStore);
        Objects.requireNonNull(preparedCut);
        Objects.requireNonNull(fence);
        Objects.requireNonNull(feeOwner);
        uses = List.copyOf(uses);
        if (closureRevision == 0
                || closureBytes < 1
                || closureBytes > MAX_CLOSURE_BYTES
                || copyManifestBytes < 1
                || copyManifestBytes > MAX_COPY_BYTES
                || uses.size() > MAX_USES
                || !preparedStore.shardId().equals(preparedCut.source().shardId())) {
            throw new IllegalArgumentException("invalid bounded retirement context");
        }
        String previous = null;
        for (var use : uses) {
            final String id = Bytes.hex(use.id);
            if (previous != null && previous.compareTo(id) >= 0) {
                throw new IllegalArgumentException("retirement uses are not unique/sorted");
            }
            previous = id;
            if (use.conclusion == Conclusion.SOURCE_FENCED
                    && (fence.proofId() == null || fence.closedThroughEpochMs() < use.retryUntil)) {
                throw new IllegalArgumentException("fenced use lacks the declared committed fence boundary");
            }
        }
    }

    @Override
    public byte[] closureDigest() {
        return Bytes.copy(closureDigest);
    }

    @Override
    public byte[] copyManifestDigest() {
        return Bytes.copy(copyManifestDigest);
    }

    public byte[] preparedUsesDigest() {
        return Bytes.sha256(
                Bytes.utf8("nereus-delay-target-evidence-closed-prepared-uses\0"), CanonicalProtobuf.message(out -> {
                    for (var use : uses) {
                        CanonicalProtobuf.bytes(out, 1, use.preparedBytes());
                    }
                }));
    }

    public void requireResource(TargetExternalEvidenceObjectResource resource) {
        if (!preparedStore.shardId().equals(resource.shard())
                || !feeOwner.identity().shard().equals(resource.shard())
                || !Arrays.equals(feeOwner.tenantScope(), resource.tenantScope())
                || !Arrays.equals(feeOwner.recoveryLineage(), resource.recoveryLineage())
                || copyManifestBytes < resource.canonicalValueBytes()) {
            throw new IllegalArgumentException("retirement context changes object/fee/source scope");
        }
        final TargetQuotaIncarnation original;
        if (resource.provenance() instanceof TargetExternalEvidenceObjectResource.Recovery p) {
            original = p.owner().primaryOwner();
            p.owner().adoptedAt().requireAtOrBefore(preparedCut);
        } else if (resource.provenance() instanceof TargetExternalEvidenceObjectResource.Protection p) {
            original = p.owner().primaryOwner();
            p.owner().createdAt().requireAtOrBefore(preparedCut);
        } else if (resource.provenance() instanceof TargetExternalEvidenceObjectResource.Anchor p) {
            original = p.owner().primaryOwner();
            p.owner().createdAt().requireAtOrBefore(preparedCut);
        } else {
            original = null;
        }
        if (original == null
                ? feeOwner.identity().kind() != TargetQuotaIdentity.Kind.SHARD
                : !Arrays.equals(original.canonicalBytes(), feeOwner.canonicalBytes())) {
            throw new IllegalArgumentException("retirement changed original frozen fee owner");
        }
        feeOwner.latestMutation().requireAtOrBefore(preparedCut);
    }

    public byte[] canonicalBytes() {
        final var encoded = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.uint64Bits(out, 2, closureRevision);
            CanonicalProtobuf.bytes(out, 3, closureDigest);
            CanonicalProtobuf.uint64(out, 4, closureBytes);
            CanonicalProtobuf.bytes(out, 5, preparedStore.encode());
            CanonicalProtobuf.bytes(out, 6, preparedCut.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, fence.canonicalBytes());
            for (var use : uses) {
                CanonicalProtobuf.bytes(out, 8, use.canonicalBytes());
            }
            CanonicalProtobuf.bytes(out, 9, copyManifestDigest);
            CanonicalProtobuf.uint64(out, 10, copyManifestBytes);
            CanonicalProtobuf.bytes(out, 11, feeOwner.canonicalBytes());
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("retirement context exceeds byte bound");
        }
        return encoded;
    }

    public static TargetEvidenceRetirementContext decode(byte[] encoded) {
        final var f =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, MAX_USES + 10, true, "retirement context");
        if (f.size() < 10) {
            throw new IllegalArgumentException("incomplete retirement context");
        }
        QueryCodecSupport.requireNumbers(f.subList(0, 7), new int[] {1, 2, 3, 4, 5, 6, 7}, "retirement context prefix");
        if (QueryCodecSupport.uint(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown retirement context schema");
        }
        final var uses = new ArrayList<Use>();
        int n = 7;
        while (n < f.size() && f.get(n).number() == 8) {
            uses.add(Use.decode(QueryCodecSupport.nested(f.get(n++), 8)));
        }
        QueryCodecSupport.requireNumbers(f.subList(n, f.size()), new int[] {9, 10, 11}, "retirement context suffix");
        final var value = new TargetEvidenceRetirementContext(
                QueryCodecSupport.uint64Bits(f.get(1), 2),
                QueryCodecSupport.fixed(f.get(2), 3, 32),
                QueryCodecSupport.uint(f.get(3), 4),
                StoreMetadata.decode(QueryCodecSupport.bytes(f.get(4), 5)),
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(5), 6)),
                IngressFenceState.decode(QueryCodecSupport.bytes(f.get(6), 7)),
                uses,
                QueryCodecSupport.fixed(f.get(n), 9, 32),
                QueryCodecSupport.uint(f.get(n + 1), 10),
                TargetQuotaIncarnation.decode(QueryCodecSupport.nested(f.get(n + 2), 11)));
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "retirement context");
        return value;
    }

    private static byte[] assigned(byte[] value) {
        return TargetCompatibilityCodec.assigned(value, 32, "retirement digest/identity");
    }
}
