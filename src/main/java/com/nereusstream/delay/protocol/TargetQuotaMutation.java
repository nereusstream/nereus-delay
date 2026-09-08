package com.nereusstream.delay.protocol;

import java.util.Arrays;

/** Stable source sequence/frontier plus an optional local Claim ordinal; local work never renumbers source replay. */
public record TargetQuotaMutation(long sequence, SourcePosition source, byte[] mutationDigest, long localClaimOrdinal) {
    public static final int MAX_SOURCE_CANONICAL_BYTES = 11 + 4 + TargetSourcePosition.MAX_CANONICAL_BYTES + 34;
    public static final int MAX_CANONICAL_BYTES = MAX_SOURCE_CANONICAL_BYTES + 11;

    public TargetQuotaMutation(final long sequence, final SourcePosition source, final byte[] mutationDigest) {
        this(sequence, source, mutationDigest, 0);
    }

    public TargetQuotaMutation {
        if (sequence == 0) {
            throw new IllegalArgumentException("quota source sequence must be nonzero");
        }
        source = TargetSourcePosition.requireBounded(source);
        mutationDigest = TargetCompatibilityCodec.assigned(mutationDigest, 32, "quotaMutationDigest");
    }

    @Override
    public byte[] mutationDigest() {
        return Bytes.copy(mutationDigest);
    }

    public boolean isLocalClaim() {
        return localClaimOrdinal != 0;
    }

    public void requireSourceApplied() {
        if (isLocalClaim()) {
            throw new IllegalArgumentException("local Claim stamp cannot represent a source-applied record");
        }
    }

    public static long increment(final long prior) {
        if (prior == -1L) {
            throw new IllegalStateException("quota revision/sequence exhausted");
        }
        return prior + 1;
    }

    /** Source mutations remain strictly ordered even after intervening local Claims. */
    public void requireAfter(final TargetQuotaMutation prior) {
        if (isLocalClaim()
                || (prior != null
                        && (Long.compareUnsigned(sequence, prior.sequence) <= 0
                                || source.compareTo(prior.source) <= 0))) {
            throw new IllegalStateException("quota mutation is not a later source-applied operation");
        }
    }

    public void requireStoreSuccessorOf(final TargetQuotaMutation prior) {
        if (prior != null) {
            prior.requireAtOrBefore(this);
            if (sequence == prior.sequence && Long.compareUnsigned(localClaimOrdinal, prior.localClaimOrdinal) <= 0) {
                throw new IllegalStateException("quota accounting stamp must strictly advance");
            }
        }
    }

    public void requireAtOrBefore(final TargetQuotaMutation later) {
        requireAtOrBefore(later.sequence, later.source);
        if (sequence == later.sequence
                && (Long.compareUnsigned(localClaimOrdinal, later.localClaimOrdinal) > 0
                        || (localClaimOrdinal == later.localClaimOrdinal && !equals(later)))) {
            throw new IllegalStateException("quota local ordinal or exact operation digest disagrees");
        }
    }

    /** A Store source frontier has no Claim ordinal; actual aggregate bytes still participate in the read guard. */
    public void requireAtOrBefore(final long laterSequence, final SourcePosition frontier) {
        if (frontier == null || Long.compareUnsigned(sequence, laterSequence) > 0) {
            throw new IllegalStateException("quota mutation is ahead of the source sequence/frontier");
        }
        TargetSourcePosition.requireBounded(frontier);
        final int order = source.compareTo(frontier);
        if (order > 0
                || (order == 0) != (sequence == laterSequence)
                || (order == 0 && !Arrays.equals(source.canonicalBytes(), frontier.canonicalBytes()))) {
            throw new IllegalStateException("quota source sequence/frontier or metadata disagrees");
        }
    }

    /** A Floor without a local ordinal cannot certify a later local write at its own source position. */
    public void requireCoveredByFloor(final RecoveryFloorRef floor) {
        requireAtOrBefore(floor.includedMutationSequence(), floor.appliedSourcePosition());
        if (isLocalClaim() && source.compareTo(floor.appliedSourcePosition()) == 0) {
            throw new IllegalStateException("Floor must advance beyond an unbound local Claim ordinal");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint64Bits(out, 1, sequence);
            CanonicalProtobuf.bytes(out, 2, source.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, mutationDigest);
            if (isLocalClaim()) {
                CanonicalProtobuf.uint64Bits(out, 4, localClaimOrdinal);
            }
        });
    }

    public static TargetQuotaMutation decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 4, false, "TargetQuotaMutation");
        QueryCodecSupport.requireNumbers(
                fields, fields.size() == 4 ? new int[] {1, 2, 3, 4} : new int[] {1, 2, 3}, "TargetQuotaMutation");
        final var result = new TargetQuotaMutation(
                QueryCodecSupport.uint64Bits(fields.get(0), 1),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32),
                fields.size() == 4 ? QueryCodecSupport.uint64Bits(fields.get(3), 4) : 0);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaMutation");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQuotaMutation that
                && sequence == that.sequence
                && localClaimOrdinal == that.localClaimOrdinal
                && Arrays.equals(source.canonicalBytes(), that.source.canonicalBytes())
                && Arrays.equals(mutationDigest, that.mutationDigest);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
