package com.nereusstream.delay.protocol;

import java.util.Arrays;

/** Source-applied accounting stamp; its digest identifies the exact accepted Command or System Mutation bytes. */
public record TargetQuotaMutation(long sequence, SourcePosition source, byte[] mutationDigest) {
    public static final int MAX_CANONICAL_BYTES = 11 + 4 + TargetSourcePosition.MAX_CANONICAL_BYTES + 34;

    public TargetQuotaMutation {
        if (sequence == 0) {
            throw new IllegalArgumentException("quota mutation sequence must be nonzero");
        }
        source = TargetSourcePosition.requireBounded(source);
        mutationDigest = TargetCompatibilityCodec.assigned(mutationDigest, 32, "quotaMutationDigest");
    }

    @Override
    public byte[] mutationDigest() {
        return Bytes.copy(mutationDigest);
    }

    public static long increment(final long prior) {
        if (prior == -1L) {
            throw new IllegalStateException("quota revision/sequence exhausted");
        }
        return prior + 1;
    }

    public void requireAfter(final TargetQuotaMutation prior) {
        if (prior != null
                && (Long.compareUnsigned(sequence, prior.sequence) <= 0 || source.compareTo(prior.source) <= 0)) {
            throw new IllegalStateException("quota mutation is not later than the persisted accounting stamp");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint64Bits(out, 1, sequence);
            CanonicalProtobuf.bytes(out, 2, source.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, mutationDigest);
        });
    }

    public static TargetQuotaMutation decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "TargetQuotaMutation");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "TargetQuotaMutation");
        final var result = new TargetQuotaMutation(
                QueryCodecSupport.uint64Bits(fields.get(0), 1),
                TargetSourcePosition.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, 32));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetQuotaMutation");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetQuotaMutation that
                && sequence == that.sequence
                && Arrays.equals(source.canonicalBytes(), that.source.canonicalBytes())
                && Arrays.equals(mutationDigest, that.mutationDigest);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
