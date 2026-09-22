package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Bounded Target reader for the existing TIME_FENCE body and its registered ProofId preimage. */
public final class TargetTimeFenceBody {
    public static final int MAX_CANONICAL_BYTES = TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES + 96;
    private static final byte[] PROOF_DOMAIN = Bytes.utf8("nereus-delay-time-fence-proof\0");
    private final ShardId shard;
    private final long retryUntil;
    private final long closeThrough;
    private final int fenceKeyVersion;
    private final TrustedUtcIntervalEvidence proof;
    private final byte[] proofId;

    public TargetTimeFenceBody(
            final ShardId shard,
            final long retryUntil,
            final long closeThrough,
            final int fenceKeyVersion,
            final TrustedUtcIntervalEvidence proof) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.proof = Objects.requireNonNull(proof, "proof");
        if (retryUntil < 0 || closeThrough < 0 || fenceKeyVersion == 0) {
            throw new IllegalArgumentException("invalid Target time fence boundary/key");
        }
        if (proof.sourceId().length > TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES
                || proof.canonicalBytes().length > TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES) {
            throw new IllegalArgumentException("Target time fence evidence exceeds its bound");
        }
        this.retryUntil = retryUntil;
        this.closeThrough = closeThrough;
        this.fenceKeyVersion = fenceKeyVersion;
        proofId = Bytes.sha256(
                PROOF_DOMAIN,
                shard.routeIncarnation().bytes(),
                Bytes.u32beBits(shard.partition()),
                Bytes.i64be(closeThrough),
                Bytes.u32beBits(fenceKeyVersion),
                Bytes.lp32(proof.canonicalBytes()));
    }

    public ShardId shard() {
        return shard;
    }

    public long retryUntil() {
        return retryUntil;
    }

    public long closeThrough() {
        return closeThrough;
    }

    public int fenceKeyVersion() {
        return fenceKeyVersion;
    }

    public TrustedUtcIntervalEvidence proof() {
        return proof;
    }

    public byte[] proofId() {
        return Bytes.copy(proofId);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.TIME_FENCE.wireValue());
            CanonicalProtobuf.int64(out, 3, retryUntil);
            CanonicalProtobuf.int64(out, 10, closeThrough);
            CanonicalProtobuf.uint32Bits(out, 11, fenceKeyVersion);
            CanonicalProtobuf.bytes(out, 12, proofId);
            CanonicalProtobuf.bytes(out, 13, proof.canonicalBytes());
        });
    }

    public static TargetTimeFenceBody decode(final byte[] encoded) {
        final var fields = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Target time fence");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 10, 11, 12, 13}, "Target time fence");
        if (QueryCodecSupport.uint32(fields.get(1), 2) != SystemMutationType.TIME_FENCE.wireValue()) {
            throw new IllegalArgumentException("not a TIME_FENCE body");
        }
        final byte[] subject = QueryCodecSupport.bytes(fields.getFirst(), 1);
        TargetCompatibilityCodec.read(subject, 24, 2, false, "Target time fence subject");
        final byte[] evidence = QueryCodecSupport.bytes(fields.getLast(), 13);
        final var proofFields = TargetCompatibilityCodec.read(
                evidence, TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES, 10, false, "Target time fence evidence");
        QueryCodecSupport.requireNumbers(
                proofFields,
                proofFields.size() == 10
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9},
                "Target time fence evidence");
        if (QueryCodecSupport.bytes(proofFields.get(3), 4).length > TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES) {
            throw new IllegalArgumentException("Target time fence sourceId exceeds its bound");
        }
        final var result = new TargetTimeFenceBody(
                ShardSubject.decode(subject).shardId(),
                QueryCodecSupport.uint(fields.get(2), 3),
                QueryCodecSupport.uint(fields.get(3), 10),
                QueryCodecSupport.uint32Bits(fields.get(4), 11),
                TrustedUtcIntervalEvidence.decode(evidence));
        if (!Bytes.constantTimeEquals(result.proofId, QueryCodecSupport.fixed(fields.get(5), 12, 32))) {
            throw new IllegalArgumentException("Target time fence ProofId mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target time fence");
        return result;
    }
}
