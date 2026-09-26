package com.nereusstream.delay.protocol;

import java.util.Objects;

/** Typed reader and writer for the existing source-ordered EXPIRE_GENERATION body. */
public final class TargetExpireGenerationBody {
    public static final int MAX_CANONICAL_BYTES = TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES + 128;

    private final ShardId shard;
    private final long retryUntil;
    private final DelayMessageId messageId;
    private final int generation;
    private final long expireAt;
    private final TrustedUtcIntervalEvidence proof;

    public TargetExpireGenerationBody(
            final ShardId shard,
            final long retryUntil,
            final DelayMessageId messageId,
            final int generation,
            final long expireAt,
            final TrustedUtcIntervalEvidence proof) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.proof = Objects.requireNonNull(proof, "proof");
        if (!shard.equals(messageId.routingId().shardId())
                || retryUntil < 0
                || expireAt < 0
                || proof.sourceId().length > TargetChannelIdentity.MAX_TIME_SOURCE_ID_BYTES
                || proof.canonicalBytes().length > TargetChannelIdentity.MAX_TIME_EVIDENCE_BYTES) {
            throw new IllegalArgumentException("Expire Generation source, time or proof exceeds its contract");
        }
        this.retryUntil = retryUntil;
        this.generation = generation;
        this.expireAt = expireAt;
    }

    public ShardId shard() {
        return shard;
    }

    public long retryUntil() {
        return retryUntil;
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int generation() {
        return generation;
    }

    public long expireAt() {
        return expireAt;
    }

    public TrustedUtcIntervalEvidence proof() {
        return proof;
    }

    public byte[] logicalOperationIdentity() {
        return SystemMutation.computeExpiryLogicalIdentity(messageId, generation, expireAt);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, new ShardSubject(shard).canonicalBytes());
            CanonicalProtobuf.uint32(out, 2, SystemMutationType.EXPIRE_GENERATION.wireValue());
            CanonicalProtobuf.int64(out, 3, retryUntil);
            CanonicalProtobuf.bytes(out, 10, messageId.bytes());
            CanonicalProtobuf.uint32Bits(out, 11, generation);
            CanonicalProtobuf.int64(out, 12, expireAt);
            CanonicalProtobuf.bytes(out, 13, proof.canonicalBytes());
        });
    }

    public static TargetExpireGenerationBody decode(final byte[] canonical) {
        Objects.requireNonNull(canonical, "canonical");
        if (canonical.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Expire Generation body exceeds its Target bound");
        }
        final var fields = SystemMutationBodyCodec.fields(SystemMutationType.EXPIRE_GENERATION, canonical);
        final var body = new TargetExpireGenerationBody(
                ShardSubject.decode(QueryCodecSupport.bytes(fields.get(0), 1)).shardId(),
                QueryCodecSupport.uint(fields.get(2), 3),
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(3), 10, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(4), 11),
                QueryCodecSupport.uint(fields.get(5), 12),
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.bytes(fields.get(6), 13)));
        QueryCodecSupport.requireCanonical(canonical, body.canonicalBytes(), "Expire Generation body");
        return body;
    }
}
