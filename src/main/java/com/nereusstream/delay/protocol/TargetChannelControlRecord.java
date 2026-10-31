package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable first-applied channel lifecycle event, including its original Control author and Source stamp. */
public final class TargetChannelControlRecord {
    public static final int VALUE_TYPE = 43;
    public static final int MAX_CANONICAL_BYTES =
            TargetChannelControlBody.MAX_CANONICAL_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 256;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-channel-control-record\0");

    private final TargetChannelControlBody body;
    private final TargetQuotaMutation mutation;
    private final ControlAuthor author;
    private final byte[] lineage;
    private final byte[] digest;

    public TargetChannelControlRecord(
            final TargetChannelControlBody body,
            final TargetQuotaMutation mutation,
            final ControlAuthor author,
            final byte[] lineage) {
        this.body = Objects.requireNonNull(body, "body");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        this.author = Objects.requireNonNull(author, "author");
        this.lineage = TargetCompatibilityCodec.assigned(lineage, 16, "recoveryLineage");
        mutation.requireSourceApplied();
        if (!body.shard().equals(mutation.source().shardId())) {
            throw new IllegalArgumentException("channel Control record belongs to another Source Shard");
        }
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public TargetChannelControlBody body() {
        return body;
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public ControlAuthor author() {
        return author;
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] key() {
        return TargetKeyCodec.channelControl(body.logicalIdentity());
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, body.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, author.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, lineage);
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 6, digest);
        });
    }

    public static TargetChannelControlRecord decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "Target channel Control record");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5, 6}, "Target channel Control record");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != 1) {
            throw new IllegalArgumentException("unknown channel Control record version");
        }
        final var result = new TargetChannelControlRecord(
                TargetChannelControlBody.decode(QueryCodecSupport.bytes(fields.get(1), 2)),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(fields.get(2), 3)),
                ControlAuthor.decode(QueryCodecSupport.bytes(fields.get(3), 4)),
                QueryCodecSupport.fixed(fields.get(4), 5, 16));
        if (!Arrays.equals(result.digest(), QueryCodecSupport.fixed(fields.get(5), 6, 32))) {
            throw new IllegalArgumentException("channel Control record digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target channel Control record");
        return result;
    }

    public static TargetChannelControlRecord decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId shard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.key()) || !shard.equals(result.body().shard())) {
            throw new IllegalArgumentException("channel Control record key/Shard mismatch");
        }
        return result;
    }
}
