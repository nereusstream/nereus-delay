package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Closed, bounded Target head summary; the selected Message still requires a Store read before Claim. */
public final class TargetHeadRef {
    public static final int MAX_CANONICAL_BYTES = 120 + 43 + 6 + 11 + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-head\0");
    private final byte[] key;
    private final DelayMessageId messageId;
    private final int generation;
    private final long timeEpochMs;
    private final TargetPartitionId target;
    private final TargetKeyCodec.Domain domain;
    private final boolean nativeCandidate;
    private final byte[] digest;

    public TargetHeadRef(
            final byte[] key, final DelayMessageId messageId, final int generation, final long timeEpochMs) {
        Objects.requireNonNull(key, "key");
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        if (timeEpochMs < 0) {
            throw new IllegalArgumentException("negative Target head time");
        }
        if (key.length > 0 && key[0] == TargetKeyCodec.ORDER_HEAD_TAG) {
            final var decoded = TargetKeyCodec.decodeOrderedHead(key);
            target = decoded.target();
            domain = decoded.domain();
            nativeCandidate = false;
            if (decoded.eligibleAtEpochMs() != timeEpochMs) {
                throw new IllegalArgumentException("ordered head time disagrees with its key");
            }
        } else {
            final var decoded = TargetKeyCodec.decodeCandidate(key);
            target = decoded.target();
            domain = decoded.domain();
            nativeCandidate = decoded.kind() == TargetKeyCodec.CandidateKind.NATIVE;
            if (!decoded.messageId().equals(messageId)
                    || decoded.generation() != generation
                    || decoded.timeEpochMs() != timeEpochMs) {
                throw new IllegalArgumentException("Target head identity/time disagrees with its key");
            }
        }
        this.key = Bytes.copy(key);
        this.generation = generation;
        this.timeEpochMs = timeEpochMs;
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public byte[] key() {
        return Bytes.copy(key);
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int generation() {
        return generation;
    }

    public long timeEpochMs() {
        return timeEpochMs;
    }

    public TargetPartitionId target() {
        return target;
    }

    public TargetKeyCodec.Domain domain() {
        return domain;
    }

    public boolean nativeCandidate() {
        return nativeCandidate;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 5, digest);
        });
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, key);
            CanonicalProtobuf.bytes(out, 2, messageId.bytes());
            CanonicalProtobuf.uint32Bits(out, 3, generation);
            CanonicalProtobuf.uint64(out, 4, timeEpochMs);
        });
    }

    public static TargetHeadRef decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target head exceeds its byte bound");
        }
        final var fields = QueryCodecSupport.read(encoded, "TargetHeadRef");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "TargetHeadRef");
        final TargetHeadRef head = new TargetHeadRef(
                QueryCodecSupport.bytes(fields.get(0), 1),
                new DelayMessageId(QueryCodecSupport.fixed(fields.get(1), 2, DelayMessageId.LENGTH)),
                QueryCodecSupport.uint32Bits(fields.get(2), 3),
                QueryCodecSupport.uint(fields.get(3), 4));
        if (!Bytes.constantTimeEquals(head.digest, QueryCodecSupport.fixed(fields.get(4), 5, 32))) {
            throw new IllegalArgumentException("Target head digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, head.canonicalBytes(), "TargetHeadRef");
        return head;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetHeadRef that
                && Arrays.equals(key, that.key)
                && messageId.equals(that.messageId)
                && generation == that.generation
                && timeEpochMs == that.timeEpochMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), messageId, generation, timeEpochMs);
    }
}
