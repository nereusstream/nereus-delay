package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Durable value stored beside a native-candidate timeline key. */
public final class NativeCandidateRef {
    public static final int SCHEMA_GENERATION = 1;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-native-candidate");

    private final DelayMessageId messageId;
    private final int generation;
    private final long candidateAtEpochMs;
    private final byte[] timelineKey;
    private final byte[] timelineKeySha256;
    private final byte[] digest;

    public NativeCandidateRef(
            final DelayMessageId messageId,
            final int generation,
            final long candidateAtEpochMs,
            final byte[] timelineKey) {
        this.messageId = Objects.requireNonNull(messageId, "messageId");
        this.generation = generation;
        if (candidateAtEpochMs < 0) {
            throw new IllegalArgumentException("candidateAtEpochMs must be non-negative");
        }
        this.candidateAtEpochMs = candidateAtEpochMs;
        this.timelineKey = Bytes.copy(Objects.requireNonNull(timelineKey, "timelineKey"));
        if (this.timelineKey.length < 2 || this.timelineKey[0] != 7 || this.timelineKey[1] != 1) {
            throw new IllegalArgumentException("native candidate key must use the registered tag 7");
        }
        this.timelineKeySha256 = Bytes.sha256(this.timelineKey);
        this.digest = computeDigest();
    }

    private NativeCandidateRef(
            final DelayMessageId messageId,
            final int generation,
            final long candidateAtEpochMs,
            final byte[] timelineKey,
            final byte[] keyHash,
            final byte[] digest) {
        this(messageId, generation, candidateAtEpochMs, timelineKey);
        if (!Arrays.equals(this.timelineKeySha256, keyHash) || !Arrays.equals(this.digest, digest)) {
            throw new IllegalArgumentException("native candidate digest mismatch");
        }
    }

    public DelayMessageId messageId() {
        return messageId;
    }

    public int generation() {
        return generation;
    }

    public long candidateAtEpochMs() {
        return candidateAtEpochMs;
    }

    public byte[] timelineKey() {
        return Bytes.copy(timelineKey);
    }

    public byte[] timelineKeySha256() {
        return Bytes.copy(timelineKeySha256);
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, SCHEMA_GENERATION);
            CanonicalProtobuf.bytes(output, 2, messageId.bytes());
            CanonicalProtobuf.uint32Bits(output, 3, generation);
            CanonicalProtobuf.uint64(output, 4, candidateAtEpochMs);
            CanonicalProtobuf.bytes(output, 5, timelineKey);
            CanonicalProtobuf.bytes(output, 6, timelineKeySha256);
            CanonicalProtobuf.bytes(output, 7, digest);
        });
    }

    public static NativeCandidateRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() != 7) {
            throw new IllegalArgumentException("native candidate has an unexpected field count");
        }
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("native candidate field order mismatch");
            }
        }
        if (uint(fields.get(0), 1) != SCHEMA_GENERATION) {
            throw new IllegalArgumentException("unsupported native candidate schema generation");
        }
        final NativeCandidateRef result = new NativeCandidateRef(
                new DelayMessageId(bytes(fields.get(1), 2)),
                intBits(fields.get(2), 3),
                uint(fields.get(3), 4),
                bytes(fields.get(4), 5),
                fixed(fields.get(5), 6),
                fixed(fields.get(6), 7));
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical native candidate");
        }
        return result;
    }

    private byte[] computeDigest() {
        return Bytes.sha256(DIGEST_DOMAIN, CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, messageId.bytes());
            CanonicalProtobuf.uint32Bits(output, 2, generation);
            CanonicalProtobuf.uint64(output, 3, candidateAtEpochMs);
            CanonicalProtobuf.bytes(output, 4, timelineKey);
            CanonicalProtobuf.bytes(output, 5, timelineKeySha256);
        }));
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("native candidate field is not bytes: " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        Bytes.requireLength(value, HASH_LENGTH, "native candidate hash");
        return value;
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("native candidate field is not uint: " + number);
        }
        return field.unsignedValue();
    }

    private static int intBits(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = uint(field, number);
        if (value > 0xffff_ffffL) {
            throw new IllegalArgumentException("native candidate generation is outside uint32");
        }
        return (int) value;
    }
}
