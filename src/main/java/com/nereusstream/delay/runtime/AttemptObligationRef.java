package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Canonical locator for one open publish attempt.
 *
 * <p>This is the registry-shaped core projection used by the generation runtime
 * index. The key, state, key hash and reference digest are checked as one
 * closed value.</p>
 */
public final class AttemptObligationRef {
    public static final int HASH_LENGTH = 32;

    private final byte[] publishAttemptId;
    private final int generation;
    private final AttemptLedgerState ledgerState;
    private final byte[] encodedInflightKey;
    private final byte[] inflightKeySha256;
    private final byte[] refDigest;

    public AttemptObligationRef(
            final byte[] publishAttemptId,
            final int generation,
            final AttemptLedgerState ledgerState,
            final byte[] encodedInflightKey) {
        Bytes.requireLength(publishAttemptId, HASH_LENGTH, "publishAttemptId");
        this.publishAttemptId = Bytes.copy(publishAttemptId);
        this.generation = generation;
        this.ledgerState = java.util.Objects.requireNonNull(ledgerState, "ledgerState");
        if (encodedInflightKey == null || encodedInflightKey.length == 0) {
            throw new IllegalArgumentException("encodedInflightKey must not be empty");
        }
        this.encodedInflightKey = Bytes.copy(encodedInflightKey);
        if (this.encodedInflightKey.length != 2 + 8 + 4 + HASH_LENGTH
                || this.encodedInflightKey[1] != 1
                || this.encodedInflightKey[0] != (byte) (ledgerState == AttemptLedgerState.PUBLISHING ? 2 : 3)) {
            throw new IllegalArgumentException("inflight key tag does not match attempt ledger state");
        }
        this.inflightKeySha256 = Bytes.sha256(this.encodedInflightKey);
        this.refDigest =
                digest(this.publishAttemptId, generation, ledgerState, this.encodedInflightKey, this.inflightKeySha256);
    }

    private AttemptObligationRef(
            final byte[] publishAttemptId,
            final int generation,
            final AttemptLedgerState ledgerState,
            final byte[] encodedInflightKey,
            final byte[] inflightKeySha256,
            final byte[] refDigest) {
        this(publishAttemptId, generation, ledgerState, encodedInflightKey);
        if (!Bytes.constantTimeEquals(this.inflightKeySha256, inflightKeySha256)
                || !Bytes.constantTimeEquals(this.refDigest, refDigest)) {
            throw new IllegalArgumentException("attempt obligation digest mismatch");
        }
    }

    public byte[] publishAttemptId() {
        return Bytes.copy(publishAttemptId);
    }

    public int generation() {
        return generation;
    }

    public AttemptLedgerState ledgerState() {
        return ledgerState;
    }

    public byte[] encodedInflightKey() {
        return Bytes.copy(encodedInflightKey);
    }

    public byte[] inflightKeySha256() {
        return Bytes.copy(inflightKeySha256);
    }

    public byte[] refDigest() {
        return Bytes.copy(refDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, publishAttemptId);
            CanonicalProtobuf.uint32Bits(output, 2, generation);
            CanonicalProtobuf.uint32(output, 3, ledgerState.wireValue());
            CanonicalProtobuf.bytes(output, 4, encodedInflightKey);
            CanonicalProtobuf.bytes(output, 5, inflightKeySha256);
            CanonicalProtobuf.bytes(output, 6, refDigest);
        });
    }

    public static AttemptObligationRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(new CanonicalProtobuf.Reader(encoded));
        if (fields.size() != 6) {
            throw new IllegalArgumentException("attempt obligation fields are incomplete or unknown");
        }
        final byte[] id = fixed(fields.get(0), 1, HASH_LENGTH);
        final long generation = uint32(fields.get(1), 2);
        final AttemptLedgerState state = AttemptLedgerState.fromWire(varint(fields.get(2), 3));
        final byte[] key = bytes(fields.get(3), 4);
        final byte[] keyHash = fixed(fields.get(4), 5, HASH_LENGTH);
        final byte[] digest = fixed(fields.get(5), 6, HASH_LENGTH);
        final AttemptObligationRef result = new AttemptObligationRef(id, (int) generation, state, key, keyHash, digest);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical attempt obligation reference");
        }
        return result;
    }

    private static byte[] digest(
            final byte[] id,
            final int generation,
            final AttemptLedgerState state,
            final byte[] key,
            final byte[] keyHash) {
        final byte[] fields = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, id);
            CanonicalProtobuf.uint32Bits(output, 2, generation);
            CanonicalProtobuf.uint32(output, 3, state.wireValue());
            CanonicalProtobuf.bytes(output, 4, key);
            CanonicalProtobuf.bytes(output, 5, keyHash);
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-attempt-obligation-ref-v1\0"), fields);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static long varint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid attempt obligation varint field " + number);
        }
        return field.unsignedValue();
    }

    private static long uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = varint(field, number);
        if (value > 0xffff_ffffL) {
            throw new IllegalArgumentException("attempt obligation uint32 field is outside its wire range: " + number);
        }
        return value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid attempt obligation bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        final byte[] result = bytes(field, number);
        Bytes.requireLength(result, length, "attempt obligation field " + number);
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof AttemptObligationRef that)) {
            return false;
        }
        return generation == that.generation
                && ledgerState == that.ledgerState
                && Arrays.equals(publishAttemptId, that.publishAttemptId)
                && Arrays.equals(encodedInflightKey, that.encodedInflightKey)
                && Arrays.equals(inflightKeySha256, that.inflightKeySha256)
                && Arrays.equals(refDigest, that.refDigest);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(generation, ledgerState);
        result = 31 * result + Arrays.hashCode(publishAttemptId);
        result = 31 * result + Arrays.hashCode(encodedInflightKey);
        result = 31 * result + Arrays.hashCode(inflightKeySha256);
        result = 31 * result + Arrays.hashCode(refDigest);
        return result;
    }
}
