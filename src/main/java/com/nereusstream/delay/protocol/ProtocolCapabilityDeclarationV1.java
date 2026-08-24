package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Canonical capability declaration published by one Worker session.
 *
 * <p>The declaration is deliberately separate from the local protocol
 * activation projection.  A Route/activation authority may use it to prove
 * that the current eligible reader set supports one exact protocol tuple
 * before it writes a source-ordered activation marker.</p>
 */
public final class ProtocolCapabilityDeclarationV1 {
    public static final int VERSION = 1;
    public static final int DIGEST_LENGTH = 32;
    private static final int MAX_TUPLES = 32;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-protocol-capability-declaration-v1\0");

    private final String workerId;
    private final byte[] workerIdentity;
    private final List<ProtocolTupleV1> supportedTuples;
    private final long capabilityEpoch;
    private final byte[] sessionIdentity;
    private final byte[] declarationDigest;

    public ProtocolCapabilityDeclarationV1(
            final String workerId,
            final byte[] workerIdentity,
            final List<ProtocolTupleV1> supportedTuples,
            final long capabilityEpoch,
            final byte[] sessionIdentity) {
        this(workerId, workerIdentity, supportedTuples, capabilityEpoch, sessionIdentity, null);
    }

    private ProtocolCapabilityDeclarationV1(
            final String workerId,
            final byte[] workerIdentity,
            final List<ProtocolTupleV1> supportedTuples,
            final long capabilityEpoch,
            final byte[] sessionIdentity,
            final byte[] declarationDigest) {
        this.workerId = canonicalText(workerId, "workerId");
        this.workerIdentity = fixed(workerIdentity, "workerIdentity");
        requireNonZero(this.workerIdentity, "workerIdentity");
        this.supportedTuples = sortedTuples(supportedTuples);
        if (capabilityEpoch <= 0) {
            throw new IllegalArgumentException("capabilityEpoch must be positive");
        }
        this.capabilityEpoch = capabilityEpoch;
        this.sessionIdentity = fixed(sessionIdentity, "sessionIdentity");
        requireNonZero(this.sessionIdentity, "sessionIdentity");
        final byte[] expected = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToFive());
        if (declarationDigest != null && !Bytes.constantTimeEquals(declarationDigest, expected)) {
            throw new IllegalArgumentException("protocol capability declaration digest mismatch");
        }
        this.declarationDigest = expected;
    }

    public String workerId() {
        return workerId;
    }

    public byte[] workerIdentity() {
        return Bytes.copy(workerIdentity);
    }

    public List<ProtocolTupleV1> supportedTuples() {
        return supportedTuples;
    }

    public long capabilityEpoch() {
        return capabilityEpoch;
    }

    public byte[] sessionIdentity() {
        return Bytes.copy(sessionIdentity);
    }

    public byte[] declarationDigest() {
        return Bytes.copy(declarationDigest);
    }

    public boolean supports(final ProtocolTupleV1 tuple) {
        Objects.requireNonNull(tuple, "tuple");
        return supportedTuples.contains(tuple);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToFive());
            CanonicalProtobuf.bytes(output, 7, declarationDigest);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("protocol capability declaration is too large");
        }
        return encoded;
    }

    public static ProtocolCapabilityDeclarationV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length == 0 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid protocol capability declaration length");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 7
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("protocol capability declaration is incomplete");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported protocol capability declaration version");
        }
        final String workerId = text(QueryCodecSupport.bytes(fields.get(1), 2), "workerId");
        final byte[] workerIdentity = QueryCodecSupport.fixed(fields.get(2), 3, DIGEST_LENGTH);
        final List<ProtocolTupleV1> tuples = new ArrayList<>();
        int index = 3;
        while (index < fields.size() - 3 && fields.get(index).number() == 4) {
            tuples.add(ProtocolTupleV1.decode(QueryCodecSupport.nested(fields.get(index++), 4)));
        }
        if (index + 3 != fields.size()
                || fields.get(index).number() != 5
                || fields.get(index + 1).number() != 6) {
            throw new IllegalArgumentException("protocol capability declaration fields are out of order");
        }
        final long epoch = QueryCodecSupport.uint(fields.get(index), 5);
        final byte[] sessionIdentity = QueryCodecSupport.fixed(fields.get(index + 1), 6, DIGEST_LENGTH);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(index + 2), 7, DIGEST_LENGTH);
        final ProtocolCapabilityDeclarationV1 result =
                new ProtocolCapabilityDeclarationV1(workerId, workerIdentity, tuples, epoch, sessionIdentity, digest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolCapabilityDeclarationV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolCapabilityDeclarationV1 that
                && workerId.equals(that.workerId)
                && Arrays.equals(workerIdentity, that.workerIdentity)
                && supportedTuples.equals(that.supportedTuples)
                && capabilityEpoch == that.capabilityEpoch
                && Arrays.equals(sessionIdentity, that.sessionIdentity)
                && Arrays.equals(declarationDigest, that.declarationDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                workerId,
                Arrays.hashCode(workerIdentity),
                supportedTuples,
                capabilityEpoch,
                Arrays.hashCode(sessionIdentity),
                Arrays.hashCode(declarationDigest));
    }

    private byte[] fieldsOneToFive() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, workerId.getBytes(StandardCharsets.UTF_8));
            CanonicalProtobuf.bytes(output, 3, workerIdentity);
            for (ProtocolTupleV1 tuple : supportedTuples) {
                CanonicalProtobuf.bytes(output, 4, tuple.canonicalBytes());
            }
            CanonicalProtobuf.uint64Bits(output, 5, capabilityEpoch);
            CanonicalProtobuf.bytes(output, 6, sessionIdentity);
        });
    }

    private static List<ProtocolTupleV1> sortedTuples(final List<ProtocolTupleV1> values) {
        Objects.requireNonNull(values, "supportedTuples");
        if (values.isEmpty() || values.size() > MAX_TUPLES) {
            throw new IllegalArgumentException("supportedTuples must be bounded and non-empty");
        }
        final List<ProtocolTupleV1> result = new ArrayList<>(values);
        result.sort(
                Comparator.comparing(ProtocolTupleV1::canonicalBytes, ProtocolCapabilityDeclarationV1::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).equals(result.get(index))) {
                throw new IllegalArgumentException("duplicate supported protocol tuple");
            }
        }
        return List.copyOf(result);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, DIGEST_LENGTH, name);
        return Bytes.copy(value);
    }

    private static void requireNonZero(final byte[] value, final String name) {
        for (byte element : value) {
            if (element != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static String text(final byte[] value, final String name) {
        final String result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(result.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException(name + " is not valid UTF-8");
        }
        return canonicalText(result, name);
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Byte.toUnsignedInt(left[index]) - Byte.toUnsignedInt(right[index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
