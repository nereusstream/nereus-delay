package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical TrustedUtcIntervalEvidenceV1 used by source-ordered timing mutations. */
public final class TrustedUtcIntervalEvidence {
    public static final int HASH_LENGTH = 32;
    public static final int SIGNATURE_LENGTH = 64;

    public enum Source {
        KAFKA_LOG_APPEND_TIME(1),
        PULSAR_BROKER_ENTRY_TIME(2),
        CERTIFIED_HOST_CLOCK(3),
        SIGNED_TIME_SERVICE(4);

        private final int wireValue;

        Source(final int wireValue) {
            this.wireValue = wireValue;
        }

        public int wireValue() {
            return wireValue;
        }

        public static Source fromWire(final long value) {
            for (Source source : values()) {
                if (source.wireValue == value) {
                    return source;
                }
            }
            throw new IllegalArgumentException("unknown time evidence source: " + value);
        }
    }

    private final long earliestEpochMs;
    private final long latestEpochMs;
    private final Source source;
    private final byte[] sourceId;
    private final long sourceConfigGeneration;
    private final long sampleSequence;
    private final long monotonicAnchorNs;
    private final byte[] sourceEvidenceSha256;
    private final int sourceKeyVersion;
    private final byte[] sourceSignature;

    public TrustedUtcIntervalEvidence(final long earliestEpochMs, final long latestEpochMs, final Source source,
                                       final byte[] sourceId, final long sourceConfigGeneration,
                                       final long sampleSequence, final long monotonicAnchorNs,
                                       final byte[] sourceEvidenceSha256, final int sourceKeyVersion,
                                       final byte[] sourceSignature) {
        if (earliestEpochMs < 0 || latestEpochMs < earliestEpochMs || sourceKeyVersion < 0) {
            throw new IllegalArgumentException("invalid trusted UTC interval");
        }
        this.earliestEpochMs = earliestEpochMs;
        this.latestEpochMs = latestEpochMs;
        this.source = Objects.requireNonNull(source, "source");
        this.sourceId = nonEmpty(sourceId, "sourceId");
        if (sourceConfigGeneration < 0 || sampleSequence < 0 || monotonicAnchorNs < 0) {
            throw new IllegalArgumentException("trusted UTC counters must be non-negative");
        }
        this.sourceConfigGeneration = sourceConfigGeneration;
        this.sampleSequence = sampleSequence;
        this.monotonicAnchorNs = monotonicAnchorNs;
        this.sourceEvidenceSha256 = fixed(sourceEvidenceSha256, HASH_LENGTH, "sourceEvidenceSha256");
        this.sourceKeyVersion = sourceKeyVersion;
        this.sourceSignature = sourceSignature == null ? new byte[0] : Bytes.copy(sourceSignature);
        if (source == Source.SIGNED_TIME_SERVICE) {
            if (sourceKeyVersion <= 0) {
                throw new IllegalArgumentException("signed time service requires a source key version");
            }
            Bytes.requireLength(this.sourceSignature, SIGNATURE_LENGTH, "sourceSignature");
        } else if (sourceKeyVersion != 0 || this.sourceSignature.length != 0) {
            throw new IllegalArgumentException("unsigned time evidence cannot carry a key or signature");
        }
    }

    public long earliestEpochMs() {
        return earliestEpochMs;
    }

    public long latestEpochMs() {
        return latestEpochMs;
    }

    public Source source() {
        return source;
    }

    public byte[] sourceId() {
        return Bytes.copy(sourceId);
    }

    public long sourceConfigGeneration() {
        return sourceConfigGeneration;
    }

    public long sampleSequence() {
        return sampleSequence;
    }

    public long monotonicAnchorNs() {
        return monotonicAnchorNs;
    }

    public byte[] sourceEvidenceSha256() {
        return Bytes.copy(sourceEvidenceSha256);
    }

    public int sourceKeyVersion() {
        return sourceKeyVersion;
    }

    public byte[] sourceSignature() {
        return Bytes.copy(sourceSignature);
    }

    /** Requires the interval to prove that a source-ordered timing boundary has passed. */
    public void requireEarliestAtLeast(final long boundaryEpochMs) {
        if (boundaryEpochMs < 0 || earliestEpochMs < boundaryEpochMs) {
            throw new IllegalArgumentException("trusted UTC interval does not prove the boundary");
        }
    }

    /** Validates the activated maximum interval width without changing canonical bytes. */
    public void requireWidthAtMost(final long maxWidthMs) {
        if (maxWidthMs < 0 || latestEpochMs - earliestEpochMs > maxWidthMs) {
            throw new IllegalArgumentException("trusted UTC interval exceeds the activated clock bound");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.int64(output, 1, earliestEpochMs);
            CanonicalProtobuf.int64(output, 2, latestEpochMs);
            CanonicalProtobuf.uint32(output, 3, source.wireValue());
            CanonicalProtobuf.bytes(output, 4, sourceId);
            CanonicalProtobuf.int64(output, 5, sourceConfigGeneration);
            CanonicalProtobuf.int64(output, 6, sampleSequence);
            CanonicalProtobuf.int64(output, 7, monotonicAnchorNs);
            CanonicalProtobuf.bytes(output, 8, sourceEvidenceSha256);
            CanonicalProtobuf.uint32(output, 9, sourceKeyVersion);
            if (sourceSignature.length != 0) {
                CanonicalProtobuf.bytes(output, 10, sourceSignature);
            }
        });
    }

    public static TrustedUtcIntervalEvidence decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readAll(
                new CanonicalProtobuf.Reader(Objects.requireNonNull(encoded, "encoded")));
        if (fields.size() != 9 && fields.size() != 10) {
            throw new IllegalArgumentException("trusted UTC evidence fields are incomplete or unknown");
        }
        final TrustedUtcIntervalEvidence result = new TrustedUtcIntervalEvidence(
                signedNonNegative(fields.get(0), 1), signedNonNegative(fields.get(1), 2),
                Source.fromWire(unsigned(fields.get(2), 3)), bytes(fields.get(3), 4),
                unsigned(fields.get(4), 5), unsigned(fields.get(5), 6), unsigned(fields.get(6), 7),
                fixed(bytes(fields.get(7), 8), HASH_LENGTH, "sourceEvidenceSha256"),
                unsignedInt(fields.get(8), 9), fields.size() == 10
                ? fixed(bytes(fields.get(9), 10), SIGNATURE_LENGTH, "sourceSignature") : new byte[0]);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("non-canonical trusted UTC evidence");
        }
        return result;
    }

    private static long signedNonNegative(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value < 0) {
            throw new IllegalArgumentException("trusted UTC field exceeds signed range: " + number);
        }
        return value;
    }

    private static int unsignedInt(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("trusted UTC field exceeds Java int range: " + number);
        }
        return (int) value;
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid trusted UTC scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid trusted UTC bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    private static List<CanonicalProtobuf.Reader.Field> readAll(final CanonicalProtobuf.Reader reader) {
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }
}
