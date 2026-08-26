package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Canonical protocol/version tuple used by route control snapshots. */
public final class ProtocolTuple {
    public static final int CLIENT_COMMAND = ShardLogFrame.CLIENT_COMMAND_KIND;
    public static final int SYSTEM_MUTATION = ShardLogFrame.SYSTEM_MUTATION_KIND;
    private static final long UINT32_MAX = 0xffff_ffffL;

    private final long framingVersion;
    private final long logEnvelopeVersion;
    private final int recordKind;
    private final long envelopeVersion;
    private final long bodyVersion;

    public ProtocolTuple(
            final long framingVersion,
            final long logEnvelopeVersion,
            final int recordKind,
            final long envelopeVersion,
            final long bodyVersion) {
        this.framingVersion = positiveUint32(framingVersion, "framingVersion");
        this.logEnvelopeVersion = positiveUint32(logEnvelopeVersion, "logEnvelopeVersion");
        if (recordKind != CLIENT_COMMAND && recordKind != SYSTEM_MUTATION) {
            throw new IllegalArgumentException("ProtocolTuple recordKind is not a record kind");
        }
        this.recordKind = recordKind;
        this.envelopeVersion = positiveUint32(envelopeVersion, "envelopeVersion");
        this.bodyVersion = positiveUint32(bodyVersion, "bodyVersion");
    }

    /** The currently managed Client Command tuple. */
    public static ProtocolTuple managedCommand() {
        return new ProtocolTuple(1, 1, CLIENT_COMMAND, 1, 1);
    }

    public long framingVersion() {
        return framingVersion;
    }

    public long logEnvelopeVersion() {
        return logEnvelopeVersion;
    }

    public int recordKind() {
        return recordKind;
    }

    public long envelopeVersion() {
        return envelopeVersion;
    }

    public long bodyVersion() {
        return bodyVersion;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, framingVersion);
            CanonicalProtobuf.uint32(output, 2, logEnvelopeVersion);
            CanonicalProtobuf.uint32(output, 3, recordKind);
            CanonicalProtobuf.uint32(output, 4, envelopeVersion);
            CanonicalProtobuf.uint32(output, 5, bodyVersion);
        });
    }

    public static ProtocolTuple decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ProtocolTuple");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "ProtocolTuple");
        final ProtocolTuple result = new ProtocolTuple(
                uint32(fields.get(0), 1),
                uint32(fields.get(1), 2),
                uint32Int(fields.get(2), 3),
                uint32(fields.get(3), 4),
                uint32(fields.get(4), 5));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolTuple");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolTuple that
                && framingVersion == that.framingVersion
                && logEnvelopeVersion == that.logEnvelopeVersion
                && recordKind == that.recordKind
                && envelopeVersion == that.envelopeVersion
                && bodyVersion == that.bodyVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(framingVersion, logEnvelopeVersion, recordKind, envelopeVersion, bodyVersion);
    }

    private static long uint32(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = QueryCodecSupport.uint(field, number);
        if (value <= 0 || value > UINT32_MAX) {
            throw new IllegalArgumentException("ProtocolTuple field must be a positive uint32: " + number);
        }
        return value;
    }

    private static int uint32Int(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = QueryCodecSupport.uint(field, number);
        if (value < 0 || value > UINT32_MAX) {
            throw new IllegalArgumentException("ProtocolTuple recordKind is outside uint32 range");
        }
        return (int) value;
    }

    private static long positiveUint32(final long value, final String name) {
        if (value <= 0 || value > UINT32_MAX) {
            throw new IllegalArgumentException(name + " must be a positive uint32");
        }
        return value;
    }
}
