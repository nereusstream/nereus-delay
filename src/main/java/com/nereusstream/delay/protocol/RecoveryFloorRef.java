package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Canonical typed Recovery Floor reference from Registry §8/§13.
 *
 * <p>This is a protocol projection only. It does not perform an Oxia CAS,
 * pin a catalog member, or prove source/evidence retention.</p>
 */
public final class RecoveryFloorRef {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-recovery-floor-ref\0");

    private final byte[] recoveryLineageId;
    private final byte[] checkpointId;
    private final byte[] manifestSha256;
    private final long catalogGeneration;
    private final SourcePosition appliedSourcePosition;
    private final long includedMutationSequence;
    private final List<EvidenceCursor> evidenceCursors;
    private final byte[] floorDigest;

    public RecoveryFloorRef(
            final byte[] recoveryLineageId,
            final byte[] checkpointId,
            final byte[] manifestSha256,
            final long catalogGeneration,
            final SourcePosition appliedSourcePosition,
            final long includedMutationSequence,
            final List<EvidenceCursor> evidenceCursors) {
        this.recoveryLineageId = nonZeroFixed(recoveryLineageId, ID_LENGTH, "recoveryLineageId");
        this.checkpointId = nonZeroFixed(checkpointId, ID_LENGTH, "checkpointId");
        this.manifestSha256 = fixed(manifestSha256, HASH_LENGTH, "manifestSha256");
        if (catalogGeneration == 0) {
            throw new IllegalArgumentException("catalogGeneration must be nonzero");
        }
        this.catalogGeneration = catalogGeneration;
        this.appliedSourcePosition = Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        this.includedMutationSequence = includedMutationSequence;
        this.evidenceCursors = sortedUnique(evidenceCursors);
        this.floorDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToSeven());
    }

    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    public byte[] manifestSha256() {
        return Bytes.copy(manifestSha256);
    }

    public long catalogGeneration() {
        return catalogGeneration;
    }

    public SourcePosition appliedSourcePosition() {
        return appliedSourcePosition;
    }

    public long includedMutationSequence() {
        return includedMutationSequence;
    }

    public List<EvidenceCursor> evidenceCursors() {
        return evidenceCursors;
    }

    public byte[] floorDigest() {
        return Bytes.copy(floorDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToSeven());
            CanonicalProtobuf.bytes(output, 8, floorDigest);
        });
    }

    public static RecoveryFloorRef decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 7
                || fields.get(0).number() != 1
                || fields.get(fields.size() - 1).number() != 8) {
            throw new IllegalArgumentException("invalid RecoveryFloorRef field order");
        }
        final int lastCursor = fields.size() - 1;
        if (fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3
                || fields.get(3).number() != 4
                || fields.get(4).number() != 5
                || fields.get(5).number() != 6) {
            throw new IllegalArgumentException("RecoveryFloorRef has invalid required field order");
        }
        final List<EvidenceCursor> cursors = new ArrayList<>();
        for (int index = 6; index < lastCursor; index++) {
            if (fields.get(index).number() != 7) {
                throw new IllegalArgumentException("RecoveryFloorRef cursor must use field 7");
            }
            cursors.add(EvidenceCursor.decode(QueryCodecSupport.nested(fields.get(index), 7)));
        }
        final RecoveryFloorRef result = new RecoveryFloorRef(
                QueryCodecSupport.fixed(fields.get(0), 1, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(1), 2, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(3), 4),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(4), 5)),
                QueryCodecSupport.uint(fields.get(5), 6),
                cursors);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(lastCursor), 8, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(digest, result.floorDigest)) {
            throw new IllegalArgumentException("RecoveryFloorRef digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RecoveryFloorRef");
        return result;
    }

    private byte[] fieldsOneToSeven() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, recoveryLineageId);
            CanonicalProtobuf.bytes(output, 2, checkpointId);
            CanonicalProtobuf.bytes(output, 3, manifestSha256);
            CanonicalProtobuf.uint64Bits(output, 4, catalogGeneration);
            CanonicalProtobuf.bytes(output, 5, QueryCodecSupport.encodeSourcePosition(appliedSourcePosition));
            CanonicalProtobuf.uint64Bits(output, 6, includedMutationSequence);
            for (EvidenceCursor cursor : evidenceCursors) {
                CanonicalProtobuf.bytes(output, 7, cursor.canonicalBytes());
            }
        });
    }

    private static List<EvidenceCursor> sortedUnique(final List<EvidenceCursor> values) {
        Objects.requireNonNull(values, "evidenceCursors");
        final List<EvidenceCursor> result = new ArrayList<>(values.size());
        EvidenceCursor previous = null;
        for (EvidenceCursor value : values) {
            Objects.requireNonNull(value, "evidence cursor");
            if (previous != null && previous.compareTo(value) >= 0) {
                throw new IllegalArgumentException("RecoveryFloorRef cursors must be sorted and unique");
            }
            result.add(value);
            previous = value;
        }
        return Collections.unmodifiableList(result);
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZeroFixed(final byte[] value, final int length, final String name) {
        final byte[] result = fixed(value, length, name);
        if (Arrays.stream(toIntArray(result)).allMatch(item -> item == 0)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return result;
    }

    private static int[] toIntArray(final byte[] value) {
        final int[] result = new int[value.length];
        for (int index = 0; index < value.length; index++) {
            result[index] = value[index];
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RecoveryFloorRef that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
