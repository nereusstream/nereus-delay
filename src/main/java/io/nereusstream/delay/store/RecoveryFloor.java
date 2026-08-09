package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.SourcePosition;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable source/evidence boundary below which checkpoint and payload
 * reclamation may eventually be proven safe.
 */
public record RecoveryFloor(
        byte[] recoveryLineageId,
        byte[] checkpointId,
        byte[] manifestSha256,
        long catalogGeneration,
        SourcePosition appliedSourcePosition,
        long includedMutationSequence,
        byte[] evidenceCursorDigest,
        byte[] floorDigest) {
    public RecoveryFloor {
        requireNonZero(recoveryLineageId, 16, "recoveryLineageId");
        requireNonZero(checkpointId, 16, "checkpointId");
        Bytes.requireLength(manifestSha256, 32, "manifestSha256");
        Objects.requireNonNull(appliedSourcePosition, "appliedSourcePosition");
        Bytes.requireLength(evidenceCursorDigest, 32, "evidenceCursorDigest");
        Bytes.requireLength(floorDigest, 32, "floorDigest");
        if (catalogGeneration == 0) {
            throw new IllegalArgumentException("invalid recovery floor catalog generation");
        }
        recoveryLineageId = Bytes.copy(recoveryLineageId);
        checkpointId = Bytes.copy(checkpointId);
        manifestSha256 = Bytes.copy(manifestSha256);
        evidenceCursorDigest = Bytes.copy(evidenceCursorDigest);
        floorDigest = Bytes.copy(floorDigest);
        final byte[] expected = computeDigest(recoveryLineageId, checkpointId, manifestSha256, catalogGeneration,
                appliedSourcePosition, includedMutationSequence, evidenceCursorDigest);
        if (!Bytes.constantTimeEquals(expected, floorDigest)) {
            throw new IllegalArgumentException("recovery floor digest mismatch");
        }
    }

    public static RecoveryFloor create(final byte[] recoveryLineageId, final byte[] checkpointId,
                                       final byte[] manifestSha256, final long catalogGeneration,
                                       final SourcePosition appliedSourcePosition,
                                       final long includedMutationSequence, final byte[] evidenceCursorDigest) {
        return new RecoveryFloor(recoveryLineageId, checkpointId, manifestSha256, catalogGeneration,
                appliedSourcePosition, includedMutationSequence, evidenceCursorDigest,
                computeDigest(recoveryLineageId, checkpointId, manifestSha256, catalogGeneration,
                        appliedSourcePosition, includedMutationSequence, evidenceCursorDigest));
    }

    @Override
    public byte[] recoveryLineageId() {
        return Bytes.copy(recoveryLineageId);
    }

    @Override
    public byte[] checkpointId() {
        return Bytes.copy(checkpointId);
    }

    @Override
    public byte[] manifestSha256() {
        return Bytes.copy(manifestSha256);
    }

    @Override
    public byte[] evidenceCursorDigest() {
        return Bytes.copy(evidenceCursorDigest);
    }

    @Override
    public byte[] floorDigest() {
        return Bytes.copy(floorDigest);
    }

    public byte[] canonicalBytes() {
        return Bytes.concat(Bytes.u32be(1), recoveryLineageId, checkpointId, manifestSha256,
                Bytes.u64beBits(catalogGeneration), Bytes.lp32(appliedSourcePosition.canonicalBytes()),
                Bytes.u64beBits(includedMutationSequence), evidenceCursorDigest, floorDigest);
    }

    /** Decodes and revalidates the exact local scalar Floor projection. */
    public static RecoveryFloor decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        final ByteBuffer input = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        requireRemaining(input, Integer.BYTES + 16 + 16 + 32 + Long.BYTES + Integer.BYTES
                + Long.BYTES + 32 + 32);
        if (input.getInt() != 1) {
            throw new IllegalArgumentException("unsupported RecoveryFloor version");
        }
        final byte[] lineage = readFixed(input, 16, "recoveryLineageId");
        final byte[] checkpoint = readFixed(input, 16, "checkpointId");
        final byte[] manifest = readFixed(input, 32, "manifestSha256");
        final long generation = input.getLong();
        final long positionLength = Integer.toUnsignedLong(input.getInt());
        if (positionLength > input.remaining()) {
            throw new IllegalArgumentException("RecoveryFloor source position is truncated");
        }
        final byte[] positionBytes = readFixed(input, Math.toIntExact(positionLength), "appliedSourcePosition");
        final SourcePosition position = SourcePositionCodec.decode(positionBytes);
        requireRemaining(input, Long.BYTES + 32 + 32);
        final long sequence = input.getLong();
        final byte[] evidence = readFixed(input, 32, "evidenceCursorDigest");
        final byte[] floorDigest = readFixed(input, 32, "floorDigest");
        if (input.hasRemaining()) {
            throw new IllegalArgumentException("RecoveryFloor has trailing bytes");
        }
        final RecoveryFloor result = new RecoveryFloor(lineage, checkpoint, manifest, generation, position,
                sequence, evidence, floorDigest);
        if (!Bytes.constantTimeEquals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("RecoveryFloor is not canonical");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof RecoveryFloor that)) {
            return false;
        }
        return catalogGeneration == that.catalogGeneration
                && includedMutationSequence == that.includedMutationSequence
                && appliedSourcePosition.equals(that.appliedSourcePosition)
                && Arrays.equals(recoveryLineageId, that.recoveryLineageId)
                && Arrays.equals(checkpointId, that.checkpointId)
                && Arrays.equals(manifestSha256, that.manifestSha256)
                && Arrays.equals(evidenceCursorDigest, that.evidenceCursorDigest)
                && Arrays.equals(floorDigest, that.floorDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(recoveryLineageId), Arrays.hashCode(checkpointId),
                Arrays.hashCode(manifestSha256), catalogGeneration, appliedSourcePosition,
                includedMutationSequence, Arrays.hashCode(evidenceCursorDigest), Arrays.hashCode(floorDigest));
    }

    private static byte[] computeDigest(final byte[] lineage, final byte[] checkpoint, final byte[] manifest,
                                         final long generation, final SourcePosition position, final long sequence,
                                         final byte[] evidence) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-recovery-floor-v1\0"), lineage, checkpoint, manifest,
                Bytes.u64beBits(generation), Bytes.lp32(position.canonicalBytes()),
                Bytes.u64beBits(sequence), evidence);
    }

    private static void requireNonZero(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        if (Arrays.stream(toIntArray(value)).allMatch(item -> item == 0)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
    }

    private static int[] toIntArray(final byte[] value) {
        final int[] result = new int[value.length];
        for (int index = 0; index < value.length; index++) {
            result[index] = value[index];
        }
        return result;
    }

    private static byte[] readFixed(final ByteBuffer input, final int length, final String name) {
        requireRemaining(input, length);
        final byte[] result = new byte[length];
        input.get(result);
        return result;
    }

    private static void requireRemaining(final ByteBuffer input, final int length) {
        if (length < 0 || input.remaining() < length) {
            throw new IllegalArgumentException("RecoveryFloor is truncated");
        }
    }

    private static void requireRemaining(final ByteBuffer input, final int length, final String name) {
        try {
            requireRemaining(input, length);
        } catch (IllegalArgumentException truncated) {
            throw new IllegalArgumentException("RecoveryFloor field is truncated: " + name, truncated);
        }
    }
}
