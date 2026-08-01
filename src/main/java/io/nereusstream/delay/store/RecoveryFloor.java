package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.SourcePosition;

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
        if (catalogGeneration <= 0 || includedMutationSequence < 0) {
            throw new IllegalArgumentException("invalid recovery floor counters");
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
                Bytes.u64be(catalogGeneration), Bytes.lp32(appliedSourcePosition.canonicalBytes()),
                Bytes.u64be(includedMutationSequence), evidenceCursorDigest, floorDigest);
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
                Bytes.u64be(generation), Bytes.lp32(position.canonicalBytes()), Bytes.u64be(sequence), evidence);
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
}
