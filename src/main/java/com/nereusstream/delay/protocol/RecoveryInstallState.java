package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Canonical local install/open projection for one Store Incarnation.
 *
 * <p>This value records the last physical install phase; it is not an
 * ownership, Recovery Pin or catalog authority. A store may be reused only
 * after the owning recovery authority validates the separate lineage/base and
 * Floor projections.</p>
 */
public final class RecoveryInstallState {
    private static final int ID_LENGTH = 16;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-recovery-install-state\0");

    private final RecoveryInstallPhase phase;
    private final byte[] storeIncarnation;
    private final byte[] checkpointId;
    private final byte[] stateDigest;

    public RecoveryInstallState(
            final RecoveryInstallPhase phase, final byte[] storeIncarnation, final byte[] checkpointId) {
        this.phase = Objects.requireNonNull(phase, "phase");
        this.storeIncarnation = nonZeroFixed(storeIncarnation, ID_LENGTH, "storeIncarnation");
        this.checkpointId = checkpointId == null ? null : nonZeroFixed(checkpointId, ID_LENGTH, "checkpointId");
        this.stateDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToFour());
    }

    public RecoveryInstallPhase phase() {
        return phase;
    }

    public byte[] storeIncarnation() {
        return Bytes.copy(storeIncarnation);
    }

    public byte[] checkpointId() {
        return checkpointId == null ? null : Bytes.copy(checkpointId);
    }

    public byte[] stateDigest() {
        return Bytes.copy(stateDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToFour());
            CanonicalProtobuf.bytes(output, 5, stateDigest);
        });
    }

    public static RecoveryInstallState decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "RecoveryInstallState");
        if (fields.size() != 4 && fields.size() != 5) {
            throw new IllegalArgumentException("invalid RecoveryInstallState field count");
        }
        QueryCodecSupport.requireNumbers(
                fields,
                fields.size() == 4 ? new int[] {1, 2, 3, 5} : new int[] {1, 2, 3, 4, 5},
                "RecoveryInstallState");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != 1) {
            throw new IllegalArgumentException("unsupported RecoveryInstallState version");
        }
        final RecoveryInstallState result = new RecoveryInstallState(
                RecoveryInstallPhase.fromWire(QueryCodecSupport.uint32(fields.get(1), 2)),
                QueryCodecSupport.fixed(fields.get(2), 3, ID_LENGTH),
                fields.size() == 5 ? QueryCodecSupport.fixed(fields.get(3), 4, ID_LENGTH) : null);
        final int digestIndex = fields.size() == 5 ? 4 : 3;
        final byte[] digest = QueryCodecSupport.fixed(fields.get(digestIndex), 5, 32);
        if (!Bytes.constantTimeEquals(digest, result.stateDigest)) {
            throw new IllegalArgumentException("RecoveryInstallState digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RecoveryInstallState");
        return result;
    }

    private byte[] fieldsOneToFour() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint32(output, 2, phase.wireValue());
            CanonicalProtobuf.bytes(output, 3, storeIncarnation);
            if (checkpointId != null) {
                CanonicalProtobuf.bytes(output, 4, checkpointId);
            }
        });
    }

    private static byte[] nonZeroFixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        boolean nonZero = false;
        for (byte current : value) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RecoveryInstallState that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
