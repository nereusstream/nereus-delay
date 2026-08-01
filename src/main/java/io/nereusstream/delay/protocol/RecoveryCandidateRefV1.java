package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical reference to either a local store or a catalog checkpoint. */
public final class RecoveryCandidateRefV1 {
    private static final int ID_LENGTH = 16;
    private static final int HASH_LENGTH = 32;
    private static final byte[] DIGEST_DOMAIN =
            Bytes.utf8("nereus-delay-recovery-candidate-ref-v1\0");

    private final RecoveryCandidateKindV1 kind;
    private final byte[] recoveryLineageId;
    private final byte[] checkpointId;
    private final byte[] manifestSha256;
    private final byte[] storeIncarnation;
    private final byte[] candidateDigest;

    public RecoveryCandidateRefV1(final RecoveryCandidateKindV1 kind, final byte[] recoveryLineageId,
                                  final byte[] checkpointId, final byte[] manifestSha256,
                                  final byte[] storeIncarnation) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.recoveryLineageId = nonZeroFixed(recoveryLineageId, ID_LENGTH, "recoveryLineageId");
        this.checkpointId = nonZeroFixed(checkpointId, ID_LENGTH, "checkpointId");
        this.manifestSha256 = fixed(manifestSha256, HASH_LENGTH, "manifestSha256");
        if ((kind == RecoveryCandidateKindV1.LOCAL_STORE) != (storeIncarnation != null)) {
            throw new IllegalArgumentException("LOCAL_STORE requires store incarnation and catalog forbids it");
        }
        this.storeIncarnation = storeIncarnation == null ? null
                : nonZeroFixed(storeIncarnation, ID_LENGTH, "storeIncarnation");
        this.candidateDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToFive());
    }

    public RecoveryCandidateKindV1 kind() {
        return kind;
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

    public byte[] storeIncarnation() {
        return storeIncarnation == null ? null : Bytes.copy(storeIncarnation);
    }

    public byte[] candidateDigest() {
        return Bytes.copy(candidateDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToFive());
            CanonicalProtobuf.bytes(output, 6, candidateDigest);
        });
    }

    public static RecoveryCandidateRefV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "RecoveryCandidateRefV1");
        if (fields.size() != 5 && fields.size() != 6) {
            throw new IllegalArgumentException("invalid RecoveryCandidateRefV1 field count");
        }
        QueryCodecSupport.requireNumbers(fields, fields.size() == 5
                ? new int[]{1, 2, 3, 4, 6} : new int[]{1, 2, 3, 4, 5, 6},
                "RecoveryCandidateRefV1");
        final RecoveryCandidateKindV1 kind = RecoveryCandidateKindV1.fromWire(
                QueryCodecSupport.uint(fields.get(0), 1));
        final RecoveryCandidateRefV1 result = new RecoveryCandidateRefV1(kind,
                QueryCodecSupport.fixed(fields.get(1), 2, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(2), 3, ID_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                fields.size() == 6 ? QueryCodecSupport.fixed(fields.get(4), 5, ID_LENGTH) : null);
        final int digestIndex = fields.size() == 6 ? 5 : 4;
        final byte[] digest = QueryCodecSupport.fixed(fields.get(digestIndex), 6, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(digest, result.candidateDigest)) {
            throw new IllegalArgumentException("RecoveryCandidateRefV1 digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RecoveryCandidateRefV1");
        return result;
    }

    private byte[] fieldsOneToFive() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.bytes(output, 2, recoveryLineageId);
            CanonicalProtobuf.bytes(output, 3, checkpointId);
            CanonicalProtobuf.bytes(output, 4, manifestSha256);
            if (storeIncarnation != null) {
                CanonicalProtobuf.bytes(output, 5, storeIncarnation);
            }
        });
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
        return other instanceof RecoveryCandidateRefV1 that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(canonicalBytes());
    }
}
