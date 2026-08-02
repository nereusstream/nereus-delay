package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Definitive Control registration rejection carrying a non-persistence proof. */
public final class ControlDefinitelyNotRecordedV1 {
    public static final int HASH_LENGTH = 32;

    private final byte[] preparedDigest;
    private final ControlNonPersistenceProofV1 proof;
    private final StableErrorV1 error;

    public ControlDefinitelyNotRecordedV1(final byte[] preparedDigest,
                                          final ControlNonPersistenceProofV1 proof,
                                          final StableErrorV1 error) {
        this.preparedDigest = fixed(preparedDigest, "preparedDigest");
        this.proof = Objects.requireNonNull(proof, "proof");
        if (!Bytes.constantTimeEquals(this.preparedDigest, proof.preparedDigest())) {
            throw new IllegalArgumentException("Control rejection prepared digest does not match proof");
        }
        this.error = requireControlError(error);
    }

    public byte[] preparedDigest() {
        return Bytes.copy(preparedDigest);
    }

    public ControlNonPersistenceProofV1 proof() {
        return proof;
    }

    public StableErrorV1 error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, preparedDigest);
            CanonicalProtobuf.bytes(output, 2, proof.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static ControlDefinitelyNotRecordedV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ControlDefinitelyNotRecordedV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3}, "ControlDefinitelyNotRecordedV1");
        final ControlDefinitelyNotRecordedV1 result = new ControlDefinitelyNotRecordedV1(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                ControlNonPersistenceProofV1.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                StableErrorV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlDefinitelyNotRecordedV1");
        return result;
    }

    private static StableErrorV1 requireControlError(final StableErrorV1 value) {
        final StableErrorV1 result = Objects.requireNonNull(value, "error");
        if (result.stage() != FailureStageV1.CONTROL) {
            throw new IllegalArgumentException("Control registration error must use CONTROL stage");
        }
        return result;
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlDefinitelyNotRecordedV1 that
                && Arrays.equals(preparedDigest, that.preparedDigest)
                && proof.equals(that.proof) && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(preparedDigest), proof, error);
    }
}
