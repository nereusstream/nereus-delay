package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Definitive Control registration rejection carrying a non-persistence proof. */
public final class ControlDefinitelyNotRecorded {
    public static final int HASH_LENGTH = 32;

    private final byte[] preparedDigest;
    private final ControlNonPersistenceProof proof;
    private final StableError error;

    public ControlDefinitelyNotRecorded(
            final byte[] preparedDigest, final ControlNonPersistenceProof proof, final StableError error) {
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

    public ControlNonPersistenceProof proof() {
        return proof;
    }

    public StableError error() {
        return error;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, preparedDigest);
            CanonicalProtobuf.bytes(output, 2, proof.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, error.canonicalBytes());
        });
    }

    public static ControlDefinitelyNotRecorded decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ControlDefinitelyNotRecorded");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "ControlDefinitelyNotRecorded");
        final ControlDefinitelyNotRecorded result = new ControlDefinitelyNotRecorded(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                ControlNonPersistenceProof.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                StableError.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlDefinitelyNotRecorded");
        return result;
    }

    private static StableError requireControlError(final StableError value) {
        final StableError result = Objects.requireNonNull(value, "error");
        if (result.stage() != FailureStage.CONTROL) {
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
        return other instanceof ControlDefinitelyNotRecorded that
                && Arrays.equals(preparedDigest, that.preparedDigest)
                && proof.equals(that.proof)
                && error.equals(that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(preparedDigest), proof, error);
    }
}
