package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Authenticated proof that a prepared Control Operation was not registered.
 * A timeout or session ambiguity cannot construct either proof branch.
 */
public final class ControlNonPersistenceProof {
    public static final int HASH_LENGTH = 32;

    private final ControlNonPersistenceProofKind kind;
    private final byte[] operationId;
    private final byte[] preparedDigest;
    private final byte[] oxiaTransactionSha256;
    private final byte[] authenticatedResponseSha256;
    private final byte[] proofDigest;

    private ControlNonPersistenceProof(
            final ControlNonPersistenceProofKind kind,
            final byte[] operationId,
            final byte[] preparedDigest,
            final byte[] oxiaTransactionSha256,
            final byte[] authenticatedResponseSha256,
            final byte[] proofDigest) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.operationId = nonZero(operationId, "operationId");
        this.preparedDigest = fixed(preparedDigest, "preparedDigest");
        this.oxiaTransactionSha256 = optionalFixed(oxiaTransactionSha256, "oxiaTransactionSha256");
        this.authenticatedResponseSha256 = optionalFixed(authenticatedResponseSha256, "authenticatedResponseSha256");
        validatePresence(kind, this.oxiaTransactionSha256, this.authenticatedResponseSha256);
        this.proofDigest = fixed(proofDigest, "proofDigest");
    }

    public static ControlNonPersistenceProof create(
            final ControlNonPersistenceProofKind kind,
            final byte[] operationId,
            final byte[] preparedDigest,
            final byte[] oxiaTransactionSha256,
            final byte[] authenticatedResponseSha256) {
        final byte[] fields =
                canonicalFields(kind, operationId, preparedDigest, oxiaTransactionSha256, authenticatedResponseSha256);
        return new ControlNonPersistenceProof(
                kind,
                operationId,
                preparedDigest,
                oxiaTransactionSha256,
                authenticatedResponseSha256,
                Bytes.sha256(fields));
    }

    public static ControlNonPersistenceProof decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ControlNonPersistenceProof");
        if (fields.size() < 4 || fields.size() > 6) {
            throw new IllegalArgumentException("ControlNonPersistenceProof fields are incomplete or unknown");
        }
        final ControlNonPersistenceProofKind kind =
                ControlNonPersistenceProofKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        if (fields.get(1).number() != 2 || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("ControlNonPersistenceProof fixed field order is invalid");
        }
        int index = 3;
        byte[] transaction = null;
        if (index < fields.size() && fields.get(index).number() == 4) {
            transaction = QueryCodecSupport.fixed(fields.get(index++), 4, HASH_LENGTH);
        }
        byte[] response = null;
        if (index < fields.size() && fields.get(index).number() == 5) {
            response = QueryCodecSupport.fixed(fields.get(index++), 5, HASH_LENGTH);
        }
        if (index != fields.size() - 1 || fields.get(index).number() != 6) {
            throw new IllegalArgumentException("ControlNonPersistenceProof proof digest is missing or out of order");
        }
        final byte[] operationId = QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH);
        final byte[] preparedDigest = QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH);
        final byte[] digest = QueryCodecSupport.fixed(fields.get(index), 6, HASH_LENGTH);
        final ControlNonPersistenceProof result =
                new ControlNonPersistenceProof(kind, operationId, preparedDigest, transaction, response, digest);
        if (!Bytes.constantTimeEquals(
                digest, Bytes.sha256(canonicalFields(kind, operationId, preparedDigest, transaction, response)))) {
            throw new IllegalArgumentException("ControlNonPersistenceProof digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlNonPersistenceProof");
        return result;
    }

    public ControlNonPersistenceProofKind kind() {
        return kind;
    }

    public byte[] operationId() {
        return Bytes.copy(operationId);
    }

    public byte[] preparedDigest() {
        return Bytes.copy(preparedDigest);
    }

    public byte[] oxiaTransactionSha256() {
        return oxiaTransactionSha256 == null ? null : Bytes.copy(oxiaTransactionSha256);
    }

    public byte[] authenticatedResponseSha256() {
        return authenticatedResponseSha256 == null ? null : Bytes.copy(authenticatedResponseSha256);
    }

    public byte[] proofDigest() {
        return Bytes.copy(proofDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFieldsOneThroughFive(output);
            CanonicalProtobuf.bytes(output, 6, proofDigest);
        });
    }

    private void writeFieldsOneThroughFive(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, kind.wireValue());
        CanonicalProtobuf.bytes(output, 2, operationId);
        CanonicalProtobuf.bytes(output, 3, preparedDigest);
        if (oxiaTransactionSha256 != null) {
            CanonicalProtobuf.bytes(output, 4, oxiaTransactionSha256);
        }
        if (authenticatedResponseSha256 != null) {
            CanonicalProtobuf.bytes(output, 5, authenticatedResponseSha256);
        }
    }

    private static byte[] canonicalFields(
            final ControlNonPersistenceProofKind kind,
            final byte[] operationId,
            final byte[] preparedDigest,
            final byte[] transaction,
            final byte[] response) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, kind.wireValue());
            CanonicalProtobuf.bytes(output, 2, operationId);
            CanonicalProtobuf.bytes(output, 3, preparedDigest);
            if (transaction != null) {
                CanonicalProtobuf.bytes(output, 4, transaction);
            }
            if (response != null) {
                CanonicalProtobuf.bytes(output, 5, response);
            }
        });
    }

    private static void validatePresence(
            final ControlNonPersistenceProofKind kind, final byte[] transaction, final byte[] response) {
        if (kind == ControlNonPersistenceProofKind.BEFORE_OXIA_OWNERSHIP && (transaction != null || response != null)) {
            throw new IllegalArgumentException("BEFORE_OXIA_OWNERSHIP cannot carry Oxia evidence");
        }
        if (kind == ControlNonPersistenceProofKind.OXIA_CONDITIONAL_REJECTION
                && (transaction == null || response == null)) {
            throw new IllegalArgumentException("OXIA_CONDITIONAL_REJECTION requires complete Oxia evidence");
        }
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        return Bytes.copy(value);
    }

    private static byte[] optionalFixed(final byte[] value, final String name) {
        return value == null ? null : fixed(value, name);
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        final byte[] result = fixed(value, name);
        for (byte current : result) {
            if (current != 0) {
                return result;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlNonPersistenceProof that
                && kind == that.kind
                && Arrays.equals(operationId, that.operationId)
                && Arrays.equals(preparedDigest, that.preparedDigest)
                && Arrays.equals(oxiaTransactionSha256, that.oxiaTransactionSha256)
                && Arrays.equals(authenticatedResponseSha256, that.authenticatedResponseSha256)
                && Arrays.equals(proofDigest, that.proofDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                kind,
                Arrays.hashCode(operationId),
                Arrays.hashCode(preparedDigest),
                Arrays.hashCode(oxiaTransactionSha256),
                Arrays.hashCode(authenticatedResponseSha256),
                Arrays.hashCode(proofDigest));
    }
}
