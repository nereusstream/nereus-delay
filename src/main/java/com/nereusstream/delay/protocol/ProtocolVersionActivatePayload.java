package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** ControlPayload field 1: activate one exact protocol tuple for eligible readers. */
public final class ProtocolVersionActivatePayload {
    private static final int HASH_LENGTH = 32;

    private final ProtocolTuple tuple;
    private final byte[] canonicalSchemaHash;
    private final byte[] compatibleReaderSetEvidenceHash;
    private final ArtifactGenerationSet artifactGenerationSet;
    private final byte[] manifestDigest;

    public ProtocolVersionActivatePayload(
            final ProtocolTuple tuple, final byte[] canonicalSchemaHash, final byte[] compatibleReaderSetEvidenceHash) {
        this.tuple = Objects.requireNonNull(tuple, "tuple");
        Bytes.requireLength(canonicalSchemaHash, HASH_LENGTH, "canonicalSchemaHash");
        Bytes.requireLength(compatibleReaderSetEvidenceHash, HASH_LENGTH, "compatibleReaderSetEvidenceHash");
        this.canonicalSchemaHash = Bytes.copy(canonicalSchemaHash);
        this.compatibleReaderSetEvidenceHash = Bytes.copy(compatibleReaderSetEvidenceHash);
        this.artifactGenerationSet = null;
        this.manifestDigest = new byte[0];
    }

    /** Creates a current activation payload bound to the full reset generation. */
    public ProtocolVersionActivatePayload(
            final ProtocolTuple tuple,
            final byte[] canonicalSchemaHash,
            final byte[] compatibleReaderSetEvidenceHash,
            final ArtifactGenerationSet artifactGenerationSet,
            final byte[] manifestDigest) {
        this.tuple = Objects.requireNonNull(tuple, "tuple");
        this.artifactGenerationSet = Objects.requireNonNull(artifactGenerationSet, "artifactGenerationSet");
        requireTupleBinding(tuple, artifactGenerationSet);
        Bytes.requireLength(canonicalSchemaHash, HASH_LENGTH, "canonicalSchemaHash");
        Bytes.requireLength(compatibleReaderSetEvidenceHash, HASH_LENGTH, "compatibleReaderSetEvidenceHash");
        Bytes.requireLength(manifestDigest, HASH_LENGTH, "manifestDigest");
        if (allZero(manifestDigest)) {
            throw new IllegalArgumentException("manifestDigest must be non-zero");
        }
        if (!Bytes.constantTimeEquals(canonicalSchemaHash, artifactGenerationSet.canonicalSchemaBundleHash())) {
            throw new IllegalArgumentException("activation schema hash does not match ArtifactGenerationSet");
        }
        this.canonicalSchemaHash = Bytes.copy(canonicalSchemaHash);
        this.compatibleReaderSetEvidenceHash = Bytes.copy(compatibleReaderSetEvidenceHash);
        this.manifestDigest = Bytes.copy(manifestDigest);
    }

    /** Convenience overload with the generation set next to the protocol tuple. */
    public ProtocolVersionActivatePayload(
            final ProtocolTuple tuple,
            final ArtifactGenerationSet artifactGenerationSet,
            final byte[] canonicalSchemaHash,
            final byte[] compatibleReaderSetEvidenceHash,
            final byte[] manifestDigest) {
        this(tuple, canonicalSchemaHash, compatibleReaderSetEvidenceHash, artifactGenerationSet, manifestDigest);
    }

    public ProtocolTuple tuple() {
        return tuple;
    }

    public byte[] canonicalSchemaHash() {
        return Bytes.copy(canonicalSchemaHash);
    }

    public byte[] compatibleReaderSetEvidenceHash() {
        return Bytes.copy(compatibleReaderSetEvidenceHash);
    }

    public boolean isCurrentGeneration() {
        return artifactGenerationSet != null;
    }

    public ArtifactGenerationSet artifactGenerationSet() {
        if (artifactGenerationSet == null) {
            throw new IllegalStateException("legacy activation payload has no ArtifactGenerationSet");
        }
        return artifactGenerationSet;
    }

    public ArtifactGenerationSet artifacts() {
        return artifactGenerationSet();
    }

    public byte[] manifestDigest() {
        if (manifestDigest.length == 0) {
            throw new IllegalStateException("legacy activation payload has no DataResetManifest digest");
        }
        return Bytes.copy(manifestDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
            CanonicalProtobuf.bytes(output, 2, canonicalSchemaHash);
            CanonicalProtobuf.bytes(output, 3, compatibleReaderSetEvidenceHash);
            if (artifactGenerationSet != null) {
                CanonicalProtobuf.bytes(output, 4, artifactGenerationSet.canonicalBytes());
                CanonicalProtobuf.bytes(output, 5, manifestDigest);
            }
        });
    }

    public static ProtocolVersionActivatePayload decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "ProtocolVersionActivatePayload");
        if (fields.size() != 3 && fields.size() != 5) {
            throw new IllegalArgumentException("ProtocolVersionActivatePayload has an unexpected field count");
        }
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("ProtocolVersionActivatePayload has an unexpected field order");
            }
        }
        final ProtocolTuple tuple = ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(0), 1));
        final ProtocolVersionActivatePayload result;
        if (fields.size() == 3) {
            result = new ProtocolVersionActivatePayload(
                    tuple,
                    QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                    QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH));
        } else {
            result = new ProtocolVersionActivatePayload(
                    tuple,
                    QueryCodecSupport.fixed(fields.get(1), 2, HASH_LENGTH),
                    QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                    ArtifactGenerationSet.decode(QueryCodecSupport.nested(fields.get(3), 4)),
                    QueryCodecSupport.fixed(fields.get(4), 5, HASH_LENGTH));
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolVersionActivatePayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolVersionActivatePayload that
                && tuple.equals(that.tuple)
                && Arrays.equals(canonicalSchemaHash, that.canonicalSchemaHash)
                && Arrays.equals(compatibleReaderSetEvidenceHash, that.compatibleReaderSetEvidenceHash)
                && Objects.equals(artifactGenerationSet, that.artifactGenerationSet)
                && Arrays.equals(manifestDigest, that.manifestDigest);
    }

    @Override
    public int hashCode() {
        int result = tuple.hashCode();
        result = 31 * result + Arrays.hashCode(canonicalSchemaHash);
        result = 31 * result + Arrays.hashCode(compatibleReaderSetEvidenceHash);
        result = 31 * result + Objects.hashCode(artifactGenerationSet);
        result = 31 * result + Arrays.hashCode(manifestDigest);
        return result;
    }

    private static void requireTupleBinding(
            final ProtocolTuple tuple, final ArtifactGenerationSet artifactGenerationSet) {
        final ProtocolTuple expected = tuple.recordKind() == ProtocolTuple.CLIENT_COMMAND
                ? artifactGenerationSet.clientCommandTuple()
                : artifactGenerationSet.systemMutationTuple();
        if (!tuple.equals(expected)) {
            throw new IllegalArgumentException("activation tuple does not match ArtifactGenerationSet");
        }
    }

    private static boolean allZero(final byte[] value) {
        for (byte element : value) {
            if (element != 0) {
                return false;
            }
        }
        return true;
    }
}
