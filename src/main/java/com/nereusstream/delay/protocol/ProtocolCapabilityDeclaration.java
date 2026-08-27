package com.nereusstream.delay.protocol;

import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Canonical capability declaration published by one Worker session.
 *
 * <p>The declaration is deliberately separate from the local protocol
 * activation projection. A Route/activation authority may use it to prove
 * that the current eligible reader set supports one exact protocol tuple
 * before it writes a source-ordered activation marker.</p>
 */
public final class ProtocolCapabilityDeclaration {
    public static final int VERSION = 2;
    public static final int LEGACY_VERSION = 1;
    public static final int DIGEST_LENGTH = 32;
    private static final int MAX_TUPLES = 32;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-protocol-capability-declaration\0");

    private final String workerId;
    private final byte[] workerIdentity;
    private final List<ProtocolTuple> supportedTuples;
    private final int version;
    private final List<ArtifactGenerationSet> artifactGenerationSets;
    private final long capabilityEpoch;
    private final byte[] sessionIdentity;
    private final byte[] declarationDigest;

    public ProtocolCapabilityDeclaration(
            final String workerId,
            final byte[] workerIdentity,
            final List<ProtocolTuple> supportedTuples,
            final long capabilityEpoch,
            final byte[] sessionIdentity) {
        this(
                LEGACY_VERSION,
                workerId,
                workerIdentity,
                supportedTuples,
                List.of(),
                capabilityEpoch,
                sessionIdentity,
                null);
    }

    /** Creates a current Worker declaration bound to the exact generation set. */
    public ProtocolCapabilityDeclaration(
            final String workerId,
            final byte[] workerIdentity,
            final List<ProtocolTuple> supportedTuples,
            final List<ArtifactGenerationSet> artifactGenerationSets,
            final long capabilityEpoch,
            final byte[] sessionIdentity) {
        this(
                VERSION,
                workerId,
                workerIdentity,
                supportedTuples,
                artifactGenerationSets,
                capabilityEpoch,
                sessionIdentity,
                null);
    }

    /** Convenience form for a Worker session supporting one exact set. */
    public ProtocolCapabilityDeclaration(
            final String workerId,
            final byte[] workerIdentity,
            final List<ProtocolTuple> supportedTuples,
            final ArtifactGenerationSet artifactGenerationSet,
            final long capabilityEpoch,
            final byte[] sessionIdentity) {
        this(
                workerId,
                workerIdentity,
                supportedTuples,
                List.of(Objects.requireNonNull(artifactGenerationSet, "artifactGenerationSet")),
                capabilityEpoch,
                sessionIdentity);
    }

    private ProtocolCapabilityDeclaration(
            final int version,
            final String workerId,
            final byte[] workerIdentity,
            final List<ProtocolTuple> supportedTuples,
            final List<ArtifactGenerationSet> artifactGenerationSets,
            final long capabilityEpoch,
            final byte[] sessionIdentity,
            final byte[] declarationDigest) {
        if (version != LEGACY_VERSION && version != VERSION) {
            throw new IllegalArgumentException("unsupported protocol capability declaration version");
        }
        this.version = version;
        this.workerId = canonicalText(workerId, "workerId");
        this.workerIdentity = fixed(workerIdentity, "workerIdentity");
        requireNonZero(this.workerIdentity, "workerIdentity");
        this.supportedTuples = sortedTuples(supportedTuples);
        this.artifactGenerationSets = sortedArtifactSets(artifactGenerationSets, version == VERSION);
        if (capabilityEpoch <= 0) {
            throw new IllegalArgumentException("capabilityEpoch must be positive");
        }
        this.capabilityEpoch = capabilityEpoch;
        this.sessionIdentity = fixed(sessionIdentity, "sessionIdentity");
        requireNonZero(this.sessionIdentity, "sessionIdentity");
        final byte[] expected = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToSixAndArtifacts());
        if (declarationDigest != null && !Bytes.constantTimeEquals(declarationDigest, expected)) {
            throw new IllegalArgumentException("protocol capability declaration digest mismatch");
        }
        this.declarationDigest = expected;
    }

    public String workerId() {
        return workerId;
    }

    public int version() {
        return version;
    }

    public byte[] workerIdentity() {
        return Bytes.copy(workerIdentity);
    }

    public List<ProtocolTuple> supportedTuples() {
        return supportedTuples;
    }

    public List<ArtifactGenerationSet> artifactGenerationSets() {
        return artifactGenerationSets;
    }

    public boolean isCurrentGeneration() {
        return version == VERSION && !artifactGenerationSets.isEmpty();
    }

    public long capabilityEpoch() {
        return capabilityEpoch;
    }

    public byte[] sessionIdentity() {
        return Bytes.copy(sessionIdentity);
    }

    public byte[] declarationDigest() {
        return Bytes.copy(declarationDigest);
    }

    public boolean supports(final ProtocolTuple tuple) {
        Objects.requireNonNull(tuple, "tuple");
        return supportedTuples.contains(tuple);
    }

    public boolean supports(final ArtifactGenerationSet artifacts) {
        Objects.requireNonNull(artifacts, "artifacts");
        return artifactGenerationSets.stream().anyMatch(candidate -> candidate.equals(artifacts));
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToSix());
            if (version == VERSION) {
                for (ArtifactGenerationSet artifactGenerationSet : artifactGenerationSets) {
                    CanonicalProtobuf.bytes(output, 7, artifactGenerationSet.canonicalBytes());
                }
                CanonicalProtobuf.bytes(output, 8, declarationDigest);
            } else {
                CanonicalProtobuf.bytes(output, 7, declarationDigest);
            }
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("protocol capability declaration is too large");
        }
        return encoded;
    }

    public static ProtocolCapabilityDeclaration decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length == 0 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid protocol capability declaration length");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 7
                || fields.get(0).number() != 1
                || fields.get(1).number() != 2
                || fields.get(2).number() != 3) {
            throw new IllegalArgumentException("protocol capability declaration is incomplete");
        }
        final int version = (int) QueryCodecSupport.uint(fields.get(0), 1);
        if (version != LEGACY_VERSION && version != VERSION) {
            throw new IllegalArgumentException("unsupported protocol capability declaration version");
        }
        final String workerId = text(QueryCodecSupport.bytes(fields.get(1), 2), "workerId");
        final byte[] workerIdentity = QueryCodecSupport.fixed(fields.get(2), 3, DIGEST_LENGTH);
        final List<ProtocolTuple> tuples = new ArrayList<>();
        int index = 3;
        while (index < fields.size() - 3 && fields.get(index).number() == 4) {
            tuples.add(ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(index++), 4)));
        }
        if (index + 2 > fields.size()
                || fields.get(index).number() != 5
                || fields.get(index + 1).number() != 6) {
            throw new IllegalArgumentException("protocol capability declaration fields are out of order");
        }
        final long epoch = QueryCodecSupport.uint(fields.get(index), 5);
        final byte[] sessionIdentity = QueryCodecSupport.fixed(fields.get(index + 1), 6, DIGEST_LENGTH);
        index += 2;
        final List<ArtifactGenerationSet> artifactGenerationSets = new ArrayList<>();
        final byte[] digest;
        if (version == VERSION) {
            while (index < fields.size() - 1 && fields.get(index).number() == 7) {
                artifactGenerationSets.add(
                        ArtifactGenerationSet.decode(QueryCodecSupport.nested(fields.get(index++), 7)));
            }
            if (artifactGenerationSets.isEmpty()
                    || index + 1 != fields.size()
                    || fields.get(index).number() != 8) {
                throw new IllegalArgumentException("current protocol capability declaration is incomplete");
            }
            digest = QueryCodecSupport.fixed(fields.get(index), 8, DIGEST_LENGTH);
        } else {
            if (index + 1 != fields.size() || fields.get(index).number() != 7) {
                throw new IllegalArgumentException("legacy protocol capability declaration is incomplete");
            }
            digest = QueryCodecSupport.fixed(fields.get(index), 7, DIGEST_LENGTH);
        }
        final ProtocolCapabilityDeclaration result = new ProtocolCapabilityDeclaration(
                version, workerId, workerIdentity, tuples, artifactGenerationSets, epoch, sessionIdentity, digest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolCapabilityDeclaration");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolCapabilityDeclaration that
                && version == that.version
                && workerId.equals(that.workerId)
                && Arrays.equals(workerIdentity, that.workerIdentity)
                && supportedTuples.equals(that.supportedTuples)
                && artifactGenerationSets.equals(that.artifactGenerationSets)
                && capabilityEpoch == that.capabilityEpoch
                && Arrays.equals(sessionIdentity, that.sessionIdentity)
                && Arrays.equals(declarationDigest, that.declarationDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                version,
                workerId,
                Arrays.hashCode(workerIdentity),
                supportedTuples,
                artifactGenerationSets,
                capabilityEpoch,
                Arrays.hashCode(sessionIdentity),
                Arrays.hashCode(declarationDigest));
    }

    private byte[] fieldsOneToSix() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, version);
            CanonicalProtobuf.bytes(output, 2, workerId.getBytes(StandardCharsets.UTF_8));
            CanonicalProtobuf.bytes(output, 3, workerIdentity);
            for (ProtocolTuple tuple : supportedTuples) {
                CanonicalProtobuf.bytes(output, 4, tuple.canonicalBytes());
            }
            CanonicalProtobuf.uint64Bits(output, 5, capabilityEpoch);
            CanonicalProtobuf.bytes(output, 6, sessionIdentity);
        });
    }

    private byte[] fieldsOneToSixAndArtifacts() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToSix());
            if (version == VERSION) {
                for (ArtifactGenerationSet artifactGenerationSet : artifactGenerationSets) {
                    CanonicalProtobuf.bytes(output, 7, artifactGenerationSet.canonicalBytes());
                }
            }
        });
    }

    private static List<ProtocolTuple> sortedTuples(final List<ProtocolTuple> values) {
        Objects.requireNonNull(values, "supportedTuples");
        if (values.isEmpty() || values.size() > MAX_TUPLES) {
            throw new IllegalArgumentException("supportedTuples must be bounded and non-empty");
        }
        final List<ProtocolTuple> result = new ArrayList<>(values);
        result.sort(Comparator.comparing(ProtocolTuple::canonicalBytes, ProtocolCapabilityDeclaration::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).equals(result.get(index))) {
                throw new IllegalArgumentException("duplicate supported protocol tuple");
            }
        }
        return List.copyOf(result);
    }

    private static List<ArtifactGenerationSet> sortedArtifactSets(
            final List<ArtifactGenerationSet> values, final boolean current) {
        Objects.requireNonNull(values, "artifactGenerationSets");
        if (!current && !values.isEmpty()) {
            throw new IllegalArgumentException("legacy declaration cannot carry an ArtifactGenerationSet");
        }
        if (current && (values.isEmpty() || values.size() > MAX_TUPLES)) {
            throw new IllegalArgumentException("current declaration requires bounded ArtifactGenerationSets");
        }
        final List<ArtifactGenerationSet> result = new ArrayList<>(values);
        result.sort(Comparator.comparing(
                ArtifactGenerationSet::canonicalBytes, ProtocolCapabilityDeclaration::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).equals(result.get(index))) {
                throw new IllegalArgumentException("duplicate ArtifactGenerationSet");
            }
        }
        return List.copyOf(result);
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, DIGEST_LENGTH, name);
        return Bytes.copy(value);
    }

    private static void requireNonZero(final byte[] value, final String name) {
        for (byte element : value) {
            if (element != 0) {
                return;
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static String text(final byte[] value, final String name) {
        final String result = new String(value, StandardCharsets.UTF_8);
        if (!Arrays.equals(result.getBytes(StandardCharsets.UTF_8), value)) {
            throw new IllegalArgumentException(name + " is not valid UTF-8");
        }
        return canonicalText(result, name);
    }

    private static String canonicalText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()
                || value.indexOf('\0') >= 0
                || !value.equals(Normalizer.normalize(value, Normalizer.Form.NFC))) {
            throw new IllegalArgumentException(name + " must be nonblank NFC UTF-8");
        }
        return value;
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Byte.toUnsignedInt(left[index]) - Byte.toUnsignedInt(right[index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }
}
