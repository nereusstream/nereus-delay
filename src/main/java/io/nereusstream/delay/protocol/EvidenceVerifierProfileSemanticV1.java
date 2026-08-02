package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Immutable Evidence Verifier Profile semantic body from Registry §5.1.1. */
public final class EvidenceVerifierProfileSemanticV1 implements ProfileSemanticBodyV1 {
    public static final int SCHEMA_VERSION = 1;
    public static final int ED25519_VERIFIER_KIND = 1;
    public static final int HASH_LENGTH = 32;

    private final int verifierKind;
    private final int keyVersion;
    private final byte[] publicKey;
    private final byte[] authenticatedScopeHash;
    private final long notBeforeEpochMs;
    private final long notAfterEpochMs;
    private final byte[] verifierPolicyDigest;

    public EvidenceVerifierProfileSemanticV1(final int verifierKind, final int keyVersion,
                                             final byte[] publicKey, final byte[] authenticatedScopeHash,
                                             final long notBeforeEpochMs, final long notAfterEpochMs,
                                             final byte[] verifierPolicyDigest) {
        if (verifierKind != ED25519_VERIFIER_KIND || keyVersion <= 0) {
            throw new IllegalArgumentException("unsupported Evidence Verifier kind/version");
        }
        this.verifierKind = verifierKind;
        this.keyVersion = keyVersion;
        this.publicKey = fixed(publicKey, "publicKey");
        this.authenticatedScopeHash = fixed(authenticatedScopeHash, "authenticatedScopeHash");
        if (notBeforeEpochMs < 0 || notAfterEpochMs <= notBeforeEpochMs) {
            throw new IllegalArgumentException("invalid Evidence Verifier validity range");
        }
        this.notBeforeEpochMs = notBeforeEpochMs;
        this.notAfterEpochMs = notAfterEpochMs;
        this.verifierPolicyDigest = fixed(verifierPolicyDigest, "verifierPolicyDigest");
    }

    @Override
    public ProfileKindV1 profileKind() {
        return ProfileKindV1.EVIDENCE_VERIFIER;
    }

    @Override
    public int schemaVersion() {
        return SCHEMA_VERSION;
    }

    public int verifierKind() {
        return verifierKind;
    }

    public int keyVersion() {
        return keyVersion;
    }

    public byte[] publicKey() {
        return Bytes.copy(publicKey);
    }

    public byte[] authenticatedScopeHash() {
        return Bytes.copy(authenticatedScopeHash);
    }

    public long notBeforeEpochMs() {
        return notBeforeEpochMs;
    }

    public long notAfterEpochMs() {
        return notAfterEpochMs;
    }

    public byte[] verifierPolicyDigest() {
        return Bytes.copy(verifierPolicyDigest);
    }

    @Override
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, verifierKind);
            CanonicalProtobuf.uint32(output, 2, keyVersion);
            CanonicalProtobuf.bytes(output, 3, publicKey);
            CanonicalProtobuf.bytes(output, 4, authenticatedScopeHash);
            CanonicalProtobuf.int64(output, 5, notBeforeEpochMs);
            CanonicalProtobuf.int64(output, 6, notAfterEpochMs);
            CanonicalProtobuf.bytes(output, 7, verifierPolicyDigest);
        });
    }

    public static EvidenceVerifierProfileSemanticV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "EvidenceVerifierProfileSemanticV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4, 5, 6, 7},
                "EvidenceVerifierProfileSemanticV1");
        final EvidenceVerifierProfileSemanticV1 result = new EvidenceVerifierProfileSemanticV1(
                QueryCodecSupport.uint32(fields.get(0), 1), QueryCodecSupport.uint32(fields.get(1), 2),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH), QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.uint(fields.get(5), 6), QueryCodecSupport.fixed(fields.get(6), 7, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "EvidenceVerifierProfileSemanticV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof EvidenceVerifierProfileSemanticV1 that && verifierKind == that.verifierKind
                && keyVersion == that.keyVersion && Arrays.equals(publicKey, that.publicKey)
                && Arrays.equals(authenticatedScopeHash, that.authenticatedScopeHash)
                && notBeforeEpochMs == that.notBeforeEpochMs && notAfterEpochMs == that.notAfterEpochMs
                && Arrays.equals(verifierPolicyDigest, that.verifierPolicyDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(verifierKind, keyVersion, Arrays.hashCode(publicKey),
                Arrays.hashCode(authenticatedScopeHash), notBeforeEpochMs, notAfterEpochMs,
                Arrays.hashCode(verifierPolicyDigest));
    }

    private static byte[] fixed(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        if (allZero(value)) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return Bytes.copy(value);
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
