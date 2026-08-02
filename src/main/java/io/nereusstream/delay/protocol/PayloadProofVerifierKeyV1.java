package io.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** One immutable Ed25519 verifier entry in a payload-proof trust set. */
public final class PayloadProofVerifierKeyV1 {
    public static final int PUBLIC_KEY_LENGTH = 32;
    private static final byte[] ED25519_X509_PREFIX = Bytes.hexToBytes("302a300506032b6570032100");

    private final int keyVersion;
    private final byte[] publicKey;
    private final long verifyNotBeforeEpochMs;
    private final long verifyNotAfterEpochMs;

    public PayloadProofVerifierKeyV1(final int keyVersion, final byte[] publicKey,
                                     final long verifyNotBeforeEpochMs, final long verifyNotAfterEpochMs) {
        if (keyVersion <= 0 || verifyNotBeforeEpochMs < 0
                || verifyNotAfterEpochMs <= verifyNotBeforeEpochMs) {
            throw new IllegalArgumentException("invalid payload proof verifier key bounds");
        }
        Bytes.requireLength(publicKey, PUBLIC_KEY_LENGTH, "publicKey");
        this.keyVersion = keyVersion;
        this.publicKey = Bytes.copy(publicKey);
        this.verifyNotBeforeEpochMs = verifyNotBeforeEpochMs;
        this.verifyNotAfterEpochMs = verifyNotAfterEpochMs;
    }

    public int keyVersion() {
        return keyVersion;
    }

    public byte[] publicKey() {
        return Bytes.copy(publicKey);
    }

    public long verifyNotBeforeEpochMs() {
        return verifyNotBeforeEpochMs;
    }

    public long verifyNotAfterEpochMs() {
        return verifyNotAfterEpochMs;
    }

    public PublicKey toPublicKey() {
        try {
            return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(
                    Bytes.concat(ED25519_X509_PREFIX, publicKey)));
        } catch (GeneralSecurityException exception) {
            throw new IllegalArgumentException("invalid Ed25519 public key", exception);
        }
    }

    public static PayloadProofVerifierKeyV1 fromPublicKey(final int keyVersion, final PublicKey publicKey,
                                                          final long verifyNotBeforeEpochMs,
                                                          final long verifyNotAfterEpochMs) {
        Objects.requireNonNull(publicKey, "publicKey");
        final byte[] encoded = publicKey.getEncoded();
        if (encoded == null || encoded.length != ED25519_X509_PREFIX.length + PUBLIC_KEY_LENGTH
                || !Arrays.equals(ED25519_X509_PREFIX,
                Arrays.copyOf(encoded, ED25519_X509_PREFIX.length))) {
            throw new IllegalArgumentException("public key is not the canonical Ed25519 encoding");
        }
        return new PayloadProofVerifierKeyV1(keyVersion,
                Arrays.copyOfRange(encoded, ED25519_X509_PREFIX.length, encoded.length), verifyNotBeforeEpochMs,
                verifyNotAfterEpochMs);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, keyVersion);
            CanonicalProtobuf.bytes(output, 2, publicKey);
            CanonicalProtobuf.int64(output, 3, verifyNotBeforeEpochMs);
            CanonicalProtobuf.int64(output, 4, verifyNotAfterEpochMs);
        });
    }

    public static PayloadProofVerifierKeyV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PayloadProofVerifierKeyV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "PayloadProofVerifierKeyV1");
        final PayloadProofVerifierKeyV1 result = new PayloadProofVerifierKeyV1(
                QueryCodecSupport.uint32(fields.get(0), 1), QueryCodecSupport.fixed(fields.get(1), 2,
                        PUBLIC_KEY_LENGTH), QueryCodecSupport.uint(fields.get(2), 3),
                QueryCodecSupport.uint(fields.get(3), 4));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadProofVerifierKeyV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadProofVerifierKeyV1 that && keyVersion == that.keyVersion
                && verifyNotBeforeEpochMs == that.verifyNotBeforeEpochMs
                && verifyNotAfterEpochMs == that.verifyNotAfterEpochMs
                && Arrays.equals(publicKey, that.publicKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyVersion, Arrays.hashCode(publicKey), verifyNotBeforeEpochMs,
                verifyNotAfterEpochMs);
    }
}
