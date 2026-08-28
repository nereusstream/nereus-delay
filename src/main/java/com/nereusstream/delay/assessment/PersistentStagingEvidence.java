package com.nereusstream.delay.assessment;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.nereusstream.delay.protocol.Bytes;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Objects;

/**
 * Signed, self-contained JSON evidence used only by the local persistent
 * staging workflow. The payload is signed as bytes and the envelope carries
 * the verification key, so a receipt cannot become authority by reference to
 * mutable sidecar state.
 */
public final class PersistentStagingEvidence {
    public static final String SCHEMA = "nereus-delay.persistent-staging-evidence";
    public static final int SCHEMA_GENERATION = 1;
    private static final byte[] SIGNATURE_DOMAIN = Bytes.utf8("nereus-delay-persistent-staging-evidence\0");
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private PersistentStagingEvidence() {}

    public static Path writeSignedNew(
            final Path target,
            final byte[] payload,
            final PrivateKey signingKey,
            final PublicKey verificationKey,
            final int keyGeneration)
            throws IOException {
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(signingKey, "signingKey");
        Objects.requireNonNull(verificationKey, "verificationKey");
        if (keyGeneration <= 0) {
            throw new IllegalArgumentException("keyGeneration must be positive");
        }
        final byte[] digest = Bytes.sha256(payload);
        final byte[] signature = sign(payload, keyGeneration, signingKey);
        final JsonObject envelope = new JsonObject();
        envelope.addProperty("evidenceSchema", SCHEMA);
        envelope.addProperty("evidenceSchemaGeneration", SCHEMA_GENERATION);
        envelope.addProperty("keyGeneration", keyGeneration);
        envelope.addProperty("publicKeyDerBase64", Base64.getEncoder().encodeToString(verificationKey.getEncoded()));
        envelope.addProperty("payloadBase64", Base64.getEncoder().encodeToString(payload));
        envelope.addProperty("payloadSha256", Bytes.hex(digest));
        envelope.addProperty("signatureBase64", Base64.getEncoder().encodeToString(signature));
        return writeNew(target, (GSON.toJson(envelope) + "\n").getBytes(StandardCharsets.UTF_8));
    }

    public static Verified readVerified(final Path path) throws IOException {
        final Path normalized = regularFile(path, "evidence");
        final byte[] encoded = Files.readAllBytes(normalized);
        final JsonObject envelope;
        try {
            envelope = JsonParser.parseString(new String(encoded, StandardCharsets.UTF_8))
                    .getAsJsonObject();
        } catch (RuntimeException failure) {
            throw new IOException("invalid persistent staging evidence JSON: " + normalized, failure);
        }
        requireText(envelope, "evidenceSchema", SCHEMA);
        if (number(envelope, "evidenceSchemaGeneration") != SCHEMA_GENERATION) {
            throw new IOException("unsupported persistent staging evidence schema: " + normalized);
        }
        final int keyGeneration = number(envelope, "keyGeneration");
        if (keyGeneration <= 0) {
            throw new IOException("persistent staging evidence key generation must be positive");
        }
        final byte[] payload = decodeBase64(envelope, "payloadBase64");
        final String payloadDigest = text(envelope, "payloadSha256");
        if (!Bytes.hex(Bytes.sha256(payload)).equals(payloadDigest)) {
            throw new IOException("persistent staging evidence payload digest mismatch: " + normalized);
        }
        final PublicKey publicKey = decodePublicKey(decodeBase64(envelope, "publicKeyDerBase64"));
        final byte[] signature = decodeBase64(envelope, "signatureBase64");
        if (!verify(payload, keyGeneration, signature, publicKey)) {
            throw new IOException("persistent staging evidence signature mismatch: " + normalized);
        }
        final JsonObject payloadJson;
        try {
            payloadJson = JsonParser.parseString(new String(payload, StandardCharsets.UTF_8))
                    .getAsJsonObject();
        } catch (RuntimeException failure) {
            throw new IOException("persistent staging evidence payload is not a JSON object: " + normalized, failure);
        }
        return new Verified(normalized, payload, payloadJson, keyGeneration, publicKey, Bytes.sha256(encoded));
    }

    public static PrivateKey decodePrivateKey(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        try {
            return KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(encoded));
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("invalid Ed25519 private key", failure);
        }
    }

    public static PublicKey decodePublicKey(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        try {
            return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encoded));
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("invalid Ed25519 public key", failure);
        }
    }

    public static Path writeNew(final Path target, final byte[] bytes) throws IOException {
        final Path normalized =
                Objects.requireNonNull(target, "target").toAbsolutePath().normalize();
        final Path parent = normalized.getParent();
        if (parent == null || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("evidence parent must be an existing non-symlink directory: " + parent);
        }
        try (FileChannel channel = FileChannel.open(
                normalized, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            channel.write(java.nio.ByteBuffer.wrap(Objects.requireNonNull(bytes, "bytes")));
            channel.force(true);
        }
        return normalized;
    }

    private static byte[] sign(final byte[] payload, final int keyGeneration, final PrivateKey key) {
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(key);
            signer.update(signatureInput(payload, keyGeneration));
            return signer.sign();
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("cannot sign persistent staging evidence", failure);
        }
    }

    private static boolean verify(
            final byte[] payload, final int keyGeneration, final byte[] signature, final PublicKey key) {
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(key);
            verifier.update(signatureInput(payload, keyGeneration));
            return verifier.verify(signature);
        } catch (GeneralSecurityException failure) {
            throw new IllegalArgumentException("cannot verify persistent staging evidence", failure);
        }
    }

    private static byte[] signatureInput(final byte[] payload, final int keyGeneration) {
        return Bytes.concat(SIGNATURE_DOMAIN, Bytes.u32be(keyGeneration), Bytes.sha256(payload));
    }

    private static Path regularFile(final Path path, final String name) throws IOException {
        final Path normalized =
                Objects.requireNonNull(path, name).toAbsolutePath().normalize();
        if (Files.isSymbolicLink(normalized) || !Files.isRegularFile(normalized, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(name + " must be a regular non-symlink file: " + normalized);
        }
        return normalized;
    }

    private static String text(final JsonObject object, final String name) throws IOException {
        if (!object.has(name) || !object.get(name).isJsonPrimitive()) {
            throw new IOException("persistent staging evidence field is missing: " + name);
        }
        final String value = object.get(name).getAsString();
        if (value.isBlank()) {
            throw new IOException("persistent staging evidence field is blank: " + name);
        }
        return value;
    }

    private static void requireText(final JsonObject object, final String name, final String expected)
            throws IOException {
        if (!expected.equals(text(object, name))) {
            throw new IOException("persistent staging evidence " + name + " mismatch");
        }
    }

    private static int number(final JsonObject object, final String name) throws IOException {
        try {
            return Integer.parseInt(text(object, name));
        } catch (NumberFormatException failure) {
            throw new IOException("persistent staging evidence field is not an integer: " + name, failure);
        }
    }

    private static byte[] decodeBase64(final JsonObject object, final String name) throws IOException {
        try {
            return Base64.getDecoder().decode(text(object, name));
        } catch (IllegalArgumentException failure) {
            throw new IOException("persistent staging evidence field is not base64: " + name, failure);
        }
    }

    public record Verified(
            Path path,
            byte[] payload,
            JsonObject payloadJson,
            int keyGeneration,
            PublicKey publicKey,
            byte[] envelopeDigest) {
        public Verified {
            Objects.requireNonNull(path, "path");
            payload = Bytes.copy(Objects.requireNonNull(payload, "payload"));
            Objects.requireNonNull(payloadJson, "payloadJson");
            Objects.requireNonNull(publicKey, "publicKey");
            envelopeDigest = Bytes.copy(Objects.requireNonNull(envelopeDigest, "envelopeDigest"));
            Bytes.requireLength(envelopeDigest, 32, "envelopeDigest");
        }

        @Override
        public byte[] payload() {
            return Bytes.copy(payload);
        }

        @Override
        public byte[] envelopeDigest() {
            return Bytes.copy(envelopeDigest);
        }
    }
}
