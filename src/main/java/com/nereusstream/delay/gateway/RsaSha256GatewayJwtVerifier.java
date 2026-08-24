package com.nereusstream.delay.gateway;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.nereusstream.delay.protocol.Bytes;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * Strict RS256 JWT verifier for the Gateway mTLS authority boundary.
 *
 * <p>The token is an authentication input only.  The verifier emits fixed
 * digest projections and never exposes a tenant string to the Delay request
 * or audit layers.  The issuer must sign the control-plane routing scope and
 * authenticated tenant scope; deriving either value from a display name is
 * deliberately forbidden.</p>
 */
public final class RsaSha256GatewayJwtVerifier implements GatewayJwtVerifier {
    private static final String ALGORITHM = "RS256";
    private static final String TOKEN_TYPE = "JWT";
    private static final int HASH_LENGTH = 32;
    private static final int MAX_TOKEN_CHARS = 16 * 1024;
    private static final int MAX_JSON_DEPTH = 16;
    private static final byte[] PRINCIPAL_DOMAIN = Bytes.utf8("nereus-delay-gateway-principal-scope-v1\0");

    private final PublicKey verificationKey;
    private final String expectedIssuer;
    private final String expectedAudience;
    private final String expectedKeyId;
    private final Clock clock;
    private final long clockSkewSeconds;
    private final long maxLifetimeSeconds;

    /**
     * Creates a verifier with an optional pinned JWT key id.
     *
     * @param verificationKey RSA public key used for RS256
     * @param expectedIssuer exact {@code iss} claim
     * @param expectedAudience exact {@code aud} value
     * @param expectedKeyId optional exact {@code kid} claim, or {@code null}
     * @param clock trusted clock used for NumericDate validation
     * @param clockSkewSeconds non-negative bounded clock skew allowance
     * @param maxLifetimeSeconds maximum {@code exp - iat} lifetime
     */
    public RsaSha256GatewayJwtVerifier(
            final PublicKey verificationKey,
            final String expectedIssuer,
            final String expectedAudience,
            final String expectedKeyId,
            final Clock clock,
            final long clockSkewSeconds,
            final long maxLifetimeSeconds) {
        this.verificationKey = requireRsaKey(verificationKey);
        this.expectedIssuer = requiredBounded(expectedIssuer, "expectedIssuer");
        this.expectedAudience = requiredBounded(expectedAudience, "expectedAudience");
        this.expectedKeyId = expectedKeyId == null ? null : requiredBounded(expectedKeyId, "expectedKeyId");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (clockSkewSeconds < 0 || clockSkewSeconds > 300) {
            throw new IllegalArgumentException("clockSkewSeconds must be 0..300");
        }
        if (maxLifetimeSeconds <= 0) {
            throw new IllegalArgumentException("maxLifetimeSeconds must be positive");
        }
        this.clockSkewSeconds = clockSkewSeconds;
        this.maxLifetimeSeconds = maxLifetimeSeconds;
    }

    public RsaSha256GatewayJwtVerifier(
            final PublicKey verificationKey,
            final String expectedIssuer,
            final String expectedAudience,
            final Clock clock,
            final long clockSkewSeconds,
            final long maxLifetimeSeconds) {
        this(verificationKey, expectedIssuer, expectedAudience, null, clock, clockSkewSeconds, maxLifetimeSeconds);
    }

    @Override
    public GatewayJwtIdentity verify(final String bearerToken, final SSLSession peerSession) {
        final TokenParts token = TokenParts.parse(bearerToken);
        final JsonObject header = parseObject(token.headerBytes(), "JWT header");
        final JsonObject claims = parseObject(token.payloadBytes(), "JWT claims");
        requireString(header, "alg", ALGORITHM);
        requireString(header, "typ", TOKEN_TYPE);
        if (expectedKeyId != null) {
            requireString(header, "kid", expectedKeyId);
        } else if (header.has("kid")) {
            requiredString(header, "kid");
        }
        verifySignature(token);

        final String issuer = requiredString(claims, "iss");
        if (!expectedIssuer.equals(issuer)) {
            throw invalid("JWT issuer does not match Gateway policy");
        }
        requireAudience(claims.get("aud"), expectedAudience);
        final String subject = requiredString(claims, "sub");
        final String tenant = requiredString(claims, "tenant");
        requiredString(claims, "jti");
        final long issuedAt = numericDate(claims, "iat");
        final long expiresAt = numericDate(claims, "exp");
        final long notBefore = claims.has("nbf") ? numericDate(claims, "nbf") : issuedAt;
        validateTimes(issuedAt, notBefore, expiresAt);

        final byte[] tenantScopeHash = fixedBytes(claims, "tenant_scope_hash");
        final byte[] tenantRoutingScope = fixedBytes(claims, "tenant_routing_scope");
        final byte[] certificateFingerprint = certificateFingerprint(peerSession);
        final JsonObject confirmation = objectClaim(claims, "cnf");
        final String certificateBinding = requiredString(confirmation, "x5t#S256");
        if (!constantTimeBase64Equals(certificateBinding, certificateFingerprint)) {
            throw invalid("JWT certificate confirmation does not match the mTLS peer");
        }
        final byte[] principalScopeHash = Bytes.sha256(
                PRINCIPAL_DOMAIN,
                Bytes.lp32(Bytes.utf8(issuer)),
                Bytes.lp32(Bytes.utf8(tenant)),
                Bytes.lp32(Bytes.utf8(subject)),
                certificateFingerprint);
        return new GatewayJwtIdentity(tenantScopeHash, tenantRoutingScope, principalScopeHash);
    }

    private void verifySignature(final TokenParts token) {
        try {
            final Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initVerify(verificationKey);
            signature.update(token.signingInput());
            if (!signature.verify(token.signature())) {
                throw invalid("JWT signature is invalid");
            }
        } catch (GeneralSecurityException failure) {
            throw invalid("JWT signature verification failed", failure);
        }
    }

    private void validateTimes(final long issuedAt, final long notBefore, final long expiresAt) {
        if (issuedAt < 0
                || notBefore < 0
                || expiresAt < 0
                || expiresAt <= issuedAt
                || notBefore > expiresAt
                || expiresAt - issuedAt > maxLifetimeSeconds) {
            throw invalid("JWT NumericDate claims are outside the configured policy");
        }
        final long now = Instant.now(clock).getEpochSecond();
        if (now < 0) {
            throw invalid("Gateway clock returned a negative epoch");
        }
        final long lowerBound = now - clockSkewSeconds;
        final long upperBound;
        try {
            upperBound = Math.addExact(now, clockSkewSeconds);
        } catch (ArithmeticException overflow) {
            throw invalid("Gateway clock range overflow", overflow);
        }
        if (expiresAt <= lowerBound || notBefore > upperBound || issuedAt > upperBound) {
            throw invalid("JWT is expired or not yet valid");
        }
    }

    private static PublicKey requireRsaKey(final PublicKey key) {
        Objects.requireNonNull(key, "verificationKey");
        if (!"RSA".equalsIgnoreCase(key.getAlgorithm())) {
            throw new IllegalArgumentException("Gateway JWT verification key must be RSA");
        }
        return key;
    }

    private static String requiredBounded(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isEmpty() || value.length() > 256 || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " is empty or too long");
        }
        return value;
    }

    private static String requiredString(final JsonObject object, final String name) {
        final JsonElement value = object.get(name);
        if (value == null
                || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isString()) {
            throw invalid("JWT claim is not a string: " + name);
        }
        return requiredBounded(value.getAsString(), name);
    }

    private static void requireString(final JsonObject object, final String name, final String expected) {
        if (!expected.equals(requiredString(object, name))) {
            throw invalid("JWT claim does not match: " + name);
        }
    }

    private static long numericDate(final JsonObject claims, final String name) {
        final JsonElement value = claims.get(name);
        if (value == null
                || !value.isJsonPrimitive()
                || !value.getAsJsonPrimitive().isNumber()) {
            throw invalid("JWT NumericDate is missing or not numeric: " + name);
        }
        try {
            return value.getAsJsonPrimitive().getAsBigDecimal().longValueExact();
        } catch (ArithmeticException | NumberFormatException failure) {
            throw invalid("JWT NumericDate is not an exact integer: " + name, failure);
        }
    }

    private static void requireAudience(final JsonElement value, final String expected) {
        if (value == null) {
            throw invalid("JWT audience is missing");
        }
        if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
            if (!expected.equals(value.getAsString())) {
                throw invalid("JWT audience does not match Gateway policy");
            }
            return;
        }
        if (!value.isJsonArray()) {
            throw invalid("JWT audience is not a string or string array");
        }
        boolean matched = false;
        for (JsonElement member : value.getAsJsonArray()) {
            if (member.isJsonPrimitive() && member.getAsJsonPrimitive().isString()) {
                matched |= expected.equals(member.getAsString());
            } else {
                throw invalid("JWT audience array contains a non-string");
            }
        }
        if (!matched) {
            throw invalid("JWT audience does not match Gateway policy");
        }
    }

    private static JsonObject objectClaim(final JsonObject object, final String name) {
        final JsonElement value = object.get(name);
        if (value == null || !value.isJsonObject()) {
            throw invalid("JWT claim is not an object: " + name);
        }
        return value.getAsJsonObject();
    }

    private static byte[] fixedBytes(final JsonObject claims, final String name) {
        final String encoded = requiredString(claims, name);
        final byte[] decoded;
        try {
            decoded = Base64.getUrlDecoder().decode(encoded);
        } catch (IllegalArgumentException failure) {
            throw invalid("JWT fixed-byte claim is not base64url: " + name, failure);
        }
        if (!constantTimeBase64Equals(encoded, decoded)) {
            throw invalid("JWT fixed-byte claim is not canonical base64url: " + name);
        }
        Bytes.requireLength(decoded, HASH_LENGTH, name);
        if (MessageDigest.isEqual(decoded, new byte[HASH_LENGTH])) {
            throw invalid("JWT fixed-byte claim must be non-zero: " + name);
        }
        return Bytes.copy(decoded);
    }

    private static byte[] certificateFingerprint(final SSLSession peerSession) {
        Objects.requireNonNull(peerSession, "peerSession");
        try {
            final Certificate[] certificates = peerSession.getPeerCertificates();
            if (certificates == null || certificates.length == 0 || certificates[0] == null) {
                throw invalid("Gateway mTLS peer has no certificate");
            }
            return Bytes.sha256(certificates[0].getEncoded());
        } catch (SSLPeerUnverifiedException | GeneralSecurityException failure) {
            throw invalid("Gateway mTLS peer certificate is not verifiable", failure);
        }
    }

    private static boolean constantTimeBase64Equals(final String encoded, final byte[] expected) {
        try {
            final byte[] decoded = Base64.getUrlDecoder().decode(encoded);
            return Base64.getUrlEncoder()
                            .withoutPadding()
                            .encodeToString(decoded)
                            .equals(encoded)
                    && MessageDigest.isEqual(decoded, expected);
        } catch (IllegalArgumentException failure) {
            return false;
        }
    }

    private static JsonObject parseObject(final byte[] bytes, final String label) {
        final String json;
        try {
            json = StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException failure) {
            throw invalid(label + " is not valid UTF-8", failure);
        }
        try (JsonReader reader = new JsonReader(new StringReader(json))) {
            // Gson's JsonReader is strict by default; do not call its deprecated
            // setLenient(false) compatibility method under -Xlint:all.
            final JsonElement value = readJson(reader, 0);
            if (reader.peek() != JsonToken.END_DOCUMENT || !value.isJsonObject()) {
                throw invalid(label + " must be one JSON object");
            }
            return value.getAsJsonObject();
        } catch (IOException | IllegalStateException failure) {
            throw invalid(label + " is not strict JSON", failure);
        }
    }

    private static JsonElement readJson(final JsonReader reader, final int depth) throws IOException {
        if (depth > MAX_JSON_DEPTH) {
            throw invalid("JWT JSON nesting is too deep");
        }
        return switch (reader.peek()) {
            case BEGIN_OBJECT -> readObject(reader, depth + 1);
            case BEGIN_ARRAY -> readArray(reader, depth + 1);
            case STRING -> new JsonPrimitive(reader.nextString());
            case NUMBER -> new JsonPrimitive(new java.math.BigDecimal(reader.nextString()));
            case BOOLEAN -> new JsonPrimitive(reader.nextBoolean());
            case NULL -> {
                reader.nextNull();
                yield JsonNull.INSTANCE;
            }
            default -> throw invalid("JWT JSON contains an invalid token");
        };
    }

    private static JsonObject readObject(final JsonReader reader, final int depth) throws IOException {
        final JsonObject object = new JsonObject();
        final Set<String> names = new HashSet<>();
        reader.beginObject();
        while (reader.hasNext()) {
            final String name = reader.nextName();
            if (!names.add(name)) {
                throw invalid("JWT JSON contains a duplicate object member: " + name);
            }
            object.add(name, readJson(reader, depth));
        }
        reader.endObject();
        return object;
    }

    private static JsonArray readArray(final JsonReader reader, final int depth) throws IOException {
        final JsonArray array = new JsonArray();
        reader.beginArray();
        while (reader.hasNext()) {
            array.add(readJson(reader, depth));
        }
        reader.endArray();
        return array;
    }

    private static IllegalArgumentException invalid(final String message) {
        return new IllegalArgumentException(message);
    }

    private static IllegalArgumentException invalid(final String message, final Throwable cause) {
        return new IllegalArgumentException(message, cause);
    }

    private record TokenParts(
            String headerSegment, String payloadSegment, byte[] headerBytes, byte[] payloadBytes, byte[] signature) {
        private static TokenParts parse(final String token) {
            if (token == null || token.isEmpty() || token.length() > MAX_TOKEN_CHARS) {
                throw invalid("JWT compact token is empty or too long");
            }
            final String[] segments = token.split("\\.", -1);
            if (segments.length != 3 || segments[0].isEmpty() || segments[1].isEmpty() || segments[2].isEmpty()) {
                throw invalid("JWT compact token must contain three non-empty segments");
            }
            final byte[] header = decodeCanonical(segments[0], "JWT header");
            final byte[] payload = decodeCanonical(segments[1], "JWT payload");
            final byte[] signature = decodeCanonical(segments[2], "JWT signature");
            if (signature.length == 0) {
                throw invalid("JWT signature is empty");
            }
            return new TokenParts(segments[0], segments[1], header, payload, signature);
        }

        private static byte[] decodeCanonical(final String segment, final String label) {
            final byte[] decoded;
            try {
                decoded = Base64.getUrlDecoder().decode(segment);
            } catch (IllegalArgumentException failure) {
                throw invalid(label + " is not base64url", failure);
            }
            if (!Base64.getUrlEncoder().withoutPadding().encodeToString(decoded).equals(segment)) {
                throw invalid(label + " is not canonical base64url");
            }
            return decoded;
        }

        private byte[] signingInput() {
            return (headerSegment + "." + payloadSegment).getBytes(StandardCharsets.US_ASCII);
        }
    }
}
