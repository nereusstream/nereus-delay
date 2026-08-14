package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RsaSha256GatewayJwtVerifierTest {
    private static final long NOW = 1_700_000_000L;
    private static final byte[] CERTIFICATE_BYTES = Bytes.utf8("gateway-client-certificate-v1");

    @Test
    void verifiesSignatureClaimsAudienceArrayAndMtlsConfirmation() throws Exception {
        final KeyPair keyPair = rsaKeyPair();
        final RsaSha256GatewayJwtVerifier verifier = verifier(keyPair);
        final byte[] tenantScope = digest(11);
        final byte[] routingScope = digest(12);
        final String token = token(keyPair, header(), claims(tenantScope, routingScope, certificateFingerprint(),
                NOW - 10, NOW - 5, NOW + 300));

        final GatewayJwtIdentity identity = verifier.verify(token, session(CERTIFICATE_BYTES));

        assertArrayEquals(tenantScope, identity.authenticatedTenantScopeHash());
        assertArrayEquals(routingScope, identity.tenantRoutingScope());
        assertDoesNotThrow(() -> Bytes.requireLength(identity.principalScopeHash(), 32, "principalScopeHash"));
    }

    @Test
    void rejectsSignatureMutationExpiryAndCertificateMismatch() throws Exception {
        final KeyPair keyPair = rsaKeyPair();
        final RsaSha256GatewayJwtVerifier verifier = verifier(keyPair);
        final String valid = token(keyPair, header(), claims(digest(21), digest(22), certificateFingerprint(),
                NOW - 10, NOW - 5, NOW + 300));

        assertThrows(IllegalArgumentException.class,
                () -> verifier.verify(mutateSignature(valid), session(CERTIFICATE_BYTES)));
        final String expired = token(keyPair, header(), claims(digest(21), digest(22), certificateFingerprint(),
                NOW - 500, NOW - 400, NOW - 100));
        assertThrows(IllegalArgumentException.class, () -> verifier.verify(expired, session(CERTIFICATE_BYTES)));
        assertThrows(IllegalArgumentException.class,
                () -> verifier.verify(valid, session(Bytes.utf8("different-certificate"))));
    }

    @Test
    void rejectsWrongPolicyDuplicateMembersAndNonCanonicalFixedBytes() throws Exception {
        final KeyPair keyPair = rsaKeyPair();
        final RsaSha256GatewayJwtVerifier verifier = verifier(keyPair);
        final String wrongIssuer = claims(digest(31), digest(32), certificateFingerprint(), NOW - 10, NOW - 5,
                NOW + 300).replace("issuer-v1", "other-issuer");
        assertThrows(IllegalArgumentException.class,
                () -> verifier.verify(token(keyPair, header(), wrongIssuer), session(CERTIFICATE_BYTES)));

        final String duplicateHeader = token(keyPair, "{\"alg\":\"RS256\",\"alg\":\"none\",\"typ\":\"JWT\"}",
                claims(digest(41), digest(42), certificateFingerprint(), NOW - 10, NOW - 5, NOW + 300));
        assertThrows(IllegalArgumentException.class,
                () -> verifier.verify(duplicateHeader, session(CERTIFICATE_BYTES)));

        final String nonCanonical = claims(digest(51), digest(52), certificateFingerprint(), NOW - 10, NOW - 5,
                NOW + 300).replace("tenant_routing_scope\":\"", "tenant_routing_scope\":\"==");
        assertThrows(IllegalArgumentException.class,
                () -> verifier.verify(token(keyPair, header(), nonCanonical), session(CERTIFICATE_BYTES)));
    }

    private static RsaSha256GatewayJwtVerifier verifier(final KeyPair keyPair) {
        return new RsaSha256GatewayJwtVerifier(keyPair.getPublic(), "issuer-v1", "delay-gateway-v1", "gateway-key-1",
                Clock.fixed(Instant.ofEpochSecond(NOW), ZoneOffset.UTC), 30, 600);
    }

    private static String header() {
        return "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"gateway-key-1\"}";
    }

    private static String claims(final byte[] tenantScope, final byte[] routingScope, final byte[] fingerprint,
                                 final long issuedAt, final long notBefore, final long expiresAt) {
        final String encodedFingerprint = encode(fingerprint);
        return "{"
                + "\"iss\":\"issuer-v1\","
                + "\"aud\":[\"other-audience\",\"delay-gateway-v1\"],"
                + "\"sub\":\"client-operator\","
                + "\"tenant\":\"tenant-a\","
                + "\"tenant_scope_hash\":\"" + encode(tenantScope) + "\","
                + "\"tenant_routing_scope\":\"" + encode(routingScope) + "\","
                + "\"iat\":" + issuedAt + ","
                + "\"nbf\":" + notBefore + ","
                + "\"exp\":" + expiresAt + ","
                + "\"jti\":\"jwt-1\","
                + "\"cnf\":{\"x5t#S256\":\"" + encodedFingerprint + "\"}"
                + "}";
    }

    private static String token(final KeyPair keyPair, final String header, final String claims) throws Exception {
        final String encodedHeader = encode(header.getBytes(StandardCharsets.UTF_8));
        final String encodedClaims = encode(claims.getBytes(StandardCharsets.UTF_8));
        final String input = encodedHeader + "." + encodedClaims;
        final Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate());
        signature.update(input.getBytes(StandardCharsets.US_ASCII));
        return input + "." + encode(signature.sign());
    }

    private static String mutateSignature(final String token) {
        final int signatureStart = token.lastIndexOf('.') + 1;
        final char original = token.charAt(signatureStart);
        final char replacement = original == 'A' ? 'B' : 'A';
        return token.substring(0, signatureStart) + replacement + token.substring(signatureStart + 1);
    }

    private static KeyPair rsaKeyPair() throws Exception {
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    private static byte[] certificateFingerprint() {
        return Bytes.sha256(CERTIFICATE_BYTES);
    }

    private static byte[] digest(final int seed) {
        final byte[] value = new byte[32];
        value[0] = (byte) seed;
        value[31] = (byte) (seed ^ 0x5a);
        return value;
    }

    private static String encode(final byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static SSLSession session(final byte[] certificateBytes) {
        final Certificate certificate = new TestCertificate(certificateBytes);
        return (SSLSession) Proxy.newProxyInstance(SSLSession.class.getClassLoader(),
                new Class<?>[]{SSLSession.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("getPeerCertificates")) {
                        return new Certificate[]{certificate};
                    }
                    if (method.getReturnType() == boolean.class) {
                        return false;
                    }
                    if (method.getReturnType() == byte[].class) {
                        return new byte[0];
                    }
                    if (method.getReturnType() == int.class) {
                        return 0;
                    }
                    if (method.getReturnType() == long.class) {
                        return 0L;
                    }
                    return null;
                });
    }

    private static final class TestCertificate extends Certificate {
        private static final long serialVersionUID = 1L;
        private final byte[] encoded;

        private TestCertificate(final byte[] encoded) {
            super("X.509");
            this.encoded = Bytes.copy(encoded);
        }

        @Override
        public byte[] getEncoded() {
            return Bytes.copy(encoded);
        }

        @Override
        public void verify(final PublicKey key) {
            // The verifier binds the exact encoded peer certificate bytes.
        }

        @Override
        public void verify(final PublicKey key, final String signatureProvider) {
            // The verifier binds the exact encoded peer certificate bytes.
        }

        @Override
        public String toString() {
            return "test-certificate";
        }

        @Override
        public PublicKey getPublicKey() {
            return null;
        }
    }
}
