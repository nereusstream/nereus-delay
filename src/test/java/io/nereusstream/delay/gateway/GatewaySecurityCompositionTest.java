package io.nereusstream.delay.gateway;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.nereusstream.delay.semantic.AuthenticatedTenantContext;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLSession;
import java.lang.reflect.Proxy;
import java.security.PublicKey;
import java.security.cert.Certificate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GatewaySecurityCompositionTest {
    @Test
    void mutualTlsJwtAuthorityRequiresBothTransportCertificateAndBearerToken() {
        final Metadata headers = new Metadata();
        final GatewayPeerContext noTls = new GatewayPeerContext(headers, Attributes.EMPTY);
        final MutualTlsJwtGatewayTenantAuthority authority = new MutualTlsJwtGatewayTenantAuthority(
                (token, session) -> new GatewayJwtIdentity(bytes(1), bytes(2), bytes(3)));
        assertThrows(IllegalArgumentException.class, () -> authority.authenticate(noTls));

        final SSLSession session = peerSession();
        final Attributes attributes = Attributes.newBuilder()
                .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, session)
                .build();
        final GatewayPeerContext missingToken = new GatewayPeerContext(headers, attributes);
        assertThrows(IllegalArgumentException.class, () -> authority.authenticate(missingToken));

        headers.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer signed-token");
        final AuthenticatedTenantContext context = authority.authenticate(
                new GatewayPeerContext(headers, attributes));
        assertArrayEquals(bytes(1), context.authenticatedTenantScopeHash());
        assertArrayEquals(bytes(2), context.tenantRoutingScope());
        assertArrayEquals(bytes(3), context.principalScopeHash());
    }

    @Test
    void jwtIdentityDefensivelyCopiesAllScopeDigests() {
        final byte[] tenant = bytes(4);
        final byte[] routing = bytes(5);
        final byte[] principal = bytes(6);
        final GatewayJwtIdentity identity = new GatewayJwtIdentity(tenant, routing, principal);
        tenant[0] = 0;
        routing[0] = 0;
        principal[0] = 0;
        assertEquals(4, identity.authenticatedTenantScopeHash()[0]);
        assertEquals(5, identity.tenantRoutingScope()[0]);
        assertEquals(6, identity.principalScopeHash()[0]);
    }

    private static SSLSession peerSession() {
        final Certificate certificate = new Certificate("TEST") {
            @Override
            public byte[] getEncoded() {
                return new byte[0];
            }

            @Override
            public void verify(final PublicKey key) {
                // The gRPC TLS stack, not this projection test, verifies the chain.
            }

            @Override
            public void verify(final PublicKey key, final String signatureProvider) {
                // The gRPC TLS stack, not this projection test, verifies the chain.
            }

            @Override
            public PublicKey getPublicKey() {
                return null;
            }

            @Override
            public String toString() {
                return "test-client-certificate";
            }
        };
        return (SSLSession) Proxy.newProxyInstance(SSLSession.class.getClassLoader(),
                new Class<?>[] {SSLSession.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("getPeerCertificates")) {
                        return new Certificate[] {certificate};
                    }
                    if (method.getReturnType() == boolean.class) {
                        return false;
                    }
                    if (method.getReturnType() == int.class) {
                        return 0;
                    }
                    if (method.getReturnType() == long.class) {
                        return 0L;
                    }
                    if (method.getReturnType() == byte[].class) {
                        return new byte[0];
                    }
                    if (method.getReturnType() == String.class) {
                        return "TLSv1.3";
                    }
                    return null;
                });
    }

    private static byte[] bytes(final int seed) {
        final byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return value;
    }
}
