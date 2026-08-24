package com.nereusstream.delay.gateway;

import com.nereusstream.delay.semantic.AuthenticatedTenantContext;
import io.grpc.Grpc;
import io.grpc.Metadata;
import java.security.cert.Certificate;
import java.util.Objects;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

/**
 * Strict mTLS plus JWT tenant authority composition.
 *
 * <p>The JWT parser/signature/claim policy is injected through
 * {@link GatewayJwtVerifier}; this class enforces that a verified token is
 * presented on a peer with an authenticated client certificate and projects
 * only digest values into the trusted tenant context.</p>
 */
public final class MutualTlsJwtGatewayTenantAuthority implements GatewayTenantAuthority {
    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final GatewayJwtVerifier verifier;

    public MutualTlsJwtGatewayTenantAuthority(final GatewayJwtVerifier verifier) {
        this.verifier = Objects.requireNonNull(verifier, "verifier");
    }

    @Override
    public AuthenticatedTenantContext authenticate(final GatewayPeerContext peerContext) {
        Objects.requireNonNull(peerContext, "peerContext");
        final SSLSession peerSession = peerContext.transportAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        requireClientCertificate(peerSession);
        final String authorization = peerContext.headers().get(AUTHORIZATION);
        if (authorization == null
                || !authorization.startsWith("Bearer ")
                || authorization.length() == "Bearer ".length()) {
            throw new IllegalArgumentException("Gateway requires a Bearer JWT");
        }
        final String token = authorization.substring("Bearer ".length());
        if (token.indexOf('\r') >= 0
                || token.indexOf('\n') >= 0
                || token.indexOf(' ') >= 0
                || token.indexOf('\t') >= 0) {
            throw new IllegalArgumentException("Gateway bearer token contains whitespace");
        }
        final GatewayJwtIdentity identity =
                Objects.requireNonNull(verifier.verify(token, peerSession), "JWT verifier returned no identity");
        return new AuthenticatedTenantContext(
                identity.authenticatedTenantScopeHash(), identity.tenantRoutingScope(), identity.principalScopeHash());
    }

    private static void requireClientCertificate(final SSLSession peerSession) {
        if (peerSession == null) {
            throw new IllegalArgumentException("Gateway requires a mutual-TLS peer");
        }
        try {
            final Certificate[] certificates = peerSession.getPeerCertificates();
            if (certificates == null || certificates.length == 0) {
                throw new IllegalArgumentException("Gateway peer has no client certificate");
            }
        } catch (SSLPeerUnverifiedException failure) {
            throw new IllegalArgumentException("Gateway peer certificate is not verified", failure);
        }
    }
}
