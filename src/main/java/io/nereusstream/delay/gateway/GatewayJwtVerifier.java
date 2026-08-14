package io.nereusstream.delay.gateway;

import javax.net.ssl.SSLSession;

/** Explicit JWT verification seam; implementations must validate signature and claims. */
@FunctionalInterface
public interface GatewayJwtVerifier {
    GatewayJwtIdentity verify(String bearerToken, SSLSession peerSession);
}
