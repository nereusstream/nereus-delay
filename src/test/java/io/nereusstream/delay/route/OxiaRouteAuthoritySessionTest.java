package io.nereusstream.delay.route;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;

class OxiaRouteAuthoritySessionTest {
    @Test
    void connectRejectsAnInvalidKeyPrefixBeforeCreatingOxiaClients() {
        assertThrows(IllegalArgumentException.class, () -> OxiaRouteAuthoritySession.connect(
                "invalid-endpoint", "default", "route-client", Duration.ofSeconds(1),
                "/nereus/route/"));
    }
}
