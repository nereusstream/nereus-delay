package com.nereusstream.delay.route;

import static org.junit.jupiter.api.Assertions.assertThrows;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class OxiaRouteAuthoritySessionTest {
    @Test
    void connectRejectsAnInvalidKeyPrefixBeforeCreatingOxiaClients() {
        assertThrows(
                IllegalArgumentException.class,
                () -> OxiaRouteAuthoritySession.connect(
                        "invalid-endpoint", "default", "route-client", Duration.ofSeconds(1), "/nereus/route/"));
    }
}
