package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.ownership.OxiaSyncOwnerLeaseBackend;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("real-service")
class OxiaRealRetryPolicyCatalogSmokeTest {
    @Test
    void independentSessionsReopenExactImmutableVersionAndRejectVisibilityRewrite() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT missing");
        final String prefix = "nereus-delay-real-retry/" + UUID.randomUUID();
        try (var first = OxiaSyncOwnerLeaseBackend.connect(endpoint, "default", "retry-catalog-a",
                    Duration.ofSeconds(15), prefix + "/sessions");
                var second = OxiaSyncOwnerLeaseBackend.connect(endpoint, "default", "retry-catalog-b",
                    Duration.ofSeconds(15), prefix + "/sessions")) {
            final var a = new OxiaSyncRetryPolicyCatalog(first, prefix + "/catalog");
            final var b = new OxiaSyncRetryPolicyCatalog(second, prefix + "/catalog");
            final var semantic = OxiaSyncRetryPolicyCatalogTest.policy(10);
            final var source = OxiaSyncRetryPolicyCatalogTest.source(10, 100);
            final var start = new java.util.concurrent.CountDownLatch(1);
            final var left = java.util.concurrent.CompletableFuture.runAsync(
                    () -> publishAfter(start, a, semantic, source));
            final var right = java.util.concurrent.CompletableFuture.runAsync(
                    () -> publishAfter(start, b, semantic, source));
            start.countDown();
            java.util.concurrent.CompletableFuture.allOf(left, right).get(30, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(semantic, b.resolve(semantic.ref(), source));
            assertNull(b.resolve(semantic.ref(), OxiaSyncRetryPolicyCatalogTest.source(9, 99)));
            assertThrows(IllegalStateException.class, () -> b.publish(semantic,
                    OxiaSyncRetryPolicyCatalogTest.source(11, 101)));
            assertThrows(IllegalStateException.class,
                    () -> b.publish(OxiaSyncRetryPolicyCatalogTest.policy(11), source));
            assertEquals(semantic, a.resolve(semantic.ref(), OxiaSyncRetryPolicyCatalogTest.source(11, 101)));
        }
    }

    private static void publishAfter(java.util.concurrent.CountDownLatch start, OxiaSyncRetryPolicyCatalog catalog,
            com.nereusstream.delay.protocol.RetryPolicySemantic semantic,
            com.nereusstream.delay.protocol.SourcePosition source) {
        try {
            if (!start.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new IllegalStateException("Retry Policy publication test start timed out");
            }
            catalog.publish(semantic, source);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
    }
}
