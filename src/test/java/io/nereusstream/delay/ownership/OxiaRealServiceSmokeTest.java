package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in smoke coverage against a running Oxia service.
 *
 * <p>The regular test suite intentionally remains self-contained. Set
 * {@code NEREUS_DELAY_OXIA_ENDPOINT} to run this test against a real service
 * and prove the client/session/CAS boundary rather than a fake record seam.</p>
 */
@Tag("real-service")
class OxiaRealServiceSmokeTest {
    @Test
    void ownerLeaseCasAndEphemeralSessionWorkAgainstRealService() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(),
                "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-smoke/" + UUID.randomUUID();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final long now = System.currentTimeMillis();
        final long leaseDurationMs = 30_000;
        final long firstEpoch;

        try (OxiaSyncOwnerLeaseBackend.ClientHandle first = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-real-smoke-a-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix)) {
            final OwnerLease acquired = first.backend().acquire(shard, "worker-a", now, leaseDurationMs)
                    .orElseThrow();
            firstEpoch = acquired.ownerEpoch();
            assertEquals(ShardLifecycleState.ACQUIRING, acquired.state());
            final OwnerLease renewed = first.backend().renew(acquired, now + 1_000, leaseDurationMs)
                    .orElseThrow();
            assertEquals(now + 1_000 + leaseDurationMs, renewed.expiresAtEpochMs());
            final OwnerLease restoring = first.backend().transition(renewed, ShardLifecycleState.RESTORING)
                    .orElseThrow();
            assertEquals(ShardLifecycleState.RESTORING, restoring.state());
        }

        try (OxiaSyncOwnerLeaseBackend.ClientHandle second = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-real-smoke-b-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix)) {
            assertTrue(second.backend().current(shard).isEmpty(),
                    "closing the first Oxia session must remove its ephemeral lease");
            final OwnerLease reacquired = second.backend().acquire(shard, "worker-b",
                    System.currentTimeMillis(), leaseDurationMs).orElseThrow();
            assertEquals(firstEpoch + 1, reacquired.ownerEpoch());
            assertTrue(second.backend().release(reacquired));
        }
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
