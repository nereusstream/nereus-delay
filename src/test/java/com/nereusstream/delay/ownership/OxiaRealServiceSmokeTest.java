package com.nereusstream.delay.ownership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
    void contextBoundOwnerLeaseTakeoverFencesTheOldSessionAgainstRealService() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-target-owner/" + UUID.randomUUID();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final UUID topicId = UUID.randomUUID();
        final SourceAssignment firstAssignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("first-assignment")),
                1,
                new KafkaActivationBarrier(shard, "cluster", topicId, 0));
        final SourceAssignment nextAssignment = new SourceAssignment(
                shard,
                Bytes.sha256(Bytes.utf8("next-assignment")),
                2,
                new KafkaActivationBarrier(shard, "cluster", topicId, 0));
        final long leaseDurationMs = 60_000;

        try (OxiaSyncOwnerLeaseBackend.ClientHandle second = OxiaSyncOwnerLeaseBackend.connect(
                endpoint, namespace, "nereus-delay-real-target-b-" + UUID.randomUUID(),
                Duration.ofSeconds(15), prefix)) {
            final OxiaOwnerLeaseStore secondLeases = new OxiaOwnerLeaseStore(second.backend());
            final OwnerLease oldActive;
            try (OxiaSyncOwnerLeaseBackend.ClientHandle first = OxiaSyncOwnerLeaseBackend.connect(
                    endpoint, namespace, "nereus-delay-real-target-a-" + UUID.randomUUID(),
                    Duration.ofSeconds(15), prefix)) {
                final OxiaOwnerLeaseStore firstLeases = new OxiaOwnerLeaseStore(first.backend());
                final long now = System.currentTimeMillis();
                final OwnerLease acquired = firstLeases.acquire(
                        firstAssignment, "target-owner-a", first.sessionIdentity(), now, leaseDurationMs)
                        .orElseThrow();
                oldActive = firstLeases.transition(acquired, ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                        .orElseThrow();
                assertEquals(firstAssignment.assignmentEpoch(), oldActive.sourceAssignmentEpoch());
                assertTrue(secondLeases.acquire(
                        nextAssignment, "target-owner-b", second.sessionIdentity(), now + 1, leaseDurationMs)
                        .isEmpty());
            }

            assertTrue(secondLeases.current(shard).isEmpty(), "closing the old session must remove its lease");
            final OwnerLease replacement = secondLeases.acquire(
                    nextAssignment, "target-owner-b", second.sessionIdentity(),
                    System.currentTimeMillis(), leaseDurationMs)
                    .orElseThrow();
            assertTrue(Long.compareUnsigned(replacement.ownerEpoch(), oldActive.ownerEpoch()) > 0);
            assertEquals(nextAssignment.assignmentEpoch(), replacement.sourceAssignmentEpoch());
            assertThrows(IllegalStateException.class, () -> secondLeases.release(oldActive));
            assertTrue(replacement.sameIdentity(secondLeases.current(shard).orElseThrow()));
            assertTrue(secondLeases.release(replacement));
        }
    }

    @Test
    void ownerLeaseCasAndEphemeralSessionWorkAgainstRealService() throws Exception {
        final String endpoint = System.getenv("NEREUS_DELAY_OXIA_ENDPOINT");
        Assumptions.assumeTrue(endpoint != null && !endpoint.isBlank(), "NEREUS_DELAY_OXIA_ENDPOINT is not configured");
        final String namespace = configured("NEREUS_DELAY_OXIA_NAMESPACE", "default");
        final String prefix = "nereus-delay-real-smoke/" + UUID.randomUUID();
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final long now = System.currentTimeMillis();
        final long leaseDurationMs = 30_000;
        final long firstEpoch;

        try (OxiaSyncOwnerLeaseBackend.ClientHandle first = OxiaSyncOwnerLeaseBackend.connect(
                endpoint,
                namespace,
                "nereus-delay-real-smoke-a-" + UUID.randomUUID(),
                Duration.ofSeconds(15),
                prefix)) {
            final OwnerLease acquired = first.backend()
                    .acquire(shard, "worker-a", now, leaseDurationMs)
                    .orElseThrow();
            firstEpoch = acquired.ownerEpoch();
            assertEquals(ShardLifecycleState.ACQUIRING, acquired.state());
            final OwnerLease renewed = first.backend()
                    .renew(acquired, now + 1_000, leaseDurationMs)
                    .orElseThrow();
            assertEquals(now + 1_000 + leaseDurationMs, renewed.expiresAtEpochMs());
            final OwnerLease restoring = first.backend()
                    .transition(renewed, ShardLifecycleState.RESTORING)
                    .orElseThrow();
            assertEquals(ShardLifecycleState.RESTORING, restoring.state());
        }

        try (OxiaSyncOwnerLeaseBackend.ClientHandle second = OxiaSyncOwnerLeaseBackend.connect(
                endpoint,
                namespace,
                "nereus-delay-real-smoke-b-" + UUID.randomUUID(),
                Duration.ofSeconds(15),
                prefix)) {
            assertTrue(
                    second.backend().current(shard).isEmpty(),
                    "closing the first Oxia session must remove its ephemeral lease");
            final OwnerLease reacquired = second.backend()
                    .acquire(shard, "worker-b", System.currentTimeMillis(), leaseDurationMs)
                    .orElseThrow();
            assertEquals(firstEpoch + 1, reacquired.ownerEpoch());
            assertTrue(second.backend().release(reacquired));
        }
    }

    private static String configured(final String name, final String fallback) {
        final String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
