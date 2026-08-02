package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.KafkaBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedDestinationPublishAdapterTest {
    @Test
    void callbackTimeoutRetainsPhysicalChargeUntilDelegateCompletes() {
        final DestinationLaneId lane = lane("timeout");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final CompletableFuture<DestinationPublishResult> pending = new CompletableFuture<>();
        final AtomicInteger calls = new AtomicInteger();
        final DestinationPublishAdapter delegate = request -> {
            calls.incrementAndGet();
            return pending;
        };
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(delegate, admission);

        final BoundedDestinationPublishAdapter.PublishCall call = adapter.submit(request(lane, 20));
        assertNotNull(call.reservation());
        assertEquals(DestinationPhysicalAdmission.ReservationState.IN_FLIGHT, call.reservation().state());
        assertTrue(call.markCallbackTimeout());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, call.reservation().state());
        final DestinationPublishResult blocked = adapter.submit(request(lane, 20)).outcome()
                .toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED, blocked.disposition());
        assertEquals(StableCode.CAPABILITY_UNAVAILABLE, blocked.stableCode());
        assertEquals(1, calls.get());

        pending.complete(published());
        assertEquals(DestinationPublishResult.Disposition.PUBLISHED,
                call.outcome().toCompletableFuture().join().disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.RELEASED, call.reservation().state());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void capacityRejectionDoesNotCallDelegateAndCompletedCallsRelease() {
        final DestinationLaneId lane = lane("capacity");
        final DestinationPhysicalAdmission admission = admission(lane, 1, 20, 1, 20);
        final AtomicInteger calls = new AtomicInteger();
        final DestinationPublishAdapter delegate = request -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(published());
        };
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(delegate, admission);

        final DestinationPublishResult notReady = adapter.publish(request(lane, 20)).toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.DEFINITIVELY_NOT_PUBLISHED, notReady.disposition());
        assertEquals(StableCode.CAPABILITY_UNAVAILABLE, notReady.stableCode());
        assertEquals(0, calls.get());

        admission.openReady(lane);
        assertEquals(DestinationPublishResult.Disposition.PUBLISHED,
                adapter.publish(request(lane, 20)).toCompletableFuture().join().disposition());
        assertEquals(1, calls.get());
        assertEquals(0, admission.workerSnapshot().activeRequests());

        assertEquals(DestinationPublishResult.Disposition.PUBLISHED,
                adapter.publish(request(lane, 20)).toCompletableFuture().join().disposition());
        assertEquals(2, calls.get());
    }

    @Test
    void failedOrNullDelegateStageBecomesUnknownAndReleasesCharge() {
        final DestinationLaneId lane = lane("failure");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final DestinationPublishAdapter failing = request -> {
            throw new IllegalStateException("transport failed before a result");
        };
        final BoundedDestinationPublishAdapter failingAdapter =
                new BoundedDestinationPublishAdapter(failing, admission);
        final DestinationPublishResult failed = failingAdapter.publish(request(lane, 10)).toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, failed.disposition());
        assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, failed.stableCode());
        assertEquals(0, admission.workerSnapshot().activeRequests());

        final DestinationPublishAdapter nullStage = request -> null;
        final BoundedDestinationPublishAdapter nullAdapter =
                new BoundedDestinationPublishAdapter(nullStage, admission);
        final DestinationPublishResult missing = nullAdapter.publish(request(lane, 10)).toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, missing.disposition());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void closeStopsNewCallsWithoutReleasingOtherPhysicalReservations() {
        final DestinationLaneId lane = lane("close");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final CompletableFuture<DestinationPublishResult> pending = new CompletableFuture<>();
        final DestinationPublishAdapter delegate = request -> pending;
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(delegate, admission);
        final var call = adapter.submit(request(lane, 10));
        adapter.close();
        final DestinationPublishResult closed = adapter.publish(request(lane, 10)).toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, closed.disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.IN_FLIGHT, call.reservation().state());
        pending.complete(published());
        call.outcome().toCompletableFuture().join();
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    private static DestinationPhysicalAdmission admission(final DestinationLaneId lane,
                                                          final long maxRequests, final long maxBytes,
                                                          final long maxZombieRequests, final long maxZombieBytes) {
        final DestinationPhysicalAdmission result = new DestinationPhysicalAdmission(maxRequests, maxBytes);
        result.registerTargetCluster("cluster-a", maxRequests, maxBytes);
        result.registerLane(new DestinationPhysicalAdmission.LaneSpec(lane, new byte[16], "cluster-a", 0, 0,
                maxRequests, maxBytes, maxZombieRequests, maxZombieBytes));
        return result;
    }

    private static DestinationPublishRequest request(final DestinationLaneId lane, final int payloadBytes) {
        return new DestinationPublishRequest(lane, new byte[16],
                DelayMessageId.random(new ShardId(RouteIncarnation.random(), 0)), 0, new byte[32], 1_000, 1_000,
                new byte[payloadBytes], new byte[0]);
    }

    private static DestinationPublishResult published() {
        return DestinationPublishResult.published(
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("cluster", UUID.randomUUID())),
                0, Bytes.utf8("record-id"), 1_001, Bytes.utf8("broker-ack"));
    }

    private static DestinationLaneId lane(final String seed) {
        return DestinationLaneId.derive(Bytes.utf8(seed));
    }
}
