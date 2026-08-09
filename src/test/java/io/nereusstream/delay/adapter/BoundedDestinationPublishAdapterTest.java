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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);

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
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);

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
    void failedOrNullDelegateStageRetainsChargeUntilExplicitRelease() {
        final DestinationLaneId lane = lane("failure");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final DestinationPublishAdapter failing = request -> {
            throw new IllegalStateException("transport failed before a result");
        };
        final BoundedDestinationPublishAdapter failingAdapter =
                new BoundedDestinationPublishAdapter(failing, admission, Runnable::run);
        final BoundedDestinationPublishAdapter.PublishCall failedCall =
                failingAdapter.submit(request(lane, 10));
        final DestinationPublishResult failed = failedCall.outcome().toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, failed.disposition());
        assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, failed.stableCode());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, failedCall.reservation().state());
        assertEquals(1, admission.workerSnapshot().activeRequests());
        assertTrue(failedCall.releasePhysicalCharge());

        final DestinationPublishAdapter nullStage = request -> null;
        final BoundedDestinationPublishAdapter nullAdapter =
                new BoundedDestinationPublishAdapter(nullStage, admission, Runnable::run);
        final BoundedDestinationPublishAdapter.PublishCall missingCall = nullAdapter.submit(request(lane, 10));
        final DestinationPublishResult missing = missingCall.outcome().toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, missing.disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, missingCall.reservation().state());
        assertEquals(1, admission.workerSnapshot().activeRequests());
        assertTrue(missingCall.releasePhysicalCharge());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void callbackRegistrationFailureRetainsPhysicalChargeUntilExplicitRelease() {
        final DestinationLaneId lane = lane("registration-failure");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final DestinationPublishAdapter delegate = request -> new RegistrationFailureFuture<>();
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);

        final BoundedDestinationPublishAdapter.PublishCall call = adapter.submit(request(lane, 20));
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN,
                call.outcome().toCompletableFuture().join().disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, call.reservation().state());
        assertEquals(1, admission.workerSnapshot().activeRequests());
        assertTrue(call.releasePhysicalCharge());
        assertEquals(DestinationPhysicalAdmission.ReservationState.RELEASED, call.reservation().state());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void nullWhenCompleteReturnIsTreatedAsUnobservedCompletion() {
        final DestinationLaneId lane = lane("null-when-complete");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final DestinationPublishAdapter delegate = request -> new NullWhenCompleteFuture<>();
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);

        final BoundedDestinationPublishAdapter.PublishCall call = adapter.submit(request(lane, 20));
        final DestinationPublishResult result = call.outcome().toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
        assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, call.reservation().state());
        assertEquals(1, admission.workerSnapshot().activeRequests());
        assertTrue(call.releasePhysicalCharge());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void pinnedAdapterRegistrationFailureRetainsPhysicalCharge() {
        final DestinationLaneId lane = lane("pinned-registration-failure");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 0);
        final DestinationPublishAdapter delegate = new PinnedKafkaDestinationAdapter(resource,
                request -> new PinnedRegistrationFailureFuture<>());
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);

        final BoundedDestinationPublishAdapter.PublishCall call = adapter.submit(request(lane, 20));
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN,
                call.outcome().toCompletableFuture().join().disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, call.reservation().state());
        assertEquals(1, admission.workerSnapshot().activeRequests());
        assertTrue(call.releasePhysicalCharge());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void pinnedAdapterNullHandledStageRetainsPhysicalCharge() {
        final DestinationLaneId lane = lane("null-handled-stage");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 0);
        final DestinationPublishAdapter delegate = new PinnedKafkaDestinationAdapter(resource,
                request -> new NullHandledFuture<>());
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);

        final BoundedDestinationPublishAdapter.PublishCall call = adapter.submit(request(lane, 20));
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN,
                call.outcome().toCompletableFuture().join().disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, call.reservation().state());
        assertEquals(1, admission.workerSnapshot().activeRequests());
        assertTrue(call.releasePhysicalCharge());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void pinnedAdapterTransportExceptionRetainsPhysicalCharge() {
        final DestinationLaneId lane = lane("pinned-transport-failure");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final KafkaTargetResource resource = new KafkaTargetResource("cluster", UUID.randomUUID(), 0);
        final DestinationPublishAdapter delegate = new PinnedKafkaDestinationAdapter(resource,
                request -> {
                    throw new IllegalStateException("transport ownership is unknown");
                });
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);

        final BoundedDestinationPublishAdapter.PublishCall call = adapter.submit(request(lane, 20));
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN,
                call.outcome().toCompletableFuture().join().disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.ZOMBIE, call.reservation().state());
        assertEquals(1, admission.workerSnapshot().activeRequests());
        assertTrue(call.releasePhysicalCharge());
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void closeStopsNewCallsWithoutReleasingOtherPhysicalReservations() {
        final DestinationLaneId lane = lane("close");
        final DestinationPhysicalAdmission admission = admission(lane, 2, 40, 1, 20);
        admission.openReady(lane);
        final CompletableFuture<DestinationPublishResult> pending = new CompletableFuture<>();
        final DestinationPublishAdapter delegate = request -> pending;
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, Runnable::run);
        final var call = adapter.submit(request(lane, 10));
        adapter.close();
        final DestinationPublishResult closed = adapter.publish(request(lane, 10)).toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, closed.disposition());
        assertEquals(DestinationPhysicalAdmission.ReservationState.IN_FLIGHT, call.reservation().state());
        pending.complete(published());
        call.outcome().toCompletableFuture().join();
        assertEquals(0, admission.workerSnapshot().activeRequests());
    }

    @Test
    void blockingDelegateCallDoesNotBlockHealthyLane() throws Exception {
        final DestinationLaneId blockedLane = lane("blocked");
        final DestinationLaneId healthyLane = lane("healthy");
        final DestinationPhysicalAdmission admission = multiLaneAdmission(blockedLane, healthyLane);
        admission.openReady(blockedLane);
        admission.openReady(healthyLane);
        final CompletableFuture<Void> unblock = new CompletableFuture<>();
        final CountDownLatch entered = new CountDownLatch(1);
        final DestinationPublishAdapter delegate = request -> {
            if (request.laneId().equals(blockedLane)) {
                entered.countDown();
                unblock.join();
            }
            return CompletableFuture.completedFuture(published());
        };
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(delegate, admission);

        final BoundedDestinationPublishAdapter.PublishCall blocked = adapter.submit(request(blockedLane, 10));
        assertTrue(entered.await(1, TimeUnit.SECONDS));
        final BoundedDestinationPublishAdapter.PublishCall healthy = adapter.submit(request(healthyLane, 10));
        assertEquals(DestinationPublishResult.Disposition.PUBLISHED,
                healthy.outcome().toCompletableFuture().get(1, TimeUnit.SECONDS).disposition());
        assertFalse(blocked.outcome().toCompletableFuture().isDone());

        unblock.complete(null);
        assertEquals(DestinationPublishResult.Disposition.PUBLISHED,
                blocked.outcome().toCompletableFuture().get(1, TimeUnit.SECONDS).disposition());
        assertEquals(0, admission.workerSnapshot().activeRequests());
        adapter.close();
    }

    @Test
    void executorRejectionReleasesPhysicalCharge() {
        final DestinationLaneId lane = lane("executor-rejection");
        final DestinationPhysicalAdmission admission = admission(lane, 1, 20, 1, 20);
        admission.openReady(lane);
        final DestinationPublishAdapter delegate = request ->
                CompletableFuture.completedFuture(published());
        final BoundedDestinationPublishAdapter adapter = new BoundedDestinationPublishAdapter(
                delegate, admission, task -> {
                    throw new RejectedExecutionException("executor is closed");
                });

        final DestinationPublishResult result = adapter.publish(request(lane, 10)).toCompletableFuture().join();
        assertEquals(DestinationPublishResult.Disposition.UNKNOWN, result.disposition());
        assertEquals(StableCode.DESTINATION_OUTCOME_UNKNOWN, result.stableCode());
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

    private static DestinationPhysicalAdmission multiLaneAdmission(final DestinationLaneId first,
                                                                   final DestinationLaneId second) {
        final DestinationPhysicalAdmission result = new DestinationPhysicalAdmission(2, 40);
        result.registerTargetCluster("cluster-a", 2, 40);
        result.registerLane(new DestinationPhysicalAdmission.LaneSpec(
                first, new byte[16], "cluster-a", 0, 0, 2, 40, 1, 20));
        result.registerLane(new DestinationPhysicalAdmission.LaneSpec(
                second, new byte[16], "cluster-a", 0, 0, 2, 40, 1, 20));
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

    private static final class RegistrationFailureFuture<T> extends CompletableFuture<T> {
        @Override
        public CompletableFuture<T> toCompletableFuture() {
            throw new IllegalStateException("CompletableFuture view unavailable");
        }

        @Override
        public CompletableFuture<T> whenComplete(
                final BiConsumer<? super T, ? super Throwable> action) {
            throw new IllegalStateException("completion callback registration failed");
        }
    }

    private static final class NullHandledFuture<T> extends CompletableFuture<T> {
        @Override
        public <U> CompletableFuture<U> handle(
                final java.util.function.BiFunction<? super T, Throwable, ? extends U> ignored) {
            return null;
        }
    }

    private static final class NullWhenCompleteFuture<T> extends CompletableFuture<T> {
        @Override
        public CompletableFuture<T> whenComplete(
                final BiConsumer<? super T, ? super Throwable> action) {
            super.whenComplete(action);
            return null;
        }
    }

    private static final class PinnedRegistrationFailureFuture<T> extends CompletableFuture<T> {
        @Override
        public CompletableFuture<T> toCompletableFuture() {
            throw new IllegalStateException("CompletableFuture view unavailable");
        }

        @Override
        public <U> CompletableFuture<U> handle(
                final java.util.function.BiFunction<? super T, Throwable, ? extends U> function) {
            throw new IllegalStateException("completion callback registration failed");
        }
    }
}
