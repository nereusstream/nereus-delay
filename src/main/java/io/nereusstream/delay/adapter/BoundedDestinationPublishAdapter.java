package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Adds the local physical request/byte gate to a target adapter.
 *
 * <p>The wrapper never turns a transport result into authoritative shard
 * state.  A rejected admission is a local pre-send result; a completed
 * delegate stage releases the physical charge even when its side-effect
 * result is {@code UNKNOWN}.  A caller that times out its logical callback
 * must retain the returned {@link PublishCall}'s reservation as a zombie
 * until the delegate stage completes or a separately certified teardown
 * releases it.</p>
 */
public final class BoundedDestinationPublishAdapter implements DestinationPublishAdapter {
    private final DestinationPublishAdapter delegate;
    private final DestinationPhysicalAdmission admission;
    private boolean closed;

    public BoundedDestinationPublishAdapter(final DestinationPublishAdapter delegate,
                                            final DestinationPhysicalAdmission admission) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.admission = Objects.requireNonNull(admission, "admission");
    }

    /**
     * Submits a request and exposes its reservation so a callback deadline can
     * mark the physical operation zombie without releasing it early.
     */
    public synchronized PublishCall submit(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        if (closed) {
            return PublishCall.completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final long physicalBytes = requestPhysicalBytes(request);
        final DestinationPhysicalAdmission.AdmissionDecision decision = admission.tryAcquire(
                request.laneId(), request.laneIncarnation(), physicalBytes);
        if (!decision.granted()) {
            final byte[] evidence = Bytes.utf8("physical-admission:" + decision.rejection().name());
            return PublishCall.completed(DestinationPublishResult.definitelyNotPublished(
                    StableCode.CAPABILITY_UNAVAILABLE, evidence));
        }
        final DestinationPhysicalAdmission.Reservation reservation = decision.reservation();
        final CompletionStage<DestinationPublishResult> raw;
        try {
            raw = delegate.publish(request);
        } catch (RuntimeException exception) {
            return withRelease(reservation, completedUnknown());
        }
        final CompletionStage<DestinationPublishResult> normalized = raw == null
                ? completedUnknown()
                : raw.handle((value, error) -> error == null && value != null ? value : completedUnknownValue());
        return withRelease(reservation, normalized);
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request) {
        return submit(request).outcome();
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            delegate.close();
        }
    }

    public static long requestPhysicalBytes(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        return Math.addExact(request.payload().length, request.adapterMetadata().length);
    }

    private static CompletionStage<DestinationPublishResult> completedUnknown() {
        return CompletableFuture.completedFuture(completedUnknownValue());
    }

    private static DestinationPublishResult completedUnknownValue() {
        return DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null);
    }

    private static PublishCall withRelease(final DestinationPhysicalAdmission.Reservation reservation,
                                           final CompletionStage<DestinationPublishResult> outcome) {
        final PublishCall call = PublishCall.from(reservation, outcome);
        outcome.whenComplete((value, error) -> reservation.release());
        return call;
    }

    public static final class PublishCall {
        private final CompletionStage<DestinationPublishResult> outcome;
        private final DestinationPhysicalAdmission.Reservation reservation;

        private PublishCall(final CompletionStage<DestinationPublishResult> outcome,
                            final DestinationPhysicalAdmission.Reservation reservation) {
            this.outcome = Objects.requireNonNull(outcome, "outcome");
            this.reservation = reservation;
        }

        private static PublishCall from(final DestinationPhysicalAdmission.Reservation reservation,
                                        final CompletionStage<DestinationPublishResult> outcome) {
            return new PublishCall(outcome, reservation);
        }

        private static PublishCall completed(final CompletionStage<DestinationPublishResult> outcome) {
            return new PublishCall(outcome, null);
        }

        private static PublishCall completed(final DestinationPublishResult outcome) {
            return completed(CompletableFuture.completedFuture(outcome));
        }

        public CompletionStage<DestinationPublishResult> outcome() {
            return outcome;
        }

        public DestinationPhysicalAdmission.Reservation reservation() {
            return reservation;
        }

        /** Marks a logical callback timeout without releasing physical charge. */
        public boolean markCallbackTimeout() {
            return reservation != null && reservation.markZombie();
        }

        /** Releases only after a certified physical completion/cancellation. */
        public boolean releasePhysicalCharge() {
            return reservation != null && reservation.release();
        }
    }
}
