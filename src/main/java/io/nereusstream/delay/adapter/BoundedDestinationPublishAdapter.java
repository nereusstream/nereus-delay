package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.StableCode;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Adds the local physical request/byte gate to a target adapter.
 *
 * <p>The wrapper never turns a transport result into authoritative shard
 * state.  A rejected admission is a local pre-send result; a completed
 * delegate stage releases the physical charge even when its side-effect
 * result is {@code UNKNOWN}.  A caller that times out its logical callback,
 * or a wrapper that cannot register a completion callback, must retain the
 * returned {@link PublishCall}'s reservation as a zombie until the delegate
 * stage completes or a separately certified teardown releases it.</p>
 */
public final class BoundedDestinationPublishAdapter implements DestinationPublishAdapter {
    private final DestinationPublishAdapter delegate;
    private final DestinationPhysicalAdmission admission;
    private final Executor executor;
    private final ExecutorService ownedExecutor;
    private final CloseGuard closeGuard = new CloseGuard();

    public BoundedDestinationPublishAdapter(final DestinationPublishAdapter delegate,
                                            final DestinationPhysicalAdmission admission) {
        this(delegate, admission, Executors.newVirtualThreadPerTaskExecutor(), true);
    }

    /**
     * Creates a wrapper with a caller-owned executor.  The executor must be a
     * bounded Lane/Adapter executor in production; this overload lets tests
     * use a deterministic direct executor without changing the admission
     * semantics.
     */
    public BoundedDestinationPublishAdapter(final DestinationPublishAdapter delegate,
                                            final DestinationPhysicalAdmission admission,
                                            final Executor executor) {
        this(delegate, admission, executor, false);
    }

    private BoundedDestinationPublishAdapter(final DestinationPublishAdapter delegate,
                                             final DestinationPhysicalAdmission admission,
                                             final Executor executor,
                                             final boolean ownsExecutor) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.admission = Objects.requireNonNull(admission, "admission");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.ownedExecutor = ownsExecutor ? (ExecutorService) executor : null;
    }

    /**
     * Submits a request and exposes its reservation so a callback deadline can
     * mark the physical operation zombie without releasing it early.
     */
    public PublishCall submit(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        if (closeGuard.isClosed()) {
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
        if (closeGuard.isClosed()) {
            reservation.release();
            return PublishCall.completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final CompletableFuture<DestinationPublishResult> outcome = new CompletableFuture<>();
        final AtomicBoolean retainPhysicalCharge = new AtomicBoolean();
        final AtomicBoolean completionObserved = new AtomicBoolean();
        try {
            executor.execute(() -> invokeDelegate(request, outcome, reservation, retainPhysicalCharge,
                    completionObserved));
        } catch (RuntimeException exception) {
            reservation.release();
            return PublishCall.completed(completedUnknownValue());
        }
        return withRelease(reservation, outcome, retainPhysicalCharge);
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request) {
        return submit(request).outcome();
    }

    @Override
    public void close() {
        closeGuard.close(() -> {
            RuntimeException failure = null;
            try {
                delegate.close();
            } catch (RuntimeException exception) {
                failure = exception;
            }
            if (ownedExecutor != null) {
                try {
                    ownedExecutor.shutdown();
                } catch (RuntimeException exception) {
                    if (failure == null) {
                        failure = exception;
                    } else {
                        failure.addSuppressed(exception);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        });
    }

    public static long requestPhysicalBytes(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        return Math.addExact(request.payload().length, request.adapterMetadata().length);
    }

    private static DestinationPublishResult completedUnknownValue() {
        return DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null);
    }

    private void invokeDelegate(final DestinationPublishRequest request,
                                final CompletableFuture<DestinationPublishResult> outcome,
                                final DestinationPhysicalAdmission.Reservation reservation,
                                final AtomicBoolean retainPhysicalCharge,
                                final AtomicBoolean completionObserved) {
        final DelegateInvocation invocation = closeGuard.invokeIfOpen(
                () -> {
                    try {
                        return new DelegateInvocation(delegate.publish(request), false);
                    } catch (RuntimeException exception) {
                        // A synchronous transport exception does not prove
                        // that the request stopped before Producer/channel
                        // ownership. Preserve the same unobserved marker used
                        // by the pinned adapters so the physical charge is
                        // retained until certified completion or teardown.
                        return new DelegateInvocation(UnobservedDestinationPublishStage.unknown(), false);
                    }
                },
                () -> new DelegateInvocation(null, true));
        if (invocation.closed()) {
            outcome.complete(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
            return;
        }
        final CompletionStage<DestinationPublishResult> raw = invocation.stage();
        if (raw == null) {
            // A null CompletionStage is not a non-persistence proof. A
            // generic delegate may have acquired physical ownership before
            // losing the stage, so retain the reservation as an unobserved
            // operation rather than releasing it on logical UNKNOWN.
            retainPhysicalCharge.set(true);
            reservation.markZombie();
            outcome.complete(completedUnknownValue());
            return;
        }
        if (raw instanceof UnobservedDestinationPublishStage) {
            // The pinned adapter completed the logical branch only after it
            // failed to observe the transport stage. This is not a physical
            // release proof; retain the charge for explicit teardown/release.
            retainPhysicalCharge.set(true);
            reservation.markZombie();
            outcome.complete(completedUnknownValue());
            return;
        }
        try {
            registerCompletion(raw, (value, error) -> {
                completionObserved.set(true);
                outcome.complete(error == null && value != null ? value : completedUnknownValue());
                if (retainPhysicalCharge.get()) {
                    // Registration may have thrown after installing the
                    // callback. In that race the outcome callback was
                    // already completed while release was intentionally
                    // suppressed; the observed delegate completion now
                    // makes the physical charge releasable.
                    reservation.release();
                }
            });
        } catch (RuntimeException registrationFailure) {
            // A custom CompletionStage may reject both callback-registration
            // paths after the delegate has already acquired Producer
            // ownership.  UNKNOWN is useful for the logical caller, but it
            // is not physical completion. Keep the reservation as a zombie
            // (or in-flight if the zombie cap is already exhausted) until a
            // caller proves completion or a fenced teardown releases it.
            if (!completionObserved.get()) {
                retainPhysicalCharge.set(true);
                reservation.markZombie();
            }
            outcome.complete(completedUnknownValue());
            if (completionObserved.get()) {
                reservation.release();
            }
        }
    }

    private static void registerCompletion(final CompletionStage<DestinationPublishResult> raw,
                                           final BiConsumer<? super DestinationPublishResult,
                                                   ? super Throwable> callback) {
        try {
            raw.whenComplete(callback);
            return;
        } catch (RuntimeException firstFailure) {
            try {
                // Some adapters expose a custom CompletionStage wrapper but
                // still provide a standard CompletableFuture view.
                final CompletableFuture<DestinationPublishResult> future = raw.toCompletableFuture();
                if (future == null) {
                    throw new IllegalStateException("CompletionStage returned a null CompletableFuture view");
                }
                future.whenComplete(callback);
                return;
            } catch (RuntimeException fallbackFailure) {
                firstFailure.addSuppressed(fallbackFailure);
                throw firstFailure;
            }
        }
    }

    private static PublishCall withRelease(final DestinationPhysicalAdmission.Reservation reservation,
                                           final CompletionStage<DestinationPublishResult> outcome,
                                           final AtomicBoolean retainPhysicalCharge) {
        final PublishCall call = PublishCall.from(reservation, outcome);
        outcome.whenComplete((value, error) -> {
            if (!retainPhysicalCharge.get()) {
                reservation.release();
            }
        });
        return call;
    }

    private record DelegateInvocation(CompletionStage<DestinationPublishResult> stage, boolean closed) {
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
