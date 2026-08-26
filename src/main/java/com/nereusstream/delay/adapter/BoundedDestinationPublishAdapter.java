package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
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
 * state. A rejected admission is a local pre-send result; a completed
 * delegate stage releases the physical charge even when its side-effect
 * result is {@code UNKNOWN}. A caller that times out its logical callback,
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

    BoundedDestinationPublishAdapter(
            final DestinationPublishAdapter delegate, final DestinationPhysicalAdmission admission) {
        this(delegate, admission, Executors.newVirtualThreadPerTaskExecutor(), true);
    }

    /**
     * Creates a wrapper with a caller-owned executor. The executor must be a
     * bounded Lane/Adapter executor in production; this overload lets tests
     * use a deterministic direct executor without changing the admission
     * semantics.
     */
    BoundedDestinationPublishAdapter(
            final DestinationPublishAdapter delegate,
            final DestinationPhysicalAdmission admission,
            final Executor executor) {
        this(delegate, admission, executor, false);
    }

    /**
     * Cross-package Worker composition entrypoint. The exact physical pool is
     * bound to the shared Worker execution graph before any adapter call.
     */
    public BoundedDestinationPublishAdapter(
            final DestinationPublishAdapter delegate,
            final DestinationPhysicalAdmission admission,
            final WorkClassExecutionRegistry workClasses,
            final Executor executor) {
        this(delegate, admission, executor, false);
        this.admission.bindWorkClassExecutionRegistry(Objects.requireNonNull(workClasses, "workClasses"));
    }

    private BoundedDestinationPublishAdapter(
            final DestinationPublishAdapter delegate,
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
        return submit(request, ignored -> null);
    }

    /**
     * Submits a request with a final pre-transport gate.
     *
     * <p>The gate runs on the bounded adapter executor after the physical
     * reservation has been acquired and immediately before the delegate is
     * invoked. A non-null result completes the call without invoking the
     * delegate. This is the narrow seam used by the Worker to recheck the
     * live Owner/Store/Claim/channel fence after queue wait; it does not
     * manufacture a physical success or mutate shard state.</p>
     */
    public PublishCall submit(final DestinationPublishRequest request, final PublishPreflight preflight) {
        return submit(request, preflight, delegate::publish);
    }

    /**
     * Submits with the source position and prepared hash retained by a
     * durable Publish Attempt. The source-bound call remains behind the same
     * physical admission, preflight and zombie-release semantics.
     */
    public PublishCall submit(
            final DestinationPublishRequest request,
            final SourcePosition sourcePosition,
            final byte[] preparedPublishHash,
            final PublishPreflight preflight) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        Bytes.requireLength(preparedPublishHash, 32, "preparedPublishHash");
        return submit(request, preflight, ignored -> delegate.publish(ignored, sourcePosition, preparedPublishHash));
    }

    private PublishCall submit(
            final DestinationPublishRequest request,
            final PublishPreflight preflight,
            final DelegateCall delegateCall) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(preflight, "preflight");
        Objects.requireNonNull(delegateCall, "delegateCall");
        if (closeGuard.isClosed()) {
            return PublishCall.completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final long physicalBytes = requestPhysicalBytes(request);
        final DestinationPhysicalAdmission.AdmissionDecision decision =
                admission.tryAcquire(request.laneId(), request.laneIncarnation(), physicalBytes);
        if (!decision.granted()) {
            final byte[] evidence =
                    Bytes.utf8("physical-admission:" + decision.rejection().name());
            return PublishCall.completed(
                    DestinationPublishResult.definitelyNotPublished(StableCode.CAPABILITY_UNAVAILABLE, evidence));
        }
        final DestinationPhysicalAdmission.Reservation reservation = decision.reservation();
        if (closeGuard.isClosed()) {
            reservation.release();
            return PublishCall.completed(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final CompletableFuture<DestinationPublishResult> outcome = new CompletableFuture<>();
        final AtomicBoolean retainPhysicalCharge = new AtomicBoolean();
        final AtomicBoolean completionObserved = new AtomicBoolean();
        final AtomicBoolean taskStarted = new AtomicBoolean();
        // Install the release observer before handing the task to the
        // executor. A custom/inline executor may run the task and then
        // throw while returning from execute(); if the observer were added
        // afterwards, a later delegate completion could never release the
        // already accepted physical reservation.
        final PublishCall call = withRelease(reservation, outcome, retainPhysicalCharge);
        try {
            executor.execute(() -> {
                // An Executor is allowed to run the task inline. Record the
                // hand-off before invoking the delegate so an Error escaping
                // from that accepted task cannot be mistaken for executor
                // rejection and release an operation whose ownership is
                // already unknown.
                taskStarted.set(true);
                invokeDelegate(
                        request,
                        preflight,
                        outcome,
                        reservation,
                        retainPhysicalCharge,
                        completionObserved,
                        delegateCall);
            });
        } catch (RuntimeException exception) {
            if (!taskStarted.get()) {
                // The task was rejected before delegate invocation, so no
                // target-side ownership could have been acquired.
                outcome.complete(completedUnknownValue());
                reservation.release();
                return PublishCall.completed(completedUnknownValue());
            } else {
                // An inline/custom executor may throw after it has already
                // accepted the task. Preserve the same conservative fence
                // as an unobserved delegate operation instead of leaking an
                // active charge behind a completed logical UNKNOWN.
                retainPhysicalCharge.set(true);
                reservation.markZombie();
                outcome.complete(completedUnknownValue());
            }
            return call;
        } catch (Error fatalFailure) {
            // If the executor rejected the task before delegate invocation,
            // no Producer ownership can have been acquired through that path,
            // so release the pre-ownership reservation before allowing the
            // fatal failure to reach the caller/supervisor. If the executor
            // ran the task, retain the charge only when no delegate
            // completion has been observed; a successful completion may
            // already have released it through the observer installed above.
            if (!taskStarted.get()) {
                reservation.release();
            } else if (!completionObserved.get()) {
                // The executor accepted and ran the task but failed after
                // the delegate hand-off. Treat that boundary as an
                // unobserved physical operation; invokeDelegate has already
                // registered the delegate completion callback when a stage
                // exists, so that callback can release the retained charge.
                retainPhysicalCharge.set(true);
                reservation.markZombie();
                outcome.complete(completedUnknownValue());
            }
            throw fatalFailure;
        }
        return call;
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final DestinationPublishRequest request) {
        return submit(request).outcome();
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(
            final DestinationPublishRequest request,
            final SourcePosition sourcePosition,
            final byte[] preparedPublishHash) {
        return submit(request, sourcePosition, preparedPublishHash, ignored -> null)
                .outcome();
    }

    @Override
    public void close() {
        closeGuard.close(() -> {
            Throwable failure = null;
            try {
                delegate.close();
            } catch (RuntimeException | Error exception) {
                failure = appendCloseFailure(failure, exception);
            }
            if (ownedExecutor != null) {
                try {
                    ownedExecutor.shutdown();
                } catch (RuntimeException | Error exception) {
                    failure = appendCloseFailure(failure, exception);
                }
            }
            if (failure != null) {
                throwUnchecked(failure);
            }
        });
    }

    private static Throwable appendCloseFailure(final Throwable first, final Throwable failure) {
        if (first == null) {
            return failure;
        }
        if (failure != first) {
            first.addSuppressed(failure);
        }
        return first;
    }

    private static void throwUnchecked(final Throwable failure) {
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        if (failure instanceof Error errorFailure) {
            throw errorFailure;
        }
        throw new IllegalStateException("unexpected checked teardown failure", failure);
    }

    public static long requestPhysicalBytes(final DestinationPublishRequest request) {
        Objects.requireNonNull(request, "request");
        return Math.addExact(request.payload().length, request.adapterMetadata().length);
    }

    private static DestinationPublishResult completedUnknownValue() {
        return DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null);
    }

    private void invokeDelegate(
            final DestinationPublishRequest request,
            final PublishPreflight preflight,
            final CompletableFuture<DestinationPublishResult> outcome,
            final DestinationPhysicalAdmission.Reservation reservation,
            final AtomicBoolean retainPhysicalCharge,
            final AtomicBoolean completionObserved,
            final DelegateCall delegateCall) {
        final DelegateInvocation invocation;
        try {
            invocation = closeGuard.invokeIfOpen(
                    () -> {
                        final DestinationPublishResult preflightResult = preflight.check(request);
                        if (preflightResult != null) {
                            return new DelegateInvocation(CompletableFuture.completedFuture(preflightResult), false);
                        }
                        try {
                            return new DelegateInvocation(delegateCall.publish(request), false);
                        } catch (RuntimeException exception) {
                            // A synchronous transport exception does not
                            // prove that the request stopped before
                            // Producer/channel ownership. Preserve the same
                            // unobserved marker used by the pinned adapters
                            // so the physical charge is retained until
                            // certified completion or teardown.
                            return new DelegateInvocation(UnobservedDestinationPublishStage.unknown(), false);
                        }
                    },
                    () -> new DelegateInvocation(null, true));
        } catch (Error fatalFailure) {
            // An asynchronous JVM/native failure must not strand the logical
            // caller behind an incomplete PublishCall. It is still not a
            // proof of non-persistence, so retain the physical charge and
            // expose UNKNOWN before rethrowing the fatal failure to the
            // executor/process supervisor.
            retainPhysicalCharge.set(true);
            reservation.markZombie();
            outcome.complete(completedUnknownValue());
            throw fatalFailure;
        }
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
                // Publish the physical completion before completing the
                // logical outcome. Callers that observe a completed outcome
                // must also observe that the lane admission has drained;
                // otherwise an otherwise healthy lane can transiently look
                // blocked behind a stale active-request charge. The release
                // is idempotent, so this also closes the race where callback
                // registration threw after installing the callback and the
                // outcome observer is still armed.
                reservation.release();
                outcome.complete(error == null && value != null ? value : completedUnknownValue());
            });
        } catch (RuntimeException registrationFailure) {
            // A custom CompletionStage may reject both callback-registration
            // paths after the delegate has already acquired Producer
            // ownership. UNKNOWN is useful for the logical caller, but it
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
        } catch (Error registrationFailure) {
            // A fatal callback-registration failure is the same absence of
            // physical completion evidence as a runtime registration failure.
            // Complete the logical branch and retain the charge before the
            // fatal error escapes the executor.
            if (!completionObserved.get()) {
                retainPhysicalCharge.set(true);
                reservation.markZombie();
                outcome.complete(completedUnknownValue());
            } else {
                reservation.release();
            }
            throw registrationFailure;
        }
    }

    private static void registerCompletion(
            final CompletionStage<DestinationPublishResult> raw,
            final BiConsumer<? super DestinationPublishResult, ? super Throwable> callback) {
        try {
            final CompletionStage<DestinationPublishResult> registered = raw.whenComplete(callback);
            if (registered == null) {
                throw new IllegalStateException("CompletionStage.whenComplete returned null");
            }
            return;
        } catch (RuntimeException firstFailure) {
            try {
                // Some adapters expose a custom CompletionStage wrapper but
                // still provide a standard CompletableFuture view.
                final CompletableFuture<DestinationPublishResult> future = raw.toCompletableFuture();
                if (future == null) {
                    throw new IllegalStateException("CompletionStage returned a null CompletableFuture view");
                }
                final CompletableFuture<DestinationPublishResult> registered = future.whenComplete(callback);
                if (registered == null) {
                    throw new IllegalStateException("CompletableFuture.whenComplete returned null");
                }
                return;
            } catch (RuntimeException fallbackFailure) {
                firstFailure.addSuppressed(fallbackFailure);
                throw firstFailure;
            }
        }
    }

    private static PublishCall withRelease(
            final DestinationPhysicalAdmission.Reservation reservation,
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

    private record DelegateInvocation(CompletionStage<DestinationPublishResult> stage, boolean closed) {}

    @FunctionalInterface
    private interface DelegateCall {
        CompletionStage<DestinationPublishResult> publish(DestinationPublishRequest request);
    }

    /** Runs immediately before the target delegate; non-null means do not call the delegate. */
    @FunctionalInterface
    public interface PublishPreflight {
        DestinationPublishResult check(DestinationPublishRequest request);
    }

    public static final class PublishCall {
        private final CompletionStage<DestinationPublishResult> outcome;
        private final DestinationPhysicalAdmission.Reservation reservation;

        private PublishCall(
                final CompletionStage<DestinationPublishResult> outcome,
                final DestinationPhysicalAdmission.Reservation reservation) {
            this.outcome = Objects.requireNonNull(outcome, "outcome");
            this.reservation = reservation;
        }

        private static PublishCall from(
                final DestinationPhysicalAdmission.Reservation reservation,
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
