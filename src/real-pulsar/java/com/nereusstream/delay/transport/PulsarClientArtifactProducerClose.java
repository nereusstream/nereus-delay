package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetSdkCleanupFailure;
import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/** Owns the first actual P1 producer close operation; bounded waits never modify its completion. */
final class PulsarClientArtifactProducerClose {
    private final Producer<?> producer;
    private final PulsarClient client;
    private final TargetDestinationSdkBudget.Client budget;
    private final long timeoutMs;
    private CompletableFuture<Void> nativeClose;
    private TargetSdkCleanupFailure retainedFailure;
    private CompletableFuture<Void> backendClose;
    private OwnedCheckpointIo<Boolean> backendShutdown;
    private boolean backendStopped;

    PulsarClientArtifactProducerClose(Producer<?> producer, Duration timeout) {
        this(null, producer, timeout);
    }

    PulsarClientArtifactProducerClose(PulsarClient client, Producer<?> producer, Duration timeout) {
        this(client, producer, timeout, null);
    }

    PulsarClientArtifactProducerClose(PulsarClient client, Producer<?> producer, Duration timeout,
            TargetDestinationSdkBudget.Client budget) {
        this.client = client;
        this.budget = budget;
        this.producer = Objects.requireNonNull(producer, "actual producer");
        timeoutMs = Math.min(60_000, Objects.requireNonNull(timeout, "close timeout").toMillis());
        if (timeoutMs <= 0) { throw new IllegalArgumentException("P1 close requires a finite positive wait"); }
    }

    private synchronized CompletableFuture<Void> firstClose() {
        if (nativeClose == null) {
            try {
                nativeClose = Objects.requireNonNull(producer.closeAsync(), "actual producer close completion");
            } catch (RuntimeException | Error failure) {
                nativeClose = CompletableFuture.failedFuture(failure);
            }
        }
        return nativeClose.copy();
    }

    void close() {
        synchronized (this) {
            if (backendStopped) {
                if (budget != null) { budget.closed(); }
                return;
            }
        }
        try {
            firstClose().get(timeoutMs, TimeUnit.MILLISECONDS);
            if (budget != null) { budget.closed(); }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw retain(failure);
        } catch (ExecutionException failure) {
            throw retain(failure.getCause());
        } catch (TimeoutException failure) {
            throw retain(failure);
        }
    }

    private synchronized TargetSdkCleanupFailure retain(Throwable failure) {
        if (retainedFailure == null) {
            retainedFailure = new PulsarClientArtifactProducerCleanupFailure(failure, this);
        }
        return retainedFailure;
    }

    static void afterFailedCreation(PulsarClient client, Producer<?> producer, Duration timeout, Throwable original) {
        afterFailedCreation(client, producer, timeout, original, null);
    }

    static void afterFailedCreation(PulsarClient client, Producer<?> producer, Duration timeout, Throwable original,
            TargetDestinationSdkBudget.Client budget) {
        final var owned = new PulsarClientArtifactProducerClose(client, producer, timeout, budget);
        try {
            owned.close();
        } catch (TargetSdkCleanupFailure held) {
            final var failure = new PulsarClientArtifactProducerCleanupFailure(original, owned);
            failure.addSuppressed(held);
            throw failure;
        }
    }

    synchronized CompletableFuture<Void> closeClientBackendAfterFailedClose() {
        if (client == null || retainedFailure == null || nativeClose == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("P1 cleanup lacks its original client owner"));
        }
        if (backendClose == null) {
            try {
                backendClose = Objects.requireNonNull(client.closeAsync(), "original client close completion");
            } catch (RuntimeException | Error failure) {
                backendClose = CompletableFuture.failedFuture(failure);
            }
        }
        return backendClose.thenRun(this::confirmBackendStopped);
    }

    synchronized CompletableFuture<Void> shutdownClientBackendAfterFailedClose(ThreadPoolExecutor ownerIo) {
        if (client == null || backendClose == null || !backendClose.isDone() || !backendClose.isCompletedExceptionally()) {
            return CompletableFuture.failedFuture(new IllegalStateException("P1 backend has no completed failed close to retry"));
        }
        if (backendShutdown == null || backendShutdown.ioExited() && backendShutdown.completedValue().isEmpty()) {
            try {
                backendShutdown = OwnedCheckpointIo.submit(ignored -> {
                    try { client.shutdown(); }
                    catch (PulsarClientException failure) { throw new IllegalStateException("Original P1 backend shutdown failed", failure); }
                    confirmBackendStopped();
                    return true;
                }, () -> {}, ownerIo);
            } catch (RuntimeException | Error rejected) {
                return CompletableFuture.failedFuture(rejected);
            }
        }
        return backendShutdown.receipt().thenApply(ignored -> null);
    }

    private synchronized void confirmBackendStopped() {
        if (nativeClose == null || !nativeClose.isDone()) {
            throw new IllegalStateException("Original P1 producer cleanup still has pending work after backend shutdown");
        }
        backendStopped = true;
    }
}
