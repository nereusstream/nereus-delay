package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.TargetDestinationMux;
import com.nereusstream.delay.adapter.TargetOrdinaryPublishRequest;
import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetNativePublicationBinding;
import com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Owns one actual BASELINE or Pulsar managed SDK generation in the shared Worker dispatch/resource graph. */
public final class TargetDestinationChannelRuntime implements DestinationPublishAdapter,
        TargetWorkerShardRuntime.PublicationResources {
    @FunctionalInterface
    public interface Open {
        DestinationPublishAdapter create(Runnable guard, TargetDestinationSdkBudget.Client reserved);
    }

    private final TargetChannelIdentity channel;
    private final DestinationPhysicalAdmission physical;
    private final TargetDestinationMux mux;
    private final ThreadPoolExecutor io;
    private OwnedCheckpointIo<DestinationPublishAdapter> opening;
    private volatile boolean initializing = true;
    private boolean registeredOwner;
    private OwnedCheckpointIo<Boolean> cleanup;
    private DestinationPublishAdapter adapter;
    private boolean ready, stopping, closed, adapterClosed, physicalRegistered, dispatchRegistered;

    /** Actual library factory runs on the existing finite I/O owner, after the Source channel and Target check. */
    public TargetDestinationChannelRuntime(TargetWorkerShardRuntime worker, TargetChannelIdentity channel,
            CanonicalTargetPartition target, TargetDestinationSdkBudget sdkBudget,
            com.nereusstream.delay.protocol.CapacityVector clientCost, DestinationPhysicalAdmission physical,
            TargetDestinationMux mux, WorkClassExecutionRegistry classes, SharedRocksDbResources resources,
            ThreadPoolExecutor io, Supplier<BoundedReadBudget> reads, LongSupplier clock,
            long maximumPhysicalBytes, Open factory) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.physical = Objects.requireNonNull(physical, "physical pool");
        this.mux = Objects.requireNonNull(mux, "Worker dispatch");
        this.io = Objects.requireNonNull(io, "bounded I/O pool");
        if (channel.context().kind() != ChannelKind.BASELINE_PRODUCER
                && channel.context().kind() != ChannelKind.PULSAR_DEDUP_PRODUCER) {
            throw new IllegalArgumentException("baseline/managed opening cannot replace K2 fencing/anchor startup");
        }
        worker.requireFleetComposition(classes, resources);
        sdkBudget.requireWorker(classes, resources);
        final var reserved = sdkBudget.reserve(Bytes.hex(channel.digest()) + "/producer", clientCost);
        try {
            physical.registerTargetChannel(new DestinationPhysicalAdmission.TargetChannelSpec(channel, target,
                    1, maximumPhysicalBytes, 1, maximumPhysicalBytes));
            physicalRegistered = true;
            worker.registerPublicationResources(Bytes.hex(channel.digest()), this, sdkBudget, clock);
            registeredOwner = true;
            final Runnable openingGuard = () -> {
                final var current = worker.requirePublicationChannel(channel, true, reads.get(), clock);
                if (!Arrays.equals(current.canonicalBytes(), target.canonicalBytes())) {
                    throw new IllegalStateException("SDK opening changes the actual Source Target");
                }
            };
            opening = OwnedCheckpointIo.submitResource(guard ->
                    Objects.requireNonNull(factory.create(guard, reserved), "actual native adapter"), openingGuard, value -> {
                try { value.close(); }
                catch (RuntimeException | Error failed) { throw new TargetSdkCleanupFailure(failed, value::close); }
            }, io);
            opening.receipt().whenComplete((value, failure) -> {
                if (failure != null) {
                    try { reserved.releaseUnstarted(); }
                    catch (IllegalStateException started) { /* original native cleanup holds this generation */ }
                }
            });
        } catch (RuntimeException | Error failure) {
            // No ownership handoff occurred if queue registration failed. A started factory retains its own handle.
            try { reserved.releaseUnstarted(); }
            catch (IllegalStateException started) { failure.addSuppressed(started); }
            if (physicalRegistered && !registeredOwner) {
                try { physical.unregisterTargetChannel(channel); physicalRegistered = false; }
                catch (RuntimeException held) { failure.addSuppressed(held); }
            }
            throw failure;
        } finally { initializing = false; }
    }

    /** No waiting here: the original opening must have exited, including its native failed-close obligation. */
    public synchronized boolean finishStartup() {
        if (stopping) { throw new IllegalStateException("Target channel is stopping"); }
        if (ready) { return true; }
        if (initializing || opening == null) { return false; }
        if (!opening.ioExited()) { return false; }
        adapter = opening.completedValue().orElseThrow(() -> new IllegalStateException("SDK opening failed",
                CheckpointInputFuture.ownedFailure(opening.receipt())));
        mux.register(channel, this);
        dispatchRegistered = true;
        physical.openTargetReady(channel);
        ready = true;
        return true;
    }

    @Override public synchronized boolean readyForPublishing() { return ready && !stopping; }
    @Override public synchronized boolean closed() { return closed; }
    @Override public synchronized TargetOwnedBackendGroup ownedBackendGroup() {
        return adapter == null ? null : adapter.ownedBackendGroup();
    }
    @Override public TargetChannelIdentity channelIdentity() { return channel; }

    private synchronized DestinationPublishAdapter current() { return ready && !stopping ? adapter : null; }
    private static CompletionStage<DestinationPublishResult> unavailable() {
        return CompletableFuture.completedFuture(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    @Override public CompletionStage<DestinationPublishResult> publish(DestinationPublishRequest request) { return unavailable(); }
    @Override public CompletionStage<DestinationPublishResult> publishTargetOrdinary(TargetOrdinaryPublishRequest request,
            BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate) {
        channel.requireExactFrozenIdentity(request.publication().channel());
        final var actual = current();
        return actual == null ? unavailable() : actual.publishTargetOrdinary(request, gate);
    }

    @Override public CompletionStage<DestinationPublishResult> publishTargetPreparedRecord(TargetOrdinaryPublicationBinding publication,
            PulsarPreparedRecord record, ArtifactGenerationSet artifacts,
            BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight gate) {
        channel.requireExactFrozenIdentity(publication.channel());
        final var actual = current();
        return actual == null ? unavailable() : actual.publishTargetPreparedRecord(publication, record, artifacts, gate);
    }

    @Override public CompletionStage<DestinationPublishResult> publishTargetPreparedRecord(TargetNativePublicationBinding publication,
            PulsarPreparedRecord record, ArtifactGenerationSet artifacts,
            BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight gate) {
        channel.requireExactFrozenIdentity(publication.channel());
        final var actual = current();
        return actual == null ? unavailable() : actual.publishTargetPreparedRecord(publication, record, artifacts, gate);
    }

    @Override public synchronized boolean settleClose() {
        stopping = true; ready = false;
        if (physicalRegistered) { physical.closeTargetReady(channel); }
        if (initializing) { return false; }
        if (opening != null) {
            opening.requestStop(); opening.retryCleanup();
            if (!opening.ioExited()) { return false; }
            if (adapter == null) { adapter = opening.completedValue().orElse(null); }
        }
        if (adapter != null && !adapterClosed) {
            if (cleanup != null) { cleanup.retryCleanup(); }
            if (cleanup == null || cleanup.ioExited()) {
                if (cleanup != null && cleanup.completedValue().orElse(false)) { adapterClosed = true; }
                else {
                    cleanup = OwnedCheckpointIo.submit(ignored -> { adapter.close(); return true; }, () -> {}, io);
                    return false;
                }
            }
            if (!cleanup.ioExited()) { return false; }
            adapterClosed = cleanup.completedValue().orElse(false);
            if (!adapterClosed) { return false; }
        }
        if (physicalRegistered) { physical.unregisterTargetChannel(channel); physicalRegistered = false; }
        if (dispatchRegistered) { mux.unregister(channel, this); dispatchRegistered = false; }
        closed = true;
        return true;
    }

    public synchronized Optional<Throwable> openingFailure() {
        return Optional.ofNullable(opening == null ? null : CheckpointInputFuture.ownedFailure(opening.receipt()));
    }

    @Override public void close() {
        if (!settleClose()) { throw new IllegalStateException("Target native cleanup remains owned"); }
    }
}
