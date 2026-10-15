package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetNativePublicationBinding;
import com.nereusstream.delay.protocol.TargetOrdinaryPublicationBinding;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Bounded exact-channel dispatch for one shared Target driver, including overlapping credential generations. */
public final class TargetDestinationMux implements DestinationPublishAdapter {
    private record Entry(TargetChannelIdentity channel, DestinationPublishAdapter adapter) {}
    private final int maximumChannels;
    private final Map<String, Entry> channels = new HashMap<>();
    private boolean closing;

    public TargetDestinationMux(int maximumChannels) {
        if (maximumChannels <= 0 || maximumChannels == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Target dispatch requires a finite channel bound");
        }
        this.maximumChannels = maximumChannels;
    }

    public synchronized void register(TargetChannelIdentity channel, DestinationPublishAdapter adapter) {
        Objects.requireNonNull(channel, "actual configured channel");
        if (closing || channels.size() >= maximumChannels
                || channels.putIfAbsent(Bytes.hex(channel.digest()), new Entry(channel,
                        Objects.requireNonNull(adapter, "owned SDK endpoint"))) != null) {
            throw new IllegalStateException("Target endpoint is stopping, duplicated or full");
        }
    }

    /** Caller closes the original owner and proves its physical window empty before dropping this dispatch entry. */
    public synchronized void unregister(TargetChannelIdentity channel, DestinationPublishAdapter expected) {
        final var entry = channels.get(Bytes.hex(channel.digest()));
        if (entry == null || entry.adapter() != expected) {
            throw new IllegalStateException("Target endpoint ownership changed");
        }
        entry.channel().requireExactFrozenIdentity(channel);
        channels.remove(Bytes.hex(channel.digest()));
    }

    private synchronized DestinationPublishAdapter endpoint(TargetChannelIdentity channel) {
        if (closing) { return null; }
        final var entry = channels.get(Bytes.hex(channel.digest()));
        if (entry == null) { return null; }
        entry.channel().requireExactFrozenIdentity(channel);
        return entry.adapter();
    }

    public synchronized void requireEndpoint(TargetChannelIdentity channel, DestinationPublishAdapter original) {
        if (endpoint(channel) != original) throw new IllegalStateException("Target dispatch lacks its original channel SDK owner");
    }

    public synchronized boolean empty() { return channels.isEmpty(); }

    private static CompletionStage<DestinationPublishResult> unavailable() {
        return CompletableFuture.completedFuture(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    @Override public CompletionStage<DestinationPublishResult> publish(DestinationPublishRequest request) {
        return unavailable();
    }

    @Override public CompletionStage<DestinationPublishResult> publishTargetOrdinary(TargetOrdinaryPublishRequest request,
            BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight gate) {
        final var adapter = endpoint(request.publication().channel());
        return adapter == null ? unavailable() : adapter.publishTargetOrdinary(request, gate);
    }

    @Override public CompletionStage<DestinationPublishResult> publishTargetPreparedRecord(TargetOrdinaryPublicationBinding publication,
            PulsarPreparedRecord record, ArtifactGenerationSet artifacts,
            BoundedDestinationPublishAdapter.TargetPreparedPublishPreflight gate) {
        PulsarPreparedRecordFactory.requireTargetBinding(publication, record, artifacts);
        final var adapter = endpoint(publication.channel());
        return adapter == null ? unavailable() : adapter.publishTargetPreparedRecord(publication, record, artifacts, gate);
    }

    @Override public CompletionStage<DestinationPublishResult> publishTargetPreparedRecord(TargetNativePublicationBinding publication,
            PulsarPreparedRecord record, ArtifactGenerationSet artifacts,
            BoundedDestinationPublishAdapter.TargetNativePreparedPublishPreflight gate) {
        PulsarPreparedRecordFactory.requireTargetBinding(publication, record, artifacts);
        final var adapter = endpoint(publication.channel());
        return adapter == null ? unavailable() : adapter.publishTargetPreparedRecord(publication, record, artifacts, gate);
    }

    /** Fences routing only; registered per-generation resource owners perform tracked, retryable native close. */
    @Override public synchronized void close() { closing = true; }
}
