package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ShardStoreConfig;
import io.nereusstream.delay.store.SharedRocksDbResources;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * In-process conformance service. It models the durable Command Topic boundary
 * explicitly and is intentionally not presented as a Kafka/Pulsar adapter.
 */
public final class EmbeddedDelayService implements DelayClient {
    private final ShardId shardId;
    private final Clock clock;
    private final SharedRocksDbResources resources;
    private final ShardStore store;
    private final DelayShard shard;
    private final Deque<QueuedRecord> pending = new ArrayDeque<>();
    private long nextOffset;
    private boolean closed;

    public EmbeddedDelayService(final ShardStoreConfig storeConfig, final ShardId shardId) {
        this(storeConfig, shardId, Clock.systemUTC());
    }

    public EmbeddedDelayService(final ShardStoreConfig storeConfig, final ShardId shardId, final Clock clock) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.clock = Objects.requireNonNull(clock, "clock");
        resources = new SharedRocksDbResources(storeConfig);
        store = ShardStore.open(storeConfig, shardId, resources);
        shard = new DelayShard(store, DelayShardConfig.defaults());
    }

    @Override
    public PreparedCommand prepareSchedule(final ScheduleIntent intent, final long retryUntilEpochMs) {
        ensureOpen();
        return PreparedCommand.schedule(shardId, intent, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareCancel(final DelayMessageId messageId, final int expectedGeneration,
                                         final long retryUntilEpochMs) {
        ensureOpen();
        return PreparedCommand.cancel(shardId, messageId, expectedGeneration, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareReschedule(final DelayMessageId messageId, final int expectedGeneration,
                                             final long deliverAtEpochMs, final long expireAtEpochMs,
                                             final long retryUntilEpochMs) {
        ensureOpen();
        return PreparedCommand.reschedule(shardId, messageId, expectedGeneration, deliverAtEpochMs,
                expireAtEpochMs, retryUntilEpochMs);
    }

    @Override
    public synchronized CompletionStage<EnqueueOutcome> enqueue(final PreparedCommand command) {
        ensureOpen();
        if (!shardId.equals(command.shardId())) {
            return CompletableFuture.completedFuture(EnqueueOutcome.definitelyNotQueued(command, 0x110a));
        }
        final long now = clock.millis();
        final SourcePosition position = new KafkaSourcePosition(shardId, "embedded", UUID.nameUUIDFromBytes(
                Bytes.utf8("embedded-command-topic")), nextOffset++, null, now);
        final CommandQueuedReceipt receipt = new CommandQueuedReceipt(command.commandId(), command.delayMessageId(),
                shardId, position);
        pending.addLast(new QueuedRecord(command, position));
        return CompletableFuture.completedFuture(EnqueueOutcome.queued(command, receipt));
    }

    /** Applies all queued records in Source Position order. */
    public synchronized void drain() {
        ensureOpen();
        while (!pending.isEmpty()) {
            final QueuedRecord record = pending.removeFirst();
            shard.apply(record.command(), record.position());
        }
    }

    @Override
    public synchronized CompletionStage<CommandResult> awaitApplied(final CommandQueuedReceipt receipt) {
        ensureOpen();
        drain();
        return CompletableFuture.completedFuture(shard.getCommandResult(receipt.commandId()));
    }

    public synchronized DelayShard shard() {
        return shard;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            store.close();
            resources.close();
        }
    }

    private synchronized void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("client is closed");
        }
    }

    private record QueuedRecord(PreparedCommand command, SourcePosition position) {
    }
}

