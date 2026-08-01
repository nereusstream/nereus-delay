package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.store.ShardStoreConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EmbeddedDelayServiceTest {
    @TempDir
    Path tempDir;

    @Test
    void queuedReceiptIsNotAppliedReceipt() {
        final long now = 1_000;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 0);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC);
        try (EmbeddedDelayService service = new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir), shard, clock)) {
            final var command = service.prepareSchedule(new ScheduleIntent(
                    DestinationLaneId.derive(Bytes.utf8("embedded-lane")), 2_000, 5_000,
                    OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final EnqueueOutcome outcome = service.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, outcome.status());
            assertNull(service.shard().getCommandResult(command.commandId()));

            final CommandResultView result = new CommandResultView(
                    service.awaitApplied(outcome.receipt()).toCompletableFuture().join().stableCode());
            assertEquals(StableCode.SCHEDULED, result.code());
        }
    }

    @Test
    void reopenedEmbeddedServiceContinuesSourceOffsets() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 1);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("reopen"));
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("reopen-lane")), 2_000, 5_000,
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"));
        try (EmbeddedDelayService first = new EmbeddedDelayService(config, shard, Clock.fixed(
                Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = first.prepareSchedule(intent, 10_000);
            final EnqueueOutcome outcome = first.enqueue(command).toCompletableFuture().join();
            first.awaitApplied(outcome.receipt()).toCompletableFuture().join();
        }
        try (EmbeddedDelayService second = new EmbeddedDelayService(config, shard, Clock.fixed(
                Instant.ofEpochMilli(1_001), ZoneOffset.UTC))) {
            final PreparedCommand command = second.prepareCancel(
                    io.nereusstream.delay.protocol.DelayMessageId.random(shard), -1, 10_000);
            final EnqueueOutcome outcome = second.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, outcome.status());
            assertEquals(1, ((io.nereusstream.delay.protocol.KafkaSourcePosition) outcome.receipt()
                    .sourcePosition()).offset());
        }
    }

    private record CommandResultView(StableCode code) {
    }
}
