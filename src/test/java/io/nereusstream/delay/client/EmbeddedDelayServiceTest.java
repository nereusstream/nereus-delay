package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.runtime.ApplyStatus;
import io.nereusstream.delay.runtime.GenerationAggregateState;
import io.nereusstream.delay.runtime.MessageQuerySnapshot;
import io.nereusstream.delay.runtime.MessageStatus;
import io.nereusstream.delay.runtime.PayloadAvailability;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.store.ShardStoreConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void boundedLocalProjectorRequiresSafeBindingAndPreservesRuntimeStates() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final KafkaSourcePosition source = new KafkaSourcePosition(shard, "projector", UUID.randomUUID(), 4, 1, 1_000);
        final PublicDestinationBindingViewV1 binding = publicBinding();
        final CommandResult rejected = new CommandResult(ApplyStatus.REJECTED, StableCode.INVALID_COMMAND, -1, 0,
                null, source.canonicalBytes());
        assertEquals(io.nereusstream.delay.protocol.CommandQueryResult.REJECTED,
                BoundedLocalQueryProjector.command(rejected, 5_000, null).resultKind());

        final CommandResult applied = new CommandResult(ApplyStatus.APPLIED, StableCode.SCHEDULED, 0, 1,
                MessageStatus.SCHEDULED, source.canonicalBytes());
        assertEquals(io.nereusstream.delay.protocol.CommandQueryResult.APPLIED,
                BoundedLocalQueryProjector.command(applied, 5_000, binding).resultKind());

        final io.nereusstream.delay.protocol.DelayMessageId messageId =
                io.nereusstream.delay.protocol.DelayMessageId.random(shard);
        final MessageQuerySnapshot active = new MessageQuerySnapshot(messageId, 0, 1,
                GenerationAggregateState.SCHEDULED, 2_000, 5_000, PayloadAvailability.INLINE_RETAINED, false, null);
        final MessageQuerySnapshot terminal = new MessageQuerySnapshot(messageId, 0, 2,
                GenerationAggregateState.PUBLISHED, 2_000, 5_000, PayloadAvailability.INLINE_RETAINED, false,
                StableCode.OK);
        assertEquals(io.nereusstream.delay.protocol.MessageQueryResult.ACTIVE,
                BoundedLocalQueryProjector.message(active, binding, DlqExportStateV1.NOT_CONFIGURED, null).resultKind());
        assertEquals(io.nereusstream.delay.protocol.MessageQueryResult.TERMINAL,
                BoundedLocalQueryProjector.message(terminal, binding, DlqExportStateV1.NOT_CONFIGURED, null)
                        .resultKind());
        assertThrows(IllegalArgumentException.class,
                () -> BoundedLocalQueryProjector.command(rejected, 5_000, binding));
    }

    private record CommandResultView(StableCode code) {
    }

    private static PublicDestinationBindingViewV1 publicBinding() {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination"), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic")), ProfileKindV1.DESTINATION);
        final ProfileRefV1 capability = new ProfileRefV1(Bytes.utf8("capability"), 1,
                Bytes.sha256(Bytes.utf8("capability-semantic")), ProfileKindV1.DELIVERY_CAPABILITY);
        return new PublicDestinationBindingViewV1(destination, capability, AdapterKindV1.KAFKA,
                Bytes.utf8("safe-destination"), 1, OrderingMode.BEST_EFFORT);
    }
}
