package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.CommandAppliedReceiptV1;
import io.nereusstream.delay.protocol.CommandQueryResult;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationQueryResultV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationStateV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EnqueueOutcomeKindV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.MessageQueryResult;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
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
import java.util.List;

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
    void embeddedSourceOffsetExhaustionFailsBeforeMutatingOffset() throws ReflectiveOperationException {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("offset-exhaustion")), shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = service.prepareSchedule(new ScheduleIntent(
                    DestinationLaneId.derive(Bytes.utf8("offset-exhaustion-lane")), 2_000, 5_000,
                    OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final var nextOffset = EmbeddedDelayService.class.getDeclaredField("nextOffset");
            nextOffset.setAccessible(true);
            nextOffset.setLong(service, Long.MAX_VALUE);

            assertThrows(IllegalStateException.class, () -> service.enqueue(command));
            assertEquals(Long.MAX_VALUE, nextOffset.getLong(service));
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
        assertThrows(IllegalArgumentException.class,
                () -> BoundedLocalQueryProjector.message(terminal, binding, DlqExportStateV1.PUBLISHED, null));
        assertThrows(IllegalArgumentException.class,
                () -> new MessageQuerySnapshot(messageId, 0, 2, GenerationAggregateState.PUBLISHED,
                        2_000, 5_000, PayloadAvailability.INLINE_RETAINED, false, StableCode.OK,
                        DlqExportStateV1.PUBLISHED));
    }

    @Test
    void embeddedQueryUsesQueuedReceiptAsSourceBarrier() {
        final long now = 1_000;
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC);
        try (EmbeddedDelayService service = new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir.resolve("query")),
                shard, clock)) {
            final PreparedCommand command = service.prepareSchedule(new ScheduleIntent(
                    DestinationLaneId.derive(Bytes.utf8("query-lane")), 2_000, 5_000,
                    OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final EnqueueOutcome outcome = service.enqueue(command).toCompletableFuture().join();
            final var queued = service.queuedReceiptV1(outcome, 10_000,
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("attempt")), 16));

            assertEquals(CommandQueryResult.PENDING,
                    service.queryCommand(queued, now, 10_000, publicBinding()).resultKind());
            service.drain();
            assertEquals(CommandQueryResult.APPLIED,
                    service.queryCommand(queued, now, 10_000, publicBinding()).resultKind());
            final CommandAppliedReceiptV1 applied = service.appliedReceiptV1(queued, 10_000, publicBinding());
            assertEquals(io.nereusstream.delay.protocol.ReceiptKind.COMMAND_APPLIED,
                    io.nereusstream.delay.protocol.ReceiptFrame.decode(applied.frame()).kind());
            assertEquals(applied, CommandAppliedReceiptV1.decodeFrame(applied.frame()));
            assertEquals(CommandQueryResult.RESULT_EXPIRED,
                    service.queryCommand(queued, 3_000, 2_000, publicBinding()).resultKind());
            assertEquals(CommandQueryResult.RESULT_EVIDENCE_EXPIRED,
                    service.queryCommand(queued, 10_001, 10_000, publicBinding()).resultKind());

            assertEquals(MessageQueryResult.ACTIVE,
                    service.queryMessage(command.delayMessageId(), publicBinding(),
                            DlqExportStateV1.NOT_CONFIGURED, null,
                            io.nereusstream.delay.protocol.FirstScheduleEligibilityV1.NOT_PROVEN).resultKind());
            assertEquals(MessageQueryResult.UNKNOWN,
                    service.queryMessage(DelayMessageId.random(shard), publicBinding(),
                            DlqExportStateV1.NOT_CONFIGURED, null,
                            io.nereusstream.delay.protocol.FirstScheduleEligibilityV1.NOT_PROVEN).resultKind());
        }
    }

    @Test
    void embeddedQueryRejectsSameOffsetReceiptWithConflictingCanonicalMetadata() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 18);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("query-position-fence")), shard, clock)) {
            final PreparedCommand command = service.prepareSchedule(new ScheduleIntent(
                    DestinationLaneId.derive(Bytes.utf8("query-position-fence-lane")), 2_000, 5_000,
                    OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final EnqueueOutcome outcome = service.enqueue(command).toCompletableFuture().join();
            final byte[] attemptId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("query-position-attempt")), 16);
            final CommandQueuedReceiptV1 queued = service.queuedReceiptV1(outcome, 10_000, attemptId);
            final KafkaSourcePosition actual = (KafkaSourcePosition) queued.sourcePosition();
            final KafkaSourcePosition conflicting = new KafkaSourcePosition(shard,
                    actual.authenticatedClusterId(), actual.nativeTopicUuid(), actual.offset(), 7,
                    Math.addExact(actual.brokerLogAppendTimeEpochMs(), 1));
            final CommandQueuedReceiptV1 forged = CommandQueuedReceiptV1.create(command, conflicting,
                    new CommandQueuedReceiptV1.KafkaQueuedAck(actual.authenticatedClusterId(), actual.nativeTopicUuid(),
                            shard.partition(), actual.offset(), 7, conflicting.brokerLogAppendTimeEpochMs(),
                            Bytes.sha256(Bytes.utf8("query-position-conflicting-ack"))),
                    10_000, attemptId);

            service.drain();
            assertEquals(io.nereusstream.delay.protocol.CommandQueryResult.INTEGRITY_ERROR,
                    service.queryCommand(forged, 1_000, 10_000, publicBinding()).resultKind());
            assertThrows(IllegalStateException.class,
                    () -> service.appliedReceiptV1(forged, 10_000, publicBinding()));
        }
    }

    @Test
    void embeddedQueuedReceiptRejectsNonQueuedOutcome() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        try (EmbeddedDelayService service = new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir.resolve("reject")),
                shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand otherShardCommand = PreparedCommand.schedule(
                    new ShardId(RouteIncarnation.random(), 0), new ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("other-lane")), 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final EnqueueOutcome outcome = service.enqueue(otherShardCommand).toCompletableFuture().join();
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, outcome.status());
            assertThrows(IllegalArgumentException.class,
                    () -> service.queuedReceiptV1(outcome, 10_000, new byte[16]));
        }
    }

    @Test
    void embeddedControlOperationEntryPointsPreserveReceiptBoundCas() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 6);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("control-operation"));
        final ControlOperationReceiptV1 receipt = controlReceipt();
        final CurrentControlOperationV1 initial = new CurrentControlOperationV1(receipt.operationId(),
                receipt.requestHash(), receipt.authenticatedScopeHash(), ControlOperationStateV1.PENDING, 1,
                List.of(), null);
        try (EmbeddedDelayService service = new EmbeddedDelayService(config, shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            assertEquals(ControlOperationQueryResultV1.CURRENT,
                    service.registerControlOperation(receipt, initial).resultKind());
            assertEquals(initial, service.queryControlOperation(receipt, 2_000).current());
            final CurrentControlOperationV1 next = new CurrentControlOperationV1(receipt.operationId(),
                    receipt.requestHash(), receipt.authenticatedScopeHash(), ControlOperationStateV1.IN_PROGRESS, 2,
                    List.of(), null);
            assertEquals(ControlOperationQueryResultV1.CURRENT,
                    service.advanceControlOperation(receipt, 1, next).resultKind());
            assertEquals(next, service.queryControlOperation(receipt, 2_000).current());
        }
    }

    @Test
    void embeddedIngressProjectsAllManagedOutcomeBranches() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        try (EmbeddedDelayService service = new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir.resolve("outcome")),
                shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand queuedCommand = service.prepareSchedule(new ScheduleIntent(
                    DestinationLaneId.derive(Bytes.utf8("outcome-queued")), 2_000, 5_000,
                    OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final EnqueueOutcome queued = service.enqueue(queuedCommand).toCompletableFuture().join();
            final byte[] attemptId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("outcome-attempt")), 16);
            final EnqueueOutcomeMessageV1 queuedWire = service.enqueueOutcomeV1(queued, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKindV1.QUEUED, queuedWire.kind());
            assertEquals(queuedWire, EnqueueOutcomeMessageV1.decode(queuedWire.canonicalBytes()));

            final PreparedCommand rejectedCommand = PreparedCommand.schedule(
                    new ShardId(RouteIncarnation.random(), 0), new ScheduleIntent(
                            DestinationLaneId.derive(Bytes.utf8("outcome-rejected")), 2_000, 5_000,
                            OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final EnqueueOutcome rejected = service.enqueue(rejectedCommand).toCompletableFuture().join();
            final EnqueueOutcomeMessageV1 definiteWire = service.enqueueOutcomeV1(rejected, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED, definiteWire.kind());
            assertEquals(definiteWire, EnqueueOutcomeMessageV1.decode(definiteWire.canonicalBytes()));

            final PreparedCommand uncertainCommand = service.prepareSchedule(new ScheduleIntent(
                    DestinationLaneId.derive(Bytes.utf8("outcome-uncertain")), 2_000, 5_000,
                    OrderingMode.BEST_EFFORT, Bytes.utf8("payload")), 10_000);
            final EnqueueOutcome uncertain = EnqueueOutcome.uncertain(uncertainCommand,
                    StableCode.ENQUEUE_RESULT_UNCERTAIN.wireValue());
            final EnqueueOutcomeMessageV1 uncertainWire = service.enqueueOutcomeV1(uncertain, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKindV1.ENQUEUE_UNCERTAIN, uncertainWire.kind());
            assertEquals(uncertainWire, EnqueueOutcomeMessageV1.decode(uncertainWire.canonicalBytes()));
        }
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

    private static ControlOperationReceiptV1 controlReceipt() {
        final TrustedUtcIntervalEvidence registered = new TrustedUtcIntervalEvidence(1_000, 1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("embedded-control-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("control-evidence")), 0, null);
        return ControlOperationReceiptV1.create(Bytes.sha256(Bytes.utf8("operation")),
                Bytes.sha256(Bytes.utf8("request")), Bytes.sha256(Bytes.utf8("scope")),
                Bytes.sha256(Bytes.utf8("targets")), 1, registered, 4_000);
    }
}
