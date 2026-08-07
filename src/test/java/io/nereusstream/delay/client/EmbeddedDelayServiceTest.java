package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AdapterKindV1;
import io.nereusstream.delay.protocol.AdapterMetadataV1;
import io.nereusstream.delay.protocol.CommandAppliedReceiptV1;
import io.nereusstream.delay.protocol.CommandBodies;
import io.nereusstream.delay.protocol.CommandQueryResult;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.ControlAuthorV1;
import io.nereusstream.delay.protocol.ControlOperationRequestV1;
import io.nereusstream.delay.protocol.ControlOperationQueryResultV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.ControlOperationStateV1;
import io.nereusstream.delay.protocol.ControlReasonKindV1;
import io.nereusstream.delay.protocol.ControlReasonV1;
import io.nereusstream.delay.protocol.ControlTargetKindV1;
import io.nereusstream.delay.protocol.ControlTargetRefV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryMode;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.EnqueueOutcomeKindV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.OrderingMode;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.KafkaMetadataV1;
import io.nereusstream.delay.protocol.ForceCheckpointRequestV1;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.MessageQueryResult;
import io.nereusstream.delay.protocol.MessagePreconditionV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.ShardSubjectV1;
import io.nereusstream.delay.protocol.PreparedControlOperationV1;
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
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void sdkBackpressureRejectsBeforeSourcePositionAndByteBudgetAreConsumed() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 30);
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("backpressure-lane")), 2_000, 5_000,
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"));
        final EmbeddedDelayServiceConfig bounded = new EmbeddedDelayServiceConfig(1, Long.MAX_VALUE);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("backpressure-count")), shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC), bounded)) {
            final PreparedCommand first = service.prepareSchedule(intent, 10_000);
            final PreparedCommand second = service.prepareSchedule(intent, 10_000);
            assertEquals(EnqueueStatus.QUEUED, service.enqueue(first).toCompletableFuture().join().status());
            final EnqueueOutcome rejected = service.enqueue(second).toCompletableFuture().join();
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, rejected.status());
            assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(), rejected.stableCode());
            assertEquals(1, service.pendingCommandCount());
            assertTrue(service.pendingCommandBytes() > 0);

            service.drain();
            assertEquals(0, service.pendingCommandCount());
            assertEquals(0, service.pendingCommandBytes());
            final PreparedCommand third = service.prepareSchedule(intent, 10_000);
            final EnqueueOutcome afterDrain = service.enqueue(third).toCompletableFuture().join();
            assertEquals(EnqueueStatus.QUEUED, afterDrain.status());
            assertEquals(1, ((KafkaSourcePosition) afterDrain.receipt().sourcePosition()).offset());
        }

        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("backpressure-bytes")), shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC),
                new EmbeddedDelayServiceConfig(4, 1))) {
            final PreparedCommand command = service.prepareSchedule(intent, 10_000);
            final EnqueueOutcome rejected = service.enqueue(command).toCompletableFuture().join();
            assertEquals(EnqueueStatus.DEFINITELY_NOT_QUEUED, rejected.status());
            assertEquals(StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue(), rejected.stableCode());
            assertEquals(0, service.pendingCommandCount());
            assertEquals(0, service.pendingCommandBytes());
        }
    }

    @Test
    void closeDrainsQueuedCommandsBeforeClosingTheShardDb() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 31);
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("close-drain-lane")), 2_000, 5_000,
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"));
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("close-drain"));
        final PreparedCommand command;
        final CommandQueuedReceipt receipt;
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                config, shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            command = service.prepareSchedule(intent, 10_000);
            receipt = service.enqueue(command).toCompletableFuture().join().receipt();
            assertEquals(1, service.pendingCommandCount());
        }

        try (EmbeddedDelayService reopened = new EmbeddedDelayService(
                config, shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final CommandResult result = reopened.awaitApplied(receipt).toCompletableFuture().join();
            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(0, reopened.pendingCommandCount());
        }
    }

    @Test
    void closedEmbeddedServiceDoesNotExposeShardOrBufferState() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 32);
        final EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("closed-access")), shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC));
        service.close();
        try {
            assertThrows(IllegalStateException.class, service::shard);
            assertThrows(IllegalStateException.class, service::pendingCommandCount);
            assertThrows(IllegalStateException.class, service::pendingCommandBytes);
        } finally {
            service.close();
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
            nextOffset.setLong(service, -1L);

            assertThrows(IllegalStateException.class, () -> service.enqueue(command));
            assertEquals(-1L, nextOffset.getLong(service));
        }
    }

    @Test
    void reopenedEmbeddedServiceKeepsUnsignedMaximumSourceOffsetExhaustion() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 20);
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("reopen-offset-exhaustion"));
        final ScheduleIntent intent = new ScheduleIntent(
                DestinationLaneId.derive(Bytes.utf8("reopen-offset-exhaustion-lane")), 2_000, 5_000,
                OrderingMode.BEST_EFFORT, Bytes.utf8("payload"));
        try (EmbeddedDelayService first = new EmbeddedDelayService(config, shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand command = first.prepareSchedule(intent, 10_000);
            first.shard().apply(command, new KafkaSourcePosition(shard, "embedded",
                    UUID.nameUUIDFromBytes(Bytes.utf8("embedded-command-topic")), -1L, null, 1_000));
        }
        try (EmbeddedDelayService second = new EmbeddedDelayService(config, shard,
                Clock.fixed(Instant.ofEpochMilli(1_001), ZoneOffset.UTC))) {
            final PreparedCommand command = second.prepareCancel(DelayMessageId.random(shard), -1, 10_000);
            assertThrows(IllegalStateException.class, () -> second.enqueue(command));
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
            final PreparedCommand command = cancelV1(shard, 10_000);
            final EnqueueOutcome outcome = service.enqueue(command).toCompletableFuture().join();
            final var queued = service.queuedReceiptV1(outcome, 10_000,
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("attempt")), 16));

            assertEquals(CommandQueryResult.PENDING,
                    service.queryCommand(queued, now, 10_000, publicBinding()).resultKind());
            service.drain();
            assertEquals(CommandQueryResult.APPLIED,
                    service.queryCommand(queued, now, 10_000, null).resultKind());
            final CommandAppliedReceiptV1 applied = service.appliedReceiptV1(queued, 10_000, null);
            assertEquals(io.nereusstream.delay.protocol.ReceiptKind.COMMAND_APPLIED,
                    io.nereusstream.delay.protocol.ReceiptFrame.decode(applied.frame()).kind());
            assertEquals(applied, CommandAppliedReceiptV1.decodeFrame(applied.frame()));
            assertEquals(CommandQueryResult.RESULT_EXPIRED,
                    service.queryCommand(queued, 3_000, 2_000, publicBinding()).resultKind());
            assertEquals(CommandQueryResult.RESULT_EVIDENCE_EXPIRED,
                    service.queryCommand(queued, 10_001, 10_000, publicBinding()).resultKind());

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
            final PreparedCommand command = cancelV1(shard, 10_000);
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
                    service.queryCommand(forged, 1_000, 10_000, null).resultKind());
            assertThrows(IllegalStateException.class,
                    () -> service.appliedReceiptV1(forged, 10_000, null));
        }
    }

    @Test
    void embeddedQueryBindsReceiptCommandHashToDurableDedupeIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 19);
        final Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("query-command-hash")), shard, clock)) {
            final PreparedCommand command = cancelV1(shard, 10_000);
            final EnqueueOutcome outcome = service.enqueue(command).toCompletableFuture().join();
            final CommandQueuedReceiptV1 queued = service.queuedReceiptV1(outcome, 10_000,
                    java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("query-command-hash-attempt")), 16));
            final PreparedCommand forgedCommand = PreparedCommand.create(shard, command.commandId(),
                    command.delayMessageId(), command.type(), command.retryUntilEpochMs(),
                    CommandBodies.cancelV1(command.delayMessageId(), command.retryUntilEpochMs(),
                            new MessagePreconditionV1(1L, null)));
            final CommandQueuedReceiptV1 forged = CommandQueuedReceiptV1.create(forgedCommand,
                    queued.sourcePosition(), queued.brokerAck(), queued.receiptQueryUntilEpochMs(),
                    queued.physicalEnqueueAttemptId());

            service.drain();
            assertEquals(CommandQueryResult.RECEIPT_MISMATCH,
                    service.queryCommand(forged, 1_000, 10_000, publicBinding()).resultKind());
            assertThrows(IllegalArgumentException.class,
                    () -> service.appliedReceiptV1(forged, 10_000, null));
            assertEquals(CommandQueryResult.APPLIED,
                    service.queryCommand(queued, 1_000, 10_000, null).resultKind());
        }
    }

    @Test
    void embeddedQueuedReceiptRejectsNonQueuedOutcome() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 4);
        try (EmbeddedDelayService service = new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir.resolve("reject")),
                shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final ShardId otherShard = new ShardId(RouteIncarnation.random(), 0);
            final PreparedCommand otherShardCommand = scheduleV1(otherShard, "other-lane", 2_000, 5_000, 10_000);
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
            final CurrentControlOperationV1 dispatching = new CurrentControlOperationV1(receipt.operationId(),
                    receipt.requestHash(), receipt.authenticatedScopeHash(), ControlOperationStateV1.DISPATCHING, 2,
                    List.of(), null);
            assertEquals(ControlOperationQueryResultV1.CURRENT,
                    service.advanceControlOperation(receipt, 1, dispatching).resultKind());
            final CurrentControlOperationV1 next = new CurrentControlOperationV1(receipt.operationId(),
                    receipt.requestHash(), receipt.authenticatedScopeHash(), ControlOperationStateV1.IN_PROGRESS, 3,
                    List.of(), null);
            assertEquals(ControlOperationQueryResultV1.CURRENT,
                    service.advanceControlOperation(receipt, 2, next).resultKind());
            assertEquals(next, service.queryControlOperation(receipt, 2_000).current());
        }
    }

    @Test
    void embeddedPreparedControlRegistrationUsesOneExactProjection() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final ControlOperationRequestV1 request = ControlOperationRequestV1.forceCheckpoint(
                new ForceCheckpointRequestV1(new ControlReasonV1(ControlReasonKindV1.MAINTENANCE, null, null)));
        final ControlTargetRefV1 target = new ControlTargetRefV1(0, ControlTargetKindV1.SHARD,
                new ShardSubjectV1(shard), null, null);
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final PreparedControlOperationV1 prepared = PreparedControlOperationV1.prepare(Bytes.sha256(
                        Bytes.utf8("embedded-prepared-control")), request.kind(),
                new ControlAuthorV1(Bytes.sha256(Bytes.utf8("actor")), Bytes.sha256(Bytes.utf8("roles")),
                        Bytes.sha256(Bytes.utf8("scope"))), request, List.of(target), 1, 2, 1,
                keyPair.getPrivate());
        final TrustedUtcIntervalEvidence registeredAt = new TrustedUtcIntervalEvidence(1_000, 1_100,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("embedded-control-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("control-evidence")), 0, null);
        try (EmbeddedDelayService service = new EmbeddedDelayService(
                ShardStoreConfig.defaults(tempDir.resolve("prepared-control")), shard,
                Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final var projection = service.registerPreparedControlOperation(prepared, registeredAt, 1_000);
            assertArrayEquals(prepared.operationId(), projection.receipt().operationId());
            assertEquals(ControlOperationStateV1.PENDING, projection.current().state());
            assertEquals(ControlOperationQueryResultV1.CURRENT,
                    service.queryControlOperation(projection.receipt(), 1_500).resultKind());
        }
    }

    @Test
    void embeddedIngressProjectsAllManagedOutcomeBranches() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 5);
        try (EmbeddedDelayService service = new EmbeddedDelayService(ShardStoreConfig.defaults(tempDir.resolve("outcome")),
                shard, Clock.fixed(Instant.ofEpochMilli(1_000), ZoneOffset.UTC))) {
            final PreparedCommand queuedCommand = scheduleV1(shard, "outcome-queued", 2_000, 5_000, 10_000);
            final EnqueueOutcome queued = service.enqueue(queuedCommand).toCompletableFuture().join();
            final byte[] attemptId = java.util.Arrays.copyOf(Bytes.sha256(Bytes.utf8("outcome-attempt")), 16);
            final EnqueueOutcomeMessageV1 queuedWire = service.enqueueOutcomeV1(queued, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKindV1.QUEUED, queuedWire.kind());
            assertEquals(queuedWire, EnqueueOutcomeMessageV1.decode(queuedWire.canonicalBytes()));

            final ShardId rejectedShard = new ShardId(RouteIncarnation.random(), 0);
            final PreparedCommand rejectedCommand = scheduleV1(rejectedShard, "outcome-rejected", 2_000, 5_000,
                    10_000);
            final EnqueueOutcome rejected = service.enqueue(rejectedCommand).toCompletableFuture().join();
            final EnqueueOutcomeMessageV1 definiteWire = service.enqueueOutcomeV1(rejected, 10_000, attemptId);
            assertEquals(EnqueueOutcomeKindV1.DEFINITELY_NOT_QUEUED, definiteWire.kind());
            assertEquals(definiteWire, EnqueueOutcomeMessageV1.decode(definiteWire.canonicalBytes()));

            final PreparedCommand uncertainCommand = scheduleV1(shard, "outcome-uncertain", 2_000, 5_000, 10_000);
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

    private static PreparedCommand scheduleV1(final ShardId shard, final String lane, final long deliverAt,
                                              final long expireAt, final long retryUntil) {
        return PreparedCommand.scheduleV1(shard, scheduleIntentV1(lane, deliverAt, expireAt, "payload"), retryUntil);
    }

    private static PreparedCommand cancelV1(final ShardId shard, final long retryUntil) {
        return PreparedCommand.cancelV1(shard, DelayMessageId.random(shard), new MessagePreconditionV1(0L, null),
                retryUntil);
    }

    private static ScheduleIntentV1 scheduleIntentV1(final String lane, final long deliverAt, final long expireAt,
                                                     final String payload) {
        final ProfileRefV1 destination = new ProfileRefV1(Bytes.utf8("destination-" + lane), 1,
                Bytes.sha256(Bytes.utf8("destination-semantic-" + lane)), ProfileKindV1.DESTINATION);
        final RetryPolicyRefV1 retryPolicy = new RetryPolicyRefV1(Bytes.utf8("retry-" + lane), 1,
                Bytes.sha256(Bytes.utf8("retry-semantic-" + lane)));
        return ScheduleIntentV1.create(destination, retryPolicy, deliverAt, expireAt, DeliveryMode.MANAGED,
                OrderingMode.BEST_EFFORT, new byte[0], Bytes.utf8(payload), null,
                AdapterMetadataV1.kafka(new KafkaMetadataV1(null, List.of())), null, null);
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
