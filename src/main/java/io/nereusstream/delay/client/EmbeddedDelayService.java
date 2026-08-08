package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandAppliedReceiptV1;
import io.nereusstream.delay.protocol.CommandApplyStatusV1;
import io.nereusstream.delay.protocol.CommandCodec;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1.KafkaQueuedAck;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
import io.nereusstream.delay.protocol.ControlRegistrationBindingV1;
import io.nereusstream.delay.protocol.ControlRegistrationOutcomeMessageV1;
import io.nereusstream.delay.protocol.ControlRegistrationProjectionV1;
import io.nereusstream.delay.protocol.PreparedControlOperationV1;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DefinitelyNotQueuedV1;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.EnqueueUncertainV1;
import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.FirstScheduleEligibilityV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.MessageQueryResponseV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.NonPersistenceProofV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PublicDestinationBindingViewV1;
import io.nereusstream.delay.protocol.ScheduleIntent;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.ownership.ControlOperationAuthority;
import io.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import io.nereusstream.delay.ownership.InMemoryControlOperationAuthority;
import io.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import io.nereusstream.delay.runtime.ApplyStatus;
import io.nereusstream.delay.runtime.CommandResult;
import io.nereusstream.delay.runtime.DelayShard;
import io.nereusstream.delay.runtime.DelayShardConfig;
import io.nereusstream.delay.runtime.MessageQuerySnapshot;
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
    private static final String EMBEDDED_CLUSTER_ID = "embedded";
    private static final UUID EMBEDDED_TOPIC_UUID = UUID.nameUUIDFromBytes(
            Bytes.utf8("embedded-command-topic"));

    private final ShardId shardId;
    private final Clock clock;
    private final SharedRocksDbResources resources;
    private final ShardStore store;
    private final DelayShard shard;
    private final ControlOperationAuthority controlOperationAuthority;
    private final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority;
    private final EmbeddedDelayServiceConfig clientConfig;
    private final Deque<QueuedRecord> pending = new ArrayDeque<>();
    private long nextOffset;
    /**
     * Kafka offsets are an unsigned 64-bit sequence.  Do not use {@code -1}
     * as an exhaustion sentinel: the all-ones offset is a valid final offset.
     */
    private boolean offsetExhausted;
    private long pendingBytes;
    /** Fences new client work while Store/Worker teardown can be retried. */
    private boolean closeStarted;
    private boolean closed;

    public EmbeddedDelayService(final ShardStoreConfig storeConfig, final ShardId shardId) {
        this(storeConfig, shardId, Clock.systemUTC(), EmbeddedDelayServiceConfig.defaults());
    }

    public EmbeddedDelayService(final ShardStoreConfig storeConfig, final ShardId shardId, final Clock clock) {
        this(storeConfig, shardId, clock, EmbeddedDelayServiceConfig.defaults());
    }

    public EmbeddedDelayService(final ShardStoreConfig storeConfig, final ShardId shardId, final Clock clock,
                                final EmbeddedDelayServiceConfig clientConfig) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.clientConfig = Objects.requireNonNull(clientConfig, "clientConfig");
        final SharedRocksDbResources openedResources = new SharedRocksDbResources(storeConfig);
        final ShardStore openedStore;
        try {
            openedStore = ShardStore.open(storeConfig, shardId, openedResources);
        } catch (RuntimeException failure) {
            closeAfterConstructionFailure(failure, null, openedResources);
            throw failure;
        }
        try {
            final ControlOperationAuthority openedControlOperationAuthority =
                    new InMemoryControlOperationAuthority();
            final ControlTargetRegistrationAuthority openedControlTargetRegistrationAuthority =
                    new InMemoryControlTargetRegistrationAuthority();
            final DelayShard openedShard = new DelayShard(openedStore, DelayShardConfig.defaults(), null, null, null,
                    null, null, openedControlTargetRegistrationAuthority);
            final SourcePosition last = openedShard.lastAppliedSourcePosition();
            if (last != null) {
                if (!(last instanceof KafkaSourcePosition kafka)
                        || !EMBEDDED_CLUSTER_ID.equals(kafka.authenticatedClusterId())
                        || !kafka.nativeTopicUuid().equals(EMBEDDED_TOPIC_UUID)) {
                    throw new IllegalStateException(
                            "embedded service cannot reopen a shard with another source identity");
                }
                if (kafka.offset() == -1L) {
                    offsetExhausted = true;
                } else {
                    nextOffset = kafka.offset() + 1;
                }
            }
            resources = openedResources;
            store = openedStore;
            controlOperationAuthority = openedControlOperationAuthority;
            controlTargetRegistrationAuthority = openedControlTargetRegistrationAuthority;
            shard = openedShard;
        } catch (RuntimeException failure) {
            closeAfterConstructionFailure(failure, openedStore, openedResources);
            throw failure;
        }
    }

    /**
     * Releases every resource acquired before a constructor can publish a
     * usable service. A source-identity or metadata mismatch is a normal
     * fail-closed startup outcome; it must not strand the RocksDB handle,
     * ownership slot, or shared native resources needed by the next retry.
     */
    private static void closeAfterConstructionFailure(final RuntimeException failure,
                                                      final ShardStore openedStore,
                                                      final SharedRocksDbResources openedResources) {
        try {
            if (openedStore != null) {
                openedStore.close();
            }
        } catch (RuntimeException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
        try {
            openedResources.close();
        } catch (RuntimeException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    @Override
    public PreparedCommand prepareSchedule(final ScheduleIntent intent, final long retryUntilEpochMs) {
        ensureOpen();
        return PreparedCommand.schedule(shardId, intent, retryUntilEpochMs);
    }

    @Override
    public PreparedCommand prepareLargeSchedule(final LargeScheduleIntent intent, final long retryUntilEpochMs) {
        ensureOpen();
        return PreparedCommand.prepareLarge(shardId, intent, retryUntilEpochMs);
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
        final int frameBytes = CommandCodec.encodeFrame(command).length;
        if (pending.size() >= clientConfig.maxPendingCommandCount()
                || (long) frameBytes > clientConfig.maxPendingCommandBytes()
                || pendingBytes > clientConfig.maxPendingCommandBytes() - frameBytes) {
            return CompletableFuture.completedFuture(EnqueueOutcome.definitelyNotQueued(command,
                    StableCode.SDK_BACKPRESSURE_NOT_SUBMITTED.wireValue()));
        }
        final long now = clock.millis();
        if (offsetExhausted) {
            throw new IllegalStateException("embedded Kafka source offset exhausted");
        }
        final long offset = nextOffset;
        final SourcePosition position = new KafkaSourcePosition(shardId, EMBEDDED_CLUSTER_ID, EMBEDDED_TOPIC_UUID,
                offset, null, now);
        // Advance only after the position has been validated.  A failed
        // position construction must not poison the next enqueue.  The
        // all-ones offset is valid, but there is no representable successor.
        if (offset == -1L) {
            offsetExhausted = true;
        } else {
            nextOffset = offset + 1;
        }
        final CommandQueuedReceipt receipt = new CommandQueuedReceipt(command.commandId(), command.delayMessageId(),
                shardId, position);
        pending.addLast(new QueuedRecord(command, position, frameBytes));
        pendingBytes = Math.addExact(pendingBytes, frameBytes);
        return CompletableFuture.completedFuture(EnqueueOutcome.queued(command, receipt));
    }

    /** Applies all queued records in Source Position order. */
    public synchronized void drain() {
        ensureOpen();
        while (!pending.isEmpty()) {
            // Keep the head charged until apply returns.  A malformed or
            // otherwise fatal local apply must not make close/drain silently
            // forget the command that was already reported as QUEUED.
            final QueuedRecord record = pending.peekFirst();
            shard.apply(record.command(), record.position());
            pending.removeFirst();
            pendingBytes -= record.frameBytes();
        }
    }

    public synchronized int pendingCommandCount() {
        ensureOpen();
        return pending.size();
    }

    public synchronized long pendingCommandBytes() {
        ensureOpen();
        return pendingBytes;
    }

    @Override
    public synchronized CompletionStage<CommandResult> awaitApplied(final CommandQueuedReceipt receipt) {
        ensureOpen();
        validateEmbeddedQueuedReceipt(receipt);
        final QueuedRecord pendingRecord = validateEmbeddedQueuedReceiptLocator(receipt);
        CommandResult physicalResult = null;
        if (pendingRecord == null) {
            drain();
        } else {
            // Keep the result returned by the exact pending physical record.
            // Looking it up by commandId after drain would collapse a later
            // position-level rejection (for example COMMAND_ID_CONFLICT) into
            // the first logical result, and would return null for a fence-only
            // rejection that intentionally has no logical dedupe result.
            while (!pending.isEmpty()) {
                final QueuedRecord record = pending.peekFirst();
                final CommandResult result = shard.apply(record.command(), record.position());
                pending.removeFirst();
                pendingBytes -= record.frameBytes();
                if (record == pendingRecord) {
                    physicalResult = result;
                }
            }
        }
        if (!shard.matchesCommandPosition(receipt.commandId(), receipt.sourcePosition())) {
            throw new IllegalArgumentException("queued receipt source position does not identify the command");
        }
        final CommandResult result = physicalResult == null
                ? shard.getCommandResult(receipt.commandId()) : physicalResult;
        return CompletableFuture.completedFuture(result);
    }

    /**
     * The embedded service can only resolve locators from its pinned source.
     * Validate before draining so a foreign or forged receipt cannot trigger
     * source application as a side effect of an invalid query.
     */
    private void validateEmbeddedQueuedReceipt(final CommandQueuedReceipt receipt) {
        Objects.requireNonNull(receipt, "receipt");
        if (!shardId.equals(receipt.shardId())
                || !shardId.equals(receipt.commandId().routingId().shardId())
                || !shardId.equals(receipt.delayMessageId().routingId().shardId())
                || !(receipt.sourcePosition() instanceof KafkaSourcePosition kafka)
                || !isEmbeddedSource(kafka)) {
            throw new IllegalArgumentException("queued receipt does not belong to embedded source");
        }
    }

    /**
     * A queued command has no durable POSITION audit until it is applied, so a
     * valid receipt may be proven by the exact pending record before drain. An
     * otherwise well-typed receipt is rejected before any pending record can be
     * applied; the command id alone is never a sufficient locator.
     */
    private QueuedRecord validateEmbeddedQueuedReceiptLocator(final CommandQueuedReceipt receipt) {
        if (shard.matchesCommandPosition(receipt.commandId(), receipt.sourcePosition())) {
            return null;
        }
        for (QueuedRecord record : pending) {
            if (record.command().commandId().equals(receipt.commandId())
                    && record.command().delayMessageId().equals(receipt.delayMessageId())
                    && Bytes.constantTimeEquals(record.position().canonicalBytes(),
                    receipt.sourcePosition().canonicalBytes())) {
                return record;
            }
        }
        throw new IllegalArgumentException("queued receipt source position does not identify a pending command");
    }

    /**
     * Converts the local queued locator into the canonical NDR1 receipt. The
     * method is an embedded adapter only; production callers must obtain the
     * broker acknowledgement and physical attempt id from the real ingress
     * adapter.
     */
    public synchronized CommandQueuedReceiptV1 queuedReceiptV1(final EnqueueOutcome outcome,
                                                               final long receiptQueryUntilEpochMs,
                                                               final byte[] physicalAttemptId) {
        ensureOpen();
        Objects.requireNonNull(outcome, "outcome");
        if (outcome.status() != EnqueueStatus.QUEUED || outcome.receipt() == null) {
            throw new IllegalArgumentException("only QUEUED outcomes have a queued receipt");
        }
        final SourcePosition source = outcome.receipt().sourcePosition();
        if (!(source instanceof KafkaSourcePosition kafka) || !isEmbeddedSource(kafka)
                || !shardId.equals(source.shardId())) {
            throw new IllegalArgumentException("embedded outcome has an unexpected source identity");
        }
        final byte[] responseHash = Bytes.sha256(Bytes.utf8("embedded-queued-ack\0"), source.canonicalBytes());
        final KafkaQueuedAck ack = new KafkaQueuedAck(kafka.authenticatedClusterId(), kafka.nativeTopicUuid(),
                kafka.shardId().partition(), kafka.offset(), kafka.leaderEpoch(),
                kafka.brokerLogAppendTimeEpochMs(), responseHash);
        return outcome.receipt().toV1(outcome.preparedCommand(), ack, receiptQueryUntilEpochMs, physicalAttemptId);
    }

    /**
     * Maps the embedded three-state ingress result to the closed wire union.
     * This bridge only emits a local pre-ownership proof for deterministic
     * embedded rejection; real adapters must supply authenticated Broker proof.
     */
    public synchronized EnqueueOutcomeMessageV1 enqueueOutcomeV1(final EnqueueOutcome outcome,
                                                                  final long receiptQueryUntilEpochMs,
                                                                  final byte[] physicalAttemptId) {
        ensureOpen();
        Objects.requireNonNull(outcome, "outcome");
        return switch (outcome.status()) {
            case QUEUED -> {
                if (!validPhysicalAttempt(physicalAttemptId)) {
                    yield localDefiniteOutcome(outcome.preparedCommand(), StableCode.INVALID_PREPARED_COMMAND);
                }
                yield EnqueueOutcomeMessageV1.queued(
                        queuedReceiptV1(outcome, receiptQueryUntilEpochMs, physicalAttemptId));
            }
            case DEFINITELY_NOT_QUEUED -> localDefiniteOutcome(outcome.preparedCommand(), stableErrorCode(outcome));
            case ENQUEUE_UNCERTAIN -> {
                if (!validPhysicalAttempt(physicalAttemptId)) {
                    yield localDefiniteOutcome(outcome.preparedCommand(), StableCode.INVALID_PREPARED_COMMAND);
                }
                final StableCode code = stableErrorCode(outcome);
                final CommandQueuedReceiptV1.PreparedCommandRef command =
                        CommandQueuedReceiptV1.PreparedCommandRef.from(outcome.preparedCommand());
                yield EnqueueOutcomeMessageV1.uncertain(new EnqueueUncertainV1(command, physicalAttemptId,
                        StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, command, null, null)));
            }
        };
    }

    private static EnqueueOutcomeMessageV1 localDefiniteOutcome(final PreparedCommand command,
                                                                  final StableCode code) {
        final CommandQueuedReceiptV1.PreparedCommandRef ref = CommandQueuedReceiptV1.PreparedCommandRef.from(command);
        final NonPersistenceProofV1 proof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.frameSha256(), null, null, null);
        return EnqueueOutcomeMessageV1.definitelyNotQueued(new DefinitelyNotQueuedV1(ref, proof,
                StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, ref, null, null)));
    }

    private static boolean validPhysicalAttempt(final byte[] physicalAttemptId) {
        if (physicalAttemptId == null || physicalAttemptId.length != NonPersistenceProofV1.ATTEMPT_ID_LENGTH) {
            return false;
        }
        for (byte value : physicalAttemptId) {
            if (value != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Performs the bounded local query chain: receipt validation, fixed-shard
     * source barrier, durable result lookup and retention projection. It does
     * not route across workers, authorize a tenant, or wait for a broker.
     */
    public synchronized CommandQueryResponseV1 queryCommand(final CommandQueuedReceiptV1 receipt,
                                                             final long nowEpochMs,
                                                             final long fullResultRetainUntilEpochMs,
                                                             final PublicDestinationBindingViewV1 binding) {
        ensureOpen();
        Objects.requireNonNull(receipt, "receipt");
        if (!isEmbeddedReceipt(receipt)) {
            return CommandQueryResponseV1.error(StableCode.RECEIPT_MISMATCH, null);
        }
        if (nowEpochMs < 0 || fullResultRetainUntilEpochMs < 0) {
            return CommandQueryResponseV1.error(StableCode.INVALID_RECEIPT, null);
        }
        if (nowEpochMs > receipt.receiptQueryUntilEpochMs()) {
            return CommandQueryResponseV1.resultEvidenceExpired();
        }
        final SourcePosition awaited = receipt.sourcePosition();
        final SourcePosition current = shard.lastAppliedSourcePosition();
        if (current == null) {
            return CommandQueryResponseV1.pending(new io.nereusstream.delay.protocol.PendingCommandViewV1(
                    awaited, null, safeRetryAt(nowEpochMs)));
        }
        try {
            if (!current.sameSourceIdentity(awaited)) {
                return CommandQueryResponseV1.error(StableCode.INTEGRITY_ERROR, null);
            }
            final int order = current.compareTo(awaited);
            if (order < 0) {
                return CommandQueryResponseV1.pending(new io.nereusstream.delay.protocol.PendingCommandViewV1(
                        awaited, current, safeRetryAt(nowEpochMs)));
            }
            if (order == 0 && !Bytes.constantTimeEquals(current.canonicalBytes(), awaited.canonicalBytes())) {
                return CommandQueryResponseV1.error(StableCode.INTEGRITY_ERROR, null);
            }
        } catch (IllegalArgumentException mismatch) {
            return CommandQueryResponseV1.error(StableCode.INTEGRITY_ERROR, null);
        }
        if (!shard.matchesCommandPosition(receipt.command().commandId(), awaited)) {
            return CommandQueryResponseV1.error(StableCode.RECEIPT_MISMATCH, null);
        }
        final CommandResult result = shard.getCommandResult(receipt.command().commandId());
        if (result == null) {
            return CommandQueryResponseV1.error(StableCode.INTEGRITY_ERROR, null);
        }
        if (!shard.matchesCommandHash(receipt.command().commandId(), receipt.command().commandHash())) {
            return CommandQueryResponseV1.error(StableCode.RECEIPT_MISMATCH, null);
        }
        return nowEpochMs > fullResultRetainUntilEpochMs
                ? BoundedLocalQueryProjector.compactCommand(result, fullResultRetainUntilEpochMs)
                : BoundedLocalQueryProjector.command(result, fullResultRetainUntilEpochMs, binding);
    }

    /**
     * Emits an applied receipt only after this local shard has crossed the
     * queued receipt's source barrier. A queued receipt is never upgraded in
     * place; the applied frame retains its queued-payload digest.
     */
    public synchronized CommandAppliedReceiptV1 appliedReceiptV1(final CommandQueuedReceiptV1 queuedReceipt,
                                                                  final long fullResultRetainUntilEpochMs,
                                                                  final PublicDestinationBindingViewV1 binding) {
        ensureOpen();
        Objects.requireNonNull(queuedReceipt, "queuedReceipt");
        if (!isEmbeddedReceipt(queuedReceipt)) {
            throw new IllegalArgumentException("queued receipt does not belong to embedded shard");
        }
        if (fullResultRetainUntilEpochMs < 0) {
            throw new IllegalArgumentException("full result retention deadline must be non-negative");
        }
        final SourcePosition current = shard.lastAppliedSourcePosition();
        if (current == null || !current.sameSourceIdentity(queuedReceipt.sourcePosition())) {
            throw new IllegalStateException("command has not crossed its source barrier");
        }
        final int order = current.compareTo(queuedReceipt.sourcePosition());
        if (order < 0 || (order == 0 && !Bytes.constantTimeEquals(current.canonicalBytes(),
                queuedReceipt.sourcePosition().canonicalBytes()))) {
            throw new IllegalStateException("command has not crossed its exact source barrier");
        }
        if (!shard.matchesCommandPosition(queuedReceipt.command().commandId(), queuedReceipt.sourcePosition())) {
            throw new IllegalArgumentException("queued receipt source position does not identify the command");
        }
        final CommandResult result = shard.getCommandResult(queuedReceipt.command().commandId());
        if (result == null) {
            throw new IllegalStateException("source barrier crossed without a durable command result");
        }
        if (!shard.matchesCommandHash(queuedReceipt.command().commandId(), queuedReceipt.command().commandHash())) {
            throw new IllegalArgumentException("queued receipt command hash does not match durable command identity");
        }
        final CommandApplyStatusV1 status = result.applyStatus() == ApplyStatus.APPLIED
                ? CommandApplyStatusV1.APPLIED : CommandApplyStatusV1.REJECTED;
        final SourcePosition appliedPosition = SourcePositionCodec.decode(result.appliedSourcePosition());
        final Integer generation = status == CommandApplyStatusV1.APPLIED && result.generation() >= 0
                ? result.generation() : null;
        final Long stateVersion = status == CommandApplyStatusV1.APPLIED && result.stateVersion() > 0
                ? result.stateVersion() : null;
        final PublicDestinationBindingViewV1 appliedBinding = status == CommandApplyStatusV1.APPLIED
                ? binding : null;
        return CommandAppliedReceiptV1.create(queuedReceipt, status, result.stableCode(), appliedPosition,
                generation, stateVersion, appliedBinding, fullResultRetainUntilEpochMs);
    }

    /** Projects a local message snapshot after the caller supplies policy inputs. */
    public synchronized MessageQueryResponseV1 queryMessage(final DelayMessageId messageId,
                                                             final PublicDestinationBindingViewV1 binding,
                                                             final io.nereusstream.delay.protocol.PublicEvidenceRefV1 evidence,
                                                             final FirstScheduleEligibilityV1 unknownEligibility) {
        ensureOpen();
        Objects.requireNonNull(messageId, "messageId");
        if (!shardId.equals(messageId.routingId().shardId())) {
            return MessageQueryResponseV1.error(StableCode.RECEIPT_MISMATCH, null);
        }
        final MessageQuerySnapshot snapshot = shard.queryMessageSnapshot(messageId);
        if (snapshot == null) {
            return MessageQueryResponseV1.unknown(Objects.requireNonNull(unknownEligibility,
                    "unknownEligibility"));
        }
        return BoundedLocalQueryProjector.message(snapshot, binding, evidence);
    }

    /** Projects a local message snapshot after the caller supplies policy inputs. */
    public synchronized MessageQueryResponseV1 queryMessage(final DelayMessageId messageId,
                                                             final PublicDestinationBindingViewV1 binding,
                                                             final DlqExportStateV1 dlqExportState,
                                                             final io.nereusstream.delay.protocol.PublicEvidenceRefV1 evidence,
                                                             final FirstScheduleEligibilityV1 unknownEligibility) {
        ensureOpen();
        Objects.requireNonNull(messageId, "messageId");
        if (!shardId.equals(messageId.routingId().shardId())) {
            return MessageQueryResponseV1.error(StableCode.RECEIPT_MISMATCH, null);
        }
        final MessageQuerySnapshot snapshot = shard.queryMessageSnapshot(messageId);
        if (snapshot == null) {
            return MessageQueryResponseV1.unknown(Objects.requireNonNull(unknownEligibility,
                    "unknownEligibility"));
        }
        return BoundedLocalQueryProjector.message(snapshot, binding, dlqExportState, evidence);
    }

    /**
     * Registers a control operation in the bounded embedded authority. This
     * local entry point preserves the complete receipt and is not a substitute
     * for production Oxia routing or authorization.
     */
    public synchronized ControlOperationQueryResponseV1 registerControlOperation(
            final ControlOperationReceiptV1 receipt, final CurrentControlOperationV1 initial) {
        ensureOpen();
        return controlOperationAuthority.register(receipt, initial);
    }

    /**
     * Registers an exact Prepared Control Operation through both local
     * registration seams and returns the receipt/current projection pair.
     * This is an embedded conformance path; production uses one Oxia
     * transaction plus authenticated actor/resource checks.
     */
    public synchronized ControlRegistrationProjectionV1 registerPreparedControlOperation(
            final PreparedControlOperationV1 prepared, final TrustedUtcIntervalEvidence registeredAt,
            final long controlOperationQueryWindowMs) {
        ensureOpen();
        Objects.requireNonNull(prepared, "prepared");
        controlTargetRegistrationAuthority.register(prepared);
        final ControlRegistrationProjectionV1 projection = ControlRegistrationProjectionV1.initialWithQueryWindow(
                prepared, registeredAt, controlOperationQueryWindowMs);
        ControlRegistrationBindingV1.validate(prepared,
                ControlRegistrationOutcomeMessageV1.recorded(projection.receipt()));
        final ControlOperationQueryResponseV1 response = controlOperationAuthority.register(projection.receipt(),
                projection.current());
        if (response.resultKind() != io.nereusstream.delay.protocol.ControlOperationQueryResultV1.CURRENT
                || !projection.current().equals(response.current())) {
            throw new IllegalStateException("embedded Control registration did not return its exact projection");
        }
        return projection;
    }

    /** Advances one embedded control operation through its exact revision CAS. */
    public synchronized ControlOperationQueryResponseV1 advanceControlOperation(
            final ControlOperationReceiptV1 receipt, final long expectedRevision,
            final CurrentControlOperationV1 next) {
        ensureOpen();
        return controlOperationAuthority.advance(receipt, expectedRevision, next);
    }

    /** Queries one embedded control operation before its fixed receipt boundary. */
    public synchronized ControlOperationQueryResponseV1 queryControlOperation(
            final ControlOperationReceiptV1 receipt, final long nowEpochMs) {
        ensureOpen();
        return controlOperationAuthority.query(receipt, nowEpochMs);
    }

    public synchronized DelayShard shard() {
        ensureOpen();
        return shard;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        if (!closeStarted) {
            // The embedded service has no asynchronous Broker producer to
            // await, so its close-drain deadline is represented by a
            // synchronous drain before the local DB is closed.  A failed
            // apply leaves the service open and the head record charged for
            // an explicit retry instead of acknowledging data loss.
            drain();
            // Fence only after the final allowed drain operation. Any later
            // Store/Worker close failure must leave this state retryable.
            closeStarted = true;
        }
        RuntimeException closeFailure = null;
        try {
            store.close();
        } catch (RuntimeException exception) {
            closeFailure = exception;
        }
        try {
            resources.close();
        } catch (RuntimeException exception) {
            if (closeFailure == null) {
                closeFailure = exception;
            } else {
                closeFailure.addSuppressed(exception);
            }
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
        closed = true;
    }

    private synchronized void ensureOpen() {
        if (closed || closeStarted) {
            throw new IllegalStateException("client is closed");
        }
    }

    private static StableCode stableErrorCode(final EnqueueOutcome outcome) {
        if (outcome.stableCode() <= 0) {
            throw new IllegalArgumentException("non-queued outcome must carry a nonzero stable code");
        }
        return StableCode.fromWire(outcome.stableCode());
    }

    private boolean isEmbeddedReceipt(final CommandQueuedReceiptV1 receipt) {
        if (!shardId.equals(receipt.command().shardId()) || !shardId.equals(receipt.sourcePosition().shardId())
                || !(receipt.sourcePosition() instanceof KafkaSourcePosition kafka) || !isEmbeddedSource(kafka)) {
            return false;
        }
        return receipt.brokerAck() instanceof KafkaQueuedAck ack
                && EMBEDDED_CLUSTER_ID.equals(ack.authenticatedClusterId())
                && EMBEDDED_TOPIC_UUID.equals(ack.nativeTopicUuid())
                && ack.partition() == shardId.partition()
                && ack.offset() == kafka.offset();
    }

    private boolean isEmbeddedSource(final KafkaSourcePosition source) {
        return shardId.equals(source.shardId()) && EMBEDDED_CLUSTER_ID.equals(source.authenticatedClusterId())
                && EMBEDDED_TOPIC_UUID.equals(source.nativeTopicUuid());
    }

    private static long safeRetryAt(final long nowEpochMs) {
        return nowEpochMs == Long.MAX_VALUE ? Long.MAX_VALUE : nowEpochMs + 1;
    }

    private record QueuedRecord(PreparedCommand command, SourcePosition position, int frameBytes) {
    }
}
