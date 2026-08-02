package io.nereusstream.delay.client;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandAppliedReceiptV1;
import io.nereusstream.delay.protocol.CommandApplyStatusV1;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1.KafkaQueuedAck;
import io.nereusstream.delay.protocol.CommandQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;
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
import io.nereusstream.delay.ownership.InMemoryControlOperationAuthority;
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
        controlOperationAuthority = new InMemoryControlOperationAuthority();
        final SourcePosition last = shard.lastAppliedSourcePosition();
        if (last != null) {
            if (!(last instanceof KafkaSourcePosition kafka)
                    || !EMBEDDED_CLUSTER_ID.equals(kafka.authenticatedClusterId())
                    || !kafka.nativeTopicUuid().equals(EMBEDDED_TOPIC_UUID)) {
                throw new IllegalStateException("embedded service cannot reopen a shard with another source identity");
            }
            nextOffset = Math.addExact(kafka.offset(), 1);
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
        final long now = clock.millis();
        final SourcePosition position = new KafkaSourcePosition(shardId, EMBEDDED_CLUSTER_ID, EMBEDDED_TOPIC_UUID,
                nextOffset++, null, now);
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
            case QUEUED -> EnqueueOutcomeMessageV1.queued(
                    queuedReceiptV1(outcome, receiptQueryUntilEpochMs, physicalAttemptId));
            case DEFINITELY_NOT_QUEUED -> {
                final StableCode code = stableErrorCode(outcome);
                final CommandQueuedReceiptV1.PreparedCommandRef command =
                        CommandQueuedReceiptV1.PreparedCommandRef.from(outcome.preparedCommand());
                final NonPersistenceProofV1 proof = NonPersistenceProofV1.create(
                        NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, command.frameSha256(),
                        null, null, null);
                yield EnqueueOutcomeMessageV1.definitelyNotQueued(new DefinitelyNotQueuedV1(command, proof,
                        StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, command, null, null)));
            }
            case ENQUEUE_UNCERTAIN -> {
                final StableCode code = stableErrorCode(outcome);
                final CommandQueuedReceiptV1.PreparedCommandRef command =
                        CommandQueuedReceiptV1.PreparedCommandRef.from(outcome.preparedCommand());
                yield EnqueueOutcomeMessageV1.uncertain(new EnqueueUncertainV1(command, physicalAttemptId,
                        StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, command, null, null)));
            }
        };
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
        final CommandResult result = shard.getCommandResult(receipt.command().commandId());
        if (result == null) {
            return CommandQueryResponseV1.error(StableCode.INTEGRITY_ERROR, null);
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
        final CommandResult result = shard.getCommandResult(queuedReceipt.command().commandId());
        if (result == null) {
            throw new IllegalStateException("source barrier crossed without a durable command result");
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

    private record QueuedRecord(PreparedCommand command, SourcePosition position) {
    }
}
