package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandBodies;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Single-writer deterministic command application loop for one Delay Shard.
 * Every state/result/source-position mutation is one synchronous RocksDB batch.
 */
public final class DelayShard {
    private static final int META_APPLIED_SOURCE_POSITION = 3;
    private static final int META_MUTATION_SEQUENCE = 5;
    private static final int META_QUOTA_USAGE = 1;

    private final ShardStore store;
    private final DelayShardConfig config;
    private SourcePosition lastAppliedSourcePosition;
    private long mutationSequence;
    private ShardQuota quota;

    public DelayShard(final ShardStore store, final DelayShardConfig config) {
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        final var sourceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_APPLIED_SOURCE_POSITION), 1);
        final byte[] source = sourceValue == null ? null : sourceValue.payload();
        lastAppliedSourcePosition = source == null ? null : SourcePositionCodec.decode(source);
        final var sequence = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_MUTATION_SEQUENCE), 1);
        mutationSequence = sequence == null ? 0 : readSequence(sequence.payload());
        final var quotaValue = store.getValue(ColumnFamily.META, KeyCodec.metaQuota(META_QUOTA_USAGE), 7);
        quota = quotaValue == null ? ShardQuota.empty() : ShardQuota.decode(quotaValue.payload());
    }

    public synchronized CommandResult apply(final PreparedCommand command, final SourcePosition sourcePosition) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!store.shardId().equals(command.shardId()) || !store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("command/source position does not belong to shard");
        }
        if (lastAppliedSourcePosition != null) {
            final int order = sourcePosition.compareTo(lastAppliedSourcePosition);
            if (order < 0) {
                throw new IllegalStateException("source position regressed");
            }
            if (order == 0) {
                final CommandDedupeRecord prior = readCommandDedupe(command.commandId());
                if (prior != null && Bytes.constantTimeEquals(prior.commandHash(), command.commandHash())) {
                    return prior.result();
                }
                throw new IllegalStateException("duplicate source position without matching command evidence");
            }
        }
        final CommandDedupeRecord prior = readCommandDedupe(command.commandId());
        if (prior != null) {
            if (!Bytes.constantTimeEquals(prior.commandHash(), command.commandHash())) {
                final CommandResult conflict = rejected(StableCode.COMMAND_ID_CONFLICT, sourcePosition, -1, 0, null);
                persistCommandOnly(command, sourcePosition);
                return conflict;
            }
            persistPositionOnly(command, sourcePosition);
            return prior.result();
        }
        if (sourcePosition.brokerPersistenceTimeEpochMs() > command.retryUntilEpochMs()) {
            return persistRejected(command, sourcePosition, StableCode.COMMAND_RETRY_WINDOW_EXPIRED);
        }

        final CommandResult result;
        try {
            result = switch (command.type()) {
                case SCHEDULE -> applySchedule(command, sourcePosition);
                case CANCEL -> applyCancel(command, sourcePosition);
                case RESCHEDULE -> applyReschedule(command, sourcePosition);
                case PREPARE_LARGE_SCHEDULE, COMMIT_LARGE_SCHEDULE ->
                        rejected(StableCode.INVALID_COMMAND, sourcePosition, -1, 0, null);
            };
        } catch (WindowViolationException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_DELIVERY_WINDOW);
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
        persistResultAndPosition(command, sourcePosition, result, nextMessage(command, sourcePosition, result));
        return result;
    }

    public synchronized MessageRecord getMessage(final DelayMessageId messageId) {
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idMessage(messageId), 1);
        return value == null ? null : MessageRecord.decode(value.payload());
    }

    public synchronized CommandResult getCommandResult(final CommandId commandId) {
        final var value = store.getValue(ColumnFamily.DEDUPE, KeyCodec.dedupeResult(commandId), 2);
        return value == null ? null : CommandResult.decode(value.payload());
    }

    public synchronized TerminalGenerationRecord getTerminalGeneration(final DelayMessageId messageId,
                                                                        final int generation) {
        final var value = store.getValue(ColumnFamily.TERMINAL, KeyCodec.terminalGeneration(messageId, generation), 1);
        return value == null ? null : TerminalGenerationRecord.decode(value.payload());
    }

    public synchronized LaneRecord getLane(final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        return readLane(laneId);
    }

    /** Applies an owner/runtime readiness transition without changing admission semantics. */
    public synchronized LaneRecord updateLaneReadiness(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId,
            final RuntimeReadiness readiness) {
        final LaneRecord current = readLane(laneId);
        if (current == null) {
            throw new IllegalArgumentException("unknown destination lane");
        }
        final LaneRecord next = current.withReadiness(readiness);
        store.write(batch -> batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(laneId), next.encode()));
        return next;
    }

    public synchronized SourcePosition lastAppliedSourcePosition() {
        return lastAppliedSourcePosition;
    }

    public synchronized long mutationSequence() {
        return mutationSequence;
    }

    public synchronized ShardQuota quota() {
        return quota;
    }

    /** Returns due work without claiming it or changing authoritative state. */
    public synchronized List<TimelineWork> discoverDue(final long earliestEpochMs, final int limit) {
        if (earliestEpochMs < 0 || limit <= 0) {
            throw new IllegalArgumentException("invalid due discovery bounds");
        }
        final List<TimelineWork> result = new ArrayList<>();
        discoverDueNamespace((byte) 1, (byte) 2, earliestEpochMs, limit, result);
        if (result.size() < limit) {
            discoverDueNamespace((byte) 2, (byte) 3, earliestEpochMs, limit, result);
        }
        return List.copyOf(result);
    }

    /** Returns expiry candidates; the caller must apply an exact source-ordered expiry mutation. */
    public synchronized List<ExpiryWork> discoverExpiry(final long earliestEpochMs, final int limit) {
        if (earliestEpochMs < 0 || limit <= 0) {
            throw new IllegalArgumentException("invalid expiry discovery bounds");
        }
        final List<ExpiryWork> result = new ArrayList<>();
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.TIMELINE,
                new byte[]{4, 1}, new byte[]{5, 1}, limit);
        for (var entry : entries) {
            final byte[] key = entry.key();
            if (key.length != 2 + 8 + 32 + DelayMessageId.LENGTH + 4) {
                throw new IllegalStateException("invalid EXPIRY key length");
            }
            final ByteBuffer input = ByteBuffer.wrap(key);
            if (input.get() != 4 || input.get() != 1) {
                throw new IllegalStateException("invalid EXPIRY key tag");
            }
            final long expireAt = input.getLong();
            final byte[] laneBytes = new byte[32];
            input.get(laneBytes);
            final byte[] messageBytes = new byte[DelayMessageId.LENGTH];
            input.get(messageBytes);
            final int generation = input.getInt();
            if (expireAt > earliestEpochMs) {
                break;
            }
            final TimelineEntry value = TimelineEntry.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1).payload());
            if (!value.messageId().equals(new DelayMessageId(messageBytes)) || value.generation() != generation) {
                throw new IllegalStateException("EXPIRY key/value identity mismatch");
            }
            result.add(new ExpiryWork(new DelayMessageId(messageBytes), new io.nereusstream.delay.protocol.DestinationLaneId(
                    laneBytes), generation, expireAt));
        }
        return List.copyOf(result);
    }

    private CommandResult applySchedule(final PreparedCommand command, final SourcePosition sourcePosition) {
        final var intent = CommandBodies.decodeSchedule(command.canonicalBody());
        validateWindow(intent.deliverAtEpochMs(), intent.expireAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
        final LaneRecord existingLane = readLane(intent.laneId());
        if (existingLane != null && existingLane.admissionGate() != AdmissionGate.OPEN) {
            final StableCode code = existingLane.admissionGate() == AdmissionGate.RETIRED
                    ? StableCode.LANE_TERMINALLY_CLOSED : StableCode.LANE_CLOSED;
            return rejected(code, sourcePosition, -1, 0, null);
        }
        final MessageRecord existing = getMessage(command.delayMessageId());
        if (existing != null) {
            return rejected(StableCode.DELAY_MESSAGE_ID_CONFLICT, sourcePosition, existing.generation(),
                    existing.stateVersion(), existing.status());
        }
        final boolean newLane = existingLane == null;
        if (newLane && quota.laneCount() >= config.maxLanes()) {
            return rejected(StableCode.DESTINATION_LANE_LIMIT_EXCEEDED, sourcePosition, -1, 0, null);
        }
        if (quota.pendingMessages() >= config.maxPendingMessages()
                || quota.pendingBytes() > config.maxPendingBytes() - intent.payload().length) {
            return rejected(StableCode.HARD_QUOTA_EXCEEDED, sourcePosition, -1, 0, null);
        }
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 0, 1,
                intent.deliverAtEpochMs(), intent.expireAtEpochMs(), intent.laneId(), intent.orderingMode(),
                intent.payload(), sourcePosition.canonicalBytes());
        return applied(StableCode.SCHEDULED, sourcePosition, message);
    }

    private void discoverDueNamespace(final byte tag, final byte nextTag, final long earliestEpochMs, final int limit,
                                      final List<TimelineWork> result) {
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.TIMELINE,
                new byte[]{tag, 1}, new byte[]{nextTag, 1}, limit - result.size());
        for (var entry : entries) {
            final byte[] key = entry.key();
            final int tokenLength = key.length > 2 + 32 + 8 && key[2 + 32 + 8] == 1 ? 9
                    : key.length > 2 + 32 + 8 && key[2 + 32 + 8] == 2 ? 21 : -1;
            if (tokenLength < 0 || key.length != 2 + 32 + 8 + tokenLength + DelayMessageId.LENGTH + 4) {
                throw new IllegalStateException("invalid timeline key length or source token");
            }
            final ByteBuffer input = ByteBuffer.wrap(key);
            if (input.get() != tag || input.get() != 1) {
                throw new IllegalStateException("invalid timeline key tag");
            }
            final byte[] laneBytes = new byte[32];
            input.get(laneBytes);
            final long eligibleAt = input.getLong();
            input.position(input.position() + tokenLength);
            final byte[] messageBytes = new byte[DelayMessageId.LENGTH];
            input.get(messageBytes);
            final int generation = input.getInt();
            if (eligibleAt > earliestEpochMs) {
                break;
            }
            final TimelineEntry value = TimelineEntry.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1).payload());
            final DelayMessageId messageId = new DelayMessageId(messageBytes);
            if (!value.messageId().equals(messageId) || value.generation() != generation) {
                throw new IllegalStateException("timeline key/value identity mismatch");
            }
            result.add(new TimelineWork(messageId, new io.nereusstream.delay.protocol.DestinationLaneId(laneBytes),
                    generation, eligibleAt, tag == 2));
            if (result.size() >= limit) {
                return;
            }
        }
    }

    private CommandResult applyCancel(final PreparedCommand command, final SourcePosition sourcePosition) {
        final MessageRecord existing = getMessage(command.delayMessageId());
        if (existing == null) {
            return applied(StableCode.NOT_FOUND, sourcePosition, null);
        }
        final int expectedGeneration = CommandBodies.decodeCancel(command.canonicalBody());
        if (expectedGeneration >= 0 && expectedGeneration != existing.generation()) {
            return applied(StableCode.VERSION_CONFLICT, sourcePosition, existing);
        }
        return switch (existing.status()) {
            case SCHEDULED -> applied(StableCode.CANCELED, sourcePosition,
                    new MessageRecord(MessageStatus.CANCELED, existing.generation(), existing.stateVersion() + 1,
                            existing.deliverAtEpochMs(), existing.expireAtEpochMs(), existing.laneId(),
                            existing.orderingMode(), existing.payload(), existing.scheduleSourcePosition()));
            case CANCELED -> applied(StableCode.ALREADY_CANCELED, sourcePosition, existing);
            case PUBLISHED, PUBLISHING, UNCERTAIN -> applied(StableCode.TOO_LATE, sourcePosition, existing);
            default -> applied(StableCode.TOO_LATE, sourcePosition, existing);
        };
    }

    private CommandResult applyReschedule(final PreparedCommand command, final SourcePosition sourcePosition) {
        final MessageRecord existing = getMessage(command.delayMessageId());
        if (existing == null) {
            return applied(StableCode.NOT_FOUND, sourcePosition, null);
        }
        final CommandBodies.RescheduleValues values = CommandBodies.decodeReschedule(command.canonicalBody());
        if (values.expectedGeneration() >= 0 && values.expectedGeneration() != existing.generation()) {
            return applied(StableCode.VERSION_CONFLICT, sourcePosition, existing);
        }
        if (existing.status() != MessageStatus.SCHEDULED) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        validateWindow(values.deliverAtEpochMs(), values.expireAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
        final MessageRecord replacement = new MessageRecord(MessageStatus.SCHEDULED, existing.generation() + 1,
                existing.stateVersion() + 1, values.deliverAtEpochMs(), values.expireAtEpochMs(), existing.laneId(),
                existing.orderingMode(), existing.payload(), sourcePosition.canonicalBytes());
        return applied(StableCode.SUPERSEDED, sourcePosition, replacement);
    }

    private void validateWindow(final long deliverAt, final long expireAt, final long brokerTime) {
        final long maxDeliver = Math.addExact(brokerTime, config.maxDelayHorizonMs());
        final long minExpire = Math.addExact(Math.max(deliverAt, brokerTime), config.minDeliveryWindowMs());
        final long maxExpire = Math.addExact(brokerTime, config.maxMessageLifetimeMs());
        if (deliverAt > maxDeliver || expireAt < minExpire || expireAt > maxExpire) {
            throw new WindowViolationException();
        }
    }

    private CommandResult persistRejected(final PreparedCommand command, final SourcePosition position,
                                          final StableCode code) {
        final CommandResult result = rejected(code, position, -1, 0, null);
        persistResultAndPosition(command, position, result, null);
        return result;
    }

    private CommandResult applied(final StableCode code, final SourcePosition sourcePosition,
                                  final MessageRecord nextMessage) {
        return new CommandResult(ApplyStatus.APPLIED, code,
                nextMessage == null ? -1 : nextMessage.generation(),
                nextMessage == null ? 0 : nextMessage.stateVersion(),
                nextMessage == null ? null : nextMessage.status(), sourcePosition.canonicalBytes());
    }

    private CommandResult rejected(final StableCode code, final SourcePosition sourcePosition, final int generation,
                                   final long stateVersion, final MessageStatus status) {
        return new CommandResult(ApplyStatus.REJECTED, code, generation, stateVersion, status,
                sourcePosition.canonicalBytes());
    }

    private void persistResultAndPosition(final PreparedCommand command, final SourcePosition position,
                                          final CommandResult result, final MessageRecord next) {
        final MessageRecord prior = getMessage(command.delayMessageId());
        final boolean existingLane = next != null && readLane(next.laneId()) != null;
        final ShardQuota nextQuota = quotaAfter(prior, next, result, existingLane);
        store.write(batch -> {
            if (next != null) {
                if (prior != null && prior.status() == MessageStatus.SCHEDULED) {
                    batch.delete(ColumnFamily.TIMELINE, timelineKey(command.delayMessageId(), prior));
                    batch.delete(ColumnFamily.TIMELINE, expiryKey(command.delayMessageId(), prior));
                    final TerminalGenerationRecord terminal = terminalFor(command, position, result, prior, next);
                    if (terminal != null) {
                        batch.putValue(ColumnFamily.TERMINAL, 1,
                                KeyCodec.terminalGeneration(command.delayMessageId(), terminal.generation()),
                                terminal.encode());
                    }
                }
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(command.delayMessageId()), next.encode());
                if (result.stableCode() == StableCode.SCHEDULED && !existingLane) {
                    batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(next.laneId()),
                            LaneRecord.initial(next.laneId(), position).encode());
                }
                if (next.status() == MessageStatus.SCHEDULED) {
                    batch.putValue(ColumnFamily.TIMELINE, 1,
                            timelineKey(command.delayMessageId(), next),
                            new TimelineEntry(command.delayMessageId(), next.generation()).encode());
                    batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(command.delayMessageId(), next),
                            new TimelineEntry(command.delayMessageId(), next.generation()).encode());
                }
            }
            batch.putValue(ColumnFamily.DEDUPE, 1, KeyCodec.dedupeCommand(command.commandId()),
                    new CommandDedupeRecord(command.commandHash(), result).encode());
            batch.putValue(ColumnFamily.DEDUPE, 2, KeyCodec.dedupeResult(command.commandId()), result.encode());
            batch.putValue(ColumnFamily.DEDUPE, 3, KeyCodec.dedupePosition(position.canonicalBytes()),
                    command.commandId().bytes());
            if (!nextQuota.equals(quota)) {
                batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            }
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence++;
        quota = nextQuota;
    }

    private ShardQuota quotaAfter(final MessageRecord prior, final MessageRecord next, final CommandResult result,
                                  final boolean existingLane) {
        if (prior == null && next != null && result.stableCode() == StableCode.SCHEDULED) {
            return quota.addSchedule(next.payload().length, !existingLane);
        }
        if (prior != null && prior.status() == MessageStatus.SCHEDULED && next != null
                && next.status() == MessageStatus.CANCELED) {
            return quota.removeSchedule(prior.payload().length);
        }
        return quota;
    }

    private void persistCommandOnly(final PreparedCommand command, final SourcePosition position) {
        persistPositionOnly(command, position);
    }

    private MessageRecord nextMessage(final PreparedCommand command, final SourcePosition position,
                                      final CommandResult result) {
        if (result.applyStatus() != ApplyStatus.APPLIED) {
            return null;
        }
        final MessageRecord prior = getMessage(command.delayMessageId());
        return switch (command.type()) {
            case SCHEDULE -> {
                if (result.stableCode() != StableCode.SCHEDULED) {
                    yield null;
                }
                final var intent = CommandBodies.decodeSchedule(command.canonicalBody());
                yield new MessageRecord(MessageStatus.SCHEDULED, 0, 1, intent.deliverAtEpochMs(),
                        intent.expireAtEpochMs(), intent.laneId(), intent.orderingMode(), intent.payload(),
                        position.canonicalBytes());
            }
            case CANCEL -> result.stableCode() == StableCode.CANCELED && prior != null
                    ? new MessageRecord(MessageStatus.CANCELED, prior.generation(), prior.stateVersion() + 1,
                    prior.deliverAtEpochMs(), prior.expireAtEpochMs(), prior.laneId(), prior.orderingMode(),
                    prior.payload(), prior.scheduleSourcePosition()) : null;
            case RESCHEDULE -> result.stableCode() == StableCode.SUPERSEDED && prior != null
                    ? rescheduledMessage(command, position, prior) : null;
            case PREPARE_LARGE_SCHEDULE, COMMIT_LARGE_SCHEDULE -> null;
        };
    }

    private TerminalGenerationRecord terminalFor(final PreparedCommand command, final SourcePosition position,
                                                 final CommandResult result, final MessageRecord prior,
                                                 final MessageRecord next) {
        final MessageStatus status;
        if (result.stableCode() == StableCode.CANCELED) {
            status = MessageStatus.CANCELED;
        } else if (result.stableCode() == StableCode.SUPERSEDED) {
            status = MessageStatus.SUPERSEDED;
        } else {
            return null;
        }
        return new TerminalGenerationRecord(command.delayMessageId(), prior.generation(), status,
                result.stableCode(), prior.stateVersion(), position.canonicalBytes(),
                next.status() == MessageStatus.UNCERTAIN);
    }

    private MessageRecord rescheduledMessage(final PreparedCommand command, final SourcePosition position,
                                             final MessageRecord prior) {
        final var values = CommandBodies.decodeReschedule(command.canonicalBody());
        return new MessageRecord(MessageStatus.SCHEDULED, prior.generation() + 1, prior.stateVersion() + 1,
                values.deliverAtEpochMs(), values.expireAtEpochMs(), prior.laneId(), prior.orderingMode(),
                prior.payload(), position.canonicalBytes());
    }

    private void persistPositionOnly(final PreparedCommand command, final SourcePosition position) {
        store.write(batch -> {
            batch.putValue(ColumnFamily.DEDUPE, 3, KeyCodec.dedupePosition(position.canonicalBytes()),
                    command.commandId().bytes());
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence++;
    }

    private CommandDedupeRecord readCommandDedupe(final CommandId commandId) {
        final var value = store.getValue(ColumnFamily.DEDUPE, KeyCodec.dedupeCommand(commandId), 1);
        return value == null ? null : CommandDedupeRecord.decode(value.payload());
    }

    private LaneRecord readLane(final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final var value = store.getValue(ColumnFamily.META, KeyCodec.metaLane(laneId), 2);
        return value == null ? null : LaneRecord.decode(value.payload());
    }

    private byte[] timelineKey(final DelayMessageId messageId, final MessageRecord message) {
        final long eligibleAt = message.deliverAtEpochMs();
        final SourcePosition position = SourcePositionCodec.decode(message.scheduleSourcePosition());
        return message.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? KeyCodec.timelineOrdered(message.laneId(), eligibleAt, position.sourceOrderToken(), messageId,
                message.generation())
                : KeyCodec.timelineDue(message.laneId(), eligibleAt, position.sourceOrderToken(), messageId,
                message.generation());
    }

    private byte[] expiryKey(final DelayMessageId messageId, final MessageRecord message) {
        return KeyCodec.timelineExpiry(message.expireAtEpochMs(), message.laneId(), messageId,
                message.generation());
    }

    private void writePosition(final ShardStore.Batch batch, final SourcePosition position) throws org.rocksdb.RocksDBException {
        batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(META_APPLIED_SOURCE_POSITION),
                position.canonicalBytes());
        batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(META_MUTATION_SEQUENCE),
                Bytes.u64be(Math.addExact(mutationSequence, 1)));
    }

    private static long readSequence(final byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalStateException("invalid shard mutation sequence");
        }
        return ByteBuffer.wrap(bytes).getLong();
    }

    private static final class WindowViolationException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;
    }

    public record TimelineWork(DelayMessageId messageId,
                               io.nereusstream.delay.protocol.DestinationLaneId laneId,
                               int generation, long eligibleAtEpochMs, boolean ordered) {
        public TimelineWork {
            Objects.requireNonNull(messageId, "messageId");
            Objects.requireNonNull(laneId, "laneId");
            if (generation < 0 || eligibleAtEpochMs < 0) {
                throw new IllegalArgumentException("invalid timeline work");
            }
        }
    }

    public record ExpiryWork(DelayMessageId messageId,
                             io.nereusstream.delay.protocol.DestinationLaneId laneId,
                             int generation, long expireAtEpochMs) {
        public ExpiryWork {
            Objects.requireNonNull(messageId, "messageId");
            Objects.requireNonNull(laneId, "laneId");
            if (generation < 0 || expireAtEpochMs < 0) {
                throw new IllegalArgumentException("invalid expiry work");
            }
        }
    }
}
