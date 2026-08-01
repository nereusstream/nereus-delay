package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandBodies;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.PayloadCommitProof;
import io.nereusstream.delay.protocol.PayloadProofTrustSet;
import io.nereusstream.delay.protocol.PayloadReference;
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
    private final PayloadProofTrustSet payloadProofTrustSet;
    private SourcePosition lastAppliedSourcePosition;
    private long mutationSequence;
    private ShardQuota quota;

    public DelayShard(final ShardStore store, final DelayShardConfig config) {
        this(store, config, null);
    }

    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet) {
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.payloadProofTrustSet = payloadProofTrustSet;
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

        if (command.type() == io.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                || command.type() == io.nereusstream.delay.protocol.CommandType.COMMIT_LARGE_SCHEDULE) {
            return applyLargePayloadCommand(command, sourcePosition);
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

    public synchronized PayloadReservation getReservation(final byte[] reservationId) {
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idReservation(reservationId), 2);
        return value == null ? null : PayloadReservation.decode(value.payload());
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

    public io.nereusstream.delay.protocol.ShardId shardId() {
        return store.shardId();
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

    private CommandResult applyLargePayloadCommand(final PreparedCommand command,
                                                   final SourcePosition sourcePosition) {
        try {
            return command.type() == io.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                    ? applyPrepareLarge(command, sourcePosition)
                    : applyCommitLarge(command, sourcePosition);
        } catch (WindowViolationException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_DELIVERY_WINDOW);
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
    }

    private CommandResult applyPrepareLarge(final PreparedCommand command, final SourcePosition sourcePosition) {
        final LargeScheduleIntent intent = CommandBodies.decodePrepareLarge(command.canonicalBody());
        validateWindow(intent.deliverAtEpochMs(), intent.expireAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
        if (intent.expectedPayloadLength() <= config.inlinePayloadThresholdBytes()
                || intent.expectedPayloadLength() > config.maxPayloadBytes()) {
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_TOO_LARGE);
        }
        if (intent.reservationTtlMs() > config.maxReservationTtlMs()) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
        if (getMessage(command.delayMessageId()) != null) {
            return persistRejected(command, sourcePosition, StableCode.DELAY_MESSAGE_ID_CONFLICT);
        }
        final var existingLane = readLane(intent.laneId());
        if (existingLane != null && existingLane.admissionGate() != AdmissionGate.OPEN) {
            final StableCode code = existingLane.admissionGate() == AdmissionGate.RETIRED
                    ? StableCode.LANE_TERMINALLY_CLOSED : StableCode.LANE_CLOSED;
            return persistRejected(command, sourcePosition, code);
        }
        final boolean newLane = existingLane == null;
        if (newLane && quota.laneCount() >= config.maxLanes()) {
            return persistRejected(command, sourcePosition, StableCode.DESTINATION_LANE_LIMIT_EXCEEDED);
        }
        final long accountedBytes = Math.addExact(quota.pendingBytes(), quota.reservationBytes());
        final long accountedMessages = Math.addExact(quota.pendingMessages(), quota.reservationMessages());
        if (accountedMessages >= config.maxPendingMessages()
                || intent.expectedPayloadLength() > config.maxPendingBytes() - accountedBytes) {
            return persistRejected(command, sourcePosition, StableCode.HARD_QUOTA_EXCEEDED);
        }
        final long expiry = Math.addExact(sourcePosition.brokerPersistenceTimeEpochMs(), intent.reservationTtlMs());
        final byte[] reservationId = reservationId(command);
        final PayloadReservation reservation = new PayloadReservation(store.shardId(), reservationId,
                command.commandId(), command.delayMessageId(), command.commandHash(), intent, expiry,
                PayloadReservationStatus.RESERVED, 1, sourcePosition.canonicalBytes(), null);
        final ShardQuota nextQuota = quota.addReservation(intent.expectedPayloadLength(), newLane);
        final CommandResult result = applied(StableCode.OK, sourcePosition, null);
        persistMutation(command, sourcePosition, result, null, reservation, nextQuota);
        return result;
    }

    private CommandResult applyCommitLarge(final PreparedCommand command, final SourcePosition sourcePosition) {
        final PayloadCommitProof proof = CommandBodies.decodeCommitLarge(command.canonicalBody());
        final PayloadReservation reservation = getReservation(proof.reservationId());
        if (reservation == null || !reservation.delayMessageId().equals(command.delayMessageId())) {
            return persistRejected(command, sourcePosition, StableCode.RESERVATION_NOT_COMMITTED);
        }
        if (reservation.status() == PayloadReservationStatus.COMMITTED) {
            if (reservation.committedPayload() != null && proofMatches(proof, reservation.committedPayload())) {
                final CommandResult result = applied(StableCode.ALREADY_COMMITTED, sourcePosition, null);
                persistResultAndPosition(command, sourcePosition, result, null);
                return result;
            }
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_COMMIT_CONFLICT);
        }
        if (reservation.status() == PayloadReservationStatus.ABANDONED) {
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_RESERVATION_CLOSED);
        }
        if (reservation.status() == PayloadReservationStatus.EXPIRED) {
            return persistRejected(command, sourcePosition, StableCode.RESERVATION_EXPIRED);
        }
        if (sourcePosition.brokerPersistenceTimeEpochMs() > reservation.reservationExpiryEpochMs()
                || sourcePosition.brokerPersistenceTimeEpochMs() > proof.notAfterEpochMs()
                || proof.notAfterEpochMs() > reservation.reservationExpiryEpochMs()
                || proof.trustSetVersion() != reservation.intent().payloadProofTrustSetVersion()
                || !java.util.Arrays.equals(proof.routeIncarnationUuid(), store.shardId().routeIncarnation().bytes())
                || proof.partition() != store.shardId().partition()
                || !proof.delayMessageId().equals(command.delayMessageId())
                || proof.length() != reservation.intent().expectedPayloadLength()
                || !Bytes.constantTimeEquals(proof.payloadSha256(), reservation.intent().payloadSha256())) {
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_PROOF_INVALID);
        }
        if (payloadProofTrustSet == null || !payloadProofTrustSet.verifies(proof)) {
            return persistRejected(command, sourcePosition,
                    StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION);
        }
        final PayloadReference reference = new PayloadReference(proof.objectStoreProfileHash(), proof.container(),
                proof.objectKey(), proof.immutableObjectVersion(), proof.etag(), proof.length(), proof.payloadSha256());
        final MessageRecord message = new MessageRecord(MessageStatus.SCHEDULED, 0, 1,
                reservation.intent().deliverAtEpochMs(), reservation.intent().expireAtEpochMs(),
                reservation.intent().laneId(), reservation.intent().orderingMode(), new byte[0],
                sourcePosition.canonicalBytes(), reference);
        final PayloadReservation committed = new PayloadReservation(reservation.shardId(), reservation.reservationId(),
                reservation.commandId(), reservation.delayMessageId(), reservation.commandHash(), reservation.intent(),
                reservation.reservationExpiryEpochMs(), PayloadReservationStatus.COMMITTED,
                Math.addExact(reservation.stateVersion(), 1), reservation.sourcePosition(), reference);
        final ShardQuota nextQuota = quota.commitReservation(reference.length());
        final CommandResult result = applied(StableCode.SCHEDULED, sourcePosition, message);
        persistMutation(command, sourcePosition, result, message, committed, nextQuota);
        return result;
    }

    private static byte[] reservationId(final PreparedCommand command) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"), command.commandId().bytes(),
                command.delayMessageId().bytes(), command.commandHash());
    }

    private static boolean proofMatches(final PayloadCommitProof proof, final PayloadReference reference) {
        return Bytes.constantTimeEquals(proof.objectStoreProfileHash(), reference.objectStoreProfileHash())
                && java.util.Arrays.equals(proof.container(), reference.container())
                && java.util.Arrays.equals(proof.objectKey(), reference.objectKey())
                && java.util.Arrays.equals(proof.immutableObjectVersion(), reference.immutableObjectVersion())
                && java.util.Arrays.equals(proof.etag(), reference.etag()) && proof.length() == reference.length()
                && Bytes.constantTimeEquals(proof.payloadSha256(), reference.payloadSha256());
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
        final long accountedMessages = Math.addExact(quota.pendingMessages(), quota.reservationMessages());
        final long accountedBytes = Math.addExact(quota.pendingBytes(), quota.reservationBytes());
        if (accountedMessages >= config.maxPendingMessages()
                || intent.payload().length > config.maxPendingBytes() - accountedBytes) {
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
            final PayloadReservation reservation = findReservationForMessage(command.delayMessageId());
            if (reservation != null) {
                final int expectedGeneration = CommandBodies.decodeCancel(command.canonicalBody());
                if (expectedGeneration >= 0 && expectedGeneration != 0) {
                    return applied(StableCode.VERSION_CONFLICT, sourcePosition, null);
                }
                return switch (reservation.status()) {
                    case RESERVED -> applied(StableCode.PAYLOAD_RESERVATION_ABANDONED, sourcePosition, null);
                    case ABANDONED -> applied(StableCode.ALREADY_ABANDONED, sourcePosition, null);
                    case EXPIRED -> applied(StableCode.RESERVATION_EXPIRED, sourcePosition, null);
                    case COMMITTED -> rejected(StableCode.INTEGRITY_ERROR, sourcePosition, -1, 0, null);
                };
            }
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
                            existing.orderingMode(), existing.payload(), existing.scheduleSourcePosition(),
                            existing.payloadReference()));
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
                existing.orderingMode(), existing.payload(), sourcePosition.canonicalBytes(),
                existing.payloadReference());
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
        final PayloadReservation reservation = reservationTransition(command, position, result);
        final ShardQuota nextQuota = reservation == null
                ? quotaAfter(prior, next, result, existingLane) : quota.removeReservation(reservation.intent()
                .expectedPayloadLength());
        persistMutation(command, position, result, next, reservation, nextQuota);
    }

    private PayloadReservation reservationTransition(final PreparedCommand command, final SourcePosition position,
                                                     final CommandResult result) {
        if (command.type() != io.nereusstream.delay.protocol.CommandType.CANCEL
                || result.stableCode() != StableCode.PAYLOAD_RESERVATION_ABANDONED) {
            return null;
        }
        final PayloadReservation current = findReservationForMessage(command.delayMessageId());
        if (current == null || current.status() != PayloadReservationStatus.RESERVED) {
            return null;
        }
        return new PayloadReservation(current.shardId(), current.reservationId(), current.commandId(),
                current.delayMessageId(), current.commandHash(), current.intent(), current.reservationExpiryEpochMs(),
                PayloadReservationStatus.ABANDONED, Math.addExact(current.stateVersion(), 1),
                position.canonicalBytes(), null);
    }

    private void persistMutation(final PreparedCommand command, final SourcePosition position,
                                 final CommandResult result, final MessageRecord next,
                                 final PayloadReservation reservation, final ShardQuota nextQuota) {
        final MessageRecord prior = getMessage(command.delayMessageId());
        final boolean existingLane = next != null && readLane(next.laneId()) != null;
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
            if (reservation != null) {
                batch.putValue(ColumnFamily.ID, 2, KeyCodec.idReservation(reservation.reservationId()),
                        reservation.encode());
                if (reservation.status() == PayloadReservationStatus.RESERVED) {
                    batch.putValue(ColumnFamily.TIMELINE, 5,
                            KeyCodec.reservationExpiry(reservation.reservationExpiryEpochMs(),
                                    reservation.reservationId()), reservation.encode());
                    if (readLane(reservation.intent().laneId()) == null) {
                        batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(reservation.intent().laneId()),
                                LaneRecord.initial(reservation.intent().laneId(), position).encode());
                    }
                } else {
                    batch.delete(ColumnFamily.TIMELINE,
                            KeyCodec.reservationExpiry(reservation.reservationExpiryEpochMs(),
                                    reservation.reservationId()));
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
            return quota.addSchedule(next.payloadLength(), !existingLane);
        }
        if (prior != null && prior.status() == MessageStatus.SCHEDULED && next != null
                && next.status() == MessageStatus.CANCELED) {
            return quota.removeSchedule(prior.payloadLength());
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
                    prior.payload(), prior.scheduleSourcePosition(), prior.payloadReference()) : null;
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
                prior.payload(), position.canonicalBytes(), prior.payloadReference());
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

    private PayloadReservation findReservationForMessage(final DelayMessageId messageId) {
        final int limit = (int) Math.min(config.maxPendingMessages(), Integer.MAX_VALUE);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.ID,
                new byte[]{2, 1}, new byte[]{3, 1}, Math.max(1, limit));
        for (var entry : entries) {
            final PayloadReservation reservation = PayloadReservation.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 2).payload());
            if (reservation.delayMessageId().equals(messageId)) {
                return reservation;
            }
        }
        return null;
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
