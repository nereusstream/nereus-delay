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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
        final TimelineCandidate candidate = findLaneCandidate(laneId, null, -1, null, null);
        final LaneProjection projection = projectLane(laneId, current, next, candidate);
        store.write(batch -> {
            deleteReadyKey(batch, current);
            putReadyProjection(batch, projection);
        });
        return projection.lane();
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

    /**
     * Returns the bounded READY head projection.  A malformed, orphaned, or
     * version-mismatched entry fences discovery instead of silently falling
     * back to a full timeline scan.
     */
    public synchronized List<ReadyWork> discoverReady(final long earliestEpochMs, final int limit) {
        if (earliestEpochMs < 0 || limit <= 0) {
            throw new IllegalArgumentException("invalid READY discovery bounds");
        }
        final List<ReadyWork> result = new ArrayList<>();
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.TIMELINE,
                new byte[]{3, 1}, new byte[]{4, 1}, limit);
        for (var entry : entries) {
            final ReadyKey key = decodeReadyKey(entry.key());
            final ReadyIndexValue value = ReadyIndexValue.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 3).payload());
            if (!key.laneId().equals(value.laneId()) || key.nextEligibleAtEpochMs() != value.nextEligibleAtEpochMs()
                    || key.laneVersion() != value.laneVersion()) {
                throw new IllegalStateException("READY key/value identity mismatch");
            }
            if (key.nextEligibleAtEpochMs() > earliestEpochMs) {
                break;
            }
            final LaneRecord lane = readLane(key.laneId());
            if (lane == null || !lane.schedulable() || lane.laneVersion() != key.laneVersion()
                    || lane.nextEligibleAtEpochMs() != key.nextEligibleAtEpochMs()) {
                throw new IllegalStateException("stale READY lane projection");
            }
            final MessageRecord message = getMessage(value.messageId());
            if (message == null || message.status() != MessageStatus.SCHEDULED
                    || message.generation() != value.generation() || !message.laneId().equals(key.laneId())) {
                throw new IllegalStateException("READY points to non-schedulable message");
            }
            final byte[] timelineKey = timelineKey(value.messageId(), message);
            if (!Bytes.constantTimeEquals(Bytes.sha256(timelineKey), value.timelineKeySha256())) {
                throw new IllegalStateException("READY timeline digest mismatch");
            }
            result.add(new ReadyWork(key.laneId(), value.messageId(), value.generation(),
                    key.nextEligibleAtEpochMs(), key.laneVersion(), message.orderingMode()
                    == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO));
        }
        return List.copyOf(result);
    }

    /**
     * Rebuilds all READY projections while the shard is fenced.  This is the
     * deterministic repair path for startup/recovery; normal command and
     * readiness mutations update the affected projection in their own batch.
     *
     * @return number of schedulable lanes that received a READY key
     */
    public synchronized int rebuildReadyIndexes() {
        final int laneLimit = boundedLimitPlusOne(config.maxLanes());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> laneEntries = store.scan(ColumnFamily.META,
                new byte[]{2, 1}, new byte[]{3, 1}, laneLimit);
        if (laneEntries.size() >= laneLimit && config.maxLanes() < Integer.MAX_VALUE) {
            throw new IllegalStateException("lane metadata exceeds configured maxLanes");
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneRecord> lanes = new HashMap<>();
        for (var entry : laneEntries) {
            final byte[] key = entry.key();
            if (key.length != 2 + 32 || key[0] != 2 || key[1] != 1) {
                throw new IllegalStateException("invalid lane metadata key");
            }
            final io.nereusstream.delay.protocol.DestinationLaneId laneId =
                    new io.nereusstream.delay.protocol.DestinationLaneId(Arrays.copyOfRange(key, 2, 34));
            final LaneRecord lane = LaneRecord.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 2).payload());
            if (!lane.laneId().equals(laneId) || lanes.put(laneId, lane) != null) {
                throw new IllegalStateException("duplicate or mismatched lane metadata");
            }
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, TimelineCandidate> candidates = new HashMap<>();
        for (var laneId : lanes.keySet()) {
            final TimelineCandidate candidate = findLaneCandidate(laneId, null, -1, null, null);
            if (candidate != null) {
                candidates.put(laneId, candidate);
            }
        }
        final int readyLimit = boundedLimitPlusOne(config.maxLanes());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> existingReady = store.scan(
                ColumnFamily.TIMELINE, new byte[]{3, 1}, new byte[]{4, 1}, readyLimit);
        if (existingReady.size() >= readyLimit && config.maxLanes() < Integer.MAX_VALUE) {
            throw new IllegalStateException("READY index exceeds configured maxLanes");
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = new HashMap<>();
        for (var entry : lanes.entrySet()) {
            final TimelineCandidate candidate = candidates.get(entry.getKey());
            projections.put(entry.getKey(), projectLane(entry.getKey(), entry.getValue(), entry.getValue(), candidate));
        }
        store.write(batch -> {
            for (var entry : existingReady) {
                batch.delete(ColumnFamily.TIMELINE, entry.key());
            }
            for (LaneProjection projection : projections.values()) {
                putReadyProjection(batch, projection);
            }
        });
        return (int) projections.values().stream().filter(projection -> projection.readyValue() != null).count();
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
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(position, command.delayMessageId(), prior, next, reservation);
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
                } else {
                    batch.delete(ColumnFamily.TIMELINE,
                            KeyCodec.reservationExpiry(reservation.reservationExpiryEpochMs(),
                                    reservation.reservationId()));
                }
            }
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
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

    private Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> readyProjections(
            final SourcePosition position, final DelayMessageId messageId, final MessageRecord prior,
            final MessageRecord next, final PayloadReservation reservation) {
        final Set<io.nereusstream.delay.protocol.DestinationLaneId> laneIds = new HashSet<>();
        if (prior != null) {
            laneIds.add(prior.laneId());
        }
        if (next != null) {
            laneIds.add(next.laneId());
        }
        if (reservation != null) {
            laneIds.add(reservation.intent().laneId());
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> result = new HashMap<>();
        for (var laneId : laneIds) {
            final LaneRecord previous = readLane(laneId);
            final LaneRecord base = previous == null ? LaneRecord.initial(laneId, position) : previous;
            final int excludedGeneration = prior != null && prior.status() == MessageStatus.SCHEDULED
                    ? prior.generation() : -1;
            final TimelineCandidate candidate = findLaneCandidate(laneId, messageId, excludedGeneration,
                    next != null && next.status() == MessageStatus.SCHEDULED ? messageId : null, next);
            result.put(laneId, projectLane(laneId, previous, base, candidate));
        }
        return result;
    }

    private LaneProjection projectLane(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId,
            final LaneRecord previous, final LaneRecord base, final TimelineCandidate candidate) {
        final long nextEligibleAt = candidate == null ? 0 : candidate.eligibleAtEpochMs();
        final LaneRecord projected = base.nextEligibleAtEpochMs() == nextEligibleAt
                ? base : base.withNextEligibleAt(nextEligibleAt);
        final ReadyIndexValue ready = projected.schedulable() && candidate != null
                ? new ReadyIndexValue(laneId, candidate.eligibleAtEpochMs(), projected.laneVersion(),
                candidate.messageId(), candidate.generation(), Bytes.sha256(candidate.timelineKey())) : null;
        return new LaneProjection(previous, projected, ready);
    }

    private void deleteReadyKey(final ShardStore.Batch batch, final LaneRecord lane) throws org.rocksdb.RocksDBException {
        if (lane != null && lane.schedulable()) {
            batch.delete(ColumnFamily.TIMELINE,
                    KeyCodec.timelineReady(lane.nextEligibleAtEpochMs(), lane.laneId(), lane.laneVersion()));
        }
    }

    private void putReadyProjection(final ShardStore.Batch batch, final LaneProjection projection)
            throws org.rocksdb.RocksDBException {
        batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(projection.lane().laneId()), projection.lane().encode());
        if (projection.readyValue() != null) {
            final ReadyIndexValue ready = projection.readyValue();
            batch.putValue(ColumnFamily.TIMELINE, 3,
                    KeyCodec.timelineReady(ready.nextEligibleAtEpochMs(), ready.laneId(), ready.laneVersion()),
                    ready.encode());
        }
    }

    private TimelineCandidate findLaneCandidate(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId,
            final DelayMessageId excludedMessageId, final int excludedGeneration,
            final DelayMessageId includedMessageId, final MessageRecord includedMessage) {
        TimelineCandidate selected = null;
        if (includedMessage != null && includedMessage.status() == MessageStatus.SCHEDULED
                && includedMessageId != null && includedMessage.laneId().equals(laneId)) {
            selected = new TimelineCandidate(includedMessageId, includedMessage.generation(),
                    includedMessage.deliverAtEpochMs(), timelineKey(includedMessageId, includedMessage),
                    includedMessage.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO);
        }
        for (byte tag = 1; tag <= 2; tag++) {
            final byte[] prefix = Bytes.concat(new byte[]{tag, 1}, laneId.bytes());
            final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.TIMELINE,
                    prefix, prefixUpperBound(prefix), boundedLimit(config.maxPendingMessages()));
            for (var entry : entries) {
                final TimelineCandidate candidate = decodeTimelineCandidate(entry, tag, laneId);
                if (excludedMessageId != null && candidate.messageId().equals(excludedMessageId)
                        && candidate.generation() == excludedGeneration) {
                    continue;
                }
                if (selected == null || candidate.compareTo(selected) < 0) {
                    selected = candidate;
                }
            }
        }
        return selected;
    }

    private TimelineCandidate decodeTimelineCandidate(
            final io.nereusstream.delay.store.ShardStore.KeyValue entry, final byte tag,
            final io.nereusstream.delay.protocol.DestinationLaneId expectedLane) {
        final byte[] key = entry.key();
        final int tokenOffset = 2 + 32 + 8;
        final int tokenLength = key.length > tokenOffset && key[tokenOffset] == 1 ? 9
                : key.length > tokenOffset && key[tokenOffset] == 2 ? 21 : -1;
        if (tokenLength < 0 || key.length != tokenOffset + tokenLength + DelayMessageId.LENGTH + 4
                || key[0] != tag || key[1] != 1) {
            throw new IllegalStateException("invalid timeline key for READY projection");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final byte[] laneBytes = new byte[32];
        input.get(laneBytes);
        final io.nereusstream.delay.protocol.DestinationLaneId lane =
                new io.nereusstream.delay.protocol.DestinationLaneId(laneBytes);
        if (!lane.equals(expectedLane)) {
            throw new IllegalStateException("timeline lane prefix mismatch");
        }
        final long eligibleAt = input.getLong();
        input.position(input.position() + tokenLength);
        final byte[] messageBytes = new byte[DelayMessageId.LENGTH];
        input.get(messageBytes);
        final int generation = input.getInt();
        final DelayMessageId messageId = new DelayMessageId(messageBytes);
        final TimelineEntry timeline = TimelineEntry.decode(
                io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1).payload());
        if (!timeline.messageId().equals(messageId) || timeline.generation() != generation) {
            throw new IllegalStateException("timeline key/value identity mismatch during READY rebuild");
        }
        final MessageRecord message = getMessage(messageId);
        if (message == null || message.status() != MessageStatus.SCHEDULED || message.generation() != generation
                || !message.laneId().equals(expectedLane)) {
            throw new IllegalStateException("timeline points to a non-current scheduled message");
        }
        final boolean ordered = message.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO;
        if ((tag == 2) != ordered) {
            throw new IllegalStateException("timeline namespace does not match ordering mode");
        }
        return new TimelineCandidate(messageId, generation, eligibleAt, key, ordered);
    }

    private static byte[] prefixUpperBound(final byte[] prefix) {
        final byte[] result = Bytes.copy(prefix);
        for (int index = result.length - 1; index >= 0; index--) {
            if ((result[index] & 0xff) != 0xff) {
                result[index]++;
                return Arrays.copyOf(result, index + 1);
            }
        }
        return null;
    }

    private static int boundedLimit(final long configured) {
        return (int) Math.max(1, Math.min(configured, Integer.MAX_VALUE));
    }

    private static int boundedLimitPlusOne(final long configured) {
        return configured >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(1, configured + 1);
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

    private static ReadyKey decodeReadyKey(final byte[] key) {
        if (key.length != 2 + 8 + 32 + 8 || key[0] != 3 || key[1] != 1) {
            throw new IllegalStateException("invalid READY key length or tag");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final long nextEligibleAt = input.getLong();
        final byte[] laneBytes = new byte[32];
        input.get(laneBytes);
        final long laneVersion = input.getLong();
        return new ReadyKey(new io.nereusstream.delay.protocol.DestinationLaneId(laneBytes), nextEligibleAt,
                laneVersion);
    }

    private record ReadyKey(io.nereusstream.delay.protocol.DestinationLaneId laneId,
                            long nextEligibleAtEpochMs, long laneVersion) {
    }

    private record TimelineCandidate(DelayMessageId messageId, int generation, long eligibleAtEpochMs,
                                     byte[] timelineKey, boolean ordered) implements Comparable<TimelineCandidate> {
        private TimelineCandidate {
            timelineKey = Bytes.copy(timelineKey);
        }

        @Override
        public byte[] timelineKey() {
            return Bytes.copy(timelineKey);
        }

        @Override
        public int compareTo(final TimelineCandidate other) {
            int result = Long.compare(eligibleAtEpochMs, other.eligibleAtEpochMs);
            if (result != 0) {
                return result;
            }
            return compareUnsigned(timelineKey, other.timelineKey);
        }
    }

    private record LaneProjection(LaneRecord previousLane, LaneRecord lane, ReadyIndexValue readyValue) {
    }

    private static int compareUnsigned(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int result = Integer.compare(left[index] & 0xff, right[index] & 0xff);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(left.length, right.length);
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

    public record ReadyWork(io.nereusstream.delay.protocol.DestinationLaneId laneId,
                            DelayMessageId messageId, int generation, long nextEligibleAtEpochMs,
                            long laneVersion, boolean ordered) {
        public ReadyWork {
            Objects.requireNonNull(laneId, "laneId");
            Objects.requireNonNull(messageId, "messageId");
            if (generation < 0 || nextEligibleAtEpochMs < 0 || laneVersion < 0) {
                throw new IllegalArgumentException("invalid READY work");
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
