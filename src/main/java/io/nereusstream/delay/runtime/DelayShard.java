package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.CommandBodies;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.ClaimResultBody;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.PayloadCommitProof;
import io.nereusstream.delay.protocol.PayloadProofTrustSet;
import io.nereusstream.delay.protocol.PayloadReference;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ReplayDeadLetterBody;
import io.nereusstream.delay.protocol.ResolveUncertainBody;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationBodyCodec;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.ShardStore;

import java.nio.ByteBuffer;
import java.security.PublicKey;
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
    private static final int META_CLAIM_SEQUENCE = 11;
    private static final int META_QUOTA_USAGE = 1;
    private static final byte INFLIGHT_CLAIMED_KIND = 1;
    private static final byte INFLIGHT_PUBLISHING_KIND = 2;
    private static final byte INFLIGHT_UNCERTAIN_KIND = 3;

    private final ShardStore store;
    private final DelayShardConfig config;
    private final PayloadProofTrustSet payloadProofTrustSet;
    private SourcePosition lastAppliedSourcePosition;
    private long mutationSequence;
    private long claimSequence;
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
        final var claimSequenceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_CLAIM_SEQUENCE), 1);
        claimSequence = claimSequenceValue == null ? 0 : readSequence(claimSequenceValue.payload());
        final var quotaValue = store.getValue(ColumnFamily.META, KeyCodec.metaQuota(META_QUOTA_USAGE), 7);
        quota = quotaValue == null ? ShardQuota.empty() : ShardQuota.decode(quotaValue.payload());
        validateRuntimeObligationIndexes();
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

    /** Returns the exact local Claim at an Owner Epoch, or {@code null} when it is no longer live. */
    public synchronized ClaimRecord getClaim(final byte[] claimId, final long ownerEpoch) {
        Bytes.requireLength(claimId, ClaimRecord.HASH_LENGTH, "claimId");
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        final byte[] key = KeyCodec.inflight(INFLIGHT_CLAIMED_KIND, ownerEpoch, claimId);
        final var value = store.getValue(ColumnFamily.INFLIGHT, key, ClaimRecord.VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final ClaimRecord claim = ClaimRecord.decode(value.payload());
        validateClaimKey(claim, key, claimId, ownerEpoch);
        return claim;
    }

    /**
     * Finds the one live Claim for a Message Identity without trusting an Owner Epoch.
     * A duplicate or over-bound scan fences the caller instead of guessing.
     */
    public synchronized ClaimRecord findClaimForMessage(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{INFLIGHT_CLAIMED_KIND, 1}, new byte[]{INFLIGHT_PUBLISHING_KIND, 1}, limit);
        ClaimRecord found = null;
        for (var entry : entries) {
            final ClaimRecord candidate = decodeClaim(entry);
            if (candidate.delayMessageId().equals(messageId)) {
                if (found != null) {
                    throw new IllegalStateException("message has multiple live Claims");
                }
                found = candidate;
            }
        }
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("Claim scan exceeded configured bound");
        }
        return found;
    }

    /** Returns the next local Claim sequence persisted by this shard. */
    public synchronized long claimSequence() {
        return claimSequence;
    }

    /**
     * Atomically takes a scheduled timeline item into a reversible local Claim.
     * This embedded method deliberately exposes no Producer call: admission must
     * later be represented by the source-ordered PUBLISH_ADMISSION mutation.
     */
    public synchronized ClaimRecord claimForPublish(final DelayMessageId messageId, final AuthorIdentity owner,
                                                     final long claimDeadlineEpochMs, final byte[] materialization,
                                                     final byte[] claimedCharge) {
        Objects.requireNonNull(messageId, "messageId");
        Objects.requireNonNull(owner, "owner");
        owner.requireFor(SystemMutationType.CLAIM_RESULT);
        if (claimDeadlineEpochMs < 0) {
            throw new IllegalArgumentException("claim deadline must be non-negative");
        }
        final MessageRecord current = getMessage(messageId);
        if (current == null || current.status() != MessageStatus.SCHEDULED) {
            throw new IllegalStateException("only a scheduled message can be Claimed");
        }
        if (claimDeadlineEpochMs > current.expireAtEpochMs()) {
            throw new IllegalArgumentException("claim deadline exceeds message expiry");
        }
        final LaneRecord lane = readLane(current.laneId());
        if (lane == null || !lane.schedulable()) {
            throw new IllegalStateException("Claim requires a schedulable lane");
        }
        final byte[] timelineKey = timelineKey(messageId, current);
        final long nextClaimSequence = Math.addExact(claimSequence, 1);
        final byte[] claimId = Bytes.sha256(Bytes.utf8("nereus-delay-claim-id-v1\0"),
                store.metadata().storeIncarnation(), Bytes.u64be(owner.generation()), Bytes.u64be(nextClaimSequence),
                messageId.bytes(), Bytes.u32be(current.generation()), Bytes.u64be(lane.laneVersion()));
        final int workKind = current.retryEligibilityAtEpochMs() == current.deliverAtEpochMs() ? 1 : 2;
        final byte[] precondition = buildClaimPrecondition(claimId, messageId, current, lane, timelineKey,
                owner, claimDeadlineEpochMs, materialization, claimedCharge, workKind);
        MessageRecord next = new MessageRecord(MessageStatus.CLAIMED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs());
        final ClaimRecord claim = ClaimRecord.claimed(messageId, current.generation(), claimId, owner.generation(),
                nextClaimSequence, current.laneId(), lane.laneIncarnation(), lane.laneControlVersion(),
                lane.laneVersion(), owner.canonicalBytes(), store.metadata().storeIncarnation(), precondition,
                timelineKey, next.stateVersion());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.claimed(claim.claimId(), current.runtimeIndex()
                .attemptObligations(), current.runtimeIndex().admissionsUsed(),
                current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(), next.stateVersion()));
        final MessageRecord claimedNext = next;
        final SourcePosition schedulePosition = SourcePositionCodec.decode(current.scheduleSourcePosition());
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                schedulePosition, messageId, current, next, null);
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, timelineKey);
            batch.putValue(ColumnFamily.INFLIGHT, ClaimRecord.VALUE_TYPE, claim.encodedKey(), claim.encode());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), claimedNext.encode());
            batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(META_CLAIM_SEQUENCE),
                    Bytes.u64be(nextClaimSequence));
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
        });
        claimSequence = nextClaimSequence;
        return claim;
    }

    /** Atomically revokes a local Claim and restores its exact timeline work. */
    public synchronized MessageRecord revokeClaim(final byte[] claimId, final long ownerEpoch) {
        final ClaimRecord claim = getClaim(claimId, ownerEpoch);
        if (claim == null) {
            return null;
        }
        final MessageRecord current = getMessage(claim.delayMessageId());
        if (current == null || current.status() != MessageStatus.CLAIMED
                || current.generation() != claim.generation()
                || current.stateVersion() != claim.runtimeRevision()) {
            throw new IllegalStateException("Claim does not match current CLAIMED message");
        }
        MessageRecord next = new MessageRecord(MessageStatus.SCHEDULED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs());
        final ClaimResultBody.ClaimPrecondition precondition =
                ClaimResultBody.decodePrecondition(claim.preconditionBytes());
        final TimelineWorkKind workKind = precondition.sourceWorkKind() == 1
                ? TimelineWorkKind.INITIAL_SCHEDULE : TimelineWorkKind.DEFINITIVE_RETRY;
        next = next.withRuntimeIndex(timelineRuntimeIndex(claim.delayMessageId(), next, workKind,
                Math.addExact(current.runtimeIndex().admissionsUsed(), 1), next.stateVersion(),
                UncertainRetryAuthority.NONE, null, null, current.runtimeIndex()));
        final MessageRecord revokedNext = next;
        final SourcePosition schedulePosition = SourcePositionCodec.decode(current.scheduleSourcePosition());
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                schedulePosition, claim.delayMessageId(), current, next, null);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, claim.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(claim.delayMessageId()), revokedNext.encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, claim.timelineKey(),
                    new TimelineEntry(claim.delayMessageId(), revokedNext.generation()).encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(claim.delayMessageId(), revokedNext),
                    new TimelineEntry(claim.delayMessageId(), revokedNext.generation()).encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
        });
        return next;
    }

    public synchronized PayloadReservation getReservation(final byte[] reservationId) {
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idReservation(reservationId), 2);
        return value == null ? null : PayloadReservation.decode(value.payload());
    }

    public synchronized CommandResult getCommandResult(final CommandId commandId) {
        final var value = store.getValue(ColumnFamily.DEDUPE, KeyCodec.dedupeResult(commandId), 2);
        return value == null ? null : CommandResult.decode(value.payload());
    }

    public synchronized SystemMutationResult getSystemMutationResult(final byte[] mutationId) {
        Bytes.requireLength(mutationId, SystemMutation.HASH_LENGTH, "mutationId");
        final var value = store.getValue(ColumnFamily.DEDUPE, KeyCodec.dedupeSystemMutation(mutationId),
                SystemMutationResult.VALUE_TYPE);
        return value == null ? null : SystemMutationResult.decode(value.payload());
    }

    /**
     * Applies the source-ordered System Mutation subset that is currently executable by this core.
     * Signature verification is deliberately explicit; production wiring must additionally supply the
     * source-protected key/ACL set before calling this method.
     */
    public synchronized SystemMutationResult applySystemMutation(final SystemMutation mutation,
                                                                  final SourcePosition sourcePosition,
                                                                  final PublicKey verificationKey) {
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(verificationKey, "verificationKey");
        validateMutationShard(mutation, sourcePosition);
        final SystemMutationResult prior = getSystemMutationResult(mutation.systemMutationId());
        if (prior != null) {
            if (!Bytes.constantTimeEquals(prior.mutationHash(), mutation.mutationHash())
                    || prior.mutationType() != mutation.type()
                    || prior.retryUntilEpochMs() != mutation.retryUntilEpochMs()
                    || !Bytes.constantTimeEquals(prior.authorIdentity(), mutation.authorIdentity())) {
                throw new IllegalStateException("System Mutation identity was reused with different bytes");
            }
            if (lastAppliedSourcePosition != null) {
                final int order = sourcePosition.compareTo(lastAppliedSourcePosition);
                if (order < 0) {
                    throw new IllegalStateException("System Mutation source position regressed");
                }
                if (order == 0 && !Arrays.equals(prior.appliedSourcePosition(), sourcePosition.canonicalBytes())) {
                    throw new IllegalStateException("duplicate source position has conflicting System Mutation");
                }
            }
            if (!Arrays.equals(prior.appliedSourcePosition(), sourcePosition.canonicalBytes())) {
                store.write(batch -> writePosition(batch, sourcePosition));
                lastAppliedSourcePosition = sourcePosition;
                mutationSequence++;
            }
            return prior;
        }
        validateMutationPosition(sourcePosition);
        if (!mutation.verifySignature(verificationKey)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (sourcePosition.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        try {
            return switch (mutation.type()) {
                case PUBLISH_ADMISSION -> applyPublishAdmissionMutation(mutation, sourcePosition);
                case REPLAY_DEAD_LETTER -> applyReplayDeadLetterMutation(mutation, sourcePosition);
                case EXPIRE_GENERATION -> applyExpireGenerationMutation(mutation, sourcePosition);
                case PUBLISH_OUTCOME -> applyPublishOutcomeMutation(mutation, sourcePosition);
                case EVIDENCE_RESOLUTION -> applyEvidenceResolutionMutation(mutation, sourcePosition);
                case RESOLVE_UNCERTAIN -> applyResolveUncertainMutation(mutation, sourcePosition);
                case CLAIM_RESULT -> applyClaimResultMutation(mutation, sourcePosition);
                default -> throw new UnsupportedOperationException(
                        "System Mutation type is not implemented: " + mutation.type());
            };
        } catch (IllegalArgumentException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    private SystemMutationResult applyPublishAdmissionMutation(final SystemMutation mutation,
                                                                final SourcePosition sourcePosition) {
        final PublishAdmissionBody body = PublishAdmissionBody.decode(mutation.canonicalBody());
        final io.nereusstream.delay.protocol.AuthorIdentity author =
                io.nereusstream.delay.protocol.AuthorIdentity.decode(mutation.authorIdentity());
        if (!Arrays.equals(body.ownerIdentity(), author.canonicalBytes())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        body.requireTiming(body.descriptor().actionAtEpochMs(), body.descriptor().expireAtEpochMs());
        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final io.nereusstream.delay.protocol.DestinationLaneId laneId =
                new io.nereusstream.delay.protocol.DestinationLaneId(body.laneId());
        final PublishAttemptLedger open = findOpenPublishAttempt(body.publishAttemptId());
        if (open != null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final MessageRecord current = getMessage(messageId);
        if (current == null || (current.status() != MessageStatus.SCHEDULED
                && current.status() != MessageStatus.CLAIMED)
                || current.generation() != body.generation() || !current.laneId().equals(laneId)
                || current.deliverAtEpochMs() != body.descriptor().deliverAtEpochMs()
                || current.expireAtEpochMs() != body.descriptor().expireAtEpochMs()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final LaneRecord lane = readLane(laneId);
        if (lane == null || !lane.schedulable()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final ClaimRecord localClaim = current.status() == MessageStatus.CLAIMED
                ? getClaim(body.claimId(), author.generation()) : null;
        final AdmissionReplayState replayState;
        try {
            replayState = validatePublishAdmissionReplayState(body, current, lane, localClaim);
        } catch (IllegalArgumentException | IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        if (current.status() == MessageStatus.CLAIMED) {
            if (localClaim != null && (!localClaim.delayMessageId().equals(messageId)
                    || localClaim.generation() != body.generation()
                    || !Arrays.equals(localClaim.preconditionBytes(), body.claimPrecondition().canonicalBytes()))) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                        StableCode.STALE_SYSTEM_MUTATION);
            }
        }
        final PublishAttemptLedger admission = PublishAttemptLedger.publishing(messageId, body.generation(),
                body.publishAttemptId(), body.claimId(), author.generation(), body.descriptor().attemptNo(), laneId,
                body.laneIncarnation(), body.ownerIdentity(), body.storeIncarnation(), body.preparedPublishHash(),
                mutation.canonicalBody(), sourcePosition.canonicalBytes());
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        try {
            admitPublishAttempt(admission, sourcePosition, result, replayState.claimMayBeMissing(),
                    replayState.uncertainRetryAdmission());
            return result;
        } catch (IllegalArgumentException | IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    private SystemMutationResult applyPublishOutcomeMutation(final SystemMutation mutation,
                                                              final SourcePosition sourcePosition) {
        final List<io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.PUBLISH_OUTCOME, mutation.canonicalBody());
        final byte[] attemptId = fixedBodyBytes(field(fields, 10), 10, PublishAttemptLedger.HASH_LENGTH);
        final int sideEffect = bodyInt(field(fields, 11), 11);
        final int disposition = bodyInt(field(fields, 12), 12);
        final StableCode code = StableCode.fromWire(bodyInt(field(fields, 13), 13));
        final byte[] evidence = optionalBodyBytes(fields, 14);
        final io.nereusstream.delay.protocol.AuthorIdentity author =
                io.nereusstream.delay.protocol.AuthorIdentity.decode(mutation.authorIdentity());
        if (sideEffect == 2) {
            final PublishOutcomeBody outcome = PublishOutcomeBody.decode(mutation.canonicalBody());
            if (!Arrays.equals(outcome.publishAttemptId(), attemptId)) {
                throw new IllegalArgumentException("Publish Outcome attempt identity mismatch");
            }
            final PublishAttemptLedger ledger = getPublishAttempt(attemptId, author.generation());
            if (ledger == null || ledger.state() != AttemptLedgerState.PUBLISHING) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                        StableCode.STALE_SYSTEM_MUTATION);
            }
            if (ledger.ownerEpoch() != author.generation()) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, code,
                    sourcePosition.canonicalBytes());
            return applyNotPublishedPublishOutcome(ledger, outcome, sourcePosition, result,
                    AttemptLedgerState.PUBLISHING, MessageStatus.PUBLISHING);
        }
        final PublishAttemptLedger ledger = getPublishAttempt(attemptId, author.generation());
        if (ledger == null || ledger.state() != AttemptLedgerState.PUBLISHING) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        if (sideEffect == 1) {
            final PublishOutcomeBody outcome = PublishOutcomeBody.decode(mutation.canonicalBody());
            if (!Arrays.equals(outcome.publishAttemptId(), attemptId)) {
                throw new IllegalArgumentException("Publish Outcome attempt identity mismatch");
            }
            final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, code,
                    sourcePosition.canonicalBytes());
            try {
                applyPublishedPublishOutcome(attemptId, author.generation(), sourcePosition, result);
                return result;
            } catch (IllegalStateException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }
        if (sideEffect == 3) {
            if (disposition == 0 || code == StableCode.OK || evidence.length != 0) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.STALE_SYSTEM_MUTATION);
            }
            final PublishOutcomeBody outcome = PublishOutcomeBody.decode(mutation.canonicalBody());
            final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, code,
                    sourcePosition.canonicalBytes());
            applyUnknownPublishOutcome(attemptId, author.generation(), mutation.canonicalBody(), evidence,
                    sourcePosition, result, outcome.retryDecision());
            return result;
        }
        throw new UnsupportedOperationException("NOT_PUBLISHED outcome application is not implemented");
    }

    private SystemMutationResult applyEvidenceResolutionMutation(final SystemMutation mutation,
                                                                  final SourcePosition sourcePosition) {
        final PublishOutcomeBody resolution =
                PublishOutcomeBody.decodeEvidenceResolution(mutation.canonicalBody());
        final PublishAttemptLedger ledger = findOpenPublishAttempt(resolution.publishAttemptId());
        if (ledger == null || ledger.state() != AttemptLedgerState.UNCERTAIN) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                resolution.stableCode(), sourcePosition.canonicalBytes());
        if (resolution.sideEffect() == 1) {
            try {
                applyPublishedPublishOutcome(ledger, sourcePosition, result, MessageStatus.UNCERTAIN);
                return result;
            } catch (IllegalStateException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }
        return applyNotPublishedPublishOutcome(ledger, resolution, sourcePosition, result,
                AttemptLedgerState.UNCERTAIN, MessageStatus.UNCERTAIN);
    }

    /**
     * Applies the source-ordered RETRY_ALLOW_POSSIBLE_DUPLICATE Resolve subset.
     * Evidence attachment and possible-delivery terminalization remain explicit
     * fail-closed branches until their dedicated evidence/terminal codecs land.
     */
    private SystemMutationResult applyResolveUncertainMutation(final SystemMutation mutation,
                                                                final SourcePosition sourcePosition) {
        final ResolveUncertainBody body = ResolveUncertainBody.decode(mutation.canonicalBody());
        if (!Arrays.equals(mutation.logicalOperationIdentity(), body.controlRef()
                .logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN))) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (body.resolutionKind() != 3) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    current.generation() > body.generation()
                            ? StableCode.GENERATION_SUPERSEDED : StableCode.STALE_SYSTEM_MUTATION);
        }
        if (!current.laneId().equals(body.laneId())
                || current.status() != MessageStatus.UNCERTAIN
                || current.runtimeIndex().currentWorkKind() != CurrentSendWorkKind.NONE
                || current.orderingMode() != io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    current.orderingMode() != io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
                            ? StableCode.ORDERING_DOMAIN_BROKEN : StableCode.TOO_LATE);
        }
        final LaneRecord lane = readLane(body.laneId());
        if (lane == null || !Arrays.equals(lane.laneIncarnation(), body.laneIncarnation())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        if (lane.admissionGate() == AdmissionGate.ORDERING_BROKEN) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.ORDERING_DOMAIN_BROKEN);
        }
        if (lane.admissionGate() == AdmissionGate.CLOSED || lane.admissionGate() == AdmissionGate.RETIRED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.LANE_CLOSED);
        }
        final AttemptObligationRef target = current.runtimeIndex().attemptObligations().stream()
                .filter(ref -> Arrays.equals(ref.publishAttemptId(), body.publishAttemptId())
                        && ref.generation() == body.generation()
                        && ref.ledgerState() == AttemptLedgerState.UNCERTAIN)
                .findFirst().orElse(null);
        if (target == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final PublishAttemptLedger ledger = readLedgerForObligation(target);
        if (ledger.state() != AttemptLedgerState.UNCERTAIN
                || !ledger.delayMessageId().equals(body.messageId())
                || !ledger.laneId().equals(body.laneId())
                || ledger.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final int candidateAttemptNo;
        try {
            candidateAttemptNo = Math.addExact(current.runtimeIndex().admissionsUsed(), 1);
        } catch (ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.TOO_LATE);
        }
        if (current.runtimeIndex().admissionsUsed() >= config.maxPublishAdmissions()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.TOO_LATE);
        }
        final long retryAt = Math.max(current.deliverAtEpochMs(), sourcePosition.brokerPersistenceTimeEpochMs());
        if (retryAt >= current.expireAtEpochMs()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.TOO_LATE);
        }
        MessageRecord scheduled = new MessageRecord(MessageStatus.SCHEDULED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), retryAt);
        scheduled = scheduled.withRuntimeIndex(timelineRuntimeIndex(body.messageId(), scheduled,
                TimelineWorkKind.UNCERTAIN_RETRY, candidateAttemptNo, scheduled.stateVersion(),
                UncertainRetryAuthority.CONTROL_OVERRIDE, body.controlRef().canonicalBytes(),
                sourcePosition.canonicalBytes(), current.runtimeIndex(), current.runtimeIndex().attemptObligations()));
        final MessageRecord scheduledForWrite = scheduled;
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, body.messageId(), current, scheduled, null);
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(body.messageId()), scheduledForWrite.encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey(body.messageId(), scheduledForWrite),
                    new TimelineEntry(body.messageId(), scheduledForWrite.generation()).encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(body.messageId(), scheduledForWrite),
                    new TimelineEntry(body.messageId(), scheduledForWrite.generation()).encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    /** Applies the bounded source-ordered Dead Letter replay generation transition. */
    private SystemMutationResult applyReplayDeadLetterMutation(final SystemMutation mutation,
                                                                final SourcePosition sourcePosition) {
        final ReplayDeadLetterBody body = ReplayDeadLetterBody.decode(mutation.canonicalBody());
        if (!Arrays.equals(mutation.logicalOperationIdentity(), body.controlRef()
                .logicalOperationIdentity(SystemMutationType.REPLAY_DEAD_LETTER))) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.NOT_FOUND);
        }
        if (current.generation() != body.expectedGeneration()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    current.generation() > body.expectedGeneration()
                            ? StableCode.GENERATION_SUPERSEDED : StableCode.STALE_SYSTEM_MUTATION);
        }
        if (current.stateVersion() != body.expectedStateVersion()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.VERSION_CONFLICT);
        }
        if (current.status() != MessageStatus.DEAD_LETTER) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    current.status() == MessageStatus.PUBLISHED
                            ? StableCode.ALREADY_PUBLISHED : StableCode.TOO_LATE);
        }
        final TerminalGenerationRecord summary = getTerminalGeneration(body.messageId(), body.expectedGeneration());
        if (summary == null || summary.status() != MessageStatus.DEAD_LETTER) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        final boolean needsDuplicateAcknowledgement = summary.possibleDestinationDuplicate()
                || !summary.openObligations().isEmpty();
        if (needsDuplicateAcknowledgement != body.allowPossibleDuplicate()
                || needsDuplicateAcknowledgement && body.acknowledgementHash().length == 0) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.TOO_LATE);
        }
        final LaneRecord lane = readLane(current.laneId());
        if (lane == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        if (lane.admissionGate() == AdmissionGate.CLOSED || lane.admissionGate() == AdmissionGate.RETIRED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.LANE_CLOSED);
        }
        if (lane.admissionGate() == AdmissionGate.ORDERING_BROKEN) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.ORDERING_DOMAIN_BROKEN);
        }
        if (body.expireAtEpochMs() <= sourcePosition.brokerPersistenceTimeEpochMs()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.INVALID_DELIVERY_WINDOW);
        }
        final ShardQuota nextQuota;
        try {
            final long accountedBytes = Math.addExact(quota.pendingBytes(), quota.reservationBytes());
            final long accountedMessages = Math.addExact(quota.pendingMessages(), quota.reservationMessages());
            if (accountedMessages >= config.maxPendingMessages()
                    || current.payloadLength() > config.maxPendingBytes() - accountedBytes) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                        StableCode.HARD_QUOTA_EXCEEDED);
            }
            nextQuota = quota.addSchedule(current.payloadLength(), false);
        } catch (ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.HARD_QUOTA_EXCEEDED);
        }
        final int nextGeneration;
        try {
            nextGeneration = Math.addExact(current.generation(), 1);
        } catch (ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        MessageRecord next = new MessageRecord(MessageStatus.SCHEDULED, nextGeneration,
                Math.addExact(current.stateVersion(), 1), body.deliverAtEpochMs(), body.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), sourcePosition.canonicalBytes(),
                current.payloadReference(), body.deliverAtEpochMs());
        next = next.withRuntimeIndex(timelineRuntimeIndex(body.messageId(), next,
                TimelineWorkKind.INITIAL_SCHEDULE, 1, next.stateVersion(), UncertainRetryAuthority.NONE,
                null, null));
        final MessageRecord nextForWrite = next;
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, body.messageId(), current, next, null);
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(body.messageId()), nextForWrite.encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey(body.messageId(), nextForWrite),
                    new TimelineEntry(body.messageId(), nextGeneration).encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(body.messageId(), nextForWrite),
                    new TimelineEntry(body.messageId(), nextGeneration).encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        quota = nextQuota;
        return result;
    }

    /**
     * Applies the replay-stable CLAIM_RESULT_V1 subset.  A locally persisted
     * Claim is consumed by exact precondition/instance identity; after replay,
     * the source-derived SCHEDULED fallback remains accepted when the Claim
     * record itself was not present in the restored checkpoint.  The full
     * GenerationRuntimeIndex/obligation model is still pending.  This never
     * treats a callback as a direct terminal write: the result, terminal
     * projection, quota transfer, indexes, and source position share one
     * synchronous batch.
     */
    private SystemMutationResult applyClaimResultMutation(final SystemMutation mutation,
                                                           final SourcePosition sourcePosition) {
        final ClaimResultBody body = ClaimResultBody.decode(mutation.canonicalBody());
        final io.nereusstream.delay.protocol.AuthorIdentity author =
                io.nereusstream.delay.protocol.AuthorIdentity.decode(mutation.authorIdentity());
        if (!Arrays.equals(author.canonicalBytes(), body.precondition().ownerIdentity())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }

        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final MessageRecord current = getMessage(messageId);
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.NOT_FOUND);
        }
        if (current.generation() != body.generation()) {
            final StableCode code = current.generation() > body.generation()
                    ? StableCode.GENERATION_SUPERSEDED : StableCode.STALE_SYSTEM_MUTATION;
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, code);
        }
        if (current.status() != MessageStatus.SCHEDULED && current.status() != MessageStatus.CLAIMED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }

        final ClaimResultBody.ClaimPrecondition precondition = body.precondition();
        final ClaimRecord currentClaim;
        final byte[] sourceTimelineKey;
        if (current.status() == MessageStatus.CLAIMED) {
            currentClaim = getClaim(body.claimId(), author.generation());
            if (currentClaim == null || !Arrays.equals(currentClaim.preconditionBytes(), precondition.canonicalBytes())
                    || currentClaim.runtimeRevision() != current.stateVersion()) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                        StableCode.STALE_SYSTEM_MUTATION);
            }
            sourceTimelineKey = currentClaim.timelineKey();
        } else {
            currentClaim = null;
            sourceTimelineKey = timelineKey(messageId, current);
        }
        final LaneRecord lane = readLane(current.laneId());
        if (lane == null || !lane.laneId().equals(current.laneId())
                || !Arrays.equals(lane.laneIncarnation(), precondition.laneIncarnation())
                || lane.laneControlVersion() != precondition.laneControlVersion()
                || (current.status() == MessageStatus.CLAIMED
                ? current.stateVersion() != Math.addExact(precondition.stateVersion(), 1)
                : current.stateVersion() != precondition.stateVersion())
                || !Arrays.equals(current.laneId().bytes(), precondition.destinationLaneId())
                || !Arrays.equals(Bytes.sha256(sourceTimelineKey), precondition.originalTimelineKeySha256())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }

        final int expectedWorkKind = currentClaim != null
                ? ClaimResultBody.decodePrecondition(currentClaim.preconditionBytes()).sourceWorkKind()
                : current.runtimeIndex().timeline() == null
                ? (current.retryEligibilityAtEpochMs() == current.deliverAtEpochMs() ? 1 : 2)
                : current.runtimeIndex().timeline().workKind().wireValue();
        final byte[] expectedSemanticDigest = currentClaim != null
                ? precondition.sourceTimelineSemanticDigest()
                : current.runtimeIndex().timeline() == null
                ? timelineRuntimeIndex(messageId, current,
                expectedWorkKind == 1 ? TimelineWorkKind.INITIAL_SCHEDULE : TimelineWorkKind.DEFINITIVE_RETRY,
                Math.addExact(current.runtimeIndex().admissionsUsed(), 1), current.stateVersion(),
                UncertainRetryAuthority.NONE, null, null, current.runtimeIndex()).timeline()
                .semanticWorkDigest()
                : current.runtimeIndex().timeline().semanticWorkDigest();
        if (precondition.sourceWorkKind() != expectedWorkKind
                || precondition.expectedAdmissionsUsed() != current.runtimeIndex().admissionsUsed()
                || precondition.expectedUncertainRetryAdmissionsUsed()
                != current.runtimeIndex().uncertainRetryAdmissionsUsed()
                || !Bytes.constantTimeEquals(precondition.expectedObligationSetDigest(),
                GenerationRuntimeIndex.obligationSetDigest(current.runtimeIndex().attemptObligations()))
                || !Bytes.constantTimeEquals(precondition.sourceTimelineSemanticDigest(), expectedSemanticDigest)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }

        MessageRecord terminalMessage = new MessageRecord(MessageStatus.DEAD_LETTER, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs());
        terminalMessage = terminalMessage.withRuntimeIndex(GenerationRuntimeIndex.none(
                GenerationAggregateState.DEAD_LETTER, current.runtimeIndex().attemptObligations(),
                current.runtimeIndex().admissionsUsed(), current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(), terminalMessage.stateVersion()));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(messageId, body.generation(),
                MessageStatus.DEAD_LETTER, StableCode.CLAIM_PERMANENT_FAILURE, terminalMessage.stateVersion(),
                sourcePosition.canonicalBytes(), terminalMessage.runtimeIndex().possibleDestinationDuplicate(),
                terminalMessage.runtimeIndex().attemptObligations());
        final ShardQuota nextQuota;
        try {
            nextQuota = quota.removeSchedule(current.payloadLength());
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, messageId, current, terminalMessage, null);
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                StableCode.CLAIM_PERMANENT_FAILURE, sourcePosition.canonicalBytes());
        final MessageRecord terminalMessageForWrite = terminalMessage;
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, sourceTimelineKey);
            batch.delete(ColumnFamily.TIMELINE, expiryKey(messageId, current));
            if (currentClaim != null) {
                batch.delete(ColumnFamily.INFLIGHT, currentClaim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), terminalMessageForWrite.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1, KeyCodec.terminalGeneration(messageId, body.generation()),
                    terminal.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        quota = nextQuota;
        return result;
    }

    /**
     * Validates the replay-stable portion of a source-ordered Publish Admission.
     *
     * <p>The local Claim and its runtime instance are useful optimizations, but
     * neither is the source of truth after checkpoint/replay.  The signed body
     * must therefore still match the current Message/Lane projection and the
     * generation runtime counters before a new PUBLISHING obligation is made
     * durable.</p>
     */
    private AdmissionReplayState validatePublishAdmissionReplayState(final PublishAdmissionBody body,
                                                                       final MessageRecord current,
                                                                       final LaneRecord lane,
                                                                       final ClaimRecord localClaim) {
        final ClaimResultBody.ClaimPrecondition precondition =
                ClaimResultBody.decodePrecondition(body.claimPrecondition().canonicalBytes());
        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final GenerationRuntimeIndex index = current.runtimeIndex();
        if (!Arrays.equals(precondition.messageId(), messageId.bytes())
                || precondition.generation() != current.generation()
                || !Arrays.equals(precondition.destinationLaneId(), current.laneId().bytes())
                || !Arrays.equals(precondition.laneIncarnation(), lane.laneIncarnation())
                || precondition.laneControlVersion() != lane.laneControlVersion()) {
            throw new IllegalStateException("Publish Admission source identity is stale");
        }
        final long expectedStateVersion = current.status() == MessageStatus.CLAIMED
                ? Math.addExact(precondition.stateVersion(), 1) : precondition.stateVersion();
        if (current.stateVersion() != expectedStateVersion) {
            throw new IllegalStateException("Publish Admission message state version is stale");
        }
        final byte[] sourceTimelineKey = localClaim == null
                ? timelineKey(messageId, current) : localClaim.timelineKey();
        if (!Bytes.constantTimeEquals(precondition.originalTimelineKeySha256(),
                Bytes.sha256(sourceTimelineKey))) {
            throw new IllegalStateException("Publish Admission timeline key projection is stale");
        }
        if (precondition.expectedAdmissionsUsed() != index.admissionsUsed()
                || precondition.expectedUncertainRetryAdmissionsUsed()
                != index.uncertainRetryAdmissionsUsed()
                || !Bytes.constantTimeEquals(precondition.expectedObligationSetDigest(),
                GenerationRuntimeIndex.obligationSetDigest(index.attemptObligations()))) {
            throw new IllegalStateException("Publish Admission runtime counters are stale");
        }
        final int expectedAttemptNo = Math.addExact(index.admissionsUsed(), 1);
        if (body.descriptor().attemptNo() != expectedAttemptNo) {
            throw new IllegalStateException("Publish Admission attempt number is not replay-stable");
        }
        if (current.status() == MessageStatus.SCHEDULED
                && index.currentWorkKind() != CurrentSendWorkKind.TIMELINE) {
            throw new IllegalStateException("scheduled message has no timeline work projection");
        }
        if (current.status() == MessageStatus.CLAIMED
                && (index.currentWorkKind() != CurrentSendWorkKind.CLAIMED
                || !Bytes.constantTimeEquals(index.claimId(), body.claimId()))) {
            throw new IllegalStateException("claimed message has a different Claim projection");
        }

        final TimelineWorkKind sourceWorkKind = TimelineWorkKind.fromWire(precondition.sourceWorkKind());
        final TimelineWorkRef sourceWork;
        if (localClaim != null) {
            // The exact Claim record already validated the historical
            // work-instance digest and retains the canonical precondition.
            sourceWork = index.timeline();
        } else if (index.timeline() != null) {
            sourceWork = index.timeline();
            if (!Arrays.equals(sourceWork.encodedTimelineKey(), sourceTimelineKey)
                    || sourceWork.candidateAttemptNo() != expectedAttemptNo) {
                throw new IllegalStateException("Publish Admission timeline work projection is stale");
            }
        } else {
            if (sourceWorkKind == TimelineWorkKind.UNCERTAIN_RETRY) {
                throw new IllegalStateException("uncertain retry lacks a persisted timeline work reference");
            }
            sourceWork = new TimelineWorkRef(sourceWorkKind, sourceTimelineKey, current.deliverAtEpochMs(),
                    current.retryEligibilityAtEpochMs(), expectedAttemptNo,
                    Math.max(1, index.runtimeRevision()),
                    current.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO,
                    UncertainRetryAuthority.NONE, null, null);
        }
        if (localClaim == null && (sourceWork == null
                || sourceWork.workKind() != sourceWorkKind
                || !Bytes.constantTimeEquals(sourceWork.semanticWorkDigest(),
                precondition.sourceTimelineSemanticDigest()))) {
            throw new IllegalStateException("Publish Admission timeline semantic digest is stale");
        }
        if (sourceWorkKind == TimelineWorkKind.DEFINITIVE_RETRY && !index.attemptObligations().isEmpty()) {
            throw new IllegalStateException("definitive retry cannot carry open attempt obligations");
        }
        final boolean uncertainRetry = index.attemptObligations().stream()
                .anyMatch(ref -> ref.ledgerState() == AttemptLedgerState.UNCERTAIN);
        if (uncertainRetry) {
            if (sourceWorkKind != TimelineWorkKind.UNCERTAIN_RETRY
                    || current.orderingMode() != io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT) {
                throw new IllegalStateException("older UNCERTAIN obligation requires an uncertain retry work item");
            }
        } else if (sourceWorkKind == TimelineWorkKind.UNCERTAIN_RETRY) {
            throw new IllegalStateException("UNCERTAIN_RETRY has no older UNCERTAIN obligation");
        }
        validateAdmissionBudget(index, uncertainRetry);
        return new AdmissionReplayState(localClaim == null, uncertainRetry);
    }

    private void validateAdmissionBudget(final GenerationRuntimeIndex index,
                                         final boolean uncertainRetryAdmission) {
        if (index.admissionsUsed() >= config.maxPublishAdmissions()) {
            throw new IllegalStateException("generation publish admission budget is exhausted");
        }
        if (uncertainRetryAdmission
                && index.uncertainRetryAdmissionsUsed() >= config.maxUncertainRetries()) {
            throw new IllegalStateException("generation uncertain-retry admission budget is exhausted");
        }
    }

    private MessageRecord normalizeCommandRuntime(final DelayMessageId messageId, final MessageRecord prior,
                                                  final MessageRecord next, final CommandResult result) {
        if (next == null) {
            return null;
        }
        if (next.status() == MessageStatus.SCHEDULED) {
            final TimelineWorkKind kind = TimelineWorkKind.INITIAL_SCHEDULE;
            return next.withRuntimeIndex(timelineRuntimeIndex(messageId, next, kind, 1, next.stateVersion(),
                    UncertainRetryAuthority.NONE, null, null));
        }
        if (prior != null && isTerminalStatus(next.status())) {
            return next.withRuntimeIndex(GenerationRuntimeIndex.none(
                    GenerationAggregateState.fromMessageStatus(next.status()),
                    prior.runtimeIndex().attemptObligations(), prior.runtimeIndex().admissionsUsed(),
                    prior.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    prior.runtimeIndex().possibleDestinationDuplicate(), next.stateVersion()));
        }
        return next.withRuntimeIndex(GenerationRuntimeIndex.none(
                GenerationAggregateState.fromMessageStatus(next.status()), List.of(), 0, 0, false,
                Math.max(1, next.stateVersion())));
    }

    private static boolean isTerminalStatus(final MessageStatus status) {
        return status == MessageStatus.CANCELED || status == MessageStatus.SUPERSEDED
                || status == MessageStatus.PUBLISHED || status == MessageStatus.EXPIRED
                || status == MessageStatus.DEAD_LETTER;
    }

    private static boolean hasUncertainObligation(final GenerationRuntimeIndex index) {
        return index.attemptObligations().stream()
                .anyMatch(obligation -> obligation.ledgerState() == AttemptLedgerState.UNCERTAIN);
    }

    private GenerationRuntimeIndex timelineRuntimeIndex(final DelayMessageId messageId, final MessageRecord message,
                                                        final TimelineWorkKind workKind, final int candidateAttemptNo,
                                                        final long runtimeRevision,
                                                        final UncertainRetryAuthority authority,
                                                        final byte[] control, final byte[] controlPosition) {
        return timelineRuntimeIndex(messageId, message, workKind, candidateAttemptNo, runtimeRevision, authority,
                control, controlPosition, null, null);
    }

    private GenerationRuntimeIndex timelineRuntimeIndex(final DelayMessageId messageId, final MessageRecord message,
                                                        final TimelineWorkKind workKind, final int candidateAttemptNo,
                                                        final long runtimeRevision,
                                                        final UncertainRetryAuthority authority,
                                                        final byte[] control, final byte[] controlPosition,
                                                        final GenerationRuntimeIndex base) {
        return timelineRuntimeIndex(messageId, message, workKind, candidateAttemptNo, runtimeRevision, authority,
                control, controlPosition, base, base == null ? null : base.attemptObligations());
    }

    private GenerationRuntimeIndex timelineRuntimeIndex(final DelayMessageId messageId, final MessageRecord message,
                                                        final TimelineWorkKind workKind, final int candidateAttemptNo,
                                                        final long runtimeRevision,
                                                        final UncertainRetryAuthority authority,
                                                        final byte[] control, final byte[] controlPosition,
                                                        final GenerationRuntimeIndex base,
                                                        final List<AttemptObligationRef> obligations) {
        final byte[] key = timelineKey(messageId, message);
        final TimelineWorkRef work = new TimelineWorkRef(workKind, key, message.deliverAtEpochMs(),
                message.retryEligibilityAtEpochMs(), candidateAttemptNo, runtimeRevision,
                message.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO,
                authority, control, controlPosition);
        final GenerationAggregateState aggregate = switch (workKind) {
            case INITIAL_SCHEDULE -> GenerationAggregateState.SCHEDULED;
            case DEFINITIVE_RETRY -> GenerationAggregateState.RETRY_WAIT;
            case UNCERTAIN_RETRY -> GenerationAggregateState.UNCERTAIN;
        };
        final List<AttemptObligationRef> retained = obligations == null ? List.of() : obligations;
        final int admissionsUsed = base == null ? 0 : base.admissionsUsed();
        final int uncertainRetryAdmissionsUsed = base == null ? 0 : base.uncertainRetryAdmissionsUsed();
        final boolean possibleDestinationDuplicate = base != null && base.possibleDestinationDuplicate();
        return GenerationRuntimeIndex.timeline(aggregate, work, retained, admissionsUsed,
                uncertainRetryAdmissionsUsed, possibleDestinationDuplicate, runtimeRevision);
    }

    private static List<AttemptObligationRef> withoutObligation(final GenerationRuntimeIndex index,
                                                                 final byte[] publishAttemptId) {
        return withoutObligation(index.attemptObligations(), publishAttemptId);
    }

    private static List<AttemptObligationRef> withoutObligation(final List<AttemptObligationRef> obligations,
                                                                 final byte[] publishAttemptId) {
        final List<AttemptObligationRef> result = new ArrayList<>();
        for (AttemptObligationRef ref : obligations) {
            if (!Arrays.equals(ref.publishAttemptId(), publishAttemptId)) {
                result.add(ref);
            }
        }
        result.sort(DelayShard::compareObligation);
        return result;
    }

    private static List<AttemptObligationRef> withObligation(final GenerationRuntimeIndex index,
                                                              final AttemptObligationRef obligation) {
        return withObligation(index.attemptObligations(), obligation);
    }

    private static List<AttemptObligationRef> withObligation(final List<AttemptObligationRef> obligations,
                                                              final AttemptObligationRef obligation) {
        final List<AttemptObligationRef> result = new ArrayList<>(obligations);
        result.removeIf(ref -> Arrays.equals(ref.publishAttemptId(), obligation.publishAttemptId()));
        result.add(obligation);
        result.sort(DelayShard::compareObligation);
        return result;
    }

    private static int compareObligation(final AttemptObligationRef left, final AttemptObligationRef right) {
        final int id = compareUnsigned(left.publishAttemptId(), right.publishAttemptId());
        return id != 0 ? id : compareUnsigned(left.encodedInflightKey(), right.encodedInflightKey());
    }

    private SystemMutationResult applyNotPublishedPublishOutcome(final PublishAttemptLedger ledger,
                                                                  final PublishOutcomeBody outcome,
                                                                  final SourcePosition sourcePosition,
                                                                  final SystemMutationResult systemResult,
                                                                  final AttemptLedgerState expectedLedgerState,
                                                                  final MessageStatus expectedMessageStatus) {
        final MessageRecord current = getMessage(ledger.delayMessageId());
        if (ledger.state() != expectedLedgerState || current == null
                || current.generation() < ledger.generation()) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        if (current.generation() > ledger.generation()) {
            final PublishOutcomeBody.RetryDecision retryDecision = outcome.retryDecision();
            if (retryDecision.completedAttemptNo() != ledger.attemptNo()) {
                return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
            settleHistoricalTerminalObligation(ledger, sourcePosition, systemResult, false);
            return systemResult;
        }
        if (isTerminalStatus(current.status())) {
            if (outcome.retryDecision().completedAttemptNo() != ledger.attemptNo()) {
                return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
            settleTerminalObligation(ledger, current, sourcePosition, systemResult, false);
            return systemResult;
        }
        if (current.status() != expectedMessageStatus) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        final PublishOutcomeBody.RetryDecision retryDecision = outcome.retryDecision();
        if (retryDecision.completedAttemptNo() != ledger.attemptNo()
                || retryDecision.retryDeadline() > current.expireAtEpochMs()
                || retryDecision.firstAttemptAt() > retryDecision.retryDeadline()
                || retryDecision.hasNextRetryAt()
                && retryDecision.nextRetryAt() < current.deliverAtEpochMs()) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        if (outcome.disposition() == 2) {
            MessageRecord terminalMessage = new MessageRecord(MessageStatus.DEAD_LETTER, current.generation(),
                    Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                    current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                    current.payloadReference(), current.retryEligibilityAtEpochMs());
            terminalMessage = terminalMessage.withRuntimeIndex(GenerationRuntimeIndex.none(
                    GenerationAggregateState.DEAD_LETTER,
                    withoutObligation(current.runtimeIndex(), ledger.publishAttemptId()),
                    current.runtimeIndex().admissionsUsed(), current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    current.runtimeIndex().possibleDestinationDuplicate(), terminalMessage.stateVersion()));
            final TerminalGenerationRecord terminal = new TerminalGenerationRecord(ledger.delayMessageId(),
                    ledger.generation(), MessageStatus.DEAD_LETTER, outcome.stableCode(), terminalMessage.stateVersion(),
                    sourcePosition.canonicalBytes(), terminalMessage.runtimeIndex().possibleDestinationDuplicate(),
                    terminalMessage.runtimeIndex().attemptObligations());
            final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
            final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                    sourcePosition, ledger.delayMessageId(), current, terminalMessage, null);
            final MessageRecord terminalMessageForWrite = terminalMessage;
            store.write(batch -> {
                batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()),
                        terminalMessageForWrite.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), terminal.encode());
                for (LaneProjection projection : projections.values()) {
                    deleteReadyKey(batch, projection.previousLane());
                    putReadyProjection(batch, projection);
                }
                batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
                writeSystemResult(batch, systemResult);
                writePosition(batch, sourcePosition);
            });
            lastAppliedSourcePosition = sourcePosition;
            mutationSequence++;
            quota = nextQuota;
            return systemResult;
        }
        if (current.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                || !retryDecision.hasNextRetryAt()) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        final long retryAt = Math.max(current.deliverAtEpochMs(), retryDecision.nextRetryAt());
        if (retryAt >= current.expireAtEpochMs()) {
            return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
        MessageRecord scheduled = new MessageRecord(MessageStatus.SCHEDULED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), retryAt);
        final List<AttemptObligationRef> remainingObligations = withoutObligation(current.runtimeIndex(),
                ledger.publishAttemptId());
        if (remainingObligations.stream().anyMatch(ref -> ref.ledgerState() == AttemptLedgerState.UNCERTAIN)) {
            throw new IllegalStateException("definitive retry cannot bypass an older UNCERTAIN obligation");
        }
        scheduled = scheduled.withRuntimeIndex(timelineRuntimeIndex(ledger.delayMessageId(), scheduled,
                TimelineWorkKind.DEFINITIVE_RETRY, Math.addExact(ledger.attemptNo(), 1), scheduled.stateVersion(),
                UncertainRetryAuthority.NONE, null, null, current.runtimeIndex(), remainingObligations));
        final MessageRecord scheduledForWrite = scheduled;
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneRecord> laneOverrides = new HashMap<>();
        if (outcome.disposition() == 3) {
            final LaneRecord lane = readLane(current.laneId());
            if (lane == null) {
                return persistSystemResultByResult(systemResult, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
            laneOverrides.put(current.laneId(), lane.withReadiness(RuntimeReadiness.BLOCKED));
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, ledger.delayMessageId(), current, scheduled, null, laneOverrides);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), scheduledForWrite.encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey(ledger.delayMessageId(), scheduledForWrite),
                    new TimelineEntry(ledger.delayMessageId(), scheduledForWrite.generation()).encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(ledger.delayMessageId(), scheduledForWrite),
                    new TimelineEntry(ledger.delayMessageId(), scheduledForWrite.generation()).encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            writeSystemResult(batch, systemResult);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return systemResult;
    }

    private SystemMutationResult persistSystemResultByResult(final SystemMutationResult original,
                                                              final SourcePosition sourcePosition,
                                                              final StableCode code) {
        final SystemMutationResult result = new SystemMutationResult(original.mutationId(), original.mutationHash(),
                original.mutationType(), original.retryUntilEpochMs(), original.authorIdentity(),
                ApplyStatus.APPLIED, code, sourcePosition.canonicalBytes());
        store.write(batch -> {
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    private SystemMutationResult applyExpireGenerationMutation(final SystemMutation mutation,
                                                                final SourcePosition sourcePosition) {
        final List<io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.EXPIRE_GENERATION, mutation.canonicalBody());
        final DelayMessageId messageId = new DelayMessageId(fixedBodyBytes(field(fields, 10), 10,
                DelayMessageId.LENGTH));
        final int generation = bodyInt(field(fields, 11), 11);
        final long expireAt = bodyNonNegative(field(fields, 12), 12);
        final TrustedUtcIntervalEvidence proof = TrustedUtcIntervalEvidence.decode(
                bytesBody(field(fields, 13), 13));
        proof.requireEarliestAtLeast(expireAt);
        final MessageRecord current = getMessage(messageId);
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.NOT_FOUND);
        }
        if (current.generation() != generation) {
            final StableCode code = current.generation() > generation
                    ? StableCode.GENERATION_SUPERSEDED : StableCode.STALE_SYSTEM_MUTATION;
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, code);
        }
        if (current.expireAtEpochMs() != expireAt) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        if (current.status() == MessageStatus.EXPIRED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.ALREADY_EXPIRED);
        }
        if (current.status() != MessageStatus.SCHEDULED && current.status() != MessageStatus.CLAIMED) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final ClaimRecord claim = current.status() == MessageStatus.CLAIMED
                ? findClaimForMessage(messageId) : null;
        if (current.status() == MessageStatus.CLAIMED && claim == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        MessageRecord next = new MessageRecord(MessageStatus.EXPIRED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.none(GenerationAggregateState.EXPIRED,
                current.runtimeIndex().attemptObligations(), current.runtimeIndex().admissionsUsed(),
                current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(), next.stateVersion()));
        final MessageRecord expiredNext = next;
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(messageId, generation,
                MessageStatus.EXPIRED, StableCode.ALREADY_EXPIRED, next.stateVersion(),
                sourcePosition.canonicalBytes(), next.runtimeIndex().possibleDestinationDuplicate(),
                next.runtimeIndex().attemptObligations());
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, messageId, current, next, null);
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, claim == null ? timelineKey(messageId, current) : claim.timelineKey());
            batch.delete(ColumnFamily.TIMELINE, expiryKey(messageId, current));
            if (claim != null) {
                batch.delete(ColumnFamily.INFLIGHT, claim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), expiredNext.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1, KeyCodec.terminalGeneration(messageId, generation),
                    terminal.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        quota = nextQuota;
        return result;
    }

    private SystemMutationResult persistSystemResult(final SystemMutation mutation, final SourcePosition position,
                                                      final ApplyStatus status, final StableCode code) {
        final SystemMutationResult result = SystemMutationResult.from(mutation, status, code,
                position.canonicalBytes());
        store.write(batch -> {
            writeSystemResult(batch, result);
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence++;
        return result;
    }

    private void writeSystemResult(final ShardStore.Batch batch, final SystemMutationResult result)
            throws org.rocksdb.RocksDBException {
        batch.putValue(ColumnFamily.DEDUPE, SystemMutationResult.VALUE_TYPE,
                KeyCodec.dedupeSystemMutation(result.mutationId()), result.encode());
    }

    private void validateMutationShard(final SystemMutation mutation, final SourcePosition sourcePosition) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!store.shardId().equals(mutation.shardId()) || !store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("System Mutation/source position does not belong to shard");
        }
    }

    private static io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field(
            final List<io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return fields.get(index);
            }
        }
        throw new IllegalArgumentException("missing System Mutation operation field " + number);
    }

    private static long bodyNonNegative(
            final io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.wireType() != 0 || field.number() != number || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid System Mutation scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static int bodyInt(final io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field,
                               final int number) {
        final long value = bodyNonNegative(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("System Mutation field exceeds Java int range: " + number);
        }
        return (int) value;
    }

    private static byte[] bytesBody(final io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field,
                                    final int number) {
        if (field.wireType() != 2 || field.number() != number) {
            throw new IllegalArgumentException("invalid System Mutation bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] fixedBodyBytes(
            final io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field field, final int number,
            final int length) {
        final byte[] value = bytesBody(field, number);
        Bytes.requireLength(value, length, "System Mutation field " + number);
        return value;
    }

    private static byte[] optionalBodyBytes(
            final List<io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields, final int number) {
        for (int index = 3; index < fields.size(); index++) {
            if (fields.get(index).number() == number) {
                return bytesBody(fields.get(index), number);
            }
        }
        return new byte[0];
    }

    public synchronized TerminalGenerationRecord getTerminalGeneration(final DelayMessageId messageId,
                                                                        final int generation) {
        final var value = store.getValue(ColumnFamily.TERMINAL, KeyCodec.terminalGeneration(messageId, generation), 1);
        return value == null ? null : TerminalGenerationRecord.decode(value.payload());
    }

    /** Returns one open publish attempt at an exact admitted Owner Epoch, or {@code null}. */
    public synchronized PublishAttemptLedger getPublishAttempt(final byte[] publishAttemptId,
                                                                final long ownerEpoch) {
        Bytes.requireLength(publishAttemptId, PublishAttemptLedger.HASH_LENGTH, "publishAttemptId");
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        final PublishAttemptLedger publishing = readPublishAttempt(publishAttemptId, ownerEpoch,
                INFLIGHT_PUBLISHING_KIND);
        final PublishAttemptLedger uncertain = readPublishAttempt(publishAttemptId, ownerEpoch,
                INFLIGHT_UNCERTAIN_KIND);
        if (publishing != null && uncertain != null) {
            throw new IllegalStateException("publish attempt has two live ledger states");
        }
        return publishing == null ? uncertain : publishing;
    }

    /**
     * Finds an open attempt without trusting a caller-supplied Owner Epoch. This is a bounded recovery lookup; a
     * duplicate ID or a scan that exceeds the configured shard bound fences the shard instead of guessing.
     */
    public synchronized PublishAttemptLedger findOpenPublishAttempt(final byte[] publishAttemptId) {
        Bytes.requireLength(publishAttemptId, PublishAttemptLedger.HASH_LENGTH, "publishAttemptId");
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{INFLIGHT_PUBLISHING_KIND, 1}, new byte[]{4, 1}, limit);
        PublishAttemptLedger found = null;
        for (var entry : entries) {
            final PublishAttemptLedger candidate = decodePublishAttempt(entry);
            if (!Bytes.constantTimeEquals(candidate.publishAttemptId(), publishAttemptId)) {
                continue;
            }
            if (found != null) {
                throw new IllegalStateException("publish attempt ID has multiple live ledgers");
            }
            found = candidate;
        }
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("open publish attempt scan exceeded configured bound");
        }
        return found;
    }

    /**
     * Applies the durable part of Publish Admission. The complete signed Registry body is retained verbatim in the
     * ledger, while nested Claim/Certificate/Channel validation is deliberately owned by the pending admission
     * body codec. The message, timeline, READY projection, attempt key and source position commit in one batch.
     */
    public synchronized PublishAttemptLedger admitPublishAttempt(final PublishAttemptLedger admission,
                                                                  final SourcePosition sourcePosition) {
        return admitPublishAttempt(admission, sourcePosition, null, false, false);
    }

    private PublishAttemptLedger admitPublishAttempt(final PublishAttemptLedger admission,
                                                     final SourcePosition sourcePosition,
                                                     final SystemMutationResult systemResult,
                                                     final boolean claimMayBeMissing,
                                                     final boolean uncertainRetryAdmission) {
        Objects.requireNonNull(admission, "admission");
        validateMutationPosition(sourcePosition);
        if (admission.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalArgumentException("Publish Admission must create a PUBLISHING ledger");
        }
        if (!Arrays.equals(admission.sourcePosition(), sourcePosition.canonicalBytes())) {
            throw new IllegalArgumentException("admission source position mismatch");
        }
        if (!store.shardId().equals(admission.delayMessageId().routingId().shardId())
                || !store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("publish admission does not belong to shard");
        }
        if (findOpenPublishAttempt(admission.publishAttemptId()) != null) {
            throw new IllegalStateException("publish attempt ID is already open");
        }
        final MessageRecord current = getMessage(admission.delayMessageId());
        if (current == null || (current.status() != MessageStatus.SCHEDULED
                && current.status() != MessageStatus.CLAIMED)
                || current.generation() != admission.generation() || !current.laneId().equals(admission.laneId())) {
            throw new IllegalStateException("publish admission is stale for the current message generation");
        }
        validateAdmissionBudget(current.runtimeIndex(), uncertainRetryAdmission);
        final ClaimRecord claim = current.status() == MessageStatus.CLAIMED
                ? getClaim(admission.claimId(), admission.ownerEpoch()) : null;
        if (current.status() == MessageStatus.CLAIMED
                && ((!claimMayBeMissing && claim == null)
                || (claim != null && (!claim.delayMessageId().equals(admission.delayMessageId())
                || claim.generation() != admission.generation()
                || !claim.laneId().equals(admission.laneId())
                || !Arrays.equals(claim.ownerIdentity(), admission.ownerIdentity())
                || !Arrays.equals(claim.storeIncarnation(), admission.storeIncarnation()))))) {
            throw new IllegalStateException("publish admission Claim is stale");
        }
        final LaneRecord lane = readLane(current.laneId());
        if (lane == null || !lane.schedulable()) {
            throw new IllegalStateException("publish admission requires a schedulable lane");
        }
        final List<AttemptObligationRef> obligations = withObligation(current.runtimeIndex(), admission.obligationRef());
        MessageRecord next = new MessageRecord(MessageStatus.PUBLISHING, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.publishing(admission.publishAttemptId(), obligations,
                Math.addExact(current.runtimeIndex().admissionsUsed(), 1),
                Math.addExact(current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                        uncertainRetryAdmission ? 1 : 0),
                current.runtimeIndex().possibleDestinationDuplicate(), next.stateVersion()));
        final MessageRecord admissionNext = next;
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, admission.delayMessageId(), current, next, null);
        final byte[] priorTimelineKey = claim == null ? timelineKey(admission.delayMessageId(), current)
                : claim.timelineKey();
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, priorTimelineKey);
            batch.delete(ColumnFamily.TIMELINE, expiryKey(admission.delayMessageId(), current));
            if (claim != null) {
                batch.delete(ColumnFamily.INFLIGHT, claim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(admission.delayMessageId()), admissionNext.encode());
            batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, admission.encodedKey(),
                    admission.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return admission;
    }

    /** Atomically records an unknown target result and moves the exact key to UNCERTAIN. */
    public synchronized PublishAttemptLedger applyUnknownPublishOutcome(final byte[] publishAttemptId,
                                                                         final long ownerEpoch,
                                                                         final byte[] canonicalOutcome,
                                                                         final byte[] evidence,
                                                                         final SourcePosition sourcePosition) {
        return applyUnknownPublishOutcome(publishAttemptId, ownerEpoch, canonicalOutcome, evidence, sourcePosition,
                null, null);
    }

    private PublishAttemptLedger applyUnknownPublishOutcome(final byte[] publishAttemptId,
                                                             final long ownerEpoch,
                                                             final byte[] canonicalOutcome,
                                                             final byte[] evidence,
                                                             final SourcePosition sourcePosition,
                                                             final SystemMutationResult systemResult,
                                                             final PublishOutcomeBody.RetryDecision retryDecision) {
        validateMutationPosition(sourcePosition);
        final PublishAttemptLedger currentLedger = getPublishAttempt(publishAttemptId, ownerEpoch);
        if (currentLedger == null || currentLedger.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalStateException("unknown outcome requires a PUBLISHING ledger");
        }
        final MessageRecord current = getMessage(currentLedger.delayMessageId());
        if (current == null || current.generation() < currentLedger.generation()) {
            throw new IllegalStateException("unknown outcome is stale for the current message");
        }
        if (current.generation() > currentLedger.generation()) {
            return settleHistoricalUnknownObligation(currentLedger, canonicalOutcome, evidence, sourcePosition,
                    systemResult);
        }
        if (current.status() != MessageStatus.PUBLISHING) {
            throw new IllegalStateException("unknown outcome is stale for the current message");
        }
        final boolean scheduleUncertainRetry = retryDecision != null && retryDecision.kind() == 2;
        final long retryAt;
        if (scheduleUncertainRetry) {
            if (current.orderingMode() != io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
                    || !retryDecision.hasNextRetryAt()
                    || config.maxUncertainRetries() == 0
                    || current.runtimeIndex().uncertainRetryAdmissionsUsed() >= config.maxUncertainRetries()
                    || current.runtimeIndex().admissionsUsed() >= config.maxPublishAdmissions()) {
                throw new IllegalArgumentException("uncertain retry is not within the pinned budget");
            }
            retryAt = Math.max(current.deliverAtEpochMs(), retryDecision.nextRetryAt());
            if (retryAt >= current.expireAtEpochMs()
                    || retryDecision.retryDeadline() > current.expireAtEpochMs()
                    || retryDecision.firstAttemptAt() > retryAt) {
                throw new IllegalArgumentException("uncertain retry timing is stale");
            }
        } else {
            retryAt = current.retryEligibilityAtEpochMs();
        }
        final PublishAttemptLedger nextLedger = currentLedger.withUnknownOutcome(canonicalOutcome, evidence,
                sourcePosition.canonicalBytes());
        final List<AttemptObligationRef> nextObligations = withObligation(
                current.runtimeIndex(), nextLedger.obligationRef());
        MessageRecord next = new MessageRecord(scheduleUncertainRetry ? MessageStatus.SCHEDULED : MessageStatus.UNCERTAIN,
                current.generation(), Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(),
                current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), retryAt);
        next = scheduleUncertainRetry
                ? next.withRuntimeIndex(timelineRuntimeIndex(currentLedger.delayMessageId(), next,
                TimelineWorkKind.UNCERTAIN_RETRY,
                Math.addExact(current.runtimeIndex().admissionsUsed(), 1), next.stateVersion(),
                UncertainRetryAuthority.PINNED_POLICY, null, null, current.runtimeIndex(), nextObligations))
                : next.withRuntimeIndex(GenerationRuntimeIndex.none(GenerationAggregateState.UNCERTAIN,
                nextObligations, current.runtimeIndex().admissionsUsed(),
                current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(), next.stateVersion()));
        final MessageRecord uncertainNext = next;
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, currentLedger.delayMessageId(), current, next, null);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, currentLedger.encodedKey());
            batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, nextLedger.encodedKey(),
                    nextLedger.encode());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(nextLedger.delayMessageId()), uncertainNext.encode());
            if (scheduleUncertainRetry) {
                batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey(nextLedger.delayMessageId(), uncertainNext),
                        new TimelineEntry(nextLedger.delayMessageId(), uncertainNext.generation()).encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(nextLedger.delayMessageId(), uncertainNext),
                        new TimelineEntry(nextLedger.delayMessageId(), uncertainNext.generation()).encode());
            }
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return nextLedger;
    }

    private PublishAttemptLedger settleHistoricalUnknownObligation(final PublishAttemptLedger ledger,
                                                                   final byte[] canonicalOutcome,
                                                                   final byte[] evidence,
                                                                   final SourcePosition sourcePosition,
                                                                   final SystemMutationResult systemResult) {
        final TerminalGenerationRecord summary = getTerminalGeneration(ledger.delayMessageId(), ledger.generation());
        if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
            throw new IllegalStateException("historical terminal obligation summary is stale or missing");
        }
        final PublishAttemptLedger nextLedger = ledger.withUnknownOutcome(canonicalOutcome, evidence,
                sourcePosition.canonicalBytes());
        final List<AttemptObligationRef> obligations = withObligation(summary.openObligations(),
                nextLedger.obligationRef());
        final TerminalGenerationRecord nextSummary = new TerminalGenerationRecord(summary.messageId(),
                summary.generation(), summary.status(), summary.terminalCode(), summary.stateVersion(),
                summary.appliedSourcePosition(), summary.possibleDestinationDuplicate(), obligations);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.INFLIGHT, PublishAttemptLedger.VALUE_TYPE, nextLedger.encodedKey(),
                    nextLedger.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), nextSummary.encode());
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return nextLedger;
    }

    /** Atomically closes a PUBLISHING attempt after a verified publish success. */
    public synchronized MessageRecord applyPublishedPublishOutcome(final byte[] publishAttemptId,
                                                                    final long ownerEpoch,
                                                                    final SourcePosition sourcePosition) {
        return applyPublishedPublishOutcome(publishAttemptId, ownerEpoch, sourcePosition, null);
    }

    private MessageRecord applyPublishedPublishOutcome(final byte[] publishAttemptId, final long ownerEpoch,
                                                       final SourcePosition sourcePosition,
                                                       final SystemMutationResult systemResult) {
        validateMutationPosition(sourcePosition);
        final PublishAttemptLedger ledger = getPublishAttempt(publishAttemptId, ownerEpoch);
        if (ledger == null || ledger.state() != AttemptLedgerState.PUBLISHING) {
            throw new IllegalStateException("published outcome requires a PUBLISHING ledger");
        }
        return applyPublishedPublishOutcome(ledger, sourcePosition, systemResult, MessageStatus.PUBLISHING);
    }

    private MessageRecord applyPublishedPublishOutcome(final PublishAttemptLedger ledger,
                                                       final SourcePosition sourcePosition,
                                                       final SystemMutationResult systemResult,
                                                       final MessageStatus expectedMessageStatus) {
        final MessageRecord current = getMessage(ledger.delayMessageId());
        if (current == null || current.generation() < ledger.generation()) {
            throw new IllegalStateException("published outcome is stale for the current message");
        }
        if (current.generation() > ledger.generation()) {
            return settleHistoricalTerminalObligation(ledger, sourcePosition, systemResult, true);
        }
        if (isTerminalStatus(current.status())) {
            return settleTerminalObligation(ledger, current, sourcePosition, systemResult, true);
        }
        if (current.status() != expectedMessageStatus) {
            throw new IllegalStateException("published outcome is stale for the current message");
        }
        MessageRecord next = new MessageRecord(MessageStatus.PUBLISHED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs());
        next = next.withRuntimeIndex(GenerationRuntimeIndex.none(GenerationAggregateState.PUBLISHED,
                withoutObligation(current.runtimeIndex(), ledger.publishAttemptId()),
                current.runtimeIndex().admissionsUsed(), current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(), next.stateVersion()));
        final MessageRecord publishedNext = next;
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(ledger.delayMessageId(),
                ledger.generation(), MessageStatus.PUBLISHED, StableCode.OK, next.stateVersion(),
                sourcePosition.canonicalBytes(), next.runtimeIndex().possibleDestinationDuplicate(),
                next.runtimeIndex().attemptObligations());
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, ledger.delayMessageId(), current, next, null);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), publishedNext.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), terminal.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return next;
    }

    private MessageRecord settleTerminalObligation(final PublishAttemptLedger ledger,
                                                   final MessageRecord current,
                                                   final SourcePosition sourcePosition,
                                                   final SystemMutationResult systemResult,
                                                   final boolean verifiedPublished) {
        final TerminalGenerationRecord summary = getTerminalGeneration(ledger.delayMessageId(), ledger.generation());
        if (summary == null || !containsObligation(current.runtimeIndex(), ledger.obligationRef())
                || !summary.openObligations().equals(current.runtimeIndex().attemptObligations())) {
            throw new IllegalStateException("terminal obligation summary is stale or missing");
        }
        final List<AttemptObligationRef> remaining = withoutObligation(current.runtimeIndex(),
                ledger.publishAttemptId());
        final boolean duplicate = current.runtimeIndex().possibleDestinationDuplicate() || verifiedPublished;
        final MessageRecord next = new MessageRecord(current.status(), current.generation(), current.stateVersion(),
                current.deliverAtEpochMs(), current.expireAtEpochMs(), current.laneId(), current.orderingMode(),
                current.payload(), current.scheduleSourcePosition(), current.payloadReference(),
                current.retryEligibilityAtEpochMs()).withRuntimeIndex(GenerationRuntimeIndex.none(
                        GenerationAggregateState.fromMessageStatus(current.status()), remaining,
                        current.runtimeIndex().admissionsUsed(), current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                        duplicate, Math.addExact(current.runtimeIndex().runtimeRevision(), 1)));
        final TerminalGenerationRecord nextSummary = new TerminalGenerationRecord(summary.messageId(),
                summary.generation(), summary.status(), summary.terminalCode(), summary.stateVersion(),
                summary.appliedSourcePosition(), duplicate, remaining);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), nextSummary.encode());
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return next;
    }

    /** Settles an old-generation obligation using its terminal summary only. */
    private MessageRecord settleHistoricalTerminalObligation(final PublishAttemptLedger ledger,
                                                             final SourcePosition sourcePosition,
                                                             final SystemMutationResult systemResult,
                                                             final boolean verifiedPublished) {
        final TerminalGenerationRecord summary = getTerminalGeneration(ledger.delayMessageId(), ledger.generation());
        if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
            throw new IllegalStateException("historical terminal obligation summary is stale or missing");
        }
        final List<AttemptObligationRef> remaining = withoutObligation(summary.openObligations(),
                ledger.publishAttemptId());
        final boolean duplicate = summary.possibleDestinationDuplicate() || verifiedPublished;
        final TerminalGenerationRecord nextSummary = new TerminalGenerationRecord(summary.messageId(),
                summary.generation(), summary.status(), summary.terminalCode(), summary.stateVersion(),
                summary.appliedSourcePosition(), duplicate, remaining);
        final MessageRecord current = getMessage(ledger.delayMessageId());
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), nextSummary.encode());
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return current;
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

    /** Applies a local management-gate transition with an exact CAS version. */
    public synchronized LaneRecord updateLaneGate(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId,
            final long expectedLaneControlVersion, final AdmissionGate gate) {
        final LaneRecord current = readLane(laneId);
        if (current == null) {
            throw new IllegalArgumentException("unknown destination lane");
        }
        if (current.laneControlVersion() != expectedLaneControlVersion) {
            throw new IllegalStateException("lane control version conflict");
        }
        final LaneRecord next = current.withGate(Objects.requireNonNull(gate, "gate"));
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
        if ((existing.status() == MessageStatus.SCHEDULED || existing.status() == MessageStatus.CLAIMED)
                && hasUncertainObligation(existing.runtimeIndex())) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        return switch (existing.status()) {
            case SCHEDULED, CLAIMED -> applied(StableCode.CANCELED, sourcePosition,
                    new MessageRecord(MessageStatus.CANCELED, existing.generation(), existing.stateVersion() + 1,
                            existing.deliverAtEpochMs(), existing.expireAtEpochMs(), existing.laneId(),
                            existing.orderingMode(), existing.payload(), existing.scheduleSourcePosition(),
                            existing.payloadReference(), existing.retryEligibilityAtEpochMs()));
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
        if ((existing.status() == MessageStatus.SCHEDULED || existing.status() == MessageStatus.CLAIMED)
                && hasUncertainObligation(existing.runtimeIndex())) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        if (existing.status() != MessageStatus.SCHEDULED && existing.status() != MessageStatus.CLAIMED) {
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
        final MessageRecord persistedNext = normalizeCommandRuntime(command.delayMessageId(), prior, next, result);
        final ClaimRecord priorClaim = prior != null && prior.status() == MessageStatus.CLAIMED
                ? findClaimForMessage(command.delayMessageId()) : null;
        if (prior != null && prior.status() == MessageStatus.CLAIMED && priorClaim == null) {
            throw new IllegalStateException("CLAIMED message has no durable Claim record");
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections =
                readyProjections(position, command.delayMessageId(), prior, persistedNext, reservation);
        store.write(batch -> {
            if (persistedNext != null) {
                if (prior != null && (prior.status() == MessageStatus.SCHEDULED
                        || prior.status() == MessageStatus.CLAIMED)) {
                    batch.delete(ColumnFamily.TIMELINE, priorClaim == null
                            ? timelineKey(command.delayMessageId(), prior) : priorClaim.timelineKey());
                    batch.delete(ColumnFamily.TIMELINE, expiryKey(command.delayMessageId(), prior));
                    if (priorClaim != null) {
                        batch.delete(ColumnFamily.INFLIGHT, priorClaim.encodedKey());
                    }
                    final TerminalGenerationRecord terminal = terminalFor(command, position, result, prior,
                            persistedNext);
                    if (terminal != null) {
                        batch.putValue(ColumnFamily.TERMINAL, 1,
                                KeyCodec.terminalGeneration(command.delayMessageId(), terminal.generation()),
                                terminal.encode());
                    }
                }
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(command.delayMessageId()), persistedNext.encode());
                if (persistedNext.status() == MessageStatus.SCHEDULED) {
                    batch.putValue(ColumnFamily.TIMELINE, 1,
                            timelineKey(command.delayMessageId(), persistedNext),
                            new TimelineEntry(command.delayMessageId(), persistedNext.generation()).encode());
                    batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(command.delayMessageId(), persistedNext),
                            new TimelineEntry(command.delayMessageId(), persistedNext.generation()).encode());
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
        return readyProjections(position, messageId, prior, next, reservation, Map.of());
    }

    private Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> readyProjections(
            final SourcePosition position, final DelayMessageId messageId, final MessageRecord prior,
            final MessageRecord next, final PayloadReservation reservation,
            final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneRecord> laneOverrides) {
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
            final LaneRecord base = laneOverrides.getOrDefault(laneId,
                    previous == null ? LaneRecord.initial(laneId, position) : previous);
            final int excludedGeneration = prior != null && (prior.status() == MessageStatus.SCHEDULED
                    || prior.status() == MessageStatus.CLAIMED)
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
                    timelineEligibilityAt(includedMessage), timelineKey(includedMessageId, includedMessage),
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
        if (prior != null && (prior.status() == MessageStatus.SCHEDULED || prior.status() == MessageStatus.CLAIMED)
                && next != null
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
                    prior.payload(), prior.scheduleSourcePosition(), prior.payloadReference(),
                    prior.retryEligibilityAtEpochMs()) : null;
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
                prior.runtimeIndex().possibleDestinationDuplicate(), prior.runtimeIndex().attemptObligations());
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

    /**
     * Reconciles the persisted runtime locator with every live Claim/attempt
     * ledger before the shard can serve work.  A checkpoint that loses one
     * side of this relationship is not safely replayable, so activation fails
     * closed instead of guessing a current obligation.
     */
    private void validateRuntimeObligationIndexes() {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> messageEntries = store.scan(ColumnFamily.ID,
                new byte[]{1, 1}, new byte[]{2, 1}, limit);
        if (messageEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message runtime-index scan exceeded configured bound");
        }
        final Map<DelayMessageId, MessageRecord> messages = new HashMap<>();
        for (var entry : messageEntries) {
            if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid MESSAGE key while reconciling runtime indexes");
            }
            final byte[] messageBytes = Arrays.copyOfRange(entry.key(), 2, entry.key().length);
            final DelayMessageId messageId = new DelayMessageId(messageBytes);
            final MessageRecord message = MessageRecord.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1).payload());
            messages.put(messageId, message);
            validateMessageRuntimeBranches(messageId, message);
            if (isTerminalStatus(message.status())) {
                validateTerminalSummary(messageId, message);
            }
            for (AttemptObligationRef obligation : message.runtimeIndex().attemptObligations()) {
                final PublishAttemptLedger ledger = readLedgerForObligation(obligation);
                if (!ledger.delayMessageId().equals(messageId)
                        || ledger.generation() != message.generation()
                        || ledger.state() != obligation.ledgerState()
                        || !Bytes.constantTimeEquals(ledger.publishAttemptId(), obligation.publishAttemptId())
                        || !Arrays.equals(ledger.obligationRef().canonicalBytes(), obligation.canonicalBytes())) {
                    throw new IllegalStateException("runtime obligation does not match its inflight ledger");
                }
            }
        }

        final Map<GenerationIdentity, TerminalGenerationRecord> terminalSummaries = new HashMap<>();
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> terminalEntries = store.scan(
                ColumnFamily.TERMINAL, new byte[]{1, 1}, new byte[]{2, 1}, limit);
        if (terminalEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("terminal summary reconciliation scan exceeded configured bound");
        }
        for (var entry : terminalEntries) {
            final byte[] key = entry.key();
            if (key.length != 2 + DelayMessageId.LENGTH + 4 || key[0] != 1 || key[1] != 1) {
                throw new IllegalStateException("invalid terminal summary key while reconciling runtime indexes");
            }
            final byte[] messageBytes = Arrays.copyOfRange(key, 2, 2 + DelayMessageId.LENGTH);
            final int generation = ByteBuffer.wrap(key, 2 + DelayMessageId.LENGTH, 4).getInt();
            final TerminalGenerationRecord summary = TerminalGenerationRecord.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1).payload());
            final GenerationIdentity identity = new GenerationIdentity(new DelayMessageId(messageBytes), generation);
            if (!summary.messageId().equals(identity.messageId()) || summary.generation() != identity.generation()
                    || terminalSummaries.put(identity, summary) != null) {
                throw new IllegalStateException("terminal summary key/value identity mismatch");
            }
            final MessageRecord current = messages.get(identity.messageId());
            if (current != null && current.generation() == identity.generation()) {
                validateTerminalSummary(identity.messageId(), current);
            }
            for (AttemptObligationRef obligation : summary.openObligations()) {
                final PublishAttemptLedger ledger = readLedgerForObligation(obligation);
                if (!ledger.delayMessageId().equals(identity.messageId())
                        || ledger.generation() != identity.generation()
                        || ledger.state() != obligation.ledgerState()
                        || !Arrays.equals(ledger.obligationRef().canonicalBytes(), obligation.canonicalBytes())) {
                    throw new IllegalStateException("terminal summary obligation does not match its inflight ledger");
                }
            }
        }

        final List<io.nereusstream.delay.store.ShardStore.KeyValue> claimEntries = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{INFLIGHT_CLAIMED_KIND, 1}, new byte[]{INFLIGHT_PUBLISHING_KIND, 1}, limit);
        if (claimEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("Claim reconciliation scan exceeded configured bound");
        }
        final Set<DelayMessageId> claimedMessages = new HashSet<>();
        for (var entry : claimEntries) {
            final ClaimRecord claim = decodeClaim(entry);
            final MessageRecord message = messages.get(claim.delayMessageId());
            if (message == null || message.status() != MessageStatus.CLAIMED
                    || message.generation() != claim.generation()
                    || message.runtimeIndex().currentWorkKind() != CurrentSendWorkKind.CLAIMED
                    || !Bytes.constantTimeEquals(message.runtimeIndex().claimId(), claim.claimId())) {
                throw new IllegalStateException("Claim is not represented by the current runtime index");
            }
            if (!claimedMessages.add(claim.delayMessageId())) {
                throw new IllegalStateException("message has multiple live Claim records");
            }
        }
        for (var entry : messageEntries) {
            final DelayMessageId messageId = new DelayMessageId(Arrays.copyOfRange(entry.key(), 2, entry.key().length));
            final MessageRecord message = messages.get(messageId);
            if (message.runtimeIndex().currentWorkKind() == CurrentSendWorkKind.CLAIMED
                    && !claimedMessages.contains(messageId)) {
                throw new IllegalStateException("CLAIMED runtime index has no live Claim record");
            }
        }

        final List<io.nereusstream.delay.store.ShardStore.KeyValue> attemptEntries = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{INFLIGHT_PUBLISHING_KIND, 1}, new byte[]{4, 1}, limit);
        if (attemptEntries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("attempt reconciliation scan exceeded configured bound");
        }
        for (var entry : attemptEntries) {
            final PublishAttemptLedger ledger = decodePublishAttempt(entry);
            final MessageRecord message = messages.get(ledger.delayMessageId());
            final boolean inCurrentRuntime = message != null && message.generation() == ledger.generation()
                    && containsObligation(message.runtimeIndex(), ledger.obligationRef());
            final TerminalGenerationRecord summary = terminalSummaries.get(
                    new GenerationIdentity(ledger.delayMessageId(), ledger.generation()));
            final boolean inTerminalSummary = summary != null
                    && summary.openObligations().stream().anyMatch(obligation ->
                    Arrays.equals(obligation.canonicalBytes(), ledger.obligationRef().canonicalBytes()));
            if (!inCurrentRuntime && !inTerminalSummary) {
                throw new IllegalStateException("inflight ledger is not represented by the current runtime index");
            }
        }
    }

    private void validateMessageRuntimeBranches(final DelayMessageId messageId, final MessageRecord message) {
        final GenerationRuntimeIndex index = message.runtimeIndex();
        if (index.admissionsUsed() > config.maxPublishAdmissions()) {
            throw new IllegalStateException("persisted generation exceeds publish admission budget");
        }
        if (index.currentWorkKind() == CurrentSendWorkKind.CLAIMED
                && (message.status() != MessageStatus.CLAIMED || index.claimId().length != ClaimRecord.HASH_LENGTH)) {
            throw new IllegalStateException("CLAIMED runtime branch does not match Message status");
        }
        if (index.currentWorkKind() == CurrentSendWorkKind.PUBLISHING
                && (message.status() != MessageStatus.PUBLISHING
                || index.publishAttemptId().length != PublishAttemptLedger.HASH_LENGTH)) {
            throw new IllegalStateException("PUBLISHING runtime branch does not match Message status");
        }
        if (index.currentWorkKind() == CurrentSendWorkKind.TIMELINE
                && (message.status() != MessageStatus.SCHEDULED || index.timeline() == null)) {
            throw new IllegalStateException("TIMELINE runtime branch does not match Message status");
        }
        if (index.currentWorkKind() == CurrentSendWorkKind.NONE
                && (message.status() == MessageStatus.CLAIMED || message.status() == MessageStatus.PUBLISHING)) {
            throw new IllegalStateException("Message status has no current runtime branch");
        }
        if (index.currentWorkKind() == CurrentSendWorkKind.PUBLISHING) {
            final long matches = index.attemptObligations().stream()
                    .filter(ref -> ref.ledgerState() == AttemptLedgerState.PUBLISHING
                            && Arrays.equals(ref.publishAttemptId(), index.publishAttemptId()))
                    .count();
            if (matches != 1) {
                throw new IllegalStateException("PUBLISHING runtime branch lacks its obligation locator");
            }
        }
    }

    private void validateTerminalSummary(final DelayMessageId messageId, final MessageRecord message) {
        final TerminalGenerationRecord summary = getTerminalGeneration(messageId, message.generation());
        if (summary == null || summary.status() != message.status()
                || !summary.openObligations().equals(message.runtimeIndex().attemptObligations())
                || summary.possibleDestinationDuplicate() != message.runtimeIndex().possibleDestinationDuplicate()) {
            throw new IllegalStateException("terminal runtime and open-obligation summary disagree");
        }
    }

    private PublishAttemptLedger readLedgerForObligation(final AttemptObligationRef obligation) {
        final byte[] key = obligation.encodedInflightKey();
        if (key.length != 2 + 8 + 4 + PublishAttemptLedger.HASH_LENGTH
                || key[1] != 1
                || (key[0] != INFLIGHT_PUBLISHING_KIND && key[0] != INFLIGHT_UNCERTAIN_KIND)) {
            throw new IllegalStateException("runtime obligation has an invalid inflight key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final long ownerEpoch = input.getLong();
        final long idLength = Integer.toUnsignedLong(input.getInt());
        final byte[] attemptId = new byte[PublishAttemptLedger.HASH_LENGTH];
        input.get(attemptId);
        if (ownerEpoch <= 0 || idLength != PublishAttemptLedger.HASH_LENGTH
                || !Bytes.constantTimeEquals(attemptId, obligation.publishAttemptId())) {
            throw new IllegalStateException("runtime obligation inflight identity is invalid");
        }
        final var value = store.getValue(ColumnFamily.INFLIGHT, key, PublishAttemptLedger.VALUE_TYPE);
        if (value == null) {
            throw new IllegalStateException("runtime obligation points to a missing inflight ledger");
        }
        final PublishAttemptLedger ledger = PublishAttemptLedger.decode(value.payload());
        validatePublishAttemptKey(ledger, key, key[0], obligation.publishAttemptId(), ownerEpoch);
        return ledger;
    }

    private static boolean containsObligation(final GenerationRuntimeIndex index,
                                              final AttemptObligationRef expected) {
        return index.attemptObligations().stream()
                .anyMatch(actual -> Arrays.equals(actual.canonicalBytes(), expected.canonicalBytes()));
    }

    private LaneRecord readLane(final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final var value = store.getValue(ColumnFamily.META, KeyCodec.metaLane(laneId), 2);
        return value == null ? null : LaneRecord.decode(value.payload());
    }

    private PublishAttemptLedger readPublishAttempt(final byte[] publishAttemptId, final long ownerEpoch,
                                                    final byte recordKind) {
        final byte[] key = KeyCodec.inflight(recordKind, ownerEpoch, publishAttemptId);
        final var value = store.getValue(ColumnFamily.INFLIGHT, key, PublishAttemptLedger.VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final PublishAttemptLedger ledger = PublishAttemptLedger.decode(value.payload());
        validatePublishAttemptKey(ledger, key, recordKind, publishAttemptId, ownerEpoch);
        return ledger;
    }

    private ClaimRecord decodeClaim(final io.nereusstream.delay.store.ShardStore.KeyValue entry) {
        final byte[] key = entry.key();
        if (key.length != 2 + 8 + 4 + ClaimRecord.HASH_LENGTH
                || key[0] != INFLIGHT_CLAIMED_KIND || key[1] != 1) {
            throw new IllegalStateException("invalid Claim key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final long ownerEpoch = input.getLong();
        final long idLength = Integer.toUnsignedLong(input.getInt());
        if (ownerEpoch <= 0 || idLength != ClaimRecord.HASH_LENGTH) {
            throw new IllegalStateException("invalid Claim key owner/ID length");
        }
        final byte[] claimId = new byte[ClaimRecord.HASH_LENGTH];
        input.get(claimId);
        final ClaimRecord claim = ClaimRecord.decode(
                io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), ClaimRecord.VALUE_TYPE).payload());
        validateClaimKey(claim, key, claimId, ownerEpoch);
        return claim;
    }

    private static void validateClaimKey(final ClaimRecord claim, final byte[] key, final byte[] claimId,
                                         final long ownerEpoch) {
        if (!Arrays.equals(key, claim.encodedKey()) || !Arrays.equals(claim.claimId(), claimId)
                || claim.ownerEpoch() != ownerEpoch) {
            throw new IllegalStateException("Claim key/value identity mismatch");
        }
    }

    private PublishAttemptLedger decodePublishAttempt(final io.nereusstream.delay.store.ShardStore.KeyValue entry) {
        final byte[] key = entry.key();
        if (key.length != 2 + 8 + 4 + PublishAttemptLedger.HASH_LENGTH
                || (key[0] != INFLIGHT_PUBLISHING_KIND && key[0] != INFLIGHT_UNCERTAIN_KIND) || key[1] != 1) {
            throw new IllegalStateException("invalid open publish attempt key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(2);
        final long ownerEpoch = input.getLong();
        if (ownerEpoch <= 0) {
            throw new IllegalStateException("invalid open publish attempt owner epoch");
        }
        final long idLength = Integer.toUnsignedLong(input.getInt());
        if (idLength != PublishAttemptLedger.HASH_LENGTH) {
            throw new IllegalStateException("invalid open publish attempt ID length");
        }
        final byte[] attemptId = new byte[PublishAttemptLedger.HASH_LENGTH];
        input.get(attemptId);
        final PublishAttemptLedger ledger = PublishAttemptLedger.decode(
                io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), PublishAttemptLedger.VALUE_TYPE)
                        .payload());
        validatePublishAttemptKey(ledger, key, key[0], attemptId, ownerEpoch);
        return ledger;
    }

    private static void validatePublishAttemptKey(final PublishAttemptLedger ledger, final byte[] key,
                                                   final byte recordKind, final byte[] publishAttemptId,
                                                   final long ownerEpoch) {
        final byte expectedKind = ledger.state() == AttemptLedgerState.PUBLISHING
                ? INFLIGHT_PUBLISHING_KIND : INFLIGHT_UNCERTAIN_KIND;
        if (recordKind != expectedKind || !Arrays.equals(key, ledger.encodedKey())
                || ledger.ownerEpoch() != ownerEpoch || !Bytes.constantTimeEquals(ledger.publishAttemptId(),
                publishAttemptId)) {
            throw new IllegalStateException("open publish attempt key/value mismatch");
        }
    }

    private byte[] buildClaimPrecondition(final byte[] claimId, final DelayMessageId messageId,
                                          final MessageRecord current, final LaneRecord lane,
                                          final byte[] timelineKey, final AuthorIdentity owner,
                                          final long claimDeadlineEpochMs, final byte[] materialization,
                                          final byte[] claimedCharge, final int workKind) {
        final byte[] normalizedMaterialization = materialization == null ? new byte[0] : Bytes.copy(materialization);
        final byte[] normalizedCharge = Bytes.copy(Objects.requireNonNull(claimedCharge, "claimedCharge"));
        final TimelineWorkRef sourceWork = current.runtimeIndex().timeline() != null
                && Arrays.equals(current.runtimeIndex().timeline().encodedTimelineKey(), timelineKey)
                ? current.runtimeIndex().timeline()
                : timelineRuntimeIndex(messageId, current,
                workKind == 1 ? TimelineWorkKind.INITIAL_SCHEDULE : TimelineWorkKind.DEFINITIVE_RETRY,
                Math.addExact(current.runtimeIndex().admissionsUsed(), 1), current.stateVersion(),
                UncertainRetryAuthority.NONE, null, null, current.runtimeIndex()).timeline();
        final byte[] semanticDigest = sourceWork.semanticWorkDigest();
        final int admissionsUsed = current.runtimeIndex().admissionsUsed();
        final int uncertainRetryAdmissionsUsed = current.runtimeIndex().uncertainRetryAdmissionsUsed();
        final byte[] obligationSetDigest = GenerationRuntimeIndex.obligationSetDigest(
                current.runtimeIndex().attemptObligations());
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, claimId);
            CanonicalProtobuf.bytes(output, 2, messageId.bytes());
            CanonicalProtobuf.uint32(output, 3, current.generation());
            CanonicalProtobuf.int64(output, 4, current.stateVersion());
            CanonicalProtobuf.bytes(output, 5, current.laneId().bytes());
            CanonicalProtobuf.bytes(output, 6, lane.laneIncarnation());
            CanonicalProtobuf.int64(output, 7, lane.laneControlVersion());
            CanonicalProtobuf.int64(output, 8, lane.laneVersion());
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(timelineKey));
            if (normalizedMaterialization.length != 0) {
                CanonicalProtobuf.bytes(output, 10, normalizedMaterialization);
                CanonicalProtobuf.bytes(output, 11, Bytes.sha256(
                        Bytes.utf8("nereus-delay-claim-materialization-v1\0"), normalizedMaterialization));
            }
            CanonicalProtobuf.bytes(output, 12, normalizedCharge);
            CanonicalProtobuf.int64(output, 13, claimDeadlineEpochMs);
            CanonicalProtobuf.bytes(output, 14, owner.canonicalBytes());
            CanonicalProtobuf.bytes(output, 15, store.metadata().storeIncarnation());
            CanonicalProtobuf.uint32(output, 16, workKind);
            CanonicalProtobuf.uint32(output, 17, admissionsUsed);
            CanonicalProtobuf.uint32(output, 18, uncertainRetryAdmissionsUsed);
            CanonicalProtobuf.bytes(output, 19, obligationSetDigest);
            CanonicalProtobuf.bytes(output, 20, semanticDigest);
        });
        // This validates ChargeVector, optional Materialization and every closed
        // ClaimPrecondition field before the bytes become durable.
        ClaimResultBody.decodePrecondition(encoded);
        return encoded;
    }

    private void validateMutationPosition(final SourcePosition sourcePosition) {
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("system mutation position does not belong to shard");
        }
        if (lastAppliedSourcePosition != null && sourcePosition.compareTo(lastAppliedSourcePosition) <= 0) {
            throw new IllegalStateException("system mutation source position is not strictly increasing");
        }
    }

    private byte[] timelineKey(final DelayMessageId messageId, final MessageRecord message) {
        final long eligibleAt = timelineEligibilityAt(message);
        final SourcePosition position = SourcePositionCodec.decode(message.scheduleSourcePosition());
        return message.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? KeyCodec.timelineOrdered(message.laneId(), eligibleAt, position.sourceOrderToken(), messageId,
                message.generation())
                : KeyCodec.timelineDue(message.laneId(), eligibleAt, position.sourceOrderToken(), messageId,
                message.generation());
    }

    private static long timelineEligibilityAt(final MessageRecord message) {
        return message.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO
                ? message.deliverAtEpochMs() : message.retryEligibilityAtEpochMs();
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

    private record AdmissionReplayState(boolean claimMayBeMissing, boolean uncertainRetryAdmission) {
    }

    private record GenerationIdentity(DelayMessageId messageId, int generation) {
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
