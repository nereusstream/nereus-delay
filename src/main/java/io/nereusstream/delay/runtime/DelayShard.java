package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ApplyShardControlBody;
import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.CommandBodies;
import io.nereusstream.delay.protocol.CommandId;
import io.nereusstream.delay.protocol.ClaimResultBody;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CapacityDimensionV1;
import io.nereusstream.delay.protocol.CapacityGrantV1;
import io.nereusstream.delay.protocol.CapacityVectorV1;
import io.nereusstream.delay.protocol.CancelCommandBodyV1;
import io.nereusstream.delay.protocol.CommitLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DlqExportStateV1;
import io.nereusstream.delay.protocol.DlqExportResultBody;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.LaneRecordEnvelopeV1;
import io.nereusstream.delay.protocol.LaneRetirementProgressV1;
import io.nereusstream.delay.protocol.LaneTerminalGuardV1;
import io.nereusstream.delay.protocol.PayloadCommitProofView;
import io.nereusstream.delay.protocol.PayloadProofTrustSet;
import io.nereusstream.delay.protocol.PayloadProofTrustSetControlState;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import io.nereusstream.delay.protocol.PayloadReference;
import io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.ReplayDeadLetterBody;
import io.nereusstream.delay.protocol.ResolveUncertainBody;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.RescheduleCommandBodyV1;
import io.nereusstream.delay.protocol.ScheduleCommandBodyV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.ShardCapacityEnvelopeV1;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.SystemMutationBodyCodec;
import io.nereusstream.delay.protocol.SystemMutationType;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.protocol.V1ScheduleBinding;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.KeyCodec;
import io.nereusstream.delay.store.RecoveryCatalogAuthority;
import io.nereusstream.delay.store.ShardStore;
import io.nereusstream.delay.store.ValueEnvelope;

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
    private static final int META_CLOSED_INGRESS_DEADLINE = 4;
    private static final int META_MUTATION_SEQUENCE = 5;
    private static final int META_CLAIM_SEQUENCE = 11;
    private static final int META_PAYLOAD_PROOF_CONTROL_STATE = 12;
    private static final int PAYLOAD_PROOF_CONTROL_VALUE_TYPE = 9;
    private static final int META_QUOTA_USAGE = 1;
    private static final int META_OUTCOME_RESERVE_USAGE = 2;
    private static final int CAPACITY_RESERVE_VALUE_TYPE = 8;
    private static final byte INFLIGHT_CLAIMED_KIND = 1;
    private static final byte INFLIGHT_PUBLISHING_KIND = 2;
    private static final byte INFLIGHT_UNCERTAIN_KIND = 3;

    private final ShardStore store;
    private final DelayShardConfig config;
    private final PayloadProofTrustSet payloadProofTrustSet;
    private final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog;
    private PayloadProofTrustSetControlState payloadProofTrustSetControlState;
    private final ShardCapacityEnvelopeV1 capacityEnvelope;
    private final V1ScheduleResolver v1ScheduleResolver;
    /** Single-writer scratch; consumed by the same apply turn before the batch is written. */
    private V1ScheduleResolver.ResolvedSchedule lastResolvedSchedule;
    private V1ScheduleResolver.ResolvedPrepare lastResolvedPrepare;
    private SourcePosition lastAppliedSourcePosition;
    private long closedIngressDeadlineThrough;
    private long mutationSequence;
    private long claimSequence;
    private ShardQuota quota;
    private OutcomeReserveUsage outcomeReserve;
    private CapacityVectorV1 outcomeReserveVector;
    private final Map<Integer, CapacityVectorV1> controlReserveUsage = new HashMap<>();

    public DelayShard(final ShardStore store, final DelayShardConfig config) {
        this(store, config, null, null, null, null);
    }

    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet) {
        this(store, config, payloadProofTrustSet, null, null, null);
    }

    /**
     * Opens a shard with an immutable capacity envelope supplied by the
     * placement/activation authority.  The envelope is persisted before any
     * command can charge its outcome grant; a different grant or digest can
     * therefore never reuse this DB's usage projection.
     */
    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet,
                      final ShardCapacityEnvelopeV1 capacityEnvelope) {
        this(store, config, payloadProofTrustSet, capacityEnvelope, null, null);
    }

    /**
     * Opens a shard with an explicit source-position-pinned V1 Schedule
     * resolver.  The resolver is optional for legacy commands; a Registry
     * Schedule/Prepare body without one fails closed with
     * {@link StableCode#ROUTE_SNAPSHOT_UNAVAILABLE}.
     */
    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet,
                      final ShardCapacityEnvelopeV1 capacityEnvelope,
                      final V1ScheduleResolver v1ScheduleResolver) {
        this(store, config, payloadProofTrustSet, capacityEnvelope, v1ScheduleResolver, null);
    }

    /**
     * Opens a shard with the local source-ordered trust-set marker projection.
     * The catalog is required before kind-12/kind-13 controls can mutate the
     * projection; missing or mismatched semantic bytes fail closed.
     */
    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet,
                      final ShardCapacityEnvelopeV1 capacityEnvelope,
                      final V1ScheduleResolver v1ScheduleResolver,
                      final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog) {
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.payloadProofTrustSet = payloadProofTrustSet;
        this.payloadProofTrustSetControlCatalog = payloadProofTrustSetControlCatalog;
        this.capacityEnvelope = capacityEnvelope;
        this.v1ScheduleResolver = v1ScheduleResolver;
        final var sourceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_APPLIED_SOURCE_POSITION), 1);
        final byte[] source = sourceValue == null ? null : sourceValue.payload();
        lastAppliedSourcePosition = source == null ? null : SourcePositionCodec.decode(source);
        final var closedDeadline = store.getValue(ColumnFamily.META,
                KeyCodec.metaFixed(META_CLOSED_INGRESS_DEADLINE), 1);
        closedIngressDeadlineThrough = closedDeadline == null ? -1 : readNonNegativeSequence(closedDeadline.payload());
        final var sequence = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_MUTATION_SEQUENCE), 1);
        mutationSequence = sequence == null ? 0 : readSequence(sequence.payload());
        final var claimSequenceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_CLAIM_SEQUENCE), 1);
        claimSequence = claimSequenceValue == null ? 0 : readSequence(claimSequenceValue.payload());
        final var payloadProofControlValue = store.getValue(ColumnFamily.META,
                KeyCodec.metaFixed(META_PAYLOAD_PROOF_CONTROL_STATE), PAYLOAD_PROOF_CONTROL_VALUE_TYPE);
        payloadProofTrustSetControlState = payloadProofControlValue == null
                ? PayloadProofTrustSetControlState.empty()
                : PayloadProofTrustSetControlState.decode(payloadProofControlValue.payload());
        final var quotaValue = store.getValue(ColumnFamily.META, KeyCodec.metaQuota(META_QUOTA_USAGE), 7);
        quota = quotaValue == null ? ShardQuota.empty() : ShardQuota.decode(quotaValue.payload());
        final var outcomeReserveValue = store.getValue(ColumnFamily.META,
                KeyCodec.metaQuota(META_OUTCOME_RESERVE_USAGE), 7);
        outcomeReserve = outcomeReserveValue == null
                ? OutcomeReserveUsage.empty() : OutcomeReserveUsage.decode(outcomeReserveValue.payload());
        if (outcomeReserve.records() > config.maxOutcomeReserveRecords()
                || outcomeReserve.bytes() > config.maxOutcomeReserveBytes()) {
            throw new IllegalStateException("persisted outcome reserve exceeds the active shard grant");
        }
        outcomeReserveVector = loadCapacityEnvelopeState(capacityEnvelope);
        loadControlReserveUsage(capacityEnvelope);
        if (capacityEnvelope != null
                && !outcomeReserve.equals(outcomeReserveUsage(outcomeReserveVector))) {
            throw new IllegalStateException("persisted outcome reserve projections disagree");
        }
        validateRuntimeObligationIndexes();
    }

    /** Returns the persisted source-ordered trust-set marker projection. */
    public synchronized PayloadProofTrustSetControlState payloadProofTrustSetControlState() {
        return payloadProofTrustSetControlState;
    }

    public synchronized CommandResult apply(final PreparedCommand command, final SourcePosition sourcePosition) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        lastResolvedSchedule = null;
        lastResolvedPrepare = null;
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
        if (closedIngressDeadlineThrough >= 0
                && command.retryUntilEpochMs() <= closedIngressDeadlineThrough) {
            return persistRejectedPositionOnly(command, sourcePosition,
                    StableCode.COMMAND_RETRY_WINDOW_EXPIRED);
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
        } catch (V1CommandResolutionException exception) {
            return persistRejected(command, sourcePosition, exception.stableCode());
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
        persistResultAndPosition(command, sourcePosition, result, nextMessage(command, sourcePosition, result));
        return result;
    }

    private CapacityVectorV1 loadCapacityEnvelopeState(final ShardCapacityEnvelopeV1 envelope) {
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> bindingEntries =
                scanControlReserveClass(1);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> usageEntries =
                scanControlReserveClass(2);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> nonOutcomeEntries =
                scanControlReserveClass(3);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> recoveryEntries =
                scanControlReserveClass(4);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> emergencyEntries =
                scanControlReserveClass(5);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> systemWriterEntries =
                scanControlReserveClass(6);
        if (envelope == null) {
            if (!bindingEntries.isEmpty() || !usageEntries.isEmpty() || !nonOutcomeEntries.isEmpty()
                    || !recoveryEntries.isEmpty() || !emergencyEntries.isEmpty() || !systemWriterEntries.isEmpty()) {
                throw new IllegalStateException("capacity envelope is required for persisted control reserve state");
            }
            return CapacityVectorV1.empty();
        }
        final byte[] bindingKey = KeyCodec.metaControlReserve(1, envelope.outcomeReserve().grantId());
        if (bindingEntries.size() > 1 || !bindingEntries.isEmpty()
                && !Arrays.equals(bindingEntries.get(0).key(), bindingKey)) {
            throw new IllegalStateException("persisted control reserve grant identity does not match envelope");
        }
        final byte[] usageKey = KeyCodec.metaControlReserve(2, envelope.outcomeReserve().grantId());
        if (usageEntries.size() > 1 || !usageEntries.isEmpty()
                && !Arrays.equals(usageEntries.get(0).key(), usageKey)) {
            throw new IllegalStateException("persisted outcome reserve key does not match envelope grant");
        }
        final ValueEnvelope.Decoded persistedBinding = store.getValue(ColumnFamily.META, bindingKey,
                CAPACITY_RESERVE_VALUE_TYPE);
        if (persistedBinding == null) {
            if (!usageEntries.isEmpty()) {
                throw new IllegalStateException("persisted outcome reserve has no envelope identity");
            }
            store.write(batch -> batch.putValue(ColumnFamily.META, CAPACITY_RESERVE_VALUE_TYPE, bindingKey,
                    envelope.canonicalBytes()));
        } else if (!envelope.equals(ShardCapacityEnvelopeV1.decode(persistedBinding.payload()))) {
            throw new IllegalStateException("persisted capacity envelope identity differs from active envelope");
        }
        reserveProjectionUsage(nonOutcomeEntries, 3, envelope.nonOutcomeControl(), "non-outcome control");
        reserveProjectionUsage(recoveryEntries, 4, envelope.recoveryWorking(), "recovery working");
        reserveProjectionUsage(emergencyEntries, 5, envelope.emergencyHeadroom(), "emergency headroom");
        if (!systemWriterEntries.isEmpty()) {
            throw new IllegalStateException("Broker system-writer reserve projection is not implemented");
        }
        final ValueEnvelope.Decoded persistedUsage = store.getValue(ColumnFamily.META, usageKey,
                CAPACITY_RESERVE_VALUE_TYPE);
        final CapacityVectorV1 usage = persistedUsage == null
                ? CapacityVectorV1.empty() : CapacityVectorV1.decode(persistedUsage.payload());
        if (!envelope.outcomeReserve().vector().covers(usage)) {
            throw new IllegalStateException("persisted outcome reserve exceeds immutable capacity grant");
        }
        return usage;
    }

    private void loadControlReserveUsage(final ShardCapacityEnvelopeV1 envelope) {
        if (envelope == null) {
            return;
        }
        controlReserveUsage.put(3, reserveProjectionUsage(scanControlReserveClass(3), 3,
                envelope.nonOutcomeControl(), "non-outcome control"));
        controlReserveUsage.put(4, reserveProjectionUsage(scanControlReserveClass(4), 4,
                envelope.recoveryWorking(), "recovery working"));
        controlReserveUsage.put(5, reserveProjectionUsage(scanControlReserveClass(5), 5,
                envelope.emergencyHeadroom(), "emergency headroom"));
    }

    private CapacityVectorV1 reserveProjectionUsage(
            final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries, final int reserveClass,
            final CapacityGrantV1 grant, final String name) {
        if (entries.size() > 1) {
            throw new IllegalStateException("multiple " + name + " reserve projections exist");
        }
        if (entries.isEmpty()) {
            return CapacityVectorV1.empty();
        }
        final byte[] expectedKey = KeyCodec.metaControlReserve(reserveClass, grant.grantId());
        if (!Arrays.equals(entries.get(0).key(), expectedKey)) {
            throw new IllegalStateException(name + " reserve grant identity does not match envelope");
        }
        final CapacityVectorV1 usage = CapacityVectorV1.decode(
                ValueEnvelope.decode(entries.get(0).value(), CAPACITY_RESERVE_VALUE_TYPE).payload());
        if (!grant.vector().covers(usage)) {
            throw new IllegalStateException(name + " reserve usage exceeds immutable capacity grant");
        }
        return usage;
    }

    private List<io.nereusstream.delay.store.ShardStore.KeyValue> scanControlReserveClass(final int reserveClass) {
        final byte[] lower = new byte[]{6, 1, (byte) reserveClass};
        final byte[] upper = new byte[]{6, 1, (byte) Math.addExact(reserveClass, 1)};
        return store.scan(ColumnFamily.META, lower, upper, 2);
    }

    private CapacityVectorV1 mutateControlReserve(final int reserveClass, final CapacityVectorV1 amount,
                                                  final boolean add) {
        validateMutableControlReserveClass(reserveClass);
        if (capacityEnvelope == null) {
            throw new IllegalStateException("capacity envelope is required for control reserve accounting");
        }
        final CapacityGrantV1 grant = controlReserveGrant(reserveClass);
        final CapacityVectorV1 current = controlReserveUsage.getOrDefault(reserveClass, CapacityVectorV1.empty());
        final CapacityVectorV1 next = add ? current.add(amount) : current.subtract(amount);
        if (!grant.vector().covers(next)) {
            throw new IllegalStateException("control reserve exceeds immutable capacity grant");
        }
        final byte[] key = KeyCodec.metaControlReserve(reserveClass, grant.grantId());
        store.write(batch -> {
            if (next.isZero()) {
                batch.delete(ColumnFamily.META, key);
            } else {
                batch.putValue(ColumnFamily.META, CAPACITY_RESERVE_VALUE_TYPE, key, next.canonicalBytes());
            }
        });
        controlReserveUsage.put(reserveClass, next);
        return next;
    }

    private CapacityGrantV1 controlReserveGrant(final int reserveClass) {
        return switch (reserveClass) {
            case 3 -> capacityEnvelope.nonOutcomeControl();
            case 4 -> capacityEnvelope.recoveryWorking();
            case 5 -> capacityEnvelope.emergencyHeadroom();
            default -> throw new IllegalArgumentException("unsupported mutable control reserve class: "
                    + reserveClass);
        };
    }

    private static void validateMutableControlReserveClass(final int reserveClass) {
        if (reserveClass < 3 || reserveClass > 5) {
            throw new IllegalArgumentException("only CONTROL_RESERVE classes 3-5 are mutable locally");
        }
    }

    private static OutcomeReserveUsage outcomeReserveUsage(final CapacityVectorV1 vector) {
        final long records = Math.addExact(
                Math.addExact(vector.amount(CapacityDimensionV1.RESULT_RECORDS),
                        vector.amount(CapacityDimensionV1.SYSTEM_MUTATION_RECORDS)),
                vector.amount(CapacityDimensionV1.EVIDENCE_RECORDS));
        final long bytes = Math.addExact(
                Math.addExact(vector.amount(CapacityDimensionV1.RESULT_BYTES),
                        vector.amount(CapacityDimensionV1.SYSTEM_MUTATION_BYTES)),
                Math.addExact(vector.amount(CapacityDimensionV1.OUTCOME_WAL_BYTES),
                        vector.amount(CapacityDimensionV1.EVIDENCE_BYTES)));
        return new OutcomeReserveUsage(records, bytes);
    }

    public synchronized MessageRecord getMessage(final DelayMessageId messageId) {
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idMessage(messageId), 1);
        return value == null ? null : MessageRecord.decode(value.payload());
    }

    /** Returns the exact accepted Registry Schedule/Prepare binding, if any. */
    public synchronized V1ScheduleBinding getV1ScheduleBinding(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idV1ScheduleBinding(messageId), 4);
        if (value == null) {
            return null;
        }
        final V1ScheduleBinding binding = V1ScheduleBinding.decode(value.payload());
        if (!binding.delayMessageId().equals(messageId)) {
            throw new IllegalStateException("V1 Schedule binding key/value identity mismatch");
        }
        final MessageRecord message = getMessage(messageId);
        if (message != null && !message.laneId().equals(binding.laneId())) {
            throw new IllegalStateException("V1 Schedule binding Lane does not match message");
        }
        return binding;
    }

    /**
     * Returns the bounded local query projection for a current Message generation.
     * A missing ID returns {@code null}; it is not evidence that the ID never existed.
     */
    public synchronized MessageQuerySnapshot queryMessageSnapshot(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        final MessageRecord current = getMessage(messageId);
        if (current == null) {
            return null;
        }
        final GenerationAggregateState state = current.runtimeIndex().aggregateState();
        final StableCode terminalCode;
        if (isTerminalAggregateState(state)) {
            final TerminalGenerationRecord terminal = getTerminalGeneration(messageId, current.generation());
            if (terminal == null || terminal.status() != current.status()) {
                throw new IllegalStateException("terminal query projection has no matching history");
            }
            terminalCode = terminal.terminalCode();
        } else {
            terminalCode = null;
        }
        return new MessageQuerySnapshot(messageId, current.generation(), current.stateVersion(), state,
                current.deliverAtEpochMs(), current.expireAtEpochMs(), payloadAvailability(current),
                current.runtimeIndex().possibleDestinationDuplicate(), terminalCode,
                terminalCode == null ? DlqExportStateV1.NOT_CONFIGURED
                        : dlqExportState(messageId, current.generation()));
    }

    /** Returns the durable local DLQ export outbox for one terminal generation. */
    public synchronized DlqExportRecord getDlqExportRecord(final DelayMessageId messageId, final int generation) {
        Objects.requireNonNull(messageId, "messageId");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        final TerminalGenerationRecord terminal = getTerminalGeneration(messageId, generation);
        if (terminal == null || terminal.status() != MessageStatus.DEAD_LETTER) {
            return null;
        }
        final byte[] exportId = DlqExportRecord.deriveId(messageId, generation, terminal.stateVersion());
        final ValueEnvelope.Decoded value = store.getValue(ColumnFamily.TERMINAL,
                KeyCodec.terminalDlqExport(exportId), DlqExportRecord.VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final DlqExportRecord result = DlqExportRecord.decode(value.payload());
        if (!result.messageId().equals(messageId) || result.generation() != generation
                || result.terminalRevision() != terminal.stateVersion()) {
            throw new IllegalStateException("DLQ export record does not match terminal generation");
        }
        return result;
    }

    /** Returns the durable export state, with legacy terminals defaulting to NOT_CONFIGURED. */
    public synchronized DlqExportStateV1 dlqExportState(final DelayMessageId messageId, final int generation) {
        final DlqExportRecord record = getDlqExportRecord(messageId, generation);
        return record == null ? DlqExportStateV1.NOT_CONFIGURED : record.state();
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
        return value == null ? null : effectiveReservation(PayloadReservation.decode(value.payload()));
    }

    /**
     * Returns a safe, bounded projection of a payload reservation without exposing
     * command hashes, object keys, object versions or inline payload bytes.
     */
    public synchronized ReservationQuerySnapshot queryReservationSnapshot(final byte[] reservationId) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        final PayloadReservation reservation = getReservation(reservationId);
        if (reservation == null) {
            return null;
        }
        final PayloadAvailability availability = switch (reservation.status()) {
            case RESERVED -> PayloadAvailability.UPLOAD_PENDING;
            case COMMITTED -> PayloadAvailability.OBJECT_RETAINED;
            case ABANDONED, EXPIRED -> PayloadAvailability.NOT_APPLICABLE;
        };
        return new ReservationQuerySnapshot(reservation.reservationId(), reservation.delayMessageId(),
                reservation.stateVersion(), reservation.status(), reservation.reservationExpiryEpochMs(),
                availability);
    }

    /**
     * Materializes a reservation that has already been logically expired by a
     * persisted TIME_FENCE watermark.  This is a local bounded cursor action,
     * not a new source-log decision.
     */
    public synchronized PayloadReservation materializeReservationExpiry(final byte[] reservationId) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idReservation(reservationId), 2);
        if (value == null) {
            return null;
        }
        final PayloadReservation current = PayloadReservation.decode(value.payload());
        if (current.status() != PayloadReservationStatus.RESERVED
                || closedIngressDeadlineThrough < current.reservationExpiryEpochMs()) {
            return effectiveReservation(current);
        }
        final PayloadReservation expired = new PayloadReservation(current.shardId(), current.reservationId(),
                current.commandId(), current.delayMessageId(), current.commandHash(), current.intent(),
                current.reservationExpiryEpochMs(), PayloadReservationStatus.EXPIRED,
                Math.addExact(current.stateVersion(), 1), current.sourcePosition(), null);
        final ShardQuota nextQuota = quota.removeReservation(current.intent().expectedPayloadLength());
        store.write(batch -> {
            batch.putValue(ColumnFamily.ID, 2, KeyCodec.idReservation(expired.reservationId()), expired.encode());
            batch.delete(ColumnFamily.TIMELINE,
                    KeyCodec.reservationExpiry(expired.reservationExpiryEpochMs(), expired.reservationId()));
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
        });
        quota = nextQuota;
        return expired;
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

    /** Returns the durable gc_cf retire intent for one exact resource identity/version. */
    public synchronized ResourceRetireIntentRecord getResourceRetireIntent(final ResourceKind resourceKind,
                                                                              final byte[] resourceIdentityHash,
                                                                              final long expectedVersion) {
        Objects.requireNonNull(resourceKind, "resourceKind");
        Bytes.requireLength(resourceIdentityHash, SystemMutation.HASH_LENGTH, "resourceIdentityHash");
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
        final byte[] raw = gcValue(resourceKind, resourceIdentityHash, expectedVersion);
        if (raw == null) {
            return null;
        }
        final int valueType = gcValueType(raw);
        if (valueType == ResourceRetireIntentRecord.VALUE_TYPE) {
            return ResourceRetireIntentRecord.decode(
                    ValueEnvelope.decode(raw, ResourceRetireIntentRecord.VALUE_TYPE).payload());
        }
        if (valueType == ResourceDeleteConfirmedRecord.VALUE_TYPE) {
            return ResourceDeleteConfirmedRecord.decode(
                    ValueEnvelope.decode(raw, ResourceDeleteConfirmedRecord.VALUE_TYPE).payload()).retireIntent();
        }
        throw new IllegalStateException("unknown gc task value type: " + valueType);
    }

    /** Returns the durable delete confirmation, if this exact task has reached that local state. */
    public synchronized ResourceDeleteConfirmedRecord getResourceDeleteConfirmation(final ResourceKind resourceKind,
                                                                                      final byte[] resourceIdentityHash,
                                                                                      final long expectedVersion) {
        Objects.requireNonNull(resourceKind, "resourceKind");
        Bytes.requireLength(resourceIdentityHash, SystemMutation.HASH_LENGTH, "resourceIdentityHash");
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
        final byte[] raw = gcValue(resourceKind, resourceIdentityHash, expectedVersion);
        if (raw == null || gcValueType(raw) != ResourceDeleteConfirmedRecord.VALUE_TYPE) {
            return null;
        }
        return ResourceDeleteConfirmedRecord.decode(
                ValueEnvelope.decode(raw, ResourceDeleteConfirmedRecord.VALUE_TYPE).payload());
    }

    /**
     * Physically removes a completed local GC task only after the exact
     * catalog-backed Floor proof is present.  This is background compaction,
     * not a new Shard Log mutation; the source-ordered intent and confirmation
     * records have already supplied the durable audit boundary.
     */
    public synchronized ResourceGcGuard.Decision compactResourceDeleteConfirmation(
            final ResourceKind resourceKind, final byte[] resourceIdentityHash, final long expectedVersion,
            final RecoveryCatalogAuthority catalog, final byte[] candidateCheckpointId) {
        Objects.requireNonNull(resourceKind, "resourceKind");
        Bytes.requireLength(resourceIdentityHash, SystemMutation.HASH_LENGTH, "resourceIdentityHash");
        if (expectedVersion < 0) {
            throw new IllegalArgumentException("expectedVersion must be non-negative");
        }
        Objects.requireNonNull(catalog, "catalog");
        Objects.requireNonNull(candidateCheckpointId, "candidateCheckpointId");
        final ResourceRetireIntentRecord intent = getResourceRetireIntent(resourceKind, resourceIdentityHash,
                expectedVersion);
        final ResourceDeleteConfirmedRecord confirmation = getResourceDeleteConfirmation(resourceKind,
                resourceIdentityHash, expectedVersion);
        final ResourceGcGuard.Decision decision = ResourceGcGuard.evaluate(intent, confirmation, catalog,
                candidateCheckpointId);
        if (decision != ResourceGcGuard.Decision.SOURCE_AND_SEQUENCE_COVERED) {
            return decision;
        }
        final byte[] key = KeyCodec.gcRetireIntent(resourceKind, resourceIdentityHash, expectedVersion);
        final byte[] raw = store.get(ColumnFamily.GC, key);
        if (raw == null || gcValueType(raw) != ResourceDeleteConfirmedRecord.VALUE_TYPE) {
            return ResourceGcGuard.Decision.DELETE_NOT_CONFIRMED;
        }
        store.write(batch -> batch.delete(ColumnFamily.GC, key));
        return decision;
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
                case APPLY_SHARD_CONTROL -> applyShardControlMutation(mutation, sourcePosition);
                case PUBLISH_ADMISSION -> applyPublishAdmissionMutation(mutation, sourcePosition);
                case REPLAY_DEAD_LETTER -> applyReplayDeadLetterMutation(mutation, sourcePosition);
                case TIME_FENCE -> applyTimeFenceMutation(mutation, sourcePosition);
                case EXPIRE_GENERATION -> applyExpireGenerationMutation(mutation, sourcePosition);
                case PUBLISH_OUTCOME -> applyPublishOutcomeMutation(mutation, sourcePosition);
                case EVIDENCE_RESOLUTION -> applyEvidenceResolutionMutation(mutation, sourcePosition);
                case RESOLVE_UNCERTAIN -> applyResolveUncertainMutation(mutation, sourcePosition);
                case CLAIM_RESULT -> applyClaimResultMutation(mutation, sourcePosition);
                case DLQ_EXPORT_RESULT -> applyDlqExportResultMutation(mutation, sourcePosition);
                case RESOURCE_RETIRE_INTENT -> applyResourceRetireIntentMutation(mutation, sourcePosition);
                case RESOURCE_DELETE_CONFIRMED -> applyResourceDeleteConfirmedMutation(mutation, sourcePosition);
                default -> persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                        StableCode.STALE_SYSTEM_MUTATION);
            };
        } catch (V1CommandResolutionException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    exception.stableCode());
        } catch (IllegalArgumentException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    /**
     * Applies the bounded source-ordered Lane PAUSE/RESUME control subset.
     * Break/Close and the shard/profile/grant control branches remain fail-closed
     * until their immutable target registrations and terminal guards are present.
     */
    private SystemMutationResult applyShardControlMutation(final SystemMutation mutation,
                                                            final SourcePosition sourcePosition) {
        final ApplyShardControlBody body = ApplyShardControlBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(),
                body.controlRef().logicalOperationIdentity(body.controlKind()))) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (body.controlKind() == 12 || body.controlKind() == 13) {
            return applyPayloadProofTrustSetControlMutation(body, mutation, sourcePosition);
        }
        if (body.controlKind() < 8 || body.controlKind() > 11) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final ApplyShardControlBody.LaneTarget target = body.laneTarget();
        if (body.expectedPriorControlVersion() != null
                && body.expectedPriorControlVersion() != target.expectedControlVersion()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final LaneRecord current = readLane(target.laneId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        if (!Arrays.equals(current.laneIncarnation(), target.laneIncarnation())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.RESOURCE_INCARNATION_MISMATCH);
        }
        if (current.laneControlVersion() != target.expectedControlVersion()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.VERSION_CONFLICT);
        }

        if (body.controlKind() == 10
                && (!body.hasAcknowledgement(1) || !body.hasAcknowledgement(3))) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (body.controlKind() == 11) {
            final boolean orderedWork;
            try {
                orderedWork = laneHasOrderedWork(target.laneId());
            } catch (IllegalStateException exception) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.INTEGRITY_ERROR);
            }
            if (orderedWork && (!body.allowOrderBreak() || !body.hasAcknowledgement(1)
                    || !body.hasAcknowledgement(3))) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
        }

        final LaneRecord next;
        try {
            next = switch (body.controlKind()) {
                case 8 -> current.pauseByAdmin();
                case 9 -> current.resumeByAdmin();
                case 10 -> current.breakOrdering();
                case 11 -> current.closeForNewAdmission();
                default -> throw new IllegalStateException("unsupported lane control kind");
            };
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }

        final List<LaneClaimRollback> rollbacks;
        try {
            rollbacks = body.controlKind() == 9 ? List.of() : prepareLaneClaimRollbacks(target.laneId());
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        final TimelineCandidate candidate = body.controlKind() == 9
                ? findLaneCandidate(target.laneId(), null, -1, null, null) : null;
        final LaneProjection projection = projectLane(target.laneId(), current, next, candidate);
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            deleteReadyKey(batch, current);
            for (LaneClaimRollback rollback : rollbacks) {
                batch.delete(ColumnFamily.INFLIGHT, rollback.claim().encodedKey());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(rollback.claim().delayMessageId()),
                        rollback.nextMessage().encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, rollback.claim().timelineKey(),
                        new TimelineEntry(rollback.claim().delayMessageId(), rollback.nextMessage().generation())
                                .encode());
                batch.putValue(ColumnFamily.TIMELINE, 1,
                        expiryKey(rollback.claim().delayMessageId(), rollback.nextMessage()),
                        new TimelineEntry(rollback.claim().delayMessageId(), rollback.nextMessage().generation())
                                .encode());
            }
            putReadyProjection(batch, projection);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    /**
     * Applies the source-ordered trust-set marker subset and persists the
     * marker state in the same batch as the mutation result and source cursor.
     * The immutable semantic value is resolved before the batch; this local
     * path does not claim Oxia/catalog durability for that authority.
     */
    private SystemMutationResult applyPayloadProofTrustSetControlMutation(
            final ApplyShardControlBody body, final SystemMutation mutation,
            final SourcePosition sourcePosition) {
        final PayloadProofTrustSetRefV1 trustSet;
        final PayloadProofTrustSetControlState next;
        if (payloadProofTrustSetControlCatalog == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.ROUTE_SNAPSHOT_UNAVAILABLE);
        }
        if (body.controlKind() == 12) {
            final var payload = body.payloadProofTrustSetActivate();
            trustSet = payload.trustSet();
            requireTrustSetSemantic(trustSet);
            if (body.semanticVersion() != trustSet.version()
                    || !Bytes.constantTimeEquals(body.semanticHash(), trustSet.semanticHash())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = payloadProofTrustSetControlState.activate(trustSet, sourcePosition);
        } else {
            final var payload = body.payloadProofIssuanceClose();
            trustSet = payload.trustSet();
            final PayloadProofTrustSetSemanticV1 semantic = requireTrustSetSemantic(trustSet);
            if (semantic.keys().stream().noneMatch(key -> key.keyVersion() == payload.proofKeyVersion())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION);
            }
            if (body.semanticVersion() != trustSet.version()
                    || !Bytes.constantTimeEquals(body.semanticHash(), trustSet.semanticHash())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = payloadProofTrustSetControlState.close(payload, sourcePosition);
        }
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.META, PAYLOAD_PROOF_CONTROL_VALUE_TYPE,
                    KeyCodec.metaFixed(META_PAYLOAD_PROOF_CONTROL_STATE), next.canonicalBytes());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        payloadProofTrustSetControlState = next;
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    private PayloadProofTrustSetSemanticV1 requireTrustSetSemantic(final PayloadProofTrustSetRefV1 reference) {
        final PayloadProofTrustSetSemanticV1 semantic =
                payloadProofTrustSetControlCatalog.resolve(reference);
        if (semantic == null || !semantic.ref().equals(reference)) {
            throw new V1CommandResolutionException(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    "trust-set semantic value is unavailable or does not match its reference");
        }
        return semantic;
    }

    /** Builds the exact reversible timeline projection that Pause must restore in its own batch. */
    private List<LaneClaimRollback> prepareLaneClaimRollbacks(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{INFLIGHT_CLAIMED_KIND, 1}, new byte[]{INFLIGHT_PUBLISHING_KIND, 1}, limit);
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("Claim scan exceeded configured bound during lane Pause");
        }
        final List<LaneClaimRollback> result = new ArrayList<>();
        for (var entry : entries) {
            final ClaimRecord claim = decodeClaim(entry);
            if (!claim.laneId().equals(laneId)) {
                continue;
            }
            final MessageRecord current = getMessage(claim.delayMessageId());
            if (current == null || current.status() != MessageStatus.CLAIMED
                    || current.generation() != claim.generation()
                    || current.stateVersion() != claim.runtimeRevision()) {
                throw new IllegalStateException("Claim does not match current message during lane Pause");
            }
            final ClaimResultBody.ClaimPrecondition precondition =
                    ClaimResultBody.decodePrecondition(claim.preconditionBytes());
            final TimelineWorkKind workKind = switch (precondition.sourceWorkKind()) {
                case 1 -> TimelineWorkKind.INITIAL_SCHEDULE;
                case 2 -> TimelineWorkKind.DEFINITIVE_RETRY;
                default -> throw new IllegalStateException("Claim source work is not reversible");
            };
            MessageRecord next = new MessageRecord(MessageStatus.SCHEDULED, current.generation(),
                    Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                    current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                    current.payloadReference(), current.retryEligibilityAtEpochMs());
            next = next.withRuntimeIndex(timelineRuntimeIndex(claim.delayMessageId(), next, workKind,
                    Math.addExact(current.runtimeIndex().admissionsUsed(), 1), next.stateVersion(),
                    UncertainRetryAuthority.NONE, null, null, current.runtimeIndex()));
            if (!Arrays.equals(claim.timelineKey(), timelineKey(claim.delayMessageId(), next))) {
                throw new IllegalStateException("Claim timeline key is not reversible");
            }
            result.add(new LaneClaimRollback(claim, next));
        }
        return List.copyOf(result);
    }

    /** Bounded scan used only to decide whether a Close marker needs strict-order acknowledgements. */
    private boolean laneHasOrderedWork(final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.ID,
                new byte[]{1, 1}, new byte[]{2, 1}, limit);
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message scan exceeded configured bound during lane Close");
        }
        for (var entry : entries) {
            if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid MESSAGE key during lane Close");
            }
            final MessageRecord message = MessageRecord.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 1).payload());
            if (message.laneId().equals(laneId) && !isTerminalStatus(message.status())
                    && message.orderingMode() == io.nereusstream.delay.protocol.OrderingMode.DELIVERY_TIME_FIFO) {
                return true;
            }
        }
        return false;
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
        final OutcomeReserveUsage admissionCharge;
        try {
            admissionCharge = OutcomeReserveUsage.from(body.chargeVector());
        } catch (ArithmeticException overflow) {
            return persistAdmissionCapacityGated(body, mutation, sourcePosition, localClaim);
        }
        if (!outcomeReserve.fits(admissionCharge, config.maxOutcomeReserveRecords(),
                config.maxOutcomeReserveBytes())) {
            return persistAdmissionCapacityGated(body, mutation, sourcePosition, localClaim);
        }
        try {
            validateOutcomeReserveVector(outcomeReserveVector.add(body.chargeVector().toCapacityVector()));
        } catch (ArithmeticException | IllegalArgumentException | IllegalStateException exception) {
            return persistAdmissionCapacityGated(body, mutation, sourcePosition, localClaim);
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
                    replayState.uncertainRetryAdmission(), admissionCharge);
            return result;
        } catch (IllegalArgumentException | IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    /**
     * Advances a source-ordered Admission that cannot fit its shard outcome
     * reserve.  A live Claim is revoked in the same batch; no attempt or
     * Producer-side state is created.
     */
    private SystemMutationResult persistAdmissionCapacityGated(final PublishAdmissionBody body,
                                                               final SystemMutation mutation,
                                                               final SourcePosition sourcePosition,
                                                               final ClaimRecord claim) {
        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final MessageRecord current = getMessage(messageId);
        if (current == null || (current.status() != MessageStatus.SCHEDULED
                && current.status() != MessageStatus.CLAIMED)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        MessageRecord next = current;
        final boolean revokeClaim = current.status() == MessageStatus.CLAIMED;
        final byte[] priorTimelineKey = claim == null ? timelineKey(messageId, current) : claim.timelineKey();
        if (revokeClaim) {
            final ClaimResultBody.ClaimPrecondition precondition = ClaimResultBody.decodePrecondition(
                    body.claimPrecondition().canonicalBytes());
            final TimelineWorkKind workKind = TimelineWorkKind.fromWire(
                    precondition.sourceWorkKind());
            next = new MessageRecord(MessageStatus.SCHEDULED, current.generation(),
                    Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                    current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                    current.payloadReference(), current.retryEligibilityAtEpochMs());
            next = next.withRuntimeIndex(timelineRuntimeIndex(messageId, next, workKind,
                    Math.addExact(current.runtimeIndex().admissionsUsed(), 1), next.stateVersion(),
                    UncertainRetryAuthority.NONE, null, null, current.runtimeIndex()));
        }
        final MessageRecord nextForWrite = next;
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = revokeClaim
                ? readyProjections(sourcePosition, messageId, current, next, null) : Map.of();
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                StableCode.ADMISSION_CAPACITY_GATED, sourcePosition.canonicalBytes());
        store.write(batch -> {
            if (revokeClaim) {
                batch.delete(ColumnFamily.INFLIGHT, claim == null
                        ? KeyCodec.inflight(INFLIGHT_CLAIMED_KIND,
                        AuthorIdentity.decode(body.ownerIdentity()).generation(), body.claimId())
                        : claim.encodedKey());
                batch.delete(ColumnFamily.TIMELINE, priorTimelineKey);
                batch.delete(ColumnFamily.TIMELINE, expiryKey(messageId, current));
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(messageId), nextForWrite.encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey(messageId, nextForWrite),
                        new TimelineEntry(messageId, nextForWrite.generation()).encode());
                batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(messageId, nextForWrite),
                        new TimelineEntry(messageId, nextForWrite.generation()).encode());
                for (LaneProjection projection : projections.values()) {
                    deleteReadyKey(batch, projection.previousLane());
                    putReadyProjection(batch, projection);
                }
            }
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    private SystemMutationResult applyTimeFenceMutation(final SystemMutation mutation,
                                                         final SourcePosition sourcePosition) {
        final List<io.nereusstream.delay.protocol.CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.TIME_FENCE, mutation.canonicalBody());
        final long closeThrough = bodyNonNegative(field(fields, 10), 10);
        final int fenceKeyVersion = bodyInt(field(fields, 11), 11);
        final byte[] proofId = fixedBodyBytes(field(fields, 12), 12, SystemMutation.HASH_LENGTH);
        final TrustedUtcIntervalEvidence proof = TrustedUtcIntervalEvidence.decode(
                bytesBody(field(fields, 13), 13));
        if (fenceKeyVersion != mutation.signingKeyVersion() || proof.earliestEpochMs() < closeThrough) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final byte[] expectedProofId = Bytes.sha256(Bytes.utf8("nereus-delay-time-fence-proof-v1\0"),
                store.shardId().routeIncarnation().bytes(), Bytes.u32be(store.shardId().partition()),
                Bytes.i64be(closeThrough), Bytes.u32be(fenceKeyVersion), Bytes.lp32(proof.canonicalBytes()));
        if (!Bytes.constantTimeEquals(proofId, expectedProofId)
                || !Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), proofId)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        if (closeThrough <= closedIngressDeadlineThrough) {
            store.write(batch -> {
                writeSystemResult(batch, result);
                writePosition(batch, sourcePosition);
            });
        } else {
            store.write(batch -> {
                batch.putValue(ColumnFamily.META, 1, KeyCodec.metaFixed(META_CLOSED_INGRESS_DEADLINE),
                        Bytes.u64be(closeThrough));
                writeSystemResult(batch, result);
                writePosition(batch, sourcePosition);
            });
            closedIngressDeadlineThrough = closeThrough;
        }
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
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
        return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                StableCode.STALE_SYSTEM_MUTATION);
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
        if (body.resolutionKind() == 4) {
            return applyPossibleDeliveryTerminalization(body, mutation, sourcePosition);
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

    /** Terminalizes an unresolved generation while retaining its exact obligation ledger. */
    private SystemMutationResult applyPossibleDeliveryTerminalization(final ResolveUncertainBody body,
                                                                       final SystemMutation mutation,
                                                                       final SourcePosition sourcePosition) {
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    current.generation() > body.generation()
                            ? StableCode.GENERATION_SUPERSEDED : StableCode.STALE_SYSTEM_MUTATION);
        }
        if (!current.laneId().equals(body.laneId()) || current.status() != MessageStatus.UNCERTAIN
                || current.runtimeIndex().currentWorkKind() != CurrentSendWorkKind.NONE) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        final LaneRecord lane = readLane(body.laneId());
        if (lane == null || !Arrays.equals(lane.laneIncarnation(), body.laneIncarnation())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
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
                || !ledger.laneId().equals(body.laneId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final MessageRecord terminalMessage = new MessageRecord(MessageStatus.DEAD_LETTER, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs()).withRuntimeIndex(
                GenerationRuntimeIndex.none(GenerationAggregateState.DEAD_LETTER,
                        current.runtimeIndex().attemptObligations(), current.runtimeIndex().admissionsUsed(),
                        current.runtimeIndex().uncertainRetryAdmissionsUsed(), true,
                        Math.addExact(current.runtimeIndex().runtimeRevision(), 1)));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(body.messageId(), body.generation(),
                MessageStatus.DEAD_LETTER, StableCode.DESTINATION_OUTCOME_UNKNOWN, terminalMessage.stateVersion(),
                sourcePosition.canonicalBytes(), true, terminalMessage.runtimeIndex().attemptObligations());
        final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(body.messageId(), body.generation(),
                terminalMessage.stateVersion(), sourcePosition.canonicalBytes());
        final ShardQuota nextQuota;
        try {
            nextQuota = quota.removeSchedule(current.payloadLength());
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, body.messageId(), current, terminalMessage, null);
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                StableCode.DESTINATION_OUTCOME_UNKNOWN, sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.delete(ColumnFamily.TIMELINE, expiryKey(body.messageId(), current));
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(body.messageId()), terminalMessage.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(body.messageId(), body.generation()), terminal.encode());
            batch.putValue(ColumnFamily.TERMINAL, DlqExportRecord.VALUE_TYPE,
                    KeyCodec.terminalDlqExport(dlqExport.dlqExportId()), dlqExport.encode());
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
        final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(messageId, body.generation(),
                terminalMessage.stateVersion(), sourcePosition.canonicalBytes());
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
            batch.putValue(ColumnFamily.TERMINAL, DlqExportRecord.VALUE_TYPE,
                    KeyCodec.terminalDlqExport(dlqExport.dlqExportId()), dlqExport.encode());
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

    private static boolean isTerminalAggregateState(final GenerationAggregateState state) {
        return switch (state) {
            case PUBLISHED, HANDED_OFF, CANCELED, EXPIRED, DEAD_LETTER, SUPERSEDED -> true;
            default -> false;
        };
    }

    private static PayloadAvailability payloadAvailability(final MessageRecord message) {
        return message.payloadReference() == null
                ? PayloadAvailability.INLINE_RETAINED : PayloadAvailability.OBJECT_RETAINED;
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
            final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(ledger.delayMessageId(),
                    ledger.generation(), terminalMessage.stateVersion(), sourcePosition.canonicalBytes());
            final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
            final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
            final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
            final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                    sourcePosition, ledger.delayMessageId(), current, terminalMessage, null);
            final MessageRecord terminalMessageForWrite = terminalMessage;
            store.write(batch -> {
                batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()),
                        terminalMessageForWrite.encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), terminal.encode());
                batch.putValue(ColumnFamily.TERMINAL, DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(dlqExport.dlqExportId()), dlqExport.encode());
                for (LaneProjection projection : projections.values()) {
                    deleteReadyKey(batch, projection.previousLane());
                    putReadyProjection(batch, projection);
                }
                batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
                persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
                writeSystemResult(batch, systemResult);
                writePosition(batch, sourcePosition);
            });
            lastAppliedSourcePosition = sourcePosition;
            mutationSequence++;
            quota = nextQuota;
            outcomeReserve = nextOutcomeReserve;
            outcomeReserveVector = nextOutcomeReserveVector;
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
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
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
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            writeSystemResult(batch, systemResult);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
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

    /**
     * Applies the local, source-ordered DLQ export outbox transition. The
     * external target/evidence adapter is deliberately outside this method;
     * only a signed result mutation may move the durable export state.
     */
    private SystemMutationResult applyDlqExportResultMutation(final SystemMutation mutation,
                                                                final SourcePosition sourcePosition) {
        final DlqExportResultBody body = DlqExportResultBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), body.logicalOperationIdentity())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final DelayMessageId messageId = new DelayMessageId(body.messageId());
        final TerminalGenerationRecord terminal = getTerminalGeneration(messageId, body.generation());
        if (terminal == null || terminal.status() != MessageStatus.DEAD_LETTER
                || terminal.stateVersion() != body.terminalRevision()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final DlqExportRecord current = getDlqExportRecord(messageId, body.generation());
        if (current == null || current.state() == DlqExportStateV1.NOT_CONFIGURED
                || !Bytes.constantTimeEquals(current.exportEnvelopeHash(), body.exportEnvelopeHash())
                || !Bytes.constantTimeEquals(current.dlqExportId(), body.dlqExportId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        try {
            validateDlqExportAttempt(current, body);
            final int nextAttempt = body.resultingState() == DlqExportStateV1.PENDING
                    ? Math.addExact(body.physicalAttemptNo(), 1) : body.physicalAttemptNo();
            final DlqExportRecord next = new DlqExportRecord(current.dlqExportId(), messageId,
                    current.generation(), current.terminalRevision(), current.exportEnvelopeHash(),
                    body.resultingState(), nextAttempt, sourcePosition.canonicalBytes());
            final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                    StableCode.OK, sourcePosition.canonicalBytes());
            store.write(batch -> {
                batch.putValue(ColumnFamily.TERMINAL, DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(next.dlqExportId()), next.encode());
                writeSystemResult(batch, result);
                writePosition(batch, sourcePosition);
            });
            lastAppliedSourcePosition = sourcePosition;
            mutationSequence++;
            return result;
        } catch (IllegalStateException | IllegalArgumentException | ArithmeticException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
    }

    private static void validateDlqExportAttempt(final DlqExportRecord current,
                                                 final DlqExportResultBody body) {
        if (body.eventKind() == 1) {
            if (current.state() == DlqExportStateV1.PUBLISHED
                    || current.state() == DlqExportStateV1.FAILED_PERMANENT) {
                throw new IllegalStateException("terminal DLQ export cannot accept another attempt outcome");
            }
            final int expectedAttempt = current.state() == DlqExportStateV1.UNCERTAIN
                    ? Math.addExact(current.physicalAttemptNo(), 1) : current.physicalAttemptNo();
            if (body.physicalAttemptNo() != expectedAttempt) {
                throw new IllegalStateException("DLQ export attempt number is not the checked successor");
            }
        } else {
            if (current.state() != DlqExportStateV1.UNCERTAIN
                    && current.state() != DlqExportStateV1.PENDING) {
                throw new IllegalStateException("evidence resolution has no open DLQ export state");
            }
            if (body.physicalAttemptNo() > current.physicalAttemptNo()) {
                throw new IllegalStateException("DLQ evidence names an unknown physical attempt");
            }
        }
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

    /**
     * Persists the source-ordered retirement intent and its immutable protection
     * set. External deletion, Floor release and delete confirmation remain
     * separate mutations and are never inferred here.
     */
    private SystemMutationResult applyResourceRetireIntentMutation(final SystemMutation mutation,
                                                                    final SourcePosition sourcePosition) {
        final ResourceRetireIntentBody body = ResourceRetireIntentBody.decode(mutation.canonicalBody());
        final byte[] expectedLogicalIdentity = SystemMutation.computeResourceRetireLogicalIdentity(
                body.resourceKind(), body.resource().identityHash(), body.expectedResourceStateVersion());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), expectedLogicalIdentity)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final ResourceRetireIntentRecord prior = getResourceRetireIntent(body.resourceKind(),
                body.resource().identityHash(), body.expectedResourceStateVersion());
        if (prior != null) {
            if (Bytes.constantTimeEquals(prior.mutationId(), mutation.systemMutationId())
                    && Bytes.constantTimeEquals(prior.mutationHash(), mutation.mutationHash())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.OK);
            }
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.VERSION_CONFLICT);
        }
        final ResourceRetireIntentRecord record = new ResourceRetireIntentRecord(mutation.systemMutationId(),
                mutation.mutationHash(), body.resourceKind(), body.resource().canonicalBytes(),
                body.resource().identityHash(), body.expectedResourceStateVersion(),
                Math.addExact(mutationSequence, 1),
                body.protections().canonicalBytes(), sourcePosition.canonicalBytes());
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.GC, ResourceRetireIntentRecord.VALUE_TYPE,
                    KeyCodec.gcRetireIntent(record.resourceKind(), record.resourceIdentityHash(),
                            record.expectedResourceStateVersion()),
                    record.encode());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    /**
     * Records an exact delete response against an already applied retire intent.
     * Provider deletion, Recovery Floor advancement and quota release remain outside this local projection.
     */
    private SystemMutationResult applyResourceDeleteConfirmedMutation(final SystemMutation mutation,
                                                                        final SourcePosition sourcePosition) {
        final ResourceDeleteConfirmedBody body = ResourceDeleteConfirmedBody.decode(mutation.canonicalBody());
        if (!Bytes.constantTimeEquals(mutation.logicalOperationIdentity(), body.intent().mutationId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final RetireIntentLookup lookup = findRetireIntent(body.intent());
        if (lookup == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        ResourceRetireIntentBody.validateExternalDeleteIdentity(lookup.resourceKind(),
                lookup.intent().resourceIdentity(), body.evidence().observedImmutableVersion(),
                body.evidence().observedEtag());
        final ResourceDeleteConfirmedRecord prior = getResourceDeleteConfirmation(lookup.resourceKind(),
                body.intent().resourceIdentityHash(), body.intent().expectedResourceStateVersion());
        if (prior != null) {
            if (Bytes.constantTimeEquals(prior.confirmationMutationId(), mutation.systemMutationId())
                    && Bytes.constantTimeEquals(prior.confirmationMutationHash(), mutation.mutationHash())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.OK);
            }
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.VERSION_CONFLICT);
        }
        final ResourceDeleteConfirmedRecord record = new ResourceDeleteConfirmedRecord(
                mutation.systemMutationId(), mutation.mutationHash(), lookup.intent(), body.outcome(),
                Math.addExact(mutationSequence, 1), body.evidence().providerRequestIdHash(),
                body.evidence().observedImmutableVersion(),
                body.evidence().observedEtag(), body.evidence().responseHash(), body.evidence().observedAt().canonicalBytes(),
                body.confirmedAt().canonicalBytes(), sourcePosition.canonicalBytes());
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.GC, ResourceDeleteConfirmedRecord.VALUE_TYPE,
                    KeyCodec.gcRetireIntent(lookup.resourceKind(), record.retireIntent().resourceIdentityHash(),
                            record.retireIntent().expectedResourceStateVersion()), record.encode());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    private RetireIntentLookup findRetireIntent(final ResourceDeleteConfirmedBody.RetireIntentRef reference) {
        for (ResourceKind resourceKind : ResourceKind.values()) {
            final ResourceRetireIntentRecord intent = getResourceRetireIntent(resourceKind,
                    reference.resourceIdentityHash(), reference.expectedResourceStateVersion());
            if (intent != null
                    && Bytes.constantTimeEquals(intent.mutationId(), reference.mutationId())
                    && Bytes.constantTimeEquals(intent.mutationHash(), reference.mutationHash())) {
                return new RetireIntentLookup(resourceKind, intent);
            }
        }
        return null;
    }

    private byte[] gcValue(final ResourceKind resourceKind, final byte[] resourceIdentityHash,
                           final long expectedVersion) {
        return store.get(ColumnFamily.GC, KeyCodec.gcRetireIntent(resourceKind, resourceIdentityHash,
                expectedVersion));
    }

    private static int gcValueType(final byte[] raw) {
        if (raw.length < 4 || raw[0] != 0x4e || raw[1] != 0x56) {
            throw new IllegalStateException("invalid gc task value envelope");
        }
        return raw[2] & 0xff;
    }

    private record RetireIntentLookup(ResourceKind resourceKind, ResourceRetireIntentRecord intent) {
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
        return admitPublishAttempt(admission, sourcePosition, null, false, false, OutcomeReserveUsage.empty());
    }

    private PublishAttemptLedger admitPublishAttempt(final PublishAttemptLedger admission,
                                                     final SourcePosition sourcePosition,
                                                     final SystemMutationResult systemResult,
                                                     final boolean claimMayBeMissing,
                                                     final boolean uncertainRetryAdmission,
                                                     final OutcomeReserveUsage admissionCharge) {
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
        final OutcomeReserveUsage nextOutcomeReserve = outcomeReserve.add(admissionCharge);
        final CapacityVectorV1 nextOutcomeReserveVector = outcomeReserveVector.add(
                outcomeCapacityCharge(admission));
        if (!nextOutcomeReserve.fits(OutcomeReserveUsage.empty(), config.maxOutcomeReserveRecords(),
                config.maxOutcomeReserveBytes())) {
            throw new IllegalStateException("Publish Admission outcome reserve exceeds shard grant");
        }
        validateOutcomeReserveVector(nextOutcomeReserveVector);
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
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return admission;
    }

    /** Returns the outcome-reserve component of a canonical Admission ledger. */
    private OutcomeReserveUsage outcomeReserveCharge(final PublishAttemptLedger ledger) {
        try {
            return OutcomeReserveUsage.from(PublishAdmissionBody.decode(ledger.admissionBytes()).chargeVector());
        } catch (RuntimeException legacyOrMalformedDirectLedger) {
            // The public embedded admission helper predates ChargeVector and is
            // intentionally usable with synthetic test bytes. Such ledgers did
            // not consume this reserve and therefore release zero.
            return OutcomeReserveUsage.empty();
        }
    }

    private OutcomeReserveUsage releasedOutcomeReserve(final PublishAttemptLedger ledger) {
        return outcomeReserve.remove(outcomeReserveCharge(ledger));
    }

    private CapacityVectorV1 outcomeCapacityCharge(final PublishAttemptLedger ledger) {
        try {
            return PublishAdmissionBody.decode(ledger.admissionBytes()).chargeVector().toCapacityVector();
        } catch (RuntimeException legacyOrMalformedDirectLedger) {
            // Synthetic direct ledgers do not carry a canonical ChargeVector.
            return CapacityVectorV1.empty();
        }
    }

    private CapacityVectorV1 releasedOutcomeReserveVector(final PublishAttemptLedger ledger) {
        return outcomeReserveVector.subtract(outcomeCapacityCharge(ledger));
    }

    private void validateOutcomeReserveVector(final CapacityVectorV1 nextUsage) {
        if (capacityEnvelope != null && !capacityEnvelope.outcomeReserve().vector().covers(nextUsage)) {
            throw new IllegalStateException("Publish Admission outcome reserve exceeds immutable capacity grant");
        }
    }

    private void persistOutcomeReserve(final ShardStore.Batch batch, final OutcomeReserveUsage nextUsage,
                                       final CapacityVectorV1 nextVector)
            throws org.rocksdb.RocksDBException {
        if (!nextUsage.equals(outcomeReserve)) {
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_OUTCOME_RESERVE_USAGE),
                    nextUsage.encode());
        }
        if (capacityEnvelope != null && !nextVector.equals(outcomeReserveVector)) {
            final byte[] key = KeyCodec.metaControlReserve(2, capacityEnvelope.outcomeReserve().grantId());
            if (nextVector.isZero()) {
                batch.delete(ColumnFamily.META, key);
            } else {
                batch.putValue(ColumnFamily.META, CAPACITY_RESERVE_VALUE_TYPE, key, nextVector.canonicalBytes());
            }
        }
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
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
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
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
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
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), nextSummary.encode());
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
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
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final MessageRecord current = getMessage(ledger.delayMessageId());
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), nextSummary.encode());
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
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
        if (current.admissionGate() == AdmissionGate.RETIRED) {
            throw new IllegalStateException("terminal lane cannot change readiness");
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
        if (current.admissionGate() == AdmissionGate.RETIRED) {
            throw new IllegalStateException("terminal lane cannot change admission gate");
        }
        if (current.laneControlVersion() != expectedLaneControlVersion) {
            throw new IllegalStateException("lane control version conflict");
        }
        if (gate == AdmissionGate.RETIRED) {
            throw new IllegalArgumentException("physical retirement requires a terminal guard");
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

    /**
     * Atomically replaces one closed active Lane with its terminal guard at
     * the same {@code meta_cf/LANE} key.  The caller supplies the already
     * applied retirement progress and must invoke this only after the
     * Recovery-Floor and external-channel checks have passed.
     */
    public synchronized LaneTerminalGuardV1 retireLaneWithTerminalGuard(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId,
            final long expectedLaneControlVersion, final LaneRetirementProgressV1 progress,
            final LaneTerminalGuardV1 guard) {
        Objects.requireNonNull(laneId, "laneId");
        Objects.requireNonNull(progress, "progress");
        Objects.requireNonNull(guard, "guard");
        final LaneValue currentValue = readLaneValue(laneId);
        if (currentValue == null || !currentValue.isActive()) {
            throw new IllegalStateException("lane is already terminal or missing");
        }
        final LaneRecord current = LaneRecord.decode(currentValue.activeStateBytes());
        if (current.admissionGate() != AdmissionGate.CLOSED) {
            throw new IllegalStateException("only a CLOSED lane can be physically retired");
        }
        if (current.laneControlVersion() != expectedLaneControlVersion) {
            throw new IllegalStateException("lane control version conflict");
        }
        if (!guard.laneId().equals(laneId)
                || !Arrays.equals(guard.laneIncarnation(), current.laneIncarnation())
                || guard.laneControlVersion() != current.laneControlVersion()
                || !Arrays.equals(progress.retireMutationId(), guard.retirementIntentId())
                || progress.appliedShardMutationSequence() != guard.retirementMutationSequence()) {
            throw new IllegalStateException("terminal guard does not match the closed lane or retirement progress");
        }
        if (lastAppliedSourcePosition == null
                || progress.intentSourcePosition().compareTo(lastAppliedSourcePosition) > 0
                || guard.terminalSourcePosition().compareTo(progress.intentSourcePosition()) > 0) {
            throw new IllegalStateException("retirement progress is not source-ordered and applied");
        }
        if (findLaneCandidate(laneId, null, -1, null, null) != null || hasLaneRuntimeWork(laneId)) {
            throw new IllegalStateException("lane still has pending or inflight work");
        }
        store.write(batch -> {
            deleteReadyKey(batch, current);
            batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(laneId),
                    LaneRecordEnvelopeV1.terminal(guard).canonicalBytes());
        });
        return guard;
    }

    /** Returns the terminal guard at the Lane key, or {@code null} while active. */
    public synchronized LaneTerminalGuardV1 getLaneTerminalGuard(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final LaneValue value = readLaneValue(laneId);
        return value == null || value.isActive() ? null : value.terminalGuard();
    }

    public synchronized SourcePosition lastAppliedSourcePosition() {
        return lastAppliedSourcePosition;
    }

    /** Returns the source-ordered ingress retry deadline, or {@code -1} before the first fence. */
    public synchronized long closedIngressDeadlineThrough() {
        return closedIngressDeadlineThrough;
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

    /** Returns the persisted non-borrowable outcome reserve usage projection. */
    public synchronized OutcomeReserveUsage outcomeReserve() {
        return outcomeReserve;
    }

    /** Returns the exact 66-dimensional outcome grant usage when an envelope is bound. */
    public synchronized CapacityVectorV1 outcomeReserveVector() {
        return outcomeReserveVector;
    }

    /** Returns the immutable placement envelope bound to this shard, if one was supplied. */
    public ShardCapacityEnvelopeV1 capacityEnvelope() {
        return capacityEnvelope;
    }

    /** Returns the persisted usage of a non-outcome control reserve class. */
    public synchronized CapacityVectorV1 controlReserveUsage(final int reserveClass) {
        validateMutableControlReserveClass(reserveClass);
        return controlReserveUsage.getOrDefault(reserveClass, CapacityVectorV1.empty());
    }

    /**
     * Charges one checked class-3/4/5 control reserve projection and persists
     * it synchronously.  This is a local accounting primitive; the source
     * ordered control mutation and Oxia placement authority remain callers'
     * responsibilities.
     */
    public synchronized CapacityVectorV1 reserveControlCapacity(final int reserveClass,
                                                                  final CapacityVectorV1 amount) {
        return mutateControlReserve(reserveClass, Objects.requireNonNull(amount, "amount"), true);
    }

    /** Releases an exact checked class-3/4/5 control reserve projection. */
    public synchronized CapacityVectorV1 releaseControlCapacity(final int reserveClass,
                                                                  final CapacityVectorV1 amount) {
        return mutateControlReserve(reserveClass, Objects.requireNonNull(amount, "amount"), false);
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
            final LaneValue laneValue = decodeLaneValue(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 2).payload());
            if (!laneValue.isActive()) {
                continue;
            }
            final LaneRecord lane = LaneRecord.decode(laneValue.activeStateBytes());
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

    /** Returns only reservations whose expiry is already decided by TIME_FENCE. */
    public synchronized List<ReservationExpiryWork> discoverReservationExpiry(final long earliestEpochMs,
                                                                                final int limit) {
        if (earliestEpochMs < 0 || limit <= 0) {
            throw new IllegalArgumentException("invalid reservation expiry discovery bounds");
        }
        final List<ReservationExpiryWork> result = new ArrayList<>();
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.TIMELINE,
                new byte[]{5, 1}, new byte[]{6, 1}, limit);
        for (var entry : entries) {
            final byte[] key = entry.key();
            if (key.length != 2 + 8 + 32 || key[0] != 5 || key[1] != 1) {
                throw new IllegalStateException("invalid RESERVATION_EXPIRY key");
            }
            final ByteBuffer input = ByteBuffer.wrap(key);
            input.position(2);
            final long expiry = input.getLong();
            final byte[] reservationId = new byte[32];
            input.get(reservationId);
            if (expiry > earliestEpochMs) {
                break;
            }
            final PayloadReservation reservation = PayloadReservation.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 5).payload());
            if (!Arrays.equals(reservation.reservationId(), reservationId)
                    || reservation.reservationExpiryEpochMs() != expiry) {
                throw new IllegalStateException("RESERVATION_EXPIRY key/value identity mismatch");
            }
            if (effectiveReservation(reservation).status() == PayloadReservationStatus.EXPIRED) {
                result.add(new ReservationExpiryWork(reservation.reservationId(), reservation.delayMessageId(),
                        reservation.reservationExpiryEpochMs(), reservation.stateVersion()));
            }
            if (result.size() >= limit) {
                break;
            }
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
        } catch (V1CommandResolutionException exception) {
            return persistRejected(command, sourcePosition, exception.stableCode());
        } catch (ArithmeticException | IllegalArgumentException exception) {
            return persistRejected(command, sourcePosition, StableCode.INVALID_COMMAND);
        }
    }

    private LargeScheduleIntent decodePrepareLargeIntent(final PreparedCommand command,
                                                          final SourcePosition sourcePosition) {
        if (!CommandBodies.isRegistryClientBodyV1(command.canonicalBody())) {
            return CommandBodies.decodePrepareLarge(command.canonicalBody());
        }
        final PrepareLargeScheduleBodyV1 body = CommandBodies.decodePrepareLargeV1(command.canonicalBody());
        requireV1BodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
        final V1ScheduleResolver resolver = requireV1ScheduleResolver();
        final V1ScheduleResolver.ResolvedPrepare resolved = Objects.requireNonNull(
                resolver.resolvePrepare(command.shardId(), command.delayMessageId(), body, sourcePosition),
                "resolved PrepareLargeSchedule projection");
        if (payloadProofTrustSetControlCatalog != null
                && !payloadProofTrustSetControlState.activatedAt(body.trustSet(), sourcePosition)) {
            throw new V1CommandResolutionException(
                    StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION,
                    "PrepareLargeSchedule trust set is not active at its source position");
        }
        lastResolvedPrepare = resolved;
        final ScheduleIntentV1 intent = body.intentWithoutPayload();
        return new LargeScheduleIntent(resolved.laneId(), intent.deliverAtEpochMs(), intent.expireAtEpochMs(),
                intent.orderingMode(), body.expectedPayloadLength(), body.payloadSha256(), body.reservationTtlMs(),
                body.trustSet().version());
    }

    private ScheduleApplication decodeScheduleApplication(final PreparedCommand command,
                                                           final SourcePosition sourcePosition) {
        if (!CommandBodies.isRegistryClientBodyV1(command.canonicalBody())) {
            final var legacy = CommandBodies.decodeSchedule(command.canonicalBody());
            return new ScheduleApplication(legacy.deliverAtEpochMs(), legacy.expireAtEpochMs(), legacy.laneId(),
                    legacy.orderingMode(), legacy.payload(), null);
        }
        final ScheduleCommandBodyV1 body = CommandBodies.decodeScheduleV1(command.canonicalBody());
        requireV1BodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
        final V1ScheduleResolver resolver = requireV1ScheduleResolver();
        final V1ScheduleResolver.ResolvedSchedule resolved = Objects.requireNonNull(
                resolver.resolveSchedule(command.shardId(), command.delayMessageId(), body.intent(), sourcePosition),
                "resolved Schedule projection");
        lastResolvedSchedule = resolved;
        validateResolvedSchedulePayload(body.intent(), resolved);
        return new ScheduleApplication(body.intent().deliverAtEpochMs(), body.intent().expireAtEpochMs(),
                resolved.laneId(), body.intent().orderingMode(),
                resolved.inlinePayload() == null ? new byte[0] : resolved.inlinePayload(),
                resolved.payloadReference());
    }

    private V1ScheduleResolver requireV1ScheduleResolver() {
        if (v1ScheduleResolver == null) {
            throw new V1CommandResolutionException(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    "V1 Schedule/Prepare requires a source-position-pinned resolver");
        }
        return v1ScheduleResolver;
    }

    private static void validateResolvedSchedulePayload(final ScheduleIntentV1 intent,
                                                         final V1ScheduleResolver.ResolvedSchedule resolved) {
        if (intent.hasInlinePayload()) {
            if (resolved.payloadReference() != null
                    || !Arrays.equals(intent.inlinePayload(), resolved.inlinePayload())) {
                throw new V1CommandResolutionException(StableCode.INVALID_COMMAND,
                        "resolved inline payload does not match ScheduleIntentV1");
            }
            return;
        }
        final var descriptor = intent.committedPayload();
        final PayloadReference reference = resolved.payloadReference();
        if (reference == null || resolved.inlinePayload() != null
                || !Bytes.constantTimeEquals(reference.objectStoreProfileHash(),
                descriptor.objectStoreProfile().semanticHash())
                || !Arrays.equals(reference.container(), descriptor.container())
                || !Arrays.equals(reference.objectKey(), descriptor.objectKey())
                || !Arrays.equals(reference.immutableObjectVersion(), descriptor.immutableObjectVersion())
                || !optionalBytesEqual(reference.etag(), descriptor.etag())
                || reference.length() != descriptor.length()
                || !Bytes.constantTimeEquals(reference.payloadSha256(), descriptor.payloadSha256())) {
            throw new V1CommandResolutionException(StableCode.INVALID_COMMAND,
                    "resolved object payload does not match ScheduleIntentV1");
        }
    }

    private CommandResult applyPrepareLarge(final PreparedCommand command, final SourcePosition sourcePosition) {
        final LargeScheduleIntent intent = decodePrepareLargeIntent(command, sourcePosition);
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
        final PayloadCommitProofView proof;
        final boolean registryBody = CommandBodies.isRegistryClientBodyV1(command.canonicalBody());
        if (CommandBodies.isRegistryClientBodyV1(command.canonicalBody())) {
            final CommitLargeScheduleBodyV1 body = CommandBodies.decodeCommitLargeV1(command.canonicalBody());
            requireV1BodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
            proof = body.proof();
        } else {
            proof = CommandBodies.decodeCommitLarge(command.canonicalBody());
        }
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
        final PayloadProofTrustSetRefV1 pinnedTrustSet = registryBody && payloadProofTrustSetControlCatalog != null
                ? pinnedPrepareTrustSet(command.delayMessageId()) : null;
        final boolean proofAuthorized;
        if (pinnedTrustSet != null && payloadProofTrustSetControlCatalog != null) {
            if (proof.trustSetVersion() != pinnedTrustSet.version()
                    || !payloadProofTrustSetControlState.firstSeenIssuanceOpen(pinnedTrustSet,
                    proof.proofKeyVersion(), sourcePosition)) {
                return persistRejected(command, sourcePosition,
                        StableCode.PAYLOAD_PROOF_KEY_NOT_AUTHORIZED_AT_SOURCE_POSITION);
            }
            proofAuthorized = PayloadProofTrustSet.fromSemantic(requireTrustSetSemantic(pinnedTrustSet))
                    .verifies(proof, sourcePosition.brokerPersistenceTimeEpochMs());
        } else {
            proofAuthorized = payloadProofTrustSet != null && payloadProofTrustSet.verifies(proof,
                    sourcePosition.brokerPersistenceTimeEpochMs());
        }
        if (!proofAuthorized) {
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

    private PayloadProofTrustSetRefV1 pinnedPrepareTrustSet(final DelayMessageId messageId) {
        final V1ScheduleBinding binding = getV1ScheduleBinding(messageId);
        if (binding == null || binding.commandType() != io.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE) {
            throw new V1CommandResolutionException(StableCode.ROUTE_SNAPSHOT_UNAVAILABLE,
                    "V1 Commit has no durable Prepare trust-set binding");
        }
        return CommandBodies.decodePrepareLargeV1(binding.canonicalBody()).trustSet();
    }

    private static byte[] reservationId(final PreparedCommand command) {
        return Bytes.sha256(Bytes.utf8("nereus-delay-reservation-id-v1\0"), command.commandId().bytes(),
                command.delayMessageId().bytes(), command.commandHash());
    }

    private static boolean proofMatches(final PayloadCommitProofView proof, final PayloadReference reference) {
        return Bytes.constantTimeEquals(proof.objectStoreProfileHash(), reference.objectStoreProfileHash())
                && java.util.Arrays.equals(proof.container(), reference.container())
                && java.util.Arrays.equals(proof.objectKey(), reference.objectKey())
                && java.util.Arrays.equals(proof.immutableObjectVersion(), reference.immutableObjectVersion())
                && optionalBytesEqual(proof.etag(), reference.etag()) && proof.length() == reference.length()
                && Bytes.constantTimeEquals(proof.payloadSha256(), reference.payloadSha256());
    }

    private static boolean optionalBytesEqual(final byte[] left, final byte[] right) {
        final byte[] normalizedLeft = left == null || left.length == 0 ? null : left;
        final byte[] normalizedRight = right == null || right.length == 0 ? null : right;
        return Arrays.equals(normalizedLeft, normalizedRight);
    }

    private CommandResult applySchedule(final PreparedCommand command, final SourcePosition sourcePosition) {
        final ScheduleApplication intent = decodeScheduleApplication(command, sourcePosition);
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
                intent.payload(), sourcePosition.canonicalBytes(), intent.payloadReference());
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
        final CancelRequest request = decodeCancelRequest(command);
        final MessageRecord existing = getMessage(command.delayMessageId());
        if (existing == null) {
            final PayloadReservation reservation = findReservationForMessage(command.delayMessageId());
            if (reservation != null) {
                if (!matchesPrecondition(request.expectedGeneration(), request.expectedStateVersion(), 0,
                        reservation.stateVersion())) {
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
        if (!matchesPrecondition(request.expectedGeneration(), request.expectedStateVersion(), existing.generation(),
                existing.stateVersion())) {
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
        final RescheduleRequest request = decodeRescheduleRequest(command);
        final MessageRecord existing = getMessage(command.delayMessageId());
        if (existing == null) {
            return applied(StableCode.NOT_FOUND, sourcePosition, null);
        }
        if (!matchesPrecondition(request.expectedGeneration(), request.expectedStateVersion(), existing.generation(),
                existing.stateVersion())) {
            return applied(StableCode.VERSION_CONFLICT, sourcePosition, existing);
        }
        if ((existing.status() == MessageStatus.SCHEDULED || existing.status() == MessageStatus.CLAIMED)
                && hasUncertainObligation(existing.runtimeIndex())) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        if (existing.status() != MessageStatus.SCHEDULED && existing.status() != MessageStatus.CLAIMED) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        validateWindow(request.deliverAtEpochMs(), request.expireAtEpochMs(),
                sourcePosition.brokerPersistenceTimeEpochMs());
        final MessageRecord replacement = new MessageRecord(MessageStatus.SCHEDULED, existing.generation() + 1,
                existing.stateVersion() + 1, request.deliverAtEpochMs(), request.expireAtEpochMs(), existing.laneId(),
                existing.orderingMode(), existing.payload(), sourcePosition.canonicalBytes(),
                existing.payloadReference());
        return applied(StableCode.SUPERSEDED, sourcePosition, replacement);
    }

    private CancelRequest decodeCancelRequest(final PreparedCommand command) {
        if (CommandBodies.isRegistryClientBodyV1(command.canonicalBody())) {
            final CancelCommandBodyV1 body = CommandBodies.decodeCancelV1(command.canonicalBody());
            requireV1BodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
            return new CancelRequest(body.precondition().expectedGeneration(),
                    body.precondition().expectedStateVersion());
        }
        final int expectedGeneration = CommandBodies.decodeCancel(command.canonicalBody());
        return new CancelRequest(expectedGeneration < 0 ? null : (long) expectedGeneration, null);
    }

    private RescheduleRequest decodeRescheduleRequest(final PreparedCommand command) {
        if (CommandBodies.isRegistryClientBodyV1(command.canonicalBody())) {
            final RescheduleCommandBodyV1 body = CommandBodies.decodeRescheduleV1(command.canonicalBody());
            requireV1BodyIdentity(command, body.delayMessageId(), body.retryUntilEpochMs());
            return new RescheduleRequest(body.precondition().expectedGeneration(),
                    body.precondition().expectedStateVersion(), body.newDeliverAtEpochMs(),
                    body.newExpireAtEpochMs());
        }
        final CommandBodies.RescheduleValues values = CommandBodies.decodeReschedule(command.canonicalBody());
        return new RescheduleRequest(values.expectedGeneration() < 0 ? null : (long) values.expectedGeneration(), null,
                values.deliverAtEpochMs(), values.expireAtEpochMs());
    }

    private static void requireV1BodyIdentity(final PreparedCommand command, final DelayMessageId bodyMessageId,
                                              final long bodyRetryUntilEpochMs) {
        if (!command.delayMessageId().equals(bodyMessageId)
                || command.retryUntilEpochMs() != bodyRetryUntilEpochMs) {
            throw new IllegalArgumentException("Client body common fields do not match outer command");
        }
    }

    private static boolean matchesPrecondition(final Long expectedGeneration, final Long expectedStateVersion,
                                               final long generation, final long stateVersion) {
        return (expectedGeneration == null || expectedGeneration == generation)
                && (expectedStateVersion == null || expectedStateVersion == stateVersion);
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

    /** Persists only the position-level fence rejection; never overwrites command identity/result dedupe. */
    private CommandResult persistRejectedPositionOnly(final PreparedCommand command,
                                                      final SourcePosition position, final StableCode code) {
        final CommandResult result = rejected(code, position, -1, 0, null);
        store.write(batch -> {
            batch.putValue(ColumnFamily.DEDUPE, 3, KeyCodec.dedupePosition(position.canonicalBytes()),
                    command.commandId().bytes());
            writePosition(batch, position);
        });
        lastAppliedSourcePosition = position;
        mutationSequence++;
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
        final V1ScheduleBinding v1Binding = v1ScheduleBinding(command, result, persistedNext, reservation);
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
            if (v1Binding != null) {
                batch.putValue(ColumnFamily.ID, 4, KeyCodec.idV1ScheduleBinding(command.delayMessageId()),
                        v1Binding.encode());
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
        lastResolvedSchedule = null;
        lastResolvedPrepare = null;
    }

    private V1ScheduleBinding v1ScheduleBinding(final PreparedCommand command, final CommandResult result,
                                                final MessageRecord next, final PayloadReservation reservation) {
        if (result.applyStatus() != ApplyStatus.APPLIED || !CommandBodies.isRegistryClientBodyV1(
                command.canonicalBody())) {
            return null;
        }
        if (command.type() == io.nereusstream.delay.protocol.CommandType.SCHEDULE
                && result.stableCode() == StableCode.SCHEDULED && next != null) {
            final V1ScheduleResolver.ResolvedSchedule resolved = Objects.requireNonNull(lastResolvedSchedule,
                    "resolved V1 Schedule projection");
            if (!resolved.laneId().equals(next.laneId())) {
                throw new IllegalStateException("resolved V1 Schedule Lane changed during apply");
            }
            return V1ScheduleBinding.fromCommand(command, next.laneId(), resolved.canonicalLaneTuple());
        }
        if (command.type() == io.nereusstream.delay.protocol.CommandType.PREPARE_LARGE_SCHEDULE
                && result.stableCode() == StableCode.OK && reservation != null) {
            final V1ScheduleResolver.ResolvedPrepare resolved = Objects.requireNonNull(lastResolvedPrepare,
                    "resolved V1 Prepare projection");
            if (!resolved.laneId().equals(reservation.intent().laneId())) {
                throw new IllegalStateException("resolved V1 Prepare Lane changed during apply");
            }
            return V1ScheduleBinding.fromCommand(command, reservation.intent().laneId(),
                    resolved.canonicalLaneTuple());
        }
        return null;
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
        batch.putValue(ColumnFamily.META, 2, KeyCodec.metaLane(projection.lane().laneId()),
                LaneRecordEnvelopeV1.active(projection.lane().encode()).canonicalBytes());
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
                final var intent = decodeScheduleApplication(command, position);
                yield new MessageRecord(MessageStatus.SCHEDULED, 0, 1, intent.deliverAtEpochMs(),
                        intent.expireAtEpochMs(), intent.laneId(), intent.orderingMode(), intent.payload(),
                        position.canonicalBytes(), intent.payloadReference());
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
        final RescheduleRequest values = decodeRescheduleRequest(command);
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
            final PayloadReservation reservation = effectiveReservation(PayloadReservation.decode(
                    io.nereusstream.delay.store.ValueEnvelope.decode(entry.value(), 2).payload()));
            if (reservation.delayMessageId().equals(messageId)) {
                return reservation;
            }
        }
        return null;
    }

    private PayloadReservation effectiveReservation(final PayloadReservation reservation) {
        if (reservation.status() != PayloadReservationStatus.RESERVED
                || closedIngressDeadlineThrough < reservation.reservationExpiryEpochMs()) {
            return reservation;
        }
        return new PayloadReservation(reservation.shardId(), reservation.reservationId(), reservation.commandId(),
                reservation.delayMessageId(), reservation.commandHash(), reservation.intent(),
                reservation.reservationExpiryEpochMs(), PayloadReservationStatus.EXPIRED,
                reservation.stateVersion(), reservation.sourcePosition(), null);
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
        final LaneValue value = readLaneValue(laneId);
        if (value == null) {
            return null;
        }
        if (value.isActive()) {
            return LaneRecord.decode(value.activeStateBytes());
        }
        final LaneTerminalGuardV1 guard = value.terminalGuard();
        return new LaneRecord(guard.laneId(), guard.laneIncarnation(), guard.laneControlVersion(), 0,
                AdmissionGate.RETIRED, RuntimeReadiness.BLOCKED, 1, 0);
    }

    private LaneValue readLaneValue(final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final var value = store.getValue(ColumnFamily.META, KeyCodec.metaLane(laneId), 2);
        return value == null ? null : decodeLaneValue(value.payload());
    }

    /**
     * Conservative local retirement proof.  A lane is retired only when this
     * bounded scan can prove that no current message or reversible attempt
     * still names it.  If the configured bound is exceeded we fail closed and
     * require the recovery/GC coordinator to retry after compaction.
     */
    private boolean hasLaneRuntimeWork(final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> messages = store.scan(ColumnFamily.ID,
                new byte[]{1, 1}, new byte[]{2, 1}, limit);
        if (messages.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message scan exceeded configured bound during lane retirement");
        }
        for (var entry : messages) {
            if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid MESSAGE key during lane retirement");
            }
            final MessageRecord message = MessageRecord.decode(
                    ValueEnvelope.decode(entry.value(), 1).payload());
            if (message.laneId().equals(laneId)) {
                return true;
            }
        }
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> attempts = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{1, 1}, new byte[]{4, 1}, limit);
        if (attempts.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("inflight scan exceeded configured bound during lane retirement");
        }
        for (var entry : attempts) {
            if (entry.key().length < 2 || entry.key()[1] != 1) {
                throw new IllegalStateException("invalid inflight key during lane retirement");
            }
            final io.nereusstream.delay.protocol.DestinationLaneId candidateLane;
            if (entry.key()[0] == INFLIGHT_CLAIMED_KIND) {
                candidateLane = ClaimRecord.decode(ValueEnvelope.decode(entry.value(), ClaimRecord.VALUE_TYPE)
                        .payload()).laneId();
            } else if (entry.key()[0] == INFLIGHT_PUBLISHING_KIND
                    || entry.key()[0] == INFLIGHT_UNCERTAIN_KIND) {
                candidateLane = PublishAttemptLedger.decode(
                        ValueEnvelope.decode(entry.value(), PublishAttemptLedger.VALUE_TYPE).payload()).laneId();
            } else {
                throw new IllegalStateException("unknown inflight kind during lane retirement");
            }
            if (candidateLane.equals(laneId)) {
                return true;
            }
        }
        return false;
    }

    private static LaneValue decodeLaneValue(final byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        // Pre-envelope databases used the fixed LaneRecord adapter bytes.  A
        // zero first byte is the big-endian version marker, while every new
        // protobuf envelope starts with field 1's tag (0x08).  Preserve read
        // compatibility without treating malformed new values as legacy data.
        if (payload.length >= 4 && payload[0] == 0) {
            LaneRecord.decode(payload);
            return LaneValue.active(payload);
        }
        final LaneRecordEnvelopeV1 envelope = LaneRecordEnvelopeV1.decode(payload);
        return envelope.isActive() ? LaneValue.active(envelope.activeStateBytes())
                : LaneValue.terminal(envelope.terminalGuard());
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

    private static long readNonNegativeSequence(final byte[] bytes) {
        final long value = readSequence(bytes);
        if (value < 0) {
            throw new IllegalStateException("negative persisted ingress deadline");
        }
        return value;
    }

    private static final class WindowViolationException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;
    }

    private record LaneValue(boolean isActive, byte[] activeStateBytes, LaneTerminalGuardV1 terminalGuard) {
        private LaneValue {
            if (isActive == (terminalGuard != null)) {
                throw new IllegalArgumentException("invalid lane value branch");
            }
            if (isActive) {
                Objects.requireNonNull(activeStateBytes, "activeStateBytes");
                activeStateBytes = Bytes.copy(activeStateBytes);
            } else {
                if (activeStateBytes != null) {
                    throw new IllegalArgumentException("terminal lane cannot carry active bytes");
                }
                Objects.requireNonNull(terminalGuard, "terminalGuard");
            }
        }

        private static LaneValue active(final byte[] stateBytes) {
            return new LaneValue(true, stateBytes, null);
        }

        private static LaneValue terminal(final LaneTerminalGuardV1 guard) {
            return new LaneValue(false, null, guard);
        }

        @Override
        public byte[] activeStateBytes() {
            return activeStateBytes == null ? null : Bytes.copy(activeStateBytes);
        }
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

    private record LaneClaimRollback(ClaimRecord claim, MessageRecord nextMessage) {
    }

    private record AdmissionReplayState(boolean claimMayBeMissing, boolean uncertainRetryAdmission) {
    }

    private record GenerationIdentity(DelayMessageId messageId, int generation) {
    }

    private record CancelRequest(Long expectedGeneration, Long expectedStateVersion) {
    }

    private record RescheduleRequest(Long expectedGeneration, Long expectedStateVersion,
                                     long deliverAtEpochMs, long expireAtEpochMs) {
    }

    private record ScheduleApplication(long deliverAtEpochMs, long expireAtEpochMs,
                                       io.nereusstream.delay.protocol.DestinationLaneId laneId,
                                       io.nereusstream.delay.protocol.OrderingMode orderingMode,
                                       byte[] payload, PayloadReference payloadReference) {
        private ScheduleApplication {
            Objects.requireNonNull(laneId, "laneId");
            Objects.requireNonNull(orderingMode, "orderingMode");
            Objects.requireNonNull(payload, "payload");
            if (deliverAtEpochMs < 0 || expireAtEpochMs < deliverAtEpochMs
                    || payloadReference != null && payload.length != 0) {
                throw new IllegalArgumentException("invalid resolved Schedule projection");
            }
            payload = Bytes.copy(payload);
        }

        @Override
        public byte[] payload() {
            return Bytes.copy(payload);
        }
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

    public record ReservationExpiryWork(byte[] reservationId, DelayMessageId messageId,
                                       long reservationExpiryEpochMs, long stateVersion) {
        public ReservationExpiryWork {
            Bytes.requireLength(reservationId, 32, "reservationId");
            Objects.requireNonNull(messageId, "messageId");
            if (reservationExpiryEpochMs < 0 || stateVersion <= 0) {
                throw new IllegalArgumentException("invalid reservation expiry work");
            }
            reservationId = Bytes.copy(reservationId);
        }

        @Override
        public byte[] reservationId() {
            return Bytes.copy(reservationId);
        }
    }
}
