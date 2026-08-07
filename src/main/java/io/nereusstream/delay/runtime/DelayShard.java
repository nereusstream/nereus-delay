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
import io.nereusstream.delay.protocol.ControlRef;
import io.nereusstream.delay.protocol.ControlTargetRefV1;
import io.nereusstream.delay.protocol.DestinationLaneId;
import io.nereusstream.delay.protocol.DestinationProfileSemanticV1;
import io.nereusstream.delay.protocol.DelayMessageId;
import io.nereusstream.delay.protocol.DeliveryCapabilitySemanticV1;
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
import io.nereusstream.delay.protocol.ProfileAcceptanceV1;
import io.nereusstream.delay.protocol.ProfileBindingActivatePayloadV1;
import io.nereusstream.delay.protocol.ProfileBindingControlState;
import io.nereusstream.delay.protocol.ProfileNewBindingClosePayloadV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.PrepareLargeScheduleBodyV1;
import io.nereusstream.delay.protocol.PublishAdmissionBody;
import io.nereusstream.delay.protocol.PublishOutcomeBody;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.PreparedControlOperationV1;
import io.nereusstream.delay.protocol.ReplayDeadLetterBody;
import io.nereusstream.delay.protocol.ResolveUncertainBody;
import io.nereusstream.delay.protocol.RetryPolicyRefV1;
import io.nereusstream.delay.protocol.RetryPolicySemanticV1;
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
import io.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import io.nereusstream.delay.store.ColumnFamily;
import io.nereusstream.delay.store.IngressFenceState;
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
    private static final int META_PROFILE_CONTROL_STATE = 13;
    private static final int PAYLOAD_PROOF_CONTROL_VALUE_TYPE = 9;
    private static final int PROFILE_CONTROL_VALUE_TYPE = 10;
    private static final int META_QUOTA_USAGE = 1;
    private static final int META_OUTCOME_RESERVE_USAGE = 2;
    private static final int CAPACITY_RESERVE_VALUE_TYPE = 8;
    private static final int CONTROL_RESERVE_NON_OUTCOME_CLASS = 3;
    private static final int CONTROL_RESERVE_RECOVERY_CLASS = 4;
    private static final int CONTROL_RESERVE_EMERGENCY_CLASS = 5;
    private static final int CONTROL_RESERVE_SYSTEM_WRITER_CLASS = 6;
    private static final byte INFLIGHT_CLAIMED_KIND = 1;
    private static final byte INFLIGHT_PUBLISHING_KIND = 2;
    private static final byte INFLIGHT_UNCERTAIN_KIND = 3;

    private final ShardStore store;
    private final DelayShardConfig config;
    private final PayloadProofTrustSet payloadProofTrustSet;
    private final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog;
    private final RetryPolicyCatalog retryPolicyCatalog;
    private final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority;
    private final ProfileCatalog profileCatalog;
    private PayloadProofTrustSetControlState payloadProofTrustSetControlState;
    private ProfileBindingControlState profileBindingControlState;
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
        this(store, config, payloadProofTrustSet, capacityEnvelope, v1ScheduleResolver,
                payloadProofTrustSetControlCatalog, null);
    }

    /**
     * Opens a shard with an optional source-position-pinned Retry Policy
     * catalog. A supplied catalog turns on fail-closed semantic lookup for
     * V1 Schedule/Prepare; older constructors retain compatibility behavior.
     */
    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet,
                      final ShardCapacityEnvelopeV1 capacityEnvelope,
                      final V1ScheduleResolver v1ScheduleResolver,
                      final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
                      final RetryPolicyCatalog retryPolicyCatalog) {
        this(store, config, payloadProofTrustSet, capacityEnvelope, v1ScheduleResolver,
                payloadProofTrustSetControlCatalog, retryPolicyCatalog, null);
    }

    /**
     * Opens a shard with an optional exact Control target-registration authority.
     * When supplied, source-ordered Control markers are rejected before their
     * local handlers run unless the immutable Prepared target registration is
     * present and the marker bytes match it exactly.
     */
    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet,
                      final ShardCapacityEnvelopeV1 capacityEnvelope,
                      final V1ScheduleResolver v1ScheduleResolver,
                      final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
                      final RetryPolicyCatalog retryPolicyCatalog,
                      final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority) {
        this(store, config, payloadProofTrustSet, capacityEnvelope, v1ScheduleResolver,
                payloadProofTrustSetControlCatalog, retryPolicyCatalog, controlTargetRegistrationAuthority, null);
    }

    /**
     * Opens a shard with an optional exact Profile semantic catalog. When
     * supplied, Publish Admission applies validate the descriptor timing
     * against the pinned Destination/Delivery Capability semantics; without
     * it, only ordinary managed {@code actionAt=deliverAt} is accepted.
     */
    public DelayShard(final ShardStore store, final DelayShardConfig config,
                      final PayloadProofTrustSet payloadProofTrustSet,
                      final ShardCapacityEnvelopeV1 capacityEnvelope,
                      final V1ScheduleResolver v1ScheduleResolver,
                      final PayloadProofTrustSetControlCatalog payloadProofTrustSetControlCatalog,
                      final RetryPolicyCatalog retryPolicyCatalog,
                      final ControlTargetRegistrationAuthority controlTargetRegistrationAuthority,
                      final ProfileCatalog profileCatalog) {
        this.store = Objects.requireNonNull(store, "store");
        this.config = Objects.requireNonNull(config, "config");
        this.payloadProofTrustSet = payloadProofTrustSet;
        this.payloadProofTrustSetControlCatalog = payloadProofTrustSetControlCatalog;
        this.retryPolicyCatalog = retryPolicyCatalog;
        this.controlTargetRegistrationAuthority = controlTargetRegistrationAuthority;
        this.profileCatalog = profileCatalog;
        this.capacityEnvelope = capacityEnvelope;
        this.v1ScheduleResolver = v1ScheduleResolver;
        final var sourceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_APPLIED_SOURCE_POSITION), 1);
        final byte[] source = sourceValue == null ? null : sourceValue.payload();
        lastAppliedSourcePosition = source == null ? null : SourcePositionCodec.decode(source);
        if (lastAppliedSourcePosition != null
                && !store.shardId().equals(lastAppliedSourcePosition.shardId())) {
            throw new IllegalStateException("persisted applied source position belongs to another shard");
        }
        final var closedDeadline = store.getValue(ColumnFamily.META,
                KeyCodec.metaFixed(META_CLOSED_INGRESS_DEADLINE), 1);
        closedIngressDeadlineThrough = closedDeadline == null
                ? IngressFenceState.OPEN : IngressFenceState.decode(closedDeadline.payload()).closedThroughEpochMs();
        final var sequence = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_MUTATION_SEQUENCE), 1);
        mutationSequence = sequence == null ? 0 : readSequence(sequence.payload());
        final var claimSequenceValue = store.getValue(ColumnFamily.META, KeyCodec.metaFixed(META_CLAIM_SEQUENCE), 1);
        claimSequence = claimSequenceValue == null ? 0 : readSequence(claimSequenceValue.payload());
        final var payloadProofControlValue = store.getValue(ColumnFamily.META,
                KeyCodec.metaFixed(META_PAYLOAD_PROOF_CONTROL_STATE), PAYLOAD_PROOF_CONTROL_VALUE_TYPE);
        payloadProofTrustSetControlState = payloadProofControlValue == null
                ? PayloadProofTrustSetControlState.empty()
                : PayloadProofTrustSetControlState.decode(payloadProofControlValue.payload());
        final var profileControlValue = store.getValue(ColumnFamily.META,
                KeyCodec.metaFixed(META_PROFILE_CONTROL_STATE), PROFILE_CONTROL_VALUE_TYPE);
        profileBindingControlState = profileControlValue == null
                ? ProfileBindingControlState.empty()
                : ProfileBindingControlState.decode(profileControlValue.payload());
        validateControlStateSourcePositions();
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

    /** Returns the persisted source-ordered Profile first-binding projection. */
    public synchronized ProfileBindingControlState profileBindingControlState() {
        return profileBindingControlState;
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
                    if (!Bytes.constantTimeEquals(prior.result().appliedSourcePosition(),
                            sourcePosition.canonicalBytes())) {
                        throw new IllegalStateException("duplicate command position has conflicting source identity");
                    }
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
        final CapacityVectorV1 nonOutcomeUsage = reserveProjectionUsage(nonOutcomeEntries,
                CONTROL_RESERVE_NON_OUTCOME_CLASS, envelope.nonOutcomeControl(), "non-outcome control");
        final CapacityVectorV1 systemWriterUsage = reserveProjectionUsage(systemWriterEntries,
                CONTROL_RESERVE_SYSTEM_WRITER_CLASS, envelope.nonOutcomeControl(), "system-writer");
        validateControlReservePartition(nonOutcomeUsage, false, "non-outcome control");
        validateControlReservePartition(systemWriterUsage, true, "system-writer");
        ensureNonOutcomeProjectionFits(envelope.nonOutcomeControl(), nonOutcomeUsage, systemWriterUsage);
        reserveProjectionUsage(recoveryEntries, CONTROL_RESERVE_RECOVERY_CLASS,
                envelope.recoveryWorking(), "recovery working");
        reserveProjectionUsage(emergencyEntries, CONTROL_RESERVE_EMERGENCY_CLASS,
                envelope.emergencyHeadroom(), "emergency headroom");
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
        final CapacityVectorV1 nonOutcomeUsage = reserveProjectionUsage(
                scanControlReserveClass(CONTROL_RESERVE_NON_OUTCOME_CLASS),
                CONTROL_RESERVE_NON_OUTCOME_CLASS, envelope.nonOutcomeControl(), "non-outcome control");
        final CapacityVectorV1 systemWriterUsage = reserveProjectionUsage(
                scanControlReserveClass(CONTROL_RESERVE_SYSTEM_WRITER_CLASS),
                CONTROL_RESERVE_SYSTEM_WRITER_CLASS, envelope.nonOutcomeControl(), "system-writer");
        validateControlReservePartition(nonOutcomeUsage, false, "non-outcome control");
        validateControlReservePartition(systemWriterUsage, true, "system-writer");
        ensureNonOutcomeProjectionFits(envelope.nonOutcomeControl(), nonOutcomeUsage, systemWriterUsage);
        controlReserveUsage.put(CONTROL_RESERVE_NON_OUTCOME_CLASS, nonOutcomeUsage);
        controlReserveUsage.put(CONTROL_RESERVE_RECOVERY_CLASS, reserveProjectionUsage(
                scanControlReserveClass(CONTROL_RESERVE_RECOVERY_CLASS), CONTROL_RESERVE_RECOVERY_CLASS,
                envelope.recoveryWorking(), "recovery working"));
        controlReserveUsage.put(CONTROL_RESERVE_EMERGENCY_CLASS, reserveProjectionUsage(
                scanControlReserveClass(CONTROL_RESERVE_EMERGENCY_CLASS), CONTROL_RESERVE_EMERGENCY_CLASS,
                envelope.emergencyHeadroom(), "emergency headroom"));
        controlReserveUsage.put(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, systemWriterUsage);
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
        validateControlReservePartition(amount, reserveClass == CONTROL_RESERVE_SYSTEM_WRITER_CLASS,
                reserveClass == CONTROL_RESERVE_SYSTEM_WRITER_CLASS ? "system-writer" : "control reserve");
        if (capacityEnvelope == null) {
            throw new IllegalStateException("capacity envelope is required for control reserve accounting");
        }
        final CapacityGrantV1 grant = controlReserveGrant(reserveClass);
        final CapacityVectorV1 current = controlReserveUsage.getOrDefault(reserveClass, CapacityVectorV1.empty());
        final CapacityVectorV1 next = add ? current.add(amount) : current.subtract(amount);
        final CapacityVectorV1 sibling = reserveClass == CONTROL_RESERVE_NON_OUTCOME_CLASS
                ? controlReserveUsage.getOrDefault(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, CapacityVectorV1.empty())
                : reserveClass == CONTROL_RESERVE_SYSTEM_WRITER_CLASS
                ? controlReserveUsage.getOrDefault(CONTROL_RESERVE_NON_OUTCOME_CLASS, CapacityVectorV1.empty())
                : CapacityVectorV1.empty();
        if (!grant.vector().covers(next.add(sibling))) {
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
            case CONTROL_RESERVE_NON_OUTCOME_CLASS, CONTROL_RESERVE_SYSTEM_WRITER_CLASS ->
                    capacityEnvelope.nonOutcomeControl();
            case CONTROL_RESERVE_RECOVERY_CLASS -> capacityEnvelope.recoveryWorking();
            case CONTROL_RESERVE_EMERGENCY_CLASS -> capacityEnvelope.emergencyHeadroom();
            default -> throw new IllegalArgumentException("unsupported mutable control reserve class: "
                    + reserveClass);
        };
    }

    private static void validateMutableControlReserveClass(final int reserveClass) {
        if (reserveClass < CONTROL_RESERVE_NON_OUTCOME_CLASS
                || reserveClass > CONTROL_RESERVE_SYSTEM_WRITER_CLASS) {
            throw new IllegalArgumentException("only CONTROL_RESERVE classes 3-6 are mutable locally");
        }
    }

    /**
     * Class 6 is a projection of the Route Broker system-writer budget.  It
     * shares the immutable NON_OUTCOME_CONTROL grant with class 3, but the
     * two projections must remain dimension-disjoint so the same grant cannot
     * be charged twice.  The actual Broker quota authority remains outside
     * this shard-local persistence primitive.
     */
    private static void validateControlReservePartition(final CapacityVectorV1 vector,
                                                         final boolean systemWriter,
                                                         final String name) {
        Objects.requireNonNull(vector, "vector");
        for (CapacityDimensionV1 dimension : CapacityDimensionV1.values()) {
            final boolean writerDimension = dimension == CapacityDimensionV1.SYSTEM_WRITER_RESERVED_RECORDS
                    || dimension == CapacityDimensionV1.SYSTEM_WRITER_RESERVED_BYTES
                    || dimension == CapacityDimensionV1.SYSTEM_WRITER_RESERVED_BYTES_PER_SECOND;
            if (writerDimension != systemWriter && vector.amount(dimension) != 0) {
                throw new IllegalArgumentException(name + " projection contains an out-of-partition dimension: "
                        + dimension);
            }
        }
    }

    private static void ensureNonOutcomeProjectionFits(final CapacityGrantV1 grant,
                                                        final CapacityVectorV1 nonOutcomeUsage,
                                                        final CapacityVectorV1 systemWriterUsage) {
        if (!grant.vector().covers(nonOutcomeUsage.add(systemWriterUsage))) {
            throw new IllegalStateException("non-outcome and system-writer projections exceed immutable grant");
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
        Objects.requireNonNull(messageId, "messageId");
        requireMessageShard(messageId, "message lookup");
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idMessage(messageId), 1);
        return value == null ? null : validateMessageSourcePosition(messageId,
                MessageRecord.decode(value.payload()), "message lookup");
    }

    /** Returns the exact accepted Registry Schedule/Prepare binding, if any. */
    public synchronized V1ScheduleBinding getV1ScheduleBinding(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        if (!store.shardId().equals(messageId.routingId().shardId())) {
            throw new IllegalStateException("V1 Schedule binding key shard mismatch");
        }
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
        validateSourcePositionShard(result.appliedSourcePosition(), "DLQ export lookup");
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
        return validateClaimShard(claim, "claim lookup");
    }

    /**
     * Finds the one live Claim for a Message Identity without trusting an Owner Epoch.
     * A duplicate or over-bound scan fences the caller instead of guessing.
     */
    public synchronized ClaimRecord findClaimForMessage(final DelayMessageId messageId) {
        Objects.requireNonNull(messageId, "messageId");
        requireMessageShard(messageId, "Claim lookup");
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
        final TimelineWorkRef currentTimeline = current.runtimeIndex().timeline();
        final int workKind = currentTimeline != null
                && Arrays.equals(currentTimeline.encodedTimelineKey(), timelineKey)
                ? currentTimeline.workKind().wireValue()
                : current.retryEligibilityAtEpochMs() == current.deliverAtEpochMs() ? 1 : 2;
        final byte[] precondition = buildClaimPrecondition(claimId, messageId, current, lane, timelineKey,
                owner, claimDeadlineEpochMs, materialization, claimedCharge, workKind);
        MessageRecord next = new MessageRecord(MessageStatus.CLAIMED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs());
        final ClaimRecord claim = ClaimRecord.claimed(messageId, current.generation(), claimId, owner.generation(),
                nextClaimSequence, current.laneId(), lane.laneIncarnation(), lane.laneControlVersion(),
                lane.laneVersion(), owner.canonicalBytes(), store.metadata().storeIncarnation(), precondition,
                timelineKey, next.stateVersion(), currentTimeline == null ? null : currentTimeline.canonicalBytes());
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
        final TimelineWorkKind workKind = TimelineWorkKind.fromWire(precondition.sourceWorkKind());
        final byte[] sourceTimelineWork = claim.sourceTimelineWork();
        if (workKind == TimelineWorkKind.UNCERTAIN_RETRY && sourceTimelineWork.length == 0) {
            throw new IllegalStateException("legacy Claim lacks UNCERTAIN_RETRY source work projection");
        }
        if (sourceTimelineWork.length == 0) {
            next = next.withRuntimeIndex(timelineRuntimeIndex(claim.delayMessageId(), next, workKind,
                    Math.addExact(current.runtimeIndex().admissionsUsed(), 1), next.stateVersion(),
                    UncertainRetryAuthority.NONE, null, null, current.runtimeIndex()));
        } else {
            final TimelineWorkRef priorWork = TimelineWorkRef.decode(sourceTimelineWork);
            final TimelineWorkRef restoredWork = new TimelineWorkRef(priorWork.workKind(),
                    priorWork.encodedTimelineKey(), priorWork.actionAtEpochMs(),
                    priorWork.retryEligibilityAtEpochMs(), priorWork.candidateAttemptNo(), next.stateVersion(),
                    priorWork.orderedHeadBlocking(), priorWork.uncertainRetryAuthority(),
                    priorWork.uncertainRetryControl(), priorWork.uncertainRetryControlPosition());
            final GenerationAggregateState requestedAggregate = switch (workKind) {
                case INITIAL_SCHEDULE -> GenerationAggregateState.SCHEDULED;
                case DEFINITIVE_RETRY -> GenerationAggregateState.RETRY_WAIT;
                case UNCERTAIN_RETRY -> GenerationAggregateState.UNCERTAIN;
            };
            next = next.withRuntimeIndex(GenerationRuntimeIndex.timeline(requestedAggregate, restoredWork,
                    current.runtimeIndex().attemptObligations(), current.runtimeIndex().admissionsUsed(),
                    current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                    current.runtimeIndex().possibleDestinationDuplicate(), next.stateVersion()));
        }
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

    /**
     * Revokes every live reversible Claim owned by one Owner Epoch.
     *
     * <p>The bounded scan is performed while the shard single-writer lock is
     * held, so the discovered Claim identities cannot change between the scan
     * and the individual atomic rollback batches.  A scan over the configured
     * bound fails closed instead of silently leaving an unknown Claim during
     * owner drain.</p>
     */
    public synchronized int revokeClaimsForOwner(final long ownerEpoch) {
        if (ownerEpoch <= 0) {
            throw new IllegalArgumentException("ownerEpoch must be positive");
        }
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{INFLIGHT_CLAIMED_KIND, 1}, new byte[]{INFLIGHT_PUBLISHING_KIND, 1}, limit);
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("Claim scan exceeded configured bound during owner drain");
        }
        final List<ClaimRecord> ownedClaims = new ArrayList<>();
        for (var entry : entries) {
            final ClaimRecord claim = decodeClaim(entry);
            if (claim.ownerEpoch() == ownerEpoch) {
                ownedClaims.add(claim);
            }
        }
        int revoked = 0;
        for (ClaimRecord claim : ownedClaims) {
            if (revokeClaim(claim.claimId(), ownerEpoch) != null) {
                revoked++;
            }
        }
        return revoked;
    }

    public synchronized PayloadReservation getReservation(final byte[] reservationId) {
        final PayloadReservation stored = readStoredReservation(reservationId);
        return stored == null ? null : effectiveReservation(stored);
    }

    private PayloadReservation readStoredReservation(final byte[] reservationId) {
        final var value = store.getValue(ColumnFamily.ID, KeyCodec.idReservation(reservationId), 2);
        if (value == null) {
            return null;
        }
        return validateReservationIdentity(reservationId, PayloadReservation.decode(value.payload()),
                "reservation lookup");
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
        final PayloadReservation current = validateReservationIdentity(reservationId,
                PayloadReservation.decode(value.payload()), "reservation expiry materialization");
        final LaneRecord lane = readLane(current.intent().laneId());
        if (lane != null && lane.admissionGate() == AdmissionGate.CLOSED
                && current.status() == PayloadReservationStatus.RESERVED) {
            return effectiveReservation(current);
        }
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
        Objects.requireNonNull(commandId, "commandId");
        requireCommandShard(commandId, "command result lookup");
        final var value = store.getValue(ColumnFamily.DEDUPE, KeyCodec.dedupeResult(commandId), 2);
        if (value == null) {
            return null;
        }
        final CommandResult result = CommandResult.decode(value.payload());
        validateSourcePositionShard(result.appliedSourcePosition(), "command result lookup");
        return result;
    }

    /**
     * Checks the immutable command identity evidence retained with a durable
     * result.  A query locator contains more than the command id; its command
     * hash must match the dedupe record before the result can be projected.
     */
    public synchronized boolean matchesCommandHash(final CommandId commandId, final byte[] expectedCommandHash) {
        Objects.requireNonNull(commandId, "commandId");
        Bytes.requireLength(expectedCommandHash, 32, "expectedCommandHash");
        requireCommandShard(commandId, "command identity lookup");
        final CommandDedupeRecord record = readCommandDedupe(commandId);
        return record != null && Bytes.constantTimeEquals(record.commandHash(), expectedCommandHash);
    }

    public synchronized SystemMutationResult getSystemMutationResult(final byte[] mutationId) {
        Bytes.requireLength(mutationId, SystemMutation.HASH_LENGTH, "mutationId");
        final var value = store.getValue(ColumnFamily.DEDUPE, KeyCodec.dedupeSystemMutation(mutationId),
                SystemMutationResult.VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final SystemMutationResult result = SystemMutationResult.decode(value.payload());
        if (!Bytes.constantTimeEquals(result.mutationId(), mutationId)) {
            throw new IllegalStateException("system mutation result key/value identity mismatch");
        }
        validateSourcePositionShard(result.appliedSourcePosition(), "system mutation result lookup");
        return result;
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
            return validateGcIntentIdentity(ResourceRetireIntentRecord.decode(
                    ValueEnvelope.decode(raw, ResourceRetireIntentRecord.VALUE_TYPE).payload()), resourceKind,
                    resourceIdentityHash, expectedVersion);
        }
        if (valueType == ResourceDeleteConfirmedRecord.VALUE_TYPE) {
            return validateGcIntentIdentity(ResourceDeleteConfirmedRecord.decode(
                    ValueEnvelope.decode(raw, ResourceDeleteConfirmedRecord.VALUE_TYPE).payload()).retireIntent(),
                    resourceKind, resourceIdentityHash, expectedVersion);
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
        return validateGcConfirmationIdentity(ResourceDeleteConfirmedRecord.decode(
                ValueEnvelope.decode(raw, ResourceDeleteConfirmedRecord.VALUE_TYPE).payload()), resourceKind,
                resourceIdentityHash, expectedVersion);
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
        if (controlTargetRegistrationAuthority != null && requiresControlTargetRegistration(mutation.type())) {
            final SystemMutationResult registrationResult = validateRegisteredControlMutation(mutation,
                    sourcePosition);
            if (registrationResult != null) {
                return registrationResult;
            }
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

    private SystemMutationResult validateRegisteredControlMutation(final SystemMutation mutation,
                                                                    final SourcePosition sourcePosition) {
        try {
            final ControlRef controlRef = controlRefFor(mutation);
            final PreparedControlOperationV1 prepared = controlTargetRegistrationAuthority.find(
                    controlRef.operationId()).orElse(null);
            if (prepared == null) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            final ControlTargetRefV1 target = prepared.targets().stream()
                    .filter(candidate -> candidate.targetIndex() == controlRef.targetIndex())
                    .findFirst().orElse(null);
            if (target == null) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            controlTargetRegistrationAuthority.validateMutation(prepared, target, mutation);
            return null;
        } catch (RuntimeException exception) {
            // A configured target registry is a fail-closed boundary.  A
            // missing/malformed/drifting registration must never reach a
            // local Control handler and must still advance the source log.
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
    }

    private static boolean requiresControlTargetRegistration(final SystemMutationType type) {
        return type == SystemMutationType.APPLY_SHARD_CONTROL
                || type == SystemMutationType.REPLAY_DEAD_LETTER
                || type == SystemMutationType.RESOLVE_UNCERTAIN;
    }

    private static ControlRef controlRefFor(final SystemMutation mutation) {
        return switch (mutation.type()) {
            case APPLY_SHARD_CONTROL -> ApplyShardControlBody.decode(mutation.canonicalBody()).controlRef();
            case REPLAY_DEAD_LETTER -> ReplayDeadLetterBody.decode(mutation.canonicalBody()).controlRef();
            case RESOLVE_UNCERTAIN -> ResolveUncertainBody.decode(mutation.canonicalBody()).controlRef();
            default -> throw new IllegalArgumentException("mutation has no ControlRef");
        };
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
        if (body.controlKind() == 2 || body.controlKind() == 3) {
            return applyProfileBindingControlMutation(body, mutation, sourcePosition);
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
        final CloseAccounting closeAccounting;
        final LaneCloseMaterializationCursor closeCursor;
        try {
            if (body.controlKind() == 11) {
                closeAccounting = prepareCloseAccounting(target.laneId(), rollbacks);
                closeCursor = new LaneCloseMaterializationCursor(target.laneId(), next.laneIncarnation(),
                        next.laneControlVersion(), sourcePosition.canonicalBytes(),
                        LaneCloseMaterializationCursor.Phase.MESSAGES, null,
                        closeAccounting.pendingMessages(), closeAccounting.pendingBytes(),
                        closeAccounting.reservationMessages(), closeAccounting.reservationBytes());
            } else {
                closeAccounting = CloseAccounting.empty();
                closeCursor = null;
            }
        } catch (IllegalStateException exception) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.INTEGRITY_ERROR);
        }
        final ShardQuota nextQuota;
        try {
            nextQuota = body.controlKind() == 11
                    ? quota.removeSchedules(closeAccounting.pendingMessages(), closeAccounting.pendingBytes())
                    .removeReservations(closeAccounting.reservationMessages(), closeAccounting.reservationBytes())
                    : quota;
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
            if (closeCursor != null) {
                batch.putValue(ColumnFamily.TIMELINE, LaneCloseMaterializationCursor.VALUE_TYPE,
                        closeCursorKey(closeCursor), closeCursor.canonicalBytes());
            }
            if (!nextQuota.equals(quota)) {
                batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            }
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        quota = nextQuota;
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

    private SystemMutationResult applyProfileBindingControlMutation(
            final ApplyShardControlBody body, final SystemMutation mutation,
            final SourcePosition sourcePosition) {
        final ProfileRefV1 profile;
        final ProfileBindingControlState next;
        if (body.controlKind() == 2) {
            final ProfileBindingActivatePayloadV1 payload = body.profileBindingActivate();
            profile = payload.profile();
            if (!profileReferenceMatchesBody(body, profile)) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = profileBindingControlState.activate(profile, sourcePosition);
        } else {
            final ProfileNewBindingClosePayloadV1 payload = body.profileNewBindingClose();
            profile = payload.profile();
            if (!profileReferenceMatchesBody(body, profile)) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                        StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            next = profileBindingControlState.close(payload, sourcePosition);
        }
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        store.write(batch -> {
            batch.putValue(ColumnFamily.META, PROFILE_CONTROL_VALUE_TYPE,
                    KeyCodec.metaFixed(META_PROFILE_CONTROL_STATE), next.canonicalBytes());
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        profileBindingControlState = next;
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        return result;
    }

    private static boolean profileReferenceMatchesBody(final ApplyShardControlBody body,
                                                       final ProfileRefV1 profile) {
        return body.semanticVersion() == profile.version()
                && Bytes.constantTimeEquals(body.semanticHash(), profile.semanticHash());
    }

    private void requireProfileFirstBinding(final ProfileRefV1 profile, final SourcePosition sourcePosition) {
        if (!profileBindingControlState.hasMarkers()) {
            return;
        }
        final ProfileAcceptanceV1 acceptance = profileBindingControlState.firstBindingAcceptance(profile,
                sourcePosition);
        if (acceptance == ProfileAcceptanceV1.ABSENT) {
            throw new V1CommandResolutionException(StableCode.PROFILE_VERSION_NOT_ACTIVE_AT_SOURCE_POSITION,
                    "Profile version is not active for first binding at this source position");
        }
        if (acceptance == ProfileAcceptanceV1.CLOSED_FOR_FIRST_BINDING) {
            throw new V1CommandResolutionException(StableCode.PROFILE_DEPRECATED_FOR_NEW_USE,
                    "Profile version is closed for first binding at this source position");
        }
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

    /**
     * Computes the one-time quota transfer owned by a Close marker.  The scan
     * is deliberately bounded by the shard's hard pending-message limit: if a
     * complete proof cannot be made, the source mutation is rejected rather
     * than installing a close overlay with guessed counters.
     */
    private CloseAccounting prepareCloseAccounting(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId,
            final List<LaneClaimRollback> rollbacks) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final Map<DelayMessageId, MessageRecord> rollbackMessages = new HashMap<>();
        for (LaneClaimRollback rollback : rollbacks) {
            rollbackMessages.put(rollback.claim().delayMessageId(), rollback.nextMessage());
        }
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> messages = store.scan(ColumnFamily.ID,
                new byte[]{1, 1}, new byte[]{2, 1}, limit);
        if (messages.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("message scan exceeded configured bound during lane close");
        }
        long pendingMessages = 0;
        long pendingBytes = 0;
        for (var entry : messages) {
            final MessageRecord stored = decodeMessageEntry(entry, "lane close accounting");
            final MessageRecord message = rollbackMessages.getOrDefault(messageIdFromEntry(entry), stored);
            if (message.laneId().equals(laneId) && isUnadmittedGeneration(message)) {
                pendingMessages = Math.addExact(pendingMessages, 1);
                pendingBytes = Math.addExact(pendingBytes, message.payloadLength());
            }
        }
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> reservations = store.scan(ColumnFamily.ID,
                new byte[]{2, 1}, new byte[]{3, 1}, limit);
        if (reservations.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("reservation scan exceeded configured bound during lane close");
        }
        long reservationMessages = 0;
        long reservationBytes = 0;
        for (var entry : reservations) {
            final PayloadReservation reservation = decodeReservationEntry(entry, "lane close accounting");
            if (reservation.intent().laneId().equals(laneId)
                    && reservation.status() == PayloadReservationStatus.RESERVED) {
                reservationMessages = Math.addExact(reservationMessages, 1);
                reservationBytes = Math.addExact(reservationBytes,
                        reservation.intent().expectedPayloadLength());
            }
        }
        return new CloseAccounting(pendingMessages, pendingBytes, reservationMessages, reservationBytes);
    }

    private static boolean isUnadmittedGeneration(final MessageRecord message) {
        return (message.status() == MessageStatus.SCHEDULED || message.status() == MessageStatus.CLAIMED)
                && message.runtimeIndex().attemptObligations().isEmpty();
    }

    private MessageRecord decodeMessageEntry(
            final io.nereusstream.delay.store.ShardStore.KeyValue entry, final String context) {
        if (entry.key().length != 2 + DelayMessageId.LENGTH || entry.key()[0] != 1 || entry.key()[1] != 1) {
            throw new IllegalStateException("invalid MESSAGE key during " + context);
        }
        final DelayMessageId messageId = messageIdFromEntry(entry);
        return validateMessageSourcePosition(messageId,
                MessageRecord.decode(ValueEnvelope.decode(entry.value(), 1).payload()), context);
    }

    private static DelayMessageId messageIdFromEntry(
            final io.nereusstream.delay.store.ShardStore.KeyValue entry) {
        return new DelayMessageId(Arrays.copyOfRange(entry.key(), 2, entry.key().length));
    }

    private PayloadReservation decodeReservationEntry(
            final io.nereusstream.delay.store.ShardStore.KeyValue entry, final String context) {
        if (entry.key().length != 2 + 32 || entry.key()[0] != 2 || entry.key()[1] != 1) {
            throw new IllegalStateException("invalid RESERVATION key during " + context);
        }
        return validateReservationIdentity(Arrays.copyOfRange(entry.key(), 2, entry.key().length),
                PayloadReservation.decode(ValueEnvelope.decode(entry.value(), 2).payload()), context);
    }

    private PayloadReservation validateReservationIdentity(final byte[] reservationId,
                                                            final PayloadReservation reservation,
                                                            final String context) {
        if (!Arrays.equals(reservation.reservationId(), reservationId)
                || !reservation.shardId().equals(store.shardId())) {
            throw new IllegalStateException("RESERVATION key/value identity mismatch during " + context);
        }
        validateSourcePositionShard(reservation.sourcePosition(), "RESERVATION " + context);
        return reservation;
    }

    private MessageRecord validateMessageSourcePosition(final DelayMessageId messageId,
                                                        final MessageRecord message,
                                                        final String context) {
        requireMessageShard(messageId, context);
        validateSourcePositionShard(message.scheduleSourcePosition(), "MESSAGE " + context);
        return message;
    }

    private void requireMessageShard(final DelayMessageId messageId, final String context) {
        if (!store.shardId().equals(messageId.routingId().shardId())) {
            throw new IllegalStateException("MESSAGE key shard mismatch during " + context);
        }
    }

    private void requireCommandShard(final CommandId commandId, final String context) {
        if (!store.shardId().equals(commandId.routingId().shardId())) {
            throw new IllegalStateException("command result key shard mismatch during " + context);
        }
    }

    private void validateSourcePositionShard(final byte[] encodedSourcePosition, final String context) {
        final SourcePosition sourcePosition = SourcePositionCodec.decode(encodedSourcePosition);
        if (!store.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalStateException("source position shard mismatch during " + context);
        }
    }

    private void validateControlStateSourcePositions() {
        payloadProofTrustSetControlState.activations().forEach(marker ->
                validateSourcePositionShard(marker.sourcePosition().canonicalBytes(),
                        "payload proof trust-set activation state"));
        payloadProofTrustSetControlState.closures().forEach(marker ->
                validateSourcePositionShard(marker.sourcePosition().canonicalBytes(),
                        "payload proof trust-set closure state"));
        profileBindingControlState.activations().forEach(marker ->
                validateSourcePositionShard(marker.sourcePosition().canonicalBytes(),
                        "Profile activation state"));
        profileBindingControlState.closures().forEach(marker ->
                validateSourcePositionShard(marker.sourcePosition().canonicalBytes(),
                        "Profile closure state"));
    }

    private List<ClosedMessageAction> prepareClosedMessageActions(
            final LaneCloseMaterializationCursor cursor,
            final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries,
            final SourcePosition closePosition) {
        final List<ClosedMessageAction> result = new ArrayList<>();
        for (var entry : entries) {
            final DelayMessageId messageId = messageIdFromEntry(entry);
            final MessageRecord current = decodeMessageEntry(entry, "Lane close materialization");
            if (!current.laneId().equals(cursor.laneId()) || !isUnadmittedGeneration(current)) {
                continue;
            }
            final ClaimRecord claim = current.status() == MessageStatus.CLAIMED
                    ? findClaimForMessage(messageId) : null;
            if (current.status() == MessageStatus.CLAIMED && claim == null) {
                throw new IllegalStateException("closed CLAIMED message has no durable Claim");
            }
            if (claim != null && (!claim.delayMessageId().equals(messageId)
                    || claim.generation() != current.generation()
                    || claim.runtimeRevision() != current.stateVersion())) {
                throw new IllegalStateException("closed Claim does not match current message");
            }
            final long nextStateVersion = Math.addExact(current.stateVersion(), 1);
            final MessageRecord terminalMessage = new MessageRecord(MessageStatus.DEAD_LETTER,
                    current.generation(), nextStateVersion, current.deliverAtEpochMs(), current.expireAtEpochMs(),
                    current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                    current.payloadReference(), current.retryEligibilityAtEpochMs()).withRuntimeIndex(
                    GenerationRuntimeIndex.none(GenerationAggregateState.DEAD_LETTER, List.of(),
                            current.runtimeIndex().admissionsUsed(),
                            current.runtimeIndex().uncertainRetryAdmissionsUsed(), false, nextStateVersion));
            final TerminalGenerationRecord terminal = new TerminalGenerationRecord(messageId,
                    current.generation(), MessageStatus.DEAD_LETTER, StableCode.LANE_CLOSED_BEFORE_ADMISSION,
                    nextStateVersion, closePosition.canonicalBytes(), false, List.of());
            result.add(new ClosedMessageAction(messageId, current, claim,
                    claim == null ? timelineKey(messageId, current) : claim.timelineKey(),
                    expiryKey(messageId, current), terminalMessage, terminal));
        }
        return List.copyOf(result);
    }

    private List<ClosedReservationAction> prepareClosedReservationActions(
            final LaneCloseMaterializationCursor cursor,
            final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries,
            final SourcePosition closePosition) {
        final List<ClosedReservationAction> result = new ArrayList<>();
        for (var entry : entries) {
            final PayloadReservation reservation = decodeReservationEntry(entry,
                    "Lane close materialization");
            if (!reservation.intent().laneId().equals(cursor.laneId())
                    || reservation.status() != PayloadReservationStatus.RESERVED) {
                continue;
            }
            final PayloadReservation closed = new PayloadReservation(reservation.shardId(),
                    reservation.reservationId(), reservation.commandId(), reservation.delayMessageId(),
                    reservation.commandHash(), reservation.intent(), reservation.reservationExpiryEpochMs(),
                    PayloadReservationStatus.ABANDONED, Math.addExact(reservation.stateVersion(), 1),
                    closePosition.canonicalBytes(), null);
            result.add(new ClosedReservationAction(reservation, closed));
        }
        return List.copyOf(result);
    }

    private CursorScan scanAfter(final ColumnFamily family, final byte[] lowerInclusive,
                                 final byte[] upperExclusive, final byte[] lastKey, final int limit) {
        final int requestLimit = limit == Integer.MAX_VALUE ? limit : Math.addExact(limit, 1);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> scanned = store.scan(family,
                lastKey.length == 0 ? lowerInclusive : lastKey, upperExclusive, requestLimit);
        int start = 0;
        if (lastKey.length != 0 && !scanned.isEmpty() && Arrays.equals(scanned.get(0).key(), lastKey)) {
            start = 1;
        }
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> after = scanned.subList(start, scanned.size());
        final boolean more = after.size() > limit;
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = more
                ? after.subList(0, limit) : after;
        return new CursorScan(List.copyOf(entries), more);
    }

    private static byte[] closeCursorKey(final LaneCloseMaterializationCursor cursor) {
        return closeCursorKey(cursor.laneId(), cursor.closeVersion());
    }

    private static byte[] closeCursorKey(final io.nereusstream.delay.protocol.DestinationLaneId laneId,
                                         final long closeVersion) {
        return KeyCodec.timelineSystem(LaneCloseMaterializationCursor.SYSTEM_WORK_KIND, 0,
                laneId.bytes(), closeVersion);
    }

    private static SystemTimelineKey decodeLaneCloseWorkKey(final byte[] key) {
        Objects.requireNonNull(key, "system work key");
        if (key.length < 3 + Long.BYTES + Integer.BYTES + Long.BYTES || key[0] != 6 || key[1] != 1
                || key[2] != LaneCloseMaterializationCursor.SYSTEM_WORK_KIND) {
            throw new IllegalStateException("invalid Lane close system work key");
        }
        final ByteBuffer input = ByteBuffer.wrap(key);
        input.position(3);
        final long nextEligibleAt = input.getLong();
        final int workIdLength = input.getInt();
        if (nextEligibleAt < 0 || workIdLength != DestinationLaneId.LENGTH
                || input.remaining() != workIdLength + Long.BYTES) {
            throw new IllegalStateException("invalid Lane close system work key fields");
        }
        final byte[] workId = new byte[workIdLength];
        input.get(workId);
        final long workVersion = input.getLong();
        if (workVersion <= 0) {
            throw new IllegalStateException("Lane close system work version must be positive");
        }
        return new SystemTimelineKey(nextEligibleAt, workId, workVersion);
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
            final MessageRecord message = decodeMessageEntry(entry, "lane Close");
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
        try {
            validatePublishAdmissionTiming(body);
            body.requireTiming(body.descriptor().actionAtEpochMs(), body.descriptor().expireAtEpochMs());
            body.requireBrokerTiming(sourcePosition.brokerPersistenceTimeEpochMs(),
                    config.maxIngressBrokerTimestampDivergenceMs(),
                    config.maximumAdmissionMutationEnqueueAgeMs());
        } catch (IllegalArgumentException timingFailure) {
            revokeMatchingAdmissionClaim(body, author);
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
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
            replayState = validatePublishAdmissionReplayState(body, current, lane, localClaim, sourcePosition);
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

    private void revokeMatchingAdmissionClaim(final PublishAdmissionBody body,
                                               final io.nereusstream.delay.protocol.AuthorIdentity author) {
        final ClaimRecord claim = getClaim(body.claimId(), author.generation());
        if (claim == null || !Arrays.equals(claim.preconditionBytes(), body.claimPrecondition().canonicalBytes())) {
            return;
        }
        final MessageRecord current = getMessage(new DelayMessageId(body.messageId()));
        if (current == null || current.status() != MessageStatus.CLAIMED
                || current.generation() != body.generation()
                || current.stateVersion() != claim.runtimeRevision()
                || !Arrays.equals(current.runtimeIndex().claimId(), claim.claimId())) {
            return;
        }
        revokeClaim(claim.claimId(), author.generation());
    }

    private void validatePublishAdmissionTiming(final PublishAdmissionBody body) {
        if (profileCatalog == null) {
            body.requireOrdinaryManagedTiming();
            return;
        }
        final ProfileRefV1 destinationRef = ProfileRefV1.decode(body.descriptor().destinationProfile());
        final ProfileRefV1 capabilityRef = ProfileRefV1.decode(body.descriptor().capabilityProfile());
        final ProfileSemanticEnvelopeV1 destination = profileCatalog.resolve(destinationRef);
        final ProfileSemanticEnvelopeV1 capability = profileCatalog.resolve(capabilityRef);
        if (destination == null || capability == null
                || !destination.ref().equals(destinationRef)
                || !capability.ref().equals(capabilityRef)
                || !(destination.body() instanceof DestinationProfileSemanticV1 destinationBody)
                || !(capability.body() instanceof DeliveryCapabilitySemanticV1 capabilityBody)) {
            throw new IllegalArgumentException("Publish Admission Profile semantics are unavailable");
        }
        body.requireTimingPolicy(destinationBody, capabilityBody);
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
                batch.putRuntimeMetadata(store.runtimeMetadata().withLastIngressFenceProofId(proofId));
                writeSystemResult(batch, result);
                writePosition(batch, sourcePosition);
            });
        } else {
            store.write(batch -> {
                batch.putIngressFenceDeadline(closeThrough);
                batch.putRuntimeMetadata(store.runtimeMetadata().withLastIngressFenceProofId(proofId));
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
     * Applies the source-ordered Resolve subset. Verified-published evidence
     * can settle the exact UNCERTAIN obligation locally; absent evidence and
     * possible-delivery terminalization retain their explicit source-ordered
     * branches until the remaining result/charge projection is available.
     */
    private SystemMutationResult applyResolveUncertainMutation(final SystemMutation mutation,
                                                                final SourcePosition sourcePosition) {
        final ResolveUncertainBody body = ResolveUncertainBody.decode(mutation.canonicalBody());
        if (!Arrays.equals(mutation.logicalOperationIdentity(), body.controlRef()
                .logicalOperationIdentity(SystemMutationType.RESOLVE_UNCERTAIN))) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.REJECTED,
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (body.resolutionKind() == 1) {
            return applyPublishedEvidenceAttachment(body, mutation, sourcePosition);
        }
        if (body.resolutionKind() == 2) {
            return applyNotPublishedEvidenceAttachment(body, mutation, sourcePosition);
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
        final RetryPolicySemanticV1 pinnedPolicy = retryPolicyFor(body.messageId(), current, sourcePosition);
        final int maxPublishAdmissions = pinnedPolicy == null
                ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
        if (current.runtimeIndex().admissionsUsed() >= maxPublishAdmissions) {
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

    /** Settles a verified-published Resolve attachment against one exact obligation. */
    private SystemMutationResult applyPublishedEvidenceAttachment(final ResolveUncertainBody body,
                                                                   final SystemMutation mutation,
                                                                   final SourcePosition sourcePosition) {
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() < body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final PublishAttemptLedger ledger = findOpenPublishAttempt(body.publishAttemptId());
        if (ledger == null || ledger.state() != AttemptLedgerState.UNCERTAIN
                || !ledger.delayMessageId().equals(body.messageId())
                || !ledger.laneId().equals(body.laneId())
                || !Arrays.equals(ledger.laneIncarnation(), body.laneIncarnation())
                || ledger.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        if (current.generation() == body.generation() && !current.laneId().equals(body.laneId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() > body.generation()) {
            final TerminalGenerationRecord summary = getTerminalGeneration(body.messageId(), body.generation());
            if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                        StableCode.TOO_LATE);
            }
        }
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED, StableCode.OK,
                sourcePosition.canonicalBytes());
        try {
            applyPublishedPublishOutcome(ledger, sourcePosition, result, MessageStatus.UNCERTAIN);
            return result;
        } catch (IllegalStateException exception) {
            return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    /**
     * Settles one exact UNCERTAIN obligation with authenticated definitive
     * non-publication evidence.  The evidence branch never invents a retry
     * policy: once the named obligation is removed, the remaining runtime
     * index either stays uncertain, preserves another current PUBLISHING
     * attempt, or follows the ordinary all-absent definitive-retry
     * normalization.
     */
    private SystemMutationResult applyNotPublishedEvidenceAttachment(final ResolveUncertainBody body,
                                                                      final SystemMutation mutation,
                                                                      final SourcePosition sourcePosition) {
        final MessageRecord current = getMessage(body.messageId());
        if (current == null) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() < body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final PublishAttemptLedger ledger = findOpenPublishAttempt(body.publishAttemptId());
        if (ledger == null || ledger.state() != AttemptLedgerState.UNCERTAIN
                || !ledger.delayMessageId().equals(body.messageId())
                || !ledger.laneId().equals(body.laneId())
                || !Arrays.equals(ledger.laneIncarnation(), body.laneIncarnation())
                || ledger.generation() != body.generation()) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        if (current.generation() == body.generation() && !current.laneId().equals(body.laneId())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED, StableCode.TOO_LATE);
        }
        if (current.generation() > body.generation()) {
            final TerminalGenerationRecord summary = getTerminalGeneration(body.messageId(), body.generation());
            if (summary == null || !summary.openObligations().contains(ledger.obligationRef())) {
                return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                        StableCode.TOO_LATE);
            }
            final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                    StableCode.OK, sourcePosition.canonicalBytes());
            try {
                settleHistoricalTerminalObligation(ledger, sourcePosition, result, false);
                return result;
            } catch (IllegalStateException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }

        final GenerationRuntimeIndex index = current.runtimeIndex();
        if (!containsObligation(index, ledger.obligationRef())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        if (isTerminalStatus(current.status())) {
            final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                    StableCode.OK, sourcePosition.canonicalBytes());
            try {
                settleTerminalObligation(ledger, current, sourcePosition, result, false);
                return result;
            } catch (IllegalStateException exception) {
                return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
            }
        }

        final LaneRecord lane = readLane(current.laneId());
        if (lane == null || !Arrays.equals(lane.laneIncarnation(), body.laneIncarnation())) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.STALE_SYSTEM_MUTATION);
        }
        final List<AttemptObligationRef> remaining = withoutObligation(index, ledger.publishAttemptId());
        final boolean remainingUncertain = remaining.stream()
                .anyMatch(ref -> ref.ledgerState() == AttemptLedgerState.UNCERTAIN);
        final SystemMutationResult result = SystemMutationResult.from(mutation, ApplyStatus.APPLIED,
                StableCode.OK, sourcePosition.canonicalBytes());
        try {
            if (remainingUncertain) {
                return settleNotPublishedUncertainObligation(ledger, current, remaining, sourcePosition, result);
            }
            if (index.currentWorkKind() == CurrentSendWorkKind.PUBLISHING) {
                return preservePublishingAfterNotPublishedEvidence(ledger, current, remaining, sourcePosition,
                        result);
            }
            if (lane.admissionGate() == AdmissionGate.CLOSED || lane.admissionGate() == AdmissionGate.RETIRED) {
                return terminalizeNotPublishedEvidence(ledger, current, remaining, sourcePosition, result,
                        MessageStatus.DEAD_LETTER, StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED);
            }

            final RetryPolicySemanticV1 pinnedPolicy = retryPolicyFor(body.messageId(), current, sourcePosition);
            final int maxPublishAdmissions = pinnedPolicy == null
                    ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
            if (index.admissionsUsed() >= maxPublishAdmissions) {
                return terminalizeNotPublishedEvidence(ledger, current, remaining, sourcePosition, result,
                        MessageStatus.DEAD_LETTER, StableCode.DESTINATION_DEFINITIVE_PERMANENT);
            }
            final long retryAt = Math.max(Math.max(current.deliverAtEpochMs(),
                    current.retryEligibilityAtEpochMs()), sourcePosition.brokerPersistenceTimeEpochMs());
            if (retryAt >= current.expireAtEpochMs()) {
                return terminalizeNotPublishedEvidence(ledger, current, remaining, sourcePosition, result,
                        MessageStatus.EXPIRED, StableCode.ALREADY_EXPIRED);
            }
            return normalizeDefinitiveRetryAfterNotPublishedEvidence(ledger, current, remaining, retryAt,
                    sourcePosition, result);
        } catch (IllegalStateException | ArithmeticException exception) {
            return persistSystemResultByResult(result, sourcePosition, StableCode.STALE_SYSTEM_MUTATION);
        }
    }

    /** Removes only the named obligation while preserving still-uncertain work. */
    private SystemMutationResult settleNotPublishedUncertainObligation(final PublishAttemptLedger ledger,
                                                                        final MessageRecord current,
                                                                        final List<AttemptObligationRef> remaining,
                                                                        final SourcePosition sourcePosition,
                                                                        final SystemMutationResult result) {
        final GenerationRuntimeIndex index = current.runtimeIndex();
        final GenerationRuntimeIndex nextRuntime;
        final ClaimRecord revokedClaim;
        final MessageStatus nextStatus;
        if (index.currentWorkKind() == CurrentSendWorkKind.TIMELINE) {
            if (index.timeline() == null) {
                throw new IllegalStateException("uncertain timeline work is missing its reference");
            }
            nextRuntime = GenerationRuntimeIndex.timeline(GenerationAggregateState.UNCERTAIN,
                    index.timeline(), remaining, index.admissionsUsed(), index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(), Math.addExact(index.runtimeRevision(), 1));
            revokedClaim = null;
            nextStatus = current.status();
        } else if (index.currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            revokedClaim = findClaimForMessage(ledger.delayMessageId());
            if (revokedClaim == null || !Arrays.equals(revokedClaim.claimId(), index.claimId())
                    || revokedClaim.runtimeRevision() != current.stateVersion()) {
                throw new IllegalStateException("uncertain Claim work is missing its exact record");
            }
            // The Claim precondition freezes the old obligation-set digest.  Once
            // one old attempt is settled, retaining that Claim would make a later
            // Admission fail against an already-invalid digest.  Revoke it
            // atomically and leave the generation UNCERTAIN/NONE; a subsequent
            // source-ordered Resolve retry can materialize a fresh timeline.
            nextRuntime = GenerationRuntimeIndex.none(GenerationAggregateState.UNCERTAIN, remaining,
                    index.admissionsUsed(), index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(), Math.addExact(index.runtimeRevision(), 1));
            nextStatus = MessageStatus.UNCERTAIN;
        } else if (index.currentWorkKind() == CurrentSendWorkKind.PUBLISHING) {
            nextRuntime = GenerationRuntimeIndex.publishing(index.publishAttemptId(), remaining,
                    index.admissionsUsed(), index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(), Math.addExact(index.runtimeRevision(), 1));
            revokedClaim = null;
            nextStatus = current.status();
        } else if (index.currentWorkKind() == CurrentSendWorkKind.NONE) {
            nextRuntime = GenerationRuntimeIndex.none(GenerationAggregateState.UNCERTAIN, remaining,
                    index.admissionsUsed(), index.uncertainRetryAdmissionsUsed(),
                    index.possibleDestinationDuplicate(), Math.addExact(index.runtimeRevision(), 1));
            revokedClaim = null;
            nextStatus = current.status();
        } else {
            throw new IllegalStateException("unsupported uncertain work kind");
        }
        final long nextStateVersion = nextStatus == current.status()
                ? current.stateVersion() : Math.addExact(current.stateVersion(), 1);
        final MessageRecord next = new MessageRecord(nextStatus, current.generation(), nextStateVersion,
                current.deliverAtEpochMs(), current.expireAtEpochMs(), current.laneId(), current.orderingMode(),
                current.payload(), current.scheduleSourcePosition(), current.payloadReference(),
                current.retryEligibilityAtEpochMs()).withRuntimeIndex(nextRuntime);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            if (revokedClaim != null) {
                batch.delete(ColumnFamily.INFLIGHT, revokedClaim.encodedKey());
            }
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return result;
    }

    /** Keeps a different admitted send as the sole current work item. */
    private SystemMutationResult preservePublishingAfterNotPublishedEvidence(final PublishAttemptLedger ledger,
                                                                               final MessageRecord current,
                                                                               final List<AttemptObligationRef> remaining,
                                                                               final SourcePosition sourcePosition,
                                                                               final SystemMutationResult result) {
        final GenerationRuntimeIndex index = current.runtimeIndex();
        if (index.publishAttemptId().length == 0
                || Arrays.equals(index.publishAttemptId(), ledger.publishAttemptId())) {
            throw new IllegalStateException("not-published evidence cannot remove current publishing work");
        }
        final GenerationRuntimeIndex nextRuntime = GenerationRuntimeIndex.publishing(index.publishAttemptId(),
                remaining, index.admissionsUsed(), index.uncertainRetryAdmissionsUsed(),
                index.possibleDestinationDuplicate(), Math.addExact(index.runtimeRevision(), 1));
        final MessageRecord next = new MessageRecord(current.status(), current.generation(), current.stateVersion(),
                current.deliverAtEpochMs(), current.expireAtEpochMs(), current.laneId(), current.orderingMode(),
                current.payload(), current.scheduleSourcePosition(), current.payloadReference(),
                current.retryEligibilityAtEpochMs()).withRuntimeIndex(nextRuntime);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        store.write(batch -> {
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return result;
    }

    /** Converts reversible timeline/Claim/NONE work to a definitive retry. */
    private SystemMutationResult normalizeDefinitiveRetryAfterNotPublishedEvidence(
            final PublishAttemptLedger ledger, final MessageRecord current,
            final List<AttemptObligationRef> remaining, final long retryAt,
            final SourcePosition sourcePosition, final SystemMutationResult result) {
        final GenerationRuntimeIndex index = current.runtimeIndex();
        final byte[] priorTimelineKey;
        final byte[] claimKey;
        if (index.currentWorkKind() == CurrentSendWorkKind.TIMELINE) {
            if (index.timeline() == null
                    || !Arrays.equals(index.timeline().encodedTimelineKey(), timelineKey(ledger.delayMessageId(), current))) {
                throw new IllegalStateException("definitive retry timeline identity is stale");
            }
            priorTimelineKey = index.timeline().encodedTimelineKey();
            claimKey = null;
        } else if (index.currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            final ClaimRecord claim = findClaimForMessage(ledger.delayMessageId());
            if (claim == null || !Arrays.equals(claim.claimId(), index.claimId())
                    || claim.runtimeRevision() != current.stateVersion()
                    || !Arrays.equals(claim.timelineKey(), timelineKey(ledger.delayMessageId(), current))) {
                throw new IllegalStateException("definitive retry Claim identity is stale");
            }
            priorTimelineKey = claim.timelineKey();
            claimKey = claim.encodedKey();
        } else if (index.currentWorkKind() == CurrentSendWorkKind.NONE) {
            priorTimelineKey = null;
            claimKey = null;
        } else {
            throw new IllegalStateException("unsupported definitive retry work kind");
        }

        final int candidateAttemptNo = index.currentWorkKind() == CurrentSendWorkKind.TIMELINE
                ? index.timeline().candidateAttemptNo() : Math.addExact(index.admissionsUsed(), 1);
        final MessageRecord scheduled = new MessageRecord(MessageStatus.SCHEDULED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), retryAt);
        final MessageRecord scheduledForWrite = scheduled.withRuntimeIndex(timelineRuntimeIndex(
                ledger.delayMessageId(), scheduled, TimelineWorkKind.DEFINITIVE_RETRY, candidateAttemptNo,
                scheduled.stateVersion(), UncertainRetryAuthority.NONE, null, null, current.runtimeIndex(),
                remaining));
        final Map<DestinationLaneId, LaneProjection> projections = readyProjections(sourcePosition,
                ledger.delayMessageId(), current, scheduledForWrite, null);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        store.write(batch -> {
            if (priorTimelineKey != null) {
                batch.delete(ColumnFamily.TIMELINE, priorTimelineKey);
                batch.delete(ColumnFamily.TIMELINE, expiryKey(ledger.delayMessageId(), current));
            }
            if (claimKey != null) {
                batch.delete(ColumnFamily.INFLIGHT, claimKey);
            }
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()),
                    scheduledForWrite.encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, timelineKey(ledger.delayMessageId(), scheduledForWrite),
                    new TimelineEntry(ledger.delayMessageId(), scheduledForWrite.generation()).encode());
            batch.putValue(ColumnFamily.TIMELINE, 1, expiryKey(ledger.delayMessageId(), scheduledForWrite),
                    new TimelineEntry(ledger.delayMessageId(), scheduledForWrite.generation()).encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return result;
    }

    /** Terminalizes after definitive absence when no further admission is safe. */
    private SystemMutationResult terminalizeNotPublishedEvidence(final PublishAttemptLedger ledger,
                                                                  final MessageRecord current,
                                                                  final List<AttemptObligationRef> remaining,
                                                                  final SourcePosition sourcePosition,
                                                                  final SystemMutationResult originalResult,
                                                                  final MessageStatus terminalStatus,
                                                                  final StableCode terminalCode) {
        if (terminalStatus != MessageStatus.DEAD_LETTER && terminalStatus != MessageStatus.EXPIRED) {
            throw new IllegalArgumentException("unsupported definitive-absence terminal status");
        }
        final GenerationRuntimeIndex nextRuntime = GenerationRuntimeIndex.none(
                GenerationAggregateState.fromMessageStatus(terminalStatus), remaining,
                current.runtimeIndex().admissionsUsed(), current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                current.runtimeIndex().possibleDestinationDuplicate(), Math.addExact(current.stateVersion(), 1));
        final MessageRecord terminalMessage = new MessageRecord(terminalStatus, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs()).withRuntimeIndex(nextRuntime);
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(ledger.delayMessageId(),
                ledger.generation(), terminalStatus, terminalCode, terminalMessage.stateVersion(),
                sourcePosition.canonicalBytes(), terminalMessage.runtimeIndex().possibleDestinationDuplicate(),
                remaining);
        final DlqExportRecord dlqExport = terminalStatus == MessageStatus.DEAD_LETTER
                ? DlqExportRecord.notConfigured(ledger.delayMessageId(), ledger.generation(),
                terminalMessage.stateVersion(), sourcePosition.canonicalBytes()) : null;
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
        final byte[] currentWorkKey;
        final byte[] claimKey;
        switch (current.runtimeIndex().currentWorkKind()) {
            case TIMELINE -> {
                if (current.runtimeIndex().timeline() == null) {
                    throw new IllegalStateException("terminalized timeline work is missing its reference");
                }
                currentWorkKey = current.runtimeIndex().timeline().encodedTimelineKey();
                claimKey = null;
            }
            case CLAIMED -> {
                final ClaimRecord claim = findClaimForMessage(ledger.delayMessageId());
                if (claim == null || !Arrays.equals(claim.claimId(), current.runtimeIndex().claimId())
                        || claim.runtimeRevision() != current.stateVersion()) {
                    throw new IllegalStateException("terminalized Claim work is missing its exact record");
                }
                currentWorkKey = claim.timelineKey();
                claimKey = claim.encodedKey();
            }
            case NONE -> {
                currentWorkKey = null;
                claimKey = null;
            }
            case PUBLISHING -> throw new IllegalStateException("terminalization cannot remove current publishing");
            default -> throw new IllegalStateException("unsupported terminalized work kind");
        }
        final Map<DestinationLaneId, LaneProjection> projections = readyProjections(sourcePosition,
                ledger.delayMessageId(), current, terminalMessage, null);
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        final SystemMutationResult result = terminalStatus == MessageStatus.DEAD_LETTER
                ? new SystemMutationResult(originalResult.mutationId(), originalResult.mutationHash(),
                originalResult.mutationType(), originalResult.retryUntilEpochMs(), originalResult.authorIdentity(),
                originalResult.applyStatus(), terminalCode, sourcePosition.canonicalBytes()) : originalResult;
        store.write(batch -> {
            if (currentWorkKey != null) {
                batch.delete(ColumnFamily.TIMELINE, currentWorkKey);
                batch.delete(ColumnFamily.TIMELINE, expiryKey(ledger.delayMessageId(), current));
            }
            if (claimKey != null) {
                batch.delete(ColumnFamily.INFLIGHT, claimKey);
            }
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), terminalMessage.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), terminal.encode());
            if (dlqExport != null) {
                batch.putValue(ColumnFamily.TERMINAL, DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(dlqExport.dlqExportId()), dlqExport.encode());
            }
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            writeSystemResult(batch, result);
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        quota = nextQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
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
                                                                       final ClaimRecord localClaim,
                                                                       final SourcePosition sourcePosition) {
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
        validateAdmissionBudget(new DelayMessageId(body.messageId()), current, index, uncertainRetry,
                sourcePosition);
        return new AdmissionReplayState(localClaim == null, uncertainRetry);
    }

    private void validateAdmissionBudget(final DelayMessageId messageId, final MessageRecord current,
                                         final GenerationRuntimeIndex index, final boolean uncertainRetryAdmission,
                                         final SourcePosition sourcePosition) {
        final RetryPolicySemanticV1 policy = retryPolicyFor(messageId, current, sourcePosition);
        final int maxPublishAdmissions = policy == null
                ? config.maxPublishAdmissions() : policy.maxPublishAdmissions();
        final int maxUncertainRetries = policy == null
                ? config.maxUncertainRetries() : policy.maxUncertainRetries();
        if (index.admissionsUsed() >= maxPublishAdmissions) {
            throw new IllegalStateException("generation publish admission budget is exhausted");
        }
        if (uncertainRetryAdmission
                && index.uncertainRetryAdmissionsUsed() >= maxUncertainRetries) {
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
        final LaneRecord currentLane = readLane(current.laneId());
        final boolean closedAfterAdmission = currentLane != null
                && currentLane.admissionGate() == AdmissionGate.CLOSED;
        final StableCode terminalCode = closedAfterAdmission
                ? StableCode.LANE_CLOSED_AFTER_ADMISSION_NOT_PUBLISHED : outcome.stableCode();
        final SystemMutationResult terminalResult = closedAfterAdmission
                ? new SystemMutationResult(systemResult.mutationId(), systemResult.mutationHash(),
                systemResult.mutationType(), systemResult.retryUntilEpochMs(), systemResult.authorIdentity(),
                systemResult.applyStatus(), terminalCode, sourcePosition.canonicalBytes())
                : systemResult;
        if (outcome.disposition() == 2 || closedAfterAdmission) {
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
                    ledger.generation(), MessageStatus.DEAD_LETTER, terminalCode, terminalMessage.stateVersion(),
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
                writeSystemResult(batch, terminalResult);
                writePosition(batch, sourcePosition);
            });
            lastAppliedSourcePosition = sourcePosition;
            mutationSequence++;
            quota = nextQuota;
            outcomeReserve = nextOutcomeReserve;
            outcomeReserveVector = nextOutcomeReserveVector;
            return terminalResult;
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
        SystemMutationBodyCodec.requireMessageShard(fields, messageId, "Expire Generation");
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
        final LaneRecord lane = readLane(current.laneId());
        if (lane != null && lane.admissionGate() == AdmissionGate.CLOSED
                && isUnadmittedGeneration(current)) {
            return persistSystemResult(mutation, sourcePosition, ApplyStatus.APPLIED,
                    StableCode.LANE_CLOSED_BEFORE_ADMISSION);
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
                nextMutationSequence(),
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
                nextMutationSequence(), body.evidence().providerRequestIdHash(),
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

    private ResourceRetireIntentRecord validateGcIntentIdentity(
            final ResourceRetireIntentRecord intent, final ResourceKind resourceKind,
            final byte[] resourceIdentityHash, final long expectedVersion) {
        if (intent.resourceKind() != resourceKind
                || !Bytes.constantTimeEquals(intent.resourceIdentityHash(), resourceIdentityHash)
                || intent.expectedResourceStateVersion() != expectedVersion) {
            throw new IllegalStateException("GC retire intent key/value identity mismatch");
        }
        validateSourcePositionShard(intent.appliedSourcePosition(), "GC retire intent lookup");
        return intent;
    }

    private ResourceDeleteConfirmedRecord validateGcConfirmationIdentity(
            final ResourceDeleteConfirmedRecord confirmation, final ResourceKind resourceKind,
            final byte[] resourceIdentityHash, final long expectedVersion) {
        validateGcIntentIdentity(confirmation.retireIntent(), resourceKind, resourceIdentityHash, expectedVersion);
        validateSourcePositionShard(confirmation.appliedSourcePosition(), "GC delete confirmation lookup");
        return confirmation;
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
        Objects.requireNonNull(messageId, "messageId");
        requireMessageShard(messageId, "terminal generation lookup");
        final var value = store.getValue(ColumnFamily.TERMINAL, KeyCodec.terminalGeneration(messageId, generation), 1);
        if (value == null) {
            return null;
        }
        final TerminalGenerationRecord terminal = TerminalGenerationRecord.decode(value.payload());
        if (!terminal.messageId().equals(messageId) || terminal.generation() != generation) {
            throw new IllegalStateException("terminal generation key/value identity mismatch");
        }
        if (!store.shardId().equals(messageId.routingId().shardId())) {
            throw new IllegalStateException("terminal generation key shard mismatch");
        }
        validateSourcePositionShard(terminal.appliedSourcePosition(), "terminal generation lookup");
        return terminal;
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
     * Returns the bounded set of all live PUBLISHING/UNCERTAIN ledgers.
     * Drain and recovery callers can use this as a callback-quiescence view;
     * the method never guesses when the persisted inflight set exceeds the
     * configured shard bound.
     */
    public synchronized List<PublishAttemptLedger> listOpenPublishAttempts() {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.INFLIGHT,
                new byte[]{INFLIGHT_PUBLISHING_KIND, 1}, new byte[]{4, 1}, limit);
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("open publish attempt scan exceeded configured bound");
        }
        final Set<String> identities = new HashSet<>();
        final List<PublishAttemptLedger> result = new ArrayList<>();
        for (var entry : entries) {
            final PublishAttemptLedger ledger = decodePublishAttempt(entry);
            if (!identities.add(Bytes.hex(ledger.publishAttemptId()))) {
                throw new IllegalStateException("publish attempt ID has multiple live ledgers");
            }
            result.add(ledger);
        }
        return List.copyOf(result);
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
        validateAdmissionBudget(admission.delayMessageId(), current, current.runtimeIndex(), uncertainRetryAdmission,
                sourcePosition);
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
        final LaneRecord currentLane = readLane(current.laneId());
        final boolean scheduleUncertainRetry = retryDecision != null && retryDecision.kind() == 2
                && (currentLane == null
                || (currentLane.admissionGate() != AdmissionGate.CLOSED
                && currentLane.admissionGate() != AdmissionGate.RETIRED));
        final long retryAt;
        if (scheduleUncertainRetry) {
            final RetryPolicySemanticV1 pinnedPolicy = retryPolicyFor(currentLedger.delayMessageId(), current,
                    sourcePosition);
            final int maxUncertainRetries = pinnedPolicy == null
                    ? config.maxUncertainRetries() : pinnedPolicy.maxUncertainRetries();
            final int maxPublishAdmissions = pinnedPolicy == null
                    ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
            if (current.orderingMode() != io.nereusstream.delay.protocol.OrderingMode.BEST_EFFORT
                    || !retryDecision.hasNextRetryAt()
                    || maxUncertainRetries == 0
                    || current.runtimeIndex().uncertainRetryAdmissionsUsed() >= maxUncertainRetries
                    || current.runtimeIndex().admissionsUsed() >= maxPublishAdmissions) {
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
        if (expectedMessageStatus == MessageStatus.UNCERTAIN
                && ledger.state() == AttemptLedgerState.UNCERTAIN
                && current.runtimeIndex().aggregateState() == GenerationAggregateState.UNCERTAIN
                && current.runtimeIndex().attemptObligations().contains(ledger.obligationRef())) {
            return settleVerifiedPublishedUncertainGeneration(ledger, current, sourcePosition, systemResult);
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
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
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
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        quota = nextQuota;
        outcomeReserve = nextOutcomeReserve;
        outcomeReserveVector = nextOutcomeReserveVector;
        return next;
    }

    /**
     * Settles verified success while another reversible work item is still
     * present for the same generation.  A late success proves that the
     * generation must not be retried: timeline/Claim work is removed and the
     * generation becomes terminal, while a different current PUBLISHING
     * attempt remains the sole current work and keeps the generation open.
     */
    private MessageRecord settleVerifiedPublishedUncertainGeneration(final PublishAttemptLedger ledger,
                                                                      final MessageRecord current,
                                                                      final SourcePosition sourcePosition,
                                                                      final SystemMutationResult systemResult) {
        final List<AttemptObligationRef> remaining = withoutObligation(current.runtimeIndex(),
                ledger.publishAttemptId());
        final boolean duplicate = true;
        final OutcomeReserveUsage nextOutcomeReserve = releasedOutcomeReserve(ledger);
        final CapacityVectorV1 nextOutcomeReserveVector = releasedOutcomeReserveVector(ledger);
        if (current.runtimeIndex().currentWorkKind() == CurrentSendWorkKind.PUBLISHING) {
            final byte[] currentAttemptId = current.runtimeIndex().publishAttemptId();
            if (currentAttemptId.length == 0 || Arrays.equals(currentAttemptId, ledger.publishAttemptId())) {
                throw new IllegalStateException("uncertain success cannot remove current publishing work");
            }
            final GenerationRuntimeIndex nextRuntime = GenerationRuntimeIndex.publishing(currentAttemptId,
                    remaining, current.runtimeIndex().admissionsUsed(),
                    current.runtimeIndex().uncertainRetryAdmissionsUsed(), duplicate,
                    Math.addExact(current.runtimeIndex().runtimeRevision(), 1));
            final MessageRecord next = new MessageRecord(current.status(), current.generation(),
                    current.stateVersion(), current.deliverAtEpochMs(), current.expireAtEpochMs(), current.laneId(),
                    current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                    current.payloadReference(), current.retryEligibilityAtEpochMs()).withRuntimeIndex(nextRuntime);
            store.write(batch -> {
                batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
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

        final byte[] currentWorkKey;
        final byte[] claimKey;
        if (current.runtimeIndex().currentWorkKind() == CurrentSendWorkKind.TIMELINE) {
            final TimelineWorkRef timeline = current.runtimeIndex().timeline();
            if (timeline == null) {
                throw new IllegalStateException("uncertain timeline work is missing its reference");
            }
            currentWorkKey = timeline.encodedTimelineKey();
            claimKey = null;
        } else if (current.runtimeIndex().currentWorkKind() == CurrentSendWorkKind.CLAIMED) {
            final ClaimRecord claim = findClaimForMessage(ledger.delayMessageId());
            if (claim == null || !Arrays.equals(claim.claimId(), current.runtimeIndex().claimId())) {
                throw new IllegalStateException("uncertain Claim work is missing its exact record");
            }
            currentWorkKey = claim.timelineKey();
            claimKey = claim.encodedKey();
        } else if (current.runtimeIndex().currentWorkKind() == CurrentSendWorkKind.NONE) {
            currentWorkKey = null;
            claimKey = null;
        } else {
            throw new IllegalStateException("unsupported current work for uncertain success");
        }

        final MessageRecord next = new MessageRecord(MessageStatus.PUBLISHED, current.generation(),
                Math.addExact(current.stateVersion(), 1), current.deliverAtEpochMs(), current.expireAtEpochMs(),
                current.laneId(), current.orderingMode(), current.payload(), current.scheduleSourcePosition(),
                current.payloadReference(), current.retryEligibilityAtEpochMs()).withRuntimeIndex(
                GenerationRuntimeIndex.none(GenerationAggregateState.PUBLISHED, remaining,
                        current.runtimeIndex().admissionsUsed(), current.runtimeIndex().uncertainRetryAdmissionsUsed(),
                        duplicate, Math.addExact(current.stateVersion(), 1)));
        final TerminalGenerationRecord terminal = new TerminalGenerationRecord(ledger.delayMessageId(),
                ledger.generation(), MessageStatus.PUBLISHED, StableCode.OK, next.stateVersion(),
                sourcePosition.canonicalBytes(), duplicate, remaining);
        final ShardQuota nextQuota = quota.removeSchedule(current.payloadLength());
        final Map<io.nereusstream.delay.protocol.DestinationLaneId, LaneProjection> projections = readyProjections(
                sourcePosition, ledger.delayMessageId(), current, next, null);
        store.write(batch -> {
            if (currentWorkKey != null) {
                batch.delete(ColumnFamily.TIMELINE, currentWorkKey);
                batch.delete(ColumnFamily.TIMELINE, expiryKey(ledger.delayMessageId(), current));
            }
            if (claimKey != null) {
                batch.delete(ColumnFamily.INFLIGHT, claimKey);
            }
            batch.delete(ColumnFamily.INFLIGHT, ledger.encodedKey());
            batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(ledger.delayMessageId()), next.encode());
            batch.putValue(ColumnFamily.TERMINAL, 1,
                    KeyCodec.terminalGeneration(ledger.delayMessageId(), ledger.generation()), terminal.encode());
            for (LaneProjection projection : projections.values()) {
                deleteReadyKey(batch, projection.previousLane());
                putReadyProjection(batch, projection);
            }
            batch.putValue(ColumnFamily.META, 7, KeyCodec.metaQuota(META_QUOTA_USAGE), nextQuota.encode());
            persistOutcomeReserve(batch, nextOutcomeReserve, nextOutcomeReserveVector);
            if (systemResult != null) {
                writeSystemResult(batch, systemResult);
            }
            writePosition(batch, sourcePosition);
        });
        lastAppliedSourcePosition = sourcePosition;
        mutationSequence++;
        quota = nextQuota;
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

    /** Returns the durable local close cursor, if this Lane has not finished materialization. */
    public synchronized LaneCloseMaterializationCursor getLaneCloseCursor(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId) {
        Objects.requireNonNull(laneId, "laneId");
        final LaneRecord lane = readLane(laneId);
        if (lane == null || lane.admissionGate() != AdmissionGate.CLOSED) {
            return null;
        }
        final var value = store.getValue(ColumnFamily.TIMELINE,
                closeCursorKey(laneId, lane.laneControlVersion()), LaneCloseMaterializationCursor.VALUE_TYPE);
        if (value == null) {
            return null;
        }
        final LaneCloseMaterializationCursor cursor = LaneCloseMaterializationCursor.decode(value.payload());
        if (!cursor.laneId().equals(laneId) || cursor.closeVersion() != lane.laneControlVersion()
                || !Arrays.equals(cursor.laneIncarnation(), lane.laneIncarnation())
                || !store.shardId().equals(SourcePositionCodec.decode(cursor.closeSourcePosition()).shardId())) {
            throw new IllegalStateException("Lane close cursor key/value identity mismatch");
        }
        return cursor;
    }

    /**
     * Discovers the durable close-materialization work queue. Close cursors
     * live in the System timeline namespace rather than the due-publish
     * namespaces, so a normal due scan must never silently consume them.
     * Every returned entry is checked against the cursor value and the
     * current closed Lane before it becomes scheduler input.
     */
    public synchronized List<LaneCloseMaterializationWork> discoverLaneCloseMaterialization(final int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        final int scanLimit = limit == Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.addExact(limit, 1);
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.TIMELINE,
                new byte[]{6, 1, LaneCloseMaterializationCursor.SYSTEM_WORK_KIND},
                new byte[]{6, 1, (byte) (LaneCloseMaterializationCursor.SYSTEM_WORK_KIND + 1)}, scanLimit);
        if (entries.size() > limit) {
            throw new IllegalStateException("Lane close materialization work exceeds scheduler bound");
        }
        final List<LaneCloseMaterializationWork> result = new ArrayList<>(entries.size());
        for (var entry : entries) {
            final SystemTimelineKey key = decodeLaneCloseWorkKey(entry.key());
            final LaneCloseMaterializationCursor cursor = LaneCloseMaterializationCursor.decode(
                    ValueEnvelope.decode(entry.value(), LaneCloseMaterializationCursor.VALUE_TYPE).payload());
            final DestinationLaneId laneId = new DestinationLaneId(key.workId());
            if (!cursor.laneId().equals(laneId) || cursor.closeVersion() != key.workVersion()
                    || key.nextEligibleAtEpochMs() != 0) {
                throw new IllegalStateException("Lane close system work key/value identity mismatch");
            }
            validateSourcePositionShard(cursor.closeSourcePosition(), "Lane close materialization discovery");
            final LaneRecord lane = readLane(laneId);
            if (lane == null || lane.admissionGate() != AdmissionGate.CLOSED
                    || lane.laneControlVersion() != cursor.closeVersion()
                    || !Arrays.equals(lane.laneIncarnation(), cursor.laneIncarnation())) {
                throw new IllegalStateException("Lane close system work points to a non-closed Lane");
            }
            result.add(new LaneCloseMaterializationWork(laneId, cursor.closeVersion(), key.nextEligibleAtEpochMs(),
                    cursor));
        }
        return List.copyOf(result);
    }

    /**
     * Applies one bounded, quota-neutral close-materialization batch. The
     * source-ordered marker is the semantic boundary; this method only resumes
     * the persisted cursor and never reclassifies an admitted obligation.
     */
    public synchronized LaneCloseMaterializationResult materializeClosedLane(
            final io.nereusstream.delay.protocol.DestinationLaneId laneId, final int maxRecords) {
        Objects.requireNonNull(laneId, "laneId");
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("maxRecords must be positive");
        }
        final LaneCloseMaterializationCursor cursor = getLaneCloseCursor(laneId);
        if (cursor == null) {
            return new LaneCloseMaterializationResult(laneId, 0, 0, 0, 0, true);
        }
        if (!cursor.laneId().equals(laneId)) {
            throw new IllegalStateException("Lane close cursor identity mismatch");
        }
        final LaneRecord lane = readLane(laneId);
        if (lane == null || lane.admissionGate() != AdmissionGate.CLOSED
                || lane.laneControlVersion() != cursor.closeVersion()
                || !Arrays.equals(lane.laneIncarnation(), cursor.laneIncarnation())) {
            throw new IllegalStateException("Lane close cursor does not match the closed Lane");
        }
        final SourcePosition closePosition = SourcePositionCodec.decode(cursor.closeSourcePosition());
        if (!store.shardId().equals(closePosition.shardId())) {
            throw new IllegalStateException("Lane close cursor source position belongs to another shard");
        }
        final int bound = Math.min(maxRecords, boundedLimit(config.maxPendingMessages()));
        final CursorScan scan = cursor.phase() == LaneCloseMaterializationCursor.Phase.MESSAGES
                ? scanAfter(ColumnFamily.ID, new byte[]{1, 1}, new byte[]{2, 1}, cursor.lastKey(), bound)
                : scanAfter(ColumnFamily.ID, new byte[]{2, 1}, new byte[]{3, 1}, cursor.lastKey(), bound);
        final List<ClosedMessageAction> messageActions = cursor.phase()
                == LaneCloseMaterializationCursor.Phase.MESSAGES
                ? prepareClosedMessageActions(cursor, scan.entries(), closePosition) : List.of();
        final List<ClosedReservationAction> reservationActions = cursor.phase()
                == LaneCloseMaterializationCursor.Phase.RESERVATIONS
                ? prepareClosedReservationActions(cursor, scan.entries(), closePosition) : List.of();
        final LaneCloseMaterializationCursor nextCursor;
        if (scan.hasMore()) {
            nextCursor = cursor.advance(scan.entries().get(scan.entries().size() - 1).key());
        } else if (cursor.phase() == LaneCloseMaterializationCursor.Phase.MESSAGES) {
            nextCursor = cursor.nextPhase();
        } else {
            nextCursor = null;
        }
        store.write(batch -> {
            for (ClosedMessageAction action : messageActions) {
                batch.delete(ColumnFamily.TIMELINE, action.timelineKey());
                batch.delete(ColumnFamily.TIMELINE, action.expiryKey());
                if (action.claim() != null) {
                    batch.delete(ColumnFamily.INFLIGHT, action.claim().encodedKey());
                }
                batch.putValue(ColumnFamily.ID, 1, KeyCodec.idMessage(action.messageId()),
                        action.terminalMessage().encode());
                batch.putValue(ColumnFamily.TERMINAL, 1,
                        KeyCodec.terminalGeneration(action.messageId(), action.terminalMessage().generation()),
                        action.terminal().encode());
                final DlqExportRecord dlqExport = DlqExportRecord.notConfigured(action.messageId(),
                        action.terminalMessage().generation(), action.terminalMessage().stateVersion(),
                        action.terminal().appliedSourcePosition());
                batch.putValue(ColumnFamily.TERMINAL, DlqExportRecord.VALUE_TYPE,
                        KeyCodec.terminalDlqExport(dlqExport.dlqExportId()), dlqExport.encode());
            }
            for (ClosedReservationAction action : reservationActions) {
                batch.putValue(ColumnFamily.ID, 2, KeyCodec.idReservation(action.reservation().reservationId()),
                        action.closedReservation().encode());
                batch.delete(ColumnFamily.TIMELINE, KeyCodec.reservationExpiry(
                        action.reservation().reservationExpiryEpochMs(), action.reservation().reservationId()));
            }
            if (nextCursor == null) {
                batch.delete(ColumnFamily.TIMELINE, closeCursorKey(cursor));
            } else {
                batch.putValue(ColumnFamily.TIMELINE, LaneCloseMaterializationCursor.VALUE_TYPE,
                        closeCursorKey(nextCursor), nextCursor.canonicalBytes());
                if (!Arrays.equals(closeCursorKey(cursor), closeCursorKey(nextCursor))) {
                    batch.delete(ColumnFamily.TIMELINE, closeCursorKey(cursor));
                }
            }
        });
        return new LaneCloseMaterializationResult(laneId, cursor.closeVersion(), scan.entries().size(),
                messageActions.size(), reservationActions.size(), nextCursor == null);
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
                || !isAtOrBeforeExact(progress.intentSourcePosition(), lastAppliedSourcePosition)
                || !isAtOrAfterExact(guard.terminalSourcePosition(), progress.intentSourcePosition())) {
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

    private static boolean isAtOrBeforeExact(final SourcePosition candidate, final SourcePosition upperBound) {
        final int order = candidate.compareTo(upperBound);
        return order < 0 || (order == 0
                && Arrays.equals(candidate.canonicalBytes(), upperBound.canonicalBytes()));
    }

    private static boolean isAtOrAfterExact(final SourcePosition candidate, final SourcePosition lowerBound) {
        final int order = candidate.compareTo(lowerBound);
        return order > 0 || (order == 0
                && Arrays.equals(candidate.canonicalBytes(), lowerBound.canonicalBytes()));
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

    /** Returns the persisted usage of a control reserve class (3-6). */
    public synchronized CapacityVectorV1 controlReserveUsage(final int reserveClass) {
        validateMutableControlReserveClass(reserveClass);
        return controlReserveUsage.getOrDefault(reserveClass, CapacityVectorV1.empty());
    }

    /** Returns the local class-6 projection for the Route Broker system writer. */
    public synchronized CapacityVectorV1 systemWriterReserveUsage() {
        return controlReserveUsage(CONTROL_RESERVE_SYSTEM_WRITER_CLASS);
    }

    /**
     * Charges one checked class-3/4/5/6 control reserve projection and
     * persists it synchronously. Class 6 is restricted to the three
     * system-writer dimensions and shares the non-outcome grant without
     * overlapping class 3. This is a local accounting primitive; the source
     * ordered control mutation, Broker writer quota and Oxia placement
     * authority remain callers' responsibilities.
     */
    public synchronized CapacityVectorV1 reserveControlCapacity(final int reserveClass,
                                                                  final CapacityVectorV1 amount) {
        return mutateControlReserve(reserveClass, Objects.requireNonNull(amount, "amount"), true);
    }

    /** Charges the local class-6 system-writer projection. */
    public synchronized CapacityVectorV1 reserveSystemWriterCapacity(final CapacityVectorV1 amount) {
        return reserveControlCapacity(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, amount);
    }

    /** Releases an exact checked class-3/4/5/6 control reserve projection. */
    public synchronized CapacityVectorV1 releaseControlCapacity(final int reserveClass,
                                                                  final CapacityVectorV1 amount) {
        return mutateControlReserve(reserveClass, Objects.requireNonNull(amount, "amount"), false);
    }

    /** Releases an exact local class-6 system-writer projection. */
    public synchronized CapacityVectorV1 releaseSystemWriterCapacity(final CapacityVectorV1 amount) {
        return releaseControlCapacity(CONTROL_RESERVE_SYSTEM_WRITER_CLASS, amount);
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
            final var timelineValue = store.getValue(ColumnFamily.TIMELINE, timelineKey, 1);
            if (timelineValue == null) {
                throw new IllegalStateException("READY points to a missing timeline entry");
            }
            final TimelineEntry timeline = TimelineEntry.decode(timelineValue.payload());
            if (!timeline.messageId().equals(value.messageId()) || timeline.generation() != value.generation()) {
                throw new IllegalStateException("READY timeline identity mismatch");
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
            final DelayMessageId messageId = new DelayMessageId(messageBytes);
            if (!value.messageId().equals(messageId) || value.generation() != generation) {
                throw new IllegalStateException("EXPIRY key/value identity mismatch");
            }
            final io.nereusstream.delay.protocol.DestinationLaneId laneId =
                    new io.nereusstream.delay.protocol.DestinationLaneId(laneBytes);
            final MessageRecord message = getMessage(messageId);
            if (message == null || (message.status() != MessageStatus.SCHEDULED
                    && message.status() != MessageStatus.CLAIMED) || message.generation() != generation
                    || message.expireAtEpochMs() != expireAt || !message.laneId().equals(laneId)
                    || !Arrays.equals(entry.key(), expiryKey(messageId, message))) {
                throw new IllegalStateException("EXPIRY timeline does not match the current message");
            }
            result.add(new ExpiryWork(messageId, laneId, generation, expireAt));
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
            final PayloadReservation current = readStoredReservation(reservationId);
            if (current == null || !Arrays.equals(current.encode(), reservation.encode())) {
                throw new IllegalStateException("RESERVATION_EXPIRY is not the current reservation projection");
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
        requireProfileFirstBinding(body.intentWithoutPayload().profile(), sourcePosition);
        requireRetryPolicy(body.intentWithoutPayload().retryPolicy(), body.intentWithoutPayload().orderingMode(),
                sourcePosition);
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
        requireProfileFirstBinding(body.intent().profile(), sourcePosition);
        requireRetryPolicy(body.intent().retryPolicy(), body.intent().orderingMode(), sourcePosition);
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

    private void requireRetryPolicy(final RetryPolicyRefV1 reference,
                                    final io.nereusstream.delay.protocol.OrderingMode orderingMode,
                                    final SourcePosition sourcePosition) {
        if (retryPolicyCatalog == null) {
            return;
        }
        final RetryPolicySemanticV1 semantic = retryPolicyCatalog.resolve(
                Objects.requireNonNull(reference, "reference"), Objects.requireNonNull(sourcePosition,
                        "sourcePosition"));
        if (semantic == null) {
            throw new V1CommandResolutionException(StableCode.RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION,
                    "Retry Policy is not active at the command Source Position");
        }
        if (!reference.matches(semantic)) {
            throw new V1CommandResolutionException(StableCode.INVALID_COMMAND,
                    "Retry Policy reference does not match catalog semantic bytes");
        }
        try {
            semantic.validateFor(orderingMode);
        } catch (IllegalArgumentException exception) {
            throw new V1CommandResolutionException(StableCode.INVALID_COMMAND,
                    "Retry Policy is incompatible with the requested ordering mode");
        }
        ensureRetryPolicyFitsConfig(semantic);
    }

    private void ensureRetryPolicyFitsConfig(final RetryPolicySemanticV1 semantic) {
        if (semantic.maxPublishAdmissions() > config.maxPublishAdmissions()
                || semantic.maxUncertainRetries() > config.maxUncertainRetries()) {
            throw new V1CommandResolutionException(StableCode.INVALID_COMMAND,
                    "local shard limits cannot honor the immutable Retry Policy budget");
        }
    }

    /** Resolves the policy pinned by an accepted V1 binding for later replay turns. */
    private RetryPolicySemanticV1 retryPolicyFor(final DelayMessageId messageId,
                                                 final MessageRecord message,
                                                 final SourcePosition sourcePosition) {
        if (retryPolicyCatalog == null) {
            return null;
        }
        final V1ScheduleBinding binding = getV1ScheduleBinding(messageId);
        if (binding == null) {
            return null;
        }
        final RetryPolicyRefV1 reference;
        if (binding.commandType() == io.nereusstream.delay.protocol.CommandType.SCHEDULE) {
            reference = CommandBodies.decodeScheduleV1(binding.canonicalBody()).intent().retryPolicy();
        } else {
            reference = CommandBodies.decodePrepareLargeV1(binding.canonicalBody())
                    .intentWithoutPayload().retryPolicy();
        }
        final RetryPolicySemanticV1 semantic = retryPolicyCatalog.resolve(reference, sourcePosition);
        if (semantic == null || !reference.matches(semantic)) {
            throw new V1CommandResolutionException(StableCode.RETRY_POLICY_NOT_ACTIVE_AT_SOURCE_POSITION,
                    "pinned Retry Policy is unavailable or mismatched for replay");
        }
        try {
            semantic.validateFor(message.orderingMode());
        } catch (IllegalArgumentException exception) {
            throw new V1CommandResolutionException(StableCode.INVALID_COMMAND,
                    "pinned Retry Policy no longer matches message ordering");
        }
        ensureRetryPolicyFitsConfig(semantic);
        return semantic;
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
        final PayloadReservation storedReservation = readStoredReservation(proof.reservationId());
        if (storedReservation == null || !storedReservation.delayMessageId().equals(command.delayMessageId())) {
            return persistRejected(command, sourcePosition, StableCode.RESERVATION_NOT_COMMITTED);
        }
        final LaneRecord reservationLane = readLane(storedReservation.intent().laneId());
        if (reservationLane != null && reservationLane.admissionGate() == AdmissionGate.CLOSED
                && storedReservation.status() == PayloadReservationStatus.RESERVED) {
            return persistRejected(command, sourcePosition, StableCode.PAYLOAD_RESERVATION_CLOSED);
        }
        final PayloadReservation reservation = effectiveReservation(storedReservation);
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
            final io.nereusstream.delay.protocol.DestinationLaneId laneId =
                    new io.nereusstream.delay.protocol.DestinationLaneId(laneBytes);
            final MessageRecord message = getMessage(messageId);
            if (message == null || message.status() != MessageStatus.SCHEDULED
                    || message.generation() != generation || !message.laneId().equals(laneId)
                    || !Arrays.equals(entry.key(), timelineKey(messageId, message))) {
                throw new IllegalStateException("DUE timeline does not match the current scheduled message");
            }
            result.add(new TimelineWork(messageId, laneId, generation, eligibleAt, tag == 2));
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
                final LaneRecord reservationLane = readLane(reservation.intent().laneId());
                if (reservationLane != null && reservationLane.admissionGate() == AdmissionGate.CLOSED
                        && reservation.status() == PayloadReservationStatus.RESERVED) {
                    return applied(StableCode.PAYLOAD_RESERVATION_CLOSED, sourcePosition, null);
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
        final LaneRecord lane = readLane(existing.laneId());
        if (lane != null && lane.admissionGate() == AdmissionGate.CLOSED
                && isUnadmittedGeneration(existing)) {
            return applied(StableCode.ALREADY_DEAD_LETTERED, sourcePosition, existing);
        }
        if ((existing.status() == MessageStatus.SCHEDULED || existing.status() == MessageStatus.CLAIMED)
                && hasUncertainObligation(existing.runtimeIndex())) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        if (existing.status() == MessageStatus.DEAD_LETTER) {
            final TerminalGenerationRecord terminal = getTerminalGeneration(command.delayMessageId(),
                    existing.generation());
            if (terminal != null && terminal.terminalCode() == StableCode.LANE_CLOSED_BEFORE_ADMISSION) {
                return applied(StableCode.ALREADY_DEAD_LETTERED, sourcePosition, existing);
            }
        }
        return switch (existing.status()) {
            case SCHEDULED, CLAIMED -> applied(StableCode.CANCELED, sourcePosition,
                    new MessageRecord(MessageStatus.CANCELED, existing.generation(),
                            Math.incrementExact(existing.stateVersion()),
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
        final LaneRecord lane = readLane(existing.laneId());
        if (lane != null && lane.admissionGate() == AdmissionGate.CLOSED
                && isUnadmittedGeneration(existing)) {
            return applied(StableCode.LANE_CLOSED, sourcePosition, existing);
        }
        if ((existing.status() == MessageStatus.SCHEDULED || existing.status() == MessageStatus.CLAIMED)
                && hasUncertainObligation(existing.runtimeIndex())) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        if (existing.status() == MessageStatus.DEAD_LETTER) {
            final TerminalGenerationRecord terminal = getTerminalGeneration(command.delayMessageId(),
                    existing.generation());
            if (terminal != null && terminal.terminalCode() == StableCode.LANE_CLOSED_BEFORE_ADMISSION) {
                return applied(StableCode.LANE_CLOSED, sourcePosition, existing);
            }
        }
        if (existing.status() != MessageStatus.SCHEDULED && existing.status() != MessageStatus.CLAIMED) {
            return applied(StableCode.TOO_LATE, sourcePosition, existing);
        }
        validateWindow(request.deliverAtEpochMs(), request.expireAtEpochMs(),
                sourcePosition.brokerPersistenceTimeEpochMs());
        final MessageRecord replacement = new MessageRecord(MessageStatus.SCHEDULED,
                Math.incrementExact(existing.generation()), Math.incrementExact(existing.stateVersion()),
                request.deliverAtEpochMs(), request.expireAtEpochMs(), existing.laneId(),
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
        final int candidateLimit = boundedLimitPlusOne(config.maxPendingMessages());
        for (byte tag = 1; tag <= 2; tag++) {
            final byte[] prefix = Bytes.concat(new byte[]{tag, 1}, laneId.bytes());
            final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.TIMELINE,
                    prefix, prefixUpperBound(prefix), candidateLimit);
            if (entries.size() >= candidateLimit
                    && config.maxPendingMessages() < Integer.MAX_VALUE) {
                throw new IllegalStateException("timeline candidate scan exceeded configured bound");
            }
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
        if (!Arrays.equals(key, timelineKey(messageId, message))) {
            throw new IllegalStateException("timeline key does not match the current scheduled message");
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
                    ? new MessageRecord(MessageStatus.CANCELED, prior.generation(),
                    Math.incrementExact(prior.stateVersion()),
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
                result.stableCode(), next.stateVersion(), position.canonicalBytes(),
                prior.runtimeIndex().possibleDestinationDuplicate(), prior.runtimeIndex().attemptObligations());
    }

    private MessageRecord rescheduledMessage(final PreparedCommand command, final SourcePosition position,
                                             final MessageRecord prior) {
        final RescheduleRequest values = decodeRescheduleRequest(command);
        return new MessageRecord(MessageStatus.SCHEDULED, Math.incrementExact(prior.generation()),
                Math.incrementExact(prior.stateVersion()),
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
        if (value == null) {
            return null;
        }
        if (!store.shardId().equals(commandId.routingId().shardId())) {
            throw new IllegalStateException("command dedupe key shard mismatch");
        }
        final CommandDedupeRecord record = CommandDedupeRecord.decode(value.payload());
        validateSourcePositionShard(record.result().appliedSourcePosition(), "command dedupe lookup");
        return record;
    }

    private PayloadReservation findReservationForMessage(final DelayMessageId messageId) {
        final int limit = boundedLimitPlusOne(config.maxPendingMessages());
        final List<io.nereusstream.delay.store.ShardStore.KeyValue> entries = store.scan(ColumnFamily.ID,
                new byte[]{2, 1}, new byte[]{3, 1}, Math.max(1, limit));
        if (entries.size() >= limit && config.maxPendingMessages() < Integer.MAX_VALUE) {
            throw new IllegalStateException("reservation scan exceeded configured bound");
        }
        PayloadReservation found = null;
        for (var entry : entries) {
            final PayloadReservation reservation = effectiveReservation(
                    decodeReservationEntry(entry, "message reservation lookup"));
            if (reservation.delayMessageId().equals(messageId)) {
                if (found != null) {
                    throw new IllegalStateException("message has multiple payload reservations");
                }
                found = reservation;
            }
        }
        return found;
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
            final MessageRecord message = decodeMessageEntry(entry, "runtime-index reconciliation");
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
        final RetryPolicySemanticV1 pinnedPolicy = retryPolicyCatalog == null
                ? null : retryPolicyFor(messageId, message,
                SourcePositionCodec.decode(message.scheduleSourcePosition()));
        final int maxPublishAdmissions = pinnedPolicy == null
                ? config.maxPublishAdmissions() : pinnedPolicy.maxPublishAdmissions();
        final int maxUncertainRetries = pinnedPolicy == null
                ? config.maxUncertainRetries() : pinnedPolicy.maxUncertainRetries();
        if (index.admissionsUsed() > maxPublishAdmissions
                || index.uncertainRetryAdmissionsUsed() > maxUncertainRetries) {
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
        validatePublishAttemptShard(ledger, "runtime obligation");
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
        if (value == null) {
            return null;
        }
        final LaneValue laneValue = decodeLaneValue(value.payload());
        if (laneValue.isActive()) {
            final LaneRecord lane = LaneRecord.decode(laneValue.activeStateBytes());
            if (!lane.laneId().equals(laneId)) {
                throw new IllegalStateException("active Lane key/value identity mismatch");
            }
        } else if (!laneValue.terminalGuard().laneId().equals(laneId)) {
            throw new IllegalStateException("terminal Lane key/value identity mismatch");
        } else {
            validateSourcePositionShard(laneValue.terminalGuard().terminalSourcePosition().canonicalBytes(),
                    "terminal Lane guard lookup");
        }
        return laneValue;
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
            final MessageRecord message = decodeMessageEntry(entry, "lane retirement");
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
                candidateLane = decodeClaim(entry).laneId();
            } else if (entry.key()[0] == INFLIGHT_PUBLISHING_KIND
                    || entry.key()[0] == INFLIGHT_UNCERTAIN_KIND) {
                candidateLane = decodePublishAttempt(entry).laneId();
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
        validatePublishAttemptShard(ledger, "publish attempt lookup");
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
        return validateClaimShard(claim, "claim scan");
    }

    private ClaimRecord validateClaimShard(final ClaimRecord claim, final String context) {
        if (!store.shardId().equals(claim.delayMessageId().routingId().shardId())) {
            throw new IllegalStateException("Claim message shard mismatch during " + context);
        }
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
        validatePublishAttemptShard(ledger, "open publish attempt scan");
        return ledger;
    }

    private void validatePublishAttemptShard(final PublishAttemptLedger ledger, final String context) {
        if (!store.shardId().equals(ledger.delayMessageId().routingId().shardId())) {
            throw new IllegalStateException("publish attempt message shard mismatch during " + context);
        }
        validateSourcePositionShard(ledger.sourcePosition(), "publish attempt " + context);
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
                Bytes.u64be(nextMutationSequence()));
    }

    /**
     * Computes the next persisted shard mutation sequence without permitting a
     * signed-long wrap. Every source-ordered WriteBatch calls this helper before
     * it can publish a new position; the in-memory counter is advanced only after
     * RocksDB acknowledges that batch.
     */
    private long nextMutationSequence() {
        return Math.addExact(mutationSequence, 1);
    }

    private static long readSequence(final byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalStateException("invalid shard mutation sequence");
        }
        final long value = ByteBuffer.wrap(bytes).getLong();
        if (value < 0) {
            throw new IllegalStateException("negative persisted shard sequence");
        }
        return value;
    }

    private static long readNonNegativeSequence(final byte[] bytes) {
        if (bytes.length != 8) {
            throw new IllegalStateException("invalid persisted ingress deadline");
        }
        final long value = ByteBuffer.wrap(bytes).getLong();
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

    private record CloseAccounting(long pendingMessages, long pendingBytes,
                                   long reservationMessages, long reservationBytes) {
        private static CloseAccounting empty() {
            return new CloseAccounting(0, 0, 0, 0);
        }
    }

    private record CursorScan(List<io.nereusstream.delay.store.ShardStore.KeyValue> entries, boolean hasMore) {
        private CursorScan {
            entries = List.copyOf(entries);
        }
    }

    private record ClosedMessageAction(DelayMessageId messageId, MessageRecord current, ClaimRecord claim,
                                       byte[] timelineKey, byte[] expiryKey, MessageRecord terminalMessage,
                                       TerminalGenerationRecord terminal) {
        private ClosedMessageAction {
            timelineKey = Bytes.copy(timelineKey);
            expiryKey = Bytes.copy(expiryKey);
        }

        @Override
        public byte[] timelineKey() {
            return Bytes.copy(timelineKey);
        }

        @Override
        public byte[] expiryKey() {
            return Bytes.copy(expiryKey);
        }
    }

    private record ClosedReservationAction(PayloadReservation reservation,
                                           PayloadReservation closedReservation) {
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

    /** A validated durable cursor ready for a bounded close-materializer turn. */
    public record LaneCloseMaterializationWork(
            DestinationLaneId laneId,
            long closeVersion,
            long nextEligibleAtEpochMs,
            LaneCloseMaterializationCursor cursor) {
        public LaneCloseMaterializationWork {
            Objects.requireNonNull(laneId, "laneId");
            Objects.requireNonNull(cursor, "cursor");
            if (closeVersion <= 0 || nextEligibleAtEpochMs < 0 || !laneId.equals(cursor.laneId())
                    || closeVersion != cursor.closeVersion()) {
                throw new IllegalArgumentException("invalid Lane close materialization work");
            }
        }
    }

    /** Result of one bounded local Lane-close materialization turn. */
    public record LaneCloseMaterializationResult(
            io.nereusstream.delay.protocol.DestinationLaneId laneId,
            long closeVersion,
            int scannedRecords,
            int materializedMessages,
            int materializedReservations,
            boolean complete) {
        public LaneCloseMaterializationResult {
            Objects.requireNonNull(laneId, "laneId");
            final int materializedTotal = checkedMaterializedTotal(materializedMessages, materializedReservations);
            if (closeVersion < 0 || scannedRecords < 0 || materializedMessages < 0
                    || materializedReservations < 0 || materializedTotal > scannedRecords) {
                throw new IllegalArgumentException("invalid Lane close materialization result");
            }
        }

        private static int checkedMaterializedTotal(final int messages, final int reservations) {
            try {
                return Math.addExact(messages, reservations);
            } catch (ArithmeticException exception) {
                throw new IllegalArgumentException("Lane close materialization counts overflow", exception);
            }
        }
    }

    private record SystemTimelineKey(long nextEligibleAtEpochMs, byte[] workId, long workVersion) {
        private SystemTimelineKey {
            workId = Bytes.copy(workId);
        }

        @Override
        public byte[] workId() {
            return Bytes.copy(workId);
        }
    }
}
